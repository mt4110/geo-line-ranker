use std::fs;

use cli::run_event_csv_import;
use config::{AppSettings, CandidateRetrievalMode, OpenSearchSettings};
use storage_postgres::{run_migrations, seed_fixture};
use tokio_postgres::NoTls;

mod common;

use common::{create_empty_database, drop_database, repo_root};

fn test_settings(raw_storage_dir: &std::path::Path, database_url: &str) -> AppSettings {
    let root = repo_root();
    AppSettings {
        bind_addr: "127.0.0.1:0".to_string(),
        database_url: database_url.to_string(),
        postgres_pool_max_size: 4,
        redis_url: None,
        ranking_config_dir: root.join("configs/ranking").display().to_string(),
        fixture_dir: root.join("storage/fixtures/minimal").display().to_string(),
        raw_storage_dir: raw_storage_dir.display().to_string(),
        algorithm_version: "phase5-test".to_string(),
        candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
        candidate_retrieval_limit: 256,
        opensearch: OpenSearchSettings {
            url: "http://127.0.0.1:9200".to_string(),
            index_name: "geo_line_ranker_candidates".to_string(),
            username: None,
            password: None,
            request_timeout_secs: 5,
        },
        recommendation_cache_ttl_secs: 60,
        worker_poll_interval_ms: 1000,
        worker_retry_delay_secs: 5,
        worker_max_attempts: 3,
    }
}

#[tokio::test]
async fn event_csv_import_is_idempotent_and_deactivates_stale_rows() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_cli_event_csv").await
    else {
        eprintln!(
            "skipping event CSV integration test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;
        seed_fixture(&database_url, root.join("storage/fixtures/minimal")).await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let csv_path = temp.path().join("events.csv");

        fs::write(
            &csv_path,
            "event_id,school_id,title,event_category,is_open_day,is_featured,priority_weight,starts_at,placement_tags\n\
event_csv_idempotent_a,school_seaside,Seaside May Open,open_campus,true,true,1.0,2026-05-01T10:00:00+09:00,home|detail\n\
event_csv_idempotent_b,school_garden,Garden Lab,trial_class,false,false,0.5,2026-05-03T13:00:00+09:00,search\n",
        )?;

        run_event_csv_import(&settings, &csv_path).await?;
        run_event_csv_import(&settings, &csv_path).await?;

        let active_count = client
            .query_one(
                "SELECT COUNT(*) AS count
             FROM events
             WHERE id = ANY($1)
               AND source_type = 'event_csv'
               AND source_key = $2
               AND is_active = TRUE",
                &[
                    &vec![
                        "event_csv_idempotent_a".to_string(),
                        "event_csv_idempotent_b".to_string(),
                    ],
                    &"event-csv",
                ],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(active_count, 2);

        fs::write(
            &csv_path,
            "event_id,school_id,title,event_category,is_open_day,is_featured,priority_weight,starts_at,placement_tags\n\
event_csv_idempotent_a,school_seaside,Seaside June Open,open_campus,true,false,0.8,2026-06-01T10:00:00+09:00,home|detail\n",
        )?;

        run_event_csv_import(&settings, &csv_path).await?;

        let updated_title = client
            .query_one(
                "SELECT title
             FROM events
             WHERE id = 'event_csv_idempotent_a'",
                &[],
            )
            .await?
            .get::<_, String>("title");
        let stale_active = client
            .query_one(
                "SELECT is_active
             FROM events
             WHERE id = 'event_csv_idempotent_b'",
                &[],
            )
            .await?
            .get::<_, bool>("is_active");

        assert_eq!(updated_title, "Seaside June Open");
        assert!(!stale_active);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn event_csv_import_deactivates_stale_rows_across_file_renames() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_cli_event_csv_renames").await
    else {
        eprintln!(
            "skipping event CSV integration test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;
        seed_fixture(&database_url, root.join("storage/fixtures/minimal")).await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let first_csv_path = temp.path().join("events-2026-05.csv");
        let second_csv_path = temp.path().join("events-2026-06.csv");

        fs::write(
            &first_csv_path,
            "event_id,school_id,title,event_category,is_open_day,is_featured,priority_weight,starts_at,placement_tags\n\
event_csv_rename_a,school_seaside,Seaside Rename May Open,open_campus,true,true,1.0,2026-05-01T10:00:00+09:00,home|detail\n\
event_csv_rename_b,school_garden,Garden Rename Lab,trial_class,false,false,0.5,2026-05-03T13:00:00+09:00,search\n",
        )?;
        run_event_csv_import(&settings, &first_csv_path).await?;

        fs::write(
            &second_csv_path,
            "event_id,school_id,title,event_category,is_open_day,is_featured,priority_weight,starts_at,placement_tags\n\
event_csv_rename_a,school_seaside,Seaside Rename June Open,open_campus,true,false,0.8,2026-06-01T10:00:00+09:00,home|detail\n",
        )?;
        run_event_csv_import(&settings, &second_csv_path).await?;

        let active_titles = client
            .query(
                "SELECT title
             FROM events
             WHERE id = ANY($1)
               AND source_type = 'event_csv'
               AND source_key = $2
               AND is_active = TRUE
               ORDER BY title ASC",
                &[
                    &vec![
                        "event_csv_rename_a".to_string(),
                        "event_csv_rename_b".to_string(),
                    ],
                    &"event-csv",
                ],
            )
            .await?;
        assert_eq!(active_titles.len(), 1);
        assert_eq!(
            active_titles[0].get::<_, String>("title"),
            "Seaside Rename June Open"
        );

        let stale_active = client
            .query_one(
                "SELECT is_active
             FROM events
             WHERE id = 'event_csv_rename_b'",
                &[],
            )
            .await?
            .get::<_, bool>("is_active");
        assert!(!stale_active);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
