use std::fs;

use cli::run_snapshot_refresh;
use config::{AppSettings, CandidateRetrievalMode, OpenSearchSettings};
use storage_postgres::{run_migrations, seed_fixture};
use tokio_postgres::NoTls;

mod common;

use common::{create_empty_database, drop_database, repo_root};

fn test_settings(
    raw_storage_dir: &std::path::Path,
    ranking_config_dir: &std::path::Path,
    database_url: &str,
) -> AppSettings {
    let root = repo_root();
    AppSettings {
        bind_addr: "127.0.0.1:0".to_string(),
        database_url: database_url.to_string(),
        postgres_pool_max_size: 4,
        redis_url: None,
        ranking_config_dir: ranking_config_dir.display().to_string(),
        fixture_dir: root.join("storage/fixtures/minimal").display().to_string(),
        raw_storage_dir: raw_storage_dir.display().to_string(),
        algorithm_version: "phase7-test".to_string(),
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

fn copy_default_configs(target: &std::path::Path) -> anyhow::Result<()> {
    let root = repo_root().join("configs/ranking");
    for name in [
        "schools.default.yaml",
        "events.default.yaml",
        "fallback.default.yaml",
        "tracking.default.yaml",
        "placement.home.yaml",
        "placement.search.yaml",
        "placement.detail.yaml",
        "placement.mypage.yaml",
    ] {
        fs::copy(root.join(name), target.join(name))?;
    }
    Ok(())
}

#[tokio::test]
async fn snapshot_refresh_uses_tracking_config_weights() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_cli_snapshot_refresh").await
    else {
        eprintln!(
            "skipping snapshot refresh integration test because PostgreSQL admin access is unavailable"
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

        client
            .execute(
                "INSERT INTO user_events (
                    user_id,
                    event_type,
                    target_station_id,
                    occurred_at,
                    payload
                 )
                 VALUES ('snapshot-user-1', 'search_execute', 'st_tamachi', '2026-04-21T00:00:00Z', '{}'::jsonb)",
                &[],
            )
            .await?;

        let temp = tempfile::tempdir()?;
        let config_dir = temp.path().join("ranking");
        fs::create_dir_all(&config_dir)?;
        copy_default_configs(&config_dir)?;
        fs::write(
            config_dir.join("tracking.default.yaml"),
            "schema_version: 1\nkind: ranking_tracking\npopularity_bonus_weight: 0.75\nuser_affinity_bonus_weight: 0.9\narea_affinity_bonus_weight: 0.35\nsearch_execute_school_signal_weight: 0.0\nsearch_execute_area_signal_weight: 0.0\n",
        )?;

        let settings = test_settings(&temp.path().join("raw"), &config_dir, &database_url);
        let summary = run_snapshot_refresh(&settings).await?;
        assert!(summary.refreshed_school_rows >= 1);
        assert!(summary.refreshed_area_rows >= 1);
        assert_eq!(summary.invalidated_cache_keys, 0);
        assert_eq!(summary.projection_indexed_documents, 0);
        assert_eq!(summary.projection_deleted_documents, 0);
        assert_eq!(summary.search_execute_school_signal_weight, 0.0);
        assert_eq!(summary.search_execute_area_signal_weight, 0.0);

        let garden_row = client
            .query_one(
                "SELECT popularity_score, total_events, search_execute_count
                 FROM popularity_snapshots
                 WHERE school_id = 'school_garden'",
                &[],
            )
            .await?;
        assert_eq!(garden_row.get::<_, f64>("popularity_score"), 0.0);
        assert_eq!(garden_row.get::<_, i64>("total_events"), 1);
        assert_eq!(garden_row.get::<_, i64>("search_execute_count"), 1);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
