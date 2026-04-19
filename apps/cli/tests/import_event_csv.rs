use std::{fs, path::PathBuf};

use cli::run_event_csv_import;
use config::{AppSettings, CandidateRetrievalMode, OpenSearchSettings};
use storage_postgres::{run_migrations, seed_fixture};
use tokio_postgres::NoTls;

fn default_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@127.0.0.1:5433/geo_line_ranker".to_string()
    })
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn test_settings(raw_storage_dir: &std::path::Path, database_url: &str) -> AppSettings {
    let root = repo_root();
    AppSettings {
        bind_addr: "127.0.0.1:0".to_string(),
        database_url: database_url.to_string(),
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
    let database_url = default_database_url();
    let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
        eprintln!("skipping event CSV integration test because PostgreSQL is not reachable");
        return Ok(());
    };
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
event_phase5_a,school_seaside,Seaside May Open,open_campus,true,true,1.0,2026-05-01T10:00:00+09:00,home|detail\n\
event_phase5_b,school_garden,Garden Lab,trial_class,false,false,0.5,2026-05-03T13:00:00+09:00,search\n",
    )?;

    run_event_csv_import(&settings, &csv_path).await?;
    run_event_csv_import(&settings, &csv_path).await?;

    let active_count = client
        .query_one(
            "SELECT COUNT(*) AS count
             FROM events
             WHERE source_type = 'event_csv'
               AND source_key = $1
               AND is_active = TRUE",
            &[&csv_path.canonicalize()?.display().to_string()],
        )
        .await?
        .get::<_, i64>("count");
    assert_eq!(active_count, 2);

    fs::write(
        &csv_path,
        "event_id,school_id,title,event_category,is_open_day,is_featured,priority_weight,starts_at,placement_tags\n\
event_phase5_a,school_seaside,Seaside June Open,open_campus,true,false,0.8,2026-06-01T10:00:00+09:00,home|detail\n",
    )?;

    run_event_csv_import(&settings, &csv_path).await?;

    let updated_title = client
        .query_one(
            "SELECT title
             FROM events
             WHERE id = 'event_phase5_a'",
            &[],
        )
        .await?
        .get::<_, String>("title");
    let stale_active = client
        .query_one(
            "SELECT is_active
             FROM events
             WHERE id = 'event_phase5_b'",
            &[],
        )
        .await?
        .get::<_, bool>("is_active");

    assert_eq!(updated_title, "Seaside June Open");
    assert!(!stale_active);

    Ok(())
}
