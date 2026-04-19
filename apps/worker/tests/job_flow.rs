use std::{path::PathBuf, sync::Arc};

use cache::RecommendationCache;
use config::AppSettings;
use storage::{JobType, NewJob, RecommendationRepository};
use storage_postgres::{run_migrations, seed_fixture, PgRepository};
use tokio_postgres::NoTls;
use worker_core::WorkerService;

fn default_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@127.0.0.1:5433/geo_line_ranker".to_string()
    })
}

fn default_redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[tokio::test]
async fn worker_processes_snapshot_and_cache_jobs() -> anyhow::Result<()> {
    let database_url = default_database_url();
    let redis_url = default_redis_url();
    let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
        eprintln!("skipping worker integration test because PostgreSQL is not reachable");
        return Ok(());
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client.simple_query("SELECT 1").await?;

    let root = repo_root();
    run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;
    seed_fixture(&database_url, root.join("storage/fixtures/minimal")).await?;

    let cache = RecommendationCache::new(Some(redis_url), 60);
    let cache_key = cache.build_key(
        "profile-worker-test",
        "algo-worker-test",
        "sql_only",
        &serde_json::json!({ "worker_test": true }),
    )?;
    if let Err(error) = cache
        .set_json(&cache_key, &serde_json::json!({ "cached": true }))
        .await
    {
        eprintln!("skipping worker integration test because Redis is not reachable: {error}");
        return Ok(());
    }

    let repository = Arc::new(PgRepository::new(database_url.clone()));
    let job_baseline = client
        .query_one("SELECT COALESCE(MAX(id), 0) AS max_id FROM job_queue", &[])
        .await?
        .get::<_, i64>("max_id");

    repository
        .enqueue_job(&NewJob {
            job_type: JobType::RefreshPopularitySnapshot,
            payload: serde_json::json!({}),
            max_attempts: 3,
        })
        .await?;
    repository
        .enqueue_job(&NewJob {
            job_type: JobType::RefreshUserAffinitySnapshot,
            payload: serde_json::json!({ "user_id": "demo-user-1" }),
            max_attempts: 3,
        })
        .await?;
    repository
        .enqueue_job(&NewJob {
            job_type: JobType::InvalidateRecommendationCache,
            payload: serde_json::json!({ "scope": "recommendations" }),
            max_attempts: 3,
        })
        .await?;

    let settings = AppSettings::from_env()?;
    let worker = WorkerService::new(
        Arc::clone(&repository),
        cache.clone(),
        format!("worker-test-{}", std::process::id()),
        None,
        settings.worker_retry_delay_secs,
    );
    let processed = worker.run_until_empty(10).await?;
    assert!(processed >= 3);

    let popularity_count = client
        .query_one("SELECT COUNT(*) AS count FROM popularity_snapshots", &[])
        .await?
        .get::<_, i64>("count");
    assert!(popularity_count >= 1);

    let affinity_count = client
        .query_one(
            "SELECT COUNT(*) AS count
             FROM user_affinity_snapshots
             WHERE user_id = 'demo-user-1'",
            &[],
        )
        .await?
        .get::<_, i64>("count");
    assert!(affinity_count >= 1);

    let cached_value = cache.get_json::<serde_json::Value>(&cache_key).await?;
    assert!(cached_value.is_none());

    let statuses = client
        .query(
            "SELECT status
             FROM job_queue
             WHERE id > $1
             ORDER BY id",
            &[&job_baseline],
        )
        .await?
        .into_iter()
        .map(|row| row.get::<_, String>("status"))
        .collect::<Vec<_>>();
    assert!(statuses.iter().all(|status| status == "succeeded"));

    Ok(())
}
