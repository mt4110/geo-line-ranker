use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

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

fn database_url_with_name(database_url: &str, database_name: &str) -> String {
    let Some((prefix, _)) = database_url.rsplit_once('/') else {
        return database_url.to_string();
    };
    format!("{prefix}/{database_name}")
}

fn unique_database_name(prefix: &str) -> String {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), suffix)
}

async fn create_empty_database(prefix: &str) -> anyhow::Result<(String, String, String)> {
    let admin_database_url = database_url_with_name(&default_database_url(), "postgres");
    let database_name = unique_database_name(prefix);
    let (client, connection) = tokio_postgres::connect(&admin_database_url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query(&format!("CREATE DATABASE \"{database_name}\""))
        .await?;

    Ok((
        admin_database_url,
        database_url_with_name(&default_database_url(), &database_name),
        database_name,
    ))
}

async fn drop_database(admin_database_url: &str, database_name: &str) -> anyhow::Result<()> {
    let (client, connection) = tokio_postgres::connect(admin_database_url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query(&format!(
            "SELECT pg_terminate_backend(pid)
             FROM pg_stat_activity
             WHERE datname = '{database_name}'
               AND pid <> pg_backend_pid()"
        ))
        .await?;
    client
        .simple_query(&format!("DROP DATABASE IF EXISTS \"{database_name}\""))
        .await?;
    Ok(())
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

#[tokio::test]
async fn worker_reclaims_stale_running_jobs() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_worker").await
    else {
        eprintln!(
            "skipping worker stale-reclaim test because PostgreSQL admin access is unavailable"
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

        let repository = Arc::new(PgRepository::new(database_url.clone()));
        let job_id = repository
            .enqueue_job(&NewJob {
                job_type: JobType::RefreshPopularitySnapshot,
                payload: serde_json::json!({}),
                max_attempts: 3,
            })
            .await?;

        let first_claim = repository
            .claim_next_job("worker-crashed")
            .await?
            .expect("initial claim");
        assert_eq!(first_claim.job_id, job_id);
        assert_eq!(first_claim.attempt_number, 1);

        client
            .execute(
                "UPDATE job_queue
                 SET locked_at = NOW() - INTERVAL '20 minutes'
                 WHERE id = $1",
                &[&job_id],
            )
            .await?;

        let reclaimed = repository
            .claim_next_job("worker-recovery")
            .await?
            .expect("reclaimed claim");
        assert_eq!(reclaimed.job_id, job_id);
        assert_eq!(reclaimed.attempt_number, 2);

        let attempts = client
            .query(
                "SELECT attempt_number, status, error_message
                 FROM job_attempts
                 WHERE job_id = $1
                 ORDER BY attempt_number ASC",
                &[&job_id],
            )
            .await?;
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].get::<_, i32>("attempt_number"), 1);
        assert_eq!(attempts[0].get::<_, String>("status"), "failed");
        assert_eq!(
            attempts[0].get::<_, Option<String>>("error_message"),
            Some("worker lock expired before completion".to_string())
        );
        assert_eq!(attempts[1].get::<_, i32>("attempt_number"), 2);
        assert_eq!(attempts[1].get::<_, String>("status"), "running");

        let queue_row = client
            .query_one(
                "SELECT status, locked_by, attempts
                 FROM job_queue
                 WHERE id = $1",
                &[&job_id],
            )
            .await?;
        assert_eq!(queue_row.get::<_, String>("status"), "running");
        assert_eq!(
            queue_row.get::<_, Option<String>>("locked_by"),
            Some("worker-recovery".to_string())
        );
        assert_eq!(queue_row.get::<_, i32>("attempts"), 2);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
