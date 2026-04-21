use std::sync::Arc;

use storage::{JobType, NewJob, RecommendationRepository};
use storage_postgres::{run_migrations, PgRepository};
use tokio_postgres::NoTls;

mod common;

use common::{create_empty_database, drop_database, repo_root};

#[tokio::test]
async fn enqueue_job_coalesces_concurrent_global_popularity_refreshes() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_job_queue").await
    else {
        eprintln!(
            "skipping storage-postgres job queue test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;

        let repository = Arc::new(PgRepository::new(database_url.clone()));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let repository = Arc::clone(&repository);
            tasks.push(tokio::spawn(async move {
                repository
                    .enqueue_job(&NewJob {
                        job_type: JobType::RefreshPopularitySnapshot,
                        payload: serde_json::json!({}),
                        max_attempts: 3,
                    })
                    .await
            }));
        }

        let mut job_ids = Vec::new();
        for task in tasks {
            job_ids.push(task.await??);
        }

        let first_job_id = job_ids[0];
        assert!(job_ids.iter().all(|job_id| *job_id == first_job_id));

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let refresh_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM job_queue
                 WHERE job_type = 'refresh_popularity_snapshot'
                   AND payload = '{}'::jsonb
                   AND status = 'queued'",
                &[],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(refresh_count, 1);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn job_recovery_helpers_preserve_attempt_history() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_job_recovery").await
    else {
        eprintln!(
            "skipping storage-postgres job recovery test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;

        let repository = PgRepository::new(database_url.clone());
        let job_id = repository
            .enqueue_job(&NewJob {
                job_type: JobType::RefreshPopularitySnapshot,
                payload: serde_json::json!({ "manual": true }),
                max_attempts: 1,
            })
            .await?;
        let claim = repository
            .claim_next_job("worker-failing")
            .await?
            .expect("queued job should be claimable");
        repository
            .mark_job_failed(job_id, claim.attempt_id, "dependency unavailable", 30)
            .await?;

        let failed = repository.inspect_job(job_id).await?;
        assert_eq!(failed.job.status, "failed");
        assert_eq!(failed.job.attempts, 1);
        assert_eq!(failed.attempts.len(), 1);
        assert_eq!(failed.attempts[0].status, "failed");
        assert_eq!(
            failed.attempts[0].error_message.as_deref(),
            Some("dependency unavailable")
        );

        let retry = repository.retry_failed_job(job_id).await?;
        assert!(retry.updated);
        assert_eq!(retry.job.status, "queued");
        assert_eq!(retry.job.attempts, 1);
        assert_eq!(retry.job.max_attempts, 2);
        assert_eq!(retry.job.last_error, None);

        let after_retry = repository.inspect_job(job_id).await?;
        assert_eq!(after_retry.attempts.len(), 1);
        assert_eq!(after_retry.attempts[0].status, "failed");

        let skipped_retry = repository.retry_failed_job(job_id).await?;
        assert!(!skipped_retry.updated);
        assert_eq!(skipped_retry.job.status, "queued");

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .execute(
                "UPDATE job_queue
                 SET run_after = NOW() + INTERVAL '1 hour'
                 WHERE id = $1",
                &[&job_id],
            )
            .await?;

        let due = repository.make_queued_job_due(job_id).await?;
        assert!(due.updated);
        assert_eq!(due.job.status, "queued");

        let already_due = repository.make_queued_job_due(job_id).await?;
        assert!(!already_due.updated);
        assert_eq!(already_due.job.status, "queued");

        let snapshot = repository.list_jobs(10).await?;
        assert!(snapshot.jobs.iter().any(|job| job.id == job_id));
        assert!(snapshot
            .pressure
            .iter()
            .any(|row| row.job_type == "refresh_popularity_snapshot" && row.status == "queued"));

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
