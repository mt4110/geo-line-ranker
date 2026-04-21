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
