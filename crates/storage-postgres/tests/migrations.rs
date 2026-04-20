use storage_postgres::{run_migrations, seed_fixture};
use tokio_postgres::NoTls;
mod common;

use common::{create_empty_database, drop_database, repo_root};

#[tokio::test]
async fn run_migrations_is_safe_when_called_concurrently() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_migrations").await
    else {
        eprintln!(
            "skipping storage-postgres migration concurrency test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let migrations_dir = repo_root().join("storage/migrations/postgres");
        let expected_count = std::fs::read_dir(&migrations_dir)?
            .collect::<std::io::Result<Vec<_>>>()?
            .into_iter()
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("sql"))
            .count() as i64;

        let database_url_a = database_url.clone();
        let database_url_b = database_url.clone();
        let migrations_dir_a = migrations_dir.clone();
        let migrations_dir_b = migrations_dir.clone();
        let first =
            tokio::spawn(async move { run_migrations(&database_url_a, &migrations_dir_a).await });
        let second =
            tokio::spawn(async move { run_migrations(&database_url_b, &migrations_dir_b).await });

        first.await??;
        second.await??;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one("SELECT COUNT(*) AS count FROM schema_migrations", &[])
            .await?;
        assert_eq!(row.get::<_, i64>("count"), expected_count);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn seed_fixture_is_idempotent_for_user_events() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_seed_fixture").await
    else {
        eprintln!(
            "skipping storage-postgres seed idempotency test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;

        let fixture_dir = root.join("storage/fixtures/minimal");
        let expected_user_event_count =
            std::fs::read_to_string(fixture_dir.join("user_events.ndjson"))?
                .lines()
                .filter(|line| !line.trim().is_empty())
                .count() as i64;

        seed_fixture(&database_url, &fixture_dir).await?;
        seed_fixture(&database_url, &fixture_dir).await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one("SELECT COUNT(*) AS count FROM user_events", &[])
            .await?;
        assert_eq!(row.get::<_, i64>("count"), expected_user_event_count);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
