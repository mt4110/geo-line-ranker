use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use storage_postgres::{run_migrations, seed_fixture};
use tokio_postgres::NoTls;

fn default_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@127.0.0.1:5433/geo_line_ranker".to_string()
    })
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
        .query(
            "SELECT pg_terminate_backend(pid)
             FROM pg_stat_activity
             WHERE datname = $1
               AND pid <> pg_backend_pid()",
            &[&database_name],
        )
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
