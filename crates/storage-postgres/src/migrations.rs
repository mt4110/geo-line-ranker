use std::{fs, path::Path, sync::OnceLock};

use anyhow::{Context, Result};
use deadpool_postgres::Client;

use crate::repository::PgRepository;

static MIGRATION_PROCESS_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

const SCHEMA_MIGRATION_LOCK_NAMESPACE: i32 = 6_042;
const SCHEMA_MIGRATION_LOCK_KEY: i32 = 1;

pub async fn run_migrations(database_url: &str, migrations_dir: impl AsRef<Path>) -> Result<()> {
    let _migration_guard = MIGRATION_PROCESS_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let repo = PgRepository::new(database_url);
    let mut client = repo.connect().await?;
    acquire_schema_migration_lock(&client).await?;
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
        )
        .await?;

    let mut entries =
        fs::read_dir(migrations_dir.as_ref())?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sql") {
            continue;
        }
        let version = path
            .file_name()
            .and_then(|name| name.to_str())
            .context("migration file name must be valid UTF-8")?
            .to_string();
        let sql = fs::read_to_string(&path)
            .with_context(|| format!("failed to read migration {}", path.display()))?;
        let transaction = client.transaction().await?;
        let claimed = transaction
            .query_opt(
                "INSERT INTO schema_migrations (version)
                 VALUES ($1)
                 ON CONFLICT (version) DO NOTHING
                 RETURNING version",
                &[&version],
            )
            .await?;
        if claimed.is_none() {
            transaction.rollback().await?;
            continue;
        }
        transaction.batch_execute(&sql).await?;
        transaction.commit().await?;
    }

    release_schema_migration_lock(&client).await?;
    Ok(())
}

async fn acquire_schema_migration_lock(client: &Client) -> Result<()> {
    client
        .query_one(
            "SELECT pg_advisory_lock($1, $2)",
            &[&SCHEMA_MIGRATION_LOCK_NAMESPACE, &SCHEMA_MIGRATION_LOCK_KEY],
        )
        .await?;
    Ok(())
}

async fn release_schema_migration_lock(client: &Client) -> Result<()> {
    let row = client
        .query_one(
            "SELECT pg_advisory_unlock($1, $2) AS unlocked",
            &[&SCHEMA_MIGRATION_LOCK_NAMESPACE, &SCHEMA_MIGRATION_LOCK_KEY],
        )
        .await?;
    anyhow::ensure!(
        row.get::<_, bool>("unlocked"),
        "failed to release schema migration advisory lock"
    );
    Ok(())
}
