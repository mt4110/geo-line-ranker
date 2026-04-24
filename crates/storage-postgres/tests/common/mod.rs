use std::{
    path::PathBuf,
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio_postgres::NoTls;

static DATABASE_DDL_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

pub fn default_database_url() -> String {
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

pub async fn create_empty_database(prefix: &str) -> anyhow::Result<(String, String, String)> {
    let _ddl_guard = DATABASE_DDL_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
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

pub async fn drop_database(admin_database_url: &str, database_name: &str) -> anyhow::Result<()> {
    let _ddl_guard = DATABASE_DDL_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
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

pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
