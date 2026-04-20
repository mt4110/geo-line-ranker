use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::json;
use storage_postgres::{
    begin_crawl_run, claim_latest_fetched_crawl_run, mark_crawl_run_fetched, run_migrations,
    SourceManifestAudit,
};
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
async fn claim_latest_fetched_crawl_run_allows_only_one_concurrent_claimer() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_crawl_claim").await
    else {
        eprintln!(
            "skipping storage-postgres crawl claim test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        run_migrations(
            &database_url,
            repo_root().join("storage/migrations/postgres"),
        )
        .await?;

        let manifest = SourceManifestAudit {
            manifest_path: "/tmp/test-crawl-manifest.yaml".to_string(),
            source_id: "test-source".to_string(),
            source_name: "Test Source".to_string(),
            manifest_version: 1,
            parser_version: "single_title_page_v1".to_string(),
            manifest_json: json!({
                "source_id": "test-source",
                "source_name": "Test Source",
                "parser_key": "single_title_page_v1"
            }),
        };
        let crawl_run_id =
            begin_crawl_run(&database_url, &manifest, "single_title_page_v1").await?;
        mark_crawl_run_fetched(&database_url, crawl_run_id, 1).await?;

        let first_database_url = database_url.clone();
        let second_database_url = database_url.clone();
        let first_manifest_path = manifest.manifest_path.clone();
        let second_manifest_path = manifest.manifest_path.clone();
        let first = tokio::spawn(async move {
            claim_latest_fetched_crawl_run(&first_database_url, &first_manifest_path).await
        });
        let second = tokio::spawn(async move {
            claim_latest_fetched_crawl_run(&second_database_url, &second_manifest_path).await
        });

        let first_claim = first.await??;
        let second_claim = second.await??;
        let claimed_run_ids = [first_claim, second_claim]
            .into_iter()
            .flatten()
            .map(|state| state.crawl_run_id)
            .collect::<Vec<_>>();

        assert_eq!(claimed_run_ids, vec![crawl_run_id]);

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let status = client
            .query_one(
                "SELECT status FROM crawl_runs WHERE id = $1",
                &[&crawl_run_id],
            )
            .await?
            .get::<_, String>("status");
        assert_eq!(status, "parsing");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
