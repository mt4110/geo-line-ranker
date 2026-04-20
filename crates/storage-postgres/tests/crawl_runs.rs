use serde_json::json;
use storage_postgres::{
    begin_crawl_run, claim_fetched_crawl_run, claim_latest_fetched_crawl_run,
    mark_crawl_run_fetched, run_migrations, SourceManifestAudit,
};
use tokio_postgres::NoTls;
mod common;

use common::{create_empty_database, drop_database, repo_root};

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

#[tokio::test]
async fn claim_fetched_crawl_run_claims_requested_run_even_when_newer_run_exists(
) -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_crawl_claim_by_id").await
    else {
        eprintln!(
            "skipping storage-postgres crawl claim-by-id test because PostgreSQL admin access is unavailable"
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
        let older_run_id =
            begin_crawl_run(&database_url, &manifest, "single_title_page_v1").await?;
        mark_crawl_run_fetched(&database_url, older_run_id, 1).await?;
        let newer_run_id =
            begin_crawl_run(&database_url, &manifest, "single_title_page_v1").await?;
        mark_crawl_run_fetched(&database_url, newer_run_id, 1).await?;

        let claimed = claim_fetched_crawl_run(&database_url, older_run_id)
            .await?
            .expect("older fetched run should still be claimable by id");
        assert_eq!(claimed.crawl_run_id, older_run_id);

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let statuses = client
            .query(
                "SELECT id, status
                 FROM crawl_runs
                 WHERE id = ANY($1)
                 ORDER BY id ASC",
                &[&vec![older_run_id, newer_run_id]],
            )
            .await?;
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0].get::<_, i64>("id"), older_run_id);
        assert_eq!(statuses[0].get::<_, String>("status"), "parsing");
        assert_eq!(statuses[1].get::<_, i64>("id"), newer_run_id);
        assert_eq!(statuses[1].get::<_, String>("status"), "fetched");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
