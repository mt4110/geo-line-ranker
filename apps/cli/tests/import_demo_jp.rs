use cli::{run_derive_school_station_links, run_import_command, ImportTarget};
use config::{AppSettings, CandidateRetrievalMode, OpenSearchSettings};
use storage_postgres::run_migrations;
use tokio_postgres::NoTls;

mod common;

use common::{create_empty_database, drop_database, repo_root};

#[tokio::test]
async fn imports_demo_jp_fixture_when_database_is_available() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_cli_import_demo").await
    else {
        eprintln!(
            "skipping importer integration test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };
    let test_result = async {
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let repo_root = repo_root();
        run_migrations(&database_url, repo_root.join("storage/migrations/postgres")).await?;

        let temp = tempfile::tempdir()?;
        let settings = AppSettings {
            bind_addr: "127.0.0.1:0".to_string(),
            database_url: database_url.clone(),
            postgres_pool_max_size: 4,
            redis_url: None,
            profile_id: "local-discovery-generic".to_string(),
            profile_pack_manifest: repo_root
                .join("configs/profiles/local-discovery-generic/profile.yaml")
                .display()
                .to_string(),
            profile_fixture_set_id: Some("minimal".to_string()),
            ranking_config_dir: repo_root.join("configs/ranking").display().to_string(),
            fixture_dir: repo_root
                .join("storage/fixtures/minimal")
                .display()
                .to_string(),
            raw_storage_dir: temp.path().join("raw").display().to_string(),
            algorithm_version: "phase3-test".to_string(),
            candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
            candidate_retrieval_limit: 256,
            opensearch: OpenSearchSettings {
                url: "http://127.0.0.1:9200".to_string(),
                index_name: "geo_line_ranker_candidates".to_string(),
                username: None,
                password: None,
                request_timeout_secs: 5,
            },
            recommendation_cache_ttl_secs: 60,
            worker_poll_interval_ms: 1000,
            worker_retry_delay_secs: 5,
            worker_max_attempts: 3,
        };

        run_import_command(
            &settings,
            ImportTarget::JpRail,
            repo_root.join("storage/sources/jp_rail/example.yaml"),
        )
        .await?;
        run_import_command(
            &settings,
            ImportTarget::JpPostal,
            repo_root.join("storage/sources/jp_postal/example.yaml"),
        )
        .await?;
        run_import_command(
            &settings,
            ImportTarget::JpSchoolCodes,
            repo_root.join("storage/sources/jp_school/example.yaml"),
        )
        .await?;
        run_import_command(
            &settings,
            ImportTarget::JpSchoolGeodata,
            repo_root.join("storage/sources/jp_school_geo/example.yaml"),
        )
        .await?;
        run_derive_school_station_links(&settings).await?;

        let (verify_client, verify_connection) =
            tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = verify_connection.await;
        });

        let school_count = verify_client
            .query_one(
                "SELECT COUNT(*) AS count FROM schools WHERE id LIKE 'jp_school_%'",
                &[],
            )
            .await?
            .get::<_, i64>("count");
        let link_count = verify_client
            .query_one(
                "SELECT COUNT(*) AS count FROM school_station_links WHERE school_id LIKE 'jp_school_%'",
                &[],
            )
            .await?
            .get::<_, i64>("count");
        let successful_runs = verify_client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM import_runs
                 WHERE source_id IN ('jp-rail', 'jp-postal', 'jp-school-codes', 'jp-school-geodata')
                   AND status = 'succeeded'",
                &[],
            )
            .await?
            .get::<_, i64>("count");

        assert!(school_count >= 3);
        assert!(link_count >= 3);
        assert!(successful_runs >= 4);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
