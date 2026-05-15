use generic_csv::{SourceFileSpec, SourceManifest, SourceManifestKind};
use serde_json::json;
use storage::{
    EvaluationRunCaseRecord, EvaluationRunCaseStatus, EvaluationRunKind, EvaluationRunRecord,
    EvaluationRunStatus, IngestRunLineageRecord, ProfileCompatibilityStatus,
    ProfileCompatibilityStatusRecord, ProfileManifestRecord, ProfileRegistryRepository,
};
use storage_postgres::{
    begin_crawl_run, begin_crawl_run_with_lineage, begin_import_run, begin_import_run_with_lineage,
    run_migrations, PgRepository, SourceManifestAudit,
};
use tokio_postgres::{NoTls, Row};

mod common;

use common::{create_empty_database, drop_database, repo_root};

#[tokio::test]
async fn profile_registry_persists_manifest_status_and_evaluation_runs() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("glr_profile_registry").await
    else {
        eprintln!(
            "skipping profile registry persistence test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        run_migrations(
            &database_url,
            repo_root().join("storage/migrations/postgres"),
        )
        .await?;
        let repository = PgRepository::new(&database_url);
        let manifest = profile_manifest_record();
        let lineage_id = repository.upsert_profile_manifest(&manifest).await?;
        let second_lineage_id = repository.upsert_profile_manifest(&manifest).await?;
        assert_eq!(
            lineage_id, second_lineage_id,
            "same manifest checksum should keep one lineage row"
        );
        repository
            .record_profile_compatibility_status(&ProfileCompatibilityStatusRecord {
                profile_id: manifest.profile_id.clone(),
                compatibility_level: manifest.compatibility_level.clone(),
                status: ProfileCompatibilityStatus::Valid,
                evidence: json!({ "command": "profile validate" }),
            })
            .await?;
        let evaluation_run_id = repository
            .record_evaluation_run(&evaluation_run_record(&manifest.profile_id, lineage_id))
            .await?;
        let mut other_manifest = profile_manifest_record();
        other_manifest.profile_id = "local-discovery-generic".to_string();
        other_manifest.display_name = "Local Discovery Generic".to_string();
        other_manifest.manifest_path =
            "configs/profiles/local-discovery-generic/profile.yaml".to_string();
        other_manifest.manifest_checksum_sha256 =
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_string();
        other_manifest.manifest_payload = json!({ "profile_id": "local-discovery-generic" });
        other_manifest.reason_catalog_path =
            "configs/profiles/local-discovery-generic/reasons.yaml".to_string();
        let other_lineage_id = repository.upsert_profile_manifest(&other_manifest).await?;
        let mismatched_run = evaluation_run_record(&manifest.profile_id, other_lineage_id);

        assert!(
            repository
                .record_evaluation_run(&mismatched_run)
                .await
                .is_err(),
            "evaluation run lineage must belong to the same profile_id"
        );

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(
                "SELECT active_manifest_lineage_id
                 FROM profile_registry
                 WHERE profile_id = $1",
                &[&manifest.profile_id],
            )
            .await?;
        assert_eq!(
            row.get::<_, Option<i64>>("active_manifest_lineage_id"),
            Some(lineage_id)
        );
        let row = client
            .query_one(
                "SELECT COUNT(*)::BIGINT AS lineage_count
                 FROM profile_pack_manifest_lineage
                 WHERE profile_id = $1",
                &[&manifest.profile_id],
            )
            .await?;
        assert_eq!(row.get::<_, i64>("lineage_count"), 1);
        let row = client
            .query_one(
                "SELECT status
                 FROM profile_compatibility_status
                 WHERE profile_id = $1",
                &[&manifest.profile_id],
            )
            .await?;
        assert_eq!(row.get::<_, String>("status"), "valid");
        let row = client
            .query_one(
                "SELECT run.status, COUNT(run_case.id)::BIGINT AS case_count
                 FROM evaluation_runs AS run
                 LEFT JOIN evaluation_run_cases AS run_case
                   ON run_case.evaluation_run_id = run.id
                 WHERE run.id = $1
                 GROUP BY run.id",
                &[&evaluation_run_id],
            )
            .await?;
        assert_eq!(row.get::<_, String>("status"), "passed");
        assert_eq!(row.get::<_, i64>("case_count"), 1);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn import_and_crawl_runs_can_reference_profile_manifest_lineage() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("glr_ingest_lineage").await
    else {
        eprintln!(
            "skipping ingest lineage persistence test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        run_migrations(
            &database_url,
            repo_root().join("storage/migrations/postgres"),
        )
        .await?;
        let repository = PgRepository::new(&database_url);
        let manifest = profile_manifest_record();
        let lineage_id = repository.upsert_profile_manifest(&manifest).await?;
        let lineage = ingest_run_lineage_record(&manifest.profile_id, lineage_id);
        let source_manifest = SourceManifest {
            schema_version: 1,
            kind: SourceManifestKind::ImportSource,
            source_id: "event-archive".to_string(),
            source_name: "Event Archive".to_string(),
            manifest_version: 1,
            parser_version: Some("event-archive-v1".to_string()),
            description: None,
            files: vec![SourceFileSpec {
                logical_name: "events".to_string(),
                path: "events.csv".to_string(),
                format: "csv".to_string(),
            }],
        };
        let import_run_id = begin_import_run_with_lineage(
            &database_url,
            "/tmp/event-archive.yaml",
            &source_manifest,
            "event-archive-v1",
            Some(&lineage),
        )
        .await?;
        let crawl_manifest = SourceManifestAudit {
            manifest_path: "/tmp/crawler.yaml".to_string(),
            source_id: "event-crawl".to_string(),
            source_name: "Event Crawl".to_string(),
            manifest_version: 1,
            parser_version: "single_title_page_v1".to_string(),
            manifest_json: json!({
                "source_id": "event-crawl",
                "kind": "crawler_source"
            }),
        };
        let crawl_lineage = IngestRunLineageRecord {
            connector_type: Some("crawler_manifest".to_string()),
            source_class: Some("html_crawl".to_string()),
            manifest_kind: Some("crawler_source".to_string()),
            field_mapping: None,
            lineage_evidence: json!({
                "source_id": "event-crawl",
                "connector_manifest_path": "/tmp/crawler.yaml"
            }),
            ..lineage.clone()
        };
        let crawl_run_id = begin_crawl_run_with_lineage(
            &database_url,
            &crawl_manifest,
            "single_title_page_v1",
            Some(&crawl_lineage),
        )
        .await?;
        let legacy_source_manifest = SourceManifest {
            schema_version: 1,
            kind: SourceManifestKind::ImportSource,
            source_id: "event-csv".to_string(),
            source_name: "Event CSV".to_string(),
            manifest_version: 1,
            parser_version: Some("event-csv-v1".to_string()),
            description: None,
            files: vec![SourceFileSpec {
                logical_name: "events".to_string(),
                path: "events.csv".to_string(),
                format: "csv".to_string(),
            }],
        };
        let legacy_import_run_id = begin_import_run(
            &database_url,
            "/tmp/legacy-event-csv.yaml",
            &legacy_source_manifest,
            "event-csv-v1",
        )
        .await?;
        let legacy_crawl_manifest = SourceManifestAudit {
            manifest_path: "/tmp/legacy-crawler.yaml".to_string(),
            source_id: "legacy-event-crawl".to_string(),
            source_name: "Legacy Event Crawl".to_string(),
            manifest_version: 1,
            parser_version: "single_title_page_v1".to_string(),
            manifest_json: json!({
                "source_id": "legacy-event-crawl",
                "kind": "crawler_source"
            }),
        };
        let legacy_crawl_run_id = begin_crawl_run(
            &database_url,
            &legacy_crawl_manifest,
            "single_title_page_v1",
        )
        .await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let import_row = client
            .query_one(
                "SELECT profile_id,
                        profile_manifest_lineage_id,
                        connector_type,
                        source_class,
                        manifest_kind,
                        manifest_schema_version,
                        field_mapping,
                        lineage_evidence
                 FROM import_runs
                 WHERE id = $1",
                &[&import_run_id],
            )
            .await?;
        assert_eq!(
            import_row.get::<_, Option<String>>("profile_id"),
            Some(manifest.profile_id.clone())
        );
        assert_eq!(
            import_row.get::<_, Option<i64>>("profile_manifest_lineage_id"),
            Some(lineage_id)
        );
        assert_eq!(
            import_row.get::<_, Option<String>>("connector_type"),
            Some("archive_source".to_string())
        );
        assert_eq!(
            import_row.get::<_, Option<String>>("source_class"),
            Some("archive_import".to_string())
        );
        assert_eq!(
            import_row.get::<_, Option<String>>("manifest_kind"),
            Some("archive_source".to_string())
        );
        assert_eq!(
            import_row.get::<_, Option<i32>>("manifest_schema_version"),
            Some(1)
        );
        assert_eq!(
            import_row.get::<_, Option<String>>("field_mapping"),
            Some("event_v1".to_string())
        );
        assert_eq!(
            import_row
                .get::<_, serde_json::Value>("lineage_evidence")
                .get("source_id"),
            Some(&json!("event-archive"))
        );

        let crawl_row = client
            .query_one(
                "SELECT profile_id,
                        profile_manifest_lineage_id,
                        connector_type,
                        source_class,
                        manifest_kind,
                        manifest_schema_version,
                        field_mapping,
                        lineage_evidence
                 FROM crawl_runs
                 WHERE id = $1",
                &[&crawl_run_id],
            )
            .await?;
        assert_eq!(
            crawl_row.get::<_, Option<String>>("profile_id"),
            Some(manifest.profile_id.clone())
        );
        assert_eq!(
            crawl_row.get::<_, Option<i64>>("profile_manifest_lineage_id"),
            Some(lineage_id)
        );
        assert_eq!(
            crawl_row.get::<_, Option<String>>("connector_type"),
            Some("crawler_manifest".to_string())
        );
        assert_eq!(
            crawl_row.get::<_, Option<String>>("source_class"),
            Some("html_crawl".to_string())
        );
        assert_eq!(
            crawl_row.get::<_, Option<String>>("manifest_kind"),
            Some("crawler_source".to_string())
        );
        assert_eq!(
            crawl_row.get::<_, Option<i32>>("manifest_schema_version"),
            Some(1)
        );
        assert_eq!(crawl_row.get::<_, Option<String>>("field_mapping"), None);
        assert_eq!(
            crawl_row
                .get::<_, serde_json::Value>("lineage_evidence")
                .get("source_id"),
            Some(&json!("event-crawl"))
        );

        let legacy_import_row = client
            .query_one(
                "SELECT profile_id,
                        profile_manifest_lineage_id,
                        connector_type,
                        source_class,
                        manifest_kind,
                        manifest_schema_version,
                        field_mapping,
                        lineage_evidence
                 FROM import_runs
                 WHERE id = $1",
                &[&legacy_import_run_id],
            )
            .await?;
        assert_empty_run_lineage(&legacy_import_row);
        let legacy_crawl_row = client
            .query_one(
                "SELECT profile_id,
                        profile_manifest_lineage_id,
                        connector_type,
                        source_class,
                        manifest_kind,
                        manifest_schema_version,
                        field_mapping,
                        lineage_evidence
                 FROM crawl_runs
                 WHERE id = $1",
                &[&legacy_crawl_run_id],
            )
            .await?;
        assert_empty_run_lineage(&legacy_crawl_row);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

fn evaluation_run_record(profile_id: &str, lineage_id: i64) -> EvaluationRunRecord {
    EvaluationRunRecord {
        profile_id: Some(profile_id.to_string()),
        profile_manifest_lineage_id: Some(lineage_id),
        run_kind: EvaluationRunKind::Golden,
        scenario_source_kind: "profile_evaluation".to_string(),
        scenario_path: "configs/evaluation/scenarios".to_string(),
        pairwise_pack_path: None,
        algorithm_version: "test-algorithm".to_string(),
        status: EvaluationRunStatus::Passed,
        scenarios: 1,
        passed: 1,
        blocked: 0,
        blockers: 0,
        warnings: 0,
        summary_payload: json!({ "scenarios": 1, "passed": 1 }),
        cases: vec![EvaluationRunCaseRecord {
            case_id: "S01".to_string(),
            title: "Scenario".to_string(),
            path: "S01.yaml".to_string(),
            status: EvaluationRunCaseStatus::Passed,
            expected_fallback_stage: "strict_station".to_string(),
            actual_fallback_stage: Some("strict_station".to_string()),
            expected_order: vec!["school:school_a".to_string()],
            actual_order: vec!["school:school_a".to_string()],
            checks_payload: json!([]),
        }],
    }
}

fn profile_manifest_record() -> ProfileManifestRecord {
    ProfileManifestRecord {
        profile_id: "school-event-jp".to_string(),
        display_name: "School Event JP".to_string(),
        schema_version: 2,
        manifest_kind: "profile_pack".to_string(),
        manifest_version: 1,
        compatibility_level: "reference".to_string(),
        default_locale: Some("ja-JP".to_string()),
        description: None,
        manifest_path: "configs/profiles/school-event-jp/profile.yaml".to_string(),
        manifest_checksum_sha256:
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        manifest_payload: json!({ "profile_id": "school-event-jp" }),
        ranking_config_dir: "configs/ranking".to_string(),
        reason_catalog_path: "configs/profiles/school-event-jp/reasons.yaml".to_string(),
        content_kind_registry: vec!["school".to_string(), "event".to_string()],
        supported_content_kinds: vec!["school".to_string(), "event".to_string()],
        context_inputs: vec!["station".to_string(), "line".to_string()],
        placements: vec!["home".to_string(), "search".to_string()],
        fallback_policy: "school_event_jp_default".to_string(),
        fixture_count: 1,
        connector_count: 1,
        evaluation_reference_count: 1,
    }
}

fn ingest_run_lineage_record(profile_id: &str, lineage_id: i64) -> IngestRunLineageRecord {
    IngestRunLineageRecord {
        profile_id: Some(profile_id.to_string()),
        profile_manifest_lineage_id: Some(lineage_id),
        connector_type: Some("archive_source".to_string()),
        source_class: Some("archive_import".to_string()),
        manifest_kind: Some("archive_source".to_string()),
        manifest_schema_version: Some(1),
        field_mapping: Some("event_v1".to_string()),
        lineage_evidence: json!({
            "source_id": "event-archive",
            "connector_manifest_path": "/tmp/event-archive.yaml"
        }),
    }
}

fn assert_empty_run_lineage(row: &Row) {
    assert_eq!(row.get::<_, Option<String>>("profile_id"), None);
    assert_eq!(
        row.get::<_, Option<i64>>("profile_manifest_lineage_id"),
        None
    );
    assert_eq!(row.get::<_, Option<String>>("connector_type"), None);
    assert_eq!(row.get::<_, Option<String>>("source_class"), None);
    assert_eq!(row.get::<_, Option<String>>("manifest_kind"), None);
    assert_eq!(row.get::<_, Option<i32>>("manifest_schema_version"), None);
    assert_eq!(row.get::<_, Option<String>>("field_mapping"), None);
    assert_eq!(
        row.get::<_, serde_json::Value>("lineage_evidence"),
        json!({})
    );
}
