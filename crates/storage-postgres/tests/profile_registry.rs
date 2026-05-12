use serde_json::json;
use storage::{
    EvaluationRunCaseRecord, EvaluationRunCaseStatus, EvaluationRunKind, EvaluationRunRecord,
    EvaluationRunStatus, ProfileCompatibilityStatus, ProfileCompatibilityStatusRecord,
    ProfileManifestRecord, ProfileRegistryRepository,
};
use storage_postgres::{run_migrations, PgRepository};
use tokio_postgres::NoTls;

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
            .record_evaluation_run(&EvaluationRunRecord {
                profile_id: Some(manifest.profile_id.clone()),
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
            })
            .await?;

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
