use serde_json::json;
use storage::{
    RecommendationRepository, RecommendationTrace, RecommendationTraceCandidatePlanStage,
    RecommendationTraceCandidatePlanTrace, RecommendationTraceContextEvidenceSummary,
};
use storage_postgres::{run_migrations, PgRepository};
use tokio_postgres::NoTls;

mod common;

use common::{create_empty_database, drop_database, repo_root};

#[tokio::test]
async fn load_recommendation_trace_reads_full_persisted_payload() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("glr_recommendation_trace").await
    else {
        eprintln!(
            "skipping recommendation trace read test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        run_migrations(
            &database_url,
            repo_root().join("storage/migrations/postgres"),
        )
        .await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let request_payload = json!({
            "request_id": "req-trace-read",
            "target_station_id": "st_tamachi",
            "placement": "search",
            "debug": false
        });
        let response_payload = json!({
            "request_id": "req-trace-read",
            "items": [],
            "fallback_stage": "same_line",
            "candidate_counts": {},
            "profile_version": "test",
            "algorithm_version": "test",
            "score_breakdown": [],
            "explanation": "test"
        });
        let trace_payload = json!({
            "response_source": "fresh",
            "candidate_retrieval": {
                "mode": "sql_only",
                "backend": "postgres",
                "candidate_count": 7
            }
        });
        let row = client
            .query_one(
                "INSERT INTO recommendation_traces (
                    request_payload,
                    response_payload,
                    trace_payload,
                    fallback_stage,
                    algorithm_version
                ) VALUES ($1, $2, $3, $4, $5)
                RETURNING id",
                &[
                    &request_payload,
                    &response_payload,
                    &trace_payload,
                    &"same_line",
                    &"trace-read-test",
                ],
            )
            .await?;
        let trace_id = row.get::<_, i64>("id");

        let repository = PgRepository::new(&database_url);
        let trace = repository
            .load_recommendation_trace(trace_id)
            .await?
            .expect("inserted recommendation trace");

        assert_eq!(trace.id, trace_id);
        assert_eq!(trace.request_payload, request_payload);
        assert_eq!(trace.response_payload, response_payload);
        assert_eq!(trace.trace_payload, trace_payload);
        assert_eq!(trace.fallback_stage, "same_line");
        assert_eq!(trace.algorithm_version, "trace-read-test");
        assert!(trace.created_at.ends_with('Z'));
        assert!(trace.context_evidence_summary.is_none());
        assert!(trace.candidate_plan_trace.is_none());
        assert!(repository
            .load_recommendation_trace(trace_id + 1)
            .await?
            .is_none());

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn record_trace_persists_context_evidence_and_candidate_plan_rows() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("glr_recommendation_trace_details").await
    else {
        eprintln!(
            "skipping recommendation trace detail persistence test because PostgreSQL admin access is unavailable"
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
        let trace = detailed_recommendation_trace("req-trace-detail");

        repository.record_trace(&trace).await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(
                "SELECT id
                 FROM recommendation_traces
                 WHERE request_payload->>'request_id' = 'req-trace-detail'",
                &[],
            )
            .await?;
        let trace_id = row.get::<_, i64>("id");

        let loaded = repository
            .load_recommendation_trace(trace_id)
            .await?
            .expect("persisted recommendation trace");
        let context_evidence = loaded
            .context_evidence_summary
            .expect("context evidence summary row");
        let candidate_plan = loaded.candidate_plan_trace.expect("candidate plan row");

        assert_eq!(context_evidence.primary_kind, "search_execute");
        assert!(context_evidence.has_search_execute);
        assert_eq!(candidate_plan.selected_stage, "same_line");
        assert_eq!(candidate_plan.stages.len(), 2);
        assert_eq!(candidate_plan.stages[1].status, "selected");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn record_trace_keeps_parent_payload_when_detail_tables_are_unavailable() -> anyhow::Result<()>
{
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("glr_trace_detail_fail").await
    else {
        eprintln!(
            "skipping recommendation trace detail fallback test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        run_migrations(
            &database_url,
            repo_root().join("storage/migrations/postgres"),
        )
        .await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .simple_query(
                "DROP TABLE recommendation_trace_context_evidence;
                 DROP TABLE recommendation_trace_candidate_plan_stages;
                 DROP TABLE recommendation_trace_candidate_plans;",
            )
            .await?;

        let repository = PgRepository::new(&database_url);
        let trace = detailed_recommendation_trace("req-detail-table-missing");

        repository.record_trace(&trace).await?;

        let row = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM recommendation_traces
                 WHERE request_payload->>'request_id' = 'req-detail-table-missing'",
                &[],
            )
            .await?;
        assert_eq!(row.get::<_, i64>("count"), 1);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

fn detailed_recommendation_trace(request_id: &str) -> RecommendationTrace {
    RecommendationTrace {
        request_payload: json!({
            "request_id": request_id,
            "target_station_id": "st_tamachi",
            "placement": "search",
            "debug": false
        }),
        response_payload: json!({
            "request_id": request_id,
            "items": [],
            "fallback_stage": "same_line",
            "candidate_counts": {},
            "profile_version": "test",
            "algorithm_version": "test",
            "score_breakdown": [],
            "explanation": "test"
        }),
        trace_payload: json!({
            "response_source": "fresh",
            "context": {
                "context_source": "recent_search_context",
                "confidence": 0.75,
                "privacy_level": "station_level",
                "evidence_summary": {
                    "primary_kind": "search_execute",
                    "evidence_count": 1,
                    "strongest_strength": 0.75,
                    "has_search_execute": true
                },
                "warning_count": 0
            },
            "candidate_retrieval": {
                "mode": "sql_only",
                "backend": "postgres",
                "candidate_count": 7
            }
        }),
        fallback_stage: "same_line".to_string(),
        algorithm_version: "trace-detail-test".to_string(),
        context_evidence_summary: Some(RecommendationTraceContextEvidenceSummary {
            context_source: "recent_search_context".to_string(),
            confidence: 0.75,
            privacy_level: "station_level".to_string(),
            primary_kind: "search_execute".to_string(),
            evidence_count: 1,
            strongest_strength: 0.75,
            has_search_execute: true,
            warning_count: 0,
            evidence_payload: json!({
                "primary_kind": "search_execute",
                "evidence_count": 1,
                "strongest_strength": 0.75,
                "has_search_execute": true
            }),
        }),
        candidate_plan_trace: Some(RecommendationTraceCandidatePlanTrace {
            minimum_candidate_count: 3,
            selected_stage: "same_line".to_string(),
            stop_reason: "sufficient_scoped_candidates".to_string(),
            area_context_usable: true,
            plan_payload: json!({ "selected_stage": "same_line" }),
            stages: vec![
                RecommendationTraceCandidatePlanStage {
                    stage_order: 0,
                    stage: "strict_station".to_string(),
                    candidate_count: 1,
                    required_min_candidates: 3,
                    status: "insufficient".to_string(),
                    reason_code: "candidate_count_below_minimum".to_string(),
                    stage_payload: json!({ "stage": "strict_station" }),
                },
                RecommendationTraceCandidatePlanStage {
                    stage_order: 1,
                    stage: "same_line".to_string(),
                    candidate_count: 4,
                    required_min_candidates: 3,
                    status: "selected".to_string(),
                    reason_code: "selected_sufficient_scoped_candidates".to_string(),
                    stage_payload: json!({ "stage": "same_line" }),
                },
            ],
        }),
    }
}
