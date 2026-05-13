use anyhow::{Context, Result};
use api_contracts::{CandidatePlanTraceDto, RecommendationRequest, RecommendationResponse};
use config::CandidateRetrievalMode;
use serde_json::Value;
use storage::{
    RecommendationRepository, RecommendationTrace, RecommendationTraceCandidatePlanStage,
    RecommendationTraceCandidatePlanTrace, RecommendationTraceContextEvidenceSummary,
};
use storage_postgres::PgRepository;

#[derive(Clone)]
pub(crate) struct TracePayloadInput<'a> {
    pub(crate) response_source: &'a str,
    pub(crate) context: &'a context::RankingContext,
    pub(crate) mode: CandidateRetrievalMode,
    pub(crate) backend: &'a str,
    pub(crate) candidate_count: usize,
    pub(crate) duration_ms: u128,
    pub(crate) candidate_plan_trace: Option<&'a CandidatePlanTraceDto>,
    pub(crate) target_station_id: &'a str,
    pub(crate) candidate_limit: usize,
    pub(crate) neighbor_distance_cap_meters: f64,
}

async fn record_trace(
    repository: &PgRepository,
    request: &RecommendationRequest,
    response: &RecommendationResponse,
    trace_payload: serde_json::Value,
) -> Result<()> {
    let candidate_plan_trace = match build_candidate_plan_trace_record(response) {
        Ok(candidate_plan_trace) => candidate_plan_trace,
        Err(error) => {
            tracing::warn!(%error, "failed to build candidate plan trace detail rows");
            None
        }
    };
    let trace = RecommendationTrace {
        request_payload: serde_json::to_value(request).unwrap_or_default(),
        response_payload: serde_json::to_value(response).unwrap_or_default(),
        context_evidence_summary: build_context_evidence_summary_record(&trace_payload),
        candidate_plan_trace,
        trace_payload,
        fallback_stage: response.fallback_stage.as_str().to_string(),
        algorithm_version: response.algorithm_version.clone(),
    };
    repository.record_trace(&trace).await
}

fn build_context_evidence_summary_record(
    trace_payload: &Value,
) -> Option<RecommendationTraceContextEvidenceSummary> {
    let context = trace_payload.get("context")?;
    let evidence_summary = context.get("evidence_summary")?;
    Some(RecommendationTraceContextEvidenceSummary {
        context_source: string_field(context, "context_source")?,
        confidence: f64_field(context, "confidence")?,
        privacy_level: string_field(context, "privacy_level")?,
        primary_kind: string_field(evidence_summary, "primary_kind")?,
        evidence_count: i64_field(evidence_summary, "evidence_count")?,
        strongest_strength: f64_field(evidence_summary, "strongest_strength")?,
        has_search_execute: bool_field(evidence_summary, "has_search_execute")?,
        warning_count: i64_field(context, "warning_count")?,
        evidence_payload: evidence_summary.clone(),
    })
}

fn build_candidate_plan_trace_record(
    response: &RecommendationResponse,
) -> Result<Option<RecommendationTraceCandidatePlanTrace>> {
    response
        .candidate_plan_trace
        .as_ref()
        .map(|trace| {
            let plan_payload = serde_json::to_value(trace)
                .context("failed to serialize candidate plan trace for storage")?;
            let stages = trace
                .stages
                .iter()
                .enumerate()
                .map(|(index, stage)| {
                    let stage_payload = serde_json::to_value(stage)
                        .context("failed to serialize candidate plan stage for storage")?;
                    Ok(RecommendationTraceCandidatePlanStage {
                        stage_order: i32::try_from(index)
                            .context("candidate plan stage index exceeds i32")?,
                        stage: stage.stage.as_str().to_string(),
                        candidate_count: i64::try_from(stage.candidate_count)
                            .context("candidate_count exceeds i64")?,
                        required_min_candidates: i64::try_from(stage.required_min_candidates)
                            .context("required_min_candidates exceeds i64")?,
                        status: stage.status.as_str().to_string(),
                        reason_code: stage.reason_code.clone(),
                        stage_payload,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(RecommendationTraceCandidatePlanTrace {
                minimum_candidate_count: i64::try_from(trace.minimum_candidate_count)
                    .context("minimum_candidate_count exceeds i64")?,
                selected_stage: trace.selected_stage.as_str().to_string(),
                stop_reason: trace.stop_reason.clone(),
                area_context_usable: trace.area_context_usable,
                plan_payload,
                stages,
            })
        })
        .transpose()
}

pub(crate) async fn record_trace_best_effort(
    repository: &PgRepository,
    request: &RecommendationRequest,
    response: &RecommendationResponse,
    response_source: &'static str,
    trace_payload: serde_json::Value,
) {
    if let Err(error) = record_trace(repository, request, response, trace_payload).await {
        tracing::warn!(response_source, %error, "failed to persist recommendation trace");
    }
}

pub(crate) fn build_trace_payload(input: TracePayloadInput<'_>) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "response_source": input.response_source,
        "context": {
            "context_source": input.context.context_source.as_str(),
            "confidence": input.context.confidence,
            "privacy_level": input.context.privacy_level.as_str(),
            "evidence_summary": input.context.evidence_summary(),
            "warning_count": input.context.warnings.len()
        },
        "candidate_retrieval": {
            "mode": input.mode.as_str(),
            "backend": input.backend,
            "candidate_count": input.candidate_count,
            "duration_ms": input.duration_ms,
            "target_station_id": input.target_station_id,
            "candidate_limit": input.candidate_limit,
            "neighbor_distance_cap_meters": input.neighbor_distance_cap_meters
        }
    });

    if let Some(candidate_plan_trace) = input.candidate_plan_trace {
        match serde_json::to_value(candidate_plan_trace) {
            Ok(candidate_plan_trace_value) => {
                payload["candidate_plan_trace"] = candidate_plan_trace_value;
            }
            Err(error) => {
                tracing::warn!(%error, "failed to serialize candidate_plan_trace");
            }
        }
    }

    payload
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_string)
}

fn bool_field(value: &Value, key: &str) -> Option<bool> {
    value.get(key).and_then(Value::as_bool)
}

fn i64_field(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
    })
}

fn f64_field(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(Value::as_f64)
}

#[cfg(test)]
mod tests {
    use api_contracts::{
        CandidatePlanStageStatusDto, CandidatePlanStageTraceDto, CandidatePlanTraceDto,
        FallbackStageDto, RecommendationResponse,
    };
    use config::CandidateRetrievalMode;
    use std::collections::BTreeMap;

    use super::{
        build_candidate_plan_trace_record, build_context_evidence_summary_record,
        build_trace_payload, TracePayloadInput,
    };

    #[test]
    fn trace_payload_includes_context_evidence_summary() {
        let mut context = context::RankingContext::default_safe();
        context.context_source = context::ContextSource::RecentSearchContext;
        context.confidence = 0.88;

        let payload = build_trace_payload(TracePayloadInput {
            response_source: "fresh",
            context: &context,
            mode: CandidateRetrievalMode::SqlOnly,
            backend: "postgres",
            candidate_count: 3,
            duration_ms: 12,
            candidate_plan_trace: None,
            target_station_id: "st_tamachi",
            candidate_limit: 256,
            neighbor_distance_cap_meters: 5_000.0,
        });

        assert_eq!(
            payload["context"]["evidence_summary"]["primary_kind"],
            "search_execute"
        );
        assert_eq!(
            payload["context"]["evidence_summary"]["has_search_execute"],
            true
        );
    }

    #[test]
    fn trace_payload_includes_candidate_plan_trace_when_available() {
        let context = context::RankingContext::default_safe();
        let candidate_plan_trace = CandidatePlanTraceDto {
            minimum_candidate_count: 3,
            selected_stage: FallbackStageDto::SafeGlobalPopular,
            stop_reason: "sufficient_safe_global_candidates".to_string(),
            area_context_usable: false,
            stages: vec![CandidatePlanStageTraceDto {
                stage: FallbackStageDto::SafeGlobalPopular,
                candidate_count: 3,
                required_min_candidates: 3,
                status: CandidatePlanStageStatusDto::Selected,
                reason_code: "selected_sufficient_safe_global_candidates".to_string(),
            }],
        };

        let payload = build_trace_payload(TracePayloadInput {
            response_source: "fresh",
            context: &context,
            mode: CandidateRetrievalMode::SqlOnly,
            backend: "postgres",
            candidate_count: 3,
            duration_ms: 12,
            candidate_plan_trace: Some(&candidate_plan_trace),
            target_station_id: "st_tamachi",
            candidate_limit: 256,
            neighbor_distance_cap_meters: 5_000.0,
        });

        assert_eq!(
            payload["candidate_plan_trace"]["selected_stage"],
            "safe_global_popular"
        );
        assert_eq!(
            payload["candidate_plan_trace"]["stages"][0]["status"],
            "selected"
        );
    }

    #[test]
    fn trace_payload_context_maps_to_storage_summary_record() {
        let mut context = context::RankingContext::default_safe();
        context.context_source = context::ContextSource::RecentSearchContext;
        context.confidence = 0.88;

        let payload = build_trace_payload(TracePayloadInput {
            response_source: "fresh",
            context: &context,
            mode: CandidateRetrievalMode::SqlOnly,
            backend: "postgres",
            candidate_count: 3,
            duration_ms: 12,
            candidate_plan_trace: None,
            target_station_id: "st_tamachi",
            candidate_limit: 256,
            neighbor_distance_cap_meters: 5_000.0,
        });
        let summary = build_context_evidence_summary_record(&payload).expect("summary record");

        assert_eq!(summary.context_source, "recent_search_context");
        assert_eq!(summary.primary_kind, "search_execute");
        assert_eq!(summary.evidence_count, 1);
        assert!(summary.has_search_execute);
    }

    #[test]
    fn response_candidate_plan_maps_to_storage_trace_record() {
        let response = RecommendationResponse {
            request_id: Some("req-1".to_string()),
            items: Vec::new(),
            explanation: "test".to_string(),
            score_breakdown: Vec::new(),
            fallback_stage: FallbackStageDto::SameLine,
            candidate_counts: BTreeMap::new(),
            candidate_plan_trace: Some(CandidatePlanTraceDto {
                minimum_candidate_count: 3,
                selected_stage: FallbackStageDto::SameLine,
                stop_reason: "sufficient_scoped_candidates".to_string(),
                area_context_usable: true,
                stages: vec![CandidatePlanStageTraceDto {
                    stage: FallbackStageDto::SameLine,
                    candidate_count: 4,
                    required_min_candidates: 3,
                    status: CandidatePlanStageStatusDto::Selected,
                    reason_code: "selected_sufficient_scoped_candidates".to_string(),
                }],
            }),
            context: None,
            profile_version: "test".to_string(),
            algorithm_version: "test".to_string(),
        };
        let trace = build_candidate_plan_trace_record(&response)
            .expect("candidate plan conversion")
            .expect("candidate plan record");

        assert_eq!(trace.selected_stage, "same_line");
        assert_eq!(trace.stages.len(), 1);
        assert_eq!(trace.stages[0].status, "selected");
        assert_eq!(trace.stages[0].stage_order, 0);
    }
}
