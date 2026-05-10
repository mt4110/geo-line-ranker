use api_contracts::{CandidatePlanTraceDto, RecommendationRequest, RecommendationResponse};
use config::CandidateRetrievalMode;
use storage::{RecommendationRepository, RecommendationTrace};
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
) -> anyhow::Result<()> {
    let trace = RecommendationTrace {
        request_payload: serde_json::to_value(request).unwrap_or_default(),
        response_payload: serde_json::to_value(response).unwrap_or_default(),
        trace_payload,
        fallback_stage: response.fallback_stage.as_str().to_string(),
        algorithm_version: response.algorithm_version.clone(),
    };
    repository.record_trace(&trace).await
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
        payload["candidate_plan_trace"] =
            serde_json::to_value(candidate_plan_trace).unwrap_or_default();
    }

    payload
}

#[cfg(test)]
mod tests {
    use api_contracts::{
        CandidatePlanStageStatusDto, CandidatePlanStageTraceDto, CandidatePlanTraceDto,
        FallbackStageDto,
    };
    use config::CandidateRetrievalMode;

    use super::{build_trace_payload, TracePayloadInput};

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
}
