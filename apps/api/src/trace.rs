use api_contracts::{RecommendationRequest, RecommendationResponse};
use config::CandidateRetrievalMode;
use storage::{RecommendationRepository, RecommendationTrace};
use storage_postgres::PgRepository;

#[derive(Clone)]
pub(crate) struct TracePayloadInput<'a> {
    pub(crate) response_source: &'a str,
    pub(crate) mode: CandidateRetrievalMode,
    pub(crate) backend: &'a str,
    pub(crate) candidate_count: usize,
    pub(crate) duration_ms: u128,
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
    serde_json::json!({
        "response_source": input.response_source,
        "candidate_retrieval": {
            "mode": input.mode.as_str(),
            "backend": input.backend,
            "candidate_count": input.candidate_count,
            "duration_ms": input.duration_ms,
            "target_station_id": input.target_station_id,
            "candidate_limit": input.candidate_limit,
            "neighbor_distance_cap_meters": input.neighbor_distance_cap_meters
        }
    })
}
