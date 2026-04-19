use std::{sync::Arc, time::Instant};

use api_contracts::{
    HealthResponse, ReadyResponse, RecommendationRequest, RecommendationResponse, TrackRequest,
    TrackResponse,
};
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use cache::RecommendationCache;
use config::CandidateRetrievalMode;
use observability::{cache_hit, cache_miss, cache_write, candidate_retrieval_completed};
use ranking::{RankingEngine, RankingError};
use storage::{JobType, NewJob, RecommendationRepository, RecommendationTrace};
use storage_opensearch::OpenSearchStore;
use storage_postgres::PgRepository;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use utoipa_swagger_ui::SwaggerUi;

#[derive(Clone)]
pub enum CandidateBackend {
    SqlOnly,
    Full(OpenSearchStore),
}

impl CandidateBackend {
    fn backend_name(&self) -> &'static str {
        match self {
            Self::SqlOnly => "postgresql",
            Self::Full(_) => "opensearch",
        }
    }
}

#[derive(Clone)]
struct TracePayloadInput<'a> {
    response_source: &'a str,
    mode: CandidateRetrievalMode,
    backend: &'a str,
    candidate_count: usize,
    duration_ms: u128,
    target_station_id: &'a str,
    candidate_limit: usize,
    neighbor_distance_cap_meters: f64,
}

#[derive(Clone)]
pub struct AppState {
    pub repository: Arc<PgRepository>,
    pub engine: RankingEngine,
    pub cache: RecommendationCache,
    pub profile_version: String,
    pub algorithm_version: String,
    pub candidate_retrieval_mode: CandidateRetrievalMode,
    pub candidate_retrieval_limit: usize,
    pub neighbor_distance_cap_meters: f64,
    pub candidate_backend: CandidateBackend,
    pub worker_max_attempts: i32,
}

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/recommendations", post(recommend))
        .route("/v1/track", post(track))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", openapi::api_doc()))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    match state.repository.ready_check().await {
        Ok(_) => (
            StatusCode::OK,
            Json(ReadyResponse {
                status: "ready".to_string(),
                database: "reachable".to_string(),
                cache: state.cache.status().await,
            }),
        ),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadyResponse {
                status: "not_ready".to_string(),
                database: error.to_string(),
                cache: state.cache.status().await,
            }),
        ),
    }
}

async fn recommend(
    State(state): State<AppState>,
    Json(request): Json<RecommendationRequest>,
) -> impl IntoResponse {
    let query: domain::RankingQuery = request.clone().into();
    let cache_key = if request.cacheable() {
        match state.cache.build_key(
            &state.profile_version,
            &state.algorithm_version,
            state.candidate_retrieval_mode.as_str(),
            &request,
        ) {
            Ok(key) => Some(key),
            Err(error) => {
                tracing::warn!(%error, "failed to build recommendation cache key");
                None
            }
        }
    } else {
        None
    };

    if let Some(cache_key) = cache_key.as_deref() {
        match state
            .cache
            .get_json::<RecommendationResponse>(cache_key)
            .await
        {
            Ok(Some(response)) => {
                cache_hit(cache_key);
                if let Err(error) = record_trace(
                    &state.repository,
                    &request,
                    &response,
                    build_trace_payload(TracePayloadInput {
                        response_source: "cache",
                        mode: state.candidate_retrieval_mode,
                        backend: state.candidate_backend.backend_name(),
                        candidate_count: 0,
                        duration_ms: 0,
                        target_station_id: &request.target_station_id,
                        candidate_limit: state.candidate_retrieval_limit,
                        neighbor_distance_cap_meters: state.neighbor_distance_cap_meters,
                    }),
                )
                .await
                {
                    return error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
                }
                return (StatusCode::OK, Json(response)).into_response();
            }
            Ok(None) => cache_miss(cache_key),
            Err(error) => tracing::warn!(cache_key, %error, "failed to read recommendation cache"),
        }
    }

    let target_station = match state
        .repository
        .load_station(&query.target_station_id)
        .await
    {
        Ok(Some(station)) => station,
        Ok(None) => {
            return error_response(StatusCode::BAD_REQUEST, query.target_station_id.clone());
        }
        Err(error) => {
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
        }
    };

    let retrieval_started = Instant::now();
    let candidate_links = match &state.candidate_backend {
        CandidateBackend::SqlOnly => match state
            .repository
            .load_candidate_links(
                &target_station,
                state.candidate_retrieval_limit,
                state.neighbor_distance_cap_meters,
            )
            .await
        {
            Ok(candidate_links) => candidate_links,
            Err(error) => {
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
            }
        },
        CandidateBackend::Full(store) => match store
            .search_candidate_links(
                &target_station,
                state.neighbor_distance_cap_meters,
                state.candidate_retrieval_limit,
            )
            .await
        {
            Ok(candidate_links) => candidate_links,
            Err(error) => {
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
            }
        },
    };
    let retrieval_duration_ms = retrieval_started.elapsed().as_millis();
    candidate_retrieval_completed(
        state.candidate_retrieval_mode.as_str(),
        state.candidate_backend.backend_name(),
        candidate_links.len(),
        retrieval_duration_ms,
    );

    let dataset = match state
        .repository
        .load_candidate_dataset(&query, &target_station, &candidate_links)
        .await
    {
        Ok(dataset) => dataset,
        Err(error) => {
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
        }
    };

    let result = match state.engine.recommend(&dataset, &query) {
        Ok(result) => result,
        Err(RankingError::UnknownStation(message)) => {
            return error_response(StatusCode::BAD_REQUEST, message);
        }
        Err(RankingError::NoCandidates(message)) => {
            return error_response(StatusCode::NOT_FOUND, message);
        }
    };

    let response: RecommendationResponse = result.into();
    if let Err(error) = record_trace(
        &state.repository,
        &request,
        &response,
        build_trace_payload(TracePayloadInput {
            response_source: "fresh",
            mode: state.candidate_retrieval_mode,
            backend: state.candidate_backend.backend_name(),
            candidate_count: candidate_links.len(),
            duration_ms: retrieval_duration_ms,
            target_station_id: &target_station.id,
            candidate_limit: state.candidate_retrieval_limit,
            neighbor_distance_cap_meters: state.neighbor_distance_cap_meters,
        }),
    )
    .await
    {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
    }

    if let Some(cache_key) = cache_key {
        if let Err(error) = state.cache.set_json(&cache_key, &response).await {
            tracing::warn!(cache_key, %error, "failed to write recommendation cache");
        } else {
            cache_write(&cache_key);
        }
    }

    (StatusCode::OK, Json(response)).into_response()
}

async fn track(
    State(state): State<AppState>,
    Json(request): Json<TrackRequest>,
) -> impl IntoResponse {
    if let Err(message) = request.validate() {
        return error_response(StatusCode::BAD_REQUEST, message);
    }

    let event = request.clone().into();
    let jobs = build_tracking_jobs(&state, &event);
    let queued_jobs = jobs
        .iter()
        .map(|job| job.job_type.as_str().to_string())
        .collect::<Vec<_>>();
    let event_id = match state
        .repository
        .record_user_event_with_jobs(&event, &jobs)
        .await
    {
        Ok(event_id) => event_id,
        Err(error) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    (
        StatusCode::ACCEPTED,
        Json(TrackResponse {
            status: "accepted".to_string(),
            event_id,
            queued_jobs,
        }),
    )
        .into_response()
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
        fallback_stage: response.fallback_stage.clone(),
        algorithm_version: response.algorithm_version.clone(),
    };
    repository.record_trace(&trace).await
}

fn build_tracking_jobs(state: &AppState, event: &domain::UserEvent) -> Vec<NewJob> {
    if !event.event_kind.is_school_affecting() || event.school_id.is_none() {
        return Vec::new();
    }

    let mut jobs = vec![
        NewJob {
            job_type: JobType::RefreshPopularitySnapshot,
            payload: serde_json::json!({}),
            max_attempts: state.worker_max_attempts,
        },
        NewJob {
            job_type: JobType::RefreshUserAffinitySnapshot,
            payload: serde_json::json!({ "user_id": event.user_id.clone() }),
            max_attempts: state.worker_max_attempts,
        },
    ];

    if state.cache.enabled() {
        jobs.push(NewJob {
            job_type: JobType::InvalidateRecommendationCache,
            payload: serde_json::json!({ "scope": "recommendations" }),
            max_attempts: state.worker_max_attempts,
        });
    }

    if state.candidate_retrieval_mode.is_full() {
        jobs.push(NewJob {
            job_type: JobType::SyncCandidateProjection,
            payload: serde_json::json!({ "scope": "full_rebuild" }),
            max_attempts: state.worker_max_attempts,
        });
    }

    jobs
}

fn error_response(status: StatusCode, message: String) -> axum::response::Response {
    (
        status,
        Json(serde_json::json!({
            "error": message,
        })),
    )
        .into_response()
}

fn build_trace_payload(input: TracePayloadInput<'_>) -> serde_json::Value {
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
