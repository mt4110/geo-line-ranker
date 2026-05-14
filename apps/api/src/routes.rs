use std::time::Instant;

use api_contracts::{
    ContextResolveRequest, ContextResolveResponse, HealthResponse, ReadyResponse,
    RecommendationRequest, RecommendationResponse, TrackRequest, TrackResponse,
};
use axum::{
    extract::{rejection::JsonRejection, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use observability::{cache_hit, cache_miss, cache_write, candidate_retrieval_completed};
use ranking::RankingError;
use storage::RecommendationRepository;
use storage_postgres::{
    is_foreign_key_violation, user_event_reference_validation_message, ContextCandidateLinkQuery,
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    cache_key::build_recommendation_cache_key,
    candidate_backend::{actual_candidate_backend_name, should_use_opensearch_candidate_retrieval},
    errors::{context_resolution_error_message, context_resolution_error_status, error_response},
    request_id::resolve_request_id,
    trace::{build_trace_payload, record_trace_best_effort, TracePayloadInput},
    trace_graph::{
        build_candidate_plan_graph_diagnostics_for_trace, candidate_graph_expansion_from_storage,
        load_candidate_graph_expansion_for_plan,
    },
    tracking::build_tracking_jobs,
    AppState, CandidateBackend,
};

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/context/resolve", post(context_resolve))
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
    let cache = state.cache.status().await;
    let (database_result, opensearch_result) = tokio::join!(
        state.repository.ready_check(),
        state.candidate_backend.ready_check()
    );
    let database_ready = database_result.is_ok();
    let opensearch_ready = opensearch_result.is_ok();

    let database = match database_result {
        Ok(_) => "reachable".to_string(),
        Err(error) => error.to_string(),
    };
    let opensearch = match opensearch_result {
        Ok(status) => status,
        Err(error) => error.to_string(),
    };
    let is_ready = database_ready && opensearch_ready;

    let status_code = if is_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let status = if is_ready { "ready" } else { "not_ready" };

    (
        status_code,
        Json(ReadyResponse {
            status: status.to_string(),
            database,
            cache,
            opensearch,
        }),
    )
}

async fn context_resolve(
    State(state): State<AppState>,
    request: Result<Json<ContextResolveRequest>, JsonRejection>,
) -> impl IntoResponse {
    let request = match request {
        Ok(Json(request)) => request,
        Err(rejection) => {
            return error_response(StatusCode::BAD_REQUEST, rejection.body_text());
        }
    };

    let request_id = match resolve_request_id(request.request_id.as_deref()) {
        Ok(request_id) => request_id,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message.to_string()),
    };

    let context_input = request.context_input();
    let resolved_context = match state
        .repository
        .resolve_context_read_only(&request_id, request.user_id.as_deref(), &context_input)
        .await
    {
        Ok(context) => context,
        Err(error) => {
            let status = context_resolution_error_status(&error);
            let message = context_resolution_error_message(&error, &context_input);
            return error_response(status, message);
        }
    };

    (
        StatusCode::OK,
        Json(ContextResolveResponse::from_context(
            request_id,
            resolved_context,
        )),
    )
        .into_response()
}

async fn recommend(
    State(state): State<AppState>,
    request: Result<Json<RecommendationRequest>, JsonRejection>,
) -> impl IntoResponse {
    let request = match request {
        Ok(Json(request)) => request,
        Err(rejection) => {
            return error_response(StatusCode::BAD_REQUEST, rejection.body_text());
        }
    };

    let request_id = match resolve_request_id(request.request_id.as_deref()) {
        Ok(request_id) => request_id,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message.to_string()),
    };

    let context_input = request.context_input();
    let resolved_context = match state
        .repository
        .resolve_context(&request_id, request.user_id.as_deref(), &context_input)
        .await
    {
        Ok(context) => context,
        Err(error) => {
            let status = context_resolution_error_status(&error);
            let message = context_resolution_error_message(&error, &context_input);
            return error_response(status, message);
        }
    };
    let target_station = match state
        .repository
        .load_station_for_context(&resolved_context)
        .await
    {
        Ok(Some(station)) => station,
        Ok(None) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "context could not be mapped to a station".to_string(),
            );
        }
        Err(error) => {
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
        }
    };
    let query = request.with_resolved_context(target_station.id.clone(), resolved_context.clone());
    let cache_key = if request.cacheable() {
        match build_recommendation_cache_key(&state, &query) {
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
            Ok(Some(mut response)) => {
                let actual_candidate_backend =
                    actual_candidate_backend_name(&state.candidate_backend, &resolved_context);
                response.request_id = Some(request_id.clone());
                cache_hit(cache_key);
                let graph_diagnostics = build_candidate_plan_graph_diagnostics_for_trace(
                    &state.repository,
                    &resolved_context,
                    &target_station,
                    response.candidate_plan_trace.as_ref(),
                )
                .await;
                record_trace_best_effort(
                    &state.repository,
                    &request,
                    &response,
                    "cache",
                    graph_diagnostics.as_ref(),
                    build_trace_payload(TracePayloadInput {
                        response_source: "cache",
                        context: &resolved_context,
                        mode: state.candidate_retrieval_mode,
                        backend: actual_candidate_backend,
                        candidate_count: 0,
                        duration_ms: 0,
                        candidate_plan_trace: response.candidate_plan_trace.as_ref(),
                        target_station_id: &target_station.id,
                        candidate_limit: state.candidate_retrieval_limit,
                        neighbor_distance_cap_meters: state.neighbor_distance_cap_meters,
                    }),
                )
                .await;
                return (StatusCode::OK, Json(response)).into_response();
            }
            Ok(None) => cache_miss(cache_key),
            Err(error) => tracing::warn!(cache_key, %error, "failed to read recommendation cache"),
        }
    }

    let retrieval_started = Instant::now();
    let neighbor_max_hops = state.engine.neighbor_max_hops(query.placement);
    let min_candidate_count = state.engine.minimum_candidate_count();
    let actual_candidate_backend =
        actual_candidate_backend_name(&state.candidate_backend, &resolved_context);
    let storage_graph_expansion = load_candidate_graph_expansion_for_plan(
        &state.repository,
        &resolved_context,
        &target_station,
    )
    .await;
    let candidate_link_query = ContextCandidateLinkQuery {
        target_station: &target_station,
        context: &resolved_context,
        candidate_limit: state.candidate_retrieval_limit,
        min_scoped_candidates: min_candidate_count,
        neighbor_distance_cap_meters: state.neighbor_distance_cap_meters,
        neighbor_max_hops,
    };
    let candidate_links = match &state.candidate_backend {
        CandidateBackend::SqlOnly => match state
            .repository
            .load_context_candidate_links_with_loaded_graph_expansion(
                candidate_link_query,
                &storage_graph_expansion,
            )
            .await
        {
            Ok(candidate_links) => candidate_links,
            Err(error) => {
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
            }
        },
        CandidateBackend::Full(store)
            if should_use_opensearch_candidate_retrieval(&resolved_context) =>
        {
            match store
                .search_candidate_links(
                    &target_station,
                    state.neighbor_distance_cap_meters,
                    state.candidate_retrieval_limit,
                )
                .await
            {
                Ok(candidate_links) if candidate_links.len() < min_candidate_count => match state
                    .repository
                    .load_context_candidate_links_with_loaded_graph_expansion(
                        candidate_link_query,
                        &storage_graph_expansion,
                    )
                    .await
                {
                    Ok(candidate_links) => candidate_links,
                    Err(error) => {
                        return error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            error.to_string(),
                        );
                    }
                },
                Ok(candidate_links) => candidate_links,
                Err(error) => {
                    return error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
                }
            }
        }
        CandidateBackend::Full(_) => match state
            .repository
            .load_context_candidate_links_with_loaded_graph_expansion(
                candidate_link_query,
                &storage_graph_expansion,
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
        actual_candidate_backend,
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
    let graph_expansion = candidate_graph_expansion_from_storage(storage_graph_expansion);

    let result =
        match state
            .engine
            .recommend_with_graph_expansion(&dataset, &query, &graph_expansion)
        {
            Ok(result) => result,
            Err(RankingError::UnknownStation(message)) => {
                return error_response(StatusCode::BAD_REQUEST, message);
            }
            Err(RankingError::NoCandidates(message)) => {
                return error_response(StatusCode::NOT_FOUND, message);
            }
        };

    let mut response: RecommendationResponse = result.into();
    response.request_id = Some(request_id.clone());
    let graph_diagnostics = build_candidate_plan_graph_diagnostics_for_trace(
        &state.repository,
        &resolved_context,
        &target_station,
        response.candidate_plan_trace.as_ref(),
    )
    .await;
    record_trace_best_effort(
        &state.repository,
        &request,
        &response,
        "fresh",
        graph_diagnostics.as_ref(),
        build_trace_payload(TracePayloadInput {
            response_source: "fresh",
            context: &resolved_context,
            mode: state.candidate_retrieval_mode,
            backend: actual_candidate_backend,
            candidate_count: candidate_links.len(),
            duration_ms: retrieval_duration_ms,
            candidate_plan_trace: response.candidate_plan_trace.as_ref(),
            target_station_id: &target_station.id,
            candidate_limit: state.candidate_retrieval_limit,
            neighbor_distance_cap_meters: state.neighbor_distance_cap_meters,
        }),
    )
    .await;

    if let Some(cache_key) = cache_key {
        let mut cached_response = response.clone();
        cached_response.request_id = None;
        if let Err(error) = state.cache.set_json(&cache_key, &cached_response).await {
            tracing::warn!(cache_key, %error, "failed to write recommendation cache");
        } else {
            cache_write(&cache_key);
        }
    }

    (StatusCode::OK, Json(response)).into_response()
}

async fn track(
    State(state): State<AppState>,
    request: Result<Json<TrackRequest>, JsonRejection>,
) -> impl IntoResponse {
    let request = match request {
        Ok(Json(request)) => request,
        Err(rejection) => {
            return error_response(StatusCode::BAD_REQUEST, rejection.body_text());
        }
    };

    const UNKNOWN_TRACK_REFERENCE_MESSAGE: &str =
        "track payload references unknown school_id, event_id, or target_station_id";

    if let Err(message) = request.validate() {
        return error_response(StatusCode::BAD_REQUEST, message);
    }

    let mut event: domain::UserEvent = request.clone().into();
    if let Some(event_id) = event.event_id.clone() {
        let event_school_id = match state.repository.load_event_school_id(&event_id).await {
            Ok(Some(event_school_id)) => event_school_id,
            Ok(None) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    UNKNOWN_TRACK_REFERENCE_MESSAGE.to_string(),
                );
            }
            Err(error) => {
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
            }
        };

        match event.school_id.as_deref() {
            Some(school_id) if school_id != event_school_id => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "event_id {event_id} belongs to school_id {event_school_id}, not {school_id}"
                    ),
                );
            }
            None => event.school_id = Some(event_school_id),
            _ => {}
        }
    }

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
        Err(error) if is_foreign_key_violation(&error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                UNKNOWN_TRACK_REFERENCE_MESSAGE.to_string(),
            );
        }
        Err(error) => {
            if let Some(message) = user_event_reference_validation_message(&error) {
                return error_response(StatusCode::BAD_REQUEST, message);
            }
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
        }
    };

    (
        StatusCode::ACCEPTED,
        Json(TrackResponse {
            status: "accepted".to_string(),
            event_id: event_id.to_string(),
            queued_jobs,
        }),
    )
        .into_response()
}
