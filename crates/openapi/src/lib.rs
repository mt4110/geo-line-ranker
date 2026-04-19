use api_contracts::{
    HealthResponse, ReadyResponse, RecommendationRequest, RecommendationResponse, TrackRequest,
    TrackResponse,
};
use utoipa::OpenApi;

#[utoipa::path(
    get,
    path = "/healthz",
    responses(
        (status = 200, description = "liveness probe", body = HealthResponse)
    ),
    tag = "system"
)]
#[allow(dead_code)]
fn healthz_doc() {}

#[utoipa::path(
    get,
    path = "/readyz",
    responses(
        (status = 200, description = "readiness probe", body = ReadyResponse)
    ),
    tag = "system"
)]
#[allow(dead_code)]
fn readyz_doc() {}

#[utoipa::path(
    post,
    path = "/v1/recommendations",
    request_body = RecommendationRequest,
    responses(
        (status = 200, description = "deterministic recommendations", body = RecommendationResponse)
    ),
    tag = "recommendations"
)]
#[allow(dead_code)]
fn recommend_doc() {}

#[utoipa::path(
    post,
    path = "/v1/track",
    request_body = TrackRequest,
    responses(
        (status = 202, description = "accepted tracking event", body = TrackResponse)
    ),
    tag = "tracking"
)]
#[allow(dead_code)]
fn track_doc() {}

#[derive(OpenApi)]
#[openapi(
    paths(healthz_doc, readyz_doc, recommend_doc, track_doc),
    components(
        schemas(
            HealthResponse,
            ReadyResponse,
            RecommendationRequest,
            RecommendationResponse,
            TrackRequest,
            TrackResponse
        )
    ),
    tags(
        (name = "system", description = "System status endpoints"),
        (name = "recommendations", description = "Deterministic recommendation endpoints"),
        (name = "tracking", description = "Append-only user event tracking")
    )
)]
pub struct ApiDoc;

pub fn api_doc() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}
