use std::sync::Arc;

use cache::RecommendationCache;
use config::CandidateRetrievalMode;
use ranking::RankingEngine;
use storage_opensearch::OpenSearchStore;
use storage_postgres::PgRepository;

mod cache_key;
mod candidate_backend;
mod errors;
mod request_id;
mod routes;
mod trace;
mod tracking;

pub use routes::build_app;

#[derive(Clone)]
pub enum CandidateBackend {
    SqlOnly,
    Full(OpenSearchStore),
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

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use api_contracts::RecommendationRequest;
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use cache::RecommendationCache;
    use config::{CandidateRetrievalMode, OpenSearchSettings, RankingProfiles};
    use domain::{EventKind, PlacementKind, UserEvent};
    use ranking::RankingEngine;
    use storage_opensearch::OpenSearchStore;
    use storage_postgres::PgRepository;
    use tower::ServiceExt;

    use super::{
        build_app,
        cache_key::build_recommendation_cache_key,
        candidate_backend::{
            actual_candidate_backend_name, should_use_opensearch_candidate_retrieval,
        },
        errors::{context_resolution_error_message, context_resolution_error_status},
        request_id::resolve_request_id,
        tracking::build_tracking_jobs,
        AppState, CandidateBackend,
    };

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    fn test_state(
        cache_enabled: bool,
        candidate_retrieval_mode: CandidateRetrievalMode,
    ) -> AppState {
        let profiles =
            RankingProfiles::load_from_dir(repo_root().join("configs/ranking")).expect("profiles");
        AppState {
            repository: Arc::new(PgRepository::new(
                "postgres://postgres:postgres@example.invalid/test_db",
            )),
            engine: RankingEngine::new(profiles.clone(), "phase7-test"),
            cache: RecommendationCache::new(
                cache_enabled.then_some("redis://127.0.0.1:6379".to_string()),
                60,
            ),
            profile_version: profiles.profile_version,
            algorithm_version: "phase7-test".to_string(),
            candidate_retrieval_mode,
            candidate_retrieval_limit: 256,
            neighbor_distance_cap_meters: profiles.fallback.neighbor_distance_cap_meters,
            candidate_backend: CandidateBackend::SqlOnly,
            worker_max_attempts: 3,
        }
    }

    #[test]
    fn search_execute_enqueues_global_refresh_without_user_affinity() {
        let state = test_state(true, CandidateRetrievalMode::Full);
        let event = UserEvent {
            user_id: "demo-user".to_string(),
            school_id: None,
            event_kind: EventKind::SearchExecute,
            event_id: None,
            target_station_id: Some("st_tamachi".to_string()),
            occurred_at: "2026-04-21T00:00:00Z".to_string(),
            payload: serde_json::json!({}),
        };

        let jobs = build_tracking_jobs(&state, &event)
            .into_iter()
            .map(|job| job.job_type.as_str().to_string())
            .collect::<Vec<_>>();

        assert_eq!(
            jobs,
            vec![
                "refresh_popularity_snapshot".to_string(),
                "invalidate_recommendation_cache".to_string(),
                "sync_candidate_projection".to_string(),
            ]
        );
    }

    #[test]
    fn context_resolution_errors_split_validation_from_operational_failures() {
        let unknown_station = anyhow::anyhow!("unknown station: station_missing");
        assert_eq!(
            context_resolution_error_status(&unknown_station),
            StatusCode::BAD_REQUEST
        );

        let unknown_line = anyhow::anyhow!("unknown line_id: line_missing");
        assert_eq!(
            context_resolution_error_status(&unknown_line),
            StatusCode::BAD_REQUEST
        );

        let missing_line = anyhow::anyhow!("line context requires line_id or line_name");
        assert_eq!(
            context_resolution_error_status(&missing_line),
            StatusCode::BAD_REQUEST
        );

        let blank_station = anyhow::anyhow!("station_id must not be blank");
        assert_eq!(
            context_resolution_error_status(&blank_station),
            StatusCode::BAD_REQUEST
        );

        let trace_write_error = anyhow::anyhow!("failed to record context trace");
        assert_eq!(
            context_resolution_error_status(&trace_write_error),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn context_resolution_unknown_station_message_uses_normalized_context_station() {
        let input = context::ContextInput {
            station_id: Some("  station_missing  ".to_string()),
            ..Default::default()
        };

        assert_eq!(
            context_resolution_error_message(
                &anyhow::anyhow!("unknown station: station_missing"),
                &input
            ),
            "unknown target_station_id: station_missing"
        );
    }

    #[test]
    fn context_resolution_blank_station_message_names_request_fields() {
        let input = context::ContextInput {
            station_id: Some("".to_string()),
            ..Default::default()
        };

        assert_eq!(
            context_resolution_error_message(
                &anyhow::anyhow!("station_id must not be blank"),
                &input
            ),
            "target_station_id or context.station_id must not be blank"
        );
    }

    #[test]
    fn opensearch_candidate_retrieval_is_only_for_station_without_area_context() {
        let mut context = context::RankingContext::default_safe();
        assert!(!should_use_opensearch_candidate_retrieval(&context));

        context.station = Some(context::StationContext {
            station_id: "st_tamachi".to_string(),
            station_name: "Tamachi".to_string(),
        });
        assert!(should_use_opensearch_candidate_retrieval(&context));

        context.area = Some(context::AreaContext {
            country: "JP".to_string(),
            prefecture_code: None,
            prefecture_name: None,
            city_code: None,
            city_name: Some("Minato".to_string()),
        });
        assert!(!should_use_opensearch_candidate_retrieval(&context));
    }

    #[test]
    fn actual_candidate_backend_uses_postgresql_when_context_disables_opensearch() {
        let settings = OpenSearchSettings {
            url: "http://127.0.0.1:9200".to_string(),
            index_name: "schools".to_string(),
            username: None,
            password: None,
            request_timeout_secs: 1,
        };
        let backend =
            CandidateBackend::Full(OpenSearchStore::new(&settings).expect("opensearch backend"));
        let mut context = context::RankingContext::default_safe();
        assert_eq!(
            actual_candidate_backend_name(&backend, &context),
            "postgresql"
        );

        context.station = Some(context::StationContext {
            station_id: "st_tamachi".to_string(),
            station_name: "Tamachi".to_string(),
        });
        assert_eq!(
            actual_candidate_backend_name(&backend, &context),
            "opensearch"
        );
    }

    #[test]
    fn resolved_context_changes_recommendation_cache_key() {
        let state = test_state(false, CandidateRetrievalMode::SqlOnly);
        let request = RecommendationRequest {
            request_id: Some("req-client-supplied".to_string()),
            target_station_id: None,
            context: None,
            limit: Some(3),
            user_id: Some("demo-user".to_string()),
            placement: PlacementKind::Search,
            debug: false,
        };
        let minato_query =
            request.with_resolved_context("st_tamachi".to_string(), area_context("Minato"));
        let shibuya_query =
            request.with_resolved_context("st_tamachi".to_string(), area_context("Shibuya"));

        let minato_key =
            build_recommendation_cache_key(&state, &minato_query).expect("minato cache key");
        let shibuya_key =
            build_recommendation_cache_key(&state, &shibuya_query).expect("shibuya cache key");

        assert_ne!(minato_key, shibuya_key);
    }

    fn area_context(city_name: &str) -> context::RankingContext {
        context::RankingContext {
            context_source: context::ContextSource::UserProfileArea,
            confidence: 0.8,
            area: Some(context::AreaContext {
                country: "JP".to_string(),
                prefecture_code: None,
                prefecture_name: Some("Tokyo".to_string()),
                city_code: None,
                city_name: Some(city_name.to_string()),
            }),
            line: None,
            station: None,
            privacy_level: context::PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        }
    }

    #[test]
    fn resolve_request_id_accepts_trimmed_client_value() {
        let request_id = resolve_request_id(Some("  req_client_123  ")).expect("request id");
        assert_eq!(request_id, "req_client_123");
    }

    #[test]
    fn resolve_request_id_rejects_invalid_characters() {
        assert_eq!(
            resolve_request_id(Some("req:bad")).unwrap_err(),
            "request_id may contain only ASCII letters, digits, '_' or '-'"
        );
    }

    #[test]
    fn resolve_request_id_rejects_oversized_values() {
        let oversized = "r".repeat(129);
        assert_eq!(
            resolve_request_id(Some(&oversized)).unwrap_err(),
            "request_id must be at most 128 characters"
        );
    }

    #[tokio::test]
    async fn malformed_json_uses_common_error_response_shape() {
        let app = build_app(test_state(false, CandidateRetrievalMode::SqlOnly));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/recommendations")
                    .header("content-type", "application/json")
                    .body(Body::from("{"))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(payload["code"], "bad_request");
        assert!(payload["error"]
            .as_str()
            .is_some_and(|value| !value.is_empty()));
    }

    #[tokio::test]
    async fn missing_json_content_type_uses_common_error_response_shape() {
        let app = build_app(test_state(false, CandidateRetrievalMode::SqlOnly));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/recommendations")
                    .body(Body::from(
                        serde_json::json!({
                            "target_station_id": "st_tamachi",
                            "placement": "search",
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(payload["code"], "bad_request");
        assert!(payload["error"]
            .as_str()
            .is_some_and(|value| value.contains("Content-Type")));
    }
}
