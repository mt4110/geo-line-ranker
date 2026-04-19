use std::{path::PathBuf, sync::Arc};

use api::{build_app, AppState};
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use cache::RecommendationCache;
use config::{CandidateRetrievalMode, RankingProfiles};
use ranking::RankingEngine;
use storage_postgres::{run_migrations, seed_fixture, PgRepository};
use tokio_postgres::NoTls;
use tower::ServiceExt;

fn default_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@127.0.0.1:5433/geo_line_ranker".to_string()
    })
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[tokio::test]
async fn track_endpoint_persists_events_and_enqueues_jobs() -> anyhow::Result<()> {
    let database_url = default_database_url();
    let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
        eprintln!("skipping api tracking integration test because PostgreSQL is not reachable");
        return Ok(());
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client.simple_query("SELECT 1").await?;

    let root = repo_root();
    run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;
    seed_fixture(&database_url, root.join("storage/fixtures/minimal")).await?;

    let job_baseline = client
        .query_one("SELECT COALESCE(MAX(id), 0) AS max_id FROM job_queue", &[])
        .await?
        .get::<_, i64>("max_id");

    let profiles = RankingProfiles::load_from_dir(root.join("configs/ranking"))?;
    let state = AppState {
        repository: Arc::new(PgRepository::new(database_url.clone())),
        engine: RankingEngine::new(profiles.clone(), "phase3-test"),
        cache: RecommendationCache::new(None, 60),
        profile_version: profiles.profile_version,
        algorithm_version: "phase3-test".to_string(),
        candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
        candidate_retrieval_limit: 256,
        neighbor_distance_cap_meters: profiles.fallback.neighbor_distance_cap_meters,
        candidate_backend: api::CandidateBackend::SqlOnly,
        worker_max_attempts: 3,
    };
    let app = build_app(state);
    let user_id = format!("api-track-test-{}", std::process::id());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/track")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "user_id": user_id.clone(),
                        "event_kind": "school_view",
                        "school_id": "school_seaside"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("tracking response");

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let payload: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(payload["status"], "accepted");

    let stored_count = client
        .query_one(
            "SELECT COUNT(*) AS count
             FROM user_events
             WHERE user_id = $1
               AND school_id = 'school_seaside'
               AND event_type = 'school_view'",
            &[&user_id],
        )
        .await?
        .get::<_, i64>("count");
    assert!(stored_count >= 1);

    let queued_job_types = client
        .query(
            "SELECT job_type
             FROM job_queue
             WHERE id > $1
             ORDER BY id",
            &[&job_baseline],
        )
        .await?
        .into_iter()
        .map(|row| row.get::<_, String>("job_type"))
        .collect::<Vec<_>>();
    assert!(queued_job_types.contains(&"refresh_popularity_snapshot".to_string()));
    assert!(queued_job_types.contains(&"refresh_user_affinity_snapshot".to_string()));

    Ok(())
}
