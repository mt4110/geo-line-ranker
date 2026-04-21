use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use api::{build_app, AppState};
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use cache::RecommendationCache;
use config::{CandidateRetrievalMode, OpenSearchSettings, RankingProfiles};
use ranking::RankingEngine;
use storage_opensearch::OpenSearchStore;
use storage_postgres::{run_migrations, seed_fixture, PgRepository};
use tokio_postgres::NoTls;
use tower::ServiceExt;

fn default_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@127.0.0.1:5433/geo_line_ranker".to_string()
    })
}

fn default_redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

fn database_url_with_name(database_url: &str, database_name: &str) -> String {
    let Some((prefix, _)) = database_url.rsplit_once('/') else {
        return database_url.to_string();
    };
    format!("{prefix}/{database_name}")
}

fn unique_database_name(prefix: &str) -> String {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), suffix)
}

async fn create_empty_database(prefix: &str) -> anyhow::Result<(String, String, String)> {
    let admin_database_url = database_url_with_name(&default_database_url(), "postgres");
    let database_name = unique_database_name(prefix);
    let (client, connection) = tokio_postgres::connect(&admin_database_url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query(&format!("CREATE DATABASE \"{database_name}\""))
        .await?;

    Ok((
        admin_database_url,
        database_url_with_name(&default_database_url(), &database_name),
        database_name,
    ))
}

async fn drop_database(admin_database_url: &str, database_name: &str) -> anyhow::Result<()> {
    let (client, connection) = tokio_postgres::connect(admin_database_url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .query(
            "SELECT pg_terminate_backend(pid)
             FROM pg_stat_activity
             WHERE datname = $1
               AND pid <> pg_backend_pid()",
            &[&database_name],
        )
        .await?;
    client
        .simple_query(&format!("DROP DATABASE IF EXISTS \"{database_name}\""))
        .await?;
    Ok(())
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[tokio::test]
async fn track_endpoint_persists_events_and_enqueues_jobs() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_api").await
    else {
        eprintln!(
            "skipping api tracking integration test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
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
        assert!(payload["event_id"].as_str().is_some());

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
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn track_endpoint_search_execute_reuses_active_popularity_refresh() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_api").await
    else {
        eprintln!(
            "skipping api tracking integration test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
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
            engine: RankingEngine::new(profiles.clone(), "phase7-test"),
            cache: RecommendationCache::new(None, 60),
            profile_version: profiles.profile_version,
            algorithm_version: "phase7-test".to_string(),
            candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
            candidate_retrieval_limit: 256,
            neighbor_distance_cap_meters: profiles.fallback.neighbor_distance_cap_meters,
            candidate_backend: api::CandidateBackend::SqlOnly,
            worker_max_attempts: 3,
        };
        let app = build_app(state);
        let user_id = format!("api-track-search-{}", std::process::id());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/track")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "user_id": user_id.clone(),
                            "event_kind": "search_execute",
                            "target_station_id": "st_tamachi"
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
        assert_eq!(
            payload["queued_jobs"],
            serde_json::json!(["refresh_popularity_snapshot"])
        );

        let stored_station_id = client
            .query_one(
                "SELECT target_station_id
                 FROM user_events
                 WHERE user_id = $1
                   AND event_type = 'search_execute'
                 ORDER BY id DESC
                 LIMIT 1",
                &[&user_id],
            )
            .await?
            .get::<_, Option<String>>("target_station_id");
        assert_eq!(stored_station_id.as_deref(), Some("st_tamachi"));

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
        assert_eq!(
            queued_job_types,
            vec!["refresh_popularity_snapshot".to_string()]
        );

        let second_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/track")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "user_id": user_id.clone(),
                            "event_kind": "search_execute",
                            "target_station_id": "st_shinbashi"
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("tracking response");
        assert_eq!(second_response.status(), StatusCode::ACCEPTED);

        let search_event_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM user_events
                 WHERE user_id = $1
                   AND event_type = 'search_execute'",
                &[&user_id],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(search_event_count, 2);

        let active_refresh_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM job_queue
                 WHERE id > $1
                   AND job_type = 'refresh_popularity_snapshot'
                   AND status IN ('queued', 'running')",
                &[&job_baseline],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(active_refresh_count, 1);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn track_endpoint_rejects_unknown_foreign_keys() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_api").await
    else {
        eprintln!(
            "skipping api tracking integration test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;
        seed_fixture(&database_url, root.join("storage/fixtures/minimal")).await?;

        let profiles = RankingProfiles::load_from_dir(root.join("configs/ranking"))?;
        let state = AppState {
            repository: Arc::new(PgRepository::new(database_url.clone())),
            engine: RankingEngine::new(profiles.clone(), "phase6-test"),
            cache: RecommendationCache::new(None, 60),
            profile_version: profiles.profile_version,
            algorithm_version: "phase6-test".to_string(),
            candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
            candidate_retrieval_limit: 256,
            neighbor_distance_cap_meters: profiles.fallback.neighbor_distance_cap_meters,
            candidate_backend: api::CandidateBackend::SqlOnly,
            worker_max_attempts: 3,
        };
        let app = build_app(state);
        let user_id = format!("api-track-invalid-{}", std::process::id());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/track")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "user_id": user_id.clone(),
                            "event_kind": "search_execute",
                            "target_station_id": "station_missing"
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("tracking response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let payload: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(
            payload["error"],
            "track payload references unknown school_id, event_id, or target_station_id"
        );

        let stored_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM user_events
                 WHERE user_id = $1",
                &[&user_id],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(stored_count, 0);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn track_endpoint_derives_school_id_from_event_id() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_api").await
    else {
        eprintln!(
            "skipping api tracking integration test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
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
            engine: RankingEngine::new(profiles.clone(), "phase6-test"),
            cache: RecommendationCache::new(None, 60),
            profile_version: profiles.profile_version,
            algorithm_version: "phase6-test".to_string(),
            candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
            candidate_retrieval_limit: 256,
            neighbor_distance_cap_meters: profiles.fallback.neighbor_distance_cap_meters,
            candidate_backend: api::CandidateBackend::SqlOnly,
            worker_max_attempts: 3,
        };
        let app = build_app(state);
        let user_id = format!("api-track-derived-{}", std::process::id());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/track")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "user_id": user_id.clone(),
                            "event_kind": "event_view",
                            "event_id": "event_seaside_open"
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

        let stored_school_id = client
            .query_one(
                "SELECT school_id
                 FROM user_events
                 WHERE user_id = $1
                   AND event_type = 'event_view'
                   AND event_id = 'event_seaside_open'
                 ORDER BY id DESC
                 LIMIT 1",
                &[&user_id],
            )
            .await?
            .get::<_, Option<String>>("school_id");
        assert_eq!(stored_school_id.as_deref(), Some("school_seaside"));

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
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn track_endpoint_rejects_mismatched_event_school_pair() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_api").await
    else {
        eprintln!(
            "skipping api tracking integration test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;
        seed_fixture(&database_url, root.join("storage/fixtures/minimal")).await?;

        let profiles = RankingProfiles::load_from_dir(root.join("configs/ranking"))?;
        let state = AppState {
            repository: Arc::new(PgRepository::new(database_url.clone())),
            engine: RankingEngine::new(profiles.clone(), "phase6-test"),
            cache: RecommendationCache::new(None, 60),
            profile_version: profiles.profile_version,
            algorithm_version: "phase6-test".to_string(),
            candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
            candidate_retrieval_limit: 256,
            neighbor_distance_cap_meters: profiles.fallback.neighbor_distance_cap_meters,
            candidate_backend: api::CandidateBackend::SqlOnly,
            worker_max_attempts: 3,
        };
        let app = build_app(state);
        let user_id = format!("api-track-mismatch-{}", std::process::id());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/track")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "user_id": user_id.clone(),
                            "event_kind": "event_view",
                            "event_id": "event_seaside_open",
                            "school_id": "school_hillside"
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("tracking response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let payload: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(
            payload["error"],
            "event_id event_seaside_open belongs to school_id school_seaside, not school_hillside"
        );

        let stored_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM user_events
                 WHERE user_id = $1",
                &[&user_id],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(stored_count, 0);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn ready_endpoint_requires_application_schema() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_ready").await
    else {
        eprintln!("skipping ready integration test because PostgreSQL admin access is unavailable");
        return Ok(());
    };

    let test_result = async {
        let profiles = RankingProfiles::load_from_dir(repo_root().join("configs/ranking"))?;
        let state = AppState {
            repository: Arc::new(PgRepository::new(database_url.clone())),
            engine: RankingEngine::new(profiles.clone(), "phase6-test"),
            cache: RecommendationCache::new(None, 60),
            profile_version: profiles.profile_version,
            algorithm_version: "phase6-test".to_string(),
            candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
            candidate_retrieval_limit: 256,
            neighbor_distance_cap_meters: profiles.fallback.neighbor_distance_cap_meters,
            candidate_backend: api::CandidateBackend::SqlOnly,
            worker_max_attempts: 3,
        };
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/readyz")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("ready response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let payload: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(payload["status"], "not_ready");
        assert!(payload["database"]
            .as_str()
            .unwrap_or_default()
            .contains("missing required PostgreSQL schema"));

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn ready_endpoint_requires_snapshot_tables() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_ready_snapshots").await
    else {
        eprintln!("skipping ready snapshot integration test because PostgreSQL admin access is unavailable");
        return Ok(());
    };

    let test_result = async {
        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .simple_query("DROP TABLE popularity_snapshots")
            .await?;

        let profiles = RankingProfiles::load_from_dir(root.join("configs/ranking"))?;
        let state = AppState {
            repository: Arc::new(PgRepository::new(database_url.clone())),
            engine: RankingEngine::new(profiles.clone(), "phase6-test"),
            cache: RecommendationCache::new(None, 60),
            profile_version: profiles.profile_version,
            algorithm_version: "phase6-test".to_string(),
            candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
            candidate_retrieval_limit: 256,
            neighbor_distance_cap_meters: profiles.fallback.neighbor_distance_cap_meters,
            candidate_backend: api::CandidateBackend::SqlOnly,
            worker_max_attempts: 3,
        };
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/readyz")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("ready response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let payload: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(payload["status"], "not_ready");
        assert!(payload["database"]
            .as_str()
            .unwrap_or_default()
            .contains("popularity_snapshots"));

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn ready_endpoint_reports_disabled_opensearch_in_sql_only_mode() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_ready_ok").await
    else {
        eprintln!("skipping ready integration test because PostgreSQL admin access is unavailable");
        return Ok(());
    };

    let test_result = async {
        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;
        seed_fixture(&database_url, root.join("storage/fixtures/minimal")).await?;

        let profiles = RankingProfiles::load_from_dir(root.join("configs/ranking"))?;
        let state = AppState {
            repository: Arc::new(PgRepository::new(database_url.clone())),
            engine: RankingEngine::new(profiles.clone(), "phase6-test"),
            cache: RecommendationCache::new(None, 60),
            profile_version: profiles.profile_version,
            algorithm_version: "phase6-test".to_string(),
            candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
            candidate_retrieval_limit: 256,
            neighbor_distance_cap_meters: profiles.fallback.neighbor_distance_cap_meters,
            candidate_backend: api::CandidateBackend::SqlOnly,
            worker_max_attempts: 3,
        };
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/readyz")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("ready response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let payload: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(payload["status"], "ready");
        assert_eq!(payload["database"], "reachable");
        assert_eq!(payload["opensearch"], "disabled");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn ready_endpoint_requires_opensearch_in_full_mode() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_ready_full").await
    else {
        eprintln!("skipping ready integration test because PostgreSQL admin access is unavailable");
        return Ok(());
    };

    let test_result = async {
        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;
        seed_fixture(&database_url, root.join("storage/fixtures/minimal")).await?;

        let profiles = RankingProfiles::load_from_dir(root.join("configs/ranking"))?;
        let candidate_backend =
            api::CandidateBackend::Full(OpenSearchStore::new(&OpenSearchSettings {
                url: "http://127.0.0.1:9".to_string(),
                index_name: "geo_line_ranker_candidates".to_string(),
                username: None,
                password: None,
                request_timeout_secs: 1,
            })?);
        let state = AppState {
            repository: Arc::new(PgRepository::new(database_url.clone())),
            engine: RankingEngine::new(profiles.clone(), "phase6-test"),
            cache: RecommendationCache::new(None, 60),
            profile_version: profiles.profile_version,
            algorithm_version: "phase6-test".to_string(),
            candidate_retrieval_mode: CandidateRetrievalMode::Full,
            candidate_retrieval_limit: 256,
            neighbor_distance_cap_meters: profiles.fallback.neighbor_distance_cap_meters,
            candidate_backend,
            worker_max_attempts: 3,
        };
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/readyz")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("ready response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let payload: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(payload["status"], "not_ready");
        assert_eq!(payload["database"], "reachable");
        assert!(payload["opensearch"]
            .as_str()
            .unwrap_or_default()
            .contains("failed to check OpenSearch index"));

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn recommend_endpoint_rejects_unknown_target_station_with_clear_message() -> anyhow::Result<()>
{
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_api").await
    else {
        eprintln!(
            "skipping api recommendation integration test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;
        seed_fixture(&database_url, root.join("storage/fixtures/minimal")).await?;

        let profiles = RankingProfiles::load_from_dir(root.join("configs/ranking"))?;
        let state = AppState {
            repository: Arc::new(PgRepository::new(database_url.clone())),
            engine: RankingEngine::new(profiles.clone(), "phase6-test"),
            cache: RecommendationCache::new(None, 60),
            profile_version: profiles.profile_version,
            algorithm_version: "phase6-test".to_string(),
            candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
            candidate_retrieval_limit: 256,
            neighbor_distance_cap_meters: profiles.fallback.neighbor_distance_cap_meters,
            candidate_backend: api::CandidateBackend::SqlOnly,
            worker_max_attempts: 3,
        };
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/recommendations")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "target_station_id": "station_missing",
                            "placement": "search",
                            "limit": 3
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("recommendation response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let payload: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(
            payload["error"],
            "unknown target_station_id: station_missing"
        );

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn recommend_endpoint_ignores_trace_persistence_failures() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_api").await
    else {
        eprintln!(
            "skipping api recommendation trace test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let redis_url = default_redis_url();
        let cache = RecommendationCache::new(Some(redis_url), 60);
        if let Err(error) = cache
            .set_json(
                "geo-line-ranker:api-trace-probe",
                &serde_json::json!({ "ok": true }),
            )
            .await
        {
            eprintln!(
                "skipping api recommendation trace test because Redis is not reachable: {error}"
            );
            return Ok(());
        }

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;
        seed_fixture(&database_url, root.join("storage/fixtures/minimal")).await?;
        client
            .simple_query("DROP TABLE recommendation_traces")
            .await?;

        let profiles = RankingProfiles::load_from_dir(root.join("configs/ranking"))?;
        let state = AppState {
            repository: Arc::new(PgRepository::new(database_url.clone())),
            engine: RankingEngine::new(profiles.clone(), "phase6-test"),
            cache,
            profile_version: profiles.profile_version,
            algorithm_version: "phase6-test".to_string(),
            candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
            candidate_retrieval_limit: 256,
            neighbor_distance_cap_meters: profiles.fallback.neighbor_distance_cap_meters,
            candidate_backend: api::CandidateBackend::SqlOnly,
            worker_max_attempts: 3,
        };
        let app = build_app(state);
        let request_body = serde_json::json!({
            "target_station_id": "st_tamachi",
            "limit": 3,
            "placement": "search"
        })
        .to_string();

        let first_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/recommendations")
                    .header("content-type", "application/json")
                    .body(Body::from(request_body.clone()))
                    .expect("request"),
            )
            .await
            .expect("first recommendation response");
        assert_eq!(first_response.status(), StatusCode::OK);
        let first_body = to_bytes(first_response.into_body(), usize::MAX).await?;
        let first_payload: serde_json::Value = serde_json::from_slice(&first_body)?;
        assert!(!first_payload["items"]
            .as_array()
            .expect("items array")
            .is_empty());

        let second_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/recommendations")
                    .header("content-type", "application/json")
                    .body(Body::from(request_body))
                    .expect("request"),
            )
            .await
            .expect("second recommendation response");
        assert_eq!(second_response.status(), StatusCode::OK);
        let second_body = to_bytes(second_response.into_body(), usize::MAX).await?;
        let second_payload: serde_json::Value = serde_json::from_slice(&second_body)?;
        assert!(!second_payload["items"]
            .as_array()
            .expect("items array")
            .is_empty());

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
