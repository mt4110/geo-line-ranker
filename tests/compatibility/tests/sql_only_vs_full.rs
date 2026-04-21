use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{head, post},
    Json, Router,
};
use config::{OpenSearchSettings, RankingProfiles};
use domain::{PlacementKind, RankingDataset, RankingQuery, School, Station};
use geo::haversine_meters;
use ranking::RankingEngine;
use reqwest::Client;
use serde_json::{json, Value};
use storage_opensearch::{OpenSearchStore, ProjectionDocument};
use test_support::load_fixture_dataset;
use tokio::time::sleep;

const TEST_INDEX_NAME: &str = "candidate_projection";

#[derive(Clone)]
struct MockSearchState {
    documents: Vec<ProjectionDocument>,
    index_name: String,
    index_ready: Arc<AtomicBool>,
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../storage/fixtures/minimal")
}

fn config_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../configs/ranking")
}

fn query(target_station_id: &str) -> RankingQuery {
    RankingQuery {
        target_station_id: target_station_id.to_string(),
        limit: Some(3),
        user_id: None,
        placement: PlacementKind::Search,
        debug: false,
    }
}

#[tokio::test]
async fn full_mode_matches_sql_only_for_strict_candidates() -> Result<()> {
    assert_sql_only_and_full_match("st_tamachi").await
}

#[tokio::test]
async fn full_mode_matches_sql_only_for_neighbor_candidates() -> Result<()> {
    assert_sql_only_and_full_match("st_shinbashi").await
}

async fn assert_sql_only_and_full_match(target_station_id: &str) -> Result<()> {
    let dataset = load_fixture_dataset(fixture_root())?;
    let profiles = RankingProfiles::load_from_dir(config_root())?;
    let engine = RankingEngine::new(profiles.clone(), "phase4-compatibility-test");
    let target_station = dataset
        .stations
        .iter()
        .find(|station| station.id == target_station_id)
        .cloned()
        .with_context(|| format!("missing target station {target_station_id}"))?;

    let documents = build_projection_documents(&dataset);
    let base_url = spawn_mock_opensearch(documents).await?;
    let store = OpenSearchStore::new(&OpenSearchSettings {
        url: base_url,
        index_name: TEST_INDEX_NAME.to_string(),
        username: None,
        password: None,
        request_timeout_secs: 5,
    })?;

    let candidate_links = store
        .search_candidate_links(
            &target_station,
            profiles.fallback.neighbor_distance_cap_meters,
            256,
            profiles.placement(PlacementKind::Search).neighbor_max_hops,
        )
        .await?;
    let sql_only_result = engine.recommend(&dataset, &query(target_station_id))?;

    let mut full_dataset = dataset.clone();
    full_dataset.school_station_links = candidate_links;
    let full_mode_result = engine.recommend(&full_dataset, &query(target_station_id))?;

    assert_eq!(full_mode_result, sql_only_result);
    Ok(())
}

fn build_projection_documents(dataset: &RankingDataset) -> Vec<ProjectionDocument> {
    let schools_by_id: HashMap<&str, &School> = dataset
        .schools
        .iter()
        .map(|school| (school.id.as_str(), school))
        .collect();
    let stations_by_id: HashMap<&str, &Station> = dataset
        .stations
        .iter()
        .map(|station| (station.id.as_str(), station))
        .collect();
    let open_day_counts = dataset
        .events
        .iter()
        .fold(HashMap::new(), |mut acc, event| {
            if event.is_open_day {
                *acc.entry(event.school_id.as_str()).or_insert(0_i64) += 1;
            }
            acc
        });

    dataset
        .school_station_links
        .iter()
        .filter_map(|link| {
            let school = schools_by_id.get(link.school_id.as_str())?;
            let station = stations_by_id.get(link.station_id.as_str())?;
            Some(ProjectionDocument::from_parts(
                school,
                station,
                link,
                open_day_counts
                    .get(link.school_id.as_str())
                    .copied()
                    .unwrap_or(0),
                0.0,
            ))
        })
        .collect()
}

async fn spawn_mock_opensearch(documents: Vec<ProjectionDocument>) -> Result<String> {
    let state = MockSearchState {
        documents,
        index_name: TEST_INDEX_NAME.to_string(),
        index_ready: Arc::new(AtomicBool::new(false)),
    };
    let app = Router::new()
        .route("/:index", head(index_exists).put(create_index))
        .route("/:index/_search", post(search))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let base_url = format!("http://{address}");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    wait_for_mock_opensearch(&base_url).await?;
    Ok(base_url)
}

async fn wait_for_mock_opensearch(base_url: &str) -> Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .context("failed to build mock OpenSearch probe client")?;
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        match client
            .head(format!("{base_url}/{TEST_INDEX_NAME}"))
            .send()
            .await
        {
            Ok(response) if matches!(response.status(), StatusCode::OK | StatusCode::NOT_FOUND) => {
                return Ok(());
            }
            Ok(_) if Instant::now() < deadline => {}
            Ok(response) => {
                anyhow::bail!(
                    "mock OpenSearch readiness probe returned unexpected status {}",
                    response.status()
                );
            }
            Err(_) if Instant::now() < deadline => {}
            Err(error) => return Err(error).context("mock OpenSearch did not become ready"),
        }

        sleep(Duration::from_millis(25)).await;
    }
}

async fn index_exists(
    Path(index): Path<String>,
    State(state): State<MockSearchState>,
) -> StatusCode {
    if index == state.index_name && state.index_ready.load(Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn create_index(
    Path(index): Path<String>,
    State(state): State<MockSearchState>,
) -> StatusCode {
    if index != state.index_name {
        return StatusCode::NOT_FOUND;
    }

    state.index_ready.store(true, Ordering::Relaxed);
    StatusCode::OK
}

async fn search(
    Path(index): Path<String>,
    State(state): State<MockSearchState>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    if index != state.index_name || !state.index_ready.load(Ordering::Relaxed) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let query = match parse_mock_search_query(&body, state.documents.len()) {
        Ok(query) => query,
        Err(message) => {
            return (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response();
        }
    };

    let mut matches = state
        .documents
        .iter()
        .filter(|document| {
            document.station_id == query.target_station_id
                || (document.line_name == query.line_name
                    && document.hop_distance <= query.neighbor_max_hops
                    && haversine_meters(
                        query.target_lat,
                        query.target_lon,
                        document.station_location.lat,
                        document.station_location.lon,
                    ) <= query.distance_cap_meters)
        })
        .cloned()
        .collect::<Vec<_>>();

    matches.sort_by(|left, right| {
        let left_is_not_direct = left.station_id != query.target_station_id;
        let right_is_not_direct = right.station_id != query.target_station_id;
        left_is_not_direct
            .cmp(&right_is_not_direct)
            .then_with(|| left.distance_meters.cmp(&right.distance_meters))
            .then_with(|| left.walking_minutes.cmp(&right.walking_minutes))
            .then_with(|| left.school_id.cmp(&right.school_id))
            .then_with(|| left.station_id.cmp(&right.station_id))
    });

    Json(json!({
        "hits": {
            "hits": matches
                .into_iter()
                .take(query.size)
                .map(|document| {
                    json!({
                        "_id": document.document_id,
                        "_source": document,
                    })
                })
                .collect::<Vec<_>>()
        }
    }))
    .into_response()
}

#[derive(Debug, Clone, Copy)]
struct ParsedMockSearchQuery<'a> {
    size: usize,
    target_station_id: &'a str,
    line_name: &'a str,
    neighbor_max_hops: u8,
    distance_cap_meters: f64,
    target_lat: f64,
    target_lon: f64,
}

fn parse_mock_search_query<'a>(
    body: &'a Value,
    default_size: usize,
) -> std::result::Result<ParsedMockSearchQuery<'a>, String> {
    validate_candidate_sort(body)?;

    let size = match body.pointer("/size") {
        Some(value) => {
            let raw = value
                .as_u64()
                .ok_or_else(|| "expected /size to be an unsigned integer".to_string())?;
            usize::try_from(raw).map_err(|_| "search size exceeds usize".to_string())?
        }
        None => default_size,
    };
    let target_station_id = required_str_at(
        body,
        "/query/bool/should/0/constant_score/filter/term/station_id/value",
        "target station id",
    )?;
    let line_name = required_str_at(
        body,
        "/query/bool/should/1/constant_score/filter/bool/filter/0/term/line_name/value",
        "line name",
    )?;
    let neighbor_max_hops = body
        .pointer("/query/bool/should/1/constant_score/filter/bool/filter/1/range/hop_distance/lte")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            "missing or invalid neighbor hop cap at /query/bool/should/1/constant_score/filter/bool/filter/1/range/hop_distance/lte".to_string()
        })
        .and_then(|raw| {
            u8::try_from(raw).map_err(|_| "neighbor hop cap exceeds u8".to_string())
        })?;
    let distance_raw = required_str_at(
        body,
        "/query/bool/should/1/constant_score/filter/bool/filter/2/geo_distance/distance",
        "distance cap",
    )?;
    let distance_cap_meters = distance_raw
        .strip_suffix('m')
        .ok_or_else(|| "expected distance cap to end with 'm'".to_string())?
        .parse::<f64>()
        .map_err(|_| "expected distance cap to be a numeric meter value".to_string())?;
    let target_lat = required_f64_at(
        body,
        "/query/bool/should/1/constant_score/filter/bool/filter/2/geo_distance/station_location/lat",
        "target latitude",
    )?;
    let target_lon = required_f64_at(
        body,
        "/query/bool/should/1/constant_score/filter/bool/filter/2/geo_distance/station_location/lon",
        "target longitude",
    )?;

    Ok(ParsedMockSearchQuery {
        size,
        target_station_id,
        line_name,
        neighbor_max_hops,
        distance_cap_meters,
        target_lat,
        target_lon,
    })
}

fn validate_candidate_sort(body: &Value) -> std::result::Result<(), String> {
    let sort = body
        .get("sort")
        .and_then(Value::as_array)
        .ok_or_else(|| "missing or invalid sort array".to_string())?;
    let expected = [
        ("_score", "desc"),
        ("distance_meters", "asc"),
        ("walking_minutes", "asc"),
        ("school_id", "asc"),
        ("station_id", "asc"),
    ];

    if sort.len() != expected.len() {
        return Err(format!(
            "expected {} sort entries, got {}",
            expected.len(),
            sort.len()
        ));
    }

    for (index, (field, order)) in expected.iter().enumerate() {
        let pointer = format!("/{field}/order");
        let actual = sort
            .get(index)
            .and_then(|entry| entry.pointer(&pointer))
            .and_then(Value::as_str)
            .ok_or_else(|| format!("missing sort order at /sort/{index}{pointer}"))?;
        if actual != *order {
            return Err(format!(
                "expected /sort/{index}{pointer} to be {order}, got {actual}"
            ));
        }
    }

    Ok(())
}

fn required_str_at<'a>(
    body: &'a Value,
    pointer: &'static str,
    field_name: &'static str,
) -> std::result::Result<&'a str, String> {
    body.pointer(pointer)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing or invalid {field_name} at {pointer}"))
}

fn required_f64_at(
    body: &Value,
    pointer: &'static str,
    field_name: &'static str,
) -> std::result::Result<f64, String> {
    body.pointer(pointer)
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("missing or invalid {field_name} at {pointer}"))
}

#[test]
fn mock_search_parser_rejects_missing_required_fields() {
    let error = parse_mock_search_query(&json!({ "size": 3 }), 10).expect_err("missing sort");
    assert!(error.contains("sort"));

    let error = parse_mock_search_query(
        &json!({
            "size": 3,
            "sort": [
                { "_score": { "order": "desc" } },
                { "distance_meters": { "order": "asc" } },
                { "walking_minutes": { "order": "asc" } },
                { "school_id": { "order": "asc" } },
                { "station_id": { "order": "asc" } }
            ]
        }),
        10,
    )
    .expect_err("missing query");
    assert!(error.contains("target station id"));
}

#[tokio::test]
async fn full_mode_candidate_retrieval_filters_out_of_hop_neighbors_before_limit() -> Result<()> {
    let target_station = Station {
        id: "st_target".to_string(),
        name: "Target".to_string(),
        line_name: "JR Yamanote Line".to_string(),
        latitude: 35.0,
        longitude: 139.0,
    };
    let documents = vec![
        projection_document("school_far_a", "st_far_a", 35.0, 139.0004, 60, 60, 2),
        projection_document("school_far_b", "st_far_b", 35.0, 139.0005, 70, 70, 3),
        projection_document("school_in_hop", "st_in_hop", 35.0, 139.0012, 120, 120, 1),
    ];
    let base_url = spawn_mock_opensearch(documents).await?;
    let store = OpenSearchStore::new(&OpenSearchSettings {
        url: base_url,
        index_name: TEST_INDEX_NAME.to_string(),
        username: None,
        password: None,
        request_timeout_secs: 5,
    })?;

    let candidate_links = store
        .search_candidate_links(&target_station, 500.0, 2, 1)
        .await?;

    assert_eq!(candidate_links.len(), 1);
    assert_eq!(candidate_links[0].school_id, "school_in_hop");
    assert_eq!(candidate_links[0].hop_distance, 1);
    Ok(())
}

#[tokio::test]
async fn full_mode_candidate_retrieval_keeps_sql_only_ordering_for_limit() -> Result<()> {
    let target_station = Station {
        id: "st_target".to_string(),
        name: "Target".to_string(),
        line_name: "JR Yamanote Line".to_string(),
        latitude: 35.0,
        longitude: 139.0,
    };
    let documents = vec![
        projection_document("school_neighbor", "st_neighbor", 35.0, 139.0001, 1, 10, 1),
        projection_document("school_direct", "st_target", 35.0, 139.0, 30, 300, 0),
    ];
    let base_url = spawn_mock_opensearch(documents).await?;
    let store = OpenSearchStore::new(&OpenSearchSettings {
        url: base_url,
        index_name: TEST_INDEX_NAME.to_string(),
        username: None,
        password: None,
        request_timeout_secs: 5,
    })?;

    let candidate_links = store
        .search_candidate_links(&target_station, 500.0, 1, 1)
        .await?;

    assert_eq!(candidate_links.len(), 1);
    assert_eq!(candidate_links[0].school_id, "school_direct");
    assert_eq!(candidate_links[0].station_id, "st_target");
    Ok(())
}

fn projection_document(
    school_id: &str,
    station_id: &str,
    latitude: f64,
    longitude: f64,
    walking_minutes: u16,
    distance_meters: u32,
    hop_distance: u8,
) -> ProjectionDocument {
    ProjectionDocument {
        document_id: format!("{school_id}:{station_id}"),
        school_id: school_id.to_string(),
        school_name: format!("{school_id} name"),
        school_area: "Minato".to_string(),
        school_type: "high_school".to_string(),
        station_id: station_id.to_string(),
        station_name: format!("{station_id} name"),
        line_name: "JR Yamanote Line".to_string(),
        station_location: storage_opensearch::ProjectionGeoPoint {
            lat: latitude,
            lon: longitude,
        },
        walking_minutes,
        distance_meters,
        hop_distance,
        open_day_count: 0,
        popularity_score: 0.0,
    }
}
