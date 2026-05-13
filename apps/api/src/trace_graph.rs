use api_contracts::CandidatePlanTraceDto;
use domain::Station;
use ranking::{AreaGraphExpansion, CandidateGraphExpansion, LineGraphExpansion};
use serde_json::{json, Value};
use storage::{
    AreaClusterDiagnostic, GeoGraph, GraphAdjacencyRepository, InterchangeDiagnostic, LineGraph,
    StationHopDiagnostic,
};
use storage_postgres::PgRepository;

const GRAPH_DIAGNOSTIC_SAMPLE_LIMIT: usize = 8;

#[derive(Debug, Clone, Copy)]
struct GraphDiagnosticOrigin<'a> {
    id: &'a str,
    source: &'static str,
}

pub(crate) async fn build_candidate_plan_graph_diagnostics_for_trace(
    repository: &PgRepository,
    context: &context::RankingContext,
    target_station: &Station,
    candidate_plan_trace: Option<&CandidatePlanTraceDto>,
) -> Option<Value> {
    if candidate_plan_trace.is_some() {
        Some(build_candidate_plan_graph_diagnostics(repository, context, target_station).await)
    } else {
        None
    }
}

pub(crate) async fn load_candidate_graph_expansion_for_plan(
    repository: &PgRepository,
    context: &context::RankingContext,
    target_station: &Station,
) -> storage::CandidatePlanGraphExpansion {
    match repository
        .load_candidate_plan_graph_expansion(target_station, context)
        .await
    {
        Ok(expansion) => expansion,
        Err(error) => {
            tracing::warn!(
                %error,
                station_id = target_station.id,
                "failed to load candidate plan graph expansion"
            );
            storage::CandidatePlanGraphExpansion::default()
        }
    }
}

pub(crate) fn candidate_graph_expansion_from_storage(
    expansion: storage::CandidatePlanGraphExpansion,
) -> CandidateGraphExpansion {
    let line = expansion
        .line
        .and_then(|line| LineGraphExpansion::new(line.origin_line_id, line.adjacent_line_ids));
    let area = expansion
        .area
        .and_then(|area| AreaGraphExpansion::new(area.origin_area_id, area.adjacent_area_ids));
    CandidateGraphExpansion::from_parts(line, area)
}

async fn build_candidate_plan_graph_diagnostics(
    repository: &PgRepository,
    context: &context::RankingContext,
    target_station: &Station,
) -> Value {
    let mut warnings = Vec::new();
    let area_origin_owned =
        resolve_area_graph_origin(repository, context, target_station, &mut warnings).await;
    let area_origin = area_origin_owned
        .as_ref()
        .map(|origin| GraphDiagnosticOrigin {
            id: origin.id.as_str(),
            source: origin.source,
        });
    let line_origin = resolve_line_graph_origin(context, target_station);

    let geo_graph = match area_origin {
        Some(origin) => match repository.load_geo_graph(origin.id).await {
            Ok(graph) => Some(graph),
            Err(error) => {
                tracing::warn!(
                    %error,
                    area_id = origin.id,
                    "failed to load candidate plan geo graph diagnostics"
                );
                warnings.push("geo_graph_load_failed".to_string());
                None
            }
        },
        None => {
            warnings.push("geo_graph_origin_unavailable".to_string());
            None
        }
    };
    let line_graph = match line_origin {
        Some(origin) => match repository.load_line_graph(origin.id).await {
            Ok(graph) => Some(graph),
            Err(error) => {
                tracing::warn!(
                    %error,
                    line_id = origin.id,
                    "failed to load candidate plan line graph diagnostics"
                );
                warnings.push("line_graph_load_failed".to_string());
                None
            }
        },
        None => {
            warnings.push("line_graph_origin_unavailable".to_string());
            None
        }
    };

    candidate_plan_graph_diagnostics_payload(
        area_origin,
        geo_graph.as_ref(),
        line_origin,
        line_graph.as_ref(),
        warnings,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OwnedGraphDiagnosticOrigin {
    id: String,
    source: &'static str,
}

async fn resolve_area_graph_origin(
    repository: &PgRepository,
    context: &context::RankingContext,
    target_station: &Station,
    warnings: &mut Vec<String>,
) -> Option<OwnedGraphDiagnosticOrigin> {
    let area_hint_was_ignored = context
        .warnings
        .iter()
        .any(|warning| warning.code == "station_area_conflict");
    if area_hint_was_ignored {
        warnings.push("area_context_ignored_by_station_conflict".to_string());
    }

    if !area_hint_was_ignored {
        if let Some(area) = context.area.as_ref() {
            match repository.load_area_id_for_context_area(area).await {
                Ok(Some(area_id)) => {
                    return Some(OwnedGraphDiagnosticOrigin {
                        id: area_id,
                        source: "context_area",
                    });
                }
                Ok(None) => warnings.push("context_area_id_unresolved".to_string()),
                Err(error) => {
                    tracing::warn!(%error, "failed to resolve candidate plan context area id");
                    warnings.push("context_area_id_lookup_failed".to_string());
                }
            }
        }
    }

    match repository.load_station_area_id(&target_station.id).await {
        Ok(Some(area_id)) => Some(OwnedGraphDiagnosticOrigin {
            id: area_id,
            source: "target_station_area",
        }),
        Ok(None) => None,
        Err(error) => {
            tracing::warn!(
                %error,
                station_id = target_station.id,
                "failed to resolve candidate plan target station area id"
            );
            warnings.push("target_station_area_id_lookup_failed".to_string());
            None
        }
    }
}

fn resolve_line_graph_origin<'a>(
    context: &'a context::RankingContext,
    target_station: &'a Station,
) -> Option<GraphDiagnosticOrigin<'a>> {
    context
        .line
        .as_ref()
        .and_then(|line| line.line_id.as_deref())
        .map(|line_id| GraphDiagnosticOrigin {
            id: line_id,
            source: "context_line",
        })
        .or_else(|| {
            target_station
                .line_id
                .as_deref()
                .map(|line_id| GraphDiagnosticOrigin {
                    id: line_id,
                    source: "target_station_line",
                })
        })
}

fn candidate_plan_graph_diagnostics_payload(
    area_origin: Option<GraphDiagnosticOrigin<'_>>,
    geo_graph: Option<&GeoGraph>,
    line_origin: Option<GraphDiagnosticOrigin<'_>>,
    line_graph: Option<&LineGraph>,
    mut warnings: Vec<String>,
) -> Value {
    let geo_graph = geo_graph_diagnostic_payload(area_origin, geo_graph, &mut warnings);
    let line_graph = line_graph_diagnostic_payload(line_origin, line_graph, &mut warnings);
    warnings.sort();
    warnings.dedup();
    json!({
        "mode": "diagnostic_read_only",
        "candidate_expansion_behavior": "graph_aware_candidate_plan",
        "origin": {
            "area_id": area_origin.map(|origin| origin.id),
            "area_source": area_origin.map(|origin| origin.source),
            "line_id": line_origin.map(|origin| origin.id),
            "line_source": line_origin.map(|origin| origin.source),
        },
        "geo_graph": geo_graph,
        "line_graph": line_graph,
        "warnings": warnings,
    })
}

fn geo_graph_diagnostic_payload(
    origin: Option<GraphDiagnosticOrigin<'_>>,
    graph: Option<&GeoGraph>,
    warnings: &mut Vec<String>,
) -> Value {
    match (origin, graph) {
        (Some(origin), Some(graph)) if graph.origin_area_id() == origin.id => {
            let adjacent_area_ids = graph.adjacent_area_ids();
            let area_clusters = graph.area_cluster_diagnostics();
            json!({
                "status": "loaded",
                "origin_area_id": graph.origin_area_id(),
                "origin_source": origin.source,
                "edge_count": graph.edges().len(),
                "adjacent_area_count": adjacent_area_ids.len(),
                "adjacent_area_id_sample": capped_string_sample(adjacent_area_ids),
                "area_cluster_count": area_clusters.len(),
                "area_cluster_sample": area_cluster_diagnostic_sample(area_clusters),
            })
        }
        (Some(origin), Some(graph)) => {
            warnings.push("geo_graph_origin_mismatch".to_string());
            json!({
                "status": "origin_mismatch",
                "origin_area_id": origin.id,
                "loaded_origin_area_id": graph.origin_area_id(),
                "origin_source": origin.source,
            })
        }
        (Some(origin), None) => json!({
            "status": "not_loaded",
            "origin_area_id": origin.id,
            "origin_source": origin.source,
        }),
        (None, _) => json!({
            "status": "origin_unavailable",
        }),
    }
}

fn line_graph_diagnostic_payload(
    origin: Option<GraphDiagnosticOrigin<'_>>,
    graph: Option<&LineGraph>,
    warnings: &mut Vec<String>,
) -> Value {
    match (origin, graph) {
        (Some(origin), Some(graph)) if graph.origin_line_id() == origin.id => {
            let adjacent_line_ids = graph.adjacent_line_ids();
            let station_hops = graph.station_hop_diagnostics();
            let interchanges = graph.interchange_diagnostics();
            json!({
                "status": "loaded",
                "origin_line_id": graph.origin_line_id(),
                "origin_source": origin.source,
                "edge_count": graph.edges().len(),
                "adjacent_line_count": adjacent_line_ids.len(),
                "adjacent_line_id_sample": capped_string_sample(adjacent_line_ids),
                "station_hop_count": station_hops.len(),
                "station_hop_sample": station_hop_diagnostic_sample(station_hops),
                "interchange_count": interchanges.len(),
                "interchange_sample": interchange_diagnostic_sample(interchanges),
            })
        }
        (Some(origin), Some(graph)) => {
            warnings.push("line_graph_origin_mismatch".to_string());
            json!({
                "status": "origin_mismatch",
                "origin_line_id": origin.id,
                "loaded_origin_line_id": graph.origin_line_id(),
                "origin_source": origin.source,
            })
        }
        (Some(origin), None) => json!({
            "status": "not_loaded",
            "origin_line_id": origin.id,
            "origin_source": origin.source,
        }),
        (None, _) => json!({
            "status": "origin_unavailable",
        }),
    }
}

fn capped_string_sample(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .take(GRAPH_DIAGNOSTIC_SAMPLE_LIMIT)
        .collect()
}

fn area_cluster_diagnostic_sample(area_clusters: Vec<AreaClusterDiagnostic>) -> Vec<Value> {
    area_clusters
        .into_iter()
        .take(GRAPH_DIAGNOSTIC_SAMPLE_LIMIT)
        .map(|cluster| {
            json!({
                "area_cluster_id": cluster.area_cluster_id,
                "observed_area_count": cluster.observed_area_ids.len(),
                "observed_area_id_sample": capped_string_sample(cluster.observed_area_ids),
            })
        })
        .collect()
}

fn station_hop_diagnostic_sample(station_hops: Vec<StationHopDiagnostic>) -> Vec<Value> {
    station_hops
        .into_iter()
        .take(GRAPH_DIAGNOSTIC_SAMPLE_LIMIT)
        .map(|hop| {
            json!({
                "from_line_id": hop.from_line_id,
                "to_line_id": hop.to_line_id,
                "adjacency_kind": hop.adjacency_kind,
                "station_hop_count": hop.station_hop_count,
                "interchange_station_id": hop.interchange_station_id,
                "requires_transfer": hop.requires_transfer,
            })
        })
        .collect()
}

fn interchange_diagnostic_sample(interchanges: Vec<InterchangeDiagnostic>) -> Vec<Value> {
    interchanges
        .into_iter()
        .take(GRAPH_DIAGNOSTIC_SAMPLE_LIMIT)
        .map(|interchange| {
            json!({
                "interchange_station_id": interchange.interchange_station_id,
                "from_line_id": interchange.from_line_id,
                "to_line_count": interchange.to_line_ids.len(),
                "to_line_id_sample": capped_string_sample(interchange.to_line_ids),
                "adjacency_kind_count": interchange.adjacency_kinds.len(),
                "adjacency_kind_sample": capped_string_sample(interchange.adjacency_kinds),
                "requires_transfer": interchange.requires_transfer,
                "minimum_station_hop_count": interchange.minimum_station_hop_count,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use domain::Station;
    use serde_json::json;
    use storage::{AreaAdjacency, GeoGraph, LineAdjacency, LineGraph};
    use storage_postgres::PgRepository;

    use super::{
        build_candidate_plan_graph_diagnostics_for_trace, candidate_plan_graph_diagnostics_payload,
        GraphDiagnosticOrigin, GRAPH_DIAGNOSTIC_SAMPLE_LIMIT,
    };

    #[tokio::test]
    async fn graph_diagnostics_for_trace_skips_when_candidate_plan_is_missing() {
        let repository = PgRepository::new("postgres://postgres:postgres@example.invalid/test_db");
        let context = context::RankingContext::default_safe();
        let target_station = Station {
            id: "st_tamachi".to_string(),
            name: "Tamachi".to_string(),
            line_name: "Yamanote".to_string(),
            line_id: Some("line_yamanote".to_string()),
            area_id: Some("area_tokyo_minato".to_string()),
            latitude: 35.645,
            longitude: 139.747,
        };

        let diagnostics = build_candidate_plan_graph_diagnostics_for_trace(
            &repository,
            &context,
            &target_station,
            None,
        )
        .await;

        assert!(diagnostics.is_none());
    }

    #[test]
    fn candidate_plan_graph_diagnostics_are_read_only_and_deterministic() {
        let geo_graph = GeoGraph::from_area_adjacencies(
            "area_tokyo_minato",
            vec![
                AreaAdjacency {
                    from_area_id: "area_tokyo_minato".to_string(),
                    to_area_id: "area_tokyo_shinagawa".to_string(),
                    adjacency_kind: "city_neighbor".to_string(),
                    distance_meters: Some(1_200.0),
                    area_cluster_id: Some("cluster_tokyo_bay".to_string()),
                    source_id: Some("fixture".to_string()),
                    source_version: Some("2026-05-13".to_string()),
                    attributes: json!({}),
                },
                AreaAdjacency {
                    from_area_id: "area_tokyo_minato".to_string(),
                    to_area_id: "area_tokyo_chuo".to_string(),
                    adjacency_kind: "city_neighbor".to_string(),
                    distance_meters: Some(800.0),
                    area_cluster_id: Some("cluster_tokyo_bay".to_string()),
                    source_id: Some("fixture".to_string()),
                    source_version: Some("2026-05-13".to_string()),
                    attributes: json!({}),
                },
            ],
        )
        .expect("geo graph");
        let line_graph = LineGraph::from_line_adjacencies(
            "line_yamanote",
            vec![LineAdjacency {
                from_line_id: "line_yamanote".to_string(),
                to_line_id: "line_keihin_tohoku".to_string(),
                adjacency_kind: "interchange".to_string(),
                interchange_station_id: Some("st_shinagawa".to_string()),
                station_hop_count: Some(0),
                requires_transfer: true,
                source_id: Some("fixture".to_string()),
                source_version: Some("2026-05-13".to_string()),
                attributes: json!({}),
            }],
        )
        .expect("line graph");

        let payload = candidate_plan_graph_diagnostics_payload(
            Some(GraphDiagnosticOrigin {
                id: "area_tokyo_minato",
                source: "context_area",
            }),
            Some(&geo_graph),
            Some(GraphDiagnosticOrigin {
                id: "line_yamanote",
                source: "context_line",
            }),
            Some(&line_graph),
            Vec::new(),
        );

        assert_eq!(payload["mode"], "diagnostic_read_only");
        assert_eq!(
            payload["candidate_expansion_behavior"],
            "graph_aware_candidate_plan"
        );
        assert_eq!(payload["geo_graph"]["status"], "loaded");
        assert_eq!(payload["geo_graph"]["edge_count"], 2);
        assert_eq!(payload["geo_graph"]["adjacent_area_count"], 2);
        assert_eq!(
            payload["geo_graph"]["adjacent_area_id_sample"],
            json!(["area_tokyo_chuo", "area_tokyo_shinagawa"])
        );
        assert_eq!(payload["geo_graph"]["area_cluster_count"], 1);
        assert_eq!(payload["line_graph"]["status"], "loaded");
        assert_eq!(payload["line_graph"]["edge_count"], 1);
        assert_eq!(payload["line_graph"]["adjacent_line_count"], 1);
        assert_eq!(payload["line_graph"]["station_hop_count"], 1);
        assert_eq!(payload["line_graph"]["interchange_count"], 1);
        assert_eq!(payload["warnings"], json!([]));
    }

    #[test]
    fn candidate_plan_graph_diagnostics_caps_detailed_samples() {
        let geo_graph = GeoGraph::from_area_adjacencies(
            "area_tokyo_minato",
            (0..12).map(|index| AreaAdjacency {
                from_area_id: "area_tokyo_minato".to_string(),
                to_area_id: format!("area_tokyo_neighbor_{index:02}"),
                adjacency_kind: "city_neighbor".to_string(),
                distance_meters: Some(f64::from(index)),
                area_cluster_id: Some(format!("cluster_{index:02}")),
                source_id: Some("fixture".to_string()),
                source_version: Some("2026-05-13".to_string()),
                attributes: json!({}),
            }),
        )
        .expect("geo graph");
        let line_graph = LineGraph::from_line_adjacencies(
            "line_yamanote",
            (0..12).map(|index| LineAdjacency {
                from_line_id: "line_yamanote".to_string(),
                to_line_id: format!("line_neighbor_{index:02}"),
                adjacency_kind: "interchange".to_string(),
                interchange_station_id: Some(format!("st_interchange_{index:02}")),
                station_hop_count: Some(index),
                requires_transfer: true,
                source_id: Some("fixture".to_string()),
                source_version: Some("2026-05-13".to_string()),
                attributes: json!({}),
            }),
        )
        .expect("line graph");

        let payload = candidate_plan_graph_diagnostics_payload(
            Some(GraphDiagnosticOrigin {
                id: "area_tokyo_minato",
                source: "context_area",
            }),
            Some(&geo_graph),
            Some(GraphDiagnosticOrigin {
                id: "line_yamanote",
                source: "context_line",
            }),
            Some(&line_graph),
            Vec::new(),
        );

        assert_eq!(payload["geo_graph"]["edge_count"], 12);
        assert_eq!(payload["geo_graph"]["adjacent_area_count"], 12);
        assert_eq!(
            payload["geo_graph"]["adjacent_area_id_sample"]
                .as_array()
                .expect("adjacent area sample")
                .len(),
            GRAPH_DIAGNOSTIC_SAMPLE_LIMIT
        );
        assert_eq!(payload["line_graph"]["edge_count"], 12);
        assert_eq!(payload["line_graph"]["station_hop_count"], 12);
        assert_eq!(
            payload["line_graph"]["station_hop_sample"]
                .as_array()
                .expect("station hop sample")
                .len(),
            GRAPH_DIAGNOSTIC_SAMPLE_LIMIT
        );
        assert_eq!(payload["line_graph"]["interchange_count"], 12);
        assert_eq!(
            payload["line_graph"]["interchange_sample"]
                .as_array()
                .expect("interchange sample")
                .len(),
            GRAPH_DIAGNOSTIC_SAMPLE_LIMIT
        );
    }

    #[test]
    fn candidate_plan_graph_diagnostics_guard_origin_mismatch() {
        let geo_graph = GeoGraph::from_area_adjacencies(
            "area_tokyo_minato",
            vec![AreaAdjacency {
                from_area_id: "area_tokyo_minato".to_string(),
                to_area_id: "area_tokyo_shinagawa".to_string(),
                adjacency_kind: "city_neighbor".to_string(),
                distance_meters: None,
                area_cluster_id: None,
                source_id: None,
                source_version: None,
                attributes: json!({}),
            }],
        )
        .expect("geo graph");
        let line_graph = LineGraph::from_line_adjacencies(
            "line_yamanote",
            vec![LineAdjacency {
                from_line_id: "line_yamanote".to_string(),
                to_line_id: "line_keihin_tohoku".to_string(),
                adjacency_kind: "interchange".to_string(),
                interchange_station_id: Some("st_shinagawa".to_string()),
                station_hop_count: Some(0),
                requires_transfer: true,
                source_id: None,
                source_version: None,
                attributes: json!({}),
            }],
        )
        .expect("line graph");

        let payload = candidate_plan_graph_diagnostics_payload(
            Some(GraphDiagnosticOrigin {
                id: "area_tokyo_other",
                source: "context_area",
            }),
            Some(&geo_graph),
            Some(GraphDiagnosticOrigin {
                id: "line_other",
                source: "context_line",
            }),
            Some(&line_graph),
            Vec::new(),
        );

        assert_eq!(payload["geo_graph"]["status"], "origin_mismatch");
        assert_eq!(payload["line_graph"]["status"], "origin_mismatch");
        assert_eq!(
            payload["warnings"],
            json!(["geo_graph_origin_mismatch", "line_graph_origin_mismatch"])
        );
    }
}
