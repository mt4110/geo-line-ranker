use api_contracts::CandidatePlanTraceDto;
use domain::Station;
use serde_json::{json, Value};
use storage::{GeoGraph, GraphAdjacencyRepository, LineGraph};
use storage_postgres::PgRepository;

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
        "candidate_expansion_behavior": "unchanged",
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
        (Some(origin), Some(graph)) if graph.origin_area_id() == origin.id => json!({
            "status": "loaded",
            "origin_area_id": graph.origin_area_id(),
            "origin_source": origin.source,
            "edge_count": graph.edges().len(),
            "adjacent_area_ids": graph.adjacent_area_ids(),
            "area_clusters": graph.area_cluster_diagnostics(),
        }),
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
        (Some(origin), Some(graph)) if graph.origin_line_id() == origin.id => json!({
            "status": "loaded",
            "origin_line_id": graph.origin_line_id(),
            "origin_source": origin.source,
            "edge_count": graph.edges().len(),
            "adjacent_line_ids": graph.adjacent_line_ids(),
            "station_hops": graph.station_hop_diagnostics(),
            "interchanges": graph.interchange_diagnostics(),
        }),
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

#[cfg(test)]
mod tests {
    use domain::Station;
    use serde_json::json;
    use storage::{AreaAdjacency, GeoGraph, LineAdjacency, LineGraph};
    use storage_postgres::PgRepository;

    use super::{
        build_candidate_plan_graph_diagnostics_for_trace, candidate_plan_graph_diagnostics_payload,
        GraphDiagnosticOrigin,
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
        assert_eq!(payload["candidate_expansion_behavior"], "unchanged");
        assert_eq!(payload["geo_graph"]["status"], "loaded");
        assert_eq!(payload["geo_graph"]["edge_count"], 2);
        assert_eq!(
            payload["geo_graph"]["adjacent_area_ids"],
            json!(["area_tokyo_chuo", "area_tokyo_shinagawa"])
        );
        assert_eq!(payload["line_graph"]["status"], "loaded");
        assert_eq!(payload["line_graph"]["edge_count"], 1);
        assert_eq!(payload["warnings"], json!([]));
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
