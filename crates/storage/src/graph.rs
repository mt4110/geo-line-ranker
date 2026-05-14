use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

use crate::{ensure_non_empty, AreaAdjacency, LineAdjacency};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CandidatePlanGraphExpansion {
    pub area: Option<CandidatePlanAreaGraphExpansion>,
    pub line: Option<CandidatePlanLineGraphExpansion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CandidatePlanAreaGraphExpansion {
    pub origin_area_id: String,
    pub adjacent_area_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CandidatePlanLineGraphExpansion {
    pub origin_line_id: String,
    pub adjacent_line_ids: Vec<String>,
}

/// Canonical read-only geographic graph component backed by area adjacency rows.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GeoGraph {
    origin_area_id: String,
    edges: Vec<GeoGraphEdge>,
}

impl GeoGraph {
    pub fn from_area_adjacencies(
        origin_area_id: impl Into<String>,
        adjacencies: impl IntoIterator<Item = AreaAdjacency>,
    ) -> Result<Self> {
        let origin_area_id = origin_area_id.into();
        ensure_non_empty("origin_area_id", &origin_area_id)?;
        let mut edges = adjacencies
            .into_iter()
            .map(GeoGraphEdge::from_area_adjacency)
            .collect::<Result<Vec<_>>>()?;
        for edge in &edges {
            anyhow::ensure!(
                edge.from_area_id == origin_area_id,
                "geo graph edge must start from origin_area_id: expected from_area_id={}, actual from_area_id={}, to_area_id={}, adjacency_kind={}",
                origin_area_id,
                edge.from_area_id,
                edge.to_area_id,
                edge.adjacency_kind
            );
        }
        sort_geo_graph_edges(&mut edges);
        Ok(Self {
            origin_area_id,
            edges,
        })
    }

    pub fn origin_area_id(&self) -> &str {
        &self.origin_area_id
    }

    pub fn edges(&self) -> &[GeoGraphEdge] {
        &self.edges
    }

    pub fn adjacent_area_ids(&self) -> Vec<String> {
        self.edges
            .iter()
            .map(|edge| edge.to_area_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn area_cluster_diagnostics(&self) -> Vec<AreaClusterDiagnostic> {
        let mut clusters = BTreeMap::<String, BTreeSet<String>>::new();
        for edge in &self.edges {
            if let Some(cluster_id) = &edge.area_cluster_id {
                let area_ids = clusters.entry(cluster_id.clone()).or_default();
                area_ids.insert(edge.from_area_id.clone());
                area_ids.insert(edge.to_area_id.clone());
            }
        }

        clusters
            .into_iter()
            .map(|(area_cluster_id, area_ids)| AreaClusterDiagnostic {
                area_cluster_id,
                observed_area_ids: area_ids.into_iter().collect(),
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GeoGraphEdge {
    pub from_area_id: String,
    pub to_area_id: String,
    pub adjacency_kind: String,
    pub distance_meters: Option<f64>,
    pub area_cluster_id: Option<String>,
    pub source_id: Option<String>,
    pub source_version: Option<String>,
    pub attributes: Value,
}

impl GeoGraphEdge {
    pub fn from_area_adjacency(adjacency: AreaAdjacency) -> Result<Self> {
        adjacency.validate()?;
        Ok(Self {
            from_area_id: adjacency.from_area_id,
            to_area_id: adjacency.to_area_id,
            adjacency_kind: adjacency.adjacency_kind,
            distance_meters: adjacency.distance_meters,
            area_cluster_id: adjacency.area_cluster_id,
            source_id: adjacency.source_id,
            source_version: adjacency.source_version,
            attributes: adjacency.attributes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AreaClusterDiagnostic {
    pub area_cluster_id: String,
    pub observed_area_ids: Vec<String>,
}

/// Canonical read-only rail line graph component backed by line adjacency rows.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LineGraph {
    origin_line_id: String,
    edges: Vec<LineGraphEdge>,
}

impl LineGraph {
    pub fn from_line_adjacencies(
        origin_line_id: impl Into<String>,
        adjacencies: impl IntoIterator<Item = LineAdjacency>,
    ) -> Result<Self> {
        let origin_line_id = origin_line_id.into();
        ensure_non_empty("origin_line_id", &origin_line_id)?;
        let mut edges = adjacencies
            .into_iter()
            .map(LineGraphEdge::from_line_adjacency)
            .collect::<Result<Vec<_>>>()?;
        for edge in &edges {
            anyhow::ensure!(
                edge.from_line_id == origin_line_id,
                "line graph edge must start from origin_line_id: expected from_line_id={}, actual from_line_id={}, to_line_id={}, adjacency_kind={}",
                origin_line_id,
                edge.from_line_id,
                edge.to_line_id,
                edge.adjacency_kind
            );
        }
        sort_line_graph_edges(&mut edges);
        Ok(Self {
            origin_line_id,
            edges,
        })
    }

    pub fn origin_line_id(&self) -> &str {
        &self.origin_line_id
    }

    pub fn edges(&self) -> &[LineGraphEdge] {
        &self.edges
    }

    pub fn adjacent_line_ids(&self) -> Vec<String> {
        self.edges
            .iter()
            .map(|edge| edge.to_line_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn station_hop_diagnostics(&self) -> Vec<StationHopDiagnostic> {
        self.edges
            .iter()
            .map(|edge| StationHopDiagnostic {
                from_line_id: edge.from_line_id.clone(),
                to_line_id: edge.to_line_id.clone(),
                adjacency_kind: edge.adjacency_kind.clone(),
                station_hop_count: edge.station_hop_count,
                interchange_station_id: edge.interchange_station_id.clone(),
                requires_transfer: edge.requires_transfer,
            })
            .collect()
    }

    pub fn interchange_diagnostics(&self) -> Vec<InterchangeDiagnostic> {
        let mut interchanges = BTreeMap::<String, InterchangeAccumulator>::new();
        for edge in &self.edges {
            let Some(station_id) = &edge.interchange_station_id else {
                continue;
            };
            let interchange = interchanges.entry(station_id.clone()).or_default();
            interchange.to_line_ids.insert(edge.to_line_id.clone());
            interchange
                .adjacency_kinds
                .insert(edge.adjacency_kind.clone());
            interchange.requires_transfer |= edge.requires_transfer;
            interchange.minimum_station_hop_count = min_optional_u32(
                interchange.minimum_station_hop_count,
                edge.station_hop_count,
            );
        }

        interchanges
            .into_iter()
            .map(
                |(interchange_station_id, interchange)| InterchangeDiagnostic {
                    interchange_station_id,
                    from_line_id: self.origin_line_id.clone(),
                    to_line_ids: interchange.to_line_ids.into_iter().collect(),
                    adjacency_kinds: interchange.adjacency_kinds.into_iter().collect(),
                    requires_transfer: interchange.requires_transfer,
                    minimum_station_hop_count: interchange.minimum_station_hop_count,
                },
            )
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LineGraphEdge {
    pub from_line_id: String,
    pub to_line_id: String,
    pub adjacency_kind: String,
    pub interchange_station_id: Option<String>,
    pub station_hop_count: Option<u32>,
    pub requires_transfer: bool,
    pub source_id: Option<String>,
    pub source_version: Option<String>,
    pub attributes: Value,
}

impl LineGraphEdge {
    pub fn from_line_adjacency(adjacency: LineAdjacency) -> Result<Self> {
        adjacency.validate()?;
        let station_hop_count = adjacency.station_hop_count.map(u32::try_from).transpose()?;
        Ok(Self {
            from_line_id: adjacency.from_line_id,
            to_line_id: adjacency.to_line_id,
            adjacency_kind: adjacency.adjacency_kind,
            interchange_station_id: adjacency.interchange_station_id,
            station_hop_count,
            requires_transfer: adjacency.requires_transfer,
            source_id: adjacency.source_id,
            source_version: adjacency.source_version,
            attributes: adjacency.attributes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StationHopDiagnostic {
    pub from_line_id: String,
    pub to_line_id: String,
    pub adjacency_kind: String,
    pub station_hop_count: Option<u32>,
    pub interchange_station_id: Option<String>,
    pub requires_transfer: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterchangeDiagnostic {
    pub interchange_station_id: String,
    pub from_line_id: String,
    pub to_line_ids: Vec<String>,
    pub adjacency_kinds: Vec<String>,
    /// True when at least one observed edge in the group requires a transfer.
    pub requires_transfer: bool,
    pub minimum_station_hop_count: Option<u32>,
}

#[derive(Debug, Default)]
struct InterchangeAccumulator {
    to_line_ids: BTreeSet<String>,
    adjacency_kinds: BTreeSet<String>,
    requires_transfer: bool,
    minimum_station_hop_count: Option<u32>,
}

fn sort_geo_graph_edges(edges: &mut [GeoGraphEdge]) {
    edges.sort_by(|left, right| {
        left.adjacency_kind
            .cmp(&right.adjacency_kind)
            .then_with(|| {
                compare_optional_f64_nulls_last(left.distance_meters, right.distance_meters)
            })
            .then_with(|| left.to_area_id.cmp(&right.to_area_id))
            .then_with(|| {
                compare_optional_str_nulls_last(
                    left.area_cluster_id.as_deref(),
                    right.area_cluster_id.as_deref(),
                )
            })
            .then_with(|| {
                compare_optional_str_nulls_last(
                    left.source_id.as_deref(),
                    right.source_id.as_deref(),
                )
            })
            .then_with(|| {
                compare_optional_str_nulls_last(
                    left.source_version.as_deref(),
                    right.source_version.as_deref(),
                )
            })
    });
}

fn sort_line_graph_edges(edges: &mut [LineGraphEdge]) {
    edges.sort_by(|left, right| {
        left.adjacency_kind
            .cmp(&right.adjacency_kind)
            .then_with(|| {
                compare_optional_u32_nulls_last(left.station_hop_count, right.station_hop_count)
            })
            .then_with(|| {
                compare_optional_str_nulls_last(
                    left.interchange_station_id.as_deref(),
                    right.interchange_station_id.as_deref(),
                )
            })
            .then_with(|| left.to_line_id.cmp(&right.to_line_id))
            .then_with(|| left.requires_transfer.cmp(&right.requires_transfer))
            .then_with(|| {
                compare_optional_str_nulls_last(
                    left.source_id.as_deref(),
                    right.source_id.as_deref(),
                )
            })
            .then_with(|| {
                compare_optional_str_nulls_last(
                    left.source_version.as_deref(),
                    right.source_version.as_deref(),
                )
            })
    });
}

fn compare_optional_f64_nulls_last(left: Option<f64>, right: Option<f64>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.total_cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn compare_optional_u32_nulls_last(left: Option<u32>, right: Option<u32>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn compare_optional_str_nulls_last(left: Option<&str>, right: Option<&str>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn min_optional_u32(left: Option<u32>, right: Option<u32>) -> Option<u32> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}
