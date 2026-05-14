use std::collections::BTreeSet;

use domain::{RankingQuery, SchoolStationLink, Station};
use geo::haversine_meters;
use serde_json::{json, Value};

const INTERCHANGE_DISTANCE_THRESHOLD_METERS: f64 = 250.0;

fn non_empty_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn area_hint_is_usable(context: &context::RankingContext) -> bool {
    context.area.is_some()
        && !context
            .warnings
            .iter()
            .any(|warning| warning.code == "station_area_conflict")
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CandidateGraphExpansion {
    line: Option<LineGraphExpansion>,
    area: Option<AreaGraphExpansion>,
}

impl CandidateGraphExpansion {
    pub fn from_parts(line: Option<LineGraphExpansion>, area: Option<AreaGraphExpansion>) -> Self {
        Self { line, area }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    fn line_for_origin(&self, origin_line_id: Option<&str>) -> Option<&LineGraphExpansion> {
        let origin_line_id = origin_line_id?;
        self.line
            .as_ref()
            .filter(|line| line.origin_line_id == origin_line_id)
    }

    fn area_for_origin(&self, origin_area_id: Option<&str>) -> Option<&AreaGraphExpansion> {
        let origin_area_id = origin_area_id?;
        self.area
            .as_ref()
            .filter(|area| area.origin_area_id == origin_area_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineGraphExpansion {
    origin_line_id: String,
    adjacent_line_ids: BTreeSet<String>,
}

impl LineGraphExpansion {
    pub fn new(
        origin_line_id: impl Into<String>,
        adjacent_line_ids: impl IntoIterator<Item = String>,
    ) -> Option<Self> {
        let origin_line_id = origin_line_id.into();
        let origin_line_id = origin_line_id.trim();
        if origin_line_id.is_empty() {
            return None;
        }

        let adjacent_line_ids = adjacent_line_ids
            .into_iter()
            .filter_map(|line_id| {
                let line_id = line_id.trim();
                (!line_id.is_empty() && line_id != origin_line_id).then(|| line_id.to_string())
            })
            .collect::<BTreeSet<_>>();
        if adjacent_line_ids.is_empty() {
            return None;
        }

        Some(Self {
            origin_line_id: origin_line_id.to_string(),
            adjacent_line_ids,
        })
    }

    pub fn origin_line_id(&self) -> &str {
        &self.origin_line_id
    }

    pub fn adjacent_line_ids(&self) -> impl Iterator<Item = &str> {
        self.adjacent_line_ids.iter().map(String::as_str)
    }

    fn contains_adjacent_line(&self, line_id: &str) -> bool {
        self.adjacent_line_ids.contains(line_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AreaGraphExpansion {
    origin_area_id: String,
    adjacent_area_ids: BTreeSet<String>,
}

impl AreaGraphExpansion {
    pub fn new(
        origin_area_id: impl Into<String>,
        adjacent_area_ids: impl IntoIterator<Item = String>,
    ) -> Option<Self> {
        let origin_area_id = origin_area_id.into();
        let origin_area_id = origin_area_id.trim();
        if origin_area_id.is_empty() {
            return None;
        }

        let adjacent_area_ids = adjacent_area_ids
            .into_iter()
            .filter_map(|area_id| {
                let area_id = area_id.trim();
                (!area_id.is_empty() && area_id != origin_area_id).then(|| area_id.to_string())
            })
            .collect::<BTreeSet<_>>();
        if adjacent_area_ids.is_empty() {
            return None;
        }

        Some(Self {
            origin_area_id: origin_area_id.to_string(),
            adjacent_area_ids,
        })
    }

    pub fn origin_area_id(&self) -> &str {
        &self.origin_area_id
    }

    pub fn adjacent_area_ids(&self) -> impl Iterator<Item = &str> {
        self.adjacent_area_ids.iter().map(String::as_str)
    }

    fn contains_adjacent_area(&self, area_id: &str) -> bool {
        self.adjacent_area_ids.contains(area_id)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CandidateGraph {
    target_station_id: String,
    target_station_name: String,
    target_line_id: Option<String>,
    target_line_name: String,
    target_area_id: Option<String>,
    line_expansion: Option<LineGraphExpansion>,
    area_expansion: Option<AreaGraphExpansion>,
    neighbor_distance_cap_meters: f64,
    neighbor_max_hops: u8,
}

impl CandidateGraph {
    #[cfg(test)]
    pub(crate) fn new(
        query: &RankingQuery,
        target_station: &Station,
        neighbor_distance_cap_meters: f64,
        neighbor_max_hops: u8,
    ) -> Self {
        Self::new_with_expansion(
            query,
            target_station,
            neighbor_distance_cap_meters,
            neighbor_max_hops,
            &CandidateGraphExpansion::empty(),
        )
    }

    pub(crate) fn new_with_expansion(
        query: &RankingQuery,
        target_station: &Station,
        neighbor_distance_cap_meters: f64,
        neighbor_max_hops: u8,
        expansion: &CandidateGraphExpansion,
    ) -> Self {
        let context = query.context.as_ref();
        let target_line_id = match context.and_then(|context| context.line.as_ref()) {
            Some(line) => non_empty_string(line.line_id.as_deref()),
            None => non_empty_string(target_station.line_id.as_deref()),
        };
        let target_line_name = context
            .and_then(|context| context.line_name())
            .unwrap_or(target_station.line_name.as_str())
            .to_string();
        let target_area_id = non_empty_string(target_station.area_id.as_deref()).or_else(|| {
            context
                .filter(|context| area_hint_is_usable(context))
                .and(expansion.area.as_ref())
                .map(|area| area.origin_area_id.clone())
        });
        let line_expansion = expansion
            .line_for_origin(target_line_id.as_deref())
            .cloned();
        let area_expansion = expansion
            .area_for_origin(target_area_id.as_deref())
            .cloned();

        Self {
            target_station_id: target_station.id.clone(),
            target_station_name: target_station.name.clone(),
            target_line_id,
            target_line_name,
            target_area_id,
            line_expansion,
            area_expansion,
            neighbor_distance_cap_meters,
            neighbor_max_hops,
        }
    }

    pub(crate) fn evidence(
        &self,
        target_station: &Station,
        candidate_station: &Station,
        link: &SchoolStationLink,
    ) -> CandidateGraphEvidence {
        let station_distance_meters = haversine_meters(
            target_station.latitude,
            target_station.longitude,
            candidate_station.latitude,
            candidate_station.longitude,
        );
        let line_match = self.line_evidence(candidate_station, link);
        let area_match = self.area_evidence(candidate_station);
        let interchange_like = station_distance_meters <= INTERCHANGE_DISTANCE_THRESHOLD_METERS
            && link.station_id != self.target_station_id
            && self
                .target_station_name
                .eq_ignore_ascii_case(candidate_station.name.as_str())
            && !line_match.is_same_line;

        CandidateGraphEvidence {
            line_match,
            area_match,
            station_distance_meters,
            within_neighbor_distance_cap: station_distance_meters
                <= self.neighbor_distance_cap_meters,
            within_neighbor_hops: link.hop_distance <= self.neighbor_max_hops,
            hop_distance: link.hop_distance,
            interchange_like,
        }
    }

    pub(crate) fn line_evidence(
        &self,
        candidate_station: &Station,
        link: &SchoolStationLink,
    ) -> LineGraphEvidence {
        let match_kind =
            match self.target_line_id.as_deref() {
                Some(target_line_id) => {
                    if candidate_station
                        .line_id
                        .as_deref()
                        .is_some_and(|candidate_line_id| candidate_line_id == target_line_id)
                    {
                        LineMatchKind::LineId
                    } else if candidate_station.line_id.as_deref().is_some_and(
                        |candidate_line_id| {
                            self.line_expansion
                                .as_ref()
                                .is_some_and(|line| line.contains_adjacent_line(candidate_line_id))
                        },
                    ) {
                        LineMatchKind::LineGraphAdjacentLineId
                    } else if candidate_station.line_id.is_none()
                        && link.line_name == self.target_line_name
                    {
                        LineMatchKind::LineNameFallback
                    } else {
                        LineMatchKind::None
                    }
                }
                None if link.line_name == self.target_line_name => LineMatchKind::LineName,
                None => LineMatchKind::None,
            };

        LineGraphEvidence {
            is_same_line: match_kind != LineMatchKind::None,
            match_kind,
            target_line_id: self.target_line_id.clone(),
            candidate_line_id: candidate_station.line_id.clone(),
            target_line_name: self.target_line_name.clone(),
            candidate_line_name: link.line_name.clone(),
        }
    }

    fn area_evidence(&self, candidate_station: &Station) -> AreaGraphEvidence {
        let is_adjacent_area =
            candidate_station
                .area_id
                .as_deref()
                .is_some_and(|candidate_area_id| {
                    self.area_expansion
                        .as_ref()
                        .is_some_and(|area| area.contains_adjacent_area(candidate_area_id))
                });

        AreaGraphEvidence {
            is_adjacent_area,
            target_area_id: self.target_area_id.clone(),
            candidate_area_id: candidate_station.area_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CandidateGraphEvidence {
    pub(crate) line_match: LineGraphEvidence,
    pub(crate) area_match: AreaGraphEvidence,
    pub(crate) station_distance_meters: f64,
    pub(crate) within_neighbor_distance_cap: bool,
    pub(crate) within_neighbor_hops: bool,
    pub(crate) hop_distance: u8,
    pub(crate) interchange_like: bool,
}

impl CandidateGraphEvidence {
    pub(crate) fn counts_as_same_line_candidate(&self) -> bool {
        if !self.line_match.is_same_line {
            return false;
        }

        !matches!(
            self.line_match.match_kind,
            LineMatchKind::LineGraphAdjacentLineId
        ) || (self.within_neighbor_distance_cap && self.within_neighbor_hops)
    }

    pub(crate) fn line_details(&self) -> Value {
        json!({
            "match_kind": self.line_match.match_kind.as_str(),
            "target_line_id": self.line_match.target_line_id.as_deref(),
            "candidate_line_id": self.line_match.candidate_line_id.as_deref(),
            "target_line_name": self.line_match.target_line_name.as_str(),
            "candidate_line_name": self.line_match.candidate_line_name.as_str(),
            "hop_distance": self.hop_distance,
            "within_neighbor_distance_cap": self.within_neighbor_distance_cap,
            "within_neighbor_hops": self.within_neighbor_hops,
        })
    }

    pub(crate) fn route_details(&self) -> Value {
        json!({
            "station_distance_meters": self.station_distance_meters.round() as u64,
            "distance_bucket": self.distance_bucket(),
            "hop_distance": self.hop_distance,
            "within_neighbor_distance_cap": self.within_neighbor_distance_cap,
            "within_neighbor_hops": self.within_neighbor_hops,
            "interchange_like": self.interchange_like,
            "area_graph": {
                "is_adjacent_area": self.area_match.is_adjacent_area,
                "target_area_id": self.area_match.target_area_id.as_deref(),
                "candidate_area_id": self.area_match.candidate_area_id.as_deref(),
            },
            "line_match": {
                "is_same_line": self.line_match.is_same_line,
                "match_kind": self.line_match.match_kind.as_str(),
                "target_line_name": self.line_match.target_line_name.as_str(),
                "candidate_line_name": self.line_match.candidate_line_name.as_str(),
            }
        })
    }

    fn distance_bucket(&self) -> &'static str {
        if self.station_distance_meters <= INTERCHANGE_DISTANCE_THRESHOLD_METERS {
            "very_near_station"
        } else if self.station_distance_meters <= 1_000.0 {
            "walkable_neighbor"
        } else if self.within_neighbor_distance_cap {
            "nearby_area"
        } else {
            "outside_neighbor_cap"
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LineGraphEvidence {
    pub(crate) is_same_line: bool,
    pub(crate) match_kind: LineMatchKind,
    pub(crate) target_line_id: Option<String>,
    pub(crate) candidate_line_id: Option<String>,
    pub(crate) target_line_name: String,
    pub(crate) candidate_line_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AreaGraphEvidence {
    pub(crate) is_adjacent_area: bool,
    pub(crate) target_area_id: Option<String>,
    pub(crate) candidate_area_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LineMatchKind {
    LineId,
    LineGraphAdjacentLineId,
    LineName,
    LineNameFallback,
    None,
}

impl LineMatchKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::LineId => "line_id",
            Self::LineGraphAdjacentLineId => "line_graph_adjacent_line_id",
            Self::LineName => "line_name",
            Self::LineNameFallback => "line_name_fallback",
            Self::None => "none",
        }
    }
}

#[cfg(test)]
mod tests {
    use domain::{PlacementKind, RankingQuery};

    use super::{
        AreaGraphExpansion, CandidateGraph, CandidateGraphExpansion, LineGraphExpansion,
        LineMatchKind,
    };
    use crate::test_utils::query;

    fn station(id: &str, name: &str, line_name: &str, line_id: Option<&str>) -> domain::Station {
        station_at(id, name, line_name, line_id, 35.0, 139.0)
    }

    fn station_in_area(
        id: &str,
        name: &str,
        line_name: &str,
        line_id: Option<&str>,
        area_id: Option<&str>,
    ) -> domain::Station {
        domain::Station {
            area_id: area_id.map(str::to_string),
            ..station(id, name, line_name, line_id)
        }
    }

    fn station_at(
        id: &str,
        name: &str,
        line_name: &str,
        line_id: Option<&str>,
        latitude: f64,
        longitude: f64,
    ) -> domain::Station {
        domain::Station {
            id: id.to_string(),
            name: name.to_string(),
            line_name: line_name.to_string(),
            line_id: line_id.map(str::to_string),
            area_id: None,
            latitude,
            longitude,
        }
    }

    fn link(station_id: &str, line_name: &str, hop_distance: u8) -> domain::SchoolStationLink {
        domain::SchoolStationLink {
            school_id: "school".to_string(),
            station_id: station_id.to_string(),
            walking_minutes: 5,
            distance_meters: 400,
            hop_distance,
            line_name: line_name.to_string(),
        }
    }

    #[test]
    fn line_id_match_wins_over_shared_line_name() {
        let target = station("st_target", "Target", "Shared Line", Some("line_target"));
        let candidate = station(
            "st_candidate",
            "Candidate",
            "Shared Line",
            Some("line_target"),
        );
        let graph = CandidateGraph::new(
            &query("st_target", PlacementKind::Search),
            &target,
            2_500.0,
            3,
        );

        let evidence = graph.evidence(&target, &candidate, &link("st_candidate", "Shared Line", 2));

        assert!(evidence.line_match.is_same_line);
        assert_eq!(evidence.line_match.match_kind, LineMatchKind::LineId);
        assert!(evidence.within_neighbor_hops);
    }

    #[test]
    fn line_id_context_rejects_same_name_different_line_id() {
        let target = station("st_target", "Target", "Shared Line", Some("line_target"));
        let candidate = station(
            "st_candidate",
            "Candidate",
            "Shared Line",
            Some("line_other"),
        );
        let graph = CandidateGraph::new(
            &query("st_target", PlacementKind::Search),
            &target,
            2_500.0,
            3,
        );

        let evidence = graph.evidence(&target, &candidate, &link("st_candidate", "Shared Line", 1));

        assert!(!evidence.line_match.is_same_line);
        assert_eq!(evidence.line_match.match_kind, LineMatchKind::None);
    }

    #[test]
    fn explicit_line_context_can_fall_back_to_line_name_for_unidentified_stations() {
        let target = station("st_target", "Target", "Shared Line", Some("line_target"));
        let candidate = station("st_candidate", "Candidate", "Shared Line", None);
        let mut request = RankingQuery {
            context: None,
            ..query("st_target", PlacementKind::Search)
        };
        request.context = Some(context::RankingContext {
            context_source: context::ContextSource::RequestLine,
            confidence: 0.95,
            area: None,
            line: Some(context::LineContext {
                line_id: Some("line_target".to_string()),
                line_name: "Shared Line".to_string(),
                operator_name: None,
            }),
            station: None,
            privacy_level: context::PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        });
        let graph = CandidateGraph::new(&request, &target, 2_500.0, 3);

        let evidence = graph.evidence(&target, &candidate, &link("st_candidate", "Shared Line", 1));

        assert!(evidence.line_match.is_same_line);
        assert_eq!(
            evidence.line_match.match_kind,
            LineMatchKind::LineNameFallback
        );
    }

    #[test]
    fn explicit_line_name_context_does_not_inherit_target_station_line_id() {
        let target = station("st_target", "Target", "Target Line", Some("line_target"));
        let candidate = station(
            "st_candidate",
            "Candidate",
            "Target Line",
            Some("line_target"),
        );
        let mut request = RankingQuery {
            context: None,
            ..query("st_target", PlacementKind::Search)
        };
        request.context = Some(context::RankingContext {
            context_source: context::ContextSource::RequestLine,
            confidence: 0.95,
            area: None,
            line: Some(context::LineContext {
                line_id: None,
                line_name: "Other Line".to_string(),
                operator_name: None,
            }),
            station: None,
            privacy_level: context::PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        });
        let graph = CandidateGraph::new(&request, &target, 2_500.0, 3);

        let evidence = graph.evidence(&target, &candidate, &link("st_candidate", "Target Line", 1));

        assert!(!evidence.line_match.is_same_line);
        assert_eq!(evidence.line_match.match_kind, LineMatchKind::None);
    }

    #[test]
    fn interchange_like_requires_nearby_same_name_station() {
        let target = station("st_target", "Shared", "Target Line", Some("line_target"));
        let nearby = station_at(
            "st_nearby",
            "Shared",
            "Other Line",
            Some("line_other"),
            35.0,
            139.0,
        );
        let far = station_at(
            "st_far",
            "Shared",
            "Other Line",
            Some("line_other"),
            36.0,
            140.0,
        );
        let graph = CandidateGraph::new(
            &query("st_target", PlacementKind::Search),
            &target,
            2_500.0,
            3,
        );

        let nearby_evidence = graph.evidence(&target, &nearby, &link("st_nearby", "Other Line", 1));
        let far_evidence = graph.evidence(&target, &far, &link("st_far", "Other Line", 1));

        assert!(nearby_evidence.interchange_like);
        assert!(!far_evidence.interchange_like);
    }

    #[test]
    fn line_graph_expansion_accepts_adjacent_line_id_when_origin_matches() {
        let target = station("st_target", "Target", "Target Line", Some("line_target"));
        let candidate = station(
            "st_adjacent",
            "Adjacent",
            "Adjacent Line",
            Some("line_adjacent"),
        );
        let expansion = CandidateGraphExpansion::from_parts(
            LineGraphExpansion::new("line_target", vec!["line_adjacent".to_string()]),
            None,
        );
        let graph = CandidateGraph::new_with_expansion(
            &query("st_target", PlacementKind::Search),
            &target,
            2_500.0,
            3,
            &expansion,
        );

        let evidence = graph.evidence(
            &target,
            &candidate,
            &link("st_adjacent", "Adjacent Line", 1),
        );

        assert!(evidence.line_match.is_same_line);
        assert_eq!(
            evidence.line_match.match_kind,
            LineMatchKind::LineGraphAdjacentLineId
        );
    }

    #[test]
    fn graph_expansion_ignores_origin_mismatch() {
        let target = station_in_area(
            "st_target",
            "Target",
            "Target Line",
            Some("line_target"),
            Some("area_target"),
        );
        let candidate = station_in_area(
            "st_adjacent",
            "Adjacent",
            "Adjacent Line",
            Some("line_adjacent"),
            Some("area_adjacent"),
        );
        let expansion = CandidateGraphExpansion::from_parts(
            LineGraphExpansion::new("line_other", vec!["line_adjacent".to_string()]),
            AreaGraphExpansion::new("area_other", vec!["area_adjacent".to_string()]),
        );
        let graph = CandidateGraph::new_with_expansion(
            &query("st_target", PlacementKind::Search),
            &target,
            2_500.0,
            3,
            &expansion,
        );

        let evidence = graph.evidence(
            &target,
            &candidate,
            &link("st_adjacent", "Adjacent Line", 1),
        );

        assert_eq!(evidence.line_match.match_kind, LineMatchKind::None);
        assert!(!evidence.area_match.is_adjacent_area);
    }

    #[test]
    fn area_graph_expansion_marks_adjacent_area_when_origin_matches() {
        let target = station_in_area(
            "st_target",
            "Target",
            "Target Line",
            Some("line_target"),
            Some("area_target"),
        );
        let candidate = station_in_area(
            "st_neighbor",
            "Neighbor",
            "Other Line",
            Some("line_other"),
            Some("area_neighbor"),
        );
        let expansion = CandidateGraphExpansion::from_parts(
            None,
            AreaGraphExpansion::new("area_target", vec!["area_neighbor".to_string()]),
        );
        let graph = CandidateGraph::new_with_expansion(
            &query("st_target", PlacementKind::Search),
            &target,
            2_500.0,
            3,
            &expansion,
        );

        let evidence = graph.evidence(&target, &candidate, &link("st_neighbor", "Other Line", 1));

        assert!(evidence.area_match.is_adjacent_area);
    }

    #[test]
    fn area_graph_expansion_uses_context_origin_when_target_area_is_missing() {
        let target = station_in_area(
            "st_target",
            "Target",
            "Target Line",
            Some("line_target"),
            None,
        );
        let candidate = station_in_area(
            "st_neighbor",
            "Neighbor",
            "Other Line",
            Some("line_other"),
            Some("area_neighbor"),
        );
        let mut request = query("st_target", PlacementKind::Search);
        request.context = Some(context::RankingContext {
            context_source: context::ContextSource::RequestArea,
            confidence: 0.95,
            area: Some(context::AreaContext {
                country: "JP".to_string(),
                prefecture_code: None,
                prefecture_name: Some("Tokyo".to_string()),
                city_code: None,
                city_name: Some("Target Ward".to_string()),
            }),
            line: None,
            station: None,
            privacy_level: context::PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        });
        let expansion = CandidateGraphExpansion::from_parts(
            None,
            AreaGraphExpansion::new("area_context", vec!["area_neighbor".to_string()]),
        );
        let graph = CandidateGraph::new_with_expansion(&request, &target, 2_500.0, 3, &expansion);

        let evidence = graph.evidence(&target, &candidate, &link("st_neighbor", "Other Line", 1));

        assert_eq!(
            evidence.area_match.target_area_id.as_deref(),
            Some("area_context")
        );
        assert!(evidence.area_match.is_adjacent_area);
    }

    #[test]
    fn area_graph_expansion_ignores_context_origin_after_area_conflict() {
        let target = station_in_area(
            "st_target",
            "Target",
            "Target Line",
            Some("line_target"),
            None,
        );
        let candidate = station_in_area(
            "st_neighbor",
            "Neighbor",
            "Other Line",
            Some("line_other"),
            Some("area_neighbor"),
        );
        let mut request = query("st_target", PlacementKind::Search);
        request.context = Some(context::RankingContext {
            context_source: context::ContextSource::RequestStation,
            confidence: 0.95,
            area: Some(context::AreaContext {
                country: "JP".to_string(),
                prefecture_code: None,
                prefecture_name: Some("Tokyo".to_string()),
                city_code: None,
                city_name: Some("Target Ward".to_string()),
            }),
            line: None,
            station: Some(context::StationContext {
                station_id: "st_target".to_string(),
                station_name: "Target".to_string(),
            }),
            privacy_level: context::PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: vec![context::ContextWarning {
                code: "station_area_conflict".to_string(),
                message: "station area took precedence".to_string(),
            }],
        });
        let expansion = CandidateGraphExpansion::from_parts(
            None,
            AreaGraphExpansion::new("area_context", vec!["area_neighbor".to_string()]),
        );
        let graph = CandidateGraph::new_with_expansion(&request, &target, 2_500.0, 3, &expansion);

        let evidence = graph.evidence(&target, &candidate, &link("st_neighbor", "Other Line", 1));

        assert!(evidence.area_match.target_area_id.is_none());
        assert!(!evidence.area_match.is_adjacent_area);
    }
}
