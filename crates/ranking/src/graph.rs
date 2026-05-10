use domain::{RankingQuery, SchoolStationLink, Station};
use geo::haversine_meters;
use serde_json::{json, Value};

const INTERCHANGE_DISTANCE_THRESHOLD_METERS: f64 = 250.0;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CandidateGraph {
    target_station_id: String,
    target_station_name: String,
    target_line_id: Option<String>,
    target_line_name: String,
    neighbor_distance_cap_meters: f64,
    neighbor_max_hops: u8,
}

impl CandidateGraph {
    pub(crate) fn new(
        query: &RankingQuery,
        target_station: &Station,
        neighbor_distance_cap_meters: f64,
        neighbor_max_hops: u8,
    ) -> Self {
        let context = query.context.as_ref();
        let target_line_id = match context.and_then(|context| context.line.as_ref()) {
            Some(line) => line.line_id.clone(),
            None => target_station.line_id.clone(),
        };
        let target_line_name = context
            .and_then(|context| context.line_name())
            .unwrap_or(target_station.line_name.as_str())
            .to_string();

        Self {
            target_station_id: target_station.id.clone(),
            target_station_name: target_station.name.clone(),
            target_line_id,
            target_line_name,
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
        let line_match = self.line_match(candidate_station, link);
        let interchange_like = station_distance_meters <= INTERCHANGE_DISTANCE_THRESHOLD_METERS
            && link.station_id != self.target_station_id
            && self
                .target_station_name
                .eq_ignore_ascii_case(candidate_station.name.as_str())
            && !line_match.is_same_line;

        CandidateGraphEvidence {
            line_match,
            station_distance_meters,
            within_neighbor_distance_cap: station_distance_meters
                <= self.neighbor_distance_cap_meters,
            within_neighbor_hops: link.hop_distance <= self.neighbor_max_hops,
            hop_distance: link.hop_distance,
            interchange_like,
        }
    }

    fn line_match(
        &self,
        candidate_station: &Station,
        link: &SchoolStationLink,
    ) -> LineGraphEvidence {
        let match_kind = match self.target_line_id.as_deref() {
            Some(target_line_id) => {
                if candidate_station
                    .line_id
                    .as_deref()
                    .is_some_and(|candidate_line_id| candidate_line_id == target_line_id)
                {
                    LineMatchKind::LineId
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
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CandidateGraphEvidence {
    pub(crate) line_match: LineGraphEvidence,
    pub(crate) station_distance_meters: f64,
    pub(crate) within_neighbor_distance_cap: bool,
    pub(crate) within_neighbor_hops: bool,
    pub(crate) hop_distance: u8,
    pub(crate) interchange_like: bool,
}

impl CandidateGraphEvidence {
    pub(crate) fn line_details(&self) -> Value {
        json!({
            "match_kind": self.line_match.match_kind.as_str(),
            "target_line_id": self.line_match.target_line_id.as_deref(),
            "candidate_line_id": self.line_match.candidate_line_id.as_deref(),
            "target_line_name": self.line_match.target_line_name.as_str(),
            "candidate_line_name": self.line_match.candidate_line_name.as_str(),
            "hop_distance": self.hop_distance,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LineMatchKind {
    LineId,
    LineName,
    LineNameFallback,
    None,
}

impl LineMatchKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::LineId => "line_id",
            Self::LineName => "line_name",
            Self::LineNameFallback => "line_name_fallback",
            Self::None => "none",
        }
    }
}

#[cfg(test)]
mod tests {
    use domain::{PlacementKind, RankingQuery};

    use super::{CandidateGraph, LineMatchKind};
    use crate::test_utils::query;

    fn station(id: &str, name: &str, line_name: &str, line_id: Option<&str>) -> domain::Station {
        station_at(id, name, line_name, line_id, 35.0, 139.0)
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
}
