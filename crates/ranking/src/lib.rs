mod diversity;
mod explanation;
mod fallback;
mod feature;
mod planning;
mod profile;
mod scoring;
#[cfg(test)]
mod test_utils;

use config::RankingProfiles;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RankingError {
    #[error("unknown station: {0}")]
    UnknownStation(String),
    #[error("no candidates available for station: {0}")]
    NoCandidates(String),
}

#[derive(Debug, Clone)]
pub struct RankingEngine {
    profiles: RankingProfiles,
    algorithm_version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReasonCatalogEntry {
    pub feature: &'static str,
    pub reason_code: &'static str,
    pub label: &'static str,
}

pub fn reason_catalog() -> &'static [ReasonCatalogEntry] {
    feature::reason_catalog()
}

pub fn reason_catalog_entry(feature: &str) -> Option<&'static ReasonCatalogEntry> {
    feature::reason_catalog_entry(feature)
}

#[cfg(test)]
mod tests {
    use config::RankingProfiles;
    use context::{
        AreaContext, ContextSource, ContextWarning, LineContext, PrivacyLevel, RankingContext,
        StationContext,
    };
    use domain::{
        FallbackStage, PlacementKind, PopularitySnapshot, RankingDataset, RankingQuery, School,
        SchoolStationLink, Station, UserAffinitySnapshot,
    };
    use test_support::load_fixture_dataset;

    use super::RankingEngine;
    use crate::test_utils::{config_root, fixture_root, query};

    fn request_area_context(
        city_name: Option<&str>,
        prefecture_name: Option<&str>,
    ) -> RankingContext {
        RankingContext {
            context_source: ContextSource::RequestArea,
            confidence: 0.95,
            area: Some(AreaContext {
                country: "JP".to_string(),
                prefecture_code: None,
                prefecture_name: prefecture_name.map(str::to_string),
                city_code: None,
                city_name: city_name.map(str::to_string),
            }),
            line: None,
            station: None,
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        }
    }

    #[test]
    fn strict_mode_returns_direct_matches() {
        let dataset = load_fixture_dataset(fixture_root()).expect("fixture dataset");
        let profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        let engine = RankingEngine::new(profiles.clone(), "phase5-test");
        let result = engine
            .recommend(&dataset, &query("st_tamachi", PlacementKind::Search))
            .expect("recommendation result");

        assert_eq!(result.fallback_stage, FallbackStage::StrictStation);
        assert_eq!(result.items[0].content_kind.as_str(), "school");
        assert!(result.items[0]
            .score_breakdown
            .iter()
            .any(|component| component.feature == "direct_station_bonus"));
        assert_eq!(result.profile_version, profiles.profile_version);
    }

    #[test]
    fn neighbor_mode_expands_when_direct_matches_are_few() {
        let dataset = load_fixture_dataset(fixture_root()).expect("fixture dataset");
        let profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        let engine = RankingEngine::new(profiles, "phase5-test");
        let result = engine
            .recommend(&dataset, &query("st_shinbashi", PlacementKind::Search))
            .expect("recommendation result");

        assert_eq!(result.fallback_stage, FallbackStage::SameLine);
        assert!(result
            .items
            .iter()
            .all(|item| item.line_name == "JR Yamanote Line"));
    }

    #[test]
    fn underfilled_scoped_stages_use_safe_global_when_available() {
        let dataset = RankingDataset {
            schools: vec![
                School {
                    id: "school_strict".to_string(),
                    name: "Strict School".to_string(),
                    area: "Minato".to_string(),
                    prefecture_name: Some("Tokyo".to_string()),
                    school_type: "high_school".to_string(),
                    group_id: "group_strict".to_string(),
                },
                School {
                    id: "school_line_a".to_string(),
                    name: "Line A".to_string(),
                    area: "Shinagawa".to_string(),
                    prefecture_name: Some("Tokyo".to_string()),
                    school_type: "high_school".to_string(),
                    group_id: "group_line_a".to_string(),
                },
                School {
                    id: "school_line_b".to_string(),
                    name: "Line B".to_string(),
                    area: "Shibuya".to_string(),
                    prefecture_name: Some("Tokyo".to_string()),
                    school_type: "high_school".to_string(),
                    group_id: "group_line_b".to_string(),
                },
                School {
                    id: "school_global".to_string(),
                    name: "Global School".to_string(),
                    area: "Naha".to_string(),
                    prefecture_name: Some("Okinawa".to_string()),
                    school_type: "high_school".to_string(),
                    group_id: "group_global".to_string(),
                },
            ],
            events: Vec::new(),
            stations: vec![
                Station {
                    id: "st_target".to_string(),
                    name: "Target".to_string(),
                    line_name: "Target Line".to_string(),
                    line_id: None,
                    latitude: 35.0,
                    longitude: 139.0,
                },
                Station {
                    id: "st_line_a".to_string(),
                    name: "Line A Station".to_string(),
                    line_name: "Target Line".to_string(),
                    line_id: None,
                    latitude: 35.01,
                    longitude: 139.01,
                },
                Station {
                    id: "st_line_b".to_string(),
                    name: "Line B Station".to_string(),
                    line_name: "Target Line".to_string(),
                    line_id: None,
                    latitude: 35.02,
                    longitude: 139.02,
                },
                Station {
                    id: "st_global".to_string(),
                    name: "Global Station".to_string(),
                    line_name: "Far Line".to_string(),
                    line_id: None,
                    latitude: 26.21,
                    longitude: 127.68,
                },
            ],
            school_station_links: vec![
                SchoolStationLink {
                    school_id: "school_strict".to_string(),
                    station_id: "st_target".to_string(),
                    walking_minutes: 5,
                    distance_meters: 400,
                    hop_distance: 0,
                    line_name: "Target Line".to_string(),
                },
                SchoolStationLink {
                    school_id: "school_line_a".to_string(),
                    station_id: "st_line_a".to_string(),
                    walking_minutes: 7,
                    distance_meters: 700,
                    hop_distance: 1,
                    line_name: "Target Line".to_string(),
                },
                SchoolStationLink {
                    school_id: "school_line_b".to_string(),
                    station_id: "st_line_b".to_string(),
                    walking_minutes: 9,
                    distance_meters: 900,
                    hop_distance: 2,
                    line_name: "Target Line".to_string(),
                },
                SchoolStationLink {
                    school_id: "school_global".to_string(),
                    station_id: "st_global".to_string(),
                    walking_minutes: 6,
                    distance_meters: 500,
                    hop_distance: 0,
                    line_name: "Far Line".to_string(),
                },
            ],
            popularity_snapshots: Vec::new(),
            user_affinity_snapshots: Vec::new(),
            area_affinity_snapshots: Vec::new(),
        };
        let mut profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        profiles.schools.strict_min_candidates = 4;
        profiles.fallback.min_results = 4;
        let engine = RankingEngine::new(profiles, "v020-insufficient-strict-test");

        let result = engine
            .recommend(&dataset, &query("st_target", PlacementKind::Search))
            .expect("recommendation result");

        assert_eq!(result.candidate_counts.get("strict_station"), Some(&1));
        assert_eq!(result.candidate_counts.get("same_line"), Some(&3));
        assert_eq!(result.candidate_counts.get("safe_global_popular"), Some(&4));
        assert_eq!(result.fallback_stage, FallbackStage::SafeGlobalPopular);
        assert_eq!(result.items.len(), 3);
    }

    #[test]
    fn area_only_context_uses_same_city_candidates() {
        let dataset = load_fixture_dataset(fixture_root()).expect("fixture dataset");
        let profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        let engine = RankingEngine::new(profiles, "v020-context-test");
        let mut query = query("st_tamachi", PlacementKind::Search);
        query.context = Some(request_area_context(Some("Minato"), Some("Tokyo")));

        let result = engine
            .recommend(&dataset, &query)
            .expect("recommendation result");

        assert_eq!(result.fallback_stage, FallbackStage::SameCity);
        assert!(result
            .candidate_counts
            .get("same_city")
            .is_some_and(|count| *count >= 2));
    }

    #[test]
    fn line_context_uses_line_id_before_line_name() {
        let dataset = RankingDataset {
            schools: vec![
                School {
                    id: "school_target_line".to_string(),
                    name: "Target Line School".to_string(),
                    area: "Minato".to_string(),
                    prefecture_name: Some("Tokyo".to_string()),
                    school_type: "high_school".to_string(),
                    group_id: "group_target_line".to_string(),
                },
                School {
                    id: "school_other_line".to_string(),
                    name: "Other Line School".to_string(),
                    area: "Shibuya".to_string(),
                    prefecture_name: Some("Tokyo".to_string()),
                    school_type: "high_school".to_string(),
                    group_id: "group_other_line".to_string(),
                },
            ],
            events: Vec::new(),
            stations: vec![
                Station {
                    id: "st_target".to_string(),
                    name: "Target".to_string(),
                    line_name: "Shared Line".to_string(),
                    line_id: Some("line_target".to_string()),
                    latitude: 35.0,
                    longitude: 139.0,
                },
                Station {
                    id: "st_target_line".to_string(),
                    name: "Target Line Station".to_string(),
                    line_name: "Shared Line".to_string(),
                    line_id: Some("line_target".to_string()),
                    latitude: 35.01,
                    longitude: 139.01,
                },
                Station {
                    id: "st_other_line".to_string(),
                    name: "Other Line Station".to_string(),
                    line_name: "Shared Line".to_string(),
                    line_id: Some("line_other".to_string()),
                    latitude: 35.02,
                    longitude: 139.02,
                },
            ],
            school_station_links: vec![
                SchoolStationLink {
                    school_id: "school_target_line".to_string(),
                    station_id: "st_target_line".to_string(),
                    walking_minutes: 6,
                    distance_meters: 600,
                    hop_distance: 1,
                    line_name: "Shared Line".to_string(),
                },
                SchoolStationLink {
                    school_id: "school_other_line".to_string(),
                    station_id: "st_other_line".to_string(),
                    walking_minutes: 7,
                    distance_meters: 700,
                    hop_distance: 1,
                    line_name: "Shared Line".to_string(),
                },
            ],
            popularity_snapshots: Vec::new(),
            user_affinity_snapshots: Vec::new(),
            area_affinity_snapshots: Vec::new(),
        };
        let mut profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        profiles.schools.strict_min_candidates = 1;
        profiles.fallback.min_results = 1;
        let engine = RankingEngine::new(profiles, "v020-line-id-stage-test");
        let mut query = query("st_target", PlacementKind::Search);
        query.context = Some(RankingContext {
            context_source: ContextSource::RequestLine,
            confidence: 0.95,
            area: None,
            line: Some(LineContext {
                line_id: Some("line_target".to_string()),
                line_name: "Shared Line".to_string(),
                operator_name: None,
            }),
            station: None,
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        });

        let result = engine
            .recommend(&dataset, &query)
            .expect("recommendation result");

        assert_eq!(result.candidate_counts.get("same_line"), Some(&1));
        assert_eq!(result.fallback_stage, FallbackStage::SameLine);
        assert_eq!(result.items[0].school_id, "school_target_line");
    }

    #[test]
    fn city_context_requires_prefecture_match_when_present() {
        let dataset = RankingDataset {
            schools: vec![
                School {
                    id: "school_tokyo_fuchu".to_string(),
                    name: "Tokyo Fuchu".to_string(),
                    area: "Fuchu".to_string(),
                    prefecture_name: Some("Tokyo".to_string()),
                    school_type: "high_school".to_string(),
                    group_id: "group_tokyo".to_string(),
                },
                School {
                    id: "school_hiroshima_fuchu".to_string(),
                    name: "Hiroshima Fuchu".to_string(),
                    area: "Fuchu".to_string(),
                    prefecture_name: Some("Hiroshima".to_string()),
                    school_type: "high_school".to_string(),
                    group_id: "group_hiroshima".to_string(),
                },
            ],
            events: Vec::new(),
            stations: vec![
                Station {
                    id: "st_target".to_string(),
                    name: "Target".to_string(),
                    line_name: "Target Line".to_string(),
                    line_id: None,
                    latitude: 35.0,
                    longitude: 139.0,
                },
                Station {
                    id: "st_tokyo_fuchu".to_string(),
                    name: "Tokyo Fuchu Station".to_string(),
                    line_name: "Target Line".to_string(),
                    line_id: None,
                    latitude: 35.01,
                    longitude: 139.01,
                },
                Station {
                    id: "st_hiroshima_fuchu".to_string(),
                    name: "Hiroshima Fuchu Station".to_string(),
                    line_name: "Other Line".to_string(),
                    line_id: None,
                    latitude: 34.57,
                    longitude: 133.24,
                },
            ],
            school_station_links: vec![
                SchoolStationLink {
                    school_id: "school_tokyo_fuchu".to_string(),
                    station_id: "st_tokyo_fuchu".to_string(),
                    walking_minutes: 6,
                    distance_meters: 600,
                    hop_distance: 1,
                    line_name: "Target Line".to_string(),
                },
                SchoolStationLink {
                    school_id: "school_hiroshima_fuchu".to_string(),
                    station_id: "st_hiroshima_fuchu".to_string(),
                    walking_minutes: 7,
                    distance_meters: 700,
                    hop_distance: 1,
                    line_name: "Other Line".to_string(),
                },
            ],
            popularity_snapshots: Vec::new(),
            user_affinity_snapshots: Vec::new(),
            area_affinity_snapshots: Vec::new(),
        };
        let mut profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        profiles.schools.strict_min_candidates = 1;
        profiles.fallback.min_results = 1;
        let engine = RankingEngine::new(profiles, "v020-city-pref-stage-test");
        let mut query = query("st_target", PlacementKind::Search);
        query.context = Some(request_area_context(Some("Fuchu"), Some("Tokyo")));

        let result = engine
            .recommend(&dataset, &query)
            .expect("recommendation result");

        assert_eq!(result.candidate_counts.get("same_city"), Some(&1));
        assert_eq!(result.fallback_stage, FallbackStage::SameCity);
        assert_eq!(result.items[0].school_id, "school_tokyo_fuchu");
    }

    #[test]
    fn station_area_conflict_warning_suppresses_area_fallback() {
        let dataset = load_fixture_dataset(fixture_root()).expect("fixture dataset");
        let profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        let engine = RankingEngine::new(profiles, "v020-context-test");
        let mut query = query("st_tamachi", PlacementKind::Search);
        let mut context = request_area_context(Some("Shibuya"), Some("Tokyo"));
        context.context_source = ContextSource::RequestStation;
        context.station = Some(StationContext {
            station_id: "st_tamachi".to_string(),
            station_name: "Tamachi".to_string(),
        });
        context.line = Some(LineContext {
            line_id: None,
            line_name: "JR Yamanote Line".to_string(),
            operator_name: None,
        });
        context.warnings.push(ContextWarning {
            code: "station_area_conflict".to_string(),
            message: "station context was used and conflicting area hint was ignored".to_string(),
        });
        query.context = Some(context);

        let result = engine
            .recommend(&dataset, &query)
            .expect("recommendation result");

        assert_eq!(result.candidate_counts.get("same_city"), Some(&0));
        assert_eq!(result.candidate_counts.get("same_prefecture"), Some(&0));
    }

    #[test]
    fn default_safe_context_skips_neighbor_area_stage() {
        let dataset = load_fixture_dataset(fixture_root()).expect("fixture dataset");
        let profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        let engine = RankingEngine::new(profiles, "v020-context-test");
        let mut query = query("st_tamachi", PlacementKind::Search);
        query.context = Some(RankingContext::default_safe());

        let result = engine
            .recommend(&dataset, &query)
            .expect("recommendation result");

        assert_eq!(result.candidate_counts.get("strict_station"), Some(&0));
        assert_eq!(result.candidate_counts.get("same_line"), Some(&0));
        assert_eq!(result.candidate_counts.get("same_city"), Some(&0));
        assert_eq!(result.candidate_counts.get("same_prefecture"), Some(&0));
        assert_eq!(result.candidate_counts.get("neighbor_area"), Some(&0));
        assert_eq!(result.fallback_stage, FallbackStage::SafeGlobalPopular);
    }

    #[test]
    fn neighbor_area_can_expand_beyond_same_line_candidates() {
        let dataset = RankingDataset {
            schools: vec![
                School {
                    id: "school_neighbor_a".to_string(),
                    name: "Neighbor A".to_string(),
                    area: "Neighbor Ward".to_string(),
                    prefecture_name: None,
                    school_type: "high_school".to_string(),
                    group_id: "group_neighbor_a".to_string(),
                },
                School {
                    id: "school_neighbor_b".to_string(),
                    name: "Neighbor B".to_string(),
                    area: "Neighbor Ward".to_string(),
                    prefecture_name: None,
                    school_type: "high_school".to_string(),
                    group_id: "group_neighbor_b".to_string(),
                },
            ],
            events: Vec::new(),
            stations: vec![
                Station {
                    id: "st_target".to_string(),
                    name: "Target".to_string(),
                    line_name: "Target Line".to_string(),
                    line_id: None,
                    latitude: 35.0,
                    longitude: 139.0,
                },
                Station {
                    id: "st_neighbor_a".to_string(),
                    name: "Neighbor A Station".to_string(),
                    line_name: "Other Line".to_string(),
                    line_id: None,
                    latitude: 35.0005,
                    longitude: 139.0005,
                },
                Station {
                    id: "st_neighbor_b".to_string(),
                    name: "Neighbor B Station".to_string(),
                    line_name: "Another Line".to_string(),
                    line_id: None,
                    latitude: 35.0007,
                    longitude: 139.0007,
                },
            ],
            school_station_links: vec![
                SchoolStationLink {
                    school_id: "school_neighbor_a".to_string(),
                    station_id: "st_neighbor_a".to_string(),
                    walking_minutes: 8,
                    distance_meters: 650,
                    hop_distance: 0,
                    line_name: "Other Line".to_string(),
                },
                SchoolStationLink {
                    school_id: "school_neighbor_b".to_string(),
                    station_id: "st_neighbor_b".to_string(),
                    walking_minutes: 9,
                    distance_meters: 780,
                    hop_distance: 0,
                    line_name: "Another Line".to_string(),
                },
            ],
            popularity_snapshots: Vec::new(),
            user_affinity_snapshots: Vec::new(),
            area_affinity_snapshots: Vec::new(),
        };
        let profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        let engine = RankingEngine::new(profiles, "v020-context-test");
        let mut query = query("st_target", PlacementKind::Search);
        query.context = Some(RankingContext {
            context_source: ContextSource::RequestStation,
            confidence: 0.95,
            area: None,
            line: Some(LineContext {
                line_id: None,
                line_name: "Target Line".to_string(),
                operator_name: None,
            }),
            station: Some(StationContext {
                station_id: "st_target".to_string(),
                station_name: "Target".to_string(),
            }),
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        });

        let result = engine
            .recommend(&dataset, &query)
            .expect("recommendation result");

        assert_eq!(result.candidate_counts.get("strict_station"), Some(&0));
        assert_eq!(result.candidate_counts.get("same_line"), Some(&0));
        assert_eq!(result.candidate_counts.get("neighbor_area"), Some(&2));
        assert_eq!(result.fallback_stage, FallbackStage::NeighborArea);
    }

    #[test]
    fn hokkaido_context_does_not_prioritize_okinawa_popularity() {
        let dataset = RankingDataset {
            schools: vec![
                School {
                    id: "school_hokkaido".to_string(),
                    name: "Hokkaido School".to_string(),
                    area: "Hokkaido".to_string(),
                    prefecture_name: None,
                    school_type: "high_school".to_string(),
                    group_id: "group_hokkaido".to_string(),
                },
                School {
                    id: "school_okinawa".to_string(),
                    name: "Okinawa Popular School".to_string(),
                    area: "Okinawa".to_string(),
                    prefecture_name: None,
                    school_type: "high_school".to_string(),
                    group_id: "group_okinawa".to_string(),
                },
            ],
            events: Vec::new(),
            stations: vec![
                Station {
                    id: "st_sapporo".to_string(),
                    name: "Sapporo".to_string(),
                    line_name: "Sapporo Line".to_string(),
                    line_id: None,
                    latitude: 43.0618,
                    longitude: 141.3545,
                },
                Station {
                    id: "st_naha".to_string(),
                    name: "Naha".to_string(),
                    line_name: "Yui Rail".to_string(),
                    line_id: None,
                    latitude: 26.2124,
                    longitude: 127.6792,
                },
            ],
            school_station_links: vec![
                SchoolStationLink {
                    school_id: "school_hokkaido".to_string(),
                    station_id: "st_sapporo".to_string(),
                    walking_minutes: 12,
                    distance_meters: 900,
                    hop_distance: 0,
                    line_name: "Sapporo Line".to_string(),
                },
                SchoolStationLink {
                    school_id: "school_okinawa".to_string(),
                    station_id: "st_naha".to_string(),
                    walking_minutes: 2,
                    distance_meters: 100,
                    hop_distance: 0,
                    line_name: "Yui Rail".to_string(),
                },
            ],
            popularity_snapshots: vec![PopularitySnapshot {
                school_id: "school_okinawa".to_string(),
                popularity_score: 100.0,
                total_events: 100,
                school_view_count: 100,
                school_save_count: 0,
                event_view_count: 0,
                apply_click_count: 0,
                share_count: 0,
                search_execute_count: 0,
            }],
            user_affinity_snapshots: Vec::new(),
            area_affinity_snapshots: Vec::new(),
        };
        let profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        let engine = RankingEngine::new(profiles, "v020-hokkaido-test");
        let mut query = query("st_sapporo", PlacementKind::Search);
        query.context = Some(request_area_context(None, Some("Hokkaido")));

        let result = engine
            .recommend(&dataset, &query)
            .expect("recommendation result");

        assert_eq!(result.items[0].school_id, "school_hokkaido");
        assert_eq!(result.fallback_stage, FallbackStage::SamePrefecture);
    }

    #[test]
    fn prefecture_context_matches_school_prefecture_metadata() {
        let dataset = RankingDataset {
            schools: vec![
                School {
                    id: "school_tokyo".to_string(),
                    name: "Tokyo School".to_string(),
                    area: "Minato".to_string(),
                    prefecture_name: Some("Tokyo".to_string()),
                    school_type: "high_school".to_string(),
                    group_id: "group_tokyo".to_string(),
                },
                School {
                    id: "school_osaka".to_string(),
                    name: "Osaka Popular School".to_string(),
                    area: "Kita".to_string(),
                    prefecture_name: Some("Osaka".to_string()),
                    school_type: "high_school".to_string(),
                    group_id: "group_osaka".to_string(),
                },
            ],
            events: Vec::new(),
            stations: vec![
                Station {
                    id: "st_tokyo".to_string(),
                    name: "Tokyo".to_string(),
                    line_name: "Tokyo Line".to_string(),
                    line_id: None,
                    latitude: 35.0,
                    longitude: 139.0,
                },
                Station {
                    id: "st_osaka".to_string(),
                    name: "Osaka".to_string(),
                    line_name: "Osaka Line".to_string(),
                    line_id: None,
                    latitude: 34.0,
                    longitude: 135.0,
                },
            ],
            school_station_links: vec![
                SchoolStationLink {
                    school_id: "school_tokyo".to_string(),
                    station_id: "st_tokyo".to_string(),
                    walking_minutes: 12,
                    distance_meters: 900,
                    hop_distance: 0,
                    line_name: "Tokyo Line".to_string(),
                },
                SchoolStationLink {
                    school_id: "school_osaka".to_string(),
                    station_id: "st_osaka".to_string(),
                    walking_minutes: 2,
                    distance_meters: 100,
                    hop_distance: 0,
                    line_name: "Osaka Line".to_string(),
                },
            ],
            popularity_snapshots: vec![PopularitySnapshot {
                school_id: "school_osaka".to_string(),
                popularity_score: 100.0,
                total_events: 100,
                school_view_count: 100,
                school_save_count: 0,
                event_view_count: 0,
                apply_click_count: 0,
                share_count: 0,
                search_execute_count: 0,
            }],
            user_affinity_snapshots: Vec::new(),
            area_affinity_snapshots: Vec::new(),
        };
        let profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        let engine = RankingEngine::new(profiles, "v020-prefecture-test");
        let mut query = query("st_tokyo", PlacementKind::Search);
        query.context = Some(request_area_context(None, Some("Tokyo")));

        let result = engine
            .recommend(&dataset, &query)
            .expect("recommendation result");

        assert_eq!(result.items[0].school_id, "school_tokyo");
        assert_eq!(result.candidate_counts.get("same_prefecture"), Some(&1));
        assert_eq!(result.fallback_stage, FallbackStage::SamePrefecture);
    }

    #[test]
    fn home_and_search_can_return_different_content_mix() {
        let dataset = load_fixture_dataset(fixture_root()).expect("fixture dataset");
        let profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        let engine = RankingEngine::new(profiles, "phase5-test");

        let home = engine
            .recommend(&dataset, &query("st_tamachi", PlacementKind::Home))
            .expect("home result");
        let search = engine
            .recommend(&dataset, &query("st_tamachi", PlacementKind::Search))
            .expect("search result");

        assert_eq!(home.items[0].content_kind.as_str(), "event");
        assert_eq!(search.items[0].content_kind.as_str(), "school");
    }

    #[test]
    fn same_school_cap_prevents_duplicate_items_from_one_school() {
        let dataset = load_fixture_dataset(fixture_root()).expect("fixture dataset");
        let profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        let engine = RankingEngine::new(profiles, "phase5-test");
        let result = engine
            .recommend(&dataset, &query("st_tamachi", PlacementKind::Home))
            .expect("recommendation result");

        let school_count = result
            .items
            .iter()
            .filter(|item| item.school_id == "school_seaside")
            .count();
        assert_eq!(school_count, 1);
        assert!(result.explanation.contains("多様性上限"));
        assert!(result.explanation.contains("同一学校"));
    }

    #[test]
    fn same_group_cap_can_filter_second_school_in_same_group() {
        let dataset = load_fixture_dataset(fixture_root()).expect("fixture dataset");
        let mut profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        profiles
            .placements
            .get_mut(&PlacementKind::Search)
            .expect("search profile")
            .diversity
            .same_group_cap = 1;
        let engine = RankingEngine::new(profiles, "phase5-test");
        let result = engine
            .recommend(&dataset, &query("st_tamachi", PlacementKind::Search))
            .expect("recommendation result");

        let bayside_count = result
            .items
            .iter()
            .filter(|item| item.school_id == "school_seaside" || item.school_id == "school_garden")
            .count();
        assert_eq!(bayside_count, 1);
        assert!(result.explanation.contains("同一グループ"));
    }

    #[test]
    fn popularity_snapshot_can_change_ranking() {
        let mut dataset = load_fixture_dataset(fixture_root()).expect("fixture dataset");
        dataset.popularity_snapshots = vec![PopularitySnapshot {
            school_id: "school_garden".to_string(),
            popularity_score: 3.0,
            total_events: 8,
            school_view_count: 3,
            school_save_count: 2,
            event_view_count: 2,
            apply_click_count: 1,
            share_count: 0,
            search_execute_count: 0,
        }];

        let profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        let engine = RankingEngine::new(profiles, "phase5-test");
        let result = engine
            .recommend(&dataset, &query("st_tamachi", PlacementKind::Search))
            .expect("recommendation result");

        assert_eq!(result.items[0].school_id, "school_garden");
        assert!(result.items[0]
            .score_breakdown
            .iter()
            .any(|component| component.feature == "popularity_snapshot_bonus"));
    }

    #[test]
    fn user_affinity_bonus_is_scoped_to_the_requested_user() {
        let mut dataset = load_fixture_dataset(fixture_root()).expect("fixture dataset");
        dataset.user_affinity_snapshots = vec![UserAffinitySnapshot {
            user_id: "demo-user-1".to_string(),
            school_id: "school_hillside".to_string(),
            affinity_score: 1.0,
            event_count: 2,
        }];

        let profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        let engine = RankingEngine::new(profiles, "phase5-test");
        let result = engine
            .recommend(
                &dataset,
                &RankingQuery {
                    target_station_id: "st_shinbashi".to_string(),
                    limit: Some(3),
                    user_id: Some("demo-user-1".to_string()),
                    placement: PlacementKind::Mypage,
                    debug: true,
                    context: None,
                },
            )
            .expect("recommendation result");

        assert!(result.items[0]
            .score_breakdown
            .iter()
            .any(|component| component.feature == "user_affinity_bonus"));
        assert!(result.items[0]
            .score_breakdown
            .iter()
            .filter(|component| component.feature == "user_affinity_bonus")
            .all(|component| component.details.is_some()));
    }
}
