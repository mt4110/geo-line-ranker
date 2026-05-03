use std::collections::HashMap;

use config::{PlacementProfile, RankingProfiles};
use domain::{FallbackStage, RankingDataset, RankingQuery, School, SchoolStationLink, Station};
use geo::haversine_meters;

use crate::RankingEngine;

struct RankingLookups<'a> {
    stations_by_id: HashMap<&'a str, &'a Station>,
    schools_by_id: HashMap<&'a str, &'a School>,
}

impl<'a> RankingLookups<'a> {
    fn new(dataset: &'a RankingDataset) -> Self {
        Self {
            stations_by_id: dataset
                .stations
                .iter()
                .map(|station| (station.id.as_str(), station))
                .collect(),
            schools_by_id: dataset
                .schools
                .iter()
                .map(|school| (school.id.as_str(), school))
                .collect(),
        }
    }
}

impl RankingEngine {
    pub(crate) fn collect_candidates_by_stage(
        &self,
        dataset: &RankingDataset,
        query: &RankingQuery,
        target_station: &Station,
        placement_profile: &PlacementProfile,
    ) -> Vec<(FallbackStage, Vec<SchoolStationLink>)> {
        let lookups = RankingLookups::new(dataset);

        vec![
            (
                FallbackStage::StrictStation,
                self.collect_stage_candidates(
                    dataset,
                    query,
                    target_station,
                    placement_profile,
                    &FallbackStage::StrictStation,
                    &lookups,
                ),
            ),
            (
                FallbackStage::SameLine,
                self.collect_stage_candidates(
                    dataset,
                    query,
                    target_station,
                    placement_profile,
                    &FallbackStage::SameLine,
                    &lookups,
                ),
            ),
            (
                FallbackStage::SameCity,
                self.collect_stage_candidates(
                    dataset,
                    query,
                    target_station,
                    placement_profile,
                    &FallbackStage::SameCity,
                    &lookups,
                ),
            ),
            (
                FallbackStage::SamePrefecture,
                self.collect_stage_candidates(
                    dataset,
                    query,
                    target_station,
                    placement_profile,
                    &FallbackStage::SamePrefecture,
                    &lookups,
                ),
            ),
            (
                FallbackStage::NeighborArea,
                self.collect_stage_candidates(
                    dataset,
                    query,
                    target_station,
                    placement_profile,
                    &FallbackStage::NeighborArea,
                    &lookups,
                ),
            ),
            (
                FallbackStage::SafeGlobalPopular,
                self.collect_stage_candidates(
                    dataset,
                    query,
                    target_station,
                    placement_profile,
                    &FallbackStage::SafeGlobalPopular,
                    &lookups,
                ),
            ),
        ]
    }

    fn collect_stage_candidates(
        &self,
        dataset: &RankingDataset,
        query: &RankingQuery,
        target_station: &Station,
        _placement_profile: &PlacementProfile,
        stage: &FallbackStage,
        lookups: &RankingLookups<'_>,
    ) -> Vec<SchoolStationLink> {
        let context = query.context.as_ref();
        let line_name = context
            .and_then(|context| context.line_name())
            .unwrap_or(target_station.line_name.as_str());
        let line_id = context.and_then(|context| {
            context
                .line
                .as_ref()
                .and_then(|line| line.line_id.as_deref())
        });
        let area_hint_was_ignored = context
            .map(|context| {
                context
                    .warnings
                    .iter()
                    .any(|warning| warning.code == "station_area_conflict")
            })
            .unwrap_or(false);
        let city_name = context
            .filter(|_| !area_hint_was_ignored)
            .and_then(|context| context.city_name());
        let prefecture_name = context
            .filter(|_| !area_hint_was_ignored)
            .and_then(|context| context.prefecture_name());
        let station_context_is_explicit = context
            .map(|context| context.station_id().is_some())
            .unwrap_or(true);
        let line_context_is_explicit = context
            .map(|context| context.line_name().is_some() || context.station_id().is_some())
            .unwrap_or(true);

        dataset
            .school_station_links
            .iter()
            .filter(|link| {
                let Some(candidate_station) = lookups.stations_by_id.get(link.station_id.as_str())
                else {
                    return false;
                };
                let Some(school) = lookups.schools_by_id.get(link.school_id.as_str()) else {
                    return false;
                };
                let is_same_line = match line_id {
                    Some(line_id) => {
                        candidate_station
                            .line_id
                            .as_deref()
                            .is_some_and(|value| value == line_id)
                            || (candidate_station.line_id.is_none() && link.line_name == line_name)
                    }
                    None => link.line_name == line_name,
                };
                let school_prefecture_matches = |prefecture: &str| {
                    school
                        .prefecture_name
                        .as_deref()
                        .is_some_and(|value| value.eq_ignore_ascii_case(prefecture))
                };
                let is_same_city = city_name.is_some_and(|city| {
                    school.area.eq_ignore_ascii_case(city)
                        && prefecture_name.is_none_or(school_prefecture_matches)
                });
                let is_same_prefecture = prefecture_name.is_some_and(|prefecture| {
                    school_prefecture_matches(prefecture)
                        || school.area.eq_ignore_ascii_case(prefecture)
                });

                match stage {
                    FallbackStage::StrictStation => {
                        station_context_is_explicit && link.station_id == target_station.id
                    }
                    FallbackStage::SameLine => line_context_is_explicit && is_same_line,
                    FallbackStage::SameCity => is_same_city,
                    FallbackStage::SamePrefecture => is_same_prefecture,
                    FallbackStage::NeighborArea => {
                        let station_distance = haversine_meters(
                            target_station.latitude,
                            target_station.longitude,
                            candidate_station.latitude,
                            candidate_station.longitude,
                        );
                        station_context_is_explicit
                            && link.station_id != target_station.id
                            && !is_same_line
                            && !is_same_city
                            && !is_same_prefecture
                            && station_distance
                                <= self.profiles.fallback.neighbor_distance_cap_meters
                    }
                    FallbackStage::SafeGlobalPopular => true,
                }
            })
            .cloned()
            .collect()
    }
}

pub(crate) fn fallback_stage_penalty(
    profiles: &RankingProfiles,
    fallback_stage: &FallbackStage,
) -> f64 {
    match fallback_stage {
        FallbackStage::StrictStation
        | FallbackStage::SameLine
        | FallbackStage::SameCity
        | FallbackStage::SamePrefecture => 0.0,
        FallbackStage::NeighborArea => profiles.fallback.neighbor_penalty,
        FallbackStage::SafeGlobalPopular => profiles.fallback.neighbor_penalty * 2.0,
    }
}

pub(crate) fn fallback_penalty_feature(fallback_stage: &FallbackStage) -> &'static str {
    match fallback_stage {
        FallbackStage::SafeGlobalPopular => "safe_global_distance_penalty",
        _ => "neighbor_area_penalty",
    }
}

pub(crate) fn fallback_penalty_reason(fallback_stage: &FallbackStage) -> &'static str {
    match fallback_stage {
        FallbackStage::SafeGlobalPopular => {
            "広域候補のため、極端に遠い提示を抑える減点を入れています。"
        }
        _ => "直結候補ではないため、控えめに減点しています。",
    }
}
