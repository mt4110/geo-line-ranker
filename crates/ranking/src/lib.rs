use std::collections::{BTreeMap, HashMap, HashSet};

use config::{PlacementProfile, RankingProfiles};
use domain::{
    AreaAffinitySnapshot, ContentKind, Event, FallbackStage, PlacementKind, PopularitySnapshot,
    RankingDataset, RankingQuery, RecommendationItem, RecommendationResult, School,
    SchoolStationLink, ScoreComponent, Station, UserAffinitySnapshot,
};
use geo::haversine_meters;
use serde_json::json;
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

#[derive(Debug, Clone)]
struct ScoredCandidate {
    content_kind: ContentKind,
    content_id: String,
    school_id: String,
    group_id: String,
    item: RecommendationItem,
}

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DiversitySelectionSummary {
    selected_count: usize,
    same_school_skipped: usize,
    same_group_skipped: usize,
    content_kind_skipped: BTreeMap<ContentKind, usize>,
}

impl DiversitySelectionSummary {
    fn skipped_count(&self) -> usize {
        self.same_school_skipped
            + self.same_group_skipped
            + self.content_kind_skipped.values().sum::<usize>()
    }
}

#[derive(Debug, Clone, PartialEq)]
struct DiversitySelection {
    items: Vec<RecommendationItem>,
    summary: DiversitySelectionSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReasonCatalogEntry {
    pub feature: &'static str,
    pub reason_code: &'static str,
    pub label: &'static str,
}

const REASON_CATALOG: &[ReasonCatalogEntry] = &[
    ReasonCatalogEntry {
        feature: "direct_station_bonus",
        reason_code: "geo.direct_station",
        label: "直結条件",
    },
    ReasonCatalogEntry {
        feature: "line_match_bonus",
        reason_code: "geo.line_match",
        label: "沿線一致",
    },
    ReasonCatalogEntry {
        feature: "school_station_distance",
        reason_code: "geo.station_distance",
        label: "駅からの近さ",
    },
    ReasonCatalogEntry {
        feature: "walking_minutes",
        reason_code: "geo.walking_minutes",
        label: "徒歩分数",
    },
    ReasonCatalogEntry {
        feature: "neighbor_station_proximity",
        reason_code: "geo.neighbor_station_proximity",
        label: "近傍駅との距離",
    },
    ReasonCatalogEntry {
        feature: "open_day_bonus",
        reason_code: "event.open_day",
        label: "公開イベント",
    },
    ReasonCatalogEntry {
        feature: "featured_event_bonus",
        reason_code: "event.featured",
        label: "注目イベント",
    },
    ReasonCatalogEntry {
        feature: "event_priority_boost",
        reason_code: "event.priority",
        label: "運用優先度",
    },
    ReasonCatalogEntry {
        feature: "popularity_snapshot_bonus",
        reason_code: "behavior.popularity",
        label: "最近の人気",
    },
    ReasonCatalogEntry {
        feature: "area_affinity_bonus",
        reason_code: "behavior.area_affinity",
        label: "エリア需要",
    },
    ReasonCatalogEntry {
        feature: "user_affinity_bonus",
        reason_code: "behavior.user_affinity",
        label: "ユーザー反応",
    },
    ReasonCatalogEntry {
        feature: "content_kind_boost",
        reason_code: "placement.content_kind_boost",
        label: "placement調整",
    },
    ReasonCatalogEntry {
        feature: "neighbor_area_penalty",
        reason_code: "fallback.neighbor_area_penalty",
        label: "近隣エリア調整",
    },
    ReasonCatalogEntry {
        feature: "safe_global_distance_penalty",
        reason_code: "fallback.safe_global_distance_penalty",
        label: "遠距離抑制",
    },
];

pub fn reason_catalog() -> &'static [ReasonCatalogEntry] {
    REASON_CATALOG
}

pub fn reason_catalog_entry(feature: &str) -> Option<&'static ReasonCatalogEntry> {
    REASON_CATALOG.iter().find(|entry| entry.feature == feature)
}

impl RankingEngine {
    pub fn new(profiles: RankingProfiles, algorithm_version: impl Into<String>) -> Self {
        Self {
            profiles,
            algorithm_version: algorithm_version.into(),
        }
    }

    pub fn neighbor_max_hops(&self, placement: PlacementKind) -> u8 {
        self.profiles.placement(placement).neighbor_max_hops
    }

    pub fn minimum_candidate_count(&self) -> usize {
        self.profiles
            .schools
            .strict_min_candidates
            .max(self.profiles.fallback.min_results)
    }

    pub fn recommend(
        &self,
        dataset: &RankingDataset,
        query: &RankingQuery,
    ) -> Result<RecommendationResult, RankingError> {
        let target_station = dataset
            .stations
            .iter()
            .find(|station| station.id == query.target_station_id)
            .cloned()
            .ok_or_else(|| RankingError::UnknownStation(query.target_station_id.clone()))?;
        let placement_profile = self.profiles.placement(query.placement);

        let limit = query
            .limit
            .unwrap_or(self.profiles.schools.limit_default)
            .clamp(1, 20);
        let strict_min_candidates = self.minimum_candidate_count();

        let staged_candidates =
            self.collect_candidates_by_stage(dataset, query, &target_station, placement_profile);
        let candidate_counts = staged_candidates
            .iter()
            .map(|(stage, candidates)| (stage.as_str().to_string(), candidates.len()))
            .collect::<BTreeMap<_, _>>();
        let area_hint_was_ignored = query.context.as_ref().is_some_and(|context| {
            context
                .warnings
                .iter()
                .any(|warning| warning.code == "station_area_conflict")
        });
        let area_context_is_usable = query.context.as_ref().is_some_and(|context| {
            !area_hint_was_ignored
                && (context.city_name().is_some() || context.prefecture_name().is_some())
        });
        let first_sufficient_scoped_match = staged_candidates
            .iter()
            .filter(|(stage, _)| !matches!(stage, FallbackStage::SafeGlobalPopular))
            .find(|(_, candidates)| candidates.len() >= strict_min_candidates);
        let underfilled_area_match = area_context_is_usable
            .then(|| {
                staged_candidates.iter().find(|(stage, candidates)| {
                    matches!(
                        stage,
                        FallbackStage::SameCity | FallbackStage::SamePrefecture
                    ) && !candidates.is_empty()
                })
            })
            .flatten();
        let sufficient_safe_global_match = staged_candidates.iter().find(|(stage, candidates)| {
            matches!(stage, FallbackStage::SafeGlobalPopular)
                && candidates.len() >= strict_min_candidates
        });
        let (fallback_stage, candidates) = first_sufficient_scoped_match
            .or(underfilled_area_match)
            .or(sufficient_safe_global_match)
            .or_else(|| {
                staged_candidates
                    .iter()
                    .filter(|(_, candidates)| !candidates.is_empty())
                    .max_by(
                        |(left_stage, left_candidates), (right_stage, right_candidates)| {
                            left_candidates
                                .len()
                                .cmp(&right_candidates.len())
                                .then_with(|| right_stage.priority().cmp(&left_stage.priority()))
                        },
                    )
            })
            .map(|(stage, candidates)| (stage.clone(), candidates.clone()))
            .unwrap_or_else(|| (FallbackStage::SafeGlobalPopular, Vec::new()));

        if candidates.is_empty() {
            return Err(RankingError::NoCandidates(target_station.id));
        }

        let scored_candidates = self.score_candidates(
            dataset,
            query,
            &target_station,
            placement_profile,
            candidates,
            &fallback_stage,
        );
        let DiversitySelection {
            items,
            summary: diversity_summary,
        } = self.select_diverse_items(scored_candidates, limit, placement_profile);
        if items.is_empty() {
            return Err(RankingError::NoCandidates(target_station.id));
        }

        let top_level_explanation = build_top_level_explanation(
            query.placement,
            &target_station,
            &fallback_stage,
            &items,
            &diversity_summary,
        );
        let score_breakdown = items
            .first()
            .map(|item| item.score_breakdown.clone())
            .unwrap_or_default();

        Ok(RecommendationResult {
            items,
            explanation: top_level_explanation,
            score_breakdown,
            fallback_stage,
            profile_version: self.profiles.profile_version.clone(),
            algorithm_version: self.algorithm_version.clone(),
            candidate_counts,
            context: query.context.clone(),
        })
    }

    fn collect_candidates_by_stage(
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

    fn score_candidates(
        &self,
        dataset: &RankingDataset,
        query: &RankingQuery,
        target_station: &Station,
        placement_profile: &PlacementProfile,
        candidates: Vec<SchoolStationLink>,
        fallback_stage: &FallbackStage,
    ) -> Vec<ScoredCandidate> {
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
        let events_by_school = dataset.events.iter().filter(|event| event.is_active).fold(
            HashMap::<&str, Vec<&Event>>::new(),
            |mut acc, event| {
                acc.entry(event.school_id.as_str()).or_default().push(event);
                acc
            },
        );
        let popularity_by_school: HashMap<&str, &PopularitySnapshot> = dataset
            .popularity_snapshots
            .iter()
            .map(|snapshot| (snapshot.school_id.as_str(), snapshot))
            .collect();
        let area_affinity_by_area: HashMap<&str, &AreaAffinitySnapshot> = dataset
            .area_affinity_snapshots
            .iter()
            .map(|snapshot| (snapshot.area.as_str(), snapshot))
            .collect();
        let user_affinity_by_school: HashMap<&str, &UserAffinitySnapshot> = dataset
            .user_affinity_snapshots
            .iter()
            .map(|snapshot| (snapshot.school_id.as_str(), snapshot))
            .collect();

        let school_enabled = placement_profile
            .mixed_ranking
            .enabled_content_kinds
            .contains(&ContentKind::School);
        let event_enabled = placement_profile
            .mixed_ranking
            .enabled_content_kinds
            .contains(&ContentKind::Event);

        let mut best_candidates = HashMap::<(ContentKind, String), ScoredCandidate>::new();

        for link in candidates {
            let Some(school) = schools_by_id.get(link.school_id.as_str()) else {
                continue;
            };
            let Some(candidate_station) = stations_by_id.get(link.station_id.as_str()) else {
                continue;
            };

            let base_breakdown = self.base_breakdown(
                school,
                target_station,
                candidate_station,
                &link,
                query,
                placement_profile,
                fallback_stage,
                &popularity_by_school,
                &area_affinity_by_area,
                &user_affinity_by_school,
            );

            if school_enabled {
                let mut breakdown = base_breakdown.clone();
                if let Some(value) = placement_profile
                    .mixed_ranking
                    .score_boosts
                    .get(&ContentKind::School)
                    .copied()
                    .filter(|value| *value != 0.0)
                {
                    breakdown.push(component(
                        "content_kind_boost",
                        value,
                        format!(
                            "{} では学校候補を少し前に出します。",
                            placement_label(query.placement)
                        ),
                        None,
                    ));
                }
                let item = build_item(
                    ContentKind::School,
                    school,
                    None,
                    candidate_station,
                    breakdown,
                    fallback_stage,
                );
                upsert_best_candidate(
                    &mut best_candidates,
                    ScoredCandidate {
                        content_kind: ContentKind::School,
                        content_id: school.id.clone(),
                        school_id: school.id.clone(),
                        group_id: school.group_id.clone(),
                        item,
                    },
                );
            }

            if !event_enabled {
                continue;
            }

            for event in events_by_school
                .get(school.id.as_str())
                .into_iter()
                .flat_map(|events| events.iter().copied())
                .filter(|event| event_visible_on_placement(event, query.placement))
            {
                let mut breakdown = base_breakdown.clone();
                if let Some(value) = placement_profile
                    .mixed_ranking
                    .score_boosts
                    .get(&ContentKind::Event)
                    .copied()
                    .filter(|value| *value != 0.0)
                {
                    breakdown.push(component(
                        "content_kind_boost",
                        value,
                        format!(
                            "{} ではイベント候補を少し前に出します。",
                            placement_label(query.placement)
                        ),
                        None,
                    ));
                }
                if event.is_open_day {
                    breakdown.push(component(
                        "open_day_bonus",
                        self.profiles.events.open_day_bonus,
                        "公開イベントを持つ候補を少しだけ押し上げます。".to_string(),
                        debug_details(
                            query,
                            json!({
                                "event_id": event.id,
                                "event_category": event.event_category,
                            }),
                        ),
                    ));
                }
                if event.is_featured {
                    breakdown.push(component(
                        "featured_event_bonus",
                        placement_profile.mixed_ranking.featured_event_bonus,
                        "注目イベントとして運用上の優先度を反映しています。".to_string(),
                        debug_details(query, json!({ "event_id": event.id })),
                    ));
                }
                if event.priority_weight > 0.0 {
                    breakdown.push(component(
                        "event_priority_boost",
                        event.priority_weight
                            * placement_profile.mixed_ranking.event_priority_weight,
                        "イベントの運用優先度を少し反映しました。".to_string(),
                        debug_details(
                            query,
                            json!({
                                "event_id": event.id,
                                "priority_weight": event.priority_weight,
                            }),
                        ),
                    ));
                }

                let item = build_item(
                    ContentKind::Event,
                    school,
                    Some(event),
                    candidate_station,
                    breakdown,
                    fallback_stage,
                );
                upsert_best_candidate(
                    &mut best_candidates,
                    ScoredCandidate {
                        content_kind: ContentKind::Event,
                        content_id: event.id.clone(),
                        school_id: school.id.clone(),
                        group_id: school.group_id.clone(),
                        item,
                    },
                );
            }
        }

        let mut scored = best_candidates.into_values().collect::<Vec<_>>();
        scored.sort_by(|left, right| compare_candidates(&left.item, &right.item));
        scored
    }

    #[allow(clippy::too_many_arguments)]
    fn base_breakdown(
        &self,
        school: &School,
        target_station: &Station,
        candidate_station: &Station,
        link: &SchoolStationLink,
        query: &RankingQuery,
        placement_profile: &PlacementProfile,
        fallback_stage: &FallbackStage,
        popularity_by_school: &HashMap<&str, &PopularitySnapshot>,
        area_affinity_by_area: &HashMap<&str, &AreaAffinitySnapshot>,
        user_affinity_by_school: &HashMap<&str, &UserAffinitySnapshot>,
    ) -> Vec<ScoreComponent> {
        let station_distance = haversine_meters(
            target_station.latitude,
            target_station.longitude,
            candidate_station.latitude,
            candidate_station.longitude,
        );

        let mut breakdown = Vec::new();
        if link.station_id == target_station.id {
            breakdown.push(component(
                "direct_station_bonus",
                self.profiles.schools.direct_station_bonus,
                format!("{} に直結しています。", target_station.name),
                None,
            ));
        }

        if link.line_name == target_station.line_name {
            let same_line_bonus = if matches!(fallback_stage, FallbackStage::NeighborArea) {
                placement_profile.neighbor_same_line_bonus
            } else {
                self.profiles.schools.line_match_bonus
            };
            breakdown.push(component(
                "line_match_bonus",
                same_line_bonus,
                format!("{} 沿線の候補です。", target_station.line_name),
                None,
            ));
        }

        let distance_value =
            (self.profiles.schools.distance_scale_meters - link.distance_meters as f64).max(0.0)
                / self.profiles.schools.distance_scale_meters;
        breakdown.push(component(
            "school_station_distance",
            distance_value,
            format!(
                "{} から徒歩 {} 分です。",
                candidate_station.name, link.walking_minutes
            ),
            None,
        ));

        let walking_value =
            (self.profiles.schools.walking_scale_minutes - link.walking_minutes as f64).max(0.0)
                / self.profiles.schools.walking_scale_minutes;
        breakdown.push(component(
            "walking_minutes",
            walking_value,
            "徒歩分数を短い順に評価しました。".to_string(),
            None,
        ));

        let neighbor_value =
            (self.profiles.fallback.neighbor_distance_cap_meters - station_distance).max(0.0)
                / self.profiles.fallback.neighbor_distance_cap_meters;
        breakdown.push(component(
            "neighbor_station_proximity",
            neighbor_value,
            format!(
                "{} から {} までの地理的な近さを反映しています。",
                target_station.name, candidate_station.name
            ),
            None,
        ));

        if let Some(popularity) = popularity_by_school.get(school.id.as_str()) {
            let value =
                popularity.popularity_score * self.profiles.tracking.popularity_bonus_weight;
            breakdown.push(component(
                "popularity_snapshot_bonus",
                value,
                "最近の行動ログで人気が高い学校を少しだけ押し上げます。".to_string(),
                debug_details(
                    query,
                    json!({
                        "popularity_score": popularity.popularity_score,
                        "total_events": popularity.total_events,
                        "school_view_count": popularity.school_view_count,
                        "school_save_count": popularity.school_save_count,
                        "event_view_count": popularity.event_view_count,
                        "apply_click_count": popularity.apply_click_count,
                        "share_count": popularity.share_count,
                        "search_execute_count": popularity.search_execute_count,
                    }),
                ),
            ));
        }

        if let Some(area_affinity) = area_affinity_by_area.get(school.area.as_str()) {
            let value =
                area_affinity.affinity_score * self.profiles.tracking.area_affinity_bonus_weight;
            breakdown.push(component(
                "area_affinity_bonus",
                value,
                format!("{} エリアの行動傾向を少し反映しました。", school.area),
                debug_details(
                    query,
                    json!({
                        "affinity_score": area_affinity.affinity_score,
                        "event_count": area_affinity.event_count,
                        "area": area_affinity.area,
                        "search_execute_count": area_affinity.search_execute_count,
                    }),
                ),
            ));
        }

        if query.user_id.is_some() {
            if let Some(user_affinity) = user_affinity_by_school.get(school.id.as_str()) {
                let value = user_affinity.affinity_score
                    * self.profiles.tracking.user_affinity_bonus_weight;
                breakdown.push(component(
                    "user_affinity_bonus",
                    value,
                    "このユーザーの最近の反応を少し反映しました。".to_string(),
                    debug_details(
                        query,
                        json!({
                            "user_id": user_affinity.user_id,
                            "affinity_score": user_affinity.affinity_score,
                            "event_count": user_affinity.event_count,
                        }),
                    ),
                ));
            }
        }

        if matches!(
            fallback_stage,
            FallbackStage::NeighborArea | FallbackStage::SafeGlobalPopular
        ) {
            breakdown.push(component(
                fallback_penalty_feature(fallback_stage),
                -fallback_stage_penalty(&self.profiles, fallback_stage),
                fallback_penalty_reason(fallback_stage),
                None,
            ));
        }

        breakdown
    }

    fn select_diverse_items(
        &self,
        candidates: Vec<ScoredCandidate>,
        limit: usize,
        placement_profile: &PlacementProfile,
    ) -> DiversitySelection {
        let mut school_counts = HashMap::<String, usize>::new();
        let mut group_counts = HashMap::<String, usize>::new();
        let mut kind_counts = BTreeMap::<ContentKind, usize>::new();
        let max_kind_counts = build_max_kind_counts(limit, placement_profile);
        let mut selected_keys = HashSet::<(ContentKind, String)>::new();
        let mut selected = Vec::new();
        let mut summary = DiversitySelectionSummary::default();

        for candidate in candidates {
            if selected.len() >= limit {
                break;
            }
            if !selected_keys.insert((candidate.content_kind, candidate.content_id.clone())) {
                continue;
            }
            if school_counts
                .get(candidate.school_id.as_str())
                .copied()
                .unwrap_or_default()
                >= placement_profile.diversity.same_school_cap
            {
                summary.same_school_skipped += 1;
                continue;
            }
            if group_counts
                .get(candidate.group_id.as_str())
                .copied()
                .unwrap_or_default()
                >= placement_profile.diversity.same_group_cap
            {
                summary.same_group_skipped += 1;
                continue;
            }
            if kind_counts
                .get(&candidate.content_kind)
                .copied()
                .unwrap_or_default()
                >= max_kind_counts
                    .get(&candidate.content_kind)
                    .copied()
                    .unwrap_or(limit)
            {
                *summary
                    .content_kind_skipped
                    .entry(candidate.content_kind)
                    .or_default() += 1;
                continue;
            }

            *school_counts
                .entry(candidate.school_id.clone())
                .or_default() += 1;
            *group_counts.entry(candidate.group_id.clone()).or_default() += 1;
            *kind_counts.entry(candidate.content_kind).or_default() += 1;
            selected.push(candidate.item);
        }

        summary.selected_count = selected.len();

        DiversitySelection {
            items: selected,
            summary,
        }
    }
}

fn build_item(
    content_kind: ContentKind,
    school: &School,
    event: Option<&Event>,
    candidate_station: &Station,
    score_breakdown: Vec<ScoreComponent>,
    fallback_stage: &FallbackStage,
) -> RecommendationItem {
    let score = score_breakdown
        .iter()
        .map(|component| component.value)
        .sum::<f64>();
    let explanation = build_item_explanation(content_kind, &score_breakdown, fallback_stage);

    RecommendationItem {
        content_kind,
        content_id: event
            .map(|event| event.id.clone())
            .unwrap_or_else(|| school.id.clone()),
        school_id: school.id.clone(),
        school_name: school.name.clone(),
        event_id: event.map(|event| event.id.clone()),
        event_title: event.map(|event| event.title.clone()),
        primary_station_id: candidate_station.id.clone(),
        primary_station_name: candidate_station.name.clone(),
        line_name: candidate_station.line_name.clone(),
        score,
        explanation,
        score_breakdown,
        fallback_stage: Some(fallback_stage.clone()),
    }
}

fn upsert_best_candidate(
    best_candidates: &mut HashMap<(ContentKind, String), ScoredCandidate>,
    candidate: ScoredCandidate,
) {
    let entry = best_candidates
        .entry((candidate.content_kind, candidate.content_id.clone()))
        .or_insert_with(|| candidate.clone());
    if compare_candidates(&candidate.item, &entry.item).is_lt() {
        *entry = candidate;
    }
}

fn compare_candidates(left: &RecommendationItem, right: &RecommendationItem) -> std::cmp::Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| {
            left.fallback_stage
                .as_ref()
                .map(FallbackStage::priority)
                .unwrap_or(usize::MAX)
                .cmp(
                    &right
                        .fallback_stage
                        .as_ref()
                        .map(FallbackStage::priority)
                        .unwrap_or(usize::MAX),
                )
        })
        .then_with(|| left.content_kind.as_str().cmp(right.content_kind.as_str()))
        .then_with(|| left.content_id.cmp(&right.content_id))
        .then_with(|| left.primary_station_id.cmp(&right.primary_station_id))
}

fn build_max_kind_counts(
    limit: usize,
    placement_profile: &PlacementProfile,
) -> BTreeMap<ContentKind, usize> {
    placement_profile
        .mixed_ranking
        .enabled_content_kinds
        .iter()
        .map(|kind| {
            let max_count = placement_profile
                .diversity
                .content_kind_max_ratio
                .get(kind)
                .map(|ratio| ((limit as f64) * ratio).ceil() as usize)
                .map(|count| count.clamp(1, limit))
                .unwrap_or(limit);
            (*kind, max_count)
        })
        .collect()
}

fn build_item_explanation(
    content_kind: ContentKind,
    breakdown: &[ScoreComponent],
    fallback_stage: &FallbackStage,
) -> String {
    let reasons = top_reason_labels(breakdown);
    let reason_text = join_reason_labels(&reasons);
    let fallback_text = match fallback_stage {
        FallbackStage::StrictStation => "指定駅直結",
        FallbackStage::SameLine => "同一路線",
        FallbackStage::SameCity => "同一市区町村",
        FallbackStage::SamePrefecture => "同一都道府県",
        FallbackStage::NeighborArea => "近隣エリア",
        FallbackStage::SafeGlobalPopular => "安全な広域fallback",
    };
    match content_kind {
        ContentKind::School => {
            format!("{reason_text} が効き、{fallback_text}の学校候補として上位になりました。")
        }
        ContentKind::Event => {
            format!("{reason_text} が効き、{fallback_text}のイベント候補として上位になりました。")
        }
        ContentKind::Article => {
            format!("{reason_text} が効き、{fallback_text}の記事候補として上位になりました。")
        }
    }
}

fn build_top_level_explanation(
    placement: PlacementKind,
    target_station: &Station,
    fallback_stage: &FallbackStage,
    items: &[RecommendationItem],
    diversity_summary: &DiversitySelectionSummary,
) -> String {
    let reasons = items
        .first()
        .map(|item| top_reason_labels(&item.score_breakdown))
        .unwrap_or_else(|| vec!["固定重み".to_string()]);
    let reason_text = join_reason_labels(&reasons);
    let fallback_text = match fallback_stage {
        FallbackStage::StrictStation => format!("{} 直結の候補群", target_station.name),
        FallbackStage::SameLine => format!("{} 沿線の候補群", target_station.line_name),
        FallbackStage::SameCity => "同一市区町村の候補群".to_string(),
        FallbackStage::SamePrefecture => "同一都道府県の候補群".to_string(),
        FallbackStage::NeighborArea => format!("{} 近傍まで広げた候補群", target_station.name),
        FallbackStage::SafeGlobalPopular => "広域人気を距離で抑制した候補群".to_string(),
    };

    let mut explanation = format!(
        "{}では {} を母集団にし、{} を効かせて決定論的に順位付けしました。",
        placement_label(placement),
        fallback_text,
        reason_text
    );
    if let Some(diversity_impact) = build_diversity_impact_sentence(diversity_summary) {
        explanation.push_str(&diversity_impact);
    }
    explanation
}

fn build_diversity_impact_sentence(summary: &DiversitySelectionSummary) -> Option<String> {
    let skipped_count = summary.skipped_count();
    if skipped_count == 0 {
        return None;
    }

    let mut reasons = Vec::new();
    if summary.same_school_skipped > 0 {
        reasons.push(format!("同一学校{}件", summary.same_school_skipped));
    }
    if summary.same_group_skipped > 0 {
        reasons.push(format!("同一グループ{}件", summary.same_group_skipped));
    }
    for (kind, count) in &summary.content_kind_skipped {
        if *count > 0 {
            reasons.push(format!("{}{}件", content_kind_label(*kind), count));
        }
    }

    Some(format!(
        " 多様性上限で{}を抑制し、{}件の表示枠に整えています。",
        join_reason_labels(&reasons),
        summary.selected_count
    ))
}

fn content_kind_label(kind: ContentKind) -> &'static str {
    match kind {
        ContentKind::School => "学校候補",
        ContentKind::Event => "イベント候補",
        ContentKind::Article => "記事候補",
    }
}

fn top_reason_labels(breakdown: &[ScoreComponent]) -> Vec<String> {
    let mut components = breakdown
        .iter()
        .filter(|component| component.value > 0.0)
        .collect::<Vec<_>>();
    components.sort_by(|left, right| {
        right
            .value
            .total_cmp(&left.value)
            .then_with(|| left.feature.cmp(&right.feature))
    });

    let mut labels = Vec::new();
    for component in components {
        let label = feature_label(&component.feature);
        if labels.contains(&label) {
            continue;
        }
        labels.push(label);
        if labels.len() >= 2 {
            break;
        }
    }
    if labels.is_empty() {
        labels.push("固定重み".to_string());
    }
    labels
}

fn feature_label(feature: &str) -> String {
    reason_catalog_entry(feature)
        .map(|entry| entry.label.to_string())
        .unwrap_or_else(|| "固定重み".to_string())
}

fn fallback_stage_penalty(profiles: &RankingProfiles, fallback_stage: &FallbackStage) -> f64 {
    match fallback_stage {
        FallbackStage::StrictStation
        | FallbackStage::SameLine
        | FallbackStage::SameCity
        | FallbackStage::SamePrefecture => 0.0,
        FallbackStage::NeighborArea => profiles.fallback.neighbor_penalty,
        FallbackStage::SafeGlobalPopular => profiles.fallback.neighbor_penalty * 2.0,
    }
}

fn fallback_penalty_feature(fallback_stage: &FallbackStage) -> &'static str {
    match fallback_stage {
        FallbackStage::SafeGlobalPopular => "safe_global_distance_penalty",
        _ => "neighbor_area_penalty",
    }
}

fn fallback_penalty_reason(fallback_stage: &FallbackStage) -> &'static str {
    match fallback_stage {
        FallbackStage::SafeGlobalPopular => {
            "広域候補のため、極端に遠い提示を抑える減点を入れています。"
        }
        _ => "直結候補ではないため、控えめに減点しています。",
    }
}

fn join_reason_labels(labels: &[String]) -> String {
    match labels {
        [] => "固定重み".to_string(),
        [only] => only.clone(),
        [first, second] => format!("{first} と {second}"),
        _ => labels.join("、"),
    }
}

fn placement_label(placement: PlacementKind) -> &'static str {
    match placement {
        PlacementKind::Home => "ホーム",
        PlacementKind::Search => "検索",
        PlacementKind::Detail => "詳細",
        PlacementKind::Mypage => "マイページ",
    }
}

fn event_visible_on_placement(event: &Event, placement: PlacementKind) -> bool {
    event.placement_tags.is_empty() || event.placement_tags.contains(&placement)
}

fn component(
    feature: impl Into<String>,
    value: f64,
    reason: impl Into<String>,
    details: Option<serde_json::Value>,
) -> ScoreComponent {
    let feature = feature.into();
    let reason_code = reason_catalog_entry(&feature)
        .map(|entry| entry.reason_code)
        .unwrap_or("uncataloged")
        .to_string();
    debug_assert_ne!(
        reason_code, "uncataloged",
        "score component feature must be in the reason catalog"
    );
    ScoreComponent {
        feature,
        reason_code,
        value,
        reason: reason.into(),
        details,
    }
}

fn debug_details(query: &RankingQuery, value: serde_json::Value) -> Option<serde_json::Value> {
    query.debug.then_some(value)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use config::RankingProfiles;
    use context::{
        AreaContext, ContextSource, ContextWarning, LineContext, PrivacyLevel, RankingContext,
        StationContext,
    };
    use domain::{
        ContentKind, PlacementKind, PopularitySnapshot, RankingDataset, RankingQuery,
        RecommendationItem, School, SchoolStationLink, Station, UserAffinitySnapshot,
    };
    use test_support::load_fixture_dataset;

    use super::{upsert_best_candidate, FallbackStage, RankingEngine, ScoredCandidate};

    fn fixture_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../storage/fixtures/minimal")
    }

    fn config_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../configs/ranking")
    }

    fn query(target_station_id: &str, placement: PlacementKind) -> RankingQuery {
        RankingQuery {
            target_station_id: target_station_id.to_string(),
            limit: Some(3),
            user_id: None,
            placement,
            debug: false,
            context: None,
        }
    }

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
    fn emitted_score_components_are_backed_by_reason_catalog() {
        let dataset = load_fixture_dataset(fixture_root()).expect("fixture dataset");
        let profiles = RankingProfiles::load_from_dir(config_root()).expect("profiles");
        let engine = RankingEngine::new(profiles, "reason-catalog-test");
        let result = engine
            .recommend(&dataset, &query("st_tamachi", PlacementKind::Home))
            .expect("recommendation result");

        for component in result
            .items
            .iter()
            .flat_map(|item| item.score_breakdown.iter())
            .chain(result.score_breakdown.iter())
        {
            let catalog_entry =
                super::reason_catalog_entry(&component.feature).expect("cataloged feature");
            assert_eq!(component.reason_code, catalog_entry.reason_code);
        }

        let top_reason_labels = super::top_reason_labels(&result.score_breakdown);
        assert!(top_reason_labels
            .iter()
            .all(|label| result.explanation.contains(label)));
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
    fn content_kind_cap_is_reflected_in_result_explanation() {
        let sentence = super::build_diversity_impact_sentence(&super::DiversitySelectionSummary {
            selected_count: 3,
            content_kind_skipped: std::collections::BTreeMap::from([(ContentKind::Event, 2)]),
            ..Default::default()
        })
        .expect("diversity impact sentence");

        assert!(sentence.contains("多様性上限"));
        assert!(sentence.contains("イベント候補2件"));
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

    #[test]
    fn best_candidate_map_keeps_school_and_event_namespaces_separate() {
        let mut best_candidates = std::collections::HashMap::new();

        upsert_best_candidate(
            &mut best_candidates,
            candidate(ContentKind::School, "shared-id", "school_seaside", 10.0),
        );
        upsert_best_candidate(
            &mut best_candidates,
            candidate(ContentKind::Event, "shared-id", "school_seaside", 11.0),
        );

        assert_eq!(best_candidates.len(), 2);
        assert!(best_candidates.contains_key(&(ContentKind::School, "shared-id".to_string())));
        assert!(best_candidates.contains_key(&(ContentKind::Event, "shared-id".to_string())));
    }

    fn candidate(
        content_kind: ContentKind,
        content_id: &str,
        school_id: &str,
        score: f64,
    ) -> ScoredCandidate {
        ScoredCandidate {
            content_kind,
            content_id: content_id.to_string(),
            school_id: school_id.to_string(),
            group_id: "group".to_string(),
            item: RecommendationItem {
                content_kind,
                content_id: content_id.to_string(),
                school_id: school_id.to_string(),
                school_name: "Test School".to_string(),
                event_id: None,
                event_title: None,
                primary_station_id: "station".to_string(),
                primary_station_name: "Test Station".to_string(),
                line_name: "Test Line".to_string(),
                score,
                explanation: "test".to_string(),
                score_breakdown: Vec::new(),
                fallback_stage: Some(FallbackStage::StrictStation),
            },
        }
    }
}
