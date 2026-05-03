use std::collections::HashMap;

use config::PlacementProfile;
use domain::{
    AreaAffinitySnapshot, ContentKind, Event, FallbackStage, PlacementKind, PopularitySnapshot,
    RankingDataset, RankingQuery, RecommendationItem, School, SchoolStationLink, ScoreComponent,
    Station, UserAffinitySnapshot,
};
use geo::haversine_meters;
use serde_json::json;

use crate::explanation::{build_item_explanation, placement_label};
use crate::fallback::{fallback_penalty_feature, fallback_penalty_reason, fallback_stage_penalty};
use crate::feature::{component, debug_details};
use crate::RankingEngine;

#[derive(Debug, Clone)]
pub(crate) struct ScoredCandidate {
    pub(crate) content_kind: ContentKind,
    pub(crate) content_id: String,
    pub(crate) school_id: String,
    pub(crate) group_id: String,
    pub(crate) item: RecommendationItem,
}

impl RankingEngine {
    pub(crate) fn score_candidates(
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

fn event_visible_on_placement(event: &Event, placement: PlacementKind) -> bool {
    event.placement_tags.is_empty() || event.placement_tags.contains(&placement)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use domain::{ContentKind, FallbackStage, RecommendationItem};

    use super::{upsert_best_candidate, ScoredCandidate};

    #[test]
    fn best_candidate_map_keeps_school_and_event_namespaces_separate() {
        let mut best_candidates = HashMap::new();

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
