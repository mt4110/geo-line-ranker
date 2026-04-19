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

impl RankingEngine {
    pub fn new(profiles: RankingProfiles, algorithm_version: impl Into<String>) -> Self {
        Self {
            profiles,
            algorithm_version: algorithm_version.into(),
        }
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
        let strict_min_candidates = self
            .profiles
            .schools
            .strict_min_candidates
            .max(self.profiles.fallback.min_results);

        let strict_candidates =
            self.collect_candidates(dataset, &target_station, placement_profile, false);
        let (fallback_stage, candidates) = if strict_candidates.len() >= strict_min_candidates {
            (FallbackStage::Strict, strict_candidates)
        } else {
            (
                FallbackStage::Neighbor,
                self.collect_candidates(dataset, &target_station, placement_profile, true),
            )
        };

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
        let items = self.select_diverse_items(scored_candidates, limit, placement_profile);
        if items.is_empty() {
            return Err(RankingError::NoCandidates(target_station.id));
        }

        let top_level_explanation =
            build_top_level_explanation(query.placement, &target_station, &fallback_stage, &items);
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
        })
    }

    fn collect_candidates(
        &self,
        dataset: &RankingDataset,
        target_station: &Station,
        placement_profile: &PlacementProfile,
        allow_neighbor: bool,
    ) -> Vec<SchoolStationLink> {
        let stations_by_id: HashMap<&str, &Station> = dataset
            .stations
            .iter()
            .map(|station| (station.id.as_str(), station))
            .collect();

        dataset
            .school_station_links
            .iter()
            .filter(|link| {
                if !allow_neighbor {
                    return link.station_id == target_station.id;
                }

                let Some(candidate_station) = stations_by_id.get(link.station_id.as_str()) else {
                    return false;
                };
                let station_distance = haversine_meters(
                    target_station.latitude,
                    target_station.longitude,
                    candidate_station.latitude,
                    candidate_station.longitude,
                );

                link.line_name == target_station.line_name
                    && link.hop_distance <= placement_profile.neighbor_max_hops
                    && station_distance <= self.profiles.fallback.neighbor_distance_cap_meters
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

        let mut best_candidates = HashMap::<String, ScoredCandidate>::new();

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
            let same_line_bonus = if matches!(fallback_stage, FallbackStage::Neighbor) {
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

        if matches!(fallback_stage, FallbackStage::Neighbor) {
            breakdown.push(component(
                "neighbor_penalty",
                -self.profiles.fallback.neighbor_penalty,
                "直結候補ではないため、控えめに減点しています。".to_string(),
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
    ) -> Vec<RecommendationItem> {
        let mut school_counts = HashMap::<String, usize>::new();
        let mut group_counts = HashMap::<String, usize>::new();
        let mut kind_counts = BTreeMap::<ContentKind, usize>::new();
        let max_kind_counts = build_max_kind_counts(limit, placement_profile);
        let mut selected_keys = HashSet::<(ContentKind, String)>::new();
        let mut selected = Vec::new();

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
                continue;
            }
            if group_counts
                .get(candidate.group_id.as_str())
                .copied()
                .unwrap_or_default()
                >= placement_profile.diversity.same_group_cap
            {
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
                continue;
            }

            *school_counts
                .entry(candidate.school_id.clone())
                .or_default() += 1;
            *group_counts.entry(candidate.group_id.clone()).or_default() += 1;
            *kind_counts.entry(candidate.content_kind).or_default() += 1;
            selected.push(candidate.item);
        }

        selected
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
    }
}

fn upsert_best_candidate(
    best_candidates: &mut HashMap<String, ScoredCandidate>,
    candidate: ScoredCandidate,
) {
    let entry = best_candidates
        .entry(candidate.content_id.clone())
        .or_insert_with(|| candidate.clone());
    if compare_candidates(&candidate.item, &entry.item).is_lt() {
        *entry = candidate;
    }
}

fn compare_candidates(left: &RecommendationItem, right: &RecommendationItem) -> std::cmp::Ordering {
    right
        .score
        .total_cmp(&left.score)
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
        FallbackStage::Strict => "直結条件",
        FallbackStage::Neighbor => "近傍展開",
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
) -> String {
    let reasons = items
        .first()
        .map(|item| top_reason_labels(&item.score_breakdown))
        .unwrap_or_else(|| vec!["固定重み".to_string()]);
    let reason_text = join_reason_labels(&reasons);
    let fallback_text = match fallback_stage {
        FallbackStage::Strict => format!("{} 直結の候補群", target_station.name),
        FallbackStage::Neighbor => format!("{} 近傍まで広げた候補群", target_station.name),
    };

    format!(
        "{}では {} を母集団にし、{} を効かせて決定論的に順位付けしました。",
        placement_label(placement),
        fallback_text,
        reason_text
    )
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
    match feature {
        "direct_station_bonus" => "直結条件".to_string(),
        "line_match_bonus" => "沿線一致".to_string(),
        "school_station_distance" => "駅からの近さ".to_string(),
        "walking_minutes" => "徒歩分数".to_string(),
        "neighbor_station_proximity" => "近傍駅との距離".to_string(),
        "open_day_bonus" => "公開イベント".to_string(),
        "featured_event_bonus" => "注目イベント".to_string(),
        "event_priority_boost" => "運用優先度".to_string(),
        "popularity_snapshot_bonus" => "最近の人気".to_string(),
        "area_affinity_bonus" => "エリア需要".to_string(),
        "user_affinity_bonus" => "ユーザー反応".to_string(),
        "content_kind_boost" => "placement調整".to_string(),
        _ => "固定重み".to_string(),
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
    ScoreComponent {
        feature: feature.into(),
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
    use domain::{PlacementKind, PopularitySnapshot, RankingQuery, UserAffinitySnapshot};
    use test_support::load_fixture_dataset;

    use super::{FallbackStage, RankingEngine};

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

        assert_eq!(result.fallback_stage, FallbackStage::Strict);
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

        assert_eq!(result.fallback_stage, FallbackStage::Neighbor);
        assert!(result
            .items
            .iter()
            .all(|item| item.line_name == "JR Yamanote Line"));
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
