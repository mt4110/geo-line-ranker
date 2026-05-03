use domain::{
    ContentKind, FallbackStage, PlacementKind, RecommendationItem, ScoreComponent, Station,
};

use crate::diversity::DiversitySelectionSummary;
use crate::reason_catalog_entry;

pub(crate) fn build_item_explanation(
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

pub(crate) fn build_top_level_explanation(
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

fn join_reason_labels(labels: &[String]) -> String {
    match labels {
        [] => "固定重み".to_string(),
        [only] => only.clone(),
        [first, second] => format!("{first} と {second}"),
        _ => labels.join("、"),
    }
}

pub(crate) fn placement_label(placement: PlacementKind) -> &'static str {
    match placement {
        PlacementKind::Home => "ホーム",
        PlacementKind::Search => "検索",
        PlacementKind::Detail => "詳細",
        PlacementKind::Mypage => "マイページ",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use config::RankingProfiles;
    use domain::{ContentKind, PlacementKind, RankingQuery};
    use test_support::load_fixture_dataset;

    use super::{build_diversity_impact_sentence, top_reason_labels};
    use crate::diversity::DiversitySelectionSummary;
    use crate::RankingEngine;

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
                crate::reason_catalog_entry(&component.feature).expect("cataloged feature");
            assert_eq!(component.reason_code, catalog_entry.reason_code);
        }

        let labels = top_reason_labels(&result.score_breakdown);
        assert!(labels
            .iter()
            .all(|label| result.explanation.contains(label)));
    }

    #[test]
    fn content_kind_cap_is_reflected_in_result_explanation() {
        let sentence = build_diversity_impact_sentence(&DiversitySelectionSummary {
            selected_count: 3,
            content_kind_skipped: BTreeMap::from([(ContentKind::Event, 2)]),
            ..Default::default()
        })
        .expect("diversity impact sentence");

        assert!(sentence.contains("多様性上限"));
        assert!(sentence.contains("イベント候補2件"));
    }
}
