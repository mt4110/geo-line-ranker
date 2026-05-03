use domain::{RankingQuery, ScoreComponent};

use crate::ReasonCatalogEntry;

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

pub(crate) fn reason_catalog() -> &'static [ReasonCatalogEntry] {
    REASON_CATALOG
}

pub(crate) fn reason_catalog_entry(feature: &str) -> Option<&'static ReasonCatalogEntry> {
    REASON_CATALOG.iter().find(|entry| entry.feature == feature)
}

pub(crate) fn component(
    feature: impl Into<String>,
    value: f64,
    reason: impl Into<String>,
    details: Option<serde_json::Value>,
) -> ScoreComponent {
    let feature = feature.into();
    let reason_code = reason_catalog_entry(&feature)
        .unwrap_or_else(|| {
            panic!(
                "score component feature must be in the reason catalog: {}",
                feature
            )
        })
        .reason_code
        .to_string();
    ScoreComponent {
        feature,
        reason_code,
        value,
        reason: reason.into(),
        details,
    }
}

pub(crate) fn debug_details(
    query: &RankingQuery,
    value: serde_json::Value,
) -> Option<serde_json::Value> {
    query.debug.then_some(value)
}

#[cfg(test)]
mod tests {
    use super::component;

    #[test]
    #[should_panic(expected = "score component feature must be in the reason catalog")]
    fn uncataloged_component_panics() {
        let _ = component("missing_feature", 1.0, "reason", None);
    }
}
