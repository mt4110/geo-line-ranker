use std::collections::BTreeMap;

use context::RankingContext;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct School {
    pub id: String,
    pub name: String,
    pub area: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefecture_name: Option<String>,
    pub school_type: String,
    pub group_id: String,
}

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum PlacementKind {
    #[default]
    Home,
    Search,
    Detail,
    Mypage,
}

impl PlacementKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Home => "home",
            Self::Search => "search",
            Self::Detail => "detail",
            Self::Mypage => "mypage",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ContentKind {
    School,
    Event,
    Article,
}

impl ContentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::School => "school",
            Self::Event => "event",
            Self::Article => "article",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub id: String,
    pub school_id: String,
    pub title: String,
    pub event_category: String,
    pub is_open_day: bool,
    pub is_featured: bool,
    pub priority_weight: f64,
    #[serde(default)]
    pub starts_at: Option<String>,
    #[serde(default)]
    pub placement_tags: Vec<PlacementKind>,
    #[serde(default = "default_true")]
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Station {
    pub id: String,
    pub name: String,
    pub line_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_id: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchoolStationLink {
    pub school_id: String,
    pub station_id: String,
    pub walking_minutes: u16,
    pub distance_meters: u32,
    pub hop_distance: u8,
    pub line_name: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    SchoolView,
    SchoolSave,
    SearchExecute,
    EventView,
    ApplyClick,
    Share,
}

impl EventKind {
    pub fn requires_school(self) -> bool {
        matches!(
            self,
            Self::SchoolView | Self::SchoolSave | Self::ApplyClick | Self::Share
        )
    }

    pub fn is_school_affecting(self) -> bool {
        !matches!(self, Self::SearchExecute)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::SchoolView => "school_view",
            Self::SchoolSave => "school_save",
            Self::SearchExecute => "search_execute",
            Self::EventView => "event_view",
            Self::ApplyClick => "apply_click",
            Self::Share => "share",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UserEvent {
    pub user_id: String,
    pub school_id: Option<String>,
    pub event_kind: EventKind,
    #[serde(default)]
    pub event_id: Option<String>,
    #[serde(default)]
    pub target_station_id: Option<String>,
    pub occurred_at: String,
    #[serde(default = "default_payload")]
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PopularitySnapshot {
    pub school_id: String,
    pub popularity_score: f64,
    pub total_events: i64,
    pub school_view_count: i64,
    pub school_save_count: i64,
    pub event_view_count: i64,
    pub apply_click_count: i64,
    pub share_count: i64,
    pub search_execute_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UserAffinitySnapshot {
    pub user_id: String,
    pub school_id: String,
    pub affinity_score: f64,
    pub event_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AreaAffinitySnapshot {
    pub area: String,
    pub affinity_score: f64,
    pub event_count: i64,
    pub search_execute_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RankingQuery {
    pub target_station_id: String,
    pub limit: Option<usize>,
    pub user_id: Option<String>,
    pub placement: PlacementKind,
    pub debug: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<RankingContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RankingDataset {
    pub schools: Vec<School>,
    pub events: Vec<Event>,
    pub stations: Vec<Station>,
    pub school_station_links: Vec<SchoolStationLink>,
    pub popularity_snapshots: Vec<PopularitySnapshot>,
    pub user_affinity_snapshots: Vec<UserAffinitySnapshot>,
    pub area_affinity_snapshots: Vec<AreaAffinitySnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScoreComponent {
    pub feature: String,
    #[serde(default = "default_reason_code")]
    pub reason_code: String,
    pub value: f64,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FallbackStage {
    StrictStation,
    SameLine,
    SameCity,
    SamePrefecture,
    NeighborArea,
    SafeGlobalPopular,
}

impl FallbackStage {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::StrictStation => "strict_station",
            Self::SameLine => "same_line",
            Self::SameCity => "same_city",
            Self::SamePrefecture => "same_prefecture",
            Self::NeighborArea => "neighbor_area",
            Self::SafeGlobalPopular => "safe_global_popular",
        }
    }

    pub fn priority(&self) -> usize {
        match self {
            Self::StrictStation => 0,
            Self::SameLine => 1,
            Self::SameCity => 2,
            Self::SamePrefecture => 3,
            Self::NeighborArea => 4,
            Self::SafeGlobalPopular => 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecommendationItem {
    pub content_kind: ContentKind,
    pub content_id: String,
    pub school_id: String,
    pub school_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_title: Option<String>,
    pub primary_station_id: String,
    pub primary_station_name: String,
    pub line_name: String,
    pub score: f64,
    pub explanation: String,
    pub score_breakdown: Vec<ScoreComponent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_stage: Option<FallbackStage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecommendationResult {
    pub items: Vec<RecommendationItem>,
    pub explanation: String,
    pub score_breakdown: Vec<ScoreComponent>,
    pub fallback_stage: FallbackStage,
    #[serde(default)]
    pub candidate_counts: BTreeMap<String, usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<RankingContext>,
    pub profile_version: String,
    pub algorithm_version: String,
}

fn default_payload() -> Value {
    Value::Object(Default::default())
}

fn default_true() -> bool {
    true
}

fn default_reason_code() -> String {
    "uncataloged".to_string()
}
