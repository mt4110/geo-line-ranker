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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(transparent)]
pub struct ContentKindRef(pub String);

impl ContentKindRef {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<ContentKind> for ContentKindRef {
    fn from(value: ContentKind) -> Self {
        Self(value.as_str().to_string())
    }
}

impl From<&str> for ContentKindRef {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for ContentKindRef {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Entity {
    pub id: String,
    pub content_kind: ContentKindRef,
    pub display_name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Occurrence {
    pub id: String,
    pub entity_id: String,
    pub content_kind: ContentKindRef,
    pub occurrence_kind: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starts_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ends_at: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub placement_tags: Vec<PlacementKind>,
    #[serde(default = "default_true")]
    pub is_active: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeatureContribution {
    pub feature: String,
    #[serde(default = "default_reason_code")]
    pub reason_code: String,
    pub value: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Candidate {
    pub content_kind: ContentKindRef,
    pub content_id: String,
    pub entity_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occurrence_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_context_id: Option<String>,
    pub score: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub feature_contributions: Vec<FeatureContribution>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProfilePolicy {
    pub profile_id: String,
    pub supported_content_kinds: Vec<ContentKindRef>,
    pub placements: Vec<PlacementKind>,
    pub fallback_policy: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalIngestRecordKind {
    Entity,
    Occurrence,
    ContextEvidence,
}

impl CanonicalIngestRecordKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Entity => "entity",
            Self::Occurrence => "occurrence",
            Self::ContextEvidence => "context_evidence",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanonicalIngestLocationContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub station_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefecture_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub city_code: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanonicalIngestLineage {
    pub source_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connector_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub import_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanonicalIngestRecord {
    pub record_kind: CanonicalIngestRecordKind,
    pub profile_id: String,
    pub content_kind: ContentKindRef,
    pub entity_id: String,
    pub record_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starts_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location_context: Option<CanonicalIngestLocationContext>,
    pub lineage: CanonicalIngestLineage,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanonicalIngestOutput {
    pub profile_id: String,
    pub source_id: String,
    pub records: Vec<CanonicalIngestRecord>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, Value>,
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
    #[serde(default = "default_payload", skip_serializing_if = "is_empty_object")]
    pub details: Value,
    #[serde(default = "default_true")]
    pub is_active: bool,
}

impl School {
    pub fn to_entity(&self) -> Entity {
        Entity::from(self)
    }
}

impl From<&School> for Entity {
    fn from(value: &School) -> Self {
        let mut attributes = BTreeMap::new();
        attributes.insert("area".to_string(), Value::String(value.area.clone()));
        if let Some(prefecture_name) = value.prefecture_name.clone() {
            attributes.insert(
                "prefecture_name".to_string(),
                Value::String(prefecture_name),
            );
        }
        attributes.insert(
            "school_type".to_string(),
            Value::String(value.school_type.clone()),
        );
        attributes.insert(
            "group_id".to_string(),
            Value::String(value.group_id.clone()),
        );

        Self {
            id: value.id.clone(),
            content_kind: ContentKind::School.into(),
            display_name: value.name.clone(),
            attributes,
        }
    }
}

impl Event {
    pub fn to_occurrence(&self) -> Occurrence {
        Occurrence::from(self)
    }
}

impl From<&Event> for Occurrence {
    fn from(value: &Event) -> Self {
        let mut attributes = BTreeMap::new();
        attributes.insert(
            "event_category".to_string(),
            Value::String(value.event_category.clone()),
        );
        attributes.insert("is_open_day".to_string(), Value::Bool(value.is_open_day));
        attributes.insert("is_featured".to_string(), Value::Bool(value.is_featured));
        if let Some(priority_weight) = serde_json::Number::from_f64(value.priority_weight) {
            attributes.insert(
                "priority_weight".to_string(),
                Value::Number(priority_weight),
            );
        }
        if !is_empty_object(&value.details) {
            attributes.insert("details".to_string(), value.details.clone());
        }

        Self {
            id: value.id.clone(),
            entity_id: value.school_id.clone(),
            content_kind: ContentKind::Event.into(),
            occurrence_kind: value.event_category.clone(),
            title: value.title.clone(),
            starts_at: value.starts_at.clone(),
            ends_at: None,
            placement_tags: value.placement_tags.clone(),
            is_active: value.is_active,
            attributes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Station {
    pub id: String,
    pub name: String,
    pub line_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub area_id: Option<String>,
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

impl From<&ScoreComponent> for FeatureContribution {
    fn from(value: &ScoreComponent) -> Self {
        Self {
            feature: value.feature.clone(),
            reason_code: value.reason_code.clone(),
            value: value.value,
            details: value.details.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
pub struct CandidatePlanTrace {
    pub minimum_candidate_count: usize,
    pub selected_stage: FallbackStage,
    pub stop_reason: String,
    pub area_context_usable: bool,
    pub stages: Vec<CandidatePlanStageTrace>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandidatePlanStageTrace {
    pub stage: FallbackStage,
    pub candidate_count: usize,
    pub required_min_candidates: usize,
    pub status: CandidatePlanStageStatus,
    pub reason_code: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CandidatePlanStageStatus {
    Selected,
    Insufficient,
    Skipped,
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
    pub candidate_plan_trace: Option<CandidatePlanTrace>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<RankingContext>,
    pub profile_version: String,
    pub algorithm_version: String,
}

fn default_payload() -> Value {
    Value::Object(Default::default())
}

fn is_empty_object(value: &Value) -> bool {
    matches!(value, Value::Object(map) if map.is_empty())
}

fn default_true() -> bool {
    true
}

fn default_reason_code() -> String {
    "uncataloged".to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        CanonicalIngestLineage, CanonicalIngestLocationContext, CanonicalIngestRecord,
        CanonicalIngestRecordKind, ContentKind, Event, School, ScoreComponent,
    };

    fn event_with_details(details: serde_json::Value) -> Event {
        Event {
            id: "event-a".to_string(),
            school_id: "school-a".to_string(),
            title: "Event A".to_string(),
            event_category: "open_campus".to_string(),
            is_open_day: true,
            is_featured: false,
            priority_weight: 0.0,
            starts_at: None,
            placement_tags: Vec::new(),
            details,
            is_active: true,
        }
    }

    #[test]
    fn event_serialization_omits_empty_details() {
        let payload =
            serde_json::to_value(event_with_details(json!({}))).expect("event serializes");

        assert!(payload.get("details").is_none());
    }

    #[test]
    fn event_serialization_keeps_non_empty_details() {
        let payload = serde_json::to_value(event_with_details(json!({
            "detail_url": "https://example.com/events/1"
        })))
        .expect("event serializes");

        assert_eq!(
            payload["details"]["detail_url"],
            "https://example.com/events/1"
        );
    }

    #[test]
    fn school_converts_to_generic_entity_boundary() {
        let school = School {
            id: "school-a".to_string(),
            name: "School A".to_string(),
            area: "Tokyo".to_string(),
            prefecture_name: Some("Tokyo".to_string()),
            school_type: "high_school".to_string(),
            group_id: "group-a".to_string(),
        };

        let entity = school.to_entity();

        assert_eq!(entity.id, "school-a");
        assert_eq!(entity.content_kind.as_str(), ContentKind::School.as_str());
        assert_eq!(entity.display_name, "School A");
        assert_eq!(entity.attributes["group_id"], json!("group-a"));
    }

    #[test]
    fn event_converts_to_generic_occurrence_boundary() {
        let event = event_with_details(json!({ "detail_url": "https://example.com/events/1" }));

        let occurrence = event.to_occurrence();

        assert_eq!(occurrence.id, "event-a");
        assert_eq!(occurrence.entity_id, "school-a");
        assert_eq!(
            occurrence.content_kind.as_str(),
            ContentKind::Event.as_str()
        );
        assert_eq!(occurrence.occurrence_kind, "open_campus");
        assert_eq!(
            occurrence.attributes["details"]["detail_url"],
            "https://example.com/events/1"
        );
        assert_eq!(occurrence.attributes["priority_weight"], json!(0.0));
    }

    #[test]
    fn score_component_converts_to_feature_contribution() {
        let component = ScoreComponent {
            feature: "line_match_bonus".to_string(),
            reason_code: "geo.line_match".to_string(),
            value: 1.25,
            reason: "same line".to_string(),
            details: Some(json!({ "line_id": "line-a" })),
        };

        let contribution = super::FeatureContribution::from(&component);

        assert_eq!(contribution.feature, "line_match_bonus");
        assert_eq!(contribution.reason_code, "geo.line_match");
        assert_eq!(contribution.value, 1.25);
        assert_eq!(contribution.details, Some(json!({ "line_id": "line-a" })));
    }

    #[test]
    fn canonical_ingest_record_serializes_minimal_occurrence_contract() {
        let record = CanonicalIngestRecord {
            record_kind: CanonicalIngestRecordKind::Occurrence,
            profile_id: "school-event-jp".to_string(),
            content_kind: ContentKind::Event.into(),
            entity_id: "school-a".to_string(),
            record_id: "event-a".to_string(),
            title: Some("Open Campus".to_string()),
            starts_at: Some("2026-06-01T10:00:00+09:00".to_string()),
            location_context: Some(CanonicalIngestLocationContext {
                station_id: Some("st_tamachi".to_string()),
                line_id: Some("jr_yamanote".to_string()),
                line_name: None,
                prefecture_code: Some("13".to_string()),
                city_code: None,
                attributes: Default::default(),
            }),
            lineage: CanonicalIngestLineage {
                source_id: "utokyo_events".to_string(),
                connector_type: Some("crawler_manifest".to_string()),
                manifest_path: Some("configs/crawler/sources/utokyo_events.yaml".to_string()),
                import_run_id: None,
                checksum_sha256: None,
                attributes: Default::default(),
            },
            attributes: Default::default(),
        };

        let payload = serde_json::to_value(record).expect("record serializes");

        assert_eq!(payload["record_kind"], json!("occurrence"));
        assert_eq!(payload["content_kind"], json!("event"));
        assert_eq!(
            payload["location_context"]["station_id"],
            json!("st_tamachi")
        );
        assert_eq!(payload["lineage"]["source_id"], json!("utokyo_events"));
        assert!(payload["lineage"].get("import_run_id").is_none());
    }
}
