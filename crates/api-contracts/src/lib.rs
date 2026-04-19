use domain::{
    ContentKind, EventKind, PlacementKind, RecommendationResult, ScoreComponent, UserEvent,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RecommendationRequest {
    pub target_station_id: String,
    pub limit: Option<usize>,
    pub user_id: Option<String>,
    #[schema(value_type = String)]
    #[serde(default)]
    pub placement: PlacementKind,
    #[serde(default)]
    pub debug: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ScoreComponentDto {
    pub feature: String,
    pub value: f64,
    pub reason: String,
    #[schema(value_type = Option<Object>)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RecommendationItemDto {
    #[schema(value_type = String)]
    pub content_kind: ContentKind,
    pub content_id: String,
    pub school_id: String,
    pub school_name: String,
    pub event_id: Option<String>,
    pub event_title: Option<String>,
    pub primary_station_id: String,
    pub primary_station_name: String,
    pub line_name: String,
    pub score: f64,
    pub explanation: String,
    pub score_breakdown: Vec<ScoreComponentDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RecommendationResponse {
    pub items: Vec<RecommendationItemDto>,
    pub explanation: String,
    pub score_breakdown: Vec<ScoreComponentDto>,
    pub fallback_stage: String,
    pub profile_version: String,
    pub algorithm_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TrackRequest {
    pub user_id: String,
    #[schema(value_type = String)]
    pub event_kind: EventKind,
    pub school_id: Option<String>,
    pub event_id: Option<String>,
    pub target_station_id: Option<String>,
    pub occurred_at: Option<String>,
    #[schema(value_type = Option<Object>)]
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TrackResponse {
    pub status: String,
    pub event_id: String,
    pub queued_jobs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReadyResponse {
    pub status: String,
    pub database: String,
    pub cache: String,
}

impl RecommendationRequest {
    pub fn cacheable(&self) -> bool {
        !self.debug
    }
}

impl TrackRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.user_id.trim().is_empty() {
            return Err("user_id must not be empty".to_string());
        }
        if self.event_kind.requires_school()
            && self
                .school_id
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
        {
            return Err(format!(
                "school_id is required for {}",
                self.event_kind.as_str()
            ));
        }
        if matches!(self.event_kind, EventKind::SearchExecute)
            && self
                .target_station_id
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
        {
            return Err("target_station_id is required for search_execute".to_string());
        }
        if matches!(self.event_kind, EventKind::EventView)
            && self
                .event_id
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            && self
                .school_id
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
        {
            return Err("event_view requires event_id or school_id".to_string());
        }
        Ok(())
    }
}

impl From<RecommendationRequest> for domain::RankingQuery {
    fn from(value: RecommendationRequest) -> Self {
        Self {
            target_station_id: value.target_station_id,
            limit: value.limit,
            user_id: value.user_id,
            placement: value.placement,
            debug: value.debug,
        }
    }
}

impl From<TrackRequest> for UserEvent {
    fn from(value: TrackRequest) -> Self {
        Self {
            user_id: value.user_id,
            school_id: value.school_id,
            event_kind: value.event_kind,
            event_id: value.event_id,
            target_station_id: value.target_station_id,
            occurred_at: value
                .occurred_at
                .unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
            payload: value
                .payload
                .unwrap_or_else(|| Value::Object(Default::default())),
        }
    }
}

impl From<ScoreComponent> for ScoreComponentDto {
    fn from(value: ScoreComponent) -> Self {
        Self {
            feature: value.feature,
            value: value.value,
            reason: value.reason,
            details: value.details,
        }
    }
}

impl From<RecommendationResult> for RecommendationResponse {
    fn from(value: RecommendationResult) -> Self {
        Self {
            items: value
                .items
                .into_iter()
                .map(|item| RecommendationItemDto {
                    content_kind: item.content_kind,
                    content_id: item.content_id,
                    school_id: item.school_id,
                    school_name: item.school_name,
                    event_id: item.event_id,
                    event_title: item.event_title,
                    primary_station_id: item.primary_station_id,
                    primary_station_name: item.primary_station_name,
                    line_name: item.line_name,
                    score: item.score,
                    explanation: item.explanation,
                    score_breakdown: item
                        .score_breakdown
                        .into_iter()
                        .map(ScoreComponentDto::from)
                        .collect(),
                })
                .collect(),
            explanation: value.explanation,
            score_breakdown: value
                .score_breakdown
                .into_iter()
                .map(ScoreComponentDto::from)
                .collect(),
            fallback_stage: match value.fallback_stage {
                domain::FallbackStage::Strict => "strict".to_string(),
                domain::FallbackStage::Neighbor => "neighbor".to_string(),
            },
            profile_version: value.profile_version,
            algorithm_version: value.algorithm_version,
        }
    }
}
