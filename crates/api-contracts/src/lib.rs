use domain::{
    ContentKind, EventKind, PlacementKind, RecommendationResult, ScoreComponent, UserEvent,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RecommendationRequest {
    pub target_station_id: String,
    #[schema(minimum = 1, maximum = 20)]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_title: Option<String>,
    pub primary_station_id: String,
    pub primary_station_name: String,
    pub line_name: String,
    pub score: f64,
    pub explanation: String,
    pub score_breakdown: Vec<ScoreComponentDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FallbackStageDto {
    Strict,
    Neighbor,
}

impl FallbackStageDto {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Neighbor => "neighbor",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RecommendationResponse {
    pub items: Vec<RecommendationItemDto>,
    pub explanation: String,
    pub score_breakdown: Vec<ScoreComponentDto>,
    pub fallback_stage: FallbackStageDto,
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
    pub opensearch: String,
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

impl From<domain::FallbackStage> for FallbackStageDto {
    fn from(value: domain::FallbackStage) -> Self {
        match value {
            domain::FallbackStage::Strict => Self::Strict,
            domain::FallbackStage::Neighbor => Self::Neighbor,
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
            fallback_stage: value.fallback_stage.into(),
            profile_version: value.profile_version,
            algorithm_version: value.algorithm_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use domain::{FallbackStage, RecommendationItem};

    use super::RecommendationResponse;

    #[test]
    fn recommendation_response_omits_empty_event_fields() {
        let response = RecommendationResponse::from(domain::RecommendationResult {
            items: vec![RecommendationItem {
                content_kind: domain::ContentKind::School,
                content_id: "school_seaside".to_string(),
                school_id: "school_seaside".to_string(),
                school_name: "Seaside High".to_string(),
                event_id: None,
                event_title: None,
                primary_station_id: "st_tamachi".to_string(),
                primary_station_name: "Tamachi".to_string(),
                line_name: "JR Yamanote Line".to_string(),
                score: 1.0,
                explanation: "school candidate".to_string(),
                score_breakdown: Vec::new(),
            }],
            explanation: "result".to_string(),
            score_breakdown: Vec::new(),
            fallback_stage: FallbackStage::Strict,
            profile_version: "phase6-profile".to_string(),
            algorithm_version: "phase6-test".to_string(),
        });

        let payload = serde_json::to_value(response).expect("serialized response");
        let item = &payload["items"][0];
        assert!(item.get("event_id").is_none());
        assert!(item.get("event_title").is_none());
    }
}
