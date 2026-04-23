use context::{
    build_request_context, ContextInput, ContextSource, ContextWarning, PrivacyLevel,
    RankingContext,
};
use domain::{
    ContentKind, EventKind, PlacementKind, RecommendationResult, ScoreComponent, UserEvent,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RecommendationRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_station_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<ContextInput>,
    #[schema(minimum = 1, maximum = 20)]
    pub limit: Option<usize>,
    pub user_id: Option<String>,
    #[schema(value_type = String)]
    #[serde(default)]
    pub placement: PlacementKind,
    #[serde(default)]
    pub debug: bool,
}

impl RecommendationRequest {
    pub fn context_input(&self) -> ContextInput {
        build_request_context(self.target_station_id.as_deref(), self.context.as_ref())
    }

    pub fn with_resolved_context(
        &self,
        target_station_id: String,
        context: RankingContext,
    ) -> domain::RankingQuery {
        domain::RankingQuery {
            target_station_id,
            limit: self.limit,
            user_id: self.user_id.clone(),
            placement: self.placement,
            debug: self.debug,
            context: Some(context),
        }
    }
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
    #[schema(value_type = Option<String>)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_stage: Option<FallbackStageDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FallbackStageDto {
    StrictStation,
    SameLine,
    SameCity,
    SamePrefecture,
    NeighborArea,
    SafeGlobalPopular,
}

impl FallbackStageDto {
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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RecommendationResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub items: Vec<RecommendationItemDto>,
    pub explanation: String,
    pub score_breakdown: Vec<ScoreComponentDto>,
    pub fallback_stage: FallbackStageDto,
    pub candidate_counts: std::collections::BTreeMap<String, usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<RecommendationContextDto>,
    pub profile_version: String,
    pub algorithm_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RecommendationContextDto {
    pub context_source: ContextSource,
    pub confidence: f64,
    pub privacy_level: PrivacyLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<ContextWarning>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TrackRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub user_id: String,
    #[schema(value_type = String)]
    pub event_kind: EventKind,
    pub school_id: Option<String>,
    pub event_id: Option<String>,
    pub target_station_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<ContextInput>,
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
        if let Some(occurred_at) = self.occurred_at.as_deref() {
            chrono::DateTime::<chrono::FixedOffset>::parse_from_rfc3339(occurred_at)
                .map_err(|_| "occurred_at must be RFC3339".to_string())?;
        }
        Ok(())
    }
}

impl From<RecommendationRequest> for domain::RankingQuery {
    fn from(value: RecommendationRequest) -> Self {
        Self {
            target_station_id: value
                .context
                .as_ref()
                .and_then(|context| context.station_id.clone())
                .or(value.target_station_id)
                .unwrap_or_default(),
            limit: value.limit,
            user_id: value.user_id,
            placement: value.placement,
            debug: value.debug,
            context: None,
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
            domain::FallbackStage::StrictStation => Self::StrictStation,
            domain::FallbackStage::SameLine => Self::SameLine,
            domain::FallbackStage::SameCity => Self::SameCity,
            domain::FallbackStage::SamePrefecture => Self::SamePrefecture,
            domain::FallbackStage::NeighborArea => Self::NeighborArea,
            domain::FallbackStage::SafeGlobalPopular => Self::SafeGlobalPopular,
        }
    }
}

impl From<RecommendationResult> for RecommendationResponse {
    fn from(value: RecommendationResult) -> Self {
        Self {
            request_id: None,
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
                    fallback_stage: item.fallback_stage.map(FallbackStageDto::from),
                })
                .collect(),
            explanation: value.explanation,
            score_breakdown: value
                .score_breakdown
                .into_iter()
                .map(ScoreComponentDto::from)
                .collect(),
            fallback_stage: value.fallback_stage.into(),
            candidate_counts: value.candidate_counts,
            context: value.context.map(RecommendationContextDto::from),
            profile_version: value.profile_version,
            algorithm_version: value.algorithm_version,
        }
    }
}

impl From<RankingContext> for RecommendationContextDto {
    fn from(value: RankingContext) -> Self {
        Self {
            context_source: value.context_source,
            confidence: value.confidence,
            privacy_level: value.privacy_level,
            warnings: value.warnings,
        }
    }
}

#[cfg(test)]
mod tests {
    use domain::{FallbackStage, RecommendationItem};

    use super::{RecommendationResponse, TrackRequest};

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
                fallback_stage: Some(FallbackStage::StrictStation),
            }],
            explanation: "result".to_string(),
            score_breakdown: Vec::new(),
            fallback_stage: FallbackStage::StrictStation,
            candidate_counts: Default::default(),
            context: None,
            profile_version: "phase6-profile".to_string(),
            algorithm_version: "phase6-test".to_string(),
        });

        let payload = serde_json::to_value(response).expect("serialized response");
        let item = &payload["items"][0];
        assert!(item.get("event_id").is_none());
        assert!(item.get("event_title").is_none());
    }

    #[test]
    fn track_request_rejects_non_rfc3339_occurred_at() {
        let request = TrackRequest {
            idempotency_key: None,
            user_id: "demo-user".to_string(),
            event_kind: domain::EventKind::SchoolView,
            school_id: Some("school_seaside".to_string()),
            event_id: None,
            target_station_id: None,
            context: None,
            occurred_at: Some("2026/04/22 12:00".to_string()),
            payload: None,
        };

        let error = request.validate().expect_err("invalid timestamp");
        assert_eq!(error, "occurred_at must be RFC3339");
    }

    #[test]
    fn search_execute_rejects_context_without_station_until_persisted() {
        let request = TrackRequest {
            idempotency_key: None,
            user_id: "demo-user".to_string(),
            event_kind: domain::EventKind::SearchExecute,
            school_id: None,
            event_id: None,
            target_station_id: None,
            context: Some(context::ContextInput {
                area: Some(context::AreaContextInput {
                    city_name: Some("Minato".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            occurred_at: Some("2026-04-22T12:00:00+09:00".to_string()),
            payload: None,
        };

        let error = request.validate().expect_err("target station required");
        assert_eq!(error, "target_station_id is required for search_execute");
    }
}
