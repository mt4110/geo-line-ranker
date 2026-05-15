use context::{
    build_request_context, AreaContext, ContextEvidenceSummary, ContextInput, ContextSource,
    ContextWarning, LineContext, PrivacyLevel, RankingContext, StationContext,
};
use domain::{
    CandidatePlanStageStatus, ContentKind, EventKind, PlacementKind, RecommendationResult,
    ScoreComponent, UserEvent,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct ContextResolveRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_station_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<ContextInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
}

impl ContextResolveRequest {
    pub fn context_input(&self) -> ContextInput {
        build_request_context(self.target_station_id.as_deref(), self.context.as_ref())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ContextResolveResponse {
    pub request_id: String,
    pub context: ContextResolveContextDto,
    pub evidence_summary: ContextEvidenceSummary,
}

impl ContextResolveResponse {
    pub fn from_context(request_id: String, context: RankingContext) -> Self {
        let evidence_summary = context.evidence_summary();
        Self {
            request_id,
            context: context.into(),
            evidence_summary,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ContextResolveContextDto {
    pub context_source: ContextSource,
    pub confidence: f64,
    pub privacy_level: PrivacyLevel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub area: Option<AreaContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<LineContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub station: Option<StationContext>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<ContextWarning>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ScoreComponentDto {
    pub feature: String,
    #[schema(required)]
    #[serde(default = "default_reason_code")]
    pub reason_code: String,
    pub value: f64,
    pub reason: String,
    #[schema(value_type = Option<Object>)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
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
pub struct CandidatePlanTraceDto {
    pub minimum_candidate_count: usize,
    pub selected_stage: FallbackStageDto,
    pub stop_reason: String,
    pub area_context_usable: bool,
    pub stages: Vec<CandidatePlanStageTraceDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CandidatePlanStageTraceDto {
    pub stage: FallbackStageDto,
    pub candidate_count: usize,
    pub required_min_candidates: usize,
    pub status: CandidatePlanStageStatusDto,
    pub reason_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CandidatePlanStageStatusDto {
    Selected,
    Insufficient,
    Skipped,
}

impl CandidatePlanStageStatusDto {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Selected => "selected",
            Self::Insufficient => "insufficient",
            Self::Skipped => "skipped",
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_plan_trace: Option<CandidatePlanTraceDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<RecommendationContextDto>,
    pub profile_version: String,
    pub algorithm_version: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RecommendationContextDto {
    pub context_source: ContextSource,
    pub confidence: f64,
    pub privacy_level: PrivacyLevel,
    #[schema(required)]
    pub evidence_summary: ContextEvidenceSummary,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<ContextWarning>,
}

impl<'de> Deserialize<'de> for RecommendationContextDto {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RecommendationContextDtoCompat {
            context_source: ContextSource,
            confidence: f64,
            privacy_level: PrivacyLevel,
            #[serde(default)]
            evidence_summary: Option<ContextEvidenceSummary>,
            #[serde(default)]
            warnings: Vec<ContextWarning>,
        }

        let value = RecommendationContextDtoCompat::deserialize(deserializer)?;
        let evidence_summary = value.evidence_summary.unwrap_or_else(|| {
            ContextEvidenceSummary::from_context_source(&value.context_source, value.confidence)
        });

        Ok(Self {
            context_source: value.context_source,
            confidence: value.confidence,
            privacy_level: value.privacy_level,
            evidence_summary,
            warnings: value.warnings,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
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

impl TryFrom<RecommendationRequest> for domain::RankingQuery {
    type Error = String;

    fn try_from(value: RecommendationRequest) -> Result<Self, Self::Error> {
        let context = value.context_input();
        let target_station_id = context
            .station_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                "target_station_id or context.station_id is required to build RankingQuery"
                    .to_string()
            })?;

        Ok(Self {
            target_station_id,
            limit: value.limit,
            user_id: value.user_id,
            placement: value.placement,
            debug: value.debug,
            context: None,
        })
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
            reason_code: value.reason_code,
            value: value.value,
            reason: value.reason,
            details: value.details,
        }
    }
}

fn default_reason_code() -> String {
    "uncataloged".to_string()
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

impl From<CandidatePlanStageStatus> for CandidatePlanStageStatusDto {
    fn from(value: CandidatePlanStageStatus) -> Self {
        match value {
            CandidatePlanStageStatus::Selected => Self::Selected,
            CandidatePlanStageStatus::Insufficient => Self::Insufficient,
            CandidatePlanStageStatus::Skipped => Self::Skipped,
        }
    }
}

impl From<domain::CandidatePlanTrace> for CandidatePlanTraceDto {
    fn from(value: domain::CandidatePlanTrace) -> Self {
        Self {
            minimum_candidate_count: value.minimum_candidate_count,
            selected_stage: value.selected_stage.into(),
            stop_reason: value.stop_reason,
            area_context_usable: value.area_context_usable,
            stages: value
                .stages
                .into_iter()
                .map(CandidatePlanStageTraceDto::from)
                .collect(),
        }
    }
}

impl From<domain::CandidatePlanStageTrace> for CandidatePlanStageTraceDto {
    fn from(value: domain::CandidatePlanStageTrace) -> Self {
        Self {
            stage: value.stage.into(),
            candidate_count: value.candidate_count,
            required_min_candidates: value.required_min_candidates,
            status: value.status.into(),
            reason_code: value.reason_code,
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
            candidate_plan_trace: value.candidate_plan_trace.map(CandidatePlanTraceDto::from),
            context: value.context.map(RecommendationContextDto::from),
            profile_version: value.profile_version,
            algorithm_version: value.algorithm_version,
        }
    }
}

impl From<RankingContext> for RecommendationContextDto {
    fn from(value: RankingContext) -> Self {
        let evidence_summary = value.evidence_summary();
        Self {
            context_source: value.context_source,
            confidence: value.confidence,
            privacy_level: value.privacy_level,
            evidence_summary,
            warnings: value.warnings,
        }
    }
}

impl From<RankingContext> for ContextResolveContextDto {
    fn from(value: RankingContext) -> Self {
        Self {
            context_source: value.context_source,
            confidence: value.confidence,
            privacy_level: value.privacy_level,
            area: value.area,
            line: value.line,
            station: value.station,
            warnings: value.warnings,
        }
    }
}

#[cfg(test)]
mod tests {
    use domain::{
        CandidatePlanStageStatus, CandidatePlanStageTrace, CandidatePlanTrace, FallbackStage,
        RecommendationItem,
    };

    use super::{
        ContextResolveRequest, ContextResolveResponse, RecommendationRequest,
        RecommendationResponse, TrackRequest,
    };

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
            candidate_plan_trace: None,
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
    fn recommendation_response_reconstructs_cached_context_evidence_summary() {
        let payload = serde_json::json!({
            "items": [],
            "explanation": "cached response",
            "score_breakdown": [],
            "fallback_stage": "strict_station",
            "candidate_counts": {},
            "context": {
                "context_source": "request_station",
                "confidence": 0.91,
                "privacy_level": "coarse_area"
            },
            "profile_version": "phase6-profile",
            "algorithm_version": "phase6-test"
        });

        let response =
            serde_json::from_value::<RecommendationResponse>(payload).expect("cached response");
        let context = response.context.expect("context dto");
        assert_eq!(
            context.evidence_summary.primary_kind,
            context::ContextEvidenceKind::RequestStation
        );
        assert_eq!(context.evidence_summary.evidence_count, 1);
        assert_eq!(context.evidence_summary.strongest_strength, 0.91);
        assert!(!context.evidence_summary.has_search_execute);
    }

    #[test]
    fn recommendation_response_carries_candidate_plan_trace() {
        let response = RecommendationResponse::from(domain::RecommendationResult {
            items: Vec::new(),
            explanation: "result".to_string(),
            score_breakdown: Vec::new(),
            fallback_stage: FallbackStage::SameLine,
            candidate_counts: Default::default(),
            candidate_plan_trace: Some(CandidatePlanTrace {
                minimum_candidate_count: 3,
                selected_stage: FallbackStage::SameLine,
                stop_reason: "sufficient_scoped_candidates".to_string(),
                area_context_usable: false,
                stages: vec![CandidatePlanStageTrace {
                    stage: FallbackStage::SameLine,
                    candidate_count: 3,
                    required_min_candidates: 3,
                    status: CandidatePlanStageStatus::Selected,
                    reason_code: "selected_sufficient_scoped_candidates".to_string(),
                }],
            }),
            context: None,
            profile_version: "phase6-profile".to_string(),
            algorithm_version: "phase6-test".to_string(),
        });

        let payload = serde_json::to_value(response).expect("serialized response");

        assert_eq!(
            payload["candidate_plan_trace"]["selected_stage"],
            "same_line"
        );
        assert_eq!(
            payload["candidate_plan_trace"]["stages"][0]["status"],
            "selected"
        );
    }

    #[test]
    fn context_resolve_request_normalizes_target_station_into_context() {
        let request = ContextResolveRequest {
            request_id: Some("req-context".to_string()),
            target_station_id: Some("  st_tamachi  ".to_string()),
            context: None,
            user_id: Some("demo-user".to_string()),
        };

        let context = request.context_input();

        assert_eq!(context.station_id.as_deref(), Some("st_tamachi"));
    }

    #[test]
    fn context_resolve_response_carries_evidence_summary() {
        let mut context = context::RankingContext::default_safe();
        context.context_source = context::ContextSource::RecentSearchContext;
        context.confidence = 0.88;

        let response = ContextResolveResponse::from_context("req-context".to_string(), context);

        assert_eq!(response.request_id, "req-context");
        assert_eq!(
            response.context.context_source,
            context::ContextSource::RecentSearchContext
        );
        assert_eq!(
            response.evidence_summary.primary_kind,
            context::ContextEvidenceKind::SearchExecute
        );
        assert!(response.evidence_summary.has_search_execute);
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

    #[test]
    fn recommendation_request_try_into_query_requires_station_context() {
        let request = RecommendationRequest {
            request_id: None,
            target_station_id: None,
            context: Some(context::ContextInput {
                area: Some(context::AreaContextInput {
                    city_name: Some("Minato".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            limit: Some(3),
            user_id: Some("demo-user".to_string()),
            placement: domain::PlacementKind::Search,
            debug: false,
        };

        let error = domain::RankingQuery::try_from(request).expect_err("query should fail");
        assert_eq!(
            error,
            "target_station_id or context.station_id is required to build RankingQuery"
        );
    }

    #[test]
    fn track_request_rejects_unknown_keys() {
        let payload = serde_json::json!({
            "user_id": "demo-user",
            "event_kind": "school_view",
            "school_id": "school_seaside",
            "unexpected": true
        });

        let error = serde_json::from_value::<TrackRequest>(payload).expect_err("unknown key");
        assert!(error.to_string().contains("unknown field `unexpected`"));
    }

    #[test]
    fn context_resolve_request_rejects_unknown_keys() {
        let payload = serde_json::json!({
            "user_id": "demo-user",
            "target_station_id": "st_tamachi",
            "unexpected": "value"
        });

        let error =
            serde_json::from_value::<ContextResolveRequest>(payload).expect_err("unknown key");
        assert!(error.to_string().contains("unknown field `unexpected`"));
    }

    #[test]
    fn recommendation_request_rejects_unknown_keys() {
        let payload = serde_json::json!({
            "target_station_id": "st_tamachi",
            "limit": 3,
            "unexpected": 1
        });

        let error =
            serde_json::from_value::<RecommendationRequest>(payload).expect_err("unknown key");
        assert!(error.to_string().contains("unknown field `unexpected`"));
    }
}
