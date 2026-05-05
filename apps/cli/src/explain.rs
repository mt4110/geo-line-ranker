use anyhow::{ensure, Context, Result};
use api_contracts::{RecommendationRequest, RecommendationResponse, ScoreComponentDto};
use config::AppSettings;
use serde::Serialize;
use serde_json::Value;
use storage_postgres::RecommendationTraceReadRow;

use crate::{
    explanation_integrity::{
        check_recommendation_response_integrity, ExplanationIntegrityCheck, QualityCheckStatus,
        QualitySeverity,
    },
    repository::pg_repository,
};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExplainTraceStatus {
    Ok,
    Warning,
}

impl ExplainTraceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warning => "warning",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainTraceReport {
    pub trace_id: i64,
    pub status: ExplainTraceStatus,
    pub created_at: String,
    pub algorithm_version: String,
    pub request: ExplainTraceRequestSummary,
    pub response: ExplainTraceResponseSummary,
    pub trace_payload: ExplainTracePayloadSummary,
    pub integrity: ExplainTraceIntegritySummary,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainTraceRequestSummary {
    pub request_id: Option<String>,
    pub user_id_present: bool,
    pub target_station_id: Option<String>,
    pub placement: Option<String>,
    pub limit: Option<usize>,
    pub debug: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainTraceResponseSummary {
    pub payload_shape: String,
    pub request_id: Option<String>,
    pub db_fallback_stage: String,
    pub response_fallback_stage: Option<String>,
    pub item_count: usize,
    pub result_order: Vec<String>,
    pub top_reasons: Vec<ExplainTraceReasonSummary>,
    pub items: Vec<ExplainTraceItemSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainTraceItemSummary {
    pub item_key: String,
    pub score: Option<f64>,
    pub fallback_stage: Option<String>,
    pub reasons: Vec<ExplainTraceReasonSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainTraceReasonSummary {
    pub feature: String,
    pub reason_code: String,
    pub label: String,
    pub value: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainTracePayloadSummary {
    pub response_source: Option<String>,
    pub context_source: Option<String>,
    pub context_confidence: Option<f64>,
    pub privacy_level: Option<String>,
    pub candidate_retrieval_mode: Option<String>,
    pub candidate_retrieval_backend: Option<String>,
    pub candidate_count: Option<usize>,
    pub duration_ms: Option<u64>,
    pub suppressed_item_reasons_recorded: bool,
    pub suppressed_item_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainTraceIntegritySummary {
    pub passed: usize,
    pub failed: usize,
    pub checks: Vec<ExplainTraceCheck>,
}

pub type ExplainTraceCheck = ExplanationIntegrityCheck;

pub async fn run_explain_trace(
    settings: &AppSettings,
    trace_id: i64,
) -> Result<ExplainTraceReport> {
    ensure!(trace_id > 0, "trace id must be positive");

    let repository = pg_repository(settings)?;
    let trace = repository
        .load_recommendation_trace(trace_id)
        .await?
        .with_context(|| format!("recommendation trace id {trace_id} was not found"))?;

    Ok(explain_trace_row(&trace))
}

pub fn explain_trace_row(trace: &RecommendationTraceReadRow) -> ExplainTraceReport {
    let mut warnings = Vec::new();
    let request = summarize_request(&trace.request_payload, &mut warnings);
    let (response, parsed_response) = summarize_response(trace, &mut warnings);
    let trace_payload = summarize_trace_payload(&trace.trace_payload, &mut warnings);

    if let Some(response_stage) = response.response_fallback_stage.as_deref() {
        if response.db_fallback_stage != response_stage {
            warnings.push(format!(
                "fallback_stage mismatch: recommendation_traces.fallback_stage={} response_payload.fallback_stage={}",
                response.db_fallback_stage, response_stage
            ));
        }
    }

    // Intentional: current API trace payloads do not include suppressed_items.
    // v0.3.1 explain trace surfaces that evidence gap when diversity caps affected
    // the response explanation, without changing ranking or replay behavior.
    if parsed_response
        .as_ref()
        .is_some_and(|response| response.explanation.contains("多様性上限"))
        && !trace_payload.suppressed_item_reasons_recorded
    {
        warnings.push(
            "trace_payload does not include suppressed item reasons, but the explanation says diversity caps suppressed items".to_string(),
        );
    }

    let integrity = parsed_response
        .as_ref()
        .map(|response| {
            integrity_summary_from_checks(check_recommendation_response_integrity(response))
        })
        .unwrap_or_else(|| ExplainTraceIntegritySummary {
            passed: 0,
            failed: 1,
            checks: vec![ExplainTraceCheck {
                name: "trace_shape.current_response".to_string(),
                severity: QualitySeverity::Warning,
                status: QualityCheckStatus::Failed,
                message:
                    "response_payload could not be parsed as the current RecommendationResponse shape; explanation integrity checks were skipped"
                        .to_string(),
            }],
        });

    let status = if warnings.is_empty() && integrity.failed == 0 {
        ExplainTraceStatus::Ok
    } else {
        ExplainTraceStatus::Warning
    };

    ExplainTraceReport {
        trace_id: trace.id,
        status,
        created_at: trace.created_at.clone(),
        algorithm_version: trace.algorithm_version.clone(),
        request,
        response,
        trace_payload,
        integrity,
        warnings,
    }
}

fn summarize_request(
    request_payload: &Value,
    warnings: &mut Vec<String>,
) -> ExplainTraceRequestSummary {
    match serde_json::from_value::<RecommendationRequest>(request_payload.clone()) {
        Ok(request) => ExplainTraceRequestSummary {
            request_id: request.request_id,
            user_id_present: request.user_id.is_some(),
            target_station_id: request.target_station_id,
            placement: Some(request.placement.as_str().to_string()),
            limit: request.limit,
            debug: Some(request.debug),
        },
        Err(error) => {
            let error_category = serde_error_category(&error);
            warnings.push(format!(
                "request_payload could not be parsed as the current RecommendationRequest shape (category: {error_category})"
            ));
            ExplainTraceRequestSummary {
                request_id: string_field(request_payload, "request_id"),
                user_id_present: field_present(request_payload, "user_id"),
                target_station_id: string_field(request_payload, "target_station_id"),
                placement: string_field(request_payload, "placement"),
                limit: usize_field(request_payload, "limit"),
                debug: bool_field(request_payload, "debug"),
            }
        }
    }
}

fn summarize_response(
    trace: &RecommendationTraceReadRow,
    warnings: &mut Vec<String>,
) -> (ExplainTraceResponseSummary, Option<RecommendationResponse>) {
    let db_fallback_stage = normalize_fallback_stage(&trace.fallback_stage);
    let fallback_stage = stored_response_fallback_stage(&trace.response_payload);
    let stored_order = match stored_response_order(&trace.response_payload) {
        Ok(order) => order,
        Err(error) => {
            warnings.push(format!(
                "response_payload item order could not be read: {error}"
            ));
            Vec::new()
        }
    };

    match serde_json::from_value::<RecommendationResponse>(trace.response_payload.clone()) {
        Ok(response) => {
            let result_order = response_order(&response);
            let summary = ExplainTraceResponseSummary {
                payload_shape: "current".to_string(),
                request_id: response.request_id.clone(),
                db_fallback_stage,
                response_fallback_stage: Some(response.fallback_stage.as_str().to_string()),
                item_count: response.items.len(),
                result_order,
                top_reasons: reason_summaries(&response.score_breakdown),
                items: response
                    .items
                    .iter()
                    .map(|item| ExplainTraceItemSummary {
                        item_key: format!("{}:{}", item.content_kind.as_str(), item.content_id),
                        score: Some(item.score),
                        fallback_stage: item
                            .fallback_stage
                            .as_ref()
                            .map(|stage| stage.as_str().to_string()),
                        reasons: reason_summaries(&item.score_breakdown),
                    })
                    .collect(),
            };
            (summary, Some(response))
        }
        Err(error) => {
            warnings.push(format!(
                "response_payload could not be parsed as the current RecommendationResponse shape: {error}"
            ));
            let summary = ExplainTraceResponseSummary {
                payload_shape: "legacy_or_invalid".to_string(),
                request_id: string_field(&trace.response_payload, "request_id"),
                db_fallback_stage,
                response_fallback_stage: fallback_stage,
                item_count: stored_order.len(),
                result_order: stored_order,
                top_reasons: Vec::new(),
                items: Vec::new(),
            };
            (summary, None)
        }
    }
}

fn summarize_trace_payload(
    trace_payload: &Value,
    warnings: &mut Vec<String>,
) -> ExplainTracePayloadSummary {
    let context = trace_payload.get("context");
    let candidate_retrieval = trace_payload.get("candidate_retrieval");
    let suppressed_items = trace_payload.get("suppressed_items");
    let suppressed_item_count = match suppressed_items {
        Some(Value::Array(items)) => Some(items.len()),
        Some(_) => {
            warnings
                .push("trace_payload.suppressed_items must be an array when present".to_string());
            None
        }
        None => None,
    };

    ExplainTracePayloadSummary {
        response_source: string_field(trace_payload, "response_source"),
        context_source: context.and_then(|value| string_field(value, "context_source")),
        context_confidence: context.and_then(|value| f64_field(value, "confidence")),
        privacy_level: context.and_then(|value| string_field(value, "privacy_level")),
        candidate_retrieval_mode: candidate_retrieval.and_then(|value| string_field(value, "mode")),
        candidate_retrieval_backend: candidate_retrieval
            .and_then(|value| string_field(value, "backend")),
        candidate_count: candidate_retrieval
            .and_then(|value| usize_field(value, "candidate_count")),
        duration_ms: candidate_retrieval.and_then(|value| u64_field(value, "duration_ms")),
        suppressed_item_reasons_recorded: suppressed_item_count.is_some(),
        suppressed_item_count,
    }
}

fn integrity_summary_from_checks(
    checks: Vec<ExplanationIntegrityCheck>,
) -> ExplainTraceIntegritySummary {
    let passed = checks
        .iter()
        .filter(|check| check.status == QualityCheckStatus::Passed)
        .count();
    let failed = checks.len() - passed;

    ExplainTraceIntegritySummary {
        passed,
        failed,
        checks,
    }
}

fn reason_summaries(components: &[ScoreComponentDto]) -> Vec<ExplainTraceReasonSummary> {
    components
        .iter()
        .map(|component| ExplainTraceReasonSummary {
            feature: component.feature.clone(),
            reason_code: component.reason_code.clone(),
            label: ranking::reason_catalog_entry(&component.feature)
                .map(|entry| entry.label.to_string())
                .unwrap_or_else(|| "固定重み".to_string()),
            value: component.value,
        })
        .collect()
}

fn response_order(response: &RecommendationResponse) -> Vec<String> {
    response
        .items
        .iter()
        .map(|item| format!("{}:{}", item.content_kind.as_str(), item.content_id))
        .collect()
}

fn stored_response_order(response: &Value) -> Result<Vec<String>> {
    let items = response
        .get("items")
        .and_then(Value::as_array)
        .with_context(|| "response_payload.items must be an array")?;
    items
        .iter()
        .map(|item| {
            let content_kind = match item.get("content_kind") {
                None => "school",
                Some(value) => value
                    .as_str()
                    .with_context(|| "response item content_kind must be a string")?,
            };
            let content_id = item
                .get("content_id")
                .and_then(Value::as_str)
                .or_else(|| item.get("school_id").and_then(Value::as_str))
                .with_context(|| "response item content_id must be a string")?;
            Ok(format!("{content_kind}:{content_id}"))
        })
        .collect()
}

fn stored_response_fallback_stage(response: &Value) -> Option<String> {
    response
        .get("fallback_stage")
        .and_then(Value::as_str)
        .map(normalize_fallback_stage)
}

fn normalize_fallback_stage(stage: &str) -> String {
    match stage {
        "strict" => "strict_station",
        other => other,
    }
    .to_string()
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_string)
}

fn field_present(value: &Value, key: &str) -> bool {
    value.get(key).is_some_and(|value| !value.is_null())
}

fn bool_field(value: &Value, key: &str) -> Option<bool> {
    value.get(key).and_then(Value::as_bool)
}

fn usize_field(value: &Value, key: &str) -> Option<usize> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
}

fn u64_field(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn f64_field(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(Value::as_f64)
}

fn serde_error_category(error: &serde_json::Error) -> &'static str {
    match error.classify() {
        serde_json::error::Category::Io => "io",
        serde_json::error::Category::Syntax => "syntax",
        serde_json::error::Category::Data => "data",
        serde_json::error::Category::Eof => "eof",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use storage_postgres::RecommendationTraceReadRow;

    use super::{explain_trace_row, ExplainTraceStatus};

    #[test]
    fn explain_trace_reports_current_payload_integrity() {
        let report = explain_trace_row(&current_trace_row(json!({
            "feature": "direct_station_bonus",
            "reason_code": "geo.direct_station",
            "value": 2.0,
            "reason": "direct"
        })));

        assert_eq!(report.status, ExplainTraceStatus::Ok);
        assert_eq!(report.response.payload_shape, "current");
        assert_eq!(report.response.result_order, vec!["school:school_a"]);
        assert!(!report.request.user_id_present);
        assert_eq!(report.integrity.failed, 0);
        assert!(report.warnings.is_empty());
    }

    #[test]
    fn explain_trace_redacts_raw_user_id_from_report() {
        let mut trace = current_trace_row(json!({
            "feature": "direct_station_bonus",
            "reason_code": "geo.direct_station",
            "value": 2.0,
            "reason": "direct"
        }));
        trace.request_payload["user_id"] = json!("raw-user-123");

        let report = explain_trace_row(&trace);
        let rendered = serde_json::to_string(&report).expect("json report");

        assert!(report.request.user_id_present);
        assert!(!rendered.contains("raw-user-123"));
    }

    #[test]
    fn explain_trace_reports_malformed_user_id_presence_by_key() {
        let mut trace = current_trace_row(json!({
            "feature": "direct_station_bonus",
            "reason_code": "geo.direct_station",
            "value": 2.0,
            "reason": "direct"
        }));
        trace.request_payload["user_id"] = json!({ "legacy": 123 });

        let report = explain_trace_row(&trace);

        assert!(report.request.user_id_present);
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("RecommendationRequest shape (category: data)")));
    }

    #[test]
    fn explain_trace_does_not_leak_scalar_request_payload_parse_errors() {
        let mut trace = current_trace_row(json!({
            "feature": "direct_station_bonus",
            "reason_code": "geo.direct_station",
            "value": 2.0,
            "reason": "direct"
        }));
        trace.request_payload = json!("raw-user-123");

        let report = explain_trace_row(&trace);
        let rendered = serde_json::to_string(&report).expect("json report");

        assert!(!report.request.user_id_present);
        assert!(!rendered.contains("raw-user-123"));
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("RecommendationRequest shape (category: data)")));
    }

    #[test]
    fn explain_trace_marks_malformed_suppressed_items_shape_as_warning() {
        let mut trace = current_trace_row(json!({
            "feature": "direct_station_bonus",
            "reason_code": "geo.direct_station",
            "value": 2.0,
            "reason": "direct"
        }));
        trace.trace_payload["suppressed_items"] = json!({ "school:school_b": "same_group_cap" });

        let report = explain_trace_row(&trace);

        assert_eq!(report.status, ExplainTraceStatus::Warning);
        assert!(!report.trace_payload.suppressed_item_reasons_recorded);
        assert_eq!(report.trace_payload.suppressed_item_count, None);
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("trace_payload.suppressed_items")));
    }

    #[test]
    fn explain_trace_marks_reason_catalog_mismatch_as_warning() {
        let report = explain_trace_row(&current_trace_row(json!({
            "feature": "direct_station_bonus",
            "reason_code": "geo.line_match",
            "value": 2.0,
            "reason": "direct"
        })));

        assert_eq!(report.status, ExplainTraceStatus::Warning);
        assert!(report
            .integrity
            .checks
            .iter()
            .any(|check| check.name == "explanation_integrity.reason_catalog"
                && check.status.as_str() == "failed"));
    }

    #[test]
    fn explain_trace_keeps_legacy_order_readable() {
        let mut trace = current_trace_row(json!({
            "feature": "direct_station_bonus",
            "reason_code": "geo.direct_station",
            "value": 2.0,
            "reason": "direct"
        }));
        trace.response_payload = json!({
            "items": [
                { "school_id": "school_a" }
            ],
            "fallback_stage": "strict"
        });

        let report = explain_trace_row(&trace);

        assert_eq!(report.status, ExplainTraceStatus::Warning);
        assert_eq!(report.response.payload_shape, "legacy_or_invalid");
        assert_eq!(
            report.response.response_fallback_stage.as_deref(),
            Some("strict_station")
        );
        assert_eq!(report.response.result_order, vec!["school:school_a"]);
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("current RecommendationResponse shape")));
    }

    #[test]
    fn explain_trace_keeps_legacy_empty_order_readable() {
        let mut trace = current_trace_row(json!({
            "feature": "direct_station_bonus",
            "reason_code": "geo.direct_station",
            "value": 2.0,
            "reason": "direct"
        }));
        trace.response_payload = json!({
            "items": [],
            "fallback_stage": "same_line"
        });
        trace.fallback_stage = "same_line".to_string();

        let report = explain_trace_row(&trace);

        assert_eq!(report.status, ExplainTraceStatus::Warning);
        assert_eq!(report.response.payload_shape, "legacy_or_invalid");
        assert_eq!(
            report.response.response_fallback_stage.as_deref(),
            Some("same_line")
        );
        assert!(report.response.result_order.is_empty());
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("current RecommendationResponse shape")));
    }

    fn current_trace_row(component: serde_json::Value) -> RecommendationTraceReadRow {
        RecommendationTraceReadRow {
            id: 42,
            request_payload: json!({
                "request_id": "req-1",
                "target_station_id": "st_tamachi",
                "limit": 1,
                "user_id": null,
                "placement": "search",
                "debug": false
            }),
            response_payload: json!({
                "request_id": "req-1",
                "items": [
                    {
                        "content_kind": "school",
                        "content_id": "school_a",
                        "school_id": "school_a",
                        "school_name": "School A",
                        "primary_station_id": "st_tamachi",
                        "primary_station_name": "Tamachi",
                        "line_name": "JR Yamanote Line",
                        "score": 2.0,
                        "explanation": "直結条件 が効き、指定駅直結の学校候補として上位になりました。",
                        "score_breakdown": [component],
                        "fallback_stage": "strict_station"
                    }
                ],
                "explanation": "searchでは Tamachi 直結の候補群を母集団にし、直結条件を効かせて決定論的に順位付けしました。",
                "score_breakdown": [
                    {
                        "feature": "direct_station_bonus",
                        "reason_code": "geo.direct_station",
                        "value": 2.0,
                        "reason": "direct"
                    }
                ],
                "fallback_stage": "strict_station",
                "candidate_counts": { "strict_station": 1 },
                "profile_version": "test",
                "algorithm_version": "test"
            }),
            trace_payload: json!({
                "response_source": "fresh",
                "context": {
                    "context_source": "request_station",
                    "confidence": 1.0,
                    "privacy_level": "station_level",
                    "warning_count": 0
                },
                "candidate_retrieval": {
                    "mode": "sql_only",
                    "backend": "postgres",
                    "candidate_count": 1,
                    "duration_ms": 8,
                    "target_station_id": "st_tamachi",
                    "candidate_limit": 256,
                    "neighbor_distance_cap_meters": 5000.0
                }
            }),
            fallback_stage: "strict_station".to_string(),
            algorithm_version: "test".to_string(),
            created_at: "2026-05-05T00:00:00.000000Z".to_string(),
        }
    }
}
