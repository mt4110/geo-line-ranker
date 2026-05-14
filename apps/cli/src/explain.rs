use anyhow::{ensure, Context, Result};
use api_contracts::{
    CandidatePlanTraceDto, RecommendationRequest, RecommendationResponse, ScoreComponentDto,
};
use config::AppSettings;
use serde::Serialize;
use serde_json::Value;
use storage::{RecommendationTraceCandidatePlanTrace, RecommendationTraceContextEvidenceSummary};
use storage_postgres::RecommendationTraceReadRow;

use crate::{
    explanation_integrity::{
        check_recommendation_response_integrity_with_catalog, ExplanationIntegrityCheck,
        QualityCheckStatus, QualitySeverity,
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
    pub context_evidence_summary: Option<ExplainTraceContextEvidenceSummary>,
    pub candidate_retrieval_mode: Option<String>,
    pub candidate_retrieval_backend: Option<String>,
    pub candidate_count: Option<usize>,
    pub duration_ms: Option<u64>,
    pub candidate_plan_trace: Option<ExplainTraceCandidatePlanSummary>,
    pub suppressed_item_reasons_recorded: bool,
    pub suppressed_item_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainTraceContextEvidenceSummary {
    pub source: String,
    pub primary_kind: String,
    pub evidence_count: i64,
    pub strongest_strength: f64,
    pub has_search_execute: bool,
    pub warning_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainTraceCandidatePlanSummary {
    pub minimum_candidate_count: usize,
    pub selected_stage: String,
    pub stop_reason: String,
    pub area_context_usable: bool,
    pub graph_diagnostics: Option<ExplainTraceCandidatePlanGraphDiagnosticsSummary>,
    pub stages: Vec<ExplainTraceCandidatePlanStageSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainTraceCandidatePlanGraphDiagnosticsSummary {
    pub mode: Option<String>,
    pub candidate_expansion_behavior: Option<String>,
    pub geo_graph_status: Option<String>,
    pub geo_graph_edge_count: Option<usize>,
    pub line_graph_status: Option<String>,
    pub line_graph_edge_count: Option<usize>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainTraceCandidatePlanStageSummary {
    pub stage: String,
    pub candidate_count: usize,
    pub required_min_candidates: usize,
    pub status: String,
    pub reason_code: String,
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
    let reason_catalog = runtime_reason_catalog(settings)?;

    Ok(explain_trace_row_with_catalog(&trace, &reason_catalog))
}

#[cfg(test)]
pub fn explain_trace_row(trace: &RecommendationTraceReadRow) -> ExplainTraceReport {
    explain_trace_row_with_catalog(trace, &ranking::ReasonCatalog::default_core())
}

pub fn explain_trace_row_with_catalog(
    trace: &RecommendationTraceReadRow,
    reason_catalog: &ranking::ReasonCatalog,
) -> ExplainTraceReport {
    let mut warnings = Vec::new();
    let request = summarize_request(&trace.request_payload, &mut warnings);
    let (response, parsed_response) = summarize_response(trace, &mut warnings, reason_catalog);
    let trace_payload = summarize_trace_payload(trace, &mut warnings);

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
            integrity_summary_from_checks(check_recommendation_response_integrity_with_catalog(
                response,
                reason_catalog,
            ))
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

fn runtime_reason_catalog(settings: &AppSettings) -> Result<ranking::ReasonCatalog> {
    if settings.profile_reason_catalog_path.is_empty() {
        return Ok(ranking::ReasonCatalog::default_core());
    }
    let profile_catalog =
        config::load_profile_reason_catalog(&settings.profile_reason_catalog_path)?;
    let reason_catalog = ranking::ReasonCatalog::from_profile_catalog(&profile_catalog)
        .with_context(|| {
            format!(
                "failed to merge profile reason catalog from {}",
                settings.profile_reason_catalog_path
            )
        })?;
    Ok(reason_catalog)
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
    reason_catalog: &ranking::ReasonCatalog,
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
                top_reasons: reason_summaries(&response.score_breakdown, reason_catalog),
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
                        reasons: reason_summaries(&item.score_breakdown, reason_catalog),
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
    trace: &RecommendationTraceReadRow,
    warnings: &mut Vec<String>,
) -> ExplainTracePayloadSummary {
    let trace_payload = &trace.trace_payload;
    let context = trace_payload.get("context");
    let candidate_retrieval = trace_payload.get("candidate_retrieval");
    let context_evidence_summary = summarize_context_evidence(
        trace.context_evidence_summary.as_ref(),
        context.and_then(|value| value.get("evidence_summary")),
        warnings,
    );
    let candidate_plan_trace = summarize_candidate_plan_trace(
        trace.candidate_plan_trace.as_ref(),
        trace_payload.get("candidate_plan_trace"),
        warnings,
    );
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
        context_source: trace
            .context_evidence_summary
            .as_ref()
            .map(|summary| summary.context_source.clone())
            .or_else(|| context.and_then(|value| string_field(value, "context_source"))),
        context_confidence: trace
            .context_evidence_summary
            .as_ref()
            .map(|summary| summary.confidence)
            .or_else(|| context.and_then(|value| f64_field(value, "confidence"))),
        privacy_level: trace
            .context_evidence_summary
            .as_ref()
            .map(|summary| summary.privacy_level.clone())
            .or_else(|| context.and_then(|value| string_field(value, "privacy_level"))),
        context_evidence_summary,
        candidate_retrieval_mode: candidate_retrieval.and_then(|value| string_field(value, "mode")),
        candidate_retrieval_backend: candidate_retrieval
            .and_then(|value| string_field(value, "backend")),
        candidate_count: candidate_retrieval
            .and_then(|value| usize_field(value, "candidate_count")),
        duration_ms: candidate_retrieval.and_then(|value| u64_field(value, "duration_ms")),
        candidate_plan_trace,
        suppressed_item_reasons_recorded: suppressed_item_count.is_some(),
        suppressed_item_count,
    }
}

fn summarize_context_evidence(
    dedicated: Option<&RecommendationTraceContextEvidenceSummary>,
    payload_evidence: Option<&Value>,
    warnings: &mut Vec<String>,
) -> Option<ExplainTraceContextEvidenceSummary> {
    if let Some(summary) = dedicated {
        return Some(ExplainTraceContextEvidenceSummary {
            source: "dedicated_rows".to_string(),
            primary_kind: summary.primary_kind.clone(),
            evidence_count: summary.evidence_count,
            strongest_strength: summary.strongest_strength,
            has_search_execute: summary.has_search_execute,
            warning_count: Some(summary.warning_count),
        });
    }

    let payload_evidence = payload_evidence?;
    let Some(primary_kind) = string_field(payload_evidence, "primary_kind") else {
        warnings.push(
            "trace_payload.context.evidence_summary.primary_kind must be a string when present"
                .to_string(),
        );
        return None;
    };
    let Some(evidence_count) = i64_field(payload_evidence, "evidence_count") else {
        warnings.push(
            "trace_payload.context.evidence_summary.evidence_count must be an integer when present"
                .to_string(),
        );
        return None;
    };
    let Some(strongest_strength) = f64_field(payload_evidence, "strongest_strength") else {
        warnings.push("trace_payload.context.evidence_summary.strongest_strength must be a number when present".to_string());
        return None;
    };
    let Some(has_search_execute) = bool_field(payload_evidence, "has_search_execute") else {
        warnings.push("trace_payload.context.evidence_summary.has_search_execute must be a boolean when present".to_string());
        return None;
    };

    Some(ExplainTraceContextEvidenceSummary {
        source: "trace_payload".to_string(),
        primary_kind,
        evidence_count,
        strongest_strength,
        has_search_execute,
        warning_count: None,
    })
}

fn summarize_candidate_plan_trace(
    dedicated: Option<&RecommendationTraceCandidatePlanTrace>,
    candidate_plan_trace: Option<&Value>,
    warnings: &mut Vec<String>,
) -> Option<ExplainTraceCandidatePlanSummary> {
    if let Some(trace) = dedicated {
        return Some(ExplainTraceCandidatePlanSummary {
            minimum_candidate_count: dedicated_i64_to_usize(
                "candidate_plan.minimum_candidate_count",
                trace.minimum_candidate_count,
                warnings,
            ),
            selected_stage: trace.selected_stage.clone(),
            stop_reason: trace.stop_reason.clone(),
            area_context_usable: trace.area_context_usable,
            graph_diagnostics: summarize_candidate_plan_graph_diagnostics(
                trace.plan_payload.get("graph_diagnostics").or_else(|| {
                    candidate_plan_trace.and_then(|payload| payload.get("graph_diagnostics"))
                }),
            ),
            stages: trace
                .stages
                .iter()
                .map(|stage| ExplainTraceCandidatePlanStageSummary {
                    stage: stage.stage.clone(),
                    candidate_count: dedicated_i64_to_usize(
                        "candidate_plan_stage.candidate_count",
                        stage.candidate_count,
                        warnings,
                    ),
                    required_min_candidates: dedicated_i64_to_usize(
                        "candidate_plan_stage.required_min_candidates",
                        stage.required_min_candidates,
                        warnings,
                    ),
                    status: stage.status.clone(),
                    reason_code: stage.reason_code.clone(),
                })
                .collect(),
        });
    }

    let candidate_plan_trace = candidate_plan_trace?;
    match serde_json::from_value::<CandidatePlanTraceDto>(candidate_plan_trace.clone()) {
        Ok(trace) => Some(ExplainTraceCandidatePlanSummary {
            minimum_candidate_count: trace.minimum_candidate_count,
            selected_stage: trace.selected_stage.as_str().to_string(),
            stop_reason: trace.stop_reason,
            area_context_usable: trace.area_context_usable,
            graph_diagnostics: summarize_candidate_plan_graph_diagnostics(
                candidate_plan_trace.get("graph_diagnostics"),
            ),
            stages: trace
                .stages
                .into_iter()
                .map(|stage| ExplainTraceCandidatePlanStageSummary {
                    stage: stage.stage.as_str().to_string(),
                    candidate_count: stage.candidate_count,
                    required_min_candidates: stage.required_min_candidates,
                    status: stage.status.as_str().to_string(),
                    reason_code: stage.reason_code,
                })
                .collect(),
        }),
        Err(error) => {
            warnings.push(format!(
                "trace_payload.candidate_plan_trace could not be parsed: {error}"
            ));
            None
        }
    }
}

fn summarize_candidate_plan_graph_diagnostics(
    graph_diagnostics: Option<&Value>,
) -> Option<ExplainTraceCandidatePlanGraphDiagnosticsSummary> {
    let graph_diagnostics = graph_diagnostics?;
    Some(ExplainTraceCandidatePlanGraphDiagnosticsSummary {
        mode: string_field(graph_diagnostics, "mode"),
        candidate_expansion_behavior: string_field(
            graph_diagnostics,
            "candidate_expansion_behavior",
        ),
        geo_graph_status: graph_diagnostics
            .get("geo_graph")
            .and_then(|value| string_field(value, "status")),
        geo_graph_edge_count: graph_diagnostics
            .get("geo_graph")
            .and_then(|value| usize_field(value, "edge_count")),
        line_graph_status: graph_diagnostics
            .get("line_graph")
            .and_then(|value| string_field(value, "status")),
        line_graph_edge_count: graph_diagnostics
            .get("line_graph")
            .and_then(|value| usize_field(value, "edge_count")),
        warnings: graph_diagnostics
            .get("warnings")
            .and_then(Value::as_array)
            .map(|warnings| {
                warnings
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
    })
}

fn dedicated_i64_to_usize(field: &str, value: i64, warnings: &mut Vec<String>) -> usize {
    match usize::try_from(value) {
        Ok(value) => value,
        Err(_) => {
            warnings.push(format!(
                "dedicated {field} value must be a non-negative usize-compatible integer"
            ));
            0
        }
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

fn reason_summaries(
    components: &[ScoreComponentDto],
    reason_catalog: &ranking::ReasonCatalog,
) -> Vec<ExplainTraceReasonSummary> {
    components
        .iter()
        .map(|component| ExplainTraceReasonSummary {
            feature: component.feature.clone(),
            reason_code: component.reason_code.clone(),
            label: reason_catalog
                .entry(&component.feature)
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

fn i64_field(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
    })
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
    fn explain_trace_summarizes_candidate_plan_trace() {
        let mut trace = current_trace_row(json!({
            "feature": "direct_station_bonus",
            "reason_code": "geo.direct_station",
            "value": 2.0,
            "reason": "direct"
        }));
        trace.trace_payload["candidate_plan_trace"] = json!({
            "minimum_candidate_count": 3,
            "selected_stage": "same_line",
            "stop_reason": "sufficient_scoped_candidates",
            "area_context_usable": false,
            "stages": [
                {
                    "stage": "strict_station",
                    "candidate_count": 1,
                    "required_min_candidates": 3,
                    "status": "insufficient",
                    "reason_code": "candidate_count_below_minimum"
                },
                {
                    "stage": "same_line",
                    "candidate_count": 3,
                    "required_min_candidates": 3,
                    "status": "selected",
                    "reason_code": "selected_sufficient_scoped_candidates"
                }
            ]
        });

        let report = explain_trace_row(&trace);
        let plan = report
            .trace_payload
            .candidate_plan_trace
            .expect("candidate plan summary");

        assert_eq!(plan.minimum_candidate_count, 3);
        assert_eq!(plan.selected_stage, "same_line");
        assert_eq!(plan.stages.len(), 2);
        assert_eq!(plan.stages[1].status, "selected");
    }

    #[test]
    fn explain_trace_prefers_dedicated_candidate_plan_rows() {
        let mut trace = current_trace_row(json!({
            "feature": "direct_station_bonus",
            "reason_code": "geo.direct_station",
            "value": 2.0,
            "reason": "direct"
        }));
        trace.trace_payload["candidate_plan_trace"] = json!({
            "minimum_candidate_count": 1,
            "selected_stage": "strict_station",
            "stop_reason": "payload_copy",
            "area_context_usable": false,
            "stages": []
        });
        trace.candidate_plan_trace = Some(storage::RecommendationTraceCandidatePlanTrace {
            minimum_candidate_count: 3,
            selected_stage: "same_line".to_string(),
            stop_reason: "dedicated_rows".to_string(),
            area_context_usable: true,
            plan_payload: json!({
                "selected_stage": "same_line",
                "graph_diagnostics": {
                    "mode": "diagnostic_read_only",
                    "candidate_expansion_behavior": "unchanged",
                    "geo_graph": {
                        "status": "loaded",
                        "edge_count": 2
                    },
                    "line_graph": {
                        "status": "origin_unavailable"
                    },
                    "warnings": ["line_graph_origin_unavailable"]
                }
            }),
            stages: vec![storage::RecommendationTraceCandidatePlanStage {
                stage_order: 0,
                stage: "same_line".to_string(),
                candidate_count: 4,
                required_min_candidates: 3,
                status: "selected".to_string(),
                reason_code: "selected_sufficient_scoped_candidates".to_string(),
                stage_payload: json!({ "stage": "same_line" }),
            }],
        });

        let report = explain_trace_row(&trace);
        let plan = report
            .trace_payload
            .candidate_plan_trace
            .expect("candidate plan summary");

        assert_eq!(plan.selected_stage, "same_line");
        assert_eq!(plan.stop_reason, "dedicated_rows");
        assert!(plan.area_context_usable);
        assert_eq!(plan.stages[0].candidate_count, 4);
        let graph = plan.graph_diagnostics.expect("graph diagnostics summary");
        assert_eq!(graph.mode.as_deref(), Some("diagnostic_read_only"));
        assert_eq!(graph.geo_graph_status.as_deref(), Some("loaded"));
        assert_eq!(graph.geo_graph_edge_count, Some(2));
        assert_eq!(
            graph.warnings,
            vec!["line_graph_origin_unavailable".to_string()]
        );
    }

    #[test]
    fn explain_trace_falls_back_to_payload_graph_diagnostics_for_legacy_plan_rows() {
        let mut trace = current_trace_row(json!({
            "feature": "direct_station_bonus",
            "reason_code": "geo.direct_station",
            "value": 2.0,
            "reason": "direct"
        }));
        trace.trace_payload["candidate_plan_trace"] = json!({
            "minimum_candidate_count": 1,
            "selected_stage": "strict_station",
            "stop_reason": "payload_copy",
            "area_context_usable": false,
            "graph_diagnostics": {
                "mode": "diagnostic_read_only",
                "candidate_expansion_behavior": "unchanged",
                "geo_graph": {
                    "status": "loaded",
                    "edge_count": 1
                },
                "line_graph": {
                    "status": "not_loaded"
                },
                "warnings": ["line_graph_load_failed"]
            },
            "stages": []
        });
        trace.candidate_plan_trace = Some(storage::RecommendationTraceCandidatePlanTrace {
            minimum_candidate_count: 3,
            selected_stage: "same_line".to_string(),
            stop_reason: "dedicated_rows".to_string(),
            area_context_usable: true,
            plan_payload: json!({ "selected_stage": "same_line" }),
            stages: vec![storage::RecommendationTraceCandidatePlanStage {
                stage_order: 0,
                stage: "same_line".to_string(),
                candidate_count: 4,
                required_min_candidates: 3,
                status: "selected".to_string(),
                reason_code: "selected_sufficient_scoped_candidates".to_string(),
                stage_payload: json!({ "stage": "same_line" }),
            }],
        });

        let report = explain_trace_row(&trace);
        let plan = report
            .trace_payload
            .candidate_plan_trace
            .expect("candidate plan summary");

        assert_eq!(plan.selected_stage, "same_line");
        assert_eq!(plan.stop_reason, "dedicated_rows");
        let graph = plan.graph_diagnostics.expect("graph diagnostics summary");
        assert_eq!(graph.geo_graph_status.as_deref(), Some("loaded"));
        assert_eq!(graph.geo_graph_edge_count, Some(1));
        assert_eq!(graph.line_graph_status.as_deref(), Some("not_loaded"));
        assert_eq!(graph.warnings, vec!["line_graph_load_failed".to_string()]);
    }

    #[test]
    fn explain_trace_warns_on_invalid_dedicated_candidate_plan_counts() {
        let mut trace = current_trace_row(json!({
            "feature": "direct_station_bonus",
            "reason_code": "geo.direct_station",
            "value": 2.0,
            "reason": "direct"
        }));
        trace.candidate_plan_trace = Some(storage::RecommendationTraceCandidatePlanTrace {
            minimum_candidate_count: -1,
            selected_stage: "same_line".to_string(),
            stop_reason: "dedicated_rows".to_string(),
            area_context_usable: true,
            plan_payload: json!({ "selected_stage": "same_line" }),
            stages: vec![storage::RecommendationTraceCandidatePlanStage {
                stage_order: 0,
                stage: "same_line".to_string(),
                candidate_count: -1,
                required_min_candidates: 3,
                status: "selected".to_string(),
                reason_code: "selected_sufficient_scoped_candidates".to_string(),
                stage_payload: json!({ "stage": "same_line" }),
            }],
        });

        let report = explain_trace_row(&trace);

        assert_eq!(report.status, ExplainTraceStatus::Warning);
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("candidate_plan.minimum_candidate_count")));
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("candidate_plan_stage.candidate_count")));
    }

    #[test]
    fn explain_trace_reads_context_evidence_from_dedicated_or_payload_rows() {
        let mut trace = current_trace_row(json!({
            "feature": "direct_station_bonus",
            "reason_code": "geo.direct_station",
            "value": 2.0,
            "reason": "direct"
        }));
        trace.trace_payload["context"]["evidence_summary"] = json!({
            "primary_kind": "request_station",
            "evidence_count": 1,
            "strongest_strength": 1.0,
            "has_search_execute": false
        });

        let report = explain_trace_row(&trace);
        let payload_evidence = report
            .trace_payload
            .context_evidence_summary
            .expect("payload evidence");

        assert_eq!(payload_evidence.source, "trace_payload");
        assert_eq!(payload_evidence.primary_kind, "request_station");

        trace.context_evidence_summary = Some(storage::RecommendationTraceContextEvidenceSummary {
            context_source: "recent_search_context".to_string(),
            confidence: 0.8,
            privacy_level: "station_level".to_string(),
            primary_kind: "search_execute".to_string(),
            evidence_count: 2,
            strongest_strength: 0.8,
            has_search_execute: true,
            warning_count: 1,
            evidence_payload: json!({ "primary_kind": "search_execute" }),
        });

        let report = explain_trace_row(&trace);
        let dedicated_evidence = report
            .trace_payload
            .context_evidence_summary
            .expect("dedicated evidence");

        assert_eq!(
            report.trace_payload.context_source.as_deref(),
            Some("recent_search_context")
        );
        assert_eq!(dedicated_evidence.source, "dedicated_rows");
        assert_eq!(dedicated_evidence.primary_kind, "search_execute");
        assert_eq!(dedicated_evidence.warning_count, Some(1));
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
            context_evidence_summary: None,
            candidate_plan_trace: None,
        }
    }
}
