use std::collections::{BTreeMap, BTreeSet};

use crawler_core::{ParserExpectedShape, SourceMaturity};
use storage_postgres::{
    CrawlParseErrorSnapshot, CrawlRunHealthSnapshot, StoredCrawlFetchLog, StoredCrawlParseError,
};

use crate::manifest::ScaffoldDomainSummary;

#[derive(Debug, Clone)]
pub struct CrawlCommandSummary {
    pub label: String,
    pub crawl_run_id: i64,
    pub fetched_targets: i64,
    pub parsed_rows: i64,
    pub imported_rows: i64,
    pub report_count: usize,
}

#[derive(Debug, Clone)]
pub struct ParserHealthSummary {
    pub manifest_path: String,
    pub source_id: String,
    pub source_name: String,
    pub source_maturity: SourceMaturity,
    pub parser_key: String,
    pub parser_version: String,
    pub expected_shape: Option<ParserExpectedShape>,
    pub total_runs: i64,
    pub shown_runs: usize,
    pub succeeded_runs: usize,
    pub failed_runs: usize,
    pub active_runs: usize,
    pub fetch_status_totals: BTreeMap<String, i64>,
    pub parse_level_totals: BTreeMap<String, i64>,
    pub dedupe_report_total: i64,
    pub recent_runs: Vec<CrawlRunHealthSnapshot>,
    pub recent_reason_trend: Vec<RunReasonTrend>,
    pub logical_name_red_flags: Vec<LogicalNameRedFlag>,
    pub healthy_logical_name_count: usize,
    pub reason_totals: BTreeMap<String, i64>,
}

#[derive(Debug, Clone)]
pub struct LogicalNameRedFlag {
    pub logical_name: String,
    pub reasons: Vec<String>,
    pub latest_fetch_status: Option<String>,
    pub observed_runs: usize,
    pub successful_runs: usize,
    pub red_runs: usize,
    pub consecutive_red_runs: usize,
    pub latest_error: Option<CrawlParseErrorSnapshot>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct LogicalNameRunSignal {
    pub(crate) fetch_logs: Vec<StoredCrawlFetchLog>,
    pub(crate) parse_errors: Vec<StoredCrawlParseError>,
}

#[derive(Debug, Clone)]
pub struct CrawlDoctorSummary {
    pub manifest_path: String,
    pub source_id: String,
    pub source_name: String,
    pub source_maturity: SourceMaturity,
    pub parser_key: String,
    pub parser_registered: bool,
    pub expected_shape: Option<ParserExpectedShape>,
    pub live_fetch_enabled: bool,
    pub robots: UrlProbeSummary,
    pub terms: UrlProbeSummary,
    pub targets: Vec<DoctorTargetSummary>,
    pub issues: Vec<DiagnosticIssue>,
}

#[derive(Debug, Clone)]
pub struct UrlProbeSummary {
    pub requested_url: String,
    pub final_url: Option<String>,
    pub http_status: Option<u16>,
    pub content_type: Option<String>,
    pub error: Option<String>,
    pub body: Option<String>,
    pub body_preview: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DoctorTargetSummary {
    pub logical_name: String,
    pub target_url: String,
    pub school_id: String,
    pub school_exists: Option<bool>,
    pub robots_allowed: Option<bool>,
    pub matched_rule: Option<String>,
    pub expected_shape: Option<ParserExpectedShape>,
    pub shape_status: Option<String>,
    pub shape_detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DiagnosticIssue {
    pub level: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct CrawlDryRunSummary {
    pub manifest_path: String,
    pub source_id: String,
    pub source_name: String,
    pub source_maturity: SourceMaturity,
    pub parser_key: String,
    pub parser_version: String,
    pub expected_shape: Option<ParserExpectedShape>,
    pub crawl_run_id: i64,
    pub ready_targets: usize,
    pub parsed_rows: i64,
    pub deduped_rows: i64,
    pub imported_rows: i64,
    pub deactivated_rows: i64,
    pub missing_school_rows: i64,
    pub date_drift_warnings: usize,
    pub parse_errors: Vec<DiagnosticIssue>,
    pub warnings: Vec<DiagnosticIssue>,
    pub logical_name_summaries: Vec<LogicalDryRunSummary>,
}

#[derive(Debug, Clone)]
pub struct LogicalDryRunSummary {
    pub logical_name: String,
    pub parsed_rows: i64,
    pub date_drift_warnings: usize,
    pub parse_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RunReasonTrend {
    pub crawl_run_id: i64,
    pub status: String,
    pub reasons: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PromotionGate {
    blockers: BTreeSet<String>,
    review: BTreeSet<String>,
}

impl PromotionGate {
    fn status(&self) -> &'static str {
        if !self.blockers.is_empty() {
            "blocked"
        } else if !self.review.is_empty() {
            "review"
        } else {
            "ready"
        }
    }
}

pub fn format_summary(summary: &CrawlCommandSummary) -> String {
    format!(
        "{} completed: crawl_run_id={}, fetched={}, parsed={}, imported={}, reports={}",
        summary.label,
        summary.crawl_run_id,
        summary.fetched_targets,
        summary.parsed_rows,
        summary.imported_rows,
        summary.report_count
    )
}

pub fn format_doctor_summary(summary: &CrawlDoctorSummary) -> String {
    let promotion_gate = doctor_promotion_gate(summary);
    let mut lines = vec![
        format!(
            "crawler doctor for {} ({})",
            summary.source_id, summary.source_name
        ),
        format!("manifest: {}", summary.manifest_path),
        format!("source_maturity: {}", summary.source_maturity),
        format!(
            "parser: {} registered={}",
            summary.parser_key, summary.parser_registered
        ),
        format!(
            "expected_shape: {}",
            summary
                .expected_shape
                .map(|shape| shape.to_string())
                .unwrap_or_else(|| "-".to_string())
        ),
        format!("live_fetch_enabled: {}", summary.live_fetch_enabled),
        format_promotion_gate(&promotion_gate),
        format!(
            "robots: requested={} status={} content_type={} final_url={}",
            summary.robots.requested_url,
            summary
                .robots
                .http_status
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            summary.robots.content_type.as_deref().unwrap_or("-"),
            summary.robots.final_url.as_deref().unwrap_or("-")
        ),
        format!(
            "terms: requested={} status={} content_type={} final_url={}",
            summary.terms.requested_url,
            summary
                .terms
                .http_status
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            summary.terms.content_type.as_deref().unwrap_or("-"),
            summary.terms.final_url.as_deref().unwrap_or("-")
        ),
    ];

    if summary.issues.is_empty() {
        lines.push("issues: none".to_string());
    } else {
        lines.push("issues:".to_string());
        for issue in &summary.issues {
            lines.push(format!(
                "- [{}] {} {}",
                issue.level, issue.code, issue.message
            ));
        }
    }

    lines.push("targets:".to_string());
    for target in &summary.targets {
        lines.push(format!(
            "- {} school_id={} school_exists={} robots_allowed={} matched_rule={} expected_shape={} shape={}",
            target.logical_name,
            target.school_id,
            target
                .school_exists
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            target
                .robots_allowed
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            target.matched_rule.as_deref().unwrap_or("-"),
            target
                .expected_shape
                .map(|shape| shape.to_string())
                .unwrap_or_else(|| "-".to_string()),
            target.shape_status.as_deref().unwrap_or("-")
        ));
        lines.push(format!("  target_url: {}", target.target_url));
        if let Some(shape_detail) = &target.shape_detail {
            lines.push(format!("  shape_detail: {}", shape_detail));
        }
    }

    lines.join("\n")
}

pub fn format_dry_run_summary(summary: &CrawlDryRunSummary) -> String {
    let promotion_gate = dry_run_promotion_gate(summary);
    let mut lines = vec![
        format!(
            "crawler dry-run for {} ({})",
            summary.source_id, summary.source_name
        ),
        format!("manifest: {}", summary.manifest_path),
        format!("source_maturity: {}", summary.source_maturity),
        format!("parser: {}@{}", summary.parser_key, summary.parser_version),
        format!(
            "expected_shape: {}",
            summary
                .expected_shape
                .map(|shape| shape.to_string())
                .unwrap_or_else(|| "-".to_string())
        ),
        format!("using fetched run: {}", summary.crawl_run_id),
        format!(
            "prediction: ready_targets={} parsed={} deduped={} imported={} inactive={} missing_school={} date_drift_warnings={}",
            summary.ready_targets,
            summary.parsed_rows,
            summary.deduped_rows,
            summary.imported_rows,
            summary.deactivated_rows,
            summary.missing_school_rows,
            summary.date_drift_warnings
        ),
        format_promotion_gate(&promotion_gate),
    ];

    if summary.parse_errors.is_empty() && summary.warnings.is_empty() {
        lines.push("warnings: none".to_string());
    } else {
        lines.push("warnings:".to_string());
        for issue in summary.parse_errors.iter().chain(summary.warnings.iter()) {
            lines.push(format!(
                "- [{}] {} {}",
                issue.level, issue.code, issue.message
            ));
        }
    }

    lines.push("logical_name summary:".to_string());
    for logical in &summary.logical_name_summaries {
        lines.push(format!(
            "- {} parsed={} date_drift_warnings={} parse_error={}",
            logical.logical_name,
            logical.parsed_rows,
            logical.date_drift_warnings,
            logical.parse_error.as_deref().unwrap_or("-")
        ));
    }

    lines.join("\n")
}

pub fn format_health_summary(summary: &ParserHealthSummary) -> String {
    let promotion_gate = health_promotion_gate(summary);
    let mut lines = vec![
        format!(
            "parser health for {} ({})",
            summary.source_id, summary.source_name
        ),
        format!("manifest: {}", summary.manifest_path),
        format!("source_maturity: {}", summary.source_maturity),
        format!("parser: {}@{}", summary.parser_key, summary.parser_version),
        format!(
            "expected_shape: {}",
            summary
                .expected_shape
                .map(|shape| shape.to_string())
                .unwrap_or_else(|| "-".to_string())
        ),
        format!(
            "runs: total={}, showing={}, succeeded={}, failed={}, active={}",
            summary.total_runs,
            summary.shown_runs,
            summary.succeeded_runs,
            summary.failed_runs,
            summary.active_runs
        ),
        format!(
            "aggregate over shown runs: fetch[{}] parse[{}] dedupe_reports={}",
            format_count_map(&summary.fetch_status_totals),
            format_count_map(&summary.parse_level_totals),
            summary.dedupe_report_total
        ),
        format_promotion_gate(&promotion_gate),
    ];

    if summary.recent_runs.is_empty() {
        lines.push("no crawl runs recorded yet".to_string());
        return lines.join("\n");
    }

    lines.push(format!(
        "logical_name signals: healthy={}, red={}",
        summary.healthy_logical_name_count,
        summary.logical_name_red_flags.len()
    ));
    lines.push(format!(
        "reason totals: {}",
        format_count_map(&summary.reason_totals)
    ));
    lines.push("recent reason trend:".to_string());
    for trend in summary.recent_reason_trend.iter().take(5) {
        lines.push(format!(
            "#{} {} reasons[{}]",
            trend.crawl_run_id,
            trend.status,
            format_count_map(&trend.reasons)
        ));
    }
    if summary.logical_name_red_flags.is_empty() {
        lines.push("red flags: none".to_string());
    } else {
        lines.push("red flags:".to_string());
        for flag in &summary.logical_name_red_flags {
            lines.push(format!(
                "- {} reasons={} latest_fetch={} observed_runs={} successful_runs={} red_runs={} consecutive_red_runs={}",
                flag.logical_name,
                flag.reasons.join(","),
                flag.latest_fetch_status.as_deref().unwrap_or("-"),
                flag.observed_runs,
                flag.successful_runs,
                flag.red_runs,
                flag.consecutive_red_runs
            ));
            if let Some(error) = &flag.latest_error {
                lines.push(format!(
                    "  latest_error: [{}] {}",
                    error.code, error.message
                ));
            }
        }
    }

    lines.push("recent runs:".to_string());
    for run in &summary.recent_runs {
        lines.push(format!(
            "#{} {} fetched={} parsed={} imported={} started={} completed={} fetch[{}] parse[{}] dedupe={}",
            run.crawl_run_id,
            run.status,
            run.fetched_targets,
            run.parsed_rows,
            run.imported_rows,
            run.started_at,
            run.completed_at.as_deref().unwrap_or("-"),
            format_count_map(&run.fetch_status_counts),
            format_count_map(&run.parse_level_counts),
            run.dedupe_count
        ));
        if let Some(error) = &run.latest_error {
            lines.push(format!(
                "  last_error: [{}] {}{}",
                error.code,
                error.message,
                error
                    .logical_name
                    .as_deref()
                    .map(|logical_name| format!(" (logical_name={logical_name})"))
                    .unwrap_or_default()
            ));
        }
    }

    lines.join("\n")
}

pub fn format_scaffold_summary(summary: &ScaffoldDomainSummary) -> String {
    format!(
        "scaffold-domain completed: manifest={} fixture={} guide={} source_maturity={} expected_shape={}",
        summary.manifest_path,
        summary.fixture_path,
        summary.guide_path,
        summary.source_maturity,
        summary.expected_shape
    )
}

pub(crate) fn merge_counts(target: &mut BTreeMap<String, i64>, source: &BTreeMap<String, i64>) {
    for (key, value) in source {
        *target.entry(key.clone()).or_insert(0) += value;
    }
}

pub(crate) fn summarize_fetch_status(fetch_logs: &[StoredCrawlFetchLog]) -> Option<String> {
    if fetch_logs.is_empty() {
        return None;
    }

    if fetch_logs
        .iter()
        .any(|log| log.fetch_status == "fetch_failed")
    {
        return Some("fetch_failed".to_string());
    }
    if fetch_logs
        .iter()
        .any(|log| log.fetch_status == "blocked_robots")
    {
        return Some("blocked_robots".to_string());
    }
    if fetch_logs
        .iter()
        .any(|log| log.fetch_status == "blocked_policy")
    {
        return Some("blocked_policy".to_string());
    }
    if fetch_logs
        .iter()
        .all(|log| log.fetch_status == "not_modified")
    {
        return Some("not_modified".to_string());
    }
    if fetch_logs.iter().any(|log| log.fetch_status == "fetched") {
        return Some("fetched".to_string());
    }

    fetch_logs.first().map(|log| log.fetch_status.clone())
}

pub(crate) fn can_deactivate_stale_rows(
    fetch_logs: &[StoredCrawlFetchLog],
    parse_failed_targets: usize,
    zero_row_targets: usize,
) -> bool {
    !fetch_logs.is_empty()
        && parse_failed_targets == 0
        && zero_row_targets == 0
        && fetch_logs
            .iter()
            .all(|entry| matches!(entry.fetch_status.as_str(), "fetched" | "not_modified"))
}

pub(crate) fn summarize_parse_error(
    parse_errors: &[StoredCrawlParseError],
) -> Option<CrawlParseErrorSnapshot> {
    parse_errors.last().map(|error| CrawlParseErrorSnapshot {
        logical_name: Some(error.logical_name.clone()),
        code: error.code.clone(),
        message: error.message.clone(),
    })
}

pub(crate) fn is_red_signal(
    fetch_status: Option<&str>,
    parse_error: Option<&CrawlParseErrorSnapshot>,
    observed: bool,
) -> bool {
    if !observed {
        return true;
    }

    matches!(
        fetch_status,
        Some("fetch_failed" | "blocked_robots" | "blocked_policy")
    ) || parse_error.is_some()
}

pub(crate) fn is_green_signal(
    fetch_status: Option<&str>,
    parse_error: Option<&CrawlParseErrorSnapshot>,
) -> bool {
    matches!(fetch_status, Some("fetched" | "not_modified")) && parse_error.is_none()
}

fn doctor_promotion_gate(summary: &CrawlDoctorSummary) -> PromotionGate {
    let mut gate = PromotionGate::default();

    if summary.source_maturity != SourceMaturity::LiveReady {
        gate.blockers
            .insert("source_maturity_not_live_ready".to_string());
    }
    if !summary.live_fetch_enabled {
        gate.blockers.insert("live_fetch_disabled".to_string());
    }
    if !summary.parser_registered {
        gate.blockers.insert("unknown_parser_key".to_string());
    }

    for issue in &summary.issues {
        if issue.level == "error" || doctor_issue_blocks_promotion(&issue.code) {
            gate.blockers.insert(issue.code.clone());
        } else {
            gate.review.insert(issue.code.clone());
        }
    }

    for target in &summary.targets {
        if matches!(target.robots_allowed, Some(false)) {
            gate.blockers
                .insert(format!("blocked_robots:{}", target.logical_name));
        }
        match target.shape_status.as_deref() {
            Some("matched") | None => {}
            Some("skipped")
                if summary.source_maturity == SourceMaturity::LiveReady
                    && summary.live_fetch_enabled =>
            {
                gate.blockers
                    .insert(format!("shape_probe_skipped:{}", target.logical_name));
            }
            Some(status) => {
                gate.blockers
                    .insert(format!("shape_{status}:{}", target.logical_name));
            }
        }
    }

    gate
}

fn dry_run_promotion_gate(summary: &CrawlDryRunSummary) -> PromotionGate {
    let mut gate = PromotionGate::default();

    if summary.source_maturity != SourceMaturity::LiveReady {
        gate.blockers
            .insert("source_maturity_not_live_ready".to_string());
    }
    if summary.ready_targets == 0 {
        gate.blockers.insert("no_ready_targets".to_string());
    }
    if summary.parsed_rows == 0 {
        gate.blockers.insert("parsed_zero_rows".to_string());
    }
    if summary.imported_rows == 0 {
        gate.blockers.insert("imported_zero_rows".to_string());
    }
    if summary.missing_school_rows > 0 {
        gate.blockers.insert("missing_school_id".to_string());
    }

    for issue in &summary.parse_errors {
        gate.blockers.insert(issue.code.clone());
    }
    for issue in &summary.warnings {
        if dry_run_issue_blocks_promotion(&issue.code) {
            gate.blockers.insert(issue.code.clone());
        } else {
            gate.review.insert(issue.code.clone());
        }
    }

    gate
}

fn health_promotion_gate(summary: &ParserHealthSummary) -> PromotionGate {
    let mut gate = PromotionGate::default();

    if summary.source_maturity != SourceMaturity::LiveReady {
        gate.blockers
            .insert("source_maturity_not_live_ready".to_string());
    }
    if summary.shown_runs == 0 {
        gate.blockers.insert("no_recent_runs".to_string());
    }
    if summary.succeeded_runs == 0 {
        gate.blockers.insert("no_succeeded_runs".to_string());
    }
    if summary.failed_runs > 0 {
        gate.blockers.insert("recent_failed_runs".to_string());
    }
    if summary.active_runs > 0 {
        gate.review.insert("active_runs".to_string());
    }
    if !summary.logical_name_red_flags.is_empty() {
        gate.blockers.insert("logical_name_red_flags".to_string());
    }
    for reason in summary.reason_totals.keys() {
        gate.blockers.insert(format!("reason:{reason}"));
    }

    gate
}

fn doctor_issue_blocks_promotion(code: &str) -> bool {
    matches!(
        code,
        "unknown_parser_key"
            | "live_fetch_disabled"
            | "missing_school_id"
            | "target_shape_fetch_failed"
            | "fixture_shape_read_failed"
            | "expected_shape_mismatch"
            | "robots_fetch_failed"
            | "robots_bad_status"
            | "robots_unexpected_content_type"
            | "robots_html_body"
            | "terms_fetch_failed"
            | "terms_bad_status"
    )
}

fn dry_run_issue_blocks_promotion(code: &str) -> bool {
    matches!(
        code,
        "parse_failed"
            | "parsed_zero_rows"
            | "no_events_found"
            | "missing_school_id"
            | "partial_import_skips_stale_deactivation"
            | "missing_school_skips_stale_deactivation"
    )
}

fn format_promotion_gate(gate: &PromotionGate) -> String {
    format!(
        "promotion_gate: {} blockers[{}] review[{}]",
        gate.status(),
        format_string_set(&gate.blockers),
        format_string_set(&gate.review)
    )
}

fn format_count_map(counts: &BTreeMap<String, i64>) -> String {
    if counts.is_empty() {
        return "-".to_string();
    }

    counts
        .iter()
        .map(|(key, value)| format!("{key}:{value}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_string_set(values: &BTreeSet<String>) -> String {
    if values.is_empty() {
        return "-".to_string();
    }

    values.iter().cloned().collect::<Vec<_>>().join(", ")
}

pub(crate) fn normalize_reason_for_total(reason: &str) -> Option<String> {
    match reason {
        "latest_blocked_policy" => Some("blocked_policy".to_string()),
        "latest_blocked_robots" => Some("blocked_robots".to_string()),
        "latest_fetch_failed" => Some("fetch_failed".to_string()),
        "missing_from_latest_run" => Some("missing_from_latest_run".to_string()),
        "no_recent_data" => Some("no_recent_data".to_string()),
        value if value.starts_with("latest_parse_error:") => Some(value.to_string()),
        _ => None,
    }
}
