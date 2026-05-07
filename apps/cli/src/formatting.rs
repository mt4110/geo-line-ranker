use storage_postgres::{JobInspection, JobMutationSummary, JobQueueSnapshot};

use crate::{
    doctor::{ExplanationIntegrityDoctorSummary, ProfilePackDoctorSummary},
    explain::{ExplainTracePayloadSummary, ExplainTraceReasonSummary, ExplainTraceReport},
    explanation_integrity::QualityCheckStatus,
    fixtures::FixtureDoctorSummary,
    import::CommandSummary,
    jobs::JobEnqueueSummary,
    replay::{ReplayEvaluationSummary, ReplayScenarioSummary},
    snapshot::SnapshotRefreshSummary,
};

pub fn format_summary(summary: &CommandSummary) -> String {
    match summary.import_run_id {
        Some(import_run_id) => format!(
            "{} completed: run_id={}, rows={}, reports={}",
            summary.label, import_run_id, summary.row_count, summary.report_count
        ),
        None => format!(
            "{} completed: rows={}, reports={}",
            summary.label, summary.row_count, summary.report_count
        ),
    }
}

pub fn format_fixture_doctor_summary(summary: &FixtureDoctorSummary) -> String {
    let mut lines = vec![format!(
        "fixture doctor ok: fixture_set_id={} profile_id={} manifest_version={} files={}",
        summary.fixture_set_id,
        summary.profile_id.as_deref().unwrap_or("-"),
        summary.manifest_version,
        summary.files.len()
    )];
    lines.push(format!("manifest: {}", summary.manifest_path.display()));
    lines.extend(summary.files.iter().map(|file| {
        format!(
            "- {} format={} rows={} checksum_sha256={} path={}",
            file.logical_name,
            file.format,
            file.row_count,
            file.checksum_sha256,
            file.path.display()
        )
    }));
    lines.join("\n")
}

pub fn format_snapshot_refresh_summary(summary: &SnapshotRefreshSummary) -> String {
    format!(
        "snapshot refresh completed: school_rows={}, area_rows={}, cache_deleted={}, projection_indexed={}, projection_deleted={}, school_weight={}, area_weight={}",
        summary.refreshed_school_rows,
        summary.refreshed_area_rows,
        summary.invalidated_cache_keys,
        summary.projection_indexed_documents,
        summary.projection_deleted_documents,
        summary.search_execute_school_signal_weight,
        summary.search_execute_area_signal_weight
    )
}

pub fn format_job_list(snapshot: &JobQueueSnapshot) -> String {
    let mut lines = vec!["job queue".to_string()];
    if snapshot.jobs.is_empty() {
        lines.push("recent: -".to_string());
    } else {
        lines.push("recent:".to_string());
        for job in &snapshot.jobs {
            lines.push(format!(
                "  id={} type={} status={} attempts={}/{} run_after={} completed_at={} last_error={}",
                job.id,
                job.job_type,
                job.status,
                job.attempts,
                job.max_attempts,
                job.run_after,
                job.completed_at.as_deref().unwrap_or("-"),
                job.last_error.as_deref().unwrap_or("-")
            ));
        }
    }

    if snapshot.pressure.is_empty() {
        lines.push("pressure: -".to_string());
    } else {
        lines.push("pressure:".to_string());
        for row in &snapshot.pressure {
            lines.push(format!(
                "  type={} status={} count={} oldest_run_after={} latest_update={}",
                row.job_type,
                row.status,
                row.job_count,
                row.oldest_run_after.as_deref().unwrap_or("-"),
                row.latest_update.as_deref().unwrap_or("-")
            ));
        }
    }

    lines.join("\n")
}

pub fn format_job_inspection(inspection: &JobInspection) -> String {
    let job = &inspection.job;
    let mut lines = vec![
        format!("job id={}", job.id),
        format!("type: {}", job.job_type),
        format!("status: {}", job.status),
        format!("attempts: {}/{}", job.attempts, job.max_attempts),
        format!("run_after: {}", job.run_after),
        format!("locked_by: {}", job.locked_by.as_deref().unwrap_or("-")),
        format!("locked_at: {}", job.locked_at.as_deref().unwrap_or("-")),
        format!(
            "completed_at: {}",
            job.completed_at.as_deref().unwrap_or("-")
        ),
        format!("last_error: {}", job.last_error.as_deref().unwrap_or("-")),
        format!("payload: {}", job.payload),
    ];

    if inspection.attempts.is_empty() {
        lines.push("attempts_detail: -".to_string());
    } else {
        lines.push("attempts_detail:".to_string());
        for attempt in &inspection.attempts {
            lines.push(format!(
                "  attempt={} status={} started_at={} finished_at={} error={}",
                attempt.attempt_number,
                attempt.status,
                attempt.started_at,
                attempt.finished_at.as_deref().unwrap_or("-"),
                attempt.error_message.as_deref().unwrap_or("-")
            ));
        }
    }

    lines.join("\n")
}

pub fn format_job_mutation_summary(action: &str, summary: &JobMutationSummary) -> String {
    let outcome = if summary.updated {
        "updated"
    } else {
        "skipped"
    };
    format!(
        "job {action} {outcome}: id={} type={} status={} attempts={}/{} run_after={} last_error={}",
        summary.job.id,
        summary.job.job_type,
        summary.job.status,
        summary.job.attempts,
        summary.job.max_attempts,
        summary.job.run_after,
        summary.job.last_error.as_deref().unwrap_or("-")
    )
}

pub fn format_job_enqueue_summary(summary: &JobEnqueueSummary) -> String {
    format!(
        "job enqueued: id={} type={} max_attempts={} payload={}",
        summary.job_id,
        summary.job_type.as_str(),
        summary.max_attempts,
        summary.payload
    )
}

pub fn format_explain_trace_report(report: &ExplainTraceReport) -> String {
    let mut lines = vec![format!(
        "explain trace: id={} status={} created_at={} algorithm_version={}",
        report.trace_id,
        report.status.as_str(),
        report.created_at,
        report.algorithm_version
    )];

    lines.push(format!(
        "request: request_id={} user_id_present={} placement={} target_station_id={} limit={} debug={}",
        optional_str(report.request.request_id.as_deref()),
        report.request.user_id_present,
        optional_str(report.request.placement.as_deref()),
        optional_str(report.request.target_station_id.as_deref()),
        optional_usize(report.request.limit),
        optional_bool(report.request.debug)
    ));
    lines.push(format!(
        "response: shape={} fallback={}=>{} items={}",
        report.response.payload_shape,
        report.response.db_fallback_stage,
        optional_str(report.response.response_fallback_stage.as_deref()),
        report.response.item_count
    ));
    lines.push(format!(
        "result_order: {}",
        format_order(&report.response.result_order)
    ));
    lines.push(format_trace_payload_summary(&report.trace_payload));
    lines.push(format!(
        "result_reasons: {}",
        format_reasons(&report.response.top_reasons)
    ));

    if report.response.items.is_empty() {
        lines.push("item_reasons: -".to_string());
    } else {
        lines.push("item_reasons:".to_string());
        for item in &report.response.items {
            lines.push(format!(
                "  {} score={} fallback={} reasons={}",
                item.item_key,
                optional_f64(item.score),
                optional_str(item.fallback_stage.as_deref()),
                format_reasons(&item.reasons)
            ));
        }
    }

    lines.push(format!(
        "integrity: passed={} failed={}",
        report.integrity.passed, report.integrity.failed
    ));
    for check in report
        .integrity
        .checks
        .iter()
        .filter(|check| check.status == QualityCheckStatus::Failed)
    {
        lines.push(format!(
            "  check={} severity={} status={} message={}",
            check.name,
            check.severity.as_str(),
            check.status.as_str(),
            check.message
        ));
    }

    if !report.warnings.is_empty() {
        lines.push("warnings:".to_string());
        lines.extend(
            report
                .warnings
                .iter()
                .map(|warning| format!("  - {warning}")),
        );
    }

    lines.join("\n")
}

pub fn format_replay_evaluation_summary(summary: &ReplayEvaluationSummary) -> String {
    let mut lines = vec![format!(
        "replay evaluation completed: evaluated={}, matched={}, mismatched={}, failed={}",
        summary.evaluated, summary.matched, summary.mismatched, summary.failed
    )];

    for case in &summary.cases {
        let expected = format_order(&case.expected_order);
        let actual = format_order(&case.actual_order);
        lines.push(format!(
            "  trace_id={} status={} request_id={} fallback={}=>{} items={}=>{}{}",
            case.trace_id,
            case.status.as_str(),
            case.request_id.as_deref().unwrap_or("-"),
            case.expected_fallback_stage.as_deref().unwrap_or("-"),
            case.actual_fallback_stage.as_deref().unwrap_or("-"),
            expected,
            actual,
            case.message
                .as_ref()
                .map(|message| format!(" message={message}"))
                .unwrap_or_default()
        ));
    }

    lines.join("\n")
}

pub fn format_replay_scenario_summary(summary: &ReplayScenarioSummary) -> String {
    let mut lines = vec![format!(
        "replay scenarios completed: scenarios={}, passed={}, blocked={}, blockers={}, warnings={}, pairwise={}/{}, explanation_integrity={}/{}",
        summary.scenarios,
        summary.passed,
        summary.blocked,
        summary.blockers,
        summary.warnings,
        summary.pairwise_passed,
        summary.pairwise_total,
        summary.explanation_integrity_passed,
        summary.explanation_integrity_total
    )];

    for case in &summary.cases {
        lines.push(format!(
            "  scenario_id={} status={} fallback={}=>{} items={}=>{} path={}",
            case.id,
            case.status.as_str(),
            case.expected_fallback_stage,
            case.actual_fallback_stage.as_deref().unwrap_or("-"),
            format_order(&case.expected_order),
            format_order(&case.actual_order),
            case.path.display()
        ));
        for check in case
            .checks
            .iter()
            .filter(|check| check.status == QualityCheckStatus::Failed)
        {
            lines.push(format!(
                "    check={} severity={} status={} message={}",
                check.name,
                check.severity.as_str(),
                check.status.as_str(),
                check.message
            ));
        }
    }

    lines.join("\n")
}

pub fn format_explanation_integrity_doctor_summary(
    summary: &ExplanationIntegrityDoctorSummary,
) -> String {
    let mut lines = vec![format!(
        "doctor explanation-integrity completed: scenarios={}, passed={}, blocked={}, blockers={}, warnings={}, explanation_integrity={}/{}",
        summary.scenarios,
        summary.passed,
        summary.blocked,
        summary.blockers,
        summary.warnings,
        summary.explanation_integrity_passed,
        summary.explanation_integrity_total
    )];

    for case in &summary.cases {
        let case_passed = case
            .checks
            .iter()
            .filter(|check| check.status == QualityCheckStatus::Passed)
            .count();
        lines.push(format!(
            "  scenario_id={} status={} explanation_integrity={}/{} path={}",
            case.id,
            case.status.as_str(),
            case_passed,
            case.checks.len(),
            case.path.display()
        ));
        for check in case
            .checks
            .iter()
            .filter(|check| check.status == QualityCheckStatus::Failed)
        {
            lines.push(format!(
                "    check={} severity={} status={} message={}",
                check.name,
                check.severity.as_str(),
                check.status.as_str(),
                check.message
            ));
        }
    }

    lines.join("\n")
}

pub fn format_profile_pack_doctor_summary(summary: &ProfilePackDoctorSummary) -> String {
    let mut lines = vec![format!(
        "doctor profile-pack completed: profile_packs={}, ranking_config_dirs={}, reasons={}, fixture_references={}, source_manifest_references={}, event_csv_example_references={}, optional_crawler_manifest_references={}",
        summary.profile_packs,
        summary.ranking_config_dirs,
        summary.reason_count,
        summary.fixture_references,
        summary.source_manifest_references,
        summary.event_csv_example_references,
        summary.optional_crawler_manifest_references
    )];

    for file in &summary.files {
        lines.push(format!(
            "  profile_id={} content_kinds={} reasons={} fixtures={} source_manifests={} event_csv_examples={} optional_crawler_manifests={} manifest={} ranking_config_dir={} reason_catalog={}",
            file.profile_id,
            file.supported_content_kinds.join(","),
            file.reason_count,
            file.fixture_references,
            file.source_manifest_references,
            file.event_csv_example_references,
            file.optional_crawler_manifest_references,
            file.path.display(),
            file.ranking_config_dir.display(),
            file.reason_catalog_path.display()
        ));
    }

    lines.join("\n")
}

fn format_order(order: &[String]) -> String {
    if order.is_empty() {
        "-".to_string()
    } else {
        order.join(",")
    }
}

fn format_trace_payload_summary(summary: &ExplainTracePayloadSummary) -> String {
    format!(
        "trace_payload: response_source={} context={} confidence={} privacy={} retrieval={}/{} candidate_count={} duration_ms={} suppressed_item_reasons={}",
        optional_str(summary.response_source.as_deref()),
        optional_str(summary.context_source.as_deref()),
        optional_f64(summary.context_confidence),
        optional_str(summary.privacy_level.as_deref()),
        optional_str(summary.candidate_retrieval_mode.as_deref()),
        optional_str(summary.candidate_retrieval_backend.as_deref()),
        optional_usize(summary.candidate_count),
        optional_u64(summary.duration_ms),
        if summary.suppressed_item_reasons_recorded {
            match summary.suppressed_item_count {
                Some(count) => format!("recorded({count})"),
                None => "recorded".to_string(),
            }
        } else {
            "not_recorded".to_string()
        }
    )
}

fn format_reasons(reasons: &[ExplainTraceReasonSummary]) -> String {
    if reasons.is_empty() {
        return "-".to_string();
    }
    reasons
        .iter()
        .map(|reason| {
            format!(
                "{}:{}({})={:.3}",
                reason.feature, reason.reason_code, reason.label, reason.value
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn optional_str(value: Option<&str>) -> &str {
    value.unwrap_or("-")
}

fn optional_bool(value: Option<bool>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn optional_usize(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn optional_f64(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.3}"))
        .unwrap_or_else(|| "-".to_string())
}
