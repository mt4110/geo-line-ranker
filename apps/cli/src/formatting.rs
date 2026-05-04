use storage_postgres::{JobInspection, JobMutationSummary, JobQueueSnapshot};

use crate::{
    fixtures::FixtureDoctorSummary, import::CommandSummary, jobs::JobEnqueueSummary,
    replay::ReplayEvaluationSummary, snapshot::SnapshotRefreshSummary,
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

fn format_order(order: &[String]) -> String {
    if order.is_empty() {
        "-".to_string()
    } else {
        order.join(",")
    }
}
