#[cfg(feature = "storage-backends")]
use storage_postgres::{JobInspection, JobMutationSummary, JobQueueSnapshot};

use crate::{
    doctor::{
        ConnectorSchemaContractSummary, ContextCoverageDoctorSummary,
        ExplanationIntegrityDoctorSummary, IngestQualityDoctorSummary, ProfilePackDoctorSummary,
        RankingConfigDoctorSummary, RetrievalParityDoctorSummary,
        StorageCompatibilityDoctorSummary,
    },
    explanation_integrity::QualityCheckStatus,
    fixtures::FixtureDoctorSummary,
    replay::{ReplayEvaluationSummary, ReplayScenarioSummary},
};
#[cfg(feature = "storage-backends")]
use crate::{
    explain::{
        ExplainTraceCandidatePlanGraphDiagnosticsSummary, ExplainTraceCandidatePlanSummary,
        ExplainTracePayloadSummary, ExplainTraceReasonSummary, ExplainTraceReport,
    },
    import::CommandSummary,
    jobs::JobEnqueueSummary,
    snapshot::SnapshotRefreshSummary,
};

#[cfg(feature = "storage-backends")]
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

#[cfg(feature = "storage-backends")]
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

#[cfg(feature = "storage-backends")]
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

#[cfg(feature = "storage-backends")]
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

#[cfg(feature = "storage-backends")]
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

#[cfg(feature = "storage-backends")]
pub fn format_job_enqueue_summary(summary: &JobEnqueueSummary) -> String {
    format!(
        "job enqueued: id={} type={} max_attempts={} payload={}",
        summary.job_id,
        summary.job_type.as_str(),
        summary.max_attempts,
        summary.payload
    )
}

#[cfg(feature = "storage-backends")]
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
    format_replay_evaluation_summary_with_label("replay evaluation", summary)
}

#[cfg(feature = "storage-backends")]
pub fn format_eval_replay_summary(summary: &ReplayEvaluationSummary) -> String {
    format_replay_evaluation_summary_with_label("eval replay", summary)
}

fn format_replay_evaluation_summary_with_label(
    label: &str,
    summary: &ReplayEvaluationSummary,
) -> String {
    let mut lines = vec![format!(
        "{label} completed: evaluated={}, matched={}, mismatched={}, failed={}",
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
    format_replay_scenario_summary_with_label("replay scenarios", summary)
}

pub fn format_eval_golden_summary(summary: &ReplayScenarioSummary) -> String {
    format_replay_scenario_summary_with_label("eval golden", summary)
}

fn format_replay_scenario_summary_with_label(
    label: &str,
    summary: &ReplayScenarioSummary,
) -> String {
    let mut lines = vec![format!(
        "{label} completed: profile_id={}, scenario_source={}, scenario_path={}, scenarios={}, passed={}, blocked={}, blockers={}, warnings={}, pairwise={}/{}, explanation_integrity={}/{}",
        summary.profile_id.as_deref().unwrap_or("-"),
        summary.scenario_source.kind.as_str(),
        summary.scenario_source.path.display(),
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
    if let Some(pairwise_pack) = summary.scenario_source.pairwise_pack.as_deref() {
        lines.push(format!("  pairwise_pack={}", pairwise_pack.display()));
    }

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
        "doctor profile-pack completed: profile_packs={}, ranking_config_dirs={}, reason_catalog_locales={}, reasons={}, fixture_references={}, connector_references={}, evaluation_references={}, source_manifest_references={}, event_csv_example_references={}, archive_source_references={}, optional_crawler_manifest_references={}, connector_schema_contract_version={}, connector_schema_contracts={}",
        summary.profile_packs,
        summary.ranking_config_dirs,
        summary.reason_catalog_locales,
        summary.reason_count,
        summary.fixture_references,
        summary.connector_references,
        summary.evaluation_references,
        summary.source_manifest_references,
        summary.event_csv_example_references,
        summary.archive_source_references,
        summary.optional_crawler_manifest_references,
        summary.connector_schema_contract_version,
        summary.connector_schema_contracts.len()
    )];
    push_connector_schema_contract_lines(&mut lines, &summary.connector_schema_contracts);

    for file in &summary.files {
        lines.push(format!(
            "  profile_id={} compatibility_level={} content_kind_registry={} content_kinds={} runtime_executable_content_kinds={} registry_only_content_kinds={} placements={} reason_catalog_locales={} reasons={} fixtures={} connectors={} evaluation_refs={} source_manifests={} event_csv_examples={} archive_sources={} optional_crawler_manifests={} manifest={} ranking_config_dir={} fallback_config={} reason_catalog={}",
            file.profile_id,
            file.compatibility_level,
            file.content_kind_registry.join(","),
            file.supported_content_kinds.join(","),
            format_order(&file.runtime_executable_content_kinds),
            format_order(&file.registry_only_content_kinds),
            file.placements.join(","),
            file.reason_catalog_locale_count,
            file.reason_count,
            file.fixture_references,
            file.connector_references,
            file.evaluation_references,
            file.source_manifest_references,
            file.event_csv_example_references,
            file.archive_source_references,
            file.optional_crawler_manifest_references,
            file.path.display(),
            file.ranking_config_dir.display(),
            file.fallback_config_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "none".to_string()),
            file.reason_catalog_path.display()
        ));
        for connector in &file.connector_registry {
            lines.push(format!(
                "    connector type={} source_class={} manifest_kind={} manifest_schema_version={} source_id={} field_mapping={} profile_compatibility={} safety=local_reference_only:{},dynamic_loading_enabled:{},live_fetch_default:{},allowlist_required:{} manifest={}",
                connector.connector_type.as_str(),
                connector.source_class.as_str(),
                connector.manifest_kind,
                format_option_u32(connector.manifest_schema_version),
                connector.source_id.as_deref().unwrap_or("none"),
                connector
                    .field_mapping
                    .as_ref()
                    .map(|mapping| mapping.as_str())
                    .unwrap_or("none"),
                connector.profile_compatibility.as_str(),
                connector.safety.local_reference_only,
                connector.safety.dynamic_loading_enabled,
                connector.safety.live_fetch_default,
                connector.safety.allowlist_required,
                connector.manifest_path.display()
            ));
        }
    }

    lines.join("\n")
}

pub fn format_ingest_quality_doctor_summary(summary: &IngestQualityDoctorSummary) -> String {
    let mut lines = vec![format!(
        "doctor ingest-quality completed: profile_packs={}, connectors={}, source_classes={}, manifest_kinds={}, manifest_schema_versions={}, runtime_executable_mappings={}, non_runtime_mappings={}, source_manifest_files={}, archive_files={}, crawler_targets={}, local_reference_only={}, dynamic_loading_enabled={}, live_fetch_default={}, crawler_allowlist_required={}, connector_schema_contract_version={}, connector_schema_contracts={}",
        summary.profile_packs,
        summary.connector_references,
        format_counts(&summary.source_class_counts),
        format_counts(&summary.manifest_kind_counts),
        format_counts(&summary.manifest_schema_version_counts),
        summary.runtime_executable_mappings,
        summary.non_runtime_mappings,
        summary.source_manifest_file_count,
        summary.archive_file_count,
        summary.crawler_target_count,
        summary.local_reference_only_connectors,
        summary.dynamic_loading_enabled_connectors,
        summary.live_fetch_default_connectors,
        summary.crawler_allowlist_required_connectors,
        summary.connector_schema_contract_version,
        summary.connector_schema_contracts.len()
    )];
    push_connector_schema_contract_lines(&mut lines, &summary.connector_schema_contracts);
    lines.push(format!(
        "evidence_scope: {} execution_scope: {}",
        summary.evidence_scope, summary.execution_scope
    ));
    lines.push(format!(
        "profile_references: source_manifests={} event_csv_examples={} archive_sources={} optional_crawler_manifests={}",
        summary.source_manifest_references,
        summary.event_csv_example_references,
        summary.archive_source_references,
        summary.optional_crawler_manifest_references
    ));
    lines.push(format!(
        "run_lineage: profile_source_import_connectors={} crawler_contract_connectors={} fields={}",
        summary.profile_source_import_lineage_connectors,
        summary.crawler_lineage_contract_connectors,
        summary.run_lineage_fields.join(",")
    ));
    lines.push(format!(
        "archive_formats: {}",
        format_counts(&summary.archive_format_counts)
    ));
    lines.push(format!(
        "crawler_source_maturity: {}",
        format_counts(&summary.crawler_source_maturity_counts)
    ));
    lines.push(format!(
        "crawler_expected_shapes: {}",
        format_counts(&summary.crawler_expected_shape_counts)
    ));

    for profile in &summary.profiles {
        lines.push(format!(
            "  profile_id={} connectors={} profile_source_import_lineage_connectors={} crawler_lineage_contract_connectors={} source_classes={} manifest_kinds={} manifest_schema_versions={} runtime_executable_mappings={} non_runtime_mappings={} source_manifest_files={} archive_files={} crawler_targets={} local_reference_only={} dynamic_loading_enabled={} live_fetch_default={} crawler_allowlist_required={} source_manifests={} event_csv_examples={} archive_sources={} optional_crawler_manifests={} manifest={}",
            profile.profile_id,
            profile.connector_references,
            profile.profile_source_import_lineage_connectors,
            profile.crawler_lineage_contract_connectors,
            format_counts(&profile.source_class_counts),
            format_counts(&profile.manifest_kind_counts),
            format_counts(&profile.manifest_schema_version_counts),
            profile.runtime_executable_mappings,
            profile.non_runtime_mappings,
            profile.source_manifest_file_count,
            profile.archive_file_count,
            profile.crawler_target_count,
            profile.local_reference_only_connectors,
            profile.dynamic_loading_enabled_connectors,
            profile.live_fetch_default_connectors,
            profile.crawler_allowlist_required_connectors,
            profile.source_manifest_references,
            profile.event_csv_example_references,
            profile.archive_source_references,
            profile.optional_crawler_manifest_references,
            profile.path.display()
        ));
        for connector in &profile.connectors {
            lines.push(format!(
                "    connector type={} source_class={} manifest_kind={} manifest_schema_version={} source_id={} field_mapping={} field_mapping_runtime_executable={} lint={} source_manifest_files={} archive_files={} archive_format={} archive_checksum_sha256={} crawler_targets={} crawler_source_maturity={} crawler_expected_shape={} local_reference_only={} dynamic_loading_enabled={} live_fetch_default={} allowlist_required={} manifest={}",
                connector.connector_type,
                connector.source_class,
                connector.manifest_kind,
                format_option_u32(connector.manifest_schema_version),
                connector.source_id.as_deref().unwrap_or("none"),
                connector.field_mapping.as_deref().unwrap_or("none"),
                connector
                    .field_mapping_runtime_executable
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                connector.manifest_lint,
                connector
                    .source_manifest_file_count
                    .map(|count| count.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                connector
                    .archive_file_count
                    .map(|count| count.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                connector.archive_format.as_deref().unwrap_or("none"),
                connector
                    .archive_checksum_sha256
                    .as_deref()
                    .unwrap_or("none"),
                connector
                    .crawler_target_count
                    .map(|count| count.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                connector
                    .crawler_source_maturity
                    .as_deref()
                    .unwrap_or("none"),
                connector.crawler_expected_shape.as_deref().unwrap_or("none"),
                connector.local_reference_only,
                connector.dynamic_loading_enabled,
                connector.live_fetch_default,
                connector.allowlist_required,
                connector.manifest_path.display()
            ));
        }
    }

    lines.join("\n")
}

pub fn format_ranking_config_doctor_summary(summary: &RankingConfigDoctorSummary) -> String {
    let mut lines = vec![format!(
        "doctor ranking-config completed: active_profile_id={}, fixture_set_id={}, ranking_files={}, ranking_kinds={}, profile_packs={}, referenced_ranking_config_dirs={}, reason_catalog_references={}, reason_catalog_locales={}, reasons={}, fixture_references={}, connector_references={}, evaluation_references={}, source_manifest_references={}, event_csv_example_references={}, archive_source_references={}, optional_crawler_manifest_references={}, profile_version={}",
        summary.active_profile_id.as_deref().unwrap_or("not-selected"),
        summary.fixture_set_id.as_deref().unwrap_or("none"),
        summary.ranking_files,
        format_counts(&summary.ranking_kind_counts),
        summary.profile_packs,
        summary.referenced_ranking_config_dirs,
        summary.reason_catalog_references,
        summary.reason_catalog_locales,
        summary.reason_count,
        summary.fixture_references,
        summary.connector_references,
        summary.evaluation_references,
        summary.source_manifest_references,
        summary.event_csv_example_references,
        summary.archive_source_references,
        summary.optional_crawler_manifest_references,
        summary.profile_version
    )];

    lines.push(format!(
        "ranking_config_dir={}",
        summary.ranking_config_dir.display()
    ));
    lines.push("ranking files:".to_string());
    lines.extend(summary.files.iter().map(|file| {
        format!(
            "  path={} schema_version={} kind={}",
            file.path.display(),
            file.schema_version,
            file.kind
        )
    }));
    lines.push("profile packs:".to_string());
    lines.extend(summary.profiles.iter().map(|profile| {
        format!(
            "  profile_id={} compatibility_level={} content_kind_registry={} content_kinds={} runtime_executable_content_kinds={} registry_only_content_kinds={} placements={} reason_catalog_locales={} reasons={} fixtures={} connectors={} evaluation_refs={} source_manifests={} event_csv_examples={} archive_sources={} optional_crawler_manifests={} manifest={} ranking_config_dir={} fallback_config={} reason_catalog={}",
            profile.profile_id,
            profile.compatibility_level,
            profile.content_kind_registry.join(","),
            profile.supported_content_kinds.join(","),
            format_order(&profile.runtime_executable_content_kinds),
            format_order(&profile.registry_only_content_kinds),
            profile.placements.join(","),
            profile.reason_catalog_locale_count,
            profile.reason_count,
            profile.fixture_references,
            profile.connector_references,
            profile.evaluation_references,
            profile.source_manifest_references,
            profile.event_csv_example_references,
            profile.archive_source_references,
            profile.optional_crawler_manifest_references,
            profile.path.display(),
            profile.ranking_config_dir.display(),
            profile.fallback_config_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "none".to_string()),
            profile.reason_catalog_path.display()
        )
    }));

    lines.join("\n")
}

pub fn format_context_coverage_doctor_summary(summary: &ContextCoverageDoctorSummary) -> String {
    let mut lines = vec![format!(
        "doctor context-coverage completed: scenarios={}, with_context={}, without_context={}, required_context_sources={}, context_shape_mismatches={}, candidate_count_scenarios={}, candidate_count_expectations={}",
        summary.scenarios,
        summary.scenarios_with_context,
        summary.scenarios_without_context,
        format_required_context_sources(summary),
        summary.context_shape_mismatches.len(),
        summary.scenarios_with_candidate_counts,
        summary.candidate_count_expectations
    )];

    lines.push(format!(
        "context_sources: {}",
        format_counts(&summary.context_source_counts)
    ));
    lines.push(format!(
        "context_tags: {}",
        format_counts(&summary.tag_counts)
    ));
    lines.push(format!(
        "fallback_stages: {}",
        format_counts(&summary.fallback_stage_counts)
    ));
    lines.push(format!(
        "candidate_count_stages: {}",
        format_counts(&summary.candidate_count_stage_counts)
    ));

    if !summary.missing_required_context_sources.is_empty() {
        lines.push(format!(
            "missing_required_context_sources: {}",
            summary.missing_required_context_sources.join(",")
        ));
    }
    if !summary.context_shape_mismatches.is_empty() {
        lines.push("context_shape_mismatches:".to_string());
        for mismatch in &summary.context_shape_mismatches {
            lines.push(format!(
                "  scenario_id={} context_source={} expected={} actual={} path={}",
                mismatch.id,
                mismatch.context_source,
                mismatch.expected_shape,
                format_shape(&mismatch.actual_shape),
                mismatch.path.display()
            ));
        }
    }

    for case in &summary.cases {
        lines.push(format!(
            "  scenario_id={} context_source={} tags={} fallback={} candidate_counts={} context_shape={} path={}",
            case.id,
            case.context_source.as_deref().unwrap_or("-"),
            format_order(&case.tags),
            case.fallback_stage,
            format_order(&case.candidate_count_stages),
            format_context_shape(case.has_area_context, case.has_line_context, case.has_station_context),
            case.path.display()
        ));
    }

    lines.join("\n")
}

pub fn format_retrieval_parity_doctor_summary(summary: &RetrievalParityDoctorSummary) -> String {
    let mut lines = vec![format!(
        "doctor retrieval-parity completed: cases={}, passed={}, failed={}, requires_database={}, requires_opensearch={}, public_mvp_gate={}",
        summary.case_count,
        summary.passed,
        summary.failed,
        summary.requires_database,
        summary.requires_opensearch,
        summary.public_mvp_gate
    )];
    lines.push(format!(
        "ordering_contract: {}",
        summary.ordering_contract.join(",")
    ));
    lines.push(format!(
        "opensearch_sort_contract: {}",
        summary
            .opensearch_sort_contract
            .iter()
            .map(|field| format!("{}={}", field.field, field.order))
            .collect::<Vec<_>>()
            .join(",")
    ));

    for case in &summary.cases {
        lines.push(format!(
            "  case_id={} status={} target_station_id={} limit={} expected={} actual={} input={} description={}",
            case.id,
            if case.passed { "passed" } else { "failed" },
            case.target_station_id,
            case.limit,
            format_order(&case.expected_order),
            format_order(&case.actual_order),
            format_order(&case.input_order),
            case.description
        ));
    }

    lines.join("\n")
}

pub fn format_storage_compatibility_doctor_summary(
    summary: &StorageCompatibilityDoctorSummary,
) -> String {
    let mut lines = vec![format!(
        "doctor storage-compatibility completed: registry_version={}, components={}, levels={}, sql_only_required={}, optional_runtime={}, public_mvp_gate={}, final_ranking_owner={}",
        summary.registry_version,
        summary.component_count,
        format_counts(&summary.compatibility_level_counts),
        format_order(&summary.sql_only_required_components),
        format_order(&summary.optional_runtime_components),
        format_order(&summary.public_mvp_gate_components),
        summary.final_ranking_owner
    )];
    lines.push(format!(
        "profile_compatibility_source: {}",
        summary.profile_compatibility_source
    ));

    for entry in &summary.entries {
        lines.push(format!(
            "  component={} display_name={} compatibility_level={} runtime_status={} data_role={} public_mvp_gate={} write_database_status={} evidence={} note={}",
            entry.component,
            entry.display_name,
            entry.compatibility_level,
            entry.runtime_status,
            entry.data_role,
            entry.public_mvp_gate,
            entry.write_database_status,
            entry.contract_evidence,
            entry.operator_note
        ));
    }

    lines.join("\n")
}

fn format_required_context_sources(summary: &ContextCoverageDoctorSummary) -> String {
    if summary.required_context_sources.is_empty() {
        return "-".to_string();
    }
    summary
        .required_context_sources
        .iter()
        .map(|source| {
            if source.covered {
                format!("{}={}", source.context_source, source.scenarios)
            } else {
                format!("{}=missing", source.context_source)
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn format_counts(counts: &std::collections::BTreeMap<String, usize>) -> String {
    if counts.is_empty() {
        return "-".to_string();
    }
    counts
        .iter()
        .map(|(key, count)| format!("{key}={count}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_option_u32(value: Option<u32>) -> String {
    value
        .map(|version| version.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn push_connector_schema_contract_lines(
    lines: &mut Vec<String>,
    contracts: &[ConnectorSchemaContractSummary],
) {
    lines.push("connector_schema_contracts:".to_string());
    if contracts.is_empty() {
        lines.push("  - none".to_string());
        return;
    }

    lines.extend(contracts.iter().map(|contract| {
        format!(
            "  connector_type={} source_class={} manifest_kind={} manifest_schema_version={} source_id_scope={} field_mapping_scope={} runtime_execution={} lint={} safety=local_reference_only:{},dynamic_loading_enabled:{},live_fetch_default:{},allowlist_required:{}",
            contract.connector_type,
            contract.source_class,
            contract.manifest_kind,
            format_option_u32(contract.manifest_schema_version),
            contract.source_id_scope,
            contract.field_mapping_scope,
            contract.runtime_execution,
            contract.manifest_lint,
            contract.local_reference_only,
            contract.dynamic_loading_enabled,
            contract.live_fetch_default,
            contract.allowlist_required
        )
    }));
}

fn format_context_shape(has_area: bool, has_line: bool, has_station: bool) -> String {
    let mut parts = Vec::new();
    if has_area {
        parts.push("area".to_string());
    }
    if has_line {
        parts.push("line".to_string());
    }
    if has_station {
        parts.push("station".to_string());
    }
    format_shape(&parts)
}

fn format_shape(shape: &[String]) -> String {
    if shape.is_empty() {
        "none".to_string()
    } else {
        shape.join(",")
    }
}

fn format_order(order: &[String]) -> String {
    if order.is_empty() {
        "-".to_string()
    } else {
        order.join(",")
    }
}

#[cfg(feature = "storage-backends")]
fn format_trace_payload_summary(summary: &ExplainTracePayloadSummary) -> String {
    format!(
        "trace_payload: response_source={} context={} confidence={} privacy={} context_evidence={} retrieval={}/{} candidate_count={} duration_ms={} candidate_plan={} suppressed_item_reasons={}",
        optional_str(summary.response_source.as_deref()),
        optional_str(summary.context_source.as_deref()),
        optional_f64(summary.context_confidence),
        optional_str(summary.privacy_level.as_deref()),
        summary
            .context_evidence_summary
            .as_ref()
            .map(|evidence| format!(
                "{}/{} count={} search_execute={}",
                evidence.source,
                evidence.primary_kind,
                evidence.evidence_count,
                evidence.has_search_execute
            ))
            .unwrap_or_else(|| "not_recorded".to_string()),
        optional_str(summary.candidate_retrieval_mode.as_deref()),
        optional_str(summary.candidate_retrieval_backend.as_deref()),
        optional_usize(summary.candidate_count),
        optional_u64(summary.duration_ms),
        summary
            .candidate_plan_trace
            .as_ref()
            .map(format_candidate_plan)
            .unwrap_or_else(|| "not_recorded".to_string()),
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

#[cfg(feature = "storage-backends")]
fn format_candidate_plan(trace: &ExplainTraceCandidatePlanSummary) -> String {
    let stage_reasons = trace
        .stages
        .iter()
        .map(|stage| format!("{}:{}:{}", stage.stage, stage.status, stage.reason_code))
        .collect::<Vec<_>>()
        .join(",");
    let graph_diagnostics = trace
        .graph_diagnostics
        .as_ref()
        .map(format_candidate_plan_graph_diagnostics)
        .unwrap_or_else(|| "not_recorded".to_string());

    format!(
        "{}/{} stages={} [{}] graph={}",
        trace.selected_stage,
        trace.stop_reason,
        trace.stages.len(),
        stage_reasons,
        graph_diagnostics
    )
}

#[cfg(feature = "storage-backends")]
fn format_candidate_plan_graph_diagnostics(
    diagnostics: &ExplainTraceCandidatePlanGraphDiagnosticsSummary,
) -> String {
    format!(
        "{}/{} geo={}({}) line={}({}) warnings={}",
        optional_str(diagnostics.mode.as_deref()),
        optional_str(diagnostics.candidate_expansion_behavior.as_deref()),
        optional_str(diagnostics.geo_graph_status.as_deref()),
        optional_usize(diagnostics.geo_graph_edge_count),
        optional_str(diagnostics.line_graph_status.as_deref()),
        optional_usize(diagnostics.line_graph_edge_count),
        diagnostics.warnings.len()
    )
}

#[cfg(feature = "storage-backends")]
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

#[cfg(feature = "storage-backends")]
fn optional_str(value: Option<&str>) -> &str {
    value.unwrap_or("-")
}

#[cfg(feature = "storage-backends")]
fn optional_bool(value: Option<bool>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(feature = "storage-backends")]
fn optional_usize(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(feature = "storage-backends")]
fn optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(feature = "storage-backends")]
fn optional_f64(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.3}"))
        .unwrap_or_else(|| "-".to_string())
}
