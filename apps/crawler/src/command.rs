use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    time::Duration,
};

use anyhow::{ensure, Context, Result};
use config::AppSettings;
use crawler_core::{
    check_expected_shape, dedupe_events, finalize_parsed_events, load_manifest, DedupeReportEntry,
    ParserRegistry, SourceMaturity,
};
use generic_http::{
    ensure_allowed_url, evaluate_robots, fetch_robots_txt, fetch_to_raw, HttpFetchRequest,
};
use serde_json::json;
use storage_postgres::{
    begin_crawl_run, claim_fetched_crawl_run, claim_latest_fetched_crawl_run, finish_crawl_run,
    import_crawled_events, latest_crawl_fetch_checksum, load_active_event_ids_for_source,
    load_crawl_fetch_logs, load_crawl_parse_errors, load_crawl_run_health,
    load_existing_school_ids, load_latest_fetched_crawl_run, mark_crawl_run_fetched,
    record_crawl_dedupe_report, record_crawl_fetch_log, record_crawl_parse_report,
    CrawlDedupeReportEntry, CrawlFetchLogEntry, CrawlParseReportEntry, CrawlRunHealthSnapshot,
};

use crate::manifest::{
    build_manifest_audit, canonical_manifest_path, check_fixture_shape, list_manifest_paths,
    resolve_and_validate_targets, resolve_manifest_metadata, to_event_csv_record,
};
use crate::report::{
    can_deactivate_stale_rows, is_green_signal, is_red_signal, merge_counts,
    normalize_reason_for_total, summarize_fetch_status, summarize_parse_error, CrawlCommandSummary,
    CrawlDoctorSummary, CrawlDryRunSummary, DiagnosticIssue, DoctorTargetSummary,
    LogicalDryRunSummary, LogicalNameRedFlag, LogicalNameRunSignal, ParserHealthSummary,
    RunReasonTrend,
};
use crate::shared::{
    build_date_drift_warning, build_http_client, build_http_fetch_client,
    classify_fetch_error_status, collect_url_probe_issues, discard_staged_fetch,
    is_zero_event_parse_message, looks_like_html, probe_target_body, probe_url,
};

pub async fn run_fetch_command(
    settings: &AppSettings,
    manifest_path: impl AsRef<Path>,
) -> Result<CrawlCommandSummary> {
    let manifest_path = canonical_manifest_path(manifest_path)?;
    let manifest = load_manifest(&manifest_path)?;
    let targets = resolve_and_validate_targets(&manifest)?;
    let registry = ParserRegistry::default();
    let parser = registry
        .get(&manifest.parser_key)
        .with_context(|| format!("unknown parser_key {}", manifest.parser_key))?;
    let _metadata = resolve_manifest_metadata(&manifest, Some(parser))?;
    let parser_version = manifest.effective_parser_version(parser.default_version());
    let manifest_audit = build_manifest_audit(&manifest_path, &manifest, &parser_version)?;
    let crawl_run_id =
        begin_crawl_run(&settings.database_url, &manifest_audit, parser.key()).await?;

    if !manifest.allowlist.live_fetch_enabled {
        let reason = manifest
            .allowlist
            .live_fetch_block_reason
            .clone()
            .unwrap_or_else(|| "live fetch disabled by manifest policy".to_string());
        for target in &targets {
            record_crawl_fetch_log(
                &settings.database_url,
                &CrawlFetchLogEntry {
                    crawl_run_id,
                    logical_name: target.logical_name.clone(),
                    target_url: target.url.clone(),
                    final_url: None,
                    http_status: None,
                    checksum_sha256: None,
                    size_bytes: None,
                    staged_path: None,
                    fetch_status: "blocked_policy".to_string(),
                    content_changed: None,
                    details: json!({
                        "reason": reason,
                        "robots_txt_url": manifest.allowlist.robots_txt_url,
                        "terms_url": manifest.allowlist.terms_url,
                        "terms_note": manifest.allowlist.terms_note,
                        "user_agent": manifest.allowlist.user_agent
                    }),
                },
            )
            .await?;
        }

        finish_crawl_run(&settings.database_url, crawl_run_id, "failed", 0, 0, 0).await?;
        anyhow::bail!(
            "live fetch disabled by manifest policy for {}: {}",
            manifest.source_id,
            reason
        );
    }

    let client = build_http_fetch_client()?;

    let mut fetched_targets = 0_i64;
    let mut report_count = 0_usize;

    let result: Result<()> = async {
        let robots_request = HttpFetchRequest {
            source_id: &manifest.source_id,
            logical_name: "robots_txt",
            url: &manifest.allowlist.robots_txt_url,
            user_agent: &manifest.allowlist.user_agent,
            allowed_domains: &manifest.allowlist.allowed_domains,
        };
        let robots_txt = fetch_robots_txt(&client, &robots_request).await?;

        for (index, target) in targets.iter().enumerate() {
            if index > 0 && manifest.allowlist.min_fetch_interval_ms > 0 {
                tokio::time::sleep(Duration::from_millis(
                    manifest.allowlist.min_fetch_interval_ms,
                ))
                .await;
            }

            let parsed_url = ensure_allowed_url(&target.url, &manifest.allowlist.allowed_domains)?;
            let robots = evaluate_robots(
                &robots_txt,
                &manifest.allowlist.user_agent,
                parsed_url.path(),
            );
            if !robots.allowed {
                report_count += 1;
                record_crawl_fetch_log(
                    &settings.database_url,
                    &CrawlFetchLogEntry {
                        crawl_run_id,
                        logical_name: target.logical_name.clone(),
                        target_url: target.url.clone(),
                        final_url: None,
                        http_status: None,
                        checksum_sha256: None,
                        size_bytes: None,
                        staged_path: None,
                        fetch_status: "blocked_robots".to_string(),
                        content_changed: None,
                        details: json!({
                            "matched_rule": robots.matched_rule,
                            "robots_txt_url": manifest.allowlist.robots_txt_url,
                            "terms_url": manifest.allowlist.terms_url,
                            "terms_note": manifest.allowlist.terms_note,
                            "user_agent": manifest.allowlist.user_agent
                        }),
                    },
                )
                .await?;
                continue;
            }

            match fetch_to_raw(
                &client,
                &HttpFetchRequest {
                    source_id: &manifest.source_id,
                    logical_name: &target.logical_name,
                    url: &target.url,
                    user_agent: &manifest.allowlist.user_agent,
                    allowed_domains: &manifest.allowlist.allowed_domains,
                },
                &settings.raw_storage_dir,
            )
                .await
            {
                Ok(fetch) => {
                    let final_url = match ensure_allowed_url(
                        &fetch.final_url,
                        &manifest.allowlist.allowed_domains,
                    ) {
                        Ok(final_url) => final_url,
                        Err(error) => {
                            report_count += 1;
                            if fetch.staged_was_created {
                                discard_staged_fetch(&fetch.staged_path);
                            }
                            record_crawl_fetch_log(
                                &settings.database_url,
                                &CrawlFetchLogEntry {
                                    crawl_run_id,
                                    logical_name: fetch.logical_name.clone(),
                                    target_url: fetch.target_url.clone(),
                                    final_url: Some(fetch.final_url.clone()),
                                    http_status: Some(fetch.status_code as i32),
                                    checksum_sha256: Some(fetch.checksum_sha256.clone()),
                                    size_bytes: Some(fetch.size_bytes as i64),
                                    staged_path: None,
                                    fetch_status: "blocked_policy".to_string(),
                                    content_changed: None,
                                    details: json!({
                                        "reason": format!("resolved final_url violated crawler allowlist: {error}"),
                                        "content_type": fetch.content_type.clone(),
                                        "robots_txt_url": manifest.allowlist.robots_txt_url,
                                        "terms_url": manifest.allowlist.terms_url,
                                        "terms_note": manifest.allowlist.terms_note,
                                        "user_agent": manifest.allowlist.user_agent,
                                        "min_fetch_interval_ms": manifest.allowlist.min_fetch_interval_ms
                                    }),
                                },
                            )
                            .await?;
                            continue;
                        }
                    };
                    let final_robots = evaluate_robots(
                        &robots_txt,
                        &manifest.allowlist.user_agent,
                        final_url.path(),
                    );
                    if !final_robots.allowed {
                        report_count += 1;
                        if fetch.staged_was_created {
                            discard_staged_fetch(&fetch.staged_path);
                        }
                        record_crawl_fetch_log(
                            &settings.database_url,
                            &CrawlFetchLogEntry {
                                crawl_run_id,
                                logical_name: fetch.logical_name.clone(),
                                target_url: fetch.target_url.clone(),
                                final_url: Some(fetch.final_url.clone()),
                                http_status: Some(fetch.status_code as i32),
                                checksum_sha256: Some(fetch.checksum_sha256.clone()),
                                size_bytes: Some(fetch.size_bytes as i64),
                                staged_path: None,
                                fetch_status: "blocked_robots".to_string(),
                                content_changed: None,
                                details: json!({
                                    "matched_rule": final_robots.matched_rule,
                                    "content_type": fetch.content_type.clone(),
                                    "robots_txt_url": manifest.allowlist.robots_txt_url,
                                    "terms_url": manifest.allowlist.terms_url,
                                    "terms_note": manifest.allowlist.terms_note,
                                    "user_agent": manifest.allowlist.user_agent,
                                    "min_fetch_interval_ms": manifest.allowlist.min_fetch_interval_ms
                                }),
                            },
                        )
                        .await?;
                        continue;
                    }

                    let previous_checksum = latest_crawl_fetch_checksum(
                        &settings.database_url,
                        &manifest_audit.manifest_path,
                        &target.logical_name,
                        &target.url,
                    )
                    .await?;
                    let content_changed =
                        previous_checksum.as_deref() != Some(fetch.checksum_sha256.as_str());
                    let fetch_status = if content_changed {
                        "fetched"
                    } else {
                        "not_modified"
                    };

                    record_crawl_fetch_log(
                        &settings.database_url,
                        &CrawlFetchLogEntry {
                            crawl_run_id,
                            logical_name: fetch.logical_name.clone(),
                            target_url: fetch.target_url.clone(),
                            final_url: Some(fetch.final_url.clone()),
                            http_status: Some(fetch.status_code as i32),
                            checksum_sha256: Some(fetch.checksum_sha256.clone()),
                            size_bytes: Some(fetch.size_bytes as i64),
                            staged_path: Some(fetch.staged_path.display().to_string()),
                            fetch_status: fetch_status.to_string(),
                            content_changed: Some(content_changed),
                            details: json!({
                                "content_type": fetch.content_type,
                                "matched_rule": final_robots.matched_rule,
                                "robots_txt_url": manifest.allowlist.robots_txt_url,
                                "terms_url": manifest.allowlist.terms_url,
                                "terms_note": manifest.allowlist.terms_note,
                                "user_agent": manifest.allowlist.user_agent,
                                "min_fetch_interval_ms": manifest.allowlist.min_fetch_interval_ms
                            }),
                        },
                    )
                    .await?;
                    fetched_targets += 1;
                }
                Err(error) => {
                    report_count += 1;
                    let error_message = error.to_string();
                    let fetch_status = classify_fetch_error_status(&error_message);
                    record_crawl_fetch_log(
                        &settings.database_url,
                        &CrawlFetchLogEntry {
                            crawl_run_id,
                            logical_name: target.logical_name.clone(),
                            target_url: target.url.clone(),
                            final_url: None,
                            http_status: None,
                            checksum_sha256: None,
                            size_bytes: None,
                            staged_path: None,
                            fetch_status: fetch_status.to_string(),
                            content_changed: None,
                            details: json!({
                                "error": error_message,
                                "robots_txt_url": manifest.allowlist.robots_txt_url,
                                "terms_url": manifest.allowlist.terms_url,
                                "terms_note": manifest.allowlist.terms_note,
                                "user_agent": manifest.allowlist.user_agent
                            }),
                        },
                    )
                    .await?;
                }
            }
        }

        ensure!(
            fetched_targets > 0,
            "no crawl targets were fetched successfully"
        );
        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            mark_crawl_run_fetched(&settings.database_url, crawl_run_id, fetched_targets).await?;
            Ok(CrawlCommandSummary {
                label: format!("crawl-fetch:{}", manifest.source_id),
                crawl_run_id,
                fetched_targets,
                parsed_rows: 0,
                imported_rows: 0,
                report_count,
            })
        }
        Err(error) => {
            let _ = finish_crawl_run(
                &settings.database_url,
                crawl_run_id,
                "failed",
                fetched_targets,
                0,
                0,
            )
            .await;
            Err(error)
        }
    }
}

pub async fn run_parse_command(
    settings: &AppSettings,
    manifest_path: impl AsRef<Path>,
) -> Result<CrawlCommandSummary> {
    let manifest_path = canonical_manifest_path(manifest_path)?;
    let pending_run = claim_latest_fetched_crawl_run(
        &settings.database_url,
        &manifest_path.display().to_string(),
    )
    .await?
    .with_context(|| {
        format!(
            "no fetched crawl run is ready for manifest {}",
            manifest_path.display()
        )
    })?;
    run_parse_command_with_claimed_run(settings, &manifest_path, pending_run.crawl_run_id).await
}

async fn run_parse_command_with_claimed_run(
    settings: &AppSettings,
    manifest_path: &Path,
    crawl_run_id: i64,
) -> Result<CrawlCommandSummary> {
    let manifest = load_manifest(manifest_path)?;
    let targets = resolve_and_validate_targets(&manifest)?;
    let targets_by_name = targets
        .into_iter()
        .map(|target| (target.logical_name.clone(), target))
        .collect::<std::collections::BTreeMap<_, _>>();
    let registry = ParserRegistry::default();
    let parser = registry
        .get(&manifest.parser_key)
        .with_context(|| format!("unknown parser_key {}", manifest.parser_key))?;
    let _metadata = resolve_manifest_metadata(&manifest, Some(parser))?;
    let parser_version = manifest.effective_parser_version(parser.default_version());

    let mut report_count = 0_usize;
    let mut parsed_rows = 0_i64;
    let mut imported_rows = 0_i64;
    let mut fetched_targets = 0_i64;
    let mut parse_failed_targets = 0_usize;
    let mut zero_row_targets = 0_usize;

    let result: Result<()> = async {
        let fetch_logs = load_crawl_fetch_logs(&settings.database_url, crawl_run_id).await?;
        fetched_targets = fetch_logs
            .iter()
            .filter(|entry| matches!(entry.fetch_status.as_str(), "fetched" | "not_modified"))
            .count() as i64;

        record_parse_report(
            &settings.database_url,
            &mut report_count,
            CrawlParseReportEntry {
                crawl_run_id,
                logical_name: None,
                level: "info".to_string(),
                code: "parser_version".to_string(),
                message: "Recorded parser version for this crawl run.".to_string(),
                parsed_rows: None,
                details: json!({
                    "parser_key": manifest.parser_key,
                    "parser_version": parser_version
                }),
            },
        )
        .await?;

        let mut collected = Vec::new();
        for fetch in fetch_logs
            .iter()
            .filter(|entry| matches!(entry.fetch_status.as_str(), "fetched" | "not_modified"))
        {
            let target = targets_by_name
                .get(&fetch.logical_name)
                .with_context(|| format!("unknown crawl target {}", fetch.logical_name))?;
            let staged_path = fetch
                .staged_path
                .as_ref()
                .with_context(|| format!("missing staged_path for {}", fetch.logical_name))?;
            let html = fs::read_to_string(staged_path)
                .with_context(|| format!("failed to read staged content {}", staged_path))?;

            match parser.parse(&crawler_core::ParseInput {
                source_id: &manifest.source_id,
                logical_name: &fetch.logical_name,
                target_url: &fetch.target_url,
                html: &html,
                target,
            }) {
                Ok(output) => {
                    for entry in output.report_entries {
                        record_parse_report(
                            &settings.database_url,
                            &mut report_count,
                            CrawlParseReportEntry {
                                crawl_run_id,
                                logical_name: entry.logical_name,
                                level: entry.level,
                                code: entry.code,
                                message: entry.message,
                                parsed_rows: entry.parsed_rows,
                                details: entry.details,
                            },
                        )
                        .await?;
                    }

                    let records = finalize_parsed_events(
                        &manifest.source_id,
                        &fetch.logical_name,
                        &fetch.target_url,
                        target,
                        output.events,
                    )?;
                    parsed_rows += records.len() as i64;
                    collected.extend(records);
                }
                Err(error) => {
                    let message = error.to_string();
                    if is_zero_event_parse_message(&message) {
                        zero_row_targets += 1;
                        record_parse_report(
                            &settings.database_url,
                            &mut report_count,
                            CrawlParseReportEntry {
                                crawl_run_id,
                                logical_name: Some(fetch.logical_name.clone()),
                                level: "warn".to_string(),
                                code: "parsed_zero_rows".to_string(),
                                message,
                                parsed_rows: Some(0),
                                details: json!({
                                    "target_url": fetch.target_url,
                                    "staged_path": staged_path
                                }),
                            },
                        )
                        .await?;
                    } else {
                        parse_failed_targets += 1;
                        record_parse_report(
                            &settings.database_url,
                            &mut report_count,
                            CrawlParseReportEntry {
                                crawl_run_id,
                                logical_name: Some(fetch.logical_name.clone()),
                                level: "error".to_string(),
                                code: "parse_failed".to_string(),
                                message,
                                parsed_rows: None,
                                details: json!({
                                    "target_url": fetch.target_url,
                                    "staged_path": staged_path
                                }),
                            },
                        )
                        .await?;
                    }
                }
            }
        }

        if collected.is_empty() && parse_failed_targets > 0 {
            anyhow::bail!("no crawler events were parsed successfully");
        }

        let (deduped, dedupe_reports) = dedupe_events(collected);
        for report in dedupe_reports {
            record_dedupe_report(&settings.database_url, crawl_run_id, report).await?;
            report_count += 1;
        }

        let summary = import_crawled_events(
            &settings.database_url,
            &manifest.source_id,
            &deduped.iter().map(to_event_csv_record).collect::<Vec<_>>(),
            can_deactivate_stale_rows(&fetch_logs, parse_failed_targets, zero_row_targets),
        )
        .await?;
        imported_rows = summary.core_rows;

        for entry in summary.report_entries {
            record_parse_report(
                &settings.database_url,
                &mut report_count,
                CrawlParseReportEntry {
                    crawl_run_id,
                    logical_name: None,
                    level: entry.level,
                    code: entry.code,
                    message: entry.message,
                    parsed_rows: entry.row_count,
                    details: entry.details,
                },
            )
            .await?;
        }

        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            finish_crawl_run(
                &settings.database_url,
                crawl_run_id,
                "succeeded",
                fetched_targets,
                parsed_rows,
                imported_rows,
            )
            .await?;
            Ok(CrawlCommandSummary {
                label: format!("crawl-parse:{}", manifest.source_id),
                crawl_run_id,
                fetched_targets,
                parsed_rows,
                imported_rows,
                report_count,
            })
        }
        Err(error) => {
            let _ = finish_crawl_run(
                &settings.database_url,
                crawl_run_id,
                "failed",
                fetched_targets,
                parsed_rows,
                imported_rows,
            )
            .await;
            Err(error)
        }
    }
}

pub async fn run_crawl_command(
    settings: &AppSettings,
    manifest_path: impl AsRef<Path>,
) -> Result<CrawlCommandSummary> {
    let manifest_path = canonical_manifest_path(manifest_path)?;
    let fetch_summary = run_fetch_command(settings, &manifest_path).await?;
    let pending_run = claim_fetched_crawl_run(&settings.database_url, fetch_summary.crawl_run_id)
        .await?
        .with_context(|| {
            format!(
                "fetched crawl run {} is no longer ready for parsing",
                fetch_summary.crawl_run_id
            )
        })?;
    let parse_summary =
        run_parse_command_with_claimed_run(settings, &manifest_path, pending_run.crawl_run_id)
            .await?;

    Ok(CrawlCommandSummary {
        label: parse_summary.label,
        crawl_run_id: parse_summary.crawl_run_id,
        fetched_targets: fetch_summary.fetched_targets,
        parsed_rows: parse_summary.parsed_rows,
        imported_rows: parse_summary.imported_rows,
        report_count: fetch_summary.report_count + parse_summary.report_count,
    })
}

pub async fn run_doctor_command(
    settings: &AppSettings,
    manifest_path: impl AsRef<Path>,
) -> Result<CrawlDoctorSummary> {
    let manifest_path = canonical_manifest_path(manifest_path)?;
    let manifest = load_manifest(&manifest_path)?;
    let registry = ParserRegistry::default();
    let parser = registry.get(&manifest.parser_key);
    let parser_registered = parser.is_some();
    let metadata = resolve_manifest_metadata(&manifest, parser)?;
    let targets = resolve_and_validate_targets(&manifest)?;
    let school_ids = targets
        .iter()
        .map(|target| target.school_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let client = build_http_client()?;
    let robots = probe_url(
        &client,
        &manifest.allowlist.robots_txt_url,
        &manifest.allowlist.user_agent,
        &manifest.allowlist.allowed_domains,
    )
    .await;
    let terms = probe_url(
        &client,
        &manifest.allowlist.terms_url,
        &manifest.allowlist.user_agent,
        &manifest.allowlist.allowed_domains,
    )
    .await;

    let robots_body = robots.body.as_deref().unwrap_or_default();
    let mut targets_summary = Vec::new();
    let mut issues = Vec::new();
    let known_school_ids = match load_existing_school_ids(&settings.database_url, &school_ids).await
    {
        Ok(known_school_ids) => Some(known_school_ids),
        Err(error) => {
            issues.push(DiagnosticIssue {
                level: "warn".to_string(),
                code: "school_lookup_failed".to_string(),
                message: format!("failed to verify target school_ids against database: {error}"),
            });
            None
        }
    };

    if !parser_registered {
        issues.push(DiagnosticIssue {
            level: "error".to_string(),
            code: "unknown_parser_key".to_string(),
            message: format!(
                "manifest parser_key {} is not registered",
                manifest.parser_key
            ),
        });
    }
    if !manifest.allowlist.live_fetch_enabled {
        issues.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "live_fetch_disabled".to_string(),
            message: manifest
                .allowlist
                .live_fetch_block_reason
                .clone()
                .unwrap_or_else(|| "live fetch disabled by manifest policy".to_string()),
        });
    }
    collect_url_probe_issues("robots", &robots, &mut issues);
    collect_url_probe_issues("terms", &terms, &mut issues);
    if robots
        .content_type
        .as_deref()
        .is_some_and(|value| !value.starts_with("text/plain"))
    {
        issues.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "robots_unexpected_content_type".to_string(),
            message: format!(
                "robots content-type is {}, expected text/plain",
                robots.content_type.as_deref().unwrap_or("-")
            ),
        });
    }
    if looks_like_html(robots_body) {
        issues.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "robots_html_body".to_string(),
            message: "robots URL returned HTML-like content instead of plain-text robots rules"
                .to_string(),
        });
    }

    for target in targets {
        let robots_decision = if robots.error.is_none() && !robots_body.is_empty() {
            let parsed_url = ensure_allowed_url(&target.url, &manifest.allowlist.allowed_domains)?;
            Some(evaluate_robots(
                robots_body,
                &manifest.allowlist.user_agent,
                parsed_url.path(),
            ))
        } else {
            None
        };
        let school_exists = known_school_ids
            .as_ref()
            .map(|known| known.contains(target.school_id.as_str()));
        if matches!(school_exists, Some(false)) {
            issues.push(DiagnosticIssue {
                level: "warn".to_string(),
                code: "missing_school_id".to_string(),
                message: format!(
                    "logical_name {} references missing school_id {}",
                    target.logical_name, target.school_id
                ),
            });
        }

        let (shape_status, shape_detail) = if let Some(expected_shape) = metadata.expected_shape {
            if !manifest.allowlist.live_fetch_enabled {
                if let Some(fixture_path) = &target.fixture_path {
                    match check_fixture_shape(&manifest_path, fixture_path, expected_shape) {
                        Ok((matched, detail)) if matched => {
                            (Some("matched".to_string()), Some(detail))
                        }
                        Ok((_, detail)) => {
                            issues.push(DiagnosticIssue {
                                level: "warn".to_string(),
                                code: "expected_shape_mismatch".to_string(),
                                message: format!(
                                    "logical_name {} fixture_path {} did not match expected_shape {}: {}",
                                    target.logical_name, fixture_path, expected_shape, detail
                                ),
                            });
                            (Some("mismatch".to_string()), Some(detail))
                        }
                        Err(error) => {
                            issues.push(DiagnosticIssue {
                                level: "warn".to_string(),
                                code: "fixture_shape_read_failed".to_string(),
                                message: format!(
                                    "logical_name {} failed fixture shape probe: {}",
                                    target.logical_name, error
                                ),
                            });
                            (
                                Some("fixture_read_failed".to_string()),
                                Some(error.to_string()),
                            )
                        }
                    }
                } else {
                    (
                        Some("skipped".to_string()),
                        Some("live fetch disabled by manifest policy".to_string()),
                    )
                }
            } else if robots_decision.is_none() {
                (
                    Some("skipped".to_string()),
                    Some("robots policy could not be evaluated for this target".to_string()),
                )
            } else if matches!(
                robots_decision.as_ref().map(|decision| decision.allowed),
                Some(false)
            ) {
                (
                    Some("skipped".to_string()),
                    Some("target is blocked by robots policy".to_string()),
                )
            } else {
                let target_probe = probe_target_body(
                    &client,
                    &target.url,
                    &manifest.allowlist.user_agent,
                    &manifest.allowlist.allowed_domains,
                    robots_body,
                )
                .await;
                if let Some(error) = target_probe.error {
                    issues.push(DiagnosticIssue {
                        level: "warn".to_string(),
                        code: "target_shape_fetch_failed".to_string(),
                        message: format!(
                            "logical_name {} failed shape probe: {}",
                            target.logical_name, error
                        ),
                    });
                    (Some("fetch_failed".to_string()), Some(error))
                } else {
                    let check = check_expected_shape(
                        expected_shape,
                        &target_probe.body,
                        target_probe.content_type.as_deref(),
                    );
                    if check.matched {
                        (Some("matched".to_string()), Some(check.summary))
                    } else {
                        issues.push(DiagnosticIssue {
                            level: "warn".to_string(),
                            code: "expected_shape_mismatch".to_string(),
                            message: format!(
                                "logical_name {} expected_shape {} did not match: {}",
                                target.logical_name, expected_shape, check.summary
                            ),
                        });
                        (Some("mismatch".to_string()), Some(check.summary))
                    }
                }
            }
        } else {
            (None, None)
        };

        targets_summary.push(DoctorTargetSummary {
            logical_name: target.logical_name,
            target_url: target.url,
            school_id: target.school_id,
            school_exists,
            robots_allowed: robots_decision.as_ref().map(|decision| decision.allowed),
            matched_rule: robots_decision.and_then(|decision| decision.matched_rule),
            expected_shape: metadata.expected_shape,
            shape_status,
            shape_detail,
        });
    }

    Ok(CrawlDoctorSummary {
        manifest_path: manifest_path.display().to_string(),
        source_id: manifest.source_id,
        source_name: manifest.source_name,
        source_maturity: metadata.source_maturity,
        parser_key: manifest.parser_key,
        parser_registered,
        expected_shape: metadata.expected_shape,
        live_fetch_enabled: manifest.allowlist.live_fetch_enabled,
        robots,
        terms,
        targets: targets_summary,
        issues,
    })
}

pub async fn run_dry_run_command(
    settings: &AppSettings,
    manifest_path: impl AsRef<Path>,
) -> Result<CrawlDryRunSummary> {
    let manifest_path = canonical_manifest_path(manifest_path)?;
    let manifest = load_manifest(&manifest_path)?;
    let targets = resolve_and_validate_targets(&manifest)?;
    let targets_by_name = targets
        .into_iter()
        .map(|target| (target.logical_name.clone(), target))
        .collect::<BTreeMap<_, _>>();
    let registry = ParserRegistry::default();
    let parser = registry
        .get(&manifest.parser_key)
        .with_context(|| format!("unknown parser_key {}", manifest.parser_key))?;
    let metadata = resolve_manifest_metadata(&manifest, Some(parser))?;
    let parser_version = manifest.effective_parser_version(parser.default_version());
    let pending_run =
        load_latest_fetched_crawl_run(&settings.database_url, &manifest_path.display().to_string())
            .await?
            .with_context(|| {
                format!(
                    "no fetched crawl run is ready for manifest {}",
                    manifest_path.display()
                )
            })?;
    let fetch_logs =
        load_crawl_fetch_logs(&settings.database_url, pending_run.crawl_run_id).await?;
    let mut collected = Vec::new();
    let mut parse_errors = Vec::new();
    let mut warnings = Vec::new();
    let mut logical_name_summaries = Vec::new();
    let mut date_drift_warnings = 0_usize;
    let mut zero_row_targets = 0_usize;

    for fetch in fetch_logs
        .iter()
        .filter(|entry| matches!(entry.fetch_status.as_str(), "fetched" | "not_modified"))
    {
        let target = targets_by_name
            .get(&fetch.logical_name)
            .with_context(|| format!("unknown crawl target {}", fetch.logical_name))?;
        let staged_path = fetch
            .staged_path
            .as_ref()
            .with_context(|| format!("missing staged_path for {}", fetch.logical_name))?;
        let html = fs::read_to_string(staged_path)
            .with_context(|| format!("failed to read staged content {}", staged_path))?;

        match parser.parse(&crawler_core::ParseInput {
            source_id: &manifest.source_id,
            logical_name: &fetch.logical_name,
            target_url: &fetch.target_url,
            html: &html,
            target,
        }) {
            Ok(output) => {
                let logical_drifts = output
                    .events
                    .iter()
                    .filter_map(|seed| {
                        build_date_drift_warning(&fetch.logical_name, &seed.title, &seed.details)
                    })
                    .collect::<Vec<_>>();
                date_drift_warnings += logical_drifts.len();
                let logical_drift_count = logical_drifts.len();
                warnings.extend(logical_drifts.into_iter().map(|message| DiagnosticIssue {
                    level: "warn".to_string(),
                    code: "date_drift".to_string(),
                    message,
                }));

                let records = finalize_parsed_events(
                    &manifest.source_id,
                    &fetch.logical_name,
                    &fetch.target_url,
                    target,
                    output.events,
                )?;
                logical_name_summaries.push(LogicalDryRunSummary {
                    logical_name: fetch.logical_name.clone(),
                    parsed_rows: records.len() as i64,
                    date_drift_warnings: logical_drift_count,
                    parse_error: None,
                });
                collected.extend(records);
            }
            Err(error) => {
                let message = error.to_string();
                if is_zero_event_parse_message(&message) {
                    zero_row_targets += 1;
                    warnings.push(DiagnosticIssue {
                        level: "warn".to_string(),
                        code: "no_events_found".to_string(),
                        message: format!("{}: {}", fetch.logical_name, message),
                    });
                    logical_name_summaries.push(LogicalDryRunSummary {
                        logical_name: fetch.logical_name.clone(),
                        parsed_rows: 0,
                        date_drift_warnings: 0,
                        parse_error: None,
                    });
                } else {
                    parse_errors.push(DiagnosticIssue {
                        level: "error".to_string(),
                        code: "parse_failed".to_string(),
                        message: format!("{}: {}", fetch.logical_name, message),
                    });
                    logical_name_summaries.push(LogicalDryRunSummary {
                        logical_name: fetch.logical_name.clone(),
                        parsed_rows: 0,
                        date_drift_warnings: 0,
                        parse_error: Some(message),
                    });
                }
            }
        }
    }

    let parsed_rows = collected.len() as i64;
    if parsed_rows == 0 {
        warnings.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "parsed_zero_rows".to_string(),
            message: "dry-run parsed 0 rows; import would be empty".to_string(),
        });
    }

    let (deduped, _dedupe_reports) = dedupe_events(collected);
    let deduped_rows = deduped.len() as i64;
    let school_ids = deduped
        .iter()
        .map(|record| record.school_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let known_school_ids = load_existing_school_ids(&settings.database_url, &school_ids)
        .await
        .with_context(|| "failed to load known school ids for dry-run")?;
    let imported_ids = deduped
        .iter()
        .filter(|record| known_school_ids.contains(record.school_id.as_str()))
        .map(|record| record.event_id.clone())
        .collect::<BTreeSet<_>>();
    let missing_school_rows = deduped
        .iter()
        .filter(|record| !known_school_ids.contains(record.school_id.as_str()))
        .count() as i64;
    if missing_school_rows > 0 {
        warnings.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "missing_school_id".to_string(),
            message: format!(
                "dry-run would skip {} rows because school_id is missing",
                missing_school_rows
            ),
        });
    }
    let active_ids =
        load_active_event_ids_for_source(&settings.database_url, "crawl", &manifest.source_id)
            .await
            .with_context(|| "failed to load active event ids for dry-run")?;
    let deactivated_rows = if !can_deactivate_stale_rows(
        &fetch_logs,
        parse_errors.len(),
        zero_row_targets,
    ) {
        warnings.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "partial_import_skips_stale_deactivation".to_string(),
            message:
                "dry-run would keep existing active rows because one or more crawl targets failed to fetch, failed to parse, or produced zero rows"
                    .to_string(),
        });
        0
    } else if missing_school_rows > 0 {
        warnings.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "missing_school_skips_stale_deactivation".to_string(),
            message:
                "dry-run would keep existing active rows because one or more rows reference a missing school_id"
                    .to_string(),
        });
        0
    } else {
        active_ids.difference(&imported_ids).count() as i64
    };

    Ok(CrawlDryRunSummary {
        manifest_path: manifest_path.display().to_string(),
        source_id: manifest.source_id,
        source_name: manifest.source_name,
        source_maturity: metadata.source_maturity,
        parser_key: manifest.parser_key,
        parser_version,
        expected_shape: metadata.expected_shape,
        crawl_run_id: pending_run.crawl_run_id,
        ready_targets: fetch_logs
            .iter()
            .filter(|entry| matches!(entry.fetch_status.as_str(), "fetched" | "not_modified"))
            .count(),
        parsed_rows,
        deduped_rows,
        imported_rows: imported_ids.len() as i64,
        deactivated_rows,
        missing_school_rows,
        date_drift_warnings,
        parse_errors,
        warnings,
        logical_name_summaries,
    })
}

pub async fn run_health_command(
    settings: &AppSettings,
    manifest_path: impl AsRef<Path>,
    limit: usize,
) -> Result<ParserHealthSummary> {
    let manifest_path = canonical_manifest_path(manifest_path)?;
    let manifest = load_manifest(&manifest_path)?;
    let registry = ParserRegistry::default();
    let parser = registry
        .get(&manifest.parser_key)
        .with_context(|| format!("unknown parser_key {}", manifest.parser_key))?;
    let metadata = resolve_manifest_metadata(&manifest, Some(parser))?;
    let parser_version = manifest.effective_parser_version(parser.default_version());
    let health = load_crawl_run_health(
        &settings.database_url,
        &manifest_path.display().to_string(),
        limit,
    )
    .await?;

    let mut fetch_status_totals = BTreeMap::new();
    let mut parse_level_totals = BTreeMap::new();
    let mut succeeded_runs = 0_usize;
    let mut failed_runs = 0_usize;
    let mut active_runs = 0_usize;
    let mut dedupe_report_total = 0_i64;
    let current_logical_names = manifest
        .targets
        .iter()
        .map(|target| target.logical_name.clone())
        .collect::<BTreeSet<_>>();

    for run in &health.runs {
        match run.status.as_str() {
            "succeeded" => succeeded_runs += 1,
            "failed" => failed_runs += 1,
            _ => active_runs += 1,
        }
        dedupe_report_total += run.dedupe_count;
        merge_counts(&mut fetch_status_totals, &run.fetch_status_counts);
        merge_counts(&mut parse_level_totals, &run.parse_level_counts);
    }

    let logical_name_red_flags =
        load_logical_name_red_flags(&settings.database_url, &health.runs, &current_logical_names)
            .await?;
    let recent_reason_trend =
        load_recent_reason_trend(&settings.database_url, &health.runs).await?;
    let healthy_logical_name_count = current_logical_names
        .len()
        .saturating_sub(logical_name_red_flags.len());
    let mut reason_totals = BTreeMap::new();
    for flag in &logical_name_red_flags {
        for reason in &flag.reasons {
            if let Some(normalized) = normalize_reason_for_total(reason) {
                *reason_totals.entry(normalized).or_insert(0) += 1;
            }
        }
    }

    Ok(ParserHealthSummary {
        manifest_path: manifest_path.display().to_string(),
        source_id: manifest.source_id,
        source_name: manifest.source_name,
        source_maturity: metadata.source_maturity,
        parser_key: parser.key().to_string(),
        parser_version,
        expected_shape: metadata.expected_shape,
        total_runs: health.total_runs,
        shown_runs: health.runs.len(),
        succeeded_runs,
        failed_runs,
        active_runs,
        fetch_status_totals,
        parse_level_totals,
        dedupe_report_total,
        recent_runs: health.runs,
        recent_reason_trend,
        logical_name_red_flags,
        healthy_logical_name_count,
        reason_totals,
    })
}

async fn load_logical_name_red_flags(
    database_url: &str,
    runs: &[CrawlRunHealthSnapshot],
    logical_names: &BTreeSet<String>,
) -> Result<Vec<LogicalNameRedFlag>> {
    let mut signals_by_run = BTreeMap::<i64, BTreeMap<String, LogicalNameRunSignal>>::new();
    for run in runs {
        let mut logical_map = BTreeMap::<String, LogicalNameRunSignal>::new();

        for fetch_log in load_crawl_fetch_logs(database_url, run.crawl_run_id).await? {
            logical_map
                .entry(fetch_log.logical_name.clone())
                .or_default()
                .fetch_logs
                .push(fetch_log);
        }

        for parse_error in load_crawl_parse_errors(database_url, run.crawl_run_id).await? {
            logical_map
                .entry(parse_error.logical_name.clone())
                .or_default()
                .parse_errors
                .push(parse_error);
        }

        signals_by_run.insert(run.crawl_run_id, logical_map);
    }

    let mut red_flags = Vec::new();
    for logical_name in logical_names {
        let mut observed_runs = 0_usize;
        let mut successful_runs = 0_usize;
        let mut red_runs = 0_usize;
        let mut consecutive_red_runs = 0_usize;
        let mut latest_fetch_status = None;
        let mut latest_error = None;
        let mut missing_from_latest_run = false;

        for (index, run) in runs.iter().enumerate() {
            let signal = signals_by_run
                .get(&run.crawl_run_id)
                .and_then(|logical_map| logical_map.get(logical_name));
            let fetch_status = signal.and_then(|signal| summarize_fetch_status(&signal.fetch_logs));
            let parse_error = signal.and_then(|signal| summarize_parse_error(&signal.parse_errors));
            let run_is_observed = signal.is_some();
            let run_is_red = is_red_signal(
                fetch_status.as_deref(),
                parse_error.as_ref(),
                run_is_observed,
            );
            let run_is_green = is_green_signal(fetch_status.as_deref(), parse_error.as_ref());

            if run_is_observed {
                observed_runs += 1;
            }
            if run_is_green {
                successful_runs += 1;
            }
            if run_is_red {
                red_runs += 1;
            }
            if run_is_red && consecutive_red_runs == index {
                consecutive_red_runs += 1;
            }

            if index == 0 {
                missing_from_latest_run = !run_is_observed;
                latest_fetch_status = fetch_status.clone();
                latest_error = parse_error.clone();
            }
        }

        let mut reasons = Vec::new();
        if runs.is_empty() || observed_runs == 0 {
            reasons.push("no_recent_data".to_string());
        } else {
            if missing_from_latest_run {
                reasons.push("missing_from_latest_run".to_string());
            }
            match latest_fetch_status.as_deref() {
                Some("fetch_failed") => reasons.push("latest_fetch_failed".to_string()),
                Some("blocked_robots") => reasons.push("latest_blocked_robots".to_string()),
                Some("blocked_policy") => reasons.push("latest_blocked_policy".to_string()),
                _ => {}
            }
            if let Some(error) = &latest_error {
                reasons.push(format!("latest_parse_error:{}", error.code));
            }
            if successful_runs == 0 {
                reasons.push("no_successful_recent_runs".to_string());
            }
            if red_runs >= 2 {
                reasons.push("repeated_recent_failures".to_string());
            }
        }

        if !reasons.is_empty() {
            red_flags.push(LogicalNameRedFlag {
                logical_name: logical_name.clone(),
                reasons,
                latest_fetch_status,
                observed_runs,
                successful_runs,
                red_runs,
                consecutive_red_runs,
                latest_error,
            });
        }
    }

    red_flags.sort_by(|left, right| {
        right
            .consecutive_red_runs
            .cmp(&left.consecutive_red_runs)
            .then_with(|| right.red_runs.cmp(&left.red_runs))
            .then_with(|| left.logical_name.cmp(&right.logical_name))
    });

    Ok(red_flags)
}

pub async fn serve_manifest_dir(
    settings: &AppSettings,
    manifest_dir: impl AsRef<Path>,
    poll_interval_secs: u64,
) -> Result<()> {
    let manifest_dir = manifest_dir.as_ref().to_path_buf();
    loop {
        crawl_manifest_dir_once(settings, &manifest_dir).await?;

        tokio::time::sleep(Duration::from_secs(poll_interval_secs.max(1))).await;
    }
}

async fn crawl_manifest_dir_once(settings: &AppSettings, manifest_dir: &Path) -> Result<()> {
    for manifest in list_manifest_paths(manifest_dir)? {
        let loaded_manifest = match load_manifest(&manifest) {
            Ok(loaded_manifest) => loaded_manifest,
            Err(error) => {
                tracing::warn!(
                    manifest = %manifest.display(),
                    %error,
                    "skipping crawler manifest because it failed to load"
                );
                continue;
            }
        };
        let source_maturity = loaded_manifest.effective_source_maturity();
        if source_maturity != SourceMaturity::LiveReady {
            tracing::info!(
                manifest = %manifest.display(),
                source_id = %loaded_manifest.source_id,
                source_maturity = %source_maturity,
                "skipping crawler manifest because source_maturity is not live_ready"
            );
            continue;
        }
        match run_crawl_command(settings, &manifest).await {
            Ok(summary) => tracing::info!(
                crawl_run_id = summary.crawl_run_id,
                fetched_targets = summary.fetched_targets,
                parsed_rows = summary.parsed_rows,
                imported_rows = summary.imported_rows,
                "crawler manifest completed"
            ),
            Err(error) => {
                tracing::warn!(manifest = %manifest.display(), %error, "crawler manifest failed")
            }
        }
    }

    Ok(())
}

async fn record_parse_report(
    database_url: &str,
    report_count: &mut usize,
    entry: CrawlParseReportEntry,
) -> Result<()> {
    record_crawl_parse_report(database_url, &entry).await?;
    *report_count += 1;
    Ok(())
}

async fn record_dedupe_report(
    database_url: &str,
    crawl_run_id: i64,
    report: DedupeReportEntry,
) -> Result<()> {
    record_crawl_dedupe_report(
        database_url,
        &CrawlDedupeReportEntry {
            crawl_run_id,
            dedupe_key: report.dedupe_key,
            kept_event_id: report.kept_event_id,
            dropped_event_id: report.dropped_event_id,
            reason: report.reason,
            details: report.details,
        },
    )
    .await
}

async fn load_recent_reason_trend(
    database_url: &str,
    runs: &[CrawlRunHealthSnapshot],
) -> Result<Vec<RunReasonTrend>> {
    let mut trend = Vec::new();
    for run in runs {
        let mut reasons = BTreeMap::new();
        for fetch_log in load_crawl_fetch_logs(database_url, run.crawl_run_id).await? {
            if matches!(
                fetch_log.fetch_status.as_str(),
                "blocked_policy" | "blocked_robots" | "fetch_failed"
            ) {
                *reasons.entry(fetch_log.fetch_status).or_insert(0) += 1;
            }
        }
        for parse_error in load_crawl_parse_errors(database_url, run.crawl_run_id).await? {
            *reasons
                .entry(format!("parse_error:{}", parse_error.code))
                .or_insert(0) += 1;
        }
        trend.push(RunReasonTrend {
            crawl_run_id: run.crawl_run_id,
            status: run.status.clone(),
            reasons,
        });
    }

    Ok(trend)
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use axum::{
        extract::State,
        http::StatusCode,
        response::{Html, IntoResponse, Redirect},
        routing::get,
        Router,
    };
    use config::{AppSettings, CandidateRetrievalMode, OpenSearchSettings};
    use crawler_core::{ParserExpectedShape, SourceMaturity};
    use tokio::net::TcpListener;
    use tokio_postgres::NoTls;

    use super::{
        crawl_manifest_dir_once, run_doctor_command, run_dry_run_command, run_fetch_command,
        run_health_command, run_parse_command,
    };
    use crate::manifest::check_fixture_shape;
    use crate::report::{format_doctor_summary, format_dry_run_summary, format_health_summary};
    use crate::shared::classify_fetch_error_status;

    #[derive(Clone)]
    struct AppState {
        robots_txt: Arc<String>,
        page_html: Arc<String>,
        page_two_html: Option<Arc<String>>,
    }

    #[derive(Clone)]
    struct PartialFetchAppState {
        robots_txt: Arc<String>,
        page_one_html: Arc<String>,
        page_two_html: Arc<String>,
        page_two_requests: Arc<AtomicUsize>,
    }

    fn default_database_url() -> String {
        std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:postgres@127.0.0.1:5433/geo_line_ranker".to_string()
        })
    }

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    fn fixture(name: &str) -> String {
        let path = repo_root().join("storage/fixtures/crawler").join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read fixture {}: {error}", path.display()))
    }

    fn test_settings(raw_storage_dir: &std::path::Path, database_url: &str) -> AppSettings {
        let root = repo_root();
        AppSettings {
            bind_addr: "127.0.0.1:0".to_string(),
            database_url: database_url.to_string(),
            postgres_pool_max_size: 4,
            redis_url: None,
            profile_id: "local-discovery-generic".to_string(),
            profile_pack_manifest: root
                .join("configs/profiles/local-discovery-generic/profile.yaml")
                .display()
                .to_string(),
            profile_reason_catalog_path: root
                .join("configs/profiles/local-discovery-generic/reasons.yaml")
                .display()
                .to_string(),
            profile_fixture_set_id: Some("minimal".to_string()),
            ranking_config_dir: root.join("configs/ranking").display().to_string(),
            fixture_dir: root.join("storage/fixtures/minimal").display().to_string(),
            raw_storage_dir: raw_storage_dir.display().to_string(),
            algorithm_version: "phase6-test".to_string(),
            candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
            candidate_retrieval_limit: 256,
            opensearch: OpenSearchSettings {
                url: "http://127.0.0.1:9200".to_string(),
                index_name: "geo_line_ranker_candidates".to_string(),
                username: None,
                password: None,
                request_timeout_secs: 5,
            },
            recommendation_cache_ttl_secs: 60,
            worker_poll_interval_ms: 1000,
            worker_retry_delay_secs: 5,
            worker_max_attempts: 3,
        }
    }

    #[test]
    fn classify_fetch_error_status_marks_policy_failures() {
        for message in [
            "host localhost is outside the crawler allowlist",
            "host 127.0.0.1 is private or local and must be explicitly allowed",
            "unsupported URL scheme ftp for ftp://example.com",
            "response content-type is missing and default policy denies it",
            "response content-type text/plain is outside the crawler allowlist",
            "redirect count exceeded max_redirects 5 for https://example.com",
            "response Content-Length 1048577 exceeds max_response_bytes 1048576 for https://example.com",
            "response body 1048577 bytes exceeds max_response_bytes 1048576 for https://example.com",
        ] {
            assert_eq!(classify_fetch_error_status(message), "blocked_policy");
        }

        assert_eq!(
            classify_fetch_error_status("failed to fetch https://example.com"),
            "fetch_failed"
        );
    }

    #[test]
    fn check_fixture_shape_rejects_fixture_path_outside_storage_fixtures() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path();
        let manifest_dir = root.join("configs").join("crawler").join("sources");
        std::fs::create_dir_all(&manifest_dir)?;
        std::fs::create_dir_all(root.join("storage").join("fixtures").join("crawler"))?;
        std::fs::write(
            root.join("outside_fixture.html"),
            "<html><body><h1>Outside</h1></body></html>",
        )?;
        let manifest_path = manifest_dir.join("custom.yaml");
        std::fs::write(&manifest_path, "placeholder")?;

        let error = check_fixture_shape(
            &manifest_path,
            "../../../outside_fixture.html",
            ParserExpectedShape::HtmlHeadingPage,
        )
        .expect_err("outside fixture");

        assert!(format!("{error:#}").contains("outside allowed fixture root"));
        Ok(())
    }

    #[tokio::test]
    async fn fetch_and_parse_crawl_manifest_imports_events() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Seaside Crawl Open Campus</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("custom_example.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: custom-example-success
source_name: Custom example crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
  placement_tags: [home, detail]
targets:
  - logical_name: custom_example
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;
        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;

        assert_eq!(fetch_summary.fetched_targets, 1);
        assert_eq!(parse_summary.imported_rows, 1);
        assert_eq!(health_summary.total_runs, 1);
        assert_eq!(health_summary.succeeded_runs, 1);
        assert_eq!(health_summary.fetch_status_totals.get("fetched"), Some(&1));
        assert!(health_summary.logical_name_red_flags.is_empty());
        assert_eq!(health_summary.healthy_logical_name_count, 1);
        assert!(
            health_summary
                .parse_level_totals
                .get("info")
                .copied()
                .unwrap_or_default()
                >= 2
        );
        assert!(format_health_summary(&health_summary).contains("single_title_page_v1"));

        let event_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = $1
                   AND is_active = TRUE",
                &[&"custom-example-success"],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(event_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn parse_command_uses_manifest_source_id_as_stable_source_key() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);

        let first_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Seaside First Crawl Open Campus</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };
        let first_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(first_state);
        let first_listener = TcpListener::bind("127.0.0.1:0").await?;
        let first_address = first_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(first_listener, first_app).await;
        });

        let first_manifest_path = temp.path().join("first_source.yaml");
        std::fs::write(
            &first_manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: stable-crawl-source
source_name: Stable crawl source
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
  placement_tags: [home, detail]
targets:
  - logical_name: custom_example
    url: http://127.0.0.1:{port}/events
"#,
                port = first_address.port()
            ),
        )?;
        run_fetch_command(&settings, &first_manifest_path).await?;
        run_parse_command(&settings, &first_manifest_path).await?;

        let second_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Seaside Second Crawl Open Campus</h1><time datetime=\"2026-09-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };
        let second_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(second_state);
        let second_listener = TcpListener::bind("127.0.0.1:0").await?;
        let second_address = second_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(second_listener, second_app).await;
        });

        let second_manifest_path = temp.path().join("renamed_source.yaml");
        std::fs::write(
            &second_manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: stable-crawl-source
source_name: Stable crawl source renamed
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
  placement_tags: [home, detail]
targets:
  - logical_name: custom_example
    url: http://127.0.0.1:{port}/events
"#,
                port = second_address.port()
            ),
        )?;
        run_fetch_command(&settings, &second_manifest_path).await?;
        run_parse_command(&settings, &second_manifest_path).await?;

        let active_titles = client
            .query(
                "SELECT title
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY title ASC",
                &[&"stable-crawl-source"],
            )
            .await?;
        assert_eq!(active_titles.len(), 1);
        assert_eq!(
            active_titles[0].get::<_, String>("title"),
            "Seaside Second Crawl Open Campus"
        );

        let stale_active = client
            .query_one(
                "SELECT is_active
                 FROM events
                 WHERE title = $1",
                &[&"Seaside First Crawl Open Campus"],
            )
            .await?
            .get::<_, bool>("is_active");
        assert!(!stale_active);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_blocks_redirected_final_url_outside_allowlist() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Redirected Allowlist Guard</h1></body></html>".to_string(),
            ),
            page_two_html: None,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let redirected_url = format!("http://localhost:{}/redirected", address.port());
        let redirect_handler = {
            let redirected_url = redirected_url.clone();
            move || {
                let redirected_url = redirected_url.clone();
                async move { Redirect::temporary(&redirected_url) }
            }
        };
        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(redirect_handler))
            .route("/redirected", get(page_handler))
            .with_state(state);
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("redirect_allowlist_guard.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: redirect-allowlist-guard
source_name: Redirect allowlist guard
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: redirected_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let error = run_fetch_command(&settings, &manifest_path)
            .await
            .expect_err("redirect outside allowlist should be blocked");
        assert!(error
            .to_string()
            .contains("no crawl targets were fetched successfully"));

        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        assert_eq!(
            health_summary.fetch_status_totals.get("blocked_policy"),
            Some(&1)
        );
        assert_eq!(health_summary.reason_totals.get("blocked_policy"), Some(&1));

        Ok(())
    }

    #[tokio::test]
    async fn blocked_redirect_keeps_shared_staged_fetch_for_latest_successful_run(
    ) -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let shared_page_html =
            "<html><body><h1>Shared Staged Redirect Guard</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>";

        let initial_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(shared_page_html.to_string()),
            page_two_html: None,
        };
        let initial_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(initial_state);
        let initial_listener = TcpListener::bind("127.0.0.1:0").await?;
        let initial_address = initial_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(initial_listener, initial_app).await;
        });

        let manifest_path = temp.path().join("shared_staged_redirect_guard.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: shared-staged-redirect-guard
source_name: Shared staged redirect guard
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: redirected_page
    url: http://127.0.0.1:{port}/events
"#,
                port = initial_address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;

        let redirected_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(shared_page_html.to_string()),
            page_two_html: None,
        };
        let redirected_listener = TcpListener::bind("127.0.0.1:0").await?;
        let redirected_address = redirected_listener.local_addr()?;
        let redirected_url = format!("http://localhost:{}/redirected", redirected_address.port());
        let redirect_handler = {
            let redirected_url = redirected_url.clone();
            move || {
                let redirected_url = redirected_url.clone();
                async move { Redirect::temporary(&redirected_url) }
            }
        };
        let redirected_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(redirect_handler))
            .route("/redirected", get(page_handler))
            .with_state(redirected_state);
        tokio::spawn(async move {
            let _ = axum::serve(redirected_listener, redirected_app).await;
        });

        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: shared-staged-redirect-guard
source_name: Shared staged redirect guard
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: redirected_page
    url: http://127.0.0.1:{port}/events
"#,
                port = redirected_address.port()
            ),
        )?;

        let blocked_error = run_fetch_command(&settings, &manifest_path)
            .await
            .expect_err("redirect outside allowlist should be blocked");
        assert!(blocked_error
            .to_string()
            .contains("no crawl targets were fetched successfully"));

        let parse_summary = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(parse_summary.imported_rows, 1);

        let active_titles = client
            .query(
                "SELECT title
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = 'shared-staged-redirect-guard'
                   AND is_active = TRUE",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| row.get::<_, String>("title"))
            .collect::<Vec<_>>();
        assert_eq!(
            active_titles,
            vec!["Shared Staged Redirect Guard".to_string()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn fetch_blocks_redirected_final_url_disallowed_by_robots() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nDisallow: /private\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Redirected Robots Guard</h1></body></html>".to_string(),
            ),
            page_two_html: None,
        };

        let redirect_handler = || async { Redirect::temporary("/private/hidden") };
        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(redirect_handler))
            .route("/private/hidden", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("redirect_robots_guard.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: redirect-robots-guard
source_name: Redirect robots guard
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: redirected_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let error = run_fetch_command(&settings, &manifest_path)
            .await
            .expect_err("redirected disallowed path should be blocked");
        assert!(error
            .to_string()
            .contains("no crawl targets were fetched successfully"));

        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        assert_eq!(
            health_summary.fetch_status_totals.get("blocked_robots"),
            Some(&1)
        );
        assert_eq!(health_summary.reason_totals.get("blocked_robots"), Some(&1));

        Ok(())
    }

    #[tokio::test]
    async fn serve_manifest_dir_skips_invalid_manifest_and_crawls_valid_one() -> anyhow::Result<()>
    {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let manifest_dir = temp.path().join("configs/crawler/sources");
        std::fs::create_dir_all(&manifest_dir)?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Serve Loop Guard Event</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        std::fs::write(manifest_dir.join("000_invalid.yaml"), "source_id: [")?;
        let valid_manifest_path = manifest_dir.join("100_valid.yaml");
        std::fs::write(
            &valid_manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: serve-loop-guard
source_name: Serve loop guard
source_maturity: live_ready
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
targets:
  - logical_name: served_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        crawl_manifest_dir_once(&settings, &manifest_dir).await?;

        let source_key = "serve-loop-guard";
        let event_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = $1
                   AND is_active = TRUE",
                &[&source_key],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(event_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn partial_crawl_import_keeps_existing_events_active() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = PartialFetchAppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_one_html: Arc::new(
                "<html><body><h1>Seaside Partial Import Page One</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: Arc::new(
                "<html><body><h1>Seaside Partial Import Page Two</h1><time datetime=\"2026-09-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_requests: Arc::new(AtomicUsize::new(0)),
        };

        let app = Router::new()
            .route("/robots.txt", get(partial_robots_handler))
            .route("/events/page1", get(partial_page_one_handler))
            .route("/events/page2", get(partial_page_two_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("partial_import.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: partial-import-local
source_name: Partial import local crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
  placement_tags: [home, detail]
targets:
  - logical_name: partial_page_one
    url: http://127.0.0.1:{port}/events/page1
  - logical_name: partial_page_two
    url: http://127.0.0.1:{port}/events/page2
"#,
                port = address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        let initial_parse = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(initial_parse.imported_rows, 2);

        run_fetch_command(&settings, &manifest_path).await?;
        let partial_parse = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(partial_parse.imported_rows, 1);

        let source_key = "partial-import-local";
        let rows = client
            .query(
                "SELECT title
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY title ASC",
                &[&source_key],
            )
            .await?;
        let titles = rows
            .into_iter()
            .map(|row| row.get::<_, String>("title"))
            .collect::<Vec<_>>();
        assert_eq!(titles.len(), 2);
        assert!(titles.contains(&"Seaside Partial Import Page One".to_string()));
        assert!(titles.contains(&"Seaside Partial Import Page Two".to_string()));

        let warning_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM crawl_parse_reports
                 WHERE crawl_run_id = $1
                   AND code = 'crawl_skipped_stale_deactivation'",
                &[&partial_parse.crawl_run_id],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(warning_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn missing_school_rows_do_not_deactivate_existing_crawl_events() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Seaside Missing School Guard</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };
        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("missing_school_guard.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: missing-school-guard
source_name: Missing school guard crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
targets:
  - logical_name: missing_school_guard_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        let initial_parse = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(initial_parse.imported_rows, 1);

        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: missing-school-guard
source_name: Missing school guard crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_missing
  event_category: open_campus
  is_open_day: true
targets:
  - logical_name: missing_school_guard_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        let dry_run = run_dry_run_command(&settings, &manifest_path).await?;
        assert_eq!(dry_run.imported_rows, 0);
        assert_eq!(dry_run.deactivated_rows, 0);
        assert_eq!(dry_run.missing_school_rows, 1);
        assert!(dry_run
            .warnings
            .iter()
            .any(|issue| issue.code == "missing_school_skips_stale_deactivation"));

        let guarded_parse = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(guarded_parse.imported_rows, 0);

        let source_key = "missing-school-guard";
        let active_titles = client
            .query(
                "SELECT title
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY title ASC",
                &[&source_key],
            )
            .await?
            .into_iter()
            .map(|row| row.get::<_, String>("title"))
            .collect::<Vec<_>>();
        assert_eq!(
            active_titles,
            vec!["Seaside Missing School Guard".to_string()]
        );

        let warning_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM crawl_parse_reports
                 WHERE crawl_run_id = $1
                   AND code = 'crawl_skipped_missing_school_deactivation'",
                &[&guarded_parse.crawl_run_id],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(warning_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_and_parse_shibaura_manifest_imports_seeded_school() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                r#"
<html>
  <body>
    <div class="qua-container">
      <h4 class="qua-wysiwyg-content"><p>本校の教育内容（学校説明会）【オンライン】※要予約</p></h4>
      <div class="qua-unit-text">
        <div class="qua-wysiwyg-content">
          <p>オンラインで実施する中学説明会</p>
        </div>
      </div>
      <div class="qua-field-list">
        <ul>
          <li class="qua-field-list__item"><div class="qua-field-list__item__in"><p><strong>第1回：2026年5月9日 (土) 14：00～15：30</strong></p></div></li>
          <li class="qua-field-list__item"><div class="qua-field-list__item__in"><p><strong>第2回：2026年6月6日 (土) 10：30～12：00</strong></p></div></li>
        </ul>
      </div>
    </div>
    <div class="qua-container">
      <h4 class="qua-wysiwyg-content"><p>教員による学校見学会＊要予約</p></h4>
      <div class="qua-field-list">
        <ul>
          <li class="qua-field-list__item"><div class="qua-field-list__item__in"><p><strong>＜5月＞：2026年5月11日 (月)、13日（水）</strong></p></div></li>
        </ul>
      </div>
    </div>
  </body>
</html>
"#
                .to_string(),
            ),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("shibaura_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: shibaura-local
source_name: Shibaura local crawler
parser_key: shibaura_junior_event_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_shibaura_it_junior
  event_category: admission_event
  placement_tags: [search, detail]
targets:
  - logical_name: junior_event_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;

        assert_eq!(fetch_summary.fetched_targets, 1);
        assert_eq!(parse_summary.imported_rows, 4);

        let rows = client
            .query(
                "SELECT title, to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS starts_at
                 FROM events
                 WHERE source_type = 'crawl'
                   AND school_id = 'school_shibaura_it_junior'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY starts_at ASC, title ASC",
                &[&"shibaura-local"],
            )
            .await?;
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows[0].get::<_, String>("title"),
            "オンラインで実施する中学説明会 第1回"
        );
        assert_eq!(
            rows[0].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2026-05-09")
        );
        assert_eq!(
            rows[1].get::<_, String>("title"),
            "教員による学校見学会 ＜5月＞"
        );

        Ok(())
    }

    #[tokio::test]
    async fn fetch_and_parse_nihon_manifest_imports_seeded_school() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(fixture("nihon_university_junior_info_session.html")),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/info-session", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("nihon_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: nihon-local
source_name: Nihon local crawler
parser_key: nihon_university_junior_info_session_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_nihon_university_junior
  event_category: admission_event
  placement_tags: [search, detail]
targets:
  - logical_name: junior_info_session_page
    url: http://127.0.0.1:{port}/info-session
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;
        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        let health_text = format_health_summary(&health_summary);

        assert_eq!(fetch_summary.fetched_targets, 1);
        assert_eq!(parse_summary.imported_rows, 8);
        assert!(health_summary.logical_name_red_flags.is_empty());
        assert!(health_summary.reason_totals.is_empty());
        assert!(health_text.contains("reason totals: -"));

        let rows = client
            .query(
                "SELECT title, to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS starts_at
                 FROM events
                 WHERE source_type = 'crawl'
                   AND school_id = 'school_nihon_university_junior'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY starts_at ASC, title ASC",
                &[&"nihon-local"],
            )
            .await?;
        assert_eq!(rows.len(), 8);
        assert_eq!(rows[0].get::<_, String>("title"), "外部フェア");
        assert_eq!(
            rows[0].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2025-07-11")
        );
        assert_eq!(rows[3].get::<_, String>("title"), "文化祭");
        assert_eq!(rows[4].get::<_, String>("title"), "学校（入試）説明会");
        assert_eq!(
            rows[7].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2027-01-16")
        );

        Ok(())
    }

    #[tokio::test]
    async fn fetch_and_parse_aoyama_manifest_imports_seeded_school() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(fixture("aoyama_junior_school_tour.html")),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/admission/explanation.html", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("aoyama_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: aoyama-local
source_name: Aoyama local crawler
source_maturity: live_ready
parser_key: aoyama_junior_school_tour_v1
expected_shape: html_school_tour_blocks
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_aoyama_gakuin_junior
  event_category: admission_event
  is_open_day: true
  placement_tags: [search, detail]
targets:
  - logical_name: school_tour_page
    url: http://127.0.0.1:{port}/admission/explanation.html
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;
        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        let health_text = format_health_summary(&health_summary);

        assert_eq!(fetch_summary.fetched_targets, 1);
        assert_eq!(parse_summary.imported_rows, 10);
        assert!(health_summary.logical_name_red_flags.is_empty());
        assert!(health_summary.reason_totals.is_empty());
        assert!(health_text.contains("source_maturity: live_ready"));
        assert!(health_text.contains("expected_shape: html_school_tour_blocks"));

        let rows = client
            .query(
                "SELECT title, to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS starts_at
                 FROM events
                 WHERE source_type = 'crawl'
                   AND school_id = 'school_aoyama_gakuin_junior'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY starts_at ASC, title ASC",
                &[&"aoyama-local"],
            )
            .await?;
        assert_eq!(rows.len(), 10);
        assert_eq!(
            rows[0].get::<_, String>("title"),
            "キリスト教学校合同フェア"
        );
        assert_eq!(
            rows[0].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2026-03-20")
        );
        assert!(rows.iter().any(|row| {
            row.get::<_, String>("title") == "学校説明会 第1回"
                && row.get::<_, Option<String>>("starts_at").as_deref() == Some("2026-06-13")
        }));

        let tokyo_private_school_expo_dates = rows
            .iter()
            .filter(|row| row.get::<_, String>("title") == "東京都私立学校展 （※資料参加のみ）")
            .map(|row| {
                row.get::<_, Option<String>>("starts_at")
                    .expect("starts_at")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            tokyo_private_school_expo_dates,
            vec!["2026-08-29".to_string(), "2026-08-30".to_string()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn doctor_reports_html_robots_and_school_presence() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler doctor test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new(String::new()),
            page_html: Arc::new(fixture("nihon_university_junior_info_session.html")),
            page_two_html: None,
        };

        let app = Router::new()
            .route(
                "/robots.txt",
                get(|| async { Redirect::temporary("/robots-home") }),
            )
            .route(
                "/robots-home",
                get(|| async { Html("<html><body>home</body></html>") }),
            )
            .route(
                "/terms",
                get(|| async { Html("<html><body>terms</body></html>") }),
            )
            .route("/info-session", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("doctor_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: doctor-local
source_name: Doctor local crawler
parser_key: nihon_university_junior_info_session_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: http://127.0.0.1:{port}/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_nihon_university_junior
  event_category: admission_event
targets:
  - logical_name: junior_info_session_page
    url: http://127.0.0.1:{port}/info-session
"#,
                port = address.port()
            ),
        )?;

        let summary = run_doctor_command(&settings, &manifest_path).await?;
        let doctor_text = format_doctor_summary(&summary);

        assert!(summary.parser_registered);
        assert_eq!(summary.source_maturity, SourceMaturity::LiveReady);
        assert_eq!(
            summary.expected_shape,
            Some(ParserExpectedShape::HtmlMonthlyDlPairs)
        );
        assert_eq!(summary.targets.len(), 1);
        assert_eq!(summary.targets[0].school_exists, Some(true));
        assert_eq!(summary.targets[0].shape_status.as_deref(), Some("matched"));
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.code == "robots_redirected"));
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.code == "robots_unexpected_content_type"));
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.code == "robots_html_body"));
        assert!(doctor_text.contains("targets:"));
        assert!(doctor_text.contains("school_exists=true"));
        assert!(doctor_text.contains("source_maturity: live_ready"));
        assert!(doctor_text.contains("expected_shape: html_monthly_dl_pairs"));

        Ok(())
    }

    #[tokio::test]
    async fn doctor_checks_fixture_shape_when_live_fetch_is_disabled() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler fixture doctor test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        std::fs::write(
            temp.path().join("nihon_fixture.html"),
            fixture("nihon_university_junior_info_session.html"),
        )?;

        let app = Router::new()
            .route("/robots.txt", get(|| async { "User-agent: *\nAllow: /\n" }))
            .route(
                "/terms",
                get(|| async { Html("<html><body>terms</body></html>") }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("doctor_fixture.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: doctor-fixture
source_name: Doctor fixture crawler
source_maturity: parser_only
parser_key: nihon_university_junior_info_session_v1
expected_shape: html_monthly_dl_pairs
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  live_fetch_enabled: false
  live_fetch_block_reason: fixture-only parser check
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: http://127.0.0.1:{port}/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_nihon_university_junior
  event_category: admission_event
targets:
  - logical_name: junior_info_session_page
    url: http://127.0.0.1:{port}/info-session
    fixture_path: nihon_fixture.html
"#,
                port = address.port()
            ),
        )?;

        let summary = run_doctor_command(&settings, &manifest_path).await?;
        let doctor_text = format_doctor_summary(&summary);

        assert_eq!(summary.source_maturity, SourceMaturity::ParserOnly);
        assert_eq!(summary.targets[0].shape_status.as_deref(), Some("matched"));
        assert!(summary
            .targets
            .first()
            .and_then(|target| target.shape_detail.as_deref())
            .is_some_and(|detail| detail.contains("fixture_path")));
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.code == "live_fetch_disabled"));
        assert!(doctor_text.contains("shape=matched"));
        assert!(doctor_text.contains("source_maturity_not_live_ready"));

        Ok(())
    }

    #[tokio::test]
    async fn doctor_flags_expected_shape_mismatch() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler doctor mismatch test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new("<html><body><p>shape mismatch</p></body></html>".to_string()),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route(
                "/terms",
                get(|| async { Html("<html><body>terms</body></html>") }),
            )
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("doctor_shape_mismatch.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: doctor-shape-mismatch
source_name: Doctor shape mismatch crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: http://127.0.0.1:{port}/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: broken_shape
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let summary = run_doctor_command(&settings, &manifest_path).await?;
        let doctor_text = format_doctor_summary(&summary);

        assert_eq!(
            summary.expected_shape,
            Some(ParserExpectedShape::HtmlHeadingPage)
        );
        assert_eq!(summary.targets[0].shape_status.as_deref(), Some("mismatch"));
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.code == "expected_shape_mismatch"));
        assert!(doctor_text.contains("promotion_gate: blocked"));
        assert!(doctor_text.contains("expected_shape_mismatch"));
        assert!(doctor_text.contains("shape=mismatch"));
        assert!(doctor_text.contains("expected_shape: html_heading_page"));

        Ok(())
    }

    #[tokio::test]
    async fn doctor_surfaces_school_lookup_failures() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let mut settings = test_settings(
            &temp.path().join("raw"),
            "postgres://127.0.0.1:9/geo_line_ranker",
        );
        settings.database_url = "postgres://127.0.0.1:9/geo_line_ranker".to_string();
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Doctor School Lookup Warning</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route(
                "/terms",
                get(|| async { Html("<html><body>terms</body></html>") }),
            )
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("doctor_school_lookup_failed.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: doctor-school-lookup-failed
source_name: Doctor school lookup failed
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: http://127.0.0.1:{port}/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: school_lookup_warning
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let summary = run_doctor_command(&settings, &manifest_path).await?;
        let doctor_text = format_doctor_summary(&summary);

        assert_eq!(summary.targets.len(), 1);
        assert_eq!(summary.targets[0].school_exists, None);
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.code == "school_lookup_failed"));
        assert!(doctor_text.contains("school_exists=unknown"));
        assert!(doctor_text.contains("school_lookup_failed"));

        Ok(())
    }

    #[tokio::test]
    async fn doctor_skips_shape_probe_when_robots_policy_is_unknown() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler doctor robots test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let probe_requests = Arc::new(AtomicUsize::new(0));

        let app = Router::new()
            .route(
                "/robots.txt",
                get(|| async {
                    (
                        StatusCode::OK,
                        [("content-type", "text/plain; charset=utf-8")],
                        "",
                    )
                }),
            )
            .route(
                "/terms",
                get(|| async { Html("<html><body>terms</body></html>") }),
            )
            .route(
                "/events",
                get({
                    let probe_requests = probe_requests.clone();
                    move || {
                        let probe_requests = probe_requests.clone();
                        async move {
                            probe_requests.fetch_add(1, Ordering::SeqCst);
                            (
                                StatusCode::OK,
                                [("content-type", "text/html; charset=utf-8")],
                                "<html><body><h1>Doctor Probe Should Not Run</h1></body></html>",
                            )
                        }
                    }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("doctor_unknown_robots.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: doctor-unknown-robots
source_name: Doctor unknown robots
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: http://127.0.0.1:{port}/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: unknown_robots_target
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let summary = run_doctor_command(&settings, &manifest_path).await?;

        assert_eq!(summary.targets.len(), 1);
        assert_eq!(summary.targets[0].robots_allowed, None);
        assert_eq!(summary.targets[0].shape_status.as_deref(), Some("skipped"));
        assert_eq!(
            summary.targets[0].shape_detail.as_deref(),
            Some("robots policy could not be evaluated for this target")
        );
        assert_eq!(probe_requests.load(Ordering::SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn doctor_revalidates_redirected_shape_probe_against_allowlist() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler doctor redirect test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Doctor Redirect Guard</h1></body></html>".to_string(),
            ),
            page_two_html: None,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let redirected_url = format!("http://localhost:{}/redirected", address.port());
        let redirect_handler = {
            let redirected_url = redirected_url.clone();
            move || {
                let redirected_url = redirected_url.clone();
                async move { Redirect::temporary(&redirected_url) }
            }
        };
        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route(
                "/terms",
                get(|| async { Html("<html><body>terms</body></html>") }),
            )
            .route("/events", get(redirect_handler))
            .route("/redirected", get(page_handler))
            .with_state(state);
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("doctor_redirect_allowlist_guard.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: doctor-redirect-allowlist-guard
source_name: Doctor redirect allowlist guard
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: http://127.0.0.1:{port}/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: redirected_target
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let summary = run_doctor_command(&settings, &manifest_path).await?;

        assert_eq!(summary.targets.len(), 1);
        assert_eq!(
            summary.targets[0].shape_status.as_deref(),
            Some("fetch_failed")
        );
        assert!(summary.targets[0]
            .shape_detail
            .as_deref()
            .unwrap_or_default()
            .contains("resolved final_url violated crawler allowlist"));
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.code == "target_shape_fetch_failed"));

        Ok(())
    }

    #[tokio::test]
    async fn doctor_evaluates_robots_from_full_response_body() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!(
                "skipping crawler doctor robots-body test because PostgreSQL is not reachable"
            );
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let probe_requests = Arc::new(AtomicUsize::new(0));
        let long_robots = format!(
            "User-agent: *\n{}Disallow: /blocked\n",
            (0..400)
                .map(|index| format!("Allow: /padding-{index:04}\n"))
                .collect::<String>()
        );

        let app = Router::new()
            .route(
                "/robots.txt",
                get({
                    let long_robots = long_robots.clone();
                    move || {
                        let long_robots = long_robots.clone();
                        async move {
                            (
                                StatusCode::OK,
                                [("content-type", "text/plain; charset=utf-8")],
                                long_robots,
                            )
                        }
                    }
                }),
            )
            .route(
                "/terms",
                get(|| async { Html("<html><body>terms</body></html>") }),
            )
            .route(
                "/blocked/events",
                get({
                    let probe_requests = probe_requests.clone();
                    move || {
                        let probe_requests = probe_requests.clone();
                        async move {
                            probe_requests.fetch_add(1, Ordering::SeqCst);
                            (
                                StatusCode::OK,
                                [("content-type", "text/html; charset=utf-8")],
                                "<html><body><h1>Doctor Probe Should Stay Blocked</h1></body></html>",
                            )
                        }
                    }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("doctor_full_robots_body.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: doctor-full-robots-body
source_name: Doctor full robots body
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: http://127.0.0.1:{port}/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: blocked_target
    url: http://127.0.0.1:{port}/blocked/events
"#,
                port = address.port()
            ),
        )?;

        let summary = run_doctor_command(&settings, &manifest_path).await?;

        assert_eq!(summary.targets.len(), 1);
        assert_eq!(summary.targets[0].robots_allowed, Some(false));
        assert_eq!(summary.targets[0].shape_status.as_deref(), Some("skipped"));
        assert_eq!(
            summary.targets[0].shape_detail.as_deref(),
            Some("target is blocked by robots policy")
        );
        assert_eq!(probe_requests.load(Ordering::SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn dry_run_predicts_import_inactive_and_date_drift() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler dry-run test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);

        let initial_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(fixture("nihon_university_junior_info_session.html")),
            page_two_html: None,
        };
        let initial_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/info-session", get(page_handler))
            .with_state(initial_state);
        let initial_listener = TcpListener::bind("127.0.0.1:0").await?;
        let initial_address = initial_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(initial_listener, initial_app).await;
        });

        let manifest_path = temp.path().join("dry_run_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: dry-run-local
source_name: Dry-run local crawler
parser_key: nihon_university_junior_info_session_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_nihon_university_junior
  event_category: admission_event
  placement_tags: [search, detail]
targets:
  - logical_name: junior_info_session_page
    url: http://127.0.0.1:{port}/info-session
"#,
                port = initial_address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        run_parse_command(&settings, &manifest_path).await?;

        let drift_html = r#"
<html>
  <body>
    <div class="schedule_box">
      <h3 class="ttl">6月</h3>
      <dl class="text_box">
        <dt>1日（月）9:15～10:15</dt>
        <dd>
          <p class="event_name">ミニ説明会</p>
          <div class="btn_box">
            <p class="link_btn blank is-junior"><a href="/assets/pdf/info-session/junior_enent_20260701.pdf" target="_blank">詳細</a></p>
            <p class="link_btn blank is-junior"><a href="https://mirai-compass.net/usr/nihonuj/common/loginEvent.jsf" target="_blank">申込み</a></p>
          </div>
        </dd>
      </dl>
    </div>
  </body>
</html>
"#;

        let second_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(drift_html.to_string()),
            page_two_html: None,
        };
        let second_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/info-session", get(page_handler))
            .with_state(second_state);
        let second_listener = TcpListener::bind("127.0.0.1:0").await?;
        let second_address = second_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(second_listener, second_app).await;
        });

        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: dry-run-local
source_name: Dry-run local crawler
parser_key: nihon_university_junior_info_session_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_nihon_university_junior
  event_category: admission_event
  placement_tags: [search, detail]
targets:
  - logical_name: junior_info_session_page
    url: http://127.0.0.1:{port}/info-session
"#,
                port = second_address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        let summary = run_dry_run_command(&settings, &manifest_path).await?;
        let dry_run_text = format_dry_run_summary(&summary);

        assert_eq!(summary.parsed_rows, 1);
        assert_eq!(summary.deduped_rows, 1);
        assert_eq!(summary.imported_rows, 1);
        assert_eq!(summary.deactivated_rows, 8);
        assert_eq!(summary.date_drift_warnings, 1);
        assert!(summary
            .warnings
            .iter()
            .any(|issue| issue.code == "date_drift"));
        assert!(dry_run_text.contains("promotion_gate: review"));
        assert!(dry_run_text.contains("review[date_drift]"));
        assert!(dry_run_text.contains("inactive=8"));
        assert!(dry_run_text.contains("date_drift"));

        Ok(())
    }

    #[tokio::test]
    async fn fetch_and_parse_keio_manifest_imports_seeded_school() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(fixture("keio_event_listing_page_1.html")),
            page_two_html: Some(Arc::new(fixture("keio_event_listing_page_2.html"))),
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events/page1", get(page_handler))
            .route("/events/page2", get(page_two_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("keio_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: keio-local
source_name: Keio local crawler
parser_key: keio_event_listing_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_keio
  event_category: general
  placement_tags: [search, detail]
targets:
  - logical_name: event_page_1
    url: http://127.0.0.1:{port}/events/page1
  - logical_name: event_page_2
    url: http://127.0.0.1:{port}/events/page2
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;

        assert_eq!(fetch_summary.fetched_targets, 2);
        assert_eq!(parse_summary.imported_rows, 4);

        let rows = client
            .query(
                "SELECT title, to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS starts_at
                 FROM events
                 WHERE source_type = 'crawl'
                   AND school_id = 'school_keio'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY starts_at ASC, title ASC",
                &[&"keio-local"],
            )
            .await?;
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows[0].get::<_, String>("title"),
            "慶應義塾ミュージアム・コモンズ 展示"
        );
        assert_eq!(
            rows[0].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2026-03-09")
        );
        assert_eq!(
            rows[2].get::<_, String>("title"),
            "オープンキャンパス2026～講義編～"
        );
        assert_eq!(
            rows[3].get::<_, String>("title"),
            "ニューヨーク学院（高等部）学院説明会（シンガポール）"
        );

        Ok(())
    }

    #[tokio::test]
    async fn parse_zero_event_targets_keep_existing_rows_active() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler zero-event parse test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);

        let initial_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(fixture("keio_event_listing_page_1.html")),
            page_two_html: Some(Arc::new(fixture("keio_event_listing_page_2.html"))),
        };
        let initial_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events/page1", get(page_handler))
            .route("/events/page2", get(page_two_handler))
            .with_state(initial_state);
        let initial_listener = TcpListener::bind("127.0.0.1:0").await?;
        let initial_address = initial_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(initial_listener, initial_app).await;
        });

        let manifest_path = temp.path().join("keio_zero_events.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: keio-zero-events
source_name: Keio zero-events crawler
parser_key: keio_event_listing_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_keio
  event_category: general
  placement_tags: [search, detail]
targets:
  - logical_name: event_page_1
    url: http://127.0.0.1:{port}/events/page1
  - logical_name: event_page_2
    url: http://127.0.0.1:{port}/events/page2
"#,
                port = initial_address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        let initial_parse = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(initial_parse.imported_rows, 4);

        let empty_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new("<html><body><p>No events right now</p></body></html>".to_string()),
            page_two_html: Some(Arc::new(
                "<html><body><p>No events right now</p></body></html>".to_string(),
            )),
        };
        let empty_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events/page1", get(page_handler))
            .route("/events/page2", get(page_two_handler))
            .with_state(empty_state);
        let empty_listener = TcpListener::bind("127.0.0.1:0").await?;
        let empty_address = empty_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(empty_listener, empty_app).await;
        });

        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: keio-zero-events
source_name: Keio zero-events crawler
parser_key: keio_event_listing_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_keio
  event_category: general
  placement_tags: [search, detail]
targets:
  - logical_name: event_page_1
    url: http://127.0.0.1:{port}/events/page1
  - logical_name: event_page_2
    url: http://127.0.0.1:{port}/events/page2
"#,
                port = empty_address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        let empty_dry_run = run_dry_run_command(&settings, &manifest_path).await?;
        assert_eq!(empty_dry_run.parsed_rows, 0);
        assert_eq!(empty_dry_run.imported_rows, 0);
        assert_eq!(empty_dry_run.deactivated_rows, 0);
        assert!(empty_dry_run
            .warnings
            .iter()
            .any(|issue| issue.code == "parsed_zero_rows"));
        assert!(empty_dry_run
            .warnings
            .iter()
            .any(|issue| issue.code == "no_events_found"));
        assert!(empty_dry_run
            .warnings
            .iter()
            .any(|issue| issue.code == "partial_import_skips_stale_deactivation"));

        let empty_parse = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(empty_parse.parsed_rows, 0);
        assert_eq!(empty_parse.imported_rows, 0);

        let active_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = $1
                   AND is_active = TRUE",
                &[&"keio-zero-events"],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(active_count, 4);

        let warning_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM crawl_parse_reports
                 WHERE crawl_run_id = $1
                   AND code = 'crawl_skipped_stale_deactivation'",
                &[&empty_parse.crawl_run_id],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(warning_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_and_parse_hachioji_manifest_imports_seeded_school() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(fixture("hachioji_junior_session_tables.html")),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("hachioji_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: hachioji-local
source_name: Hachioji local crawler
parser_key: hachioji_junior_session_tables_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_hachioji_gakuen_junior
  event_category: admission_event
  placement_tags: [search, detail]
targets:
  - logical_name: junior_session_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;
        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        let health_text = format_health_summary(&health_summary);

        assert_eq!(fetch_summary.fetched_targets, 1);
        assert_eq!(parse_summary.imported_rows, 5);
        assert!(health_summary.logical_name_red_flags.is_empty());
        assert!(health_summary.reason_totals.is_empty());
        assert!(health_text.contains("promotion_gate: ready"));

        let rows = client
            .query(
                "SELECT title, to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS starts_at
                 FROM events
                 WHERE source_type = 'crawl'
                   AND school_id = 'school_hachioji_gakuen_junior'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY starts_at ASC, title ASC",
                &[&"hachioji-local"],
            )
            .await?;
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].get::<_, String>("title"), "保護者対象説明会");
        assert_eq!(
            rows[0].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2026-05-07")
        );
        assert_eq!(
            rows[4].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2027-01-09")
        );

        Ok(())
    }

    #[tokio::test]
    async fn health_flags_logical_name_with_recent_fetch_failure() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Seaside Crawl Open Campus</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("custom_with_failure.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: custom-example-failure
source_name: Custom example crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
  placement_tags: [home, detail]
targets:
  - logical_name: healthy_page
    url: http://127.0.0.1:{port}/events
  - logical_name: broken_page
    url: http://127.0.0.1:{port}/missing
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;
        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        let health_text = format_health_summary(&health_summary);

        assert_eq!(fetch_summary.fetched_targets, 1);
        assert_eq!(parse_summary.imported_rows, 1);
        assert_eq!(health_summary.logical_name_red_flags.len(), 1);
        assert_eq!(health_summary.healthy_logical_name_count, 1);
        assert_eq!(health_summary.reason_totals.get("fetch_failed"), Some(&1));
        assert!(health_text.contains("recent reason trend:"));
        assert!(health_text.contains("fetch_failed:1"));
        assert_eq!(
            health_summary.logical_name_red_flags[0].logical_name,
            "broken_page"
        );
        assert!(health_summary.logical_name_red_flags[0]
            .reasons
            .contains(&"latest_fetch_failed".to_string()));
        assert!(health_text.contains("reason totals: fetch_failed:1"));
        assert!(health_text.contains("broken_page"));
        assert!(health_text.contains("latest_fetch_failed"));

        Ok(())
    }

    #[tokio::test]
    async fn health_counts_latest_parse_error_reasons() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new("<html><body><p>heading missing</p></body></html>".to_string()),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("custom_parse_error.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: custom-parse-error
source_name: Custom parse error crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
targets:
  - logical_name: parse_error_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        assert_eq!(fetch_summary.fetched_targets, 1);
        let error = run_parse_command(&settings, &manifest_path)
            .await
            .expect_err("parse should fail");
        assert!(error
            .to_string()
            .contains("no crawler events were parsed successfully"));

        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        let health_text = format_health_summary(&health_summary);

        assert_eq!(
            health_summary
                .reason_totals
                .get("latest_parse_error:parse_failed"),
            Some(&1)
        );
        assert!(health_text.contains("recent reason trend:"));
        assert!(health_text.contains("parse_error:parse_failed:1"));
        assert!(health_text.contains("latest_parse_error:parse_failed:1"));
        assert_eq!(health_summary.logical_name_red_flags.len(), 1);
        assert!(health_summary.logical_name_red_flags[0]
            .reasons
            .contains(&"latest_parse_error:parse_failed".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn health_flags_manifest_policy_block() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let manifest_path = temp.path().join("policy_blocked.yaml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: policy-blocked
source_name: Policy blocked crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  live_fetch_enabled: false
  live_fetch_block_reason: robots URL is not published yet.
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: blocked_page
    url: https://example.com/events
"#,
        )?;

        let error = run_fetch_command(&settings, &manifest_path)
            .await
            .expect_err("policy-blocked fetch");
        assert!(error
            .to_string()
            .contains("live fetch disabled by manifest policy"));

        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        let health_text = format_health_summary(&health_summary);

        assert_eq!(health_summary.failed_runs, 1);
        assert_eq!(
            health_summary.source_maturity,
            SourceMaturity::PolicyBlocked
        );
        assert_eq!(
            health_summary.expected_shape,
            Some(ParserExpectedShape::HtmlHeadingPage)
        );
        assert_eq!(
            health_summary.fetch_status_totals.get("blocked_policy"),
            Some(&1)
        );
        assert_eq!(health_summary.logical_name_red_flags.len(), 1);
        assert_eq!(
            health_summary.logical_name_red_flags[0].logical_name,
            "blocked_page"
        );
        assert_eq!(health_summary.reason_totals.get("blocked_policy"), Some(&1));
        assert!(health_text.contains("recent reason trend:"));
        assert!(health_text.contains("blocked_policy:1"));
        assert!(health_summary.logical_name_red_flags[0]
            .reasons
            .contains(&"latest_blocked_policy".to_string()));
        assert!(health_text.contains("reason totals: blocked_policy:1"));
        assert!(health_text.contains("latest_blocked_policy"));
        assert!(health_text.contains("source_maturity: policy_blocked"));
        assert!(health_text.contains("expected_shape: html_heading_page"));
        assert!(health_text.contains("promotion_gate: blocked"));
        assert!(health_text.contains("source_maturity_not_live_ready"));

        Ok(())
    }

    #[tokio::test]
    async fn robots_bootstrap_failure_marks_crawl_run_failed() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let closed_listener = TcpListener::bind("127.0.0.1:0").await?;
        let closed_port = closed_listener.local_addr()?.port();
        drop(closed_listener);

        let manifest_path = temp.path().join("robots_bootstrap_failure.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: robots-bootstrap-failure
source_name: Robots bootstrap failure crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: robots_bootstrap_failure_page
    url: http://127.0.0.1:{port}/events
"#,
                port = closed_port
            ),
        )?;

        let error = run_fetch_command(&settings, &manifest_path)
            .await
            .expect_err("robots bootstrap should fail");
        assert!(
            error.to_string().contains("failed"),
            "unexpected fetch error: {error}"
        );

        let manifest_key = manifest_path.canonicalize()?.display().to_string();
        let status = client
            .query_one(
                "SELECT status
                 FROM crawl_runs
                 WHERE manifest_path = $1
                 ORDER BY id DESC
                 LIMIT 1",
                &[&manifest_key],
            )
            .await?
            .get::<_, String>("status");
        assert_eq!(status, "failed");

        Ok(())
    }

    async fn robots_handler(State(state): State<AppState>) -> impl IntoResponse {
        (StatusCode::OK, (*state.robots_txt).clone())
    }

    async fn partial_robots_handler(
        State(state): State<PartialFetchAppState>,
    ) -> impl IntoResponse {
        (StatusCode::OK, (*state.robots_txt).clone())
    }

    async fn page_handler(State(state): State<AppState>) -> impl IntoResponse {
        (
            StatusCode::OK,
            [("content-type", "text/html; charset=utf-8")],
            (*state.page_html).clone(),
        )
    }

    async fn partial_page_one_handler(
        State(state): State<PartialFetchAppState>,
    ) -> impl IntoResponse {
        (
            StatusCode::OK,
            [("content-type", "text/html; charset=utf-8")],
            (*state.page_one_html).clone(),
        )
    }

    async fn page_two_handler(State(state): State<AppState>) -> impl IntoResponse {
        (
            StatusCode::OK,
            [("content-type", "text/html; charset=utf-8")],
            state
                .page_two_html
                .as_deref()
                .map(ToString::to_string)
                .unwrap_or_default(),
        )
    }

    async fn partial_page_two_handler(
        State(state): State<PartialFetchAppState>,
    ) -> impl IntoResponse {
        let request_count = state.page_two_requests.fetch_add(1, Ordering::SeqCst);
        if request_count == 0 {
            (
                StatusCode::OK,
                [("content-type", "text/html; charset=utf-8")],
                (*state.page_two_html).clone(),
            )
                .into_response()
        } else {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "simulated partial fetch failure",
            )
                .into_response()
        }
    }
}
