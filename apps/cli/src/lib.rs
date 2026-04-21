use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use cache::RecommendationCache;
use chrono::{DateTime, FixedOffset, NaiveDate};
use config::{AppSettings, RankingProfiles};
use generic_csv::{
    count_csv_rows, load_manifest, read_csv_rows, stage_raw_files, stage_single_csv_file,
    PreparedSourceFile, SourceFileSpec, SourceManifest,
};
use jp_postal::{parse_postal_codes, PARSER_VERSION as JP_POSTAL_PARSER_VERSION};
use jp_rail::{parse_rail_stations, PARSER_VERSION as JP_RAIL_PARSER_VERSION};
use jp_school::{
    parse_school_codes, parse_school_geodata, SCHOOL_CODES_PARSER_VERSION,
    SCHOOL_GEODATA_PARSER_VERSION,
};
use serde_json::json;
use storage::{RecommendationRepository, SnapshotTuning};
use storage_opensearch::ProjectionSyncService;
use storage_postgres::{
    begin_import_run, derive_school_station_links, finish_import_run, import_event_csv,
    import_jp_postal, import_jp_rail, import_jp_school_codes, import_jp_school_geodata,
    record_import_report, upsert_import_run_file, EventCsvRecord, ImportReportEntry,
    ImportRunFileAudit, ImportSummary,
};

const EVENT_CSV_PARSER_VERSION: &str = "event-csv-v1";
const EVENT_CSV_SOURCE_ID: &str = "event-csv";

#[derive(Debug, Clone, Copy)]
pub enum ImportTarget {
    JpRail,
    JpPostal,
    JpSchoolCodes,
    JpSchoolGeodata,
}

impl ImportTarget {
    pub fn source_id(self) -> &'static str {
        match self {
            Self::JpRail => "jp-rail",
            Self::JpPostal => "jp-postal",
            Self::JpSchoolCodes => "jp-school-codes",
            Self::JpSchoolGeodata => "jp-school-geodata",
        }
    }

    pub fn default_parser_version(self) -> &'static str {
        match self {
            Self::JpRail => JP_RAIL_PARSER_VERSION,
            Self::JpPostal => JP_POSTAL_PARSER_VERSION,
            Self::JpSchoolCodes => SCHOOL_CODES_PARSER_VERSION,
            Self::JpSchoolGeodata => SCHOOL_GEODATA_PARSER_VERSION,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommandSummary {
    pub label: String,
    pub import_run_id: Option<i64>,
    pub row_count: i64,
    pub report_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotRefreshSummary {
    pub refreshed_school_rows: i64,
    pub refreshed_area_rows: i64,
    pub invalidated_cache_keys: usize,
    pub projection_indexed_documents: i64,
    pub projection_deleted_documents: i64,
    pub search_execute_school_signal_weight: f64,
    pub search_execute_area_signal_weight: f64,
}

pub async fn run_import_command(
    settings: &AppSettings,
    target: ImportTarget,
    manifest_path: impl AsRef<Path>,
) -> Result<CommandSummary> {
    let manifest_path = fs::canonicalize(manifest_path.as_ref()).with_context(|| {
        format!(
            "failed to resolve source manifest {}",
            manifest_path.as_ref().display()
        )
    })?;
    let manifest = load_manifest(&manifest_path)?;
    ensure!(
        manifest.source_id == target.source_id(),
        "manifest source_id {} does not match requested target {}",
        manifest.source_id,
        target.source_id()
    );
    let parser_version = manifest.effective_parser_version(target.default_parser_version());
    let import_run_id = begin_import_run(
        &settings.database_url,
        &manifest_path,
        &manifest,
        &parser_version,
    )
    .await?;

    let result: Result<CommandSummary> = async {
        let prepared_files = stage_raw_files(&manifest_path, &manifest, &settings.raw_storage_dir)?;
        register_staged_files(settings, import_run_id, &prepared_files).await?;

        let summary = match target {
            ImportTarget::JpRail => {
                let records = parse_rail_stations(&prepared_files)?;
                update_file_row_counts(
                    settings,
                    import_run_id,
                    &prepared_files,
                    &[("rail_stations", records.len() as i64)],
                )
                .await?;
                import_jp_rail(&settings.database_url, &records).await?
            }
            ImportTarget::JpPostal => {
                let records = parse_postal_codes(&prepared_files)?;
                update_file_row_counts(
                    settings,
                    import_run_id,
                    &prepared_files,
                    &[("postal_codes", records.len() as i64)],
                )
                .await?;
                import_jp_postal(&settings.database_url, &records).await?
            }
            ImportTarget::JpSchoolCodes => {
                let records = parse_school_codes(&prepared_files)?;
                update_file_row_counts(
                    settings,
                    import_run_id,
                    &prepared_files,
                    &[("school_codes", records.len() as i64)],
                )
                .await?;
                import_jp_school_codes(&settings.database_url, &records).await?
            }
            ImportTarget::JpSchoolGeodata => {
                let records = parse_school_geodata(&prepared_files)?;
                update_file_row_counts(
                    settings,
                    import_run_id,
                    &prepared_files,
                    &[("school_geodata", records.len() as i64)],
                )
                .await?;
                import_jp_school_geodata(&settings.database_url, &records).await?
            }
        };

        persist_success_reports(
            &settings.database_url,
            import_run_id,
            &parser_version,
            &summary,
        )
        .await?;
        finish_import_run(
            &settings.database_url,
            import_run_id,
            "succeeded",
            summary.normalized_rows,
        )
        .await?;

        Ok(CommandSummary {
            label: target.source_id().to_string(),
            import_run_id: Some(import_run_id),
            row_count: summary.normalized_rows,
            report_count: summary.report_entries.len() + 1,
        })
    }
    .await;

    match result {
        Ok(summary) => Ok(summary),
        Err(error) => {
            let _ = record_import_report(
                &settings.database_url,
                import_run_id,
                &ImportReportEntry {
                    level: "error".to_string(),
                    code: "import_failed".to_string(),
                    message: error.to_string(),
                    row_count: None,
                    details: json!({
                        "source_id": target.source_id(),
                        "manifest_path": manifest_path.display().to_string()
                    }),
                },
            )
            .await;
            let _ = finish_import_run(&settings.database_url, import_run_id, "failed", 0).await;
            Err(error)
        }
    }
}

pub async fn run_derive_school_station_links(settings: &AppSettings) -> Result<CommandSummary> {
    let summary = derive_school_station_links(&settings.database_url).await?;
    Ok(CommandSummary {
        label: "derive-school-station-links".to_string(),
        import_run_id: None,
        row_count: summary.link_rows,
        report_count: summary.report_entries.len(),
    })
}

pub async fn run_snapshot_refresh(settings: &AppSettings) -> Result<SnapshotRefreshSummary> {
    let profiles = RankingProfiles::load_from_dir(&settings.ranking_config_dir)?;
    let tuning = SnapshotTuning {
        search_execute_school_signal_weight: profiles.tracking.search_execute_school_signal_weight,
        search_execute_area_signal_weight: profiles.tracking.search_execute_area_signal_weight,
    };
    let repository = storage_postgres::PgRepository::new(settings.database_url.clone());
    let snapshot_stats = repository.refresh_popularity_snapshots(tuning).await?;

    let invalidated_cache_keys = RecommendationCache::new(
        settings.redis_url.clone(),
        settings.recommendation_cache_ttl_secs,
    )
    .invalidate_recommendations()
    .await?;

    let (projection_indexed_documents, projection_deleted_documents) =
        if settings.candidate_retrieval_mode.is_full() {
            let summary =
                ProjectionSyncService::new(settings.database_url.clone(), &settings.opensearch)?
                    .sync_projection_once()
                    .await?;
            (summary.indexed_documents, summary.deleted_documents)
        } else {
            (0, 0)
        };

    Ok(SnapshotRefreshSummary {
        refreshed_school_rows: snapshot_stats.refreshed_rows,
        refreshed_area_rows: snapshot_stats.related_rows,
        invalidated_cache_keys,
        projection_indexed_documents,
        projection_deleted_documents,
        search_execute_school_signal_weight: tuning.search_execute_school_signal_weight,
        search_execute_area_signal_weight: tuning.search_execute_area_signal_weight,
    })
}

pub async fn run_event_csv_import(
    settings: &AppSettings,
    file_path: impl AsRef<Path>,
) -> Result<CommandSummary> {
    let file_path = fs::canonicalize(file_path.as_ref()).with_context(|| {
        format!(
            "failed to resolve event CSV {}",
            file_path.as_ref().display()
        )
    })?;
    let manifest = SourceManifest {
        source_id: EVENT_CSV_SOURCE_ID.to_string(),
        source_name: "Operational event CSV".to_string(),
        manifest_version: 1,
        parser_version: Some(EVENT_CSV_PARSER_VERSION.to_string()),
        description: Some("Direct CSV import for placement-aware events.".to_string()),
        files: vec![SourceFileSpec {
            logical_name: "events".to_string(),
            path: file_path.display().to_string(),
            format: "csv".to_string(),
        }],
    };
    let import_run_id = begin_import_run(
        &settings.database_url,
        &file_path,
        &manifest,
        EVENT_CSV_PARSER_VERSION,
    )
    .await?;

    let result: Result<CommandSummary> = async {
        let prepared_file = stage_single_csv_file(
            EVENT_CSV_SOURCE_ID,
            "events",
            &file_path,
            &settings.raw_storage_dir,
        )?;
        register_staged_files(
            settings,
            import_run_id,
            std::slice::from_ref(&prepared_file),
        )
        .await?;
        let row_count = count_csv_rows(&prepared_file)?;
        update_file_row_counts(
            settings,
            import_run_id,
            std::slice::from_ref(&prepared_file),
            &[("events", row_count)],
        )
        .await?;
        let records = read_csv_rows::<EventCsvRecord>(&prepared_file)?;
        validate_event_csv_records(&records)?;
        let summary =
            import_event_csv(&settings.database_url, EVENT_CSV_SOURCE_ID, &records).await?;

        persist_success_reports(
            &settings.database_url,
            import_run_id,
            EVENT_CSV_PARSER_VERSION,
            &summary,
        )
        .await?;
        finish_import_run(
            &settings.database_url,
            import_run_id,
            "succeeded",
            summary.normalized_rows,
        )
        .await?;

        Ok(CommandSummary {
            label: EVENT_CSV_SOURCE_ID.to_string(),
            import_run_id: Some(import_run_id),
            row_count: summary.core_rows,
            report_count: summary.report_entries.len() + 1,
        })
    }
    .await;

    match result {
        Ok(summary) => Ok(summary),
        Err(error) => {
            let _ = record_import_report(
                &settings.database_url,
                import_run_id,
                &ImportReportEntry {
                    level: "error".to_string(),
                    code: "event_csv_import_failed".to_string(),
                    message: error.to_string(),
                    row_count: None,
                    details: json!({
                        "source_id": EVENT_CSV_SOURCE_ID,
                        "file_path": file_path.display().to_string()
                    }),
                },
            )
            .await;
            let _ = finish_import_run(&settings.database_url, import_run_id, "failed", 0).await;
            Err(error)
        }
    }
}

pub fn generate_demo_jp_fixture(output_dir: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;

    let files = vec![
        (
            output_dir.join("jp_school_codes.csv"),
            "school_code,name,prefecture_name,city_name,school_type\n13101A,Minato Science High,Tokyo,Minato,high_school\n13101B,Harbor Commerce High,Tokyo,Minato,high_school\n13103A,Shinagawa Technical College,Tokyo,Shinagawa,college\n",
        ),
        (
            output_dir.join("jp_school_geodata.csv"),
            "school_code,name,prefecture_name,city_name,address,school_type,latitude,longitude\n13101A,Minato Science High,Tokyo,Minato,芝浦1-1-1,high_school,35.6412,139.7487\n13101B,Harbor Commerce High,Tokyo,Minato,海岸1-2-3,high_school,35.6376,139.7604\n13103A,Shinagawa Technical College,Tokyo,Shinagawa,港南2-16-1,college,35.6289,139.7393\n",
        ),
        (
            output_dir.join("jp_rail_stations.csv"),
            "station_code,station_name,line_name,prefecture_name,latitude,longitude\n1130217,Tamachi,JR Yamanote Line,Tokyo,35.6456,139.7476\n1130218,Shinagawa,JR Yamanote Line,Tokyo,35.6285,139.7388\n1130104,Shimbashi,JR Yamanote Line,Tokyo,35.6663,139.7587\n",
        ),
        (
            output_dir.join("jp_postal_codes.csv"),
            "postal_code,prefecture_name,city_name,town_name\n1080023,Tokyo,Minato,Shibaura\n1050022,Tokyo,Minato,Kaigan\n1080075,Tokyo,Minato,Konan\n",
        ),
    ];

    let mut written = Vec::new();
    for (path, contents) in files {
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        written.push(path);
    }
    Ok(written)
}

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

async fn register_staged_files(
    settings: &AppSettings,
    import_run_id: i64,
    prepared_files: &[PreparedSourceFile],
) -> Result<()> {
    for file in prepared_files {
        upsert_import_run_file(
            &settings.database_url,
            &ImportRunFileAudit {
                import_run_id,
                logical_name: file.logical_name.clone(),
                staged_path: file.staged_path.display().to_string(),
                checksum_sha256: file.checksum_sha256.clone(),
                size_bytes: file.size_bytes as i64,
                row_count: None,
                status: "staged".to_string(),
            },
        )
        .await?;
    }
    Ok(())
}

async fn update_file_row_counts(
    settings: &AppSettings,
    import_run_id: i64,
    prepared_files: &[PreparedSourceFile],
    row_counts: &[(&str, i64)],
) -> Result<()> {
    for file in prepared_files {
        let row_count = row_counts.iter().find_map(|(logical_name, count)| {
            (*logical_name == file.logical_name).then_some(*count)
        });
        upsert_import_run_file(
            &settings.database_url,
            &ImportRunFileAudit {
                import_run_id,
                logical_name: file.logical_name.clone(),
                staged_path: file.staged_path.display().to_string(),
                checksum_sha256: file.checksum_sha256.clone(),
                size_bytes: file.size_bytes as i64,
                row_count,
                status: "imported".to_string(),
            },
        )
        .await?;
    }
    Ok(())
}

async fn persist_success_reports(
    database_url: &str,
    import_run_id: i64,
    parser_version: &str,
    summary: &ImportSummary,
) -> Result<()> {
    record_import_report(
        database_url,
        import_run_id,
        &ImportReportEntry {
            level: "info".to_string(),
            code: "parser_version".to_string(),
            message: "Recorded parser version for this import run.".to_string(),
            row_count: None,
            details: json!({ "parser_version": parser_version }),
        },
    )
    .await?;

    for report in &summary.report_entries {
        record_import_report(database_url, import_run_id, report).await?;
    }
    Ok(())
}

fn validate_event_csv_records(records: &[EventCsvRecord]) -> Result<()> {
    let mut seen_event_ids = std::collections::BTreeSet::new();
    for record in records {
        ensure!(
            !record.event_id.trim().is_empty(),
            "event_id must not be empty in event CSV"
        );
        ensure!(
            !record.school_id.trim().is_empty(),
            "school_id must not be empty in event CSV"
        );
        ensure!(
            !record.title.trim().is_empty(),
            "title must not be empty in event CSV"
        );
        ensure!(
            seen_event_ids.insert(record.event_id.clone()),
            "duplicate event_id {} in event CSV",
            record.event_id
        );
        if let Some(starts_at) = record.starts_at.as_deref() {
            validate_starts_at(starts_at)?;
        }
        let _ = record.normalized_placement_tags()?;
    }
    Ok(())
}

fn validate_starts_at(raw: &str) -> Result<()> {
    let value = raw.trim();
    if value.is_empty() {
        return Ok(());
    }

    let is_valid = NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
        || DateTime::<FixedOffset>::parse_from_rfc3339(value).is_ok();
    ensure!(
        is_valid,
        "starts_at must be ISO-8601 date (YYYY-MM-DD) or RFC3339 timestamp, got {}",
        raw
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use storage_postgres::EventCsvRecord;

    use super::{generate_demo_jp_fixture, validate_event_csv_records};

    #[test]
    fn writes_demo_fixture_files() {
        let temp = tempfile::tempdir().expect("tempdir");
        let written = generate_demo_jp_fixture(temp.path()).expect("fixture generation");
        assert_eq!(written.len(), 4);
        assert!(written.iter().all(|path| path.exists()));
    }

    #[test]
    fn event_csv_accepts_date_or_rfc3339_starts_at() {
        let records = vec![
            EventCsvRecord {
                event_id: "event-date".to_string(),
                school_id: "school-a".to_string(),
                title: "Date Event".to_string(),
                event_category: "open_campus".to_string(),
                is_open_day: true,
                is_featured: false,
                priority_weight: 0.0,
                starts_at: Some("2026-05-10".to_string()),
                placement_tags: "home".to_string(),
            },
            EventCsvRecord {
                event_id: "event-rfc3339".to_string(),
                school_id: "school-a".to_string(),
                title: "Timestamp Event".to_string(),
                event_category: "open_campus".to_string(),
                is_open_day: true,
                is_featured: false,
                priority_weight: 0.0,
                starts_at: Some("2026-05-10T10:00:00+09:00".to_string()),
                placement_tags: "detail".to_string(),
            },
        ];

        validate_event_csv_records(&records).expect("valid starts_at formats");
    }

    #[test]
    fn event_csv_rejects_non_iso_starts_at() {
        let records = vec![EventCsvRecord {
            event_id: "event-invalid".to_string(),
            school_id: "school-a".to_string(),
            title: "Bad Event".to_string(),
            event_category: "open_campus".to_string(),
            is_open_day: true,
            is_featured: false,
            priority_weight: 0.0,
            starts_at: Some("05/10/2026 10:00".to_string()),
            placement_tags: "home".to_string(),
        }];

        let error = validate_event_csv_records(&records).expect_err("invalid starts_at");
        assert!(error
            .to_string()
            .contains("starts_at must be ISO-8601 date"));
    }
}
