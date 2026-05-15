use std::{fs, path::Path};

use anyhow::{ensure, Context, Result};
use chrono::{DateTime, FixedOffset, NaiveDate};
use config::{
    load_and_lint_profile_pack_file, AppSettings, ProfileConnectorFieldMapping,
    ProfileConnectorRegistryEntry, ProfileConnectorType, ProfilePackRegistry, DEFAULT_PROFILE_ID,
};
use generic_csv::{
    count_csv_rows, is_source_id, load_archive_manifest, load_manifest, read_csv_rows,
    stage_raw_files, stage_single_csv_file, stage_single_source_file,
    unpack_loaded_archive_manifest, ArchiveSourceManifest, SourceFileSpec, SourceManifest,
    SourceManifestKind, UnpackedArchiveFile, UnpackedArchiveSource, SOURCE_ID_RULE_DESCRIPTION,
    SOURCE_MANIFEST_SCHEMA_VERSION,
};
use jp_postal::{parse_postal_codes, PARSER_VERSION as JP_POSTAL_PARSER_VERSION};
use jp_rail::{parse_rail_stations, PARSER_VERSION as JP_RAIL_PARSER_VERSION};
use jp_school::{
    parse_school_codes, parse_school_geodata, SCHOOL_CODES_PARSER_VERSION,
    SCHOOL_GEODATA_PARSER_VERSION,
};
use serde::{de, Deserialize};
use serde_json::{json, Value};
use storage_postgres::{
    begin_import_run, derive_school_station_links, finish_import_run, import_event_csv,
    import_event_ndjson, import_jp_postal, import_jp_rail, import_jp_school_codes,
    import_jp_school_geodata, record_import_report, EventCsvRecord, ImportReportEntry,
};

use crate::repository::{persist_success_reports, register_staged_files, update_file_row_counts};

const EVENT_CSV_PARSER_VERSION: &str = "event-csv-v1";
const EVENT_CSV_SOURCE_ID: &str = "event-csv";
const EVENT_NDJSON_PARSER_VERSION: &str = "event-ndjson-v1";
pub const DEFAULT_EVENT_NDJSON_SOURCE_ID: &str = "event-ndjson";
const EVENT_ARCHIVE_PARSER_VERSION: &str = "event-archive-v1";

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

pub async fn run_profile_source_import(
    settings: &AppSettings,
    profiles_path: impl AsRef<Path>,
    profile_id: Option<&str>,
    source_id: &str,
) -> Result<CommandSummary> {
    ensure_import_source_id("profile source", source_id)?;
    let registry = ProfilePackRegistry::new(profiles_path.as_ref());
    let selected_profile_id = registry.selected_profile_id(profile_id, DEFAULT_PROFILE_ID)?;
    let manifest_path = registry.manifest_path_for_profile_id(&selected_profile_id)?;
    let (_manifest, lint_file) = load_and_lint_profile_pack_file(&manifest_path)?;
    let connector = select_profile_source_connector(
        &lint_file.connector_registry,
        &selected_profile_id,
        source_id,
    )?;

    match connector.connector_type {
        ProfileConnectorType::SourceManifest => {
            let source_id = connector.source_id.as_deref().unwrap_or(source_id);
            let target = import_target_for_source_id(source_id)?;
            run_import_command(settings, target, &connector.manifest_path).await
        }
        ProfileConnectorType::CsvImport => {
            ensure_event_v1_mapping(connector)?;
            let source_id = connector.source_id.as_deref().unwrap_or(source_id);
            run_event_csv_import_with_source_id(settings, source_id, &connector.manifest_path).await
        }
        ProfileConnectorType::NdjsonImport => {
            ensure_event_v1_mapping(connector)?;
            let source_id = connector.source_id.as_deref().unwrap_or(source_id);
            run_event_ndjson_import(settings, &connector.manifest_path, source_id).await
        }
        ProfileConnectorType::ArchiveSource => {
            ensure_event_v1_mapping(connector)?;
            let source_id = connector.source_id.as_deref().unwrap_or(source_id);
            run_event_archive_import(settings, source_id, &connector.manifest_path).await
        }
        ProfileConnectorType::CrawlerManifest => {
            anyhow::bail!(
                "profile {} source_id {} points to crawler_manifest {}; use crawler fetch/parse commands for crawler sources",
                selected_profile_id,
                source_id,
                connector.manifest_path.display()
            )
        }
    }
}

pub async fn run_event_csv_import(
    settings: &AppSettings,
    file_path: impl AsRef<Path>,
) -> Result<CommandSummary> {
    run_event_csv_import_with_source_id(settings, EVENT_CSV_SOURCE_ID, file_path).await
}

async fn run_event_csv_import_with_source_id(
    settings: &AppSettings,
    source_id: &str,
    file_path: impl AsRef<Path>,
) -> Result<CommandSummary> {
    ensure_import_source_id("event CSV", source_id)?;
    let file_path = fs::canonicalize(file_path.as_ref()).with_context(|| {
        format!(
            "failed to resolve event CSV {}",
            file_path.as_ref().display()
        )
    })?;
    let manifest = SourceManifest {
        schema_version: SOURCE_MANIFEST_SCHEMA_VERSION,
        kind: SourceManifestKind::ImportSource,
        source_id: source_id.to_string(),
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
        let prepared_file =
            stage_single_csv_file(source_id, "events", &file_path, &settings.raw_storage_dir)?;
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
        let summary = import_event_csv(&settings.database_url, source_id, &records).await?;

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
            label: source_id.to_string(),
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
                        "source_id": source_id,
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

pub async fn run_event_ndjson_import(
    settings: &AppSettings,
    file_path: impl AsRef<Path>,
    source_id: &str,
) -> Result<CommandSummary> {
    ensure_import_source_id("event NDJSON", source_id)?;
    let file_path = fs::canonicalize(file_path.as_ref()).with_context(|| {
        format!(
            "failed to resolve event NDJSON {}",
            file_path.as_ref().display()
        )
    })?;
    let manifest = SourceManifest {
        schema_version: SOURCE_MANIFEST_SCHEMA_VERSION,
        kind: SourceManifestKind::ImportSource,
        source_id: source_id.to_string(),
        source_name: "Operational event NDJSON".to_string(),
        manifest_version: 1,
        parser_version: Some(EVENT_NDJSON_PARSER_VERSION.to_string()),
        description: Some("Direct NDJSON import for placement-aware events.".to_string()),
        files: vec![SourceFileSpec {
            logical_name: "events".to_string(),
            path: file_path.display().to_string(),
            format: "ndjson".to_string(),
        }],
    };
    let import_run_id = begin_import_run(
        &settings.database_url,
        &file_path,
        &manifest,
        EVENT_NDJSON_PARSER_VERSION,
    )
    .await?;

    let result: Result<CommandSummary> = async {
        let prepared_file = stage_single_source_file(
            source_id,
            "events",
            "ndjson",
            &file_path,
            &settings.raw_storage_dir,
        )?;
        register_staged_files(
            settings,
            import_run_id,
            std::slice::from_ref(&prepared_file),
        )
        .await?;
        let records = read_event_ndjson_records(&prepared_file.staged_path)?;
        let row_count = records.len() as i64;
        update_file_row_counts(
            settings,
            import_run_id,
            std::slice::from_ref(&prepared_file),
            &[("events", row_count)],
        )
        .await?;
        validate_event_csv_records(&records)?;
        let summary = import_event_ndjson(&settings.database_url, source_id, &records).await?;

        persist_success_reports(
            &settings.database_url,
            import_run_id,
            EVENT_NDJSON_PARSER_VERSION,
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
            label: source_id.to_string(),
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
                    code: "event_ndjson_import_failed".to_string(),
                    message: error.to_string(),
                    row_count: None,
                    details: json!({
                        "source_id": source_id,
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

async fn run_event_archive_import(
    settings: &AppSettings,
    source_id: &str,
    manifest_path: impl AsRef<Path>,
) -> Result<CommandSummary> {
    ensure_import_source_id("event archive", source_id)?;
    let manifest_path = fs::canonicalize(manifest_path.as_ref()).with_context(|| {
        format!(
            "failed to resolve event archive manifest {}",
            manifest_path.as_ref().display()
        )
    })?;
    let manifest = load_archive_manifest(&manifest_path)?;
    ensure!(
        manifest.source_id == source_id,
        "archive source manifest {} source_id {} does not match requested source_id {}",
        manifest_path.display(),
        manifest.source_id,
        source_id
    );
    let parser_version = manifest.effective_parser_version(EVENT_ARCHIVE_PARSER_VERSION);
    let unpack_dir = tempfile::tempdir().context("failed to create archive unpack tempdir")?;
    let unpacked = unpack_loaded_archive_manifest(&manifest_path, &manifest, unpack_dir.path())?;
    let event_file = select_archive_event_file(&manifest_path, &unpacked)?;
    let audit_manifest = archive_audit_manifest(&manifest, &unpacked, &parser_version);
    let import_run_id = begin_import_run(
        &settings.database_url,
        &manifest_path,
        &audit_manifest,
        &parser_version,
    )
    .await?;

    let result: Result<CommandSummary> = async {
        record_archive_evidence(
            &settings.database_url,
            import_run_id,
            &manifest_path,
            &manifest,
            &unpacked,
        )
        .await?;
        let prepared_file = stage_single_source_file(
            source_id,
            &event_file.logical_name,
            &event_file.format,
            &event_file.source_path,
            &settings.raw_storage_dir,
        )?;
        register_staged_files(
            settings,
            import_run_id,
            std::slice::from_ref(&prepared_file),
        )
        .await?;

        let summary = match event_file.format.as_str() {
            "csv" => {
                let row_count = count_csv_rows(&prepared_file)?;
                update_file_row_counts(
                    settings,
                    import_run_id,
                    std::slice::from_ref(&prepared_file),
                    &[(&event_file.logical_name, row_count)],
                )
                .await?;
                let records = read_csv_rows::<EventCsvRecord>(&prepared_file)?;
                validate_event_csv_records(&records)?;
                import_event_csv(&settings.database_url, source_id, &records).await?
            }
            "ndjson" => {
                let records = read_event_ndjson_records(&prepared_file.staged_path)?;
                let row_count = records.len() as i64;
                update_file_row_counts(
                    settings,
                    import_run_id,
                    std::slice::from_ref(&prepared_file),
                    &[(&event_file.logical_name, row_count)],
                )
                .await?;
                validate_event_csv_records(&records)?;
                import_event_ndjson(&settings.database_url, source_id, &records).await?
            }
            other => anyhow::bail!(
                "archive source {} file {} uses unsupported runtime format {}; expected csv or ndjson",
                source_id,
                event_file.logical_name,
                other
            ),
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
            label: source_id.to_string(),
            import_run_id: Some(import_run_id),
            row_count: summary.core_rows,
            report_count: summary.report_entries.len() + 2,
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
                    code: "event_archive_import_failed".to_string(),
                    message: error.to_string(),
                    row_count: None,
                    details: json!({
                        "source_id": source_id,
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

async fn record_archive_evidence(
    database_url: &str,
    import_run_id: i64,
    manifest_path: &Path,
    manifest: &ArchiveSourceManifest,
    unpacked: &UnpackedArchiveSource,
) -> Result<()> {
    record_import_report(
        database_url,
        import_run_id,
        &ImportReportEntry {
            level: "info".to_string(),
            code: "archive_source_evidence".to_string(),
            message: "Recorded archive source manifest and checksum evidence.".to_string(),
            row_count: None,
            details: json!({
                "manifest_path": manifest_path.display().to_string(),
                "archive_path": unpacked.archive_path.display().to_string(),
                "archive_format": unpacked.archive_format.as_str(),
                "archive_checksum_sha256": &unpacked.archive_checksum_sha256,
                "archive_size_bytes": unpacked.archive_size_bytes,
                "files": manifest.files.iter().map(|file| json!({
                    "logical_name": &file.logical_name,
                    "path": &file.path,
                    "format": &file.format
                })).collect::<Vec<_>>()
            }),
        },
    )
    .await
}

fn select_archive_event_file<'a>(
    manifest_path: &Path,
    unpacked: &'a UnpackedArchiveSource,
) -> Result<&'a UnpackedArchiveFile> {
    ensure!(
        unpacked.files.len() == 1,
        "archive source manifest {} has {} files; current event_v1 archive import runtime supports exactly one CSV or NDJSON file",
        manifest_path.display(),
        unpacked.files.len()
    );
    let event_file = &unpacked.files[0];
    ensure!(
        event_file.logical_name == "events",
        "archive source manifest {} file logical_name {} is unsupported by event_v1 archive import; expected events",
        manifest_path.display(),
        event_file.logical_name
    );
    ensure!(
        matches!(event_file.format.as_str(), "csv" | "ndjson"),
        "archive source manifest {} file {} uses unsupported runtime format {}; expected csv or ndjson",
        manifest_path.display(),
        event_file.logical_name,
        event_file.format
    );
    Ok(event_file)
}

fn archive_audit_manifest(
    manifest: &ArchiveSourceManifest,
    unpacked: &UnpackedArchiveSource,
    parser_version: &str,
) -> SourceManifest {
    SourceManifest {
        schema_version: SOURCE_MANIFEST_SCHEMA_VERSION,
        kind: SourceManifestKind::ImportSource,
        source_id: manifest.source_id.clone(),
        source_name: manifest.source_name.clone(),
        manifest_version: manifest.manifest_version,
        parser_version: Some(parser_version.to_string()),
        description: Some(format!(
            "Archive source import; archive_format={}, archive_checksum_sha256={}",
            unpacked.archive_format.as_str(),
            unpacked.archive_checksum_sha256
        )),
        files: unpacked
            .files
            .iter()
            .map(|file| SourceFileSpec {
                logical_name: file.logical_name.clone(),
                path: file.archive_entry_path.clone(),
                format: file.format.clone(),
            })
            .collect(),
    }
}

fn select_profile_source_connector<'a>(
    connectors: &'a [ProfileConnectorRegistryEntry],
    profile_id: &str,
    source_id: &str,
) -> Result<&'a ProfileConnectorRegistryEntry> {
    let matches = connectors
        .iter()
        .filter(|connector| connector.source_id.as_deref() == Some(source_id))
        .collect::<Vec<_>>();
    ensure!(
        !matches.is_empty(),
        "profile {} does not declare a connector with source_id {}",
        profile_id,
        source_id
    );
    ensure!(
        matches.len() == 1,
        "profile {} declares multiple connectors with source_id {}",
        profile_id,
        source_id
    );
    Ok(matches[0])
}

fn import_target_for_source_id(source_id: &str) -> Result<ImportTarget> {
    match source_id {
        "jp-rail" => Ok(ImportTarget::JpRail),
        "jp-postal" => Ok(ImportTarget::JpPostal),
        "jp-school-codes" => Ok(ImportTarget::JpSchoolCodes),
        "jp-school-geodata" => Ok(ImportTarget::JpSchoolGeodata),
        other => anyhow::bail!(
            "profile source_id {} uses source_manifest but has no CLI importer mapping",
            other
        ),
    }
}

fn ensure_event_v1_mapping(connector: &ProfileConnectorRegistryEntry) -> Result<()> {
    ensure!(
        matches!(
            connector.field_mapping.as_ref(),
            Some(ProfileConnectorFieldMapping::EventV1)
        ),
        "profile source_id {} connector {} must use field_mapping event_v1",
        connector.source_id.as_deref().unwrap_or("unknown"),
        connector.connector_type.as_str()
    );
    Ok(())
}

fn ensure_import_source_id(label: &str, source_id: &str) -> Result<()> {
    ensure!(
        is_source_id(source_id),
        "{label} source_id '{}' is invalid; {}",
        source_id,
        SOURCE_ID_RULE_DESCRIPTION
    );
    Ok(())
}

fn read_event_ndjson_records(path: &Path) -> Result<Vec<EventCsvRecord>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read event NDJSON {}", path.display()))?;
    let mut records = Vec::new();
    for (index, line) in raw.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let record: EventNdjsonRecord = serde_json::from_str(line).with_context(|| {
            format!(
                "failed to parse event NDJSON {} line {}",
                path.display(),
                index + 1
            )
        })?;
        records.push(record.into());
    }
    ensure!(
        !records.is_empty(),
        "event NDJSON {} did not contain any records",
        path.display()
    );
    Ok(records)
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
        ensure!(
            record.details.is_object(),
            "details must be a JSON object in event import records"
        );
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct EventNdjsonRecord {
    event_id: String,
    school_id: String,
    title: String,
    #[serde(default = "default_event_category")]
    event_category: String,
    #[serde(default)]
    is_open_day: bool,
    #[serde(default)]
    is_featured: bool,
    #[serde(default)]
    priority_weight: f64,
    #[serde(default)]
    starts_at: Option<String>,
    #[serde(default, deserialize_with = "deserialize_ndjson_placement_tags")]
    placement_tags: String,
    #[serde(
        default = "default_event_details",
        deserialize_with = "deserialize_ndjson_event_details"
    )]
    details: Value,
}

impl From<EventNdjsonRecord> for EventCsvRecord {
    fn from(record: EventNdjsonRecord) -> Self {
        Self {
            event_id: record.event_id,
            school_id: record.school_id,
            title: record.title,
            event_category: record.event_category,
            is_open_day: record.is_open_day,
            is_featured: record.is_featured,
            priority_weight: record.priority_weight,
            starts_at: record.starts_at,
            placement_tags: record.placement_tags,
            details: record.details,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PlacementTagValue {
    PipeDelimited(String),
    List(Vec<String>),
}

fn deserialize_ndjson_placement_tags<'de, D>(
    deserializer: D,
) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(value) = Option::<PlacementTagValue>::deserialize(deserializer)? else {
        return Ok(String::new());
    };
    Ok(match value {
        PlacementTagValue::PipeDelimited(value) => value,
        PlacementTagValue::List(values) => values.join("|"),
    })
}

fn deserialize_ndjson_event_details<'de, D>(deserializer: D) -> std::result::Result<Value, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(value) = Option::<Value>::deserialize(deserializer)? else {
        return Ok(default_event_details());
    };
    if value.is_object() {
        Ok(value)
    } else {
        Err(de::Error::custom("details must be a JSON object"))
    }
}

fn default_event_category() -> String {
    "general".to_string()
}

fn default_event_details() -> Value {
    Value::Object(Default::default())
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
    use std::{fs, path::PathBuf};

    use config::{
        ProfileCompatibilityLevel, ProfileConnectorFieldMapping, ProfileConnectorRegistryEntry,
        ProfileConnectorSafetyMetadata, ProfileConnectorType, ProfileSourceClass,
    };
    use generic_csv::{
        ArchiveEntrySpec, ArchiveFileSpec, ArchiveFormat, ArchiveSourceManifest,
        ArchiveSourceManifestKind, UnpackedArchiveFile, UnpackedArchiveSource,
    };
    use serde_json::json;
    use storage_postgres::EventCsvRecord;

    use super::{
        archive_audit_manifest, ensure_event_v1_mapping, ensure_import_source_id,
        read_event_ndjson_records, select_archive_event_file, validate_event_csv_records,
    };

    #[test]
    fn import_source_id_rejects_path_like_values() {
        let error = ensure_import_source_id("event NDJSON", "../bad-source")
            .expect_err("invalid source_id");

        assert!(error
            .to_string()
            .contains("event NDJSON source_id '../bad-source' is invalid"));
    }

    #[test]
    fn profile_import_rejects_non_event_v1_field_mapping() {
        let connector = ProfileConnectorRegistryEntry {
            connector_type: ProfileConnectorType::CsvImport,
            source_class: ProfileSourceClass::CsvImport,
            manifest_path: PathBuf::from("events.csv"),
            manifest_kind: "csv_file".to_string(),
            source_id: Some("example-events".to_string()),
            field_mapping: Some(ProfileConnectorFieldMapping::Custom(
                "custom_event_v1".to_string(),
            )),
            profile_compatibility: ProfileCompatibilityLevel::Experimental,
            safety: ProfileConnectorSafetyMetadata {
                local_reference_only: true,
                dynamic_loading_enabled: false,
                live_fetch_default: false,
                allowlist_required: false,
            },
        };

        let error = ensure_event_v1_mapping(&connector).expect_err("unsupported mapping");

        assert!(error.to_string().contains(
            "profile source_id example-events connector csv_import must use field_mapping event_v1"
        ));
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
                details: json!({}),
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
                details: json!({}),
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
            details: json!({}),
        }];

        let error = validate_event_csv_records(&records).expect_err("invalid starts_at");
        assert!(error
            .to_string()
            .contains("starts_at must be ISO-8601 date"));
    }

    #[test]
    fn event_ndjson_accepts_event_v1_mapping() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("events.ndjson");
        fs::write(
            &path,
            r#"{"event_id":"event-array","school_id":"school-a","title":"Array Tags","placement_tags":["home","detail"],"details":{"detail_url":"https://example.com/detail"}}
{"event_id":"event-pipe","school_id":"school-a","title":"Pipe Tags","placement_tags":"search|mypage","starts_at":"2026-05-10"}
"#,
        )
        .expect("ndjson");

        let records = read_event_ndjson_records(&path).expect("records");

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].placement_tags, "home|detail");
        assert_eq!(
            records[0].details["detail_url"],
            "https://example.com/detail"
        );
        assert_eq!(records[1].event_category, "general");
        validate_event_csv_records(&records).expect("valid event records");
    }

    #[test]
    fn event_ndjson_rejects_non_object_details() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("events.ndjson");
        fs::write(
            &path,
            r#"{"event_id":"event-bad-details","school_id":"school-a","title":"Bad Details","details":"not-an-object"}
"#,
        )
        .expect("ndjson");

        let error = read_event_ndjson_records(&path).expect_err("details object");

        assert!(format!("{error:#}").contains("details must be a JSON object"));
        assert!(format!("{error:#}").contains("line 1"));
    }

    #[test]
    fn event_ndjson_treats_null_details_as_empty_object() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("events.ndjson");
        fs::write(
            &path,
            r#"{"event_id":"event-null-details","school_id":"school-a","title":"Null Details","details":null}
"#,
        )
        .expect("ndjson");

        let records = read_event_ndjson_records(&path).expect("records");

        assert!(records[0].details.is_object());
        validate_event_csv_records(&records).expect("valid event records");
    }

    #[test]
    fn archive_event_runtime_accepts_single_events_csv_or_ndjson_file() {
        let unpacked = UnpackedArchiveSource {
            archive_path: PathBuf::from("events.tar"),
            archive_format: ArchiveFormat::Tar,
            archive_checksum_sha256:
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            archive_size_bytes: 512,
            files: vec![UnpackedArchiveFile {
                logical_name: "events".to_string(),
                format: "csv".to_string(),
                archive_entry_path: "events.csv".to_string(),
                source_path: PathBuf::from("events.csv"),
            }],
        };

        let file = select_archive_event_file(&PathBuf::from("events.archive.yaml"), &unpacked)
            .expect("archive event file");

        assert_eq!(file.logical_name, "events");
        assert_eq!(file.format, "csv");
    }

    #[test]
    fn archive_event_runtime_rejects_multiple_files() {
        let unpacked = UnpackedArchiveSource {
            archive_path: PathBuf::from("events.tar"),
            archive_format: ArchiveFormat::Tar,
            archive_checksum_sha256:
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            archive_size_bytes: 512,
            files: vec![
                UnpackedArchiveFile {
                    logical_name: "events".to_string(),
                    format: "csv".to_string(),
                    archive_entry_path: "events.csv".to_string(),
                    source_path: PathBuf::from("events.csv"),
                },
                UnpackedArchiveFile {
                    logical_name: "extra".to_string(),
                    format: "csv".to_string(),
                    archive_entry_path: "extra.csv".to_string(),
                    source_path: PathBuf::from("extra.csv"),
                },
            ],
        };

        let error = select_archive_event_file(&PathBuf::from("events.archive.yaml"), &unpacked)
            .expect_err("multiple files");

        assert!(format!("{error:#}").contains("supports exactly one CSV or NDJSON file"));
    }

    #[test]
    fn archive_audit_manifest_keeps_archive_entry_paths() {
        let manifest = ArchiveSourceManifest {
            schema_version: 1,
            kind: ArchiveSourceManifestKind::ArchiveSource,
            source_id: "event-archive".to_string(),
            source_name: "Event archive".to_string(),
            manifest_version: 1,
            parser_version: None,
            description: None,
            archive: ArchiveFileSpec {
                path: "events.tar".to_string(),
                format: ArchiveFormat::Tar,
                checksum_sha256: Some(
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                ),
            },
            files: vec![ArchiveEntrySpec {
                logical_name: "events".to_string(),
                path: "events.csv".to_string(),
                format: "csv".to_string(),
            }],
        };
        let unpacked = UnpackedArchiveSource {
            archive_path: PathBuf::from("events.tar"),
            archive_format: ArchiveFormat::Tar,
            archive_checksum_sha256:
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            archive_size_bytes: 512,
            files: vec![UnpackedArchiveFile {
                logical_name: "events".to_string(),
                format: "csv".to_string(),
                archive_entry_path: "events.csv".to_string(),
                source_path: PathBuf::from("/tmp/archive-unpack/events/events.csv"),
            }],
        };

        let audit_manifest = archive_audit_manifest(&manifest, &unpacked, "archive-event-v1");

        assert_eq!(audit_manifest.files[0].path, "events.csv");
    }
}
