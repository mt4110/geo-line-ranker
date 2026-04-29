use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use api_contracts::{FallbackStageDto, RecommendationRequest, RecommendationResponse};
use cache::RecommendationCache;
use chrono::{DateTime, FixedOffset, NaiveDate};
use config::{is_profile_id, AppSettings, RankingProfiles, PROFILE_ID_RULE_DESCRIPTION};
use csv::Reader;
use generic_csv::{
    count_csv_rows, load_manifest, read_csv_rows, stage_raw_files, stage_single_csv_file,
    PreparedSourceFile, SourceFileSpec, SourceManifest, SourceManifestKind,
    SOURCE_MANIFEST_SCHEMA_VERSION,
};
use jp_postal::{parse_postal_codes, PARSER_VERSION as JP_POSTAL_PARSER_VERSION};
use jp_rail::{parse_rail_stations, PARSER_VERSION as JP_RAIL_PARSER_VERSION};
use jp_school::{
    parse_school_codes, parse_school_geodata, SCHOOL_CODES_PARSER_VERSION,
    SCHOOL_GEODATA_PARSER_VERSION,
};
use ranking::RankingEngine;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use storage::{JobType, NewJob, RecommendationRepository, SnapshotTuning};
use storage_opensearch::ProjectionSyncService;
use storage_postgres::{
    begin_import_run, derive_school_station_links, finish_import_run, import_event_csv,
    import_jp_postal, import_jp_rail, import_jp_school_codes, import_jp_school_geodata,
    record_import_report, upsert_import_run_file, EventCsvRecord, ImportReportEntry,
    ImportRunFileAudit, ImportSummary, JobInspection, JobMutationSummary, JobQueueSnapshot,
    PgRepository, RecommendationTraceReplayRow,
};

const EVENT_CSV_PARSER_VERSION: &str = "event-csv-v1";
const EVENT_CSV_SOURCE_ID: &str = "event-csv";
const FIXTURE_SET_SCHEMA_VERSION: u32 = 1;

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

#[derive(Debug, Clone, PartialEq)]
pub struct JobEnqueueSummary {
    pub job_id: i64,
    pub job_type: JobType,
    pub payload: serde_json::Value,
    pub max_attempts: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayEvaluationSummary {
    pub evaluated: usize,
    pub matched: usize,
    pub mismatched: usize,
    pub failed: usize,
    pub cases: Vec<ReplayEvaluationCase>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayEvaluationCase {
    pub trace_id: i64,
    pub status: ReplayEvaluationStatus,
    pub request_id: Option<String>,
    pub expected_fallback_stage: Option<String>,
    pub actual_fallback_stage: Option<String>,
    pub expected_order: Vec<String>,
    pub actual_order: Vec<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FixtureManifestKind {
    FixtureSet,
}

impl FixtureManifestKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FixtureSet => "fixture_set",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FixtureSetManifest {
    pub schema_version: u32,
    pub kind: FixtureManifestKind,
    pub manifest_version: u32,
    pub fixture_set_id: String,
    #[serde(default)]
    pub profile_id: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    pub files: Vec<FixtureFileManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FixtureFileManifest {
    pub logical_name: String,
    pub path: String,
    pub format: String,
    pub checksum_sha256: String,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixtureDoctorSummary {
    pub manifest_path: PathBuf,
    pub fixture_set_id: String,
    pub profile_id: Option<String>,
    pub manifest_version: u32,
    pub files: Vec<FixtureDoctorFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixtureDoctorFile {
    pub logical_name: String,
    pub path: PathBuf,
    pub format: String,
    pub checksum_sha256: String,
    pub row_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayEvaluationStatus {
    Matched,
    Mismatched,
    Failed,
}

impl ReplayEvaluationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Matched => "matched",
            Self::Mismatched => "mismatched",
            Self::Failed => "failed",
        }
    }
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
    let repository = pg_repository(settings)?;
    let snapshot_stats = repository.refresh_popularity_snapshots(tuning).await?;

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

    let invalidated_cache_keys = RecommendationCache::new(
        settings.redis_url.clone(),
        settings.recommendation_cache_ttl_secs,
    )
    .invalidate_recommendations()
    .await?;

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

pub async fn run_job_list(settings: &AppSettings, limit: i64) -> Result<JobQueueSnapshot> {
    pg_repository(settings)?.list_jobs(limit).await
}

pub async fn run_job_inspect(settings: &AppSettings, job_id: i64) -> Result<JobInspection> {
    pg_repository(settings)?.inspect_job(job_id).await
}

pub async fn run_job_retry(settings: &AppSettings, job_id: i64) -> Result<JobMutationSummary> {
    pg_repository(settings)?.retry_failed_job(job_id).await
}

pub async fn run_job_due(settings: &AppSettings, job_id: i64) -> Result<JobMutationSummary> {
    pg_repository(settings)?.make_queued_job_due(job_id).await
}

pub async fn run_job_enqueue(
    settings: &AppSettings,
    job_type: &str,
    payload: &str,
    max_attempts: i32,
) -> Result<JobEnqueueSummary> {
    ensure!(max_attempts > 0, "max_attempts must be positive");
    let job_type =
        JobType::parse(job_type).with_context(|| format!("unsupported job_type {job_type}"))?;
    let payload: serde_json::Value =
        serde_json::from_str(payload).with_context(|| "failed to parse job payload JSON")?;
    ensure!(payload.is_object(), "job payload must be a JSON object");

    let repository = pg_repository(settings)?;
    let job_id = repository
        .enqueue_job(&NewJob {
            job_type,
            payload: payload.clone(),
            max_attempts,
        })
        .await?;

    Ok(JobEnqueueSummary {
        job_id,
        job_type,
        payload,
        max_attempts,
    })
}

pub async fn run_replay_evaluate(
    settings: &AppSettings,
    limit: i64,
) -> Result<ReplayEvaluationSummary> {
    let profiles = RankingProfiles::load_from_dir(&settings.ranking_config_dir)?;
    let neighbor_distance_cap_meters = profiles.fallback.neighbor_distance_cap_meters;
    let engine = RankingEngine::new(profiles, settings.algorithm_version.clone());
    let repository = pg_repository(settings)?;
    let traces = repository
        .list_recommendation_traces_for_replay(limit)
        .await?;
    let mut cases = Vec::new();

    for trace in traces {
        cases.push(
            evaluate_replay_trace(
                &repository,
                &engine,
                &trace,
                settings.candidate_retrieval_limit,
                neighbor_distance_cap_meters,
            )
            .await,
        );
    }

    let matched = cases
        .iter()
        .filter(|case| case.status == ReplayEvaluationStatus::Matched)
        .count();
    let mismatched = cases
        .iter()
        .filter(|case| case.status == ReplayEvaluationStatus::Mismatched)
        .count();
    let failed = cases
        .iter()
        .filter(|case| case.status == ReplayEvaluationStatus::Failed)
        .count();

    Ok(ReplayEvaluationSummary {
        evaluated: cases.len(),
        matched,
        mismatched,
        failed,
        cases,
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
        schema_version: SOURCE_MANIFEST_SCHEMA_VERSION,
        kind: SourceManifestKind::ImportSource,
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

pub fn run_fixture_doctor(path: impl AsRef<Path>) -> Result<FixtureDoctorSummary> {
    let manifest_path = resolve_fixture_manifest_path(path.as_ref());
    let manifest_dir = parent_or_current_dir(&manifest_path);
    let canonical_manifest_dir = manifest_dir.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize fixture directory {}",
            manifest_dir.display()
        )
    })?;
    let raw = fs::read_to_string(&manifest_path).with_context(|| {
        format!(
            "failed to read fixture manifest {}",
            manifest_path.display()
        )
    })?;
    let manifest: FixtureSetManifest = serde_yaml::from_str(&raw).with_context(|| {
        format!(
            "failed to parse fixture manifest {}",
            manifest_path.display()
        )
    })?;

    ensure!(
        manifest.schema_version == FIXTURE_SET_SCHEMA_VERSION,
        "fixture manifest {} schema_version {} is unsupported; expected {}",
        manifest_path.display(),
        manifest.schema_version,
        FIXTURE_SET_SCHEMA_VERSION
    );
    ensure!(
        manifest.kind == FixtureManifestKind::FixtureSet,
        "fixture manifest {} kind {} is invalid; expected {}",
        manifest_path.display(),
        manifest.kind.as_str(),
        FixtureManifestKind::FixtureSet.as_str()
    );
    ensure!(
        !manifest.fixture_set_id.trim().is_empty(),
        "fixture manifest {} is missing fixture_set_id",
        manifest_path.display()
    );
    if let Some(profile_id) = manifest.profile_id.as_deref() {
        ensure!(
            is_profile_id(profile_id),
            "fixture manifest {} invalid profile_id '{}': {}",
            manifest_path.display(),
            profile_id,
            PROFILE_ID_RULE_DESCRIPTION
        );
    }
    ensure!(
        !manifest.files.is_empty(),
        "fixture manifest {} does not list any files",
        manifest_path.display()
    );

    let mut seen_logical_names = BTreeSet::new();
    let mut seen_paths = BTreeSet::new();
    let mut files = Vec::new();
    for file in &manifest.files {
        ensure!(
            !file.logical_name.trim().is_empty(),
            "fixture manifest {} contains a file with empty logical_name",
            manifest_path.display()
        );
        ensure!(
            seen_logical_names.insert(file.logical_name.clone()),
            "fixture manifest {} contains duplicate logical_name {}",
            manifest_path.display(),
            file.logical_name
        );
        ensure!(
            !file.path.trim().is_empty(),
            "fixture manifest {} file {} has an empty path",
            manifest_path.display(),
            file.logical_name
        );
        let normalized_path =
            normalize_fixture_manifest_path(&manifest_path, &file.logical_name, &file.path)?;
        let normalized_path_key = manifest_path_value(&normalized_path);
        ensure!(
            seen_paths.insert(normalized_path_key.clone()),
            "fixture manifest {} contains duplicate path {}",
            manifest_path.display(),
            normalized_path_key
        );
        ensure!(
            matches!(file.format.as_str(), "csv" | "ndjson"),
            "fixture manifest {} file {} uses unsupported format {}; expected csv or ndjson",
            manifest_path.display(),
            file.logical_name,
            file.format
        );

        let fixture_path = manifest_dir.join(&normalized_path);
        ensure!(
            fixture_path.is_file(),
            "fixture manifest {} file {} points to missing fixture file {}",
            manifest_path.display(),
            file.logical_name,
            fixture_path.display()
        );
        let canonical_fixture_path = fixture_path.canonicalize().with_context(|| {
            format!(
                "failed to canonicalize fixture manifest {} file {} path {}",
                manifest_path.display(),
                file.logical_name,
                fixture_path.display()
            )
        })?;
        ensure!(
            canonical_fixture_path.starts_with(&canonical_manifest_dir),
            "fixture manifest {} file {} path {} must stay inside fixture directory {}",
            manifest_path.display(),
            file.logical_name,
            fixture_path.display(),
            canonical_manifest_dir.display()
        );
        let checksum_sha256 = checksum_file(&canonical_fixture_path)?;
        ensure!(
            checksum_sha256 == file.checksum_sha256,
            "fixture manifest {} file {} checksum mismatch: expected {}, got {}",
            manifest_path.display(),
            file.logical_name,
            file.checksum_sha256,
            checksum_sha256
        );
        let row_count = count_fixture_rows(&canonical_fixture_path, &file.format)?;
        ensure!(
            row_count == file.row_count,
            "fixture manifest {} file {} row_count mismatch: expected {}, got {}",
            manifest_path.display(),
            file.logical_name,
            file.row_count,
            row_count
        );
        files.push(FixtureDoctorFile {
            logical_name: file.logical_name.clone(),
            path: fixture_path,
            format: file.format.clone(),
            checksum_sha256,
            row_count,
        });
    }

    Ok(FixtureDoctorSummary {
        manifest_path,
        fixture_set_id: manifest.fixture_set_id,
        profile_id: manifest.profile_id,
        manifest_version: manifest.manifest_version,
        files,
    })
}

fn parent_or_current_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn normalize_fixture_manifest_path(
    manifest_path: &Path,
    logical_name: &str,
    raw_path: &str,
) -> Result<PathBuf> {
    ensure!(
        !raw_path.contains('\\') && !has_windows_drive_prefix(raw_path),
        "fixture manifest {} file {} path must use portable POSIX relative syntax",
        manifest_path.display(),
        logical_name
    );
    let path = Path::new(raw_path);
    ensure!(
        !path.is_absolute(),
        "fixture manifest {} file {} path must be relative",
        manifest_path.display(),
        logical_name
    );

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir => {
                anyhow::bail!(
                    "fixture manifest {} file {} path must stay inside the fixture directory",
                    manifest_path.display(),
                    logical_name
                );
            }
            Component::Prefix(_) | Component::RootDir => {
                anyhow::bail!(
                    "fixture manifest {} file {} path must be relative",
                    manifest_path.display(),
                    logical_name
                );
            }
        }
    }
    ensure!(
        !normalized.as_os_str().is_empty(),
        "fixture manifest {} file {} has an empty path",
        manifest_path.display(),
        logical_name
    );
    Ok(normalized)
}

fn has_windows_drive_prefix(raw_path: &str) -> bool {
    let bytes = raw_path.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn manifest_path_value(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn pg_repository(settings: &AppSettings) -> Result<PgRepository> {
    PgRepository::with_pool_max_size(
        settings.database_url.clone(),
        settings.postgres_pool_max_size,
    )
}

async fn evaluate_replay_trace(
    repository: &PgRepository,
    engine: &RankingEngine,
    trace: &RecommendationTraceReplayRow,
    candidate_limit: usize,
    neighbor_distance_cap_meters: f64,
) -> ReplayEvaluationCase {
    let expected_order = match stored_response_order(&trace.response_payload) {
        Ok(order) => order,
        Err(error) => {
            return failed_replay_case(
                trace,
                None,
                Some(normalize_fallback_stage(&trace.fallback_stage)),
                format!("failed to read stored response item order: {error}"),
            );
        }
    };
    let expected_fallback_stage = stored_response_fallback_stage(&trace.response_payload)
        .unwrap_or_else(|| normalize_fallback_stage(&trace.fallback_stage));
    let request =
        match serde_json::from_value::<RecommendationRequest>(trace.request_payload.clone()) {
            Ok(request) => request,
            Err(error) => {
                return failed_replay_case(
                    trace,
                    None,
                    Some(expected_fallback_stage),
                    format!("failed to parse stored request_payload: {error}"),
                );
            }
        };
    let request_id = request
        .request_id
        .clone()
        .unwrap_or_else(|| format!("replay-trace-{}", trace.id));
    let context_input = request.context_input();
    let resolved_context = match repository
        .resolve_context_for_replay(&request_id, request.user_id.as_deref(), &context_input)
        .await
    {
        Ok(context) => context,
        Err(error) => {
            return failed_replay_case(
                trace,
                Some(request_id),
                Some(expected_fallback_stage),
                format!("failed to resolve replay context: {error}"),
            );
        }
    };
    let target_station = match repository.load_station_for_context(&resolved_context).await {
        Ok(Some(station)) => station,
        Ok(None) => {
            return failed_replay_case(
                trace,
                Some(request_id),
                Some(expected_fallback_stage),
                "resolved context did not map to a station".to_string(),
            );
        }
        Err(error) => {
            return failed_replay_case(
                trace,
                Some(request_id),
                Some(expected_fallback_stage),
                format!("failed to load replay station: {error}"),
            );
        }
    };
    let query = request.with_resolved_context(target_station.id.clone(), resolved_context);
    let neighbor_max_hops = engine.neighbor_max_hops(query.placement);
    let min_candidate_count = engine.minimum_candidate_count();
    let candidate_links = match repository
        .load_context_candidate_links(
            &target_station,
            query.context.as_ref().expect("resolved context is set"),
            candidate_limit,
            min_candidate_count,
            neighbor_distance_cap_meters,
            neighbor_max_hops,
        )
        .await
    {
        Ok(candidate_links) => candidate_links,
        Err(error) => {
            return failed_replay_case(
                trace,
                Some(request_id),
                Some(expected_fallback_stage),
                format!("failed to load replay candidates: {error}"),
            );
        }
    };
    let dataset = match repository
        .load_candidate_dataset(&query, &target_station, &candidate_links)
        .await
    {
        Ok(dataset) => dataset,
        Err(error) => {
            return failed_replay_case(
                trace,
                Some(request_id),
                Some(expected_fallback_stage),
                format!("failed to load replay dataset: {error}"),
            );
        }
    };
    let actual = match engine.recommend(&dataset, &query) {
        Ok(result) => RecommendationResponse::from(result),
        Err(error) => {
            return failed_replay_case(
                trace,
                Some(request_id),
                Some(expected_fallback_stage),
                format!("ranking replay failed: {error}"),
            );
        }
    };

    let actual_order = response_order(&actual);
    let actual_fallback_stage = fallback_stage_label(&actual.fallback_stage);
    let status =
        if expected_order == actual_order && expected_fallback_stage == actual_fallback_stage {
            ReplayEvaluationStatus::Matched
        } else {
            ReplayEvaluationStatus::Mismatched
        };

    ReplayEvaluationCase {
        trace_id: trace.id,
        status,
        request_id: Some(request_id),
        expected_fallback_stage: Some(expected_fallback_stage),
        actual_fallback_stage: Some(actual_fallback_stage),
        expected_order,
        actual_order,
        message: (status == ReplayEvaluationStatus::Mismatched)
            .then_some("stored response differs from current deterministic replay".to_string()),
    }
}

fn failed_replay_case(
    trace: &RecommendationTraceReplayRow,
    request_id: Option<String>,
    expected_fallback_stage: Option<String>,
    message: String,
) -> ReplayEvaluationCase {
    ReplayEvaluationCase {
        trace_id: trace.id,
        status: ReplayEvaluationStatus::Failed,
        request_id,
        expected_fallback_stage,
        actual_fallback_stage: None,
        expected_order: Vec::new(),
        actual_order: Vec::new(),
        message: Some(message),
    }
}

fn response_order(response: &RecommendationResponse) -> Vec<String> {
    response
        .items
        .iter()
        .map(|item| format!("{}:{}", item.content_kind.as_str(), item.content_id))
        .collect()
}

fn stored_response_order(response: &serde_json::Value) -> Result<Vec<String>> {
    let items = response
        .get("items")
        .and_then(serde_json::Value::as_array)
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
                .and_then(serde_json::Value::as_str)
                .or_else(|| item.get("school_id").and_then(serde_json::Value::as_str))
                .with_context(|| "response item content_id must be a string")?;
            Ok(format!("{content_kind}:{content_id}"))
        })
        .collect()
}

fn stored_response_fallback_stage(response: &serde_json::Value) -> Option<String> {
    response
        .get("fallback_stage")
        .and_then(serde_json::Value::as_str)
        .map(normalize_fallback_stage)
}

fn normalize_fallback_stage(stage: &str) -> String {
    match stage {
        "strict" => "strict_station",
        other => other,
    }
    .to_string()
}

fn fallback_stage_label(fallback_stage: &FallbackStageDto) -> String {
    fallback_stage.as_str().to_string()
}

fn format_order(order: &[String]) -> String {
    if order.is_empty() {
        "-".to_string()
    } else {
        order.join(",")
    }
}

pub fn generate_demo_jp_fixture(output_dir: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;

    let files = vec![
        (
            "school_codes",
            output_dir.join("jp_school_codes.csv"),
            "school_code,name,prefecture_name,city_name,school_type\n13101A,Minato Science High,Tokyo,Minato,high_school\n13101B,Harbor Commerce High,Tokyo,Minato,high_school\n13103A,Shinagawa Technical College,Tokyo,Shinagawa,college\n",
        ),
        (
            "school_geodata",
            output_dir.join("jp_school_geodata.csv"),
            "school_code,name,prefecture_name,city_name,address,school_type,latitude,longitude\n13101A,Minato Science High,Tokyo,Minato,芝浦1-1-1,high_school,35.6412,139.7487\n13101B,Harbor Commerce High,Tokyo,Minato,海岸1-2-3,high_school,35.6376,139.7604\n13103A,Shinagawa Technical College,Tokyo,Shinagawa,港南2-16-1,college,35.6289,139.7393\n",
        ),
        (
            "rail_stations",
            output_dir.join("jp_rail_stations.csv"),
            "station_code,station_name,line_name,prefecture_name,latitude,longitude\n1130217,Tamachi,JR Yamanote Line,Tokyo,35.6456,139.7476\n1130218,Shinagawa,JR Yamanote Line,Tokyo,35.6285,139.7388\n1130104,Shimbashi,JR Yamanote Line,Tokyo,35.6663,139.7587\n",
        ),
        (
            "postal_codes",
            output_dir.join("jp_postal_codes.csv"),
            "postal_code,prefecture_name,city_name,town_name\n1080023,Tokyo,Minato,Shibaura\n1050022,Tokyo,Minato,Kaigan\n1080075,Tokyo,Minato,Konan\n",
        ),
    ];

    let mut written = Vec::new();
    let mut manifest_files = Vec::new();
    for (logical_name, path, contents) in files {
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        manifest_files.push(fixture_file_manifest(logical_name, &path, "csv")?);
        written.push(path);
    }
    let manifest_path = output_dir.join("fixture_manifest.yaml");
    write_fixture_manifest(
        &manifest_path,
        "demo_jp",
        Some("school-event-jp"),
        "Small JP adapter fixture set for deterministic import smoke tests.",
        manifest_files,
    )?;
    written.push(manifest_path);
    Ok(written)
}

fn resolve_fixture_manifest_path(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.join("fixture_manifest.yaml")
    } else {
        path.to_path_buf()
    }
}

fn fixture_file_manifest(
    logical_name: &str,
    path: &Path,
    format: &str,
) -> Result<FixtureFileManifest> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .with_context(|| format!("fixture file name is invalid for {}", path.display()))?;
    Ok(FixtureFileManifest {
        logical_name: logical_name.to_string(),
        path: file_name.to_string(),
        format: format.to_string(),
        checksum_sha256: checksum_file(path)?,
        row_count: count_fixture_rows(path, format)?,
    })
}

fn write_fixture_manifest(
    manifest_path: &Path,
    fixture_set_id: &str,
    profile_id: Option<&str>,
    description: &str,
    files: Vec<FixtureFileManifest>,
) -> Result<()> {
    let manifest = FixtureSetManifest {
        schema_version: FIXTURE_SET_SCHEMA_VERSION,
        kind: FixtureManifestKind::FixtureSet,
        manifest_version: 2,
        fixture_set_id: fixture_set_id.to_string(),
        profile_id: profile_id.map(str::to_string),
        description: Some(description.to_string()),
        files,
    };
    let raw = serde_yaml::to_string(&manifest)?;
    fs::write(manifest_path, raw)
        .with_context(|| format!("failed to write {}", manifest_path.display()))
}

fn checksum_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(&bytes)))
}

fn count_fixture_rows(path: &Path, format: &str) -> Result<u64> {
    match format {
        "csv" => {
            let mut reader = Reader::from_path(path)
                .with_context(|| format!("failed to open fixture CSV {}", path.display()))?;
            let mut count = 0_u64;
            for row in reader.records() {
                row.with_context(|| format!("failed to parse {}", path.display()))?;
                count += 1;
            }
            Ok(count)
        }
        "ndjson" => {
            let raw = fs::read_to_string(path)
                .with_context(|| format!("failed to read fixture NDJSON {}", path.display()))?;
            Ok(raw.lines().filter(|line| !line.trim().is_empty()).count() as u64)
        }
        _ => anyhow::bail!("unsupported fixture format {format}"),
    }
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
    use std::path::Path;

    use storage_postgres::EventCsvRecord;

    use super::{
        checksum_file, generate_demo_jp_fixture, normalize_fallback_stage, parent_or_current_dir,
        run_fixture_doctor, stored_response_order, validate_event_csv_records,
    };

    #[test]
    fn writes_demo_fixture_files() {
        let temp = tempfile::tempdir().expect("tempdir");
        let written = generate_demo_jp_fixture(temp.path()).expect("fixture generation");
        assert_eq!(written.len(), 5);
        assert!(written.iter().all(|path| path.exists()));
        let summary = run_fixture_doctor(temp.path()).expect("fixture doctor");
        assert_eq!(summary.fixture_set_id, "demo_jp");
        assert_eq!(summary.profile_id.as_deref(), Some("school-event-jp"));
        assert_eq!(summary.files.len(), 4);
    }

    #[test]
    fn fixture_doctor_rejects_checksum_mismatch() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(temp.path().join("data.csv"), "id,name\n1,Example\n").expect("fixture");
        std::fs::write(
            temp.path().join("fixture_manifest.yaml"),
            r#"
schema_version: 1
kind: fixture_set
manifest_version: 1
fixture_set_id: test
files:
  - logical_name: data
    path: data.csv
    format: csv
    checksum_sha256: deadbeef
    row_count: 1
"#,
        )
        .expect("manifest");

        let error = run_fixture_doctor(temp.path()).expect_err("checksum mismatch");
        assert!(format!("{error:#}").contains("checksum mismatch"));
    }

    #[test]
    fn fixture_doctor_invalid_profile_id_error_includes_value() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("fixture_manifest.yaml"),
            r#"
schema_version: 1
kind: fixture_set
manifest_version: 1
fixture_set_id: test
profile_id: "school-event-jp "
files: []
"#,
        )
        .expect("manifest");

        let error = run_fixture_doctor(temp.path()).expect_err("invalid profile id");
        let rendered = format!("{error:#}");
        assert!(rendered.contains("invalid profile_id 'school-event-jp '"));
        assert!(rendered.contains("must be non-empty and trimmed"));
    }

    #[test]
    fn fixture_doctor_rejects_duplicate_paths_after_normalization() {
        let temp = tempfile::tempdir().expect("tempdir");
        let fixture_path = temp.path().join("data.csv");
        std::fs::write(&fixture_path, "id,name\n1,Example\n").expect("fixture");
        let checksum = checksum_file(&fixture_path).expect("checksum");
        std::fs::write(
            temp.path().join("fixture_manifest.yaml"),
            format!(
                r#"
schema_version: 1
kind: fixture_set
manifest_version: 1
fixture_set_id: test
files:
  - logical_name: data_a
    path: data.csv
    format: csv
    checksum_sha256: {checksum}
    row_count: 1
  - logical_name: data_b
    path: ./data.csv
    format: csv
    checksum_sha256: {checksum}
    row_count: 1
"#
            ),
        )
        .expect("manifest");

        let error = run_fixture_doctor(temp.path()).expect_err("duplicate normalized path");
        assert!(format!("{error:#}").contains("duplicate path data.csv"));
    }

    #[test]
    fn fixture_doctor_rejects_windows_style_fixture_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("fixture_manifest.yaml"),
            r#"
schema_version: 1
kind: fixture_set
manifest_version: 1
fixture_set_id: test
files:
  - logical_name: data
    path: C:/fixtures/data.csv
    format: csv
    checksum_sha256: deadbeef
    row_count: 1
"#,
        )
        .expect("manifest");

        let error = run_fixture_doctor(temp.path()).expect_err("windows-style path");
        assert!(format!("{error:#}").contains("portable POSIX relative syntax"));
    }

    #[cfg(unix)]
    #[test]
    fn fixture_doctor_rejects_symlink_escape() {
        let temp = tempfile::tempdir().expect("tempdir");
        let fixture_dir = temp.path().join("fixtures");
        std::fs::create_dir_all(&fixture_dir).expect("fixture dir");
        let outside_path = temp.path().join("outside.csv");
        std::fs::write(&outside_path, "id,name\n1,Outside\n").expect("outside fixture");
        std::os::unix::fs::symlink(&outside_path, fixture_dir.join("data.csv")).expect("symlink");
        let checksum = checksum_file(&outside_path).expect("checksum");
        std::fs::write(
            fixture_dir.join("fixture_manifest.yaml"),
            format!(
                r#"
schema_version: 1
kind: fixture_set
manifest_version: 1
fixture_set_id: test
files:
  - logical_name: data
    path: data.csv
    format: csv
    checksum_sha256: {checksum}
    row_count: 1
"#
            ),
        )
        .expect("manifest");

        let error = run_fixture_doctor(&fixture_dir).expect_err("symlink escape");
        assert!(format!("{error:#}").contains("must stay inside fixture directory"));
    }

    #[test]
    fn parent_or_current_dir_treats_bare_manifest_filename_as_current_dir() {
        assert_eq!(
            parent_or_current_dir(Path::new("fixture_manifest.yaml")),
            Path::new(".")
        );
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
    fn replay_reader_accepts_legacy_school_only_trace_shape() {
        let payload = serde_json::json!({
            "items": [
                { "school_id": "school_seaside" },
                { "content_kind": "event", "content_id": "event_open" }
            ],
            "fallback_stage": "strict"
        });

        let order = stored_response_order(&payload).expect("legacy order");

        assert_eq!(order, vec!["school:school_seaside", "event:event_open"]);
        assert_eq!(normalize_fallback_stage("strict"), "strict_station");
    }

    #[test]
    fn replay_reader_rejects_non_string_content_kind() {
        let payload = serde_json::json!({
            "items": [
                { "content_kind": 7, "content_id": "event_open" }
            ]
        });

        let error = stored_response_order(&payload).expect_err("invalid content kind");

        assert!(error
            .to_string()
            .contains("response item content_kind must be a string"));
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
