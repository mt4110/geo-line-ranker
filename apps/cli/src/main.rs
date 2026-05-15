#[cfg(feature = "api-docs")]
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand};
use cli::{
    format_context_coverage_doctor_summary, format_eval_golden_summary,
    format_explanation_integrity_doctor_summary, format_fixture_doctor_summary,
    format_ingest_quality_doctor_summary, format_profile_pack_doctor_summary,
    format_ranking_config_doctor_summary, format_retrieval_parity_doctor_summary,
    format_storage_compatibility_doctor_summary, generate_demo_jp_fixture,
    ranking_config_doctor_summary_from_lint, run_context_coverage_doctor,
    run_explanation_integrity_doctor, run_fixture_doctor, run_ingest_quality_doctor,
    run_profile_pack_doctor, run_replay_scenarios_with_source, run_retrieval_parity_doctor,
    run_storage_compatibility_doctor, ReplayScenarioSource, DEFAULT_REPLAY_SCENARIO_PATH,
};
#[cfg(feature = "storage-backends")]
use cli::{
    format_context_inspect_summary, format_eval_replay_summary, format_explain_trace_report,
    format_job_enqueue_summary, format_job_inspection, format_job_list,
    format_job_mutation_summary, format_replay_evaluation_summary, format_replay_scenario_summary,
    format_snapshot_refresh_summary, format_summary, run_context_inspect,
    run_derive_school_station_links, run_event_csv_import, run_event_ndjson_import,
    run_explain_trace, run_import_command, run_job_due, run_job_enqueue, run_job_inspect,
    run_job_list, run_job_retry, run_profile_source_import, run_replay_evaluate,
    run_replay_scenarios, run_snapshot_refresh, ContextInspectInput, ImportTarget,
    DEFAULT_EVENT_NDJSON_SOURCE_ID,
};
#[cfg(feature = "storage-backends")]
use config::AppSettings;
use config::{
    lint_profile_pack_dir, lint_ranking_config_dir, load_and_lint_profile_pack_file,
    resolve_linted_profile_pack_runtime_selection, resolve_runtime_path, ProfilePackLintFile,
    ProfilePackLintSummary, ProfilePackManifest, ProfilePackRegistry, RankingConfigLintSummary,
    DEFAULT_ALGORITHM_VERSION, DEFAULT_PROFILE_ID, DEFAULT_PROFILE_PACKS_DIR,
    DEFAULT_RANKING_CONFIG_DIR,
};
use generic_csv::{lint_source_manifest_dir, SourceManifestLintSummary};
#[cfg(feature = "storage-backends")]
use sha2::{Digest, Sha256};
#[cfg(feature = "storage-backends")]
use storage::{
    EvaluationRunCaseRecord, EvaluationRunCaseStatus, EvaluationRunKind, EvaluationRunRecord,
    EvaluationRunStatus, ProfileCompatibilityStatus, ProfileCompatibilityStatusRecord,
    ProfileManifestRecord, ProfileRegistryRepository,
};
#[cfg(feature = "storage-backends")]
use storage_opensearch::ProjectionSyncService;
#[cfg(feature = "storage-backends")]
use storage_postgres::{run_migrations, seed_fixture, PgRepository};

#[derive(Debug, Parser)]
#[command(name = "geo-line-ranker-cli")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[cfg(feature = "storage-backends")]
    Migrate,
    #[cfg(feature = "storage-backends")]
    Seed {
        #[command(subcommand)]
        target: SeedTarget,
    },
    #[cfg(feature = "storage-backends")]
    Import {
        #[command(subcommand)]
        target: ImportCommand,
    },
    #[cfg(feature = "storage-backends")]
    Derive {
        #[command(subcommand)]
        target: DeriveCommand,
    },
    Fixtures {
        #[command(subcommand)]
        target: FixtureCommand,
    },
    #[cfg(feature = "storage-backends")]
    Index {
        #[command(subcommand)]
        target: IndexCommand,
    },
    #[cfg(feature = "storage-backends")]
    Projection {
        #[command(subcommand)]
        target: ProjectionCommand,
    },
    #[cfg(feature = "storage-backends")]
    Snapshot {
        #[command(subcommand)]
        target: SnapshotCommand,
    },
    #[cfg(feature = "storage-backends")]
    Replay {
        #[command(subcommand)]
        target: ReplayCommand,
    },
    #[command(about = "Run evaluation quality gates with operator-facing names")]
    Eval {
        #[command(subcommand)]
        target: EvalCommand,
    },
    #[cfg(feature = "storage-backends")]
    Explain {
        #[command(subcommand)]
        target: ExplainCommand,
    },
    Doctor {
        #[command(subcommand)]
        target: DoctorCommand,
    },
    Profile {
        #[command(subcommand)]
        target: ProfileCommand,
    },
    #[cfg(feature = "storage-backends")]
    Context {
        #[command(subcommand)]
        target: ContextCommand,
    },
    Config {
        #[command(subcommand)]
        target: ConfigCommand,
    },
    #[command(name = "source-manifest", about = "Inspect import source manifests")]
    SourceManifest {
        #[command(subcommand)]
        target: SourceManifestCommand,
    },
    #[command(about = "Inspect and recover DB-backed worker jobs")]
    #[cfg(feature = "storage-backends")]
    Jobs {
        #[command(subcommand)]
        target: JobsCommand,
    },
    #[cfg(feature = "api-docs")]
    DumpOpenapi {
        #[arg(default_value = "schemas/openapi.json")]
        output: PathBuf,
    },
}

#[cfg(feature = "storage-backends")]
#[derive(Debug, Subcommand)]
enum SeedTarget {
    Example,
}

#[cfg(feature = "storage-backends")]
#[derive(Debug, Subcommand)]
enum ImportCommand {
    #[command(name = "jp-rail")]
    Rail {
        #[arg(long)]
        manifest: PathBuf,
    },
    #[command(name = "jp-postal")]
    Postal {
        #[arg(long)]
        manifest: PathBuf,
    },
    #[command(name = "jp-school-codes")]
    SchoolCodes {
        #[arg(long)]
        manifest: PathBuf,
    },
    #[command(name = "jp-school-geodata")]
    SchoolGeodata {
        #[arg(long)]
        manifest: PathBuf,
    },
    #[command(name = "event-csv")]
    EventCsv {
        #[arg(long)]
        file: PathBuf,
    },
    #[command(name = "event-ndjson")]
    EventNdjson {
        #[arg(long)]
        file: PathBuf,
        #[arg(long, default_value = DEFAULT_EVENT_NDJSON_SOURCE_ID)]
        source_id: String,
    },
    #[command(
        name = "profile-source",
        about = "Import one profile-declared source by source_id"
    )]
    ProfileSource {
        #[arg(
            long,
            help = "Profile id to resolve. Defaults to PROFILE_ID or local-discovery-generic."
        )]
        profile_id: Option<String>,
        #[arg(
            long = "profiles-path",
            help = "Profile pack root directory or explicit profile.yaml file."
        )]
        profiles_path: Option<PathBuf>,
        #[arg(
            long,
            help = "Connector source_id declared by the selected profile pack."
        )]
        source_id: String,
    },
}

#[cfg(feature = "storage-backends")]
#[derive(Debug, Subcommand)]
enum DeriveCommand {
    SchoolStationLinks,
}

#[derive(Debug, Subcommand)]
enum FixtureCommand {
    GenerateDemoJp {
        #[arg(default_value = "storage/fixtures/demo_jp")]
        output: PathBuf,
    },
    Doctor {
        #[arg(
            long,
            default_value = "storage/fixtures/minimal",
            help = "Fixture directory or fixture_manifest.yaml to verify."
        )]
        path: PathBuf,
    },
}

#[cfg(feature = "storage-backends")]
#[derive(Debug, Subcommand)]
enum IndexCommand {
    Rebuild,
}

#[cfg(feature = "storage-backends")]
#[derive(Debug, Subcommand)]
enum ProjectionCommand {
    Sync,
}

#[cfg(feature = "storage-backends")]
#[derive(Debug, Subcommand)]
enum SnapshotCommand {
    Refresh,
}

#[cfg(feature = "storage-backends")]
#[derive(Debug, Subcommand)]
enum ReplayCommand {
    #[command(about = "Replay recent recommendation traces against the current SQL-only path")]
    Evaluate {
        #[arg(long, default_value_t = 20, help = "Maximum recent traces to replay")]
        limit: i64,
        #[arg(long, help = "Exit non-zero when any replay mismatches or fails")]
        fail_on_mismatch: bool,
    },
    #[command(about = "Replay committed golden scenarios without requiring persisted traces")]
    Scenarios {
        #[arg(
            long,
            default_value = DEFAULT_REPLAY_SCENARIO_PATH,
            help = "Scenario YAML file or directory to replay"
        )]
        path: PathBuf,
        #[arg(
            long,
            help = "Ranking config directory. Defaults to RANKING_CONFIG_DIR or configs/ranking."
        )]
        ranking_config_dir: Option<PathBuf>,
        #[arg(
            long,
            help = "Algorithm version label for report parity. Defaults to ALGORITHM_VERSION or the runtime default."
        )]
        algorithm_version: Option<String>,
        #[arg(long, help = "Print the scenario report as JSON")]
        json: bool,
        #[arg(long, help = "Exit zero even when blocker checks fail")]
        allow_blockers: bool,
    },
}

#[derive(Debug, Subcommand)]
enum EvalCommand {
    #[command(
        about = "Run the committed golden scenario evaluation",
        long_about = "Run the committed golden scenario evaluation without requiring persisted PostgreSQL traces. This is an operator-facing alias for `replay scenarios`; both commands use the same deterministic scenario runner, checks, and JSON report shape. When --profile-id or PROFILE_ID is set, the default scenario path comes from the selected profile manifest's evaluation.scenario_pack.\n\nExamples:\n  geo-line-ranker-cli eval golden\n  geo-line-ranker-cli eval golden --json\n  geo-line-ranker-cli eval golden --profile-id school-event-jp\n  geo-line-ranker-cli eval golden --scenario-path configs/evaluation/scenarios"
    )]
    Golden {
        #[arg(
            long = "scenario-path",
            visible_alias = "path",
            help = "Scenario YAML file or directory to replay. Overrides profile evaluation.scenario_pack and skips profile evaluation.pairwise_pack when set."
        )]
        scenario_path: Option<PathBuf>,
        #[arg(
            long,
            help = "Profile id whose evaluation.scenario_pack should be replayed. Defaults to PROFILE_ID when set."
        )]
        profile_id: Option<String>,
        #[arg(
            long = "profiles-path",
            help = "Profile pack root directory or explicit profile.yaml file. Used only with --profile-id or PROFILE_ID."
        )]
        profiles_path: Option<PathBuf>,
        #[arg(
            long,
            help = "Ranking config directory. Defaults to RANKING_CONFIG_DIR or configs/ranking."
        )]
        ranking_config_dir: Option<PathBuf>,
        #[arg(
            long,
            help = "Algorithm version label for report parity. Defaults to ALGORITHM_VERSION or the runtime default."
        )]
        algorithm_version: Option<String>,
        #[arg(long, help = "Print the scenario report as JSON")]
        json: bool,
        #[arg(long, help = "Exit zero even when blocker checks fail")]
        allow_blockers: bool,
        #[arg(
            long,
            help = "Persist the evaluation run to PostgreSQL without changing ranking behavior"
        )]
        persist: bool,
    },
    #[cfg(feature = "storage-backends")]
    #[command(
        about = "Replay recent recommendation traces against the current SQL-only path",
        long_about = "Replay recent recommendation traces against the current SQL-only path. This is an operator-facing alias for `replay evaluate`; both commands use the same persisted trace replay runner, checks, and JSON report shape.\n\nExamples:\n  geo-line-ranker-cli eval replay --limit 20\n  geo-line-ranker-cli eval replay --limit 20 --fail-on-mismatch"
    )]
    Replay {
        #[arg(long, default_value_t = 20, help = "Maximum recent traces to replay")]
        limit: i64,
        #[arg(long, help = "Exit non-zero when any replay mismatches or fails")]
        fail_on_mismatch: bool,
    },
}

#[cfg(feature = "storage-backends")]
#[derive(Debug, Subcommand)]
enum ExplainCommand {
    #[command(
        about = "Read one persisted recommendation trace and explain its request, response, reasons, and integrity",
        long_about = "Read one recommendation_traces row without replaying ranking, then print the stored request, response fallback stage, result order, reason codes, trace payload, and explanation integrity checks.\n\nExamples:\n  geo-line-ranker-cli explain trace --id 42\n  geo-line-ranker-cli explain trace --id 42 --json"
    )]
    Trace {
        #[arg(long, help = "recommendation_traces.id to inspect")]
        id: i64,
        #[arg(long, help = "Print the trace explanation report as JSON")]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum DoctorCommand {
    #[command(
        name = "ranking-config",
        about = "Run the ranking config contract doctor for operator-facing quality evidence",
        long_about = "Run the ranking config contract doctor for operator-facing quality evidence. This reuses the same active ranking config and profile-pack lint path as `config lint`, then summarizes ranking file kinds, active profile selection, profile pack coverage, profile compatibility levels, referenced ranking config directories, reason catalog references, fixture references, source manifest references, event CSV example references, and optional crawler manifest references.\n\nExamples:\n  geo-line-ranker-cli doctor ranking-config\n  geo-line-ranker-cli doctor ranking-config --json\n  geo-line-ranker-cli doctor ranking-config --path configs/ranking --profiles-path configs/profiles"
    )]
    RankingConfig {
        #[arg(
            long,
            help = "Ranking config directory to diagnose. Defaults to RANKING_CONFIG_DIR or the selected profile pack."
        )]
        path: Option<PathBuf>,
        #[arg(
            long,
            help = "Profile pack root directory or explicit profile.yaml file to diagnose. Defaults to PROFILE_PACKS_DIR or configs/profiles."
        )]
        profiles_path: Option<PathBuf>,
        #[arg(long, help = "Print the doctor report as JSON")]
        json: bool,
    },
    #[command(
        name = "explanation-integrity",
        about = "Run the DB-free explanation integrity doctor against committed replay scenarios",
        long_about = "Run the DB-free explanation integrity doctor against committed replay scenarios. This reports only reason-code integrity and explanation-template checks; use `eval golden` or `replay scenarios` for the full ranking correctness gate.\n\nExamples:\n  geo-line-ranker-cli doctor explanation-integrity\n  geo-line-ranker-cli doctor explanation-integrity --json"
    )]
    ExplanationIntegrity {
        #[arg(
            long,
            default_value = DEFAULT_REPLAY_SCENARIO_PATH,
            help = "Scenario YAML file or directory to use as doctor input"
        )]
        path: PathBuf,
        #[arg(
            long,
            help = "Ranking config directory. Defaults to RANKING_CONFIG_DIR or configs/ranking."
        )]
        ranking_config_dir: Option<PathBuf>,
        #[arg(
            long,
            help = "Algorithm version label for report parity. Defaults to ALGORITHM_VERSION or the runtime default."
        )]
        algorithm_version: Option<String>,
        #[arg(long, help = "Print the doctor report as JSON")]
        json: bool,
        #[arg(
            long,
            help = "Exit zero even when explanation integrity blocker checks fail"
        )]
        allow_blockers: bool,
    },
    #[command(
        name = "profile-pack",
        about = "Run the profile pack validation doctor against committed profile manifests",
        long_about = "Run the profile pack validation doctor against committed profile manifests. This reuses the same manifest, reason catalog, ranking config, fixture, compatibility level, and local reference validation as `profile validate`, then prints operator-facing profile-pack coverage metrics.\n\nExamples:\n  geo-line-ranker-cli doctor profile-pack\n  geo-line-ranker-cli doctor profile-pack --json\n  geo-line-ranker-cli doctor profile-pack --profiles-path configs/profiles"
    )]
    ProfilePack {
        #[arg(
            long,
            help = "Profile pack root directory or explicit profile.yaml file to diagnose. Defaults to PROFILE_PACKS_DIR or configs/profiles."
        )]
        profiles_path: Option<PathBuf>,
        #[arg(long, help = "Print the doctor report as JSON")]
        json: bool,
        #[arg(
            long,
            help = "Persist validated profile registry and compatibility status to PostgreSQL"
        )]
        persist: bool,
    },
    #[command(
        name = "ingest-quality",
        about = "Run the DB-free ingest quality doctor for profile connector coverage",
        long_about = "Run the DB-free ingest quality doctor for profile connector coverage. This reuses the same profile-pack lint path as `profile validate`, then validates declared source manifests, archive manifests, and crawler manifests without running imports, touching PostgreSQL, or making live crawl requests. It summarizes source classes, manifest kinds, runtime-executable field mappings, registry-only mapping boundaries, crawler allowlist requirements, source-manifest file counts, archive file/format counts, and crawler target coverage.\n\nExamples:\n  geo-line-ranker-cli doctor ingest-quality\n  geo-line-ranker-cli doctor ingest-quality --json\n  geo-line-ranker-cli doctor ingest-quality --profiles-path configs/profiles"
    )]
    IngestQuality {
        #[arg(
            long,
            help = "Profile pack root directory or explicit profile.yaml file to diagnose. Defaults to PROFILE_PACKS_DIR or configs/profiles."
        )]
        profiles_path: Option<PathBuf>,
        #[arg(long, help = "Print the doctor report as JSON")]
        json: bool,
    },
    #[command(
        name = "context-coverage",
        about = "Run the DB-free context coverage doctor against committed replay scenarios",
        long_about = "Run the DB-free context coverage doctor against committed replay scenarios. This reads scenario metadata and expectations only, then summarizes context source, context shape, scenario tags, fallback-stage, and candidate-count coverage; use `eval golden` or `replay scenarios` for ranking correctness.\n\nExamples:\n  geo-line-ranker-cli doctor context-coverage\n  geo-line-ranker-cli doctor context-coverage --json\n  geo-line-ranker-cli doctor context-coverage --path configs/evaluation/scenarios"
    )]
    ContextCoverage {
        #[arg(
            long,
            default_value = DEFAULT_REPLAY_SCENARIO_PATH,
            help = "Scenario YAML file or directory to use as doctor input"
        )]
        path: PathBuf,
        #[arg(long, help = "Print the doctor report as JSON")]
        json: bool,
    },
    #[command(
        name = "retrieval-parity",
        about = "Run the DB-free retrieval parity doctor for candidate-slice ordering",
        long_about = "Run the DB-free retrieval parity doctor for candidate-slice ordering. This checks that the full-mode OpenSearch candidate retrieval contract keeps the same pre-ranking ordering as the SQL-only candidate slice: direct station first, then distance, walking minutes, school id, and station id. It does not require PostgreSQL or OpenSearch, and it remains optional full-mode evidence rather than part of the public MVP gate.\n\nExamples:\n  geo-line-ranker-cli doctor retrieval-parity\n  geo-line-ranker-cli doctor retrieval-parity --json"
    )]
    RetrievalParity {
        #[arg(long, help = "Print the doctor report as JSON")]
        json: bool,
    },
    #[command(
        name = "storage-compatibility",
        about = "Print the static storage/cache/index compatibility registry",
        long_about = "Print the static storage/cache/index compatibility registry. This is a DB-free operator-facing status report for PostgreSQL/PostGIS, Redis, OpenSearch, MySQL, and SQLite support levels. It separates storage compatibility from profile-pack compatibility levels and does not claim MySQL write support.\n\nExamples:\n  geo-line-ranker-cli doctor storage-compatibility\n  geo-line-ranker-cli doctor storage-compatibility --json"
    )]
    StorageCompatibility {
        #[arg(long, help = "Print the doctor report as JSON")]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum ProfileCommand {
    #[command(about = "List profile pack manifests with validated contract metadata")]
    List {
        #[arg(
            long,
            help = "Profile pack root directory or explicit profile.yaml file to list after validation. Defaults to PROFILE_PACKS_DIR or configs/profiles."
        )]
        profiles_path: Option<PathBuf>,
    },
    #[command(about = "Validate profile pack manifests, reason catalogs, and local references")]
    Validate {
        #[arg(
            long,
            help = "Profile pack root directory or explicit profile.yaml file to validate. Defaults to PROFILE_PACKS_DIR or configs/profiles."
        )]
        profiles_path: Option<PathBuf>,
        #[arg(
            long,
            help = "Persist validated profile registry and compatibility status to PostgreSQL"
        )]
        persist: bool,
    },
    #[command(about = "Inspect one profile pack manifest")]
    Inspect {
        #[arg(
            long,
            help = "Profile id to inspect. Defaults to PROFILE_ID, the selected profile.yaml file id, or local-discovery-generic."
        )]
        profile_id: Option<String>,
        #[arg(
            long,
            help = "Profile pack root directory or explicit profile.yaml file to inspect. Defaults to PROFILE_PACKS_DIR or configs/profiles."
        )]
        profiles_path: Option<PathBuf>,
    },
}

#[cfg(feature = "storage-backends")]
#[derive(Debug, Subcommand)]
enum ContextCommand {
    #[command(
        about = "Resolve and inspect ranking context without writing a trace",
        long_about = "Resolve ranking context from explicit request hints or recent user search evidence, then print the normalized context and evidence summary without writing context_resolution_traces.",
        after_long_help = "Examples:\n  geo-line-ranker-cli context inspect --city-name Minato --prefecture-name Tokyo\n  geo-line-ranker-cli context inspect --user-id manual-user-1 --json"
    )]
    Inspect {
        #[arg(
            long,
            default_value = "cli-context-inspect",
            help = "Opaque request id used only for read-only resolver diagnostics"
        )]
        request_id: String,
        #[arg(
            long,
            help = "Optional user id for resolving recent search evidence or profile context"
        )]
        user_id: Option<String>,
        #[arg(long, help = "Explicit station context hint")]
        station_id: Option<String>,
        #[arg(long, help = "Explicit line id context hint")]
        line_id: Option<String>,
        #[arg(long, help = "Explicit line name context hint")]
        line_name: Option<String>,
        #[arg(long, help = "Area country code; country-only input is ignored")]
        country: Option<String>,
        #[arg(long, help = "Explicit prefecture code context hint")]
        prefecture_code: Option<String>,
        #[arg(long, help = "Explicit prefecture name context hint")]
        prefecture_name: Option<String>,
        #[arg(long, help = "Explicit city code context hint")]
        city_code: Option<String>,
        #[arg(long, help = "Explicit city name context hint")]
        city_name: Option<String>,
        #[arg(long, help = "Print the resolved context summary as JSON")]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    #[command(about = "Lint active ranking config files and profile packs")]
    Lint {
        #[arg(
            long,
            help = "Ranking config directory to lint. Defaults to RANKING_CONFIG_DIR or the selected profile pack."
        )]
        path: Option<PathBuf>,
        #[arg(
            long,
            help = "Profile pack root directory or explicit profile.yaml file to lint. Defaults to PROFILE_PACKS_DIR or configs/profiles."
        )]
        profiles_path: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum SourceManifestCommand {
    #[command(about = "Lint import source manifest files")]
    Lint {
        #[arg(
            long,
            default_value = "storage/sources",
            help = "Directory or YAML file containing import source manifests."
        )]
        path: PathBuf,
    },
}

#[cfg(feature = "storage-backends")]
#[derive(Debug, Subcommand)]
enum JobsCommand {
    #[command(about = "Show recent jobs and queue pressure by type and status")]
    List {
        #[arg(long, default_value_t = 20, help = "Maximum recent jobs to print")]
        limit: i64,
    },
    #[command(about = "Show one job with payload, lock state, and attempt history")]
    Inspect {
        #[arg(long, help = "Job queue id to inspect")]
        id: i64,
    },
    #[command(about = "Queue one more attempt for a failed job")]
    Retry {
        #[arg(long, help = "Failed job queue id to retry")]
        id: i64,
    },
    #[command(about = "Make a delayed queued job due now")]
    Due {
        #[arg(long, help = "Queued job id to make due")]
        id: i64,
    },
    #[command(about = "Create a manual worker job for scoped recovery")]
    Enqueue {
        #[arg(
            long,
            help = "Job type: refresh_popularity_snapshot, refresh_user_affinity_snapshot, invalidate_recommendation_cache, or sync_candidate_projection"
        )]
        job_type: String,
        #[arg(long, default_value = "{}", help = "JSON object payload for the job")]
        payload: String,
        #[arg(long, default_value_t = 3, help = "Maximum attempts before failure")]
        max_attempts: i32,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    config::load_dotenv();
    let cli = Cli::parse();

    match cli.command {
        #[cfg(feature = "storage-backends")]
        Command::Migrate => {
            let settings = AppSettings::from_env_without_profile_pack()?;
            run_migrations(&settings.database_url, "storage/migrations/postgres").await?;
        }
        #[cfg(feature = "storage-backends")]
        Command::Seed { target } => match target {
            SeedTarget::Example => {
                let settings = AppSettings::from_env_requiring_fixture()?;
                seed_fixture(&settings.database_url, &settings.fixture_dir).await?
            }
        },
        #[cfg(feature = "storage-backends")]
        Command::Import { target } => {
            let settings = AppSettings::from_env_without_profile_pack()?;
            let summary = match target {
                ImportCommand::Rail { manifest } => {
                    run_import_command(&settings, ImportTarget::JpRail, manifest).await?
                }
                ImportCommand::Postal { manifest } => {
                    run_import_command(&settings, ImportTarget::JpPostal, manifest).await?
                }
                ImportCommand::SchoolCodes { manifest } => {
                    run_import_command(&settings, ImportTarget::JpSchoolCodes, manifest).await?
                }
                ImportCommand::SchoolGeodata { manifest } => {
                    run_import_command(&settings, ImportTarget::JpSchoolGeodata, manifest).await?
                }
                ImportCommand::EventCsv { file } => run_event_csv_import(&settings, file).await?,
                ImportCommand::EventNdjson { file, source_id } => {
                    run_event_ndjson_import(&settings, file, &source_id).await?
                }
                ImportCommand::ProfileSource {
                    profile_id,
                    profiles_path,
                    source_id,
                } => {
                    let profiles_path = profiles_path.unwrap_or(env_path_or_default(
                        "PROFILE_PACKS_DIR",
                        PathBuf::from(DEFAULT_PROFILE_PACKS_DIR),
                    )?);
                    let env_profile_id = config::env_optional_non_empty("PROFILE_ID")?;
                    run_profile_source_import(
                        &settings,
                        profiles_path,
                        profile_id.as_deref().or(env_profile_id.as_deref()),
                        &source_id,
                    )
                    .await?
                }
            };
            println!("{}", format_summary(&summary));
        }
        #[cfg(feature = "storage-backends")]
        Command::Derive { target } => match target {
            DeriveCommand::SchoolStationLinks => {
                let settings = AppSettings::from_env_without_profile_pack()?;
                let summary = run_derive_school_station_links(&settings).await?;
                println!("{}", format_summary(&summary));
            }
        },
        Command::Fixtures { target } => match target {
            FixtureCommand::GenerateDemoJp { output } => {
                let files = generate_demo_jp_fixture(output)?;
                println!("generated {} fixture files", files.len());
            }
            FixtureCommand::Doctor { path } => {
                let summary = run_fixture_doctor(path)?;
                println!("{}", format_fixture_doctor_summary(&summary));
            }
        },
        #[cfg(feature = "storage-backends")]
        Command::Index { target } => match target {
            IndexCommand::Rebuild => {
                let settings = AppSettings::from_env_without_profile_pack()?;
                let service = ProjectionSyncService::new(
                    settings.database_url.clone(),
                    &settings.opensearch,
                )?;
                let summary = service.rebuild_index().await?;
                println!(
                    "index rebuild completed: indexed_documents={}, deleted_documents={}",
                    summary.indexed_documents, summary.deleted_documents
                );
            }
        },
        #[cfg(feature = "storage-backends")]
        Command::Projection { target } => match target {
            ProjectionCommand::Sync => {
                let settings = AppSettings::from_env_without_profile_pack()?;
                let service = ProjectionSyncService::new(
                    settings.database_url.clone(),
                    &settings.opensearch,
                )?;
                let summary = service.sync_projection_once().await?;
                println!(
                    "projection sync completed: indexed_documents={}, deleted_documents={}",
                    summary.indexed_documents, summary.deleted_documents
                );
            }
        },
        #[cfg(feature = "storage-backends")]
        Command::Snapshot { target } => match target {
            SnapshotCommand::Refresh => {
                let settings = AppSettings::from_env()?;
                let summary = run_snapshot_refresh(&settings).await?;
                println!("{}", format_snapshot_refresh_summary(&summary));
            }
        },
        #[cfg(feature = "storage-backends")]
        Command::Replay { target } => match target {
            ReplayCommand::Evaluate {
                limit,
                fail_on_mismatch,
            } => {
                run_replay_evaluate_command(
                    limit,
                    fail_on_mismatch,
                    format_replay_evaluation_summary,
                    "replay evaluation",
                )
                .await?
            }
            ReplayCommand::Scenarios {
                path,
                ranking_config_dir,
                algorithm_version,
                json,
                allow_blockers,
            } => run_replay_scenarios_command(
                path,
                ranking_config_dir,
                algorithm_version,
                json,
                allow_blockers,
                format_replay_scenario_summary,
                "replay scenarios",
            )?,
        },
        Command::Eval { target } => match target {
            EvalCommand::Golden {
                scenario_path,
                profile_id,
                profiles_path,
                ranking_config_dir,
                algorithm_version,
                json,
                allow_blockers,
                persist,
            } => {
                run_eval_golden_command(EvalGoldenCommandInput {
                    scenario_path,
                    profile_id,
                    profiles_path,
                    ranking_config_dir,
                    algorithm_version,
                    json,
                    allow_blockers,
                    persist,
                })
                .await?
            }
            #[cfg(feature = "storage-backends")]
            EvalCommand::Replay {
                limit,
                fail_on_mismatch,
            } => {
                run_replay_evaluate_command(
                    limit,
                    fail_on_mismatch,
                    format_eval_replay_summary,
                    "eval replay",
                )
                .await?
            }
        },
        #[cfg(feature = "storage-backends")]
        Command::Explain { target } => match target {
            ExplainCommand::Trace { id, json } => {
                let settings = explain_trace_settings()?;
                let report = run_explain_trace(&settings, id).await?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!("{}", format_explain_trace_report(&report));
                }
            }
        },
        Command::Doctor { target } => match target {
            DoctorCommand::RankingConfig {
                path,
                profiles_path,
                json,
            } => {
                let report = build_config_lint_report(path, profiles_path)?;
                let summary = ranking_config_doctor_summary_from_lint(
                    report.active_profile_id,
                    report.fixture_set_id,
                    report.ranking_summary,
                    report.profile_summary,
                );
                if json {
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                } else {
                    println!("{}", format_ranking_config_doctor_summary(&summary));
                }
            }
            DoctorCommand::ExplanationIntegrity {
                path,
                ranking_config_dir,
                algorithm_version,
                json,
                allow_blockers,
            } => {
                let ranking_config_dir = match ranking_config_dir {
                    Some(path) => resolve_runtime_path(path),
                    None => env_path_or_default(
                        "RANKING_CONFIG_DIR",
                        PathBuf::from(DEFAULT_RANKING_CONFIG_DIR),
                    )?,
                };
                let algorithm_version = algorithm_version
                    .or(config::env_optional_non_empty("ALGORITHM_VERSION")?)
                    .unwrap_or_else(|| DEFAULT_ALGORITHM_VERSION.to_string());
                let path = resolve_runtime_path(path);
                let summary =
                    run_explanation_integrity_doctor(path, ranking_config_dir, &algorithm_version)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                } else {
                    println!("{}", format_explanation_integrity_doctor_summary(&summary));
                }
                if summary.has_blockers() && !allow_blockers {
                    anyhow::bail!(
                        "doctor explanation-integrity had blocker checks={}",
                        summary.blockers
                    );
                }
            }
            DoctorCommand::ProfilePack {
                profiles_path,
                json,
                persist,
            } => {
                let profiles_path = profiles_path.unwrap_or(env_path_or_default(
                    "PROFILE_PACKS_DIR",
                    PathBuf::from(DEFAULT_PROFILE_PACKS_DIR),
                )?);
                let summary = run_profile_pack_doctor(&profiles_path)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                } else {
                    println!("{}", format_profile_pack_doctor_summary(&summary));
                }
                if persist {
                    let lint_summary = lint_profile_pack_dir(profiles_path)?;
                    let persisted =
                        persist_profile_lint_summary(&lint_summary, "doctor profile-pack").await?;
                    print_profile_registry_persisted(persisted.len(), &persisted, json);
                }
            }
            DoctorCommand::IngestQuality {
                profiles_path,
                json,
            } => {
                let profiles_path = profiles_path.unwrap_or(env_path_or_default(
                    "PROFILE_PACKS_DIR",
                    PathBuf::from(DEFAULT_PROFILE_PACKS_DIR),
                )?);
                let summary = run_ingest_quality_doctor(&profiles_path)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                } else {
                    println!("{}", format_ingest_quality_doctor_summary(&summary));
                }
            }
            DoctorCommand::ContextCoverage { path, json } => {
                let path = resolve_runtime_path(path);
                let summary = run_context_coverage_doctor(path)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                } else {
                    println!("{}", format_context_coverage_doctor_summary(&summary));
                }
                if summary.has_blockers() {
                    anyhow::bail!(
                        "doctor context-coverage had blocker checks: {}",
                        summary
                            .blocker_message()
                            .unwrap_or_else(|| "unknown".to_string())
                    );
                }
            }
            DoctorCommand::RetrievalParity { json } => {
                let summary = run_retrieval_parity_doctor();
                if json {
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                } else {
                    println!("{}", format_retrieval_parity_doctor_summary(&summary));
                }
                if summary.has_blockers() {
                    anyhow::bail!(
                        "doctor retrieval-parity had blocker checks={}",
                        summary.failed
                    );
                }
            }
            DoctorCommand::StorageCompatibility { json } => {
                let summary = run_storage_compatibility_doctor();
                if json {
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                } else {
                    println!("{}", format_storage_compatibility_doctor_summary(&summary));
                }
            }
        },
        Command::Profile { target } => match target {
            ProfileCommand::List { profiles_path } => {
                let profiles_path = profiles_path.unwrap_or(env_path_or_default(
                    "PROFILE_PACKS_DIR",
                    PathBuf::from(DEFAULT_PROFILE_PACKS_DIR),
                )?);
                let summary = lint_profile_pack_dir(profiles_path)?;
                println!("{}", format_profile_list_summary(&summary));
            }
            ProfileCommand::Validate {
                profiles_path,
                persist,
            } => {
                let profiles_path = profiles_path.unwrap_or(env_path_or_default(
                    "PROFILE_PACKS_DIR",
                    PathBuf::from(DEFAULT_PROFILE_PACKS_DIR),
                )?);
                let summary = lint_profile_pack_dir(profiles_path)?;
                println!("{}", format_profile_validate_summary(&summary));
                if persist {
                    let persisted =
                        persist_profile_lint_summary(&summary, "profile validate").await?;
                    println!(
                        "profile registry persisted: profile_packs={}, manifest_lineage_ids={}",
                        persisted.len(),
                        format_i64_order(&persisted)
                    );
                }
            }
            ProfileCommand::Inspect {
                profile_id,
                profiles_path,
            } => {
                let profiles_path = profiles_path.unwrap_or(env_path_or_default(
                    "PROFILE_PACKS_DIR",
                    PathBuf::from(DEFAULT_PROFILE_PACKS_DIR),
                )?);
                let registry = ProfilePackRegistry::new(&profiles_path);
                let env_profile_id = config::env_optional_non_empty("PROFILE_ID")?;
                let profile_id = registry.selected_profile_id(
                    profile_id.as_deref().or(env_profile_id.as_deref()),
                    DEFAULT_PROFILE_ID,
                )?;
                let fixture_set_id = config::env_optional_non_empty("PROFILE_FIXTURE_SET_ID")?;
                let manifest_path = registry.manifest_path_for_profile_id(&profile_id)?;
                let (manifest, lint_file) = load_and_lint_profile_pack_file(&manifest_path)?;
                let runtime_selection = resolve_linted_profile_pack_runtime_selection(
                    &manifest_path,
                    &manifest,
                    &lint_file,
                    fixture_set_id.as_deref(),
                )?;
                println!(
                    "{}",
                    format_profile_inspect_summary(
                        &manifest_path,
                        &manifest,
                        &lint_file,
                        &runtime_selection
                    )
                );
            }
        },
        #[cfg(feature = "storage-backends")]
        Command::Context { target } => match target {
            ContextCommand::Inspect {
                request_id,
                user_id,
                station_id,
                line_id,
                line_name,
                country,
                prefecture_code,
                prefecture_name,
                city_code,
                city_name,
                json,
            } => {
                let settings = AppSettings::from_env_without_profile_pack()?;
                let summary = run_context_inspect(
                    &settings,
                    ContextInspectInput {
                        request_id,
                        user_id,
                        station_id,
                        line_id,
                        line_name,
                        area: context::AreaContextInput {
                            country,
                            prefecture_code,
                            prefecture_name,
                            city_code,
                            city_name,
                        },
                    },
                )
                .await?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                } else {
                    println!("{}", format_context_inspect_summary(&summary));
                }
            }
        },
        Command::Config { target } => match target {
            ConfigCommand::Lint {
                path,
                profiles_path,
            } => {
                let report = build_config_lint_report(path, profiles_path)?;
                println!(
                    "{}",
                    format_config_lint_summary(
                        report.active_profile_id.as_deref(),
                        report.fixture_set_id.as_deref(),
                        &report.ranking_summary,
                        &report.profile_summary
                    )
                );
            }
        },
        Command::SourceManifest { target } => match target {
            SourceManifestCommand::Lint { path } => {
                let summary = lint_source_manifest_dir(path)?;
                println!("{}", format_source_manifest_lint_summary(&summary));
            }
        },
        #[cfg(feature = "storage-backends")]
        Command::Jobs { target } => match target {
            JobsCommand::List { limit } => {
                let settings = AppSettings::from_env_without_profile_pack()?;
                let summary = run_job_list(&settings, limit).await?;
                println!("{}", format_job_list(&summary));
            }
            JobsCommand::Inspect { id } => {
                let settings = AppSettings::from_env_without_profile_pack()?;
                let inspection = run_job_inspect(&settings, id).await?;
                println!("{}", format_job_inspection(&inspection));
            }
            JobsCommand::Retry { id } => {
                let settings = AppSettings::from_env_without_profile_pack()?;
                let summary = run_job_retry(&settings, id).await?;
                println!("{}", format_job_mutation_summary("retry", &summary));
            }
            JobsCommand::Due { id } => {
                let settings = AppSettings::from_env_without_profile_pack()?;
                let summary = run_job_due(&settings, id).await?;
                println!("{}", format_job_mutation_summary("due", &summary));
            }
            JobsCommand::Enqueue {
                job_type,
                payload,
                max_attempts,
            } => {
                let settings = AppSettings::from_env_without_profile_pack()?;
                let summary = run_job_enqueue(&settings, &job_type, &payload, max_attempts).await?;
                println!("{}", format_job_enqueue_summary(&summary));
            }
        },
        #[cfg(feature = "api-docs")]
        Command::DumpOpenapi { output } => {
            let raw = serde_json::to_string_pretty(&openapi::api_doc())?;
            if let Some(parent) = output.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(output, raw)?;
        }
    }

    Ok(())
}

#[cfg(feature = "storage-backends")]
fn explain_trace_settings() -> anyhow::Result<AppSettings> {
    match AppSettings::from_env() {
        Ok(settings) => Ok(settings),
        Err(profile_error) => {
            let settings = AppSettings::from_env_without_profile_pack().with_context(|| {
                format!(
                    "failed to load DB-only explain trace settings after profile-aware settings failed: {profile_error:#}"
                )
            })?;
            eprintln!(
                "warning: explain trace is using DB-only settings because profile pack resolution failed: {profile_error:#}"
            );
            Ok(settings)
        }
    }
}

#[cfg(feature = "storage-backends")]
async fn run_replay_evaluate_command(
    limit: i64,
    fail_on_mismatch: bool,
    format_summary: fn(&cli::ReplayEvaluationSummary) -> String,
    failure_label: &str,
) -> anyhow::Result<()> {
    let settings = AppSettings::from_env()?;
    let summary = run_replay_evaluate(&settings, limit).await?;
    println!("{}", format_summary(&summary));
    if fail_on_mismatch && (summary.mismatched > 0 || summary.failed > 0) {
        anyhow::bail!(
            "{failure_label} had mismatches={} failed={}",
            summary.mismatched,
            summary.failed
        );
    }
    Ok(())
}

struct EvalGoldenScenarioSelection {
    profile_id: Option<String>,
    profile_manifest: Option<PathBuf>,
    ranking_config_dir: PathBuf,
    scenario_source: ReplayScenarioSource,
}

struct EvalGoldenCommandInput {
    scenario_path: Option<PathBuf>,
    profile_id: Option<String>,
    profiles_path: Option<PathBuf>,
    ranking_config_dir: Option<PathBuf>,
    algorithm_version: Option<String>,
    json: bool,
    allow_blockers: bool,
    persist: bool,
}

async fn run_eval_golden_command(input: EvalGoldenCommandInput) -> anyhow::Result<()> {
    let algorithm_version = resolve_algorithm_version(input.algorithm_version)?;
    let selection = resolve_eval_golden_scenario_selection(
        input.scenario_path,
        input.profile_id,
        input.profiles_path,
        input.ranking_config_dir,
    )?;
    let profile_manifest = selection.profile_manifest.clone();
    let summary = run_replay_scenarios_with_source(
        &selection.ranking_config_dir,
        &algorithm_version,
        selection.profile_id,
        selection.scenario_source,
    )?;
    if input.json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        println!("{}", format_eval_golden_summary(&summary));
    }
    if input.persist {
        let evaluation_run_id =
            persist_eval_golden_summary(&summary, &algorithm_version, profile_manifest.as_deref())
                .await?;
        print_evaluation_run_persisted(evaluation_run_id, input.json);
    }
    if summary.has_blockers() && !input.allow_blockers {
        anyhow::bail!("eval golden had blocker checks={}", summary.blockers);
    }
    Ok(())
}

fn resolve_eval_golden_scenario_selection(
    scenario_path: Option<PathBuf>,
    profile_id: Option<String>,
    profiles_path: Option<PathBuf>,
    ranking_config_dir: Option<PathBuf>,
) -> anyhow::Result<EvalGoldenScenarioSelection> {
    let scenario_path = scenario_path.map(resolve_runtime_path);
    let profiles_path = profiles_path.map(resolve_runtime_path);
    let requested_profile_id = profile_id.or(config::env_optional_non_empty("PROFILE_ID")?);
    ensure_profiles_path_has_profile_selector(
        profiles_path.as_deref(),
        requested_profile_id.as_deref(),
    )?;
    let ranking_config_override = resolve_eval_ranking_config_override(ranking_config_dir)?;
    if let (Some(profile_id), Some(scenario_path)) =
        (requested_profile_id.as_deref(), scenario_path.as_ref())
    {
        let profiles_path = resolve_eval_profiles_path(profiles_path.clone())?;
        let registry = ProfilePackRegistry::new(&profiles_path);
        let profile_id = registry.selected_profile_id(Some(profile_id), DEFAULT_PROFILE_ID)?;
        let profile_selection = registry.ranking_selection_with_ranking_config_dir(
            &profile_id,
            ranking_config_override.as_deref(),
        )?;

        return Ok(EvalGoldenScenarioSelection {
            profile_id: Some(profile_id),
            profile_manifest: Some(profile_selection.profile_pack_manifest),
            ranking_config_dir: profile_selection.ranking_config_dir,
            scenario_source: ReplayScenarioSource::explicit_path(scenario_path.clone())
                .with_reason_catalog_path(profile_selection.reason_catalog_path),
        });
    }

    let profile_selection = requested_profile_id
        .as_deref()
        .map(|profile_id| {
            let profiles_path = resolve_eval_profiles_path(profiles_path.clone())?;
            let registry = ProfilePackRegistry::new(&profiles_path);
            let profile_id = registry.selected_profile_id(Some(profile_id), DEFAULT_PROFILE_ID)?;
            registry.evaluation_selection_with_ranking_config_dir(
                &profile_id,
                ranking_config_override.as_deref(),
            )
        })
        .transpose()?;

    let ranking_config_dir = profile_selection
        .as_ref()
        .map(|selection| selection.ranking_config_dir.clone())
        .or(ranking_config_override)
        .unwrap_or_else(|| resolve_runtime_path(DEFAULT_RANKING_CONFIG_DIR));

    if let Some(profile_selection) = profile_selection {
        let profile_id = Some(profile_selection.profile_id.clone());
        return Ok(EvalGoldenScenarioSelection {
            profile_id,
            profile_manifest: Some(profile_selection.profile_pack_manifest.clone()),
            ranking_config_dir,
            scenario_source: ReplayScenarioSource::profile_evaluation(
                profile_selection.scenario_pack,
                profile_selection.profile_pack_manifest,
                profile_selection.reason_catalog_path,
                profile_selection.pairwise_pack,
            ),
        });
    }

    let scenario_source = match scenario_path {
        Some(path) => ReplayScenarioSource::explicit_path(path),
        None => {
            let path = resolve_runtime_path(DEFAULT_REPLAY_SCENARIO_PATH);
            ReplayScenarioSource::default_path(path)
        }
    };

    Ok(EvalGoldenScenarioSelection {
        profile_id: None,
        profile_manifest: None,
        ranking_config_dir,
        scenario_source,
    })
}

#[cfg(feature = "storage-backends")]
async fn persist_profile_lint_summary(
    summary: &ProfilePackLintSummary,
    command_name: &str,
) -> anyhow::Result<Vec<i64>> {
    let settings = AppSettings::from_env_without_profile_pack()?;
    let repository = PgRepository::new(&settings.database_url);
    let mut lineage_ids = Vec::new();
    for lint_file in &summary.files {
        let (manifest, loaded_lint_file) = load_and_lint_profile_pack_file(&lint_file.path)?;
        let record = profile_manifest_record(&manifest, &loaded_lint_file)?;
        let lineage_id = repository.upsert_profile_manifest(&record).await?;
        repository
            .record_profile_compatibility_status(&ProfileCompatibilityStatusRecord {
                profile_id: record.profile_id.clone(),
                compatibility_level: record.compatibility_level.clone(),
                status: ProfileCompatibilityStatus::Valid,
                evidence: serde_json::json!({
                    "command": command_name,
                    "manifest_lineage_id": lineage_id,
                    "ranking_config_dir": record.ranking_config_dir,
                    "reason_catalog_path": record.reason_catalog_path,
                    "fixture_count": record.fixture_count,
                    "connector_count": record.connector_count,
                    "evaluation_reference_count": record.evaluation_reference_count
                }),
            })
            .await?;
        lineage_ids.push(lineage_id);
    }
    Ok(lineage_ids)
}

#[cfg(not(feature = "storage-backends"))]
async fn persist_profile_lint_summary(
    _summary: &ProfilePackLintSummary,
    _command_name: &str,
) -> anyhow::Result<Vec<i64>> {
    anyhow::bail!("profile registry persistence requires the storage-backends feature")
}

#[cfg(feature = "storage-backends")]
async fn persist_eval_golden_summary(
    summary: &cli::ReplayScenarioSummary,
    algorithm_version: &str,
    profile_manifest: Option<&Path>,
) -> anyhow::Result<i64> {
    let settings = AppSettings::from_env_without_profile_pack()?;
    let repository = PgRepository::new(&settings.database_url);
    let profile_manifest_lineage_id = match profile_manifest {
        Some(profile_manifest) => {
            let (manifest, lint_file) = load_and_lint_profile_pack_file(profile_manifest)?;
            let record = profile_manifest_record(&manifest, &lint_file)?;
            let lineage_id = repository.upsert_profile_manifest(&record).await?;
            repository
                .record_profile_compatibility_status(&ProfileCompatibilityStatusRecord {
                    profile_id: record.profile_id.clone(),
                    compatibility_level: record.compatibility_level.clone(),
                    status: ProfileCompatibilityStatus::Valid,
                    evidence: serde_json::json!({
                        "command": "eval golden",
                        "manifest_lineage_id": lineage_id,
                        "scenario_source": summary.scenario_source.kind.as_str(),
                        "scenario_path": summary.scenario_source.path.display().to_string()
                    }),
                })
                .await?;
            Some(lineage_id)
        }
        None => None,
    };
    let status = if summary.blockers > 0 {
        EvaluationRunStatus::Blocked
    } else {
        EvaluationRunStatus::Passed
    };
    let run = EvaluationRunRecord {
        profile_id: summary.profile_id.clone(),
        profile_manifest_lineage_id,
        run_kind: EvaluationRunKind::Golden,
        scenario_source_kind: summary.scenario_source.kind.as_str().to_string(),
        scenario_path: summary.scenario_source.path.display().to_string(),
        pairwise_pack_path: summary
            .scenario_source
            .pairwise_pack
            .as_ref()
            .map(|path| path.display().to_string()),
        algorithm_version: algorithm_version.to_string(),
        status,
        scenarios: usize_to_i32("scenarios", summary.scenarios)?,
        passed: usize_to_i32("passed", summary.passed)?,
        blocked: usize_to_i32("blocked", summary.blocked)?,
        blockers: usize_to_i32("blockers", summary.blockers)?,
        warnings: usize_to_i32("warnings", summary.warnings)?,
        summary_payload: serde_json::to_value(summary)?,
        cases: summary
            .cases
            .iter()
            .map(evaluation_run_case_record)
            .collect::<anyhow::Result<Vec<_>>>()?,
    };
    repository.record_evaluation_run(&run).await
}

#[cfg(not(feature = "storage-backends"))]
async fn persist_eval_golden_summary(
    _summary: &cli::ReplayScenarioSummary,
    _algorithm_version: &str,
    _profile_manifest: Option<&Path>,
) -> anyhow::Result<i64> {
    anyhow::bail!("evaluation run persistence requires the storage-backends feature")
}

#[cfg(feature = "storage-backends")]
fn profile_manifest_record(
    manifest: &ProfilePackManifest,
    lint_file: &ProfilePackLintFile,
) -> anyhow::Result<ProfileManifestRecord> {
    let raw = std::fs::read(&lint_file.path)
        .with_context(|| format!("failed to read profile pack {}", lint_file.path.display()))?;
    let checksum = format!("{:x}", Sha256::digest(&raw));
    Ok(ProfileManifestRecord {
        profile_id: manifest.profile_id.clone(),
        display_name: manifest.display_name.clone(),
        schema_version: manifest.schema_version.try_into()?,
        manifest_kind: manifest.kind.as_str().to_string(),
        manifest_version: manifest.manifest_version.try_into()?,
        compatibility_level: manifest.compatibility_level.as_str().to_string(),
        default_locale: manifest.default_locale.clone(),
        description: manifest.description.clone(),
        manifest_path: lint_file
            .path
            .canonicalize()
            .with_context(|| {
                format!(
                    "failed to canonicalize profile pack {}",
                    lint_file.path.display()
                )
            })?
            .display()
            .to_string(),
        manifest_checksum_sha256: checksum,
        manifest_payload: serde_json::to_value(manifest)?,
        ranking_config_dir: canonicalize_profile_registry_path(
            "ranking config dir",
            &lint_file.ranking_config_dir,
        )?,
        reason_catalog_path: canonicalize_profile_registry_path(
            "reason catalog",
            &lint_file.reason_catalog_path,
        )?,
        content_kind_registry: lint_file
            .content_kind_registry
            .iter()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        supported_content_kinds: lint_file
            .supported_content_kinds
            .iter()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        context_inputs: manifest
            .context_inputs
            .iter()
            .map(|input| input.as_str().to_string())
            .collect(),
        placements: lint_file
            .placements
            .iter()
            .map(|placement| placement.as_str().to_string())
            .collect(),
        fallback_policy: manifest.fallback_policy.display(),
        fixture_count: usize_to_i32("fixture_count", lint_file.fixture_count)?,
        connector_count: usize_to_i32("connector_count", lint_file.connector_count)?,
        evaluation_reference_count: usize_to_i32(
            "evaluation_reference_count",
            lint_file.evaluation_reference_count,
        )?,
    })
}

#[cfg(feature = "storage-backends")]
fn canonicalize_profile_registry_path(field: &str, path: &Path) -> anyhow::Result<String> {
    Ok(path
        .canonicalize()
        .with_context(|| {
            format!(
                "failed to canonicalize profile registry {field} {}",
                path.display()
            )
        })?
        .display()
        .to_string())
}

#[cfg(feature = "storage-backends")]
fn evaluation_run_case_record(
    case: &cli::ReplayScenarioCase,
) -> anyhow::Result<EvaluationRunCaseRecord> {
    Ok(EvaluationRunCaseRecord {
        case_id: case.id.clone(),
        title: case.title.clone(),
        path: case.path.display().to_string(),
        status: evaluation_run_case_status(case.status),
        expected_fallback_stage: case.expected_fallback_stage.clone(),
        actual_fallback_stage: case.actual_fallback_stage.clone(),
        expected_order: case.expected_order.clone(),
        actual_order: case.actual_order.clone(),
        checks_payload: serde_json::to_value(&case.checks)?,
    })
}

#[cfg(feature = "storage-backends")]
fn evaluation_run_case_status(status: cli::ReplayScenarioStatus) -> EvaluationRunCaseStatus {
    match status {
        cli::ReplayScenarioStatus::Passed => EvaluationRunCaseStatus::Passed,
        cli::ReplayScenarioStatus::Blocked => EvaluationRunCaseStatus::Blocked,
    }
}

#[cfg(feature = "storage-backends")]
fn usize_to_i32(field: &str, value: usize) -> anyhow::Result<i32> {
    value
        .try_into()
        .with_context(|| format!("{field} is too large for storage"))
}

fn ensure_profiles_path_has_profile_selector(
    profiles_path: Option<&Path>,
    requested_profile_id: Option<&str>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        requested_profile_id.is_some() || profiles_path.is_none(),
        "--profiles-path requires --profile-id or PROFILE_ID"
    );
    Ok(())
}

fn resolve_eval_profiles_path(profiles_path: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    profiles_path.map(Ok).unwrap_or_else(|| {
        env_path_or_default(
            "PROFILE_PACKS_DIR",
            PathBuf::from(DEFAULT_PROFILE_PACKS_DIR),
        )
    })
}

fn resolve_eval_ranking_config_override(
    ranking_config_dir: Option<PathBuf>,
) -> anyhow::Result<Option<PathBuf>> {
    if let Some(path) = ranking_config_dir {
        return Ok(Some(resolve_runtime_path(path)));
    }
    config::env_path_optional("RANKING_CONFIG_DIR")
}

fn resolve_algorithm_version(algorithm_version: Option<String>) -> anyhow::Result<String> {
    Ok(algorithm_version
        .or(config::env_optional_non_empty("ALGORITHM_VERSION")?)
        .unwrap_or_else(|| DEFAULT_ALGORITHM_VERSION.to_string()))
}

#[cfg(feature = "storage-backends")]
fn run_replay_scenarios_command(
    path: PathBuf,
    ranking_config_dir: Option<PathBuf>,
    algorithm_version: Option<String>,
    json: bool,
    allow_blockers: bool,
    format_summary: fn(&cli::ReplayScenarioSummary) -> String,
    failure_label: &str,
) -> anyhow::Result<()> {
    let ranking_config_dir = match ranking_config_dir {
        Some(path) => resolve_runtime_path(path),
        None => env_path_or_default(
            "RANKING_CONFIG_DIR",
            PathBuf::from(DEFAULT_RANKING_CONFIG_DIR),
        )?,
    };
    let algorithm_version = resolve_algorithm_version(algorithm_version)?;
    let path = resolve_runtime_path(path);
    let summary = run_replay_scenarios(path, ranking_config_dir, &algorithm_version)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        println!("{}", format_summary(&summary));
    }
    if summary.has_blockers() && !allow_blockers {
        anyhow::bail!("{failure_label} had blocker checks={}", summary.blockers);
    }
    Ok(())
}

struct ConfigLintReport {
    active_profile_id: Option<String>,
    fixture_set_id: Option<String>,
    ranking_summary: RankingConfigLintSummary,
    profile_summary: ProfilePackLintSummary,
}

fn build_config_lint_report(
    path: Option<PathBuf>,
    profiles_path: Option<PathBuf>,
) -> anyhow::Result<ConfigLintReport> {
    let profiles_path = profiles_path.unwrap_or(env_path_or_default(
        "PROFILE_PACKS_DIR",
        PathBuf::from(DEFAULT_PROFILE_PACKS_DIR),
    )?);
    let ranking_config_dir_override = config::env_path_optional("RANKING_CONFIG_DIR")?;
    let path = path.map(resolve_runtime_path);
    let needs_active_profile = path.is_none() && ranking_config_dir_override.is_none();
    let active_profile = active_profile_selection_for_lint(&profiles_path)
        .map(Some)
        .or_else(|error| {
            if needs_active_profile {
                Err(error)
            } else {
                Ok(None)
            }
        })?;
    let path = path
        .or(ranking_config_dir_override)
        .or_else(|| {
            active_profile
                .as_ref()
                .map(|profile| profile.ranking_config_dir.clone())
        })
        .context("active profile selection is required to choose ranking config dir")?;
    let profile_summary = lint_profile_pack_dir(profiles_path)?;
    let ranking_summary = match cached_ranking_summary_for_path(&profile_summary, &path)? {
        Some(summary) => ranking_summary_with_base_path(summary, &path),
        None => lint_ranking_config_dir(&path)?,
    };
    Ok(ConfigLintReport {
        active_profile_id: active_profile
            .as_ref()
            .map(|profile| profile.profile_id.clone()),
        fixture_set_id: active_profile
            .as_ref()
            .and_then(|profile| profile.fixture_set_id.clone()),
        ranking_summary,
        profile_summary,
    })
}

fn active_profile_selection_for_lint(
    profiles_path: &Path,
) -> anyhow::Result<config::ProfilePackRuntimeSelection> {
    let registry = ProfilePackRegistry::new(profiles_path);
    let requested_profile_id = config::env_optional_non_empty("PROFILE_ID")?;
    let profile_id =
        registry.selected_profile_id(requested_profile_id.as_deref(), DEFAULT_PROFILE_ID)?;
    let fixture_set_id = config::env_optional_non_empty("PROFILE_FIXTURE_SET_ID")?;
    registry.runtime_selection(&profile_id, fixture_set_id.as_deref())
}

fn env_path_or_default(name: &str, default: PathBuf) -> anyhow::Result<PathBuf> {
    Ok(config::env_path_optional(name)?.unwrap_or_else(|| resolve_runtime_path(default)))
}

fn cached_ranking_summary_for_path(
    profiles: &ProfilePackLintSummary,
    path: &Path,
) -> anyhow::Result<Option<RankingConfigLintSummary>> {
    let canonical_path = path.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize ranking config dir {}",
            path.display()
        )
    })?;
    Ok(profiles
        .ranking_configs
        .iter()
        .find(|summary| summary.path == canonical_path)
        .cloned())
}

fn ranking_summary_with_base_path(
    mut summary: RankingConfigLintSummary,
    path: &Path,
) -> RankingConfigLintSummary {
    for file in &mut summary.files {
        if let Some(file_name) = file.path.file_name() {
            file.path = path.join(file_name);
        }
    }
    summary
}

fn format_config_lint_summary(
    active_profile_id: Option<&str>,
    fixture_set_id: Option<&str>,
    ranking: &RankingConfigLintSummary,
    profiles: &ProfilePackLintSummary,
) -> String {
    let active_profile = active_profile_id.unwrap_or("not-selected");
    let fixture_set = fixture_set_id.unwrap_or("none");
    let mut lines = vec![format!(
        "config lint ok: active_profile_id={}, fixture_set_id={}, ranking_files={}, profile_packs={}, profile_version={}",
        active_profile,
        fixture_set,
        ranking.files.len(),
        profiles.files.len(),
        ranking.profile_version
    )];
    lines.push("ranking files:".to_string());
    lines.extend(ranking.files.iter().map(|file| {
        format!(
            "- {} schema_version={} kind={}",
            file.path.display(),
            file.schema_version,
            file.kind.as_str()
        )
    }));
    lines.push("profile packs:".to_string());
    lines.extend(profiles.files.iter().map(format_profile_lint_file_line));
    lines.join("\n")
}

fn format_profile_list_summary(summary: &ProfilePackLintSummary) -> String {
    let mut lines = vec![format!(
        "profile list ok: profile_packs={}",
        summary.files.len()
    )];
    lines.extend(summary.files.iter().map(format_profile_lint_file_line));
    lines.join("\n")
}

fn format_profile_validate_summary(summary: &ProfilePackLintSummary) -> String {
    let mut lines = vec![format!(
        "profile validate ok: profile_packs={}, ranking_config_dirs={}",
        summary.files.len(),
        summary.ranking_configs.len()
    )];
    lines.extend(summary.files.iter().map(format_profile_lint_file_line));
    lines.join("\n")
}

fn format_profile_lint_file_line(file: &ProfilePackLintFile) -> String {
    let content_kind_registry = file
        .content_kind_registry
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let content_kinds = file
        .supported_content_kinds
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let runtime_executable_content_kinds =
        format_content_kind_refs(file.runtime_executable_content_kinds.as_slice());
    let registry_only_content_kinds =
        format_content_kind_refs(file.registry_only_content_kinds.as_slice());
    let placements = file
        .placements
        .iter()
        .map(|placement| placement.as_str())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "- {} profile_id={} schema_version={} kind={} manifest_version={} compatibility_level={} content_kind_registry={} content_kinds={} runtime_executable_content_kinds={} registry_only_content_kinds={} placements={} reason_catalog_locales={} reasons={} fixtures={} connectors={} evaluation_refs={} source_manifests={} event_csv_examples={} archive_sources={} optional_crawler_manifests={}",
        file.path.display(),
        file.profile_id,
        file.schema_version,
        file.kind.as_str(),
        file.manifest_version,
        file.compatibility_level.as_str(),
        content_kind_registry,
        content_kinds,
        runtime_executable_content_kinds,
        registry_only_content_kinds,
        placements,
        file.reason_catalog_locale_count,
        file.reason_count,
        file.fixture_count,
        file.connector_count,
        file.evaluation_reference_count,
        file.source_manifest_count,
        file.event_csv_example_count,
        file.archive_source_count,
        file.optional_crawler_manifest_count
    )
}

fn format_content_kind_refs(content_kinds: &[domain::ContentKindRef]) -> String {
    if content_kinds.is_empty() {
        "-".to_string()
    } else {
        content_kinds
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn format_i64_order(values: &[i64]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn print_profile_registry_persisted(profile_packs: usize, lineage_ids: &[i64], json: bool) {
    let message = profile_registry_persisted_message(profile_packs, lineage_ids);
    if json {
        eprintln!("{message}");
    } else {
        println!("{message}");
    }
}

fn profile_registry_persisted_message(profile_packs: usize, lineage_ids: &[i64]) -> String {
    format!(
        "profile registry persisted: profile_packs={}, manifest_lineage_ids={}",
        profile_packs,
        format_i64_order(lineage_ids)
    )
}

fn print_evaluation_run_persisted(evaluation_run_id: i64, json: bool) {
    let message = evaluation_run_persisted_message(evaluation_run_id);
    if json {
        eprintln!("{message}");
    } else {
        println!("{message}");
    }
}

fn evaluation_run_persisted_message(evaluation_run_id: i64) -> String {
    format!("evaluation run persisted: id={evaluation_run_id}")
}

fn format_profile_inspect_summary(
    manifest_path: &Path,
    manifest: &ProfilePackManifest,
    lint_file: &ProfilePackLintFile,
    runtime_selection: &config::ProfilePackRuntimeSelection,
) -> String {
    let content_kind_registry = lint_file
        .content_kind_registry
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let content_kinds = manifest
        .supported_content_kinds
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let runtime_executable_content_kinds =
        format_content_kind_refs(lint_file.runtime_executable_content_kinds.as_slice());
    let registry_only_content_kinds =
        format_content_kind_refs(lint_file.registry_only_content_kinds.as_slice());
    let context_inputs = manifest
        .context_inputs
        .iter()
        .map(|input| input.as_str())
        .collect::<Vec<_>>()
        .join(",");

    let mut lines = vec![
        format!(
            "profile inspect ok: profile_id={} display_name={:?}",
            manifest.profile_id, manifest.display_name
        ),
        format!("manifest={}", manifest_path.display()),
        format!(
            "schema_version={} kind={} manifest_version={} compatibility_level={}",
            manifest.schema_version,
            manifest.kind.as_str(),
            manifest.manifest_version,
            manifest.compatibility_level.as_str()
        ),
        format!("content_kind_registry={content_kind_registry}"),
        format!("content_kinds={content_kinds}"),
        format!("runtime_executable_content_kinds={runtime_executable_content_kinds}"),
        format!("registry_only_content_kinds={registry_only_content_kinds}"),
        format!("context_inputs={context_inputs}"),
        format!("fallback_policy={}", manifest.fallback_policy.display()),
        format!("ranking_config_dir={}", manifest.ranking_config_dir),
        format!(
            "reason_catalog={} reason_catalog_locales={} reasons={}",
            manifest.reason_catalog.display(),
            lint_file.reason_catalog_locale_count,
            lint_file.reason_count
        ),
        format!(
            "runtime_reason_catalog_path={}",
            runtime_selection.reason_catalog_path.display()
        ),
        format!(
            "runtime_ranking_config_dir={}",
            runtime_selection.ranking_config_dir.display()
        ),
        format!(
            "runtime_fallback_config_path={}",
            runtime_selection
                .fallback_config_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "none".to_string())
        ),
        format!("article_support={}", manifest.article_support.as_str()),
        format!(
            "placements={}",
            manifest
                .placements
                .iter()
                .map(|placement| placement.as_str())
                .collect::<Vec<_>>()
                .join(",")
        ),
        format!(
            "connectors={} evaluation_refs={}",
            manifest.connectors.len(),
            lint_file.evaluation_reference_count
        ),
    ];

    lines.push(format!(
        "runtime_fixture_set_id={}",
        runtime_selection
            .fixture_set_id
            .as_deref()
            .unwrap_or("none")
    ));
    lines.push(format!(
        "runtime_fixture_dir={}",
        runtime_selection
            .fixture_dir
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "none".to_string())
    ));

    if let Some(description) = manifest.description.as_deref() {
        lines.push(format!("description={description:?}"));
    }

    lines.push("fixtures:".to_string());
    if manifest.fixtures.is_empty() {
        lines.push("- none".to_string());
    } else {
        lines.extend(manifest.fixtures.iter().map(|fixture| {
            format!(
                "- fixture_set_id={} path={}",
                fixture.fixture_set_id, fixture.path
            )
        }));
    }

    lines.push("connector_registry:".to_string());
    if lint_file.connector_registry.is_empty() {
        lines.push("- none".to_string());
    } else {
        lines.extend(lint_file.connector_registry.iter().map(|connector| {
            format!(
                "- type={} source_class={} manifest_kind={} source_id={} field_mapping={} profile_compatibility={} safety=local_reference_only:{},dynamic_loading_enabled:{},live_fetch_default:{},allowlist_required:{} manifest={}",
                connector.connector_type.as_str(),
                connector.source_class.as_str(),
                connector.manifest_kind,
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
            )
        }));
    }

    push_profile_ref_summary(&mut lines, "source_manifests", &manifest.source_manifests);
    push_profile_ref_summary(
        &mut lines,
        "event_csv_examples",
        &manifest.event_csv_examples,
    );
    push_profile_ref_summary(
        &mut lines,
        "optional_crawler_manifests",
        &manifest.optional_crawler_manifests,
    );
    push_profile_ref_summary(&mut lines, "examples", &manifest.examples);

    lines.join("\n")
}

fn push_profile_ref_summary(lines: &mut Vec<String>, label: &str, refs: &[String]) {
    if refs.is_empty() {
        lines.push(format!("{label}=none"));
    } else {
        lines.push(format!("{label}={}", refs.join(",")));
    }
}

fn format_source_manifest_lint_summary(summary: &SourceManifestLintSummary) -> String {
    let mut lines = vec![format!(
        "source manifest lint ok: files={}",
        summary.files.len()
    )];
    lines.extend(summary.files.iter().map(|file| {
        format!(
            "- {} schema_version={} kind={} manifest_version={} source_id={} files={}",
            file.path.display(),
            file.schema_version,
            file.kind.as_str(),
            file.manifest_version,
            file.source_id,
            file.file_count
        )
    }));
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use cli::ReplayScenarioSourceKind;
    use config::{
        ProfileCompatibilityLevel, ProfileConnectorRegistryEntry, ProfileConnectorSafetyMetadata,
        ProfileConnectorType, ProfilePackKind, ProfilePackLintFile, ProfilePackLintSummary,
        ProfileSourceClass, RankingConfigKind, RankingConfigLintFile, RankingConfigLintSummary,
    };
    use domain::PlacementKind;
    use std::{collections::BTreeMap, fs};

    fn repo_ranking_config_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../configs/ranking")
    }

    fn copy_default_ranking_configs(target: &Path) {
        for name in [
            "schools.default.yaml",
            "events.default.yaml",
            "fallback.default.yaml",
            "tracking.default.yaml",
            "placement.home.yaml",
            "placement.search.yaml",
            "placement.detail.yaml",
            "placement.mypage.yaml",
        ] {
            fs::copy(repo_ranking_config_root().join(name), target.join(name))
                .expect("copy ranking config");
        }
    }

    fn write_minimal_profile_reason_catalog(path: &Path, profile_id: &str) {
        fs::write(
            path,
            format!(
                r#"schema_version: 1
kind: profile_reason_catalog
profile_id: {profile_id}
reasons:
  - feature: direct_station_bonus
    reason_code: geo.direct_station
    label: Direct station
    layer: core
"#
            ),
        )
        .expect("write profile reason catalog");
    }

    #[cfg(feature = "storage-backends")]
    #[test]
    fn context_inspect_help_explains_read_only_evidence_flow() {
        let mut command = Cli::command();
        let context = command
            .find_subcommand_mut("context")
            .expect("context command");
        let inspect = context
            .find_subcommand_mut("inspect")
            .expect("context inspect command");
        let mut buffer = Vec::new();
        inspect.write_long_help(&mut buffer).expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("without writing context_resolution_traces"));
        assert!(help.contains("--user-id manual-user-1 --json"));
        assert!(help.contains("recent search evidence"));
        assert!(help.contains("country-only input is ignored"));
    }

    #[cfg(feature = "storage-backends")]
    #[test]
    fn explain_trace_help_points_to_persisted_trace_debugging() {
        let mut command = Cli::command();
        let explain = command
            .find_subcommand_mut("explain")
            .expect("explain command");
        let trace = explain
            .find_subcommand_mut("trace")
            .expect("explain trace command");
        let mut buffer = Vec::new();
        trace.write_long_help(&mut buffer).expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("recommendation_traces row"));
        assert!(help.contains("reason codes"));
        assert!(help.contains("explanation integrity checks"));
        assert!(help.contains("explain trace --id 42 --json"));
    }

    #[test]
    fn top_level_help_lists_eval_command() {
        let mut command = Cli::command();
        let mut buffer = Vec::new();
        command.write_long_help(&mut buffer).expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("eval"));
        assert!(help.contains("Run evaluation quality gates with operator-facing names"));
    }

    #[test]
    fn eval_help_lists_golden_and_replay_aliases() {
        let mut command = Cli::command();
        let eval = command.find_subcommand_mut("eval").expect("eval command");
        let mut buffer = Vec::new();
        eval.write_long_help(&mut buffer).expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("golden"));
        assert!(help.contains("Run the committed golden scenario evaluation"));
        #[cfg(feature = "storage-backends")]
        {
            assert!(help.contains("replay"));
            assert!(help.contains("Replay recent recommendation traces"));
        }
    }

    #[test]
    fn eval_golden_help_points_to_replay_scenarios_alias() {
        let mut command = Cli::command();
        let eval = command.find_subcommand_mut("eval").expect("eval command");
        let golden = eval
            .find_subcommand_mut("golden")
            .expect("eval golden command");
        let mut buffer = Vec::new();
        golden.write_long_help(&mut buffer).expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("operator-facing alias for `replay scenarios`"));
        assert!(help.contains("eval golden --json"));
        assert!(help.contains("--scenario-path"));
        assert!(help.contains("--path"));
        assert!(help.contains("--profile-id"));
        assert!(help.contains("--profiles-path"));
        assert!(help.contains("--ranking-config-dir"));
        assert!(help.contains("--allow-blockers"));
        assert!(help.contains("--persist"));
    }

    #[cfg(feature = "storage-backends")]
    #[test]
    fn eval_replay_help_points_to_replay_evaluate_alias() {
        let mut command = Cli::command();
        let eval = command.find_subcommand_mut("eval").expect("eval command");
        let replay = eval
            .find_subcommand_mut("replay")
            .expect("eval replay command");
        let mut buffer = Vec::new();
        replay.write_long_help(&mut buffer).expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("operator-facing alias for `replay evaluate`"));
        assert!(help.contains("current SQL-only path"));
        assert!(help.contains("--limit"));
        assert!(help.contains("--fail-on-mismatch"));
    }

    #[test]
    fn eval_golden_accepts_legacy_path_option_alias() {
        let cli = Cli::parse_from([
            "cli",
            "eval",
            "golden",
            "--path",
            "configs/evaluation/scenarios",
            "--allow-blockers",
        ]);

        let Command::Eval {
            target:
                EvalCommand::Golden {
                    scenario_path,
                    allow_blockers,
                    ..
                },
        } = cli.command
        else {
            panic!("expected eval golden command");
        };

        assert_eq!(
            scenario_path,
            Some(PathBuf::from("configs/evaluation/scenarios"))
        );
        assert!(allow_blockers);
    }

    #[test]
    fn eval_golden_accepts_profile_selection_options() {
        let cli = Cli::parse_from([
            "cli",
            "eval",
            "golden",
            "--profile-id",
            "school-event-jp",
            "--profiles-path",
            "configs/profiles",
        ]);

        let Command::Eval {
            target:
                EvalCommand::Golden {
                    scenario_path,
                    profile_id,
                    profiles_path,
                    ..
                },
        } = cli.command
        else {
            panic!("expected eval golden command");
        };

        assert_eq!(scenario_path, None);
        assert_eq!(profile_id.as_deref(), Some("school-event-jp"));
        assert_eq!(profiles_path, Some(PathBuf::from("configs/profiles")));
    }

    #[test]
    fn persist_options_are_available_on_profile_and_eval_commands() {
        let eval = Cli::parse_from(["cli", "eval", "golden", "--persist"]);
        let Command::Eval {
            target: EvalCommand::Golden { persist, .. },
        } = eval.command
        else {
            panic!("expected eval golden command");
        };
        assert!(persist);

        let profile = Cli::parse_from(["cli", "profile", "validate", "--persist"]);
        let Command::Profile {
            target: ProfileCommand::Validate { persist, .. },
        } = profile.command
        else {
            panic!("expected profile validate command");
        };
        assert!(persist);

        let doctor = Cli::parse_from(["cli", "doctor", "profile-pack", "--persist"]);
        let Command::Doctor {
            target: DoctorCommand::ProfilePack { persist, .. },
        } = doctor.command
        else {
            panic!("expected doctor profile-pack command");
        };
        assert!(persist);
    }

    #[test]
    fn persistence_messages_are_stable_for_stdout_or_stderr_routing() {
        assert_eq!(
            profile_registry_persisted_message(2, &[7, 11]),
            "profile registry persisted: profile_packs=2, manifest_lineage_ids=7,11"
        );
        assert_eq!(
            evaluation_run_persisted_message(42),
            "evaluation run persisted: id=42"
        );
    }

    #[test]
    fn eval_golden_rejects_profiles_path_without_profile_selector() {
        let error =
            ensure_profiles_path_has_profile_selector(Some(Path::new("configs/profiles")), None)
                .expect_err("profiles-path without profile selector should fail");

        assert_eq!(
            error.to_string(),
            "--profiles-path requires --profile-id or PROFILE_ID"
        );
    }

    #[test]
    fn eval_golden_scenario_path_profile_override_skips_evaluation_refs() {
        let temp = tempfile::tempdir().expect("tempdir");
        let ranking_dir = temp.path().join("ranking");
        let profile_dir = temp.path().join("profiles").join("override-profile");
        fs::create_dir_all(&ranking_dir).expect("ranking dir");
        fs::create_dir_all(&profile_dir).expect("profile dir");
        copy_default_ranking_configs(&ranking_dir);
        write_minimal_profile_reason_catalog(&profile_dir.join("reasons.yaml"), "override-profile");
        fs::write(
            profile_dir.join("profile.yaml"),
            r#"
schema_version: 2
kind: profile_pack
manifest_version: 1
profile_id: override-profile
display_name: Override Profile
compatibility_level: experimental
content_kinds:
  - school
  - event
supported_content_kinds:
  - school
  - event
context_inputs:
  - station
placements:
  - home
  - search
  - detail
  - mypage
fallback_policy: override_default
ranking_config_dir: ../../ranking
reason_catalog: reasons.yaml
article_support: reserved
"#,
        )
        .expect("profile manifest");

        let selection = resolve_eval_golden_scenario_selection(
            Some(PathBuf::from(DEFAULT_REPLAY_SCENARIO_PATH)),
            Some("override-profile".to_string()),
            Some(temp.path().join("profiles")),
            None,
        )
        .expect("scenario override selection");

        assert_eq!(selection.profile_id.as_deref(), Some("override-profile"));
        assert_eq!(
            selection.scenario_source.kind,
            ReplayScenarioSourceKind::ExplicitPath
        );
        assert_eq!(selection.scenario_source.pairwise_pack, None);
        assert_eq!(
            selection.ranking_config_dir,
            ranking_dir.canonicalize().unwrap()
        );
    }

    #[test]
    fn eval_golden_scenario_path_ranking_override_skips_profile_ranking_ref() {
        let temp = tempfile::tempdir().expect("tempdir");
        let ranking_dir = temp.path().join("ranking");
        let profile_dir = temp.path().join("profiles").join("override-profile");
        fs::create_dir_all(&ranking_dir).expect("ranking dir");
        fs::create_dir_all(&profile_dir).expect("profile dir");
        copy_default_ranking_configs(&ranking_dir);
        write_minimal_profile_reason_catalog(&profile_dir.join("reasons.yaml"), "override-profile");
        fs::write(
            profile_dir.join("profile.yaml"),
            r#"
schema_version: 2
kind: profile_pack
manifest_version: 1
profile_id: override-profile
display_name: Override Profile
compatibility_level: experimental
content_kinds:
  - school
  - event
supported_content_kinds:
  - school
  - event
context_inputs:
  - station
placements:
  - home
  - search
  - detail
  - mypage
fallback_policy: override_default
ranking_config_dir: ../../missing-ranking
reason_catalog: reasons.yaml
article_support: reserved
"#,
        )
        .expect("profile manifest");

        let selection = resolve_eval_golden_scenario_selection(
            Some(PathBuf::from(DEFAULT_REPLAY_SCENARIO_PATH)),
            Some("override-profile".to_string()),
            Some(temp.path().join("profiles")),
            Some(ranking_dir.clone()),
        )
        .expect("scenario and ranking override selection");

        assert_eq!(selection.profile_id.as_deref(), Some("override-profile"));
        assert_eq!(
            selection.scenario_source.kind,
            ReplayScenarioSourceKind::ExplicitPath
        );
        assert_eq!(
            selection.ranking_config_dir,
            ranking_dir.canonicalize().unwrap()
        );
    }

    #[test]
    fn eval_golden_profile_pack_uses_ranking_override_for_manifest_scenario() {
        let temp = tempfile::tempdir().expect("tempdir");
        let ranking_dir = temp.path().join("ranking");
        let profile_dir = temp.path().join("profiles").join("override-profile");
        let scenario_dir = profile_dir.join("eval");
        fs::create_dir_all(&ranking_dir).expect("ranking dir");
        fs::create_dir_all(&scenario_dir).expect("scenario dir");
        copy_default_ranking_configs(&ranking_dir);
        write_minimal_profile_reason_catalog(&profile_dir.join("reasons.yaml"), "override-profile");
        fs::write(
            profile_dir.join("profile.yaml"),
            r#"
schema_version: 2
kind: profile_pack
manifest_version: 1
profile_id: override-profile
display_name: Override Profile
compatibility_level: experimental
content_kinds:
  - school
  - event
supported_content_kinds:
  - school
  - event
context_inputs:
  - station
placements:
  - home
  - search
  - detail
  - mypage
fallback_policy: override_default
ranking_config_dir: ../../missing-ranking
reason_catalog: reasons.yaml
article_support: reserved
evaluation:
  scenario_pack: eval
"#,
        )
        .expect("profile manifest");

        let selection = resolve_eval_golden_scenario_selection(
            None,
            Some("override-profile".to_string()),
            Some(temp.path().join("profiles")),
            Some(ranking_dir.clone()),
        )
        .expect("profile evaluation with ranking override selection");

        assert_eq!(selection.profile_id.as_deref(), Some("override-profile"));
        assert_eq!(
            selection.scenario_source.kind,
            ReplayScenarioSourceKind::ProfileEvaluation
        );
        assert_eq!(
            selection.scenario_source.path,
            scenario_dir.canonicalize().unwrap()
        );
        assert_eq!(
            selection.ranking_config_dir,
            ranking_dir.canonicalize().unwrap()
        );
    }

    #[test]
    fn eval_text_formatters_use_operator_facing_command_names() {
        let scenario_summary = cli::ReplayScenarioSummary {
            profile_id: None,
            scenario_source: ReplayScenarioSource::default_path(PathBuf::from(
                DEFAULT_REPLAY_SCENARIO_PATH,
            )),
            scenarios: 0,
            passed: 0,
            blocked: 0,
            blockers: 0,
            warnings: 0,
            pairwise_passed: 0,
            pairwise_total: 0,
            explanation_integrity_passed: 0,
            explanation_integrity_total: 0,
            cases: Vec::new(),
        };

        assert!(format_eval_golden_summary(&scenario_summary).starts_with("eval golden completed:"));
        #[cfg(feature = "storage-backends")]
        {
            let evaluation_summary = cli::ReplayEvaluationSummary {
                evaluated: 0,
                matched: 0,
                mismatched: 0,
                failed: 0,
                cases: Vec::new(),
            };

            assert!(format_replay_scenario_summary(&scenario_summary)
                .starts_with("replay scenarios completed:"));
            assert!(format_eval_replay_summary(&evaluation_summary)
                .starts_with("eval replay completed:"));
            assert!(format_replay_evaluation_summary(&evaluation_summary)
                .starts_with("replay evaluation completed:"));
        }
    }

    #[test]
    fn explanation_integrity_doctor_help_points_to_full_replay_gate() {
        let mut command = Cli::command();
        let doctor = command
            .find_subcommand_mut("doctor")
            .expect("doctor command");
        let explanation_integrity = doctor
            .find_subcommand_mut("explanation-integrity")
            .expect("doctor explanation-integrity command");
        let mut buffer = Vec::new();
        explanation_integrity
            .write_long_help(&mut buffer)
            .expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("reason-code integrity"));
        assert!(help.contains("explanation-template checks"));
        assert!(help.contains("use `eval golden` or `replay scenarios`"));
        assert!(help.contains("doctor explanation-integrity --json"));
    }

    #[test]
    fn doctor_help_lists_ranking_config_doctor() {
        let mut command = Cli::command();
        let doctor = command
            .find_subcommand_mut("doctor")
            .expect("doctor command");
        let mut buffer = Vec::new();
        doctor.write_long_help(&mut buffer).expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("ranking-config"));
        assert!(help.contains("Run the ranking config contract doctor"));
    }

    #[test]
    fn ranking_config_doctor_help_points_to_config_lint_reuse() {
        let mut command = Cli::command();
        let doctor = command
            .find_subcommand_mut("doctor")
            .expect("doctor command");
        let ranking_config = doctor
            .find_subcommand_mut("ranking-config")
            .expect("doctor ranking-config command");
        let mut buffer = Vec::new();
        ranking_config
            .write_long_help(&mut buffer)
            .expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("reuses the same active ranking config"));
        assert!(help.contains("profile-pack lint path as `config lint`"));
        assert!(help.contains("referenced ranking config directories"));
        assert!(help.contains("event CSV example references"));
        assert!(help.contains("doctor ranking-config --json"));
        assert!(help.contains("--path configs/ranking --profiles-path configs/profiles"));
    }

    #[test]
    fn profile_pack_doctor_help_points_to_profile_validation_metrics() {
        let mut command = Cli::command();
        let doctor = command
            .find_subcommand_mut("doctor")
            .expect("doctor command");
        let profile_pack = doctor
            .find_subcommand_mut("profile-pack")
            .expect("doctor profile-pack command");
        let mut buffer = Vec::new();
        profile_pack
            .write_long_help(&mut buffer)
            .expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("reuses the same manifest"));
        assert!(help.contains("operator-facing profile-pack coverage metrics"));
        assert!(help.contains("doctor profile-pack --json"));
        assert!(help.contains("--profiles-path configs/profiles"));
    }

    #[test]
    fn ingest_quality_doctor_help_points_to_db_free_connector_coverage() {
        let mut command = Cli::command();
        let doctor = command
            .find_subcommand_mut("doctor")
            .expect("doctor command");
        let ingest_quality = doctor
            .find_subcommand_mut("ingest-quality")
            .expect("doctor ingest-quality command");
        let mut buffer = Vec::new();
        ingest_quality
            .write_long_help(&mut buffer)
            .expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("profile connector coverage"));
        assert!(help.contains("without running imports"));
        assert!(help.contains("touching PostgreSQL"));
        assert!(help.contains("making live crawl requests"));
        assert!(help.contains("runtime-executable field mappings"));
        assert!(help.contains("doctor ingest-quality --json"));
        assert!(help.contains("--profiles-path configs/profiles"));
    }

    #[test]
    fn context_coverage_doctor_help_points_to_replay_metadata_coverage() {
        let mut command = Cli::command();
        let doctor = command
            .find_subcommand_mut("doctor")
            .expect("doctor command");
        let context_coverage = doctor
            .find_subcommand_mut("context-coverage")
            .expect("doctor context-coverage command");
        let mut buffer = Vec::new();
        context_coverage
            .write_long_help(&mut buffer)
            .expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("context source"));
        assert!(help.contains("context shape"));
        assert!(help.contains("scenario tags"));
        assert!(help.contains("candidate-count coverage"));
        assert!(help.contains("use `eval golden` or `replay scenarios`"));
        assert!(help.contains("doctor context-coverage --json"));
        assert!(help.contains("--path configs/evaluation/scenarios"));
    }

    #[test]
    fn retrieval_parity_doctor_help_points_to_full_mode_ordering_contract() {
        let mut command = Cli::command();
        let doctor = command
            .find_subcommand_mut("doctor")
            .expect("doctor command");
        let retrieval_parity = doctor
            .find_subcommand_mut("retrieval-parity")
            .expect("doctor retrieval-parity command");
        let mut buffer = Vec::new();
        retrieval_parity
            .write_long_help(&mut buffer)
            .expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("full-mode OpenSearch candidate retrieval contract"));
        assert!(help.contains("SQL-only candidate slice"));
        assert!(help.contains("direct station first"));
        assert!(help.contains("does not require PostgreSQL or OpenSearch"));
        assert!(help.contains("public MVP gate"));
        assert!(help.contains("doctor retrieval-parity --json"));
    }

    #[test]
    fn storage_compatibility_doctor_help_points_to_static_registry() {
        let mut command = Cli::command();
        let doctor = command
            .find_subcommand_mut("doctor")
            .expect("doctor command");
        let storage_compatibility = doctor
            .find_subcommand_mut("storage-compatibility")
            .expect("doctor storage-compatibility command");
        let mut buffer = Vec::new();
        storage_compatibility
            .write_long_help(&mut buffer)
            .expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("static storage/cache/index compatibility registry"));
        assert!(help.contains("PostgreSQL/PostGIS, Redis, OpenSearch, MySQL, and SQLite"));
        assert!(help.contains("separates storage compatibility from profile-pack compatibility"));
        assert!(help.contains("does not claim MySQL write support"));
        assert!(help.contains("doctor storage-compatibility --json"));
    }

    #[test]
    fn doctor_help_lists_retrieval_parity_doctor() {
        let mut command = Cli::command();
        let doctor = command
            .find_subcommand_mut("doctor")
            .expect("doctor command");
        let mut buffer = Vec::new();
        doctor.write_long_help(&mut buffer).expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("retrieval-parity"));
        assert!(help.contains("Run the DB-free retrieval parity doctor"));
    }

    #[test]
    fn doctor_help_lists_ingest_quality_doctor() {
        let mut command = Cli::command();
        let doctor = command
            .find_subcommand_mut("doctor")
            .expect("doctor command");
        let mut buffer = Vec::new();
        doctor.write_long_help(&mut buffer).expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("ingest-quality"));
        assert!(help.contains("Run the DB-free ingest quality doctor"));
    }

    #[test]
    fn doctor_help_lists_storage_compatibility_doctor() {
        let mut command = Cli::command();
        let doctor = command
            .find_subcommand_mut("doctor")
            .expect("doctor command");
        let mut buffer = Vec::new();
        doctor.write_long_help(&mut buffer).expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("storage-compatibility"));
        assert!(help.contains("Print the static storage/cache/index compatibility registry"));
    }

    #[test]
    fn retrieval_parity_doctor_summary_reports_contract_cases() {
        let summary = cli::run_retrieval_parity_doctor();
        let rendered = format_retrieval_parity_doctor_summary(&summary);

        assert!(rendered.contains("doctor retrieval-parity completed: cases=6"));
        assert!(rendered.contains("passed=6"));
        assert!(rendered.contains("failed=0"));
        assert!(rendered.contains("requires_database=false"));
        assert!(rendered.contains("requires_opensearch=false"));
        assert!(rendered.contains("public_mvp_gate=false"));
        assert!(
            rendered.contains("ordering_contract: direct_station,distance_meters,walking_minutes")
        );
        assert!(rendered.contains("opensearch_sort_contract: _score=desc"));
        assert!(rendered.contains("case_id=direct_station_first status=passed"));
        assert!(rendered.contains("case_id=limit_after_ordering status=passed"));
    }

    #[test]
    fn storage_compatibility_doctor_summary_reports_registry() {
        let summary = cli::run_storage_compatibility_doctor();
        let rendered = format_storage_compatibility_doctor_summary(&summary);

        assert!(rendered.contains("doctor storage-compatibility completed:"));
        assert!(rendered.contains("registry_version=v0.4.0-static-storage-compatibility"));
        assert!(rendered.contains("components=5"));
        assert!(rendered
            .contains("levels=artifact_only=1,experimental=1,reference=1,stable_optional=2"));
        assert!(rendered.contains("sql_only_required=postgres_postgis"));
        assert!(rendered.contains("optional_runtime=redis,opensearch"));
        assert!(rendered.contains("public_mvp_gate=postgres_postgis,redis"));
        assert!(rendered.contains("final_ranking_owner=rust"));
        assert!(rendered.contains("profile_compatibility_source: profile manifests"));
        assert!(rendered.contains(
            "component=postgres_postgis display_name=PostgreSQL/PostGIS compatibility_level=reference"
        ));
        assert!(rendered
            .contains("component=redis display_name=Redis compatibility_level=stable_optional"));
        assert!(rendered.contains("component=redis display_name=Redis compatibility_level=stable_optional runtime_status=optional data_role=cache_only public_mvp_gate=true"));
        assert!(rendered.contains(
            "component=opensearch display_name=OpenSearch compatibility_level=stable_optional"
        ));
        assert!(rendered.contains("component=opensearch display_name=OpenSearch compatibility_level=stable_optional runtime_status=optional data_role=candidate_retrieval_only public_mvp_gate=false"));
        assert!(rendered
            .contains("component=mysql display_name=MySQL compatibility_level=experimental"));
        assert!(rendered.contains("component=mysql display_name=MySQL compatibility_level=experimental runtime_status=not_runtime_dependency data_role=compatibility_subset public_mvp_gate=false"));
        assert!(rendered.contains("write_database_status=not_implemented"));
        assert!(rendered
            .contains("component=sqlite display_name=SQLite compatibility_level=artifact_only"));
        assert!(rendered.contains("write_database_status=read_only_artifact"));
    }

    #[test]
    fn context_coverage_doctor_summary_reports_coverage_metrics() {
        let mut context_source_counts = BTreeMap::new();
        context_source_counts.insert("request_area".to_string(), 1);
        context_source_counts.insert("request_line".to_string(), 1);
        context_source_counts.insert("default_safe_context".to_string(), 1);
        let mut tag_counts = BTreeMap::new();
        tag_counts.insert("area_context".to_string(), 1);
        tag_counts.insert("line_context".to_string(), 1);
        let mut fallback_stage_counts = BTreeMap::new();
        fallback_stage_counts.insert("same_city".to_string(), 1);
        fallback_stage_counts.insert("same_line".to_string(), 1);
        let mut candidate_count_stage_counts = BTreeMap::new();
        candidate_count_stage_counts.insert("same_city".to_string(), 1);
        candidate_count_stage_counts.insert("same_line".to_string(), 1);
        let summary = cli::ContextCoverageDoctorSummary {
            scenarios: 3,
            scenarios_with_context: 3,
            scenarios_without_context: 0,
            scenarios_with_candidate_counts: 2,
            candidate_count_expectations: 2,
            context_shape_mismatches: Vec::new(),
            context_source_counts,
            tag_counts,
            fallback_stage_counts,
            candidate_count_stage_counts,
            required_context_sources: vec![
                cli::ContextCoverageRequirement {
                    context_source: "request_area".to_string(),
                    covered: true,
                    scenarios: 1,
                },
                cli::ContextCoverageRequirement {
                    context_source: "request_line".to_string(),
                    covered: true,
                    scenarios: 1,
                },
                cli::ContextCoverageRequirement {
                    context_source: "default_safe_context".to_string(),
                    covered: true,
                    scenarios: 1,
                },
            ],
            missing_required_context_sources: Vec::new(),
            cases: vec![cli::ContextCoverageDoctorCase {
                id: "S00_CONTEXT".to_string(),
                title: "Context".to_string(),
                path: PathBuf::from("configs/evaluation/scenarios/S00_context.yaml"),
                context_source: Some("request_area".to_string()),
                tags: vec!["area_context".to_string()],
                fallback_stage: "same_city".to_string(),
                candidate_count_stages: vec!["same_city".to_string()],
                has_area_context: true,
                has_line_context: false,
                has_station_context: false,
            }],
        };

        let rendered = format_context_coverage_doctor_summary(&summary);

        assert!(rendered.contains("doctor context-coverage completed: scenarios=3"));
        assert!(rendered.contains("context_shape_mismatches=0"));
        assert!(rendered.contains(
            "required_context_sources=request_area=1,request_line=1,default_safe_context=1"
        ));
        assert!(rendered
            .contains("context_sources: default_safe_context=1,request_area=1,request_line=1"));
        assert!(rendered.contains("context_tags: area_context=1,line_context=1"));
        assert!(rendered.contains("fallback_stages: same_city=1,same_line=1"));
        assert!(rendered.contains("candidate_count_stages: same_city=1,same_line=1"));
        assert!(rendered.contains("context_shape=area"));
    }

    #[test]
    fn profile_pack_doctor_summary_reports_profile_metrics() {
        let summary = cli::ProfilePackDoctorSummary {
            profile_packs: 1,
            ranking_config_dirs: 1,
            reason_catalog_locales: 1,
            reason_count: 14,
            fixture_references: 1,
            connector_references: 6,
            evaluation_references: 1,
            source_manifest_references: 4,
            event_csv_example_references: 1,
            archive_source_references: 0,
            optional_crawler_manifest_references: 1,
            files: vec![cli::ProfilePackDoctorFile {
                path: PathBuf::from("configs/profiles/school-event-jp/profile.yaml"),
                profile_id: "school-event-jp".to_string(),
                ranking_config_dir: PathBuf::from("configs/ranking"),
                fallback_config_path: None,
                reason_catalog_path: PathBuf::from("configs/profiles/school-event-jp/reasons.yaml"),
                schema_version: 2,
                kind: "profile_pack".to_string(),
                manifest_version: 1,
                compatibility_level: "reference".to_string(),
                content_kind_registry: vec!["school".to_string(), "event".to_string()],
                supported_content_kinds: vec!["school".to_string(), "event".to_string()],
                runtime_executable_content_kinds: vec!["school".to_string(), "event".to_string()],
                registry_only_content_kinds: Vec::new(),
                placements: vec![
                    "home".to_string(),
                    "search".to_string(),
                    "detail".to_string(),
                    "mypage".to_string(),
                ],
                reason_catalog_locale_count: 1,
                reason_count: 14,
                fixture_references: 1,
                connector_references: 6,
                connector_registry: vec![ProfileConnectorRegistryEntry {
                    connector_type: ProfileConnectorType::SourceManifest,
                    source_class: ProfileSourceClass::CsvImport,
                    manifest_path: PathBuf::from("storage/sources/jp_rail/example.yaml"),
                    manifest_kind: "import_source".to_string(),
                    source_id: Some("jp-rail".to_string()),
                    field_mapping: None,
                    profile_compatibility: ProfileCompatibilityLevel::Reference,
                    safety: ProfileConnectorSafetyMetadata {
                        local_reference_only: true,
                        dynamic_loading_enabled: false,
                        live_fetch_default: false,
                        allowlist_required: false,
                    },
                }],
                evaluation_references: 1,
                source_manifest_references: 4,
                event_csv_example_references: 1,
                archive_source_references: 0,
                optional_crawler_manifest_references: 1,
            }],
        };

        let rendered = format_profile_pack_doctor_summary(&summary);

        assert!(rendered.contains("doctor profile-pack completed: profile_packs=1"));
        assert!(rendered.contains("reasons=14"));
        assert!(rendered.contains("fixture_references=1"));
        assert!(rendered.contains("connector_references=6"));
        assert!(rendered.contains("evaluation_references=1"));
        assert!(rendered.contains("source_manifest_references=4"));
        assert!(rendered.contains("event_csv_example_references=1"));
        assert!(rendered.contains("archive_source_references=0"));
        assert!(rendered.contains("optional_crawler_manifest_references=1"));
        assert!(rendered.contains("event_csv_examples=1"));
        assert!(rendered.contains("connector type=source_manifest source_class=csv_import"));
        assert!(rendered.contains("manifest_kind=import_source"));
        assert!(rendered.contains("source_id=jp-rail"));
        assert!(rendered.contains("profile_id=school-event-jp"));
        assert!(rendered.contains("fallback_config=none"));
        assert!(rendered.contains("compatibility_level=reference"));
        assert!(rendered.contains("runtime_executable_content_kinds=school,event"));
        assert!(rendered.contains("registry_only_content_kinds=-"));

        let json = serde_json::to_string(&summary).expect("json");
        assert!(json.contains("\"compatibility_level\":\"reference\""));
        assert!(json.contains("\"runtime_executable_content_kinds\":[\"school\",\"event\"]"));
        assert!(json.contains("\"registry_only_content_kinds\":[]"));
    }

    #[test]
    fn ingest_quality_doctor_summary_reports_connector_coverage() {
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let summary = cli::run_ingest_quality_doctor(repo_root.join("configs/profiles"))
            .expect("ingest quality doctor");
        let rendered = format_ingest_quality_doctor_summary(&summary);

        assert!(rendered.contains("doctor ingest-quality completed: profile_packs=2"));
        assert!(rendered.contains("connectors=10"));
        assert!(rendered
            .contains("source_classes=archive_import=1,csv_import=6,html_crawl=1,ndjson_import=2"));
        assert!(rendered.contains(
            "manifest_kinds=archive_source=1,crawler_source=1,csv_file=2,import_source=4,ndjson_file=2"
        ));
        assert!(rendered.contains("runtime_executable_mappings=5"));
        assert!(rendered.contains("non_runtime_mappings=0"));
        assert!(rendered.contains("source_manifest_files=4"));
        assert!(rendered.contains("archive_files=1"));
        assert!(rendered.contains("crawler_targets=1"));
        assert!(rendered.contains("crawler_allowlist_required=1"));
        assert!(rendered.contains("evidence_scope: db_free_profile_connector_manifest_coverage"));
        assert!(rendered.contains("execution_scope: no_import_or_live_crawl"));
        assert!(rendered.contains("archive_formats: tar=1"));
        assert!(rendered.contains("crawler_source_maturity: parser_only=1"));
        assert!(rendered.contains("crawler_expected_shapes: html_heading_page=1"));
        assert!(rendered.contains("profile_id=school-event-jp connectors=7"));
        assert!(rendered.contains("profile_id=local-discovery-generic connectors=3"));
        assert!(rendered.contains("connector type=archive_source source_class=archive_import"));
        assert!(rendered.contains("lint=archive_source_lint"));
        assert!(rendered.contains("connector type=crawler_manifest source_class=html_crawl"));
        assert!(rendered.contains("lint=crawler_manifest_lint"));
        assert!(rendered.contains("field_mapping_runtime_executable=true"));
    }

    #[test]
    fn ranking_config_doctor_summary_reports_config_and_profile_metrics() {
        let mut kind_counts = BTreeMap::new();
        kind_counts.insert("ranking_placement".to_string(), 4);
        kind_counts.insert("ranking_schools".to_string(), 1);
        let summary = cli::RankingConfigDoctorSummary {
            active_profile_id: Some("local-discovery-generic".to_string()),
            fixture_set_id: Some("minimal".to_string()),
            ranking_config_dir: PathBuf::from("configs/ranking"),
            profile_version: "profile-version".to_string(),
            ranking_files: 5,
            ranking_kind_counts: kind_counts,
            profile_packs: 1,
            referenced_ranking_config_dirs: 1,
            reason_catalog_references: 1,
            reason_catalog_locales: 1,
            reason_count: 14,
            fixture_references: 1,
            connector_references: 0,
            evaluation_references: 1,
            source_manifest_references: 2,
            event_csv_example_references: 1,
            archive_source_references: 0,
            optional_crawler_manifest_references: 1,
            files: vec![cli::RankingConfigDoctorFile {
                path: PathBuf::from("configs/ranking/schools.default.yaml"),
                schema_version: 1,
                kind: "ranking_schools".to_string(),
            }],
            profiles: vec![cli::RankingConfigDoctorProfile {
                path: PathBuf::from("configs/profiles/local-discovery-generic/profile.yaml"),
                profile_id: "local-discovery-generic".to_string(),
                ranking_config_dir: PathBuf::from("configs/ranking"),
                fallback_config_path: None,
                reason_catalog_path: PathBuf::from(
                    "configs/profiles/local-discovery-generic/reasons.yaml",
                ),
                compatibility_level: "stable".to_string(),
                content_kind_registry: vec!["school".to_string(), "event".to_string()],
                supported_content_kinds: vec!["school".to_string(), "event".to_string()],
                runtime_executable_content_kinds: vec!["school".to_string(), "event".to_string()],
                registry_only_content_kinds: Vec::new(),
                placements: vec![
                    "home".to_string(),
                    "search".to_string(),
                    "detail".to_string(),
                    "mypage".to_string(),
                ],
                reason_catalog_locale_count: 1,
                reason_count: 14,
                fixture_references: 1,
                connector_references: 0,
                evaluation_references: 1,
                source_manifest_references: 2,
                event_csv_example_references: 1,
                archive_source_references: 0,
                optional_crawler_manifest_references: 1,
            }],
        };

        let rendered = format_ranking_config_doctor_summary(&summary);

        assert!(rendered.contains("doctor ranking-config completed"));
        assert!(rendered.contains("active_profile_id=local-discovery-generic"));
        assert!(rendered.contains("fixture_set_id=minimal"));
        assert!(rendered.contains("ranking_kinds=ranking_placement=4,ranking_schools=1"));
        assert!(rendered.contains("referenced_ranking_config_dirs=1"));
        assert!(rendered.contains("reason_catalog_references=1"));
        assert!(rendered.contains("fixture_references=1"));
        assert!(rendered.contains("evaluation_references=1"));
        assert!(rendered.contains("ranking_config_dir=configs/ranking"));
        assert!(rendered.contains("kind=ranking_schools"));
        assert!(rendered.contains("profile_id=local-discovery-generic"));
        assert!(rendered.contains("fallback_config=none"));
        assert!(rendered.contains("compatibility_level=stable"));
        assert!(rendered.contains("runtime_executable_content_kinds=school,event"));
        assert!(rendered.contains("registry_only_content_kinds=-"));
    }

    #[test]
    fn config_lint_report_reuses_profile_pack_ranking_summary_for_explicit_paths() {
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let report = build_config_lint_report(
            Some(repo_root.join("configs/ranking")),
            Some(repo_root.join("configs/profiles")),
        );
        let report = report.expect("config lint report");

        assert_eq!(report.ranking_summary.files.len(), 8);
        assert_eq!(report.profile_summary.files.len(), 2);
        assert_eq!(report.profile_summary.ranking_configs.len(), 1);
    }

    #[test]
    fn ranking_config_doctor_summary_from_config_lint_report_is_json_ready() {
        let ranking_summary = RankingConfigLintSummary {
            path: PathBuf::from("configs/ranking"),
            profile_version: "profile-version".to_string(),
            files: vec![
                RankingConfigLintFile {
                    path: PathBuf::from("configs/ranking/placement.home.yaml"),
                    schema_version: 1,
                    kind: RankingConfigKind::RankingPlacement,
                },
                RankingConfigLintFile {
                    path: PathBuf::from("configs/ranking/schools.default.yaml"),
                    schema_version: 1,
                    kind: RankingConfigKind::RankingSchools,
                },
            ],
        };
        let profile_summary = ProfilePackLintSummary {
            files: vec![
                ProfilePackLintFile {
                    path: PathBuf::from("configs/profiles/local-discovery-generic/profile.yaml"),
                    profile_id: "local-discovery-generic".to_string(),
                    ranking_config_dir: PathBuf::from("configs/ranking"),
                    fallback_config_path: None,
                    reason_catalog_path: PathBuf::from(
                        "configs/profiles/local-discovery-generic/reasons.yaml",
                    ),
                    schema_version: 2,
                    kind: ProfilePackKind::ProfilePack,
                    manifest_version: 1,
                    compatibility_level: ProfileCompatibilityLevel::Stable,
                    content_kind_registry: vec!["school".into(), "event".into()],
                    supported_content_kinds: Vec::new(),
                    runtime_executable_content_kinds: Vec::new(),
                    registry_only_content_kinds: Vec::new(),
                    placements: vec![
                        PlacementKind::Home,
                        PlacementKind::Search,
                        PlacementKind::Detail,
                        PlacementKind::Mypage,
                    ],
                    reason_catalog_locale_count: 1,
                    reason_count: 14,
                    fixture_count: 1,
                    connector_count: 0,
                    connector_registry: Vec::new(),
                    evaluation_reference_count: 1,
                    source_manifest_count: 2,
                    event_csv_example_count: 1,
                    archive_source_count: 0,
                    optional_crawler_manifest_count: 1,
                },
                ProfilePackLintFile {
                    path: PathBuf::from("configs/profiles/school-event-jp/profile.yaml"),
                    profile_id: "school-event-jp".to_string(),
                    ranking_config_dir: PathBuf::from("configs/ranking"),
                    fallback_config_path: None,
                    reason_catalog_path: PathBuf::from(
                        "configs/profiles/local-discovery-generic/reasons.yaml",
                    ),
                    schema_version: 2,
                    kind: ProfilePackKind::ProfilePack,
                    manifest_version: 1,
                    compatibility_level: ProfileCompatibilityLevel::Reference,
                    content_kind_registry: vec!["school".into(), "event".into()],
                    supported_content_kinds: Vec::new(),
                    runtime_executable_content_kinds: Vec::new(),
                    registry_only_content_kinds: Vec::new(),
                    placements: vec![
                        PlacementKind::Home,
                        PlacementKind::Search,
                        PlacementKind::Detail,
                        PlacementKind::Mypage,
                    ],
                    reason_catalog_locale_count: 1,
                    reason_count: 7,
                    fixture_count: 0,
                    connector_count: 6,
                    connector_registry: Vec::new(),
                    evaluation_reference_count: 1,
                    source_manifest_count: 1,
                    event_csv_example_count: 0,
                    archive_source_count: 0,
                    optional_crawler_manifest_count: 0,
                },
            ],
            ranking_configs: vec![RankingConfigLintSummary {
                path: PathBuf::from("configs/ranking"),
                profile_version: "profile-version".to_string(),
                files: Vec::new(),
            }],
        };

        let summary = ranking_config_doctor_summary_from_lint(
            Some("local-discovery-generic".to_string()),
            Some("minimal".to_string()),
            ranking_summary,
            profile_summary,
        );
        let json = serde_json::to_string(&summary).expect("json");

        assert!(json.contains("\"active_profile_id\":\"local-discovery-generic\""));
        assert!(json.contains("\"compatibility_level\":\"stable\""));
        assert!(json.contains("\"ranking_placement\":1"));
        assert_eq!(summary.profile_packs, 2);
        assert_eq!(summary.referenced_ranking_config_dirs, 1);
        assert_eq!(summary.reason_catalog_references, 1);
        assert_eq!(summary.reason_catalog_locales, 2);
        assert_eq!(summary.reason_count, 21);
        assert_eq!(summary.connector_references, 6);
        assert_eq!(summary.evaluation_references, 2);
        assert_eq!(summary.source_manifest_references, 3);
    }

    #[test]
    fn profile_validate_summary_reports_profile_count() {
        let summary = ProfilePackLintSummary {
            files: vec![ProfilePackLintFile {
                path: PathBuf::from("configs/profiles/local-discovery-generic/profile.yaml"),
                profile_id: "local-discovery-generic".to_string(),
                ranking_config_dir: PathBuf::from("configs/ranking"),
                fallback_config_path: None,
                reason_catalog_path: PathBuf::from(
                    "configs/profiles/local-discovery-generic/reasons.yaml",
                ),
                schema_version: 2,
                kind: ProfilePackKind::ProfilePack,
                manifest_version: 1,
                compatibility_level: ProfileCompatibilityLevel::Stable,
                content_kind_registry: vec!["school".into(), "event".into()],
                supported_content_kinds: vec!["school".into()],
                runtime_executable_content_kinds: vec!["school".into()],
                registry_only_content_kinds: vec!["event".into()],
                placements: vec![
                    PlacementKind::Home,
                    PlacementKind::Search,
                    PlacementKind::Detail,
                    PlacementKind::Mypage,
                ],
                reason_catalog_locale_count: 1,
                reason_count: 14,
                fixture_count: 1,
                connector_count: 0,
                connector_registry: Vec::new(),
                evaluation_reference_count: 1,
                source_manifest_count: 0,
                event_csv_example_count: 0,
                archive_source_count: 0,
                optional_crawler_manifest_count: 0,
            }],
            ranking_configs: Vec::new(),
        };

        let rendered = format_profile_validate_summary(&summary);

        assert!(rendered.contains("profile validate ok: profile_packs=1"));
        assert!(rendered.contains("profile_id=local-discovery-generic"));
        assert!(rendered.contains("compatibility_level=stable"));
        assert!(rendered.contains("runtime_executable_content_kinds=school"));
        assert!(rendered.contains("registry_only_content_kinds=event"));
    }

    #[test]
    fn profile_inspect_summary_reports_runtime_paths() {
        let manifest_path = PathBuf::from("configs/profiles/local-discovery-generic/profile.yaml");
        let manifest: ProfilePackManifest = serde_yaml::from_str(
            r#"schema_version: 2
kind: profile_pack
manifest_version: 1
profile_id: local-discovery-generic
display_name: Local Discovery Generic
compatibility_level: stable
content_kinds:
  - school
  - event
supported_content_kinds:
  - school
context_inputs:
  - station
placements:
  - home
  - search
  - detail
  - mypage
fallback_policy: geo_line_default
ranking_config_dir: ../../ranking
reason_catalog: reasons.yaml
article_support: reserved
"#,
        )
        .expect("profile manifest");
        let lint_file = ProfilePackLintFile {
            path: manifest_path.clone(),
            profile_id: "local-discovery-generic".to_string(),
            ranking_config_dir: PathBuf::from("configs/ranking"),
            fallback_config_path: None,
            reason_catalog_path: PathBuf::from(
                "configs/profiles/local-discovery-generic/reasons.yaml",
            ),
            schema_version: 2,
            kind: ProfilePackKind::ProfilePack,
            manifest_version: 1,
            compatibility_level: ProfileCompatibilityLevel::Stable,
            content_kind_registry: manifest
                .content_kinds
                .clone()
                .unwrap_or_else(|| manifest.supported_content_kinds.clone()),
            supported_content_kinds: manifest.supported_content_kinds.clone(),
            runtime_executable_content_kinds: manifest.supported_content_kinds.clone(),
            registry_only_content_kinds: vec!["event".into()],
            placements: manifest.placements.clone(),
            reason_catalog_locale_count: 1,
            reason_count: 14,
            fixture_count: 0,
            connector_count: 0,
            connector_registry: Vec::new(),
            evaluation_reference_count: 0,
            source_manifest_count: 0,
            event_csv_example_count: 0,
            archive_source_count: 0,
            optional_crawler_manifest_count: 0,
        };
        let runtime_manifest_path = PathBuf::from("repo")
            .join("configs")
            .join("profiles")
            .join("local-discovery-generic")
            .join("profile.yaml");
        let reason_catalog_path = PathBuf::from("repo")
            .join("configs")
            .join("profiles")
            .join("local-discovery-generic")
            .join("reasons.yaml");
        let ranking_config_dir = PathBuf::from("repo").join("configs").join("ranking");
        let fixture_dir = PathBuf::from("repo")
            .join("storage")
            .join("fixtures")
            .join("minimal");
        let runtime_selection = config::ProfilePackRuntimeSelection {
            profile_id: "local-discovery-generic".to_string(),
            profile_pack_manifest: runtime_manifest_path,
            reason_catalog_path: reason_catalog_path.clone(),
            fallback_config_path: None,
            ranking_config_dir: ranking_config_dir.clone(),
            fixture_set_id: Some("minimal".to_string()),
            fixture_dir: Some(fixture_dir.clone()),
        };

        let rendered = format_profile_inspect_summary(
            &manifest_path,
            &manifest,
            &lint_file,
            &runtime_selection,
        );

        assert!(rendered.contains(&format!(
            "runtime_reason_catalog_path={}",
            reason_catalog_path.display()
        )));
        assert!(rendered.contains("content_kind_registry=school,event"));
        assert!(rendered.contains("content_kinds=school"));
        assert!(rendered.contains("runtime_executable_content_kinds=school"));
        assert!(rendered.contains("registry_only_content_kinds=event"));
        assert!(rendered.contains(&format!(
            "runtime_ranking_config_dir={}",
            ranking_config_dir.display()
        )));
        assert!(rendered.contains("runtime_fallback_config_path=none"));
        assert!(rendered.contains("runtime_fixture_set_id=minimal"));
        assert!(rendered.contains(&format!("runtime_fixture_dir={}", fixture_dir.display())));
        assert!(rendered.contains("connector_registry:"));
        assert!(rendered.contains("- none"));
        assert!(rendered.contains("compatibility_level=stable"));
    }

    #[test]
    fn profile_registry_selects_manifest_by_profile_id() {
        let temp = tempfile::tempdir().expect("tempdir");
        let profiles_dir = temp.path().join("profiles");
        let other_profile_dir = profiles_dir.join("other-profile");
        fs::create_dir_all(&other_profile_dir).expect("profile dir");
        fs::write(
            profiles_dir.join("profile.yaml"),
            r#"
schema_version: 2
kind: profile_pack
manifest_version: 1
profile_id: root-profile
display_name: Root Profile
compatibility_level: experimental
supported_content_kinds:
  - school
context_inputs:
  - station
placements:
  - home
  - search
  - detail
  - mypage
fallback_policy: custom_default
ranking_config_dir: ../../ranking
reason_catalog: reasons.yaml
article_support: reserved
"#,
        )
        .expect("root profile manifest");
        fs::write(
            other_profile_dir.join("profile.yaml"),
            r#"
schema_version: 2
kind: profile_pack
manifest_version: 1
profile_id: other-profile
display_name: Other Profile
compatibility_level: experimental
supported_content_kinds:
  - school
context_inputs:
  - station
placements:
  - home
  - search
  - detail
  - mypage
fallback_policy: other_default
ranking_config_dir: ../../ranking
reason_catalog: reasons.yaml
article_support: reserved
"#,
        )
        .expect("other profile manifest");

        let registry = ProfilePackRegistry::new(&profiles_dir);

        let path = registry
            .manifest_path_for_profile_id("other-profile")
            .expect("profile manifest path");

        assert_eq!(path, other_profile_dir.join("profile.yaml"));
    }
}
