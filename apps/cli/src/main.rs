use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Context;
use clap::{Parser, Subcommand};
use cli::{
    format_context_coverage_doctor_summary, format_context_inspect_summary,
    format_explain_trace_report, format_explanation_integrity_doctor_summary,
    format_fixture_doctor_summary, format_job_enqueue_summary, format_job_inspection,
    format_job_list, format_job_mutation_summary, format_profile_pack_doctor_summary,
    format_ranking_config_doctor_summary, format_replay_evaluation_summary,
    format_replay_scenario_summary, format_snapshot_refresh_summary, format_summary,
    generate_demo_jp_fixture, ranking_config_doctor_summary_from_lint, run_context_coverage_doctor,
    run_context_inspect, run_derive_school_station_links, run_event_csv_import, run_explain_trace,
    run_explanation_integrity_doctor, run_fixture_doctor, run_import_command, run_job_due,
    run_job_enqueue, run_job_inspect, run_job_list, run_job_retry, run_profile_pack_doctor,
    run_replay_evaluate, run_replay_scenarios, run_snapshot_refresh, ContextInspectInput,
    ImportTarget, DEFAULT_REPLAY_SCENARIO_PATH,
};
use config::{
    lint_profile_pack_dir, lint_ranking_config_dir, load_and_lint_profile_pack_file,
    resolve_linted_profile_pack_runtime_selection, resolve_runtime_path, AppSettings,
    ProfilePackLintFile, ProfilePackLintSummary, ProfilePackManifest, ProfilePackRegistry,
    RankingConfigLintSummary, DEFAULT_ALGORITHM_VERSION, DEFAULT_PROFILE_ID,
    DEFAULT_PROFILE_PACKS_DIR, DEFAULT_RANKING_CONFIG_DIR,
};
use generic_csv::{lint_source_manifest_dir, SourceManifestLintSummary};
use storage_opensearch::ProjectionSyncService;
use storage_postgres::{run_migrations, seed_fixture};

#[derive(Debug, Parser)]
#[command(name = "geo-line-ranker-cli")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Migrate,
    Seed {
        #[command(subcommand)]
        target: SeedTarget,
    },
    Import {
        #[command(subcommand)]
        target: ImportCommand,
    },
    Derive {
        #[command(subcommand)]
        target: DeriveCommand,
    },
    Fixtures {
        #[command(subcommand)]
        target: FixtureCommand,
    },
    Index {
        #[command(subcommand)]
        target: IndexCommand,
    },
    Projection {
        #[command(subcommand)]
        target: ProjectionCommand,
    },
    Snapshot {
        #[command(subcommand)]
        target: SnapshotCommand,
    },
    Replay {
        #[command(subcommand)]
        target: ReplayCommand,
    },
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
    Jobs {
        #[command(subcommand)]
        target: JobsCommand,
    },
    DumpOpenapi {
        #[arg(default_value = "schemas/openapi.json")]
        output: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum SeedTarget {
    Example,
}

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
}

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

#[derive(Debug, Subcommand)]
enum IndexCommand {
    Rebuild,
}

#[derive(Debug, Subcommand)]
enum ProjectionCommand {
    Sync,
}

#[derive(Debug, Subcommand)]
enum SnapshotCommand {
    Refresh,
}

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
        long_about = "Run the ranking config contract doctor for operator-facing quality evidence. This reuses the same active ranking config and profile-pack lint path as `config lint`, then summarizes ranking file kinds, active profile selection, profile pack coverage, referenced ranking config directories, reason catalog references, fixture references, source manifest references, event CSV example references, and optional crawler manifest references.\n\nExamples:\n  geo-line-ranker-cli doctor ranking-config\n  geo-line-ranker-cli doctor ranking-config --json\n  geo-line-ranker-cli doctor ranking-config --path configs/ranking --profiles-path configs/profiles"
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
        long_about = "Run the DB-free explanation integrity doctor against committed replay scenarios. This reports only reason-code integrity and explanation-template checks; use `replay scenarios` for the full ranking correctness gate.\n\nExamples:\n  geo-line-ranker-cli doctor explanation-integrity\n  geo-line-ranker-cli doctor explanation-integrity --json"
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
        long_about = "Run the profile pack validation doctor against committed profile manifests. This reuses the same manifest, reason catalog, ranking config, fixture, and local reference validation as `profile validate`, then prints operator-facing profile-pack coverage metrics.\n\nExamples:\n  geo-line-ranker-cli doctor profile-pack\n  geo-line-ranker-cli doctor profile-pack --json\n  geo-line-ranker-cli doctor profile-pack --profiles-path configs/profiles"
    )]
    ProfilePack {
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
        long_about = "Run the DB-free context coverage doctor against committed replay scenarios. This reads scenario metadata and expectations only, then summarizes context source, context shape, scenario tags, fallback-stage, and candidate-count coverage; use `replay scenarios` for ranking correctness.\n\nExamples:\n  geo-line-ranker-cli doctor context-coverage\n  geo-line-ranker-cli doctor context-coverage --json\n  geo-line-ranker-cli doctor context-coverage --path configs/evaluation/scenarios"
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
        Command::Migrate => {
            let settings = AppSettings::from_env_without_profile_pack()?;
            run_migrations(&settings.database_url, "storage/migrations/postgres").await?;
        }
        Command::Seed { target } => match target {
            SeedTarget::Example => {
                let settings = AppSettings::from_env_requiring_fixture()?;
                seed_fixture(&settings.database_url, &settings.fixture_dir).await?
            }
        },
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
            };
            println!("{}", format_summary(&summary));
        }
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
        Command::Snapshot { target } => match target {
            SnapshotCommand::Refresh => {
                let settings = AppSettings::from_env()?;
                let summary = run_snapshot_refresh(&settings).await?;
                println!("{}", format_snapshot_refresh_summary(&summary));
            }
        },
        Command::Replay { target } => match target {
            ReplayCommand::Evaluate {
                limit,
                fail_on_mismatch,
            } => {
                let settings = AppSettings::from_env()?;
                let summary = run_replay_evaluate(&settings, limit).await?;
                println!("{}", format_replay_evaluation_summary(&summary));
                if fail_on_mismatch && (summary.mismatched > 0 || summary.failed > 0) {
                    anyhow::bail!(
                        "replay evaluation had mismatches={} failed={}",
                        summary.mismatched,
                        summary.failed
                    );
                }
            }
            ReplayCommand::Scenarios {
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
                let summary = run_replay_scenarios(path, ranking_config_dir, &algorithm_version)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                } else {
                    println!("{}", format_replay_scenario_summary(&summary));
                }
                if summary.has_blockers() && !allow_blockers {
                    anyhow::bail!("replay scenarios had blocker checks={}", summary.blockers);
                }
            }
        },
        Command::Explain { target } => match target {
            ExplainCommand::Trace { id, json } => {
                let settings = AppSettings::from_env_without_profile_pack()?;
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
            } => {
                let profiles_path = profiles_path.unwrap_or(env_path_or_default(
                    "PROFILE_PACKS_DIR",
                    PathBuf::from(DEFAULT_PROFILE_PACKS_DIR),
                )?);
                let summary = run_profile_pack_doctor(profiles_path)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                } else {
                    println!("{}", format_profile_pack_doctor_summary(&summary));
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
            ProfileCommand::Validate { profiles_path } => {
                let profiles_path = profiles_path.unwrap_or(env_path_or_default(
                    "PROFILE_PACKS_DIR",
                    PathBuf::from(DEFAULT_PROFILE_PACKS_DIR),
                )?);
                let summary = lint_profile_pack_dir(profiles_path)?;
                println!("{}", format_profile_validate_summary(&summary));
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
    let content_kinds = file
        .supported_content_kinds
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "- {} profile_id={} schema_version={} kind={} manifest_version={} content_kinds={} reasons={} fixtures={} source_manifests={} event_csv_examples={} optional_crawler_manifests={}",
        file.path.display(),
        file.profile_id,
        file.schema_version,
        file.kind.as_str(),
        file.manifest_version,
        content_kinds,
        file.reason_count,
        file.fixture_count,
        file.source_manifest_count,
        file.event_csv_example_count,
        file.optional_crawler_manifest_count
    )
}

fn format_profile_inspect_summary(
    manifest_path: &Path,
    manifest: &ProfilePackManifest,
    lint_file: &ProfilePackLintFile,
    runtime_selection: &config::ProfilePackRuntimeSelection,
) -> String {
    let content_kinds = manifest
        .supported_content_kinds
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(",");
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
            "schema_version={} kind={} manifest_version={}",
            manifest.schema_version,
            manifest.kind.as_str(),
            manifest.manifest_version
        ),
        format!("content_kinds={content_kinds}"),
        format!("context_inputs={context_inputs}"),
        format!("fallback_policy={}", manifest.fallback_policy),
        format!("ranking_config_dir={}", manifest.ranking_config_dir),
        format!(
            "reason_catalog={} reasons={}",
            manifest.reason_catalog, lint_file.reason_count
        ),
        format!(
            "runtime_reason_catalog_path={}",
            runtime_selection.reason_catalog_path.display()
        ),
        format!(
            "runtime_ranking_config_dir={}",
            runtime_selection.ranking_config_dir.display()
        ),
        format!("article_support={}", manifest.article_support.as_str()),
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
    use config::{
        ProfilePackKind, ProfilePackLintFile, ProfilePackLintSummary, RankingConfigKind,
        RankingConfigLintFile, RankingConfigLintSummary,
    };
    use std::{collections::BTreeMap, fs};

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
        assert!(help.contains("use `replay scenarios` for the full ranking correctness gate"));
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
        assert!(help.contains("use `replay scenarios` for ranking correctness"));
        assert!(help.contains("doctor context-coverage --json"));
        assert!(help.contains("--path configs/evaluation/scenarios"));
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
            reason_count: 14,
            fixture_references: 1,
            source_manifest_references: 4,
            event_csv_example_references: 1,
            optional_crawler_manifest_references: 1,
            files: vec![cli::ProfilePackDoctorFile {
                path: PathBuf::from("configs/profiles/school-event-jp/profile.yaml"),
                profile_id: "school-event-jp".to_string(),
                ranking_config_dir: PathBuf::from("configs/ranking"),
                reason_catalog_path: PathBuf::from("configs/profiles/school-event-jp/reasons.yaml"),
                schema_version: 1,
                kind: "profile_pack".to_string(),
                manifest_version: 1,
                supported_content_kinds: vec!["school".to_string(), "event".to_string()],
                reason_count: 14,
                fixture_references: 1,
                source_manifest_references: 4,
                event_csv_example_references: 1,
                optional_crawler_manifest_references: 1,
            }],
        };

        let rendered = format_profile_pack_doctor_summary(&summary);

        assert!(rendered.contains("doctor profile-pack completed: profile_packs=1"));
        assert!(rendered.contains("reasons=14"));
        assert!(rendered.contains("fixture_references=1"));
        assert!(rendered.contains("source_manifest_references=4"));
        assert!(rendered.contains("event_csv_example_references=1"));
        assert!(rendered.contains("optional_crawler_manifest_references=1"));
        assert!(rendered.contains("event_csv_examples=1"));
        assert!(rendered.contains("profile_id=school-event-jp"));
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
            reason_count: 14,
            fixture_references: 1,
            source_manifest_references: 2,
            event_csv_example_references: 1,
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
                reason_catalog_path: PathBuf::from(
                    "configs/profiles/local-discovery-generic/reasons.yaml",
                ),
                reason_count: 14,
                fixture_references: 1,
                source_manifest_references: 2,
                event_csv_example_references: 1,
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
        assert!(rendered.contains("ranking_config_dir=configs/ranking"));
        assert!(rendered.contains("kind=ranking_schools"));
        assert!(rendered.contains("profile_id=local-discovery-generic"));
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
            files: vec![ProfilePackLintFile {
                path: PathBuf::from("configs/profiles/local-discovery-generic/profile.yaml"),
                profile_id: "local-discovery-generic".to_string(),
                ranking_config_dir: PathBuf::from("configs/ranking"),
                reason_catalog_path: PathBuf::from(
                    "configs/profiles/local-discovery-generic/reasons.yaml",
                ),
                schema_version: 1,
                kind: ProfilePackKind::ProfilePack,
                manifest_version: 1,
                supported_content_kinds: Vec::new(),
                reason_count: 14,
                fixture_count: 1,
                source_manifest_count: 2,
                event_csv_example_count: 1,
                optional_crawler_manifest_count: 1,
            }],
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
        assert!(json.contains("\"ranking_placement\":1"));
        assert_eq!(summary.referenced_ranking_config_dirs, 1);
        assert_eq!(summary.reason_catalog_references, 1);
        assert_eq!(summary.source_manifest_references, 2);
    }

    #[test]
    fn profile_validate_summary_reports_profile_count() {
        let summary = ProfilePackLintSummary {
            files: vec![ProfilePackLintFile {
                path: PathBuf::from("configs/profiles/local-discovery-generic/profile.yaml"),
                profile_id: "local-discovery-generic".to_string(),
                ranking_config_dir: PathBuf::from("configs/ranking"),
                reason_catalog_path: PathBuf::from(
                    "configs/profiles/local-discovery-generic/reasons.yaml",
                ),
                schema_version: 1,
                kind: ProfilePackKind::ProfilePack,
                manifest_version: 1,
                supported_content_kinds: Vec::new(),
                reason_count: 14,
                fixture_count: 1,
                source_manifest_count: 0,
                event_csv_example_count: 0,
                optional_crawler_manifest_count: 0,
            }],
            ranking_configs: Vec::new(),
        };

        let rendered = format_profile_validate_summary(&summary);

        assert!(rendered.contains("profile validate ok: profile_packs=1"));
        assert!(rendered.contains("profile_id=local-discovery-generic"));
    }

    #[test]
    fn profile_inspect_summary_reports_runtime_paths() {
        let manifest_path = PathBuf::from("configs/profiles/local-discovery-generic/profile.yaml");
        let manifest: ProfilePackManifest = serde_yaml::from_str(
            r#"schema_version: 1
kind: profile_pack
manifest_version: 1
profile_id: local-discovery-generic
display_name: Local Discovery Generic
supported_content_kinds:
  - school
context_inputs:
  - station
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
            reason_catalog_path: PathBuf::from(
                "configs/profiles/local-discovery-generic/reasons.yaml",
            ),
            schema_version: 1,
            kind: ProfilePackKind::ProfilePack,
            manifest_version: 1,
            supported_content_kinds: manifest.supported_content_kinds.clone(),
            reason_count: 14,
            fixture_count: 0,
            source_manifest_count: 0,
            event_csv_example_count: 0,
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
        assert!(rendered.contains(&format!(
            "runtime_ranking_config_dir={}",
            ranking_config_dir.display()
        )));
        assert!(rendered.contains("runtime_fixture_set_id=minimal"));
        assert!(rendered.contains(&format!("runtime_fixture_dir={}", fixture_dir.display())));
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
schema_version: 1
kind: profile_pack
manifest_version: 1
profile_id: root-profile
display_name: Root Profile
supported_content_kinds:
  - school
context_inputs:
  - station
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
schema_version: 1
kind: profile_pack
manifest_version: 1
profile_id: other-profile
display_name: Other Profile
supported_content_kinds:
  - school
context_inputs:
  - station
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
