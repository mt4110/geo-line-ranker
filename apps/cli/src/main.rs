use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Context;
use clap::{Parser, Subcommand};
use cli::{
    format_fixture_doctor_summary, format_job_enqueue_summary, format_job_inspection,
    format_job_list, format_job_mutation_summary, format_replay_evaluation_summary,
    format_snapshot_refresh_summary, format_summary, generate_demo_jp_fixture,
    run_derive_school_station_links, run_event_csv_import, run_fixture_doctor, run_import_command,
    run_job_due, run_job_enqueue, run_job_inspect, run_job_list, run_job_retry,
    run_replay_evaluate, run_snapshot_refresh, ImportTarget,
};
use config::{
    lint_profile_pack_dir, lint_ranking_config_dir, load_profile_pack_manifest,
    resolve_profile_pack_runtime_selection, resolve_runtime_path, AppSettings,
    ProfilePackLintSummary, RankingConfigLintSummary, DEFAULT_PROFILE_ID,
    DEFAULT_PROFILE_PACKS_DIR,
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
            help = "Profile pack directory or profile.yaml file to lint. Defaults to PROFILE_PACKS_DIR or configs/profiles."
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
        },
        Command::Config { target } => match target {
            ConfigCommand::Lint {
                path,
                profiles_path,
            } => {
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
                let ranking_summary =
                    match cached_ranking_summary_for_path(&profile_summary, &path)? {
                        Some(summary) => ranking_summary_with_base_path(summary, &path),
                        None => lint_ranking_config_dir(&path)?,
                    };
                println!(
                    "{}",
                    format_config_lint_summary(
                        active_profile
                            .as_ref()
                            .map(|profile| profile.profile_id.as_str()),
                        active_profile
                            .as_ref()
                            .and_then(|profile| profile.fixture_set_id.as_deref()),
                        &ranking_summary,
                        &profile_summary
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

fn active_profile_selection_for_lint(
    profiles_path: &Path,
) -> anyhow::Result<config::ProfilePackRuntimeSelection> {
    let profile_id = profile_id_for_lint(profiles_path)?;
    let fixture_set_id = config::env_optional_non_empty("PROFILE_FIXTURE_SET_ID")?;
    resolve_profile_pack_runtime_selection(profiles_path, &profile_id, fixture_set_id.as_deref())
}

fn profile_id_for_lint(profiles_path: &Path) -> anyhow::Result<String> {
    match std::env::var("PROFILE_ID") {
        Ok(raw) => Ok(raw),
        Err(std::env::VarError::NotPresent) if profiles_path.is_file() => {
            Ok(load_profile_pack_manifest(profiles_path)?.profile_id)
        }
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_PROFILE_ID.to_string()),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("PROFILE_ID must be valid unicode"),
    }
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
    lines.extend(profiles.files.iter().map(|file| {
        let content_kinds = file
            .supported_content_kinds
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "- {} profile_id={} schema_version={} kind={} manifest_version={} content_kinds={} reasons={} fixtures={} source_manifests={} optional_crawler_manifests={}",
            file.path.display(),
            file.profile_id,
            file.schema_version,
            file.kind.as_str(),
            file.manifest_version,
            content_kinds,
            file.reason_count,
            file.fixture_count,
            file.source_manifest_count,
            file.optional_crawler_manifest_count
        )
    }));
    lines.join("\n")
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
