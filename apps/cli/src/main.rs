use std::{fs, path::PathBuf};

use clap::{Parser, Subcommand};
use cli::{
    format_job_enqueue_summary, format_job_inspection, format_job_list,
    format_job_mutation_summary, format_replay_evaluation_summary, format_snapshot_refresh_summary,
    format_summary, generate_demo_jp_fixture, run_derive_school_station_links,
    run_event_csv_import, run_import_command, run_job_due, run_job_enqueue, run_job_inspect,
    run_job_list, run_job_retry, run_replay_evaluate, run_snapshot_refresh, ImportTarget,
};
use config::AppSettings;
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
    let settings = AppSettings::from_env()?;
    let cli = Cli::parse();

    match cli.command {
        Command::Migrate => {
            run_migrations(&settings.database_url, "storage/migrations/postgres").await?;
        }
        Command::Seed { target } => match target {
            SeedTarget::Example => {
                seed_fixture(&settings.database_url, &settings.fixture_dir).await?
            }
        },
        Command::Import { target } => {
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
                let summary = run_derive_school_station_links(&settings).await?;
                println!("{}", format_summary(&summary));
            }
        },
        Command::Fixtures { target } => match target {
            FixtureCommand::GenerateDemoJp { output } => {
                let files = generate_demo_jp_fixture(output)?;
                println!("generated {} fixture files", files.len());
            }
        },
        Command::Index { target } => match target {
            IndexCommand::Rebuild => {
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
                let summary = run_snapshot_refresh(&settings).await?;
                println!("{}", format_snapshot_refresh_summary(&summary));
            }
        },
        Command::Replay { target } => match target {
            ReplayCommand::Evaluate {
                limit,
                fail_on_mismatch,
            } => {
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
        Command::Jobs { target } => match target {
            JobsCommand::List { limit } => {
                let summary = run_job_list(&settings, limit).await?;
                println!("{}", format_job_list(&summary));
            }
            JobsCommand::Inspect { id } => {
                let inspection = run_job_inspect(&settings, id).await?;
                println!("{}", format_job_inspection(&inspection));
            }
            JobsCommand::Retry { id } => {
                let summary = run_job_retry(&settings, id).await?;
                println!("{}", format_job_mutation_summary("retry", &summary));
            }
            JobsCommand::Due { id } => {
                let summary = run_job_due(&settings, id).await?;
                println!("{}", format_job_mutation_summary("due", &summary));
            }
            JobsCommand::Enqueue {
                job_type,
                payload,
                max_attempts,
            } => {
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
