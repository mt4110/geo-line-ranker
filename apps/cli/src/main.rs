use std::{fs, path::PathBuf};

use clap::{Parser, Subcommand};
use cli::{
    format_summary, generate_demo_jp_fixture, run_derive_school_station_links,
    run_event_csv_import, run_import_command, ImportTarget,
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
