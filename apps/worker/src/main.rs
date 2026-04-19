use std::{sync::Arc, time::Duration};

use cache::RecommendationCache;
use clap::{Parser, Subcommand};
use config::AppSettings;
use observability::init_tracing;
use storage::{CandidateProjectionSync, RecommendationRepository};
use storage_opensearch::ProjectionSyncService;
use storage_postgres::PgRepository;
use worker_core::WorkerService;

#[derive(Debug, Parser)]
#[command(name = "geo-line-ranker-worker")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve,
    RunOnce {
        #[arg(long, default_value_t = 10)]
        max_jobs: usize,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing("info");

    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => serve().await,
        Command::RunOnce { max_jobs } => run_once(max_jobs).await,
    }
}

async fn serve() -> anyhow::Result<()> {
    let settings = AppSettings::from_env();
    let repository = Arc::new(PgRepository::new(settings.database_url.clone()));
    let cache = RecommendationCache::new(
        settings.redis_url.clone(),
        settings.recommendation_cache_ttl_secs,
    );
    let projection_sync = build_projection_sync(&settings)?;
    let worker_id = format!("worker-{}", std::process::id());
    let service = WorkerService::new(
        Arc::clone(&repository),
        cache,
        worker_id.clone(),
        projection_sync,
        settings.worker_retry_delay_secs,
    );

    match repository.health_check().await {
        Ok(_) => tracing::info!(worker_id = %worker_id, "worker connected to database"),
        Err(error) => {
            tracing::warn!(worker_id = %worker_id, %error, "worker started without a ready database connection")
        }
    }

    service
        .serve(Duration::from_millis(settings.worker_poll_interval_ms))
        .await
}

async fn run_once(max_jobs: usize) -> anyhow::Result<()> {
    let settings = AppSettings::from_env();
    let repository = Arc::new(PgRepository::new(settings.database_url.clone()));
    let cache = RecommendationCache::new(
        settings.redis_url.clone(),
        settings.recommendation_cache_ttl_secs,
    );
    let projection_sync = build_projection_sync(&settings)?;
    let worker_id = format!("worker-once-{}", std::process::id());
    let service = WorkerService::new(
        repository,
        cache,
        worker_id,
        projection_sync,
        settings.worker_retry_delay_secs,
    );
    let processed = service.run_until_empty(max_jobs).await?;
    tracing::info!(processed_jobs = processed, "worker run-once completed");
    Ok(())
}

fn build_projection_sync(
    settings: &AppSettings,
) -> anyhow::Result<Option<Arc<dyn CandidateProjectionSync>>> {
    if !settings.candidate_retrieval_mode.is_full() {
        return Ok(None);
    }

    Ok(Some(Arc::new(ProjectionSyncService::new(
        settings.database_url.clone(),
        &settings.opensearch,
    )?) as Arc<dyn CandidateProjectionSync>))
}
