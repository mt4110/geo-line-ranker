use std::sync::Arc;

use api::AppState;
use cache::RecommendationCache;
use clap::{Parser, Subcommand};
use config::{AppSettings, RankingProfiles};
use observability::init_tracing;
use ranking::RankingEngine;
use storage_opensearch::OpenSearchStore;
use storage_postgres::PgRepository;

#[derive(Debug, Parser)]
#[command(name = "geo-line-ranker-api")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing("info,hyper=warn,tower_http=info");

    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => serve().await,
    }
}

async fn serve() -> anyhow::Result<()> {
    let settings = AppSettings::from_env()?;
    let profiles = RankingProfiles::load_from_dir(&settings.ranking_config_dir)?;
    let profile_version = profiles.profile_version.clone();
    let neighbor_distance_cap_meters = profiles.fallback.neighbor_distance_cap_meters;
    let candidate_backend = if settings.candidate_retrieval_mode.is_full() {
        api::CandidateBackend::Full(OpenSearchStore::new(&settings.opensearch)?)
    } else {
        api::CandidateBackend::SqlOnly
    };
    let state = AppState {
        repository: Arc::new(PgRepository::new(settings.database_url.clone())),
        engine: RankingEngine::new(profiles, settings.algorithm_version.clone()),
        cache: RecommendationCache::new(
            settings.redis_url.clone(),
            settings.recommendation_cache_ttl_secs,
        ),
        profile_version,
        algorithm_version: settings.algorithm_version.clone(),
        candidate_retrieval_mode: settings.candidate_retrieval_mode,
        candidate_retrieval_limit: settings.candidate_retrieval_limit,
        neighbor_distance_cap_meters,
        candidate_backend,
        worker_max_attempts: settings.worker_max_attempts,
    };

    let app = api::build_app(state);
    let listener = tokio::net::TcpListener::bind(&settings.bind_addr).await?;
    tracing::info!("api listening on http://{}", settings.bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}
