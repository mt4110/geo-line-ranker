use anyhow::Result;
use cache::RecommendationCache;
use config::{AppSettings, RankingProfiles};
use storage::{RecommendationRepository, SnapshotTuning};
use storage_opensearch::ProjectionSyncService;

use crate::repository::pg_repository;

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
