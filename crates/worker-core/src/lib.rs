use std::{sync::Arc, time::Duration};

use anyhow::{bail, Result};
use cache::RecommendationCache;
use observability::{job_failed, job_started, job_succeeded};
use storage::{CandidateProjectionSync, ClaimedJob, JobType, RecommendationRepository};

#[derive(Clone)]
pub struct WorkerService<R> {
    repository: Arc<R>,
    cache: RecommendationCache,
    worker_id: String,
    projection_sync: Option<Arc<dyn CandidateProjectionSync>>,
    retry_delay_secs: u64,
}

impl<R> WorkerService<R>
where
    R: RecommendationRepository + 'static,
{
    pub fn new(
        repository: Arc<R>,
        cache: RecommendationCache,
        worker_id: impl Into<String>,
        projection_sync: Option<Arc<dyn CandidateProjectionSync>>,
        retry_delay_secs: u64,
    ) -> Self {
        Self {
            repository,
            cache,
            worker_id: worker_id.into(),
            projection_sync,
            retry_delay_secs,
        }
    }

    pub async fn serve(&self, poll_interval: Duration) -> Result<()> {
        loop {
            if self.run_once().await? {
                continue;
            }

            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!(worker_id = %self.worker_id, "worker received shutdown signal");
                    break;
                }
                _ = tokio::time::sleep(poll_interval) => {}
            }
        }

        Ok(())
    }

    pub async fn run_until_empty(&self, max_jobs: usize) -> Result<usize> {
        let mut processed = 0;
        while processed < max_jobs && self.run_once().await? {
            processed += 1;
        }
        Ok(processed)
    }

    pub async fn run_once(&self) -> Result<bool> {
        let Some(job) = self.repository.claim_next_job(&self.worker_id).await? else {
            return Ok(false);
        };

        let job_type = job.job_type.as_str().to_string();
        job_started(&self.worker_id, job.job_id, &job_type, job.attempt_number);

        match self.process_job(&job).await {
            Ok(()) => {
                self.repository
                    .mark_job_succeeded(job.job_id, job.attempt_id)
                    .await?;
                job_succeeded(&self.worker_id, job.job_id, &job_type);
            }
            Err(error) => {
                job_failed(&self.worker_id, job.job_id, &job_type, &error.to_string());
                self.repository
                    .mark_job_failed(
                        job.job_id,
                        job.attempt_id,
                        &error.to_string(),
                        self.retry_delay_secs,
                    )
                    .await?;
            }
        }

        Ok(true)
    }

    async fn process_job(&self, job: &ClaimedJob) -> Result<()> {
        match job.job_type {
            JobType::RefreshPopularitySnapshot => {
                let stats = self.repository.refresh_popularity_snapshots().await?;
                tracing::info!(
                    worker_id = %self.worker_id,
                    job_id = job.job_id,
                    refreshed_rows = stats.refreshed_rows,
                    related_rows = stats.related_rows,
                    "refreshed popularity and area snapshots"
                );
                Ok(())
            }
            JobType::RefreshUserAffinitySnapshot => {
                let user_id = job.payload.get("user_id").and_then(|value| value.as_str());
                let stats = self
                    .repository
                    .refresh_user_affinity_snapshots(user_id)
                    .await?;
                tracing::info!(
                    worker_id = %self.worker_id,
                    job_id = job.job_id,
                    user_id = user_id.unwrap_or("*"),
                    refreshed_rows = stats.refreshed_rows,
                    "refreshed user affinity snapshots"
                );
                Ok(())
            }
            JobType::InvalidateRecommendationCache => {
                let deleted = self.cache.invalidate_recommendations().await?;
                tracing::info!(
                    worker_id = %self.worker_id,
                    job_id = job.job_id,
                    deleted_keys = deleted,
                    "invalidated recommendation cache"
                );
                Ok(())
            }
            JobType::SyncCandidateProjection => {
                let Some(projection_sync) = self.projection_sync.as_ref() else {
                    bail!("candidate projection sync requested but full mode is not configured");
                };
                let stats = projection_sync.sync_projection().await?;
                tracing::info!(
                    worker_id = %self.worker_id,
                    job_id = job.job_id,
                    indexed_documents = stats.indexed_documents,
                    deleted_documents = stats.deleted_documents,
                    "synced OpenSearch candidate projection"
                );
                Ok(())
            }
        }
    }
}
