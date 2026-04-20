use std::{future::Future, sync::Arc, time::Duration};

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
        self.serve_until(poll_interval, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    }

    async fn serve_until<S>(&self, poll_interval: Duration, shutdown: S) -> Result<()>
    where
        S: Future<Output = ()>,
    {
        tokio::pin!(shutdown);

        loop {
            let processed_job = self.run_once().await?;
            if Self::shutdown_requested(&mut shutdown).await {
                tracing::info!(worker_id = %self.worker_id, "worker received shutdown signal");
                break;
            }

            if processed_job {
                continue;
            }

            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!(worker_id = %self.worker_id, "worker received shutdown signal");
                    break;
                }
                _ = tokio::time::sleep(poll_interval) => {}
            }
        }

        Ok(())
    }

    async fn shutdown_requested<S>(shutdown: &mut std::pin::Pin<&mut S>) -> bool
    where
        S: Future<Output = ()>,
    {
        tokio::select! {
            _ = shutdown => true,
            else => false,
        }
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use domain::{RankingDataset, RankingQuery, UserEvent};
    use serde_json::json;
    use storage::{NewJob, RecommendationTrace, SnapshotRefreshStats};
    use tokio::sync::{oneshot, Notify};

    use super::*;

    #[derive(Default)]
    struct FakeRepository {
        claim_count: AtomicUsize,
        success_count: AtomicUsize,
        job_started: Notify,
        release_job: Notify,
    }

    #[async_trait]
    impl RecommendationRepository for FakeRepository {
        async fn health_check(&self) -> Result<()> {
            Ok(())
        }

        async fn ready_check(&self) -> Result<()> {
            Ok(())
        }

        async fn load_dataset(&self, _query: &RankingQuery) -> Result<RankingDataset> {
            unreachable!("load_dataset is not used in worker-core tests")
        }

        async fn record_trace(&self, _trace: &RecommendationTrace) -> Result<()> {
            unreachable!("record_trace is not used in worker-core tests")
        }

        async fn record_user_event(&self, _event: &UserEvent) -> Result<i64> {
            unreachable!("record_user_event is not used in worker-core tests")
        }

        async fn enqueue_job(&self, _job: &NewJob) -> Result<i64> {
            unreachable!("enqueue_job is not used in worker-core tests")
        }

        async fn claim_next_job(&self, _worker_id: &str) -> Result<Option<ClaimedJob>> {
            let job_id = self.claim_count.fetch_add(1, Ordering::SeqCst) as i64 + 1;
            Ok(Some(ClaimedJob {
                job_id,
                attempt_id: job_id,
                attempt_number: 1,
                max_attempts: 3,
                job_type: JobType::RefreshPopularitySnapshot,
                payload: json!({}),
            }))
        }

        async fn mark_job_succeeded(&self, _job_id: i64, _attempt_id: i64) -> Result<()> {
            self.success_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn mark_job_failed(
            &self,
            _job_id: i64,
            _attempt_id: i64,
            _error_message: &str,
            _retry_delay_secs: u64,
        ) -> Result<()> {
            unreachable!("mark_job_failed is not used in worker-core tests")
        }

        async fn refresh_popularity_snapshots(&self) -> Result<SnapshotRefreshStats> {
            self.job_started.notify_one();
            self.release_job.notified().await;
            Ok(SnapshotRefreshStats::default())
        }

        async fn refresh_user_affinity_snapshots(
            &self,
            _user_id: Option<&str>,
        ) -> Result<SnapshotRefreshStats> {
            unreachable!("refresh_user_affinity_snapshots is not used in worker-core tests")
        }
    }

    #[tokio::test]
    async fn serve_stops_after_current_job_when_shutdown_arrives() -> Result<()> {
        let repository = Arc::new(FakeRepository::default());
        let service = WorkerService::new(
            repository.clone(),
            RecommendationCache::new(None, 60),
            "worker-test",
            None,
            5,
        );
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let serve_task = tokio::spawn({
            async move {
                service
                    .serve_until(Duration::from_secs(60), async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
            }
        });

        repository.job_started.notified().await;
        shutdown_tx.send(()).expect("shutdown signal");
        repository.release_job.notify_one();

        serve_task.await??;

        assert_eq!(repository.claim_count.load(Ordering::SeqCst), 1);
        assert_eq!(repository.success_count.load(Ordering::SeqCst), 1);

        Ok(())
    }
}
