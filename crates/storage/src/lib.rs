use anyhow::Result;
use async_trait::async_trait;
use domain::{RankingDataset, RankingQuery, UserEvent};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct RecommendationTrace {
    pub request_payload: Value,
    pub response_payload: Value,
    pub trace_payload: Value,
    pub fallback_stage: String,
    pub algorithm_version: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    RefreshPopularitySnapshot,
    RefreshUserAffinitySnapshot,
    InvalidateRecommendationCache,
    SyncCandidateProjection,
}

impl JobType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RefreshPopularitySnapshot => "refresh_popularity_snapshot",
            Self::RefreshUserAffinitySnapshot => "refresh_user_affinity_snapshot",
            Self::InvalidateRecommendationCache => "invalidate_recommendation_cache",
            Self::SyncCandidateProjection => "sync_candidate_projection",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "refresh_popularity_snapshot" => Some(Self::RefreshPopularitySnapshot),
            "refresh_user_affinity_snapshot" => Some(Self::RefreshUserAffinitySnapshot),
            "invalidate_recommendation_cache" => Some(Self::InvalidateRecommendationCache),
            "sync_candidate_projection" => Some(Self::SyncCandidateProjection),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewJob {
    pub job_type: JobType,
    pub payload: Value,
    pub max_attempts: i32,
}

#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub job_id: i64,
    pub attempt_id: i64,
    pub attempt_number: i32,
    pub max_attempts: i32,
    pub job_type: JobType,
    pub payload: Value,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotRefreshStats {
    pub refreshed_rows: i64,
    pub related_rows: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectionSyncStats {
    pub indexed_documents: i64,
    pub deleted_documents: i64,
}

#[async_trait]
pub trait CandidateProjectionSync: Send + Sync {
    async fn sync_projection(&self) -> Result<ProjectionSyncStats>;
}

#[async_trait]
pub trait RecommendationRepository: Send + Sync {
    async fn health_check(&self) -> Result<()>;
    async fn ready_check(&self) -> Result<()>;
    async fn load_dataset(&self, query: &RankingQuery) -> Result<RankingDataset>;
    async fn record_trace(&self, trace: &RecommendationTrace) -> Result<()>;
    async fn record_user_event(&self, event: &UserEvent) -> Result<i64>;
    async fn enqueue_job(&self, job: &NewJob) -> Result<i64>;
    async fn claim_next_job(&self, worker_id: &str) -> Result<Option<ClaimedJob>>;
    async fn mark_job_succeeded(&self, job_id: i64, attempt_id: i64) -> Result<()>;
    async fn mark_job_failed(
        &self,
        job_id: i64,
        attempt_id: i64,
        error_message: &str,
        retry_delay_secs: u64,
    ) -> Result<()>;
    async fn refresh_popularity_snapshots(&self) -> Result<SnapshotRefreshStats>;
    async fn refresh_user_affinity_snapshots(
        &self,
        user_id: Option<&str>,
    ) -> Result<SnapshotRefreshStats>;
}
