use anyhow::Result;
use async_trait::async_trait;
use domain::{RankingDataset, RankingQuery, SchoolStationLink, UserEvent};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const CANDIDATE_RETRIEVAL_ORDERING_CONTRACT: [&str; 5] = [
    "direct_station",
    "distance_meters",
    "walking_minutes",
    "school_id",
    "station_id",
];
const CANDIDATE_RETRIEVAL_OPENSEARCH_SORT_CONTRACT: [(&str, &str); 5] = [
    ("_score", "desc"),
    ("distance_meters", "asc"),
    ("walking_minutes", "asc"),
    ("school_id", "asc"),
    ("station_id", "asc"),
];

pub fn candidate_retrieval_ordering_contract() -> &'static [&'static str] {
    &CANDIDATE_RETRIEVAL_ORDERING_CONTRACT
}

pub fn candidate_retrieval_opensearch_sort_contract() -> &'static [(&'static str, &'static str)] {
    &CANDIDATE_RETRIEVAL_OPENSEARCH_SORT_CONTRACT
}

pub fn sort_candidate_links_for_retrieval(
    links: &mut [SchoolStationLink],
    target_station_id: &str,
) {
    links.sort_by(|left, right| {
        let left_is_not_direct = left.station_id != target_station_id;
        let right_is_not_direct = right.station_id != target_station_id;
        left_is_not_direct
            .cmp(&right_is_not_direct)
            .then_with(|| left.distance_meters.cmp(&right.distance_meters))
            .then_with(|| left.walking_minutes.cmp(&right.walking_minutes))
            .then_with(|| left.school_id.cmp(&right.school_id))
            .then_with(|| left.station_id.cmp(&right.station_id))
    });
}

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SnapshotTuning {
    pub search_execute_school_signal_weight: f64,
    pub search_execute_area_signal_weight: f64,
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
    async fn refresh_popularity_snapshots(
        &self,
        tuning: SnapshotTuning,
    ) -> Result<SnapshotRefreshStats>;
    async fn refresh_user_affinity_snapshots(
        &self,
        user_id: Option<&str>,
    ) -> Result<SnapshotRefreshStats>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retrieval_sort_contract_keeps_direct_station_first_then_tiebreakers() {
        let mut links = vec![
            school_station_link("school_b", "st_b", 8, 120),
            school_station_link("school_a", "st_target", 30, 300),
            school_station_link("school_a", "st_a", 8, 120),
            school_station_link("school_a", "st_c", 6, 120),
            school_station_link("school_c", "st_c", 5, 90),
        ];

        sort_candidate_links_for_retrieval(&mut links, "st_target");

        assert_eq!(
            links
                .iter()
                .map(|link| format!("{}@{}", link.school_id, link.station_id))
                .collect::<Vec<_>>(),
            vec![
                "school_a@st_target",
                "school_c@st_c",
                "school_a@st_c",
                "school_a@st_a",
                "school_b@st_b"
            ]
        );
    }

    fn school_station_link(
        school_id: &str,
        station_id: &str,
        walking_minutes: u16,
        distance_meters: u32,
    ) -> SchoolStationLink {
        SchoolStationLink {
            school_id: school_id.to_string(),
            station_id: station_id.to_string(),
            walking_minutes,
            distance_meters,
            hop_distance: 1,
            line_name: "JR Yamanote Line".to_string(),
        }
    }
}
