use std::collections::BTreeSet;

use anyhow::Result;
use async_trait::async_trait;
use domain::{RankingDataset, RankingQuery, SchoolStationLink, UserEvent};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod graph;

pub use graph::{
    AreaClusterDiagnostic, GeoGraph, GeoGraphEdge, InterchangeDiagnostic, LineGraph, LineGraphEdge,
    StationHopDiagnostic,
};

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
const CANDIDATE_PLAN_STAGE_STATUSES: [&str; 3] = ["selected", "insufficient", "skipped"];

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
    pub context_evidence_summary: Option<RecommendationTraceContextEvidenceSummary>,
    pub candidate_plan_trace: Option<RecommendationTraceCandidatePlanTrace>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecommendationTraceContextEvidenceSummary {
    pub context_source: String,
    pub confidence: f64,
    pub privacy_level: String,
    pub primary_kind: String,
    pub evidence_count: i64,
    pub strongest_strength: f64,
    pub has_search_execute: bool,
    pub warning_count: i64,
    pub evidence_payload: Value,
}

impl RecommendationTraceContextEvidenceSummary {
    pub fn validate(&self) -> Result<()> {
        ensure_non_empty("context_source", &self.context_source)?;
        ensure_non_empty("privacy_level", &self.privacy_level)?;
        ensure_non_empty("primary_kind", &self.primary_kind)?;
        ensure_non_negative_i64("evidence_count", self.evidence_count)?;
        ensure_non_negative_i64("warning_count", self.warning_count)?;
        ensure_non_negative_f64("confidence", self.confidence)?;
        ensure_non_negative_f64("strongest_strength", self.strongest_strength)?;
        ensure_json_object("evidence_payload", &self.evidence_payload)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecommendationTraceCandidatePlanTrace {
    pub minimum_candidate_count: i64,
    pub selected_stage: String,
    pub stop_reason: String,
    pub area_context_usable: bool,
    pub plan_payload: Value,
    pub stages: Vec<RecommendationTraceCandidatePlanStage>,
}

impl RecommendationTraceCandidatePlanTrace {
    pub fn validate(&self) -> Result<()> {
        ensure_non_negative_i64("minimum_candidate_count", self.minimum_candidate_count)?;
        ensure_non_empty("selected_stage", &self.selected_stage)?;
        ensure_non_empty("stop_reason", &self.stop_reason)?;
        ensure_json_object("plan_payload", &self.plan_payload)?;
        let mut stage_orders = BTreeSet::new();
        for stage in &self.stages {
            stage.validate()?;
            anyhow::ensure!(
                stage_orders.insert(stage.stage_order),
                "stage_order must be unique within candidate plan trace: {}",
                stage.stage_order
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecommendationTraceCandidatePlanStage {
    pub stage_order: i32,
    pub stage: String,
    pub candidate_count: i64,
    pub required_min_candidates: i64,
    pub status: String,
    pub reason_code: String,
    pub stage_payload: Value,
}

impl RecommendationTraceCandidatePlanStage {
    pub fn validate(&self) -> Result<()> {
        ensure_non_negative("stage_order", self.stage_order)?;
        ensure_non_empty("stage", &self.stage)?;
        ensure_non_negative_i64("candidate_count", self.candidate_count)?;
        ensure_non_negative_i64("required_min_candidates", self.required_min_candidates)?;
        ensure_non_empty("status", &self.status)?;
        anyhow::ensure!(
            CANDIDATE_PLAN_STAGE_STATUSES.contains(&self.status.as_str()),
            "status must be one of selected, insufficient, skipped"
        );
        ensure_non_empty("reason_code", &self.reason_code)?;
        ensure_json_object("stage_payload", &self.stage_payload)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AreaAdjacency {
    pub from_area_id: String,
    pub to_area_id: String,
    pub adjacency_kind: String,
    pub distance_meters: Option<f64>,
    pub area_cluster_id: Option<String>,
    pub source_id: Option<String>,
    pub source_version: Option<String>,
    #[serde(default = "default_json_object")]
    pub attributes: Value,
}

impl AreaAdjacency {
    pub fn validate(&self) -> Result<()> {
        ensure_non_empty("from_area_id", &self.from_area_id)?;
        ensure_non_empty("to_area_id", &self.to_area_id)?;
        anyhow::ensure!(
            self.from_area_id.trim() != self.to_area_id.trim(),
            "area adjacency must connect different areas"
        );
        ensure_non_empty("adjacency_kind", &self.adjacency_kind)?;
        ensure_optional_non_negative_f64("distance_meters", self.distance_meters)?;
        ensure_optional_non_empty("area_cluster_id", self.area_cluster_id.as_deref())?;
        ensure_optional_non_empty("source_id", self.source_id.as_deref())?;
        ensure_optional_non_empty("source_version", self.source_version.as_deref())?;
        ensure_json_object("attributes", &self.attributes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LineAdjacency {
    pub from_line_id: String,
    pub to_line_id: String,
    pub adjacency_kind: String,
    pub interchange_station_id: Option<String>,
    pub station_hop_count: Option<i32>,
    #[serde(default = "default_true")]
    pub requires_transfer: bool,
    pub source_id: Option<String>,
    pub source_version: Option<String>,
    #[serde(default = "default_json_object")]
    pub attributes: Value,
}

impl LineAdjacency {
    pub fn validate(&self) -> Result<()> {
        ensure_non_empty("from_line_id", &self.from_line_id)?;
        ensure_non_empty("to_line_id", &self.to_line_id)?;
        anyhow::ensure!(
            self.from_line_id.trim() != self.to_line_id.trim(),
            "line adjacency must connect different lines"
        );
        ensure_non_empty("adjacency_kind", &self.adjacency_kind)?;
        ensure_optional_non_empty(
            "interchange_station_id",
            self.interchange_station_id.as_deref(),
        )?;
        ensure_optional_non_negative("station_hop_count", self.station_hop_count)?;
        ensure_optional_non_empty("source_id", self.source_id.as_deref())?;
        ensure_optional_non_empty("source_version", self.source_version.as_deref())?;
        ensure_json_object("attributes", &self.attributes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionContextSummary {
    pub session_id_hash: String,
    pub context_source: String,
    pub confidence: f64,
    pub privacy_level: String,
    pub primary_kind: String,
    pub evidence_count: i64,
    pub search_execute_count: i64,
    pub warning_count: i64,
    pub area_id: Option<String>,
    pub line_id: Option<String>,
    pub station_id: Option<String>,
    #[serde(default = "default_json_object")]
    pub summary_payload: Value,
    pub first_seen_at: String,
    pub last_seen_at: String,
    pub updated_at: String,
}

impl SessionContextSummary {
    pub fn validate(&self) -> Result<()> {
        ensure_sha256_hex("session_id_hash", &self.session_id_hash)?;
        ensure_non_empty("context_source", &self.context_source)?;
        ensure_non_negative_f64("confidence", self.confidence)?;
        ensure_non_empty("privacy_level", &self.privacy_level)?;
        ensure_non_empty("primary_kind", &self.primary_kind)?;
        ensure_non_negative_i64("evidence_count", self.evidence_count)?;
        ensure_non_negative_i64("search_execute_count", self.search_execute_count)?;
        ensure_non_negative_i64("warning_count", self.warning_count)?;
        ensure_optional_non_empty("area_id", self.area_id.as_deref())?;
        ensure_optional_non_empty("line_id", self.line_id.as_deref())?;
        ensure_optional_non_empty("station_id", self.station_id.as_deref())?;
        ensure_json_object("summary_payload", &self.summary_payload)?;
        ensure_non_empty("first_seen_at", &self.first_seen_at)?;
        ensure_non_empty("last_seen_at", &self.last_seen_at)?;
        ensure_non_empty("updated_at", &self.updated_at)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ProfileManifestRecord {
    pub profile_id: String,
    pub display_name: String,
    pub schema_version: i32,
    pub manifest_kind: String,
    pub manifest_version: i32,
    pub compatibility_level: String,
    pub default_locale: Option<String>,
    pub description: Option<String>,
    pub manifest_path: String,
    pub manifest_checksum_sha256: String,
    pub manifest_payload: Value,
    pub ranking_config_dir: String,
    pub reason_catalog_path: String,
    pub content_kind_registry: Vec<String>,
    pub supported_content_kinds: Vec<String>,
    pub context_inputs: Vec<String>,
    pub placements: Vec<String>,
    pub fallback_policy: String,
    pub fixture_count: i32,
    pub connector_count: i32,
    pub evaluation_reference_count: i32,
}

impl ProfileManifestRecord {
    pub fn validate(&self) -> Result<()> {
        ensure_non_empty("profile_id", &self.profile_id)?;
        ensure_non_empty("display_name", &self.display_name)?;
        ensure_non_empty("manifest_kind", &self.manifest_kind)?;
        ensure_non_empty("compatibility_level", &self.compatibility_level)?;
        ensure_non_empty("manifest_path", &self.manifest_path)?;
        ensure_non_empty("manifest_checksum_sha256", &self.manifest_checksum_sha256)?;
        ensure_non_empty("ranking_config_dir", &self.ranking_config_dir)?;
        ensure_non_empty("reason_catalog_path", &self.reason_catalog_path)?;
        ensure_non_empty("fallback_policy", &self.fallback_policy)?;
        ensure_positive("schema_version", self.schema_version)?;
        ensure_positive("manifest_version", self.manifest_version)?;
        ensure_non_negative("fixture_count", self.fixture_count)?;
        ensure_non_negative("connector_count", self.connector_count)?;
        ensure_non_negative(
            "evaluation_reference_count",
            self.evaluation_reference_count,
        )?;
        anyhow::ensure!(
            self.manifest_checksum_sha256.len() == 64
                && self
                    .manifest_checksum_sha256
                    .chars()
                    .all(|value| value.is_ascii_hexdigit()),
            "manifest_checksum_sha256 must be a 64-character hex digest"
        );
        ensure_json_object("manifest_payload", &self.manifest_payload)?;
        ensure_non_empty_string_list("content_kind_registry", &self.content_kind_registry)?;
        ensure_non_empty_string_list("supported_content_kinds", &self.supported_content_kinds)?;
        ensure_non_empty_string_list("context_inputs", &self.context_inputs)?;
        ensure_non_empty_string_list("placements", &self.placements)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProfileCompatibilityStatus {
    Valid,
    Warning,
    Blocked,
}

impl ProfileCompatibilityStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Warning => "warning",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProfileCompatibilityStatusRecord {
    pub profile_id: String,
    pub compatibility_level: String,
    pub status: ProfileCompatibilityStatus,
    pub evidence: Value,
}

impl ProfileCompatibilityStatusRecord {
    pub fn validate(&self) -> Result<()> {
        ensure_non_empty("profile_id", &self.profile_id)?;
        ensure_non_empty("compatibility_level", &self.compatibility_level)?;
        ensure_json_object("evidence", &self.evidence)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationRunKind {
    Golden,
}

impl EvaluationRunKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Golden => "golden",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationRunStatus {
    Passed,
    Blocked,
    Failed,
}

impl EvaluationRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Blocked => "blocked",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationRunCaseStatus {
    Passed,
    Blocked,
}

impl EvaluationRunCaseStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EvaluationRunCaseRecord {
    pub case_id: String,
    pub title: String,
    pub path: String,
    pub status: EvaluationRunCaseStatus,
    pub expected_fallback_stage: String,
    pub actual_fallback_stage: Option<String>,
    pub expected_order: Vec<String>,
    pub actual_order: Vec<String>,
    pub checks_payload: Value,
}

impl EvaluationRunCaseRecord {
    pub fn validate(&self) -> Result<()> {
        ensure_non_empty("case_id", &self.case_id)?;
        ensure_non_empty("title", &self.title)?;
        ensure_non_empty("path", &self.path)?;
        ensure_non_empty("expected_fallback_stage", &self.expected_fallback_stage)?;
        ensure_string_list("expected_order", &self.expected_order)?;
        ensure_string_list("actual_order", &self.actual_order)?;
        ensure_json_array("checks_payload", &self.checks_payload)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct EvaluationRunRecord {
    pub profile_id: Option<String>,
    pub profile_manifest_lineage_id: Option<i64>,
    pub run_kind: EvaluationRunKind,
    pub scenario_source_kind: String,
    pub scenario_path: String,
    pub pairwise_pack_path: Option<String>,
    pub algorithm_version: String,
    pub status: EvaluationRunStatus,
    pub scenarios: i32,
    pub passed: i32,
    pub blocked: i32,
    pub blockers: i32,
    pub warnings: i32,
    pub summary_payload: Value,
    pub cases: Vec<EvaluationRunCaseRecord>,
}

impl EvaluationRunRecord {
    pub fn validate(&self) -> Result<()> {
        if let Some(profile_id) = self.profile_id.as_deref() {
            ensure_non_empty("profile_id", profile_id)?;
        }
        anyhow::ensure!(
            self.profile_manifest_lineage_id.is_none() || self.profile_id.is_some(),
            "profile_manifest_lineage_id requires profile_id"
        );
        ensure_non_empty("scenario_source_kind", &self.scenario_source_kind)?;
        ensure_non_empty("scenario_path", &self.scenario_path)?;
        ensure_non_empty("algorithm_version", &self.algorithm_version)?;
        ensure_json_object("summary_payload", &self.summary_payload)?;
        ensure_non_negative("scenarios", self.scenarios)?;
        ensure_non_negative("passed", self.passed)?;
        ensure_non_negative("blocked", self.blocked)?;
        ensure_non_negative("blockers", self.blockers)?;
        ensure_non_negative("warnings", self.warnings)?;
        let completed_cases = self
            .passed
            .checked_add(self.blocked)
            .ok_or_else(|| anyhow::anyhow!("passed plus blocked overflowed"))?;
        anyhow::ensure!(
            completed_cases == self.scenarios,
            "passed plus blocked must match scenarios"
        );
        let cases_len: i32 = self
            .cases
            .len()
            .try_into()
            .map_err(|_| anyhow::anyhow!("cases length is too large for storage"))?;
        anyhow::ensure!(
            cases_len == self.scenarios,
            "cases length must match scenarios"
        );
        let passed_cases: i32 = self
            .cases
            .iter()
            .filter(|case| case.status == EvaluationRunCaseStatus::Passed)
            .count()
            .try_into()
            .map_err(|_| anyhow::anyhow!("passed case count is too large for storage"))?;
        let blocked_cases: i32 = self
            .cases
            .iter()
            .filter(|case| case.status == EvaluationRunCaseStatus::Blocked)
            .count()
            .try_into()
            .map_err(|_| anyhow::anyhow!("blocked case count is too large for storage"))?;
        anyhow::ensure!(
            passed_cases == self.passed,
            "passed case count must match passed"
        );
        anyhow::ensure!(
            blocked_cases == self.blocked,
            "blocked case count must match blocked"
        );
        match self.status {
            EvaluationRunStatus::Passed => {
                anyhow::ensure!(
                    self.blocked == 0,
                    "passed runs must not include blocked cases"
                );
                anyhow::ensure!(self.blockers == 0, "passed runs must not include blockers");
            }
            EvaluationRunStatus::Blocked => {
                anyhow::ensure!(
                    self.blocked > 0 || self.blockers > 0,
                    "blocked runs must include blocked cases or blockers"
                );
            }
            EvaluationRunStatus::Failed => {}
        }
        for case in &self.cases {
            case.validate()?;
        }
        Ok(())
    }
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
pub trait GraphAdjacencyRepository: Send + Sync {
    async fn load_area_adjacencies(&self, area_id: &str) -> Result<Vec<AreaAdjacency>>;
    async fn load_line_adjacencies(&self, line_id: &str) -> Result<Vec<LineAdjacency>>;

    async fn load_geo_graph(&self, area_id: &str) -> Result<GeoGraph> {
        GeoGraph::from_area_adjacencies(area_id, self.load_area_adjacencies(area_id).await?)
    }

    async fn load_line_graph(&self, line_id: &str) -> Result<LineGraph> {
        LineGraph::from_line_adjacencies(line_id, self.load_line_adjacencies(line_id).await?)
    }
}

#[async_trait]
pub trait SessionContextSummaryRepository: Send + Sync {
    async fn load_session_context_summary(
        &self,
        session_id_hash: &str,
    ) -> Result<Option<SessionContextSummary>>;
    async fn list_recent_session_context_summaries(
        &self,
        limit: i64,
    ) -> Result<Vec<SessionContextSummary>>;
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

#[async_trait]
pub trait ProfileRegistryRepository: Send + Sync {
    async fn upsert_profile_manifest(&self, manifest: &ProfileManifestRecord) -> Result<i64>;
    async fn record_profile_compatibility_status(
        &self,
        status: &ProfileCompatibilityStatusRecord,
    ) -> Result<()>;
    async fn record_evaluation_run(&self, run: &EvaluationRunRecord) -> Result<i64>;
}

fn ensure_non_empty(field: &str, value: &str) -> Result<()> {
    anyhow::ensure!(!value.trim().is_empty(), "{field} must not be empty");
    Ok(())
}

fn ensure_sha256_hex(field: &str, value: &str) -> Result<()> {
    anyhow::ensure!(
        value.len() == 64 && value.chars().all(|character| character.is_ascii_hexdigit()),
        "{field} must be a 64-character hex digest"
    );
    Ok(())
}

fn ensure_non_negative(field: &str, value: i32) -> Result<()> {
    anyhow::ensure!(value >= 0, "{field} must not be negative");
    Ok(())
}

fn ensure_optional_non_negative(field: &str, value: Option<i32>) -> Result<()> {
    if let Some(value) = value {
        ensure_non_negative(field, value)?;
    }
    Ok(())
}

fn ensure_non_negative_i64(field: &str, value: i64) -> Result<()> {
    anyhow::ensure!(value >= 0, "{field} must not be negative");
    Ok(())
}

fn ensure_non_negative_f64(field: &str, value: f64) -> Result<()> {
    anyhow::ensure!(
        value.is_finite() && value >= 0.0,
        "{field} must be a finite non-negative number"
    );
    Ok(())
}

fn ensure_optional_non_negative_f64(field: &str, value: Option<f64>) -> Result<()> {
    if let Some(value) = value {
        ensure_non_negative_f64(field, value)?;
    }
    Ok(())
}

fn ensure_positive(field: &str, value: i32) -> Result<()> {
    anyhow::ensure!(value > 0, "{field} must be positive");
    Ok(())
}

fn ensure_optional_non_empty(field: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        ensure_non_empty(field, value)?;
    }
    Ok(())
}

fn ensure_string_list(field: &str, values: &[String]) -> Result<()> {
    for value in values {
        ensure_non_empty(field, value)?;
    }
    Ok(())
}

fn ensure_non_empty_string_list(field: &str, values: &[String]) -> Result<()> {
    anyhow::ensure!(!values.is_empty(), "{field} must not be empty");
    ensure_string_list(field, values)
}

fn ensure_json_object(field: &str, value: &Value) -> Result<()> {
    anyhow::ensure!(value.is_object(), "{field} must be a JSON object");
    Ok(())
}

fn ensure_json_array(field: &str, value: &Value) -> Result<()> {
    anyhow::ensure!(value.is_array(), "{field} must be a JSON array");
    Ok(())
}

fn default_json_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn default_true() -> bool {
    true
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

    #[test]
    fn profile_manifest_record_validation_rejects_bad_lineage_digest() {
        let mut record = profile_manifest_record();
        record.manifest_checksum_sha256 = "not-a-digest".to_string();

        let error = record.validate().expect_err("digest should be rejected");

        assert!(error
            .to_string()
            .contains("manifest_checksum_sha256 must be a 64-character hex digest"));
    }

    #[test]
    fn evaluation_run_record_validation_checks_nested_cases() {
        let mut run = evaluation_run_record();
        assert!(run.validate().is_ok());

        run.cases[0].case_id = " ".to_string();

        let error = run.validate().expect_err("blank case id should fail");

        assert!(error.to_string().contains("case_id must not be empty"));
    }

    #[test]
    fn recommendation_trace_detail_validation_rejects_invalid_audit_rows() {
        let mut context_summary = RecommendationTraceContextEvidenceSummary {
            context_source: "recent_search_context".to_string(),
            confidence: 0.75,
            privacy_level: "station_level".to_string(),
            primary_kind: "search_execute".to_string(),
            evidence_count: 1,
            strongest_strength: 0.75,
            has_search_execute: true,
            warning_count: 0,
            evidence_payload: serde_json::json!({
                "primary_kind": "search_execute",
                "evidence_count": 1
            }),
        };
        assert!(context_summary.validate().is_ok());

        context_summary.evidence_count = -1;
        let error = context_summary
            .validate()
            .expect_err("negative evidence count should fail");
        assert!(error
            .to_string()
            .contains("evidence_count must not be negative"));

        let mut plan = RecommendationTraceCandidatePlanTrace {
            minimum_candidate_count: 3,
            selected_stage: "same_line".to_string(),
            stop_reason: "sufficient_scoped_candidates".to_string(),
            area_context_usable: true,
            plan_payload: serde_json::json!({ "selected_stage": "same_line" }),
            stages: vec![RecommendationTraceCandidatePlanStage {
                stage_order: 0,
                stage: "same_line".to_string(),
                candidate_count: 4,
                required_min_candidates: 3,
                status: "selected".to_string(),
                reason_code: "selected_sufficient_scoped_candidates".to_string(),
                stage_payload: serde_json::json!({ "stage": "same_line" }),
            }],
        };
        assert!(plan.validate().is_ok());

        plan.stages[0].status = " ".to_string();
        let error = plan.validate().expect_err("blank stage status should fail");
        assert!(error.to_string().contains("status must not be empty"));

        plan.stages[0].status = "deferred".to_string();
        let error = plan
            .validate()
            .expect_err("unexpected stage status should fail");
        assert!(error
            .to_string()
            .contains("status must be one of selected, insufficient, skipped"));

        plan.stages[0].status = "selected".to_string();
        plan.stages.push(RecommendationTraceCandidatePlanStage {
            stage_order: 0,
            stage: "same_station".to_string(),
            candidate_count: 1,
            required_min_candidates: 3,
            status: "insufficient".to_string(),
            reason_code: "insufficient_scoped_candidates".to_string(),
            stage_payload: serde_json::json!({ "stage": "same_station" }),
        });
        let error = plan
            .validate()
            .expect_err("duplicate stage order should fail");
        assert!(error
            .to_string()
            .contains("stage_order must be unique within candidate plan trace: 0"));
    }

    #[test]
    fn graph_adjacency_validation_rejects_invalid_reference_edges() {
        let mut area = area_adjacency();
        assert!(area.validate().is_ok());

        area.to_area_id = area.from_area_id.clone();
        let error = area
            .validate()
            .expect_err("self-referential area adjacency should fail");
        assert!(error
            .to_string()
            .contains("area adjacency must connect different areas"));

        let mut line = line_adjacency();
        assert!(line.validate().is_ok());

        line.station_hop_count = Some(-1);
        let error = line
            .validate()
            .expect_err("negative station hop count should fail");
        assert!(error
            .to_string()
            .contains("station_hop_count must not be negative"));

        let mut area = area_adjacency();
        area.attributes = serde_json::json!([]);
        let error = area
            .validate()
            .expect_err("adjacency attributes must be an object");
        assert!(error
            .to_string()
            .contains("attributes must be a JSON object"));

        let decoded: LineAdjacency = serde_json::from_value(serde_json::json!({
            "from_line_id": "line_yamanote",
            "to_line_id": "line_keihin_tohoku",
            "adjacency_kind": "interchange"
        }))
        .expect("line adjacency serde defaults should match storage defaults");
        assert!(decoded.requires_transfer);
        assert_eq!(decoded.attributes, serde_json::json!({}));
        assert!(decoded.validate().is_ok());
    }

    #[test]
    fn graph_components_normalize_reference_adjacencies_for_diagnostic_reads() {
        let mut first_area = area_adjacency();
        first_area.to_area_id = "area_tokyo_chuo".to_string();
        first_area.distance_meters = Some(800.0);

        let second_area = area_adjacency();

        let mut third_area = area_adjacency();
        third_area.to_area_id = "area_tokyo_ota".to_string();
        third_area.adjacency_kind = "prefecture_neighbor".to_string();
        third_area.distance_meters = None;
        third_area.area_cluster_id = None;

        let geo_graph = GeoGraph::from_area_adjacencies(
            "area_tokyo_minato",
            vec![third_area, second_area, first_area],
        )
        .expect("geo graph component");

        assert_eq!(
            geo_graph
                .edges()
                .iter()
                .map(|edge| edge.to_area_id.as_str())
                .collect::<Vec<_>>(),
            vec!["area_tokyo_chuo", "area_tokyo_shinagawa", "area_tokyo_ota"]
        );
        assert_eq!(
            geo_graph.adjacent_area_ids(),
            vec![
                "area_tokyo_chuo".to_string(),
                "area_tokyo_ota".to_string(),
                "area_tokyo_shinagawa".to_string()
            ]
        );
        assert_eq!(
            geo_graph.area_cluster_diagnostics(),
            vec![AreaClusterDiagnostic {
                area_cluster_id: "cluster_tokyo_bay".to_string(),
                observed_area_ids: vec![
                    "area_tokyo_chuo".to_string(),
                    "area_tokyo_minato".to_string(),
                    "area_tokyo_shinagawa".to_string()
                ],
            }]
        );

        let first_line = line_adjacency();

        let mut second_line = line_adjacency();
        second_line.to_line_id = "line_asakusa".to_string();
        second_line.station_hop_count = Some(1);
        second_line.requires_transfer = false;

        let mut third_line = line_adjacency();
        third_line.to_line_id = "line_yamanote_branch".to_string();
        third_line.adjacency_kind = "operator_relation".to_string();
        third_line.interchange_station_id = None;
        third_line.station_hop_count = None;
        third_line.requires_transfer = false;

        let line_graph = LineGraph::from_line_adjacencies(
            "line_yamanote",
            vec![third_line, second_line, first_line],
        )
        .expect("line graph component");

        assert_eq!(
            line_graph
                .edges()
                .iter()
                .map(|edge| edge.to_line_id.as_str())
                .collect::<Vec<_>>(),
            vec!["line_keihin_tohoku", "line_asakusa", "line_yamanote_branch"]
        );
        assert_eq!(
            line_graph
                .station_hop_diagnostics()
                .iter()
                .map(|diagnostic| diagnostic.station_hop_count)
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1), None]
        );
        assert_eq!(
            line_graph.interchange_diagnostics(),
            vec![InterchangeDiagnostic {
                interchange_station_id: "st_shinagawa".to_string(),
                from_line_id: "line_yamanote".to_string(),
                to_line_ids: vec!["line_asakusa".to_string(), "line_keihin_tohoku".to_string()],
                adjacency_kinds: vec!["interchange".to_string()],
                requires_transfer: true,
                minimum_station_hop_count: Some(0),
            }]
        );
    }

    #[test]
    fn graph_components_reject_edges_loaded_for_different_origin() {
        let area_error =
            GeoGraph::from_area_adjacencies("area_tokyo_other", vec![area_adjacency()])
                .expect_err("area graph origin mismatch should fail");
        assert!(area_error
            .to_string()
            .contains("geo graph edge must start from origin_area_id"));

        let line_error = LineGraph::from_line_adjacencies("line_other", vec![line_adjacency()])
            .expect_err("line graph origin mismatch should fail");
        assert!(line_error
            .to_string()
            .contains("line graph edge must start from origin_line_id"));
    }

    #[test]
    fn session_context_summary_validation_keeps_diagnostic_shape_tight() {
        let mut summary = session_context_summary();
        assert!(summary.validate().is_ok());

        summary.session_id_hash = "not-a-hash".to_string();
        let error = summary
            .validate()
            .expect_err("session id hash should be a digest");
        assert!(error
            .to_string()
            .contains("session_id_hash must be a 64-character hex digest"));

        let mut summary = session_context_summary();
        summary.search_execute_count = -1;
        let error = summary
            .validate()
            .expect_err("negative search execute count should fail");
        assert!(error
            .to_string()
            .contains("search_execute_count must not be negative"));

        let mut summary = session_context_summary();
        summary.summary_payload = serde_json::json!([]);
        let error = summary
            .validate()
            .expect_err("summary payload must be an object");
        assert!(error
            .to_string()
            .contains("summary_payload must be a JSON object"));
    }

    #[test]
    fn evaluation_run_record_validation_requires_complete_case_counts() {
        let mut run = evaluation_run_record();
        run.scenarios = 2;

        let error = run
            .validate()
            .expect_err("scenario count must match persisted case outcomes");

        assert!(error
            .to_string()
            .contains("passed plus blocked must match scenarios"));
    }

    #[test]
    fn evaluation_run_record_validation_requires_status_to_match_outcomes() {
        let mut run = evaluation_run_record();
        run.status = EvaluationRunStatus::Passed;
        run.passed = 0;
        run.blocked = 1;
        run.blockers = 1;
        run.cases[0].status = EvaluationRunCaseStatus::Blocked;

        let error = run
            .validate()
            .expect_err("passed run must not carry blocker outcomes");

        assert!(error
            .to_string()
            .contains("passed runs must not include blocked cases"));

        let mut run = evaluation_run_record();
        run.status = EvaluationRunStatus::Blocked;

        let error = run
            .validate()
            .expect_err("blocked run must carry blocked evidence");

        assert!(error
            .to_string()
            .contains("blocked runs must include blocked cases or blockers"));
    }

    #[test]
    fn profile_manifest_record_validation_rejects_loose_json_and_empty_registry() {
        let mut record = profile_manifest_record();
        record.manifest_payload = serde_json::json!([]);

        let error = record
            .validate()
            .expect_err("manifest payload must be an object");

        assert!(error
            .to_string()
            .contains("manifest_payload must be a JSON object"));

        let mut record = profile_manifest_record();
        record.content_kind_registry.clear();

        let error = record
            .validate()
            .expect_err("content kind registry must not be empty");

        assert!(error
            .to_string()
            .contains("content_kind_registry must not be empty"));
    }

    #[test]
    fn profile_status_strings_are_storage_contract_values() {
        assert_eq!(ProfileCompatibilityStatus::Valid.as_str(), "valid");
        assert_eq!(EvaluationRunKind::Golden.as_str(), "golden");
        assert_eq!(EvaluationRunStatus::Blocked.as_str(), "blocked");
        assert_eq!(EvaluationRunCaseStatus::Passed.as_str(), "passed");
    }

    fn area_adjacency() -> AreaAdjacency {
        AreaAdjacency {
            from_area_id: "area_tokyo_minato".to_string(),
            to_area_id: "area_tokyo_shinagawa".to_string(),
            adjacency_kind: "city_neighbor".to_string(),
            distance_meters: Some(1_250.0),
            area_cluster_id: Some("cluster_tokyo_bay".to_string()),
            source_id: Some("fixture".to_string()),
            source_version: Some("2026-05-13".to_string()),
            attributes: serde_json::json!({ "note": "fixture" }),
        }
    }

    fn line_adjacency() -> LineAdjacency {
        LineAdjacency {
            from_line_id: "line_yamanote".to_string(),
            to_line_id: "line_keihin_tohoku".to_string(),
            adjacency_kind: "interchange".to_string(),
            interchange_station_id: Some("st_shinagawa".to_string()),
            station_hop_count: Some(0),
            requires_transfer: true,
            source_id: Some("fixture".to_string()),
            source_version: Some("2026-05-13".to_string()),
            attributes: serde_json::json!({ "platform_hint": "same_station" }),
        }
    }

    fn session_context_summary() -> SessionContextSummary {
        SessionContextSummary {
            session_id_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_string(),
            context_source: "recent_search_context".to_string(),
            confidence: 0.75,
            privacy_level: "coarse_area".to_string(),
            primary_kind: "search_execute".to_string(),
            evidence_count: 2,
            search_execute_count: 1,
            warning_count: 0,
            area_id: Some("area_tokyo_minato".to_string()),
            line_id: Some("line_yamanote".to_string()),
            station_id: Some("st_tamachi".to_string()),
            summary_payload: serde_json::json!({ "evidence_age_bucket": "recent" }),
            first_seen_at: "2026-05-13T00:00:00.000000Z".to_string(),
            last_seen_at: "2026-05-13T00:05:00.000000Z".to_string(),
            updated_at: "2026-05-13T00:05:00.000000Z".to_string(),
        }
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

    fn profile_manifest_record() -> ProfileManifestRecord {
        ProfileManifestRecord {
            profile_id: "school-event-jp".to_string(),
            display_name: "School Event JP".to_string(),
            schema_version: 2,
            manifest_kind: "profile_pack".to_string(),
            manifest_version: 1,
            compatibility_level: "reference".to_string(),
            default_locale: Some("ja-JP".to_string()),
            description: None,
            manifest_path: "configs/profiles/school-event-jp/profile.yaml".to_string(),
            manifest_checksum_sha256:
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            manifest_payload: serde_json::json!({ "profile_id": "school-event-jp" }),
            ranking_config_dir: "configs/ranking".to_string(),
            reason_catalog_path: "configs/profiles/school-event-jp/reasons/ja-JP.yaml".to_string(),
            content_kind_registry: vec!["school".to_string(), "event".to_string()],
            supported_content_kinds: vec!["school".to_string(), "event".to_string()],
            context_inputs: vec!["station".to_string(), "line".to_string()],
            placements: vec!["home".to_string(), "search".to_string()],
            fallback_policy: "school_event_jp_default".to_string(),
            fixture_count: 1,
            connector_count: 1,
            evaluation_reference_count: 1,
        }
    }

    fn evaluation_run_record() -> EvaluationRunRecord {
        EvaluationRunRecord {
            profile_id: Some("school-event-jp".to_string()),
            profile_manifest_lineage_id: Some(7),
            run_kind: EvaluationRunKind::Golden,
            scenario_source_kind: "profile_evaluation".to_string(),
            scenario_path: "configs/profiles/school-event-jp/evaluation".to_string(),
            pairwise_pack_path: None,
            algorithm_version: "test-algorithm".to_string(),
            status: EvaluationRunStatus::Passed,
            scenarios: 1,
            passed: 1,
            blocked: 0,
            blockers: 0,
            warnings: 0,
            summary_payload: serde_json::json!({ "scenarios": 1 }),
            cases: vec![EvaluationRunCaseRecord {
                case_id: "S01".to_string(),
                title: "Scenario".to_string(),
                path: "scenario.yaml".to_string(),
                status: EvaluationRunCaseStatus::Passed,
                expected_fallback_stage: "strict_station".to_string(),
                actual_fallback_stage: Some("strict_station".to_string()),
                expected_order: vec!["school:school_a".to_string()],
                actual_order: vec!["school:school_a".to_string()],
                checks_payload: serde_json::json!([]),
            }],
        }
    }
}
