#[cfg(feature = "storage-backends")]
mod context_inspect;
mod doctor;
#[cfg(feature = "storage-backends")]
mod explain;
mod explanation_integrity;
mod fixtures;
mod formatting;
#[cfg(feature = "storage-backends")]
mod import;
#[cfg(feature = "storage-backends")]
mod jobs;
mod replay;
#[cfg(feature = "storage-backends")]
mod repository;
#[cfg(feature = "storage-backends")]
mod snapshot;

#[cfg(feature = "storage-backends")]
pub use context_inspect::{
    format_context_inspect_summary, run_context_inspect, ContextInspectInput, ContextInspectSummary,
};
pub use doctor::{
    ranking_config_doctor_summary_from_lint, run_context_coverage_doctor,
    run_explanation_integrity_doctor, run_ingest_quality_doctor, run_profile_pack_doctor,
    run_retrieval_parity_doctor, run_storage_compatibility_doctor, ConnectorSchemaContractSummary,
    ContextCoverageDoctorCase, ContextCoverageDoctorSummary, ContextCoverageRequirement,
    ContextCoverageShapeMismatch, ExplanationIntegrityDoctorCase,
    ExplanationIntegrityDoctorSummary, IngestQualityDoctorConnector, IngestQualityDoctorProfile,
    IngestQualityDoctorSummary, ProfilePackDoctorFile, ProfilePackDoctorSummary,
    RankingConfigDoctorFile, RankingConfigDoctorProfile, RankingConfigDoctorSummary,
    RetrievalParityDoctorCase, RetrievalParityDoctorSummary, RetrievalParitySortField,
    StorageCompatibilityDoctorSummary, StorageCompatibilityEntry,
};
#[cfg(feature = "storage-backends")]
pub use explain::{
    run_explain_trace, ExplainTraceCheck, ExplainTraceContextEvidenceSummary,
    ExplainTraceIntegritySummary, ExplainTraceItemSummary, ExplainTracePayloadSummary,
    ExplainTraceReasonSummary, ExplainTraceReport, ExplainTraceRequestSummary,
    ExplainTraceResponseSummary, ExplainTraceStatus,
};
pub use explanation_integrity::{QualityCheckStatus, QualitySeverity};
pub use fixtures::{
    generate_demo_jp_fixture, run_fixture_doctor, FixtureDoctorFile, FixtureDoctorSummary,
    FixtureFileManifest, FixtureManifestKind, FixtureSetManifest,
};
#[cfg(feature = "storage-backends")]
pub use formatting::format_eval_replay_summary;
pub use formatting::{
    format_context_coverage_doctor_summary, format_eval_golden_summary,
    format_explanation_integrity_doctor_summary, format_fixture_doctor_summary,
    format_ingest_quality_doctor_summary, format_profile_pack_doctor_summary,
    format_ranking_config_doctor_summary, format_replay_evaluation_summary,
    format_replay_scenario_summary, format_retrieval_parity_doctor_summary,
    format_storage_compatibility_doctor_summary,
};
#[cfg(feature = "storage-backends")]
pub use formatting::{
    format_explain_trace_report, format_job_enqueue_summary, format_job_inspection,
    format_job_list, format_job_mutation_summary, format_snapshot_refresh_summary, format_summary,
};
#[cfg(feature = "storage-backends")]
pub use import::{
    run_derive_school_station_links, run_event_csv_import, run_event_ndjson_import,
    run_import_command, run_profile_source_import, CommandSummary, ImportTarget,
    DEFAULT_EVENT_NDJSON_SOURCE_ID,
};
#[cfg(feature = "storage-backends")]
pub use jobs::{
    run_job_due, run_job_enqueue, run_job_inspect, run_job_list, run_job_retry, JobEnqueueSummary,
};
#[cfg(feature = "storage-backends")]
pub use replay::run_replay_evaluate;
pub use replay::{
    run_replay_scenarios, run_replay_scenarios_with_source, PairwiseExpectation,
    ReplayEvaluationCase, ReplayEvaluationStatus, ReplayEvaluationSummary, ReplayScenario,
    ReplayScenarioCase, ReplayScenarioCheck, ReplayScenarioExpectations, ReplayScenarioKind,
    ReplayScenarioSource, ReplayScenarioSourceKind, ReplayScenarioStatus, ReplayScenarioSummary,
    DEFAULT_REPLAY_SCENARIO_PATH,
};
#[cfg(feature = "storage-backends")]
pub use snapshot::{run_snapshot_refresh, SnapshotRefreshSummary};
