use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use config::{
    lint_profile_pack_dir, profile_connector_schema_contracts, ProfileConnectorRegistryEntry,
    ProfileConnectorType, ProfilePackLintFile, ProfilePackLintSummary, RankingConfigLintFile,
    RankingConfigLintSummary, PROFILE_CONNECTOR_SCHEMA_CONTRACT_VERSION,
};
use context::ContextSource;
use crawler_core::lint_manifest_file as lint_crawl_manifest_file;
use domain::SchoolStationLink;
use generic_csv::{
    lint_archive_manifest_file, lint_source_manifest_file, ArchiveSourceManifestLintFile,
};
use serde::Serialize;
use storage::{
    candidate_retrieval_opensearch_sort_contract, candidate_retrieval_ordering_contract,
    sort_candidate_links_for_retrieval,
};

use crate::{
    explanation_integrity::{QualityCheckStatus, QualitySeverity},
    replay::{
        load_replay_scenarios, run_replay_scenarios, ReplayScenario, ReplayScenarioCheck,
        ReplayScenarioStatus, ReplayScenarioSummary,
    },
};

const REQUIRED_CONTEXT_SOURCES: &[ContextSource] = &[
    ContextSource::RequestArea,
    ContextSource::RequestLine,
    ContextSource::DefaultSafeContext,
];
const UNHANDLED_CONTEXT_SHAPE: &str = "unhandled";

enum ContextShapeExpectation {
    Allowed(Vec<Vec<String>>),
    Unhandled(&'static str),
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExplanationIntegrityDoctorSummary {
    pub scenarios: usize,
    pub passed: usize,
    pub blocked: usize,
    pub blockers: usize,
    pub warnings: usize,
    pub explanation_integrity_passed: usize,
    pub explanation_integrity_total: usize,
    pub cases: Vec<ExplanationIntegrityDoctorCase>,
}

impl ExplanationIntegrityDoctorSummary {
    pub fn has_blockers(&self) -> bool {
        self.blockers > 0
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExplanationIntegrityDoctorCase {
    pub id: String,
    pub title: String,
    pub path: PathBuf,
    pub status: ReplayScenarioStatus,
    pub checks: Vec<ReplayScenarioCheck>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProfilePackDoctorSummary {
    pub profile_packs: usize,
    pub ranking_config_dirs: usize,
    pub reason_catalog_locales: usize,
    pub reason_count: usize,
    pub fixture_references: usize,
    pub connector_references: usize,
    pub evaluation_references: usize,
    pub source_manifest_references: usize,
    pub event_csv_example_references: usize,
    pub archive_source_references: usize,
    pub optional_crawler_manifest_references: usize,
    pub connector_schema_contract_version: String,
    pub connector_schema_contracts: Vec<ConnectorSchemaContractSummary>,
    pub files: Vec<ProfilePackDoctorFile>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProfilePackDoctorFile {
    pub path: PathBuf,
    pub profile_id: String,
    pub ranking_config_dir: PathBuf,
    pub fallback_config_path: Option<PathBuf>,
    pub reason_catalog_path: PathBuf,
    pub schema_version: u32,
    pub kind: String,
    pub manifest_version: u32,
    pub compatibility_level: String,
    pub content_kind_registry: Vec<String>,
    pub supported_content_kinds: Vec<String>,
    pub runtime_executable_content_kinds: Vec<String>,
    pub registry_only_content_kinds: Vec<String>,
    pub placements: Vec<String>,
    pub reason_catalog_locale_count: usize,
    pub reason_count: usize,
    pub fixture_references: usize,
    pub connector_references: usize,
    pub connector_registry: Vec<ProfileConnectorRegistryEntry>,
    pub evaluation_references: usize,
    pub source_manifest_references: usize,
    pub event_csv_example_references: usize,
    pub archive_source_references: usize,
    pub optional_crawler_manifest_references: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IngestQualityDoctorSummary {
    pub profile_packs: usize,
    pub connector_references: usize,
    pub source_manifest_references: usize,
    pub event_csv_example_references: usize,
    pub archive_source_references: usize,
    pub optional_crawler_manifest_references: usize,
    pub source_class_counts: BTreeMap<String, usize>,
    pub manifest_kind_counts: BTreeMap<String, usize>,
    pub manifest_schema_version_counts: BTreeMap<String, usize>,
    pub runtime_executable_mappings: usize,
    pub non_runtime_mappings: usize,
    pub local_reference_only_connectors: usize,
    pub dynamic_loading_enabled_connectors: usize,
    pub live_fetch_default_connectors: usize,
    pub crawler_allowlist_required_connectors: usize,
    pub source_manifest_file_count: usize,
    pub archive_file_count: usize,
    pub crawler_target_count: usize,
    pub archive_format_counts: BTreeMap<String, usize>,
    pub crawler_source_maturity_counts: BTreeMap<String, usize>,
    pub crawler_expected_shape_counts: BTreeMap<String, usize>,
    pub evidence_scope: String,
    pub execution_scope: String,
    pub connector_schema_contract_version: String,
    pub connector_schema_contracts: Vec<ConnectorSchemaContractSummary>,
    pub profiles: Vec<IngestQualityDoctorProfile>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IngestQualityDoctorProfile {
    pub path: PathBuf,
    pub profile_id: String,
    pub connector_references: usize,
    pub source_manifest_references: usize,
    pub event_csv_example_references: usize,
    pub archive_source_references: usize,
    pub optional_crawler_manifest_references: usize,
    pub source_class_counts: BTreeMap<String, usize>,
    pub manifest_kind_counts: BTreeMap<String, usize>,
    pub manifest_schema_version_counts: BTreeMap<String, usize>,
    pub runtime_executable_mappings: usize,
    pub non_runtime_mappings: usize,
    pub local_reference_only_connectors: usize,
    pub dynamic_loading_enabled_connectors: usize,
    pub live_fetch_default_connectors: usize,
    pub crawler_allowlist_required_connectors: usize,
    pub source_manifest_file_count: usize,
    pub archive_file_count: usize,
    pub crawler_target_count: usize,
    pub archive_format_counts: BTreeMap<String, usize>,
    pub crawler_source_maturity_counts: BTreeMap<String, usize>,
    pub crawler_expected_shape_counts: BTreeMap<String, usize>,
    pub connectors: Vec<IngestQualityDoctorConnector>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IngestQualityDoctorConnector {
    pub connector_type: String,
    pub source_class: String,
    pub manifest_kind: String,
    pub manifest_schema_version: Option<u32>,
    pub source_id: Option<String>,
    pub field_mapping: Option<String>,
    pub field_mapping_runtime_executable: Option<bool>,
    pub manifest_lint: String,
    pub source_manifest_file_count: Option<usize>,
    pub archive_file_count: Option<usize>,
    pub archive_format: Option<String>,
    pub archive_checksum_sha256: Option<String>,
    pub crawler_target_count: Option<usize>,
    pub crawler_source_maturity: Option<String>,
    pub crawler_expected_shape: Option<String>,
    pub local_reference_only: bool,
    pub dynamic_loading_enabled: bool,
    pub live_fetch_default: bool,
    pub allowlist_required: bool,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ConnectorSchemaContractSummary {
    pub connector_type: String,
    pub source_class: String,
    pub manifest_kind: String,
    pub manifest_schema_version: Option<u32>,
    pub source_id_scope: String,
    pub field_mapping_scope: String,
    pub runtime_execution: String,
    pub manifest_lint: String,
    pub local_reference_only: bool,
    pub dynamic_loading_enabled: bool,
    pub live_fetch_default: bool,
    pub allowlist_required: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RankingConfigDoctorSummary {
    pub active_profile_id: Option<String>,
    pub fixture_set_id: Option<String>,
    pub ranking_config_dir: PathBuf,
    pub profile_version: String,
    pub ranking_files: usize,
    pub ranking_kind_counts: BTreeMap<String, usize>,
    pub profile_packs: usize,
    pub referenced_ranking_config_dirs: usize,
    pub reason_catalog_references: usize,
    pub reason_catalog_locales: usize,
    pub reason_count: usize,
    pub fixture_references: usize,
    pub connector_references: usize,
    pub evaluation_references: usize,
    pub source_manifest_references: usize,
    pub event_csv_example_references: usize,
    pub archive_source_references: usize,
    pub optional_crawler_manifest_references: usize,
    pub files: Vec<RankingConfigDoctorFile>,
    pub profiles: Vec<RankingConfigDoctorProfile>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RankingConfigDoctorFile {
    pub path: PathBuf,
    pub schema_version: u32,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RankingConfigDoctorProfile {
    pub path: PathBuf,
    pub profile_id: String,
    pub ranking_config_dir: PathBuf,
    pub fallback_config_path: Option<PathBuf>,
    pub reason_catalog_path: PathBuf,
    pub compatibility_level: String,
    pub content_kind_registry: Vec<String>,
    pub supported_content_kinds: Vec<String>,
    pub runtime_executable_content_kinds: Vec<String>,
    pub registry_only_content_kinds: Vec<String>,
    pub placements: Vec<String>,
    pub reason_catalog_locale_count: usize,
    pub reason_count: usize,
    pub fixture_references: usize,
    pub connector_references: usize,
    pub evaluation_references: usize,
    pub source_manifest_references: usize,
    pub event_csv_example_references: usize,
    pub archive_source_references: usize,
    pub optional_crawler_manifest_references: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContextCoverageDoctorSummary {
    pub scenarios: usize,
    pub scenarios_with_context: usize,
    pub scenarios_without_context: usize,
    pub scenarios_with_candidate_counts: usize,
    pub candidate_count_expectations: usize,
    pub context_shape_mismatches: Vec<ContextCoverageShapeMismatch>,
    pub context_source_counts: BTreeMap<String, usize>,
    pub tag_counts: BTreeMap<String, usize>,
    pub fallback_stage_counts: BTreeMap<String, usize>,
    pub candidate_count_stage_counts: BTreeMap<String, usize>,
    pub required_context_sources: Vec<ContextCoverageRequirement>,
    pub missing_required_context_sources: Vec<String>,
    pub cases: Vec<ContextCoverageDoctorCase>,
}

impl ContextCoverageDoctorSummary {
    pub fn has_blockers(&self) -> bool {
        !self.missing_required_context_sources.is_empty()
            || !self.context_shape_mismatches.is_empty()
    }

    pub fn blocker_message(&self) -> Option<String> {
        if !self.has_blockers() {
            return None;
        }

        let mut parts = Vec::new();
        if !self.missing_required_context_sources.is_empty() {
            parts.push(format!(
                "missing_required_context_sources={}",
                self.missing_required_context_sources.join(",")
            ));
        }
        if !self.context_shape_mismatches.is_empty() {
            parts.push(format!(
                "context_shape_mismatches={}",
                self.context_shape_mismatches
                    .iter()
                    .map(|mismatch| mismatch.id.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        Some(parts.join("; "))
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContextCoverageRequirement {
    pub context_source: String,
    pub covered: bool,
    pub scenarios: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContextCoverageShapeMismatch {
    pub id: String,
    pub path: PathBuf,
    pub context_source: String,
    pub expected_shape: String,
    pub actual_shape: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContextCoverageDoctorCase {
    pub id: String,
    pub title: String,
    pub path: PathBuf,
    pub context_source: Option<String>,
    pub tags: Vec<String>,
    pub fallback_stage: String,
    pub candidate_count_stages: Vec<String>,
    pub has_area_context: bool,
    pub has_line_context: bool,
    pub has_station_context: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RetrievalParityDoctorSummary {
    pub case_count: usize,
    pub passed: usize,
    pub failed: usize,
    pub requires_database: bool,
    pub requires_opensearch: bool,
    pub public_mvp_gate: bool,
    pub ordering_contract: Vec<String>,
    pub opensearch_sort_contract: Vec<RetrievalParitySortField>,
    pub cases: Vec<RetrievalParityDoctorCase>,
}

impl RetrievalParityDoctorSummary {
    pub fn has_blockers(&self) -> bool {
        self.failed > 0
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StorageCompatibilityDoctorSummary {
    pub registry_version: String,
    pub component_count: usize,
    pub compatibility_level_counts: BTreeMap<String, usize>,
    pub sql_only_required_components: Vec<String>,
    pub optional_runtime_components: Vec<String>,
    pub public_mvp_gate_components: Vec<String>,
    pub final_ranking_owner: String,
    pub profile_compatibility_source: String,
    pub entries: Vec<StorageCompatibilityEntry>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StorageCompatibilityEntry {
    pub component: String,
    pub display_name: String,
    pub compatibility_level: String,
    pub runtime_status: String,
    pub data_role: String,
    pub public_mvp_gate: bool,
    pub write_database_status: String,
    pub contract_evidence: String,
    pub operator_note: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RetrievalParitySortField {
    pub field: String,
    pub order: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RetrievalParityDoctorCase {
    pub id: String,
    pub description: String,
    pub target_station_id: String,
    pub limit: usize,
    pub input_order: Vec<String>,
    pub expected_order: Vec<String>,
    pub actual_order: Vec<String>,
    pub passed: bool,
}

pub fn run_explanation_integrity_doctor(
    scenario_path: impl AsRef<Path>,
    ranking_config_dir: impl AsRef<Path>,
    algorithm_version: &str,
) -> Result<ExplanationIntegrityDoctorSummary> {
    let replay_summary =
        run_replay_scenarios(scenario_path, ranking_config_dir, algorithm_version)?;
    Ok(explanation_integrity_summary_from_replay(replay_summary))
}

pub fn run_profile_pack_doctor(
    profiles_path: impl AsRef<Path>,
) -> Result<ProfilePackDoctorSummary> {
    let lint_summary = lint_profile_pack_dir(profiles_path)?;
    Ok(profile_pack_doctor_summary_from_lint(lint_summary))
}

pub fn run_ingest_quality_doctor(
    profiles_path: impl AsRef<Path>,
) -> Result<IngestQualityDoctorSummary> {
    let lint_summary = lint_profile_pack_dir(profiles_path)?;
    ingest_quality_doctor_summary_from_lint(lint_summary)
}

pub fn ranking_config_doctor_summary_from_lint(
    active_profile_id: Option<String>,
    fixture_set_id: Option<String>,
    ranking_summary: RankingConfigLintSummary,
    profile_summary: ProfilePackLintSummary,
) -> RankingConfigDoctorSummary {
    let files = ranking_summary
        .files
        .into_iter()
        .map(ranking_config_doctor_file)
        .collect::<Vec<_>>();
    let mut ranking_kind_counts = BTreeMap::new();
    for file in &files {
        increment(&mut ranking_kind_counts, &file.kind);
    }
    let referenced_ranking_config_dirs = profile_summary.ranking_configs.len();
    let profiles = profile_summary
        .files
        .into_iter()
        .map(ranking_config_doctor_profile)
        .collect::<Vec<_>>();
    let reason_catalog_references = profiles
        .iter()
        .map(|profile| profile.reason_catalog_path.clone())
        .collect::<BTreeSet<_>>()
        .len();

    RankingConfigDoctorSummary {
        active_profile_id,
        fixture_set_id,
        ranking_config_dir: ranking_summary.path,
        profile_version: ranking_summary.profile_version,
        ranking_files: files.len(),
        ranking_kind_counts,
        profile_packs: profiles.len(),
        referenced_ranking_config_dirs,
        reason_catalog_references,
        reason_catalog_locales: profiles
            .iter()
            .map(|profile| profile.reason_catalog_locale_count)
            .sum(),
        reason_count: profiles.iter().map(|profile| profile.reason_count).sum(),
        fixture_references: profiles
            .iter()
            .map(|profile| profile.fixture_references)
            .sum(),
        connector_references: profiles
            .iter()
            .map(|profile| profile.connector_references)
            .sum(),
        evaluation_references: profiles
            .iter()
            .map(|profile| profile.evaluation_references)
            .sum(),
        source_manifest_references: profiles
            .iter()
            .map(|profile| profile.source_manifest_references)
            .sum(),
        event_csv_example_references: profiles
            .iter()
            .map(|profile| profile.event_csv_example_references)
            .sum(),
        archive_source_references: profiles
            .iter()
            .map(|profile| profile.archive_source_references)
            .sum(),
        optional_crawler_manifest_references: profiles
            .iter()
            .map(|profile| profile.optional_crawler_manifest_references)
            .sum(),
        files,
        profiles,
    }
}

pub fn run_context_coverage_doctor(
    scenario_path: impl AsRef<Path>,
) -> Result<ContextCoverageDoctorSummary> {
    let scenarios = load_replay_scenarios(scenario_path)?;
    Ok(context_coverage_summary_from_scenarios(scenarios))
}

pub fn run_retrieval_parity_doctor() -> RetrievalParityDoctorSummary {
    let cases = retrieval_parity_cases()
        .into_iter()
        .map(run_retrieval_parity_case)
        .collect::<Vec<_>>();
    let passed = cases.iter().filter(|case| case.passed).count();
    let failed = cases.len() - passed;

    RetrievalParityDoctorSummary {
        case_count: cases.len(),
        passed,
        failed,
        requires_database: false,
        requires_opensearch: false,
        public_mvp_gate: false,
        ordering_contract: candidate_retrieval_ordering_contract()
            .iter()
            .map(|field| (*field).to_string())
            .collect(),
        opensearch_sort_contract: candidate_retrieval_opensearch_sort_contract()
            .iter()
            .map(|(field, order)| RetrievalParitySortField {
                field: (*field).to_string(),
                order: (*order).to_string(),
            })
            .collect(),
        cases,
    }
}

pub fn run_storage_compatibility_doctor() -> StorageCompatibilityDoctorSummary {
    let entries = storage_compatibility_entries();
    let mut compatibility_level_counts = BTreeMap::new();
    for entry in &entries {
        increment(&mut compatibility_level_counts, &entry.compatibility_level);
    }

    StorageCompatibilityDoctorSummary {
        registry_version: "v0.4.0-static-storage-compatibility".to_string(),
        component_count: entries.len(),
        compatibility_level_counts,
        sql_only_required_components: entries
            .iter()
            .filter(|entry| entry.runtime_status == "required_for_sql_only")
            .map(|entry| entry.component.clone())
            .collect(),
        optional_runtime_components: entries
            .iter()
            .filter(|entry| entry.runtime_status == "optional")
            .map(|entry| entry.component.clone())
            .collect(),
        public_mvp_gate_components: entries
            .iter()
            .filter(|entry| entry.public_mvp_gate)
            .map(|entry| entry.component.clone())
            .collect(),
        final_ranking_owner: "rust".to_string(),
        profile_compatibility_source:
            "profile manifests; see doctor profile-pack and doctor ranking-config".to_string(),
        entries,
    }
}

fn context_coverage_summary_from_scenarios(
    scenarios: Vec<(PathBuf, ReplayScenario)>,
) -> ContextCoverageDoctorSummary {
    let cases = scenarios
        .into_iter()
        .map(|(path, scenario)| context_coverage_case(path, scenario))
        .collect::<Vec<_>>();

    let mut context_source_counts = BTreeMap::new();
    let mut tag_counts = BTreeMap::new();
    let mut fallback_stage_counts = BTreeMap::new();
    let mut candidate_count_stage_counts = BTreeMap::new();

    for case in &cases {
        if let Some(context_source) = &case.context_source {
            increment(&mut context_source_counts, context_source);
        }
        for tag in &case.tags {
            increment(&mut tag_counts, tag);
        }
        increment(&mut fallback_stage_counts, &case.fallback_stage);
        for stage in &case.candidate_count_stages {
            increment(&mut candidate_count_stage_counts, stage);
        }
    }

    let required_context_sources = REQUIRED_CONTEXT_SOURCES
        .iter()
        .map(|source| {
            let context_source = source.as_str();
            let scenarios = context_source_counts
                .get(context_source)
                .copied()
                .unwrap_or(0);
            ContextCoverageRequirement {
                context_source: context_source.to_string(),
                covered: scenarios > 0,
                scenarios,
            }
        })
        .collect::<Vec<_>>();
    let missing_required_context_sources = required_context_sources
        .iter()
        .filter(|source| !source.covered)
        .map(|source| source.context_source.clone())
        .collect::<Vec<_>>();
    let context_shape_mismatches = cases
        .iter()
        .filter_map(context_shape_mismatch)
        .collect::<Vec<_>>();

    ContextCoverageDoctorSummary {
        scenarios: cases.len(),
        scenarios_with_context: cases
            .iter()
            .filter(|case| case.context_source.is_some())
            .count(),
        scenarios_without_context: cases
            .iter()
            .filter(|case| case.context_source.is_none())
            .count(),
        scenarios_with_candidate_counts: cases
            .iter()
            .filter(|case| !case.candidate_count_stages.is_empty())
            .count(),
        candidate_count_expectations: cases
            .iter()
            .map(|case| case.candidate_count_stages.len())
            .sum(),
        context_shape_mismatches,
        context_source_counts,
        tag_counts,
        fallback_stage_counts,
        candidate_count_stage_counts,
        required_context_sources,
        missing_required_context_sources,
        cases,
    }
}

fn context_coverage_case(path: PathBuf, scenario: ReplayScenario) -> ContextCoverageDoctorCase {
    let context_source = scenario
        .query
        .context
        .as_ref()
        .map(|context| context.context_source.as_str().to_string());
    let has_area_context = scenario
        .query
        .context
        .as_ref()
        .is_some_and(|context| context.area.is_some());
    let has_line_context = scenario
        .query
        .context
        .as_ref()
        .is_some_and(|context| context.line.is_some());
    let has_station_context = scenario
        .query
        .context
        .as_ref()
        .is_some_and(|context| context.station.is_some());

    ContextCoverageDoctorCase {
        id: scenario.id,
        title: scenario.title,
        path,
        context_source,
        tags: scenario.tags,
        fallback_stage: scenario.expectations.fallback_stage.as_str().to_string(),
        candidate_count_stages: scenario
            .expectations
            .candidate_counts
            .keys()
            .cloned()
            .collect(),
        has_area_context,
        has_line_context,
        has_station_context,
    }
}

fn increment(counts: &mut BTreeMap<String, usize>, key: &str) {
    *counts.entry(key.to_string()).or_insert(0) += 1;
}

fn merge_counts(target: &mut BTreeMap<String, usize>, source: &BTreeMap<String, usize>) {
    for (key, count) in source {
        *target.entry(key.clone()).or_insert(0) += count;
    }
}

fn context_shape_mismatch(
    case: &ContextCoverageDoctorCase,
) -> Option<ContextCoverageShapeMismatch> {
    let context_source = case.context_source.as_deref()?;
    let actual_shape = context_shape_parts(case);
    match expected_context_shapes(context_source) {
        ContextShapeExpectation::Allowed(expected_shapes) => {
            if context_shape_matches(&actual_shape, &expected_shapes) {
                return None;
            }

            Some(ContextCoverageShapeMismatch {
                id: case.id.clone(),
                path: case.path.clone(),
                context_source: context_source.to_string(),
                expected_shape: context_shapes_label(&expected_shapes),
                actual_shape,
            })
        }
        ContextShapeExpectation::Unhandled(expected_shape) => Some(ContextCoverageShapeMismatch {
            id: case.id.clone(),
            path: case.path.clone(),
            context_source: context_source.to_string(),
            expected_shape: expected_shape.to_string(),
            actual_shape,
        }),
    }
}

fn expected_context_shapes(context_source: &str) -> ContextShapeExpectation {
    match context_source {
        source if source == ContextSource::RequestArea.as_str() => {
            ContextShapeExpectation::Allowed(vec![context_shape(&["area"])])
        }
        source if source == ContextSource::RequestLine.as_str() => {
            ContextShapeExpectation::Allowed(vec![
                context_shape(&["line"]),
                context_shape(&["area", "line"]),
            ])
        }
        source if source == ContextSource::RequestStation.as_str() => {
            ContextShapeExpectation::Allowed(vec![
                context_shape(&["line", "station"]),
                context_shape(&["area", "line", "station"]),
            ])
        }
        source if source == ContextSource::UserProfileArea.as_str() => {
            // User profile contexts can persist any non-empty coarse context shape.
            ContextShapeExpectation::Allowed(vec![
                context_shape(&["area"]),
                context_shape(&["line"]),
                context_shape(&["station"]),
                context_shape(&["area", "line"]),
                context_shape(&["area", "station"]),
                context_shape(&["line", "station"]),
                context_shape(&["area", "line", "station"]),
            ])
        }
        source if source == ContextSource::RecentSearchContext.as_str() => {
            ContextShapeExpectation::Allowed(vec![context_shape(&["line", "station"])])
        }
        source if source == ContextSource::RecentBehaviorContext.as_str() => {
            // This enum variant is reserved, but there is no resolver-backed replay shape yet.
            ContextShapeExpectation::Unhandled(UNHANDLED_CONTEXT_SHAPE)
        }
        source if source == ContextSource::DefaultSafeContext.as_str() => {
            ContextShapeExpectation::Allowed(vec![Vec::new()])
        }
        _ => ContextShapeExpectation::Unhandled(UNHANDLED_CONTEXT_SHAPE),
    }
}

fn context_shape(shape: &[&str]) -> Vec<String> {
    shape.iter().map(|part| (*part).to_string()).collect()
}

fn context_shape_matches(actual_shape: &[String], expected_shapes: &[Vec<String>]) -> bool {
    expected_shapes
        .iter()
        .any(|expected_shape| actual_shape == expected_shape)
}

fn context_shape_parts(case: &ContextCoverageDoctorCase) -> Vec<String> {
    let mut parts = Vec::new();
    if case.has_area_context {
        parts.push("area".to_string());
    }
    if case.has_line_context {
        parts.push("line".to_string());
    }
    if case.has_station_context {
        parts.push("station".to_string());
    }
    parts
}

fn context_shape_label(shape: &[String]) -> String {
    if shape.is_empty() {
        "none".to_string()
    } else {
        shape.join(",")
    }
}

struct RetrievalParityCaseInput {
    id: &'static str,
    description: &'static str,
    target_station_id: &'static str,
    limit: usize,
    input_links: Vec<SchoolStationLink>,
    expected_order: Vec<&'static str>,
}

fn retrieval_parity_cases() -> Vec<RetrievalParityCaseInput> {
    vec![
        RetrievalParityCaseInput {
            id: "direct_station_first",
            description: "direct station candidate stays ahead of a closer neighbor",
            target_station_id: "st_target",
            limit: 2,
            input_links: vec![
                retrieval_link("school_neighbor", "st_neighbor", 2, 20),
                retrieval_link("school_direct", "st_target", 30, 300),
            ],
            expected_order: vec!["school_direct@st_target", "school_neighbor@st_neighbor"],
        },
        RetrievalParityCaseInput {
            id: "distance_meters_tiebreaker",
            description: "non-direct candidates sort by distance before walking minutes",
            target_station_id: "st_target",
            limit: 2,
            input_links: vec![
                retrieval_link("school_far", "st_far", 1, 200),
                retrieval_link("school_near", "st_near", 20, 100),
            ],
            expected_order: vec!["school_near@st_near", "school_far@st_far"],
        },
        RetrievalParityCaseInput {
            id: "walking_minutes_tiebreaker",
            description: "equal-distance candidates sort by walking minutes",
            target_station_id: "st_target",
            limit: 2,
            input_links: vec![
                retrieval_link("school_slow", "st_slow", 9, 120),
                retrieval_link("school_fast", "st_fast", 4, 120),
            ],
            expected_order: vec!["school_fast@st_fast", "school_slow@st_slow"],
        },
        RetrievalParityCaseInput {
            id: "school_id_tiebreaker",
            description: "equal-distance and equal-walk candidates sort by school id",
            target_station_id: "st_target",
            limit: 2,
            input_links: vec![
                retrieval_link("school_b", "st_same", 8, 120),
                retrieval_link("school_a", "st_same", 8, 120),
            ],
            expected_order: vec!["school_a@st_same", "school_b@st_same"],
        },
        RetrievalParityCaseInput {
            id: "station_id_tiebreaker",
            description: "same-school ties sort by station id",
            target_station_id: "st_target",
            limit: 2,
            input_links: vec![
                retrieval_link("school_a", "st_b", 8, 120),
                retrieval_link("school_a", "st_a", 8, 120),
            ],
            expected_order: vec!["school_a@st_a", "school_a@st_b"],
        },
        RetrievalParityCaseInput {
            id: "limit_after_ordering",
            description: "candidate limit is applied after contract ordering",
            target_station_id: "st_target",
            limit: 1,
            input_links: vec![
                retrieval_link("school_neighbor", "st_neighbor", 1, 10),
                retrieval_link("school_direct", "st_target", 30, 300),
            ],
            expected_order: vec!["school_direct@st_target"],
        },
    ]
}

fn run_retrieval_parity_case(input: RetrievalParityCaseInput) -> RetrievalParityDoctorCase {
    let RetrievalParityCaseInput {
        id,
        description,
        target_station_id,
        limit,
        input_links,
        expected_order,
    } = input;
    let input_order = input_links
        .iter()
        .map(candidate_link_key)
        .collect::<Vec<_>>();
    let mut actual_links = input_links;
    sort_candidate_links_for_retrieval(&mut actual_links, target_station_id);
    let effective_limit = limit.clamp(1, 10_000);
    let actual_order = actual_links
        .iter()
        .take(effective_limit)
        .map(candidate_link_key)
        .collect::<Vec<_>>();
    let expected_order = expected_order
        .into_iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    RetrievalParityDoctorCase {
        id: id.to_string(),
        description: description.to_string(),
        target_station_id: target_station_id.to_string(),
        limit,
        input_order,
        passed: actual_order == expected_order,
        expected_order,
        actual_order,
    }
}

fn retrieval_link(
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

fn candidate_link_key(link: &SchoolStationLink) -> String {
    format!("{}@{}", link.school_id, link.station_id)
}

fn storage_compatibility_entries() -> Vec<StorageCompatibilityEntry> {
    vec![
        storage_compatibility_entry(StorageCompatibilityEntrySpec {
            component: "postgres_postgis",
            display_name: "PostgreSQL/PostGIS",
            compatibility_level: "reference",
            runtime_status: "required_for_sql_only",
            data_role: "reference_write_store",
            public_mvp_gate: true,
            write_database_status: "reference",
            contract_evidence: "migrations_and_tests",
            operator_note: "Reference implementation for API, worker, migrations, imports, traces, jobs, and SQL-only candidate retrieval.",
        }),
        storage_compatibility_entry(StorageCompatibilityEntrySpec {
            component: "redis",
            display_name: "Redis",
            compatibility_level: "stable_optional",
            runtime_status: "optional",
            data_role: "cache_only",
            public_mvp_gate: true,
            write_database_status: "not_applicable",
            contract_evidence: "readyz_cache_status",
            operator_note: "Optional recommendation response cache; the fixed public-MVP gate starts it as cache-only service, but correctness must not depend on it.",
        }),
        storage_compatibility_entry(StorageCompatibilityEntrySpec {
            component: "opensearch",
            display_name: "OpenSearch",
            compatibility_level: "stable_optional",
            runtime_status: "optional",
            data_role: "candidate_retrieval_only",
            public_mvp_gate: false,
            write_database_status: "not_applicable",
            contract_evidence: "retrieval_parity_doctor",
            operator_note: "Optional full-mode candidate retrieval index; Rust recomputes final ranking and SQL-only remains the reference path.",
        }),
        storage_compatibility_entry(StorageCompatibilityEntrySpec {
            component: "mysql",
            display_name: "MySQL",
            compatibility_level: "experimental",
            runtime_status: "not_runtime_dependency",
            data_role: "compatibility_subset",
            public_mvp_gate: false,
            write_database_status: "not_implemented",
            contract_evidence: "docs_only_until_contract_tests",
            operator_note: "No committed write adapter, migrations, CI shard, or contract-report tables exist yet; do not claim production parity.",
        }),
        storage_compatibility_entry(StorageCompatibilityEntrySpec {
            component: "sqlite",
            display_name: "SQLite",
            compatibility_level: "artifact_only",
            runtime_status: "not_runtime_dependency",
            data_role: "artifact_export_only",
            public_mvp_gate: false,
            write_database_status: "read_only_artifact",
            contract_evidence: "no_write_contract",
            operator_note: "Reserved for read-only artifacts or exports only; never a primary write store.",
        }),
    ]
}

struct StorageCompatibilityEntrySpec {
    component: &'static str,
    display_name: &'static str,
    compatibility_level: &'static str,
    runtime_status: &'static str,
    data_role: &'static str,
    public_mvp_gate: bool,
    write_database_status: &'static str,
    contract_evidence: &'static str,
    operator_note: &'static str,
}

fn storage_compatibility_entry(spec: StorageCompatibilityEntrySpec) -> StorageCompatibilityEntry {
    StorageCompatibilityEntry {
        component: spec.component.to_string(),
        display_name: spec.display_name.to_string(),
        compatibility_level: spec.compatibility_level.to_string(),
        runtime_status: spec.runtime_status.to_string(),
        data_role: spec.data_role.to_string(),
        public_mvp_gate: spec.public_mvp_gate,
        write_database_status: spec.write_database_status.to_string(),
        contract_evidence: spec.contract_evidence.to_string(),
        operator_note: spec.operator_note.to_string(),
    }
}

fn context_shapes_label(shapes: &[Vec<String>]) -> String {
    shapes
        .iter()
        .map(|shape| context_shape_label(shape))
        .collect::<Vec<_>>()
        .join("|")
}

fn explanation_integrity_summary_from_replay(
    replay_summary: ReplayScenarioSummary,
) -> ExplanationIntegrityDoctorSummary {
    let cases = replay_summary
        .cases
        .into_iter()
        .map(|case| {
            let checks = case
                .checks
                .into_iter()
                .filter(|check| is_explanation_integrity_check(&check.name))
                .collect::<Vec<_>>();
            explanation_integrity_case(case.id, case.title, case.path, checks)
        })
        .collect::<Vec<_>>();

    let scenarios = cases.len();
    let passed = cases
        .iter()
        .filter(|case| case.status == ReplayScenarioStatus::Passed)
        .count();
    let blocked = scenarios - passed;
    let blockers = count_checks(
        &cases,
        Some(QualitySeverity::Blocker),
        Some(QualityCheckStatus::Failed),
    );
    let warnings = count_checks(
        &cases,
        Some(QualitySeverity::Warning),
        Some(QualityCheckStatus::Failed),
    );
    let explanation_integrity_total = count_checks(&cases, None, None);
    let explanation_integrity_passed = count_checks(&cases, None, Some(QualityCheckStatus::Passed));

    ExplanationIntegrityDoctorSummary {
        scenarios,
        passed,
        blocked,
        blockers,
        warnings,
        explanation_integrity_passed,
        explanation_integrity_total,
        cases,
    }
}

fn explanation_integrity_case(
    id: String,
    title: String,
    path: PathBuf,
    mut checks: Vec<ReplayScenarioCheck>,
) -> ExplanationIntegrityDoctorCase {
    if checks.is_empty() {
        checks.push(ReplayScenarioCheck {
            name: "explanation_integrity.presence".to_string(),
            severity: QualitySeverity::Blocker,
            status: QualityCheckStatus::Failed,
            message: "scenario emitted no explanation integrity or template checks".to_string(),
        });
    }
    let status = if has_failed_blocker(&checks) {
        ReplayScenarioStatus::Blocked
    } else {
        ReplayScenarioStatus::Passed
    };
    ExplanationIntegrityDoctorCase {
        id,
        title,
        path,
        status,
        checks,
    }
}

fn is_explanation_integrity_check(name: &str) -> bool {
    name.starts_with("explanation_")
}

fn has_failed_blocker(checks: &[ReplayScenarioCheck]) -> bool {
    checks.iter().any(|check| {
        check.severity == QualitySeverity::Blocker && check.status == QualityCheckStatus::Failed
    })
}

fn count_checks(
    cases: &[ExplanationIntegrityDoctorCase],
    severity: Option<QualitySeverity>,
    status: Option<QualityCheckStatus>,
) -> usize {
    cases
        .iter()
        .flat_map(|case| case.checks.iter())
        .filter(|check| severity.is_none_or(|severity| check.severity == severity))
        .filter(|check| status.is_none_or(|status| check.status == status))
        .count()
}

fn profile_pack_doctor_summary_from_lint(
    lint_summary: ProfilePackLintSummary,
) -> ProfilePackDoctorSummary {
    let files = lint_summary
        .files
        .into_iter()
        .map(profile_pack_doctor_file)
        .collect::<Vec<_>>();
    ProfilePackDoctorSummary {
        profile_packs: files.len(),
        ranking_config_dirs: lint_summary.ranking_configs.len(),
        reason_catalog_locales: files
            .iter()
            .map(|file| file.reason_catalog_locale_count)
            .sum(),
        reason_count: files.iter().map(|file| file.reason_count).sum(),
        fixture_references: files.iter().map(|file| file.fixture_references).sum(),
        connector_references: files.iter().map(|file| file.connector_references).sum(),
        evaluation_references: files.iter().map(|file| file.evaluation_references).sum(),
        source_manifest_references: files
            .iter()
            .map(|file| file.source_manifest_references)
            .sum(),
        event_csv_example_references: files
            .iter()
            .map(|file| file.event_csv_example_references)
            .sum(),
        archive_source_references: files
            .iter()
            .map(|file| file.archive_source_references)
            .sum(),
        optional_crawler_manifest_references: files
            .iter()
            .map(|file| file.optional_crawler_manifest_references)
            .sum(),
        connector_schema_contract_version: PROFILE_CONNECTOR_SCHEMA_CONTRACT_VERSION.to_string(),
        connector_schema_contracts: connector_schema_contract_summaries(),
        files,
    }
}

fn profile_pack_doctor_file(file: ProfilePackLintFile) -> ProfilePackDoctorFile {
    ProfilePackDoctorFile {
        path: file.path,
        profile_id: file.profile_id,
        ranking_config_dir: file.ranking_config_dir,
        fallback_config_path: file.fallback_config_path,
        reason_catalog_path: file.reason_catalog_path,
        schema_version: file.schema_version,
        kind: file.kind.as_str().to_string(),
        manifest_version: file.manifest_version,
        compatibility_level: file.compatibility_level.as_str().to_string(),
        content_kind_registry: file
            .content_kind_registry
            .into_iter()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        supported_content_kinds: file
            .supported_content_kinds
            .into_iter()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        runtime_executable_content_kinds: file
            .runtime_executable_content_kinds
            .into_iter()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        registry_only_content_kinds: file
            .registry_only_content_kinds
            .into_iter()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        placements: file
            .placements
            .into_iter()
            .map(|placement| placement.as_str().to_string())
            .collect(),
        reason_catalog_locale_count: file.reason_catalog_locale_count,
        reason_count: file.reason_count,
        fixture_references: file.fixture_count,
        connector_references: file.connector_count,
        connector_registry: file.connector_registry,
        evaluation_references: file.evaluation_reference_count,
        source_manifest_references: file.source_manifest_count,
        event_csv_example_references: file.event_csv_example_count,
        archive_source_references: file.archive_source_count,
        optional_crawler_manifest_references: file.optional_crawler_manifest_count,
    }
}

fn ingest_quality_doctor_summary_from_lint(
    lint_summary: ProfilePackLintSummary,
) -> Result<IngestQualityDoctorSummary> {
    let profiles = lint_summary
        .files
        .into_iter()
        .map(ingest_quality_doctor_profile)
        .collect::<Result<Vec<_>>>()?;
    let mut source_class_counts = BTreeMap::new();
    let mut manifest_kind_counts = BTreeMap::new();
    let mut manifest_schema_version_counts = BTreeMap::new();
    let mut archive_format_counts = BTreeMap::new();
    let mut crawler_source_maturity_counts = BTreeMap::new();
    let mut crawler_expected_shape_counts = BTreeMap::new();

    for profile in &profiles {
        merge_counts(&mut source_class_counts, &profile.source_class_counts);
        merge_counts(&mut manifest_kind_counts, &profile.manifest_kind_counts);
        merge_counts(
            &mut manifest_schema_version_counts,
            &profile.manifest_schema_version_counts,
        );
        merge_counts(&mut archive_format_counts, &profile.archive_format_counts);
        merge_counts(
            &mut crawler_source_maturity_counts,
            &profile.crawler_source_maturity_counts,
        );
        merge_counts(
            &mut crawler_expected_shape_counts,
            &profile.crawler_expected_shape_counts,
        );
    }

    Ok(IngestQualityDoctorSummary {
        profile_packs: profiles.len(),
        connector_references: profiles
            .iter()
            .map(|profile| profile.connector_references)
            .sum(),
        source_manifest_references: profiles
            .iter()
            .map(|profile| profile.source_manifest_references)
            .sum(),
        event_csv_example_references: profiles
            .iter()
            .map(|profile| profile.event_csv_example_references)
            .sum(),
        archive_source_references: profiles
            .iter()
            .map(|profile| profile.archive_source_references)
            .sum(),
        optional_crawler_manifest_references: profiles
            .iter()
            .map(|profile| profile.optional_crawler_manifest_references)
            .sum(),
        source_class_counts,
        manifest_kind_counts,
        manifest_schema_version_counts,
        runtime_executable_mappings: profiles
            .iter()
            .map(|profile| profile.runtime_executable_mappings)
            .sum(),
        non_runtime_mappings: profiles
            .iter()
            .map(|profile| profile.non_runtime_mappings)
            .sum(),
        local_reference_only_connectors: profiles
            .iter()
            .map(|profile| profile.local_reference_only_connectors)
            .sum(),
        dynamic_loading_enabled_connectors: profiles
            .iter()
            .map(|profile| profile.dynamic_loading_enabled_connectors)
            .sum(),
        live_fetch_default_connectors: profiles
            .iter()
            .map(|profile| profile.live_fetch_default_connectors)
            .sum(),
        crawler_allowlist_required_connectors: profiles
            .iter()
            .map(|profile| profile.crawler_allowlist_required_connectors)
            .sum(),
        source_manifest_file_count: profiles
            .iter()
            .map(|profile| profile.source_manifest_file_count)
            .sum(),
        archive_file_count: profiles
            .iter()
            .map(|profile| profile.archive_file_count)
            .sum(),
        crawler_target_count: profiles
            .iter()
            .map(|profile| profile.crawler_target_count)
            .sum(),
        archive_format_counts,
        crawler_source_maturity_counts,
        crawler_expected_shape_counts,
        evidence_scope: "db_free_profile_connector_manifest_coverage".to_string(),
        execution_scope: "no_import_or_live_crawl".to_string(),
        connector_schema_contract_version: PROFILE_CONNECTOR_SCHEMA_CONTRACT_VERSION.to_string(),
        connector_schema_contracts: connector_schema_contract_summaries(),
        profiles,
    })
}

fn ingest_quality_doctor_profile(file: ProfilePackLintFile) -> Result<IngestQualityDoctorProfile> {
    let connectors = file
        .connector_registry
        .iter()
        .map(|connector| ingest_quality_doctor_connector(&file.profile_id, connector))
        .collect::<Result<Vec<_>>>()?;
    let mut source_class_counts = BTreeMap::new();
    let mut manifest_kind_counts = BTreeMap::new();
    let mut manifest_schema_version_counts = BTreeMap::new();
    let mut archive_format_counts = BTreeMap::new();
    let mut crawler_source_maturity_counts = BTreeMap::new();
    let mut crawler_expected_shape_counts = BTreeMap::new();

    for connector in &connectors {
        increment(&mut source_class_counts, &connector.source_class);
        increment(&mut manifest_kind_counts, &connector.manifest_kind);
        increment(
            &mut manifest_schema_version_counts,
            &connector
                .manifest_schema_version
                .map(|version| version.to_string())
                .unwrap_or_else(|| "none".to_string()),
        );
        if let Some(archive_format) = connector.archive_format.as_deref() {
            increment(&mut archive_format_counts, archive_format);
        }
        if let Some(source_maturity) = connector.crawler_source_maturity.as_deref() {
            increment(&mut crawler_source_maturity_counts, source_maturity);
        }
        if let Some(expected_shape) = connector.crawler_expected_shape.as_deref() {
            increment(&mut crawler_expected_shape_counts, expected_shape);
        }
    }

    Ok(IngestQualityDoctorProfile {
        path: file.path,
        profile_id: file.profile_id,
        connector_references: connectors.len(),
        source_manifest_references: file.source_manifest_count,
        event_csv_example_references: file.event_csv_example_count,
        archive_source_references: file.archive_source_count,
        optional_crawler_manifest_references: file.optional_crawler_manifest_count,
        source_class_counts,
        manifest_kind_counts,
        manifest_schema_version_counts,
        runtime_executable_mappings: connectors
            .iter()
            .filter(|connector| connector.field_mapping_runtime_executable == Some(true))
            .count(),
        non_runtime_mappings: connectors
            .iter()
            .filter(|connector| connector.field_mapping_runtime_executable == Some(false))
            .count(),
        local_reference_only_connectors: connectors
            .iter()
            .filter(|connector| connector.local_reference_only)
            .count(),
        dynamic_loading_enabled_connectors: connectors
            .iter()
            .filter(|connector| connector.dynamic_loading_enabled)
            .count(),
        live_fetch_default_connectors: connectors
            .iter()
            .filter(|connector| connector.live_fetch_default)
            .count(),
        crawler_allowlist_required_connectors: connectors
            .iter()
            .filter(|connector| connector.allowlist_required)
            .count(),
        source_manifest_file_count: connectors
            .iter()
            .filter_map(|connector| connector.source_manifest_file_count)
            .sum(),
        archive_file_count: connectors
            .iter()
            .filter_map(|connector| connector.archive_file_count)
            .sum(),
        crawler_target_count: connectors
            .iter()
            .filter_map(|connector| connector.crawler_target_count)
            .sum(),
        archive_format_counts,
        crawler_source_maturity_counts,
        crawler_expected_shape_counts,
        connectors,
    })
}

fn ingest_quality_doctor_connector(
    profile_id: &str,
    connector: &ProfileConnectorRegistryEntry,
) -> Result<IngestQualityDoctorConnector> {
    let mut manifest_lint = "file_reference".to_string();
    let mut source_manifest_file_count = None;
    let mut archive_file_count = None;
    let mut archive_format = None;
    let mut archive_checksum_sha256 = None;
    let mut crawler_target_count = None;
    let mut crawler_source_maturity = None;
    let mut crawler_expected_shape = None;

    match connector.connector_type {
        ProfileConnectorType::SourceManifest => {
            let lint = lint_source_manifest_file(&connector.manifest_path).with_context(|| {
                format!(
                    "failed to lint profile {profile_id} source_manifest connector source_id={} manifest {}",
                    connector.source_id.as_deref().unwrap_or("unknown"),
                    connector.manifest_path.display()
                )
            })?;
            source_manifest_file_count = Some(lint.file_count);
            manifest_lint = "source_manifest_lint".to_string();
        }
        ProfileConnectorType::CrawlerManifest => {
            let lint = lint_crawl_manifest_file(&connector.manifest_path).with_context(|| {
                format!(
                    "failed to lint profile {profile_id} crawler_manifest connector source_id={} manifest {}",
                    connector.source_id.as_deref().unwrap_or("unknown"),
                    connector.manifest_path.display()
                )
            })?;
            crawler_target_count = Some(lint.target_count);
            crawler_source_maturity = Some(lint.source_maturity.as_str().to_string());
            crawler_expected_shape = lint.expected_shape.map(|shape| shape.as_str().to_string());
            manifest_lint = "crawler_manifest_lint".to_string();
        }
        ProfileConnectorType::ArchiveSource => {
            let lint = lint_archive_manifest_file(&connector.manifest_path).with_context(|| {
                format!(
                    "failed to lint profile {profile_id} archive_source connector source_id={} manifest {}",
                    connector.source_id.as_deref().unwrap_or("unknown"),
                    connector.manifest_path.display()
                )
            })?;
            ensure_archive_source_event_v1_runtime(profile_id, connector, &lint)?;
            archive_file_count = Some(lint.file_count);
            archive_format = Some(lint.archive_format.as_str().to_string());
            archive_checksum_sha256 = Some(lint.archive_checksum_sha256);
            manifest_lint = "archive_source_lint".to_string();
        }
        ProfileConnectorType::CsvImport | ProfileConnectorType::NdjsonImport => {}
    }

    Ok(IngestQualityDoctorConnector {
        connector_type: connector.connector_type.as_str().to_string(),
        source_class: connector.source_class.as_str().to_string(),
        manifest_kind: connector.manifest_kind.clone(),
        manifest_schema_version: connector.manifest_schema_version,
        source_id: connector.source_id.clone(),
        field_mapping: connector
            .field_mapping
            .as_ref()
            .map(|mapping| mapping.as_str().to_string()),
        field_mapping_runtime_executable: connector
            .field_mapping
            .as_ref()
            .map(|mapping| mapping.is_runtime_executable()),
        manifest_lint,
        source_manifest_file_count,
        archive_file_count,
        archive_format,
        archive_checksum_sha256,
        crawler_target_count,
        crawler_source_maturity,
        crawler_expected_shape,
        local_reference_only: connector.safety.local_reference_only,
        dynamic_loading_enabled: connector.safety.dynamic_loading_enabled,
        live_fetch_default: connector.safety.live_fetch_default,
        allowlist_required: connector.safety.allowlist_required,
        manifest_path: connector.manifest_path.clone(),
    })
}

fn connector_schema_contract_summaries() -> Vec<ConnectorSchemaContractSummary> {
    profile_connector_schema_contracts()
        .iter()
        .map(|contract| ConnectorSchemaContractSummary {
            connector_type: contract.connector_type.as_str().to_string(),
            source_class: contract.source_class.as_str().to_string(),
            manifest_kind: contract.manifest_kind.to_string(),
            manifest_schema_version: contract.manifest_schema_version,
            source_id_scope: contract.source_id_scope.to_string(),
            field_mapping_scope: contract.field_mapping_scope.to_string(),
            runtime_execution: contract.runtime_execution.to_string(),
            manifest_lint: contract.manifest_lint.to_string(),
            local_reference_only: contract.safety.local_reference_only,
            dynamic_loading_enabled: contract.safety.dynamic_loading_enabled,
            live_fetch_default: contract.safety.live_fetch_default,
            allowlist_required: contract.safety.allowlist_required,
        })
        .collect()
}

fn ensure_archive_source_event_v1_runtime(
    profile_id: &str,
    connector: &ProfileConnectorRegistryEntry,
    lint: &ArchiveSourceManifestLintFile,
) -> Result<()> {
    if !connector
        .field_mapping
        .as_ref()
        .is_some_and(|mapping| mapping.is_runtime_executable())
    {
        return Ok(());
    }

    anyhow::ensure!(
        lint.files.len() == 1,
        "profile {profile_id} archive_source connector source_id={} manifest {} has {} files; current event_v1 archive import runtime supports exactly one CSV or NDJSON file",
        connector.source_id.as_deref().unwrap_or("unknown"),
        connector.manifest_path.display(),
        lint.files.len()
    );
    let file = &lint.files[0];
    anyhow::ensure!(
        file.logical_name == "events",
        "profile {profile_id} archive_source connector source_id={} manifest {} file logical_name {} is unsupported by event_v1 archive import; expected events",
        connector.source_id.as_deref().unwrap_or("unknown"),
        connector.manifest_path.display(),
        file.logical_name
    );
    anyhow::ensure!(
        matches!(file.format.as_str(), "csv" | "ndjson"),
        "profile {profile_id} archive_source connector source_id={} manifest {} file {} uses unsupported runtime format {}; expected csv or ndjson",
        connector.source_id.as_deref().unwrap_or("unknown"),
        connector.manifest_path.display(),
        file.logical_name,
        file.format
    );
    Ok(())
}

fn ranking_config_doctor_file(file: RankingConfigLintFile) -> RankingConfigDoctorFile {
    RankingConfigDoctorFile {
        path: file.path,
        schema_version: file.schema_version,
        kind: file.kind.as_str().to_string(),
    }
}

fn ranking_config_doctor_profile(file: ProfilePackLintFile) -> RankingConfigDoctorProfile {
    RankingConfigDoctorProfile {
        path: file.path,
        profile_id: file.profile_id,
        ranking_config_dir: file.ranking_config_dir,
        fallback_config_path: file.fallback_config_path,
        reason_catalog_path: file.reason_catalog_path,
        compatibility_level: file.compatibility_level.as_str().to_string(),
        content_kind_registry: file
            .content_kind_registry
            .into_iter()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        supported_content_kinds: file
            .supported_content_kinds
            .into_iter()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        runtime_executable_content_kinds: file
            .runtime_executable_content_kinds
            .into_iter()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        registry_only_content_kinds: file
            .registry_only_content_kinds
            .into_iter()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        placements: file
            .placements
            .into_iter()
            .map(|placement| placement.as_str().to_string())
            .collect(),
        reason_catalog_locale_count: file.reason_catalog_locale_count,
        reason_count: file.reason_count,
        fixture_references: file.fixture_count,
        connector_references: file.connector_count,
        evaluation_references: file.evaluation_reference_count,
        source_manifest_references: file.source_manifest_count,
        event_csv_example_references: file.event_csv_example_count,
        archive_source_references: file.archive_source_count,
        optional_crawler_manifest_references: file.optional_crawler_manifest_count,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        explanation_integrity_summary_from_replay, ranking_config_doctor_summary_from_lint,
        run_context_coverage_doctor, run_explanation_integrity_doctor, run_ingest_quality_doctor,
        run_profile_pack_doctor, run_retrieval_parity_doctor, run_storage_compatibility_doctor,
    };
    use crate::{
        explanation_integrity::{QualityCheckStatus, QualitySeverity},
        replay::{
            ReplayScenarioCase, ReplayScenarioCheck, ReplayScenarioSource, ReplayScenarioStatus,
            ReplayScenarioSummary, DEFAULT_REPLAY_SCENARIO_PATH,
        },
    };

    #[test]
    fn committed_replay_scenarios_pass_explanation_integrity_doctor() {
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let summary = run_explanation_integrity_doctor(
            repo_root.join(DEFAULT_REPLAY_SCENARIO_PATH),
            repo_root.join("configs/ranking"),
            "explanation-integrity-doctor-test",
        )
        .expect("explanation integrity doctor");

        assert!(summary.scenarios >= 10);
        assert_eq!(summary.blockers, 0);
        assert_eq!(summary.blocked, 0);
        assert!(summary.explanation_integrity_total >= summary.scenarios * 6);
        assert_eq!(
            summary.explanation_integrity_passed,
            summary.explanation_integrity_total
        );
        assert!(summary
            .cases
            .iter()
            .all(|case| case.status == ReplayScenarioStatus::Passed));
        assert!(summary.cases.iter().all(|case| case
            .checks
            .iter()
            .all(|check| check.name.starts_with("explanation_"))));
    }

    #[test]
    fn committed_profile_packs_pass_profile_pack_doctor() {
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let summary =
            run_profile_pack_doctor(repo_root.join("configs/profiles")).expect("profile doctor");

        assert_eq!(summary.profile_packs, summary.files.len());
        assert!(summary.reason_count > 0);
        assert_eq!(
            summary.reason_count,
            summary
                .files
                .iter()
                .map(|file| file.reason_count)
                .sum::<usize>()
        );
        assert_eq!(
            summary.fixture_references,
            summary
                .files
                .iter()
                .map(|file| file.fixture_references)
                .sum::<usize>()
        );
        assert_eq!(
            summary.source_manifest_references,
            summary
                .files
                .iter()
                .map(|file| file.source_manifest_references)
                .sum::<usize>()
        );
        assert_eq!(
            summary.event_csv_example_references,
            summary
                .files
                .iter()
                .map(|file| file.event_csv_example_references)
                .sum::<usize>()
        );
        assert_eq!(
            summary.archive_source_references,
            summary
                .files
                .iter()
                .map(|file| file.archive_source_references)
                .sum::<usize>()
        );
        assert_eq!(
            summary.optional_crawler_manifest_references,
            summary
                .files
                .iter()
                .map(|file| file.optional_crawler_manifest_references)
                .sum::<usize>()
        );
        assert_eq!(
            summary.connector_schema_contract_version,
            "local_stable_connector_manifest_schema_v1"
        );
        assert_eq!(summary.connector_schema_contracts.len(), 5);
        assert!(summary
            .connector_schema_contracts
            .iter()
            .any(|contract| contract.connector_type == "archive_source"
                && contract.manifest_kind == "archive_source"
                && contract.manifest_schema_version == Some(1)
                && contract.field_mapping_scope == "event_v1_required_for_runtime"));

        let local_discovery = summary
            .files
            .iter()
            .find(|file| file.profile_id == "local-discovery-generic")
            .expect("local discovery profile");
        assert!(local_discovery.fixture_references > 0);
        assert_eq!(local_discovery.archive_source_references, 1);

        let school_event_jp = summary
            .files
            .iter()
            .find(|file| file.profile_id == "school-event-jp")
            .expect("school event jp profile");
        assert_eq!(
            school_event_jp.supported_content_kinds,
            vec!["school", "event"]
        );
        assert!(school_event_jp.source_manifest_references > 0);
        assert!(school_event_jp.event_csv_example_references > 0);
        assert!(school_event_jp.optional_crawler_manifest_references > 0);
        assert_eq!(
            school_event_jp.connector_registry.len(),
            school_event_jp.connector_references
        );
        assert!(school_event_jp
            .connector_registry
            .iter()
            .any(|entry| entry.safety.allowlist_required));
    }

    #[test]
    fn committed_profile_packs_pass_ingest_quality_doctor() {
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let summary = run_ingest_quality_doctor(repo_root.join("configs/profiles"))
            .expect("ingest quality doctor");

        assert_eq!(summary.profile_packs, 2);
        assert_eq!(summary.connector_references, 10);
        assert_eq!(summary.source_class_counts.get("archive_import"), Some(&1));
        assert_eq!(summary.source_class_counts.get("csv_import"), Some(&6));
        assert_eq!(summary.source_class_counts.get("ndjson_import"), Some(&2));
        assert_eq!(summary.source_class_counts.get("html_crawl"), Some(&1));
        assert_eq!(summary.manifest_kind_counts.get("archive_source"), Some(&1));
        assert_eq!(summary.manifest_kind_counts.get("import_source"), Some(&4));
        assert_eq!(summary.manifest_kind_counts.get("csv_file"), Some(&2));
        assert_eq!(summary.manifest_kind_counts.get("ndjson_file"), Some(&2));
        assert_eq!(summary.manifest_kind_counts.get("crawler_source"), Some(&1));
        assert_eq!(summary.manifest_schema_version_counts.get("1"), Some(&6));
        assert_eq!(summary.manifest_schema_version_counts.get("none"), Some(&4));
        assert_eq!(summary.runtime_executable_mappings, 5);
        assert_eq!(summary.non_runtime_mappings, 0);
        assert_eq!(summary.source_manifest_file_count, 4);
        assert_eq!(summary.archive_file_count, 1);
        assert_eq!(summary.crawler_target_count, 1);
        assert_eq!(summary.crawler_allowlist_required_connectors, 1);
        assert_eq!(summary.archive_format_counts.get("tar"), Some(&1));
        assert_eq!(
            summary.crawler_source_maturity_counts.get("parser_only"),
            Some(&1)
        );
        assert_eq!(
            summary
                .crawler_expected_shape_counts
                .get("html_heading_page"),
            Some(&1)
        );
        assert_eq!(summary.execution_scope, "no_import_or_live_crawl");
        assert_eq!(
            summary.connector_schema_contract_version,
            "local_stable_connector_manifest_schema_v1"
        );
        assert_eq!(summary.connector_schema_contracts.len(), 5);

        let school_event_jp = summary
            .profiles
            .iter()
            .find(|profile| profile.profile_id == "school-event-jp")
            .expect("school event jp profile");
        assert_eq!(school_event_jp.connector_references, 7);
        assert_eq!(school_event_jp.source_manifest_file_count, 4);
        assert_eq!(school_event_jp.crawler_target_count, 1);
        assert_eq!(
            school_event_jp.manifest_schema_version_counts.get("1"),
            Some(&5)
        );
        assert_eq!(
            school_event_jp.manifest_schema_version_counts.get("none"),
            Some(&2)
        );
        assert!(school_event_jp.connectors.iter().any(|connector| {
            connector.connector_type == "crawler_manifest"
                && connector.allowlist_required
                && connector.manifest_lint == "crawler_manifest_lint"
                && connector.manifest_schema_version == Some(1)
        }));

        let local_discovery = summary
            .profiles
            .iter()
            .find(|profile| profile.profile_id == "local-discovery-generic")
            .expect("local discovery profile");
        assert_eq!(local_discovery.archive_source_references, 1);
        assert_eq!(local_discovery.archive_file_count, 1);
        assert_eq!(
            local_discovery.manifest_schema_version_counts.get("1"),
            Some(&1)
        );
        assert_eq!(
            local_discovery.manifest_schema_version_counts.get("none"),
            Some(&2)
        );
        assert!(local_discovery.connectors.iter().any(|connector| {
            connector.connector_type == "archive_source"
                && connector.manifest_lint == "archive_source_lint"
                && connector.manifest_schema_version == Some(1)
                && connector.archive_format.as_deref() == Some("tar")
                && connector.archive_checksum_sha256.is_some()
        }));
    }

    #[test]
    fn committed_config_lint_summary_feeds_ranking_config_doctor() {
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let ranking_summary =
            config::lint_ranking_config_dir(repo_root.join("configs/ranking")).expect("ranking");
        let profile_summary =
            config::lint_profile_pack_dir(repo_root.join("configs/profiles")).expect("profiles");

        let summary = ranking_config_doctor_summary_from_lint(
            Some("local-discovery-generic".to_string()),
            Some("minimal".to_string()),
            ranking_summary,
            profile_summary,
        );

        assert_eq!(
            summary.active_profile_id.as_deref(),
            Some("local-discovery-generic")
        );
        assert_eq!(summary.fixture_set_id.as_deref(), Some("minimal"));
        assert_eq!(summary.ranking_files, 8);
        assert_eq!(
            summary.ranking_kind_counts.get("ranking_placement"),
            Some(&4)
        );
        assert_eq!(summary.ranking_kind_counts.get("ranking_schools"), Some(&1));
        assert_eq!(summary.profile_packs, 2);
        assert_eq!(summary.referenced_ranking_config_dirs, 1);
        assert_eq!(summary.reason_catalog_references, summary.profile_packs);
        assert!(summary.reason_count > 0);
        assert!(summary.fixture_references > 0);
        assert_eq!(summary.files.len(), summary.ranking_files);
        assert_eq!(summary.profiles.len(), summary.profile_packs);
    }

    #[test]
    fn committed_replay_scenarios_pass_context_coverage_doctor() {
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let summary = run_context_coverage_doctor(repo_root.join(DEFAULT_REPLAY_SCENARIO_PATH))
            .expect("context coverage doctor");

        assert!(summary.scenarios >= 10);
        assert_eq!(summary.scenarios_without_context, 0);
        assert!(!summary.has_blockers());
        assert!(summary.context_shape_mismatches.is_empty());
        for expected_source in ["request_area", "request_line", "default_safe_context"] {
            assert!(summary
                .required_context_sources
                .iter()
                .any(|source| source.context_source == expected_source && source.covered));
        }
        assert!(summary
            .tag_counts
            .get("area_context")
            .is_some_and(|count| *count > 0));
        assert!(summary
            .tag_counts
            .get("line_context")
            .is_some_and(|count| *count > 0));
        assert!(summary
            .fallback_stage_counts
            .get("safe_global_popular")
            .is_some_and(|count| *count > 0));
        assert_eq!(summary.scenarios_with_candidate_counts, summary.scenarios);
        assert!(summary.candidate_count_expectations >= summary.scenarios);
    }

    #[test]
    fn retrieval_parity_doctor_passes_candidate_ordering_contract() {
        let summary = run_retrieval_parity_doctor();

        assert_eq!(summary.case_count, 6);
        assert_eq!(summary.passed, summary.case_count);
        assert_eq!(summary.failed, 0);
        assert!(!summary.has_blockers());
        assert!(!summary.requires_database);
        assert!(!summary.requires_opensearch);
        assert!(!summary.public_mvp_gate);
        assert_eq!(
            summary.ordering_contract,
            vec![
                "direct_station".to_string(),
                "distance_meters".to_string(),
                "walking_minutes".to_string(),
                "school_id".to_string(),
                "station_id".to_string()
            ]
        );
        assert_eq!(summary.opensearch_sort_contract[0].field, "_score");
        assert_eq!(summary.opensearch_sort_contract[0].order, "desc");

        let direct_case = summary
            .cases
            .iter()
            .find(|case| case.id == "direct_station_first")
            .expect("direct station case");
        assert_eq!(
            direct_case.actual_order,
            vec![
                "school_direct@st_target".to_string(),
                "school_neighbor@st_neighbor".to_string()
            ]
        );
    }

    #[test]
    fn retrieval_parity_case_clamps_candidate_limit_like_runtime() {
        let case = super::run_retrieval_parity_case(super::RetrievalParityCaseInput {
            id: "limit_zero_clamp",
            description: "zero candidate limit uses the runtime minimum",
            target_station_id: "st_target",
            limit: 0,
            input_links: vec![
                super::retrieval_link("school_neighbor", "st_neighbor", 1, 10),
                super::retrieval_link("school_direct", "st_target", 30, 300),
            ],
            expected_order: vec!["school_direct@st_target"],
        });

        assert!(case.passed);
        assert_eq!(case.limit, 0);
        assert_eq!(
            case.actual_order,
            vec!["school_direct@st_target".to_string()]
        );
    }

    #[test]
    fn storage_compatibility_doctor_reports_static_registry() {
        let summary = run_storage_compatibility_doctor();

        assert_eq!(
            summary.registry_version,
            "v0.4.0-static-storage-compatibility"
        );
        assert_eq!(summary.component_count, 5);
        assert_eq!(summary.final_ranking_owner, "rust");
        assert!(summary
            .profile_compatibility_source
            .contains("profile manifests"));
        assert_eq!(
            summary.sql_only_required_components,
            vec!["postgres_postgis".to_string()]
        );
        assert_eq!(
            summary.optional_runtime_components,
            vec!["redis".to_string(), "opensearch".to_string()]
        );
        assert_eq!(
            summary.public_mvp_gate_components,
            vec!["postgres_postgis".to_string(), "redis".to_string()]
        );
        assert_eq!(
            summary.compatibility_level_counts.get("reference"),
            Some(&1)
        );
        assert_eq!(
            summary.compatibility_level_counts.get("stable_optional"),
            Some(&2)
        );
        assert_eq!(
            summary.compatibility_level_counts.get("experimental"),
            Some(&1)
        );
        assert_eq!(
            summary.compatibility_level_counts.get("artifact_only"),
            Some(&1)
        );

        let postgres = summary
            .entries
            .iter()
            .find(|entry| entry.component == "postgres_postgis")
            .expect("postgres entry");
        assert_eq!(postgres.compatibility_level, "reference");
        assert_eq!(postgres.runtime_status, "required_for_sql_only");
        assert!(postgres.public_mvp_gate);
        assert_eq!(postgres.write_database_status, "reference");

        let opensearch = summary
            .entries
            .iter()
            .find(|entry| entry.component == "opensearch")
            .expect("opensearch entry");
        assert_eq!(opensearch.data_role, "candidate_retrieval_only");
        assert!(!opensearch.public_mvp_gate);
        assert_eq!(opensearch.write_database_status, "not_applicable");

        let mysql = summary
            .entries
            .iter()
            .find(|entry| entry.component == "mysql")
            .expect("mysql entry");
        assert_eq!(mysql.compatibility_level, "experimental");
        assert!(!mysql.public_mvp_gate);
        assert_eq!(mysql.write_database_status, "not_implemented");

        let sqlite = summary
            .entries
            .iter()
            .find(|entry| entry.component == "sqlite")
            .expect("sqlite entry");
        assert_eq!(sqlite.compatibility_level, "artifact_only");
        assert!(!sqlite.public_mvp_gate);
        assert_eq!(sqlite.write_database_status, "read_only_artifact");
    }

    #[test]
    fn storage_compatibility_doctor_json_uses_contract_field_names() {
        let summary = run_storage_compatibility_doctor();
        let payload = serde_json::to_value(&summary).expect("json payload");

        assert_eq!(
            payload["public_mvp_gate_components"],
            serde_json::json!(["postgres_postgis", "redis"])
        );

        let entries = payload["entries"].as_array().expect("entries array");
        let mysql = entries
            .iter()
            .find(|entry| entry["component"] == "mysql")
            .expect("mysql entry");

        assert_eq!(mysql["compatibility_level"], "experimental");
        assert_eq!(mysql["write_database_status"], "not_implemented");
        assert!(mysql.get("write_path_status").is_none());
    }

    #[test]
    fn context_coverage_doctor_blocks_when_required_sources_are_missing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let scenario_path = temp.path().join("request_area_only.yaml");
        std::fs::write(
            &scenario_path,
            r#"
schema_version: 1
kind: replay_scenario
id: S00_CONTEXT_COVERAGE
title: Context coverage
tags:
  - area_context
query:
  target_station_id: st_tokyo
  limit: 1
  placement: search
  debug: false
  context:
    context_source: request_area
    confidence: 0.95
    area:
      country: JP
      prefecture_name: Tokyo
    privacy_level: coarse_area
    fallback_policy: school_event_jp_default
    gate_policy: geo_line_default
dataset:
  schools: []
  events: []
  stations: []
  school_station_links: []
  popularity_snapshots: []
  user_affinity_snapshots: []
  area_affinity_snapshots: []
expectations:
  fallback_stage: same_city
  ordered:
    - "school:school_tokyo"
  candidate_counts:
    same_city: 0
"#,
        )
        .expect("write scenario");

        let summary = run_context_coverage_doctor(&scenario_path).expect("context coverage doctor");

        assert!(summary.has_blockers());
        assert_eq!(
            summary.missing_required_context_sources,
            vec![
                "request_line".to_string(),
                "default_safe_context".to_string()
            ]
        );
        assert_eq!(summary.context_source_counts.get("request_area"), Some(&1));
        assert_eq!(summary.scenarios_with_candidate_counts, 1);
        assert_eq!(
            summary.blocker_message().as_deref(),
            Some("missing_required_context_sources=request_line,default_safe_context")
        );
    }

    #[test]
    fn context_coverage_doctor_blocks_when_context_source_shape_mismatches() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("area.yaml"),
            context_coverage_scenario_yaml(
                "S00_AREA",
                "request_area",
                r#"
    area:
      country: JP
      prefecture_name: Tokyo
"#,
                "same_city",
            ),
        )
        .expect("write area scenario");
        std::fs::write(
            temp.path().join("line_bad_shape.yaml"),
            context_coverage_scenario_yaml(
                "S01_LINE_BAD_SHAPE",
                "request_area",
                r#"
    area:
      country: JP
      prefecture_name: Tokyo
    line:
      line_name: Yamanote Line
"#,
                "same_line",
            ),
        )
        .expect("write line scenario");
        std::fs::write(
            temp.path().join("line.yaml"),
            context_coverage_scenario_yaml(
                "S01_LINE",
                "request_line",
                r#"
    line:
      line_name: Yamanote Line
"#,
                "same_line",
            ),
        )
        .expect("write valid line scenario");
        std::fs::write(
            temp.path().join("default.yaml"),
            context_coverage_scenario_yaml(
                "S02_DEFAULT",
                "default_safe_context",
                "",
                "safe_global_popular",
            ),
        )
        .expect("write default scenario");

        let summary = run_context_coverage_doctor(temp.path()).expect("context coverage doctor");

        assert!(summary.has_blockers());
        assert!(summary.missing_required_context_sources.is_empty());
        assert_eq!(summary.context_shape_mismatches.len(), 1);
        let mismatch = &summary.context_shape_mismatches[0];
        assert_eq!(mismatch.id, "S01_LINE_BAD_SHAPE");
        assert_eq!(mismatch.context_source, "request_area");
        assert_eq!(mismatch.expected_shape, "area");
        assert_eq!(
            mismatch.actual_shape,
            vec!["area".to_string(), "line".to_string()]
        );
        assert_eq!(
            summary.blocker_message().as_deref(),
            Some("context_shape_mismatches=S01_LINE_BAD_SHAPE")
        );
    }

    #[test]
    fn context_coverage_doctor_accepts_resolver_backed_context_source_shapes() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("area.yaml"),
            context_coverage_scenario_yaml(
                "S00_AREA",
                "request_area",
                r#"
    area:
      country: JP
      prefecture_name: Tokyo
"#,
                "same_city",
            ),
        )
        .expect("write area scenario");
        std::fs::write(
            temp.path().join("line.yaml"),
            context_coverage_scenario_yaml(
                "S01_LINE",
                "request_line",
                r#"
    area:
      country: JP
      prefecture_name: Tokyo
    line:
      line_name: Yamanote Line
"#,
                "same_line",
            ),
        )
        .expect("write line scenario");
        std::fs::write(
            temp.path().join("default.yaml"),
            context_coverage_scenario_yaml(
                "S02_DEFAULT",
                "default_safe_context",
                "",
                "safe_global_popular",
            ),
        )
        .expect("write default scenario");
        std::fs::write(
            temp.path().join("request_station.yaml"),
            context_coverage_scenario_yaml(
                "S03_REQUEST_STATION",
                "request_station",
                r#"
    area:
      country: JP
      prefecture_name: Tokyo
    line:
      line_name: Yamanote Line
    station:
      station_id: st_tokyo
      station_name: Tokyo
"#,
                "strict_station",
            ),
        )
        .expect("write request station scenario");
        std::fs::write(
            temp.path().join("user_profile.yaml"),
            context_coverage_scenario_yaml(
                "S04_USER_PROFILE",
                "user_profile_area",
                r#"
    station:
      station_id: st_shibuya
      station_name: Shibuya
"#,
                "strict_station",
            ),
        )
        .expect("write user profile scenario");
        std::fs::write(
            temp.path().join("recent_search.yaml"),
            context_coverage_scenario_yaml(
                "S05_RECENT_SEARCH",
                "recent_search_context",
                r#"
    line:
      line_name: Yamanote Line
    station:
      station_id: st_tamachi
      station_name: Tamachi
"#,
                "strict_station",
            ),
        )
        .expect("write recent search scenario");

        let summary = run_context_coverage_doctor(temp.path()).expect("context coverage doctor");

        assert!(!summary.has_blockers());
        assert!(summary.context_shape_mismatches.is_empty());
    }

    #[test]
    fn context_coverage_doctor_blocks_unhandled_context_sources() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("area.yaml"),
            context_coverage_scenario_yaml(
                "S00_AREA",
                "request_area",
                r#"
    area:
      country: JP
      prefecture_name: Tokyo
"#,
                "same_city",
            ),
        )
        .expect("write area scenario");
        std::fs::write(
            temp.path().join("line.yaml"),
            context_coverage_scenario_yaml(
                "S01_LINE",
                "request_line",
                r#"
    line:
      line_name: Yamanote Line
"#,
                "same_line",
            ),
        )
        .expect("write line scenario");
        std::fs::write(
            temp.path().join("default.yaml"),
            context_coverage_scenario_yaml(
                "S02_DEFAULT",
                "default_safe_context",
                "",
                "safe_global_popular",
            ),
        )
        .expect("write default scenario");
        std::fs::write(
            temp.path().join("recent_behavior.yaml"),
            context_coverage_scenario_yaml(
                "S03_RECENT_BEHAVIOR",
                "recent_behavior_context",
                r#"
    area:
      country: JP
      prefecture_name: Tokyo
"#,
                "same_prefecture",
            ),
        )
        .expect("write recent behavior scenario");

        let summary = run_context_coverage_doctor(temp.path()).expect("context coverage doctor");

        assert!(summary.has_blockers());
        assert!(summary.missing_required_context_sources.is_empty());
        assert_eq!(summary.context_shape_mismatches.len(), 1);
        let mismatch = &summary.context_shape_mismatches[0];
        assert_eq!(mismatch.id, "S03_RECENT_BEHAVIOR");
        assert_eq!(mismatch.context_source, "recent_behavior_context");
        assert_eq!(mismatch.expected_shape, super::UNHANDLED_CONTEXT_SHAPE);
        assert_eq!(mismatch.actual_shape, vec!["area".to_string()]);
    }

    #[test]
    fn doctor_summary_ignores_non_explanation_replay_blockers() {
        let summary = explanation_integrity_summary_from_replay(replay_summary_with_checks(vec![
            ReplayScenarioCheck {
                name: "pairwise.school_a_before_school_b".to_string(),
                severity: QualitySeverity::Blocker,
                status: QualityCheckStatus::Failed,
                message: "ordering failed".to_string(),
            },
            ReplayScenarioCheck {
                name: "explanation_integrity.reason_catalog".to_string(),
                severity: QualitySeverity::Blocker,
                status: QualityCheckStatus::Passed,
                message: "catalog ok".to_string(),
            },
        ]));

        assert_eq!(summary.passed, 1);
        assert_eq!(summary.blocked, 0);
        assert_eq!(summary.blockers, 0);
        assert_eq!(summary.explanation_integrity_passed, 1);
        assert_eq!(summary.explanation_integrity_total, 1);
        assert_eq!(summary.cases[0].status, ReplayScenarioStatus::Passed);
        assert_eq!(
            summary.cases[0].checks[0].name,
            "explanation_integrity.reason_catalog"
        );
    }

    #[test]
    fn doctor_summary_blocks_when_explanation_checks_are_missing() {
        let summary = explanation_integrity_summary_from_replay(replay_summary_with_checks(vec![
            ReplayScenarioCheck {
                name: "pairwise.school_a_before_school_b".to_string(),
                severity: QualitySeverity::Blocker,
                status: QualityCheckStatus::Passed,
                message: "ordering ok".to_string(),
            },
        ]));

        assert_eq!(summary.passed, 0);
        assert_eq!(summary.blocked, 1);
        assert_eq!(summary.blockers, 1);
        assert_eq!(summary.explanation_integrity_passed, 0);
        assert_eq!(summary.explanation_integrity_total, 1);
        assert_eq!(summary.cases[0].status, ReplayScenarioStatus::Blocked);
        assert_eq!(
            summary.cases[0].checks[0].name,
            "explanation_integrity.presence"
        );
    }

    fn replay_summary_with_checks(checks: Vec<ReplayScenarioCheck>) -> ReplayScenarioSummary {
        ReplayScenarioSummary {
            profile_id: None,
            scenario_source: ReplayScenarioSource::explicit_path(PathBuf::from("self_review.yaml")),
            scenarios: 1,
            passed: 0,
            blocked: 1,
            blockers: checks
                .iter()
                .filter(|check| {
                    check.severity == QualitySeverity::Blocker
                        && check.status == QualityCheckStatus::Failed
                })
                .count(),
            warnings: checks
                .iter()
                .filter(|check| {
                    check.severity == QualitySeverity::Warning
                        && check.status == QualityCheckStatus::Failed
                })
                .count(),
            pairwise_passed: 0,
            pairwise_total: 0,
            explanation_integrity_passed: 0,
            explanation_integrity_total: 0,
            cases: vec![ReplayScenarioCase {
                id: "S00_SELF_REVIEW".to_string(),
                title: "Self review".to_string(),
                path: PathBuf::from("self_review.yaml"),
                status: ReplayScenarioStatus::Blocked,
                expected_fallback_stage: "same_city".to_string(),
                actual_fallback_stage: Some("same_city".to_string()),
                expected_order: vec!["school:school_a".to_string()],
                actual_order: vec!["school:school_a".to_string()],
                checks,
            }],
        }
    }

    fn context_coverage_scenario_yaml(
        id: &str,
        context_source: &str,
        context_shape: &str,
        fallback_stage: &str,
    ) -> String {
        format!(
            r#"
schema_version: 1
kind: replay_scenario
id: {id}
title: Context coverage
query:
  target_station_id: st_tokyo
  limit: 1
  placement: search
  debug: false
  context:
    context_source: {context_source}
    confidence: 0.95
{context_shape}
    privacy_level: coarse_area
    fallback_policy: school_event_jp_default
    gate_policy: geo_line_default
dataset:
  schools: []
  events: []
  stations: []
  school_station_links: []
  popularity_snapshots: []
  user_affinity_snapshots: []
  area_affinity_snapshots: []
expectations:
  fallback_stage: {fallback_stage}
  ordered:
    - "school:school_tokyo"
  candidate_counts:
    {fallback_stage}: 0
"#
        )
    }
}
