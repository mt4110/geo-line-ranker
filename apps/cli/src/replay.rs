use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
#[cfg(feature = "storage-backends")]
use api_contracts::{FallbackStageDto, RecommendationRequest, RecommendationResponse};
#[cfg(feature = "storage-backends")]
use config::AppSettings;
use config::RankingProfiles;
use domain::{
    ContentKind, FallbackStage, RankingDataset, RankingQuery, RecommendationItem,
    RecommendationResult,
};
use ranking::RankingEngine;
#[cfg(feature = "storage-backends")]
use ranking::{AreaGraphExpansion, CandidateGraphExpansion, LineGraphExpansion};
use serde::{Deserialize, Serialize};
#[cfg(feature = "storage-backends")]
use storage_postgres::{ContextCandidateLinkQuery, PgRepository, RecommendationTraceReplayRow};

use crate::explanation_integrity::{
    check_recommendation_result_integrity_with_catalog, QualityCheckStatus, QualitySeverity,
};
#[cfg(feature = "storage-backends")]
use crate::repository::pg_repository;

pub const DEFAULT_REPLAY_SCENARIO_PATH: &str = "configs/evaluation/scenarios";
const REPLAY_SCENARIO_SCHEMA_VERSION: u32 = 1;
const REPLAY_PAIRWISE_PACK_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayEvaluationSummary {
    pub evaluated: usize,
    pub matched: usize,
    pub mismatched: usize,
    pub failed: usize,
    pub cases: Vec<ReplayEvaluationCase>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayEvaluationCase {
    pub trace_id: i64,
    pub status: ReplayEvaluationStatus,
    pub request_id: Option<String>,
    pub expected_fallback_stage: Option<String>,
    pub actual_fallback_stage: Option<String>,
    pub expected_order: Vec<String>,
    pub actual_order: Vec<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayEvaluationStatus {
    Matched,
    Mismatched,
    Failed,
}

impl ReplayEvaluationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Matched => "matched",
            Self::Mismatched => "mismatched",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReplayScenario {
    pub schema_version: u32,
    pub kind: ReplayScenarioKind,
    pub id: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub query: RankingQuery,
    pub dataset: RankingDataset,
    pub expectations: ReplayScenarioExpectations,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplayScenarioKind {
    ReplayScenario,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReplayScenarioExpectations {
    pub fallback_stage: FallbackStage,
    #[serde(default)]
    pub ordered: Vec<String>,
    #[serde(default)]
    pub pairwise: Vec<PairwiseExpectation>,
    #[serde(default)]
    pub absent_from_result: Vec<String>,
    #[serde(default)]
    pub candidate_counts: BTreeMap<String, usize>,
    #[serde(default)]
    pub required_reason_codes: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub absent_content_kinds: Vec<ContentKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_items_per_school: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_items_per_group: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PairwiseExpectation {
    pub higher: String,
    pub lower: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ReplayPairwisePack {
    pub schema_version: u32,
    pub kind: ReplayPairwisePackKind,
    pub expectations: Vec<ReplayPairwiseScenarioExpectation>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ReplayPairwisePackKind {
    ReplayPairwisePack,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ReplayPairwiseScenarioExpectation {
    pub scenario_id: String,
    pub pairwise: Vec<PairwiseExpectation>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplayScenarioSource {
    pub kind: ReplayScenarioSourceKind,
    pub path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_manifest: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason_catalog_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pairwise_pack: Option<PathBuf>,
}

impl ReplayScenarioSource {
    pub fn default_path(path: PathBuf) -> Self {
        Self {
            kind: ReplayScenarioSourceKind::DefaultPath,
            path,
            profile_manifest: None,
            reason_catalog_path: None,
            pairwise_pack: None,
        }
    }

    pub fn explicit_path(path: PathBuf) -> Self {
        Self {
            kind: ReplayScenarioSourceKind::ExplicitPath,
            path,
            profile_manifest: None,
            reason_catalog_path: None,
            pairwise_pack: None,
        }
    }

    pub fn profile_evaluation(
        path: PathBuf,
        profile_manifest: PathBuf,
        reason_catalog_path: PathBuf,
        pairwise_pack: Option<PathBuf>,
    ) -> Self {
        Self {
            kind: ReplayScenarioSourceKind::ProfileEvaluation,
            path,
            profile_manifest: Some(profile_manifest),
            reason_catalog_path: Some(reason_catalog_path),
            pairwise_pack,
        }
    }

    pub fn with_reason_catalog_path(mut self, reason_catalog_path: PathBuf) -> Self {
        self.reason_catalog_path = Some(reason_catalog_path);
        self
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplayScenarioSourceKind {
    DefaultPath,
    ExplicitPath,
    ProfileEvaluation,
}

impl ReplayScenarioSourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DefaultPath => "default_path",
            Self::ExplicitPath => "explicit_path",
            Self::ProfileEvaluation => "profile_evaluation",
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplayScenarioSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    pub scenario_source: ReplayScenarioSource,
    pub scenarios: usize,
    pub passed: usize,
    pub blocked: usize,
    pub blockers: usize,
    pub warnings: usize,
    pub pairwise_passed: usize,
    pub pairwise_total: usize,
    pub explanation_integrity_passed: usize,
    pub explanation_integrity_total: usize,
    pub cases: Vec<ReplayScenarioCase>,
}

impl ReplayScenarioSummary {
    pub fn has_blockers(&self) -> bool {
        self.blockers > 0
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplayScenarioCase {
    pub id: String,
    pub title: String,
    pub path: PathBuf,
    pub status: ReplayScenarioStatus,
    pub expected_fallback_stage: String,
    pub actual_fallback_stage: Option<String>,
    pub expected_order: Vec<String>,
    pub actual_order: Vec<String>,
    pub checks: Vec<ReplayScenarioCheck>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplayScenarioStatus {
    Passed,
    Blocked,
}

impl ReplayScenarioStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplayScenarioCheck {
    pub name: String,
    pub severity: QualitySeverity,
    pub status: QualityCheckStatus,
    pub message: String,
}

pub fn run_replay_scenarios(
    scenario_path: impl AsRef<Path>,
    ranking_config_dir: impl AsRef<Path>,
    algorithm_version: &str,
) -> Result<ReplayScenarioSummary> {
    let source = replay_scenario_source_for_path(scenario_path.as_ref());
    run_replay_scenarios_with_source(ranking_config_dir, algorithm_version, None, source)
}

fn replay_scenario_source_for_path(scenario_path: &Path) -> ReplayScenarioSource {
    if is_default_replay_scenario_path(scenario_path) {
        ReplayScenarioSource::default_path(scenario_path.to_path_buf())
    } else {
        ReplayScenarioSource::explicit_path(scenario_path.to_path_buf())
    }
}

fn is_default_replay_scenario_path(scenario_path: &Path) -> bool {
    if scenario_path == Path::new(DEFAULT_REPLAY_SCENARIO_PATH) {
        return true;
    }

    let default_path = config::resolve_runtime_path(DEFAULT_REPLAY_SCENARIO_PATH);
    if scenario_path == default_path {
        return true;
    }

    match (scenario_path.canonicalize(), default_path.canonicalize()) {
        (Ok(scenario_path), Ok(default_path)) => scenario_path == default_path,
        _ => false,
    }
}

fn runtime_reason_catalog_from_path(
    reason_catalog_path: Option<&Path>,
) -> Result<ranking::ReasonCatalog> {
    let Some(reason_catalog_path) = reason_catalog_path else {
        return Ok(ranking::ReasonCatalog::default_core());
    };
    let profile_catalog = config::load_profile_reason_catalog(reason_catalog_path)?;
    ranking::ReasonCatalog::from_profile_catalog(&profile_catalog).with_context(|| {
        format!(
            "failed to merge profile reason catalog from {}",
            reason_catalog_path.display()
        )
    })
}

pub fn run_replay_scenarios_with_source(
    ranking_config_dir: impl AsRef<Path>,
    algorithm_version: &str,
    profile_id: Option<String>,
    scenario_source: ReplayScenarioSource,
) -> Result<ReplayScenarioSummary> {
    let reason_catalog =
        runtime_reason_catalog_from_path(scenario_source.reason_catalog_path.as_deref())?;
    let mut scenarios = load_replay_scenarios_with_catalog(&scenario_source.path, &reason_catalog)?;
    if let Some(pairwise_pack) = scenario_source.pairwise_pack.as_deref() {
        apply_replay_pairwise_pack(&mut scenarios, pairwise_pack)?;
    }
    let profiles = RankingProfiles::load_from_dir(ranking_config_dir)?;
    let engine = RankingEngine::new(profiles, algorithm_version.to_string())
        .with_reason_catalog(reason_catalog);
    let cases = scenarios
        .iter()
        .map(|(path, scenario)| evaluate_replay_scenario(path, scenario, &engine))
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
        None,
    );
    let warnings = count_checks(
        &cases,
        Some(QualitySeverity::Warning),
        Some(QualityCheckStatus::Failed),
        None,
    );
    let pairwise_total = count_checks(&cases, None, None, Some("pairwise."));
    let pairwise_passed = count_checks(
        &cases,
        None,
        Some(QualityCheckStatus::Passed),
        Some("pairwise."),
    );
    let explanation_integrity_total = count_checks(&cases, None, None, Some("explanation_"));
    let explanation_integrity_passed = count_checks(
        &cases,
        None,
        Some(QualityCheckStatus::Passed),
        Some("explanation_"),
    );

    Ok(ReplayScenarioSummary {
        profile_id,
        scenario_source,
        scenarios,
        passed,
        blocked,
        blockers,
        warnings,
        pairwise_passed,
        pairwise_total,
        explanation_integrity_passed,
        explanation_integrity_total,
        cases,
    })
}

pub(crate) fn load_replay_scenarios(
    path: impl AsRef<Path>,
) -> Result<Vec<(PathBuf, ReplayScenario)>> {
    load_replay_scenarios_with_catalog(path, &ranking::ReasonCatalog::default_core())
}

fn load_replay_scenarios_with_catalog(
    path: impl AsRef<Path>,
    reason_catalog: &ranking::ReasonCatalog,
) -> Result<Vec<(PathBuf, ReplayScenario)>> {
    let path = path.as_ref();
    let mut scenario_paths = Vec::new();
    if path.is_dir() {
        for entry in fs::read_dir(path)
            .with_context(|| format!("failed to read scenario directory {}", path.display()))?
        {
            let entry = entry.with_context(|| {
                format!("failed to read scenario directory entry {}", path.display())
            })?;
            let entry_path = entry.path();
            if is_yaml_path(&entry_path) {
                scenario_paths.push(entry_path);
            }
        }
        scenario_paths.sort();
    } else {
        ensure!(
            path.is_file(),
            "scenario path {} must be a YAML file or directory",
            path.display()
        );
        scenario_paths.push(path.to_path_buf());
    }

    ensure!(
        !scenario_paths.is_empty(),
        "scenario path {} did not contain any YAML scenarios",
        path.display()
    );

    let mut seen_ids = BTreeSet::new();
    let mut scenarios = Vec::new();
    for scenario_path in scenario_paths {
        let raw = fs::read_to_string(&scenario_path)
            .with_context(|| format!("failed to read scenario {}", scenario_path.display()))?;
        let scenario = serde_yaml::from_str::<ReplayScenario>(&raw)
            .with_context(|| format!("failed to parse scenario {}", scenario_path.display()))?;
        validate_replay_scenario_with_catalog(&scenario_path, &scenario, reason_catalog)?;
        ensure!(
            seen_ids.insert(scenario.id.clone()),
            "duplicate replay scenario id {} in {}",
            scenario.id,
            scenario_path.display()
        );
        scenarios.push((scenario_path, scenario));
    }

    Ok(scenarios)
}

fn apply_replay_pairwise_pack(
    scenarios: &mut [(PathBuf, ReplayScenario)],
    path: &Path,
) -> Result<()> {
    let expectations = load_replay_pairwise_expectations(path)?;
    let mut scenario_index_by_id = BTreeMap::new();
    for (index, (_, scenario)) in scenarios.iter().enumerate() {
        scenario_index_by_id.insert(scenario.id.clone(), index);
    }

    for expectation in expectations {
        let index = scenario_index_by_id
            .get(&expectation.scenario_id)
            .with_context(|| {
                format!(
                    "pairwise pack {} references unknown scenario_id {}",
                    path.display(),
                    expectation.scenario_id
                )
            })?;
        scenarios[*index]
            .1
            .expectations
            .pairwise
            .extend(expectation.pairwise);
    }

    Ok(())
}

fn load_replay_pairwise_expectations(
    path: &Path,
) -> Result<Vec<ReplayPairwiseScenarioExpectation>> {
    let mut pack_paths = Vec::new();
    if path.is_dir() {
        for entry in fs::read_dir(path)
            .with_context(|| format!("failed to read pairwise pack directory {}", path.display()))?
        {
            let entry = entry.with_context(|| {
                format!(
                    "failed to read pairwise pack directory entry {}",
                    path.display()
                )
            })?;
            let entry_path = entry.path();
            if is_yaml_path(&entry_path) {
                pack_paths.push(entry_path);
            }
        }
        pack_paths.sort();
    } else {
        ensure!(
            path.is_file(),
            "pairwise pack path {} must be a YAML file or directory",
            path.display()
        );
        pack_paths.push(path.to_path_buf());
    }

    ensure!(
        !pack_paths.is_empty(),
        "pairwise pack path {} did not contain any YAML packs",
        path.display()
    );

    let mut expectations = Vec::new();
    for pack_path in pack_paths {
        let raw = fs::read_to_string(&pack_path)
            .with_context(|| format!("failed to read pairwise pack {}", pack_path.display()))?;
        let pack = serde_yaml::from_str::<ReplayPairwisePack>(&raw)
            .with_context(|| format!("failed to parse pairwise pack {}", pack_path.display()))?;
        validate_replay_pairwise_pack(&pack_path, &pack)?;
        expectations.extend(pack.expectations);
    }

    Ok(expectations)
}

fn validate_replay_pairwise_pack(path: &Path, pack: &ReplayPairwisePack) -> Result<()> {
    ensure!(
        pack.schema_version == REPLAY_PAIRWISE_PACK_SCHEMA_VERSION,
        "pairwise pack {} has schema_version={}, expected {}",
        path.display(),
        pack.schema_version,
        REPLAY_PAIRWISE_PACK_SCHEMA_VERSION
    );
    ensure!(
        !pack.expectations.is_empty(),
        "pairwise pack {} must declare expectations",
        path.display()
    );

    for expectation in &pack.expectations {
        ensure!(
            !expectation.scenario_id.trim().is_empty(),
            "pairwise pack {} must not contain an empty scenario_id",
            path.display()
        );
        ensure!(
            !expectation.pairwise.is_empty(),
            "pairwise pack {} scenario_id {} must declare pairwise expectations",
            path.display(),
            expectation.scenario_id
        );
        for pairwise in &expectation.pairwise {
            validate_pairwise_expectation(&expectation.scenario_id, pairwise)?;
        }
    }

    Ok(())
}

fn is_yaml_path(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("yaml" | "yml")
    )
}

#[cfg(test)]
fn validate_replay_scenario(path: &Path, scenario: &ReplayScenario) -> Result<()> {
    validate_replay_scenario_with_catalog(path, scenario, &ranking::ReasonCatalog::default_core())
}

fn validate_replay_scenario_with_catalog(
    path: &Path,
    scenario: &ReplayScenario,
    reason_catalog: &ranking::ReasonCatalog,
) -> Result<()> {
    ensure!(
        scenario.schema_version == REPLAY_SCENARIO_SCHEMA_VERSION,
        "scenario {} has schema_version={}, expected {}",
        path.display(),
        scenario.schema_version,
        REPLAY_SCENARIO_SCHEMA_VERSION
    );
    ensure!(
        !scenario.id.trim().is_empty(),
        "scenario {} must have a non-empty id",
        path.display()
    );
    ensure!(
        !scenario.title.trim().is_empty(),
        "scenario {} must have a non-empty title",
        path.display()
    );
    ensure!(
        !scenario.expectations.ordered.is_empty(),
        "scenario {} must declare expectations.ordered",
        scenario.id
    );
    validate_unique_item_keys(
        &scenario.id,
        "expectations.ordered",
        &scenario.expectations.ordered,
    )?;
    validate_unique_item_keys(
        &scenario.id,
        "expectations.absent_from_result",
        &scenario.expectations.absent_from_result,
    )?;

    for pairwise in &scenario.expectations.pairwise {
        validate_pairwise_expectation(&scenario.id, pairwise)?;
    }

    for stage in scenario.expectations.candidate_counts.keys() {
        ensure!(
            is_known_fallback_stage(stage),
            "scenario {} candidate_counts has unknown fallback stage {}",
            scenario.id,
            stage
        );
    }

    for (item_key, reason_codes) in &scenario.expectations.required_reason_codes {
        ensure!(
            !item_key.trim().is_empty(),
            "scenario {} required_reason_codes keys must be non-empty",
            scenario.id
        );
        validate_item_key(&scenario.id, "expectations.required_reason_codes", item_key)?;
        ensure!(
            !reason_codes.is_empty(),
            "scenario {} required_reason_codes for {} must not be empty",
            scenario.id,
            item_key
        );
        for reason_code in reason_codes {
            ensure!(
                reason_catalog.contains_reason_code(reason_code),
                "scenario {} expects unknown reason_code {} for {}",
                scenario.id,
                reason_code,
                item_key
            );
        }
    }
    validate_unique_content_kinds(
        &scenario.id,
        "expectations.absent_content_kinds",
        &scenario.expectations.absent_content_kinds,
    )?;
    if let Some(max_items_per_school) = scenario.expectations.max_items_per_school {
        ensure!(
            max_items_per_school > 0,
            "scenario {} max_items_per_school must be greater than zero",
            scenario.id
        );
    }
    if let Some(max_items_per_group) = scenario.expectations.max_items_per_group {
        ensure!(
            max_items_per_group > 0,
            "scenario {} max_items_per_group must be greater than zero",
            scenario.id
        );
    }

    Ok(())
}

fn validate_pairwise_expectation(scenario_id: &str, pairwise: &PairwiseExpectation) -> Result<()> {
    ensure!(
        !pairwise.higher.trim().is_empty() && !pairwise.lower.trim().is_empty(),
        "scenario {} pairwise expectations must name both higher and lower items",
        scenario_id
    );
    ensure!(
        pairwise.higher != pairwise.lower,
        "scenario {} pairwise expectation cannot compare {} to itself",
        scenario_id,
        pairwise.higher
    );
    validate_item_key(
        scenario_id,
        "expectations.pairwise.higher",
        &pairwise.higher,
    )?;
    validate_item_key(scenario_id, "expectations.pairwise.lower", &pairwise.lower)?;
    Ok(())
}

fn validate_unique_item_keys(scenario_id: &str, field: &str, item_keys: &[String]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for item_key in item_keys {
        validate_item_key(scenario_id, field, item_key)?;
        ensure!(
            seen.insert(item_key.as_str()),
            "scenario {} {} contains duplicate item key {}",
            scenario_id,
            field,
            item_key
        );
    }
    Ok(())
}

fn validate_unique_content_kinds(
    scenario_id: &str,
    field: &str,
    content_kinds: &[ContentKind],
) -> Result<()> {
    let mut seen = BTreeSet::new();
    for content_kind in content_kinds {
        ensure!(
            seen.insert(*content_kind),
            "scenario {} {} contains duplicate content kind {}",
            scenario_id,
            field,
            content_kind.as_str()
        );
    }
    Ok(())
}

fn validate_item_key(scenario_id: &str, field: &str, item_key: &str) -> Result<()> {
    let Some((content_kind, content_id)) = item_key.split_once(':') else {
        anyhow::bail!(
            "scenario {} {} item key {} must use <content_kind>:<content_id>",
            scenario_id,
            field,
            item_key
        );
    };
    ensure!(
        matches!(content_kind, "school" | "event" | "article"),
        "scenario {} {} item key {} has unknown content_kind {}",
        scenario_id,
        field,
        item_key,
        content_kind
    );
    ensure!(
        !content_id.trim().is_empty(),
        "scenario {} {} item key {} must include a content id",
        scenario_id,
        field,
        item_key
    );
    Ok(())
}

fn is_known_fallback_stage(stage: &str) -> bool {
    serde_yaml::from_value::<FallbackStage>(serde_yaml::Value::String(stage.to_string())).is_ok()
}

fn evaluate_replay_scenario(
    path: &Path,
    scenario: &ReplayScenario,
    engine: &RankingEngine,
) -> ReplayScenarioCase {
    let mut case = ReplayScenarioCase {
        id: scenario.id.clone(),
        title: scenario.title.clone(),
        path: path.to_path_buf(),
        status: ReplayScenarioStatus::Passed,
        expected_fallback_stage: scenario.expectations.fallback_stage.as_str().to_string(),
        actual_fallback_stage: None,
        expected_order: scenario.expectations.ordered.clone(),
        actual_order: Vec::new(),
        checks: Vec::new(),
    };

    let result = match engine.recommend(&scenario.dataset, &scenario.query) {
        Ok(result) => result,
        Err(error) => {
            push_check(
                &mut case,
                "ranking.run",
                QualitySeverity::Blocker,
                false,
                format!("ranking scenario failed before producing output: {error}"),
            );
            return finalize_scenario_case(case);
        }
    };

    case.actual_fallback_stage = Some(result.fallback_stage.as_str().to_string());
    case.actual_order = result.items.iter().map(item_key).collect();

    check_expected_fallback_stage(&mut case, scenario, &result);
    check_expected_order(&mut case, scenario);
    check_pairwise_expectations(&mut case, scenario);
    check_absent_items(&mut case, scenario);
    check_absent_content_kinds(&mut case, scenario, &result);
    check_candidate_counts(&mut case, scenario, &result);
    check_max_items_per_school(&mut case, scenario, &result);
    check_max_items_per_group(&mut case, scenario, &result);
    check_required_reason_codes(&mut case, scenario, &result);
    check_explanation_integrity(&mut case, &result, engine.reason_catalog());

    finalize_scenario_case(case)
}

fn check_expected_fallback_stage(
    case: &mut ReplayScenarioCase,
    scenario: &ReplayScenario,
    result: &RecommendationResult,
) {
    let expected = scenario.expectations.fallback_stage.as_str();
    let actual = result.fallback_stage.as_str();
    push_check(
        case,
        "fallback_stage",
        QualitySeverity::Blocker,
        expected == actual,
        format!("expected {expected}, actual {actual}"),
    );
}

fn check_expected_order(case: &mut ReplayScenarioCase, scenario: &ReplayScenario) {
    let expected = &scenario.expectations.ordered;
    let actual_prefix = case
        .actual_order
        .iter()
        .take(expected.len())
        .cloned()
        .collect::<Vec<_>>();
    push_check(
        case,
        "ordered_prefix",
        QualitySeverity::Blocker,
        *expected == actual_prefix,
        format!(
            "expected prefix {}, actual prefix {}, full actual {}",
            format_order(expected),
            format_order(&actual_prefix),
            format_order(&case.actual_order)
        ),
    );
}

fn check_pairwise_expectations(case: &mut ReplayScenarioCase, scenario: &ReplayScenario) {
    for (index, expectation) in scenario.expectations.pairwise.iter().enumerate() {
        let higher_index = case
            .actual_order
            .iter()
            .position(|item| item == &expectation.higher);
        let lower_index = case
            .actual_order
            .iter()
            .position(|item| item == &expectation.lower);
        let passed = higher_index
            .zip(lower_index)
            .is_some_and(|(higher_index, lower_index)| higher_index < lower_index);
        push_check(
            case,
            &format!("pairwise.{}", index + 1),
            QualitySeverity::Blocker,
            passed,
            format!(
                "expected {} above {}, actual order {}{}",
                expectation.higher,
                expectation.lower,
                format_order(&case.actual_order),
                expectation
                    .note
                    .as_ref()
                    .map(|note| format!(" note={note}"))
                    .unwrap_or_default()
            ),
        );
    }
}

fn check_absent_items(case: &mut ReplayScenarioCase, scenario: &ReplayScenario) {
    for item in &scenario.expectations.absent_from_result {
        push_check(
            case,
            &format!("absent_from_result.{item}"),
            QualitySeverity::Blocker,
            !case.actual_order.contains(item),
            format!(
                "expected {item} to be absent, actual order {}",
                format_order(&case.actual_order)
            ),
        );
    }
}

fn check_absent_content_kinds(
    case: &mut ReplayScenarioCase,
    scenario: &ReplayScenario,
    result: &RecommendationResult,
) {
    for content_kind in &scenario.expectations.absent_content_kinds {
        let actual_items = result
            .items
            .iter()
            .filter(|item| item.content_kind == *content_kind)
            .map(item_key)
            .collect::<Vec<_>>();
        push_check(
            case,
            &format!("absent_content_kind.{}", content_kind.as_str()),
            QualitySeverity::Blocker,
            actual_items.is_empty(),
            format!(
                "expected no {} items, actual {}",
                content_kind.as_str(),
                format_order(&actual_items)
            ),
        );
    }
}

fn check_candidate_counts(
    case: &mut ReplayScenarioCase,
    scenario: &ReplayScenario,
    result: &RecommendationResult,
) {
    for (stage, expected) in &scenario.expectations.candidate_counts {
        let actual = result.candidate_counts.get(stage).copied();
        push_check(
            case,
            &format!("candidate_count.{stage}"),
            QualitySeverity::Blocker,
            actual == Some(*expected),
            format!(
                "expected {stage} count {expected}, actual {}",
                actual
                    .map(|count| count.to_string())
                    .unwrap_or_else(|| "missing".to_string())
            ),
        );
    }
}

fn check_max_items_per_school(
    case: &mut ReplayScenarioCase,
    scenario: &ReplayScenario,
    result: &RecommendationResult,
) {
    let Some(max_items_per_school) = scenario.expectations.max_items_per_school else {
        return;
    };
    let mut counts = BTreeMap::<String, usize>::new();
    for item in &result.items {
        *counts.entry(item.school_id.clone()).or_default() += 1;
    }
    let violations = counts
        .iter()
        .filter(|(_, count)| **count > max_items_per_school)
        .map(|(school_id, count)| format!("{school_id}={count}"))
        .collect::<Vec<_>>();
    push_check(
        case,
        "max_items_per_school",
        QualitySeverity::Blocker,
        violations.is_empty(),
        format!(
            "expected at most {max_items_per_school} items per school, violations {}",
            format_order(&violations)
        ),
    );
}

fn check_max_items_per_group(
    case: &mut ReplayScenarioCase,
    scenario: &ReplayScenario,
    result: &RecommendationResult,
) {
    let Some(max_items_per_group) = scenario.expectations.max_items_per_group else {
        return;
    };
    let group_by_school = scenario
        .dataset
        .schools
        .iter()
        .map(|school| (school.id.as_str(), school.group_id.as_str()))
        .collect::<BTreeMap<_, _>>();
    let mut counts = BTreeMap::<String, usize>::new();
    let mut missing_schools = BTreeSet::<String>::new();
    for item in &result.items {
        if let Some(group_id) = group_by_school.get(item.school_id.as_str()) {
            *counts.entry((*group_id).to_string()).or_default() += 1;
        } else {
            missing_schools.insert(item.school_id.clone());
        }
    }
    let mut violations = counts
        .iter()
        .filter(|(_, count)| **count > max_items_per_group)
        .map(|(group_id, count)| format!("{group_id}={count}"))
        .collect::<Vec<_>>();
    violations.extend(
        missing_schools
            .into_iter()
            .map(|school_id| format!("{school_id}=missing_school")),
    );
    push_check(
        case,
        "max_items_per_group",
        QualitySeverity::Blocker,
        violations.is_empty(),
        format!(
            "expected at most {max_items_per_group} items per group, violations {}",
            format_order(&violations)
        ),
    );
}

fn check_required_reason_codes(
    case: &mut ReplayScenarioCase,
    scenario: &ReplayScenario,
    result: &RecommendationResult,
) {
    for (expected_item_key, expected_reason_codes) in &scenario.expectations.required_reason_codes {
        let Some(item) = result
            .items
            .iter()
            .find(|item| item_key(item) == expected_item_key.as_str())
        else {
            push_check(
                case,
                &format!("required_reason_codes.{expected_item_key}"),
                QualitySeverity::Blocker,
                false,
                format!(
                    "expected item {} was not present in actual order {}",
                    expected_item_key,
                    format_order(&case.actual_order)
                ),
            );
            continue;
        };

        let actual_reason_codes = item
            .score_breakdown
            .iter()
            .map(|component| component.reason_code.as_str())
            .collect::<BTreeSet<_>>();
        let missing = expected_reason_codes
            .iter()
            .filter(|reason_code| !actual_reason_codes.contains(reason_code.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        push_check(
            case,
            &format!("required_reason_codes.{expected_item_key}"),
            QualitySeverity::Blocker,
            missing.is_empty(),
            format!(
                "expected reason codes {}, actual reason codes {}, missing {}",
                expected_reason_codes.join(","),
                actual_reason_codes
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
                    .join(","),
                format_order(&missing)
            ),
        );
    }
}

fn check_explanation_integrity(
    case: &mut ReplayScenarioCase,
    result: &RecommendationResult,
    reason_catalog: &ranking::ReasonCatalog,
) {
    for check in check_recommendation_result_integrity_with_catalog(result, reason_catalog) {
        case.checks.push(ReplayScenarioCheck {
            name: check.name,
            severity: check.severity,
            status: check.status,
            message: check.message,
        });
    }
}

fn push_check(
    case: &mut ReplayScenarioCase,
    name: &str,
    severity: QualitySeverity,
    passed: bool,
    message: String,
) {
    case.checks.push(ReplayScenarioCheck {
        name: name.to_string(),
        severity,
        status: if passed {
            QualityCheckStatus::Passed
        } else {
            QualityCheckStatus::Failed
        },
        message,
    });
}

fn finalize_scenario_case(mut case: ReplayScenarioCase) -> ReplayScenarioCase {
    case.status = if case.checks.iter().any(|check| {
        check.severity == QualitySeverity::Blocker && check.status == QualityCheckStatus::Failed
    }) {
        ReplayScenarioStatus::Blocked
    } else {
        ReplayScenarioStatus::Passed
    };
    case
}

fn count_checks(
    cases: &[ReplayScenarioCase],
    severity: Option<QualitySeverity>,
    status: Option<QualityCheckStatus>,
    name_prefix: Option<&str>,
) -> usize {
    cases
        .iter()
        .flat_map(|case| case.checks.iter())
        .filter(|check| severity.is_none_or(|severity| check.severity == severity))
        .filter(|check| status.is_none_or(|status| check.status == status))
        .filter(|check| name_prefix.is_none_or(|prefix| check.name.starts_with(prefix)))
        .count()
}

fn item_key(item: &RecommendationItem) -> String {
    format!("{}:{}", item.content_kind.as_str(), item.content_id)
}

fn format_order(order: &[String]) -> String {
    if order.is_empty() {
        "-".to_string()
    } else {
        order.join(",")
    }
}

#[cfg(feature = "storage-backends")]
pub async fn run_replay_evaluate(
    settings: &AppSettings,
    limit: i64,
) -> Result<ReplayEvaluationSummary> {
    let profiles = RankingProfiles::load_from_dir(&settings.ranking_config_dir)?;
    let neighbor_distance_cap_meters = profiles.fallback.neighbor_distance_cap_meters;
    let mut engine = RankingEngine::new(profiles, settings.algorithm_version.clone());
    if !settings.profile_reason_catalog_path.is_empty() {
        let profile_catalog =
            config::load_profile_reason_catalog(&settings.profile_reason_catalog_path)?;
        engine = engine
            .with_profile_reason_catalog(&profile_catalog)
            .with_context(|| {
                format!(
                    "failed to merge profile reason catalog from {}",
                    settings.profile_reason_catalog_path
                )
            })?;
    }
    let repository = pg_repository(settings)?;
    let traces = repository
        .list_recommendation_traces_for_replay(limit)
        .await?;
    let mut cases = Vec::new();

    for trace in traces {
        cases.push(
            evaluate_replay_trace(
                &repository,
                &engine,
                &trace,
                settings.candidate_retrieval_limit,
                neighbor_distance_cap_meters,
            )
            .await,
        );
    }

    let matched = cases
        .iter()
        .filter(|case| case.status == ReplayEvaluationStatus::Matched)
        .count();
    let mismatched = cases
        .iter()
        .filter(|case| case.status == ReplayEvaluationStatus::Mismatched)
        .count();
    let failed = cases
        .iter()
        .filter(|case| case.status == ReplayEvaluationStatus::Failed)
        .count();

    Ok(ReplayEvaluationSummary {
        evaluated: cases.len(),
        matched,
        mismatched,
        failed,
        cases,
    })
}

#[cfg(feature = "storage-backends")]
async fn evaluate_replay_trace(
    repository: &PgRepository,
    engine: &RankingEngine,
    trace: &RecommendationTraceReplayRow,
    candidate_limit: usize,
    neighbor_distance_cap_meters: f64,
) -> ReplayEvaluationCase {
    let expected_order = match stored_response_order(&trace.response_payload) {
        Ok(order) => order,
        Err(error) => {
            return failed_replay_case(
                trace,
                None,
                Some(normalize_fallback_stage(&trace.fallback_stage)),
                format!("failed to read stored response item order: {error}"),
            );
        }
    };
    let expected_fallback_stage = stored_response_fallback_stage(&trace.response_payload)
        .unwrap_or_else(|| normalize_fallback_stage(&trace.fallback_stage));
    let request =
        match serde_json::from_value::<RecommendationRequest>(trace.request_payload.clone()) {
            Ok(request) => request,
            Err(error) => {
                return failed_replay_case(
                    trace,
                    None,
                    Some(expected_fallback_stage),
                    format!("failed to parse stored request_payload: {error}"),
                );
            }
        };
    let request_id = request
        .request_id
        .clone()
        .unwrap_or_else(|| format!("replay-trace-{}", trace.id));
    let context_input = request.context_input();
    let resolved_context = match repository
        .resolve_context_for_replay(&request_id, request.user_id.as_deref(), &context_input)
        .await
    {
        Ok(context) => context,
        Err(error) => {
            return failed_replay_case(
                trace,
                Some(request_id),
                Some(expected_fallback_stage),
                format!("failed to resolve replay context: {error}"),
            );
        }
    };
    let target_station = match repository.load_station_for_context(&resolved_context).await {
        Ok(Some(station)) => station,
        Ok(None) => {
            return failed_replay_case(
                trace,
                Some(request_id),
                Some(expected_fallback_stage),
                "resolved context did not map to a station".to_string(),
            );
        }
        Err(error) => {
            return failed_replay_case(
                trace,
                Some(request_id),
                Some(expected_fallback_stage),
                format!("failed to load replay station: {error}"),
            );
        }
    };
    let query = request.with_resolved_context(target_station.id.clone(), resolved_context);
    let neighbor_max_hops = engine.neighbor_max_hops(query.placement);
    let min_candidate_count = engine.minimum_candidate_count();
    let storage_graph_expansion = match repository
        .load_candidate_plan_graph_expansion(
            &target_station,
            query.context.as_ref().expect("resolved context is set"),
        )
        .await
    {
        Ok(expansion) => expansion,
        Err(error) => {
            return failed_replay_case(
                trace,
                Some(request_id),
                Some(expected_fallback_stage),
                format!("failed to load replay graph expansion: {error}"),
            );
        }
    };
    let candidate_link_query = ContextCandidateLinkQuery {
        target_station: &target_station,
        context: query.context.as_ref().expect("resolved context is set"),
        candidate_limit,
        min_scoped_candidates: min_candidate_count,
        neighbor_distance_cap_meters,
        neighbor_max_hops,
    };
    let candidate_links = match repository
        .load_context_candidate_links_with_loaded_graph_expansion(
            candidate_link_query,
            &storage_graph_expansion,
        )
        .await
    {
        Ok(candidate_links) => candidate_links,
        Err(error) => {
            return failed_replay_case(
                trace,
                Some(request_id),
                Some(expected_fallback_stage),
                format!("failed to load replay candidates: {error}"),
            );
        }
    };
    let dataset = match repository
        .load_candidate_dataset(&query, &target_station, &candidate_links)
        .await
    {
        Ok(dataset) => dataset,
        Err(error) => {
            return failed_replay_case(
                trace,
                Some(request_id),
                Some(expected_fallback_stage),
                format!("failed to load replay dataset: {error}"),
            );
        }
    };
    let graph_expansion = candidate_graph_expansion_from_storage(storage_graph_expansion);
    let actual = match engine.recommend_with_graph_expansion(&dataset, &query, &graph_expansion) {
        Ok(result) => RecommendationResponse::from(result),
        Err(error) => {
            return failed_replay_case(
                trace,
                Some(request_id),
                Some(expected_fallback_stage),
                format!("ranking replay failed: {error}"),
            );
        }
    };

    let actual_order = response_order(&actual);
    let actual_fallback_stage = fallback_stage_label(&actual.fallback_stage);
    let status =
        if expected_order == actual_order && expected_fallback_stage == actual_fallback_stage {
            ReplayEvaluationStatus::Matched
        } else {
            ReplayEvaluationStatus::Mismatched
        };

    ReplayEvaluationCase {
        trace_id: trace.id,
        status,
        request_id: Some(request_id),
        expected_fallback_stage: Some(expected_fallback_stage),
        actual_fallback_stage: Some(actual_fallback_stage),
        expected_order,
        actual_order,
        message: (status == ReplayEvaluationStatus::Mismatched)
            .then_some("stored response differs from current deterministic replay".to_string()),
    }
}

#[cfg(feature = "storage-backends")]
fn candidate_graph_expansion_from_storage(
    expansion: storage::CandidatePlanGraphExpansion,
) -> CandidateGraphExpansion {
    let line = expansion
        .line
        .and_then(|line| LineGraphExpansion::new(line.origin_line_id, line.adjacent_line_ids));
    let area = expansion
        .area
        .and_then(|area| AreaGraphExpansion::new(area.origin_area_id, area.adjacent_area_ids));
    CandidateGraphExpansion::from_parts(line, area)
}

#[cfg(feature = "storage-backends")]
fn failed_replay_case(
    trace: &RecommendationTraceReplayRow,
    request_id: Option<String>,
    expected_fallback_stage: Option<String>,
    message: String,
) -> ReplayEvaluationCase {
    ReplayEvaluationCase {
        trace_id: trace.id,
        status: ReplayEvaluationStatus::Failed,
        request_id,
        expected_fallback_stage,
        actual_fallback_stage: None,
        expected_order: Vec::new(),
        actual_order: Vec::new(),
        message: Some(message),
    }
}

#[cfg(feature = "storage-backends")]
fn response_order(response: &RecommendationResponse) -> Vec<String> {
    response
        .items
        .iter()
        .map(|item| format!("{}:{}", item.content_kind.as_str(), item.content_id))
        .collect()
}

#[cfg(feature = "storage-backends")]
fn stored_response_order(response: &serde_json::Value) -> Result<Vec<String>> {
    let items = response
        .get("items")
        .and_then(serde_json::Value::as_array)
        .with_context(|| "response_payload.items must be an array")?;
    items
        .iter()
        .map(|item| {
            let content_kind = match item.get("content_kind") {
                None => "school",
                Some(value) => value
                    .as_str()
                    .with_context(|| "response item content_kind must be a string")?,
            };
            let content_id = item
                .get("content_id")
                .and_then(serde_json::Value::as_str)
                .or_else(|| item.get("school_id").and_then(serde_json::Value::as_str))
                .with_context(|| "response item content_id must be a string")?;
            Ok(format!("{content_kind}:{content_id}"))
        })
        .collect()
}

#[cfg(feature = "storage-backends")]
fn stored_response_fallback_stage(response: &serde_json::Value) -> Option<String> {
    response
        .get("fallback_stage")
        .and_then(serde_json::Value::as_str)
        .map(normalize_fallback_stage)
}

#[cfg(feature = "storage-backends")]
fn normalize_fallback_stage(stage: &str) -> String {
    match stage {
        "strict" => "strict_station",
        other => other,
    }
    .to_string()
}

#[cfg(feature = "storage-backends")]
fn fallback_stage_label(fallback_stage: &FallbackStageDto) -> String {
    fallback_stage.as_str().to_string()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        path::{Path, PathBuf},
    };

    use super::{
        check_max_items_per_school, run_replay_scenarios, run_replay_scenarios_with_source,
        validate_replay_scenario, validate_replay_scenario_with_catalog, ReplayScenario,
        ReplayScenarioCase, ReplayScenarioSource, ReplayScenarioSourceKind, ReplayScenarioStatus,
        DEFAULT_REPLAY_SCENARIO_PATH,
    };
    #[cfg(feature = "storage-backends")]
    use super::{normalize_fallback_stage, stored_response_order};
    use config::{
        ProfileReason, ProfileReasonCatalog, ProfileReasonCatalogKind, ProfileReasonLayer,
    };
    use domain::{ContentKind, FallbackStage, RecommendationItem, RecommendationResult};

    #[cfg(feature = "storage-backends")]
    #[test]
    fn replay_reader_accepts_legacy_school_only_trace_shape() {
        let payload = serde_json::json!({
            "items": [
                { "school_id": "school_seaside" },
                { "content_kind": "event", "content_id": "event_open" }
            ],
            "fallback_stage": "strict"
        });

        let order = stored_response_order(&payload).expect("legacy order");

        assert_eq!(order, vec!["school:school_seaside", "event:event_open"]);
        assert_eq!(normalize_fallback_stage("strict"), "strict_station");
    }

    #[cfg(feature = "storage-backends")]
    #[test]
    fn replay_reader_rejects_non_string_content_kind() {
        let payload = serde_json::json!({
            "items": [
                { "content_kind": 7, "content_id": "event_open" }
            ]
        });

        let error = stored_response_order(&payload).expect_err("invalid content kind");

        assert!(error
            .to_string()
            .contains("response item content_kind must be a string"));
    }

    #[test]
    fn committed_replay_scenarios_pass_quality_gate() {
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let summary = run_replay_scenarios(
            repo_root.join(DEFAULT_REPLAY_SCENARIO_PATH),
            repo_root.join("configs/ranking"),
            "replay-scenario-test",
        )
        .expect("committed replay scenarios");

        let scenario_ids = summary
            .cases
            .iter()
            .map(|case| case.id.as_str())
            .collect::<BTreeSet<_>>();
        for expected_id in [
            "S01_HOKKAIDO_GUARDRAIL",
            "S02_LINE_INTENT",
            "S03_CITY_FALLBACK",
            "S04_COLD_START",
            "S05_SAME_SCHOOL_CAP",
            "S06_SAME_GROUP_CAP",
            "S07_FRESHNESS_BOUNDED",
            "S08_EXPLANATION_INTEGRITY",
            "S09_ARTICLE_RESERVED_SLOT",
            "S10_SQL_ONLY_FULL_MODE_PARITY",
        ] {
            assert!(scenario_ids.contains(expected_id));
        }
        assert!(summary.scenarios >= 10);
        assert_eq!(summary.blockers, 0);
        assert!(summary.pairwise_total >= 7);
        assert!(summary.explanation_integrity_total >= summary.scenarios * 6);
        assert_eq!(
            summary.scenario_source.kind,
            ReplayScenarioSourceKind::DefaultPath
        );
    }

    #[test]
    fn replay_scenario_validation_rejects_unknown_fallback_count_keys() {
        let yaml = minimal_scenario_yaml(
            r#"
    typo_stage: 0
"#,
        );
        let scenario: ReplayScenario = serde_yaml::from_str(&yaml).expect("scenario yaml");

        let error = validate_replay_scenario(Path::new("scenario.yaml"), &scenario)
            .expect_err("unknown fallback stage");

        assert!(error
            .to_string()
            .contains("candidate_counts has unknown fallback stage typo_stage"));
    }

    #[test]
    fn replay_scenario_validation_rejects_malformed_item_keys() {
        let yaml = minimal_scenario_yaml("{}");
        let scenario: ReplayScenario = serde_yaml::from_str(&yaml).expect("scenario yaml");
        let mut scenario = scenario;
        scenario.expectations.ordered = vec!["school".to_string()];

        let error = validate_replay_scenario(Path::new("scenario.yaml"), &scenario)
            .expect_err("malformed item key");

        assert!(error
            .to_string()
            .contains("must use <content_kind>:<content_id>"));
    }

    #[test]
    fn replay_scenario_validation_accepts_profile_reason_codes() {
        let yaml = minimal_scenario_yaml("{}");
        let mut scenario: ReplayScenario = serde_yaml::from_str(&yaml).expect("scenario yaml");
        scenario.expectations.required_reason_codes.insert(
            "school:school_test".to_string(),
            vec!["profile.custom".to_string()],
        );
        let profile_catalog = ProfileReasonCatalog {
            schema_version: config::PROFILE_REASON_CATALOG_SCHEMA_VERSION,
            kind: ProfileReasonCatalogKind::ProfileReasonCatalog,
            profile_id: "local-discovery-generic".to_string(),
            reasons: vec![ProfileReason {
                feature: "custom_profile_bonus".to_string(),
                reason_code: "profile.custom".to_string(),
                label: "Profile custom".to_string(),
                layer: ProfileReasonLayer::Profile,
            }],
        };
        let reason_catalog = ranking::ReasonCatalog::from_profile_catalog(&profile_catalog)
            .expect("profile reason catalog");

        let error = validate_replay_scenario(Path::new("scenario.yaml"), &scenario)
            .expect_err("core catalog should reject profile reason code");
        assert!(error
            .to_string()
            .contains("expects unknown reason_code profile.custom"));

        validate_replay_scenario_with_catalog(
            Path::new("scenario.yaml"),
            &scenario,
            &reason_catalog,
        )
        .expect("profile reason code should be accepted");
    }

    #[test]
    fn replay_scenario_validation_rejects_duplicate_absent_content_kinds() {
        let yaml = minimal_scenario_yaml("{}");
        let mut scenario: ReplayScenario = serde_yaml::from_str(&yaml).expect("scenario yaml");
        scenario.expectations.absent_content_kinds =
            vec![ContentKind::Article, ContentKind::Article];

        let error = validate_replay_scenario(Path::new("scenario.yaml"), &scenario)
            .expect_err("duplicate absent content kind");

        assert!(error
            .to_string()
            .contains("absent_content_kinds contains duplicate content kind article"));
    }

    #[test]
    fn replay_scenario_validation_rejects_zero_diversity_caps() {
        let yaml = minimal_scenario_yaml("{}");
        let mut scenario: ReplayScenario = serde_yaml::from_str(&yaml).expect("scenario yaml");
        scenario.expectations.max_items_per_school = Some(0);

        let error = validate_replay_scenario(Path::new("scenario.yaml"), &scenario)
            .expect_err("zero school cap");

        assert!(error
            .to_string()
            .contains("max_items_per_school must be greater than zero"));

        scenario.expectations.max_items_per_school = None;
        scenario.expectations.max_items_per_group = Some(0);

        let error = validate_replay_scenario(Path::new("scenario.yaml"), &scenario)
            .expect_err("zero group cap");

        assert!(error
            .to_string()
            .contains("max_items_per_group must be greater than zero"));
    }

    #[test]
    fn replay_scenario_blocks_absent_content_kind_violations() {
        let temp = tempfile::tempdir().expect("tempdir");
        let scenario_path = temp.path().join("absent_school.yaml");
        std::fs::write(
            &scenario_path,
            replay_scenario_yaml(
                "S00_ABSENT_SCHOOL_KIND",
                r#"
  schools:
    - id: school_a
      name: School A
      area: Minato
      prefecture_name: Tokyo
      school_type: high_school
      group_id: group_a
  events: []
  stations:
    - id: st_target
      name: Target
      line_name: Test Line
      latitude: 35.6500
      longitude: 139.7500
  school_station_links:
    - school_id: school_a
      station_id: st_target
      walking_minutes: 5
      distance_meters: 400
      hop_distance: 0
      line_name: Test Line
  popularity_snapshots: []
  user_affinity_snapshots: []
  area_affinity_snapshots: []
"#,
                r#"
  fallback_stage: strict_station
  ordered:
    - "school:school_a"
  absent_content_kinds:
    - school
"#,
            ),
        )
        .expect("write scenario");
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");

        let summary = run_replay_scenarios(
            &scenario_path,
            repo_root.join("configs/ranking"),
            "replay-scenario-test",
        )
        .expect("scenario summary");

        assert_eq!(summary.blockers, 1);
        assert_eq!(summary.cases[0].status.as_str(), "blocked");
        assert!(summary.cases[0].checks.iter().any(|check| {
            check.name == "absent_content_kind.school" && check.status.as_str() == "failed"
        }));
    }

    #[test]
    fn replay_scenario_applies_profile_pairwise_pack() {
        let temp = tempfile::tempdir().expect("tempdir");
        let scenario_path = temp.path().join("pairwise_pack_scenario.yaml");
        let pairwise_pack_path = temp.path().join("pairwise_pack.yaml");
        std::fs::write(
            &scenario_path,
            replay_scenario_yaml(
                "S00_PAIRWISE_PACK",
                r#"
  schools:
    - id: school_a
      name: School A
      area: Minato
      prefecture_name: Tokyo
      school_type: high_school
      group_id: group_a
    - id: school_b
      name: School B
      area: Minato
      prefecture_name: Tokyo
      school_type: high_school
      group_id: group_b
  events: []
  stations:
    - id: st_target
      name: Target
      line_name: Test Line
      latitude: 35.6500
      longitude: 139.7500
  school_station_links:
    - school_id: school_a
      station_id: st_target
      walking_minutes: 5
      distance_meters: 400
      hop_distance: 0
      line_name: Test Line
    - school_id: school_b
      station_id: st_target
      walking_minutes: 6
      distance_meters: 500
      hop_distance: 0
      line_name: Test Line
  popularity_snapshots: []
  user_affinity_snapshots: []
  area_affinity_snapshots: []
"#,
                r#"
  fallback_stage: strict_station
  ordered:
    - "school:school_a"
    - "school:school_b"
"#,
            ),
        )
        .expect("write scenario");
        std::fs::write(
            &pairwise_pack_path,
            r#"
schema_version: 1
kind: replay_pairwise_pack
expectations:
  - scenario_id: S00_PAIRWISE_PACK
    pairwise:
      - higher: "school:school_b"
        lower: "school:school_a"
        note: external pairwise pack should be executed
"#,
        )
        .expect("write pairwise pack");
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let source = ReplayScenarioSource::profile_evaluation(
            scenario_path.clone(),
            temp.path().join("profile.yaml"),
            repo_root
                .join("configs")
                .join("profiles")
                .join("local-discovery-generic")
                .join("reasons.yaml"),
            Some(pairwise_pack_path.clone()),
        );

        let summary = run_replay_scenarios_with_source(
            repo_root.join("configs/ranking"),
            "replay-scenario-test",
            Some("school-event-jp".to_string()),
            source,
        )
        .expect("scenario summary");

        assert_eq!(summary.profile_id.as_deref(), Some("school-event-jp"));
        assert_eq!(
            summary.scenario_source.pairwise_pack,
            Some(pairwise_pack_path)
        );
        assert_eq!(summary.pairwise_total, 1);
        assert_eq!(summary.blockers, 1);
        assert!(summary.cases[0]
            .checks
            .iter()
            .any(|check| { check.name == "pairwise.1" && check.status.as_str() == "failed" }));
    }

    #[test]
    fn replay_scenario_blocks_group_cap_violations() {
        let temp = tempfile::tempdir().expect("tempdir");
        let scenario_path = temp.path().join("group_cap.yaml");
        std::fs::write(
            &scenario_path,
            replay_scenario_yaml(
                "S00_GROUP_CAP",
                r#"
  schools:
    - id: school_a
      name: School A
      area: Minato
      prefecture_name: Tokyo
      school_type: high_school
      group_id: shared_group
    - id: school_b
      name: School B
      area: Minato
      prefecture_name: Tokyo
      school_type: high_school
      group_id: shared_group
  events: []
  stations:
    - id: st_target
      name: Target
      line_name: Test Line
      latitude: 35.6500
      longitude: 139.7500
  school_station_links:
    - school_id: school_a
      station_id: st_target
      walking_minutes: 5
      distance_meters: 400
      hop_distance: 0
      line_name: Test Line
    - school_id: school_b
      station_id: st_target
      walking_minutes: 6
      distance_meters: 500
      hop_distance: 0
      line_name: Test Line
  popularity_snapshots: []
  user_affinity_snapshots: []
  area_affinity_snapshots: []
"#,
                r#"
  fallback_stage: strict_station
  ordered:
    - "school:school_a"
    - "school:school_b"
  max_items_per_group: 1
"#,
            ),
        )
        .expect("write scenario");
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");

        let summary = run_replay_scenarios(
            &scenario_path,
            repo_root.join("configs/ranking"),
            "replay-scenario-test",
        )
        .expect("scenario summary");

        assert_eq!(summary.blockers, 1);
        assert_eq!(summary.cases[0].status.as_str(), "blocked");
        assert!(summary.cases[0].checks.iter().any(|check| {
            check.name == "max_items_per_group" && check.status.as_str() == "failed"
        }));
    }

    #[test]
    fn replay_scenario_school_cap_check_blocks_duplicate_school_items() {
        let yaml = minimal_scenario_yaml("{}");
        let mut scenario: ReplayScenario = serde_yaml::from_str(&yaml).expect("scenario yaml");
        scenario.expectations.max_items_per_school = Some(1);
        let result = RecommendationResult {
            items: vec![
                recommendation_item(ContentKind::School, "school_a", "school_a"),
                recommendation_item(ContentKind::Event, "event_a", "school_a"),
            ],
            explanation: "test".to_string(),
            score_breakdown: Vec::new(),
            fallback_stage: FallbackStage::StrictStation,
            candidate_counts: BTreeMap::new(),
            candidate_plan_trace: None,
            context: None,
            profile_version: "test".to_string(),
            algorithm_version: "test".to_string(),
        };
        let mut case = ReplayScenarioCase {
            id: scenario.id.clone(),
            title: scenario.title.clone(),
            path: PathBuf::from("scenario.yaml"),
            status: ReplayScenarioStatus::Passed,
            expected_fallback_stage: "strict_station".to_string(),
            actual_fallback_stage: Some("strict_station".to_string()),
            expected_order: Vec::new(),
            actual_order: Vec::new(),
            checks: Vec::new(),
        };

        check_max_items_per_school(&mut case, &scenario, &result);

        assert!(case.checks.iter().any(|check| {
            check.name == "max_items_per_school" && check.status.as_str() == "failed"
        }));
    }

    fn minimal_scenario_yaml(candidate_counts: &str) -> String {
        format!(
            r#"
schema_version: 1
kind: replay_scenario
id: S00_TEST
title: Test scenario
query:
  target_station_id: st_test
  limit: 1
  placement: search
  debug: false
dataset:
  schools: []
  events: []
  stations: []
  school_station_links: []
  popularity_snapshots: []
  user_affinity_snapshots: []
  area_affinity_snapshots: []
expectations:
  fallback_stage: strict_station
  ordered:
    - "school:school_test"
  candidate_counts: {candidate_counts}
"#
        )
    }

    fn recommendation_item(
        content_kind: ContentKind,
        content_id: &str,
        school_id: &str,
    ) -> RecommendationItem {
        RecommendationItem {
            content_kind,
            content_id: content_id.to_string(),
            school_id: school_id.to_string(),
            school_name: "Test School".to_string(),
            event_id: (content_kind == ContentKind::Event).then(|| content_id.to_string()),
            event_title: (content_kind == ContentKind::Event).then(|| "Test Event".to_string()),
            primary_station_id: "st_target".to_string(),
            primary_station_name: "Target".to_string(),
            line_name: "Test Line".to_string(),
            score: 1.0,
            explanation: "test".to_string(),
            score_breakdown: Vec::new(),
            fallback_stage: Some(FallbackStage::StrictStation),
        }
    }

    fn replay_scenario_yaml(id: &str, dataset: &str, expectations: &str) -> String {
        format!(
            r#"
schema_version: 1
kind: replay_scenario
id: {id}
title: Test scenario
query:
  target_station_id: st_target
  limit: 2
  placement: search
  debug: false
  context:
    context_source: request_station
    confidence: 0.98
    station:
      station_id: st_target
      station_name: Target
    line:
      line_name: Test Line
    privacy_level: coarse_area
    fallback_policy: school_event_jp_default
    gate_policy: geo_line_default
    warnings: []
dataset:
{dataset}
expectations:
{expectations}
"#
        )
    }
}
