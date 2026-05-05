use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use api_contracts::{FallbackStageDto, RecommendationRequest, RecommendationResponse};
use config::{AppSettings, RankingProfiles};
use domain::{
    FallbackStage, RankingDataset, RankingQuery, RecommendationItem, RecommendationResult,
    ScoreComponent,
};
use ranking::RankingEngine;
use serde::{Deserialize, Serialize};
use storage_postgres::{PgRepository, RecommendationTraceReplayRow};

use crate::repository::pg_repository;

pub const DEFAULT_REPLAY_SCENARIO_PATH: &str = "configs/evaluation/scenarios";
const REPLAY_SCENARIO_SCHEMA_VERSION: u32 = 1;

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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PairwiseExpectation {
    pub higher: String,
    pub lower: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplayScenarioSummary {
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

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QualitySeverity {
    Blocker,
    Warning,
}

impl QualitySeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Blocker => "blocker",
            Self::Warning => "warning",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QualityCheckStatus {
    Passed,
    Failed,
}

impl QualityCheckStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
        }
    }
}

pub fn run_replay_scenarios(
    scenario_path: impl AsRef<Path>,
    ranking_config_dir: impl AsRef<Path>,
    algorithm_version: &str,
) -> Result<ReplayScenarioSummary> {
    let scenarios = load_replay_scenarios(scenario_path)?;
    let profiles = RankingProfiles::load_from_dir(ranking_config_dir)?;
    let engine = RankingEngine::new(profiles, algorithm_version.to_string());
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

fn load_replay_scenarios(path: impl AsRef<Path>) -> Result<Vec<(PathBuf, ReplayScenario)>> {
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
        validate_replay_scenario(&scenario_path, &scenario)?;
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

fn is_yaml_path(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("yaml" | "yml")
    )
}

fn validate_replay_scenario(path: &Path, scenario: &ReplayScenario) -> Result<()> {
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
        ensure!(
            !pairwise.higher.trim().is_empty() && !pairwise.lower.trim().is_empty(),
            "scenario {} pairwise expectations must name both higher and lower items",
            scenario.id
        );
        ensure!(
            pairwise.higher != pairwise.lower,
            "scenario {} pairwise expectation cannot compare {} to itself",
            scenario.id,
            pairwise.higher
        );
        validate_item_key(
            &scenario.id,
            "expectations.pairwise.higher",
            &pairwise.higher,
        )?;
        validate_item_key(&scenario.id, "expectations.pairwise.lower", &pairwise.lower)?;
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
                ranking::reason_catalog()
                    .iter()
                    .any(|entry| entry.reason_code == reason_code),
                "scenario {} expects unknown reason_code {} for {}",
                scenario.id,
                reason_code,
                item_key
            );
        }
    }

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
    matches!(
        stage,
        "strict_station"
            | "same_line"
            | "same_city"
            | "same_prefecture"
            | "neighbor_area"
            | "safe_global_popular"
    )
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
    check_candidate_counts(&mut case, scenario, &result);
    check_required_reason_codes(&mut case, scenario, &result);
    check_explanation_integrity(&mut case, &result);

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

fn check_explanation_integrity(case: &mut ReplayScenarioCase, result: &RecommendationResult) {
    let component_failures = result
        .items
        .iter()
        .flat_map(|item| {
            item.score_breakdown
                .iter()
                .map(move |component| (item_key(item), component))
        })
        .chain(
            result
                .score_breakdown
                .iter()
                .map(|component| ("result".to_string(), component)),
        )
        .filter_map(|(scope, component)| reason_component_failure(&scope, component))
        .collect::<Vec<_>>();
    push_check(
        case,
        "explanation_integrity.reason_catalog",
        QualitySeverity::Blocker,
        component_failures.is_empty(),
        format!(
            "score component reason catalog failures: {}",
            if component_failures.is_empty() {
                "-".to_string()
            } else {
                component_failures.join("; ")
            }
        ),
    );

    let missing_labels = top_reason_labels(&result.score_breakdown)
        .into_iter()
        .filter(|label| !result.explanation.contains(label))
        .collect::<Vec<_>>();
    push_check(
        case,
        "explanation_integrity.top_reason_labels",
        QualitySeverity::Blocker,
        missing_labels.is_empty(),
        format!(
            "top-level explanation must mention top contributing labels; missing {}",
            format_order(&missing_labels)
        ),
    );

    let item_reason_label_failures = result
        .items
        .iter()
        .filter_map(|item| {
            let missing = top_reason_labels(&item.score_breakdown)
                .into_iter()
                .filter(|label| !item.explanation.contains(label))
                .collect::<Vec<_>>();
            (!missing.is_empty())
                .then(|| format!("{} missing {}", item_key(item), missing.join(",")))
        })
        .collect::<Vec<_>>();
    push_check(
        case,
        "explanation_integrity.item_reason_labels",
        QualitySeverity::Blocker,
        item_reason_label_failures.is_empty(),
        format!(
            "item explanations must mention top contributing labels; failures {}",
            format_order(&item_reason_label_failures)
        ),
    );

    let stage_markers = top_level_stage_markers(&result.fallback_stage);
    push_check(
        case,
        "explanation_template.fallback_stage",
        QualitySeverity::Blocker,
        stage_markers
            .iter()
            .any(|marker| result.explanation.contains(marker)),
        format!(
            "top-level explanation must mention fallback stage {}; markers {}",
            result.fallback_stage.as_str(),
            stage_markers.join(",")
        ),
    );

    let item_stage_failures = result
        .items
        .iter()
        .filter(|item| item.fallback_stage.as_ref() != Some(&result.fallback_stage))
        .map(item_key)
        .collect::<Vec<_>>();
    push_check(
        case,
        "explanation_integrity.item_fallback_stage",
        QualitySeverity::Blocker,
        item_stage_failures.is_empty(),
        format!(
            "items must carry actual fallback stage {}; mismatched items {}",
            result.fallback_stage.as_str(),
            format_order(&item_stage_failures)
        ),
    );

    let item_markers = item_stage_markers(&result.fallback_stage);
    let item_template_failures = result
        .items
        .iter()
        .filter(|item| {
            !item_markers
                .iter()
                .any(|marker| item.explanation.contains(marker))
        })
        .map(item_key)
        .collect::<Vec<_>>();
    push_check(
        case,
        "explanation_template.item_fallback_stage",
        QualitySeverity::Blocker,
        item_template_failures.is_empty(),
        format!(
            "item explanations must mention fallback stage {}; markers {}; missing items {}",
            result.fallback_stage.as_str(),
            item_markers.join(","),
            format_order(&item_template_failures)
        ),
    );
}

fn reason_component_failure(scope: &str, component: &ScoreComponent) -> Option<String> {
    let catalog_entry = match ranking::reason_catalog_entry(&component.feature) {
        Some(entry) => entry,
        None => {
            return Some(format!(
                "{scope}: feature {} is missing from reason catalog",
                component.feature
            ));
        }
    };
    (component.reason_code != catalog_entry.reason_code).then(|| {
        format!(
            "{scope}: feature {} emitted reason_code {}, expected {}",
            component.feature, component.reason_code, catalog_entry.reason_code
        )
    })
}

fn top_reason_labels(breakdown: &[ScoreComponent]) -> Vec<String> {
    let mut components = breakdown
        .iter()
        .filter(|component| component.value > 0.0)
        .collect::<Vec<_>>();
    components.sort_by(|left, right| {
        right
            .value
            .total_cmp(&left.value)
            .then_with(|| left.feature.cmp(&right.feature))
    });

    let mut labels = Vec::new();
    for component in components {
        let label = ranking::reason_catalog_entry(&component.feature)
            .map(|entry| entry.label.to_string())
            .unwrap_or_else(|| "固定重み".to_string());
        if labels.contains(&label) {
            continue;
        }
        labels.push(label);
        if labels.len() >= 2 {
            break;
        }
    }
    if labels.is_empty() {
        labels.push("固定重み".to_string());
    }
    labels
}

fn top_level_stage_markers(stage: &FallbackStage) -> &'static [&'static str] {
    match stage {
        FallbackStage::StrictStation => &["直結の候補群"],
        FallbackStage::SameLine => &["沿線の候補群"],
        FallbackStage::SameCity => &["同一市区町村"],
        FallbackStage::SamePrefecture => &["同一都道府県"],
        FallbackStage::NeighborArea => &["近傍まで広げた候補群"],
        FallbackStage::SafeGlobalPopular => &["広域人気を距離で抑制した候補群"],
    }
}

fn item_stage_markers(stage: &FallbackStage) -> &'static [&'static str] {
    match stage {
        FallbackStage::StrictStation => &["指定駅直結"],
        FallbackStage::SameLine => &["同一路線"],
        FallbackStage::SameCity => &["同一市区町村"],
        FallbackStage::SamePrefecture => &["同一都道府県"],
        FallbackStage::NeighborArea => &["近隣エリア"],
        FallbackStage::SafeGlobalPopular => &["広域fallback"],
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

pub async fn run_replay_evaluate(
    settings: &AppSettings,
    limit: i64,
) -> Result<ReplayEvaluationSummary> {
    let profiles = RankingProfiles::load_from_dir(&settings.ranking_config_dir)?;
    let neighbor_distance_cap_meters = profiles.fallback.neighbor_distance_cap_meters;
    let engine = RankingEngine::new(profiles, settings.algorithm_version.clone());
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
    let candidate_links = match repository
        .load_context_candidate_links(
            &target_station,
            query.context.as_ref().expect("resolved context is set"),
            candidate_limit,
            min_candidate_count,
            neighbor_distance_cap_meters,
            neighbor_max_hops,
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
    let actual = match engine.recommend(&dataset, &query) {
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

fn response_order(response: &RecommendationResponse) -> Vec<String> {
    response
        .items
        .iter()
        .map(|item| format!("{}:{}", item.content_kind.as_str(), item.content_id))
        .collect()
}

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

fn stored_response_fallback_stage(response: &serde_json::Value) -> Option<String> {
    response
        .get("fallback_stage")
        .and_then(serde_json::Value::as_str)
        .map(normalize_fallback_stage)
}

fn normalize_fallback_stage(stage: &str) -> String {
    match stage {
        "strict" => "strict_station",
        other => other,
    }
    .to_string()
}

fn fallback_stage_label(fallback_stage: &FallbackStageDto) -> String {
    fallback_stage.as_str().to_string()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        path::{Path, PathBuf},
    };

    use super::{
        normalize_fallback_stage, run_replay_scenarios, stored_response_order,
        validate_replay_scenario, ReplayScenario, DEFAULT_REPLAY_SCENARIO_PATH,
    };

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
        ] {
            assert!(scenario_ids.contains(expected_id));
        }
        assert!(summary.scenarios >= 4);
        assert_eq!(summary.blockers, 0);
        assert!(summary.pairwise_total >= 3);
        assert!(summary.explanation_integrity_total >= summary.scenarios * 6);
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
}
