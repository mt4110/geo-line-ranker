use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::Result;
use config::{lint_profile_pack_dir, ProfilePackLintFile, ProfilePackLintSummary};
use context::ContextSource;
use serde::Serialize;

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
    pub reason_count: usize,
    pub fixture_references: usize,
    pub source_manifest_references: usize,
    pub event_csv_example_references: usize,
    pub optional_crawler_manifest_references: usize,
    pub files: Vec<ProfilePackDoctorFile>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProfilePackDoctorFile {
    pub path: PathBuf,
    pub profile_id: String,
    pub ranking_config_dir: PathBuf,
    pub reason_catalog_path: PathBuf,
    pub schema_version: u32,
    pub kind: String,
    pub manifest_version: u32,
    pub supported_content_kinds: Vec<String>,
    pub reason_count: usize,
    pub fixture_references: usize,
    pub source_manifest_references: usize,
    pub event_csv_example_references: usize,
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

pub fn run_context_coverage_doctor(
    scenario_path: impl AsRef<Path>,
) -> Result<ContextCoverageDoctorSummary> {
    let scenarios = load_replay_scenarios(scenario_path)?;
    Ok(context_coverage_summary_from_scenarios(scenarios))
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

fn context_shape_mismatch(
    case: &ContextCoverageDoctorCase,
) -> Option<ContextCoverageShapeMismatch> {
    let context_source = case.context_source.as_deref()?;
    let expected_shape = expected_context_shape(context_source)?;
    if context_shape_matches(case, expected_shape) {
        return None;
    }

    Some(ContextCoverageShapeMismatch {
        id: case.id.clone(),
        path: case.path.clone(),
        context_source: context_source.to_string(),
        expected_shape: expected_shape.to_string(),
        actual_shape: context_shape_parts(case),
    })
}

fn expected_context_shape(context_source: &str) -> Option<&'static str> {
    match context_source {
        source if source == ContextSource::RequestArea.as_str() => Some("area"),
        source if source == ContextSource::RequestLine.as_str() => Some("line"),
        source if source == ContextSource::RequestStation.as_str() => Some("station"),
        source if source == ContextSource::DefaultSafeContext.as_str() => Some("none"),
        _ => None,
    }
}

fn context_shape_matches(case: &ContextCoverageDoctorCase, expected_shape: &str) -> bool {
    match expected_shape {
        "area" => case.has_area_context,
        "line" => case.has_line_context,
        "station" => case.has_station_context,
        "none" => !case.has_area_context && !case.has_line_context && !case.has_station_context,
        _ => true,
    }
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
        reason_count: files.iter().map(|file| file.reason_count).sum(),
        fixture_references: files.iter().map(|file| file.fixture_references).sum(),
        source_manifest_references: files
            .iter()
            .map(|file| file.source_manifest_references)
            .sum(),
        event_csv_example_references: files
            .iter()
            .map(|file| file.event_csv_example_references)
            .sum(),
        optional_crawler_manifest_references: files
            .iter()
            .map(|file| file.optional_crawler_manifest_references)
            .sum(),
        files,
    }
}

fn profile_pack_doctor_file(file: ProfilePackLintFile) -> ProfilePackDoctorFile {
    ProfilePackDoctorFile {
        path: file.path,
        profile_id: file.profile_id,
        ranking_config_dir: file.ranking_config_dir,
        reason_catalog_path: file.reason_catalog_path,
        schema_version: file.schema_version,
        kind: file.kind.as_str().to_string(),
        manifest_version: file.manifest_version,
        supported_content_kinds: file
            .supported_content_kinds
            .into_iter()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        reason_count: file.reason_count,
        fixture_references: file.fixture_count,
        source_manifest_references: file.source_manifest_count,
        event_csv_example_references: file.event_csv_example_count,
        optional_crawler_manifest_references: file.optional_crawler_manifest_count,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        explanation_integrity_summary_from_replay, run_context_coverage_doctor,
        run_explanation_integrity_doctor, run_profile_pack_doctor,
    };
    use crate::{
        explanation_integrity::{QualityCheckStatus, QualitySeverity},
        replay::{
            ReplayScenarioCase, ReplayScenarioCheck, ReplayScenarioStatus, ReplayScenarioSummary,
            DEFAULT_REPLAY_SCENARIO_PATH,
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

        assert!(summary.scenarios >= 4);
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
            summary.optional_crawler_manifest_references,
            summary
                .files
                .iter()
                .map(|file| file.optional_crawler_manifest_references)
                .sum::<usize>()
        );

        let local_discovery = summary
            .files
            .iter()
            .find(|file| file.profile_id == "local-discovery-generic")
            .expect("local discovery profile");
        assert!(local_discovery.fixture_references > 0);

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
    }

    #[test]
    fn committed_replay_scenarios_pass_context_coverage_doctor() {
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let summary = run_context_coverage_doctor(repo_root.join(DEFAULT_REPLAY_SCENARIO_PATH))
            .expect("context coverage doctor");

        assert!(summary.scenarios >= 4);
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
                "request_line",
                r#"
    area:
      country: JP
      prefecture_name: Tokyo
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

        let summary = run_context_coverage_doctor(temp.path()).expect("context coverage doctor");

        assert!(summary.has_blockers());
        assert!(summary.missing_required_context_sources.is_empty());
        assert_eq!(summary.context_shape_mismatches.len(), 1);
        let mismatch = &summary.context_shape_mismatches[0];
        assert_eq!(mismatch.id, "S01_LINE_BAD_SHAPE");
        assert_eq!(mismatch.context_source, "request_line");
        assert_eq!(mismatch.expected_shape, "line");
        assert_eq!(mismatch.actual_shape, vec!["area".to_string()]);
        assert_eq!(
            summary.blocker_message().as_deref(),
            Some("context_shape_mismatches=S01_LINE_BAD_SHAPE")
        );
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
