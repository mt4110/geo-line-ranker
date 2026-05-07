use std::path::{Path, PathBuf};

use anyhow::Result;
use config::{lint_profile_pack_dir, ProfilePackLintFile, ProfilePackLintSummary};
use serde::Serialize;

use crate::{
    explanation_integrity::{QualityCheckStatus, QualitySeverity},
    replay::{
        run_replay_scenarios, ReplayScenarioCheck, ReplayScenarioStatus, ReplayScenarioSummary,
    },
};

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
        explanation_integrity_summary_from_replay, run_explanation_integrity_doctor,
        run_profile_pack_doctor,
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
}
