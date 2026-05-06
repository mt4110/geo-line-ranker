use std::path::{Path, PathBuf};

use anyhow::Result;
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

pub fn run_explanation_integrity_doctor(
    scenario_path: impl AsRef<Path>,
    ranking_config_dir: impl AsRef<Path>,
    algorithm_version: &str,
) -> Result<ExplanationIntegrityDoctorSummary> {
    let replay_summary =
        run_replay_scenarios(scenario_path, ranking_config_dir, algorithm_version)?;
    Ok(explanation_integrity_summary_from_replay(replay_summary))
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{explanation_integrity_summary_from_replay, run_explanation_integrity_doctor};
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
