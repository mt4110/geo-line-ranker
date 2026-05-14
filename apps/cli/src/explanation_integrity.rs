#[cfg(feature = "storage-backends")]
use api_contracts::{FallbackStageDto, RecommendationResponse, ScoreComponentDto};
use domain::{FallbackStage, RecommendationItem, RecommendationResult, ScoreComponent};
use ranking::ReasonCatalog;
use serde::Serialize;

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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExplanationIntegrityCheck {
    pub name: String,
    pub severity: QualitySeverity,
    pub status: QualityCheckStatus,
    pub message: String,
}

struct IntegrityInput<'a> {
    explanation: &'a str,
    fallback_stage: &'a str,
    score_breakdown: Vec<ComponentRef<'a>>,
    items: Vec<ItemRef<'a>>,
}

struct ItemRef<'a> {
    key: String,
    explanation: &'a str,
    fallback_stage: Option<&'a str>,
    score_breakdown: Vec<ComponentRef<'a>>,
}

#[derive(Clone, Copy)]
struct ComponentRef<'a> {
    feature: &'a str,
    reason_code: &'a str,
    value: f64,
}

pub fn check_recommendation_result_integrity_with_catalog(
    result: &RecommendationResult,
    reason_catalog: &ReasonCatalog,
) -> Vec<ExplanationIntegrityCheck> {
    check_integrity(
        IntegrityInput {
            explanation: &result.explanation,
            fallback_stage: result.fallback_stage.as_str(),
            score_breakdown: result.score_breakdown.iter().map(component_ref).collect(),
            items: result
                .items
                .iter()
                .map(|item| ItemRef {
                    key: item_key(item),
                    explanation: &item.explanation,
                    fallback_stage: item.fallback_stage.as_ref().map(FallbackStage::as_str),
                    score_breakdown: item.score_breakdown.iter().map(component_ref).collect(),
                })
                .collect(),
        },
        reason_catalog,
    )
}

#[cfg(feature = "storage-backends")]
pub fn check_recommendation_response_integrity_with_catalog(
    response: &RecommendationResponse,
    reason_catalog: &ReasonCatalog,
) -> Vec<ExplanationIntegrityCheck> {
    check_integrity(
        IntegrityInput {
            explanation: &response.explanation,
            fallback_stage: response.fallback_stage.as_str(),
            score_breakdown: response
                .score_breakdown
                .iter()
                .map(component_dto_ref)
                .collect(),
            items: response
                .items
                .iter()
                .map(|item| ItemRef {
                    key: format!("{}:{}", item.content_kind.as_str(), item.content_id),
                    explanation: &item.explanation,
                    fallback_stage: item.fallback_stage.as_ref().map(FallbackStageDto::as_str),
                    score_breakdown: item.score_breakdown.iter().map(component_dto_ref).collect(),
                })
                .collect(),
        },
        reason_catalog,
    )
}

fn check_integrity(
    input: IntegrityInput<'_>,
    reason_catalog: &ReasonCatalog,
) -> Vec<ExplanationIntegrityCheck> {
    let mut checks = Vec::new();

    let component_failures = input
        .items
        .iter()
        .flat_map(|item| {
            item.score_breakdown
                .iter()
                .map(move |component| (item.key.as_str(), *component))
        })
        .chain(
            input
                .score_breakdown
                .iter()
                .map(|component| ("result", *component)),
        )
        .filter_map(|(scope, component)| reason_component_failure(scope, component, reason_catalog))
        .collect::<Vec<_>>();
    push_check(
        &mut checks,
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

    let missing_labels = top_reason_labels(&input.score_breakdown, reason_catalog)
        .into_iter()
        .filter(|label| !input.explanation.contains(label))
        .collect::<Vec<_>>();
    push_check(
        &mut checks,
        "explanation_integrity.top_reason_labels",
        QualitySeverity::Blocker,
        missing_labels.is_empty(),
        format!(
            "top-level explanation must mention top contributing labels; missing {}",
            format_order(&missing_labels)
        ),
    );

    let item_reason_label_failures = input
        .items
        .iter()
        .filter_map(|item| {
            let missing = top_reason_labels(&item.score_breakdown, reason_catalog)
                .into_iter()
                .filter(|label| !item.explanation.contains(label))
                .collect::<Vec<_>>();
            (!missing.is_empty()).then(|| format!("{} missing {}", item.key, missing.join(",")))
        })
        .collect::<Vec<_>>();
    push_check(
        &mut checks,
        "explanation_integrity.item_reason_labels",
        QualitySeverity::Blocker,
        item_reason_label_failures.is_empty(),
        format!(
            "item explanations must mention top contributing labels; failures {}",
            format_order(&item_reason_label_failures)
        ),
    );

    let stage_markers = top_level_stage_markers(input.fallback_stage);
    push_check(
        &mut checks,
        "explanation_template.fallback_stage",
        QualitySeverity::Blocker,
        stage_markers
            .iter()
            .any(|marker| input.explanation.contains(marker)),
        format!(
            "top-level explanation must mention fallback stage {}; markers {}",
            input.fallback_stage,
            stage_markers.join(",")
        ),
    );

    let item_stage_failures = input
        .items
        .iter()
        .filter(|item| item.fallback_stage != Some(input.fallback_stage))
        .map(|item| item.key.clone())
        .collect::<Vec<_>>();
    push_check(
        &mut checks,
        "explanation_integrity.item_fallback_stage",
        QualitySeverity::Blocker,
        item_stage_failures.is_empty(),
        format!(
            "items must carry actual fallback stage {}; mismatched items {}",
            input.fallback_stage,
            format_order(&item_stage_failures)
        ),
    );

    let item_markers = item_stage_markers(input.fallback_stage);
    let item_template_failures = input
        .items
        .iter()
        .filter(|item| {
            !item_markers
                .iter()
                .any(|marker| item.explanation.contains(marker))
        })
        .map(|item| item.key.clone())
        .collect::<Vec<_>>();
    push_check(
        &mut checks,
        "explanation_template.item_fallback_stage",
        QualitySeverity::Blocker,
        item_template_failures.is_empty(),
        format!(
            "item explanations must mention fallback stage {}; markers {}; missing items {}",
            input.fallback_stage,
            item_markers.join(","),
            format_order(&item_template_failures)
        ),
    );

    checks
}

fn reason_component_failure(
    scope: &str,
    component: ComponentRef<'_>,
    reason_catalog: &ReasonCatalog,
) -> Option<String> {
    let catalog_entry = match reason_catalog.entry(component.feature) {
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

fn top_reason_labels(
    breakdown: &[ComponentRef<'_>],
    reason_catalog: &ReasonCatalog,
) -> Vec<String> {
    let mut components = breakdown
        .iter()
        .filter(|component| component.value > 0.0)
        .copied()
        .collect::<Vec<_>>();
    components.sort_by(|left, right| {
        right
            .value
            .total_cmp(&left.value)
            .then_with(|| left.feature.cmp(right.feature))
    });

    let mut labels = Vec::new();
    for component in components {
        let label = reason_catalog
            .entry(component.feature)
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

fn top_level_stage_markers(stage: &str) -> &'static [&'static str] {
    match stage {
        "strict_station" => &["直結の候補群"],
        "same_line" => &["沿線の候補群"],
        "same_city" => &["同一市区町村"],
        "same_prefecture" => &["同一都道府県"],
        "neighbor_area" => &["近傍まで広げた候補群"],
        "safe_global_popular" => &["広域人気を距離で抑制した候補群"],
        _ => &[],
    }
}

fn item_stage_markers(stage: &str) -> &'static [&'static str] {
    match stage {
        "strict_station" => &["指定駅直結"],
        "same_line" => &["同一路線"],
        "same_city" => &["同一市区町村"],
        "same_prefecture" => &["同一都道府県"],
        "neighbor_area" => &["近隣エリア"],
        "safe_global_popular" => &["広域fallback"],
        _ => &[],
    }
}

fn push_check(
    checks: &mut Vec<ExplanationIntegrityCheck>,
    name: &str,
    severity: QualitySeverity,
    passed: bool,
    message: String,
) {
    checks.push(ExplanationIntegrityCheck {
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

fn component_ref(component: &ScoreComponent) -> ComponentRef<'_> {
    ComponentRef {
        feature: &component.feature,
        reason_code: &component.reason_code,
        value: component.value,
    }
}

#[cfg(feature = "storage-backends")]
fn component_dto_ref(component: &ScoreComponentDto) -> ComponentRef<'_> {
    ComponentRef {
        feature: &component.feature,
        reason_code: &component.reason_code,
        value: component.value,
    }
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
