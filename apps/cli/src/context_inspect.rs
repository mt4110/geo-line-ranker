use anyhow::Result;
use config::AppSettings;
use context::{AreaContextInput, ContextEvidenceSummary, ContextInput, RankingContext};
use serde::Serialize;

use crate::repository::pg_repository;

#[derive(Debug, Clone)]
pub struct ContextInspectInput {
    pub request_id: String,
    pub user_id: Option<String>,
    pub station_id: Option<String>,
    pub line_id: Option<String>,
    pub line_name: Option<String>,
    pub area: AreaContextInput,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContextInspectSummary {
    pub request_id: String,
    pub context: RankingContext,
    pub evidence_summary: ContextEvidenceSummary,
}

pub async fn run_context_inspect(
    settings: &AppSettings,
    input: ContextInspectInput,
) -> Result<ContextInspectSummary> {
    let repository = pg_repository(settings)?;
    let context_input = ContextInput {
        station_id: input.station_id,
        line_id: input.line_id,
        line_name: input.line_name,
        area: (!input.area.is_empty()).then_some(input.area),
    };
    let context = repository
        .resolve_context_read_only(&input.request_id, input.user_id.as_deref(), &context_input)
        .await?;
    let evidence_summary = context.evidence_summary();

    Ok(ContextInspectSummary {
        request_id: input.request_id,
        context,
        evidence_summary,
    })
}

pub fn format_context_inspect_summary(summary: &ContextInspectSummary) -> String {
    let mut lines = vec![
        format!(
            "context inspect ok: request_id={} context_source={} confidence={:.2} privacy_level={}",
            summary.request_id,
            summary.context.context_source.as_str(),
            summary.context.confidence,
            summary.context.privacy_level.as_str()
        ),
        format!(
            "evidence: primary_kind={} evidence_count={} strongest_strength={:.2} has_search_execute={}",
            summary.evidence_summary.primary_kind.as_str(),
            summary.evidence_summary.evidence_count,
            summary.evidence_summary.strongest_strength,
            summary.evidence_summary.has_search_execute
        ),
    ];

    match summary.context.area.as_ref() {
        Some(area) => lines.push(format!(
            "area: country={} prefecture_code={} prefecture_name={} city_code={} city_name={}",
            area.country,
            display_optional(area.prefecture_code.as_deref()),
            display_optional(area.prefecture_name.as_deref()),
            display_optional(area.city_code.as_deref()),
            display_optional(area.city_name.as_deref())
        )),
        None => lines.push("area: none".to_string()),
    }

    match summary.context.line.as_ref() {
        Some(line) => lines.push(format!(
            "line: line_id={} line_name={} operator_name={}",
            display_optional(line.line_id.as_deref()),
            line.line_name,
            display_optional(line.operator_name.as_deref())
        )),
        None => lines.push("line: none".to_string()),
    }

    match summary.context.station.as_ref() {
        Some(station) => lines.push(format!(
            "station: station_id={} station_name={}",
            station.station_id, station.station_name
        )),
        None => lines.push("station: none".to_string()),
    }

    if summary.context.warnings.is_empty() {
        lines.push("warnings: none".to_string());
    } else {
        lines.push("warnings:".to_string());
        lines.extend(
            summary
                .context
                .warnings
                .iter()
                .map(|warning| format!("- {}: {}", warning.code, warning.message)),
        );
    }

    lines.join("\n")
}

fn display_optional(value: Option<&str>) -> &str {
    value
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("-")
}

#[cfg(test)]
mod tests {
    use context::{ContextEvidenceKind, RankingContext};

    use super::{format_context_inspect_summary, ContextInspectSummary};

    #[test]
    fn format_context_inspect_summary_reports_empty_evidence_for_default_safe_context() {
        let context = RankingContext::default_safe();
        let summary = ContextInspectSummary {
            request_id: "req-test".to_string(),
            evidence_summary: context.evidence_summary(),
            context,
        };

        let output = format_context_inspect_summary(&summary);

        assert!(output.contains("context_source=default_safe_context"));
        assert!(output.contains("evidence_count=0"));
        assert!(output.contains("warnings: none"));
    }

    #[test]
    fn format_context_inspect_summary_reports_recent_search_evidence() {
        let mut context = RankingContext::default_safe();
        context.context_source = context::ContextSource::RecentSearchContext;
        context.confidence = 0.88;
        let evidence_summary = context.evidence_summary();
        assert_eq!(
            evidence_summary.primary_kind,
            ContextEvidenceKind::SearchExecute
        );
        let summary = ContextInspectSummary {
            request_id: "req-test".to_string(),
            context,
            evidence_summary,
        };

        let output = format_context_inspect_summary(&summary);

        assert!(output.contains("context_source=recent_search_context"));
        assert!(output.contains("primary_kind=search_execute"));
        assert!(output.contains("has_search_execute=true"));
    }
}
