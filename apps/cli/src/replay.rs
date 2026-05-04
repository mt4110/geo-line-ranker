use anyhow::{Context, Result};
use api_contracts::{FallbackStageDto, RecommendationRequest, RecommendationResponse};
use config::{AppSettings, RankingProfiles};
use ranking::RankingEngine;
use storage_postgres::{PgRepository, RecommendationTraceReplayRow};

use crate::repository::pg_repository;

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
    use super::{normalize_fallback_stage, stored_response_order};

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
}
