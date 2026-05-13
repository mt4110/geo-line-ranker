use std::collections::BTreeMap;

use domain::{
    CandidatePlanStageStatus, CandidatePlanStageTrace, CandidatePlanTrace, FallbackStage,
    RankingDataset, RankingQuery, RecommendationResult,
};

use crate::diversity::DiversitySelection;
use crate::explanation::build_top_level_explanation;
use crate::graph::CandidateGraphExpansion;
use crate::scoring::CandidateScoringInput;
use crate::{RankingEngine, RankingError};

impl RankingEngine {
    pub fn recommend(
        &self,
        dataset: &RankingDataset,
        query: &RankingQuery,
    ) -> Result<RecommendationResult, RankingError> {
        self.recommend_with_graph_expansion(dataset, query, &CandidateGraphExpansion::empty())
    }

    pub fn recommend_with_graph_expansion(
        &self,
        dataset: &RankingDataset,
        query: &RankingQuery,
        graph_expansion: &CandidateGraphExpansion,
    ) -> Result<RecommendationResult, RankingError> {
        let target_station = dataset
            .stations
            .iter()
            .find(|station| station.id == query.target_station_id)
            .cloned()
            .ok_or_else(|| RankingError::UnknownStation(query.target_station_id.clone()))?;
        let placement_profile = self.profiles.placement(query.placement);

        let limit = query
            .limit
            .unwrap_or(self.profiles.schools.limit_default)
            .clamp(1, 20);
        let strict_min_candidates = self.minimum_candidate_count();

        let staged_candidates =
            self.collect_candidates_by_stage(dataset, query, &target_station, graph_expansion);
        let candidate_counts = staged_candidates
            .iter()
            .map(|(stage, candidates)| (stage.as_str().to_string(), candidates.len()))
            .collect::<BTreeMap<_, _>>();
        let area_hint_was_ignored = query.context.as_ref().is_some_and(|context| {
            context
                .warnings
                .iter()
                .any(|warning| warning.code == "station_area_conflict")
        });
        let area_context_is_usable = query.context.as_ref().is_some_and(|context| {
            !area_hint_was_ignored
                && (context.city_name().is_some() || context.prefecture_name().is_some())
        });
        let first_sufficient_scoped_match = staged_candidates
            .iter()
            .filter(|(stage, _)| !matches!(stage, FallbackStage::SafeGlobalPopular))
            .find(|(_, candidates)| candidates.len() >= strict_min_candidates);
        let underfilled_area_match = area_context_is_usable
            .then(|| {
                staged_candidates.iter().find(|(stage, candidates)| {
                    matches!(
                        stage,
                        FallbackStage::SameCity | FallbackStage::SamePrefecture
                    ) && !candidates.is_empty()
                })
            })
            .flatten();
        let sufficient_safe_global_match = staged_candidates.iter().find(|(stage, candidates)| {
            matches!(stage, FallbackStage::SafeGlobalPopular)
                && candidates.len() >= strict_min_candidates
        });
        let selected_stage = if let Some((stage, candidates)) = first_sufficient_scoped_match {
            Some((
                stage,
                candidates,
                CandidatePlanSelectionReason::SufficientScoped,
            ))
        } else if let Some((stage, candidates)) = underfilled_area_match {
            Some((
                stage,
                candidates,
                CandidatePlanSelectionReason::UnderfilledAreaContext,
            ))
        } else if let Some((stage, candidates)) = sufficient_safe_global_match {
            Some((
                stage,
                candidates,
                CandidatePlanSelectionReason::SufficientSafeGlobal,
            ))
        } else {
            staged_candidates
                .iter()
                .filter(|(_, candidates)| !candidates.is_empty())
                .max_by(
                    |(left_stage, left_candidates), (right_stage, right_candidates)| {
                        left_candidates
                            .len()
                            .cmp(&right_candidates.len())
                            .then_with(|| right_stage.priority().cmp(&left_stage.priority()))
                    },
                )
                .map(|(stage, candidates)| {
                    (
                        stage,
                        candidates,
                        CandidatePlanSelectionReason::BestAvailableNonEmpty,
                    )
                })
        };
        let (fallback_stage, candidates, selection_reason) = selected_stage
            .map(|(stage, candidates, reason)| (stage.clone(), candidates.clone(), reason))
            .unwrap_or_else(|| {
                (
                    FallbackStage::SafeGlobalPopular,
                    Vec::new(),
                    CandidatePlanSelectionReason::NoCandidates,
                )
            });
        let candidate_plan_trace = build_candidate_plan_trace(
            &staged_candidates,
            strict_min_candidates,
            &fallback_stage,
            selection_reason,
            query,
            area_hint_was_ignored,
            area_context_is_usable,
        );

        if candidates.is_empty() {
            return Err(RankingError::NoCandidates(target_station.id));
        }

        let scored_candidates = self.score_candidates(
            CandidateScoringInput {
                dataset,
                query,
                target_station: &target_station,
                placement_profile,
                fallback_stage: &fallback_stage,
                graph_expansion,
            },
            candidates,
        );
        let DiversitySelection {
            items,
            summary: diversity_summary,
        } = self.select_diverse_items(scored_candidates, limit, placement_profile);
        if items.is_empty() {
            return Err(RankingError::NoCandidates(target_station.id));
        }

        let top_level_explanation = build_top_level_explanation(
            query.placement,
            &target_station,
            &fallback_stage,
            &items,
            &diversity_summary,
        );
        let score_breakdown = items
            .first()
            .map(|item| item.score_breakdown.clone())
            .unwrap_or_default();

        Ok(RecommendationResult {
            items,
            explanation: top_level_explanation,
            score_breakdown,
            fallback_stage,
            profile_version: self.profiles.profile_version.clone(),
            algorithm_version: self.algorithm_version.clone(),
            candidate_counts,
            candidate_plan_trace: Some(candidate_plan_trace),
            context: query.context.clone(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum CandidatePlanSelectionReason {
    SufficientScoped,
    UnderfilledAreaContext,
    SufficientSafeGlobal,
    BestAvailableNonEmpty,
    NoCandidates,
}

impl CandidatePlanSelectionReason {
    fn stop_reason(self) -> &'static str {
        match self {
            Self::SufficientScoped => "sufficient_scoped_candidates",
            Self::UnderfilledAreaContext => "underfilled_area_context",
            Self::SufficientSafeGlobal => "sufficient_safe_global_candidates",
            Self::BestAvailableNonEmpty => "best_available_non_empty_stage",
            Self::NoCandidates => "no_candidates_available",
        }
    }

    fn selected_stage_reason(self) -> &'static str {
        match self {
            Self::SufficientScoped => "selected_sufficient_scoped_candidates",
            Self::UnderfilledAreaContext => "selected_underfilled_area_context",
            Self::SufficientSafeGlobal => "selected_sufficient_safe_global_candidates",
            Self::BestAvailableNonEmpty => "selected_best_available_non_empty_stage",
            Self::NoCandidates => "selected_no_candidates_available",
        }
    }

    fn stops_before_later_stages(self) -> bool {
        matches!(
            self,
            Self::SufficientScoped | Self::UnderfilledAreaContext | Self::SufficientSafeGlobal
        )
    }
}

fn build_candidate_plan_trace(
    staged_candidates: &[(FallbackStage, Vec<domain::SchoolStationLink>)],
    minimum_candidate_count: usize,
    selected_stage: &FallbackStage,
    selection_reason: CandidatePlanSelectionReason,
    query: &RankingQuery,
    area_hint_was_ignored: bool,
    area_context_is_usable: bool,
) -> CandidatePlanTrace {
    CandidatePlanTrace {
        minimum_candidate_count,
        selected_stage: selected_stage.clone(),
        stop_reason: selection_reason.stop_reason().to_string(),
        area_context_usable: area_context_is_usable,
        stages: staged_candidates
            .iter()
            .map(|(stage, candidates)| {
                let selected = stage == selected_stage;
                let status = if selected {
                    CandidatePlanStageStatus::Selected
                } else if selection_reason.stops_before_later_stages()
                    && stage.priority() > selected_stage.priority()
                {
                    CandidatePlanStageStatus::Skipped
                } else {
                    CandidatePlanStageStatus::Insufficient
                };
                let reason_code = if selected {
                    selection_reason.selected_stage_reason()
                } else if matches!(status, CandidatePlanStageStatus::Skipped) {
                    "not_needed_after_selected_stage"
                } else {
                    insufficient_stage_reason(
                        stage,
                        candidates.len(),
                        minimum_candidate_count,
                        query,
                        area_hint_was_ignored,
                    )
                };

                CandidatePlanStageTrace {
                    stage: stage.clone(),
                    candidate_count: candidates.len(),
                    required_min_candidates: minimum_candidate_count,
                    status,
                    reason_code: reason_code.to_string(),
                }
            })
            .collect(),
    }
}

fn insufficient_stage_reason(
    stage: &FallbackStage,
    candidate_count: usize,
    minimum_candidate_count: usize,
    query: &RankingQuery,
    area_hint_was_ignored: bool,
) -> &'static str {
    let context = query.context.as_ref();
    // Legacy callers pass an explicit station through target_station_id while
    // leaving context unset, so None is still station-explicit for stage traces.
    let station_context_is_explicit = context
        .map(|context| context.station_id().is_some())
        .unwrap_or(true);
    let line_context_is_explicit = context
        .map(|context| context.line_name().is_some() || context.station_id().is_some())
        .unwrap_or(true);
    let has_city_context = context
        .filter(|_| !area_hint_was_ignored)
        .and_then(|context| context.city_name())
        .is_some();
    let has_prefecture_context = context
        .filter(|_| !area_hint_was_ignored)
        .and_then(|context| context.prefecture_name())
        .is_some();

    if candidate_count > 0 && candidate_count < minimum_candidate_count {
        return match stage {
            FallbackStage::SameLine => "line_graph_same_line_candidates_below_minimum",
            FallbackStage::NeighborArea => "area_graph_neighbor_candidates_below_minimum",
            _ => "candidate_count_below_minimum",
        };
    }

    match stage {
        FallbackStage::StrictStation if !station_context_is_explicit => {
            "station_context_not_explicit"
        }
        FallbackStage::SameLine if !line_context_is_explicit => "line_context_not_explicit",
        FallbackStage::SameLine => "line_graph_no_same_line_candidates",
        FallbackStage::SameCity | FallbackStage::SamePrefecture if area_hint_was_ignored => {
            "area_hint_ignored_by_station_conflict"
        }
        FallbackStage::SameCity if !has_city_context => "city_context_missing",
        FallbackStage::SamePrefecture if !has_prefecture_context => "prefecture_context_missing",
        FallbackStage::NeighborArea if !station_context_is_explicit => {
            "station_context_not_explicit"
        }
        FallbackStage::NeighborArea => "area_graph_no_neighbor_candidates_within_distance_cap",
        _ => "candidate_count_below_minimum",
    }
}

#[cfg(test)]
mod tests {
    use domain::{
        CandidatePlanStageStatus, FallbackStage, PlacementKind, RankingQuery, SchoolStationLink,
    };

    use super::{build_candidate_plan_trace, CandidatePlanSelectionReason};

    fn link(id: &str) -> SchoolStationLink {
        SchoolStationLink {
            school_id: id.to_string(),
            station_id: format!("st_{id}"),
            walking_minutes: 5,
            distance_meters: 400,
            hop_distance: 1,
            line_name: "Target Line".to_string(),
        }
    }

    fn query() -> RankingQuery {
        RankingQuery {
            target_station_id: "st_target".to_string(),
            limit: Some(3),
            user_id: None,
            placement: PlacementKind::Search,
            debug: false,
            context: None,
        }
    }

    #[test]
    fn best_available_trace_marks_later_evaluated_stages_insufficient() {
        let staged_candidates = vec![
            (FallbackStage::StrictStation, vec![link("strict")]),
            (
                FallbackStage::SameLine,
                vec![link("line_a"), link("line_b")],
            ),
            (FallbackStage::SafeGlobalPopular, vec![link("global")]),
        ];

        let trace = build_candidate_plan_trace(
            &staged_candidates,
            3,
            &FallbackStage::SameLine,
            CandidatePlanSelectionReason::BestAvailableNonEmpty,
            &query(),
            false,
            false,
        );
        let safe_global_stage = trace
            .stages
            .iter()
            .find(|stage| stage.stage == FallbackStage::SafeGlobalPopular)
            .expect("safe global stage");

        assert_eq!(trace.stop_reason, "best_available_non_empty_stage");
        assert_eq!(
            safe_global_stage.status,
            CandidatePlanStageStatus::Insufficient
        );
        assert_eq!(
            safe_global_stage.reason_code,
            "candidate_count_below_minimum"
        );
    }
}
