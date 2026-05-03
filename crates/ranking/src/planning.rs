use std::collections::BTreeMap;

use domain::{FallbackStage, RankingDataset, RankingQuery, RecommendationResult};

use crate::diversity::DiversitySelection;
use crate::explanation::build_top_level_explanation;
use crate::{RankingEngine, RankingError};

impl RankingEngine {
    pub fn recommend(
        &self,
        dataset: &RankingDataset,
        query: &RankingQuery,
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
            self.collect_candidates_by_stage(dataset, query, &target_station, placement_profile);
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
        let (fallback_stage, candidates) = first_sufficient_scoped_match
            .or(underfilled_area_match)
            .or(sufficient_safe_global_match)
            .or_else(|| {
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
            })
            .map(|(stage, candidates)| (stage.clone(), candidates.clone()))
            .unwrap_or_else(|| (FallbackStage::SafeGlobalPopular, Vec::new()));

        if candidates.is_empty() {
            return Err(RankingError::NoCandidates(target_station.id));
        }

        let scored_candidates = self.score_candidates(
            dataset,
            query,
            &target_station,
            placement_profile,
            candidates,
            &fallback_stage,
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
            context: query.context.clone(),
        })
    }
}
