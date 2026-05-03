use std::collections::{BTreeMap, HashMap, HashSet};

use config::PlacementProfile;
use domain::{ContentKind, RecommendationItem};

use crate::scoring::ScoredCandidate;
use crate::RankingEngine;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DiversitySelectionSummary {
    pub(crate) selected_count: usize,
    pub(crate) same_school_skipped: usize,
    pub(crate) same_group_skipped: usize,
    pub(crate) content_kind_skipped: BTreeMap<ContentKind, usize>,
}

impl DiversitySelectionSummary {
    pub(crate) fn skipped_count(&self) -> usize {
        self.same_school_skipped
            + self.same_group_skipped
            + self.content_kind_skipped.values().sum::<usize>()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DiversitySelection {
    pub(crate) items: Vec<RecommendationItem>,
    pub(crate) summary: DiversitySelectionSummary,
}

impl RankingEngine {
    pub(crate) fn select_diverse_items(
        &self,
        candidates: Vec<ScoredCandidate>,
        limit: usize,
        placement_profile: &PlacementProfile,
    ) -> DiversitySelection {
        let mut school_counts = HashMap::<String, usize>::new();
        let mut group_counts = HashMap::<String, usize>::new();
        let mut kind_counts = BTreeMap::<ContentKind, usize>::new();
        let max_kind_counts = build_max_kind_counts(limit, placement_profile);
        let mut selected_keys = HashSet::<(ContentKind, String)>::new();
        let mut selected = Vec::new();
        let mut summary = DiversitySelectionSummary::default();

        for candidate in candidates {
            if selected.len() >= limit {
                break;
            }
            if !selected_keys.insert((candidate.content_kind, candidate.content_id.clone())) {
                continue;
            }
            if school_counts
                .get(candidate.school_id.as_str())
                .copied()
                .unwrap_or_default()
                >= placement_profile.diversity.same_school_cap
            {
                summary.same_school_skipped += 1;
                continue;
            }
            if group_counts
                .get(candidate.group_id.as_str())
                .copied()
                .unwrap_or_default()
                >= placement_profile.diversity.same_group_cap
            {
                summary.same_group_skipped += 1;
                continue;
            }
            if kind_counts
                .get(&candidate.content_kind)
                .copied()
                .unwrap_or_default()
                >= max_kind_counts
                    .get(&candidate.content_kind)
                    .copied()
                    .unwrap_or(limit)
            {
                *summary
                    .content_kind_skipped
                    .entry(candidate.content_kind)
                    .or_default() += 1;
                continue;
            }

            *school_counts
                .entry(candidate.school_id.clone())
                .or_default() += 1;
            *group_counts.entry(candidate.group_id.clone()).or_default() += 1;
            *kind_counts.entry(candidate.content_kind).or_default() += 1;
            selected.push(candidate.item);
        }

        summary.selected_count = selected.len();

        DiversitySelection {
            items: selected,
            summary,
        }
    }
}

fn build_max_kind_counts(
    limit: usize,
    placement_profile: &PlacementProfile,
) -> BTreeMap<ContentKind, usize> {
    placement_profile
        .mixed_ranking
        .enabled_content_kinds
        .iter()
        .map(|kind| {
            let max_count = placement_profile
                .diversity
                .content_kind_max_ratio
                .get(kind)
                .map(|ratio| ((limit as f64) * ratio).ceil() as usize)
                .map(|count| count.clamp(1, limit))
                .unwrap_or(limit);
            (*kind, max_count)
        })
        .collect()
}
