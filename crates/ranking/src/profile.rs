use config::RankingProfiles;
use domain::PlacementKind;

use crate::RankingEngine;

impl RankingEngine {
    pub fn new(profiles: RankingProfiles, algorithm_version: impl Into<String>) -> Self {
        Self {
            profiles,
            algorithm_version: algorithm_version.into(),
        }
    }

    pub fn neighbor_max_hops(&self, placement: PlacementKind) -> u8 {
        self.profiles.placement(placement).neighbor_max_hops
    }

    pub fn minimum_candidate_count(&self) -> usize {
        self.profiles
            .schools
            .strict_min_candidates
            .max(self.profiles.fallback.min_results)
    }
}
