use config::RankingProfiles;
use domain::PlacementKind;

use crate::{RankingEngine, ReasonCatalog, ReasonCatalogError};

impl RankingEngine {
    pub fn new(profiles: RankingProfiles, algorithm_version: impl Into<String>) -> Self {
        Self {
            profiles,
            algorithm_version: algorithm_version.into(),
            reason_catalog: ReasonCatalog::default_core(),
        }
    }

    pub fn with_profile_reason_catalog(
        mut self,
        catalog: &config::ProfileReasonCatalog,
    ) -> Result<Self, ReasonCatalogError> {
        self.reason_catalog = ReasonCatalog::from_profile_catalog(catalog)?;
        Ok(self)
    }

    pub fn with_reason_catalog(mut self, reason_catalog: ReasonCatalog) -> Self {
        self.reason_catalog = reason_catalog;
        self
    }

    pub fn reason_catalog(&self) -> &ReasonCatalog {
        &self.reason_catalog
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
