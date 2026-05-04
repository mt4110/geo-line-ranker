use anyhow::Result;
use domain::RankingQuery;

use crate::AppState;

pub(crate) fn build_recommendation_cache_key(
    state: &AppState,
    query: &RankingQuery,
) -> Result<String> {
    state.cache.build_key(
        &state.profile_version,
        &state.algorithm_version,
        state.candidate_retrieval_mode.as_str(),
        state.candidate_retrieval_limit,
        state.neighbor_distance_cap_meters,
        query,
    )
}
