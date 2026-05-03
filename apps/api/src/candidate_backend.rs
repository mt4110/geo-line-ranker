use anyhow::Result;

use crate::CandidateBackend;

impl CandidateBackend {
    pub(crate) async fn ready_check(&self) -> Result<String> {
        match self {
            Self::SqlOnly => Ok("disabled".to_string()),
            Self::Full(store) => {
                store.ready_check().await?;
                Ok("reachable".to_string())
            }
        }
    }
}

pub(crate) fn actual_candidate_backend_name(
    candidate_backend: &CandidateBackend,
    context: &context::RankingContext,
) -> &'static str {
    match candidate_backend {
        CandidateBackend::SqlOnly => "postgresql",
        CandidateBackend::Full(_) if should_use_opensearch_candidate_retrieval(context) => {
            "opensearch"
        }
        CandidateBackend::Full(_) => "postgresql",
    }
}

pub(crate) fn should_use_opensearch_candidate_retrieval(context: &context::RankingContext) -> bool {
    context.station_id().is_some() && context.area.is_none()
}
