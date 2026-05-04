use storage::{JobType, NewJob};

use crate::AppState;

pub(crate) fn build_tracking_jobs(state: &AppState, event: &domain::UserEvent) -> Vec<NewJob> {
    let mut jobs = Vec::new();
    match event.event_kind {
        domain::EventKind::SearchExecute => {
            jobs.push(NewJob {
                job_type: JobType::RefreshPopularitySnapshot,
                payload: serde_json::json!({}),
                max_attempts: state.worker_max_attempts,
            });
        }
        _ if event.event_kind.is_school_affecting() && event.school_id.is_some() => {
            jobs.push(NewJob {
                job_type: JobType::RefreshPopularitySnapshot,
                payload: serde_json::json!({}),
                max_attempts: state.worker_max_attempts,
            });
            jobs.push(NewJob {
                job_type: JobType::RefreshUserAffinitySnapshot,
                payload: serde_json::json!({ "user_id": event.user_id.clone() }),
                max_attempts: state.worker_max_attempts,
            });
        }
        _ => return Vec::new(),
    }

    if state.cache.enabled() {
        jobs.push(NewJob {
            job_type: JobType::InvalidateRecommendationCache,
            payload: serde_json::json!({ "scope": "recommendations" }),
            max_attempts: state.worker_max_attempts,
        });
    }

    if state.candidate_retrieval_mode.is_full() {
        jobs.push(NewJob {
            job_type: JobType::SyncCandidateProjection,
            payload: serde_json::json!({ "scope": "full_rebuild" }),
            max_attempts: state.worker_max_attempts,
        });
    }

    jobs
}
