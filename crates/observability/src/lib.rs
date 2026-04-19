pub fn init_tracing(default_filter: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| default_filter.to_string()))
        .init();
}

pub fn cache_hit(key: &str) {
    tracing::info!(metric = "recommendation_cache", outcome = "hit", cache_key = %key);
}

pub fn cache_miss(key: &str) {
    tracing::info!(metric = "recommendation_cache", outcome = "miss", cache_key = %key);
}

pub fn cache_write(key: &str) {
    tracing::info!(metric = "recommendation_cache", outcome = "write", cache_key = %key);
}

pub fn candidate_retrieval_completed(
    mode: &str,
    backend: &str,
    candidate_count: usize,
    duration_ms: u128,
) {
    tracing::info!(
        metric = "candidate_retrieval",
        mode,
        backend,
        candidate_count,
        duration_ms
    );
}

pub fn job_started(worker_id: &str, job_id: i64, job_type: &str, attempt_number: i32) {
    tracing::info!(
        metric = "worker_job",
        outcome = "started",
        worker_id = worker_id,
        job_id,
        job_type,
        attempt_number
    );
}

pub fn job_succeeded(worker_id: &str, job_id: i64, job_type: &str) {
    tracing::info!(
        metric = "worker_job",
        outcome = "succeeded",
        worker_id = worker_id,
        job_id,
        job_type
    );
}

pub fn job_failed(worker_id: &str, job_id: i64, job_type: &str, error: &str) {
    tracing::error!(
        metric = "worker_job",
        outcome = "failed",
        worker_id = worker_id,
        job_id,
        job_type,
        error = error
    );
}
