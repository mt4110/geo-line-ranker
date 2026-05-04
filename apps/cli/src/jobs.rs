use anyhow::{ensure, Context, Result};
use config::AppSettings;
use storage::{JobType, NewJob, RecommendationRepository};
use storage_postgres::{JobInspection, JobMutationSummary, JobQueueSnapshot};

use crate::repository::pg_repository;

#[derive(Debug, Clone, PartialEq)]
pub struct JobEnqueueSummary {
    pub job_id: i64,
    pub job_type: JobType,
    pub payload: serde_json::Value,
    pub max_attempts: i32,
}

pub async fn run_job_list(settings: &AppSettings, limit: i64) -> Result<JobQueueSnapshot> {
    pg_repository(settings)?.list_jobs(limit).await
}

pub async fn run_job_inspect(settings: &AppSettings, job_id: i64) -> Result<JobInspection> {
    pg_repository(settings)?.inspect_job(job_id).await
}

pub async fn run_job_retry(settings: &AppSettings, job_id: i64) -> Result<JobMutationSummary> {
    pg_repository(settings)?.retry_failed_job(job_id).await
}

pub async fn run_job_due(settings: &AppSettings, job_id: i64) -> Result<JobMutationSummary> {
    pg_repository(settings)?.make_queued_job_due(job_id).await
}

pub async fn run_job_enqueue(
    settings: &AppSettings,
    job_type: &str,
    payload: &str,
    max_attempts: i32,
) -> Result<JobEnqueueSummary> {
    ensure!(max_attempts > 0, "max_attempts must be positive");
    let job_type =
        JobType::parse(job_type).with_context(|| format!("unsupported job_type {job_type}"))?;
    let payload: serde_json::Value =
        serde_json::from_str(payload).with_context(|| "failed to parse job payload JSON")?;
    ensure!(payload.is_object(), "job payload must be a JSON object");

    let repository = pg_repository(settings)?;
    let job_id = repository
        .enqueue_job(&NewJob {
            job_type,
            payload: payload.clone(),
            max_attempts,
        })
        .await?;

    Ok(JobEnqueueSummary {
        job_id,
        job_type,
        payload,
        max_attempts,
    })
}
