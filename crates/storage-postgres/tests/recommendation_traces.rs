use serde_json::json;
use storage_postgres::{run_migrations, PgRepository};
use tokio_postgres::NoTls;

mod common;

use common::{create_empty_database, drop_database, repo_root};

#[tokio::test]
async fn load_recommendation_trace_reads_full_persisted_payload() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("glr_recommendation_trace").await
    else {
        eprintln!(
            "skipping recommendation trace read test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        run_migrations(
            &database_url,
            repo_root().join("storage/migrations/postgres"),
        )
        .await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let request_payload = json!({
            "request_id": "req-trace-read",
            "target_station_id": "st_tamachi",
            "placement": "search",
            "debug": false
        });
        let response_payload = json!({
            "request_id": "req-trace-read",
            "items": [],
            "fallback_stage": "same_line",
            "candidate_counts": {},
            "profile_version": "test",
            "algorithm_version": "test",
            "score_breakdown": [],
            "explanation": "test"
        });
        let trace_payload = json!({
            "response_source": "fresh",
            "candidate_retrieval": {
                "mode": "sql_only",
                "backend": "postgres",
                "candidate_count": 7
            }
        });
        let row = client
            .query_one(
                "INSERT INTO recommendation_traces (
                    request_payload,
                    response_payload,
                    trace_payload,
                    fallback_stage,
                    algorithm_version
                ) VALUES ($1, $2, $3, $4, $5)
                RETURNING id",
                &[
                    &request_payload,
                    &response_payload,
                    &trace_payload,
                    &"same_line",
                    &"trace-read-test",
                ],
            )
            .await?;
        let trace_id = row.get::<_, i64>("id");

        let repository = PgRepository::new(&database_url);
        let trace = repository
            .load_recommendation_trace(trace_id)
            .await?
            .expect("inserted recommendation trace");

        assert_eq!(trace.id, trace_id);
        assert_eq!(trace.request_payload, request_payload);
        assert_eq!(trace.response_payload, response_payload);
        assert_eq!(trace.trace_payload, trace_payload);
        assert_eq!(trace.fallback_stage, "same_line");
        assert_eq!(trace.algorithm_version, "trace-read-test");
        assert!(trace.created_at.ends_with('Z'));
        assert!(repository
            .load_recommendation_trace(trace_id + 1)
            .await?
            .is_none());

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
