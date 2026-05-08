use serde_json::{json, Value};
use storage_postgres::{import_crawled_events, run_migrations, seed_fixture, EventCsvRecord};
use tokio_postgres::NoTls;

mod common;

use common::{create_empty_database, drop_database, repo_root};

#[tokio::test]
async fn import_crawled_events_persists_parser_details() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_event_details").await
    else {
        eprintln!(
            "skipping storage-postgres event details test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;
        seed_fixture(&database_url, root.join("storage/fixtures/minimal")).await?;

        let record = EventCsvRecord {
            event_id: "crawler_event_details".to_string(),
            school_id: "school_seaside".to_string(),
            title: "Crawler Details Event".to_string(),
            event_category: "open_campus".to_string(),
            is_open_day: true,
            is_featured: false,
            priority_weight: 0.25,
            starts_at: Some("2026-05-10T10:00:00+09:00".to_string()),
            placement_tags: "search|detail".to_string(),
            details: json!({
                "detail_url": "https://example.com/events/details",
                "apply_url": "https://example.com/events/apply"
            }),
        };
        import_crawled_events(&database_url, "test-crawl-source", &[record], true).await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(
                "SELECT details
                 FROM events
                 WHERE id = 'crawler_event_details'",
                &[],
            )
            .await?;
        let details = row.get::<_, Value>("details");

        assert_eq!(details["detail_url"], "https://example.com/events/details");
        assert_eq!(details["apply_url"], "https://example.com/events/apply");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
