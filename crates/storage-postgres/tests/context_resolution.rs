use context::{AreaContextInput, ContextInput, ContextSource};
use storage_postgres::{run_migrations, seed_fixture, PgRepository};
use tokio_postgres::NoTls;

mod common;

use common::{create_empty_database, drop_database, repo_root};

#[tokio::test]
async fn area_context_resolves_without_raw_user_id_in_trace() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_context").await
    else {
        eprintln!(
            "skipping context resolution integration test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;
        seed_fixture(&database_url, root.join("storage/fixtures/minimal")).await?;

        let repo = PgRepository::new(&database_url);
        let context = repo
            .resolve_context(
                "req-context-area",
                Some("raw-user-id"),
                &ContextInput {
                    area: Some(AreaContextInput {
                        city_name: Some("Minato".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await?;

        assert_eq!(context.context_source, ContextSource::RequestArea);
        assert_eq!(context.city_name(), Some("Minato"));
        assert!(context.station.is_none());
        let target_station = repo
            .load_station_for_context(&context)
            .await?
            .expect("representative station");
        let candidate_links = repo
            .load_context_candidate_links(&target_station, &context, 20, 5_000.0, 2)
            .await?;
        assert!(candidate_links
            .iter()
            .all(|link| link.school_id != "school_creative"));

        let conflicted_context = repo
            .resolve_context(
                "req-context-station-conflict",
                None,
                &ContextInput {
                    station_id: Some("st_tamachi".to_string()),
                    area: Some(AreaContextInput {
                        city_name: Some("Shibuya".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await?;
        assert!(conflicted_context.area.is_none());
        assert_eq!(
            conflicted_context
                .line
                .as_ref()
                .and_then(|line| line.line_id.as_deref()),
            Some("line_jr_yamanote_line")
        );
        assert!(conflicted_context
            .warnings
            .iter()
            .any(|warning| warning.code == "station_area_conflict"));

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(
                "SELECT user_id_hash, context_source, area_id, line_id, station_id
                 FROM context_resolution_traces
                 WHERE request_id = 'req-context-area'",
                &[],
            )
            .await?;
        let user_id_hash = row.get::<_, Option<String>>("user_id_hash");
        assert!(user_id_hash
            .as_deref()
            .is_some_and(|value| value != "raw-user-id"));
        assert_eq!(
            row.get::<_, String>("context_source"),
            "request_area".to_string()
        );
        assert_eq!(
            row.get::<_, Option<String>>("area_id"),
            Some("area_minato".to_string())
        );
        assert_eq!(row.get::<_, Option<String>>("line_id"), None);
        assert_eq!(row.get::<_, Option<String>>("station_id"), None);
        let station_row = client
            .query_one(
                "SELECT line_id
                 FROM context_resolution_traces
                 WHERE request_id = 'req-context-station-conflict'",
                &[],
            )
            .await?;
        assert_eq!(
            station_row.get::<_, Option<String>>("line_id"),
            Some("line_jr_yamanote_line".to_string())
        );

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
