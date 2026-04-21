use storage::{RecommendationRepository, SnapshotTuning};
use storage_postgres::{run_migrations, seed_fixture, PgRepository};
use tokio_postgres::NoTls;

mod common;

use common::{create_empty_database, drop_database, repo_root};

#[tokio::test]
async fn refresh_popularity_snapshots_counts_search_execute_by_station_and_area(
) -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_search_signal").await
    else {
        eprintln!(
            "skipping storage-postgres search signal test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;
        seed_fixture(&database_url, root.join("storage/fixtures/minimal")).await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .execute(
                "INSERT INTO user_events (
                    user_id,
                    event_type,
                    target_station_id,
                    occurred_at,
                    payload
                 )
                 VALUES
                    ('search-user-1', 'search_execute', 'st_tamachi', '2026-04-20T01:00:00Z', '{}'::jsonb),
                    ('search-user-2', 'search_execute', 'st_tamachi', '2026-04-20T02:00:00Z', '{}'::jsonb),
                    ('search-user-3', 'search_execute', 'st_shibuya', '2026-04-20T03:00:00Z', '{}'::jsonb)",
                &[],
            )
            .await?;

        let repository = PgRepository::new(&database_url);
        let stats = repository
            .refresh_popularity_snapshots(SnapshotTuning {
                search_execute_school_signal_weight: 0.4,
                search_execute_area_signal_weight: 0.2,
            })
            .await?;
        assert!(stats.refreshed_rows >= 1);
        assert!(stats.related_rows >= 1);

        let popularity_rows = client
            .query(
                "SELECT school_id, total_events, search_execute_count
                 FROM popularity_snapshots
                 WHERE school_id IN (
                    'school_seaside',
                    'school_garden',
                    'school_hillside',
                    'school_creative',
                    'school_aoyama_gakuin_junior'
                 )
                 ORDER BY school_id",
                &[],
            )
            .await?;
        let rendered_popularity = popularity_rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, String>("school_id"),
                    (
                        row.get::<_, i64>("total_events"),
                        row.get::<_, i64>("search_execute_count"),
                    ),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            rendered_popularity.get("school_seaside"),
            Some(&(3, 2))
        );
        assert_eq!(
            rendered_popularity.get("school_garden"),
            Some(&(2, 2))
        );
        assert_eq!(
            rendered_popularity.get("school_hillside"),
            Some(&(1, 0))
        );
        assert_eq!(
            rendered_popularity.get("school_creative"),
            Some(&(1, 1))
        );
        assert_eq!(
            rendered_popularity.get("school_aoyama_gakuin_junior"),
            Some(&(1, 1))
        );

        let area_rows = client
            .query(
                "SELECT area, event_count, search_execute_count
                 FROM area_affinity_snapshots
                 WHERE area IN ('Minato', 'Shibuya', 'Shiodome')
                 ORDER BY area",
                &[],
            )
            .await?;
        let rendered_areas = area_rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, String>("area"),
                    (
                        row.get::<_, i64>("event_count"),
                        row.get::<_, i64>("search_execute_count"),
                    ),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(rendered_areas.get("Minato"), Some(&(3, 2)));
        assert_eq!(rendered_areas.get("Shibuya"), Some(&(1, 1)));
        assert_eq!(rendered_areas.get("Shiodome"), Some(&(1, 0)));

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
