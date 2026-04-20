use domain::Station;
use storage_postgres::{run_migrations, PgRepository};
use tokio_postgres::NoTls;
mod common;

use common::{create_empty_database, drop_database, repo_root};

#[tokio::test]
async fn load_candidate_links_filters_neighbor_hops_before_limit() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_candidate_links").await
    else {
        eprintln!(
            "skipping storage-postgres candidate link test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        run_migrations(&database_url, repo_root().join("storage/migrations/postgres")).await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .batch_execute(
                "INSERT INTO schools (id, name, area, school_type, group_id) VALUES
                    ('school_far_a', 'Far A', 'Minato', 'high_school', 'group_far_a'),
                    ('school_far_b', 'Far B', 'Minato', 'high_school', 'group_far_b'),
                    ('school_in_hop', 'In Hop', 'Minato', 'high_school', 'group_in_hop');

                 INSERT INTO stations (id, name, line_name, latitude, longitude) VALUES
                    ('st_target', 'Target', 'JR Yamanote Line', 35.0, 139.0),
                    ('st_far_a', 'Far A Station', 'JR Yamanote Line', 35.0, 139.0004),
                    ('st_far_b', 'Far B Station', 'JR Yamanote Line', 35.0, 139.0005),
                    ('st_in_hop', 'In Hop Station', 'JR Yamanote Line', 35.0, 139.0012);

                 INSERT INTO school_station_links
                    (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES
                    ('school_far_a', 'st_far_a', 6, 60, 2, 'JR Yamanote Line'),
                    ('school_far_b', 'st_far_b', 7, 70, 3, 'JR Yamanote Line'),
                    ('school_in_hop', 'st_in_hop', 12, 120, 1, 'JR Yamanote Line');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let candidate_links = repo
            .load_candidate_links(
                &Station {
                    id: "st_target".to_string(),
                    name: "Target".to_string(),
                    line_name: "JR Yamanote Line".to_string(),
                    latitude: 35.0,
                    longitude: 139.0,
                },
                2,
                500.0,
                1,
            )
            .await?;

        assert_eq!(candidate_links.len(), 1);
        assert_eq!(candidate_links[0].school_id, "school_in_hop");
        assert_eq!(candidate_links[0].hop_distance, 1);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
