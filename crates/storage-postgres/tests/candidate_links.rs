use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use domain::Station;
use storage_postgres::{run_migrations, PgRepository};
use tokio_postgres::NoTls;

fn default_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@127.0.0.1:5433/geo_line_ranker".to_string()
    })
}

fn database_url_with_name(database_url: &str, database_name: &str) -> String {
    let Some((prefix, _)) = database_url.rsplit_once('/') else {
        return database_url.to_string();
    };
    format!("{prefix}/{database_name}")
}

fn unique_database_name(prefix: &str) -> String {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), suffix)
}

async fn create_empty_database(prefix: &str) -> anyhow::Result<(String, String, String)> {
    let admin_database_url = database_url_with_name(&default_database_url(), "postgres");
    let database_name = unique_database_name(prefix);
    let (client, connection) = tokio_postgres::connect(&admin_database_url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query(&format!("CREATE DATABASE \"{database_name}\""))
        .await?;

    Ok((
        admin_database_url,
        database_url_with_name(&default_database_url(), &database_name),
        database_name,
    ))
}

async fn drop_database(admin_database_url: &str, database_name: &str) -> anyhow::Result<()> {
    let (client, connection) = tokio_postgres::connect(admin_database_url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .query(
            "SELECT pg_terminate_backend(pid)
             FROM pg_stat_activity
             WHERE datname = $1
               AND pid <> pg_backend_pid()",
            &[&database_name],
        )
        .await?;
    client
        .simple_query(&format!("DROP DATABASE IF EXISTS \"{database_name}\""))
        .await?;
    Ok(())
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

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
