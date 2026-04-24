use context::{
    AreaContext, ContextSource, LineContext, PrivacyLevel, RankingContext, StationContext,
};
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

#[tokio::test]
async fn context_candidate_links_use_line_id_when_available() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_context_links").await
    else {
        eprintln!(
            "skipping storage-postgres context candidate link test because PostgreSQL admin access is unavailable"
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
                "INSERT INTO lines (line_id, line_name, country_code) VALUES
                    ('line_target', 'Shared Line', 'JP'),
                    ('line_other', 'Shared Line', 'JP');

                 INSERT INTO schools (id, name, area, school_type, group_id) VALUES
                    ('school_target', 'Target School', 'Minato', 'high_school', 'group_target'),
                    ('school_other', 'Other School', 'Minato', 'high_school', 'group_other');

                 INSERT INTO stations (id, name, line_name, latitude, longitude, line_id) VALUES
                    ('st_target', 'Target', 'Shared Line', 35.0, 139.0, 'line_target'),
                    ('st_target_candidate', 'Target Candidate', 'Shared Line', 35.0, 139.0004, 'line_target'),
                    ('st_other_candidate', 'Other Candidate', 'Shared Line', 35.0, 139.0005, 'line_other');

                 INSERT INTO school_station_links
                    (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES
                    ('school_target', 'st_target_candidate', 6, 60, 1, 'Shared Line'),
                    ('school_other', 'st_other_candidate', 7, 70, 1, 'Shared Line');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let context = RankingContext {
            context_source: ContextSource::RequestLine,
            confidence: 0.95,
            area: None,
            line: Some(LineContext {
                line_id: Some("line_target".to_string()),
                line_name: "Shared Line".to_string(),
                operator_name: None,
            }),
            station: None,
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        };
        let representative_station = repo
            .load_station_for_context(&context)
            .await?
            .expect("line representative station");
        assert_eq!(representative_station.id, "st_target");

        let candidate_links = repo
            .load_context_candidate_links(
                &representative_station,
                &context,
                10,
                1_000.0,
                2,
            )
            .await?;

        assert_eq!(
            candidate_links
                .iter()
                .map(|link| link.school_id.as_str())
                .collect::<Vec<_>>(),
            vec!["school_target"]
        );

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn station_context_candidate_links_include_full_same_line_fallback() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_station_same_line").await
    else {
        eprintln!(
            "skipping storage-postgres station same-line link test because PostgreSQL admin access is unavailable"
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
                "INSERT INTO lines (line_id, line_name, country_code) VALUES
                    ('line_target', 'Same Line', 'JP');

                 INSERT INTO schools (id, name, area, school_type, group_id) VALUES
                    ('school_far_same_line', 'Far Same Line', 'Minato', 'high_school', 'group_far_same_line');

                 INSERT INTO stations (id, name, line_name, latitude, longitude, line_id) VALUES
                    ('st_target', 'Target', 'Same Line', 35.0, 139.0, 'line_target'),
                    ('st_far_same_line', 'Far Same Line Station', 'Same Line', 36.0, 140.0, 'line_target');

                 INSERT INTO school_station_links
                    (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES
                    ('school_far_same_line', 'st_far_same_line', 20, 120000, 9, 'Same Line');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let target_station = Station {
            id: "st_target".to_string(),
            name: "Target".to_string(),
            line_name: "Same Line".to_string(),
            latitude: 35.0,
            longitude: 139.0,
        };
        let context = RankingContext {
            context_source: ContextSource::RequestStation,
            confidence: 0.95,
            area: None,
            line: Some(LineContext {
                line_id: Some("line_target".to_string()),
                line_name: "Same Line".to_string(),
                operator_name: None,
            }),
            station: Some(StationContext {
                station_id: "st_target".to_string(),
                station_name: "Target".to_string(),
            }),
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        };
        let candidate_links = repo
            .load_context_candidate_links(&target_station, &context, 10, 50.0, 1)
            .await?;

        assert_eq!(candidate_links.len(), 1);
        assert_eq!(candidate_links[0].school_id, "school_far_same_line");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn station_context_candidate_links_include_nearby_off_line_candidates() -> anyhow::Result<()>
{
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_station_neighbor_area").await
    else {
        eprintln!(
            "skipping storage-postgres station neighbor-area link test because PostgreSQL admin access is unavailable"
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
                    ('school_neighbor', 'Neighbor School', 'Neighbor Ward', 'high_school', 'group_neighbor');

                 INSERT INTO stations (id, name, line_name, latitude, longitude) VALUES
                    ('st_target', 'Target', 'Target Line', 35.0, 139.0),
                    ('st_neighbor', 'Neighbor Station', 'Other Line', 35.0005, 139.0005);

                 INSERT INTO school_station_links
                    (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES
                    ('school_neighbor', 'st_neighbor', 8, 650, 0, 'Other Line');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let target_station = Station {
            id: "st_target".to_string(),
            name: "Target".to_string(),
            line_name: "Target Line".to_string(),
            latitude: 35.0,
            longitude: 139.0,
        };
        let context = RankingContext {
            context_source: ContextSource::RequestStation,
            confidence: 0.95,
            area: None,
            line: Some(LineContext {
                line_id: None,
                line_name: "Target Line".to_string(),
                operator_name: None,
            }),
            station: Some(StationContext {
                station_id: "st_target".to_string(),
                station_name: "Target".to_string(),
            }),
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        };

        let candidate_links = repo
            .load_context_candidate_links(&target_station, &context, 10, 2_500.0, 1)
            .await?;

        assert_eq!(candidate_links.len(), 1);
        assert_eq!(candidate_links[0].school_id, "school_neighbor");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn line_context_candidate_links_fall_back_to_line_name_when_station_line_id_is_missing(
) -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_line_name_fallback").await
    else {
        eprintln!(
            "skipping storage-postgres line-name fallback test because PostgreSQL admin access is unavailable"
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
                "INSERT INTO lines (line_id, line_name, country_code) VALUES
                    ('line_target', 'Legacy Shared Line', 'JP');

                 INSERT INTO schools (id, name, area, school_type, group_id) VALUES
                    ('school_legacy', 'Legacy School', 'Minato', 'high_school', 'group_legacy');

                 INSERT INTO stations (id, name, line_name, latitude, longitude, line_id) VALUES
                    ('st_target', 'Target', 'Legacy Shared Line', 35.0, 139.0, 'line_target'),
                    ('st_legacy', 'Legacy Candidate', 'Legacy Shared Line', 35.0, 139.0004, NULL);

                 INSERT INTO school_station_links
                    (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES
                    ('school_legacy', 'st_legacy', 6, 60, 1, 'Legacy Shared Line');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let target_station = Station {
            id: "st_target".to_string(),
            name: "Target".to_string(),
            line_name: "Legacy Shared Line".to_string(),
            latitude: 35.0,
            longitude: 139.0,
        };
        let context = RankingContext {
            context_source: ContextSource::RequestLine,
            confidence: 0.95,
            area: None,
            line: Some(LineContext {
                line_id: Some("line_target".to_string()),
                line_name: "Legacy Shared Line".to_string(),
                operator_name: None,
            }),
            station: None,
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        };

        let candidate_links = repo
            .load_context_candidate_links(&target_station, &context, 10, 1_000.0, 2)
            .await?;

        assert_eq!(candidate_links.len(), 1);
        assert_eq!(candidate_links[0].school_id, "school_legacy");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn line_context_station_lookup_falls_back_to_line_name_when_station_line_id_is_null(
) -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_line_station_fallback").await
    else {
        eprintln!(
            "skipping storage-postgres line fallback station test because PostgreSQL admin access is unavailable"
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
                "INSERT INTO lines (line_id, line_name, country_code)
                 VALUES
                    ('line_target', 'Fallback Line', 'JP'),
                    ('line_other', 'Fallback Line', 'JP');

                 INSERT INTO stations (id, name, line_name, latitude, longitude, line_id)
                 VALUES
                    ('st_line_name_only', 'Fallback Station', 'Fallback Line', 35.0, 139.0, NULL),
                    ('st_a_other_line', 'Other Line Station', 'Fallback Line', 35.0, 139.0, 'line_other');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let context = RankingContext {
            context_source: ContextSource::RequestLine,
            confidence: 0.95,
            area: None,
            line: Some(LineContext {
                line_id: Some("line_target".to_string()),
                line_name: "Fallback Line".to_string(),
                operator_name: None,
            }),
            station: None,
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        };

        let station = repo
            .load_station_for_context(&context)
            .await?
            .expect("representative station");

        assert_eq!(station.id, "st_line_name_only");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn prefecture_context_candidate_links_match_school_prefecture_name() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_prefecture_links").await
    else {
        eprintln!(
            "skipping storage-postgres prefecture candidate link test because PostgreSQL admin access is unavailable"
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
                "INSERT INTO schools (id, name, area, prefecture_name, school_type, group_id) VALUES
                    ('school_tokyo', 'Tokyo School', 'Minato', 'Tokyo', 'high_school', 'group_tokyo'),
                    ('school_osaka', 'Osaka School', 'Kita', 'Osaka', 'high_school', 'group_osaka');

                 INSERT INTO stations (id, name, line_name, latitude, longitude) VALUES
                    ('st_target', 'Target', 'Target Line', 35.0, 139.0),
                    ('st_tokyo', 'Tokyo Station', 'Tokyo Line', 35.1, 139.1),
                    ('st_osaka', 'Osaka Station', 'Osaka Line', 34.7, 135.5);

                 INSERT INTO school_station_links
                    (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES
                    ('school_tokyo', 'st_tokyo', 6, 60, 0, 'Tokyo Line'),
                    ('school_osaka', 'st_osaka', 7, 70, 0, 'Osaka Line');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let target_station = Station {
            id: "st_target".to_string(),
            name: "Target".to_string(),
            line_name: "Target Line".to_string(),
            latitude: 35.0,
            longitude: 139.0,
        };
        let context = RankingContext {
            context_source: ContextSource::RequestArea,
            confidence: 0.95,
            area: Some(AreaContext {
                country: "JP".to_string(),
                prefecture_code: None,
                prefecture_name: Some("Tokyo".to_string()),
                city_code: None,
                city_name: None,
            }),
            line: None,
            station: None,
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        };

        let candidate_links = repo
            .load_context_candidate_links(&target_station, &context, 1, 1_000.0, 1)
            .await?;

        assert_eq!(candidate_links.len(), 1);
        assert_eq!(candidate_links[0].school_id, "school_tokyo");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn context_candidate_links_include_safe_global_when_scoped_filters_are_empty(
) -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_safe_global_links").await
    else {
        eprintln!(
            "skipping storage-postgres safe-global candidate link test because PostgreSQL admin access is unavailable"
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
                "INSERT INTO schools (id, name, area, prefecture_name, school_type, group_id) VALUES
                    ('school_global', 'Global School', 'Minato', 'Tokyo', 'high_school', 'group_global');

                 INSERT INTO stations (id, name, line_name, latitude, longitude) VALUES
                    ('st_target', 'Target', 'Target Line', 35.0, 139.0),
                    ('st_global', 'Global Station', 'Global Line', 35.1, 139.1);

                 INSERT INTO school_station_links
                    (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES
                    ('school_global', 'st_global', 6, 60, 0, 'Global Line');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let target_station = Station {
            id: "st_target".to_string(),
            name: "Target".to_string(),
            line_name: "Target Line".to_string(),
            latitude: 35.0,
            longitude: 139.0,
        };
        let context = RankingContext {
            context_source: ContextSource::RequestArea,
            confidence: 0.95,
            area: Some(AreaContext {
                country: "JP".to_string(),
                prefecture_code: None,
                prefecture_name: None,
                city_code: None,
                city_name: Some("Nowhere".to_string()),
            }),
            line: None,
            station: None,
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        };

        let candidate_links = repo
            .load_context_candidate_links(&target_station, &context, 10, 1_000.0, 1)
            .await?;

        assert_eq!(candidate_links.len(), 1);
        assert_eq!(candidate_links[0].school_id, "school_global");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
