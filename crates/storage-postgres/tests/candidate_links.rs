use context::{
    AreaContext, ContextSource, LineContext, PrivacyLevel, RankingContext, StationContext,
};
use domain::Station;
use storage::{
    CandidatePlanAreaGraphExpansion, CandidatePlanGraphExpansion, CandidatePlanLineGraphExpansion,
};
use storage_postgres::{run_migrations, ContextCandidateLinkQuery, PgRepository};
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
                    line_id: None,
                    area_id: None,
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
                1,
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
            line_id: None,
            area_id: None,
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
            .load_context_candidate_links(&target_station, &context, 10, 1, 50.0, 1)
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
            line_id: None,
            area_id: None,
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
            .load_context_candidate_links(&target_station, &context, 10, 1, 2_500.0, 1)
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
async fn context_candidate_links_include_line_graph_adjacent_candidates() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_line_graph_candidates").await
    else {
        eprintln!(
            "skipping storage-postgres line graph candidate link test because PostgreSQL admin access is unavailable"
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
                    ('line_target', 'Target Line', 'JP'),
                    ('line_adjacent', 'Adjacent Line', 'JP');

                 INSERT INTO schools (id, name, area, school_type, group_id) VALUES
                    ('school_direct', 'Direct School', 'Target Ward', 'high_school', 'group_direct'),
                    ('school_adjacent', 'Adjacent School', 'Neighbor Ward', 'high_school', 'group_adjacent');

                 INSERT INTO stations (id, name, line_name, latitude, longitude, line_id) VALUES
                    ('st_target', 'Target', 'Target Line', 35.0, 139.0, 'line_target'),
                    ('st_adjacent', 'Adjacent', 'Adjacent Line', 35.0004, 139.0004, 'line_adjacent');

                 INSERT INTO line_adjacencies (
                    from_line_id, to_line_id, adjacency_kind, interchange_station_id,
                    station_hop_count, requires_transfer
                 ) VALUES (
                    'line_target', 'line_adjacent', 'interchange', 'st_target', 1, TRUE
                 );

                 INSERT INTO school_station_links
                    (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES
                    ('school_direct', 'st_target', 4, 250, 0, 'Target Line'),
                    ('school_adjacent', 'st_adjacent', 8, 650, 1, 'Adjacent Line');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let target_station = Station {
            id: "st_target".to_string(),
            name: "Target".to_string(),
            line_name: "Target Line".to_string(),
            line_id: Some("line_target".to_string()),
            area_id: None,
            latitude: 35.0,
            longitude: 139.0,
        };
        let context = RankingContext {
            context_source: ContextSource::RequestLine,
            confidence: 0.95,
            area: None,
            line: Some(LineContext {
                line_id: Some("line_target".to_string()),
                line_name: "Target Line".to_string(),
                operator_name: None,
            }),
            station: None,
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        };

        let candidate_links = repo
            .load_context_candidate_links(&target_station, &context, 10, 1, 1_000.0, 1)
            .await?;

        assert_eq!(
            candidate_links
                .iter()
                .map(|link| link.school_id.as_str())
                .collect::<Vec<_>>(),
            vec!["school_direct", "school_adjacent"]
        );

        let mismatched_expansion = CandidatePlanGraphExpansion {
            area: None,
            line: Some(CandidatePlanLineGraphExpansion {
                origin_line_id: "line_other".to_string(),
                adjacent_line_ids: vec!["line_adjacent".to_string()],
            }),
        };
        let guarded_links = repo
            .load_context_candidate_links_with_graph_expansion(
                ContextCandidateLinkQuery {
                    target_station: &target_station,
                    context: &context,
                    candidate_limit: 10,
                    min_scoped_candidates: 1,
                    neighbor_distance_cap_meters: 1_000.0,
                    neighbor_max_hops: 1,
                },
                &mismatched_expansion,
            )
            .await?;

        assert_eq!(guarded_links.len(), 1);
        assert_eq!(guarded_links[0].school_id, "school_direct");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn station_context_candidate_links_include_area_graph_neighbors() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_area_graph_candidates").await
    else {
        eprintln!(
            "skipping storage-postgres area graph candidate link test because PostgreSQL admin access is unavailable"
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
                "INSERT INTO areas (area_id, country_code, prefecture_name, city_name, area_level) VALUES
                    ('area_target', 'JP', 'Tokyo', 'Target Ward', 'city'),
                    ('area_neighbor', 'JP', 'Tokyo', 'Neighbor Ward', 'city');

                 INSERT INTO area_adjacencies (
                    from_area_id, to_area_id, adjacency_kind, distance_meters, area_cluster_id
                 ) VALUES (
                    'area_target', 'area_neighbor', 'city_neighbor', 1200.0, 'cluster_tokyo'
                 );

                 INSERT INTO schools (id, name, area, school_type, group_id) VALUES
                    ('school_direct', 'Direct School', 'Target Ward', 'high_school', 'group_direct'),
                    ('school_area_neighbor', 'Area Neighbor', 'Neighbor Ward', 'high_school', 'group_area_neighbor'),
                    ('school_school_area_only', 'School Area Only', 'Neighbor Ward', 'high_school', 'group_school_area_only');

                 INSERT INTO stations (id, name, line_name, latitude, longitude, area_id) VALUES
                    ('st_target', 'Target', 'Target Line', 35.0, 139.0, 'area_target'),
                    ('st_area_neighbor', 'Area Neighbor', 'Other Line', 36.0, 140.0, 'area_neighbor'),
                    ('st_school_area_only', 'School Area Only', 'Other Line', 36.2, 140.2, NULL);

                 INSERT INTO school_station_links
                    (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES
                    ('school_direct', 'st_target', 4, 250, 0, 'Target Line'),
                    ('school_area_neighbor', 'st_area_neighbor', 9, 800, 0, 'Other Line'),
                    ('school_school_area_only', 'st_school_area_only', 9, 800, 0, 'Other Line');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let target_station = Station {
            id: "st_target".to_string(),
            name: "Target".to_string(),
            line_name: "Target Line".to_string(),
            line_id: None,
            area_id: Some("area_target".to_string()),
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
            .load_context_candidate_links(&target_station, &context, 10, 1, 50.0, 1)
            .await?;

        assert_eq!(
            candidate_links
                .iter()
                .map(|link| link.school_id.as_str())
                .collect::<Vec<_>>(),
            vec!["school_direct", "school_area_neighbor"]
        );

        let mismatched_expansion = CandidatePlanGraphExpansion {
            area: Some(CandidatePlanAreaGraphExpansion {
                origin_area_id: "area_other".to_string(),
                adjacent_area_ids: vec!["area_neighbor".to_string()],
            }),
            line: None,
        };
        let guarded_links = repo
            .load_context_candidate_links_with_graph_expansion(
                ContextCandidateLinkQuery {
                    target_station: &target_station,
                    context: &context,
                    candidate_limit: 10,
                    min_scoped_candidates: 1,
                    neighbor_distance_cap_meters: 50.0,
                    neighbor_max_hops: 1,
                },
                &mismatched_expansion,
            )
            .await?;

        assert_eq!(guarded_links.len(), 1);
        assert_eq!(guarded_links[0].school_id, "school_direct");

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
            line_id: None,
            area_id: None,
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
            .load_context_candidate_links(&target_station, &context, 10, 1, 1_000.0, 2)
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
async fn city_context_station_lookup_honors_prefecture_name() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_city_station_pref").await
    else {
        eprintln!(
            "skipping storage-postgres city station prefecture test because PostgreSQL admin access is unavailable"
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
                    ('school_tokyo_fuchu', 'Tokyo Fuchu School', 'Fuchu', 'Tokyo', 'high_school', 'group_tokyo_fuchu'),
                    ('school_hiroshima_fuchu', 'Hiroshima Fuchu School', 'Fuchu', 'Hiroshima', 'high_school', 'group_hiroshima_fuchu');

                 INSERT INTO stations (id, name, line_name, latitude, longitude) VALUES
                    ('st_tokyo_fuchu', 'Tokyo Fuchu Station', 'Tokyo Line', 35.67, 139.48),
                    ('st_hiroshima_fuchu', 'Hiroshima Fuchu Station', 'Hiroshima Line', 34.57, 133.24);

                 INSERT INTO school_station_links
                    (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES
                    ('school_tokyo_fuchu', 'st_tokyo_fuchu', 1, 10, 0, 'Tokyo Line'),
                    ('school_hiroshima_fuchu', 'st_hiroshima_fuchu', 8, 700, 0, 'Hiroshima Line');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let context = RankingContext {
            context_source: ContextSource::RequestArea,
            confidence: 0.95,
            area: Some(AreaContext {
                country: "JP".to_string(),
                prefecture_code: None,
                prefecture_name: Some("Hiroshima".to_string()),
                city_code: None,
                city_name: Some("Fuchu".to_string()),
            }),
            line: None,
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

        assert_eq!(station.id, "st_hiroshima_fuchu");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn city_context_candidate_links_honor_prefecture_name() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_city_links_pref").await
    else {
        eprintln!(
            "skipping storage-postgres city candidate prefecture test because PostgreSQL admin access is unavailable"
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
                    ('school_tokyo_fuchu', 'Tokyo Fuchu School', 'Fuchu', 'Tokyo', 'high_school', 'group_tokyo_fuchu'),
                    ('school_hiroshima_fuchu', 'Hiroshima Fuchu School', 'Fuchu', 'Hiroshima', 'high_school', 'group_hiroshima_fuchu');

                 INSERT INTO stations (id, name, line_name, latitude, longitude) VALUES
                    ('st_target', 'Target Station', 'Target Line', 34.57, 133.24),
                    ('st_tokyo_fuchu', 'Tokyo Fuchu Station', 'Tokyo Line', 35.67, 139.48),
                    ('st_hiroshima_fuchu', 'Hiroshima Fuchu Station', 'Hiroshima Line', 34.5705, 133.2405);

                 INSERT INTO school_station_links
                    (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES
                    ('school_tokyo_fuchu', 'st_tokyo_fuchu', 1, 10, 0, 'Tokyo Line'),
                    ('school_hiroshima_fuchu', 'st_hiroshima_fuchu', 8, 700, 0, 'Hiroshima Line');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let target_station = Station {
            id: "st_target".to_string(),
            name: "Target Station".to_string(),
            line_name: "Target Line".to_string(),
            line_id: None,
            area_id: None,
            latitude: 34.57,
            longitude: 133.24,
        };
        let context = RankingContext {
            context_source: ContextSource::RequestArea,
            confidence: 0.95,
            area: Some(AreaContext {
                country: "JP".to_string(),
                prefecture_code: None,
                prefecture_name: Some("Hiroshima".to_string()),
                city_code: None,
                city_name: Some("Fuchu".to_string()),
            }),
            line: None,
            station: None,
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        };

        let candidate_links = repo
            .load_context_candidate_links(&target_station, &context, 10, 1, 1_000.0, 1)
            .await?;

        assert_eq!(
            candidate_links
                .iter()
                .map(|link| link.school_id.as_str())
                .collect::<Vec<_>>(),
            vec!["school_hiroshima_fuchu"]
        );

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
            line_id: None,
            area_id: None,
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
            .load_context_candidate_links(&target_station, &context, 1, 1, 1_000.0, 1)
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
async fn context_candidate_links_include_safe_global_when_scoped_filters_are_underfilled(
) -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("glr_underfilled_links").await
    else {
        eprintln!(
            "skipping storage-postgres underfilled safe-global link test because PostgreSQL admin access is unavailable"
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
                    ('school_strict', 'Strict School', 'Minato', 'Tokyo', 'high_school', 'group_strict'),
                    ('school_global', 'Global School', 'Naha', 'Okinawa', 'high_school', 'group_global');

                 INSERT INTO stations (id, name, line_name, latitude, longitude) VALUES
                    ('st_target', 'Target', 'Target Line', 35.0, 139.0),
                    ('st_global', 'Global Station', 'Global Line', 26.2124, 127.6792);

                 INSERT INTO school_station_links
                    (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES
                    ('school_strict', 'st_target', 8, 620, 0, 'Target Line'),
                    ('school_global', 'st_global', 4, 300, 0, 'Global Line');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let target_station = Station {
            id: "st_target".to_string(),
            name: "Target".to_string(),
            line_name: "Target Line".to_string(),
            line_id: None,
            area_id: None,
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
            .load_context_candidate_links(&target_station, &context, 10, 2, 1_000.0, 1)
            .await?;

        assert_eq!(
            candidate_links
                .iter()
                .map(|link| link.school_id.as_str())
                .collect::<Vec<_>>(),
            vec!["school_strict", "school_global"]
        );

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
                    ('school_near_global', 'Near Global School', 'Minato', 'Tokyo', 'high_school', 'group_near_global'),
                    ('school_far_short_walk', 'Far Short Walk School', 'Naha', 'Okinawa', 'high_school', 'group_far_short_walk');

                 INSERT INTO stations (id, name, line_name, latitude, longitude) VALUES
                    ('st_target', 'Target', 'Target Line', 35.0, 139.0),
                    ('st_near_global', 'Near Global Station', 'Global Line', 35.0005, 139.0005),
                    ('st_far_short_walk', 'Far Short Walk Station', 'Remote Line', 26.2124, 127.6792);

                 INSERT INTO school_station_links
                    (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES
                    ('school_near_global', 'st_near_global', 20, 900, 0, 'Global Line'),
                    ('school_far_short_walk', 'st_far_short_walk', 1, 20, 0, 'Remote Line');",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        let target_station = Station {
            id: "st_target".to_string(),
            name: "Target".to_string(),
            line_name: "Target Line".to_string(),
            line_id: None,
            area_id: None,
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
            .load_context_candidate_links(&target_station, &context, 1, 1, 1_000.0, 1)
            .await?;

        assert_eq!(candidate_links.len(), 1);
        assert_eq!(candidate_links[0].school_id, "school_near_global");

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
