use std::{fs, path::Path};

use context::AreaContext;
use serde_json::json;
use storage::{
    AreaClusterDiagnostic, GraphAdjacencyRepository, InterchangeDiagnostic,
    RecommendationRepository, SessionContextSummaryRepository, StationHopDiagnostic,
};
use storage_postgres::{run_migrations, seed_fixture, PgRepository};
use tokio_postgres::{error::SqlState, NoTls};
mod common;

use common::{create_empty_database, drop_database, repo_root};

async fn apply_legacy_migrations_through_0007(
    database_url: &str,
    migrations_dir: &Path,
) -> anyhow::Result<()> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
        )
        .await?;

    let mut entries = std::fs::read_dir(migrations_dir)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sql") {
            continue;
        }
        let version = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow::anyhow!("migration file name must be valid UTF-8"))?
            .to_string();
        if version.starts_with("0008_") {
            break;
        }
        let sql = std::fs::read_to_string(&path)?;
        client.batch_execute(&sql).await?;
        client
            .execute(
                "INSERT INTO schema_migrations (version) VALUES ($1)",
                &[&version],
            )
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn run_migrations_is_safe_when_called_concurrently() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_migrations").await
    else {
        eprintln!(
            "skipping storage-postgres migration concurrency test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let migrations_dir = repo_root().join("storage/migrations/postgres");
        let expected_count = std::fs::read_dir(&migrations_dir)?
            .collect::<std::io::Result<Vec<_>>>()?
            .into_iter()
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("sql"))
            .count() as i64;

        let database_url_a = database_url.clone();
        let database_url_b = database_url.clone();
        let migrations_dir_a = migrations_dir.clone();
        let migrations_dir_b = migrations_dir.clone();
        let first =
            tokio::spawn(async move { run_migrations(&database_url_a, &migrations_dir_a).await });
        let second =
            tokio::spawn(async move { run_migrations(&database_url_b, &migrations_dir_b).await });

        first.await??;
        second.await??;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one("SELECT COUNT(*) AS count FROM schema_migrations", &[])
            .await?;
        assert_eq!(row.get::<_, i64>("count"), expected_count);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn graph_adjacency_tables_support_reference_reads() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_graph_adjacencies").await
    else {
        eprintln!(
            "skipping storage-postgres graph adjacency test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "INSERT INTO areas (area_id, country_code, prefecture_name, city_name, area_level)
                 VALUES
                    ('area_tokyo_minato', 'JP', 'Tokyo', 'Minato', 'city'),
                    ('area_tokyo_shinagawa', 'JP', 'Tokyo', 'Shinagawa', 'city');

                 INSERT INTO lines (line_id, line_name, country_code, source_id, source_version)
                 VALUES
                    ('line_yamanote', 'JR Yamanote Line', 'JP', 'fixture', '2026-05-13'),
                    ('line_keihin_tohoku', 'JR Keihin-Tohoku Line', 'JP', 'fixture', '2026-05-13');

                 INSERT INTO stations (id, name, line_name, latitude, longitude, line_id, area_id)
                 VALUES
                    ('st_shinagawa_yamanote', 'Shinagawa', 'JR Yamanote Line', 35.6285, 139.7388, 'line_yamanote', 'area_tokyo_minato');

                 INSERT INTO area_adjacencies (
                    from_area_id,
                    to_area_id,
                    adjacency_kind,
                    distance_meters,
                    area_cluster_id,
                    source_id,
                    source_version,
                    attributes
                 )
                 VALUES (
                    'area_tokyo_minato',
                    'area_tokyo_shinagawa',
                    'city_neighbor',
                    1250.0,
                    'cluster_tokyo_bay',
                    'fixture',
                    '2026-05-13',
                    '{\"rank\": 1}'::jsonb
                 );

                 INSERT INTO line_adjacencies (
                    from_line_id,
                    to_line_id,
                    adjacency_kind,
                    interchange_station_id,
                    station_hop_count,
                    requires_transfer,
                    source_id,
                    source_version,
                    attributes
                 )
                 VALUES (
                    'line_yamanote',
                    'line_keihin_tohoku',
                    'interchange',
                    'st_shinagawa_yamanote',
                    0,
                    TRUE,
                    'fixture',
                    '2026-05-13',
                    '{\"interchange_name\": \"Shinagawa\"}'::jsonb
                 );",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        repo.ready_check().await?;

        assert_eq!(
            repo.load_station_area_id("st_shinagawa_yamanote").await?,
            Some("area_tokyo_minato".to_string())
        );
        assert_eq!(
            repo.load_area_id_for_context_area(&AreaContext {
                country: "JP".to_string(),
                prefecture_code: None,
                prefecture_name: Some("Tokyo".to_string()),
                city_code: None,
                city_name: Some("Minato".to_string()),
            })
            .await?,
            Some("area_tokyo_minato".to_string())
        );

        let area_edges = repo.load_area_adjacencies("area_tokyo_minato").await?;
        assert_eq!(area_edges.len(), 1);
        assert_eq!(area_edges[0].to_area_id, "area_tokyo_shinagawa");
        assert_eq!(area_edges[0].adjacency_kind, "city_neighbor");
        assert_eq!(
            area_edges[0].area_cluster_id.as_deref(),
            Some("cluster_tokyo_bay")
        );
        assert_eq!(area_edges[0].attributes, json!({ "rank": 1 }));

        let geo_graph = repo.load_geo_graph("area_tokyo_minato").await?;
        assert_eq!(geo_graph.origin_area_id(), "area_tokyo_minato");
        assert_eq!(geo_graph.adjacent_area_ids(), vec!["area_tokyo_shinagawa"]);
        assert_eq!(
            geo_graph.area_cluster_diagnostics(),
            vec![AreaClusterDiagnostic {
                area_cluster_id: "cluster_tokyo_bay".to_string(),
                observed_area_ids: vec![
                    "area_tokyo_minato".to_string(),
                    "area_tokyo_shinagawa".to_string()
                ],
            }]
        );

        for invalid_distance in ["'Infinity'::double precision", "'NaN'::double precision"] {
            let statement = format!(
                "INSERT INTO area_adjacencies (
                    from_area_id,
                    to_area_id,
                    adjacency_kind,
                    distance_meters
                 )
                 VALUES (
                    'area_tokyo_minato',
                    'area_tokyo_shinagawa',
                    'invalid_distance',
                    {invalid_distance}
                 )"
            );
            let error = client
                .execute(statement.as_str(), &[])
                .await
                .expect_err("non-finite area adjacency distance should be rejected");
            let db_error = error
                .as_db_error()
                .unwrap_or_else(|| panic!("unexpected non-DB error for {invalid_distance}: {error}"));
            assert_eq!(db_error.code(), &SqlState::CHECK_VIOLATION);
        }

        let line_edges = repo.load_line_adjacencies("line_yamanote").await?;
        assert_eq!(line_edges.len(), 1);
        assert_eq!(line_edges[0].to_line_id, "line_keihin_tohoku");
        assert_eq!(line_edges[0].adjacency_kind, "interchange");
        assert_eq!(
            line_edges[0].interchange_station_id.as_deref(),
            Some("st_shinagawa_yamanote")
        );
        assert!(line_edges[0].requires_transfer);
        assert_eq!(
            line_edges[0].attributes,
            json!({ "interchange_name": "Shinagawa" })
        );

        let line_graph = repo.load_line_graph("line_yamanote").await?;
        assert_eq!(line_graph.origin_line_id(), "line_yamanote");
        assert_eq!(
            line_graph.adjacent_line_ids(),
            vec!["line_keihin_tohoku"]
        );
        assert_eq!(
            line_graph.station_hop_diagnostics(),
            vec![StationHopDiagnostic {
                from_line_id: "line_yamanote".to_string(),
                to_line_id: "line_keihin_tohoku".to_string(),
                adjacency_kind: "interchange".to_string(),
                station_hop_count: Some(0),
                interchange_station_id: Some("st_shinagawa_yamanote".to_string()),
                requires_transfer: true,
            }]
        );
        assert_eq!(
            line_graph.interchange_diagnostics(),
            vec![InterchangeDiagnostic {
                interchange_station_id: "st_shinagawa_yamanote".to_string(),
                from_line_id: "line_yamanote".to_string(),
                to_line_ids: vec!["line_keihin_tohoku".to_string()],
                adjacency_kinds: vec!["interchange".to_string()],
                requires_transfer: true,
                minimum_station_hop_count: Some(0),
            }]
        );

        client
            .execute(
                "DELETE FROM stations WHERE id = 'st_shinagawa_yamanote'",
                &[],
            )
            .await?;
        assert!(
            repo.load_line_adjacencies("line_yamanote")
                .await?
                .is_empty()
        );

        let error = repo
            .load_area_adjacencies(" ")
            .await
            .expect_err("blank area_id should be rejected");
        assert!(error.to_string().contains("area_id must not be empty"));

        let error = repo
            .load_line_adjacencies(" ")
            .await
            .expect_err("blank line_id should be rejected");
        assert!(error.to_string().contains("line_id must not be empty"));

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn session_context_summary_table_supports_reference_reads() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_session_context").await
    else {
        eprintln!(
            "skipping storage-postgres session context summary test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "INSERT INTO session_context_summaries (
                    session_id_hash,
                    context_source,
                    confidence,
                    privacy_level,
                    primary_kind,
                    evidence_count,
                    search_execute_count,
                    warning_count,
                    area_id,
                    line_id,
                    station_id,
                    summary_payload,
                    first_seen_at,
                    last_seen_at,
                    updated_at
                 )
                 VALUES
                    (
                        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                        'recent_search_context',
                        0.75,
                        'coarse_area',
                        'search_execute',
                        2,
                        1,
                        0,
                        'area_tokyo_minato',
                        'line_yamanote',
                        'st_tamachi',
                        '{\"evidence_age_bucket\": \"recent\"}'::jsonb,
                        TIMESTAMPTZ '2026-05-13 00:00:00+00',
                        TIMESTAMPTZ '2026-05-13 00:05:00+00',
                        TIMESTAMPTZ '2026-05-13 00:05:01+00'
                    ),
                    (
                        'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                        'request_area',
                        0.90,
                        'coarse_area',
                        'request_area',
                        1,
                        0,
                        0,
                        'area_tokyo_shinagawa',
                        NULL,
                        NULL,
                        '{\"evidence_age_bucket\": \"fresh\"}'::jsonb,
                        TIMESTAMPTZ '2026-05-13 00:10:00+00',
                        TIMESTAMPTZ '2026-05-13 00:12:00+00',
                        TIMESTAMPTZ '2026-05-13 00:12:01+00'
                    );",
            )
            .await?;

        let repo = PgRepository::new(&database_url);
        repo.ready_check().await?;

        let summary = repo
            .load_session_context_summary(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .await?
            .expect("inserted session context summary");
        assert_eq!(summary.context_source, "recent_search_context");
        assert_eq!(summary.primary_kind, "search_execute");
        assert_eq!(summary.search_execute_count, 1);
        assert_eq!(summary.station_id.as_deref(), Some("st_tamachi"));
        assert_eq!(
            summary.summary_payload,
            json!({ "evidence_age_bucket": "recent" })
        );
        assert_eq!(summary.last_seen_at, "2026-05-13T00:05:00.000000Z");

        let recent = repo.list_recent_session_context_summaries(1).await?;
        assert_eq!(recent.len(), 1);
        assert_eq!(
            recent[0].session_id_hash,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );

        let missing = repo
            .load_session_context_summary(
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            )
            .await?;
        assert!(missing.is_none());

        let error = repo
            .load_session_context_summary(" ")
            .await
            .expect_err("blank session id hash should be rejected");
        assert!(error
            .to_string()
            .contains("session_id_hash must not be empty"));

        let error = repo
            .load_session_context_summary("not-a-hash")
            .await
            .expect_err("malformed session id hash should be rejected");
        assert!(error
            .to_string()
            .contains("session_id_hash must be a 64-character hex digest"));

        let error = client
            .execute(
                "INSERT INTO session_context_summaries (
                    session_id_hash,
                    context_source,
                    confidence,
                    privacy_level,
                    primary_kind,
                    summary_payload,
                    first_seen_at,
                    last_seen_at
                 )
                 VALUES (
                    'not-a-hash',
                    'request_area',
                    0.50,
                    'coarse_area',
                    'request_area',
                    '{}'::jsonb,
                    NOW(),
                    NOW()
                 )",
                &[],
            )
            .await
            .expect_err("raw or malformed session ids should be rejected");
        let db_error = error
            .as_db_error()
            .unwrap_or_else(|| panic!("unexpected non-DB error for malformed session id: {error}"));
        assert_eq!(db_error.code(), &SqlState::CHECK_VIOLATION);

        for invalid_confidence in ["'Infinity'::double precision", "'NaN'::double precision"] {
            let statement = format!(
                "INSERT INTO session_context_summaries (
                    session_id_hash,
                    context_source,
                    confidence,
                    privacy_level,
                    primary_kind,
                    summary_payload,
                    first_seen_at,
                    last_seen_at
                 )
                 VALUES (
                    'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
                    'request_area',
                    {invalid_confidence},
                    'coarse_area',
                    'request_area',
                    '{{}}'::jsonb,
                    NOW(),
                    NOW()
                 )"
            );
            let error = client
                .execute(statement.as_str(), &[])
                .await
                .expect_err("non-finite session context confidence should be rejected");
            let db_error = error.as_db_error().unwrap_or_else(|| {
                panic!("unexpected non-DB error for {invalid_confidence}: {error}")
            });
            assert_eq!(db_error.code(), &SqlState::CHECK_VIOLATION);
        }

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn seed_fixture_is_idempotent_for_user_events() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_seed_fixture").await
    else {
        eprintln!(
            "skipping storage-postgres seed idempotency test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;

        let fixture_dir = root.join("storage/fixtures/minimal");
        let expected_user_event_count =
            std::fs::read_to_string(fixture_dir.join("user_events.ndjson"))?
                .lines()
                .filter(|line| !line.trim().is_empty())
                .count() as i64;

        seed_fixture(&database_url, &fixture_dir).await?;
        seed_fixture(&database_url, &fixture_dir).await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one("SELECT COUNT(*) AS count FROM user_events", &[])
            .await?;
        assert_eq!(row.get::<_, i64>("count"), expected_user_event_count);

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn seed_fixture_keeps_same_city_areas_per_prefecture() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("glr_city_area_ids").await
    else {
        eprintln!(
            "skipping storage-postgres city area seed test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let root = repo_root();
        run_migrations(&database_url, root.join("storage/migrations/postgres")).await?;

        let fixture_dir = tempfile::tempdir()?;
        fs::write(
            fixture_dir.path().join("stations.csv"),
            "station_id,name,line_name,latitude,longitude\nst_target,Target,Target Line,35.0,139.0\n",
        )?;
        fs::write(
            fixture_dir.path().join("schools.csv"),
            "school_id,name,area,prefecture_name,school_type,group_id\n\
             school_tokyo_fuchu,Tokyo Fuchu,Fuchu,Tokyo,high_school,group_tokyo\n\
             school_hiroshima_fuchu,Hiroshima Fuchu,Fuchu,Hiroshima,high_school,group_hiroshima\n",
        )?;
        fs::write(
            fixture_dir.path().join("events.csv"),
            "event_id,school_id,title,event_category,is_open_day,is_featured,priority_weight,starts_at,placement_tags\n",
        )?;
        fs::write(
            fixture_dir.path().join("school_station_links.csv"),
            "school_id,station_id,walking_minutes,distance_meters,hop_distance,line_name\n",
        )?;
        fs::write(fixture_dir.path().join("user_events.ndjson"), "")?;

        seed_fixture(&database_url, fixture_dir.path()).await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let rows = client
            .query(
                "SELECT area_id, prefecture_name, city_name
                 FROM areas
                 WHERE area_level = 'city'
                   AND city_name = 'Fuchu'
                 ORDER BY prefecture_name",
                &[],
            )
            .await?;
        let rendered = rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, String>("area_id"),
                    row.get::<_, Option<String>>("prefecture_name"),
                    row.get::<_, Option<String>>("city_name"),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rendered,
            vec![
                (
                    "area_hiroshima_fuchu".to_string(),
                    Some("Hiroshima".to_string()),
                    Some("Fuchu".to_string())
                ),
                (
                    "area_tokyo_fuchu".to_string(),
                    Some("Tokyo".to_string()),
                    Some("Fuchu".to_string())
                ),
            ]
        );

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}

#[tokio::test]
async fn v020_migration_preserves_date_only_starts_at_as_utc_midnight() -> anyhow::Result<()> {
    let Ok((admin_database_url, database_url, database_name)) =
        create_empty_database("geo_line_ranker_legacy_starts_at").await
    else {
        eprintln!(
            "skipping storage-postgres legacy starts_at migration test because PostgreSQL admin access is unavailable"
        );
        return Ok(());
    };

    let test_result = async {
        let (admin_client, admin_connection) =
            tokio_postgres::connect(&admin_database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = admin_connection.await;
        });
        admin_client
            .simple_query(&format!(
                "ALTER DATABASE \"{database_name}\" SET timezone TO 'Asia/Tokyo'"
            ))
            .await?;

        let migrations_dir = repo_root().join("storage/migrations/postgres");
        apply_legacy_migrations_through_0007(&database_url, &migrations_dir).await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "INSERT INTO schools (id, name, area, school_type, group_id)
                 VALUES ('school_legacy', 'Legacy School', 'Minato', 'high_school', 'group_legacy');

                 INSERT INTO events (id, school_id, title, starts_at)
                 VALUES
                    ('event_legacy_date', 'school_legacy', 'Legacy Open Day', '2026-04-22'),
                    ('event_legacy_invalid', 'school_legacy', 'Legacy Broken Date', 'not-a-time');

                 INSERT INTO user_events (user_id, school_id, event_type, occurred_at)
                 VALUES
                    ('user_date', 'school_legacy', 'school_view', '2026-04-22'),
                    ('user_invalid', 'school_legacy', 'school_view', 'not-a-time');",
            )
            .await?;
        drop(client);

        run_migrations(&database_url, migrations_dir).await?;

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(
                "SELECT to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS utc_start
                 FROM events
                 WHERE id = 'event_legacy_date'",
                &[],
            )
            .await?;
        assert_eq!(row.get::<_, String>("utc_start"), "2026-04-22 00:00:00");
        let invalid_event_row = client
            .query_one(
                "SELECT starts_at IS NULL AS starts_at_is_null
                 FROM events
                 WHERE id = 'event_legacy_invalid'",
                &[],
            )
            .await?;
        assert!(invalid_event_row.get::<_, bool>("starts_at_is_null"));
        let user_event_rows = client
            .query(
                "SELECT user_id, to_char(occurred_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS utc_occurred
                 FROM user_events
                 WHERE user_id IN ('user_date', 'user_invalid')
                 ORDER BY user_id",
                &[],
            )
            .await?;
        let occurred = user_event_rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, String>("user_id"),
                    row.get::<_, String>("utc_occurred"),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            occurred,
            vec![
                (
                    "user_date".to_string(),
                    "2026-04-22 00:00:00".to_string()
                ),
                (
                    "user_invalid".to_string(),
                    "1970-01-01 00:00:00".to_string()
                ),
            ]
        );
        let school_row = client
            .query_one(
                "SELECT prefecture_name IS NULL AS prefecture_name_is_null
                 FROM schools
                 WHERE id = 'school_legacy'",
                &[],
            )
            .await?;
        assert!(school_row.get::<_, bool>("prefecture_name_is_null"));

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
