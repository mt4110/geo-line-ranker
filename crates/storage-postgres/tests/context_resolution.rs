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
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "UPDATE areas
                 SET prefecture_code = '13',
                     prefecture_name = 'Tokyo',
                     city_code = '13103'
                 WHERE area_id = 'area_minato';

                 INSERT INTO areas (
                    area_id,
                    country_code,
                    prefecture_code,
                    prefecture_name,
                    area_level
                 )
                 VALUES ('area_tokyo', 'JP', '13', 'Tokyo', 'prefecture')
                 ON CONFLICT (area_id) DO UPDATE
                 SET prefecture_code = EXCLUDED.prefecture_code,
                     prefecture_name = EXCLUDED.prefecture_name,
                     area_level = EXCLUDED.area_level;

                 INSERT INTO areas (
                    area_id,
                    country_code,
                    city_code,
                    city_name,
                    area_level
                 )
                 VALUES ('area_minato_station_sparse', 'JP', '13103', 'Minato', 'city')
                 ON CONFLICT (area_id) DO UPDATE
                 SET city_code = EXCLUDED.city_code,
                     city_name = EXCLUDED.city_name,
                     area_level = EXCLUDED.area_level;

                 UPDATE stations
                 SET area_id = 'area_minato_station_sparse'
                 WHERE id = 'st_tamachi';",
            )
            .await?;

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
        let matching_prefecture_context = repo
            .resolve_context(
                "req-context-station-prefecture-match",
                None,
                &ContextInput {
                    station_id: Some("st_tamachi".to_string()),
                    area: Some(AreaContextInput {
                        city_name: Some("Minato".to_string()),
                        prefecture_name: Some("Tokyo".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(matching_prefecture_context.city_name(), Some("Minato"));
        assert_eq!(matching_prefecture_context.prefecture_name(), Some("Tokyo"));
        assert!(matching_prefecture_context
            .warnings
            .iter()
            .all(|warning| warning.code != "station_area_conflict"));
        let mixed_conflicted_context = repo
            .resolve_context(
                "req-context-station-prefecture-conflict",
                None,
                &ContextInput {
                    station_id: Some("st_tamachi".to_string()),
                    area: Some(AreaContextInput {
                        city_name: Some("Minato".to_string()),
                        prefecture_name: Some("Osaka".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await?;
        assert!(mixed_conflicted_context.area.is_none());
        assert!(mixed_conflicted_context
            .warnings
            .iter()
            .any(|warning| warning.code == "station_area_conflict"));
        let trimmed_line_context = repo
            .resolve_context(
                "req-context-line-trimmed",
                None,
                &ContextInput {
                    line_id: Some("  line_jr_yamanote_line  ".to_string()),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(
            trimmed_line_context
                .line
                .as_ref()
                .and_then(|line| line.line_id.as_deref()),
            Some("line_jr_yamanote_line")
        );
        let unknown_line_error = repo
            .resolve_context(
                "req-context-line-missing",
                None,
                &ContextInput {
                    line_id: Some("line_missing".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("missing line_id should fail");
        assert!(unknown_line_error
            .to_string()
            .contains("unknown line_id: line_missing"));
        let coded_area_context = repo
            .resolve_context(
                "req-context-area-code",
                None,
                &ContextInput {
                    area: Some(AreaContextInput {
                        city_code: Some("13103".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(
            coded_area_context.context_source,
            ContextSource::RequestArea
        );
        assert_eq!(coded_area_context.city_name(), Some("Minato"));
        assert_eq!(coded_area_context.prefecture_name(), Some("Tokyo"));
        let coded_station = repo
            .load_station_for_context(&coded_area_context)
            .await?
            .expect("representative station from city code");
        let coded_links = repo
            .load_context_candidate_links(&coded_station, &coded_area_context, 20, 5_000.0, 2)
            .await?;
        assert!(!coded_links.is_empty());
        let mismatched_area_context = repo
            .resolve_context(
                "req-context-area-mismatch",
                None,
                &ContextInput {
                    area: Some(AreaContextInput {
                        city_name: Some("Minato".to_string()),
                        prefecture_name: Some("Osaka".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(
            mismatched_area_context.context_source,
            ContextSource::RequestArea
        );

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
        let coded_area_row = client
            .query_one(
                "SELECT area_id
                 FROM context_resolution_traces
                 WHERE request_id = 'req-context-area-code'",
                &[],
            )
            .await?;
        assert_eq!(
            coded_area_row.get::<_, Option<String>>("area_id"),
            Some("area_minato".to_string())
        );
        let mismatched_area_row = client
            .query_one(
                "SELECT area_id
                 FROM context_resolution_traces
                 WHERE request_id = 'req-context-area-mismatch'",
                &[],
            )
            .await?;
        assert_eq!(
            mismatched_area_row.get::<_, Option<String>>("area_id"),
            None
        );
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

        client
            .execute(
                "INSERT INTO user_profile_contexts (
                    user_id,
                    area_id,
                    line_id,
                    station_id,
                    context_source,
                    confidence,
                    consent_scope
                ) VALUES ($1, NULL, NULL, NULL, 'user_profile_area', 0.7, 'coarse_area')
                ON CONFLICT (user_id) DO UPDATE
                SET area_id = EXCLUDED.area_id,
                    line_id = EXCLUDED.line_id,
                    station_id = EXCLUDED.station_id,
                    context_source = EXCLUDED.context_source,
                    confidence = EXCLUDED.confidence,
                    consent_scope = EXCLUDED.consent_scope",
                &[&"empty-profile-user"],
            )
            .await?;

        let empty_profile_context = repo
            .resolve_context(
                "req-context-empty-profile",
                Some("empty-profile-user"),
                &ContextInput::default(),
            )
            .await?;
        assert_eq!(
            empty_profile_context.context_source,
            ContextSource::DefaultSafeContext
        );
        assert!(empty_profile_context.area.is_none());
        assert!(empty_profile_context.line.is_none());
        assert!(empty_profile_context.station.is_none());

        let country_only_context = repo
            .resolve_context(
                "req-context-country-only",
                None,
                &ContextInput {
                    area: Some(AreaContextInput {
                        country: Some("JP".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(
            country_only_context.context_source,
            ContextSource::DefaultSafeContext
        );
        assert!(country_only_context.area.is_none());
        assert!(country_only_context.line.is_none());
        assert!(country_only_context.station.is_none());

        Ok(())
    }
    .await;

    drop_database(&admin_database_url, &database_name).await?;
    test_result
}
