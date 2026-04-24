use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::OnceLock,
};

use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use context::{
    AreaContext, AreaContextInput, ContextInput, ContextSource, ContextWarning, LineContext,
    PrivacyLevel, RankingContext, StationContext,
};
use csv::Reader;
use domain::{
    AreaAffinitySnapshot, Event, PlacementKind, PopularitySnapshot, RankingDataset, RankingQuery,
    School, SchoolStationLink, Station, UserAffinitySnapshot, UserEvent,
};
use generic_csv::SourceManifest;
use jp_postal::PostalCodeRecord;
use jp_rail::RailStationRecord;
use jp_school::{SchoolCodeRecord, SchoolGeodataRecord};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use storage::{
    ClaimedJob, JobType, NewJob, RecommendationRepository, RecommendationTrace,
    SnapshotRefreshStats, SnapshotTuning,
};
use tokio_postgres::{Client, GenericClient, NoTls, Row};
use uuid::Uuid;

static MIGRATION_PROCESS_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

const REQUIRED_READY_TABLES: [&str; 14] = [
    "areas",
    "context_resolution_traces",
    "lines",
    "schools",
    "events",
    "stations",
    "school_station_links",
    "popularity_snapshots",
    "user_affinity_snapshots",
    "area_affinity_snapshots",
    "user_events",
    "user_profile_contexts",
    "recommendation_traces",
    "job_queue",
];
const SCHEMA_MIGRATION_LOCK_NAMESPACE: i32 = 6_042;
const SCHEMA_MIGRATION_LOCK_KEY: i32 = 1;
const JOB_COALESCE_LOCK_NAMESPACE: i32 = 6_042;
const POPULARITY_REFRESH_COALESCE_LOCK_KEY: i32 = 2;
const STALE_JOB_LOCK_TIMEOUT_SECS: i64 = 15 * 60;
const STALE_JOB_LOCK_ERROR: &str = "worker lock expired before completion";
const USER_EVENT_REFERENCE_VALIDATION_PREFIX: &str = "user event reference validation: ";
#[derive(Debug, Clone)]
pub struct PgRepository {
    database_url: String,
    trace_hash_salt: String,
}

#[derive(Debug, Clone, Default)]
struct StationAreaColumns {
    country_code: Option<String>,
    prefecture_code: Option<String>,
    prefecture_name: Option<String>,
    city_code: Option<String>,
    city_name: Option<String>,
}

#[derive(Debug, Clone)]
struct AreaLookupRow {
    area_id: String,
    country_code: String,
    prefecture_code: Option<String>,
    prefecture_name: Option<String>,
    city_code: Option<String>,
    city_name: Option<String>,
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn matches_ignore_ascii(actual: Option<&str>, expected: &str) -> bool {
    actual.is_some_and(|actual| actual.eq_ignore_ascii_case(expected))
}

fn normalized_area_context(area_input: &AreaContextInput) -> AreaContext {
    AreaContext {
        country: non_empty(area_input.country.as_deref())
            .unwrap_or("JP")
            .to_string(),
        prefecture_code: non_empty(area_input.prefecture_code.as_deref()).map(str::to_string),
        prefecture_name: non_empty(area_input.prefecture_name.as_deref()).map(str::to_string),
        city_code: non_empty(area_input.city_code.as_deref()).map(str::to_string),
        city_name: non_empty(area_input.city_name.as_deref()).map(str::to_string),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobQueueRow {
    pub id: i64,
    pub job_type: String,
    pub payload: Value,
    pub status: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub locked_by: Option<String>,
    pub locked_at: Option<String>,
    pub last_error: Option<String>,
    pub run_after: String,
    pub completed_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobAttemptRow {
    pub attempt_number: i32,
    pub status: String,
    pub error_message: Option<String>,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobQueuePressureRow {
    pub job_type: String,
    pub status: String,
    pub job_count: i64,
    pub oldest_run_after: Option<String>,
    pub latest_update: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobQueueSnapshot {
    pub jobs: Vec<JobQueueRow>,
    pub pressure: Vec<JobQueuePressureRow>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobInspection {
    pub job: JobQueueRow,
    pub attempts: Vec<JobAttemptRow>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobMutationSummary {
    pub job: JobQueueRow,
    pub updated: bool,
}

impl PgRepository {
    pub fn new(database_url: impl Into<String>) -> Self {
        let database_url = database_url.into();
        Self {
            trace_hash_salt: load_trace_hash_salt(),
            database_url,
        }
    }

    async fn connect(&self) -> Result<Client> {
        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .with_context(|| "failed to connect to PostgreSQL")?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "postgres connection terminated");
            }
        });
        Ok(client)
    }

    pub async fn record_user_event_with_jobs(
        &self,
        event: &UserEvent,
        jobs: &[NewJob],
    ) -> Result<i64> {
        let mut client = self.connect().await?;
        let transaction = client.transaction().await?;
        let event_id = insert_user_event(&transaction, event).await?;
        for job in jobs {
            insert_job(&transaction, job).await?;
        }
        transaction.commit().await?;
        Ok(event_id)
    }

    pub async fn load_station(&self, station_id: &str) -> Result<Option<Station>> {
        let client = self.connect().await?;
        client
            .query_opt(
                "SELECT id, name, line_name, line_id, latitude, longitude
                 FROM stations
                 WHERE id = $1",
                &[&station_id],
            )
            .await
            .map(|row| {
                row.map(|row| Station {
                    id: row.get("id"),
                    name: row.get("name"),
                    line_name: row.get("line_name"),
                    line_id: row.get("line_id"),
                    latitude: row.get("latitude"),
                    longitude: row.get("longitude"),
                })
            })
            .context("failed to load target station")
    }

    pub async fn resolve_context(
        &self,
        request_id: &str,
        user_id: Option<&str>,
        input: &ContextInput,
    ) -> Result<RankingContext> {
        let client = self.connect().await?;
        if input
            .station_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            bail!("station_id must not be blank");
        }
        let mut context = if let Some(station_id) = input
            .station_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            self.resolve_station_context(&client, station_id, input)
                .await?
        } else if input.has_line() {
            self.resolve_line_context(&client, input).await?
        } else if input.area.as_ref().is_some_and(|area| !area.is_empty()) {
            let area = match input.area.as_ref() {
                Some(area_input) => self.resolve_area_context(&client, area_input).await?,
                None => None,
            };
            RankingContext {
                context_source: ContextSource::RequestArea,
                confidence: 0.95,
                area,
                line: None,
                station: None,
                privacy_level: PrivacyLevel::CoarseArea,
                fallback_policy: "school_event_jp_default".to_string(),
                gate_policy: "geo_line_default".to_string(),
                warnings: Vec::new(),
            }
        } else if let Some(user_id) = user_id {
            self.resolve_user_profile_context(&client, user_id)
                .await?
                .unwrap_or_else(RankingContext::default_safe)
        } else {
            RankingContext::default_safe()
        };

        context
            .warnings
            .sort_by(|left, right| left.code.cmp(&right.code));
        if let Err(error) = self
            .record_context_trace(&client, request_id, user_id, &context)
            .await
        {
            tracing::warn!(%error, request_id, "failed to record context trace");
        }
        Ok(context)
    }

    pub async fn load_station_for_context(
        &self,
        context: &RankingContext,
    ) -> Result<Option<Station>> {
        let client = self.connect().await?;
        if let Some(station_id) = context.station_id() {
            return client
                .query_opt(
                    "SELECT id, name, line_name, line_id, latitude, longitude
                     FROM stations
                     WHERE id = $1",
                    &[&station_id],
                )
                .await
                .map(|row| row.map(station_from_row))
                .with_context(|| format!("failed to load station {station_id}"));
        }

        if let Some(line_name) = context.line_name() {
            let line_id = context
                .line
                .as_ref()
                .and_then(|line| line.line_id.as_deref());
            return client
                .query_opt(
                    "SELECT id, name, line_name, line_id, latitude, longitude
                     FROM stations
                     WHERE ($1::TEXT IS NOT NULL AND line_id = $1)
                        OR (
                            $2::TEXT IS NOT NULL
                            AND ($1::TEXT IS NULL OR line_id IS NULL)
                            AND line_name = $2
                        )
                     ORDER BY
                        CASE
                            WHEN $1::TEXT IS NOT NULL AND line_id = $1 THEN 0
                            WHEN
                                $2::TEXT IS NOT NULL
                                AND ($1::TEXT IS NULL OR line_id IS NULL)
                                AND line_name = $2
                                THEN 1
                            ELSE 2
                        END,
                        id
                     LIMIT 1",
                    &[&line_id, &line_name],
                )
                .await
                .map(|row| row.map(station_from_row))
                .context("failed to load representative station for line context");
        }

        if let Some(city_name) = context.city_name() {
            if let Some(station) = self
                .load_station_for_school_area(&client, city_name, context.prefecture_name())
                .await
                .with_context(|| {
                    format!("failed to load representative station for city {city_name}")
                })?
            {
                return Ok(Some(station));
            }
        }

        if let Some(prefecture_name) = context.prefecture_name() {
            if let Some(station) = self
                .load_station_for_school_area(&client, prefecture_name, None)
                .await
                .with_context(|| {
                    format!(
                        "failed to load representative station for prefecture {prefecture_name}"
                    )
                })?
            {
                return Ok(Some(station));
            }
        }

        client
            .query_opt(
                "SELECT id, name, line_name, line_id, latitude, longitude
                 FROM stations
                 ORDER BY id
                 LIMIT 1",
                &[],
            )
            .await
            .map(|row| row.map(station_from_row))
            .context("failed to load default representative station")
    }

    async fn resolve_station_context(
        &self,
        client: &Client,
        station_id: &str,
        input: &ContextInput,
    ) -> Result<RankingContext> {
        let station_row = client
            .query_opt(
                "SELECT
                    station.id,
                    station.name,
                    station.line_name,
                    station.line_id,
                    station.latitude,
                    station.longitude,
                    line.operator_name,
                    area.country_code AS station_country_code,
                    area.prefecture_code AS station_prefecture_code,
                    area.prefecture_name AS station_prefecture_name,
                    area.city_code AS station_city_code,
                    area.city_name AS station_city_name
                 FROM stations AS station
                 LEFT JOIN lines AS line
                   ON line.line_id = station.line_id
                 LEFT JOIN areas AS area
                   ON area.area_id = station.area_id
                 WHERE station.id = $1",
                &[&station_id],
            )
            .await?
            .with_context(|| format!("unknown station: {station_id}"))?;
        let station = Station {
            id: station_row.get("id"),
            name: station_row.get("name"),
            line_name: station_row.get("line_name"),
            line_id: station_row.get("line_id"),
            latitude: station_row.get("latitude"),
            longitude: station_row.get("longitude"),
        };
        let station_line_id = station_row.get::<_, Option<String>>("line_id");
        let station_operator_name = station_row.get::<_, Option<String>>("operator_name");
        let station_area = StationAreaColumns {
            country_code: station_row.get("station_country_code"),
            prefecture_code: station_row.get("station_prefecture_code"),
            prefecture_name: station_row.get("station_prefecture_name"),
            city_code: station_row.get("station_city_code"),
            city_name: station_row.get("station_city_name"),
        };

        let mut area = match input.area.as_ref().filter(|area| !area.is_empty()) {
            Some(area_input) => self.resolve_area_context(client, area_input).await?,
            None => None,
        };
        let mut warnings = Vec::new();
        if let Some(area_input) = input.area.as_ref().filter(|area| !area.is_empty()) {
            let matches = self
                .station_matches_area_hint(client, &station.id, area_input, &station_area)
                .await?;
            if !matches {
                warnings.push(ContextWarning {
                    code: "station_area_conflict".to_string(),
                    message: "station context was used and conflicting area hint was ignored"
                        .to_string(),
                });
                area = None;
            }
        }

        Ok(RankingContext {
            context_source: ContextSource::RequestStation,
            confidence: 1.0,
            area,
            line: Some(LineContext {
                line_id: station_line_id,
                line_name: station.line_name.clone(),
                operator_name: station_operator_name,
            }),
            station: Some(StationContext {
                station_id: station.id,
                station_name: station.name,
            }),
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings,
        })
    }

    async fn station_matches_area_hint(
        &self,
        client: &Client,
        station_id: &str,
        area_input: &AreaContextInput,
        station_area: &StationAreaColumns,
    ) -> Result<bool> {
        let mut matched_city_hint = false;
        let country = non_empty(area_input.country.as_deref())
            .or_else(|| non_empty(station_area.country_code.as_deref()))
            .unwrap_or("JP");
        if let Some(country) = non_empty(area_input.country.as_deref()) {
            let station_country = non_empty(station_area.country_code.as_deref()).unwrap_or("JP");
            if !station_country.eq_ignore_ascii_case(country) {
                return Ok(false);
            }
        }
        if let Some(city_code) = non_empty(area_input.city_code.as_deref()) {
            if !matches_ignore_ascii(station_area.city_code.as_deref(), city_code) {
                return Ok(false);
            }
            matched_city_hint = true;
        }
        if let Some(city_name) = non_empty(area_input.city_name.as_deref()) {
            if let Some(station_city_name) = non_empty(station_area.city_name.as_deref()) {
                if !station_city_name.eq_ignore_ascii_case(city_name) {
                    return Ok(false);
                }
                matched_city_hint = true;
            } else if !self
                .station_linked_school_area_matches(client, station_id, city_name)
                .await?
            {
                return Ok(false);
            } else {
                matched_city_hint = true;
            }
        }
        let inferred_city_area = if matched_city_hint {
            self.lookup_area_row(
                client,
                country,
                None,
                None,
                non_empty(area_input.city_code.as_deref()),
                non_empty(area_input.city_name.as_deref()),
            )
            .await?
        } else {
            None
        };
        if let Some(prefecture_code) = non_empty(area_input.prefecture_code.as_deref()) {
            if let Some(station_prefecture_code) =
                non_empty(station_area.prefecture_code.as_deref())
            {
                if !station_prefecture_code.eq_ignore_ascii_case(prefecture_code) {
                    return Ok(false);
                }
            } else if let Some(inferred_prefecture_code) = inferred_city_area
                .as_ref()
                .and_then(|area| area.prefecture_code.as_deref())
            {
                if !inferred_prefecture_code.eq_ignore_ascii_case(prefecture_code) {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
        }
        if let Some(prefecture_name) = non_empty(area_input.prefecture_name.as_deref()) {
            if let Some(station_prefecture_name) =
                non_empty(station_area.prefecture_name.as_deref())
            {
                if !station_prefecture_name.eq_ignore_ascii_case(prefecture_name) {
                    return Ok(false);
                }
            } else if let Some(inferred_prefecture_name) = inferred_city_area
                .as_ref()
                .and_then(|area| area.prefecture_name.as_deref())
            {
                if !inferred_prefecture_name.eq_ignore_ascii_case(prefecture_name) {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn station_linked_school_area_matches(
        &self,
        client: &Client,
        station_id: &str,
        city_name: &str,
    ) -> Result<bool> {
        client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1
                    FROM school_station_links AS link
                    INNER JOIN schools AS school
                      ON school.id = link.school_id
                    WHERE link.station_id = $1
                      AND lower(school.area) = lower($2)
                 ) AS matches_area",
                &[&station_id, &city_name],
            )
            .await
            .map(|row| row.get::<_, bool>("matches_area"))
            .context("failed to validate station area hint")
    }

    async fn resolve_line_context(
        &self,
        client: &Client,
        input: &ContextInput,
    ) -> Result<RankingContext> {
        let requested_line_id = input
            .line_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let requested_line_name = input
            .line_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let line = if let Some(line_id) = requested_line_id.as_deref() {
            client
                .query_opt(
                    "SELECT line_id, line_name, operator_name
                     FROM lines
                     WHERE line_id = $1",
                    &[&line_id],
                )
                .await?
                .map(|row| LineContext {
                    line_id: Some(row.get("line_id")),
                    line_name: row.get("line_name"),
                    operator_name: row.get("operator_name"),
                })
        } else {
            None
        };

        let line = if let Some(line) = line {
            line
        } else {
            if let Some(line_id) = requested_line_id.as_deref() {
                if requested_line_name.is_none() {
                    bail!("unknown line_id: {line_id}");
                }
            }
            let line_name = requested_line_name
                .with_context(|| "line context requires line_id or line_name")?;
            let row = client
                .query_opt(
                    "SELECT line_id, line_name, operator_name
                     FROM lines
                     WHERE line_name = $1
                     LIMIT 1",
                    &[&line_name],
                )
                .await?;
            row.map(|row| LineContext {
                line_id: Some(row.get("line_id")),
                line_name: row.get("line_name"),
                operator_name: row.get("operator_name"),
            })
            .unwrap_or_else(|| LineContext {
                line_id: None,
                line_name: line_name.to_string(),
                operator_name: None,
            })
        };
        let area = match input.area.as_ref().filter(|area| !area.is_empty()) {
            Some(area_input) => self.resolve_area_context(client, area_input).await?,
            None => None,
        };

        Ok(RankingContext {
            context_source: ContextSource::RequestLine,
            confidence: 0.95,
            area,
            line: Some(line),
            station: None,
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        })
    }

    async fn resolve_user_profile_context(
        &self,
        client: &Client,
        user_id: &str,
    ) -> Result<Option<RankingContext>> {
        let row = client
            .query_opt(
                "SELECT
                    profile.confidence,
                    area.country_code,
                    area.prefecture_code,
                    area.prefecture_name,
                    area.city_code,
                    area.city_name,
                    line.line_id,
                    line.line_name,
                    line.operator_name,
                    station.id AS station_id,
                    station.name AS station_name
                 FROM user_profile_contexts AS profile
                 LEFT JOIN areas AS area
                   ON area.area_id = profile.area_id
                 LEFT JOIN lines AS line
                   ON line.line_id = profile.line_id
                 LEFT JOIN stations AS station
                   ON station.id = profile.station_id
                 WHERE profile.user_id = $1
                   AND (profile.retained_until IS NULL OR profile.retained_until > NOW())",
                &[&user_id],
            )
            .await?;

        Ok(row.and_then(|row| {
            let country_code = row.get::<_, Option<String>>("country_code");
            let prefecture_code = row.get::<_, Option<String>>("prefecture_code");
            let prefecture_name = row.get::<_, Option<String>>("prefecture_name");
            let city_code = row.get::<_, Option<String>>("city_code");
            let city_name = row.get::<_, Option<String>>("city_name");
            let has_area_signal = [
                prefecture_code.as_deref(),
                prefecture_name.as_deref(),
                city_code.as_deref(),
                city_name.as_deref(),
            ]
            .into_iter()
            .any(|value| non_empty(value).is_some());
            let area = country_code.and_then(|country| {
                has_area_signal.then_some(AreaContext {
                    country,
                    prefecture_code,
                    prefecture_name,
                    city_code,
                    city_name,
                })
            });
            let line = row
                .get::<_, Option<String>>("line_name")
                .map(|line_name| LineContext {
                    line_id: row.get("line_id"),
                    line_name,
                    operator_name: row.get("operator_name"),
                });
            let station = row
                .get::<_, Option<String>>("station_id")
                .map(|station_id| StationContext {
                    station_id,
                    station_name: row.get("station_name"),
                });

            (area.is_some() || line.is_some() || station.is_some()).then(|| RankingContext {
                context_source: ContextSource::UserProfileArea,
                confidence: row.get("confidence"),
                area,
                line,
                station,
                privacy_level: PrivacyLevel::CoarseArea,
                fallback_policy: "school_event_jp_default".to_string(),
                gate_policy: "geo_line_default".to_string(),
                warnings: Vec::new(),
            })
        }))
    }

    async fn resolve_area_context(
        &self,
        client: &Client,
        area_input: &AreaContextInput,
    ) -> Result<Option<AreaContext>> {
        if area_input.is_empty() {
            return Ok(None);
        }

        let fallback_area = normalized_area_context(area_input);
        let area = self
            .lookup_area_row(
                client,
                &fallback_area.country,
                fallback_area.prefecture_code.as_deref(),
                fallback_area.prefecture_name.as_deref(),
                fallback_area.city_code.as_deref(),
                fallback_area.city_name.as_deref(),
            )
            .await?
            .map(|area| AreaContext {
                country: area.country_code,
                prefecture_code: area.prefecture_code,
                prefecture_name: area.prefecture_name,
                city_code: area.city_code,
                city_name: area.city_name,
            })
            .unwrap_or(fallback_area);

        Ok(Some(area))
    }

    async fn record_context_trace(
        &self,
        client: &Client,
        request_id: &str,
        user_id: Option<&str>,
        context: &RankingContext,
    ) -> Result<()> {
        let user_id_hash = user_id.map(|user_id| self.hash_user_id(user_id));
        let area_id = match context.area.as_ref() {
            Some(area) => self.resolve_trace_area_id(client, area).await?,
            None => None,
        };
        let line_id = context.line.as_ref().and_then(|line| line.line_id.clone());
        let station_id = context
            .station
            .as_ref()
            .map(|station| station.station_id.clone());
        let warnings = serde_json::to_value(&context.warnings)?;
        client
            .execute(
                "INSERT INTO context_resolution_traces (
                    request_id,
                    user_id_hash,
                    context_source,
                    confidence,
                    area_id,
                    line_id,
                    station_id,
                    warnings
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[
                    &request_id,
                    &user_id_hash,
                    &context.context_source.as_str(),
                    &context.confidence,
                    &area_id,
                    &line_id,
                    &station_id,
                    &warnings,
                ],
            )
            .await?;
        Ok(())
    }

    async fn lookup_area_row(
        &self,
        client: &Client,
        country: &str,
        prefecture_code: Option<&str>,
        prefecture_name: Option<&str>,
        city_code: Option<&str>,
        city_name: Option<&str>,
    ) -> Result<Option<AreaLookupRow>> {
        if city_code.is_none()
            && prefecture_code.is_none()
            && city_name.is_none()
            && prefecture_name.is_none()
        {
            return Ok(None);
        }

        client
            .query_opt(
                "SELECT
                    area_id,
                    country_code,
                    prefecture_code,
                    prefecture_name,
                    city_code,
                    city_name
                 FROM areas
                 WHERE country_code = $5
                   AND ($1::TEXT IS NULL OR city_code = $1)
                   AND ($2::TEXT IS NULL OR prefecture_code = $2)
                   AND ($3::TEXT IS NULL OR lower(city_name) = lower($3))
                   AND ($4::TEXT IS NULL OR lower(prefecture_name) = lower($4))
                 ORDER BY
                    CASE
                        WHEN ($1::TEXT IS NOT NULL OR $3::TEXT IS NOT NULL) AND area_level = 'city' THEN 0
                        WHEN ($2::TEXT IS NOT NULL OR $4::TEXT IS NOT NULL) AND area_level = 'prefecture' THEN 0
                        ELSE 1
                    END,
                    CASE
                        WHEN ($1::TEXT IS NOT NULL OR $3::TEXT IS NOT NULL)
                          AND area_level = 'city'
                          AND prefecture_name IS NOT NULL
                          THEN 0
                        ELSE 1
                    END,
                    area_id ASC
                 LIMIT 1",
                &[
                    &city_code,
                    &prefecture_code,
                    &city_name,
                    &prefecture_name,
                    &country,
                ],
            )
            .await
            .map(|row| {
                row.map(|row| AreaLookupRow {
                    area_id: row.get("area_id"),
                    country_code: row.get("country_code"),
                    prefecture_code: row.get("prefecture_code"),
                    prefecture_name: row.get("prefecture_name"),
                    city_code: row.get("city_code"),
                    city_name: row.get("city_name"),
                })
            })
            .context("failed to resolve area context")
    }

    async fn resolve_trace_area_id(
        &self,
        client: &Client,
        area: &AreaContext,
    ) -> Result<Option<String>> {
        self.lookup_area_row(
            client,
            &area.country,
            area.prefecture_code.as_deref(),
            area.prefecture_name.as_deref(),
            area.city_code.as_deref(),
            area.city_name.as_deref(),
        )
        .await
        .map(|row| row.map(|row| row.area_id))
    }

    async fn load_station_for_school_area(
        &self,
        client: &Client,
        area: &str,
        prefecture_name: Option<&str>,
    ) -> Result<Option<Station>> {
        client
            .query_opt(
                "SELECT station.id, station.name, station.line_name, station.line_id, station.latitude, station.longitude
                 FROM schools AS school
                 INNER JOIN school_station_links AS link
                   ON link.school_id = school.id
                 INNER JOIN stations AS station
                   ON station.id = link.station_id
                 LEFT JOIN LATERAL (
                    SELECT area.prefecture_name
                    FROM areas AS area
                    WHERE area.country_code = 'JP'
                      AND area.area_level = 'city'
                      AND lower(area.city_name) = lower(school.area)
                      AND (
                          $2::TEXT IS NULL
                          OR area.prefecture_name IS NULL
                          OR lower(area.prefecture_name) = lower($2)
                      )
                    ORDER BY
                        CASE
                            WHEN $2::TEXT IS NOT NULL
                              AND area.prefecture_name IS NOT NULL
                              AND lower(area.prefecture_name) = lower($2)
                              THEN 0
                            ELSE 1
                        END,
                        area.area_id ASC
                    LIMIT 1
                 ) AS school_area ON TRUE
                 WHERE (
                    $2::TEXT IS NOT NULL
                    AND lower(school.area) = lower($1)
                    AND lower(COALESCE(school.prefecture_name, school_area.prefecture_name, '')) = lower($2)
                 )
                    OR (
                        $2::TEXT IS NULL
                        AND (
                            lower(school.area) = lower($1)
                            OR lower(school.prefecture_name) = lower($1)
                            OR lower(school_area.prefecture_name) = lower($1)
                        )
                    )
                 ORDER BY link.distance_meters ASC, link.walking_minutes ASC, station.id ASC
                 LIMIT 1",
                &[&area, &prefecture_name],
            )
            .await
            .map(|row| row.map(station_from_row))
            .context("failed to load representative station for area")
    }

    // Salt trace hashes with deployment-local configuration so trace tables do not
    // expose a reusable raw-user-id digest on their own.
    fn hash_user_id(&self, user_id: &str) -> String {
        let mut digest = Sha256::new();
        digest.update(self.trace_hash_salt.as_bytes());
        digest.update(b"\0");
        digest.update(user_id.as_bytes());
        format!("{:x}", digest.finalize())
    }

    pub async fn list_jobs(&self, limit: i64) -> Result<JobQueueSnapshot> {
        let client = self.connect().await?;
        let limit = limit.clamp(1, 500);
        let jobs = client
            .query(
                r#"SELECT id, job_type, payload, status, attempts, max_attempts,
                        locked_by,
                        to_char(locked_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS locked_at,
                        last_error,
                        to_char(run_after AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS run_after,
                        to_char(completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS completed_at,
                        to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at,
                        to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at
                 FROM job_queue
                 ORDER BY id DESC
                 LIMIT $1"#,
                &[&limit],
            )
            .await?
            .into_iter()
            .map(job_queue_row)
            .collect::<Result<Vec<_>>>()?;

        let pressure = client
            .query(
                r#"SELECT job_type, status, COUNT(*)::BIGINT AS job_count,
                        to_char(MIN(run_after) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS oldest_run_after,
                        to_char(MAX(updated_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS latest_update
                 FROM job_queue
                 GROUP BY job_type, status
                 ORDER BY job_type ASC, status ASC"#,
                &[],
            )
            .await?
            .into_iter()
            .map(|row| JobQueuePressureRow {
                job_type: row.get("job_type"),
                status: row.get("status"),
                job_count: row.get("job_count"),
                oldest_run_after: row.get("oldest_run_after"),
                latest_update: row.get("latest_update"),
            })
            .collect();

        Ok(JobQueueSnapshot { jobs, pressure })
    }

    pub async fn inspect_job(&self, job_id: i64) -> Result<JobInspection> {
        let client = self.connect().await?;
        let job = client
            .query_opt(
                r#"SELECT id, job_type, payload, status, attempts, max_attempts,
                        locked_by,
                        to_char(locked_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS locked_at,
                        last_error,
                        to_char(run_after AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS run_after,
                        to_char(completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS completed_at,
                        to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at,
                        to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at
                 FROM job_queue
                 WHERE id = $1"#,
                &[&job_id],
            )
            .await?
            .map(job_queue_row)
            .transpose()?
            .with_context(|| format!("job_queue id {job_id} not found"))?;

        let attempts = client
            .query(
                r#"SELECT attempt_number, status, error_message,
                        to_char(started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS started_at,
                        to_char(finished_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS finished_at
                 FROM job_attempts
                 WHERE job_id = $1
                 ORDER BY attempt_number ASC"#,
                &[&job_id],
            )
            .await?
            .into_iter()
            .map(job_attempt_row)
            .collect::<Result<Vec<_>>>()?;

        Ok(JobInspection { job, attempts })
    }

    pub async fn retry_failed_job(&self, job_id: i64) -> Result<JobMutationSummary> {
        let mut client = self.connect().await?;
        let transaction = client.transaction().await?;
        let before_status = transaction
            .query_opt(
                "SELECT status
                 FROM job_queue
                 WHERE id = $1
                 FOR UPDATE",
                &[&job_id],
            )
            .await?
            .map(|row| row.get::<_, String>("status"))
            .with_context(|| format!("job_queue id {job_id} not found"))?;

        let updated = before_status == "failed";
        if updated {
            transaction
                .execute(
                    "UPDATE job_queue
                     SET status = 'queued',
                         max_attempts = GREATEST(max_attempts, attempts + 1),
                         run_after = NOW(),
                         locked_at = NULL,
                         locked_by = NULL,
                         completed_at = NULL,
                         last_error = NULL,
                         updated_at = NOW()
                     WHERE id = $1",
                    &[&job_id],
                )
                .await?;
        }

        let job = transaction
            .query_one(
                r#"SELECT id, job_type, payload, status, attempts, max_attempts,
                        locked_by,
                        to_char(locked_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS locked_at,
                        last_error,
                        to_char(run_after AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS run_after,
                        to_char(completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS completed_at,
                        to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at,
                        to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at
                 FROM job_queue
                 WHERE id = $1"#,
                &[&job_id],
            )
            .await?;
        let job = job_queue_row(job)?;
        transaction.commit().await?;

        Ok(JobMutationSummary { job, updated })
    }

    pub async fn make_queued_job_due(&self, job_id: i64) -> Result<JobMutationSummary> {
        let mut client = self.connect().await?;
        let transaction = client.transaction().await?;
        let updated = transaction
            .execute(
                "UPDATE job_queue
                 SET run_after = NOW(),
                     updated_at = NOW()
                 WHERE id = $1
                   AND status = 'queued'
                   AND run_after > NOW()",
                &[&job_id],
            )
            .await?
            > 0;

        let job = transaction
            .query_opt(
                r#"SELECT id, job_type, payload, status, attempts, max_attempts,
                        locked_by,
                        to_char(locked_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS locked_at,
                        last_error,
                        to_char(run_after AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS run_after,
                        to_char(completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS completed_at,
                        to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at,
                        to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at
                 FROM job_queue
                 WHERE id = $1"#,
                &[&job_id],
            )
            .await?
            .map(job_queue_row)
            .transpose()?
            .with_context(|| format!("job_queue id {job_id} not found"))?;
        transaction.commit().await?;

        Ok(JobMutationSummary { job, updated })
    }

    pub async fn load_candidate_links(
        &self,
        target_station: &Station,
        candidate_limit: usize,
        neighbor_distance_cap_meters: f64,
        neighbor_max_hops: u8,
    ) -> Result<Vec<SchoolStationLink>> {
        let client = self.connect().await?;
        let rows = client
            .query(
                "SELECT
                    link.school_id,
                    link.station_id,
                    link.walking_minutes,
                    link.distance_meters,
                    link.hop_distance,
                    link.line_name
                 FROM school_station_links AS link
                 INNER JOIN stations AS candidate_station
                   ON candidate_station.id = link.station_id
                 WHERE link.station_id = $1
                    OR (
                        link.line_name = $2
                        AND link.hop_distance <= $6
                        AND ST_DWithin(
                            candidate_station.geom,
                            ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography,
                            $5
                        )
                    )
                 ORDER BY
                    CASE WHEN link.station_id = $1 THEN 0 ELSE 1 END,
                    link.distance_meters ASC,
                    link.walking_minutes ASC,
                    link.school_id ASC,
                    link.station_id ASC
                 LIMIT $7",
                &[
                    &target_station.id,
                    &target_station.line_name,
                    &target_station.longitude,
                    &target_station.latitude,
                    &neighbor_distance_cap_meters,
                    &(neighbor_max_hops as i16),
                    &((candidate_limit.clamp(1, 10_000)) as i64),
                ],
            )
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| SchoolStationLink {
                school_id: row.get("school_id"),
                station_id: row.get("station_id"),
                walking_minutes: row.get::<_, i16>("walking_minutes") as u16,
                distance_meters: row.get::<_, i32>("distance_meters") as u32,
                hop_distance: row.get::<_, i16>("hop_distance") as u8,
                line_name: row.get("line_name"),
            })
            .collect())
    }

    pub async fn load_context_candidate_links(
        &self,
        target_station: &Station,
        context: &RankingContext,
        candidate_limit: usize,
        min_scoped_candidates: usize,
        neighbor_distance_cap_meters: f64,
        neighbor_max_hops: u8,
    ) -> Result<Vec<SchoolStationLink>> {
        let client = self.connect().await?;
        let station_id = context.station_id().map(str::to_string);
        let line_id = context.line.as_ref().and_then(|line| line.line_id.clone());
        let line_name = context.line_name().map(str::to_string);
        let city_name = context.city_name().map(str::to_string);
        let prefecture_name = context.prefecture_name().map(str::to_string);
        let station_context_is_explicit = context.station.is_some();
        let include_safe_global_candidates = true;
        let rows = client
            .query(
                "WITH candidate_rows AS (
                    SELECT
                        link.school_id,
                        link.station_id,
                        link.walking_minutes,
                        link.distance_meters,
                        link.hop_distance,
                        link.line_name,
                        candidate_station.line_id AS candidate_line_id,
                        candidate_station.geom AS candidate_geom,
                        ST_Distance(
                            candidate_station.geom,
                            ST_SetSRID(ST_MakePoint($5, $6), 4326)::geography
                        ) AS target_distance_meters,
                        school.area AS school_area,
                        COALESCE(school.prefecture_name, school_area.prefecture_name) AS school_prefecture_name
                    FROM school_station_links AS link
                    INNER JOIN stations AS candidate_station
                      ON candidate_station.id = link.station_id
                    INNER JOIN schools AS school
                      ON school.id = link.school_id
                    LEFT JOIN LATERAL (
                        SELECT area.prefecture_name
                        FROM areas AS area
                        WHERE area.country_code = 'JP'
                          AND area.area_level = 'city'
                          AND lower(area.city_name) = lower(school.area)
                          AND (
                              school.prefecture_name IS NULL
                              OR area.prefecture_name IS NULL
                              OR lower(area.prefecture_name) = lower(school.prefecture_name)
                          )
                        ORDER BY
                            CASE
                                WHEN school.prefecture_name IS NOT NULL
                                  AND area.prefecture_name IS NOT NULL
                                  AND lower(area.prefecture_name) = lower(school.prefecture_name)
                                  THEN 0
                                ELSE 1
                            END,
                            area.area_id ASC
                        LIMIT 1
                    ) AS school_area ON TRUE
                ),
                scored_rows AS (
                    SELECT
                        candidate_rows.*,
                        ($1::TEXT IS NOT NULL AND station_id = $1) AS is_strict_station,
                        (
                            ($11::TEXT IS NOT NULL AND candidate_line_id = $11)
                            OR (
                                $2::TEXT IS NOT NULL
                                AND ($11::TEXT IS NULL OR candidate_line_id IS NULL)
                                AND line_name = $2
                            )
                        ) AS is_same_line,
                        (
                            $3::TEXT IS NOT NULL
                            AND lower(school_area) = lower($3)
                            AND (
                                $4::TEXT IS NULL
                                OR lower(COALESCE(school_prefecture_name, '')) = lower($4)
                            )
                        ) AS is_same_city,
                        (
                            $4::TEXT IS NOT NULL
                            AND (
                                lower(school_area) = lower($4)
                                OR lower(COALESCE(school_prefecture_name, '')) = lower($4)
                            )
                        ) AS is_same_prefecture,
                        (
                            $12
                            AND station_id <> $13
                            AND NOT (
                                ($11::TEXT IS NOT NULL AND candidate_line_id = $11)
                                OR (
                                    $2::TEXT IS NOT NULL
                                    AND ($11::TEXT IS NULL OR candidate_line_id IS NULL)
                                    AND line_name = $2
                                )
                            )
                            AND ($3::TEXT IS NULL OR lower(school_area) <> lower($3))
                            AND (
                                $4::TEXT IS NULL
                                OR NOT (
                                    lower(school_area) = lower($4)
                                    OR lower(COALESCE(school_prefecture_name, '')) = lower($4)
                                )
                            )
                            AND ST_DWithin(
                                candidate_geom,
                                ST_SetSRID(ST_MakePoint($5, $6), 4326)::geography,
                                $8
                            )
                        ) AS is_neighbor_area,
                        (
                            (
                                ($11::TEXT IS NOT NULL AND candidate_line_id = $11)
                                OR (
                                    $2::TEXT IS NOT NULL
                                    AND ($11::TEXT IS NULL OR candidate_line_id IS NULL)
                                    AND line_name = $2
                                )
                            )
                            AND hop_distance <= $7
                            AND ST_DWithin(
                                candidate_geom,
                                ST_SetSRID(ST_MakePoint($5, $6), 4326)::geography,
                                $8
                            )
                        ) AS is_near_same_line
                    FROM candidate_rows
                )
                 SELECT
                    link.school_id,
                    link.station_id,
                    link.walking_minutes,
                    link.distance_meters,
                    link.hop_distance,
                    link.line_name
                 FROM scored_rows AS link
                 WHERE link.is_strict_station
                    OR link.is_same_line
                    OR link.is_same_city
                    OR link.is_same_prefecture
                    OR link.is_neighbor_area
                    OR link.is_near_same_line
                    OR (
                        $9
                        AND (
                            SELECT COUNT(*)
                            FROM scored_rows AS scoped
                            WHERE scoped.is_strict_station
                               OR scoped.is_same_line
                               OR scoped.is_same_city
                               OR scoped.is_same_prefecture
                               OR scoped.is_neighbor_area
                               OR scoped.is_near_same_line
                        ) < $14
                    )
                 ORDER BY
                    CASE
                        WHEN link.is_strict_station THEN 0
                        WHEN link.is_same_line THEN 1
                        WHEN link.is_same_city THEN 3
                        WHEN link.is_same_prefecture THEN 4
                        WHEN link.is_neighbor_area THEN 5
                        ELSE 6
                    END,
                    CASE
                        WHEN NOT (
                            link.is_strict_station
                            OR link.is_same_line
                            OR link.is_same_city
                            OR link.is_same_prefecture
                            OR link.is_neighbor_area
                            OR link.is_near_same_line
                        )
                            THEN link.target_distance_meters
                        ELSE 0.0
                    END ASC,
                    link.distance_meters ASC,
                    link.walking_minutes ASC,
                    link.school_id ASC,
                    link.station_id ASC
                LIMIT $10",
                &[
                    &station_id,
                    &line_name,
                    &city_name,
                    &prefecture_name,
                    &target_station.longitude,
                    &target_station.latitude,
                    &(neighbor_max_hops as i16),
                    &neighbor_distance_cap_meters,
                    &include_safe_global_candidates,
                    &((candidate_limit.clamp(1, 10_000)) as i64),
                    &line_id,
                    &station_context_is_explicit,
                    &target_station.id,
                    &((min_scoped_candidates.max(1)) as i64),
                ],
            )
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| SchoolStationLink {
                school_id: row.get("school_id"),
                station_id: row.get("station_id"),
                walking_minutes: row.get::<_, i16>("walking_minutes") as u16,
                distance_meters: row.get::<_, i32>("distance_meters") as u32,
                hop_distance: row.get::<_, i16>("hop_distance") as u8,
                line_name: row.get("line_name"),
            })
            .collect())
    }

    pub async fn load_candidate_dataset(
        &self,
        query: &RankingQuery,
        target_station: &Station,
        candidate_links: &[SchoolStationLink],
    ) -> Result<RankingDataset> {
        if candidate_links.is_empty() {
            return Ok(RankingDataset {
                schools: Vec::new(),
                events: Vec::new(),
                stations: vec![target_station.clone()],
                school_station_links: Vec::new(),
                popularity_snapshots: Vec::new(),
                user_affinity_snapshots: Vec::new(),
                area_affinity_snapshots: Vec::new(),
            });
        }

        let mut school_ids = BTreeSet::new();
        let mut station_ids = BTreeSet::from([target_station.id.clone()]);
        for link in candidate_links {
            school_ids.insert(link.school_id.clone());
            station_ids.insert(link.station_id.clone());
        }
        let school_ids: Vec<String> = school_ids.into_iter().collect();
        let station_ids: Vec<String> = station_ids.into_iter().collect();

        let client = self.connect().await?;

        let schools: Vec<School> = client
            .query(
                "SELECT id, name, area, prefecture_name, school_type, group_id
                 FROM schools
                 WHERE id = ANY($1)
                 ORDER BY id",
                &[&school_ids],
            )
            .await?
            .into_iter()
            .map(|row| School {
                id: row.get("id"),
                name: row.get("name"),
                area: row.get("area"),
                prefecture_name: row.get("prefecture_name"),
                school_type: row.get("school_type"),
                group_id: row.get("group_id"),
            })
            .collect();
        let school_lookup: BTreeSet<String> =
            schools.iter().map(|school| school.id.clone()).collect();

        let events = client
            .query(
                "SELECT
                    id,
                    school_id,
                    title,
                    event_category,
                    is_open_day,
                    is_featured,
                    priority_weight,
                    to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS starts_at,
                    placement_tags,
                    is_active
                 FROM events
                 WHERE school_id = ANY($1)
                   AND is_active = TRUE
                 ORDER BY id",
                &[&school_ids],
            )
            .await?
            .into_iter()
            .map(|row| -> Result<Event> {
                let placement_tags = row
                    .get::<_, Vec<String>>("placement_tags")
                    .into_iter()
                    .map(|value| parse_placement_kind(&value))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Event {
                    id: row.get("id"),
                    school_id: row.get("school_id"),
                    title: row.get("title"),
                    event_category: row.get("event_category"),
                    is_open_day: row.get("is_open_day"),
                    is_featured: row.get("is_featured"),
                    priority_weight: row.get("priority_weight"),
                    starts_at: row.get("starts_at"),
                    placement_tags,
                    is_active: row.get("is_active"),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let stations: Vec<Station> = client
            .query(
                "SELECT id, name, line_name, line_id, latitude, longitude
                 FROM stations
                 WHERE id = ANY($1)
                 ORDER BY id",
                &[&station_ids],
            )
            .await?
            .into_iter()
            .map(|row| Station {
                id: row.get("id"),
                name: row.get("name"),
                line_name: row.get("line_name"),
                line_id: row.get("line_id"),
                latitude: row.get("latitude"),
                longitude: row.get("longitude"),
            })
            .collect();
        let station_lookup: BTreeSet<String> =
            stations.iter().map(|station| station.id.clone()).collect();

        let popularity_snapshots = client
            .query(
                "SELECT
                    school_id,
                    popularity_score,
                    total_events,
                    school_view_count,
                    school_save_count,
                    event_view_count,
                    apply_click_count,
                    share_count,
                    search_execute_count
                 FROM popularity_snapshots
                 WHERE school_id = ANY($1)
                 ORDER BY school_id",
                &[&school_ids],
            )
            .await?
            .into_iter()
            .map(|row| PopularitySnapshot {
                school_id: row.get("school_id"),
                popularity_score: row.get("popularity_score"),
                total_events: row.get("total_events"),
                school_view_count: row.get("school_view_count"),
                school_save_count: row.get("school_save_count"),
                event_view_count: row.get("event_view_count"),
                apply_click_count: row.get("apply_click_count"),
                share_count: row.get("share_count"),
                search_execute_count: row.get("search_execute_count"),
            })
            .collect();

        let areas: Vec<String> = schools.iter().map(|school| school.area.clone()).collect();
        let area_affinity_snapshots = if areas.is_empty() {
            Vec::new()
        } else {
            client
                .query(
                    "SELECT area, affinity_score, event_count, search_execute_count
                     FROM area_affinity_snapshots
                     WHERE area = ANY($1)
                     ORDER BY area",
                    &[&areas],
                )
                .await?
                .into_iter()
                .map(|row| AreaAffinitySnapshot {
                    area: row.get("area"),
                    affinity_score: row.get("affinity_score"),
                    event_count: row.get("event_count"),
                    search_execute_count: row.get("search_execute_count"),
                })
                .collect()
        };

        let user_affinity_snapshots = if let Some(user_id) = query.user_id.as_deref() {
            client
                .query(
                    "SELECT user_id, school_id, affinity_score, event_count
                     FROM user_affinity_snapshots
                     WHERE user_id = $1
                       AND school_id = ANY($2)
                     ORDER BY school_id",
                    &[&user_id, &school_ids],
                )
                .await?
                .into_iter()
                .map(|row| UserAffinitySnapshot {
                    user_id: row.get("user_id"),
                    school_id: row.get("school_id"),
                    affinity_score: row.get("affinity_score"),
                    event_count: row.get("event_count"),
                })
                .collect()
        } else {
            Vec::new()
        };

        let school_station_links = candidate_links
            .iter()
            .filter(|link| {
                school_lookup.contains(link.school_id.as_str())
                    && station_lookup.contains(link.station_id.as_str())
            })
            .cloned()
            .collect();

        Ok(RankingDataset {
            schools,
            events,
            stations,
            school_station_links,
            popularity_snapshots,
            user_affinity_snapshots,
            area_affinity_snapshots,
        })
    }

    pub async fn load_event_school_id(&self, event_id: &str) -> Result<Option<String>> {
        let client = self.connect().await?;
        Ok(client
            .query_opt("SELECT school_id FROM events WHERE id = $1", &[&event_id])
            .await
            .map(|row| row.map(|row| row.get("school_id")))?)
    }
}

fn load_trace_hash_salt() -> String {
    match std::env::var("GEO_LINE_RANKER_TRACE_HASH_SALT") {
        Ok(value) if !value.trim().is_empty() => value,
        Ok(_) | Err(_) => {
            let generated = Uuid::new_v4().to_string();
            tracing::warn!(
                "GEO_LINE_RANKER_TRACE_HASH_SALT is unset; generated an ephemeral trace hash salt for this process"
            );
            generated
        }
    }
}

pub fn is_foreign_key_violation(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<tokio_postgres::Error>()
            .and_then(|pg_error| pg_error.code())
            .is_some_and(|code| *code == tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION)
    })
}

pub fn user_event_reference_validation_message(error: &anyhow::Error) -> Option<String> {
    error.chain().find_map(|cause| {
        let message = cause.to_string();
        message
            .strip_prefix(USER_EVENT_REFERENCE_VALIDATION_PREFIX)
            .map(str::to_string)
    })
}

async fn insert_user_event(client: &(impl GenericClient + Sync), event: &UserEvent) -> Result<i64> {
    let school_id = resolve_user_event_school_id(client, event).await?;
    let occurred_at = parse_rfc3339_utc("occurred_at", &event.occurred_at)?;
    let row = client
        .query_one(
            "INSERT INTO user_events (
                user_id,
                school_id,
                event_type,
                event_id,
                target_station_id,
                occurred_at,
                payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id",
            &[
                &event.user_id,
                &school_id,
                &event.event_kind.as_str(),
                &event.event_id,
                &event.target_station_id,
                &occurred_at,
                &event.payload,
            ],
        )
        .await?;
    Ok(row.get("id"))
}

async fn resolve_user_event_school_id(
    client: &(impl GenericClient + Sync),
    event: &UserEvent,
) -> Result<Option<String>> {
    let Some(event_id) = event.event_id.as_deref() else {
        return Ok(event.school_id.clone());
    };

    let event_school_id = client
        .query_opt("SELECT school_id FROM events WHERE id = $1", &[&event_id])
        .await?
        .map(|row| row.get::<_, String>("school_id"));

    let Some(event_school_id) = event_school_id else {
        return Ok(event.school_id.clone());
    };

    if let Some(school_id) = event.school_id.as_deref() {
        ensure!(
            school_id == event_school_id,
            "{USER_EVENT_REFERENCE_VALIDATION_PREFIX}event_id {event_id} belongs to school_id {event_school_id}, not {school_id}"
        );
    }

    Ok(Some(event_school_id))
}

fn parse_rfc3339_utc(field_name: &str, value: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("{field_name} must be RFC3339: {value}"))?
        .with_timezone(&Utc))
}

fn parse_optional_event_time(value: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(Some(timestamp.with_timezone(&Utc)));
    }

    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").with_context(|| {
        format!("starts_at must be RFC3339 timestamp or YYYY-MM-DD date: {value}")
    })?;
    let naive = date
        .and_hms_opt(0, 0, 0)
        .with_context(|| format!("starts_at date is out of range: {value}"))?;
    Ok(Some(Utc.from_utc_datetime(&naive)))
}

async fn insert_job(client: &(impl GenericClient + Sync), job: &NewJob) -> Result<i64> {
    if is_reusable_global_refresh(job) {
        return insert_or_reuse_queued_global_refresh(client, job).await;
    }

    insert_job_row(client, job).await
}

fn is_reusable_global_refresh(job: &NewJob) -> bool {
    job.job_type == JobType::RefreshPopularitySnapshot
        && job
            .payload
            .as_object()
            .is_some_and(serde_json::Map::is_empty)
}

async fn insert_or_reuse_queued_global_refresh(
    client: &(impl GenericClient + Sync),
    job: &NewJob,
) -> Result<i64> {
    client
        .query_one(
            "SELECT pg_advisory_xact_lock($1, $2)",
            &[
                &JOB_COALESCE_LOCK_NAMESPACE,
                &POPULARITY_REFRESH_COALESCE_LOCK_KEY,
            ],
        )
        .await?;

    if let Some(row) = client
        .query_opt(
            "WITH existing_job AS (
                 SELECT id
                 FROM job_queue
                 WHERE job_type = $1
                   AND payload = $2
                   AND status = 'queued'
                   AND attempts = 0
                   AND last_error IS NULL
                 ORDER BY id
                 LIMIT 1
                 FOR UPDATE
             )
             UPDATE job_queue
             SET run_after = LEAST(run_after, NOW()),
                 updated_at = NOW()
             WHERE id = (SELECT id FROM existing_job)
             RETURNING id",
            &[&job.job_type.as_str(), &job.payload],
        )
        .await?
    {
        return Ok(row.get("id"));
    }

    insert_job_row(client, job).await
}

async fn insert_job_row(client: &(impl GenericClient + Sync), job: &NewJob) -> Result<i64> {
    let row = client
        .query_one(
            "INSERT INTO job_queue (job_type, payload, max_attempts)
             VALUES ($1, $2, $3)
             RETURNING id",
            &[&job.job_type.as_str(), &job.payload, &job.max_attempts],
        )
        .await?;
    Ok(row.get("id"))
}

fn job_queue_row(row: Row) -> Result<JobQueueRow> {
    Ok(JobQueueRow {
        id: row.get("id"),
        job_type: row.get("job_type"),
        payload: row.get("payload"),
        status: row.get("status"),
        attempts: row.get("attempts"),
        max_attempts: row.get("max_attempts"),
        locked_by: row.get("locked_by"),
        locked_at: row.get("locked_at"),
        last_error: row.get("last_error"),
        run_after: row.get("run_after"),
        completed_at: row.get("completed_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn station_from_row(row: Row) -> Station {
    Station {
        id: row.get("id"),
        name: row.get("name"),
        line_name: row.get("line_name"),
        line_id: row.get("line_id"),
        latitude: row.get("latitude"),
        longitude: row.get("longitude"),
    }
}

fn stable_id(prefix: &str, value: &str) -> String {
    let normalized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    let slug = normalized
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_");
    if slug.is_empty() {
        format!("{}_unknown", prefix)
    } else {
        format!("{prefix}_{slug}")
    }
}

fn city_area_stable_id(area_name: &str, prefecture_name: Option<&str>) -> String {
    let key = match non_empty(prefecture_name) {
        Some(prefecture_name) => format!("{prefecture_name}:{area_name}"),
        None => area_name.to_string(),
    };
    stable_id("area", &key)
}

fn job_attempt_row(row: Row) -> Result<JobAttemptRow> {
    Ok(JobAttemptRow {
        attempt_number: row.get("attempt_number"),
        status: row.get("status"),
        error_message: row.get("error_message"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
    })
}

async fn acquire_schema_migration_lock(client: &Client) -> Result<()> {
    client
        .query_one(
            "SELECT pg_advisory_lock($1, $2)",
            &[&SCHEMA_MIGRATION_LOCK_NAMESPACE, &SCHEMA_MIGRATION_LOCK_KEY],
        )
        .await?;
    Ok(())
}

async fn release_schema_migration_lock(client: &Client) -> Result<()> {
    let row = client
        .query_one(
            "SELECT pg_advisory_unlock($1, $2) AS unlocked",
            &[&SCHEMA_MIGRATION_LOCK_NAMESPACE, &SCHEMA_MIGRATION_LOCK_KEY],
        )
        .await?;
    anyhow::ensure!(
        row.get::<_, bool>("unlocked"),
        "failed to release schema migration advisory lock"
    );
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ImportReportEntry {
    pub level: String,
    pub code: String,
    pub message: String,
    pub row_count: Option<i64>,
    pub details: Value,
}

#[derive(Debug, Clone)]
pub struct ImportSummary {
    pub normalized_rows: i64,
    pub core_rows: i64,
    pub report_entries: Vec<ImportReportEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventCsvRecord {
    pub event_id: String,
    pub school_id: String,
    pub title: String,
    #[serde(default = "default_event_category")]
    pub event_category: String,
    #[serde(default)]
    pub is_open_day: bool,
    #[serde(default)]
    pub is_featured: bool,
    #[serde(default)]
    pub priority_weight: f64,
    #[serde(default)]
    pub starts_at: Option<String>,
    #[serde(default)]
    pub placement_tags: String,
}

impl EventCsvRecord {
    pub fn normalized_placement_tags(&self) -> Result<Vec<PlacementKind>> {
        self.placement_tags
            .split('|')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(parse_placement_kind)
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct ImportRunFileAudit {
    pub import_run_id: i64,
    pub logical_name: String,
    pub staged_path: String,
    pub checksum_sha256: String,
    pub size_bytes: i64,
    pub row_count: Option<i64>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct SourceManifestAudit {
    pub manifest_path: String,
    pub source_id: String,
    pub source_name: String,
    pub manifest_version: i32,
    pub parser_version: String,
    pub manifest_json: Value,
}

#[derive(Debug, Clone)]
pub struct DeriveLinksSummary {
    pub link_rows: i64,
    pub report_entries: Vec<ImportReportEntry>,
}

#[derive(Debug, Clone)]
pub struct CrawlRunState {
    pub crawl_run_id: i64,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct CrawlFetchLogEntry {
    pub crawl_run_id: i64,
    pub logical_name: String,
    pub target_url: String,
    pub final_url: Option<String>,
    pub http_status: Option<i32>,
    pub checksum_sha256: Option<String>,
    pub size_bytes: Option<i64>,
    pub staged_path: Option<String>,
    pub fetch_status: String,
    pub content_changed: Option<bool>,
    pub details: Value,
}

#[derive(Debug, Clone)]
pub struct StoredCrawlFetchLog {
    pub logical_name: String,
    pub target_url: String,
    pub final_url: Option<String>,
    pub http_status: Option<i32>,
    pub checksum_sha256: Option<String>,
    pub size_bytes: Option<i64>,
    pub staged_path: Option<String>,
    pub fetch_status: String,
    pub content_changed: Option<bool>,
    pub details: Value,
}

#[derive(Debug, Clone)]
pub struct StoredCrawlParseError {
    pub logical_name: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct CrawlParseReportEntry {
    pub crawl_run_id: i64,
    pub logical_name: Option<String>,
    pub level: String,
    pub code: String,
    pub message: String,
    pub parsed_rows: Option<i64>,
    pub details: Value,
}

#[derive(Debug, Clone)]
pub struct CrawlDedupeReportEntry {
    pub crawl_run_id: i64,
    pub dedupe_key: String,
    pub kept_event_id: String,
    pub dropped_event_id: String,
    pub reason: String,
    pub details: Value,
}

#[derive(Debug, Clone)]
pub struct CrawlParseErrorSnapshot {
    pub logical_name: Option<String>,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct CrawlRunHealthSnapshot {
    pub crawl_run_id: i64,
    pub source_id: String,
    pub parser_key: String,
    pub parser_version: String,
    pub status: String,
    pub fetched_targets: i64,
    pub parsed_rows: i64,
    pub imported_rows: i64,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub fetch_status_counts: BTreeMap<String, i64>,
    pub parse_level_counts: BTreeMap<String, i64>,
    pub dedupe_count: i64,
    pub latest_error: Option<CrawlParseErrorSnapshot>,
}

#[derive(Debug, Clone)]
pub struct CrawlRunHealthPage {
    pub total_runs: i64,
    pub runs: Vec<CrawlRunHealthSnapshot>,
}

#[derive(Debug, Clone)]
pub struct CandidateProjectionRow {
    pub school_id: String,
    pub school_name: String,
    pub school_area: String,
    pub school_type: String,
    pub station_id: String,
    pub station_name: String,
    pub station_line_name: String,
    pub station_latitude: f64,
    pub station_longitude: f64,
    pub walking_minutes: u16,
    pub distance_meters: u32,
    pub hop_distance: u8,
    pub open_day_count: i64,
    pub popularity_score: f64,
}

#[async_trait]
impl RecommendationRepository for PgRepository {
    async fn health_check(&self) -> Result<()> {
        let client = self.connect().await?;
        client.simple_query("SELECT 1").await?;
        Ok(())
    }

    async fn ready_check(&self) -> Result<()> {
        let client = self.connect().await?;
        let ready_tables = client
            .query(
                "SELECT table_name
                 FROM information_schema.tables
                 WHERE table_schema = 'public'
                   AND table_name = ANY($1)",
                &[&REQUIRED_READY_TABLES.to_vec()],
            )
            .await?
            .into_iter()
            .map(|row| row.get::<_, String>("table_name"))
            .collect::<BTreeSet<_>>();
        let missing_tables = REQUIRED_READY_TABLES
            .into_iter()
            .filter(|table_name| !ready_tables.contains(*table_name))
            .collect::<Vec<_>>();
        if !missing_tables.is_empty() {
            anyhow::bail!(
                "missing required PostgreSQL schema: {}",
                missing_tables.join(", ")
            );
        }
        Ok(())
    }

    async fn load_dataset(&self, query: &RankingQuery) -> Result<RankingDataset> {
        let client = self.connect().await?;

        let schools = client
            .query(
                "SELECT id, name, area, prefecture_name, school_type, group_id FROM schools ORDER BY id",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| School {
                id: row.get("id"),
                name: row.get("name"),
                area: row.get("area"),
                prefecture_name: row.get("prefecture_name"),
                school_type: row.get("school_type"),
                group_id: row.get("group_id"),
            })
            .collect();

        let events = client
            .query(
                "SELECT
                    id,
                    school_id,
                    title,
                    event_category,
                    is_open_day,
                    is_featured,
                    priority_weight,
                    to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS starts_at,
                    placement_tags,
                    is_active
                 FROM events
                 WHERE is_active = TRUE
                 ORDER BY id",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| -> Result<Event> {
                let placement_tags = row
                    .get::<_, Vec<String>>("placement_tags")
                    .into_iter()
                    .map(|value| parse_placement_kind(&value))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Event {
                    id: row.get("id"),
                    school_id: row.get("school_id"),
                    title: row.get("title"),
                    event_category: row.get("event_category"),
                    is_open_day: row.get("is_open_day"),
                    is_featured: row.get("is_featured"),
                    priority_weight: row.get("priority_weight"),
                    starts_at: row.get("starts_at"),
                    placement_tags,
                    is_active: row.get("is_active"),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let stations = client
            .query(
                "SELECT id, name, line_name, line_id, latitude, longitude FROM stations ORDER BY id",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| Station {
                id: row.get("id"),
                name: row.get("name"),
                line_name: row.get("line_name"),
                line_id: row.get("line_id"),
                latitude: row.get("latitude"),
                longitude: row.get("longitude"),
            })
            .collect();

        let school_station_links = client
            .query(
                "SELECT school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name FROM school_station_links ORDER BY school_id, station_id",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| SchoolStationLink {
                school_id: row.get("school_id"),
                station_id: row.get("station_id"),
                walking_minutes: row.get::<_, i16>("walking_minutes") as u16,
                distance_meters: row.get::<_, i32>("distance_meters") as u32,
                hop_distance: row.get::<_, i16>("hop_distance") as u8,
                line_name: row.get("line_name"),
            })
            .collect();

        let popularity_snapshots = client
            .query(
                "SELECT
                    school_id,
                    popularity_score,
                    total_events,
                    school_view_count,
                    school_save_count,
                    event_view_count,
                    apply_click_count,
                    share_count,
                    search_execute_count
                 FROM popularity_snapshots
                 ORDER BY school_id",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| PopularitySnapshot {
                school_id: row.get("school_id"),
                popularity_score: row.get("popularity_score"),
                total_events: row.get("total_events"),
                school_view_count: row.get("school_view_count"),
                school_save_count: row.get("school_save_count"),
                event_view_count: row.get("event_view_count"),
                apply_click_count: row.get("apply_click_count"),
                share_count: row.get("share_count"),
                search_execute_count: row.get("search_execute_count"),
            })
            .collect();

        let area_affinity_snapshots = client
            .query(
                "SELECT area, affinity_score, event_count, search_execute_count
                 FROM area_affinity_snapshots
                 ORDER BY area",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| AreaAffinitySnapshot {
                area: row.get("area"),
                affinity_score: row.get("affinity_score"),
                event_count: row.get("event_count"),
                search_execute_count: row.get("search_execute_count"),
            })
            .collect();

        let user_affinity_snapshots = if let Some(user_id) = query.user_id.as_deref() {
            client
                .query(
                    "SELECT user_id, school_id, affinity_score, event_count
                     FROM user_affinity_snapshots
                     WHERE user_id = $1
                     ORDER BY school_id",
                    &[&user_id],
                )
                .await?
                .into_iter()
                .map(|row| UserAffinitySnapshot {
                    user_id: row.get("user_id"),
                    school_id: row.get("school_id"),
                    affinity_score: row.get("affinity_score"),
                    event_count: row.get("event_count"),
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok(RankingDataset {
            schools,
            events,
            stations,
            school_station_links,
            popularity_snapshots,
            user_affinity_snapshots,
            area_affinity_snapshots,
        })
    }

    async fn record_trace(&self, trace: &RecommendationTrace) -> Result<()> {
        let client = self.connect().await?;
        client
            .execute(
                "INSERT INTO recommendation_traces (
                    request_payload,
                    response_payload,
                    trace_payload,
                    fallback_stage,
                    algorithm_version
                ) VALUES ($1, $2, $3, $4, $5)",
                &[
                    &trace.request_payload,
                    &trace.response_payload,
                    &trace.trace_payload,
                    &trace.fallback_stage,
                    &trace.algorithm_version,
                ],
            )
            .await?;
        Ok(())
    }

    async fn record_user_event(&self, event: &UserEvent) -> Result<i64> {
        let client = self.connect().await?;
        insert_user_event(&client, event).await
    }

    async fn enqueue_job(&self, job: &NewJob) -> Result<i64> {
        let mut client = self.connect().await?;
        let transaction = client.transaction().await?;
        let job_id = insert_job(&transaction, job).await?;
        transaction.commit().await?;
        Ok(job_id)
    }

    async fn claim_next_job(&self, worker_id: &str) -> Result<Option<ClaimedJob>> {
        let mut client = self.connect().await?;
        let transaction = client.transaction().await?;

        // Recover jobs orphaned by crashed workers before selecting the next claimable row.
        transaction
            .execute(
                "UPDATE job_attempts AS attempt
                 SET status = 'failed',
                     error_message = COALESCE(attempt.error_message, $2),
                     finished_at = COALESCE(attempt.finished_at, NOW())
                 FROM job_queue AS job
                 WHERE attempt.job_id = job.id
                   AND attempt.status = 'running'
                   AND job.status = 'running'
                   AND job.locked_at IS NOT NULL
                   AND job.locked_at <= NOW() - ($1::bigint * INTERVAL '1 second')",
                &[&STALE_JOB_LOCK_TIMEOUT_SECS, &STALE_JOB_LOCK_ERROR],
            )
            .await?;
        transaction
            .execute(
                "UPDATE job_queue
                 SET status = 'failed',
                     locked_at = NULL,
                     locked_by = NULL,
                     last_error = $2,
                     completed_at = NOW(),
                     updated_at = NOW()
                 WHERE status = 'running'
                   AND locked_at IS NOT NULL
                   AND locked_at <= NOW() - ($1::bigint * INTERVAL '1 second')
                   AND attempts >= max_attempts",
                &[&STALE_JOB_LOCK_TIMEOUT_SECS, &STALE_JOB_LOCK_ERROR],
            )
            .await?;

        let row = transaction
            .query_opt(
                "WITH next_job AS (
                    SELECT id
                    FROM job_queue
                    WHERE (
                            status = 'queued'
                            AND run_after <= NOW()
                          )
                       OR (
                            status = 'running'
                            AND locked_at IS NOT NULL
                            AND locked_at <= NOW() - ($2::bigint * INTERVAL '1 second')
                            AND attempts < max_attempts
                          )
                    ORDER BY created_at ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE job_queue
                SET status = 'running',
                    attempts = attempts + 1,
                    locked_at = NOW(),
                    locked_by = $1,
                    last_error = NULL,
                    completed_at = NULL,
                    updated_at = NOW()
                WHERE id = (SELECT id FROM next_job)
                RETURNING id, job_type, payload, attempts, max_attempts",
                &[&worker_id, &STALE_JOB_LOCK_TIMEOUT_SECS],
            )
            .await?;

        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };

        let job_type_raw: String = row.get("job_type");
        let job_type = JobType::parse(&job_type_raw)
            .with_context(|| format!("unsupported queued job type: {job_type_raw}"))?;
        let job_id: i64 = row.get("id");
        let attempt_number: i32 = row.get("attempts");
        let max_attempts: i32 = row.get("max_attempts");
        let payload: Value = row.get("payload");
        let attempt_row = transaction
            .query_one(
                "INSERT INTO job_attempts (job_id, attempt_number, status)
                 VALUES ($1, $2, 'running')
                 RETURNING id",
                &[&job_id, &attempt_number],
            )
            .await?;
        let attempt_id = attempt_row.get("id");
        transaction.commit().await?;

        Ok(Some(ClaimedJob {
            job_id,
            attempt_id,
            attempt_number,
            max_attempts,
            job_type,
            payload,
        }))
    }

    async fn mark_job_succeeded(&self, job_id: i64, attempt_id: i64) -> Result<()> {
        let mut client = self.connect().await?;
        let transaction = client.transaction().await?;
        let attempt = transaction
            .query_opt(
                "SELECT attempt_number
                 FROM job_attempts
                 WHERE id = $1
                   AND job_id = $2
                   AND status = 'running'
                 FOR UPDATE",
                &[&attempt_id, &job_id],
            )
            .await?;
        let Some(attempt) = attempt else {
            transaction.rollback().await?;
            return Ok(());
        };
        let attempt_number: i32 = attempt.get("attempt_number");
        let updated = transaction
            .execute(
                "UPDATE job_queue
                 SET status = 'succeeded',
                     locked_at = NULL,
                     locked_by = NULL,
                     last_error = NULL,
                     completed_at = NOW(),
                     updated_at = NOW()
                 WHERE id = $1
                   AND status = 'running'
                   AND attempts = $2",
                &[&job_id, &attempt_number],
            )
            .await?;
        if updated == 0 {
            transaction.rollback().await?;
            return Ok(());
        }
        transaction
            .execute(
                "UPDATE job_attempts
                 SET status = 'succeeded',
                     finished_at = NOW()
                 WHERE id = $1",
                &[&attempt_id],
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn mark_job_failed(
        &self,
        job_id: i64,
        attempt_id: i64,
        error_message: &str,
        retry_delay_secs: u64,
    ) -> Result<()> {
        let mut client = self.connect().await?;
        let transaction = client.transaction().await?;
        let attempt = transaction
            .query_opt(
                "SELECT attempt_number
                 FROM job_attempts
                 WHERE id = $1
                   AND job_id = $2
                   AND status = 'running'
                 FOR UPDATE",
                &[&attempt_id, &job_id],
            )
            .await?;
        let Some(attempt) = attempt else {
            transaction.rollback().await?;
            return Ok(());
        };
        let attempt_number: i32 = attempt.get("attempt_number");
        let row = transaction
            .query_opt(
                "SELECT attempts, max_attempts
                 FROM job_queue
                 WHERE id = $1
                   AND status = 'running'
                   AND attempts = $2
                 FOR UPDATE",
                &[&job_id, &attempt_number],
            )
            .await?;
        let Some(row) = row else {
            transaction.rollback().await?;
            return Ok(());
        };
        let attempts: i32 = row.get("attempts");
        let max_attempts: i32 = row.get("max_attempts");
        let next_status = if attempts >= max_attempts {
            "failed"
        } else {
            "queued"
        };

        if next_status == "queued" {
            transaction
                .execute(
                    "UPDATE job_queue
                     SET status = 'queued',
                         locked_at = NULL,
                         locked_by = NULL,
                         last_error = $2,
                         run_after = NOW() + make_interval(secs => $3::INTEGER),
                         updated_at = NOW()
                     WHERE id = $1",
                    &[
                        &job_id,
                        &error_message,
                        &(retry_delay_secs.min(i32::MAX as u64) as i32),
                    ],
                )
                .await?;
        } else {
            transaction
                .execute(
                    "UPDATE job_queue
                     SET status = 'failed',
                         locked_at = NULL,
                         locked_by = NULL,
                         last_error = $2,
                         completed_at = NOW(),
                         updated_at = NOW()
                     WHERE id = $1",
                    &[&job_id, &error_message],
                )
                .await?;
        }

        transaction
            .execute(
                "UPDATE job_attempts
                 SET status = 'failed',
                     error_message = $2,
                     finished_at = NOW()
                 WHERE id = $1",
                &[&attempt_id, &error_message],
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn refresh_popularity_snapshots(
        &self,
        tuning: SnapshotTuning,
    ) -> Result<SnapshotRefreshStats> {
        let mut client = self.connect().await?;
        let transaction = client.transaction().await?;
        transaction
            .execute(
                "DELETE FROM popularity_snapshots
                 WHERE school_id NOT IN (SELECT id FROM schools)",
                &[],
            )
            .await?;
        let refreshed_rows = transaction
            .execute(
                "WITH school_event_scores AS (
                    SELECT
                        school.id AS school_id,
                        COALESCE(SUM(
                            CASE user_event.event_type
                                WHEN 'school_view' THEN 1.0
                                WHEN 'event_view' THEN 0.75
                                WHEN 'school_save' THEN 2.0
                                WHEN 'apply_click' THEN 2.5
                                WHEN 'share' THEN 1.5
                                ELSE 0.0
                            END
                        ), 0.0) AS raw_score,
                        COUNT(user_event.id) AS total_events,
                        COUNT(user_event.id) FILTER (WHERE user_event.event_type = 'school_view') AS school_view_count,
                        COUNT(user_event.id) FILTER (WHERE user_event.event_type = 'school_save') AS school_save_count,
                        COUNT(user_event.id) FILTER (WHERE user_event.event_type = 'event_view') AS event_view_count,
                        COUNT(user_event.id) FILTER (WHERE user_event.event_type = 'apply_click') AS apply_click_count,
                        COUNT(user_event.id) FILTER (WHERE user_event.event_type = 'share') AS share_count
                    FROM schools AS school
                    LEFT JOIN user_events AS user_event
                      ON user_event.school_id = school.id
                     AND user_event.event_type IN (
                         'school_view',
                         'school_save',
                         'event_view',
                         'apply_click',
                         'share'
                     )
                    GROUP BY school.id
                ),
                school_search_scores AS (
                    SELECT
                        link.school_id,
                        COUNT(user_event.id) AS search_execute_count
                    FROM school_station_links AS link
                    INNER JOIN user_events AS user_event
                      ON user_event.target_station_id = link.station_id
                     AND user_event.event_type = 'search_execute'
                    GROUP BY link.school_id
                ),
                school_scores AS (
                    SELECT
                        school.id AS school_id,
                        COALESCE(event_scores.raw_score, 0.0)
                            + COALESCE(search_scores.search_execute_count, 0) * $1::DOUBLE PRECISION AS raw_score,
                        COALESCE(event_scores.total_events, 0)
                            + COALESCE(search_scores.search_execute_count, 0) AS total_events,
                        COALESCE(event_scores.school_view_count, 0) AS school_view_count,
                        COALESCE(event_scores.school_save_count, 0) AS school_save_count,
                        COALESCE(event_scores.event_view_count, 0) AS event_view_count,
                        COALESCE(event_scores.apply_click_count, 0) AS apply_click_count,
                        COALESCE(event_scores.share_count, 0) AS share_count,
                        COALESCE(search_scores.search_execute_count, 0) AS search_execute_count
                    FROM schools AS school
                    LEFT JOIN school_event_scores AS event_scores
                      ON event_scores.school_id = school.id
                    LEFT JOIN school_search_scores AS search_scores
                      ON search_scores.school_id = school.id
                ),
                normalized AS (
                    SELECT
                        school_id,
                        CASE
                            WHEN MAX(raw_score) OVER () > 0 THEN raw_score / MAX(raw_score) OVER ()
                            ELSE 0.0
                        END AS popularity_score,
                        total_events,
                        school_view_count,
                        school_save_count,
                        event_view_count,
                        apply_click_count,
                        share_count,
                        search_execute_count
                    FROM school_scores
                )
                INSERT INTO popularity_snapshots (
                    school_id,
                    popularity_score,
                    total_events,
                    school_view_count,
                    school_save_count,
                    event_view_count,
                    apply_click_count,
                    share_count,
                    search_execute_count,
                    refreshed_at
                )
                SELECT
                    school_id,
                    popularity_score,
                    total_events,
                    school_view_count,
                    school_save_count,
                    event_view_count,
                    apply_click_count,
                    share_count,
                    search_execute_count,
                    NOW()
                FROM normalized
                ON CONFLICT (school_id) DO UPDATE
                SET popularity_score = EXCLUDED.popularity_score,
                    total_events = EXCLUDED.total_events,
                    school_view_count = EXCLUDED.school_view_count,
                    school_save_count = EXCLUDED.school_save_count,
                    event_view_count = EXCLUDED.event_view_count,
                    apply_click_count = EXCLUDED.apply_click_count,
                    share_count = EXCLUDED.share_count,
                    search_execute_count = EXCLUDED.search_execute_count,
                    refreshed_at = EXCLUDED.refreshed_at",
                &[&tuning.search_execute_school_signal_weight],
            )
            .await? as i64;

        transaction
            .execute(
                "DELETE FROM area_affinity_snapshots
                 WHERE area NOT IN (SELECT DISTINCT area FROM schools)",
                &[],
            )
            .await?;
        let related_rows = transaction
            .execute(
                "WITH area_event_scores AS (
                    SELECT
                        school.area,
                        COALESCE(SUM(
                            CASE user_event.event_type
                                WHEN 'school_view' THEN 1.0
                                WHEN 'event_view' THEN 0.75
                                WHEN 'school_save' THEN 2.0
                                WHEN 'apply_click' THEN 2.5
                                WHEN 'share' THEN 1.5
                                ELSE 0.0
                            END
                        ), 0.0) AS raw_score,
                        COUNT(user_event.id) AS event_count
                    FROM schools AS school
                    LEFT JOIN user_events AS user_event
                      ON user_event.school_id = school.id
                     AND user_event.event_type IN (
                         'school_view',
                         'school_save',
                         'event_view',
                         'apply_click',
                         'share'
                     )
                    GROUP BY school.area
                ),
                area_search_scores AS (
                    SELECT
                        school.area,
                        COUNT(DISTINCT user_event.id) AS search_execute_count
                    FROM schools AS school
                    INNER JOIN school_station_links AS link
                      ON link.school_id = school.id
                    INNER JOIN user_events AS user_event
                      ON user_event.target_station_id = link.station_id
                     AND user_event.event_type = 'search_execute'
                    GROUP BY school.area
                ),
                area_scores AS (
                    SELECT
                        area.area,
                        COALESCE(event_scores.raw_score, 0.0)
                            + COALESCE(search_scores.search_execute_count, 0) * $1::DOUBLE PRECISION AS raw_score,
                        COALESCE(event_scores.event_count, 0)
                            + COALESCE(search_scores.search_execute_count, 0) AS event_count,
                        COALESCE(search_scores.search_execute_count, 0) AS search_execute_count
                    FROM (SELECT DISTINCT area FROM schools) AS area
                    LEFT JOIN area_event_scores AS event_scores
                      ON event_scores.area = area.area
                    LEFT JOIN area_search_scores AS search_scores
                      ON search_scores.area = area.area
                ),
                normalized AS (
                    SELECT
                        area,
                        CASE
                            WHEN MAX(raw_score) OVER () > 0 THEN raw_score / MAX(raw_score) OVER ()
                            ELSE 0.0
                        END AS affinity_score,
                        event_count,
                        search_execute_count
                    FROM area_scores
                )
                INSERT INTO area_affinity_snapshots (
                    area,
                    affinity_score,
                    event_count,
                    search_execute_count,
                    refreshed_at
                )
                SELECT area, affinity_score, event_count, search_execute_count, NOW()
                FROM normalized
                ON CONFLICT (area) DO UPDATE
                SET affinity_score = EXCLUDED.affinity_score,
                    event_count = EXCLUDED.event_count,
                    search_execute_count = EXCLUDED.search_execute_count,
                    refreshed_at = EXCLUDED.refreshed_at",
                &[&tuning.search_execute_area_signal_weight],
            )
            .await? as i64;
        transaction.commit().await?;

        Ok(SnapshotRefreshStats {
            refreshed_rows,
            related_rows,
        })
    }

    async fn refresh_user_affinity_snapshots(
        &self,
        user_id: Option<&str>,
    ) -> Result<SnapshotRefreshStats> {
        let mut client = self.connect().await?;
        let transaction = client.transaction().await?;
        transaction
            .execute(
                "DELETE FROM user_affinity_snapshots
                 WHERE $1::TEXT IS NULL OR user_id = $1",
                &[&user_id],
            )
            .await?;
        let refreshed_rows = transaction
            .execute(
                "WITH filtered AS (
                    SELECT
                        user_event.user_id,
                        user_event.school_id,
                        COALESCE(SUM(
                            CASE user_event.event_type
                                WHEN 'school_view' THEN 1.0
                                WHEN 'event_view' THEN 0.75
                                WHEN 'school_save' THEN 2.0
                                WHEN 'apply_click' THEN 2.5
                                WHEN 'share' THEN 1.5
                                ELSE 0.0
                            END
                        ), 0.0) AS raw_score,
                        COUNT(user_event.id) AS event_count
                    FROM user_events AS user_event
                    WHERE user_event.school_id IS NOT NULL
                      AND user_event.event_type IN (
                          'school_view',
                          'school_save',
                          'event_view',
                          'apply_click',
                          'share'
                      )
                      AND ($1::TEXT IS NULL OR user_event.user_id = $1)
                    GROUP BY user_event.user_id, user_event.school_id
                ),
                normalized AS (
                    SELECT
                        user_id,
                        school_id,
                        CASE
                            WHEN MAX(raw_score) OVER (PARTITION BY user_id) > 0
                                THEN raw_score / MAX(raw_score) OVER (PARTITION BY user_id)
                            ELSE 0.0
                        END AS affinity_score,
                        event_count
                    FROM filtered
                )
                INSERT INTO user_affinity_snapshots (
                    user_id,
                    school_id,
                    affinity_score,
                    event_count,
                    refreshed_at
                )
                SELECT user_id, school_id, affinity_score, event_count, NOW()
                FROM normalized
                ON CONFLICT (user_id, school_id) DO UPDATE
                SET affinity_score = EXCLUDED.affinity_score,
                    event_count = EXCLUDED.event_count,
                    refreshed_at = EXCLUDED.refreshed_at",
                &[&user_id],
            )
            .await? as i64;
        transaction.commit().await?;

        Ok(SnapshotRefreshStats {
            refreshed_rows,
            related_rows: 0,
        })
    }
}

pub async fn run_migrations(database_url: &str, migrations_dir: impl AsRef<Path>) -> Result<()> {
    let _migration_guard = MIGRATION_PROCESS_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let repo = PgRepository::new(database_url);
    let mut client = repo.connect().await?;
    acquire_schema_migration_lock(&client).await?;
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
        )
        .await?;

    let mut entries =
        fs::read_dir(migrations_dir.as_ref())?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sql") {
            continue;
        }
        let version = path
            .file_name()
            .and_then(|name| name.to_str())
            .context("migration file name must be valid UTF-8")?
            .to_string();
        let sql = fs::read_to_string(&path)
            .with_context(|| format!("failed to read migration {}", path.display()))?;
        let transaction = client.transaction().await?;
        let claimed = transaction
            .query_opt(
                "INSERT INTO schema_migrations (version)
                 VALUES ($1)
                 ON CONFLICT (version) DO NOTHING
                 RETURNING version",
                &[&version],
            )
            .await?;
        if claimed.is_none() {
            transaction.rollback().await?;
            continue;
        }
        transaction.batch_execute(&sql).await?;
        transaction.commit().await?;
    }

    release_schema_migration_lock(&client).await?;
    Ok(())
}

pub async fn begin_import_run(
    database_url: &str,
    manifest_path: impl AsRef<Path>,
    manifest: &SourceManifest,
    parser_version: &str,
) -> Result<i64> {
    let manifest = SourceManifestAudit {
        manifest_path: manifest_path.as_ref().display().to_string(),
        source_id: manifest.source_id.clone(),
        source_name: manifest.source_name.clone(),
        manifest_version: manifest.manifest_version as i32,
        parser_version: parser_version.to_string(),
        manifest_json: serde_json::to_value(manifest)?,
    };
    upsert_source_manifest(database_url, &manifest).await?;

    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    let row = client
        .query_one(
            "INSERT INTO import_runs (
                manifest_path,
                source_id,
                parser_version,
                status
            )
            VALUES ($1, $2, $3, 'running')
            RETURNING id",
            &[
                &manifest.manifest_path,
                &manifest.source_id,
                &manifest.parser_version,
            ],
        )
        .await?;
    Ok(row.get("id"))
}

pub async fn upsert_source_manifest(
    database_url: &str,
    manifest: &SourceManifestAudit,
) -> Result<()> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    client
        .execute(
            "INSERT INTO source_manifests (
                manifest_path,
                source_id,
                source_name,
                manifest_version,
                parser_version,
                manifest_json
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (manifest_path) DO UPDATE
            SET source_id = EXCLUDED.source_id,
                source_name = EXCLUDED.source_name,
                manifest_version = EXCLUDED.manifest_version,
                parser_version = EXCLUDED.parser_version,
                manifest_json = EXCLUDED.manifest_json,
                updated_at = NOW()",
            &[
                &manifest.manifest_path,
                &manifest.source_id,
                &manifest.source_name,
                &manifest.manifest_version,
                &manifest.parser_version,
                &manifest.manifest_json,
            ],
        )
        .await?;
    Ok(())
}

pub async fn upsert_import_run_file(database_url: &str, audit: &ImportRunFileAudit) -> Result<()> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;

    client
        .execute(
            "INSERT INTO import_run_files (
                import_run_id,
                logical_name,
                staged_path,
                checksum_sha256,
                size_bytes,
                row_count,
                status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (import_run_id, logical_name) DO UPDATE
            SET staged_path = EXCLUDED.staged_path,
                checksum_sha256 = EXCLUDED.checksum_sha256,
                size_bytes = EXCLUDED.size_bytes,
                row_count = EXCLUDED.row_count,
                status = EXCLUDED.status,
                updated_at = NOW()",
            &[
                &audit.import_run_id,
                &audit.logical_name,
                &audit.staged_path,
                &audit.checksum_sha256,
                &audit.size_bytes,
                &audit.row_count,
                &audit.status,
            ],
        )
        .await?;
    Ok(())
}

pub async fn record_import_report(
    database_url: &str,
    import_run_id: i64,
    entry: &ImportReportEntry,
) -> Result<()> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    client
        .execute(
            "INSERT INTO import_reports (
                import_run_id,
                level,
                code,
                message,
                row_count,
                details
            )
            VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                &import_run_id,
                &entry.level,
                &entry.code,
                &entry.message,
                &entry.row_count,
                &entry.details,
            ],
        )
        .await?;
    Ok(())
}

pub async fn finish_import_run(
    database_url: &str,
    import_run_id: i64,
    status: &str,
    total_rows: i64,
) -> Result<()> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    client
        .execute(
            "UPDATE import_runs
             SET status = $2,
                 total_rows = $3,
                 completed_at = NOW()
             WHERE id = $1",
            &[&import_run_id, &status, &total_rows],
        )
        .await?;
    Ok(())
}

pub async fn begin_crawl_run(
    database_url: &str,
    manifest: &SourceManifestAudit,
    parser_key: &str,
) -> Result<i64> {
    upsert_source_manifest(database_url, manifest).await?;

    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    let row = client
        .query_one(
            "INSERT INTO crawl_runs (
                manifest_path,
                source_id,
                parser_key,
                parser_version,
                status
            )
            VALUES ($1, $2, $3, $4, 'fetching')
            RETURNING id",
            &[
                &manifest.manifest_path,
                &manifest.source_id,
                &parser_key,
                &manifest.parser_version,
            ],
        )
        .await?;
    Ok(row.get("id"))
}

pub async fn latest_crawl_fetch_checksum(
    database_url: &str,
    manifest_path: &str,
    logical_name: &str,
    target_url: &str,
) -> Result<Option<String>> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    let row = client
        .query_opt(
            "SELECT fetch_log.checksum_sha256
             FROM crawl_fetch_logs AS fetch_log
             INNER JOIN crawl_runs AS run
               ON run.id = fetch_log.crawl_run_id
             WHERE run.manifest_path = $1
               AND fetch_log.logical_name = $2
               AND fetch_log.target_url = $3
               AND fetch_log.checksum_sha256 IS NOT NULL
             ORDER BY fetch_log.fetched_at DESC, fetch_log.id DESC
             LIMIT 1",
            &[&manifest_path, &logical_name, &target_url],
        )
        .await?;
    Ok(row.map(|row| row.get("checksum_sha256")))
}

pub async fn record_crawl_fetch_log(database_url: &str, entry: &CrawlFetchLogEntry) -> Result<()> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    client
        .execute(
            "INSERT INTO crawl_fetch_logs (
                crawl_run_id,
                logical_name,
                target_url,
                final_url,
                http_status,
                checksum_sha256,
                size_bytes,
                staged_path,
                fetch_status,
                content_changed,
                details
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (crawl_run_id, logical_name, target_url) DO UPDATE
            SET final_url = EXCLUDED.final_url,
                http_status = EXCLUDED.http_status,
                checksum_sha256 = EXCLUDED.checksum_sha256,
                size_bytes = EXCLUDED.size_bytes,
                staged_path = EXCLUDED.staged_path,
                fetch_status = EXCLUDED.fetch_status,
                content_changed = EXCLUDED.content_changed,
                details = EXCLUDED.details,
                fetched_at = NOW()",
            &[
                &entry.crawl_run_id,
                &entry.logical_name,
                &entry.target_url,
                &entry.final_url,
                &entry.http_status,
                &entry.checksum_sha256,
                &entry.size_bytes,
                &entry.staged_path,
                &entry.fetch_status,
                &entry.content_changed,
                &entry.details,
            ],
        )
        .await?;
    Ok(())
}

pub async fn mark_crawl_run_fetched(
    database_url: &str,
    crawl_run_id: i64,
    fetched_targets: i64,
) -> Result<()> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    client
        .execute(
            "UPDATE crawl_runs
             SET status = 'fetched',
                 fetched_targets = $2
             WHERE id = $1",
            &[&crawl_run_id, &fetched_targets],
        )
        .await?;
    Ok(())
}

pub async fn load_latest_fetched_crawl_run(
    database_url: &str,
    manifest_path: &str,
) -> Result<Option<CrawlRunState>> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    let row = client
        .query_opt(
            "SELECT id, status
             FROM crawl_runs
             WHERE manifest_path = $1
               AND status = 'fetched'
             ORDER BY id DESC
             LIMIT 1",
            &[&manifest_path],
        )
        .await?;
    Ok(row.map(|row| CrawlRunState {
        crawl_run_id: row.get("id"),
        status: row.get("status"),
    }))
}

pub async fn claim_latest_fetched_crawl_run(
    database_url: &str,
    manifest_path: &str,
) -> Result<Option<CrawlRunState>> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    let row = client
        .query_opt(
            "UPDATE crawl_runs
             SET status = 'parsing'
             WHERE manifest_path = $1
               AND status = 'fetched'
               AND id = (
                   SELECT id
                   FROM crawl_runs
                   WHERE manifest_path = $1
                     AND status = 'fetched'
                   ORDER BY id DESC
                   LIMIT 1
               )
             RETURNING id, status",
            &[&manifest_path],
        )
        .await?;
    Ok(row.map(|row| CrawlRunState {
        crawl_run_id: row.get("id"),
        status: row.get("status"),
    }))
}

pub async fn claim_fetched_crawl_run(
    database_url: &str,
    crawl_run_id: i64,
) -> Result<Option<CrawlRunState>> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    let row = client
        .query_opt(
            "UPDATE crawl_runs
             SET status = 'parsing'
             WHERE id = $1
               AND status = 'fetched'
             RETURNING id, status",
            &[&crawl_run_id],
        )
        .await?;
    Ok(row.map(|row| CrawlRunState {
        crawl_run_id: row.get("id"),
        status: row.get("status"),
    }))
}

pub async fn set_crawl_run_status(
    database_url: &str,
    crawl_run_id: i64,
    status: &str,
) -> Result<()> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    client
        .execute(
            "UPDATE crawl_runs
             SET status = $2
             WHERE id = $1",
            &[&crawl_run_id, &status],
        )
        .await?;
    Ok(())
}

pub async fn load_crawl_fetch_logs(
    database_url: &str,
    crawl_run_id: i64,
) -> Result<Vec<StoredCrawlFetchLog>> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    let rows = client
        .query(
            "SELECT
                logical_name,
                target_url,
                final_url,
                http_status,
                checksum_sha256,
                size_bytes,
                staged_path,
                fetch_status,
                content_changed,
                details
             FROM crawl_fetch_logs
             WHERE crawl_run_id = $1
             ORDER BY id ASC",
            &[&crawl_run_id],
        )
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| StoredCrawlFetchLog {
            logical_name: row.get("logical_name"),
            target_url: row.get("target_url"),
            final_url: row.get("final_url"),
            http_status: row.get("http_status"),
            checksum_sha256: row.get("checksum_sha256"),
            size_bytes: row.get("size_bytes"),
            staged_path: row.get("staged_path"),
            fetch_status: row.get("fetch_status"),
            content_changed: row.get("content_changed"),
            details: row.get("details"),
        })
        .collect())
}

pub async fn load_crawl_parse_errors(
    database_url: &str,
    crawl_run_id: i64,
) -> Result<Vec<StoredCrawlParseError>> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    let rows = client
        .query(
            "SELECT logical_name, code, message
             FROM crawl_parse_reports
             WHERE crawl_run_id = $1
               AND level = 'error'
               AND logical_name IS NOT NULL
             ORDER BY id ASC",
            &[&crawl_run_id],
        )
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| StoredCrawlParseError {
            logical_name: row.get("logical_name"),
            code: row.get("code"),
            message: row.get("message"),
        })
        .collect())
}

pub async fn load_crawl_run_health(
    database_url: &str,
    manifest_path: &str,
    limit: usize,
) -> Result<CrawlRunHealthPage> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    let total_runs = client
        .query_one(
            "SELECT COUNT(*) AS total_runs
             FROM crawl_runs
             WHERE manifest_path = $1",
            &[&manifest_path],
        )
        .await?
        .get::<_, i64>("total_runs");

    if total_runs == 0 {
        return Ok(CrawlRunHealthPage {
            total_runs: 0,
            runs: Vec::new(),
        });
    }

    let rows = client
        .query(
            "SELECT
                id,
                source_id,
                parser_key,
                parser_version,
                status,
                fetched_targets,
                parsed_rows,
                imported_rows,
                started_at::TEXT AS started_at,
                completed_at::TEXT AS completed_at
             FROM crawl_runs
             WHERE manifest_path = $1
             ORDER BY id DESC
             LIMIT $2",
            &[&manifest_path, &((limit.clamp(1, 100)) as i64)],
        )
        .await?;

    let mut runs = rows
        .into_iter()
        .map(|row| CrawlRunHealthSnapshot {
            crawl_run_id: row.get("id"),
            source_id: row.get("source_id"),
            parser_key: row.get("parser_key"),
            parser_version: row.get("parser_version"),
            status: row.get("status"),
            fetched_targets: row.get("fetched_targets"),
            parsed_rows: row.get("parsed_rows"),
            imported_rows: row.get("imported_rows"),
            started_at: row.get("started_at"),
            completed_at: row.get("completed_at"),
            fetch_status_counts: BTreeMap::new(),
            parse_level_counts: BTreeMap::new(),
            dedupe_count: 0,
            latest_error: None,
        })
        .collect::<Vec<_>>();

    let run_ids = runs.iter().map(|run| run.crawl_run_id).collect::<Vec<_>>();
    let run_positions = runs
        .iter()
        .enumerate()
        .map(|(index, run)| (run.crawl_run_id, index))
        .collect::<BTreeMap<_, _>>();

    let fetch_counts = client
        .query(
            "SELECT crawl_run_id, fetch_status, COUNT(*)::BIGINT AS count
             FROM crawl_fetch_logs
             WHERE crawl_run_id = ANY($1)
             GROUP BY crawl_run_id, fetch_status",
            &[&run_ids],
        )
        .await?;
    for row in fetch_counts {
        let crawl_run_id = row.get::<_, i64>("crawl_run_id");
        if let Some(index) = run_positions.get(&crawl_run_id) {
            runs[*index]
                .fetch_status_counts
                .insert(row.get("fetch_status"), row.get("count"));
        }
    }

    let parse_counts = client
        .query(
            "SELECT crawl_run_id, level, COUNT(*)::BIGINT AS count
             FROM crawl_parse_reports
             WHERE crawl_run_id = ANY($1)
             GROUP BY crawl_run_id, level",
            &[&run_ids],
        )
        .await?;
    for row in parse_counts {
        let crawl_run_id = row.get::<_, i64>("crawl_run_id");
        if let Some(index) = run_positions.get(&crawl_run_id) {
            runs[*index]
                .parse_level_counts
                .insert(row.get("level"), row.get("count"));
        }
    }

    let dedupe_counts = client
        .query(
            "SELECT crawl_run_id, COUNT(*)::BIGINT AS count
             FROM crawl_dedupe_reports
             WHERE crawl_run_id = ANY($1)
             GROUP BY crawl_run_id",
            &[&run_ids],
        )
        .await?;
    for row in dedupe_counts {
        let crawl_run_id = row.get::<_, i64>("crawl_run_id");
        if let Some(index) = run_positions.get(&crawl_run_id) {
            runs[*index].dedupe_count = row.get("count");
        }
    }

    let latest_errors = client
        .query(
            "SELECT DISTINCT ON (crawl_run_id)
                crawl_run_id,
                logical_name,
                code,
                message
             FROM crawl_parse_reports
             WHERE crawl_run_id = ANY($1)
               AND level = 'error'
             ORDER BY crawl_run_id, id DESC",
            &[&run_ids],
        )
        .await?;
    for row in latest_errors {
        let crawl_run_id = row.get::<_, i64>("crawl_run_id");
        if let Some(index) = run_positions.get(&crawl_run_id) {
            runs[*index].latest_error = Some(CrawlParseErrorSnapshot {
                logical_name: row.get("logical_name"),
                code: row.get("code"),
                message: row.get("message"),
            });
        }
    }

    Ok(CrawlRunHealthPage { total_runs, runs })
}

pub async fn record_crawl_parse_report(
    database_url: &str,
    entry: &CrawlParseReportEntry,
) -> Result<()> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    client
        .execute(
            "INSERT INTO crawl_parse_reports (
                crawl_run_id,
                logical_name,
                level,
                code,
                message,
                parsed_rows,
                details
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &entry.crawl_run_id,
                &entry.logical_name,
                &entry.level,
                &entry.code,
                &entry.message,
                &entry.parsed_rows,
                &entry.details,
            ],
        )
        .await?;
    Ok(())
}

pub async fn record_crawl_dedupe_report(
    database_url: &str,
    entry: &CrawlDedupeReportEntry,
) -> Result<()> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    client
        .execute(
            "INSERT INTO crawl_dedupe_reports (
                crawl_run_id,
                dedupe_key,
                kept_event_id,
                dropped_event_id,
                reason,
                details
            )
            VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                &entry.crawl_run_id,
                &entry.dedupe_key,
                &entry.kept_event_id,
                &entry.dropped_event_id,
                &entry.reason,
                &entry.details,
            ],
        )
        .await?;
    Ok(())
}

pub async fn finish_crawl_run(
    database_url: &str,
    crawl_run_id: i64,
    status: &str,
    fetched_targets: i64,
    parsed_rows: i64,
    imported_rows: i64,
) -> Result<()> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    client
        .execute(
            "UPDATE crawl_runs
             SET status = $2,
                 fetched_targets = $3,
                 parsed_rows = $4,
                 imported_rows = $5,
                 completed_at = NOW()
             WHERE id = $1",
            &[
                &crawl_run_id,
                &status,
                &fetched_targets,
                &parsed_rows,
                &imported_rows,
            ],
        )
        .await?;
    Ok(())
}

pub async fn load_candidate_projection_rows(
    database_url: &str,
) -> Result<Vec<CandidateProjectionRow>> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    let rows = client
        .query(
            "WITH event_stats AS (
                SELECT
                    school_id,
                    COUNT(*) FILTER (WHERE is_open_day) AS open_day_count
                FROM events
                WHERE is_active = TRUE
                GROUP BY school_id
            )
            SELECT
                school.id AS school_id,
                school.name AS school_name,
                school.area AS school_area,
                school.school_type AS school_type,
                station.id AS station_id,
                station.name AS station_name,
                station.line_name AS station_line_name,
                station.latitude AS station_latitude,
                station.longitude AS station_longitude,
                link.walking_minutes,
                link.distance_meters,
                link.hop_distance,
                COALESCE(event_stats.open_day_count, 0) AS open_day_count,
                COALESCE(popularity.popularity_score, 0.0) AS popularity_score
            FROM school_station_links AS link
            INNER JOIN schools AS school
              ON school.id = link.school_id
            INNER JOIN stations AS station
              ON station.id = link.station_id
            LEFT JOIN event_stats
              ON event_stats.school_id = school.id
            LEFT JOIN popularity_snapshots AS popularity
              ON popularity.school_id = school.id
            ORDER BY school.id ASC, station.id ASC",
            &[],
        )
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| CandidateProjectionRow {
            school_id: row.get("school_id"),
            school_name: row.get("school_name"),
            school_area: row.get("school_area"),
            school_type: row.get("school_type"),
            station_id: row.get("station_id"),
            station_name: row.get("station_name"),
            station_line_name: row.get("station_line_name"),
            station_latitude: row.get("station_latitude"),
            station_longitude: row.get("station_longitude"),
            walking_minutes: row.get::<_, i16>("walking_minutes") as u16,
            distance_meters: row.get::<_, i32>("distance_meters") as u32,
            hop_distance: row.get::<_, i16>("hop_distance") as u8,
            open_day_count: row.get("open_day_count"),
            popularity_score: row.get("popularity_score"),
        })
        .collect())
}

pub async fn import_jp_school_codes(
    database_url: &str,
    records: &[SchoolCodeRecord],
) -> Result<ImportSummary> {
    let repo = PgRepository::new(database_url);
    let mut client = repo.connect().await?;
    let transaction = client.transaction().await?;

    for record in records {
        transaction
            .execute(
                "INSERT INTO jp_school_codes (
                    school_code,
                    school_id,
                    name,
                    prefecture_name,
                    city_name,
                    school_type,
                    raw_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (school_code) DO UPDATE
                SET school_id = EXCLUDED.school_id,
                    name = EXCLUDED.name,
                    prefecture_name = EXCLUDED.prefecture_name,
                    city_name = EXCLUDED.city_name,
                    school_type = EXCLUDED.school_type,
                    raw_payload = EXCLUDED.raw_payload,
                    updated_at = NOW()",
                &[
                    &record.school_code,
                    &record.school_id,
                    &record.name,
                    &record.prefecture_name,
                    &record.city_name,
                    &record.school_type,
                    &serde_json::to_value(record)?,
                ],
            )
            .await?;

        transaction
            .execute(
                "INSERT INTO schools (id, name, area, prefecture_name, school_type, group_id)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (id) DO UPDATE
                 SET name = EXCLUDED.name,
                     area = EXCLUDED.area,
                     prefecture_name = EXCLUDED.prefecture_name,
                     school_type = EXCLUDED.school_type,
                     group_id = COALESCE(NULLIF(schools.group_id, ''), EXCLUDED.group_id)",
                &[
                    &record.school_id,
                    &record.name,
                    &record.city_name,
                    &record.prefecture_name,
                    &record.school_type,
                    &record.school_id,
                ],
            )
            .await?;
    }

    transaction.commit().await?;

    Ok(ImportSummary {
        normalized_rows: records.len() as i64,
        core_rows: records.len() as i64,
        report_entries: vec![ImportReportEntry {
            level: "info".to_string(),
            code: "jp_school_codes_imported".to_string(),
            message: "Imported school code rows into normalized and core tables.".to_string(),
            row_count: Some(records.len() as i64),
            details: json!({ "table": "jp_school_codes" }),
        }],
    })
}

pub async fn import_jp_school_geodata(
    database_url: &str,
    records: &[SchoolGeodataRecord],
) -> Result<ImportSummary> {
    let repo = PgRepository::new(database_url);
    let mut client = repo.connect().await?;
    let transaction = client.transaction().await?;

    for record in records {
        transaction
            .execute(
                "INSERT INTO jp_school_geodata (
                    school_code,
                    school_id,
                    name,
                    prefecture_name,
                    city_name,
                    address,
                    school_type,
                    latitude,
                    longitude,
                    raw_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (school_code) DO UPDATE
                SET school_id = EXCLUDED.school_id,
                    name = EXCLUDED.name,
                    prefecture_name = EXCLUDED.prefecture_name,
                    city_name = EXCLUDED.city_name,
                    address = EXCLUDED.address,
                    school_type = EXCLUDED.school_type,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    raw_payload = EXCLUDED.raw_payload,
                    updated_at = NOW()",
                &[
                    &record.school_code,
                    &record.school_id,
                    &record.name,
                    &record.prefecture_name,
                    &record.city_name,
                    &record.address,
                    &record.school_type,
                    &record.latitude,
                    &record.longitude,
                    &serde_json::to_value(record)?,
                ],
            )
            .await?;

        transaction
            .execute(
                "INSERT INTO schools (id, name, area, prefecture_name, school_type, group_id)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (id) DO UPDATE
                 SET name = EXCLUDED.name,
                     area = EXCLUDED.area,
                     prefecture_name = EXCLUDED.prefecture_name,
                     school_type = EXCLUDED.school_type,
                     group_id = COALESCE(NULLIF(schools.group_id, ''), EXCLUDED.group_id)",
                &[
                    &record.school_id,
                    &record.name,
                    &record.city_name,
                    &record.prefecture_name,
                    &record.school_type,
                    &record.school_id,
                ],
            )
            .await?;
    }

    transaction.commit().await?;

    Ok(ImportSummary {
        normalized_rows: records.len() as i64,
        core_rows: records.len() as i64,
        report_entries: vec![ImportReportEntry {
            level: "info".to_string(),
            code: "jp_school_geodata_imported".to_string(),
            message: "Imported school geodata rows and refreshed school metadata.".to_string(),
            row_count: Some(records.len() as i64),
            details: json!({ "table": "jp_school_geodata" }),
        }],
    })
}

pub async fn import_jp_rail(
    database_url: &str,
    records: &[RailStationRecord],
) -> Result<ImportSummary> {
    let repo = PgRepository::new(database_url);
    let mut client = repo.connect().await?;
    let transaction = client.transaction().await?;

    for record in records {
        transaction
            .execute(
                "INSERT INTO jp_rail_stations (
                    station_code,
                    station_id,
                    station_name,
                    line_name,
                    prefecture_name,
                    latitude,
                    longitude,
                    raw_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (station_code) DO UPDATE
                SET station_id = EXCLUDED.station_id,
                    station_name = EXCLUDED.station_name,
                    line_name = EXCLUDED.line_name,
                    prefecture_name = EXCLUDED.prefecture_name,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    raw_payload = EXCLUDED.raw_payload,
                    updated_at = NOW()",
                &[
                    &record.station_code,
                    &record.station_id,
                    &record.station_name,
                    &record.line_name,
                    &record.prefecture_name,
                    &record.latitude,
                    &record.longitude,
                    &serde_json::to_value(record)?,
                ],
            )
            .await?;

        transaction
            .execute(
                "INSERT INTO stations (id, name, line_name, latitude, longitude)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (id) DO UPDATE
                 SET name = EXCLUDED.name,
                     line_name = EXCLUDED.line_name,
                     latitude = EXCLUDED.latitude,
                     longitude = EXCLUDED.longitude",
                &[
                    &record.station_id,
                    &record.station_name,
                    &record.line_name,
                    &record.latitude,
                    &record.longitude,
                ],
            )
            .await?;
    }

    transaction.commit().await?;

    Ok(ImportSummary {
        normalized_rows: records.len() as i64,
        core_rows: records.len() as i64,
        report_entries: vec![ImportReportEntry {
            level: "info".to_string(),
            code: "jp_rail_imported".to_string(),
            message: "Imported rail stations and refreshed core station rows.".to_string(),
            row_count: Some(records.len() as i64),
            details: json!({ "table": "jp_rail_stations" }),
        }],
    })
}

pub async fn import_jp_postal(
    database_url: &str,
    records: &[PostalCodeRecord],
) -> Result<ImportSummary> {
    let repo = PgRepository::new(database_url);
    let mut client = repo.connect().await?;
    let transaction = client.transaction().await?;

    for record in records {
        transaction
            .execute(
                "INSERT INTO jp_postal_codes (
                    postal_code,
                    prefecture_name,
                    city_name,
                    town_name,
                    raw_payload
                )
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (postal_code, prefecture_name, city_name, town_name) DO UPDATE
                SET raw_payload = EXCLUDED.raw_payload,
                    updated_at = NOW()",
                &[
                    &record.postal_code,
                    &record.prefecture_name,
                    &record.city_name,
                    &record.town_name,
                    &serde_json::to_value(record)?,
                ],
            )
            .await?;
    }

    transaction.commit().await?;

    Ok(ImportSummary {
        normalized_rows: records.len() as i64,
        core_rows: 0,
        report_entries: vec![ImportReportEntry {
            level: "info".to_string(),
            code: "jp_postal_imported".to_string(),
            message: "Imported postal code rows into the normalized table.".to_string(),
            row_count: Some(records.len() as i64),
            details: json!({ "table": "jp_postal_codes" }),
        }],
    })
}

pub async fn import_event_csv(
    database_url: &str,
    source_key: &str,
    records: &[EventCsvRecord],
) -> Result<ImportSummary> {
    import_event_records(database_url, "event_csv", source_key, records, true).await
}

pub async fn import_crawled_events(
    database_url: &str,
    source_key: &str,
    records: &[EventCsvRecord],
    deactivate_stale: bool,
) -> Result<ImportSummary> {
    import_event_records(database_url, "crawl", source_key, records, deactivate_stale).await
}

pub async fn load_existing_school_ids(
    database_url: &str,
    school_ids: &[String],
) -> Result<BTreeSet<String>> {
    if school_ids.is_empty() {
        return Ok(BTreeSet::new());
    }

    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    Ok(client
        .query("SELECT id FROM schools WHERE id = ANY($1)", &[&school_ids])
        .await?
        .into_iter()
        .map(|row| row.get::<_, String>("id"))
        .collect())
}

pub async fn load_active_event_ids_for_source(
    database_url: &str,
    source_type: &str,
    source_key: &str,
) -> Result<BTreeSet<String>> {
    let repo = PgRepository::new(database_url);
    let client = repo.connect().await?;
    Ok(client
        .query(
            "SELECT id
             FROM events
             WHERE source_type = $1
               AND source_key = $2
               AND is_active = TRUE",
            &[&source_type, &source_key],
        )
        .await?
        .into_iter()
        .map(|row| row.get::<_, String>("id"))
        .collect())
}

async fn import_event_records(
    database_url: &str,
    source_type: &str,
    source_key: &str,
    records: &[EventCsvRecord],
    deactivate_stale: bool,
) -> Result<ImportSummary> {
    let repo = PgRepository::new(database_url);
    let mut client = repo.connect().await?;
    let transaction = client.transaction().await?;

    let school_ids = records
        .iter()
        .map(|record| record.school_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let known_school_ids = if school_ids.is_empty() {
        BTreeSet::new()
    } else {
        transaction
            .query("SELECT id FROM schools WHERE id = ANY($1)", &[&school_ids])
            .await?
            .into_iter()
            .map(|row| row.get::<_, String>("id"))
            .collect::<BTreeSet<_>>()
    };

    let mut imported_ids = Vec::new();
    let mut skipped_missing_school = 0_i64;
    for record in records {
        if !known_school_ids.contains(record.school_id.as_str()) {
            skipped_missing_school += 1;
            continue;
        }
        let placement_tags = record.normalized_placement_tags()?;
        let placement_tags = placement_tags
            .into_iter()
            .map(|placement| placement.as_str().to_string())
            .collect::<Vec<_>>();
        let starts_at = parse_optional_event_time(record.starts_at.as_deref())?;

        transaction
            .execute(
                "INSERT INTO events (
                    id,
                    school_id,
                    title,
                    event_category,
                    is_open_day,
                    is_featured,
                    priority_weight,
                    starts_at,
                    placement_tags,
                    is_active,
                    source_type,
                    source_key,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, TRUE, $10, $11, NOW())
                ON CONFLICT (id) DO UPDATE
                SET school_id = EXCLUDED.school_id,
                    title = EXCLUDED.title,
                    event_category = EXCLUDED.event_category,
                    is_open_day = EXCLUDED.is_open_day,
                    is_featured = EXCLUDED.is_featured,
                    priority_weight = EXCLUDED.priority_weight,
                    starts_at = EXCLUDED.starts_at,
                    placement_tags = EXCLUDED.placement_tags,
                    is_active = TRUE,
                    source_type = EXCLUDED.source_type,
                    source_key = EXCLUDED.source_key,
                    updated_at = NOW()",
                &[
                    &record.event_id,
                    &record.school_id,
                    &record.title,
                    &record.event_category,
                    &record.is_open_day,
                    &record.is_featured,
                    &record.priority_weight,
                    &starts_at,
                    &placement_tags,
                    &source_type,
                    &source_key,
                ],
            )
            .await?;
        imported_ids.push(record.event_id.clone());
    }

    let skipped_missing_school_deactivation = deactivate_stale && skipped_missing_school > 0;
    let deactivated_rows = if !deactivate_stale || skipped_missing_school_deactivation {
        0
    } else if imported_ids.is_empty() {
        transaction
            .execute(
                "UPDATE events
                 SET is_active = FALSE,
                     updated_at = NOW()
                 WHERE source_type = $2
                   AND source_key = $1
                   AND is_active = TRUE",
                &[&source_key, &source_type],
            )
            .await? as i64
    } else {
        transaction
            .execute(
                "UPDATE events
                 SET is_active = FALSE,
                     updated_at = NOW()
                 WHERE source_type = $3
                   AND source_key = $1
                   AND is_active = TRUE
                   AND NOT (id = ANY($2))",
                &[&source_key, &imported_ids, &source_type],
            )
            .await? as i64
    };

    transaction.commit().await?;

    let mut report_entries = vec![ImportReportEntry {
        level: "info".to_string(),
        code: format!("{source_type}_imported"),
        message: format!("Imported {source_type} rows into the core events table."),
        row_count: Some(imported_ids.len() as i64),
        details: json!({
            "source_key": source_key,
            "source_type": source_type
        }),
    }];
    if skipped_missing_school > 0 {
        report_entries.push(ImportReportEntry {
            level: "warn".to_string(),
            code: format!("{source_type}_missing_school"),
            message: format!(
                "Skipped {source_type} rows because the referenced school_id was missing."
            ),
            row_count: Some(skipped_missing_school),
            details: json!({
                "source_key": source_key,
                "source_type": source_type
            }),
        });
    }
    if !deactivate_stale {
        report_entries.push(ImportReportEntry {
            level: "warn".to_string(),
            code: format!("{source_type}_skipped_stale_deactivation"),
            message: format!(
                "Skipped stale {source_type} deactivation because the import was partial."
            ),
            row_count: None,
            details: json!({
                "source_key": source_key,
                "source_type": source_type
            }),
        });
    }
    if skipped_missing_school_deactivation {
        report_entries.push(ImportReportEntry {
            level: "warn".to_string(),
            code: format!("{source_type}_skipped_missing_school_deactivation"),
            message: format!(
                "Skipped stale {source_type} deactivation because one or more rows referenced missing school_id values."
            ),
            row_count: Some(skipped_missing_school),
            details: json!({
                "source_key": source_key,
                "source_type": source_type,
                "skipped_missing_school": skipped_missing_school
            }),
        });
    }
    if deactivated_rows > 0 {
        report_entries.push(ImportReportEntry {
            level: "info".to_string(),
            code: format!("{source_type}_deactivated_stale_rows"),
            message: format!("Marked stale {source_type} rows inactive for the same source."),
            row_count: Some(deactivated_rows),
            details: json!({
                "source_key": source_key,
                "source_type": source_type
            }),
        });
    }

    Ok(ImportSummary {
        normalized_rows: records.len() as i64,
        core_rows: imported_ids.len() as i64,
        report_entries,
    })
}

pub async fn derive_school_station_links(database_url: &str) -> Result<DeriveLinksSummary> {
    let repo = PgRepository::new(database_url);
    let mut client = repo.connect().await?;
    let transaction = client.transaction().await?;

    transaction
        .execute(
            "DELETE FROM school_station_links
             WHERE school_id IN (SELECT school_id FROM jp_school_geodata)",
            &[],
        )
        .await?;

    let inserted = transaction
        .execute(
            "WITH ranked AS (
                SELECT
                    school.school_id,
                    station.station_id,
                    station.line_name,
                    ROUND(ST_Distance(school.geom, station.geom))::INTEGER AS distance_meters,
                    ROW_NUMBER() OVER (
                        PARTITION BY school.school_id
                        ORDER BY ST_Distance(school.geom, station.geom) ASC, station.station_id ASC
                    ) AS rank
                FROM jp_school_geodata AS school
                JOIN jp_rail_stations AS station
                  ON ST_DWithin(school.geom, station.geom, $1)
            )
            INSERT INTO school_station_links (
                school_id,
                station_id,
                walking_minutes,
                distance_meters,
                hop_distance,
                line_name
            )
            SELECT
                school_id,
                station_id,
                GREATEST(1, CEIL(distance_meters::NUMERIC / 80.0))::SMALLINT,
                distance_meters,
                0,
                line_name
            FROM ranked
            WHERE rank <= $2
            ON CONFLICT (school_id, station_id) DO UPDATE
            SET walking_minutes = EXCLUDED.walking_minutes,
                distance_meters = EXCLUDED.distance_meters,
                hop_distance = EXCLUDED.hop_distance,
                line_name = EXCLUDED.line_name",
            &[&2500_f64, &3_i64],
        )
        .await? as i64;

    let unlinked_row = transaction
        .query_one(
            "SELECT COUNT(*) AS count
             FROM jp_school_geodata AS school
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM school_station_links AS link
                 WHERE link.school_id = school.school_id
             )",
            &[],
        )
        .await?;
    let unlinked_count = unlinked_row.get::<_, i64>("count");

    transaction.commit().await?;

    let mut report_entries = vec![ImportReportEntry {
        level: "info".to_string(),
        code: "school_station_links_derived".to_string(),
        message: "Derived school to station links from normalized JP geodata.".to_string(),
        row_count: Some(inserted),
        details: json!({
            "distance_radius_meters": 2500,
            "max_links_per_school": 3
        }),
    }];
    if unlinked_count > 0 {
        report_entries.push(ImportReportEntry {
            level: "warn".to_string(),
            code: "schools_without_station_link".to_string(),
            message: "Some schools did not get a nearby station link within the derive radius."
                .to_string(),
            row_count: Some(unlinked_count),
            details: json!({ "distance_radius_meters": 2500 }),
        });
    }

    Ok(DeriveLinksSummary {
        link_rows: inserted,
        report_entries,
    })
}

pub async fn seed_fixture(database_url: &str, fixture_dir: impl AsRef<Path>) -> Result<()> {
    let repo = PgRepository::new(database_url);
    let mut client = repo.connect().await?;
    let transaction = client.transaction().await?;
    let fixture_dir = fixture_dir.as_ref();

    let stations: Vec<StationRow> = read_csv(fixture_dir.join("stations.csv"))?;
    let line_names = stations
        .iter()
        .map(|station| station.line_name.clone())
        .collect::<BTreeSet<_>>();
    for line_name in &line_names {
        let line_id = stable_id("line", line_name);
        transaction
            .execute(
                "INSERT INTO lines (line_id, line_name, country_code, source_id, source_version)
                 VALUES ($1, $2, 'JP', 'fixture', 'minimal')
                 ON CONFLICT (line_id) DO UPDATE
                 SET line_name = EXCLUDED.line_name,
                     source_id = EXCLUDED.source_id,
                     source_version = EXCLUDED.source_version",
                &[&line_id, &line_name],
            )
            .await?;
    }

    for station in stations {
        let line_id = stable_id("line", &station.line_name);
        transaction
            .execute(
                "INSERT INTO stations (id, name, line_name, latitude, longitude, line_id)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (id) DO UPDATE
                 SET name = EXCLUDED.name,
                     line_name = EXCLUDED.line_name,
                     latitude = EXCLUDED.latitude,
                     longitude = EXCLUDED.longitude,
                     line_id = EXCLUDED.line_id",
                &[
                    &station.station_id,
                    &station.name,
                    &station.line_name,
                    &station.latitude,
                    &station.longitude,
                    &line_id,
                ],
            )
            .await?;
    }

    let schools: Vec<SchoolRow> = read_csv(fixture_dir.join("schools.csv"))?;
    let area_names = schools
        .iter()
        .map(|school| (school.area.clone(), school.prefecture_name.clone()))
        .collect::<BTreeSet<_>>();
    let prefecture_names = schools
        .iter()
        .filter_map(|school| non_empty(school.prefecture_name.as_deref()).map(str::to_string))
        .collect::<BTreeSet<_>>();
    for prefecture_name in &prefecture_names {
        let area_id = stable_id("area", prefecture_name);
        transaction
            .execute(
                "INSERT INTO areas (area_id, country_code, prefecture_name, area_level)
                 VALUES ($1, 'JP', $2, 'prefecture')
                 ON CONFLICT (area_id) DO UPDATE
                 SET prefecture_name = EXCLUDED.prefecture_name,
                     area_level = EXCLUDED.area_level",
                &[&area_id, &prefecture_name],
            )
            .await?;
    }
    for (area_name, prefecture_name) in &area_names {
        let area_id = city_area_stable_id(area_name, prefecture_name.as_deref());
        transaction
            .execute(
                "INSERT INTO areas (area_id, country_code, prefecture_name, city_name, area_level)
                 VALUES ($1, 'JP', $2, $3, 'city')
                 ON CONFLICT (area_id) DO UPDATE
                 SET prefecture_name = EXCLUDED.prefecture_name,
                     city_name = EXCLUDED.city_name,
                     area_level = EXCLUDED.area_level",
                &[&area_id, &prefecture_name, &area_name],
            )
            .await?;
    }
    for school in schools {
        transaction
            .execute(
                "INSERT INTO schools (id, name, area, prefecture_name, school_type, group_id)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (id) DO UPDATE
                 SET name = EXCLUDED.name,
                     area = EXCLUDED.area,
                     prefecture_name = EXCLUDED.prefecture_name,
                     school_type = EXCLUDED.school_type,
                     group_id = EXCLUDED.group_id",
                &[
                    &school.school_id,
                    &school.name,
                    &school.area,
                    &school.prefecture_name,
                    &school.school_type,
                    &school.group_id,
                ],
            )
            .await?;
    }

    let events: Vec<EventRow> = read_csv(fixture_dir.join("events.csv"))?;
    for event in events {
        let starts_at = parse_optional_event_time(event.starts_at.as_deref())?;
        transaction
            .execute(
                "INSERT INTO events (
                    id,
                    school_id,
                    title,
                    event_category,
                    is_open_day,
                    is_featured,
                    priority_weight,
                    starts_at,
                    placement_tags,
                    is_active,
                    source_type,
                    updated_at
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, TRUE, 'seed', NOW())
                 ON CONFLICT (id) DO UPDATE
                 SET school_id = EXCLUDED.school_id,
                     title = EXCLUDED.title,
                     event_category = EXCLUDED.event_category,
                     is_open_day = EXCLUDED.is_open_day,
                     is_featured = EXCLUDED.is_featured,
                     priority_weight = EXCLUDED.priority_weight,
                     starts_at = EXCLUDED.starts_at,
                     placement_tags = EXCLUDED.placement_tags,
                     is_active = TRUE,
                     source_type = EXCLUDED.source_type,
                     updated_at = NOW()",
                &[
                    &event.event_id,
                    &event.school_id,
                    &event.title,
                    &event.event_category,
                    &event.is_open_day,
                    &event.is_featured,
                    &event.priority_weight,
                    &starts_at,
                    &event.normalized_placement_tags()?,
                ],
            )
            .await?;
    }

    let links: Vec<LinkRow> = read_csv(fixture_dir.join("school_station_links.csv"))?;
    for link in links {
        transaction
            .execute(
                "INSERT INTO school_station_links (school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (school_id, station_id) DO UPDATE
                 SET walking_minutes = EXCLUDED.walking_minutes,
                     distance_meters = EXCLUDED.distance_meters,
                     hop_distance = EXCLUDED.hop_distance,
                     line_name = EXCLUDED.line_name",
                &[
                    &link.school_id,
                    &link.station_id,
                    &(link.walking_minutes as i16),
                    &(link.distance_meters as i32),
                    &(link.hop_distance as i16),
                    &link.line_name,
                ],
            )
            .await?;
    }

    for user_event in read_ndjson(fixture_dir.join("user_events.ndjson"))? {
        let occurred_at = parse_rfc3339_utc("occurred_at", &user_event.occurred_at)?;
        transaction
            .execute(
                "DELETE FROM user_events
                 WHERE user_id = $1
                   AND school_id IS NOT DISTINCT FROM $2
                   AND event_type = $3
                   AND event_id IS NOT DISTINCT FROM $4
                   AND target_station_id IS NOT DISTINCT FROM $5
                   AND occurred_at = $6
                   AND payload = $7",
                &[
                    &user_event.user_id,
                    &user_event.school_id,
                    &user_event.event_kind.as_str(),
                    &user_event.event_id,
                    &user_event.target_station_id,
                    &occurred_at,
                    &user_event.payload,
                ],
            )
            .await?;
        transaction
            .execute(
                "INSERT INTO user_events (
                    user_id,
                    school_id,
                    event_type,
                    event_id,
                    target_station_id,
                    occurred_at,
                     payload
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    &user_event.user_id,
                    &user_event.school_id,
                    &user_event.event_kind.as_str(),
                    &user_event.event_id,
                    &user_event.target_station_id,
                    &occurred_at,
                    &user_event.payload,
                ],
            )
            .await?;
    }

    transaction.commit().await?;
    Ok(())
}

fn read_csv<T: for<'de> Deserialize<'de>>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let mut reader = Reader::from_path(path)
        .with_context(|| format!("failed to open fixture CSV {}", path.display()))?;
    let mut items = Vec::new();
    for row in reader.deserialize() {
        items.push(row.with_context(|| format!("failed to parse {}", path.display()))?);
    }
    Ok(items)
}

fn read_ndjson(path: impl AsRef<Path>) -> Result<Vec<UserEvent>> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read fixture NDJSON {}", path.display()))?;
    raw.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<UserEvent>(line).context("failed to parse NDJSON row"))
        .collect()
}

fn parse_placement_kind(raw: &str) -> Result<PlacementKind> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "home" => Ok(PlacementKind::Home),
        "search" => Ok(PlacementKind::Search),
        "detail" => Ok(PlacementKind::Detail),
        "mypage" => Ok(PlacementKind::Mypage),
        other => anyhow::bail!(
            "unsupported placement tag {other} in event CSV (expected home, search, detail, or mypage)"
        ),
    }
}

fn default_event_category() -> String {
    "general".to_string()
}

#[derive(Debug, Deserialize)]
struct SchoolRow {
    school_id: String,
    name: String,
    area: String,
    #[serde(default)]
    prefecture_name: Option<String>,
    school_type: String,
    group_id: String,
}

#[derive(Debug, Deserialize)]
struct EventRow {
    event_id: String,
    school_id: String,
    title: String,
    event_category: String,
    is_open_day: bool,
    is_featured: bool,
    priority_weight: f64,
    starts_at: Option<String>,
    #[serde(default)]
    placement_tags: String,
}

impl EventRow {
    fn normalized_placement_tags(&self) -> Result<Vec<String>> {
        self.placement_tags
            .split('|')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(parse_placement_kind)
            .map(|placement| placement.map(|value| value.as_str().to_string()))
            .collect()
    }
}

#[derive(Debug, Deserialize)]
struct StationRow {
    station_id: String,
    name: String,
    line_name: String,
    latitude: f64,
    longitude: f64,
}

#[derive(Debug, Deserialize)]
struct LinkRow {
    school_id: String,
    station_id: String,
    walking_minutes: u16,
    distance_meters: u32,
    hop_distance: u8,
    line_name: String,
}
