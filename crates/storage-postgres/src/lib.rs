use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
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
use storage::{
    ClaimedJob, JobType, NewJob, RecommendationRepository, RecommendationTrace,
    SnapshotRefreshStats, SnapshotTuning,
};
use tokio_postgres::{Client, GenericClient, NoTls};

const REQUIRED_READY_TABLES: [&str; 10] = [
    "schools",
    "events",
    "stations",
    "school_station_links",
    "popularity_snapshots",
    "user_affinity_snapshots",
    "area_affinity_snapshots",
    "user_events",
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
}

impl PgRepository {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
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
                "SELECT id, name, line_name, latitude, longitude
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
                    latitude: row.get("latitude"),
                    longitude: row.get("longitude"),
                })
            })
            .context("failed to load target station")
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
                "SELECT id, name, area, school_type, group_id
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
                    starts_at,
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
                "SELECT id, name, line_name, latitude, longitude
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
                &event.occurred_at,
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
                "SELECT id, name, area, school_type, group_id FROM schools ORDER BY id",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| School {
                id: row.get("id"),
                name: row.get("name"),
                area: row.get("area"),
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
                    starts_at,
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
                "SELECT id, name, line_name, latitude, longitude FROM stations ORDER BY id",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| Station {
                id: row.get("id"),
                name: row.get("name"),
                line_name: row.get("line_name"),
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
        let client = self.connect().await?;
        insert_job(&client, job).await
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
                "INSERT INTO schools (id, name, area, school_type, group_id)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (id) DO UPDATE
                 SET name = EXCLUDED.name,
                     area = EXCLUDED.area,
                     school_type = EXCLUDED.school_type,
                     group_id = COALESCE(NULLIF(schools.group_id, ''), EXCLUDED.group_id)",
                &[
                    &record.school_id,
                    &record.name,
                    &format!("{} {}", record.prefecture_name, record.city_name),
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
                "INSERT INTO schools (id, name, area, school_type, group_id)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (id) DO UPDATE
                 SET name = EXCLUDED.name,
                     area = EXCLUDED.area,
                     school_type = EXCLUDED.school_type,
                     group_id = COALESCE(NULLIF(schools.group_id, ''), EXCLUDED.group_id)",
                &[
                    &record.school_id,
                    &record.name,
                    &format!("{} {}", record.prefecture_name, record.city_name),
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
                    &record.starts_at,
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
    for station in stations {
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
                    &station.station_id,
                    &station.name,
                    &station.line_name,
                    &station.latitude,
                    &station.longitude,
                ],
            )
            .await?;
    }

    let schools: Vec<SchoolRow> = read_csv(fixture_dir.join("schools.csv"))?;
    for school in schools {
        transaction
            .execute(
                "INSERT INTO schools (id, name, area, school_type, group_id)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (id) DO UPDATE
                 SET name = EXCLUDED.name,
                     area = EXCLUDED.area,
                     school_type = EXCLUDED.school_type,
                     group_id = EXCLUDED.group_id",
                &[
                    &school.school_id,
                    &school.name,
                    &school.area,
                    &school.school_type,
                    &school.group_id,
                ],
            )
            .await?;
    }

    let events: Vec<EventRow> = read_csv(fixture_dir.join("events.csv"))?;
    for event in events {
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
                    &event.starts_at,
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
                    &user_event.occurred_at,
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
                    &user_event.occurred_at,
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
