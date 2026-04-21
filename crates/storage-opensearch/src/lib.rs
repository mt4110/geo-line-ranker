use std::{collections::HashSet, time::Duration};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use config::OpenSearchSettings;
use domain::{School, SchoolStationLink, Station};
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use storage::{CandidateProjectionSync, ProjectionSyncStats};
use storage_postgres::{load_candidate_projection_rows, CandidateProjectionRow};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProjectionGeoPoint {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProjectionDocument {
    pub document_id: String,
    pub school_id: String,
    pub school_name: String,
    pub school_area: String,
    pub school_type: String,
    pub station_id: String,
    pub station_name: String,
    pub line_name: String,
    pub station_location: ProjectionGeoPoint,
    pub walking_minutes: u16,
    pub distance_meters: u32,
    pub hop_distance: u8,
    pub open_day_count: i64,
    pub popularity_score: f64,
}

impl ProjectionDocument {
    pub fn from_parts(
        school: &School,
        station: &Station,
        link: &SchoolStationLink,
        open_day_count: i64,
        popularity_score: f64,
    ) -> Self {
        Self {
            document_id: format!("{}:{}", link.school_id, link.station_id),
            school_id: school.id.clone(),
            school_name: school.name.clone(),
            school_area: school.area.clone(),
            school_type: school.school_type.clone(),
            station_id: station.id.clone(),
            station_name: station.name.clone(),
            line_name: link.line_name.clone(),
            station_location: ProjectionGeoPoint {
                lat: station.latitude,
                lon: station.longitude,
            },
            walking_minutes: link.walking_minutes,
            distance_meters: link.distance_meters,
            hop_distance: link.hop_distance,
            open_day_count,
            popularity_score,
        }
    }

    pub fn from_projection_row(row: &CandidateProjectionRow) -> Self {
        Self {
            document_id: format!("{}:{}", row.school_id, row.station_id),
            school_id: row.school_id.clone(),
            school_name: row.school_name.clone(),
            school_area: row.school_area.clone(),
            school_type: row.school_type.clone(),
            station_id: row.station_id.clone(),
            station_name: row.station_name.clone(),
            line_name: row.station_line_name.clone(),
            station_location: ProjectionGeoPoint {
                lat: row.station_latitude,
                lon: row.station_longitude,
            },
            walking_minutes: row.walking_minutes,
            distance_meters: row.distance_meters,
            hop_distance: row.hop_distance,
            open_day_count: row.open_day_count,
            popularity_score: row.popularity_score,
        }
    }

    pub fn index_mapping() -> Value {
        json!({
            "settings": {
                "index": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            },
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "document_id": { "type": "keyword" },
                    "school_id": { "type": "keyword" },
                    "school_name": {
                        "type": "text",
                        "fields": {
                            "keyword": { "type": "keyword" }
                        }
                    },
                    "school_area": { "type": "keyword" },
                    "school_type": { "type": "keyword" },
                    "station_id": { "type": "keyword" },
                    "station_name": {
                        "type": "text",
                        "fields": {
                            "keyword": { "type": "keyword" }
                        }
                    },
                    "line_name": { "type": "keyword" },
                    "station_location": { "type": "geo_point" },
                    "walking_minutes": { "type": "short" },
                    "distance_meters": { "type": "integer" },
                    "hop_distance": { "type": "short" },
                    "open_day_count": { "type": "integer" },
                    "popularity_score": { "type": "double" }
                }
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct OpenSearchStore {
    client: Client,
    base_url: String,
    index_name: String,
    username: Option<String>,
    password: Option<String>,
}

impl OpenSearchStore {
    pub fn new(settings: &OpenSearchSettings) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(settings.request_timeout_secs.max(1)))
            .build()
            .context("failed to build OpenSearch client")?;
        Ok(Self {
            client,
            base_url: settings.url.trim_end_matches('/').to_string(),
            index_name: settings.index_name.clone(),
            username: settings.username.clone(),
            password: settings.password.clone(),
        })
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub async fn ready_check(&self) -> Result<()> {
        if self.index_exists().await? {
            return Ok(());
        }

        bail!("OpenSearch candidate index {} is missing", self.index_name);
    }

    pub async fn ensure_index(&self) -> Result<()> {
        if self.index_exists().await? {
            return Ok(());
        }
        self.create_index().await
    }

    pub async fn rebuild_projection(
        &self,
        documents: &[ProjectionDocument],
    ) -> Result<ProjectionSyncStats> {
        self.delete_index_if_exists().await?;
        self.create_index().await?;
        let indexed_documents = self.bulk_index(documents).await?;
        Ok(ProjectionSyncStats {
            indexed_documents,
            deleted_documents: 0,
        })
    }

    pub async fn sync_projection(
        &self,
        documents: &[ProjectionDocument],
    ) -> Result<ProjectionSyncStats> {
        let existed = self.index_exists().await?;
        if !existed {
            self.create_index().await?;
        }

        let indexed_documents = self.bulk_index(documents).await?;
        let deleted_documents = if existed {
            let existing_ids = self.fetch_document_ids().await?;
            let desired_ids: HashSet<String> = documents
                .iter()
                .map(|document| document.document_id.clone())
                .collect();
            let stale_ids: Vec<String> = existing_ids
                .into_iter()
                .filter(|document_id| !desired_ids.contains(document_id))
                .collect();
            self.bulk_delete(&stale_ids).await?
        } else {
            0
        };

        Ok(ProjectionSyncStats {
            indexed_documents,
            deleted_documents,
        })
    }

    pub async fn search_candidate_links(
        &self,
        target_station: &Station,
        neighbor_distance_cap_meters: f64,
        candidate_limit: usize,
        neighbor_max_hops: u8,
    ) -> Result<Vec<SchoolStationLink>> {
        self.ensure_index().await?;

        let query = json!({
            "size": candidate_limit.clamp(1, 10_000),
            "sort": [
                {
                    "_script": {
                        "type": "number",
                        "script": {
                            "lang": "painless",
                            "source": "doc['station_id'].size() != 0 && doc['station_id'].value == params.target_station_id ? 0 : 1",
                            "params": {
                                "target_station_id": target_station.id.as_str()
                            }
                        },
                        "order": "asc",
                    }
                },
                { "distance_meters": { "order": "asc" } },
                { "walking_minutes": { "order": "asc" } },
                { "school_id": { "order": "asc" } },
                { "station_id": { "order": "asc" } }
            ],
            "query": {
                "bool": {
                    "should": [
                        {
                            "term": {
                                "station_id": {
                                    "value": target_station.id.as_str()
                                }
                            }
                        },
                        {
                            "bool": {
                                "filter": [
                                    {
                                        "term": {
                                            "line_name": {
                                                "value": target_station.line_name.as_str()
                                            }
                                        }
                                    },
                                    {
                                        "range": {
                                            "hop_distance": {
                                                "lte": neighbor_max_hops
                                            }
                                        }
                                    },
                                    {
                                        "geo_distance": {
                                            "distance": format!("{}m", neighbor_distance_cap_meters.ceil() as i64),
                                            "station_location": {
                                                "lat": target_station.latitude,
                                                "lon": target_station.longitude
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            }
        });

        let response = self
            .send_json(
                Method::POST,
                &format!("{}/_search", self.index_name),
                Some(&query),
            )
            .await?;
        let body: Value = response
            .json()
            .await
            .context("failed to decode OpenSearch search response")?;
        let hits = body
            .get("hits")
            .and_then(|hits| hits.get("hits"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        let mut seen = HashSet::new();
        let mut links = Vec::new();
        for hit in hits {
            let Some(source) = hit.get("_source") else {
                continue;
            };
            let Some(school_id) = source.get("school_id").and_then(Value::as_str) else {
                continue;
            };
            let Some(station_id) = source.get("station_id").and_then(Value::as_str) else {
                continue;
            };
            if !seen.insert((school_id.to_string(), station_id.to_string())) {
                continue;
            }
            let Some(walking_minutes) = source.get("walking_minutes").and_then(Value::as_u64)
            else {
                continue;
            };
            let Some(distance_meters) = source.get("distance_meters").and_then(Value::as_u64)
            else {
                continue;
            };
            let Some(hop_distance) = source.get("hop_distance").and_then(Value::as_u64) else {
                continue;
            };
            let Some(line_name) = source.get("line_name").and_then(Value::as_str) else {
                continue;
            };

            links.push(SchoolStationLink {
                school_id: school_id.to_string(),
                station_id: station_id.to_string(),
                walking_minutes: walking_minutes as u16,
                distance_meters: distance_meters as u32,
                hop_distance: hop_distance as u8,
                line_name: line_name.to_string(),
            });
        }

        links.sort_by(|left, right| {
            let left_direct = left.station_id != target_station.id;
            let right_direct = right.station_id != target_station.id;
            left_direct
                .cmp(&right_direct)
                .then_with(|| left.distance_meters.cmp(&right.distance_meters))
                .then_with(|| left.walking_minutes.cmp(&right.walking_minutes))
                .then_with(|| left.school_id.cmp(&right.school_id))
                .then_with(|| left.station_id.cmp(&right.station_id))
        });
        links.truncate(candidate_limit.clamp(1, 10_000));

        Ok(links)
    }

    async fn index_exists(&self) -> Result<bool> {
        let response = self
            .send_json(Method::HEAD, &self.index_name, None)
            .await
            .context("failed to check OpenSearch index")?;
        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            status => {
                let body = response.text().await.unwrap_or_default();
                bail!(
                    "unexpected OpenSearch status while checking index {}: {} {}",
                    self.index_name,
                    status,
                    body
                );
            }
        }
    }

    async fn create_index(&self) -> Result<()> {
        let response = self
            .send_json(
                Method::PUT,
                &self.index_name,
                Some(&ProjectionDocument::index_mapping()),
            )
            .await?;
        Self::ensure_success(response, "create OpenSearch index").await?;
        Ok(())
    }

    async fn delete_index_if_exists(&self) -> Result<()> {
        let response = self
            .send_json(Method::DELETE, &self.index_name, None)
            .await
            .context("failed to delete OpenSearch index")?;
        match response.status() {
            StatusCode::OK | StatusCode::NOT_FOUND => Ok(()),
            status => {
                let body = response.text().await.unwrap_or_default();
                bail!(
                    "delete OpenSearch index failed with status {}: {}",
                    status,
                    body
                );
            }
        }
    }

    async fn fetch_document_ids(&self) -> Result<Vec<String>> {
        const SCROLL_TIMEOUT: &str = "1m";
        const BATCH_SIZE: usize = 1_000;

        let mut document_ids = Vec::new();
        let mut scroll_id = None;
        let mut body = Self::ensure_success(
            self.send_json(
                Method::POST,
                &format!("{}/_search?scroll={SCROLL_TIMEOUT}", self.index_name),
                Some(&json!({
                    "size": BATCH_SIZE,
                    "_source": false,
                    "sort": ["_doc"],
                    "query": { "match_all": {} }
                })),
            )
            .await?,
            "start OpenSearch id scan",
        )
        .await?
        .json::<Value>()
        .await
        .context("failed to decode OpenSearch id scan response")?;

        loop {
            if let Some(next_scroll_id) = body
                .get("_scroll_id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
            {
                scroll_id = Some(next_scroll_id);
            }

            let hits = body
                .get("hits")
                .and_then(|hits| hits.get("hits"))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if hits.is_empty() {
                break;
            }

            document_ids.extend(hits.into_iter().filter_map(|hit| {
                hit.get("_id")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            }));

            let active_scroll_id = scroll_id
                .as_deref()
                .context("OpenSearch id scan did not return a scroll ID")?;
            body = Self::ensure_success(
                self.send_json(
                    Method::POST,
                    "_search/scroll",
                    Some(&json!({
                        "scroll": SCROLL_TIMEOUT,
                        "scroll_id": active_scroll_id
                    })),
                )
                .await?,
                "continue OpenSearch id scan",
            )
            .await?
            .json::<Value>()
            .await
            .context("failed to decode OpenSearch scroll response")?;
        }

        if let Some(scroll_id) = scroll_id.as_deref() {
            self.clear_scroll(scroll_id).await?;
        }

        Ok(document_ids)
    }

    async fn clear_scroll(&self, scroll_id: &str) -> Result<()> {
        let response = self
            .send_json(
                Method::DELETE,
                "_search/scroll",
                Some(&json!({
                    "scroll_id": [scroll_id]
                })),
            )
            .await?;
        Self::ensure_success(response, "clear OpenSearch scroll").await?;
        Ok(())
    }

    async fn bulk_index(&self, documents: &[ProjectionDocument]) -> Result<i64> {
        if documents.is_empty() {
            return Ok(0);
        }

        let mut payload = String::new();
        for document in documents {
            payload.push_str(
                &serde_json::to_string(&json!({
                    "index": {
                        "_index": self.index_name.as_str(),
                        "_id": document.document_id.as_str(),
                    }
                }))
                .context("failed to encode bulk index header")?,
            );
            payload.push('\n');
            payload.push_str(
                &serde_json::to_string(document).context("failed to encode projection document")?,
            );
            payload.push('\n');
        }

        let response = self
            .send_raw(Method::POST, "_bulk", payload, Some("application/x-ndjson"))
            .await?;
        let body: Value = Self::ensure_success(response, "bulk index OpenSearch documents")
            .await?
            .json()
            .await
            .context("failed to decode bulk index response")?;
        if body.get("errors").and_then(Value::as_bool) == Some(true) {
            bail!("OpenSearch bulk index reported item-level failures");
        }

        Ok(documents.len() as i64)
    }

    async fn bulk_delete(&self, document_ids: &[String]) -> Result<i64> {
        if document_ids.is_empty() {
            return Ok(0);
        }

        let mut payload = String::new();
        for document_id in document_ids {
            payload.push_str(
                &serde_json::to_string(&json!({
                    "delete": {
                        "_index": self.index_name.as_str(),
                        "_id": document_id,
                    }
                }))
                .context("failed to encode bulk delete header")?,
            );
            payload.push('\n');
        }

        let response = self
            .send_raw(Method::POST, "_bulk", payload, Some("application/x-ndjson"))
            .await?;
        let body: Value = Self::ensure_success(response, "bulk delete OpenSearch documents")
            .await?
            .json()
            .await
            .context("failed to decode bulk delete response")?;
        if body.get("errors").and_then(Value::as_bool) == Some(true) {
            bail!("OpenSearch bulk delete reported item-level failures");
        }

        Ok(document_ids.len() as i64)
    }

    async fn send_json(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<reqwest::Response> {
        let mut request = self.request(method, path);
        if let Some(body) = body {
            request = request.json(body);
        }
        request
            .send()
            .await
            .context("failed to send OpenSearch request")
    }

    async fn send_raw(
        &self,
        method: Method,
        path: &str,
        body: String,
        content_type: Option<&str>,
    ) -> Result<reqwest::Response> {
        let mut request = self.request(method, path);
        if let Some(content_type) = content_type {
            request = request.header("content-type", content_type);
        }
        request
            .body(body)
            .send()
            .await
            .context("failed to send OpenSearch bulk request")
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}/{}", self.base_url, path.trim_start_matches('/'));
        let request = self.client.request(method, url);
        match self.username.as_deref() {
            Some(username) => request.basic_auth(username, self.password.as_deref()),
            None => request,
        }
    }

    async fn ensure_success(
        response: reqwest::Response,
        action: &str,
    ) -> Result<reqwest::Response> {
        if response.status().is_success() {
            return Ok(response);
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("{action} failed with status {status}: {body}");
    }
}

#[derive(Debug, Clone)]
pub struct ProjectionSyncService {
    database_url: String,
    store: OpenSearchStore,
}

impl ProjectionSyncService {
    pub fn new(database_url: impl Into<String>, settings: &OpenSearchSettings) -> Result<Self> {
        Ok(Self {
            database_url: database_url.into(),
            store: OpenSearchStore::new(settings)?,
        })
    }

    pub fn store(&self) -> &OpenSearchStore {
        &self.store
    }

    pub async fn rebuild_index(&self) -> Result<ProjectionSyncStats> {
        let documents = self.load_documents().await?;
        self.store.rebuild_projection(&documents).await
    }

    pub async fn sync_projection_once(&self) -> Result<ProjectionSyncStats> {
        let documents = self.load_documents().await?;
        self.store.sync_projection(&documents).await
    }

    async fn load_documents(&self) -> Result<Vec<ProjectionDocument>> {
        let rows = load_candidate_projection_rows(&self.database_url).await?;
        Ok(rows
            .iter()
            .map(ProjectionDocument::from_projection_row)
            .collect())
    }
}

#[async_trait]
impl CandidateProjectionSync for ProjectionSyncService {
    async fn sync_projection(&self) -> Result<ProjectionSyncStats> {
        self.sync_projection_once().await
    }
}
