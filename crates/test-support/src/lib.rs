use std::{fs, fs::OpenOptions, path::Path, sync::OnceLock};

use anyhow::{Context, Result};
use csv::Reader;
use domain::{Event, PlacementKind, RankingDataset, School, SchoolStationLink, Station, UserEvent};
use fs2::FileExt;
use serde::Deserialize;

static POSTGRES_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

pub struct PostgresTestLockGuard {
    _in_process: tokio::sync::MutexGuard<'static, ()>,
    file: fs::File,
}

impl Drop for PostgresTestLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub async fn acquire_postgres_test_lock() -> PostgresTestLockGuard {
    let in_process = POSTGRES_TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let file = tokio::task::spawn_blocking(|| -> Result<fs::File> {
        let lock_path = std::env::temp_dir().join("geo-line-ranker-postgres-tests.lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("failed to open {}", lock_path.display()))?;
        file.lock_exclusive()
            .with_context(|| format!("failed to lock {}", lock_path.display()))?;
        Ok(file)
    })
    .await
    .expect("postgres test lock task should complete")
    .expect("postgres test lock should be acquired");
    PostgresTestLockGuard {
        _in_process: in_process,
        file,
    }
}

pub fn load_fixture_dataset(path: impl AsRef<Path>) -> Result<RankingDataset> {
    let path = path.as_ref();
    Ok(RankingDataset {
        schools: read_csv(path.join("schools.csv"))?
            .into_iter()
            .map(|row: SchoolRow| School {
                id: row.school_id,
                name: row.name,
                area: row.area,
                prefecture_name: row.prefecture_name,
                school_type: row.school_type,
                group_id: row.group_id,
            })
            .collect(),
        events: read_csv(path.join("events.csv"))?
            .into_iter()
            .map(|row: EventRow| -> Result<Event> {
                let placement_tags = row.normalized_placement_tags()?;
                Ok(Event {
                    id: row.event_id,
                    school_id: row.school_id,
                    title: row.title,
                    event_category: row.event_category,
                    is_open_day: row.is_open_day,
                    is_featured: row.is_featured,
                    priority_weight: row.priority_weight,
                    starts_at: row.starts_at,
                    placement_tags,
                    is_active: true,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        stations: read_csv(path.join("stations.csv"))?
            .into_iter()
            .map(|row: StationRow| Station {
                id: row.station_id,
                name: row.name,
                line_name: row.line_name,
                line_id: None,
                latitude: row.latitude,
                longitude: row.longitude,
            })
            .collect(),
        school_station_links: read_csv(path.join("school_station_links.csv"))?
            .into_iter()
            .map(|row: LinkRow| SchoolStationLink {
                school_id: row.school_id,
                station_id: row.station_id,
                walking_minutes: row.walking_minutes,
                distance_meters: row.distance_meters,
                hop_distance: row.hop_distance,
                line_name: row.line_name,
            })
            .collect(),
        popularity_snapshots: Vec::new(),
        user_affinity_snapshots: Vec::new(),
        area_affinity_snapshots: Vec::new(),
    })
}

pub fn load_user_event_count(path: impl AsRef<Path>) -> Result<usize> {
    let path = path.as_ref();
    Ok(load_user_events(path)?.len())
}

pub fn load_user_events(path: impl AsRef<Path>) -> Result<Vec<UserEvent>> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path.join("user_events.ndjson"))
        .with_context(|| "failed to read user_events.ndjson")?;
    raw.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<UserEvent>(line).context("failed to parse user event"))
        .collect()
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
    fn normalized_placement_tags(&self) -> Result<Vec<PlacementKind>> {
        self.placement_tags
            .split('|')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| match value {
                "home" => Ok(PlacementKind::Home),
                "search" => Ok(PlacementKind::Search),
                "detail" => Ok(PlacementKind::Detail),
                "mypage" => Ok(PlacementKind::Mypage),
                _ => Err(anyhow::anyhow!(
                    "unsupported placement tag `{value}` in fixture event {}",
                    self.event_id
                )),
            })
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

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Result;
    use tempfile::tempdir;

    use super::load_fixture_dataset;

    #[test]
    fn load_fixture_dataset_rejects_unknown_placement_tags() -> Result<()> {
        let temp = tempdir()?;
        fs::write(
            temp.path().join("schools.csv"),
            "school_id,name,area,school_type,group_id\nschool_a,School A,Area,high_school,group_a\n",
        )?;
        fs::write(
            temp.path().join("events.csv"),
            "event_id,school_id,title,event_category,is_open_day,is_featured,priority_weight,starts_at,placement_tags\nevent_a,school_a,Event A,open_campus,true,false,0.5,2026-05-10T10:00:00+09:00,typo_tag\n",
        )?;
        fs::write(
            temp.path().join("stations.csv"),
            "station_id,name,line_name,latitude,longitude\n",
        )?;
        fs::write(
            temp.path().join("school_station_links.csv"),
            "school_id,station_id,walking_minutes,distance_meters,hop_distance,line_name\n",
        )?;

        let error =
            load_fixture_dataset(temp.path()).expect_err("unknown placement tag should fail");
        assert!(error
            .to_string()
            .contains("unsupported placement tag `typo_tag` in fixture event event_a"));

        Ok(())
    }
}
