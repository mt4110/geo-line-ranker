use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::Path,
    sync::Once,
};

use anyhow::{ensure, Context, Result};
use domain::{ContentKind, PlacementKind};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CandidateRetrievalMode {
    SqlOnly,
    Full,
}

impl CandidateRetrievalMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SqlOnly => "sql_only",
            Self::Full => "full",
        }
    }

    pub fn is_full(self) -> bool {
        matches!(self, Self::Full)
    }
}

impl std::str::FromStr for CandidateRetrievalMode {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "sql_only" => Ok(Self::SqlOnly),
            "full" => Ok(Self::Full),
            other => anyhow::bail!(
                "unsupported CANDIDATE_RETRIEVAL_MODE: {other} (expected sql_only or full)"
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenSearchSettings {
    pub url: String,
    pub index_name: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub request_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSettings {
    pub bind_addr: String,
    pub database_url: String,
    pub postgres_pool_max_size: usize,
    pub redis_url: Option<String>,
    pub ranking_config_dir: String,
    pub fixture_dir: String,
    pub raw_storage_dir: String,
    pub algorithm_version: String,
    pub candidate_retrieval_mode: CandidateRetrievalMode,
    pub candidate_retrieval_limit: usize,
    pub opensearch: OpenSearchSettings,
    pub recommendation_cache_ttl_secs: u64,
    pub worker_poll_interval_ms: u64,
    pub worker_retry_delay_secs: u64,
    pub worker_max_attempts: i32,
}

impl AppSettings {
    pub fn from_env() -> Result<Self> {
        load_dotenv();
        let candidate_retrieval_mode =
            parse_candidate_retrieval_mode(match env::var("CANDIDATE_RETRIEVAL_MODE") {
                Ok(raw) => Some(raw),
                Err(env::VarError::NotPresent) => None,
                Err(env::VarError::NotUnicode(_)) => {
                    anyhow::bail!("CANDIDATE_RETRIEVAL_MODE must be valid unicode")
                }
            })?;

        Ok(Self {
            bind_addr: env::var("APP_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string()),
            database_url: env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgres://postgres:postgres@127.0.0.1:5433/geo_line_ranker".to_string()
            }),
            postgres_pool_max_size: parse_env("POSTGRES_POOL_MAX_SIZE", 16)?,
            redis_url: env::var("REDIS_URL").ok().filter(|value| !value.is_empty()),
            ranking_config_dir: env::var("RANKING_CONFIG_DIR")
                .unwrap_or_else(|_| "configs/ranking".to_string()),
            fixture_dir: env::var("FIXTURE_DIR")
                .unwrap_or_else(|_| "storage/fixtures/minimal".to_string()),
            raw_storage_dir: env::var("RAW_STORAGE_DIR")
                .unwrap_or_else(|_| ".storage/raw".to_string()),
            algorithm_version: env::var("ALGORITHM_VERSION")
                .unwrap_or_else(|_| "phase8-policy-diversity-v1".to_string()),
            candidate_retrieval_mode,
            candidate_retrieval_limit: parse_env("CANDIDATE_RETRIEVAL_LIMIT", 256)?,
            opensearch: OpenSearchSettings {
                url: env::var("OPENSEARCH_URL")
                    .unwrap_or_else(|_| "http://127.0.0.1:9200".to_string()),
                index_name: env::var("OPENSEARCH_INDEX_NAME")
                    .unwrap_or_else(|_| "geo_line_ranker_candidates".to_string()),
                username: env::var("OPENSEARCH_USERNAME")
                    .ok()
                    .filter(|value| !value.is_empty()),
                password: env::var("OPENSEARCH_PASSWORD")
                    .ok()
                    .filter(|value| !value.is_empty()),
                request_timeout_secs: parse_env("OPENSEARCH_REQUEST_TIMEOUT_SECS", 5)?,
            },
            recommendation_cache_ttl_secs: parse_env("RECOMMENDATION_CACHE_TTL_SECS", 120)?,
            worker_poll_interval_ms: parse_env("WORKER_POLL_INTERVAL_MS", 1000)?,
            worker_retry_delay_secs: parse_env("WORKER_RETRY_DELAY_SECS", 5)?,
            worker_max_attempts: parse_env("WORKER_MAX_ATTEMPTS", 3)?,
        })
    }
}

fn load_dotenv() {
    static DOTENV: Once = Once::new();
    DOTENV.call_once(|| {
        let _ = dotenvy::dotenv();
    });
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchoolsProfile {
    pub limit_default: usize,
    pub strict_min_candidates: usize,
    pub direct_station_bonus: f64,
    pub line_match_bonus: f64,
    pub distance_scale_meters: f64,
    pub walking_scale_minutes: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsProfile {
    pub open_day_bonus: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MixedRankingProfile {
    pub enabled_content_kinds: Vec<ContentKind>,
    #[serde(default)]
    pub score_boosts: BTreeMap<ContentKind, f64>,
    pub featured_event_bonus: f64,
    pub event_priority_weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiversityProfile {
    pub same_school_cap: usize,
    pub same_group_cap: usize,
    #[serde(default)]
    pub content_kind_max_ratio: BTreeMap<ContentKind, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementProfile {
    pub neighbor_max_hops: u8,
    pub neighbor_same_line_bonus: f64,
    pub mixed_ranking: MixedRankingProfile,
    pub diversity: DiversityProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FallbackProfile {
    pub min_results: usize,
    pub neighbor_penalty: f64,
    pub neighbor_distance_cap_meters: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrackingProfile {
    pub popularity_bonus_weight: f64,
    pub user_affinity_bonus_weight: f64,
    pub area_affinity_bonus_weight: f64,
    #[serde(default = "default_search_execute_school_signal_weight")]
    pub search_execute_school_signal_weight: f64,
    #[serde(default = "default_search_execute_area_signal_weight")]
    pub search_execute_area_signal_weight: f64,
}

fn default_search_execute_school_signal_weight() -> f64 {
    0.0
}

fn default_search_execute_area_signal_weight() -> f64 {
    0.0
}

#[derive(Debug, Clone)]
pub struct RankingProfiles {
    pub schools: SchoolsProfile,
    pub events: EventsProfile,
    pub placements: BTreeMap<PlacementKind, PlacementProfile>,
    pub fallback: FallbackProfile,
    pub tracking: TrackingProfile,
    pub profile_version: String,
}

impl RankingProfiles {
    pub fn load_from_dir(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let schools_path = path.join("schools.default.yaml");
        let events_path = path.join("events.default.yaml");
        let fallback_path = path.join("fallback.default.yaml");
        let tracking_path = path.join("tracking.default.yaml");

        let schools_raw = read_raw(&schools_path)?;
        let events_raw = read_raw(&events_path)?;
        let fallback_raw = read_raw(&fallback_path)?;
        let tracking_raw = read_raw(&tracking_path)?;

        let mut digest = Sha256::new();
        let mut placement_raws = Vec::new();
        let mut placements = BTreeMap::new();
        for placement in [
            PlacementKind::Home,
            PlacementKind::Search,
            PlacementKind::Detail,
            PlacementKind::Mypage,
        ] {
            let placement_path = path.join(format!("placement.{}.yaml", placement.as_str()));
            let placement_raw = read_raw(&placement_path)?;
            digest.update(placement_raw.as_bytes());
            placement_raws.push((placement, placement_path, placement_raw));
        }

        for raw in [&schools_raw, &events_raw, &fallback_raw, &tracking_raw] {
            digest.update(raw.as_bytes());
        }

        for (placement, placement_path, placement_raw) in placement_raws {
            let profile: PlacementProfile =
                serde_yaml::from_str(&placement_raw).with_context(|| {
                    format!("failed to parse config file {}", placement_path.display())
                })?;
            placements.insert(placement, profile);
        }

        let profiles = Self {
            schools: serde_yaml::from_str(&schools_raw).with_context(|| {
                format!("failed to parse config file {}", schools_path.display())
            })?,
            events: serde_yaml::from_str(&events_raw).with_context(|| {
                format!("failed to parse config file {}", events_path.display())
            })?,
            placements,
            fallback: serde_yaml::from_str(&fallback_raw).with_context(|| {
                format!("failed to parse config file {}", fallback_path.display())
            })?,
            tracking: serde_yaml::from_str(&tracking_raw).with_context(|| {
                format!("failed to parse config file {}", tracking_path.display())
            })?,
            profile_version: format!("{:x}", digest.finalize()),
        };
        profiles.validate()?;
        Ok(profiles)
    }

    pub fn placement(&self, placement: PlacementKind) -> &PlacementProfile {
        self.placements
            .get(&placement)
            .unwrap_or_else(|| panic!("missing placement profile {}", placement.as_str()))
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.schools.limit_default > 0,
            "schools.limit_default must be positive"
        );
        ensure!(
            self.schools.strict_min_candidates > 0,
            "schools.strict_min_candidates must be positive"
        );
        ensure!(
            self.schools.distance_scale_meters > 0.0,
            "schools.distance_scale_meters must be greater than zero"
        );
        ensure!(
            self.schools.walking_scale_minutes > 0.0,
            "schools.walking_scale_minutes must be greater than zero"
        );
        ensure!(
            self.events.open_day_bonus >= 0.0,
            "events.open_day_bonus must be zero or positive"
        );
        ensure!(
            self.fallback.min_results > 0,
            "fallback.min_results must be positive"
        );
        ensure!(
            self.fallback.neighbor_penalty >= 0.0,
            "fallback.neighbor_penalty must be zero or positive"
        );
        ensure!(
            self.fallback.neighbor_distance_cap_meters > 0.0,
            "fallback.neighbor_distance_cap_meters must be greater than zero"
        );
        ensure!(
            self.tracking.popularity_bonus_weight >= 0.0
                && self.tracking.user_affinity_bonus_weight >= 0.0
                && self.tracking.area_affinity_bonus_weight >= 0.0
                && self.tracking.search_execute_school_signal_weight >= 0.0
                && self.tracking.search_execute_area_signal_weight >= 0.0,
            "tracking weights must be zero or positive"
        );

        for placement in [
            PlacementKind::Home,
            PlacementKind::Search,
            PlacementKind::Detail,
            PlacementKind::Mypage,
        ] {
            let profile = self
                .placements
                .get(&placement)
                .with_context(|| format!("missing placement profile {}", placement.as_str()))?;
            ensure!(
                profile.diversity.same_school_cap > 0,
                "placement.{}.diversity.same_school_cap must be positive",
                placement.as_str()
            );
            ensure!(
                profile.diversity.same_group_cap > 0,
                "placement.{}.diversity.same_group_cap must be positive",
                placement.as_str()
            );
            ensure!(
                !profile.mixed_ranking.enabled_content_kinds.is_empty(),
                "placement.{}.mixed_ranking.enabled_content_kinds must not be empty",
                placement.as_str()
            );
            ensure!(
                profile.mixed_ranking.featured_event_bonus >= 0.0,
                "placement.{}.mixed_ranking.featured_event_bonus must be zero or positive",
                placement.as_str()
            );
            ensure!(
                profile.mixed_ranking.event_priority_weight >= 0.0,
                "placement.{}.mixed_ranking.event_priority_weight must be zero or positive",
                placement.as_str()
            );

            let mut seen_kinds = BTreeSet::new();
            for kind in &profile.mixed_ranking.enabled_content_kinds {
                ensure!(
                    seen_kinds.insert(*kind),
                    "placement.{}.mixed_ranking.enabled_content_kinds contains duplicate {}",
                    placement.as_str(),
                    kind.as_str()
                );
                ensure!(
                    !matches!(kind, ContentKind::Article),
                    "placement.{}.mixed_ranking.enabled_content_kinds.article is reserved until article candidates are implemented",
                    placement.as_str()
                );
                if let Some(max_ratio) = profile.diversity.content_kind_max_ratio.get(kind) {
                    ensure!(
                        (0.0..=1.0).contains(max_ratio) && *max_ratio > 0.0,
                        "placement.{}.diversity.content_kind_max_ratio.{} must be within (0, 1]",
                        placement.as_str(),
                        kind.as_str()
                    );
                }
            }

            for (kind, max_ratio) in &profile.diversity.content_kind_max_ratio {
                ensure!(
                    profile.mixed_ranking.enabled_content_kinds.contains(kind),
                    "placement.{}.diversity.content_kind_max_ratio.{} requires the content kind to be enabled",
                    placement.as_str(),
                    kind.as_str()
                );
                ensure!(
                    (0.0..=1.0).contains(max_ratio) && *max_ratio > 0.0,
                    "placement.{}.diversity.content_kind_max_ratio.{} must be within (0, 1]",
                    placement.as_str(),
                    kind.as_str()
                );
            }

            let max_ratio_sum = profile
                .mixed_ranking
                .enabled_content_kinds
                .iter()
                .map(|kind| {
                    profile
                        .diversity
                        .content_kind_max_ratio
                        .get(kind)
                        .copied()
                        .unwrap_or(1.0)
                })
                .sum::<f64>();
            ensure!(
                max_ratio_sum >= 1.0,
                "placement.{}.diversity.content_kind_max_ratio must allow filling the requested limit",
                placement.as_str()
            );
        }

        Ok(())
    }
}

fn read_raw(path: impl AsRef<Path>) -> Result<String> {
    let path = path.as_ref();
    fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))
}

fn parse_candidate_retrieval_mode(raw: Option<String>) -> Result<CandidateRetrievalMode> {
    match raw {
        Some(raw) => raw
            .parse()
            .with_context(|| format!("failed to parse CANDIDATE_RETRIEVAL_MODE={raw}")),
        None => Ok(CandidateRetrievalMode::SqlOnly),
    }
}

fn parse_env<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(name) {
        Ok(raw) => raw
            .parse::<T>()
            .map_err(|error| anyhow::anyhow!("{name} has invalid value {raw}: {error}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => anyhow::bail!("{name} must be valid unicode"),
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use tempfile::tempdir;

    use super::{parse_candidate_retrieval_mode, CandidateRetrievalMode, RankingProfiles};

    fn repo_config_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../configs/ranking")
    }

    fn copy_default_configs(target: &std::path::Path) {
        for name in [
            "schools.default.yaml",
            "events.default.yaml",
            "fallback.default.yaml",
            "tracking.default.yaml",
            "placement.home.yaml",
            "placement.search.yaml",
            "placement.detail.yaml",
            "placement.mypage.yaml",
        ] {
            fs::copy(repo_config_root().join(name), target.join(name)).expect("copy config");
        }
    }

    #[test]
    fn loads_default_phase5_profiles() {
        let profiles = RankingProfiles::load_from_dir(repo_config_root()).expect("profiles");
        assert!(!profiles.profile_version.is_empty());
        assert_eq!(profiles.placements.len(), 4);
    }

    #[test]
    fn rejects_article_until_runtime_support_exists() {
        let temp = tempdir().expect("tempdir");
        copy_default_configs(temp.path());
        fs::write(
            temp.path().join("placement.home.yaml"),
            r#"neighbor_max_hops: 3
neighbor_same_line_bonus: 0.9
mixed_ranking:
  enabled_content_kinds:
    - school
    - article
  score_boosts:
    school: 0.0
    article: 0.5
  featured_event_bonus: 0.4
  event_priority_weight: 0.8
diversity:
  same_school_cap: 1
  same_group_cap: 2
  content_kind_max_ratio:
    school: 0.7
    article: 0.4
"#,
        )
        .expect("write config");

        let error = RankingProfiles::load_from_dir(temp.path()).expect_err("article should fail");
        assert!(error
            .to_string()
            .contains("article is reserved until article candidates are implemented"));
    }

    #[test]
    fn defaults_candidate_retrieval_mode_when_env_is_absent() {
        assert_eq!(
            parse_candidate_retrieval_mode(None).expect("default mode"),
            CandidateRetrievalMode::SqlOnly
        );
    }

    #[test]
    fn rejects_invalid_candidate_retrieval_mode_env() {
        let error = parse_candidate_retrieval_mode(Some("nearest".to_string()))
            .expect_err("invalid mode should fail");
        let rendered = format!("{error:#}");
        assert!(rendered.contains("failed to parse CANDIDATE_RETRIEVAL_MODE=nearest"));
        assert!(rendered.contains("unsupported CANDIDATE_RETRIEVAL_MODE"));
    }

    #[test]
    fn rejects_negative_search_signal_weights() {
        let temp = tempdir().expect("tempdir");
        copy_default_configs(temp.path());
        fs::write(
            temp.path().join("tracking.default.yaml"),
            r#"popularity_bonus_weight: 0.75
user_affinity_bonus_weight: 0.9
area_affinity_bonus_weight: 0.35
search_execute_school_signal_weight: -0.1
search_execute_area_signal_weight: 0.2
"#,
        )
        .expect("write config");

        let error = RankingProfiles::load_from_dir(temp.path()).expect_err("negative weight");
        assert!(error
            .to_string()
            .contains("tracking weights must be zero or positive"));
    }

    #[test]
    fn defaults_missing_search_signal_weights_for_legacy_tracking_config() {
        let temp = tempdir().expect("tempdir");
        copy_default_configs(temp.path());
        fs::write(
            temp.path().join("tracking.default.yaml"),
            r#"popularity_bonus_weight: 0.75
user_affinity_bonus_weight: 0.9
area_affinity_bonus_weight: 0.35
"#,
        )
        .expect("write config");

        let profiles = RankingProfiles::load_from_dir(temp.path()).expect("legacy config");
        assert_eq!(profiles.tracking.search_execute_school_signal_weight, 0.0);
        assert_eq!(profiles.tracking.search_execute_area_signal_weight, 0.0);
    }
}
