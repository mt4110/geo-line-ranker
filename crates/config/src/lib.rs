use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
    sync::Once,
};

use anyhow::{ensure, Context, Result};
use domain::{ContentKind, PlacementKind};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const DEFAULT_POSTGRES_POOL_MAX_SIZE: usize = 16;
pub const RANKING_CONFIG_SCHEMA_VERSION: u32 = 1;
pub const PROFILE_PACK_SCHEMA_VERSION: u32 = 1;
pub const PROFILE_REASON_CATALOG_SCHEMA_VERSION: u32 = 1;
pub const PROFILE_FIXTURE_SET_SCHEMA_VERSION: u32 = 1;

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
            postgres_pool_max_size: parse_postgres_pool_max_size_env()?,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RankingConfigKind {
    RankingSchools,
    RankingEvents,
    RankingPlacement,
    RankingFallback,
    RankingTracking,
}

impl RankingConfigKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RankingSchools => "ranking_schools",
            Self::RankingEvents => "ranking_events",
            Self::RankingPlacement => "ranking_placement",
            Self::RankingFallback => "ranking_fallback",
            Self::RankingTracking => "ranking_tracking",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchoolsProfile {
    pub schema_version: u32,
    pub kind: RankingConfigKind,
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
    pub schema_version: u32,
    pub kind: RankingConfigKind,
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
    pub schema_version: u32,
    pub kind: RankingConfigKind,
    pub neighbor_max_hops: u8,
    pub neighbor_same_line_bonus: f64,
    pub mixed_ranking: MixedRankingProfile,
    pub diversity: DiversityProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FallbackProfile {
    pub schema_version: u32,
    pub kind: RankingConfigKind,
    pub min_results: usize,
    pub neighbor_penalty: f64,
    pub neighbor_distance_cap_meters: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrackingProfile {
    pub schema_version: u32,
    pub kind: RankingConfigKind,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankingConfigLintFile {
    pub path: PathBuf,
    pub schema_version: u32,
    pub kind: RankingConfigKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankingConfigLintSummary {
    pub files: Vec<RankingConfigLintFile>,
    pub profile_version: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProfilePackKind {
    ProfilePack,
}

impl ProfilePackKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProfilePack => "profile_pack",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ProfileContextInput {
    Station,
    Line,
    Area,
    UserProfile,
}

impl ProfileContextInput {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Station => "station",
            Self::Line => "line",
            Self::Area => "area",
            Self::UserProfile => "user_profile",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArticleSupport {
    Reserved,
    Implemented,
}

impl ArticleSupport {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Implemented => "implemented",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProfilePackManifest {
    pub schema_version: u32,
    pub kind: ProfilePackKind,
    pub manifest_version: u32,
    pub profile_id: String,
    pub display_name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub supported_content_kinds: Vec<ContentKind>,
    pub context_inputs: Vec<ProfileContextInput>,
    pub fallback_policy: String,
    pub ranking_config_dir: String,
    pub reason_catalog: String,
    pub article_support: ArticleSupport,
    #[serde(default)]
    pub fixtures: Vec<ProfileFixtureRef>,
    #[serde(default)]
    pub source_manifests: Vec<String>,
    #[serde(default)]
    pub event_csv_examples: Vec<String>,
    #[serde(default)]
    pub optional_crawler_manifests: Vec<String>,
    #[serde(default)]
    pub examples: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProfileFixtureRef {
    pub fixture_set_id: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ProfileFixtureManifestHeader {
    schema_version: u32,
    kind: String,
    manifest_version: u32,
    fixture_set_id: String,
    #[serde(default)]
    profile_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProfileReasonCatalogKind {
    ProfileReasonCatalog,
}

impl ProfileReasonCatalogKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProfileReasonCatalog => "profile_reason_catalog",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProfileReasonLayer {
    Core,
    Profile,
}

impl ProfileReasonLayer {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::Profile => "profile",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProfileReasonCatalog {
    pub schema_version: u32,
    pub kind: ProfileReasonCatalogKind,
    pub profile_id: String,
    pub reasons: Vec<ProfileReason>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProfileReason {
    pub feature: String,
    pub reason_code: String,
    pub label: String,
    pub layer: ProfileReasonLayer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfilePackLintFile {
    pub path: PathBuf,
    pub profile_id: String,
    pub schema_version: u32,
    pub kind: ProfilePackKind,
    pub manifest_version: u32,
    pub supported_content_kinds: Vec<ContentKind>,
    pub reason_count: usize,
    pub fixture_count: usize,
    pub source_manifest_count: usize,
    pub optional_crawler_manifest_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfilePackLintSummary {
    pub files: Vec<ProfilePackLintFile>,
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
        validate_config_contract(
            "schools.default.yaml",
            self.schools.schema_version,
            self.schools.kind,
            RankingConfigKind::RankingSchools,
        )?;
        validate_config_contract(
            "events.default.yaml",
            self.events.schema_version,
            self.events.kind,
            RankingConfigKind::RankingEvents,
        )?;
        validate_config_contract(
            "fallback.default.yaml",
            self.fallback.schema_version,
            self.fallback.kind,
            RankingConfigKind::RankingFallback,
        )?;
        validate_config_contract(
            "tracking.default.yaml",
            self.tracking.schema_version,
            self.tracking.kind,
            RankingConfigKind::RankingTracking,
        )?;

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
            validate_config_contract(
                &format!("placement.{}.yaml", placement.as_str()),
                profile.schema_version,
                profile.kind,
                RankingConfigKind::RankingPlacement,
            )?;
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

pub fn lint_ranking_config_dir(path: impl AsRef<Path>) -> Result<RankingConfigLintSummary> {
    let path = path.as_ref();
    let profiles = RankingProfiles::load_from_dir(path)?;
    let mut files = vec![
        RankingConfigLintFile {
            path: path.join("schools.default.yaml"),
            schema_version: profiles.schools.schema_version,
            kind: profiles.schools.kind,
        },
        RankingConfigLintFile {
            path: path.join("events.default.yaml"),
            schema_version: profiles.events.schema_version,
            kind: profiles.events.kind,
        },
        RankingConfigLintFile {
            path: path.join("fallback.default.yaml"),
            schema_version: profiles.fallback.schema_version,
            kind: profiles.fallback.kind,
        },
        RankingConfigLintFile {
            path: path.join("tracking.default.yaml"),
            schema_version: profiles.tracking.schema_version,
            kind: profiles.tracking.kind,
        },
    ];

    for placement in [
        PlacementKind::Home,
        PlacementKind::Search,
        PlacementKind::Detail,
        PlacementKind::Mypage,
    ] {
        let profile = profiles.placement(placement);
        files.push(RankingConfigLintFile {
            path: path.join(format!("placement.{}.yaml", placement.as_str())),
            schema_version: profile.schema_version,
            kind: profile.kind,
        });
    }

    files.sort_by(|left, right| left.path.cmp(&right.path));

    Ok(RankingConfigLintSummary {
        files,
        profile_version: profiles.profile_version,
    })
}

pub fn load_profile_pack_manifest(path: impl AsRef<Path>) -> Result<ProfilePackManifest> {
    let path = path.as_ref();
    let raw = read_raw(path)
        .with_context(|| format!("failed to read profile pack manifest {}", path.display()))?;
    let manifest: ProfilePackManifest = serde_yaml::from_str(&raw)
        .with_context(|| format!("failed to parse profile pack manifest {}", path.display()))?;
    validate_profile_pack_contract(path, &manifest)?;
    Ok(manifest)
}

pub fn lint_profile_pack_file(path: impl AsRef<Path>) -> Result<ProfilePackLintFile> {
    let path = path.as_ref();
    let manifest = load_profile_pack_manifest(path)?;
    let manifest_dir = path.parent().unwrap_or_else(|| Path::new("."));

    let ranking_config_dir =
        resolve_profile_ref(path, "ranking_config_dir", &manifest.ranking_config_dir)?;
    ensure!(
        ranking_config_dir.is_dir(),
        "profile pack {} ranking_config_dir {} is missing or not a directory",
        path.display(),
        ranking_config_dir.display()
    );
    lint_ranking_config_dir(&ranking_config_dir)?;

    let reason_catalog_path =
        resolve_profile_ref(path, "reason_catalog", &manifest.reason_catalog)?;
    ensure!(
        reason_catalog_path.is_file(),
        "profile pack {} reason_catalog {} is missing or not a file",
        path.display(),
        reason_catalog_path.display()
    );
    let reason_catalog = load_profile_reason_catalog(&reason_catalog_path)?;
    ensure!(
        reason_catalog.profile_id == manifest.profile_id,
        "profile pack {} reason_catalog profile_id {} does not match {}",
        path.display(),
        reason_catalog.profile_id,
        manifest.profile_id
    );

    let mut fixture_ids = BTreeSet::new();
    for fixture in &manifest.fixtures {
        ensure!(
            !fixture.fixture_set_id.trim().is_empty(),
            "profile pack {} contains a fixture with empty fixture_set_id",
            path.display()
        );
        ensure!(
            fixture_ids.insert(fixture.fixture_set_id.clone()),
            "profile pack {} contains duplicate fixture_set_id {}",
            path.display(),
            fixture.fixture_set_id
        );
        let fixture_dir = resolve_profile_ref(path, "fixtures.path", &fixture.path)?;
        ensure!(
            fixture_dir.is_dir(),
            "profile pack {} fixture {} path {} is missing or not a directory",
            path.display(),
            fixture.fixture_set_id,
            fixture_dir.display()
        );
        ensure!(
            fixture_dir.join("fixture_manifest.yaml").is_file(),
            "profile pack {} fixture {} path {} is missing fixture_manifest.yaml",
            path.display(),
            fixture.fixture_set_id,
            fixture_dir.display()
        );
        validate_profile_fixture_ref(
            path,
            &manifest.profile_id,
            fixture,
            &fixture_dir.join("fixture_manifest.yaml"),
        )?;
    }

    for referenced_file in manifest
        .source_manifests
        .iter()
        .chain(manifest.event_csv_examples.iter())
        .chain(manifest.optional_crawler_manifests.iter())
        .chain(manifest.examples.iter())
    {
        let resolved = manifest_dir.join(validate_portable_relative_path(
            path,
            "profile file reference",
            referenced_file,
        )?);
        ensure!(
            resolved.is_file(),
            "profile pack {} file reference {} is missing or not a file",
            path.display(),
            resolved.display()
        );
    }

    Ok(ProfilePackLintFile {
        path: path.to_path_buf(),
        profile_id: manifest.profile_id,
        schema_version: manifest.schema_version,
        kind: manifest.kind,
        manifest_version: manifest.manifest_version,
        supported_content_kinds: manifest.supported_content_kinds,
        reason_count: reason_catalog.reasons.len(),
        fixture_count: manifest.fixtures.len(),
        source_manifest_count: manifest.source_manifests.len(),
        optional_crawler_manifest_count: manifest.optional_crawler_manifests.len(),
    })
}

pub fn lint_profile_pack_dir(path: impl AsRef<Path>) -> Result<ProfilePackLintSummary> {
    let path = path.as_ref();
    let mut files = Vec::new();
    let mut seen_profile_ids = BTreeSet::new();
    for manifest_path in list_profile_manifest_paths(path)? {
        let file = lint_profile_pack_file(manifest_path)?;
        ensure!(
            seen_profile_ids.insert(file.profile_id.clone()),
            "profile pack path {} contains duplicate profile_id {}",
            path.display(),
            file.profile_id
        );
        files.push(file);
    }
    ensure!(
        !files.is_empty(),
        "profile pack path {} does not contain any profile.yaml manifests",
        path.display()
    );
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(ProfilePackLintSummary { files })
}

fn load_profile_reason_catalog(path: &Path) -> Result<ProfileReasonCatalog> {
    let raw = read_raw(path)
        .with_context(|| format!("failed to read profile reason catalog {}", path.display()))?;
    let catalog: ProfileReasonCatalog = serde_yaml::from_str(&raw)
        .with_context(|| format!("failed to parse profile reason catalog {}", path.display()))?;
    validate_profile_reason_catalog(path, &catalog)?;
    Ok(catalog)
}

fn validate_profile_pack_contract(path: &Path, manifest: &ProfilePackManifest) -> Result<()> {
    ensure!(
        manifest.schema_version == PROFILE_PACK_SCHEMA_VERSION,
        "profile pack {} schema_version {} is unsupported; expected {}",
        path.display(),
        manifest.schema_version,
        PROFILE_PACK_SCHEMA_VERSION
    );
    ensure!(
        manifest.kind == ProfilePackKind::ProfilePack,
        "profile pack {} kind {} is invalid; expected {}",
        path.display(),
        manifest.kind.as_str(),
        ProfilePackKind::ProfilePack.as_str()
    );
    ensure!(
        is_profile_id(&manifest.profile_id),
        "profile pack {} profile_id must use lowercase letters, digits, and hyphens",
        path.display()
    );
    ensure!(
        !manifest.display_name.trim().is_empty(),
        "profile pack {} display_name must not be empty",
        path.display()
    );
    ensure!(
        !manifest.fallback_policy.trim().is_empty(),
        "profile pack {} fallback_policy must not be empty",
        path.display()
    );
    validate_portable_relative_path(path, "ranking_config_dir", &manifest.ranking_config_dir)?;
    validate_portable_relative_path(path, "reason_catalog", &manifest.reason_catalog)?;

    let mut seen_content_kinds = BTreeSet::new();
    ensure!(
        !manifest.supported_content_kinds.is_empty(),
        "profile pack {} supported_content_kinds must not be empty",
        path.display()
    );
    for kind in &manifest.supported_content_kinds {
        ensure!(
            seen_content_kinds.insert(*kind),
            "profile pack {} supported_content_kinds contains duplicate {}",
            path.display(),
            kind.as_str()
        );
    }
    ensure!(
        manifest.article_support == ArticleSupport::Implemented
            || !manifest
                .supported_content_kinds
                .contains(&ContentKind::Article),
        "profile pack {} cannot enable article while article_support is {}",
        path.display(),
        manifest.article_support.as_str()
    );

    let mut seen_context_inputs = BTreeSet::new();
    ensure!(
        !manifest.context_inputs.is_empty(),
        "profile pack {} context_inputs must not be empty",
        path.display()
    );
    for input in &manifest.context_inputs {
        ensure!(
            seen_context_inputs.insert(*input),
            "profile pack {} context_inputs contains duplicate {}",
            path.display(),
            input.as_str()
        );
    }

    for fixture in &manifest.fixtures {
        validate_portable_relative_path(path, "fixtures.path", &fixture.path)?;
    }
    for referenced_file in manifest
        .source_manifests
        .iter()
        .chain(manifest.event_csv_examples.iter())
        .chain(manifest.optional_crawler_manifests.iter())
        .chain(manifest.examples.iter())
    {
        validate_portable_relative_path(path, "profile file reference", referenced_file)?;
    }

    Ok(())
}

fn validate_profile_reason_catalog(path: &Path, catalog: &ProfileReasonCatalog) -> Result<()> {
    ensure!(
        catalog.schema_version == PROFILE_REASON_CATALOG_SCHEMA_VERSION,
        "profile reason catalog {} schema_version {} is unsupported; expected {}",
        path.display(),
        catalog.schema_version,
        PROFILE_REASON_CATALOG_SCHEMA_VERSION
    );
    ensure!(
        catalog.kind == ProfileReasonCatalogKind::ProfileReasonCatalog,
        "profile reason catalog {} kind {} is invalid; expected {}",
        path.display(),
        catalog.kind.as_str(),
        ProfileReasonCatalogKind::ProfileReasonCatalog.as_str()
    );
    ensure!(
        is_profile_id(&catalog.profile_id),
        "profile reason catalog {} profile_id must use lowercase letters, digits, and hyphens",
        path.display()
    );
    ensure!(
        !catalog.reasons.is_empty(),
        "profile reason catalog {} reasons must not be empty",
        path.display()
    );
    let mut seen_features = BTreeSet::new();
    let mut seen_reason_codes = BTreeSet::new();
    for reason in &catalog.reasons {
        ensure!(
            !reason.feature.trim().is_empty(),
            "profile reason catalog {} contains an empty feature",
            path.display()
        );
        ensure!(
            seen_features.insert(reason.feature.clone()),
            "profile reason catalog {} contains duplicate feature {}",
            path.display(),
            reason.feature
        );
        ensure!(
            !reason.reason_code.trim().is_empty(),
            "profile reason catalog {} feature {} has empty reason_code",
            path.display(),
            reason.feature
        );
        ensure!(
            seen_reason_codes.insert(reason.reason_code.clone()),
            "profile reason catalog {} contains duplicate reason_code {}",
            path.display(),
            reason.reason_code
        );
        ensure!(
            !reason.label.trim().is_empty(),
            "profile reason catalog {} feature {} has empty label",
            path.display(),
            reason.feature
        );
    }
    Ok(())
}

fn validate_profile_fixture_ref(
    profile_path: &Path,
    profile_id: &str,
    fixture: &ProfileFixtureRef,
    fixture_manifest_path: &Path,
) -> Result<()> {
    let raw = read_raw(fixture_manifest_path).with_context(|| {
        format!(
            "failed to read profile pack {} fixture {} manifest {}",
            profile_path.display(),
            fixture.fixture_set_id,
            fixture_manifest_path.display()
        )
    })?;
    let fixture_manifest: ProfileFixtureManifestHeader =
        serde_yaml::from_str(&raw).with_context(|| {
            format!(
                "failed to parse profile pack {} fixture {} manifest {}",
                profile_path.display(),
                fixture.fixture_set_id,
                fixture_manifest_path.display()
            )
        })?;
    ensure!(
        fixture_manifest.schema_version == PROFILE_FIXTURE_SET_SCHEMA_VERSION,
        "profile pack {} fixture {} schema_version {} is unsupported; expected {}",
        profile_path.display(),
        fixture.fixture_set_id,
        fixture_manifest.schema_version,
        PROFILE_FIXTURE_SET_SCHEMA_VERSION
    );
    ensure!(
        fixture_manifest.kind == "fixture_set",
        "profile pack {} fixture {} kind {} is invalid; expected fixture_set",
        profile_path.display(),
        fixture.fixture_set_id,
        fixture_manifest.kind
    );
    ensure!(
        fixture_manifest.fixture_set_id == fixture.fixture_set_id,
        "profile pack {} fixture reference {} points to fixture_set_id {}",
        profile_path.display(),
        fixture.fixture_set_id,
        fixture_manifest.fixture_set_id
    );
    if let Some(fixture_profile_id) = fixture_manifest.profile_id.as_deref() {
        ensure!(
            fixture_profile_id == profile_id,
            "profile pack {} profile_id {} does not match fixture {} profile_id {}",
            profile_path.display(),
            profile_id,
            fixture.fixture_set_id,
            fixture_profile_id
        );
    }
    Ok(())
}

fn resolve_profile_ref(manifest_path: &Path, label: &str, raw_path: &str) -> Result<PathBuf> {
    let manifest_dir = manifest_path.parent().unwrap_or_else(|| Path::new("."));
    Ok(manifest_dir.join(validate_portable_relative_path(
        manifest_path,
        label,
        raw_path,
    )?))
}

fn validate_portable_relative_path(
    manifest_path: &Path,
    label: &str,
    raw_path: &str,
) -> Result<PathBuf> {
    ensure!(
        !raw_path.trim().is_empty(),
        "profile pack {} {} path must not be empty",
        manifest_path.display(),
        label
    );
    ensure!(
        !raw_path.contains('\\') && !has_windows_drive_prefix(raw_path),
        "profile pack {} {} path must use portable POSIX relative syntax",
        manifest_path.display(),
        label
    );
    let path = Path::new(raw_path);
    ensure!(
        !path.is_absolute(),
        "profile pack {} {} path must be relative",
        manifest_path.display(),
        label
    );
    ensure!(
        !path.components().any(|component| {
            matches!(
                component,
                std::path::Component::Prefix(_) | std::path::Component::RootDir
            )
        }),
        "profile pack {} {} path must be relative without a root or prefix",
        manifest_path.display(),
        label
    );
    Ok(path.to_path_buf())
}

fn has_windows_drive_prefix(raw_path: &str) -> bool {
    let bytes = raw_path.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn is_profile_id(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty()
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn list_profile_manifest_paths(path: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    collect_profile_manifest_paths(path, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn collect_profile_manifest_paths(path: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_file() {
        if is_yaml_path(path) {
            paths.push(path.to_path_buf());
        }
        return Ok(());
    }

    for entry in fs::read_dir(path)
        .with_context(|| format!("failed to read profile pack dir {}", path.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to read entry under {}", path.display()))?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            collect_profile_manifest_paths(&entry_path, paths)?;
        } else if entry_path
            .file_name()
            .and_then(|file_name| file_name.to_str())
            == Some("profile.yaml")
        {
            paths.push(entry_path);
        }
    }
    Ok(())
}

fn is_yaml_path(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("yaml" | "yml")
    )
}

fn validate_config_contract(
    label: &str,
    schema_version: u32,
    kind: RankingConfigKind,
    expected_kind: RankingConfigKind,
) -> Result<()> {
    ensure!(
        schema_version == RANKING_CONFIG_SCHEMA_VERSION,
        "{label}.schema_version {schema_version} is unsupported; expected {RANKING_CONFIG_SCHEMA_VERSION}"
    );
    ensure!(
        kind == expected_kind,
        "{label}.kind {} is invalid; expected {}",
        kind.as_str(),
        expected_kind.as_str()
    );
    Ok(())
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

pub fn parse_postgres_pool_max_size(raw: Option<&str>) -> usize {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_POSTGRES_POOL_MAX_SIZE)
}

fn parse_postgres_pool_max_size_env() -> Result<usize> {
    match env::var("POSTGRES_POOL_MAX_SIZE") {
        Ok(raw) => Ok(parse_postgres_pool_max_size(Some(&raw))),
        Err(env::VarError::NotPresent) => Ok(DEFAULT_POSTGRES_POOL_MAX_SIZE),
        Err(env::VarError::NotUnicode(_)) => {
            anyhow::bail!("POSTGRES_POOL_MAX_SIZE must be valid unicode")
        }
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

    use super::{
        lint_profile_pack_dir, lint_profile_pack_file, lint_ranking_config_dir,
        parse_candidate_retrieval_mode, parse_postgres_pool_max_size, CandidateRetrievalMode,
        RankingConfigKind, RankingProfiles, DEFAULT_POSTGRES_POOL_MAX_SIZE,
    };

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

    fn repo_profile_root() -> PathBuf {
        repo_config_root()
            .parent()
            .expect("configs dir")
            .join("profiles")
    }

    fn write_minimal_reason_catalog(path: &std::path::Path, profile_id: &str) {
        fs::write(
            path,
            format!(
                r#"schema_version: 1
kind: profile_reason_catalog
profile_id: {profile_id}
reasons:
  - feature: direct_station_bonus
    reason_code: geo.direct_station
    label: Direct station
    layer: core
"#
            ),
        )
        .expect("reason catalog");
    }

    fn write_minimal_profile_manifest(
        path: &std::path::Path,
        profile_id: &str,
        fixture_set_id: &str,
    ) {
        fs::write(
            path,
            format!(
                r#"schema_version: 1
kind: profile_pack
manifest_version: 1
profile_id: {profile_id}
display_name: Example Profile
supported_content_kinds:
  - school
context_inputs:
  - station
fallback_policy: example_default
ranking_config_dir: ../../ranking
reason_catalog: reasons.yaml
article_support: reserved
fixtures:
  - fixture_set_id: {fixture_set_id}
    path: ../../fixtures/minimal
"#
            ),
        )
        .expect("profile");
    }

    fn write_minimal_fixture_manifest(
        path: &std::path::Path,
        fixture_set_id: &str,
        profile_id: &str,
    ) {
        fs::write(
            path,
            format!(
                r#"schema_version: 1
kind: fixture_set
manifest_version: 2
fixture_set_id: {fixture_set_id}
profile_id: {profile_id}
files: []
"#
            ),
        )
        .expect("fixture manifest");
    }

    #[test]
    fn loads_default_phase5_profiles() {
        let profiles = RankingProfiles::load_from_dir(repo_config_root()).expect("profiles");
        assert!(!profiles.profile_version.is_empty());
        assert_eq!(profiles.placements.len(), 4);
    }

    #[test]
    fn lints_default_ranking_config_contract() {
        let summary = lint_ranking_config_dir(repo_config_root()).expect("lint");
        assert_eq!(summary.files.len(), 8);
        assert!(summary
            .files
            .iter()
            .all(|file| file.schema_version == super::RANKING_CONFIG_SCHEMA_VERSION));
        assert!(summary
            .files
            .iter()
            .any(|file| file.kind == RankingConfigKind::RankingPlacement));
    }

    #[test]
    fn lints_default_profile_pack_contracts() {
        let summary = lint_profile_pack_dir(repo_profile_root()).expect("profile lint");
        let profile_ids = summary
            .files
            .iter()
            .map(|file| file.profile_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            profile_ids,
            vec!["local-discovery-generic", "school-event-jp"]
        );
        assert!(summary.files.iter().all(|file| file.reason_count > 0));
    }

    #[test]
    fn rejects_article_without_profile_support() {
        let temp = tempdir().expect("tempdir");
        let profile_dir = temp.path().join("profiles").join("example-profile");
        let ranking_dir = temp.path().join("ranking");
        let fixture_dir = temp.path().join("fixtures").join("minimal");
        fs::create_dir_all(&profile_dir).expect("profile dir");
        fs::create_dir_all(&ranking_dir).expect("ranking dir");
        fs::create_dir_all(&fixture_dir).expect("fixture dir");
        copy_default_configs(&ranking_dir);
        fs::write(
            fixture_dir.join("fixture_manifest.yaml"),
            "schema_version: 1\n",
        )
        .expect("fixture manifest");
        write_minimal_reason_catalog(&profile_dir.join("reasons.yaml"), "example-profile");
        fs::write(
            profile_dir.join("profile.yaml"),
            r#"schema_version: 1
kind: profile_pack
manifest_version: 1
profile_id: example-profile
display_name: Example Profile
supported_content_kinds:
  - school
  - article
context_inputs:
  - station
fallback_policy: example_default
ranking_config_dir: ../../ranking
reason_catalog: reasons.yaml
article_support: reserved
fixtures:
  - fixture_set_id: minimal
    path: ../../fixtures/minimal
"#,
        )
        .expect("profile");

        let error = lint_profile_pack_file(profile_dir.join("profile.yaml")).expect_err("article");
        assert!(format!("{error:#}").contains("cannot enable article"));
    }

    #[test]
    fn rejects_fixture_set_id_mismatch_in_profile_pack() {
        let temp = tempdir().expect("tempdir");
        let profile_dir = temp.path().join("profiles").join("example-profile");
        let ranking_dir = temp.path().join("ranking");
        let fixture_dir = temp.path().join("fixtures").join("minimal");
        fs::create_dir_all(&profile_dir).expect("profile dir");
        fs::create_dir_all(&ranking_dir).expect("ranking dir");
        fs::create_dir_all(&fixture_dir).expect("fixture dir");
        copy_default_configs(&ranking_dir);
        write_minimal_reason_catalog(&profile_dir.join("reasons.yaml"), "example-profile");
        write_minimal_profile_manifest(
            &profile_dir.join("profile.yaml"),
            "example-profile",
            "minimal",
        );
        write_minimal_fixture_manifest(
            &fixture_dir.join("fixture_manifest.yaml"),
            "other-fixture",
            "example-profile",
        );

        let error =
            lint_profile_pack_file(profile_dir.join("profile.yaml")).expect_err("fixture mismatch");
        assert!(format!("{error:#}").contains("points to fixture_set_id other-fixture"));
    }

    #[test]
    fn rejects_fixture_profile_id_mismatch_in_profile_pack() {
        let temp = tempdir().expect("tempdir");
        let profile_dir = temp.path().join("profiles").join("example-profile");
        let ranking_dir = temp.path().join("ranking");
        let fixture_dir = temp.path().join("fixtures").join("minimal");
        fs::create_dir_all(&profile_dir).expect("profile dir");
        fs::create_dir_all(&ranking_dir).expect("ranking dir");
        fs::create_dir_all(&fixture_dir).expect("fixture dir");
        copy_default_configs(&ranking_dir);
        write_minimal_reason_catalog(&profile_dir.join("reasons.yaml"), "example-profile");
        write_minimal_profile_manifest(
            &profile_dir.join("profile.yaml"),
            "example-profile",
            "minimal",
        );
        write_minimal_fixture_manifest(
            &fixture_dir.join("fixture_manifest.yaml"),
            "minimal",
            "other-profile",
        );

        let error =
            lint_profile_pack_file(profile_dir.join("profile.yaml")).expect_err("profile mismatch");
        assert!(format!("{error:#}").contains("does not match fixture minimal profile_id"));
    }

    #[test]
    fn rejects_mismatched_ranking_config_kind() {
        let temp = tempdir().expect("tempdir");
        copy_default_configs(temp.path());
        fs::write(
            temp.path().join("schools.default.yaml"),
            r#"schema_version: 1
kind: ranking_events
limit_default: 3
strict_min_candidates: 2
direct_station_bonus: 3.0
line_match_bonus: 1.25
distance_scale_meters: 1600.0
walking_scale_minutes: 20.0
"#,
        )
        .expect("write config");

        let error = RankingProfiles::load_from_dir(temp.path()).expect_err("kind mismatch");
        assert!(error
            .to_string()
            .contains("schools.default.yaml.kind ranking_events is invalid"));
    }

    #[test]
    fn rejects_article_until_runtime_support_exists() {
        let temp = tempdir().expect("tempdir");
        copy_default_configs(temp.path());
        fs::write(
            temp.path().join("placement.home.yaml"),
            r#"schema_version: 1
kind: ranking_placement
neighbor_max_hops: 3
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
    fn postgres_pool_max_size_defaults_when_missing_or_invalid() {
        assert_eq!(
            parse_postgres_pool_max_size(None),
            DEFAULT_POSTGRES_POOL_MAX_SIZE
        );
        assert_eq!(
            parse_postgres_pool_max_size(Some("0")),
            DEFAULT_POSTGRES_POOL_MAX_SIZE
        );
        assert_eq!(
            parse_postgres_pool_max_size(Some("invalid")),
            DEFAULT_POSTGRES_POOL_MAX_SIZE
        );
    }

    #[test]
    fn postgres_pool_max_size_accepts_positive_values() {
        assert_eq!(parse_postgres_pool_max_size(Some("32")), 32);
        assert_eq!(parse_postgres_pool_max_size(Some(" 8 ")), 8);
    }

    #[test]
    fn rejects_negative_search_signal_weights() {
        let temp = tempdir().expect("tempdir");
        copy_default_configs(temp.path());
        fs::write(
            temp.path().join("tracking.default.yaml"),
            r#"schema_version: 1
kind: ranking_tracking
popularity_bonus_weight: 0.75
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
            r#"schema_version: 1
kind: ranking_tracking
popularity_bonus_weight: 0.75
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
