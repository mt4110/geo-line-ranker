use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{bail, ensure, Context, Result};
use domain::PlacementKind;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub const CRAWL_MANIFEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceMaturity {
    LiveReady,
    PolicyBlocked,
    ParserOnly,
}

impl SourceMaturity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LiveReady => "live_ready",
            Self::PolicyBlocked => "policy_blocked",
            Self::ParserOnly => "parser_only",
        }
    }
}

impl fmt::Display for SourceMaturity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SourceMaturity {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim() {
            "live_ready" => Ok(Self::LiveReady),
            "policy_blocked" => Ok(Self::PolicyBlocked),
            "parser_only" => Ok(Self::ParserOnly),
            _ => bail!(
                "unknown source_maturity {}; expected one of live_ready, policy_blocked, parser_only",
                value
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParserExpectedShape {
    HtmlHeadingPage,
    HtmlCardListing,
    HtmlKeioEventCards,
    HtmlSchoolTourBlocks,
    JsonFeed,
    HtmlQuaSections,
    HtmlSessionTables,
    HtmlMonthlyDlPairs,
}

impl ParserExpectedShape {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HtmlHeadingPage => "html_heading_page",
            Self::HtmlCardListing => "html_card_listing",
            Self::HtmlKeioEventCards => "html_keio_event_cards",
            Self::HtmlSchoolTourBlocks => "html_school_tour_blocks",
            Self::JsonFeed => "json_feed",
            Self::HtmlQuaSections => "html_qua_sections",
            Self::HtmlSessionTables => "html_session_tables",
            Self::HtmlMonthlyDlPairs => "html_monthly_dl_pairs",
        }
    }

    pub fn fixture_extension(self) -> &'static str {
        match self {
            Self::JsonFeed => "json",
            _ => "html",
        }
    }
}

impl fmt::Display for ParserExpectedShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ParserExpectedShape {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim() {
            "html_heading_page" => Ok(Self::HtmlHeadingPage),
            "html_card_listing" => Ok(Self::HtmlCardListing),
            "html_keio_event_cards" => Ok(Self::HtmlKeioEventCards),
            "html_school_tour_blocks" => Ok(Self::HtmlSchoolTourBlocks),
            "json_feed" => Ok(Self::JsonFeed),
            "html_qua_sections" => Ok(Self::HtmlQuaSections),
            "html_session_tables" => Ok(Self::HtmlSessionTables),
            "html_monthly_dl_pairs" => Ok(Self::HtmlMonthlyDlPairs),
            _ => bail!(
                "unknown expected_shape {}; expected one of html_heading_page, html_card_listing, html_keio_event_cards, html_school_tour_blocks, json_feed, html_qua_sections, html_session_tables, html_monthly_dl_pairs",
                value
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedShapeCheck {
    pub matched: bool,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CrawlManifestKind {
    CrawlerSource,
}

impl CrawlManifestKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CrawlerSource => "crawler_source",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CrawlSourceManifest {
    pub schema_version: u32,
    pub kind: CrawlManifestKind,
    pub source_id: String,
    pub source_name: String,
    pub manifest_version: u32,
    pub parser_key: String,
    #[serde(default)]
    pub parser_version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub source_maturity: Option<SourceMaturity>,
    #[serde(default)]
    pub expected_shape: Option<ParserExpectedShape>,
    pub allowlist: AllowlistPolicy,
    #[serde(default)]
    pub defaults: CrawlEventDefaults,
    pub targets: Vec<CrawlTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrawlManifestLintFile {
    pub path: PathBuf,
    pub source_id: String,
    pub schema_version: u32,
    pub kind: CrawlManifestKind,
    pub manifest_version: u32,
    pub parser_key: String,
    pub expected_shape: Option<ParserExpectedShape>,
    pub target_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrawlManifestLintSummary {
    pub files: Vec<CrawlManifestLintFile>,
}

impl CrawlSourceManifest {
    pub fn effective_parser_version(&self, default: &str) -> String {
        self.parser_version
            .clone()
            .unwrap_or_else(|| default.to_string())
    }

    pub fn effective_source_maturity(&self) -> SourceMaturity {
        self.source_maturity.unwrap_or({
            if self.allowlist.live_fetch_enabled {
                SourceMaturity::LiveReady
            } else {
                SourceMaturity::PolicyBlocked
            }
        })
    }

    pub fn effective_expected_shape(
        &self,
        parser: Option<&dyn CrawlParser>,
    ) -> Option<ParserExpectedShape> {
        self.expected_shape
            .or_else(|| parser.map(|parser| parser.expected_shape()))
    }

    pub fn resolved_targets(&self) -> Result<Vec<ResolvedCrawlTarget>> {
        self.targets
            .iter()
            .map(|target| target.resolve(&self.defaults))
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AllowlistPolicy {
    pub allowed_domains: Vec<String>,
    pub user_agent: String,
    #[serde(default = "default_min_fetch_interval_ms")]
    pub min_fetch_interval_ms: u64,
    #[serde(default = "default_live_fetch_enabled")]
    pub live_fetch_enabled: bool,
    #[serde(default)]
    pub live_fetch_block_reason: Option<String>,
    pub robots_txt_url: String,
    pub terms_url: String,
    pub terms_note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CrawlEventDefaults {
    #[serde(default)]
    pub school_id: Option<String>,
    #[serde(default = "default_event_category")]
    pub event_category: String,
    #[serde(default)]
    pub is_open_day: bool,
    #[serde(default)]
    pub is_featured: bool,
    #[serde(default)]
    pub priority_weight: f64,
    #[serde(default)]
    pub placement_tags: Vec<PlacementKind>,
}

impl Default for CrawlEventDefaults {
    fn default() -> Self {
        Self {
            school_id: None,
            event_category: default_event_category(),
            is_open_day: false,
            is_featured: false,
            priority_weight: 0.0,
            placement_tags: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CrawlTarget {
    pub logical_name: String,
    pub url: String,
    #[serde(default)]
    pub fixture_path: Option<String>,
    #[serde(default)]
    pub school_id: Option<String>,
    #[serde(default)]
    pub event_category: Option<String>,
    #[serde(default)]
    pub is_open_day: Option<bool>,
    #[serde(default)]
    pub is_featured: Option<bool>,
    #[serde(default)]
    pub priority_weight: Option<f64>,
    #[serde(default)]
    pub placement_tags: Option<Vec<PlacementKind>>,
}

impl CrawlTarget {
    fn resolve(&self, defaults: &CrawlEventDefaults) -> Result<ResolvedCrawlTarget> {
        let school_id = self
            .school_id
            .clone()
            .or_else(|| defaults.school_id.clone())
            .context("crawler target is missing school_id")?;
        ensure!(
            !self.logical_name.trim().is_empty(),
            "crawler target logical_name must not be empty"
        );
        ensure!(
            !self.url.trim().is_empty(),
            "crawler target URL must not be empty"
        );

        Ok(ResolvedCrawlTarget {
            logical_name: self.logical_name.clone(),
            url: self.url.clone(),
            fixture_path: self.fixture_path.clone(),
            school_id,
            event_category: self
                .event_category
                .clone()
                .unwrap_or_else(|| defaults.event_category.clone()),
            is_open_day: self.is_open_day.unwrap_or(defaults.is_open_day),
            is_featured: self.is_featured.unwrap_or(defaults.is_featured),
            priority_weight: self.priority_weight.unwrap_or(defaults.priority_weight),
            placement_tags: self
                .placement_tags
                .clone()
                .unwrap_or_else(|| defaults.placement_tags.clone()),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedCrawlTarget {
    pub logical_name: String,
    pub url: String,
    pub fixture_path: Option<String>,
    pub school_id: String,
    pub event_category: String,
    pub is_open_day: bool,
    pub is_featured: bool,
    pub priority_weight: f64,
    pub placement_tags: Vec<PlacementKind>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParseInput<'a> {
    pub source_id: &'a str,
    pub logical_name: &'a str,
    pub target_url: &'a str,
    pub html: &'a str,
    pub target: &'a ResolvedCrawlTarget,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParseReportEntry {
    pub logical_name: Option<String>,
    pub level: String,
    pub code: String,
    pub message: String,
    pub parsed_rows: Option<i64>,
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedEventSeed {
    pub title: String,
    pub starts_at: Option<String>,
    pub school_id: Option<String>,
    pub event_category: Option<String>,
    pub is_open_day: Option<bool>,
    pub is_featured: Option<bool>,
    pub priority_weight: Option<f64>,
    pub placement_tags: Option<Vec<PlacementKind>>,
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParserOutput {
    pub events: Vec<ParsedEventSeed>,
    pub report_entries: Vec<ParseReportEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedEventRecord {
    pub event_id: String,
    pub school_id: String,
    pub title: String,
    pub event_category: String,
    pub is_open_day: bool,
    pub is_featured: bool,
    pub priority_weight: f64,
    pub starts_at: Option<String>,
    pub placement_tags: Vec<PlacementKind>,
    pub logical_name: String,
    pub target_url: String,
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupeReportEntry {
    pub dedupe_key: String,
    pub kept_event_id: String,
    pub dropped_event_id: String,
    pub reason: String,
    pub details: Value,
}

pub trait CrawlParser: Send + Sync {
    fn key(&self) -> &'static str;
    fn default_version(&self) -> &'static str;
    fn expected_shape(&self) -> ParserExpectedShape;
    fn parse(&self, input: &ParseInput<'_>) -> Result<ParserOutput>;
}

pub struct ParserRegistry {
    parsers: BTreeMap<&'static str, Box<dyn CrawlParser>>,
}

impl Default for ParserRegistry {
    fn default() -> Self {
        let mut parsers: BTreeMap<&'static str, Box<dyn CrawlParser>> = BTreeMap::new();
        parsers.insert("single_title_page_v1", Box::new(SingleTitlePageParser));
        parsers.insert("card_listing_v1", Box::new(CardListingParser));
        parsers.insert("utokyo_events_json_v1", Box::new(UtokyoEventsJsonParser));
        parsers.insert("keio_event_listing_v1", Box::new(KeioEventListingParser));
        parsers.insert(
            "aoyama_junior_school_tour_v1",
            Box::new(AoyamaJuniorSchoolTourParser),
        );
        parsers.insert(
            "shibaura_junior_event_page_v1",
            Box::new(ShibauraJuniorEventPageParser),
        );
        parsers.insert(
            "hachioji_junior_session_tables_v1",
            Box::new(HachiojiJuniorSessionTablesParser),
        );
        parsers.insert(
            "nihon_university_junior_info_session_v1",
            Box::new(NihonUniversityJuniorInfoSessionParser),
        );
        Self { parsers }
    }
}

impl ParserRegistry {
    pub fn get(&self, key: &str) -> Option<&dyn CrawlParser> {
        self.parsers.get(key).map(Box::as_ref)
    }
}

pub fn load_manifest(path: impl AsRef<Path>) -> Result<CrawlSourceManifest> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read crawl manifest {}", path.display()))?;
    let manifest: CrawlSourceManifest = serde_yaml::from_str(&raw)
        .with_context(|| format!("failed to parse crawl manifest {}", path.display()))?;
    validate_manifest(&manifest, path)?;
    Ok(manifest)
}

pub fn lint_manifest_file(path: impl AsRef<Path>) -> Result<CrawlManifestLintFile> {
    let path = path.as_ref();
    let registry = ParserRegistry::default();
    lint_manifest_file_with_registry(path, &registry)
}

fn lint_manifest_file_with_registry(
    path: &Path,
    registry: &ParserRegistry,
) -> Result<CrawlManifestLintFile> {
    let manifest = load_manifest(path)?;
    let parser = registry.get(&manifest.parser_key).with_context(|| {
        format!(
            "crawl manifest {} parser_key {} is not registered",
            path.display(),
            manifest.parser_key
        )
    })?;
    if let Some(manifest_shape) = manifest.expected_shape {
        ensure!(
            manifest_shape == parser.expected_shape(),
            "crawl manifest {} expected_shape {} does not match parser {} expected_shape {}",
            path.display(),
            manifest_shape,
            parser.key(),
            parser.expected_shape()
        );
    }
    let expected_shape = manifest.effective_expected_shape(Some(parser));
    validate_fixture_paths(path, &manifest, expected_shape)?;
    Ok(CrawlManifestLintFile {
        path: path.to_path_buf(),
        source_id: manifest.source_id,
        schema_version: manifest.schema_version,
        kind: manifest.kind,
        manifest_version: manifest.manifest_version,
        parser_key: manifest.parser_key,
        expected_shape,
        target_count: manifest.targets.len(),
    })
}

pub fn lint_manifest_dir(path: impl AsRef<Path>) -> Result<CrawlManifestLintSummary> {
    let path = path.as_ref();
    let registry = ParserRegistry::default();
    let mut files = Vec::new();
    for manifest_path in list_yaml_paths(path)? {
        files.push(lint_manifest_file_with_registry(&manifest_path, &registry)?);
    }
    if files.is_empty() {
        if path.is_file() {
            bail!("crawl manifest path {} is not a yaml file", path.display());
        }
        bail!(
            "crawl manifest path {} does not contain any yaml manifests",
            path.display()
        );
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(CrawlManifestLintSummary { files })
}

pub fn check_expected_shape(
    expected_shape: ParserExpectedShape,
    body: &str,
    content_type: Option<&str>,
) -> ExpectedShapeCheck {
    match expected_shape {
        ParserExpectedShape::JsonFeed => check_json_feed_shape(body, content_type),
        ParserExpectedShape::HtmlHeadingPage => {
            check_html_shape(body, content_type, &[("heading", "h1")])
        }
        ParserExpectedShape::HtmlCardListing => check_html_shape(
            body,
            content_type,
            &[("event_card", "article[data-crawl-event], .crawl-event-card")],
        ),
        ParserExpectedShape::HtmlKeioEventCards => check_html_shape(
            body,
            content_type,
            &[
                ("card_link", "a[href]"),
                ("card_title", "h2"),
                ("card_date", "div[class*='_cardEventDate_']"),
            ],
        ),
        ParserExpectedShape::HtmlSchoolTourBlocks => check_html_shape(
            body,
            content_type,
            &[
                ("school_tour_schedule", "section.explan1 dl"),
                ("school_tour_date", "section.explan1 .date"),
                ("external_event_rows", "section.explan3 .tbody > .table"),
            ],
        ),
        ParserExpectedShape::HtmlQuaSections => check_html_shape(
            body,
            content_type,
            &[
                ("container", "div.qua-container"),
                ("section_heading", "h4.qua-wysiwyg-content"),
                ("dated_item", "div.qua-field-list li.qua-field-list__item"),
            ],
        ),
        ParserExpectedShape::HtmlSessionTables => check_html_shape(
            body,
            content_type,
            &[
                ("page_title", "h3[class*='c-pagetitle02']"),
                ("session_table", "table.c-table02"),
            ],
        ),
        ParserExpectedShape::HtmlMonthlyDlPairs => check_html_shape(
            body,
            content_type,
            &[
                ("schedule_box", "div.schedule_box"),
                ("month_heading", "h3.ttl"),
                ("schedule_dt", "dl.text_box > dt"),
                ("schedule_dd", "dl.text_box > dd"),
                ("event_name", "p.event_name"),
            ],
        ),
    }
}

fn check_html_shape(
    body: &str,
    content_type: Option<&str>,
    selectors: &[(&str, &str)],
) -> ExpectedShapeCheck {
    let document = Html::parse_document(body);
    let mut missing = Vec::new();

    for (label, raw_selector) in selectors {
        match Selector::parse(raw_selector) {
            Ok(selector) => {
                if document.select(&selector).next().is_none() {
                    missing.push(*label);
                }
            }
            Err(error) => {
                return ExpectedShapeCheck {
                    matched: false,
                    summary: format!("invalid expected-shape selector {raw_selector}: {error}"),
                };
            }
        }
    }

    let content_type_note = content_type
        .map(|value| format!(" content_type={value}"))
        .unwrap_or_default();
    if missing.is_empty() {
        ExpectedShapeCheck {
            matched: true,
            summary: format!(
                "matched selectors [{}]{}",
                selectors
                    .iter()
                    .map(|(label, _)| *label)
                    .collect::<Vec<_>>()
                    .join(", "),
                content_type_note
            ),
        }
    } else {
        ExpectedShapeCheck {
            matched: false,
            summary: format!(
                "missing selectors [{}]{}",
                missing.join(", "),
                content_type_note
            ),
        }
    }
}

fn check_json_feed_shape(body: &str, content_type: Option<&str>) -> ExpectedShapeCheck {
    let content_type_note = content_type
        .map(|value| format!(" content_type={value}"))
        .unwrap_or_default();
    let parsed = match serde_json::from_str::<Value>(body) {
        Ok(parsed) => parsed,
        Err(error) => {
            return ExpectedShapeCheck {
                matched: false,
                summary: format!("failed to parse JSON feed: {error}{content_type_note}"),
            };
        }
    };

    let Some(rows) = parsed.as_array() else {
        return ExpectedShapeCheck {
            matched: false,
            summary: format!("expected a top-level JSON array{content_type_note}"),
        };
    };
    if rows.is_empty() {
        return ExpectedShapeCheck {
            matched: false,
            summary: format!("expected a non-empty JSON array{content_type_note}"),
        };
    }
    let has_known_keys = rows.iter().any(|row| {
        row.get("pageTitle").is_some()
            || row.get("title").is_some()
            || row.get("url").is_some()
            || row.get("eventStartDate").is_some()
    });
    if !has_known_keys {
        return ExpectedShapeCheck {
            matched: false,
            summary: format!(
                "JSON feed is present but missing expected event keys (pageTitle/url/eventStartDate){content_type_note}"
            ),
        };
    }

    ExpectedShapeCheck {
        matched: true,
        summary: format!("matched JSON feed rows={}{}", rows.len(), content_type_note),
    }
}

pub fn dedupe_events(
    mut events: Vec<ParsedEventRecord>,
) -> (Vec<ParsedEventRecord>, Vec<DedupeReportEntry>) {
    events.sort_by(|left, right| {
        left.event_id
            .cmp(&right.event_id)
            .then_with(|| left.logical_name.cmp(&right.logical_name))
            .then_with(|| left.target_url.cmp(&right.target_url))
            .then_with(|| left.title.cmp(&right.title))
    });

    let mut deduped = Vec::new();
    let mut reports = Vec::new();
    let mut seen = BTreeMap::<String, ParsedEventRecord>::new();

    for event in events {
        let dedupe_key = event.event_id.clone();
        if let Some(existing) = seen.get(&dedupe_key) {
            reports.push(DedupeReportEntry {
                dedupe_key,
                kept_event_id: existing.event_id.clone(),
                dropped_event_id: event.event_id.clone(),
                reason: "duplicate_event_id".to_string(),
                details: json!({
                    "kept_logical_name": existing.logical_name,
                    "dropped_logical_name": event.logical_name,
                    "kept_target_url": existing.target_url,
                    "dropped_target_url": event.target_url
                }),
            });
            continue;
        }

        seen.insert(event.event_id.clone(), event.clone());
        deduped.push(event);
    }

    (deduped, reports)
}

pub fn finalize_parsed_events(
    source_id: &str,
    logical_name: &str,
    target_url: &str,
    target: &ResolvedCrawlTarget,
    seeds: Vec<ParsedEventSeed>,
) -> Result<Vec<ParsedEventRecord>> {
    seeds
        .into_iter()
        .map(|seed| {
            let title = seed.title.trim().to_string();
            ensure!(!title.is_empty(), "parsed event title must not be empty");

            let school_id = seed
                .school_id
                .clone()
                .unwrap_or_else(|| target.school_id.clone());
            let starts_at = seed
                .starts_at
                .clone()
                .filter(|value| !value.trim().is_empty());
            let event_id = build_event_id(source_id, &school_id, &title, starts_at.as_deref());

            Ok(ParsedEventRecord {
                event_id,
                school_id,
                title,
                event_category: seed
                    .event_category
                    .clone()
                    .unwrap_or_else(|| target.event_category.clone()),
                is_open_day: seed.is_open_day.unwrap_or(target.is_open_day),
                is_featured: seed.is_featured.unwrap_or(target.is_featured),
                priority_weight: seed.priority_weight.unwrap_or(target.priority_weight),
                starts_at,
                placement_tags: seed
                    .placement_tags
                    .clone()
                    .unwrap_or_else(|| target.placement_tags.clone()),
                logical_name: logical_name.to_string(),
                target_url: target_url.to_string(),
                details: seed.details,
            })
        })
        .collect()
}

const UTOKYO_EVENTS_JSON_LIMIT: usize = 60;

struct SingleTitlePageParser;

impl CrawlParser for SingleTitlePageParser {
    fn key(&self) -> &'static str {
        "single_title_page_v1"
    }

    fn default_version(&self) -> &'static str {
        "single-title-page-v1"
    }

    fn expected_shape(&self) -> ParserExpectedShape {
        ParserExpectedShape::HtmlHeadingPage
    }

    fn parse(&self, input: &ParseInput<'_>) -> Result<ParserOutput> {
        let document = Html::parse_document(input.html);
        let title_selector = selector("h1")?;
        let time_selector = selector("time[datetime]")?;

        let title = document
            .select(&title_selector)
            .next()
            .map(extract_text)
            .filter(|value| !value.is_empty())
            .context("single_title_page_v1 could not find <h1>")?;
        let starts_at = document
            .select(&time_selector)
            .next()
            .and_then(|node| node.value().attr("datetime"))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        Ok(ParserOutput {
            events: vec![ParsedEventSeed {
                title: title.clone(),
                starts_at: starts_at.clone(),
                school_id: None,
                event_category: None,
                is_open_day: None,
                is_featured: None,
                priority_weight: None,
                placement_tags: None,
                details: json!({
                    "parser": self.key(),
                    "logical_name": input.logical_name,
                    "target_url": input.target_url
                }),
            }],
            report_entries: vec![ParseReportEntry {
                logical_name: Some(input.logical_name.to_string()),
                level: "info".to_string(),
                code: "single_title_page_extracted".to_string(),
                message: "Extracted one event title from the page heading.".to_string(),
                parsed_rows: Some(1),
                details: json!({
                    "title": title,
                    "starts_at": starts_at
                }),
            }],
        })
    }
}

struct CardListingParser;

impl CrawlParser for CardListingParser {
    fn key(&self) -> &'static str {
        "card_listing_v1"
    }

    fn default_version(&self) -> &'static str {
        "card-listing-v1"
    }

    fn expected_shape(&self) -> ParserExpectedShape {
        ParserExpectedShape::HtmlCardListing
    }

    fn parse(&self, input: &ParseInput<'_>) -> Result<ParserOutput> {
        let document = Html::parse_document(input.html);
        let card_selector = selector("article[data-crawl-event], .crawl-event-card")?;
        let title_selector = selector("h2, h3")?;
        let time_selector = selector("time[datetime]")?;
        let placement_selector = selector("[data-placement-tag]")?;

        let mut events = Vec::new();
        for card in document.select(&card_selector) {
            let title = card
                .select(&title_selector)
                .next()
                .map(extract_text)
                .filter(|value| !value.is_empty())
                .context("card_listing_v1 found a card without a title")?;
            let starts_at = card
                .select(&time_selector)
                .next()
                .and_then(|node| node.value().attr("datetime"))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            let placement_tags = placement_tags_from_card(&card, &placement_selector)?;
            let event_category = card
                .value()
                .attr("data-category")
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            let school_id = card
                .value()
                .attr("data-school-id")
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            let is_open_day = card
                .value()
                .attr("data-open-day")
                .map(parse_bool)
                .transpose()?;
            let is_featured = card
                .value()
                .attr("data-featured")
                .map(parse_bool)
                .transpose()?;
            let priority_weight = card
                .value()
                .attr("data-priority-weight")
                .map(|value| value.trim().parse::<f64>())
                .transpose()
                .with_context(|| "card_listing_v1 failed to parse data-priority-weight")?;

            events.push(ParsedEventSeed {
                title,
                starts_at,
                school_id,
                event_category,
                is_open_day,
                is_featured,
                priority_weight,
                placement_tags,
                details: json!({
                    "parser": self.key(),
                    "logical_name": input.logical_name,
                    "target_url": input.target_url
                }),
            });
        }

        ensure!(
            !events.is_empty(),
            "card_listing_v1 did not find any event cards"
        );

        let parsed_count = events.len() as i64;
        Ok(ParserOutput {
            events,
            report_entries: vec![ParseReportEntry {
                logical_name: Some(input.logical_name.to_string()),
                level: "info".to_string(),
                code: "card_listing_extracted".to_string(),
                message: "Extracted event cards from the crawler page.".to_string(),
                parsed_rows: Some(parsed_count),
                details: json!({
                    "target_url": input.target_url
                }),
            }],
        })
    }
}

#[derive(Debug, Deserialize)]
struct UtokyoEventEntry {
    #[serde(rename = "pageTitle")]
    page_title: Option<String>,
    #[serde(rename = "eventStartDate")]
    event_start_date: Option<String>,
    #[serde(rename = "eventEndDate")]
    event_end_date: Option<String>,
    #[serde(rename = "eventType")]
    event_type: Option<String>,
    #[serde(rename = "eventTarget")]
    event_target: Option<String>,
    #[serde(rename = "eventArea")]
    event_area: Option<String>,
    #[serde(rename = "eventApp")]
    event_app: Option<String>,
    busho: Option<String>,
    url: Option<String>,
}

#[derive(Debug)]
struct UtokyoParsedEvent {
    title: String,
    starts_at: Option<String>,
    event_end_date: Option<String>,
    event_type: Option<String>,
    event_target: Option<String>,
    event_area: Option<String>,
    event_app: Option<String>,
    busho: Option<String>,
    detail_url: String,
}

struct UtokyoEventsJsonParser;

impl CrawlParser for UtokyoEventsJsonParser {
    fn key(&self) -> &'static str {
        "utokyo_events_json_v1"
    }

    fn default_version(&self) -> &'static str {
        "utokyo-events-json-v1"
    }

    fn expected_shape(&self) -> ParserExpectedShape {
        ParserExpectedShape::JsonFeed
    }

    fn parse(&self, input: &ParseInput<'_>) -> Result<ParserOutput> {
        let feed: Vec<UtokyoEventEntry> = serde_json::from_str(input.html)
            .with_context(|| "utokyo_events_json_v1 failed to parse JSON feed")?;

        let mut dropped_missing_title = 0_usize;
        let mut dropped_missing_url = 0_usize;
        let mut events = Vec::new();

        for entry in feed {
            let title = match normalize_optional_text(entry.page_title.as_deref()) {
                Some(title) => title,
                None => {
                    dropped_missing_title += 1;
                    continue;
                }
            };
            let detail_url = match normalize_optional_text(entry.url.as_deref()) {
                Some(url) => to_absolute_utokyo_url(&url),
                None => {
                    dropped_missing_url += 1;
                    continue;
                }
            };

            events.push(UtokyoParsedEvent {
                title,
                starts_at: entry
                    .event_start_date
                    .as_deref()
                    .and_then(normalize_slash_date),
                event_end_date: entry
                    .event_end_date
                    .as_deref()
                    .and_then(normalize_slash_date),
                event_type: normalize_optional_text(entry.event_type.as_deref()),
                event_target: normalize_optional_text(entry.event_target.as_deref()),
                event_area: normalize_optional_text(entry.event_area.as_deref()),
                event_app: normalize_optional_text(entry.event_app.as_deref()),
                busho: normalize_optional_text(entry.busho.as_deref()),
                detail_url,
            });
        }

        ensure!(
            !events.is_empty(),
            "utokyo_events_json_v1 did not find any usable event rows"
        );

        events.sort_by(|left, right| {
            right
                .starts_at
                .cmp(&left.starts_at)
                .then_with(|| left.title.cmp(&right.title))
                .then_with(|| left.detail_url.cmp(&right.detail_url))
        });

        let input_rows = events.len() + dropped_missing_title + dropped_missing_url;
        if events.len() > UTOKYO_EVENTS_JSON_LIMIT {
            events.truncate(UTOKYO_EVENTS_JSON_LIMIT);
        }

        let parsed_count = events.len() as i64;
        let events = events
            .into_iter()
            .map(|event| ParsedEventSeed {
                title: event.title,
                starts_at: event.starts_at,
                school_id: None,
                event_category: None,
                is_open_day: None,
                is_featured: None,
                priority_weight: None,
                placement_tags: None,
                details: json!({
                    "parser": self.key(),
                    "logical_name": input.logical_name,
                    "target_url": input.target_url,
                    "detail_url": event.detail_url,
                    "event_type": event.event_type,
                    "event_target": event.event_target,
                    "event_area": event.event_area,
                    "event_app": event.event_app,
                    "event_end_date": event.event_end_date,
                    "busho": event.busho
                }),
            })
            .collect::<Vec<_>>();

        Ok(ParserOutput {
            events,
            report_entries: vec![ParseReportEntry {
                logical_name: Some(input.logical_name.to_string()),
                level: "info".to_string(),
                code: "utokyo_events_json_extracted".to_string(),
                message: "Extracted recent University of Tokyo events from the public JSON feed."
                    .to_string(),
                parsed_rows: Some(parsed_count),
                details: json!({
                    "target_url": input.target_url,
                    "input_rows": input_rows,
                    "kept_rows": parsed_count,
                    "max_rows": UTOKYO_EVENTS_JSON_LIMIT,
                    "dropped_missing_title": dropped_missing_title,
                    "dropped_missing_url": dropped_missing_url
                }),
            }],
        })
    }
}

#[derive(Debug, Clone)]
struct KeioParsedEvent {
    title: String,
    starts_at: String,
    event_end_date: Option<String>,
    detail_url: String,
    venue: Option<String>,
    registration: Option<String>,
    ongoing: bool,
}

#[derive(Debug, Clone)]
struct AoyamaSchoolTourEvent {
    title: String,
    starts_at: String,
    raw_date: String,
    sequence_label: Option<String>,
    section_kind: String,
    time_text: Option<String>,
    venue: Option<String>,
    organizer: Option<String>,
    detail_url: String,
}

struct KeioEventListingParser;

impl CrawlParser for KeioEventListingParser {
    fn key(&self) -> &'static str {
        "keio_event_listing_v1"
    }

    fn default_version(&self) -> &'static str {
        "keio-event-listing-v1"
    }

    fn expected_shape(&self) -> ParserExpectedShape {
        ParserExpectedShape::HtmlKeioEventCards
    }

    fn parse(&self, input: &ParseInput<'_>) -> Result<ParserOutput> {
        let document = Html::parse_document(input.html);
        let events = collect_keio_events(&document)?;
        ensure!(
            !events.is_empty(),
            "keio_event_listing_v1 did not find any event cards"
        );

        let parsed_count = events.len() as i64;
        let events = events
            .into_iter()
            .map(|event| ParsedEventSeed {
                title: event.title,
                starts_at: Some(event.starts_at),
                school_id: None,
                event_category: None,
                is_open_day: None,
                is_featured: None,
                priority_weight: None,
                placement_tags: None,
                details: json!({
                    "parser": self.key(),
                    "logical_name": input.logical_name,
                    "target_url": input.target_url,
                    "detail_url": event.detail_url,
                    "venue": event.venue,
                    "registration": event.registration,
                    "event_end_date": event.event_end_date,
                    "ongoing": event.ongoing
                }),
            })
            .collect::<Vec<_>>();

        Ok(ParserOutput {
            events,
            report_entries: vec![ParseReportEntry {
                logical_name: Some(input.logical_name.to_string()),
                level: "info".to_string(),
                code: "keio_event_listing_extracted".to_string(),
                message: "Extracted public Keio University events from the listing page."
                    .to_string(),
                parsed_rows: Some(parsed_count),
                details: json!({
                    "target_url": input.target_url,
                    "kept_rows": parsed_count
                }),
            }],
        })
    }
}

struct AoyamaJuniorSchoolTourParser;

impl CrawlParser for AoyamaJuniorSchoolTourParser {
    fn key(&self) -> &'static str {
        "aoyama_junior_school_tour_v1"
    }

    fn default_version(&self) -> &'static str {
        "aoyama-junior-school-tour-v1"
    }

    fn expected_shape(&self) -> ParserExpectedShape {
        ParserExpectedShape::HtmlSchoolTourBlocks
    }

    fn parse(&self, input: &ParseInput<'_>) -> Result<ParserOutput> {
        let document = Html::parse_document(input.html);
        let events = collect_aoyama_school_tour_events(&document, input.target_url)?;
        ensure!(
            !events.is_empty(),
            "aoyama_junior_school_tour_v1 did not find any school-tour rows"
        );

        let parsed_count = events.len() as i64;
        let events = events
            .into_iter()
            .map(|event| ParsedEventSeed {
                title: event.title,
                starts_at: Some(event.starts_at),
                school_id: None,
                event_category: None,
                is_open_day: None,
                is_featured: None,
                priority_weight: None,
                placement_tags: None,
                details: json!({
                    "parser": self.key(),
                    "logical_name": input.logical_name,
                    "target_url": input.target_url,
                    "raw_date": event.raw_date,
                    "sequence_label": event.sequence_label,
                    "section_kind": event.section_kind,
                    "time_text": event.time_text,
                    "venue": event.venue,
                    "organizer": event.organizer,
                    "detail_url": event.detail_url
                }),
            })
            .collect::<Vec<_>>();

        Ok(ParserOutput {
            events,
            report_entries: vec![ParseReportEntry {
                logical_name: Some(input.logical_name.to_string()),
                level: "info".to_string(),
                code: "aoyama_junior_school_tour_extracted".to_string(),
                message:
                    "Extracted dated school-tour events from the Aoyama Gakuin Junior High admissions page."
                        .to_string(),
                parsed_rows: Some(parsed_count),
                details: json!({
                    "target_url": input.target_url,
                    "kept_rows": parsed_count
                }),
            }],
        })
    }
}

#[derive(Debug, Clone)]
struct ShibauraSection {
    heading: String,
    summary: Option<String>,
    items: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedJapaneseDate {
    iso_date: String,
    start_offset: usize,
    end_offset: usize,
}

#[derive(Debug, Clone)]
struct HachiojiSessionTable {
    title: String,
    summary: Option<String>,
    schedule: String,
    capacity: Option<String>,
    reservation_url: Option<String>,
}

#[derive(Debug, Clone)]
struct NihonUniversityJuniorScheduleEntry {
    month_label: String,
    month_number: u32,
    raw_schedule: String,
    title: String,
    detail_url: Option<String>,
    apply_url: Option<String>,
    official_url: Option<String>,
    extra_links: Vec<(String, String)>,
}

struct ShibauraJuniorEventPageParser;

impl CrawlParser for ShibauraJuniorEventPageParser {
    fn key(&self) -> &'static str {
        "shibaura_junior_event_page_v1"
    }

    fn default_version(&self) -> &'static str {
        "shibaura-junior-event-page-v1"
    }

    fn expected_shape(&self) -> ParserExpectedShape {
        ParserExpectedShape::HtmlQuaSections
    }

    fn parse(&self, input: &ParseInput<'_>) -> Result<ParserOutput> {
        let document = Html::parse_document(input.html);
        let sections = collect_shibaura_sections(&document)?;
        let section_count = sections.len();

        let mut events = Vec::new();
        let mut skipped_items = 0_usize;

        for section in sections {
            for item in section.items {
                let item = normalize_free_text(&item);
                if item.is_empty() || item.contains("未定") || item.contains("再調整中") {
                    skipped_items += 1;
                    continue;
                }

                let dates = extract_japanese_dates(&item);
                if dates.is_empty() {
                    skipped_items += 1;
                    continue;
                }

                let item_label = extract_item_label(&item, &dates);
                let explicit_title = if item_label.is_empty() {
                    let remainder = trim_leading_schedule_noise(&item[dates[0].end_offset..]);
                    if remainder.is_empty() || is_schedule_only_fragment(&remainder) {
                        None
                    } else {
                        Some(remainder)
                    }
                } else {
                    None
                };
                let base_title = explicit_title
                    .clone()
                    .or_else(|| section.summary.clone())
                    .unwrap_or_else(|| section.heading.clone());

                for date in dates {
                    let title = if item_label.is_empty() {
                        base_title.clone()
                    } else {
                        format!("{base_title} {item_label}")
                    };
                    let title = normalize_free_text(&title);

                    events.push(ParsedEventSeed {
                        title,
                        starts_at: Some(date.iso_date.clone()),
                        school_id: None,
                        event_category: None,
                        is_open_day: None,
                        is_featured: None,
                        priority_weight: None,
                        placement_tags: None,
                        details: json!({
                            "parser": self.key(),
                            "logical_name": input.logical_name,
                            "target_url": input.target_url,
                            "section_heading": section.heading,
                            "section_summary": section.summary,
                            "item_label": if item_label.is_empty() { None::<String> } else { Some(item_label.clone()) },
                            "raw_item_text": item,
                            "derived_title": explicit_title
                        }),
                    });
                }
            }
        }

        ensure!(
            !events.is_empty(),
            "shibaura_junior_event_page_v1 did not find any dated event rows"
        );

        events.sort_by(|left, right| {
            left.starts_at
                .cmp(&right.starts_at)
                .then_with(|| left.title.cmp(&right.title))
        });

        let parsed_count = events.len() as i64;
        Ok(ParserOutput {
            events,
            report_entries: vec![ParseReportEntry {
                logical_name: Some(input.logical_name.to_string()),
                level: "info".to_string(),
                code: "shibaura_junior_event_page_extracted".to_string(),
                message:
                    "Extracted dated admission events from the Shibaura Institute of Technology Junior High event page."
                        .to_string(),
                parsed_rows: Some(parsed_count),
                details: json!({
                    "target_url": input.target_url,
                    "section_count": section_count,
                    "skipped_items": skipped_items
                }),
            }],
        })
    }
}

struct HachiojiJuniorSessionTablesParser;

impl CrawlParser for HachiojiJuniorSessionTablesParser {
    fn key(&self) -> &'static str {
        "hachioji_junior_session_tables_v1"
    }

    fn default_version(&self) -> &'static str {
        "hachioji-junior-session-tables-v1"
    }

    fn expected_shape(&self) -> ParserExpectedShape {
        ParserExpectedShape::HtmlSessionTables
    }

    fn parse(&self, input: &ParseInput<'_>) -> Result<ParserOutput> {
        let document = Html::parse_document(input.html);
        let (academic_year, tables) = collect_hachioji_session_tables(&document)?;
        let table_count = tables.len();

        let mut events = Vec::new();
        let mut skipped_tables = 0_usize;

        for table in tables {
            let dates = extract_month_day_dates_for_academic_year(&table.schedule, academic_year);
            if dates.is_empty() {
                skipped_tables += 1;
                continue;
            }

            for starts_at in dates {
                events.push(ParsedEventSeed {
                    title: table.title.clone(),
                    starts_at: Some(starts_at),
                    school_id: None,
                    event_category: None,
                    is_open_day: None,
                    is_featured: None,
                    priority_weight: None,
                    placement_tags: None,
                    details: json!({
                        "parser": self.key(),
                        "logical_name": input.logical_name,
                        "target_url": input.target_url,
                        "academic_year": academic_year,
                        "summary": table.summary,
                        "schedule": table.schedule,
                        "capacity": table.capacity,
                        "reservation_url": table.reservation_url
                    }),
                });
            }
        }

        ensure!(
            !events.is_empty(),
            "hachioji_junior_session_tables_v1 did not find any dated session rows"
        );

        events.sort_by(|left, right| {
            left.starts_at
                .cmp(&right.starts_at)
                .then_with(|| left.title.cmp(&right.title))
        });

        let parsed_count = events.len() as i64;
        Ok(ParserOutput {
            events,
            report_entries: vec![ParseReportEntry {
                logical_name: Some(input.logical_name.to_string()),
                level: "info".to_string(),
                code: "hachioji_junior_session_tables_extracted".to_string(),
                message:
                    "Extracted dated admissions sessions from the Hachioji Gakuen Hachioji Junior High schedule page."
                        .to_string(),
                parsed_rows: Some(parsed_count),
                details: json!({
                    "target_url": input.target_url,
                    "academic_year": academic_year,
                    "table_count": table_count,
                    "skipped_tables": skipped_tables
                }),
            }],
        })
    }
}

struct NihonUniversityJuniorInfoSessionParser;

impl CrawlParser for NihonUniversityJuniorInfoSessionParser {
    fn key(&self) -> &'static str {
        "nihon_university_junior_info_session_v1"
    }

    fn default_version(&self) -> &'static str {
        "nihon-university-junior-info-session-v1"
    }

    fn expected_shape(&self) -> ParserExpectedShape {
        ParserExpectedShape::HtmlMonthlyDlPairs
    }

    fn parse(&self, input: &ParseInput<'_>) -> Result<ParserOutput> {
        let document = Html::parse_document(input.html);
        let entries = collect_nihon_junior_schedule_entries(&document)?;
        ensure!(
            !entries.is_empty(),
            "nihon_university_junior_info_session_v1 did not find any schedule entries"
        );

        let mut year_hint = entries
            .iter()
            .filter_map(|entry| entry.detail_url.as_deref())
            .filter_map(extract_ymd_from_url_path)
            .find_map(|date| date[..4].parse::<i32>().ok());
        let mut previous_month = None::<u32>;
        let mut events = Vec::new();

        for entry in entries {
            let detail_url_date = entry
                .detail_url
                .as_deref()
                .and_then(extract_ymd_from_url_path);
            let starts_at = if let Some(date) = detail_url_date.clone() {
                year_hint = date[..4].parse::<i32>().ok();
                previous_month = Some(entry.month_number);
                date
            } else {
                let day =
                    extract_first_day_from_schedule(&entry.raw_schedule).with_context(|| {
                        format!(
                        "nihon_university_junior_info_session_v1 could not parse day from {} {}",
                        entry.month_label, entry.raw_schedule
                    )
                    })?;
                let mut fallback_year = year_hint.with_context(|| {
                    format!(
                        "nihon_university_junior_info_session_v1 could not infer year for {} {}",
                        entry.month_label, entry.raw_schedule
                    )
                })?;
                if let Some(previous_month_value) = previous_month {
                    if entry.month_number < previous_month_value {
                        fallback_year += 1;
                        year_hint = Some(fallback_year);
                    }
                } else {
                    year_hint = Some(fallback_year);
                }
                previous_month = Some(entry.month_number);
                format!(
                    "{fallback_year:04}-{month:02}-{day:02}",
                    month = entry.month_number
                )
            };

            events.push(ParsedEventSeed {
                title: entry.title,
                starts_at: Some(starts_at),
                school_id: None,
                event_category: None,
                is_open_day: None,
                is_featured: None,
                priority_weight: None,
                placement_tags: None,
                details: json!({
                    "parser": self.key(),
                    "logical_name": input.logical_name,
                    "target_url": input.target_url,
                    "month_label": entry.month_label,
                    "raw_schedule": entry.raw_schedule,
                    "date_source": if detail_url_date.is_some() { "detail_url" } else { "month_heading" },
                    "detail_url": entry.detail_url,
                    "detail_url_date": detail_url_date,
                    "apply_url": entry.apply_url,
                    "official_url": entry.official_url,
                    "extra_links": entry.extra_links.into_iter().map(|(label, url)| json!({
                        "label": label,
                        "url": url
                    })).collect::<Vec<_>>()
                }),
            });
        }

        events.sort_by(|left, right| {
            left.starts_at
                .cmp(&right.starts_at)
                .then_with(|| left.title.cmp(&right.title))
        });

        let parsed_count = events.len() as i64;
        Ok(ParserOutput {
            events,
            report_entries: vec![ParseReportEntry {
                logical_name: Some(input.logical_name.to_string()),
                level: "info".to_string(),
                code: "nihon_university_junior_info_session_extracted".to_string(),
                message:
                    "Extracted dated admissions events from the Nihon University Junior High info-session page."
                        .to_string(),
                parsed_rows: Some(parsed_count),
                details: json!({
                    "target_url": input.target_url,
                    "kept_rows": parsed_count
                }),
            }],
        })
    }
}

fn placement_tags_from_card(
    card: &scraper::ElementRef<'_>,
    placement_selector: &Selector,
) -> Result<Option<Vec<PlacementKind>>> {
    if let Some(raw) = card.value().attr("data-placement-tags") {
        return Ok(Some(parse_placement_tags(raw)?));
    }

    let mut tags = Vec::new();
    for node in card.select(placement_selector) {
        if let Some(raw) = node.value().attr("data-placement-tag") {
            tags.push(parse_placement_kind(raw.trim())?);
        }
    }

    if tags.is_empty() {
        Ok(None)
    } else {
        Ok(Some(tags))
    }
}

fn validate_manifest(manifest: &CrawlSourceManifest, path: &Path) -> Result<()> {
    let source_maturity = manifest.effective_source_maturity();

    ensure!(
        manifest.schema_version == CRAWL_MANIFEST_SCHEMA_VERSION,
        "crawl manifest {} schema_version {} is unsupported; expected {}",
        path.display(),
        manifest.schema_version,
        CRAWL_MANIFEST_SCHEMA_VERSION
    );
    ensure!(
        manifest.kind == CrawlManifestKind::CrawlerSource,
        "crawl manifest {} kind {} is invalid; expected {}",
        path.display(),
        manifest.kind.as_str(),
        CrawlManifestKind::CrawlerSource.as_str()
    );
    ensure!(
        !manifest.source_id.trim().is_empty(),
        "crawl manifest {} is missing source_id",
        path.display()
    );
    ensure!(
        !manifest.source_name.trim().is_empty(),
        "crawl manifest {} is missing source_name",
        path.display()
    );
    ensure!(
        !manifest.parser_key.trim().is_empty(),
        "crawl manifest {} is missing parser_key",
        path.display()
    );
    ensure!(
        !manifest.allowlist.allowed_domains.is_empty(),
        "crawl manifest {} must list allowed_domains",
        path.display()
    );
    ensure!(
        !manifest.allowlist.user_agent.trim().is_empty(),
        "crawl manifest {} must set user_agent",
        path.display()
    );
    ensure!(
        !manifest.allowlist.robots_txt_url.trim().is_empty(),
        "crawl manifest {} must set robots_txt_url",
        path.display()
    );
    ensure!(
        !manifest.allowlist.terms_url.trim().is_empty(),
        "crawl manifest {} must set terms_url",
        path.display()
    );
    ensure!(
        !manifest.allowlist.terms_note.trim().is_empty(),
        "crawl manifest {} must set terms_note",
        path.display()
    );
    if matches!(manifest.source_maturity, Some(SourceMaturity::LiveReady)) {
        ensure!(
            manifest.allowlist.live_fetch_enabled,
            "crawl manifest {} cannot set source_maturity live_ready while live_fetch_enabled is false",
            path.display()
        );
    }
    if matches!(
        manifest.source_maturity,
        Some(SourceMaturity::PolicyBlocked)
    ) {
        ensure!(
            !manifest.allowlist.live_fetch_enabled,
            "crawl manifest {} cannot set source_maturity policy_blocked while live_fetch_enabled is true",
            path.display()
        );
    }
    if matches!(source_maturity, SourceMaturity::PolicyBlocked) {
        ensure!(
            !manifest.allowlist.live_fetch_enabled,
            "crawl manifest {} resolved to policy_blocked while live_fetch_enabled is true",
            path.display()
        );
    }
    if !manifest.allowlist.live_fetch_enabled {
        ensure!(
            manifest
                .allowlist
                .live_fetch_block_reason
                .as_deref()
                .map(str::trim)
                .is_some_and(|value| !value.is_empty()),
            "crawl manifest {} must set live_fetch_block_reason when live_fetch_enabled is false",
            path.display()
        );
    }
    ensure!(
        !manifest.targets.is_empty(),
        "crawl manifest {} must define at least one target",
        path.display()
    );

    for target in manifest.resolved_targets()? {
        ensure!(
            !target.school_id.trim().is_empty(),
            "crawler target {} resolved to an empty school_id",
            target.logical_name
        );
    }
    let mut logical_names = BTreeSet::new();
    for target in &manifest.targets {
        ensure!(
            logical_names.insert(target.logical_name.clone()),
            "crawl manifest {} contains duplicate logical_name {}",
            path.display(),
            target.logical_name
        );
        if let Some(fixture_path) = &target.fixture_path {
            let fixture_path_value = Path::new(fixture_path);
            ensure!(
                !fixture_path.trim().is_empty(),
                "crawl manifest {} target {} has an empty fixture_path",
                path.display(),
                target.logical_name
            );
            ensure!(
                !fixture_path.contains('\\') && !has_windows_drive_prefix(fixture_path),
                "crawl manifest {} target {} fixture_path must use portable POSIX relative syntax",
                path.display(),
                target.logical_name
            );
            ensure!(
                !fixture_path_value.is_absolute(),
                "crawl manifest {} target {} fixture_path must be relative",
                path.display(),
                target.logical_name
            );
            ensure!(
                !fixture_path_value.components().any(|component| {
                    matches!(
                        component,
                        std::path::Component::Prefix(_) | std::path::Component::RootDir
                    )
                }),
                "crawl manifest {} target {} fixture_path must be relative without a root or prefix",
                path.display(),
                target.logical_name
            );
        }
    }

    Ok(())
}

fn has_windows_drive_prefix(raw_path: &str) -> bool {
    let bytes = raw_path.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn validate_fixture_paths(
    manifest_path: &Path,
    manifest: &CrawlSourceManifest,
    expected_shape: Option<ParserExpectedShape>,
) -> Result<()> {
    for target in &manifest.targets {
        let Some(fixture_path) = &target.fixture_path else {
            continue;
        };
        let canonical_path = resolve_manifest_fixture_path(manifest_path, fixture_path)
            .with_context(|| {
                format!(
                    "failed to resolve fixture_path for crawl manifest {} target {}",
                    manifest_path.display(),
                    target.logical_name
                )
            })?;
        if let Some(expected_shape) = expected_shape {
            let body = fs::read_to_string(&canonical_path)
                .with_context(|| format!("failed to read fixture {}", canonical_path.display()))?;
            let check =
                check_expected_shape(expected_shape, &body, fixture_content_type(&canonical_path));
            ensure!(
                check.matched,
                "crawl manifest {} target {} fixture_path {} does not match expected_shape {}: {}",
                manifest_path.display(),
                target.logical_name,
                fixture_path,
                expected_shape,
                check.summary
            );
        }
    }
    Ok(())
}

pub fn resolve_manifest_fixture_path(manifest_path: &Path, fixture_path: &str) -> Result<PathBuf> {
    let manifest_dir = manifest_path.parent().unwrap_or_else(|| Path::new("."));
    let resolved_path = manifest_dir.join(fixture_path);
    let allowed_root = allowed_fixture_root(manifest_dir)?;
    ensure!(
        resolved_path.is_file(),
        "fixture_path {} does not exist",
        resolved_path.display()
    );
    let canonical_path = resolved_path.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize fixture_path {}",
            resolved_path.display(),
        )
    })?;
    ensure!(
        canonical_path.starts_with(&allowed_root),
        "fixture_path {} resolves outside allowed fixture root {}",
        resolved_path.display(),
        allowed_root.display()
    );
    Ok(canonical_path)
}

fn allowed_fixture_root(manifest_dir: &Path) -> Result<PathBuf> {
    let canonical_manifest_dir = manifest_dir.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize crawl manifest dir {}",
            manifest_dir.display()
        )
    })?;
    for ancestor in canonical_manifest_dir.ancestors() {
        let candidate = ancestor.join("storage").join("fixtures");
        if candidate.is_dir() {
            return candidate.canonicalize().with_context(|| {
                format!(
                    "failed to canonicalize fixture root {}",
                    candidate.display()
                )
            });
        }
    }
    Ok(canonical_manifest_dir)
}

fn selector(raw: &str) -> Result<Selector> {
    Selector::parse(raw).map_err(|_| anyhow::anyhow!("failed to parse selector {raw}"))
}

fn extract_text(node: scraper::ElementRef<'_>) -> String {
    node.text()
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn extract_joined_text(node: scraper::ElementRef<'_>) -> String {
    node.text()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_free_text(raw: &str) -> String {
    raw.replace(['\u{00a0}', '\u{3000}'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_optional_text(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn normalize_slash_date(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    let mut parts = raw.split('/');
    let year = parts.next()?.parse::<u32>().ok()?;
    let month = parts.next()?.parse::<u32>().ok()?;
    let day = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }

    Some(format!("{year:04}-{month:02}-{day:02}"))
}

fn to_absolute_utokyo_url(raw: &str) -> String {
    let raw = raw.trim();
    if raw.starts_with("https://") || raw.starts_with("http://") {
        raw.to_string()
    } else {
        format!("https://www.u-tokyo.ac.jp/{}", raw.trim_start_matches('/'))
    }
}

fn to_absolute_keio_url(raw: &str) -> String {
    let raw = raw.trim();
    if raw.starts_with("https://") || raw.starts_with("http://") {
        raw.to_string()
    } else {
        format!("https://www.keio.ac.jp/{}", raw.trim_start_matches('/'))
    }
}

fn to_absolute_aoyama_url(raw: &str) -> String {
    let raw = raw.trim();
    if raw.starts_with("https://") || raw.starts_with("http://") {
        raw.to_string()
    } else {
        format!(
            "https://www.jh.aoyama.ed.jp/{}",
            raw.trim_start_matches('/')
        )
    }
}

fn to_absolute_nihon_url(raw: &str) -> String {
    let raw = raw.trim();
    if raw.starts_with("https://") || raw.starts_with("http://") {
        raw.to_string()
    } else {
        format!(
            "https://www.yokohama.hs.nihon-u.ac.jp/{}",
            raw.trim_start_matches('/')
        )
    }
}

fn collect_aoyama_school_tour_events(
    document: &Html,
    target_url: &str,
) -> Result<Vec<AoyamaSchoolTourEvent>> {
    let internal_section_selector = selector("section.explan1")?;
    let external_section_selector = selector("section.explan3")?;
    let table_selector = selector("div.table")?;
    let label_selector = selector("div.cell.th")?;
    let value_selector = selector("div.cell.td")?;
    let dl_selector = selector("dl")?;
    let dt_selector = selector("dt")?;
    let dd_selector = selector("dd")?;
    let date_selector = selector("div.date")?;
    let time_selector = selector("div.time")?;
    let venue_selector = selector("p")?;
    let row_selector = selector("div.tbody > div.table")?;
    let cell_selector = selector("div.cell")?;
    let link_selector = selector("a[href]")?;

    let internal_section = document
        .select(&internal_section_selector)
        .next()
        .context("aoyama_junior_school_tour_v1 could not find the internal school-tour section")?;
    let external_section = document
        .select(&external_section_selector)
        .next()
        .context("aoyama_junior_school_tour_v1 could not find the external events section")?;

    let internal_venue = internal_section.select(&table_selector).find_map(|table| {
        let label = table
            .select(&label_selector)
            .next()
            .map(extract_text)
            .map(|text| normalize_free_text(&text))?;
        if !label.contains("場") {
            return None;
        }
        table.select(&value_selector).next().and_then(|cell| {
            cell.select(&venue_selector)
                .next()
                .map(extract_joined_text)
                .map(|text| normalize_free_text(&text))
                .filter(|text| !text.is_empty())
        })
    });

    let schedule_dl = internal_section.select(&dl_selector).next().context(
        "aoyama_junior_school_tour_v1 could not find the internal school-tour schedule list",
    )?;
    let labels = schedule_dl.select(&dt_selector).collect::<Vec<_>>();
    let values = schedule_dl.select(&dd_selector).collect::<Vec<_>>();
    ensure!(
        labels.len() == values.len(),
        "aoyama_junior_school_tour_v1 found mismatched dt/dd pairs in the internal school-tour section"
    );

    let mut events = Vec::new();
    for (label_node, value_node) in labels.into_iter().zip(values) {
        let sequence_label = normalize_free_text(&extract_joined_text(label_node));
        let raw_date = value_node
            .select(&date_selector)
            .next()
            .map(extract_joined_text)
            .map(|text| normalize_free_text(&text))
            .filter(|text| !text.is_empty())
            .with_context(|| {
                format!(
                    "aoyama_junior_school_tour_v1 could not find an internal date for {sequence_label}"
                )
            })?;
        let time_text = value_node
            .select(&time_selector)
            .next()
            .map(extract_joined_text)
            .map(|text| normalize_free_text(&text))
            .filter(|text| !text.is_empty());

        for starts_at in extract_explicit_japanese_dates(&raw_date)? {
            events.push(AoyamaSchoolTourEvent {
                title: format!("学校説明会 {sequence_label}"),
                starts_at,
                raw_date: raw_date.clone(),
                sequence_label: Some(sequence_label.clone()),
                section_kind: "school_tour".to_string(),
                time_text: time_text.clone(),
                venue: internal_venue.clone(),
                organizer: None,
                detail_url: target_url.to_string(),
            });
        }
    }

    for row in external_section.select(&row_selector) {
        let cells = row.select(&cell_selector).collect::<Vec<_>>();
        ensure!(
            cells.len() >= 4,
            "aoyama_junior_school_tour_v1 found an external row with fewer than four cells"
        );

        let raw_date = cells[0]
            .select(&date_selector)
            .next()
            .map(extract_joined_text)
            .map(|text| normalize_free_text(&text))
            .filter(|text| !text.is_empty())
            .context("aoyama_junior_school_tour_v1 could not find an external event date")?;
        let time_text = cells[0]
            .select(&time_selector)
            .next()
            .map(extract_joined_text)
            .map(|text| normalize_free_text(&text))
            .filter(|text| !text.is_empty());
        let title = normalize_free_text(&extract_joined_text(cells[1]));
        ensure!(
            !title.is_empty(),
            "aoyama_junior_school_tour_v1 found an external row without a title"
        );
        let detail_url = cells[1]
            .select(&link_selector)
            .next()
            .and_then(|link| link.value().attr("href"))
            .map(to_absolute_aoyama_url)
            .unwrap_or_else(|| target_url.to_string());
        let venue = normalize_free_text(&extract_joined_text(cells[2]));
        let organizer = normalize_free_text(&extract_joined_text(cells[3]));

        for starts_at in extract_explicit_japanese_dates(&raw_date)? {
            events.push(AoyamaSchoolTourEvent {
                title: title.clone(),
                starts_at,
                raw_date: raw_date.clone(),
                sequence_label: None,
                section_kind: "external_school_tour".to_string(),
                time_text: time_text.clone(),
                venue: if venue.is_empty() {
                    None
                } else {
                    Some(venue.clone())
                },
                organizer: if organizer.is_empty() {
                    None
                } else {
                    Some(organizer.clone())
                },
                detail_url: detail_url.clone(),
            });
        }
    }

    events.sort_by(|left, right| {
        left.starts_at
            .cmp(&right.starts_at)
            .then_with(|| left.title.cmp(&right.title))
            .then_with(|| left.detail_url.cmp(&right.detail_url))
    });

    Ok(events)
}

fn extract_explicit_japanese_dates(raw: &str) -> Result<Vec<String>> {
    let year = extract_number_before_marker(raw, '年')
        .with_context(|| format!("could not parse year from {raw}"))?;
    let after_year = raw
        .split_once('年')
        .map(|(_, rest)| rest)
        .context("missing 年 marker")?;
    let month = extract_number_before_marker(after_year, '月')
        .with_context(|| format!("could not parse month from {raw}"))?;
    ensure!(
        (1..=12).contains(&month),
        "unsupported month {} in {}",
        month,
        raw
    );
    let day_part = after_year
        .split_once('月')
        .map(|(_, rest)| rest)
        .and_then(|rest| rest.split_once('日').map(|(days, _)| days))
        .context("missing 日 marker")?;
    let days = extract_all_numbers(day_part);
    ensure!(!days.is_empty(), "could not parse day from {}", raw);

    Ok(days
        .into_iter()
        .map(|day| format!("{year:04}-{month:02}-{day:02}"))
        .collect())
}

fn extract_number_before_marker(raw: &str, marker: char) -> Option<u32> {
    let before = raw.split_once(marker)?.0;
    let numbers = extract_all_numbers(before);
    numbers.last().copied()
}

fn extract_all_numbers(raw: &str) -> Vec<u32> {
    let chars = raw.char_indices().collect::<Vec<_>>();
    let mut index = 0_usize;
    let mut numbers = Vec::new();

    while index < chars.len() {
        if !chars[index].1.is_ascii_digit() {
            index += 1;
            continue;
        }

        let start = chars[index].0;
        let mut end_index = index;
        while end_index < chars.len() && chars[end_index].1.is_ascii_digit() {
            end_index += 1;
        }
        let end = if end_index < chars.len() {
            chars[end_index].0
        } else {
            raw.len()
        };
        if let Ok(value) = raw[start..end].parse::<u32>() {
            numbers.push(value);
        }
        index = end_index;
    }

    numbers
}

fn collect_keio_events(document: &Html) -> Result<Vec<KeioParsedEvent>> {
    let card_selector = selector("a[href]")?;
    let title_selector = selector("h2")?;
    let date_selector = selector("div[class*='_cardEventDate_']")?;
    let year_selector = selector("span[class*='_year_']")?;
    let month_selector = selector("span[class*='_dot_']")?;
    let day_selector = selector("span[class*='_day_']")?;
    let venue_selector = selector("p[class*='_venues_']")?;
    let registration_selector = selector("p[class*='_registration_']")?;
    let ongoing_selector = selector("div[class*='_going_']")?;

    let mut events = Vec::new();
    for card in document.select(&card_selector) {
        let Some(href) = card.value().attr("href") else {
            continue;
        };
        if !href.starts_with("/ja/") && !href.starts_with("https://www.keio.ac.jp/ja/") {
            continue;
        }

        let Some(title_node) = card.select(&title_selector).next() else {
            continue;
        };
        let title = normalize_free_text(&extract_text(title_node));
        if title.is_empty() {
            continue;
        }

        let date_nodes = card.select(&date_selector).collect::<Vec<_>>();
        if date_nodes.is_empty() {
            continue;
        }

        let Some(starts_at) = parse_keio_card_date(
            &date_nodes[0],
            &year_selector,
            &month_selector,
            &day_selector,
            None,
        ) else {
            continue;
        };
        let start_year = starts_at[..4].parse::<u32>().ok();
        let event_end_date = date_nodes.get(1).and_then(|node| {
            parse_keio_card_date(
                node,
                &year_selector,
                &month_selector,
                &day_selector,
                start_year,
            )
        });

        let venue = card
            .select(&venue_selector)
            .next()
            .map(extract_text)
            .map(|text| normalize_free_text(&text))
            .filter(|text| !text.is_empty());
        let registration = card
            .select(&registration_selector)
            .next()
            .map(extract_text)
            .map(|text| normalize_free_text(&text))
            .filter(|text| !text.is_empty());
        let ongoing = card.select(&ongoing_selector).next().is_some();

        events.push(KeioParsedEvent {
            title,
            starts_at,
            event_end_date,
            detail_url: to_absolute_keio_url(href),
            venue,
            registration,
            ongoing,
        });
    }

    events.sort_by(|left, right| {
        left.starts_at
            .cmp(&right.starts_at)
            .then_with(|| left.title.cmp(&right.title))
            .then_with(|| left.detail_url.cmp(&right.detail_url))
    });
    Ok(events)
}

fn parse_keio_card_date(
    node: &scraper::ElementRef<'_>,
    year_selector: &Selector,
    month_selector: &Selector,
    day_selector: &Selector,
    fallback_year: Option<u32>,
) -> Option<String> {
    let year = node
        .select(year_selector)
        .next()
        .and_then(extract_digits_as_u32)
        .or(fallback_year)?;
    let month = node
        .select(month_selector)
        .next()
        .and_then(extract_digits_as_u32)?;
    let day = node
        .select(day_selector)
        .next()
        .and_then(extract_digits_as_u32)?;
    Some(format!("{year:04}-{month:02}-{day:02}"))
}

fn extract_digits_as_u32(node: scraper::ElementRef<'_>) -> Option<u32> {
    let digits = extract_text(node)
        .chars()
        .filter(|character| character.is_ascii_digit())
        .collect::<String>();
    (!digits.is_empty()).then_some(digits)?.parse::<u32>().ok()
}

fn collect_shibaura_sections(document: &Html) -> Result<Vec<ShibauraSection>> {
    let container_selector = selector("div.qua-container")?;
    let heading_selector = selector("h4.qua-wysiwyg-content")?;
    let item_selector = selector("div.qua-field-list li.qua-field-list__item")?;
    let summary_selector = selector("div.qua-unit-text .qua-wysiwyg-content p")?;

    let mut sections = Vec::new();
    for container in document.select(&container_selector) {
        let Some(heading_node) = container.select(&heading_selector).next() else {
            continue;
        };
        let heading = clean_shibaura_heading(&extract_text(heading_node));
        if heading.is_empty() {
            continue;
        }

        let items = container
            .select(&item_selector)
            .map(extract_text)
            .map(|text| normalize_free_text(&text))
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>();
        if items.is_empty() {
            continue;
        }

        let summary = container
            .select(&summary_selector)
            .map(extract_text)
            .map(|text| normalize_free_text(&text))
            .map(|text| clean_shibaura_summary_candidate(&text))
            .find(|text| !text.is_empty());
        let summary = summary.filter(|text| text != &heading);

        sections.push(ShibauraSection {
            heading,
            summary,
            items,
        });
    }

    Ok(sections)
}

fn clean_shibaura_heading(raw: &str) -> String {
    normalize_free_text(
        raw.replace("※要予約", "")
            .replace("＊要予約", "")
            .replace("*要予約", "")
            .replace("New!", "")
            .trim(),
    )
}

fn clean_shibaura_summary_candidate(raw: &str) -> String {
    let text = normalize_free_text(raw);
    if text.starts_with('※') || text.starts_with('★') || text.starts_with('【') {
        String::new()
    } else {
        text
    }
}

fn extract_japanese_dates(raw: &str) -> Vec<ParsedJapaneseDate> {
    let chars = raw.char_indices().collect::<Vec<_>>();
    let mut year = None::<u32>;
    let mut month = None::<u32>;
    let mut year_start = None::<usize>;
    let mut month_start = None::<usize>;
    let mut dates = Vec::new();

    let mut index = 0_usize;
    while index < chars.len() {
        let (start_offset, character) = chars[index];
        if !character.is_ascii_digit() {
            index += 1;
            continue;
        }

        let mut end_index = index;
        let mut value = 0_u32;
        while end_index < chars.len() && chars[end_index].1.is_ascii_digit() {
            value = value * 10 + chars[end_index].1.to_digit(10).unwrap_or(0);
            end_index += 1;
        }

        if end_index >= chars.len() {
            break;
        }

        let marker = chars[end_index].1;
        let end_offset = chars[end_index].0 + marker.len_utf8();
        match marker {
            '年' if value >= 2000 => {
                year = Some(value);
                year_start = Some(start_offset);
                month = None;
                month_start = None;
            }
            '月' if (1..=12).contains(&value) => {
                month = Some(value);
                month_start = Some(start_offset);
            }
            '日' if (1..=31).contains(&value) => {
                if let (Some(year), Some(month)) = (year, month) {
                    let date_start = year_start.or(month_start).unwrap_or(start_offset);
                    dates.push(ParsedJapaneseDate {
                        iso_date: format!("{year:04}-{month:02}-{value:02}"),
                        start_offset: date_start,
                        end_offset,
                    });
                }
            }
            _ => {}
        }

        index = end_index + 1;
    }

    dates
}

fn extract_item_label(raw: &str, dates: &[ParsedJapaneseDate]) -> String {
    let Some(first_date) = dates.first() else {
        return String::new();
    };
    normalize_free_text(
        raw[..first_date.start_offset]
            .trim()
            .trim_end_matches([':', '：'])
            .trim(),
    )
}

fn trim_leading_schedule_noise(raw: &str) -> String {
    let mut value = raw.trim();
    loop {
        let trimmed = value
            .trim_start_matches(|character: char| {
                character.is_whitespace()
                    || matches!(character, ':' | '：' | '・' | '、' | '-' | '－' | '—')
            })
            .trim_start();
        if trimmed.starts_with('(') || trimmed.starts_with('（') {
            let closing = if trimmed.starts_with('(') { ')' } else { '）' };
            if let Some(index) = trimmed.find(closing) {
                value = &trimmed[index + closing.len_utf8()..];
                continue;
            }
        }
        value = trimmed;
        break;
    }

    normalize_free_text(value)
}

fn is_schedule_only_fragment(raw: &str) -> bool {
    let mut cleaned = raw.to_string();
    for pattern in [
        "午前・午後",
        "午前",
        "午後",
        "各日",
        "来校型",
        "オンライン",
        "入替制",
        "祝",
    ] {
        cleaned = cleaned.replace(pattern, "");
    }

    cleaned
        .chars()
        .filter(|character| {
            !character.is_ascii_digit()
                && !character.is_whitespace()
                && !matches!(
                    character,
                    ':' | '：'
                        | '～'
                        | '〜'
                        | '-'
                        | '－'
                        | '–'
                        | '('
                        | ')'
                        | '（'
                        | '）'
                        | '・'
                        | '、'
                        | '【'
                        | '】'
                        | '<'
                        | '>'
                        | '＜'
                        | '＞'
                        | '/'
                )
        })
        .collect::<String>()
        .is_empty()
}

fn collect_hachioji_session_tables(document: &Html) -> Result<(u32, Vec<HachiojiSessionTable>)> {
    let page_title_selector = selector("h3[class*='c-pagetitle02']")?;
    let table_selector = selector("table.c-table02")?;
    let heading_selector = selector("thead th")?;
    let row_selector = selector("tbody tr")?;
    let label_selector = selector("th")?;
    let value_selector = selector("td")?;
    let link_selector = selector("a[href]")?;

    let academic_year = document
        .select(&page_title_selector)
        .map(extract_text)
        .map(|text| normalize_free_text(&text))
        .find_map(|text| extract_academic_year(&text))
        .context("hachioji_junior_session_tables_v1 could not find academic year heading")?;

    let mut tables = Vec::new();
    for table in document.select(&table_selector) {
        let Some(title_node) = table.select(&heading_selector).next() else {
            continue;
        };
        let title = normalize_free_text(&extract_text(title_node));
        if title.is_empty() {
            continue;
        }

        let mut summary = None;
        let mut schedule = None;
        let mut capacity = None;
        for row in table.select(&row_selector) {
            let Some(label_node) = row.select(&label_selector).next() else {
                continue;
            };
            let Some(value_node) = row.select(&value_selector).next() else {
                continue;
            };
            let label = normalize_free_text(&extract_text(label_node));
            let value = normalize_free_text(&extract_text(value_node));
            if value.is_empty() {
                continue;
            }

            match label.as_str() {
                "内容" => summary = Some(value),
                "実施" => schedule = Some(value),
                "定員" => capacity = Some(value),
                _ => {}
            }
        }

        let Some(schedule) = schedule else {
            continue;
        };
        let reservation_url = table
            .select(&link_selector)
            .find_map(|node| node.value().attr("href"))
            .map(str::trim)
            .filter(|href| !href.is_empty())
            .map(str::to_string);

        tables.push(HachiojiSessionTable {
            title,
            summary,
            schedule,
            capacity,
            reservation_url,
        });
    }

    Ok((academic_year, tables))
}

fn collect_nihon_junior_schedule_entries(
    document: &Html,
) -> Result<Vec<NihonUniversityJuniorScheduleEntry>> {
    let schedule_box_selector = selector("div.schedule_box")?;
    let month_selector = selector("h3.ttl")?;
    let dt_selector = selector("dl.text_box > dt")?;
    let dd_selector = selector("dl.text_box > dd")?;
    let title_selector = selector("p.event_name")?;
    let link_selector = selector("p.link_btn a[href]")?;

    let mut entries = Vec::new();
    for schedule_box in document.select(&schedule_box_selector) {
        let Some(month_node) = schedule_box.select(&month_selector).next() else {
            continue;
        };
        let month_label = normalize_free_text(&extract_text(month_node));
        let month_number = extract_first_number(&month_label).with_context(|| {
            format!(
                "nihon_university_junior_info_session_v1 could not parse month heading {month_label}"
            )
        })?;
        ensure!(
            (1..=12).contains(&month_number),
            "nihon_university_junior_info_session_v1 found unsupported month {month_number}"
        );

        let schedules = schedule_box
            .select(&dt_selector)
            .map(extract_joined_text)
            .map(|text| normalize_free_text(&text))
            .collect::<Vec<_>>();
        let details = schedule_box.select(&dd_selector).collect::<Vec<_>>();
        ensure!(
            schedules.len() == details.len(),
            "nihon_university_junior_info_session_v1 found mismatched dt/dd pairs for {month_label}"
        );

        for (raw_schedule, detail_node) in schedules.into_iter().zip(details) {
            let title = detail_node
                .select(&title_selector)
                .next()
                .map(extract_text)
                .map(|text| normalize_free_text(&text))
                .filter(|text| !text.is_empty())
                .with_context(|| {
                    format!(
                        "nihon_university_junior_info_session_v1 could not find event title for {} {}",
                        month_label, raw_schedule
                    )
                })?;

            let mut detail_url = None;
            let mut apply_url = None;
            let mut official_url = None;
            let mut extra_links = Vec::new();

            for link in detail_node.select(&link_selector) {
                let Some(href) = link.value().attr("href") else {
                    continue;
                };
                let label = normalize_free_text(&extract_text(link));
                if label.is_empty() {
                    continue;
                }
                let absolute_url = to_absolute_nihon_url(href);

                if label.contains("申込み") {
                    apply_url = Some(absolute_url);
                } else if label.contains("公式サイト") {
                    official_url = Some(absolute_url);
                } else if label.contains("詳細") && detail_url.is_none() {
                    detail_url = Some(absolute_url);
                } else {
                    extra_links.push((label, absolute_url));
                }
            }

            entries.push(NihonUniversityJuniorScheduleEntry {
                month_label: month_label.clone(),
                month_number,
                raw_schedule,
                title,
                detail_url,
                apply_url,
                official_url,
                extra_links,
            });
        }
    }

    Ok(entries)
}

fn extract_academic_year(raw: &str) -> Option<u32> {
    let before_marker = raw.split("年度").next()?;
    let digits = before_marker
        .chars()
        .filter(|character| character.is_ascii_digit())
        .collect::<String>();
    (digits.len() >= 4)
        .then_some(&digits[digits.len().saturating_sub(4)..])?
        .parse::<u32>()
        .ok()
}

fn extract_month_day_dates_for_academic_year(raw: &str, academic_year: u32) -> Vec<String> {
    let chars = raw.char_indices().collect::<Vec<_>>();
    let mut dates = Vec::new();
    let mut index = 0_usize;

    while index < chars.len() {
        let start_index = index;
        if !chars[index].1.is_ascii_digit() {
            index += 1;
            continue;
        }

        let mut month = 0_u32;
        while index < chars.len() && chars[index].1.is_ascii_digit() {
            month = month * 10 + chars[index].1.to_digit(10).unwrap_or(0);
            index += 1;
        }

        if index >= chars.len() || chars[index].1 != '/' {
            index = start_index + 1;
            continue;
        }
        index += 1;

        if index >= chars.len() || !chars[index].1.is_ascii_digit() {
            index = start_index + 1;
            continue;
        }

        let mut day = 0_u32;
        while index < chars.len() && chars[index].1.is_ascii_digit() {
            day = day * 10 + chars[index].1.to_digit(10).unwrap_or(0);
            index += 1;
        }

        if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            index = start_index + 1;
            continue;
        }

        let year = if month >= 4 {
            academic_year
        } else {
            academic_year + 1
        };
        let iso_date = format!("{year:04}-{month:02}-{day:02}");
        if !dates.contains(&iso_date) {
            dates.push(iso_date);
        }
    }

    dates
}

fn extract_first_number(raw: &str) -> Option<u32> {
    let digits = raw
        .chars()
        .skip_while(|character| !character.is_ascii_digit())
        .take_while(|character| character.is_ascii_digit())
        .collect::<String>();
    (!digits.is_empty()).then_some(digits)?.parse::<u32>().ok()
}

fn extract_ymd_from_url_path(raw: &str) -> Option<String> {
    let path = url::Url::parse(raw).ok()?.path().to_string();
    let chars = path.char_indices().collect::<Vec<_>>();
    let mut index = 0_usize;

    while index < chars.len() {
        if !chars[index].1.is_ascii_digit() {
            index += 1;
            continue;
        }

        let start = chars[index].0;
        let mut end_index = index;
        while end_index < chars.len() && chars[end_index].1.is_ascii_digit() {
            end_index += 1;
        }

        let end = if end_index < chars.len() {
            chars[end_index].0
        } else {
            path.len()
        };
        let digits = &path[start..end];
        if digits.len() >= 8 {
            let candidate = &digits[..8];
            let year = candidate[..4].parse::<u32>().ok()?;
            let month = candidate[4..6].parse::<u32>().ok()?;
            let day = candidate[6..8].parse::<u32>().ok()?;
            if (2000..=2100).contains(&year) && (1..=12).contains(&month) && (1..=31).contains(&day)
            {
                return Some(format!("{year:04}-{month:02}-{day:02}"));
            }
        }

        index = end_index + 1;
    }

    None
}

fn extract_first_day_from_schedule(raw: &str) -> Option<u32> {
    let chars = raw.char_indices().collect::<Vec<_>>();
    let mut index = 0_usize;

    while index < chars.len() {
        if !chars[index].1.is_ascii_digit() {
            index += 1;
            continue;
        }

        let start = chars[index].0;
        let mut end_index = index;
        while end_index < chars.len() && chars[end_index].1.is_ascii_digit() {
            end_index += 1;
        }
        if end_index >= chars.len() || chars[end_index].1 != '日' {
            index = end_index + 1;
            continue;
        }

        let end = chars[end_index].0;
        return raw[start..end].parse::<u32>().ok();
    }

    None
}

fn parse_bool(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        other => bail!("unsupported boolean value {other}"),
    }
}

fn parse_placement_tags(raw: &str) -> Result<Vec<PlacementKind>> {
    raw.split('|')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(parse_placement_kind)
        .collect()
}

fn parse_placement_kind(raw: &str) -> Result<PlacementKind> {
    match raw {
        "home" => Ok(PlacementKind::Home),
        "search" => Ok(PlacementKind::Search),
        "detail" => Ok(PlacementKind::Detail),
        "mypage" => Ok(PlacementKind::Mypage),
        other => bail!("unsupported placement tag {other}"),
    }
}

fn build_event_id(
    source_id: &str,
    school_id: &str,
    title: &str,
    starts_at: Option<&str>,
) -> String {
    let mut digest = Sha256::new();
    digest.update(source_id.as_bytes());
    digest.update([0]);
    digest.update(school_id.as_bytes());
    digest.update([0]);
    digest.update(normalize_key(title).as_bytes());
    digest.update([0]);
    digest.update(starts_at.unwrap_or("").as_bytes());
    let checksum = format!("{:x}", digest.finalize());
    format!("crawl_{}_{}", slugify(source_id), &checksum[..16])
}

fn normalize_key(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn slugify(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' => character.to_ascii_lowercase(),
            _ => '_',
        })
        .collect()
}

fn default_min_fetch_interval_ms() -> u64 {
    1_000
}

pub fn fixture_content_type(path: &Path) -> Option<&'static str> {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("json") => Some("application/json"),
        Some("html" | "htm") => Some("text/html"),
        _ => None,
    }
}

fn default_live_fetch_enabled() -> bool {
    true
}

fn default_event_category() -> String {
    "general".to_string()
}

fn list_yaml_paths(path: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    collect_yaml_paths(path, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn collect_yaml_paths(path: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_file() {
        if is_yaml_path(path) {
            paths.push(path.to_path_buf());
        }
        return Ok(());
    }

    for entry in fs::read_dir(path)
        .with_context(|| format!("failed to read crawl manifest dir {}", path.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to read entry under {}", path.display()))?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            collect_yaml_paths(&entry_path, paths)?;
        } else if is_yaml_path(&entry_path) {
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

#[cfg(test)]
mod tests {
    use super::{
        check_expected_shape, dedupe_events, finalize_parsed_events, lint_manifest_dir,
        lint_manifest_file, load_manifest, parse_placement_kind, ParseInput, ParsedEventSeed,
        ParserExpectedShape, ParserRegistry, ResolvedCrawlTarget, SourceMaturity,
        UTOKYO_EVENTS_JSON_LIMIT,
    };
    use domain::PlacementKind;

    fn fixture(name: &str) -> String {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../storage/fixtures/crawler")
            .join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read fixture {}: {error}", path.display()))
    }

    #[test]
    fn manifest_requires_school_id_from_target_or_defaults() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("crawler.yaml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: crawler_source
source_id: custom-example
source_name: Custom example
manifest_version: 1
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
targets:
  - logical_name: example_home
    url: https://example.com/
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("missing school_id");
        assert!(error.to_string().contains("school_id"));
    }

    #[test]
    fn manifest_requires_block_reason_when_live_fetch_is_disabled() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("crawler.yaml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: crawler_source
source_id: custom-example
source_name: Custom example
manifest_version: 1
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  live_fetch_enabled: false
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
targets:
  - logical_name: example_home
    url: https://example.com/
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("missing live fetch block reason");
        assert!(error.to_string().contains("live_fetch_block_reason"));
    }

    #[test]
    fn manifest_defaults_source_maturity_from_live_fetch_setting() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("crawler.yaml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: crawler_source
source_id: custom-example
source_name: Custom example
manifest_version: 1
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
targets:
  - logical_name: example_home
    url: https://example.com/
"#,
        )
        .expect("manifest");

        let manifest = load_manifest(&manifest_path).expect("loaded manifest");
        assert_eq!(
            manifest.effective_source_maturity(),
            SourceMaturity::LiveReady
        );
    }

    #[test]
    fn manifest_rejects_live_ready_when_live_fetch_is_disabled() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("crawler.yaml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: crawler_source
source_id: custom-example
source_name: Custom example
source_maturity: live_ready
manifest_version: 1
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  live_fetch_enabled: false
  live_fetch_block_reason: manual block
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
targets:
  - logical_name: example_home
    url: https://example.com/
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("invalid source_maturity");
        assert!(error.to_string().contains("source_maturity live_ready"));
    }

    #[test]
    fn manifest_rejects_duplicate_logical_names() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("crawler.yaml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: crawler_source
source_id: custom-example
source_name: Custom example
manifest_version: 1
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
targets:
  - logical_name: example_home
    url: https://example.com/first
  - logical_name: example_home
    url: https://example.com/second
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("duplicate logical_name");
        assert!(error.to_string().contains("duplicate logical_name"));
    }

    #[test]
    fn manifest_rejects_unknown_keys() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("crawler.yaml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: crawler_source
source_id: custom-example
source_name: Custom example
manifest_version: 1
parser_key: single_title_page_v1
unknown_key: true
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
targets:
  - logical_name: example_home
    url: https://example.com/
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("unknown key");
        assert!(format!("{error:#}").contains("unknown field `unknown_key`"));
    }

    #[test]
    fn manifest_rejects_missing_schema_contract() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("crawler.yaml");
        std::fs::write(
            &manifest_path,
            r#"
source_id: custom-example
source_name: Custom example
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
targets:
  - logical_name: example_home
    url: https://example.com/
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("missing schema contract");
        assert!(format!("{error:#}").contains("missing field `schema_version`"));
    }

    #[test]
    fn manifest_rejects_missing_kind() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("crawler.yaml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version: 1
source_id: custom-example
source_name: Custom example
manifest_version: 1
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
targets:
  - logical_name: example_home
    url: https://example.com/
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("missing kind");
        assert!(format!("{error:#}").contains("missing field `kind`"));
    }

    #[test]
    fn manifest_rejects_missing_manifest_version() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("crawler.yaml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: crawler_source
source_id: custom-example
source_name: Custom example
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
targets:
  - logical_name: example_home
    url: https://example.com/
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("missing manifest_version");
        assert!(format!("{error:#}").contains("missing field `manifest_version`"));
    }

    #[test]
    fn lints_crawl_manifest_dir_recursively() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_dir = temp.path().join("configs").join("crawler").join("sources");
        std::fs::create_dir_all(&manifest_dir).expect("manifest dir");
        std::fs::write(
            manifest_dir.join("custom.yaml"),
            r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: custom-example
source_name: Custom example
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
targets:
  - logical_name: example_home
    url: https://example.com/
"#,
        )
        .expect("manifest");

        let summary = lint_manifest_dir(temp.path().join("configs")).expect("lint");
        assert_eq!(summary.files.len(), 1);
        assert_eq!(summary.files[0].source_id, "custom-example");
        assert_eq!(summary.files[0].target_count, 1);
        assert_eq!(
            summary.files[0].expected_shape,
            Some(ParserExpectedShape::HtmlHeadingPage)
        );
    }

    #[test]
    fn lint_rejects_unknown_parser_key() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("custom.yaml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: custom-example
source_name: Custom example
parser_key: missing_parser_v1
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
targets:
  - logical_name: example_home
    url: https://example.com/
"#,
        )
        .expect("manifest");

        let error = lint_manifest_file(&manifest_path).expect_err("unknown parser");
        assert!(format!("{error:#}").contains("parser_key missing_parser_v1 is not registered"));
    }

    #[test]
    fn lint_rejects_expected_shape_mismatch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("custom.yaml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: custom-example
source_name: Custom example
parser_key: single_title_page_v1
expected_shape: json_feed
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
targets:
  - logical_name: example_home
    url: https://example.com/
"#,
        )
        .expect("manifest");

        let error = lint_manifest_file(&manifest_path).expect_err("shape mismatch");
        assert!(format!("{error:#}").contains("does not match parser"));
    }

    #[test]
    fn lint_rejects_fixture_path_outside_storage_fixtures() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        let manifest_dir = root.join("configs").join("crawler").join("sources");
        std::fs::create_dir_all(&manifest_dir).expect("manifest dir");
        std::fs::create_dir_all(root.join("storage").join("fixtures").join("crawler"))
            .expect("fixture root");
        std::fs::write(
            root.join("outside_fixture.html"),
            "<html><body><h1>Outside</h1></body></html>",
        )
        .expect("outside fixture");
        let manifest_path = manifest_dir.join("custom.yaml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: custom-example
source_name: Custom example
parser_key: single_title_page_v1
expected_shape: html_heading_page
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
targets:
  - logical_name: example_home
    url: https://example.com/
    fixture_path: ../../../outside_fixture.html
"#,
        )
        .expect("manifest");

        let error = lint_manifest_file(&manifest_path).expect_err("fixture outside root");
        assert!(format!("{error:#}").contains("outside allowed fixture root"));
    }

    #[test]
    fn manifest_rejects_windows_style_fixture_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("crawler.yaml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: crawler_source
manifest_version: 1
source_id: custom-example
source_name: Custom example
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
targets:
  - logical_name: example_home
    url: https://example.com/
    fixture_path: C:/fixtures/example.html
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("windows-style fixture path");
        assert!(format!("{error:#}").contains("portable POSIX relative syntax"));
    }

    #[test]
    fn expected_shape_check_flags_missing_monthly_dl_pairs() {
        let check = check_expected_shape(
            ParserExpectedShape::HtmlMonthlyDlPairs,
            "<html><body><p>not a schedule page</p></body></html>",
            Some("text/html"),
        );

        assert!(!check.matched);
        assert!(check.summary.contains("missing selectors"));
    }

    #[test]
    fn single_title_parser_extracts_heading() {
        let registry = ParserRegistry::default();
        let parser = registry
            .get("single_title_page_v1")
            .expect("single title parser");
        let target = ResolvedCrawlTarget {
            logical_name: "example_home".to_string(),
            url: "https://example.com/".to_string(),
            fixture_path: None,
            school_id: "school_seaside".to_string(),
            event_category: "open_campus".to_string(),
            is_open_day: true,
            is_featured: false,
            priority_weight: 0.0,
            placement_tags: vec![PlacementKind::Home],
        };

        let output = parser
            .parse(&ParseInput {
                source_id: "custom-example",
                logical_name: "example_home",
                target_url: "https://example.com/",
                html: "<html><body><h1>Example Domain Open Campus</h1><time datetime=\"2026-06-01T10:00:00+09:00\"></time></body></html>",
                target: &target,
            })
            .expect("parsed");

        assert_eq!(output.events.len(), 1);
        assert_eq!(output.events[0].title, "Example Domain Open Campus");
    }

    #[test]
    fn card_listing_parser_extracts_multiple_cards() {
        let registry = ParserRegistry::default();
        let parser = registry.get("card_listing_v1").expect("card parser");
        let target = ResolvedCrawlTarget {
            logical_name: "example_cards".to_string(),
            url: "https://example.com/events".to_string(),
            fixture_path: None,
            school_id: "school_default".to_string(),
            event_category: "open_campus".to_string(),
            is_open_day: true,
            is_featured: false,
            priority_weight: 0.0,
            placement_tags: vec![PlacementKind::Home],
        };

        let output = parser
            .parse(&ParseInput {
                source_id: "custom-example",
                logical_name: "example_cards",
                target_url: "https://example.com/events",
                html: r#"
<html>
  <body>
    <article data-crawl-event data-school-id="school_seaside" data-placement-tags="home|detail">
      <h2>Seaside Open Campus</h2>
      <time datetime="2026-07-01T10:00:00+09:00"></time>
    </article>
    <article class="crawl-event-card" data-school-id="school_garden" data-category="trial_class" data-open-day="false">
      <h3>Garden Trial Class</h3>
      <span data-placement-tag="search"></span>
    </article>
  </body>
</html>
"#,
                target: &target,
            })
            .expect("parsed");

        assert_eq!(output.events.len(), 2);
        assert_eq!(output.events[0].title, "Seaside Open Campus");
        assert_eq!(
            output.events[1].event_category.as_deref(),
            Some("trial_class")
        );
    }

    #[test]
    fn finalize_and_dedupe_events_keeps_one_record_per_event_id() {
        let target = ResolvedCrawlTarget {
            logical_name: "example_cards".to_string(),
            url: "https://example.com/events".to_string(),
            fixture_path: None,
            school_id: "school_seaside".to_string(),
            event_category: "open_campus".to_string(),
            is_open_day: true,
            is_featured: false,
            priority_weight: 0.0,
            placement_tags: vec![PlacementKind::Home],
        };
        let seeds = vec![
            ParsedEventSeed {
                title: "Seaside Open Campus".to_string(),
                starts_at: Some("2026-07-01T10:00:00+09:00".to_string()),
                school_id: None,
                event_category: None,
                is_open_day: None,
                is_featured: None,
                priority_weight: None,
                placement_tags: None,
                details: serde_json::json!({}),
            },
            ParsedEventSeed {
                title: "Seaside Open Campus".to_string(),
                starts_at: Some("2026-07-01T10:00:00+09:00".to_string()),
                school_id: None,
                event_category: None,
                is_open_day: None,
                is_featured: None,
                priority_weight: None,
                placement_tags: None,
                details: serde_json::json!({}),
            },
        ];

        let records = finalize_parsed_events(
            "custom-example",
            "example_cards",
            "https://example.com/events",
            &target,
            seeds,
        )
        .expect("records");
        let (deduped, reports) = dedupe_events(records);

        assert_eq!(deduped.len(), 1);
        assert_eq!(reports.len(), 1);
    }

    #[test]
    fn placement_kind_parser_matches_runtime_values() {
        assert_eq!(
            parse_placement_kind("mypage").expect("placement"),
            PlacementKind::Mypage
        );
    }

    #[test]
    fn utokyo_events_json_parser_extracts_newest_items() {
        let registry = ParserRegistry::default();
        let parser = registry
            .get("utokyo_events_json_v1")
            .expect("utokyo parser");
        let target = ResolvedCrawlTarget {
            logical_name: "focus_events_json".to_string(),
            url: "https://www.u-tokyo.ac.jp/focus/ja/events/events.json".to_string(),
            fixture_path: None,
            school_id: "school_utokyo".to_string(),
            event_category: "general".to_string(),
            is_open_day: false,
            is_featured: false,
            priority_weight: 0.0,
            placement_tags: vec![PlacementKind::Search],
        };
        let payload = serde_json::json!([
            {
                "pageTitle": "Past Event",
                "eventStartDate": "2024/01/15",
                "eventEndDate": "2024/01/15",
                "eventType": "講演会等",
                "eventTarget": "高校生",
                "eventArea": "本郷地区",
                "eventApp": "要事前申込",
                "busho": "Past Department",
                "url": "/focus/ja/events/past.html"
            },
            {
                "pageTitle": "Newest Event",
                "eventStartDate": "2026/05/01",
                "eventEndDate": "2026/05/02",
                "eventType": "説明会",
                "eventTarget": "受験生",
                "eventArea": "駒場地区",
                "eventApp": "自由参加",
                "busho": "Newest Department",
                "url": "https://www.u-tokyo.ac.jp/focus/ja/events/newest.html"
            },
            {
                "pageTitle": "Middle Event",
                "eventStartDate": "2026/04/01",
                "busho": "Middle Department",
                "url": "focus/ja/events/middle.html"
            }
        ])
        .to_string();

        let output = parser
            .parse(&ParseInput {
                source_id: "utokyo-events",
                logical_name: "focus_events_json",
                target_url: "https://www.u-tokyo.ac.jp/focus/ja/events/events.json",
                html: &payload,
                target: &target,
            })
            .expect("parsed");

        assert_eq!(output.events.len(), 3);
        assert_eq!(output.events[0].title, "Newest Event");
        assert_eq!(output.events[0].starts_at.as_deref(), Some("2026-05-01"));
        assert_eq!(
            output.events[0].details["detail_url"],
            "https://www.u-tokyo.ac.jp/focus/ja/events/newest.html"
        );
        assert_eq!(output.events[1].title, "Middle Event");
        assert_eq!(
            output.events[1].details["detail_url"],
            "https://www.u-tokyo.ac.jp/focus/ja/events/middle.html"
        );
        assert_eq!(output.events[2].title, "Past Event");
        assert_eq!(output.report_entries[0].parsed_rows, Some(3));
    }

    #[test]
    fn utokyo_events_json_parser_applies_limit_and_skips_invalid_rows() {
        let registry = ParserRegistry::default();
        let parser = registry
            .get("utokyo_events_json_v1")
            .expect("utokyo parser");
        let target = ResolvedCrawlTarget {
            logical_name: "focus_events_json".to_string(),
            url: "https://www.u-tokyo.ac.jp/focus/ja/events/events.json".to_string(),
            fixture_path: None,
            school_id: "school_utokyo".to_string(),
            event_category: "general".to_string(),
            is_open_day: false,
            is_featured: false,
            priority_weight: 0.0,
            placement_tags: vec![PlacementKind::Search],
        };

        let mut rows = vec![
            serde_json::json!({
                "pageTitle": "",
                "url": "/focus/ja/events/missing-title.html"
            }),
            serde_json::json!({
                "pageTitle": "Missing Url"
            }),
        ];
        rows.extend((0..(UTOKYO_EVENTS_JSON_LIMIT + 5)).map(|index| {
            let month = 1 + (index / 28);
            let day = 1 + (index % 28);
            serde_json::json!({
                "pageTitle": format!("Event {index:03}"),
                "eventStartDate": format!("2026/{month:02}/{day:02}"),
                "url": format!("/focus/ja/events/event-{index:03}.html")
            })
        }));
        let payload = serde_json::Value::Array(rows).to_string();

        let output = parser
            .parse(&ParseInput {
                source_id: "utokyo-events",
                logical_name: "focus_events_json",
                target_url: "https://www.u-tokyo.ac.jp/focus/ja/events/events.json",
                html: &payload,
                target: &target,
            })
            .expect("parsed");

        assert_eq!(output.events.len(), UTOKYO_EVENTS_JSON_LIMIT);
        assert_eq!(output.events[0].title, "Event 064");
        assert_eq!(output.events[0].starts_at.as_deref(), Some("2026-03-09"));
        assert_eq!(output.report_entries[0].details["dropped_missing_title"], 1);
        assert_eq!(output.report_entries[0].details["dropped_missing_url"], 1);
        assert_eq!(output.report_entries[0].details["input_rows"], 67);
    }

    #[test]
    fn keio_event_listing_parser_extracts_cards_and_ranges() {
        let registry = ParserRegistry::default();
        let parser = registry.get("keio_event_listing_v1").expect("keio parser");
        let target = ResolvedCrawlTarget {
            logical_name: "event_page_1".to_string(),
            url: "https://www.keio.ac.jp/ja/event/".to_string(),
            fixture_path: None,
            school_id: "school_keio".to_string(),
            event_category: "general".to_string(),
            is_open_day: false,
            is_featured: false,
            priority_weight: 0.0,
            placement_tags: vec![PlacementKind::Search],
        };
        let html = fixture("keio_event_listing_page_1.html");

        let output = parser
            .parse(&ParseInput {
                source_id: "keio-events",
                logical_name: "event_page_1",
                target_url: "https://www.keio.ac.jp/ja/event/",
                html: &html,
                target: &target,
            })
            .expect("parsed");

        assert_eq!(output.events.len(), 2);
        assert_eq!(
            output.events[0].title,
            "慶應義塾ミュージアム・コモンズ 展示"
        );
        assert_eq!(output.events[0].starts_at.as_deref(), Some("2026-03-09"));
        assert_eq!(
            output.events[0].details["event_end_date"],
            serde_json::Value::String("2026-05-15".to_string())
        );
        assert_eq!(
            output.events[0].details["detail_url"],
            "https://www.keio.ac.jp/ja/event/20260309-kemco-exhibition/"
        );
        assert_eq!(output.events[0].details["ongoing"], true);
        assert_eq!(output.events[1].title, "オープンキャンパス2026～講義編～");
        assert_eq!(output.events[1].starts_at.as_deref(), Some("2026-06-07"));
        assert_eq!(output.events[1].details["registration"], "※事前申込制");
        assert_eq!(output.report_entries[0].parsed_rows, Some(2));
    }

    #[test]
    fn aoyama_junior_school_tour_parser_extracts_internal_and_external_events() {
        let registry = ParserRegistry::default();
        let parser = registry
            .get("aoyama_junior_school_tour_v1")
            .expect("aoyama parser");
        let target = ResolvedCrawlTarget {
            logical_name: "school_tour_page".to_string(),
            url: "https://www.jh.aoyama.ed.jp/admission/explanation.html".to_string(),
            fixture_path: None,
            school_id: "school_aoyama_gakuin_junior".to_string(),
            event_category: "admission_event".to_string(),
            is_open_day: true,
            is_featured: false,
            priority_weight: 0.15,
            placement_tags: vec![PlacementKind::Search, PlacementKind::Detail],
        };
        let html = fixture("aoyama_junior_school_tour.html");

        let output = parser
            .parse(&ParseInput {
                source_id: "aoyama-junior-school-tour",
                logical_name: "school_tour_page",
                target_url: "https://www.jh.aoyama.ed.jp/admission/explanation.html",
                html: &html,
                target: &target,
            })
            .expect("parsed");

        assert_eq!(output.events.len(), 10);
        assert_eq!(output.events[0].title, "キリスト教学校合同フェア");
        assert_eq!(output.events[0].starts_at.as_deref(), Some("2026-03-20"));

        let internal_event = output
            .events
            .iter()
            .find(|event| {
                event.title == "学校説明会 第1回"
                    && event.starts_at.as_deref() == Some("2026-06-13")
            })
            .expect("internal school tour event");
        assert_eq!(internal_event.details["section_kind"], "school_tour");
        assert_eq!(internal_event.details["venue"], "青山学院講堂");
        assert_eq!(internal_event.details["sequence_label"], "第1回");
        assert_eq!(
            internal_event.details["detail_url"],
            "https://www.jh.aoyama.ed.jp/admission/explanation.html"
        );

        let tokyo_private_school_expo_dates = output
            .events
            .iter()
            .filter(|event| event.title == "東京都私立学校展 （※資料参加のみ）")
            .map(|event| event.starts_at.clone().expect("starts_at"))
            .collect::<Vec<_>>();
        assert_eq!(
            tokyo_private_school_expo_dates,
            vec!["2026-08-29".to_string(), "2026-08-30".to_string()]
        );
        assert_eq!(output.report_entries[0].parsed_rows, Some(10));
    }

    #[test]
    fn shibaura_junior_event_parser_extracts_sections_and_dates() {
        let registry = ParserRegistry::default();
        let parser = registry
            .get("shibaura_junior_event_page_v1")
            .expect("shibaura parser");
        let target = ResolvedCrawlTarget {
            logical_name: "junior_event_page".to_string(),
            url: "https://www.fzk.shibaura-it.ac.jp/admission/junior/event/".to_string(),
            fixture_path: None,
            school_id: "school_shibaura_it_junior".to_string(),
            event_category: "admission_event".to_string(),
            is_open_day: false,
            is_featured: false,
            priority_weight: 0.0,
            placement_tags: vec![PlacementKind::Search],
        };

        let html = r#"
<html>
  <body>
    <div class="qua-container">
      <h4 class="qua-wysiwyg-content"><p>本校の教育内容（学校説明会）【オンライン】※要予約</p></h4>
      <div class="qua-unit-text">
        <div class="qua-wysiwyg-content">
          <p>オンラインで実施する中学説明会</p>
          <p>※校長挨拶・教育内容・生徒インタビュー</p>
        </div>
      </div>
      <div class="qua-field-list">
        <ul>
          <li class="qua-field-list__item"><div class="qua-field-list__item__in"><p><strong>第1回：2026年5月9日 (土) 14：00～15：30</strong></p></div></li>
          <li class="qua-field-list__item"><div class="qua-field-list__item__in"><p><strong>第2回：2026年6月6日 (土) 10：30～12：00</strong></p></div></li>
        </ul>
      </div>
    </div>
    <div class="qua-container">
      <h4 class="qua-wysiwyg-content"><p>女子向けイベント＊要予約</p></h4>
      <div class="qua-unit-text">
        <div class="qua-wysiwyg-content">
          <p>理系のヒミツを発見！校長が語る未来へのヒント（校長特別講座）</p>
        </div>
      </div>
      <div class="qua-field-list">
        <ul>
          <li class="qua-field-list__item"><div class="qua-field-list__item__in"><p><strong>2026年5月30日 (土) 14：00-15：30</strong></p></div></li>
        </ul>
      </div>
    </div>
    <div class="qua-container">
      <h4 class="qua-wysiwyg-content"><p>教員による学校見学会＊要予約</p></h4>
      <div class="qua-field-list">
        <ul>
          <li class="qua-field-list__item"><div class="qua-field-list__item__in"><p><strong>＜5月＞：2026年5月11日 (月)、13日（水）、14日（木）</strong></p></div></li>
        </ul>
      </div>
    </div>
    <div class="qua-container">
      <h4 class="qua-wysiwyg-content"><p>部活動交流会＊要予約</p></h4>
      <div class="qua-field-list">
        <ul>
          <li class="qua-field-list__item"><div class="qua-field-list__item__in"><p><strong>日程：再調整中</strong></p></div></li>
        </ul>
      </div>
    </div>
  </body>
</html>
"#;

        let output = parser
            .parse(&ParseInput {
                source_id: "shibaura-junior-events",
                logical_name: "junior_event_page",
                target_url: "https://www.fzk.shibaura-it.ac.jp/admission/junior/event/",
                html,
                target: &target,
            })
            .expect("parsed");

        assert_eq!(output.events.len(), 6);
        assert_eq!(
            output.events[0].title,
            "オンラインで実施する中学説明会 第1回"
        );
        assert_eq!(output.events[0].starts_at.as_deref(), Some("2026-05-09"));
        assert_eq!(output.events[1].title, "教員による学校見学会 ＜5月＞");
        assert_eq!(output.events[3].starts_at.as_deref(), Some("2026-05-14"));
        assert_eq!(
            output.events[4].title,
            "理系のヒミツを発見！校長が語る未来へのヒント（校長特別講座）"
        );
        assert_eq!(output.events[5].starts_at.as_deref(), Some("2026-06-06"));
        assert_eq!(output.report_entries[0].parsed_rows, Some(6));
        assert_eq!(output.report_entries[0].details["skipped_items"], 1);
    }

    #[test]
    fn shibaura_junior_event_parser_prefers_item_title_for_external_events() {
        let registry = ParserRegistry::default();
        let parser = registry
            .get("shibaura_junior_event_page_v1")
            .expect("shibaura parser");
        let target = ResolvedCrawlTarget {
            logical_name: "junior_event_page".to_string(),
            url: "https://www.fzk.shibaura-it.ac.jp/admission/junior/event/".to_string(),
            fixture_path: None,
            school_id: "school_shibaura_it_junior".to_string(),
            event_category: "admission_event".to_string(),
            is_open_day: false,
            is_featured: false,
            priority_weight: 0.0,
            placement_tags: vec![PlacementKind::Search],
        };
        let html = r#"
<html>
  <body>
    <div class="qua-container">
      <h4 class="qua-wysiwyg-content"><p>学外イベント（合同説明会・相談会など）</p></h4>
      <div class="qua-field-list">
        <ul>
          <li class="qua-field-list__item"><div class="qua-field-list__item__in"><p><strong>2026年8月29日（土）東京私立中高協会主催：私学展（東京国際フォーラム）</strong></p></div></li>
        </ul>
      </div>
    </div>
  </body>
</html>
"#;

        let output = parser
            .parse(&ParseInput {
                source_id: "shibaura-junior-events",
                logical_name: "junior_event_page",
                target_url: "https://www.fzk.shibaura-it.ac.jp/admission/junior/event/",
                html,
                target: &target,
            })
            .expect("parsed");

        assert_eq!(output.events.len(), 1);
        assert_eq!(
            output.events[0].title,
            "東京私立中高協会主催：私学展（東京国際フォーラム）"
        );
        assert_eq!(output.events[0].starts_at.as_deref(), Some("2026-08-29"));
    }

    #[test]
    fn hachioji_junior_session_parser_expands_table_dates_and_rolls_year() {
        let registry = ParserRegistry::default();
        let parser = registry
            .get("hachioji_junior_session_tables_v1")
            .expect("hachioji parser");
        let target = ResolvedCrawlTarget {
            logical_name: "junior_session_page".to_string(),
            url: "https://www.hachioji.ed.jp/junr/exam/session/".to_string(),
            fixture_path: None,
            school_id: "school_hachioji_gakuen_junior".to_string(),
            event_category: "admission_event".to_string(),
            is_open_day: false,
            is_featured: false,
            priority_weight: 0.0,
            placement_tags: vec![PlacementKind::Search],
        };
        let html = fixture("hachioji_junior_session_tables.html");

        let output = parser
            .parse(&ParseInput {
                source_id: "hachioji-junior-events",
                logical_name: "junior_session_page",
                target_url: "https://www.hachioji.ed.jp/junr/exam/session/",
                html: &html,
                target: &target,
            })
            .expect("parsed");

        assert_eq!(output.events.len(), 5);
        assert_eq!(output.events[0].title, "保護者対象説明会");
        assert_eq!(output.events[0].starts_at.as_deref(), Some("2026-05-07"));
        assert_eq!(output.events[2].starts_at.as_deref(), Some("2026-09-26"));
        assert_eq!(output.events[3].starts_at.as_deref(), Some("2026-09-27"));
        assert_eq!(output.events[4].starts_at.as_deref(), Some("2027-01-09"));
        assert_eq!(
            output.events[0].details["reservation_url"],
            "https://mirai-compass.net/usr/hachiojj/event/evtIndex.jsf"
        );
        assert_eq!(
            output.report_entries[0].details["academic_year"],
            serde_json::Value::Number(2026.into())
        );
    }

    #[test]
    fn nihon_university_junior_parser_extracts_schedule_entries() {
        let registry = ParserRegistry::default();
        let parser = registry
            .get("nihon_university_junior_info_session_v1")
            .expect("nihon parser");
        let target = ResolvedCrawlTarget {
            logical_name: "junior_info_session_page".to_string(),
            url: "https://www.yokohama.hs.nihon-u.ac.jp/junior/info-session/".to_string(),
            fixture_path: None,
            school_id: "school_nihon_university_junior".to_string(),
            event_category: "admission_event".to_string(),
            is_open_day: false,
            is_featured: false,
            priority_weight: 0.0,
            placement_tags: vec![PlacementKind::Search],
        };
        let html = fixture("nihon_university_junior_info_session.html");

        let output = parser
            .parse(&ParseInput {
                source_id: "nihon-university-junior-events",
                logical_name: "junior_info_session_page",
                target_url: "https://www.yokohama.hs.nihon-u.ac.jp/junior/info-session/",
                html: &html,
                target: &target,
            })
            .expect("parsed");

        assert_eq!(output.events.len(), 8);
        assert_eq!(output.events[0].title, "外部フェア");
        assert_eq!(output.events[0].starts_at.as_deref(), Some("2025-07-11"));
        assert_eq!(output.events[1].title, "ミニ説明会");
        assert_eq!(output.events[1].starts_at.as_deref(), Some("2026-04-25"));
        assert_eq!(
            output.events[2].details["official_url"],
            "https://phsk.or.jp/soudankai2026/"
        );
        assert_eq!(output.events[0].details["date_source"], "detail_url");
        assert_eq!(output.events[3].title, "文化祭");
        assert_eq!(output.events[3].starts_at.as_deref(), Some("2026-09-12"));
        assert_eq!(
            output.events[3].details["raw_schedule"],
            "12日（土）9:00～15:45 13日（日）9:00～14:00"
        );
        assert_eq!(output.events[4].title, "学校（入試）説明会");
        assert_eq!(output.events[5].title, "学校（入試）説明会");
        assert_eq!(output.events[5].starts_at.as_deref(), Some("2026-11-14"));
        assert_eq!(output.events[6].starts_at.as_deref(), Some("2027-01-09"));
        assert_eq!(output.events[7].starts_at.as_deref(), Some("2027-01-16"));
        assert_eq!(
            output.events[7].details["apply_url"],
            "https://mirai-compass.net/usr/nihonuj/common/loginEvent.jsf"
        );
        assert_eq!(output.report_entries[0].parsed_rows, Some(8));
    }
}
