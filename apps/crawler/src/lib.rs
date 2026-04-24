use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{ensure, Context, Result};
use config::AppSettings;
use crawler_core::{
    check_expected_shape, dedupe_events, finalize_parsed_events, load_manifest, CrawlParser,
    CrawlSourceManifest, DedupeReportEntry, ParsedEventRecord, ParserExpectedShape, ParserRegistry,
    ResolvedCrawlTarget, SourceMaturity,
};
use generic_http::{
    ensure_allowed_url, evaluate_robots, fetch_robots_txt, fetch_to_raw, HttpFetchClient,
    HttpFetchRequest,
};
use serde_json::{json, Value};
use storage_postgres::{
    begin_crawl_run, claim_fetched_crawl_run, claim_latest_fetched_crawl_run, finish_crawl_run,
    import_crawled_events, latest_crawl_fetch_checksum, load_active_event_ids_for_source,
    load_crawl_fetch_logs, load_crawl_parse_errors, load_crawl_run_health,
    load_existing_school_ids, load_latest_fetched_crawl_run, mark_crawl_run_fetched,
    record_crawl_dedupe_report, record_crawl_fetch_log, record_crawl_parse_report,
    CrawlDedupeReportEntry, CrawlFetchLogEntry, CrawlParseErrorSnapshot, CrawlParseReportEntry,
    CrawlRunHealthSnapshot, EventCsvRecord, SourceManifestAudit, StoredCrawlFetchLog,
    StoredCrawlParseError,
};

const CRAWLER_CONTACT_URL: &str = "https://github.com/mt4110/geo-line-ranker";

fn discard_staged_fetch(path: &Path) {
    if let Err(error) = fs::remove_file(path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(path = %path.display(), %error, "failed to discard blocked staged fetch");
        }
    }
}

fn classify_fetch_error_status(error_message: &str) -> &'static str {
    const BLOCKED_POLICY_MARKERS: &[&str] = &[
        "outside the crawler allowlist",
        "private or local",
        "unsupported URL scheme",
        "response content-type",
        "response Content-Length",
        "response body",
        "redirect count exceeded",
        "max_response_bytes",
    ];

    if BLOCKED_POLICY_MARKERS
        .iter()
        .any(|marker| error_message.contains(marker))
    {
        "blocked_policy"
    } else {
        "fetch_failed"
    }
}

#[derive(Debug, Clone)]
pub struct CrawlCommandSummary {
    pub label: String,
    pub crawl_run_id: i64,
    pub fetched_targets: i64,
    pub parsed_rows: i64,
    pub imported_rows: i64,
    pub report_count: usize,
}

#[derive(Debug, Clone)]
pub struct ParserHealthSummary {
    pub manifest_path: String,
    pub source_id: String,
    pub source_name: String,
    pub source_maturity: SourceMaturity,
    pub parser_key: String,
    pub parser_version: String,
    pub expected_shape: Option<ParserExpectedShape>,
    pub total_runs: i64,
    pub shown_runs: usize,
    pub succeeded_runs: usize,
    pub failed_runs: usize,
    pub active_runs: usize,
    pub fetch_status_totals: BTreeMap<String, i64>,
    pub parse_level_totals: BTreeMap<String, i64>,
    pub dedupe_report_total: i64,
    pub recent_runs: Vec<CrawlRunHealthSnapshot>,
    pub recent_reason_trend: Vec<RunReasonTrend>,
    pub logical_name_red_flags: Vec<LogicalNameRedFlag>,
    pub healthy_logical_name_count: usize,
    pub reason_totals: BTreeMap<String, i64>,
}

#[derive(Debug, Clone)]
pub struct LogicalNameRedFlag {
    pub logical_name: String,
    pub reasons: Vec<String>,
    pub latest_fetch_status: Option<String>,
    pub observed_runs: usize,
    pub successful_runs: usize,
    pub red_runs: usize,
    pub consecutive_red_runs: usize,
    pub latest_error: Option<CrawlParseErrorSnapshot>,
}

#[derive(Debug, Clone, Default)]
struct LogicalNameRunSignal {
    fetch_logs: Vec<StoredCrawlFetchLog>,
    parse_errors: Vec<StoredCrawlParseError>,
}

#[derive(Debug, Clone)]
pub struct CrawlDoctorSummary {
    pub manifest_path: String,
    pub source_id: String,
    pub source_name: String,
    pub source_maturity: SourceMaturity,
    pub parser_key: String,
    pub parser_registered: bool,
    pub expected_shape: Option<ParserExpectedShape>,
    pub live_fetch_enabled: bool,
    pub robots: UrlProbeSummary,
    pub terms: UrlProbeSummary,
    pub targets: Vec<DoctorTargetSummary>,
    pub issues: Vec<DiagnosticIssue>,
}

#[derive(Debug, Clone)]
pub struct UrlProbeSummary {
    pub requested_url: String,
    pub final_url: Option<String>,
    pub http_status: Option<u16>,
    pub content_type: Option<String>,
    pub error: Option<String>,
    pub body: Option<String>,
    pub body_preview: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DoctorTargetSummary {
    pub logical_name: String,
    pub target_url: String,
    pub school_id: String,
    pub school_exists: Option<bool>,
    pub robots_allowed: Option<bool>,
    pub matched_rule: Option<String>,
    pub expected_shape: Option<ParserExpectedShape>,
    pub shape_status: Option<String>,
    pub shape_detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DiagnosticIssue {
    pub level: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct CrawlDryRunSummary {
    pub manifest_path: String,
    pub source_id: String,
    pub source_name: String,
    pub source_maturity: SourceMaturity,
    pub parser_key: String,
    pub parser_version: String,
    pub expected_shape: Option<ParserExpectedShape>,
    pub crawl_run_id: i64,
    pub ready_targets: usize,
    pub parsed_rows: i64,
    pub deduped_rows: i64,
    pub imported_rows: i64,
    pub deactivated_rows: i64,
    pub missing_school_rows: i64,
    pub date_drift_warnings: usize,
    pub parse_errors: Vec<DiagnosticIssue>,
    pub warnings: Vec<DiagnosticIssue>,
    pub logical_name_summaries: Vec<LogicalDryRunSummary>,
}

#[derive(Debug, Clone)]
pub struct LogicalDryRunSummary {
    pub logical_name: String,
    pub parsed_rows: i64,
    pub date_drift_warnings: usize,
    pub parse_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RunReasonTrend {
    pub crawl_run_id: i64,
    pub status: String,
    pub reasons: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PromotionGate {
    blockers: BTreeSet<String>,
    review: BTreeSet<String>,
}

impl PromotionGate {
    fn status(&self) -> &'static str {
        if !self.blockers.is_empty() {
            "blocked"
        } else if !self.review.is_empty() {
            "review"
        } else {
            "ready"
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ResolvedManifestMetadata {
    source_maturity: SourceMaturity,
    expected_shape: Option<ParserExpectedShape>,
}

#[derive(Debug, Clone)]
pub struct ScaffoldDomainRequest {
    pub source_id: String,
    pub source_name: String,
    pub school_id: String,
    pub parser_key: String,
    pub source_maturity: SourceMaturity,
    pub expected_shape: ParserExpectedShape,
    pub target_url: String,
    pub logical_name: Option<String>,
    pub manifest_dir: PathBuf,
    pub fixture_dir: PathBuf,
    pub guide_dir: PathBuf,
    pub force: bool,
}

#[derive(Debug, Clone)]
pub struct ScaffoldDomainSummary {
    pub manifest_path: String,
    pub fixture_path: String,
    pub guide_path: String,
    pub source_maturity: SourceMaturity,
    pub expected_shape: ParserExpectedShape,
}

fn resolve_manifest_metadata(
    manifest: &CrawlSourceManifest,
    parser: Option<&dyn CrawlParser>,
) -> Result<ResolvedManifestMetadata> {
    if let (Some(manifest_shape), Some(parser)) = (manifest.expected_shape, parser) {
        ensure!(
            manifest_shape == parser.expected_shape(),
            "manifest expected_shape {} does not match parser {} expected_shape {}",
            manifest_shape,
            parser.key(),
            parser.expected_shape()
        );
    }

    Ok(ResolvedManifestMetadata {
        source_maturity: manifest.effective_source_maturity(),
        expected_shape: manifest.effective_expected_shape(parser),
    })
}

pub fn scaffold_domain(request: ScaffoldDomainRequest) -> Result<ScaffoldDomainSummary> {
    ensure!(
        !request.source_id.trim().is_empty(),
        "source_id must not be empty"
    );
    ensure!(
        !request.source_name.trim().is_empty(),
        "source_name must not be empty"
    );
    ensure!(
        !request.school_id.trim().is_empty(),
        "school_id must not be empty"
    );
    ensure!(
        !request.parser_key.trim().is_empty(),
        "parser_key must not be empty"
    );

    let parsed_target = reqwest::Url::parse(&request.target_url)
        .with_context(|| format!("failed to parse target_url {}", request.target_url))?;
    let host = parsed_target
        .host_str()
        .with_context(|| format!("target_url {} is missing a host", request.target_url))?;
    let preset = infer_scaffold_template_preset(&request, host, &parsed_target);
    let logical_name = preset.logical_name.clone();
    let fixture_stub = request.source_id.replace('-', "_");
    let fixture_name = format!(
        "{}.{}",
        fixture_stub,
        request.expected_shape.fixture_extension()
    );
    let manifest_path = request
        .manifest_dir
        .join(format!("{}.yaml", request.source_id));
    let fixture_path = request.fixture_dir.join(&fixture_name);
    let guide_path = request.guide_dir.join(format!("{}.md", request.source_id));

    fs::create_dir_all(&request.manifest_dir).with_context(|| {
        format!(
            "failed to create manifest dir {}",
            request.manifest_dir.display()
        )
    })?;
    fs::create_dir_all(&request.fixture_dir).with_context(|| {
        format!(
            "failed to create fixture dir {}",
            request.fixture_dir.display()
        )
    })?;
    fs::create_dir_all(&request.guide_dir)
        .with_context(|| format!("failed to create guide dir {}", request.guide_dir.display()))?;

    write_template_file(
        &manifest_path,
        &build_scaffold_manifest(&request, host, &parsed_target, &fixture_name),
        request.force,
    )?;
    write_template_file(
        &fixture_path,
        &build_scaffold_fixture(&request, &logical_name),
        request.force,
    )?;
    write_template_file(
        &guide_path,
        &build_scaffold_guide(&request, &logical_name, &manifest_path, &fixture_path),
        request.force,
    )?;

    Ok(ScaffoldDomainSummary {
        manifest_path: manifest_path.display().to_string(),
        fixture_path: fixture_path.display().to_string(),
        guide_path: guide_path.display().to_string(),
        source_maturity: request.source_maturity,
        expected_shape: request.expected_shape,
    })
}

fn write_template_file(path: &Path, contents: &str, force: bool) -> Result<()> {
    if path.exists() && !force {
        anyhow::bail!(
            "refusing to overwrite existing file {}; rerun with --force to replace it",
            path.display()
        );
    }
    fs::write(path, contents)
        .with_context(|| format!("failed to write template file {}", path.display()))
}

fn default_logical_name(expected_shape: ParserExpectedShape) -> &'static str {
    match expected_shape {
        ParserExpectedShape::JsonFeed => "primary_feed",
        _ => "primary_page",
    }
}

#[derive(Debug, Clone)]
struct ScaffoldTemplatePreset {
    logical_name: String,
    logical_name_reason: String,
    event_category: &'static str,
    event_category_reason: String,
    is_open_day: bool,
    is_open_day_reason: String,
    priority_weight: f64,
    priority_weight_reason: String,
    terms_url: String,
    terms_note: String,
    description: String,
}

fn infer_scaffold_template_preset(
    request: &ScaffoldDomainRequest,
    host: &str,
    parsed_target: &reqwest::Url,
) -> ScaffoldTemplatePreset {
    let ascii_haystack = format!(
        "{} {}",
        request.source_name.to_ascii_lowercase(),
        request.target_url.to_ascii_lowercase()
    );
    let raw_haystack = format!("{} {}", request.source_name, request.target_url);
    let admission_like = contains_any(
        &ascii_haystack,
        &[
            "admission",
            "open campus",
            "open-campus",
            "school tour",
            "info session",
            "info-session",
            "session",
            "briefing",
            "entrance",
            "explanation",
            "guidance",
        ],
    ) || contains_any(
        &raw_haystack,
        &[
            "説明会",
            "学校見学",
            "学校紹介",
            "入試",
            "受験",
            "オープンキャンパス",
        ],
    );
    let open_day_like =
        matches!(
            request.expected_shape,
            ParserExpectedShape::HtmlSchoolTourBlocks
        ) || contains_any(
            &ascii_haystack,
            &[
                "open campus",
                "open-campus",
                "open day",
                "open-day",
                "school tour",
                "tour",
                "visit",
            ],
        ) || contains_any(&raw_haystack, &["見学", "学校説明会", "オープンキャンパス"]);
    let path_stem = infer_path_stem(parsed_target);
    let logical_name = request
        .logical_name
        .clone()
        .unwrap_or_else(|| infer_logical_name(request.expected_shape, &path_stem, admission_like));
    let logical_name_reason = if request.logical_name.is_some() {
        "provided explicitly via --logical-name".to_string()
    } else {
        format!(
            "inferred from expected_shape={} and target path `{}`",
            request.expected_shape,
            parsed_target.path()
        )
    };
    let (event_category, event_category_reason) = if admission_like {
        (
            "admission_event",
            "source_name / target_url look admissions-oriented, so the scaffold starts in the common school-event lane".to_string(),
        )
    } else {
        (
            "general",
            "no admissions-specific signal was detected, so the scaffold stays conservative"
                .to_string(),
        )
    };
    let (is_open_day, is_open_day_reason) = if open_day_like {
        (
            true,
            "school-tour / open-campus style wording was detected, so open-day defaults start enabled".to_string(),
        )
    } else {
        (
            false,
            "the scaffold did not detect an open-day signal, so this stays off until the source proves otherwise".to_string(),
        )
    };
    let (priority_weight, priority_weight_reason) = if admission_like || open_day_like {
        (
            0.15,
            "admissions-oriented sources usually deserve a slightly higher default weight in search/detail placements".to_string(),
        )
    } else {
        (
            0.1,
            "general event sources start at the baseline weight".to_string(),
        )
    };
    let terms_url = format!("{}://{host}/", parsed_target.scheme());
    let terms_note = if request.source_maturity == SourceMaturity::LiveReady {
        "Temporary placeholder generated by scaffold-domain. Replace `terms_url` with the real privacy or site-policy page and record the manual review result before keeping this source live-ready.".to_string()
    } else {
        "Temporary placeholder generated by scaffold-domain. Replace `terms_url` with the real privacy or site-policy page and record the manual review result before promotion.".to_string()
    };
    let description = format!(
        "Scaffolded allowlist crawler manifest for {}.",
        request.source_name
    );

    ScaffoldTemplatePreset {
        logical_name,
        logical_name_reason,
        event_category,
        event_category_reason,
        is_open_day,
        is_open_day_reason,
        priority_weight,
        priority_weight_reason,
        terms_url,
        terms_note,
        description,
    }
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

fn infer_path_stem(parsed_target: &reqwest::Url) -> String {
    let segment = parsed_target
        .path_segments()
        .and_then(|mut segments| segments.rfind(|segment| !segment.is_empty()))
        .unwrap_or_default();
    let segment = segment
        .split('.')
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or(segment);
    let snake = to_snake_identifier(segment);
    if snake.is_empty() {
        "primary".to_string()
    } else {
        snake
    }
}

fn to_snake_identifier(raw: &str) -> String {
    let mut output = String::new();
    let mut previous_was_separator = false;

    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_lowercase());
            previous_was_separator = false;
        } else if !output.is_empty() && !previous_was_separator {
            output.push('_');
            previous_was_separator = true;
        }
    }

    output.trim_matches('_').to_string()
}

fn with_suffix(base: &str, suffix: &str) -> String {
    if base.ends_with(suffix) {
        base.to_string()
    } else {
        format!("{base}{suffix}")
    }
}

fn infer_logical_name(
    expected_shape: ParserExpectedShape,
    path_stem: &str,
    admission_like: bool,
) -> String {
    match expected_shape {
        ParserExpectedShape::JsonFeed => {
            if path_stem == "primary" {
                "primary_feed".to_string()
            } else {
                with_suffix(path_stem, "_feed")
            }
        }
        ParserExpectedShape::HtmlSchoolTourBlocks => "school_tour_page".to_string(),
        ParserExpectedShape::HtmlCardListing | ParserExpectedShape::HtmlKeioEventCards => {
            if path_stem.contains("event") {
                with_suffix(path_stem, "_page")
            } else {
                "event_listing_page".to_string()
            }
        }
        ParserExpectedShape::HtmlQuaSections => {
            if path_stem.contains("junior") {
                "junior_event_page".to_string()
            } else if admission_like {
                "admission_event_page".to_string()
            } else {
                "event_page".to_string()
            }
        }
        ParserExpectedShape::HtmlSessionTables => {
            if admission_like {
                "session_schedule_page".to_string()
            } else if path_stem == "primary" {
                "schedule_page".to_string()
            } else {
                with_suffix(path_stem, "_page")
            }
        }
        ParserExpectedShape::HtmlMonthlyDlPairs => {
            if path_stem.contains("info") || path_stem.contains("session") {
                "info_session_page".to_string()
            } else if admission_like {
                "admission_schedule_page".to_string()
            } else if path_stem == "primary" {
                "monthly_schedule_page".to_string()
            } else {
                with_suffix(path_stem, "_page")
            }
        }
        ParserExpectedShape::HtmlHeadingPage => {
            if path_stem == "primary" || path_stem == "index" || path_stem == "home" {
                default_logical_name(expected_shape).to_string()
            } else {
                with_suffix(path_stem, "_page")
            }
        }
    }
}

fn format_priority_weight(value: f64) -> String {
    let raw = format!("{value:.2}");
    raw.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn shape_contract_lines(expected_shape: ParserExpectedShape) -> &'static [&'static str] {
    match expected_shape {
        ParserExpectedShape::HtmlHeadingPage => &[
            "Keep one visible heading node such as `h1` or `title` and one dated value.",
            "Trim away unrelated layout chrome; the parser only needs a stable heading and date source.",
        ],
        ParserExpectedShape::HtmlCardListing => &[
            "Keep one or more `data-crawl-event` / `.crawl-event-card` blocks with title and optional metadata.",
            "If placement or school overrides matter, keep those `data-*` attributes in the fixture.",
        ],
        ParserExpectedShape::HtmlKeioEventCards => &[
            "Keep the card anchor, title node, and the separate year / month / day fragments.",
            "Include at least one row with optional venue or registration text if the parser uses those fields.",
        ],
        ParserExpectedShape::HtmlSchoolTourBlocks => &[
            "Keep `section.explan1` with a `dl` of `dt/dd` pairs and a `場 所` table.",
            "Keep `section.explan3 > .tbody > .table` rows with date/time, title link, venue, and organizer cells.",
            "Preserve real Japanese date text such as `8月 29・30日` when the parser expands multi-day rows.",
        ],
        ParserExpectedShape::JsonFeed => &[
            "Keep the smallest valid JSON array/object that still contains the parser's required keys.",
            "Use real key names from production and delete every field the parser does not read.",
        ],
        ParserExpectedShape::HtmlQuaSections => &[
            "Keep each `div.qua-container` block with its heading, summary text, and list items.",
            "Include at least one item with multiple dates if the parser expands a single bullet into several events.",
        ],
        ParserExpectedShape::HtmlSessionTables => &[
            "Keep the academic-year heading and one representative `table.c-table02` schedule block.",
            "Use month/day text exactly as the live page writes it so year-rollover logic stays testable.",
        ],
        ParserExpectedShape::HtmlMonthlyDlPairs => &[
            "Keep `h3.ttl + dl.text_box` pairs with matching `dt`/`dd` rows.",
            "If the parser prefers detail/apply links, keep those links as separate anchors in the fixture.",
        ],
    }
}

fn fixture_rule_lines(expected_shape: ParserExpectedShape) -> &'static [&'static str] {
    match expected_shape {
        ParserExpectedShape::JsonFeed => &[
            "Prefer one to three rows. Enough to prove ordering or field fallback is better than dumping a full feed.",
            "Freeze dates and URLs so tests stay deterministic.",
        ],
        ParserExpectedShape::HtmlSchoolTourBlocks => &[
            "Keep one internal row and one external row at minimum; add a multi-day row if the parser expands it.",
            "Leave only the selectors the parser needs so fixture drift becomes obvious during review.",
        ],
        _ => &[
            "Trim the fixture until every remaining node is there for a parser reason.",
            "Keep one happy-path row plus one row that proves the edge the parser must preserve.",
        ],
    }
}

fn build_scaffold_manifest(
    request: &ScaffoldDomainRequest,
    host: &str,
    parsed_target: &reqwest::Url,
    fixture_name: &str,
) -> String {
    let preset = infer_scaffold_template_preset(request, host, parsed_target);
    let live_fetch_enabled = request.source_maturity == SourceMaturity::LiveReady;
    let block_reason = if live_fetch_enabled {
        String::new()
    } else {
        "  live_fetch_enabled: false\n  live_fetch_block_reason: TODO: keep this source blocked until robots/terms/manual review are complete.\n".to_string()
    };

    format!(
        "# Generated by `crawler scaffold-domain`.\n# Replace the temporary policy placeholder values before you promote this source.\nsource_id: {source_id}\nsource_name: {source_name}\nsource_maturity: {source_maturity}\nparser_key: {parser_key}\nexpected_shape: {expected_shape}\ndescription: {description}\nallowlist:\n  allowed_domains:\n    - {host}\n  user_agent: geo-line-ranker-crawler/0.1 (+{crawler_contact_url})\n  min_fetch_interval_ms: 1000\n{block_reason}  robots_txt_url: {scheme}://{host}/robots.txt\n  terms_url: {terms_url}\n  terms_note: {terms_note}\ndefaults:\n  school_id: {school_id}\n  event_category: {event_category}\n  is_open_day: {is_open_day}\n  is_featured: false\n  priority_weight: {priority_weight}\n  placement_tags:\n    - search\n    - detail\ntargets:\n  - logical_name: {logical_name}\n    url: {target_url}\n\n# Fixture seed: storage/fixtures/crawler/{fixture_name}\n",
        crawler_contact_url = CRAWLER_CONTACT_URL,
        source_id = request.source_id,
        source_name = request.source_name,
        source_maturity = request.source_maturity,
        parser_key = request.parser_key,
        expected_shape = request.expected_shape,
        description = preset.description,
        host = host,
        scheme = parsed_target.scheme(),
        block_reason = block_reason,
        terms_url = preset.terms_url,
        terms_note = preset.terms_note,
        school_id = request.school_id,
        event_category = preset.event_category,
        is_open_day = preset.is_open_day,
        priority_weight = format_priority_weight(preset.priority_weight),
        logical_name = preset.logical_name,
        target_url = request.target_url,
        fixture_name = fixture_name
    )
}

fn build_scaffold_fixture(request: &ScaffoldDomainRequest, logical_name: &str) -> String {
    match request.expected_shape {
        ParserExpectedShape::HtmlHeadingPage => format!(
            "<!-- Trim this fixture to the smallest heading/date pair the parser actually needs. -->\n<html>\n  <body>\n    <h1>{}</h1>\n    <time datetime=\"2026-09-01T10:00:00+09:00\"></time>\n  </body>\n</html>\n",
            request.source_name
        ),
        ParserExpectedShape::HtmlCardListing => format!(
            "<!-- Keep only the card attributes and child nodes the parser reads. -->\n<html>\n  <body>\n    <article data-crawl-event data-school-id=\"{}\" data-category=\"general\">\n      <h2>{}</h2>\n      <time datetime=\"2026-09-01\"></time>\n    </article>\n  </body>\n</html>\n",
            request.school_id, request.source_name
        ),
        ParserExpectedShape::HtmlKeioEventCards => format!(
            "<!-- Keep the split year / month / day fragments because the parser depends on them. -->\n<html>\n  <body>\n    <a href=\"{}\">\n      <h2>{}</h2>\n      <div class=\"sample_cardEventDate_01\"><span class=\"sample_year_01\">2026</span><span class=\"sample_dot_01\">9</span><span class=\"sample_day_01\">1</span></div>\n    </a>\n  </body>\n</html>\n",
            request.target_url, request.source_name
        ),
        ParserExpectedShape::HtmlSchoolTourBlocks => format!(
            "<!-- Keep one internal row, one external row, and add a multi-day row once the parser expands it. -->\n<html>\n  <body>\n    <section class=\"explan1\">\n      <div class=\"table\">\n        <div class=\"cell th\">日 時</div>\n        <div class=\"cell td\">\n          <dl>\n            <dt><span>第1回</span></dt>\n            <dd>\n              <div class=\"date\">2026年 9月 1日(火)</div>\n              <div class=\"time\">10:00 - 11:00</div>\n            </dd>\n          </dl>\n        </div>\n      </div>\n      <div class=\"table\">\n        <div class=\"cell th\">場 所</div>\n        <div class=\"cell td\"><p>{}</p></div>\n      </div>\n    </section>\n    <section class=\"explan3\">\n      <div class=\"tbody\">\n        <div class=\"table\">\n          <div class=\"cell th\">\n            <div class=\"date\">2026年 9月 20日(日)</div>\n            <div class=\"time\">9:30 - 16:00</div>\n          </div>\n          <div class=\"cell td\"><a href=\"{}\">外部説明会</a></div>\n          <div class=\"cell td\">{} 会場</div>\n          <div class=\"cell td\">主催者</div>\n        </div>\n        <div class=\"table\">\n          <div class=\"cell th\">\n            <div class=\"date\">2026年 9月 27・28日(日・月)</div>\n            <div class=\"time\">10:00 - 15:00</div>\n          </div>\n          <div class=\"cell td\"><a href=\"{}\">合同相談会</a></div>\n          <div class=\"cell td\">{} サテライト会場</div>\n          <div class=\"cell td\">共催団体</div>\n        </div>\n      </div>\n    </section>\n  </body>\n</html>\n",
            request.source_name,
            request.target_url,
            logical_name,
            request.target_url,
            logical_name
        ),
        ParserExpectedShape::JsonFeed => format!(
            "[\n  {{\n    \"pageTitle\": \"{}\",\n    \"eventStartDate\": \"2026/09/01\",\n    \"url\": \"{}\"\n  }}\n]\n",
            request.source_name, request.target_url
        ),
        ParserExpectedShape::HtmlQuaSections => format!(
            "<!-- Keep each qua-container as one parser section. -->\n<html>\n  <body>\n    <div class=\"qua-container\">\n      <h4 class=\"qua-wysiwyg-content\"><p>{}</p></h4>\n      <div class=\"qua-unit-text\"><div class=\"qua-wysiwyg-content\"><p>Fixture for {}</p></div></div>\n      <div class=\"qua-field-list\">\n        <ul>\n          <li class=\"qua-field-list__item\"><div class=\"qua-field-list__item__in\"><p><strong>2026年9月1日 (火) 10：00～11：00</strong></p></div></li>\n        </ul>\n      </div>\n    </div>\n  </body>\n</html>\n",
            request.source_name, logical_name
        ),
        ParserExpectedShape::HtmlSessionTables => format!(
            "<!-- Preserve the academic-year heading and one representative schedule table. -->\n<html>\n  <body>\n    <h3 class=\"c-pagetitle02\">2026年度 {}</h3>\n    <table class=\"c-table02\">\n      <thead><tr><th>イベント</th><th>内容</th></tr></thead>\n      <tbody>\n        <tr>\n          <th>{}</th>\n          <td>9/1(火) 10:00-11:00</td>\n        </tr>\n      </tbody>\n    </table>\n  </body>\n</html>\n",
            request.source_name, request.source_name
        ),
        ParserExpectedShape::HtmlMonthlyDlPairs => format!(
            "<!-- Keep matching h3.ttl + dl.text_box pairs and only the anchors the parser reads. -->\n<html>\n  <body>\n    <div class=\"schedule_box\">\n      <h3 class=\"ttl\">9月</h3>\n      <dl class=\"text_box\">\n        <dt>1日（火）10:00〜11:00</dt>\n        <dd>\n          <p class=\"event_name\">{}</p>\n          <p class=\"link_btn\"><a href=\"{}\">詳細</a></p>\n        </dd>\n      </dl>\n    </div>\n  </body>\n</html>\n",
            request.source_name, request.target_url
        ),
    }
}

fn build_scaffold_guide(
    request: &ScaffoldDomainRequest,
    logical_name: &str,
    manifest_path: &Path,
    fixture_path: &Path,
) -> String {
    let host = reqwest::Url::parse(&request.target_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| "example.com".to_string());
    let parsed_target = reqwest::Url::parse(&request.target_url)
        .ok()
        .unwrap_or_else(|| {
            reqwest::Url::parse("https://example.com/").expect("valid fallback url")
        });
    let preset = infer_scaffold_template_preset(request, &host, &parsed_target);
    let shape_contract = shape_contract_lines(request.expected_shape)
        .iter()
        .map(|line| format!("- {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    let fixture_rules = fixture_rule_lines(request.expected_shape)
        .iter()
        .map(|line| format!("- {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    let live_ready_line = if request.source_maturity == SourceMaturity::LiveReady {
        "- This scaffold is already marked `live_ready`, so replace the temporary `terms_url` placeholder before the first real fetch.".to_string()
    } else {
        format!(
            "- Keep `source_maturity={}` until robots / terms / fixture-backed parser checks are genuinely clean.",
            request.source_maturity
        )
    };

    format!(
        "# Crawler Scaffold: {source_name}\n\n## Snapshot\n\n- source_id: `{source_id}`\n- source_maturity: `{source_maturity}`\n- parser_key: `{parser_key}`\n- expected_shape: `{expected_shape}`\n- school_id: `{school_id}`\n- logical_name: `{logical_name}`\n- target_url: `{target_url}`\n- manifest: `{manifest_path}`\n- fixture: `{fixture_path}`\n\n## Generated Defaults\n\n- `logical_name={logical_name}`: {logical_name_reason}\n- `event_category={event_category}`: {event_category_reason}\n- `is_open_day={is_open_day}`: {is_open_day_reason}\n- `priority_weight={priority_weight}`: {priority_weight_reason}\n- `terms_url={terms_url}` starts as a temporary placeholder so the manifest stays runnable while policy review catches up.\n\n## Edit In This Order\n\n1. Confirm the parser key or replace the placeholder parser wiring.\n2. Replace `terms_url` with the real privacy / site-policy page and write a concrete `terms_note`.\n3. Trim the fixture to the smallest real snippet that still satisfies `{expected_shape}`.\n4. Add or update fixture-backed tests in `crates/crawler-core/src/lib.rs` and `apps/crawler/src/lib.rs`.\n5. Run `doctor`, `dry-run`, and `health`, then promote the source only when the checks are quiet.\n\n## Shape Contract\n\n{shape_contract}\n\n## Fixture Rules\n\n{fixture_rules}\n\n## Promotion Gate\n\n- `robots.txt` resolves and is plain text, not HTML.\n- `terms_url` resolves without auth or soft blocks.\n- `{school_id}` exists in `schools` for the environment you test against.\n- `expected_shape` matches the live target or the committed fixture.\n- `source_maturity` and `live_fetch_enabled` still say the same thing operationally.\n{live_ready_line}\n\n## Suggested Commands\n\n```bash\ncargo run -p crawler -- doctor --manifest {manifest_path}\ncargo run -p crawler -- dry-run --manifest {manifest_path}\ncargo run -p crawler -- health --manifest {manifest_path}\n```\n\n## Test Skeleton\n\n```rust\n#[tokio::test]\nasync fn fetch_and_parse_{test_name}_imports_seeded_school() -> anyhow::Result<()> {{\n    let fixture_name = \"{fixture_file}\";\n    let logical_name = \"{logical_name}\";\n    let manifest_path = \"{manifest_path}\";\n\n    // 1. serve the local fixture over axum\n    // 2. point a temporary manifest at that local server\n    // 3. run fetch -> parse -> health\n    // 4. assert imported rows for school_id = \"{school_id}\"\n    // 5. assert the earliest stable title/date pair\n    // 6. assert health text includes `source_maturity` and `expected_shape`\n\n    Ok(())\n}}\n```\n",
        source_name = request.source_name,
        source_id = request.source_id,
        source_maturity = request.source_maturity,
        parser_key = request.parser_key,
        expected_shape = request.expected_shape,
        school_id = request.school_id,
        logical_name = logical_name,
        target_url = request.target_url,
        manifest_path = manifest_path.display(),
        fixture_path = fixture_path.display(),
        logical_name_reason = preset.logical_name_reason,
        event_category = preset.event_category,
        event_category_reason = preset.event_category_reason,
        is_open_day = preset.is_open_day,
        is_open_day_reason = preset.is_open_day_reason,
        priority_weight = format_priority_weight(preset.priority_weight),
        priority_weight_reason = preset.priority_weight_reason,
        terms_url = preset.terms_url,
        shape_contract = shape_contract,
        fixture_rules = fixture_rules,
        live_ready_line = live_ready_line,
        test_name = request.source_id.replace('-', "_"),
        fixture_file = fixture_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("fixture.html")
    )
}

pub async fn run_fetch_command(
    settings: &AppSettings,
    manifest_path: impl AsRef<Path>,
) -> Result<CrawlCommandSummary> {
    let manifest_path = canonical_manifest_path(manifest_path)?;
    let manifest = load_manifest(&manifest_path)?;
    let targets = resolve_and_validate_targets(&manifest)?;
    let registry = ParserRegistry::default();
    let parser = registry
        .get(&manifest.parser_key)
        .with_context(|| format!("unknown parser_key {}", manifest.parser_key))?;
    let _metadata = resolve_manifest_metadata(&manifest, Some(parser))?;
    let parser_version = manifest.effective_parser_version(parser.default_version());
    let manifest_audit = build_manifest_audit(&manifest_path, &manifest, &parser_version)?;
    let crawl_run_id =
        begin_crawl_run(&settings.database_url, &manifest_audit, parser.key()).await?;

    if !manifest.allowlist.live_fetch_enabled {
        let reason = manifest
            .allowlist
            .live_fetch_block_reason
            .clone()
            .unwrap_or_else(|| "live fetch disabled by manifest policy".to_string());
        for target in &targets {
            record_crawl_fetch_log(
                &settings.database_url,
                &CrawlFetchLogEntry {
                    crawl_run_id,
                    logical_name: target.logical_name.clone(),
                    target_url: target.url.clone(),
                    final_url: None,
                    http_status: None,
                    checksum_sha256: None,
                    size_bytes: None,
                    staged_path: None,
                    fetch_status: "blocked_policy".to_string(),
                    content_changed: None,
                    details: json!({
                        "reason": reason,
                        "robots_txt_url": manifest.allowlist.robots_txt_url,
                        "terms_url": manifest.allowlist.terms_url,
                        "terms_note": manifest.allowlist.terms_note,
                        "user_agent": manifest.allowlist.user_agent
                    }),
                },
            )
            .await?;
        }

        finish_crawl_run(&settings.database_url, crawl_run_id, "failed", 0, 0, 0).await?;
        anyhow::bail!(
            "live fetch disabled by manifest policy for {}: {}",
            manifest.source_id,
            reason
        );
    }

    let client = build_http_fetch_client()?;

    let mut fetched_targets = 0_i64;
    let mut report_count = 0_usize;

    let result: Result<()> = async {
        let robots_request = HttpFetchRequest {
            source_id: &manifest.source_id,
            logical_name: "robots_txt",
            url: &manifest.allowlist.robots_txt_url,
            user_agent: &manifest.allowlist.user_agent,
            allowed_domains: &manifest.allowlist.allowed_domains,
        };
        let robots_txt = fetch_robots_txt(&client, &robots_request).await?;

        for (index, target) in targets.iter().enumerate() {
            if index > 0 && manifest.allowlist.min_fetch_interval_ms > 0 {
                tokio::time::sleep(Duration::from_millis(
                    manifest.allowlist.min_fetch_interval_ms,
                ))
                .await;
            }

            let parsed_url = ensure_allowed_url(&target.url, &manifest.allowlist.allowed_domains)?;
            let robots = evaluate_robots(
                &robots_txt,
                &manifest.allowlist.user_agent,
                parsed_url.path(),
            );
            if !robots.allowed {
                report_count += 1;
                record_crawl_fetch_log(
                    &settings.database_url,
                    &CrawlFetchLogEntry {
                        crawl_run_id,
                        logical_name: target.logical_name.clone(),
                        target_url: target.url.clone(),
                        final_url: None,
                        http_status: None,
                        checksum_sha256: None,
                        size_bytes: None,
                        staged_path: None,
                        fetch_status: "blocked_robots".to_string(),
                        content_changed: None,
                        details: json!({
                            "matched_rule": robots.matched_rule,
                            "robots_txt_url": manifest.allowlist.robots_txt_url,
                            "terms_url": manifest.allowlist.terms_url,
                            "terms_note": manifest.allowlist.terms_note,
                            "user_agent": manifest.allowlist.user_agent
                        }),
                    },
                )
                .await?;
                continue;
            }

            match fetch_to_raw(
                &client,
                &HttpFetchRequest {
                    source_id: &manifest.source_id,
                    logical_name: &target.logical_name,
                    url: &target.url,
                    user_agent: &manifest.allowlist.user_agent,
                    allowed_domains: &manifest.allowlist.allowed_domains,
                },
                &settings.raw_storage_dir,
            )
                .await
            {
                Ok(fetch) => {
                    let final_url = match ensure_allowed_url(
                        &fetch.final_url,
                        &manifest.allowlist.allowed_domains,
                    ) {
                        Ok(final_url) => final_url,
                        Err(error) => {
                            report_count += 1;
                            if fetch.staged_was_created {
                                discard_staged_fetch(&fetch.staged_path);
                            }
                            record_crawl_fetch_log(
                                &settings.database_url,
                                &CrawlFetchLogEntry {
                                    crawl_run_id,
                                    logical_name: fetch.logical_name.clone(),
                                    target_url: fetch.target_url.clone(),
                                    final_url: Some(fetch.final_url.clone()),
                                    http_status: Some(fetch.status_code as i32),
                                    checksum_sha256: Some(fetch.checksum_sha256.clone()),
                                    size_bytes: Some(fetch.size_bytes as i64),
                                    staged_path: None,
                                    fetch_status: "blocked_policy".to_string(),
                                    content_changed: None,
                                    details: json!({
                                        "reason": format!("resolved final_url violated crawler allowlist: {error}"),
                                        "content_type": fetch.content_type.clone(),
                                        "robots_txt_url": manifest.allowlist.robots_txt_url,
                                        "terms_url": manifest.allowlist.terms_url,
                                        "terms_note": manifest.allowlist.terms_note,
                                        "user_agent": manifest.allowlist.user_agent,
                                        "min_fetch_interval_ms": manifest.allowlist.min_fetch_interval_ms
                                    }),
                                },
                            )
                            .await?;
                            continue;
                        }
                    };
                    let final_robots = evaluate_robots(
                        &robots_txt,
                        &manifest.allowlist.user_agent,
                        final_url.path(),
                    );
                    if !final_robots.allowed {
                        report_count += 1;
                        if fetch.staged_was_created {
                            discard_staged_fetch(&fetch.staged_path);
                        }
                        record_crawl_fetch_log(
                            &settings.database_url,
                            &CrawlFetchLogEntry {
                                crawl_run_id,
                                logical_name: fetch.logical_name.clone(),
                                target_url: fetch.target_url.clone(),
                                final_url: Some(fetch.final_url.clone()),
                                http_status: Some(fetch.status_code as i32),
                                checksum_sha256: Some(fetch.checksum_sha256.clone()),
                                size_bytes: Some(fetch.size_bytes as i64),
                                staged_path: None,
                                fetch_status: "blocked_robots".to_string(),
                                content_changed: None,
                                details: json!({
                                    "matched_rule": final_robots.matched_rule,
                                    "content_type": fetch.content_type.clone(),
                                    "robots_txt_url": manifest.allowlist.robots_txt_url,
                                    "terms_url": manifest.allowlist.terms_url,
                                    "terms_note": manifest.allowlist.terms_note,
                                    "user_agent": manifest.allowlist.user_agent,
                                    "min_fetch_interval_ms": manifest.allowlist.min_fetch_interval_ms
                                }),
                            },
                        )
                        .await?;
                        continue;
                    }

                    let previous_checksum = latest_crawl_fetch_checksum(
                        &settings.database_url,
                        &manifest_audit.manifest_path,
                        &target.logical_name,
                        &target.url,
                    )
                    .await?;
                    let content_changed =
                        previous_checksum.as_deref() != Some(fetch.checksum_sha256.as_str());
                    let fetch_status = if content_changed {
                        "fetched"
                    } else {
                        "not_modified"
                    };

                    record_crawl_fetch_log(
                        &settings.database_url,
                        &CrawlFetchLogEntry {
                            crawl_run_id,
                            logical_name: fetch.logical_name.clone(),
                            target_url: fetch.target_url.clone(),
                            final_url: Some(fetch.final_url.clone()),
                            http_status: Some(fetch.status_code as i32),
                            checksum_sha256: Some(fetch.checksum_sha256.clone()),
                            size_bytes: Some(fetch.size_bytes as i64),
                            staged_path: Some(fetch.staged_path.display().to_string()),
                            fetch_status: fetch_status.to_string(),
                            content_changed: Some(content_changed),
                            details: json!({
                                "content_type": fetch.content_type,
                                "matched_rule": final_robots.matched_rule,
                                "robots_txt_url": manifest.allowlist.robots_txt_url,
                                "terms_url": manifest.allowlist.terms_url,
                                "terms_note": manifest.allowlist.terms_note,
                                "user_agent": manifest.allowlist.user_agent,
                                "min_fetch_interval_ms": manifest.allowlist.min_fetch_interval_ms
                            }),
                        },
                    )
                    .await?;
                    fetched_targets += 1;
                }
                Err(error) => {
                    report_count += 1;
                    let error_message = error.to_string();
                    let fetch_status = classify_fetch_error_status(&error_message);
                    record_crawl_fetch_log(
                        &settings.database_url,
                        &CrawlFetchLogEntry {
                            crawl_run_id,
                            logical_name: target.logical_name.clone(),
                            target_url: target.url.clone(),
                            final_url: None,
                            http_status: None,
                            checksum_sha256: None,
                            size_bytes: None,
                            staged_path: None,
                            fetch_status: fetch_status.to_string(),
                            content_changed: None,
                            details: json!({
                                "error": error_message,
                                "robots_txt_url": manifest.allowlist.robots_txt_url,
                                "terms_url": manifest.allowlist.terms_url,
                                "terms_note": manifest.allowlist.terms_note,
                                "user_agent": manifest.allowlist.user_agent
                            }),
                        },
                    )
                    .await?;
                }
            }
        }

        ensure!(
            fetched_targets > 0,
            "no crawl targets were fetched successfully"
        );
        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            mark_crawl_run_fetched(&settings.database_url, crawl_run_id, fetched_targets).await?;
            Ok(CrawlCommandSummary {
                label: format!("crawl-fetch:{}", manifest.source_id),
                crawl_run_id,
                fetched_targets,
                parsed_rows: 0,
                imported_rows: 0,
                report_count,
            })
        }
        Err(error) => {
            let _ = finish_crawl_run(
                &settings.database_url,
                crawl_run_id,
                "failed",
                fetched_targets,
                0,
                0,
            )
            .await;
            Err(error)
        }
    }
}

pub async fn run_parse_command(
    settings: &AppSettings,
    manifest_path: impl AsRef<Path>,
) -> Result<CrawlCommandSummary> {
    let manifest_path = canonical_manifest_path(manifest_path)?;
    let pending_run = claim_latest_fetched_crawl_run(
        &settings.database_url,
        &manifest_path.display().to_string(),
    )
    .await?
    .with_context(|| {
        format!(
            "no fetched crawl run is ready for manifest {}",
            manifest_path.display()
        )
    })?;
    run_parse_command_with_claimed_run(settings, &manifest_path, pending_run.crawl_run_id).await
}

async fn run_parse_command_with_claimed_run(
    settings: &AppSettings,
    manifest_path: &Path,
    crawl_run_id: i64,
) -> Result<CrawlCommandSummary> {
    let manifest = load_manifest(manifest_path)?;
    let targets = resolve_and_validate_targets(&manifest)?;
    let targets_by_name = targets
        .into_iter()
        .map(|target| (target.logical_name.clone(), target))
        .collect::<std::collections::BTreeMap<_, _>>();
    let registry = ParserRegistry::default();
    let parser = registry
        .get(&manifest.parser_key)
        .with_context(|| format!("unknown parser_key {}", manifest.parser_key))?;
    let _metadata = resolve_manifest_metadata(&manifest, Some(parser))?;
    let parser_version = manifest.effective_parser_version(parser.default_version());

    let mut report_count = 0_usize;
    let mut parsed_rows = 0_i64;
    let mut imported_rows = 0_i64;
    let mut fetched_targets = 0_i64;
    let mut parse_failed_targets = 0_usize;
    let mut zero_row_targets = 0_usize;

    let result: Result<()> = async {
        let fetch_logs = load_crawl_fetch_logs(&settings.database_url, crawl_run_id).await?;
        fetched_targets = fetch_logs
            .iter()
            .filter(|entry| matches!(entry.fetch_status.as_str(), "fetched" | "not_modified"))
            .count() as i64;

        record_parse_report(
            &settings.database_url,
            &mut report_count,
            CrawlParseReportEntry {
                crawl_run_id,
                logical_name: None,
                level: "info".to_string(),
                code: "parser_version".to_string(),
                message: "Recorded parser version for this crawl run.".to_string(),
                parsed_rows: None,
                details: json!({
                    "parser_key": manifest.parser_key,
                    "parser_version": parser_version
                }),
            },
        )
        .await?;

        let mut collected = Vec::new();
        for fetch in fetch_logs
            .iter()
            .filter(|entry| matches!(entry.fetch_status.as_str(), "fetched" | "not_modified"))
        {
            let target = targets_by_name
                .get(&fetch.logical_name)
                .with_context(|| format!("unknown crawl target {}", fetch.logical_name))?;
            let staged_path = fetch
                .staged_path
                .as_ref()
                .with_context(|| format!("missing staged_path for {}", fetch.logical_name))?;
            let html = fs::read_to_string(staged_path)
                .with_context(|| format!("failed to read staged content {}", staged_path))?;

            match parser.parse(&crawler_core::ParseInput {
                source_id: &manifest.source_id,
                logical_name: &fetch.logical_name,
                target_url: &fetch.target_url,
                html: &html,
                target,
            }) {
                Ok(output) => {
                    for entry in output.report_entries {
                        record_parse_report(
                            &settings.database_url,
                            &mut report_count,
                            CrawlParseReportEntry {
                                crawl_run_id,
                                logical_name: entry.logical_name,
                                level: entry.level,
                                code: entry.code,
                                message: entry.message,
                                parsed_rows: entry.parsed_rows,
                                details: entry.details,
                            },
                        )
                        .await?;
                    }

                    let records = finalize_parsed_events(
                        &manifest.source_id,
                        &fetch.logical_name,
                        &fetch.target_url,
                        target,
                        output.events,
                    )?;
                    parsed_rows += records.len() as i64;
                    collected.extend(records);
                }
                Err(error) => {
                    let message = error.to_string();
                    if is_zero_event_parse_message(&message) {
                        zero_row_targets += 1;
                        record_parse_report(
                            &settings.database_url,
                            &mut report_count,
                            CrawlParseReportEntry {
                                crawl_run_id,
                                logical_name: Some(fetch.logical_name.clone()),
                                level: "warn".to_string(),
                                code: "parsed_zero_rows".to_string(),
                                message,
                                parsed_rows: Some(0),
                                details: json!({
                                    "target_url": fetch.target_url,
                                    "staged_path": staged_path
                                }),
                            },
                        )
                        .await?;
                    } else {
                        parse_failed_targets += 1;
                        record_parse_report(
                            &settings.database_url,
                            &mut report_count,
                            CrawlParseReportEntry {
                                crawl_run_id,
                                logical_name: Some(fetch.logical_name.clone()),
                                level: "error".to_string(),
                                code: "parse_failed".to_string(),
                                message,
                                parsed_rows: None,
                                details: json!({
                                    "target_url": fetch.target_url,
                                    "staged_path": staged_path
                                }),
                            },
                        )
                        .await?;
                    }
                }
            }
        }

        if collected.is_empty() && parse_failed_targets > 0 {
            anyhow::bail!("no crawler events were parsed successfully");
        }

        let (deduped, dedupe_reports) = dedupe_events(collected);
        for report in dedupe_reports {
            record_dedupe_report(&settings.database_url, crawl_run_id, report).await?;
            report_count += 1;
        }

        let summary = import_crawled_events(
            &settings.database_url,
            &manifest.source_id,
            &deduped.iter().map(to_event_csv_record).collect::<Vec<_>>(),
            can_deactivate_stale_rows(&fetch_logs, parse_failed_targets, zero_row_targets),
        )
        .await?;
        imported_rows = summary.core_rows;

        for entry in summary.report_entries {
            record_parse_report(
                &settings.database_url,
                &mut report_count,
                CrawlParseReportEntry {
                    crawl_run_id,
                    logical_name: None,
                    level: entry.level,
                    code: entry.code,
                    message: entry.message,
                    parsed_rows: entry.row_count,
                    details: entry.details,
                },
            )
            .await?;
        }

        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            finish_crawl_run(
                &settings.database_url,
                crawl_run_id,
                "succeeded",
                fetched_targets,
                parsed_rows,
                imported_rows,
            )
            .await?;
            Ok(CrawlCommandSummary {
                label: format!("crawl-parse:{}", manifest.source_id),
                crawl_run_id,
                fetched_targets,
                parsed_rows,
                imported_rows,
                report_count,
            })
        }
        Err(error) => {
            let _ = finish_crawl_run(
                &settings.database_url,
                crawl_run_id,
                "failed",
                fetched_targets,
                parsed_rows,
                imported_rows,
            )
            .await;
            Err(error)
        }
    }
}

pub async fn run_crawl_command(
    settings: &AppSettings,
    manifest_path: impl AsRef<Path>,
) -> Result<CrawlCommandSummary> {
    let manifest_path = canonical_manifest_path(manifest_path)?;
    let fetch_summary = run_fetch_command(settings, &manifest_path).await?;
    let pending_run = claim_fetched_crawl_run(&settings.database_url, fetch_summary.crawl_run_id)
        .await?
        .with_context(|| {
            format!(
                "fetched crawl run {} is no longer ready for parsing",
                fetch_summary.crawl_run_id
            )
        })?;
    let parse_summary =
        run_parse_command_with_claimed_run(settings, &manifest_path, pending_run.crawl_run_id)
            .await?;

    Ok(CrawlCommandSummary {
        label: parse_summary.label,
        crawl_run_id: parse_summary.crawl_run_id,
        fetched_targets: fetch_summary.fetched_targets,
        parsed_rows: parse_summary.parsed_rows,
        imported_rows: parse_summary.imported_rows,
        report_count: fetch_summary.report_count + parse_summary.report_count,
    })
}

pub async fn run_doctor_command(
    settings: &AppSettings,
    manifest_path: impl AsRef<Path>,
) -> Result<CrawlDoctorSummary> {
    let manifest_path = canonical_manifest_path(manifest_path)?;
    let manifest = load_manifest(&manifest_path)?;
    let registry = ParserRegistry::default();
    let parser = registry.get(&manifest.parser_key);
    let parser_registered = parser.is_some();
    let metadata = resolve_manifest_metadata(&manifest, parser)?;
    let targets = resolve_and_validate_targets(&manifest)?;
    let school_ids = targets
        .iter()
        .map(|target| target.school_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let client = build_http_client()?;
    let robots = probe_url(
        &client,
        &manifest.allowlist.robots_txt_url,
        &manifest.allowlist.user_agent,
        &manifest.allowlist.allowed_domains,
    )
    .await;
    let terms = probe_url(
        &client,
        &manifest.allowlist.terms_url,
        &manifest.allowlist.user_agent,
        &manifest.allowlist.allowed_domains,
    )
    .await;

    let robots_body = robots.body.as_deref().unwrap_or_default();
    let mut targets_summary = Vec::new();
    let mut issues = Vec::new();
    let known_school_ids = match load_existing_school_ids(&settings.database_url, &school_ids).await
    {
        Ok(known_school_ids) => Some(known_school_ids),
        Err(error) => {
            issues.push(DiagnosticIssue {
                level: "warn".to_string(),
                code: "school_lookup_failed".to_string(),
                message: format!("failed to verify target school_ids against database: {error}"),
            });
            None
        }
    };

    if !parser_registered {
        issues.push(DiagnosticIssue {
            level: "error".to_string(),
            code: "unknown_parser_key".to_string(),
            message: format!(
                "manifest parser_key {} is not registered",
                manifest.parser_key
            ),
        });
    }
    if !manifest.allowlist.live_fetch_enabled {
        issues.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "live_fetch_disabled".to_string(),
            message: manifest
                .allowlist
                .live_fetch_block_reason
                .clone()
                .unwrap_or_else(|| "live fetch disabled by manifest policy".to_string()),
        });
    }
    collect_url_probe_issues("robots", &robots, &mut issues);
    collect_url_probe_issues("terms", &terms, &mut issues);
    if robots
        .content_type
        .as_deref()
        .is_some_and(|value| !value.starts_with("text/plain"))
    {
        issues.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "robots_unexpected_content_type".to_string(),
            message: format!(
                "robots content-type is {}, expected text/plain",
                robots.content_type.as_deref().unwrap_or("-")
            ),
        });
    }
    if looks_like_html(robots_body) {
        issues.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "robots_html_body".to_string(),
            message: "robots URL returned HTML-like content instead of plain-text robots rules"
                .to_string(),
        });
    }

    for target in targets {
        let robots_decision = if robots.error.is_none() && !robots_body.is_empty() {
            let parsed_url = ensure_allowed_url(&target.url, &manifest.allowlist.allowed_domains)?;
            Some(evaluate_robots(
                robots_body,
                &manifest.allowlist.user_agent,
                parsed_url.path(),
            ))
        } else {
            None
        };
        let school_exists = known_school_ids
            .as_ref()
            .map(|known| known.contains(target.school_id.as_str()));
        if matches!(school_exists, Some(false)) {
            issues.push(DiagnosticIssue {
                level: "warn".to_string(),
                code: "missing_school_id".to_string(),
                message: format!(
                    "logical_name {} references missing school_id {}",
                    target.logical_name, target.school_id
                ),
            });
        }

        let (shape_status, shape_detail) = if let Some(expected_shape) = metadata.expected_shape {
            if !manifest.allowlist.live_fetch_enabled {
                (
                    Some("skipped".to_string()),
                    Some("live fetch disabled by manifest policy".to_string()),
                )
            } else if robots_decision.is_none() {
                (
                    Some("skipped".to_string()),
                    Some("robots policy could not be evaluated for this target".to_string()),
                )
            } else if matches!(
                robots_decision.as_ref().map(|decision| decision.allowed),
                Some(false)
            ) {
                (
                    Some("skipped".to_string()),
                    Some("target is blocked by robots policy".to_string()),
                )
            } else {
                let target_probe = probe_target_body(
                    &client,
                    &target.url,
                    &manifest.allowlist.user_agent,
                    &manifest.allowlist.allowed_domains,
                    robots_body,
                )
                .await;
                if let Some(error) = target_probe.error {
                    issues.push(DiagnosticIssue {
                        level: "warn".to_string(),
                        code: "target_shape_fetch_failed".to_string(),
                        message: format!(
                            "logical_name {} failed shape probe: {}",
                            target.logical_name, error
                        ),
                    });
                    (Some("fetch_failed".to_string()), Some(error))
                } else {
                    let check = check_expected_shape(
                        expected_shape,
                        &target_probe.body,
                        target_probe.content_type.as_deref(),
                    );
                    if check.matched {
                        (Some("matched".to_string()), Some(check.summary))
                    } else {
                        issues.push(DiagnosticIssue {
                            level: "warn".to_string(),
                            code: "expected_shape_mismatch".to_string(),
                            message: format!(
                                "logical_name {} expected_shape {} did not match: {}",
                                target.logical_name, expected_shape, check.summary
                            ),
                        });
                        (Some("mismatch".to_string()), Some(check.summary))
                    }
                }
            }
        } else {
            (None, None)
        };

        targets_summary.push(DoctorTargetSummary {
            logical_name: target.logical_name,
            target_url: target.url,
            school_id: target.school_id,
            school_exists,
            robots_allowed: robots_decision.as_ref().map(|decision| decision.allowed),
            matched_rule: robots_decision.and_then(|decision| decision.matched_rule),
            expected_shape: metadata.expected_shape,
            shape_status,
            shape_detail,
        });
    }

    Ok(CrawlDoctorSummary {
        manifest_path: manifest_path.display().to_string(),
        source_id: manifest.source_id,
        source_name: manifest.source_name,
        source_maturity: metadata.source_maturity,
        parser_key: manifest.parser_key,
        parser_registered,
        expected_shape: metadata.expected_shape,
        live_fetch_enabled: manifest.allowlist.live_fetch_enabled,
        robots,
        terms,
        targets: targets_summary,
        issues,
    })
}

pub async fn run_dry_run_command(
    settings: &AppSettings,
    manifest_path: impl AsRef<Path>,
) -> Result<CrawlDryRunSummary> {
    let manifest_path = canonical_manifest_path(manifest_path)?;
    let manifest = load_manifest(&manifest_path)?;
    let targets = resolve_and_validate_targets(&manifest)?;
    let targets_by_name = targets
        .into_iter()
        .map(|target| (target.logical_name.clone(), target))
        .collect::<BTreeMap<_, _>>();
    let registry = ParserRegistry::default();
    let parser = registry
        .get(&manifest.parser_key)
        .with_context(|| format!("unknown parser_key {}", manifest.parser_key))?;
    let metadata = resolve_manifest_metadata(&manifest, Some(parser))?;
    let parser_version = manifest.effective_parser_version(parser.default_version());
    let pending_run =
        load_latest_fetched_crawl_run(&settings.database_url, &manifest_path.display().to_string())
            .await?
            .with_context(|| {
                format!(
                    "no fetched crawl run is ready for manifest {}",
                    manifest_path.display()
                )
            })?;
    let fetch_logs =
        load_crawl_fetch_logs(&settings.database_url, pending_run.crawl_run_id).await?;
    let mut collected = Vec::new();
    let mut parse_errors = Vec::new();
    let mut warnings = Vec::new();
    let mut logical_name_summaries = Vec::new();
    let mut date_drift_warnings = 0_usize;
    let mut zero_row_targets = 0_usize;

    for fetch in fetch_logs
        .iter()
        .filter(|entry| matches!(entry.fetch_status.as_str(), "fetched" | "not_modified"))
    {
        let target = targets_by_name
            .get(&fetch.logical_name)
            .with_context(|| format!("unknown crawl target {}", fetch.logical_name))?;
        let staged_path = fetch
            .staged_path
            .as_ref()
            .with_context(|| format!("missing staged_path for {}", fetch.logical_name))?;
        let html = fs::read_to_string(staged_path)
            .with_context(|| format!("failed to read staged content {}", staged_path))?;

        match parser.parse(&crawler_core::ParseInput {
            source_id: &manifest.source_id,
            logical_name: &fetch.logical_name,
            target_url: &fetch.target_url,
            html: &html,
            target,
        }) {
            Ok(output) => {
                let logical_drifts = output
                    .events
                    .iter()
                    .filter_map(|seed| {
                        build_date_drift_warning(&fetch.logical_name, &seed.title, &seed.details)
                    })
                    .collect::<Vec<_>>();
                date_drift_warnings += logical_drifts.len();
                let logical_drift_count = logical_drifts.len();
                warnings.extend(logical_drifts.into_iter().map(|message| DiagnosticIssue {
                    level: "warn".to_string(),
                    code: "date_drift".to_string(),
                    message,
                }));

                let records = finalize_parsed_events(
                    &manifest.source_id,
                    &fetch.logical_name,
                    &fetch.target_url,
                    target,
                    output.events,
                )?;
                logical_name_summaries.push(LogicalDryRunSummary {
                    logical_name: fetch.logical_name.clone(),
                    parsed_rows: records.len() as i64,
                    date_drift_warnings: logical_drift_count,
                    parse_error: None,
                });
                collected.extend(records);
            }
            Err(error) => {
                let message = error.to_string();
                if is_zero_event_parse_message(&message) {
                    zero_row_targets += 1;
                    warnings.push(DiagnosticIssue {
                        level: "warn".to_string(),
                        code: "no_events_found".to_string(),
                        message: format!("{}: {}", fetch.logical_name, message),
                    });
                    logical_name_summaries.push(LogicalDryRunSummary {
                        logical_name: fetch.logical_name.clone(),
                        parsed_rows: 0,
                        date_drift_warnings: 0,
                        parse_error: None,
                    });
                } else {
                    parse_errors.push(DiagnosticIssue {
                        level: "error".to_string(),
                        code: "parse_failed".to_string(),
                        message: format!("{}: {}", fetch.logical_name, message),
                    });
                    logical_name_summaries.push(LogicalDryRunSummary {
                        logical_name: fetch.logical_name.clone(),
                        parsed_rows: 0,
                        date_drift_warnings: 0,
                        parse_error: Some(message),
                    });
                }
            }
        }
    }

    let parsed_rows = collected.len() as i64;
    if parsed_rows == 0 {
        warnings.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "parsed_zero_rows".to_string(),
            message: "dry-run parsed 0 rows; import would be empty".to_string(),
        });
    }

    let (deduped, _dedupe_reports) = dedupe_events(collected);
    let deduped_rows = deduped.len() as i64;
    let school_ids = deduped
        .iter()
        .map(|record| record.school_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let known_school_ids = load_existing_school_ids(&settings.database_url, &school_ids)
        .await
        .with_context(|| "failed to load known school ids for dry-run")?;
    let imported_ids = deduped
        .iter()
        .filter(|record| known_school_ids.contains(record.school_id.as_str()))
        .map(|record| record.event_id.clone())
        .collect::<BTreeSet<_>>();
    let missing_school_rows = deduped
        .iter()
        .filter(|record| !known_school_ids.contains(record.school_id.as_str()))
        .count() as i64;
    if missing_school_rows > 0 {
        warnings.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "missing_school_id".to_string(),
            message: format!(
                "dry-run would skip {} rows because school_id is missing",
                missing_school_rows
            ),
        });
    }
    let active_ids =
        load_active_event_ids_for_source(&settings.database_url, "crawl", &manifest.source_id)
            .await
            .with_context(|| "failed to load active event ids for dry-run")?;
    let deactivated_rows = if !can_deactivate_stale_rows(
        &fetch_logs,
        parse_errors.len(),
        zero_row_targets,
    ) {
        warnings.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "partial_import_skips_stale_deactivation".to_string(),
            message:
                "dry-run would keep existing active rows because one or more crawl targets failed to fetch, failed to parse, or produced zero rows"
                    .to_string(),
        });
        0
    } else if missing_school_rows > 0 {
        warnings.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: "missing_school_skips_stale_deactivation".to_string(),
            message:
                "dry-run would keep existing active rows because one or more rows reference a missing school_id"
                    .to_string(),
        });
        0
    } else {
        active_ids.difference(&imported_ids).count() as i64
    };

    Ok(CrawlDryRunSummary {
        manifest_path: manifest_path.display().to_string(),
        source_id: manifest.source_id,
        source_name: manifest.source_name,
        source_maturity: metadata.source_maturity,
        parser_key: manifest.parser_key,
        parser_version,
        expected_shape: metadata.expected_shape,
        crawl_run_id: pending_run.crawl_run_id,
        ready_targets: fetch_logs
            .iter()
            .filter(|entry| matches!(entry.fetch_status.as_str(), "fetched" | "not_modified"))
            .count(),
        parsed_rows,
        deduped_rows,
        imported_rows: imported_ids.len() as i64,
        deactivated_rows,
        missing_school_rows,
        date_drift_warnings,
        parse_errors,
        warnings,
        logical_name_summaries,
    })
}

pub async fn run_health_command(
    settings: &AppSettings,
    manifest_path: impl AsRef<Path>,
    limit: usize,
) -> Result<ParserHealthSummary> {
    let manifest_path = canonical_manifest_path(manifest_path)?;
    let manifest = load_manifest(&manifest_path)?;
    let registry = ParserRegistry::default();
    let parser = registry
        .get(&manifest.parser_key)
        .with_context(|| format!("unknown parser_key {}", manifest.parser_key))?;
    let metadata = resolve_manifest_metadata(&manifest, Some(parser))?;
    let parser_version = manifest.effective_parser_version(parser.default_version());
    let health = load_crawl_run_health(
        &settings.database_url,
        &manifest_path.display().to_string(),
        limit,
    )
    .await?;

    let mut fetch_status_totals = BTreeMap::new();
    let mut parse_level_totals = BTreeMap::new();
    let mut succeeded_runs = 0_usize;
    let mut failed_runs = 0_usize;
    let mut active_runs = 0_usize;
    let mut dedupe_report_total = 0_i64;
    let current_logical_names = manifest
        .targets
        .iter()
        .map(|target| target.logical_name.clone())
        .collect::<BTreeSet<_>>();

    for run in &health.runs {
        match run.status.as_str() {
            "succeeded" => succeeded_runs += 1,
            "failed" => failed_runs += 1,
            _ => active_runs += 1,
        }
        dedupe_report_total += run.dedupe_count;
        merge_counts(&mut fetch_status_totals, &run.fetch_status_counts);
        merge_counts(&mut parse_level_totals, &run.parse_level_counts);
    }

    let logical_name_red_flags =
        load_logical_name_red_flags(&settings.database_url, &health.runs, &current_logical_names)
            .await?;
    let recent_reason_trend =
        load_recent_reason_trend(&settings.database_url, &health.runs).await?;
    let healthy_logical_name_count = current_logical_names
        .len()
        .saturating_sub(logical_name_red_flags.len());
    let mut reason_totals = BTreeMap::new();
    for flag in &logical_name_red_flags {
        for reason in &flag.reasons {
            if let Some(normalized) = normalize_reason_for_total(reason) {
                *reason_totals.entry(normalized).or_insert(0) += 1;
            }
        }
    }

    Ok(ParserHealthSummary {
        manifest_path: manifest_path.display().to_string(),
        source_id: manifest.source_id,
        source_name: manifest.source_name,
        source_maturity: metadata.source_maturity,
        parser_key: parser.key().to_string(),
        parser_version,
        expected_shape: metadata.expected_shape,
        total_runs: health.total_runs,
        shown_runs: health.runs.len(),
        succeeded_runs,
        failed_runs,
        active_runs,
        fetch_status_totals,
        parse_level_totals,
        dedupe_report_total,
        recent_runs: health.runs,
        recent_reason_trend,
        logical_name_red_flags,
        healthy_logical_name_count,
        reason_totals,
    })
}

async fn load_logical_name_red_flags(
    database_url: &str,
    runs: &[CrawlRunHealthSnapshot],
    logical_names: &BTreeSet<String>,
) -> Result<Vec<LogicalNameRedFlag>> {
    let mut signals_by_run = BTreeMap::<i64, BTreeMap<String, LogicalNameRunSignal>>::new();
    for run in runs {
        let mut logical_map = BTreeMap::<String, LogicalNameRunSignal>::new();

        for fetch_log in load_crawl_fetch_logs(database_url, run.crawl_run_id).await? {
            logical_map
                .entry(fetch_log.logical_name.clone())
                .or_default()
                .fetch_logs
                .push(fetch_log);
        }

        for parse_error in load_crawl_parse_errors(database_url, run.crawl_run_id).await? {
            logical_map
                .entry(parse_error.logical_name.clone())
                .or_default()
                .parse_errors
                .push(parse_error);
        }

        signals_by_run.insert(run.crawl_run_id, logical_map);
    }

    let mut red_flags = Vec::new();
    for logical_name in logical_names {
        let mut observed_runs = 0_usize;
        let mut successful_runs = 0_usize;
        let mut red_runs = 0_usize;
        let mut consecutive_red_runs = 0_usize;
        let mut latest_fetch_status = None;
        let mut latest_error = None;
        let mut missing_from_latest_run = false;

        for (index, run) in runs.iter().enumerate() {
            let signal = signals_by_run
                .get(&run.crawl_run_id)
                .and_then(|logical_map| logical_map.get(logical_name));
            let fetch_status = signal.and_then(|signal| summarize_fetch_status(&signal.fetch_logs));
            let parse_error = signal.and_then(|signal| summarize_parse_error(&signal.parse_errors));
            let run_is_observed = signal.is_some();
            let run_is_red = is_red_signal(
                fetch_status.as_deref(),
                parse_error.as_ref(),
                run_is_observed,
            );
            let run_is_green = is_green_signal(fetch_status.as_deref(), parse_error.as_ref());

            if run_is_observed {
                observed_runs += 1;
            }
            if run_is_green {
                successful_runs += 1;
            }
            if run_is_red {
                red_runs += 1;
            }
            if run_is_red && consecutive_red_runs == index {
                consecutive_red_runs += 1;
            }

            if index == 0 {
                missing_from_latest_run = !run_is_observed;
                latest_fetch_status = fetch_status.clone();
                latest_error = parse_error.clone();
            }
        }

        let mut reasons = Vec::new();
        if runs.is_empty() || observed_runs == 0 {
            reasons.push("no_recent_data".to_string());
        } else {
            if missing_from_latest_run {
                reasons.push("missing_from_latest_run".to_string());
            }
            match latest_fetch_status.as_deref() {
                Some("fetch_failed") => reasons.push("latest_fetch_failed".to_string()),
                Some("blocked_robots") => reasons.push("latest_blocked_robots".to_string()),
                Some("blocked_policy") => reasons.push("latest_blocked_policy".to_string()),
                _ => {}
            }
            if let Some(error) = &latest_error {
                reasons.push(format!("latest_parse_error:{}", error.code));
            }
            if successful_runs == 0 {
                reasons.push("no_successful_recent_runs".to_string());
            }
            if red_runs >= 2 {
                reasons.push("repeated_recent_failures".to_string());
            }
        }

        if !reasons.is_empty() {
            red_flags.push(LogicalNameRedFlag {
                logical_name: logical_name.clone(),
                reasons,
                latest_fetch_status,
                observed_runs,
                successful_runs,
                red_runs,
                consecutive_red_runs,
                latest_error,
            });
        }
    }

    red_flags.sort_by(|left, right| {
        right
            .consecutive_red_runs
            .cmp(&left.consecutive_red_runs)
            .then_with(|| right.red_runs.cmp(&left.red_runs))
            .then_with(|| left.logical_name.cmp(&right.logical_name))
    });

    Ok(red_flags)
}

pub async fn serve_manifest_dir(
    settings: &AppSettings,
    manifest_dir: impl AsRef<Path>,
    poll_interval_secs: u64,
) -> Result<()> {
    let manifest_dir = manifest_dir.as_ref().to_path_buf();
    loop {
        crawl_manifest_dir_once(settings, &manifest_dir).await?;

        tokio::time::sleep(Duration::from_secs(poll_interval_secs.max(1))).await;
    }
}

async fn crawl_manifest_dir_once(settings: &AppSettings, manifest_dir: &Path) -> Result<()> {
    for manifest in list_manifest_paths(manifest_dir)? {
        let loaded_manifest = match load_manifest(&manifest) {
            Ok(loaded_manifest) => loaded_manifest,
            Err(error) => {
                tracing::warn!(
                    manifest = %manifest.display(),
                    %error,
                    "skipping crawler manifest because it failed to load"
                );
                continue;
            }
        };
        let source_maturity = loaded_manifest.effective_source_maturity();
        if source_maturity != SourceMaturity::LiveReady {
            tracing::info!(
                manifest = %manifest.display(),
                source_id = %loaded_manifest.source_id,
                source_maturity = %source_maturity,
                "skipping crawler manifest because source_maturity is not live_ready"
            );
            continue;
        }
        match run_crawl_command(settings, &manifest).await {
            Ok(summary) => tracing::info!(
                crawl_run_id = summary.crawl_run_id,
                fetched_targets = summary.fetched_targets,
                parsed_rows = summary.parsed_rows,
                imported_rows = summary.imported_rows,
                "crawler manifest completed"
            ),
            Err(error) => {
                tracing::warn!(manifest = %manifest.display(), %error, "crawler manifest failed")
            }
        }
    }

    Ok(())
}

pub fn format_summary(summary: &CrawlCommandSummary) -> String {
    format!(
        "{} completed: crawl_run_id={}, fetched={}, parsed={}, imported={}, reports={}",
        summary.label,
        summary.crawl_run_id,
        summary.fetched_targets,
        summary.parsed_rows,
        summary.imported_rows,
        summary.report_count
    )
}

pub fn format_doctor_summary(summary: &CrawlDoctorSummary) -> String {
    let promotion_gate = doctor_promotion_gate(summary);
    let mut lines = vec![
        format!(
            "crawler doctor for {} ({})",
            summary.source_id, summary.source_name
        ),
        format!("manifest: {}", summary.manifest_path),
        format!("source_maturity: {}", summary.source_maturity),
        format!(
            "parser: {} registered={}",
            summary.parser_key, summary.parser_registered
        ),
        format!(
            "expected_shape: {}",
            summary
                .expected_shape
                .map(|shape| shape.to_string())
                .unwrap_or_else(|| "-".to_string())
        ),
        format!("live_fetch_enabled: {}", summary.live_fetch_enabled),
        format_promotion_gate(&promotion_gate),
        format!(
            "robots: requested={} status={} content_type={} final_url={}",
            summary.robots.requested_url,
            summary
                .robots
                .http_status
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            summary.robots.content_type.as_deref().unwrap_or("-"),
            summary.robots.final_url.as_deref().unwrap_or("-")
        ),
        format!(
            "terms: requested={} status={} content_type={} final_url={}",
            summary.terms.requested_url,
            summary
                .terms
                .http_status
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            summary.terms.content_type.as_deref().unwrap_or("-"),
            summary.terms.final_url.as_deref().unwrap_or("-")
        ),
    ];

    if summary.issues.is_empty() {
        lines.push("issues: none".to_string());
    } else {
        lines.push("issues:".to_string());
        for issue in &summary.issues {
            lines.push(format!(
                "- [{}] {} {}",
                issue.level, issue.code, issue.message
            ));
        }
    }

    lines.push("targets:".to_string());
    for target in &summary.targets {
        lines.push(format!(
            "- {} school_id={} school_exists={} robots_allowed={} matched_rule={} expected_shape={} shape={}",
            target.logical_name,
            target.school_id,
            target
                .school_exists
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            target
                .robots_allowed
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            target.matched_rule.as_deref().unwrap_or("-"),
            target
                .expected_shape
                .map(|shape| shape.to_string())
                .unwrap_or_else(|| "-".to_string()),
            target.shape_status.as_deref().unwrap_or("-")
        ));
        lines.push(format!("  target_url: {}", target.target_url));
        if let Some(shape_detail) = &target.shape_detail {
            lines.push(format!("  shape_detail: {}", shape_detail));
        }
    }

    lines.join("\n")
}

pub fn format_dry_run_summary(summary: &CrawlDryRunSummary) -> String {
    let promotion_gate = dry_run_promotion_gate(summary);
    let mut lines = vec![
        format!(
            "crawler dry-run for {} ({})",
            summary.source_id, summary.source_name
        ),
        format!("manifest: {}", summary.manifest_path),
        format!("source_maturity: {}", summary.source_maturity),
        format!("parser: {}@{}", summary.parser_key, summary.parser_version),
        format!(
            "expected_shape: {}",
            summary
                .expected_shape
                .map(|shape| shape.to_string())
                .unwrap_or_else(|| "-".to_string())
        ),
        format!("using fetched run: {}", summary.crawl_run_id),
        format!(
            "prediction: ready_targets={} parsed={} deduped={} imported={} inactive={} missing_school={} date_drift_warnings={}",
            summary.ready_targets,
            summary.parsed_rows,
            summary.deduped_rows,
            summary.imported_rows,
            summary.deactivated_rows,
            summary.missing_school_rows,
            summary.date_drift_warnings
        ),
        format_promotion_gate(&promotion_gate),
    ];

    if summary.parse_errors.is_empty() && summary.warnings.is_empty() {
        lines.push("warnings: none".to_string());
    } else {
        lines.push("warnings:".to_string());
        for issue in summary.parse_errors.iter().chain(summary.warnings.iter()) {
            lines.push(format!(
                "- [{}] {} {}",
                issue.level, issue.code, issue.message
            ));
        }
    }

    lines.push("logical_name summary:".to_string());
    for logical in &summary.logical_name_summaries {
        lines.push(format!(
            "- {} parsed={} date_drift_warnings={} parse_error={}",
            logical.logical_name,
            logical.parsed_rows,
            logical.date_drift_warnings,
            logical.parse_error.as_deref().unwrap_or("-")
        ));
    }

    lines.join("\n")
}

pub fn format_health_summary(summary: &ParserHealthSummary) -> String {
    let promotion_gate = health_promotion_gate(summary);
    let mut lines = vec![
        format!(
            "parser health for {} ({})",
            summary.source_id, summary.source_name
        ),
        format!("manifest: {}", summary.manifest_path),
        format!("source_maturity: {}", summary.source_maturity),
        format!("parser: {}@{}", summary.parser_key, summary.parser_version),
        format!(
            "expected_shape: {}",
            summary
                .expected_shape
                .map(|shape| shape.to_string())
                .unwrap_or_else(|| "-".to_string())
        ),
        format!(
            "runs: total={}, showing={}, succeeded={}, failed={}, active={}",
            summary.total_runs,
            summary.shown_runs,
            summary.succeeded_runs,
            summary.failed_runs,
            summary.active_runs
        ),
        format!(
            "aggregate over shown runs: fetch[{}] parse[{}] dedupe_reports={}",
            format_count_map(&summary.fetch_status_totals),
            format_count_map(&summary.parse_level_totals),
            summary.dedupe_report_total
        ),
        format_promotion_gate(&promotion_gate),
    ];

    if summary.recent_runs.is_empty() {
        lines.push("no crawl runs recorded yet".to_string());
        return lines.join("\n");
    }

    lines.push(format!(
        "logical_name signals: healthy={}, red={}",
        summary.healthy_logical_name_count,
        summary.logical_name_red_flags.len()
    ));
    lines.push(format!(
        "reason totals: {}",
        format_count_map(&summary.reason_totals)
    ));
    lines.push("recent reason trend:".to_string());
    for trend in summary.recent_reason_trend.iter().take(5) {
        lines.push(format!(
            "#{} {} reasons[{}]",
            trend.crawl_run_id,
            trend.status,
            format_count_map(&trend.reasons)
        ));
    }
    if summary.logical_name_red_flags.is_empty() {
        lines.push("red flags: none".to_string());
    } else {
        lines.push("red flags:".to_string());
        for flag in &summary.logical_name_red_flags {
            lines.push(format!(
                "- {} reasons={} latest_fetch={} observed_runs={} successful_runs={} red_runs={} consecutive_red_runs={}",
                flag.logical_name,
                flag.reasons.join(","),
                flag.latest_fetch_status.as_deref().unwrap_or("-"),
                flag.observed_runs,
                flag.successful_runs,
                flag.red_runs,
                flag.consecutive_red_runs
            ));
            if let Some(error) = &flag.latest_error {
                lines.push(format!(
                    "  latest_error: [{}] {}",
                    error.code, error.message
                ));
            }
        }
    }

    lines.push("recent runs:".to_string());
    for run in &summary.recent_runs {
        lines.push(format!(
            "#{} {} fetched={} parsed={} imported={} started={} completed={} fetch[{}] parse[{}] dedupe={}",
            run.crawl_run_id,
            run.status,
            run.fetched_targets,
            run.parsed_rows,
            run.imported_rows,
            run.started_at,
            run.completed_at.as_deref().unwrap_or("-"),
            format_count_map(&run.fetch_status_counts),
            format_count_map(&run.parse_level_counts),
            run.dedupe_count
        ));
        if let Some(error) = &run.latest_error {
            lines.push(format!(
                "  last_error: [{}] {}{}",
                error.code,
                error.message,
                error
                    .logical_name
                    .as_deref()
                    .map(|logical_name| format!(" (logical_name={logical_name})"))
                    .unwrap_or_default()
            ));
        }
    }

    lines.join("\n")
}

pub fn format_scaffold_summary(summary: &ScaffoldDomainSummary) -> String {
    format!(
        "scaffold-domain completed: manifest={} fixture={} guide={} source_maturity={} expected_shape={}",
        summary.manifest_path,
        summary.fixture_path,
        summary.guide_path,
        summary.source_maturity,
        summary.expected_shape
    )
}

#[derive(Debug, Clone)]
struct TargetBodyProbe {
    body: String,
    content_type: Option<String>,
    error: Option<String>,
}

fn build_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(20))
        .build()
        .context("failed to build crawler HTTP client")
}

fn build_http_fetch_client() -> Result<HttpFetchClient> {
    HttpFetchClient::from_builder(reqwest::Client::builder().timeout(Duration::from_secs(20)))
        .context("failed to build crawler HTTP fetch client")
}

async fn probe_target_body(
    client: &reqwest::Client,
    url: &str,
    user_agent: &str,
    allowed_domains: &[String],
    robots_body: &str,
) -> TargetBodyProbe {
    let mut current_url = match ensure_allowed_url(url, allowed_domains) {
        Ok(url) => url,
        Err(error) => {
            return TargetBodyProbe {
                body: String::new(),
                content_type: None,
                error: Some(error.to_string()),
            };
        }
    };
    let mut response_result = None;
    for redirect_count in 0..=3 {
        match client
            .get(current_url.clone())
            .header(reqwest::header::USER_AGENT, user_agent)
            .send()
            .await
        {
            Ok(response) if response.status().is_redirection() => {
                if redirect_count >= 3 {
                    return TargetBodyProbe {
                        body: String::new(),
                        content_type: None,
                        error: Some("redirect count exceeded max_redirects 3".to_string()),
                    };
                }
                let Some(location) = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                else {
                    return TargetBodyProbe {
                        body: String::new(),
                        content_type: None,
                        error: Some("redirect response missing Location".to_string()),
                    };
                };
                let next_url = match current_url.join(location) {
                    Ok(url) => url,
                    Err(error) => {
                        return TargetBodyProbe {
                            body: String::new(),
                            content_type: None,
                            error: Some(format!("failed to resolve redirect Location: {error}")),
                        };
                    }
                };
                current_url = match ensure_allowed_url(next_url.as_str(), allowed_domains) {
                    Ok(url) => url,
                    Err(error) => {
                        return TargetBodyProbe {
                            body: String::new(),
                            content_type: None,
                            error: Some(format!(
                                "resolved final_url violated crawler allowlist: {error}"
                            )),
                        };
                    }
                };
            }
            result => {
                response_result = Some(result);
                break;
            }
        }
    }

    match response_result.expect("target probe loop should return a response result") {
        Ok(response) => {
            let status = response.status();
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let parsed_final_url = match ensure_allowed_url(current_url.as_str(), allowed_domains) {
                Ok(url) => url,
                Err(error) => {
                    return TargetBodyProbe {
                        body: String::new(),
                        content_type,
                        error: Some(format!(
                            "resolved final_url violated crawler allowlist: {error}"
                        )),
                    };
                }
            };
            let final_robots = evaluate_robots(robots_body, user_agent, parsed_final_url.path());
            if !final_robots.allowed {
                return TargetBodyProbe {
                    body: String::new(),
                    content_type,
                    error: Some(format!(
                        "resolved final_url is blocked by robots policy{}",
                        final_robots
                            .matched_rule
                            .as_deref()
                            .map(|rule| format!(" ({rule})"))
                            .unwrap_or_default()
                    )),
                };
            }
            match response.text().await {
                Ok(body) if status.is_success() => TargetBodyProbe {
                    body,
                    content_type,
                    error: None,
                },
                Ok(_) => TargetBodyProbe {
                    body: String::new(),
                    content_type,
                    error: Some(format!("target returned HTTP {}", status.as_u16())),
                },
                Err(error) => TargetBodyProbe {
                    body: String::new(),
                    content_type,
                    error: Some(format!("failed to read target body: {error}")),
                },
            }
        }
        Err(error) => TargetBodyProbe {
            body: String::new(),
            content_type: None,
            error: Some(error.to_string()),
        },
    }
}

fn build_manifest_audit(
    manifest_path: &Path,
    manifest: &CrawlSourceManifest,
    parser_version: &str,
) -> Result<SourceManifestAudit> {
    Ok(SourceManifestAudit {
        manifest_path: manifest_path.display().to_string(),
        source_id: manifest.source_id.clone(),
        source_name: manifest.source_name.clone(),
        manifest_version: manifest.manifest_version as i32,
        parser_version: parser_version.to_string(),
        manifest_json: serde_json::to_value(manifest)?,
    })
}

fn resolve_and_validate_targets(
    manifest: &CrawlSourceManifest,
) -> Result<Vec<ResolvedCrawlTarget>> {
    let targets = manifest.resolved_targets()?;
    for target in &targets {
        ensure_allowed_url(&target.url, &manifest.allowlist.allowed_domains)?;
    }
    Ok(targets)
}

fn canonical_manifest_path(path: impl AsRef<Path>) -> Result<PathBuf> {
    fs::canonicalize(path.as_ref()).with_context(|| {
        format!(
            "failed to resolve crawl manifest {}",
            path.as_ref().display()
        )
    })
}

fn list_manifest_paths(manifest_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut manifests = fs::read_dir(manifest_dir)
        .with_context(|| format!("failed to read manifest dir {}", manifest_dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("yaml"))
        .collect::<Vec<_>>();
    manifests.sort();
    Ok(manifests)
}

fn to_event_csv_record(record: &ParsedEventRecord) -> EventCsvRecord {
    EventCsvRecord {
        event_id: record.event_id.clone(),
        school_id: record.school_id.clone(),
        title: record.title.clone(),
        event_category: record.event_category.clone(),
        is_open_day: record.is_open_day,
        is_featured: record.is_featured,
        priority_weight: record.priority_weight,
        starts_at: record.starts_at.clone(),
        placement_tags: record
            .placement_tags
            .iter()
            .map(|placement| placement.as_str().to_string())
            .collect::<Vec<_>>()
            .join("|"),
    }
}

async fn record_parse_report(
    database_url: &str,
    report_count: &mut usize,
    entry: CrawlParseReportEntry,
) -> Result<()> {
    record_crawl_parse_report(database_url, &entry).await?;
    *report_count += 1;
    Ok(())
}

async fn record_dedupe_report(
    database_url: &str,
    crawl_run_id: i64,
    report: DedupeReportEntry,
) -> Result<()> {
    record_crawl_dedupe_report(
        database_url,
        &CrawlDedupeReportEntry {
            crawl_run_id,
            dedupe_key: report.dedupe_key,
            kept_event_id: report.kept_event_id,
            dropped_event_id: report.dropped_event_id,
            reason: report.reason,
            details: report.details,
        },
    )
    .await
}

fn merge_counts(target: &mut BTreeMap<String, i64>, source: &BTreeMap<String, i64>) {
    for (key, value) in source {
        *target.entry(key.clone()).or_insert(0) += value;
    }
}

fn summarize_fetch_status(fetch_logs: &[StoredCrawlFetchLog]) -> Option<String> {
    if fetch_logs.is_empty() {
        return None;
    }

    if fetch_logs
        .iter()
        .any(|log| log.fetch_status == "fetch_failed")
    {
        return Some("fetch_failed".to_string());
    }
    if fetch_logs
        .iter()
        .any(|log| log.fetch_status == "blocked_robots")
    {
        return Some("blocked_robots".to_string());
    }
    if fetch_logs
        .iter()
        .any(|log| log.fetch_status == "blocked_policy")
    {
        return Some("blocked_policy".to_string());
    }
    if fetch_logs
        .iter()
        .all(|log| log.fetch_status == "not_modified")
    {
        return Some("not_modified".to_string());
    }
    if fetch_logs.iter().any(|log| log.fetch_status == "fetched") {
        return Some("fetched".to_string());
    }

    fetch_logs.first().map(|log| log.fetch_status.clone())
}

fn can_deactivate_stale_rows(
    fetch_logs: &[StoredCrawlFetchLog],
    parse_failed_targets: usize,
    zero_row_targets: usize,
) -> bool {
    !fetch_logs.is_empty()
        && parse_failed_targets == 0
        && zero_row_targets == 0
        && fetch_logs
            .iter()
            .all(|entry| matches!(entry.fetch_status.as_str(), "fetched" | "not_modified"))
}

fn summarize_parse_error(
    parse_errors: &[StoredCrawlParseError],
) -> Option<CrawlParseErrorSnapshot> {
    parse_errors.last().map(|error| CrawlParseErrorSnapshot {
        logical_name: Some(error.logical_name.clone()),
        code: error.code.clone(),
        message: error.message.clone(),
    })
}

fn is_red_signal(
    fetch_status: Option<&str>,
    parse_error: Option<&CrawlParseErrorSnapshot>,
    observed: bool,
) -> bool {
    if !observed {
        return true;
    }

    matches!(
        fetch_status,
        Some("fetch_failed" | "blocked_robots" | "blocked_policy")
    ) || parse_error.is_some()
}

fn is_green_signal(
    fetch_status: Option<&str>,
    parse_error: Option<&CrawlParseErrorSnapshot>,
) -> bool {
    matches!(fetch_status, Some("fetched" | "not_modified")) && parse_error.is_none()
}

fn doctor_promotion_gate(summary: &CrawlDoctorSummary) -> PromotionGate {
    let mut gate = PromotionGate::default();

    if summary.source_maturity != SourceMaturity::LiveReady {
        gate.blockers
            .insert("source_maturity_not_live_ready".to_string());
    }
    if !summary.live_fetch_enabled {
        gate.blockers.insert("live_fetch_disabled".to_string());
    }
    if !summary.parser_registered {
        gate.blockers.insert("unknown_parser_key".to_string());
    }

    for issue in &summary.issues {
        if issue.level == "error" || doctor_issue_blocks_promotion(&issue.code) {
            gate.blockers.insert(issue.code.clone());
        } else {
            gate.review.insert(issue.code.clone());
        }
    }

    for target in &summary.targets {
        if matches!(target.robots_allowed, Some(false)) {
            gate.blockers
                .insert(format!("blocked_robots:{}", target.logical_name));
        }
        match target.shape_status.as_deref() {
            Some("matched") | None => {}
            Some("skipped")
                if summary.source_maturity == SourceMaturity::LiveReady
                    && summary.live_fetch_enabled =>
            {
                gate.blockers
                    .insert(format!("shape_probe_skipped:{}", target.logical_name));
            }
            Some(status) => {
                gate.blockers
                    .insert(format!("shape_{status}:{}", target.logical_name));
            }
        }
    }

    gate
}

fn dry_run_promotion_gate(summary: &CrawlDryRunSummary) -> PromotionGate {
    let mut gate = PromotionGate::default();

    if summary.source_maturity != SourceMaturity::LiveReady {
        gate.blockers
            .insert("source_maturity_not_live_ready".to_string());
    }
    if summary.ready_targets == 0 {
        gate.blockers.insert("no_ready_targets".to_string());
    }
    if summary.parsed_rows == 0 {
        gate.blockers.insert("parsed_zero_rows".to_string());
    }
    if summary.imported_rows == 0 {
        gate.blockers.insert("imported_zero_rows".to_string());
    }
    if summary.missing_school_rows > 0 {
        gate.blockers.insert("missing_school_id".to_string());
    }

    for issue in &summary.parse_errors {
        gate.blockers.insert(issue.code.clone());
    }
    for issue in &summary.warnings {
        if dry_run_issue_blocks_promotion(&issue.code) {
            gate.blockers.insert(issue.code.clone());
        } else {
            gate.review.insert(issue.code.clone());
        }
    }

    gate
}

fn health_promotion_gate(summary: &ParserHealthSummary) -> PromotionGate {
    let mut gate = PromotionGate::default();

    if summary.source_maturity != SourceMaturity::LiveReady {
        gate.blockers
            .insert("source_maturity_not_live_ready".to_string());
    }
    if summary.shown_runs == 0 {
        gate.blockers.insert("no_recent_runs".to_string());
    }
    if summary.succeeded_runs == 0 {
        gate.blockers.insert("no_succeeded_runs".to_string());
    }
    if summary.failed_runs > 0 {
        gate.blockers.insert("recent_failed_runs".to_string());
    }
    if summary.active_runs > 0 {
        gate.review.insert("active_runs".to_string());
    }
    if !summary.logical_name_red_flags.is_empty() {
        gate.blockers.insert("logical_name_red_flags".to_string());
    }
    for reason in summary.reason_totals.keys() {
        gate.blockers.insert(format!("reason:{reason}"));
    }

    gate
}

fn doctor_issue_blocks_promotion(code: &str) -> bool {
    matches!(
        code,
        "unknown_parser_key"
            | "live_fetch_disabled"
            | "missing_school_id"
            | "target_shape_fetch_failed"
            | "expected_shape_mismatch"
            | "robots_fetch_failed"
            | "robots_bad_status"
            | "robots_unexpected_content_type"
            | "robots_html_body"
            | "terms_fetch_failed"
            | "terms_bad_status"
    )
}

fn dry_run_issue_blocks_promotion(code: &str) -> bool {
    matches!(
        code,
        "parse_failed"
            | "parsed_zero_rows"
            | "no_events_found"
            | "missing_school_id"
            | "partial_import_skips_stale_deactivation"
            | "missing_school_skips_stale_deactivation"
    )
}

fn format_promotion_gate(gate: &PromotionGate) -> String {
    format!(
        "promotion_gate: {} blockers[{}] review[{}]",
        gate.status(),
        format_string_set(&gate.blockers),
        format_string_set(&gate.review)
    )
}

fn format_count_map(counts: &BTreeMap<String, i64>) -> String {
    if counts.is_empty() {
        return "-".to_string();
    }

    counts
        .iter()
        .map(|(key, value)| format!("{key}:{value}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_string_set(values: &BTreeSet<String>) -> String {
    if values.is_empty() {
        return "-".to_string();
    }

    values.iter().cloned().collect::<Vec<_>>().join(", ")
}

fn normalize_reason_for_total(reason: &str) -> Option<String> {
    match reason {
        "latest_blocked_policy" => Some("blocked_policy".to_string()),
        "latest_blocked_robots" => Some("blocked_robots".to_string()),
        "latest_fetch_failed" => Some("fetch_failed".to_string()),
        "missing_from_latest_run" => Some("missing_from_latest_run".to_string()),
        "no_recent_data" => Some("no_recent_data".to_string()),
        value if value.starts_with("latest_parse_error:") => Some(value.to_string()),
        _ => None,
    }
}

async fn probe_url(
    client: &reqwest::Client,
    url: &str,
    user_agent: &str,
    allowed_domains: &[String],
) -> UrlProbeSummary {
    let mut summary = UrlProbeSummary {
        requested_url: url.to_string(),
        final_url: None,
        http_status: None,
        content_type: None,
        error: None,
        body: None,
        body_preview: None,
    };

    let mut current_url = match ensure_allowed_url(url, allowed_domains) {
        Ok(url) => url,
        Err(error) => {
            summary.error = Some(error.to_string());
            return summary;
        }
    };

    let mut response_result = None;
    for redirect_count in 0..=3 {
        match client
            .get(current_url.clone())
            .header(reqwest::header::USER_AGENT, user_agent)
            .send()
            .await
        {
            Ok(response) if response.status().is_redirection() => {
                if redirect_count >= 3 {
                    summary.error = Some("redirect count exceeded max_redirects 3".to_string());
                    return summary;
                }
                let Some(location) = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                else {
                    summary.error = Some("redirect response missing Location".to_string());
                    return summary;
                };
                let next_url = match current_url.join(location) {
                    Ok(url) => url,
                    Err(error) => {
                        summary.error =
                            Some(format!("failed to resolve redirect Location: {error}"));
                        return summary;
                    }
                };
                current_url = match ensure_allowed_url(next_url.as_str(), allowed_domains) {
                    Ok(url) => url,
                    Err(error) => {
                        summary.final_url = Some(next_url.to_string());
                        summary.error = Some(format!(
                            "resolved final_url violated crawler allowlist: {error}"
                        ));
                        return summary;
                    }
                };
            }
            result => {
                response_result = Some(result);
                break;
            }
        }
    }

    match response_result.expect("probe loop should return a response result") {
        Ok(response) => {
            summary.final_url = Some(current_url.to_string());
            summary.http_status = Some(response.status().as_u16());
            summary.content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            match response.bytes().await {
                Ok(body) => {
                    let full_body = String::from_utf8_lossy(&body).to_string();
                    let preview =
                        String::from_utf8_lossy(&body[..body.len().min(2048)]).to_string();
                    summary.body = Some(full_body);
                    summary.body_preview = Some(preview);
                }
                Err(error) => {
                    summary.error = Some(format!("failed to read response body: {error}"));
                }
            }
        }
        Err(error) => {
            summary.error = Some(error.to_string());
        }
    }

    summary
}

fn collect_url_probe_issues(
    prefix: &str,
    probe: &UrlProbeSummary,
    issues: &mut Vec<DiagnosticIssue>,
) {
    if let Some(error) = &probe.error {
        issues.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: format!("{prefix}_fetch_failed"),
            message: error.clone(),
        });
        return;
    }

    if probe
        .final_url
        .as_deref()
        .is_some_and(|final_url| final_url != probe.requested_url)
    {
        issues.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: format!("{prefix}_redirected"),
            message: format!(
                "{prefix} redirected from {} to {}",
                probe.requested_url,
                probe.final_url.as_deref().unwrap_or("-")
            ),
        });
    }

    if probe.http_status.is_some_and(|status| status >= 400) {
        issues.push(DiagnosticIssue {
            level: "warn".to_string(),
            code: format!("{prefix}_bad_status"),
            message: format!(
                "{prefix} returned HTTP {}",
                probe
                    .http_status
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string())
            ),
        });
    }
}

fn looks_like_html(body: &str) -> bool {
    let lowercase = body.trim().to_ascii_lowercase();
    lowercase.starts_with("<!doctype html") || lowercase.starts_with("<html")
}

fn build_date_drift_warning(logical_name: &str, title: &str, details: &Value) -> Option<String> {
    let month_label = details.get("month_label")?.as_str()?;
    let detail_url_date = details.get("detail_url_date")?.as_str()?;
    let month_from_label = month_label
        .chars()
        .skip_while(|character| !character.is_ascii_digit())
        .take_while(|character| character.is_ascii_digit())
        .collect::<String>()
        .parse::<u32>()
        .ok()?;
    let month_from_url = detail_url_date.get(5..7)?.parse::<u32>().ok()?;

    if month_from_label == month_from_url {
        return None;
    }

    Some(format!(
        "{} title={} month_label={} detail_url_date={}",
        logical_name, title, month_label, detail_url_date
    ))
}

fn is_zero_event_parse_message(message: &str) -> bool {
    [
        "did not find any event cards",
        "did not find any usable event rows",
        "did not find any school-tour rows",
        "did not find any dated event rows",
        "did not find any dated session rows",
        "did not find any schedule entries",
    ]
    .iter()
    .any(|fragment| message.contains(fragment))
}

async fn load_recent_reason_trend(
    database_url: &str,
    runs: &[CrawlRunHealthSnapshot],
) -> Result<Vec<RunReasonTrend>> {
    let mut trend = Vec::new();
    for run in runs {
        let mut reasons = BTreeMap::new();
        for fetch_log in load_crawl_fetch_logs(database_url, run.crawl_run_id).await? {
            if matches!(
                fetch_log.fetch_status.as_str(),
                "blocked_policy" | "blocked_robots" | "fetch_failed"
            ) {
                *reasons.entry(fetch_log.fetch_status).or_insert(0) += 1;
            }
        }
        for parse_error in load_crawl_parse_errors(database_url, run.crawl_run_id).await? {
            *reasons
                .entry(format!("parse_error:{}", parse_error.code))
                .or_insert(0) += 1;
        }
        trend.push(RunReasonTrend {
            crawl_run_id: run.crawl_run_id,
            status: run.status.clone(),
            reasons,
        });
    }

    Ok(trend)
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use axum::{
        extract::State,
        http::StatusCode,
        response::{Html, IntoResponse, Redirect},
        routing::get,
        Router,
    };
    use config::{AppSettings, CandidateRetrievalMode, OpenSearchSettings};
    use crawler_core::{ParserExpectedShape, SourceMaturity};
    use tokio::net::TcpListener;
    use tokio_postgres::NoTls;

    use super::{
        classify_fetch_error_status, crawl_manifest_dir_once, format_doctor_summary,
        format_dry_run_summary, format_health_summary, format_scaffold_summary, run_doctor_command,
        run_dry_run_command, run_fetch_command, run_health_command, run_parse_command,
        scaffold_domain, ScaffoldDomainRequest,
    };

    #[derive(Clone)]
    struct AppState {
        robots_txt: Arc<String>,
        page_html: Arc<String>,
        page_two_html: Option<Arc<String>>,
    }

    #[derive(Clone)]
    struct PartialFetchAppState {
        robots_txt: Arc<String>,
        page_one_html: Arc<String>,
        page_two_html: Arc<String>,
        page_two_requests: Arc<AtomicUsize>,
    }

    fn default_database_url() -> String {
        std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:postgres@127.0.0.1:5433/geo_line_ranker".to_string()
        })
    }

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    fn fixture(name: &str) -> String {
        let path = repo_root().join("storage/fixtures/crawler").join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read fixture {}: {error}", path.display()))
    }

    fn test_settings(raw_storage_dir: &std::path::Path, database_url: &str) -> AppSettings {
        let root = repo_root();
        AppSettings {
            bind_addr: "127.0.0.1:0".to_string(),
            database_url: database_url.to_string(),
            redis_url: None,
            ranking_config_dir: root.join("configs/ranking").display().to_string(),
            fixture_dir: root.join("storage/fixtures/minimal").display().to_string(),
            raw_storage_dir: raw_storage_dir.display().to_string(),
            algorithm_version: "phase6-test".to_string(),
            candidate_retrieval_mode: CandidateRetrievalMode::SqlOnly,
            candidate_retrieval_limit: 256,
            opensearch: OpenSearchSettings {
                url: "http://127.0.0.1:9200".to_string(),
                index_name: "geo_line_ranker_candidates".to_string(),
                username: None,
                password: None,
                request_timeout_secs: 5,
            },
            recommendation_cache_ttl_secs: 60,
            worker_poll_interval_ms: 1000,
            worker_retry_delay_secs: 5,
            worker_max_attempts: 3,
        }
    }

    #[test]
    fn classify_fetch_error_status_marks_policy_failures() {
        for message in [
            "host localhost is outside the crawler allowlist",
            "host 127.0.0.1 is private or local and must be explicitly allowed",
            "unsupported URL scheme ftp for ftp://example.com",
            "response content-type is missing and default policy denies it",
            "response content-type text/plain is outside the crawler allowlist",
            "redirect count exceeded max_redirects 5 for https://example.com",
            "response Content-Length 1048577 exceeds max_response_bytes 1048576 for https://example.com",
            "response body 1048577 bytes exceeds max_response_bytes 1048576 for https://example.com",
        ] {
            assert_eq!(classify_fetch_error_status(message), "blocked_policy");
        }

        assert_eq!(
            classify_fetch_error_status("failed to fetch https://example.com"),
            "fetch_failed"
        );
    }

    #[tokio::test]
    async fn fetch_and_parse_crawl_manifest_imports_events() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Seaside Crawl Open Campus</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("custom_example.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: custom-example-success
source_name: Custom example crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
  placement_tags: [home, detail]
targets:
  - logical_name: custom_example
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;
        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;

        assert_eq!(fetch_summary.fetched_targets, 1);
        assert_eq!(parse_summary.imported_rows, 1);
        assert_eq!(health_summary.total_runs, 1);
        assert_eq!(health_summary.succeeded_runs, 1);
        assert_eq!(health_summary.fetch_status_totals.get("fetched"), Some(&1));
        assert!(health_summary.logical_name_red_flags.is_empty());
        assert_eq!(health_summary.healthy_logical_name_count, 1);
        assert!(
            health_summary
                .parse_level_totals
                .get("info")
                .copied()
                .unwrap_or_default()
                >= 2
        );
        assert!(format_health_summary(&health_summary).contains("single_title_page_v1"));

        let event_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = $1
                   AND is_active = TRUE",
                &[&"custom-example-success"],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(event_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn parse_command_uses_manifest_source_id_as_stable_source_key() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);

        let first_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Seaside First Crawl Open Campus</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };
        let first_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(first_state);
        let first_listener = TcpListener::bind("127.0.0.1:0").await?;
        let first_address = first_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(first_listener, first_app).await;
        });

        let first_manifest_path = temp.path().join("first_source.yaml");
        std::fs::write(
            &first_manifest_path,
            format!(
                r#"
source_id: stable-crawl-source
source_name: Stable crawl source
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
  placement_tags: [home, detail]
targets:
  - logical_name: custom_example
    url: http://127.0.0.1:{port}/events
"#,
                port = first_address.port()
            ),
        )?;
        run_fetch_command(&settings, &first_manifest_path).await?;
        run_parse_command(&settings, &first_manifest_path).await?;

        let second_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Seaside Second Crawl Open Campus</h1><time datetime=\"2026-09-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };
        let second_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(second_state);
        let second_listener = TcpListener::bind("127.0.0.1:0").await?;
        let second_address = second_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(second_listener, second_app).await;
        });

        let second_manifest_path = temp.path().join("renamed_source.yaml");
        std::fs::write(
            &second_manifest_path,
            format!(
                r#"
source_id: stable-crawl-source
source_name: Stable crawl source renamed
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
  placement_tags: [home, detail]
targets:
  - logical_name: custom_example
    url: http://127.0.0.1:{port}/events
"#,
                port = second_address.port()
            ),
        )?;
        run_fetch_command(&settings, &second_manifest_path).await?;
        run_parse_command(&settings, &second_manifest_path).await?;

        let active_titles = client
            .query(
                "SELECT title
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY title ASC",
                &[&"stable-crawl-source"],
            )
            .await?;
        assert_eq!(active_titles.len(), 1);
        assert_eq!(
            active_titles[0].get::<_, String>("title"),
            "Seaside Second Crawl Open Campus"
        );

        let stale_active = client
            .query_one(
                "SELECT is_active
                 FROM events
                 WHERE title = $1",
                &[&"Seaside First Crawl Open Campus"],
            )
            .await?
            .get::<_, bool>("is_active");
        assert!(!stale_active);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_blocks_redirected_final_url_outside_allowlist() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Redirected Allowlist Guard</h1></body></html>".to_string(),
            ),
            page_two_html: None,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let redirected_url = format!("http://localhost:{}/redirected", address.port());
        let redirect_handler = {
            let redirected_url = redirected_url.clone();
            move || {
                let redirected_url = redirected_url.clone();
                async move { Redirect::temporary(&redirected_url) }
            }
        };
        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(redirect_handler))
            .route("/redirected", get(page_handler))
            .with_state(state);
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("redirect_allowlist_guard.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: redirect-allowlist-guard
source_name: Redirect allowlist guard
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: redirected_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let error = run_fetch_command(&settings, &manifest_path)
            .await
            .expect_err("redirect outside allowlist should be blocked");
        assert!(error
            .to_string()
            .contains("no crawl targets were fetched successfully"));

        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        assert_eq!(
            health_summary.fetch_status_totals.get("blocked_policy"),
            Some(&1)
        );
        assert_eq!(health_summary.reason_totals.get("blocked_policy"), Some(&1));

        Ok(())
    }

    #[tokio::test]
    async fn blocked_redirect_keeps_shared_staged_fetch_for_latest_successful_run(
    ) -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let shared_page_html =
            "<html><body><h1>Shared Staged Redirect Guard</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>";

        let initial_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(shared_page_html.to_string()),
            page_two_html: None,
        };
        let initial_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(initial_state);
        let initial_listener = TcpListener::bind("127.0.0.1:0").await?;
        let initial_address = initial_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(initial_listener, initial_app).await;
        });

        let manifest_path = temp.path().join("shared_staged_redirect_guard.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: shared-staged-redirect-guard
source_name: Shared staged redirect guard
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: redirected_page
    url: http://127.0.0.1:{port}/events
"#,
                port = initial_address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;

        let redirected_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(shared_page_html.to_string()),
            page_two_html: None,
        };
        let redirected_listener = TcpListener::bind("127.0.0.1:0").await?;
        let redirected_address = redirected_listener.local_addr()?;
        let redirected_url = format!("http://localhost:{}/redirected", redirected_address.port());
        let redirect_handler = {
            let redirected_url = redirected_url.clone();
            move || {
                let redirected_url = redirected_url.clone();
                async move { Redirect::temporary(&redirected_url) }
            }
        };
        let redirected_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(redirect_handler))
            .route("/redirected", get(page_handler))
            .with_state(redirected_state);
        tokio::spawn(async move {
            let _ = axum::serve(redirected_listener, redirected_app).await;
        });

        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: shared-staged-redirect-guard
source_name: Shared staged redirect guard
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: redirected_page
    url: http://127.0.0.1:{port}/events
"#,
                port = redirected_address.port()
            ),
        )?;

        let blocked_error = run_fetch_command(&settings, &manifest_path)
            .await
            .expect_err("redirect outside allowlist should be blocked");
        assert!(blocked_error
            .to_string()
            .contains("no crawl targets were fetched successfully"));

        let parse_summary = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(parse_summary.imported_rows, 1);

        let active_titles = client
            .query(
                "SELECT title
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = 'shared-staged-redirect-guard'
                   AND is_active = TRUE",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| row.get::<_, String>("title"))
            .collect::<Vec<_>>();
        assert_eq!(
            active_titles,
            vec!["Shared Staged Redirect Guard".to_string()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn fetch_blocks_redirected_final_url_disallowed_by_robots() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nDisallow: /private\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Redirected Robots Guard</h1></body></html>".to_string(),
            ),
            page_two_html: None,
        };

        let redirect_handler = || async { Redirect::temporary("/private/hidden") };
        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(redirect_handler))
            .route("/private/hidden", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("redirect_robots_guard.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: redirect-robots-guard
source_name: Redirect robots guard
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: redirected_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let error = run_fetch_command(&settings, &manifest_path)
            .await
            .expect_err("redirected disallowed path should be blocked");
        assert!(error
            .to_string()
            .contains("no crawl targets were fetched successfully"));

        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        assert_eq!(
            health_summary.fetch_status_totals.get("blocked_robots"),
            Some(&1)
        );
        assert_eq!(health_summary.reason_totals.get("blocked_robots"), Some(&1));

        Ok(())
    }

    #[tokio::test]
    async fn serve_manifest_dir_skips_invalid_manifest_and_crawls_valid_one() -> anyhow::Result<()>
    {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let manifest_dir = temp.path().join("configs/crawler/sources");
        std::fs::create_dir_all(&manifest_dir)?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Serve Loop Guard Event</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        std::fs::write(manifest_dir.join("000_invalid.yaml"), "source_id: [")?;
        let valid_manifest_path = manifest_dir.join("100_valid.yaml");
        std::fs::write(
            &valid_manifest_path,
            format!(
                r#"
source_id: serve-loop-guard
source_name: Serve loop guard
source_maturity: live_ready
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
targets:
  - logical_name: served_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        crawl_manifest_dir_once(&settings, &manifest_dir).await?;

        let source_key = "serve-loop-guard";
        let event_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = $1
                   AND is_active = TRUE",
                &[&source_key],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(event_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn partial_crawl_import_keeps_existing_events_active() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = PartialFetchAppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_one_html: Arc::new(
                "<html><body><h1>Seaside Partial Import Page One</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: Arc::new(
                "<html><body><h1>Seaside Partial Import Page Two</h1><time datetime=\"2026-09-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_requests: Arc::new(AtomicUsize::new(0)),
        };

        let app = Router::new()
            .route("/robots.txt", get(partial_robots_handler))
            .route("/events/page1", get(partial_page_one_handler))
            .route("/events/page2", get(partial_page_two_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("partial_import.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: partial-import-local
source_name: Partial import local crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
  placement_tags: [home, detail]
targets:
  - logical_name: partial_page_one
    url: http://127.0.0.1:{port}/events/page1
  - logical_name: partial_page_two
    url: http://127.0.0.1:{port}/events/page2
"#,
                port = address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        let initial_parse = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(initial_parse.imported_rows, 2);

        run_fetch_command(&settings, &manifest_path).await?;
        let partial_parse = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(partial_parse.imported_rows, 1);

        let source_key = "partial-import-local";
        let rows = client
            .query(
                "SELECT title
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY title ASC",
                &[&source_key],
            )
            .await?;
        let titles = rows
            .into_iter()
            .map(|row| row.get::<_, String>("title"))
            .collect::<Vec<_>>();
        assert_eq!(titles.len(), 2);
        assert!(titles.contains(&"Seaside Partial Import Page One".to_string()));
        assert!(titles.contains(&"Seaside Partial Import Page Two".to_string()));

        let warning_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM crawl_parse_reports
                 WHERE crawl_run_id = $1
                   AND code = 'crawl_skipped_stale_deactivation'",
                &[&partial_parse.crawl_run_id],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(warning_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn missing_school_rows_do_not_deactivate_existing_crawl_events() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Seaside Missing School Guard</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };
        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("missing_school_guard.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: missing-school-guard
source_name: Missing school guard crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
targets:
  - logical_name: missing_school_guard_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        let initial_parse = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(initial_parse.imported_rows, 1);

        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: missing-school-guard
source_name: Missing school guard crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_missing
  event_category: open_campus
  is_open_day: true
targets:
  - logical_name: missing_school_guard_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        let dry_run = run_dry_run_command(&settings, &manifest_path).await?;
        assert_eq!(dry_run.imported_rows, 0);
        assert_eq!(dry_run.deactivated_rows, 0);
        assert_eq!(dry_run.missing_school_rows, 1);
        assert!(dry_run
            .warnings
            .iter()
            .any(|issue| issue.code == "missing_school_skips_stale_deactivation"));

        let guarded_parse = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(guarded_parse.imported_rows, 0);

        let source_key = "missing-school-guard";
        let active_titles = client
            .query(
                "SELECT title
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY title ASC",
                &[&source_key],
            )
            .await?
            .into_iter()
            .map(|row| row.get::<_, String>("title"))
            .collect::<Vec<_>>();
        assert_eq!(
            active_titles,
            vec!["Seaside Missing School Guard".to_string()]
        );

        let warning_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM crawl_parse_reports
                 WHERE crawl_run_id = $1
                   AND code = 'crawl_skipped_missing_school_deactivation'",
                &[&guarded_parse.crawl_run_id],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(warning_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_and_parse_shibaura_manifest_imports_seeded_school() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                r#"
<html>
  <body>
    <div class="qua-container">
      <h4 class="qua-wysiwyg-content"><p>本校の教育内容（学校説明会）【オンライン】※要予約</p></h4>
      <div class="qua-unit-text">
        <div class="qua-wysiwyg-content">
          <p>オンラインで実施する中学説明会</p>
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
      <h4 class="qua-wysiwyg-content"><p>教員による学校見学会＊要予約</p></h4>
      <div class="qua-field-list">
        <ul>
          <li class="qua-field-list__item"><div class="qua-field-list__item__in"><p><strong>＜5月＞：2026年5月11日 (月)、13日（水）</strong></p></div></li>
        </ul>
      </div>
    </div>
  </body>
</html>
"#
                .to_string(),
            ),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("shibaura_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: shibaura-local
source_name: Shibaura local crawler
parser_key: shibaura_junior_event_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_shibaura_it_junior
  event_category: admission_event
  placement_tags: [search, detail]
targets:
  - logical_name: junior_event_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;

        assert_eq!(fetch_summary.fetched_targets, 1);
        assert_eq!(parse_summary.imported_rows, 4);

        let rows = client
            .query(
                "SELECT title, to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS starts_at
                 FROM events
                 WHERE source_type = 'crawl'
                   AND school_id = 'school_shibaura_it_junior'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY starts_at ASC, title ASC",
                &[&"shibaura-local"],
            )
            .await?;
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows[0].get::<_, String>("title"),
            "オンラインで実施する中学説明会 第1回"
        );
        assert_eq!(
            rows[0].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2026-05-09")
        );
        assert_eq!(
            rows[1].get::<_, String>("title"),
            "教員による学校見学会 ＜5月＞"
        );

        Ok(())
    }

    #[tokio::test]
    async fn fetch_and_parse_nihon_manifest_imports_seeded_school() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(fixture("nihon_university_junior_info_session.html")),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/info-session", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("nihon_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: nihon-local
source_name: Nihon local crawler
parser_key: nihon_university_junior_info_session_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_nihon_university_junior
  event_category: admission_event
  placement_tags: [search, detail]
targets:
  - logical_name: junior_info_session_page
    url: http://127.0.0.1:{port}/info-session
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;
        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        let health_text = format_health_summary(&health_summary);

        assert_eq!(fetch_summary.fetched_targets, 1);
        assert_eq!(parse_summary.imported_rows, 8);
        assert!(health_summary.logical_name_red_flags.is_empty());
        assert!(health_summary.reason_totals.is_empty());
        assert!(health_text.contains("reason totals: -"));

        let rows = client
            .query(
                "SELECT title, to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS starts_at
                 FROM events
                 WHERE source_type = 'crawl'
                   AND school_id = 'school_nihon_university_junior'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY starts_at ASC, title ASC",
                &[&"nihon-local"],
            )
            .await?;
        assert_eq!(rows.len(), 8);
        assert_eq!(rows[0].get::<_, String>("title"), "外部フェア");
        assert_eq!(
            rows[0].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2025-07-11")
        );
        assert_eq!(rows[3].get::<_, String>("title"), "文化祭");
        assert_eq!(rows[4].get::<_, String>("title"), "学校（入試）説明会");
        assert_eq!(
            rows[7].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2027-01-16")
        );

        Ok(())
    }

    #[tokio::test]
    async fn fetch_and_parse_aoyama_manifest_imports_seeded_school() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(fixture("aoyama_junior_school_tour.html")),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/admission/explanation.html", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("aoyama_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: aoyama-local
source_name: Aoyama local crawler
source_maturity: live_ready
parser_key: aoyama_junior_school_tour_v1
expected_shape: html_school_tour_blocks
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_aoyama_gakuin_junior
  event_category: admission_event
  is_open_day: true
  placement_tags: [search, detail]
targets:
  - logical_name: school_tour_page
    url: http://127.0.0.1:{port}/admission/explanation.html
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;
        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        let health_text = format_health_summary(&health_summary);

        assert_eq!(fetch_summary.fetched_targets, 1);
        assert_eq!(parse_summary.imported_rows, 10);
        assert!(health_summary.logical_name_red_flags.is_empty());
        assert!(health_summary.reason_totals.is_empty());
        assert!(health_text.contains("source_maturity: live_ready"));
        assert!(health_text.contains("expected_shape: html_school_tour_blocks"));

        let rows = client
            .query(
                "SELECT title, to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS starts_at
                 FROM events
                 WHERE source_type = 'crawl'
                   AND school_id = 'school_aoyama_gakuin_junior'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY starts_at ASC, title ASC",
                &[&"aoyama-local"],
            )
            .await?;
        assert_eq!(rows.len(), 10);
        assert_eq!(
            rows[0].get::<_, String>("title"),
            "キリスト教学校合同フェア"
        );
        assert_eq!(
            rows[0].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2026-03-20")
        );
        assert!(rows.iter().any(|row| {
            row.get::<_, String>("title") == "学校説明会 第1回"
                && row.get::<_, Option<String>>("starts_at").as_deref() == Some("2026-06-13")
        }));

        let tokyo_private_school_expo_dates = rows
            .iter()
            .filter(|row| row.get::<_, String>("title") == "東京都私立学校展 （※資料参加のみ）")
            .map(|row| {
                row.get::<_, Option<String>>("starts_at")
                    .expect("starts_at")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            tokyo_private_school_expo_dates,
            vec!["2026-08-29".to_string(), "2026-08-30".to_string()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn doctor_reports_html_robots_and_school_presence() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler doctor test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new(String::new()),
            page_html: Arc::new(fixture("nihon_university_junior_info_session.html")),
            page_two_html: None,
        };

        let app = Router::new()
            .route(
                "/robots.txt",
                get(|| async { Redirect::temporary("/robots-home") }),
            )
            .route(
                "/robots-home",
                get(|| async { Html("<html><body>home</body></html>") }),
            )
            .route(
                "/terms",
                get(|| async { Html("<html><body>terms</body></html>") }),
            )
            .route("/info-session", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("doctor_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: doctor-local
source_name: Doctor local crawler
parser_key: nihon_university_junior_info_session_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: http://127.0.0.1:{port}/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_nihon_university_junior
  event_category: admission_event
targets:
  - logical_name: junior_info_session_page
    url: http://127.0.0.1:{port}/info-session
"#,
                port = address.port()
            ),
        )?;

        let summary = run_doctor_command(&settings, &manifest_path).await?;
        let doctor_text = format_doctor_summary(&summary);

        assert!(summary.parser_registered);
        assert_eq!(summary.source_maturity, SourceMaturity::LiveReady);
        assert_eq!(
            summary.expected_shape,
            Some(ParserExpectedShape::HtmlMonthlyDlPairs)
        );
        assert_eq!(summary.targets.len(), 1);
        assert_eq!(summary.targets[0].school_exists, Some(true));
        assert_eq!(summary.targets[0].shape_status.as_deref(), Some("matched"));
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.code == "robots_redirected"));
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.code == "robots_unexpected_content_type"));
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.code == "robots_html_body"));
        assert!(doctor_text.contains("targets:"));
        assert!(doctor_text.contains("school_exists=true"));
        assert!(doctor_text.contains("source_maturity: live_ready"));
        assert!(doctor_text.contains("expected_shape: html_monthly_dl_pairs"));

        Ok(())
    }

    #[tokio::test]
    async fn doctor_flags_expected_shape_mismatch() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler doctor mismatch test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new("<html><body><p>shape mismatch</p></body></html>".to_string()),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route(
                "/terms",
                get(|| async { Html("<html><body>terms</body></html>") }),
            )
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("doctor_shape_mismatch.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: doctor-shape-mismatch
source_name: Doctor shape mismatch crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: http://127.0.0.1:{port}/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: broken_shape
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let summary = run_doctor_command(&settings, &manifest_path).await?;
        let doctor_text = format_doctor_summary(&summary);

        assert_eq!(
            summary.expected_shape,
            Some(ParserExpectedShape::HtmlHeadingPage)
        );
        assert_eq!(summary.targets[0].shape_status.as_deref(), Some("mismatch"));
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.code == "expected_shape_mismatch"));
        assert!(doctor_text.contains("promotion_gate: blocked"));
        assert!(doctor_text.contains("expected_shape_mismatch"));
        assert!(doctor_text.contains("shape=mismatch"));
        assert!(doctor_text.contains("expected_shape: html_heading_page"));

        Ok(())
    }

    #[tokio::test]
    async fn doctor_surfaces_school_lookup_failures() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let mut settings = test_settings(
            &temp.path().join("raw"),
            "postgres://127.0.0.1:9/geo_line_ranker",
        );
        settings.database_url = "postgres://127.0.0.1:9/geo_line_ranker".to_string();
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Doctor School Lookup Warning</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route(
                "/terms",
                get(|| async { Html("<html><body>terms</body></html>") }),
            )
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("doctor_school_lookup_failed.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: doctor-school-lookup-failed
source_name: Doctor school lookup failed
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: http://127.0.0.1:{port}/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: school_lookup_warning
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let summary = run_doctor_command(&settings, &manifest_path).await?;
        let doctor_text = format_doctor_summary(&summary);

        assert_eq!(summary.targets.len(), 1);
        assert_eq!(summary.targets[0].school_exists, None);
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.code == "school_lookup_failed"));
        assert!(doctor_text.contains("school_exists=unknown"));
        assert!(doctor_text.contains("school_lookup_failed"));

        Ok(())
    }

    #[tokio::test]
    async fn doctor_skips_shape_probe_when_robots_policy_is_unknown() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler doctor robots test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let probe_requests = Arc::new(AtomicUsize::new(0));

        let app = Router::new()
            .route(
                "/robots.txt",
                get(|| async {
                    (
                        StatusCode::OK,
                        [("content-type", "text/plain; charset=utf-8")],
                        "",
                    )
                }),
            )
            .route(
                "/terms",
                get(|| async { Html("<html><body>terms</body></html>") }),
            )
            .route(
                "/events",
                get({
                    let probe_requests = probe_requests.clone();
                    move || {
                        let probe_requests = probe_requests.clone();
                        async move {
                            probe_requests.fetch_add(1, Ordering::SeqCst);
                            (
                                StatusCode::OK,
                                [("content-type", "text/html; charset=utf-8")],
                                "<html><body><h1>Doctor Probe Should Not Run</h1></body></html>",
                            )
                        }
                    }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("doctor_unknown_robots.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: doctor-unknown-robots
source_name: Doctor unknown robots
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: http://127.0.0.1:{port}/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: unknown_robots_target
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let summary = run_doctor_command(&settings, &manifest_path).await?;

        assert_eq!(summary.targets.len(), 1);
        assert_eq!(summary.targets[0].robots_allowed, None);
        assert_eq!(summary.targets[0].shape_status.as_deref(), Some("skipped"));
        assert_eq!(
            summary.targets[0].shape_detail.as_deref(),
            Some("robots policy could not be evaluated for this target")
        );
        assert_eq!(probe_requests.load(Ordering::SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn doctor_revalidates_redirected_shape_probe_against_allowlist() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler doctor redirect test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Doctor Redirect Guard</h1></body></html>".to_string(),
            ),
            page_two_html: None,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let redirected_url = format!("http://localhost:{}/redirected", address.port());
        let redirect_handler = {
            let redirected_url = redirected_url.clone();
            move || {
                let redirected_url = redirected_url.clone();
                async move { Redirect::temporary(&redirected_url) }
            }
        };
        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route(
                "/terms",
                get(|| async { Html("<html><body>terms</body></html>") }),
            )
            .route("/events", get(redirect_handler))
            .route("/redirected", get(page_handler))
            .with_state(state);
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("doctor_redirect_allowlist_guard.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: doctor-redirect-allowlist-guard
source_name: Doctor redirect allowlist guard
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: http://127.0.0.1:{port}/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: redirected_target
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let summary = run_doctor_command(&settings, &manifest_path).await?;

        assert_eq!(summary.targets.len(), 1);
        assert_eq!(
            summary.targets[0].shape_status.as_deref(),
            Some("fetch_failed")
        );
        assert!(summary.targets[0]
            .shape_detail
            .as_deref()
            .unwrap_or_default()
            .contains("resolved final_url violated crawler allowlist"));
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.code == "target_shape_fetch_failed"));

        Ok(())
    }

    #[tokio::test]
    async fn doctor_evaluates_robots_from_full_response_body() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!(
                "skipping crawler doctor robots-body test because PostgreSQL is not reachable"
            );
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let probe_requests = Arc::new(AtomicUsize::new(0));
        let long_robots = format!(
            "User-agent: *\n{}Disallow: /blocked\n",
            (0..400)
                .map(|index| format!("Allow: /padding-{index:04}\n"))
                .collect::<String>()
        );

        let app = Router::new()
            .route(
                "/robots.txt",
                get({
                    let long_robots = long_robots.clone();
                    move || {
                        let long_robots = long_robots.clone();
                        async move {
                            (
                                StatusCode::OK,
                                [("content-type", "text/plain; charset=utf-8")],
                                long_robots,
                            )
                        }
                    }
                }),
            )
            .route(
                "/terms",
                get(|| async { Html("<html><body>terms</body></html>") }),
            )
            .route(
                "/blocked/events",
                get({
                    let probe_requests = probe_requests.clone();
                    move || {
                        let probe_requests = probe_requests.clone();
                        async move {
                            probe_requests.fetch_add(1, Ordering::SeqCst);
                            (
                                StatusCode::OK,
                                [("content-type", "text/html; charset=utf-8")],
                                "<html><body><h1>Doctor Probe Should Stay Blocked</h1></body></html>",
                            )
                        }
                    }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("doctor_full_robots_body.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: doctor-full-robots-body
source_name: Doctor full robots body
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: http://127.0.0.1:{port}/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: blocked_target
    url: http://127.0.0.1:{port}/blocked/events
"#,
                port = address.port()
            ),
        )?;

        let summary = run_doctor_command(&settings, &manifest_path).await?;

        assert_eq!(summary.targets.len(), 1);
        assert_eq!(summary.targets[0].robots_allowed, Some(false));
        assert_eq!(summary.targets[0].shape_status.as_deref(), Some("skipped"));
        assert_eq!(
            summary.targets[0].shape_detail.as_deref(),
            Some("target is blocked by robots policy")
        );
        assert_eq!(probe_requests.load(Ordering::SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn dry_run_predicts_import_inactive_and_date_drift() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler dry-run test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);

        let initial_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(fixture("nihon_university_junior_info_session.html")),
            page_two_html: None,
        };
        let initial_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/info-session", get(page_handler))
            .with_state(initial_state);
        let initial_listener = TcpListener::bind("127.0.0.1:0").await?;
        let initial_address = initial_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(initial_listener, initial_app).await;
        });

        let manifest_path = temp.path().join("dry_run_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: dry-run-local
source_name: Dry-run local crawler
parser_key: nihon_university_junior_info_session_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_nihon_university_junior
  event_category: admission_event
  placement_tags: [search, detail]
targets:
  - logical_name: junior_info_session_page
    url: http://127.0.0.1:{port}/info-session
"#,
                port = initial_address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        run_parse_command(&settings, &manifest_path).await?;

        let drift_html = r#"
<html>
  <body>
    <div class="schedule_box">
      <h3 class="ttl">6月</h3>
      <dl class="text_box">
        <dt>1日（月）9:15～10:15</dt>
        <dd>
          <p class="event_name">ミニ説明会</p>
          <div class="btn_box">
            <p class="link_btn blank is-junior"><a href="/assets/pdf/info-session/junior_enent_20260701.pdf" target="_blank">詳細</a></p>
            <p class="link_btn blank is-junior"><a href="https://mirai-compass.net/usr/nihonuj/common/loginEvent.jsf" target="_blank">申込み</a></p>
          </div>
        </dd>
      </dl>
    </div>
  </body>
</html>
"#;

        let second_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(drift_html.to_string()),
            page_two_html: None,
        };
        let second_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/info-session", get(page_handler))
            .with_state(second_state);
        let second_listener = TcpListener::bind("127.0.0.1:0").await?;
        let second_address = second_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(second_listener, second_app).await;
        });

        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: dry-run-local
source_name: Dry-run local crawler
parser_key: nihon_university_junior_info_session_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_nihon_university_junior
  event_category: admission_event
  placement_tags: [search, detail]
targets:
  - logical_name: junior_info_session_page
    url: http://127.0.0.1:{port}/info-session
"#,
                port = second_address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        let summary = run_dry_run_command(&settings, &manifest_path).await?;
        let dry_run_text = format_dry_run_summary(&summary);

        assert_eq!(summary.parsed_rows, 1);
        assert_eq!(summary.deduped_rows, 1);
        assert_eq!(summary.imported_rows, 1);
        assert_eq!(summary.deactivated_rows, 8);
        assert_eq!(summary.date_drift_warnings, 1);
        assert!(summary
            .warnings
            .iter()
            .any(|issue| issue.code == "date_drift"));
        assert!(dry_run_text.contains("promotion_gate: review"));
        assert!(dry_run_text.contains("review[date_drift]"));
        assert!(dry_run_text.contains("inactive=8"));
        assert!(dry_run_text.contains("date_drift"));

        Ok(())
    }

    #[tokio::test]
    async fn fetch_and_parse_keio_manifest_imports_seeded_school() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(fixture("keio_event_listing_page_1.html")),
            page_two_html: Some(Arc::new(fixture("keio_event_listing_page_2.html"))),
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events/page1", get(page_handler))
            .route("/events/page2", get(page_two_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("keio_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: keio-local
source_name: Keio local crawler
parser_key: keio_event_listing_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_keio
  event_category: general
  placement_tags: [search, detail]
targets:
  - logical_name: event_page_1
    url: http://127.0.0.1:{port}/events/page1
  - logical_name: event_page_2
    url: http://127.0.0.1:{port}/events/page2
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;

        assert_eq!(fetch_summary.fetched_targets, 2);
        assert_eq!(parse_summary.imported_rows, 4);

        let rows = client
            .query(
                "SELECT title, to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS starts_at
                 FROM events
                 WHERE source_type = 'crawl'
                   AND school_id = 'school_keio'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY starts_at ASC, title ASC",
                &[&"keio-local"],
            )
            .await?;
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows[0].get::<_, String>("title"),
            "慶應義塾ミュージアム・コモンズ 展示"
        );
        assert_eq!(
            rows[0].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2026-03-09")
        );
        assert_eq!(
            rows[2].get::<_, String>("title"),
            "オープンキャンパス2026～講義編～"
        );
        assert_eq!(
            rows[3].get::<_, String>("title"),
            "ニューヨーク学院（高等部）学院説明会（シンガポール）"
        );

        Ok(())
    }

    #[tokio::test]
    async fn parse_zero_event_targets_keep_existing_rows_active() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler zero-event parse test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);

        let initial_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(fixture("keio_event_listing_page_1.html")),
            page_two_html: Some(Arc::new(fixture("keio_event_listing_page_2.html"))),
        };
        let initial_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events/page1", get(page_handler))
            .route("/events/page2", get(page_two_handler))
            .with_state(initial_state);
        let initial_listener = TcpListener::bind("127.0.0.1:0").await?;
        let initial_address = initial_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(initial_listener, initial_app).await;
        });

        let manifest_path = temp.path().join("keio_zero_events.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: keio-zero-events
source_name: Keio zero-events crawler
parser_key: keio_event_listing_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_keio
  event_category: general
  placement_tags: [search, detail]
targets:
  - logical_name: event_page_1
    url: http://127.0.0.1:{port}/events/page1
  - logical_name: event_page_2
    url: http://127.0.0.1:{port}/events/page2
"#,
                port = initial_address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        let initial_parse = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(initial_parse.imported_rows, 4);

        let empty_state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new("<html><body><p>No events right now</p></body></html>".to_string()),
            page_two_html: Some(Arc::new(
                "<html><body><p>No events right now</p></body></html>".to_string(),
            )),
        };
        let empty_app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events/page1", get(page_handler))
            .route("/events/page2", get(page_two_handler))
            .with_state(empty_state);
        let empty_listener = TcpListener::bind("127.0.0.1:0").await?;
        let empty_address = empty_listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(empty_listener, empty_app).await;
        });

        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: keio-zero-events
source_name: Keio zero-events crawler
parser_key: keio_event_listing_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_keio
  event_category: general
  placement_tags: [search, detail]
targets:
  - logical_name: event_page_1
    url: http://127.0.0.1:{port}/events/page1
  - logical_name: event_page_2
    url: http://127.0.0.1:{port}/events/page2
"#,
                port = empty_address.port()
            ),
        )?;

        run_fetch_command(&settings, &manifest_path).await?;
        let empty_dry_run = run_dry_run_command(&settings, &manifest_path).await?;
        assert_eq!(empty_dry_run.parsed_rows, 0);
        assert_eq!(empty_dry_run.imported_rows, 0);
        assert_eq!(empty_dry_run.deactivated_rows, 0);
        assert!(empty_dry_run
            .warnings
            .iter()
            .any(|issue| issue.code == "parsed_zero_rows"));
        assert!(empty_dry_run
            .warnings
            .iter()
            .any(|issue| issue.code == "no_events_found"));
        assert!(empty_dry_run
            .warnings
            .iter()
            .any(|issue| issue.code == "partial_import_skips_stale_deactivation"));

        let empty_parse = run_parse_command(&settings, &manifest_path).await?;
        assert_eq!(empty_parse.parsed_rows, 0);
        assert_eq!(empty_parse.imported_rows, 0);

        let active_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM events
                 WHERE source_type = 'crawl'
                   AND source_key = $1
                   AND is_active = TRUE",
                &[&"keio-zero-events"],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(active_count, 4);

        let warning_count = client
            .query_one(
                "SELECT COUNT(*) AS count
                 FROM crawl_parse_reports
                 WHERE crawl_run_id = $1
                   AND code = 'crawl_skipped_stale_deactivation'",
                &[&empty_parse.crawl_run_id],
            )
            .await?
            .get::<_, i64>("count");
        assert_eq!(warning_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_and_parse_hachioji_manifest_imports_seeded_school() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(fixture("hachioji_junior_session_tables.html")),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("hachioji_local.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: hachioji-local
source_name: Hachioji local crawler
parser_key: hachioji_junior_session_tables_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_hachioji_gakuen_junior
  event_category: admission_event
  placement_tags: [search, detail]
targets:
  - logical_name: junior_session_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;
        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        let health_text = format_health_summary(&health_summary);

        assert_eq!(fetch_summary.fetched_targets, 1);
        assert_eq!(parse_summary.imported_rows, 5);
        assert!(health_summary.logical_name_red_flags.is_empty());
        assert!(health_summary.reason_totals.is_empty());
        assert!(health_text.contains("promotion_gate: ready"));

        let rows = client
            .query(
                "SELECT title, to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS starts_at
                 FROM events
                 WHERE source_type = 'crawl'
                   AND school_id = 'school_hachioji_gakuen_junior'
                   AND source_key = $1
                   AND is_active = TRUE
                 ORDER BY starts_at ASC, title ASC",
                &[&"hachioji-local"],
            )
            .await?;
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].get::<_, String>("title"), "保護者対象説明会");
        assert_eq!(
            rows[0].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2026-05-07")
        );
        assert_eq!(
            rows[4].get::<_, Option<String>>("starts_at").as_deref(),
            Some("2027-01-09")
        );

        Ok(())
    }

    #[tokio::test]
    async fn health_flags_logical_name_with_recent_fetch_failure() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new(
                "<html><body><h1>Seaside Crawl Open Campus</h1><time datetime=\"2026-08-01T10:00:00+09:00\"></time></body></html>"
                    .to_string(),
            ),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("custom_with_failure.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: custom-example-failure
source_name: Custom example crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
  placement_tags: [home, detail]
targets:
  - logical_name: healthy_page
    url: http://127.0.0.1:{port}/events
  - logical_name: broken_page
    url: http://127.0.0.1:{port}/missing
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        let parse_summary = run_parse_command(&settings, &manifest_path).await?;
        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        let health_text = format_health_summary(&health_summary);

        assert_eq!(fetch_summary.fetched_targets, 1);
        assert_eq!(parse_summary.imported_rows, 1);
        assert_eq!(health_summary.logical_name_red_flags.len(), 1);
        assert_eq!(health_summary.healthy_logical_name_count, 1);
        assert_eq!(health_summary.reason_totals.get("fetch_failed"), Some(&1));
        assert!(health_text.contains("recent reason trend:"));
        assert!(health_text.contains("fetch_failed:1"));
        assert_eq!(
            health_summary.logical_name_red_flags[0].logical_name,
            "broken_page"
        );
        assert!(health_summary.logical_name_red_flags[0]
            .reasons
            .contains(&"latest_fetch_failed".to_string()));
        assert!(health_text.contains("reason totals: fetch_failed:1"));
        assert!(health_text.contains("broken_page"));
        assert!(health_text.contains("latest_fetch_failed"));

        Ok(())
    }

    #[tokio::test]
    async fn health_counts_latest_parse_error_reasons() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let state = AppState {
            robots_txt: Arc::new("User-agent: *\nAllow: /\n".to_string()),
            page_html: Arc::new("<html><body><p>heading missing</p></body></html>".to_string()),
            page_two_html: None,
        };

        let app = Router::new()
            .route("/robots.txt", get(robots_handler))
            .route("/events", get(page_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let manifest_path = temp.path().join("custom_parse_error.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: custom-parse-error
source_name: Custom parse error crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
  is_open_day: true
targets:
  - logical_name: parse_error_page
    url: http://127.0.0.1:{port}/events
"#,
                port = address.port()
            ),
        )?;

        let fetch_summary = run_fetch_command(&settings, &manifest_path).await?;
        assert_eq!(fetch_summary.fetched_targets, 1);
        let error = run_parse_command(&settings, &manifest_path)
            .await
            .expect_err("parse should fail");
        assert!(error
            .to_string()
            .contains("no crawler events were parsed successfully"));

        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        let health_text = format_health_summary(&health_summary);

        assert_eq!(
            health_summary
                .reason_totals
                .get("latest_parse_error:parse_failed"),
            Some(&1)
        );
        assert!(health_text.contains("recent reason trend:"));
        assert!(health_text.contains("parse_error:parse_failed:1"));
        assert!(health_text.contains("latest_parse_error:parse_failed:1"));
        assert_eq!(health_summary.logical_name_red_flags.len(), 1);
        assert!(health_summary.logical_name_red_flags[0]
            .reasons
            .contains(&"latest_parse_error:parse_failed".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn health_flags_manifest_policy_block() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let manifest_path = temp.path().join("policy_blocked.yaml");
        std::fs::write(
            &manifest_path,
            r#"
source_id: policy-blocked
source_name: Policy blocked crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["example.com"]
  user_agent: geo-line-ranker-crawler/0.1
  live_fetch_enabled: false
  live_fetch_block_reason: robots URL is not published yet.
  robots_txt_url: https://example.com/robots.txt
  terms_url: https://example.com/terms
  terms_note: Manual review completed.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: blocked_page
    url: https://example.com/events
"#,
        )?;

        let error = run_fetch_command(&settings, &manifest_path)
            .await
            .expect_err("policy-blocked fetch");
        assert!(error
            .to_string()
            .contains("live fetch disabled by manifest policy"));

        let health_summary = run_health_command(&settings, &manifest_path, 10).await?;
        let health_text = format_health_summary(&health_summary);

        assert_eq!(health_summary.failed_runs, 1);
        assert_eq!(
            health_summary.source_maturity,
            SourceMaturity::PolicyBlocked
        );
        assert_eq!(
            health_summary.expected_shape,
            Some(ParserExpectedShape::HtmlHeadingPage)
        );
        assert_eq!(
            health_summary.fetch_status_totals.get("blocked_policy"),
            Some(&1)
        );
        assert_eq!(health_summary.logical_name_red_flags.len(), 1);
        assert_eq!(
            health_summary.logical_name_red_flags[0].logical_name,
            "blocked_page"
        );
        assert_eq!(health_summary.reason_totals.get("blocked_policy"), Some(&1));
        assert!(health_text.contains("recent reason trend:"));
        assert!(health_text.contains("blocked_policy:1"));
        assert!(health_summary.logical_name_red_flags[0]
            .reasons
            .contains(&"latest_blocked_policy".to_string()));
        assert!(health_text.contains("reason totals: blocked_policy:1"));
        assert!(health_text.contains("latest_blocked_policy"));
        assert!(health_text.contains("source_maturity: policy_blocked"));
        assert!(health_text.contains("expected_shape: html_heading_page"));
        assert!(health_text.contains("promotion_gate: blocked"));
        assert!(health_text.contains("source_maturity_not_live_ready"));

        Ok(())
    }

    #[tokio::test]
    async fn robots_bootstrap_failure_marks_crawl_run_failed() -> anyhow::Result<()> {
        let database_url = default_database_url();
        let Ok((client, connection)) = tokio_postgres::connect(&database_url, NoTls).await else {
            eprintln!("skipping crawler integration test because PostgreSQL is not reachable");
            return Ok(());
        };
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query("SELECT 1").await?;

        let root = repo_root();
        storage_postgres::run_migrations(&database_url, root.join("storage/migrations/postgres"))
            .await?;
        storage_postgres::seed_fixture(&database_url, root.join("storage/fixtures/minimal"))
            .await?;

        let temp = tempfile::tempdir()?;
        let settings = test_settings(&temp.path().join("raw"), &database_url);
        let closed_listener = TcpListener::bind("127.0.0.1:0").await?;
        let closed_port = closed_listener.local_addr()?.port();
        drop(closed_listener);

        let manifest_path = temp.path().join("robots_bootstrap_failure.yaml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
source_id: robots-bootstrap-failure
source_name: Robots bootstrap failure crawler
parser_key: single_title_page_v1
allowlist:
  allowed_domains: ["127.0.0.1"]
  user_agent: geo-line-ranker-crawler/0.1
  min_fetch_interval_ms: 1
  robots_txt_url: http://127.0.0.1:{port}/robots.txt
  terms_url: https://example.com/terms
  terms_note: Test-only local source.
defaults:
  school_id: school_seaside
  event_category: open_campus
targets:
  - logical_name: robots_bootstrap_failure_page
    url: http://127.0.0.1:{port}/events
"#,
                port = closed_port
            ),
        )?;

        let error = run_fetch_command(&settings, &manifest_path)
            .await
            .expect_err("robots bootstrap should fail");
        assert!(
            error.to_string().contains("failed"),
            "unexpected fetch error: {error}"
        );

        let manifest_key = manifest_path.canonicalize()?.display().to_string();
        let status = client
            .query_one(
                "SELECT status
                 FROM crawl_runs
                 WHERE manifest_path = $1
                 ORDER BY id DESC
                 LIMIT 1",
                &[&manifest_key],
            )
            .await?
            .get::<_, String>("status");
        assert_eq!(status, "failed");

        Ok(())
    }

    #[test]
    fn scaffold_domain_writes_manifest_fixture_and_guide() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let summary = scaffold_domain(ScaffoldDomainRequest {
            source_id: "sample-domain".to_string(),
            source_name: "Sample Domain Events".to_string(),
            school_id: "school_sample".to_string(),
            parser_key: "sample_parser_v1".to_string(),
            source_maturity: SourceMaturity::ParserOnly,
            expected_shape: ParserExpectedShape::HtmlMonthlyDlPairs,
            target_url: "https://example.com/events".to_string(),
            logical_name: None,
            manifest_dir: temp.path().join("configs/crawler/sources"),
            fixture_dir: temp.path().join("storage/fixtures/crawler"),
            guide_dir: temp.path().join("docs/crawler_scaffolds"),
            force: false,
        })?;
        let summary_text = format_scaffold_summary(&summary);

        let manifest = std::fs::read_to_string(&summary.manifest_path)?;
        let fixture = std::fs::read_to_string(&summary.fixture_path)?;
        let guide = std::fs::read_to_string(&summary.guide_path)?;

        assert!(manifest.contains("source_maturity: parser_only"));
        assert!(manifest.contains("expected_shape: html_monthly_dl_pairs"));
        assert!(manifest.contains("live_fetch_enabled: false"));
        assert!(manifest.contains("terms_url: https://example.com/"));
        assert!(manifest.contains("event_category: general"));
        assert!(manifest.contains("logical_name: events_page"));
        assert!(fixture.contains("div class=\"schedule_box\""));
        assert!(guide.contains("Generated Defaults"));
        assert!(guide.contains("Shape Contract"));
        assert!(guide.contains("fetch_and_parse_sample_domain_imports_seeded_school"));
        assert!(summary_text.contains("expected_shape=html_monthly_dl_pairs"));

        Ok(())
    }

    #[test]
    fn scaffold_domain_infers_admission_defaults_for_school_tour_shape() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let summary = scaffold_domain(ScaffoldDomainRequest {
            source_id: "sample-school-tour".to_string(),
            source_name: "Sample Junior High admissions school tours".to_string(),
            school_id: "school_sample".to_string(),
            parser_key: "sample_school_tour_v1".to_string(),
            source_maturity: SourceMaturity::ParserOnly,
            expected_shape: ParserExpectedShape::HtmlSchoolTourBlocks,
            target_url: "https://example.com/admission/explanation.html".to_string(),
            logical_name: None,
            manifest_dir: temp.path().join("configs/crawler/sources"),
            fixture_dir: temp.path().join("storage/fixtures/crawler"),
            guide_dir: temp.path().join("docs/crawler_scaffolds"),
            force: false,
        })?;

        let manifest = std::fs::read_to_string(&summary.manifest_path)?;
        let fixture = std::fs::read_to_string(&summary.fixture_path)?;
        let guide = std::fs::read_to_string(&summary.guide_path)?;

        assert!(manifest.contains("logical_name: school_tour_page"));
        assert!(manifest.contains("event_category: admission_event"));
        assert!(manifest.contains("is_open_day: true"));
        assert!(manifest.contains("priority_weight: 0.15"));
        assert!(fixture.contains("2026年 9月 27・28日"));
        assert!(guide.contains("logical_name=school_tour_page"));
        assert!(guide.contains("section.explan1"));
        assert!(guide.contains("section.explan3 > .tbody > .table"));

        Ok(())
    }

    async fn robots_handler(State(state): State<AppState>) -> impl IntoResponse {
        (StatusCode::OK, (*state.robots_txt).clone())
    }

    async fn partial_robots_handler(
        State(state): State<PartialFetchAppState>,
    ) -> impl IntoResponse {
        (StatusCode::OK, (*state.robots_txt).clone())
    }

    async fn page_handler(State(state): State<AppState>) -> impl IntoResponse {
        (
            StatusCode::OK,
            [("content-type", "text/html; charset=utf-8")],
            (*state.page_html).clone(),
        )
    }

    async fn partial_page_one_handler(
        State(state): State<PartialFetchAppState>,
    ) -> impl IntoResponse {
        (
            StatusCode::OK,
            [("content-type", "text/html; charset=utf-8")],
            (*state.page_one_html).clone(),
        )
    }

    async fn page_two_handler(State(state): State<AppState>) -> impl IntoResponse {
        (
            StatusCode::OK,
            [("content-type", "text/html; charset=utf-8")],
            state
                .page_two_html
                .as_deref()
                .map(ToString::to_string)
                .unwrap_or_default(),
        )
    }

    async fn partial_page_two_handler(
        State(state): State<PartialFetchAppState>,
    ) -> impl IntoResponse {
        let request_count = state.page_two_requests.fetch_add(1, Ordering::SeqCst);
        if request_count == 0 {
            (
                StatusCode::OK,
                [("content-type", "text/html; charset=utf-8")],
                (*state.page_two_html).clone(),
            )
                .into_response()
        } else {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "simulated partial fetch failure",
            )
                .into_response()
        }
    }
}
