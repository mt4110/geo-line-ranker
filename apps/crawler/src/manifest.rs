use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use crawler_core::{
    check_expected_shape, fixture_content_type, resolve_manifest_fixture_path, CrawlParser,
    CrawlSourceManifest, ParsedEventRecord, ParserExpectedShape, ResolvedCrawlTarget,
    SourceMaturity,
};
use generic_http::ensure_allowed_url;
use storage_postgres::{EventCsvRecord, SourceManifestAudit};

use crate::shared::CRAWLER_CONTACT_URL;

pub(crate) struct ResolvedManifestMetadata {
    pub(crate) source_maturity: SourceMaturity,
    pub(crate) expected_shape: Option<ParserExpectedShape>,
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

pub(crate) fn resolve_manifest_metadata(
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
        &build_scaffold_manifest(&request, host, &parsed_target, &fixture_name)?,
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
) -> Result<String> {
    let preset = infer_scaffold_template_preset(request, host, parsed_target);
    let live_fetch_enabled = request.source_maturity == SourceMaturity::LiveReady;
    let fixture_path = manifest_path_value(&lexical_relative_path(
        &request.manifest_dir,
        &request.fixture_dir.join(fixture_name),
    )?);
    let block_reason = if live_fetch_enabled {
        String::new()
    } else {
        "  live_fetch_enabled: false\n  live_fetch_block_reason: TODO: keep this source blocked until robots/terms/manual review are complete.\n".to_string()
    };

    Ok(format!(
        "# Generated by `crawler scaffold-domain`.\n# Replace the temporary policy placeholder values before you promote this source.\nschema_version: 1\nkind: crawler_source\nmanifest_version: 1\nsource_id: {source_id}\nsource_name: {source_name}\nsource_maturity: {source_maturity}\nparser_key: {parser_key}\nexpected_shape: {expected_shape}\ndescription: {description}\nallowlist:\n  allowed_domains:\n    - {host}\n  user_agent: geo-line-ranker-crawler/0.1 (+{crawler_contact_url})\n  min_fetch_interval_ms: 1000\n{block_reason}  robots_txt_url: {scheme}://{host}/robots.txt\n  terms_url: {terms_url}\n  terms_note: {terms_note}\ndefaults:\n  school_id: {school_id}\n  event_category: {event_category}\n  is_open_day: {is_open_day}\n  is_featured: false\n  priority_weight: {priority_weight}\n  placement_tags:\n    - search\n    - detail\ntargets:\n  - logical_name: {logical_name}\n    url: {target_url}\n    fixture_path: {fixture_path}\n\n# Fixture seed: {fixture_path}\n",
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
        fixture_path = fixture_path
    ))
}

fn manifest_path_value(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn lexical_relative_path(from_dir: &Path, to_path: &Path) -> Result<PathBuf> {
    let from_dir = scaffold_path_for_diff(from_dir)?;
    let to_path = scaffold_path_for_diff(to_path)?;
    let from_anchor = path_anchor(&from_dir);
    let to_anchor = path_anchor(&to_path);
    ensure!(
        from_anchor == to_anchor,
        "cannot derive relative scaffold fixture_path across different path roots: {} -> {}",
        from_dir.display(),
        to_path.display()
    );
    let from = normalized_path_components(&from_dir);
    let to = normalized_path_components(&to_path);
    let common_len = from
        .iter()
        .zip(to.iter())
        .take_while(|(left, right)| left == right)
        .count();

    let mut output = PathBuf::new();
    for _ in common_len..from.len() {
        output.push("..");
    }
    for component in &to[common_len..] {
        output.push(component);
    }
    if output.as_os_str().is_empty() {
        Ok(PathBuf::from("."))
    } else {
        Ok(output)
    }
}

fn path_anchor(path: &Path) -> Vec<String> {
    path.components()
        .take_while(|component| {
            matches!(
                component,
                std::path::Component::Prefix(_) | std::path::Component::RootDir
            )
        })
        .map(|component| component.as_os_str().to_string_lossy().to_string())
        .collect()
}

fn scaffold_path_for_diff(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()
            .context("failed to resolve current working directory for scaffold paths")?
            .join(path))
    }
}

fn normalized_path_components(path: &Path) -> Vec<String> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if components.last().is_some_and(|value| value != "..") {
                    components.pop();
                } else {
                    components.push("..".to_string());
                }
            }
            std::path::Component::Normal(value) => {
                components.push(value.to_string_lossy().to_string());
            }
            other => components.push(other.as_os_str().to_string_lossy().to_string()),
        }
    }
    components
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

pub(crate) fn check_fixture_shape(
    manifest_path: &Path,
    fixture_path: &str,
    expected_shape: ParserExpectedShape,
) -> Result<(bool, String)> {
    let resolved_path = resolve_manifest_fixture_path(manifest_path, fixture_path)?;
    let body = fs::read_to_string(&resolved_path)
        .with_context(|| format!("failed to read fixture {}", resolved_path.display()))?;
    let check = check_expected_shape(expected_shape, &body, fixture_content_type(&resolved_path));
    Ok((
        check.matched,
        format!(
            "fixture_path {}: {}",
            resolved_path.display(),
            check.summary
        ),
    ))
}

pub(crate) fn build_manifest_audit(
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

pub(crate) fn resolve_and_validate_targets(
    manifest: &CrawlSourceManifest,
) -> Result<Vec<ResolvedCrawlTarget>> {
    let targets = manifest.resolved_targets()?;
    for target in &targets {
        ensure_allowed_url(&target.url, &manifest.allowlist.allowed_domains)?;
    }
    Ok(targets)
}

pub(crate) fn canonical_manifest_path(path: impl AsRef<Path>) -> Result<PathBuf> {
    fs::canonicalize(path.as_ref()).with_context(|| {
        format!(
            "failed to resolve crawl manifest {}",
            path.as_ref().display()
        )
    })
}

pub(crate) fn list_manifest_paths(manifest_dir: &Path) -> Result<Vec<PathBuf>> {
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

pub(crate) fn to_event_csv_record(record: &ParsedEventRecord) -> EventCsvRecord {
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crawler_core::{ParserExpectedShape, SourceMaturity};

    use super::{
        lexical_relative_path, manifest_path_value, scaffold_domain, ScaffoldDomainRequest,
    };
    use crate::report::format_scaffold_summary;

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

        assert!(manifest.contains("manifest_version: 1"));
        assert!(manifest.contains("source_maturity: parser_only"));
        assert!(manifest.contains("expected_shape: html_monthly_dl_pairs"));
        assert!(manifest.contains("live_fetch_enabled: false"));
        assert!(manifest.contains("terms_url: https://example.com/"));
        assert!(manifest.contains("event_category: general"));
        assert!(manifest.contains("logical_name: events_page"));
        assert!(
            manifest.contains("fixture_path: ../../../storage/fixtures/crawler/sample_domain.html")
        );
        assert!(fixture.contains("div class=\"schedule_box\""));
        assert!(guide.contains("Generated Defaults"));
        assert!(guide.contains("Shape Contract"));
        assert!(guide.contains("fetch_and_parse_sample_domain_imports_seeded_school"));
        assert!(summary_text.contains("expected_shape=html_monthly_dl_pairs"));

        Ok(())
    }

    #[test]
    fn manifest_path_value_normalizes_backslashes() {
        let value = manifest_path_value(Path::new("..\\storage\\fixtures\\crawler\\sample.html"));
        assert_eq!(value, "../storage/fixtures/crawler/sample.html");
    }

    #[test]
    fn lexical_relative_path_handles_mixed_absolute_and_relative_inputs() -> anyhow::Result<()> {
        let cwd = std::env::current_dir()?;
        let path = lexical_relative_path(
            Path::new("relative_manifest_dir"),
            &cwd.join("relative_fixture_dir").join("sample.html"),
        )?;

        assert_eq!(
            manifest_path_value(&path),
            "../relative_fixture_dir/sample.html"
        );
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn lexical_relative_path_rejects_different_windows_roots() {
        let error = lexical_relative_path(
            Path::new(r"C:\geo-line-ranker\configs\crawler\sources"),
            Path::new(r"D:\geo-line-ranker\storage\fixtures\crawler\sample.html"),
        )
        .expect_err("different roots");

        assert!(format!("{error:#}").contains("different path roots"));
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
}
