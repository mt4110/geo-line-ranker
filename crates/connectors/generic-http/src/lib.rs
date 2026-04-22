use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION, USER_AGENT};
use sha2::{Digest, Sha256};
use url::Url;

const DEFAULT_MAX_REDIRECTS: usize = 3;
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 10 * 1024 * 1024;
const DEFAULT_ALLOWED_CONTENT_TYPES: [&str; 4] =
    ["text/html", "text/csv", "application/json", "text/json"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpFetchRequest<'a> {
    pub source_id: &'a str,
    pub logical_name: &'a str,
    pub url: &'a str,
    pub user_agent: &'a str,
    pub allowed_domains: &'a [String],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedHttpFetch {
    pub logical_name: String,
    pub target_url: String,
    pub final_url: String,
    pub staged_path: PathBuf,
    pub staged_was_created: bool,
    pub checksum_sha256: String,
    pub size_bytes: u64,
    pub status_code: u16,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RobotsDecision {
    pub allowed: bool,
    pub matched_rule: Option<String>,
}

pub fn ensure_allowed_url(raw_url: &str, allowed_domains: &[String]) -> Result<Url> {
    let url =
        Url::parse(raw_url).with_context(|| format!("failed to parse target URL {raw_url}"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "unsupported URL scheme {} for {}",
        url.scheme(),
        raw_url
    );

    let host = url
        .host_str()
        .context("target URL must include a host name")?
        .to_ascii_lowercase();
    let is_allowed = allowed_domains.iter().any(|domain| {
        let domain = domain.trim().trim_start_matches('.').to_ascii_lowercase();
        host == domain || host.ends_with(&format!(".{domain}"))
    });
    ensure!(is_allowed, "host {} is outside the crawler allowlist", host);
    ensure!(
        !is_private_or_local_host(&host)
            || allowed_domains.iter().any(|domain| {
                domain
                    .trim()
                    .trim_start_matches('.')
                    .eq_ignore_ascii_case(&host)
            }),
        "host {} is private or local and must be explicitly allowed",
        host
    );

    Ok(url)
}

pub async fn fetch_robots_txt(
    client: &reqwest::Client,
    robots_txt_url: &str,
    user_agent: &str,
) -> Result<String> {
    let response = client
        .get(robots_txt_url)
        .header(USER_AGENT, user_agent)
        .send()
        .await
        .with_context(|| format!("failed to fetch robots.txt from {robots_txt_url}"))?;
    ensure!(
        response.status().is_success(),
        "robots.txt returned HTTP {} from {}",
        response.status(),
        robots_txt_url
    );
    response
        .text()
        .await
        .with_context(|| format!("failed to read robots.txt body from {robots_txt_url}"))
}

pub fn evaluate_robots(robots_txt: &str, user_agent: &str, target_path: &str) -> RobotsDecision {
    let requested_agent = user_agent.to_ascii_lowercase();
    let requested_path = if target_path.is_empty() {
        "/"
    } else {
        target_path
    };
    let groups = parse_robots_groups(robots_txt);

    let best_specificity = groups
        .iter()
        .filter_map(|group| group.match_specificity(&requested_agent))
        .max()
        .unwrap_or(0);

    let matching_rules = groups
        .iter()
        .filter(|group| group.match_specificity(&requested_agent) == Some(best_specificity))
        .flat_map(|group| group.rules.iter())
        .filter(|rule| rule.matches(requested_path))
        .collect::<Vec<_>>();

    let selected_rule = matching_rules.into_iter().max_by(|left, right| {
        left.path
            .len()
            .cmp(&right.path.len())
            .then_with(|| left.allow.cmp(&right.allow))
    });

    match selected_rule {
        Some(rule) => RobotsDecision {
            allowed: rule.allow || rule.path.is_empty(),
            matched_rule: Some(format!(
                "{}:{}",
                if rule.allow { "allow" } else { "disallow" },
                rule.path
            )),
        },
        None => RobotsDecision {
            allowed: true,
            matched_rule: None,
        },
    }
}

pub async fn fetch_to_raw(
    client: &reqwest::Client,
    request: &HttpFetchRequest<'_>,
    raw_root: impl AsRef<Path>,
) -> Result<PreparedHttpFetch> {
    let (response, final_url) = fetch_with_manual_redirects(client, request).await?;
    ensure!(
        response.status().is_success(),
        "fetch returned HTTP {} for {}",
        response.status(),
        request.url
    );

    let status_code = response.status().as_u16();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    ensure_allowed_content_type(content_type.as_deref())?;
    if let Some(content_length) = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
    {
        ensure!(
            content_length <= DEFAULT_MAX_RESPONSE_BYTES,
            "response Content-Length {} exceeds max_response_bytes {} for {}",
            content_length,
            DEFAULT_MAX_RESPONSE_BYTES,
            request.url
        );
    }
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("failed to read response body from {}", request.url))?;
    ensure!(
        bytes.len() as u64 <= DEFAULT_MAX_RESPONSE_BYTES,
        "response body {} bytes exceeds max_response_bytes {} for {}",
        bytes.len(),
        DEFAULT_MAX_RESPONSE_BYTES,
        request.url
    );
    let checksum_sha256 = format!("{:x}", Sha256::digest(&bytes));
    let size_bytes = bytes.len() as u64;

    let staged_dir = raw_root
        .as_ref()
        .join(request.source_id)
        .join(&checksum_sha256[..12]);
    fs::create_dir_all(&staged_dir)
        .with_context(|| format!("failed to create {}", staged_dir.display()))?;
    let staged_extension =
        infer_staged_extension(content_type.as_deref(), final_url.as_str(), request.url);
    let staged_path = staged_dir.join(format!(
        "{}.{}",
        sanitize_name(request.logical_name),
        staged_extension
    ));
    let staged_was_created = if !staged_path.exists() {
        fs::write(&staged_path, &bytes)
            .with_context(|| format!("failed to stage {}", staged_path.display()))?;
        true
    } else {
        false
    };

    Ok(PreparedHttpFetch {
        logical_name: request.logical_name.to_string(),
        target_url: request.url.to_string(),
        final_url: final_url.to_string(),
        staged_path,
        staged_was_created,
        checksum_sha256,
        size_bytes,
        status_code,
        content_type,
    })
}

async fn fetch_with_manual_redirects<'a>(
    client: &reqwest::Client,
    request: &HttpFetchRequest<'a>,
) -> Result<(reqwest::Response, Url)> {
    let mut current_url = ensure_allowed_url(request.url, request.allowed_domains)?;
    for redirect_count in 0..=DEFAULT_MAX_REDIRECTS {
        let response = client
            .get(current_url.clone())
            .header(USER_AGENT, request.user_agent)
            .send()
            .await
            .with_context(|| format!("failed to fetch {}", current_url))?;

        if !response.status().is_redirection() {
            return Ok((response, current_url));
        }

        ensure!(
            redirect_count < DEFAULT_MAX_REDIRECTS,
            "redirect count exceeded max_redirects {} for {}",
            DEFAULT_MAX_REDIRECTS,
            request.url
        );
        let location = response
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
            .with_context(|| format!("redirect response missing Location for {}", current_url))?;
        let next_url = current_url
            .join(location)
            .with_context(|| format!("failed to resolve redirect Location {location}"))?;
        current_url = ensure_allowed_url(next_url.as_str(), request.allowed_domains)?;
    }

    unreachable!("redirect loop returns or errors before exceeding max_redirects")
}

fn ensure_allowed_content_type(content_type: Option<&str>) -> Result<()> {
    let mime = content_type
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .with_context(|| "response content-type is missing and default policy denies it")?
        .to_ascii_lowercase();
    let allowed = DEFAULT_ALLOWED_CONTENT_TYPES
        .iter()
        .any(|allowed| mime == *allowed || (allowed.ends_with("/json") && mime.ends_with("+json")));
    ensure!(
        allowed,
        "response content-type {} is outside the crawler allowlist",
        mime
    );
    Ok(())
}

fn is_private_or_local_host(host: &str) -> bool {
    if matches!(host, "localhost" | "0.0.0.0") || host.ends_with(".localhost") {
        return true;
    }
    let Ok(ip) = host.parse::<std::net::IpAddr>() else {
        return false;
    };
    match ip {
        std::net::IpAddr::V4(ip) => {
            ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.is_unspecified()
        }
        std::net::IpAddr::V6(ip) => ip.is_loopback() || ip.is_unspecified(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RobotsGroup {
    user_agents: Vec<String>,
    rules: Vec<RobotsRule>,
}

impl RobotsGroup {
    fn match_specificity(&self, requested_agent: &str) -> Option<usize> {
        self.user_agents
            .iter()
            .filter_map(|agent| {
                if agent == "*" {
                    Some(0)
                } else if requested_agent == agent || requested_agent.starts_with(agent) {
                    Some(agent.len())
                } else {
                    None
                }
            })
            .max()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RobotsRule {
    allow: bool,
    path: String,
}

impl RobotsRule {
    fn matches(&self, requested_path: &str) -> bool {
        self.path.is_empty() || requested_path.starts_with(&self.path)
    }
}

fn parse_robots_groups(input: &str) -> Vec<RobotsGroup> {
    let mut groups = Vec::new();
    let mut current_agents = Vec::new();
    let mut current_rules = Vec::new();
    let mut saw_rule = false;

    for raw_line in input.lines() {
        let line = raw_line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            if !current_agents.is_empty() || !current_rules.is_empty() {
                groups.push(RobotsGroup {
                    user_agents: std::mem::take(&mut current_agents),
                    rules: std::mem::take(&mut current_rules),
                });
                saw_rule = false;
            }
            continue;
        }

        if let Some(value) = split_directive(line, "user-agent") {
            if saw_rule && !current_agents.is_empty() {
                groups.push(RobotsGroup {
                    user_agents: std::mem::take(&mut current_agents),
                    rules: std::mem::take(&mut current_rules),
                });
                saw_rule = false;
            }
            current_agents.push(value.to_ascii_lowercase());
            continue;
        }

        if let Some(value) = split_directive(line, "allow") {
            current_rules.push(RobotsRule {
                allow: true,
                path: value.to_string(),
            });
            saw_rule = true;
            continue;
        }

        if let Some(value) = split_directive(line, "disallow") {
            current_rules.push(RobotsRule {
                allow: false,
                path: value.to_string(),
            });
            saw_rule = true;
        }
    }

    if !current_agents.is_empty() || !current_rules.is_empty() {
        groups.push(RobotsGroup {
            user_agents: current_agents,
            rules: current_rules,
        });
    }

    groups
}

fn split_directive<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    let (left, right) = line.split_once(':')?;
    left.trim()
        .eq_ignore_ascii_case(name)
        .then_some(right.trim())
}

fn infer_staged_extension(
    content_type: Option<&str>,
    final_url: &str,
    request_url: &str,
) -> String {
    extension_from_content_type(content_type)
        .map(str::to_string)
        .or_else(|| extension_from_url(final_url))
        .or_else(|| extension_from_url(request_url))
        .unwrap_or_else(|| "bin".to_string())
}

fn extension_from_content_type(content_type: Option<&str>) -> Option<&'static str> {
    let mime = content_type?
        .split(';')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_ascii_lowercase();
    match mime.as_str() {
        "text/html" => Some("html"),
        "application/json" | "text/json" => Some("json"),
        value if value.ends_with("+json") => Some("json"),
        "application/xml" | "text/xml" => Some("xml"),
        value if value.ends_with("+xml") => Some("xml"),
        "text/plain" => Some("txt"),
        "text/csv" | "application/csv" | "application/vnd.ms-excel" => Some("csv"),
        "application/pdf" => Some("pdf"),
        _ => None,
    }
}

fn extension_from_url(raw_url: &str) -> Option<String> {
    let url = Url::parse(raw_url).ok()?;
    let extension = Path::new(url.path()).extension()?.to_str()?.trim();
    if extension.is_empty() {
        return None;
    }

    let normalized = extension
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .take(8)
        .collect::<String>()
        .to_ascii_lowercase();
    (!normalized.is_empty()).then_some(normalized)
}

fn sanitize_name(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => character,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{ensure_allowed_url, evaluate_robots, infer_staged_extension};

    #[test]
    fn allowlist_accepts_matching_subdomain() {
        let url = ensure_allowed_url(
            "https://events.example.com/open-campus",
            &[String::from("example.com")],
        )
        .expect("allowed URL");

        assert_eq!(url.host_str(), Some("events.example.com"));
    }

    #[test]
    fn allowlist_rejects_outside_domain() {
        let error = ensure_allowed_url(
            "https://example.net/open-campus",
            &[String::from("example.com")],
        )
        .expect_err("outside domain");

        assert!(error.to_string().contains("outside the crawler allowlist"));
    }

    #[test]
    fn allowlist_accepts_explicit_local_dev_host() {
        let url = ensure_allowed_url(
            "http://127.0.0.1:3000/open-campus",
            &[String::from("127.0.0.1")],
        )
        .expect("explicit local dev host");

        assert_eq!(url.host_str(), Some("127.0.0.1"));
    }

    #[test]
    fn robots_blocks_disallowed_path() {
        let robots = r#"
User-agent: *
Disallow: /private
"#;

        let decision = evaluate_robots(robots, "geo-line-ranker-bot/0.1", "/private/feed");

        assert!(!decision.allowed);
        assert_eq!(decision.matched_rule.as_deref(), Some("disallow:/private"));
    }

    #[test]
    fn robots_prefers_more_specific_allow_rule() {
        let robots = r#"
User-agent: *
Disallow: /events
Allow: /events/open-campus
"#;

        let decision = evaluate_robots(robots, "geo-line-ranker-bot/0.1", "/events/open-campus");

        assert!(decision.allowed);
        assert_eq!(
            decision.matched_rule.as_deref(),
            Some("allow:/events/open-campus")
        );
    }

    #[test]
    fn robots_does_not_treat_arbitrary_substrings_as_agent_matches() {
        let robots = r#"
User-agent: line
Disallow: /

User-agent: *
Allow: /
"#;

        let decision = evaluate_robots(robots, "geo-line-ranker-bot/0.1", "/events/open-campus");

        assert!(decision.allowed);
        assert_eq!(decision.matched_rule.as_deref(), Some("allow:/"));
    }

    #[test]
    fn staged_extension_prefers_content_type_for_json_feeds() {
        let extension = infer_staged_extension(
            Some("application/json; charset=utf-8"),
            "https://example.com/events",
            "https://example.com/events",
        );

        assert_eq!(extension, "json");
    }

    #[test]
    fn staged_extension_falls_back_to_url_path() {
        let extension = infer_staged_extension(
            None,
            "https://example.com/archive/feed.XML?download=1",
            "https://example.com/archive/feed.XML?download=1",
        );

        assert_eq!(extension, "xml");
    }

    #[test]
    fn staged_extension_uses_bin_when_unknown() {
        let extension = infer_staged_extension(
            Some("application/octet-stream"),
            "https://example.com/download",
            "https://example.com/download",
        );

        assert_eq!(extension, "bin");
    }
}
