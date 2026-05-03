use std::{fs, path::Path, time::Duration};

use anyhow::{Context, Result};
use generic_http::{ensure_allowed_url, evaluate_robots, HttpFetchClient};
use serde_json::Value;

use crate::report::{DiagnosticIssue, UrlProbeSummary};

pub(crate) const CRAWLER_CONTACT_URL: &str = "https://github.com/mt4110/geo-line-ranker";

pub(crate) fn discard_staged_fetch(path: &Path) {
    if let Err(error) = fs::remove_file(path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(path = %path.display(), %error, "failed to discard blocked staged fetch");
        }
    }
}

pub(crate) fn classify_fetch_error_status(error_message: &str) -> &'static str {
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

pub(crate) struct TargetBodyProbe {
    pub(crate) body: String,
    pub(crate) content_type: Option<String>,
    pub(crate) error: Option<String>,
}

pub(crate) fn build_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(20))
        .build()
        .context("failed to build crawler HTTP client")
}

pub(crate) fn build_http_fetch_client() -> Result<HttpFetchClient> {
    HttpFetchClient::from_builder(reqwest::Client::builder().timeout(Duration::from_secs(20)))
        .context("failed to build crawler HTTP fetch client")
}

pub(crate) async fn probe_target_body(
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

pub(crate) async fn probe_url(
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

pub(crate) fn collect_url_probe_issues(
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

pub(crate) fn looks_like_html(body: &str) -> bool {
    let lowercase = body.trim().to_ascii_lowercase();
    lowercase.starts_with("<!doctype html") || lowercase.starts_with("<html")
}

pub(crate) fn build_date_drift_warning(
    logical_name: &str,
    title: &str,
    details: &Value,
) -> Option<String> {
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

pub(crate) fn is_zero_event_parse_message(message: &str) -> bool {
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
