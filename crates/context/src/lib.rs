use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, ToSchema)]
pub struct ContextInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub station_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub area: Option<AreaContextInput>,
}

impl ContextInput {
    pub fn is_empty(&self) -> bool {
        self.station_id.as_deref().is_none_or(str::is_empty)
            && self.line_id.as_deref().is_none_or(str::is_empty)
            && self.line_name.as_deref().is_none_or(str::is_empty)
            && self.area.as_ref().is_none_or(AreaContextInput::is_empty)
    }

    pub fn has_line(&self) -> bool {
        self.line_id
            .as_deref()
            .is_some_and(|value| !value.is_empty())
            || self
                .line_name
                .as_deref()
                .is_some_and(|value| !value.is_empty())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, ToSchema)]
pub struct AreaContextInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefecture_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefecture_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub city_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub city_name: Option<String>,
}

impl AreaContextInput {
    pub fn is_empty(&self) -> bool {
        self.country.as_deref().is_none_or(str::is_empty)
            && self.prefecture_code.as_deref().is_none_or(str::is_empty)
            && self.prefecture_name.as_deref().is_none_or(str::is_empty)
            && self.city_code.as_deref().is_none_or(str::is_empty)
            && self.city_name.as_deref().is_none_or(str::is_empty)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct RankingContext {
    pub context_source: ContextSource,
    pub confidence: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub area: Option<AreaContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<LineContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub station: Option<StationContext>,
    pub privacy_level: PrivacyLevel,
    pub fallback_policy: String,
    pub gate_policy: String,
    #[serde(default)]
    pub warnings: Vec<ContextWarning>,
}

impl RankingContext {
    pub fn default_safe() -> Self {
        Self {
            context_source: ContextSource::DefaultSafeContext,
            confidence: 0.20,
            area: None,
            line: None,
            station: None,
            privacy_level: PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        }
    }

    pub fn station_id(&self) -> Option<&str> {
        self.station
            .as_ref()
            .map(|station| station.station_id.as_str())
    }

    pub fn line_name(&self) -> Option<&str> {
        self.line.as_ref().map(|line| line.line_name.as_str())
    }

    pub fn city_name(&self) -> Option<&str> {
        self.area
            .as_ref()
            .and_then(|area| area.city_name.as_deref())
    }

    pub fn prefecture_name(&self) -> Option<&str> {
        self.area
            .as_ref()
            .and_then(|area| area.prefecture_name.as_deref())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct AreaContext {
    #[serde(default = "default_country")]
    pub country: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefecture_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefecture_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub city_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub city_name: Option<String>,
}

impl From<AreaContextInput> for AreaContext {
    fn from(value: AreaContextInput) -> Self {
        Self {
            country: value.country.unwrap_or_else(default_country),
            prefecture_code: value.prefecture_code,
            prefecture_name: value.prefecture_name,
            city_code: value.city_code,
            city_name: value.city_name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct LineContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_id: Option<String>,
    pub line_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct StationContext {
    pub station_id: String,
    pub station_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ContextSource {
    RequestStation,
    RequestLine,
    RequestArea,
    UserProfileArea,
    RecentSearchContext,
    RecentBehaviorContext,
    DefaultSafeContext,
}

impl ContextSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RequestStation => "request_station",
            Self::RequestLine => "request_line",
            Self::RequestArea => "request_area",
            Self::UserProfileArea => "user_profile_area",
            Self::RecentSearchContext => "recent_search_context",
            Self::RecentBehaviorContext => "recent_behavior_context",
            Self::DefaultSafeContext => "default_safe_context",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyLevel {
    CoarseArea,
}

impl PrivacyLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CoarseArea => "coarse_area",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ContextWarning {
    pub code: String,
    pub message: String,
}

pub fn build_request_context(
    target_station_id: Option<&str>,
    context: Option<&ContextInput>,
) -> ContextInput {
    let mut input = context.cloned().unwrap_or_default();
    if input.station_id.as_deref().is_none_or(str::is_empty) {
        input.station_id = target_station_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
    }
    input
}

fn default_country() -> String {
    "JP".to_string()
}

#[cfg(test)]
mod tests {
    use super::{build_request_context, AreaContextInput, ContextInput};

    #[test]
    fn target_station_id_is_normalized_into_context_station() {
        let input = build_request_context(Some("st_tamachi"), None);

        assert_eq!(input.station_id.as_deref(), Some("st_tamachi"));
    }

    #[test]
    fn explicit_context_station_overrides_compatibility_station() {
        let input = build_request_context(
            Some("st_tamachi"),
            Some(&ContextInput {
                station_id: Some("st_shibuya".to_string()),
                ..Default::default()
            }),
        );

        assert_eq!(input.station_id.as_deref(), Some("st_shibuya"));
    }

    #[test]
    fn area_context_counts_as_non_empty_without_raw_address() {
        let input = ContextInput {
            area: Some(AreaContextInput {
                city_name: Some("Minato".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(!input.is_empty());
    }
}
