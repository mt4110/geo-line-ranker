/// Golden scenarios for validating geo-first ranking stability.
/// These are deterministic acceptance tests ensuring no remote surprises and
/// correct fallback behavior.
///
/// # Usage (Phase 2)
///
/// These scenarios will be integrated into the acceptance test harness during
/// Phase 2 (Candidate Plan Execution). They define the hard constraints that
/// fallback ladder and retrieval planning must satisfy:
///
/// - **hokkaido_tokyo_no_okinawa**: Geo-first rule — user location must not be
///   violated by fallback distance. Tokyo results must prioritize over Okinawa
///   for Hokkaido residents.
///
/// - **area_only_no_remote_jump**: Area-context rule — requests with city context
///   must expand only to adjacent areas, never skip to remote regions.
///
/// - **line_identity_preserved**: Line-context rule — requests with line info
///   must keep line intent visible through fallback chain reasoning.
///
/// # Implementation Notes
///
/// - Scenarios are framework definitions; actual validation happens in ranking
///   engine acceptance tests.
/// - `TestContextBuilder` is for unit test setup; prefer `ContextNormalizer::resolve_hierarchy`
///   for production context resolution.
use context::{AreaContext, AreaContextInput, ContextSource, RankingContext};

/// A golden scenario specification for testing ranking behavior.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct GoldenScenario {
    pub name: &'static str,
    pub description: &'static str,
    pub request_station_id: Option<&'static str>,
    pub request_area: Option<AreaContextInput>,
    pub user_profile_area: Option<AreaContextInput>,
    pub expected_min_confidence: f64,
    pub forbidden_prefectures: Vec<&'static str>,
}

#[allow(dead_code)]
impl GoldenScenario {
    /// Scenario: Hokkaido user searching near Tokyo must not recommend Okinawa.
    pub fn hokkaido_tokyo_no_okinawa() -> Self {
        Self {
            name: "hokkaido_tokyo_no_okinawa",
            description: "Hokkaido user searching in Tokyo area (Tamachi) \
                        must not return Okinawa stations in top candidates",
            request_station_id: Some("st_tamachi"),
            request_area: Some(AreaContextInput {
                prefecture_code: Some("13".to_string()), // Tokyo
                ..Default::default()
            }),
            user_profile_area: Some(AreaContextInput {
                prefecture_code: Some("01".to_string()), // Hokkaido
                ..Default::default()
            }),
            expected_min_confidence: 0.7,
            forbidden_prefectures: vec!["47"], // Okinawa
        }
    }

    /// Scenario: Area-only context (no station) must not jump to remote areas.
    pub fn area_only_no_remote_jump() -> Self {
        Self {
            name: "area_only_no_remote_jump",
            description: "Request with city context (Minato/Tokyo) but no station \
                        must expand within same area or adjacent prefectures only",
            request_station_id: None,
            request_area: Some(AreaContextInput {
                prefecture_code: Some("13".to_string()), // Tokyo
                city_code: Some("13103".to_string()),    // Minato
                city_name: Some("Minato".to_string()),
                ..Default::default()
            }),
            user_profile_area: None,
            expected_min_confidence: 0.6,
            forbidden_prefectures: vec!["47", "02", "37"], // No Okinawa, Aomori, Fukuoka
        }
    }

    /// Scenario: Line-based request must keep line intent visible through fallback.
    pub fn line_identity_preserved() -> Self {
        Self {
            name: "line_identity_preserved",
            description: "Request with line context (Yamanote Line) must prioritize \
                        same-line candidates and preserve line intent in fallback reasoning",
            request_station_id: Some("st_tamachi"),
            request_area: None,
            user_profile_area: None,
            expected_min_confidence: 0.8,
            forbidden_prefectures: vec![], // No hard forbid, but trace must show line intent
        }
    }
}

/// Context builder for testing resolver behavior.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct TestContextBuilder {
    source: Option<ContextSource>,
    confidence: f64,
    area: Option<AreaContext>,
    prefecture_code: Option<String>,
}

#[allow(dead_code)]
impl TestContextBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn source(mut self, source: ContextSource) -> Self {
        self.source = Some(source);
        self
    }

    pub fn confidence(mut self, conf: f64) -> Self {
        self.confidence = conf;
        self
    }

    pub fn prefecture_code(mut self, code: impl Into<String>) -> Self {
        self.prefecture_code = Some(code.into());
        self
    }

    pub fn build(self) -> RankingContext {
        let mut ctx = RankingContext::default_safe();
        if let Some(source) = self.source {
            ctx.context_source = source;
        }
        ctx.confidence = if self.confidence > 0.0 {
            self.confidence
        } else {
            0.5
        };

        if let Some(pref_code) = self.prefecture_code {
            ctx.area = Some(AreaContext {
                country: "JP".to_string(),
                prefecture_code: Some(pref_code),
                prefecture_name: None,
                city_code: None,
                city_name: None,
            });
        }

        ctx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn golden_scenario_hokkaido_tokyo_no_okinawa_is_well_formed() {
        let scenario = GoldenScenario::hokkaido_tokyo_no_okinawa();
        assert!(!scenario.forbidden_prefectures.is_empty());
        assert!(scenario.forbidden_prefectures.contains(&"47"));
        assert!(scenario.expected_min_confidence >= 0.5);
    }

    #[test]
    fn golden_scenario_area_only_forbids_remote() {
        let scenario = GoldenScenario::area_only_no_remote_jump();
        assert_eq!(scenario.request_station_id, None);
        assert!(scenario.forbidden_prefectures.len() >= 3);
    }

    #[test]
    fn test_context_builder_creates_hokkaido_context() {
        let ctx = TestContextBuilder::new()
            .source(ContextSource::UserProfileArea)
            .confidence(0.75)
            .prefecture_code("01")
            .build();

        assert_eq!(ctx.context_source, ContextSource::UserProfileArea);
        assert_eq!(ctx.confidence, 0.75);
        assert_eq!(
            ctx.prefecture_code(),
            Some("01"),
            "Hokkaido code should be 01"
        );
    }

    #[test]
    fn test_context_builder_area_defaults_to_safe_when_empty() {
        let ctx = TestContextBuilder::new().build();
        assert_eq!(
            ctx.context_source,
            ContextSource::DefaultSafeContext,
            "Should default to safe context"
        );
    }

    #[test]
    fn hokkaido_and_okinawa_are_not_adjacent() {
        use geo::GeoGraph;
        let graph = GeoGraph::new();
        assert!(
            !graph.are_adjacent("01", "47"),
            "Hokkaido and Okinawa must not be adjacent"
        );
    }

    #[test]
    fn tokyo_and_adjacent_prefectures_form_safe_fallback_zone() {
        use geo::GeoGraph;
        let graph = GeoGraph::new();
        let neighbors = graph.adjacent_prefectures("13");
        // Tokyo (13) should have Saitama, Chiba, Kanagawa as neighbors
        assert!(
            neighbors.len() >= 3,
            "Tokyo should have at least 3 neighbors"
        );
    }
}
