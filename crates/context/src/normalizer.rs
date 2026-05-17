/// Context normalization and resolution logic.
/// Implements deterministic area/line context normalization with evidence tracking.

use crate::{AreaContext, AreaContextInput, ContextInput, ContextSource, RankingContext};

/// Normalizes request context into a ranked context for retrieval planning.
pub struct ContextNormalizer;

impl ContextNormalizer {
    /// Normalizes area-only context (no station ID) into retrieval context.
    /// Used when request has prefecture/city but no specific station.
    pub fn normalize_area_context(
        area_input: &AreaContextInput,
        source: ContextSource,
        confidence: f64,
    ) -> RankingContext {
        let area = AreaContext {
            country: area_input
                .country
                .clone()
                .unwrap_or_else(|| "JP".to_string()),
            prefecture_code: area_input.prefecture_code.clone(),
            prefecture_name: area_input.prefecture_name.clone(),
            city_code: area_input.city_code.clone(),
            city_name: area_input.city_name.clone(),
        };

        RankingContext {
            context_source: source,
            confidence,
            area: Some(area),
            line: None,
            station: None,
            privacy_level: crate::PrivacyLevel::CoarseArea,
            fallback_policy: "school_event_jp_default".to_string(),
            gate_policy: "geo_line_default".to_string(),
            warnings: Vec::new(),
        }
    }

    /// Resolves context hierarchy: request > session > user profile > safe fallback.
    pub fn resolve_hierarchy(
        request_context: Option<&ContextInput>,
        user_profile_area: Option<&AreaContextInput>,
    ) -> RankingContext {
        // 1. Request context takes priority
        if let Some(ctx) = request_context {
            if let Some(station_id) = ctx.station_id.as_deref() {
                if !station_id.trim().is_empty() {
                    // Station context is highest priority
                    return RankingContext {
                        context_source: ContextSource::RequestStation,
                        confidence: 0.95,
                        area: ctx.area.as_ref().map(|a| AreaContext::from(a.clone())),
                        line: None,
                        station: Some(crate::StationContext {
                            station_id: station_id.to_string(),
                            station_name: String::new(),
                        }),
                        privacy_level: crate::PrivacyLevel::CoarseArea,
                        fallback_policy: "school_event_jp_default".to_string(),
                        gate_policy: "geo_line_default".to_string(),
                        warnings: Vec::new(),
                    };
                }
            }

            if let Some(area) = ctx.area.as_ref() {
                if !area.is_empty() {
                    // Area context is second priority
                    return Self::normalize_area_context(
                        area,
                        ContextSource::RequestArea,
                        0.85,
                    );
                }
            }
        }

        // 2. User profile area
        if let Some(area) = user_profile_area {
            if !area.is_empty() {
                return Self::normalize_area_context(area, ContextSource::UserProfileArea, 0.60);
            }
        }

        // 3. Safe fallback
        RankingContext::default_safe()
    }

    /// Decay confidence based on evidence age.
    /// Used for time-based context freshness.
    pub fn decay_confidence(base_confidence: f64, age_hours: f64, max_age_hours: f64) -> f64 {
        if age_hours <= 0.0 {
            return base_confidence;
        }

        // Non-linear decay: confidence decreases more rapidly for older evidence
        let normalized_age = (age_hours / max_age_hours).min(1.0);
        let decay_factor = 1.0 - normalized_age.powi(2);
        base_confidence * decay_factor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_area_context_preserves_prefecture_code() {
        let area_input = AreaContextInput {
            prefecture_code: Some("13".to_string()),
            prefecture_name: Some("Tokyo".to_string()),
            city_code: Some("13103".to_string()),
            city_name: Some("Minato".to_string()),
            ..Default::default()
        };

        let ctx = ContextNormalizer::normalize_area_context(
            &area_input,
            ContextSource::RequestArea,
            0.85,
        );

        assert_eq!(ctx.prefecture_code(), Some("13"));
        assert_eq!(ctx.city_name(), Some("Minato"));
        assert_eq!(ctx.confidence, 0.85);
    }

    #[test]
    fn resolve_hierarchy_prioritizes_request_station() {
        let request_context = ContextInput {
            station_id: Some("st_tamachi".to_string()),
            area: Some(AreaContextInput {
                prefecture_code: Some("13".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let resolved = ContextNormalizer::resolve_hierarchy(Some(&request_context), None);

        assert_eq!(
            resolved.context_source,
            ContextSource::RequestStation,
            "Station should be highest priority"
        );
        assert_eq!(resolved.confidence, 0.95);
    }

    #[test]
    fn resolve_hierarchy_falls_back_to_area_when_no_station() {
        let request_context = ContextInput {
            station_id: None,
            area: Some(AreaContextInput {
                prefecture_code: Some("13".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let resolved = ContextNormalizer::resolve_hierarchy(Some(&request_context), None);

        assert_eq!(resolved.context_source, ContextSource::RequestArea);
        assert_eq!(resolved.prefecture_code(), Some("13"));
    }

    #[test]
    fn resolve_hierarchy_uses_user_profile_when_no_request() {
        let user_area = AreaContextInput {
            prefecture_code: Some("01".to_string()),
            ..Default::default()
        };

        let resolved = ContextNormalizer::resolve_hierarchy(None, Some(&user_area));

        assert_eq!(resolved.context_source, ContextSource::UserProfileArea);
        assert_eq!(resolved.prefecture_code(), Some("01"));
        assert_eq!(resolved.confidence, 0.60, "User profile should have lower confidence");
    }

    #[test]
    fn resolve_hierarchy_falls_back_to_safe_context_when_empty() {
        let resolved = ContextNormalizer::resolve_hierarchy(None, None);

        assert_eq!(resolved.context_source, ContextSource::DefaultSafeContext);
        assert!(resolved.area.is_none());
    }

    #[test]
    fn confidence_decay_is_zero_for_fresh_evidence() {
        let decayed = ContextNormalizer::decay_confidence(0.88, 0.0, 72.0);
        assert_eq!(decayed, 0.88);
    }

    #[test]
    fn confidence_decay_reduces_old_evidence() {
        let fresh = ContextNormalizer::decay_confidence(0.88, 0.0, 72.0);
        let aged_24h = ContextNormalizer::decay_confidence(0.88, 24.0, 72.0);
        let aged_72h = ContextNormalizer::decay_confidence(0.88, 72.0, 72.0);

        assert!(aged_24h < fresh);
        assert!(aged_72h < aged_24h);
        assert!(aged_72h < 0.10);
    }

    #[test]
    fn blank_area_context_is_empty() {
        let area_input = AreaContextInput {
            country: Some("JP".to_string()),
            ..Default::default()
        };

        let resolved = ContextNormalizer::resolve_hierarchy(
            Some(&ContextInput {
                area: Some(area_input),
                ..Default::default()
            }),
            None,
        );

        assert_eq!(
            resolved.context_source,
            ContextSource::DefaultSafeContext,
            "Blank area should not be used; should fall back to safe"
        );
    }
}
