//! Pure streaming cascade math, scoring functions, weather index, and radar coordinate mapping.

use crate::types::{
    NetworkStorm, StormCategory, StormSeverity, StormStanceVerdict, StormStatus, TargetTier,
    WeatherLevel,
};

/// Computes `(replies + quotes) / (likes + reposts + 1.0)`.
///
/// # Examples
/// ```
/// assert!((skydoppler::math::compute_direct_ratio(40, 10, 4, 0) - 10.0).abs() < 1e-9);
/// ```
#[must_use]
pub fn compute_direct_ratio(replies: u64, quotes: u64, likes: u64, reposts: u64) -> f64 {
    let hostile_surface = replies.saturating_add(quotes) as f64;
    let approval_surface = likes.saturating_add(reposts) as f64 + 1.0;
    hostile_surface / approval_surface
}

/// Computes `catalyst_likes / (root_likes + 1.0)`.
///
/// # Examples
/// ```
/// assert!((skydoppler::math::compute_dunk_ratio(1000, 9) - 100.0).abs() < 1e-9);
/// ```
#[must_use]
pub fn compute_dunk_ratio(catalyst_likes: u64, root_likes: u64) -> f64 {
    catalyst_likes as f64 / (root_likes as f64 + 1.0)
}

/// Computes `catalyst_followers / (target_followers + 10.0)`, defaulting to `1.0` when unknown.
#[must_use]
pub fn compute_power_asymmetry(
    catalyst_followers: Option<u64>,
    target_followers: Option<u64>,
) -> f64 {
    match (catalyst_followers, target_followers) {
        (Some(cat), Some(tgt)) => (cat as f64) / (tgt as f64 + 10.0),
        _ => 1.0,
    }
}

/// Computes Poisson-normalized burst acceleration comparing `short_count` in `short_secs`
/// against the historical rate derived from `medium_count` over `medium_secs`.
#[must_use]
pub fn compute_burst_acceleration(
    short_count: usize,
    short_secs: f64,
    medium_count: usize,
    medium_secs: f64,
) -> f64 {
    if short_count == 0 || short_secs <= 0.0 || medium_secs <= short_secs {
        return 0.0;
    }
    let prior_count = medium_count.saturating_sub(short_count) as f64;
    let prior_secs = medium_secs - short_secs;
    let prior_rate = prior_count / prior_secs;
    let expected = (prior_rate * short_secs).max(0.5);
    (short_count as f64 - expected) / expected.sqrt()
}

/// Returns the vulnerability multiplier for [`TargetTier`], slightly boosting smaller accounts
/// while dampening mega-account ratio chatter unless corroborated by hostility.
#[must_use]
pub const fn compute_vulnerability_weight(tier: TargetTier) -> f64 {
    match tier {
        TargetTier::Small => 1.15,
        TargetTier::Mid => 1.05,
        TargetTier::Unknown => 1.0,
        TargetTier::Large => 0.90,
        TargetTier::PublicFigure => 0.80,
    }
}

/// Combines velocity, out-of-network share, direct ratio, dunk ratio, power asymmetry, burst z-score,
/// and defensive distress signals into a bounded `[0.0, 1.0]` structural storm score.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn compute_structural_score(
    oon_authors: usize,
    oon_ratio: f64,
    direct_ratio: f64,
    dunk_ratio: f64,
    power_asymmetry: f64,
    burst_z_score: f64,
    defensive_signals: usize,
    target_tier: TargetTier,
) -> f64 {
    let oon_ratio_clean = if oon_ratio.is_nan() {
        0.0
    } else {
        oon_ratio.clamp(0.0, 1.0)
    };
    let velocity_term = ((oon_authors as f64) / 20.0).clamp(0.0, 1.0);
    let ratio_term = (direct_ratio.max(0.0) / 3.0).clamp(0.0, 1.0);
    let dunk_term = (dunk_ratio.max(0.0) / 8.0).clamp(0.0, 1.0);
    let asym_term = if power_asymmetry.is_finite() {
        (power_asymmetry.max(1.0).log10() / 2.5).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let burst_term = (burst_z_score.max(0.0) / 8.0).clamp(0.0, 1.0);
    let distress_term = ((defensive_signals as f64) / 3.0).clamp(0.0, 1.0);

    let raw = 0.24 * velocity_term
        + 0.18 * oon_ratio_clean
        + 0.20 * ratio_term
        + 0.14 * dunk_term
        + 0.08 * asym_term
        + 0.08 * burst_term
        + 0.08 * distress_term;

    (raw * compute_vulnerability_weight(target_tier)).clamp(0.0, 1.0)
}

/// Fuses `structural_score` with optional Stage 3 LLM [`StormStanceVerdict`] into `[0.0, 1.0]`.
#[must_use]
pub fn compute_composite_score(structural: f64, stance: Option<&StormStanceVerdict>) -> f64 {
    let clean_struct = if structural.is_nan() {
        0.0
    } else {
        structural.clamp(0.0, 1.0)
    };
    let Some(verdict) = stance else {
        return clean_struct;
    };
    if verdict.evaluator == "structural_fallback" {
        return clean_struct;
    }

    let stance_signal = if verdict.collective_hostility {
        match verdict.category {
            StormCategory::Dogpile => verdict.confidence,
            StormCategory::PublicFigureBacklash => 0.85 * verdict.confidence,
            StormCategory::HeatedDebate => 0.55 * verdict.confidence,
            StormCategory::Uncertain => 0.45 * verdict.confidence,
            StormCategory::BenignViral => 0.10,
        }
    } else {
        match verdict.category {
            StormCategory::BenignViral => 0.05 * (1.0 - verdict.confidence),
            StormCategory::HeatedDebate => 0.35 * verdict.confidence,
            _ => 0.20,
        }
    };

    (0.55 * clean_struct + 0.45 * stance_signal).clamp(0.0, 1.0)
}

/// Maps a composite score into [`StormSeverity`].
///
/// # Examples
/// ```
/// use skydoppler::math::severity_from_score;
/// use skydoppler::types::StormSeverity;
///
/// assert_eq!(severity_from_score(0.90), StormSeverity::Critical);
/// assert_eq!(severity_from_score(0.75), StormSeverity::High);
/// assert_eq!(severity_from_score(0.65), StormSeverity::Elevated);
/// ```
#[must_use]
pub fn severity_from_score(score: f64) -> StormSeverity {
    if score.is_nan() {
        return StormSeverity::Elevated;
    }
    if score >= 0.88 {
        StormSeverity::Critical
    } else if score >= 0.74 {
        StormSeverity::High
    } else {
        StormSeverity::Elevated
    }
}

/// Maps an atmospheric weather index in `[0.0, 100.0]` to [`WeatherLevel`].
#[must_use]
pub fn classify_weather_level(weather_index: f64) -> WeatherLevel {
    if weather_index.is_nan() || weather_index < 15.0 {
        WeatherLevel::Clear
    } else if weather_index < 35.0 {
        WeatherLevel::Breezy
    } else if weather_index < 60.0 {
        WeatherLevel::Squall
    } else if weather_index < 80.0 {
        WeatherLevel::Storm
    } else {
        WeatherLevel::Supercell
    }
}

/// Computes an aggregate network atmospheric weather index in `[0.0, 100.0]` across active storms.
#[must_use]
pub fn compute_weather_index(storms: &[NetworkStorm]) -> f64 {
    let mut sum = 0.0_f64;
    for storm in storms {
        if storm.status != StormStatus::Active {
            continue;
        }
        let base = match storm.severity {
            StormSeverity::Elevated => 8.0,
            StormSeverity::High => 16.0,
            StormSeverity::Critical => 26.0,
        };
        let scale = 0.75 + 0.50 * storm.peak_score.clamp(0.0, 1.0);
        sum += base * scale;
    }
    sum.clamp(0.0, 100.0)
}

/// Computes deterministic polar `(angle_deg, radius)` radar coordinates for `focal_post_uri`
/// and `composite_score`: `angle_deg` is in `[0.0, 360.0)` and `radius` is in `[0.15, 0.95]`
/// where higher-severity storms appear closer to the radar center.
#[must_use]
pub fn radar_blip_coordinates(focal_post_uri: &str, composite_score: f64) -> (f64, f64) {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in focal_post_uri.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    let angle_deg = ((hash % 3600) as f64) / 10.0;
    let clean_score = if composite_score.is_nan() {
        0.5
    } else {
        composite_score.clamp(0.0, 1.0)
    };
    let radius = (0.95 - clean_score * 0.80).clamp(0.15, 0.95);
    (angle_deg, radius)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use crate::types::StormMetrics;

    fn mk_storm(severity: StormSeverity, peak_score: f64, status: StormStatus) -> NetworkStorm {
        NetworkStorm {
            id: 1,
            cell_id: "cell-00000001".to_string(),
            target_did: "did:plc:alice".to_string(),
            target_handle: Some("alice.bsky.social".to_string()),
            target_followers: Some(1_000),
            target_tier: TargetTier::Small,
            focal_post_uri: "at://did:plc:alice/app.bsky.feed.post/3k1".to_string(),
            focal_post_snippet: "post".to_string(),
            status,
            severity,
            category: StormCategory::Dogpile,
            peak_score,
            radar_angle_deg: 45.0,
            radar_radius: 0.4,
            started_at_us: 1,
            updated_at_us: 2,
            resolved_at_us: None,
            metrics: StormMetrics::default(),
            top_catalyst: None,
            stance: None,
        }
    }

    #[test]
    fn ratios_power_asymmetry_and_burst() {
        assert!((compute_direct_ratio(0, 0, 0, 0) - 0.0).abs() < 1e-9);
        assert!((compute_direct_ratio(40, 10, 4, 0) - 10.0).abs() < 1e-9);
        assert!((compute_dunk_ratio(1000, 9) - 100.0).abs() < 1e-9);

        assert!((compute_power_asymmetry(None, Some(100)) - 1.0).abs() < 1e-9);
        assert!((compute_power_asymmetry(Some(100), None) - 1.0).abs() < 1e-9);
        assert!((compute_power_asymmetry(Some(50_000), Some(90)) - 500.0).abs() < 1e-9);

        assert!((compute_burst_acceleration(0, 300.0, 10, 1800.0) - 0.0).abs() < 1e-9);
        assert!((compute_burst_acceleration(10, 0.0, 10, 1800.0) - 0.0).abs() < 1e-9);
        assert!((compute_burst_acceleration(10, 300.0, 10, 300.0) - 0.0).abs() < 1e-9);
        let spike = compute_burst_acceleration(20, 300.0, 20, 1800.0);
        assert!(spike > 10.0);

        // Non-zero prior_count tests exact burst arithmetic across all operators.
        let exact_burst = compute_burst_acceleration(16, 2.0, 32, 6.0);
        assert!((exact_burst - (8.0 / 8.0_f64.sqrt())).abs() < 1e-9);
    }

    #[test]
    fn structural_composite_vulnerability_and_severity() {
        assert!((compute_vulnerability_weight(TargetTier::Small) - 1.15).abs() < 1e-9);
        assert!((compute_vulnerability_weight(TargetTier::Mid) - 1.05).abs() < 1e-9);
        assert!((compute_vulnerability_weight(TargetTier::Unknown) - 1.0).abs() < 1e-9);
        assert!((compute_vulnerability_weight(TargetTier::Large) - 0.90).abs() < 1e-9);
        assert!((compute_vulnerability_weight(TargetTier::PublicFigure) - 0.80).abs() < 1e-9);

        let high_small =
            compute_structural_score(30, 0.95, 5.0, 12.0, 150.0, 10.0, 4, TargetTier::Small);
        assert!(high_small > 0.85 && high_small <= 1.0);

        // Isolated non-saturated term checks for compute_structural_score
        assert!(
            (compute_structural_score(10, 0.0, 0.0, 0.0, 1.0, 0.0, 0, TargetTier::Unknown) - 0.12)
                .abs()
                < 1e-9
        );
        assert!(
            (compute_structural_score(0, 0.5, 0.0, 0.0, 1.0, 0.0, 0, TargetTier::Unknown) - 0.09)
                .abs()
                < 1e-9
        );
        assert!(
            (compute_structural_score(0, 0.0, 1.5, 0.0, 1.0, 0.0, 0, TargetTier::Unknown) - 0.10)
                .abs()
                < 1e-9
        );
        assert!(
            (compute_structural_score(0, 0.0, 0.0, 2.0, 1.0, 0.0, 0, TargetTier::Unknown) - 0.035)
                .abs()
                < 1e-9
        );
        assert!(
            (compute_structural_score(0, 0.0, 0.0, 0.0, 10.0, 0.0, 0, TargetTier::Unknown) - 0.032)
                .abs()
                < 1e-9
        );
        assert!(
            (compute_structural_score(0, 0.0, 0.0, 0.0, 0.5, 0.0, 0, TargetTier::Unknown) - 0.0)
                .abs()
                < 1e-9
        );
        assert!(
            (compute_structural_score(
                0,
                0.0,
                0.0,
                0.0,
                f64::INFINITY,
                0.0,
                0,
                TargetTier::Unknown
            ) - 0.0)
                .abs()
                < 1e-9
        );
        assert!(
            (compute_structural_score(0, 0.0, 0.0, 0.0, 1.0, 2.0, 0, TargetTier::Unknown) - 0.02)
                .abs()
                < 1e-9
        );
        assert!(
            (compute_structural_score(0, 0.0, 0.0, 0.0, 1.0, 0.0, 1, TargetTier::Unknown)
                - (0.08 / 3.0))
                .abs()
                < 1e-9
        );

        let expected_combined = 0.12 + 0.09 + 0.10 + 0.035 + 0.032 + 0.02 + (0.08 / 3.0);
        let actual_combined =
            compute_structural_score(10, 0.5, 1.5, 2.0, 10.0, 2.0, 1, TargetTier::Unknown);
        assert!((actual_combined - expected_combined).abs() < 1e-9);

        let actual_small =
            compute_structural_score(10, 0.5, 1.5, 2.0, 10.0, 2.0, 1, TargetTier::Small);
        assert!((actual_small - expected_combined * 1.15).abs() < 1e-9);

        let nan_inputs =
            compute_structural_score(0, f64::NAN, 0.0, 0.0, 0.5, 0.0, 0, TargetTier::Unknown);
        assert!((nan_inputs - 0.0).abs() < 1e-9);

        let dogpile = StormStanceVerdict::new(true, 0.95, StormCategory::Dogpile, "d", "llm");
        let benign = StormStanceVerdict::new(false, 0.50, StormCategory::BenignViral, "b", "llm");
        let backlash =
            StormStanceVerdict::new(true, 0.80, StormCategory::PublicFigureBacklash, "p", "llm");
        let debate_hostile =
            StormStanceVerdict::new(true, 0.80, StormCategory::HeatedDebate, "h", "llm");
        let debate_calm =
            StormStanceVerdict::new(false, 0.80, StormCategory::HeatedDebate, "h", "llm");
        let unc = StormStanceVerdict::new(true, 0.80, StormCategory::Uncertain, "u", "llm");
        let unc_nonhostile =
            StormStanceVerdict::new(false, 0.80, StormCategory::Uncertain, "u", "llm");
        let fallback = StormStanceVerdict::new(
            false,
            0.0,
            StormCategory::Uncertain,
            "fb",
            "structural_fallback",
        );

        assert!((compute_composite_score(f64::NAN, None) - 0.0).abs() < 1e-9);
        assert!((compute_composite_score(0.77, Some(&fallback)) - 0.77).abs() < 1e-9);
        assert!(
            (compute_composite_score(0.70, Some(&dogpile)) - (0.55 * 0.70 + 0.45 * 0.95)).abs()
                < 1e-9
        );
        assert!((compute_composite_score(0.50, Some(&benign)) - 0.28625).abs() < 1e-9);
        assert!((compute_composite_score(0.50, Some(&backlash)) - 0.581).abs() < 1e-9);
        assert!((compute_composite_score(0.50, Some(&debate_hostile)) - 0.473).abs() < 1e-9);
        assert!((compute_composite_score(0.50, Some(&debate_calm)) - 0.401).abs() < 1e-9);
        assert!((compute_composite_score(0.50, Some(&unc)) - 0.437).abs() < 1e-9);
        assert!(
            (compute_composite_score(0.50, Some(&unc_nonhostile)) - (0.55 * 0.50 + 0.45 * 0.20))
                .abs()
                < 1e-9
        );

        assert_eq!(severity_from_score(f64::NAN), StormSeverity::Elevated);
        assert_eq!(severity_from_score(0.8799), StormSeverity::High);
        assert_eq!(severity_from_score(0.88), StormSeverity::Critical);
        assert_eq!(severity_from_score(0.7399), StormSeverity::Elevated);
        assert_eq!(severity_from_score(0.74), StormSeverity::High);
    }

    #[test]
    fn weather_index_levels_and_radar_coordinates() {
        assert_eq!(classify_weather_level(f64::NAN), WeatherLevel::Clear);
        assert_eq!(classify_weather_level(14.9), WeatherLevel::Clear);
        assert_eq!(classify_weather_level(15.0), WeatherLevel::Breezy);
        assert_eq!(classify_weather_level(34.9), WeatherLevel::Breezy);
        assert_eq!(classify_weather_level(35.0), WeatherLevel::Squall);
        assert_eq!(classify_weather_level(59.9), WeatherLevel::Squall);
        assert_eq!(classify_weather_level(60.0), WeatherLevel::Storm);
        assert_eq!(classify_weather_level(79.9), WeatherLevel::Storm);
        assert_eq!(classify_weather_level(80.0), WeatherLevel::Supercell);

        let storms = vec![
            mk_storm(StormSeverity::Elevated, 0.60, StormStatus::Active),
            mk_storm(StormSeverity::High, 0.80, StormStatus::Active),
            mk_storm(StormSeverity::Critical, 0.95, StormStatus::Active),
            mk_storm(StormSeverity::Critical, 1.0, StormStatus::Resolved),
        ];
        let idx = compute_weather_index(&storms);
        assert!(idx > 50.0 && idx < 70.0);

        let single = [mk_storm(StormSeverity::Elevated, 0.60, StormStatus::Active)];
        assert!((compute_weather_index(&single) - 8.4).abs() < 1e-9);

        let (a1, r1) = radar_blip_coordinates("at://did:plc:alice/app.bsky.feed.post/1", 0.95);
        let (a2, r2) = radar_blip_coordinates("at://did:plc:bob/app.bsky.feed.post/2", 0.50);
        assert!((0.0..360.0).contains(&a1));
        assert!((0.0..360.0).contains(&a2));
        assert!(a1 >= 10.0 || a2 >= 10.0);
        assert!((0.15..=0.95).contains(&r1));
        assert!((0.15..=0.95).contains(&r2));
        assert!(r1 < r2);
        assert_ne!(a1, a2);
        assert_ne!(
            radar_blip_coordinates("\0", 0.5).0,
            radar_blip_coordinates("%", 0.5).0
        );
        let (_, r_nan) = radar_blip_coordinates("uri", f64::NAN);
        assert!((r_nan - 0.55).abs() < 1e-6);
    }
}
