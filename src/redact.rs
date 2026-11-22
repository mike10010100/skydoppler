//! Fail-closed public redaction boundary protecting small accounts from secondary amplification.
//!
//! Targets with fewer than `public_follower_threshold` followers (or unknown follower counts)
//! are anonymized at the type boundary in [`PublicStormView`] before leaving the REST or SSE layer.

use serde::{Deserialize, Serialize};

use crate::types::{
    CatalystInfo, NetworkStorm, RadarEvent, StormCategory, StormMetrics, StormSeverity,
    StormStatus, TargetTier, WeatherSummary,
};

/// Fixed redaction explanation code emitted on redacted public storm views.
pub const REDACTION_REASON_SMALL_ACCOUNT: &str = "small_account_protection";

/// Deterministic opaque radar cell identifier derived from `focal_post_uri` via 64-bit FNV-1a.
///
/// # Examples
/// ```
/// let id1 = skydoppler::redact::opaque_cell_id("at://did:plc:alice/app.bsky.feed.post/1");
/// let id2 = skydoppler::redact::opaque_cell_id("at://did:plc:alice/app.bsky.feed.post/2");
/// assert!(id1.starts_with("cell-"));
/// assert_ne!(id1, id2);
/// ```
#[must_use]
pub fn opaque_cell_id(focal_post_uri: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in focal_post_uri.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("cell-{:08x}", (hash & 0xffff_ffff) as u32)
}

/// Returns `true` when `storm` MUST be redacted on public unauthenticated endpoints.
///
/// Fails closed: if `target_followers` is `None` **or** `< public_follower_threshold`, returns `true`.
///
/// # Examples
/// ```
/// use skydoppler::redact::should_redact_followers;
///
/// assert!(should_redact_followers(None, 20));
/// assert!(should_redact_followers(Some(19), 20));
/// assert!(!should_redact_followers(Some(20), 20));
/// ```
#[must_use]
pub const fn should_redact_followers(
    target_followers: Option<u64>,
    public_follower_threshold: u64,
) -> bool {
    match target_followers {
        None => true,
        Some(count) => count < public_follower_threshold,
    }
}

/// Convenience wrapper inspecting [`NetworkStorm::target_followers`] against `threshold`.
#[must_use]
pub const fn should_redact_publicly(storm: &NetworkStorm, threshold: u64) -> bool {
    should_redact_followers(storm.target_followers, threshold)
}

/// Publicly visible representation of a [`NetworkStorm`], strictly stripping PII on redacted cells.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicStormView {
    /// Database storm row identifier.
    pub id: i64,
    /// Stable opaque radar cell slug (`cell-<hex>`).
    pub cell_id: String,
    /// `true` when identifying post/account fields were stripped by small-account protection.
    pub redacted: bool,
    /// Redaction policy reason slug when `redacted == true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redaction_reason: Option<String>,
    /// Target follower audience tier.
    pub target_tier: TargetTier,
    /// Current lifecycle state.
    pub status: StormStatus,
    /// Severity tier.
    pub severity: StormSeverity,
    /// Classified storm category.
    pub category: StormCategory,
    /// Peak composite score observed.
    pub peak_score: f64,
    /// Radar polar angle in `[0.0, 360.0)`.
    pub radar_angle_deg: f64,
    /// Radar radial distance in `[0.15, 0.95]`.
    pub radar_radius: f64,
    /// Storm start timestamp (microseconds).
    pub started_at_us: u64,
    /// Latest update timestamp (microseconds).
    pub updated_at_us: u64,
    /// Resolution timestamp, if closed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at_us: Option<u64>,
    /// Aggregate quantitative telemetry metrics (non-identifying).
    pub metrics: StormMetrics,
    /// Non-identifying LLM stance summary, if evaluated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stance_classification: Option<StormCategory>,
    /// Target DID (`None` when redacted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_did: Option<String>,
    /// Target handle (`None` when redacted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_handle: Option<String>,
    /// Target follower count (`None` when redacted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_followers: Option<u64>,
    /// Canonical focal post AT-URI (`None` when redacted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focal_post_uri: Option<String>,
    /// Focal post snippet (`None` when redacted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focal_post_snippet: Option<String>,
    /// Amplifying quote catalyst (`None` when redacted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_catalyst: Option<CatalystInfo>,
}

/// Full operator-scoped storm cell view retaining all attribution plus public redaction status.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperatorStormView {
    /// Complete unredacted [`NetworkStorm`].
    #[serde(flatten)]
    pub storm: NetworkStorm,
    /// Whether this storm is redacted on public unauthenticated endpoints.
    pub redacted_for_public: bool,
}

impl PublicStormView {
    /// Projects a [`NetworkStorm`] into a fail-closed [`PublicStormView`].
    #[must_use]
    pub fn from_storm(storm: &NetworkStorm, public_follower_threshold: u64) -> Self {
        let redacted = should_redact_publicly(storm, public_follower_threshold);
        let stance_classification = storm.stance.as_ref().map(|s| s.category);

        if redacted {
            Self {
                id: storm.id,
                cell_id: storm.cell_id.clone(),
                redacted: true,
                redaction_reason: Some(REDACTION_REASON_SMALL_ACCOUNT.to_string()),
                target_tier: storm.target_tier,
                status: storm.status,
                severity: storm.severity,
                category: storm.category,
                peak_score: storm.peak_score,
                radar_angle_deg: storm.radar_angle_deg,
                radar_radius: storm.radar_radius,
                started_at_us: storm.started_at_us,
                updated_at_us: storm.updated_at_us,
                resolved_at_us: storm.resolved_at_us,
                metrics: storm.metrics.clone(),
                stance_classification,
                target_did: None,
                target_handle: None,
                target_followers: None,
                focal_post_uri: None,
                focal_post_snippet: None,
                top_catalyst: None,
            }
        } else {
            Self {
                id: storm.id,
                cell_id: storm.cell_id.clone(),
                redacted: false,
                redaction_reason: None,
                target_tier: storm.target_tier,
                status: storm.status,
                severity: storm.severity,
                category: storm.category,
                peak_score: storm.peak_score,
                radar_angle_deg: storm.radar_angle_deg,
                radar_radius: storm.radar_radius,
                started_at_us: storm.started_at_us,
                updated_at_us: storm.updated_at_us,
                resolved_at_us: storm.resolved_at_us,
                metrics: storm.metrics.clone(),
                stance_classification,
                target_did: Some(storm.target_did.clone()),
                target_handle: storm.target_handle.clone(),
                target_followers: storm.target_followers,
                focal_post_uri: Some(storm.focal_post_uri.clone()),
                focal_post_snippet: if storm.focal_post_snippet.trim().is_empty() {
                    None
                } else {
                    Some(storm.focal_post_snippet.clone())
                },
                top_catalyst: storm.top_catalyst.clone(),
            }
        }
    }
}

impl OperatorStormView {
    /// Projects a [`NetworkStorm`] into an unredacted [`OperatorStormView`].
    #[must_use]
    pub fn from_storm(storm: NetworkStorm, public_follower_threshold: u64) -> Self {
        let redacted_for_public = should_redact_publicly(&storm, public_follower_threshold);
        Self {
            storm,
            redacted_for_public,
        }
    }
}

/// Wire SSE event delivered to public vs. operator streams via explicit type-safe projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WireRadarEvent {
    /// Storm event projected for public unauthenticated subscribers.
    PublicStormUpsert {
        /// Redaction-enforced public storm view.
        storm: PublicStormView,
        /// Whether severity escalated.
        is_escalation: bool,
    },
    /// Full unredacted storm event delivered exclusively to authenticated operators.
    OperatorStormUpsert {
        /// Complete operator view.
        storm: OperatorStormView,
        /// Whether severity escalated.
        is_escalation: bool,
    },
    /// Storm lifecycle closure notice (safe for all subscribers).
    StormClosed {
        /// Database storm ID.
        storm_id: i64,
        /// Opaque radar cell ID.
        cell_id: String,
        /// Closing status (`Resolved` or `Dismissed`).
        status: StormStatus,
    },
    /// Periodic macro weather index pulse.
    WeatherPulse {
        /// Aggregate weather summary.
        weather: WeatherSummary,
    },
}

impl WireRadarEvent {
    /// Converts an internal [`RadarEvent`] into a [`WireRadarEvent`], redacting unless `is_operator == true`.
    #[must_use]
    pub fn project(event: &RadarEvent, is_operator: bool, public_follower_threshold: u64) -> Self {
        match event {
            RadarEvent::StormUpsert {
                storm,
                is_escalation,
            } => {
                if is_operator {
                    Self::OperatorStormUpsert {
                        storm: OperatorStormView::from_storm(
                            storm.as_ref().clone(),
                            public_follower_threshold,
                        ),
                        is_escalation: *is_escalation,
                    }
                } else {
                    Self::PublicStormUpsert {
                        storm: PublicStormView::from_storm(storm, public_follower_threshold),
                        is_escalation: *is_escalation,
                    }
                }
            }
            RadarEvent::StormClosed {
                storm_id,
                cell_id,
                status,
            } => Self::StormClosed {
                storm_id: *storm_id,
                cell_id: cell_id.clone(),
                status: *status,
            },
            RadarEvent::WeatherPulse { weather } => Self::WeatherPulse {
                weather: weather.clone(),
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use crate::types::StormStanceVerdict;

    fn sample_storm(followers: Option<u64>) -> NetworkStorm {
        NetworkStorm {
            id: 42,
            cell_id: opaque_cell_id("at://did:plc:secret_target/app.bsky.feed.post/3ksecret"),
            target_did: "did:plc:secret_target".to_string(),
            target_handle: Some("secretuser.bsky.social".to_string()),
            target_followers: followers,
            target_tier: TargetTier::from_followers(followers),
            focal_post_uri: "at://did:plc:secret_target/app.bsky.feed.post/3ksecret".to_string(),
            focal_post_snippet: "Sensitive personal post text".to_string(),
            status: StormStatus::Active,
            severity: StormSeverity::High,
            category: StormCategory::Dogpile,
            peak_score: 0.84,
            radar_angle_deg: 120.5,
            radar_radius: 0.28,
            started_at_us: 100,
            updated_at_us: 200,
            resolved_at_us: None,
            metrics: StormMetrics::default(),
            top_catalyst: Some(CatalystInfo {
                post_uri: "at://did:plc:bigcat/app.bsky.feed.post/3kcat".to_string(),
                author_did: "did:plc:bigcat".to_string(),
                author_handle: Some("bigcat.bsky.social".to_string()),
                author_followers: Some(90_000),
                like_count: 900,
                repost_count: 100,
                quote_count: 10,
                dunk_ratio: 15.0,
                text_snippet: "Catalyst dunk text".to_string(),
            }),
            stance: Some(StormStanceVerdict::new(
                true,
                0.9,
                StormCategory::Dogpile,
                "summary",
                "llm",
            )),
        }
    }

    #[test]
    fn fail_closed_redacts_small_and_unknown_accounts_and_exposes_large() {
        for followers in [None, Some(0), Some(19)] {
            let storm = sample_storm(followers);
            assert!(should_redact_publicly(&storm, 20));
            let pub_view = PublicStormView::from_storm(&storm, 20);
            assert!(pub_view.redacted);
            assert_eq!(
                pub_view.redaction_reason.as_deref(),
                Some(REDACTION_REASON_SMALL_ACCOUNT)
            );
            assert!(pub_view.target_did.is_none());
            assert!(pub_view.target_handle.is_none());
            assert!(pub_view.focal_post_uri.is_none());
            assert!(pub_view.focal_post_snippet.is_none());
            assert!(pub_view.top_catalyst.is_none());

            let json = serde_json::to_string(&pub_view).unwrap();
            assert!(!json.contains("did:plc:secret_target"));
            assert!(!json.contains("secretuser.bsky.social"));
            assert!(!json.contains("Sensitive personal post text"));
            assert!(!json.contains("bigcat.bsky.social"));
            assert!(!json.contains("Catalyst dunk text"));
        }

        // Exact boundary >= 20 is unredacted publicly
        let large = sample_storm(Some(20));
        assert!(!should_redact_publicly(&large, 20));
        let pub_large = PublicStormView::from_storm(&large, 20);
        assert!(!pub_large.redacted);
        assert!(pub_large.redaction_reason.is_none());
        assert_eq!(
            pub_large.target_did.as_deref(),
            Some("did:plc:secret_target")
        );
        assert!(pub_large.top_catalyst.is_some());

        // Blank focal snippet normalizes to None in PublicStormView
        let mut blank_snip = large.clone();
        blank_snip.focal_post_snippet = "   ".to_string();
        assert!(PublicStormView::from_storm(&blank_snip, 20)
            .focal_post_snippet
            .is_none());

        // Operator view preserves full details regardless of follower count
        let small = sample_storm(Some(15));
        let op_view = OperatorStormView::from_storm(small.clone(), 20);
        assert!(op_view.redacted_for_public);
        assert_eq!(op_view.storm.target_did, "did:plc:secret_target");

        // SSE projection enforces the exact same privacy split
        let ev = RadarEvent::StormUpsert {
            storm: Box::new(small),
            is_escalation: true,
        };
        let pub_ev = WireRadarEvent::project(&ev, false, 20);
        let pub_ev_json = serde_json::to_string(&pub_ev).unwrap();
        assert!(!pub_ev_json.contains("did:plc:secret_target"));
        assert!(!pub_ev_json.contains("Sensitive personal post text"));

        let op_ev = WireRadarEvent::project(&ev, true, 20);
        assert!(matches!(op_ev, WireRadarEvent::OperatorStormUpsert { .. }));

        let closed = RadarEvent::StormClosed {
            storm_id: 42,
            cell_id: "cell-123".to_string(),
            status: StormStatus::Resolved,
        };
        assert!(matches!(
            WireRadarEvent::project(&closed, false, 20),
            WireRadarEvent::StormClosed { .. }
        ));

        // Opaque cell ID format and FNV XOR sensitivity
        let c1 = opaque_cell_id("at://did:plc:alice/app.bsky.feed.post/1");
        let c2 = opaque_cell_id("at://did:plc:alice/app.bsky.feed.post/2");
        assert!(c1.starts_with("cell-") && c1.len() == 13);
        assert!(c1[5..].chars().all(|ch| ch.is_ascii_hexdigit()));
        assert_ne!(c1, c2);
        assert_ne!(opaque_cell_id("\0"), opaque_cell_id("%"));
        assert_ne!(opaque_cell_id("\0"), opaque_cell_id("\x02"));
    }
}
