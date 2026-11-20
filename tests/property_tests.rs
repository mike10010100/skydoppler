#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use proptest::prelude::*;
use serde_json::json;
use skybase::ingest::events::{CommitOperation, JetstreamCommit};
use skydoppler::math::{
    compute_composite_score, compute_structural_score, compute_weather_index,
    radar_blip_coordinates,
};
use skydoppler::redact::{
    opaque_cell_id, should_redact_publicly, PublicStormView, WireRadarEvent,
    REDACTION_REASON_SMALL_ACCOUNT,
};
use skydoppler::types::{
    CatalystInfo, NetworkStorm, RadarConfig, RadarEvent, StormCategory, StormMetrics,
    StormSeverity, StormStanceVerdict, StormStatus, TargetTier,
};
use skydoppler::NetworkTracker;

fn build_storm_fixture(
    target_id: &str,
    handle: &str,
    snippet: &str,
    catalyst_handle: &str,
    followers: Option<u64>,
) -> NetworkStorm {
    let focal_uri = format!("at://did:plc:{target_id}/app.bsky.feed.post/3kfocal999");
    NetworkStorm {
        id: 7,
        cell_id: opaque_cell_id(&focal_uri),
        target_did: format!("did:plc:{target_id}"),
        target_handle: Some(format!("{handle}.bsky.social")),
        target_followers: followers,
        target_tier: TargetTier::from_followers(followers),
        focal_post_uri: focal_uri,
        focal_post_snippet: snippet.to_string(),
        status: StormStatus::Active,
        severity: StormSeverity::High,
        category: StormCategory::Dogpile,
        peak_score: 0.85,
        radar_angle_deg: 45.0,
        radar_radius: 0.30,
        started_at_us: 1_000_000,
        updated_at_us: 2_000_000,
        resolved_at_us: None,
        metrics: StormMetrics::default(),
        top_catalyst: Some(CatalystInfo {
            post_uri: format!("at://did:plc:cat{catalyst_handle}/app.bsky.feed.post/3kcat999"),
            author_did: format!("did:plc:cat{catalyst_handle}"),
            author_handle: Some(format!("{catalyst_handle}.bsky.social")),
            author_followers: Some(95_000),
            like_count: 1500,
            repost_count: 120,
            quote_count: 10,
            dunk_ratio: 15.0,
            text_snippet: format!("Catalyst quote attacking {snippet}"),
        }),
        stance: Some(StormStanceVerdict::new(
            true,
            0.9,
            StormCategory::Dogpile,
            "Hostile pile-on",
            "mock",
        )),
    }
}

proptest! {
    #[test]
    fn redaction_never_leaks_identifying_fields_when_small_or_unknown(
        target_id in "[a-z0-9]{8,14}",
        handle in "[a-z]{6,12}",
        snippet in "[A-Za-z0-9]{12,24}",
        cat_handle in "[a-z]{6,12}",
        threshold in 100_u64..=50_000,
        below_offset in 1_u64..=50_000,
        use_none in any::<bool>(),
    ) {
        let followers = if use_none {
            None
        } else {
            Some(threshold.saturating_sub((below_offset % threshold).max(1)))
        };
        let storm = build_storm_fixture(&target_id, &handle, &snippet, &cat_handle, followers);
        prop_assert!(should_redact_publicly(&storm, threshold));

        let view = PublicStormView::from_storm(&storm, threshold);
        prop_assert!(view.redacted);
        prop_assert_eq!(view.redaction_reason.as_deref(), Some(REDACTION_REASON_SMALL_ACCOUNT));
        prop_assert!(view.target_did.is_none());
        prop_assert!(view.target_handle.is_none());
        prop_assert!(view.focal_post_uri.is_none());
        prop_assert!(view.focal_post_snippet.is_none());
        prop_assert!(view.top_catalyst.is_none());

        let view_json = serde_json::to_string(&view).unwrap();
        prop_assert!(!view_json.contains(&storm.target_did));
        prop_assert!(!view_json.contains(&handle));
        prop_assert!(!view_json.contains(&snippet));
        prop_assert!(!view_json.contains(&cat_handle));
        prop_assert!(!view_json.contains(&storm.focal_post_uri));

        let sse_ev = RadarEvent::StormUpsert {
            storm: Box::new(storm.clone()),
            is_escalation: true,
        };
        let pub_sse = WireRadarEvent::project(&sse_ev, false, threshold);
        let pub_sse_json = serde_json::to_string(&pub_sse).unwrap();
        prop_assert!(!pub_sse_json.contains(&storm.target_did));
        prop_assert!(!pub_sse_json.contains(&snippet));
        prop_assert!(!pub_sse_json.contains(&cat_handle));

        // At >= threshold followers, public visibility is allowed
        let public_storm = build_storm_fixture(&target_id, &handle, &snippet, &cat_handle, Some(threshold));
        prop_assert!(!should_redact_publicly(&public_storm, threshold));
        let unredacted = PublicStormView::from_storm(&public_storm, threshold);
        prop_assert!(!unredacted.redacted);
        prop_assert_eq!(unredacted.target_did.as_deref(), Some(public_storm.target_did.as_str()));
    }

    #[test]
    fn scoring_weather_and_coordinates_stay_strictly_bounded(
        oon_authors in 0usize..2000,
        oon_ratio in -10.0f64..10.0,
        direct_ratio in -5.0f64..500.0,
        dunk_ratio in -5.0f64..500.0,
        power_asym in -5.0f64..100_000.0,
        burst_z in -10.0f64..200.0,
        distress in 0usize..50,
        confidence in -2.0f64..3.0,
        hostile in any::<bool>(),
        active_count in 0usize..30,
        uri in "at://did:plc:[a-z0-9]{6,12}/app\\.bsky\\.feed\\.post/[a-z0-9]{4,10}",
    ) {
        for tier in [
            TargetTier::Small,
            TargetTier::Mid,
            TargetTier::Large,
            TargetTier::PublicFigure,
            TargetTier::Unknown,
        ] {
            let s = compute_structural_score(
                oon_authors,
                oon_ratio,
                direct_ratio,
                dunk_ratio,
                power_asym,
                burst_z,
                distress,
                tier,
            );
            prop_assert!(s.is_finite() && (0.0..=1.0).contains(&s));

            let verdict = StormStanceVerdict::new(hostile, confidence, StormCategory::Dogpile, "prop", "llm");
            let c = compute_composite_score(s, Some(&verdict));
            prop_assert!(c.is_finite() && (0.0..=1.0).contains(&c));

            let (angle, radius) = radar_blip_coordinates(&uri, c);
            prop_assert!(angle.is_finite() && (0.0..360.0).contains(&angle));
            prop_assert!(radius.is_finite() && (0.15..=0.95).contains(&radius));
        }

        let storms: Vec<NetworkStorm> = (0..active_count)
            .map(|idx| NetworkStorm {
                id: idx as i64,
                cell_id: format!("cell-{idx:08x}"),
                target_did: format!("did:plc:u{idx}"),
                target_handle: None,
                target_followers: Some(100),
                target_tier: TargetTier::Small,
                focal_post_uri: uri.clone(),
                focal_post_snippet: String::new(),
                status: StormStatus::Active,
                severity: match idx % 3 {
                    0 => StormSeverity::Critical,
                    1 => StormSeverity::High,
                    _ => StormSeverity::Elevated,
                },
                category: StormCategory::Dogpile,
                peak_score: 0.9,
                radar_angle_deg: 0.0,
                radar_radius: 0.5,
                started_at_us: 1,
                updated_at_us: 1,
                resolved_at_us: None,
                metrics: StormMetrics::default(),
                top_catalyst: None,
                stance: None,
            })
            .collect();
        let w = compute_weather_index(&storms);
        prop_assert!(w.is_finite() && (0.0..=100.0).contains(&w));
    }

    #[test]
    fn tracker_never_exceeds_configured_table_caps(
        ops in prop::collection::vec((0u16..150, 0u16..150, any::<bool>()), 1..160),
    ) {
        let cfg = RadarConfig::default().with_table_caps(24, 20);
        let tracker = NetworkTracker::new(cfg);

        let mut ts = 1_700_000_000_000_000_u64;
        for (target_idx, actor_idx, is_block) in ops {
            ts += 1_000;
            let commit = if is_block {
                JetstreamCommit {
                    did: format!("did:plc:tgt{target_idx}"),
                    time_us: ts,
                    collection: "app.bsky.graph.block".to_string(),
                    rkey: format!("rk{ts}"),
                    operation: CommitOperation::Create,
                    cid: None,
                    record: Some(json!({ "subject": format!("did:plc:act{actor_idx}") })),
                }
            } else {
                let root_uri = format!("at://did:plc:tgt{target_idx}/app.bsky.feed.post/3k{target_idx}");
                JetstreamCommit {
                    did: format!("did:plc:act{actor_idx}"),
                    time_us: ts,
                    collection: "app.bsky.feed.post".to_string(),
                    rkey: format!("rk{ts}"),
                    operation: CommitOperation::Create,
                    cid: Some(format!("cid{ts}")),
                    record: Some(json!({
                        "text": "replying",
                        "reply": { "root": { "uri": root_uri }, "parent": { "uri": root_uri } }
                    })),
                }
            };
            let _ = tracker.observe_commit(&commit);
            let snap = tracker.snapshot();
            prop_assert!(snap.tracked_posts <= 24);
            prop_assert!(snap.tracked_authors <= 20);
        }
    }
}
