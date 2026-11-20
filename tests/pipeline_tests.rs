#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use skybase::ingest::MockJetstreamServer;
use skydoppler::redact::{PublicStormView, WireRadarEvent};
use skydoppler::stance::{LlmStormStanceClassifier, StanceConfig};
use skydoppler::stream::{run_jetstream_streamer, StreamConfig};
use skydoppler::time::current_time_us;
use skydoppler::{
    RadarConfig, RadarEvent, SkydopplerEngineBuilder, StormCategory, StormStore, TargetTier,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

struct AppViewRouterMock;

impl Respond for AppViewRouterMock {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let p = req.url.path();
        let query = req.url.query().unwrap_or("");
        if p == "/xrpc/app.bsky.feed.getPosts" {
            let mut posts = Vec::new();
            for pair in query.split('&') {
                if let Some((k, v)) = pair.split_once('=') {
                    if k == "uris" {
                        let uri = url::form_urlencoded::parse(v.as_bytes())
                            .map(|(a, _)| a.into_owned())
                            .next()
                            .unwrap_or_else(|| v.replace("%3A", ":").replace("%2F", "/"));
                        let obj = if uri.contains("did:plc:smallvictim") && uri.ends_with("/3kroot")
                        {
                            json!({
                                "uri": uri,
                                "author": { "did": "did:plc:smallvictim", "handle": "smallvictim.bsky.social", "followersCount": 420 },
                                "record": { "text": "I just like sourdough bread." },
                                "replyCount": 45, "repostCount": 1, "likeCount": 3, "quoteCount": 9
                            })
                        } else if uri.contains("did:plc:catalystqt") {
                            json!({
                                "uri": uri,
                                "author": { "did": "did:plc:catalystqt", "handle": "bigdunker.bsky.social", "followersCount": 120000 },
                                "record": { "text": "Look at this absurd take lol" },
                                "replyCount": 20, "repostCount": 110, "likeCount": 1800, "quoteCount": 12
                            })
                        } else if uri.contains("did:plc:pubfig") {
                            json!({
                                "uri": uri,
                                "author": { "did": "did:plc:pubfig", "handle": "celeb.bsky.social", "followersCount": 250000 },
                                "record": { "text": "Controversial policy announcement" },
                                "replyCount": 95, "repostCount": 4, "likeCount": 10, "quoteCount": 20
                            })
                        } else if uri.contains("did:plc:benign") {
                            json!({
                                "uri": uri,
                                "author": { "did": "did:plc:benign", "handle": "artist.bsky.social", "followersCount": 18000 },
                                "record": { "text": "Cute rescue kitten photo" },
                                "replyCount": 12, "repostCount": 900, "likeCount": 8500, "quoteCount": 2
                            })
                        } else {
                            json!({
                                "uri": uri,
                                "author": { "did": "did:plc:distressed", "handle": "distressed.bsky.social", "followersCount": 180 },
                                "record": { "text": "Why are random strangers flooding me" },
                                "replyCount": 16, "repostCount": 0, "likeCount": 1, "quoteCount": 3
                            })
                        };
                        posts.push(obj);
                    }
                }
            }
            return ResponseTemplate::new(200).set_body_json(json!({ "posts": posts }));
        }

        if p == "/xrpc/app.bsky.actor.getProfiles" {
            let profiles = vec![
                json!({ "did": "did:plc:smallvictim", "handle": "smallvictim.bsky.social", "followersCount": 420 }),
                json!({ "did": "did:plc:catalystqt", "handle": "bigdunker.bsky.social", "followersCount": 120000 }),
                json!({ "did": "did:plc:pubfig", "handle": "celeb.bsky.social", "followersCount": 250000 }),
                json!({ "did": "did:plc:benign", "handle": "artist.bsky.social", "followersCount": 18000 }),
                json!({ "did": "did:plc:distressed", "handle": "distressed.bsky.social", "followersCount": 180 }),
            ];
            return ResponseTemplate::new(200).set_body_json(json!({ "profiles": profiles }));
        }

        if p == "/xrpc/app.bsky.graph.getRelationships" {
            let mut rels = Vec::new();
            for pair in query.split('&') {
                if let Some((k, v)) = pair.split_once('=') {
                    if k == "others" {
                        let did = v.replace("%3A", ":");
                        rels.push(json!({
                            "$type": "app.bsky.graph.defs#relationship",
                            "did": did
                        }));
                    }
                }
            }
            return ResponseTemplate::new(200).set_body_json(json!({
                "actor": "did:plc:target",
                "relationships": rels
            }));
        }

        ResponseTemplate::new(404)
    }
}

struct StanceMockRespond;

impl Respond for StanceMockRespond {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let body = String::from_utf8_lossy(&req.body);
        let verdict = if body.contains("celeb.bsky.social") {
            json!({
                "collective_hostility": true,
                "confidence": 0.91,
                "classification": "public_figure_backlash",
                "summary": "Public figure facing coordinated ratio backlash."
            })
        } else if body.contains("artist.bsky.social") {
            json!({
                "collective_hostility": false,
                "confidence": 0.97,
                "classification": "benign_viral",
                "summary": "Positive appreciation replies on viral art/pet post."
            })
        } else {
            json!({
                "collective_hostility": true,
                "confidence": 0.94,
                "classification": "dogpile",
                "summary": "Hostile out-of-network pile-on and quote-dunk wave."
            })
        };
        ResponseTemplate::new(200).set_body_json(json!({
            "message": { "content": verdict.to_string() }
        }))
    }
}

#[tokio::test]
async fn end_to_end_5000_commit_streaming_funnel_and_redaction_and_persistence() {
    let mock_ws = MockJetstreamServer::start().await.unwrap();
    let appview = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex("^/xrpc/app\\.bsky\\..*$"))
        .respond_with(AppViewRouterMock)
        .mount(&appview)
        .await;

    let llm = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/chat"))
        .respond_with(StanceMockRespond)
        .mount(&llm)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("pipeline.db");
    let store = Arc::new(StormStore::open(&db_path).unwrap());

    let cfg = RadarConfig::default()
        .with_min_unique_authors(6)
        .with_enable_llm_stance(true)
        .with_thresholds(0.32, 0.58, 0.62);
    let mut cfg = cfg;
    cfg.min_streaming_ratio = 0.45;
    cfg.appview_rps = 200.0;
    cfg.defensive_distress_threshold = 2;

    let stance = Arc::new(
        LlmStormStanceClassifier::new(StanceConfig {
            base_url: format!("{}/api/chat", llm.uri()),
            model: "qwen3.5:9b".to_string(),
            ..Default::default()
        })
        .unwrap(),
    );

    let engine = Arc::new(
        SkydopplerEngineBuilder::new(cfg.clone())
            .with_store(store.clone())
            .with_appview_endpoint(appview.uri())
            .with_stance_classifier(stance)
            .build()
            .unwrap(),
    );

    let mut events_rx = engine.subscribe_events();
    let (tx, rx) = mpsc::channel(8_192);
    let cancel = CancellationToken::new();

    let s_cancel = cancel.clone();
    let stream_cfg = StreamConfig::new(mock_ws.ws_url());
    let stream_handle =
        tokio::spawn(async move { run_jetstream_streamer(stream_cfg, tx, s_cancel).await });

    let e_ref = Arc::clone(&engine);
    let e_cancel = cancel.clone();
    let eng_handle = tokio::spawn(async move { e_ref.run(rx, e_cancel).await });

    for _ in 0..50 {
        if mock_ws.active_connections() >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(mock_ws.active_connections() >= 1);

    let mut now = current_time_us();

    // 1) Flash quote-dunk dogpile against small account (< 5,000 followers)
    let small_root = "at://did:plc:smallvictim/app.bsky.feed.post/3kroot";
    now += 1_000;
    mock_ws
        .emit_commit_payload(
            "did:plc:catalystqt",
            now,
            "app.bsky.feed.post",
            "3kcatpost",
            "create",
            Some("cidcat"),
            Some(json!({
                "text": "Ratio this small account",
                "embed": { "$type": "app.bsky.embed.record", "record": { "uri": small_root, "cid": "cidsm" } }
            })),
        )
        .unwrap();
    for idx in 0..8 {
        now += 1_000;
        mock_ws
            .emit_commit_payload(
                &format!("did:plc:oon_sm_{idx}"),
                now,
                "app.bsky.feed.post",
                &format!("3ksmr{idx}"),
                "create",
                Some("cid"),
                Some(json!({
                    "text": format!("Piling on #{idx}"),
                    "reply": { "root": { "uri": small_root }, "parent": { "uri": small_root } }
                })),
            )
            .unwrap();
    }

    // 2) Benign viral post: 8 replies overshadowed by live likes on Jetstream
    let benign_root = "at://did:plc:benign/app.bsky.feed.post/3kben";
    for idx in 0..3 {
        now += 1_000;
        mock_ws
            .emit_commit_payload(
                &format!("did:plc:fan_{idx}"),
                now,
                "app.bsky.feed.post",
                &format!("3kbr{idx}"),
                "create",
                Some("cid"),
                Some(json!({
                    "text": "So cute!",
                    "reply": { "root": { "uri": benign_root }, "parent": { "uri": benign_root } }
                })),
            )
            .unwrap();
    }
    for idx in 0..40 {
        now += 1_000;
        mock_ws
            .emit_commit_payload(
                &format!("did:plc:liker_{idx}"),
                now,
                "app.bsky.feed.like",
                &format!("3kbl{idx}"),
                "create",
                None,
                Some(json!({ "subject": { "uri": benign_root, "cid": "cidben" } })),
            )
            .unwrap();
    }
    for idx in 3..9 {
        now += 1_000;
        mock_ws
            .emit_commit_payload(
                &format!("did:plc:fan_{idx}"),
                now,
                "app.bsky.feed.post",
                &format!("3kbr{idx}"),
                "create",
                Some("cid"),
                Some(json!({
                    "text": "Adorable!",
                    "reply": { "root": { "uri": benign_root }, "parent": { "uri": benign_root } }
                })),
            )
            .unwrap();
    }

    // 3) Public figure backlash (>= 5,000 followers)
    let pub_root = "at://did:plc:pubfig/app.bsky.feed.post/3kpub";
    for idx in 0..8 {
        now += 1_000;
        mock_ws
            .emit_commit_payload(
                &format!("did:plc:crit_{idx}"),
                now,
                "app.bsky.feed.post",
                &format!("3kpr{idx}"),
                "create",
                Some("cid"),
                Some(json!({
                    "text": "Terrible policy announcement",
                    "reply": { "root": { "uri": pub_root }, "parent": { "uri": pub_root } }
                })),
            )
            .unwrap();
    }

    // 4) Target defensive distress signal burst (lowered author threshold)
    let distress_root = "at://did:plc:distressed/app.bsky.feed.post/3kdis";
    for idx in 0..2 {
        now += 1_000;
        mock_ws
            .emit_commit_payload(
                "did:plc:distressed",
                now,
                "app.bsky.graph.block",
                &format!("3kblk{idx}"),
                "create",
                None,
                Some(json!({ "subject": format!("did:plc:pest_{idx}") })),
            )
            .unwrap();
    }
    now += 1_000;
    mock_ws
        .emit_commit_payload(
            "did:plc:distressed",
            now,
            "app.bsky.feed.postgate",
            "3kdis",
            "create",
            None,
            Some(json!({
                "post": distress_root,
                "detachedEmbeddingUris": ["at://did:plc:qt1/app.bsky.feed.post/1"]
            })),
        )
        .unwrap();
    for idx in 0..4 {
        now += 1_000;
        mock_ws
            .emit_commit_payload(
                &format!("did:plc:oon_dis_{idx}"),
                now,
                "app.bsky.feed.post",
                &format!("3kdr{idx}"),
                "create",
                Some("cid"),
                Some(json!({
                    "text": "Stop deleting replies",
                    "reply": { "root": { "uri": distress_root }, "parent": { "uri": distress_root } }
                })),
            )
            .unwrap();
    }

    // 5) 5,000-commit burst of cold untracked likes proving zero-allocation cold filter
    for idx in 0..5_000 {
        now += 10;
        mock_ws
            .emit_commit_payload(
                &format!("did:plc:cold_{}", idx % 500),
                now,
                "app.bsky.feed.like",
                &format!("3kcold{idx}"),
                "create",
                None,
                Some(json!({
                    "subject": {
                        "uri": format!("at://did:plc:random{idx}/app.bsky.feed.post/3k{idx}"),
                        "cid": "cid"
                    }
                })),
            )
            .unwrap();
        if idx % 200 == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    // Wait up to 8s for all 5,000+ commits and Stage 2/3 evaluations to land
    let mut storms = Vec::new();
    for _ in 0..80 {
        storms = engine.list_storms(true, 100).unwrap();
        let snap = engine.stats().snapshot();
        if snap.cold_likes_ignored >= 4_500 && storms.len() >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    cancel.cancel();
    let final_stats = eng_handle.await.unwrap().unwrap();
    let _ = stream_handle.await;

    assert!(
        final_stats.cold_likes_ignored >= 4_500,
        "expected >=4500 cold likes ignored, got {}",
        final_stats.cold_likes_ignored
    );
    assert!(
        engine.tracker().snapshot().tracked_posts < 50,
        "cold likes must not allocate tracked posts; got {}",
        engine.tracker().snapshot().tracked_posts
    );

    // Benign viral was NOT detected as a storm
    assert!(
        !storms.iter().any(|s| s.focal_post_uri == benign_root),
        "benign viral post must be suppressed"
    );

    // Small victim redacted publicly
    let small_storm = storms
        .iter()
        .find(|s| s.focal_post_uri == small_root)
        .expect("small account dogpile missing");
    assert_eq!(small_storm.target_tier, TargetTier::Small);
    assert!(small_storm.top_catalyst.is_some());
    let small_pub = PublicStormView::from_storm(small_storm, cfg.public_follower_threshold);
    assert!(small_pub.redacted);
    assert!(small_pub.target_did.is_none());
    assert!(small_pub.top_catalyst.is_none());

    // Public figure backlash unredacted publicly
    let pub_storm = storms
        .iter()
        .find(|s| s.focal_post_uri == pub_root)
        .expect("public figure backlash missing");
    assert_eq!(pub_storm.target_tier, TargetTier::PublicFigure);
    assert_eq!(pub_storm.category, StormCategory::PublicFigureBacklash);
    assert!(
        pub_storm
            .stance
            .as_ref()
            .is_some_and(|s| s.evaluator != "structural_fallback"),
        "Stage 3 LLM stance verdict should not have fallen back"
    );
    assert!(final_stats.stage3_stance_calls >= 3);
    let pub_view = PublicStormView::from_storm(pub_storm, cfg.public_follower_threshold);
    assert!(!pub_view.redacted);
    assert_eq!(pub_view.target_handle.as_deref(), Some("celeb.bsky.social"));

    // Defensive distress storm detected cleanly below normal author threshold
    assert!(
        storms.iter().any(|s| s.focal_post_uri == distress_root),
        "defensive distress burst storm should be detected"
    );

    // SSE events broadcast for upserted storms
    let mut saw_redacted_sse = false;
    while let Ok(ev) = events_rx.try_recv() {
        if let RadarEvent::StormUpsert { ref storm, .. } = ev {
            if storm.focal_post_uri == small_root {
                let wire = WireRadarEvent::project(&ev, false, cfg.public_follower_threshold);
                let json_str = serde_json::to_string(&wire).unwrap();
                assert!(!json_str.contains("smallvictim"));
                saw_redacted_sse = true;
            }
        }
    }
    assert!(saw_redacted_sse);

    // Restart cursor + stats persisted cleanly in SQLite WAL
    drop(engine);
    drop(store);
    let reopened = Arc::new(StormStore::open(&db_path).unwrap());
    let restored_engine = SkydopplerEngineBuilder::new(cfg)
        .with_store(reopened)
        .build()
        .unwrap();
    assert!(restored_engine.latest_cursor_us() > 0);
    assert!(restored_engine.stats().snapshot().cold_likes_ignored >= 4_500);
    restored_engine.run_maintenance_tick().unwrap();
}
