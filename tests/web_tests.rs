#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use futures_util::StreamExt;
use serde_json::Value;
use skydoppler::enricher::MockNetworkEnricher;
use skydoppler::stance::MockStormStanceClassifier;
use skydoppler::time::current_time_us;
use skydoppler::web::{create_web_router, run_web_server, WebServerConfig, OPERATOR_COOKIE_NAME};
use skydoppler::{
    ActorSummary, InteractionKind, NetworkSurgeCandidate, PostEngagementSnapshot, RadarConfig,
    SampleReply, SkydopplerEngine, SkydopplerEngineBuilder, StormCategory, StormMetrics,
    StormStanceVerdict, WeatherPoint,
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const OPERATOR_SECRET: &str = "super_secret_doppler_operator_2026";

fn setup_test_engine() -> Arc<SkydopplerEngine> {
    let enricher = Arc::new(MockNetworkEnricher::new());

    // 1. Small account (< 20 followers)
    let small_uri = "at://did:plc:small123/app.bsky.feed.post/3ksmall";
    enricher.set_post(PostEngagementSnapshot {
        uri: small_uri.to_string(),
        author_did: "did:plc:small123".to_string(),
        author_handle: Some("small123.bsky.social".to_string()),
        author_followers: Some(12),
        text: "Private rant snippet that must never leak publicly".to_string(),
        reply_count: 50,
        repost_count: 0,
        like_count: 2,
        quote_count: 8,
        ..Default::default()
    });
    enricher.set_profile(ActorSummary {
        did: "did:plc:small123".to_string(),
        handle: Some("small123.bsky.social".to_string()),
        followers_count: Some(12),
        ..Default::default()
    });
    enricher.set_post(PostEngagementSnapshot {
        uri: "at://did:plc:cat99/app.bsky.feed.post/3kcat99".to_string(),
        author_did: "did:plc:cat99".to_string(),
        author_handle: Some("megacatalyst.bsky.social".to_string()),
        author_followers: Some(90_000),
        text: "Dunking!".to_string(),
        reply_count: 5,
        repost_count: 50,
        like_count: 1_100,
        quote_count: 2,
        ..Default::default()
    });

    // 2. Public figure (>= 20 followers)
    let big_uri = "at://did:plc:public777/app.bsky.feed.post/3kpub777";
    enricher.set_post(PostEngagementSnapshot {
        uri: big_uri.to_string(),
        author_did: "did:plc:public777".to_string(),
        author_handle: Some("publicfigure.bsky.social".to_string()),
        author_followers: Some(150_000),
        text: "Public announcement post text".to_string(),
        reply_count: 140,
        repost_count: 5,
        like_count: 12,
        quote_count: 25,
        ..Default::default()
    });
    enricher.set_profile(ActorSummary {
        did: "did:plc:public777".to_string(),
        handle: Some("publicfigure.bsky.social".to_string()),
        followers_count: Some(150_000),
        ..Default::default()
    });

    let stance = Arc::new(MockStormStanceClassifier::new(StormStanceVerdict::new(
        true,
        0.92,
        StormCategory::Dogpile,
        "Coordinated dogpile wave",
        "mock-stance",
    )));

    let cfg = RadarConfig::default()
        .with_min_unique_authors(4)
        .with_enable_llm_stance(true)
        .with_thresholds(0.30, 0.55, 0.60);

    Arc::new(
        SkydopplerEngineBuilder::new(cfg)
            .with_enricher(enricher)
            .with_stance_classifier(stance)
            .with_appview_endpoint("http://127.0.0.1:9999")
            .build()
            .unwrap(),
    )
}

fn mk_candidate(did: &str, uri: &str) -> NetworkSurgeCandidate {
    NetworkSurgeCandidate {
        target_did: did.to_string(),
        focal_post_uri: uri.to_string(),
        observed_at_us: current_time_us(),
        initial_metrics: StormMetrics {
            unique_authors_short: 24,
            unique_oon_authors_short: 24,
            reply_count_short: 24,
            burst_z_score: 8.5,
            ..Default::default()
        },
        sample_replies: vec![SampleReply {
            author_did: "did:plc:stranger_a".to_string(),
            kind: InteractionKind::Reply,
            post_uri: "at://did:plc:stranger_a/app.bsky.feed.post/1".to_string(),
            text_snippet: "Pile on".to_string(),
        }],
        recent_quote_uris: if did == "did:plc:small123" {
            vec!["at://did:plc:cat99/app.bsky.feed.post/3kcat99".to_string()]
        } else {
            vec![]
        },
    }
}

#[tokio::test]
async fn dashboard_health_metrics_weather_redaction_operator_auth_and_dismiss() {
    let engine = setup_test_engine();
    let small_storm = engine
        .evaluate_candidate(mk_candidate(
            "did:plc:small123",
            "at://did:plc:small123/app.bsky.feed.post/3ksmall",
        ))
        .await
        .unwrap()
        .unwrap();

    let big_storm = engine
        .evaluate_candidate(mk_candidate(
            "did:plc:public777",
            "at://did:plc:public777/app.bsky.feed.post/3kpub777",
        ))
        .await
        .unwrap()
        .unwrap();

    engine
        .store()
        .record_weather_snapshot(&WeatherPoint {
            timestamp_us: current_time_us(),
            weather_index: 38.4,
            active_storms: 2,
            commits_per_sec: 1120.0,
        })
        .unwrap();

    let web_cfg = WebServerConfig::default()
        .with_operator_token(Some(OPERATOR_SECRET.to_string()))
        .with_cors_origins(vec!["https://doppler.example.com".to_string()]);
    let app = create_web_router(Arc::clone(&engine), &web_cfg);

    // 1. Dashboard UI & security headers
    let root_resp = app
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(root_resp.status(), StatusCode::OK);
    assert_eq!(
        root_resp.headers().get(header::X_FRAME_OPTIONS).unwrap(),
        "DENY"
    );
    assert_eq!(
        root_resp
            .headers()
            .get(header::X_CONTENT_TYPE_OPTIONS)
            .unwrap(),
        "nosniff"
    );
    assert!(root_resp.headers().contains_key("content-security-policy"));
    let html_bytes = axum::body::to_bytes(root_resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let html = String::from_utf8(html_bytes.to_vec()).unwrap();
    assert!(html.contains("doppler-canvas"));
    assert!(html.contains("escapeHtml"));
    assert!(html.contains("Protected Small Account"));
    assert!(html.contains("&lt; 20 followers"));
    assert!(html.contains("encodeBskyActor"));
    assert!(html.contains("replace(/%3A/gi, ':')"));

    // 2. Health & Metrics
    for path in ["/healthz", "/api/v1/health"] {
        let r = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(r.into_body(), 64 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body["status"], "healthy");
        assert_eq!(body["active_storms"], 2);
    }

    for path in ["/metrics", "/api/v1/metrics"] {
        let r = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let txt = String::from_utf8(
            axum::body::to_bytes(r.into_body(), 128 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(txt.contains("skydoppler_commits_received_total"));
        assert!(txt.contains("skydoppler_stage3_stance_fallbacks_total"));
        assert!(txt.contains("skydoppler_stage3_benign_cache_hits_total"));
        assert!(txt.contains("skydoppler_storms_detected_total 2"));
        assert!(txt.contains("skydoppler_active_storms 2"));
        assert!(txt.contains("skydoppler_weather_index"));
        assert!(txt.contains("skydoppler_build_info"));
    }

    // 3. Weather endpoint
    let w_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/weather?history_limit=50")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(w_resp.status(), StatusCode::OK);
    let w_json: Value = serde_json::from_slice(
        &axum::body::to_bytes(w_resp.into_body(), 64 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(w_json["summary"]["active_storms"], 2);
    assert!(!w_json["history"].as_array().unwrap().is_empty());

    // 4. Public /api/v1/storms MUST redact small123 while exposing public777
    let pub_list = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/storms?active_only=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pub_list.status(), StatusCode::OK);
    let pub_list_str = String::from_utf8(
        axum::body::to_bytes(pub_list.into_body(), 128 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(!pub_list_str.contains("small123"));
    assert!(!pub_list_str.contains("Private rant snippet"));
    assert!(!pub_list_str.contains("megacatalyst"));
    assert!(pub_list_str.contains(&small_storm.cell_id));
    assert!(pub_list_str.contains("publicfigure.bsky.social"));

    // Query filters & 400 on invalid severity
    let bad_sev = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/storms?severity=bogus")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad_sev.status(), StatusCode::BAD_REQUEST);

    // Detail endpoint by cell_id & numeric id + 404
    let small_detail = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/storms/{}", small_storm.cell_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(small_detail.status(), StatusCode::OK);
    let small_detail_str = String::from_utf8(
        axum::body::to_bytes(small_detail.into_body(), 64 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(!small_detail_str.contains("small123"));

    let big_detail = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/storms/{}", big_storm.id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(big_detail.status(), StatusCode::OK);

    let missing_detail = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/storms/cell-doesnotexist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing_detail.status(), StatusCode::NOT_FOUND);

    // 5. Operator Auth failure & Cookie unlock -> unredacted view -> logout
    let bad_auth = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/operator/auth")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"token":"wrong_token_0000000"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad_auth.status(), StatusCode::UNAUTHORIZED);

    let ok_auth = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/operator/auth")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-forwarded-proto", "https")
                .body(Body::from(format!(r#"{{"token":"{OPERATOR_SECRET}"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok_auth.status(), StatusCode::OK);
    let set_cookie = ok_auth
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(set_cookie.contains("HttpOnly"));
    assert!(set_cookie.contains("SameSite=Strict"));
    assert!(set_cookie.contains("Secure"));
    let cookie_pair = set_cookie.split(';').next().unwrap().to_string();

    // Operator cookie sees full unredacted details for small123
    let op_list = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/storms")
                .header(header::COOKIE, &cookie_pair)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(op_list.status(), StatusCode::OK);
    let op_list_str = String::from_utf8(
        axum::body::to_bytes(op_list.into_body(), 128 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(op_list_str.contains("\"operator_mode\":true"));
    assert!(op_list_str.contains("small123.bsky.social"));
    assert!(op_list_str.contains("Private rant snippet"));

    let op_detail = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/storms/{}", small_storm.cell_id))
                .header(header::AUTHORIZATION, format!("Bearer {OPERATOR_SECRET}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(op_detail.status(), StatusCode::OK);
    let op_detail_json: Value = serde_json::from_slice(
        &axum::body::to_bytes(op_detail.into_body(), 64 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(op_detail_json["redacted_for_public"], true);
    assert_eq!(op_detail_json["target_did"], "did:plc:small123");

    // Dismiss requires auth, returns 404 on missing storm, succeeds with Bearer
    let unauth_dismiss = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/v1/operator/storms/{}/dismiss",
                    small_storm.id
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauth_dismiss.status(), StatusCode::UNAUTHORIZED);

    let missing_dismiss = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/operator/storms/999999/dismiss")
                .header(header::AUTHORIZATION, format!("Bearer {OPERATOR_SECRET}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing_dismiss.status(), StatusCode::NOT_FOUND);

    let ok_dismiss = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/v1/operator/storms/{}/dismiss",
                    small_storm.id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {OPERATOR_SECRET}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok_dismiss.status(), StatusCode::OK);
    let d_json: Value = serde_json::from_slice(
        &axum::body::to_bytes(ok_dismiss.into_body(), 64 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(d_json["dismissed"], true);

    let logout_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/operator/logout")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(logout_resp.status(), StatusCode::OK);
    let logout_cookie = logout_resp
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(logout_cookie.contains(&format!("{OPERATOR_COOKIE_NAME}=")));
    assert!(logout_cookie.contains("Max-Age=0"));
}

#[tokio::test]
async fn sse_stream_enforces_public_redaction_and_operator_unredacted_projection() {
    let engine = setup_test_engine();
    let cfg = WebServerConfig::default().with_operator_token(Some(OPERATOR_SECRET.to_string()));
    let app = create_web_router(Arc::clone(&engine), &cfg);

    let pub_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pub_resp.status(), StatusCode::OK);

    let op_resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/stream")
                .header(header::AUTHORIZATION, format!("Bearer {OPERATOR_SECRET}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(op_resp.status(), StatusCode::OK);

    // Emit small-account storm AFTER both streams subscribed
    let _ = engine
        .evaluate_candidate(mk_candidate(
            "did:plc:small123",
            "at://did:plc:small123/app.bsky.feed.post/3ksmall",
        ))
        .await
        .unwrap()
        .unwrap();

    let mut pub_stream = pub_resp.into_body().into_data_stream();
    let mut op_stream = op_resp.into_body().into_data_stream();

    let pub_chunk = tokio::time::timeout(Duration::from_secs(2), pub_stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let pub_text = String::from_utf8(pub_chunk.to_vec()).unwrap();
    assert!(pub_text.contains("public_storm_upsert"));
    assert!(!pub_text.contains("small123"));
    assert!(!pub_text.contains("Private rant snippet"));
    assert!(!pub_text.contains("megacatalyst"));

    let op_chunk = tokio::time::timeout(Duration::from_secs(2), op_stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let op_text = String::from_utf8(op_chunk.to_vec()).unwrap();
    assert!(op_text.contains("operator_storm_upsert"));
    assert!(op_text.contains("small123.bsky.social"));
    assert!(op_text.contains("Private rant snippet"));
}

#[tokio::test]
async fn run_web_server_graceful_shutdown_and_invalid_addr_error() {
    let engine = setup_test_engine();
    let bad_cfg = WebServerConfig::new("not-an-ip-address", 3000);
    assert!(
        run_web_server(bad_cfg, Arc::clone(&engine), CancellationToken::new())
            .await
            .is_err()
    );

    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    let cfg = WebServerConfig::new("127.0.0.1", 0);
    let h = tokio::spawn(async move { run_web_server(cfg, engine, c2).await });
    tokio::time::sleep(Duration::from_millis(60)).await;
    cancel.cancel();
    h.await.unwrap().unwrap();
}
