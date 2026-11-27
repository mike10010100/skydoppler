//! Live dry-run Jetstream firehose soak and threshold calibration utility.
//!
//! Connects read-only to the ATProto Jetstream firehose, routes commits through the 16-shard
//! heavy-hitter tracker and rate-limited public AppView enricher, and logs throughput &
//! network weather diagnostics without persisting to disk.
//!
//! Usage:
//! `cargo run --example live_radar_soak -- --duration-secs 60`

#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    missing_docs,
    rust_2018_idioms
)]

use std::sync::Arc;
use std::time::Duration;

use skydoppler::redact::PublicStormView;
use skydoppler::{
    run_jetstream_streamer, RadarConfig, SkydopplerEngineBuilder, StormStore, StreamConfig,
    DEFAULT_JETSTREAM_ENDPOINT,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> skydoppler::Result<()> {
    skydoppler::env::init_tracing();

    let args: Vec<String> = std::env::args().collect();
    let mut duration_secs: u64 = 60;
    let mut endpoint = DEFAULT_JETSTREAM_ENDPOINT.to_string();

    let mut i = 1;
    while i < args.len() {
        if args[i] == "--duration-secs" && i + 1 < args.len() {
            duration_secs = args[i + 1].parse::<u64>().unwrap_or(60).max(1);
            i += 2;
        } else if args[i] == "--endpoint" && i + 1 < args.len() {
            endpoint.clone_from(&args[i + 1]);
            i += 2;
        } else {
            i += 1;
        }
    }

    let cfg = RadarConfig::from_env();
    let public_threshold = cfg.public_follower_threshold;
    let store = Arc::new(StormStore::open_in_memory()?);
    let engine = Arc::new(
        SkydopplerEngineBuilder::new(cfg)
            .with_store(store)
            .build()?,
    );

    let (tx, rx) = mpsc::channel(4_096);
    let cancel = CancellationToken::new();

    let stream_cfg = StreamConfig::new(endpoint);
    let stream_cancel = cancel.clone();
    let stream_handle =
        tokio::spawn(async move { run_jetstream_streamer(stream_cfg, tx, stream_cancel).await });

    let eng_handle_ref = Arc::clone(&engine);
    let eng_cancel = cancel.clone();
    let engine_handle = tokio::spawn(async move { eng_handle_ref.run(rx, eng_cancel).await });

    let maint_eng = Arc::clone(&engine);
    let maint_cancel = cancel.clone();
    let maint_handle = tokio::spawn(async move {
        maint_eng
            .run_maintenance(Duration::from_secs(10), maint_cancel)
            .await;
    });

    info!(
        duration_secs,
        "🌩️ Starting SkyDoppler dry-run Jetstream soak calibration"
    );

    let tick_dur = Duration::from_secs(5);
    let mut elapsed = 0_u64;
    while elapsed < duration_secs {
        let step = tick_dur.min(Duration::from_secs(duration_secs - elapsed));
        tokio::time::sleep(step).await;
        elapsed += step.as_secs();

        let stats = engine.stats().snapshot();
        let snap = engine.tracker().snapshot();
        let weather = engine.current_weather()?;
        info!(
            elapsed_s = elapsed,
            commits = stats.commits_received,
            posts = stats.posts_observed,
            defensive = stats.defensive_signals,
            cold_ignored = stats.cold_likes_ignored,
            hot_likes = stats.hot_likes_observed,
            candidates = stats.stage1_candidates,
            enrichments = stats.stage2_enrichments,
            suppressed = stats.storms_suppressed_benign,
            storms = stats.storms_detected,
            errors = stats.errors_encountered,
            tracked_posts = snap.tracked_posts,
            tracked_authors = snap.tracked_authors,
            tracked_quotes = snap.tracked_quotes,
            weather_index = %format!("{:.1}", weather.weather_index),
            level = %weather.level,
            "Soak telemetry"
        );
    }

    cancel.cancel();
    let _ = stream_handle.await;
    let _ = engine_handle.await;
    let _ = maint_handle.await;

    let storms = engine.list_storms(false, 50)?;
    if storms.is_empty() {
        info!("Soak summary: 0 confirmed storms during dry-run window");
    } else {
        info!(
            count = storms.len(),
            "Soak summary: confirmed storms (publicly redacted views)"
        );
        for storm in &storms {
            let pub_view = PublicStormView::from_storm(storm, public_threshold);
            info!(
                cell = %pub_view.cell_id,
                redacted = pub_view.redacted,
                tier = %pub_view.target_tier,
                severity = %pub_view.severity,
                category = %pub_view.category,
                score = %format!("{:.3}", pub_view.metrics.composite_score),
                direct_ratio = %format!("{:.2}", pub_view.metrics.direct_ratio),
                dunk_ratio = %format!("{:.2}", pub_view.metrics.dunk_ratio),
                oon_ratio = %format!("{:.2}", pub_view.metrics.out_of_network_ratio),
                handle = ?pub_view.target_handle,
                "Confirmed storm cell"
            );
        }
    }

    Ok(())
}
