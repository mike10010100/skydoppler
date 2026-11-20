//! Live 24/7 SkyDoppler firehose daemon orchestration tying Jetstream ingest,
//! [`SkydopplerEngine`], maintenance ticks, SQLite state persistence, and the Axum radar web server.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::engine::{EngineStatsSnapshot, SkydopplerEngine, SkydopplerEngineBuilder};
use crate::error::{Result, SkydopplerError};
use crate::stance::{LlmStormStanceClassifier, StanceConfig, StormStanceClassifier};
use crate::storage::StormStore;
use crate::stream::{
    resolve_resume_cursor, run_jetstream_streamer, StreamConfig, DEFAULT_JETSTREAM_ENDPOINT,
};
use crate::time::current_time_us;
use crate::types::RadarConfig;
use crate::web::{run_web_server, WebServerConfig};

/// Default SQLite database file path.
pub const DEFAULT_DATABASE_PATH: &str = "skydoppler.db";
/// Default background maintenance interval in seconds.
pub const DEFAULT_MAINTENANCE_INTERVAL_SECS: u64 = 60;
const COMMIT_CHANNEL_CAPACITY: usize = 4_096;

/// Complete daemon runtime configuration assembled from environment variables and CLI flags.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// Rolling shard, threshold, redaction, and AppView tuning.
    pub radar: RadarConfig,
    /// HTTP/SSE web server bind settings and operator authentication.
    pub web: WebServerConfig,
    /// Jetstream WebSocket firehose subscription settings.
    pub stream: StreamConfig,
    /// Path to SQLite WAL database (or `":memory:"` for ephemeral runs).
    pub database_path: String,
    /// Maintenance sweep cadence (stale TTL resolution, weather snapshot, retention prune).
    pub maintenance_interval: Duration,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            radar: RadarConfig::default(),
            web: WebServerConfig::default(),
            stream: StreamConfig::default(),
            database_path: DEFAULT_DATABASE_PATH.to_string(),
            maintenance_interval: Duration::from_secs(DEFAULT_MAINTENANCE_INTERVAL_SECS),
        }
    }
}

impl DaemonConfig {
    /// Loads full [`DaemonConfig`] from environment variables with safe fallbacks.
    #[must_use]
    pub fn from_env() -> Self {
        let radar = RadarConfig::from_env();
        let web = WebServerConfig::from_env();
        let endpoint = crate::env::var_or(
            &["SKYDOPPLER_JETSTREAM_ENDPOINT"],
            DEFAULT_JETSTREAM_ENDPOINT,
        );
        let stream = StreamConfig::new(endpoint);
        let database_path =
            crate::env::var_or(&["SKYDOPPLER_DATABASE_PATH"], DEFAULT_DATABASE_PATH);
        let maint_secs = crate::env::parsed_or(
            &["SKYDOPPLER_MAINTENANCE_INTERVAL_SECS"],
            DEFAULT_MAINTENANCE_INTERVAL_SECS,
        )
        .max(1);

        Self {
            radar,
            web,
            stream,
            database_path,
            maintenance_interval: Duration::from_secs(maint_secs),
        }
    }
}

/// Constructs an initialized [`SkydopplerEngine`] from `cfg`, opening SQLite and wiring
/// Stage 3 LLM stance verification if enabled and configured.
///
/// # Errors
/// Returns [`SkydopplerError::Storage`] if the SQLite store cannot be opened or initialized.
pub fn build_engine_from_config(cfg: &DaemonConfig) -> Result<Arc<SkydopplerEngine>> {
    let store = if cfg.database_path.trim() == ":memory:" {
        Arc::new(StormStore::open_in_memory()?)
    } else {
        if let Some(parent) = Path::new(&cfg.database_path).parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        Arc::new(StormStore::open(&cfg.database_path)?)
    };

    let mut builder = SkydopplerEngineBuilder::new(cfg.radar.clone()).with_store(store);

    if cfg.radar.enable_llm_stance {
        match StanceConfig::from_env().and_then(LlmStormStanceClassifier::new) {
            Ok(classifier) => {
                info!(
                    endpoint = %classifier.endpoint_url(),
                    model = %classifier.model_name(),
                    "Stage 3 bundled LLM stance verifier enabled"
                );
                builder = builder.with_stance_classifier(Arc::new(classifier));
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "SKYDOPPLER_ENABLE_LLM_STANCE=true but stance backend failed to configure; using deterministic structural fallback"
                );
            }
        }
    }

    Ok(Arc::new(builder.build()?))
}

/// Runs the coordinated Jetstream streamer, radar engine, maintenance worker, and web dashboard
/// until `cancel` is triggered or a subsystem fails fatally.
///
/// # Errors
/// Returns [`SkydopplerError`] if initialization fails or a fatal runtime error occurs before
/// shutdown is requested.
pub async fn run(cfg: DaemonConfig, cancel: CancellationToken) -> Result<EngineStatsSnapshot> {
    let engine = build_engine_from_config(&cfg)?;

    let mut stream_cfg = cfg.stream.clone();
    if stream_cfg.cursor.is_none() {
        let resume_cursor = resolve_resume_cursor(engine.latest_cursor_us(), current_time_us());
        if let Some(c) = resume_cursor {
            stream_cfg = stream_cfg.with_cursor(c);
        }
    }

    let (commit_tx, commit_rx) = mpsc::channel(COMMIT_CHANNEL_CAPACITY);
    let mut join_set: JoinSet<Result<()>> = JoinSet::new();

    {
        let child_cancel = cancel.child_token();
        join_set.spawn(
            async move { run_jetstream_streamer(stream_cfg, commit_tx, child_cancel).await },
        );
    }
    {
        let eng = Arc::clone(&engine);
        let child_cancel = cancel.child_token();
        join_set.spawn(async move { eng.run(commit_rx, child_cancel).await.map(|_| ()) });
    }
    {
        let eng = Arc::clone(&engine);
        let child_cancel = cancel.child_token();
        let interval_dur = cfg.maintenance_interval;
        join_set.spawn(async move {
            eng.run_maintenance(interval_dur, child_cancel).await;
            Ok(())
        });
    }
    {
        let eng = Arc::clone(&engine);
        let web_cfg = cfg.web.clone();
        let child_cancel = cancel.child_token();
        join_set.spawn(async move { run_web_server(web_cfg, eng, child_cancel).await });
    }

    info!(
        db = %cfg.database_path,
        bind = %format!("{}:{}", cfg.web.host, cfg.web.port),
        endpoint = %cfg.stream.endpoint,
        "🌪️ SkyDoppler daemon started"
    );

    let mut fatal_err: Option<SkydopplerError> = None;

    tokio::select! {
        _ = cancel.cancelled() => {
            info!("Shutdown signal received; draining SkyDoppler tasks...");
        }
        Some(res) = join_set.join_next() => {
            match res {
                Ok(Ok(())) => {
                    warn!("SkyDoppler subsystem exited unexpectedly; initiating shutdown");
                }
                Ok(Err(e)) => {
                    error!(error = %e, "SkyDoppler subsystem failed; initiating shutdown");
                    fatal_err = Some(e);
                }
                Err(join_err) => {
                    error!(error = %join_err, "SkyDoppler worker task failed");
                    fatal_err = Some(SkydopplerError::Internal(join_err.to_string()));
                }
            }
        }
    }

    cancel.cancel();

    let drain_deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(drain_deadline);
    loop {
        tokio::select! {
            _ = &mut drain_deadline => {
                join_set.abort_all();
                break;
            }
            next = join_set.join_next() => {
                if next.is_none() {
                    break;
                }
            }
        }
    }

    if let Err(e) = engine.persist_state() {
        warn!(error = %e, "Failed persisting engine state during shutdown");
    }

    if let Some(err) = fatal_err {
        return Err(err);
    }

    Ok(engine.stats().snapshot())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use serde_json::json;
    use skybase::ingest::MockJetstreamServer;

    #[test]
    fn daemon_config_and_engine_builder_fallback_gracefully() {
        let mut cfg = DaemonConfig::from_env();
        cfg.database_path = ":memory:".to_string();
        cfg.radar.enable_llm_stance = true;
        // Ensure LLM env is absent to exercise graceful fallback warning branch.
        std::env::remove_var("SKYDOPPLER_STANCE_BASE_URL");

        let eng = build_engine_from_config(&cfg).unwrap();
        assert_eq!(eng.latest_cursor_us(), 0);

        let dir = tempfile::tempdir().unwrap();
        let file_cfg = DaemonConfig {
            database_path: dir
                .path()
                .join("nested")
                .join("radar.db")
                .display()
                .to_string(),
            ..DaemonConfig::default()
        };
        let eng_file = build_engine_from_config(&file_cfg).unwrap();
        assert!(dir.path().join("nested").join("radar.db").exists());
        eng_file.persist_state().unwrap();
    }

    #[tokio::test]
    async fn daemon_run_consumes_mock_firehose_and_shuts_down_cleanly() {
        let mock_ws = MockJetstreamServer::start().await.unwrap();

        let cfg = DaemonConfig {
            database_path: ":memory:".to_string(),
            web: WebServerConfig::new("127.0.0.1", 0),
            stream: StreamConfig::new(mock_ws.ws_url()),
            maintenance_interval: Duration::from_secs(1),
            ..Default::default()
        };

        let cancel = CancellationToken::new();
        let run_cancel = cancel.clone();
        let handle = tokio::spawn(async move { run(cfg, run_cancel).await });

        for _ in 0..40 {
            if mock_ws.active_connections() >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(mock_ws.active_connections() >= 1);

        let _ = mock_ws.emit_commit_payload(
            "did:plc:tester1",
            current_time_us(),
            "app.bsky.feed.post",
            "3kpost1",
            "create",
            Some("bafy1"),
            Some(json!({ "text": "hello global network" })),
        );

        tokio::time::sleep(Duration::from_millis(120)).await;
        cancel.cancel();

        let stats = handle.await.unwrap().unwrap();
        assert!(stats.commits_received >= 1);
    }

    #[tokio::test]
    async fn daemon_run_propagates_web_bind_failure() {
        let cfg = DaemonConfig {
            database_path: ":memory:".to_string(),
            web: WebServerConfig::new("not-an-ip", 3000),
            ..Default::default()
        };
        let cancel = CancellationToken::new();
        let res = run(cfg, cancel).await;
        assert!(res.is_err());
    }
}
