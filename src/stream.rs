//! Jetstream firehose consumer wrapper subscribing to network-wide post, engagement, and distress collections.

use std::time::Duration;

use skybase::ingest::events::JetstreamCommit;
pub use skybase::ingest::streamer::{
    resolve_resume_cursor, CURSOR_REWIND_OVERLAP_US, DEFAULT_JETSTREAM_ENDPOINT,
    DEFAULT_READ_TIMEOUT, INITIAL_BACKOFF, MAX_BACKOFF, MAX_JETSTREAM_REPLAY_WINDOW_US,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::{Result, SkydopplerError};

/// Jetstream configuration for global multi-collection ingestion.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// WebSocket endpoint URL.
    pub endpoint: String,
    /// Collection NSIDs to subscribe to at the edge.
    pub collections: Vec<String>,
    /// Optional resume cursor in microseconds.
    pub cursor: Option<u64>,
    /// Idle WebSocket read timeout forcing reconnect.
    pub read_timeout: Duration,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_JETSTREAM_ENDPOINT.to_string(),
            collections: vec![
                "app.bsky.feed.post".to_string(),
                "app.bsky.feed.like".to_string(),
                "app.bsky.feed.repost".to_string(),
                "app.bsky.graph.block".to_string(),
                "app.bsky.feed.postgate".to_string(),
                "app.bsky.feed.threadgate".to_string(),
            ],
            cursor: None,
            read_timeout: DEFAULT_READ_TIMEOUT,
        }
    }
}

impl StreamConfig {
    /// Creates a [`StreamConfig`] targeting `endpoint` with default radar collections.
    #[must_use]
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Self::default()
        }
    }

    /// Sets the resume cursor in microseconds (`0` clears the cursor).
    #[must_use]
    pub fn with_cursor(mut self, cursor_us: u64) -> Self {
        self.cursor = if cursor_us > 0 { Some(cursor_us) } else { None };
        self
    }

    /// Sets the idle read timeout.
    #[must_use]
    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = timeout.max(Duration::from_millis(50));
        self
    }

    /// Builds the full Jetstream subscription URL.
    #[must_use]
    pub fn build_url(&self) -> String {
        skybase::ingest::build_subscription_url_full(
            &self.endpoint,
            &self.collections,
            &[],
            self.cursor.filter(|c| *c > 0),
        )
    }

    /// Converts into [`skybase::ingest::streamer::StreamConfig`].
    #[must_use]
    pub fn to_skybase_config(&self) -> skybase::ingest::streamer::StreamConfig {
        let mut cfg = skybase::ingest::streamer::StreamConfig::new(&self.endpoint)
            .with_collections(self.collections.clone())
            .with_read_timeout(self.read_timeout);
        if let Some(c) = self.cursor {
            cfg = cfg.with_cursor(c);
        }
        cfg
    }
}

/// Runs the resilient Jetstream streamer forwarding parsed commits into `tx` until `cancel` fires.
///
/// # Errors
/// Returns [`SkydopplerError::Ingestion`] if stream setup fails unrecoverably.
pub async fn run_jetstream_streamer(
    config: StreamConfig,
    tx: mpsc::Sender<JetstreamCommit>,
    cancel: CancellationToken,
) -> Result<()> {
    skybase::ingest::streamer::run_jetstream_streamer(config.to_skybase_config(), tx, cancel)
        .await
        .map_err(|e| SkydopplerError::Ingestion(e.to_string()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn default_config_subscribes_all_six_radar_collections() {
        let cfg = StreamConfig::default();
        assert_eq!(cfg.endpoint, DEFAULT_JETSTREAM_ENDPOINT);
        for col in [
            "app.bsky.feed.post",
            "app.bsky.feed.like",
            "app.bsky.feed.repost",
            "app.bsky.graph.block",
            "app.bsky.feed.postgate",
            "app.bsky.feed.threadgate",
        ] {
            assert!(cfg.collections.contains(&col.to_string()), "missing {col}");
        }
        let url = cfg
            .clone()
            .with_cursor(42_000)
            .with_read_timeout(Duration::from_secs(5))
            .build_url();
        assert!(url.contains("cursor=42000"));
        assert_eq!(StreamConfig::new("wss://x/s").with_cursor(0).cursor, None);
    }
}
