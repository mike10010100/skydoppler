//! # `skydoppler`
//!
//! Network-Wide Bluesky Atmospheric Dogpile Radar & Crowd-Surge Observatory.

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

#[cfg(all(feature = "web", feature = "stream", feature = "storage"))]
pub mod cli;
#[cfg(all(feature = "web", feature = "stream", feature = "storage"))]
pub mod daemon;
#[cfg(feature = "storage")]
pub mod engine;
pub mod enricher;
pub mod env;
pub mod error;
pub mod math;
pub mod redact;
pub mod stance;
#[cfg(feature = "storage")]
pub mod storage;
#[cfg(feature = "stream")]
pub mod stream;
pub mod time;
pub mod tracker;
pub mod types;
#[cfg(feature = "web")]
pub mod web;

#[cfg(feature = "storage")]
pub use engine::{EngineStats, EngineStatsSnapshot, SkydopplerEngine, SkydopplerEngineBuilder};
pub use enricher::{
    MockNetworkEnricher, NetworkEnricher, NoopNetworkEnricher, PublicAppViewEnricher,
};
pub use error::{Result, SkydopplerError};
pub use redact::{
    opaque_cell_id, should_redact_followers, should_redact_publicly, OperatorStormView,
    PublicStormView, WireRadarEvent, REDACTION_REASON_SMALL_ACCOUNT,
};
pub use stance::{
    LlmStormStanceClassifier, MockStormStanceClassifier, NoopStormStanceClassifier, StanceConfig,
    StanceDialect, StormStanceClassifier, StormStanceInput,
};
#[cfg(feature = "storage")]
pub use storage::StormStore;
#[cfg(feature = "stream")]
pub use stream::{
    resolve_resume_cursor, run_jetstream_streamer, StreamConfig, DEFAULT_JETSTREAM_ENDPOINT,
};
pub use tracker::{NetworkTracker, ObserveOutcome, TrackerSnapshot};
pub use types::{
    ActorSummary, CatalystInfo, InteractionKind, NetworkStorm, NetworkSurgeCandidate,
    PostEngagementSnapshot, RadarConfig, RadarEvent, RelationshipStatus, SampleReply,
    StormCategory, StormMetrics, StormSeverity, StormStanceVerdict, StormStatus, TargetTier,
    WeatherLevel, WeatherPoint, WeatherSummary, DEFAULT_MAX_TRACKED_AUTHORS,
    DEFAULT_MAX_TRACKED_POSTS, DEFAULT_MEDIUM_WINDOW_SECS, DEFAULT_MIN_STREAMING_RATIO,
    DEFAULT_MIN_UNIQUE_AUTHORS, DEFAULT_PUBLIC_FOLLOWER_THRESHOLD, DEFAULT_SHORT_WINDOW_SECS,
};
#[cfg(feature = "web")]
pub use web::{create_web_router, run_web_server, WebServerConfig};
