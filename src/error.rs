//! Strongly typed error definitions for `skydoppler`.

use thiserror::Error;

/// Convenience result alias using [`SkydopplerError`].
pub type Result<T> = std::result::Result<T, SkydopplerError>;

/// Root error type across `skydoppler` subsystems.
#[derive(Debug, Error)]
pub enum SkydopplerError {
    /// Invalid configuration or input parameters.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Firehose or Jetstream ingestion failure.
    #[error("Ingestion error: {0}")]
    Ingestion(String),

    /// Embedded SQLite storage error.
    #[cfg(feature = "storage")]
    #[error("Database error: {0}")]
    Storage(#[from] rusqlite::Error),

    /// Storage error string fallback when compiled without `storage` feature.
    #[cfg(not(feature = "storage"))]
    #[error("Database error: {0}")]
    Storage(String),

    /// AppView enrichment or upstream query error.
    #[error("Enricher error: {0}")]
    Enricher(String),

    /// LLM stance verification error.
    #[error("Stance classifier error: {0}")]
    Stance(String),

    /// HTTP client or transport failure.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// JSON serialization or deserialization failure.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Operator authentication or authorization failure.
    #[error("Authentication error: {0}")]
    Auth(String),

    /// Internal concurrency or web server runtime error.
    #[error("Internal error: {0}")]
    Internal(String),
}
