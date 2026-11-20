//! `skydoppler` CLI binary entrypoint.
//!
//! Loads optional `.env` overrides, wires OS signal graceful shutdown, and delegates subcommand
//! execution to [`skydoppler::cli::dispatch`].

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

use std::path::Path;

use skydoppler::error::Result;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    skydoppler::env::load_dotenv_file(Path::new(".env"));

    let args: Vec<String> = std::env::args().collect();
    let cancel = CancellationToken::new();
    let signal_cancel = cancel.clone();

    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal_cancel.cancel();
    });

    skydoppler::cli::dispatch(&args, cancel).await
}
