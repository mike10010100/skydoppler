# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-20

### Added

- **Core Network Radar Library (`src/types.rs`, `src/math.rs`, `src/redact.rs`, `src/tracker.rs`, `src/stance.rs`)**:
  - Added 16-shard memory-bounded `NetworkTracker` monitoring global Jetstream posts, hot-post like/repost velocity proxies, quote-post catalyst cascades, and target defensive `block`/`postgate`/`threadgate` distress clusters.
  - Added fail-closed `PublicStormView` vs. `OperatorStormView` privacy boundary redacting small-account (`< 5,000` or unknown followers) identifiers from public API and SSE streams.
  - Added optional bundled Stage 3 LLM stance verification (`Ollama`, `SystemOne`, `StandardJev`) with structural fallback and radar weather coordinate/index scoring.
- **Pipeline, AppView Enrichment & SQLite Projection (`src/enricher.rs`, `src/stream.rs`, `src/storage.rs`, `src/engine.rs`, `src/daemon.rs`, `src/cli.rs`)**:
  - Added token-bucketed `PublicAppViewEnricher` querying `app.bsky.feed.getPosts`, `app.bsky.actor.getProfiles`, and `app.bsky.graph.getRelationships` for zero-graph out-of-network estimation and target tiering.
  - Added SQLite WAL `StormStore` for active/resolved/dismissed storms, weather history snapshots, and Jetstream cursor persistence.
  - Added bounded `SkydopplerEngine`, CLI entrypoint (`serve` / `status`), and read-only calibration example (`examples/live_radar_soak.rs`).
- **Live Doppler Radar Web UI, SSE & Prometheus Telemetry (`src/web/`, `assets/dashboard.html`)**:
  - Added Axum HTTP/SSE gateway with constant-time (`subtle::ConstantTimeEq`) operator token authentication, `/metrics` Prometheus exporter, and an interactive Canvas Doppler Radar dashboard.
- **Governance, CI & Container Operations (`AGENTS.md`, `PRD.md`, `.coderabbit.yaml`, `.github/workflows/ci.yml`, `deny.toml`, `Dockerfile`, `docker-compose.yml`, `scripts/`)**:
  - Enforced `#![forbid(unsafe_code)]`, strict Clippy & `--no-default-features` lean core builds, `>= 92%` line coverage via `cargo llvm-cov nextest`, mutation testing smoke gates, `cargo-deny`, `cargo-audit`, and hardened multi-stage container builds.
