# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.2] - 2026-11-21

### Fixed

- **Stage 3 Stance Observability & Fallback Telemetry (`src/engine.rs`, `src/web/api.rs`)**:
  - Promoted Stage 3 LLM stance failures from silent `debug!` logs to `warn!` and incremented both `errors_encountered` and a new backward-compatible (`#[serde(default)]`) `stage3_stance_fallbacks` counter exported as `skydoppler_stage3_stance_fallbacks_total` on `/metrics`.
  - Logged privacy-safe Stage 3 stance verdicts (`cell_id`, `tier`, `evaluator`, `category`, `hostile`, `confidence`, and aggregate ratios — never target handles/DIDs/text) at `INFO`.

## [0.2.1] - 2026-11-20

### Added

- **Stage 3 Target-Attribution Disambiguation (`src/types.rs`, `src/enricher.rs`, `src/stance.rs`, `src/engine.rs`)**:
  - Added profile `description` (bio), `display_name`, and focal `embed_kind` (`external_link`, `images`, `video`, `quote_post`, `quote_with_media`) enrichment and included them in `StormStanceInput` / `format_bundle_prompt`.
  - Upgraded the Stage 3 Ollama JSON schema and prompt with explicit `hostility_directed_at_author` and `hostility_target` (`post_author`, `third_party_subject`, `none`) fields plus `third_party_news_reaction` classification, deterministically filtering out news/aggregator/screenshot outrage directed at third-party subjects while keeping direct backlash against an outlet's own framing or conduct.

### Changed

- **Stage 2/3 Ratio, Dunk & Tier-Scaled OON Gates (`src/engine.rs`)**:
  - Raised `MIN_STANCE_DIRECT_RATIO` (`0.25` -> `0.60`), added `MIN_CATALYST_DUNK_RATIO` (`0.50`) so high-like supportive posts with minor quote-posts cannot inflate `power_asymmetry`, and enforced tier-scaled minimum OON responders (`min_unique_authors` for `Large`/`PublicFigure`, `(min_unique_authors / 2).max(2)` for `Small`/`Mid`/`Unknown`) alongside requiring `qualifies_for_stance` for confirmation.

## [0.2.0] - 2026-11-20

### Added

- **Historical Quote-Dunk Discovery (`src/enricher.rs`, `src/engine.rs`)**:
  - Added `app.bsky.feed.getQuotes` enrichment (`NetworkEnricher::fetch_post_quotes`) during Stage 2 evaluation so earlier viral quote-dunk catalysts are discovered even when Jetstream only observed recent replies.
  - Added subthread `parent_uri` preference in `NetworkTracker::observe_post_create` and `2x`-author-growth cooldown bypass in `maybe_build_candidate`.

### Changed

- **Scoring Calibration & Catalyst Gating (`src/types.rs`, `src/math.rs`, `src/engine.rs`, `src/tracker.rs`)**:
  - Calibrated `compute_structural_score` weights (`0.28`/`0.22`/`0.25`/`0.15`/`0.10`/`0.10`/`0.10`, velocity scale `18`) and `compute_vulnerability_weight` (`Large => 0.95`, `PublicFigure => 0.90`).
  - Lowered default `enrich_cooldown` (`600s` -> `120s`), `stage1_trigger_score` (`0.40` -> `0.35`), `confirmed_score_threshold` (`0.62` -> `0.56`), and `structural_fallback_threshold` (`0.68` -> `0.65`), and raised `DEFAULT_MAX_TRACKED_POSTS` to `40_000`.
  - Gated catalyst attribution (`MIN_CATALYST_ENGAGEMENT = 5`) so `0`-like quotes cannot inflate `power_asymmetry`, added a Stage 3 pre-filter for low-OON/low-ratio threads, and added a high-confidence `BenignViral` veto (`>= 0.70`).
- **Hardened Ollama Stage 3 Integration (`src/stance.rs`, `src/daemon.rs`, `.env.example`)**:
  - Sent `"think": false` and explicit JSON Schema `"format"` to Ollama `/api/chat`, increased default stance timeout to `30s`, refined the classification prompt for Q&A quote-prompts vs. dogpiles, and logged the configured stance model name on startup.

## [0.1.1] - 2026-11-20

### Fixed

- **Web Dashboard `bsky.app` DID Links (`assets/dashboard.html`, `tests/web_tests.rs`)**:
  - Preserved literal colons (`did:plc:...` rather than `did%3Aplc%3A...`) via `encodeBskyActor` in `atUriToBskyWebUrl` and `safeHandleUrl` so "Open focal post", "Focal Post", and profile links resolve cleanly on `bsky.app`.

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
