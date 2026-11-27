# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-11-27

### Added

- **Medium-Window (`30m`) Sustained-Burn Trigger (`src/types.rs`, `src/tracker.rs`, `.env.example`)**:
  - Added `DEFAULT_MIN_UNIQUE_AUTHORS_MEDIUM = 8` (`SKYDOPPLER_MIN_UNIQUE_AUTHORS_MEDIUM`) and backward-compatible `#[serde(default)] StormMetrics::unique_authors_medium` to catch slow-burn dogpiles spanning `30m` that stay below the `10m` velocity threshold, guarded by a `0.45` sustained streaming ratio floor and `>= 2` short-window responders.
- **Bounded 16-Shard Catalyst Quote-Post Index (`src/tracker.rs`, `src/types.rs`, `src/web/api.rs`)**:
  - Indexed quote-posts (`app.bsky.embed.record` / `recordWithMedia`) in bounded `quote_shards` (`DEFAULT_MIN_CATALYST_LIKES = 15`, `SKYDOPPLER_MIN_CATALYST_LIKES`), routing live quote-post likes back to quoted focal posts as `ObserveOutcome::CatalystDunk` while preserving zero-allocation cold lookups and single-lock scoping.
  - Exported `TrackerSnapshot::tracked_quotes` and the `skydoppler_tracked_quotes` Prometheus gauge.

### Changed

- **Stage 2/3 OON Scoring, Catalyst Qualification & Prompt Context (`src/engine.rs`, `src/stance.rs`)**:
  - Scaled `unique_oon_authors_medium`, incorporated `effective_oon_authors = max(short, medium)` into structural scoring and benign-verdict cache activity tracking, enabled verified high-dunk catalyst qualification (`like_count >= 15`, `dunk_ratio >= 2.0`, `>= 2` OON authors) for `Small`/`Mid`/`Unknown` targets, and included `authors_30m=` in Stage 3 LLM stance prompts when medium-window responders exceed the short window.

## [0.3.3] - 2026-11-23

### Changed

- **Aligned Ollama Stance Context Window (`num_ctx = 16384`) (`src/stance.rs`, `.env.example`)**:
  - Raised `StanceConfig::default().num_ctx` (`4096` -> `DEFAULT_STANCE_NUM_CTX = 16_384`) to match sibling `skybouncer` and eliminate Ollama KV-cache runner tear-down/reload latency when both services share `gemma4:26b` on `nmo`.
- Updated `skybase` dependency (`0.1.11` -> `0.1.12`) to pick up the persistent Jetstream idle read deadline fix.

## [0.3.2] - 2026-11-22

### Changed

- **Faster CI Verification, Mutation Testing & Docker Builds (`.cargo/mutants.toml`, `.github/workflows/ci.yml`, `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `AGENTS.md`)**:
  - Added `.cargo/mutants.toml` (`test_tool = "nextest"`) and sharded `mutants-smoke` across 4 parallel lean-core (`--no-default-features`) CI matrix shards (`0..3/4`, round-robin) with `shared-key: "mutants-lean"` so all 187 kernel mutants continue to run without compilation bottlenecks.
  - Added Stage 0 `manifest-prep` version normalization and dummy dependency pre-building in `Dockerfile` plus `.dockerignore` and `5s` container healthcheck intervals across `Dockerfile` and `docker-compose.yml`.
  - Added `Swatinem/rust-cache@v2` (`shared-key: "publish"`) to the `publish` job in `.github/workflows/ci.yml` and unified local test + `llvm-cov nextest` execution in `AGENTS.md`.

## [0.3.1] - 2026-11-22

### Changed

- **Lowered Public Follower Redaction Threshold (`src/types.rs`, `src/engine.rs`, `assets/dashboard.html`, `.env.example`)**:
  - Lowered `DEFAULT_PUBLIC_FOLLOWER_THRESHOLD` (`5_000` -> `20`) so only accounts under `20` followers (or with unknown follower counts) are hidden on public unauthenticated REST and SSE endpoints.
  - Updated `SkydopplerEngine::current_weather` so `WeatherSummary::small_target_storms` tracks storms redacted under `config.public_follower_threshold` via `should_redact_publicly`.
  - Updated dashboard UI redaction copy (`< 20 followers`) and responder window label (`10m`).

## [0.3.0] - 2026-11-22

### Changed

- **Stage 1 Recall Calibration & Quote-Cascade Bypass (`src/types.rs`, `src/tracker.rs`, `.env.example`)**:
  - Extended default `DEFAULT_SHORT_WINDOW_SECS` (`300s` -> `600s`), lowered `DEFAULT_MIN_UNIQUE_AUTHORS` (`8` -> `6`), and lowered `DEFAULT_MIN_STREAMING_RATIO` (`0.50` -> `0.30`) to catch medium-paced dogpiles and ratio cascades on popular posts.
  - Added a `>= 2` short-window quote-post cascade bypass (`MIN_QUOTES_RATIO_BYPASS`) of `min_streaming_ratio` and `stage1_trigger_score` so liked posts suddenly drawing quote-dunks still reach Stage 2 `getQuotes` enrichment.
- **Stage 2/3 Direct-Ratio Calibration (`src/engine.rs`)**:
  - Lowered `MIN_STANCE_DIRECT_RATIO` (`0.60` -> `0.45`) so dogpiles on moderately liked posts can advance to Stage 3 LLM stance verification while keeping beloved high-like viral posts (`< 0.45`) filtered out.

### Added

- **Bounded Stage 2/3 Benign-Verdict Suppression Cache (`src/engine.rs`, `src/web/api.rs`)**:
  - Added an in-memory bounded 30-minute suppression cache (`BENIGN_CACHE_TTL_US`, `BENIGN_CACHE_MIN_CONFIDENCE = 0.90`, `BENIGN_CACHE_MAX_ENTRIES = 4096`) short-circuiting repeat AppView/LLM re-evaluations on confirmed benign viral threads, automatically bypassed on `2x` author growth or target defensive distress and pruned on maintenance ticks.
  - Exported backward-compatible `stage3_benign_cache_hits` telemetry (`#[serde(default)]`) as `skydoppler_stage3_benign_cache_hits_total` on `/metrics`.

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
