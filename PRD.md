# SkyDoppler (`skydoppler`) — Product Requirements & Architecture Specification

**Version:** 0.1.0  
**License:** MIT OR Apache-2.0  
**Ecosystem Sibling Crates:** `skybase`, `skyauth`, `skybouncer`

---

## 1. Mission & Product Vision

**SkyDoppler** is a real-time, network-wide **Bluesky Atmospheric Dogpile Radar**. While **Skybouncer** acts as an account-level personal shield for individual enrolled users, SkyDoppler watches the global AT Protocol **Jetstream** firehose to measure the macro weather of Bluesky discourse—spotting flash quote-dunk cascades, reply-ratio storms, public-figure backlashes, and defensive block/gate bursts as they form in real time.

---

## 2. Core Technical Challenges & Funnel Architecture

Tracking every post across the entire Bluesky network without a global follow graph or high AppView costs requires a bounded **4-stage streaming funnel**:

1. **Stage 0 — Multi-Collection Firehose Ingestion (`src/stream.rs`)**:
   - Subscribes to `app.bsky.feed.post`, `app.bsky.feed.like`, `app.bsky.feed.repost`, `app.bsky.graph.block`, `app.bsky.feed.postgate`, and `app.bsky.feed.threadgate`.
   - Applies a **zero-allocation hot-post check** on `like` and `repost` commits: cold posts are discarded in one hash lookup, while hot posts already seeing reply/quote surges accumulate live streaming ratio updates.
2. **Stage 1 — 16-Shard Bounded Heavy-Hitter Tracker (`src/tracker.rs`)**:
   - Maintains sharded, memory-capped sliding short (`5m`) and medium (`30m`) windows over focal posts and target author distress events (`block`, `postgate` detach deltas, `threadgate` locks).
   - Computes live structural triggers (`unique_authors`, `streaming_direct_ratio`, `burst_z_score`, `defensive_signals`) and emits a `NetworkSurgeCandidate` only on surging posts.
3. **Stage 2 — Polite Batched AppView Enrichment & OON Sampling (`src/enricher.rs`)**:
   - Uses a token-bucket rate limiter and three batched unauthenticated AppView RPCs per candidate:
     - `app.bsky.feed.getPosts` for cumulative engagement & top quote catalysts (`direct_ratio`, `dunk_ratio`).
     - `app.bsky.actor.getProfiles` for `power_asymmetry` and `TargetTier` (`Small`, `Mid`, `Large`, `PublicFigure`).
     - `app.bsky.graph.getRelationships` to compute an exact out-of-network fraction across up to `30` sampled responders without storing a global social graph in RAM.
4. **Stage 3 — Optional Bundled LLM Stance Verification (`src/stance.rs`)**:
   - Sends a single bounded prompt containing the focal post, top quote catalysts, and sampled replies to classify `Dogpile` vs. `PublicFigureBacklash` vs. `HeatedDebate` vs. `BenignViral` across `Ollama`, `SystemOne`, or `StandardJev` endpoints—falling back deterministically to `structural_fallback_threshold` when disabled or unavailable.

---

## 3. Privacy & Anti-Amplification Guardrails (`src/redact.rs`)

Surfacing an unredacted real-time list of small accounts currently being dogpiled creates secondary harassment and rubbernecking risks. **SkyDoppler enforces fail-closed privacy by construction**:

- Any target with fewer than `public_follower_threshold` followers (`5,000` by default) **or unknown follower count (`None`)** is serialized publicly as an anonymized radar cell (`cell-<hex>`), stripping `target_did`, `target_handle`, `focal_post_uri`, `focal_post_snippet`, and `top_catalyst`.
- Full attribution is visible only for `>= 5,000` follower accounts/public figures publicly, or to authenticated operators (`Authorization: Bearer` / `HttpOnly; SameSite=Strict` cookie using `subtle::ConstantTimeEq`).

---

## 4. Persistence, Telemetry & Live Radar Dashboard

- **SQLite WAL Projection (`src/storage.rs`)**: Durable Jetstream cursor, `network_storms` table, and `weather_snapshots` time-series with configurable TTL retention pruning.
- **REST + SSE + Prometheus (`src/web/`)**:
  - `GET /api/v1/weather`, `GET /api/v1/storms`, `GET /api/v1/storms/{id}`, `GET /api/v1/stream` (per-subscriber redacted vs. operator SSE stream), `POST /api/v1/operator/{auth,logout,storms/{id}/dismiss}`, `/metrics`.
- **Animated Canvas Radar UI (`assets/dashboard.html`)**:
  - Real-time polar Doppler sweep, Weather Index gauge (`Clear` -> `Supercell`), 24h weather sparkline, storm cell inspector, redaction privacy badges, and operator unlock drawer.
