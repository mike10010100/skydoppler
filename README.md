# SkyDoppler (`skydoppler`)

**Network-Wide Bluesky Atmospheric Dogpile Radar & Crowd-Surge Observatory**

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

`skydoppler` monitors the global Bluesky Jetstream firehose in real time to measure network-wide crowd-surge weather—detecting flash quote-dunks, high reply-to-like ratio storms, public-figure backlashes, and defensive `block`/`postgate`/`threadgate` distress clusters while protecting small accounts via fail-closed public redaction.

---

## Architecture Highlights

- **4-Stage Streaming Funnel**:
  1. Multi-collection Jetstream ingestion (`post`, `like`, `repost`, `block`, `postgate`, `threadgate`) via [`skybase`](https://crates.io/crates/skybase) with zero-allocation cold-post filtering.
  2. 16-shard bounded heavy-hitter tracker ([`src/tracker.rs`](src/tracker.rs)) with rolling `5m`/`30m` windows and defensive distress deltas.
  3. Token-bucketed batched AppView enrichment ([`src/enricher.rs`](src/enricher.rs)): `getPosts`, `getProfiles`, and `getRelationships` (on-demand out-of-network ratio without storing a global social graph).
  4. Optional bundled LLM stance classification ([`src/stance.rs`](src/stance.rs)) with deterministic structural-score fallback.
- **Fail-Closed Small-Account Redaction** ([`src/redact.rs`](src/redact.rs)): Targets under `20` followers (or with unknown follower counts) have identifying handles, DIDs, post URIs, snippets, and catalyst links stripped on all public REST and SSE endpoints.
- **Live Animated Doppler Radar UI** ([`assets/dashboard.html`](assets/dashboard.html)): HTML5 Canvas polar Doppler sweep, Atmospheric Weather Index (`Clear` through `Supercell`), live SSE feed, Prometheus `/metrics`, and constant-time operator authentication.
- **Pure Safe Rust**: `#![forbid(unsafe_code)]`, zero `.unwrap()`/`.expect()`/`panic!()` in production code, zero C/OpenSSL dependencies (Rustls only).

---

## Quick Start

```bash
# Run locally with live firehose + Doppler Radar UI at http://127.0.0.1:3000
cargo run -- serve

# Inspect environment configuration without opening network listeners
cargo run -- status

# Dry-run firehose calibration tool (examples/live_radar_soak.rs)
cargo run --example live_radar_soak -- --duration-secs 60
```

Or deploy via Docker Compose on port `3032`:

```bash
cp .env.example .env
./scripts/deploy.sh
```

---

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
