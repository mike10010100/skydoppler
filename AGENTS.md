# 🤖 Agent Coding & Engineering Handover Guide — `skydoppler`

Welcome, Agent! This repository implements **SkyDoppler**, the network-wide Bluesky atmospheric dogpile radar & crowd-surge observatory built on top of `skybase`.

---

## 🎯 Repository Standards & Reference Blueprint

This project adheres to the **Production-Grade Rust Best Practices & Architecture Standards** shared across the `sky*` ecosystem (`skyauth`, `skybase`, `skybouncer`):
- **Reference Repo**: [`rust-best-practices`](https://github.com/mike10010100/rust-best-practices)
- Treat every safety gate, compiler lint, privacy boundary, and architectural invariant below as non-negotiable.
- Never lower quality gates, weaken lint rules, or bypass defensive error handling for convenience.

---

## 🛡️ Core Non-Negotiable Invariants

### 1. Zero Unsafe Code
Both crate roots ([`src/lib.rs`](src/lib.rs) and [`src/main.rs`](src/main.rs)) enforce:
```rust
#![forbid(unsafe_code)]
```

### 2. Strict Crate-Root Safety Guard
Both crate roots enforce:
```rust
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
```

### 3. Zero Production Panics & Typed Errors
- **Banned in production**: `.unwrap()`, `.expect()`, `panic!`, `todo!`, `unimplemented!`.
- All fallible operations must return `Result<T, SkydopplerError>` using strongly typed variants in [`src/error.rs`](src/error.rs).
- Test modules (`#[cfg(test)]` / `tests/`) may permit unwrap/expect via explicit local `#[allow(...)]`.

### 4. Fail-Closed Small-Account Redaction Invariant
- Public REST and SSE views ([`src/redact.rs`](src/redact.rs)) must NEVER expose PII, handles, DIDs, post URIs, snippets, or amplifying catalyst links for targets with fewer than `public_follower_threshold` followers (default `20`) **or unknown follower count (`None`)**.
- Redaction must be enforced at the type boundary (`PublicStormView` vs `OperatorStormView`), never via ad-hoc template filtering.

### 5. Bounded Firehose Memory, Concurrency & Time Safety
- **Zero-Allocation Cold Paths**: `like` and `repost` commits whose subject is not already tracked must do a single shard lookup and immediately return without allocating or caching cold posts.
- **Hard Table Caps**: `max_tracked_posts` and `max_tracked_authors` bounds must never be exceeded under sustained firehose load.
- **Never Hold Locks Across `.await`**: Drop all `parking_lot` mutex guards before `.await`, channel sends that yield, or network I/O.
- **Clock-Warp Safety**: Use `saturating_sub` / `saturating_duration_since` and drift-free `tokio::time::interval`.
- **Task Lifecycle**: Track all background tasks in `tokio::task::JoinSet` tied to `CancellationToken`.

### 6. Constant-Time Operator Auth
- Operator authentication ([`src/web/auth.rs`](src/web/auth.rs)) must validate bearer tokens and cookies with `subtle::ConstantTimeEq`, rejecting missing/weak tokens (`< 16` chars).

### 7. 100% Documentation & Lean Core
- All public structs, fields, enums, constants, modules, and functions require rustdoc comments with bare URLs enclosed in `<...>`.
- `--no-default-features` must always compile with zero warnings under strict clippy.

---

## ⚡ Mandatory Pre-Completion Checklist

Every step below must exit `0` before opening a Pull Request or releasing:

```bash
# 1. Semantic Version & CHANGELOG verification (on PR branches)
./scripts/check_version_bump.sh

# 2. Formatting
cargo fmt --all -- --check

# 3. Strict Clippy (full workspace + lean core)
cargo clippy --all-targets -- -D warnings
cargo clippy --lib --no-default-features -- -D warnings

# 4. Run Unit, Integration, Coverage (>= 92%) & Doc tests in one pass
cargo llvm-cov clean --workspace
cargo llvm-cov nextest --all-targets --lcov --output-path lcov.info
cargo llvm-cov report --fail-under-lines 92
rm -f lcov.info
cargo test --doc

# 5. Mutation smoke test on critical lean-core kernels
cargo mutants --in-place --cargo-arg --no-default-features --cargo-arg --lib --file src/time.rs --file src/math.rs --file src/redact.rs

# 6. Dependency & security audits
cargo deny check
cargo audit
```
