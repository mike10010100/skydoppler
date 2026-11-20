# ==============================================================================
# Multi-Stage Production Dockerfile for SkyDoppler
# Network-Wide Bluesky Atmospheric Dogpile Radar
# ==============================================================================

# ------------------------------------------------------------------------------
# Stage 1: Build Release Binary
# ------------------------------------------------------------------------------
FROM rust:1-bookworm AS builder

WORKDIR /build

# Copy manifests; skybase is fetched from crates.io.
COPY Cargo.toml Cargo.lock ./

# Copy application source, embedded assets, and example manifest targets
COPY src ./src
COPY assets ./assets
COPY examples ./examples

# Build optimized production binary and strip debug symbols
RUN cargo build --release --locked --bin skydoppler && \
    strip /build/target/release/skydoppler

# ------------------------------------------------------------------------------
# Stage 2: Minimal Hardened Runtime Image
# ------------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install CA certificates for TLS/WSS connections, curl for healthchecks, and sqlite3 for online backups.
# Explicitly distrust the October 2026 compromised eMudhra / Comodo AAA roots in the OS store.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl sqlite3 && \
    sed -i \
      -e 's|^mozilla/emSign_ECC_Root_CA_-_G3.crt|!&|' \
      -e 's|^mozilla/Comodo_AAA_Services_root.crt|!&|' \
      /etc/ca-certificates.conf && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Dedicated non-root application user and group (UID/GID 10001)
RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -s /sbin/nologin -d /app appuser

# Create application directory and persistent SQLite volume directory
RUN mkdir -p /app /data && \
    chown -R appuser:appgroup /app /data

WORKDIR /app

# Copy stripped binary from builder
COPY --from=builder /build/target/release/skydoppler /usr/local/bin/skydoppler

# Run as non-root user
USER appuser:appgroup

# Production environment defaults
ENV HOST=0.0.0.0 \
    PORT=3000 \
    SKYDOPPLER_DATABASE_PATH=/data/skydoppler.db \
    RUST_LOG=info,skydoppler=info,skybase=info

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:3000/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/skydoppler"]
CMD ["serve"]
