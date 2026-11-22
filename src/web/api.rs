//! REST, SSE push stream, operator auth, and Prometheus `/metrics` HTTP handlers.

use std::convert::Infallible;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures_util::stream::{self, Stream};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::engine::{EngineStatsSnapshot, SkydopplerEngine};
use crate::redact::{OperatorStormView, PublicStormView, WireRadarEvent};
use crate::types::{StormCategory, StormSeverity, WeatherPoint, WeatherSummary};
use crate::web::auth::{
    build_operator_cookie_header, build_operator_logout_cookie_header, extract_is_operator,
    is_https_request, verify_operator_token, OperatorAuthConfig,
};

/// Shared HTTP API application state.
#[derive(Clone)]
pub struct ApiState {
    /// Core SkyDoppler streaming engine.
    pub engine: Arc<SkydopplerEngine>,
    /// Operator token configuration for unredacted inspections and dismissal mutations.
    pub auth: OperatorAuthConfig,
}

impl ApiState {
    /// Constructs a new [`ApiState`] wrapping `engine` and `auth`.
    #[must_use]
    pub fn new(engine: Arc<SkydopplerEngine>, auth: OperatorAuthConfig) -> Self {
        Self { engine, auth }
    }
}

/// JSON error envelope returned for `4xx`/`5xx` HTTP failures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// Human-readable error message.
    pub error: String,
}

/// Response payload for `GET /healthz` and `GET /api/v1/health`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Service health literal (`"healthy"`).
    pub status: String,
    /// Crate version (`CARGO_PKG_VERSION`).
    pub version: String,
    /// Latest processed or persisted Jetstream microsecond cursor.
    pub latest_cursor_us: u64,
    /// Count of currently active storm cells.
    pub active_storms: usize,
}

/// Query parameters for `GET /api/v1/weather`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WeatherQuery {
    /// Maximum historical weather points to return (`1..=2000`, default `120`).
    pub history_limit: Option<usize>,
}

/// Response payload for `GET /api/v1/weather`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherResponse {
    /// Aggregate macro weather summary.
    pub summary: WeatherSummary,
    /// Chronological weather index time-series points.
    pub history: Vec<WeatherPoint>,
    /// Cumulative engine telemetry counters.
    pub stats: EngineStatsSnapshot,
    /// Crate version.
    pub version: String,
}

/// Query parameters for `GET /api/v1/storms`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct StormsQuery {
    /// Restrict results to `active` storms when `true` (default `false`).
    pub active_only: Option<bool>,
    /// Maximum storms returned (`1..=500`, default `50`).
    pub limit: Option<usize>,
    /// Optional severity filter (`elevated`, `high`, `critical`).
    pub severity: Option<String>,
    /// Optional category filter (`dogpile`, `public_figure_backlash`, etc.).
    pub category: Option<String>,
}

/// Polymorphic storm item respecting the fail-closed public vs. operator boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ApiStormItem {
    /// Operator-scoped view retaining full identity details plus public redaction status.
    Operator(OperatorStormView),
    /// Public view strictly stripping identifying details on small/unknown accounts.
    Public(PublicStormView),
}

/// Response payload for `GET /api/v1/storms`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StormsResponse {
    /// Whether the request authenticated as an authorized operator.
    pub operator_mode: bool,
    /// Current network weather summary.
    pub weather: WeatherSummary,
    /// Ordered list of storm cells (most recently updated first).
    pub storms: Vec<ApiStormItem>,
}

/// Request payload for `POST /api/v1/operator/auth`.
#[derive(Debug, Clone, Deserialize)]
pub struct OperatorAuthRequest {
    /// Operator secret token matching `SKYDOPPLER_OPERATOR_TOKEN`.
    pub token: String,
}

/// Response payload for `POST /api/v1/operator/auth` and `POST /api/v1/operator/logout`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorAuthResponse {
    /// Resulting operator unlock state.
    pub operator_mode: bool,
}

/// Response payload for `POST /api/v1/operator/storms/{id}/dismiss`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DismissStormResponse {
    /// `true` when the storm row transitioned cleanly.
    pub dismissed: bool,
    /// Database row identifier.
    pub storm_id: i64,
    /// Opaque radar cell identifier.
    pub cell_id: String,
}

fn json_err(status: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (status, Json(ErrorResponse { error: msg.into() }))
}

/// Handler for `GET /healthz` and `GET /api/v1/health`.
pub async fn health_check(State(state): State<ApiState>) -> (StatusCode, Json<HealthResponse>) {
    let active_storms = state
        .engine
        .list_storms(true, 500)
        .map(|s| s.len())
        .unwrap_or(0);
    (
        StatusCode::OK,
        Json(HealthResponse {
            status: "healthy".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            latest_cursor_us: state.engine.latest_cursor_us(),
            active_storms,
        }),
    )
}

/// Handler for `GET /metrics` and `GET /api/v1/metrics`: Prometheus text exposition.
pub async fn get_prometheus_metrics(State(state): State<ApiState>) -> Response {
    let stats = state.engine.stats().snapshot();
    let weather = state.engine.current_weather().ok();
    let active_storms = weather.as_ref().map_or(0, |w| w.active_storms);
    let weather_index = weather.as_ref().map_or(0.0, |w| w.weather_index);
    let tracker_snap = state.engine.tracker().snapshot();

    let mut out = String::with_capacity(4096);

    let _ = writeln!(
        out,
        "# HELP skydoppler_commits_received_total Total Jetstream commits observed."
    );
    let _ = writeln!(out, "# TYPE skydoppler_commits_received_total counter");
    let _ = writeln!(
        out,
        "skydoppler_commits_received_total {}",
        stats.commits_received
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_posts_observed_total Post replies and quotes processed by Stage 1."
    );
    let _ = writeln!(out, "# TYPE skydoppler_posts_observed_total counter");
    let _ = writeln!(
        out,
        "skydoppler_posts_observed_total {}",
        stats.posts_observed
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_hot_likes_observed_total Likes/reposts hitting already-tracked hot posts."
    );
    let _ = writeln!(out, "# TYPE skydoppler_hot_likes_observed_total counter");
    let _ = writeln!(
        out,
        "skydoppler_hot_likes_observed_total {}",
        stats.hot_likes_observed
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_cold_likes_ignored_total Cold likes/reposts skipped without allocation."
    );
    let _ = writeln!(out, "# TYPE skydoppler_cold_likes_ignored_total counter");
    let _ = writeln!(
        out,
        "skydoppler_cold_likes_ignored_total {}",
        stats.cold_likes_ignored
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_defensive_signals_total Target defensive block, postgate, and threadgate signals."
    );
    let _ = writeln!(out, "# TYPE skydoppler_defensive_signals_total counter");
    let _ = writeln!(
        out,
        "skydoppler_defensive_signals_total {}",
        stats.defensive_signals
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_stage1_candidates_total Stage 1 surge candidates forwarded to Stage 2."
    );
    let _ = writeln!(out, "# TYPE skydoppler_stage1_candidates_total counter");
    let _ = writeln!(
        out,
        "skydoppler_stage1_candidates_total {}",
        stats.stage1_candidates
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_stage1_queue_shed_total Stage 1 candidates shed due to worker queue backpressure."
    );
    let _ = writeln!(out, "# TYPE skydoppler_stage1_queue_shed_total counter");
    let _ = writeln!(
        out,
        "skydoppler_stage1_queue_shed_total {}",
        stats.stage1_queue_shed
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_stage2_enrichments_total Stage 2 batched AppView enrichments completed."
    );
    let _ = writeln!(out, "# TYPE skydoppler_stage2_enrichments_total counter");
    let _ = writeln!(
        out,
        "skydoppler_stage2_enrichments_total {}",
        stats.stage2_enrichments
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_stage3_stance_calls_total Stage 3 LLM stance verifications executed."
    );
    let _ = writeln!(out, "# TYPE skydoppler_stage3_stance_calls_total counter");
    let _ = writeln!(
        out,
        "skydoppler_stage3_stance_calls_total {}",
        stats.stage3_stance_calls
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_stage3_stance_fallbacks_total Stage 3 LLM stance calls that failed and fell back to structural scoring."
    );
    let _ = writeln!(
        out,
        "# TYPE skydoppler_stage3_stance_fallbacks_total counter"
    );
    let _ = writeln!(
        out,
        "skydoppler_stage3_stance_fallbacks_total {}",
        stats.stage3_stance_fallbacks
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_stage3_benign_cache_hits_total Stage 2/3 evaluations short-circuited by the benign verdict cache."
    );
    let _ = writeln!(
        out,
        "# TYPE skydoppler_stage3_benign_cache_hits_total counter"
    );
    let _ = writeln!(
        out,
        "skydoppler_stage3_benign_cache_hits_total {}",
        stats.stage3_benign_cache_hits
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_storms_detected_total Confirmed network storms persisted."
    );
    let _ = writeln!(out, "# TYPE skydoppler_storms_detected_total counter");
    let _ = writeln!(
        out,
        "skydoppler_storms_detected_total {}",
        stats.storms_detected
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_storms_escalated_total Storm severity escalations emitted."
    );
    let _ = writeln!(out, "# TYPE skydoppler_storms_escalated_total counter");
    let _ = writeln!(
        out,
        "skydoppler_storms_escalated_total {}",
        stats.storms_escalated
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_storms_suppressed_benign_total Surges suppressed cleanly after Stage 2/3 verification."
    );
    let _ = writeln!(
        out,
        "# TYPE skydoppler_storms_suppressed_benign_total counter"
    );
    let _ = writeln!(
        out,
        "skydoppler_storms_suppressed_benign_total {}",
        stats.storms_suppressed_benign
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_errors_total Operational errors during enrichment, stance, or storage."
    );
    let _ = writeln!(out, "# TYPE skydoppler_errors_total counter");
    let _ = writeln!(out, "skydoppler_errors_total {}", stats.errors_encountered);

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_active_storms Current count of active network storm cells."
    );
    let _ = writeln!(out, "# TYPE skydoppler_active_storms gauge");
    let _ = writeln!(out, "skydoppler_active_storms {active_storms}");

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_weather_index Current macro atmospheric weather index (0..100)."
    );
    let _ = writeln!(out, "# TYPE skydoppler_weather_index gauge");
    let _ = writeln!(out, "skydoppler_weather_index {weather_index:.2}");

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_tracked_posts Hot focal posts currently in sharded rolling RAM windows."
    );
    let _ = writeln!(out, "# TYPE skydoppler_tracked_posts gauge");
    let _ = writeln!(
        out,
        "skydoppler_tracked_posts {}",
        tracker_snap.tracked_posts
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_tracked_authors Target DIDs currently in defensive distress shards."
    );
    let _ = writeln!(out, "# TYPE skydoppler_tracked_authors gauge");
    let _ = writeln!(
        out,
        "skydoppler_tracked_authors {}",
        tracker_snap.tracked_authors
    );

    let _ = writeln!(
        out,
        "\n# HELP skydoppler_build_info Build and version metadata."
    );
    let _ = writeln!(out, "# TYPE skydoppler_build_info gauge");
    let _ = writeln!(
        out,
        "skydoppler_build_info{{version=\"{}\"}} 1",
        env!("CARGO_PKG_VERSION")
    );

    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        out,
    )
        .into_response()
}

/// Handler for `GET /api/v1/weather`.
pub async fn get_weather(
    State(state): State<ApiState>,
    Query(query): Query<WeatherQuery>,
) -> Result<Json<WeatherResponse>, (StatusCode, Json<ErrorResponse>)> {
    let history_limit = query.history_limit.unwrap_or(120).clamp(1, 2_000);
    let summary = state
        .engine
        .current_weather()
        .map_err(|e| json_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let history = state
        .engine
        .store()
        .list_weather_history(history_limit)
        .map_err(|e| json_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let stats = state.engine.stats().snapshot();

    Ok(Json(WeatherResponse {
        summary,
        history,
        stats,
        version: env!("CARGO_PKG_VERSION").to_string(),
    }))
}

/// Handler for `GET /api/v1/storms`.
///
/// Always applies [`PublicStormView`] fail-closed redaction unless the caller presents a valid
/// `SKYDOPPLER_OPERATOR_TOKEN`.
pub async fn list_storms(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<StormsQuery>,
) -> Result<Json<StormsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let severity_filter = match query
        .severity
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all"))
    {
        Some(raw) => Some(
            raw.parse::<StormSeverity>()
                .map_err(|e| json_err(StatusCode::BAD_REQUEST, e.to_string()))?,
        ),
        None => None,
    };

    let category_filter = match query
        .category
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all"))
    {
        Some(raw) => Some(
            raw.parse::<StormCategory>()
                .map_err(|e| json_err(StatusCode::BAD_REQUEST, e.to_string()))?,
        ),
        None => None,
    };

    let active_only = query.active_only.unwrap_or(false);
    let limit = query.limit.unwrap_or(50).clamp(1, 500);

    let is_operator = extract_is_operator(&headers, state.auth.expected_token());
    let threshold = state.engine.config().public_follower_threshold;

    let raw_storms = state
        .engine
        .list_storms(active_only, 500)
        .map_err(|e| json_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let storms = raw_storms
        .into_iter()
        .filter(|s| severity_filter.is_none_or(|sev| s.severity == sev))
        .filter(|s| category_filter.is_none_or(|cat| s.category == cat))
        .take(limit)
        .map(|storm| {
            if is_operator {
                ApiStormItem::Operator(OperatorStormView::from_storm(storm, threshold))
            } else {
                ApiStormItem::Public(PublicStormView::from_storm(&storm, threshold))
            }
        })
        .collect();

    let weather = state
        .engine
        .current_weather()
        .map_err(|e| json_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(StormsResponse {
        operator_mode: is_operator,
        weather,
        storms,
    }))
}

/// Handler for `GET /api/v1/storms/{id}` accepting either numeric row ID or `cell-<hex>`.
pub async fn get_storm(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiStormItem>, (StatusCode, Json<ErrorResponse>)> {
    let storm = state
        .engine
        .get_storm(&id)
        .map_err(|e| json_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            json_err(
                StatusCode::NOT_FOUND,
                format!("Storm cell '{id}' not found"),
            )
        })?;

    let is_operator = extract_is_operator(&headers, state.auth.expected_token());
    let threshold = state.engine.config().public_follower_threshold;

    let item = if is_operator {
        ApiStormItem::Operator(OperatorStormView::from_storm(storm, threshold))
    } else {
        ApiStormItem::Public(PublicStormView::from_storm(&storm, threshold))
    };

    Ok(Json(item))
}

/// Handler for `GET /api/v1/stream`: real-time Server-Sent Events feed enforcing per-subscriber
/// [`WireRadarEvent::project`] redaction.
pub async fn stream_events(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Sse<impl Stream<Item = std::result::Result<Event, Infallible>>> {
    let is_operator = extract_is_operator(&headers, state.auth.expected_token());
    let threshold = state.engine.config().public_follower_threshold;
    let rx = state.engine.subscribe_events();

    let sse_stream = stream::unfold(rx, move |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    let wire = WireRadarEvent::project(&ev, is_operator, threshold);
                    if let Ok(json) = serde_json::to_string(&wire) {
                        return Some((Ok::<Event, Infallible>(Event::default().data(json)), rx));
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    Sse::new(sse_stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

/// Handler for `POST /api/v1/operator/auth`: verifies token in constant time and issues cookie.
pub async fn operator_auth(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<OperatorAuthRequest>,
) -> Response {
    if !verify_operator_token(&payload.token, state.auth.expected_token()) {
        return json_err(
            StatusCode::UNAUTHORIZED,
            "Invalid or unconfigured operator token",
        )
        .into_response();
    }

    let cookie = build_operator_cookie_header(&payload.token, is_https_request(&headers));
    (
        StatusCode::OK,
        [(header::SET_COOKIE, cookie)],
        Json(OperatorAuthResponse {
            operator_mode: true,
        }),
    )
        .into_response()
}

/// Handler for `POST /api/v1/operator/logout`: clears the operator cookie immediately.
pub async fn operator_logout(headers: HeaderMap) -> Response {
    let cookie = build_operator_logout_cookie_header(is_https_request(&headers));
    (
        StatusCode::OK,
        [(header::SET_COOKIE, cookie)],
        Json(OperatorAuthResponse {
            operator_mode: false,
        }),
    )
        .into_response()
}

/// Handler for `POST /api/v1/operator/storms/{id}/dismiss`: marks a storm cell dismissed.
///
/// Requires valid operator authentication (`401 Unauthorized` otherwise).
pub async fn dismiss_storm(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<DismissStormResponse>, (StatusCode, Json<ErrorResponse>)> {
    if !extract_is_operator(&headers, state.auth.expected_token()) {
        return Err(json_err(
            StatusCode::UNAUTHORIZED,
            "Operator authentication required to dismiss storm cells",
        ));
    }

    let storm = state
        .engine
        .get_storm(&id)
        .map_err(|e| json_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            json_err(
                StatusCode::NOT_FOUND,
                format!("Storm cell '{id}' not found"),
            )
        })?;

    let dismissed = state
        .engine
        .dismiss_storm(storm.id)
        .map_err(|e| json_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(DismissStormResponse {
        dismissed,
        storm_id: storm.id,
        cell_id: storm.cell_id,
    }))
}
