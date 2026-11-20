//! Axum web dashboard, REST API, live SSE push stream, and Prometheus metrics server.

pub mod api;
pub mod auth;
pub mod ui;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::{header, HeaderValue, Method};
use axum::routing::{get, post};
use axum::Router;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;

pub use api::{
    dismiss_storm, get_prometheus_metrics, get_storm, get_weather, health_check, list_storms,
    operator_auth, operator_logout, stream_events, ApiState, ApiStormItem, DismissStormResponse,
    ErrorResponse, HealthResponse, OperatorAuthRequest, OperatorAuthResponse, StormsQuery,
    StormsResponse, WeatherQuery, WeatherResponse,
};
pub use auth::{
    build_operator_cookie_header, build_operator_logout_cookie_header,
    extract_bearer_or_cookie_token, extract_is_operator, is_valid_configured_token,
    verify_operator_token, OperatorAuthConfig, MIN_OPERATOR_TOKEN_LEN, OPERATOR_COOKIE_NAME,
};
pub use ui::{serve_dashboard, DASHBOARD_HTML};

use crate::engine::SkydopplerEngine;
use crate::error::{Result, SkydopplerError};

/// Default bind host when `HOST` / `SKYDOPPLER_HOST` are unset.
pub const DEFAULT_WEB_HOST: &str = "127.0.0.1";
/// Default TCP port when `PORT` / `SKYDOPPLER_PORT` are unset.
pub const DEFAULT_WEB_PORT: u16 = 3000;

/// Configuration options governing the embedded SkyDoppler HTTP/SSE server.
#[derive(Debug, Clone)]
pub struct WebServerConfig {
    /// IP address or interface to bind (e.g. `"127.0.0.1"` or `"0.0.0.0"`).
    pub host: String,
    /// TCP port to listen on.
    pub port: u16,
    /// Explicit CORS allowed origins (`SKYDOPPLER_CORS_ORIGINS`), comma-separated.
    pub cors_origins: Vec<String>,
    /// Operator authentication settings (`SKYDOPPLER_OPERATOR_TOKEN`).
    pub auth: OperatorAuthConfig,
}

impl Default for WebServerConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_WEB_HOST.to_string(),
            port: DEFAULT_WEB_PORT,
            cors_origins: Vec::new(),
            auth: OperatorAuthConfig::default(),
        }
    }
}

impl WebServerConfig {
    /// Creates a new [`WebServerConfig`] bound to `host:port`.
    #[must_use]
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            cors_origins: Vec::new(),
            auth: OperatorAuthConfig::default(),
        }
    }

    /// Configures operator authentication from an explicit token option.
    #[must_use]
    pub fn with_operator_token(mut self, token: Option<String>) -> Self {
        self.auth = OperatorAuthConfig::with_token(token);
        self
    }

    /// Configures explicit allowed CORS origins.
    #[must_use]
    pub fn with_cors_origins(mut self, origins: Vec<String>) -> Self {
        self.cors_origins = origins;
        self
    }

    /// Loads [`WebServerConfig`] from environment variables (`HOST`, `PORT`, `SKYDOPPLER_*`).
    #[must_use]
    pub fn from_env() -> Self {
        let host = crate::env::var_or(&["HOST", "SKYDOPPLER_HOST"], DEFAULT_WEB_HOST);
        let port = crate::env::parsed_or(&["PORT", "SKYDOPPLER_PORT"], DEFAULT_WEB_PORT);
        let cors_origins = crate::env::var(&["SKYDOPPLER_CORS_ORIGINS"])
            .map(|raw| {
                raw.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        let auth = OperatorAuthConfig::from_env();
        Self {
            host,
            port,
            cors_origins,
            auth,
        }
    }
}

/// Builds the full Axum [`Router`] for SkyDoppler's UI, REST API, SSE stream, and `/metrics`.
pub fn create_web_router(engine: Arc<SkydopplerEngine>, config: &WebServerConfig) -> Router {
    let api_state = ApiState::new(engine, config.auth.clone());

    let parsed_origins: Vec<HeaderValue> = config
        .cors_origins
        .iter()
        .filter_map(|origin| HeaderValue::from_str(origin).ok())
        .collect();

    let mut cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION, header::ACCEPT]);

    if !parsed_origins.is_empty() {
        cors = cors.allow_origin(AllowOrigin::list(parsed_origins));
    }

    let v1_router = Router::new()
        .route("/health", get(api::health_check))
        .route("/metrics", get(api::get_prometheus_metrics))
        .route("/weather", get(api::get_weather))
        .route("/storms", get(api::list_storms))
        .route("/storms/{id}", get(api::get_storm))
        .route("/stream", get(api::stream_events))
        .route("/operator/auth", post(api::operator_auth))
        .route("/operator/logout", post(api::operator_logout))
        .route("/operator/storms/{id}/dismiss", post(api::dismiss_storm))
        .with_state(api_state.clone());

    Router::new()
        .route("/", get(ui::serve_dashboard))
        .route(
            "/healthz",
            get(api::health_check).with_state(api_state.clone()),
        )
        .route(
            "/metrics",
            get(api::get_prometheus_metrics).with_state(api_state),
        )
        .nest("/api/v1", v1_router)
        .layer(cors)
        .layer(axum::middleware::from_fn(security_headers_middleware))
        .layer(TraceLayer::new_for_http())
}

async fn security_headers_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mut resp = next.run(req).await;
    let headers = resp.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    headers.insert(
        header::HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static(
            "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none';",
        ),
    );
    resp
}

/// Runs the embedded Axum web server until `cancel` triggers graceful shutdown.
///
/// # Errors
/// Returns [`SkydopplerError::Config`] or [`SkydopplerError::Internal`] on invalid bind addresses
/// or TCP listener failures.
pub async fn run_web_server(
    config: WebServerConfig,
    engine: Arc<SkydopplerEngine>,
    cancel: CancellationToken,
) -> Result<()> {
    let app = create_web_router(engine, &config);
    let addr_str = format!("{}:{}", config.host, config.port);
    let addr: SocketAddr = addr_str.parse().map_err(|e| {
        SkydopplerError::Config(format!("Invalid web listen address '{addr_str}': {e}"))
    })?;

    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        SkydopplerError::Internal(format!(
            "Failed to bind SkyDoppler HTTP listener on {addr}: {e}"
        ))
    })?;

    info!(
        address = %addr,
        operator_auth = config.auth.is_enabled(),
        "🌐 SkyDoppler Web Dashboard & SSE Radar listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel.cancelled().await;
            info!("SkyDoppler HTTP server shutting down gracefully");
        })
        .await
        .map_err(|e| SkydopplerError::Internal(format!("Web server error: {e}")))?;

    Ok(())
}
