//! Embedded HTML/Canvas single-page dashboard for the SkyDoppler Live Doppler Radar.

use axum::response::Html;

/// Static single-page Doppler Radar UI compiled from `assets/dashboard.html`.
pub const DASHBOARD_HTML: &str = include_str!("../../assets/dashboard.html");

/// Handler for `GET /`: serves the live SkyDoppler Doppler Radar UI.
pub async fn serve_dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}
