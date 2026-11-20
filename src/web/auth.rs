//! Constant-time operator authentication helpers for `skydoppler`.
//!
//! Public endpoints and `/api/v1/stream` always fail closed on small accounts unless the request
//! carries a valid `SKYDOPPLER_OPERATOR_TOKEN` via `Authorization: Bearer <token>` or an
//! `HttpOnly; SameSite=Strict` session cookie.

use axum::http::{header, HeaderMap};
use subtle::ConstantTimeEq;

/// Cookie name storing the operator session token when authenticated via the dashboard UI.
pub const OPERATOR_COOKIE_NAME: &str = "skydoppler_operator";

/// Minimum accepted character length for `SKYDOPPLER_OPERATOR_TOKEN`.
pub const MIN_OPERATOR_TOKEN_LEN: usize = 16;

/// Validated operator authentication configuration held in web router state.
#[derive(Debug, Clone, Default)]
pub struct OperatorAuthConfig {
    expected_token: Option<String>,
}

impl OperatorAuthConfig {
    /// Loads operator authentication settings from `SKYDOPPLER_OPERATOR_TOKEN`.
    ///
    /// Empty, placeholder, or `< 16`-character values are discarded (operator mode disabled).
    #[must_use]
    pub fn from_env() -> Self {
        let token = crate::env::var(&["SKYDOPPLER_OPERATOR_TOKEN"]);
        Self::with_token(token)
    }

    /// Constructs an [`OperatorAuthConfig`] from an optional raw token string, enforcing
    /// minimum length and placeholder rejection.
    #[must_use]
    pub fn with_token(token: Option<String>) -> Self {
        let expected_token = token
            .map(|t| t.trim().to_string())
            .filter(|t| is_valid_configured_token(t));
        Self { expected_token }
    }

    /// Returns `true` when operator authentication is configured and active.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.expected_token.is_some()
    }

    /// Returns the configured operator token slice, if active.
    #[must_use]
    pub fn expected_token(&self) -> Option<&str> {
        self.expected_token.as_deref()
    }

    /// Returns `true` when `candidate` matches the configured token in constant time.
    #[must_use]
    pub fn verify_token(&self, candidate: &str) -> bool {
        verify_operator_token(candidate, self.expected_token.as_deref())
    }
}

/// Returns `true` if `raw` meets operator secret length and non-placeholder rules.
#[must_use]
pub fn is_valid_configured_token(raw: &str) -> bool {
    let trimmed = raw.trim();
    if trimmed.len() < MIN_OPERATOR_TOKEN_LEN {
        return false;
    }
    let lower = trimmed.to_ascii_lowercase();
    !(lower.contains("placeholder")
        || lower.contains("changeme")
        || lower.starts_with("your_")
        || lower.starts_with("replace_with_")
        || lower == "skydoppler_operator_token")
}

/// Verifies `candidate` against `expected` using [`subtle::ConstantTimeEq`].
///
/// Fails closed (`false`) whenever `expected` is `None`, invalid/too short, or empty.
#[must_use]
pub fn verify_operator_token(candidate: &str, expected: Option<&str>) -> bool {
    let Some(exp) = expected else {
        return false;
    };
    if !is_valid_configured_token(exp) {
        return false;
    }
    let cand_bytes = candidate.trim().as_bytes();
    let exp_bytes = exp.trim().as_bytes();
    if cand_bytes.is_empty() {
        return false;
    }
    bool::from(cand_bytes.ct_eq(exp_bytes))
}

/// Extracts an operator bearer or cookie token from HTTP request headers, if present.
#[must_use]
pub fn extract_bearer_or_cookie_token(headers: &HeaderMap) -> Option<String> {
    if let Some(auth_val) = headers.get(header::AUTHORIZATION) {
        if let Ok(raw) = auth_val.to_str() {
            let trimmed = raw.trim();
            if trimmed.len() > 7 && trimmed[..7].eq_ignore_ascii_case("bearer ") {
                let tok = trimmed[7..].trim();
                if !tok.is_empty() {
                    return Some(tok.to_string());
                }
            }
        }
    }

    if let Some(cookie_val) = headers.get(header::COOKIE) {
        if let Ok(raw) = cookie_val.to_str() {
            for part in raw.split(';') {
                let part = part.trim();
                if let Some(val) = part.strip_prefix(&format!("{OPERATOR_COOKIE_NAME}=")) {
                    let tok = val.trim();
                    if !tok.is_empty() {
                        return Some(tok.to_string());
                    }
                }
            }
        }
    }

    None
}

/// Returns `true` if `headers` authenticate as a valid operator session.
#[must_use]
pub fn extract_is_operator(headers: &HeaderMap, expected_token: Option<&str>) -> bool {
    let Some(candidate) = extract_bearer_or_cookie_token(headers) else {
        return false;
    };
    verify_operator_token(&candidate, expected_token)
}

/// Returns `true` when proxy headers indicate the client arrived over HTTPS.
#[must_use]
pub fn is_https_request(headers: &HeaderMap) -> bool {
    headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("https"))
}

/// Formats an `HttpOnly; SameSite=Strict` `Set-Cookie` header value storing `token`.
#[must_use]
pub fn build_operator_cookie_header(token: &str, secure: bool) -> String {
    let secure_attr = if secure { "; Secure" } else { "" };
    format!(
        "{OPERATOR_COOKIE_NAME}={}; Path=/; HttpOnly; SameSite=Strict{secure_attr}",
        token.trim()
    )
}

/// Formats a `Set-Cookie` header value clearing the operator cookie immediately.
#[must_use]
pub fn build_operator_logout_cookie_header(secure: bool) -> String {
    let secure_attr = if secure { "; Secure" } else { "" };
    format!("{OPERATOR_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0{secure_attr}")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn operator_token_validation_and_constant_time_check() {
        assert!(!is_valid_configured_token("short"));
        assert!(!is_valid_configured_token("placeholder_123456"));
        assert!(!is_valid_configured_token("your_operator_token_here"));
        let valid = "doppler_secret_token_0123456789";
        assert!(is_valid_configured_token(valid));

        let cfg = OperatorAuthConfig::with_token(Some("too_short".to_string()));
        assert!(!cfg.is_enabled());
        assert!(!cfg.verify_token("too_short"));

        let active = OperatorAuthConfig::with_token(Some(format!("  {valid}  ")));
        assert!(active.is_enabled());
        assert!(active.verify_token(valid));
        assert!(!active.verify_token(""));
        assert!(!active.verify_token("doppler_secret_token_0123456780"));
        assert!(!verify_operator_token(valid, None));
        assert!(!verify_operator_token(valid, Some("short")));
    }

    #[test]
    fn header_cookie_extraction_and_formatting() {
        let valid = "doppler_secret_token_0123456789";
        let mut h = HeaderMap::new();
        assert!(!extract_is_operator(&h, Some(valid)));

        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("bearer {valid}")).unwrap(),
        );
        assert!(extract_is_operator(&h, Some(valid)));

        let mut cookie_headers = HeaderMap::new();
        cookie_headers.insert(
            header::COOKIE,
            HeaderValue::from_str(&format!("other=1; {OPERATOR_COOKIE_NAME}={valid}")).unwrap(),
        );
        assert!(extract_is_operator(&cookie_headers, Some(valid)));
        assert!(!extract_is_operator(
            &cookie_headers,
            Some("another_valid_token_999")
        ));

        cookie_headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        assert!(is_https_request(&cookie_headers));

        let set_c = build_operator_cookie_header(valid, true);
        assert!(set_c.contains("HttpOnly; SameSite=Strict; Secure"));
        let clear_c = build_operator_logout_cookie_header(false);
        assert!(clear_c.contains("Max-Age=0"));
        assert!(!clear_c.contains("; Secure"));
    }
}
