//! Environment variable parsing helpers with safe fallbacks.

use std::str::FromStr;

/// Returns the first non-empty trimmed environment variable value matching `keys`.
#[must_use]
pub fn var(keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Ok(val) = std::env::var(key) {
            let trimmed = val.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

/// Returns the first non-empty environment variable in `keys`, or `default`.
#[must_use]
pub fn var_or(keys: &[&str], default: &str) -> String {
    var(keys).unwrap_or_else(|| default.to_string())
}

/// Parses the first matching environment variable into `T`, returning `None` if absent or invalid.
#[must_use]
pub fn parsed<T: FromStr>(keys: &[&str]) -> Option<T> {
    var(keys).and_then(|v| v.parse::<T>().ok())
}

/// Parses the first matching environment variable into `T`, falling back to `default`.
#[must_use]
pub fn parsed_or<T: FromStr>(keys: &[&str], default: T) -> T {
    parsed(keys).unwrap_or(default)
}

/// Parses the first matching boolean environment variable, or falls back to `default`.
#[must_use]
pub fn bool_or(keys: &[&str], default: bool) -> bool {
    match var(keys).map(|s| s.to_ascii_lowercase()) {
        Some(v) if matches!(v.as_str(), "1" | "true" | "yes" | "on") => true,
        Some(v) if matches!(v.as_str(), "0" | "false" | "no" | "off") => false,
        _ => default,
    }
}

/// Loads `KEY=VALUE` pairs from a `.env`-style file without overwriting existing env vars.
///
/// Blank lines and `#` comments are skipped, and matching surrounding `"` or `'` quotes are
/// stripped from values.
pub fn load_dotenv_file(path: &std::path::Path) {
    let Ok(content) = std::fs::read_to_string(path) else {
        return;
    };
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some((raw_key, raw_val)) = trimmed.split_once('=') {
            let key = raw_key.trim();
            if key.is_empty() {
                continue;
            }
            let val = raw_val.trim();
            let unquoted = if (val.starts_with('"') && val.ends_with('"') && val.len() >= 2)
                || (val.starts_with('\'') && val.ends_with('\'') && val.len() >= 2)
            {
                &val[1..val.len() - 1]
            } else {
                val
            };
            if std::env::var_os(key).is_none() {
                std::env::set_var(key, unquoted);
            }
        }
    }
}

/// Initializes the global structured tracing subscriber (`RUST_LOG` honored).
#[cfg(feature = "telemetry")]
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,skydoppler=info,skybase=info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn env_helpers_fallback_and_parse() {
        assert_eq!(var(&["SKYDOPPLER_DEFINITELY_UNSET_123"]), None);
        assert_eq!(
            var_or(&["SKYDOPPLER_DEFINITELY_UNSET_123"], "fallback"),
            "fallback"
        );
        assert_eq!(parsed_or(&["SKYDOPPLER_DEFINITELY_UNSET_123"], 42_u64), 42);
        assert!(bool_or(&["SKYDOPPLER_DEFINITELY_UNSET_123"], true));
        assert!(!bool_or(&["SKYDOPPLER_DEFINITELY_UNSET_123"], false));
        // `PATH` is guaranteed non-empty in test runner environments.
        assert!(var(&["PATH"]).is_some());
    }

    #[test]
    fn dotenv_loader_parses_without_overwriting_existing_vars() {
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join(".env");
        std::fs::write(
            &env_path,
            "# Comment line\n\nSKYDOPPLER_TEST_DOTENV_A=\"alpha_val\"\nSKYDOPPLER_TEST_DOTENV_B='beta_val'\nSKYDOPPLER_TEST_DOTENV_BOOL=true\nPATH=/should_not_override\n",
        )
        .unwrap();

        let prev_path = std::env::var("PATH").unwrap();
        load_dotenv_file(&env_path);
        load_dotenv_file(&dir.path().join("missing.env"));

        assert_eq!(
            var(&["SKYDOPPLER_TEST_DOTENV_A"]).as_deref(),
            Some("alpha_val")
        );
        assert_eq!(
            var(&["SKYDOPPLER_TEST_DOTENV_B"]).as_deref(),
            Some("beta_val")
        );
        assert!(bool_or(&["SKYDOPPLER_TEST_DOTENV_BOOL"], false));
        assert_eq!(std::env::var("PATH").unwrap(), prev_path);
    }

    #[cfg(feature = "telemetry")]
    #[test]
    fn init_tracing_is_idempotent() {
        init_tracing();
        init_tracing();
    }
}
