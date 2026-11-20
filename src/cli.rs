//! Command-line argument parser, configuration status formatter, and subcommand dispatcher.

use std::io::Write;

use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::daemon::{self, DaemonConfig};
use crate::error::{Result, SkydopplerError};
use crate::web::WebServerConfig;

/// Parsed top-level CLI subcommand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliCommand {
    /// Start the live Jetstream radar daemon and HTTP/SSE dashboard (`serve` / `daemon`).
    Serve,
    /// Display resolved runtime configuration without opening network sockets (`status`).
    Status,
    /// Query `GET /api/v1/weather` from a running SkyDoppler instance (`weather`).
    Weather,
    /// Query `GET /api/v1/storms` from a running SkyDoppler instance (`storms`).
    Storms,
    /// Print CLI help usage (`--help` / `-h`).
    Help,
    /// Print package version (`--version` / `-V`).
    Version,
}

/// Parsed CLI options overriding environment defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliArgs {
    /// Selected subcommand.
    pub command: CliCommand,
    /// Optional HTTP bind host override (`--host`).
    pub host: Option<String>,
    /// Optional HTTP bind port override (`--port`).
    pub port: Option<u16>,
    /// Optional SQLite database path override (`--db`).
    pub db: Option<String>,
    /// Optional public redaction follower ceiling override (`--follower-threshold`).
    pub follower_threshold: Option<u64>,
    /// Optional Jetstream WebSocket URL override (`--jetstream`).
    pub jetstream: Option<String>,
    /// Optional remote daemon base URL for `weather`/`storms` queries (`--url`).
    pub url: Option<String>,
    /// Optional operator bearer token (`--token`).
    pub token: Option<String>,
    /// Filter remote `storms` query to active cells (`--active-only`).
    pub active_only: bool,
    /// Optional storm result limit (`--limit`).
    pub limit: Option<usize>,
    /// Emit raw JSON output for `weather` / `storms` queries (`--json`).
    pub json: bool,
}

fn take_next_val<'a>(args: &'a [String], i: &mut usize, flag: &str) -> Result<&'a str> {
    if *i + 1 >= args.len() {
        return Err(SkydopplerError::Config(format!(
            "Flag '{flag}' requires a value"
        )));
    }
    *i += 1;
    Ok(args[*i].as_str())
}

/// Parses raw process arguments (including `argv[0]`) into [`CliArgs`].
///
/// # Errors
/// Returns [`SkydopplerError::Config`] on unknown subcommands, unrecognized flags, or invalid
/// numeric option values.
pub fn parse_args(args: &[String]) -> Result<CliArgs> {
    let mut command = CliCommand::Serve;
    let mut command_set = false;
    let mut host = None;
    let mut port = None;
    let mut db = None;
    let mut follower_threshold = None;
    let mut jetstream = None;
    let mut url = None;
    let mut token = None;
    let mut active_only = false;
    let mut limit = None;
    let mut json = false;

    let mut i = 1;
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "-h" | "--help" | "help" => {
                command = CliCommand::Help;
                command_set = true;
            }
            "-V" | "--version" | "version" => {
                command = CliCommand::Version;
                command_set = true;
            }
            "serve" | "daemon" if !command_set && !arg.starts_with('-') => {
                command = CliCommand::Serve;
                command_set = true;
            }
            "status" if !command_set => {
                command = CliCommand::Status;
                command_set = true;
            }
            "weather" if !command_set => {
                command = CliCommand::Weather;
                command_set = true;
            }
            "storms" if !command_set => {
                command = CliCommand::Storms;
                command_set = true;
            }
            "--host" => {
                host = Some(take_next_val(args, &mut i, "--host")?.to_string());
            }
            "--port" => {
                let raw = take_next_val(args, &mut i, "--port")?;
                let parsed = raw.parse::<u16>().map_err(|_| {
                    SkydopplerError::Config(format!("Invalid --port value '{raw}'"))
                })?;
                port = Some(parsed);
            }
            "--db" => {
                db = Some(take_next_val(args, &mut i, "--db")?.to_string());
            }
            "--follower-threshold" => {
                let raw = take_next_val(args, &mut i, "--follower-threshold")?;
                let parsed = raw.parse::<u64>().map_err(|_| {
                    SkydopplerError::Config(format!("Invalid --follower-threshold value '{raw}'"))
                })?;
                follower_threshold = Some(parsed);
            }
            "--jetstream" => {
                jetstream = Some(take_next_val(args, &mut i, "--jetstream")?.to_string());
            }
            "--url" => {
                url = Some(take_next_val(args, &mut i, "--url")?.to_string());
            }
            "--token" => {
                token = Some(take_next_val(args, &mut i, "--token")?.to_string());
            }
            "--active-only" => active_only = true,
            "--json" => json = true,
            "--limit" => {
                let raw = take_next_val(args, &mut i, "--limit")?;
                let parsed = raw.parse::<usize>().map_err(|_| {
                    SkydopplerError::Config(format!("Invalid --limit value '{raw}'"))
                })?;
                limit = Some(parsed.clamp(1, 500));
            }
            other => {
                return Err(SkydopplerError::Config(format!(
                    "Unrecognized argument '{other}'. Run with --help for usage."
                )));
            }
        }
        i += 1;
    }

    Ok(CliArgs {
        command,
        host,
        port,
        db,
        follower_threshold,
        jetstream,
        url,
        token,
        active_only,
        limit,
        json,
    })
}

/// Applies parsed CLI flag overrides cleanly onto [`DaemonConfig`].
pub fn apply_daemon_overrides(cfg: &mut DaemonConfig, parsed: &CliArgs) {
    if let Some(ref h) = parsed.host {
        cfg.web.host.clone_from(h);
    }
    if let Some(p) = parsed.port {
        cfg.web.port = p;
    }
    if let Some(ref d) = parsed.db {
        cfg.database_path.clone_from(d);
    }
    if let Some(thresh) = parsed.follower_threshold {
        cfg.radar.public_follower_threshold = thresh;
    }
    if let Some(ref js) = parsed.jetstream {
        cfg.stream.endpoint.clone_from(js);
    }
    if let Some(ref tok) = parsed.token {
        cfg.web = cfg.web.clone().with_operator_token(Some(tok.clone()));
    }
}

/// Resolves the target SkyDoppler HTTP base URL for `weather` / `storms` queries.
#[must_use]
pub fn resolve_base_url(parsed: &CliArgs) -> String {
    if let Some(ref u) = parsed.url {
        return u.trim_end_matches('/').to_string();
    }
    if let Some(u) = crate::env::var(&["SKYDOPPLER_URL"]) {
        return u.trim_end_matches('/').to_string();
    }
    let web_cfg = WebServerConfig::from_env();
    let host = if web_cfg.host == "0.0.0.0" {
        "127.0.0.1"
    } else {
        web_cfg.host.as_str()
    };
    let port = parsed.port.unwrap_or(web_cfg.port);
    format!("http://{host}:{port}")
}

/// Resolves an operator bearer token from `--token` or `SKYDOPPLER_OPERATOR_TOKEN`.
#[must_use]
pub fn resolve_token(parsed: &CliArgs) -> Option<String> {
    parsed
        .token
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| crate::env::var(&["SKYDOPPLER_OPERATOR_TOKEN"]))
}

/// Formats CLI help documentation.
#[must_use]
pub fn format_usage() -> String {
    format!(
        "skydoppler v{ver} — Network-Wide Bluesky Atmospheric Dogpile Radar\n\n\
USAGE:\n  skydoppler [COMMAND] [OPTIONS]\n\n\
COMMANDS:\n  serve, daemon     Run Jetstream radar daemon & web UI (default)\n  status            Show resolved configuration & privacy guardrails\n  weather           Query macro atmospheric weather index from running daemon\n  storms            List detected storm cells from running daemon\n\n\
OPTIONS:\n  --host <ADDR>                HTTP bind host (default: 127.0.0.1)\n  --port <PORT>                HTTP bind port (default: 3000)\n  --db <PATH>                  SQLite database path or ':memory:'\n  --follower-threshold <N>     Public redaction follower threshold (default: 5000)\n  --jetstream <WS_URL>         Jetstream WebSocket endpoint override\n  --url <HTTP_URL>             Daemon base URL for weather/storms queries\n  --token <SECRET>             Operator bearer token\n  --active-only                Restrict 'storms' output to active cells\n  --limit <N>                  Maximum storm rows (1..=500)\n  --json                       Emit raw JSON for weather/storms queries\n  -h, --help                   Show this help message\n  -V, --version                Print version\n",
        ver = env!("CARGO_PKG_VERSION")
    )
}

/// Formats resolved configuration telemetry cleanly without exposing raw secrets.
#[must_use]
pub fn format_status_report(cfg: &DaemonConfig) -> String {
    let operator_auth = if cfg.web.auth.is_enabled() {
        "configured"
    } else {
        "disabled"
    };
    let stance_mode = if cfg.radar.enable_llm_stance {
        "llm_enabled"
    } else {
        "structural_fallback"
    };
    format!(
        "SkyDoppler v{ver} Status\n\
  Bind Address            : {host}:{port}\n\
  SQLite Database         : {db}\n\
  Jetstream Endpoint      : {endpoint}\n\
  Collections Subscribed  : {cols}\n\
  Short / Medium Windows  : {sw}s / {mw}s\n\
  Table Caps (Posts/Auth) : {posts} / {authors}\n\
  Public Redact Threshold : < {thresh} followers (or unknown -> fail closed)\n\
  Stage 3 Stance Mode     : {stance}\n\
  Operator Auth Token     : {auth}\n",
        ver = env!("CARGO_PKG_VERSION"),
        host = cfg.web.host,
        port = cfg.web.port,
        db = cfg.database_path,
        endpoint = cfg.stream.endpoint,
        cols = cfg.stream.collections.join(", "),
        sw = cfg.radar.short_window.as_secs(),
        mw = cfg.radar.medium_window.as_secs(),
        posts = cfg.radar.max_tracked_posts,
        authors = cfg.radar.max_tracked_authors,
        thresh = cfg.radar.public_follower_threshold,
        stance = stance_mode,
        auth = operator_auth,
    )
}

/// Fetches JSON from a running SkyDoppler HTTP endpoint using optional operator Bearer token.
///
/// # Errors
/// Returns [`SkydopplerError::Http`] on transport failures or [`SkydopplerError::Internal`] on
/// non-2xx HTTP status codes.
pub async fn fetch_endpoint_json(full_url: &str, token: Option<&str>) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    let mut req = client.get(full_url);
    if let Some(tok) = token {
        req = req.bearer_auth(tok);
    }
    let resp = req.send().await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(SkydopplerError::Internal(format!(
            "HTTP {status} from {full_url}: {body}"
        )));
    }
    Ok(resp.json::<Value>().await?)
}

/// Formats the `/api/v1/weather` JSON response as human-readable text or pretty JSON.
///
/// # Errors
/// Returns [`SkydopplerError::Serialization`] if pretty JSON formatting fails.
pub fn format_weather_output(val: &Value, as_json: bool) -> Result<String> {
    if as_json {
        return Ok(serde_json::to_string_pretty(val)?);
    }
    let s = &val["summary"];
    let idx = s["weather_index"].as_f64().unwrap_or(0.0);
    let level = s["level"].as_str().unwrap_or("clear");
    let active = s["active_storms"].as_u64().unwrap_or(0);
    let small = s["small_target_storms"].as_u64().unwrap_or(0);
    let cps = s["commits_per_sec"].as_f64().unwrap_or(0.0);
    Ok(format!(
        "SkyDoppler Weather: index={idx:.1} ({level}) | active={active} | protected_small={small} | throughput={cps:.1} commits/s\n"
    ))
}

/// Formats the `/api/v1/storms` JSON response as human-readable lines or pretty JSON.
///
/// # Errors
/// Returns [`SkydopplerError::Serialization`] if pretty JSON formatting fails.
pub fn format_storms_output(val: &Value, as_json: bool) -> Result<String> {
    if as_json {
        return Ok(serde_json::to_string_pretty(val)?);
    }
    let empty = Vec::new();
    let storms = val["storms"].as_array().unwrap_or(&empty);
    if storms.is_empty() {
        return Ok("No matching storm cells found.\n".to_string());
    }
    let mut out = String::new();
    for st in storms {
        let cell = st["cell_id"].as_str().unwrap_or("-");
        let sev = st["severity"].as_str().unwrap_or("-");
        let cat = st["category"].as_str().unwrap_or("-");
        let score = st["metrics"]["composite_score"].as_f64().unwrap_or(0.0);
        let target = if st["redacted"].as_bool().unwrap_or(false) {
            "[redacted small account]"
        } else {
            st["target_handle"]
                .as_str()
                .or_else(|| st["target_did"].as_str())
                .unwrap_or("[unknown]")
        };
        out.push_str(&format!(
            "{cell}  severity={sev}  category={cat}  score={score:.2}  target={target}\n"
        ));
    }
    Ok(out)
}

/// Dispatches parsed CLI arguments writing output cleanly to `writer`.
///
/// # Errors
/// Returns [`SkydopplerError`] if argument parsing, remote HTTP queries, I/O writing, or
/// daemon execution fails.
pub async fn dispatch_to_writer<W: Write>(
    args: &[String],
    cancel: CancellationToken,
    writer: &mut W,
) -> Result<()> {
    let parsed = parse_args(args)?;
    let mut cfg = DaemonConfig::from_env();
    apply_daemon_overrides(&mut cfg, &parsed);

    match parsed.command {
        CliCommand::Help => {
            writeln!(writer, "{}", format_usage())
                .map_err(|e| SkydopplerError::Internal(e.to_string()))?;
            Ok(())
        }
        CliCommand::Version => {
            writeln!(writer, "skydoppler {}", env!("CARGO_PKG_VERSION"))
                .map_err(|e| SkydopplerError::Internal(e.to_string()))?;
            Ok(())
        }
        CliCommand::Status => {
            writeln!(writer, "{}", format_status_report(&cfg))
                .map_err(|e| SkydopplerError::Internal(e.to_string()))?;
            Ok(())
        }
        CliCommand::Weather => {
            let base = resolve_base_url(&parsed);
            let tok = resolve_token(&parsed);
            let val =
                fetch_endpoint_json(&format!("{base}/api/v1/weather"), tok.as_deref()).await?;
            let formatted = format_weather_output(&val, parsed.json)?;
            write!(writer, "{formatted}").map_err(|e| SkydopplerError::Internal(e.to_string()))?;
            Ok(())
        }
        CliCommand::Storms => {
            let base = resolve_base_url(&parsed);
            let tok = resolve_token(&parsed);
            let active_q = if parsed.active_only { "true" } else { "false" };
            let limit_q = parsed.limit.unwrap_or(50);
            let url = format!("{base}/api/v1/storms?active_only={active_q}&limit={limit_q}");
            let val = fetch_endpoint_json(&url, tok.as_deref()).await?;
            let formatted = format_storms_output(&val, parsed.json)?;
            write!(writer, "{formatted}").map_err(|e| SkydopplerError::Internal(e.to_string()))?;
            Ok(())
        }
        CliCommand::Serve => {
            #[cfg(feature = "telemetry")]
            crate::env::init_tracing();
            let _ = daemon::run(cfg, cancel).await?;
            Ok(())
        }
    }
}

/// Main CLI entrypoint writing directly to standard output.
///
/// # Errors
/// Propagates errors from [`dispatch_to_writer`].
pub async fn dispatch(args: &[String], cancel: CancellationToken) -> Result<()> {
    let mut stdout = std::io::stdout();
    dispatch_to_writer(args, cancel, &mut stdout).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use skybase::ingest::MockJetstreamServer;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn parse_args_and_overrides_cover_all_flags_and_errors() {
        let args = sv(&[
            "skydoppler",
            "serve",
            "--host",
            "0.0.0.0",
            "--port",
            "8088",
            "--db",
            ":memory:",
            "--follower-threshold",
            "10000",
            "--jetstream",
            "wss://custom/sub",
            "--token",
            "very_secret_operator_token_999",
        ]);
        let parsed = parse_args(&args).unwrap();
        assert_eq!(parsed.command, CliCommand::Serve);
        let mut cfg = DaemonConfig::default();
        apply_daemon_overrides(&mut cfg, &parsed);
        assert_eq!(cfg.web.host, "0.0.0.0");
        assert_eq!(cfg.web.port, 8088);
        assert_eq!(cfg.database_path, ":memory:");
        assert_eq!(cfg.radar.public_follower_threshold, 10_000);
        assert_eq!(cfg.stream.endpoint, "wss://custom/sub");
        assert!(cfg.web.auth.is_enabled());

        let report = format_status_report(&cfg);
        assert!(report.contains("configured"));
        assert!(!report.contains("very_secret_operator_token_999"));

        // Error cases
        assert!(parse_args(&sv(&["skydoppler", "bogus"])).is_err());
        assert!(parse_args(&sv(&["skydoppler", "--port"])).is_err());
        assert!(parse_args(&sv(&["skydoppler", "--port", "abc"])).is_err());
        assert!(parse_args(&sv(&["skydoppler", "--follower-threshold", "bad"])).is_err());
        assert!(parse_args(&sv(&["skydoppler", "--limit", "bad"])).is_err());
    }

    #[tokio::test]
    async fn dispatch_handles_help_version_status_weather_storms_and_serve() {
        let mut buf = Vec::new();
        dispatch_to_writer(
            &sv(&["skydoppler", "--help"]),
            CancellationToken::new(),
            &mut buf,
        )
        .await
        .unwrap();
        assert!(String::from_utf8(buf).unwrap().contains("USAGE:"));

        let mut buf = Vec::new();
        dispatch_to_writer(
            &sv(&["skydoppler", "--version"]),
            CancellationToken::new(),
            &mut buf,
        )
        .await
        .unwrap();
        assert!(String::from_utf8(buf)
            .unwrap()
            .contains(concat!("skydoppler ", env!("CARGO_PKG_VERSION"))));

        let mut buf = Vec::new();
        dispatch_to_writer(
            &sv(&["skydoppler", "status"]),
            CancellationToken::new(),
            &mut buf,
        )
        .await
        .unwrap();
        assert!(String::from_utf8(buf)
            .unwrap()
            .contains("Public Redact Threshold"));

        // Wiremock weather + storms queries
        let srv = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/weather"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "summary": {
                    "weather_index": 34.5,
                    "level": "storm_watch",
                    "active_storms": 2,
                    "small_target_storms": 1,
                    "commits_per_sec": 1200.0
                }
            })))
            .mount(&srv)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/v1/storms"))
            .and(query_param("active_only", "true"))
            .and(query_param("limit", "10"))
            .and(header("authorization", "Bearer operator_secret_123456"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "storms": [
                    {
                        "cell_id": "cell-aaaa1111",
                        "redacted": true,
                        "severity": "high",
                        "category": "dogpile",
                        "metrics": { "composite_score": 0.85 }
                    },
                    {
                        "cell_id": "cell-bbbb2222",
                        "redacted": false,
                        "target_handle": "pubfig.bsky.social",
                        "severity": "critical",
                        "category": "public_figure_backlash",
                        "metrics": { "composite_score": 0.93 }
                    }
                ]
            })))
            .mount(&srv)
            .await;

        let mut wbuf = Vec::new();
        dispatch_to_writer(
            &sv(&["skydoppler", "weather", "--url", &srv.uri()]),
            CancellationToken::new(),
            &mut wbuf,
        )
        .await
        .unwrap();
        let wtxt = String::from_utf8(wbuf).unwrap();
        assert!(wtxt.contains("34.5"));
        assert!(wtxt.contains("storm_watch"));

        let mut sbuf = Vec::new();
        dispatch_to_writer(
            &sv(&[
                "skydoppler",
                "storms",
                "--url",
                &srv.uri(),
                "--active-only",
                "--limit",
                "10",
                "--token",
                "operator_secret_123456",
            ]),
            CancellationToken::new(),
            &mut sbuf,
        )
        .await
        .unwrap();
        let stxt = String::from_utf8(sbuf).unwrap();
        assert!(stxt.contains("[redacted small account]"));
        assert!(stxt.contains("pubfig.bsky.social"));

        // JSON mode + empty storms formatting + non-2xx HTTP error handling
        let j = format_storms_output(&serde_json::json!({ "storms": [] }), false).unwrap();
        assert!(j.contains("No matching"));
        let wj = format_weather_output(&serde_json::json!({ "summary": {} }), true).unwrap();
        assert!(wj.contains("summary"));
        assert!(fetch_endpoint_json(&format!("{}/missing", srv.uri()), None)
            .await
            .is_err());

        // Serve cancellation via MockJetstreamServer on ephemeral port 0
        let mock_ws = MockJetstreamServer::start().await.unwrap();
        let cancel = CancellationToken::new();
        let c2 = cancel.clone();
        let ws_url = mock_ws.ws_url();
        let h = tokio::spawn(async move {
            let mut out = Vec::new();
            dispatch_to_writer(
                &sv(&[
                    "skydoppler",
                    "serve",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "0",
                    "--db",
                    ":memory:",
                    "--jetstream",
                    &ws_url,
                ]),
                c2,
                &mut out,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(80)).await;
        cancel.cancel();
        h.await.unwrap().unwrap();
    }

    use std::time::Duration;
}
