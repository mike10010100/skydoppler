//! Optional Stage 3 bundled LLM stance verifier with deterministic structural fallback.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Result, SkydopplerError};
use crate::types::{
    PostEngagementSnapshot, SampleReply, StormCategory, StormMetrics, StormStanceVerdict,
    TargetTier,
};

/// Supported LLM API dialects for bundled Stage 3 stance verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StanceDialect {
    /// Local Ollama `/api/chat` JSON schema endpoint.
    #[default]
    Ollama,
    /// `SystemOne` classification endpoint (`/v1/systemone`).
    SystemOne,
    /// Standard OpenAI-compatible/JEV `/v1/classify` endpoint.
    StandardJev,
}

/// Configuration for [`LlmStormStanceClassifier`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StanceConfig {
    /// Base URL for the LLM stance backend.
    pub base_url: String,
    /// Optional Bearer API token.
    pub api_key: Option<String>,
    /// Model identifier.
    pub model: String,
    /// Request timeout.
    pub timeout: Duration,
    /// Maximum retry count on transient 5xx / transport errors.
    pub max_retries: usize,
    /// Ollama context token window.
    pub num_ctx: u32,
}

impl Default for StanceConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:11434".to_string(),
            api_key: None,
            model: "qwen3.5:9b".to_string(),
            timeout: Duration::from_secs(15),
            max_retries: 1,
            num_ctx: 4096,
        }
    }
}

impl StanceConfig {
    /// Loads [`StanceConfig`] from `SKYDOPPLER_STANCE_*` environment variables if `BASE_URL` is configured.
    ///
    /// # Errors
    /// Returns [`SkydopplerError::Config`] if `SKYDOPPLER_STANCE_BASE_URL` is missing or blank.
    pub fn from_env() -> Result<Self> {
        let base_url = crate::env::var(&["SKYDOPPLER_STANCE_BASE_URL"]).ok_or_else(|| {
            SkydopplerError::Config("SKYDOPPLER_STANCE_BASE_URL is not configured".to_string())
        })?;
        let d = Self::default();
        let timeout_ms = crate::env::parsed_or(
            &["SKYDOPPLER_STANCE_TIMEOUT_MS"],
            d.timeout.as_millis() as u64,
        )
        .clamp(500, 120_000);
        Ok(Self {
            base_url,
            api_key: crate::env::var(&["SKYDOPPLER_STANCE_API_KEY"]),
            model: crate::env::var_or(&["SKYDOPPLER_STANCE_MODEL"], &d.model),
            timeout: Duration::from_millis(timeout_ms),
            max_retries: crate::env::parsed_or(&["SKYDOPPLER_STANCE_MAX_RETRIES"], d.max_retries)
                .min(4),
            num_ctx: crate::env::parsed_or(&["SKYDOPPLER_STANCE_NUM_CTX"], d.num_ctx).max(1024),
        })
    }
}

/// Bundled context supplied to a Stage 3 [`StormStanceClassifier`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StormStanceInput {
    /// Target post author DID.
    pub target_did: String,
    /// Target handle, if known.
    pub target_handle: Option<String>,
    /// Target audience tier.
    pub target_tier: TargetTier,
    /// Canonical AT-URI of the focal post.
    pub focal_post_uri: String,
    /// Enriched snapshot of the focal post, if resolved.
    pub focal_post: Option<PostEngagementSnapshot>,
    /// Enriched top amplifying quote-posts.
    pub top_quotes: Vec<PostEngagementSnapshot>,
    /// Sampled recent replies/quotes.
    pub sample_replies: Vec<SampleReply>,
    /// Quantitative structural metrics.
    pub metrics: StormMetrics,
}

/// Pluggable trait for verifying collective crowd hostility in a Stage 2 surge bundle.
#[async_trait]
pub trait StormStanceClassifier: Send + Sync {
    /// Evaluates `input` and returns a [`StormStanceVerdict`].
    async fn classify_storm(&self, input: &StormStanceInput) -> Result<StormStanceVerdict>;

    /// Human-readable model or evaluator identifier.
    fn model_name(&self) -> &str;
}

/// No-op stance classifier used when LLM verification is disabled (triggers structural fallback).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopStormStanceClassifier;

#[async_trait]
impl StormStanceClassifier for NoopStormStanceClassifier {
    async fn classify_storm(&self, _input: &StormStanceInput) -> Result<StormStanceVerdict> {
        Err(SkydopplerError::Stance(
            "Stage 3 LLM stance verification is disabled; using structural fallback".to_string(),
        ))
    }

    fn model_name(&self) -> &str {
        "structural_fallback"
    }
}

/// Deterministic mock stance classifier for unit and pipeline integration tests.
#[derive(Debug, Clone)]
pub struct MockStormStanceClassifier {
    verdict: std::sync::Arc<parking_lot::RwLock<std::result::Result<StormStanceVerdict, String>>>,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl MockStormStanceClassifier {
    /// Creates a mock returning `verdict`.
    #[must_use]
    pub fn new(verdict: StormStanceVerdict) -> Self {
        Self {
            verdict: std::sync::Arc::new(parking_lot::RwLock::new(Ok(verdict))),
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Creates a mock returning an error string.
    #[must_use]
    pub fn failing(err_msg: impl Into<String>) -> Self {
        Self {
            verdict: std::sync::Arc::new(parking_lot::RwLock::new(Err(err_msg.into()))),
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Updates the mock verdict dynamically.
    pub fn set_verdict(&self, verdict: StormStanceVerdict) {
        *self.verdict.write() = Ok(verdict);
    }

    /// Returns total invocations observed.
    #[must_use]
    pub fn call_count(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[async_trait]
impl StormStanceClassifier for MockStormStanceClassifier {
    async fn classify_storm(&self, _input: &StormStanceInput) -> Result<StormStanceVerdict> {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match self.verdict.read().clone() {
            Ok(v) => Ok(v),
            Err(e) => Err(SkydopplerError::Stance(e)),
        }
    }

    fn model_name(&self) -> &str {
        "mock-storm-stance"
    }
}

#[derive(Debug, Deserialize)]
struct OllamaChatResponse {
    message: OllamaMessage,
}

#[derive(Debug, Deserialize)]
struct OllamaMessage {
    content: String,
}

#[derive(Debug, Deserialize)]
struct OllamaStormJson {
    collective_hostility: bool,
    confidence: f64,
    #[serde(default)]
    classification: Option<String>,
    #[serde(default)]
    summary: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JevClassifyResponse {
    violates: bool,
    #[serde(default)]
    category: Option<String>,
    confidence: f64,
    #[serde(default)]
    reason: String,
}

#[derive(Debug, Deserialize)]
struct SystemOneResponse {
    #[serde(default)]
    answers: HashMap<String, SystemOneAnswer>,
}

#[derive(Debug, Deserialize)]
struct SystemOneAnswer {
    choice: String,
    #[serde(default)]
    probabilities: Option<HashMap<String, f64>>,
    #[serde(default)]
    confidence: Option<f64>,
}

/// Production HTTP Stage 3 classifier supporting `Ollama`, `SystemOne`, and `StandardJev`.
#[derive(Debug, Clone)]
pub struct LlmStormStanceClassifier {
    config: StanceConfig,
    http_client: reqwest::Client,
    endpoint_url: String,
    dialect: StanceDialect,
}

impl LlmStormStanceClassifier {
    /// Builds a new [`LlmStormStanceClassifier`] from `config`.
    ///
    /// # Errors
    /// Returns [`SkydopplerError::Config`] if the endpoint URL is invalid or client build fails.
    pub fn new(config: StanceConfig) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .use_rustls_tls()
            .timeout(config.timeout)
            .build()
            .map_err(|e| {
                SkydopplerError::Config(format!("Failed to build stance HTTP client: {e}"))
            })?;

        let base = config.base_url.trim_end_matches('/');
        let (dialect, endpoint_url) = if base.contains(":8000")
            || base.ends_with("/systemone")
            || base.ends_with("/v1/systemone")
        {
            let url = if base.ends_with("/systemone") || base.ends_with("/v1/systemone") {
                base.to_string()
            } else {
                format!("{base}/v1/systemone")
            };
            (StanceDialect::SystemOne, url)
        } else if base.contains(":11434") || base.ends_with("/api/chat") || base.ends_with("/api") {
            let url = if base.ends_with("/api/chat") {
                base.to_string()
            } else if base.ends_with("/api") {
                format!("{base}/chat")
            } else {
                format!("{base}/api/chat")
            };
            (StanceDialect::Ollama, url)
        } else {
            let url = if base.ends_with("/v1/classify") {
                base.to_string()
            } else if base.ends_with("/v1") {
                format!("{base}/classify")
            } else {
                format!("{base}/v1/classify")
            };
            (StanceDialect::StandardJev, url)
        };

        url::Url::parse(&endpoint_url).map_err(|e| {
            SkydopplerError::Config(format!("Invalid stance endpoint URL '{endpoint_url}': {e}"))
        })?;

        Ok(Self {
            config,
            http_client,
            endpoint_url,
            dialect,
        })
    }

    /// Returns the resolved [`StanceDialect`].
    #[must_use]
    pub const fn dialect(&self) -> StanceDialect {
        self.dialect
    }

    /// Returns the resolved HTTP URL.
    #[must_use]
    pub fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }

    /// Formats a compact single-shot classification prompt summarizing focal post, catalysts, and replies.
    #[must_use]
    pub fn format_bundle_prompt(input: &StormStanceInput) -> String {
        use std::fmt::Write;
        let mut out = String::with_capacity(2048);
        let target_label = input
            .target_handle
            .as_deref()
            .filter(|h| !h.trim().is_empty())
            .unwrap_or(&input.target_did);

        let _ = writeln!(
            out,
            "Target: {target_label} (tier={})",
            input.target_tier.as_str()
        );
        if let Some(ref fp) = input.focal_post {
            let snippet: String = fp.text.chars().take(280).collect();
            let _ = writeln!(
                out,
                "Focal Post ({} likes, {} reposts, {} replies, {} quotes): \"{}\"",
                fp.like_count, fp.repost_count, fp.reply_count, fp.quote_count, snippet
            );
        } else {
            let _ = writeln!(out, "Focal URI: {}", input.focal_post_uri);
        }

        let _ = writeln!(
            out,
            "Metrics: oon_authors={}, oon_ratio={:.2}, direct_ratio={:.2}, dunk_ratio={:.2}, power_asymmetry={:.2}",
            input.metrics.unique_oon_authors_short,
            input.metrics.out_of_network_ratio,
            input.metrics.direct_ratio,
            input.metrics.dunk_ratio,
            input.metrics.power_asymmetry
        );

        if !input.top_quotes.is_empty() {
            let _ = writeln!(out, "Top Catalyst Quote-Posts:");
            for qt in input.top_quotes.iter().take(3) {
                let author = qt.author_handle.as_deref().unwrap_or(&qt.author_did);
                let snippet: String = qt.text.chars().take(160).collect();
                let _ = writeln!(out, "- {author} ({} likes): \"{snippet}\"", qt.like_count);
            }
        }

        if !input.sample_replies.is_empty() {
            let _ = writeln!(out, "Sampled Responders:");
            for rep in input.sample_replies.iter().take(15) {
                let snippet: String = rep.text_snippet.chars().take(160).collect();
                let _ = writeln!(out, "- \"{snippet}\"");
            }
        }
        out
    }

    fn parse_category(raw: Option<&str>, hostile: bool) -> StormCategory {
        match raw.and_then(|s| s.parse::<StormCategory>().ok()) {
            Some(c) => c,
            None if hostile => StormCategory::Dogpile,
            None => StormCategory::BenignViral,
        }
    }

    async fn read_bounded_error(resp: reqwest::Response) -> String {
        resp.text()
            .await
            .map(|s| s.chars().take(200).collect())
            .unwrap_or_else(|_| "unreadable body".to_string())
    }
}

#[async_trait]
impl StormStanceClassifier for LlmStormStanceClassifier {
    async fn classify_storm(&self, input: &StormStanceInput) -> Result<StormStanceVerdict> {
        let bundle = Self::format_bundle_prompt(input);
        let system_prompt = "Classify the network crowd reaction into one of: dogpile, public_figure_backlash, heated_debate, benign_viral, uncertain. Return strict JSON with collective_hostility (bool), confidence (0..1), classification, summary.";

        let mut last_err: Option<SkydopplerError> = None;
        for attempt in 0..=self.config.max_retries {
            let mut req = self.http_client.post(&self.endpoint_url);
            if let Some(ref key) = self.config.api_key {
                req = req.bearer_auth(key);
            }

            let req = match self.dialect {
                StanceDialect::Ollama => req.json(&serde_json::json!({
                    "model": self.config.model,
                    "stream": false,
                    "format": "json",
                    "options": { "temperature": 0.0, "num_ctx": self.config.num_ctx },
                    "messages": [
                        { "role": "system", "content": system_prompt },
                        { "role": "user", "content": bundle }
                    ]
                })),
                StanceDialect::StandardJev => req.json(&serde_json::json!({
                    "rules": system_prompt,
                    "content": bundle,
                    "model": self.config.model
                })),
                StanceDialect::SystemOne => req.json(&serde_json::json!({
                    "model": self.config.model,
                    "text": bundle,
                    "questions": {
                        "storm_stance": {
                            "choices": ["dogpile", "public_figure_backlash", "heated_debate", "benign_viral"]
                        }
                    }
                })),
            };

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    last_err = Some(SkydopplerError::Http(e));
                    continue;
                }
            };

            let status = resp.status();
            if status.is_success() {
                let verdict = match self.dialect {
                    StanceDialect::Ollama => {
                        let wrap = resp.json::<OllamaChatResponse>().await.map_err(|e| {
                            SkydopplerError::Stance(format!("Ollama decode error: {e}"))
                        })?;
                        let cleaned = wrap
                            .message
                            .content
                            .trim()
                            .trim_start_matches("```json")
                            .trim_start_matches("```")
                            .trim_end_matches("```")
                            .trim();
                        let parsed =
                            serde_json::from_str::<OllamaStormJson>(cleaned).map_err(|e| {
                                SkydopplerError::Stance(format!(
                                    "Ollama JSON parse error (attempt {attempt}): {e}"
                                ))
                            })?;
                        let cat = Self::parse_category(
                            parsed.classification.as_deref(),
                            parsed.collective_hostility,
                        );
                        StormStanceVerdict::new(
                            parsed.collective_hostility,
                            parsed.confidence,
                            cat,
                            parsed.summary.unwrap_or_default(),
                            self.config.model.clone(),
                        )
                    }
                    StanceDialect::StandardJev => {
                        let parsed = resp.json::<JevClassifyResponse>().await.map_err(|e| {
                            SkydopplerError::Stance(format!("StandardJev decode error: {e}"))
                        })?;
                        let cat = Self::parse_category(parsed.category.as_deref(), parsed.violates);
                        StormStanceVerdict::new(
                            parsed.violates,
                            parsed.confidence,
                            cat,
                            parsed.reason,
                            self.config.model.clone(),
                        )
                    }
                    StanceDialect::SystemOne => {
                        let parsed = resp.json::<SystemOneResponse>().await.map_err(|e| {
                            SkydopplerError::Stance(format!("SystemOne decode error: {e}"))
                        })?;
                        let ans = parsed.answers.get("storm_stance").ok_or_else(|| {
                            SkydopplerError::Stance(
                                "SystemOne response missing 'storm_stance' answer".to_string(),
                            )
                        })?;
                        let conf = ans
                            .probabilities
                            .as_ref()
                            .and_then(|p| p.get(&ans.choice).copied())
                            .or(ans.confidence)
                            .unwrap_or(0.5);
                        let cat = Self::parse_category(Some(&ans.choice), false);
                        let hostile = matches!(
                            cat,
                            StormCategory::Dogpile | StormCategory::PublicFigureBacklash
                        );
                        StormStanceVerdict::new(
                            hostile,
                            conf,
                            cat,
                            format!(
                                "SystemOne classified '{cat}' ({:.0}% confidence)",
                                conf * 100.0
                            ),
                            self.config.model.clone(),
                        )
                    }
                };
                return Ok(verdict);
            }

            if status.is_client_error() {
                let body = Self::read_bounded_error(resp).await;
                return Err(SkydopplerError::Stance(format!(
                    "Stance endpoint client error {}: {body}",
                    status.as_u16()
                )));
            }

            let body = Self::read_bounded_error(resp).await;
            last_err = Some(SkydopplerError::Stance(format!(
                "Stance endpoint server error {}: {body}",
                status.as_u16()
            )));
        }

        Err(last_err.unwrap_or_else(|| {
            SkydopplerError::Stance("Stance classification failed after retries".to_string())
        }))
    }

    fn model_name(&self) -> &str {
        &self.config.model
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use crate::types::InteractionKind;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_input() -> StormStanceInput {
        StormStanceInput {
            target_did: "did:plc:alice".to_string(),
            target_handle: Some("alice.bsky.social".to_string()),
            target_tier: TargetTier::Small,
            focal_post_uri: "at://did:plc:alice/app.bsky.feed.post/3k1".to_string(),
            focal_post: Some(PostEngagementSnapshot {
                uri: "at://did:plc:alice/app.bsky.feed.post/3k1".to_string(),
                author_did: "did:plc:alice".to_string(),
                author_handle: Some("alice.bsky.social".to_string()),
                author_followers: Some(400),
                text: "Hot take".to_string(),
                reply_count: 45,
                repost_count: 1,
                like_count: 2,
                quote_count: 12,
            }),
            top_quotes: vec![PostEngagementSnapshot {
                uri: "at://did:plc:big/app.bsky.feed.post/q1".to_string(),
                author_did: "did:plc:big".to_string(),
                author_handle: Some("big.bsky.social".to_string()),
                author_followers: Some(90_000),
                text: "Look at this".to_string(),
                reply_count: 10,
                repost_count: 40,
                like_count: 600,
                quote_count: 3,
            }],
            sample_replies: vec![SampleReply {
                author_did: "did:plc:r1".to_string(),
                kind: InteractionKind::Reply,
                post_uri: "at://did:plc:r1/app.bsky.feed.post/r1".to_string(),
                text_snippet: "Bad take".to_string(),
            }],
            metrics: StormMetrics {
                unique_oon_authors_short: 25,
                out_of_network_ratio: 0.92,
                direct_ratio: 19.0,
                dunk_ratio: 200.0,
                power_asymmetry: 219.0,
                ..Default::default()
            },
        }
    }

    #[tokio::test]
    async fn noop_mock_and_all_llm_dialects_work_and_fail_safely() {
        let noop = NoopStormStanceClassifier;
        assert_eq!(noop.model_name(), "structural_fallback");
        assert!(noop.classify_storm(&sample_input()).await.is_err());

        let mock = MockStormStanceClassifier::new(StormStanceVerdict::new(
            true,
            0.91,
            StormCategory::Dogpile,
            "Hostile",
            "mock",
        ));
        assert_eq!(mock.model_name(), "mock-storm-stance");
        assert!(
            mock.classify_storm(&sample_input())
                .await
                .unwrap()
                .collective_hostility
        );
        mock.set_verdict(StormStanceVerdict::new(
            false,
            0.95,
            StormCategory::BenignViral,
            "ok",
            "mock",
        ));
        assert!(
            !mock
                .classify_storm(&sample_input())
                .await
                .unwrap()
                .collective_hostility
        );
        assert_eq!(mock.call_count(), 2);
        assert!(MockStormStanceClassifier::failing("down")
            .classify_storm(&sample_input())
            .await
            .is_err());

        // Dialect URL detection
        assert!(LlmStormStanceClassifier::new(StanceConfig {
            base_url: "not a url".to_string(),
            ..Default::default()
        })
        .is_err());
        let o1 = LlmStormStanceClassifier::new(StanceConfig {
            base_url: "http://127.0.0.1:11434/api".to_string(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(o1.dialect(), StanceDialect::Ollama);
        assert_eq!(o1.endpoint_url(), "http://127.0.0.1:11434/api/chat");

        let s1 = LlmStormStanceClassifier::new(StanceConfig {
            base_url: "http://127.0.0.1:8000".to_string(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(s1.dialect(), StanceDialect::SystemOne);
        assert_eq!(s1.endpoint_url(), "http://127.0.0.1:8000/v1/systemone");

        // Ollama live mock
        let ollama = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/chat"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "message": {
                    "content": "```json\n{\"collective_hostility\": true, \"confidence\": 0.91, \"classification\": \"dogpile\", \"summary\": \"Dunking wave\"}\n```"
                }
            })))
            .mount(&ollama)
            .await;
        let ol = LlmStormStanceClassifier::new(StanceConfig {
            base_url: format!("{}/api/chat", ollama.uri()),
            api_key: Some("k".to_string()),
            max_retries: 0,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(ol.model_name(), "qwen3.5:9b");
        let ov = ol.classify_storm(&sample_input()).await.unwrap();
        assert!(ov.collective_hostility);
        assert_eq!(ov.category, StormCategory::Dogpile);

        // SystemOne live mock + missing answer error
        let sys = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "answers": {
                    "storm_stance": {
                        "choice": "public_figure_backlash",
                        "probabilities": { "public_figure_backlash": 0.88 }
                    }
                }
            })))
            .mount(&sys)
            .await;
        let sys_cls = LlmStormStanceClassifier::new(StanceConfig {
            base_url: format!("{}/v1/systemone", sys.uri()),
            ..Default::default()
        })
        .unwrap();
        let sv = sys_cls.classify_storm(&sample_input()).await.unwrap();
        assert!(sv.collective_hostility);
        assert_eq!(sv.category, StormCategory::PublicFigureBacklash);

        // StandardJev + 4xx no-retry + 5xx retry
        let jev = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/classify"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "violates": false,
                "category": "benign_viral",
                "confidence": 0.94,
                "reason": "Wholesome"
            })))
            .mount(&jev)
            .await;
        let jev_cls = LlmStormStanceClassifier::new(StanceConfig {
            base_url: format!("{}/v1", jev.uri()),
            ..Default::default()
        })
        .unwrap();
        let jv = jev_cls.classify_storm(&sample_input()).await.unwrap();
        assert!(!jv.collective_hostility);
        assert_eq!(jv.category, StormCategory::BenignViral);

        let err_srv = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/classify"))
            .respond_with(ResponseTemplate::new(400).set_body_string("bad"))
            .expect(1)
            .mount(&err_srv)
            .await;
        let err_cls = LlmStormStanceClassifier::new(StanceConfig {
            base_url: err_srv.uri(),
            max_retries: 2,
            ..Default::default()
        })
        .unwrap();
        assert!(err_cls.classify_storm(&sample_input()).await.is_err());

        let s5 = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/classify"))
            .respond_with(ResponseTemplate::new(502).set_body_string("down"))
            .expect(2)
            .mount(&s5)
            .await;
        let s5_cls = LlmStormStanceClassifier::new(StanceConfig {
            base_url: s5.uri(),
            max_retries: 1,
            ..Default::default()
        })
        .unwrap();
        assert!(s5_cls.classify_storm(&sample_input()).await.is_err());

        // Minimal prompt without focal_post, handle, quotes, or replies
        let minimal = LlmStormStanceClassifier::format_bundle_prompt(&StormStanceInput {
            target_did: "did:plc:x".to_string(),
            target_handle: None,
            target_tier: TargetTier::Unknown,
            focal_post_uri: "at://did:plc:x/app.bsky.feed.post/1".to_string(),
            focal_post: None,
            top_quotes: vec![],
            sample_replies: vec![],
            metrics: StormMetrics::default(),
        });
        assert!(minimal.contains("did:plc:x"));
    }
}
