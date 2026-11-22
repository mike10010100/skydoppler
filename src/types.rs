//! Core domain types, configuration, and streaming/API event schemas for `skydoppler`.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::SkydopplerError;

/// Default sliding short burst window duration in seconds (10 minutes).
pub const DEFAULT_SHORT_WINDOW_SECS: u64 = 600;
/// Default sliding medium baseline window duration in seconds (30 minutes).
pub const DEFAULT_MEDIUM_WINDOW_SECS: u64 = 1800;
/// Default minimum unique responder DIDs before Stage 2 AppView enrichment triggers.
pub const DEFAULT_MIN_UNIQUE_AUTHORS: usize = 6;
/// Default minimum streaming replies+quotes / (likes+reposts+replies+quotes) ratio trigger.
pub const DEFAULT_MIN_STREAMING_RATIO: f64 = 0.30;
/// Default hard cap on concurrently tracked hot posts across all shards.
pub const DEFAULT_MAX_TRACKED_POSTS: usize = 40_000;
/// Default hard cap on concurrently tracked target authors for defensive distress bursts.
pub const DEFAULT_MAX_TRACKED_AUTHORS: usize = 20_000;
/// Default follower threshold below which (or when unknown) public storm targets are redacted.
pub const DEFAULT_PUBLIC_FOLLOWER_THRESHOLD: u64 = 5_000;

/// Configuration governing `skydoppler` detection windows, thresholds, redaction, and retention.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RadarConfig {
    /// Short rolling burst detection window.
    pub short_window: Duration,
    /// Medium rolling baseline window.
    pub medium_window: Duration,
    /// Minimum distinct authors in `short_window` required to emit a Stage 2 surge candidate.
    pub min_unique_authors: usize,
    /// Minimum defensive distress events (blocks / postgate detaches / threadgates) lowering Stage 1 threshold.
    pub defensive_distress_threshold: usize,
    /// Minimum Stage 1 streaming ratio (`(replies+quotes) / (likes+reposts+replies+quotes)`) when no distress burst.
    pub min_streaming_ratio: f64,
    /// Maximum number of hot focal posts retained in RAM across shards.
    pub max_tracked_posts: usize,
    /// Maximum number of target DIDs retained in the defensive distress table across shards.
    pub max_tracked_authors: usize,
    /// Maximum sampled responder DIDs retained per focal post for `getRelationships` & Stage 3 LLM checks.
    pub max_sample_replies: usize,
    /// Per-post Stage 2 enrichment cooldown before re-evaluating a tracked post.
    pub enrich_cooldown: Duration,
    /// Inactivity TTL before an active storm transitions to `Resolved`.
    pub storm_ttl: Duration,
    /// SQLite retention window for resolved/dismissed storms and historical weather snapshots.
    pub history_retention: Duration,
    /// Minimum Stage 1 structural score required to advance to Stage 2 enrichment.
    pub stage1_trigger_score: f64,
    /// Minimum composite score required when Stage 3 LLM stance is available.
    pub confirmed_score_threshold: f64,
    /// Minimum structural score required when Stage 3 LLM stance is disabled or unavailable.
    pub structural_fallback_threshold: f64,
    /// Follower ceiling under which public views fail closed into redacted radar cells.
    pub public_follower_threshold: u64,
    /// Whether Stage 3 bundled LLM stance verification is enabled.
    pub enable_llm_stance: bool,
    /// Bounded queue capacity between Stage 1 firehose ingestion and Stage 2/3 workers.
    pub eval_queue_capacity: usize,
    /// Maximum concurrent Stage 2/3 enrichment & stance tasks.
    pub eval_concurrency: usize,
    /// Polite AppView request rate ceiling in requests per second.
    pub appview_rps: f64,
}

impl Default for RadarConfig {
    fn default() -> Self {
        Self {
            short_window: Duration::from_secs(DEFAULT_SHORT_WINDOW_SECS),
            medium_window: Duration::from_secs(DEFAULT_MEDIUM_WINDOW_SECS),
            min_unique_authors: DEFAULT_MIN_UNIQUE_AUTHORS,
            defensive_distress_threshold: 2,
            min_streaming_ratio: DEFAULT_MIN_STREAMING_RATIO,
            max_tracked_posts: DEFAULT_MAX_TRACKED_POSTS,
            max_tracked_authors: DEFAULT_MAX_TRACKED_AUTHORS,
            max_sample_replies: 20,
            enrich_cooldown: Duration::from_secs(120),
            storm_ttl: Duration::from_secs(1800),
            history_retention: Duration::from_secs(604_800),
            stage1_trigger_score: 0.35,
            confirmed_score_threshold: 0.56,
            structural_fallback_threshold: 0.65,
            public_follower_threshold: DEFAULT_PUBLIC_FOLLOWER_THRESHOLD,
            enable_llm_stance: false,
            eval_queue_capacity: 512,
            eval_concurrency: 4,
            appview_rps: 4.0,
        }
    }
}

impl RadarConfig {
    /// Loads configuration from `SKYDOPPLER_*` environment variables with safe clamped defaults.
    #[must_use]
    pub fn from_env() -> Self {
        let d = Self::default();
        let short_secs =
            crate::env::parsed_or(&["SKYDOPPLER_SHORT_WINDOW_SECS"], d.short_window.as_secs())
                .max(1);
        let medium_secs = crate::env::parsed_or(
            &["SKYDOPPLER_MEDIUM_WINDOW_SECS"],
            d.medium_window.as_secs(),
        )
        .max(short_secs);

        Self {
            short_window: Duration::from_secs(short_secs),
            medium_window: Duration::from_secs(medium_secs),
            min_unique_authors: crate::env::parsed_or(
                &["SKYDOPPLER_MIN_UNIQUE_AUTHORS"],
                d.min_unique_authors,
            )
            .max(1),
            defensive_distress_threshold: crate::env::parsed_or(
                &["SKYDOPPLER_DISTRESS_THRESHOLD"],
                d.defensive_distress_threshold,
            )
            .max(1),
            min_streaming_ratio: crate::env::parsed_or(
                &["SKYDOPPLER_MIN_STREAMING_RATIO"],
                d.min_streaming_ratio,
            )
            .clamp(0.0, 1.0),
            max_tracked_posts: crate::env::parsed_or(
                &["SKYDOPPLER_MAX_TRACKED_POSTS"],
                d.max_tracked_posts,
            )
            .max(64),
            max_tracked_authors: crate::env::parsed_or(
                &["SKYDOPPLER_MAX_TRACKED_AUTHORS"],
                d.max_tracked_authors,
            )
            .max(64),
            max_sample_replies: crate::env::parsed_or(
                &["SKYDOPPLER_MAX_SAMPLE_REPLIES"],
                d.max_sample_replies,
            )
            .clamp(1, 50),
            enrich_cooldown: Duration::from_secs(crate::env::parsed_or(
                &["SKYDOPPLER_ALERT_COOLDOWN_SECS"],
                d.enrich_cooldown.as_secs(),
            )),
            storm_ttl: Duration::from_secs(
                crate::env::parsed_or(&["SKYDOPPLER_STORM_TTL_SECS"], d.storm_ttl.as_secs()).max(5),
            ),
            history_retention: Duration::from_secs(
                crate::env::parsed_or(
                    &["SKYDOPPLER_HISTORY_RETENTION_SECS"],
                    d.history_retention.as_secs(),
                )
                .max(60),
            ),
            stage1_trigger_score: crate::env::parsed_or(
                &["SKYDOPPLER_STAGE1_TRIGGER_SCORE"],
                d.stage1_trigger_score,
            )
            .clamp(0.0, 1.0),
            confirmed_score_threshold: crate::env::parsed_or(
                &["SKYDOPPLER_CONFIRMED_SCORE_THRESHOLD"],
                d.confirmed_score_threshold,
            )
            .clamp(0.0, 1.0),
            structural_fallback_threshold: crate::env::parsed_or(
                &["SKYDOPPLER_STRUCTURAL_FALLBACK_THRESHOLD"],
                d.structural_fallback_threshold,
            )
            .clamp(0.0, 1.0),
            public_follower_threshold: crate::env::parsed_or(
                &["SKYDOPPLER_PUBLIC_FOLLOWER_THRESHOLD"],
                d.public_follower_threshold,
            ),
            enable_llm_stance: crate::env::bool_or(
                &["SKYDOPPLER_ENABLE_LLM_STANCE"],
                d.enable_llm_stance,
            ),
            eval_queue_capacity: crate::env::parsed_or(
                &["SKYDOPPLER_EVAL_QUEUE_CAPACITY"],
                d.eval_queue_capacity,
            )
            .max(8),
            eval_concurrency: crate::env::parsed_or(
                &["SKYDOPPLER_EVAL_CONCURRENCY"],
                d.eval_concurrency,
            )
            .max(1),
            appview_rps: crate::env::parsed_or(&["SKYDOPPLER_APPVIEW_RPS"], d.appview_rps)
                .clamp(0.2, 50.0),
        }
    }

    /// Sets the short rolling burst window.
    #[must_use]
    pub fn with_short_window(mut self, window: Duration) -> Self {
        self.short_window = window.max(Duration::from_secs(1));
        self
    }

    /// Sets the medium baseline window (clamped to at least `short_window`).
    #[must_use]
    pub fn with_medium_window(mut self, window: Duration) -> Self {
        self.medium_window = window.max(self.short_window);
        self
    }

    /// Sets the minimum unique responders required in Stage 1.
    #[must_use]
    pub fn with_min_unique_authors(mut self, min: usize) -> Self {
        self.min_unique_authors = min.max(1);
        self
    }

    /// Sets hard caps for tracked posts and tracked target authors.
    #[must_use]
    pub fn with_table_caps(mut self, max_posts: usize, max_authors: usize) -> Self {
        self.max_tracked_posts = max_posts.max(16);
        self.max_tracked_authors = max_authors.max(16);
        self
    }

    /// Sets the public follower privacy threshold.
    #[must_use]
    pub fn with_public_follower_threshold(mut self, threshold: u64) -> Self {
        self.public_follower_threshold = threshold;
        self
    }

    /// Enables or disables optional Stage 3 LLM stance verification.
    #[must_use]
    pub fn with_enable_llm_stance(mut self, enabled: bool) -> Self {
        self.enable_llm_stance = enabled;
        self
    }

    /// Sets the Stage 1, confirmed LLM, and structural fallback thresholds.
    #[must_use]
    pub fn with_thresholds(mut self, stage1: f64, confirmed: f64, fallback: f64) -> Self {
        self.stage1_trigger_score = if stage1.is_nan() {
            0.0
        } else {
            stage1.clamp(0.0, 1.0)
        };
        self.confirmed_score_threshold = if confirmed.is_nan() {
            0.0
        } else {
            confirmed.clamp(0.0, 1.0)
        };
        self.structural_fallback_threshold = if fallback.is_nan() {
            0.0
        } else {
            fallback.clamp(0.0, 1.0)
        };
        self
    }
}

/// Intensity tier for a detected network storm cell.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum StormSeverity {
    /// Early elevated surge (`composite_score >= threshold` and `< 0.74`).
    #[default]
    Elevated,
    /// High-intensity dogpile or backlash (`composite_score >= 0.74`).
    High,
    /// Severe coordinated flash swarm (`composite_score >= 0.88`).
    Critical,
}

impl StormSeverity {
    /// Returns lowercase wire identifier.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Elevated => "elevated",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }
}

impl fmt::Display for StormSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for StormSeverity {
    type Err = SkydopplerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "elevated" => Ok(Self::Elevated),
            "high" => Ok(Self::High),
            "critical" => Ok(Self::Critical),
            other => Err(SkydopplerError::Config(format!(
                "Invalid storm severity '{other}'; expected elevated, high, or critical"
            ))),
        }
    }
}

/// Lifecycle state of a tracked network storm cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StormStatus {
    /// Currently surging.
    #[default]
    Active,
    /// Cooled below activity thresholds and closed automatically.
    Resolved,
    /// Manually dismissed by an authenticated operator.
    Dismissed,
}

impl StormStatus {
    /// Returns lowercase wire identifier.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Resolved => "resolved",
            Self::Dismissed => "dismissed",
        }
    }
}

impl fmt::Display for StormStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for StormStatus {
    type Err = SkydopplerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "active" => Ok(Self::Active),
            "resolved" => Ok(Self::Resolved),
            "dismissed" => Ok(Self::Dismissed),
            other => Err(SkydopplerError::Config(format!(
                "Invalid storm status '{other}'; expected active, resolved, or dismissed"
            ))),
        }
    }
}

/// Stance classification taxonomy distinguishing predatory dogpiles from broader backlash or benign virality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StormCategory {
    /// Hostile out-of-network pile-on / mockery against a non-mega account.
    #[default]
    Dogpile,
    /// Widespread critical backlash aimed at a large/public-figure account or institution.
    PublicFigureBacklash,
    /// Two-sided controversy without overwhelming unilateral mockery.
    HeatedDebate,
    /// Positive or playful viral post without hostility.
    BenignViral,
    /// Insufficient or mixed signals.
    Uncertain,
}

impl StormCategory {
    /// Returns lowercase wire slug.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Dogpile => "dogpile",
            Self::PublicFigureBacklash => "public_figure_backlash",
            Self::HeatedDebate => "heated_debate",
            Self::BenignViral => "benign_viral",
            Self::Uncertain => "uncertain",
        }
    }
}

impl fmt::Display for StormCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for StormCategory {
    type Err = SkydopplerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "dogpile" => Ok(Self::Dogpile),
            "public_figure_backlash" => Ok(Self::PublicFigureBacklash),
            "heated_debate" => Ok(Self::HeatedDebate),
            "benign_viral" => Ok(Self::BenignViral),
            "uncertain" => Ok(Self::Uncertain),
            other => Err(SkydopplerError::Config(format!(
                "Invalid storm category '{other}'"
            ))),
        }
    }
}

/// Audience size tier of the target account, used for vulnerability weighting and redaction boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetTier {
    /// Follower count unavailable (`None`). Fails closed as redacted publicly.
    #[default]
    Unknown,
    /// Under 2,000 followers.
    Small,
    /// 2,000 to 24,999 followers.
    Mid,
    /// 25,000 to 249,999 followers.
    Large,
    /// 250,000+ followers (celebrity, brand, official org).
    PublicFigure,
}

impl TargetTier {
    /// Derives [`TargetTier`] from an optional AppView follower count.
    #[must_use]
    pub const fn from_followers(followers: Option<u64>) -> Self {
        match followers {
            None => Self::Unknown,
            Some(f) if f < 2_000 => Self::Small,
            Some(f) if f < 25_000 => Self::Mid,
            Some(f) if f < 250_000 => Self::Large,
            Some(_) => Self::PublicFigure,
        }
    }

    /// Returns lowercase wire slug.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Small => "small",
            Self::Mid => "mid",
            Self::Large => "large",
            Self::PublicFigure => "public_figure",
        }
    }
}

impl fmt::Display for TargetTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TargetTier {
    type Err = SkydopplerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "unknown" => Ok(Self::Unknown),
            "small" => Ok(Self::Small),
            "mid" => Ok(Self::Mid),
            "large" => Ok(Self::Large),
            "public_figure" => Ok(Self::PublicFigure),
            other => Err(SkydopplerError::Config(format!(
                "Invalid target tier '{other}'"
            ))),
        }
    }
}

/// Macro atmospheric condition label for the overall Bluesky network.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WeatherLevel {
    /// Quiet baseline weather (`weather_index < 15`).
    #[default]
    Clear,
    /// Scattered minor chatter (`15 <= weather_index < 35`).
    Breezy,
    /// Active localized dogpiles or debate spikes (`35 <= weather_index < 60`).
    Squall,
    /// Heavy multi-front storm activity (`60 <= weather_index < 80`).
    Storm,
    /// Network-wide severe crowd surge (`weather_index >= 80`).
    Supercell,
}

impl WeatherLevel {
    /// Returns lowercase slug.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Clear => "clear",
            Self::Breezy => "breezy",
            Self::Squall => "squall",
            Self::Storm => "storm",
            Self::Supercell => "supercell",
        }
    }
}

impl fmt::Display for WeatherLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// AppView engagement snapshot for a focal post or amplifying quote-post (`app.bsky.feed.getPosts`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PostEngagementSnapshot {
    /// Canonical AT-URI of the post.
    pub uri: String,
    /// Author DID.
    pub author_did: String,
    /// Resolved author handle, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_handle: Option<String>,
    /// Author follower count, if resolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_followers: Option<u64>,
    /// Plaintext body.
    #[serde(default)]
    pub text: String,
    /// Total AppView reply count.
    #[serde(default)]
    pub reply_count: u64,
    /// Total AppView repost count.
    #[serde(default)]
    pub repost_count: u64,
    /// Total AppView like count.
    #[serde(default)]
    pub like_count: u64,
    /// Total AppView quote count.
    #[serde(default)]
    pub quote_count: u64,
    /// Compact embed kind summary extracted from the post record (e.g. `"external_link"`, `"images"`, `"video"`, `"quote_post"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embed_kind: Option<String>,
}

/// Lightweight profile metadata fetched from `app.bsky.actor.getProfiles`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ActorSummary {
    /// Actor DID.
    pub did: String,
    /// Actor handle, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    /// Display name, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Follower count, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub followers_count: Option<u64>,
    /// Profile bio / description, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Follow relationship bitmask returned by `app.bsky.graph.getRelationships` relative to Target.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RelationshipStatus {
    /// Candidate responder DID.
    pub did: String,
    /// `true` if Target follows the responder (`following` URI present).
    pub target_follows_responder: bool,
    /// `true` if the responder follows Target (`followedBy` URI present).
    pub responder_follows_target: bool,
}

impl RelationshipStatus {
    /// Returns `true` when neither party follows the other (strictly out-of-network).
    #[must_use]
    pub const fn is_out_of_network(&self) -> bool {
        !self.target_follows_responder && !self.responder_follows_target
    }
}

/// Identifying details of the top amplifying quote-post catalyst driving a surge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalystInfo {
    /// Canonical AT-URI of the catalyst quote-post.
    pub post_uri: String,
    /// Author DID of the catalyst post.
    pub author_did: String,
    /// Resolved handle, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_handle: Option<String>,
    /// Catalyst follower count, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_followers: Option<u64>,
    /// Like count on the catalyst post (combining AppView + streaming proxy).
    pub like_count: u64,
    /// Repost count on the catalyst post.
    pub repost_count: u64,
    /// Quote count on the catalyst post.
    pub quote_count: u64,
    /// Ratio of `catalyst_likes / (root_likes + 1)`.
    pub dunk_ratio: f64,
    /// Truncated catalyst text snippet.
    pub text_snippet: String,
}

/// Aggregate Stage 3 stance verification result for a storm bundle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StormStanceVerdict {
    /// Whether the sampled crowd response is predominantly hostile/mocking against Target.
    pub collective_hostility: bool,
    /// Confidence score clamped into `[0.0, 1.0]`.
    pub confidence: f64,
    /// Classified storm category.
    pub category: StormCategory,
    /// Concise summary explaining the crowd reaction.
    pub summary: String,
    /// Evaluator identifier (model name or `"structural_fallback"`).
    pub evaluator: String,
}

impl StormStanceVerdict {
    /// Constructs a [`StormStanceVerdict`], clamping `confidence` into `[0.0, 1.0]`.
    #[must_use]
    pub fn new(
        collective_hostility: bool,
        confidence: f64,
        category: StormCategory,
        summary: impl Into<String>,
        evaluator: impl Into<String>,
    ) -> Self {
        let sanitized = if confidence.is_nan() {
            0.0
        } else {
            confidence.clamp(0.0, 1.0)
        };
        Self {
            collective_hostility,
            confidence: sanitized,
            category,
            summary: summary.into(),
            evaluator: evaluator.into(),
        }
    }
}

/// Quantitative cascade, ratio, asymmetry, and distress telemetry for a storm cell.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct StormMetrics {
    /// Distinct responder DIDs in `short_window`.
    pub unique_authors_short: usize,
    /// Sampled out-of-network responders count (refined in Stage 2 via `getRelationships`).
    pub unique_oon_authors_short: usize,
    /// Replies observed in `short_window`.
    pub reply_count_short: usize,
    /// Quotes observed in `short_window`.
    pub quote_count_short: usize,
    /// Live likes observed on this hot post while tracked in `short_window`.
    pub streaming_likes_short: usize,
    /// Live reposts observed on this hot post while tracked in `short_window`.
    pub streaming_reposts_short: usize,
    /// Out-of-network author fraction in `[0.0, 1.0]`.
    pub out_of_network_ratio: f64,
    /// `(replies + quotes) / (likes + reposts + 1.0)` on the focal post.
    pub direct_ratio: f64,
    /// Maximum `catalyst_likes / (root_likes + 1.0)` among recent quote catalysts.
    pub dunk_ratio: f64,
    /// `catalyst_followers / (target_followers + 10.0)` (`1.0` default when unknown).
    pub power_asymmetry: f64,
    /// Poisson-normalized burst acceleration z-score (`short_window` vs. `medium_window`).
    pub burst_z_score: f64,
    /// Target's defensive `block` mutations in `short_window`.
    pub defensive_blocks: usize,
    /// Target's defensive `postgate` detach deltas in `short_window`.
    pub defensive_detaches: usize,
    /// Target's defensive `threadgate` locks in `short_window`.
    pub defensive_threadgates: usize,
    /// Vulnerability multiplier derived from [`TargetTier`].
    pub vulnerability_weight: f64,
    /// Stage 1+2 structural score (`[0.0, 1.0]`).
    pub structural_score: f64,
    /// Final composite score combining structural + Stage 3 stance (`[0.0, 1.0]`).
    pub composite_score: f64,
}

/// Interaction type observed on the Jetstream firehose toward a focal post.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionKind {
    /// Direct or thread reply referencing Target's post.
    Reply,
    /// Quote-post embedding Target's post.
    Quote,
}

/// Bounded reply/quote sample retained for `getRelationships` OON checking and Stage 3 stance prompts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleReply {
    /// Responder author DID.
    pub author_did: String,
    /// Reply vs. quote kind.
    pub kind: InteractionKind,
    /// Post AT-URI of the response.
    pub post_uri: String,
    /// Truncated plaintext snippet.
    pub text_snippet: String,
}

/// Stage 1 snapshot emitted when a hot post clears streaming velocity/ratio/distress triggers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NetworkSurgeCandidate {
    /// Author DID of the focal post.
    pub target_did: String,
    /// Canonical AT-URI of the surging focal post.
    pub focal_post_uri: String,
    /// Microsecond Unix timestamp of the triggering commit.
    pub observed_at_us: u64,
    /// Stage 1 structural metrics prior to Stage 2/3 enrichment.
    pub initial_metrics: StormMetrics,
    /// Bounded sample of responders for OON `getRelationships` & LLM stance verification.
    pub sample_replies: Vec<SampleReply>,
    /// Recent quote-post AT-URIs referencing `focal_post_uri` (capped, newest first).
    pub recent_quote_uris: Vec<String>,
}

/// Persisted canonical network storm cell surfaced to operators, SQLite, and redacted public views.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NetworkStorm {
    /// Database row identifier (`0` before persistence).
    pub id: i64,
    /// Deterministic opaque radar cell identifier (`cell-<hex>`).
    pub cell_id: String,
    /// Target post author DID.
    pub target_did: String,
    /// Target resolved handle, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_handle: Option<String>,
    /// Target follower count resolved via AppView `getProfiles`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_followers: Option<u64>,
    /// Derived audience size tier.
    pub target_tier: TargetTier,
    /// Canonical AT-URI of the focal post.
    pub focal_post_uri: String,
    /// Truncated snippet of the focal post text.
    pub focal_post_snippet: String,
    /// Storm lifecycle status.
    pub status: StormStatus,
    /// Severity tier (`Elevated`, `High`, `Critical`).
    pub severity: StormSeverity,
    /// Category (`Dogpile`, `PublicFigureBacklash`, `HeatedDebate`, etc.).
    pub category: StormCategory,
    /// Peak composite score observed across the storm lifetime.
    pub peak_score: f64,
    /// Radar polar angle in `[0.0, 360.0)`.
    pub radar_angle_deg: f64,
    /// Radar radial distance in `[0.15, 0.95]` (`0` = center core).
    pub radar_radius: f64,
    /// Microsecond Unix timestamp when the storm was first detected.
    pub started_at_us: u64,
    /// Microsecond Unix timestamp of the latest update.
    pub updated_at_us: u64,
    /// Microsecond Unix timestamp when resolved or dismissed, if closed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at_us: Option<u64>,
    /// Latest computed metrics snapshot.
    pub metrics: StormMetrics,
    /// Highest-amplification quote-post catalyst, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_catalyst: Option<CatalystInfo>,
    /// Stage 3 stance verdict, if evaluated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stance: Option<StormStanceVerdict>,
}

/// Network-wide Bluesky atmospheric weather summary computed from active storms and stream throughput.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WeatherSummary {
    /// Timestamp when this summary was generated (microseconds).
    pub timestamp_us: u64,
    /// Composite network weather index in `[0.0, 100.0]`.
    pub weather_index: f64,
    /// Atmospheric severity classification (`Clear` .. `Supercell`).
    pub level: WeatherLevel,
    /// Total currently active storm cells.
    pub active_storms: usize,
    /// Active `Critical` storm cells.
    pub critical_storms: usize,
    /// Active `High` storm cells.
    pub high_storms: usize,
    /// Active `Elevated` storm cells.
    pub elevated_storms: usize,
    /// Active storms targeting protected small accounts (`target_tier == Small | Unknown`).
    pub small_target_storms: usize,
    /// Active storms classified as `PublicFigureBacklash`.
    pub public_figure_backlashes: usize,
    /// Number of hot focal posts currently in RAM windows.
    pub tracked_posts: usize,
    /// Observed firehose commits per second over the recent sampling interval.
    pub commits_per_sec: f64,
}

/// Historical weather data point persisted periodically to SQLite for dashboard sparklines.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WeatherPoint {
    /// Timestamp in microseconds.
    pub timestamp_us: u64,
    /// Composite weather index in `[0.0, 100.0]`.
    pub weather_index: f64,
    /// Active storm count at snapshot time.
    pub active_storms: usize,
    /// Observed commits/sec at snapshot time.
    pub commits_per_sec: f64,
}

/// Internal broadcast event emitted whenever storm state or weather changes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RadarEvent {
    /// A storm was detected, updated, or escalated.
    StormUpsert {
        /// Full storm payload (redacted before sending to non-operator SSE subscribers).
        storm: Box<NetworkStorm>,
        /// `true` if severity escalated.
        is_escalation: bool,
    },
    /// A storm closed (`Resolved` or `Dismissed`).
    StormClosed {
        /// Database storm row ID.
        storm_id: i64,
        /// Opaque radar cell identifier.
        cell_id: String,
        /// Final status.
        status: StormStatus,
    },
    /// Periodic weather pulse update.
    WeatherPulse {
        /// Current macro weather summary.
        weather: WeatherSummary,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn radar_config_builders_and_clamping() {
        let cfg = RadarConfig::from_env()
            .with_short_window(Duration::from_secs(120))
            .with_medium_window(Duration::from_secs(60))
            .with_min_unique_authors(0)
            .with_table_caps(5, 4)
            .with_public_follower_threshold(10_000)
            .with_enable_llm_stance(true)
            .with_thresholds(f64::NAN, 1.5, -0.2);

        assert_eq!(cfg.short_window, Duration::from_secs(120));
        // medium_window clamped to at least short_window
        assert_eq!(cfg.medium_window, Duration::from_secs(120));
        assert_eq!(cfg.min_unique_authors, 1);
        assert_eq!(cfg.max_tracked_posts, 16);
        assert_eq!(cfg.max_tracked_authors, 16);
        assert_eq!(cfg.public_follower_threshold, 10_000);
        assert!(cfg.enable_llm_stance);
        assert!((cfg.stage1_trigger_score - 0.0).abs() < 1e-9);
        assert!((cfg.confirmed_score_threshold - 1.0).abs() < 1e-9);
        assert!((cfg.structural_fallback_threshold - 0.0).abs() < 1e-9);
    }

    #[test]
    fn enums_parse_display_and_tier_classification() {
        assert_eq!(
            "critical".parse::<StormSeverity>().unwrap(),
            StormSeverity::Critical
        );
        assert!("invalid".parse::<StormSeverity>().is_err());
        assert_eq!(StormSeverity::High.to_string(), "high");

        assert_eq!(
            "dismissed".parse::<StormStatus>().unwrap(),
            StormStatus::Dismissed
        );
        assert!("bogus".parse::<StormStatus>().is_err());
        assert_eq!(StormStatus::Resolved.to_string(), "resolved");

        assert_eq!(
            "public_figure_backlash".parse::<StormCategory>().unwrap(),
            StormCategory::PublicFigureBacklash
        );
        assert!("unknown_cat".parse::<StormCategory>().is_err());
        assert_eq!(StormCategory::Dogpile.to_string(), "dogpile");

        assert_eq!(TargetTier::from_followers(None), TargetTier::Unknown);
        assert_eq!(TargetTier::from_followers(Some(500)), TargetTier::Small);
        assert_eq!(TargetTier::from_followers(Some(5_000)), TargetTier::Mid);
        assert_eq!(TargetTier::from_followers(Some(50_000)), TargetTier::Large);
        assert_eq!(
            TargetTier::from_followers(Some(300_000)),
            TargetTier::PublicFigure
        );
        assert_eq!(
            "public_figure".parse::<TargetTier>().unwrap(),
            TargetTier::PublicFigure
        );
        assert!("invalid".parse::<TargetTier>().is_err());

        assert_eq!(WeatherLevel::Supercell.to_string(), "supercell");

        let r_oon = RelationshipStatus {
            did: "did:plc:r".to_string(),
            target_follows_responder: false,
            responder_follows_target: false,
        };
        assert!(r_oon.is_out_of_network());
        let r_in = RelationshipStatus {
            responder_follows_target: true,
            ..r_oon
        };
        assert!(!r_in.is_out_of_network());
    }

    #[test]
    fn stance_verdict_sanitizes_confidence() {
        let v_nan = StormStanceVerdict::new(true, f64::NAN, StormCategory::Dogpile, "s", "e");
        assert!((v_nan.confidence - 0.0).abs() < 1e-9);
        let v_hi = StormStanceVerdict::new(true, 1.8, StormCategory::Dogpile, "s", "e");
        assert!((v_hi.confidence - 1.0).abs() < 1e-9);
    }
}
