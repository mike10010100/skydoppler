//! Real-time streaming radar engine coordinating Stage 1 ingestion, Stage 2 AppView enrichment,
//! Stage 3 LLM stance verification, weather computation, SQLite projection, and live SSE fanout.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use skybase::ingest::events::JetstreamCommit;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::enricher::{NetworkEnricher, PublicAppViewEnricher};
use crate::error::Result;
use crate::math::{
    classify_weather_level, compute_composite_score, compute_direct_ratio, compute_dunk_ratio,
    compute_power_asymmetry, compute_structural_score, compute_vulnerability_weight,
    compute_weather_index, radar_blip_coordinates, severity_from_score,
};
use crate::redact::opaque_cell_id;
use crate::stance::{NoopStormStanceClassifier, StormStanceClassifier, StormStanceInput};
use crate::storage::StormStore;
use crate::time::{current_time_us, duration_to_us};
use crate::tracker::{NetworkTracker, ObserveOutcome};
use crate::types::{
    CatalystInfo, NetworkStorm, NetworkSurgeCandidate, PostEngagementSnapshot, RadarConfig,
    RadarEvent, StormCategory, StormStanceVerdict, StormStatus, TargetTier, WeatherPoint,
    WeatherSummary,
};

const STATS_META_KEY: &str = "engine_stats_v1";
const MIN_CATALYST_ENGAGEMENT: u64 = 5;
const MIN_CATALYST_DUNK_RATIO: f64 = 0.50;
const MIN_STANCE_STRUCTURAL_SCORE: f64 = 0.30;
const MIN_STANCE_OON_RATIO: f64 = 0.25;
const MIN_STANCE_DIRECT_RATIO: f64 = 0.60;
const MIN_STANCE_DUNK_RATIO: f64 = 1.0;
const BENIGN_VIRAL_VETO_CONFIDENCE: f64 = 0.60;

/// Thread-safe atomic counters tracking firehose throughput and radar detections.
#[derive(Debug, Default)]
pub struct EngineStats {
    /// Total Jetstream commits received by the engine loop.
    pub commits_received: AtomicU64,
    /// Post interactions (`app.bsky.feed.post` Create) processed by Stage 1.
    pub posts_observed: AtomicU64,
    /// Likes/reposts matching an already-tracked hot post or catalyst QT.
    pub hot_likes_observed: AtomicU64,
    /// Likes/reposts ignored cleanly on the cold zero-allocation path.
    pub cold_likes_ignored: AtomicU64,
    /// Target defensive `block` / `postgate` / `threadgate` commits observed.
    pub defensive_signals: AtomicU64,
    /// Stage 1 surge candidates forwarded for Stage 2 enrichment.
    pub stage1_candidates: AtomicU64,
    /// Stage 1 candidates shed due to bounded channel backpressure.
    pub stage1_queue_shed: AtomicU64,
    /// Stage 2 AppView enrichment bundles completed.
    pub stage2_enrichments: AtomicU64,
    /// Stage 3 LLM stance invocations completed.
    pub stage3_stance_calls: AtomicU64,
    /// Confirmed storm cells persisted to storage.
    pub storms_detected: AtomicU64,
    /// Escalations emitted on previously active storms.
    pub storms_escalated: AtomicU64,
    /// Surges suppressed cleanly after Stage 2/3 verification (e.g. benign viral).
    pub storms_suppressed_benign: AtomicU64,
    /// Operational errors encountered during enrichment, stance, or persistence.
    pub errors_encountered: AtomicU64,
}

/// Serializable point-in-time snapshot of [`EngineStats`].
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EngineStatsSnapshot {
    /// Total Jetstream commits received.
    pub commits_received: u64,
    /// Post interactions observed.
    pub posts_observed: u64,
    /// Hot likes/reposts observed.
    pub hot_likes_observed: u64,
    /// Cold likes/reposts ignored without allocation.
    pub cold_likes_ignored: u64,
    /// Defensive signals observed.
    pub defensive_signals: u64,
    /// Stage 1 candidates emitted.
    pub stage1_candidates: u64,
    /// Stage 1 candidates shed due to backpressure.
    pub stage1_queue_shed: u64,
    /// Stage 2 enrichments executed.
    pub stage2_enrichments: u64,
    /// Stage 3 LLM stance calls executed.
    pub stage3_stance_calls: u64,
    /// Confirmed storms detected.
    pub storms_detected: u64,
    /// Storm escalations observed.
    pub storms_escalated: u64,
    /// Benign surges suppressed.
    pub storms_suppressed_benign: u64,
    /// Operational errors encountered.
    pub errors_encountered: u64,
}

impl EngineStats {
    /// Captures a point-in-time [`EngineStatsSnapshot`].
    #[must_use]
    pub fn snapshot(&self) -> EngineStatsSnapshot {
        EngineStatsSnapshot {
            commits_received: self.commits_received.load(Ordering::Relaxed),
            posts_observed: self.posts_observed.load(Ordering::Relaxed),
            hot_likes_observed: self.hot_likes_observed.load(Ordering::Relaxed),
            cold_likes_ignored: self.cold_likes_ignored.load(Ordering::Relaxed),
            defensive_signals: self.defensive_signals.load(Ordering::Relaxed),
            stage1_candidates: self.stage1_candidates.load(Ordering::Relaxed),
            stage1_queue_shed: self.stage1_queue_shed.load(Ordering::Relaxed),
            stage2_enrichments: self.stage2_enrichments.load(Ordering::Relaxed),
            stage3_stance_calls: self.stage3_stance_calls.load(Ordering::Relaxed),
            storms_detected: self.storms_detected.load(Ordering::Relaxed),
            storms_escalated: self.storms_escalated.load(Ordering::Relaxed),
            storms_suppressed_benign: self.storms_suppressed_benign.load(Ordering::Relaxed),
            errors_encountered: self.errors_encountered.load(Ordering::Relaxed),
        }
    }

    fn seed_from(&self, snap: &EngineStatsSnapshot) {
        self.commits_received
            .store(snap.commits_received, Ordering::Relaxed);
        self.posts_observed
            .store(snap.posts_observed, Ordering::Relaxed);
        self.hot_likes_observed
            .store(snap.hot_likes_observed, Ordering::Relaxed);
        self.cold_likes_ignored
            .store(snap.cold_likes_ignored, Ordering::Relaxed);
        self.defensive_signals
            .store(snap.defensive_signals, Ordering::Relaxed);
        self.stage1_candidates
            .store(snap.stage1_candidates, Ordering::Relaxed);
        self.stage1_queue_shed
            .store(snap.stage1_queue_shed, Ordering::Relaxed);
        self.stage2_enrichments
            .store(snap.stage2_enrichments, Ordering::Relaxed);
        self.stage3_stance_calls
            .store(snap.stage3_stance_calls, Ordering::Relaxed);
        self.storms_detected
            .store(snap.storms_detected, Ordering::Relaxed);
        self.storms_escalated
            .store(snap.storms_escalated, Ordering::Relaxed);
        self.storms_suppressed_benign
            .store(snap.storms_suppressed_benign, Ordering::Relaxed);
        self.errors_encountered
            .store(snap.errors_encountered, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct ThroughputMeter {
    last_instant: Instant,
    last_commits: u64,
    commits_per_sec: f64,
}

impl Default for ThroughputMeter {
    fn default() -> Self {
        Self {
            last_instant: Instant::now(),
            last_commits: 0,
            commits_per_sec: 0.0,
        }
    }
}

/// Core streaming SkyDoppler radar engine.
#[derive(Clone)]
pub struct SkydopplerEngine {
    config: RadarConfig,
    tracker: Arc<NetworkTracker>,
    enricher: Arc<dyn NetworkEnricher>,
    stance: Arc<dyn StormStanceClassifier>,
    store: Arc<StormStore>,
    stats: Arc<EngineStats>,
    events_tx: broadcast::Sender<RadarEvent>,
    throughput: Arc<Mutex<ThroughputMeter>>,
    latest_cursor_us: Arc<AtomicU64>,
}

/// Ergonomic builder for constructing a [`SkydopplerEngine`].
pub struct SkydopplerEngineBuilder {
    config: RadarConfig,
    enricher: Option<Arc<dyn NetworkEnricher>>,
    stance: Option<Arc<dyn StormStanceClassifier>>,
    store: Option<Arc<StormStore>>,
    appview_endpoint: String,
}

impl SkydopplerEngineBuilder {
    /// Starts building an engine with `config`.
    #[must_use]
    pub fn new(config: RadarConfig) -> Self {
        let appview_endpoint = crate::env::var_or(
            &["SKYDOPPLER_APPVIEW_URL"],
            skybase::appview::DEFAULT_APPVIEW_ENDPOINT,
        );
        Self {
            config,
            enricher: None,
            stance: None,
            store: None,
            appview_endpoint,
        }
    }

    /// Configures an explicit [`NetworkEnricher`].
    #[must_use]
    pub fn with_enricher(mut self, enricher: Arc<dyn NetworkEnricher>) -> Self {
        self.enricher = Some(enricher);
        self
    }

    /// Configures an explicit Stage 3 [`StormStanceClassifier`].
    #[must_use]
    pub fn with_stance_classifier(mut self, stance: Arc<dyn StormStanceClassifier>) -> Self {
        self.stance = Some(stance);
        self
    }

    /// Configures an explicit [`StormStore`].
    #[must_use]
    pub fn with_store(mut self, store: Arc<StormStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Configures a custom AppView base URL for the default enricher.
    #[must_use]
    pub fn with_appview_endpoint(mut self, url: impl Into<String>) -> Self {
        self.appview_endpoint = url.into();
        self
    }

    /// Builds the [`SkydopplerEngine`], restoring persisted stats/cursor if present.
    ///
    /// # Errors
    /// Returns [`SkydopplerError`] if store initialization fails.
    pub fn build(self) -> Result<SkydopplerEngine> {
        let store = match self.store {
            Some(s) => s,
            None => Arc::new(StormStore::open_in_memory()?),
        };
        let enricher: Arc<dyn NetworkEnricher> = self.enricher.unwrap_or_else(|| {
            Arc::new(PublicAppViewEnricher::new(
                self.appview_endpoint,
                self.config.appview_rps,
            ))
        });
        let stance: Arc<dyn StormStanceClassifier> = self
            .stance
            .unwrap_or_else(|| Arc::new(NoopStormStanceClassifier));

        let stats = Arc::new(EngineStats::default());
        if let Ok(Some(json)) = store.load_meta_json(STATS_META_KEY) {
            if let Ok(snap) = serde_json::from_str::<EngineStatsSnapshot>(&json) {
                stats.seed_from(&snap);
            }
        }
        let cursor = store.load_cursor_us().ok().flatten().unwrap_or(0);
        let (events_tx, _) = broadcast::channel(512);

        let initial_commits = stats.commits_received.load(Ordering::Relaxed);
        Ok(SkydopplerEngine {
            tracker: Arc::new(NetworkTracker::new(self.config.clone())),
            config: self.config,
            enricher,
            stance,
            store,
            stats,
            events_tx,
            throughput: Arc::new(Mutex::new(ThroughputMeter {
                last_commits: initial_commits,
                ..Default::default()
            })),
            latest_cursor_us: Arc::new(AtomicU64::new(cursor)),
        })
    }
}

impl SkydopplerEngine {
    /// Returns the engine configuration.
    #[must_use]
    pub const fn config(&self) -> &RadarConfig {
        &self.config
    }

    /// Returns shared engine statistics handle.
    #[must_use]
    pub fn stats(&self) -> &Arc<EngineStats> {
        &self.stats
    }

    /// Returns the underlying SQLite [`StormStore`].
    #[must_use]
    pub fn store(&self) -> &Arc<StormStore> {
        &self.store
    }

    /// Returns the rolling 16-shard [`NetworkTracker`].
    #[must_use]
    pub fn tracker(&self) -> &Arc<NetworkTracker> {
        &self.tracker
    }

    /// Returns the latest persisted or seen Jetstream cursor timestamp.
    #[must_use]
    pub fn latest_cursor_us(&self) -> u64 {
        self.latest_cursor_us.load(Ordering::Relaxed)
    }

    /// Subscribes to real-time [`RadarEvent`] notifications (bounded ring buffer).
    #[must_use]
    pub fn subscribe_events(&self) -> broadcast::Receiver<RadarEvent> {
        self.events_tx.subscribe()
    }

    /// Runs the firehose commit consumer loop + Stage 2/3 worker pool until `cancel` or channel close.
    ///
    /// # Errors
    /// Returns [`SkydopplerError`] on unrecoverable shutdown failure.
    pub async fn run(
        &self,
        mut commit_rx: mpsc::Receiver<JetstreamCommit>,
        cancel: CancellationToken,
    ) -> Result<EngineStatsSnapshot> {
        let (cand_tx, mut cand_rx) =
            mpsc::channel::<NetworkSurgeCandidate>(self.config.eval_queue_capacity);

        let mut workers = JoinSet::new();
        let worker_eng = self.clone();
        let sem = Arc::new(Semaphore::new(self.config.eval_concurrency.max(1)));
        let worker_loop = async move {
            let mut active = JoinSet::new();
            while let Ok(permit) = sem.clone().acquire_owned().await {
                match cand_rx.recv().await {
                    Some(cand) => {
                        let eng = worker_eng.clone();
                        active.spawn(async move {
                            let _permit = permit;
                            if let Err(e) = eng.evaluate_candidate(cand).await {
                                eng.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
                                warn!(error = %e, "Stage 2/3 storm evaluation failed");
                            }
                        });
                    }
                    None => {
                        drop(permit);
                        break;
                    }
                }
                while let Some(res) = active.try_join_next() {
                    let _ = res;
                }
            }
            while let Some(res) = active.join_next().await {
                let _ = res;
            }
        };
        workers.spawn(worker_loop);

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    while let Ok(commit) = commit_rx.try_recv() {
                        self.handle_commit(&commit, &cand_tx);
                    }
                    break;
                }
                maybe_commit = commit_rx.recv() => {
                    match maybe_commit {
                        Some(commit) => self.handle_commit(&commit, &cand_tx),
                        None => break,
                    }
                }
            }
        }

        drop(cand_tx);
        let drain = async {
            while let Some(res) = workers.join_next().await {
                let _ = res;
            }
        };
        if tokio::time::timeout(Duration::from_secs(5), drain)
            .await
            .is_err()
        {
            workers.abort_all();
            while workers.join_next().await.is_some() {}
        }

        let _ = self.persist_state();
        Ok(self.stats.snapshot())
    }

    fn handle_commit(
        &self,
        commit: &JetstreamCommit,
        cand_tx: &mpsc::Sender<NetworkSurgeCandidate>,
    ) {
        self.stats.commits_received.fetch_add(1, Ordering::Relaxed);
        if commit.time_us > 0 {
            self.latest_cursor_us
                .fetch_max(commit.time_us, Ordering::Relaxed);
        }

        let candidate = match self.tracker.observe_commit(commit) {
            ObserveOutcome::Ignored => {
                if matches!(
                    commit.collection.as_str(),
                    "app.bsky.feed.like" | "app.bsky.feed.repost"
                ) {
                    self.stats
                        .cold_likes_ignored
                        .fetch_add(1, Ordering::Relaxed);
                }
                None
            }
            ObserveOutcome::PostInteraction(maybe_c) => {
                self.stats.posts_observed.fetch_add(1, Ordering::Relaxed);
                maybe_c
            }
            ObserveOutcome::HotEngagementUpdated => {
                self.stats
                    .hot_likes_observed
                    .fetch_add(1, Ordering::Relaxed);
                None
            }
            ObserveOutcome::DefensiveSignal(maybe_c) => {
                self.stats.defensive_signals.fetch_add(1, Ordering::Relaxed);
                maybe_c
            }
        };

        if let Some(c) = candidate {
            self.stats.stage1_candidates.fetch_add(1, Ordering::Relaxed);
            if cand_tx.try_send(c).is_err() {
                self.stats.stage1_queue_shed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Evaluates a Stage 1 [`NetworkSurgeCandidate`] through batched AppView enrichment,
    /// optional LLM stance verification, SQLite persistence, and SSE broadcast.
    ///
    /// Returns `Ok(Some(NetworkStorm))` when confirmed, or `Ok(None)` when suppressed.
    ///
    /// # Errors
    /// Returns [`SkydopplerError`] if database persistence fails.
    pub async fn evaluate_candidate(
        &self,
        candidate: NetworkSurgeCandidate,
    ) -> Result<Option<NetworkStorm>> {
        self.stats
            .stage2_enrichments
            .fetch_add(1, Ordering::Relaxed);

        // 1. Batched getPosts (focal + recent quote catalysts)
        let mut fetch_uris = Vec::with_capacity(1 + candidate.recent_quote_uris.len());
        fetch_uris.push(candidate.focal_post_uri.clone());
        for qt in &candidate.recent_quote_uris {
            if !fetch_uris.contains(qt) {
                fetch_uris.push(qt.clone());
            }
        }
        let posts = self.enricher.fetch_post_engagements(&fetch_uris).await;

        let mut focal_post: Option<PostEngagementSnapshot> = None;
        let mut quote_snaps: Vec<PostEngagementSnapshot> = Vec::new();
        for p in posts {
            if p.uri == candidate.focal_post_uri {
                focal_post = Some(p);
            } else {
                quote_snaps.push(p);
            }
        }
        if focal_post.as_ref().is_some_and(|f| f.quote_count > 0)
            || !candidate.recent_quote_uris.is_empty()
        {
            let fetched_quotes = self
                .enricher
                .fetch_post_quotes(&candidate.focal_post_uri, 25)
                .await;
            for qt in fetched_quotes {
                if qt.uri != candidate.focal_post_uri
                    && !quote_snaps.iter().any(|existing| existing.uri == qt.uri)
                {
                    quote_snaps.push(qt);
                }
            }
        }
        quote_snaps.sort_by(|a, b| {
            let sa = a
                .like_count
                .saturating_add(a.repost_count.saturating_mul(2));
            let sb = b
                .like_count
                .saturating_add(b.repost_count.saturating_mul(2));
            sb.cmp(&sa)
        });

        // 2. Batched getProfiles (target + top engaged dunk catalyst author)
        let root_likes = focal_post.as_ref().map_or(0, |f| f.like_count);
        let engaged_top_qt = quote_snaps.first().filter(|qt| {
            qt.like_count.saturating_add(qt.repost_count) >= MIN_CATALYST_ENGAGEMENT
                && compute_dunk_ratio(qt.like_count, root_likes) >= MIN_CATALYST_DUNK_RATIO
        });
        let mut profile_dids = vec![candidate.target_did.clone()];
        if let Some(top_qt) = engaged_top_qt {
            if !top_qt.author_did.is_empty() && !profile_dids.contains(&top_qt.author_did) {
                profile_dids.push(top_qt.author_did.clone());
            }
        }
        let profiles = self.enricher.fetch_actor_profiles(&profile_dids).await;
        let target_profile = profiles.iter().find(|p| p.did == candidate.target_did);
        let target_followers = target_profile
            .and_then(|p| p.followers_count)
            .or_else(|| focal_post.as_ref().and_then(|f| f.author_followers));
        let target_handle = target_profile
            .and_then(|p| p.handle.clone())
            .or_else(|| focal_post.as_ref().and_then(|f| f.author_handle.clone()));
        let target_display_name = target_profile.and_then(|p| p.display_name.clone());
        let target_bio = target_profile.and_then(|p| p.description.clone());
        let target_tier = TargetTier::from_followers(target_followers);

        // 3. On-demand pairwise out-of-network ratio via getRelationships (no global graph needed!)
        let sample_dids: Vec<String> = candidate
            .sample_replies
            .iter()
            .map(|r| r.author_did.clone())
            .take(30)
            .collect();
        let rels = self
            .enricher
            .fetch_relationships(&candidate.target_did, &sample_dids)
            .await;

        let mut metrics = candidate.initial_metrics.clone();
        if !rels.is_empty() {
            let oon_sampled = rels.iter().filter(|r| r.is_out_of_network()).count();
            let ratio = (oon_sampled as f64) / (rels.len() as f64);
            metrics.out_of_network_ratio = ratio;
            metrics.unique_oon_authors_short =
                ((metrics.unique_authors_short as f64) * ratio).round() as usize;
        }

        if let Some(ref fp) = focal_post {
            metrics.direct_ratio = compute_direct_ratio(
                fp.reply_count,
                fp.quote_count,
                fp.like_count,
                fp.repost_count,
            );
        }

        let top_catalyst = if let Some(top_qt) = engaged_top_qt {
            let cat_followers = profiles
                .iter()
                .find(|p| p.did == top_qt.author_did)
                .and_then(|p| p.followers_count)
                .or(top_qt.author_followers);
            let dunk = compute_dunk_ratio(top_qt.like_count, root_likes);
            let asym = compute_power_asymmetry(cat_followers, target_followers);
            metrics.dunk_ratio = dunk;
            metrics.power_asymmetry = asym;
            Some(CatalystInfo {
                post_uri: top_qt.uri.clone(),
                author_did: top_qt.author_did.clone(),
                author_handle: top_qt.author_handle.clone(),
                author_followers: cat_followers,
                like_count: top_qt.like_count,
                repost_count: top_qt.repost_count,
                quote_count: top_qt.quote_count,
                dunk_ratio: dunk,
                text_snippet: top_qt.text.chars().take(180).collect(),
            })
        } else {
            metrics.dunk_ratio = 0.0;
            metrics.power_asymmetry = 1.0;
            None
        };

        let distress_total = metrics
            .defensive_blocks
            .saturating_add(metrics.defensive_detaches)
            .saturating_add(metrics.defensive_threadgates);
        metrics.vulnerability_weight = compute_vulnerability_weight(target_tier);
        metrics.structural_score = compute_structural_score(
            metrics.unique_oon_authors_short,
            metrics.out_of_network_ratio,
            metrics.direct_ratio,
            metrics.dunk_ratio,
            metrics.power_asymmetry,
            metrics.burst_z_score,
            distress_total,
            target_tier,
        );

        let min_oon_authors = match target_tier {
            TargetTier::Large | TargetTier::PublicFigure => self.config.min_unique_authors,
            TargetTier::Small | TargetTier::Mid | TargetTier::Unknown => {
                (self.config.min_unique_authors / 2).max(2)
            }
        };
        let has_distress = distress_total >= self.config.defensive_distress_threshold;
        let has_sufficient_oon = metrics.unique_oon_authors_short >= min_oon_authors
            && metrics.out_of_network_ratio >= MIN_STANCE_OON_RATIO;
        let has_hostile_signal = metrics.direct_ratio >= MIN_STANCE_DIRECT_RATIO
            || metrics.dunk_ratio >= MIN_STANCE_DUNK_RATIO
            || has_distress;
        let qualifies_for_stance = metrics.structural_score >= MIN_STANCE_STRUCTURAL_SCORE
            && (has_sufficient_oon || has_distress)
            && has_hostile_signal;

        // 4. Optional Stage 3 bundled LLM stance check
        let stance = if self.config.enable_llm_stance && qualifies_for_stance {
            self.stats
                .stage3_stance_calls
                .fetch_add(1, Ordering::Relaxed);
            let input = StormStanceInput {
                target_did: candidate.target_did.clone(),
                target_handle: target_handle.clone(),
                target_display_name,
                target_bio,
                target_tier,
                focal_post_uri: candidate.focal_post_uri.clone(),
                focal_post: focal_post.clone(),
                top_quotes: quote_snaps.into_iter().take(3).collect(),
                sample_replies: candidate.sample_replies.clone(),
                metrics: metrics.clone(),
            };
            match self.stance.classify_storm(&input).await {
                Ok(v) => Some(v),
                Err(e) => {
                    debug!(error = %e, "Stage 3 stance failed; using structural fallback");
                    Some(StormStanceVerdict::new(
                        false,
                        0.0,
                        StormCategory::Uncertain,
                        "LLM stance unavailable; structural fallback active",
                        "structural_fallback",
                    ))
                }
            }
        } else {
            None
        };

        let used_llm = stance
            .as_ref()
            .is_some_and(|s| s.evaluator != "structural_fallback");
        metrics.composite_score =
            compute_composite_score(metrics.structural_score, stance.as_ref());

        let llm_benign_veto = used_llm
            && stance.as_ref().is_some_and(|s| {
                !s.collective_hostility
                    && s.category == StormCategory::BenignViral
                    && s.confidence >= BENIGN_VIRAL_VETO_CONFIDENCE
            });
        let confirmed = if !qualifies_for_stance || llm_benign_veto {
            false
        } else if used_llm {
            metrics.composite_score >= self.config.confirmed_score_threshold
        } else {
            metrics.structural_score >= self.config.structural_fallback_threshold
        };

        let cell_id = opaque_cell_id(&candidate.focal_post_uri);

        if !confirmed {
            self.stats
                .storms_suppressed_benign
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                cell = %cell_id,
                tier = %target_tier,
                oon_authors = metrics.unique_oon_authors_short,
                oon_ratio = %format!("{:.2}", metrics.out_of_network_ratio),
                direct_ratio = %format!("{:.2}", metrics.direct_ratio),
                dunk_ratio = %format!("{:.2}", metrics.dunk_ratio),
                burst_z = %format!("{:.2}", metrics.burst_z_score),
                structural = %format!("{:.3}", metrics.structural_score),
                composite = %format!("{:.3}", metrics.composite_score),
                "Stage 2/3 candidate suppressed below threshold"
            );
            return Ok(None);
        }

        let category = if let Some(ref s) = stance {
            if s.evaluator != "structural_fallback" {
                s.category
            } else if matches!(target_tier, TargetTier::PublicFigure | TargetTier::Large) {
                StormCategory::PublicFigureBacklash
            } else {
                StormCategory::Dogpile
            }
        } else if matches!(target_tier, TargetTier::PublicFigure | TargetTier::Large) {
            StormCategory::PublicFigureBacklash
        } else {
            StormCategory::Dogpile
        };

        let severity = severity_from_score(metrics.composite_score);
        let (radar_angle_deg, radar_radius) =
            radar_blip_coordinates(&candidate.focal_post_uri, metrics.composite_score);
        let snippet: String = focal_post
            .as_ref()
            .map(|f| f.text.chars().take(200).collect())
            .unwrap_or_default();

        let storm = NetworkStorm {
            id: 0,
            cell_id,
            target_did: candidate.target_did,
            target_handle,
            target_followers,
            target_tier,
            focal_post_uri: candidate.focal_post_uri,
            focal_post_snippet: snippet,
            status: StormStatus::Active,
            severity,
            category,
            peak_score: metrics.composite_score,
            radar_angle_deg,
            radar_radius,
            started_at_us: candidate.observed_at_us,
            updated_at_us: candidate.observed_at_us,
            resolved_at_us: None,
            metrics,
            top_catalyst,
            stance,
        };

        let (storm_id, is_new, is_escalation) = self.store.upsert_active_storm(&storm)?;
        if is_new {
            self.stats.storms_detected.fetch_add(1, Ordering::Relaxed);
        } else if is_escalation {
            self.stats.storms_escalated.fetch_add(1, Ordering::Relaxed);
        }

        let persisted = self.store.get_storm(storm_id)?.unwrap_or(NetworkStorm {
            id: storm_id,
            ..storm
        });

        let _ = self.events_tx.send(RadarEvent::StormUpsert {
            storm: Box::new(persisted.clone()),
            is_escalation,
        });

        info!(
            storm_id,
            cell = %persisted.cell_id,
            severity = %persisted.severity,
            category = %persisted.category,
            tier = %persisted.target_tier,
            score = %persisted.metrics.composite_score,
            "🌩️ Network storm cell confirmed"
        );

        Ok(Some(persisted))
    }

    /// Computes a real-time [`WeatherSummary`] from currently active SQLite storms and throughput.
    ///
    /// # Errors
    /// Returns [`SkydopplerError::Storage`] on database query error.
    pub fn current_weather(&self) -> Result<WeatherSummary> {
        let active = self.store.list_storms(true, 500)?;
        let weather_index = compute_weather_index(&active);
        let level = classify_weather_level(weather_index);
        let mut critical_storms = 0usize;
        let mut high_storms = 0usize;
        let mut elevated_storms = 0usize;
        let mut small_target_storms = 0usize;
        let mut public_figure_backlashes = 0usize;

        for s in &active {
            match s.severity {
                crate::types::StormSeverity::Critical => critical_storms += 1,
                crate::types::StormSeverity::High => high_storms += 1,
                crate::types::StormSeverity::Elevated => elevated_storms += 1,
            }
            if matches!(s.target_tier, TargetTier::Small | TargetTier::Unknown) {
                small_target_storms += 1;
            }
            if s.category == StormCategory::PublicFigureBacklash {
                public_figure_backlashes += 1;
            }
        }

        let commits_per_sec = {
            let mut m = self.throughput.lock();
            let now = Instant::now();
            let dt = now.saturating_duration_since(m.last_instant).as_secs_f64();
            let cur = self.stats.commits_received.load(Ordering::Relaxed);
            if dt >= 1.0 {
                let delta = cur.saturating_sub(m.last_commits) as f64;
                m.commits_per_sec = delta / dt;
                m.last_instant = now;
                m.last_commits = cur;
            }
            m.commits_per_sec
        };

        Ok(WeatherSummary {
            timestamp_us: current_time_us(),
            weather_index,
            level,
            active_storms: active.len(),
            critical_storms,
            high_storms,
            elevated_storms,
            small_target_storms,
            public_figure_backlashes,
            tracked_posts: self.tracker.snapshot().tracked_posts,
            commits_per_sec,
        })
    }

    /// Lists persisted storms (`active_only` optional filter).
    ///
    /// # Errors
    /// Returns [`SkydopplerError::Storage`] on query failure.
    pub fn list_storms(&self, active_only: bool, limit: usize) -> Result<Vec<NetworkStorm>> {
        self.store.list_storms(active_only, limit)
    }

    /// Looks up a single storm by opaque `cell_id` or numeric ID string.
    ///
    /// # Errors
    /// Returns [`SkydopplerError::Storage`] on query failure.
    pub fn get_storm(&self, id_or_cell: &str) -> Result<Option<NetworkStorm>> {
        if let Ok(id) = id_or_cell.trim().parse::<i64>() {
            return self.store.get_storm(id);
        }
        self.store.get_storm_by_cell_id(id_or_cell.trim())
    }

    /// Dismisses a storm cell manually and emits a closure notification.
    ///
    /// # Errors
    /// Returns [`SkydopplerError::Storage`] on database failure.
    pub fn dismiss_storm(&self, storm_id: i64) -> Result<bool> {
        let Some(existing) = self.store.get_storm(storm_id)? else {
            return Ok(false);
        };
        let now_us = current_time_us();
        let updated =
            self.store
                .update_storm_status(storm_id, StormStatus::Dismissed, Some(now_us))?;
        if updated {
            let _ = self.events_tx.send(RadarEvent::StormClosed {
                storm_id,
                cell_id: existing.cell_id,
                status: StormStatus::Dismissed,
            });
        }
        Ok(updated)
    }

    /// Persists cumulative stats snapshot and latest Jetstream cursor.
    ///
    /// # Errors
    /// Returns [`SkydopplerError::Storage`] or serialization failure.
    pub fn persist_state(&self) -> Result<()> {
        let cursor = self.latest_cursor_us.load(Ordering::Relaxed);
        if cursor > 0 {
            self.store.save_cursor_us(cursor)?;
        }
        let snap_json = serde_json::to_string(&self.stats.snapshot())?;
        self.store.save_meta_json(STATS_META_KEY, &snap_json)?;
        Ok(())
    }

    /// Executes one maintenance sweep: prunes stale RAM windows, resolves expired storms,
    /// records a weather snapshot, prunes aged SQLite history, and emits `WeatherPulse`.
    ///
    /// # Errors
    /// Returns [`SkydopplerError::Storage`] on failure.
    pub fn run_maintenance_tick(&self) -> Result<()> {
        let now_us = current_time_us();
        let _ = self.tracker.prune_stale(now_us);

        let resolve_cutoff = now_us.saturating_sub(duration_to_us(self.config.storm_ttl));
        for (storm_id, cell_id) in self.store.resolve_stale_storms(resolve_cutoff, now_us)? {
            let _ = self.events_tx.send(RadarEvent::StormClosed {
                storm_id,
                cell_id,
                status: StormStatus::Resolved,
            });
        }

        let retention_cutoff = now_us.saturating_sub(duration_to_us(self.config.history_retention));
        let _ = self.store.prune_older_than(retention_cutoff)?;

        let weather = self.current_weather()?;
        self.store.record_weather_snapshot(&WeatherPoint {
            timestamp_us: weather.timestamp_us,
            weather_index: weather.weather_index,
            active_storms: weather.active_storms,
            commits_per_sec: weather.commits_per_sec,
        })?;
        self.persist_state()?;
        let _ = self.events_tx.send(RadarEvent::WeatherPulse { weather });
        Ok(())
    }

    /// Periodic background maintenance worker running until `cancel` fires.
    pub async fn run_maintenance(&self, interval_dur: Duration, cancel: CancellationToken) {
        let mut interval = tokio::time::interval(interval_dur.max(Duration::from_secs(1)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(e) = self.run_maintenance_tick() {
                        self.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
                        warn!(error = %e, "Radar maintenance tick failed");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use crate::enricher::MockNetworkEnricher;
    use crate::stance::MockStormStanceClassifier;
    use crate::types::{ActorSummary, InteractionKind, SampleReply, StormMetrics};

    #[tokio::test]
    async fn engine_evaluates_confirms_escalates_suppresses_and_maintains() {
        let enricher = Arc::new(MockNetworkEnricher::new());
        let focal_uri = "at://did:plc:small/app.bsky.feed.post/3k1";
        let qt_uri = "at://did:plc:giant/app.bsky.feed.post/3kqt";

        enricher.set_post(PostEngagementSnapshot {
            uri: focal_uri.to_string(),
            author_did: "did:plc:small".to_string(),
            author_handle: Some("small.bsky.social".to_string()),
            author_followers: Some(150),
            text: "Small account post".to_string(),
            reply_count: 55,
            repost_count: 1,
            like_count: 3,
            quote_count: 12,
            ..Default::default()
        });
        enricher.set_post(PostEngagementSnapshot {
            uri: qt_uri.to_string(),
            author_did: "did:plc:giant".to_string(),
            author_handle: Some("giant.bsky.social".to_string()),
            author_followers: Some(120_000),
            text: "Dunking!".to_string(),
            reply_count: 10,
            repost_count: 90,
            like_count: 1200,
            quote_count: 5,
            ..Default::default()
        });
        enricher.set_profile(ActorSummary {
            did: "did:plc:small".to_string(),
            handle: Some("small.bsky.social".to_string()),
            followers_count: Some(150),
            ..Default::default()
        });
        enricher.set_profile(ActorSummary {
            did: "did:plc:giant".to_string(),
            handle: Some("giant.bsky.social".to_string()),
            followers_count: Some(120_000),
            ..Default::default()
        });

        let stance = MockStormStanceClassifier::new(StormStanceVerdict::new(
            true,
            0.94,
            StormCategory::Dogpile,
            "Coordinated quote-dunk wave",
            "mock-stance",
        ));

        let cfg = RadarConfig::default()
            .with_min_unique_authors(4)
            .with_enable_llm_stance(true)
            .with_thresholds(0.35, 0.62, 0.68);
        let engine = SkydopplerEngineBuilder::new(cfg)
            .with_enricher(enricher.clone())
            .with_stance_classifier(Arc::new(stance))
            .with_appview_endpoint("http://127.0.0.1:9999")
            .build()
            .unwrap();

        let mut events = engine.subscribe_events();
        let now = current_time_us();
        let cand = NetworkSurgeCandidate {
            target_did: "did:plc:small".to_string(),
            focal_post_uri: focal_uri.to_string(),
            observed_at_us: now,
            initial_metrics: StormMetrics {
                unique_authors_short: 24,
                unique_oon_authors_short: 24,
                burst_z_score: 9.0,
                ..Default::default()
            },
            sample_replies: vec![SampleReply {
                author_did: "did:plc:stranger1".to_string(),
                kind: InteractionKind::Reply,
                post_uri: "at://did:plc:stranger1/app.bsky.feed.post/1".to_string(),
                text_snippet: "Pile on".to_string(),
            }],
            recent_quote_uris: vec![qt_uri.to_string()],
        };

        let storm = engine.evaluate_candidate(cand).await.unwrap().unwrap();
        assert_eq!(storm.target_tier, TargetTier::Small);
        assert_eq!(storm.category, StormCategory::Dogpile);
        assert!(matches!(
            events.try_recv().unwrap(),
            RadarEvent::StormUpsert { .. }
        ));

        let weather = engine.current_weather().unwrap();
        assert_eq!(weather.active_storms, 1);
        assert_eq!(weather.small_target_storms, 1);
        assert!(engine.get_storm(&storm.cell_id).unwrap().is_some());
        assert!(engine.get_storm(&storm.id.to_string()).unwrap().is_some());

        engine.run_maintenance_tick().unwrap();
        assert!(!engine.store().list_weather_history(10).unwrap().is_empty());

        assert!(engine.dismiss_storm(storm.id).unwrap());
        assert!(!engine.dismiss_storm(999_999).unwrap());
    }

    #[tokio::test]
    async fn engine_discovers_getquotes_gates_zero_like_catalysts_prefilters_and_vetoes_benign_viral(
    ) {
        let enricher = Arc::new(MockNetworkEnricher::new());
        let focal_uri = "at://did:plc:qa/app.bsky.feed.post/3kqa";
        let zero_qt_uri = "at://did:plc:bigreply/app.bsky.feed.post/3k0qt";
        let viral_dunk_uri = "at://did:plc:dunker/app.bsky.feed.post/3kdunk";

        enricher.set_post(PostEngagementSnapshot {
            uri: focal_uri.to_string(),
            author_did: "did:plc:qa".to_string(),
            author_handle: Some("qa.bsky.social".to_string()),
            author_followers: Some(1_000),
            text: "20. What is your most mundane celebrity encounter?".to_string(),
            reply_count: 60,
            repost_count: 5,
            like_count: 35,
            quote_count: 18,
            ..Default::default()
        });
        enricher.set_post(PostEngagementSnapshot {
            uri: zero_qt_uri.to_string(),
            author_did: "did:plc:bigreply".to_string(),
            author_handle: Some("bigreply.bsky.social".to_string()),
            author_followers: Some(40_000),
            text: "Keanu in a coffee shop line".to_string(),
            reply_count: 0,
            repost_count: 0,
            like_count: 0,
            quote_count: 0,
            ..Default::default()
        });
        enricher.set_profile(ActorSummary {
            did: "did:plc:qa".to_string(),
            handle: Some("qa.bsky.social".to_string()),
            followers_count: Some(1_000),
            ..Default::default()
        });
        enricher.set_profile(ActorSummary {
            did: "did:plc:bigreply".to_string(),
            handle: Some("bigreply.bsky.social".to_string()),
            followers_count: Some(40_000),
            ..Default::default()
        });

        let stance = Arc::new(MockStormStanceClassifier::new(StormStanceVerdict::new(
            false,
            0.96,
            StormCategory::BenignViral,
            "Benign Q&A participation thread",
            "mock-stance",
        )));

        let cfg_fallback = RadarConfig::default()
            .with_min_unique_authors(4)
            .with_enable_llm_stance(false);
        let engine_no_llm = SkydopplerEngineBuilder::new(cfg_fallback)
            .with_enricher(enricher.clone())
            .build()
            .unwrap();

        // Zero-like quote must NOT become top_catalyst, inflate power_asymmetry, or trip 0.65 threshold.
        let mk_cand = || NetworkSurgeCandidate {
            target_did: "did:plc:qa".to_string(),
            focal_post_uri: focal_uri.to_string(),
            observed_at_us: current_time_us(),
            initial_metrics: StormMetrics {
                unique_authors_short: 10,
                unique_oon_authors_short: 10,
                burst_z_score: 1.0,
                ..Default::default()
            },
            sample_replies: vec![SampleReply {
                author_did: "did:plc:stranger1".to_string(),
                kind: InteractionKind::Reply,
                post_uri: "at://did:plc:stranger1/app.bsky.feed.post/1".to_string(),
                text_snippet: "I saw Danny DeVito once".to_string(),
            }],
            recent_quote_uris: vec![zero_qt_uri.to_string()],
        };

        let without_catalyst = engine_no_llm.evaluate_candidate(mk_cand()).await.unwrap();
        assert!(without_catalyst.is_none());

        // Even with a permissive fallback threshold, verify 0-like quote leaves catalyst=None & asym=1.0.
        let cfg_permissive = RadarConfig::default()
            .with_min_unique_authors(4)
            .with_enable_llm_stance(false)
            .with_thresholds(0.35, 0.56, 0.50);
        let engine_permissive = SkydopplerEngineBuilder::new(cfg_permissive)
            .with_enricher(enricher.clone())
            .build()
            .unwrap();
        let zero_gated = engine_permissive
            .evaluate_candidate(mk_cand())
            .await
            .unwrap()
            .expect("permissive threshold confirms to inspect metrics");
        assert!(zero_gated.top_catalyst.is_none());
        assert!((zero_gated.metrics.power_asymmetry - 1.0).abs() < 1e-9);
        assert!((zero_gated.metrics.dunk_ratio - 0.0).abs() < 1e-9);

        // Now getQuotes returns an earlier engaged dunk quote (even when recent_quote_uris is empty) -> discovers catalyst!
        let dunk_focal = "at://did:plc:dunked/app.bsky.feed.post/3kfocal";
        enricher.set_post(PostEngagementSnapshot {
            uri: dunk_focal.to_string(),
            author_did: "did:plc:dunked".to_string(),
            author_handle: Some("dunked.bsky.social".to_string()),
            author_followers: Some(300),
            text: "Bad take".to_string(),
            reply_count: 35,
            repost_count: 1,
            like_count: 2,
            quote_count: 15,
            ..Default::default()
        });
        enricher.set_profile(ActorSummary {
            did: "did:plc:dunked".to_string(),
            handle: Some("dunked.bsky.social".to_string()),
            followers_count: Some(300),
            ..Default::default()
        });
        enricher.set_profile(ActorSummary {
            did: "did:plc:dunker".to_string(),
            handle: Some("dunker.bsky.social".to_string()),
            followers_count: Some(120_000),
            ..Default::default()
        });
        enricher.set_quotes(
            dunk_focal,
            vec![PostEngagementSnapshot {
                uri: viral_dunk_uri.to_string(),
                author_did: "did:plc:dunker".to_string(),
                author_handle: Some("dunker.bsky.social".to_string()),
                author_followers: Some(120_000),
                text: "Worst post today".to_string(),
                reply_count: 12,
                repost_count: 80,
                like_count: 900,
                quote_count: 4,
                ..Default::default()
            }],
        );
        let dunk_cand = NetworkSurgeCandidate {
            target_did: "did:plc:dunked".to_string(),
            focal_post_uri: dunk_focal.to_string(),
            observed_at_us: current_time_us(),
            initial_metrics: StormMetrics {
                unique_authors_short: 20,
                unique_oon_authors_short: 20,
                burst_z_score: 8.0,
                ..Default::default()
            },
            sample_replies: vec![SampleReply {
                author_did: "did:plc:stranger1".to_string(),
                kind: InteractionKind::Reply,
                post_uri: "at://did:plc:stranger1/app.bsky.feed.post/2".to_string(),
                text_snippet: "Ratioed".to_string(),
            }],
            recent_quote_uris: vec![],
        };
        let discovered = engine_no_llm
            .evaluate_candidate(dunk_cand)
            .await
            .unwrap()
            .expect("getQuotes catalyst should confirm structural storm");
        assert_eq!(
            discovered.top_catalyst.as_ref().unwrap().post_uri,
            viral_dunk_uri
        );

        // Stage 3 pre-filter skips mutual-friend threads without invoking LLM.
        let cfg_llm = RadarConfig::default()
            .with_min_unique_authors(4)
            .with_enable_llm_stance(true)
            .with_thresholds(0.35, 0.56, 0.65);
        let engine_llm = SkydopplerEngineBuilder::new(cfg_llm)
            .with_enricher(enricher.clone())
            .with_stance_classifier(stance.clone())
            .build()
            .unwrap();

        enricher.set_relationship("did:plc:qa", "did:plc:friend1", true, true);
        let mutual_cand = NetworkSurgeCandidate {
            sample_replies: vec![SampleReply {
                author_did: "did:plc:friend1".to_string(),
                kind: InteractionKind::Reply,
                post_uri: "at://did:plc:friend1/app.bsky.feed.post/1".to_string(),
                text_snippet: "haha".to_string(),
            }],
            ..mk_cand()
        };
        assert!(engine_llm
            .evaluate_candidate(mutual_cand)
            .await
            .unwrap()
            .is_none());
        assert_eq!(stance.call_count(), 0);

        // High-structural Q&A prompt qualifies for LLM, and BenignViral veto suppresses even if threshold low.
        let cfg_low = RadarConfig::default()
            .with_min_unique_authors(4)
            .with_enable_llm_stance(true)
            .with_thresholds(0.35, 0.30, 0.65);
        let engine_veto = SkydopplerEngineBuilder::new(cfg_low)
            .with_enricher(enricher)
            .with_stance_classifier(stance.clone())
            .build()
            .unwrap();
        let high_struct_qa = NetworkSurgeCandidate {
            initial_metrics: StormMetrics {
                unique_authors_short: 24,
                unique_oon_authors_short: 24,
                burst_z_score: 9.0,
                ..Default::default()
            },
            ..mk_cand()
        };
        assert!(engine_veto
            .evaluate_candidate(high_struct_qa)
            .await
            .unwrap()
            .is_none());
        assert_eq!(stance.call_count(), 1);
    }

    #[tokio::test]
    async fn engine_gates_beloved_viral_posts_and_low_oon_mega_blips_while_confirming_outlet_backlash(
    ) {
        let enricher = Arc::new(MockNetworkEnricher::new());
        let stance = Arc::new(MockStormStanceClassifier::new(StormStanceVerdict::new(
            true,
            0.95,
            StormCategory::PublicFigureBacklash,
            "Direct editorial backlash",
            "mock-stance",
        )));

        // 1. Beloved viral post (1,384 likes vs 122 replies -> direct_ratio 0.30, minor quote 44 likes -> dunk 0.03)
        let beloved_uri = "at://did:plc:beloved/app.bsky.feed.post/3kbel";
        let minor_qt = "at://did:plc:commenter/app.bsky.feed.post/3kcm";
        enricher.set_post(PostEngagementSnapshot {
            uri: beloved_uri.to_string(),
            author_did: "did:plc:beloved".to_string(),
            author_handle: Some("beloved.bsky.social".to_string()),
            author_followers: Some(13_500),
            text: "Sarcastic commentary on discourse".to_string(),
            reply_count: 122,
            repost_count: 128,
            like_count: 1384,
            quote_count: 332,
            embed_kind: Some("images".to_string()),
        });
        enricher.set_post(PostEngagementSnapshot {
            uri: minor_qt.to_string(),
            author_did: "did:plc:commenter".to_string(),
            author_handle: Some("commenter.bsky.social".to_string()),
            author_followers: Some(27_180),
            text: "Agreement commentary".to_string(),
            like_count: 44,
            repost_count: 2,
            ..Default::default()
        });
        enricher.set_profile(ActorSummary {
            did: "did:plc:beloved".to_string(),
            handle: Some("beloved.bsky.social".to_string()),
            followers_count: Some(13_500),
            ..Default::default()
        });
        enricher.set_profile(ActorSummary {
            did: "did:plc:commenter".to_string(),
            handle: Some("commenter.bsky.social".to_string()),
            followers_count: Some(27_180),
            ..Default::default()
        });

        let cfg = RadarConfig::default()
            .with_min_unique_authors(8)
            .with_enable_llm_stance(true);
        let engine = SkydopplerEngineBuilder::new(cfg)
            .with_enricher(enricher.clone())
            .with_stance_classifier(stance.clone())
            .build()
            .unwrap();

        let beloved_cand = NetworkSurgeCandidate {
            target_did: "did:plc:beloved".to_string(),
            focal_post_uri: beloved_uri.to_string(),
            observed_at_us: current_time_us(),
            initial_metrics: StormMetrics {
                unique_authors_short: 8,
                unique_oon_authors_short: 8,
                burst_z_score: 10.6,
                ..Default::default()
            },
            sample_replies: vec![SampleReply {
                author_did: "did:plc:s1".to_string(),
                kind: InteractionKind::Reply,
                post_uri: "at://did:plc:s1/app.bsky.feed.post/1".to_string(),
                text_snippet: "Totally true lol".to_string(),
            }],
            recent_quote_uris: vec![minor_qt.to_string()],
        };
        assert!(engine
            .evaluate_candidate(beloved_cand)
            .await
            .unwrap()
            .is_none());
        assert_eq!(stance.call_count(), 0);

        // 2. Low-OON blip on PublicFigure (665k followers, 4 OON authors < min_unique_authors=8)
        let outlet_blip_uri = "at://did:plc:outlet/app.bsky.feed.post/3kblip";
        enricher.set_post(PostEngagementSnapshot {
            uri: outlet_blip_uri.to_string(),
            author_did: "did:plc:outlet".to_string(),
            author_handle: Some("outlet.com".to_string()),
            author_followers: Some(665_000),
            text: "Breaking headline update".to_string(),
            reply_count: 16,
            repost_count: 1,
            like_count: 3,
            quote_count: 4,
            embed_kind: Some("external_link".to_string()),
        });
        enricher.set_profile(ActorSummary {
            did: "did:plc:outlet".to_string(),
            handle: Some("outlet.com".to_string()),
            display_name: Some("Major Outlet".to_string()),
            followers_count: Some(665_000),
            description: Some("Breaking news".to_string()),
        });
        let blip_cand = NetworkSurgeCandidate {
            target_did: "did:plc:outlet".to_string(),
            focal_post_uri: outlet_blip_uri.to_string(),
            observed_at_us: current_time_us(),
            initial_metrics: StormMetrics {
                unique_authors_short: 4,
                unique_oon_authors_short: 4,
                burst_z_score: 9.0,
                ..Default::default()
            },
            sample_replies: vec![SampleReply {
                author_did: "did:plc:r1".to_string(),
                kind: InteractionKind::Reply,
                post_uri: "at://did:plc:r1/app.bsky.feed.post/1".to_string(),
                text_snippet: "Mocking news subject".to_string(),
            }],
            recent_quote_uris: vec![],
        };
        assert!(engine
            .evaluate_candidate(blip_cand)
            .await
            .unwrap()
            .is_none());
        assert_eq!(stance.call_count(), 0);

        // 3. Genuine PublicFigureBacklash directed at outlet's own framing (12 OON >= 8, direct_ratio=12.0)
        let outlet_backlash_uri = "at://did:plc:outlet/app.bsky.feed.post/3kbacklash";
        enricher.set_post(PostEngagementSnapshot {
            uri: outlet_backlash_uri.to_string(),
            author_did: "did:plc:outlet".to_string(),
            author_handle: Some("outlet.com".to_string()),
            author_followers: Some(665_000),
            text: "Opinion: Why Both Sides Share Equal Blame".to_string(),
            reply_count: 140,
            repost_count: 4,
            like_count: 10,
            quote_count: 40,
            embed_kind: Some("external_link".to_string()),
        });
        let backlash_cand = NetworkSurgeCandidate {
            target_did: "did:plc:outlet".to_string(),
            focal_post_uri: outlet_backlash_uri.to_string(),
            observed_at_us: current_time_us(),
            initial_metrics: StormMetrics {
                unique_authors_short: 14,
                unique_oon_authors_short: 14,
                burst_z_score: 10.0,
                ..Default::default()
            },
            sample_replies: vec![SampleReply {
                author_did: "did:plc:r2".to_string(),
                kind: InteractionKind::Reply,
                post_uri: "at://did:plc:r2/app.bsky.feed.post/1".to_string(),
                text_snippet: "Embarrassing outlet framing".to_string(),
            }],
            recent_quote_uris: vec![],
        };
        let confirmed = engine
            .evaluate_candidate(backlash_cand)
            .await
            .unwrap()
            .expect("genuine editorial backlash should confirm");
        assert_eq!(confirmed.category, StormCategory::PublicFigureBacklash);
        assert_eq!(stance.call_count(), 1);
    }
}
