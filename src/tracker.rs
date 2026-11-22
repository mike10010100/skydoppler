//! 16-shard memory-bounded heavy-hitter tracker over the global Jetstream firehose.
//!
//! Cold `like` and `repost` commits are filtered with a single shard hash lookup and **zero
//! heap allocations**. Only focal posts already receiving replies/quotes retain rolling counters,
//! quote-catalyst indexes, and bounded sample responders.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;
use skybase::ingest::events::{CommitOperation, JetstreamCommit};

use crate::math::{compute_burst_acceleration, compute_direct_ratio, compute_structural_score};
use crate::time::duration_to_us;
use crate::types::{
    InteractionKind, NetworkSurgeCandidate, RadarConfig, SampleReply, StormMetrics, TargetTier,
};

const NUM_SHARDS: usize = 16;
const MAX_QUOTES_PER_POST: usize = 15;
const MAX_UNIQUE_AUTHOR_SET: usize = 2_048;
const MAX_KNOWN_POSTGATE_DETACHES: usize = 512;
const MIN_QUOTES_RATIO_BYPASS: usize = 2;

/// Outcome of observing a single [`JetstreamCommit`] in [`NetworkTracker::observe_commit`].
#[derive(Debug, Clone, PartialEq)]
pub enum ObserveOutcome {
    /// Commit ignored (delete, unknown collection, self-reply, cold like/repost).
    Ignored,
    /// Post reply/quote counted; optional [`NetworkSurgeCandidate`] emitted if thresholds crossed.
    PostInteraction(Option<NetworkSurgeCandidate>),
    /// Like or repost hit an already-tracked hot post or catalyst QT.
    HotEngagementUpdated,
    /// Target defensive block, postgate detach, or threadgate lock observed.
    DefensiveSignal(Option<NetworkSurgeCandidate>),
}

#[derive(Debug, Clone)]
struct PostEvent {
    timestamp_us: u64,
    author_did: String,
    kind: InteractionKind,
}

#[derive(Debug)]
struct PostBucket {
    target_did: String,
    focal_post_uri: String,
    last_seen_us: u64,
    events: VecDeque<PostEvent>,
    recent_quote_uris: VecDeque<String>,
    samples: VecDeque<SampleReply>,
    sample_authors: HashSet<String>,
    live_likes: VecDeque<u64>,
    live_reposts: VecDeque<u64>,
    last_emitted_us: Option<u64>,
    last_emitted_authors: usize,
}

impl PostBucket {
    fn new(target_did: String, focal_post_uri: String, now_us: u64) -> Self {
        Self {
            target_did,
            focal_post_uri,
            last_seen_us: now_us,
            events: VecDeque::new(),
            recent_quote_uris: VecDeque::new(),
            samples: VecDeque::new(),
            sample_authors: HashSet::new(),
            live_likes: VecDeque::new(),
            live_reposts: VecDeque::new(),
            last_emitted_us: None,
            last_emitted_authors: 0,
        }
    }

    fn prune(&mut self, medium_cutoff_us: u64) {
        while self
            .events
            .front()
            .is_some_and(|e| e.timestamp_us < medium_cutoff_us)
        {
            self.events.pop_front();
        }
        while self
            .live_likes
            .front()
            .is_some_and(|&ts| ts < medium_cutoff_us)
        {
            self.live_likes.pop_front();
        }
        while self
            .live_reposts
            .front()
            .is_some_and(|&ts| ts < medium_cutoff_us)
        {
            self.live_reposts.pop_front();
        }
    }

    fn count_short(&self, short_cutoff_us: u64) -> (usize, usize, usize, usize, usize) {
        let mut unique_authors = HashSet::new();
        let mut replies = 0usize;
        let mut quotes = 0usize;
        for ev in self.events.iter().rev() {
            if ev.timestamp_us < short_cutoff_us {
                break;
            }
            if unique_authors.len() < MAX_UNIQUE_AUTHOR_SET {
                unique_authors.insert(ev.author_did.as_str());
            }
            match ev.kind {
                InteractionKind::Reply => replies += 1,
                InteractionKind::Quote => quotes += 1,
            }
        }
        let likes = self
            .live_likes
            .iter()
            .rev()
            .take_while(|&&ts| ts >= short_cutoff_us)
            .count();
        let reposts = self
            .live_reposts
            .iter()
            .rev()
            .take_while(|&&ts| ts >= short_cutoff_us)
            .count();
        (unique_authors.len(), replies, quotes, likes, reposts)
    }
}

#[derive(Debug)]
struct AuthorDistressBucket {
    last_seen_us: u64,
    last_focal_uri: Option<String>,
    blocks: VecDeque<u64>,
    detaches: VecDeque<u64>,
    threadgates: VecDeque<u64>,
    known_postgate_detaches: HashSet<String>,
}

impl AuthorDistressBucket {
    fn new(now_us: u64) -> Self {
        Self {
            last_seen_us: now_us,
            last_focal_uri: None,
            blocks: VecDeque::new(),
            detaches: VecDeque::new(),
            threadgates: VecDeque::new(),
            known_postgate_detaches: HashSet::new(),
        }
    }

    fn prune(&mut self, medium_cutoff_us: u64) {
        while self.blocks.front().is_some_and(|&t| t < medium_cutoff_us) {
            self.blocks.pop_front();
        }
        while self.detaches.front().is_some_and(|&t| t < medium_cutoff_us) {
            self.detaches.pop_front();
        }
        while self
            .threadgates
            .front()
            .is_some_and(|&t| t < medium_cutoff_us)
        {
            self.threadgates.pop_front();
        }
        if self.detaches.is_empty() {
            self.known_postgate_detaches.clear();
        }
    }

    fn counts_short(&self, short_cutoff_us: u64) -> (usize, usize, usize) {
        let blocks = self
            .blocks
            .iter()
            .rev()
            .take_while(|&&t| t >= short_cutoff_us)
            .count();
        let detaches = self
            .detaches
            .iter()
            .rev()
            .take_while(|&&t| t >= short_cutoff_us)
            .count();
        let threadgates = self
            .threadgates
            .iter()
            .rev()
            .take_while(|&&t| t >= short_cutoff_us)
            .count();
        (blocks, detaches, threadgates)
    }
}

#[derive(Debug, Default)]
struct PostShard {
    posts: HashMap<String, PostBucket>,
}

#[derive(Debug, Default)]
struct AuthorShard {
    authors: HashMap<String, AuthorDistressBucket>,
}

/// Snapshot of tracked table sizes inside [`NetworkTracker`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TrackerSnapshot {
    /// Total focal posts currently held across all 16 post shards.
    pub tracked_posts: usize,
    /// Total authors currently held across all 16 defensive-distress shards.
    pub tracked_authors: usize,
}

fn shard_idx(key: &str) -> usize {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    (hash as usize) % NUM_SHARDS
}

fn parse_did_from_at_uri(uri: &str) -> Option<&str> {
    let rest = uri.strip_prefix("at://")?;
    let end = rest.find('/')?;
    let did = &rest[..end];
    if did.starts_with("did:") {
        Some(did)
    } else {
        None
    }
}

/// 16-shard rolling network-wide heavy-hitter tracker.
#[derive(Debug)]
pub struct NetworkTracker {
    config: RadarConfig,
    post_shards: Vec<Mutex<PostShard>>,
    author_shards: Vec<Mutex<AuthorShard>>,
    tracked_posts_total: AtomicUsize,
    tracked_authors_total: AtomicUsize,
}

impl NetworkTracker {
    /// Creates a new [`NetworkTracker`] governed by `config`.
    #[must_use]
    pub fn new(mut config: RadarConfig) -> Self {
        config.max_tracked_posts = config.max_tracked_posts.max(NUM_SHARDS);
        config.max_tracked_authors = config.max_tracked_authors.max(NUM_SHARDS);
        let post_shards = (0..NUM_SHARDS)
            .map(|_| Mutex::new(PostShard::default()))
            .collect();
        let author_shards = (0..NUM_SHARDS)
            .map(|_| Mutex::new(AuthorShard::default()))
            .collect();
        Self {
            config,
            post_shards,
            author_shards,
            tracked_posts_total: AtomicUsize::new(0),
            tracked_authors_total: AtomicUsize::new(0),
        }
    }

    /// Returns the tracker configuration.
    #[must_use]
    pub const fn config(&self) -> &RadarConfig {
        &self.config
    }

    /// Returns current hot-post and author-distress table sizes.
    #[must_use]
    pub fn snapshot(&self) -> TrackerSnapshot {
        TrackerSnapshot {
            tracked_posts: self.tracked_posts_total.load(Ordering::Relaxed),
            tracked_authors: self.tracked_authors_total.load(Ordering::Relaxed),
        }
    }

    /// Ingests a Jetstream commit across `post`, `like`, `repost`, `block`, `postgate`, and `threadgate`.
    pub fn observe_commit(&self, commit: &JetstreamCommit) -> ObserveOutcome {
        match commit.collection.as_str() {
            "app.bsky.feed.post" => {
                if commit.operation != CommitOperation::Create {
                    return ObserveOutcome::Ignored;
                }
                self.observe_post_create(commit)
            }
            "app.bsky.feed.like" => {
                if commit.operation != CommitOperation::Create {
                    return ObserveOutcome::Ignored;
                }
                self.observe_hot_engagement(commit, true)
            }
            "app.bsky.feed.repost" => {
                if commit.operation != CommitOperation::Create {
                    return ObserveOutcome::Ignored;
                }
                self.observe_hot_engagement(commit, false)
            }
            "app.bsky.graph.block" => {
                if commit.operation != CommitOperation::Create {
                    return ObserveOutcome::Ignored;
                }
                self.observe_author_distress(commit.did.as_str(), commit.time_us, 1, 0, 0, None)
            }
            "app.bsky.feed.postgate" => {
                if !matches!(
                    commit.operation,
                    CommitOperation::Create | CommitOperation::Update
                ) {
                    return ObserveOutcome::Ignored;
                }
                self.observe_postgate(commit)
            }
            "app.bsky.feed.threadgate" => {
                if !matches!(
                    commit.operation,
                    CommitOperation::Create | CommitOperation::Update
                ) {
                    return ObserveOutcome::Ignored;
                }
                self.observe_threadgate(commit)
            }
            _ => ObserveOutcome::Ignored,
        }
    }

    fn observe_post_create(&self, commit: &JetstreamCommit) -> ObserveOutcome {
        let Some(ref record) = commit.record else {
            return ObserveOutcome::Ignored;
        };
        let text = record
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();

        // Extract reply target (prefer direct parent so subthread dogpiles target the replied-to post,
        // falling back to root if parent is omitted) and quote target.
        let reply_obj = record.get("reply");
        let root_uri = reply_obj
            .and_then(|r| r.get("root"))
            .and_then(|p| p.get("uri"))
            .and_then(|u| u.as_str());
        let parent_uri = reply_obj
            .and_then(|r| r.get("parent"))
            .and_then(|p| p.get("uri"))
            .and_then(|u| u.as_str());

        let embed = record.get("embed");
        let quote_uri = embed
            .and_then(|e| {
                e.get("record").and_then(|r| {
                    r.get("uri")
                        .or_else(|| r.get("record").and_then(|rr| rr.get("uri")))
                })
            })
            .and_then(|u| u.as_str());

        let (focal_post_uri, kind) = if let Some(q_uri) = quote_uri {
            (q_uri, InteractionKind::Quote)
        } else if let Some(p_uri) = parent_uri {
            (p_uri, InteractionKind::Reply)
        } else if let Some(r_uri) = root_uri {
            (r_uri, InteractionKind::Reply)
        } else {
            return ObserveOutcome::Ignored;
        };

        let Some(target_did) = parse_did_from_at_uri(focal_post_uri).map(ToString::to_string)
        else {
            return ObserveOutcome::Ignored;
        };
        if target_did == commit.did {
            return ObserveOutcome::Ignored;
        }

        let interaction_post_uri =
            format!("at://{}/app.bsky.feed.post/{}", commit.did, commit.rkey);
        let now_us = commit.time_us;
        let short_cutoff = now_us.saturating_sub(duration_to_us(self.config.short_window));
        let medium_cutoff = now_us.saturating_sub(duration_to_us(self.config.medium_window));
        let cooldown_us = duration_to_us(self.config.enrich_cooldown);

        // Fetch target distress first (never hold two shard locks together across unknown order!).
        let distress = self.read_distress_and_remember_focal(
            &target_did,
            focal_post_uri,
            now_us,
            short_cutoff,
            medium_cutoff,
        );

        let shard_cap = (self.config.max_tracked_posts / NUM_SHARDS).max(1);
        let sidx = shard_idx(focal_post_uri);
        let mut guard = self.post_shards[sidx].lock();

        if !guard.posts.contains_key(focal_post_uri) && guard.posts.len() >= shard_cap {
            if let Some(evict_key) = guard
                .posts
                .iter()
                .min_by_key(|(_, b)| (b.events.len(), b.last_seen_us))
                .map(|(k, _)| k.clone())
            {
                if guard.posts.remove(&evict_key).is_some() {
                    self.tracked_posts_total.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }

        let is_new = !guard.posts.contains_key(focal_post_uri);
        let bucket = guard
            .posts
            .entry(focal_post_uri.to_string())
            .or_insert_with(|| {
                PostBucket::new(target_did.clone(), focal_post_uri.to_string(), now_us)
            });
        if is_new {
            self.tracked_posts_total.fetch_add(1, Ordering::Relaxed);
        }

        bucket.last_seen_us = now_us;
        bucket.prune(medium_cutoff);
        bucket.events.push_back(PostEvent {
            timestamp_us: now_us,
            author_did: commit.did.clone(),
            kind,
        });

        if kind == InteractionKind::Quote
            && !bucket
                .recent_quote_uris
                .iter()
                .any(|u| u == &interaction_post_uri)
        {
            if bucket.recent_quote_uris.len() >= MAX_QUOTES_PER_POST {
                bucket.recent_quote_uris.pop_front();
            }
            bucket
                .recent_quote_uris
                .push_back(interaction_post_uri.clone());
        }

        if !text.is_empty() && !bucket.sample_authors.contains(&commit.did) {
            if bucket.samples.len() >= self.config.max_sample_replies {
                if let Some(old) = bucket.samples.pop_front() {
                    bucket.sample_authors.remove(&old.author_did);
                }
            }
            bucket.sample_authors.insert(commit.did.clone());
            bucket.samples.push_back(SampleReply {
                author_did: commit.did.clone(),
                kind,
                post_uri: interaction_post_uri,
                text_snippet: text.chars().take(180).collect(),
            });
        }

        let candidate =
            self.maybe_build_candidate(bucket, distress, now_us, short_cutoff, cooldown_us);
        ObserveOutcome::PostInteraction(candidate)
    }

    fn observe_hot_engagement(&self, commit: &JetstreamCommit, is_like: bool) -> ObserveOutcome {
        let Some(ref record) = commit.record else {
            return ObserveOutcome::Ignored;
        };
        let Some(subject_uri) = record
            .get("subject")
            .and_then(|s| s.get("uri"))
            .and_then(|u| u.as_str())
        else {
            return ObserveOutcome::Ignored;
        };

        let sidx = shard_idx(subject_uri);
        let mut guard = self.post_shards[sidx].lock();
        let Some(bucket) = guard.posts.get_mut(subject_uri) else {
            // Cold path: zero allocations!
            return ObserveOutcome::Ignored;
        };
        if commit.did == bucket.target_did {
            return ObserveOutcome::HotEngagementUpdated;
        }
        let list = if is_like {
            &mut bucket.live_likes
        } else {
            &mut bucket.live_reposts
        };
        if list.len() < 4_096 {
            list.push_back(commit.time_us);
        }
        ObserveOutcome::HotEngagementUpdated
    }

    fn observe_postgate(&self, commit: &JetstreamCommit) -> ObserveOutcome {
        let Some(ref record) = commit.record else {
            return ObserveOutcome::Ignored;
        };
        let post_uri = record
            .get("post")
            .and_then(|v| v.as_str())
            .map(ToString::to_string)
            .filter(|s| s.starts_with("at://"));

        let sidx = shard_idx(&commit.did);
        let short_cutoff = commit
            .time_us
            .saturating_sub(duration_to_us(self.config.short_window));
        let medium_cutoff = commit
            .time_us
            .saturating_sub(duration_to_us(self.config.medium_window));
        let mut new_detaches = 0usize;

        {
            let cap = (self.config.max_tracked_authors / NUM_SHARDS).max(1);
            let mut guard = self.author_shards[sidx].lock();
            if !guard.authors.contains_key(&commit.did) && guard.authors.len() >= cap {
                if let Some(evict) = guard
                    .authors
                    .iter()
                    .min_by_key(|(_, b)| b.last_seen_us)
                    .map(|(k, _)| k.clone())
                {
                    if guard.authors.remove(&evict).is_some() {
                        self.tracked_authors_total.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
            let is_new = !guard.authors.contains_key(&commit.did);
            let bucket = guard
                .authors
                .entry(commit.did.clone())
                .or_insert_with(|| AuthorDistressBucket::new(commit.time_us));
            if is_new {
                self.tracked_authors_total.fetch_add(1, Ordering::Relaxed);
            }
            bucket.last_seen_us = commit.time_us;
            bucket.prune(medium_cutoff);
            if let Some(ref p) = post_uri {
                bucket.last_focal_uri = Some(p.clone());
            }

            if let Some(arr) = record
                .get("detachedEmbeddingUris")
                .and_then(|v| v.as_array())
            {
                for item in arr {
                    if let Some(u) = item.as_str().filter(|s| !s.is_empty()) {
                        if bucket.known_postgate_detaches.len() >= MAX_KNOWN_POSTGATE_DETACHES {
                            break;
                        }
                        if bucket.known_postgate_detaches.insert(u.to_string()) {
                            bucket.detaches.push_back(commit.time_us);
                            new_detaches += 1;
                        }
                    }
                }
            } else {
                // DisableRule without individual URIs still signals defensive postgate usage.
                bucket.detaches.push_back(commit.time_us);
                new_detaches = 1;
            }
            let _ = short_cutoff;
        }

        if new_detaches == 0 {
            return ObserveOutcome::Ignored;
        }
        self.check_distress_trigger(&commit.did, commit.time_us, post_uri.as_deref())
    }

    fn observe_threadgate(&self, commit: &JetstreamCommit) -> ObserveOutcome {
        let post_uri = commit
            .record
            .as_ref()
            .and_then(|r| r.get("post"))
            .and_then(|v| v.as_str())
            .filter(|s| s.starts_with("at://"));
        self.observe_author_distress(&commit.did, commit.time_us, 0, 0, 1, post_uri)
    }

    fn observe_author_distress(
        &self,
        author_did: &str,
        now_us: u64,
        blocks: usize,
        detaches: usize,
        threadgates: usize,
        focal_uri: Option<&str>,
    ) -> ObserveOutcome {
        let sidx = shard_idx(author_did);
        let medium_cutoff = now_us.saturating_sub(duration_to_us(self.config.medium_window));
        {
            let cap = (self.config.max_tracked_authors / NUM_SHARDS).max(1);
            let mut guard = self.author_shards[sidx].lock();
            if !guard.authors.contains_key(author_did) && guard.authors.len() >= cap {
                if let Some(evict) = guard
                    .authors
                    .iter()
                    .min_by_key(|(_, b)| b.last_seen_us)
                    .map(|(k, _)| k.clone())
                {
                    if guard.authors.remove(&evict).is_some() {
                        self.tracked_authors_total.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
            let is_new = !guard.authors.contains_key(author_did);
            let bucket = guard
                .authors
                .entry(author_did.to_string())
                .or_insert_with(|| AuthorDistressBucket::new(now_us));
            if is_new {
                self.tracked_authors_total.fetch_add(1, Ordering::Relaxed);
            }
            bucket.last_seen_us = now_us;
            bucket.prune(medium_cutoff);
            if let Some(f) = focal_uri {
                bucket.last_focal_uri = Some(f.to_string());
            }
            for _ in 0..blocks {
                bucket.blocks.push_back(now_us);
            }
            for _ in 0..detaches {
                bucket.detaches.push_back(now_us);
            }
            for _ in 0..threadgates {
                bucket.threadgates.push_back(now_us);
            }
        }
        self.check_distress_trigger(author_did, now_us, focal_uri)
    }

    fn read_distress_and_remember_focal(
        &self,
        target_did: &str,
        focal_uri: &str,
        now_us: u64,
        short_cutoff: u64,
        medium_cutoff: u64,
    ) -> (usize, usize, usize) {
        let sidx = shard_idx(target_did);
        let mut guard = self.author_shards[sidx].lock();
        if let Some(bucket) = guard.authors.get_mut(target_did) {
            bucket.prune(medium_cutoff);
            bucket.last_seen_us = now_us;
            bucket.last_focal_uri = Some(focal_uri.to_string());
            bucket.counts_short(short_cutoff)
        } else {
            (0, 0, 0)
        }
    }

    fn check_distress_trigger(
        &self,
        author_did: &str,
        now_us: u64,
        explicit_focal: Option<&str>,
    ) -> ObserveOutcome {
        let short_cutoff = now_us.saturating_sub(duration_to_us(self.config.short_window));
        let medium_cutoff = now_us.saturating_sub(duration_to_us(self.config.medium_window));
        let (distress, focal_opt) = {
            let sidx = shard_idx(author_did);
            let mut guard = self.author_shards[sidx].lock();
            if let Some(b) = guard.authors.get_mut(author_did) {
                b.prune(medium_cutoff);
                let counts = b.counts_short(short_cutoff);
                let focal = explicit_focal
                    .map(ToString::to_string)
                    .or_else(|| b.last_focal_uri.clone());
                (counts, focal)
            } else {
                ((0, 0, 0), explicit_focal.map(ToString::to_string))
            }
        };

        let Some(focal_uri) = focal_opt else {
            return ObserveOutcome::DefensiveSignal(None);
        };
        let sidx = shard_idx(&focal_uri);
        let mut post_guard = self.post_shards[sidx].lock();
        let Some(bucket) = post_guard.posts.get_mut(&focal_uri) else {
            return ObserveOutcome::DefensiveSignal(None);
        };
        let cooldown_us = duration_to_us(self.config.enrich_cooldown);
        let candidate =
            self.maybe_build_candidate(bucket, distress, now_us, short_cutoff, cooldown_us);
        ObserveOutcome::DefensiveSignal(candidate)
    }

    fn maybe_build_candidate(
        &self,
        bucket: &mut PostBucket,
        distress: (usize, usize, usize),
        now_us: u64,
        short_cutoff: u64,
        cooldown_us: u64,
    ) -> Option<NetworkSurgeCandidate> {
        let (unique_authors, replies, quotes, likes, reposts) = bucket.count_short(short_cutoff);
        let distress_total = distress
            .0
            .saturating_add(distress.1)
            .saturating_add(distress.2);
        let has_distress = distress_total >= self.config.defensive_distress_threshold;
        let has_quote_surge = quotes >= MIN_QUOTES_RATIO_BYPASS;
        let effective_min_authors = if has_distress {
            (self.config.min_unique_authors / 2).max(2)
        } else {
            self.config.min_unique_authors
        };
        if unique_authors < effective_min_authors {
            return None;
        }

        let interactions_total = replies.saturating_add(quotes) as f64;
        let all_eng = interactions_total + likes.saturating_add(reposts) as f64;
        let streaming_share = if all_eng > 0.0 {
            interactions_total / all_eng
        } else {
            1.0
        };
        if !has_distress && !has_quote_surge && streaming_share < self.config.min_streaming_ratio {
            return None;
        }

        let direct_ratio =
            compute_direct_ratio(replies as u64, quotes as u64, likes as u64, reposts as u64);
        let burst_z = compute_burst_acceleration(
            replies.saturating_add(quotes),
            self.config.short_window.as_secs_f64(),
            bucket.events.len(),
            self.config.medium_window.as_secs_f64(),
        );
        // Optimistic OON baseline at Stage 1 (refined strictly via getRelationships in Stage 2).
        let structural = compute_structural_score(
            unique_authors,
            0.75,
            direct_ratio,
            0.0,
            1.0,
            burst_z,
            distress_total,
            TargetTier::Unknown,
        );
        if !has_distress && !has_quote_surge && structural < self.config.stage1_trigger_score {
            return None;
        }

        if let Some(last) = bucket.last_emitted_us {
            let within_cooldown = now_us.saturating_sub(last) < cooldown_us;
            let doubled_authors = bucket.last_emitted_authors > 0
                && unique_authors >= bucket.last_emitted_authors.saturating_mul(2);
            if within_cooldown && !doubled_authors {
                return None;
            }
        }
        bucket.last_emitted_us = Some(now_us);
        bucket.last_emitted_authors = unique_authors;

        let metrics = StormMetrics {
            unique_authors_short: unique_authors,
            unique_oon_authors_short: unique_authors,
            reply_count_short: replies,
            quote_count_short: quotes,
            streaming_likes_short: likes,
            streaming_reposts_short: reposts,
            out_of_network_ratio: 0.75,
            direct_ratio,
            dunk_ratio: 0.0,
            power_asymmetry: 1.0,
            burst_z_score: burst_z,
            defensive_blocks: distress.0,
            defensive_detaches: distress.1,
            defensive_threadgates: distress.2,
            vulnerability_weight: 1.0,
            structural_score: structural,
            composite_score: structural,
        };

        Some(NetworkSurgeCandidate {
            target_did: bucket.target_did.clone(),
            focal_post_uri: bucket.focal_post_uri.clone(),
            observed_at_us: now_us,
            initial_metrics: metrics,
            sample_replies: bucket.samples.iter().cloned().collect(),
            recent_quote_uris: bucket.recent_quote_uris.iter().cloned().rev().collect(),
        })
    }

    /// Prunes idle posts and author distress buckets older than `medium_window`, returning removed count.
    pub fn prune_stale(&self, now_us: u64) -> usize {
        let medium_cutoff = now_us.saturating_sub(duration_to_us(self.config.medium_window));
        let mut removed = 0usize;
        for shard in &self.post_shards {
            let mut guard = shard.lock();
            guard.posts.retain(|_, b| {
                b.prune(medium_cutoff);
                let keep = b.last_seen_us >= medium_cutoff
                    && (!b.events.is_empty() || !b.live_likes.is_empty());
                if !keep {
                    removed += 1;
                    self.tracked_posts_total.fetch_sub(1, Ordering::Relaxed);
                }
                keep
            });
        }
        for shard in &self.author_shards {
            let mut guard = shard.lock();
            guard.authors.retain(|_, b| {
                b.prune(medium_cutoff);
                let keep = b.last_seen_us >= medium_cutoff;
                if !keep {
                    self.tracked_authors_total.fetch_sub(1, Ordering::Relaxed);
                }
                keep
            });
        }
        removed
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;

    fn mk_reply(
        author: &str,
        target: &str,
        post_rkey: &str,
        ts_us: u64,
        text: &str,
    ) -> JetstreamCommit {
        JetstreamCommit {
            did: author.to_string(),
            time_us: ts_us,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("rep-{author}-{post_rkey}-{ts_us}"),
            operation: CommitOperation::Create,
            cid: Some("bafy1".to_string()),
            record: Some(json!({
                "$type": "app.bsky.feed.post",
                "text": text,
                "reply": {
                    "root": { "uri": format!("at://{target}/app.bsky.feed.post/{post_rkey}"), "cid": "c1" },
                    "parent": { "uri": format!("at://{target}/app.bsky.feed.post/{post_rkey}"), "cid": "c1" }
                }
            })),
        }
    }

    fn mk_like(author: &str, subject_uri: &str, ts_us: u64) -> JetstreamCommit {
        JetstreamCommit {
            did: author.to_string(),
            time_us: ts_us,
            collection: "app.bsky.feed.like".to_string(),
            rkey: format!("like-{ts_us}"),
            operation: CommitOperation::Create,
            cid: None,
            record: Some(json!({
                "subject": { "uri": subject_uri, "cid": "c1" }
            })),
        }
    }

    #[test]
    fn cold_likes_allocate_zero_entries_and_benign_likes_suppress_surge() {
        let tracker = NetworkTracker::new(
            RadarConfig::default()
                .with_min_unique_authors(4)
                .with_thresholds(0.30, 0.62, 0.68),
        );
        // Cold likes on unseen posts must never enter tracked_posts
        for i in 0..20 {
            let res = tracker.observe_commit(&mk_like(
                &format!("did:plc:u{i}"),
                &format!("at://did:plc:other/app.bsky.feed.post/{i}"),
                1_000_000 + i,
            ));
            assert_eq!(res, ObserveOutcome::Ignored);
        }
        assert_eq!(tracker.snapshot().tracked_posts, 0);

        // Hot post receiving 20 likes and only 4 replies -> low streaming ratio suppresses Stage 1
        let focal = "at://did:plc:alice/app.bsky.feed.post/viral1";
        let _ = tracker.observe_commit(&mk_reply(
            "did:plc:r1",
            "did:plc:alice",
            "viral1",
            2_000_000,
            "cute",
        ));
        for i in 0..20 {
            assert_eq!(
                tracker.observe_commit(&mk_like(&format!("did:plc:l{i}"), focal, 2_000_100 + i)),
                ObserveOutcome::HotEngagementUpdated
            );
        }
        for i in 2..=4 {
            let res = tracker.observe_commit(&mk_reply(
                &format!("did:plc:r{i}"),
                "did:plc:alice",
                "viral1",
                2_000_200 + i,
                "nice!",
            ));
            assert_eq!(res, ObserveOutcome::PostInteraction(None));
        }
    }

    #[test]
    fn surge_distress_cooldown_bounds_and_prune() {
        let mut cfg = RadarConfig::default()
            .with_short_window(Duration::from_secs(60))
            .with_medium_window(Duration::from_secs(300))
            .with_min_unique_authors(4)
            .with_table_caps(20, 20)
            .with_thresholds(0.25, 0.60, 0.65);
        cfg.enrich_cooldown = Duration::from_secs(600);
        let tracker = NetworkTracker::new(cfg);

        // Quote post + replies trigger Stage 1 candidate
        let quote = JetstreamCommit {
            did: "did:plc:cat".to_string(),
            time_us: 10_000_000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: "qt1".to_string(),
            operation: CommitOperation::Create,
            cid: None,
            record: Some(json!({
                "text": "Look at this take",
                "embed": {
                    "$type": "app.bsky.embed.record",
                    "record": { "uri": "at://did:plc:bob/app.bsky.feed.post/3khot", "cid": "c1" }
                }
            })),
        };
        let _ = tracker.observe_commit(&quote);
        let mut triggered = None;
        for i in 1..=4 {
            if let ObserveOutcome::PostInteraction(Some(c)) = tracker.observe_commit(&mk_reply(
                &format!("did:plc:o{i}"),
                "did:plc:bob",
                "3khot",
                10_000_100 + i,
                "Ratio!",
            )) {
                triggered = Some(c);
            }
        }
        let cand = triggered.expect("Stage 1 should trigger");
        assert_eq!(cand.target_did, "did:plc:bob");
        assert_eq!(cand.recent_quote_uris.len(), 1);
        assert_eq!(cand.initial_metrics.unique_authors_short, 4);

        // Cooldown prevents immediate re-trigger while authors < 2x (4 -> 5, 6, 7)
        for idx in 9..=10 {
            assert_eq!(
                tracker.observe_commit(&mk_reply(
                    &format!("did:plc:o{idx}"),
                    "did:plc:bob",
                    "3khot",
                    10_000_500 + idx,
                    "more"
                )),
                ObserveOutcome::PostInteraction(None)
            );
        }
        // Reaching 2x unique authors (8 >= 4 * 2) bypasses enrich_cooldown for escalating storms
        let escalated = tracker.observe_commit(&mk_reply(
            "did:plc:o11",
            "did:plc:bob",
            "3khot",
            10_000_520,
            "escalating",
        ));
        match escalated {
            ObserveOutcome::PostInteraction(Some(c2)) => {
                assert_eq!(c2.initial_metrics.unique_authors_short, 8);
            }
            other => panic!("expected 2x author escalation bypass, got {other:?}"),
        }

        // Defensive block + deduped postgate + threadgate lower threshold for alice
        let _ = tracker.observe_commit(&mk_reply(
            "did:plc:a1",
            "did:plc:alice",
            "3kdist",
            20_000_000,
            "mean",
        ));
        let _ = tracker.observe_commit(&mk_reply(
            "did:plc:a2",
            "did:plc:alice",
            "3kdist",
            20_000_010,
            "mean",
        ));
        let block = JetstreamCommit {
            did: "did:plc:alice".to_string(),
            time_us: 20_000_020,
            collection: "app.bsky.graph.block".to_string(),
            rkey: "b1".to_string(),
            operation: CommitOperation::Create,
            cid: None,
            record: Some(json!({ "subject": "did:plc:a1" })),
        };
        let pg = JetstreamCommit {
            did: "did:plc:alice".to_string(),
            time_us: 20_000_030,
            collection: "app.bsky.feed.postgate".to_string(),
            rkey: "3kdist".to_string(),
            operation: CommitOperation::Update,
            cid: None,
            record: Some(json!({
                "post": "at://did:plc:alice/app.bsky.feed.post/3kdist",
                "detachedEmbeddingUris": ["at://did:plc:cat/app.bsky.feed.post/qt2"]
            })),
        };
        let _ = tracker.observe_commit(&block);
        let res = tracker.observe_commit(&pg);
        assert!(matches!(res, ObserveOutcome::DefensiveSignal(Some(_))));

        // Eviction under bounded capacity and pruning after medium window
        for i in 0..80 {
            let _ = tracker.observe_commit(&mk_reply(
                &format!("did:plc:spam{i}"),
                "did:plc:many",
                &format!("p{i}"),
                30_000_000 + i,
                "hi",
            ));
        }
        assert!(tracker.snapshot().tracked_posts <= 64);
        assert!(tracker.prune_stale(500_000_000) > 0);
        assert_eq!(tracker.snapshot().tracked_posts, 0);
    }

    #[test]
    fn subthread_reply_targets_parent_post_not_thread_root() {
        let tracker = NetworkTracker::new(
            RadarConfig::default()
                .with_min_unique_authors(2)
                .with_thresholds(0.10, 0.50, 0.60),
        );
        let root_uri = "at://did:plc:threadop/app.bsky.feed.post/3kroot";
        let parent_uri = "at://did:plc:subreply/app.bsky.feed.post/3ksub";

        let mut last = ObserveOutcome::Ignored;
        for idx in 1..=2 {
            let commit = JetstreamCommit {
                did: format!("did:plc:dogpiler{idx}"),
                time_us: 100_000 + idx,
                collection: "app.bsky.feed.post".to_string(),
                rkey: format!("sr{idx}"),
                operation: CommitOperation::Create,
                cid: Some("cid".to_string()),
                record: Some(json!({
                    "$type": "app.bsky.feed.post",
                    "text": "Replying directly to the subthread comment",
                    "reply": {
                        "root": { "uri": root_uri, "cid": "croot" },
                        "parent": { "uri": parent_uri, "cid": "cparent" }
                    }
                })),
            };
            last = tracker.observe_commit(&commit);
        }

        match last {
            ObserveOutcome::PostInteraction(Some(cand)) => {
                assert_eq!(cand.focal_post_uri, parent_uri);
                assert_eq!(cand.target_did, "did:plc:subreply");
            }
            other => panic!("expected subthread parent candidate, got {other:?}"),
        }
    }

    #[test]
    fn quote_surge_bypasses_streaming_ratio_on_liked_post() {
        let tracker = NetworkTracker::new(RadarConfig::default());
        let focal_uri = "at://did:plc:likedtarget/app.bsky.feed.post/3kliked";

        // Prior medium-window activity keeps short-window burst_z modest.
        for i in 0..6 {
            let _ = tracker.observe_commit(&mk_reply(
                &format!("did:plc:prior{i}"),
                "did:plc:likedtarget",
                "3kliked",
                300_000_000 + i,
                "earlier reply",
            ));
        }

        // 60 short-window live likes push streaming_share well below 0.30.
        let short_base = 1_900_000_000_u64;
        for i in 0..60 {
            assert_eq!(
                tracker.observe_commit(&mk_like(
                    &format!("did:plc:liker{i}"),
                    focal_uri,
                    short_base + i,
                )),
                ObserveOutcome::HotEngagementUpdated
            );
        }

        // 5 replies + 1 quote (6 unique authors, quotes == 1 < 2): blocked by streaming_ratio.
        for i in 0..5 {
            assert_eq!(
                tracker.observe_commit(&mk_reply(
                    &format!("did:plc:rep{i}"),
                    "did:plc:likedtarget",
                    "3kliked",
                    short_base + 100 + i,
                    "replying",
                )),
                ObserveOutcome::PostInteraction(None)
            );
        }

        let mk_quote = |author: &str, rkey: &str, ts_us: u64| JetstreamCommit {
            did: author.to_string(),
            time_us: ts_us,
            collection: "app.bsky.feed.post".to_string(),
            rkey: rkey.to_string(),
            operation: CommitOperation::Create,
            cid: None,
            record: Some(json!({
                "text": "Look at this post",
                "embed": {
                    "$type": "app.bsky.embed.record",
                    "record": { "uri": focal_uri, "cid": "c1" }
                }
            })),
        };

        assert_eq!(
            tracker.observe_commit(&mk_quote("did:plc:qt1", "q1", short_base + 200)),
            ObserveOutcome::PostInteraction(None)
        );

        // 2nd quote from a 7th unique author triggers quote-cascade bypass for Stage 2 getQuotes.
        match tracker.observe_commit(&mk_quote("did:plc:qt2", "q2", short_base + 201)) {
            ObserveOutcome::PostInteraction(Some(cand)) => {
                assert_eq!(cand.focal_post_uri, focal_uri);
                assert_eq!(cand.initial_metrics.quote_count_short, 2);
                assert_eq!(cand.recent_quote_uris.len(), 2);
                assert_eq!(cand.initial_metrics.unique_authors_short, 7);
            }
            other => panic!("expected quote-surge candidate, got {other:?}"),
        }
    }
}
