//! Polite rate-limited AppView enrichment (`getPosts`, `getQuotes`, `getProfiles`, `getRelationships`).
//!
//! Fetches batched focal/catalyst post metrics, target & catalyst follower counts, and exact
//! pairwise out-of-network follow status for Stage 2 surge verification without holding a
//! global social graph in memory.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use serde::Deserialize;
use skybase::appview::AppViewClient;

use crate::types::{ActorSummary, PostEngagementSnapshot, RelationshipStatus};

const MAX_POSTS_PER_BATCH: usize = 25;
const MAX_PROFILES_PER_BATCH: usize = 25;
const MAX_RELATIONSHIPS_OTHERS: usize = 30;

/// Abstract read-only AppView interface used during Stage 2 network storm verification.
#[async_trait]
pub trait NetworkEnricher: Send + Sync {
    /// Fetches cumulative engagement snapshots for up to `25` AT-URIs per batch.
    async fn fetch_post_engagements(&self, uris: &[String]) -> Vec<PostEngagementSnapshot>;

    /// Fetches lightweight profile metadata for up to `25` DIDs per batch.
    async fn fetch_actor_profiles(&self, dids: &[String]) -> Vec<ActorSummary>;

    /// Checks bidirectional follow relationships between `target_did` and up to `30` responders.
    async fn fetch_relationships(
        &self,
        target_did: &str,
        others: &[String],
    ) -> Vec<RelationshipStatus>;

    /// Fetches recent quote-posts referencing `focal_post_uri` via `app.bsky.feed.getQuotes`.
    async fn fetch_post_quotes(
        &self,
        _focal_post_uri: &str,
        _limit: usize,
    ) -> Vec<PostEngagementSnapshot> {
        Vec::new()
    }
}

/// No-op enricher returning empty vectors (useful when operating strictly offline).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopNetworkEnricher;

#[async_trait]
impl NetworkEnricher for NoopNetworkEnricher {
    async fn fetch_post_engagements(&self, _uris: &[String]) -> Vec<PostEngagementSnapshot> {
        Vec::new()
    }

    async fn fetch_actor_profiles(&self, _dids: &[String]) -> Vec<ActorSummary> {
        Vec::new()
    }

    async fn fetch_relationships(
        &self,
        _target_did: &str,
        _others: &[String],
    ) -> Vec<RelationshipStatus> {
        Vec::new()
    }
}

/// Deterministic mock [`NetworkEnricher`] for unit and hermetic pipeline integration tests.
#[derive(Debug, Default, Clone)]
pub struct MockNetworkEnricher {
    posts: Arc<RwLock<HashMap<String, PostEngagementSnapshot>>>,
    quotes: Arc<RwLock<HashMap<String, Vec<PostEngagementSnapshot>>>>,
    profiles: Arc<RwLock<HashMap<String, ActorSummary>>>,
    relationships: Arc<RwLock<HashMap<(String, String), RelationshipStatus>>>,
}

impl MockNetworkEnricher {
    /// Creates an empty [`MockNetworkEnricher`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or replaces a mock post engagement snapshot.
    pub fn set_post(&self, snapshot: PostEngagementSnapshot) {
        self.posts.write().insert(snapshot.uri.clone(), snapshot);
    }

    /// Inserts or replaces mock quote-posts returned by [`Self::fetch_post_quotes`] for `focal_post_uri`.
    pub fn set_quotes(
        &self,
        focal_post_uri: impl Into<String>,
        quotes: Vec<PostEngagementSnapshot>,
    ) {
        self.quotes.write().insert(focal_post_uri.into(), quotes);
    }

    /// Inserts or replaces a mock actor profile summary.
    pub fn set_profile(&self, profile: ActorSummary) {
        self.profiles.write().insert(profile.did.clone(), profile);
    }

    /// Inserts or replaces a mock follow relationship between `target_did` and `other_did`.
    pub fn set_relationship(
        &self,
        target_did: impl Into<String>,
        other_did: impl Into<String>,
        target_follows_responder: bool,
        responder_follows_target: bool,
    ) {
        let t = target_did.into();
        let o = other_did.into();
        self.relationships.write().insert(
            (t, o.clone()),
            RelationshipStatus {
                did: o,
                target_follows_responder,
                responder_follows_target,
            },
        );
    }
}

#[async_trait]
impl NetworkEnricher for MockNetworkEnricher {
    async fn fetch_post_engagements(&self, uris: &[String]) -> Vec<PostEngagementSnapshot> {
        let guard = self.posts.read();
        uris.iter().filter_map(|u| guard.get(u).cloned()).collect()
    }

    async fn fetch_actor_profiles(&self, dids: &[String]) -> Vec<ActorSummary> {
        let guard = self.profiles.read();
        dids.iter().filter_map(|d| guard.get(d).cloned()).collect()
    }

    async fn fetch_relationships(
        &self,
        target_did: &str,
        others: &[String],
    ) -> Vec<RelationshipStatus> {
        let guard = self.relationships.read();
        others
            .iter()
            .take(MAX_RELATIONSHIPS_OTHERS)
            .map(|other| {
                guard
                    .get(&(target_did.to_string(), other.clone()))
                    .cloned()
                    .unwrap_or_else(|| RelationshipStatus {
                        did: other.clone(),
                        target_follows_responder: false,
                        responder_follows_target: false,
                    })
            })
            .collect()
    }

    async fn fetch_post_quotes(
        &self,
        focal_post_uri: &str,
        limit: usize,
    ) -> Vec<PostEngagementSnapshot> {
        let guard = self.quotes.read();
        guard
            .get(focal_post_uri)
            .map(|list| {
                list.iter()
                    .take(limit.min(MAX_POSTS_PER_BATCH))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Debug, Deserialize)]
struct GetPostsResponse {
    #[serde(default)]
    posts: Vec<GetPostsView>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetPostsView {
    uri: String,
    author: Option<GetPostAuthor>,
    record: Option<serde_json::Value>,
    #[serde(default)]
    reply_count: Option<u64>,
    #[serde(default)]
    repost_count: Option<u64>,
    #[serde(default)]
    like_count: Option<u64>,
    #[serde(default)]
    quote_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetPostAuthor {
    did: String,
    handle: Option<String>,
    followers_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct GetProfilesResponse {
    #[serde(default)]
    profiles: Vec<GetProfileItem>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetProfileItem {
    did: String,
    handle: Option<String>,
    display_name: Option<String>,
    followers_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct GetRelationshipsResponse {
    #[serde(default)]
    relationships: Vec<RelationshipUnionItem>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RelationshipUnionItem {
    did: Option<String>,
    following: Option<String>,
    followed_by: Option<String>,
}

/// Token-bucket rate-limited public AppView XRPC enricher backed by [`skybase::appview::AppViewClient`].
#[derive(Debug, Clone)]
pub struct PublicAppViewEnricher {
    client: AppViewClient,
    min_interval: Duration,
    next_slot: Arc<Mutex<Instant>>,
}

impl PublicAppViewEnricher {
    /// Creates a new [`PublicAppViewEnricher`] targeting `appview_endpoint` capped at `requests_per_sec`.
    #[must_use]
    pub fn new(appview_endpoint: impl Into<String>, requests_per_sec: f64) -> Self {
        let clamped_rps = if requests_per_sec.is_finite() && requests_per_sec > 0.0 {
            requests_per_sec.clamp(0.2, 50.0)
        } else {
            4.0
        };
        let min_interval = Duration::from_secs_f64(1.0 / clamped_rps);
        Self {
            client: AppViewClient::with_endpoint(appview_endpoint),
            min_interval,
            next_slot: Arc::new(Mutex::new(Instant::now())),
        }
    }

    async fn wait_rate_slot(&self) {
        let wait_dur = {
            let now = Instant::now();
            let mut slot = self.next_slot.lock();
            if *slot > now {
                let wait = slot.saturating_duration_since(now);
                *slot += self.min_interval;
                wait
            } else {
                *slot = now + self.min_interval;
                Duration::ZERO
            }
        };
        if !wait_dur.is_zero() {
            tokio::time::sleep(wait_dur).await;
        }
    }

    fn map_post_view(p: GetPostsView) -> PostEngagementSnapshot {
        let author_did = p.author.as_ref().map(|a| a.did.clone()).unwrap_or_default();
        let author_handle = p.author.as_ref().and_then(|a| a.handle.clone());
        let author_followers = p.author.as_ref().and_then(|a| a.followers_count);
        let text = p
            .record
            .as_ref()
            .and_then(|r| r.get("text"))
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_string();
        PostEngagementSnapshot {
            uri: p.uri,
            author_did,
            author_handle,
            author_followers,
            text,
            reply_count: p.reply_count.unwrap_or(0),
            repost_count: p.repost_count.unwrap_or(0),
            like_count: p.like_count.unwrap_or(0),
            quote_count: p.quote_count.unwrap_or(0),
        }
    }
}

#[async_trait]
impl NetworkEnricher for PublicAppViewEnricher {
    async fn fetch_post_engagements(&self, uris: &[String]) -> Vec<PostEngagementSnapshot> {
        let mut out = Vec::new();
        for chunk in uris.chunks(MAX_POSTS_PER_BATCH) {
            let query: Vec<(&str, String)> = chunk
                .iter()
                .map(String::as_str)
                .map(str::trim)
                .filter(|s| s.starts_with("at://"))
                .map(|s| ("uris", s.to_string()))
                .collect();
            if query.is_empty() {
                continue;
            }
            self.wait_rate_slot().await;
            if let Ok(resp) = self
                .client
                .get::<GetPostsResponse>("app.bsky.feed.getPosts", &query)
                .await
            {
                out.extend(resp.posts.into_iter().map(Self::map_post_view));
            }
        }
        out
    }

    async fn fetch_post_quotes(
        &self,
        focal_post_uri: &str,
        limit: usize,
    ) -> Vec<PostEngagementSnapshot> {
        let clean_uri = focal_post_uri.trim();
        if !clean_uri.starts_with("at://") {
            return Vec::new();
        }
        let clamped_limit = limit.clamp(1, MAX_POSTS_PER_BATCH);
        let query = vec![
            ("uri", clean_uri.to_string()),
            ("limit", clamped_limit.to_string()),
        ];
        self.wait_rate_slot().await;
        match self
            .client
            .get::<GetPostsResponse>("app.bsky.feed.getQuotes", &query)
            .await
        {
            Ok(resp) => resp.posts.into_iter().map(Self::map_post_view).collect(),
            Err(_) => Vec::new(),
        }
    }

    async fn fetch_actor_profiles(&self, dids: &[String]) -> Vec<ActorSummary> {
        let mut out = Vec::new();
        for chunk in dids.chunks(MAX_PROFILES_PER_BATCH) {
            let query: Vec<(&str, String)> = chunk
                .iter()
                .map(String::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| ("actors", s.to_string()))
                .collect();
            if query.is_empty() {
                continue;
            }
            self.wait_rate_slot().await;
            if let Ok(resp) = self
                .client
                .get::<GetProfilesResponse>("app.bsky.actor.getProfiles", &query)
                .await
            {
                for item in resp.profiles {
                    out.push(ActorSummary {
                        did: item.did,
                        handle: item.handle,
                        display_name: item.display_name,
                        followers_count: item.followers_count,
                    });
                }
            }
        }
        out
    }

    async fn fetch_relationships(
        &self,
        target_did: &str,
        others: &[String],
    ) -> Vec<RelationshipStatus> {
        let clean_target = target_did.trim();
        if clean_target.is_empty() || others.is_empty() {
            return Vec::new();
        }
        let mut query = Vec::with_capacity(1 + others.len().min(MAX_RELATIONSHIPS_OTHERS));
        query.push(("actor", clean_target.to_string()));
        for other in others.iter().take(MAX_RELATIONSHIPS_OTHERS) {
            let trimmed = other.trim();
            if !trimmed.is_empty() {
                query.push(("others", trimmed.to_string()));
            }
        }
        if query.len() <= 1 {
            return Vec::new();
        }
        self.wait_rate_slot().await;
        match self
            .client
            .get::<GetRelationshipsResponse>("app.bsky.graph.getRelationships", &query)
            .await
        {
            Ok(resp) => resp
                .relationships
                .into_iter()
                .filter_map(|item| {
                    let did = item.did?;
                    Some(RelationshipStatus {
                        did,
                        target_follows_responder: item
                            .following
                            .as_deref()
                            .is_some_and(|s| !s.is_empty()),
                        responder_follows_target: item
                            .followed_by
                            .as_deref()
                            .is_some_and(|s| !s.is_empty()),
                    })
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn noop_and_mock_enrichers_work() {
        let noop = NoopNetworkEnricher;
        assert!(noop.fetch_post_engagements(&[]).await.is_empty());
        assert!(noop
            .fetch_post_quotes("at://did:plc:alice/app.bsky.feed.post/1", 10)
            .await
            .is_empty());
        assert!(noop.fetch_actor_profiles(&[]).await.is_empty());
        assert!(noop.fetch_relationships("a", &[]).await.is_empty());

        let mock = MockNetworkEnricher::new();
        mock.set_post(PostEngagementSnapshot {
            uri: "at://did:plc:alice/app.bsky.feed.post/1".to_string(),
            author_did: "did:plc:alice".to_string(),
            like_count: 5,
            reply_count: 40,
            ..Default::default()
        });
        mock.set_quotes(
            "at://did:plc:alice/app.bsky.feed.post/1",
            vec![PostEngagementSnapshot {
                uri: "at://did:plc:qt/app.bsky.feed.post/2".to_string(),
                author_did: "did:plc:qt".to_string(),
                like_count: 250,
                ..Default::default()
            }],
        );
        mock.set_profile(ActorSummary {
            did: "did:plc:alice".to_string(),
            handle: Some("alice.bsky.social".to_string()),
            followers_count: Some(150),
            ..Default::default()
        });
        mock.set_relationship("did:plc:alice", "did:plc:friend", false, true);

        assert_eq!(
            mock.fetch_post_engagements(&["at://did:plc:alice/app.bsky.feed.post/1".to_string()])
                .await
                .len(),
            1
        );
        assert_eq!(
            mock.fetch_post_quotes("at://did:plc:alice/app.bsky.feed.post/1", 10)
                .await
                .len(),
            1
        );
        assert_eq!(
            mock.fetch_actor_profiles(&["did:plc:alice".to_string()])
                .await
                .len(),
            1
        );
        let rels = mock
            .fetch_relationships(
                "did:plc:alice",
                &["did:plc:friend".to_string(), "did:plc:stranger".to_string()],
            )
            .await;
        assert_eq!(rels.len(), 2);
        assert!(!rels[0].is_out_of_network());
        assert!(rels[1].is_out_of_network());
    }

    #[tokio::test]
    async fn public_appview_enricher_queries_posts_profiles_and_relationships() {
        let srv = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getPosts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "posts": [{
                    "uri": "at://did:plc:alice/app.bsky.feed.post/1",
                    "author": { "did": "did:plc:alice", "handle": "alice.bsky.social", "followersCount": 300 },
                    "record": { "text": "hello world" },
                    "replyCount": 42,
                    "repostCount": 2,
                    "likeCount": 4,
                    "quoteCount": 9
                }]
            })))
            .mount(&srv)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.actor.getProfiles"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "profiles": [{
                    "did": "did:plc:alice",
                    "handle": "alice.bsky.social",
                    "displayName": "Alice",
                    "followersCount": 300
                }]
            })))
            .mount(&srv)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.graph.getRelationships"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "actor": "did:plc:alice",
                "relationships": [
                    { "$type": "app.bsky.graph.defs#relationship", "did": "did:plc:r1" },
                    { "$type": "app.bsky.graph.defs#relationship", "did": "did:plc:r2", "followedBy": "at://did:plc:r2/app.bsky.graph.follow/1" },
                    { "$type": "app.bsky.graph.defs#notFoundActor", "actor": "did:plc:gone", "notFound": true }
                ]
            })))
            .mount(&srv)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getQuotes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "posts": [{
                    "uri": "at://did:plc:big/app.bsky.feed.post/qt99",
                    "author": { "did": "did:plc:big", "handle": "big.bsky.social", "followersCount": 50000 },
                    "record": { "text": "Dunk quote" },
                    "replyCount": 12,
                    "repostCount": 40,
                    "likeCount": 600,
                    "quoteCount": 3
                }]
            })))
            .mount(&srv)
            .await;

        let enricher = PublicAppViewEnricher::new(srv.uri(), 40.0);
        let posts = enricher
            .fetch_post_engagements(&[
                "not-at-uri".to_string(),
                "at://did:plc:alice/app.bsky.feed.post/1".to_string(),
            ])
            .await;
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].reply_count, 42);
        assert_eq!(posts[0].text, "hello world");

        let quotes = enricher
            .fetch_post_quotes("at://did:plc:alice/app.bsky.feed.post/1", 25)
            .await;
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].like_count, 600);
        assert!(enricher
            .fetch_post_quotes("not-at-uri", 10)
            .await
            .is_empty());

        let profiles = enricher
            .fetch_actor_profiles(&["did:plc:alice".to_string()])
            .await;
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].followers_count, Some(300));

        let rels = enricher
            .fetch_relationships(
                "did:plc:alice",
                &["did:plc:r1".to_string(), "did:plc:r2".to_string()],
            )
            .await;
        assert_eq!(rels.len(), 2);
        assert!(rels[0].is_out_of_network());
        assert!(!rels[1].is_out_of_network());
        assert!(enricher.fetch_relationships("", &[]).await.is_empty());
    }
}
