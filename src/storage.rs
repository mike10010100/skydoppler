//! Embedded SQLite WAL projection store for detected storms, weather history, and Jetstream cursor.

use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};

use crate::error::Result;
use crate::redact::opaque_cell_id;
use crate::time::{i64_to_us, us_to_i64};
use crate::types::{
    CatalystInfo, NetworkStorm, StormCategory, StormMetrics, StormSeverity, StormStanceVerdict,
    StormStatus, TargetTier, WeatherPoint,
};

const STORM_COLUMNS: &str = "id, cell_id, target_did, target_handle, target_followers, target_tier, focal_post_uri, focal_post_snippet, status, severity, category, peak_score, radar_angle_deg, radar_radius, started_at_us, updated_at_us, resolved_at_us, metrics_json, top_catalyst_json, stance_json";

/// Thread-safe SQLite WAL store for `skydoppler` operational state.
#[derive(Debug, Clone)]
pub struct StormStore {
    conn: Arc<Mutex<Connection>>,
}

impl StormStore {
    /// Opens a persistent SQLite WAL database at `path` and runs schema initialization.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on SQLite open or schema failure.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::init_with_connection(conn)
    }

    /// Opens an isolated in-memory SQLite database for hermetic tests.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on SQLite init failure.
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::init_with_connection(conn)
    }

    fn init_with_connection(conn: Connection) -> Result<Self> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;
             PRAGMA temp_store = MEMORY;
             PRAGMA foreign_keys = ON;

             CREATE TABLE IF NOT EXISTS network_storms (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 cell_id TEXT NOT NULL UNIQUE,
                 target_did TEXT NOT NULL,
                 target_handle TEXT,
                 target_followers INTEGER,
                 target_tier TEXT NOT NULL,
                 focal_post_uri TEXT NOT NULL UNIQUE,
                 focal_post_snippet TEXT NOT NULL DEFAULT '',
                 status TEXT NOT NULL,
                 severity TEXT NOT NULL,
                 category TEXT NOT NULL,
                 peak_score REAL NOT NULL DEFAULT 0.0,
                 radar_angle_deg REAL NOT NULL DEFAULT 0.0,
                 radar_radius REAL NOT NULL DEFAULT 0.5,
                 started_at_us INTEGER NOT NULL,
                 updated_at_us INTEGER NOT NULL,
                 resolved_at_us INTEGER,
                 metrics_json TEXT NOT NULL,
                 top_catalyst_json TEXT,
                 stance_json TEXT
             );

             CREATE INDEX IF NOT EXISTS idx_network_storms_status_updated
                 ON network_storms (status, updated_at_us DESC, id DESC);
             CREATE INDEX IF NOT EXISTS idx_network_storms_target
                 ON network_storms (target_did, updated_at_us DESC);

             CREATE TABLE IF NOT EXISTS weather_snapshots (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 timestamp_us INTEGER NOT NULL,
                 weather_index REAL NOT NULL,
                 active_storms INTEGER NOT NULL,
                 commits_per_sec REAL NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_weather_snapshots_ts
                 ON weather_snapshots (timestamp_us DESC);

             CREATE TABLE IF NOT EXISTS _skydoppler_meta (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );",
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Inserts or updates a storm by `focal_post_uri`, preserving initial `started_at_us`,
    /// monotonic `peak_score`, and reactivation if a recurring surge hits an archived post.
    ///
    /// Returns `(storm_id, is_new, is_escalation)`.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] or serialization failure.
    pub fn upsert_active_storm(&self, storm: &NetworkStorm) -> Result<(i64, bool, bool)> {
        let cell_id = if storm.cell_id.trim().is_empty() {
            opaque_cell_id(&storm.focal_post_uri)
        } else {
            storm.cell_id.clone()
        };
        let metrics_json = serde_json::to_string(&storm.metrics)?;
        let catalyst_json = storm
            .top_catalyst
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let stance_json = storm
            .stance
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;

        let conn = self.conn.lock();
        let existing: Option<(i64, String, String, f64)> = conn
            .query_row(
                "SELECT id, status, severity, peak_score FROM network_storms WHERE focal_post_uri = ?1;",
                params![&storm.focal_post_uri],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;

        if let Some((row_id, prev_status_str, prev_sev_str, prev_peak)) = existing {
            let prev_sev = prev_sev_str.parse::<StormSeverity>().unwrap_or_default();
            let prev_status = prev_status_str.parse::<StormStatus>().unwrap_or_default();
            let is_escalation = prev_status != StormStatus::Active || storm.severity > prev_sev;
            let peak = prev_peak
                .max(storm.peak_score)
                .max(storm.metrics.composite_score);
            conn.execute(
                "UPDATE network_storms
                 SET cell_id = ?2,
                     target_did = ?3,
                     target_handle = COALESCE(?4, target_handle),
                     target_followers = COALESCE(?5, target_followers),
                     target_tier = ?6,
                     focal_post_snippet = CASE WHEN length(?7) > 0 THEN ?7 ELSE focal_post_snippet END,
                     status = 'active',
                     severity = ?8,
                     category = ?9,
                     peak_score = ?10,
                     radar_angle_deg = ?11,
                     radar_radius = ?12,
                     updated_at_us = ?13,
                     resolved_at_us = NULL,
                     metrics_json = ?14,
                     top_catalyst_json = COALESCE(?15, top_catalyst_json),
                     stance_json = COALESCE(?16, stance_json)
                 WHERE id = ?1;",
                params![
                    row_id,
                    cell_id,
                    &storm.target_did,
                    &storm.target_handle,
                    storm.target_followers.map(us_to_i64),
                    storm.target_tier.as_str(),
                    &storm.focal_post_snippet,
                    storm.severity.as_str(),
                    storm.category.as_str(),
                    peak,
                    storm.radar_angle_deg,
                    storm.radar_radius,
                    us_to_i64(storm.updated_at_us),
                    metrics_json,
                    catalyst_json,
                    stance_json,
                ],
            )?;
            Ok((row_id, false, is_escalation))
        } else {
            let peak = storm.peak_score.max(storm.metrics.composite_score);
            conn.execute(
                "INSERT INTO network_storms (
                    cell_id, target_did, target_handle, target_followers, target_tier,
                    focal_post_uri, focal_post_snippet, status, severity, category,
                    peak_score, radar_angle_deg, radar_radius, started_at_us, updated_at_us,
                    resolved_at_us, metrics_json, top_catalyst_json, stance_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'active', ?8, ?9, ?10, ?11, ?12, ?13, ?14, NULL, ?15, ?16, ?17);",
                params![
                    cell_id,
                    &storm.target_did,
                    &storm.target_handle,
                    storm.target_followers.map(us_to_i64),
                    storm.target_tier.as_str(),
                    &storm.focal_post_uri,
                    &storm.focal_post_snippet,
                    storm.severity.as_str(),
                    storm.category.as_str(),
                    peak,
                    storm.radar_angle_deg,
                    storm.radar_radius,
                    us_to_i64(storm.started_at_us),
                    us_to_i64(storm.updated_at_us),
                    metrics_json,
                    catalyst_json,
                    stance_json,
                ],
            )?;
            Ok((conn.last_insert_rowid(), true, false))
        }
    }

    /// Fetches a single storm by numeric database ID.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on query failure.
    pub fn get_storm(&self, id: i64) -> Result<Option<NetworkStorm>> {
        let conn = self.conn.lock();
        let sql = format!("SELECT {STORM_COLUMNS} FROM network_storms WHERE id = ?1;");
        let mut stmt = conn.prepare_cached(&sql)?;
        let res = stmt.query_row(params![id], map_storm).optional()?;
        Ok(res)
    }

    /// Fetches a single storm by opaque radar `cell_id`.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on query failure.
    pub fn get_storm_by_cell_id(&self, cell_id: &str) -> Result<Option<NetworkStorm>> {
        let conn = self.conn.lock();
        let sql = format!("SELECT {STORM_COLUMNS} FROM network_storms WHERE cell_id = ?1;");
        let mut stmt = conn.prepare_cached(&sql)?;
        let res = stmt.query_row(params![cell_id], map_storm).optional()?;
        Ok(res)
    }

    /// Lists storm cells ordered newest-first, optionally filtered by `active_only`.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on query failure.
    pub fn list_storms(&self, active_only: bool, limit: usize) -> Result<Vec<NetworkStorm>> {
        let cap = i64::try_from(limit.clamp(1, 500)).unwrap_or(50);
        let conn = self.conn.lock();
        let sql = if active_only {
            format!(
                "SELECT {STORM_COLUMNS} FROM network_storms
                 WHERE status = 'active'
                 ORDER BY updated_at_us DESC, id DESC LIMIT ?1;"
            )
        } else {
            format!(
                "SELECT {STORM_COLUMNS} FROM network_storms
                 ORDER BY updated_at_us DESC, id DESC LIMIT ?1;"
            )
        };
        let mut stmt = conn.prepare_cached(&sql)?;
        let rows = stmt.query_map(params![cap], map_storm)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// Updates a storm's status to `Resolved` or `Dismissed`, returning `true` if a row changed.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on update failure.
    pub fn update_storm_status(
        &self,
        storm_id: i64,
        status: StormStatus,
        resolved_at_us: Option<u64>,
    ) -> Result<bool> {
        let conn = self.conn.lock();
        let updated = conn.execute(
            "UPDATE network_storms SET status = ?2, resolved_at_us = ?3 WHERE id = ?1;",
            params![storm_id, status.as_str(), resolved_at_us.map(us_to_i64)],
        )?;
        Ok(updated > 0)
    }

    /// Transitions active storms whose `updated_at_us < cutoff_us` to `Resolved`, returning resolved `(id, cell_id)`.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on query/update failure.
    pub fn resolve_stale_storms(&self, cutoff_us: u64, now_us: u64) -> Result<Vec<(i64, String)>> {
        let conn = self.conn.lock();
        let mut sel = conn.prepare_cached(
            "SELECT id, cell_id FROM network_storms WHERE status = 'active' AND updated_at_us < ?1;",
        )?;
        let rows = sel.query_map(params![us_to_i64(cutoff_us)], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut expired = Vec::new();
        for r in rows {
            expired.push(r?);
        }
        if !expired.is_empty() {
            conn.execute(
                "UPDATE network_storms
                 SET status = 'resolved', resolved_at_us = ?2
                 WHERE status = 'active' AND updated_at_us < ?1;",
                params![us_to_i64(cutoff_us), us_to_i64(now_us)],
            )?;
        }
        Ok(expired)
    }

    /// Persists a periodic atmospheric [`WeatherPoint`].
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on insert failure.
    pub fn record_weather_snapshot(&self, point: &WeatherPoint) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO weather_snapshots (timestamp_us, weather_index, active_storms, commits_per_sec)
             VALUES (?1, ?2, ?3, ?4);",
            params![
                us_to_i64(point.timestamp_us),
                point.weather_index,
                i64::try_from(point.active_storms).unwrap_or(i64::MAX),
                point.commits_per_sec
            ],
        )?;
        Ok(())
    }

    /// Returns recent weather snapshots ordered oldest-to-newest for charting.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on query failure.
    pub fn list_weather_history(&self, limit: usize) -> Result<Vec<WeatherPoint>> {
        let cap = i64::try_from(limit.clamp(1, 500)).unwrap_or(100);
        let conn = self.conn.lock();
        let mut stmt = conn.prepare_cached(
            "SELECT timestamp_us, weather_index, active_storms, commits_per_sec
             FROM weather_snapshots
             ORDER BY timestamp_us DESC, id DESC LIMIT ?1;",
        )?;
        let rows = stmt.query_map(params![cap], |r| {
            Ok(WeatherPoint {
                timestamp_us: i64_to_us(r.get(0)?),
                weather_index: r.get(1)?,
                active_storms: usize::try_from(r.get::<_, i64>(2)?.max(0)).unwrap_or(0),
                commits_per_sec: r.get(3)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        out.reverse();
        Ok(out)
    }

    /// Prunes archived storms and old weather snapshots older than `cutoff_us`, returning rows removed.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on delete failure.
    pub fn prune_older_than(&self, cutoff_us: u64) -> Result<usize> {
        let cut = us_to_i64(cutoff_us);
        let conn = self.conn.lock();
        let d1 = conn.execute(
            "DELETE FROM network_storms WHERE status != 'active' AND updated_at_us < ?1;",
            params![cut],
        )?;
        let d2 = conn.execute(
            "DELETE FROM weather_snapshots WHERE timestamp_us < ?1;",
            params![cut],
        )?;
        Ok(d1.saturating_add(d2))
    }

    /// Saves the durable Jetstream cursor timestamp in microseconds.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on write failure.
    pub fn save_cursor_us(&self, cursor_us: u64) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO _skydoppler_meta (key, value) VALUES ('jetstream_cursor_us', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value;",
            params![cursor_us.to_string()],
        )?;
        Ok(())
    }

    /// Loads the persisted Jetstream cursor timestamp, if any.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on read failure.
    pub fn load_cursor_us(&self) -> Result<Option<u64>> {
        let conn = self.conn.lock();
        let raw: Option<String> = conn
            .query_row(
                "SELECT value FROM _skydoppler_meta WHERE key = 'jetstream_cursor_us';",
                [],
                |r| r.get(0),
            )
            .optional()?;
        Ok(raw.and_then(|s| s.parse::<u64>().ok()))
    }

    /// Saves arbitrary stats JSON into `_skydoppler_meta`.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on write failure.
    pub fn save_meta_json(&self, key: &str, json: &str) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO _skydoppler_meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value;",
            params![key, json],
        )?;
        Ok(())
    }

    /// Loads previously saved metadata JSON from `_skydoppler_meta`.
    ///
    /// # Errors
    /// Returns [`crate::SkydopplerError::Storage`] on read failure.
    pub fn load_meta_json(&self, key: &str) -> Result<Option<String>> {
        let conn = self.conn.lock();
        let res = conn
            .query_row(
                "SELECT value FROM _skydoppler_meta WHERE key = ?1;",
                params![key],
                |r| r.get(0),
            )
            .optional()?;
        Ok(res)
    }
}

fn map_storm(row: &rusqlite::Row<'_>) -> rusqlite::Result<NetworkStorm> {
    let tier_str: String = row.get(5)?;
    let status_str: String = row.get(8)?;
    let sev_str: String = row.get(9)?;
    let cat_str: String = row.get(10)?;
    let metrics_json: String = row.get(17)?;
    let catalyst_json: Option<String> = row.get(18)?;
    let stance_json: Option<String> = row.get(19)?;
    let followers_i64: Option<i64> = row.get(4)?;
    let resolved_i64: Option<i64> = row.get(16)?;

    Ok(NetworkStorm {
        id: row.get(0)?,
        cell_id: row.get(1)?,
        target_did: row.get(2)?,
        target_handle: row.get(3)?,
        target_followers: followers_i64.map(i64_to_us),
        target_tier: tier_str.parse::<TargetTier>().unwrap_or_default(),
        focal_post_uri: row.get(6)?,
        focal_post_snippet: row.get(7)?,
        status: status_str.parse::<StormStatus>().unwrap_or_default(),
        severity: sev_str.parse::<StormSeverity>().unwrap_or_default(),
        category: cat_str.parse::<StormCategory>().unwrap_or_default(),
        peak_score: row.get(11)?,
        radar_angle_deg: row.get(12)?,
        radar_radius: row.get(13)?,
        started_at_us: i64_to_us(row.get(14)?),
        updated_at_us: i64_to_us(row.get(15)?),
        resolved_at_us: resolved_i64.map(i64_to_us),
        metrics: serde_json::from_str::<StormMetrics>(&metrics_json).unwrap_or_default(),
        top_catalyst: catalyst_json
            .as_deref()
            .and_then(|s| serde_json::from_str::<CatalystInfo>(s).ok()),
        stance: stance_json
            .as_deref()
            .and_then(|s| serde_json::from_str::<StormStanceVerdict>(s).ok()),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    fn fixture_storm(uri: &str, sev: StormSeverity, score: f64, ts: u64) -> NetworkStorm {
        NetworkStorm {
            id: 0,
            cell_id: opaque_cell_id(uri),
            target_did: "did:plc:alice".to_string(),
            target_handle: Some("alice.bsky.social".to_string()),
            target_followers: Some(420),
            target_tier: TargetTier::Small,
            focal_post_uri: uri.to_string(),
            focal_post_snippet: "Hot take".to_string(),
            status: StormStatus::Active,
            severity: sev,
            category: StormCategory::Dogpile,
            peak_score: score,
            radar_angle_deg: 30.0,
            radar_radius: 0.35,
            started_at_us: ts,
            updated_at_us: ts,
            resolved_at_us: None,
            metrics: StormMetrics {
                unique_oon_authors_short: 18,
                composite_score: score,
                ..Default::default()
            },
            top_catalyst: None,
            stance: None,
        }
    }

    #[test]
    fn storm_store_crud_stale_resolution_weather_and_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let store = StormStore::open(dir.path().join("radar.db")).unwrap();

        let uri = "at://did:plc:alice/app.bsky.feed.post/3k1";
        let (id, is_new, is_esc) = store
            .upsert_active_storm(&fixture_storm(uri, StormSeverity::Elevated, 0.68, 100))
            .unwrap();
        assert!(is_new);
        assert!(!is_esc);

        let (id2, is_new2, is_esc2) = store
            .upsert_active_storm(&fixture_storm(uri, StormSeverity::Critical, 0.92, 200))
            .unwrap();
        assert_eq!(id, id2);
        assert!(!is_new2);
        assert!(is_esc2);

        let got = store.get_storm(id).unwrap().unwrap();
        assert_eq!(got.started_at_us, 100);
        assert_eq!(got.updated_at_us, 200);
        assert!((got.peak_score - 0.92).abs() < 1e-9);
        assert!(store.get_storm_by_cell_id(&got.cell_id).unwrap().is_some());

        let stale = store.resolve_stale_storms(300, 350).unwrap();
        assert_eq!(stale.len(), 1);
        assert!(store.list_storms(true, 10).unwrap().is_empty());
        assert_eq!(store.list_storms(false, 10).unwrap().len(), 1);

        store
            .record_weather_snapshot(&WeatherPoint {
                timestamp_us: 100,
                weather_index: 22.0,
                active_storms: 1,
                commits_per_sec: 1450.0,
            })
            .unwrap();
        store
            .record_weather_snapshot(&WeatherPoint {
                timestamp_us: 200,
                weather_index: 44.0,
                active_storms: 2,
                commits_per_sec: 1620.0,
            })
            .unwrap();
        let hist = store.list_weather_history(10).unwrap();
        assert_eq!(hist.len(), 2);
        assert_eq!(hist[0].timestamp_us, 100);

        assert!(store
            .update_storm_status(id, StormStatus::Dismissed, Some(220))
            .unwrap());
        assert_eq!(
            store.get_storm(id).unwrap().unwrap().status,
            StormStatus::Dismissed
        );

        assert!(store.prune_older_than(250).unwrap() >= 3);
        assert!(store.get_storm(id).unwrap().is_none());

        store.save_cursor_us(9_999_999).unwrap();
        assert_eq!(store.load_cursor_us().unwrap(), Some(9_999_999));
        store.save_meta_json("stats", "{\"ok\":true}").unwrap();
        assert_eq!(
            store.load_meta_json("stats").unwrap().as_deref(),
            Some("{\"ok\":true}")
        );

        assert!(!store
            .update_storm_status(id, StormStatus::Dismissed, Some(500))
            .unwrap());
    }
}
