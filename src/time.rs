//! Clock-warp-safe Unix timestamp and duration utilities.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Returns the current Unix timestamp in microseconds, clamped cleanly on clock skew.
///
/// # Examples
/// ```
/// let now = skydoppler::time::current_time_us();
/// assert!(now > 0);
/// ```
#[must_use]
pub fn current_time_us() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => u64::try_from(d.as_micros()).unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

/// Converts a microsecond timestamp to SQLite signed 64-bit integer without wrapping negative.
///
/// # Examples
/// ```
/// assert_eq!(skydoppler::time::us_to_i64(42), 42);
/// assert_eq!(skydoppler::time::us_to_i64(u64::MAX), i64::MAX);
/// ```
#[must_use]
pub fn us_to_i64(us: u64) -> i64 {
    i64::try_from(us).unwrap_or(i64::MAX)
}

/// Converts a SQLite signed 64-bit timestamp back to unsigned microseconds (clamping negative to `0`).
///
/// # Examples
/// ```
/// assert_eq!(skydoppler::time::i64_to_us(123), 123);
/// assert_eq!(skydoppler::time::i64_to_us(-5), 0);
/// ```
#[must_use]
pub fn i64_to_us(raw: i64) -> u64 {
    u64::try_from(raw).unwrap_or(0)
}

/// Returns saturating elapsed microseconds (`now_us - earlier_us`, or `0` if clock jumped backwards).
///
/// # Examples
/// ```
/// assert_eq!(skydoppler::time::elapsed_us(100, 150), 50);
/// assert_eq!(skydoppler::time::elapsed_us(200, 150), 0);
/// ```
#[must_use]
pub const fn elapsed_us(earlier_us: u64, now_us: u64) -> u64 {
    now_us.saturating_sub(earlier_us)
}

/// Converts a [`Duration`] to whole microseconds (`u64`), saturating at `u64::MAX` on overflow.
#[must_use]
pub fn duration_to_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// Formats a microsecond Unix timestamp as a canonical UTC ISO 8601 string.
///
/// # Examples
/// ```
/// assert_eq!(
///     skydoppler::time::format_unix_us_iso8601(0),
///     "1970-01-01T00:00:00.000Z"
/// );
/// ```
#[must_use]
pub fn format_unix_us_iso8601(timestamp_us: u64) -> String {
    let secs = timestamp_us / 1_000_000;
    let t = UNIX_EPOCH
        .checked_add(Duration::from_secs(secs))
        .unwrap_or(UNIX_EPOCH);
    skybase::lexicon::format_system_time_iso8601(t)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn time_conversions_and_clock_warp_safety() {
        assert!(current_time_us() > 1_700_000_000_000_000);
        assert_eq!(us_to_i64(0), 0);
        assert_eq!(us_to_i64(999), 999);
        assert_eq!(us_to_i64(u64::MAX), i64::MAX);

        assert_eq!(i64_to_us(-1), 0);
        assert_eq!(i64_to_us(0), 0);
        assert_eq!(i64_to_us(555), 555);

        assert_eq!(elapsed_us(10, 10), 0);
        assert_eq!(elapsed_us(10, 35), 25);
        assert_eq!(elapsed_us(50, 35), 0);

        assert_eq!(duration_to_us(Duration::from_millis(250)), 250_000);
        assert_eq!(duration_to_us(Duration::ZERO), 0);

        assert_eq!(format_unix_us_iso8601(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format_unix_us_iso8601(1_700_000_000_000_000),
            "2023-11-14T22:13:20.000Z"
        );
    }
}
