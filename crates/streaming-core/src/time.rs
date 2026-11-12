//! Event time, windows and human-readable durations.

use std::{fmt, str::FromStr, time::Duration};

use serde::{Deserialize, Serialize};

/// Milliseconds since the Unix epoch.
pub type Timestamp = i64;

/// A half-open event-time interval `[start, end)`.
///
/// Windows order by `(end, start)`: the order in which they close.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Window {
    /// Inclusive start.
    pub start: Timestamp,
    /// Exclusive end.
    pub end: Timestamp,
}

impl Window {
    /// Build a window.
    #[must_use]
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }

    /// Whether `ts` falls inside the window.
    #[must_use]
    pub fn contains(&self, ts: Timestamp) -> bool {
        ts >= self.start && ts < self.end
    }
}

impl Ord for Window {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.end, self.start).cmp(&(other.end, other.start))
    }
}

impl PartialOrd for Window {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Window {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.start, self.end)
    }
}

impl FromStr for Window {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        // Timestamps may be negative, so split on the `-` that follows a digit.
        let bytes = s.as_bytes();
        let split = (1..bytes.len())
            .find(|&i| bytes[i] == b'-' && bytes[i - 1].is_ascii_digit())
            .ok_or_else(|| format!("invalid window `{s}`"))?;
        let start = s[..split]
            .parse()
            .map_err(|_| format!("invalid window start in `{s}`"))?;
        let end = s[split + 1..]
            .parse()
            .map_err(|_| format!("invalid window end in `{s}`"))?;
        Ok(Self { start, end })
    }
}

/// Parse `250ms`, `10s`, `5m`, `2h`, `1d`, or a bare number of milliseconds.
pub fn parse_duration(input: &str) -> Result<Duration, String> {
    let s = input.trim();
    let digits = s.chars().take_while(char::is_ascii_digit).count();
    if digits == 0 {
        return Err(format!("invalid duration `{input}`"));
    }
    let value: u64 = s[..digits].parse().map_err(|_| format!("invalid duration `{input}`"))?;
    let millis = match s[digits..].trim() {
        "" | "ms" => value,
        "s" => value * 1_000,
        "m" => value * 60_000,
        "h" => value * 3_600_000,
        "d" => value * 86_400_000,
        unit => return Err(format!("unknown duration unit `{unit}` in `{input}`")),
    };
    Ok(Duration::from_millis(millis))
}

/// Inverse of [`parse_duration`], using the largest exact unit.
#[must_use]
pub fn format_duration(duration: Duration) -> String {
    let ms = duration.as_millis();
    for (unit, size) in [("d", 86_400_000), ("h", 3_600_000), ("m", 60_000), ("s", 1_000)] {
        if ms != 0 && ms.is_multiple_of(size) {
            return format!("{}{unit}", ms / size);
        }
    }
    format!("{ms}ms")
}

/// JSON Schema of a duration in the [`duration_str`] form, for
/// `#[schemars(with = "DurationText")]`: a text such as `"5m"`, or a number of
/// milliseconds on input.
pub struct DurationText;

impl schemars::JsonSchema for DurationText {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Duration".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "A duration: a number followed by `ms`, `s`, `m`, `h` or `d` (`\"5m\"`), or a number of milliseconds.",
            "anyOf": [
                {"type": "string", "pattern": "^[0-9]+(ms|s|m|h|d)$"},
                {"type": "integer", "minimum": 0}
            ]
        })
    }
}

/// `#[serde(with = "streaming_core::time::duration_str")]` for config structs.
pub mod duration_str {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    /// Serialize as a human-readable string.
    pub fn serialize<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&super::format_duration(*value))
    }

    /// Deserialize from a human-readable string or a number of milliseconds.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Text(String),
            Millis(u64),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Text(text) => super::parse_duration(&text).map_err(serde::de::Error::custom),
            Raw::Millis(ms) => Ok(Duration::from_millis(ms)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("3s").unwrap(), Duration::from_secs(3));
        assert_eq!(parse_duration("3h").unwrap(), Duration::from_secs(3 * 3_600));
        assert_eq!(parse_duration("3d").unwrap(), Duration::from_secs(3 * 86_400));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("42").unwrap(), Duration::from_millis(42));
        assert!(parse_duration("5 parsecs").is_err());
        assert_eq!(format_duration(Duration::from_secs(300)), "5m");
        assert_eq!(format_duration(Duration::from_millis(1500)), "1500ms");
    }

    #[test]
    fn windows() {
        let w = Window::new(-600, -300);
        assert_eq!(w.to_string().parse::<Window>().unwrap(), w);
        let w = Window::new(0, 300_000);
        assert_eq!(w.to_string().parse::<Window>().unwrap(), w);
        assert!(Window::new(0, 10) < Window::new(5, 20));
        assert!(w.contains(0) && !w.contains(300_000));
    }
}
