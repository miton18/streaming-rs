//! The data type sinks receive ([`Gts`]) and its encoding to the Warp 10
//! "GTS input format".
//!
//! One point per line:
//!
//! ```text
//! TS// CLASS{label0=value0,label1=value1} VALUE
//! ```
//!
//! - `TS` is the timestamp in the platform time unit ([`TimeUnit`]);
//! - the two empty fields between the slashes are the (omitted) latitude:longitude
//!   and elevation;
//! - `CLASS` and the label names and values are percent-encoded (UTF-8 bytes,
//!   everything outside `A-Z a-z 0-9 - _ . ~` becomes `%XX`, so a space is
//!   `%20`, `+` is `%2B`, `,` `=` `{` `}` are escaped);
//! - labels come in sorted order of their raw (unencoded) names, i.e. the order
//!   of the `BTreeMap`;
//! - values: `42` (long), `1.5` (double, always with a `.`), `T` / `F`
//!   (boolean), `'text'` (string, percent-encoded like names).
//!
//! The inverse, [`parse_line`], is used by the fake ingress and by the tests to
//! check that what is emitted reads back.

use std::collections::BTreeMap;
use std::fmt::Write as _;

/// A Geo Time Series point: one value of one series at one instant.
///
/// A series is identified by its class and its labels; the point by the series
/// and its timestamp. `ts_ms` is expressed in milliseconds whatever the time
/// unit of the platform, the sink converts.
#[derive(Debug, Clone, PartialEq)]
pub struct Gts {
    /// Series class name (metric name).
    pub class: String,
    /// Series labels.
    pub labels: BTreeMap<String, String>,
    /// Timestamp, in milliseconds since the Unix epoch.
    pub ts_ms: i64,
    /// The value.
    pub value: GtsValue,
}

/// The value of a [`Gts`] point.
#[derive(Debug, Clone, PartialEq)]
pub enum GtsValue {
    /// 64-bit integer.
    Long(i64),
    /// Double. Must be finite to be encodable.
    Double(f64),
    /// Boolean.
    Bool(bool),
    /// String.
    Text(String),
}

impl Gts {
    /// A point of class `class`, no label, timestamp 0 and value `Long(0)`;
    /// complete it with [`Gts::label`], [`Gts::at`] and [`Gts::value`].
    #[must_use]
    pub fn new(class: impl Into<String>) -> Self {
        Self {
            class: class.into(),
            labels: BTreeMap::new(),
            ts_ms: 0,
            value: GtsValue::Long(0),
        }
    }

    /// Add (or replace) a label.
    #[must_use]
    pub fn label(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.labels.insert(k.into(), v.into());
        self
    }

    /// Set the timestamp, in milliseconds since the Unix epoch.
    #[must_use]
    pub fn at(mut self, ts_ms: i64) -> Self {
        self.ts_ms = ts_ms;
        self
    }

    /// Set the value.
    #[must_use]
    pub fn value(mut self, v: impl Into<GtsValue>) -> Self {
        self.value = v.into();
        self
    }
}

impl From<i64> for GtsValue {
    fn from(v: i64) -> Self {
        Self::Long(v)
    }
}
impl From<i32> for GtsValue {
    fn from(v: i32) -> Self {
        Self::Long(i64::from(v))
    }
}
impl From<u32> for GtsValue {
    fn from(v: u32) -> Self {
        Self::Long(i64::from(v))
    }
}
impl From<f64> for GtsValue {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}
impl From<bool> for GtsValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}
impl From<&str> for GtsValue {
    fn from(v: &str) -> Self {
        Self::Text(v.to_owned())
    }
}
impl From<String> for GtsValue {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}

/// Time unit of the Warp 10 platform (`warp.timeunits`). The default of Warp 10
/// is microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeUnit {
    /// Milliseconds.
    Ms,
    /// Microseconds (Warp 10 default).
    #[default]
    Us,
    /// Nanoseconds.
    Ns,
}

impl TimeUnit {
    /// Parse `"ms"`, `"us"` or `"ns"`.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "ms" => Ok(Self::Ms),
            "us" => Ok(Self::Us),
            "ns" => Ok(Self::Ns),
            other => Err(format!(
                "unknown time unit `{other}` (expected `ms`, `us` or `ns`)"
            )),
        }
    }

    /// How many platform units in one millisecond.
    #[must_use]
    pub fn per_ms(self) -> i64 {
        match self {
            Self::Ms => 1,
            Self::Us => 1_000,
            Self::Ns => 1_000_000,
        }
    }
}

/// A point that cannot be encoded.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum EncodeError {
    /// Empty class name.
    #[error("empty class name")]
    EmptyClass,
    /// Empty label name.
    #[error("series `{class}`: empty label name")]
    EmptyLabelName {
        /// Class of the series.
        class: String,
    },
    /// Empty label value (Warp 10 reads it as "remove this label").
    #[error("series `{class}`: label `{label}` has an empty value")]
    EmptyLabelValue {
        /// Class of the series.
        class: String,
        /// The label.
        label: String,
    },
    /// NaN or infinite double.
    #[error("series `{class}`: non-finite double value {value}")]
    NonFinite {
        /// Class of the series.
        class: String,
        /// The offending value.
        value: f64,
    },
    /// The timestamp does not fit in an i64 once converted to the platform unit.
    #[error("series `{class}`: timestamp {ts_ms} ms overflows the platform time unit")]
    TimestampOverflow {
        /// Class of the series.
        class: String,
        /// The offending timestamp.
        ts_ms: i64,
    },
}

fn is_unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~')
}

/// Percent-encode `s` (UTF-8 bytes); only `A-Za-z0-9-_.~` stay as is.
#[must_use]
pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    percent_encode_into(s, &mut out);
    out
}

fn percent_encode_into(s: &str, out: &mut String) {
    for &b in s.as_bytes() {
        if is_unreserved(b) {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
}

/// Inverse of [`percent_encode`], with the `application/x-www-form-urlencoded`
/// quirk Warp 10 inherits from Java's `URLDecoder`: `+` reads as a space.
pub fn percent_decode(s: &str) -> Result<String, String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = bytes
                    .get(i + 1..i + 3)
                    .and_then(|h| std::str::from_utf8(h).ok())
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                    .ok_or_else(|| format!("invalid percent escape in `{s}`"))?;
                out.push(hex);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| format!("percent-decoded `{s}` is not valid UTF-8"))
}

/// A double as text, always readable as a double and never as a long: plain
/// decimal notation (never an exponent) with at least one fractional digit.
fn format_double(v: f64) -> String {
    let mut s = format!("{v}");
    if !s.contains('.') {
        s.push_str(".0");
    }
    s
}

/// Append the line of `gts` (without the trailing newline) to `out`.
///
/// On error `out` is left unchanged.
pub fn encode_line(gts: &Gts, unit: TimeUnit, out: &mut String) -> Result<(), EncodeError> {
    if gts.class.is_empty() {
        return Err(EncodeError::EmptyClass);
    }
    for (k, v) in &gts.labels {
        if k.is_empty() {
            return Err(EncodeError::EmptyLabelName {
                class: gts.class.clone(),
            });
        }
        if v.is_empty() {
            return Err(EncodeError::EmptyLabelValue {
                class: gts.class.clone(),
                label: k.clone(),
            });
        }
    }
    if let GtsValue::Double(d) = gts.value
        && !d.is_finite()
    {
        return Err(EncodeError::NonFinite {
            class: gts.class.clone(),
            value: d,
        });
    }
    let ts =
        gts.ts_ms
            .checked_mul(unit.per_ms())
            .ok_or_else(|| EncodeError::TimestampOverflow {
                class: gts.class.clone(),
                ts_ms: gts.ts_ms,
            })?;

    let start = out.len();
    let _ = write!(out, "{ts}// ");
    percent_encode_into(&gts.class, out);
    out.push('{');
    for (i, (k, v)) in gts.labels.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        percent_encode_into(k, out);
        out.push('=');
        percent_encode_into(v, out);
    }
    out.push_str("} ");
    match &gts.value {
        GtsValue::Long(v) => {
            let _ = write!(out, "{v}");
        }
        GtsValue::Double(v) => out.push_str(&format_double(*v)),
        GtsValue::Bool(true) => out.push('T'),
        GtsValue::Bool(false) => out.push('F'),
        GtsValue::Text(s) => {
            out.push('\'');
            percent_encode_into(s, out);
            out.push('\'');
        }
    }
    debug_assert!(out.len() > start);
    Ok(())
}

/// One line of the GTS input format, decoded.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedLine {
    /// Timestamp, in the platform unit of the sender.
    pub ts: i64,
    /// Class name.
    pub class: String,
    /// Labels.
    pub labels: BTreeMap<String, String>,
    /// Value.
    pub value: GtsValue,
}

/// Parse one line of the GTS input format. Location and elevation are
/// validated but dropped; an empty timestamp is rejected (the fake has no
/// clock to default it with).
pub fn parse_line(line: &str) -> Result<ParsedLine, String> {
    let (head, rest) = line
        .split_once(' ')
        .ok_or_else(|| "expected `TS/LOC/ELEV CLASS{LABELS} VALUE`".to_owned())?;
    let mut parts = head.split('/');
    let (ts, loc, elev) = match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(ts), Some(loc), Some(elev), None) => (ts, loc, elev),
        _ => return Err(format!("bad timestamp/location/elevation field `{head}`")),
    };
    let ts: i64 = ts.parse().map_err(|_| format!("bad timestamp `{ts}`"))?;
    if !loc.is_empty() {
        let ok = loc
            .split_once(':')
            .is_some_and(|(a, b)| a.parse::<f64>().is_ok() && b.parse::<f64>().is_ok());
        if !ok {
            return Err(format!("bad location `{loc}`"));
        }
    }
    if !elev.is_empty() && elev.parse::<i64>().is_err() {
        return Err(format!("bad elevation `{elev}`"));
    }

    let open = rest
        .find('{')
        .ok_or_else(|| "missing `{` after the class name".to_owned())?;
    let close = rest
        .find('}')
        .ok_or_else(|| "missing `}` after the labels".to_owned())?;
    if close < open {
        return Err("misplaced braces".to_owned());
    }
    let class = percent_decode(&rest[..open])?;
    if class.is_empty() {
        return Err("empty class name".to_owned());
    }
    let mut labels = BTreeMap::new();
    let label_part = &rest[open + 1..close];
    if !label_part.is_empty() {
        for pair in label_part.split(',') {
            let (k, v) = pair
                .split_once('=')
                .ok_or_else(|| format!("bad label `{pair}`"))?;
            let (k, v) = (percent_decode(k)?, percent_decode(v)?);
            if k.is_empty() {
                return Err("empty label name".to_owned());
            }
            labels.insert(k, v);
        }
    }
    let raw = rest[close + 1..].trim();
    if raw.is_empty() {
        return Err("missing value".to_owned());
    }
    let value = parse_value(raw)?;
    Ok(ParsedLine {
        ts,
        class,
        labels,
        value,
    })
}

fn parse_value(raw: &str) -> Result<GtsValue, String> {
    let quoted = |q: char| raw.len() >= 2 && raw.starts_with(q) && raw.ends_with(q);
    if quoted('\'') || quoted('"') {
        return Ok(GtsValue::Text(percent_decode(&raw[1..raw.len() - 1])?));
    }
    match raw {
        "T" | "true" => return Ok(GtsValue::Bool(true)),
        "F" | "false" => return Ok(GtsValue::Bool(false)),
        _ => {}
    }
    if let Ok(v) = raw.parse::<i64>() {
        return Ok(GtsValue::Long(v));
    }
    if raw.contains(['.', 'e', 'E'])
        && let Ok(v) = raw.parse::<f64>()
        && v.is_finite()
    {
        return Ok(GtsValue::Double(v));
    }
    Err(format!("unreadable value `{raw}`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(g: &Gts, unit: TimeUnit) -> String {
        let mut s = String::new();
        encode_line(g, unit, &mut s).unwrap();
        s
    }

    #[test]
    fn long_with_labels_sorted() {
        let g = Gts::new("os.cpu")
            .label("zone", "eu")
            .label("host", "a")
            .at(1_700_000_000_123)
            .value(42);
        assert_eq!(
            enc(&g, TimeUnit::Us),
            "1700000000123000// os.cpu{host=a,zone=eu} 42"
        );
    }

    #[test]
    fn time_units() {
        let g = Gts::new("m").at(5).value(1);
        assert_eq!(enc(&g, TimeUnit::Ms), "5// m{} 1");
        assert_eq!(enc(&g, TimeUnit::Us), "5000// m{} 1");
        assert_eq!(enc(&g, TimeUnit::Ns), "5000000// m{} 1");
        let neg = Gts::new("m").at(-2).value(1);
        assert_eq!(enc(&neg, TimeUnit::Us), "-2000// m{} 1");
    }

    #[test]
    fn timestamp_overflow() {
        let g = Gts::new("m").at(i64::MAX / 10).value(1);
        let mut s = String::from("keep");
        assert!(matches!(
            encode_line(&g, TimeUnit::Ns, &mut s),
            Err(EncodeError::TimestampOverflow { .. })
        ));
        assert_eq!(s, "keep");
    }

    #[test]
    fn value_types() {
        let base = Gts::new("m").at(1);
        assert_eq!(enc(&base.clone().value(-7), TimeUnit::Ms), "1// m{} -7");
        assert_eq!(
            enc(&base.clone().value(i64::MIN), TimeUnit::Ms),
            "1// m{} -9223372036854775808"
        );
        assert_eq!(enc(&base.clone().value(1.5), TimeUnit::Ms), "1// m{} 1.5");
        assert_eq!(enc(&base.clone().value(true), TimeUnit::Ms), "1// m{} T");
        assert_eq!(enc(&base.clone().value(false), TimeUnit::Ms), "1// m{} F");
        assert_eq!(
            enc(&base.value("héllo 'x' + %"), TimeUnit::Ms),
            "1// m{} 'h%C3%A9llo%20%27x%27%20%2B%20%25'"
        );
    }

    #[test]
    fn doubles_are_never_longs() {
        for (v, expected) in [
            (3.0, "3.0"),
            (-0.0, "-0.0"),
            (0.0, "0.0"),
            (1e21, "1000000000000000000000.0"),
            (1.5e-7, "0.00000015"),
            (f64::MAX, &format!("{}.0", f64::MAX)),
        ] {
            let text = format_double(v);
            assert_eq!(text, expected);
            assert!(text.contains('.'));
            assert_eq!(text.parse::<f64>().unwrap(), v);
            assert!(!text.contains(['e', 'E']));
        }
    }

    #[test]
    fn names_and_labels_are_escaped() {
        let g = Gts::new("a b{c}")
            .label("k=1", "v,2")
            .label("é", "x+y")
            .at(0)
            .value(1);
        assert_eq!(
            enc(&g, TimeUnit::Ms),
            "0// a%20b%7Bc%7D{k%3D1=v%2C2,%C3%A9=x%2By} 1"
        );
    }

    #[test]
    fn unreserved_characters_are_kept() {
        assert_eq!(percent_encode("aZ09-_.~"), "aZ09-_.~");
        assert_eq!(percent_encode("/:@!*'()"), "%2F%3A%40%21%2A%27%28%29");
        assert_eq!(percent_encode("\n\t\"\\"), "%0A%09%22%5C");
    }

    #[test]
    fn unencodable_points() {
        let mut s = String::new();
        assert_eq!(
            encode_line(&Gts::new(""), TimeUnit::Us, &mut s),
            Err(EncodeError::EmptyClass)
        );
        assert!(matches!(
            encode_line(&Gts::new("m").label("", "v"), TimeUnit::Us, &mut s),
            Err(EncodeError::EmptyLabelName { .. })
        ));
        assert!(matches!(
            encode_line(&Gts::new("m").label("k", ""), TimeUnit::Us, &mut s),
            Err(EncodeError::EmptyLabelValue { .. })
        ));
        for d in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(matches!(
                encode_line(&Gts::new("m").value(d), TimeUnit::Us, &mut s),
                Err(EncodeError::NonFinite { .. })
            ));
        }
        assert!(s.is_empty());
    }

    #[test]
    fn roundtrip() {
        let points = [
            Gts::new("os.cpu").label("host", "a b").at(1).value(1),
            Gts::new("x{y}").label("k=,", "+%").at(-5).value(-2.25),
            Gts::new("flag").at(9).value(true),
            Gts::new("flag").at(9).value(false),
            Gts::new("msg").at(3).value("it's \"quoted\"\nnewline é+"),
            Gts::new("big").at(3).value(1e300),
            Gts::new("min").at(3).value(i64::MIN),
        ];
        for g in points {
            let line = enc(&g, TimeUnit::Us);
            assert!(!line.contains('\n'));
            let parsed = parse_line(&line).unwrap();
            assert_eq!(parsed.ts, g.ts_ms * 1000);
            assert_eq!(parsed.class, g.class);
            assert_eq!(parsed.labels, g.labels);
            assert_eq!(parsed.value, g.value, "{line}");
        }
    }

    #[test]
    fn parse_rejects_garbage() {
        for bad in [
            "",
            "1// m{}",
            "x// m{} 1",
            "1// m 1",
            "1// m{k} 1",
            "1// {} 1",
            "1// m{} abc",
            "1// m{} 'unterminated",
            "1// m{} 1e5x",
            "1/zz/ m{} 1",
            "1 m{} 1",
            "1// m%zz{} 1",
        ] {
            assert!(parse_line(bad).is_err(), "`{bad}` should be rejected");
        }
        // location and elevation are accepted and dropped
        let p = parse_line("1/48.5:2.3/100 m{a=b} 3").unwrap();
        assert_eq!((p.ts, p.value), (1, GtsValue::Long(3)));
    }
}
