//! Values flowing through a graph.
//!
//! Operators are registered with concrete Rust types, but a graph is assembled
//! at run time from a description stored in the KV. Edges therefore carry
//! type-erased values ([`Datum`]); the registry checks type compatibility when
//! a graph is submitted, and operators downcast on the hot path.

use crate::{
    ids::SliceId,
    time::{Timestamp, Window},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{any::Any, fmt, str::FromStr, sync::Arc};

/// A type-erased, cheaply clonable value.
pub type Datum = Arc<dyn Any + Send + Sync>;

/// Wrap a value into a [`Datum`].
pub fn datum<T: Any + Send + Sync>(value: T) -> Datum {
    Arc::new(value)
}

/// A replayable position inside one partition of a source.
///
/// Positions are totally ordered inside a partition. The three components are
/// source-defined: Pulsar uses `(ledger, entry, batch index)`, a log with plain
/// offsets uses `(0, offset, 0)`, and the emitting half of an aggregate node
/// uses `(window end, window start, key index)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Position(pub u64, pub u64, pub u64);

// Text formats (JSON) carry a position as the string `a.b.c`: its components
// are full 64-bit integers, which JavaScript consumers would silently round.
// Binary formats keep the three integers.
impl Serialize for Position {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            (self.0, self.1, self.2).serialize(serializer)
        }
    }
}

impl schemars::JsonSchema for Position {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Position".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "A position inside a source partition, `a.b.c`: three 64-bit integers, as text so that JavaScript does not round them.",
            "pattern": "^[0-9]+\\.[0-9]+\\.[0-9]+$",
        })
    }
}

impl<'de> Deserialize<'de> for Position {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            #[derive(Deserialize)]
            #[serde(untagged)]
            enum Repr {
                Text(String),
                Parts(u64, u64, u64),
            }
            match Repr::deserialize(deserializer)? {
                Repr::Text(text) => text.parse().map_err(serde::de::Error::custom),
                Repr::Parts(a, b, c) => Ok(Position(a, b, c)),
            }
        } else {
            let (a, b, c) = <(u64, u64, u64)>::deserialize(deserializer)?;
            Ok(Position(a, b, c))
        }
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.0, self.1, self.2)
    }
}

impl FromStr for Position {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        let mut parts = s.split('.').map(str::parse::<u64>);
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(Ok(a)), Some(Ok(b)), Some(Ok(c)), None) => Ok(Self(a, b, c)),
            _ => Err(format!("invalid position `{s}`")),
        }
    }
}

/// A grouping key, stored as its `postcard` encoding.
///
/// Keys are opaque bytes so that they can be used as KV fields and, later, as
/// routing keys between workers, whatever their Rust type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Key(Arc<[u8]>);

impl Key {
    /// Wrap already-encoded bytes.
    #[must_use]
    pub fn from_bytes(bytes: impl Into<Arc<[u8]>>) -> Self {
        Self(bytes.into())
    }

    /// Encode a typed key.
    pub fn encode<K: Serialize>(key: &K) -> Result<Self, String> {
        postcard::to_allocvec(key)
            .map(|bytes| Self(bytes.into()))
            .map_err(|e| e.to_string())
    }

    /// Decode back into the type it was encoded from.
    pub fn decode<K: DeserializeOwned>(&self) -> Result<K, String> {
        postcard::from_bytes(&self.0).map_err(|e| e.to_string())
    }

    /// The encoded bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Lowercase hexadecimal form, used as a KV field name.
    #[must_use]
    pub fn to_hex(&self) -> String {
        let mut out = String::with_capacity(self.0.len() * 2);
        for byte in self.0.iter() {
            out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
            out.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
        }
        out
    }

    /// Inverse of [`Key::to_hex`].
    pub fn from_hex(hex: &str) -> Result<Self, String> {
        if !hex.len().is_multiple_of(2) {
            return Err(format!("odd-length hex key `{hex}`"));
        }
        let digit = |c: u8| {
            char::from(c)
                .to_digit(16)
                .ok_or_else(|| format!("invalid hex key `{hex}`"))
        };
        let mut bytes = Vec::with_capacity(hex.len() / 2);
        for pair in hex.as_bytes().chunks(2) {
            bytes.push(((digit(pair[0])? << 4) | digit(pair[1])?) as u8);
        }
        Ok(Self(bytes.into()))
    }

    /// Best-effort human-readable form: the string if the key was a `String`,
    /// otherwise its hexadecimal form.
    #[must_use]
    pub fn display_lossy(&self) -> String {
        match self.decode::<String>() {
            Ok(text) if !text.chars().any(char::is_control) => text,
            _ => format!("0x{}", self.to_hex()),
        }
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Key({})", self.display_lossy())
    }
}

/// Where a record comes from: the source position it derives from, plus a hash
/// of the path it took through the graph. Together they identify a record
/// deterministically, which is what makes re-emission after a failure
/// detectable by sinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Origin {
    /// Position of the source record (or closed window) this record derives from.
    pub position: Position,
    /// Hash of the nodes traversed and of the output index at each hop.
    pub lineage: u64,
}

impl Origin {
    /// Origin of a record freshly read from a source.
    #[must_use]
    pub fn source(position: Position) -> Self {
        Self { position, lineage: 0 }
    }

    /// Origin of the `index`-th output produced by node `node_hash` from a
    /// record with this origin.
    #[must_use]
    pub fn derive(&self, node_hash: u64, index: u32) -> Self {
        Self {
            position: self.position,
            lineage: mix(mix(self.lineage, node_hash), u64::from(index)),
        }
    }
}

/// Deterministic 64-bit mixer (stable across processes and versions).
#[must_use]
pub fn mix(a: u64, b: u64) -> u64 {
    let mut z = a ^ b
        .wrapping_add(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(a << 6)
        .wrapping_add(a >> 2);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministic FNV-1a hash of a string (stable across processes and versions).
#[must_use]
pub fn hash_str(s: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in s.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// A value travelling on an edge, with its event time and annotations.
#[derive(Clone)]
pub struct Record {
    /// The payload.
    pub value: Datum,
    /// Event time.
    pub ts: Timestamp,
    /// Grouping key, set by a `key_by` node.
    pub key: Option<Key>,
    /// Window, set by a window node (one record per assigned window).
    pub window: Option<Window>,
    /// Deterministic identity.
    pub origin: Origin,
}

impl fmt::Debug for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Record")
            .field("ts", &self.ts)
            .field("key", &self.key)
            .field("window", &self.window)
            .field("origin", &self.origin)
            .finish_non_exhaustive()
    }
}

/// Deterministic identifier of one element handed to a sink.
///
/// The same logical element always carries the same id, however many times it
/// is re-emitted after a failure. Sinks use it to make writes idempotent.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EmitId(String);

impl EmitId {
    /// Build the id of a record emitted by a slice.
    #[must_use]
    pub fn new(slice: &SliceId, origin: &Origin) -> Self {
        Self(format!("{slice}/{}/{:016x}", origin.position, origin.lineage))
    }

    /// The id as a string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// A stable 64-bit digest of the id.
    #[must_use]
    pub fn digest(&self) -> u64 {
        hash_str(&self.0)
    }
}

impl fmt::Display for EmitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One element handed to a sink.
#[derive(Clone)]
pub struct Emit {
    /// Deterministic identifier, identical on every re-emission.
    pub id: EmitId,
    /// Where the element comes from. Inside one slice, positions never
    /// decrease from one element to the next, which lets a sink derive a
    /// monotonic sequence number when its destination deduplicates on one.
    pub origin: Origin,
    /// Event time.
    pub ts: Timestamp,
    /// Grouping key, when the element went through a `key_by`.
    pub key: Option<Key>,
    /// The payload.
    pub value: Datum,
}

impl fmt::Debug for Emit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Emit")
            .field("id", &self.id)
            .field("ts", &self.ts)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

/// Output of an aggregate node: the result of one key in one closed window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Windowed<V> {
    /// The closed window.
    pub window: Window,
    /// The grouping key.
    pub key: Key,
    /// The aggregated value.
    pub value: V,
}

impl<V> Windowed<V> {
    /// Decode the key into its original type.
    pub fn key<K: DeserializeOwned>(&self) -> Result<K, String> {
        self.key.decode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::NodeId;

    #[test]
    fn key_round_trips() {
        let key = Key::encode(&"user-42".to_owned()).unwrap();
        assert_eq!(key.decode::<String>().unwrap(), "user-42");
        assert_eq!(Key::from_hex(&key.to_hex()).unwrap(), key);
        assert_eq!(key.display_lossy(), "user-42");
        assert!(Key::from_hex("abc").is_err());
    }

    #[test]
    fn position_orders_lexicographically() {
        assert!(Position(1, 9, 9) < Position(2, 0, 0));
        assert!(Position(1, 2, 3) < Position(1, 2, 4));
        assert_eq!("7.8.9".parse::<Position>().unwrap(), Position(7, 8, 9));
    }

    #[test]
    fn position_keeps_64_bits_in_json_and_stays_compact_in_binary() {
        let position = Position(u64::MAX, 1 << 63, 3);
        let json = serde_json::to_string(&position).unwrap();
        assert_eq!(json, "\"18446744073709551615.9223372036854775808.3\"");
        assert_eq!(serde_json::from_str::<Position>(&json).unwrap(), position);
        assert_eq!(serde_json::from_str::<Position>("[7,8,9]").unwrap(), Position(7, 8, 9));
        let small = Position(0, 5, 0);
        let bytes = postcard::to_allocvec(&small).unwrap();
        assert_eq!(bytes, [0, 5, 0]);
        assert_eq!(postcard::from_bytes::<Position>(&bytes).unwrap(), small);
    }

    #[test]
    fn lineage_is_deterministic_and_path_sensitive() {
        let origin = Origin::source(Position(0, 5, 0));
        let via_b = origin.derive(hash_str("b"), 0);
        let via_c = origin.derive(hash_str("c"), 0);
        assert_ne!(via_b, via_c);
        assert_eq!(via_b, origin.derive(hash_str("b"), 0));
        assert_ne!(via_b, origin.derive(hash_str("b"), 1));
        let slice = SliceId::Source {
            node: NodeId::new("events").unwrap(),
            partition: 2,
        };
        assert_eq!(
            EmitId::new(&slice, &origin).as_str(),
            "src.events.2/0.5.0/0000000000000000"
        );
    }

    #[test]
    fn hashes_are_pinned() {
        // Lineages and emit ids are persisted and compared across restarts
        // and upgrades: these values must never change. `mix(0, 0)` is the
        // first output of SplitMix64 seeded with 0, and `hash_str("abc")` the
        // published 64-bit FNV-1a of "abc".
        assert_eq!(mix(0, 0), 0xe220_a839_7b1d_cdaf);
        assert_eq!(mix(1, 2), 0x3706_970b_052f_16b1);
        assert_eq!(mix(u64::MAX, 7), 0x94ca_6496_1cc0_585f);
        assert_eq!(hash_str(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(hash_str("abc"), 0xe71f_a219_0541_574b);
        let id = EmitId("abc".to_owned());
        assert_eq!(id.digest(), hash_str("abc"));
    }

    #[test]
    fn hex_keys_decode_every_nibble() {
        let key = Key::from_hex("00ff7a1e").unwrap();
        assert_eq!(key.as_bytes(), [0x00, 0xff, 0x7a, 0x1e]);
        assert_eq!(key.to_hex(), "00ff7a1e");
        assert!(Key::from_hex("zz").is_err());
    }

    #[test]
    fn keys_with_control_characters_display_as_hex() {
        let key = Key::encode(&"a\nb".to_owned()).unwrap();
        assert_eq!(key.display_lossy(), format!("0x{}", key.to_hex()));
    }
}
