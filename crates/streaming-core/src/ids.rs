//! Validated string identifiers.
//!
//! Identifiers end up inside KV keys (`t:<tenant>:g:<graph>:...`) and inside
//! slice names (`src.<node>.<partition>`), so their alphabet is restricted.

use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

/// Error returned when an identifier does not respect its alphabet.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid {kind} `{value}`: {reason}")]
pub struct IdError {
    /// Which identifier kind was being parsed.
    pub kind: &'static str,
    /// The rejected value.
    pub value: String,
    /// Why it was rejected.
    pub reason: IdProblem,
}

/// Why an identifier was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IdProblem {
    /// The value is empty.
    #[error("must not be empty")]
    Empty,
    /// The value is longer than [`MAX_ID_LEN`] bytes.
    #[error("must be at most 128 bytes")]
    TooLong,
    /// A character is outside the alphabet.
    #[error("contains a character outside the allowed alphabet")]
    InvalidCharacter,
    /// A composite identifier does not have the expected shape.
    #[error("expected `{0}`")]
    Malformed(&'static str),
    /// The partition of a slice id is not a number.
    #[error("partition is not a number")]
    BadPartition,
}

impl IdProblem {
    /// A constant name for the problem, for machines (`empty`, `tooLong`, ...).
    #[must_use]
    pub fn code(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::TooLong => "tooLong",
            Self::InvalidCharacter => "invalidCharacter",
            Self::Malformed(_) => "malformed",
            Self::BadPartition => "badPartition",
        }
    }
}

/// Longest identifier, in bytes.
pub const MAX_ID_LEN: usize = 128;

fn check(kind: &'static str, value: &str, extra: &[char]) -> Result<(), IdError> {
    let err = |reason| IdError {
        kind,
        value: value.to_owned(),
        reason,
    };
    if value.is_empty() {
        return Err(err(IdProblem::Empty));
    }
    if value.len() > MAX_ID_LEN {
        return Err(err(IdProblem::TooLong));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || extra.contains(&c))
    {
        return Err(err(IdProblem::InvalidCharacter));
    }
    Ok(())
}

/// The JSON Schema of an identifier checked by [`check`].
fn id_schema(kind: &str, extra: &[char]) -> schemars::Schema {
    // Inside a character class, `.`, `/` and `@` need no escape; `-` last is
    // literal.
    let extra_class: String = extra.iter().collect();
    schemars::json_schema!({
        "type": "string",
        "description": format!("A {kind}: 1 to 128 letters, digits, `-` or `_`{}.", extra_text(extra)),
        "minLength": 1,
        "maxLength": 128,
        "pattern": format!("^[A-Za-z0-9_{extra_class}-]+$"),
    })
}

fn extra_text(extra: &[char]) -> String {
    extra.iter().map(|c| format!(", `{c}`")).collect()
}

macro_rules! string_id {
    ($(#[$m:meta])* $name:ident, $kind:literal, [$($extra:literal),*]) => {
        $(#[$m])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            /// Validate and wrap a string.
            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                check($kind, &value, &[$($extra),*])?;
                Ok(Self(value))
            }

            /// Borrow the underlying string.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl schemars::JsonSchema for $name {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                stringify!($name).into()
            }

            fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
                id_schema($kind, &[$($extra),*])
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;
            fn try_from(value: String) -> Result<Self, IdError> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> String {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdError;
            fn from_str(s: &str) -> Result<Self, IdError> {
                Self::new(s)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

string_id!(
    /// A tenant. Every graph, actor, key and metric belongs to exactly one. Alphabet `[A-Za-z0-9_-]`.
    TenantId,
    "tenant id",
    []
);
string_id!(
    /// A graph, unique inside its tenant. Alphabet `[A-Za-z0-9_-]`.
    GraphId,
    "graph id",
    []
);
string_id!(
    /// A node of a graph, unique inside its graph. Alphabet `[A-Za-z0-9_-]`.
    NodeId,
    "node id",
    []
);
string_id!(
    /// A worker process of the cluster. Alphabet `[A-Za-z0-9_.-]`.
    WorkerId,
    "worker id",
    ['.']
);
string_id!(
    /// Reference to a registered operator, e.g. `std/count@1`. Alphabet `[A-Za-z0-9_./@-]`.
    OpRef,
    "operator ref",
    ['/', '@', '.']
);

/// A graph inside its tenant. Displays as `tenant/graph`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GraphKey {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Graph id.
    pub graph: GraphId,
}

impl GraphKey {
    /// Build a graph key.
    #[must_use]
    pub fn new(tenant: TenantId, graph: GraphId) -> Self {
        Self { tenant, graph }
    }
}

impl fmt::Display for GraphKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.tenant, self.graph)
    }
}

impl FromStr for GraphKey {
    type Err = IdError;
    fn from_str(s: &str) -> Result<Self, IdError> {
        let (tenant, graph) = s.split_once('/').ok_or(IdError {
            kind: "graph key",
            value: s.to_owned(),
            reason: IdProblem::Malformed("tenant/graph"),
        })?;
        Ok(Self {
            tenant: TenantId::new(tenant)?,
            graph: GraphId::new(graph)?,
        })
    }
}

/// A slice is the unit of exactly-once execution and of placement.
///
/// A graph is cut at its stateful (aggregate) nodes. Each cut piece is run once
/// per input partition:
///
/// - `Source { node, partition }` runs everything reachable from one partition
///   of a source node, up to and including the first aggregate nodes;
/// - `Emitter { node }` reads the closed windows of an aggregate node and runs
///   everything downstream of it.
///
/// Displays as `src.<node>.<partition>` or `agg.<node>`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SliceId {
    /// One partition of a source node.
    Source {
        /// The source node.
        node: NodeId,
        /// Partition index.
        partition: u32,
    },
    /// The emitting half of an aggregate node.
    Emitter {
        /// The aggregate node.
        node: NodeId,
    },
}

impl SliceId {
    /// The node this slice starts from.
    #[must_use]
    pub fn root(&self) -> &NodeId {
        match self {
            SliceId::Source { node, .. } | SliceId::Emitter { node } => node,
        }
    }
}

impl fmt::Display for SliceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SliceId::Source { node, partition } => write!(f, "src.{node}.{partition}"),
            SliceId::Emitter { node } => write!(f, "agg.{node}"),
        }
    }
}

impl FromStr for SliceId {
    type Err = IdError;
    fn from_str(s: &str) -> Result<Self, IdError> {
        let err = |reason| IdError {
            kind: "slice id",
            value: s.to_owned(),
            reason,
        };
        if let Some(rest) = s.strip_prefix("src.") {
            let (node, partition) = rest
                .rsplit_once('.')
                .ok_or_else(|| err(IdProblem::Malformed("src.<node>.<partition>")))?;
            Ok(SliceId::Source {
                node: NodeId::new(node)?,
                partition: partition.parse().map_err(|_| err(IdProblem::BadPartition))?,
            })
        } else if let Some(node) = s.strip_prefix("agg.") {
            Ok(SliceId::Emitter {
                node: NodeId::new(node)?,
            })
        } else {
            Err(err(IdProblem::Malformed("src.<node>.<partition>` or `agg.<node>")))
        }
    }
}

impl Serialize for SliceId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl schemars::JsonSchema for SliceId {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SliceId".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "A slice: `src.<node>.<partition>` (one source partition) or `agg.<node>` (the emitter of an aggregate).",
            "pattern": "^(src\\.[A-Za-z0-9_-]+\\.[0-9]+|agg\\.[A-Za-z0-9_-]+)$",
        })
    }
}

impl<'de> Deserialize<'de> for SliceId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// A slice of a graph of a tenant. Displays as `tenant/graph/slice`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SliceKey {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Graph id.
    pub graph: GraphId,
    /// Slice inside the graph.
    pub slice: SliceId,
}

impl SliceKey {
    /// The graph this slice belongs to.
    #[must_use]
    pub fn graph_key(&self) -> GraphKey {
        GraphKey::new(self.tenant.clone(), self.graph.clone())
    }
}

impl fmt::Display for SliceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.tenant, self.graph, self.slice)
    }
}

impl FromStr for SliceKey {
    type Err = IdError;
    fn from_str(s: &str) -> Result<Self, IdError> {
        let mut parts = s.splitn(3, '/');
        let (Some(tenant), Some(graph), Some(slice)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(IdError {
                kind: "slice key",
                value: s.to_owned(),
                reason: IdProblem::Malformed("tenant/graph/slice"),
            });
        };
        Ok(Self {
            tenant: TenantId::new(tenant)?,
            graph: GraphId::new(graph)?,
            slice: slice.parse()?,
        })
    }
}

impl Serialize for SliceKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl schemars::JsonSchema for SliceKey {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SliceKey".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "A slice of a graph of a tenant: `<tenant>/<graph>/<slice>`.",
        })
    }
}

impl<'de> Deserialize<'de> for SliceKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_reject_separators() {
        assert!(TenantId::new("acme").is_ok());
        assert!(TenantId::new("ac:me").is_err());
        assert!(TenantId::new("ac/me").is_err());
        assert!(NodeId::new("by.key").is_err());
        assert!(OpRef::new("std/count@1").is_ok());
        assert!(TenantId::new("").is_err());
        assert!(TenantId::new("a".repeat(128)).is_ok());
        assert!(TenantId::new("a".repeat(129)).is_err());
    }

    #[test]
    fn graph_keys_display_and_parse() {
        let key = GraphKey::new(TenantId::new("acme").unwrap(), GraphId::new("g").unwrap());
        assert_eq!(key.to_string(), "acme/g");
        assert_eq!("acme/g".parse::<GraphKey>().unwrap(), key);
    }

    #[test]
    fn slice_round_trip() {
        for s in ["src.events.3", "agg.count"] {
            let slice: SliceId = s.parse().unwrap();
            assert_eq!(slice.to_string(), s);
        }
        let key: SliceKey = "acme/count-per-key/src.events.0".parse().unwrap();
        assert_eq!(key.to_string(), "acme/count-per-key/src.events.0");
        assert!("acme/x".parse::<SliceKey>().is_err());
        assert!("zzz.events".parse::<SliceId>().is_err());
    }
}
