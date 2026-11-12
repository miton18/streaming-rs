//! The parameters of an operator: the fields of its structure.
//!
//! `#[derive(Operator)]` reads the structure of an operator and makes of each
//! field marked `#[param]` a parameter the API and the UI may change: its
//! name in the configuration (`rate_per_sec` is `ratePerSec`), its kind from
//! its Rust type through [`Param`], its description from its documentation
//! comment, its default value and its bounds from the attribute. A field
//! without `#[param]` is part of the configuration all the same, but only the
//! code sets it.
//!
//! ```
//! use std::time::Duration;
//! use streaming_core::{Operator, Param};
//!
//! /// Where a reader starts on a topic it has never read.
//! #[derive(Debug, Clone, Copy, PartialEq, Eq, Param)]
//! pub enum Start {
//!     Earliest,
//!     Latest,
//! }
//!
//! /// Reads the events of a topic.
//! #[derive(Debug, Clone, PartialEq, Eq, Operator)]
//! #[operator("acme/topic@1")]
//! pub struct Topic {
//!     /// Name of the topic.
//!     pub name: String,
//!     /// Where to start on a topic never read before.
//!     #[param(default = "earliest")]
//!     pub start: Start,
//!     /// Records read at once.
//!     #[param(default = 500, min = 1, max = 10_000)]
//!     pub batch: u32,
//!     /// How long a read may wait for records.
//!     #[param(default = "1s", min = "10ms")]
//!     pub patience: Duration,
//! }
//!
//! assert_eq!(Topic::OP, "acme/topic@1");
//! assert_eq!(Topic::ABOUT, "Reads the events of a topic.");
//! let params = Topic::params();
//! assert_eq!(params.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(), ["start", "batch", "patience"]);
//! assert_eq!(params[0].choices, ["earliest", "latest"]);
//! assert_eq!(params[1].kind, "integer");
//!
//! // What the code does not give takes its default.
//! let topic: Topic = serde_json::from_value(serde_json::json!({"name": "clicks", "batch": 20})).unwrap();
//! assert_eq!((topic.start, topic.batch, topic.patience), (Start::Earliest, 20, Duration::from_secs(1)));
//! assert!(topic.check().is_ok());
//! assert_eq!(
//!     serde_json::to_value(&topic).unwrap(),
//!     serde_json::json!({"name": "clicks", "start": "earliest", "batch": 20, "patience": "1s"})
//! );
//!
//! // The bounds are checked, in words.
//! let greedy = Topic { batch: 50_000, ..topic };
//! assert_eq!(
//!     greedy.check().unwrap_err().to_string(),
//!     "invalid operator configuration: `batch` must be between 1 and 10000"
//! );
//! ```

use crate::{
    ops::OpError,
    registry::ParamSpec,
    time::{format_duration, parse_duration},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{borrow::Cow, fmt, time::Duration};

/// A type a parameter of an operator may have.
///
/// Implemented for integers, floating-point numbers, booleans, texts,
/// durations (`"5m"`), [`Secret`]s, any JSON value, and `Option`s of those.
/// `#[derive(Param)]` implements it for an enumeration without data, offered
/// as a list of choices: a variant `EarliestFirst` is written
/// `earliest-first`, or the text given by `#[param(rename = "...")]`.
pub trait Param: Sized {
    /// The kind tools show: `integer`, `number`, `boolean`, `text`,
    /// `duration`, `secret` or `json`.
    const KIND: &'static str;

    /// Whether a configuration may leave the parameter out although it has no
    /// default value.
    const OPTIONAL: bool = false;

    /// The only texts accepted, when there is a fixed set of them.
    #[must_use]
    fn choices() -> Vec<String> { Vec::new() }

    /// The value as a configuration holds it.
    fn to_json(&self) -> Value;

    /// Read the value from a configuration.
    ///
    /// # Errors
    ///
    /// What was expected, in words.
    fn from_json(value: &Value) -> Result<Self, String>;

    /// The value of a parameter a configuration leaves out, when
    /// [`Param::OPTIONAL`].
    #[must_use]
    fn absent() -> Option<Self> { None }

    /// The secret the value is, if it is one.
    #[must_use]
    fn secret(&self) -> Option<&Secret> { None }
}

macro_rules! integer_params {
    ($($ty:ty),*) => {$(
        impl Param for $ty {
            const KIND: &'static str = "integer";

            fn to_json(&self) -> Value { Value::from(*self) }

            fn from_json(value: &Value) -> Result<Self, String> {
                let out_of_range = || format!("expected an integer between {} and {}", <$ty>::MIN, <$ty>::MAX);
                if let Some(number) = value.as_u64() {
                    return <$ty>::try_from(number).map_err(|_| out_of_range());
                }
                match value.as_i64() {
                    Some(number) => <$ty>::try_from(number).map_err(|_| out_of_range()),
                    None => Err("expected an integer".to_owned()),
                }
            }
        }
    )*};
}

integer_params!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl Param for f64 {
    const KIND: &'static str = "number";

    fn to_json(&self) -> Value { Value::from(*self) }

    fn from_json(value: &Value) -> Result<Self, String> { value.as_f64().ok_or_else(|| "expected a number".to_owned()) }
}

impl Param for f32 {
    const KIND: &'static str = "number";

    fn to_json(&self) -> Value { Value::from(*self) }

    fn from_json(value: &Value) -> Result<Self, String> { f64::from_json(value).map(|number| number as f32) }
}

impl Param for bool {
    const KIND: &'static str = "boolean";

    fn to_json(&self) -> Value { Value::from(*self) }

    fn from_json(value: &Value) -> Result<Self, String> {
        value.as_bool().ok_or_else(|| "expected `true` or `false`".to_owned())
    }
}

impl Param for String {
    const KIND: &'static str = "text";

    fn to_json(&self) -> Value { Value::from(self.as_str()) }

    fn from_json(value: &Value) -> Result<Self, String> {
        value
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| "expected a text".to_owned())
    }
}

impl Param for Duration {
    const KIND: &'static str = "duration";

    fn to_json(&self) -> Value { Value::from(format_duration(*self)) }

    fn from_json(value: &Value) -> Result<Self, String> {
        match value {
            Value::String(text) => parse_duration(text),
            Value::Number(number) => number
                .as_u64()
                .map(Duration::from_millis)
                .ok_or_else(|| "expected a duration such as `5m`".to_owned()),
            _ => Err("expected a duration such as `5m`".to_owned()),
        }
    }
}

impl Param for Value {
    const KIND: &'static str = "json";

    fn to_json(&self) -> Value { self.clone() }

    fn from_json(value: &Value) -> Result<Self, String> { Ok(value.clone()) }
}

impl<T: Param> Param for Option<T> {
    const KIND: &'static str = T::KIND;
    const OPTIONAL: bool = true;

    fn choices() -> Vec<String> { T::choices() }

    fn to_json(&self) -> Value { self.as_ref().map_or(Value::Null, T::to_json) }

    fn from_json(value: &Value) -> Result<Self, String> {
        if value.is_null() {
            Ok(None)
        } else {
            T::from_json(value).map(Some)
        }
    }

    fn absent() -> Option<Self> { Some(None) }

    fn secret(&self) -> Option<&Secret> { self.as_ref().and_then(T::secret) }
}

/// A secret of a configuration: the text itself, or the name of one of the
/// tenant's secrets (`{"secret": "<name>"}`), which is what a description
/// should hold. [`crate::OpContext::reveal`] gives the text.
///
/// Its `Debug` form never shows a literal.
#[derive(Clone, PartialEq, Eq)]
pub enum Secret {
    /// The secret itself, written in the configuration.
    Literal(String),
    /// The name of a secret of the tenant.
    Named(String),
}

impl Secret {
    /// The secret of the tenant called `name`.
    #[must_use]
    pub fn named(name: impl Into<String>) -> Self { Self::Named(name.into()) }

    /// A secret written as is. It ends up in the stored description: prefer
    /// [`Secret::named`] outside of tests.
    #[must_use]
    pub fn literal(text: impl Into<String>) -> Self { Self::Literal(text.into()) }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Secret::Literal(_) => f.write_str("Secret(<redacted>)"),
            Secret::Named(name) => write!(f, "Secret(named `{name}`)"),
        }
    }
}

impl Param for Secret {
    const KIND: &'static str = "secret";

    fn secret(&self) -> Option<&Secret> { Some(self) }

    fn to_json(&self) -> Value {
        match self {
            Secret::Literal(text) => Value::from(text.as_str()),
            Secret::Named(name) => serde_json::json!({"secret": name}),
        }
    }

    fn from_json(value: &Value) -> Result<Self, String> {
        let expected = || "expected a text or `{\"secret\": \"<name>\"}`".to_owned();
        match value {
            Value::String(text) => Ok(Secret::Literal(text.clone())),
            Value::Object(map) if map.len() == 1 => map
                .get("secret")
                .and_then(Value::as_str)
                .map(|name| Secret::Named(name.to_owned()))
                .ok_or_else(expected),
            _ => Err(expected()),
        }
    }
}

impl Serialize for Secret {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_json().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Secret {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Self::from_json(&value).map_err(serde::de::Error::custom)
    }
}

/// What the code generated by `#[derive(Operator)]` and `#[derive(Param)]`
/// calls. Not an interface: it changes with the derives.
#[doc(hidden)]
pub mod private {
    use super::{Cow, DeserializeOwned, OpError, Param, ParamSpec, Serialize};
    pub use serde;
    pub use serde_json::{Map, Value};

    /// The description of the parameter `name` of type `T`.
    #[must_use]
    pub fn spec<T: Param>(name: &str, description: &str, default: Option<Value>) -> ParamSpec {
        ParamSpec {
            name: name.to_owned(),
            kind: T::KIND.to_owned(),
            required: default.is_none() && !T::OPTIONAL,
            default,
            description: description.to_owned(),
            choices: T::choices(),
        }
    }

    /// The fields of a configuration, which holds no other than `known`.
    pub fn object<'a>(value: &'a Value, known: &[&str]) -> Result<Cow<'a, Map<String, Value>>, String> {
        let map = match value {
            Value::Null => return Ok(Cow::Owned(Map::new())),
            Value::Object(map) => map,
            _ => return Err("expected an object".to_owned()),
        };
        match map.keys().find(|key| !known.contains(&key.as_str())) {
            Some(unknown) if known.is_empty() => Err(format!("unknown field `{unknown}`: there is none")),
            Some(unknown) => Err(format!("unknown field `{unknown}`: expected {}", one_of_fields(known))),
            None => Ok(Cow::Borrowed(map)),
        }
    }

    fn one_of_fields(names: &[&str]) -> String {
        let quoted: Vec<String> = names.iter().map(|name| format!("`{name}`")).collect();
        match quoted.split_last() {
            Some((last, rest)) if !rest.is_empty() => format!("{} or {last}", rest.join(", ")),
            _ => quoted.concat(),
        }
    }

    /// The message for a text that is none of `choices`.
    #[must_use]
    pub fn one_of(choices: &[&str]) -> String { format!("expected {}", one_of_fields(choices)) }

    /// Read the parameter `name`.
    pub fn param<T: Param>(map: &Map<String, Value>, name: &str, default: Option<Value>) -> Result<T, String> {
        let read = |value: &Value| T::from_json(value).map_err(|error| format!("`{name}`: {error}"));
        match map.get(name) {
            Some(value) if !value.is_null() || T::OPTIONAL => read(value),
            _ => match default {
                Some(default) => read(&default),
                None => T::absent().ok_or_else(|| format!("`{name}` is required")),
            },
        }
    }

    /// Read the field `name`, which only the code sets.
    pub fn field<T: DeserializeOwned>(map: &Map<String, Value>, name: &str) -> Result<T, String> {
        match map.get(name) {
            Some(value) => serde_json::from_value(value.clone()).map_err(|error| format!("`{name}`: {error}")),
            None => serde_json::from_value(Value::Null).map_err(|_| format!("`{name}` is required")),
        }
    }

    /// Write the parameter `name`. One that may be left out and has no value
    /// is left out.
    pub fn put_param<T: Param>(map: &mut Map<String, Value>, name: &str, value: &T) {
        let value = value.to_json();
        if !(T::OPTIONAL && value.is_null()) {
            map.insert(name.to_owned(), value);
        }
    }

    /// Write the field `name`.
    pub fn put_field<T: Serialize>(map: &mut Map<String, Value>, name: &str, value: &T) -> Result<(), String> {
        let value = serde_json::to_value(value).map_err(|error| format!("`{name}`: {error}"))?;
        map.insert(name.to_owned(), value);
        Ok(())
    }

    /// Refuse a value outside of its bounds, each given as it is worth and as
    /// a message shows it.
    pub fn range<T: Param + PartialOrd>(
        name: &str,
        value: &T,
        min: Option<(Value, &str)>,
        max: Option<(Value, &str)>,
    ) -> Result<(), OpError> {
        let bound = |bound: &Option<(Value, &str)>| -> Result<Option<T>, OpError> {
            bound
                .as_ref()
                .map(|(json, shown)| {
                    T::from_json(json).map_err(|error| {
                        OpError::Config(format!("the bound `{shown}` of `{name}` is unusable: {error}"))
                    })
                })
                .transpose()
        };
        let below = bound(&min)?.is_some_and(|min| *value < min);
        let above = bound(&max)?.is_some_and(|max| *value > max);
        if !below && !above {
            return Ok(());
        }
        Err(OpError::Config(match (min, max) {
            (Some((_, min)), Some((_, max))) => format!("`{name}` must be between {min} and {max}"),
            (Some((_, min)), None) => format!("`{name}` must be at least {min}"),
            (None, Some((_, max))) => format!("`{name}` must be at most {max}"),
            (None, None) => return Ok(()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Operator;
    use serde_json::json;

    /// Where to start.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, crate::Param)]
    enum Start {
        Earliest,
        #[param(rename = "now")]
        RightNow,
        LatestFirst,
    }

    /// Does things.
    ///
    /// A second paragraph the catalogue does not show.
    #[derive(Debug, Clone, PartialEq, crate::Operator)]
    #[operator("test/thing@1", check = thing_is_sane)]
    struct Thing {
        /// Fixed by the code.
        topic: String,
        tags: Vec<String>,
        note: Option<String>,
        /// Where to start
        /// on a new topic.
        #[param(default = "earliest")]
        start: Start,
        /// How many.
        #[param(min = 1, max = 64)]
        partitions: u32,
        #[param(default = -5, min = -10)]
        offset: i64,
        #[param(default = 0.5, max = 1.0)]
        ratio: f64,
        #[param(default = true)]
        enabled: bool,
        #[param(default = "10s", min = "1ms")]
        timeout: Duration,
        #[param(max = "1h")]
        linger: Option<Duration>,
        #[param]
        token: Secret,
        #[param(rename = "extra-headers")]
        headers: Option<Value>,
    }

    fn thing_is_sane(thing: &Thing) -> Result<(), OpError> {
        if thing.topic.is_empty() {
            return Err(OpError::Config("`topic` is empty".into()));
        }
        Ok(())
    }

    /// Nothing to configure.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, crate::Operator)]
    #[operator("test/unit@1", about = "Said otherwise.")]
    struct Unit;

    fn minimal() -> Value { json!({"topic": "t", "tags": ["a"], "partitions": 4, "token": {"secret": "tok"}}) }

    fn thing(config: Value) -> Result<Thing, String> { serde_json::from_value(config).map_err(|e| e.to_string()) }

    #[test]
    fn an_operator_is_named_and_described_by_its_structure() {
        assert_eq!(Thing::OP, "test/thing@1");
        assert_eq!(Thing::ABOUT, "Does things.");
        assert_eq!(Unit::OP, "test/unit@1");
        assert_eq!(Unit::ABOUT, "Said otherwise.");
        assert!(Unit::params().is_empty());
        let params = Thing::params();
        let names: Vec<&str> = params.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "start",
                "partitions",
                "offset",
                "ratio",
                "enabled",
                "timeout",
                "linger",
                "token",
                "extra-headers"
            ]
        );
        let by_name = |name: &str| params.iter().find(|p| p.name == name).unwrap().clone();
        let start = by_name("start");
        assert_eq!(start.kind, "text");
        assert_eq!(start.choices, ["earliest", "now", "latest-first"]);
        assert_eq!(start.default, Some(json!("earliest")));
        assert_eq!(start.description, "Where to start on a new topic.");
        assert!(!start.required);
        let partitions = by_name("partitions");
        assert_eq!((partitions.kind.as_str(), partitions.required), ("integer", true));
        assert_eq!(partitions.default, None);
        assert_eq!(by_name("offset").default, Some(json!(-5)));
        assert_eq!(by_name("ratio").kind, "number");
        assert_eq!(by_name("enabled").kind, "boolean");
        assert_eq!(by_name("timeout").kind, "duration");
        let linger = by_name("linger");
        assert_eq!((linger.kind.as_str(), linger.required), ("duration", false));
        let token = by_name("token");
        assert_eq!((token.kind.as_str(), token.required), ("secret", true));
        assert_eq!(by_name("extra-headers").kind, "json");
    }

    #[test]
    fn a_configuration_takes_its_defaults_and_round_trips() {
        let read = thing(minimal()).unwrap();
        assert_eq!(
            read,
            Thing {
                topic: "t".into(),
                tags: vec!["a".into()],
                note: None,
                start: Start::Earliest,
                partitions: 4,
                offset: -5,
                ratio: 0.5,
                enabled: true,
                timeout: Duration::from_secs(10),
                linger: None,
                token: Secret::named("tok"),
                headers: None,
            }
        );
        assert!(read.check().is_ok());
        let written = serde_json::to_value(&read).unwrap();
        assert_eq!(
            written,
            json!({
                "topic": "t", "tags": ["a"], "note": null, "start": "earliest", "partitions": 4,
                "offset": -5, "ratio": 0.5, "enabled": true, "timeout": "10s",
                "token": {"secret": "tok"},
            })
        );
        assert_eq!(thing(written).unwrap(), read);

        let mut full = minimal();
        full["start"] = json!("now");
        full["timeout"] = json!(250);
        full["linger"] = json!("5m");
        full["token"] = json!("plain");
        full["extra-headers"] = json!({"a": 1});
        full["note"] = json!("hello");
        let read = thing(full).unwrap();
        assert_eq!(read.start, Start::RightNow);
        assert_eq!(read.timeout, Duration::from_millis(250));
        assert_eq!(read.linger, Some(Duration::from_secs(300)));
        assert_eq!(read.token, Secret::literal("plain"));
        assert_eq!(read.headers, Some(json!({"a": 1})));
        assert_eq!(read.note.as_deref(), Some("hello"));
        assert_eq!(thing(serde_json::to_value(&read).unwrap()).unwrap(), read);
    }

    #[test]
    fn a_refused_configuration_says_why() {
        let with = |name: &str, value: Value| {
            let mut config = minimal();
            config[name] = value;
            thing(config).unwrap_err()
        };
        let without = |name: &str| {
            let mut config = minimal();
            config.as_object_mut().unwrap().remove(name);
            thing(config).unwrap_err()
        };
        assert_eq!(without("partitions"), "`partitions` is required");
        assert_eq!(without("token"), "`token` is required");
        assert_eq!(without("topic"), "`topic` is required");
        assert_eq!(with("partitions", json!("four")), "`partitions`: expected an integer");
        assert_eq!(
            with("partitions", json!(-1)),
            "`partitions`: expected an integer between 0 and 4294967295"
        );
        assert_eq!(
            with("start", json!("yesterday")),
            "`start`: expected `earliest`, `now` or `latest-first`"
        );
        assert_eq!(with("enabled", json!(1)), "`enabled`: expected `true` or `false`");
        assert_eq!(with("ratio", json!("x")), "`ratio`: expected a number");
        assert_eq!(with("timeout", json!("soon")), "`timeout`: invalid duration `soon`");
        assert_eq!(
            with("timeout", json!(true)),
            "`timeout`: expected a duration such as `5m`"
        );
        assert_eq!(
            with("token", json!({"name": "x"})),
            "`token`: expected a text or `{\"secret\": \"<name>\"}`"
        );
        assert!(with("tags", json!("a")).starts_with("`tags`: "));
        assert!(with("partition", json!(1)).starts_with("unknown field `partition`: expected `topic`, "));
        assert_eq!(thing(json!([1])).unwrap_err(), "expected an object");
        // A parameter given as `null` takes its default.
        assert_eq!(thing(with_null("start")).unwrap().start, Start::Earliest);
    }

    fn with_null(name: &str) -> Value {
        let mut config = minimal();
        config[name] = Value::Null;
        config
    }

    #[test]
    fn bounds_are_checked_in_words() {
        let ok = thing(minimal()).unwrap();
        let refused = |thing: Thing| thing.check().unwrap_err().to_string();
        let prefix = "invalid operator configuration: ";
        assert_eq!(
            refused(Thing {
                partitions: 0,
                ..ok.clone()
            }),
            format!("{prefix}`partitions` must be between 1 and 64")
        );
        assert_eq!(
            refused(Thing {
                partitions: 65,
                ..ok.clone()
            }),
            format!("{prefix}`partitions` must be between 1 and 64")
        );
        assert!(
            Thing {
                partitions: 64,
                ..ok.clone()
            }
            .check()
            .is_ok()
        );
        assert_eq!(
            refused(Thing {
                offset: -11,
                ..ok.clone()
            }),
            format!("{prefix}`offset` must be at least -10")
        );
        assert_eq!(
            refused(Thing {
                ratio: 1.5,
                ..ok.clone()
            }),
            format!("{prefix}`ratio` must be at most 1")
        );
        assert_eq!(
            refused(Thing {
                timeout: Duration::ZERO,
                ..ok.clone()
            }),
            format!("{prefix}`timeout` must be at least 1ms")
        );
        assert_eq!(
            refused(Thing {
                linger: Some(Duration::from_secs(7200)),
                ..ok.clone()
            }),
            format!("{prefix}`linger` must be at most 1h")
        );
        assert!(
            Thing {
                linger: Some(Duration::from_secs(3600)),
                ..ok.clone()
            }
            .check()
            .is_ok()
        );
        // The check of the author runs too.
        assert_eq!(
            refused(Thing {
                topic: String::new(),
                ..ok
            }),
            format!("{prefix}`topic` is empty")
        );
    }

    #[test]
    fn an_operator_without_fields_takes_any_configuration() {
        assert_eq!(serde_json::to_value(Unit).unwrap(), Value::Null);
        for config in [Value::Null, json!({}), json!({"ignored": 1})] {
            assert_eq!(serde_json::from_value::<Unit>(config).unwrap(), Unit);
        }
        assert!(Unit.check().is_ok());
    }

    #[test]
    fn a_secret_never_shows_its_text() {
        assert_eq!(format!("{:?}", Secret::literal("hunter2")), "Secret(<redacted>)");
        assert_eq!(format!("{:?}", Secret::named("token")), "Secret(named `token`)");
        for secret in [Secret::literal("a"), Secret::named("b")] {
            let json = serde_json::to_value(&secret).unwrap();
            assert_eq!(serde_json::from_value::<Secret>(json).unwrap(), secret);
        }
    }
}
