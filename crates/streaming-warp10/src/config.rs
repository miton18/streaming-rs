//! Configuration of the sink.

use std::fmt;
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;
use streaming_core::{OpError, parse_duration};

use crate::gts::TimeUnit;
use crate::http::Endpoint;

/// Default request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// Default maximum size of the body of one request.
pub const DEFAULT_MAX_BATCH_BYTES: usize = 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    endpoint: String,
    token: Value,
    #[serde(default)]
    time_unit: Option<String>,
    #[serde(default)]
    timeout: Option<String>,
    #[serde(default)]
    max_batch_bytes: Option<usize>,
}

/// Validated configuration of a [`crate::Warp10Sink`].
///
/// JSON form (`std/warp10-sink@1`):
///
/// ```json
/// {"endpoint": "https://host/api/v0/update",
///  "token": "..." | {"secret": "name"},
///  "time_unit": "us", "timeout": "10s", "max_batch_bytes": 1048576}
/// ```
///
/// Only `endpoint` and `token` are required.
#[derive(Clone, PartialEq, Eq)]
pub struct Warp10Config {
    /// Where to POST (`http://` or, with the `tls` feature, `https://`).
    pub endpoint: Endpoint,
    /// Write token, sent as `X-Warp10-Token`. Never logged nor put in errors.
    pub token: String,
    /// Time unit of the platform; timestamps are converted to it.
    pub time_unit: TimeUnit,
    /// Deadline of one request (connect, send, receive the answer).
    pub timeout: Duration,
    /// A batch whose encoding is larger is split in several requests.
    pub max_batch_bytes: usize,
    /// Address handed to `NetworkProvider::connect` instead of
    /// `host:port` of the endpoint (simulations address nodes by bare IP).
    pub connect_address: Option<String>,
    /// DER certificates trusted in addition to the web PKI roots.
    #[cfg(feature = "tls")]
    pub tls_extra_roots_der: Vec<Vec<u8>>,
}

impl fmt::Debug for Warp10Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Warp10Config")
            .field("endpoint", &self.endpoint)
            .field("token", &"<redacted>")
            .field("time_unit", &self.time_unit)
            .field("timeout", &self.timeout)
            .field("max_batch_bytes", &self.max_batch_bytes)
            .field("connect_address", &self.connect_address)
            .finish_non_exhaustive()
    }
}

impl Warp10Config {
    /// A configuration with the defaults for everything but the endpoint and
    /// the token.
    pub fn new(endpoint: &str, token: impl Into<String>) -> Result<Self, OpError> {
        let endpoint = Endpoint::parse(endpoint).map_err(OpError::Config)?;
        let config = Self {
            endpoint,
            token: token.into(),
            time_unit: TimeUnit::default(),
            timeout: DEFAULT_TIMEOUT,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            connect_address: None,
            #[cfg(feature = "tls")]
            tls_extra_roots_der: Vec::new(),
        };
        config.validate()?;
        Ok(config)
    }

    /// Parse the JSON node configuration. `resolve_token` turns the `token`
    /// value (a literal string or `{"secret": "name"}`) into the token, e.g.
    /// `|v| ctx.secret_or_literal(v)`.
    pub fn from_json(
        config: &Value,
        resolve_token: impl FnOnce(&Value) -> Result<String, OpError>,
    ) -> Result<Self, OpError> {
        let raw: RawConfig = serde_json::from_value(config.clone())
            .map_err(|e| OpError::Config(format!("std/warp10-sink@1: {e}")))?;
        let mut out = Self::new(&raw.endpoint, resolve_token(&raw.token)?)?;
        if let Some(unit) = raw.time_unit {
            out.time_unit = TimeUnit::parse(&unit).map_err(OpError::Config)?;
        }
        if let Some(timeout) = raw.timeout {
            out.timeout =
                parse_duration(&timeout).map_err(|e| OpError::Config(format!("timeout: {e}")))?;
        }
        if let Some(max) = raw.max_batch_bytes {
            out.max_batch_bytes = max;
        }
        out.validate()?;
        Ok(out)
    }

    /// Check the invariants; called by the constructors.
    pub fn validate(&self) -> Result<(), OpError> {
        if self.endpoint.tls && !cfg!(feature = "tls") {
            return Err(OpError::Config(
                "https endpoints need TLS, which this build does not have: enable the \
                 `tls` cargo feature of streaming-warp10 (or use an http:// endpoint)"
                    .into(),
            ));
        }
        if self.token.is_empty() {
            return Err(OpError::Config("the write token is empty".into()));
        }
        if self.token.contains(['\r', '\n', '\0']) {
            return Err(OpError::Config(
                "the write token contains a control character".into(),
            ));
        }
        if self.timeout.is_zero() {
            return Err(OpError::Config("timeout must be positive".into()));
        }
        if self.max_batch_bytes == 0 {
            return Err(OpError::Config("max_batch_bytes must be positive".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn literal(v: &Value) -> Result<String, OpError> {
        v.as_str()
            .map(str::to_owned)
            .ok_or_else(|| OpError::Config("not a literal".into()))
    }

    #[test]
    fn defaults() {
        let c = Warp10Config::from_json(
            &json!({"endpoint": "http://h:8080/api/v0/update", "token": "t"}),
            literal,
        )
        .unwrap();
        assert_eq!(c.time_unit, TimeUnit::Us);
        assert_eq!(c.timeout, Duration::from_secs(10));
        assert_eq!(c.max_batch_bytes, 1_048_576);
        assert_eq!(c.endpoint.address(), "h:8080");
        assert_eq!(c.token, "t");
    }

    #[test]
    fn everything_set() {
        let c = Warp10Config::from_json(
            &json!({"endpoint": "http://h/api/v0/update", "token": "t", "time_unit": "ns",
                    "timeout": "250ms", "max_batch_bytes": 100}),
            literal,
        )
        .unwrap();
        assert_eq!(c.time_unit, TimeUnit::Ns);
        assert_eq!(c.timeout, Duration::from_millis(250));
        assert_eq!(c.max_batch_bytes, 100);
    }

    #[test]
    fn the_token_resolver_receives_the_raw_value() {
        let c = Warp10Config::from_json(
            &json!({"endpoint": "http://h/", "token": {"secret": "w"}}),
            |v| {
                assert_eq!(v, &json!({"secret": "w"}));
                Ok("resolved".into())
            },
        )
        .unwrap();
        assert_eq!(c.token, "resolved");
        // an unresolvable secret is a configuration error
        assert!(
            Warp10Config::from_json(&json!({"endpoint": "http://h/", "token": 1}), literal)
                .is_err()
        );
    }

    #[test]
    fn invalid_configurations() {
        let base = |extra: Value| {
            let mut v = json!({"endpoint": "http://h/", "token": "t"});
            v.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            Warp10Config::from_json(&v, literal)
        };
        assert!(base(json!({"time_unit": "s"})).is_err());
        assert!(base(json!({"timeout": "soon"})).is_err());
        assert!(base(json!({"timeout": "0s"})).is_err());
        assert!(base(json!({"max_batch_bytes": 0})).is_err());
        assert!(base(json!({"unknown": 1})).is_err());
        assert!(base(json!({"endpoint": "ftp://h/"})).is_err());
        assert!(base(json!({"token": ""})).is_err());
        assert!(base(json!({"token": "a\nb"})).is_err());
        assert!(Warp10Config::from_json(&json!({"token": "t"}), literal).is_err());
        assert!(Warp10Config::from_json(&json!(null), literal).is_err());
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn https_without_the_tls_feature_is_a_clear_error() {
        let err = Warp10Config::from_json(
            &json!({"endpoint": "https://warp.example/api/v0/update", "token": "t"}),
            literal,
        )
        .unwrap_err();
        let OpError::Config(message) = err else {
            panic!("expected a configuration error")
        };
        assert!(message.contains("https") && message.contains("`tls` cargo feature"));
    }

    #[cfg(feature = "tls")]
    #[test]
    fn https_with_the_tls_feature_is_accepted() {
        let c = Warp10Config::new("https://warp.example/api/v0/update", "t").unwrap();
        assert!(c.endpoint.tls);
    }

    #[test]
    fn debug_never_shows_the_token() {
        let c = Warp10Config::new("http://h/", "super-secret").unwrap();
        assert!(!format!("{c:?}").contains("super-secret"));
    }
}
