//! Configuration of the sink.

use crate::{gts::TimeUnit, http::Endpoint};
use std::{fmt, time::Duration};
use streaming_core::OpError;

/// Default request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// Default maximum size of the body of one request.
pub const DEFAULT_MAX_BATCH_BYTES: usize = 1024 * 1024;

/// Validated configuration of a [`crate::Warp10Writer`].
///
/// Built from the node configuration by [`crate::Warp10Sink::config`], once
/// the token is known.
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
            return Err(OpError::Config("the write token contains a control character".into()));
        }
        if self.timeout.is_zero() {
            return Err(OpError::Config("timeout must be positive".into()));
        }
        if self.max_batch_bytes == 0 {
            return Err(OpError::Config("maxBatchBytes must be positive".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configuration_starts_from_the_defaults() {
        let config = Warp10Config::new("http://h:8080/api/v0/update", "t").unwrap();
        assert_eq!(config.time_unit, TimeUnit::Us);
        assert_eq!(config.timeout, DEFAULT_TIMEOUT);
        assert_eq!(config.max_batch_bytes, DEFAULT_MAX_BATCH_BYTES);
        assert_eq!(config.endpoint.address(), "h:8080");
        assert!(config.connect_address.is_none());
    }

    #[test]
    fn invalid_configurations() {
        assert!(Warp10Config::new("ftp://h/", "t").is_err());
        assert!(Warp10Config::new("http://h/", "").is_err());
        assert!(Warp10Config::new("http://h/", "a\nb").is_err());
        let mut config = Warp10Config::new("http://h/", "t").unwrap();
        config.timeout = Duration::ZERO;
        assert!(config.validate().is_err());
        let mut config = Warp10Config::new("http://h/", "t").unwrap();
        config.max_batch_bytes = 0;
        assert!(config.validate().is_err());
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn https_without_the_tls_feature_is_a_clear_error() {
        let OpError::Config(message) = Warp10Config::new("https://warp.example/api/v0/update", "t").unwrap_err() else {
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
