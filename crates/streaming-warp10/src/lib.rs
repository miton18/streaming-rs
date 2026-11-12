//! Warp 10 sink connector: writes [`Gts`] points to the HTTP ingress
//! (`/api/v0/update`) of a Warp 10 platform.
//!
//! All I/O goes through the Moonpool provider traits (`Providers`): the
//! connection through `network().connect`, the deadline through
//! `time().timeout`. The HTTP/1.1 client is written here (module [`http`]) so
//! that the very same code runs on real sockets (`TokioProviders`) and in
//! deterministic simulation (`SimProviders`). No `tokio::net`, no
//! `tokio::time`, no `Instant`, no randomness.
//!
//! # Wire format
//!
//! One point per line, `TS// CLASS{LABELS} VALUE`, percent-encoded names, sorted
//! labels, see [`gts`].
//!
//! # Why a retried batch is harmless (idempotence)
//!
//! A sink must be idempotent: after any error the runtime
//! sends the same elements again. A Warp 10 data point is identified by its
//! series (class + labels) and its timestamp; writing a point whose
//! (class, labels, timestamp) already exists overwrites it. The sink encodes
//! an element deterministically (same class, same sorted labels, same
//! timestamp, same value), so a batch written twice, or half written then
//! written again, leaves exactly the data of a batch written once. An error
//! (HTTP status other than 200, timeout, connection reset, torn response) only
//! means "outcome unknown"; the caller retries.
//!
//! The guarantee holds as long as an element is deterministic in the emit it
//! comes from, and as long as two different elements do not share (class,
//! labels, timestamp) with different values, in which case the last write wins.
//!
//! # Configuration (`std/warp10-sink@1`)
//!
//! ```json
//! {"endpoint": "https://host/api/v0/update",
//!  "token": "..." | {"secret": "name"},
//!  "timeUnit": "us", "timeout": "10s", "maxBatchBytes": 1048576}
//! ```
//!
//! `https://` needs the `tls` cargo feature (rustls over the futures-io
//! stream, ring, webpki-roots); without it an https endpoint is a
//! configuration error.
//!
//! A batch is split into several requests when its encoding exceeds
//! `max_batch_bytes`. The requests are sent one after the other; if one fails
//! the whole batch is reported as failed (the first requests may have been
//! applied, which is fine, see above).

pub mod config;
pub mod fake;
pub mod gts;
pub mod http;
mod sink;
#[cfg(feature = "tls")]
mod tls;

pub use config::Warp10Config;
pub use gts::{Gts, GtsValue, TimeUnit};
use moonpool_core::Providers;
pub use sink::Warp10Writer;
use std::time::Duration;
use streaming_core::{Context, OpError, Operator, Registry, Secret, Sink, SinkPipe};

/// Reference under which the sink is registered.
pub const SINK_OP: &str = "std/warp10-sink@1";

/// Writes Gts points to a Warp 10 platform through /api/v0/update.
///
/// A retried batch is harmless: see the crate documentation.
#[derive(Debug, Clone, PartialEq, Eq, Operator)]
#[operator("std/warp10-sink@1", check = Warp10Sink::check_endpoint)]
pub struct Warp10Sink {
    /// Update endpoint of the platform, e.g. `https://host/api/v0/update`.
    /// `https://` needs a build with the `tls` feature.
    #[param]
    pub endpoint: String,
    /// Write token, sent as `X-Warp10-Token`. Never logged.
    #[param]
    pub token: Secret,
    /// Time unit the platform was configured with; timestamps are converted
    /// to it.
    #[param(default = "us")]
    pub time_unit: TimeUnit,
    /// Deadline of one request: connect, send, and receive the answer.
    #[param(default = "10s", min = "1ms")]
    pub timeout: Duration,
    /// Largest body of one request, in bytes; a larger batch is split into
    /// several requests.
    #[param(default = 1048576, min = 1)]
    pub max_batch_bytes: usize,
}

impl Warp10Sink {
    /// A sink writing to `endpoint` with `token`, and the defaults for the
    /// rest.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, token: Secret) -> Self {
        Self {
            endpoint: endpoint.into(),
            token,
            time_unit: TimeUnit::default(),
            timeout: config::DEFAULT_TIMEOUT,
            max_batch_bytes: config::DEFAULT_MAX_BATCH_BYTES,
        }
    }

    /// What can be refused without the token: the endpoint.
    fn check_endpoint(&self) -> Result<(), OpError> { self.config("unset").map(drop) }

    /// The validated configuration, once the token is known.
    ///
    /// # Errors
    ///
    /// The endpoint cannot be used, or the token cannot travel in a header.
    pub fn config(&self, token: impl Into<String>) -> Result<Warp10Config, OpError> {
        let mut config = Warp10Config::new(&self.endpoint, token)?;
        config.time_unit = self.time_unit;
        config.timeout = self.timeout;
        config.max_batch_bytes = self.max_batch_bytes;
        config.validate()?;
        Ok(config)
    }
}

impl Sink<Gts> for Warp10Sink {
    async fn start<P: Providers>(&self, ctx: &Context<P>, mut input: SinkPipe<Gts>) -> Result<(), OpError> {
        let writer = Warp10Writer::new(ctx.providers.clone(), self.config(ctx.reveal(&self.token)?)?)?;
        // A failed batch comes back: its outcome is unknown, and writing the
        // same points twice is writing them once.
        input.write_with(&writer).await;
        Ok(())
    }
}

/// Register `std/warp10-sink@1` (input: [`Gts`]).
pub fn register<P: Providers>(registry: &mut Registry<P>) { registry.sink::<Gts, Warp10Sink>(); }

#[cfg(test)]
mod tests {
    use super::*;
    use moonpool_core::TokioProviders;
    use serde_json::{Value, json};
    use streaming_core::OpRef;

    fn parse(config: Value) -> Result<Warp10Sink, String> { serde_json::from_value(config).map_err(|e| e.to_string()) }

    #[test]
    fn the_sink_describes_its_configuration() {
        let mut registry = Registry::<TokioProviders>::empty();
        register(&mut registry);
        let descriptor = registry.catalog().get(&OpRef::new(SINK_OP).unwrap()).unwrap().clone();
        assert_eq!(Warp10Sink::OP, SINK_OP);
        assert_eq!(
            descriptor.description,
            "Writes Gts points to a Warp 10 platform through /api/v0/update."
        );
        let params: Vec<(&str, &str, bool)> = descriptor
            .params
            .iter()
            .map(|p| (p.name.as_str(), p.kind.as_str(), p.required))
            .collect();
        assert_eq!(
            params,
            [
                ("endpoint", "text", true),
                ("token", "secret", true),
                ("timeUnit", "text", false),
                ("timeout", "duration", false),
                ("maxBatchBytes", "integer", false),
            ]
        );
        assert!(descriptor.params.iter().all(|p| !p.description.is_empty()));
        assert_eq!(descriptor.params[2].choices, ["ms", "us", "ns"]);
        assert_eq!(descriptor.params[2].default, Some(json!("us")));
        assert_eq!(descriptor.params[3].default, Some(json!("10s")));
        assert_eq!(
            descriptor.params[4].default,
            Some(json!(config::DEFAULT_MAX_BATCH_BYTES))
        );
    }

    #[test]
    fn defaults() {
        let sink = parse(json!({"endpoint": "http://h:8080/api/v0/update", "token": "t"})).unwrap();
        assert_eq!(
            sink,
            Warp10Sink::new("http://h:8080/api/v0/update", Secret::literal("t"))
        );
        sink.check().unwrap();
        let config = sink.config("t").unwrap();
        assert_eq!(config.time_unit, TimeUnit::Us);
        assert_eq!(config.timeout, Duration::from_secs(10));
        assert_eq!(config.max_batch_bytes, 1_048_576);
        assert_eq!(config.endpoint.address(), "h:8080");
        assert_eq!(config.token, "t");
    }

    #[test]
    fn everything_set() {
        let sink = parse(
            json!({"endpoint": "http://h/api/v0/update", "token": {"secret": "w"}, "timeUnit": "ns",
                                "timeout": "250ms", "maxBatchBytes": 100}),
        )
        .unwrap();
        assert_eq!(sink.token, Secret::named("w"));
        let config = sink.config("resolved").unwrap();
        assert_eq!(config.time_unit, TimeUnit::Ns);
        assert_eq!(config.timeout, Duration::from_millis(250));
        assert_eq!(config.max_batch_bytes, 100);
        assert_eq!(config.token, "resolved");
        // What the store holds parses back to the same sink.
        assert_eq!(parse(serde_json::to_value(&sink).unwrap()).unwrap(), sink);
    }

    #[test]
    fn invalid_configurations() {
        let refused = |extra: Value| {
            let mut value = json!({"endpoint": "http://h/", "token": "t"});
            value
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            match parse(value) {
                Ok(sink) => sink.check().is_err(),
                Err(_) => true,
            }
        };
        assert!(!refused(json!({})));
        assert!(refused(json!({"timeUnit": "s"})));
        assert!(refused(json!({"timeout": "soon"})));
        assert!(refused(json!({"timeout": "0s"})));
        assert!(refused(json!({"maxBatchBytes": 0})));
        assert!(refused(json!({"unknown": 1})));
        assert!(refused(json!({"endpoint": "ftp://h/"})));
        assert!(refused(json!({"token": 1})));
        assert!(parse(json!({"token": "t"})).is_err());
        assert!(parse(json!(null)).is_err());
        // The token is only known when the sink starts: it is checked then.
        let sink = parse(json!({"endpoint": "http://h/", "token": "t"})).unwrap();
        assert!(sink.config("").is_err());
        assert!(sink.config("a\nb").is_err());
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn https_without_the_tls_feature_is_a_clear_error() {
        let sink = parse(json!({"endpoint": "https://warp.example/api/v0/update", "token": "t"})).unwrap();
        let OpError::Config(message) = sink.check().unwrap_err() else {
            panic!("expected a configuration error")
        };
        assert!(message.contains("https") && message.contains("`tls` cargo feature"));
    }
}
