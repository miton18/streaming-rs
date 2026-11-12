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
//! `SinkConnector::write` must be idempotent: after any error the runtime
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
//!  "time_unit": "us", "timeout": "10s", "max_batch_bytes": 1048576}
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

use std::sync::Arc;

pub use config::Warp10Config;
pub use gts::{Gts, GtsValue, TimeUnit};
use moonpool_core::Providers;
pub use sink::Warp10Sink;
use streaming_core::{ParamSpec, Registry, SinkConnector, TypeTag};

/// Reference under which the sink is registered.
pub const SINK_OP: &str = "std/warp10-sink@1";

/// Register `std/warp10-sink@1` (input: [`Gts`]).
pub fn register<P: Providers>(registry: &mut Registry<P>) {
    registry.register_sink(
        SINK_OP,
        "Writes Gts points to a Warp 10 platform through /api/v0/update.",
        TypeTag::of::<Gts>(),
        |context, config| {
            let config = Warp10Config::from_json(config, |token| context.secret_or_literal(token))?;
            let sink = Warp10Sink::new(context.providers.clone(), config)?;
            Ok(Arc::new(sink) as Arc<dyn SinkConnector>)
        },
    );
    registry.with_params(SINK_OP, sink_params());
}

/// What the configuration of `std/warp10-sink@1` is made of.
fn sink_params() -> Vec<ParamSpec> {
    vec![
        ParamSpec::new(
            "endpoint",
            "text",
            "Update endpoint of the platform, e.g. `https://host/api/v0/update`. `https://` \
             needs a build with the `tls` feature.",
        )
        .required(),
        ParamSpec::new(
            "token",
            "secret",
            "Write token, sent as `X-Warp10-Token`. Never logged.",
        )
        .required(),
        ParamSpec::new(
            "time_unit",
            "text",
            "Time unit the platform was configured with; timestamps are converted to it.",
        )
        .choices(["ms", "us", "ns"])
        .default("us"),
        ParamSpec::new(
            "timeout",
            "duration",
            "Deadline of one request: connect, send, and receive the answer.",
        )
        .default("10s"),
        ParamSpec::new(
            "max_batch_bytes",
            "integer",
            "Largest body of one request, in bytes; a larger batch is split into several \
             requests.",
        )
        .default(config::DEFAULT_MAX_BATCH_BYTES),
    ]
}

#[cfg(test)]
mod tests {
    use moonpool_core::TokioProviders;
    use serde_json::{Value, json};
    use streaming_core::{OpError, OpRef};

    use super::*;

    fn parse(config: &Value) -> Result<Warp10Config, OpError> {
        Warp10Config::from_json(config, |token| {
            token
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| OpError::Config("token must be a string".into()))
        })
    }

    /// The descriptions shown by the designer match what the parser accepts:
    /// every described parameter exists, the required ones are required and
    /// nothing else is, the defaults are the parser's and every choice parses.
    #[test]
    fn parameter_descriptions_match_the_parser() {
        let mut registry = Registry::<TokioProviders>::empty();
        register(&mut registry);
        let params = registry
            .catalog()
            .get(&OpRef::new(SINK_OP).unwrap())
            .unwrap()
            .params
            .clone();
        assert!(!params.is_empty());
        let samples = json!({"endpoint": "http://warp10.example/api/v0/update", "token": "t"});
        let config = |pick: &dyn Fn(&ParamSpec) -> bool| {
            let mut config = json!({});
            for p in params.iter().filter(|p| pick(p)) {
                assert!(!p.description.is_empty(), "`{}` is not described", p.name);
                config[&p.name] = p
                    .default
                    .clone()
                    .or_else(|| samples.get(&p.name).cloned())
                    .unwrap_or_else(|| panic!("no sample value for `{}`", p.name));
            }
            config
        };
        let required = config(&|p| p.required);
        let minimal = parse(&required).unwrap();
        assert_eq!(parse(&config(&|_| true)).unwrap(), minimal);
        for p in params.iter().filter(|p| p.required) {
            let mut without = required.clone();
            without.as_object_mut().unwrap().remove(&p.name);
            assert!(parse(&without).is_err(), "`{}` is not required", p.name);
        }
        for p in &params {
            for choice in &p.choices {
                let mut with = required.clone();
                with[&p.name] = json!(choice);
                parse(&with).unwrap_or_else(|e| panic!("{}={choice}: {e}", p.name));
            }
        }
    }
}
