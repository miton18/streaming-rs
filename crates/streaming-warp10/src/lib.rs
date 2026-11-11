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

use moonpool_core::Providers;
use streaming_core::{Registry, SinkConnector, TypeTag};

pub use config::Warp10Config;
pub use gts::{Gts, GtsValue, TimeUnit};
pub use sink::Warp10Sink;

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
}
