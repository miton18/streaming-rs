//! Apache Pulsar connectors for `streaming-rs`, built on
//! [Magnetar](https://github.com/CleverCloud/magnetar)'s moonpool engine.
//!
//! | reference | kind | type | configuration |
//! |---|---|---|---|
//! | `std/pulsar-source@1` | source | out: [`PulsarMessage`] | see below |
//! | `std/pulsar-sink@1` | sink | in: [`PulsarOutput`] | see below |
//!
//! # Engine
//!
//! Both connectors are generic over the framework's [`Providers`] and drive
//! Magnetar's `MoonpoolEngine<P>` (`magnetar::runtime_moonpool`): network,
//! timers and tasks all go through `P`. In production `P` is
//! `moonpool_core::TokioProviders`, which Magnetar documents as the supported
//! way to run its moonpool engine against a real broker; under
//! `moonpool_sim::SimProviders` the same code runs in deterministic
//! simulation. The tokio engine (`magnetar::TokioEngine`) is not used: its
//! timers and sockets are tokio's, not the providers'. Known engine
//! limitation: the moonpool producer only publishes uncompressed messages
//! (the consumer reads every codec).
//!
//! # Source
//!
//! ```json
//! {"service_url": "pulsar://host:6650", "topic": "persistent://tenant/ns/name",
//!  "subscription": "name", "token": "<jwt>" | {"secret": "name"},
//!  "event_time": "publish" | "event", "receiver_queue_size": 1000,
//!  "initial_position": "earliest" | "latest", "tls_trust_certs_file": "/ca.pem"}
//! ```
//!
//! - `partitions()` asks the broker for the partitioned-topic metadata; a
//!   non-partitioned topic counts as one partition.
//! - `open(n, _)` subscribes to `<topic>-partition-<n>` (or the topic itself)
//!   with a durable **Failover** subscription: while two workers briefly hold a
//!   consumer on the same partition during a hand-over, the broker delivers to
//!   one of them only. A new subscription starts at `initial_position`
//!   (earliest by default). The `after` hint is ignored: the subscription's
//!   cursor only moves on `ack`, so the broker redelivers from at or before
//!   `after` and the runtime drops what it already processed.
//! - Positions are `Position(ledger_id, entry_id, batch_index + 1)` (see
//!   [`position_of`]).
//! - `poll(max, max_wait)` is Magnetar's `receive_batch`: up to `max_wait`
//!   (on the providers' clock) for the first message, then what is buffered.
//! - `ack(upto)` is a cumulative ack of the greatest delivered message at or
//!   before `upto`.
//! - `end_position()` is Pulsar's `GetLastMessageId` on the partition.
//!
//! # Sink
//!
//! ```json
//! {"service_url": "...", "topic": "persistent://tenant/ns/name", "token": ...,
//!  "producer_name": "orders-writer", "send_timeout_ms": 30000,
//!  "broker_dedup": {"position_bits": [24, 24, 7], "index_bits": 8}}
//! ```
//!
//! Every element becomes one unbatched message carrying the element's emit id
//! in the [`EMIT_ID_PROPERTY`] property, so any consumer can deduplicate;
//! `write` returns once the broker acknowledged every message of the batch,
//! and fails (the runtime then retries the same elements) if any send failed.
//! A configured `producer_name` gets the slice id appended (one producer name
//! per input slice). On a partitioned destination one producer is opened per
//! partition and elements are routed by [`route`].
//!
//! Without `broker_dedup` the sink is **at-least-once**. With it (opt-in,
//! needs a `producer_name` and deduplication enabled on the namespace), every
//! message leaves with a sequence id derived from the element's origin
//! ([`sequence`] says how, and when that is sound) and the broker drops
//! re-emissions: effectively-once towards Pulsar. Magnetar 1.8 has no
//! per-message sequence id — sends are numbered consecutively from the
//! producer's `initial_sequence_id` — so the sink reopens a partition's
//! producer at the derived id whenever the next id is not the producer's next
//! one, and skips, without sending, the elements whose id is at or below the
//! high-water mark the broker reports when the producer attaches. Cost: one
//! producer reopen (close + lookup + open round trips, after the receipts of
//! what was in flight) per discontinuity in a partition's sequence ids; choose
//! a layout that makes consecutive elements consecutive ids (e.g.
//! `[24, 39, 0]` + `index_bits: 0` for a non-batched Pulsar upstream with one
//! element per source message: ledger ids below 2^24, consecutive entries give
//! consecutive ids, one reopen per ledger rollover), and a non-partitioned
//! destination or keyless elements (which stick to one partition per slice in
//! this mode).

//! # Limitations
//!
//! - `end_position()` is a broker round trip (`GetLastMessageId`) each time the
//!   runtime asks (only while a partition is idle).
//! - The source ignores the `after` hint (no seek); redelivery restarts at the
//!   subscription cursor, i.e. after the last acknowledged position.
//! - Single-host service URLs only; TLS trusts the system roots or a PEM file
//!   (`tls_trust_certs_file`), no client certificates. The token is static:
//!   no refresh on an in-band `AUTH_CHALLENGE` beyond what the connection
//!   configuration carries.
//! - The moonpool producer cannot compress, and the sink does not batch.
//! - Broker-side deduplication is per destination partition; changing the
//!   destination's partition count while elements are replayed re-routes
//!   them and breaks effectively-once.

pub mod config;
pub mod position;
pub mod sequence;

mod client;
mod sink;
mod source;

use std::sync::Arc;

use moonpool_core::Providers;
use streaming_core::{ParamSpec, Registry, SinkConnector, SourceConnector, TypeTag};

pub use config::{
    ConnectionSettings, DedupConfig, EventTimeMode, InitialPosition, ServiceUrl, SinkConfig,
    SourceConfig,
};
pub use position::{end_position_of, partition_topic, position_of};
pub use sequence::{SequenceLayout, SequenceState};
pub use sink::{EMIT_ID_PROPERTY, PulsarOutput, PulsarSink, WriteStats, producer_name_for, route};
pub use source::{PulsarMessage, PulsarSource};

/// Reference of the source operator.
pub const SOURCE_OP: &str = "std/pulsar-source@1";
/// Reference of the sink operator.
pub const SINK_OP: &str = "std/pulsar-sink@1";

/// Register `std/pulsar-source@1` and `std/pulsar-sink@1`.
pub fn register<P: Providers>(registry: &mut Registry<P>) {
    registry.register_source::<PulsarMessage>(
        SOURCE_OP,
        "Reads a Pulsar topic through a durable Failover subscription, one slice per partition.",
        |context, config| {
            let config = SourceConfig::from_json(context, config)?;
            Ok(
                Arc::new(PulsarSource::new(context.providers.clone(), config))
                    as Arc<dyn SourceConnector>,
            )
        },
    );
    registry.register_sink(
        SINK_OP,
        "Publishes elements to a Pulsar topic, tagged with their emit id; optionally \
         deduplicated by the broker.",
        TypeTag::of::<PulsarOutput>(),
        |context, config| {
            let config = SinkConfig::from_json(context, config)?;
            Ok(Arc::new(PulsarSink::new(context.providers.clone(), config))
                as Arc<dyn SinkConnector>)
        },
    );
    registry.with_params(SOURCE_OP, source_params());
    registry.with_params(SINK_OP, sink_params());
}

/// The connection parameters shared by both operators.
fn connection_params() -> [ParamSpec; 3] {
    [
        ParamSpec::new(
            "service_url",
            "text",
            "Broker to connect to: `pulsar://host:6650`, or `pulsar+ssl://host:6651` for TLS. \
             A single host; the port defaults to 6650 (6651 with TLS).",
        )
        .required(),
        ParamSpec::new(
            "token",
            "secret",
            "JWT sent to the broker for authentication. Absent: no authentication.",
        ),
        ParamSpec::new(
            "tls_trust_certs_file",
            "text",
            "PEM file holding the certificate authorities to trust instead of the system \
             store. Only with a `pulsar+ssl://` service_url.",
        ),
    ]
}

/// What the configuration of `std/pulsar-source@1` is made of.
fn source_params() -> Vec<ParamSpec> {
    let [service_url, token, tls] = connection_params();
    vec![
        service_url,
        ParamSpec::new(
            "topic",
            "text",
            "Topic to read, as a full name `persistent://tenant/namespace/name`. A partitioned \
             topic gives one slice per partition.",
        )
        .required(),
        ParamSpec::new(
            "subscription",
            "text",
            "Name of the durable Failover subscription. Its cursor holds the read position: \
             keep it stable for the lifetime of the graph.",
        )
        .required(),
        token,
        tls,
        ParamSpec::new(
            "event_time",
            "text",
            "Event time of a record: `publish` takes the broker's publish time, `event` the \
             producer-set event time (the publish time when a message has none).",
        )
        .choices(["publish", "event"])
        .default("publish"),
        ParamSpec::new(
            "initial_position",
            "text",
            "Where a subscription that does not exist yet starts: `earliest` reads the topic \
             from its oldest retained message, `latest` only what is published afterwards.",
        )
        .choices(["earliest", "latest"])
        .default("earliest"),
        ParamSpec::new(
            "receiver_queue_size",
            "integer",
            "Messages the broker may push ahead of consumption, per partition. Positive.",
        )
        .default(1000),
    ]
}

/// What the configuration of `std/pulsar-sink@1` is made of.
fn sink_params() -> Vec<ParamSpec> {
    let [service_url, token, tls] = connection_params();
    vec![
        service_url,
        ParamSpec::new(
            "topic",
            "text",
            "Topic to publish to, as a full name `persistent://tenant/namespace/name` \
             (partitioned or not).",
        )
        .required(),
        token,
        tls,
        ParamSpec::new(
            "producer_name",
            "text",
            "Base name of the producers; the slice id is appended to it. Required by \
             broker_dedup. Absent: the broker names them.",
        ),
        ParamSpec::new(
            "send_timeout_ms",
            "integer",
            "Milliseconds to wait for the broker to acknowledge a message before the write \
             fails and is retried. 0 waits forever.",
        )
        .default(30_000),
        ParamSpec::new(
            "broker_dedup",
            "json",
            "Opt-in broker-side deduplication (effectively-once towards Pulsar), e.g. \
             `{\"position_bits\": [24, 39, 0], \"index_bits\": 0}`: bits of the sequence id \
             given to the three components of the origin position and to the index of an \
             element within it (63 bits at most in all). Needs producer_name and deduplication \
             enabled on the namespace. Absent: at-least-once.",
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use moonpool_core::TokioProviders;
    use serde_json::{Value, json};
    use streaming_core::{
        GraphId, NodeId, OpContext, OpError, OpFactory, OpKind, OpRef, StaticSecrets, SystemClock,
        TenantId,
    };

    fn context() -> OpContext<TokioProviders> {
        let tenant = TenantId::new("acme").unwrap();
        OpContext {
            providers: TokioProviders::new(),
            clock: Arc::new(SystemClock),
            secrets: Arc::new(StaticSecrets::new().with(&tenant, "pulsar-jwt", "eyJ.secret")),
            tenant,
            graph: GraphId::new("g").unwrap(),
            node: NodeId::new("n").unwrap(),
        }
    }

    fn source(config: &Value) -> Result<SourceConfig, OpError> {
        SourceConfig::from_json(&context(), config)
    }

    fn sink(config: &Value) -> Result<SinkConfig, OpError> {
        SinkConfig::from_json(&context(), config)
    }

    #[test]
    fn registers_both_operators() {
        let mut registry = Registry::<TokioProviders>::empty();
        register(&mut registry);
        let catalog = registry.catalog();
        let src = catalog.get(&OpRef::new(SOURCE_OP).unwrap()).unwrap();
        assert_eq!(src.kind, OpKind::Source);
        assert_eq!(src.output, TypeTag::of::<PulsarMessage>());
        let snk = catalog.get(&OpRef::new(SINK_OP).unwrap()).unwrap();
        assert_eq!(snk.kind, OpKind::Sink);
        assert_eq!(snk.input, TypeTag::of::<PulsarOutput>());

        // Factories validate the configuration without connecting.
        let Some((_, OpFactory::Source(factory))) = registry.get(&OpRef::new(SOURCE_OP).unwrap())
        else {
            panic!("not a source");
        };
        let ok = json!({"service_url": "pulsar://localhost:6650",
                        "topic": "persistent://public/default/t", "subscription": "s"});
        assert!(factory(&context(), &ok).is_ok());
        assert!(matches!(
            factory(
                &context(),
                &json!({"topic": "persistent://public/default/t"})
            ),
            Err(OpError::Config(_))
        ));
        let Some((_, OpFactory::Sink(factory))) = registry.get(&OpRef::new(SINK_OP).unwrap())
        else {
            panic!("not a sink");
        };
        assert!(
            factory(
                &context(),
                &json!({"service_url": "pulsar://localhost:6650",
                        "topic": "persistent://public/default/out"})
            )
            .is_ok()
        );
    }

    #[test]
    fn source_config_defaults_and_options() {
        let config = source(&json!({
            "service_url": "pulsar://broker:6650",
            "topic": "persistent://public/default/orders",
            "subscription": "graph-g",
        }))
        .unwrap();
        assert_eq!(config.event_time, EventTimeMode::Publish);
        assert_eq!(config.receiver_queue_size, 1000);
        assert_eq!(config.initial_position, InitialPosition::Earliest);
        assert_eq!(config.connection.token, None);
        assert_eq!(config.connection.url.addr(), "broker:6650");

        let config = source(&json!({
            "service_url": "pulsar+ssl://broker",
            "topic": "persistent://public/default/orders",
            "subscription": "graph-g",
            "token": {"secret": "pulsar-jwt"},
            "event_time": "event",
            "receiver_queue_size": 50,
            "initial_position": "latest",
        }))
        .unwrap();
        assert_eq!(config.connection.token.as_deref(), Some("eyJ.secret"));
        assert!(config.connection.url.tls);
        assert_eq!(config.connection.url.port, 6651);
        assert_eq!(config.event_time, EventTimeMode::Event);
        assert_eq!(config.receiver_queue_size, 50);
        assert_eq!(config.initial_position, InitialPosition::Latest);

        let literal = source(&json!({
            "service_url": "pulsar://b", "topic": "persistent://p/d/t",
            "subscription": "s", "token": "literal-jwt"}))
        .unwrap();
        assert_eq!(literal.connection.token.as_deref(), Some("literal-jwt"));
    }

    #[test]
    fn source_config_errors() {
        let base = json!({"service_url": "pulsar://b", "topic": "persistent://p/d/t",
                          "subscription": "s"});
        let with = |key: &str, value: Value| {
            let mut config = base.clone();
            config[key] = value;
            source(&config)
        };
        assert!(source(&base).is_ok());
        for (key, value) in [
            ("service_url", json!("http://b")),
            ("topic", json!("t")),
            ("subscription", json!(" ")),
            ("token", json!({"secret": "missing"})),
            ("token", json!(42)),
            ("event_time", json!("ingest")),
            ("receiver_queue_size", json!(0)),
            ("tls_trust_certs_file", json!("/ca.pem")), // not a TLS URL
            ("unknown_field", json!(1)),
        ] {
            assert!(
                matches!(with(key, value.clone()), Err(OpError::Config(_))),
                "{key} = {value} should be rejected"
            );
        }
    }

    #[test]
    fn sink_config() {
        let config =
            sink(&json!({"service_url": "pulsar://b", "topic": "persistent://p/d/t"})).unwrap();
        assert_eq!(config.producer_name, None);
        assert_eq!(config.dedup, None);
        assert_eq!(config.send_timeout_ms, 30_000);

        let config = sink(&json!({
            "service_url": "pulsar://b", "topic": "persistent://p/d/t",
            "producer_name": "writer", "send_timeout_ms": 0,
            "broker_dedup": {"position_bits": [24, 24, 7], "index_bits": 8},
        }))
        .unwrap();
        assert_eq!(config.dedup.unwrap().bits(), [24, 24, 7, 8]);
        assert_eq!(config.send_timeout_ms, 0);

        // Deduplication needs a stable producer name and a layout that fits.
        assert!(matches!(
            sink(
                &json!({"service_url": "pulsar://b", "topic": "persistent://p/d/t",
                         "broker_dedup": {"position_bits": [24, 24, 7]}})
            ),
            Err(OpError::Config(_))
        ));
        assert!(matches!(
            sink(
                &json!({"service_url": "pulsar://b", "topic": "persistent://p/d/t",
                         "producer_name": "w",
                         "broker_dedup": {"position_bits": [32, 24, 7], "index_bits": 8}})
            ),
            Err(OpError::Config(_))
        ));
        assert!(matches!(
            sink(
                &json!({"service_url": "pulsar://b", "topic": "persistent://p/d/t",
                         "producer_name": ""})
            ),
            Err(OpError::Config(_))
        ));
    }

    #[test]
    fn token_becomes_connect_auth() {
        let config = source(
            &json!({"service_url": "pulsar://b", "topic": "persistent://p/d/t",
                                    "subscription": "s", "token": "jwt"}),
        )
        .unwrap();
        let connection = client::connection_config(&config.connection);
        assert_eq!(connection.auth_method_name, "token");
        assert_eq!(connection.auth_data.as_deref(), Some(b"jwt".as_slice()));
        assert!(connection.supervisor.is_some());
        let anonymous = source(
            &json!({"service_url": "pulsar://b", "topic": "persistent://p/d/t",
                                       "subscription": "s"}),
        )
        .unwrap();
        assert_eq!(
            client::connection_config(&anonymous.connection).auth_data,
            None
        );
    }

    /// A configuration made of the described parameters: `pick` decides
    /// which, with their default or else the sample value.
    fn described(
        params: &[ParamSpec],
        samples: &Value,
        pick: impl Fn(&ParamSpec) -> bool,
    ) -> Value {
        let mut config = json!({});
        for p in params.iter().filter(|p| pick(p)) {
            let value = p
                .default
                .clone()
                .or_else(|| samples.get(&p.name).cloned())
                .unwrap_or_else(|| panic!("no sample value for `{}`", p.name));
            config[&p.name] = value;
        }
        config
    }

    /// The descriptions shown by the designer match what the parsers accept:
    /// every described parameter exists, the required ones are required and
    /// nothing else is, the defaults are the parser's and every choice parses.
    fn check_descriptions<T: PartialEq + std::fmt::Debug>(
        op: &str,
        samples: &Value,
        parse: impl Fn(&Value) -> Result<T, OpError>,
    ) {
        let mut registry = Registry::<TokioProviders>::empty();
        register(&mut registry);
        let params = registry
            .catalog()
            .get(&OpRef::new(op).unwrap())
            .unwrap()
            .params
            .clone();
        assert!(!params.is_empty(), "{op} describes no parameter");
        for p in &params {
            assert!(
                !p.description.is_empty(),
                "{op}: `{}` is not described",
                p.name
            );
        }
        let everything = described(&params, samples, |_| true);
        parse(&everything).unwrap_or_else(|e| panic!("{op}: {e}"));
        let required = described(&params, samples, |p| p.required);
        let minimal = parse(&required).unwrap_or_else(|e| panic!("{op}: {e}"));
        let defaults = described(&params, samples, |p| p.required || p.default.is_some());
        assert_eq!(
            parse(&defaults).unwrap(),
            minimal,
            "{op}: a default is not the parser's"
        );
        for p in params.iter().filter(|p| p.required) {
            let mut config = required.clone();
            config.as_object_mut().unwrap().remove(&p.name);
            assert!(
                parse(&config).is_err(),
                "{op}: `{}` is not required",
                p.name
            );
        }
        for p in &params {
            for choice in &p.choices {
                let mut config = required.clone();
                config[&p.name] = json!(choice);
                parse(&config).unwrap_or_else(|e| panic!("{op}: {}={choice}: {e}", p.name));
            }
        }
    }

    #[test]
    fn parameter_descriptions_match_the_parsers() {
        let tls = json!({"service_url": "pulsar+ssl://broker",
                         "topic": "persistent://public/default/t", "subscription": "s",
                         "token": {"secret": "pulsar-jwt"}, "tls_trust_certs_file": "/ca.pem",
                         "producer_name": "writer",
                         "broker_dedup": {"position_bits": [24, 39, 0], "index_bits": 0}});
        check_descriptions(SOURCE_OP, &tls, source);
        check_descriptions(SINK_OP, &tls, sink);
    }
}
