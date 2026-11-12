//! What the connectors say to the broker, checked on the fake one.

mod common;

use common::Broker;
use magnetar::proto::pb::{self, base_command::Type};
use moonpool_core::TokioProviders;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use streaming_core::{
    Emit, EmitId, GraphId, NodeId, OpContext, Origin, Position, SinkConnector, SliceId, SourceConnector, StaticSecrets,
    SystemClock, TenantId, datum,
};
use streaming_pulsar::{PulsarMessage, PulsarOutput, PulsarSink, PulsarSource, SinkConfig, SourceConfig};

fn context() -> OpContext<TokioProviders> {
    OpContext {
        providers: TokioProviders::new(),
        clock: Arc::new(SystemClock),
        secrets: Arc::new(StaticSecrets::new()),
        tenant: TenantId::new("acme").unwrap(),
        graph: GraphId::new("g").unwrap(),
        node: NodeId::new("n").unwrap(),
    }
}

fn source(broker: &Broker, config: serde_json::Value) -> PulsarSource<TokioProviders> {
    let mut config = config;
    config["serviceUrl"] = json!(broker.url);
    PulsarSource::new(
        TokioProviders::new(),
        SourceConfig::from_json(&context(), &config).unwrap(),
    )
}

fn sink(broker: &Broker, config: serde_json::Value) -> PulsarSink<TokioProviders> {
    let mut config = config;
    config["serviceUrl"] = json!(broker.url);
    PulsarSink::new(
        TokioProviders::new(),
        SinkConfig::from_json(&context(), &config).unwrap(),
    )
}

fn emit(i: u64, ts: i64) -> Emit {
    let slice = SliceId::Source {
        node: NodeId::new("events").unwrap(),
        partition: 0,
    };
    let origin = Origin::source(Position(0, i, 0));
    Emit {
        id: EmitId::new(&slice, &origin),
        origin,
        ts,
        key: None,
        value: datum(PulsarOutput {
            key: None,
            payload: format!("e{i}").into_bytes(),
            properties: Vec::new(),
        }),
    }
}

fn metadata(event_time: Option<u64>) -> pb::MessageMetadata {
    pb::MessageMetadata {
        producer_name: "test".into(),
        sequence_id: 0,
        publish_time: 1_700_000_000_000,
        event_time,
        ..pb::MessageMetadata::default()
    }
}

const TOPIC: &str = "persistent://public/default/t";

/// The subscription: Failover, durable, under the configured name and
/// queue size; one connection for every call of a connector, closed with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_source_subscribes_as_configured_on_one_connection() {
    let broker = Broker::start().await;
    let source = source(
        &broker,
        json!({"topic": TOPIC, "subscription": "subs", "receiverQueueSize": 7}),
    );
    assert_eq!(source.partitions().await.unwrap(), 1);
    assert_eq!(source.partitions().await.unwrap(), 1);
    let reader = source.open(0, None).await.unwrap();
    let subscribe = broker.seen(Type::Subscribe);
    let subscribe = subscribe[0].subscribe.as_ref().unwrap();
    assert_eq!(subscribe.subscription, "subs");
    assert_eq!(subscribe.sub_type, pb::command_subscribe::SubType::Failover as i32);
    assert_eq!(subscribe.durable, Some(true));
    // The first flow may follow the subscription by a moment.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while broker.seen(Type::Flow).is_empty() {
        assert!(std::time::Instant::now() < deadline, "no flow was sent");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let flows: u32 = broker
        .seen(Type::Flow)
        .iter()
        .map(|flow| flow.flow.as_ref().unwrap().message_permits)
        .max()
        .unwrap();
    assert_eq!(flows, 7, "the receiver queue size is the flow window");
    let connect = broker.seen(Type::Connect);
    assert_eq!(connect.len(), 1, "one connection for the whole connector");
    let version = &connect[0].connect.as_ref().unwrap().client_version;
    assert!(version.starts_with("streaming-pulsar/"), "{version}");
    drop(reader);
    drop(source);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while broker.live_connections() > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the connection outlives the connector"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Pulsar says "no event time" with 0; acks move the cursor up to the
/// greatest delivered message at or before the position.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_time_zero_is_none_and_acks_follow_positions() {
    let broker = Broker::start().await;
    broker.publish(TOPIC, metadata(Some(0)), b"a");
    for i in 1..4 {
        broker.publish(TOPIC, metadata(Some(1_000 + i)), b"b");
    }
    let source = source(
        &broker,
        json!({"topic": TOPIC, "subscription": "s", "eventTime": "event"}),
    );
    let mut reader = source.open(0, None).await.unwrap();
    let mut records = Vec::new();
    while records.len() < 4 {
        records.extend(reader.poll(4, Duration::from_millis(200)).await.unwrap());
    }
    let first = records[0].value.downcast_ref::<PulsarMessage>().unwrap();
    assert_eq!(first.event_time_ms, None);
    assert_eq!(records[0].event_time, 1_700_000_000_000, "the publish time stands in");
    reader.ack(records[1].position).await.unwrap();
    reader.ack(records[3].position).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while broker.acked(TOPIC, "s") < 4 {
        assert!(
            std::time::Instant::now() < deadline,
            "acked {}",
            broker.acked(TOPIC, "s")
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Without deduplication the sink keeps one unbatched producer per
/// partition, with the configured send timeout, and stamps event times
/// only when there is one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_sink_keeps_one_unbatched_producer() {
    let broker = Broker::start().await;
    let sink = sink(&broker, json!({"topic": TOPIC, "sendTimeoutMs": 1500}));
    sink.write(&[emit(1, 0), emit(2, 42)]).await.unwrap();
    sink.write(&[emit(3, 43)]).await.unwrap();
    let producers = broker.seen(Type::Producer);
    assert_eq!(producers.len(), 1, "one producer for every write");
    let stored = broker.messages(TOPIC);
    assert_eq!(stored.len(), 3);
    assert!(
        stored.iter().all(|m| m.metadata.num_messages_in_batch.is_none()),
        "not batched"
    );
    assert_eq!(stored[0].metadata.event_time, None);
    assert_eq!(stored[1].metadata.event_time, Some(42));
}

/// `sendTimeoutMs` bounds how long a write waits for the broker; 0 waits for
/// as long as it takes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_waits_for_the_broker_as_long_as_configured() {
    let broker = Broker::start().await;
    broker.withhold_receipts();
    let bounded = sink(&broker, json!({"topic": TOPIC, "sendTimeoutMs": 300}));
    let outcome = tokio::time::timeout(Duration::from_secs(10), bounded.write(&[emit(1, 0)])).await;
    assert!(
        matches!(outcome, Ok(Err(_))),
        "a write the broker never confirms fails after its timeout"
    );
    let patient = sink(&broker, json!({"topic": TOPIC, "sendTimeoutMs": 0}));
    let outcome = tokio::time::timeout(Duration::from_secs(2), patient.write(&[emit(2, 0)])).await;
    assert!(outcome.is_err(), "without a timeout, a write keeps waiting");
}
