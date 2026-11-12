//! End-to-end tests against a real Pulsar broker.
//!
//! Skipped (they pass trivially, with a notice on stderr) unless
//! `PULSAR_E2E_URL` is set, e.g.
//!
//! ```sh
//! bin/pulsar standalone   # Pulsar 4.x
//! bin/pulsar-admin namespaces create public/streaming-it
//! bin/pulsar-admin namespaces create public/streaming-dedup
//! bin/pulsar-admin namespaces set-deduplication public/streaming-dedup --enable
//! PULSAR_E2E_URL=pulsar://localhost:6650 PULSAR_E2E_ADMIN=localhost:8080 \
//!   cargo test -p streaming-pulsar --test e2e_broker
//! ```
//!
//! `broker_dedup_through_an_aliased_url` expects `PULSAR_E2E_URL` to be
//! `pulsar://localhost:<port>` (it reaches the same broker as `127.0.0.1`).

use magnetar::{
    ConnectionConfig, OutgoingMessage,
    proto::{
        CreateProducerRequest, SubscribeRequest,
        pb::command_subscribe::{InitialPosition, SubType},
    },
    runtime_moonpool::{Client, MoonpoolEngine},
};
use moonpool_core::TokioProviders;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    net::TcpStream,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use streaming_core::{
    Emit, EmitId, GraphId, NodeId, OpContext, OpFactory, OpRef, Origin, PartitionReader, Position, Registry,
    SinkConnector, SliceId, SourceConnector, SourceRecord, StaticSecrets, SystemClock, TenantId, datum,
};
use streaming_pulsar::{
    EMIT_ID_PROPERTY, PulsarMessage, PulsarOutput, PulsarSink, SinkConfig, WriteStats, partition_topic,
};

fn broker() -> Option<String> {
    let url = std::env::var("PULSAR_E2E_URL").ok();
    if url.is_none() {
        eprintln!("PULSAR_E2E_URL is not set: skipping the end-to-end test");
    }
    url
}

fn admin_addr() -> String {
    std::env::var("PULSAR_E2E_ADMIN").unwrap_or_else(|_| "localhost:8080".to_owned())
}

/// Minimal HTTP/1.1 client for the admin REST API (no extra dependency).
fn admin(method: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(admin_addr()).expect("admin API reachable");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status, response)
}

fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{prefix}-{nanos}")
}

/// Create a partitioned topic, returns its full name.
fn partitioned_topic(namespace: &str, name: &str, partitions: u32) -> String {
    let (status, response) = admin(
        "PUT",
        &format!("/admin/v2/persistent/{namespace}/{name}/partitions"),
        &partitions.to_string(),
    );
    assert!(status == 204 || status == 200, "create topic: {response}");
    format!("persistent://{namespace}/{name}")
}

fn context() -> OpContext<TokioProviders> {
    let tenant = TenantId::new("acme").unwrap();
    OpContext {
        providers: TokioProviders::new(),
        clock: Arc::new(SystemClock),
        secrets: Arc::new(StaticSecrets::new()),
        tenant,
        graph: GraphId::new("g").unwrap(),
        node: NodeId::new("n").unwrap(),
    }
}

fn registry() -> Registry<TokioProviders> {
    let mut registry = Registry::empty();
    streaming_pulsar::register(&mut registry);
    registry
}

/// Build a source through the registry, as the runtime does.
fn make_source(config: Value) -> Arc<dyn SourceConnector> {
    let registry = registry();
    let Some((_, OpFactory::Source(factory))) = registry.get(&OpRef::new("std/pulsar-source@1").unwrap()) else {
        panic!("source not registered");
    };
    factory(&context(), &config).expect("valid source config")
}

fn make_sink(config: Value) -> Arc<dyn SinkConnector> {
    let registry = registry();
    let Some((_, OpFactory::Sink(factory))) = registry.get(&OpRef::new("std/pulsar-sink@1").unwrap()) else {
        panic!("sink not registered");
    };
    factory(&context(), &config).expect("valid sink config")
}

async fn raw_client(url: &str) -> Client<TokioProviders> {
    let addr = url.trim_start_matches("pulsar://").to_owned();
    Client::connect_plain_supervised(
        &MoonpoolEngine::new(TokioProviders::new()),
        &addr,
        ConnectionConfig::default(),
        None,
        None,
    )
    .await
    .expect("connect")
}

/// Publish `messages` to one topic (partition), batched or not, waiting for
/// every receipt.
async fn produce(client: &Client<TokioProviders>, topic: &str, batching: bool, messages: Vec<OutgoingMessage>) {
    let producer = client
        .open_producer(CreateProducerRequest {
            topic: topic.to_owned(),
            enable_batching: batching,
            max_messages_in_batch: 4,
            batching_max_publish_delay: Some(Duration::from_millis(20)),
            ..CreateProducerRequest::default()
        })
        .await
        .expect("open producer");
    let sends: Vec<_> = messages.into_iter().map(|m| producer.send(m.into())).collect();
    for result in futures::future::join_all(sends).await {
        result.expect("send");
    }
    producer.close().await.expect("close producer");
}

fn message(i: usize) -> OutgoingMessage {
    OutgoingMessage::with_payload(format!("m{i}").into_bytes())
        .key(format!("k{}", i % 3))
        .property("i", i.to_string())
        .event_time_ms(1_000_000 + i as u64)
}

/// Poll until `count` records arrived or `within` elapsed.
async fn drain(reader: &mut Box<dyn PartitionReader>, count: usize, within: Duration) -> Vec<SourceRecord> {
    let deadline = Instant::now() + within;
    let mut records = Vec::new();
    while records.len() < count && Instant::now() < deadline {
        records.extend(
            reader
                .poll(count - records.len(), Duration::from_millis(200))
                .await
                .expect("poll"),
        );
    }
    records
}

fn msg(record: &SourceRecord) -> &PulsarMessage {
    record.value.downcast_ref::<PulsarMessage>().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_reads_partitions_acks_and_redelivers() {
    let Some(url) = broker() else { return };
    let topic = partitioned_topic("public/streaming-it", &unique("src"), 3);
    let client = raw_client(&url).await;
    // Partition 1: 3 plain messages, then 8 batched (two batches of 4), then 2 plain.
    let p1 = partition_topic(&topic, 1);
    produce(&client, &p1, false, (0..3).map(message).collect()).await;
    produce(&client, &p1, true, (3..11).map(message).collect()).await;
    produce(&client, &p1, false, (11..13).map(message).collect()).await;
    // Partition 0: a single message; partition 2 stays empty.
    produce(&client, &partition_topic(&topic, 0), false, vec![message(99)]).await;

    let config = json!({
        "serviceUrl": url, "topic": topic, "subscription": "it",
        "eventTime": "event", "receiverQueueSize": 5,
    });
    let source = make_source(config.clone());
    assert_eq!(source.partitions().await.unwrap(), 3);
    assert!(source.open(3, None).await.is_err(), "partition 3 does not exist");

    let mut reader = source.open(1, None).await.expect("open partition 1");
    let records = drain(&mut reader, 13, Duration::from_secs(20)).await;
    assert_eq!(records.len(), 13, "every message of partition 1");
    let positions: Vec<Position> = records.iter().map(|r| r.position).collect();
    assert!(
        positions.windows(2).all(|w| w[0] < w[1]),
        "positions strictly increase in delivery order: {positions:?}"
    );
    // Plain messages have batch component 0, batch members 1..=n.
    assert!(records[..3].iter().all(|r| r.position.2 == 0));
    let batched: Vec<u64> = records[3..11].iter().map(|r| r.position.2).collect();
    assert!(batched.iter().all(|b| *b >= 1), "batch members: {batched:?}");
    assert!(
        records[3..11].windows(2).any(|w| w[0].position.1 == w[1].position.1),
        "some members share an entry: {positions:?}"
    );
    assert!(records[11..].iter().all(|r| r.position.2 == 0));
    for (i, record) in records.iter().enumerate() {
        let m = msg(record);
        assert_eq!(m.payload.as_ref(), format!("m{i}").as_bytes());
        assert_eq!(m.key.as_deref(), Some(format!("k{}", i % 3).as_str()));
        assert_eq!(m.property("i"), Some(i.to_string().as_str()));
        assert_eq!(m.event_time_ms, Some(1_000_000 + i as i64));
        assert_eq!(record.event_time, 1_000_000 + i as i64, "event_time = event");
        assert!(m.publish_time_ms > 1_600_000_000_000);
        assert_eq!(m.topic, p1);
        assert_eq!(m.partition, 1);
    }
    // end_position: the last message of the partition.
    assert_eq!(reader.end_position().await.unwrap(), Some(positions[12]));
    let mut empty = source.open(2, None).await.unwrap();
    assert_eq!(empty.end_position().await.unwrap(), None, "empty partition");
    assert!(empty.poll(10, Duration::from_millis(300)).await.unwrap().is_empty());

    // Acknowledge up to the third plain message (an entry boundary), then
    // restart the reader: redelivery starts right after it.
    reader.ack(positions[2]).await.expect("ack");
    drop(reader);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut reader = source.open(1, None).await.expect("reopen");
    let again = drain(&mut reader, 10, Duration::from_secs(20)).await;
    let again_positions: Vec<Position> = again.iter().map(|r| r.position).collect();
    assert_eq!(again_positions, positions[3..].to_vec(), "redelivery after the ack");

    // Acknowledge in the middle of a batch: nothing unacknowledged is
    // skipped, nothing at or before an earlier ack comes back.
    let mid = positions[4];
    reader.ack(mid).await.expect("ack mid-batch");
    drop(reader);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut reader = source.open(1, None).await.expect("reopen");
    let third = drain(&mut reader, 13, Duration::from_secs(5)).await;
    let third_positions: Vec<Position> = third.iter().map(|r| r.position).collect();
    assert!(
        third_positions
            .first()
            .is_some_and(|p| *p > positions[2] && *p <= positions[5]),
        "restart after a mid-batch ack: {third_positions:?}"
    );
    let unacked: BTreeSet<Position> = positions[5..].iter().copied().collect();
    let seen: BTreeSet<Position> = third_positions.iter().copied().collect();
    assert!(unacked.is_subset(&seen), "no unacked record skipped");
    eprintln!("mid-batch ack at {mid}: redelivery restarted at {}", third_positions[0]);

    // Event time "publish".
    let mut publish = make_source(json!({
        "serviceUrl": url, "topic": topic, "subscription": "it-publish",
    }))
    .open(0, None)
    .await
    .unwrap();
    let only = drain(&mut publish, 1, Duration::from_secs(20)).await;
    assert_eq!(only.len(), 1);
    assert_eq!(only[0].event_time, msg(&only[0]).publish_time_ms);
    assert_eq!(msg(&only[0]).partition, 0);
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_delivers_to_one_consumer_and_hands_over() {
    let Some(url) = broker() else { return };
    let topic = partitioned_topic("public/streaming-it", &unique("failover"), 2);
    let client = raw_client(&url).await;
    let config = json!({"serviceUrl": url, "topic": topic, "subscription": "fo"});
    // Two connectors, as on two workers during a hand-over.
    let (a, b) = (make_source(config.clone()), make_source(config));
    let mut ra = a.open(0, None).await.unwrap();
    let mut rb = b.open(0, None).await.unwrap();
    produce(
        &client,
        &partition_topic(&topic, 0),
        false,
        (0..6).map(message).collect(),
    )
    .await;
    let mut got_a = Vec::new();
    let mut got_b = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while got_a.len() + got_b.len() < 6 && Instant::now() < deadline {
        got_a.extend(ra.poll(10, Duration::from_millis(100)).await.unwrap());
        got_b.extend(rb.poll(10, Duration::from_millis(100)).await.unwrap());
    }
    // A little longer, to catch a double delivery.
    got_a.extend(ra.poll(10, Duration::from_millis(500)).await.unwrap());
    got_b.extend(rb.poll(10, Duration::from_millis(500)).await.unwrap());
    eprintln!("failover: a got {}, b got {}", got_a.len(), got_b.len());
    assert_eq!(got_a.len() + got_b.len(), 6, "each message delivered once");
    assert!(got_a.is_empty() || got_b.is_empty(), "only one active consumer");

    // The active one acks two and goes away: the other takes over from there.
    let (mut active, mut standby, got) = if got_a.is_empty() {
        (rb, ra, got_b)
    } else {
        (ra, rb, got_a)
    };
    active.ack(got[1].position).await.unwrap();
    drop(active);
    let taken = drain(&mut standby, 4, Duration::from_secs(20)).await;
    let expected: Vec<Position> = got[2..].iter().map(|r| r.position).collect();
    assert_eq!(
        taken.iter().map(|r| r.position).collect::<Vec<_>>(),
        expected,
        "the standby resumes after the last ack"
    );
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_partitioned_topic_is_one_partition() {
    let Some(url) = broker() else { return };
    let topic = format!("persistent://public/streaming-it/{}", unique("plain"));
    let client = raw_client(&url).await;
    produce(&client, &topic, false, (0..2).map(message).collect()).await;
    let source = make_source(json!({"serviceUrl": url, "topic": topic, "subscription": "s"}));
    assert_eq!(source.partitions().await.unwrap(), 1);
    assert!(source.open(1, None).await.is_err());
    let mut reader = source.open(0, None).await.unwrap();
    let records = drain(&mut reader, 2, Duration::from_secs(20)).await;
    assert_eq!(records.len(), 2);
    assert_eq!(msg(&records[0]).topic, topic);
    assert_eq!(reader.end_position().await.unwrap(), Some(records[1].position));
    client.close().await;
}

fn emit(partition: u32, position: Position, key: Option<&str>, payload: &str) -> Emit {
    let slice = SliceId::Source {
        node: NodeId::new("events").unwrap(),
        partition,
    };
    let origin = Origin::source(position).derive(42, 0);
    Emit {
        id: EmitId::new(&slice, &origin),
        origin,
        ts: 1_700_000_000_000 + position.1 as i64,
        key: None,
        value: datum(PulsarOutput {
            key: key.map(str::to_owned),
            payload: payload.as_bytes().to_vec(),
            properties: vec![("origin".to_owned(), position.to_string())],
        }),
    }
}

/// Read everything currently in every partition of `topic`, by payload.
async fn read_all(url: &str, topic: &str, expected: usize) -> Vec<PulsarMessage> {
    let source = make_source(json!({"serviceUrl": url, "topic": topic,
                               "subscription": unique("check")}));
    let partitions = source.partitions().await.unwrap();
    let mut out = Vec::new();
    for p in 0..partitions {
        let mut reader = source.open(p, None).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let batch = reader.poll(100, Duration::from_millis(300)).await.unwrap();
            let done = batch.is_empty() && (out.len() >= expected || Instant::now() > deadline);
            out.extend(batch.iter().map(|r| msg(r).clone()));
            if done {
                break;
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_round_trip_with_emit_ids() {
    let Some(url) = broker() else { return };
    let topic = partitioned_topic("public/streaming-it", &unique("sink"), 2);
    let sink = make_sink(json!({"serviceUrl": url, "topic": topic, "producerName": "rt"}));
    let batch: Vec<Emit> = (0..10)
        .map(|i| {
            let key = if i % 2 == 0 { Some("same-key") } else { None };
            emit(0, Position(3, i, 0), key, &format!("v{i}"))
        })
        .collect();
    sink.write(&batch).await.expect("write");
    sink.write(&[]).await.expect("empty write");

    let read = read_all(&url, &topic, 10).await;
    assert_eq!(read.len(), 10, "every element published once");
    let by_id: BTreeMap<String, &PulsarMessage> = read
        .iter()
        .map(|m| (m.property(EMIT_ID_PROPERTY).unwrap().to_owned(), m))
        .collect();
    for e in &batch {
        let m = by_id.get(e.id.as_str()).expect("message carries the emit id");
        let out = e.value.downcast_ref::<PulsarOutput>().unwrap();
        assert_eq!(m.payload.as_ref(), out.payload.as_slice());
        assert_eq!(m.key, out.key);
        assert_eq!(m.property("origin"), Some(e.origin.position.to_string().as_str()));
        assert_eq!(m.event_time_ms, Some(e.ts));
    }
    // Every keyed element landed on the same partition.
    let keyed: BTreeSet<u32> = read
        .iter()
        .filter(|m| m.key.as_deref() == Some("same-key"))
        .map(|m| m.partition)
        .collect();
    assert_eq!(keyed.len(), 1);
    // Elements of another slice cannot go through the same sink instance.
    assert!(sink.write(&[emit(1, Position(3, 0, 0), None, "x")]).await.is_err());
}

fn concrete_sink(config: Value) -> PulsarSink<TokioProviders> {
    PulsarSink::new(
        TokioProviders::new(),
        SinkConfig::from_json(&context(), &config).expect("valid sink config"),
    )
}

/// Elements of positions `range` (position 3 yields two elements, as a
/// `flat_map` would), all from slice `src.events.0`.
fn elements(range: std::ops::RangeInclusive<u64>) -> Vec<Emit> {
    range
        .flat_map(|i| {
            let n = if i == 3 { 2 } else { 1 };
            (0..n).map(move |k| {
                let mut e = emit(0, Position(7, i, 0), None, &format!("p{i}.{k}"));
                e.origin = e.origin.derive(7, k);
                e.id = EmitId::new(
                    &SliceId::Source {
                        node: NodeId::new("events").unwrap(),
                        partition: 0,
                    },
                    &e.origin,
                );
                e
            })
        })
        .collect()
}

/// Write positions 1..=5, "crash", then a fresh sink instance re-emits from
/// position 3 and goes on to 8. Returns what the topic holds and the stats of
/// the replayed write.
async fn replay_scenario(url: &str, topic: &str, dedup: bool) -> (Vec<PulsarMessage>, WriteStats) {
    let mut config = json!({"serviceUrl": url, "topic": topic, "producerName": "dd"});
    if dedup {
        config["brokerDedup"] = json!({"positionBits": [20, 30, 5], "indexBits": 8});
    }
    let first = concrete_sink(config.clone());
    first.write(&elements(1..=2)).await.expect("first write");
    first.write(&elements(3..=5)).await.expect("second write");
    drop(first);
    let second = concrete_sink(config);
    second.write(&elements(3..=8)).await.expect("replayed write");
    let stats = second.last_write();
    (read_all(url, topic, 9).await, stats)
}

fn payloads(read: &[PulsarMessage]) -> Vec<String> {
    read.iter()
        .map(|m| String::from_utf8(m.payload.to_vec()).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_dedup_drops_reemissions() {
    let Some(url) = broker() else { return };
    // Control: without deduplication the replay duplicates 4 messages.
    let plain = partitioned_topic("public/streaming-it", &unique("nodedup"), 2);
    let (read, _) = replay_scenario(&url, &plain, false).await;
    assert_eq!(read.len(), 13, "at-least-once: the replay is duplicated");

    // With derived sequence ids on a dedup-enabled namespace: each element once.
    let topic = partitioned_topic("public/streaming-dedup", &unique("dedup"), 2);
    let (read, stats) = replay_scenario(&url, &topic, true).await;
    let got = payloads(&read);
    let distinct: BTreeSet<&String> = got.iter().collect();
    assert_eq!(distinct.len(), 9, "{got:?}");
    assert_eq!(got.len(), 9, "no duplicate: {got:?}");
    assert!(distinct.contains(&"p3.0".to_owned()) && distinct.contains(&"p3.1".to_owned()));
    eprintln!("replayed write: {stats:?}");
    // The four already stored were skipped client-side (the broker reported
    // its high-water mark when the producer attached).
    assert_eq!(stats.skipped, 4, "{stats:?}");
    assert_eq!(stats.sent, 3, "{stats:?}");
    // Keyless elements of one slice stick to one partition in dedup mode.
    let partitions: BTreeSet<u32> = read.iter().map(|m| m.partition).collect();
    assert_eq!(partitions.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_dedup_contiguous_positions_and_broker_side_drop() {
    let Some(url) = broker() else { return };
    let topic = format!("persistent://public/streaming-dedup/{}", unique("contig"));
    // (1, i, 0) with this layout gives consecutive sequence ids.
    let config = json!({"serviceUrl": url, "topic": topic, "producerName": "twice",
                        "brokerDedup": {"positionBits": [8, 47, 0], "indexBits": 0}});
    let one = |i: u64| emit(0, Position(1, i, 0), None, &format!("e{i}"));
    let first = concrete_sink(config.clone());
    first.write(&(1..=4).map(one).collect::<Vec<_>>()).await.unwrap();
    let s1 = first.last_write();
    // Contiguous sequence ids: a single producer open for the whole batch.
    assert_eq!(
        s1,
        WriteStats {
            sent: 4,
            skipped: 0,
            reopened: 1
        }
    );
    // A gap (position 6 filtered out upstream) costs one reopen.
    first.write(&[one(5), one(7)]).await.unwrap();
    assert_eq!(
        first.last_write(),
        WriteStats {
            sent: 2,
            skipped: 0,
            reopened: 1
        }
    );
    drop(first);
    // A restarted instance replaying 1..=8: the stored ones are skipped.
    let second = concrete_sink(config);
    second
        .write(&(1..=8).filter(|i| *i != 6).map(one).collect::<Vec<_>>())
        .await
        .unwrap();
    let s2 = second.last_write();
    eprintln!("first {s1:?}, second {s2:?}");
    assert_eq!(s2.skipped, 6);
    assert_eq!(s2.sent, 1);
    drop(second);

    // The broker itself drops a stale sequence id under that producer name:
    // a raw Magnetar producer re-sending with the id of e2.
    let client = raw_client(&url).await;
    let producer = client
        .open_producer(CreateProducerRequest {
            topic: topic.clone(),
            producer_name: Some("twice-src.events.0".into()),
            initial_sequence_id: Some((1 << 47) | 2),
            ..CreateProducerRequest::default()
        })
        .await
        .expect("open raw producer");
    eprintln!(
        "broker high-water mark for the name: {}",
        producer.last_sequence_id_published()
    );
    assert_eq!(producer.last_sequence_id_published(), (1 << 47) | 8);
    let receipt = producer
        .send(OutgoingMessage::with_payload(b"stale".as_slice()).into())
        .await;
    eprintln!("receipt of the stale send: {receipt:?}");
    producer.close().await.unwrap();
    client.close().await;
    let read = read_all(&url, &topic, 7).await;
    assert_eq!(
        payloads(&read),
        ["e1", "e2", "e3", "e4", "e5", "e7", "e8"],
        "each element once, the stale resend dropped by the broker"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_dedup_through_an_aliased_url() {
    // The broker named by an address other than the one it advertises, so
    // Magnetar puts the producers on a pool connection: deduplication only
    // relies on public producer options and works the same.
    let Some(url) = broker() else { return };
    let Some(alias) = url
        .strip_prefix("pulsar://localhost:")
        .map(|port| format!("pulsar://127.0.0.1:{port}"))
    else {
        eprintln!("PULSAR_E2E_URL is not pulsar://localhost:<port>: skipping");
        return;
    };
    let topic = partitioned_topic("public/streaming-dedup", &unique("alias"), 3);
    let (read, stats) = replay_scenario(&alias, &topic, true).await;
    assert_eq!(payloads(&read).len(), 9, "{:?} / {stats:?}", payloads(&read));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_failover_subscribe_sanity() {
    // Guards the test harness itself: the raw client sees what the source sees.
    let Some(url) = broker() else { return };
    let topic = format!("persistent://public/streaming-it/{}", unique("raw"));
    let client = raw_client(&url).await;
    produce(&client, &topic, false, vec![message(0)]).await;
    let consumer = client
        .subscribe(SubscribeRequest {
            topic: topic.clone(),
            subscription: "raw".into(),
            sub_type: SubType::Failover,
            initial_position: InitialPosition::Earliest,
            ..SubscribeRequest::default()
        })
        .await
        .unwrap();
    let got = consumer.receive_with_timeout(Duration::from_secs(10)).await.unwrap();
    assert!(got.is_some());
    client.close().await;
}
