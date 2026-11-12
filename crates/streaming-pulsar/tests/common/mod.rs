//! A fake Pulsar broker, enough of the binary protocol for the connectors:
//! connect, lookup, partition metadata, producers with optional broker-side
//! deduplication, Failover subscriptions with flow control, cumulative acks,
//! last message id. One process, in memory, on a loopback port.

#![allow(dead_code)]

use bytes::{Bytes, BytesMut};
use magnetar::proto::{
    frame::{decode_one, encode_command, encode_payload, peek_full_frame_len},
    pb::{self, base_command::Type},
};
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};

/// A message as the broker stored it.
#[derive(Debug, Clone)]
pub struct Stored {
    pub metadata: pb::MessageMetadata,
    pub body: Bytes,
}

#[derive(Default)]
struct Topic {
    messages: Vec<Stored>,
    /// Highest sequence id persisted, by producer name (deduplication).
    last_sequence: HashMap<String, i64>,
}

#[derive(Default)]
struct Subscription {
    /// Entries before this one are acknowledged.
    acked: u64,
}

struct Consumer {
    topic: String,
    subscription: String,
    permits: u32,
    /// Next entry to dispatch.
    next: u64,
    /// Order of arrival: the first consumer of a subscription is the
    /// active one (Failover).
    order: u64,
    /// Whether it was the active one when last looked at.
    active: bool,
    out: mpsc::UnboundedSender<BytesMut>,
}

struct Producer {
    topic: String,
    name: String,
}

#[derive(Default)]
struct State {
    /// Partition count of the partitioned topics (base name).
    partitioned: HashMap<String, u32>,
    topics: HashMap<String, Topic>,
    subscriptions: HashMap<(String, String), Subscription>,
    /// By (connection, consumer id).
    consumers: BTreeMap<(u64, u64), Consumer>,
    producers: HashMap<(u64, u64), Producer>,
    dedup: bool,
    /// Store messages without ever confirming them.
    silent: bool,
    next_connection: u64,
    next_order: u64,
    /// Connections open now.
    live: usize,
    /// Commands received, by type name, for the tests to look at.
    seen: Vec<pb::BaseCommand>,
}

/// Handle on a running fake broker.
#[derive(Clone)]
pub struct Broker {
    pub url: String,
    state: Arc<Mutex<State>>,
}

const LEDGER: u64 = 7;

fn command(kind: Type) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: kind as i32,
        ..pb::BaseCommand::default()
    }
}

fn encode(cmd: &pb::BaseCommand) -> BytesMut {
    let mut out = BytesMut::new();
    encode_command(&mut out, cmd).expect("encodable");
    out
}

fn message_id(entry: u64) -> pb::MessageIdData {
    pb::MessageIdData {
        ledger_id: LEDGER,
        entry_id: entry,
        ..pb::MessageIdData::default()
    }
}

impl Broker {
    /// Start a broker on a free loopback port.
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("pulsar://localhost:{}", listener.local_addr().unwrap().port());
        let broker = Self {
            url,
            state: Arc::default(),
        };
        let accepting = broker.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let broker = accepting.clone();
                tokio::spawn(async move { broker.serve(stream).await });
            }
        });
        broker
    }

    /// Declare `topic` partitioned in `partitions`.
    pub fn partitioned(&self, topic: &str, partitions: u32) {
        self.state.lock().partitioned.insert(topic.to_owned(), partitions);
    }

    /// Turn broker-side deduplication on.
    pub fn deduplicate(&self) {
        self.state.lock().dedup = true;
    }

    /// Stop confirming what producers send, as a broker that stalls would.
    pub fn withhold_receipts(&self) {
        self.state.lock().silent = true;
    }

    /// Messages stored on `topic`.
    pub fn messages(&self, topic: &str) -> Vec<Stored> {
        self.state
            .lock()
            .topics
            .get(topic)
            .map(|topic| topic.messages.clone())
            .unwrap_or_default()
    }

    /// Entries of `topic` acknowledged by `subscription`.
    pub fn acked(&self, topic: &str, subscription: &str) -> u64 {
        self.state
            .lock()
            .subscriptions
            .get(&(topic.to_owned(), subscription.to_owned()))
            .map_or(0, |subscription| subscription.acked)
    }

    /// Client connections open now.
    pub fn live_connections(&self) -> usize {
        self.state.lock().live
    }

    /// Commands of type `kind` received so far.
    pub fn seen(&self, kind: Type) -> Vec<pb::BaseCommand> {
        self.state
            .lock()
            .seen
            .iter()
            .filter(|cmd| cmd.r#type == kind as i32)
            .cloned()
            .collect()
    }

    /// Append a message to `topic` as a producer would.
    pub fn publish(&self, topic: &str, metadata: pb::MessageMetadata, body: &[u8]) {
        let mut state = self.state.lock();
        state.topics.entry(topic.to_owned()).or_default().messages.push(Stored {
            metadata,
            body: Bytes::copy_from_slice(body),
        });
        dispatch(&mut state);
    }

    async fn serve(self, stream: tokio::net::TcpStream) {
        let (mut read, mut write) = stream.into_split();
        let (tx, mut rx) = mpsc::unbounded_channel::<BytesMut>();
        let connection = {
            let mut state = self.state.lock();
            state.next_connection += 1;
            state.live += 1;
            state.next_connection
        };
        let writer = tokio::spawn(async move {
            while let Some(bytes) = rx.recv().await {
                if write.write_all(&bytes).await.is_err() {
                    break;
                }
            }
        });
        let mut inbound = BytesMut::new();
        let mut chunk = vec![0_u8; 64 * 1024];
        'read: loop {
            let n = match read.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            inbound.extend_from_slice(&chunk[..n]);
            while let Ok(Some(len)) = peek_full_frame_len(&inbound) {
                let mut bytes = inbound.split_to(len).freeze();
                let Ok(frame) = decode_one(&mut bytes) else {
                    break 'read;
                };
                self.handle(connection, frame, &tx);
            }
        }
        let mut state = self.state.lock();
        state.consumers.retain(|(conn, _), _| *conn != connection);
        state.producers.retain(|(conn, _), _| *conn != connection);
        hand_over(&mut state);
        state.live -= 1;
        drop(state);
        writer.abort();
    }

    fn handle(&self, connection: u64, frame: magnetar::proto::frame::Frame, tx: &mpsc::UnboundedSender<BytesMut>) {
        let cmd = frame.command;
        let mut state = self.state.lock();
        state.seen.push(cmd.clone());
        let reply = |cmd: pb::BaseCommand| {
            let _ = tx.send(encode(&cmd));
        };
        let success = |request_id: u64| {
            let mut out = command(Type::Success);
            out.success = Some(pb::CommandSuccess {
                request_id,
                ..pb::CommandSuccess::default()
            });
            out
        };
        match Type::try_from(cmd.r#type) {
            Ok(Type::Connect) => {
                let mut out = command(Type::Connected);
                out.connected = Some(pb::CommandConnected {
                    server_version: "fake-broker".into(),
                    protocol_version: Some(21),
                    max_message_size: Some(5 * 1024 * 1024),
                    feature_flags: None,
                });
                reply(out);
            }
            Ok(Type::Ping) => reply(command(Type::Pong)),
            Ok(Type::PartitionedMetadata) => {
                let request = cmd.partition_metadata.unwrap_or_default();
                let mut out = command(Type::PartitionedMetadataResponse);
                out.partition_metadata_response = Some(pb::CommandPartitionedTopicMetadataResponse {
                    partitions: Some(*state.partitioned.get(&request.topic).unwrap_or(&0)),
                    request_id: request.request_id,
                    response: Some(0),
                    ..Default::default()
                });
                reply(out);
            }
            Ok(Type::Lookup) => {
                let request = cmd.lookup_topic.unwrap_or_default();
                let mut out = command(Type::LookupResponse);
                out.lookup_topic_response = Some(pb::CommandLookupTopicResponse {
                    broker_service_url: Some(self.url.clone()),
                    response: Some(1), // Connect
                    request_id: request.request_id,
                    authoritative: Some(true),
                    proxy_through_service_url: Some(false),
                    ..Default::default()
                });
                reply(out);
            }
            Ok(Type::Producer) => {
                let request = cmd.producer.unwrap_or_default();
                let name = request
                    .producer_name
                    .clone()
                    .unwrap_or_else(|| format!("fake-{connection}-{}", request.producer_id));
                let topic = state.topics.entry(request.topic.clone()).or_default();
                let last = *topic.last_sequence.get(&name).unwrap_or(&-1);
                state.producers.insert(
                    (connection, request.producer_id),
                    Producer {
                        topic: request.topic.clone(),
                        name: name.clone(),
                    },
                );
                let mut out = command(Type::ProducerSuccess);
                out.producer_success = Some(pb::CommandProducerSuccess {
                    request_id: request.request_id,
                    producer_name: name,
                    last_sequence_id: Some(last),
                    producer_ready: Some(true),
                    ..Default::default()
                });
                reply(out);
            }
            Ok(Type::Send) => {
                let request = cmd.send.unwrap_or_default();
                let Some(payload) = frame.payload else { return };
                let Some(producer) = state.producers.get(&(connection, request.producer_id)) else {
                    return;
                };
                let (topic_name, name) = (producer.topic.clone(), producer.name.clone());
                let dedup = state.dedup || topic_name.contains("/streaming-dedup/");
                let topic = state.topics.entry(topic_name).or_default();
                let sequence = i64::try_from(request.sequence_id).unwrap_or(i64::MAX);
                let last = *topic.last_sequence.get(&name).unwrap_or(&-1);
                let entry = if dedup && sequence <= last {
                    // A duplicate: acknowledged, not stored.
                    None
                } else {
                    topic.messages.push(Stored {
                        metadata: payload.metadata,
                        body: payload.body,
                    });
                    topic.last_sequence.insert(name, sequence.max(last));
                    Some(topic.messages.len() as u64 - 1)
                };
                if state.silent {
                    return;
                }
                let mut out = command(Type::SendReceipt);
                out.send_receipt = Some(pb::CommandSendReceipt {
                    producer_id: request.producer_id,
                    sequence_id: request.sequence_id,
                    message_id: Some(message_id(entry.unwrap_or(u64::MAX))),
                    highest_sequence_id: request.highest_sequence_id,
                });
                reply(out);
                dispatch(&mut state);
            }
            Ok(Type::CloseProducer) => {
                let request = cmd.close_producer.unwrap_or_default();
                state.producers.remove(&(connection, request.producer_id));
                reply(success(request.request_id));
            }
            Ok(Type::Subscribe) => {
                let request = cmd.subscribe.unwrap_or_default();
                let topic_len = state.topics.entry(request.topic.clone()).or_default().messages.len() as u64;
                let key = (request.topic.clone(), request.subscription.clone());
                let latest = request.initial_position == Some(0);
                let acked = state
                    .subscriptions
                    .entry(key)
                    .or_insert_with(|| Subscription {
                        acked: if latest { topic_len } else { 0 },
                    })
                    .acked;
                state.next_order += 1;
                let order = state.next_order;
                state.consumers.insert(
                    (connection, request.consumer_id),
                    Consumer {
                        topic: request.topic,
                        subscription: request.subscription,
                        permits: 0,
                        next: acked,
                        order,
                        active: false,
                        out: tx.clone(),
                    },
                );
                reply(success(request.request_id));
            }
            Ok(Type::Flow) => {
                let request = cmd.flow.unwrap_or_default();
                if let Some(consumer) = state.consumers.get_mut(&(connection, request.consumer_id)) {
                    consumer.permits = consumer.permits.saturating_add(request.message_permits);
                }
                dispatch(&mut state);
            }
            Ok(Type::Ack) => {
                let request = cmd.ack.unwrap_or_default();
                if let Some(consumer) = state.consumers.get(&(connection, request.consumer_id)) {
                    let key = (consumer.topic.clone(), consumer.subscription.clone());
                    let messages = state.topics.get(&consumer.topic).map(|topic| &topic.messages);
                    // An entry counts as acknowledged once its last batch
                    // member is: a mid-batch ack leaves the entry to redeliver.
                    let upto = request
                        .message_id
                        .iter()
                        .map(|id| {
                            let size = messages
                                .and_then(|messages| messages.get(id.entry_id as usize))
                                .and_then(|stored| stored.metadata.num_messages_in_batch)
                                .unwrap_or(1);
                            match id.batch_index {
                                Some(index) if index >= 0 && index + 1 < size => id.entry_id,
                                _ => id.entry_id + 1,
                            }
                        })
                        .max()
                        .unwrap_or(0);
                    let subscription = state.subscriptions.entry(key).or_default();
                    subscription.acked = subscription.acked.max(upto);
                }
                if let Some(request_id) = request.request_id {
                    let mut out = command(Type::AckResponse);
                    out.ack_response = Some(pb::CommandAckResponse {
                        consumer_id: request.consumer_id,
                        request_id: Some(request_id),
                        ..Default::default()
                    });
                    reply(out);
                }
            }
            Ok(Type::RedeliverUnacknowledgedMessages) => {
                let request = cmd.redeliver_unacknowledged_messages.unwrap_or_default();
                let acked = state.consumers.get(&(connection, request.consumer_id)).map(|consumer| {
                    state
                        .subscriptions
                        .get(&(consumer.topic.clone(), consumer.subscription.clone()))
                        .map_or(0, |subscription| subscription.acked)
                });
                if let (Some(acked), Some(consumer)) =
                    (acked, state.consumers.get_mut(&(connection, request.consumer_id)))
                {
                    consumer.next = acked;
                }
                dispatch(&mut state);
            }
            Ok(Type::GetLastMessageId) => {
                let request = cmd.get_last_message_id.unwrap_or_default();
                let consumer = state.consumers.get(&(connection, request.consumer_id));
                let len = consumer
                    .and_then(|consumer| state.topics.get(&consumer.topic))
                    .map_or(0, |topic| topic.messages.len() as u64);
                let last = if len == 0 {
                    pb::MessageIdData {
                        ledger_id: u64::MAX,
                        entry_id: u64::MAX,
                        ..Default::default()
                    }
                } else {
                    message_id(len - 1)
                };
                let mut out = command(Type::GetLastMessageIdResponse);
                out.get_last_message_id_response = Some(pb::CommandGetLastMessageIdResponse {
                    last_message_id: last,
                    request_id: request.request_id,
                    consumer_mark_delete_position: None,
                });
                reply(out);
            }
            Ok(Type::CloseConsumer) => {
                let request = cmd.close_consumer.unwrap_or_default();
                state.consumers.remove(&(connection, request.consumer_id));
                hand_over(&mut state);
                reply(success(request.request_id));
            }
            _ => {}
        }
    }
}

/// When the active consumer of a subscription goes, the next one resumes
/// from what is acknowledged, as a broker redelivers on a hand-over.
fn hand_over(state: &mut State) {
    let State {
        consumers,
        subscriptions,
        ..
    } = state;
    let mut active: HashMap<(String, String), u64> = HashMap::new();
    for consumer in consumers.values() {
        let key = (consumer.topic.clone(), consumer.subscription.clone());
        let order = active.entry(key).or_insert(consumer.order);
        *order = (*order).min(consumer.order);
    }
    for consumer in consumers.values_mut() {
        let key = (consumer.topic.clone(), consumer.subscription.clone());
        if active[&key] == consumer.order && !consumer.active {
            consumer.active = true;
            consumer.next = subscriptions.get(&key).map_or(0, |subscription| subscription.acked);
        }
    }
    dispatch(state);
}

/// Hand the active consumer of every subscription what it has permits for.
fn dispatch(state: &mut State) {
    let State { topics, consumers, .. } = state;
    let mut active: HashMap<(String, String), u64> = HashMap::new();
    for consumer in consumers.values() {
        let key = (consumer.topic.clone(), consumer.subscription.clone());
        let order = active.entry(key).or_insert(consumer.order);
        *order = (*order).min(consumer.order);
    }
    for ((_, consumer_id), consumer) in consumers.iter_mut() {
        if active[&(consumer.topic.clone(), consumer.subscription.clone())] != consumer.order {
            continue;
        }
        consumer.active = true;
        let Some(topic) = topics.get(&consumer.topic) else {
            continue;
        };
        while consumer.permits > 0 && (consumer.next as usize) < topic.messages.len() {
            let stored = &topic.messages[consumer.next as usize];
            let mut cmd = command(Type::Message);
            cmd.message = Some(pb::CommandMessage {
                consumer_id: *consumer_id,
                message_id: message_id(consumer.next),
                redelivery_count: Some(0),
                ..Default::default()
            });
            let mut out = BytesMut::new();
            encode_payload(&mut out, &cmd, &stored.metadata, &stored.body).expect("encodable");
            let _ = consumer.out.send(out);
            consumer.permits -= 1;
            consumer.next += 1;
        }
    }
}

/// One fake broker for the whole test binary, on a runtime of its own so
/// that it outlives the runtime of each test.
pub fn shared() -> &'static Broker {
    static BROKER: std::sync::OnceLock<Broker> = std::sync::OnceLock::new();
    BROKER.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                tx.send(Broker::start().await).unwrap();
                futures::future::pending::<()>().await;
            });
        });
        rx.recv().unwrap()
    })
}
