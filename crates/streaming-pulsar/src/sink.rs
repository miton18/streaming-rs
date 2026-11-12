//! `std/pulsar-sink@1`.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use futures::lock::Mutex;
use magnetar::{
    OutgoingMessage, java_string_hash,
    proto::CreateProducerRequest,
    runtime_moonpool::{Producer, SendFut},
};
use moonpool_core::Providers;
use streaming_core::{Emit, EmitId, SinkConnector, SinkError, data::hash_str, ops::downcast};

use crate::{
    client::{ClientCell, Conn},
    config::SinkConfig,
    position::partition_topic,
    sequence::{SequenceLayout, SequenceState},
};

/// Message property holding the framework's emit id of the element.
pub const EMIT_ID_PROPERTY: &str = "streaming-emit-id";

/// One message to publish: the input type of `std/pulsar-sink@1`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PulsarOutput {
    /// Message key. Routes the message on a partitioned topic (Java's default
    /// `JavaStringHash`), and is what compaction and `Key_Shared` work on.
    pub key: Option<String>,
    /// Payload bytes.
    pub payload: Vec<u8>,
    /// Message properties. The sink adds [`EMIT_ID_PROPERTY`].
    pub properties: Vec<(String, String)>,
}

fn err(context: &str, error: impl std::fmt::Display) -> SinkError {
    SinkError(format!("pulsar sink: {context}: {error}"))
}

/// The slice part of an emit id (`<slice>/<position>/<lineage>`).
pub(crate) fn slice_of(id: &EmitId) -> &str {
    id.as_str().split('/').next().unwrap_or_default()
}

/// Producer name used for the elements of one slice.
#[must_use]
pub fn producer_name_for(base: &str, slice: &str) -> String {
    format!("{base}-{slice}")
}

/// Partition an element goes to.
///
/// - keyed: Java's default `JavaStringHash` of the key, so that keyed messages
///   land where a Java producer would put them;
/// - keyless, at-least-once: a digest of the emit id (spreads one slice over
///   every partition);
/// - keyless, `broker_dedup`: a digest of the slice id, so that a slice writes
///   to a single partition and its sequence ids stay contiguous there.
///
/// In every case the choice is a function of the element alone: a re-emitted
/// element lands on the partition it went to the first time, which broker
/// deduplication (per partition) requires and round-robin would break.
#[must_use]
pub fn route(key: Option<&str>, id: &EmitId, partitions: usize, dedup: bool) -> usize {
    if partitions <= 1 {
        return 0;
    }
    let n = partitions as u64;
    match key {
        Some(key) if !key.is_empty() => java_string_hash(key) as usize % partitions,
        _ if dedup => (hash_str(slice_of(id)) % n) as usize,
        _ => (id.digest() % n) as usize,
    }
}

/// The producer of one destination partition.
struct PartitionProducer<P: Providers> {
    topic: String,
    open: Option<OpenProducer<P>>,
}

struct OpenProducer<P: Providers> {
    producer: Producer<P>,
    /// Sequence id the producer's state machine assigns to its next send.
    next_seq: u64,
    /// Highest sequence id the broker had persisted for this producer name
    /// when the producer attached (`-1` when none, or when deduplication is
    /// disabled on the namespace).
    persisted: i64,
}

struct Producers<P: Providers> {
    /// One per partition (one for a non-partitioned topic).
    partitions: Vec<PartitionProducer<P>>,
    /// Keeps the client alive as long as the producers.
    conn: Arc<Conn<P>>,
}

struct SinkState<P: Providers> {
    producers: Option<Producers<P>>,
    /// The slice this sink instance writes for, from the first emit id.
    slice: Option<String>,
    sequence: SequenceState,
}

/// What the last successful write did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteStats {
    /// Messages handed to the broker and acknowledged by it.
    pub sent: usize,
    /// Elements not sent because the broker already persisted their sequence
    /// id under this producer name (dedup mode).
    pub skipped: usize,
    /// Producers (re)opened to jump to a new sequence id (dedup mode).
    pub reopened: usize,
}

/// The connector: one producer per destination partition, opened lazily.
pub struct PulsarSink<P: Providers> {
    config: SinkConfig,
    client: ClientCell<P>,
    state: Mutex<SinkState<P>>,
    last: parking_lot::Mutex<WriteStats>,
}

impl<P: Providers> PulsarSink<P> {
    /// A sink writing to `config`, connecting lazily through `providers`.
    #[must_use]
    pub fn new(providers: P, config: SinkConfig) -> Self {
        Self {
            client: ClientCell::new(providers, config.connection.clone()),
            config,
            state: Mutex::new(SinkState {
                producers: None,
                slice: None,
                sequence: SequenceState::default(),
            }),
            last: parking_lot::Mutex::new(WriteStats::default()),
        }
    }

    /// Counters of the last successful write.
    #[must_use]
    pub fn last_write(&self) -> WriteStats {
        *self.last.lock()
    }

    async fn destination(&self) -> Result<Producers<P>, SinkError> {
        let conn = self.client.get().await.map_err(|e| err("connect", e))?;
        let count = conn
            .client()
            .partitioned_topic_metadata(&self.config.topic)
            .await
            .map_err(|e| err("partitioned topic metadata", e))?;
        let topics: Vec<String> = if count == 0 {
            vec![self.config.topic.clone()]
        } else {
            (0..count).map(|p| partition_topic(&self.config.topic, p)).collect()
        };
        Ok(Producers {
            partitions: topics
                .into_iter()
                .map(|topic| PartitionProducer { topic, open: None })
                .collect(),
            conn,
        })
    }

    async fn open_producer(
        &self,
        conn: &Conn<P>,
        topic: &str,
        name: Option<String>,
        initial_sequence_id: Option<u64>,
    ) -> Result<OpenProducer<P>, String> {
        let request = CreateProducerRequest {
            topic: topic.to_owned(),
            producer_name: name,
            initial_sequence_id,
            // One frame per message, so that the broker checks each message's
            // own sequence id; Magnetar also fails (rather than replays)
            // batched sends caught by a reconnect.
            enable_batching: false,
            send_timeout: (self.config.send_timeout_ms > 0).then(|| Duration::from_millis(self.config.send_timeout_ms)),
            ..CreateProducerRequest::default()
        };
        let producer = conn
            .client()
            .open_producer(request)
            .await
            .map_err(|e| format!("open producer on `{topic}`: {e}"))?;
        Ok(OpenProducer {
            // Overwritten by the broker's `CommandProducerSuccess.last_sequence_id`.
            persisted: producer.last_sequence_id_published(),
            producer,
            next_seq: initial_sequence_id.unwrap_or(0),
        })
    }

    async fn publish(
        &self,
        state: &mut SinkState<P>,
        name: Option<String>,
        prepared: Vec<(&EmitId, OutgoingMessage, Option<u64>)>,
    ) -> Result<WriteStats, String> {
        let producers = state
            .producers
            .as_mut()
            .unwrap_or_else(|| unreachable!("destination resolved before publishing"));
        let conn = producers.conn.clone();
        let count = producers.partitions.len();
        let mut pending: Vec<Vec<SendFut>> = (0..count).map(|_| Vec::new()).collect();
        let mut stats = WriteStats::default();
        for (id, message, seq) in prepared {
            let p = route(message.key.as_deref(), id, count, self.config.dedup.is_some());
            let partition = &mut producers.partitions[p];
            match seq {
                None if partition.open.is_none() => {
                    let open = self.open_producer(&conn, &partition.topic, name.clone(), None).await?;
                    partition.open = Some(open);
                }
                None => {}
                Some(seq) => {
                    // Broker-side deduplication: the message must leave with
                    // sequence id `seq`. Magnetar numbers sends consecutively
                    // from the producer's `initial_sequence_id` and has no
                    // per-message override, so a jump means reopening the
                    // producer at `seq`, once what it already sent is
                    // acknowledged.
                    let stored = |persisted: i64| i64::try_from(seq).is_ok_and(|s| s <= persisted);
                    if partition.open.as_ref().is_some_and(|o| stored(o.persisted)) {
                        // The broker already holds it under this producer name
                        // and would drop it as a duplicate.
                        stats.skipped += 1;
                        continue;
                    }
                    if partition.open.as_ref().is_none_or(|o| o.next_seq != seq) {
                        wait_all(&mut pending[p]).await?;
                        if let Some(old) = partition.open.take() {
                            old.producer
                                .close()
                                .await
                                .map_err(|e| format!("close producer on `{}`: {e}", partition.topic))?;
                        }
                        let open = self
                            .open_producer(&conn, &partition.topic, name.clone(), Some(seq))
                            .await?;
                        stats.reopened += 1;
                        let skip = stored(open.persisted);
                        partition.open = Some(open);
                        if skip {
                            stats.skipped += 1;
                            continue;
                        }
                    }
                }
            }
            let open = partition
                .open
                .as_mut()
                .unwrap_or_else(|| unreachable!("producer opened above"));
            pending[p].push(open.producer.send(message.into()));
            open.next_seq = open.next_seq.wrapping_add(1);
            stats.sent += 1;
        }
        // Wait for the broker receipt of every message of the batch.
        for partition_pending in &mut pending {
            wait_all(partition_pending).await?;
        }
        Ok(stats)
    }
}

async fn wait_all(pending: &mut Vec<SendFut>) -> Result<(), String> {
    let results = futures::future::join_all(pending.drain(..)).await;
    let total = results.len();
    let failed: Vec<String> = results
        .into_iter()
        .filter_map(|r| r.err().map(|e| e.to_string()))
        .collect();
    match failed.first() {
        None => Ok(()),
        Some(first) => Err(format!("{} of {total} messages failed, first: {first}", failed.len())),
    }
}

#[async_trait]
impl<P: Providers> SinkConnector for PulsarSink<P> {
    async fn write(&self, batch: &[Emit]) -> Result<(), SinkError> {
        if batch.is_empty() {
            return Ok(());
        }
        // One write at a time: producers and sequence state are per instance.
        let mut state = self.state.lock().await;

        let slice = slice_of(&batch[0].id).to_owned();
        if let Some(known) = &state.slice
            && *known != slice
        {
            return Err(err(
                "write",
                format!("one sink instance received elements of slices `{known}` and `{slice}`"),
            ));
        }
        if batch.iter().any(|emit| slice_of(&emit.id) != slice) {
            return Err(err("write", "a batch mixes elements of several slices"));
        }
        state.slice = Some(slice.clone());

        // Everything that can fail on the data alone, before touching the wire.
        let layout: Option<SequenceLayout> = self.config.dedup;
        let mut sequence = state.sequence;
        let mut prepared = Vec::with_capacity(batch.len());
        for emit in batch {
            let output = downcast::<PulsarOutput>(&emit.value).map_err(|e| err("write", e))?;
            let seq = match &layout {
                Some(layout) => Some(
                    layout
                        .next(&mut sequence, emit.origin.position)
                        .map_err(|e| err("write", e))?,
                ),
                None => None,
            };
            let mut message = OutgoingMessage::with_payload(Bytes::from(output.payload.clone()));
            message.key = output.key.clone();
            message.properties = output.properties.clone();
            message
                .properties
                .push((EMIT_ID_PROPERTY.to_owned(), emit.id.to_string()));
            if let Ok(ts) = u64::try_from(emit.ts)
                && ts > 0
            {
                message.event_time_ms = Some(ts);
            }
            prepared.push((&emit.id, message, seq));
        }

        if state.producers.is_none() {
            match self.destination().await {
                Ok(producers) => state.producers = Some(producers),
                Err(error) => {
                    self.client.reset_if_closed().await;
                    return Err(error);
                }
            }
        }
        let name = self
            .config
            .producer_name
            .as_deref()
            .map(|base| producer_name_for(base, &slice));
        match self.publish(&mut state, name, prepared).await {
            Ok(stats) => {
                state.sequence = sequence;
                *self.last.lock() = stats;
                Ok(())
            }
            Err(error) => {
                // The outcome of the failed sends is unknown; start over with
                // fresh producers (the runtime retries the same elements).
                if let Some(producers) = state.producers.take() {
                    for partition in producers.partitions {
                        if let Some(open) = partition.open {
                            let _ = open.producer.close().await;
                        }
                    }
                }
                self.client.reset_if_closed().await;
                Err(err("send", error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use streaming_core::{NodeId, Origin, Position, SliceId};

    use super::*;

    fn emit_id(partition: u32, position: Position) -> EmitId {
        let slice = SliceId::Source {
            node: NodeId::new("events").unwrap(),
            partition,
        };
        EmitId::new(&slice, &Origin::source(position))
    }

    #[test]
    fn slice_and_producer_name() {
        let id = emit_id(3, Position(1, 2, 0));
        assert_eq!(slice_of(&id), "src.events.3");
        assert_eq!(producer_name_for("orders", "src.events.3"), "orders-src.events.3");
    }

    #[test]
    fn routing_is_deterministic() {
        let a = emit_id(0, Position(1, 2, 0));
        let b = emit_id(0, Position(1, 3, 0));
        assert_eq!(route(None, &a, 1, false), 0);
        assert_eq!(route(Some("k"), &a, 1, true), 0);
        // Keyed: Java's String.hashCode() & i32::MAX, modulo the partitions,
        // whatever the element.
        let h = "user-42"
            .bytes()
            .fold(0_i32, |h, b| h.wrapping_mul(31).wrapping_add(i32::from(b)));
        let expected = ((h & i32::MAX) as usize) % 7;
        for dedup in [false, true] {
            assert_eq!(route(Some("user-42"), &a, 7, dedup), expected);
            assert_eq!(route(Some("user-42"), &b, 7, dedup), expected);
            // Keyless: stable per element.
            assert_eq!(route(None, &a, 7, dedup), route(None, &a.clone(), 7, dedup));
            assert_eq!(route(Some(""), &b, 7, dedup), route(None, &b, 7, dedup));
        }
        // At-least-once: one slice spreads over the partitions.
        let spread: std::collections::BTreeSet<usize> = (0..64)
            .map(|i| route(None, &emit_id(0, Position(1, i, 0)), 7, false))
            .collect();
        assert!(spread.len() > 3, "keyless elements should spread: {spread:?}");
        // Dedup: one slice sticks to one partition, slices spread.
        let sticky: std::collections::BTreeSet<usize> = (0..64)
            .map(|i| route(None, &emit_id(0, Position(1, i, 0)), 7, true))
            .collect();
        assert_eq!(sticky.len(), 1);
        let slices: std::collections::BTreeSet<usize> = (0..64)
            .map(|p| route(None, &emit_id(p, Position(1, 1, 0)), 7, true))
            .collect();
        assert!(slices.len() > 3, "slices should spread: {slices:?}");
    }

    #[test]
    fn routes_stay_within_the_partitions() {
        for dedup in [false, true] {
            for i in 0..64 {
                let id = emit_id(i, Position(1, u64::from(i) * 7, 0));
                for key in [None, Some("user-42")] {
                    let partition = route(key, &id, 7, dedup);
                    assert!(partition < 7, "{key:?} {dedup} -> {partition}");
                }
            }
        }
    }
}
