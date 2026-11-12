//! `std/pulsar-source@1`.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use magnetar::{
    MessageId,
    proto::{
        IncomingMessage, SubscribeRequest,
        pb::command_subscribe::{InitialPosition as PbInitialPosition, SubType},
    },
    runtime_moonpool::Consumer,
};
use moonpool_core::Providers;
use streaming_core::{PartitionReader, Position, SourceConnector, SourceError, SourceRecord, Timestamp, datum};

use crate::{
    client::{ClientCell, Conn},
    config::{EventTimeMode, InitialPosition, SourceConfig},
    position::{end_position_of, partition_topic, position_of},
};

/// One message read from a Pulsar topic partition: the output type of
/// `std/pulsar-source@1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PulsarMessage {
    /// Message key (`partition_key`), when the producer set one.
    pub key: Option<String>,
    /// Payload bytes (decompressed).
    pub payload: Bytes,
    /// Message properties, in wire order.
    pub properties: Vec<(String, String)>,
    /// Publish time stamped by the producer client, milliseconds since the epoch.
    pub publish_time_ms: i64,
    /// Event time set by the producer, when it set one.
    pub event_time_ms: Option<i64>,
    /// The topic the message was read from: `<topic>-partition-<n>` for a
    /// partitioned topic, the topic itself otherwise.
    pub topic: String,
    /// Partition index (0 for a non-partitioned topic).
    pub partition: u32,
}

impl PulsarMessage {
    /// Value of the first property named `name`.
    #[must_use]
    pub fn property(&self, name: &str) -> Option<&str> {
        self.properties.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }
}

fn err(context: &str, error: impl std::fmt::Display) -> SourceError {
    SourceError(format!("pulsar source: {context}: {error}"))
}

/// The connector: one shared client, one consumer per opened partition.
pub struct PulsarSource<P: Providers> {
    config: SourceConfig,
    client: ClientCell<P>,
}

impl<P: Providers> PulsarSource<P> {
    /// A source reading `config`, connecting lazily through `providers`.
    #[must_use]
    pub fn new(providers: P, config: SourceConfig) -> Self {
        Self {
            client: ClientCell::new(providers, config.connection.clone()),
            config,
        }
    }

    async fn partition_count(&self, conn: &Conn<P>) -> Result<u32, SourceError> {
        conn.client()
            .partitioned_topic_metadata(&self.config.topic)
            .await
            .map_err(|e| err("partitioned topic metadata", e))
    }
}

#[async_trait]
impl<P: Providers> SourceConnector for PulsarSource<P> {
    async fn partitions(&self) -> Result<u32, SourceError> {
        let conn = self.client.get().await.map_err(|e| err("connect", e))?;
        let result = self.partition_count(&conn).await;
        if result.is_err() {
            self.client.reset_if_closed().await;
        }
        // A non-partitioned topic reports 0 partitions and is read as one.
        Ok(result?.max(1))
    }

    async fn open(&self, partition: u32, _after: Option<Position>) -> Result<Box<dyn PartitionReader>, SourceError> {
        // `after` is ignored on purpose: the durable subscription's cursor only
        // moves on `ack`, which the runtime calls after its own commit, so the
        // broker redelivers from at or before `after` and the runtime drops what
        // it already processed. A seek would move the cursor of the whole
        // subscription (both Failover consumers during a hand-over) and
        // disconnect them; it is neither cheap nor safe here.
        let conn = self.client.get().await.map_err(|e| err("connect", e))?;
        let opened = async {
            let count = self.partition_count(&conn).await?;
            let topic = match count {
                0 if partition == 0 => self.config.topic.clone(),
                0 => {
                    return Err(SourceError(format!(
                        "pulsar source: `{}` is not partitioned, there is no partition {partition}",
                        self.config.topic
                    )));
                }
                n if partition < n => partition_topic(&self.config.topic, partition),
                n => {
                    return Err(SourceError(format!(
                        "pulsar source: `{}` has {n} partitions, there is no partition {partition}",
                        self.config.topic
                    )));
                }
            };
            let request = SubscribeRequest {
                topic: topic.clone(),
                subscription: self.config.subscription.clone(),
                sub_type: SubType::Failover,
                durable: true,
                receiver_queue_size: self.config.receiver_queue_size,
                initial_position: match self.config.initial_position {
                    InitialPosition::Earliest => PbInitialPosition::Earliest,
                    InitialPosition::Latest => PbInitialPosition::Latest,
                },
                ..SubscribeRequest::default()
            };
            let consumer = conn
                .client()
                .subscribe(request)
                .await
                .map_err(|e| err(&format!("subscribe to `{topic}`"), e))?;
            Ok((topic, consumer))
        }
        .await;
        let (topic, consumer) = match opened {
            Ok(opened) => opened,
            Err(error) => {
                self.client.reset_if_closed().await;
                return Err(error);
            }
        };
        Ok(Box::new(PulsarReader {
            consumer,
            _conn: conn,
            topic,
            partition,
            event_time: self.config.event_time,
            delivered: BTreeMap::new(),
        }))
    }
}

/// A Failover consumer on one partition.
struct PulsarReader<P: Providers> {
    consumer: Consumer<P>,
    /// Keeps the shared client alive as long as the consumer.
    _conn: Arc<Conn<P>>,
    topic: String,
    partition: u32,
    event_time: EventTimeMode,
    /// Message ids delivered and not acknowledged yet, by position: `ack`
    /// turns a position back into the id Magnetar's cumulative ack needs.
    delivered: BTreeMap<Position, MessageId>,
}

/// Turn a Magnetar message into a framework record. Batch members carry their
/// key, properties and event time in their single-message metadata.
pub(crate) fn to_record(
    message: IncomingMessage,
    topic: &str,
    partition: u32,
    mode: EventTimeMode,
) -> (MessageId, SourceRecord) {
    let id = message.message_id;
    let meta = &message.metadata;
    let (key, properties, event_time) = match &message.single_metadata {
        Some(single) => (single.partition_key.clone(), &single.properties, single.event_time),
        None => (meta.partition_key.clone(), &meta.properties, meta.event_time),
    };
    let publish_time_ms = i64::try_from(meta.publish_time).unwrap_or(i64::MAX);
    // Pulsar uses 0 for "no event time".
    let event_time_ms = event_time
        .filter(|t| *t > 0)
        .map(|t| i64::try_from(t).unwrap_or(i64::MAX));
    let ts: Timestamp = match mode {
        EventTimeMode::Publish => publish_time_ms,
        EventTimeMode::Event => event_time_ms.unwrap_or(publish_time_ms),
    };
    let value = PulsarMessage {
        key,
        payload: message.payload,
        properties: properties.iter().map(|kv| (kv.key.clone(), kv.value.clone())).collect(),
        publish_time_ms,
        event_time_ms,
        topic: topic.to_owned(),
        partition,
    };
    (
        id,
        SourceRecord {
            position: position_of(&id),
            event_time: ts,
            value: datum(value),
        },
    )
}

#[async_trait]
impl<P: Providers> PartitionReader for PulsarReader<P> {
    async fn poll(&mut self, max: usize, max_wait: Duration) -> Result<Vec<SourceRecord>, SourceError> {
        // Waits up to `max_wait` (on the providers' clock) for the first
        // message, then drains what is already buffered, up to `max`.
        let messages = self
            .consumer
            .receive_batch(max.max(1), max_wait)
            .await
            .map_err(|e| err(&format!("receive from `{}`", self.topic), e))?;
        let mut records = Vec::with_capacity(messages.len());
        for message in messages {
            let (id, record) = to_record(message, &self.topic, self.partition, self.event_time);
            self.delivered.insert(record.position, id);
            records.push(record);
        }
        Ok(records)
    }

    async fn ack(&mut self, upto: Position) -> Result<(), SourceError> {
        // The greatest delivered message at or before `upto`. When `upto` was
        // delivered by a previous reader, an earlier id (or nothing) is acked:
        // under-acknowledging is safe, the broker redelivers a little more.
        let Some((&position, &id)) = self.delivered.range(..=upto).next_back() else {
            return Ok(());
        };
        self.consumer
            .ack_cumulative(id)
            .await
            .map_err(|e| err(&format!("cumulative ack of {position} on `{}`", self.topic), e))?;
        self.delivered.retain(|delivered, _| *delivered > position);
        Ok(())
    }

    async fn end_position(&mut self) -> Result<Option<Position>, SourceError> {
        let last = self
            .consumer
            .last_message_id()
            .await
            .map_err(|e| err(&format!("get last message id of `{}`", self.topic), e))?;
        Ok(end_position_of(&last))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_property_of_a_name_wins() {
        let message = PulsarMessage {
            key: None,
            payload: Bytes::new(),
            properties: vec![
                ("a".into(), "1".into()),
                ("b".into(), "2".into()),
                ("a".into(), "3".into()),
            ],
            publish_time_ms: 0,
            event_time_ms: None,
            topic: String::new(),
            partition: 0,
        };
        assert_eq!(message.property("a"), Some("1"));
        assert_eq!(message.property("b"), Some("2"));
        assert_eq!(message.property("c"), None);
    }
}
