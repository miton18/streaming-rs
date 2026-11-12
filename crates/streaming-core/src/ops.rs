//! Operator contracts.
//!
//! Two layers:
//!
//! - *typed* traits and closures, which is what operator authors write
//!   ([`Aggregator`], plain `Fn(&In) -> Option<Out>` functions, ...);
//! - *erased* traits ([`DynMap`], [`DynKey`], [`DynAggregator`], ...), which is
//!   what the runtime drives. The [`crate::Registry`] builds the erased form
//!   from the typed one.
//!
//! Sources and sinks are connectors: they talk to the outside world and are
//! implemented directly against [`SourceConnector`] and [`SinkConnector`].

use std::{any::Any, time::Duration};

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    data::{Datum, Emit, Key, Position},
    time::{Timestamp, Window},
};

/// Failure while building or running an operator.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OpError {
    /// The node configuration is invalid.
    #[error("invalid operator configuration: {0}")]
    Config(String),
    /// A value of an unexpected type reached the operator.
    #[error("type mismatch: expected `{expected}`")]
    Type {
        /// The type the operator was registered with.
        expected: &'static str,
    },
    /// The operator failed on a value.
    #[error("operator failed: {0}")]
    Failed(String),
}

/// A source failure. The runtime retries with backoff.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("source error: {0}")]
pub struct SourceError(pub String);

/// A sink failure. The outcome of the write is unknown; the runtime retries
/// the same elements with the same [`crate::EmitId`]s.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("sink error: {0}")]
pub struct SinkError(pub String);

/// One record read from a source partition.
#[derive(Clone)]
pub struct SourceRecord {
    /// Position inside the partition. Strictly increasing in delivery order,
    /// except for redeliveries.
    pub position: Position,
    /// Event time of the record.
    pub event_time: Timestamp,
    /// The payload.
    pub value: Datum,
}

/// A partitioned, replayable source.
#[async_trait]
pub trait SourceConnector: Send + Sync + 'static {
    /// Number of partitions. Each one becomes a slice.
    async fn partitions(&self) -> Result<u32, SourceError>;

    /// Open a reader on one partition.
    ///
    /// `after` is the last position the framework has durably processed, when
    /// there is one. A connector able to seek should start right after it; a
    /// connector that cannot may start earlier, the runtime drops what it has
    /// already processed.
    async fn open(&self, partition: u32, after: Option<Position>) -> Result<Box<dyn PartitionReader>, SourceError>;
}

/// A reader on one partition.
///
/// Contract: records come in position order; a record may be delivered more
/// than once (after a reconnection the reader may rewind), but an unacknowledged
/// record is never skipped.
#[async_trait]
pub trait PartitionReader: Send + 'static {
    /// Wait up to `max_wait` for records and return at most `max` of them.
    /// An empty vector means nothing arrived in time.
    async fn poll(&mut self, max: usize, max_wait: Duration) -> Result<Vec<SourceRecord>, SourceError>;

    /// Everything up to `upto` included is durably processed and may be
    /// released by the source.
    async fn ack(&mut self, upto: Position) -> Result<(), SourceError>;

    /// Position of the last record currently stored in the partition, or
    /// `None` when it is empty. Used to tell an idle partition from a lagging
    /// reader.
    async fn end_position(&mut self) -> Result<Option<Position>, SourceError>;
}

/// A destination.
///
/// Contract: `write` must be idempotent per [`crate::EmitId`]. After a failure
/// the runtime calls it again with the same ids and the same values; the sink
/// must end up holding each element once.
#[async_trait]
pub trait SinkConnector: Send + Sync + 'static {
    /// Durably write a batch.
    async fn write(&self, batch: &[Emit]) -> Result<(), SinkError>;
}

/// Erased stateless transformation (`map`, `filter` and `flat_map` alike).
pub trait DynMap: Send + Sync + 'static {
    /// Push the outputs produced from `input` into `out`.
    fn apply(&self, input: &Datum, out: &mut Vec<Datum>) -> Result<(), OpError>;
}

/// Erased key extraction.
pub trait DynKey: Send + Sync + 'static {
    /// Compute the grouping key of `input`.
    fn key(&self, input: &Datum) -> Result<Key, OpError>;
}

/// Assigns records to event-time windows.
pub trait WindowAssigner: Send + Sync + 'static {
    /// Push the windows containing `ts` into `out`.
    fn assign(&self, ts: Timestamp, out: &mut Vec<Window>);

    /// How long after its end a window still accepts records.
    fn allowed_lateness(&self) -> Duration {
        Duration::ZERO
    }
}

/// A windowed aggregation, as written by operator authors.
///
/// `merge` is required because the state of one key may be spread over several
/// partitions; accumulators are merged when the window closes.
pub trait Aggregator: Send + Sync + 'static {
    /// Type of the aggregated elements.
    type In: Any + Send + Sync;
    /// Accumulator, persisted in the KV.
    type Acc: Serialize + DeserializeOwned + Send + Sync + 'static;
    /// Result emitted when a window closes.
    type Out: Any + Send + Sync;

    /// A fresh accumulator.
    fn init(&self) -> Self::Acc;
    /// Fold one element in.
    fn add(&self, acc: &mut Self::Acc, item: &Self::In);
    /// Fold another accumulator in.
    fn merge(&self, into: &mut Self::Acc, other: &Self::Acc);
    /// Turn the final accumulator into the result.
    fn finish(&self, acc: &Self::Acc) -> Self::Out;
}

/// An erased accumulator.
pub type DynAcc = Box<dyn Any + Send + Sync>;

/// Erased windowed aggregation.
pub trait DynAggregator: Send + Sync + 'static {
    /// A fresh accumulator.
    fn init(&self) -> DynAcc;
    /// Fold one element in.
    fn add(&self, acc: &mut DynAcc, item: &Datum) -> Result<(), OpError>;
    /// Fold another accumulator in.
    fn merge(&self, into: &mut DynAcc, other: &DynAcc) -> Result<(), OpError>;
    /// Build the `Windowed<Out>` datum emitted for one key of a closed window.
    fn finish(&self, acc: &DynAcc, window: Window, key: &Key) -> Result<Datum, OpError>;
    /// Serialize an accumulator for the KV.
    fn encode(&self, acc: &DynAcc) -> Result<Vec<u8>, OpError>;
    /// Deserialize an accumulator read from the KV.
    fn decode(&self, bytes: &[u8]) -> Result<DynAcc, OpError>;
}

pub(crate) struct TypedAggregator<A>(pub(crate) A);

impl<A: Aggregator> DynAggregator for TypedAggregator<A> {
    fn init(&self) -> DynAcc {
        Box::new(self.0.init())
    }

    fn add(&self, acc: &mut DynAcc, item: &Datum) -> Result<(), OpError> {
        let acc = downcast_acc_mut::<A::Acc>(acc)?;
        let item = downcast::<A::In>(item)?;
        self.0.add(acc, item);
        Ok(())
    }

    fn merge(&self, into: &mut DynAcc, other: &DynAcc) -> Result<(), OpError> {
        let other = downcast_acc::<A::Acc>(other)?;
        let into = downcast_acc_mut::<A::Acc>(into)?;
        self.0.merge(into, other);
        Ok(())
    }

    fn finish(&self, acc: &DynAcc, window: Window, key: &Key) -> Result<Datum, OpError> {
        let acc = downcast_acc::<A::Acc>(acc)?;
        Ok(crate::data::datum(crate::data::Windowed {
            window,
            key: key.clone(),
            value: self.0.finish(acc),
        }))
    }

    fn encode(&self, acc: &DynAcc) -> Result<Vec<u8>, OpError> {
        let acc = downcast_acc::<A::Acc>(acc)?;
        postcard::to_allocvec(acc).map_err(|e| OpError::Failed(e.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<DynAcc, OpError> {
        postcard::from_bytes::<A::Acc>(bytes)
            .map(|acc| Box::new(acc) as DynAcc)
            .map_err(|e| OpError::Failed(format!("cannot decode accumulator: {e}")))
    }
}

/// Downcast a datum to the type an operator was registered with.
pub fn downcast<T: Any>(value: &Datum) -> Result<&T, OpError> {
    value.downcast_ref::<T>().ok_or(OpError::Type {
        expected: std::any::type_name::<T>(),
    })
}

fn downcast_acc<T: Any>(acc: &DynAcc) -> Result<&T, OpError> {
    acc.downcast_ref::<T>().ok_or(OpError::Type {
        expected: std::any::type_name::<T>(),
    })
}

fn downcast_acc_mut<T: Any>(acc: &mut DynAcc) -> Result<&mut T, OpError> {
    acc.downcast_mut::<T>().ok_or(OpError::Type {
        expected: std::any::type_name::<T>(),
    })
}
