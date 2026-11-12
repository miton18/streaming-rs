//! What an operator author writes: [`Source`], [`Sink`] and [`Transformer`],
//! and the pipes they are handed.
//!
//! An [`Operator`] names a thing and carries its configuration. What the thing
//! does is one of three traits:
//!
//! - a [`Source`] is started on a partition and pushes what it reads into a
//!   [`SourcePipe`];
//! - a [`Sink`] is started with a [`SinkPipe`] and writes the batches that
//!   come out of it;
//! - everything else is a [`Transformer`]: it takes a [`Pipe`] and returns a
//!   [`Pipe`].
//!
//! The pipes of sources and sinks are channels: the code of the author loops
//! on them. The pipe of a transformer is lazy, the way an iterator is:
//! `transform` is called once and says what happens to each element, it does
//! not loop. A loop would keep state in its variables (a buffer, a counter)
//! that the engine could not commit with the position of the source: the
//! state of a window or of an aggregate is declared to the pipe instead
//! ([`Pipe::window`], [`Pipe::aggregate`]), and the engine commits it, merges
//! it across partitions and emits it.
//!
//! ```
//! use streaming_core::{Keyed, Operator, Pipe, Transformer, Windowed, Windows};
//!
//! #[derive(Debug, Clone)]
//! pub struct Order {
//!     pub customer: String,
//!     pub amount: u64,
//! }
//!
//! /// Groups orders by customer.
//! #[derive(Debug, Clone, Operator)]
//! #[operator("acme/by-customer@1")]
//! pub struct ByCustomer;
//!
//! impl Transformer<Order> for ByCustomer {
//!     type Out = Keyed<Order>;
//!
//!     fn transform(&self, input: Pipe<Order>) -> Pipe<Keyed<Order>> { input.key_by(|order| order.customer.clone()) }
//! }
//!
//! /// The amount ordered by each customer in each window.
//! #[derive(Debug, Clone, Operator)]
//! #[operator("acme/total@1")]
//! pub struct Total;
//!
//! impl Transformer<Windows<Order>> for Total {
//!     type Out = Windowed<u64>;
//!
//!     fn transform(&self, input: Pipe<Windows<Order>>) -> Pipe<Windowed<u64>> {
//!         input.aggregate(
//!             || 0_u64,                               // the state of one key in one window
//!             |total, order| *total += order.amount,  // an element comes
//!             |total, other| *total += other,         // two partitions each held a part of the window
//!         )
//!     }
//! }
//! ```
//!
//! The engine does not drive these traits directly: it drives
//! [`SourceConnector`], [`SinkConnector`] and the erased operators of
//! [`crate::ops`], which is also where the simulation injects its faults. The
//! adapters of this module stand in between ([`crate::Registry::source`],
//! [`crate::Registry::sink`], [`crate::Registry::transformer`]). They spawn
//! nothing: the code of a source or of a sink runs while the engine waits on
//! it (a read, an acknowledgement, a write), which keeps it deterministic in
//! simulation and makes backpressure exact.

use crate::{
    builder::Operator,
    data::{Datum, Emit, EmitId, Key, Origin, Position, Windowed, datum},
    mutation::{Mutant, planted},
    ops::{
        Aggregator, DynAcc, DynAggregator, DynKey, DynMap, OpError, PartitionReader, SinkConnector, SinkError,
        SourceConnector, SourceError, SourceRecord, TypedAggregator, WindowAssigner,
    },
    registry::{OpContext, OpKind, TypeTag},
    time::{Timestamp, Window},
};
use async_trait::async_trait;
use moonpool_core::{Providers, TimeProvider};
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    any::{Any, TypeId},
    collections::VecDeque,
    future::{Future, poll_fn},
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll, Waker},
    time::Duration,
};

/// What an operator is handed when it starts: the providers of the runtime,
/// the clock, the secrets of its tenant, and where it runs.
pub type Context<P> = OpContext<P>;

/// The other end of a pipe is gone: the node is stopping.
///
/// A source returns it from `start` with `?`, and the engine takes it for what
/// it is, not for a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the pipe is closed")]
pub struct Closed;

impl From<Closed> for OpError {
    fn from(_: Closed) -> Self { OpError::Closed }
}

type Run = Pin<Box<dyn Future<Output = Result<(), OpError>> + Send>>;

fn wake(waker: &mut Option<Waker>) {
    if let Some(waker) = waker.take() {
        waker.wake();
    }
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

/// Reads from the outside world, partition by partition.
///
/// The engine calls [`Source::start`] once per partition it reads, and again
/// after a failure or when another worker takes the partition over, each time
/// with the last position it committed: a source must be able to read again
/// from a position. What it pushes after that position twice is dropped, what
/// it skips is lost.
///
/// ```
/// use streaming_core::{Context, OpError, Operator, Position, Source, SourcePipe};
/// use streaming_core::moonpool_core::Providers;
///
/// /// The integers, in order, on every partition.
/// #[derive(Debug, Clone, Operator)]
/// #[operator("acme/integers@1")]
/// pub struct Integers {
///     /// Number of partitions.
///     #[param(default = 1, min = 1)]
///     pub partitions: u32,
/// }
///
/// impl Source for Integers {
///     type Out = u64;
///
///     async fn partitions<P: Providers>(&self, _: &Context<P>) -> Result<u32, OpError> { Ok(self.partitions) }
///
///     async fn start<P: Providers>(
///         &self,
///         ctx: &Context<P>,
///         _partition: u32,
///         after: Option<Position>,
///         out: SourcePipe<u64>,
///     ) -> Result<(), OpError> {
///         let mut next = after.map_or(0, |position| position.2 + 1);
///         loop {
///             // Waits while downstream is full: this is the backpressure.
///             out.send(Position(0, 0, next), ctx.clock.now_ms(), next).await?;
///             next += 1;
///         }
///     }
/// }
/// ```
pub trait Source: Operator {
    /// What the source produces.
    type Out: Any + Send + Sync;

    /// How many partitions the source has now. Asked again from time to time:
    /// a change replans the graph.
    fn partitions<P: Providers>(&self, ctx: &Context<P>) -> impl Future<Output = Result<u32, OpError>> + Send;

    /// Read `partition` after `after`, the last position the engine committed
    /// (`None` on a partition never read), and push every record into `out`
    /// until the pipe closes.
    ///
    /// Returning while the pipe is open is a failure: the engine starts the
    /// partition again after a pause.
    fn start<P: Providers>(
        &self,
        ctx: &Context<P>,
        partition: u32,
        after: Option<Position>,
        out: SourcePipe<Self::Out>,
    ) -> impl Future<Output = Result<(), OpError>> + Send;
}

#[derive(Clone, Copy)]
enum End {
    /// The source never said: the last position it sent stands for it.
    Unknown,
    Known(Option<Position>),
}

struct SourceState {
    queue: VecDeque<SourceRecord>,
    capacity: usize,
    closed: bool,
    committed: Option<Position>,
    sent: Option<Position>,
    end: End,
    /// The reader of a connector the source handed the partition to.
    handed: Option<Box<dyn PartitionReader>>,
    source: Option<Waker>,
    reader: Option<Waker>,
}

/// Where a [`Source`] pushes what it reads.
pub struct SourcePipe<T> {
    state: Arc<Mutex<SourceState>>,
    _out: PhantomData<fn(T)>,
}

impl<T: Any + Send + Sync> SourcePipe<T> {
    /// Push one record, with its position in the partition and its event
    /// time. Positions grow from one record to the next.
    ///
    /// Waits while downstream is full, which is how backpressure reaches the
    /// source.
    ///
    /// # Errors
    ///
    /// [`Closed`] once the node stops.
    pub async fn send(&self, position: Position, event_time: Timestamp, value: T) -> Result<(), Closed> {
        let mut record = Some(SourceRecord {
            position,
            event_time,
            value: datum(value),
        });
        poll_fn(|cx| {
            let mut state = self.state.lock();
            if state.closed {
                return Poll::Ready(Err(Closed));
            }
            if state.queue.len() >= state.capacity {
                state.source = Some(cx.waker().clone());
                return Poll::Pending;
            }
            if let Some(record) = record.take() {
                state.sent = state.sent.max(Some(record.position));
                state.queue.push_back(record);
                wake(&mut state.reader);
            }
            Poll::Ready(Ok(()))
        })
        .await
    }

    /// Wait for the engine to commit, and return the position up to which
    /// everything is durably processed: what a source acknowledges to its
    /// broker. Positions committed while nobody waited are not replayed: the
    /// latest one is returned.
    ///
    /// A source that has nothing to acknowledge never calls it.
    ///
    /// # Errors
    ///
    /// [`Closed`] once the node stops.
    pub async fn committed(&self) -> Result<Position, Closed> {
        poll_fn(|cx| {
            let mut state = self.state.lock();
            if let Some(position) = state.committed.take() {
                return Poll::Ready(Ok(position));
            }
            if state.closed {
                return Poll::Ready(Err(Closed));
            }
            state.source = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    /// Say where the partition ends now: the position of its last record, or
    /// `None` when it holds nothing. The engine tells an idle partition from
    /// a reader that lags with it.
    ///
    /// A source that cannot know never calls it, and the last position it
    /// sent stands for the end.
    pub fn end(&self, position: Option<Position>) { self.state.lock().end = End::Known(position); }

    /// The position the engine committed since the last time it was asked,
    /// without waiting: see [`SourcePipe::committed`].
    #[must_use]
    pub fn take_committed(&self) -> Option<Position> { self.state.lock().committed.take() }

    /// How many records downstream takes right now before [`SourcePipe::send`]
    /// waits. At least one, so that a source always has something to ask
    /// its broker for.
    #[must_use]
    pub fn room(&self) -> usize {
        let state = self.state.lock();
        state.capacity.saturating_sub(state.queue.len()).max(1)
    }

    /// Hand the partition to a [`SourceConnector`], for a source built on a
    /// connector that already exists: opens `partition` after `after`, then
    /// lets the engine read, acknowledge and ask for the end of the partition
    /// straight from the connector, until the node stops. Nothing is pushed
    /// through the pipe any more: the connector gives values of type `T`.
    ///
    /// # Errors
    ///
    /// The connector could not open the partition, or [`OpError::Closed`]
    /// once the node stops.
    pub async fn read(
        &self,
        connector: &dyn SourceConnector,
        partition: u32,
        after: Option<Position>,
    ) -> Result<(), OpError> {
        let reader = connector
            .open(partition, after)
            .await
            .map_err(|error| OpError::Failed(error.0))?;
        {
            let mut state = self.state.lock();
            state.handed = Some(reader);
            wake(&mut state.reader);
        }
        self.closed().await;
        Err(OpError::Closed)
    }

    /// Wait for the node to stop.
    pub async fn closed(&self) {
        poll_fn(|cx| {
            let mut state = self.state.lock();
            if state.closed {
                return Poll::Ready(());
            }
            state.source = Some(cx.waker().clone());
            Poll::Pending
        })
        .await;
    }
}

struct PipeSource<P: Providers, S> {
    op: Arc<S>,
    ctx: Arc<Context<P>>,
}

/// The connector the engine drives for a [`Source`].
pub(crate) fn source_connector<P: Providers, S: Source>(op: S, ctx: &Context<P>) -> Arc<dyn SourceConnector> {
    Arc::new(PipeSource {
        op: Arc::new(op),
        ctx: Arc::new(ctx.clone()),
    })
}

#[async_trait]
impl<P: Providers, S: Source> SourceConnector for PipeSource<P, S> {
    async fn partitions(&self) -> Result<u32, SourceError> {
        self.op
            .partitions(&self.ctx)
            .await
            .map_err(|error| SourceError(error.to_string()))
    }

    async fn open(&self, partition: u32, after: Option<Position>) -> Result<Box<dyn PartitionReader>, SourceError> {
        let state = Arc::new(Mutex::new(SourceState {
            queue: VecDeque::new(),
            capacity: 1,
            closed: false,
            committed: None,
            sent: None,
            end: End::Unknown,
            handed: None,
            source: None,
            reader: None,
        }));
        let pipe = SourcePipe {
            state: state.clone(),
            _out: PhantomData,
        };
        let (op, ctx) = (self.op.clone(), self.ctx.clone());
        Ok(Box::new(PipeReader {
            providers: self.ctx.providers.clone(),
            state,
            run: Some(Box::pin(async move { op.start(&ctx, partition, after, pipe).await })),
            ended: None,
            handed: None,
        }))
    }
}

/// The reader of one partition of a [`Source`]: the code of the source runs
/// inside its calls.
struct PipeReader<P: Providers> {
    providers: P,
    state: Arc<Mutex<SourceState>>,
    run: Option<Run>,
    /// Why the source stopped, once it has.
    ended: Option<String>,
    /// The reader the source handed the partition to
    /// ([`SourcePipe::read`]): every call goes to it.
    handed: Option<Box<dyn PartitionReader>>,
}

impl<P: Providers> PipeReader<P> {
    /// Let the source run until it waits.
    fn drive(&mut self, cx: &mut TaskContext<'_>) {
        let Some(run) = &mut self.run else {
            return;
        };
        if let Poll::Ready(outcome) = run.as_mut().poll(cx) {
            self.run = None;
            self.ended = Some(match outcome {
                Ok(()) | Err(OpError::Closed) => "the source stopped while its pipe was open".to_owned(),
                Err(error) => error.to_string(),
            });
        }
    }

    async fn drive_once(&mut self) {
        poll_fn(|cx| {
            self.drive(cx);
            Poll::Ready(())
        })
        .await;
        self.adopt().await;
    }

    /// Take the reader the source handed the partition to, if it just did,
    /// and tell it what the engine committed while the partition was being
    /// opened.
    async fn adopt(&mut self) {
        if self.handed.is_some() {
            return;
        }
        let (handed, committed) = {
            let mut state = self.state.lock();
            match state.handed.take() {
                Some(handed) => (handed, state.committed.take()),
                None => return,
            }
        };
        let reader = self.handed.insert(handed);
        if let Some(upto) = committed {
            // The engine acknowledges again with its next commit.
            let _ = reader.ack(upto).await;
        }
    }
}

impl<P: Providers> Drop for PipeReader<P> {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        state.closed = true;
        wake(&mut state.source);
    }
}

#[async_trait]
impl<P: Providers> PartitionReader for PipeReader<P> {
    async fn poll(&mut self, max: usize, max_wait: Duration) -> Result<Vec<SourceRecord>, SourceError> {
        if let Some(reader) = &mut self.handed {
            return reader.poll(max, max_wait).await;
        }
        self.state.lock().capacity = max.max(1);
        let providers = self.providers.clone();
        let mut timer = std::pin::pin!(providers.time().sleep(max_wait));
        let polled = poll_fn(|cx| {
            self.drive(cx);
            let mut state = self.state.lock();
            if state.handed.is_some() {
                return Poll::Ready(None);
            }
            if !state.queue.is_empty() {
                let count = state.queue.len().min(max.max(1));
                let records = state.queue.drain(..count).collect();
                wake(&mut state.source);
                return Poll::Ready(Some(Ok(records)));
            }
            // What the source pushed before it stopped is read first.
            if let Some(why) = &self.ended {
                return Poll::Ready(Some(Err(SourceError(why.clone()))));
            }
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Some(Ok(Vec::new())));
            }
            state.reader = Some(cx.waker().clone());
            Poll::Pending
        })
        .await;
        match polled {
            Some(polled) => polled,
            // The source just handed the partition to a connector.
            None => {
                self.adopt().await;
                match &mut self.handed {
                    Some(reader) => reader.poll(max, max_wait).await,
                    None => Ok(Vec::new()),
                }
            }
        }
    }

    async fn ack(&mut self, upto: Position) -> Result<(), SourceError> {
        if let Some(reader) = &mut self.handed {
            return reader.ack(upto).await;
        }
        {
            let mut state = self.state.lock();
            state.committed = state.committed.max(Some(upto));
            wake(&mut state.source);
        }
        // Let the source start acknowledging; it goes on during the next reads.
        self.drive_once().await;
        Ok(())
    }

    async fn end_position(&mut self) -> Result<Option<Position>, SourceError> {
        if self.handed.is_none() {
            self.drive_once().await;
        }
        if let Some(reader) = &mut self.handed {
            return reader.end_position().await;
        }
        // A record pushed and not read yet lies beyond whatever end the
        // source said before pushing it. The engine asks right after a read
        // that emptied the pipe, so only a source that pushes while it is
        // asked (fed by another thread, say) opens that window: no planted
        // bug stands for this precaution, the simulation, where nothing runs
        // between the two calls, could not notice it. `tests/pipes.rs` holds
        // it.
        let state = self.state.lock();
        Ok(match state.end {
            End::Known(end) => end.max(state.sent),
            End::Unknown => state.sent,
        })
    }
}

// ---------------------------------------------------------------------------
// Sinks
// ---------------------------------------------------------------------------

/// Writes to the outside world.
///
/// The engine hands batches through the pipe, one at a time. A batch is
/// written once [`Batch::done`] is called: a sink that says it failed
/// ([`Batch::failed`]), that asks for the next batch without confirming this
/// one, or that returns is given the same batch again, with the same
/// [`EmitId`]s, a failure of the worker included. The
/// destination must end up holding each element once: writes are idempotent
/// on these identifiers.
///
/// ```
/// use streaming_core::{Context, OpError, Operator, Sink, SinkPipe};
/// use streaming_core::moonpool_core::Providers;
///
/// /// Prints what it receives.
/// #[derive(Debug, Clone, Operator)]
/// #[operator("acme/print@1")]
/// pub struct Print;
///
/// impl Sink<String> for Print {
///     async fn start<P: Providers>(&self, _: &Context<P>, mut input: SinkPipe<String>) -> Result<(), OpError> {
///         while let Some(batch) = input.next().await {
///             for element in batch.iter() {
///                 println!("{} {}", element.id, element.value);
///             }
///             batch.done();
///         }
///         Ok(())
///     }
/// }
/// ```
pub trait Sink<In>: Operator {
    /// Write the batches of `input` until it ends.
    fn start<P: Providers>(
        &self,
        ctx: &Context<P>,
        input: SinkPipe<In>,
    ) -> impl Future<Output = Result<(), OpError>> + Send;
}

#[derive(Default)]
struct SinkState {
    /// The batch the engine wants written, until the sink takes it.
    offered: Option<(u64, Vec<Emit>)>,
    /// The last batch the sink took.
    taken: Option<u64>,
    /// The last batch the sink confirmed.
    done: Option<u64>,
    /// The sink asked for more without confirming what it took.
    skipped: bool,
    /// The sink said it could not write the batch it took, and why.
    failed: Option<(u64, String)>,
    closed: bool,
    sink: Option<Waker>,
    writer: Option<Waker>,
}

/// Where a [`Sink`] receives what it writes.
pub struct SinkPipe<T> {
    state: Arc<Mutex<SinkState>>,
    _in: PhantomData<fn() -> T>,
}

impl<T: Any + Send + Sync> SinkPipe<T> {
    /// The next batch to write, or `None` once the node stops.
    pub async fn next(&mut self) -> Option<Batch<T>> {
        poll_fn(|cx| {
            let mut state = self.state.lock();
            if state.closed {
                return Poll::Ready(None);
            }
            if let Some((sequence, emits)) = state.offered.take() {
                state.taken = Some(sequence);
                return Poll::Ready(Some(Batch {
                    sequence,
                    emits,
                    state: self.state.clone(),
                    _in: PhantomData,
                }));
            }
            let settled = state.done == state.taken
                || state
                    .failed
                    .as_ref()
                    .is_some_and(|(sequence, _)| Some(*sequence) == state.taken);
            if state.taken.is_some() && !settled && !state.skipped {
                state.skipped = true;
                wake(&mut state.writer);
            }
            state.sink = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

impl<T: Any + Send + Sync> SinkPipe<T> {
    /// Hand every batch to a [`SinkConnector`], for a sink built on a
    /// connector that already exists: a batch is done once the connector
    /// wrote it, and comes back when it failed. Returns once the node stops.
    pub async fn write_with(&mut self, connector: &dyn SinkConnector) {
        while let Some(batch) = self.next().await {
            match connector.write(batch.emits()).await {
                Ok(()) => batch.done(),
                Err(error) => batch.failed(error.0),
            }
        }
    }
}

/// Elements to write together.
pub struct Batch<T> {
    sequence: u64,
    emits: Vec<Emit>,
    state: Arc<Mutex<SinkState>>,
    _in: PhantomData<fn() -> T>,
}

/// One element of a [`Batch`].
pub struct Element<'a, T> {
    /// Deterministic identifier, the same every time the element comes back.
    pub id: &'a EmitId,
    /// Where the element comes from. Inside one slice, positions never
    /// decrease from one element to the next.
    pub origin: &'a Origin,
    /// Event time.
    pub ts: Timestamp,
    /// Grouping key, when the element went through a `key_by`.
    pub key: Option<&'a Key>,
    /// The payload.
    pub value: &'a T,
}

impl<T: Any + Send + Sync> Batch<T> {
    /// The elements, in order.
    pub fn iter(&self) -> impl Iterator<Item = Element<'_, T>> {
        self.emits.iter().filter_map(|emit| {
            Some(Element {
                id: &emit.id,
                origin: &emit.origin,
                ts: emit.ts,
                key: emit.key.as_ref(),
                value: typed::<T>(&emit.value).ok()?,
            })
        })
    }

    /// The elements as the engine holds them, payloads not yet typed.
    #[must_use]
    pub fn emits(&self) -> &[Emit] { &self.emits }

    /// Number of elements.
    #[must_use]
    pub fn len(&self) -> usize { self.emits.len() }

    /// Whether the batch holds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.emits.is_empty() }

    /// The batch is durably written.
    pub fn done(self) {
        let mut state = self.state.lock();
        state.done = Some(self.sequence);
        wake(&mut state.writer);
    }

    /// The batch could not be written, or nobody knows whether it was: the
    /// engine says `why` and hands it over again after a pause.
    pub fn failed(self, why: impl std::fmt::Display) {
        let mut state = self.state.lock();
        state.failed = Some((self.sequence, why.to_string()));
        wake(&mut state.writer);
    }
}

struct SinkRun {
    state: Arc<Mutex<SinkState>>,
    future: Option<Run>,
    sequence: u64,
}

impl Drop for SinkRun {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        state.closed = true;
        wake(&mut state.sink);
    }
}

struct PipeSink<P: Providers, In, S> {
    op: Arc<S>,
    ctx: Arc<Context<P>>,
    /// The running sink, between two writes.
    run: Mutex<Option<SinkRun>>,
    _in: PhantomData<fn(In)>,
}

/// The connector the engine drives for a [`Sink`].
pub(crate) fn sink_connector<P: Providers, In: Any + Send + Sync, S: Sink<In>>(
    op: S,
    ctx: &Context<P>,
) -> Arc<dyn SinkConnector> {
    Arc::new(PipeSink {
        op: Arc::new(op),
        ctx: Arc::new(ctx.clone()),
        run: Mutex::new(None),
        _in: PhantomData::<fn(In)>,
    })
}

impl<P: Providers, In: Any + Send + Sync, S: Sink<In>> PipeSink<P, In, S> {
    fn begin(&self) -> SinkRun {
        let state = Arc::new(Mutex::new(SinkState::default()));
        let pipe = SinkPipe::<In> {
            state: state.clone(),
            _in: PhantomData,
        };
        let (op, ctx) = (self.op.clone(), self.ctx.clone());
        SinkRun {
            state,
            future: Some(Box::pin(async move { op.start(&ctx, pipe).await })),
            sequence: 0,
        }
    }
}

#[async_trait]
impl<P: Providers, In: Any + Send + Sync, S: Sink<In>> SinkConnector for PipeSink<P, In, S> {
    async fn write(&self, batch: &[Emit]) -> Result<(), SinkError> {
        if batch.is_empty() {
            return Ok(());
        }
        for emit in batch {
            typed::<In>(&emit.value).map_err(|error| SinkError(error.to_string()))?;
        }
        // A write that is abandoned half-way drops the run: the sink starts
        // again with the next one, and is given the batch again.
        let taken = self.run.lock().take();
        let mut run = taken.unwrap_or_else(|| self.begin());
        run.sequence += 1;
        let sequence = run.sequence;
        {
            let mut state = run.state.lock();
            state.offered = Some((sequence, batch.to_vec()));
            state.skipped = false;
            wake(&mut state.sink);
        }
        // A batch is written once its sink said so, and only then.
        let unconfirmed = |why: String| {
            if planted(Mutant::ConfirmWithoutTheSink) {
                Ok(())
            } else {
                Err(SinkError(why))
            }
        };
        let outcome = poll_fn(|cx| {
            let ended = match run.future.as_mut().map(|future| future.as_mut().poll(cx)) {
                Some(Poll::Ready(outcome)) => {
                    run.future = None;
                    Some(outcome)
                }
                Some(Poll::Pending) => None,
                None => Some(Ok(())),
            };
            let mut state = run.state.lock();
            if state.done == Some(sequence) {
                return Poll::Ready(Ok(()));
            }
            if let Some((_, why)) = state.failed.take_if(|(failed, _)| *failed == sequence) {
                return Poll::Ready(unconfirmed(why));
            }
            if let Some(outcome) = ended {
                return Poll::Ready(unconfirmed(match outcome {
                    Ok(()) | Err(OpError::Closed) => "the sink stopped before confirming the batch".to_owned(),
                    Err(error) => error.to_string(),
                }));
            }
            if state.skipped {
                return Poll::Ready(unconfirmed(
                    "the sink asked for the next batch without confirming this one".to_owned(),
                ));
            }
            state.writer = Some(cx.waker().clone());
            Poll::Pending
        })
        .await;
        if run.future.is_some() {
            *self.run.lock() = Some(run);
        }
        outcome
    }
}

// ---------------------------------------------------------------------------
// Transformers
// ---------------------------------------------------------------------------

/// Everything between a source and a sink: takes a pipe, returns a pipe.
///
/// `In` is what flows in: a plain type, [`Keyed`] after a `key_by`,
/// [`Windows`] after a window. A transformer written for any element type
/// (`impl<T> Transformer<Keyed<T>> for Tumbling`) serves every graph.
///
/// A transformer is one node of a graph, so it does one thing: any number of
/// `map`, `filter` and `flat_map` in a row, or one `key_by`, or one window, or
/// one aggregate. What it does must not depend on its configuration.
///
/// `transform` is called when an actor of the node starts, on each worker
/// running one: what it captures is per actor, and is not state the engine
/// keeps.
pub trait Transformer<In>: Operator {
    /// What flows out.
    type Out;

    /// Say what happens to the elements of `input`.
    fn transform(&self, input: Pipe<In>) -> Pipe<Self::Out>;
}

/// Elements that carry a grouping key: what a `key_by` produces and a window
/// takes.
pub struct Keyed<T>(PhantomData<fn() -> T>);

/// Elements assigned to their event-time windows: what a window produces and
/// an aggregate takes.
pub struct Windows<T>(PhantomData<fn() -> T>);

/// An element whose type an operator does not look at.
///
/// Registering a transformer written for any element type under it
/// (`registry.transformer::<Windows<Opaque>, _>(&Count)`) makes it serve
/// every type at once, for graphs that are not built with typed nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Opaque;

static OPAQUE: Opaque = Opaque;

/// A payload as the type an operator was written for.
fn typed<T: Any>(value: &Datum) -> Result<&T, OpError> {
    if TypeId::of::<T>() == TypeId::of::<Opaque>() {
        let opaque: &dyn Any = &OPAQUE;
        return opaque.downcast_ref::<T>().ok_or(OpError::Type { expected: "Opaque" });
    }
    crate::ops::downcast::<T>(value)
}

/// The tag of a type an operator was written for.
pub(crate) fn tag<T: Any>() -> TypeTag {
    if TypeId::of::<T>() == TypeId::of::<Opaque>() {
        TypeTag::Any
    } else {
        TypeTag::of::<T>()
    }
}

#[derive(Clone)]
enum Step {
    Map(Arc<dyn DynMap>),
    Key(Arc<dyn DynKey>),
    Window(Arc<dyn WindowAssigner>),
    Aggregate(Arc<dyn DynAggregator>),
}

/// What flows through a [`Transformer`].
///
/// Lazy: its methods say what happens to each element once the node runs.
pub struct Pipe<T> {
    steps: Vec<Step>,
    /// What the first step takes and the last one gives.
    types: Option<(TypeTag, TypeTag)>,
    problem: Option<String>,
    _of: PhantomData<fn() -> T>,
}

impl<T> Pipe<T> {
    /// The pipe a transformer is handed.
    pub(crate) fn input() -> Self {
        Self {
            steps: Vec::new(),
            types: None,
            problem: None,
            _of: PhantomData,
        }
    }

    fn then<O>(mut self, step: Step, input: TypeTag, output: TypeTag) -> Pipe<O> {
        self.steps.push(step);
        Pipe {
            steps: self.steps,
            types: Some((self.types.map_or(input, |(first, _)| first), output)),
            problem: self.problem,
            _of: PhantomData,
        }
    }
}

struct MapFn<In, Out, F>(F, PhantomData<fn(&In) -> Out>);

impl<In, Out, F> DynMap for MapFn<In, Out, F>
where
    In: Any + Send + Sync,
    Out: Any + Send + Sync,
    F: Fn(&In, &mut Vec<Out>) + Send + Sync + 'static,
{
    fn apply(&self, input: &Datum, out: &mut Vec<Datum>) -> Result<(), OpError> {
        let mut produced = Vec::new();
        (self.0)(typed::<In>(input)?, &mut produced);
        out.extend(produced.into_iter().map(datum));
        Ok(())
    }
}

struct FilterFn<In, F>(F, PhantomData<fn(&In)>);

impl<In, F> DynMap for FilterFn<In, F>
where
    In: Any + Send + Sync,
    F: Fn(&In) -> bool + Send + Sync + 'static,
{
    fn apply(&self, input: &Datum, out: &mut Vec<Datum>) -> Result<(), OpError> {
        if (self.0)(typed::<In>(input)?) {
            out.push(input.clone());
        }
        Ok(())
    }
}

struct KeyFn<In, K, F>(F, PhantomData<fn(&In) -> K>);

impl<In, K, F> DynKey for KeyFn<In, K, F>
where
    In: Any + Send + Sync,
    K: Serialize + 'static,
    F: Fn(&In) -> K + Send + Sync + 'static,
{
    fn key(&self, input: &Datum) -> Result<Key, OpError> {
        Key::encode(&(self.0)(typed::<In>(input)?)).map_err(OpError::Failed)
    }
}

/// Several stateless steps in a row, as one.
struct Chain(Vec<Arc<dyn DynMap>>);

impl DynMap for Chain {
    fn apply(&self, input: &Datum, out: &mut Vec<Datum>) -> Result<(), OpError> {
        let mut current = vec![input.clone()];
        for step in &self.0 {
            let mut next = Vec::new();
            for value in &current {
                step.apply(value, &mut next)?;
            }
            current = next;
        }
        out.append(&mut current);
        Ok(())
    }
}

/// Forwards every element unchanged.
struct Forward;

impl DynMap for Forward {
    fn apply(&self, input: &Datum, out: &mut Vec<Datum>) -> Result<(), OpError> {
        out.push(input.clone());
        Ok(())
    }
}

impl<T: Any + Send + Sync> Pipe<T> {
    /// Turn each element into another.
    #[must_use]
    pub fn map<O: Any + Send + Sync>(self, f: impl Fn(&T) -> O + Send + Sync + 'static) -> Pipe<O> {
        self.flat_map(move |input, out| out.push(f(input)))
    }

    /// Keep the elements `f` accepts.
    #[must_use]
    pub fn filter(self, f: impl Fn(&T) -> bool + Send + Sync + 'static) -> Pipe<T> {
        self.elements("filter")
            .then(Step::Map(Arc::new(FilterFn(f, PhantomData))), tag::<T>(), tag::<T>())
    }

    /// Turn each element into another, or drop it.
    #[must_use]
    pub fn filter_map<O: Any + Send + Sync>(self, f: impl Fn(&T) -> Option<O> + Send + Sync + 'static) -> Pipe<O> {
        self.flat_map(move |input, out| out.extend(f(input)))
    }

    /// Turn each element into any number of others.
    #[must_use]
    pub fn flat_map<O: Any + Send + Sync>(self, f: impl Fn(&T, &mut Vec<O>) + Send + Sync + 'static) -> Pipe<O> {
        self.elements("map")
            .then(Step::Map(Arc::new(MapFn(f, PhantomData))), tag::<T>(), tag::<O>())
    }

    /// Group the elements: those of one key meet in the same windows.
    #[must_use]
    pub fn key_by<K: Serialize + 'static>(self, f: impl Fn(&T) -> K + Send + Sync + 'static) -> Pipe<Keyed<T>> {
        self.elements("key_by")
            .then(Step::Key(Arc::new(KeyFn(f, PhantomData))), tag::<T>(), tag::<T>())
    }

    /// Remember that `step` was asked of a pipe that does not carry plain
    /// elements: the compiler lets it through, the registration refuses it.
    fn elements(mut self, step: &str) -> Self {
        let name = std::any::type_name::<T>();
        let carries = if name.starts_with(concat!(module_path!(), "::Keyed<")) {
            Some("keyed elements, which only take a window")
        } else if name.starts_with(concat!(module_path!(), "::Windows<")) {
            Some("windows, which only take an aggregate")
        } else {
            None
        };
        if let Some(carries) = carries {
            self.problem
                .get_or_insert_with(|| format!("`{step}` is asked of a pipe that carries {carries}"));
        }
        self
    }
}

struct AssignFn<F>(Duration, F);

impl<F: Fn(Timestamp, &mut Vec<Window>) + Send + Sync + 'static> WindowAssigner for AssignFn<F> {
    fn assign(&self, ts: Timestamp, out: &mut Vec<Window>) { (self.1)(ts, out); }

    fn allowed_lateness(&self) -> Duration { self.0 }
}

impl<T: Any + Send + Sync> Pipe<Keyed<T>> {
    /// Assign each element to the event-time windows `assign` pushes for its
    /// timestamp. A window still accepts elements for `allowed_lateness`
    /// after its end.
    #[must_use]
    pub fn window(
        self,
        allowed_lateness: Duration,
        assign: impl Fn(Timestamp, &mut Vec<Window>) + Send + Sync + 'static,
    ) -> Pipe<Windows<T>> {
        self.window_with(AssignFn(allowed_lateness, assign))
    }

    /// Assign each element to the windows of `assigner`.
    #[must_use]
    pub fn window_with(self, assigner: impl WindowAssigner) -> Pipe<Windows<T>> {
        self.then(Step::Window(Arc::new(assigner)), TypeTag::Any, TypeTag::Any)
    }
}

struct Fold<T, A, I, D, M> {
    init: I,
    add: D,
    merge: M,
    _of: PhantomData<fn(&T) -> A>,
}

impl<T, A, I, D, M> DynAggregator for Fold<T, A, I, D, M>
where
    T: Any + Send + Sync,
    A: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    I: Fn() -> A + Send + Sync + 'static,
    D: Fn(&mut A, &T) + Send + Sync + 'static,
    M: Fn(&mut A, &A) + Send + Sync + 'static,
{
    fn init(&self) -> DynAcc { Box::new((self.init)()) }

    fn add(&self, acc: &mut DynAcc, item: &Datum) -> Result<(), OpError> {
        let item = typed::<T>(item)?;
        (self.add)(accumulator_mut::<A>(acc)?, item);
        Ok(())
    }

    fn merge(&self, into: &mut DynAcc, other: &DynAcc) -> Result<(), OpError> {
        let other = accumulator::<A>(other)?;
        (self.merge)(accumulator_mut::<A>(into)?, other);
        Ok(())
    }

    fn finish(&self, acc: &DynAcc, window: Window, key: &Key) -> Result<Datum, OpError> {
        Ok(datum(Windowed {
            window,
            key: key.clone(),
            value: accumulator::<A>(acc)?.clone(),
        }))
    }

    fn encode(&self, acc: &DynAcc) -> Result<Vec<u8>, OpError> {
        postcard::to_allocvec(accumulator::<A>(acc)?).map_err(|error| OpError::Failed(error.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<DynAcc, OpError> {
        postcard::from_bytes::<A>(bytes)
            .map(|acc| Box::new(acc) as DynAcc)
            .map_err(|error| OpError::Failed(format!("cannot decode accumulator: {error}")))
    }
}

fn accumulator<A: Any>(acc: &DynAcc) -> Result<&A, OpError> {
    acc.downcast_ref::<A>().ok_or(OpError::Type {
        expected: std::any::type_name::<A>(),
    })
}

fn accumulator_mut<A: Any>(acc: &mut DynAcc) -> Result<&mut A, OpError> {
    acc.downcast_mut::<A>().ok_or(OpError::Type {
        expected: std::any::type_name::<A>(),
    })
}

impl<T: Any + Send + Sync> Pipe<Windows<T>> {
    /// Keep a state per key and per window, and emit it when the window
    /// closes.
    ///
    /// `init` is the state before any element, `add` folds an element in,
    /// `merge` folds in the state another partition kept for the same key and
    /// window. The engine stores the state with the position of the source,
    /// in one commit: this is what makes the count exact.
    #[must_use]
    pub fn aggregate<A>(
        self,
        init: impl Fn() -> A + Send + Sync + 'static,
        add: impl Fn(&mut A, &T) + Send + Sync + 'static,
        merge: impl Fn(&mut A, &A) + Send + Sync + 'static,
    ) -> Pipe<Windowed<A>>
    where
        A: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let fold = Fold {
            init,
            add,
            merge,
            _of: PhantomData::<fn(&T) -> A>,
        };
        self.then(Step::Aggregate(Arc::new(fold)), tag::<T>(), tag::<Windowed<A>>())
    }

    /// Aggregate with an [`Aggregator`], whose result may differ from its
    /// state.
    #[must_use]
    pub fn aggregate_with<A: Aggregator<In = T>>(self, aggregator: A) -> Pipe<Windowed<A::Out>> {
        self.then(
            Step::Aggregate(Arc::new(TypedAggregator(aggregator))),
            tag::<T>(),
            tag::<Windowed<A::Out>>(),
        )
    }
}

/// What a transformer turned out to be, for the engine.
pub(crate) enum Built {
    Map(Arc<dyn DynMap>),
    Key(Arc<dyn DynKey>),
    Window(Arc<dyn WindowAssigner>),
    Aggregate(Arc<dyn DynAggregator>),
}

impl Built {
    pub(crate) fn kind(&self) -> OpKind {
        match self {
            Built::Map(_) => OpKind::Map,
            Built::Key(_) => OpKind::KeyBy,
            Built::Window(_) => OpKind::Window,
            Built::Aggregate(_) => OpKind::Aggregate,
        }
    }
}

/// Run `transform` and read what it asked for: the step of the node, what it
/// takes and what it gives.
pub(crate) fn build<In, T: Transformer<In>>(op: &T) -> Result<(Built, TypeTag, TypeTag), OpError> {
    let pipe = op.transform(Pipe::input());
    if let Some(problem) = pipe.problem {
        return Err(OpError::Config(format!("`{}`: {problem}", T::OP)));
    }
    let (input, output) = pipe.types.unwrap_or((TypeTag::Any, TypeTag::Any));
    let mut steps = pipe.steps;
    let built = if steps.iter().all(|step| matches!(step, Step::Map(_))) {
        let mut maps: Vec<Arc<dyn DynMap>> = steps
            .into_iter()
            .filter_map(|step| match step {
                Step::Map(map) => Some(map),
                _ => None,
            })
            .collect();
        Built::Map(match maps.len() {
            0 => Arc::new(Forward),
            1 => maps.remove(0),
            _ => Arc::new(Chain(maps)),
        })
    } else if steps.len() == 1 {
        match steps.remove(0) {
            Step::Map(map) => Built::Map(map),
            Step::Key(key) => Built::Key(key),
            Step::Window(window) => Built::Window(window),
            Step::Aggregate(aggregate) => Built::Aggregate(aggregate),
        }
    } else {
        return Err(OpError::Config(format!(
            "`{}` does several things: a transformer is one node, which is any number of maps and filters in a row, \
             or one `key_by`, or one window, or one aggregate",
            T::OP
        )));
    };
    Ok((built, input, output))
}
