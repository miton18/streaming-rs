//! One running slice: its actors, its epochs, its commits.
//!
//! A slice instance wires one actor per node, connected by bounded edges. Its
//! *root* actor (a source partition reader, or the emitter of closed windows)
//! cuts its input into epochs: a batch of records followed by a barrier. When
//! every *terminal* actor (aggregates and sinks) has reported the barrier of an
//! epoch, the *commit* actor writes the epoch to the KV in one field. The root
//! only acknowledges its input once the epoch is committed.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::FutureExt;
use moonpool_core::{Providers, TaskProvider, TimeProvider};
use parking_lot::Mutex;
use streaming_core::control::{ActorKind, ActorState, GraphPlan, RunState};
use streaming_core::data::hash_str;
use streaming_core::ops::DynAcc;
use streaming_core::spec::GraphSettings;
use streaming_core::{
    DynAggregator, DynKey, DynMap, Emit, EmitId, Key, KvError, NodeId, OpContext, OpError,
    OpFactory, OpKind, Origin, PartitionReader, Position, Record, SinkConnector, SliceId, SliceKey,
    SourceConnector, Timestamp, Topology, Window, WindowAssigner, WorkerId,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::RuntimeEnv;
use crate::mailbox::{Barrier, Input, Inputs, Msg, Outputs};
use crate::metrics::{ActorIdent, ActorMetrics, Meter, SliceMetrics, ts_key};
use crate::mutation::{Mutant, planted};
use crate::scope::TaskScope;
use crate::state::{
    self, CommitOutcome, CommitRecord, NO_WATERMARK, NodeChanges, StateEntry, StateError,
    WindowChanges, emitted,
};

const MIN_BACKOFF: Duration = Duration::from_millis(50);
const MAX_BACKOFF: Duration = Duration::from_secs(2);

/// What the control plane asks of a running slice.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Control {
    pub run: RunState,
    pub paused_nodes: BTreeSet<NodeId>,
}

/// Why a slice instance ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SliceExit {
    /// Stopped by the worker.
    Stopped,
    /// Finished its in-flight work after a drain request.
    Drained,
    /// Another instance of the slice committed in its place.
    Fenced,
    /// Something failed; the slice restarts from its last commit.
    Failed(String),
}

/// What the worker shares with a slice instance.
#[derive(Clone)]
pub(crate) struct SliceShared {
    pub metrics: Arc<SliceMetrics>,
    pub actors: Arc<Mutex<Vec<Arc<ActorMetrics>>>>,
    exit: Arc<Mutex<Option<SliceExit>>>,
}

impl SliceShared {
    pub(crate) fn new() -> Self {
        Self {
            metrics: SliceMetrics::new(),
            actors: Arc::new(Mutex::new(Vec::new())),
            exit: Arc::new(Mutex::new(None)),
        }
    }

    /// The exit reason, once the instance has ended.
    pub(crate) fn exit(&self) -> Option<SliceExit> {
        self.exit.lock().clone()
    }
}

/// Everything needed to run one slice instance.
pub(crate) struct SliceParams<P: Providers> {
    pub env: Arc<RuntimeEnv<P>>,
    pub key: SliceKey,
    pub topology: Arc<Topology>,
    pub plan: Arc<GraphPlan>,
    pub worker: WorkerId,
    pub catch_panics: bool,
}

#[derive(Debug, thiserror::Error)]
enum ActorError {
    #[error("{0}")]
    Op(#[from] OpError),
    #[error("{0}")]
    State(#[from] StateError),
    #[error("{0}")]
    Kv(#[from] KvError),
    #[error("actor panicked")]
    Panicked,
}

/// Records the first exit reason of a slice and stops its actors.
#[derive(Clone)]
struct Exit {
    slot: Arc<Mutex<Option<SliceExit>>>,
    stop: CancellationToken,
}

impl Exit {
    fn finish(&self, exit: SliceExit) {
        self.slot.lock().get_or_insert(exit);
        self.stop.cancel();
    }
}

enum NodeOp {
    Source(Arc<dyn SourceConnector>),
    Emitter(Arc<dyn DynAggregator>, i64),
    Map(Arc<dyn DynMap>),
    Key(Arc<dyn DynKey>),
    Window(Arc<dyn WindowAssigner>),
    Aggregate(Arc<dyn DynAggregator>, i64),
    Sink(Arc<dyn SinkConnector>),
}

/// What terminal actors and the root tell the commit actor.
enum Contribution {
    /// From the root: epoch `epoch` ends at `position` with `watermark`.
    Header {
        epoch: u64,
        position: Option<Position>,
        watermark: Timestamp,
    },
    /// From an aggregate: its state changes for the epoch.
    State { epoch: u64, changes: NodeChanges },
    /// From a sink: everything of the epoch is written.
    Ack { epoch: u64 },
}

type AggState = HashMap<(Window, Key), (DynAcc, u64)>;

/// Windows an aggregate of this slice has closed and that, as far as this
/// instance knows, are not emitted yet. The aggregate actor adds to it; the
/// root of the slice holds back when it grows (see `Root::throttled`).
type Closed = Arc<Mutex<BTreeSet<Window>>>;

/// What the root of a slice watches for one of its aggregates.
struct Gate {
    /// The slice that emits the windows of the aggregate.
    emitter: SliceKey,
    closed: Closed,
}

fn is_closed(window: &Window, lateness_ms: i64, watermark: Timestamp) -> bool {
    watermark != NO_WATERMARK && window.end.saturating_add(lateness_ms) <= watermark
}

fn bump(backoff: &mut Duration) -> Duration {
    let current = *backoff;
    *backoff = (current * 2).min(MAX_BACKOFF);
    current
}

/// Whether repeating a store call can cure its error.
trait Retry {
    fn transient(&self) -> bool;
}

impl Retry for KvError {
    fn transient(&self) -> bool {
        !matches!(self, KvError::TooLarge { .. })
    }
}

impl Retry for StateError {
    fn transient(&self) -> bool {
        matches!(self, StateError::Kv(error) if error.transient())
    }
}

/// Repeat an idempotent store call until it gets through, however long the
/// store stays away. An error that repeating cannot cure is returned.
///
/// A slice that gave up on the first error would have to get every call of a
/// sequence through in a row: with a store failing one call in six, thirty
/// calls in a row succeed less than once in two hundred attempts.
async fn persist<P, T, E, Fut>(
    env: &RuntimeEnv<P>,
    note: impl Fn(String),
    mut call: impl FnMut() -> Fut,
) -> Result<T, E>
where
    P: Providers,
    E: Retry + std::fmt::Display,
    Fut: Future<Output = Result<T, E>>,
{
    let mut backoff = MIN_BACKOFF;
    loop {
        match call().await {
            Ok(value) => return Ok(value),
            Err(error) if error.transient() => {
                note(error.to_string());
                let _ = env.providers.time().sleep(bump(&mut backoff)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn context<P: Providers>(env: &RuntimeEnv<P>, key: &SliceKey, node: &NodeId) -> OpContext<P> {
    OpContext {
        providers: env.providers.clone(),
        clock: env.clock.clone(),
        secrets: env.secrets.clone(),
        tenant: key.tenant.clone(),
        graph: key.graph.clone(),
        node: node.clone(),
    }
}

fn lateness_of<P: Providers>(
    env: &RuntimeEnv<P>,
    topology: &Topology,
    aggregate: &NodeId,
) -> Result<i64, OpError> {
    let window = topology
        .inputs
        .get(aggregate)
        .and_then(|inputs| inputs.first())
        .ok_or_else(|| OpError::Config(format!("aggregate `{aggregate}` has no window input")))?;
    let spec = topology
        .spec
        .node(window)
        .ok_or_else(|| OpError::Config(format!("unknown node `{window}`")))?;
    match env.registry.get(&spec.op) {
        Some((_, OpFactory::Window(factory))) => {
            Ok(factory(&spec.config)?.allowed_lateness().as_millis() as i64)
        }
        _ => Err(OpError::Config(format!(
            "`{window}` is not a window operator known to this worker"
        ))),
    }
}

fn build_op<P: Providers>(
    env: &RuntimeEnv<P>,
    topology: &Topology,
    key: &SliceKey,
    node: &NodeId,
    is_root: bool,
) -> Result<NodeOp, OpError> {
    let spec = topology
        .spec
        .node(node)
        .ok_or_else(|| OpError::Config(format!("unknown node `{node}`")))?;
    let (_, factory) = env.registry.get(&spec.op).ok_or_else(|| {
        OpError::Config(format!(
            "operator `{}` is not compiled into this worker",
            spec.op
        ))
    })?;
    Ok(match factory {
        OpFactory::Source(factory) => {
            NodeOp::Source(factory(&context(env, key, node), &spec.config)?)
        }
        OpFactory::Sink(factory) => NodeOp::Sink(factory(&context(env, key, node), &spec.config)?),
        OpFactory::Map(factory) => NodeOp::Map(factory(&spec.config)?),
        OpFactory::Key(factory) => NodeOp::Key(factory(&spec.config)?),
        OpFactory::Window(factory) => NodeOp::Window(factory(&spec.config)?),
        OpFactory::Aggregate(factory) => {
            let aggregator = factory(&spec.config)?;
            let lateness = lateness_of(env, topology, node)?;
            if is_root {
                NodeOp::Emitter(aggregator, lateness)
            } else {
                NodeOp::Aggregate(aggregator, lateness)
            }
        }
    })
}

/// What every actor of a slice holds.
struct ActorCtx<P: Providers> {
    env: Arc<RuntimeEnv<P>>,
    key: SliceKey,
    node: NodeId,
    node_hash: u64,
    meter: Meter<P>,
    control: watch::Receiver<Control>,
    settings: GraphSettings,
}

impl<P: Providers> ActorCtx<P> {
    async fn sleep(&self, duration: Duration) {
        let _ = self.env.providers.time().sleep(duration).await;
    }

    fn now(&self) -> Duration {
        self.env.providers.time().now()
    }

    /// Repeat an idempotent store call until it gets through.
    async fn persist<T, E, Fut>(&self, call: impl FnMut() -> Fut) -> Result<T, E>
    where
        E: Retry + std::fmt::Display,
        Fut: Future<Output = Result<T, E>>,
    {
        persist(&self.env, |error| self.meter.metrics.error(error), call).await
    }

    /// Suspend while the node is paused by the control plane.
    async fn wait_unpaused(&mut self) {
        loop {
            if !self
                .control
                .borrow_and_update()
                .paused_nodes
                .contains(&self.node)
            {
                self.meter.metrics.set_state(ActorState::Running);
                return;
            }
            self.meter.metrics.set_state(ActorState::Paused);
            self.meter.idle();
            if self.control.changed().await.is_err() {
                return futures::future::pending().await;
            }
        }
    }

    /// An operator failed on a record: the slice fails and restarts from its
    /// last commit, so that no record is ever dropped silently.
    fn op_error(&self, error: OpError) -> ActorError {
        self.meter.metrics.error(error.to_string());
        ActorError::Op(error)
    }
}

async fn run_transform<P: Providers>(
    mut ctx: ActorCtx<P>,
    mut inputs: Inputs,
    outputs: Outputs,
    mut transform: impl FnMut(&Record, u64, &mut Vec<Record>) -> Result<(), OpError> + Send,
) -> Result<(), ActorError> {
    loop {
        ctx.wait_unpaused().await;
        ctx.meter.idle();
        match inputs.next().await {
            Input::Records(records) => {
                ctx.meter.busy();
                ctx.meter.records_in(records.len());
                let mut out = Vec::with_capacity(records.len());
                for record in &records {
                    transform(record, ctx.node_hash, &mut out).map_err(|e| ctx.op_error(e))?;
                }
                if !out.is_empty() {
                    ctx.meter.records_out(out.len());
                    outputs.send(&ctx.meter, Msg::Records(out)).await;
                }
            }
            Input::Barrier(barrier) => {
                ctx.meter.busy();
                outputs.send(&ctx.meter, Msg::Barrier(barrier)).await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_aggregate<P: Providers>(
    mut ctx: ActorCtx<P>,
    aggregator: Arc<dyn DynAggregator>,
    lateness_ms: i64,
    mut inputs: Inputs,
    commits: mpsc::UnboundedSender<Contribution>,
    mut state: AggState,
    mut watermark: Timestamp,
    closed: Closed,
) -> Result<(), ActorError> {
    let mut dirty: BTreeSet<(Window, Key)> = BTreeSet::new();
    loop {
        ctx.wait_unpaused().await;
        ctx.meter.idle();
        match inputs.next().await {
            Input::Records(records) => {
                ctx.meter.busy();
                ctx.meter.records_in(records.len());
                for record in &records {
                    let (Some(window), Some(key)) = (record.window, record.key.as_ref()) else {
                        return Err(ctx.op_error(OpError::Failed(
                            "a record reached an aggregate without key and window".into(),
                        )));
                    };
                    if is_closed(&window, lateness_ms, watermark) {
                        ctx.meter
                            .metrics
                            .late_dropped
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let slot = state
                        .entry((window, key.clone()))
                        .or_insert_with(|| (aggregator.init(), 0));
                    aggregator
                        .add(&mut slot.0, &record.value)
                        .map_err(|e| ctx.op_error(e))?;
                    dirty.insert((window, key.clone()));
                }
            }
            Input::Barrier(barrier) => {
                ctx.meter.busy();
                let mut windows: BTreeMap<Window, Vec<StateEntry>> = BTreeMap::new();
                for (window, key) in std::mem::take(&mut dirty) {
                    if let Some((acc, last_epoch)) = state.get_mut(&(window, key.clone())) {
                        let bytes = aggregator.encode(acc).map_err(|e| ctx.op_error(e))?;
                        windows.entry(window).or_default().push(StateEntry {
                            key,
                            acc: bytes,
                            prev_epoch: *last_epoch,
                        });
                        *last_epoch = barrier.epoch;
                    }
                }
                let changes = NodeChanges {
                    node: ctx.node.clone(),
                    windows: windows
                        .into_iter()
                        .map(|(window, entries)| WindowChanges { window, entries })
                        .collect(),
                };
                let _ = commits.send(Contribution::State {
                    epoch: barrier.epoch,
                    changes,
                });
                watermark = watermark.max(barrier.watermark);
                let mut done: BTreeSet<Window> = BTreeSet::new();
                state.retain(|(window, _), _| {
                    let open = !is_closed(window, lateness_ms, watermark);
                    if !open {
                        done.insert(*window);
                    }
                    open
                });
                if !done.is_empty() {
                    closed.lock().extend(done);
                }
            }
        }
    }
}

async fn run_sink<P: Providers>(
    mut ctx: ActorCtx<P>,
    sink: Arc<dyn SinkConnector>,
    mut inputs: Inputs,
    commits: mpsc::UnboundedSender<Contribution>,
) -> Result<(), ActorError> {
    loop {
        ctx.wait_unpaused().await;
        ctx.meter.idle();
        match inputs.next().await {
            Input::Records(records) => {
                ctx.meter.busy();
                ctx.meter.records_in(records.len());
                let batch: Vec<Emit> = records
                    .iter()
                    .map(|record| {
                        let origin = record.origin.derive(ctx.node_hash, 0);
                        Emit {
                            id: EmitId::new(&ctx.key.slice, &origin),
                            origin,
                            ts: record.ts,
                            key: record.key.clone(),
                            value: record.value.clone(),
                        }
                    })
                    .collect();
                let mut backoff = MIN_BACKOFF;
                // The same elements with the same ids are retried until the sink
                // takes them: at-least-once delivery of idempotent writes.
                while let Err(error) = sink.write(&batch).await {
                    ctx.meter.metrics.error(error.to_string());
                    ctx.sleep(bump(&mut backoff)).await;
                }
                ctx.meter.records_out(batch.len());
            }
            Input::Barrier(barrier) => {
                let _ = commits.send(Contribution::Ack {
                    epoch: barrier.epoch,
                });
            }
        }
    }
}

/// State shared by the two kinds of root actors: epoch numbering, in-flight
/// accounting and reaction to the control plane.
struct Root<P: Providers> {
    ctx: ActorCtx<P>,
    outputs: Outputs,
    commits: mpsc::UnboundedSender<Contribution>,
    committed: watch::Receiver<u64>,
    in_flight: VecDeque<(u64, Option<Position>)>,
    epoch: u64,
    slice: Arc<SliceMetrics>,
    exit: Exit,
    /// Whether the slice holds an aggregate: only then is it worth committing
    /// an epoch whose sole content is a watermark advance.
    stateful: bool,
    /// Token identifying this instance in the commit log.
    owner: String,
    /// Head epoch of the log when this instance started.
    start_epoch: u64,
    /// One per aggregate of the slice.
    gates: Vec<Gate>,
}

impl<P: Providers> Root<P> {
    /// Forget the epochs that got committed; returns the last position they covered.
    fn settle(&mut self) -> Option<Position> {
        let committed = *self.committed.borrow_and_update();
        let mut position = None;
        while self
            .in_flight
            .front()
            .is_some_and(|(epoch, _)| *epoch <= committed)
        {
            if let Some((_, Some(covered))) = self.in_flight.pop_front() {
                position = Some(covered);
            }
        }
        self.slice
            .in_flight
            .store(self.in_flight.len() as u32, Ordering::Relaxed);
        position
    }

    async fn wait_commit(&mut self) {
        if self.committed.changed().await.is_err() {
            futures::future::pending::<()>().await;
        }
    }

    /// Whether a new epoch may start now. `false` means something changed
    /// (a commit landed, the control plane spoke): go round the loop again.
    async fn ready(&mut self) -> bool {
        let control = self.ctx.control.borrow_and_update().clone();
        match control.run {
            RunState::Draining | RunState::Stopped => {
                if self.in_flight.is_empty() {
                    self.ctx.meter.metrics.set_state(ActorState::Stopped);
                    self.exit.finish(SliceExit::Drained);
                    return futures::future::pending().await;
                }
                self.ctx.meter.metrics.set_state(ActorState::Draining);
                self.ctx.meter.blocked();
                self.wait_commit().await;
                return false;
            }
            RunState::Paused => {}
            RunState::Running => {
                if !control.paused_nodes.contains(&self.ctx.node) {
                    self.ctx.meter.metrics.set_state(ActorState::Running);
                    if self.in_flight.len() >= self.ctx.settings.max_in_flight_epochs.max(1) {
                        self.ctx.meter.blocked();
                        self.wait_commit().await;
                        self.ctx.meter.busy();
                        return false;
                    }
                    if !self.throttled().await {
                        return true;
                    }
                    // Emission is too far behind: leave it time to catch up.
                    self.ctx.meter.blocked();
                    self.ctx.sleep(self.ctx.settings.flush_interval).await;
                    self.ctx.meter.busy();
                    return false;
                }
            }
        }
        self.ctx.meter.metrics.set_state(ActorState::Paused);
        self.ctx.meter.idle();
        moonpool_core::select! {
            changed = self.ctx.control.changed() => {
                if changed.is_err() {
                    futures::future::pending::<()>().await;
                }
            }
            changed = self.committed.changed() => {
                if changed.is_err() {
                    futures::future::pending::<()>().await;
                }
            }
        }
        false
    }

    /// Whether emission has fallen too far behind this slice: for one of its
    /// aggregates, more windows are closed and not emitted than
    /// `max_pending_windows`. Backpressure cannot travel through an aggregate
    /// the way it does along an edge, since closed windows wait in the store;
    /// this is what carries it across.
    ///
    /// Costs nothing while the slice is within the limit: the emitter is only
    /// asked where it is once the limit is passed.
    async fn throttled(&mut self) -> bool {
        let limit = self.ctx.settings.max_pending_windows;
        if limit == 0 {
            return false;
        }
        let mut behind = false;
        for gate in &self.gates {
            if gate.closed.lock().len() <= limit {
                continue;
            }
            let kv = self.ctx.env.kv.as_ref();
            let Ok(log) = self
                .ctx
                .persist(|| state::read_log(kv, &gate.emitter))
                .await
            else {
                continue;
            };
            let upto = log.head().and_then(|head| head.position);
            let left = {
                let mut closed = gate.closed.lock();
                closed.retain(|window| !emitted(window, upto));
                closed.len()
            };
            behind |= left > limit;
        }
        if behind {
            self.slice.throttled.fetch_add(1, Ordering::Relaxed);
        }
        behind
    }

    /// Emit one epoch: its header to the commit actor, its records and its
    /// barrier downstream.
    async fn emit(
        &mut self,
        records: Vec<Record>,
        position: Option<Position>,
        watermark: Timestamp,
    ) {
        self.epoch += 1;
        let epoch = self.epoch;
        let _ = self.commits.send(Contribution::Header {
            epoch,
            position,
            watermark,
        });
        self.in_flight.push_back((epoch, position));
        self.slice
            .in_flight
            .store(self.in_flight.len() as u32, Ordering::Relaxed);
        self.ctx.meter.records_out(records.len());
        let chunk = self.ctx.settings.chunk_size.max(1);
        let mut records = records;
        while !records.is_empty() {
            let rest = records.split_off(chunk.min(records.len()));
            self.outputs
                .send(
                    &self.ctx.meter,
                    Msg::Records(std::mem::replace(&mut records, rest)),
                )
                .await;
        }
        self.outputs
            .send(&self.ctx.meter, Msg::Barrier(Barrier { epoch, watermark }))
            .await;
    }
}

struct Start {
    epoch: u64,
    position: Option<Position>,
    watermark: Timestamp,
    /// Generation of the plan the head of the log was written under.
    plan: u64,
}

async fn run_source<P: Providers>(
    mut root: Root<P>,
    connector: Arc<dyn SourceConnector>,
    partition: u32,
    start: Start,
) -> Result<(), ActorError> {
    let settings = root.ctx.settings.clone();
    let out_of_orderness = settings.max_out_of_orderness.as_millis() as i64;
    let idle_epoch_interval = settings.flush_interval.max(Duration::from_millis(100));
    let mut reader: Option<Box<dyn PartitionReader>> = None;
    let mut position = start.position;
    let mut watermark = start.watermark;
    let mut max_ts = Timestamp::MIN;
    let mut idle_since: Option<Duration> = None;
    let mut last_idle_epoch: Option<Duration> = None;
    let mut backoff = MIN_BACKOFF;
    loop {
        let acked = root.settle();
        if let (Some(upto), Some(reader)) = (acked, reader.as_mut())
            && let Err(error) = reader.ack(upto).await
        {
            root.ctx.meter.metrics.error(error.to_string());
        }
        if !root.ready().await {
            continue;
        }
        let Some(open) = reader.as_mut() else {
            root.ctx.meter.busy();
            match connector.open(partition, position).await {
                Ok(opened) => reader = Some(opened),
                Err(error) => {
                    root.ctx.meter.metrics.error(error.to_string());
                    root.ctx.meter.idle();
                    root.ctx.sleep(bump(&mut backoff)).await;
                }
            }
            continue;
        };
        root.ctx.meter.idle();
        let polled = open.poll(settings.max_batch.max(1), settings.linger).await;
        root.ctx.meter.busy();
        let mut polled = match polled {
            Ok(polled) => {
                backoff = MIN_BACKOFF;
                polled
            }
            Err(error) => {
                root.ctx.meter.metrics.error(error.to_string());
                reader = None;
                root.ctx.meter.idle();
                root.ctx.sleep(bump(&mut backoff)).await;
                continue;
            }
        };
        // Redeliveries: keep only what lies strictly after what was already read.
        let mut last = position;
        polled.retain(|record| {
            let fresh = last.is_none_or(|seen| record.position > seen);
            if fresh {
                last = Some(record.position);
            }
            fresh
        });

        // One epoch costs one commit: once a first record is in, keep reading
        // for up to `linger` so that the epoch is worth it.
        let mut lost_reader = false;
        if !polled.is_empty() {
            let time = root.ctx.env.providers.time().clone();
            let max_batch = settings.max_batch.max(1);
            let deadline = time.now() + settings.linger;
            root.ctx.meter.idle();
            while polled.len() < max_batch {
                let now = time.now();
                if now >= deadline {
                    break;
                }
                match open.poll(max_batch - polled.len(), deadline - now).await {
                    Ok(more) => {
                        for record in more {
                            if last.is_none_or(|seen| record.position > seen) {
                                last = Some(record.position);
                                polled.push(record);
                            }
                        }
                    }
                    Err(error) => {
                        // What was read is good; the reader is reopened afterwards.
                        root.ctx.meter.metrics.error(error.to_string());
                        lost_reader = true;
                        break;
                    }
                }
            }
            root.ctx.meter.busy();
        }

        let mut new_watermark = watermark;
        if polled.is_empty() {
            let now = root.ctx.env.providers.time().now();
            let since = *idle_since.get_or_insert(now);
            let due =
                last_idle_epoch.is_none_or(|at| now.saturating_sub(at) >= idle_epoch_interval);
            if root.stateful && now.saturating_sub(since) >= settings.idle_timeout && due {
                match open.end_position().await {
                    Ok(end) => {
                        let caught_up = match (end, position) {
                            (None, _) => true,
                            (Some(end), Some(read)) => read >= end,
                            (Some(_), None) => false,
                        };
                        if caught_up {
                            // Nothing left to read and nothing arrived for a while: let
                            // event time follow the wall clock so windows can close.
                            new_watermark = watermark
                                .max(root.ctx.env.clock.now_ms().saturating_sub(out_of_orderness));
                        }
                    }
                    Err(error) => root.ctx.meter.metrics.error(error.to_string()),
                }
            }
            if new_watermark == watermark {
                continue;
            }
            last_idle_epoch = Some(now);
        } else {
            idle_since = None;
            root.ctx.meter.records_in(polled.len());
            for record in &polled {
                max_ts = max_ts.max(record.event_time);
            }
            new_watermark = watermark.max(max_ts.saturating_sub(out_of_orderness));
        }

        let records: Vec<Record> = polled
            .into_iter()
            .map(|record| Record {
                value: record.value,
                ts: record.event_time,
                key: None,
                window: None,
                origin: Origin::source(record.position),
            })
            .collect();
        position = last;
        watermark = new_watermark;
        if lost_reader {
            reader = None;
        }
        root.emit(records, position, watermark).await;
        if planted(Mutant::AckBeforeCommit)
            && let (Some(upto), Some(reader)) = (position, reader.as_mut())
        {
            let _ = reader.ack(upto).await;
        }
    }
}

async fn run_emitter<P: Providers>(
    mut root: Root<P>,
    aggregator: Arc<dyn DynAggregator>,
    lateness_ms: i64,
    feeding: Vec<SliceKey>,
    active: usize,
    generation: u64,
    start: Start,
) -> Result<(), ActorError> {
    let kv = root.ctx.env.kv.clone();
    let node = root.ctx.node.clone();
    let flush_interval = root.ctx.settings.flush_interval;
    let max_batch = root.ctx.settings.max_batch.max(1);
    // Position of the last emitted element: (window end, window start, index),
    // the index being `u64::MAX` once the window is complete.
    let mut position = start.position;
    let mut committed = start.position;
    // Watermark of what this slice emits: results carry the timestamp
    // `window end - 1`, and windows are emitted in closing order.
    let mut watermark = start.watermark;
    let mut admitted = start.plan >= generation;
    let grace_ms = root.ctx.settings.gc_grace.as_millis() as i64;
    // Emitted windows whose state this instance has deleted already, per
    // feeding slice: no need to delete it at every round.
    let mut released: Vec<BTreeSet<Window>> = vec![BTreeSet::new(); feeding.len()];
    loop {
        if let Some(covered) = root.settle() {
            committed = Some(covered);
        }
        if !root.ready().await {
            continue;
        }
        root.ctx.meter.busy();
        if !admitted {
            // The log does not say yet that an emitter of this plan runs:
            // say so before anything else. Slices new to the plan wait for
            // it, and emitters of an older plan are fenced by it.
            root.emit(Vec::new(), position, watermark).await;
            admitted = true;
            continue;
        }

        // Logs first: they give, in one read per slice, the watermark and the
        // commits that are not copied out yet.
        let mut logs = Vec::with_capacity(feeding.len());
        let mut global = Timestamp::MAX;
        for (index, slice) in feeding.iter().enumerate() {
            let log = root
                .ctx
                .persist(|| state::read_log(kv.as_ref(), slice))
                .await?;
            // A retired slice has stopped for good: its windows are waited
            // for, its watermark is not.
            if index < active {
                global = global.min(log.head().map_or(NO_WATERMARK, |head| head.watermark));
            }
            logs.push(log);
        }
        let mut indexed = Vec::with_capacity(feeding.len());
        for slice in &feeding {
            let found = root
                .ctx
                .persist(|| state::indexed_windows(kv.as_ref(), slice, &node))
                .await?;
            indexed.push(found);
        }
        let read_at = root.ctx.env.clock.now_ms();
        let mut windows: BTreeSet<Window> = BTreeSet::new();
        for (log, found) in logs.iter().zip(&indexed) {
            windows.extend(found.keys().copied());
            windows.extend(state::pending_windows(&log.records, &node));
        }

        // Windows whose emission is committed are garbage. Their state goes at
        // once; their index entry stays until no commit record has carried a
        // change to them for `gc_grace`, so that a write issued meanwhile by
        // an instance that lost the slice is deleted too (see `state`).
        for (((slice, log), found), released) in
            feeding.iter().zip(&logs).zip(&indexed).zip(&mut released)
        {
            let changing = state::pending_windows(&log.records, &node);
            released.retain(|window| found.contains_key(window));
            let mut release = Vec::new();
            let mut quiet = Vec::new();
            let mut forget = Vec::new();
            for (window, since) in found
                .iter()
                .filter(|(window, _)| emitted(window, committed))
            {
                if !released.contains(window) {
                    release.push(*window);
                }
                match since {
                    _ if planted(Mutant::ForgetAtOnce) => forget.push(*window),
                    _ if changing.contains(window) => {}
                    None => quiet.push(*window),
                    Some(since) if read_at.saturating_sub(*since) >= grace_ms => {
                        forget.push(*window);
                    }
                    Some(_) => {}
                }
            }
            let outcome = async {
                state::delete_windows(kv.as_ref(), slice, &node, &release).await?;
                released.extend(release.iter().copied());
                // The countdown starts once the state is known to be deleted.
                quiet.retain(|window| released.contains(window));
                state::mark_quiet(kv.as_ref(), slice, &node, &quiet, read_at).await?;
                if forget.is_empty() {
                    return Ok(());
                }
                // State first: stopped between the two, the entries are still
                // there for whoever comes next.
                state::delete_windows(kv.as_ref(), slice, &node, &forget).await?;
                state::unindex_windows(kv.as_ref(), slice, &node, &forget).await
            };
            if let Err::<(), KvError>(error) = outcome.await {
                root.ctx.meter.metrics.error(error.to_string());
            }
        }

        let ready: Vec<Window> = windows
            .into_iter()
            .filter(|window| !feeding.is_empty() && is_closed(window, lateness_ms, global))
            .filter(|window| !emitted(window, position))
            .collect();
        if ready.is_empty() {
            // Every closed window is out. Whatever comes next belongs to a window
            // still open upstream, so its timestamp lies beyond the upstream
            // watermark: let the output watermark follow, so that windows
            // downstream of this node can close even when nothing new arrives.
            let reached = if feeding.is_empty() || global == NO_WATERMARK {
                NO_WATERMARK
            } else {
                global.saturating_sub(lateness_ms).saturating_sub(1)
            };
            if root.stateful && reached > watermark {
                watermark = reached;
                root.emit(Vec::new(), position, watermark).await;
            }
            root.ctx.meter.idle();
            root.ctx.sleep(flush_interval).await;
            continue;
        }

        for window in ready {
            // A closed window is frozen: every feeding slice has a committed
            // watermark past its end, so no later commit can change it.
            let mut merged: BTreeMap<Key, DynAcc> = BTreeMap::new();
            for (slice, log) in feeding.iter().zip(&logs) {
                let partial = root
                    .ctx
                    .persist(|| {
                        state::read_window(kv.as_ref(), slice, &node, &window, &log.records)
                    })
                    .await?;
                for (key, (bytes, _)) in partial {
                    let acc = aggregator
                        .decode(&bytes)
                        .map_err(|e| root.ctx.op_error(e))?;
                    match merged.get_mut(&key) {
                        Some(existing) => aggregator
                            .merge(existing, &acc)
                            .map_err(|e| root.ctx.op_error(e))?,
                        None => {
                            merged.insert(key, acc);
                        }
                    }
                }
            }
            // The accumulators just read are only trustworthy if nobody else
            // emitted in our place meanwhile: another instance committing this
            // window would then delete the very state we were reading. Its
            // commit comes first, so checking the log after the read settles it.
            let own = root
                .ctx
                .persist(|| state::read_log(kv.as_ref(), &root.ctx.key))
                .await?;
            let ours = match own.head() {
                None => root.start_epoch == 0,
                Some(head) => head.owner == root.owner || head.epoch == root.start_epoch,
            };
            if !ours && !planted(Mutant::EmitWithoutOwnership) {
                root.exit.finish(SliceExit::Fenced);
                return futures::future::pending().await;
            }
            let window_position = (ts_key(window.end), ts_key(window.start));
            let resume = match position {
                Some(at) if (at.0, at.1) == window_position => at.2 + 1,
                _ => 0,
            };
            let mut results = Vec::with_capacity(merged.len());
            for (index, (key, acc)) in merged.iter().enumerate() {
                if (index as u64) < resume {
                    continue;
                }
                let value = aggregator
                    .finish(acc, window, key)
                    .map_err(|e| root.ctx.op_error(e))?;
                results.push((
                    index as u64,
                    Record {
                        value,
                        ts: window.end - 1,
                        key: Some(key.clone()),
                        window: Some(window),
                        origin: Origin::source(Position(
                            window_position.0,
                            window_position.1,
                            index as u64,
                        )),
                    },
                ));
            }
            root.ctx.meter.records_in(results.len());
            let mut results = results.into_iter().peekable();
            loop {
                let mut batch = Vec::with_capacity(max_batch.min(1024));
                let mut last_index = 0;
                while batch.len() < max_batch {
                    let Some((index, record)) = results.next() else {
                        break;
                    };
                    last_index = index;
                    batch.push(record);
                }
                let complete = results.peek().is_none();
                let at = Position(
                    window_position.0,
                    window_position.1,
                    if complete { u64::MAX } else { last_index },
                );
                // Wait for room before each epoch, still honouring pause and drain.
                loop {
                    if let Some(covered) = root.settle() {
                        committed = Some(covered);
                    }
                    if root.ready().await {
                        break;
                    }
                }
                root.ctx.meter.busy();
                // Not `window.end`: the rest of this window, or another window
                // ending at the same instant, may still follow.
                watermark = watermark.max(window.end - 1);
                root.emit(batch, Some(at), watermark).await;
                position = Some(at);
                if complete {
                    break;
                }
            }
        }
    }
}

struct Pending {
    header: Option<(Option<Position>, Timestamp)>,
    reports: usize,
    changes: Vec<NodeChanges>,
}

/// Copy the accumulators of `records` into the window hashes, oldest record
/// first. `false` means another instance took the slice over: nothing more may
/// be written.
///
/// A write is only safe while the record it comes from still holds its payload
/// in the log: until then the emitter keeps the window indexed (see `state`).
/// `proven` is the last instant at which that was known to be true; a quarter
/// of `gc_grace` later, the log is read again before going on.
async fn copy_out<P: Providers>(
    ctx: &ActorCtx<P>,
    head: u64,
    owner: &str,
    records: &[&CommitRecord],
    proven: &mut Duration,
) -> Result<bool, ActorError> {
    let kv = ctx.env.kv.as_ref();
    let validity = ctx.settings.gc_grace / 4;
    // Inherited records that somebody else finished and emptied meanwhile.
    let mut gone: BTreeSet<u64> = BTreeSet::new();
    let mut backoff = MIN_BACKOFF;
    for record in records {
        for node in &record.changes {
            for changes in &node.windows {
                while !gone.contains(&record.epoch) {
                    if ctx.now().saturating_sub(*proven) > validity {
                        let asked = ctx.now();
                        let view = ctx.persist(|| state::read_log(kv, &ctx.key)).await?;
                        let ours = view
                            .head()
                            .is_some_and(|at| at.epoch == head && at.owner == owner);
                        if !ours {
                            return Ok(false);
                        }
                        let present: BTreeSet<u64> =
                            view.records.iter().map(|record| record.epoch).collect();
                        gone.extend(
                            records
                                .iter()
                                .map(|record| record.epoch)
                                .filter(|epoch| !present.contains(epoch)),
                        );
                        *proven = asked;
                        continue;
                    }
                    match state::materialize_window(kv, &ctx.key, &node.node, changes, record.epoch)
                        .await
                    {
                        Ok(()) => {
                            backoff = MIN_BACKOFF;
                            break;
                        }
                        Err(error) if error.transient() => {
                            ctx.meter.metrics.error(error.to_string());
                            ctx.sleep(bump(&mut backoff)).await;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }
        // An inherited record is emptied as soon as it is copied out, not
        // once all of them are: an instance that does not live long enough
        // to finish still leaves less to do to the next one. Best effort.
        if record.epoch < head && !gone.contains(&record.epoch) {
            let _ = state::tombstone(kv, &ctx.key, &[record.epoch]).await;
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn run_committer<P: Providers>(
    ctx: ActorCtx<P>,
    terminals: usize,
    mut contributions: mpsc::UnboundedReceiver<Contribution>,
    committed: watch::Sender<u64>,
    start_epoch: u64,
    owner: String,
    plan: u64,
    slice: Arc<SliceMetrics>,
    exit: Exit,
) -> Result<(), ActorError> {
    let kv = ctx.env.kv.clone();
    let mut next = start_epoch + 1;
    let mut pending: BTreeMap<u64, Pending> = BTreeMap::new();
    // What this instance knows of the log, from the read that follows its
    // first commit: the epochs present above the seal, and the seal.
    let mut taken_over = false;
    let mut live: Vec<u64> = Vec::new();
    let mut floor = 0;
    // Records copied out whose payload is still to be emptied.
    let mut heavy: Vec<u64> = Vec::new();
    ctx.meter.metrics.set_state(ActorState::Running);
    loop {
        ctx.meter.idle();
        let Some(contribution) = contributions.recv().await else {
            return futures::future::pending().await;
        };
        ctx.meter.busy();
        let (epoch, header, changes) = match contribution {
            Contribution::Header {
                epoch,
                position,
                watermark,
            } => (epoch, Some((position, watermark)), None),
            Contribution::State { epoch, changes } => (epoch, None, Some(changes)),
            Contribution::Ack { epoch } => (epoch, None, None),
        };
        let entry = pending.entry(epoch).or_insert(Pending {
            header: None,
            reports: 0,
            changes: Vec::new(),
        });
        match header {
            Some(header) => entry.header = Some(header),
            None => {
                entry.reports += 1;
                entry
                    .changes
                    .extend(changes.filter(|c| !c.windows.is_empty()));
            }
        }

        while let Some(ready) = pending
            .get(&next)
            .filter(|p| p.reports == terminals)
            .and_then(|p| p.header)
        {
            let mut changes = pending.remove(&next).map(|p| p.changes).unwrap_or_default();
            changes.sort_by(|a, b| a.node.cmp(&b.node));
            let record = CommitRecord {
                epoch: next,
                owner: owner.clone(),
                position: ready.0,
                watermark: ready.1,
                changes,
                plan,
            };
            let started = ctx.now();
            let mut backoff = MIN_BACKOFF;
            // The last instant at which the payloads this instance copies out
            // were known to be in the log.
            let mut proven;
            let outcome = loop {
                proven = ctx.now();
                match state::try_commit(kv.as_ref(), &ctx.key, &record).await {
                    Ok(outcome) => break outcome,
                    Err(error) if error.transient() => {
                        // Outcome unknown: the next attempt recognises our own record.
                        ctx.meter.metrics.error(error.to_string());
                        ctx.sleep(bump(&mut backoff)).await;
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            if outcome == CommitOutcome::Fenced {
                exit.finish(SliceExit::Fenced);
                return futures::future::pending().await;
            }

            // The first commit of an instance makes the slice its own. What
            // earlier instances committed without copying it out is ours to
            // finish, as of a read that comes after our commit.
            let mut inherited: Vec<CommitRecord> = Vec::new();
            if taken_over {
                live.push(next);
            } else {
                let asked = ctx.now();
                let view = ctx
                    .persist(|| state::read_log(kv.as_ref(), &ctx.key))
                    .await?;
                let ours = view
                    .head()
                    .is_some_and(|head| head.epoch == next && head.owner == owner);
                if !ours {
                    exit.finish(SliceExit::Fenced);
                    return futures::future::pending().await;
                }
                proven = asked;
                // What sits at or below the seal is final or void.
                let _ = state::sweep(kv.as_ref(), &ctx.key, &view.sealed).await;
                live = view.live;
                floor = view.floor;
                inherited = view
                    .records
                    .into_iter()
                    .filter(|record| record.epoch < next && !record.changes.is_empty())
                    .collect();
                taken_over = true;
            }
            let mut work: Vec<&CommitRecord> = if planted(Mutant::EmptyWithoutCopy) {
                Vec::new()
            } else {
                inherited.iter().collect()
            };
            work.push(&record);
            if !copy_out(&ctx, next, &owner, &work, &mut proven).await? {
                exit.finish(SliceExit::Fenced);
                return futures::future::pending().await;
            }
            // Everything below this epoch is copied out: empty it. The fields
            // stay, so that a worker left behind cannot take those epochs.
            // Best effort: what is not emptied now is at the next commit.
            heavy.extend(inherited.iter().map(|record| record.epoch));
            if state::tombstone(kv.as_ref(), &ctx.key, &heavy)
                .await
                .is_ok()
            {
                heavy.clear();
            }
            if !record.changes.is_empty() {
                heavy.push(next);
            }
            if live.len() > COMPACT_AT {
                // Seal what is copied out, then delete the fields below the seal.
                let sealed = next - 1;
                if state::compact(kv.as_ref(), &ctx.key, sealed, &live, floor)
                    .await
                    .is_ok()
                {
                    live.retain(|epoch| *epoch > sealed);
                    heavy.retain(|epoch| *epoch > sealed);
                    floor = sealed;
                }
            }

            let elapsed = ctx.now().saturating_sub(started);
            slice.epoch.store(next, Ordering::Relaxed);
            if let Some(position) = record.position {
                *slice.position.lock() = Some(position);
            }
            slice.watermark.store(record.watermark, Ordering::Relaxed);
            slice.commits.fetch_add(1, Ordering::Relaxed);
            slice
                .commit_ns
                .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
            ctx.meter.records_in(1);
            ctx.meter.records_out(1);
            let _ = committed.send(next);
            next += 1;
        }
    }
}

struct Prepared {
    ops: BTreeMap<NodeId, NodeOp>,
    start: Start,
    restored: BTreeMap<NodeId, AggState>,
    /// Per aggregate, the windows found closed: emitted or not, the root
    /// finds out when it matters.
    closed: BTreeMap<NodeId, BTreeSet<Window>>,
}

/// Number of fields a log may hold before it is sealed and swept.
const COMPACT_AT: usize = 24;

/// Let a slice that never committed join a pipeline that may have been
/// running without it, as the slice of a partition added to a source does.
/// Returns the watermark it starts from.
///
/// Emitters of the previous plan ignore this slice: a window they emit lacks
/// what the slice would add to it, and an emitter of the new plan may emit the
/// same window again, under the same ids, with it. So the slice first waits
/// for an emitter of its own plan to have committed, which fences the others;
/// then it starts from the lowest watermark of the slices already feeding the
/// aggregate. Every window those emitters may have written is closed below
/// that watermark, and what the slice reads for such windows is late.
async fn join<P: Providers>(
    params: &SliceParams<P>,
    ops: &BTreeMap<NodeId, NodeOp>,
    note: &impl Fn(String),
) -> Result<Timestamp, ActorError> {
    let env = &params.env;
    let kv = env.kv.as_ref();
    let generation = params.plan.generation;
    let key_of = |slice: SliceId| SliceKey {
        tenant: params.key.tenant.clone(),
        graph: params.key.graph.clone(),
        slice,
    };
    let mut watermark = NO_WATERMARK;
    for (node, op) in ops {
        let NodeOp::Aggregate(..) = op else {
            continue;
        };
        let emitter = key_of(SliceId::Emitter { node: node.clone() });
        loop {
            let log = persist(env, note, || state::read_log(kv, &emitter)).await?;
            if log.head().is_some_and(|head| head.plan >= generation) {
                break;
            }
            let pause = params.topology.spec.settings.flush_interval;
            let _ = env.providers.time().sleep(pause).await;
        }
        // Slices without a commit are joining as well: they added nothing to
        // any window yet.
        let mut others: Option<Timestamp> = None;
        for other in params.topology.feeding_slices(node, &params.plan.slices) {
            if other == params.key.slice {
                continue;
            }
            let other = key_of(other);
            let log = persist(env, note, || state::read_log(kv, &other)).await?;
            if let Some(head) = log.head() {
                others = Some(others.map_or(head.watermark, |low| low.min(head.watermark)));
            }
        }
        watermark = watermark.max(others.unwrap_or(NO_WATERMARK));
    }
    Ok(watermark)
}

async fn prepare<P: Providers>(
    params: &SliceParams<P>,
    nodes: &[NodeId],
    metrics: &SliceMetrics,
) -> Result<Prepared, ActorError> {
    let env = &params.env;
    let kv = env.kv.as_ref();
    let root = params.key.slice.root();
    let mut ops = BTreeMap::new();
    for node in nodes {
        ops.insert(
            node.clone(),
            build_op(env, &params.topology, &params.key, node, node == root)?,
        );
    }
    let note = |error: String| *metrics.last_error.lock() = Some(error);

    // Reading only: copying records out is for the instance that owns the
    // slice, and this one does not before its first commit. Commits not copied
    // out yet are laid over what the window hashes hold.
    let log = persist(env, &note, || state::read_log(kv, &params.key)).await?;
    let generation = params.plan.generation;
    if let Some(head) = log.head().filter(|head| head.plan > generation) {
        // The set of slices changed and this worker has not heard of it yet:
        // nothing is written under a plan older than what the log holds.
        return Err(OpError::Failed(format!(
            "the commit log was written under plan {}, this worker still has plan {generation}",
            head.plan
        ))
        .into());
    }
    let mut start = log.head().map_or(
        Start {
            epoch: 0,
            position: None,
            watermark: NO_WATERMARK,
            plan: 0,
        },
        |head| Start {
            epoch: head.epoch,
            plan: head.plan,
            position: head.position,
            watermark: if planted(Mutant::RestartWithoutWatermark) {
                NO_WATERMARK
            } else {
                head.watermark
            },
        },
    );
    // Commits not copied out yet, laid over what the window hashes hold.
    let pending: &[CommitRecord] = if planted(Mutant::RestoreWithoutPending) {
        &[]
    } else {
        &log.records
    };

    if start.epoch == 0 && params.plan.joined.contains_key(&params.key.slice) {
        let joined = join(params, &ops, &note).await?;
        if !planted(Mutant::JoinWithoutWatermark) {
            start.watermark = joined;
        }
    }

    let mut restored = BTreeMap::new();
    let mut closed: BTreeMap<NodeId, BTreeSet<Window>> = BTreeMap::new();
    for (node, op) in &ops {
        let NodeOp::Aggregate(aggregator, lateness_ms) = op else {
            continue;
        };
        let mut state = AggState::new();
        let windows = persist(env, &note, || {
            state::list_windows(kv, &params.key, node, pending)
        })
        .await?;
        for window in windows {
            if is_closed(&window, *lateness_ms, start.watermark) {
                closed.entry(node.clone()).or_default().insert(window);
                continue;
            }
            let entries = persist(env, &note, || {
                state::read_window(kv, &params.key, node, &window, pending)
            })
            .await?;
            for (key, (bytes, epoch)) in entries {
                state.insert((window, key), (aggregator.decode(&bytes)?, epoch));
            }
        }
        restored.insert(node.clone(), state);
    }
    Ok(Prepared {
        ops,
        start,
        restored,
        closed,
    })
}

fn actor_kind(op: &NodeOp) -> ActorKind {
    match op {
        NodeOp::Source(_) => ActorKind::Source,
        NodeOp::Emitter(..) => ActorKind::Emitter,
        NodeOp::Map(_) => ActorKind::Map,
        NodeOp::Key(_) => ActorKind::KeyBy,
        NodeOp::Window(_) => ActorKind::Window,
        NodeOp::Aggregate(..) => ActorKind::Aggregate,
        NodeOp::Sink(_) => ActorKind::Sink,
    }
}

fn spawn_actor<P: Providers>(
    scope: &TaskScope<P>,
    name: String,
    metrics: Arc<ActorMetrics>,
    exit: Exit,
    catch_panics: bool,
    actor: impl Future<Output = Result<(), ActorError>> + Send + 'static,
) -> <P::Task as TaskProvider>::JoinHandle {
    let node = metrics.ident.node.clone();
    scope.spawn(&name, async move {
        let result = if catch_panics {
            AssertUnwindSafe(actor)
                .catch_unwind()
                .await
                .unwrap_or(Err(ActorError::Panicked))
        } else {
            actor.await
        };
        if let Err(error) = result {
            metrics.error(error.to_string());
            metrics.set_state(ActorState::Failed);
            exit.finish(SliceExit::Failed(format!("{node}: {error}")));
        }
    })
}

/// Run one slice instance until it is stopped, drained, fenced or fails.
///
/// `scope` is the slice's own scope: cancelling its token stops every actor at
/// its current await point. The exit reason ends up in `shared`.
pub(crate) async fn run_slice<P: Providers>(
    params: SliceParams<P>,
    scope: TaskScope<P>,
    control: watch::Receiver<Control>,
    shared: SliceShared,
) {
    let exit = Exit {
        slot: shared.exit.clone(),
        stop: scope.token().clone(),
    };
    let nodes = params.topology.slice_nodes(&params.key.slice);
    let prepared = moonpool_core::select! {
        () = scope.token().cancelled() => None,
        prepared = prepare(&params, &nodes, &shared.metrics) => Some(prepared),
    };
    let handles = match prepared {
        None => Vec::new(),
        Some(Err(error)) => {
            *shared.metrics.last_error.lock() = Some(error.to_string());
            exit.finish(SliceExit::Failed(error.to_string()));
            Vec::new()
        }
        Some(Ok(prepared)) => {
            start_actors(&params, &scope, control, &shared, &exit, &nodes, prepared)
        }
    };
    scope.token().cancelled().await;
    for handle in handles {
        let _ = handle.await;
    }
    let exit = shared.exit.lock().get_or_insert(SliceExit::Stopped).clone();
    shared.metrics.set_state(match &exit {
        SliceExit::Failed(error) => {
            *shared.metrics.last_error.lock() = Some(error.clone());
            ActorState::Failed
        }
        SliceExit::Fenced => {
            *shared.metrics.last_error.lock() =
                Some("fenced: another worker committed this slice".to_owned());
            ActorState::Failed
        }
        SliceExit::Stopped | SliceExit::Drained => ActorState::Stopped,
    });
}

fn start_actors<P: Providers>(
    params: &SliceParams<P>,
    scope: &TaskScope<P>,
    control: watch::Receiver<Control>,
    shared: &SliceShared,
    exit: &Exit,
    nodes: &[NodeId],
    prepared: Prepared,
) -> Vec<<P::Task as TaskProvider>::JoinHandle> {
    let env = &params.env;
    let topology = &params.topology;
    let settings = topology.spec.settings.clone();
    let now = env.providers.time().now();
    let root = params.key.slice.root().clone();
    let Prepared {
        mut ops,
        start,
        mut restored,
        mut closed,
    } = prepared;

    let ident = |node: &NodeId, kind: ActorKind| ActorIdent {
        tenant: params.key.tenant.clone(),
        graph: params.key.graph.clone(),
        slice: params.key.slice.clone(),
        node: node.clone(),
        kind,
        op: topology.descriptors[node].op.clone(),
    };
    let metrics: BTreeMap<NodeId, Arc<ActorMetrics>> = nodes
        .iter()
        .map(|node| {
            (
                node.clone(),
                ActorMetrics::new(ident(node, actor_kind(&ops[node])), now),
            )
        })
        .collect();
    let commit_metrics = ActorMetrics::new(ident(&root, ActorKind::Commit), now);
    {
        let mut actors = shared.actors.lock();
        actors.clear();
        actors.extend(metrics.values().cloned());
        actors.push(commit_metrics.clone());
    }

    let mut inputs: BTreeMap<NodeId, Vec<mpsc::Receiver<Msg>>> = BTreeMap::new();
    let mut outputs: BTreeMap<NodeId, Vec<mpsc::Sender<Msg>>> = BTreeMap::new();
    for (from, to) in topology.slice_edges(&params.key.slice) {
        let (tx, rx) = mpsc::channel(settings.mailbox_capacity.max(1));
        metrics[&to].watch_input(&tx);
        outputs.entry(from).or_default().push(tx);
        inputs.entry(to).or_default().push(rx);
    }

    let stateful = nodes
        .iter()
        .any(|node| *node != root && topology.kind(node) == OpKind::Aggregate);
    let (commits_tx, commits_rx) = mpsc::unbounded_channel();
    let (committed_tx, committed_rx) = watch::channel(start.epoch);
    let terminals = nodes
        .iter()
        .filter(|node| **node != root)
        .filter(|node| matches!(topology.kind(node), OpKind::Aggregate | OpKind::Sink))
        .count();
    shared.metrics.epoch.store(start.epoch, Ordering::Relaxed);
    *shared.metrics.position.lock() = start.position;
    shared
        .metrics
        .watermark
        .store(start.watermark, Ordering::Relaxed);

    let ctx = |node: &NodeId, metrics: Arc<ActorMetrics>| ActorCtx {
        env: env.clone(),
        key: params.key.clone(),
        node: node.clone(),
        node_hash: hash_str(node.as_str()),
        meter: Meter::new(&env.providers, metrics),
        control: control.clone(),
        settings: settings.clone(),
    };
    let name = |node: &NodeId, role: &str| format!("slice/{}/{node}{role}", params.key);
    let owner = format!(
        "{}#{:016x}",
        params.worker,
        moonpool_core::RandomProvider::random::<u64>(env.providers.random())
    );

    let mut handles = Vec::with_capacity(nodes.len() + 1);
    handles.push(spawn_actor(
        scope,
        name(&root, "#commit"),
        commit_metrics.clone(),
        exit.clone(),
        params.catch_panics,
        run_committer(
            ctx(&root, commit_metrics),
            terminals,
            commits_rx,
            committed_tx,
            start.epoch,
            owner.clone(),
            params.plan.generation,
            shared.metrics.clone(),
            exit.clone(),
        ),
    ));

    // One gate per aggregate of the slice, shared by its actor and the root.
    let mut gates = Vec::new();
    let mut closed_of: BTreeMap<NodeId, Closed> = BTreeMap::new();
    for node in nodes
        .iter()
        .filter(|node| **node != root && topology.kind(node) == OpKind::Aggregate)
    {
        let set: Closed = Arc::new(Mutex::new(closed.remove(node).unwrap_or_default()));
        gates.push(Gate {
            emitter: SliceKey {
                tenant: params.key.tenant.clone(),
                graph: params.key.graph.clone(),
                slice: SliceId::Emitter { node: node.clone() },
            },
            closed: set.clone(),
        });
        closed_of.insert(node.clone(), set);
    }
    let mut gates = Some(gates);

    let mut start = Some(start);
    for node in nodes {
        let Some(op) = ops.remove(node) else { continue };
        let actor_ctx = ctx(node, metrics[node].clone());
        let node_inputs = Inputs::new(inputs.remove(node).unwrap_or_default());
        let node_outputs = Outputs::new(outputs.remove(node).unwrap_or_default());
        let make_root = |ctx: ActorCtx<P>, outputs: Outputs, epoch: u64, gates: Vec<Gate>| Root {
            ctx,
            outputs,
            commits: commits_tx.clone(),
            committed: committed_rx.clone(),
            in_flight: VecDeque::new(),
            epoch,
            slice: shared.metrics.clone(),
            exit: exit.clone(),
            stateful,
            owner: owner.clone(),
            start_epoch: epoch,
            gates,
        };
        let actor: std::pin::Pin<Box<dyn Future<Output = Result<(), ActorError>> + Send>> = match op
        {
            NodeOp::Source(connector) => {
                let Some(start) = start.take() else { continue };
                let partition = match &params.key.slice {
                    SliceId::Source { partition, .. } => *partition,
                    SliceId::Emitter { .. } => 0,
                };
                Box::pin(run_source(
                    make_root(
                        actor_ctx,
                        node_outputs,
                        start.epoch,
                        gates.take().unwrap_or_default(),
                    ),
                    connector,
                    partition,
                    start,
                ))
            }
            NodeOp::Emitter(aggregator, lateness_ms) => {
                let Some(start) = start.take() else { continue };
                let running = topology.feeding_slices(node, &params.plan.slices);
                let active = running.len();
                let retired = if planted(Mutant::EmitWithoutRetired) {
                    Vec::new()
                } else {
                    topology.feeding_slices(node, &params.plan.retired)
                };
                let feeding = running
                    .into_iter()
                    .chain(retired)
                    .map(|slice| SliceKey {
                        tenant: params.key.tenant.clone(),
                        graph: params.key.graph.clone(),
                        slice,
                    })
                    .collect();
                Box::pin(run_emitter(
                    make_root(
                        actor_ctx,
                        node_outputs,
                        start.epoch,
                        gates.take().unwrap_or_default(),
                    ),
                    aggregator,
                    lateness_ms,
                    feeding,
                    active,
                    params.plan.generation,
                    start,
                ))
            }
            NodeOp::Map(map) => {
                let mut scratch = Vec::new();
                Box::pin(run_transform(
                    actor_ctx,
                    node_inputs,
                    node_outputs,
                    move |record, hash, out| {
                        scratch.clear();
                        map.apply(&record.value, &mut scratch)?;
                        for (index, value) in scratch.drain(..).enumerate() {
                            out.push(Record {
                                value,
                                ts: record.ts,
                                key: record.key.clone(),
                                window: record.window,
                                origin: record.origin.derive(hash, index as u32),
                            });
                        }
                        Ok(())
                    },
                ))
            }
            NodeOp::Key(key) => Box::pin(run_transform(
                actor_ctx,
                node_inputs,
                node_outputs,
                move |record, hash, out| {
                    out.push(Record {
                        key: Some(key.key(&record.value)?),
                        origin: record.origin.derive(hash, 0),
                        ..record.clone()
                    });
                    Ok(())
                },
            )),
            NodeOp::Window(assigner) => {
                let mut windows = Vec::new();
                Box::pin(run_transform(
                    actor_ctx,
                    node_inputs,
                    node_outputs,
                    move |record, hash, out| {
                        windows.clear();
                        assigner.assign(record.ts, &mut windows);
                        for (index, window) in windows.iter().enumerate() {
                            out.push(Record {
                                window: Some(*window),
                                origin: record.origin.derive(hash, index as u32),
                                ..record.clone()
                            });
                        }
                        Ok(())
                    },
                ))
            }
            NodeOp::Aggregate(aggregator, lateness_ms) => {
                let watermark = shared.metrics.watermark.load(Ordering::Relaxed);
                Box::pin(run_aggregate(
                    actor_ctx,
                    aggregator,
                    lateness_ms,
                    node_inputs,
                    commits_tx.clone(),
                    restored.remove(node).unwrap_or_default(),
                    watermark,
                    closed_of.get(node).cloned().unwrap_or_default(),
                ))
            }
            NodeOp::Sink(sink) => {
                Box::pin(run_sink(actor_ctx, sink, node_inputs, commits_tx.clone()))
            }
        };
        handles.push(spawn_actor(
            scope,
            name(node, ""),
            metrics[node].clone(),
            exit.clone(),
            params.catch_panics,
            actor,
        ));
    }
    shared.metrics.set_state(ActorState::Running);
    handles
}
