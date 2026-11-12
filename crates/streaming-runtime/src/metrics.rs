//! Per-actor and per-slice instrumentation.
//!
//! An actor is, at any instant, in exactly one of three phases: waiting for
//! input (*idle*), working (*busy*) or waiting for room downstream
//! (*backpressured*). The time spent in each is what the control plane uses to
//! locate the bottleneck of a graph.

use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU8, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use moonpool_core::{Providers, TimeProvider};
use parking_lot::Mutex;
use streaming_core::{
    GraphId, NodeId, OpRef, Position, SliceId, TenantId, Timestamp,
    control::{ActorKind, ActorSnapshot, ActorState, EmittedWindow, SliceSnapshot},
};
use tokio::sync::mpsc;

use crate::mailbox::Msg;

/// What an actor is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Phase {
    Idle = 0,
    Busy = 1,
    Backpressured = 2,
    /// Working, but on somebody else's time: waiting for the system a
    /// connector talks to (a sink's destination) to answer.
    Waiting = 3,
}

fn state_to_u8(state: ActorState) -> u8 {
    match state {
        ActorState::Starting => 0,
        ActorState::Running => 1,
        ActorState::Paused => 2,
        ActorState::Draining => 3,
        ActorState::Stopped => 4,
        ActorState::Failed => 5,
    }
}

fn state_from_u8(value: u8) -> ActorState {
    match value {
        1 => ActorState::Running,
        2 => ActorState::Paused,
        3 => ActorState::Draining,
        4 => ActorState::Stopped,
        5 => ActorState::Failed,
        _ => ActorState::Starting,
    }
}

/// Identity of an actor.
#[derive(Debug, Clone)]
pub(crate) struct ActorIdent {
    pub tenant: TenantId,
    pub graph: GraphId,
    pub slice: SliceId,
    pub node: NodeId,
    pub kind: ActorKind,
    pub op: OpRef,
}

/// Counters of one actor. Written by the actor, read by the snapshotter.
pub(crate) struct ActorMetrics {
    pub ident: ActorIdent,
    state: AtomicU8,
    pub records_in: AtomicU64,
    pub records_out: AtomicU64,
    pub late_dropped: AtomicU64,
    pub errors: AtomicU64,
    phase_ns: [AtomicU64; 4],
    phase: AtomicU8,
    phase_since_ns: AtomicU64,
    last_error: Mutex<Option<String>>,
    inputs: Mutex<Vec<mpsc::WeakSender<Msg>>>,
}

impl ActorMetrics {
    pub(crate) fn new(ident: ActorIdent, now: Duration) -> Arc<Self> {
        Arc::new(Self {
            ident,
            state: AtomicU8::new(state_to_u8(ActorState::Starting)),
            records_in: AtomicU64::new(0),
            records_out: AtomicU64::new(0),
            late_dropped: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            phase_ns: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            phase: AtomicU8::new(Phase::Idle as u8),
            phase_since_ns: AtomicU64::new(now.as_nanos() as u64),
            last_error: Mutex::new(None),
            inputs: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn watch_input(&self, sender: &mpsc::Sender<Msg>) {
        self.inputs.lock().push(sender.downgrade());
    }

    pub(crate) fn set_state(&self, state: ActorState) {
        self.state.store(state_to_u8(state), Ordering::Relaxed);
    }

    pub(crate) fn state(&self) -> ActorState {
        state_from_u8(self.state.load(Ordering::Relaxed))
    }

    pub(crate) fn error(&self, message: impl Into<String>) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        *self.last_error.lock() = Some(message.into());
    }

    /// Close the current phase at `now` and enter `phase`.
    pub(crate) fn switch(&self, now: Duration, phase: Phase) {
        let now_ns = now.as_nanos() as u64;
        let since = self.phase_since_ns.swap(now_ns, Ordering::Relaxed);
        let previous = self.phase.swap(phase as u8, Ordering::Relaxed);
        self.phase_ns[usize::from(previous.min(3))].fetch_add(now_ns.saturating_sub(since), Ordering::Relaxed);
    }

    /// Cumulative nanoseconds per phase, including the phase in progress.
    fn phase_totals(&self, now: Duration) -> [u64; 4] {
        let mut totals = [
            self.phase_ns[0].load(Ordering::Relaxed),
            self.phase_ns[1].load(Ordering::Relaxed),
            self.phase_ns[2].load(Ordering::Relaxed),
            self.phase_ns[3].load(Ordering::Relaxed),
        ];
        let since = self.phase_since_ns.load(Ordering::Relaxed);
        let current = usize::from(self.phase.load(Ordering::Relaxed).min(3));
        totals[current] += (now.as_nanos() as u64).saturating_sub(since);
        totals
    }

    fn queue(&self) -> (u32, u32) {
        let mut len = 0;
        let mut cap = 0;
        for weak in self.inputs.lock().iter() {
            if let Some(sender) = weak.upgrade() {
                let max = sender.max_capacity();
                cap += max;
                len += max.saturating_sub(sender.capacity());
            }
        }
        (len as u32, cap as u32)
    }
}

/// Phase switching bound to a time provider: what actors hold.
pub(crate) struct Meter<P: Providers> {
    time: P::Time,
    pub metrics: Arc<ActorMetrics>,
}

impl<P: Providers> Meter<P> {
    pub(crate) fn new(providers: &P, metrics: Arc<ActorMetrics>) -> Self {
        Self {
            time: providers.time().clone(),
            metrics,
        }
    }

    pub(crate) fn idle(&self) {
        self.metrics.switch(self.time.now(), Phase::Idle);
    }

    pub(crate) fn busy(&self) {
        self.metrics.switch(self.time.now(), Phase::Busy);
    }

    pub(crate) fn blocked(&self) {
        self.metrics.switch(self.time.now(), Phase::Backpressured);
    }

    /// Entering a call to the system a connector talks to.
    pub(crate) fn waiting(&self) {
        self.metrics.switch(self.time.now(), Phase::Waiting);
    }

    pub(crate) fn records_in(&self, count: usize) {
        self.metrics.records_in.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub(crate) fn records_out(&self, count: usize) {
        self.metrics.records_out.fetch_add(count as u64, Ordering::Relaxed);
    }
}

/// Progress of one slice instance.
pub(crate) struct SliceMetrics {
    state: AtomicU8,
    pub epoch: AtomicU64,
    pub position: Mutex<Option<Position>>,
    pub watermark: AtomicI64,
    pub commits: AtomicU64,
    pub commit_ns: AtomicU64,
    pub in_flight: AtomicU32,
    pub throttled: AtomicU64,
    pub last_error: Mutex<Option<String>>,
}

impl SliceMetrics {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicU8::new(state_to_u8(ActorState::Starting)),
            epoch: AtomicU64::new(0),
            position: Mutex::new(None),
            watermark: AtomicI64::new(i64::MIN),
            commits: AtomicU64::new(0),
            commit_ns: AtomicU64::new(0),
            in_flight: AtomicU32::new(0),
            throttled: AtomicU64::new(0),
            last_error: Mutex::new(None),
        })
    }

    pub(crate) fn set_state(&self, state: ActorState) {
        self.state.store(state_to_u8(state), Ordering::Relaxed);
    }

    pub(crate) fn state(&self) -> ActorState {
        state_from_u8(self.state.load(Ordering::Relaxed))
    }
}

/// Order-preserving mapping of a timestamp onto an unsigned position component.
pub(crate) fn ts_key(ts: Timestamp) -> u64 {
    (ts as u64) ^ (1 << 63)
}

/// Inverse of [`ts_key`].
pub(crate) fn ts_unkey(key: u64) -> Timestamp {
    (key ^ (1 << 63)) as Timestamp
}

/// What the snapshotter remembers about an actor between two snapshots.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ActorSample {
    phases: [u64; 4],
    records_in: u64,
    records_out: u64,
    in_per_sec: f64,
    out_per_sec: f64,
}

/// Weight of the latest interval in the published rates. Actors work in
/// bursts (an epoch at a time), so raw per-interval rates jump around.
const RATE_SMOOTHING: f64 = 0.5;

/// Build the snapshot of an actor and the sample to diff against next time.
pub(crate) fn snapshot_actor(
    metrics: &ActorMetrics,
    previous: Option<ActorSample>,
    now: Duration,
    interval: Duration,
) -> (ActorSnapshot, ActorSample) {
    let mut sample = ActorSample {
        phases: metrics.phase_totals(now),
        records_in: metrics.records_in.load(Ordering::Relaxed),
        records_out: metrics.records_out.load(Ordering::Relaxed),
        in_per_sec: 0.0,
        out_per_sec: 0.0,
    };
    let first = previous.is_none();
    let previous = previous.unwrap_or_default();
    let delta = |index: usize| sample.phases[index].saturating_sub(previous.phases[index]) as f64;
    let (idle, working, blocked, waiting) = (delta(0), delta(1), delta(2), delta(3));
    // Waiting for a destination keeps the actor as occupied as computing
    // does: both make it the one others wait for. They are told apart below.
    let busy = working + waiting;
    let total = idle + busy + blocked;
    let ratio = |value: f64, fallback: f64| if total > 0.0 { value / total } else { fallback };
    let seconds = interval.as_secs_f64().max(1e-9);
    let rate = |current: u64, before: u64, smoothed: f64| {
        let instant = current.saturating_sub(before) as f64 / seconds;
        if first {
            instant
        } else {
            RATE_SMOOTHING * instant + (1.0 - RATE_SMOOTHING) * smoothed
        }
    };
    sample.in_per_sec = rate(sample.records_in, previous.records_in, previous.in_per_sec);
    sample.out_per_sec = rate(sample.records_out, previous.records_out, previous.out_per_sec);
    let (queue_len, queue_cap) = metrics.queue();
    let snapshot = ActorSnapshot {
        tenant: metrics.ident.tenant.clone(),
        graph: metrics.ident.graph.clone(),
        slice: metrics.ident.slice.clone(),
        node: metrics.ident.node.clone(),
        kind: metrics.ident.kind,
        op: metrics.ident.op.clone(),
        state: metrics.state(),
        records_in: sample.records_in,
        records_out: sample.records_out,
        in_per_sec: sample.in_per_sec,
        out_per_sec: sample.out_per_sec,
        busy: ratio(busy, 0.0),
        idle: ratio(idle, 1.0),
        backpressured: ratio(blocked, 0.0),
        waiting: ratio(waiting, 0.0),
        queue_len,
        queue_cap,
        late_dropped: metrics.late_dropped.load(Ordering::Relaxed),
        errors: metrics.errors.load(Ordering::Relaxed),
        last_error: metrics.last_error.lock().clone(),
    };
    (snapshot, sample)
}

/// What the snapshotter remembers about a slice between two snapshots.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SliceSample {
    commits: u64,
    commit_ns: u64,
}

/// Counters that outlive the instances of a slice on a worker.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SliceLifetime {
    pub fenced: u64,
    pub restarts: u64,
}

/// Build the snapshot of a slice and the sample to diff against next time.
pub(crate) fn snapshot_slice(
    tenant: &TenantId,
    graph: &GraphId,
    slice: &SliceId,
    metrics: &SliceMetrics,
    lifetime: SliceLifetime,
    previous: Option<SliceSample>,
) -> (SliceSnapshot, SliceSample) {
    let sample = SliceSample {
        commits: metrics.commits.load(Ordering::Relaxed),
        commit_ns: metrics.commit_ns.load(Ordering::Relaxed),
    };
    let previous = previous.unwrap_or_default();
    let commits = sample.commits.saturating_sub(previous.commits);
    let commit_ms = if commits > 0 {
        sample.commit_ns.saturating_sub(previous.commit_ns) as f64 / commits as f64 / 1e6
    } else {
        0.0
    };
    let watermark: Timestamp = metrics.watermark.load(Ordering::Relaxed);
    let position = *metrics.position.lock();
    let window = match (slice, position) {
        (SliceId::Emitter { .. }, Some(at)) => Some(EmittedWindow {
            start_ms: ts_unkey(at.1),
            end_ms: ts_unkey(at.0),
            complete: at.2 == u64::MAX,
        }),
        _ => None,
    };
    let snapshot = SliceSnapshot {
        tenant: tenant.clone(),
        graph: graph.clone(),
        slice: slice.clone(),
        state: metrics.state(),
        epoch: metrics.epoch.load(Ordering::Relaxed),
        position,
        window,
        watermark_ms: (watermark != i64::MIN).then_some(watermark),
        commits: sample.commits,
        commit_ms,
        in_flight_epochs: metrics.in_flight.load(Ordering::Relaxed),
        fenced: lifetime.fenced,
        restarts: lifetime.restarts,
        throttled: metrics.throttled.load(Ordering::Relaxed),
        last_error: metrics.last_error.lock().clone(),
    };
    (snapshot, sample)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident() -> ActorIdent {
        ActorIdent {
            tenant: TenantId::new("acme").unwrap(),
            graph: GraphId::new("g").unwrap(),
            slice: "src.events.0".parse().unwrap(),
            node: NodeId::new("decode").unwrap(),
            kind: ActorKind::Map,
            op: OpRef::new("test/decode@1").unwrap(),
        }
    }

    #[test]
    fn phases_split_the_interval() {
        let ms = Duration::from_millis;
        let metrics = ActorMetrics::new(ident(), ms(0));
        metrics.switch(ms(100), Phase::Busy); // idle 0..100
        metrics.switch(ms(700), Phase::Backpressured); // busy 100..700
        // Still backpressured at 1000: the phase in progress must be counted.
        metrics.records_in.store(500, Ordering::Relaxed);
        let (snapshot, sample) = snapshot_actor(&metrics, None, ms(1000), ms(1000));
        assert!((snapshot.idle - 0.1).abs() < 1e-9, "{snapshot:?}");
        assert!((snapshot.busy - 0.6).abs() < 1e-9);
        assert!((snapshot.backpressured - 0.3).abs() < 1e-9);
        assert!((snapshot.in_per_sec - 500.0).abs() < 1e-9);

        // Next interval: blocked the whole time.
        let (snapshot, _) = snapshot_actor(&metrics, Some(sample), ms(2000), ms(1000));
        assert!((snapshot.backpressured - 1.0).abs() < 1e-9, "{snapshot:?}");
        // Rates are smoothed: an interval without records halves the rate.
        assert!((snapshot.in_per_sec - 250.0).abs() < 1e-9);
    }

    #[test]
    fn queue_depth_is_read_from_the_edges() {
        let metrics = ActorMetrics::new(ident(), Duration::ZERO);
        let (tx, _rx) = mpsc::channel::<Msg>(4);
        metrics.watch_input(&tx);
        tx.try_send(Msg::Records(Vec::new())).unwrap();
        tx.try_send(Msg::Records(Vec::new())).unwrap();
        let (snapshot, _) = snapshot_actor(&metrics, None, Duration::from_secs(1), Duration::from_secs(1));
        assert_eq!((snapshot.queue_len, snapshot.queue_cap), (2, 4));
    }
}

#[cfg(test)]
mod window_tests {
    use super::*;

    #[test]
    fn emitter_positions_decode_into_windows() {
        for ts in [-5_000_i64, 0, 1_790_000_000_000] {
            assert_eq!(ts_unkey(ts_key(ts)), ts);
        }
        assert!(ts_key(-1) < ts_key(0) && ts_key(0) < ts_key(1));
        let metrics = SliceMetrics::new();
        *metrics.position.lock() = Some(Position(ts_key(20_000), ts_key(10_000), u64::MAX));
        let tenant = TenantId::new("acme").unwrap();
        let graph = GraphId::new("g").unwrap();
        let (emitter, _) = snapshot_slice(
            &tenant,
            &graph,
            &"agg.count".parse().unwrap(),
            &metrics,
            SliceLifetime::default(),
            None,
        );
        assert_eq!(
            emitter.window,
            Some(EmittedWindow {
                start_ms: 10_000,
                end_ms: 20_000,
                complete: true
            })
        );
        let (source, _) = snapshot_slice(
            &tenant,
            &graph,
            &"src.events.0".parse().unwrap(),
            &metrics,
            SliceLifetime::default(),
            None,
        );
        assert_eq!(source.window, None);
    }
}
