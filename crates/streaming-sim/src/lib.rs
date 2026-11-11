//! Deterministic simulation of a `streaming-rs` cluster.
//!
//! The real worker code runs inside Moonpool processes that the simulator
//! crashes and reboots at will. Everything around it is simulated and hostile:
//!
//! - [`ChaosKv`]: a KV whose commands are delayed and fail, either before
//!   being applied or after (the reply is lost);
//! - the `sim/log@1` source: redelivers records it already delivered, cuts
//!   batches anywhere, fails;
//! - the `sim/*` sinks: fail before writing, after writing, or halfway
//!   through a batch, and remember every element ever written by emit id.
//!
//! A [`Driver`] workload produces a known set of events, waits for the
//! pipeline to settle, then checks that every event was accounted for exactly
//! once.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use moonpool_core::{Providers, RandomProvider, TimeProvider};
use moonpool_sim::providers::SimProviders;
use moonpool_sim::{
    Process, SimContext, SimulationError, SimulationResult, Workload, assert_always,
    assert_sometimes, buggify, buggify_with_prob,
};
use parking_lot::Mutex;
use streaming_core::control::{DesiredState, GraphIndexEntry, RunState, WorkerSnapshot};
use streaming_core::{
    Clock, Datum, DynKv, Emit, GraphSpec, Kv, KvError, MemKv, NoSecrets, NodeId, PartitionReader,
    Position, Registry, SinkConnector, SinkError, SourceConnector, SourceError, SourceRecord,
    Timestamp, TypeTag, Window, Windowed, WorkerId, datum, keys,
};
use streaming_runtime::inspect::{SliceHead, slice_head};
use streaming_runtime::memlog::MemLog;
use streaming_runtime::{GossipConfig, GossipMembership, NoLoad, RuntimeEnv, Worker, WorkerConfig};

/// Wall-clock origin of every simulation, in milliseconds since the epoch.
pub const EPOCH_MS: Timestamp = 1_790_000_000_000;

/// One event of the simulated topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimEvent {
    /// Unique, increasing id.
    pub id: u64,
    /// Grouping key.
    pub key: String,
}

/// Everything a sink ever received, by emit id.
#[derive(Debug, Default)]
pub struct SinkProbe {
    rows: Mutex<BTreeMap<String, (u32, String)>>,
    conflicts: Mutex<Vec<String>>,
}

impl SinkProbe {
    fn record(&self, id: &str, value: String) {
        let mut rows = self.rows.lock();
        match rows.get_mut(id) {
            Some((writes, existing)) => {
                *writes += 1;
                if *existing != value {
                    self.conflicts.lock().push(format!(
                        "emit id `{id}` was written as `{existing}` then as `{value}`"
                    ));
                }
            }
            None => {
                rows.insert(id.to_owned(), (1, value));
            }
        }
    }

    /// The value held for each emit id.
    #[must_use]
    pub fn values(&self) -> Vec<String> {
        self.rows
            .lock()
            .values()
            .map(|(_, value)| value.clone())
            .collect()
    }

    /// Number of distinct emit ids received.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.lock().len()
    }

    /// Whether nothing was received.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.lock().is_empty()
    }

    /// Number of writes that hit an emit id already present.
    #[must_use]
    pub fn rewrites(&self) -> u64 {
        self.rows
            .lock()
            .values()
            .map(|(writes, _)| u64::from(writes - 1))
            .sum()
    }

    /// Emit ids that were written with two different values.
    #[must_use]
    pub fn conflicts(&self) -> Vec<String> {
        self.conflicts.lock().clone()
    }
}

/// What the driver produced and therefore expects to find.
#[derive(Debug, Default)]
pub struct Produced {
    /// Number of events appended.
    pub total: u64,
    /// Expected count per first-level window and key.
    pub counts: BTreeMap<(Window, String), u64>,
}

/// The world shared by the processes and workloads of one simulation run.
pub struct SimEnv {
    /// The store behind every [`ChaosKv`].
    pub kv: Arc<MemKv>,
    /// The simulated topic.
    pub log: Arc<MemLog>,
    /// Sink of the per-key counts.
    pub counts: Arc<SinkProbe>,
    /// Sink fed by the stateless path, one element per event.
    pub raw: Arc<SinkProbe>,
    /// Sink fed twice per event, through two parallel branches.
    pub both: Arc<SinkProbe>,
    /// Sink of the second-level aggregation.
    pub totals: Arc<SinkProbe>,
    /// What was produced.
    pub produced: Mutex<Produced>,
    /// Number of KV replies lost after the command was applied.
    pub kv_replies_lost: Mutex<u64>,
    /// Number of redeliveries injected by the source.
    pub redeliveries: Mutex<u64>,
    /// Number of sink writes that failed after being partly or fully applied.
    pub sink_ambiguous: Mutex<u64>,
    /// Number of worker boots.
    pub boots: Mutex<u64>,
    /// Set by the driver once every event is produced.
    pub production_done: Mutex<bool>,
    /// Multiplier applied to the probability of every injected fault.
    pub fault_scale: f64,
    /// Upper bound of the latency of an ordinary KV command, in milliseconds.
    pub kv_latency_ms: u64,
    /// Number of actions the operator took, per kind.
    pub operator_actions: Mutex<BTreeMap<&'static str, u64>>,
    /// What the auditor found wrong while the run was in progress.
    pub audit: Mutex<Vec<String>>,
    /// Number of times the auditor looked at the commit logs.
    pub audits: Mutex<u64>,
}

impl SimEnv {
    /// A fresh world for one run of `scenario`.
    #[must_use]
    pub fn new(scenario: &Scenario) -> Arc<Self> {
        Arc::new(Self {
            fault_scale: scenario.fault_scale,
            kv_latency_ms: scenario.kv_latency_ms.max(1),
            kv: MemKv::shared(),
            log: MemLog::with_retention(scenario.partitions, true),
            counts: Arc::default(),
            raw: Arc::default(),
            both: Arc::default(),
            totals: Arc::default(),
            produced: Mutex::default(),
            kv_replies_lost: Mutex::new(0),
            redeliveries: Mutex::new(0),
            sink_ambiguous: Mutex::new(0),
            boots: Mutex::new(0),
            production_done: Mutex::new(false),
            operator_actions: Mutex::default(),
            audit: Mutex::default(),
            audits: Mutex::new(0),
        })
    }
}

/// Where the current run's world is kept between the builder hooks.
pub type EnvSlot = Arc<Mutex<Option<Arc<SimEnv>>>>;

fn env_of(slot: &EnvSlot) -> SimulationResult<Arc<SimEnv>> {
    slot.lock()
        .clone()
        .ok_or_else(|| SimulationError::InvalidState("simulation world not initialised".into()))
}

/// Simulated wall clock: the simulation's logical time on top of [`EPOCH_MS`].
pub struct SimClock<T: TimeProvider> {
    time: T,
    drifting: bool,
}

impl<T: TimeProvider> SimClock<T> {
    /// The exact simulated time.
    pub fn exact(time: T) -> Self {
        Self {
            time,
            drifting: false,
        }
    }

    /// The simulated time as a process with a drifting clock reads it.
    pub fn drifting(time: T) -> Self {
        Self {
            time,
            drifting: true,
        }
    }
}

impl<T: TimeProvider> Clock for SimClock<T> {
    fn now_ms(&self) -> Timestamp {
        let elapsed = if self.drifting {
            self.time.timer()
        } else {
            self.time.now()
        };
        EPOCH_MS + elapsed.as_millis() as Timestamp
    }
}

/// A [`Kv`] that misbehaves the way a remote store does.
pub struct ChaosKv<P: Providers> {
    env: Arc<SimEnv>,
    providers: P,
}

impl<P: Providers> ChaosKv<P> {
    /// A hostile view of the world's store.
    pub fn new(env: Arc<SimEnv>, providers: P) -> Self {
        Self { env, providers }
    }

    async fn before(&self) -> Result<(), KvError> {
        let scale = self.env.fault_scale;
        let max_ms = if buggify_with_prob!(0.02 * scale) {
            400
        } else {
            self.env.kv_latency_ms
        };
        let delay = self.providers.random().random_range(0..max_ms);
        let _ = self
            .providers
            .time()
            .sleep(Duration::from_millis(delay))
            .await;
        if buggify_with_prob!(0.03 * scale) {
            return Err(KvError::Unavailable("injected: request lost".into()));
        }
        Ok(())
    }

    fn after<T>(&self, value: T) -> Result<T, KvError> {
        if buggify_with_prob!(0.03 * self.env.fault_scale) {
            *self.env.kv_replies_lost.lock() += 1;
            return Err(KvError::Unavailable("injected: reply lost".into()));
        }
        Ok(value)
    }
}

#[async_trait]
impl<P: Providers> Kv for ChaosKv<P> {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> {
        self.before().await?;
        self.after(self.env.kv.get(key).await?)
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), KvError> {
        self.before().await?;
        self.after(self.env.kv.set(key, value).await?)
    }

    async fn del(&self, keys: Vec<String>) -> Result<(), KvError> {
        self.before().await?;
        self.after(self.env.kv.del(keys).await?)
    }

    async fn incr(&self, key: &str) -> Result<i64, KvError> {
        self.before().await?;
        self.after(self.env.kv.incr(key).await?)
    }

    async fn hsetnx(&self, key: &str, field: &str, value: Vec<u8>) -> Result<bool, KvError> {
        self.before().await?;
        self.after(self.env.kv.hsetnx(key, field, value).await?)
    }

    async fn hset(&self, key: &str, entries: Vec<(String, Vec<u8>)>) -> Result<(), KvError> {
        self.before().await?;
        self.after(self.env.kv.hset(key, entries).await?)
    }

    async fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>, KvError> {
        self.before().await?;
        self.after(self.env.kv.hget(key, field).await?)
    }

    async fn hgetall(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>, KvError> {
        self.before().await?;
        self.after(self.env.kv.hgetall(key).await?)
    }

    async fn hdel(&self, key: &str, fields: Vec<String>) -> Result<(), KvError> {
        self.before().await?;
        self.after(self.env.kv.hdel(key, fields).await?)
    }
}

struct ChaosSource<P: Providers> {
    env: Arc<SimEnv>,
    providers: P,
}

#[async_trait]
impl<P: Providers> SourceConnector for ChaosSource<P> {
    async fn partitions(&self) -> Result<u32, SourceError> {
        if buggify!() {
            return Err(SourceError("injected: metadata lookup failed".into()));
        }
        Ok(self.env.log.partitions())
    }

    async fn open(
        &self,
        partition: u32,
        after: Option<Position>,
    ) -> Result<Box<dyn PartitionReader>, SourceError> {
        if buggify!() {
            return Err(SourceError("injected: cannot open reader".into()));
        }
        // A real broker resumes from its own cursor, which may be behind what
        // the framework has committed: start from the beginning of what the
        // log still retains when the hint is ignored.
        let next = if buggify!() {
            0
        } else {
            after.map_or(0, |position| position.1)
        };
        Ok(Box::new(ChaosReader {
            env: self.env.clone(),
            providers: self.providers.clone(),
            partition,
            next,
        }))
    }
}

struct ChaosReader<P: Providers> {
    env: Arc<SimEnv>,
    providers: P,
    partition: u32,
    next: u64,
}

#[async_trait]
impl<P: Providers> PartitionReader for ChaosReader<P> {
    async fn poll(
        &mut self,
        max: usize,
        max_wait: Duration,
    ) -> Result<Vec<SourceRecord>, SourceError> {
        let scale = self.env.fault_scale;
        if buggify_with_prob!(0.02 * scale) {
            return Err(SourceError(
                "injected: connection to the broker lost".into(),
            ));
        }
        if buggify_with_prob!(0.05 * scale) {
            // Redelivery: the broker rewinds to somewhere before what it delivered.
            let back = self.providers.random().random_range(1..64_u64);
            self.next = self.next.saturating_sub(back);
            *self.env.redeliveries.lock() += 1;
        }
        let max = if buggify!() {
            self.providers.random().random_range(1..max.max(2))
        } else {
            max
        };
        let log = &self.env.log;
        let wait = log.appended();
        let mut records = log.read(self.partition, self.next, max);
        if records.is_empty() {
            let _ = self.providers.time().timeout(max_wait, wait).await;
            records = log.read(self.partition, self.next, max);
        }
        if let Some(last) = records.last() {
            self.next = last.position.1;
        }
        Ok(records)
    }

    async fn ack(&mut self, upto: Position) -> Result<(), SourceError> {
        if buggify_with_prob!(0.1) {
            return Err(SourceError("injected: ack lost".into()));
        }
        self.env.log.ack(self.partition, upto);
        Ok(())
    }

    async fn end_position(&mut self) -> Result<Option<Position>, SourceError> {
        if buggify_with_prob!(0.1) {
            return Err(SourceError("injected: cannot read the end position".into()));
        }
        Ok(self.env.log.end(self.partition))
    }
}

/// Turns an element into the text a sink probe remembers.
type Render = fn(&Datum) -> String;

struct ChaosSink<P: Providers> {
    env: Arc<SimEnv>,
    probe: Arc<SinkProbe>,
    providers: P,
    render: Render,
}

#[async_trait]
impl<P: Providers> SinkConnector for ChaosSink<P> {
    async fn write(&self, batch: &[Emit]) -> Result<(), SinkError> {
        let delay = self.providers.random().random_range(0..8_u64);
        let _ = self
            .providers
            .time()
            .sleep(Duration::from_millis(delay))
            .await;
        let scale = self.env.fault_scale;
        if buggify_with_prob!(0.03 * scale) {
            return Err(SinkError("injected: sink unreachable".into()));
        }
        let applied = if buggify_with_prob!(0.03 * scale) {
            // Fails halfway: a prefix of the batch is durably written.
            self.providers
                .random()
                .random_range(0..batch.len().max(1) + 1)
        } else {
            batch.len()
        };
        for emit in &batch[..applied] {
            self.probe
                .record(emit.id.as_str(), (self.render)(&emit.value));
        }
        if applied < batch.len() || buggify_with_prob!(0.03 * scale) {
            *self.env.sink_ambiguous.lock() += 1;
            return Err(SinkError("injected: write outcome unknown".into()));
        }
        Ok(())
    }
}

fn render_count(value: &Datum) -> String {
    match value.downcast_ref::<Windowed<u64>>() {
        Some(windowed) => format!(
            "{}|{}|{}",
            windowed.window,
            windowed.key.display_lossy(),
            windowed.value
        ),
        None => "unexpected type".to_owned(),
    }
}

fn render_event(value: &Datum) -> String {
    match value.downcast_ref::<SimEvent>() {
        Some(event) => event.id.to_string(),
        None => "unexpected type".to_owned(),
    }
}

/// The operators of the simulated graphs.
pub fn registry<P: Providers>(env: &Arc<SimEnv>) -> Registry<P> {
    let mut registry = Registry::<P>::standard();
    {
        let env = env.clone();
        registry.register_source::<SimEvent>("sim/log@1", "Simulated topic.", move |context, _| {
            Ok(Arc::new(ChaosSource {
                env: env.clone(),
                providers: context.providers.clone(),
            }) as Arc<dyn SourceConnector>)
        });
    }
    registry.register_key("sim/key@1", "Key of an event.", |event: &SimEvent| {
        event.key.clone()
    });
    registry.register_map(
        "sim/branch-a@1",
        "First parallel branch.",
        |event: &SimEvent| Some(event.clone()),
    );
    registry.register_map(
        "sim/branch-b@1",
        "Second parallel branch.",
        |event: &SimEvent| Some(event.clone()),
    );
    registry.register_key(
        "sim/all@1",
        "One key for everything.",
        |_: &Windowed<u64>| "all".to_owned(),
    );
    let sinks: [(&str, Arc<SinkProbe>, Render); 4] = [
        ("sim/counts@1", env.counts.clone(), render_count),
        ("sim/raw@1", env.raw.clone(), render_event),
        ("sim/both@1", env.both.clone(), render_event),
        ("sim/totals@1", env.totals.clone(), render_count),
    ];
    for (op, probe, render) in sinks {
        let env = env.clone();
        registry.register_sink(op, "Recording sink.", TypeTag::Any, move |context, _| {
            Ok(Arc::new(ChaosSink {
                env: env.clone(),
                probe: probe.clone(),
                providers: context.providers.clone(),
                render,
            }) as Arc<dyn SinkConnector>)
        });
    }
    registry
}

/// How the simulated cluster is set up.
#[derive(Debug, Clone)]
pub struct Scenario {
    /// Partitions of the simulated topic.
    pub partitions: u32,
    /// Number of events to produce.
    pub events: u64,
    /// Number of distinct keys.
    pub keys: u64,
    /// Simulated time over which events are produced.
    pub produce_for: Duration,
    /// Size of the first-level windows.
    pub window: Duration,
    /// Distance between the starts of two first-level windows. Equal to
    /// `window` for tumbling windows, smaller for sliding ones.
    pub slide: Duration,
    /// How long after its end a first-level window still accepts records.
    pub allowed_lateness: Duration,
    /// How far in the past an event may be stamped when it is produced.
    pub max_event_age: Duration,
    /// Maximum number of records per epoch.
    pub max_batch: usize,
    /// An operator pauses, drains and re-submits the graph while it runs.
    pub operator: bool,
    /// Multiplier applied to the probability of every injected fault.
    pub fault_scale: f64,
    /// Upper bound of the latency of an ordinary KV command, in milliseconds.
    pub kv_latency_ms: u64,
    /// Silence after which a fully-read partition follows the wall clock.
    pub idle_timeout: Duration,
    /// Number of partitions, among the last ones, that never receive an event.
    pub silent_partitions: u32,
    /// Every worker runs every slice, whatever the placement says.
    pub duel: bool,
    /// Workers find each other through gossip over the simulated network
    /// instead of KV heartbeats.
    pub gossip: bool,
    /// How long the driver waits for the pipeline to settle.
    pub settle_timeout: Duration,
}

impl Default for Scenario {
    fn default() -> Self {
        Self {
            partitions: 3,
            events: 1_500,
            keys: 8,
            produce_for: Duration::from_secs(20),
            window: Duration::from_secs(2),
            slide: Duration::from_secs(2),
            allowed_lateness: Duration::ZERO,
            max_event_age: Duration::from_millis(150),
            max_batch: 40,
            operator: false,
            fault_scale: 1.0,
            kv_latency_ms: 6,
            idle_timeout: Duration::from_millis(1_500),
            silent_partitions: 0,
            duel: false,
            gossip: false,
            settle_timeout: Duration::from_secs(900),
        }
    }
}

impl Scenario {
    /// The first-level windows an event stamped `ts` belongs to.
    #[must_use]
    pub fn windows_of(&self, ts: Timestamp) -> Vec<Window> {
        let size = self.window.as_millis() as i64;
        let slide = self.slide.as_millis().max(1) as i64;
        let mut windows = Vec::new();
        let mut start = ts.div_euclid(slide) * slide;
        while start > ts - size {
            windows.push(Window::new(start, start + size));
            start -= slide;
        }
        windows
    }

    /// Size of the second-level windows.
    #[must_use]
    pub fn big_window(&self) -> Duration {
        self.window * 4
    }

    /// The simulated graph: a windowed count per key, a stateless copy of every
    /// event, a diamond delivering every event twice to one sink, and a
    /// second aggregation over the results of the first.
    #[must_use]
    pub fn spec(&self) -> GraphSpec {
        let millis = |duration: Duration| format!("{}ms", duration.as_millis());
        let first_window = if self.slide == self.window {
            serde_json::json!({"id": "win", "op": "std/tumbling@1", "config": {
                "size": millis(self.window),
                "allowed_lateness": millis(self.allowed_lateness)}})
        } else {
            serde_json::json!({"id": "win", "op": "std/sliding@1", "config": {
                "size": millis(self.window),
                "slide": millis(self.slide),
                "allowed_lateness": millis(self.allowed_lateness)}})
        };
        let big = millis(self.big_window());
        serde_json::from_value(serde_json::json!({
            "tenant": "acme",
            "graph": "sim",
            "version": 1,
            "settings": {
                "max_out_of_orderness": "600ms",
                "idle_timeout": millis(self.idle_timeout),
                "linger": "40ms",
                "flush_interval": "250ms",
                "max_batch": self.max_batch,
                "chunk_size": 7,
                "mailbox_capacity": 2,
                "max_in_flight_epochs": 2,
                // Above three times the slowest store command of `ChaosKv`.
                "gc_grace": "2s"
            },
            "nodes": [
                {"id": "events", "op": "sim/log@1"},
                {"id": "by-key", "op": "sim/key@1"},
                first_window,
                {"id": "count", "op": "std/count@1"},
                {"id": "counts", "op": "sim/counts@1"},
                {"id": "raw", "op": "sim/raw@1"},
                {"id": "branch-a", "op": "sim/branch-a@1"},
                {"id": "branch-b", "op": "sim/branch-b@1"},
                {"id": "both", "op": "sim/both@1"},
                {"id": "all", "op": "sim/all@1"},
                {"id": "big-win", "op": "std/tumbling@1", "config": {"size": big}},
                {"id": "total", "op": "std/count@1"},
                {"id": "totals", "op": "sim/totals@1"}
            ],
            "edges": [
                ["events", "by-key"], ["by-key", "win"], ["win", "count"], ["count", "counts"],
                ["events", "raw"],
                ["events", "branch-a"], ["events", "branch-b"],
                ["branch-a", "both"], ["branch-b", "both"],
                ["count", "all"], ["all", "big-win"], ["big-win", "total"], ["total", "totals"]
            ]
        }))
        .unwrap_or_else(|error| panic!("invalid simulated graph: {error}"))
    }
}

/// A worker of the simulated cluster. Rebooted at will by the simulator.
pub struct WorkerProcess {
    slot: EnvSlot,
    scenario: Scenario,
}

impl WorkerProcess {
    /// A worker of the given scenario.
    #[must_use]
    pub fn new(slot: EnvSlot, scenario: Scenario) -> Self {
        Self { slot, scenario }
    }
}

#[async_trait]
impl Process for WorkerProcess {
    fn name(&self) -> &str {
        "worker"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let env = env_of(&self.slot)?;
        *env.boots.lock() += 1;
        let providers: SimProviders = ctx.providers().clone();
        let worker = WorkerId::new(format!("worker-{}", ctx.my_ip().replace(':', "-")))
            .map_err(|error| SimulationError::InvalidState(error.to_string()))?;
        let kv: DynKv = Arc::new(ChaosKv::new(env.clone(), providers.clone()));
        let mut config = WorkerConfig::new(worker);
        config.tick = Duration::from_millis(200);
        config.member_timeout = Duration::from_millis(1_500);
        config.restart_backoff = Duration::from_millis(150);
        config.controller_grace = Duration::from_millis(600);
        config.catch_panics = false;
        config.ignore_placement = self.scenario.duel;
        let runtime = RuntimeEnv {
            clock: Arc::new(SimClock::drifting(providers.time().clone())),
            registry: Arc::new(registry::<SimProviders>(&env)),
            secrets: Arc::new(NoSecrets),
            kv,
            providers,
        };
        let mut worker = Worker::new(runtime, config).with_load_probe(Arc::new(NoLoad));
        if self.scenario.gossip {
            // The seed is the process with the lowest address; a process's
            // topology lists every address but its own.
            let mut addresses: Vec<std::net::IpAddr> = ctx
                .topology()
                .all_process_ips()
                .iter()
                .map(String::as_str)
                .chain(std::iter::once(ctx.my_ip()))
                .filter_map(|ip| ip.parse().ok())
                .collect();
            addresses.sort();
            let seed = addresses
                .first()
                .map(ToString::to_string)
                .unwrap_or_default();
            let providers = ctx.providers().clone();
            let membership = loop {
                // A new generation at every boot: what the previous incarnation
                // gossiped is replaced.
                let generation = providers.time().now().as_millis() as u64 + 1;
                let mut gossip = GossipConfig::new(
                    "sim",
                    format!("worker-{}", ctx.my_ip()),
                    generation,
                    ctx.my_ip(),
                );
                gossip.seeds = vec![seed.clone()];
                gossip.gossip_interval = Duration::from_millis(250);
                gossip.dead_after = Duration::from_millis(2_500);
                gossip.forget_after = Duration::from_secs(30);
                match GossipMembership::join(providers.clone(), gossip, env.kv.clone()).await {
                    Ok(membership) => break membership,
                    Err(_) => {
                        let _ = providers.time().sleep(Duration::from_millis(200)).await;
                    }
                }
            };
            worker = worker.with_membership(Arc::new(membership));
        }
        worker.run(ctx.shutdown().clone()).await;
        Ok(())
    }
}

/// Produces the events, waits for the pipeline to settle and checks the result.
pub struct Driver {
    slot: EnvSlot,
    scenario: Scenario,
}

impl Driver {
    /// A driver for the given scenario.
    #[must_use]
    pub fn new(slot: EnvSlot, scenario: Scenario) -> Self {
        Self { slot, scenario }
    }

    fn counted(env: &SimEnv) -> u64 {
        env.counts
            .values()
            .iter()
            .filter_map(|row| row.rsplit('|').next()?.parse::<u64>().ok())
            .sum()
    }

    /// Expected second-level results: for each big window, how many first-level
    /// results (one per window and key) fall into it.
    fn expected_totals(&self, counts: &BTreeMap<(Window, String), u64>) -> BTreeMap<Window, u64> {
        let big = self.scenario.big_window().as_millis() as i64;
        let mut totals = BTreeMap::new();
        for (window, _) in counts.keys() {
            let start = (window.end - 1).div_euclid(big) * big;
            *totals.entry(Window::new(start, start + big)).or_default() += 1;
        }
        totals
    }

    fn settled(&self, env: &SimEnv) -> bool {
        let produced = env.produced.lock();
        let expected_totals: u64 = self.expected_totals(&produced.counts).values().sum();
        let totals: u64 = env
            .totals
            .values()
            .iter()
            .filter_map(|row| row.rsplit('|').next()?.parse::<u64>().ok())
            .sum();
        Self::counted(env) == produced.counts.values().sum::<u64>()
            && env.raw.len() as u64 == produced.total
            && env.both.len() as u64 == 2 * produced.total
            && totals == expected_totals
    }
}

/// Print what the control plane looked like, to diagnose a run that hangs.
async fn dump(env: &SimEnv) {
    let kv: DynKv = env.kv.clone();
    eprintln!("--- KV keys: {:?}", env.kv.keys());
    for key in env.kv.keys() {
        let interesting = key.ends_with(":plan")
            || key == keys::CLUSTER_PLACEMENT
            || key == keys::CLUSTER_EPOCH
            || key.starts_with("cluster:metrics:");
        if !interesting {
            continue;
        }
        match kv.get(&key).await {
            Ok(Some(bytes)) if key.starts_with("cluster:metrics:") => {
                if let Ok(snapshot) = serde_json::from_slice::<WorkerSnapshot>(&bytes) {
                    for slice in snapshot.slices {
                        eprintln!(
                            "--- {key} slice {} state={:?} epoch={} pos={:?} wm={:?} fenced={} restarts={} err={:?}",
                            slice.slice,
                            slice.state,
                            slice.epoch,
                            slice.position,
                            slice.watermark_ms,
                            slice.fenced,
                            slice.restarts,
                            slice.last_error
                        );
                    }
                    for actor in snapshot.actors.iter().filter(|actor| actor.errors > 0) {
                        eprintln!(
                            "--- {key} actor {}/{} {:?} errors={} last={:?}",
                            actor.slice, actor.node, actor.kind, actor.errors, actor.last_error
                        );
                    }
                }
            }
            Ok(Some(bytes)) => eprintln!("--- {key} = {}", String::from_utf8_lossy(&bytes)),
            other => eprintln!("--- {key}: {other:?}"),
        }
    }
    if let Ok(members) = kv.hgetall(keys::CLUSTER_MEMBERS).await {
        for (worker, info) in members {
            eprintln!("--- member {worker} = {}", String::from_utf8_lossy(&info));
        }
    }
    for key in env.kv.keys() {
        let state = key.ends_with(":log")
            || key.ends_with(":seal")
            || key.contains(":idx:")
            || key.contains(":w:");
        if !state {
            continue;
        }
        if let Ok(fields) = kv.hgetall(&key).await {
            let heavy: Vec<String> = fields
                .iter()
                .filter(|(_, value)| value.len() > 1)
                .map(|(field, value)| format!("{field}({}b)", value.len()))
                .collect();
            let names: Vec<&str> = fields.iter().map(|(field, _)| field.as_str()).collect();
            if key.ends_with(":log") {
                eprintln!(
                    "--- {key}: {} fields, with payload: {heavy:?}",
                    fields.len()
                );
            } else {
                eprintln!(
                    "--- {key}: {} fields {:?}",
                    fields.len(),
                    &names[..names.len().min(12)]
                );
            }
        }
    }
}

fn violation(message: String) -> SimulationError {
    eprintln!(
        "[VIOLATION] seed={} | {message}",
        moonpool_sim::current_sim_seed()
    );
    SimulationError::InvalidState(message)
}

#[async_trait]
impl Workload for Driver {
    fn name(&self) -> &str {
        "driver"
    }

    async fn setup(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        let env = env_of(&self.slot)?;
        let spec = self.scenario.spec();
        let graph = spec.key();
        let kv: DynKv = env.kv.clone();
        let failed = |error: KvError| violation(error.to_string());
        kv.set_json(&keys::graph_spec(&graph), &spec)
            .await
            .map_err(failed)?;
        kv.hset_json(
            keys::CLUSTER_GRAPHS,
            &graph.to_string(),
            &GraphIndexEntry {
                tenant: spec.tenant.clone(),
                graph: spec.graph.clone(),
                version: spec.version,
                updated_ms: EPOCH_MS,
                deleting: false,
            },
        )
        .await
        .map_err(failed)?;
        kv.incr(keys::CLUSTER_EPOCH).await.map_err(failed)?;
        Ok(())
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let env = env_of(&self.slot)?;
        let scenario = &self.scenario;
        let clock = SimClock::exact(ctx.time().clone());
        let pause = scenario.produce_for / scenario.events.max(1) as u32;
        let max_age = scenario.max_event_age.as_millis().max(1) as i64;

        for id in 0..scenario.events {
            let key = format!("key-{}", ctx.random().random_range(0..scenario.keys.max(1)));
            // Events are stamped in the past, as if they had travelled.
            let ts = clock.now_ms() - ctx.random().random_range(0..max_age);
            let fed = scenario
                .partitions
                .saturating_sub(scenario.silent_partitions)
                .max(1);
            let partition = ctx.random().random_range(0..fed);
            {
                let mut produced = env.produced.lock();
                produced.total += 1;
                for window in scenario.windows_of(ts) {
                    *produced.counts.entry((window, key.clone())).or_default() += 1;
                }
            }
            env.log.append(partition, ts, datum(SimEvent { id, key }));
            if ctx.random().random_bool(0.3) {
                let _ = ctx.time().sleep(pause * 3).await;
            }
        }

        *env.production_done.lock() = true;

        let started = ctx.time().now();
        while !self.settled(&env) {
            if ctx.time().now().saturating_sub(started) > scenario.settle_timeout {
                dump(&env).await;
                let produced = env.produced.lock();
                return Err(violation(format!(
                    "the pipeline did not settle: counted {}/{}, raw {}/{}, both {}/{}, totals rows {}",
                    Self::counted(&env),
                    produced.counts.values().sum::<u64>(),
                    env.raw.len(),
                    produced.total,
                    env.both.len(),
                    2 * produced.total,
                    env.totals.len(),
                )));
            }
            let _ = ctx.time().sleep(Duration::from_millis(500)).await;
        }

        // Everything is out: the state of the emitted windows must leave the
        // KV, and the commit logs must have stayed small. Leaks would not
        // falsify a count, but they would fill a real store.
        let started = ctx.time().now();
        loop {
            let leaked: Vec<String> = env
                .kv
                .keys()
                .into_iter()
                .filter(|key| key.contains(":w:") || key.contains(":idx:"))
                .collect();
            if leaked.is_empty() {
                break;
            }
            if ctx.time().now().saturating_sub(started) > Duration::from_secs(300) {
                dump(&env).await;
                return Err(violation(format!(
                    "window state was not garbage-collected: {} keys left, e.g. {:?}",
                    leaked.len(),
                    &leaked[..leaked.len().min(3)]
                )));
            }
            let _ = ctx.time().sleep(Duration::from_millis(500)).await;
        }
        let store: DynKv = env.kv.clone();
        for key in env.kv.keys() {
            if !key.ends_with(":log") {
                continue;
            }
            let fields = store
                .hgetall(&key)
                .await
                .map(|fields| fields.len())
                .unwrap_or(0);
            if fields > 60 {
                return Err(violation(format!(
                    "commit log `{key}` grew to {fields} fields"
                )));
            }
        }
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        let env = env_of(&self.slot)?;
        let produced = {
            let produced = env.produced.lock();
            Produced {
                total: produced.total,
                counts: produced.counts.clone(),
            }
        };

        let audit = env.audit.lock().clone();
        assert_always!(
            audit.is_empty(),
            "the committed history of a slice never goes backwards"
        );
        if let Some(finding) = audit.first() {
            return Err(violation(finding.clone()));
        }
        assert_sometimes!(
            *env.audits.lock() > 10,
            "the auditor watched the commit logs"
        );

        for probe in [&env.counts, &env.raw, &env.both, &env.totals] {
            let conflicts = probe.conflicts();
            assert_always!(
                conflicts.is_empty(),
                "an emit id always carries the same value"
            );
            if let Some(conflict) = conflicts.first() {
                return Err(violation(conflict.clone()));
            }
        }

        // Per-key counts: exactly what was produced, each (window, key) once.
        let mut counts: BTreeMap<(Window, String), u64> = BTreeMap::new();
        for row in env.counts.values() {
            let mut parts = row.split('|');
            let (Some(window), Some(key), Some(count)) = (parts.next(), parts.next(), parts.next())
            else {
                return Err(violation(format!("malformed count row `{row}`")));
            };
            let window: Window = window.parse().map_err(violation)?;
            let count: u64 = count
                .parse()
                .map_err(|_| violation(format!("bad count in `{row}`")))?;
            if counts.insert((window, key.to_owned()), count).is_some() {
                return Err(violation(format!(
                    "window {window} key {key} was emitted under two different ids"
                )));
            }
        }
        let exact = counts == produced.counts;
        assert_always!(exact, "every event is counted exactly once");
        if !exact {
            let wrong: Vec<String> = produced
                .counts
                .iter()
                .filter(|(at, expected)| counts.get(*at) != Some(*expected))
                .take(5)
                .map(|((window, key), expected)| {
                    format!(
                        "{window}/{key}: expected {expected}, got {:?}",
                        counts.get(&(*window, key.clone()))
                    )
                })
                .collect();
            return Err(violation(format!("counts differ: {}", wrong.join("; "))));
        }

        // Stateless path: every event once.
        let mut raw: Vec<u64> = env
            .raw
            .values()
            .iter()
            .filter_map(|v| v.parse().ok())
            .collect();
        raw.sort_unstable();
        let raw_exact = raw == (0..produced.total).collect::<Vec<u64>>();
        assert_always!(
            raw_exact,
            "the stateless path delivers every event exactly once"
        );
        if !raw_exact {
            return Err(violation(format!(
                "raw sink holds {} ids for {} events",
                raw.len(),
                produced.total
            )));
        }

        // Diamond: every event exactly twice, once per branch.
        let mut both: BTreeMap<u64, u32> = BTreeMap::new();
        for value in env.both.values() {
            if let Ok(id) = value.parse::<u64>() {
                *both.entry(id).or_default() += 1;
            }
        }
        let both_exact = both.len() as u64 == produced.total && both.values().all(|n| *n == 2);
        assert_always!(both_exact, "a fan-in delivers each branch exactly once");
        if !both_exact {
            return Err(violation(
                "the two-branch sink does not hold every event twice".into(),
            ));
        }

        // Second-level aggregation over the results of the first.
        let mut totals: BTreeMap<Window, u64> = BTreeMap::new();
        for row in env.totals.values() {
            let mut parts = row.split('|');
            let (Some(window), Some(_), Some(count)) = (parts.next(), parts.next(), parts.next())
            else {
                return Err(violation(format!("malformed total row `{row}`")));
            };
            let window: Window = window.parse().map_err(violation)?;
            let count: u64 = count
                .parse()
                .map_err(|_| violation(format!("bad total in `{row}`")))?;
            if totals.insert(window, count).is_some() {
                return Err(violation(format!("big window {window} was emitted twice")));
            }
        }
        let expected_totals = self.expected_totals(&produced.counts);
        let totals_exact = totals == expected_totals;
        assert_always!(
            totals_exact,
            "a second aggregation sees every first-level result once"
        );
        if !totals_exact {
            return Err(violation(format!(
                "second-level totals differ: expected {expected_totals:?}, got {totals:?}"
            )));
        }

        // Coverage: the faults this harness exists for did happen.
        let kv: DynKv = env.kv.clone();
        let mut fenced = 0;
        let mut restarts = 0;
        for key in env.kv.keys() {
            if key.starts_with("cluster:metrics:")
                && let Ok(Some(snapshot)) = kv.get_json::<WorkerSnapshot>(&key).await
            {
                fenced += snapshot
                    .slices
                    .iter()
                    .map(|slice| slice.fenced)
                    .sum::<u64>();
                restarts += snapshot
                    .slices
                    .iter()
                    .map(|slice| slice.restarts)
                    .sum::<u64>();
            }
        }
        assert_sometimes!(fenced > 0, "a worker lost a commit to another worker");
        assert_sometimes!(restarts > 0, "a slice restarted from its last commit");
        assert_sometimes!(
            *env.kv_replies_lost.lock() > 0,
            "a KV reply was lost after the command applied"
        );
        assert_sometimes!(
            *env.redeliveries.lock() > 0,
            "the source redelivered records"
        );
        assert_sometimes!(
            *env.sink_ambiguous.lock() > 0,
            "a sink write failed after being applied"
        );
        assert_sometimes!(
            env.counts.rewrites() + env.raw.rewrites() > 0,
            "a sink received an element it already held"
        );
        assert_sometimes!(*env.boots.lock() > 3, "a worker was rebooted");
        let actions = env.operator_actions.lock().clone();
        for kind in ["pause", "pause-node", "drain", "resubmit"] {
            assert_sometimes!(
                actions.get(kind).copied().unwrap_or(0) > 0,
                format!("the operator did: {kind}")
            );
        }
        let widest = produced
            .counts
            .keys()
            .fold(BTreeMap::<Window, usize>::new(), |mut keys, (window, _)| {
                *keys.entry(*window).or_default() += 1;
                keys
            })
            .into_values()
            .max()
            .unwrap_or(0);
        assert_sometimes!(
            widest > self.scenario.max_batch,
            "a window held more keys than one epoch can emit"
        );
        Ok(())
    }
}

/// A human operator, as the pipeline sees one: pauses the graph, pauses a
/// node, drains, re-submits the description, at random, while events flow.
/// Leaves the graph running once production is over.
pub struct Operator {
    slot: EnvSlot,
    scenario: Scenario,
}

impl Operator {
    /// An operator for the given scenario. Does nothing unless the scenario
    /// asks for one.
    #[must_use]
    pub fn new(slot: EnvSlot, scenario: Scenario) -> Self {
        Self { slot, scenario }
    }
}

#[async_trait]
impl Workload for Operator {
    fn name(&self) -> &str {
        "operator"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // The simulation ends as soon as one workload returns: this one must
        // outlive the driver, which decides when the run is over.
        if !self.scenario.operator {
            ctx.shutdown().cancelled().await;
            return Ok(());
        }
        let env = env_of(&self.slot)?;
        let kv: DynKv = env.kv.clone();
        let mut spec = self.scenario.spec();
        let graph = spec.key();
        let nodes: Vec<NodeId> = spec.nodes.iter().map(|node| node.id.clone()).collect();
        let failed = |error: KvError| violation(error.to_string());
        let pause =
            |low: u64, high: u64| Duration::from_millis(ctx.random().random_range(low..high));

        let desire = |state: RunState, paused: Option<NodeId>| {
            let kv = kv.clone();
            let graph = graph.clone();
            async move {
                let desired = DesiredState {
                    state,
                    paused_nodes: paused.into_iter().collect::<BTreeSet<_>>(),
                    updated_ms: 0,
                };
                kv.set_json(&keys::graph_desired(&graph), &desired).await?;
                kv.incr(keys::CLUSTER_EPOCH).await.map(|_| ())
            }
        };

        while !*env.production_done.lock() {
            let _ = ctx.time().sleep(pause(800, 3_000)).await;
            let kind = match ctx.random().random_range(0..4_u32) {
                0 => {
                    desire(RunState::Paused, None).await.map_err(failed)?;
                    let _ = ctx.time().sleep(pause(300, 2_000)).await;
                    desire(RunState::Running, None).await.map_err(failed)?;
                    "pause"
                }
                1 => {
                    let node = nodes[ctx.random().random_range(0..nodes.len())].clone();
                    desire(RunState::Running, Some(node))
                        .await
                        .map_err(failed)?;
                    let _ = ctx.time().sleep(pause(300, 2_000)).await;
                    desire(RunState::Running, None).await.map_err(failed)?;
                    "pause-node"
                }
                2 => {
                    desire(RunState::Draining, None).await.map_err(failed)?;
                    let _ = ctx.time().sleep(pause(500, 2_500)).await;
                    desire(RunState::Running, None).await.map_err(failed)?;
                    "drain"
                }
                _ => {
                    // The same description under a new version: workers stop
                    // the graph's slices and start them again from their logs.
                    spec.version += 1;
                    kv.set_json(&keys::graph_spec(&graph), &spec)
                        .await
                        .map_err(failed)?;
                    kv.hset_json(
                        keys::CLUSTER_GRAPHS,
                        &graph.to_string(),
                        &GraphIndexEntry {
                            tenant: spec.tenant.clone(),
                            graph: spec.graph.clone(),
                            version: spec.version,
                            updated_ms: EPOCH_MS,
                            deleting: false,
                        },
                    )
                    .await
                    .map_err(failed)?;
                    kv.incr(keys::CLUSTER_EPOCH).await.map_err(failed)?;
                    "resubmit"
                }
            };
            *env.operator_actions.lock().entry(kind).or_default() += 1;
        }
        desire(RunState::Running, None).await.map_err(failed)?;
        ctx.shutdown().cancelled().await;
        Ok(())
    }
}

/// Watches the commit logs while the run is in progress and records anything
/// that goes backwards: an epoch, a position or a watermark of a slice must
/// never decrease, whoever commits and however often workers are replaced.
pub struct Auditor {
    slot: EnvSlot,
}

impl Auditor {
    /// An auditor for the current run.
    #[must_use]
    pub fn new(slot: EnvSlot) -> Self {
        Self { slot }
    }
}

/// `t:<tenant>:g:<graph>:s:<slice>:log` -> `tenant/graph/slice`.
fn slice_of_log(key: &str) -> Option<streaming_core::SliceKey> {
    let rest = key.strip_prefix("t:")?.strip_suffix(":log")?;
    let (tenant, rest) = rest.split_once(":g:")?;
    let (graph, slice) = rest.split_once(":s:")?;
    format!("{tenant}/{graph}/{slice}").parse().ok()
}

#[async_trait]
impl Workload for Auditor {
    fn name(&self) -> &str {
        "auditor"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let env = env_of(&self.slot)?;
        let kv: DynKv = env.kv.clone();
        let mut seen: BTreeMap<String, SliceHead> = BTreeMap::new();
        loop {
            moonpool_sim::select! {
                () = ctx.shutdown().cancelled() => return Ok(()),
                _ = ctx.time().sleep(Duration::from_millis(100)) => {}
            }
            *env.audits.lock() += 1;
            for key in env.kv.keys() {
                let Some(slice) = slice_of_log(&key) else {
                    continue;
                };
                let Ok(Some(head)) = slice_head(kv.as_ref(), &slice).await else {
                    continue;
                };
                if let Some(before) = seen.get(&key) {
                    let backwards = head.epoch < before.epoch
                        || head.position < before.position
                        || head.watermark_ms < before.watermark_ms
                        || head.sealed < before.sealed
                        || (head.epoch == before.epoch && head.owner != before.owner);
                    if backwards {
                        env.audit.lock().push(format!(
                            "slice {slice} went backwards: {before:?} then {head:?}"
                        ));
                    }
                }
                seen.insert(key, head);
            }
        }
    }
}
