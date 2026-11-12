//! A worker: the process-level loop that makes the local actors match what
//! the KV says should run here, and reports back.

use crate::{
    RuntimeEnv,
    controller::{self, ControllerState},
    load::{LoadProbe, ProcessLoad},
    metrics::{ActorSample, SliceLifetime, SliceSample, snapshot_actor, snapshot_slice},
    scope::TaskScope,
    slice::{Control, SliceExit, SliceParams, SliceShared, run_slice},
};
use async_trait::async_trait;
use moonpool_core::{Providers, RandomProvider, TimeProvider};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use streaming_core::{
    DynKv, GraphKey, GraphSpec, KvError, NodeId, SliceKey, TenantId, Timestamp, Topology, WorkerId,
    control::{
        ActorKind, ActorState, DesiredState, GraphIndexEntry, GraphPlan, MemberInfo, Placement, RunState,
        SliceSnapshot, TenantUsage, WorkerSnapshot,
    },
    keys,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// How workers learn about each other.
///
/// Membership only drives who acts as controller and where slices are placed.
/// It is never relied upon for correctness: two workers disagreeing on who is
/// alive can at worst run the same slice twice, which the commit log settles.
#[async_trait]
pub trait Membership: Send + Sync + 'static {
    /// Advertise this worker and its load.
    async fn announce(&self, me: &MemberInfo);
    /// Workers currently believed alive. May or may not include the caller.
    /// `None` when that cannot be told right now: not knowing who is there
    /// must never be mistaken for being alone.
    async fn alive(&self, now_ms: Timestamp, timeout_ms: i64) -> Option<Vec<MemberInfo>>;
    /// Withdraw a worker that is shutting down cleanly.
    async fn leave(&self, worker: &WorkerId);
}

/// Membership through heartbeats written to the `cluster:members` hash.
pub struct KvMembership {
    kv: DynKv,
}

impl KvMembership {
    /// Membership stored in `kv`.
    #[must_use]
    pub fn new(kv: DynKv) -> Self {
        Self { kv }
    }
}

#[async_trait]
impl Membership for KvMembership {
    async fn announce(&self, me: &MemberInfo) {
        if let Err(error) = self.kv.hset_json(keys::CLUSTER_MEMBERS, me.worker.as_str(), me).await {
            tracing::warn!(%error, worker = %me.worker, "heartbeat failed");
        }
    }

    async fn alive(&self, now_ms: Timestamp, timeout_ms: i64) -> Option<Vec<MemberInfo>> {
        let members = self.kv.hgetall_json::<MemberInfo>(keys::CLUSTER_MEMBERS).await.ok()?;
        Some(
            members
                .into_iter()
                .map(|(_, member)| member)
                .filter(|member| member.is_alive(now_ms, timeout_ms))
                .collect(),
        )
    }

    async fn leave(&self, worker: &WorkerId) {
        let _ = self.kv.hdel(keys::CLUSTER_MEMBERS, vec![worker.to_string()]).await;
    }
}

/// Settings of a worker.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Identity of the worker in the cluster.
    pub worker: WorkerId,
    /// Address of the HTTP API served next to this worker, if any.
    pub api_addr: Option<String>,
    /// Period of the heartbeat, of the reconciliation and of metrics publication.
    pub tick: Duration,
    /// How long without heartbeat before a worker is considered dead.
    pub member_timeout: Duration,
    /// Base delay before restarting a slice that failed or was fenced.
    pub restart_backoff: Duration,
    /// Minimum time between two load-driven moves of a slice.
    pub rebalance_cooldown: Duration,
    /// How often the controller asks the sources again for their partitions.
    pub replan_interval: Duration,
    /// Whether this worker may act as controller.
    pub controller: bool,
    /// How long after its start a worker waits before acting as controller,
    /// so that it first learns who else is there. Without it, a cluster
    /// starting up would see its first worker place everything on itself.
    pub controller_grace: Duration,
    /// Turn an actor panic into a slice restart instead of letting it unwind.
    /// On in production so that one tenant's operator cannot take the process
    /// down; off in simulation so that a bug fails the run.
    pub catch_panics: bool,
    /// Run every slice of every graph regardless of the placement. Only for
    /// tests of the commit protocol: several workers then fight over each slice.
    pub ignore_placement: bool,
}

impl WorkerConfig {
    /// Defaults suitable for production.
    #[must_use]
    pub fn new(worker: WorkerId) -> Self {
        Self {
            worker,
            api_addr: None,
            tick: Duration::from_secs(1),
            member_timeout: Duration::from_secs(5),
            restart_backoff: Duration::from_secs(1),
            rebalance_cooldown: Duration::from_secs(30),
            replan_interval: Duration::from_secs(30),
            controller: true,
            controller_grace: Duration::from_secs(3),
            catch_panics: true,
            ignore_placement: false,
        }
    }
}

struct GraphDocs {
    topology: Arc<Topology>,
    plan: Arc<GraphPlan>,
    desired: DesiredState,
    spec_version: u64,
}

struct Synced {
    epoch: i64,
    age: u32,
    graphs: BTreeMap<GraphKey, GraphDocs>,
    placement: Placement,
}

struct Running {
    stop: CancellationToken,
    control: watch::Sender<Control>,
    shared: SliceShared,
    spec_version: u64,
    plan_generation: u64,
    done: Arc<AtomicBool>,
    actor_samples: BTreeMap<(NodeId, ActorKind), ActorSample>,
    slice_sample: Option<SliceSample>,
}

#[derive(Default)]
struct Book {
    lifetime: SliceLifetime,
    started: bool,
    /// Consecutive failures without a single commit in between.
    failures: u32,
    retry_at: Option<Duration>,
    last_error: Option<String>,
}

/// Number of ticks after which documents are re-read even if the cluster
/// epoch did not move, as a safety net against a lost epoch bump.
const RESYNC_TICKS: u32 = 30;

/// A worker process.
pub struct Worker<P: Providers> {
    env: Arc<RuntimeEnv<P>>,
    config: WorkerConfig,
    load: Arc<dyn LoadProbe>,
    membership: Arc<dyn Membership>,
}

impl<P: Providers> Worker<P> {
    /// A worker using KV heartbeats for membership and `/proc` for its load.
    pub fn new(env: RuntimeEnv<P>, config: WorkerConfig) -> Self {
        let membership = Arc::new(KvMembership::new(env.kv.clone()));
        Self {
            env: Arc::new(env),
            config,
            load: Arc::new(ProcessLoad::new()),
            membership,
        }
    }

    /// Replace the load probe.
    #[must_use]
    pub fn with_load_probe(mut self, probe: Arc<dyn LoadProbe>) -> Self {
        self.load = probe;
        self
    }

    /// Replace the membership mechanism.
    #[must_use]
    pub fn with_membership(mut self, membership: Arc<dyn Membership>) -> Self {
        self.membership = membership;
        self
    }

    async fn publish_catalog(&self) -> Result<(), KvError> {
        let mut entries = Vec::new();
        for (op, descriptor) in self.env.registry.catalog().ops {
            entries.push((
                op.to_string(),
                streaming_core::kv::encode_json(keys::CLUSTER_CATALOG, &descriptor)?,
            ));
        }
        self.env.kv.hset(keys::CLUSTER_CATALOG, entries).await
    }

    async fn sync(&self, synced: &mut Option<Synced>) -> Result<(), KvError> {
        let kv = self.env.kv.as_ref();
        let me = &self.config.worker;
        let epoch = kv
            .get(keys::CLUSTER_EPOCH)
            .await?
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .and_then(|text| text.parse::<i64>().ok())
            .unwrap_or(0);
        if let Some(current) = synced.as_mut()
            && current.epoch == epoch
            && current.age < RESYNC_TICKS
        {
            current.age += 1;
            return Ok(());
        }
        let index: Vec<(String, GraphIndexEntry)> = kv.hgetall_json(keys::CLUSTER_GRAPHS).await?;
        let placement: Placement = kv.get_json(keys::CLUSTER_PLACEMENT).await?.unwrap_or_default();
        let catalog = self.env.registry.catalog();
        let mut graphs = BTreeMap::new();
        for (_, entry) in index {
            if entry.deleting {
                continue;
            }
            let graph = GraphKey::new(entry.tenant, entry.graph);
            let mine = self.config.ignore_placement
                || placement
                    .assignments
                    .iter()
                    .any(|(slice, worker)| worker == me && slice.graph_key() == graph);
            if !mine {
                continue;
            }
            let Some(spec) = kv.get_json::<GraphSpec>(&keys::graph_spec(&graph)).await? else {
                continue;
            };
            let Some(plan) = kv.get_json::<GraphPlan>(&keys::graph_plan(&graph)).await? else {
                continue;
            };
            if plan.error.is_some() || plan.spec_version != spec.version {
                continue;
            }
            let desired: DesiredState = kv.get_json(&keys::graph_desired(&graph)).await?.unwrap_or_default();
            match Topology::build(&spec, &catalog) {
                Ok(topology) => {
                    graphs.insert(
                        graph,
                        GraphDocs {
                            spec_version: spec.version,
                            topology: Arc::new(topology),
                            plan: Arc::new(plan),
                            desired,
                        },
                    );
                }
                Err(error) => {
                    tracing::warn!(%graph, %error, "graph cannot run on this worker");
                }
            }
        }
        *synced = Some(Synced {
            epoch,
            age: 0,
            graphs,
            placement,
        });
        Ok(())
    }

    fn reconcile(
        &self,
        scope: &TaskScope<P>,
        synced: &Synced,
        running: &mut BTreeMap<SliceKey, Running>,
        books: &mut BTreeMap<SliceKey, Book>,
        now: Duration,
    ) {
        let me = &self.config.worker;
        let mut target: BTreeMap<SliceKey, &GraphDocs> = BTreeMap::new();
        for (graph, docs) in &synced.graphs {
            if docs.desired.state == RunState::Stopped {
                continue;
            }
            for slice in &docs.plan.slices {
                let key = SliceKey {
                    tenant: graph.tenant.clone(),
                    graph: graph.graph.clone(),
                    slice: slice.clone(),
                };
                if self.config.ignore_placement || synced.placement.assignments.get(&key) == Some(me) {
                    target.insert(key, docs);
                }
            }
        }

        self.reap(running, books, now);

        for (key, slice) in running.iter() {
            match target.get(key) {
                Some(docs)
                    if docs.spec_version == slice.spec_version && docs.plan.generation == slice.plan_generation =>
                {
                    let wanted = Control {
                        run: docs.desired.state,
                        paused_nodes: docs.desired.paused_nodes.clone(),
                    };
                    slice.control.send_if_modified(|current| {
                        let changed = *current != wanted;
                        if changed {
                            *current = wanted;
                        }
                        changed
                    });
                }
                _ => slice.stop.cancel(),
            }
        }

        for (key, docs) in &target {
            if running.contains_key(key) || !matches!(docs.desired.state, RunState::Running | RunState::Paused) {
                continue;
            }
            let book = books.entry(key.clone()).or_default();
            if book.retry_at.is_some_and(|at| now < at) {
                continue;
            }
            if book.started {
                book.lifetime.restarts += 1;
            }
            book.started = true;
            book.retry_at = None;

            let slice_scope = scope.child();
            let stop = slice_scope.token().clone();
            let (control, control_rx) = watch::channel(Control {
                run: docs.desired.state,
                paused_nodes: docs.desired.paused_nodes.clone(),
            });
            let shared = SliceShared::new();
            let done = Arc::new(AtomicBool::new(false));
            let params = SliceParams {
                env: self.env.clone(),
                key: key.clone(),
                topology: docs.topology.clone(),
                plan: docs.plan.clone(),
                worker: me.clone(),
                catch_panics: self.config.catch_panics,
            };
            let task = {
                let shared = shared.clone();
                let done = done.clone();
                async move {
                    run_slice(params, slice_scope, control_rx, shared).await;
                    done.store(true, Ordering::Relaxed);
                }
            };
            drop(scope.spawn(&format!("slice/{key}"), task));
            running.insert(
                key.clone(),
                Running {
                    stop,
                    control,
                    shared,
                    spec_version: docs.spec_version,
                    plan_generation: docs.plan.generation,
                    done,
                    actor_samples: BTreeMap::new(),
                    slice_sample: None,
                },
            );
        }
        books.retain(|key, _| target.contains_key(key) || running.contains_key(key));
    }

    /// Book the slice instances that ended: why, and when to start them again.
    fn reap(&self, running: &mut BTreeMap<SliceKey, Running>, books: &mut BTreeMap<SliceKey, Book>, now: Duration) {
        let finished: Vec<SliceKey> = running
            .iter()
            .filter(|(_, slice)| slice.done.load(Ordering::Relaxed))
            .map(|(key, _)| key.clone())
            .collect();
        for key in finished {
            let Some(slice) = running.remove(&key) else {
                continue;
            };
            let book = books.entry(key.clone()).or_default();
            if slice.shared.metrics.commits.load(Ordering::Relaxed) > 0 {
                book.failures = 0;
            }
            // Back off exponentially while a slice keeps failing before its
            // first commit, so that a broken graph does not spin.
            let jitter = 1.0 + self.env.providers.random().random_ratio();
            let factor = f64::from(1_u32 << book.failures.min(5));
            let retry_at = now + self.config.restart_backoff.mul_f64(jitter * factor);
            match slice.shared.exit() {
                Some(SliceExit::Fenced) => {
                    book.lifetime.fenced += 1;
                    book.failures += 1;
                    book.retry_at = Some(retry_at);
                    book.last_error = Some("fenced: another worker committed this slice".to_owned());
                    tracing::info!(slice = %key, "slice fenced");
                }
                Some(SliceExit::Failed(error)) => {
                    tracing::warn!(slice = %key, %error, "slice failed");
                    book.failures += 1;
                    book.retry_at = Some(retry_at);
                    book.last_error = Some(error);
                }
                _ => book.last_error = None,
            }
        }
    }

    fn snapshot(
        &self,
        running: &mut BTreeMap<SliceKey, Running>,
        books: &BTreeMap<SliceKey, Book>,
        now: Duration,
        interval: Duration,
        load: streaming_core::control::Load,
    ) -> WorkerSnapshot {
        let mut actors = Vec::new();
        let mut slices = Vec::new();
        let mut tenants: BTreeMap<TenantId, TenantUsage> = BTreeMap::new();
        for (key, slice) in running.iter_mut() {
            let live = slice.shared.actors.lock().clone();
            let mut samples = BTreeMap::new();
            for metrics in live {
                let id = (metrics.ident.node.clone(), metrics.ident.kind);
                let (snapshot, sample) = snapshot_actor(&metrics, slice.actor_samples.get(&id).copied(), now, interval);
                samples.insert(id, sample);
                let usage = tenants.entry(key.tenant.clone()).or_default();
                usage.actors += 1;
                usage.busy += snapshot.busy;
                if snapshot.kind == ActorKind::Source {
                    usage.in_per_sec += snapshot.out_per_sec;
                }
                actors.push(snapshot);
            }
            slice.actor_samples = samples;
            let lifetime = books.get(key).map(|book| book.lifetime).unwrap_or_default();
            let (snapshot, sample) = snapshot_slice(
                &key.tenant,
                &key.graph,
                &key.slice,
                &slice.shared.metrics,
                lifetime,
                slice.slice_sample,
            );
            slice.slice_sample = Some(sample);
            slices.push(snapshot);
        }
        // Slices assigned here but waiting for a restart stay visible.
        for (key, book) in books {
            if !running.contains_key(key) && book.last_error.is_some() {
                slices.push(SliceSnapshot {
                    tenant: key.tenant.clone(),
                    graph: key.graph.clone(),
                    slice: key.slice.clone(),
                    state: ActorState::Failed,
                    epoch: 0,
                    position: None,
                    window: None,
                    watermark_ms: None,
                    commits: 0,
                    commit_ms: 0.0,
                    in_flight_epochs: 0,
                    fenced: book.lifetime.fenced,
                    restarts: book.lifetime.restarts,
                    throttled: 0,
                    last_error: book.last_error.clone(),
                });
            }
        }
        WorkerSnapshot {
            worker: self.config.worker.clone(),
            at_ms: self.env.clock.now_ms(),
            interval_ms: interval.as_millis() as u64,
            load,
            actors,
            slices,
            tenants,
        }
    }

    /// Run until `shutdown` is cancelled.
    ///
    /// Dropping the returned future stops every task of the worker at its
    /// current await point, exactly like a crash of the process would.
    pub async fn run(self, shutdown: CancellationToken) {
        let kill = CancellationToken::new();
        let _kill_on_drop = kill.clone().drop_guard();
        let scope = TaskScope::new(self.env.providers.clone(), kill.clone());
        let time = self.env.providers.time().clone();
        let kv = self.env.kv.clone();
        let me = self.config.worker.clone();
        let generation = self.env.clock.now_ms().max(0) as u64;
        let timeout_ms = self.config.member_timeout.as_millis() as i64;

        let mut running: BTreeMap<SliceKey, Running> = BTreeMap::new();
        let mut books: BTreeMap<SliceKey, Book> = BTreeMap::new();
        let mut synced: Option<Synced> = None;
        let mut controller_state = ControllerState::new(self.config.replan_interval);
        let mut catalog_published = false;
        let mut last_tick = time.now();
        let started = last_tick;

        loop {
            if !catalog_published {
                match self.publish_catalog().await {
                    Ok(()) => catalog_published = true,
                    Err(error) => tracing::warn!(%error, "cannot publish the operator catalog"),
                }
            }
            if let Err(error) = self.sync(&mut synced).await {
                tracing::warn!(%error, "cannot read the control plane");
            }
            let now = time.now();
            if let Some(synced) = &synced {
                self.reconcile(&scope, synced, &mut running, &mut books, now);
            }

            let interval = now.saturating_sub(last_tick).max(Duration::from_millis(1));
            last_tick = now;
            let load = self.load.load();
            let snapshot = self.snapshot(&mut running, &books, now, interval, load);
            let info = MemberInfo {
                worker: me.clone(),
                generation,
                heartbeat_ms: self.env.clock.now_ms(),
                load,
                actors: snapshot.actors.len() as u32,
                slices: running.len() as u32,
                api_addr: self.config.api_addr.clone(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
            };
            self.membership.announce(&info).await;
            if let Err(error) = kv.set_json(&keys::worker_metrics(&me), &snapshot).await {
                tracing::warn!(%error, "cannot publish metrics");
            }

            // A worker that cannot tell who is alive leaves the controller's
            // work alone for this round: taking a failed read for an empty
            // cluster would make it claim every slice.
            let alive = if self.config.controller && now.saturating_sub(started) >= self.config.controller_grace {
                self.membership.alive(self.env.clock.now_ms(), timeout_ms).await
            } else {
                None
            };
            if let Some(mut alive) = alive {
                alive.retain(|member| member.worker != me);
                alive.push(info);
                alive.sort_by(|a, b| a.worker.cmp(&b.worker));
                if alive.first().is_some_and(|leader| leader.worker == me)
                    && let Err(error) = controller::run_once(
                        &self.env,
                        &me,
                        &alive,
                        &mut controller_state,
                        self.config.tick * 3,
                        self.config.rebalance_cooldown,
                    )
                    .await
                {
                    tracing::warn!(%error, "controller round failed");
                }
            }

            moonpool_core::select! {
                biased;
                () = shutdown.cancelled() => break,
                _ = time.sleep(self.config.tick) => {}
            }
        }

        kill.cancel();
        self.membership.leave(&me).await;
        let _ = kv.del(vec![keys::worker_metrics(&me)]).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        memlog::MemLog,
        metrics::{ActorIdent, ActorMetrics},
    };
    use moonpool_core::TokioProviders;
    use std::sync::atomic::AtomicU64;
    use streaming_core::{
        GraphId, Kv, MemKv, NoSecrets, Registry, SliceId, SystemClock, TenantId, TypeTag, control::GraphIndexEntry,
    };

    /// Counts the reads of the cluster epoch and of the graph index.
    struct Counting {
        inner: MemKv,
        epochs: AtomicU64,
        indexes: AtomicU64,
    }

    #[async_trait]
    impl Kv for Counting {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> {
            if key == keys::CLUSTER_EPOCH {
                self.epochs.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get(key).await
        }
        async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), KvError> {
            self.inner.set(key, value).await
        }
        async fn del(&self, keys: Vec<String>) -> Result<(), KvError> {
            self.inner.del(keys).await
        }
        async fn incr(&self, key: &str) -> Result<i64, KvError> {
            self.inner.incr(key).await
        }
        async fn hsetnx(&self, key: &str, field: &str, value: Vec<u8>) -> Result<bool, KvError> {
            self.inner.hsetnx(key, field, value).await
        }
        async fn hset(&self, key: &str, entries: Vec<(String, Vec<u8>)>) -> Result<(), KvError> {
            self.inner.hset(key, entries).await
        }
        async fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>, KvError> {
            self.inner.hget(key, field).await
        }
        async fn hgetall(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>, KvError> {
            if key == keys::CLUSTER_GRAPHS {
                self.indexes.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.hgetall(key).await
        }
        async fn hdel(&self, key: &str, fields: Vec<String>) -> Result<(), KvError> {
            self.inner.hdel(key, fields).await
        }
    }

    fn me() -> WorkerId {
        WorkerId::new("worker-a").unwrap()
    }

    fn registry(log: &Arc<MemLog>) -> Registry<TokioProviders> {
        let mut registry = Registry::<TokioProviders>::standard();
        let log = log.clone();
        registry.register_source::<u64>("test/log@1", "", move |context, _| {
            Ok(log.source(context.providers.clone()))
        });
        registry.register_sink("test/broken@1", "", TypeTag::Any, |_, _| {
            Err(streaming_core::OpError::Config("broken on purpose".into()))
        });
        registry
    }

    fn worker(kv: DynKv, config: WorkerConfig) -> Worker<TokioProviders> {
        let log = MemLog::new(1);
        let mut registry = registry(&log);
        crate::builtin::register(&mut registry, kv.clone());
        Worker::new(
            RuntimeEnv {
                providers: TokioProviders::new(),
                kv,
                clock: Arc::new(SystemClock),
                secrets: Arc::new(NoSecrets),
                registry: Arc::new(registry),
            },
            config,
        )
        .with_load_probe(Arc::new(crate::NoLoad))
    }

    fn config() -> WorkerConfig {
        let mut config = WorkerConfig::new(me());
        config.tick = Duration::from_millis(10);
        config.restart_backoff = Duration::from_millis(100);
        config.controller_grace = Duration::ZERO;
        config
    }

    fn slice_key(graph: &str) -> SliceKey {
        format!("acme/{graph}/src.events.0").parse().unwrap()
    }

    fn graph_key(graph: &str) -> GraphKey {
        GraphKey::new(TenantId::new("acme").unwrap(), GraphId::new(graph).unwrap())
    }

    fn spec(graph: &str, version: u64, sink: &str) -> GraphSpec {
        serde_json::from_value(serde_json::json!({
            "tenant": "acme",
            "graph": graph,
            "version": version,
            "nodes": [
                {"id": "events", "op": "test/log@1"},
                {"id": "out", "op": sink}
            ],
            "edges": [["events", "out"]]
        }))
        .unwrap()
    }

    /// Store a graph, its plan and its placement on `worker`.
    async fn store(kv: &dyn Kv, graph: &str, spec_version: u64, plan_version: u64, worker: &str) {
        let key = graph_key(graph);
        kv.set_json(&keys::graph_spec(&key), &spec(graph, spec_version, "test/broken@1"))
            .await
            .unwrap();
        let plan = GraphPlan {
            spec_version: plan_version,
            slices: vec!["src.events.0".parse::<SliceId>().unwrap()],
            generation: 1,
            ..GraphPlan::default()
        };
        kv.set_json(&keys::graph_plan(&key), &plan).await.unwrap();
        kv.hset_json(
            keys::CLUSTER_GRAPHS,
            &key.to_string(),
            &GraphIndexEntry {
                tenant: key.tenant.clone(),
                graph: key.graph.clone(),
                version: spec_version,
                updated_ms: 0,
                deleting: false,
            },
        )
        .await
        .unwrap();
        let mut placement: Placement = kv.get_json(keys::CLUSTER_PLACEMENT).await.unwrap().unwrap_or_default();
        placement
            .assignments
            .insert(slice_key(graph), WorkerId::new(worker).unwrap());
        kv.set_json(keys::CLUSTER_PLACEMENT, &placement).await.unwrap();
    }

    fn finished(exit: Option<SliceExit>, commits: u64) -> Running {
        let shared = SliceShared::new();
        if let Some(exit) = exit {
            shared.set_exit(exit);
        }
        shared.metrics.commits.store(commits, Ordering::Relaxed);
        Running {
            stop: CancellationToken::new(),
            control: watch::channel(Control {
                run: RunState::Running,
                paused_nodes: Default::default(),
            })
            .0,
            shared,
            spec_version: 1,
            plan_generation: 1,
            done: Arc::new(AtomicBool::new(true)),
            actor_samples: BTreeMap::new(),
            slice_sample: None,
        }
    }

    #[tokio::test]
    async fn ended_slices_are_booked_with_an_exponential_backoff() {
        let w = worker(MemKv::shared(), config());
        let base = Duration::from_millis(100);
        let now = Duration::from_secs(100);
        let cases = [
            // (exit, commits, failures before, failures after, factor, error)
            (Some(SliceExit::Fenced), 0, 2, 3, 4, Some("fenced")),
            (Some(SliceExit::Failed("boom".into())), 0, 7, 8, 32, Some("boom")),
            // A slice that committed before failing starts afresh.
            (Some(SliceExit::Failed("boom".into())), 1, 3, 1, 1, Some("boom")),
            (Some(SliceExit::Stopped), 0, 2, 2, 0, None),
            (None, 0, 2, 2, 0, None),
        ];
        for (exit, commits, before, after, factor, error) in cases {
            let key = slice_key("g");
            let mut running = BTreeMap::from([(key.clone(), finished(exit, commits))]);
            let mut books = BTreeMap::from([(
                key.clone(),
                Book {
                    failures: before,
                    last_error: Some("old".into()),
                    ..Book::default()
                },
            )]);
            w.reap(&mut running, &mut books, now);
            assert!(running.is_empty());
            let book = &books[&key];
            assert_eq!(book.failures, after, "{error:?}");
            assert_eq!(book.last_error.as_deref().map(|e| e.split(':').next().unwrap()), error,);
            assert_eq!(book.lifetime.fenced, u64::from(error == Some("fenced")));
            match book.retry_at {
                None => assert_eq!(factor, 0),
                Some(at) => {
                    let delay = at - now;
                    assert!(
                        delay >= base * factor && delay <= base * factor * 2,
                        "{delay:?} for a factor of {factor}"
                    );
                }
            }
        }
    }

    fn actor(kind: ActorKind, node: &str, records_out: u64) -> Arc<ActorMetrics> {
        let metrics = ActorMetrics::new(
            ActorIdent {
                tenant: TenantId::new("acme").unwrap(),
                graph: GraphId::new("g").unwrap(),
                slice: "src.events.0".parse().unwrap(),
                node: NodeId::new(node).unwrap(),
                kind,
                op: streaming_core::OpRef::new("test/x@1").unwrap(),
            },
            Duration::ZERO,
        );
        metrics.records_out.store(records_out, Ordering::Relaxed);
        metrics.switch(Duration::from_millis(500), crate::metrics::Phase::Busy);
        metrics
    }

    #[test]
    fn snapshots_add_up_tenant_usage_and_show_waiting_slices() {
        let w = worker(MemKv::shared(), config());
        let running_slice = finished(None, 0);
        running_slice.shared.actors.lock().extend([
            actor(ActorKind::Source, "events", 30),
            actor(ActorKind::Map, "decode", 1000),
            actor(ActorKind::Sink, "out", 7),
        ]);
        let mut running = BTreeMap::from([(slice_key("g"), running_slice)]);
        let waiting = slice_key("h");
        let books = BTreeMap::from([
            (
                waiting.clone(),
                Book {
                    last_error: Some("boom".into()),
                    lifetime: SliceLifetime { fenced: 2, restarts: 3 },
                    ..Book::default()
                },
            ),
            // Assigned and healthy, not running yet: not shown.
            (slice_key("i"), Book::default()),
        ]);
        let snapshot = w.snapshot(
            &mut running,
            &books,
            Duration::from_secs(1),
            Duration::from_secs(1),
            streaming_core::control::Load::default(),
        );
        let usage = &snapshot.tenants[&TenantId::new("acme").unwrap()];
        assert_eq!(usage.actors, 3);
        // Busy half of the second each.
        assert!((usage.busy - 1.5).abs() < 1e-9, "{usage:?}");
        // What the sources read.
        assert!((usage.in_per_sec - 30.0).abs() < 1e-9, "{usage:?}");
        assert_eq!(snapshot.slices.len(), 2);
        let shown = snapshot
            .slices
            .iter()
            .find(|slice| slice.graph.as_str() == "h")
            .unwrap();
        assert_eq!(shown.state, ActorState::Failed);
        assert_eq!(shown.last_error.as_deref(), Some("boom"));
        assert_eq!((shown.fenced, shown.restarts), (2, 3));
    }

    #[tokio::test]
    async fn documents_are_read_again_on_epoch_changes_or_every_thirty_ticks() {
        let kv = Arc::new(Counting {
            inner: MemKv::new(),
            epochs: AtomicU64::new(0),
            indexes: AtomicU64::new(0),
        });
        let w = worker(kv.clone(), config());
        let mut synced = None;
        for _ in 0..94 {
            w.sync(&mut synced).await.unwrap();
        }
        assert_eq!(kv.epochs.load(Ordering::Relaxed), 94);
        // Calls 1, 32, 63 and 94.
        assert_eq!(kv.indexes.load(Ordering::Relaxed), 4);
        kv.inner.incr(keys::CLUSTER_EPOCH).await.unwrap();
        w.sync(&mut synced).await.unwrap();
        assert_eq!(kv.indexes.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn only_runnable_graphs_placed_here_are_synced() {
        let kv: DynKv = MemKv::shared();
        store(kv.as_ref(), "mine", 1, 1, "worker-a").await;
        store(kv.as_ref(), "theirs", 1, 1, "worker-b").await;
        store(kv.as_ref(), "stale", 2, 1, "worker-a").await;
        let mut failed = GraphPlan {
            spec_version: 1,
            error: Some("cannot plan".into()),
            ..GraphPlan::default()
        };
        store(kv.as_ref(), "failed", 1, 1, "worker-a").await;
        failed.slices = vec!["src.events.0".parse().unwrap()];
        kv.set_json(&keys::graph_plan(&graph_key("failed")), &failed)
            .await
            .unwrap();
        store(kv.as_ref(), "gone", 1, 1, "worker-a").await;
        let mut entry: GraphIndexEntry = kv
            .hget_json(keys::CLUSTER_GRAPHS, &graph_key("gone").to_string())
            .await
            .unwrap()
            .unwrap();
        entry.deleting = true;
        kv.hset_json(keys::CLUSTER_GRAPHS, &graph_key("gone").to_string(), &entry)
            .await
            .unwrap();
        let w = worker(kv.clone(), config());
        let mut synced = None;
        w.sync(&mut synced).await.unwrap();
        let graphs: Vec<String> = synced
            .unwrap()
            .graphs
            .keys()
            .map(|graph| graph.graph.to_string())
            .collect();
        assert_eq!(graphs, ["mine"]);
        // Unless placement is ignored.
        let mut config = config();
        config.ignore_placement = true;
        let w = worker(kv.clone(), config);
        let mut synced = None;
        w.sync(&mut synced).await.unwrap();
        assert_eq!(synced.unwrap().graphs.len(), 2);
    }

    #[tokio::test]
    async fn a_broken_slice_fails_restarts_and_shows_why_then_the_worker_leaves() {
        let kv: DynKv = MemKv::shared();
        let graph = graph_key("broken");
        kv.set_json(&keys::graph_spec(&graph), &spec("broken", 1, "test/broken@1"))
            .await
            .unwrap();
        kv.hset_json(
            keys::CLUSTER_GRAPHS,
            &graph.to_string(),
            &GraphIndexEntry {
                tenant: graph.tenant.clone(),
                graph: graph.graph.clone(),
                version: 1,
                updated_ms: 0,
                deleting: false,
            },
        )
        .await
        .unwrap();
        kv.incr(keys::CLUSTER_EPOCH).await.unwrap();
        let mut config = config();
        config.restart_backoff = Duration::from_millis(20);
        let w = worker(kv.clone(), config);
        let shutdown = CancellationToken::new();
        let run = tokio::spawn(w.run(shutdown.clone()));
        let metrics = keys::worker_metrics(&me());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let shown = loop {
            let snapshot: Option<WorkerSnapshot> = kv.get_json(&metrics).await.unwrap();
            if let Some(slice) = snapshot.and_then(|snapshot| {
                snapshot
                    .slices
                    .into_iter()
                    .find(|s| s.restarts >= 2 && s.state == ActorState::Failed)
            }) {
                break slice;
            }
            assert!(tokio::time::Instant::now() < deadline, "the slice never restarted");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert!(
            shown
                .last_error
                .as_deref()
                .is_some_and(|e| e.contains("broken on purpose")),
            "{shown:?}"
        );
        // The catalog is published.
        assert!(kv.hget(keys::CLUSTER_CATALOG, "std/count@1").await.unwrap().is_some());
        assert!(kv.hget(keys::CLUSTER_MEMBERS, me().as_str()).await.unwrap().is_some());
        shutdown.cancel();
        run.await.unwrap();
        assert!(kv.hget(keys::CLUSTER_MEMBERS, me().as_str()).await.unwrap().is_none());
        assert!(kv.get(&metrics).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_worker_that_may_not_control_places_nothing() {
        let kv: DynKv = MemKv::shared();
        store(kv.as_ref(), "g", 1, 1, "worker-a").await;
        kv.del(vec![keys::CLUSTER_PLACEMENT.to_owned()]).await.unwrap();
        let mut config = config();
        config.controller = false;
        let w = worker(kv.clone(), config);
        let shutdown = CancellationToken::new();
        let run = tokio::spawn(w.run(shutdown.clone()));
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(kv.get(keys::CLUSTER_PLACEMENT).await.unwrap().is_none());
        shutdown.cancel();
        run.await.unwrap();
    }

    #[tokio::test]
    async fn slices_are_started_steered_stopped_and_restarted() {
        let kv: DynKv = MemKv::shared();
        store(kv.as_ref(), "g", 1, 1, "worker-a").await;
        let w = worker(kv.clone(), config());
        let kill = CancellationToken::new();
        let scope = TaskScope::new(TokioProviders::new(), kill.clone());
        let mut synced = None;
        w.sync(&mut synced).await.unwrap();
        let mut synced = synced.unwrap();
        let key = slice_key("g");
        let (mut running, mut books) = (BTreeMap::new(), BTreeMap::new());
        let now = Duration::from_secs(10);
        w.reconcile(&scope, &synced, &mut running, &mut books, now);
        assert!(running.contains_key(&key));
        assert_eq!(books[&key].lifetime.restarts, 0);
        // Still running: nothing new.
        running.get_mut(&key).unwrap().done = Arc::new(AtomicBool::new(false));
        w.reconcile(&scope, &synced, &mut running, &mut books, now);
        assert_eq!(books[&key].lifetime.restarts, 0);
        // The control plane pauses the graph: the slice hears of it.
        let docs = synced.graphs.get_mut(&graph_key("g")).unwrap();
        docs.desired.state = RunState::Paused;
        w.reconcile(&scope, &synced, &mut running, &mut books, now);
        assert_eq!(running[&key].control.borrow().run, RunState::Paused);
        assert!(!running[&key].stop.is_cancelled());
        // A new version of the graph: the instance stops.
        let docs = synced.graphs.get_mut(&graph_key("g")).unwrap();
        docs.spec_version = 2;
        w.reconcile(&scope, &synced, &mut running, &mut books, now);
        assert!(running[&key].stop.is_cancelled());
        // It fails; it is started again once its backoff has passed.
        let docs = synced.graphs.get_mut(&graph_key("g")).unwrap();
        docs.spec_version = 1;
        running[&key].shared.set_exit(SliceExit::Failed("boom".into()));
        running[&key].done.store(true, Ordering::Relaxed);
        w.reconcile(&scope, &synced, &mut running, &mut books, now);
        assert!(!running.contains_key(&key));
        let at = books[&key].retry_at.unwrap();
        w.reconcile(&scope, &synced, &mut running, &mut books, at - Duration::from_millis(1));
        assert!(!running.contains_key(&key));
        w.reconcile(&scope, &synced, &mut running, &mut books, at);
        assert!(running.contains_key(&key));
        assert_eq!(books[&key].lifetime.restarts, 1);
        // A stopped graph runs nothing.
        let docs = synced.graphs.get_mut(&graph_key("g")).unwrap();
        docs.desired.state = RunState::Stopped;
        w.reconcile(&scope, &synced, &mut running, &mut books, at);
        assert!(running[&key].stop.is_cancelled());
        kill.cancel();
    }
}
