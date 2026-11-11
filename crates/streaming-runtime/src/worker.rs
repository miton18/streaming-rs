//! A worker: the process-level loop that makes the local actors match what
//! the KV says should run here, and reports back.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_core::{Providers, RandomProvider, TimeProvider};
use streaming_core::control::{
    ActorKind, ActorState, DesiredState, GraphIndexEntry, GraphPlan, MemberInfo, Placement,
    RunState, SliceSnapshot, TenantUsage, WorkerSnapshot,
};
use streaming_core::{
    DynKv, GraphKey, GraphSpec, KvError, NodeId, SliceKey, TenantId, Timestamp, Topology, WorkerId,
    keys,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::RuntimeEnv;
use crate::controller::{self, ControllerState};
use crate::load::{LoadProbe, ProcessLoad};
use crate::metrics::{ActorSample, SliceLifetime, SliceSample, snapshot_actor, snapshot_slice};
use crate::scope::TaskScope;
use crate::slice::{Control, SliceExit, SliceParams, SliceShared, run_slice};

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
    async fn alive(&self, now_ms: Timestamp, timeout_ms: i64) -> Vec<MemberInfo>;
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
        if let Err(error) = self
            .kv
            .hset_json(keys::CLUSTER_MEMBERS, me.worker.as_str(), me)
            .await
        {
            tracing::warn!(%error, worker = %me.worker, "heartbeat failed");
        }
    }

    async fn alive(&self, now_ms: Timestamp, timeout_ms: i64) -> Vec<MemberInfo> {
        self.kv
            .hgetall_json::<MemberInfo>(keys::CLUSTER_MEMBERS)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|(_, member)| member)
            .filter(|member| member.is_alive(now_ms, timeout_ms))
            .collect()
    }

    async fn leave(&self, worker: &WorkerId) {
        let _ = self
            .kv
            .hdel(keys::CLUSTER_MEMBERS, vec![worker.to_string()])
            .await;
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
    /// Whether this worker may act as controller.
    pub controller: bool,
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
            controller: true,
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
        let placement: Placement = kv
            .get_json(keys::CLUSTER_PLACEMENT)
            .await?
            .unwrap_or_default();
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
            let desired: DesiredState = kv
                .get_json(&keys::graph_desired(&graph))
                .await?
                .unwrap_or_default();
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
                if self.config.ignore_placement
                    || synced.placement.assignments.get(&key) == Some(me)
                {
                    target.insert(key, docs);
                }
            }
        }

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
                    book.last_error =
                        Some("fenced: another worker committed this slice".to_owned());
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

        for (key, slice) in running.iter() {
            match target.get(key) {
                Some(docs) if docs.spec_version == slice.spec_version => {
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
            if running.contains_key(key)
                || !matches!(docs.desired.state, RunState::Running | RunState::Paused)
            {
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
                    done,
                    actor_samples: BTreeMap::new(),
                    slice_sample: None,
                },
            );
        }
        books.retain(|key, _| target.contains_key(key) || running.contains_key(key));
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
                let (snapshot, sample) = snapshot_actor(
                    &metrics,
                    slice.actor_samples.get(&id).copied(),
                    now,
                    interval,
                );
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
                    watermark_ms: None,
                    commits: 0,
                    commit_ms: 0.0,
                    in_flight_epochs: 0,
                    fenced: book.lifetime.fenced,
                    restarts: book.lifetime.restarts,
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
        let mut controller_state = ControllerState::default();
        let mut catalog_published = false;
        let mut last_tick = time.now();

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

            if self.config.controller {
                let mut alive = self
                    .membership
                    .alive(self.env.clock.now_ms(), timeout_ms)
                    .await;
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
