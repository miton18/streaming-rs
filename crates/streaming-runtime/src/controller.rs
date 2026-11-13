//! The controller: planning graphs and placing their slices on workers.
//!
//! One worker at a time acts as controller (the alive worker with the
//! smallest id). Nothing here is needed for correctness: if two workers
//! believe they are the controller, or if a placement is stale, the worst
//! outcome is two workers running the same slice for a while, which the
//! commit log arbitrates.

use crate::{
    RuntimeEnv,
    mutation::{Mutant, planted},
    state,
};
use moonpool_core::{Providers, TimeProvider};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use streaming_core::{
    GraphKey, GraphSpec, Kv, KvError, NodeSpec, OpContext, OpFactory, OpKind, SliceId, SliceKey, Topology, WorkerId,
    control::{DesiredState, GraphIndexEntry, GraphPlan, Load, MemberInfo, Placement, RunState},
    keys,
};

/// A worker as the placement policy sees it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Candidate {
    pub worker: WorkerId,
    pub load: Load,
    /// Graphs (`tenant/graph`) the worker steps aside from: it runs none of
    /// their slices.
    pub aside: BTreeSet<String>,
}

/// Place `slices` on `workers`.
///
/// The policy keeps a graph together and only spreads it on evidence:
///
/// 0. a slice only goes to, and only stays on, a worker that runs its graph:
///    one whose code declares the graph as the store does. While none does,
///    the slice is placed as if all did, and waits there;
/// 1. an assignment to a worker that is still alive is kept;
/// 2. an unassigned slice joins the worker already running most of its graph,
///    unless that worker is saturated; otherwise it goes to the worker with
///    the fewest slices;
/// 3. when `rebalance` is allowed and a worker is saturated while another is
///    not, one slice moves from the former to the latter. One move per call:
///    the caller spaces calls out so that the effect of a move is measured
///    before the next one.
///
/// Returns the assignments and whether rule 3 moved a slice.
pub(crate) fn assign(
    slices: &[SliceKey],
    workers: &[Candidate],
    current: &BTreeMap<SliceKey, WorkerId>,
    rebalance: bool,
) -> (BTreeMap<SliceKey, WorkerId>, bool) {
    let mut assignments: BTreeMap<SliceKey, WorkerId> = BTreeMap::new();
    if workers.is_empty() {
        return (assignments, false);
    }
    // Whether `candidate` may hold a slice of `graph`.
    let runs = |candidate: &Candidate, graph: &GraphKey| {
        let graph = graph.to_string();
        !candidate.aside.contains(&graph) || workers.iter().all(|other| other.aside.contains(&graph))
    };
    let wanted: BTreeSet<&SliceKey> = slices.iter().collect();
    for (slice, worker) in current {
        let holder = workers.iter().find(|candidate| &candidate.worker == worker);
        if wanted.contains(slice) && holder.is_some_and(|holder| runs(holder, &slice.graph_key())) {
            assignments.insert(slice.clone(), worker.clone());
        }
    }

    let count = |assignments: &BTreeMap<SliceKey, WorkerId>, worker: &WorkerId| {
        assignments.values().filter(|w| *w == worker).count()
    };
    let fewest = |assignments: &BTreeMap<SliceKey, WorkerId>, among: &[&Candidate]| -> Option<WorkerId> {
        among
            .iter()
            .min_by(|a, b| {
                count(assignments, &a.worker)
                    .cmp(&count(assignments, &b.worker))
                    .then(a.load.cpu.total_cmp(&b.load.cpu))
                    .then(a.worker.cmp(&b.worker))
            })
            .map(|candidate| candidate.worker.clone())
    };
    let everyone: Vec<&Candidate> = workers.iter().collect();
    let with_headroom: Vec<&Candidate> = workers
        .iter()
        .filter(|candidate| !candidate.load.is_saturated())
        .collect();

    for slice in slices {
        if assignments.contains_key(slice) {
            continue;
        }
        let graph = slice.graph_key();
        let everyone: Vec<&Candidate> = everyone
            .iter()
            .copied()
            .filter(|candidate| runs(candidate, &graph))
            .collect();
        let with_headroom: Vec<&Candidate> = with_headroom
            .iter()
            .copied()
            .filter(|candidate| runs(candidate, &graph))
            .collect();
        let home = with_headroom
            .iter()
            .map(|candidate| {
                let colocated = assignments
                    .iter()
                    .filter(|(other, worker)| other.graph_key() == graph && **worker == candidate.worker)
                    .count();
                (colocated, &candidate.worker)
            })
            .filter(|(colocated, _)| *colocated > 0)
            .max_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(a.1)))
            .map(|(_, worker)| worker.clone());
        let target = home
            .or_else(|| fewest(&assignments, &with_headroom))
            .or_else(|| fewest(&assignments, &everyone));
        if let Some(worker) = target {
            assignments.insert(slice.clone(), worker);
        }
    }

    let mut moved = false;
    if rebalance {
        let overloaded = workers
            .iter()
            .filter(|candidate| candidate.load.is_saturated() && count(&assignments, &candidate.worker) > 1)
            .max_by(|a, b| {
                count(&assignments, &a.worker)
                    .cmp(&count(&assignments, &b.worker))
                    .then(b.worker.cmp(&a.worker))
            });
        if let (Some(from), Some(to)) = (overloaded, fewest(&assignments, &with_headroom)) {
            // Move a slice of the graph weighing most on the saturated worker,
            // preferring a source partition over the emitter.
            let mut per_graph: BTreeMap<GraphKey, Vec<&SliceKey>> = BTreeMap::new();
            for (slice, worker) in &assignments {
                if *worker == from.worker {
                    per_graph.entry(slice.graph_key()).or_default().push(slice);
                }
            }
            let victim = per_graph
                .values()
                .max_by_key(|slices| slices.len())
                .and_then(|slices| {
                    slices
                        .iter()
                        .rev()
                        .find(|slice| matches!(slice.slice, streaming_core::SliceId::Source { .. }))
                        .or(slices.last())
                })
                .map(|slice| (*slice).clone());
            let welcome = |slice: &SliceKey| {
                workers
                    .iter()
                    .find(|candidate| candidate.worker == to)
                    .is_some_and(|candidate| runs(candidate, &slice.graph_key()))
            };
            if let Some(slice) = victim.filter(welcome) {
                assignments.insert(slice, to);
                moved = true;
            }
        }
    }
    (assignments, moved)
}

/// What the controller remembers between two rounds.
#[derive(Debug, Default)]
pub(crate) struct ControllerState {
    last_move: Option<Duration>,
    /// Graphs being deleted: when their logs were closed or last purged, and
    /// how long to wait from then on before purging them again.
    deleting_since: BTreeMap<GraphKey, (Duration, Duration)>,
    /// When each graph was last planned.
    planned: BTreeMap<GraphKey, Duration>,
    /// How often the sources are asked again for their partitions.
    pub replan_interval: Duration,
}

impl ControllerState {
    /// A controller that asks the sources for their partitions again every
    /// `replan_interval`.
    pub(crate) fn new(replan_interval: Duration) -> Self {
        Self {
            replan_interval,
            ..Self::default()
        }
    }
}

/// Cut a graph into slices: validate it and ask each source for its partitions.
pub(crate) async fn plan_graph<P: Providers>(env: &RuntimeEnv<P>, spec: &GraphSpec) -> GraphPlan {
    let mut plan = GraphPlan {
        spec_version: spec.version,
        planned_ms: env.clock.now_ms(),
        ..GraphPlan::default()
    };
    let topology = match Topology::build(spec, &env.registry.catalog()) {
        Ok(topology) => topology,
        Err(error) => {
            plan.error = Some(error.to_string());
            return plan;
        }
    };
    for source in topology.nodes_of(OpKind::Source) {
        let Some(node) = spec.node(&source) else {
            continue;
        };
        let context = OpContext {
            providers: env.providers.clone(),
            clock: env.clock.clone(),
            secrets: env.secrets.clone(),
            kv: env.kv.clone(),
            shared: env.shared.clone(),
            tenant: spec.tenant.clone(),
            graph: spec.graph.clone(),
            node: source.clone(),
        };
        let partitions = match env.registry.get(&node.op) {
            Some((_, OpFactory::Source(factory))) => match factory(&context, &node.config) {
                Ok(connector) => connector.partitions().await.map_err(|e| e.to_string()),
                Err(error) => Err(error.to_string()),
            },
            _ => Err(format!("`{}` is not a source operator", node.op)),
        };
        match partitions {
            Ok(count) => {
                plan.partitions.insert(source, count);
            }
            Err(error) => {
                plan.error = Some(format!("source `{source}`: {error}"));
                return plan;
            }
        }
    }
    plan.slices = topology.slices(&plan.partitions);
    plan
}

/// Read a JSON document. A document that is absent or cannot be decoded is
/// `None`; a store failure is an error, never mistaken for an absent document.
async fn read_doc<T: serde::de::DeserializeOwned>(kv: &dyn Kv, key: &str) -> Result<Option<T>, KvError> {
    match kv.get_json(key).await {
        Ok(doc) => Ok(doc),
        Err(KvError::Malformed { key, reason }) => {
            tracing::warn!(%key, %reason, "ignoring a malformed control-plane document");
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// Remove everything a slice left in the KV.
async fn purge_slice(kv: &dyn Kv, nodes: &[NodeSpec], slice: &SliceKey) -> Result<(), KvError> {
    for node in nodes {
        let windows: Vec<_> = state::indexed_windows(kv, slice, &node.id).await?.into_keys().collect();
        state::delete_windows(kv, slice, &node.id, &windows).await?;
        kv.del(vec![keys::slice_window_index(slice, &node.id)]).await?;
    }
    kv.del(vec![keys::slice_log(slice), keys::slice_seal(slice)]).await
}

/// What the mark of a graph keeps once its purge is over: a controller that
/// took over while the purge ended may still close a log the purge removed,
/// from a plan it read before the plan went. Its seal would then outlive the
/// graph, and close to every plan the logs of a graph declared again under
/// the same name. So the mark stays, with what the plan listed, until such
/// writes have landed; then whatever came back is deleted and the mark goes.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Purged {
    /// Every slice the plan knew of.
    slices: Vec<SliceId>,
    /// The aggregates, which keep window state.
    aggregates: Vec<streaming_core::NodeId>,
    /// When the purge ended, and how long writes issued before may take to land.
    at_ms: i64,
    grace_ms: i64,
}

impl Purged {
    /// What `self` and an earlier purge of the same graph listed, counted
    /// from the later one.
    fn merge(mut self, earlier: Purged) -> Purged {
        for slice in earlier.slices {
            if !self.slices.contains(&slice) {
                self.slices.push(slice);
            }
        }
        for node in earlier.aggregates {
            if !self.aggregates.contains(&node) {
                self.aggregates.push(node);
            }
        }
        self.grace_ms = self.grace_ms.max(earlier.grace_ms);
        self
    }
}

/// Delete what came back of the slices of a purged graph.
async fn sweep_purged(kv: &dyn Kv, graph: &GraphKey, purged: &Purged) -> Result<(), KvError> {
    // Only a controller closing a log comes back this late (every instance
    // stopped writing before the purge began): deletes alone, no reads, so
    // that the sweep ends in a call or two whatever the store does.
    let mut doomed = Vec::new();
    for slice in &purged.slices {
        let slice = SliceKey {
            tenant: graph.tenant.clone(),
            graph: graph.graph.clone(),
            slice: slice.clone(),
        };
        doomed.push(keys::slice_log(&slice));
        doomed.push(keys::slice_seal(&slice));
        doomed.extend(
            purged
                .aggregates
                .iter()
                .map(|node| keys::slice_window_index(&slice, node)),
        );
    }
    for chunk in doomed.chunks(100) {
        kv.del(chunk.to_vec()).await?;
    }
    Ok(())
}

/// Whether everything a retired slice committed has been emitted: each window
/// it holds accumulators for, in the store or in its log, is behind the
/// position of the emitter of its aggregate.
async fn drained(kv: &dyn Kv, topology: &Topology, slice: &SliceKey, log: &state::LogView) -> Result<bool, KvError> {
    let unreadable = |error: state::StateError| KvError::Unavailable(error.to_string());
    for node in topology.slice_nodes(&slice.slice) {
        if &node == slice.slice.root() || topology.kind(&node) != OpKind::Aggregate {
            continue;
        }
        let emitter = SliceKey {
            tenant: slice.tenant.clone(),
            graph: slice.graph.clone(),
            slice: SliceId::Emitter { node: node.clone() },
        };
        let upto = state::read_log(kv, &emitter)
            .await
            .map_err(unreadable)?
            .head()
            .and_then(|head| head.position);
        let windows = state::list_windows(kv, slice, &node, &log.records).await?;
        if windows.iter().any(|window| !state::emitted(window, upto)) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Close the log of every slice of a graph being deleted, so that whatever
/// it still commits is void. Returns how long writes issued before may still
/// take to land: two `gc_grace`.
async fn close_graph(kv: &dyn Kv, graph: &GraphKey) -> Result<Duration, KvError> {
    let spec: Option<GraphSpec> = read_doc(kv, &keys::graph_spec(graph)).await?;
    let plan: GraphPlan = read_doc(kv, &keys::graph_plan(graph)).await?.unwrap_or_default();
    for slice in plan.slices.iter().chain(&plan.retired) {
        let slice = SliceKey {
            tenant: graph.tenant.clone(),
            graph: graph.graph.clone(),
            slice: slice.clone(),
        };
        state::close_log(kv, &slice, u64::MAX).await?;
    }
    Ok(spec.map_or(Duration::ZERO, |spec| 2 * spec.settings.gc_grace))
}

/// Remove everything a deleted graph left in the KV.
///
/// Any store command may fail, so the purge must not need all of them to
/// succeed in one round: a slice whose seal is gone is purged already, and
/// the next round only does what is left. The purge only deletes: a write
/// issued now could land after it is over. Returns whether the graph is gone:
/// only a round that finds every slice purged already deletes the documents
/// that tell what to look for.
async fn purge_graph(
    kv: &dyn Kv,
    graph: &GraphKey,
    catalog: &streaming_core::Catalog,
    now_ms: i64,
) -> Result<bool, KvError> {
    let spec: Option<GraphSpec> = read_doc(kv, &keys::graph_spec(graph)).await?;
    let plan: GraphPlan = read_doc(kv, &keys::graph_plan(graph)).await?.unwrap_or_default();
    let all = spec.as_ref().map(|spec| spec.nodes.clone()).unwrap_or_default();
    // Only aggregates keep window state.
    let nodes: Vec<NodeSpec> = match spec.as_ref().map(|spec| Topology::build(spec, catalog)) {
        Some(Ok(topology)) => all
            .iter()
            .filter(|node| topology.kind(&node.id) == OpKind::Aggregate)
            .cloned()
            .collect(),
        _ => all.clone(),
    };
    let key_of = |slice: &SliceId| SliceKey {
        tenant: graph.tenant.clone(),
        graph: graph.graph.clone(),
        slice: slice.clone(),
    };
    // What is left of the slices purged earlier.
    let seals: Vec<String> = plan
        .closed
        .iter()
        .map(|slice| keys::slice_seal(&key_of(slice)))
        .collect();
    for chunk in seals.chunks(64) {
        kv.del(chunk.to_vec()).await?;
    }
    let mut found = false;
    for slice in plan.slices.iter().chain(&plan.retired) {
        let slice = key_of(slice);
        // Every log was closed first: no seal, nothing left.
        if kv.hgetall(&keys::slice_seal(&slice)).await?.is_empty() {
            continue;
        }
        found = true;
        purge_slice(kv, &nodes, &slice).await?;
    }
    if found {
        // Something was still there: look again once whatever was written
        // meanwhile has landed.
        return Ok(false);
    }
    if !planted(Mutant::ForgetPurgedSlices) {
        let purged = Purged {
            slices: plan
                .slices
                .iter()
                .chain(&plan.retired)
                .chain(&plan.closed)
                .cloned()
                .collect(),
            aggregates: nodes.iter().map(|node| node.id.clone()).collect(),
            at_ms: now_ms,
            grace_ms: 2 * spec
                .as_ref()
                .map_or_else(
                    || streaming_core::spec::GraphSettings::default().gc_grace,
                    |spec| spec.settings.gc_grace,
                )
                .as_millis() as i64,
        };
        // A deletion undone by an index entry written back is purged again,
        // without the plan the first purge removed: keep what it listed.
        let earlier = kv.hget(keys::CLUSTER_DELETING, &graph.to_string()).await?;
        let purged = match earlier.and_then(|value| serde_json::from_slice::<Purged>(&value).ok()) {
            Some(earlier) => purged.merge(earlier),
            None => purged,
        };
        let value = serde_json::to_vec(&purged).map_err(|error| KvError::Unavailable(error.to_string()))?;
        kv.hset(keys::CLUSTER_DELETING, vec![(graph.to_string(), value)])
            .await?;
    }
    let mut doomed: Vec<String> = all.iter().map(|node| keys::graph_output(graph, &node.id)).collect();
    doomed.extend([
        keys::graph_spec(graph),
        keys::graph_code(graph),
        keys::graph_overrides(graph),
        keys::graph_declarations(graph),
        keys::graph_version(graph),
        keys::graph_desired(graph),
        keys::graph_plan(graph),
    ]);
    for chunk in doomed.chunks(64) {
        kv.del(chunk.to_vec()).await?;
    }
    kv.hdel(keys::CLUSTER_GRAPHS, vec![graph.to_string()]).await?;
    Ok(true)
}

/// One controller round: plan what needs planning, purge what is being
/// deleted, and update the placement.
pub(crate) async fn run_once<P: Providers>(
    env: &RuntimeEnv<P>,
    me: &WorkerId,
    members: &[MemberInfo],
    state: &mut ControllerState,
    grace: Duration,
    cooldown: Duration,
) -> Result<(), KvError> {
    let kv = env.kv.as_ref();
    let now = env.providers.time().now();
    let mut changed = false;
    let mut slices: Vec<SliceKey> = Vec::new();

    // Read before the index: a graph marked after this read is seen as
    // deleted by the next round, never taken for a mark left behind.
    let marked: BTreeMap<String, Vec<u8>> = if planted(Mutant::DeleteByTheIndexEntry) {
        BTreeMap::new()
    } else {
        kv.hgetall(keys::CLUSTER_DELETING).await?.into_iter().collect()
    };
    let index: Vec<(String, GraphIndexEntry)> = kv.hgetall_json(keys::CLUSTER_GRAPHS).await?;
    let known: BTreeSet<GraphKey> = index
        .iter()
        .map(|(_, entry)| GraphKey::new(entry.tenant.clone(), entry.graph.clone()))
        .collect();
    state.deleting_since.retain(|graph, _| known.contains(graph));
    // The mark of a graph that is gone: its purge is over. Once the writes
    // issued before it have landed, delete what came back, then the mark.
    let now_ms = env.clock.now_ms();
    let mut left = Vec::new();
    for field in marked.keys() {
        if known.iter().any(|known| known.to_string() == *field) {
            continue;
        }
        let Some(graph) = field.parse::<GraphKey>().ok() else {
            left.push(field.clone());
            continue;
        };
        // Read again: the mark read before the index may be older than the
        // purge that ended in between, which writes its list into the mark
        // before it removes the index entry.
        let Some(value) = kv.hget(keys::CLUSTER_DELETING, field).await? else {
            continue;
        };
        match serde_json::from_slice::<Purged>(&value) {
            Ok(purged) if now_ms.saturating_sub(purged.at_ms) < purged.grace_ms => continue,
            Ok(purged) => sweep_purged(kv, &graph, &purged).await?,
            // Marked, then purged before this kind of mark existed.
            Err(_) => {}
        }
        left.push(field.clone());
    }
    if !left.is_empty() {
        kv.hdel(keys::CLUSTER_DELETING, left).await?;
    }
    let marked: BTreeSet<String> = marked.into_keys().collect();

    for (field, mut entry) in index {
        let graph = GraphKey::new(entry.tenant.clone(), entry.graph.clone());
        if !entry.deleting && marked.contains(&graph.to_string()) {
            // The mark decides. The entry was written back by someone who
            // had read it before the deletion: say it again, for the workers
            // and the API, which read the entry.
            entry.deleting = true;
            kv.hset_json(keys::CLUSTER_GRAPHS, &field, &entry).await?;
            changed = true;
        }
        if entry.deleting {
            // Close every log first, so that an instance still running
            // commits nothing more; then leave workers the time to notice,
            // and the store commands in flight the time to land.
            if !state.deleting_since.contains_key(&graph) {
                let linger = if planted(Mutant::DeleteWithoutClosing) {
                    Duration::ZERO
                } else {
                    close_graph(kv, &graph).await?
                };
                state.deleting_since.insert(graph.clone(), (now, linger));
            }
            let (since, linger) = state.deleting_since[&graph];
            if now.saturating_sub(since) >= grace.max(linger) {
                if purge_graph(kv, &graph, &env.registry.catalog(), env.clock.now_ms()).await? {
                    state.deleting_since.remove(&graph);
                    if planted(Mutant::ForgetPurgedSlices) {
                        kv.hdel(keys::CLUSTER_DELETING, vec![graph.to_string()]).await?;
                    }
                } else {
                    state.deleting_since.insert(graph.clone(), (now, linger));
                }
                changed = true;
            }
            continue;
        }
        let Some(spec) = read_doc::<GraphSpec>(kv, &keys::graph_spec(&graph)).await? else {
            continue;
        };
        let plan_key = keys::graph_plan(&graph);
        let mut plan: Option<GraphPlan> = read_doc(kv, &plan_key).await?;
        let stale = plan
            .as_ref()
            .is_none_or(|plan| plan.spec_version != spec.version || plan.error.is_some());
        // A source may gain or lose partitions at any time: ask again now
        // and then.
        let due = state
            .planned
            .get(&graph)
            .is_none_or(|at| now.saturating_sub(*at) >= state.replan_interval);
        if stale || due {
            state.planned.insert(graph.clone(), now);
            let mut fresh = plan_graph(env, &spec).await;
            if let (Some(old), Some(_)) = (&plan, &fresh.error) {
                // A graph that cannot be planned keeps what the last plan
                // knew of its slices: an error must never pass for a source
                // that lost all its partitions.
                fresh.partitions = old.partitions.clone();
                fresh.slices = old.slices.clone();
                fresh.retired = old.retired.clone();
                fresh.joined = old.joined.clone();
                fresh.closed = old.closed.clone();
                fresh.closing = old.closing.clone();
                fresh.generation = old.generation;
            }
            let same = plan.as_ref().is_some_and(|plan| {
                plan.spec_version == fresh.spec_version && plan.error == fresh.error && plan.slices == fresh.slices
            });
            if same {
                // Nothing changed: see whether retired slices can go.
                if let Some(current) = plan
                    .as_mut()
                    .filter(|plan| plan.error.is_none() && !plan.retired.is_empty())
                    && let Ok(topology) = Topology::build(&spec, &env.registry.catalog())
                {
                    // A retired slice goes in two steps. Once all it
                    // committed is emitted its log is closed; its state is
                    // deleted two `gc_grace` later, when no write issued
                    // before the closing can still land.
                    let now_ms = env.clock.now_ms();
                    let grace_ms = 2 * spec.settings.gc_grace.as_millis() as i64;
                    let mut rewritten = false;
                    let mut kept = Vec::new();
                    for slice in current.retired.clone() {
                        let key = SliceKey {
                            tenant: graph.tenant.clone(),
                            graph: graph.graph.clone(),
                            slice: slice.clone(),
                        };
                        match current.closing.get(&slice).copied() {
                            None => {
                                let unreadable = |error: state::StateError| KvError::Unavailable(error.to_string());
                                let log = state::read_log(kv, &key).await.map_err(unreadable)?;
                                // Closing voids what the log holds: first take
                                // the epoch after the one found drained, so that
                                // an instance left behind cannot commit, then
                                // acknowledge, what would be voided unemitted.
                                if drained(kv, &topology, &key, &log).await?
                                    && (planted(Mutant::CloseWithoutClaim)
                                        || state::claim_end(kv, &key, &log, current.generation)
                                            .await
                                            .map_err(unreadable)?)
                                {
                                    state::close_log(kv, &key, current.generation).await?;
                                    current.closing.insert(slice.clone(), now_ms);
                                    rewritten = true;
                                }
                                kept.push(slice);
                            }
                            Some(since) if now_ms.saturating_sub(since) >= grace_ms => {
                                // Only aggregates keep window state.
                                let nodes: Vec<_> = topology
                                    .slice_nodes(&slice)
                                    .into_iter()
                                    .filter(|node| topology.kind(node) == OpKind::Aggregate)
                                    .collect();
                                if let Err(error) = state::purge_closed(kv, &key, &nodes).await {
                                    // Try again next round; the slices after
                                    // this one and the placement go on.
                                    tracing::warn!(slice = %key, %error, "purging a retired slice failed");
                                    kept.push(slice);
                                    continue;
                                }
                                current.closing.remove(&slice);
                                if !current.closed.contains(&slice) {
                                    current.closed.push(slice);
                                }
                                rewritten = true;
                            }
                            Some(_) => kept.push(slice),
                        }
                    }
                    if rewritten {
                        current.retired = kept;
                        kv.set_json(&plan_key, current).await?;
                        changed = true;
                    }
                }
            } else if !stale && fresh.error.is_some() {
                // A source that cannot be asked right now does not unplan a
                // graph that runs.
            } else if fresh.error.is_some() {
                kv.set_json(&plan_key, &fresh).await?;
                changed = true;
                plan = Some(fresh);
            } else {
                // The set of slices changes: a new generation. Slices that
                // leave the plan are retired, not forgotten, so that what
                // they committed is still emitted.
                if let Some(old) = &plan {
                    fresh.generation = old.generation + 1;
                    fresh.closed = old.closed.clone();
                    fresh.closing = old.closing.clone();
                    for slice in old.retired.iter().chain(&old.slices) {
                        if !fresh.slices.contains(slice) && !fresh.retired.contains(slice) {
                            fresh.retired.push(slice.clone());
                        }
                    }
                    for slice in &fresh.slices {
                        if let Some(since) = old.joined.get(slice) {
                            fresh.joined.insert(slice.clone(), *since);
                        } else if old.generation > 0 && !old.slices.contains(slice) {
                            // The graph has run without this slice.
                            fresh.joined.insert(slice.clone(), fresh.generation);
                        }
                    }
                } else {
                    fresh.generation = 1;
                }
                kv.set_json(&plan_key, &fresh).await?;
                changed = true;
                plan = Some(fresh);
            }
        }
        let desired: DesiredState = read_doc(kv, &keys::graph_desired(&graph)).await?.unwrap_or_default();
        if desired.state == RunState::Stopped {
            continue;
        }
        if let Some(plan) = plan.filter(|plan| plan.error.is_none()) {
            slices.extend(plan.slices.into_iter().map(|slice| SliceKey {
                tenant: graph.tenant.clone(),
                graph: graph.graph.clone(),
                slice,
            }));
        }
    }

    let candidates: Vec<Candidate> = members
        .iter()
        .map(|member| Candidate {
            worker: member.worker.clone(),
            load: member.load,
            aside: member.aside.clone(),
        })
        .collect();
    let placement: Placement = read_doc(kv, keys::CLUSTER_PLACEMENT).await?.unwrap_or_default();
    let rebalance = state.last_move.is_none_or(|at| now.saturating_sub(at) >= cooldown);
    let (assignments, moved) = assign(&slices, &candidates, &placement.assignments, rebalance);
    if moved {
        state.last_move = Some(now);
    }
    if assignments != placement.assignments {
        kv.set_json(
            keys::CLUSTER_PLACEMENT,
            &Placement {
                version: placement.version + 1,
                computed_by: Some(me.clone()),
                computed_ms: env.clock.now_ms(),
                assignments,
            },
        )
        .await?;
        changed = true;
    }
    if changed {
        kv.incr(keys::CLUSTER_EPOCH).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(id: &str, cpu: f64) -> Candidate {
        Candidate {
            worker: WorkerId::new(id).unwrap(),
            load: Load { cpu, ..Load::default() },
            aside: BTreeSet::new(),
        }
    }

    fn slices(graph: &str, partitions: u32) -> Vec<SliceKey> {
        let mut slices: Vec<SliceKey> = (0..partitions)
            .map(|p| format!("acme/{graph}/src.events.{p}").parse().unwrap())
            .collect();
        slices.push(format!("acme/{graph}/agg.count").parse().unwrap());
        slices
    }

    fn hosts(assignments: &BTreeMap<SliceKey, WorkerId>, graph: &str) -> BTreeSet<String> {
        assignments
            .iter()
            .filter(|(slice, _)| slice.graph.as_str() == graph)
            .map(|(_, worker)| worker.to_string())
            .collect()
    }

    #[test]
    fn a_graph_stays_on_one_worker_and_graphs_spread() {
        let workers = [worker("w1", 0.1), worker("w2", 0.1), worker("w3", 0.1)];
        let mut all = slices("a", 4);
        all.extend(slices("b", 2));
        let (assignments, moved) = assign(&all, &workers, &BTreeMap::new(), true);
        assert!(!moved);
        assert_eq!(assignments.len(), all.len());
        assert_eq!(hosts(&assignments, "a").len(), 1, "graph a is colocated");
        assert_eq!(hosts(&assignments, "b").len(), 1, "graph b is colocated");
        assert_ne!(hosts(&assignments, "a"), hosts(&assignments, "b"));
    }

    #[test]
    fn a_slice_goes_to_and_stays_on_a_worker_that_runs_its_graph() {
        let aside = |id: &str, graphs: &[&str]| Candidate {
            aside: graphs.iter().map(|graph| format!("acme/{graph}")).collect(),
            ..worker(id, 0.1)
        };
        let mut all = slices("a", 2);
        all.extend(slices("b", 2));
        // Two workers of the binary being replaced, one of the new binary,
        // which declares `a` otherwise.
        let workers = [aside("w1", &["a"]), aside("w2", &["a"]), aside("w3", &[])];
        let (first, _) = assign(&all, &workers, &BTreeMap::new(), true);
        assert_eq!(hosts(&first, "a"), BTreeSet::from(["w3".to_owned()]));
        assert_eq!(first.len(), all.len());

        // What a worker held before it stepped aside leaves it.
        let before: BTreeMap<SliceKey, WorkerId> = all
            .iter()
            .map(|slice| (slice.clone(), WorkerId::new("w1").unwrap()))
            .collect();
        let (moved, _) = assign(&all, &workers, &before, false);
        assert_eq!(hosts(&moved, "a"), BTreeSet::from(["w3".to_owned()]));
        assert_eq!(hosts(&moved, "b"), BTreeSet::from(["w1".to_owned()]));

        // While nobody runs a graph its slices stay where they are, and new
        // ones are placed as if everybody did.
        let nobody = [aside("w1", &["a"]), aside("w2", &["a"])];
        let (kept, _) = assign(&all, &nobody, &before, false);
        assert_eq!(kept, before);
        let (placed, _) = assign(&all, &nobody, &BTreeMap::new(), false);
        assert_eq!(placed.len(), all.len());

        // A saturated worker does not shed a slice onto one that steps aside
        // from its graph.
        let busy = [aside("w1", &[]), aside("w2", &["a"])];
        let busy = [
            Candidate {
                load: Load {
                    cpu: 0.95,
                    ..Load::default()
                },
                ..busy[0].clone()
            },
            busy[1].clone(),
        ];
        let only_a: BTreeMap<SliceKey, WorkerId> = slices("a", 2)
            .into_iter()
            .map(|slice| (slice, WorkerId::new("w1").unwrap()))
            .collect();
        let (after, moved) = assign(&slices("a", 2), &busy, &only_a, true);
        assert!(!moved);
        assert_eq!(after, only_a);
    }

    #[test]
    fn assignments_are_stable_and_follow_failures() {
        let workers = [worker("w1", 0.1), worker("w2", 0.1)];
        let all = slices("a", 3);
        let (first, _) = assign(&all, &workers, &BTreeMap::new(), true);
        let (second, _) = assign(&all, &workers, &first, true);
        assert_eq!(first, second, "nothing moves without a reason");

        let home = hosts(&first, "a").into_iter().next().unwrap();
        let survivors: Vec<Candidate> = workers
            .iter()
            .filter(|candidate| candidate.worker.as_str() != home)
            .cloned()
            .collect();
        let (after, _) = assign(&all, &survivors, &first, true);
        assert_eq!(after.len(), all.len());
        assert!(after.values().all(|w| w.as_str() != home));
    }

    #[test]
    fn spreading_requires_saturation_and_moves_one_slice_at_a_time() {
        let all = slices("a", 3);
        let calm = [worker("w1", 0.5), worker("w2", 0.1)];
        let (colocated, _) = assign(&all, &calm, &BTreeMap::new(), true);
        let home = hosts(&colocated, "a").into_iter().next().unwrap();

        // Busy but not saturated: no reason to pay for network hops.
        let (same, moved) = assign(&all, &calm, &colocated, true);
        assert!(!moved);
        assert_eq!(same, colocated);

        let hot: Vec<Candidate> = calm
            .iter()
            .map(|c| worker(c.worker.as_str(), if c.worker.as_str() == home { 0.95 } else { 0.1 }))
            .collect();
        let (spread, moved) = assign(&all, &hot, &colocated, true);
        assert!(moved);
        let changed: Vec<_> = spread.iter().filter(|(slice, w)| colocated[*slice] != **w).collect();
        assert_eq!(changed.len(), 1, "exactly one slice moves");
        assert!(matches!(changed[0].0.slice, streaming_core::SliceId::Source { .. }));

        // During the cooldown nothing else moves.
        let (held, moved) = assign(&all, &hot, &spread, false);
        assert!(!moved);
        assert_eq!(held, spread);

        // With every worker saturated there is nowhere to go.
        let all_hot = [worker("w1", 0.95), worker("w2", 0.95)];
        let (stuck, moved) = assign(&all, &all_hot, &colocated, true);
        assert!(!moved);
        assert_eq!(stuck, colocated);
    }

    #[test]
    fn no_worker_means_no_assignment() {
        let (assignments, moved) = assign(&slices("a", 2), &[], &BTreeMap::new(), true);
        assert!(assignments.is_empty() && !moved);
    }

    #[test]
    fn a_saturated_worker_keeps_its_only_slice() {
        let all = slices("a", 0);
        let workers = [worker("w1", 0.95), worker("w2", 0.1)];
        let current = BTreeMap::from([(all[0].clone(), WorkerId::new("w1").unwrap())]);
        let (kept, moved) = assign(&all, &workers, &current, true);
        assert!(!moved);
        assert_eq!(kept, current);
    }

    mod rounds {
        use super::super::*;
        use crate::memlog::MemLog;
        use moonpool_core::TokioProviders;
        use std::sync::Arc;
        use streaming_core::{
            DynKv, MemKv, NoSecrets, Registry, SystemClock, TypeTag,
            control::{GraphIndexEntry, MemberInfo},
        };

        fn env(kv: &DynKv) -> RuntimeEnv<TokioProviders> {
            let mut registry = Registry::<TokioProviders>::standard();
            let log = MemLog::new(3);
            registry.register_source::<u64>("test/log@1", "", move |context, _| {
                Ok(log.source(context.providers.clone()))
            });
            registry.register_sink("test/out@1", "", TypeTag::Any, |_, _| {
                Err(streaming_core::OpError::Config("not run here".into()))
            });
            RuntimeEnv {
                shared: Arc::default(),
                providers: TokioProviders::new(),
                kv: kv.clone(),
                clock: Arc::new(SystemClock),
                secrets: Arc::new(NoSecrets),
                registry: Arc::new(registry),
            }
        }

        async fn submit(kv: &DynKv, version: u64) -> GraphKey {
            let spec: GraphSpec = serde_json::from_value(serde_json::json!({
                "tenant": "acme",
                "graph": "g",
                "version": version,
                "nodes": [
                    {"id": "events", "op": "test/log@1"},
                    {"id": "out", "op": "test/out@1"}
                ],
                "edges": [["events", "out"]]
            }))
            .unwrap();
            let graph = spec.key();
            kv.set_json(&keys::graph_spec(&graph), &spec).await.unwrap();
            kv.hset_json(
                keys::CLUSTER_GRAPHS,
                &graph.to_string(),
                &GraphIndexEntry {
                    tenant: spec.tenant.clone(),
                    graph: spec.graph.clone(),
                    version,
                    updated_ms: 0,
                    deleting: false,
                    orphan: false,
                    fingerprint: None,
                },
            )
            .await
            .unwrap();
            graph
        }

        fn member(id: &str, cpu: f64) -> MemberInfo {
            MemberInfo {
                worker: WorkerId::new(id).unwrap(),
                generation: 1,
                heartbeat_ms: 0,
                load: Load { cpu, ..Load::default() },
                actors: 0,
                slices: 0,
                api_addr: None,
                version: String::new(),
                aside: Default::default(),
            }
        }

        /// A controller that takes over while another one ends a purge may
        /// close a log the purge removed, from a plan read before it went:
        /// what it writes back is swept once such writes have landed, and only
        /// then does the mark of the deletion go.
        #[tokio::test]
        async fn what_a_late_controller_closes_after_the_purge_is_swept() {
            let kv: DynKv = MemKv::shared();
            let env = env(&kv);
            let graph = submit(&kv, 1).await;
            let me = WorkerId::new("w1").unwrap();
            let members = [member("w1", 0.1)];
            let hour = Duration::from_secs(3600);
            let mut first = ControllerState::new(hour);
            run_once(&env, &me, &members, &mut first, hour, hour).await.unwrap();
            let plan: GraphPlan = kv.get_json(&keys::graph_plan(&graph)).await.unwrap().unwrap();
            let slices: Vec<SliceKey> = plan
                .slices
                .iter()
                .map(|slice| SliceKey {
                    tenant: graph.tenant.clone(),
                    graph: graph.graph.clone(),
                    slice: slice.clone(),
                })
                .collect();
            for slice in &slices {
                kv.hset(&keys::slice_log(slice), vec![("c:1".into(), b"x".to_vec())])
                    .await
                    .unwrap();
            }
            kv.hset(keys::CLUSTER_DELETING, vec![(graph.to_string(), b"1".to_vec())])
                .await
                .unwrap();
            // The first controller closes every log, purges the slices, then
            // makes its last pass, which removes the plan.
            close_graph(kv.as_ref(), &graph).await.unwrap();
            assert!(
                !purge_graph(kv.as_ref(), &graph, &env.registry.catalog(), 0)
                    .await
                    .unwrap()
            );
            assert!(
                purge_graph(kv.as_ref(), &graph, &env.registry.catalog(), 0)
                    .await
                    .unwrap()
            );
            // Another one had read the plan before it went, and closes after.
            kv.set_json(&keys::graph_plan(&graph), &plan).await.unwrap();
            close_graph(kv.as_ref(), &graph).await.unwrap();
            kv.del(vec![keys::graph_plan(&graph)]).await.unwrap();
            assert!(
                kv.hget(keys::CLUSTER_DELETING, &graph.to_string())
                    .await
                    .unwrap()
                    .is_some()
            );
            // The next round, long after the purge, sweeps it.
            run_once(&env, &me, &members, &mut first, hour, hour).await.unwrap();
            assert!(
                kv.hget(keys::CLUSTER_DELETING, &graph.to_string())
                    .await
                    .unwrap()
                    .is_none()
            );
            for slice in &slices {
                let seal = kv.hgetall(&keys::slice_seal(slice)).await.unwrap();
                assert!(seal.is_empty(), "{slice} kept {seal:?}");
            }
        }

        #[tokio::test]
        async fn a_new_version_is_planned_at_once() {
            let kv: DynKv = MemKv::shared();
            let env = env(&kv);
            let graph = submit(&kv, 1).await;
            let me = WorkerId::new("w1").unwrap();
            let members = [member("w1", 0.1)];
            let mut state = ControllerState::new(Duration::from_secs(3600));
            let hour = Duration::from_secs(3600);
            run_once(&env, &me, &members, &mut state, hour, hour).await.unwrap();
            let plan: GraphPlan = kv.get_json(&keys::graph_plan(&graph)).await.unwrap().unwrap();
            assert_eq!((plan.spec_version, plan.slices.len()), (1, 3));
            assert!(plan.planned_ms > 1_700_000_000_000);
            submit(&kv, 2).await;
            run_once(&env, &me, &members, &mut state, hour, hour).await.unwrap();
            let plan: GraphPlan = kv.get_json(&keys::graph_plan(&graph)).await.unwrap().unwrap();
            assert_eq!(plan.spec_version, 2);
        }

        #[tokio::test]
        async fn one_move_per_cooldown() {
            let kv: DynKv = MemKv::shared();
            let env = env(&kv);
            submit(&kv, 1).await;
            let me = WorkerId::new("w1").unwrap();
            let hour = Duration::from_secs(3600);
            let mut state = ControllerState::new(hour);
            // Everything lands on w1 first.
            run_once(&env, &me, &[member("w1", 0.1)], &mut state, hour, hour)
                .await
                .unwrap();
            let moved = |placement: &Placement| {
                placement
                    .assignments
                    .values()
                    .filter(|worker| worker.as_str() == "w2")
                    .count()
            };
            let members = [member("w1", 0.95), member("w2", 0.1)];
            for _ in 0..3 {
                run_once(&env, &me, &members, &mut state, hour, hour).await.unwrap();
                let placement: Placement = kv.get_json(keys::CLUSTER_PLACEMENT).await.unwrap().unwrap();
                assert_eq!(moved(&placement), 1);
            }
        }
    }
}
