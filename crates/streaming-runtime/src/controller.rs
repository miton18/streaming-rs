//! The controller: planning graphs and placing their slices on workers.
//!
//! One worker at a time acts as controller (the alive worker with the
//! smallest id). Nothing here is needed for correctness: if two workers
//! believe they are the controller, or if a placement is stale, the worst
//! outcome is two workers running the same slice for a while, which the
//! commit log arbitrates.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use moonpool_core::{Providers, TimeProvider};
use streaming_core::{
    GraphKey, GraphSpec, Kv, KvError, NodeSpec, OpContext, OpFactory, OpKind, SliceId, SliceKey, Topology, WorkerId,
    control::{DesiredState, GraphIndexEntry, GraphPlan, Load, MemberInfo, Placement, RunState},
    keys,
};

use crate::{RuntimeEnv, state};

/// A worker as the placement policy sees it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Candidate {
    pub worker: WorkerId,
    pub load: Load,
}

/// Place `slices` on `workers`.
///
/// The policy keeps a graph together and only spreads it on evidence:
///
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
    let alive: BTreeSet<&WorkerId> = workers.iter().map(|candidate| &candidate.worker).collect();
    let wanted: BTreeSet<&SliceKey> = slices.iter().collect();
    for (slice, worker) in current {
        if wanted.contains(slice) && alive.contains(worker) {
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
            if let Some(slice) = victim {
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
    deleting_since: BTreeMap<GraphKey, Duration>,
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

/// Whether everything a retired slice committed has been emitted: each window
/// it holds accumulators for, in the store or in its log, is behind the
/// position of the emitter of its aggregate.
async fn drained(kv: &dyn Kv, topology: &Topology, slice: &SliceKey) -> Result<bool, KvError> {
    let unreadable = |error: state::StateError| KvError::Unavailable(error.to_string());
    let log = state::read_log(kv, slice).await.map_err(unreadable)?;
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

/// Remove everything a deleted graph left in the KV.
async fn purge_graph(kv: &dyn Kv, graph: &GraphKey) -> Result<(), KvError> {
    let spec: Option<GraphSpec> = read_doc(kv, &keys::graph_spec(graph)).await?;
    let plan: Option<GraphPlan> = read_doc(kv, &keys::graph_plan(graph)).await?;
    let nodes = spec.map(|spec| spec.nodes).unwrap_or_default();
    let plan = plan.unwrap_or_default();
    for slice in &plan.closed {
        // What is left of the slices purged earlier.
        let slice = SliceKey {
            tenant: graph.tenant.clone(),
            graph: graph.graph.clone(),
            slice: slice.clone(),
        };
        kv.del(vec![keys::slice_seal(&slice)]).await?;
    }
    for slice in plan.slices.into_iter().chain(plan.retired) {
        let slice = SliceKey {
            tenant: graph.tenant.clone(),
            graph: graph.graph.clone(),
            slice,
        };
        purge_slice(kv, &nodes, &slice).await?;
    }
    let mut doomed: Vec<String> = nodes.iter().map(|node| keys::graph_output(graph, &node.id)).collect();
    doomed.extend([
        keys::graph_spec(graph),
        keys::graph_desired(graph),
        keys::graph_plan(graph),
    ]);
    for chunk in doomed.chunks(64) {
        kv.del(chunk.to_vec()).await?;
    }
    kv.hdel(keys::CLUSTER_GRAPHS, vec![graph.to_string()]).await
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

    let index: Vec<(String, GraphIndexEntry)> = kv.hgetall_json(keys::CLUSTER_GRAPHS).await?;
    let known: BTreeSet<GraphKey> = index
        .iter()
        .map(|(_, entry)| GraphKey::new(entry.tenant.clone(), entry.graph.clone()))
        .collect();
    state.deleting_since.retain(|graph, _| known.contains(graph));

    for (_, entry) in index {
        let graph = GraphKey::new(entry.tenant.clone(), entry.graph.clone());
        if entry.deleting {
            // Leave workers the time to notice and stop the graph's slices.
            let since = *state.deleting_since.entry(graph.clone()).or_insert(now);
            if now.saturating_sub(since) >= grace {
                purge_graph(kv, &graph).await?;
                state.deleting_since.remove(&graph);
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
                    let nodes: Vec<_> = spec.nodes.iter().map(|node| node.id.clone()).collect();
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
                                if drained(kv, &topology, &key).await? {
                                    state::close_log(kv, &key, current.generation).await?;
                                    current.closing.insert(slice.clone(), now_ms);
                                    rewritten = true;
                                }
                                kept.push(slice);
                            }
                            Some(since) if now_ms.saturating_sub(since) >= grace_ms => {
                                state::purge_closed(kv, &key, &nodes).await?;
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
}
