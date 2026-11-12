//! Status views: pure aggregation of the documents workers and the controller
//! publish into the structures the web UI shows. No I/O.
//!
//! Serialized shapes follow `docs/http-api.md`.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::Value;
use streaming_core::{
    Catalog, GraphSpec, NodeId, OpDescriptor, SliceId, SliceKey, TenantId, Timestamp, WorkerId,
    control::{
        ActorKind, ActorSnapshot, ActorState, DesiredState, GraphIndexEntry, GraphPlan, Load, MemberInfo, Placement,
        RunState, SliceSnapshot, TenantUsage, WorkerSnapshot,
    },
};

/// A member with the metrics snapshot it published, if any.
pub type MemberReport = (MemberInfo, Option<WorkerSnapshot>);

/// A node is saturated from this busy ratio.
pub const BUSY_THRESHOLD: f64 = 0.5;
/// A node is waiting for room downstream from this backpressure ratio.
pub const BACKPRESSURE_THRESHOLD: f64 = 0.3;

/// One worker of `GET /api/v1/cluster`.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct WorkerView {
    /// Worker id.
    pub worker: WorkerId,
    /// Heartbeat younger than the member timeout.
    pub alive: bool,
    /// Process generation.
    pub generation: u64,
    /// Last heartbeat.
    pub heartbeat_ms: Timestamp,
    /// Resource usage.
    pub load: Load,
    /// Actors on the worker.
    pub actors: u32,
    /// Slices on the worker.
    pub slices: u32,
    /// Worker HTTP address.
    pub api_addr: Option<String>,
    /// Worker version.
    pub version: String,
    /// Consumption per tenant, from the worker's snapshot (empty without one).
    pub tenants: BTreeMap<TenantId, TenantUsage>,
}

/// `GET /api/v1/cluster`.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct ClusterView {
    /// Value of `cluster:epoch`.
    pub epoch: i64,
    /// Server time.
    pub now_ms: Timestamp,
    /// Members.
    pub workers: Vec<WorkerView>,
    /// Current placement (empty, version 0, when none was published).
    pub placement: Placement,
    /// KV statistics, when the embedding process provides them.
    #[schemars(with = "Option<serde_json::Map<String, Value>>")]
    pub kv: Option<Value>,
}

/// Build the cluster view.
#[must_use]
pub fn cluster_view(
    epoch: i64,
    now_ms: Timestamp,
    members: &[MemberReport],
    placement: Option<Placement>,
    kv: Option<Value>,
    member_timeout_ms: i64,
) -> ClusterView {
    let workers = members
        .iter()
        .map(|(member, snapshot)| WorkerView {
            worker: member.worker.clone(),
            alive: member.is_alive(now_ms, member_timeout_ms),
            generation: member.generation,
            heartbeat_ms: member.heartbeat_ms,
            load: member.load,
            actors: member.actors,
            slices: member.slices,
            api_addr: member.api_addr.clone(),
            version: member.version.clone(),
            tenants: snapshot
                .as_ref()
                .map(|snapshot| snapshot.tenants.clone())
                .unwrap_or_default(),
        })
        .collect();
    ClusterView {
        epoch,
        now_ms,
        workers,
        placement: placement.unwrap_or_default(),
        kv,
    }
}

/// `GET /api/v1/catalog`.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct CatalogView {
    /// Descriptors sorted by reference.
    pub ops: Vec<OpDescriptor>,
}

/// Build the catalog view (sorted by operator reference).
#[must_use]
pub fn catalog_view(catalog: &Catalog) -> CatalogView {
    let mut ops: Vec<OpDescriptor> = catalog.ops.values().cloned().collect();
    ops.sort_by(|a, b| a.op.as_str().cmp(b.op.as_str()));
    CatalogView { ops }
}

/// One tenant of `GET /api/v1/tenants`.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct TenantSummary {
    /// Tenant id.
    pub tenant: TenantId,
    /// Graphs in the index (including those being deleted).
    pub graphs: usize,
    /// Actors of the tenant on alive workers.
    pub actors: u64,
    /// Input rate of the tenant on alive workers, records per second.
    pub in_per_sec: f64,
}

/// `GET /api/v1/tenants`.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct TenantsView {
    /// Tenants sorted by id.
    pub tenants: Vec<TenantSummary>,
}

/// Alive members with a snapshot.
fn alive_snapshots(
    members: &[MemberReport],
    now_ms: Timestamp,
    member_timeout_ms: i64,
) -> impl Iterator<Item = (&MemberInfo, &WorkerSnapshot)> {
    members.iter().filter_map(move |(member, snapshot)| {
        if member.is_alive(now_ms, member_timeout_ms) {
            snapshot.as_ref().map(|snapshot| (member, snapshot))
        } else {
            None
        }
    })
}

/// Group the graph index by tenant; actor counts and input rates are the sums
/// of the [`TenantUsage`] reported by alive workers.
#[must_use]
pub fn tenants_view(
    index: &[GraphIndexEntry],
    members: &[MemberReport],
    now_ms: Timestamp,
    member_timeout_ms: i64,
) -> TenantsView {
    let mut tenants: BTreeMap<&TenantId, TenantSummary> = BTreeMap::new();
    for entry in index {
        tenants
            .entry(&entry.tenant)
            .or_insert_with(|| TenantSummary {
                tenant: entry.tenant.clone(),
                graphs: 0,
                actors: 0,
                in_per_sec: 0.0,
            })
            .graphs += 1;
    }
    for (_, snapshot) in alive_snapshots(members, now_ms, member_timeout_ms) {
        for (tenant, usage) in &snapshot.tenants {
            if let Some(summary) = tenants.get_mut(tenant) {
                summary.actors += u64::from(usage.actors);
                summary.in_per_sec += usage.in_per_sec;
            }
        }
    }
    TenantsView {
        tenants: tenants.into_values().collect(),
    }
}

/// Worst-first state of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    /// At least one instance failed.
    Failed,
    /// At least one instance is starting.
    Starting,
    /// At least one instance is paused.
    Paused,
    /// At least one instance is draining.
    Draining,
    /// Every instance runs.
    Running,
    /// Every instance stopped.
    Stopped,
    /// No instance reported.
    Pending,
}

impl From<ActorState> for NodeState {
    fn from(state: ActorState) -> Self {
        match state {
            ActorState::Failed => NodeState::Failed,
            ActorState::Starting => NodeState::Starting,
            ActorState::Paused => NodeState::Paused,
            ActorState::Draining => NodeState::Draining,
            ActorState::Running => NodeState::Running,
            ActorState::Stopped => NodeState::Stopped,
        }
    }
}

/// An actor snapshot with the worker that reported it.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct InstanceView {
    /// The actor's snapshot.
    #[serde(flatten)]
    pub actor: ActorSnapshot,
    /// Reporting worker.
    pub worker: WorkerId,
}

/// A slice snapshot with the reporting and the assigned worker.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct SliceView {
    /// The slice's snapshot (synthetic, `starting`, when nobody reports it).
    #[serde(flatten)]
    pub slice: SliceSnapshot,
    /// Reporting worker; null for a slice nobody reports yet.
    pub worker: Option<WorkerId>,
    /// Worker the placement assigns the slice to.
    pub assigned_to: Option<WorkerId>,
}

/// A node that limits the graph.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct Bottleneck {
    /// The limiting node.
    pub node: NodeId,
    /// Its kind (`aggregate`, ..., or `commit`).
    pub kind: String,
    /// Its busy ratio.
    pub busy: f64,
    /// Part of that ratio spent waiting for an external system to answer.
    #[serde(default)]
    pub waiting: f64,
    /// What holds the node back: `"destination"` when most of its busy time
    /// is spent waiting for the system it writes to, so that neither more
    /// CPU nor moving it helps; `"work"` otherwise.
    #[serde(default)]
    pub limited_by: String,
    /// Human-readable explanation.
    pub reason: String,
}

/// A node of a graph with its aggregated metrics.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct NodeView {
    /// Node id.
    pub id: NodeId,
    /// Operator reference.
    pub op: String,
    /// Operator kind (`unknown` when the operator is not in the catalog).
    pub kind: String,
    /// Operator description.
    pub description: String,
    /// Accepted type (null when the operator is not in the catalog).
    pub input_type: Option<String>,
    /// Produced type (null when the operator is not in the catalog).
    pub output_type: Option<String>,
    /// Node configuration.
    pub config: Value,
    /// Listed in `desired.paused_nodes`.
    pub paused: bool,
    /// This node is the graph's bottleneck.
    pub bottleneck: bool,
    /// Worst instance state.
    pub state: NodeState,
    /// Number of instances.
    pub actors: usize,
    /// Workers running instances, sorted.
    pub workers: Vec<WorkerId>,
    /// Records received.
    pub records_in: u64,
    /// Records emitted.
    pub records_out: u64,
    /// Input rate.
    pub in_per_sec: f64,
    /// Output rate.
    pub out_per_sec: f64,
    /// Busy ratio of the hot instance.
    pub busy: f64,
    /// Idle ratio of the hot instance.
    pub idle: f64,
    /// Backpressure ratio of the hot instance.
    pub backpressured: f64,
    /// Highest share of time an instance spent waiting for an external
    /// system to answer. Part of `busy`: a node busy mostly waiting is
    /// limited by that system, not by its worker.
    pub waiting: f64,
    /// Queued chunks, summed.
    pub queue_len: u64,
    /// Queue capacity, summed.
    pub queue_cap: u64,
    /// Late records dropped, summed.
    pub late_dropped: u64,
    /// Errors, summed.
    pub errors: u64,
    /// Some instance's last error.
    pub last_error: Option<String>,
    /// The instances.
    pub instances: Vec<InstanceView>,
}

/// An edge of a graph.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct EdgeView {
    /// Origin.
    pub from: NodeId,
    /// Destination.
    pub to: NodeId,
    /// `queue_len / queue_cap` of the destination node.
    pub fill: f64,
}

/// `GET /api/v1/tenants/{tenant}/graphs/{graph}`.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct GraphDetail {
    /// Tenant.
    pub tenant: TenantId,
    /// Graph.
    pub graph: streaming_core::GraphId,
    /// Version of the stored description.
    pub version: u64,
    /// Desired run state.
    pub state: RunState,
    /// The description.
    pub spec: GraphSpec,
    /// The plan, if computed.
    pub plan: Option<GraphPlan>,
    /// The desired state document.
    pub desired: DesiredState,
    /// What limits the graph, if anything.
    pub bottleneck: Option<Bottleneck>,
    /// Minimum watermark of the source slices; null if any is unknown.
    pub watermark_ms: Option<Timestamp>,
    /// Nodes, in description order.
    pub nodes: Vec<NodeView>,
    /// Edges, in description order.
    pub edges: Vec<EdgeView>,
    /// Slices.
    pub slices: Vec<SliceView>,
    /// Every actor of the graph on alive workers, commit actors included.
    #[serde(skip)]
    pub total_actors: usize,
}

/// One graph of `GET /api/v1/tenants/{tenant}/graphs`.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct GraphSummary {
    /// Tenant.
    pub tenant: TenantId,
    /// Graph.
    pub graph: streaming_core::GraphId,
    /// Version.
    pub version: u64,
    /// Desired run state.
    pub state: RunState,
    /// Node count.
    pub nodes: usize,
    /// Slice count (see [`GraphDetail::slices`]).
    pub slices: usize,
    /// Actors on alive workers, commit actors included.
    pub actors: usize,
    /// Sum of the output rates of the source nodes.
    pub in_per_sec: f64,
    /// Sum of the output rates of the sink nodes.
    pub out_per_sec: f64,
    /// What limits the graph.
    pub bottleneck: Option<Bottleneck>,
    /// Minimum watermark of the source slices.
    pub watermark_ms: Option<Timestamp>,
    /// Last change of the description (from the index).
    pub updated_ms: Timestamp,
    /// Plan error.
    pub error: Option<String>,
}

/// `GET /api/v1/tenants/{tenant}/graphs`.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct GraphsView {
    /// Graphs.
    pub graphs: Vec<GraphSummary>,
}

/// Summarize a detail. `updated_ms` comes from the index entry.
#[must_use]
pub fn graph_summary(detail: &GraphDetail, updated_ms: Timestamp) -> GraphSummary {
    let rate = |kind: &str| {
        detail
            .nodes
            .iter()
            .filter(|node| node.kind == kind)
            .map(|node| node.out_per_sec)
            .sum()
    };
    GraphSummary {
        tenant: detail.tenant.clone(),
        graph: detail.graph.clone(),
        version: detail.version,
        state: detail.state,
        nodes: detail.nodes.len(),
        slices: detail.slices.len(),
        actors: detail.total_actors,
        in_per_sec: rate("source"),
        out_per_sec: rate("sink"),
        bottleneck: detail.bottleneck.clone(),
        watermark_ms: detail.watermark_ms,
        updated_ms,
        error: detail.plan.as_ref().and_then(|plan| plan.error.clone()),
    }
}

fn aggregate_node(
    spec_node: &streaming_core::NodeSpec,
    descriptor: Option<&OpDescriptor>,
    paused: bool,
    instances: &[(&WorkerId, &ActorSnapshot)],
) -> NodeView {
    let sum = |f: fn(&ActorSnapshot) -> f64, pick: &dyn Fn(&ActorSnapshot) -> bool| -> f64 {
        instances.iter().filter(|(_, a)| pick(a)).map(|(_, a)| f(a)).sum()
    };
    let sum_u = |f: fn(&ActorSnapshot) -> u64, pick: &dyn Fn(&ActorSnapshot) -> bool| -> u64 {
        instances.iter().filter(|(_, a)| pick(a)).map(|(_, a)| f(a)).sum()
    };
    let any = |_: &ActorSnapshot| true;
    let not_emitter = |a: &ActorSnapshot| a.kind != ActorKind::Emitter;
    let has_emitter = instances.iter().any(|(_, a)| a.kind == ActorKind::Emitter);
    let not_aggregate = |a: &ActorSnapshot| a.kind != ActorKind::Aggregate;
    let out_pick: &dyn Fn(&ActorSnapshot) -> bool = if has_emitter { &not_aggregate } else { &any };

    // A node runs as several actors: one per slice, plus the emitting half of
    // an aggregate. For one actor the three phases exclude each other, so the
    // actor waiting for room downstream is never the busiest one: each ratio
    // is the worst found over the instances, on its own.
    let worst = |f: fn(&ActorSnapshot) -> f64| instances.iter().map(|(_, a)| f(a)).fold(0.0_f64, f64::max);
    let busy = worst(|a| a.busy);
    let backpressured = worst(|a| a.backpressured);
    let waiting = worst(|a| a.waiting).min(busy);
    let idle = if instances.is_empty() {
        0.0
    } else {
        (1.0 - busy - backpressured).max(0.0)
    };
    let state = instances
        .iter()
        .map(|(_, a)| NodeState::from(a.state))
        .min()
        .unwrap_or(NodeState::Pending);
    let workers: BTreeSet<WorkerId> = instances.iter().map(|(w, _)| (*w).clone()).collect();

    NodeView {
        id: spec_node.id.clone(),
        op: spec_node.op.to_string(),
        kind: descriptor.map_or_else(|| "unknown".to_owned(), |d| d.kind.to_string()),
        description: descriptor.map(|d| d.description.clone()).unwrap_or_default(),
        input_type: descriptor.map(|d| d.input.to_string()),
        output_type: descriptor.map(|d| d.output.to_string()),
        config: spec_node.config.clone(),
        paused,
        bottleneck: false,
        state,
        actors: instances.len(),
        workers: workers.into_iter().collect(),
        records_in: sum_u(|a| a.records_in, &not_emitter),
        records_out: sum_u(|a| a.records_out, out_pick),
        in_per_sec: sum(|a| a.in_per_sec, &not_emitter),
        out_per_sec: sum(|a| a.out_per_sec, out_pick),
        busy,
        idle,
        backpressured,
        waiting,
        queue_len: sum_u(|a| u64::from(a.queue_len), &any),
        queue_cap: sum_u(|a| u64::from(a.queue_cap), &any),
        late_dropped: sum_u(|a| a.late_dropped, &any),
        errors: sum_u(|a| a.errors, &any),
        last_error: instances.iter().find_map(|(_, a)| a.last_error.clone()),
        instances: instances
            .iter()
            .map(|(worker, actor)| InstanceView {
                actor: (*actor).clone(),
                worker: (*worker).clone(),
            })
            .collect(),
    }
}

fn percent(ratio: f64) -> String {
    format!("{:.0}%", ratio * 100.0)
}

/// Find what limits a graph. See the bottleneck rule of `docs/http-api.md`.
///
/// `commits` are the `commit` actors of the graph (their `node` is the root
/// node of their slice).
#[must_use]
pub fn find_bottleneck(spec: &GraphSpec, nodes: &[NodeView], commits: &[&ActorSnapshot]) -> Option<Bottleneck> {
    let by_id: BTreeMap<&NodeId, &NodeView> = nodes.iter().map(|n| (&n.id, n)).collect();
    let mut preds: BTreeMap<&NodeId, Vec<&NodeId>> = BTreeMap::new();
    for (from, to) in &spec.edges {
        preds.entry(to).or_default().push(from);
    }

    // Topological rank (Kahn, description order); nodes left over by a cycle
    // come last.
    let mut rank: BTreeMap<&NodeId, usize> = BTreeMap::new();
    let mut missing: BTreeMap<&NodeId, usize> = spec
        .nodes
        .iter()
        .map(|n| (&n.id, preds.get(&n.id).map_or(0, Vec::len)))
        .collect();
    loop {
        let ready: Vec<&NodeId> = spec
            .nodes
            .iter()
            .map(|n| &n.id)
            .filter(|id| missing.get(id) == Some(&0))
            .collect();
        if ready.is_empty() {
            break;
        }
        for id in ready {
            missing.remove(id);
            rank.insert(id, rank.len());
            for (from, to) in &spec.edges {
                if from == id
                    && let Some(count) = missing.get_mut(to)
                {
                    *count = count.saturating_sub(1);
                }
            }
        }
    }
    for id in missing.into_keys() {
        rank.insert(id, rank.len());
    }

    // Rule 1 and 2.
    let mut best: Option<(&NodeView, String)> = None;
    for node in nodes {
        if node.busy < BUSY_THRESHOLD || node.backpressured >= BACKPRESSURE_THRESHOLD {
            continue;
        }
        let mut seen: BTreeSet<&NodeId> = BTreeSet::new();
        let mut stack: Vec<&NodeId> = preds.get(&node.id).cloned().unwrap_or_default();
        let mut worst_upstream: Option<&NodeView> = None;
        while let Some(id) = stack.pop() {
            if !seen.insert(id) {
                continue;
            }
            if let Some(upstream) = by_id.get(id)
                && upstream.backpressured >= BACKPRESSURE_THRESHOLD
                && worst_upstream.is_none_or(|w| upstream.backpressured > w.backpressured)
            {
                worst_upstream = Some(upstream);
            }
            stack.extend(preds.get(id).into_iter().flatten().copied());
        }
        let Some(upstream) = worst_upstream else {
            continue;
        };
        let better = match &best {
            None => true,
            Some((current, _)) => {
                node.busy > current.busy || (node.busy == current.busy && rank.get(&node.id) > rank.get(&current.id))
            }
        };
        if better {
            let reason = if node.waiting * 2.0 >= node.busy {
                format!(
                    "busy {}, {} of it waiting for its destination, while upstream `{}` is backpressured {}: the destination is the limit, not this worker",
                    percent(node.busy),
                    percent(node.waiting),
                    upstream.id,
                    percent(upstream.backpressured)
                )
            } else {
                format!(
                    "busy {} while upstream `{}` is backpressured {}",
                    percent(node.busy),
                    upstream.id,
                    percent(upstream.backpressured)
                )
            };
            best = Some((node, reason));
        }
    }
    if let Some((node, reason)) = best {
        return Some(Bottleneck {
            node: node.id.clone(),
            kind: node.kind.clone(),
            busy: node.busy,
            waiting: node.waiting,
            limited_by: if node.waiting * 2.0 >= node.busy {
                "destination".to_owned()
            } else {
                "work".to_owned()
            },
            reason,
        });
    }

    // Rule 3: commits.
    let mut commit_best: Option<(&ActorSnapshot, &NodeView)> = None;
    for commit in commits {
        if commit.busy < BUSY_THRESHOLD {
            continue;
        }
        let Some(root) = by_id.get(&commit.node) else {
            continue;
        };
        if root.backpressured < BACKPRESSURE_THRESHOLD {
            continue;
        }
        if commit_best.is_none_or(|(current, _)| commit.busy > current.busy) {
            commit_best = Some((commit, root));
        }
    }
    commit_best.map(|(commit, root)| Bottleneck {
        node: root.id.clone(),
        kind: "commit".to_owned(),
        busy: commit.busy,
        waiting: 0.0,
        limited_by: "store".to_owned(),
        reason: format!(
            "KV commits of slice `{}` are busy {} while `{}` is backpressured {}",
            commit.slice,
            percent(commit.busy),
            root.id,
            percent(root.backpressured)
        ),
    })
}

/// Build the detailed view of a graph.
///
/// Only snapshots of workers alive at `now_ms` (heartbeat within
/// `member_timeout_ms`) are used.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn graph_detail(
    spec: &GraphSpec,
    plan: Option<&GraphPlan>,
    desired: &DesiredState,
    catalog: &Catalog,
    placement: Option<&Placement>,
    members: &[MemberReport],
    now_ms: Timestamp,
    member_timeout_ms: i64,
) -> GraphDetail {
    let in_graph = |tenant: &TenantId, graph: &streaming_core::GraphId| *tenant == spec.tenant && *graph == spec.graph;

    let mut instances: BTreeMap<&NodeId, Vec<(&WorkerId, &ActorSnapshot)>> = BTreeMap::new();
    let mut commits: Vec<&ActorSnapshot> = Vec::new();
    let mut reported: Vec<(&WorkerId, &SliceSnapshot)> = Vec::new();
    let mut total_actors = 0;
    for (member, snapshot) in alive_snapshots(members, now_ms, member_timeout_ms) {
        for actor in snapshot.actors.iter().filter(|a| in_graph(&a.tenant, &a.graph)) {
            total_actors += 1;
            if actor.kind == ActorKind::Commit {
                commits.push(actor);
            } else {
                instances.entry(&actor.node).or_default().push((&member.worker, actor));
            }
        }
        for slice in snapshot.slices.iter().filter(|s| in_graph(&s.tenant, &s.graph)) {
            reported.push((&member.worker, slice));
        }
    }

    let mut nodes: Vec<NodeView> = spec
        .nodes
        .iter()
        .map(|node| {
            aggregate_node(
                node,
                catalog.get(&node.op),
                desired.paused_nodes.contains(&node.id),
                instances.get(&node.id).map_or(&[][..], Vec::as_slice),
            )
        })
        .collect();

    let bottleneck = find_bottleneck(spec, &nodes, &commits);
    if let Some(found) = &bottleneck {
        for node in nodes.iter_mut().filter(|n| n.id == found.node) {
            node.bottleneck = true;
        }
    }

    let fill = |id: &NodeId| {
        nodes
            .iter()
            .find(|n| &n.id == id)
            .filter(|n| n.queue_cap > 0)
            .map_or(0.0, |n| n.queue_len as f64 / n.queue_cap as f64)
    };
    let edges = spec
        .edges
        .iter()
        .map(|(from, to)| EdgeView {
            from: from.clone(),
            to: to.clone(),
            fill: fill(to),
        })
        .collect();

    // Slices.
    let key = spec.key();
    let assigned = |slice: &SliceId| -> Option<WorkerId> {
        placement.and_then(|p| {
            p.assignments
                .get(&SliceKey {
                    tenant: key.tenant.clone(),
                    graph: key.graph.clone(),
                    slice: slice.clone(),
                })
                .cloned()
        })
    };
    let mut slices: Vec<SliceView> = reported
        .iter()
        .map(|(worker, slice)| SliceView {
            slice: (*slice).clone(),
            worker: Some((*worker).clone()),
            assigned_to: assigned(&slice.slice),
        })
        .collect();
    let reported_ids: BTreeSet<&SliceId> = reported.iter().map(|(_, s)| &s.slice).collect();
    let mut unreported_sources = false;
    for slice in plan.iter().flat_map(|p| p.slices.iter()) {
        if reported_ids.contains(slice) {
            continue;
        }
        if matches!(slice, SliceId::Source { .. }) {
            unreported_sources = true;
        }
        slices.push(SliceView {
            slice: SliceSnapshot {
                tenant: spec.tenant.clone(),
                graph: spec.graph.clone(),
                slice: slice.clone(),
                state: ActorState::Starting,
                epoch: 0,
                position: None,
                window: None,
                watermark_ms: None,
                commits: 0,
                commit_ms: 0.0,
                in_flight_epochs: 0,
                fenced: 0,
                restarts: 0,
                throttled: 0,
                last_error: None,
            },
            worker: None,
            assigned_to: assigned(slice),
        });
    }
    slices.sort_by(|a, b| (&a.slice.slice, &a.worker).cmp(&(&b.slice.slice, &b.worker)));

    // Watermark: minimum over source slices, null when one is unknown.
    let source_marks: Vec<Option<Timestamp>> = reported
        .iter()
        .filter(|(_, s)| matches!(s.slice, SliceId::Source { .. }))
        .map(|(_, s)| s.watermark_ms)
        .collect();
    let watermark_ms = if unreported_sources || source_marks.is_empty() {
        None
    } else {
        source_marks
            .into_iter()
            .try_fold(Timestamp::MAX, |min, mark| mark.map(|mark| min.min(mark)))
    };

    GraphDetail {
        tenant: spec.tenant.clone(),
        graph: spec.graph.clone(),
        version: spec.version,
        state: desired.state,
        spec: spec.clone(),
        plan: plan.cloned(),
        desired: desired.clone(),
        bottleneck,
        watermark_ms,
        nodes,
        edges,
        slices,
        total_actors,
    }
}

fn dot_escape(text: &str) -> String {
    text.replace('\\', "\\\\").replace('"', "\\\"")
}

/// The topology as Graphviz DOT: one line per node (label: id and operator)
/// and one per edge.
#[must_use]
pub fn dot(spec: &GraphSpec) -> String {
    let mut out = format!(
        "digraph \"{}\" {{\n",
        dot_escape(&format!("{}/{}", spec.tenant, spec.graph))
    );
    for node in &spec.nodes {
        out.push_str(&format!(
            "  \"{}\" [label=\"{}\\n{}\"];\n",
            dot_escape(node.id.as_str()),
            dot_escape(node.id.as_str()),
            dot_escape(node.op.as_str())
        ));
    }
    for (from, to) in &spec.edges {
        out.push_str(&format!(
            "  \"{}\" -> \"{}\";\n",
            dot_escape(from.as_str()),
            dot_escape(to.as_str())
        ));
    }
    out.push_str("}\n");
    out
}

#[cfg(test)]
mod tests {
    use streaming_core::{GraphId, NodeSpec, OpKind, OpRef, TypeTag};

    use super::*;

    const NOW: i64 = 1_000_000;
    const TIMEOUT: i64 = 5_000;

    fn node(id: &str) -> NodeId {
        NodeId::new(id).unwrap()
    }

    fn worker(id: &str) -> WorkerId {
        WorkerId::new(id).unwrap()
    }

    fn descriptor(op: &str, kind: OpKind) -> OpDescriptor {
        OpDescriptor {
            op: OpRef::new(op).unwrap(),
            kind,
            input: TypeTag::Any,
            output: TypeTag::Any,
            description: format!("{op} description"),
            params: Vec::new(),
        }
    }

    fn catalog() -> Catalog {
        let mut catalog = Catalog::default();
        for (op, kind) in [
            ("t/src@1", OpKind::Source),
            ("t/key@1", OpKind::KeyBy),
            ("t/win@1", OpKind::Window),
            ("t/count@1", OpKind::Aggregate),
            ("t/sink@1", OpKind::Sink),
        ] {
            catalog.insert(descriptor(op, kind));
        }
        catalog
    }

    /// events -> key -> win -> count -> out
    fn spec() -> GraphSpec {
        let nodes = [
            ("events", "t/src@1"),
            ("key", "t/key@1"),
            ("win", "t/win@1"),
            ("count", "t/count@1"),
            ("out", "t/sink@1"),
        ];
        GraphSpec {
            tenant: TenantId::new("acme").unwrap(),
            graph: GraphId::new("g").unwrap(),
            version: 3,
            settings: Default::default(),
            nodes: nodes
                .iter()
                .map(|(id, op)| NodeSpec {
                    id: node(id),
                    op: OpRef::new(*op).unwrap(),
                    config: Value::Null,
                })
                .collect(),
            edges: nodes
                .windows(2)
                .map(|pair| (node(pair[0].0), node(pair[1].0)))
                .collect(),
        }
    }

    fn actor(node_id: &str, kind: ActorKind) -> ActorSnapshot {
        let slice = match kind {
            ActorKind::Emitter => SliceId::Emitter { node: node("count") },
            _ => SliceId::Source {
                node: node("events"),
                partition: 0,
            },
        };
        ActorSnapshot {
            tenant: TenantId::new("acme").unwrap(),
            graph: GraphId::new("g").unwrap(),
            slice,
            node: node(node_id),
            kind,
            op: OpRef::new("t/x@1").unwrap(),
            state: ActorState::Running,
            records_in: 0,
            records_out: 0,
            in_per_sec: 0.0,
            out_per_sec: 0.0,
            busy: 0.0,
            idle: 0.0,
            backpressured: 0.0,
            waiting: 0.0,
            queue_len: 0,
            queue_cap: 0,
            late_dropped: 0,
            errors: 0,
            last_error: None,
        }
    }

    fn with_load(mut a: ActorSnapshot, busy: f64, backpressured: f64) -> ActorSnapshot {
        a.busy = busy;
        a.backpressured = backpressured;
        a.idle = (1.0 - busy - backpressured).max(0.0);
        a
    }

    fn slice_snapshot(id: &str, watermark: Option<i64>) -> SliceSnapshot {
        SliceSnapshot {
            tenant: TenantId::new("acme").unwrap(),
            graph: GraphId::new("g").unwrap(),
            slice: id.parse().unwrap(),
            state: ActorState::Running,
            epoch: 5,
            position: None,
            window: None,
            watermark_ms: watermark,
            commits: 1,
            commit_ms: 1.0,
            in_flight_epochs: 0,
            fenced: 0,
            restarts: 0,
            throttled: 0,
            last_error: None,
        }
    }

    fn member(id: &str, heartbeat_ms: i64) -> MemberInfo {
        MemberInfo {
            worker: worker(id),
            generation: 1,
            heartbeat_ms,
            load: Load::default(),
            actors: 0,
            slices: 0,
            api_addr: None,
            version: "0.1.0".to_owned(),
        }
    }

    fn snapshot(id: &str, actors: Vec<ActorSnapshot>, slices: Vec<SliceSnapshot>) -> WorkerSnapshot {
        WorkerSnapshot {
            worker: worker(id),
            at_ms: NOW,
            interval_ms: 1000,
            load: Load::default(),
            actors,
            slices,
            tenants: BTreeMap::new(),
        }
    }

    fn alive(id: &str, actors: Vec<ActorSnapshot>, slices: Vec<SliceSnapshot>) -> MemberReport {
        (member(id, NOW), Some(snapshot(id, actors, slices)))
    }

    fn detail_of(members: &[MemberReport]) -> GraphDetail {
        graph_detail(
            &spec(),
            None,
            &DesiredState::default(),
            &catalog(),
            None,
            members,
            NOW,
            TIMEOUT,
        )
    }

    fn node_of<'a>(detail: &'a GraphDetail, id: &str) -> &'a NodeView {
        detail.nodes.iter().find(|n| n.id.as_str() == id).unwrap()
    }

    #[test]
    fn each_ratio_is_the_worst_over_the_instances() {
        let a = with_load(actor("count", ActorKind::Aggregate), 0.2, 0.1);
        let b = with_load(actor("count", ActorKind::Aggregate), 0.9, 0.05);
        let detail = detail_of(&[alive("w1", vec![a, b], vec![])]);
        let count = node_of(&detail, "count");
        assert_eq!(count.actors, 2);
        assert_eq!(count.busy, 0.9);
        assert_eq!(count.backpressured, 0.1);
        assert!(count.idle.abs() < 1e-9);
        assert_eq!(count.kind, "aggregate");
        assert_eq!(count.input_type.as_deref(), Some("*"));
        assert_eq!(count.description, "t/count@1 description");
    }

    #[test]
    fn sums_with_an_emitter_instance() {
        let mut agg = actor("count", ActorKind::Aggregate);
        agg.records_in = 100;
        agg.in_per_sec = 10.0;
        agg.records_out = 90;
        agg.out_per_sec = 9.0;
        agg.queue_len = 2;
        agg.queue_cap = 8;
        agg.errors = 1;
        agg.late_dropped = 4;
        let mut emitter = actor("count", ActorKind::Emitter);
        emitter.records_in = 7;
        emitter.in_per_sec = 0.7;
        emitter.records_out = 5;
        emitter.out_per_sec = 0.5;
        emitter.queue_len = 1;
        emitter.queue_cap = 8;
        emitter.errors = 2;
        emitter.last_error = Some("boom".into());
        let detail = detail_of(&[alive("w1", vec![agg, emitter], vec![])]);
        let count = node_of(&detail, "count");
        assert_eq!(count.records_in, 100);
        assert_eq!(count.in_per_sec, 10.0);
        assert_eq!(count.records_out, 5);
        assert_eq!(count.out_per_sec, 0.5);
        assert_eq!(count.queue_len, 3);
        assert_eq!(count.queue_cap, 16);
        assert_eq!(count.errors, 3);
        assert_eq!(count.late_dropped, 4);
        assert_eq!(count.last_error.as_deref(), Some("boom"));
    }

    #[test]
    fn sums_without_an_emitter_instance() {
        let mut a1 = actor("count", ActorKind::Aggregate);
        a1.records_in = 10;
        a1.records_out = 3;
        a1.out_per_sec = 1.0;
        let mut a2 = actor("count", ActorKind::Aggregate);
        a2.records_in = 20;
        a2.records_out = 4;
        a2.out_per_sec = 2.0;
        let detail = detail_of(&[alive("w1", vec![a1], vec![]), alive("w2", vec![a2], vec![])]);
        let count = node_of(&detail, "count");
        assert_eq!(count.records_in, 30);
        assert_eq!(count.records_out, 7);
        assert_eq!(count.out_per_sec, 3.0);
        assert_eq!(count.workers, vec![worker("w1"), worker("w2")]);
        // Instances carry their worker.
        let json = serde_json::to_value(&count.instances[1]).unwrap();
        assert_eq!(json["worker"], "w2");
        assert_eq!(json["node"], "count");
    }

    #[test]
    fn state_is_the_worst_instance_state() {
        let mk = |state| {
            let mut a = actor("count", ActorKind::Aggregate);
            a.state = state;
            a
        };
        let state_of = |states: Vec<ActorState>| {
            let actors = states.into_iter().map(mk).collect();
            node_of(&detail_of(&[alive("w1", actors, vec![])]), "count").state
        };
        use ActorState::*;
        assert_eq!(state_of(vec![Running, Stopped, Failed]), NodeState::Failed);
        assert_eq!(state_of(vec![Running, Starting, Paused]), NodeState::Starting);
        assert_eq!(state_of(vec![Running, Draining, Paused]), NodeState::Paused);
        assert_eq!(state_of(vec![Running, Draining]), NodeState::Draining);
        assert_eq!(state_of(vec![Stopped, Running]), NodeState::Running);
        assert_eq!(state_of(vec![Stopped]), NodeState::Stopped);
        let detail = detail_of(&[]);
        assert_eq!(node_of(&detail, "count").state, NodeState::Pending);
        assert_eq!(
            serde_json::to_value(node_of(&detail, "count")).unwrap()["state"],
            "pending"
        );
    }

    #[test]
    fn commit_actors_are_not_instances() {
        let commit = actor("events", ActorKind::Commit);
        let source = actor("events", ActorKind::Source);
        let detail = detail_of(&[alive("w1", vec![commit, source], vec![])]);
        assert_eq!(node_of(&detail, "events").actors, 1);
        assert_eq!(detail.total_actors, 2);
    }

    #[test]
    fn dead_workers_are_ignored() {
        let mut busy = actor("count", ActorKind::Aggregate);
        busy.busy = 0.9;
        let dead = (
            member("w2", NOW - TIMEOUT - 1),
            Some(snapshot(
                "w2",
                vec![busy],
                vec![slice_snapshot("src.events.0", Some(1))],
            )),
        );
        let boundary = (member("w3", NOW - TIMEOUT), Some(snapshot("w3", vec![], vec![])));
        let detail = detail_of(&[dead, boundary]);
        assert_eq!(node_of(&detail, "count").actors, 0);
        assert_eq!(node_of(&detail, "count").state, NodeState::Pending);
        assert!(detail.slices.is_empty());
        // A member without snapshot is simply absent.
        let none = detail_of(&[(member("w4", NOW), None)]);
        assert_eq!(none.total_actors, 0);
    }

    #[test]
    fn other_graphs_are_ignored() {
        let mut foreign = actor("count", ActorKind::Aggregate);
        foreign.tenant = TenantId::new("globex").unwrap();
        let mut foreign_graph = actor("count", ActorKind::Aggregate);
        foreign_graph.graph = GraphId::new("other").unwrap();
        let detail = detail_of(&[alive("w1", vec![foreign, foreign_graph], vec![])]);
        assert_eq!(detail.total_actors, 0);
    }

    #[test]
    fn paused_flag_and_edge_fill() {
        let mut count = actor("count", ActorKind::Aggregate);
        count.queue_len = 12;
        count.queue_cap = 16;
        let desired = DesiredState {
            state: RunState::Paused,
            paused_nodes: [node("key")].into(),
            updated_ms: 0,
        };
        let detail = graph_detail(
            &spec(),
            None,
            &desired,
            &catalog(),
            None,
            &[alive("w1", vec![count], vec![])],
            NOW,
            TIMEOUT,
        );
        assert!(node_of(&detail, "key").paused);
        assert!(!node_of(&detail, "count").paused);
        assert_eq!(detail.state, RunState::Paused);
        let fill = |to: &str| detail.edges.iter().find(|e| e.to.as_str() == to).unwrap().fill;
        assert_eq!(fill("count"), 0.75);
        assert_eq!(fill("win"), 0.0, "capacity 0 gives 0");
    }

    #[test]
    fn unknown_operator_is_tolerated() {
        let detail = graph_detail(
            &spec(),
            None,
            &DesiredState::default(),
            &Catalog::default(),
            None,
            &[],
            NOW,
            TIMEOUT,
        );
        let events = node_of(&detail, "events");
        assert_eq!(events.kind, "unknown");
        assert_eq!(events.input_type, None);
    }

    #[test]
    fn slices_carry_assignment_and_missing_ones_are_starting() {
        let plan = GraphPlan {
            spec_version: 3,
            partitions: [(node("events"), 2)].into(),
            slices: vec![
                "src.events.0".parse().unwrap(),
                "src.events.1".parse().unwrap(),
                "agg.count".parse().unwrap(),
            ],
            planned_ms: 0,
            error: Some("nope".into()),
            ..GraphPlan::default()
        };
        let placement = Placement {
            version: 1,
            computed_by: None,
            computed_ms: 0,
            assignments: [
                ("acme/g/src.events.0".parse().unwrap(), worker("w1")),
                ("acme/g/src.events.1".parse().unwrap(), worker("w2")),
            ]
            .into(),
        };
        let members = [alive(
            "w1",
            vec![],
            vec![
                slice_snapshot("src.events.0", Some(10)),
                slice_snapshot("src.events.9", Some(10)),
            ],
        )];
        let detail = graph_detail(
            &spec(),
            Some(&plan),
            &DesiredState::default(),
            &catalog(),
            Some(&placement),
            &members,
            NOW,
            TIMEOUT,
        );
        assert_eq!(detail.slices.len(), 4);
        let find = |id: &str| detail.slices.iter().find(|s| s.slice.slice.to_string() == id).unwrap();
        assert_eq!(find("src.events.0").worker, Some(worker("w1")));
        assert_eq!(find("src.events.0").assigned_to, Some(worker("w1")));
        assert_eq!(find("src.events.9").assigned_to, None, "reported, not planned");
        let missing = find("src.events.1");
        assert_eq!(missing.worker, None);
        assert_eq!(missing.assigned_to, Some(worker("w2")));
        assert_eq!(missing.slice.state, ActorState::Starting);
        assert_eq!(find("agg.count").slice.state, ActorState::Starting);
        let json = serde_json::to_value(missing).unwrap();
        assert_eq!(json["worker"], Value::Null);
        assert_eq!(json["slice"], "src.events.1");
        assert_eq!(json["assigned_to"], "w2");
        let summary = graph_summary(&detail, 42);
        assert_eq!(summary.slices, 4);
        assert_eq!(summary.error.as_deref(), Some("nope"));
        assert_eq!(summary.updated_ms, 42);
    }

    #[test]
    fn watermark_is_the_minimum_or_null() {
        let plan = |slices: &[&str]| GraphPlan {
            slices: slices.iter().map(|s| s.parse().unwrap()).collect(),
            ..GraphPlan::default()
        };
        let run = |plan: Option<&GraphPlan>, slices: Vec<SliceSnapshot>| {
            graph_detail(
                &spec(),
                plan,
                &DesiredState::default(),
                &catalog(),
                None,
                &[alive("w1", vec![], slices)],
                NOW,
                TIMEOUT,
            )
            .watermark_ms
        };
        // Minimum over source slices; emitter slices do not count.
        assert_eq!(
            run(
                None,
                vec![
                    slice_snapshot("src.events.0", Some(30)),
                    slice_snapshot("src.events.1", Some(20)),
                    slice_snapshot("agg.count", Some(5)),
                ]
            ),
            Some(20)
        );
        // A source slice without watermark.
        assert_eq!(
            run(
                None,
                vec![
                    slice_snapshot("src.events.0", Some(30)),
                    slice_snapshot("src.events.1", None),
                ]
            ),
            None
        );
        // A planned source slice nobody reports.
        let p = plan(&["src.events.0", "src.events.1"]);
        assert_eq!(run(Some(&p), vec![slice_snapshot("src.events.0", Some(30))]), None);
        // No source slice at all.
        assert_eq!(run(None, vec![slice_snapshot("agg.count", Some(5))]), None);
        assert_eq!(run(None, vec![]), None);
    }

    fn bottleneck_of(actors: Vec<ActorSnapshot>) -> (Option<Bottleneck>, GraphDetail) {
        let detail = detail_of(&[alive("w1", actors, vec![])]);
        (detail.bottleneck.clone(), detail)
    }

    #[test]
    fn bottleneck_clear() {
        let (bottleneck, detail) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.1, 0.8),
            with_load(actor("count", ActorKind::Aggregate), 0.97, 0.0),
        ]);
        let bottleneck = bottleneck.unwrap();
        assert_eq!(bottleneck.node, node("count"));
        assert_eq!(bottleneck.kind, "aggregate");
        assert_eq!(bottleneck.busy, 0.97);
        assert!(bottleneck.reason.contains("events"), "{}", bottleneck.reason);
        assert!(node_of(&detail, "count").bottleneck);
        assert!(!node_of(&detail, "events").bottleneck);
        let summary = graph_summary(&detail, 0);
        assert_eq!(summary.bottleneck.unwrap().node, node("count"));
    }

    #[test]
    fn bottleneck_is_found_through_transitive_upstream() {
        // Only the source is backpressured; `out` is two hops... four hops away.
        let (bottleneck, _) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.1, 0.8),
            with_load(actor("out", ActorKind::Sink), 0.6, 0.0),
        ]);
        assert_eq!(bottleneck.unwrap().node, node("out"));
    }

    #[test]
    fn a_waiting_emitter_is_not_hidden_by_busy_aggregates() {
        // A sink slower than the windows close: the emitting half of `count`
        // waits for it, while its accumulating halves keep working upstream.
        let (bottleneck, detail) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.3, 0.0),
            with_load(actor("count", ActorKind::Aggregate), 0.2, 0.0),
            with_load(actor("count", ActorKind::Emitter), 0.0, 1.0),
            with_load(actor("out", ActorKind::Sink), 1.0, 0.0),
        ]);
        let count = node_of(&detail, "count");
        assert_eq!((count.busy, count.backpressured), (0.2, 1.0));
        let bottleneck = bottleneck.unwrap();
        assert_eq!(bottleneck.node, node("out"));
        assert!(bottleneck.reason.contains("count"), "{}", bottleneck.reason);
    }

    #[test]
    fn a_bottleneck_says_whether_its_destination_or_its_work_limits_it() {
        // The same occupation, spent waiting for the destination or computing.
        for (waiting, limited_by) in [(0.95, "destination"), (0.1, "work")] {
            let mut sink = with_load(actor("out", ActorKind::Sink), 1.0, 0.0);
            sink.waiting = waiting;
            let (bottleneck, detail) =
                bottleneck_of(vec![with_load(actor("events", ActorKind::Source), 0.1, 0.8), sink]);
            let bottleneck = bottleneck.unwrap();
            assert_eq!(bottleneck.node, node("out"));
            assert_eq!(bottleneck.limited_by, limited_by);
            assert_eq!(bottleneck.waiting, waiting);
            assert_eq!(node_of(&detail, "out").waiting, waiting);
            assert_eq!(
                bottleneck.reason.contains("the destination is the limit"),
                limited_by == "destination",
                "{}",
                bottleneck.reason
            );
        }
    }

    #[test]
    fn bottleneck_tie_goes_downstream() {
        let (bottleneck, _) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.1, 0.8),
            with_load(actor("key", ActorKind::KeyBy), 0.9, 0.0),
            with_load(actor("count", ActorKind::Aggregate), 0.9, 0.0),
        ]);
        assert_eq!(bottleneck.unwrap().node, node("count"));
        // The highest busy still wins over position.
        let (bottleneck, _) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.1, 0.8),
            with_load(actor("key", ActorKind::KeyBy), 0.95, 0.0),
            with_load(actor("count", ActorKind::Aggregate), 0.9, 0.0),
        ]);
        assert_eq!(bottleneck.unwrap().node, node("key"));
    }

    #[test]
    fn bottleneck_requires_low_own_backpressure_and_high_busy() {
        // `key` is busy but itself backpressured: not a candidate; `count` is.
        let (bottleneck, _) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.1, 0.8),
            with_load(actor("key", ActorKind::KeyBy), 0.9, 0.3),
            with_load(actor("count", ActorKind::Aggregate), 0.5, 0.29),
        ]);
        assert_eq!(bottleneck.unwrap().node, node("count"));
        // Not busy enough.
        let (bottleneck, _) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.1, 0.8),
            with_load(actor("count", ActorKind::Aggregate), 0.49, 0.0),
        ]);
        assert_eq!(bottleneck, None);
    }

    #[test]
    fn no_bottleneck_without_upstream_backpressure() {
        let (bottleneck, detail) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.1, 0.29),
            with_load(actor("count", ActorKind::Aggregate), 0.99, 0.0),
        ]);
        assert_eq!(bottleneck, None);
        assert!(detail.nodes.iter().all(|n| !n.bottleneck));
        // Downstream backpressure alone does not count either.
        let (bottleneck, _) = bottleneck_of(vec![
            with_load(actor("count", ActorKind::Aggregate), 0.99, 0.0),
            with_load(actor("out", ActorKind::Sink), 0.1, 0.9),
        ]);
        assert_eq!(bottleneck, None);
    }

    #[test]
    fn commit_bottleneck() {
        let (bottleneck, detail) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.2, 0.8),
            with_load(actor("events", ActorKind::Commit), 0.7, 0.0),
        ]);
        let bottleneck = bottleneck.unwrap();
        assert_eq!(bottleneck.node, node("events"));
        assert_eq!(bottleneck.kind, "commit");
        assert_eq!(bottleneck.busy, 0.7);
        assert!(node_of(&detail, "events").bottleneck);

        // A node bottleneck takes precedence over commits.
        let (bottleneck, _) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.2, 0.8),
            with_load(actor("events", ActorKind::Commit), 0.9, 0.0),
            with_load(actor("count", ActorKind::Aggregate), 0.6, 0.0),
        ]);
        assert_eq!(bottleneck.unwrap().kind, "aggregate");

        // Commit not busy enough, or root not backpressured.
        let (bottleneck, _) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.2, 0.8),
            with_load(actor("events", ActorKind::Commit), 0.49, 0.0),
        ]);
        assert_eq!(bottleneck, None);
        let (bottleneck, _) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.2, 0.1),
            with_load(actor("events", ActorKind::Commit), 0.9, 0.0),
        ]);
        assert_eq!(bottleneck, None);
    }

    #[test]
    fn idle_is_what_busy_and_backpressure_leave() {
        let (_, detail) = bottleneck_of(vec![with_load(actor("key", ActorKind::KeyBy), 0.25, 0.5)]);
        assert_eq!(node_of(&detail, "key").idle, 0.25);
    }

    #[test]
    fn the_most_backpressured_upstream_is_blamed() {
        // Both upstream nodes are backpressured: the reason names the worse
        // one, whichever is nearer, and the nearer one on a tie.
        for (events, key, blamed) in [(0.9, 0.5, "events"), (0.5, 0.9, "key"), (0.8, 0.8, "key")] {
            let (bottleneck, _) = bottleneck_of(vec![
                with_load(actor("events", ActorKind::Source), 0.1, events),
                with_load(actor("key", ActorKind::KeyBy), 0.1, key),
                with_load(actor("count", ActorKind::Aggregate), 0.97, 0.0),
            ]);
            let reason = bottleneck.unwrap().reason;
            assert!(
                reason.contains(&format!("upstream `{blamed}` is backpressured")),
                "{reason}"
            );
        }
    }

    #[test]
    fn the_busier_candidate_wins_wherever_it_sits() {
        let (bottleneck, _) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.1, 0.8),
            with_load(actor("key", ActorKind::KeyBy), 0.9, 0.0),
            with_load(actor("count", ActorKind::Aggregate), 0.95, 0.0),
        ]);
        let bottleneck = bottleneck.unwrap();
        assert_eq!(bottleneck.node, node("count"));
        assert!(bottleneck.reason.starts_with("busy 95% "), "{}", bottleneck.reason);
    }

    #[test]
    fn commit_thresholds_are_inclusive() {
        let (bottleneck, _) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.2, BACKPRESSURE_THRESHOLD),
            with_load(actor("events", ActorKind::Commit), BUSY_THRESHOLD, 0.0),
        ]);
        assert_eq!(bottleneck.unwrap().kind, "commit");
    }

    #[test]
    fn the_busiest_commit_is_reported() {
        let commit = |partition, busy| {
            let mut a = with_load(actor("events", ActorKind::Commit), busy, 0.0);
            a.slice = SliceId::Source {
                node: node("events"),
                partition,
            };
            a
        };
        for (first, second, reported) in [(0.6, 0.9, 0.9), (0.9, 0.6, 0.9), (0.7, 0.7, 0.7)] {
            let (bottleneck, _) = bottleneck_of(vec![
                with_load(actor("events", ActorKind::Source), 0.2, 0.8),
                commit(0, first),
                commit(1, second),
            ]);
            let bottleneck = bottleneck.unwrap();
            assert_eq!(bottleneck.busy, reported);
            // On a tie, the first commit found is kept.
            if first == second {
                assert!(bottleneck.reason.contains("`src.events.0`"), "{}", bottleneck.reason);
            }
        }
    }

    #[test]
    fn nothing_to_report() {
        let (bottleneck, _) = bottleneck_of(vec![]);
        assert_eq!(bottleneck, None);
        let (bottleneck, _) = bottleneck_of(vec![
            with_load(actor("events", ActorKind::Source), 0.2, 0.0),
            with_load(actor("count", ActorKind::Aggregate), 0.2, 0.0),
        ]);
        assert_eq!(bottleneck, None);
    }

    #[test]
    fn summary_rates_come_from_sources_and_sinks() {
        let mut source = actor("events", ActorKind::Source);
        source.out_per_sec = 100.0;
        let mut source2 = actor("events", ActorKind::Source);
        source2.out_per_sec = 50.0;
        let mut sink = actor("out", ActorKind::Sink);
        sink.out_per_sec = 4.0;
        let mut middle = actor("key", ActorKind::KeyBy);
        middle.out_per_sec = 999.0;
        let detail = detail_of(&[alive("w1", vec![source, source2, sink, middle], vec![])]);
        let summary = graph_summary(&detail, 0);
        assert_eq!(summary.in_per_sec, 150.0);
        assert_eq!(summary.out_per_sec, 4.0);
        assert_eq!(summary.nodes, 5);
        assert_eq!(summary.actors, 4);
        assert_eq!(summary.state, RunState::Running);
    }

    #[test]
    fn cluster_view_marks_dead_workers() {
        let mut snap = snapshot("w1", vec![], vec![]);
        snap.tenants.insert(
            TenantId::new("acme").unwrap(),
            TenantUsage {
                actors: 7,
                in_per_sec: 1200.0,
                busy: 0.4,
            },
        );
        let members = [
            (member("w1", NOW - TIMEOUT), Some(snap)),
            (member("w2", NOW - TIMEOUT - 1), None),
        ];
        let view = cluster_view(12, NOW, &members, None, None, TIMEOUT);
        assert_eq!(view.epoch, 12);
        assert!(view.workers[0].alive);
        assert!(!view.workers[1].alive);
        assert_eq!(view.workers[0].tenants[&TenantId::new("acme").unwrap()].actors, 7);
        assert!(view.workers[1].tenants.is_empty());
        assert_eq!(view.placement.version, 0);
        let json = serde_json::to_value(&view).unwrap();
        assert_eq!(json["kv"], Value::Null);
        assert_eq!(json["workers"][0]["tenants"]["acme"]["in_per_sec"], 1200.0);
    }

    #[test]
    fn tenants_are_grouped_and_use_alive_snapshots() {
        let entry = |tenant: &str, graph: &str| GraphIndexEntry {
            tenant: TenantId::new(tenant).unwrap(),
            graph: GraphId::new(graph).unwrap(),
            version: 1,
            updated_ms: 0,
            deleting: false,
        };
        let index = [entry("acme", "a"), entry("acme", "b"), entry("globex", "c")];
        let usage = |actors, rate| TenantUsage {
            actors,
            in_per_sec: rate,
            busy: 0.0,
        };
        let mut s1 = snapshot("w1", vec![], vec![]);
        s1.tenants.insert(TenantId::new("acme").unwrap(), usage(5, 100.0));
        s1.tenants.insert(TenantId::new("orphan").unwrap(), usage(1, 1.0));
        let mut s2 = snapshot("w2", vec![], vec![]);
        s2.tenants.insert(TenantId::new("acme").unwrap(), usage(9, 900.0));
        let mut dead = snapshot("w3", vec![], vec![]);
        dead.tenants.insert(TenantId::new("acme").unwrap(), usage(100, 1e6));
        let members = [
            (member("w1", NOW), Some(s1)),
            (member("w2", NOW), Some(s2)),
            (member("w3", 0), Some(dead)),
        ];
        let view = tenants_view(&index, &members, NOW, TIMEOUT);
        assert_eq!(view.tenants.len(), 2, "tenants come from the index only");
        assert_eq!(view.tenants[0].tenant.as_str(), "acme");
        assert_eq!(view.tenants[0].graphs, 2);
        assert_eq!(view.tenants[0].actors, 14);
        assert_eq!(view.tenants[0].in_per_sec, 1000.0);
        assert_eq!(view.tenants[1].graphs, 1);
        assert_eq!(view.tenants[1].actors, 0);
    }

    #[test]
    fn catalog_is_sorted() {
        let view = catalog_view(&catalog());
        let ops: Vec<&str> = view.ops.iter().map(|d| d.op.as_str()).collect();
        let mut sorted = ops.clone();
        sorted.sort_unstable();
        assert_eq!(ops, sorted);
        assert_eq!(ops.len(), 5);
    }

    #[test]
    fn dot_has_one_line_per_node_and_edge() {
        let text = dot(&spec());
        assert!(text.starts_with("digraph \"acme/g\" {"), "{text}");
        assert!(text.contains("\"events\" [label=\"events\\nt/src@1\"];"), "{text}");
        assert!(text.contains("\"events\" -> \"key\";"), "{text}");
        assert_eq!(text.lines().count(), 1 + 5 + 4 + 1);
    }
}
