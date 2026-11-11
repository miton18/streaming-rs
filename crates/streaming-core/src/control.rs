//! Documents exchanged through the KV between workers, the controller, the
//! HTTP API and the CLI. See [`crate::keys`] for where each one lives.
//!
//! The control plane is deliberately KV-only: any process holding a KV handle
//! can submit a graph, pause it or read its status, and workers converge on
//! what they read. Nothing here is needed for correctness of the data plane:
//! a stale or conflicting document can at worst make two workers run the same
//! slice, which the commit log arbitrates.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::data::Position;
use crate::ids::{GraphId, NodeId, OpRef, SliceId, SliceKey, TenantId, WorkerId};
use crate::time::Timestamp;

/// Resource usage of a worker process. Ratios are in `0.0..=1.0`.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct Load {
    /// CPU usage of the process, relative to the cores it may use.
    pub cpu: f64,
    /// Memory usage of the process, relative to its limit.
    pub memory: f64,
    /// Resident memory, in bytes.
    pub memory_bytes: u64,
}

impl Load {
    /// Whether the worker has no headroom left. Only a saturated worker
    /// justifies spreading a graph over more workers.
    #[must_use]
    pub fn is_saturated(&self) -> bool {
        self.cpu >= 0.85 || self.memory >= 0.85
    }
}

/// What a worker advertises about itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberInfo {
    /// Worker id.
    pub worker: WorkerId,
    /// Changes every time the worker process restarts.
    pub generation: u64,
    /// Wall-clock time of the last heartbeat.
    pub heartbeat_ms: Timestamp,
    /// Resource usage.
    #[serde(default)]
    pub load: Load,
    /// Number of actors currently running on the worker.
    #[serde(default)]
    pub actors: u32,
    /// Number of slices currently running on the worker.
    #[serde(default)]
    pub slices: u32,
    /// Address of the worker's HTTP API, when it serves one.
    #[serde(default)]
    pub api_addr: Option<String>,
    /// Framework version of the worker binary.
    #[serde(default)]
    pub version: String,
}

impl MemberInfo {
    /// Whether the last heartbeat is recent enough.
    #[must_use]
    pub fn is_alive(&self, now_ms: Timestamp, timeout_ms: i64) -> bool {
        now_ms - self.heartbeat_ms <= timeout_ms
    }
}

/// Entry of the graph index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphIndexEntry {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Graph id.
    pub graph: GraphId,
    /// Version of the description currently stored.
    pub version: u64,
    /// When the description was last changed.
    pub updated_ms: Timestamp,
    /// Set when the graph is being deleted: the controller removes its state,
    /// then this entry.
    #[serde(default)]
    pub deleting: bool,
}

/// Run state requested for a graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    /// Sources read, everything flows.
    #[default]
    Running,
    /// Sources stop reading; state and positions are kept.
    Paused,
    /// Sources stop reading, what is in flight is committed, then slices stop.
    Draining,
    /// No actor runs; state and positions are kept.
    Stopped,
}

/// What operators want a graph to do.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DesiredState {
    /// Run state of the whole graph.
    #[serde(default)]
    pub state: RunState,
    /// Nodes paused individually. A paused node stops consuming its inputs;
    /// backpressure then stalls everything upstream of it.
    #[serde(default)]
    pub paused_nodes: BTreeSet<NodeId>,
    /// When this document was last changed.
    #[serde(default)]
    pub updated_ms: Timestamp,
}

/// How a graph is cut into slices, computed by the controller.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GraphPlan {
    /// Version of the description this plan was computed from.
    pub spec_version: u64,
    /// Partition count of every source node.
    #[serde(default)]
    pub partitions: BTreeMap<NodeId, u32>,
    /// Every slice to run.
    #[serde(default)]
    pub slices: Vec<SliceId>,
    /// When the plan was computed.
    #[serde(default)]
    pub planned_ms: Timestamp,
    /// Why the graph cannot be planned (unknown operator, unreachable source, ...).
    #[serde(default)]
    pub error: Option<String>,
}

/// Which worker runs which slice, computed by the controller.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Placement {
    /// Incremented on every change.
    pub version: u64,
    /// The worker that computed this placement.
    #[serde(default)]
    pub computed_by: Option<WorkerId>,
    /// When it was computed.
    #[serde(default)]
    pub computed_ms: Timestamp,
    /// Slice -> worker.
    #[serde(default)]
    pub assignments: BTreeMap<SliceKey, WorkerId>,
}

/// Role of an actor inside a slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    /// Reads one partition of a source.
    Source,
    /// Stateless transformation.
    Map,
    /// Key annotation.
    KeyBy,
    /// Window annotation.
    Window,
    /// Accumulating half of an aggregate node.
    Aggregate,
    /// Emitting half of an aggregate node: reads closed windows.
    Emitter,
    /// Writes to a sink.
    Sink,
    /// Commits the epochs of a slice to the KV.
    Commit,
}

/// Life-cycle state of an actor or of a slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorState {
    /// Restoring state from the KV.
    #[default]
    Starting,
    /// Processing.
    Running,
    /// Paused by the control plane.
    Paused,
    /// Finishing in-flight work before stopping.
    Draining,
    /// Stopped cleanly.
    Stopped,
    /// Failed; the slice restarts from its last commit.
    Failed,
}

/// Metrics of one actor, as of one snapshot.
///
/// `busy + idle + backpressured` is about 1: the three ratios split the last
/// interval between working, waiting for input and waiting for room downstream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActorSnapshot {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Graph.
    pub graph: GraphId,
    /// Slice the actor belongs to.
    pub slice: SliceId,
    /// Graph node the actor runs. For [`ActorKind::Commit`], the root node of the slice.
    pub node: NodeId,
    /// Role of the actor.
    pub kind: ActorKind,
    /// Operator reference of the node.
    pub op: OpRef,
    /// Life-cycle state.
    #[serde(default)]
    pub state: ActorState,
    /// Records received since the actor started.
    #[serde(default)]
    pub records_in: u64,
    /// Records sent downstream (or written, for a sink) since the actor started.
    #[serde(default)]
    pub records_out: u64,
    /// Input rate over the last interval, in records per second.
    #[serde(default)]
    pub in_per_sec: f64,
    /// Output rate over the last interval, in records per second.
    #[serde(default)]
    pub out_per_sec: f64,
    /// Share of the last interval spent working.
    #[serde(default)]
    pub busy: f64,
    /// Share of the last interval spent waiting for input.
    #[serde(default)]
    pub idle: f64,
    /// Share of the last interval spent waiting for room downstream.
    #[serde(default)]
    pub backpressured: f64,
    /// Chunks waiting in the actor's input edges.
    #[serde(default)]
    pub queue_len: u32,
    /// Total capacity of the actor's input edges, in chunks.
    #[serde(default)]
    pub queue_cap: u32,
    /// Records dropped because their window was already closed.
    #[serde(default)]
    pub late_dropped: u64,
    /// Errors since the actor started.
    #[serde(default)]
    pub errors: u64,
    /// Most recent error, if any.
    #[serde(default)]
    pub last_error: Option<String>,
}

/// Progress of one slice, as of one snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SliceSnapshot {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Graph.
    pub graph: GraphId,
    /// Slice.
    pub slice: SliceId,
    /// Life-cycle state.
    #[serde(default)]
    pub state: ActorState,
    /// Last committed epoch.
    #[serde(default)]
    pub epoch: u64,
    /// Last committed source position.
    #[serde(default)]
    pub position: Option<Position>,
    /// Last committed watermark.
    #[serde(default)]
    pub watermark_ms: Option<Timestamp>,
    /// Commits since the slice started on this worker.
    #[serde(default)]
    pub commits: u64,
    /// Average duration of a commit over the last interval, in milliseconds.
    #[serde(default)]
    pub commit_ms: f64,
    /// Epochs read but not yet committed.
    #[serde(default)]
    pub in_flight_epochs: u32,
    /// Times this worker lost a commit to another worker.
    #[serde(default)]
    pub fenced: u64,
    /// Times the slice restarted on this worker.
    #[serde(default)]
    pub restarts: u64,
    /// Most recent error, if any.
    #[serde(default)]
    pub last_error: Option<String>,
}

/// What one tenant consumes on one worker.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct TenantUsage {
    /// Actors of the tenant running on the worker.
    pub actors: u32,
    /// Records its source actors read over the last interval, per second.
    pub in_per_sec: f64,
    /// Sum of the busy ratios of its actors: an estimate of the cores it uses.
    pub busy: f64,
}

/// Everything a worker reports about itself, published periodically.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerSnapshot {
    /// The reporting worker.
    pub worker: WorkerId,
    /// When the snapshot was taken.
    pub at_ms: Timestamp,
    /// Length of the interval the rates and ratios cover, in milliseconds.
    pub interval_ms: u64,
    /// Resource usage of the worker.
    #[serde(default)]
    pub load: Load,
    /// One entry per actor.
    #[serde(default)]
    pub actors: Vec<ActorSnapshot>,
    /// One entry per slice.
    #[serde(default)]
    pub slices: Vec<SliceSnapshot>,
    /// Consumption per tenant.
    #[serde(default)]
    pub tenants: BTreeMap<TenantId, TenantUsage>,
}

/// One row written by a `std/kv-sink@1` node, readable through the API.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutputRow {
    /// Emit id of the element.
    pub id: String,
    /// Event time of the element.
    pub ts: Timestamp,
    /// Grouping key, in human-readable form.
    #[serde(default)]
    pub key: Option<String>,
    /// The element, as JSON.
    pub value: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_serializes_with_string_keys() {
        let mut placement = Placement::default();
        placement.assignments.insert(
            "acme/g/src.events.0".parse().unwrap(),
            WorkerId::new("worker-a").unwrap(),
        );
        let json = serde_json::to_string(&placement).unwrap();
        assert!(
            json.contains("\"acme/g/src.events.0\":\"worker-a\""),
            "{json}"
        );
        assert_eq!(serde_json::from_str::<Placement>(&json).unwrap(), placement);
    }

    #[test]
    fn documents_tolerate_missing_fields() {
        let desired: DesiredState = serde_json::from_str("{}").unwrap();
        assert_eq!(desired.state, RunState::Running);
        let state: RunState = serde_json::from_str("\"draining\"").unwrap();
        assert_eq!(state, RunState::Draining);
    }
}
