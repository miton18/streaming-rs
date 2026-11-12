//! Layout of the keys stored in the KV. Every tenant-owned key starts with
//! `t:<tenant>:`.
//!
//! ```text
//! cluster:epoch                                  INCR on every control-plane change
//! cluster:members                                hash  worker        -> MemberInfo (JSON)
//! cluster:catalog                                hash  op ref        -> OpDescriptor (JSON)
//! cluster:tenants                                hash  tenant        -> TenantRecord (JSON)
//! cluster:graphs                                 hash  tenant/graph  -> GraphIndexEntry (JSON)
//! cluster:deleting                               hash  tenant/graph  -> "1": the graph is being deleted, whatever its entry says
//! cluster:placement                              string Placement (JSON)
//! cluster:metrics:<worker>                       string WorkerSnapshot (JSON)
//! t:<tenant>:g:<graph>:spec                      string GraphSpec (JSON): what workers run
//! t:<tenant>:g:<graph>:version                   INCR for every description stored: its version
//! t:<tenant>:g:<graph>:code                      string GraphSpec (JSON): as the code built it
//! t:<tenant>:g:<graph>:overrides                 hash  node         -> config fields set through the API (JSON)
//! t:<tenant>:g:<graph>:declarations              hash  <n>          -> Declaration (JSON), written once: the highest is in force
//! t:<tenant>:g:<graph>:desired                   string DesiredState (JSON)
//! t:<tenant>:g:<graph>:plan                      string GraphPlan (JSON)
//! t:<tenant>:g:<graph>:out:<node>                hash  emit id       -> OutputRow (JSON)
//! t:<tenant>:g:<graph>:s:<slice>:log             hash  c:<epoch>     -> commit record (empty once superseded)
//! t:<tenant>:g:<graph>:s:<slice>:seal            hash  f:<epoch>     -> "1": epochs up to it are final
//! t:<tenant>:g:<graph>:s:<slice>:idx:<node>      hash  <window>      -> "1"
//! t:<tenant>:g:<graph>:s:<slice>:w:<node>:<win>  hash  <hexkey>@<epoch> -> accumulator
//! ```

use crate::{
    data::Key,
    ids::{GraphKey, NodeId, SliceKey, TenantId, WorkerId},
    time::Window,
};

/// Counter bumped on every control-plane change.
pub const CLUSTER_EPOCH: &str = "cluster:epoch";
/// Hash of live workers.
pub const CLUSTER_MEMBERS: &str = "cluster:members";
/// Hash of the operators the cluster can run.
pub const CLUSTER_CATALOG: &str = "cluster:catalog";
/// Hash of created tenants. A graph can only be submitted to a tenant listed
/// here.
pub const CLUSTER_TENANTS: &str = "cluster:tenants";
/// Hash of deployed graphs.
pub const CLUSTER_GRAPHS: &str = "cluster:graphs";
/// Hash of the graphs being deleted. An index entry is rewritten whole by
/// whoever changes a part of it, and such a write may be issued by someone
/// who read the entry before the deletion: the mark of a deletion is kept
/// here, where only deleting writes and only the end of the purge removes.
pub const CLUSTER_DELETING: &str = "cluster:deleting";
/// Which worker runs which slice.
pub const CLUSTER_PLACEMENT: &str = "cluster:placement";

/// Metrics snapshot of a worker.
#[must_use]
pub fn worker_metrics(worker: &WorkerId) -> String { format!("cluster:metrics:{worker}") }

/// Prefix of every key owned by a tenant.
#[must_use]
pub fn tenant_prefix(tenant: &TenantId) -> String { format!("t:{tenant}:") }

/// Prefix of every key owned by a graph.
#[must_use]
pub fn graph_prefix(graph: &GraphKey) -> String { format!("t:{}:g:{}", graph.tenant, graph.graph) }

/// Description of a graph.
#[must_use]
pub fn graph_spec(graph: &GraphKey) -> String { format!("{}:spec", graph_prefix(graph)) }

/// Counter the versions of a graph's descriptions are drawn from: two
/// writers never store two descriptions under one version.
#[must_use]
pub fn graph_version(graph: &GraphKey) -> String { format!("{}:version", graph_prefix(graph)) }

/// Description of a graph as the code of the workers built it, before the
/// configuration changes made through the API are applied.
#[must_use]
pub fn graph_code(graph: &GraphKey) -> String { format!("{}:code", graph_prefix(graph)) }

/// Configuration fields of a graph's nodes set through the API, laid over
/// [`graph_code`] to make [`graph_spec`].
#[must_use]
pub fn graph_overrides(graph: &GraphKey) -> String { format!("{}:overrides", graph_prefix(graph)) }

/// The successive declarations of a graph by the code of the workers.
#[must_use]
pub fn graph_declarations(graph: &GraphKey) -> String { format!("{}:declarations", graph_prefix(graph)) }

/// Desired run state of a graph.
#[must_use]
pub fn graph_desired(graph: &GraphKey) -> String { format!("{}:desired", graph_prefix(graph)) }

/// Execution plan of a graph.
#[must_use]
pub fn graph_plan(graph: &GraphKey) -> String { format!("{}:plan", graph_prefix(graph)) }

/// Rows written by a `std/kv-sink@1` node.
#[must_use]
pub fn graph_output(graph: &GraphKey, node: &NodeId) -> String { format!("{}:out:{node}", graph_prefix(graph)) }

fn slice_prefix(slice: &SliceKey) -> String { format!("t:{}:g:{}:s:{}", slice.tenant, slice.graph, slice.slice) }

/// Commit log of a slice.
#[must_use]
pub fn slice_log(slice: &SliceKey) -> String { format!("{}:log", slice_prefix(slice)) }

/// Seal of a slice log: the epochs at or below its highest field are final.
#[must_use]
pub fn slice_seal(slice: &SliceKey) -> String { format!("{}:seal", slice_prefix(slice)) }

/// Field of a slice seal declaring every epoch up to `epoch` final.
#[must_use]
pub fn seal_field(epoch: u64) -> String { format!("f:{epoch}") }

/// Inverse of [`seal_field`].
#[must_use]
pub fn parse_seal_field(field: &str) -> Option<u64> { field.strip_prefix("f:")?.parse().ok() }

/// Index of the windows a slice holds state for, for one aggregate node.
#[must_use]
pub fn slice_window_index(slice: &SliceKey, node: &NodeId) -> String { format!("{}:idx:{node}", slice_prefix(slice)) }

/// Accumulators of one window of one aggregate node, for one slice.
#[must_use]
pub fn slice_window(slice: &SliceKey, node: &NodeId, window: &Window) -> String {
    format!("{}:w:{node}:{window}", slice_prefix(slice))
}

/// Field of a slice log holding the commit record of `epoch`.
#[must_use]
pub fn commit_field(epoch: u64) -> String { format!("c:{epoch}") }

/// Inverse of [`commit_field`].
#[must_use]
pub fn parse_commit_field(field: &str) -> Option<u64> { field.strip_prefix("c:")?.parse().ok() }

/// Field of a window hash holding the accumulator of `key` as of `epoch`.
#[must_use]
pub fn state_field(key: &Key, epoch: u64) -> String { format!("{}@{epoch}", key.to_hex()) }

/// Inverse of [`state_field`].
#[must_use]
pub fn parse_state_field(field: &str) -> Option<(Key, u64)> {
    let (hex, epoch) = field.rsplit_once('@')?;
    Some((Key::from_hex(hex).ok()?, epoch.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_tenant_prefixed() {
        let slice: SliceKey = "acme/count-per-key/src.events.0".parse().unwrap();
        let graph = slice.graph_key();
        let node = NodeId::new("count").unwrap();
        let window = Window::new(0, 300_000);
        for key in [
            graph_spec(&graph),
            graph_desired(&graph),
            graph_plan(&graph),
            graph_output(&graph, &node),
            slice_log(&slice),
            slice_seal(&slice),
            slice_window_index(&slice, &node),
            slice_window(&slice, &node, &window),
        ] {
            assert!(key.starts_with(&tenant_prefix(&slice.tenant)), "{key}");
        }
        assert_eq!(
            slice_window(&slice, &node, &window),
            "t:acme:g:count-per-key:s:src.events.0:w:count:0-300000"
        );
    }

    #[test]
    fn cluster_and_tenant_keys() {
        let worker = crate::ids::WorkerId::new("worker-1").unwrap();
        assert_eq!(worker_metrics(&worker), "cluster:metrics:worker-1");
        let tenant = crate::ids::TenantId::new("acme").unwrap();
        assert_eq!(tenant_prefix(&tenant), "t:acme:");
    }

    #[test]
    fn fields_round_trip() {
        assert_eq!(parse_commit_field(&commit_field(42)), Some(42));
        assert_eq!(parse_commit_field("m"), None);
        assert_eq!(parse_seal_field(&seal_field(9)), Some(9));
        assert_eq!(parse_seal_field("c:9"), None);
        let key = Key::encode(&"user".to_owned()).unwrap();
        assert_eq!(parse_state_field(&state_field(&key, 7)), Some((key, 7)));
    }
}
