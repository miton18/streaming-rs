//! The declarative description of a graph, as stored in the KV, and its
//! validation.
//!
//! A description never contains code. Each node references an operator by its
//! [`OpRef`]; the operator itself is Rust code compiled into the workers and
//! registered under the same reference.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    ids::{GraphId, GraphKey, NodeId, OpRef, SliceId, TenantId},
    registry::{Catalog, OpDescriptor, OpKind, TypeTag},
    time::duration_str,
};

/// One node of a graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeSpec {
    /// Unique id inside the graph. Names the node in metrics, control and state keys.
    pub id: NodeId,
    /// The operator to run.
    pub op: OpRef,
    /// Operator-specific configuration.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub config: Value,
}

fn default_out_of_orderness() -> Duration {
    Duration::from_millis(500)
}
fn default_idle_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_max_batch() -> usize {
    1_000
}
fn default_linger() -> Duration {
    Duration::from_millis(200)
}
fn default_mailbox_capacity() -> usize {
    8
}
fn default_chunk_size() -> usize {
    256
}
fn default_flush_interval() -> Duration {
    Duration::from_secs(1)
}
fn default_max_in_flight_epochs() -> usize {
    2
}
fn default_gc_grace() -> Duration {
    Duration::from_secs(30)
}
fn default_max_pending_windows() -> usize {
    16
}

/// Engine settings of a graph. Every field has a default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GraphSettings {
    /// Watermark = highest event time seen minus this bound.
    #[schemars(with = "crate::time::DurationText")]
    #[serde(with = "duration_str", default = "default_out_of_orderness")]
    pub max_out_of_orderness: Duration,
    /// After this long without any record on a fully-read partition, its
    /// watermark follows the wall clock so that downstream windows can close.
    #[schemars(with = "crate::time::DurationText")]
    #[serde(with = "duration_str", default = "default_idle_timeout")]
    pub idle_timeout: Duration,
    /// Maximum number of source records per epoch (one KV commit per epoch).
    #[serde(default = "default_max_batch")]
    pub max_batch: usize,
    /// Maximum time a source waits to fill a batch.
    #[schemars(with = "crate::time::DurationText")]
    #[serde(with = "duration_str", default = "default_linger")]
    pub linger: Duration,
    /// Capacity of every edge, in chunks. A full edge blocks its producer:
    /// this is what propagates backpressure up to the source.
    #[serde(default = "default_mailbox_capacity")]
    pub mailbox_capacity: usize,
    /// Number of records per chunk on an edge.
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
    /// How often closed windows are looked for.
    #[schemars(with = "crate::time::DurationText")]
    #[serde(with = "duration_str", default = "default_flush_interval")]
    pub flush_interval: Duration,
    /// How many epochs a slice may have read but not yet committed.
    #[serde(default = "default_max_in_flight_epochs")]
    pub max_in_flight_epochs: usize,
    /// How long the index entry of an emitted window outlives its state. A
    /// store command issued by a worker that has since lost its slice may
    /// still land: the entry is what lets such a write be found and deleted.
    /// Must exceed three times the command timeout of the store, plus the
    /// clock difference between workers.
    #[schemars(with = "crate::time::DurationText")]
    #[serde(with = "duration_str", default = "default_gc_grace")]
    pub gc_grace: Duration,
    /// How many windows a slice may have closed, per aggregate, without their
    /// results being emitted yet. Past that number the slice stops reading
    /// until emission catches up: this is how a slow sink behind an aggregate
    /// slows the sources down, and what bounds the state waiting in the store.
    /// `0` removes the limit.
    #[serde(default = "default_max_pending_windows")]
    pub max_pending_windows: usize,
}

impl Default for GraphSettings {
    fn default() -> Self {
        Self {
            max_out_of_orderness: default_out_of_orderness(),
            idle_timeout: default_idle_timeout(),
            max_batch: default_max_batch(),
            linger: default_linger(),
            mailbox_capacity: default_mailbox_capacity(),
            chunk_size: default_chunk_size(),
            flush_interval: default_flush_interval(),
            max_in_flight_epochs: default_max_in_flight_epochs(),
            gc_grace: default_gc_grace(),
            max_pending_windows: default_max_pending_windows(),
        }
    }
}

/// The description of a graph: nodes referencing operators, and edges.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GraphSpec {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Graph id, unique inside the tenant.
    pub graph: GraphId,
    /// Incremented by the control plane on every accepted change.
    #[serde(default)]
    pub version: u64,
    /// Engine settings.
    #[serde(default)]
    pub settings: GraphSettings,
    /// The nodes.
    pub nodes: Vec<NodeSpec>,
    /// Directed edges `[from, to]`.
    pub edges: Vec<(NodeId, NodeId)>,
}

impl GraphSpec {
    /// The `tenant/graph` key of this graph.
    #[must_use]
    pub fn key(&self) -> GraphKey {
        GraphKey::new(self.tenant.clone(), self.graph.clone())
    }

    /// Look a node up.
    #[must_use]
    pub fn node(&self, id: &NodeId) -> Option<&NodeSpec> {
        self.nodes.iter().find(|node| &node.id == id)
    }
}

/// Why a description was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpecError {
    /// The graph has no node.
    #[error("the graph has no node")]
    Empty,
    /// Two nodes share an id.
    #[error("node `{0}` is declared twice")]
    DuplicateNode(NodeId),
    /// An edge references a node that does not exist.
    #[error("edge `{from}` -> `{to}` references unknown node `{unknown}`")]
    UnknownNode {
        /// Edge origin.
        from: NodeId,
        /// Edge destination.
        to: NodeId,
        /// The missing node.
        unknown: NodeId,
    },
    /// An edge appears twice, or loops on one node.
    #[error("edge `{from}` -> `{to}` is duplicated or a self-loop")]
    BadEdge {
        /// Edge origin.
        from: NodeId,
        /// Edge destination.
        to: NodeId,
    },
    /// The graph is not acyclic.
    #[error("the graph has a cycle through {0:?}")]
    Cycle(Vec<NodeId>),
    /// No worker of the cluster knows this operator.
    #[error("node `{node}` references unknown operator `{op}`")]
    UnknownOp {
        /// The node.
        node: NodeId,
        /// The operator reference.
        op: OpRef,
    },
    /// A node is wired in a way its kind does not allow.
    #[error("node `{node}` ({kind}): {reason}")]
    Wiring {
        /// The node.
        node: NodeId,
        /// Its kind.
        kind: OpKind,
        /// What is wrong.
        reason: String,
    },
    /// Two connected nodes disagree on the type flowing between them.
    #[error("edge `{from}` -> `{to}`: `{from}` produces `{produced}` but `{to}` expects `{expected}`")]
    Type {
        /// Edge origin.
        from: NodeId,
        /// Edge destination.
        to: NodeId,
        /// Type produced upstream.
        produced: TypeTag,
        /// Type expected downstream.
        expected: TypeTag,
    },
}

/// A validated graph, with everything derived from its shape.
#[derive(Debug, Clone)]
pub struct Topology {
    /// The description this topology was built from.
    pub spec: GraphSpec,
    /// Descriptor of the operator of every node.
    pub descriptors: BTreeMap<NodeId, OpDescriptor>,
    /// Nodes in topological order.
    pub order: Vec<NodeId>,
    /// Direct predecessors of every node.
    pub inputs: BTreeMap<NodeId, Vec<NodeId>>,
    /// Direct successors of every node.
    pub outputs: BTreeMap<NodeId, Vec<NodeId>>,
    /// Type effectively produced by every node.
    pub types: BTreeMap<NodeId, TypeTag>,
}

impl Topology {
    /// Validate `spec` against the operators the cluster can run.
    pub fn build(spec: &GraphSpec, catalog: &Catalog) -> Result<Self, SpecError> {
        if spec.nodes.is_empty() {
            return Err(SpecError::Empty);
        }
        let mut descriptors = BTreeMap::new();
        for node in &spec.nodes {
            let descriptor = catalog.get(&node.op).ok_or_else(|| SpecError::UnknownOp {
                node: node.id.clone(),
                op: node.op.clone(),
            })?;
            if descriptors.insert(node.id.clone(), descriptor.clone()).is_some() {
                return Err(SpecError::DuplicateNode(node.id.clone()));
            }
        }

        let mut inputs: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();
        let mut outputs: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();
        for node in &spec.nodes {
            inputs.insert(node.id.clone(), Vec::new());
            outputs.insert(node.id.clone(), Vec::new());
        }
        let mut seen = BTreeSet::new();
        for (from, to) in &spec.edges {
            for end in [from, to] {
                if !descriptors.contains_key(end) {
                    return Err(SpecError::UnknownNode {
                        from: from.clone(),
                        to: to.clone(),
                        unknown: end.clone(),
                    });
                }
            }
            if from == to || !seen.insert((from.clone(), to.clone())) {
                return Err(SpecError::BadEdge {
                    from: from.clone(),
                    to: to.clone(),
                });
            }
            outputs.entry(from.clone()).or_default().push(to.clone());
            inputs.entry(to.clone()).or_default().push(from.clone());
        }

        // Kahn's algorithm, keeping the declaration order among ready nodes.
        let mut pending: BTreeMap<&NodeId, usize> = inputs.iter().map(|(id, ins)| (id, ins.len())).collect();
        let mut order = Vec::with_capacity(spec.nodes.len());
        loop {
            let ready: Vec<NodeId> = spec
                .nodes
                .iter()
                .filter(|node| pending.get(&node.id) == Some(&0))
                .map(|node| node.id.clone())
                .collect();
            if ready.is_empty() {
                break;
            }
            for id in ready {
                pending.remove(&id);
                for next in &outputs[&id] {
                    if let Some(count) = pending.get_mut(next) {
                        *count -= 1;
                    }
                }
                order.push(id);
            }
        }
        if !pending.is_empty() {
            return Err(SpecError::Cycle(pending.keys().map(|id| (*id).clone()).collect()));
        }

        let kind = |id: &NodeId| descriptors[id].kind;
        for node in &spec.nodes {
            let id = &node.id;
            let (ins, outs) = (&inputs[id], &outputs[id]);
            let wiring = |reason: &str| SpecError::Wiring {
                node: id.clone(),
                kind: kind(id),
                reason: reason.to_owned(),
            };
            match kind(id) {
                OpKind::Source => {
                    if !ins.is_empty() {
                        return Err(wiring("a source cannot have inputs"));
                    }
                    if outs.is_empty() {
                        return Err(wiring("a source needs at least one output"));
                    }
                }
                OpKind::Sink => {
                    if ins.is_empty() {
                        return Err(wiring("a sink needs at least one input"));
                    }
                    if !outs.is_empty() {
                        return Err(wiring("a sink cannot have outputs"));
                    }
                }
                OpKind::Map => {
                    if ins.is_empty() || outs.is_empty() {
                        return Err(wiring("needs at least one input and one output"));
                    }
                }
                OpKind::KeyBy => {
                    if ins.is_empty() || outs.is_empty() {
                        return Err(wiring("needs at least one input and one output"));
                    }
                    if outs.iter().any(|out| kind(out) != OpKind::Window) {
                        return Err(wiring("a key_by node must feed window nodes only"));
                    }
                }
                OpKind::Window => {
                    if ins.is_empty() || ins.iter().any(|input| kind(input) != OpKind::KeyBy) {
                        return Err(wiring("a window node must be fed by key_by nodes only"));
                    }
                    if outs.is_empty() || outs.iter().any(|out| kind(out) != OpKind::Aggregate) {
                        return Err(wiring("a window node must feed aggregate nodes only"));
                    }
                }
                OpKind::Aggregate => {
                    if ins.len() != 1 || kind(&ins[0]) != OpKind::Window {
                        return Err(wiring("an aggregate node needs exactly one input, a window node"));
                    }
                    if outs.is_empty() {
                        return Err(wiring("an aggregate node needs at least one output"));
                    }
                }
            }
        }

        let mut types: BTreeMap<NodeId, TypeTag> = BTreeMap::new();
        for id in &order {
            let descriptor = &descriptors[id];
            for input in &inputs[id] {
                let produced = &types[input];
                if !produced.flows_into(&descriptor.input) {
                    return Err(SpecError::Type {
                        from: input.clone(),
                        to: id.clone(),
                        produced: produced.clone(),
                        expected: descriptor.input.clone(),
                    });
                }
            }
            let pass_through = matches!(descriptor.kind, OpKind::KeyBy | OpKind::Window)
                || (descriptor.kind == OpKind::Map && descriptor.output == TypeTag::Any);
            let produced = if pass_through {
                let mut upstream = inputs[id].iter().map(|input| types[input].clone());
                let first = upstream.next().unwrap_or(TypeTag::Any);
                if upstream.all(|other| other == first) {
                    first
                } else {
                    TypeTag::Any
                }
            } else {
                descriptor.output.clone()
            };
            types.insert(id.clone(), produced);
        }

        Ok(Self {
            spec: spec.clone(),
            descriptors,
            order,
            inputs,
            outputs,
            types,
        })
    }

    /// Kind of a node.
    ///
    /// # Panics
    ///
    /// Panics if `id` is not a node of this graph.
    #[must_use]
    pub fn kind(&self, id: &NodeId) -> OpKind {
        self.descriptors[id].kind
    }

    /// Nodes of a given kind, in topological order.
    #[must_use]
    pub fn nodes_of(&self, kind: OpKind) -> Vec<NodeId> {
        self.order.iter().filter(|id| self.kind(id) == kind).cloned().collect()
    }

    /// Every slice of the graph, given the partition count of each source.
    #[must_use]
    pub fn slices(&self, partitions: &BTreeMap<NodeId, u32>) -> Vec<SliceId> {
        let mut slices = Vec::new();
        for id in &self.order {
            match self.kind(id) {
                OpKind::Source => {
                    for partition in 0..partitions.get(id).copied().unwrap_or(0) {
                        slices.push(SliceId::Source {
                            node: id.clone(),
                            partition,
                        });
                    }
                }
                OpKind::Aggregate => slices.push(SliceId::Emitter { node: id.clone() }),
                _ => {}
            }
        }
        slices
    }

    /// Nodes run by a slice, in topological order, root first.
    ///
    /// A slice starts at its root and stops at the next aggregate nodes, which
    /// it includes (as their accumulating half).
    #[must_use]
    pub fn slice_nodes(&self, slice: &SliceId) -> Vec<NodeId> {
        let root = slice.root();
        let mut members = BTreeSet::from([root.clone()]);
        let mut stack = vec![root.clone()];
        while let Some(id) = stack.pop() {
            for next in self.outputs.get(&id).into_iter().flatten() {
                if members.insert(next.clone()) && self.kind(next) != OpKind::Aggregate {
                    stack.push(next.clone());
                }
            }
        }
        self.order.iter().filter(|id| members.contains(*id)).cloned().collect()
    }

    /// Edges internal to a slice.
    #[must_use]
    pub fn slice_edges(&self, slice: &SliceId) -> Vec<(NodeId, NodeId)> {
        let members: BTreeSet<NodeId> = self.slice_nodes(slice).into_iter().collect();
        let root = slice.root();
        self.spec
            .edges
            .iter()
            .filter(|(from, to)| {
                members.contains(from) && members.contains(to) && (from == root || self.kind(from) != OpKind::Aggregate)
            })
            .cloned()
            .collect()
    }

    /// Among `slices`, those that accumulate into `aggregate`.
    #[must_use]
    pub fn feeding_slices(&self, aggregate: &NodeId, slices: &[SliceId]) -> Vec<SliceId> {
        slices
            .iter()
            .filter(|slice| slice.root() != aggregate && self.slice_nodes(slice).contains(aggregate))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use moonpool_core::TokioProviders;

    use super::*;

    #[test]
    fn rejects_unknown_keys_such_as_snake_case_ones() {
        let spec = serde_json::json!({
            "tenant": "acme", "graph": "g", "nodes": [], "edges": [],
            "settings": {"gc_grace": "1m"}
        });
        let error = serde_json::from_value::<GraphSpec>(spec).unwrap_err().to_string();
        assert!(error.contains("unknown field `gc_grace`"), "{error}");
        assert!(error.contains("gcGrace"), "{error}");
        let node = serde_json::json!({"id": "a", "op": "std/count@1", "cfg": {}});
        assert!(serde_json::from_value::<NodeSpec>(node).is_err());
        let spec = serde_json::json!({"tenant": "acme", "graph": "g", "nodes": [], "edges": [], "extra": 1});
        assert!(serde_json::from_value::<GraphSpec>(spec).is_err());
    }
    use crate::registry::Registry;

    struct Event;

    fn catalog() -> Catalog {
        let mut registry = Registry::<TokioProviders>::standard();
        registry.register_source::<Vec<u8>>("test/source@1", "", |_, _| {
            Err(crate::ops::OpError::Config("not runnable".into()))
        });
        registry.register_sink("test/sink@1", "", TypeTag::Any, |_, _| {
            Err(crate::ops::OpError::Config("not runnable".into()))
        });
        registry.register_map("test/decode@1", "", |_: &Vec<u8>| Some(Event));
        registry.register_key("test/key@1", "", |_: &Event| "k".to_owned());
        registry.register_map("test/fmt@1", "", |_: &crate::data::Windowed<u64>| Some(String::new()));
        registry.catalog()
    }

    fn spec(nodes: &[(&str, &str)], edges: &[(&str, &str)]) -> GraphSpec {
        serde_json::from_value(serde_json::json!({
            "tenant": "acme",
            "graph": "g",
            "nodes": nodes.iter().map(|(id, op)| serde_json::json!({"id": id, "op": op})).collect::<Vec<_>>(),
            "edges": edges,
        }))
        .unwrap()
    }

    fn count_per_key() -> GraphSpec {
        let mut spec = spec(
            &[
                ("events", "test/source@1"),
                ("decode", "test/decode@1"),
                ("by-key", "test/key@1"),
                ("win", "std/tumbling@1"),
                ("count", "std/count@1"),
                ("fmt", "test/fmt@1"),
                ("out", "test/sink@1"),
                ("raw", "test/sink@1"),
            ],
            &[
                ("events", "decode"),
                ("decode", "by-key"),
                ("by-key", "win"),
                ("win", "count"),
                ("count", "fmt"),
                ("fmt", "out"),
                ("decode", "raw"),
            ],
        );
        spec.nodes[3].config = serde_json::json!({"size": "5m"});
        spec
    }

    fn id(s: &str) -> NodeId {
        NodeId::new(s).unwrap()
    }

    #[test]
    fn valid_graph_is_cut_into_slices() {
        let topology = Topology::build(&count_per_key(), &catalog()).unwrap();
        let partitions = BTreeMap::from([(id("events"), 2)]);
        let slices = topology.slices(&partitions);
        assert_eq!(
            slices.iter().map(ToString::to_string).collect::<Vec<_>>(),
            ["src.events.0", "src.events.1", "agg.count"]
        );
        assert_eq!(
            topology.slice_nodes(&slices[0]),
            ["events", "decode", "by-key", "raw", "win", "count"].map(id)
        );
        assert_eq!(topology.slice_nodes(&slices[2]), ["count", "fmt", "out"].map(id));
        assert_eq!(
            topology.slice_edges(&slices[2]),
            [(id("count"), id("fmt")), (id("fmt"), id("out"))]
        );
        assert!(!topology.slice_edges(&slices[0]).contains(&(id("count"), id("fmt"))));
        assert_eq!(topology.feeding_slices(&id("count"), &slices), &slices[..2]);
        assert_eq!(topology.types[&id("win")], TypeTag::of::<Event>());
    }

    #[test]
    fn type_mismatch_is_rejected() {
        let mut spec = count_per_key();
        // Feed raw bytes straight into the key extractor, which expects `Event`.
        spec.edges[1] = (id("events"), id("by-key"));
        spec.edges.push((id("decode"), id("raw")));
        spec.edges.dedup();
        let err = Topology::build(&spec, &catalog()).unwrap_err();
        assert!(matches!(err, SpecError::Type { .. }), "{err}");
    }

    #[test]
    fn structural_errors() {
        let catalog = catalog();
        let cyclic = spec(
            &[
                ("s", "test/source@1"),
                ("a", "std/identity@1"),
                ("b", "std/identity@1"),
                ("k", "test/sink@1"),
            ],
            &[("s", "a"), ("a", "b"), ("b", "a"), ("b", "k")],
        );
        assert!(matches!(Topology::build(&cyclic, &catalog), Err(SpecError::Cycle(_))));

        let unknown_op = spec(&[("s", "nope/x@1")], &[]);
        assert!(matches!(
            Topology::build(&unknown_op, &catalog),
            Err(SpecError::UnknownOp { .. })
        ));

        let dangling = spec(&[("s", "test/source@1")], &[]);
        assert!(matches!(
            Topology::build(&dangling, &catalog),
            Err(SpecError::Wiring { .. })
        ));

        let window_without_key = spec(
            &[
                ("s", "test/source@1"),
                ("w", "std/tumbling@1"),
                ("c", "std/count@1"),
                ("k", "test/sink@1"),
            ],
            &[("s", "w"), ("w", "c"), ("c", "k")],
        );
        assert!(matches!(
            Topology::build(&window_without_key, &catalog),
            Err(SpecError::Wiring { .. })
        ));
    }

    fn wiring_error_on(spec: &GraphSpec, catalog: &Catalog) -> NodeId {
        match Topology::build(spec, catalog) {
            Err(SpecError::Wiring { node, .. }) => node,
            other => panic!("expected a wiring error, got {other:?}"),
        }
    }

    #[test]
    fn self_loops_are_bad_edges() {
        let looped = spec(
            &[("s", "test/source@1"), ("a", "std/identity@1"), ("k", "test/sink@1")],
            &[("s", "a"), ("a", "a"), ("a", "k")],
        );
        assert!(matches!(
            Topology::build(&looped, &catalog()),
            Err(SpecError::BadEdge { .. })
        ));
    }

    #[test]
    fn every_inner_node_needs_an_input_and_an_output() {
        let catalog = catalog();
        // A map, a key_by and a window that each lack an output.
        let map_without_output = spec(
            &[("s", "test/source@1"), ("a", "std/identity@1"), ("k", "test/sink@1")],
            &[("s", "a"), ("s", "k")],
        );
        assert_eq!(wiring_error_on(&map_without_output, &catalog), id("a"));

        let key_by_without_output = spec(
            &[
                ("s", "test/source@1"),
                ("d", "test/decode@1"),
                ("by", "test/key@1"),
                ("k", "test/sink@1"),
            ],
            &[("s", "d"), ("d", "by"), ("d", "k")],
        );
        assert_eq!(wiring_error_on(&key_by_without_output, &catalog), id("by"));

        let mut window_without_output = spec(
            &[
                ("s", "test/source@1"),
                ("d", "test/decode@1"),
                ("by", "test/key@1"),
                ("w", "std/tumbling@1"),
                ("k", "test/sink@1"),
            ],
            &[("s", "d"), ("d", "by"), ("by", "w"), ("d", "k")],
        );
        window_without_output.nodes[3].config = serde_json::json!({"size": "5m"});
        assert_eq!(wiring_error_on(&window_without_output, &catalog), id("w"));

        // A window with no input at all.
        let mut window_without_input = spec(
            &[("w", "std/tumbling@1"), ("c", "std/count@1"), ("k", "test/sink@1")],
            &[("w", "c"), ("c", "k")],
        );
        window_without_input.nodes[0].config = serde_json::json!({"size": "5m"});
        assert_eq!(wiring_error_on(&window_without_input, &catalog), id("w"));
    }

    #[test]
    fn pass_through_nodes_carry_the_type_of_their_inputs() {
        let catalog = catalog();
        // An untyped map after `decode` carries `Event`; one fed by two
        // different types carries `Any`.
        let typed = spec(
            &[
                ("s", "test/source@1"),
                ("d", "test/decode@1"),
                ("same", "std/identity@1"),
                ("mixed", "std/identity@1"),
                ("k", "test/sink@1"),
                ("k2", "test/sink@1"),
            ],
            &[
                ("s", "d"),
                ("d", "same"),
                ("same", "k"),
                ("d", "mixed"),
                ("s", "mixed"),
                ("mixed", "k2"),
            ],
        );
        let topology = Topology::build(&typed, &catalog).unwrap();
        assert_eq!(topology.types[&id("same")], TypeTag::of::<Event>());
        assert_eq!(topology.types[&id("mixed")], TypeTag::Any);
        // A typed map produces its declared output, not its input.
        assert_eq!(topology.types[&id("d")], TypeTag::of::<Event>());
        assert_eq!(topology.nodes_of(OpKind::Sink), [id("k"), id("k2")]);
        assert_eq!(topology.nodes_of(OpKind::Source), [id("s")]);
    }

    #[test]
    fn nodes_are_looked_up_by_id() {
        let spec = count_per_key();
        assert_eq!(spec.node(&id("win")).unwrap().id, id("win"));
        assert!(spec.node(&id("nope")).is_none());
    }
}
