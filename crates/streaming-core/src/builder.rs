//! Building a [`GraphSpec`] in Rust.
//!
//! A graph is a set of nodes, each running an [`Operator`], and the edges
//! between them. An operator is a Rust value: its type names the registered
//! operator ([`Operator::OP`]) and its fields are the node's configuration.
//! The same type is what the operator's factory receives, so a configuration
//! written here is the one the worker runs.
//!
//! ```
//! use std::time::Duration;
//! use streaming_core::{GraphBuilder, GraphSettings, stdops::{Count, Identity, Tumbling}};
//!
//! let mut graph = GraphBuilder::new("acme", "count-per-key").settings(GraphSettings {
//!     max_pending_windows: 3,
//!     ..GraphSettings::default()
//! });
//! # let (events, by_key, sink) = (Identity, Identity, Identity);
//! let events = graph.node("events", events);
//! let by_key = graph.then(events, "by-key", by_key);
//! let window = graph.then(by_key, "window", Tumbling::of(Duration::from_secs(300)));
//! let count = graph.then(window, "count", Count);
//! graph.then(count, "out", sink);
//! let spec = graph.build().unwrap();
//! assert_eq!(spec.nodes.len(), 5);
//! assert_eq!(spec.edges.len(), 4);
//! ```
//!
//! [`GraphBuilder::build`] checks the identifiers and that no two nodes share
//! one. Whether the operators exist, agree on their types and are wired as
//! their kinds allow depends on the cluster: [`crate::Topology::build`] checks
//! it against a [`crate::Catalog`].

use crate::{
    ids::{GraphId, IdError, NodeId, OpRef, TenantId},
    spec::{GraphSettings, GraphSpec, NodeSpec},
};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;

/// A registered operator, as the configuration of a node.
///
/// Implemented by the configuration type of every operator: the type the
/// operator's factory deserializes the node configuration into. Operators
/// without configuration are unit structs.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use streaming_core::Operator;
///
/// /// Keeps the events of one action.
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// #[serde(rename_all = "camelCase")]
/// pub struct OnlyAction {
///     pub action: String,
/// }
///
/// impl Operator for OnlyAction {
///     const OP: &'static str = "acme/only-action@1";
/// }
/// ```
pub trait Operator: Serialize {
    /// The reference the operator is registered under (`std/tumbling@1`).
    const OP: &'static str;
}

/// A node added to a [`GraphBuilder`], to connect it to others.
///
/// A handle only means something to the builder that returned it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Node(usize);

/// Why [`GraphBuilder::build`] refused a graph.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    /// An identifier does not respect its alphabet.
    #[error(transparent)]
    Id(#[from] IdError),
    /// The configuration of a node cannot be represented in a graph
    /// description.
    #[error("the configuration of node `{node}` cannot be stored: {reason}")]
    Config {
        /// The node.
        node: NodeId,
        /// Why.
        reason: String,
    },
    /// Two nodes share an id.
    #[error("node `{0}` is declared twice")]
    DuplicateNode(NodeId),
    /// An edge uses a handle another builder returned.
    #[error("an edge uses a node of another graph")]
    ForeignNode,
}

struct Pending {
    id: String,
    op: &'static str,
    config: Result<Value, String>,
}

/// Builds a [`GraphSpec`] from typed operators.
///
/// Adding nodes and edges never fails: every problem is reported, the first
/// one found, by [`GraphBuilder::build`].
pub struct GraphBuilder {
    tenant: String,
    graph: String,
    settings: GraphSettings,
    nodes: Vec<Pending>,
    edges: Vec<(Node, Node)>,
}

impl GraphBuilder {
    /// An empty graph `graph` of `tenant`, with the default settings.
    #[must_use]
    pub fn new(tenant: impl Into<String>, graph: impl Into<String>) -> Self {
        Self {
            tenant: tenant.into(),
            graph: graph.into(),
            settings: GraphSettings::default(),
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Replace the engine settings.
    #[must_use]
    pub fn settings(mut self, settings: GraphSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Add a node `id` running `operator`, connected to nothing yet.
    pub fn node<O: Operator>(&mut self, id: impl Into<String>, operator: O) -> Node {
        self.nodes.push(Pending {
            id: id.into(),
            op: O::OP,
            config: serde_json::to_value(&operator).map_err(|error| error.to_string()),
        });
        Node(self.nodes.len() - 1)
    }

    /// Add a node `id` running `operator`, fed by `upstream`.
    pub fn then<O: Operator>(&mut self, upstream: Node, id: impl Into<String>, operator: O) -> Node {
        let node = self.node(id, operator);
        self.edge(upstream, node);
        node
    }

    /// Connect `from` to `to`.
    pub fn edge(&mut self, from: Node, to: Node) -> &mut Self {
        self.edges.push((from, to));
        self
    }

    /// The description of the graph, at version 0.
    pub fn build(self) -> Result<GraphSpec, BuildError> {
        let tenant = TenantId::new(self.tenant)?;
        let graph = GraphId::new(self.graph)?;
        let mut seen = BTreeSet::new();
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for pending in self.nodes {
            let id = NodeId::new(pending.id)?;
            if !seen.insert(id.clone()) {
                return Err(BuildError::DuplicateNode(id));
            }
            let op = OpRef::new(pending.op)?;
            let config = pending.config.map_err(|reason| BuildError::Config {
                node: id.clone(),
                reason,
            })?;
            nodes.push(NodeSpec { id, op, config });
        }
        let id = |node: Node| {
            nodes
                .get(node.0)
                .map(|spec| spec.id.clone())
                .ok_or(BuildError::ForeignNode)
        };
        let edges = self
            .edges
            .iter()
            .map(|&(from, to)| Ok((id(from)?, id(to)?)))
            .collect::<Result<_, BuildError>>()?;
        Ok(GraphSpec {
            tenant,
            graph,
            version: 0,
            settings: self.settings,
            nodes,
            edges,
            fingerprint: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stdops::{Count, Identity, Sliding, Tumbling};
    use std::time::Duration;

    fn id(s: &str) -> NodeId { NodeId::new(s).unwrap() }

    #[test]
    fn builds_the_description_a_worker_reads() {
        let mut graph = GraphBuilder::new("acme", "g");
        let a = graph.node("a", Identity);
        let w = graph.then(a, "w", Tumbling::of(Duration::from_secs(300)));
        let c = graph.then(w, "c", Count);
        let s = graph.node("s", Identity);
        graph.edge(c, s).edge(a, s);
        let spec = graph.build().unwrap();
        assert_eq!(spec.key().to_string(), "acme/g");
        assert_eq!(spec.settings, GraphSettings::default());
        assert_eq!(
            spec.edges,
            [
                (id("a"), id("w")),
                (id("w"), id("c")),
                (id("c"), id("s")),
                (id("a"), id("s"))
            ]
        );
        // Operators without configuration store none.
        assert_eq!(spec.nodes[0].config, Value::Null);
        assert_eq!(spec.nodes[2].op, OpRef::new("std/count@1").unwrap());
        // The stored form is the wire form, and parses back to the same graph.
        let wire = serde_json::to_value(&spec).unwrap();
        assert_eq!(wire["nodes"][1]["config"], serde_json::json!({"size": "5m"}));
        assert_eq!(serde_json::from_value::<GraphSpec>(wire).unwrap(), spec);
    }

    #[test]
    fn configurations_round_trip_through_their_operator_type() {
        let sliding = Sliding {
            size: Duration::from_secs(20),
            slide: Duration::from_secs(5),
            allowed_lateness: Duration::from_millis(1_500),
        };
        let mut graph = GraphBuilder::new("acme", "g");
        graph.node("w", sliding);
        let spec = graph.build().unwrap();
        assert_eq!(
            serde_json::from_value::<Sliding>(spec.nodes[0].config.clone()).unwrap(),
            sliding
        );
    }

    #[test]
    fn problems_surface_at_build() {
        let mut graph = GraphBuilder::new("acme", "g");
        graph.node("a", Identity);
        graph.node("a", Identity);
        assert_eq!(graph.build().unwrap_err(), BuildError::DuplicateNode(id("a")));

        let mut graph = GraphBuilder::new("acme", "g");
        graph.node("not an id", Identity);
        assert!(matches!(graph.build(), Err(BuildError::Id(error)) if error.kind == "node id"));

        assert!(matches!(GraphBuilder::new("", "g").build(), Err(BuildError::Id(_))));
        assert!(matches!(
            GraphBuilder::new("acme", "a/b").build(),
            Err(BuildError::Id(_))
        ));

        let mut other = GraphBuilder::new("acme", "other");
        other.node("x", Identity);
        let foreign = other.node("y", Identity);
        let mut graph = GraphBuilder::new("acme", "g");
        let a = graph.node("a", Identity);
        graph.edge(a, foreign);
        assert_eq!(graph.build().unwrap_err(), BuildError::ForeignNode);
    }
}
