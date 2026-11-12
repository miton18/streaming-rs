//! Building a [`GraphSpec`] in Rust.
//!
//! A graph is a set of nodes, each running an [`Operator`], and the edges
//! between them. An operator is a Rust value: its type names the operator
//! ([`Operator::OP`]) and its fields are the node's configuration. What the
//! node does is what the type implements: [`Source`], [`Transformer`] or
//! [`Sink`].
//!
//! Nodes are typed by what flows out of them, so that a graph wired the wrong
//! way does not compile:
//!
//! ```
//! use std::time::Duration;
//! use streaming_core::moonpool_core::Providers;
//! use streaming_core::stdops::{Count, Tumbling};
//! use streaming_core::{
//!     Context, GraphBuilder, GraphSettings, Keyed, OpError, Operator, Pipe, Position, Sink, SinkPipe, Source,
//!     SourcePipe, Transformer, Windowed,
//! };
//!
//! /// The lines of a file.
//! #[derive(Debug, Clone, Operator)]
//! #[operator("acme/lines@1")]
//! struct Lines {
//!     /// The file to read.
//!     #[param]
//!     path: String,
//! }
//!
//! impl Source for Lines {
//!     type Out = String;
//!
//!     async fn partitions<P: Providers>(&self, _: &Context<P>) -> Result<u32, OpError> { Ok(1) }
//!
//!     async fn start<P: Providers>(
//!         &self,
//!         ctx: &Context<P>,
//!         _partition: u32,
//!         after: Option<Position>,
//!         out: SourcePipe<String>,
//!     ) -> Result<(), OpError> {
//!         let text = std::fs::read_to_string(&self.path).map_err(|error| OpError::Failed(error.to_string()))?;
//!         let read = after.map_or(0, |position| position.1 as usize);
//!         for (number, line) in text.lines().enumerate().skip(read) {
//!             out.send(Position(0, number as u64 + 1, 0), ctx.clock.now_ms(), line.to_owned()).await?;
//!         }
//!         out.closed().await;
//!         Ok(())
//!     }
//! }
//!
//! /// Groups texts by their first letter.
//! #[derive(Debug, Clone, Operator)]
//! #[operator("acme/by-initial@1")]
//! struct ByInitial;
//!
//! impl Transformer<String> for ByInitial {
//!     type Out = Keyed<String>;
//!
//!     fn transform(&self, input: Pipe<String>) -> Pipe<Keyed<String>> { input.key_by(|text| text.chars().next()) }
//! }
//!
//! /// Prints the counts.
//! #[derive(Debug, Clone, Operator)]
//! #[operator("acme/print@1")]
//! struct Print;
//!
//! impl Sink<Windowed<u64>> for Print {
//!     async fn start<P: Providers>(&self, _: &Context<P>, mut input: SinkPipe<Windowed<u64>>) -> Result<(), OpError> {
//!         while let Some(batch) = input.next().await {
//!             for count in batch.iter() {
//!                 println!("{}: {}", count.value.key.display_lossy(), count.value.value);
//!             }
//!             batch.done();
//!         }
//!         Ok(())
//!     }
//! }
//!
//! let mut graph = GraphBuilder::new("acme", "count-per-initial").settings(GraphSettings {
//!     max_pending_windows: 3,
//!     ..GraphSettings::default()
//! });
//! let lines = graph.source("lines", Lines { path: "/var/log/app.log".into() });
//! let by_initial = graph.then(lines, "by-initial", ByInitial);
//! let windows = graph.then(by_initial, "window", Tumbling::of(Duration::from_secs(300)));
//! let counts = graph.then(windows, "count", Count);
//! graph.sink(counts, "out", Print);
//! let spec = graph.build().unwrap();
//! assert_eq!(spec.nodes.len(), 5);
//! assert_eq!(spec.edges.len(), 4);
//! assert_eq!(spec.nodes[0].config, serde_json::json!({"path": "/var/log/app.log"}));
//! ```
//!
//! Naming an operator in a graph registers it: an application has no list of
//! operators to keep next to its graphs ([`GraphBuilder::into_parts`]).
//!
//! [`GraphBuilder::build`] checks the identifiers and that no two nodes share
//! one. What the types cannot say (an aggregate without a sink, a window fed
//! by two nodes) is checked by [`crate::Topology::build`].

use crate::{
    ids::{GraphId, IdError, NodeId, OpRef, TenantId},
    ops::OpError,
    param::Secret,
    pipe::{Sink, Source, Transformer},
    registry::{ParamSpec, Registry},
    spec::{GraphSettings, GraphSpec, NodeSpec},
};
use moonpool_core::{Providers, TokioProviders};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{any::Any, collections::BTreeSet, fmt, marker::PhantomData, sync::Arc};

/// An operator: a name, and the configuration of the nodes that run it.
///
/// Implemented by `#[derive(Operator)]` on a structure whose fields are the
/// configuration (see [`crate::param`]): the reference comes from
/// `#[operator("acme/thing@1")]`, the line the catalogue shows from the
/// documentation comment, the parameters the API and the UI may change from
/// the fields marked `#[param]`, their bounds included. An operator without
/// configuration is a structure without fields.
///
/// What the operator does is said by one more trait: [`Source`],
/// [`Transformer`] or [`Sink`].
///
/// ```
/// use streaming_core::Operator;
///
/// /// Keeps the events of one action.
/// #[derive(Debug, Clone, Operator)]
/// #[operator("acme/only-action@1")]
/// pub struct OnlyAction {
///     /// The action kept.
///     #[param(default = "click")]
///     pub action: String,
/// }
///
/// assert_eq!(OnlyAction::OP, "acme/only-action@1");
/// assert_eq!(OnlyAction::ABOUT, "Keeps the events of one action.");
/// assert_eq!(OnlyAction::params()[0].name, "action");
/// ```
pub trait Operator: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The reference the operator is registered under (`std/tumbling@1`).
    const OP: &'static str;

    /// What the operator does, in one line, for the catalogue.
    const ABOUT: &'static str = "";

    /// The fields of the configuration the API and the UI may change.
    #[must_use]
    fn params() -> Vec<ParamSpec> { Vec::new() }

    /// Refuse a configuration that cannot run. Called before a node is
    /// started, and when the application checks its graphs.
    ///
    /// # Errors
    ///
    /// What is wrong, in words.
    fn check(&self) -> Result<(), OpError> { Ok(()) }

    /// The secrets the configuration names. A node is not started while its
    /// tenant lacks one of them.
    #[must_use]
    fn secrets(&self) -> Vec<&Secret> { Vec::new() }
}

/// A node added to a [`GraphBuilder`], to connect it to others. `T` is what
/// flows out of it.
///
/// A handle only means something to the builder that returned it.
pub struct Node<T>(usize, PhantomData<fn() -> T>);

impl<T> Clone for Node<T> {
    fn clone(&self) -> Self { *self }
}

impl<T> Copy for Node<T> {}

impl<T> fmt::Debug for Node<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_tuple("Node").field(&self.0).finish() }
}

/// What flows out of a node added with [`GraphBuilder::node`]: the builder
/// does not know.
#[derive(Debug, Clone, Copy)]
pub struct Untyped;

/// What flows out of a sink: nothing.
#[derive(Debug, Clone, Copy)]
pub struct Sunk;

impl Node<Untyped> {
    /// Say what flows out of the node.
    #[must_use]
    pub fn typed<T>(self) -> Node<T> { Node(self.0, PhantomData) }
}

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

type Registration<P> = Arc<dyn Fn(&mut Registry<P>) -> Result<(), OpError> + Send + Sync>;

/// The operators the nodes of a graph name, ready to be registered.
pub struct Operators<P: Providers = TokioProviders> {
    registrations: Vec<(String, Registration<P>)>,
}

impl<P: Providers> Clone for Operators<P> {
    fn clone(&self) -> Self {
        Self {
            registrations: self.registrations.clone(),
        }
    }
}

impl<P: Providers> Default for Operators<P> {
    fn default() -> Self {
        Self {
            registrations: Vec::new(),
        }
    }
}

impl<P: Providers> Operators<P> {
    /// Register them. Returns the nodes whose operator cannot be, and why.
    pub fn register(&self, registry: &mut Registry<P>) -> Vec<(String, OpError)> {
        self.registrations
            .iter()
            .filter_map(|(node, register)| register(registry).err().map(|error| (node.clone(), error)))
            .collect()
    }

    /// Add those of another graph.
    pub fn extend(&mut self, other: Operators<P>) { self.registrations.extend(other.registrations); }
}

/// Builds a [`GraphSpec`] from typed operators.
///
/// Adding nodes and edges never fails: every problem is reported, the first
/// one found, by [`GraphBuilder::build`].
///
/// `P` is the runtime the operators will run on: tokio, unless a simulation
/// builds the graph ([`GraphBuilder::on`]).
pub struct GraphBuilder<P: Providers = TokioProviders> {
    tenant: String,
    graph: String,
    settings: GraphSettings,
    nodes: Vec<Pending>,
    edges: Vec<(usize, usize)>,
    operators: Operators<P>,
}

impl GraphBuilder<TokioProviders> {
    /// An empty graph `graph` of `tenant`, with the default settings.
    #[must_use]
    pub fn new(tenant: impl Into<String>, graph: impl Into<String>) -> Self { Self::on(tenant, graph) }
}

impl<P: Providers> GraphBuilder<P> {
    /// An empty graph `graph` of `tenant` whose operators run on the
    /// providers `P`.
    #[must_use]
    pub fn on(tenant: impl Into<String>, graph: impl Into<String>) -> Self {
        Self {
            tenant: tenant.into(),
            graph: graph.into(),
            settings: GraphSettings::default(),
            nodes: Vec::new(),
            edges: Vec::new(),
            operators: Operators::default(),
        }
    }

    /// Replace the engine settings.
    #[must_use]
    pub fn settings(mut self, settings: GraphSettings) -> Self {
        self.settings = settings;
        self
    }

    fn add<O: Operator>(&mut self, id: impl Into<String>, operator: &O) -> usize {
        self.nodes.push(Pending {
            id: id.into(),
            op: O::OP,
            config: serde_json::to_value(operator).map_err(|error| error.to_string()),
        });
        self.nodes.len() - 1
    }

    fn registers(
        &mut self,
        node: usize,
        register: impl Fn(&mut Registry<P>) -> Result<(), OpError> + Send + Sync + 'static,
    ) {
        let id = self.nodes[node].id.clone();
        self.operators.registrations.push((id, Arc::new(register)));
    }

    /// Add a node `id` running `operator`, connected to nothing yet, without
    /// registering the operator: for one registered by other means
    /// ([`Registry::register`]), whose types the builder does not know.
    pub fn node<O: Operator>(&mut self, id: impl Into<String>, operator: O) -> Node<Untyped> {
        Node(self.add(id, &operator), PhantomData)
    }

    /// Add a node `id` reading from `source`.
    pub fn source<S: Source>(&mut self, id: impl Into<String>, source: S) -> Node<S::Out> {
        let node = self.add(id, &source);
        self.registers(node, |registry| {
            registry.source::<S>();
            Ok(())
        });
        Node(node, PhantomData)
    }

    /// Add a node `id` running `transformer` on what flows out of `upstream`.
    pub fn then<In: 'static, T: Transformer<In>>(
        &mut self,
        upstream: Node<In>,
        id: impl Into<String>,
        transformer: T,
    ) -> Node<T::Out> {
        let node = self.add(id, &transformer);
        self.edges.push((upstream.0, node));
        let example = Arc::new(transformer);
        self.registers(node, move |registry| registry.transformer::<In, T>(&example).map(drop));
        Node(node, PhantomData)
    }

    /// Add a node `id` writing what flows out of `upstream` to `sink`.
    pub fn sink<In: Any + Send + Sync, S: Sink<In>>(
        &mut self,
        upstream: Node<In>,
        id: impl Into<String>,
        sink: S,
    ) -> Node<Sunk> {
        let node = self.add(id, &sink);
        self.edges.push((upstream.0, node));
        self.registers(node, |registry| {
            registry.sink::<In, S>();
            Ok(())
        });
        Node(node, PhantomData)
    }

    /// Connect `from` to `to`, on top of the edges [`GraphBuilder::then`] and
    /// [`GraphBuilder::sink`] made: a node fed by several others. The types
    /// are checked when the application starts, not by the compiler.
    pub fn edge<A, B>(&mut self, from: Node<A>, to: Node<B>) -> &mut Self {
        self.edges.push((from.0, to.0));
        self
    }

    /// The description of the graph, at version 0.
    ///
    /// # Errors
    ///
    /// The first problem found.
    pub fn build(self) -> Result<GraphSpec, BuildError> { self.into_parts().map(|(spec, _)| spec) }

    /// The description of the graph, at version 0, and the operators its
    /// nodes name.
    ///
    /// # Errors
    ///
    /// The first problem found.
    pub fn into_parts(self) -> Result<(GraphSpec, Operators<P>), BuildError> {
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
        let id = |node: usize| {
            nodes
                .get(node)
                .map(|spec| spec.id.clone())
                .ok_or(BuildError::ForeignNode)
        };
        let edges = self
            .edges
            .iter()
            .map(|&(from, to)| Ok((id(from)?, id(to)?)))
            .collect::<Result<_, BuildError>>()?;
        let spec = GraphSpec {
            tenant,
            graph,
            version: 0,
            settings: self.settings,
            nodes,
            edges,
            fingerprint: None,
        };
        Ok((spec, self.operators))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Keyed, Pipe,
        stdops::{Count, Identity, Sliding, Tumbling},
    };
    use std::time::Duration;

    fn id(s: &str) -> NodeId { NodeId::new(s).unwrap() }

    /// Groups numbers by parity.
    #[derive(Debug, Clone, crate::Operator)]
    #[operator("test/parity@1")]
    struct Parity;

    impl Transformer<u64> for Parity {
        type Out = Keyed<u64>;

        fn transform(&self, input: Pipe<u64>) -> Pipe<Keyed<u64>> { input.key_by(|n| n % 2) }
    }

    #[test]
    fn builds_the_description_a_worker_reads() {
        let mut graph = GraphBuilder::new("acme", "g");
        let a = graph.node("a", Identity).typed::<u64>();
        let k = graph.then(a, "k", Parity);
        let w = graph.then(k, "w", Tumbling::of(Duration::from_secs(300)));
        let c = graph.then(w, "c", Count);
        let s = graph.node("s", Identity);
        graph.edge(c, s).edge(a, s);
        let (spec, operators) = graph.into_parts().unwrap();
        assert_eq!(spec.key().to_string(), "acme/g");
        assert_eq!(spec.settings, GraphSettings::default());
        assert_eq!(
            spec.edges,
            [
                (id("a"), id("k")),
                (id("k"), id("w")),
                (id("w"), id("c")),
                (id("c"), id("s")),
                (id("a"), id("s"))
            ]
        );
        // Operators without configuration store none.
        assert_eq!(spec.nodes[0].config, Value::Null);
        assert_eq!(spec.nodes[3].op, OpRef::new("std/count@1").unwrap());
        // The stored form is the wire form, and parses back to the same graph.
        let wire = serde_json::to_value(&spec).unwrap();
        assert_eq!(
            wire["nodes"][2]["config"],
            serde_json::json!({"size": "5m", "allowedLateness": "0ms"})
        );
        assert_eq!(serde_json::from_value::<GraphSpec>(wire).unwrap(), spec);

        // Naming an operator registers it, for the type it is given.
        let mut registry = Registry::<TokioProviders>::empty();
        assert!(operators.register(&mut registry).is_empty());
        let catalog = registry.catalog();
        let parity = catalog.get(&OpRef::new("test/parity@1").unwrap()).unwrap();
        assert_eq!(parity.kind, crate::OpKind::KeyBy);
        assert_eq!(parity.input, crate::TypeTag::of::<u64>());
        assert_eq!(parity.description, "Groups numbers by parity.");
        let count = catalog.get(&OpRef::new("std/count@1").unwrap()).unwrap();
        assert_eq!(count.kind, crate::OpKind::Aggregate);
        assert_eq!(count.input, crate::TypeTag::of::<u64>());
        assert!(catalog.get(&OpRef::new("std/tumbling@1").unwrap()).is_some());
        // A node added without its types registers nothing.
        assert!(catalog.get(&OpRef::new("std/identity@1").unwrap()).is_none());

        // Next to the stock operators, registered for any type, the ones of
        // the graph are one more way of building them.
        let mut registry = Registry::<TokioProviders>::standard();
        assert!(operators.register(&mut registry).is_empty());
        let count = registry
            .catalog()
            .get(&OpRef::new("std/count@1").unwrap())
            .unwrap()
            .clone();
        assert_eq!(count.input, crate::TypeTag::Any);
        assert_eq!(count.output, crate::TypeTag::of::<crate::Windowed<u64>>());
        assert_eq!(count.signatures.len(), 2);
        assert_eq!(
            count.output_for(&crate::TypeTag::of::<u64>()),
            Some(count.output.clone())
        );
        assert_eq!(
            count.output_for(&crate::TypeTag::of::<String>()),
            Some(count.output.clone())
        );
        // Registering the same graph again changes nothing.
        assert!(operators.register(&mut registry).is_empty());
        assert_eq!(
            registry.catalog().get(&OpRef::new("std/count@1").unwrap()),
            Some(&count)
        );
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
