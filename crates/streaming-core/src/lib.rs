//! Contracts shared by every `streaming-rs` crate.
//!
//! This crate holds no runtime. It defines:
//!
//! - identifiers ([`TenantId`], [`GraphId`], [`NodeId`], [`SliceId`], ...),
//! - the declarative graph description stored in the KV ([`GraphSpec`]),
//!   and its typed construction ([`GraphBuilder`], [`Operator`]),
//! - the operator contracts and their registry ([`Registry`]),
//! - the key-value store contract ([`Kv`]) with an in-memory implementation,
//! - the control-plane documents exchanged through the KV ([`control`]).
//!
//! Everything a worker, the HTTP API, the CLI or a connector needs to agree on
//! lives here.

// The derives name this crate by its name, here too.
extern crate self as streaming_core;

pub mod builder;
pub mod clock;
pub mod control;
pub mod data;
pub mod ids;
pub mod keys;
pub mod kv;
pub mod mutation;
pub mod ops;
pub mod param;
pub mod pipe;
pub mod registry;
pub mod spec;
pub mod stdops;
pub mod time;

pub use builder::{BuildError, GraphBuilder, Node, Operator, Operators, Sunk, Untyped};
pub use clock::{Clock, EnvSecrets, NoSecrets, SecretProvider, StaticSecrets, SystemClock};
pub use data::{Datum, Emit, EmitId, Key, Origin, Position, Record, Windowed, datum};
pub use ids::{
    GraphId, GraphKey, IdError, IdProblem, MAX_ID_LEN, NodeId, OpRef, SliceId, SliceKey, TenantId, WorkerId,
};
pub use kv::{DynKv, Kv, KvError, MemKv};
pub use moonpool_core;
pub use ops::{
    Aggregator, DynAggregator, DynKey, DynMap, OpError, PartitionReader, SinkConnector, SinkError, SourceConnector,
    SourceError, SourceRecord, WindowAssigner,
};
#[doc(hidden)]
pub use param::private as __private;
pub use param::{Param, Secret};
pub use pipe::{
    Batch, Closed, Context, Element, Keyed, Opaque, Pipe, Sink, SinkPipe, Source, SourcePipe, Transformer, Windows,
};
pub use registry::{
    Catalog, OpContext, OpDescriptor, OpFactory, OpKind, ParamSpec, Registry, Shared, Signature, TypeTag,
};
pub use spec::{GraphSettings, GraphSpec, NodeSpec, SpecError, Topology, WiringRule};
/// Derives [`Operator`] for a structure whose fields are its configuration:
/// see [`param`].
pub use streaming_macros::Operator;
/// Derives [`Param`] for an enumeration offered as a list of choices: see
/// [`param`].
pub use streaming_macros::Param;
pub use time::{Timestamp, Window, format_duration, parse_duration};
