//! Contracts shared by every `streaming-rs` crate.
//!
//! This crate holds no runtime. It defines:
//!
//! - identifiers ([`TenantId`], [`GraphId`], [`NodeId`], [`SliceId`], ...),
//! - the declarative graph description stored in the KV ([`GraphSpec`]),
//! - the operator contracts and their registry ([`Registry`]),
//! - the key-value store contract ([`Kv`]) with an in-memory implementation,
//! - the control-plane documents exchanged through the KV ([`control`]).
//!
//! Everything a worker, the HTTP API, the CLI or a connector needs to agree on
//! lives here.

pub mod clock;
pub mod control;
pub mod data;
pub mod ids;
pub mod keys;
pub mod kv;
pub mod ops;
pub mod registry;
pub mod spec;
pub mod stdops;
pub mod time;

pub use clock::{Clock, EnvSecrets, NoSecrets, SecretProvider, StaticSecrets, SystemClock};
pub use data::{Datum, Emit, EmitId, Key, Origin, Position, Record, Windowed, datum};
pub use ids::{
    GraphId, GraphKey, IdError, IdProblem, MAX_ID_LEN, NodeId, OpRef, SliceId, SliceKey, TenantId, WorkerId,
};
pub use kv::{DynKv, Kv, KvError, MemKv};
pub use ops::{
    Aggregator, DynAggregator, DynKey, DynMap, OpError, PartitionReader, SinkConnector, SinkError, SourceConnector,
    SourceError, SourceRecord, WindowAssigner,
};
pub use registry::{Catalog, OpContext, OpDescriptor, OpFactory, OpKind, ParamSpec, Registry, TypeTag};
pub use spec::{GraphSpec, NodeSpec, SpecError, Topology, WiringRule};
pub use time::{Timestamp, Window, format_duration, parse_duration};
