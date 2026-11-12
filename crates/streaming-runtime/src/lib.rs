//! The `streaming-rs` execution engine.
//!
//! A [`Worker`] turns graph descriptions found in the KV into running actors:
//!
//! - every node of a graph is an actor with bounded input edges, so a slow
//!   actor suspends its producers and backpressure reaches the source;
//! - every actor accounts its time as busy, idle or backpressured, which is
//!   what lets the control plane name the bottleneck of a graph;
//! - a graph is cut into *slices* (one per source partition, plus one per
//!   aggregate node for emitting closed windows). A slice commits its
//!   progress and its state changes atomically, one epoch at a time, with a
//!   single `HSETNX`: that is the whole exactly-once mechanism.
//!
//! Everything is generic over [`moonpool_core::Providers`]: the same code runs
//! on tokio in production and under deterministic simulation.

use moonpool_core::Providers;
use std::sync::Arc;
use streaming_core::{Clock, DynKv, Registry, SecretProvider};

pub mod builtin;
mod controller;
#[cfg(feature = "gossip")]
mod gossip;
pub mod inspect;
mod load;
mod mailbox;
pub mod memlog;
mod metrics;
pub mod mutation;
mod scope;
mod slice;
mod state;
mod worker;

#[cfg(feature = "gossip")]
pub use gossip::{GossipConfig, GossipError, GossipMembership};
pub use load::{LoadProbe, NoLoad, ProcessLoad};
pub use scope::TaskScope;
pub use worker::{KvMembership, Membership, Worker, WorkerConfig};

/// The services a worker runs on.
pub struct RuntimeEnv<P: Providers> {
    /// Network, time, tasks, randomness and storage.
    pub providers: P,
    /// The key-value store holding descriptions, placement and state.
    pub kv: DynKv,
    /// Wall clock.
    pub clock: Arc<dyn Clock>,
    /// Tenant-scoped secrets.
    pub secrets: Arc<dyn SecretProvider>,
    /// The operators compiled into this worker.
    pub registry: Arc<Registry<P>>,
    /// What the operators of the process share
    /// ([`streaming_core::OpContext::shared`]).
    pub shared: Arc<streaming_core::Shared>,
}
