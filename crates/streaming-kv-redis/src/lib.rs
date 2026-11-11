//! Redis-protocol (RESP2) client implementing [`streaming_core::Kv`], for
//! Redis and Clever Cloud Materia KV, written against the Moonpool provider
//! traits so the same client code runs under deterministic simulation.
//!
//! * [`RedisKv`] - the pooled client (`GET SET DEL INCR HSETNX HSET HGET
//!   HSCAN HDEL AUTH PING`, nothing else: no `MULTI`, Lua, pub/sub, lists).
//! * [`RedisConfig`] - settings and [`RedisConfig::from_url`].
//! * [`resp`] - the pure RESP2 codec.
//! * [`fake`] / [`FakeRedis`] - an in-process server for simulations.
//! * [`testkit`] - the reusable [`Kv`](streaming_core::Kv) conformance suite.
//!
//! Every error is "outcome unknown" for the caller: I/O errors, timeouts and
//! protocol desyncs drop the connection and surface as
//! `KvError::Unavailable`, and no command is ever retried here.
//!
//! ```no_run
//! # async fn demo() -> Result<(), streaming_core::KvError> {
//! use moonpool_core::TokioProviders;
//! use streaming_core::Kv;
//! use streaming_kv_redis::{RedisConfig, RedisKv};
//!
//! let kv = RedisKv::new(
//!     TokioProviders::new(),
//!     RedisConfig::from_url("redis://:my-token@localhost:6379")?,
//! );
//! kv.ping().await?;
//! assert!(kv.hsetnx("log", "c:1", b"first".to_vec()).await?);
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod config;
pub mod fake;
pub mod resp;
pub mod testkit;
#[cfg(feature = "tls")]
mod tls;

pub use client::RedisKv;
pub use config::RedisConfig;
pub use fake::FakeRedis;
