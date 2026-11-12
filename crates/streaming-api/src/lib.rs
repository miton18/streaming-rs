//! HTTP API server of streaming-rs.
//!
//! The control plane is KV-only: this server reads and writes documents in the
//! KV (see [`streaming_core::keys`]) and computes the status views the web UI
//! shows. See `docs/http-api.md` for the contract.
//!
//! - [`router`] / [`serve`]: the axum application (API under `/api/v1`, health
//!   check on `/healthz`, embedded web UI on everything else).
//! - [`control`]: the KV-level operations behind the write endpoints, reusable
//!   from a CLI.
//! - [`status`]: pure aggregation of worker snapshots into views.
//!
//! # Security
//!
//! **This version has no authentication and no authorization.** Anyone who can
//! reach the port can read every tenant's graphs and outputs, and can create,
//! reconfigure, pause and delete any graph. The API must **not** be exposed
//! publicly as is: bind it to a loopback or private interface, or put an
//! authenticating reverse proxy in front of it. CORS is permissive.

use axum::Router;
use std::{future::Future, sync::Arc};
use streaming_core::{Clock, DynKv};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

pub mod control;
pub mod error;
mod routes;
pub use routes::openapi;
pub mod status;
mod ui;

pub use control::ControlError;

/// Settings of the API server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiConfig {
    /// A worker is alive when `now - heartbeat_ms <= member_timeout_ms`.
    pub member_timeout_ms: i64,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            member_timeout_ms: 5000,
        }
    }
}

/// Provider of KV statistics shown by `GET /api/v1/cluster` (standalone mode).
pub type KvStatsFn = Arc<dyn Fn() -> serde_json::Value + Send + Sync>;

/// Shared state of the handlers.
#[derive(Clone)]
pub struct ApiState {
    /// The control-plane KV.
    pub kv: DynKv,
    /// Wall clock.
    pub clock: Arc<dyn Clock>,
    /// KV statistics, when the embedding process has them.
    pub kv_stats: Option<KvStatsFn>,
    /// Settings.
    pub config: ApiConfig,
}

/// Build the application: API routes, embedded UI as fallback, permissive CORS
/// and a debug-level request trace.
pub fn router(state: ApiState) -> Router {
    let app = routes::routes().fallback(ui::serve_ui).with_state(state);
    // Outside the routes: axum adds the `Allow` header of a 405 after the
    // layers of a route have run, and this one reads it.
    Router::new()
        .fallback_service(app)
        .layer(axum::middleware::from_fn(error::method_not_allowed))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
}

/// Serve [`router`] on `listener` until `shutdown` resolves.
pub async fn serve(
    listener: tokio::net::TcpListener,
    state: ApiState,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await
}
