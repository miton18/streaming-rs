//! HTTP handlers.

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use streaming_core::control::{GraphIndexEntry, OutputRow, RunState};
use streaming_core::{Catalog, GraphId, GraphKey, GraphSpec, NodeId, TenantId, keys};

use crate::ApiState;
use crate::control::{self, ControlError};
use crate::status::{self, GraphDetail, MemberReport};

/// Error answered as `{"error": "<message>"}`.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }
}

impl From<ControlError> for ApiError {
    fn from(error: ControlError) -> Self {
        match error {
            ControlError::NotFound(message) => Self::not_found(message),
            ControlError::Invalid(message) => Self::bad_request(message),
            ControlError::Kv(e) => {
                tracing::warn!(error = %e, "kv failure");
                Self::new(StatusCode::SERVICE_UNAVAILABLE, e.to_string())
            }
        }
    }
}

impl From<streaming_core::IdError> for ApiError {
    fn from(error: streaming_core::IdError) -> Self {
        Self::bad_request(error.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, axum::Json(json!({ "error": self.message }))).into_response()
    }
}

type ApiResult<T> = Result<axum::Json<T>, ApiError>;

fn graph_key(tenant: &str, graph: &str) -> Result<GraphKey, ApiError> {
    Ok(GraphKey::new(TenantId::new(tenant)?, GraphId::new(graph)?))
}

/// Everything read once to build graph views.
struct Context {
    catalog: Catalog,
    placement: Option<streaming_core::control::Placement>,
    members: Vec<MemberReport>,
    now_ms: i64,
}

impl Context {
    async fn load(state: &ApiState) -> Result<Self, ApiError> {
        let kv = &*state.kv;
        Ok(Self {
            catalog: control::load_catalog(kv).await?,
            placement: control::load_placement(kv).await?,
            members: control::load_member_reports(kv).await?,
            now_ms: state.clock.now_ms(),
        })
    }
}

async fn detail(
    state: &ApiState,
    context: &Context,
    key: &GraphKey,
) -> Result<Option<GraphDetail>, ApiError> {
    let kv = &*state.kv;
    let Some(spec) = control::load_spec(kv, key).await? else {
        return Ok(None);
    };
    let plan = control::load_plan(kv, key).await?;
    let desired = control::load_desired(kv, key).await?.unwrap_or_default();
    Ok(Some(status::graph_detail(
        &spec,
        plan.as_ref(),
        &desired,
        &context.catalog,
        context.placement.as_ref(),
        &context.members,
        context.now_ms,
        state.config.member_timeout_ms,
    )))
}

async fn healthz() -> &'static str {
    "ok"
}

async fn cluster(State(state): State<ApiState>) -> ApiResult<status::ClusterView> {
    let kv = &*state.kv;
    Ok(axum::Json(status::cluster_view(
        control::load_epoch(kv).await?,
        state.clock.now_ms(),
        &control::load_member_reports(kv).await?,
        control::load_placement(kv).await?,
        state.kv_stats.as_ref().map(|stats| stats()),
        state.config.member_timeout_ms,
    )))
}

async fn catalog(State(state): State<ApiState>) -> ApiResult<status::CatalogView> {
    let catalog = control::load_catalog(&*state.kv).await?;
    Ok(axum::Json(status::catalog_view(&catalog)))
}

async fn tenants(State(state): State<ApiState>) -> ApiResult<status::TenantsView> {
    let kv = &*state.kv;
    Ok(axum::Json(status::tenants_view(
        &control::load_index(kv).await?,
        &control::load_member_reports(kv).await?,
        state.clock.now_ms(),
        state.config.member_timeout_ms,
    )))
}

async fn tenant_graphs(
    State(state): State<ApiState>,
    Path(tenant): Path<String>,
) -> ApiResult<status::GraphsView> {
    let tenant = TenantId::new(tenant)?;
    let entries: Vec<GraphIndexEntry> = control::load_index(&*state.kv)
        .await?
        .into_iter()
        .filter(|entry| entry.tenant == tenant)
        .collect();
    if entries.is_empty() {
        return Err(ApiError::not_found(format!("unknown tenant `{tenant}`")));
    }
    let context = Context::load(&state).await?;
    let mut graphs = Vec::new();
    for entry in entries {
        let key = GraphKey::new(entry.tenant.clone(), entry.graph.clone());
        if let Some(detail) = detail(&state, &context, &key).await? {
            graphs.push(status::graph_summary(&detail, entry.updated_ms));
        }
    }
    Ok(axum::Json(status::GraphsView { graphs }))
}

/// The detail of a graph, or 404 when it is not in the index.
async fn existing_detail(state: &ApiState, key: &GraphKey) -> Result<GraphDetail, ApiError> {
    let unknown = || ApiError::not_found(format!("unknown graph `{key}`"));
    control::load_index_entry(&*state.kv, key)
        .await?
        .ok_or_else(unknown)?;
    let context = Context::load(state).await?;
    detail(state, &context, key).await?.ok_or_else(unknown)
}

async fn graph(
    State(state): State<ApiState>,
    Path((tenant, graph)): Path<(String, String)>,
) -> ApiResult<GraphDetail> {
    let key = graph_key(&tenant, &graph)?;
    Ok(axum::Json(existing_detail(&state, &key).await?))
}

async fn dot(
    State(state): State<ApiState>,
    Path((tenant, graph)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let key = graph_key(&tenant, &graph)?;
    let spec = control::load_spec(&*state.kv, &key)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("unknown graph `{key}`")))?;
    Ok((
        [(header::CONTENT_TYPE, "text/vnd.graphviz; charset=utf-8")],
        status::dot(&spec),
    )
        .into_response())
}

const DEFAULT_LIMIT: usize = 200;
const MAX_LIMIT: usize = 2000;

fn parse_limit(query: Option<&str>) -> Result<usize, ApiError> {
    for pair in query.unwrap_or_default().split('&') {
        if let Some(("limit", value)) = pair.split_once('=') {
            return value
                .parse::<usize>()
                .map(|limit| limit.min(MAX_LIMIT))
                .map_err(|_| ApiError::bad_request(format!("invalid limit `{value}`")));
        }
    }
    Ok(DEFAULT_LIMIT)
}

async fn outputs(
    State(state): State<ApiState>,
    Path((tenant, graph, node)): Path<(String, String, String)>,
    RawQuery(query): RawQuery,
) -> ApiResult<Value> {
    let key = graph_key(&tenant, &graph)?;
    let node = NodeId::new(node)?;
    let limit = parse_limit(query.as_deref())?;
    let spec = control::load_spec(&*state.kv, &key)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("unknown graph `{key}`")))?;
    if spec.node(&node).is_none() {
        return Err(ApiError::not_found(format!(
            "unknown node `{node}` in graph `{key}`"
        )));
    }
    let mut rows: Vec<OutputRow> = state
        .kv
        .hgetall_json::<OutputRow>(&keys::graph_output(&key, &node))
        .await
        .map_err(ControlError::from)?
        .into_iter()
        .map(|(_, row)| row)
        .collect();
    rows.sort_by(|a, b| b.ts.cmp(&a.ts).then_with(|| a.key.cmp(&b.key)));
    rows.truncate(limit);
    Ok(axum::Json(json!({ "rows": rows })))
}

fn parse_body<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<T, ApiError> {
    serde_json::from_slice(body).map_err(|e| ApiError::bad_request(format!("invalid body: {e}")))
}

async fn put_graph(
    State(state): State<ApiState>,
    Path((tenant, graph)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Value> {
    let key = graph_key(&tenant, &graph)?;
    let spec: GraphSpec = parse_body(&body)?;
    if spec.key() != key {
        return Err(ApiError::bad_request(format!(
            "the description is for `{}` but the path names `{key}`",
            spec.key()
        )));
    }
    let version = control::apply_graph(&*state.kv, &*state.clock, spec).await?;
    Ok(axum::Json(json!({ "version": version })))
}

async fn delete_graph(
    State(state): State<ApiState>,
    Path((tenant, graph)): Path<(String, String)>,
) -> ApiResult<Value> {
    let key = graph_key(&tenant, &graph)?;
    control::delete_graph(&*state.kv, &*state.clock, &key).await?;
    Ok(axum::Json(json!({ "deleting": true })))
}

#[derive(Deserialize)]
struct StateBody {
    state: RunState,
}

async fn set_state(
    State(state): State<ApiState>,
    Path((tenant, graph)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Value> {
    let key = graph_key(&tenant, &graph)?;
    let StateBody { state: run_state } = parse_body(&body)?;
    control::set_state(&*state.kv, &*state.clock, &key, run_state).await?;
    Ok(axum::Json(json!({ "state": run_state })))
}

async fn set_paused(
    state: ApiState,
    (tenant, graph, node): (String, String, String),
    paused: bool,
) -> ApiResult<Value> {
    let key = graph_key(&tenant, &graph)?;
    let node = NodeId::new(node)?;
    control::set_node_paused(&*state.kv, &*state.clock, &key, &node, paused).await?;
    Ok(axum::Json(json!({ "node": node, "paused": paused })))
}

async fn pause(
    State(state): State<ApiState>,
    Path(path): Path<(String, String, String)>,
) -> ApiResult<Value> {
    set_paused(state, path, true).await
}

async fn resume(
    State(state): State<ApiState>,
    Path(path): Path<(String, String, String)>,
) -> ApiResult<Value> {
    set_paused(state, path, false).await
}

async fn patch_config(
    State(state): State<ApiState>,
    Path((tenant, graph, node)): Path<(String, String, String)>,
    body: Bytes,
) -> ApiResult<Value> {
    let key = graph_key(&tenant, &graph)?;
    let node = NodeId::new(node)?;
    let patch: Map<String, Value> = parse_body(&body)?;
    let version = control::patch_node_config(&*state.kv, &*state.clock, &key, &node, patch).await?;
    Ok(axum::Json(json!({ "version": version })))
}

/// The `/healthz` and `/api/v1` routes.
pub fn routes() -> Router<ApiState> {
    let graph_path = "/api/v1/tenants/{tenant}/graphs/{graph}";
    Router::new()
        .route("/healthz", get(healthz))
        .route("/api/v1/cluster", get(cluster))
        .route("/api/v1/catalog", get(catalog))
        .route("/api/v1/tenants", get(tenants))
        .route("/api/v1/tenants/{tenant}/graphs", get(tenant_graphs))
        .route(graph_path, get(graph).put(put_graph).delete(delete_graph))
        .route(&format!("{graph_path}/state"), post(set_state))
        .route(&format!("{graph_path}/dot"), get(dot))
        .route(&format!("{graph_path}/outputs/{{node}}"), get(outputs))
        .route(&format!("{graph_path}/nodes/{{node}}/pause"), post(pause))
        .route(&format!("{graph_path}/nodes/{{node}}/resume"), post(resume))
        .route(
            &format!("{graph_path}/nodes/{{node}}/config"),
            axum::routing::patch(patch_config),
        )
}
