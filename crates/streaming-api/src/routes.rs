//! HTTP handlers. Each one carries its method and path in a `#[route]`
//! attribute; [`api`] collects them, and the OpenAPI document is derived from
//! their signatures.

use crate::{
    ApiState,
    control::{self, ControlError},
    error::{self, ApiError},
    status::{self, GraphDetail, MemberReport},
};
use axum::{
    Extension, Router,
    body::Bytes,
    extract::{FromRequest, FromRequestParts, Path, Request, State},
    http::{StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use std::sync::Arc;
use streaming_core::{
    Catalog, GraphId, GraphKey, NodeId, TenantId,
    control::{GraphIndexEntry, OutputRow, RunState, TenantRecord},
    keys,
};
use streaming_openapi::{ApiRouter, Info, OpenApi, OperationContext, OperationInput, OperationOutput, route};

type ApiResult<T> = Result<axum::Json<T>, ApiError>;

/// A JSON request body. Unlike `axum::Json`, it does not require a
/// `Content-Type` header and answers a malformed body with an [`ApiError`].
pub(crate) struct JsonBody<T>(pub T);

impl<T: DeserializeOwned, S: Send + Sync> FromRequest<S> for JsonBody<T> {
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, ApiError> {
        let body = Bytes::from_request(request, state).await.map_err(|rejection| {
            ApiError::new(if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
                &error::BODY_TOO_LARGE
            } else {
                &error::BODY_UNREADABLE
            })
        })?;
        serde_json::from_slice(&body)
            .map(JsonBody)
            .map_err(|e| ApiError::body(&e))
    }
}

impl<T: JsonSchema> OperationInput for JsonBody<T> {
    fn describe(operation: &mut OperationContext<'_>) {
        <axum::Json<T> as OperationInput>::describe(operation);
    }
}

/// Query string parameters. Unlike `axum::extract::Query`, it answers a
/// malformed query with an [`ApiError`] carrying the parser's message.
pub(crate) struct QueryParams<T>(pub T);

impl<T: DeserializeOwned, S: Send + Sync> FromRequestParts<S> for QueryParams<T> {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, ApiError> {
        serde_urlencoded::from_str(parts.uri.query().unwrap_or_default())
            .map(QueryParams)
            .map_err(|e| ApiError::new(&error::QUERY_INVALID).with("detail", e.to_string()))
    }
}

impl<T: JsonSchema> OperationInput for QueryParams<T> {
    fn describe(operation: &mut OperationContext<'_>) {
        <axum::extract::Query<T> as OperationInput>::describe(operation);
    }
}

/// A graph description in Graphviz's DOT language.
pub(crate) struct Dot(String);

impl IntoResponse for Dot {
    fn into_response(self) -> Response {
        ([(header::CONTENT_TYPE, "text/vnd.graphviz; charset=utf-8")], self.0).into_response()
    }
}

impl OperationOutput for Dot {
    fn describe(operation: &mut OperationContext<'_>) {
        operation.add_response(
            "200",
            streaming_openapi::Response::with_content(
                "The graph in Graphviz's DOT language.",
                "text/vnd.graphviz",
                json!({"type": "string"}),
            ),
        );
    }
}

/// The OpenAPI document, served as JSON.
#[derive(Clone)]
pub(crate) struct OpenApiDocument(Arc<Value>);

impl IntoResponse for OpenApiDocument {
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}

impl OperationOutput for OpenApiDocument {
    fn describe(operation: &mut OperationContext<'_>) {
        operation.add_response(
            "200",
            streaming_openapi::Response::json(
                "This document.",
                json!({"type": "object", "description": "An OpenAPI 3.2 document."}),
            ),
        );
    }
}

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

async fn detail(state: &ApiState, context: &Context, key: &GraphKey) -> Result<Option<GraphDetail>, ApiError> {
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

/// Liveness check.
///
/// Answers `ok` as long as the process serves HTTP; it reads nothing from the
/// store.
#[route(GET, "/healthz", tag = "cluster")]
async fn healthz() -> &'static str {
    "ok"
}

/// The OpenAPI document of this API.
#[route(GET, "/api/v1/openapi.json", tag = "cluster")]
async fn openapi_document(Extension(document): Extension<OpenApiDocument>) -> OpenApiDocument {
    document
}

/// The workers, the placement and the store.
///
/// `workers` comes from `cluster:members` (a worker is `alive` when its
/// heartbeat is younger than the member timeout) joined with each worker's
/// metrics snapshot. `kv` is present only when the embedding process provides
/// store statistics (standalone mode).
#[route(GET, "/api/v1/cluster", tag = "cluster")]
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

/// The operators the cluster can run, sorted by reference.
#[route(GET, "/api/v1/catalog", tag = "cluster")]
async fn catalog(State(state): State<ApiState>) -> ApiResult<status::CatalogView> {
    let catalog = control::load_catalog(&*state.kv).await?;
    Ok(axum::Json(status::catalog_view(&catalog)))
}

/// The tenants: those created, and those the graph index names without having
/// been created. Each comes with its consumption on alive workers.
#[route(GET, "/api/v1/tenants", tag = "tenants")]
async fn tenants(State(state): State<ApiState>) -> ApiResult<status::TenantsView> {
    let kv = &*state.kv;
    Ok(axum::Json(status::tenants_view(
        &control::load_tenants(kv).await?,
        &control::load_index(kv).await?,
        &control::load_member_reports(kv).await?,
        state.clock.now_ms(),
        state.config.member_timeout_ms,
    )))
}

/// A tenant with its consumption on alive workers.
#[route(GET, "/api/v1/tenants/{tenant}", tag = "tenants")]
async fn tenant(State(state): State<ApiState>, Path(tenant): Path<String>) -> ApiResult<status::TenantSummary> {
    let tenant = TenantId::new(tenant)?;
    let kv = &*state.kv;
    let records: Vec<TenantRecord> = control::load_tenant(kv, &tenant).await?.into_iter().collect();
    let entries = tenant_entries(&state, &tenant).await?;
    let view = status::tenants_view(
        &records,
        &entries,
        &control::load_member_reports(kv).await?,
        state.clock.now_ms(),
        state.config.member_timeout_ms,
    );
    view.tenants
        .into_iter()
        .next()
        .map(axum::Json)
        .ok_or_else(|| unknown_tenant(&tenant))
}

/// What a tenant is created with.
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TenantBody {
    /// Name shown to people (at most 100 characters, trimmed; empty means
    /// none).
    #[serde(default)]
    display_name: Option<String>,
}

/// A tenant answered with `201 Created` when this request created it, `200`
/// when it already existed as asked.
struct TenantCreated(control::TenantCreation);

impl IntoResponse for TenantCreated {
    fn into_response(self) -> Response {
        let status = match self.0 {
            control::TenantCreation::Created(_) => StatusCode::CREATED,
            control::TenantCreation::Unchanged(_) => StatusCode::OK,
        };
        (status, axum::Json(self.0.record())).into_response()
    }
}

impl OperationOutput for TenantCreated {
    fn describe(operation: &mut OperationContext<'_>) {
        let schema = operation.response_schema::<TenantRecord>();
        operation.add_response(
            "201",
            streaming_openapi::Response::json("The tenant was created.", schema.clone()),
        );
        operation.add_response(
            "200",
            streaming_openapi::Response::json(
                "The tenant already existed with this display name; nothing changed.",
                schema,
            ),
        );
    }
}

/// Create a tenant.
///
/// A tenant must exist before graphs are submitted to it. Creating it again
/// with the same display name changes nothing and answers 200, so a retry is
/// harmless; with another display name it answers 409. A tenant is never
/// overwritten.
#[route(PUT, "/api/v1/tenants/{tenant}", tag = "tenants")]
async fn put_tenant(
    State(state): State<ApiState>,
    Path(tenant): Path<String>,
    JsonBody(body): JsonBody<TenantBody>,
) -> Result<TenantCreated, ApiError> {
    let tenant = TenantId::new(tenant)?;
    let creation = control::create_tenant(&*state.kv, &*state.clock, tenant, body.display_name).await?;
    Ok(TenantCreated(creation))
}

fn unknown_tenant(tenant: &TenantId) -> ApiError {
    ApiError::new(&error::TENANT_NOT_FOUND).with("tenant", tenant.as_str())
}

/// The index entries of a tenant's graphs.
async fn tenant_entries(state: &ApiState, tenant: &TenantId) -> Result<Vec<GraphIndexEntry>, ApiError> {
    Ok(control::load_index(&*state.kv)
        .await?
        .into_iter()
        .filter(|entry| &entry.tenant == tenant)
        .collect())
}

/// The graphs of a tenant, summarized. A created tenant without graphs has
/// an empty list.
#[route(GET, "/api/v1/tenants/{tenant}/graphs", tag = "tenants")]
async fn tenant_graphs(State(state): State<ApiState>, Path(tenant): Path<String>) -> ApiResult<status::GraphsView> {
    let tenant = TenantId::new(tenant)?;
    let entries = tenant_entries(&state, &tenant).await?;
    if entries.is_empty() && control::load_tenant(&*state.kv, &tenant).await?.is_none() {
        return Err(unknown_tenant(&tenant));
    }
    let context = Context::load(&state).await?;
    let mut graphs = Vec::new();
    for entry in entries {
        let key = GraphKey::new(entry.tenant.clone(), entry.graph.clone());
        if let Some(mut detail) = detail(&state, &context, &key).await? {
            detail.orphan = entry.orphan;
            graphs.push(status::graph_summary(&detail, entry.updated_ms));
        }
    }
    Ok(axum::Json(status::GraphsView { graphs }))
}

/// The detail of a graph, or 404 when it is not in the index.
async fn existing_detail(state: &ApiState, key: &GraphKey) -> Result<GraphDetail, ApiError> {
    let unknown = || ApiError::from(ControlError::GraphNotFound(key.clone()));
    let entry = control::load_index_entry(&*state.kv, key).await?.ok_or_else(unknown)?;
    let context = Context::load(state).await?;
    let mut detail = detail(state, &context, key).await?.ok_or_else(unknown)?;
    detail.orphan = entry.orphan;
    Ok(detail)
}

/// A graph: its description, plan, nodes with their metrics, edges, slices
/// and bottleneck.
#[route(GET, "/api/v1/tenants/{tenant}/graphs/{graph}", tag = "graphs")]
async fn graph(State(state): State<ApiState>, Path((tenant, graph)): Path<(String, String)>) -> ApiResult<GraphDetail> {
    let key = graph_key(&tenant, &graph)?;
    Ok(axum::Json(existing_detail(&state, &key).await?))
}

/// A graph's description in Graphviz's DOT language.
#[route(GET, "/api/v1/tenants/{tenant}/graphs/{graph}/dot", tag = "graphs")]
async fn dot(State(state): State<ApiState>, Path((tenant, graph)): Path<(String, String)>) -> Result<Dot, ApiError> {
    let key = graph_key(&tenant, &graph)?;
    let spec = control::load_spec(&*state.kv, &key)
        .await?
        .ok_or_else(|| ControlError::GraphNotFound(key.clone()))?;
    Ok(Dot(status::dot(&spec)))
}

const DEFAULT_LIMIT: usize = 200;
const MAX_LIMIT: usize = 2000;

/// How many rows to return, at most 2000. Kept as read until the handler
/// checks it, so that a bad value is answered with its own error code.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Limit(Result<usize, String>);

impl Default for Limit {
    fn default() -> Self {
        Self(Ok(DEFAULT_LIMIT))
    }
}

impl Limit {
    fn rows(self) -> Result<usize, ApiError> {
        self.0
            .map(|limit| limit.min(MAX_LIMIT))
            .map_err(|value| ApiError::new(&error::LIMIT_INVALID).with("value", value))
    }
}

impl<'de> Deserialize<'de> for Limit {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Ok(Self(
            value.parse::<usize>().map_err(|_| value.chars().take(160).collect()),
        ))
    }
}

impl JsonSchema for Limit {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Limit".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "integer",
            "minimum": 0,
            "default": DEFAULT_LIMIT,
            "description": format!("Rows to return at most ({DEFAULT_LIMIT} by default); above {MAX_LIMIT}, {MAX_LIMIT}."),
        })
    }
}

/// The query of [`outputs`].
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct OutputsQuery {
    /// Rows to return at most (200 by default); above 2000, 2000.
    #[serde(default)]
    limit: Limit,
}

/// The rows of a `std/kv-sink@1` node.
#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct OutputsView {
    /// Rows, most recent event first, then by key.
    rows: Vec<OutputRow>,
}

/// The latest rows written by a `std/kv-sink@1` node.
///
/// Rows are sorted by event time, most recent first, then by key. A row the
/// store holds but cannot decode is skipped.
#[route(GET, "/api/v1/tenants/{tenant}/graphs/{graph}/outputs/{node}", tag = "nodes")]
async fn outputs(
    State(state): State<ApiState>,
    Path((tenant, graph, node)): Path<(String, String, String)>,
    QueryParams(OutputsQuery { limit }): QueryParams<OutputsQuery>,
) -> ApiResult<OutputsView> {
    let key = graph_key(&tenant, &graph)?;
    let node = NodeId::new(node)?;
    let limit = limit.rows()?;
    let spec = control::load_spec(&*state.kv, &key)
        .await?
        .ok_or_else(|| ControlError::GraphNotFound(key.clone()))?;
    if spec.node(&node).is_none() {
        return Err(ControlError::NodeNotFound { graph: key, node }.into());
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
    Ok(axum::Json(OutputsView { rows }))
}

/// The version of a description after a change.
#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct VersionReply {
    /// Version of the stored description.
    version: u64,
}

/// A graph being deleted.
#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct DeletingReply {
    /// Always `true`: the controller removes the state of the graph.
    deleting: bool,
}

/// Delete a graph the code no longer declares, and its state.
///
/// Graphs are declared by the code of the application. One the code no
/// longer declares is stopped with its state kept (`orphan`); this deletes
/// it for good: the controller removes its state. A graph the code declares
/// is refused (`GRAPH_DECLARED_BY_CODE`).
#[route(DELETE, "/api/v1/tenants/{tenant}/graphs/{graph}", tag = "graphs")]
async fn delete_graph(
    State(state): State<ApiState>,
    Path((tenant, graph)): Path<(String, String)>,
) -> ApiResult<DeletingReply> {
    let key = graph_key(&tenant, &graph)?;
    control::delete_graph(&*state.kv, &*state.clock, &key).await?;
    Ok(axum::Json(DeletingReply { deleting: true }))
}

/// The run state of a graph.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct StateBody {
    /// The desired run state.
    state: RunState,
}

/// Run or stop a graph.
#[route(POST, "/api/v1/tenants/{tenant}/graphs/{graph}/state", tag = "graphs")]
async fn set_state(
    State(state): State<ApiState>,
    Path((tenant, graph)): Path<(String, String)>,
    JsonBody(StateBody { state: run_state }): JsonBody<StateBody>,
) -> ApiResult<StateBody> {
    let key = graph_key(&tenant, &graph)?;
    control::set_state(&*state.kv, &*state.clock, &key, run_state).await?;
    Ok(axum::Json(StateBody { state: run_state }))
}

/// Whether a node is paused.
#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct PausedReply {
    /// The node.
    node: NodeId,
    /// Whether it is listed in `desired.paused_nodes`.
    paused: bool,
}

async fn set_paused(
    state: ApiState,
    (tenant, graph, node): (String, String, String),
    paused: bool,
) -> ApiResult<PausedReply> {
    let key = graph_key(&tenant, &graph)?;
    let node = NodeId::new(node)?;
    control::set_node_paused(&*state.kv, &*state.clock, &key, &node, paused).await?;
    Ok(axum::Json(PausedReply { node, paused }))
}

/// Pause a node: its instances stop reading and hold their state.
#[route(POST, "/api/v1/tenants/{tenant}/graphs/{graph}/nodes/{node}/pause", tag = "nodes")]
async fn pause(State(state): State<ApiState>, Path(path): Path<(String, String, String)>) -> ApiResult<PausedReply> {
    set_paused(state, path, true).await
}

/// Resume a paused node.
#[route(POST, "/api/v1/tenants/{tenant}/graphs/{graph}/nodes/{node}/resume", tag = "nodes")]
async fn resume(State(state): State<ApiState>, Path(path): Path<(String, String, String)>) -> ApiResult<PausedReply> {
    set_paused(state, path, false).await
}

/// Change some exposed fields of a node's configuration.
///
/// Graphs are built by the code of the workers; only the fields an operator
/// lists among its parameters can be changed here. Each field of the body is
/// laid over the configuration the code gave the node, `null` going back to
/// the code's value. The new description is validated and stored as a new
/// version, on which the node's instances restart. The fields are kept apart
/// from the description, so that a new version of the code keeps them.
#[route(PATCH, "/api/v1/tenants/{tenant}/graphs/{graph}/nodes/{node}/config", tag = "nodes")]
async fn patch_config(
    State(state): State<ApiState>,
    Path((tenant, graph, node)): Path<(String, String, String)>,
    JsonBody(patch): JsonBody<Map<String, Value>>,
) -> ApiResult<VersionReply> {
    let key = graph_key(&tenant, &graph)?;
    let node = NodeId::new(node)?;
    let version = control::patch_node_config(&*state.kv, &*state.clock, &key, &node, patch).await?;
    Ok(axum::Json(VersionReply { version }))
}

/// Every route of the API, described.
fn api() -> ApiRouter<ApiState> {
    ApiRouter::new()
        .route(healthz::route())
        .route(openapi_document::route())
        .route(cluster::route())
        .route(catalog::route())
        .route(tenants::route())
        .route(tenant::route())
        .route(put_tenant::route())
        .route(tenant_graphs::route())
        .route(graph::route())
        .route(delete_graph::route())
        .route(set_state::route())
        .route(dot::route())
        .route(outputs::route())
        .route(pause::route())
        .route(resume::route())
        .route(patch_config::route())
}

fn info() -> Info {
    Info::new("streaming-rs", env!("CARGO_PKG_VERSION")).description(
        "Control plane of streaming-rs: graphs, their state and their metrics. \
         Errors are `{\"message\": \"<message>\"}`. There is no authentication: \
         do not expose this API publicly.",
    )
}

/// The OpenAPI document of the API.
#[must_use]
pub fn openapi() -> OpenApi {
    api().openapi(info())
}

/// The `/healthz` and `/api/v1` routes.
pub fn routes() -> Router<ApiState> {
    let api = api();
    let document = serde_json::to_value(api.openapi(info())).expect("an OpenAPI document serializes");
    api.into_router().layer(Extension(OpenApiDocument(Arc::new(document))))
}
