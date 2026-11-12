//! KV-level control operations.
//!
//! These functions read and write the documents described in
//! [`streaming_core::keys`]. They hold no state and need no HTTP server, so
//! the CLI and the standalone bootstrap reuse them. Every successful write
//! bumps `cluster:epoch`, which is what workers watch to converge.
//!
//! Writes are not transactional (the KV contract has no multi-key atomicity):
//! the epoch is bumped last, so that whoever sees the new epoch sees the new
//! documents. Two concurrent writers on the same graph can interleave; the
//! last writer wins.

use serde_json::{Map, Value};
use streaming_core::{
    Catalog, Clock, GraphKey, GraphSpec, Kv, KvError, NodeId, OpDescriptor, SpecError, TenantId, Topology,
    control::{
        DesiredState, GraphIndexEntry, GraphPlan, MemberInfo, Placement, RunState, TenantRecord, WorkerSnapshot,
    },
    keys,
};

/// Why a control operation failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ControlError {
    /// The graph is not in the index (HTTP 404).
    #[error("unknown graph `{0}`")]
    GraphNotFound(GraphKey),
    /// The graph has no such node (HTTP 404).
    #[error("unknown node `{node}` in graph `{graph}`")]
    NodeNotFound {
        /// The graph.
        graph: GraphKey,
        /// The missing node.
        node: NodeId,
    },
    /// The graph is being deleted and cannot change any more (HTTP 400).
    #[error("graph `{0}` is being deleted")]
    GraphBeingDeleted(GraphKey),
    /// The description is invalid (HTTP 400).
    #[error(transparent)]
    InvalidGraph(#[from] SpecError),
    /// The configuration of the node is not an object, so it cannot be
    /// patched key by key (HTTP 400).
    #[error("the configuration of node `{node}` is not an object and cannot be patched")]
    ConfigNotAnObject {
        /// The graph.
        graph: GraphKey,
        /// The node.
        node: NodeId,
    },
    /// A graph was submitted to a tenant that was not created (HTTP 404).
    #[error("tenant `{0}` does not exist: create it before submitting graphs to it")]
    TenantNotCreated(TenantId),
    /// The tenant exists with another display name (HTTP 409).
    #[error("tenant `{0}` already exists with another display name")]
    TenantConflict(TenantId),
    /// The display name is longer than [`MAX_DISPLAY_NAME_CHARS`] (HTTP 400).
    #[error("the display name is longer than {MAX_DISPLAY_NAME_CHARS} characters")]
    DisplayNameTooLong,
    /// The display name holds a control character (HTTP 400).
    #[error("the display name contains a control character")]
    DisplayNameInvalidCharacter,
    /// The KV failed or holds a malformed document (HTTP 503).
    #[error("{0}")]
    Kv(#[from] KvError),
}

/// Load the operator catalog from the `cluster:catalog` hash. Undecodable
/// descriptors are skipped.
pub async fn load_catalog(kv: &dyn Kv) -> Result<Catalog, ControlError> {
    let mut catalog = Catalog::default();
    for (_, descriptor) in kv.hgetall_json::<OpDescriptor>(keys::CLUSTER_CATALOG).await? {
        catalog.insert(descriptor);
    }
    Ok(catalog)
}

/// Current value of `cluster:epoch` (0 when never bumped).
pub async fn load_epoch(kv: &dyn Kv) -> Result<i64, ControlError> {
    match kv.get(keys::CLUSTER_EPOCH).await? {
        None => Ok(0),
        Some(bytes) => std::str::from_utf8(&bytes)
            .ok()
            .and_then(|text| text.trim().parse().ok())
            .ok_or_else(|| {
                KvError::Malformed {
                    key: keys::CLUSTER_EPOCH.to_owned(),
                    reason: "not an integer".to_owned(),
                }
                .into()
            }),
    }
}

/// Longest accepted tenant display name, in characters.
pub const MAX_DISPLAY_NAME_CHARS: usize = 100;

/// Created tenants, sorted by id. Undecodable records are skipped.
pub async fn load_tenants(kv: &dyn Kv) -> Result<Vec<TenantRecord>, ControlError> {
    let mut tenants: Vec<TenantRecord> = kv
        .hgetall_json::<TenantRecord>(keys::CLUSTER_TENANTS)
        .await?
        .into_iter()
        .map(|(_, record)| record)
        .collect();
    tenants.sort_by(|a, b| a.tenant.cmp(&b.tenant));
    Ok(tenants)
}

/// One tenant's record, if it was created.
pub async fn load_tenant(kv: &dyn Kv, tenant: &TenantId) -> Result<Option<TenantRecord>, ControlError> {
    Ok(kv.hget_json(keys::CLUSTER_TENANTS, tenant.as_str()).await?)
}

/// What [`create_tenant`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantCreation {
    /// The tenant did not exist and was created.
    Created(TenantRecord),
    /// The tenant already existed with the same display name: nothing was
    /// written. This is what a retry of a creation whose answer was lost
    /// sees.
    Unchanged(TenantRecord),
}

impl TenantCreation {
    /// The stored record.
    #[must_use]
    pub fn record(&self) -> &TenantRecord {
        match self {
            Self::Created(record) | Self::Unchanged(record) => record,
        }
    }
}

fn normalize_display_name(display_name: Option<String>) -> Result<Option<String>, ControlError> {
    let Some(name) = display_name else {
        return Ok(None);
    };
    let name = name.trim();
    if name.is_empty() {
        return Ok(None);
    }
    if name.chars().count() > MAX_DISPLAY_NAME_CHARS {
        return Err(ControlError::DisplayNameTooLong);
    }
    if name.chars().any(char::is_control) {
        return Err(ControlError::DisplayNameInvalidCharacter);
    }
    Ok(Some(name.to_owned()))
}

/// Create a tenant with one `HSETNX` on `cluster:tenants`, then bump the
/// epoch. Creating a tenant that exists with the same display name changes
/// nothing (so that a retry is harmless); with another one, it is a
/// [`ControlError::TenantConflict`]. A tenant is never overwritten.
pub async fn create_tenant(
    kv: &dyn Kv,
    clock: &dyn Clock,
    tenant: TenantId,
    display_name: Option<String>,
) -> Result<TenantCreation, ControlError> {
    let display_name = normalize_display_name(display_name)?;
    let record = TenantRecord {
        tenant,
        display_name,
        created_ms: clock.now_ms(),
    };
    let encoded = streaming_core::kv::encode_json(keys::CLUSTER_TENANTS, &record)?;
    if kv
        .hsetnx(keys::CLUSTER_TENANTS, record.tenant.as_str(), encoded)
        .await?
    {
        bump_epoch(kv).await?;
        return Ok(TenantCreation::Created(record));
    }
    let existing = load_tenant(kv, &record.tenant).await?.ok_or_else(|| {
        // HSETNX refused the field, so it exists: an absent answer here is a
        // store that lost a write, not a tenant that does not exist.
        ControlError::Kv(KvError::Malformed {
            key: keys::CLUSTER_TENANTS.to_owned(),
            reason: format!("field `{}` vanished after HSETNX refused it", record.tenant),
        })
    })?;
    if existing.display_name == record.display_name {
        Ok(TenantCreation::Unchanged(existing))
    } else {
        Err(ControlError::TenantConflict(record.tenant))
    }
}

/// Entries of the `cluster:graphs` index, sorted by `(tenant, graph)`.
pub async fn load_index(kv: &dyn Kv) -> Result<Vec<GraphIndexEntry>, ControlError> {
    let mut entries: Vec<GraphIndexEntry> = kv
        .hgetall_json::<GraphIndexEntry>(keys::CLUSTER_GRAPHS)
        .await?
        .into_iter()
        .map(|(_, entry)| entry)
        .collect();
    entries.sort_by(|a, b| (&a.tenant, &a.graph).cmp(&(&b.tenant, &b.graph)));
    Ok(entries)
}

/// One graph's index entry, if it exists.
pub async fn load_index_entry(kv: &dyn Kv, graph: &GraphKey) -> Result<Option<GraphIndexEntry>, ControlError> {
    Ok(kv.hget_json(keys::CLUSTER_GRAPHS, &graph.to_string()).await?)
}

/// Stored description of a graph.
pub async fn load_spec(kv: &dyn Kv, graph: &GraphKey) -> Result<Option<GraphSpec>, ControlError> {
    Ok(kv.get_json(&keys::graph_spec(graph)).await?)
}

/// Stored plan of a graph.
pub async fn load_plan(kv: &dyn Kv, graph: &GraphKey) -> Result<Option<GraphPlan>, ControlError> {
    Ok(kv.get_json(&keys::graph_plan(graph)).await?)
}

/// Stored desired state of a graph.
pub async fn load_desired(kv: &dyn Kv, graph: &GraphKey) -> Result<Option<DesiredState>, ControlError> {
    Ok(kv.get_json(&keys::graph_desired(graph)).await?)
}

/// Current placement, if the controller published one.
pub async fn load_placement(kv: &dyn Kv) -> Result<Option<Placement>, ControlError> {
    Ok(kv.get_json(keys::CLUSTER_PLACEMENT).await?)
}

/// Every member of `cluster:members` joined with its metrics snapshot
/// (`None` when it published none or an undecodable one), sorted by worker id.
pub async fn load_member_reports(kv: &dyn Kv) -> Result<Vec<(MemberInfo, Option<WorkerSnapshot>)>, ControlError> {
    let mut members: Vec<MemberInfo> = kv
        .hgetall_json::<MemberInfo>(keys::CLUSTER_MEMBERS)
        .await?
        .into_iter()
        .map(|(_, member)| member)
        .collect();
    members.sort_by(|a, b| a.worker.cmp(&b.worker));
    let snapshots = futures::future::join_all(members.iter().map(|member| async move {
        match kv
            .get_json::<WorkerSnapshot>(&keys::worker_metrics(&member.worker))
            .await
        {
            Ok(snapshot) => Ok(snapshot),
            Err(KvError::Malformed { key, reason }) => {
                tracing::warn!(%key, %reason, "ignoring malformed worker snapshot");
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }))
    .await;
    let mut reports = Vec::with_capacity(members.len());
    for (member, snapshot) in members.into_iter().zip(snapshots) {
        reports.push((member, snapshot?));
    }
    Ok(reports)
}

async fn bump_epoch(kv: &dyn Kv) -> Result<(), ControlError> {
    kv.incr(keys::CLUSTER_EPOCH).await?;
    Ok(())
}

fn not_found_graph(graph: &GraphKey) -> ControlError {
    ControlError::GraphNotFound(graph.clone())
}

/// The index entry of a graph that exists and is not being deleted.
async fn live_entry(kv: &dyn Kv, graph: &GraphKey) -> Result<GraphIndexEntry, ControlError> {
    let entry = load_index_entry(kv, graph)
        .await?
        .ok_or_else(|| not_found_graph(graph))?;
    if entry.deleting {
        return Err(ControlError::GraphBeingDeleted(graph.clone()));
    }
    Ok(entry)
}

async fn desired_or_default(kv: &dyn Kv, graph: &GraphKey, clock: &dyn Clock) -> Result<DesiredState, ControlError> {
    Ok(load_desired(kv, graph).await?.unwrap_or_else(|| DesiredState {
        updated_ms: clock.now_ms(),
        ..DesiredState::default()
    }))
}

async fn save_desired(
    kv: &dyn Kv,
    graph: &GraphKey,
    clock: &dyn Clock,
    mut desired: DesiredState,
) -> Result<(), ControlError> {
    desired.updated_ms = clock.now_ms();
    kv.set_json(&keys::graph_desired(graph), &desired).await?;
    Ok(())
}

/// Validate `spec`, then store it as `previous version + 1`.
async fn store_spec(kv: &dyn Kv, clock: &dyn Clock, mut spec: GraphSpec) -> Result<u64, ControlError> {
    let catalog = load_catalog(kv).await?;
    Topology::build(&spec, &catalog)?;
    let key = spec.key();
    let entry = load_index_entry(kv, &key).await?;
    if entry.as_ref().is_some_and(|entry| entry.deleting) {
        return Err(ControlError::GraphBeingDeleted(key));
    }
    let stored_version = match load_spec(kv, &key).await {
        Ok(stored) => stored.map_or(0, |stored| stored.version),
        // A broken stored description must not prevent replacing it.
        Err(ControlError::Kv(KvError::Malformed { .. })) => 0,
        Err(e) => return Err(e),
    };
    let version = entry.map_or(0, |entry| entry.version).max(stored_version) + 1;
    spec.version = version;
    let now = clock.now_ms();

    kv.set_json(&keys::graph_spec(&key), &spec).await?;
    if load_desired(kv, &key).await?.is_none() {
        let desired = DesiredState {
            state: RunState::Running,
            updated_ms: now,
            ..DesiredState::default()
        };
        kv.set_json(&keys::graph_desired(&key), &desired).await?;
    }
    let entry = GraphIndexEntry {
        tenant: spec.tenant.clone(),
        graph: spec.graph.clone(),
        version,
        updated_ms: now,
        deleting: false,
    };
    kv.hset_json(keys::CLUSTER_GRAPHS, &key.to_string(), &entry).await?;
    bump_epoch(kv).await?;
    Ok(version)
}

/// Validate a description against the catalog and store it. Returns the new
/// version (`previous + 1`, 1 for a new graph). Writes the spec, the index
/// entry, the desired state (`running`) when absent, then bumps the epoch.
///
/// The tenant must have been created ([`create_tenant`]).
pub async fn apply_graph(kv: &dyn Kv, clock: &dyn Clock, spec: GraphSpec) -> Result<u64, ControlError> {
    if load_tenant(kv, &spec.tenant).await?.is_none() {
        return Err(ControlError::TenantNotCreated(spec.tenant));
    }
    store_spec(kv, clock, spec).await
}

/// Stop a graph and mark it for deletion: desired state `stopped`, index
/// entry `deleting`. The controller removes the rest.
pub async fn delete_graph(kv: &dyn Kv, clock: &dyn Clock, graph: &GraphKey) -> Result<(), ControlError> {
    let mut entry = load_index_entry(kv, graph)
        .await?
        .ok_or_else(|| not_found_graph(graph))?;
    let mut desired = desired_or_default(kv, graph, clock).await?;
    desired.state = RunState::Stopped;
    save_desired(kv, graph, clock, desired).await?;
    entry.deleting = true;
    entry.updated_ms = clock.now_ms();
    kv.hset_json(keys::CLUSTER_GRAPHS, &graph.to_string(), &entry).await?;
    bump_epoch(kv).await
}

/// Change the desired run state of a graph.
pub async fn set_state(kv: &dyn Kv, clock: &dyn Clock, graph: &GraphKey, state: RunState) -> Result<(), ControlError> {
    live_entry(kv, graph).await?;
    let mut desired = desired_or_default(kv, graph, clock).await?;
    desired.state = state;
    save_desired(kv, graph, clock, desired).await?;
    bump_epoch(kv).await
}

async fn spec_with_node(kv: &dyn Kv, graph: &GraphKey, node: &NodeId) -> Result<GraphSpec, ControlError> {
    let spec = load_spec(kv, graph).await?.ok_or_else(|| not_found_graph(graph))?;
    if spec.node(node).is_none() {
        return Err(ControlError::NodeNotFound {
            graph: graph.clone(),
            node: node.clone(),
        });
    }
    Ok(spec)
}

/// Add (`paused`) or remove the node from `desired.paused_nodes`.
pub async fn set_node_paused(
    kv: &dyn Kv,
    clock: &dyn Clock,
    graph: &GraphKey,
    node: &NodeId,
    paused: bool,
) -> Result<(), ControlError> {
    live_entry(kv, graph).await?;
    spec_with_node(kv, graph, node).await?;
    let mut desired = desired_or_default(kv, graph, clock).await?;
    if paused {
        desired.paused_nodes.insert(node.clone());
    } else {
        desired.paused_nodes.remove(node);
    }
    save_desired(kv, graph, clock, desired).await?;
    bump_epoch(kv).await
}

/// Shallow-merge `patch` into the configuration of `node` (a `null` value
/// removes the field), validate the resulting description and store it as a
/// new version, like [`apply_graph`]. Returns the new version.
pub async fn patch_node_config(
    kv: &dyn Kv,
    clock: &dyn Clock,
    graph: &GraphKey,
    node: &NodeId,
    patch: Map<String, Value>,
) -> Result<u64, ControlError> {
    live_entry(kv, graph).await?;
    let mut spec = spec_with_node(kv, graph, node).await?;
    let target = spec
        .nodes
        .iter_mut()
        .find(|candidate| &candidate.id == node)
        .ok_or_else(|| not_found_graph(graph))?;
    let mut config = match std::mem::take(&mut target.config) {
        Value::Null => Map::new(),
        Value::Object(map) => map,
        // Not echoed back: a configuration may hold secrets.
        _ => {
            return Err(ControlError::ConfigNotAnObject {
                graph: graph.clone(),
                node: node.clone(),
            });
        }
    };
    for (field, value) in patch {
        if value.is_null() {
            config.remove(&field);
        } else {
            config.insert(field, value);
        }
    }
    target.config = Value::Object(config);
    store_spec(kv, clock, spec).await
}
