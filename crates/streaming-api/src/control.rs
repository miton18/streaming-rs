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
use std::{collections::BTreeMap, time::Duration};
use streaming_core::{
    Catalog, Clock, GraphKey, GraphSpec, Kv, KvError, NodeId, NodeSpec, OpDescriptor, SpecError, TenantId, Topology,
    WorkerId,
    control::{
        Declaration, DesiredState, GraphIndexEntry, GraphPlan, MemberInfo, Placement, RunState, TenantRecord,
        WorkerSnapshot,
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
    /// The code of the workers declares the graph: it cannot be deleted
    /// through the API (HTTP 409).
    #[error("graph `{0}` is declared by the code of the application: remove it from the code to delete it")]
    GraphDeclaredByCode(GraphKey),
    /// The code of the workers no longer declares the graph: it is stopped
    /// and can only be deleted (HTTP 409).
    #[error("graph `{0}` is no longer declared by the code of the application: it can only be deleted")]
    GraphNotInCode(GraphKey),
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
    /// The operator of the node does not expose this configuration field
    /// (HTTP 400).
    #[error("node `{node}` of graph `{graph}` does not expose the configuration field `{field}`")]
    FieldNotExposed {
        /// The graph.
        graph: GraphKey,
        /// The node.
        node: NodeId,
        /// The field.
        field: String,
    },
    /// A graph belongs to a tenant that was not created (HTTP 404).
    #[error("tenant `{0}` does not exist: declare it with the graphs of its code")]
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

/// Whether `graph` is marked as being deleted. The mark decides, not the
/// index entry, which a writer that read it before the deletion may have
/// written back without it.
async fn being_deleted(kv: &dyn Kv, graph: &GraphKey) -> Result<bool, ControlError> {
    Ok(kv.hget(keys::CLUSTER_DELETING, &graph.to_string()).await?.is_some())
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
    if entry.orphan {
        return Err(ControlError::GraphNotInCode(graph.clone()));
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

/// Validate `spec` against `catalog`, then store it under the next version.
async fn store_spec(
    kv: &dyn Kv,
    clock: &dyn Clock,
    catalog: &Catalog,
    mut spec: GraphSpec,
) -> Result<u64, ControlError> {
    Topology::build(&spec, catalog)?;
    let key = spec.key();
    let entry = load_index_entry(kv, &key).await?;
    if entry.as_ref().is_some_and(|entry| entry.deleting) || being_deleted(kv, &key).await? {
        return Err(ControlError::GraphBeingDeleted(key));
    }
    let stored_version = match load_spec(kv, &key).await {
        Ok(stored) => stored.map_or(0, |stored| stored.version),
        // A broken stored description must not prevent replacing it.
        Err(ControlError::Kv(KvError::Malformed { .. })) => 0,
        Err(e) => return Err(e),
    };
    // Drawn from a counter: the store has no transaction, and two writers
    // computing "the stored version plus one" would store two descriptions
    // under one version. The plan of a graph is computed for a version, and
    // workers pair a plan with the description of that version.
    let floor = entry.map_or(0, |entry| entry.version).max(stored_version);
    let counter = keys::graph_version(&key);
    let mut version = kv.incr(&counter).await?.max(0) as u64;
    while version <= floor {
        // A description stored before there was a counter.
        version = kv.incr(&counter).await?.max(0) as u64;
    }
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
        orphan: false,
        fingerprint: spec.fingerprint.clone(),
    };
    kv.hset_json(keys::CLUSTER_GRAPHS, &key.to_string(), &entry).await?;
    bump_epoch(kv).await?;
    Ok(version)
}

/// Configuration fields set through the API, by node.
type Overrides = BTreeMap<NodeId, Map<String, Value>>;

async fn load_overrides(kv: &dyn Kv, graph: &GraphKey) -> Result<Overrides, ControlError> {
    let mut overrides = Overrides::new();
    for (node, fields) in kv
        .hgetall_json::<Map<String, Value>>(&keys::graph_overrides(graph))
        .await?
    {
        match NodeId::new(node) {
            Ok(node) => {
                overrides.insert(node, fields);
            }
            Err(error) => tracing::warn!(graph = %graph, %error, "ignoring an override of an invalid node id"),
        }
    }
    Ok(overrides)
}

/// The description as the code of the workers built it: the `code` document,
/// or, for a graph stored before there was one, its stored description.
async fn load_code(kv: &dyn Kv, graph: &GraphKey) -> Result<GraphSpec, ControlError> {
    match kv.get_json::<GraphSpec>(&keys::graph_code(graph)).await? {
        Some(code) => Ok(code),
        None => load_spec(kv, graph).await?.ok_or_else(|| not_found_graph(graph)),
    }
}

/// Whether the operator of `node` exposes `field`: whether it lists it among
/// the parameters it describes.
fn exposes(catalog: &Catalog, node: &NodeSpec, field: &str) -> bool {
    catalog
        .get(&node.op)
        .is_some_and(|descriptor| descriptor.params.iter().any(|param| param.name == field))
}

/// `code` with `overrides` laid over the configurations of its nodes. A field
/// its operator no longer exposes, or a node the code no longer has, is left
/// out and returned, as `(node, field)`.
fn lay_overrides(
    catalog: &Catalog,
    mut code: GraphSpec,
    overrides: &Overrides,
) -> Result<(GraphSpec, Vec<(NodeId, String)>), ControlError> {
    let mut ignored = Vec::new();
    let graph = code.key();
    for (node_id, fields) in overrides {
        let Some(node) = code.nodes.iter_mut().find(|node| &node.id == node_id) else {
            ignored.extend(fields.keys().map(|field| (node_id.clone(), field.clone())));
            continue;
        };
        let mut config = match std::mem::take(&mut node.config) {
            Value::Null => Map::new(),
            Value::Object(map) => map,
            // Not echoed back: a configuration may hold secrets.
            _ => {
                return Err(ControlError::ConfigNotAnObject {
                    graph,
                    node: node_id.clone(),
                });
            }
        };
        for (field, value) in fields {
            if exposes(catalog, node, field) {
                config.insert(field.clone(), value.clone());
            } else {
                ignored.push((node_id.clone(), field.clone()));
            }
        }
        node.config = Value::Object(config);
    }
    Ok((code, ignored))
}

/// Whether two descriptions are the same but for their version.
fn same_description(a: &GraphSpec, b: &GraphSpec) -> bool {
    let mut b = b.clone();
    b.version = a.version;
    *a == b
}

/// What [`publish`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Publication {
    /// Tenants created, because no worker had declared them before.
    pub created_tenants: Vec<TenantId>,
    /// Tenants already stored with another display name, kept as they are.
    pub renamed_tenants: Vec<TenantId>,
    /// Graphs whose description changed, with their new version.
    pub updated: Vec<(GraphKey, u64)>,
    /// Graphs already stored as the code describes them.
    pub unchanged: Vec<GraphKey>,
    /// Graphs the code declares again after having left it: running again,
    /// on the state they had, with their new version.
    pub restored: Vec<(GraphKey, u64)>,
    /// Graphs no longer in the code, stopped by this publication. Their
    /// state is kept.
    pub orphaned: Vec<GraphKey>,
    /// Graphs that had already left the code, still stopped with their
    /// state.
    pub orphans: Vec<GraphKey>,
    /// Graphs of the code whose previous instance is still being deleted:
    /// they are published once the deletion is over.
    pub still_deleting: Vec<GraphKey>,
    /// Graphs this code steps aside from: the store holds a declaration that
    /// replaced the code's (or the code has none), and a worker running
    /// another binary is still there. They are left as they are; the worker
    /// runs none of their slices, and publishes again once every worker runs
    /// its binary.
    pub aside: Vec<GraphKey>,
    /// Configuration fields set through the API that the code no longer
    /// exposes, as `(graph, node, field)`: they are not applied.
    pub ignored_overrides: Vec<(GraphKey, NodeId, String)>,
}

/// Who publishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publisher {
    /// The workers of the publishing process. What the previous run of one
    /// of them advertised does not count as another binary being there.
    pub workers: Vec<WorkerId>,
    /// Silence after which a worker is taken for gone.
    pub member_timeout: Duration,
}

impl Default for Publisher {
    fn default() -> Self {
        Self {
            workers: Vec::new(),
            member_timeout: Duration::from_secs(5),
        }
    }
}

/// The declarations of `graph`, by number.
pub async fn load_declarations(kv: &dyn Kv, graph: &GraphKey) -> Result<BTreeMap<u64, Declaration>, ControlError> {
    let key = keys::graph_declarations(graph);
    let mut declarations = BTreeMap::new();
    for (field, declaration) in kv.hgetall_json::<Declaration>(&key).await? {
        let number = field.parse::<u64>().map_err(|_| KvError::Malformed {
            key: key.clone(),
            reason: "a declaration is not numbered".to_owned(),
        })?;
        declarations.insert(number, declaration);
    }
    Ok(declarations)
}

/// Tells whether the publishing binary is the only one running, as far as a
/// graph is concerned. The members are read once, when first needed.
struct Cluster<'a> {
    kv: &'a dyn Kv,
    clock: &'a dyn Clock,
    publisher: &'a Publisher,
    others: Option<Vec<MemberInfo>>,
}

impl Cluster<'_> {
    /// Whether every other live worker steps aside from `graph`: none of
    /// them runs what the store holds for it, so that the store can go back
    /// to a declaration it has had, or lose the graph. A worker that has
    /// just started advertises nothing yet and counts as running it.
    async fn steps_aside(&mut self, graph: &GraphKey) -> Result<bool, ControlError> {
        if self.others.is_none() {
            let now = self.clock.now_ms();
            let timeout = self.publisher.member_timeout.as_millis() as i64;
            self.others = Some(
                self.kv
                    .hgetall_json::<MemberInfo>(keys::CLUSTER_MEMBERS)
                    .await?
                    .into_iter()
                    .map(|(_, member)| member)
                    .filter(|member| member.is_alive(now, timeout) && !self.publisher.workers.contains(&member.worker))
                    .collect(),
            );
        }
        let graph = graph.to_string();
        Ok(self.others.iter().flatten().all(|member| member.aside.contains(&graph)))
    }

    /// Put `fingerprint` in force for `graph` (none: the code stopped
    /// declaring it). `false` when the code must step aside instead: the
    /// store has moved on from that declaration and another binary is still
    /// running.
    ///
    /// A declaration the graph never had is the code of a new binary, and is
    /// published at once. One it has had and left is either an older binary
    /// restarting while a deployment is in progress, or a deliberate return
    /// to an earlier code: only the second leaves no worker running what
    /// replaced it. The same goes for a graph the code does not have, which
    /// an older binary never knew and a newer one removed.
    async fn declare(&mut self, graph: &GraphKey, fingerprint: Option<&str>) -> Result<bool, ControlError> {
        let key = keys::graph_declarations(graph);
        loop {
            let declarations = load_declarations(self.kv, graph).await?;
            let latest = declarations.last_key_value();
            if latest.is_some_and(|(_, latest)| latest.fingerprint.as_deref() == fingerprint) {
                return Ok(true);
            }
            let new = fingerprint.is_some()
                && !declarations
                    .values()
                    .any(|declaration| declaration.fingerprint.as_deref() == fingerprint);
            if !new && !self.steps_aside(graph).await? {
                return Ok(false);
            }
            let number = latest.map_or(1, |(number, _)| number + 1);
            let declaration = Declaration {
                fingerprint: fingerprint.map(str::to_owned),
                since_ms: self.clock.now_ms(),
            };
            let value = streaming_core::kv::encode_json(&key, &declaration)?;
            if self.kv.hsetnx(&key, &number.to_string(), value).await? {
                return Ok(true);
            }
            // Another worker declared first: see what it declared.
        }
    }
}

/// Make the store describe what the code describes: the cluster's code is
/// the truth about tenants and graphs, the store only a copy that the
/// controller, the workers and the API read.
///
/// Every tenant of `tenants` is created if missing. Every graph of `graphs`
/// is validated against `catalog`, its description kept as the `code`
/// document, the configuration fields set through the API laid over it, and
/// the result stored as a new version when it differs from the stored one.
/// A graph the store holds but `graphs` does not becomes an *orphan*: it is
/// stopped and its state is kept, since deleting a state cannot be undone.
/// An orphan is deleted through the API ([`delete_graph`]), or runs again on
/// its state when the code declares it again.
///
/// Every worker publishes when it starts, and two binaries run side by side
/// while a deployment is in progress. Each graph therefore keeps the list of
/// its declarations, by fingerprint
/// ([`GraphSpec::declaration_fingerprint`]): a binary whose declaration the
/// store has left, or which does not declare a graph the store runs, does
/// not touch that graph while a worker of another binary is alive (see
/// [`Publication::aside`]). Concurrent publications of one binary write the
/// same documents.
pub async fn publish(
    kv: &dyn Kv,
    clock: &dyn Clock,
    catalog: &Catalog,
    tenants: &[(TenantId, Option<String>)],
    graphs: &[GraphSpec],
    publisher: &Publisher,
) -> Result<Publication, ControlError> {
    for spec in graphs {
        Topology::build(spec, catalog)?;
    }
    let mut cluster = Cluster {
        kv,
        clock,
        publisher,
        others: None,
    };
    let mut publication = Publication::default();
    for (tenant, display_name) in tenants {
        match create_tenant(kv, clock, tenant.clone(), display_name.clone()).await {
            Ok(TenantCreation::Created(_)) => publication.created_tenants.push(tenant.clone()),
            Ok(TenantCreation::Unchanged(_)) => {}
            Err(ControlError::TenantConflict(tenant)) => publication.renamed_tenants.push(tenant),
            Err(error) => return Err(error),
        }
    }
    for code in graphs {
        let key = code.key();
        if load_tenant(kv, &key.tenant).await?.is_none() {
            return Err(ControlError::TenantNotCreated(key.tenant));
        }
        let entry = load_index_entry(kv, &key).await?;
        if entry.as_ref().is_some_and(|entry| entry.deleting) || being_deleted(kv, &key).await? {
            publication.still_deleting.push(key);
            continue;
        }
        let mut code = code.clone();
        code.version = 0;
        let fingerprint = code.declaration_fingerprint();
        if !cluster.declare(&key, Some(&fingerprint)).await? {
            publication.aside.push(key);
            continue;
        }
        code.fingerprint = Some(fingerprint);
        let stored_code = match kv.get_json::<GraphSpec>(&keys::graph_code(&key)).await {
            Ok(stored) => stored,
            Err(KvError::Malformed { .. }) => None,
            Err(error) => return Err(error.into()),
        };
        if stored_code.as_ref() != Some(&code) {
            kv.set_json(&keys::graph_code(&key), &code).await?;
        }
        let (spec, ignored) = lay_overrides(catalog, code, &load_overrides(kv, &key).await?)?;
        publication
            .ignored_overrides
            .extend(ignored.into_iter().map(|(node, field)| (key.clone(), node, field)));
        let stored = match load_spec(kv, &key).await {
            Ok(stored) => stored,
            Err(ControlError::Kv(KvError::Malformed { .. })) => None,
            Err(error) => return Err(error),
        };
        if entry.as_ref().is_some_and(|entry| entry.orphan) {
            // Back in the code: run again. Storing the description clears
            // the mark and bumps the epoch, after the desired state.
            let mut desired = desired_or_default(kv, &key, clock).await?;
            desired.state = RunState::Running;
            save_desired(kv, &key, clock, desired).await?;
            let version = store_spec(kv, clock, catalog, spec).await?;
            publication.restored.push((key, version));
            continue;
        }
        if entry.is_some_and(|entry| {
            entry.fingerprint == spec.fingerprint
                && stored.as_ref().is_some_and(|stored| stored.version == entry.version)
        }) && stored.as_ref().is_some_and(|stored| same_description(stored, &spec))
        {
            publication.unchanged.push(key);
            continue;
        }
        let version = store_spec(kv, clock, catalog, spec).await?;
        publication.updated.push((key, version));
    }
    for mut entry in load_index(kv).await? {
        let key = GraphKey::new(entry.tenant.clone(), entry.graph.clone());
        if entry.deleting || graphs.iter().any(|spec| spec.key() == key) {
            continue;
        }
        if entry.orphan {
            publication.orphans.push(key);
            continue;
        }
        if being_deleted(kv, &key).await? {
            continue;
        }
        if !cluster.declare(&key, None).await? {
            publication.aside.push(key);
            continue;
        }
        let mut desired = desired_or_default(kv, &key, clock).await?;
        desired.state = RunState::Stopped;
        save_desired(kv, &key, clock, desired).await?;
        entry.orphan = true;
        entry.fingerprint = None;
        entry.updated_ms = clock.now_ms();
        kv.hset_json(keys::CLUSTER_GRAPHS, &key.to_string(), &entry).await?;
        bump_epoch(kv).await?;
        publication.orphaned.push(key);
    }
    Ok(publication)
}

/// Whether the store no longer holds what the code declares: a graph of
/// `graphs` is missing, stopped as an orphan, or stored under another
/// declaration, or the store runs a graph `graphs` does not have. The worker
/// then steps aside from that graph, and should [`publish`] again: it takes
/// over once every worker runs its binary. One read of the graph index and
/// one of each description: a description written late, by a command its
/// sender had given up on, may have replaced a newer one behind the index.
pub async fn out_of_date(kv: &dyn Kv, graphs: &[GraphSpec]) -> Result<bool, ControlError> {
    let index = load_index(kv).await?;
    for code in graphs {
        let fingerprint = Some(code.declaration_fingerprint());
        let entry = index
            .iter()
            .find(|entry| entry.tenant == code.tenant && entry.graph == code.graph);
        if !entry.is_some_and(|entry| !entry.orphan && !entry.deleting && entry.fingerprint == fingerprint) {
            return Ok(true);
        }
        let stored = match load_spec(kv, &code.key()).await {
            Ok(stored) => stored,
            Err(ControlError::Kv(KvError::Malformed { .. })) => None,
            Err(error) => return Err(error),
        };
        let version = entry.map(|entry| entry.version);
        if !stored.is_some_and(|stored| stored.fingerprint == fingerprint && Some(stored.version) == version) {
            return Ok(true);
        }
    }
    Ok(index.iter().any(|entry| {
        !entry.orphan
            && !entry.deleting
            && !graphs
                .iter()
                .any(|code| code.tenant == entry.tenant && code.graph == entry.graph)
    }))
}

/// Delete a graph the code no longer declares, and its state: a mark in
/// `cluster:deleting`, desired state `stopped`, index entry `deleting`. The
/// controller removes the rest, the mark last. A
/// graph the code declares is not deleted: it would come back, empty, with
/// the next worker that starts.
pub async fn delete_graph(kv: &dyn Kv, clock: &dyn Clock, graph: &GraphKey) -> Result<(), ControlError> {
    let mut entry = load_index_entry(kv, graph)
        .await?
        .ok_or_else(|| not_found_graph(graph))?;
    if !entry.orphan {
        return Err(ControlError::GraphDeclaredByCode(graph.clone()));
    }
    // The mark first: from here on the graph is being deleted, even if a
    // writer that read its index entry earlier writes it back.
    kv.hset(keys::CLUSTER_DELETING, vec![(graph.to_string(), b"1".to_vec())])
        .await?;
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

/// Set or clear some exposed configuration fields of `node`: each field of
/// `patch` is laid over the configuration the code gave the node, a `null`
/// value going back to the code's. Only the fields its operator describes
/// among its parameters can be set. The resulting description is validated
/// and stored as a new version, which the workers restart the node's
/// instances on. Returns the new version.
///
/// The fields are kept apart from the description, so that a new version of
/// the code keeps them.
pub async fn patch_node_config(
    kv: &dyn Kv,
    clock: &dyn Clock,
    graph: &GraphKey,
    node: &NodeId,
    patch: Map<String, Value>,
) -> Result<u64, ControlError> {
    live_entry(kv, graph).await?;
    let code = load_code(kv, graph).await?;
    let Some(code_node) = code.node(node) else {
        return Err(ControlError::NodeNotFound {
            graph: graph.clone(),
            node: node.clone(),
        });
    };
    let catalog = load_catalog(kv).await?;
    if let Some(field) = patch.keys().find(|field| !exposes(&catalog, code_node, field)) {
        return Err(ControlError::FieldNotExposed {
            graph: graph.clone(),
            node: node.clone(),
            field: field.clone(),
        });
    }
    let mut overrides = load_overrides(kv, graph).await?;
    let fields = overrides.entry(node.clone()).or_default();
    for (field, value) in patch {
        if value.is_null() {
            fields.remove(&field);
        } else {
            fields.insert(field, value);
        }
    }
    let fields = fields.clone();
    let (spec, _) = lay_overrides(&catalog, code, &overrides)?;
    Topology::build(&spec, &catalog)?;
    let field = node.to_string();
    if fields.is_empty() {
        kv.hdel(&keys::graph_overrides(graph), vec![field]).await?;
    } else {
        kv.hset_json(&keys::graph_overrides(graph), &field, &fields).await?;
    }
    store_spec(kv, clock, &catalog, spec).await
}
