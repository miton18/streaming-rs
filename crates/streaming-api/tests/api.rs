//! Integration tests: the real router over a `MemKv`.

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicI64, Ordering},
};
use streaming_api::{
    ApiConfig, ApiState,
    control::{self, ControlError, IgnoredOverride, Publication},
    router,
};
use streaming_core::{
    Clock, DynKv, GraphKey, GraphSpec, MemKv, OpDescriptor, OpKind, OpRef, ParamRefusal, ParamSpec, Registry, TypeTag,
    control::{
        ActorKind, ActorSnapshot, ActorState, DesiredState, GraphIndexEntry, GraphPlan, Load, MemberInfo, OutputRow,
        Placement, RunState, SliceSnapshot, TenantRecord, TenantUsage, WorkerSnapshot,
    },
    keys,
};
use tower::ServiceExt;

const NOW: i64 = 1_790_000_000_000;

/// An error answer follows the contract: a known `code` with its status, and
/// a `message` that `code` and `context` alone rebuild, so that a client can
/// translate it.
fn check_error_contract(method: &str, uri: &str, status: StatusCode, body: &[u8]) {
    let body: Value = serde_json::from_slice(body).unwrap();
    let at = format!("{method} {uri} answered {status} {body}");
    let code = body["code"].as_str().unwrap_or_else(|| panic!("{at}: no code"));
    let known = streaming_api::error::ERROR_CODES
        .iter()
        .find(|known| known.code == code)
        .unwrap_or_else(|| panic!("{at}: unknown code"));
    assert_eq!(known.status, status, "{at}: the status of {code} is {}", known.status);
    let context = body["context"]
        .as_object()
        .unwrap_or_else(|| panic!("{at}: no context"));
    assert_eq!(
        body["message"].as_str(),
        Some(streaming_api::error::render(known.template, context).as_str()),
        "{at}: the message is not the template of its code filled with its context"
    );
    let mut wanted = streaming_api::error::placeholders(known.template);
    wanted.sort_unstable();
    wanted.dedup();
    let mut given: Vec<&str> = context.keys().map(String::as_str).collect();
    given.sort_unstable();
    assert_eq!(
        given, wanted,
        "{at}: the context does not match the placeholders of {code}"
    );
}

/// The OpenAPI template of a concrete path (`/a/{b}` for `/a/x`).
fn template_of<'a>(document: &'a Value, path: &str) -> Option<&'a str> {
    let segments: Vec<&str> = path.split('/').collect();
    document["paths"]
        .as_object()?
        .keys()
        .map(String::as_str)
        .find(|template| {
            let parts: Vec<&str> = template.split('/').collect();
            parts.len() == segments.len()
                && parts
                    .iter()
                    .zip(&segments)
                    .all(|(part, segment)| part.starts_with('{') || part == segment)
        })
}

/// Every answer of the API matches the OpenAPI document: the operation
/// exists, the status is described, and a JSON body validates against its
/// schema.
fn check_against_openapi(method: &str, uri: &str, status: StatusCode, body: &[u8]) {
    static DOCUMENT: std::sync::LazyLock<Value> =
        std::sync::LazyLock::new(|| serde_json::to_value(streaming_api::openapi()).unwrap());
    let document = &*DOCUMENT;
    let path = uri.split('?').next().unwrap();
    if status == StatusCode::METHOD_NOT_ALLOWED {
        // Not an operation of the document, but it keeps the error contract.
        check_error_contract(method, uri, status, body);
    }
    let Some(template) = template_of(document, path) else {
        // Served by the UI fallback, which answers 404 under `/api/`.
        assert!(
            !path.starts_with("/api/") || status == StatusCode::NOT_FOUND,
            "{method} {uri} answered {status} but is not in the OpenAPI document"
        );
        return;
    };
    let Some(operation) = document["paths"][template].get(method) else {
        assert_eq!(
            status,
            StatusCode::METHOD_NOT_ALLOWED,
            "{method} {uri} is not in the OpenAPI document"
        );
        return;
    };
    let responses = &operation["responses"];
    let code = status.as_str();
    // A success is described by its own status; an error by `default`.
    let response = responses
        .get(code)
        .or_else(|| {
            (status.is_client_error() || status.is_server_error())
                .then(|| responses.get("default"))
                .flatten()
        })
        .unwrap_or_else(|| panic!("{method} {uri} answered {status}, which the OpenAPI document does not describe"));
    let Some(schema) = response["content"]["application/json"].get("schema") else {
        return;
    };
    if status.is_client_error() || status.is_server_error() {
        check_error_contract(method, uri, status, body);
    }
    let root = json!({"allOf": [schema], "components": document["components"]});
    let validator = jsonschema::draft202012::new(&root).expect("the OpenAPI schemas compile");
    let body: Value = serde_json::from_slice(body)
        .unwrap_or_else(|e| panic!("{method} {uri} answered {status} with a body that is not JSON: {e}"));
    let errors: Vec<String> = validator
        .iter_errors(&body)
        .map(|error| format!("{} at {}", error, error.instance_path()))
        .collect();
    assert!(
        errors.is_empty(),
        "{method} {uri} answered {status} with a body that does not match the OpenAPI document: {errors:#?}\n{body:#}"
    );
}

struct Env {
    kv: DynKv,
    mem: Arc<MemKv>,
    clock: Arc<AtomicI64>,
    app: Router,
    /// The graphs of the code, as the workers would publish them.
    code: Mutex<Vec<GraphSpec>>,
}

fn descriptor(op: &str, kind: OpKind) -> OpDescriptor {
    // The parameters the fake operators expose.
    let params = match op {
        "t/src@1" => vec![
            ParamSpec::new("topic", "text", "Topic."),
            ParamSpec::new("rate", "integer", "Records per second.")
                .min(1)
                .max(1000),
            ParamSpec::new("mode", "text", "How it reads.").choices(["fast", "safe"]),
            ParamSpec::new("linger", "duration", "How long it waits for a full batch.").min("10ms"),
        ],
        "t/key@1" => vec![ParamSpec::new("by", "string", "Field.")],
        _ => Vec::new(),
    };
    OpDescriptor {
        op: OpRef::new(op).unwrap(),
        kind,
        input: TypeTag::Any,
        output: TypeTag::Any,
        description: format!("fake {op}"),
        params,
        signatures: Vec::new(),
    }
}

/// What the code of the fake operators refuses beyond what their parameters
/// tell: a source does not linger in `fast` mode.
fn operators_accept(op: &OpRef, _fed: Option<&TypeTag>, config: &Value) -> Result<(), String> {
    if op.to_string() == "t/src@1" && config["mode"] == "fast" && !config["linger"].is_null() {
        return Err("`linger` only goes with the `safe` mode".to_owned());
    }
    Ok(())
}

impl Env {
    async fn new() -> Self {
        let mem = MemKv::shared();
        let kv: DynKv = mem.clone();
        for (op, kind) in [
            ("t/src@1", OpKind::Source),
            ("t/key@1", OpKind::KeyBy),
            ("t/win@1", OpKind::Window),
            ("t/count@1", OpKind::Aggregate),
            ("t/map@1", OpKind::Map),
            ("t/sink@1", OpKind::Sink),
        ] {
            kv.hset_json(keys::CLUSTER_CATALOG, op, &descriptor(op, kind))
                .await
                .unwrap();
        }
        // The tenants most tests use, created without bumping the epoch.
        for tenant in ["acme", "globex"] {
            let record = TenantRecord {
                tenant: tenant.parse().unwrap(),
                display_name: None,
                created_ms: NOW - 1,
            };
            kv.hset_json(keys::CLUSTER_TENANTS, tenant, &record).await.unwrap();
        }
        let clock = Arc::new(AtomicI64::new(NOW));
        let clock_for_state = clock.clone();
        let stats_kv = mem.clone();
        let state = ApiState {
            kv: kv.clone(),
            clock: Arc::new(move || clock_for_state.load(Ordering::SeqCst)),
            kv_stats: Some(Arc::new(move || {
                let s = stats_kv.stats();
                json!({"keys": s.keys, "bytes": s.bytes, "reads": s.reads, "writes": s.writes})
            })),
            config: ApiConfig::default(),
            accepts: Some(Arc::new(operators_accept)),
        };
        Self {
            kv: kv.clone(),
            mem,
            clock,
            app: router(state),
            code: Mutex::new(Vec::new()),
        }
    }

    fn now(&self) -> i64 { self.clock.load(Ordering::SeqCst) }

    /// Publish `graphs` as the code of the workers, the way a starting worker
    /// does.
    async fn publish(&self, graphs: Vec<GraphSpec>) -> Result<Publication, ControlError> {
        let catalog = control::load_catalog(self.kv.as_ref()).await?;
        let clock = self.clock.clone();
        let clock = move || clock.load(Ordering::SeqCst);
        *self.code.lock().unwrap() = graphs.clone();
        control::publish(
            self.kv.as_ref(),
            &clock as &dyn Clock,
            &catalog,
            &[],
            &graphs,
            &control::Publisher::default(),
        )
        .await
    }

    /// Add a graph to the code and publish it; returns its version.
    async fn put_spec(&self, spec: GraphSpec) -> Publication {
        let mut graphs = self.code.lock().unwrap().clone();
        graphs.retain(|known| known.key() != spec.key());
        graphs.push(spec);
        self.publish(graphs).await.unwrap()
    }

    async fn call(&self, method: Method, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
        let (status, _, bytes) = self.raw(method, uri, body).await;
        let json = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
        (status, json)
    }

    async fn raw(
        &self,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
        let method_name = method.as_str().to_lowercase();
        let mut request = Request::builder().method(method).uri(uri);
        let body = match body {
            Some(value) => {
                request = request.header(header::CONTENT_TYPE, "application/json");
                Body::from(value.to_string())
            }
            None => Body::empty(),
        };
        let response = self.app.clone().oneshot(request.body(body).unwrap()).await.unwrap();
        let (parts, body) = response.into_parts();
        let bytes = body.collect().await.unwrap().to_bytes().to_vec();
        check_against_openapi(&method_name, uri, parts.status, &bytes);
        (parts.status, parts.headers, bytes)
    }

    async fn get(&self, uri: &str) -> (StatusCode, Value) { self.call(Method::GET, uri, None).await }

    async fn epoch(&self) -> i64 {
        self.kv
            .get(keys::CLUSTER_EPOCH)
            .await
            .unwrap()
            .map_or(0, |b| String::from_utf8(b).unwrap().parse().unwrap())
    }

    /// Add the graph `acme/<name>` of [`graph_body`] to the code.
    async fn put_graph(&self, name: &str) -> Publication { self.put_spec(spec(graph_body(name))).await }
}

fn spec(body: Value) -> GraphSpec { serde_json::from_value(body).unwrap() }

fn graph_body(name: &str) -> Value {
    json!({
        "tenant": "acme",
        "graph": name,
        "nodes": [
            {"id": "events", "op": "t/src@1", "config": {"topic": "events", "rate": 10}},
            {"id": "key", "op": "t/key@1"},
            {"id": "win", "op": "t/win@1"},
            {"id": "count", "op": "t/count@1"},
            {"id": "out", "op": "t/sink@1"}
        ],
        "edges": [["events", "key"], ["key", "win"], ["win", "count"], ["count", "out"]]
    })
}

fn gk(graph: &str) -> GraphKey { GraphKey::new("acme".parse().unwrap(), graph.parse().unwrap()) }

async fn publish_worker(env: &Env, worker: &str, heartbeat_ms: i64, snapshot: Option<WorkerSnapshot>) {
    let member = MemberInfo {
        worker: worker.parse().unwrap(),
        generation: 7,
        heartbeat_ms,
        load: Load {
            cpu: 0.31,
            memory: 0.12,
            memory_bytes: 52_428_800,
        },
        actors: 14,
        slices: 3,
        api_addr: Some("127.0.0.1:8080".into()),
        version: "0.1.0".into(),
        aside: Default::default(),
    };
    env.kv.hset_json(keys::CLUSTER_MEMBERS, worker, &member).await.unwrap();
    if let Some(snapshot) = snapshot {
        env.kv
            .set_json(&keys::worker_metrics(&worker.parse().unwrap()), &snapshot)
            .await
            .unwrap();
    }
}

fn actor(graph: &str, node: &str, kind: ActorKind, busy: f64, backpressured: f64) -> ActorSnapshot {
    ActorSnapshot {
        tenant: "acme".parse().unwrap(),
        graph: graph.parse().unwrap(),
        slice: "src.events.0".parse().unwrap(),
        node: node.parse().unwrap(),
        kind,
        op: "t/x@1".parse().unwrap(),
        state: ActorState::Running,
        records_in: 1000,
        records_out: 900,
        in_per_sec: 100.0,
        out_per_sec: 90.0,
        busy,
        idle: 0.0,
        backpressured,
        waiting: 0.0,
        queue_len: 4,
        queue_cap: 8,
        late_dropped: 0,
        errors: 0,
        last_error: None,
    }
}

fn worker_snapshot(worker: &str, graph: &str) -> WorkerSnapshot {
    WorkerSnapshot {
        worker: worker.parse().unwrap(),
        at_ms: NOW,
        interval_ms: 1000,
        load: Load::default(),
        actors: vec![
            actor(graph, "events", ActorKind::Source, 0.1, 0.8),
            actor(graph, "count", ActorKind::Aggregate, 0.97, 0.0),
        ],
        slices: vec![SliceSnapshot {
            tenant: "acme".parse().unwrap(),
            graph: graph.parse().unwrap(),
            slice: "src.events.0".parse().unwrap(),
            state: ActorState::Running,
            epoch: 3,
            position: None,
            window: None,
            watermark_ms: Some(NOW - 1000),
            commits: 3,
            commit_ms: 2.0,
            in_flight_epochs: 1,
            fenced: 0,
            restarts: 0,
            throttled: 0,
            last_error: None,
        }],
        tenants: [(
            "acme".parse().unwrap(),
            TenantUsage {
                actors: 7,
                in_per_sec: 1200.0,
                busy: 0.4,
            },
        )]
        .into(),
    }
}

#[tokio::test]
async fn a_method_a_path_does_not_take_answers_405_with_the_error_body() {
    let env = Env::new().await;
    let (status, headers, body) = env.raw(Method::POST, "/api/v1/cluster", None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["code"], "METHOD_NOT_ALLOWED");
    assert_eq!(
        body["context"],
        json!({"method": "POST", "path": "/api/v1/cluster", "allowed": ["GET", "HEAD"]})
    );
    assert_eq!(
        body["message"],
        "`/api/v1/cluster` does not take the method POST, only GET, HEAD"
    );
    assert_eq!(headers[header::ALLOW], "GET,HEAD");
    // The UI's paths only take reads.
    let (status, headers, body) = env.raw(Method::DELETE, "/assets/js/app.js", None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers[header::ALLOW], "GET,HEAD");
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["context"]["allowed"], json!(["GET", "HEAD"]));
}

#[tokio::test]
async fn healthz() {
    let env = Env::new().await;
    let (status, _, body) = env.raw(Method::GET, "/healthz", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"ok");
}

#[tokio::test]
async fn cluster_lists_workers_placement_and_kv_stats() {
    let env = Env::new().await;
    publish_worker(&env, "worker-a", NOW, Some(worker_snapshot("worker-a", "g"))).await;
    publish_worker(&env, "worker-b", NOW - 5001, None).await;
    let placement = Placement {
        version: 3,
        computed_by: Some("worker-a".parse().unwrap()),
        computed_ms: NOW,
        assignments: [("acme/g/src.events.0".parse().unwrap(), "worker-a".parse().unwrap())].into(),
    };
    env.kv.set_json(keys::CLUSTER_PLACEMENT, &placement).await.unwrap();
    env.put_graph("g").await;

    let (status, body) = env.get("/api/v1/cluster").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["epoch"], 1);
    assert_eq!(body["nowMs"], NOW);
    let workers = body["workers"].as_array().unwrap();
    assert_eq!(workers.len(), 2);
    assert_eq!(workers[0]["worker"], "worker-a");
    assert_eq!(workers[0]["alive"], true);
    assert_eq!(workers[0]["generation"], 7);
    assert_eq!(workers[0]["load"]["memoryBytes"], 52_428_800);
    assert_eq!(workers[0]["apiAddr"], "127.0.0.1:8080");
    assert_eq!(workers[0]["tenants"]["acme"]["actors"], 7);
    assert_eq!(workers[1]["alive"], false);
    assert_eq!(workers[1]["tenants"], json!({}));
    assert_eq!(body["placement"]["version"], 3);
    assert_eq!(body["placement"]["assignments"]["acme/g/src.events.0"], "worker-a");
    assert!(body["kv"]["keys"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn cluster_on_an_empty_kv() {
    let env = Env::new().await;
    let (status, body) = env.get("/api/v1/cluster").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["epoch"], 0);
    assert_eq!(body["workers"], json!([]));
    assert_eq!(body["placement"]["version"], 0);
}

#[tokio::test]
async fn catalog_is_sorted_by_reference() {
    let env = Env::new().await;
    let real = Registry::<moonpool_core::TokioProviders>::standard().catalog();
    for descriptor in real.ops.values() {
        env.kv
            .hset_json(keys::CLUSTER_CATALOG, descriptor.op.as_str(), descriptor)
            .await
            .unwrap();
    }
    let (status, body) = env.get("/api/v1/catalog").await;
    assert_eq!(status, StatusCode::OK);
    let ops: Vec<&str> = body["ops"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["op"].as_str().unwrap())
        .collect();
    assert_eq!(ops.len(), real.ops.len() + 6);
    let mut sorted = ops.clone();
    sorted.sort_unstable();
    assert_eq!(ops, sorted);
    assert!(ops.contains(&"std/count@1"));
    let count = body["ops"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["op"] == "std/count@1")
        .unwrap();
    assert_eq!(count["kind"], "aggregate");
    assert!(count["output"].is_string());
}

#[tokio::test]
async fn publishing_stores_what_changed_and_bumps_the_epoch() {
    let env = Env::new().await;
    assert_eq!(env.epoch().await, 0);
    let publication = env.put_graph("g").await;
    assert_eq!(publication.updated, [(gk("g"), 1)]);
    assert_eq!(env.epoch().await, 1);

    // Everything the contract says was written.
    let stored: Value =
        serde_json::from_slice(&env.kv.get(&keys::graph_spec(&gk("g"))).await.unwrap().unwrap()).unwrap();
    assert_eq!(stored["version"], 1);
    let code: Value = serde_json::from_slice(&env.kv.get(&keys::graph_code(&gk("g"))).await.unwrap().unwrap()).unwrap();
    assert_eq!(code["nodes"], stored["nodes"]);
    let desired: DesiredState = env.kv.get_json(&keys::graph_desired(&gk("g"))).await.unwrap().unwrap();
    assert_eq!(desired.state, RunState::Running);
    assert_eq!(desired.updated_ms, NOW);
    let entry: GraphIndexEntry = env.kv.hget_json(keys::CLUSTER_GRAPHS, "acme/g").await.unwrap().unwrap();
    assert_eq!((entry.version, entry.deleting), (1, false));

    // A worker starting with the same code changes nothing.
    let publication = env.put_graph("g").await;
    assert_eq!(publication.unchanged, [gk("g")]);
    assert!(publication.updated.is_empty());
    assert_eq!(env.epoch().await, 1);

    // An existing desired state is kept by a new version of the code.
    env.call(
        Method::POST,
        "/api/v1/tenants/acme/graphs/g/state",
        Some(json!({"state": "paused"})),
    )
    .await;
    env.clock.store(NOW + 10, Ordering::SeqCst);
    let mut changed = graph_body("g");
    changed["nodes"][0]["config"]["rate"] = json!(20);
    let publication = env.put_spec(spec(changed)).await;
    assert_eq!(publication.updated, [(gk("g"), 2)]);
    let desired: DesiredState = env.kv.get_json(&keys::graph_desired(&gk("g"))).await.unwrap().unwrap();
    assert_eq!(desired.state, RunState::Paused);
    let entry: GraphIndexEntry = env.kv.hget_json(keys::CLUSTER_GRAPHS, "acme/g").await.unwrap().unwrap();
    assert_eq!((entry.version, entry.updated_ms), (2, NOW + 10));
    assert_eq!(env.epoch().await, 3);
}

#[tokio::test]
async fn graphs_cannot_be_written_through_the_api() {
    let env = Env::new().await;
    env.put_graph("g").await;
    let uri = "/api/v1/tenants/acme/graphs/g";
    let (status, answer) = env.call(Method::PUT, uri, Some(graph_body("g"))).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(answer["code"], "METHOD_NOT_ALLOWED");
    // A graph of the code is not deleted: it would come back empty.
    let (status, answer) = env.call(Method::DELETE, uri, None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(answer["code"], "GRAPH_DECLARED_BY_CODE");
    assert_eq!(answer["context"], json!({"tenant": "acme", "graph": "g"}));
    let entry: GraphIndexEntry = env.kv.hget_json(keys::CLUSTER_GRAPHS, "acme/g").await.unwrap().unwrap();
    assert!(!entry.deleting);
    assert_eq!(env.epoch().await, 1);
}

#[tokio::test]
async fn publishing_refuses_a_graph_its_operators_do_not_fit() {
    let env = Env::new().await;
    // Unknown operator.
    let mut body = graph_body("g");
    body["nodes"][0]["op"] = json!("t/nope@1");
    let error = env.publish(vec![spec(body)]).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "node `events` references unknown operator `t/nope@1`"
    );

    // Wiring error.
    let mut body = graph_body("g");
    body["edges"] = json!([["events", "key"]]);
    let error = env.publish(vec![spec(body)]).await.unwrap_err();
    assert!(matches!(error, ControlError::InvalidGraph(_)), "{error}");

    // Nothing was written, even for the valid graphs published with them.
    let mut body = graph_body("h");
    body["nodes"][0]["op"] = json!("t/nope@1");
    env.publish(vec![spec(graph_body("g")), spec(body)]).await.unwrap_err();
    assert_eq!(env.epoch().await, 0);
    assert!(
        env.mem
            .keys()
            .iter()
            .all(|k| k.starts_with("cluster:catalog") || k == keys::CLUSTER_TENANTS)
    );
}

#[tokio::test]
async fn publishing_creates_the_declared_tenants() {
    let env = Env::new().await;
    let clock = move || NOW;
    let catalog = control::load_catalog(env.kv.as_ref()).await.unwrap();
    let mut body = graph_body("g");
    body["tenant"] = json!("initech");
    let graph = spec(body);
    // A graph of a tenant the code does not declare.
    let error = control::publish(
        env.kv.as_ref(),
        &clock,
        &catalog,
        &[],
        std::slice::from_ref(&graph),
        &control::Publisher::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error, ControlError::TenantNotCreated("initech".parse().unwrap()));
    assert!(env.kv.hget(keys::CLUSTER_GRAPHS, "initech/g").await.unwrap().is_none());

    let tenants = [("initech".parse().unwrap(), Some("Initech".to_owned()))];
    let publication = control::publish(
        env.kv.as_ref(),
        &clock,
        &catalog,
        &tenants,
        std::slice::from_ref(&graph),
        &control::Publisher::default(),
    )
    .await
    .unwrap();
    assert_eq!(publication.created_tenants, ["initech".parse().unwrap()]);
    let (_, body) = env.get("/api/v1/tenants/initech").await;
    assert_eq!(body["displayName"], "Initech");
    // Another display name in the code keeps the stored one.
    let renamed = [("initech".parse().unwrap(), Some("Initech Ltd".to_owned()))];
    let publication = control::publish(
        env.kv.as_ref(),
        &clock,
        &catalog,
        &renamed,
        &[graph],
        &control::Publisher::default(),
    )
    .await
    .unwrap();
    assert_eq!(publication.renamed_tenants, ["initech".parse().unwrap()]);
    let (_, body) = env.get("/api/v1/tenants/initech").await;
    assert_eq!(body["displayName"], "Initech");
}

#[tokio::test]
async fn tenants_are_created_once_and_retries_are_harmless() {
    let env = Env::new().await;
    let uri = "/api/v1/tenants/initech";
    let (status, body) = env
        .call(Method::PUT, uri, Some(json!({"displayName": "  Initech  "})))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        body,
        json!({"tenant": "initech", "displayName": "Initech", "createdMs": NOW})
    );
    assert_eq!(env.epoch().await, 1);
    let record: TenantRecord = env
        .kv
        .hget_json(keys::CLUSTER_TENANTS, "initech")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.display_name.as_deref(), Some("Initech"));

    // A retry changes nothing, not even the creation time.
    env.clock.store(NOW + 10, Ordering::SeqCst);
    let (status, again) = env
        .call(Method::PUT, uri, Some(json!({"displayName": "Initech"})))
        .await;
    assert_eq!((status, &again), (StatusCode::OK, &body));
    assert_eq!(env.epoch().await, 1, "an unchanged tenant does not bump the epoch");

    // Another display name is a conflict; the tenant is never overwritten.
    let (status, answer) = env.call(Method::PUT, uri, Some(json!({"displayName": "Other"}))).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(answer["code"], "TENANT_DISPLAY_NAME_CONFLICT");
    assert_eq!(answer["context"], json!({"tenant": "initech"}));
    let (status, answer) = env.call(Method::PUT, uri, Some(json!({}))).await;
    assert_eq!(status, StatusCode::CONFLICT, "{answer}");
    let stored: TenantRecord = env
        .kv
        .hget_json(keys::CLUSTER_TENANTS, "initech")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, record);

    // A created tenant without graphs is listed and readable.
    let (status, body) = env.get(uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({"tenant": "initech", "displayName": "Initech", "createdMs": NOW, "graphs": 0, "actors": 0, "inPerSec": 0.0})
    );
    let (status, body) = env.get("/api/v1/tenants/initech/graphs").await;
    assert_eq!((status, body), (StatusCode::OK, json!({"graphs": []})));
    let (_, body) = env.get("/api/v1/tenants").await;
    let ids: Vec<&str> = body["tenants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["tenant"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["acme", "globex", "initech"]);

    // Without a display name.
    let (status, body) = env.call(Method::PUT, "/api/v1/tenants/hooli", Some(json!({}))).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body, json!({"tenant": "hooli", "createdMs": NOW + 10}));
}

#[tokio::test]
async fn tenant_creation_rejects_invalid_input() {
    let env = Env::new().await;
    for (uri, body, needle) in [
        ("/api/v1/tenants/bad.id", json!({}), "IDENTIFIER_INVALID_CHARACTER"),
        (
            "/api/v1/tenants/ok",
            json!({"displayName": "a".repeat(101)}),
            "DISPLAY_NAME_TOO_LONG",
        ),
        (
            "/api/v1/tenants/ok",
            json!({"displayName": "a\nb"}),
            "DISPLAY_NAME_INVALID_CHARACTER",
        ),
        ("/api/v1/tenants/ok", json!({"name": "x"}), "BODY_UNKNOWN_FIELD"),
    ] {
        let (status, answer) = env.call(Method::PUT, uri, Some(body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {answer}");
        assert_eq!(answer["code"], needle, "{uri}: {answer}");
    }
    let (status, answer) = env.get("/api/v1/tenants/ok").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(answer["code"], "TENANT_NOT_FOUND");
    assert_eq!(env.epoch().await, 0);
}

#[tokio::test]
async fn graph_detail_joins_snapshots_and_computes_the_bottleneck() {
    let env = Env::new().await;
    env.put_graph("g").await;
    publish_worker(&env, "worker-a", NOW, Some(worker_snapshot("worker-a", "g"))).await;
    // A dead worker's snapshot must not count.
    publish_worker(&env, "worker-b", NOW - 60_000, Some(worker_snapshot("worker-b", "g"))).await;
    env.kv
        .set_json(
            keys::CLUSTER_PLACEMENT,
            &Placement {
                version: 1,
                computed_by: None,
                computed_ms: NOW,
                assignments: [("acme/g/src.events.0".parse().unwrap(), "worker-a".parse().unwrap())].into(),
            },
        )
        .await
        .unwrap();

    let (status, body) = env.get("/api/v1/tenants/acme/graphs/g").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["tenant"], "acme");
    assert_eq!(body["version"], 1);
    assert_eq!(body["state"], "running");
    assert_eq!(body["plan"], Value::Null);
    assert_eq!(body["spec"]["nodes"][0]["config"]["topic"], "events");
    assert_eq!(body["bottleneck"]["node"], "count");
    assert_eq!(body["bottleneck"]["kind"], "aggregate");
    assert_eq!(body["watermarkMs"], NOW - 1000);
    let nodes = body["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 5);
    let count = &nodes[3];
    assert_eq!(count["id"], "count");
    assert_eq!(count["bottleneck"], true);
    assert_eq!(count["actors"], 1);
    assert_eq!(count["workers"], json!(["worker-a"]));
    assert_eq!(count["busy"], 0.97);
    assert_eq!(count["state"], "running");
    assert_eq!(count["instances"][0]["worker"], "worker-a");
    assert_eq!(count["instances"][0]["kind"], "aggregate");
    assert_eq!(count["instances"][0]["slice"], "src.events.0");
    assert_eq!(nodes[1]["state"], "pending");
    assert_eq!(nodes[0]["kind"], "source");
    assert_eq!(nodes[0]["paused"], false);
    assert_eq!(body["edges"][2], json!({"from": "win", "to": "count", "fill": 0.5}));
    let slices = body["slices"].as_array().unwrap();
    assert_eq!(slices.len(), 1);
    assert_eq!(slices[0]["worker"], "worker-a");
    assert_eq!(slices[0]["assignedTo"], "worker-a");
    assert_eq!(slices[0]["slice"], "src.events.0");
    assert_eq!(slices[0]["epoch"], 3);
    assert_eq!(body["desired"]["state"], "running");
}

#[tokio::test]
async fn graph_list_and_tenants() {
    let env = Env::new().await;
    env.put_graph("g").await;
    env.put_graph("h").await;
    env.put_spec(spec({
        let mut b = graph_body("x");
        b["tenant"] = json!("globex");
        b
    }))
    .await;
    publish_worker(&env, "worker-a", NOW, Some(worker_snapshot("worker-a", "g"))).await;

    let (status, body) = env.get("/api/v1/tenants").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({"tenants": [
            {"tenant": "acme", "createdMs": NOW - 1, "graphs": 2, "actors": 7, "inPerSec": 1200.0},
            {"tenant": "globex", "createdMs": NOW - 1, "graphs": 1, "actors": 0, "inPerSec": 0.0}
        ]})
    );

    let (status, body) = env.get("/api/v1/tenants/acme/graphs").await;
    assert_eq!(status, StatusCode::OK);
    let graphs = body["graphs"].as_array().unwrap();
    assert_eq!(graphs.len(), 2);
    let g = &graphs[0];
    assert_eq!(g["graph"], "g");
    assert_eq!(g["nodes"], 5);
    assert_eq!(g["actors"], 2);
    assert_eq!(g["slices"], 1);
    assert_eq!(g["state"], "running");
    assert_eq!(g["inPerSec"], 90.0);
    assert_eq!(g["outPerSec"], 0.0);
    assert_eq!(g["bottleneck"]["node"], "count");
    assert_eq!(g["updatedMs"], NOW);
    assert_eq!(g["error"], Value::Null);
    assert_eq!(graphs[1]["graph"], "h");
    assert_eq!(graphs[1]["bottleneck"], Value::Null);
    assert_eq!(graphs[1]["watermarkMs"], Value::Null);
}

#[tokio::test]
async fn unknown_resources_are_404_and_bad_ids_400() {
    let env = Env::new().await;
    env.put_graph("g").await;
    for uri in [
        "/api/v1/tenants/nobody",
        "/api/v1/tenants/nobody/graphs",
        "/api/v1/tenants/acme/graphs/nope",
        "/api/v1/tenants/acme/graphs/nope/dot",
        "/api/v1/tenants/acme/graphs/nope/outputs/out",
        "/api/v1/tenants/acme/graphs/g/outputs/nope",
        "/api/v1/nonsense",
    ] {
        let (status, body) = env.get(uri).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        assert!(body["message"].is_string(), "{uri}: {body}");
    }
    for (method, uri) in [
        (Method::POST, "/api/v1/tenants/acme/graphs/nope/state"),
        (Method::POST, "/api/v1/tenants/acme/graphs/nope/nodes/out/pause"),
        (Method::PATCH, "/api/v1/tenants/acme/graphs/nope/nodes/out/config"),
        (Method::DELETE, "/api/v1/tenants/acme/graphs/nope"),
    ] {
        let body = match method {
            Method::POST if uri.ends_with("state") => json!({"state": "paused"}),
            Method::PATCH => json!({"a": 1}),
            _ => Value::Null,
        };
        let body = if body.is_null() { None } else { Some(body) };
        let (status, _) = env.call(method, uri, body).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
    }
    for uri in [
        "/api/v1/tenants/bad.id/graphs",
        "/api/v1/tenants/acme/graphs/bad.id",
        "/api/v1/tenants/acme/graphs/g/outputs/bad.node",
    ] {
        let (status, body) = env.get(uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert!(body["message"].is_string());
    }
    assert_eq!(env.epoch().await, 1, "failed writes do not bump the epoch");
}

#[tokio::test]
async fn patch_config_sets_exposed_fields_over_the_code() {
    let env = Env::new().await;
    env.put_graph("g").await;
    let config = "/api/v1/tenants/acme/graphs/g/nodes/events/config";
    let (status, body) = env.call(Method::PATCH, config, Some(json!({"rate": 50}))).await;
    assert_eq!((status, body), (StatusCode::OK, json!({"version": 2})));
    let (_, detail) = env.get("/api/v1/tenants/acme/graphs/g").await;
    assert_eq!(detail["version"], 2);
    assert_eq!(
        detail["spec"]["nodes"][0]["config"],
        json!({"topic": "events", "rate": 50})
    );
    // Other nodes untouched.
    assert_eq!(detail["spec"]["nodes"][1]["id"], "key");
    assert_eq!(env.epoch().await, 2);
    // The code's description is kept apart.
    let code: GraphSpec = env.kv.get_json(&keys::graph_code(&gk("g"))).await.unwrap().unwrap();
    assert_eq!(code.nodes[0].config, json!({"topic": "events", "rate": 10}));

    // A node without configuration gets an object.
    let (status, _) = env
        .call(
            Method::PATCH,
            "/api/v1/tenants/acme/graphs/g/nodes/key/config",
            Some(json!({"by": "user"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (_, detail) = env.get("/api/v1/tenants/acme/graphs/g").await;
    assert_eq!(detail["spec"]["nodes"][1]["config"], json!({"by": "user"}));
    assert_eq!(detail["version"], 3);

    // A new version of the code keeps what was set through the API, and a
    // worker restarting with the same code changes nothing.
    let mut changed = graph_body("g");
    changed["nodes"][0]["config"]["topic"] = json!("other");
    let publication = env.put_spec(spec(changed)).await;
    assert_eq!(publication.updated, [(gk("g"), 4)]);
    let (_, detail) = env.get("/api/v1/tenants/acme/graphs/g").await;
    assert_eq!(
        detail["spec"]["nodes"][0]["config"],
        json!({"topic": "other", "rate": 50})
    );
    assert_eq!(detail["spec"]["nodes"][1]["config"], json!({"by": "user"}));
    let epoch = env.epoch().await;
    let mut changed = graph_body("g");
    changed["nodes"][0]["config"]["topic"] = json!("other");
    assert_eq!(env.put_spec(spec(changed)).await.unchanged, [gk("g")]);
    assert_eq!(env.epoch().await, epoch);

    // `null` goes back to the code's value.
    let (status, _) = env.call(Method::PATCH, config, Some(json!({"rate": null}))).await;
    assert_eq!(status, StatusCode::OK);
    let (_, detail) = env.get("/api/v1/tenants/acme/graphs/g").await;
    assert_eq!(
        detail["spec"]["nodes"][0]["config"],
        json!({"topic": "other", "rate": 10})
    );
    let overrides = env
        .kv
        .hgetall_json::<Value>(&keys::graph_overrides(&gk("g")))
        .await
        .unwrap();
    assert_eq!(overrides, [("key".to_owned(), json!({"by": "user"}))]);
    let epoch = env.epoch().await;

    // Only the fields the operator exposes.
    let (status, body) = env
        .call(Method::PATCH, config, Some(json!({"rate": 1, "new": 2})))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "NODE_FIELD_NOT_EXPOSED");
    assert_eq!(
        body["context"],
        json!({"tenant": "acme", "graph": "g", "node": "events", "field": "new"})
    );
    // Errors.
    let (status, _) = env
        .call(
            Method::PATCH,
            "/api/v1/tenants/acme/graphs/g/nodes/ghost/config",
            Some(json!({"a": 1})),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, body) = env.call(Method::PATCH, config, Some(json!([1, 2]))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["message"].is_string());
    let (status, body) = env.call(Method::PATCH, config, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "BODY_NOT_JSON");
    assert_eq!(body["context"], json!({"line": 1, "column": 0}));
    assert_eq!(env.epoch().await, epoch);
}

#[tokio::test]
async fn fields_the_code_no_longer_exposes_are_not_applied() {
    let env = Env::new().await;
    env.put_graph("g").await;
    env.call(
        Method::PATCH,
        "/api/v1/tenants/acme/graphs/g/nodes/events/config",
        Some(json!({"rate": 50})),
    )
    .await;
    // A new version of the operator stops exposing `rate`.
    let mut src = descriptor("t/src@1", OpKind::Source);
    src.params.retain(|param| param.name != "rate");
    env.kv.hset_json(keys::CLUSTER_CATALOG, "t/src@1", &src).await.unwrap();
    let mut changed = graph_body("g");
    changed["nodes"][0]["config"]["topic"] = json!("other");
    let publication = env.put_spec(spec(changed)).await;
    assert_eq!(
        publication.ignored_overrides,
        [IgnoredOverride {
            graph: gk("g"),
            node: "events".parse().unwrap(),
            field: "rate".to_owned(),
            refusal: None,
        }]
    );
    let (_, detail) = env.get("/api/v1/tenants/acme/graphs/g").await;
    assert_eq!(
        detail["spec"]["nodes"][0]["config"],
        json!({"topic": "other", "rate": 10})
    );
}

#[tokio::test]
async fn a_field_only_takes_what_its_parameter_describes() {
    let env = Env::new().await;
    env.put_graph("g").await;
    let config = "/api/v1/tenants/acme/graphs/g/nodes/events/config";
    let epoch = env.epoch().await;
    let context = |field: &str| json!({"tenant": "acme", "graph": "g", "node": "events", "field": field});
    let with = |field: &str, key: &str, value: Value| {
        let mut context = context(field);
        context[key] = value;
        context
    };
    for (patch, code, context, message) in [
        (
            json!({"rate": "fast"}),
            "NODE_FIELD_WRONG_KIND",
            with("rate", "kind", json!("integer")),
            "the configuration field `rate` of node `events` in graph `acme/g` takes a value of kind `integer`",
        ),
        (
            json!({"rate": 2.5}),
            "NODE_FIELD_WRONG_KIND",
            with("rate", "kind", json!("integer")),
            "the configuration field `rate` of node `events` in graph `acme/g` takes a value of kind `integer`",
        ),
        (
            json!({"linger": "soon"}),
            "NODE_FIELD_WRONG_KIND",
            with("linger", "kind", json!("duration")),
            "the configuration field `linger` of node `events` in graph `acme/g` takes a value of kind `duration`",
        ),
        (
            json!({"mode": "reckless"}),
            "NODE_FIELD_NOT_A_CHOICE",
            with("mode", "choices", json!(["fast", "safe"])),
            "the configuration field `mode` of node `events` in graph `acme/g` must be one of fast, safe",
        ),
        (
            json!({"rate": 0}),
            "NODE_FIELD_BELOW_MIN",
            with("rate", "min", json!(1)),
            "the configuration field `rate` of node `events` in graph `acme/g` must be at least 1",
        ),
        (
            json!({"linger": "5ms"}),
            "NODE_FIELD_BELOW_MIN",
            with("linger", "min", json!("10ms")),
            "the configuration field `linger` of node `events` in graph `acme/g` must be at least 10ms",
        ),
        (
            // A field that is fine does not let the next one through.
            json!({"mode": "safe", "rate": 70000}),
            "NODE_FIELD_ABOVE_MAX",
            with("rate", "max", json!(1000)),
            "the configuration field `rate` of node `events` in graph `acme/g` must be at most 1000",
        ),
    ] {
        let (status, body) = env.call(Method::PATCH, config, Some(patch.clone())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{patch}");
        assert_eq!(body["code"], code, "{patch}");
        assert_eq!(body["context"], context, "{patch}");
        assert_eq!(body["message"], message, "{patch}");
    }
    // Nothing was stored, nothing changed.
    assert!(
        env.kv
            .hgetall_json::<Value>(&keys::graph_overrides(&gk("g")))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(env.epoch().await, epoch);

    // The bounds themselves are taken, and `null` always is.
    let (status, _) = env
        .call(
            Method::PATCH,
            config,
            Some(json!({"rate": 1000, "linger": "10ms", "mode": null})),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = env.call(Method::PATCH, config, Some(json!({"rate": 1}))).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn the_operator_has_the_last_word_on_a_configuration() {
    let env = Env::new().await;
    env.put_graph("g").await;
    let config = "/api/v1/tenants/acme/graphs/g/nodes/events/config";
    // Each field is one its parameter takes: only the operator knows they do
    // not go together.
    let (status, _) = env.call(Method::PATCH, config, Some(json!({"linger": "50ms"}))).await;
    assert_eq!(status, StatusCode::OK);
    let epoch = env.epoch().await;
    let (status, body) = env.call(Method::PATCH, config, Some(json!({"mode": "fast"}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "NODE_CONFIG_REFUSED");
    assert_eq!(
        body["context"],
        json!({
            "tenant": "acme",
            "graph": "g",
            "node": "events",
            "detail": "`linger` only goes with the `safe` mode",
        })
    );
    assert_eq!(
        body["message"],
        "the operator of node `events` in graph `acme/g` refuses this configuration: `linger` only goes with the \
         `safe` mode"
    );
    // What was stored before stays, and nothing changed.
    let overrides = env
        .kv
        .hgetall_json::<Value>(&keys::graph_overrides(&gk("g")))
        .await
        .unwrap();
    assert_eq!(overrides, [("events".to_owned(), json!({"linger": "50ms"}))]);
    assert_eq!(env.epoch().await, epoch);
    let (status, _) = env
        .call(Method::PATCH, config, Some(json!({"mode": "fast", "linger": null})))
        .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_value_its_parameter_no_longer_takes_is_not_applied() {
    let env = Env::new().await;
    env.put_graph("g").await;
    env.call(
        Method::PATCH,
        "/api/v1/tenants/acme/graphs/g/nodes/events/config",
        Some(json!({"rate": 50, "mode": "safe"})),
    )
    .await;
    // A new version of the operator takes a lower `rate` at most.
    let mut src = descriptor("t/src@1", OpKind::Source);
    for param in &mut src.params {
        if param.name == "rate" {
            param.max = Some(json!(20));
        }
    }
    env.kv.hset_json(keys::CLUSTER_CATALOG, "t/src@1", &src).await.unwrap();
    let mut changed = graph_body("g");
    changed["nodes"][0]["config"]["topic"] = json!("other");
    let publication = env.put_spec(spec(changed)).await;
    assert_eq!(
        publication.ignored_overrides,
        [IgnoredOverride {
            graph: gk("g"),
            node: "events".parse().unwrap(),
            field: "rate".to_owned(),
            refusal: Some(ParamRefusal::AboveMax(json!(20))),
        }]
    );
    // The code's value stands, and the other field is still applied.
    let (_, detail) = env.get("/api/v1/tenants/acme/graphs/g").await;
    assert_eq!(
        detail["spec"]["nodes"][0]["config"],
        json!({"topic": "other", "rate": 10, "mode": "safe"})
    );
}

#[tokio::test]
async fn patch_revalidates_against_the_catalog() {
    let env = Env::new().await;
    env.put_graph("g").await;
    // An operator disappeared from the catalog: the patched description is
    // rejected.
    env.kv
        .hdel(keys::CLUSTER_CATALOG, vec!["t/win@1".to_owned()])
        .await
        .unwrap();
    let (status, body) = env
        .call(
            Method::PATCH,
            "/api/v1/tenants/acme/graphs/g/nodes/events/config",
            Some(json!({"rate": 1})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "OPERATOR_UNKNOWN");
    assert!(
        env.kv
            .hgetall(&keys::graph_overrides(&gk("g")))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn pause_and_resume_nodes() {
    let env = Env::new().await;
    env.put_graph("g").await;
    let base = "/api/v1/tenants/acme/graphs/g/nodes";
    let (status, _) = env.call(Method::POST, &format!("{base}/count/pause"), None).await;
    assert_eq!(status, StatusCode::OK);
    let (_, detail) = env.get("/api/v1/tenants/acme/graphs/g").await;
    assert_eq!(detail["desired"]["pausedNodes"], json!(["count"]));
    assert_eq!(detail["nodes"][3]["paused"], true);
    assert_eq!(detail["nodes"][0]["paused"], false);
    // Idempotent.
    env.call(Method::POST, &format!("{base}/count/pause"), None).await;
    let (_, detail) = env.get("/api/v1/tenants/acme/graphs/g").await;
    assert_eq!(detail["desired"]["pausedNodes"], json!(["count"]));

    let (status, _) = env.call(Method::POST, &format!("{base}/count/resume"), None).await;
    assert_eq!(status, StatusCode::OK);
    let (_, detail) = env.get("/api/v1/tenants/acme/graphs/g").await;
    assert_eq!(detail["desired"]["pausedNodes"], json!([]));

    let (status, body) = env.call(Method::POST, &format!("{base}/ghost/pause"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["message"].as_str().unwrap().contains("ghost"));
    assert_eq!(env.epoch().await, 1 + 2 + 1);
}

#[tokio::test]
async fn state_changes() {
    let env = Env::new().await;
    env.put_graph("g").await;
    for state in ["paused", "draining", "stopped", "running"] {
        let (status, _) = env
            .call(
                Method::POST,
                "/api/v1/tenants/acme/graphs/g/state",
                Some(json!({"state": state})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{state}");
        let (_, detail) = env.get("/api/v1/tenants/acme/graphs/g").await;
        assert_eq!(detail["state"], state);
        assert_eq!(detail["desired"]["state"], state);
    }
    assert_eq!(env.epoch().await, 5);
    for bad in [json!({"state": "exploding"}), json!({}), json!("running")] {
        let (status, body) = env
            .call(Method::POST, "/api/v1/tenants/acme/graphs/g/state", Some(bad))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["message"].is_string());
    }
    assert_eq!(env.epoch().await, 5);
}

#[tokio::test]
async fn a_graph_the_code_no_longer_has_is_stopped_and_keeps_its_state() {
    let env = Env::new().await;
    env.put_graph("g").await;
    env.put_graph("h").await;
    let uri = "/api/v1/tenants/acme/graphs/g";
    env.call(
        Method::PATCH,
        &format!("{uri}/nodes/events/config"),
        Some(json!({"rate": 50})),
    )
    .await;
    env.clock.store(NOW + 99, Ordering::SeqCst);
    let epoch = env.epoch().await;
    let publication = env.publish(vec![spec(graph_body("h"))]).await.unwrap();
    assert_eq!(publication.orphaned, [gk("g")]);
    assert_eq!(publication.unchanged, [gk("h")]);
    let desired: DesiredState = env.kv.get_json(&keys::graph_desired(&gk("g"))).await.unwrap().unwrap();
    assert_eq!(desired.state, RunState::Stopped);
    let entry: GraphIndexEntry = env.kv.hget_json(keys::CLUSTER_GRAPHS, "acme/g").await.unwrap().unwrap();
    assert!(entry.orphan && !entry.deleting);
    assert_eq!(entry.updated_ms, env.now());
    assert_eq!(env.epoch().await, epoch + 1);
    let (_, detail) = env.get(uri).await;
    assert_eq!((&detail["orphan"], &detail["state"]), (&json!(true), &json!("stopped")));
    let (_, list) = env.get("/api/v1/tenants/acme/graphs").await;
    assert_eq!(list["graphs"][0]["orphan"], true);
    assert_eq!(list["graphs"][1]["orphan"], false);

    // Another worker with the same code says so and writes nothing.
    let publication = env.publish(vec![spec(graph_body("h"))]).await.unwrap();
    assert_eq!(publication.orphans, [gk("g")]);
    assert!(publication.orphaned.is_empty());
    assert_eq!(env.epoch().await, epoch + 1);

    // An orphan can only be deleted.
    for (method, path, body) in [
        (Method::POST, "/state", Some(json!({"state": "running"}))),
        (Method::POST, "/nodes/out/pause", None),
        (Method::PATCH, "/nodes/events/config", Some(json!({"rate": 1}))),
    ] {
        let (status, body) = env.call(method, &format!("{uri}{path}"), body).await;
        assert_eq!(status, StatusCode::CONFLICT, "{path}");
        assert_eq!(body["code"], "GRAPH_NOT_IN_CODE", "{path}");
    }
    assert_eq!(env.epoch().await, epoch + 1);

    // Back in the code: it runs again, on its state and with what was set
    // through the API.
    let publication = env.put_graph("g").await;
    assert_eq!(publication.restored, [(gk("g"), 3)]);
    let (_, detail) = env.get(uri).await;
    assert_eq!(
        (&detail["orphan"], &detail["state"]),
        (&json!(false), &json!("running"))
    );
    assert_eq!(detail["spec"]["nodes"][0]["config"]["rate"], 50);
}

#[tokio::test]
async fn deleting_an_orphan_graph_removes_it_for_good() {
    let env = Env::new().await;
    env.put_graph("g").await;
    env.publish(Vec::new()).await.unwrap();
    env.clock.store(NOW + 99, Ordering::SeqCst);
    let (status, body) = env.call(Method::DELETE, "/api/v1/tenants/acme/graphs/g", None).await;
    assert_eq!((status, body), (StatusCode::OK, json!({"deleting": true})));
    let entry: GraphIndexEntry = env.kv.hget_json(keys::CLUSTER_GRAPHS, "acme/g").await.unwrap().unwrap();
    assert!(entry.deleting);
    assert_eq!(entry.updated_ms, NOW + 99);
    assert_eq!(env.epoch().await, 3);
    // The spec is still there for the controller.
    assert!(env.kv.get(&keys::graph_spec(&gk("g"))).await.unwrap().is_some());

    // Back in the code before the deletion is over: published by a later
    // start.
    let publication = env.put_graph("g").await;
    assert_eq!(publication.still_deleting, [gk("g")]);
    let (status, body) = env
        .call(
            Method::POST,
            "/api/v1/tenants/acme/graphs/g/state",
            Some(json!({"state": "running"})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["message"].as_str().unwrap().contains("being deleted"));
}

#[tokio::test]
async fn outputs_are_sorted_and_limited() {
    let env = Env::new().await;
    env.put_graph("g").await;
    let out = keys::graph_output(&gk("g"), &"out".parse().unwrap());
    for (id, ts, key) in [
        ("a", 10, Some("zed")),
        ("b", 30, Some("b-key")),
        ("c", 30, Some("a-key")),
        ("d", 20, None),
        ("e", 5, Some("k")),
    ] {
        let row = OutputRow {
            id: id.into(),
            ts,
            key: key.map(Into::into),
            value: json!({"count": ts}),
        };
        env.kv.hset_json(&out, id, &row).await.unwrap();
    }
    // A malformed row is skipped.
    env.kv.hset(&out, vec![("junk".into(), b"{".to_vec())]).await.unwrap();

    let base = "/api/v1/tenants/acme/graphs/g/outputs/out";
    let (status, body) = env.get(base).await;
    assert_eq!(status, StatusCode::OK);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["c", "b", "d", "a", "e"]);
    assert_eq!(body["rows"][0]["value"], json!({"count": 30}));
    assert_eq!(body["rows"][2]["key"], Value::Null);

    let (_, body) = env.get(&format!("{base}?limit=2")).await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);
    assert_eq!(body["rows"][1]["id"], "b");
    let (_, body) = env.get(&format!("{base}?limit=0")).await;
    assert_eq!(body["rows"], json!([]));
    let (status, body) = env.get(&format!("{base}?limit=abc")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["message"].as_str().unwrap().contains("limit"));

    // Cap at 2000.
    let rows: Vec<(String, Vec<u8>)> = (0..2100)
        .map(|i| {
            let row = OutputRow {
                id: format!("r{i}"),
                ts: i,
                key: None,
                value: json!(i),
            };
            (format!("r{i}"), serde_json::to_vec(&row).unwrap())
        })
        .collect();
    env.kv.hset(&out, rows).await.unwrap();
    let (_, body) = env.get(&format!("{base}?limit=100000")).await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 2000);
    assert_eq!(body["rows"][0]["ts"], 2099);
    let (_, body) = env.get(base).await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 200);

    // A node without rows is an empty list.
    let (status, body) = env.get("/api/v1/tenants/acme/graphs/g/outputs/count").await;
    assert_eq!((status, body), (StatusCode::OK, json!({"rows": []})));
}

#[tokio::test]
async fn dot_is_graphviz() {
    let env = Env::new().await;
    env.put_graph("g").await;
    let (status, headers, body) = env.raw(Method::GET, "/api/v1/tenants/acme/graphs/g/dot", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        headers[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/vnd.graphviz")
    );
    let text = String::from_utf8(body).unwrap();
    assert!(text.starts_with("digraph"));
    assert!(text.contains("\"count\" [label=\"count\\nt/count@1\"];"), "{text}");
    assert!(text.contains("\"win\" -> \"count\";"), "{text}");
}

#[tokio::test]
async fn every_write_bumps_the_epoch() {
    let env = Env::new().await;
    let mut expected = 0;
    macro_rules! check {
        ($label:expr) => {{
            expected += 1;
            assert_eq!(env.epoch().await, expected, $label);
        }};
    }
    env.call(Method::PUT, "/api/v1/tenants/initech", Some(json!({}))).await;
    check!("tenant");
    env.put_graph("g").await;
    check!("publish");
    env.call(
        Method::PATCH,
        "/api/v1/tenants/acme/graphs/g/nodes/events/config",
        Some(json!({"rate": 1})),
    )
    .await;
    check!("patch");
    env.call(Method::POST, "/api/v1/tenants/acme/graphs/g/nodes/out/pause", None)
        .await;
    check!("pause");
    env.call(Method::POST, "/api/v1/tenants/acme/graphs/g/nodes/out/resume", None)
        .await;
    check!("resume");
    env.call(
        Method::POST,
        "/api/v1/tenants/acme/graphs/g/state",
        Some(json!({"state": "stopped"})),
    )
    .await;
    check!("state");
    env.publish(Vec::new()).await.unwrap();
    check!("removal from the code");
    env.call(Method::DELETE, "/api/v1/tenants/acme/graphs/g", None).await;
    check!("delete");
    // Reads do not.
    env.get("/api/v1/cluster").await;
    env.get("/api/v1/tenants").await;
    assert_eq!(env.epoch().await, expected);
}

#[tokio::test]
async fn ui_is_served_with_a_spa_fallback() {
    let env = Env::new().await;
    let (status, headers, index) = env.raw(Method::GET, "/", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers[header::CONTENT_TYPE].to_str().unwrap().starts_with("text/html"));
    assert_eq!(headers[header::CACHE_CONTROL], "no-cache");
    assert!(String::from_utf8_lossy(&index).contains("<title>"));

    let (status, headers, body) = env.raw(Method::GET, "/index.html", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, index);
    assert_eq!(headers[header::CACHE_CONTROL], "no-cache");

    // Unknown paths (hash routing, deep links) get the index.
    let (status, _, body) = env.raw(Method::GET, "/some/deep/link", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, index);
    let (status, _, body) = env.raw(Method::GET, "/assets/missing.js", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, index);

    // Under /api an unknown path is a JSON 404, never the UI.
    let (status, body) = env.get("/api/v1/whatever").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body,
        json!({
            "message": "nothing is served at `/api/v1/whatever`",
            "code": "ROUTE_NOT_FOUND",
            "context": {"path": "/api/v1/whatever"}
        })
    );
}

#[tokio::test]
async fn ui_assets_are_served_for_get_and_head_only() {
    let env = Env::new().await;
    let (status, headers, css) = env.raw(Method::GET, "/assets/app.css", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers[header::CONTENT_TYPE].to_str().unwrap().starts_with("text/css"));
    assert!(!String::from_utf8_lossy(&css).contains("<title>"));
    let (status, _, _) = env.raw(Method::HEAD, "/", None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = env.raw(Method::POST, "/", None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    // A path climbing out of the embedded files gets the index.
    let (_, _, index) = env.raw(Method::GET, "/", None).await;
    let (status, _, body) = env.raw(Method::GET, "/assets/../assets/app.css", None).await;
    assert_eq!((status, body), (StatusCode::OK, index));
}

#[tokio::test]
async fn graph_detail_shows_the_stored_plan() {
    let env = Env::new().await;
    env.put_graph("g").await;
    let plan = GraphPlan {
        spec_version: 1,
        planned_ms: NOW,
        slices: vec!["src.events.0".parse().unwrap()],
        ..GraphPlan::default()
    };
    env.kv.set_json(&keys::graph_plan(&gk("g")), &plan).await.unwrap();
    let (status, body) = env.get("/api/v1/tenants/acme/graphs/g").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["plan"]["specVersion"], 1);
    assert_eq!(body["plan"]["slices"], json!(["src.events.0"]));
}

#[tokio::test]
async fn a_state_change_keeps_the_paused_nodes() {
    let env = Env::new().await;
    env.put_graph("g").await;
    env.call(Method::POST, "/api/v1/tenants/acme/graphs/g/nodes/count/pause", None)
        .await;
    env.clock.store(NOW + 5, Ordering::SeqCst);
    env.call(
        Method::POST,
        "/api/v1/tenants/acme/graphs/g/state",
        Some(json!({"state": "paused"})),
    )
    .await;
    let desired: DesiredState = env.kv.get_json(&keys::graph_desired(&gk("g"))).await.unwrap().unwrap();
    assert_eq!(desired.state, RunState::Paused);
    assert_eq!(desired.updated_ms, NOW + 5);
    assert_eq!(
        desired.paused_nodes.iter().map(ToString::to_string).collect::<Vec<_>>(),
        ["count"]
    );
}

#[tokio::test]
async fn cors_is_permissive() {
    let env = Env::new().await;
    let request = Request::builder()
        .method(Method::OPTIONS)
        .uri("/api/v1/cluster")
        .header(header::ORIGIN, "http://localhost:5173")
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "PUT")
        .body(Body::empty())
        .unwrap();
    let response = env.app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
}

#[tokio::test]
async fn kv_failures_are_503() {
    // A key holding the wrong type makes MemKv fail like a broken store.
    let env = Env::new().await;
    env.kv.set(keys::CLUSTER_MEMBERS, b"x".to_vec()).await.unwrap();
    let (status, body) = env.get("/api/v1/cluster").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body["message"].is_string());
}

/// What the binary of a worker publishes when it starts.
async fn publish_code(env: &Env, graphs: &[GraphSpec]) -> Publication {
    let catalog = control::load_catalog(env.kv.as_ref()).await.unwrap();
    let clock = env.clock.clone();
    let clock = move || clock.load(Ordering::SeqCst);
    let publisher = control::Publisher::default();
    control::publish(env.kv.as_ref(), &clock as &dyn Clock, &catalog, &[], graphs, &publisher)
        .await
        .unwrap()
}

/// A worker heard of at `heartbeat_ms`, stepping aside from `aside`.
async fn member_aside(env: &Env, worker: &str, heartbeat_ms: i64, aside: &[&str]) {
    let member = MemberInfo {
        worker: worker.parse().unwrap(),
        generation: 1,
        heartbeat_ms,
        load: Load::default(),
        actors: 0,
        slices: 0,
        api_addr: None,
        version: String::new(),
        aside: aside.iter().map(|graph| (*graph).to_owned()).collect(),
    };
    env.kv.hset_json(keys::CLUSTER_MEMBERS, worker, &member).await.unwrap();
}

async fn fingerprints(env: &Env, graph: &str) -> Vec<Option<String>> {
    control::load_declarations(env.kv.as_ref(), &gk(graph))
        .await
        .unwrap()
        .into_values()
        .map(|declaration| declaration.fingerprint)
        .collect()
}

#[tokio::test]
async fn a_binary_the_store_has_moved_on_from_steps_aside_while_another_one_runs() {
    let env = Env::new().await;
    let old = spec(graph_body("g"));
    let mut body = graph_body("g");
    body["nodes"][0]["config"]["rate"] = json!(20);
    let new = spec(body);
    let (old_print, new_print) = (old.declaration_fingerprint(), new.declaration_fingerprint());
    assert_ne!(old_print, new_print);

    // The first binary, then the one that replaces it: a declaration the
    // graph never had is published at once, whoever else is there.
    assert_eq!(
        publish_code(&env, std::slice::from_ref(&old)).await.updated,
        [(gk("g"), 1)]
    );
    member_aside(&env, "w-old", NOW, &[]).await;
    assert_eq!(
        publish_code(&env, std::slice::from_ref(&new)).await.updated,
        [(gk("g"), 2)]
    );
    assert_eq!(
        fingerprints(&env, "g").await,
        [Some(old_print.clone()), Some(new_print.clone())]
    );
    let stored: GraphSpec = env.kv.get_json(&keys::graph_spec(&gk("g"))).await.unwrap().unwrap();
    assert_eq!(stored.fingerprint.as_ref(), Some(&new_print));
    let entry: GraphIndexEntry = env.kv.hget_json(keys::CLUSTER_GRAPHS, "acme/g").await.unwrap().unwrap();
    assert_eq!(entry.fingerprint.as_ref(), Some(&new_print));
    assert!(
        !control::out_of_date(env.kv.as_ref(), std::slice::from_ref(&new))
            .await
            .unwrap()
    );
    assert!(
        control::out_of_date(env.kv.as_ref(), std::slice::from_ref(&old))
            .await
            .unwrap()
    );

    // The old binary restarts while a worker of the new one runs the graph:
    // it writes nothing.
    member_aside(&env, "w-new", NOW, &[]).await;
    member_aside(&env, "w-old", NOW, &["acme/g"]).await;
    let (epoch, before) = (env.epoch().await, env.mem.stats().writes);
    let publication = publish_code(&env, std::slice::from_ref(&old)).await;
    assert_eq!(publication.aside, [gk("g")]);
    assert!(publication.updated.is_empty() && publication.unchanged.is_empty());
    assert_eq!((env.epoch().await, env.mem.stats().writes), (epoch, before));

    // The new binary publishing again changes nothing either.
    assert_eq!(
        publish_code(&env, std::slice::from_ref(&new)).await.unchanged,
        [gk("g")]
    );

    // Once no worker runs the new declaration any more (here: the deployment
    // was given up, its worker is silent), the old one is the code again.
    env.clock.store(NOW + 60_000, Ordering::SeqCst);
    member_aside(&env, "w-old", NOW + 60_000, &["acme/g"]).await;
    let publication = publish_code(&env, std::slice::from_ref(&old)).await;
    assert_eq!(publication.updated, [(gk("g"), 3)]);
    assert_eq!(
        fingerprints(&env, "g").await,
        [Some(old_print.clone()), Some(new_print), Some(old_print.clone())]
    );
    let stored: GraphSpec = env.kv.get_json(&keys::graph_spec(&gk("g"))).await.unwrap().unwrap();
    assert_eq!(stored.fingerprint, Some(old_print));
    assert_eq!(stored.nodes[0].config["rate"], 10);
}

#[tokio::test]
async fn going_back_to_an_earlier_declaration_waits_for_a_worker_that_says_nothing_yet() {
    // A worker that has just started advertises no graph it steps aside
    // from: it counts as running what the store holds.
    let env = Env::new().await;
    let old = spec(graph_body("g"));
    let mut body = graph_body("g");
    body["nodes"][0]["config"]["rate"] = json!(20);
    let new = spec(body);
    publish_code(&env, std::slice::from_ref(&old)).await;
    publish_code(&env, std::slice::from_ref(&new)).await;
    member_aside(&env, "w-1", NOW, &["acme/g"]).await;
    member_aside(&env, "w-2", NOW, &[]).await;
    assert_eq!(publish_code(&env, std::slice::from_ref(&old)).await.aside, [gk("g")]);
    member_aside(&env, "w-2", NOW, &["acme/other", "acme/g"]).await;
    assert_eq!(
        publish_code(&env, std::slice::from_ref(&old)).await.updated,
        [(gk("g"), 3)]
    );
}

#[tokio::test]
async fn the_previous_run_of_the_publishing_worker_is_not_another_binary() {
    let env = Env::new().await;
    let old = spec(graph_body("g"));
    let mut body = graph_body("g");
    body["nodes"][0]["config"]["rate"] = json!(20);
    let new = spec(body);
    publish_code(&env, std::slice::from_ref(&old)).await;
    publish_code(&env, std::slice::from_ref(&new)).await;
    // The only worker, restarted with the earlier code a second after it
    // last advertised itself.
    member_aside(&env, "w-1", NOW, &[]).await;
    let catalog = control::load_catalog(env.kv.as_ref()).await.unwrap();
    let clock = move || NOW + 1_000;
    let publisher = control::Publisher {
        workers: vec!["w-1".parse().unwrap()],
        ..control::Publisher::default()
    };
    let publication = control::publish(
        env.kv.as_ref(),
        &clock as &dyn Clock,
        &catalog,
        &[],
        std::slice::from_ref(&old),
        &publisher,
    )
    .await
    .unwrap();
    assert_eq!(publication.updated, [(gk("g"), 3)]);
}

#[tokio::test]
async fn a_binary_that_does_not_have_a_graph_leaves_it_alone_while_another_one_runs_it() {
    let env = Env::new().await;
    let g = spec(graph_body("g"));
    let h = spec(graph_body("h"));
    // The new binary adds `h`; a worker of it is running.
    publish_code(&env, std::slice::from_ref(&g)).await;
    publish_code(&env, &[g.clone(), h.clone()]).await;
    member_aside(&env, "w-new", NOW, &[]).await;
    // The old binary restarts: it never had `h`, and must not stop it.
    let epoch = env.epoch().await;
    let publication = publish_code(&env, std::slice::from_ref(&g)).await;
    assert_eq!(publication.aside, [gk("h")]);
    assert_eq!(publication.unchanged, [gk("g")]);
    assert!(publication.orphaned.is_empty());
    assert_eq!(env.epoch().await, epoch);
    assert!(
        control::out_of_date(env.kv.as_ref(), std::slice::from_ref(&g))
            .await
            .unwrap()
    );

    // A binary that removed `h` stops it once every worker runs that binary.
    member_aside(&env, "w-new", NOW, &["acme/h"]).await;
    let publication = publish_code(&env, std::slice::from_ref(&g)).await;
    assert_eq!(publication.orphaned, [gk("h")]);
    let entry: GraphIndexEntry = env.kv.hget_json(keys::CLUSTER_GRAPHS, "acme/h").await.unwrap().unwrap();
    assert!(entry.orphan);
    assert_eq!(entry.fingerprint, None);
    assert_eq!(fingerprints(&env, "h").await, [Some(h.declaration_fingerprint()), None]);
    assert!(
        !control::out_of_date(env.kv.as_ref(), std::slice::from_ref(&g))
            .await
            .unwrap()
    );

    // Declared again as it was: going back, which waits for the workers of
    // the binary without it.
    member_aside(&env, "w-new", NOW, &[]).await;
    assert_eq!(publish_code(&env, &[g.clone(), h.clone()]).await.aside, [gk("h")]);
    member_aside(&env, "w-new", NOW, &["acme/h"]).await;
    assert_eq!(publish_code(&env, &[g, h]).await.restored, [(gk("h"), 2)]);
}

#[tokio::test]
async fn a_description_written_late_over_a_newer_one_is_noticed_and_repaired() {
    let env = Env::new().await;
    let old = spec(graph_body("g"));
    let mut body = graph_body("g");
    body["nodes"][0]["config"]["rate"] = json!(20);
    let new = spec(body);
    publish_code(&env, std::slice::from_ref(&old)).await;
    let late: GraphSpec = env.kv.get_json(&keys::graph_spec(&gk("g"))).await.unwrap().unwrap();
    publish_code(&env, std::slice::from_ref(&new)).await;
    assert!(
        !control::out_of_date(env.kv.as_ref(), std::slice::from_ref(&new))
            .await
            .unwrap()
    );
    // The write of the first publication lands now, behind the index.
    env.kv.set_json(&keys::graph_spec(&gk("g")), &late).await.unwrap();
    assert!(
        control::out_of_date(env.kv.as_ref(), std::slice::from_ref(&new))
            .await
            .unwrap()
    );
    assert_eq!(
        publish_code(&env, std::slice::from_ref(&new)).await.updated,
        [(gk("g"), 3)]
    );
    assert!(
        !control::out_of_date(env.kv.as_ref(), std::slice::from_ref(&new))
            .await
            .unwrap()
    );
}
