//! Integration tests: the real router over a `MemKv`.

use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use streaming_api::{ApiConfig, ApiState, router};
use streaming_core::{
    DynKv, GraphKey, MemKv, OpDescriptor, OpKind, OpRef, Registry, TypeTag,
    control::{
        ActorKind, ActorSnapshot, ActorState, DesiredState, GraphIndexEntry, Load, MemberInfo, OutputRow, Placement,
        RunState, SliceSnapshot, TenantUsage, WorkerSnapshot,
    },
    keys,
};
use tower::ServiceExt;

const NOW: i64 = 1_790_000_000_000;

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
}

fn descriptor(op: &str, kind: OpKind) -> OpDescriptor {
    OpDescriptor {
        op: OpRef::new(op).unwrap(),
        kind,
        input: TypeTag::Any,
        output: TypeTag::Any,
        description: format!("fake {op}"),
        params: Vec::new(),
    }
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
        };
        Self {
            kv: kv.clone(),
            mem,
            clock,
            app: router(state),
        }
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

    async fn get(&self, uri: &str) -> (StatusCode, Value) {
        self.call(Method::GET, uri, None).await
    }

    async fn epoch(&self) -> i64 {
        self.kv
            .get(keys::CLUSTER_EPOCH)
            .await
            .unwrap()
            .map_or(0, |b| String::from_utf8(b).unwrap().parse().unwrap())
    }

    async fn put_graph(&self, name: &str) -> (StatusCode, Value) {
        self.call(
            Method::PUT,
            &format!("/api/v1/tenants/acme/graphs/{name}"),
            Some(graph_body(name)),
        )
        .await
    }
}

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

fn gk(graph: &str) -> GraphKey {
    GraphKey::new("acme".parse().unwrap(), graph.parse().unwrap())
}

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
async fn put_creates_then_bumps_the_version_and_the_epoch() {
    let env = Env::new().await;
    assert_eq!(env.epoch().await, 0);
    let (status, body) = env.put_graph("g").await;
    assert_eq!((status, body), (StatusCode::OK, json!({"version": 1})));
    assert_eq!(env.epoch().await, 1);

    // Everything the contract says was written.
    let spec: Value = serde_json::from_slice(&env.kv.get(&keys::graph_spec(&gk("g"))).await.unwrap().unwrap()).unwrap();
    assert_eq!(spec["version"], 1);
    let desired: DesiredState = env.kv.get_json(&keys::graph_desired(&gk("g"))).await.unwrap().unwrap();
    assert_eq!(desired.state, RunState::Running);
    let entry: GraphIndexEntry = env.kv.hget_json(keys::CLUSTER_GRAPHS, "acme/g").await.unwrap().unwrap();
    assert_eq!((entry.version, entry.deleting), (1, false));

    // An existing desired state is kept.
    env.call(
        Method::POST,
        "/api/v1/tenants/acme/graphs/g/state",
        Some(json!({"state": "paused"})),
    )
    .await;
    env.clock.store(NOW + 10, Ordering::SeqCst);
    let (_, body) = env.put_graph("g").await;
    assert_eq!(body, json!({"version": 2}));
    let desired: DesiredState = env.kv.get_json(&keys::graph_desired(&gk("g"))).await.unwrap().unwrap();
    assert_eq!(desired.state, RunState::Paused);
    let entry: GraphIndexEntry = env.kv.hget_json(keys::CLUSTER_GRAPHS, "acme/g").await.unwrap().unwrap();
    assert_eq!((entry.version, entry.updated_ms), (2, NOW + 10));
    assert_eq!(env.epoch().await, 3);
}

#[tokio::test]
async fn put_rejects_invalid_input_with_a_message() {
    let env = Env::new().await;
    // Unknown operator.
    let mut body = graph_body("g");
    body["nodes"][0]["op"] = json!("t/nope@1");
    let (status, answer) = env.call(Method::PUT, "/api/v1/tenants/acme/graphs/g", Some(body)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let message = answer["message"].as_str().unwrap();
    assert!(message.contains("unknown operator `t/nope@1`"), "{message}");

    // Wiring error.
    let mut body = graph_body("g");
    body["edges"] = json!([["events", "key"]]);
    let (status, answer) = env.call(Method::PUT, "/api/v1/tenants/acme/graphs/g", Some(body)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(answer["message"].as_str().unwrap().contains("key"), "{answer}");

    // Body for another graph, another tenant.
    let (status, answer) = env
        .call(Method::PUT, "/api/v1/tenants/acme/graphs/other", Some(graph_body("g")))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(answer["message"].as_str().unwrap().contains("acme/g"), "{answer}");
    let (status, _) = env
        .call(Method::PUT, "/api/v1/tenants/globex/graphs/g", Some(graph_body("g")))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Not a description at all / invalid id inside it.
    let (status, answer) = env
        .call(Method::PUT, "/api/v1/tenants/acme/graphs/g", Some(json!({"x": 1})))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(answer["message"].is_string());
    let mut body = graph_body("g");
    body["nodes"][0]["id"] = json!("bad id");
    let (status, _) = env.call(Method::PUT, "/api/v1/tenants/acme/graphs/g", Some(body)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Invalid id in the path.
    let (status, answer) = env
        .call(Method::PUT, "/api/v1/tenants/acme/graphs/bad.id", Some(graph_body("g")))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(answer["message"].as_str().unwrap().contains("graph id"), "{answer}");

    // Nothing was written.
    assert_eq!(env.epoch().await, 0);
    assert!(env.mem.keys().iter().all(|k| k.starts_with("cluster:catalog")));
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
    env.call(
        Method::PUT,
        "/api/v1/tenants/globex/graphs/x",
        Some({
            let mut b = graph_body("x");
            b["tenant"] = json!("globex");
            b
        }),
    )
    .await;
    publish_worker(&env, "worker-a", NOW, Some(worker_snapshot("worker-a", "g"))).await;

    let (status, body) = env.get("/api/v1/tenants").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({"tenants": [
            {"tenant": "acme", "graphs": 2, "actors": 7, "inPerSec": 1200.0},
            {"tenant": "globex", "graphs": 1, "actors": 0, "inPerSec": 0.0}
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
async fn patch_config_merges_removes_and_versions() {
    let env = Env::new().await;
    env.put_graph("g").await;
    let (status, body) = env
        .call(
            Method::PATCH,
            "/api/v1/tenants/acme/graphs/g/nodes/events/config",
            Some(json!({"rate": 50, "topic": null, "new": {"a": [1]}})),
        )
        .await;
    assert_eq!((status, body), (StatusCode::OK, json!({"version": 2})));
    let (_, detail) = env.get("/api/v1/tenants/acme/graphs/g").await;
    assert_eq!(detail["version"], 2);
    assert_eq!(
        detail["spec"]["nodes"][0]["config"],
        json!({"rate": 50, "new": {"a": [1]}})
    );
    // Other nodes untouched.
    assert_eq!(detail["spec"]["nodes"][1]["id"], "key");
    assert_eq!(env.epoch().await, 2);

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

    // Errors.
    let (status, _) = env
        .call(
            Method::PATCH,
            "/api/v1/tenants/acme/graphs/g/nodes/ghost/config",
            Some(json!({"a": 1})),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, body) = env
        .call(
            Method::PATCH,
            "/api/v1/tenants/acme/graphs/g/nodes/events/config",
            Some(json!([1, 2])),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["message"].is_string());
    assert_eq!(env.epoch().await, 3);
}

#[tokio::test]
async fn patch_revalidates_against_the_catalog() {
    let env = Env::new().await;
    env.put_graph("g").await;
    // The operator disappeared from the catalog: the patched description is
    // rejected like a PUT.
    env.kv
        .hdel(keys::CLUSTER_CATALOG, vec!["t/src@1".to_owned()])
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
    assert!(body["message"].as_str().unwrap().contains("unknown operator"));
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
async fn delete_marks_the_graph_deleting() {
    let env = Env::new().await;
    env.put_graph("g").await;
    env.clock.store(NOW + 99, Ordering::SeqCst);
    let (status, _) = env.call(Method::DELETE, "/api/v1/tenants/acme/graphs/g", None).await;
    assert_eq!(status, StatusCode::OK);
    let desired: DesiredState = env.kv.get_json(&keys::graph_desired(&gk("g"))).await.unwrap().unwrap();
    assert_eq!(desired.state, RunState::Stopped);
    let entry: GraphIndexEntry = env.kv.hget_json(keys::CLUSTER_GRAPHS, "acme/g").await.unwrap().unwrap();
    assert!(entry.deleting);
    assert_eq!(entry.updated_ms, NOW + 99);
    assert_eq!(env.epoch().await, 2);
    // The spec is still there for the controller.
    assert!(env.kv.get(&keys::graph_spec(&gk("g"))).await.unwrap().is_some());

    // A deleting graph cannot be reconfigured.
    let (status, body) = env.put_graph("g").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["message"].as_str().unwrap().contains("being deleted"));
    let (status, _) = env
        .call(
            Method::POST,
            "/api/v1/tenants/acme/graphs/g/state",
            Some(json!({"state": "running"})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
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
    env.put_graph("g").await;
    check!("put");
    env.call(
        Method::PATCH,
        "/api/v1/tenants/acme/graphs/g/nodes/events/config",
        Some(json!({"a": 1})),
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
    assert_eq!(body, json!({"message": "not found"}));
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
