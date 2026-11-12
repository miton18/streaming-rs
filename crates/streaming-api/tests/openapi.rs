//! The OpenAPI document: complete, consistent, and committed.

use std::collections::BTreeSet;

use serde_json::Value;

fn document() -> Value {
    serde_json::to_value(streaming_api::openapi()).unwrap()
}

const COMMITTED: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/openapi.json");

/// `docs/openapi.json` is what the code describes. Regenerate it with
/// `UPDATE_OPENAPI=1 cargo test -p streaming-api --test openapi`.
#[test]
fn the_committed_document_is_up_to_date() {
    let fresh = serde_json::to_string_pretty(&document()).unwrap() + "\n";
    if std::env::var_os("UPDATE_OPENAPI").is_some() {
        std::fs::write(COMMITTED, &fresh).unwrap();
        return;
    }
    let committed = std::fs::read_to_string(COMMITTED).unwrap_or_default();
    assert!(
        committed == fresh,
        "docs/openapi.json is stale: regenerate it with `UPDATE_OPENAPI=1 cargo test -p streaming-api --test openapi`"
    );
}

#[test]
fn describes_every_operation() {
    let document = document();
    assert_eq!(document["openapi"], "3.2.0");
    let operations: BTreeSet<String> = document["paths"]
        .as_object()
        .unwrap()
        .iter()
        .flat_map(|(path, item)| {
            item.as_object()
                .unwrap()
                .keys()
                .map(move |method| format!("{method} {path}"))
        })
        .collect();
    let graph = "/api/v1/tenants/{tenant}/graphs/{graph}";
    let expected: BTreeSet<String> = [
        "get /healthz".to_owned(),
        "get /api/v1/openapi.json".to_owned(),
        "get /api/v1/cluster".to_owned(),
        "get /api/v1/catalog".to_owned(),
        "get /api/v1/tenants".to_owned(),
        "get /api/v1/tenants/{tenant}".to_owned(),
        "put /api/v1/tenants/{tenant}".to_owned(),
        "get /api/v1/tenants/{tenant}/graphs".to_owned(),
        format!("get {graph}"),
        format!("put {graph}"),
        format!("delete {graph}"),
        format!("post {graph}/state"),
        format!("get {graph}/dot"),
        format!("get {graph}/outputs/{{node}}"),
        format!("post {graph}/nodes/{{node}}/pause"),
        format!("post {graph}/nodes/{{node}}/resume"),
        format!("patch {graph}/nodes/{{node}}/config"),
    ]
    .into_iter()
    .collect();
    assert_eq!(operations, expected);
}

#[test]
fn every_operation_is_documented_and_declares_its_path_parameters() {
    let document = document();
    for (path, item) in document["paths"].as_object().unwrap() {
        let names: Vec<&str> = path
            .split('{')
            .skip(1)
            .map(|rest| rest.split('}').next().unwrap())
            .collect();
        for (method, operation) in item.as_object().unwrap() {
            let at = format!("{method} {path}");
            assert!(operation["summary"].is_string(), "{at} has no summary");
            assert!(operation["operationId"].is_string(), "{at} has no operation id");
            assert_eq!(operation["tags"].as_array().map(Vec::len), Some(1), "{at}");
            assert!(
                operation["responses"]["200"].is_object(),
                "{at} has no success response"
            );
            let declared: Vec<&str> = operation["parameters"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|p| p["in"] == "path")
                .map(|p| p["name"].as_str().unwrap())
                .collect();
            assert_eq!(declared, names, "{at}");
        }
    }
}

#[test]
fn derives_bodies_and_responses_from_the_signatures() {
    let document = document();
    let graph = &document["paths"]["/api/v1/tenants/{tenant}/graphs/{graph}"];
    assert_eq!(graph["put"]["operationId"], "putGraph");
    let json = |value: &Value| value["content"]["application/json"]["schema"]["$ref"].clone();
    assert_eq!(
        json(&graph["put"]["requestBody"]),
        "#/components/schemas/GraphSpecInput"
    );
    assert_eq!(
        json(&graph["put"]["responses"]["200"]),
        "#/components/schemas/VersionReply"
    );
    assert_eq!(
        json(&graph["get"]["responses"]["200"]),
        "#/components/schemas/GraphDetail"
    );
    assert_eq!(
        json(&graph["delete"]["responses"]["200"]),
        "#/components/schemas/DeletingReply"
    );
    assert_eq!(
        json(&graph["get"]["responses"]["default"]),
        "#/components/schemas/ErrorBody"
    );
    assert_eq!(
        graph["get"]["responses"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        ["200", "default"]
    );
    let cluster = &document["paths"]["/api/v1/cluster"]["get"];
    assert_eq!(json(&cluster["responses"]["200"]), "#/components/schemas/ClusterView");
    let outputs = &document["paths"]["/api/v1/tenants/{tenant}/graphs/{graph}/outputs/{node}"]["get"];
    let limit = outputs["parameters"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["in"] == "query")
        .unwrap();
    assert_eq!(limit["name"], "limit");
    assert_eq!(limit["required"], false);
    let dot = &document["paths"]["/api/v1/tenants/{tenant}/graphs/{graph}/dot"]["get"];
    assert!(dot["responses"]["200"]["content"]["text/vnd.graphviz"].is_object());
    let schemas = &document["components"]["schemas"];
    // Identifiers carry their alphabet.
    assert_eq!(schemas["TenantId"]["pattern"], "^[A-Za-z0-9_-]+$");
    assert_eq!(schemas["OpRef"]["pattern"], "^[A-Za-z0-9_/@.-]+$");
    // Field names are lowerCamelCase, as on the wire.
    assert!(schemas["GraphSummary"]["properties"]["watermarkMs"].is_object());
    assert!(schemas["GraphSettings"]["properties"]["maxPendingWindows"].is_object());
    let text = document.to_string();
    for (name, schema) in schemas.as_object().unwrap() {
        for field in schema["properties"].as_object().into_iter().flat_map(|p| p.keys()) {
            assert!(!field.contains('_'), "{name}.{field} is not lowerCamelCase");
        }
    }
    assert!(!text.contains("\"operationId\":\"put_graph\""));
    // A description read back always has its version; one sent may omit it.
    let required = |name: &str| schemas[name]["required"].as_array().unwrap().clone();
    assert!(required("GraphSpec").contains(&"version".into()));
    assert!(!required("GraphSpecInput").contains(&"version".into()));
}

#[test]
fn every_reference_resolves() {
    let document = document();
    let text = document.to_string();
    let schemas = document["components"]["schemas"].as_object().unwrap();
    let mut used = BTreeSet::new();
    for reference in text.split("\"$ref\":\"").skip(1) {
        let reference = reference.split('"').next().unwrap();
        let name = reference
            .strip_prefix("#/components/schemas/")
            .unwrap_or_else(|| panic!("unexpected reference {reference}"));
        assert!(schemas.contains_key(name), "dangling reference to {name}");
        used.insert(name.to_owned());
    }
    let unused: Vec<&String> = schemas.keys().filter(|name| !used.contains(*name)).collect();
    assert!(unused.is_empty(), "unreferenced schemas: {unused:?}");
}

#[tokio::test]
async fn is_served() {
    use tower::ServiceExt;
    let state = streaming_api::ApiState {
        kv: std::sync::Arc::new(streaming_core::MemKv::new()),
        clock: std::sync::Arc::new(streaming_core::SystemClock),
        kv_stats: None,
        config: streaming_api::ApiConfig::default(),
    };
    let request = axum::http::Request::get("/api/v1/openapi.json")
        .body(axum::body::Body::empty())
        .unwrap();
    let response = streaming_api::router(state).oneshot(request).await.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = http_body_util::BodyExt::collect(response.into_body())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), document());
}
