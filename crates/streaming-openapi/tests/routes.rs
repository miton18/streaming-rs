//! `#[route]` handlers: what they serve and how they are described.

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{Request, StatusCode},
    response::{IntoResponse, Response as AxumResponse},
};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use streaming_openapi::{
    ApiRouter, Info, Method, OPENAPI_VERSION, OpenApi, OperationContext, OperationOutput, Response, route,
};
use tower::ServiceExt;

#[derive(Clone)]
struct AppState {
    greeting: &'static str,
}

/// A greeting.
#[derive(Serialize, Deserialize, JsonSchema)]
struct Greeting {
    /// The text.
    text: String,
    /// How many times.
    #[serde(default)]
    times: u32,
}

#[derive(Deserialize, JsonSchema)]
struct Page {
    /// Rows to return at most.
    limit: Option<usize>,
    /// First row.
    offset: usize,
}

#[derive(Deserialize, JsonSchema)]
struct Coordinates {
    /// The row.
    row: u32,
    /// The column.
    column: u32,
}

/// Failure, answered as 404.
struct Missing;

impl IntoResponse for Missing {
    fn into_response(self) -> AxumResponse {
        StatusCode::NOT_FOUND.into_response()
    }
}

impl OperationOutput for Missing {
    fn describe(operation: &mut OperationContext<'_>) {
        operation.add_response("404", Response::new("Not found."));
    }
}

/// Liveness.
#[route(GET, "/healthz")]
async fn healthz() -> &'static str {
    "ok"
}

/// Greet a name
/// in two lines.
///
/// Longer explanation.
///
/// Second paragraph.
#[route(GET, "/greet/{name}", tag = "greetings")]
async fn greet(State(state): State<AppState>, Path(name): Path<String>) -> Json<Greeting> {
    Json(Greeting {
        text: format!("{} {name}", state.greeting),
        times: 1,
    })
}

#[route(PUT, "/greet/{lang}/{name}", tag = "greetings")]
async fn store(
    State(_): State<AppState>,
    Path((lang, name)): Path<(String, u32)>,
    Json(greeting): Json<Greeting>,
) -> Result<Json<Greeting>, Missing> {
    if lang == "none" {
        return Err(Missing);
    }
    Ok(Json(Greeting {
        text: format!("{lang}:{name}:{}", greeting.text),
        times: greeting.times,
    }))
}

#[route(GET, "/cells/{row}/{column}", tag = "cells")]
async fn cell(Path(at): Path<Coordinates>, Query(page): Query<Page>) -> Json<Vec<u32>> {
    Json(vec![
        at.row,
        at.column,
        page.offset as u32,
        page.limit.unwrap_or(0) as u32,
    ])
}

fn api() -> ApiRouter<AppState> {
    ApiRouter::new()
        .route(healthz::route())
        .route(greet::route())
        .route(store::route())
        .route(cell::route())
}

fn document() -> Value {
    let document: OpenApi = api().openapi(Info::new("Test", "1.2.3").description("For tests."));
    serde_json::to_value(document).unwrap()
}

async fn call(router: Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, String) {
    let request = Request::builder().method(method).uri(uri);
    let request = match body {
        Some(body) => request
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())),
        None => request.body(Body::empty()),
    }
    .unwrap();
    let response = router.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn serves_every_route() {
    let router = api().into_router().with_state(AppState { greeting: "hello" });
    assert_eq!(
        call(router.clone(), "GET", "/healthz", None).await,
        (StatusCode::OK, "ok".into())
    );
    let (status, body) = call(router.clone(), "GET", "/greet/bob", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        json!({"text": "hello bob", "times": 1})
    );
    let (status, body) = call(router.clone(), "PUT", "/greet/fr/7", Some(json!({"text": "salut"}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        json!({"text": "fr:7:salut", "times": 0})
    );
    let (status, _) = call(router.clone(), "PUT", "/greet/none/7", Some(json!({"text": "x"}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, body) = call(router.clone(), "GET", "/cells/3/4?offset=5&limit=6", None).await;
    assert_eq!((status, body.as_str()), (StatusCode::OK, "[3,4,5,6]"));
    // Two methods on one path are both served.
    let (status, _) = call(router, "POST", "/greet/bob", None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

#[test]
fn lists_operations_in_order() {
    assert_eq!(
        api().operations(),
        vec![
            (Method::Get, "/healthz"),
            (Method::Get, "/greet/{name}"),
            (Method::Put, "/greet/{lang}/{name}"),
            (Method::Get, "/cells/{row}/{column}"),
        ]
    );
}

#[test]
fn writes_the_document_header() {
    let document = document();
    assert_eq!(document["openapi"], OPENAPI_VERSION);
    assert_eq!(
        document["info"],
        json!({"title": "Test", "version": "1.2.3", "description": "For tests."})
    );
    assert_eq!(document["tags"], json!([{"name": "cells"}, {"name": "greetings"}]));
}

#[test]
fn takes_summary_and_description_from_the_doc_comment() {
    let document = document();
    let greet = &document["paths"]["/greet/{name}"]["get"];
    assert_eq!(greet["operationId"], "greet");
    assert_eq!(greet["summary"], "Greet a name in two lines.");
    assert_eq!(greet["description"], "Longer explanation.\n\nSecond paragraph.");
    assert_eq!(greet["tags"], json!(["greetings"]));
    let store = &document["paths"]["/greet/{lang}/{name}"]["put"];
    assert!(store.get("summary").is_none() && store.get("description").is_none());
    assert!(store.get("summary").is_none());
    assert!(document["paths"]["/healthz"]["get"].get("tags").is_none());
}

#[test]
fn describes_text_responses() {
    let document = document();
    let healthz = &document["paths"]["/healthz"]["get"];
    assert!(healthz.get("parameters").is_none() && healthz.get("requestBody").is_none());
    assert_eq!(
        healthz["responses"],
        json!({"200": {"description": "Success.", "content": {"text/plain": {"schema": {"type": "string"}}}}})
    );
}

#[test]
fn describes_path_parameters_by_position() {
    let document = document();
    assert_eq!(
        document["paths"]["/greet/{name}"]["get"]["parameters"],
        json!([{"name": "name", "in": "path", "required": true, "schema": {"type": "string"}}])
    );
    let parameters = &document["paths"]["/greet/{lang}/{name}"]["put"]["parameters"];
    assert_eq!(
        parameters[0],
        json!({"name": "lang", "in": "path", "required": true, "schema": {"type": "string"}})
    );
    assert_eq!(parameters[1]["name"], "name");
    assert_eq!(parameters[1]["schema"]["type"], "integer");
    assert_eq!(parameters.as_array().unwrap().len(), 2);
}

#[test]
fn describes_struct_parameters_by_name() {
    let document = document();
    let parameters = document["paths"]["/cells/{row}/{column}"]["get"]["parameters"]
        .as_array()
        .unwrap()
        .clone();
    let find = |name: &str, location: &str| {
        parameters
            .iter()
            .find(|p| p["name"] == name && p["in"] == location)
            .unwrap_or_else(|| panic!("no {location} parameter {name}"))
            .clone()
    };
    assert_eq!(parameters.len(), 4);
    assert_eq!(find("row", "path")["required"], true);
    assert_eq!(find("row", "path")["description"], "The row.");
    assert!(find("row", "path")["schema"].get("description").is_none());
    assert_eq!(find("column", "path")["required"], true);
    assert_eq!(find("offset", "query")["required"], true);
    assert_eq!(find("limit", "query")["required"], false);
    assert_eq!(find("limit", "query")["description"], "Rows to return at most.");
}

#[test]
fn describes_bodies_and_errors() {
    let document = document();
    let store = &document["paths"]["/greet/{lang}/{name}"]["put"];
    assert_eq!(store["requestBody"]["required"], true);
    assert_eq!(
        store["requestBody"]["content"]["application/json"]["schema"],
        json!({"$ref": "#/components/schemas/GreetingInput"})
    );
    assert_eq!(
        store["responses"]["200"]["content"]["application/json"]["schema"],
        json!({"$ref": "#/components/schemas/Greeting"})
    );
    assert_eq!(store["responses"]["404"], json!({"description": "Not found."}));
}

#[test]
fn lists_a_type_twice_when_its_input_differs() {
    let document = document();
    let schemas = &document["components"]["schemas"];
    // Serialized, `times` is always there; deserialized, it may be omitted.
    assert_eq!(schemas["Greeting"]["required"], json!(["text", "times"]));
    assert_eq!(schemas["GreetingInput"]["required"], json!(["text"]));
    assert_eq!(schemas["Greeting"]["description"], "A greeting.");
    assert_eq!(schemas["Greeting"]["properties"]["text"]["description"], "The text.");
    // No dangling reference.
    let text = document.to_string();
    for reference in text.split("\"$ref\":\"#/components/schemas/").skip(1) {
        let name = reference.split('"').next().unwrap();
        assert!(schemas.get(name).is_some(), "dangling reference to {name}");
    }
}

#[test]
#[should_panic(expected = "two handlers serve GET /healthz")]
fn refuses_two_handlers_for_one_operation() {
    let _ = ApiRouter::<AppState>::new()
        .route(healthz::route())
        .route(healthz::route());
}
