//! OpenAPI documents derived from the signatures of axum handlers.
//!
//! A handler carries its method and path in a [`route`] attribute. The
//! attribute leaves the function as it is and generates, next to it, a module
//! of the same name whose `route()` gives the handler, its method, its path and
//! the way to describe it. An [`ApiRouter`] collects routes, registers them
//! with axum and writes the [`OpenApi`] document of exactly what it serves:
//!
//! ```
//! use axum::Json;
//! use axum::extract::{Path, State};
//! use streaming_openapi::{ApiRouter, Info, route};
//!
//! #[derive(Clone)]
//! struct AppState;
//!
//! #[derive(serde::Serialize, schemars::JsonSchema)]
//! struct Greeting {
//!     /// Who is greeted.
//!     name: String,
//! }
//!
//! /// Greet someone.
//! #[route(GET, "/hello/{name}", tag = "greetings")]
//! async fn hello(State(_): State<AppState>, Path(name): Path<String>) -> Json<Greeting> {
//!     Json(Greeting { name })
//! }
//!
//! fn main() {
//!     let api = ApiRouter::new().route(hello::route());
//!     let document = api.openapi(Info::new("Greeter", "1.0.0"));
//!     assert!(document.paths["/hello/{name}"].contains_key("get"));
//!     let router: axum::Router = api.into_router().with_state(AppState);
//!     # let _ = router;
//! }
//! ```
//!
//! The generated module sees what the handler's module sees (`use super::*`),
//! so handlers live at module level, not inside a function body.
//!
//! Each argument type implements [`OperationInput`] and the return type
//! [`OperationOutput`]: that is how the schemas come from the signature, type
//! aliases included. Schemas are JSON Schema 2020-12, written by
//! [`schemars`]; a type that crosses the API derives `schemars::JsonSchema`.
//! Responses are described as the types serialize, request bodies and
//! parameters as they deserialize; a type whose two forms differ is listed
//! twice, the second time as `<Name>Input`.

use std::collections::{BTreeMap, BTreeSet};

use axum::{Router, routing::MethodRouter};
use schemars::{
    JsonSchema, Schema, SchemaGenerator,
    generate::{Contract, SchemaSettings},
};
use serde::Serialize;
use serde_json::{Map, Value, json};
pub use streaming_openapi_macros::route;

/// Version of the OpenAPI Specification the documents follow.
pub const OPENAPI_VERSION: &str = "3.2.0";

#[doc(hidden)]
pub mod __private {
    pub use axum;
}

/// An HTTP method of an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Method {
    /// `GET`.
    Get,
    /// `POST`.
    Post,
    /// `PUT`.
    Put,
    /// `DELETE`.
    Delete,
    /// `PATCH`.
    Patch,
    /// `HEAD`.
    Head,
    /// `OPTIONS`.
    Options,
    /// `TRACE`.
    Trace,
}

impl Method {
    /// The method whose axum router function is `name` (`get`, `post`, ...).
    ///
    /// # Panics
    ///
    /// On another name; `#[route]` only passes known ones.
    #[doc(hidden)]
    #[must_use]
    pub fn from_axum_name(name: &str) -> Self {
        match name {
            "get" => Self::Get,
            "post" => Self::Post,
            "put" => Self::Put,
            "delete" => Self::Delete,
            "patch" => Self::Patch,
            "head" => Self::Head,
            "options" => Self::Options,
            "trace" => Self::Trace,
            other => panic!("unknown HTTP method `{other}`"),
        }
    }

    /// The key of the operation in an OpenAPI path item (`get`, `post`, ...).
    #[must_use]
    pub fn key(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Post => "post",
            Self::Put => "put",
            Self::Delete => "delete",
            Self::Patch => "patch",
            Self::Head => "head",
            Self::Options => "options",
            Self::Trace => "trace",
        }
    }
}

/// A handler with its method, its path and its description, as `#[route]`
/// builds it.
pub struct Route<S> {
    method: Method,
    path: &'static str,
    handler: MethodRouter<S>,
    describe: fn(&mut OperationContext<'_>),
}

impl<S> Route<S> {
    /// Assemble a route. `#[route]` calls this; write the attribute instead.
    #[doc(hidden)]
    pub fn new(
        method: Method,
        path: &'static str,
        handler: MethodRouter<S>,
        describe: fn(&mut OperationContext<'_>),
    ) -> Self {
        Self {
            method,
            path,
            handler,
            describe,
        }
    }

    /// Its method.
    #[must_use]
    pub fn method(&self) -> Method {
        self.method
    }

    /// Its path, in axum's syntax, which is also OpenAPI's (`/a/{b}`).
    #[must_use]
    pub fn path(&self) -> &'static str {
        self.path
    }
}

/// How `#[route]` describes an operation.
type Describe = fn(&mut OperationContext<'_>);

/// The described routes of an axum router.
pub struct ApiRouter<S> {
    router: Router<S>,
    routes: Vec<(Method, &'static str, Describe)>,
}

impl<S: Clone + Send + Sync + 'static> Default for ApiRouter<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Clone + Send + Sync + 'static> ApiRouter<S> {
    /// No route yet.
    #[must_use]
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            routes: Vec::new(),
        }
    }

    /// Serve `route` and describe it.
    ///
    /// # Panics
    ///
    /// When another route has the same method and path.
    #[must_use]
    pub fn route(mut self, route: Route<S>) -> Self {
        assert!(
            !self
                .routes
                .iter()
                .any(|(method, path, _)| *method == route.method && *path == route.path),
            "two handlers serve {} {}",
            route.method.key().to_uppercase(),
            route.path
        );
        self.router = self.router.route(route.path, route.handler);
        self.routes.push((route.method, route.path, route.describe));
        self
    }

    /// The `(method, path)` of every route, in the order they were added.
    #[must_use]
    pub fn operations(&self) -> Vec<(Method, &'static str)> {
        self.routes.iter().map(|(method, path, _)| (*method, *path)).collect()
    }

    /// The document of every route.
    #[must_use]
    pub fn openapi(&self, info: Info) -> OpenApi {
        let mut responses = generator(Contract::Serialize);
        let mut requests = generator(Contract::Deserialize);
        let mut paths: BTreeMap<String, BTreeMap<String, Operation>> = BTreeMap::new();
        for (method, path, describe) in &self.routes {
            let mut context = OperationContext {
                path_params: path_params(path),
                operation: Operation::default(),
                responses: &mut responses,
                requests: &mut requests,
            };
            describe(&mut context);
            paths
                .entry((*path).to_owned())
                .or_default()
                .insert(method.key().to_owned(), context.operation);
        }

        let mut schemas = responses.take_definitions(true);
        let request_schemas = requests.take_definitions(true);
        // A type that deserializes from another shape than it serializes to
        // (default fields, for instance) is listed twice.
        let renamed: BTreeMap<String, String> = request_schemas
            .iter()
            .filter(|(name, schema)| schemas.get(*name).is_some_and(|other| other != *schema))
            .map(|(name, _)| (name.clone(), input_name(name, &schemas, &request_schemas)))
            .collect();
        for (name, mut schema) in request_schemas {
            rename_refs(&mut schema, &renamed);
            let name = renamed.get(&name).cloned().unwrap_or(name);
            schemas.entry(name).or_insert(schema);
        }
        for operation in paths.values_mut().flat_map(BTreeMap::values_mut) {
            for parameter in &mut operation.parameters {
                rename_refs(&mut parameter.schema, &renamed);
            }
            if let Some(body) = &mut operation.request_body {
                for media in body.content.values_mut() {
                    rename_refs(&mut media.schema, &renamed);
                }
            }
        }

        let tags = paths
            .values()
            .flat_map(BTreeMap::values)
            .flat_map(|operation| operation.tags.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|name| Tag { name })
            .collect();
        OpenApi {
            openapi: OPENAPI_VERSION.to_owned(),
            info,
            tags,
            paths,
            components: Components {
                schemas: schemas.into_iter().collect(),
            },
        }
    }

    /// The axum router serving every route.
    pub fn into_router(self) -> Router<S> {
        self.router
    }
}

fn generator(contract: Contract) -> SchemaGenerator {
    let mut settings = SchemaSettings::draft2020_12();
    settings.definitions_path = "/components/schemas".into();
    settings.meta_schema = None;
    settings.contract = contract;
    SchemaGenerator::new(settings)
}

/// `<name>Input`, or `<name>Input2`, ... when taken.
fn input_name(name: &str, responses: &Map<String, Value>, requests: &Map<String, Value>) -> String {
    let taken = |candidate: &String| responses.contains_key(candidate) || requests.contains_key(candidate);
    let mut candidate = format!("{name}Input");
    let mut n = 2;
    while taken(&candidate) {
        candidate = format!("{name}Input{n}");
        n += 1;
    }
    candidate
}

const REF_PREFIX: &str = "#/components/schemas/";

/// Point the `$ref`s of `value` at the renamed definitions.
fn rename_refs(value: &mut Value, renamed: &BTreeMap<String, String>) {
    match value {
        Value::Object(object) => {
            if let Some(Value::String(reference)) = object.get_mut("$ref")
                && let Some(new) = reference.strip_prefix(REF_PREFIX).and_then(|name| renamed.get(name))
            {
                *reference = format!("{REF_PREFIX}{new}");
            }
            for child in object.values_mut() {
                rename_refs(child, renamed);
            }
        }
        Value::Array(items) => {
            for child in items {
                rename_refs(child, renamed);
            }
        }
        _ => {}
    }
}

/// The names of the `{param}` segments of a path, in order.
fn path_params(path: &str) -> Vec<String> {
    path.split('{')
        .skip(1)
        .filter_map(|rest| rest.split_once('}').map(|(name, _)| name.to_owned()))
        .collect()
}

/// What an operation is being described with.
pub struct OperationContext<'a> {
    path_params: Vec<String>,
    operation: Operation,
    responses: &'a mut SchemaGenerator,
    requests: &'a mut SchemaGenerator,
}

impl OperationContext<'_> {
    /// Set the operation id (the handler's name).
    pub fn set_operation_id(&mut self, id: &str) {
        self.operation.operation_id = Some(id.to_owned());
    }

    /// Set the summary (first paragraph) and description (the rest) from the
    /// handler's doc comment.
    pub fn set_docs(&mut self, docs: &str) {
        let docs = docs.trim();
        let (summary, description) = docs.split_once("\n\n").unwrap_or((docs, ""));
        let summary = summary.split_whitespace().collect::<Vec<_>>().join(" ");
        self.operation.summary = (!summary.is_empty()).then_some(summary);
        let description = description.trim();
        self.operation.description = (!description.is_empty()).then(|| description.to_owned());
    }

    /// Add a tag.
    pub fn add_tag(&mut self, tag: &str) {
        self.operation.tags.push(tag.to_owned());
    }

    /// The names of the path's `{param}` segments, in order.
    #[must_use]
    pub fn path_params(&self) -> &[String] {
        &self.path_params
    }

    /// The schema of `T` as it serializes (for a response), usually a
    /// reference to a component.
    pub fn response_schema<T: JsonSchema + ?Sized>(&mut self) -> Value {
        self.responses.subschema_for::<T>().to_value()
    }

    /// The schema of `T` as it deserializes (for a request body), usually a
    /// reference to a component.
    pub fn request_schema<T: JsonSchema + ?Sized>(&mut self) -> Value {
        self.requests.subschema_for::<T>().to_value()
    }

    /// The schema of `T` as it deserializes, inline: the object whose
    /// properties are parameters.
    pub fn inline_request_schema<T: JsonSchema + ?Sized>(&mut self) -> Value {
        let schema: Schema = T::json_schema(self.requests);
        schema.to_value()
    }

    /// Add a parameter.
    pub fn add_parameter(&mut self, parameter: Parameter) {
        self.operation.parameters.push(parameter);
    }

    /// Set the request body.
    pub fn set_request_body(&mut self, body: RequestBody) {
        self.operation.request_body = Some(body);
    }

    /// Add (or replace) the response of a status (`"200"`, `"4XX"`, ...).
    pub fn add_response(&mut self, status: &str, response: Response) {
        self.operation.responses.insert(status.to_owned(), response);
    }

    /// Add one parameter per property of an object schema (path parameters
    /// read into a struct, query strings).
    fn add_object_parameters(&mut self, location: ParameterLocation, schema: &Value) {
        let required: BTreeSet<&str> = schema
            .get("required")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
            return;
        };
        for (name, property) in properties {
            let (description, schema) = split_description(property.clone());
            self.add_parameter(Parameter {
                name: name.clone(),
                location,
                required: location == ParameterLocation::Path || required.contains(name.as_str()),
                description,
                schema,
            });
        }
    }
}

/// Take the description out of a schema, for the object that holds it.
fn split_description(mut schema: Value) -> (Option<String>, Value) {
    let description = schema
        .as_object_mut()
        .and_then(|object| object.remove("description"))
        .and_then(|description| description.as_str().map(str::to_owned));
    (description, schema)
}

/// An argument of a handler, as OpenAPI sees it.
#[diagnostic::on_unimplemented(
    message = "`#[route]` cannot describe the argument type `{Self}`",
    label = "no OpenAPI description for this extractor",
    note = "implement `streaming_openapi::OperationInput` for it, or read the request with an extractor that does (`Json`, `Path`, `Query`, `State`)"
)]
pub trait OperationInput {
    /// Add what the argument reads from the request to the operation.
    fn describe(operation: &mut OperationContext<'_>);
}

/// The return type of a handler, as OpenAPI sees it.
#[diagnostic::on_unimplemented(
    message = "`#[route]` cannot describe the return type `{Self}`",
    label = "no OpenAPI description for this response",
    note = "return a type implementing `streaming_openapi::OperationOutput` (`Json<T>`, `Result<T, E>`, `&'static str`, ...) instead of an opaque response"
)]
pub trait OperationOutput {
    /// Add the responses to the operation.
    fn describe(operation: &mut OperationContext<'_>);
}

impl<T> OperationInput for axum::extract::State<T> {
    fn describe(_: &mut OperationContext<'_>) {}
}

impl<T> OperationInput for axum::Extension<T> {
    fn describe(_: &mut OperationContext<'_>) {}
}

impl<T: JsonSchema> OperationInput for axum::extract::Path<T> {
    fn describe(operation: &mut OperationContext<'_>) {
        let schema = operation.inline_request_schema::<T>();
        let names = operation.path_params().to_vec();
        if schema.get("properties").is_some() {
            operation.add_object_parameters(ParameterLocation::Path, &schema);
            return;
        }
        let items: Vec<Value> = match schema.get("prefixItems").and_then(Value::as_array) {
            Some(items) => items.clone(),
            None => vec![schema],
        };
        for (name, item) in names.into_iter().zip(items) {
            let (description, schema) = split_description(item);
            operation.add_parameter(Parameter {
                name,
                location: ParameterLocation::Path,
                required: true,
                description,
                schema,
            });
        }
    }
}

impl<T: JsonSchema> OperationInput for axum::extract::Query<T> {
    fn describe(operation: &mut OperationContext<'_>) {
        let schema = operation.inline_request_schema::<T>();
        operation.add_object_parameters(ParameterLocation::Query, &schema);
    }
}

impl<T: JsonSchema> OperationInput for axum::Json<T> {
    fn describe(operation: &mut OperationContext<'_>) {
        let schema = operation.request_schema::<T>();
        operation.set_request_body(RequestBody::json(schema));
    }
}

impl OperationInput for String {
    fn describe(operation: &mut OperationContext<'_>) {
        operation.set_request_body(RequestBody::with_content("text/plain", json!({"type": "string"})));
    }
}

impl<T: JsonSchema> OperationOutput for axum::Json<T> {
    fn describe(operation: &mut OperationContext<'_>) {
        let schema = operation.response_schema::<T>();
        operation.add_response("200", Response::json("Success.", schema));
    }
}

impl OperationOutput for &'static str {
    fn describe(operation: &mut OperationContext<'_>) {
        operation.add_response(
            "200",
            Response::with_content("Success.", "text/plain", json!({"type": "string"})),
        );
    }
}

impl OperationOutput for String {
    fn describe(operation: &mut OperationContext<'_>) {
        <&'static str as OperationOutput>::describe(operation);
    }
}

impl OperationOutput for () {
    fn describe(operation: &mut OperationContext<'_>) {
        operation.add_response("200", Response::new("Success, with an empty body."));
    }
}

impl<T: OperationOutput, E: OperationOutput> OperationOutput for Result<T, E> {
    fn describe(operation: &mut OperationContext<'_>) {
        T::describe(operation);
        E::describe(operation);
    }
}

/// An OpenAPI document.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OpenApi {
    /// [`OPENAPI_VERSION`].
    pub openapi: String,
    /// About the API.
    pub info: Info,
    /// Tags of the operations, sorted.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<Tag>,
    /// Path -> method key -> operation.
    pub paths: BTreeMap<String, BTreeMap<String, Operation>>,
    /// Reusable schemas.
    pub components: Components,
}

/// About the API.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Info {
    /// Name of the API.
    pub title: String,
    /// Version of the API (not of OpenAPI).
    pub version: String,
    /// What it does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl Info {
    /// An API without description.
    #[must_use]
    pub fn new(title: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            version: version.into(),
            description: None,
        }
    }

    /// Describe it.
    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// A tag.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Tag {
    /// Its name.
    pub name: String,
}

/// Reusable parts of the document.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Components {
    /// Schemas by name, referenced as `#/components/schemas/<name>`.
    pub schemas: BTreeMap<String, Value>,
}

/// One operation (a method on a path).
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    /// The handler's name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    /// First paragraph of the handler's doc comment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// The rest of it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Tags.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Path and query parameters.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub parameters: Vec<Parameter>,
    /// Request body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_body: Option<RequestBody>,
    /// Responses by status.
    pub responses: BTreeMap<String, Response>,
}

/// Where a parameter is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ParameterLocation {
    /// A `{param}` of the path.
    Path,
    /// The query string.
    Query,
    /// A header.
    Header,
    /// A cookie.
    Cookie,
}

/// A parameter.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Parameter {
    /// Its name.
    pub name: String,
    /// Where it is.
    #[serde(rename = "in")]
    pub location: ParameterLocation,
    /// Whether it must be present (always, for a path parameter).
    pub required: bool,
    /// What it is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Its schema.
    pub schema: Value,
}

/// A request body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RequestBody {
    /// Whether it must be present.
    pub required: bool,
    /// Media type -> content.
    pub content: BTreeMap<String, MediaType>,
}

impl RequestBody {
    /// A required JSON body.
    #[must_use]
    pub fn json(schema: Value) -> Self {
        Self::with_content("application/json", schema)
    }

    /// A required body of one media type.
    #[must_use]
    pub fn with_content(media_type: &str, schema: Value) -> Self {
        Self {
            required: true,
            content: BTreeMap::from([(media_type.to_owned(), MediaType { schema })]),
        }
    }
}

/// A response.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Response {
    /// What it means.
    pub description: String,
    /// Media type -> content; empty for an empty body.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub content: BTreeMap<String, MediaType>,
}

impl Response {
    /// A response without body.
    #[must_use]
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
            content: BTreeMap::new(),
        }
    }

    /// A JSON response.
    #[must_use]
    pub fn json(description: impl Into<String>, schema: Value) -> Self {
        Self::with_content(description, "application/json", schema)
    }

    /// A response of one media type.
    #[must_use]
    pub fn with_content(description: impl Into<String>, media_type: &str, schema: Value) -> Self {
        Self {
            description: description.into(),
            content: BTreeMap::from([(media_type.to_owned(), MediaType { schema })]),
        }
    }
}

/// The content of one media type.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MediaType {
    /// Its schema.
    pub schema: Value,
}
