//! Errors of the API: `{"message": ..., "code": ..., "context": {...}}`.
//!
//! - `code` is a constant naming the case, so that two answers sharing a
//!   status (an invalid identifier and an invalid description, both 400) can
//!   be told apart.
//! - `context` holds the values the case is about.
//! - `message` is for people, in English. It is always the template of the
//!   code with the values of the context put in its `{placeholders}`
//!   ([`render`]): a client can therefore write it in another language from
//!   `code` and `context` alone.
//!
//! Nothing internal goes into an answer: no store key, address or driver
//! message, no configuration value (it may hold a secret). Those are logged.

use crate::control::ControlError;
use axum::{
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Map, Value, json};
use streaming_core::{IdError, IdProblem, KvError, MAX_ID_LEN, ParamRefusal, SpecError, WiringRule};
use streaming_openapi::{OperationContext, OperationOutput};

/// One kind of error: its code, status and message template.
#[derive(Debug, PartialEq, Eq)]
pub struct ErrorCode {
    /// The constant clients match on.
    pub code: &'static str,
    /// HTTP status of the answer.
    pub status: StatusCode,
    /// English message; `{name}` is replaced by the context value `name`.
    pub template: &'static str,
}

macro_rules! codes {
    ($( $(#[$doc:meta])* $name:ident = $status:ident, $template:literal; )*) => {
        $(
            $(#[$doc])*
            pub const $name: ErrorCode = ErrorCode {
                code: stringify!($name),
                status: StatusCode::$status,
                template: $template,
            };
        )*

        /// Every error code, in declaration order.
        pub const ERROR_CODES: &[&ErrorCode] = &[$(&$name),*];
    };
}

codes! {
    /// An identifier in the path is empty.
    IDENTIFIER_EMPTY = BAD_REQUEST, "the {parameter} must not be empty";
    /// An identifier in the path is too long.
    IDENTIFIER_TOO_LONG = BAD_REQUEST, "the {parameter} `{value}` is longer than {maxLength} bytes";
    /// An identifier in the path has a character outside its alphabet.
    IDENTIFIER_INVALID_CHARACTER = BAD_REQUEST,
        "the {parameter} `{value}` may only contain letters, digits, `-` and `_`";
    /// The body could not be read.
    BODY_UNREADABLE = BAD_REQUEST, "the request body could not be read";
    /// The body is larger than the server accepts.
    BODY_TOO_LARGE = PAYLOAD_TOO_LARGE, "the request body is too large";
    /// The body is not JSON.
    BODY_NOT_JSON = BAD_REQUEST, "the request body is not valid JSON (line {line}, column {column})";
    /// The body has a field the expected document does not have.
    BODY_UNKNOWN_FIELD = BAD_REQUEST,
        "the request body has an unknown field `{field}` (line {line}, column {column})";
    /// The body lacks a mandatory field.
    BODY_MISSING_FIELD = BAD_REQUEST, "the request body lacks the field `{field}` (line {line}, column {column})";
    /// The body is JSON of another shape (a value of the wrong type, an
    /// invalid identifier...). `detail` is the parser's explanation, in
    /// English.
    BODY_INVALID = BAD_REQUEST, "the request body is invalid: {detail} (line {line}, column {column})";
    /// The `limit` query parameter is not a number.
    LIMIT_INVALID = BAD_REQUEST, "invalid limit `{value}`: expected a number of rows";
    /// The query string could not be parsed. `detail` is the parser's
    /// explanation, in English.
    QUERY_INVALID = BAD_REQUEST, "the query string is invalid: {detail}";
    /// The tenant was neither created nor has graphs.
    TENANT_NOT_FOUND = NOT_FOUND, "unknown tenant `{tenant}`";
    /// The tenant exists with another display name; it is never overwritten.
    TENANT_DISPLAY_NAME_CONFLICT = CONFLICT, "tenant `{tenant}` already exists with another display name";
    /// The display name of a tenant is too long.
    DISPLAY_NAME_TOO_LONG = BAD_REQUEST, "the display name is longer than {maxLength} characters";
    /// The display name of a tenant holds a control character.
    DISPLAY_NAME_INVALID_CHARACTER = BAD_REQUEST, "the display name contains a control character";
    /// The graph does not exist.
    GRAPH_NOT_FOUND = NOT_FOUND, "unknown graph `{tenant}/{graph}`";
    /// The graph has no such node.
    NODE_NOT_FOUND = NOT_FOUND, "unknown node `{node}` in graph `{tenant}/{graph}`";
    /// The graph is being deleted.
    GRAPH_BEING_DELETED = BAD_REQUEST, "graph `{tenant}/{graph}` is being deleted";
    /// The code of the application declares the graph, so the API cannot
    /// delete it.
    GRAPH_DECLARED_BY_CODE = CONFLICT,
        "graph `{tenant}/{graph}` is declared by the code of the application: remove it from the code to delete it";
    /// The code of the application no longer declares the graph: it is
    /// stopped and can only be deleted.
    GRAPH_NOT_IN_CODE = CONFLICT,
        "graph `{tenant}/{graph}` is no longer declared by the code of the application: it can only be deleted";
    /// The configuration of the node is not an object.
    NODE_CONFIG_NOT_OBJECT = BAD_REQUEST,
        "the configuration of node `{node}` in graph `{tenant}/{graph}` is not an object and cannot be patched";
    /// The operator of the node does not list the field among the parameters
    /// it exposes.
    NODE_FIELD_NOT_EXPOSED = BAD_REQUEST,
        "node `{node}` in graph `{tenant}/{graph}` does not expose the configuration field `{field}`";
    /// The value given to a field is not of the kind of its parameter
    /// (`integer`, `number`, `boolean`, `text`, `duration`, `secret`).
    NODE_FIELD_WRONG_KIND = BAD_REQUEST,
        "the configuration field `{field}` of node `{node}` in graph `{tenant}/{graph}` takes a value of kind `{kind}`";
    /// The value given to a field is none of the choices of its parameter.
    NODE_FIELD_NOT_A_CHOICE = BAD_REQUEST,
        "the configuration field `{field}` of node `{node}` in graph `{tenant}/{graph}` must be one of {choices}";
    /// The value given to a field is below the least one its parameter
    /// accepts.
    NODE_FIELD_BELOW_MIN = BAD_REQUEST,
        "the configuration field `{field}` of node `{node}` in graph `{tenant}/{graph}` must be at least {min}";
    /// The value given to a field is above the greatest one its parameter
    /// accepts.
    NODE_FIELD_ABOVE_MAX = BAD_REQUEST,
        "the configuration field `{field}` of node `{node}` in graph `{tenant}/{graph}` must be at most {max}";
    /// The operator refuses the configuration its node would have, for a
    /// reason the description of its parameters does not tell. `detail` is
    /// the operator's own explanation, in English.
    NODE_CONFIG_REFUSED = BAD_REQUEST,
        "the operator of node `{node}` in graph `{tenant}/{graph}` refuses this configuration: {detail}";
    /// The description has no node.
    GRAPH_EMPTY = BAD_REQUEST, "the graph has no node";
    /// Two nodes share an id.
    NODE_DUPLICATED = BAD_REQUEST, "node `{node}` is declared twice";
    /// An edge names a node that does not exist.
    EDGE_UNKNOWN_NODE = BAD_REQUEST, "edge `{from}` -> `{to}` references unknown node `{unknown}`";
    /// An edge is listed twice, or loops on one node.
    EDGE_INVALID = BAD_REQUEST, "edge `{from}` -> `{to}` is duplicated or a self-loop";
    /// The graph has a cycle.
    GRAPH_CYCLE = BAD_REQUEST, "the graph has a cycle through {nodes}";
    /// A node names an operator the cluster does not know.
    OPERATOR_UNKNOWN = BAD_REQUEST, "node `{node}` references unknown operator `{op}`";
    /// Two connected nodes disagree on the type between them.
    TYPE_MISMATCH = BAD_REQUEST,
        "edge `{from}` -> `{to}`: `{from}` produces `{produced}` but `{to}` expects `{expected}`";
    /// A source has an input.
    WIRING_SOURCE_HAS_INPUTS = BAD_REQUEST, "node `{node}` ({kind}): a source cannot have inputs";
    /// A source feeds nothing.
    WIRING_SOURCE_WITHOUT_OUTPUTS = BAD_REQUEST, "node `{node}` ({kind}): a source needs at least one output";
    /// Nothing feeds a sink.
    WIRING_SINK_WITHOUT_INPUTS = BAD_REQUEST, "node `{node}` ({kind}): a sink needs at least one input";
    /// A sink has an output.
    WIRING_SINK_HAS_OUTPUTS = BAD_REQUEST, "node `{node}` ({kind}): a sink cannot have outputs";
    /// A map or key_by node lacks an input or an output.
    WIRING_NEEDS_INPUT_AND_OUTPUT = BAD_REQUEST, "node `{node}` ({kind}): needs at least one input and one output";
    /// A key_by node feeds something else than windows.
    WIRING_KEY_BY_MUST_FEED_WINDOWS = BAD_REQUEST,
        "node `{node}` ({kind}): a key_by node must feed window nodes only";
    /// A window node is fed by something else than key_by nodes.
    WIRING_WINDOW_MUST_BE_FED_BY_KEY_BY = BAD_REQUEST,
        "node `{node}` ({kind}): a window node must be fed by key_by nodes only";
    /// A window node feeds something else than aggregates.
    WIRING_WINDOW_MUST_FEED_AGGREGATES = BAD_REQUEST,
        "node `{node}` ({kind}): a window node must feed aggregate nodes only";
    /// An aggregate is not fed by exactly one window node.
    WIRING_AGGREGATE_NEEDS_ONE_WINDOW = BAD_REQUEST,
        "node `{node}` ({kind}): an aggregate node needs exactly one input, a window node";
    /// An aggregate feeds nothing.
    WIRING_AGGREGATE_WITHOUT_OUTPUTS = BAD_REQUEST,
        "node `{node}` ({kind}): an aggregate node needs at least one output";
    /// A document is larger than the store accepts.
    DOCUMENT_TOO_LARGE = PAYLOAD_TOO_LARGE, "the document is {size} bytes, above the store's limit of {limit}";
    /// The store did not answer.
    STORE_UNAVAILABLE = SERVICE_UNAVAILABLE, "the store did not answer, try again later";
    /// A document of the store cannot be read.
    STORE_DOCUMENT_UNREADABLE = SERVICE_UNAVAILABLE, "a document of the store is unreadable";
    /// Nothing is served at this path of the API.
    ROUTE_NOT_FOUND = NOT_FOUND, "nothing is served at `{path}`";
    /// The path exists but does not take this method. `allowed` lists the
    /// methods it takes, as the `Allow` header does.
    METHOD_NOT_ALLOWED = METHOD_NOT_ALLOWED, "`{path}` does not take the method {method}, only {allowed}";
    /// This build does not embed the web UI.
    UI_NOT_EMBEDDED = NOT_FOUND, "the web UI is not embedded in this build";
}

/// The code of a wiring rule.
fn wiring_code(rule: WiringRule) -> &'static ErrorCode {
    match rule {
        WiringRule::SourceHasInputs => &WIRING_SOURCE_HAS_INPUTS,
        WiringRule::SourceWithoutOutputs => &WIRING_SOURCE_WITHOUT_OUTPUTS,
        WiringRule::SinkWithoutInputs => &WIRING_SINK_WITHOUT_INPUTS,
        WiringRule::SinkHasOutputs => &WIRING_SINK_HAS_OUTPUTS,
        WiringRule::NeedsInputAndOutput => &WIRING_NEEDS_INPUT_AND_OUTPUT,
        WiringRule::KeyByMustFeedWindows => &WIRING_KEY_BY_MUST_FEED_WINDOWS,
        WiringRule::WindowMustBeFedByKeyBy => &WIRING_WINDOW_MUST_BE_FED_BY_KEY_BY,
        WiringRule::WindowMustFeedAggregates => &WIRING_WINDOW_MUST_FEED_AGGREGATES,
        WiringRule::AggregateNeedsOneWindow => &WIRING_AGGREGATE_NEEDS_ONE_WINDOW,
        WiringRule::AggregateWithoutOutputs => &WIRING_AGGREGATE_WITHOUT_OUTPUTS,
    }
}

/// The message of `template` with the values of `context` in its
/// placeholders: a text as is, a list joined with `, `, anything else as
/// JSON. A placeholder without value stays as written.
#[must_use]
pub fn render(template: &str, context: &Map<String, Value>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        match after.find('}') {
            Some(close) => {
                let name = &after[..close];
                match context.get(name) {
                    Some(value) => out.push_str(&text_of(value)),
                    None => {
                        out.push('{');
                        out.push_str(name);
                        out.push('}');
                    }
                }
                rest = &after[close + 1..];
            }
            None => {
                out.push_str(&rest[open..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

fn text_of(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(items) => items.iter().map(text_of).collect::<Vec<_>>().join(", "),
        other => other.to_string(),
    }
}

/// The placeholders of a template, in order.
#[must_use]
pub fn placeholders(template: &str) -> Vec<&str> {
    template
        .split('{')
        .skip(1)
        .filter_map(|rest| rest.split_once('}').map(|(name, _)| name))
        .collect()
}

/// At most this many characters of a value sent by the client are echoed.
const ECHO_MAX: usize = 160;

/// A value the client sent, shortened for an answer.
fn echo(value: &str) -> String {
    if value.chars().count() <= ECHO_MAX {
        value.to_owned()
    } else {
        value.chars().take(ECHO_MAX).chain(std::iter::once('…')).collect()
    }
}

/// An error answered as `{"message", "code", "context"}`.
#[derive(Debug)]
pub struct ApiError {
    code: &'static ErrorCode,
    context: Map<String, Value>,
}

impl ApiError {
    /// An error without context.
    #[must_use]
    pub fn new(code: &'static ErrorCode) -> Self {
        Self {
            code,
            context: Map::new(),
        }
    }

    /// Add a value to the context.
    #[must_use]
    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.context.insert(key.to_owned(), value.into());
        self
    }

    /// Its code.
    #[must_use]
    pub fn code(&self) -> &'static ErrorCode { self.code }

    /// Its message, rendered from the code and the context.
    #[must_use]
    pub fn message(&self) -> String { render(self.code.template, &self.context) }

    /// A body that is not the expected JSON.
    pub(crate) fn body(error: &serde_json::Error) -> Self {
        let (line, column) = (error.line(), error.column());
        let detail = error.to_string();
        // serde_json appends " at line L column C" to its message.
        let detail = detail
            .rsplit_once(" at line ")
            .map_or(detail.as_str(), |(head, _)| head)
            .to_owned();
        let quoted = |prefix: &str| {
            detail
                .strip_prefix(prefix)
                .and_then(|rest| rest.split('`').next())
                .map(str::to_owned)
        };
        let error = if !error.is_data() {
            Self::new(&BODY_NOT_JSON)
        } else if let Some(field) = quoted("unknown field `") {
            Self::new(&BODY_UNKNOWN_FIELD).with("field", echo(&field))
        } else if let Some(field) = quoted("missing field `") {
            Self::new(&BODY_MISSING_FIELD).with("field", echo(&field))
        } else {
            Self::new(&BODY_INVALID).with("detail", echo(&detail))
        };
        error.with("line", line).with("column", column)
    }
}

/// The body of an error.
#[derive(Serialize)]
pub(crate) struct ErrorBody {
    message: String,
    code: &'static str,
    context: Map<String, Value>,
}

impl JsonSchema for ErrorBody {
    fn schema_name() -> std::borrow::Cow<'static, str> { "ErrorBody".into() }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let mut codes = String::new();
        for code in ERROR_CODES {
            codes.push_str(&format!(
                "\n- `{}` ({}): {}",
                code.code,
                code.status.as_u16(),
                code.template
            ));
        }
        let names: Vec<&str> = ERROR_CODES.iter().map(|code| code.code).collect();
        schemars::json_schema!({
            "type": "object",
            "description": format!(
                "An error. `message` is the template of `code` with the values of `context` in its \
                 `{{placeholders}}`, so that a client can write it in another language from `code` and \
                 `context` alone. The codes, their status and their template:{codes}"
            ),
            "properties": {
                "message": {"type": "string", "description": "What went wrong, for a person, in English."},
                "code": {"type": "string", "enum": names, "description": "The case, a constant for machines."},
                "context": {
                    "type": "object",
                    "additionalProperties": true,
                    "description": "The values the message is about, by placeholder name."
                }
            },
            "required": ["message", "code", "context"]
        })
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            message: self.message(),
            code: self.code.code,
            context: self.context,
        };
        (self.code.status, axum::Json(body)).into_response()
    }
}

impl OperationOutput for ApiError {
    fn describe(operation: &mut OperationContext<'_>) {
        let schema = operation.response_schema::<ErrorBody>();
        operation.add_response(
            "default",
            streaming_openapi::Response::json(
                "Error: `code` names the case (see `ErrorBody`), `context` holds its values.",
                schema,
            ),
        );
    }
}

/// The path parameter an identifier kind comes from (`tenant id` -> `tenant`).
fn parameter_of(kind: &str) -> &str { kind.strip_suffix(" id").unwrap_or(kind) }

impl From<IdError> for ApiError {
    fn from(error: IdError) -> Self {
        let parameter = parameter_of(error.kind).to_owned();
        let value = echo(&error.value);
        match error.reason {
            IdProblem::Empty => Self::new(&IDENTIFIER_EMPTY).with("parameter", parameter),
            IdProblem::TooLong => Self::new(&IDENTIFIER_TOO_LONG)
                .with("parameter", parameter)
                .with("value", value)
                .with("maxLength", MAX_ID_LEN),
            // Composite identifiers never come from a request; report them
            // like a bad character rather than inventing a code.
            IdProblem::InvalidCharacter | IdProblem::Malformed(_) | IdProblem::BadPartition => {
                Self::new(&IDENTIFIER_INVALID_CHARACTER)
                    .with("parameter", parameter)
                    .with("value", value)
            }
        }
    }
}

fn graph_context(error: ApiError, key: &streaming_core::GraphKey) -> ApiError {
    error
        .with("tenant", key.tenant.as_str())
        .with("graph", key.graph.as_str())
}

impl From<SpecError> for ApiError {
    fn from(error: SpecError) -> Self {
        let id = |node: &streaming_core::NodeId| node.as_str().to_owned();
        match error {
            SpecError::Empty => Self::new(&GRAPH_EMPTY),
            SpecError::DuplicateNode(node) => Self::new(&NODE_DUPLICATED).with("node", id(&node)),
            SpecError::UnknownNode { from, to, unknown } => Self::new(&EDGE_UNKNOWN_NODE)
                .with("from", id(&from))
                .with("to", id(&to))
                .with("unknown", id(&unknown)),
            SpecError::BadEdge { from, to } => Self::new(&EDGE_INVALID).with("from", id(&from)).with("to", id(&to)),
            SpecError::Cycle(nodes) => Self::new(&GRAPH_CYCLE).with("nodes", nodes.iter().map(id).collect::<Vec<_>>()),
            SpecError::UnknownOp { node, op } => Self::new(&OPERATOR_UNKNOWN)
                .with("node", id(&node))
                .with("op", op.as_str()),
            SpecError::Wiring { node, kind, rule } => Self::new(wiring_code(rule))
                .with("node", id(&node))
                .with("kind", kind.to_string()),
            SpecError::Type {
                from,
                to,
                produced,
                expected,
            } => Self::new(&TYPE_MISMATCH)
                .with("from", id(&from))
                .with("to", id(&to))
                .with("produced", produced.to_string())
                .with("expected", expected.to_string()),
        }
    }
}

impl From<KvError> for ApiError {
    fn from(error: KvError) -> Self {
        // The detail names keys and addresses: logged, not answered.
        tracing::warn!(error = %error, "kv failure");
        match error {
            KvError::TooLarge { size, limit } => Self::new(&DOCUMENT_TOO_LARGE)
                .with("size", json!(size))
                .with("limit", json!(limit)),
            KvError::Malformed { .. } => Self::new(&STORE_DOCUMENT_UNREADABLE),
            _ => Self::new(&STORE_UNAVAILABLE),
        }
    }
}

/// Middleware giving the router's bare `405` answers (a known path, another
/// method) the error body, with the methods of their `Allow` header.
pub async fn method_not_allowed(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let method = request.method().to_string();
    let path = echo(request.uri().path());
    let response = next.run(request).await;
    if response.status() != StatusCode::METHOD_NOT_ALLOWED || response.headers().contains_key(header::CONTENT_TYPE) {
        return response;
    }
    let allowed: Vec<String> = response
        .headers()
        .get(header::ALLOW)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|method| !method.is_empty())
        .map(str::to_owned)
        .collect();
    let allow = response.headers().get(header::ALLOW).cloned();
    let mut answer = ApiError::new(&METHOD_NOT_ALLOWED)
        .with("method", echo(&method))
        .with("path", path)
        .with("allowed", allowed)
        .into_response();
    if let Some(allow) = allow {
        answer.headers_mut().insert(header::ALLOW, allow);
    }
    answer
}

impl From<ControlError> for ApiError {
    fn from(error: ControlError) -> Self {
        match error {
            ControlError::GraphNotFound(key) => graph_context(Self::new(&GRAPH_NOT_FOUND), &key),
            ControlError::NodeNotFound { graph, node } => {
                graph_context(Self::new(&NODE_NOT_FOUND), &graph).with("node", node.as_str())
            }
            ControlError::GraphBeingDeleted(key) => graph_context(Self::new(&GRAPH_BEING_DELETED), &key),
            ControlError::GraphDeclaredByCode(key) => graph_context(Self::new(&GRAPH_DECLARED_BY_CODE), &key),
            ControlError::GraphNotInCode(key) => graph_context(Self::new(&GRAPH_NOT_IN_CODE), &key),
            ControlError::InvalidGraph(error) => error.into(),
            ControlError::ConfigNotAnObject { graph, node } => {
                graph_context(Self::new(&NODE_CONFIG_NOT_OBJECT), &graph).with("node", node.as_str())
            }
            ControlError::FieldNotExposed { graph, node, field } => {
                graph_context(Self::new(&NODE_FIELD_NOT_EXPOSED), &graph)
                    .with("node", node.as_str())
                    .with("field", field)
            }
            ControlError::FieldRefused {
                graph,
                node,
                field,
                refusal,
            } => {
                let (code, key, expected) = match *refusal {
                    ParamRefusal::Kind(kind) => (&NODE_FIELD_WRONG_KIND, "kind", Value::from(kind)),
                    ParamRefusal::Choice(choices) => (&NODE_FIELD_NOT_A_CHOICE, "choices", Value::from(choices)),
                    ParamRefusal::BelowMin(min) => (&NODE_FIELD_BELOW_MIN, "min", min),
                    ParamRefusal::AboveMax(max) => (&NODE_FIELD_ABOVE_MAX, "max", max),
                };
                graph_context(Self::new(code), &graph)
                    .with("node", node.as_str())
                    .with("field", field)
                    .with(key, expected)
            }
            ControlError::ConfigRefused { graph, node, detail } => {
                graph_context(Self::new(&NODE_CONFIG_REFUSED), &graph)
                    .with("node", node.as_str())
                    .with("detail", detail)
            }
            // Only a publication, never a request, meets an undeclared tenant.
            ControlError::TenantNotCreated(tenant) => Self::new(&TENANT_NOT_FOUND).with("tenant", tenant.as_str()),
            ControlError::TenantConflict(tenant) => {
                Self::new(&TENANT_DISPLAY_NAME_CONFLICT).with("tenant", tenant.as_str())
            }
            ControlError::DisplayNameTooLong => {
                Self::new(&DISPLAY_NAME_TOO_LONG).with("maxLength", crate::control::MAX_DISPLAY_NAME_CHARS)
            }
            ControlError::DisplayNameInvalidCharacter => Self::new(&DISPLAY_NAME_INVALID_CHARACTER),
            ControlError::Kv(error) => error.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn codes_are_unique_constants() {
        let mut seen = BTreeSet::new();
        for code in ERROR_CODES {
            assert!(seen.insert(code.code), "{} twice", code.code);
            assert!(
                code.code.chars().all(|c| c.is_ascii_uppercase() || c == '_'),
                "{}",
                code.code
            );
            assert!(
                code.status.is_client_error() || code.status.is_server_error(),
                "{}",
                code.code
            );
            for name in placeholders(code.template) {
                assert!(
                    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric()),
                    "{}: placeholder `{name}`",
                    code.code
                );
            }
        }
    }

    #[test]
    fn renders_texts_lists_and_numbers() {
        let context = json!({"node": "a", "nodes": ["a", "b"], "size": 12})
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(render("{node}: {nodes} ({size} B)", &context), "a: a, b (12 B)");
        assert_eq!(render("{missing} {", &context), "{missing} {");
        assert_eq!(placeholders("a {b} c {d}"), ["b", "d"]);
    }

    /// The point of the format: `message` comes from `code` and `context`.
    fn check(error: &ApiError) {
        let missing: Vec<&str> = placeholders(error.code.template)
            .into_iter()
            .filter(|name| !error.context.contains_key(*name))
            .collect();
        assert!(missing.is_empty(), "{}: no value for {missing:?}", error.code.code);
        let extra: Vec<&String> = error
            .context
            .keys()
            .filter(|key| !placeholders(error.code.template).contains(&key.as_str()))
            .collect();
        assert!(extra.is_empty(), "{}: unused context {extra:?}", error.code.code);
    }

    #[test]
    fn every_conversion_fills_its_template() {
        use streaming_core::{GraphKey, NodeId, OpKind, OpRef, TenantId, TypeTag};
        let node = || NodeId::new("n").unwrap();
        let key = GraphKey::new(TenantId::new("t").unwrap(), streaming_core::GraphId::new("g").unwrap());
        let mut errors: Vec<ApiError> = vec![
            TenantId::new("").unwrap_err().into(),
            TenantId::new("x".repeat(200)).unwrap_err().into(),
            TenantId::new("a b").unwrap_err().into(),
            SpecError::Empty.into(),
            SpecError::DuplicateNode(node()).into(),
            SpecError::UnknownNode {
                from: node(),
                to: node(),
                unknown: node(),
            }
            .into(),
            SpecError::BadEdge {
                from: node(),
                to: node(),
            }
            .into(),
            SpecError::Cycle(vec![node(), node()]).into(),
            SpecError::UnknownOp {
                node: node(),
                op: OpRef::new("a/b@1").unwrap(),
            }
            .into(),
            SpecError::Type {
                from: node(),
                to: node(),
                produced: TypeTag::Any,
                expected: TypeTag::Any,
            }
            .into(),
            KvError::TooLarge { size: 9, limit: 5 }.into(),
            KvError::Unavailable("redis at 10.0.0.1".into()).into(),
            KvError::Malformed {
                key: "t:secret".into(),
                reason: "x".into(),
            }
            .into(),
            ControlError::GraphNotFound(key.clone()).into(),
            ControlError::NodeNotFound {
                graph: key.clone(),
                node: node(),
            }
            .into(),
            ControlError::GraphBeingDeleted(key.clone()).into(),
            ControlError::GraphDeclaredByCode(key.clone()).into(),
            ControlError::GraphNotInCode(key.clone()).into(),
            ControlError::ConfigNotAnObject {
                graph: key.clone(),
                node: node(),
            }
            .into(),
            ControlError::FieldNotExposed {
                graph: key.clone(),
                node: node(),
                field: "size".into(),
            }
            .into(),
            ControlError::FieldRefused {
                graph: key.clone(),
                node: node(),
                field: "size".into(),
                refusal: Box::new(ParamRefusal::Kind("duration".into())),
            }
            .into(),
            ControlError::FieldRefused {
                graph: key.clone(),
                node: node(),
                field: "mode".into(),
                refusal: Box::new(ParamRefusal::Choice(vec!["fast".into(), "safe".into()])),
            }
            .into(),
            ControlError::FieldRefused {
                graph: key.clone(),
                node: node(),
                field: "size".into(),
                refusal: Box::new(ParamRefusal::BelowMin("1ms".into())),
            }
            .into(),
            ControlError::FieldRefused {
                graph: key.clone(),
                node: node(),
                field: "rate".into(),
                refusal: Box::new(ParamRefusal::AboveMax(64.into())),
            }
            .into(),
            ControlError::ConfigRefused {
                graph: key.clone(),
                node: node(),
                detail: "`slide` must not exceed `size`".into(),
            }
            .into(),
            ControlError::TenantNotCreated(key.tenant.clone()).into(),
            ControlError::TenantConflict(key.tenant.clone()).into(),
            ControlError::DisplayNameTooLong.into(),
            ControlError::DisplayNameInvalidCharacter.into(),
        ];
        for rule in [
            WiringRule::SourceHasInputs,
            WiringRule::SourceWithoutOutputs,
            WiringRule::SinkWithoutInputs,
            WiringRule::SinkHasOutputs,
            WiringRule::NeedsInputAndOutput,
            WiringRule::KeyByMustFeedWindows,
            WiringRule::WindowMustBeFedByKeyBy,
            WiringRule::WindowMustFeedAggregates,
            WiringRule::AggregateNeedsOneWindow,
            WiringRule::AggregateWithoutOutputs,
        ] {
            let error: ApiError = SpecError::Wiring {
                node: node(),
                kind: OpKind::Map,
                rule,
            }
            .into();
            // The English message of the core and of the API agree.
            assert!(error.message().ends_with(&rule.to_string()), "{}", error.message());
            errors.push(error);
        }
        for text in ["{", "{\"a\": 1}", "{\"tenant\": 3}", "[]"] {
            let error = serde_json::from_str::<streaming_core::GraphSpec>(text).unwrap_err();
            errors.push(ApiError::body(&error));
        }
        for error in &errors {
            check(error);
        }
        let used: BTreeSet<&str> = errors.iter().map(|error| error.code.code).collect();
        assert!(used.len() >= 30, "{used:?}");
    }

    #[test]
    fn leaks_nothing_internal() {
        let unavailable = ApiError::from(KvError::Unavailable("redis at 10.0.0.1:6379".into()));
        assert!(!unavailable.message().contains("10.0.0.1"));
        let malformed = ApiError::from(KvError::Malformed {
            key: "t:acme:g:x:spec".into(),
            reason: "bad".into(),
        });
        assert!(!malformed.message().contains("acme") && malformed.context.is_empty());
        let long = ApiError::from(TenantId::new("x".repeat(1000)).unwrap_err());
        assert!(long.message().len() < 400, "{}", long.message());
    }

    use streaming_core::TenantId;

    #[test]
    fn classifies_body_errors() {
        let error = |text: &str| ApiError::body(&serde_json::from_str::<streaming_core::GraphSpec>(text).unwrap_err());
        assert_eq!(error("{").code.code, "BODY_NOT_JSON");
        let unknown = error(r#"{"tenant": "t", "graph": "g", "nodes": [], "edges": [], "gc_grace": 1}"#);
        assert_eq!(unknown.code.code, "BODY_UNKNOWN_FIELD");
        assert_eq!(unknown.context["field"], "gc_grace");
        let missing = error(r#"{"tenant": "t", "graph": "g", "edges": []}"#);
        assert_eq!(missing.code.code, "BODY_MISSING_FIELD");
        assert_eq!(missing.context["field"], "nodes");
        let invalid = error(r#"{"tenant": 3}"#);
        assert_eq!(invalid.code.code, "BODY_INVALID");
        assert!(!invalid.context["detail"].as_str().unwrap().contains("at line"));
        assert_eq!(invalid.context["line"], 1);
    }

    /// The web UI writes every message in French from `code` and `context`:
    /// it must know every code, with the same placeholders.
    #[test]
    fn the_ui_translates_every_code() {
        let source = include_str!("../ui/assets/js/errors-fr.js");
        let mut french = std::collections::BTreeMap::new();
        for line in source.lines() {
            let Some((code, rest)) = line.trim().split_once(": ") else {
                continue;
            };
            if code.is_empty() || !code.bytes().all(|b| b.is_ascii_uppercase() || b == b'_') {
                continue;
            }
            let rest = rest.strip_suffix(',').unwrap_or(rest);
            let quote = rest.chars().next().unwrap();
            assert!(quote == '\'' || quote == '"', "{code}: {rest}");
            let template = rest.strip_prefix(quote).and_then(|r| r.strip_suffix(quote)).unwrap();
            assert!(french.insert(code, template).is_none(), "{code} translated twice");
        }
        for code in ERROR_CODES {
            let template = french
                .remove(code.code)
                .unwrap_or_else(|| panic!("{} has no French message", code.code));
            let mut expected = placeholders(code.template);
            let mut found = placeholders(template);
            expected.sort_unstable();
            found.sort_unstable();
            expected.dedup();
            found.dedup();
            assert_eq!(found, expected, "{}", code.code);
        }
        assert!(french.is_empty(), "unknown codes translated: {french:?}");
    }
}
