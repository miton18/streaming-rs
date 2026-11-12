# HTTP API (v1)

Served by every worker of an application started with `--listen` (crate
`streaming-api`). Everything is JSON. The API only reads and writes documents
in the KV (see `streaming_core::keys` and `streaming_core::control`): any
process holding a KV handle can serve it, and workers converge on what it
writes.

Graphs are not written through the API: they are the code of the
application, which every worker publishes into the KV when it starts
(`streaming_api::control::publish`). What the API changes is what is not
structure: run states, paused nodes and the configuration fields operators
expose. A graph the code no longer declares is an *orphan* (`"orphan": true`
in its summary and detail): it is stopped, its state kept, and every write on
it but its deletion answers `409` (`GRAPH_NOT_IN_CODE`).

## Errors

An error is answered with a 4xx/5xx status and this body:

```json
{
  "message": "unknown node `events` in graph `acme/count-per-key`",
  "code": "NODE_NOT_FOUND",
  "context": { "tenant": "acme", "graph": "count-per-key", "node": "events" }
}
```

- `code` is a constant (`UPPER_SNAKE_CASE`) naming the case, so that two
  answers sharing a status can be told apart (`IDENTIFIER_INVALID_CHARACTER`
  and `WIRING_SINK_HAS_OUTPUTS` are both `400`). A code never changes meaning.
- `context` maps names to the values the case is about (texts, numbers or
  lists of texts).
- `message` is for people, in English. It is always the template of the code
  with the values of `context` put in its `{placeholders}`, and every
  placeholder has its value: a client can write the message in another
  language from `code` and `context` alone (the web UI does, in French, from
  `ui/assets/js/errors-fr.js`). A list is written with its items separated by
  `, `.
- Nothing internal is echoed: no store key, address or driver message, no
  operator configuration (it may hold a secret). Those go to the server's
  log. Values the client sent are cut at 160 characters.

The codes, their status and template are listed in `crates/streaming-api/src/error.rs`
and in the description of the `ErrorBody` schema of the OpenAPI document. A
`405` (a known path, another method) is `METHOD_NOT_ALLOWED`, with the
methods the path takes in `context.allowed` and in the `Allow` header.

The machine-readable contract is the OpenAPI 3.2 document served at
`GET /api/v1/openapi.json` and committed as [`openapi.json`](openapi.json).
It is derived from the handlers themselves: each carries its method and path
in a `#[route(METHOD, "/path")]` attribute (crate `streaming-openapi`), and
the parameters, bodies and responses come from the types of its arguments and
of its return value. Schemas are JSON Schema 2020-12. A type whose input form
differs from its output form (fields with a default) is listed twice, the
input form as `<Name>Input`. The test suite checks every answer of the API
tests against the document, and that the committed copy is up to date.

## Read

### `GET /healthz`
`200 ok`.

### `GET /api/v1/openapi.json`
The OpenAPI document of this API.

### `GET /api/v1/cluster`
```json
{
  "epoch": 12,
  "nowMs": 1790000000000,
  "workers": [
    {
      "worker": "worker-a", "alive": true, "generation": 1790000000000,
      "heartbeatMs": 1790000000000,
      "load": {"cpu": 0.31, "memory": 0.12, "memoryBytes": 52428800},
      "actors": 14, "slices": 3, "apiAddr": "127.0.0.1:8080", "version": "0.1.0",
      "aside": [],
      "tenants": {"acme": {"actors": 7, "inPerSec": 1200.0, "busy": 0.4}}
    }
  ],
  "placement": {
    "version": 3, "computedBy": "worker-a", "computedMs": 1790000000000,
    "assignments": {"acme/count-per-key/src.events.0": "worker-a"}
  },
  "kv": {"keys": 120, "bytes": 34567, "reads": 1000, "writes": 2000}
}
```
`workers` comes from `cluster:members` (a worker is `alive` when its heartbeat is
younger than the member timeout, 5 s by default) joined with each worker's
`cluster:metrics:<worker>` snapshot (`tenants`). `aside` lists the graphs
(`tenant/graph`) a worker steps aside from: its code declares them otherwise
than the store does, or not at all, which is what the binary being replaced
does while a deployment is in progress; it runs none of their slices. `kv` is present only when the
embedding process provides KV statistics (standalone mode), otherwise `null`.

### `GET /api/v1/catalog`
`{"ops": [OpDescriptor, ...]}` sorted by reference, from `cluster:catalog`.
```json
{"op": "std/count@1", "kind": "aggregate", "input": "*",
 "output": "streaming_core::data::Windowed<u64>", "description": "..."}
```
Each operator may carry `params`, the parameters of its configuration, for
tools that build graphs: `{"name": "size", "kind": "duration", "required":
true, "default": null, "description": "..."}`. `kind` is `duration`,
`integer`, `number`, `text`, `boolean`, `secret` (a text or a
`{"secret": "<name>"}` reference) or `json` (any JSON value, such as an
object). A parameter restricted to a fixed set of texts also carries
`"choices": ["...", ...]`. An empty list means the operator takes no
parameter, or does not say.

An operator written for several types of elements (a sink of anything, a
count of anything) also carries `signatures`, one `{"input": "...",
"output": "..."}` per type it is registered for; `input` and `output` then
say what they have in common, `*` where they differ.

### `GET /api/v1/tenants`
```json
{"tenants": [{"tenant": "acme", "displayName": "Acme", "createdMs": 1790000000000,
              "graphs": 2, "actors": 14, "inPerSec": 8400.0}]}
```
The created tenants (`cluster:tenants`), plus any tenant the graph index names
without a record (written before tenants had to be created); those have no
`createdMs`. `displayName` is absent when none was given.

### `GET /api/v1/tenants/{tenant}`
One entry of the list above; `404` (`TENANT_NOT_FOUND`) for a tenant neither
created nor named by the index.

### `GET /api/v1/tenants/{tenant}/graphs`
A created tenant without graphs answers `{"graphs": []}`.
```json
{"graphs": [{
  "tenant": "acme", "graph": "count-per-key", "version": 3, "state": "running",
  "nodes": 8, "slices": 5, "actors": 22,
  "inPerSec": 8400.0, "outPerSec": 41.0,
  "bottleneck": {"node": "count", "kind": "aggregate", "busy": 0.97, "reason": "..."},
  "watermarkMs": 1790000000000, "updatedMs": 1790000000000, "error": null
}]}
```
`inPerSec` is the sum of the output rates of the source nodes, `outPerSec`
the sum of the output rates of the sink nodes. `error` is the plan error, if any.

### `GET /api/v1/tenants/{tenant}/graphs/{graph}`
```json
{
  "tenant": "acme", "graph": "count-per-key", "version": 3,
  "state": "running",
  "spec": { "...": "GraphSpec" },
  "plan": { "...": "GraphPlan, or null" },
  "desired": { "...": "DesiredState" },
  "bottleneck": {"node": "count", "kind": "aggregate", "busy": 0.97, "reason": "..."},
  "watermarkMs": 1790000000000,
  "nodes": [{
    "id": "count", "op": "std/count@1", "kind": "aggregate", "description": "...",
    "inputType": "*", "outputType": "streaming_core::data::Windowed<u64>",
    "config": {},
    "paused": false, "bottleneck": true,
    "state": "running",
    "actors": 3, "workers": ["worker-a"],
    "recordsIn": 123456, "recordsOut": 4567,
    "inPerSec": 8400.0, "outPerSec": 41.0,
    "busy": 0.97, "idle": 0.03, "backpressured": 0.0,
    "queueLen": 12, "queueCap": 16,
    "lateDropped": 0, "errors": 0, "lastError": null,
    "instances": [{ "...": "ActorSnapshot", "worker": "worker-a" }]
  }],
  "edges": [{"from": "events", "to": "decode", "fill": 0.75}],
  "slices": [{ "...": "SliceSnapshot", "worker": "worker-a", "assignedTo": "worker-a" }]
}
```

Node aggregation rules (one node has one actor per slice running it; an
aggregate node additionally has one `emitter` actor):

- `instances`: every `ActorSnapshot` whose `node` is this node and whose `kind`
  is not `commit`, from every *alive* worker's snapshot, each with a `worker` field.
- `state`: the worst instance state, in the order `failed`, `starting`,
  `paused`, `draining`, `running`, `stopped`; `"pending"` when there is no instance.
- `busy`, `backpressured`: each the highest value found over the instances,
  independently: an instance waiting for room downstream is never the busiest
  one (for an aggregate node, typically its `emitter` instance waits while its
  `aggregate` instances work). `idle`: `1 - busy - backpressured`, floored at 0.
- `waiting`: the part of `busy` spent waiting for an external system to answer
  (a sink's destination), highest over the instances. `busy` is occupation,
  not CPU: a sink busy mostly waiting is limited by its destination, and
  neither more CPU nor another worker helps. The bottleneck carries
  `waiting` and `limitedBy` (`"destination"`, `"work"`, or `"store"` for
  commits) accordingly.
- `recordsIn`, `inPerSec`: summed over instances whose kind is not `emitter`.
- `recordsOut`, `outPerSec`: summed over instances whose kind is not
  `aggregate`; if the node has no `emitter` instance, summed over all.
- `queueLen`, `queueCap`, `lateDropped`, `errors`: summed. `lastError`: any.
- `paused`: the node is in `desired.pausedNodes`.
- `edges[].fill`: `queueLen / queue_cap` of the destination node (0 when `queueCap` is 0).
- `slices`: every `SliceSnapshot` of the graph from alive workers, with the
  reporting `worker` and the worker the placement assigns it to (`assignedTo`,
  may be null). Slices of the plan nobody reports yet appear with
  `state: "starting"`, `worker: null`.
- `watermarkMs`: minimum `watermarkMs` over the reported `src.*` slices,
  null if any is missing.

Bottleneck rule: a node *limits* the graph when it works most of the time while
what feeds it is waiting for it.

1. Candidates are nodes with `busy >= 0.5` and `backpressured < 0.3` that have
   at least one (transitive) upstream node with `backpressured >= 0.3`.
2. The bottleneck is the candidate with the highest `busy`; ties go to the most
   downstream one. `kind` is the node kind.
3. Otherwise, if some `commit` actor of the graph has `busy >= 0.5` while the
   root node of its slice has `backpressured >= 0.3`, the bottleneck is
   `{"node": <root node>, "kind": "commit", ...}`: KV commits limit the graph.
4. Otherwise `null`.

### `GET /api/v1/tenants/{tenant}/graphs/{graph}/outputs/{node}?limit=200`
`{"rows": [OutputRow, ...]}`: rows written by a `std/kv-sink@1` node, most
recent `ts` first, then by `key`. `limit` defaults to 200, capped at 2000.
```json
{"id": "agg.count/1790000300000.1790000000000.4/0f...", "ts": 1790000000000,
 "key": "user-42", "value": {"windowStart": 1790000000000, "count": 17}}
```

### `GET /api/v1/tenants/{tenant}/graphs/{graph}/dot`
The topology as Graphviz DOT (`text/vnd.graphviz`).

## Write

Every write bumps `cluster:epoch`.

### `PUT /api/v1/tenants/{tenant}`
Creates a tenant. The tenants of an application are created by its workers
when they start; this creates one the code does not declare. Body: `{"displayName": "Acme"}` (optional, at most 100
characters, trimmed; `{}` for none). The record is written once with
`HSETNX` and never overwritten. Answers the record,
`{"tenant": "acme", "displayName": "Acme", "createdMs": 1790000000000}`, with
`201` when this request created it, `200` when it already existed with the
same display name (a harmless retry: nothing is written, the epoch is not
bumped), `409` (`TENANT_DISPLAY_NAME_CONFLICT`) when it exists with another
one. `400` for a display name that is too long (`DISPLAY_NAME_TOO_LONG`) or
holds a control character (`DISPLAY_NAME_INVALID_CHARACTER`).

### `DELETE /api/v1/tenants/{tenant}/graphs/{graph}`
Deletes an orphan graph and its state: marks the graph in `cluster:deleting`,
sets the desired state to `stopped` and the index entry to `deleting`; the
controller removes the graph's state, then the entry, then the mark. Answers `{"deleting": true}`. `409`
(`GRAPH_DECLARED_BY_CODE`) for a graph the code declares: it would come back,
empty, with the next worker that starts.

### `POST /api/v1/tenants/{tenant}/graphs/{graph}/state`
Body `{"state": "running" | "paused" | "draining" | "stopped"}`.

### `POST /api/v1/tenants/{tenant}/graphs/{graph}/nodes/{node}/pause`
### `POST /api/v1/tenants/{tenant}/graphs/{graph}/nodes/{node}/resume`
Add or remove the node from `desired.pausedNodes`. `404` for an unknown node.

### `PATCH /api/v1/tenants/{tenant}/graphs/{graph}/nodes/{node}/config`
Body: a JSON object of configuration fields, each laid over the
configuration the code gave the node; a `null` value goes back to the code's
value. Only the fields the node's operator lists among its `params` in the
catalogue can be set: another one is a `400` (`NODE_FIELD_NOT_EXPOSED`). The
fields are stored apart from the description
(`t:<tenant>:g:<graph>:overrides`), so that a new version of the code keeps
them. The resulting description is validated against the catalogue and
stored as a new version, on which the node's instances restart. Answers
`{"version": 5}`; `400` with the code of the rule broken (`OPERATOR_UNKNOWN`,
`TYPE_MISMATCH`, `WIRING_*`...) when it does not validate.

## Static UI

`GET /` and `GET /assets/*` serve the embedded web UI.
