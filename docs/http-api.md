# HTTP API (v1)

Served by `streamd` (crate `streaming-api`). Everything is JSON. The API only
reads and writes documents in the KV (see `streaming_core::keys` and
`streaming_core::control`): any process holding a KV handle can serve it, and
workers converge on what it writes.

Errors are `{"error": "<message>"}` with a 4xx/5xx status.

## Read

### `GET /healthz`
`200 ok`.

### `GET /api/v1/cluster`
```json
{
  "epoch": 12,
  "now_ms": 1790000000000,
  "workers": [
    {
      "worker": "worker-a", "alive": true, "generation": 1790000000000,
      "heartbeat_ms": 1790000000000,
      "load": {"cpu": 0.31, "memory": 0.12, "memory_bytes": 52428800},
      "actors": 14, "slices": 3, "api_addr": "127.0.0.1:8080", "version": "0.1.0",
      "tenants": {"acme": {"actors": 7, "in_per_sec": 1200.0, "busy": 0.4}}
    }
  ],
  "placement": {
    "version": 3, "computed_by": "worker-a", "computed_ms": 1790000000000,
    "assignments": {"acme/count-per-key/src.events.0": "worker-a"}
  },
  "kv": {"keys": 120, "bytes": 34567, "reads": 1000, "writes": 2000}
}
```
`workers` comes from `cluster:members` (a worker is `alive` when its heartbeat is
younger than the member timeout, 5 s by default) joined with each worker's
`cluster:metrics:<worker>` snapshot (`tenants`). `kv` is present only when the
embedding process provides KV statistics (standalone mode), otherwise `null`.

### `GET /api/v1/catalog`
`{"ops": [OpDescriptor, ...]}` sorted by reference, from `cluster:catalog`.
```json
{"op": "std/count@1", "kind": "aggregate", "input": "*",
 "output": "streaming_core::data::Windowed<u64>", "description": "..."}
```

### `GET /api/v1/tenants`
```json
{"tenants": [{"tenant": "acme", "graphs": 2, "actors": 14, "in_per_sec": 8400.0}]}
```

### `GET /api/v1/tenants/{tenant}/graphs`
```json
{"graphs": [{
  "tenant": "acme", "graph": "count-per-key", "version": 3, "state": "running",
  "nodes": 8, "slices": 5, "actors": 22,
  "in_per_sec": 8400.0, "out_per_sec": 41.0,
  "bottleneck": {"node": "count", "kind": "aggregate", "busy": 0.97, "reason": "..."},
  "watermark_ms": 1790000000000, "updated_ms": 1790000000000, "error": null
}]}
```
`in_per_sec` is the sum of the output rates of the source nodes, `out_per_sec`
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
  "watermark_ms": 1790000000000,
  "nodes": [{
    "id": "count", "op": "std/count@1", "kind": "aggregate", "description": "...",
    "input_type": "*", "output_type": "streaming_core::data::Windowed<u64>",
    "config": {},
    "paused": false, "bottleneck": true,
    "state": "running",
    "actors": 3, "workers": ["worker-a"],
    "records_in": 123456, "records_out": 4567,
    "in_per_sec": 8400.0, "out_per_sec": 41.0,
    "busy": 0.97, "idle": 0.03, "backpressured": 0.0,
    "queue_len": 12, "queue_cap": 16,
    "late_dropped": 0, "errors": 0, "last_error": null,
    "instances": [{ "...": "ActorSnapshot", "worker": "worker-a" }]
  }],
  "edges": [{"from": "events", "to": "decode", "fill": 0.75}],
  "slices": [{ "...": "SliceSnapshot", "worker": "worker-a", "assigned_to": "worker-a" }]
}
```

Node aggregation rules (one node has one actor per slice running it; an
aggregate node additionally has one `emitter` actor):

- `instances`: every `ActorSnapshot` whose `node` is this node and whose `kind`
  is not `commit`, from every *alive* worker's snapshot, each with a `worker` field.
- `state`: the worst instance state, in the order `failed`, `starting`,
  `paused`, `draining`, `running`, `stopped`; `"pending"` when there is no instance.
- `busy`, `idle`, `backpressured`: those of the instance with the highest `busy`
  (the hot instance decides whether the node limits the graph).
- `records_in`, `in_per_sec`: summed over instances whose kind is not `emitter`.
- `records_out`, `out_per_sec`: summed over instances whose kind is not
  `aggregate`; if the node has no `emitter` instance, summed over all.
- `queue_len`, `queue_cap`, `late_dropped`, `errors`: summed. `last_error`: any.
- `paused`: the node is in `desired.paused_nodes`.
- `edges[].fill`: `queue_len / queue_cap` of the destination node (0 when `queue_cap` is 0).
- `slices`: every `SliceSnapshot` of the graph from alive workers, with the
  reporting `worker` and the worker the placement assigns it to (`assigned_to`,
  may be null). Slices of the plan nobody reports yet appear with
  `state: "starting"`, `worker: null`.
- `watermark_ms`: minimum `watermark_ms` over the reported `src.*` slices,
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
 "key": "user-42", "value": {"window_start": 1790000000000, "count": 17}}
```

### `GET /api/v1/tenants/{tenant}/graphs/{graph}/dot`
The topology as Graphviz DOT (`text/vnd.graphviz`).

## Write

Every write bumps `cluster:epoch`.

### `PUT /api/v1/tenants/{tenant}/graphs/{graph}`
Body: a `GraphSpec`. `tenant` and `graph` in the body must match the path. The
description is validated against the catalog (`Topology::build`); on success it
is stored with `version = previous + 1`, the index entry is updated and the
desired state is created as `running` if absent. Answers `{"version": 4}`.
`400` with the validation message otherwise.

### `DELETE /api/v1/tenants/{tenant}/graphs/{graph}`
Sets the desired state to `stopped` and marks the index entry `deleting`. The
controller removes the graph's state, then the entry.

### `POST /api/v1/tenants/{tenant}/graphs/{graph}/state`
Body `{"state": "running" | "paused" | "draining" | "stopped"}`.

### `POST /api/v1/tenants/{tenant}/graphs/{graph}/nodes/{node}/pause`
### `POST /api/v1/tenants/{tenant}/graphs/{graph}/nodes/{node}/resume`
Add or remove the node from `desired.paused_nodes`. `404` for an unknown node.

### `PATCH /api/v1/tenants/{tenant}/graphs/{graph}/nodes/{node}/config`
Body: a JSON object, shallow-merged into the node configuration (a `null`
value removes the field). The resulting description is validated and stored as
a new version, exactly like `PUT`.

## Static UI

`GET /` and `GET /assets/*` serve the embedded web UI.
