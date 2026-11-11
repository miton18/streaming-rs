# streaming-rs

A stream-processing framework in Rust, in the spirit of Flink but smaller:
pipelines are DAGs of operators written in Rust, run as actors, with
backpressure, per-node metrics, event-time windows and exactly-once state.
It is multi-tenant, keeps its state in a Redis-protocol key-value store
(Redis, Clever Cloud Materia KV), and is built from day one to run under
[Moonpool](https://github.com/PierreZ/moonpool) deterministic simulation.

> **Status: early.** The engine, the control plane, the connectors and the UI
> work and are tested as described below. Read [Limits](#limits) before
> relying on it.

## Try it

```sh
cargo run --release -p streamd -- standalone
```

This starts a whole cluster in one process (in-memory KV, three workers
gossiping over loopback, two demonstration graphs fed by a generator) and
opens the web UI on <http://127.0.0.1:8080/>.

Things to try in the UI:

- open `acme / count-per-key`, click the `archive` node, set `latency_ms` to
  `400` and apply: the sink saturates, backpressure climbs up to the source
  and the node is flagged as the bottleneck;
- do the same on `warp10`, the sink behind the aggregate, with `15000` (more
  than the ten-second window): the emitting half of `count` waits for it at
  once, and half a minute later, three closed windows being held up, the
  source stops reading too;
- raise `rate_per_sec` on `events`;
- pause a node, pause or drain the graph;
- watch the per-key counts arrive in the *Résultats* tab every ten seconds.

## Concepts

**A graph is a description, not code.** It is a JSON document stored in the
KV: nodes referencing operators by name (`std/count@1`, `acme/decode@1`) and
edges. The operators themselves are Rust code compiled into the workers and
registered under the same reference. A graph is validated when submitted:
acyclic, every operator known to the cluster, types matching along each edge.

```json
{
  "tenant": "acme", "graph": "count-per-key",
  "nodes": [
    {"id": "events", "op": "std/pulsar-source@1",
     "config": {"service_url": "pulsar://broker:6650",
                "topic": "persistent://acme/prod/events",
                "subscription": "count-per-key"}},
    {"id": "by-key", "op": "std/pulsar-key@1"},
    {"id": "window", "op": "std/tumbling@1", "config": {"size": "5m"}},
    {"id": "count",  "op": "std/count@1"},
    {"id": "to-gts", "op": "std/count-to-gts@1", "config": {"class": "events.count"}},
    {"id": "warp10", "op": "std/warp10-sink@1",
     "config": {"endpoint": "https://warp10.example/api/v0/update",
                "token": {"secret": "warp10-write"}}}
  ],
  "edges": [["events", "by-key"], ["by-key", "window"], ["window", "count"],
            ["count", "to-gts"], ["to-gts", "warp10"]]
}
```

**Every node is an actor.** Actors are connected by bounded edges: a full edge
suspends its producer, which is how backpressure reaches the source, which
then stops reading. Each actor accounts its time as *busy*, *idle* (waiting
for input) or *backpressured* (waiting for room downstream). The control plane
names the bottleneck of a graph from those three numbers: the node that works
most of the time while what feeds it waits.

Closed windows wait in the store, not on an edge, so backpressure crosses an
aggregate differently: a slice stops reading when too many of the windows it
closed are still waiting to be emitted (`max_pending_windows`). A sink that
cannot keep up behind an aggregate thus ends up slowing the source down, and
the state waiting in the store stays bounded.

**Slices.** A graph is cut at its aggregate nodes. Each piece runs once per
input partition: `src.<node>.<partition>` runs everything from one partition
of a source up to the first aggregates; `agg.<node>` reads the closed windows
of an aggregate and runs everything downstream. A slice is the unit of
placement and of exactly-once. By default all the slices of a graph run on one
worker; the controller only moves a slice to another worker when the worker
reports being saturated (CPU or memory), one slice at a time.

**Exactly-once on one command.** The store only needs single-command
atomicity (no `MULTI`, no scripting), so the protocol also runs on Materia KV:

- a slice cuts its input into *epochs*; a barrier travels through the actors
  and, once every aggregate and sink has seen it, the epoch is committed with
  one `HSETNX` of field `c:<epoch>` in the slice's log. The field holds the
  source position, the watermark and every accumulator the epoch changed;
- `HSETNX` succeeds for one writer only: a second worker running the same
  slice loses the commit, is *fenced*, and restarts from the log. Membership
  and placement are therefore never needed for correctness;
- the source is acknowledged only after the commit;
- superseded log records are emptied in place rather than deleted, and fields
  are only deleted below a write-once *seal* that every committer re-reads
  after its `HSETNX`, so that a worker left far behind cannot revive an old
  epoch;
- accumulators are copied out of the log into one hash per window, by the
  instance that owns the slice and by it alone. Once a window is emitted its
  state is deleted at once; its index entry stays until no log record has
  mentioned the window for `gc_grace` (30 s by default), then state and entry
  go for good. A write still in flight from a worker that lost the slice thus
  cannot leave a window behind, provided a store command takes effect within
  a third of `gc_grace` or not at all, clock difference between workers
  included;
- sinks receive each element with a deterministic `EmitId` and must be
  idempotent on it: after a failure the same elements are written again with
  the same ids.

**Event time.** Windows close on watermarks (highest event time seen minus
`max_out_of_orderness`); a partition that is fully read and silent for
`idle_timeout` follows the wall clock so that windows still close. Records
arriving for a closed window are dropped and counted.

**Multi-tenant.** Every graph, actor, metric and KV key belongs to a tenant
(`t:<tenant>:...`); secrets are resolved per tenant. Workers are shared by all
tenants.

**Control plane in the KV.** Workers, the controller, the HTTP API and the UI
only exchange documents through the KV (see `streaming_core::keys`), plus a
light gossip for membership: each worker gossips a couple of hundred bytes
(identity and load), nothing else.

## Crates

| crate | role |
|---|---|
| `streaming-core` | contracts: ids, graph description and validation, operator traits and registry, KV trait, control-plane documents |
| `streaming-runtime` | the engine: actors, edges, epochs, commit log, window state, worker loop, controller and placement |
| `streaming-gossip` | Scuttlebutt gossip membership written against Moonpool providers |
| `streaming-kv-redis` | RESP client for Redis and Materia KV (`HSCAN` paging, size limits, optional TLS) |
| `streaming-pulsar` | Apache Pulsar source and sink on the [Magnetar](https://github.com/CleverCloud/magnetar) client |
| `streaming-warp10` | Warp 10 sink (`/api/v0/update`) |
| `streaming-api` | HTTP API and embedded web UI ([docs/http-api.md](docs/http-api.md)) |
| `streaming-sim` | simulation harness and the exactly-once tests |
| `streamd` | the daemon: `standalone` and `worker` |

## Running a real cluster

```sh
cargo build --release -p streamd --features pulsar,tls

streamd worker --id worker-1 --kv redis://:TOKEN@kv-host:6379 \
    --listen 0.0.0.0:8080 --gossip 0.0.0.0:7946 --gossip-advertise 10.0.0.1:7946
streamd worker --id worker-2 --kv redis://:TOKEN@kv-host:6379 \
    --gossip 0.0.0.0:7946 --gossip-advertise 10.0.0.2:7946 --seeds 10.0.0.1:7946
```

Use `rediss://` for TLS (Materia KV). Submit a graph with the API:

```sh
curl -X PUT http://10.0.0.1:8080/api/v1/tenants/acme/graphs/count-per-key \
     --data @count-per-key.json
```

Secrets referenced as `{"secret": "name"}` are read from the environment
variable `STREAMING_SECRET__<TENANT>__<NAME>`.

The API has no authentication: do not expose it publicly.

## Writing operators

The stock `streamd` only knows the `std/*` operators. Your own operators live
in your own worker binary, which is a few lines around `streaming-runtime`:

```rust
let mut registry = Registry::<TokioProviders>::standard();
streaming_runtime::builtin::register(&mut registry, kv.clone());

registry.register_map("acme/decode@1", "Decodes an event.", |bytes: &Vec<u8>| {
    serde_json::from_slice::<Event>(bytes).ok()          // None filters the record out
});
registry.register_key("acme/by-user@1", "Groups by user.", |event: &Event| event.user.clone());
registry.register_aggregator("acme/revenue@1", "Sums amounts.", |_: IgnoredAny| Ok(Revenue));

Worker::new(RuntimeEnv { providers, kv, clock, secrets, registry: Arc::new(registry) },
            WorkerConfig::new(WorkerId::new("worker-1")?))
    .run(shutdown)
    .await;
```

An aggregator implements `init`, `add`, `merge` and `finish`; `merge` is
required because the state of one key may be spread over several partitions.
Sources and sinks implement `SourceConnector` and `SinkConnector`. A graph
referencing an operator no worker knows is refused at submission.

## Simulation

Everything that does I/O is generic over `moonpool_core::Providers`, so the
real worker code runs under the simulator:

```sh
cargo test -p streaming-sim                       # a few dozen seeds
SIM_ITERATIONS=2000 cargo test --release -p streaming-sim
SIM_SEEDS=12685844343199869141 cargo test -p streaming-sim -- --nocapture   # replay one seed
```

The harness runs workers in processes that the simulator crashes and reboots,
against a source that redelivers and cuts batches anywhere, sinks that fail
halfway through a batch, and a KV whose commands are lost, applied without
their reply reaching the caller, applied long after the caller was told they
had failed (even after its death), or frozen for seconds. A driver then
checks, on a graph combining a windowed count, a stateless path, a diamond
and a second-level aggregation, that:

- every event was counted exactly once, on every path;
- the committed history of a slice never went backwards (an auditor reads the
  commit logs every 100 ms);
- nothing was acknowledged to the source before being committed;
- once everything is emitted, no window state is left in the store and the
  commit logs stayed small.

Seventeen hand-written scenarios: quiet cluster, reboots, gossip membership
over the simulated network, "every worker runs every slice" (three workers
fighting over each commit), windows wider than an epoch, allowed lateness,
sliding windows, an operator pausing, draining and re-submitting the graph, a
larger cluster, a slow store, eager idle watermarks, a silent partition, a
hostile mix of the above with three times the faults, a sink behind the
aggregate slower than the windows close, the tightest limit on pending
windows with workers fighting over every slice, long stalls, and a store
applying commands late on windows that close all the time. On top of them,
`exactly_once_in_random_configurations` draws every knob at random
(`SIM_CONFIGS=200` for more draws, `SIM_CONFIG_SEED=<n>` to replay one).

A simulation that passes says little unless it is known to fail when the
engine is wrong. `streaming_runtime::mutation` holds eight bugs, each one
precaution of the protocol removed (commit without fencing, commit below the
seal, acknowledge before commit, emit without checking ownership, forget a
window at once, restart without the watermark, restore without the pending
commits, empty inherited records without copying them out). They are only
compiled in with the `mutants` feature, and

```sh
cargo test --release -p streaming-sim --features mutants --test mutants -- --nocapture
```

plants them one at a time and requires the scenarios to notice every one.

The gossip, the KV client and the Warp 10 sink have their own simulation
tests. The Pulsar connector is tested against a real broker instead
(`PULSAR_E2E_URL=pulsar://localhost:6650 cargo test -p streaming-pulsar`).

`scripts/e2e-pulsar-redis.sh` runs the stock `streamd` on real
infrastructure: a Pulsar topic with three partitions, a real Redis for the
state, two workers found through gossip, the one running the pipeline killed
with `SIGKILL` halfway through, and a check that the counts per key equal
what was produced.

## Limits

- **Parallelism follows the source.** A node runs one actor per partition of
  the source feeding it. There is no keyed exchange between workers yet: the
  actors of one slice always share a worker, and scaling out means spreading
  slices. A node cannot be scaled independently of its source.
- **Placement** moves slices off a saturated worker but never brings them back
  together; load is the process's CPU and memory as read from `/proc`.
- **Backpressure crosses an aggregate by counting windows**, not records: a
  slice stops reading once `max_pending_windows` (16 by default) of the
  windows it closed are still waiting to be emitted. Until then a slow sink
  behind an aggregate only shows as backpressure on the emitting half of the
  aggregate, while closed windows wait in the KV.
- **Sinks must be idempotent** for the result to be exactly-once. Warp 10 is
  by nature. The Pulsar sink is at-least-once with the emit id in a message
  property, or effectively-once with broker deduplication under the
  conditions documented in `streaming-pulsar`.
- Messages written by the Pulsar sink carry a wrong *publish* time: Magnetar
  1.8's moonpool engine, which the connector runs on, anchors its wall clock
  at 2024-01-01. Their *event* time is right, and reading is not affected.
- **State lives in the KV**, one commit per epoch: throughput and state size
  are bounded by the store (Materia KV: 5 MB per value, 10 MB per command).
- **Aggregations only**: tumbling and sliding windows; no joins, no session
  windows. Accumulators must be mergeable.
- **Isolation between tenants is logical.** Operators are trusted code; there
  are no quotas and no fair scheduling yet. An operator that panics restarts
  its slice, not the process.
- A graph is planned once per version: a change in the partition count of a
  source is not followed. Changing a node's configuration restarts the
  graph's slices (state is kept).
- Not yet run against a real Materia KV or a real Warp 10.
