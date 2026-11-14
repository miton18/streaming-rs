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
cargo run --release -p streaming --example demo -- standalone
```

This runs the demonstration application
([`crates/streaming/examples/demo`](crates/streaming/examples/demo)) as a
whole cluster in one process (in-memory KV, three workers gossiping over
loopback, two graphs fed by a generator) and opens the web UI on
<http://127.0.0.1:8080/>.

Things to try in the UI:

- open `acme / count-per-key`, click the `archive` node, set `latencyMs` to
  `400` and apply: the sink saturates, backpressure climbs up to the source
  and the node is flagged as the bottleneck;
- do the same on `warp10`, the sink behind the aggregate, with `15000` (more
  than the ten-second window): the emitting half of `count` waits for it at
  once, and half a minute later, three closed windows being held up, the
  source stops reading too;
- raise `ratePerSec` on `events`, or change its `partitions`: slices join
  and leave the running pipeline;
- pause a node, pause or drain the graph;
- watch the per-key counts arrive in the *Résultats* tab every ten seconds.

## Concepts

**The code is the truth.** streaming-rs is an SDK: an application declares
its tenants, its operators and its graphs in Rust, and its binary is the
worker. Every worker of a cluster runs the same binary and, when it starts,
publishes the graphs of its code into the KV, where the controller, the other
workers and the HTTP API read them. A graph the code no longer has is
stopped and keeps its state (it is an *orphan*): it runs again on that state
if the code declares it again, and is deleted, state included, only when
someone asks for it through the API or the UI. A graph is validated before it is published: acyclic, every operator
known, types matching along each edge, the configuration of every node
accepted by its operator.

```rust
App::new()
    .tenant("acme", "Acme Corporation")
    .graph(count_per_key())                 // a GraphBuilder, see below
    .main()                                 // `standalone` or `worker`
```

What the API changes at run time is what is not structure: the run state of
a graph (running, paused, draining, stopped), paused nodes, and the
configuration fields an operator *exposes* (the parameters it describes in
the catalogue, such as a window's size or a sink's latency). Those are kept
apart in the KV, so a new version of the code keeps them; changing one
restarts the node's instances on a new version of the graph.

What the store holds of a graph is a JSON description: nodes referencing
operators by name (`std/count@1`, `acme/decode@1`) with their configuration,
and edges. The API and the UI show it as is:

```json
{
  "tenant": "acme", "graph": "count-per-key",
  "nodes": [
    {"id": "events", "op": "std/pulsar-source@1",
     "config": {"serviceUrl": "pulsar://broker:6650",
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
closed are still waiting to be emitted (`maxPendingWindows`). A sink that
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
  mentioned the window for `gcGrace` (30 s by default), then state and entry
  go for good. A write still in flight from a worker that lost the slice thus
  cannot leave a window behind, provided a store command takes effect within
  a third of `gcGrace` or not at all, clock difference between workers
  included;
- sinks receive each element with a deterministic `EmitId` and must be
  idempotent on it: after a failure the same elements are written again with
  the same ids.

**Event time.** Windows close on watermarks (highest event time seen minus
`maxOutOfOrderness`); a partition that is fully read and silent for
`idleTimeout` follows the wall clock so that windows still close. Records
arriving for a closed window are dropped and counted.

**Partitions come and go.** The controller asks every source for its number
of partitions again every `replan_interval` (a worker setting, 30 s). A new partition becomes a
new slice, under a new *generation* of the plan. The pipeline may have emitted
windows without it, so the slice first waits for an emitter of its generation
to have committed, which fences the emitters of the previous one, then starts
from the lowest watermark of the slices already there: what it reads for a
window that may have been emitted without it is late, dropped and counted,
rather than emitted a second time under the same ids with another value. A
partition that disappears *retires* its slice: the slice stops, the emitter
keeps reading what it committed until every window it holds is out; the
controller then closes its log, so that an instance still running somewhere
commits nothing more, and deletes its state two `gcGrace` later.

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
| `streaming-core` | contracts: ids, graph description and validation, the `Operator`, `Source`, `Sink` and `Transformer` traits and their pipes, the registry, KV trait, control-plane documents |
| `streaming-macros` | `#[derive(Operator)]` and `#[derive(Param)]` |
| `streaming-runtime` | the engine: actors, edges, epochs, commit log, window state, worker loop, controller and placement |
| `streaming-gossip` | Scuttlebutt gossip membership written against Moonpool providers |
| `streaming-kv-redis` | RESP client for Redis and Materia KV (`HSCAN` paging, size limits, optional TLS) |
| `streaming-pulsar` | Apache Pulsar source and sink on the [Magnetar](https://github.com/CleverCloud/magnetar) client, `PulsarKey`, `CountToPulsar` (feature `pulsar` of `streaming`) |
| `streaming-warp10` | Warp 10 sink (`/api/v0/update`), `CountToGts` (feature `warp10` of `streaming`) |
| `streaming-api` | HTTP API and embedded web UI ([docs/http-api.md](docs/http-api.md), [OpenAPI](docs/openapi.json)) |
| `streaming-openapi` | `#[route]` attribute: registers an axum handler and derives its OpenAPI description from its signature (macro in `streaming-openapi-macros`) |
| `streaming-sim` | simulation harness and the exactly-once tests |
| `streaming` | the SDK: `App` (tenants, operators, graphs, the `standalone` and `worker` commands), the stock operators, the demonstration application (`examples/demo`) |

## Connectors

`streaming` turns on no connector by default: an application names those it
uses as features, and each one adds its operators to the registry (the
connector and its glue), re-exported under the feature's name.

| feature | module | operators |
|---|---|---|
| `pulsar` | `streaming::pulsar` | `std/pulsar-source@1`, `std/pulsar-sink@1`, `std/pulsar-key@1`, `std/count-to-pulsar@1` |
| `warp10` | `streaming::warp10` | `std/warp10-sink@1`, `std/count-to-gts@1` |
| `tls` | | `rediss://` towards the store, and `https://` towards Warp 10 with `warp10` |

```toml
streaming = { git = "https://github.com/miton18/streaming-rs", features = ["pulsar", "warp10", "tls"] }
```

Any other source or sink is written the same way, as an operator
(`Source`, `Sink`) in its own crate with a `register` function; naming it in a
graph is enough for `App` to register it.

## Running a real cluster

Build your application (a crate depending on `streaming`, with the features
of the connectors it uses) and run the same binary on every machine:

```sh
my-app worker --id worker-1 --kv redis://:TOKEN@kv-host:6379 \
    --listen 0.0.0.0:8080 --gossip 0.0.0.0:7946 --gossip-advertise 10.0.0.1:7946
my-app worker --id worker-2 --kv redis://:TOKEN@kv-host:6379 \
    --listen 0.0.0.0:8080 --gossip 0.0.0.0:7946 --gossip-advertise 10.0.0.2:7946 --seeds 10.0.0.1:7946
```

Use `rediss://` for TLS (Materia KV). Each worker publishes the tenants and
graphs of the code when it starts and prints what changed; every worker
started with `--listen` serves the HTTP API and the web UI. Changing a graph
means deploying a new version of the binary. While a deployment replaces the
workers, old and new binaries run side by side. Each graph remembers the
declarations it has had, by fingerprint (its settings, nodes, operators,
configuration and edges): the first worker of the new binary publishes the
new declaration, and from then on the workers of the old one step aside from
that graph. They run none of its slices, and if one restarts it publishes
nothing for it. The cluster view shows which graphs a worker steps aside
from. Going back to an earlier version works the same way, with one
difference: a declaration the graph has already had only takes over once no
worker runs what replaced it, a few seconds after the last of them stops. No
state is deleted by a deployment.

Secrets referenced as `{"secret": "name"}` are read from the environment
variable `STREAMING_SECRET__<TENANT>__<NAME>`.

The API has no authentication: do not expose it publicly.

## Writing operators

[docs/operators.md](docs/operators.md) describes every kind of operator, its
contract, and the operators the project ships.

An operator is a structure, and one trait saying what it does.

The structure is its configuration: `#[derive(Operator)]` names it
(`#[operator("acme/thing@1")]`), takes the line the catalogue shows from its
documentation comment, and turns the fields marked `#[param]` into the
parameters the API and the UI may change, with their default value and their
bounds. A field without `#[param]` is fixed by the code. The stored
configuration is the structure as JSON (camelCase names, unknown fields
refused), so what the code declares is what a worker runs.

```rust
/// Synthetic events, as JSON bytes.
#[derive(Debug, Clone, Operator)]
#[operator("acme/generator@1")]
pub struct Generator {
    /// Number of partitions.
    #[param(default = 4, min = 1, max = 64)]
    pub partitions: u32,
    /// Events per second.
    #[param(default = 2000, max = 200000)]
    pub rate_per_sec: u64,
}
```

A parameter is of any type implementing `Param`: integers, floats, `bool`,
`String`, `Duration` (`"5m"`), `Secret` (the text, or `{"secret": "name"}`,
revealed by `ctx.reveal`), `serde_json::Value`, `Option` of these, and the
enumerations deriving `Param` (shown as a list of choices). `check = path`
in `#[operator(...)]` names a function refusing what the bounds cannot say.
The API refuses what the operator would: the catalogue carries the kind, the
choices and the bounds of each parameter, and the worker serving the API asks
the operator itself, `check` included, before a configuration is stored.

**A source** is started once per partition, with a pipe to push into:

```rust
impl Source for Generator {
    type Out = Vec<u8>;

    async fn partitions<P: Providers>(&self, _: &Context<P>) -> Result<u32, OpError> { Ok(self.partitions) }

    async fn start<P: Providers>(
        &self, ctx: &Context<P>, partition: u32, after: Option<Position>, out: SourcePipe<Vec<u8>>,
    ) -> Result<(), OpError> {
        let mut next = after.map_or(0, |position| position.2 + 1);
        loop {
            // Waits while downstream is full: this is the backpressure.
            out.send(Position(0, 0, next), ctx.clock.now_ms(), event(next)).await?;
            next += 1;
        }
    }
}
```

`after` is the last position the engine committed: the source resumes behind
it. `out.committed()` gives the positions the engine commits, for a source
that acknowledges to a broker, and `out.end(..)` says where the partition
ends, which is how the engine tells an idle partition from a reader that
lags. A source built on a connector that already exists hands it the
partition with `out.read(&connector, partition, after)`.

**A sink** receives a pipe of batches, and confirms each one:

```rust
impl Sink<Event> for Archive {
    async fn start<P: Providers>(&self, ctx: &Context<P>, mut input: SinkPipe<Event>) -> Result<(), OpError> {
        while let Some(batch) = input.next().await {
            for element in batch.iter() { /* element.id, element.value */ }
            batch.done();               // or batch.failed("why")
        }
        Ok(())
    }
}
```

A batch that is not confirmed (the sink said it failed, asked for the next
one, returned, or the worker died) comes back with the same emit ids: the
destination must be idempotent on them. `impl<T> Sink<T> for Archive` makes a
sink of any type.

**Everything else is a transformer.** The simplest are plain functions,
which know nothing of pipes: a `StatelessTransformer` turns each element into
exactly one other, a `StatelessFilterMap` into another or none.

```rust
impl StatelessTransformer<u64> for Scale {
    type Out = u64;
    fn apply(&self, value: &u64) -> u64 { value * self.factor }
}

impl StatelessFilterMap<Vec<u8>> for Decode {
    type Out = Event;
    fn apply(&self, bytes: &Vec<u8>) -> Option<Event> { serde_json::from_slice(bytes).ok() }
}
```

`graph.map(upstream, "scale", Scale { factor: 3 })` and
`graph.filter_map(upstream, "decode", Decode)` make nodes of them. The node is
an ordinary map, registered, stored and shown under the operator's own
reference and configuration; its parameters, bounds and `check` hold as for
any operator. The operator must be `Clone`. Outside a `GraphBuilder`, the
wrappers `Stateless(op)` and `StatelessFilter(op)` make `Transformer`s of
them.

`json_decoder!` declares such an operator for JSON, bytes or text, decoded
into a type of the application's (what does not decode is dropped):

```rust
json_decoder! {
    /// Decodes a JSON event.
    pub struct Decode("demo/decode@1") -> Event;
}
```

Any other transformer takes a pipe and returns a pipe:

```rust
impl Transformer<Event> for ByUser {
    type Out = Keyed<Event>;
    fn transform(&self, input: Pipe<Event>) -> Pipe<Keyed<Event>> { input.key_by(|event| event.user.clone()) }
}
```

`map`, `filter`, `filter_map` and `flat_map` may follow one another; `key_by`
gives a `Pipe<Keyed<T>>`, on which `window` gives a `Pipe<Windows<T>>`, on
which `aggregate(init, add, merge)` gives a `Pipe<Windowed<A>>`. `merge` is
required because the state of one key may be spread over several partitions.
A transformer is one node of the graph, hence one of these steps (several
maps count as one): the engine cuts a graph into slices at its aggregates,
and a key, a window and an aggregate are three nodes. The types are checked
by the compiler: a window on a pipe without key does not build.

## Building graphs in Rust

`GraphBuilder` connects typed nodes, and registers the operators it names:

```rust
let mut graph = GraphBuilder::new("acme", "count-per-key").settings(GraphSettings {
    max_pending_windows: 3,
    ..GraphSettings::default()
});
let events = graph.source("events", Generator { partitions: 4, rate_per_sec: 2_000 });
let decoded = graph.filter_map(events, "decode", Decode);
let by_user = graph.then(decoded, "by-user", ByUser);
let windows = graph.then(by_user, "window", Tumbling::of(Duration::from_secs(300)));
let counts = graph.then(windows, "count", Count);
let rows = graph.then(counts, "to-row", CountToJson);
graph.sink(rows, "results", KvSink { retain: 400 });
graph.sink(decoded, "archive", Archive);                // any DAG
```

Two shapes have their own method. `graph.union([a, b], "all")` brings
together nodes carrying one type (`std/union@1`). `graph.split(upstream,
"check", Check, ["valid", "invalid"])` sends each element to one branch with
an operator implementing `Split` (`fn branch(&self, &In) -> usize`, an index
into the names; an index with no branch drops the element), evaluated once
per element; it returns one node per branch, `check-valid` and
`check-invalid` (`std/branch@1`), carrying the type of `upstream`:

```rust
let [valid, invalid] = graph.split(decoded, "check", Check, ["valid", "invalid"]);
graph.sink(invalid, "rejects", Archive);
let all = graph.union([valid, other], "all");
```

`source`, `map`, `filter_map`, `union`, `split`, `then` and `sink` only accept an operator written for what flows
out of the node it is connected to. The application checks the graph when it
starts (wiring, types, the configuration of every node against its operator)
and refuses to run otherwise. The `std/*` operators have their types
<<<<<<< HEAD
(`Tumbling`, `Sliding`, `Count`, `Identity`, `Union`, `Flatten`, `KvSink`, `Warp10Sink`, and in
`streaming::stock` `CountToJson`, `CountToGts` and the Pulsar nodes);
=======
(`Tumbling`, `Sliding`, `Count`, `Identity`, `KvSink`, `CountToJson` in
`streaming::stock`, and those of the connectors, see below);
`crates/streaming/examples/demo/graphs.rs` builds the demonstration graphs
this way.

Below these traits, the engine drives `SourceConnector`, `SinkConnector` and
type-erased functions held by a `Registry`. `App::operators(|registry| ...)`
still registers operators at that level (`register_map`, `register_source`,
...), and `graph.node(id, operator)` names one of them without saying what
flows.

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
- no slice ever committed a change to a window already emitted (every commit
  is looked at the instant it takes effect);
- once everything is emitted, no window state is left in the store and the
  commit logs stayed small.

Nineteen hand-written scenarios: quiet cluster, reboots, gossip membership
over the simulated network, "every worker runs every slice" (three workers
fighting over each commit), windows wider than an epoch, allowed lateness,
sliding windows, an operator pausing, draining and re-submitting the graph, a
larger cluster, a slow store, eager idle watermarks, a silent partition, a
hostile mix of the above with three times the faults, a sink behind the
aggregate slower than the windows close, the tightest limit on pending
windows with workers fighting over every slice, long stalls, a store applying
commands late on windows that close all the time, and a source that gains a
partition then loses one while events flow, with and without a fight. On top
of them,
`exactly_once_in_random_configurations` draws every knob at random
(`SIM_CONFIGS=200` for more draws, `SIM_CONFIG_SEED=<n>` to replay one).

A simulation that passes says little unless it is known to fail when the
engine is wrong. `streaming_runtime::mutation` holds ten bugs, each one
precaution of the protocol removed (commit without fencing, commit below the
seal, acknowledge before commit, emit without checking ownership, forget a
window at once, restart without the watermark, restore without the pending
commits, empty inherited records without copying them out, join a running
pipeline without its watermark, forget the slices of partitions that are
gone). They are only compiled in with the `mutants` feature, and

```sh
cargo test --release -p streaming-sim --features mutants --test mutants -- --nocapture
```

plants them one at a time and requires the scenarios to notice every one.

The gossip, the KV client and the Warp 10 sink have their own simulation
tests. The Pulsar connector is tested against a real broker instead
(`PULSAR_E2E_URL=pulsar://localhost:6650 cargo test -p streaming-pulsar`).

`scripts/e2e-pulsar-redis.sh` runs an application (example `pulsar-e2e`) on
real infrastructure: a Pulsar topic with three partitions, a real Redis for the
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
  slice stops reading once `maxPendingWindows` (16 by default) of the
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
- Events a new partition receives before the pipeline reads it may be late
  by then: budget `allowedLateness` for `replan_interval` if that matters.
  A partition removed and later added again under the same index resumes
  from the state of the old one (Pulsar only ever adds partitions).
- Changing a node's configuration restarts the graph's slices (state is
  kept). A change that makes the state meaningless, such as a window's size,
  is not told apart yet: the state already accumulated is kept as it is.
- A graph changes only with a new version of the application. During a
  deployment, an old worker that restarts publishes its graphs again.
- Not yet run against a real Materia KV or a real Warp 10.
