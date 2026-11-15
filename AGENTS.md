# streaming-rs — handover for agents

A Rust streaming framework, "like Flink but simpler", built for one first use
case: **Pulsar source → 5-minute windows → count per key → flush elsewhere
(Pulsar or Warp 10)**, exactly-once, with state in a Redis-protocol store
(Materia KV). Everything is simulated deterministically with Moonpool.

Read `README.md` for the user-facing description and `docs/http-api.md` for
the HTTP contract. This file holds what they do not: the decisions, the
invariants, how to verify, and what is still open.

**Status.** `main` builds, lints and passes every test. A standalone
demonstrator (`cargo run -p streaming --example demo -- standalone`) runs on
the owner's workstation. Nothing
is deployed in production; the on-disk formats may still change freely.

## Working with the owner

- Answer in **French**. Code, comments, commit messages, `README.md` and this
  file are in English; the web UI is in French.
- Work is pushed **directly to `main`** (the repository started empty). No
  pull request unless asked. The licence is MIT (the owner's choice,
  2026-11-15). The first two choices
  were stated to the owner and never explicitly confirmed: ask before
  changing them.
- The owner's standard: *"if the whole simulation passes on a project this
  young, it is not severe enough"*. A green run is not an argument. Measure
  the harness (mutation test), then trust it.
- Error messages are for people: never the `Debug` form of a value in a log or
  on stderr (`App::main` reports fatal errors as `error: <Display>`).
- Never write a model identifier in commits, code or documents.

## What the owner asked for, in order

Code, not SQL. Pluggable persistent store usable from several nodes.
**Exactly-once**. No Lua, full Rust, Moonpool simulation from day one. The
destination is a `Sink` trait (Pulsar and Warp 10 `/update`). The Pulsar
driver is **Magnetar**, not pulsar-rs. Every element (source, sink, map,
window, aggregate) is pilotable and observable; the bottleneck must be
identifiable; backpressure. The framework stays agnostic (nothing about
"windowed (key, count)" in the `Sink` trait). Pipelines are **DAGs**. Each
node is an actor so that it can be distributed; split across servers only
when metrics show saturation, colocate small pipelines, ideally
self-managed. Membership by gossip, light, re-implemented (Scuttlebutt) so
that it is simulable; graph descriptions live in the store, not in gossip.
A web UI in the owner's "Nocturne" design system. **Multi-tenant**
everywhere; servers shared by all tenants. The store holds *references* to
operators (`std/count@1`), the code lives in the binary. Then, during the
last session: backpressure must reach the source across an aggregate;
simulate Pulsar partitions being added and removed; tell a sink slowed by
its destination from one that lacks CPU; a UI to design a pipeline.

**Then the owner reversed the graph model (2026-11-12): the code is the
truth.** streaming-rs is an SDK, not a daemon: an application declares its
tenants, operators and graphs in Rust (`streaming::App`), one binary holds
every graph of a cluster and every worker runs the same one (modulo the time
a Clever Cloud deployment takes). Each worker publishes the graphs of its code
into the store when it starts; the store keeps state, run states, paused
nodes and the configuration fields operators *expose* (their `ParamSpec`s),
which the API and the UI may change, a change restarting the node. Creating
and editing graphs through the API or the UI is gone (the designer too);
deleting only applies to a graph the code no longer declares. The API and the UI are served by every worker started with `--listen`.
Two binaries run side by side while a deployment is in progress: each graph
keeps the list of its declarations by **fingerprint**, and a binary the store
has moved on from steps aside from that graph (protocol, points 12 and 13).
The owner confirmed that a graph leaving the code is stopped and keeps its
state, deletion being explicit. Agreed for
later, telling apart exposed parameters that break the state (a window's
size) from those a restart absorbs. The comparison with Flink, Beam, Kafka
Streams, Temporal and others that led there is a Claude Doc of the project,
"Graphes : représentation stockée ou code ?".

**Then the owner reshaped the SDK (2026-11-12).** The `Operator` trait stays
("it gives the thing a name"). Two traits require it: `Source`, whose `start`
is handed a pipe to push into, and `Sink`, whose `start` receives a pipe;
everything else is a `Transformer`, which takes a pipe and returns a pipe.
Parameters are real fields of the operator's structure, not a list built
next to it: `#[derive(Operator)]` with `#[operator("ref")]` and
`#[param(default, min, max)]` (the owner saw the whole shape in the thread
before it was written, and asked for the fields). A field without `#[param]`
is fixed by the code, invisible to the API and the UI: proposed, never
contradicted, not explicitly confirmed. Agreed for later, still: telling
apart the exposed parameters that break the state; `#[param(breaks_state)]`
was suggested as its place.

## Layout

| Crate | Role |
|---|---|
| `streaming-core` | Ids, time, `Kv` trait (9 commands) + `MemKv`, the SDK traits (`Operator`, `Source`, `Sink`, `Transformer`, `Param`) and their pipes (`pipe`), the typed `GraphBuilder`, the engine-level operator traits, `Registry`/`Catalog` (+ `ParamSpec`), `GraphSpec`/`Topology`, KV key layout (`keys`), control-plane documents (`control`), the planted bugs (`mutation`) |
| `streaming-macros` | `#[derive(Operator)]` and `#[derive(Param)]` |
| `streaming-runtime` | The engine: `slice.rs` (actors, epochs, commits), `state.rs` (commit log, seal, window state), `worker.rs`, `controller.rs`, `metrics.rs`, `memlog.rs`, `inspect.rs` |
| `streaming-sim` | Moonpool harness: fault-injecting KV, source and sinks written on the SDK traits, `Scenario`, `Driver`, `Operator`, `Auditor`; `tests/exactly_once.rs`, `tests/mutants.rs` |
| `streaming-gossip` | Scuttlebutt membership, simulable |
| `streaming-kv-redis` | `RedisKv` (RESP client, TLS feature), conformance kit |
| `streaming-warp10` | `std/warp10-sink@1`, `std/count-to-gts@1` |
| `streaming-pulsar` | `std/pulsar-source@1`, `std/pulsar-sink@1` on Magnetar, `std/pulsar-key@1`, `std/count-to-pulsar@1` |
| `streaming-api` | axum API, status aggregation, embedded UI in `ui/` (vanilla JS) |
| `streaming-openapi` (+ `-macros`) | `#[route(GET, "/path")]` on a handler: registers it and derives its OpenAPI 3.2 operation from the signature |
| `streaming` | The application side of the SDK: `App` (tenants, graphs and the operators they name; publication at start; the `standalone` and `worker` commands), stock operators (`stock`), the connectors behind features (`pulsar`, `warp10`, none by default), examples `demo` (the two demonstration graphs) and `pulsar-e2e` |

**Connectors are features of `streaming`** (none by default; the owner asked
for it, 2026-11-13: sources and sinks are many, Pulsar and Warp 10 only the
first). A connector crate holds its operators *and their glue*
(`CountToGts`, `PulsarKey`, `CountToPulsar`) with one `register` function,
which `App` calls under the feature; `streaming::stock` keeps only what no
connector owns. References are unchanged, so fingerprints are too. The
demonstration names `CountToGts` through a dev-dependency on
`streaming-warp10` and writes to a simulated sink: it builds and tests
without any feature, and `cargo test --workspace` therefore covers the build
without connectors. A test checks that each connector is in the catalogue
with its feature and only then.

Rust 1.95 minimum, edition 2024. Everything that does I/O is generic over
`moonpool_core::Providers`: use `moonpool_core::select!`, never
`tokio::select!`; spawn through `TaskScope` (a simulated crash only aborts
the main task of a process, `TaskScope` kills the rest).

## The store

Materia KV speaks Redis without `MULTI`/`WATCH`, Lua, pub/sub or lists, and
only guarantees **single-command atomicity**. The `Kv` trait is therefore
limited to `GET SET DEL INCR HSETNX HSET HGET HGETALL HDEL`. Limits: 5 MB per
value, 10 MB and 5 s per command, `DEL` of 100 keys at most. Every protocol
below is built on one-command atomicity; do not add a command to `Kv` without
checking Materia supports it.

## The protocol, and what must not be broken

A graph is cut into **slices**: `src.<node>.<partition>` (one source
partition up to the first aggregates) and `agg.<node>` (the *emitter* of an
aggregate and everything downstream). A slice is the unit of placement and of
exactly-once.

1. **Commit = one `HSETNX c:<epoch>`** in the slice log, holding position,
   watermark, changed accumulators, owner token and plan generation. The
   loser is *fenced* and restarts from the log. The source is acknowledged
   only after the commit.
2. **Seal.** Superseded records are emptied in place (tombstones); fields are
   deleted only below a write-once seal `f:<epoch>` that every committer
   re-reads *after* its `HSETNX` (a write at or below the seal is void).
   Readers ignore what is at or below the seal.
3. **Only the owner copies records out** of the log into window hashes, after
   its first commit; a starting instance only reads, laying uncopied records
   over the hashes. Whoever empties a record has copied it out entirely. An
   instance taking a slice over empties each inherited record as soon as it
   is copied.
4. **Index first.** A window is entered in the slice's index before its hash
   is written. A writer re-reads the log when its proof that the record still
   holds its payload is older than `gc_grace / 4`.
5. **Forgetting a window takes two steps.** Once emitted its state is deleted
   at once; its index entry stays until no log record has mentioned it for
   `gc_grace`, the countdown being stored in the entry so that it survives
   emitter instances; then state and entry go. Assumption: a store command
   takes effect within `gc_grace / 3` or never, clock skew included.
6. **The emitter** reads the feeding logs first (watermarks and pending
   records), merges accumulators, checks it still owns its log *after*
   reading and *before* emitting, and emits with deterministic `EmitId`s.
   Sinks must be idempotent on them.
7. **No slice commits a change to a window already emitted.** A slice drops
   what arrives behind its watermark; the emitter waits for every slice.
8. **Backpressure across an aggregate** counts windows: a slice stops reading
   once `max_pending_windows` of the windows it closed are not emitted yet.
9. **Plan generations.** The controller asks sources for their partitions
   every `replan_interval`. A change bumps `GraphPlan.generation`, carried by
   instances and commit records; an instance refuses to start behind its log.
   - *Joining* slice (listed in `plan.joined`): waits for an emitter of its
     generation to have committed, then starts from the lowest watermark of
     the slices already feeding the aggregate.
   - *Retired* slice: stops; the emitter still reads it (it no longer holds
     the watermark back) until all it committed is emitted; the controller
     then takes the next epoch of its log itself (one `HSETNX`, from the very
     read that found it drained: an instance left behind is fenced instead of
     committing, acknowledging, then seeing its commit voided unemitted),
     closes it (`p:<generation>` in the seal hash, older-plan commits are
     void, the claimed record stays the head) and deletes its state two
     `gc_grace` later.
   - A graph that cannot be planned keeps the previous plan's bookkeeping.
10. **Deleting a graph.** The controller first closes every slice log
    (older commits void), then waits for workers to notice and for the
    commands in flight to land (`2 * gc_grace`), then purges with deletes
    only, resumably. A pass that still finds something starts the
    countdown again; spec, plan and index entry go last, after a clean
    pass.
    The mark of a deletion is a field of `cluster:deleting`, written first
    and removed last: the index entry is rewritten whole by whoever changes
    a part of it, possibly from a read older than the deletion, so the
    controller follows the mark and says `deleting` again in the entry.
    Workers follow the mark too: slices started again on the word of an
    entry written back would commit into logs whose seal the purge has
    removed, and the purge, which goes by the seals, would leave them.
    The mark outlives the purge: its last pass writes into it the slices
    and aggregates of the graph (`Purged`, merged with what an earlier
    purge listed, the plan being gone when an entry written back has the
    graph purged again) before removing the documents. A controller that
    had read the plan before it went may still close a log after the
    purge; two `gc_grace` later, the first controller to see the mark
    without its graph reads the mark again (the one it read before the
    index may predate the purge), deletes those logs, seals and window
    indexes (deletes only, a call or two), then the mark. Planted bug:
    `ForgetPurgedSlices`.
11. **Control plane.** Not knowing who is alive is never being alone
    (`Membership::alive` returns `None` on a failed read). Read errors are
    never taken for absent documents.
12. **Declarations.** A graph's declarations are numbered fields of
    `t:..:g:..:declarations`, each written once with `HSETNX` and holding
    the fingerprint of what a binary declares
    (`GraphSpec::declaration_fingerprint`: settings, nodes, operators,
    configuration, edges; none when the code no longer has the graph). The
    highest is in force. A fingerprint the graph never had is a new binary:
    published at once. One it has had and left, or a graph the code does not
    have, is an older binary restarting or a deliberate return: published
    only when every other live worker *steps aside* from the graph
    (`MemberInfo.aside`), a worker that advertises nothing yet counting as
    running it. Otherwise the binary writes nothing for that graph
    (`Publication::aside`) and tries again later (`control::out_of_date`,
    every ten seconds in `App`).
13. **A worker only runs what its code declares.** The stored description
    carries the fingerprint it comes from; a worker whose code declares the
    graph otherwise, or not at all, runs none of its slices
    (`WorkerConfig::declared`) and says so in its heartbeat, going by the
    description for every graph, placed on it or not, and never by the index
    entry, which may be written back from an older read. The placement
    keeps slices off such workers while another one runs the graph, and the
    controller is the first of the workers stepping aside from the fewest
    graphs. Descriptions get their version from a counter (`INCR
    t:..:g:..:version`), never from "the stored version plus one": a plan is
    computed for a version, and two descriptions under one version would
    share it. Every new version is a new plan generation, which is what
    fences an instance still running the previous description.

## The SDK layer, and what must not be broken

The engine knows nothing of `Source`, `Sink` and `Transformer`: it drives
`SourceConnector`/`PartitionReader`, `SinkConnector` and type-erased
`DynMap`/`DynKey`/`WindowAssigner`/`DynAggregator` built by a `Registry`. The
traits are adapted to these in `streaming-core/src/pipe.rs`
(`Registry::source`, `sink`, `transformer`; a `GraphBuilder` calls them for
the operators it names, `App` collects them).

1. **The code of a source or a sink runs inside the engine's calls**
   (`poll`, `ack`, `end_position`, `write`): its future is polled there and
   nowhere else. No task is spawned, so nothing outlives a simulated crash,
   a run is deterministic, and backpressure is exact: a source is never more
   than one read ahead of the engine (`SourcePipe::send` waits once the pipe
   holds what the last `poll` asked for).
2. **A batch is written once its sink said so** (`Batch::done`). A sink that
   says it failed, asks for the next batch, returns, or whose write the
   engine gave up, leaves the batch unwritten: the engine sends it again, to
   a sink started again in the last two cases. Planted bug:
   `ConfirmWithoutTheSink`.
3. **A source learns of a commit after it happened**, through
   `SourcePipe::committed` (the latest position when several were missed).
   The engine does not wait for the source to have acknowledged: a source
   that pushes acknowledges during the following calls.
4. **Where a partition ends** is what the source said last (`SourcePipe::end`)
   and never before the last record it pushed: a record waiting in the pipe
   must not let the engine take the partition for idle and close windows
   over it. A source that says nothing is idle as soon as it has pushed
   nothing more. Saying an end older than a record the source has not read
   yet is the source's mistake, and closes windows too early.
5. **A source built on a connector hands it the partition**
   (`SourcePipe::read`): every call of the engine then goes straight to the
   connector's reader, with the exact contract the engine was validated
   against, and a commit received while the partition was being opened is
   passed on. This is what Pulsar does; the simulation runs both ways
   (`Scenario::piped`). Likewise `SinkPipe::write_with` for sinks (Warp 10,
   Pulsar).
6. **A stateless transformer is a map under its own name.**
   `StatelessTransformer<In>` (`apply(&In) -> Out`) and
   `StatelessFilterMap<In>` (`apply(&In) -> Option<Out>`) know nothing of
   pipes; the wrappers `Stateless<T>` and `StatelessFilter<T>` make
   `Transformer`s of them, delegating everything `Operator` says (reference,
   parameters, `check`, secrets) and serializing as the operator itself, so
   the stored configuration and the declaration fingerprint are those of the
   wrapped operator. `GraphBuilder::map` and `filter_map` apply the wrappers.
   Not a blanket `impl<T: StatelessTransformer<In>> Transformer<In> for T`:
   it conflicts with every generic `Transformer` implementation
   (`impl<T> Transformer<T> for Identity`, an application's own), and one
   wrapper cannot implement both traits (a type could implement the two).
   `json_decoder!` declares a `StatelessFilterMap` from `Vec<u8>` and
   `String` to a type of the application's, under a reference the
   application names: a generic `JsonDecoder<T>` would have one reference
   for every `T`, and the registry keeps one factory per reference and input
   type, so two graphs decoding to two types would share one. What does not
   decode is dropped (asked of the owner, answer pending: drop, fail the
   node, count, or a dedicated output).
   **Union and split are maps.** `GraphBuilder::union` is a `std/union@1`
   node with several inputs (the engine aligns barriers over a node's
   inputs, as for the diamond; inputs from several sources make the node a
   member of each source's slices). `GraphBuilder::split` is a node running
   the `Split` operator once per element, which wraps each in a
   `Routed<T>` (the index of its branch and an `Arc` of the element, never
   copied), then one `std/branch@1` node per branch, `{"branch": i}` fixed
   by the code, keeping its own and unwrapping them. One node holds the
   routing and its configuration, so a parameter changed through the API
   cannot make two branches disagree. The engine is untouched: no new
   precaution, hence no planted bug. Held by
   `a_split_routes_each_element_once_and_a_union_brings_them_back` and
   `a_union_of_two_sources_writes_each_element_once` in
   `streaming-runtime/tests/engine.rs`; the simulation's graph does not use
   them yet.
   **A batch is a window over one key** (the owner chose it, 2026-11-14,
   over a batch flushed at each epoch barrier, which is not exactly-once:
   a replay ends its epochs elsewhere, and the replayed batches would
   differ under the same emit ids; and over a batch held in the commit by
   a new protocol). `GraphBuilder::batch` adds `std/one-key@1`,
   `std/tumbling@1` (`max_wait`), `std/collect@1` (an aggregate whose state
   is the `Vec<T>`) and `std/batch@1` (`maxSize`, cutting at emission). The
   engine is untouched. Limits: event time; the whole `Vec` in every commit
   that changes the window (5 MB); `postcard` encoding; and the order of
   the partitions in a batch is that of the feeding slices, so an emitter
   retried across a change of plan could cut a window otherwise under the
   same emit ids. Held by
   `a_batch_gathers_every_element_once_in_pieces_no_larger_than_asked`
   (measured: a `merge` that drops the other partitions fails it); the
   simulation's graph does not use it yet.
7. **A transformer is one node, hence one kind**: any number of maps and
   filters in a row (fused), or one `key_by`, or one window, or one
   aggregate. `Registry::transformer` calls `transform` on an example to
   learn the kind and the types; what a transformer is must not depend on
   its configuration (a node whose configuration changes its kind is refused
   when it starts). `Keyed<T>` and `Windows<T>` only exist for the compiler:
   the values that flow are `T`.
8. **An operator written for several types has one factory per type**
   (registry *variants*: `impl<T> Sink<T>`, `impl<T> Transformer<Windows<T>>
   for Count`). The engine and `App::check` pick the one registered for what
   feeds the node (`Registry::get_for`, `OpDescriptor::signatures`,
   `output_for`). `Opaque` registers a type-agnostic transformer once for any
   type, which is how `Registry::standard()` serves graphs written by hand.
9. **The stored configuration of a node is its operator as JSON**: camelCase
   names, unknown fields refused, a parameter that may be absent and has no
   value left out. This is part of the declaration fingerprint: changing how
   an operator serializes makes every graph naming it a new declaration.
   A configuration is checked (bounds, then `check = ...`) by the factory of
   every kind and by `App::check` when the binary starts
   (`Registry::accepts`); a secret its tenant lacks is refused when a source
   or a sink is built, before anything starts.

`streaming-core/tests/pipes.rs` holds the adapters to these points one by
one; the simulation holds them under faults.

`streaming_core::mutation` (shown as `streaming_runtime::mutation`) holds eighteen **planted bugs** in its bench, one per precaution
above (`SIM_MUTANT=<name>` plants a single one). `CloseWithoutClaim` is
noticed by the `Auditor`: closing a log without the claim voids its head, and
a log that still holds fields but no head is history going backwards (every
run of the two repartition scenarios of the bench). When you add a precaution, add its mutant and make the simulation
notice it.

One precaution has no planted bug, point 4 of the SDK layer (the end of a
partition is never before what its source pushed). The engine asks for the
end right after a read that emptied the pipe, and in a simulation nothing
runs between the two calls: a source can only push in between when
something outside the engine's task feeds it, another thread say. Planted,
the bug went unnoticed in 144 runs, as it had to. The precaution is held by
`the_end_of_a_partition_is_what_the_source_says_and_never_before_what_it_pushed`
in `streaming-core/tests/pipes.rs`.

## Verifying

```sh
cargo +nightly fmt --all                                  # rustfmt.toml uses nightly-only options
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p streaming-sim --features mutants --all-targets -- -D warnings
cargo test --workspace                                   # every crate, a few minutes
cargo clippy -p streaming --all-targets --features pulsar,warp10,tls -- -D warnings   # the connectors on
cargo test -p streaming --features pulsar,warp10,tls
cargo test -p streaming-core --test pipes                # the pipes of sources and sinks, in a second
SIM_ITERATIONS=200 SIM_CONFIGS=40 cargo test --release -p streaming-sim --test exactly_once
SIM_ITERATIONS=8 cargo test --release -p streaming-sim --features mutants --test mutants -- --nocapture
SIM_PIPED=1 SIM_MUTANT=<name> ...                        # the bench, every scenario reading through a source that pushes
SIM_SEEDS=<seed> cargo test -p streaming-sim --test exactly_once <test> -- --nocapture   # replay
SIM_CONFIG_SEED=<n> cargo test -p streaming-sim --test exactly_once exactly_once_in_random_configurations
SIM_REPORT=1 ...                                         # print coverage of the assert_sometimes
SIM_DUMP_ACTORS=1 ...                                    # every actor in the dump of a failing run
```

Other suites: `GOSSIP_SIM_ITERATIONS`, `KVREDIS_SIM_SEEDS`, `WARP10_SIM_SEEDS`.

`scripts/sim-sweep.sh` runs the exactly-once scenarios in turn, on every
core, on fresh seeds for a wall-clock budget (`SWEEP_BUDGET_SECS`, 900 by
default), and writes `target/sim-sweep/summary.md` with a replay command per
failing seed. `.github/workflows/nightly-simulation.yml` runs it every night
at 02:17 UTC (schedule paused while the account has no Actions minutes; it
still runs by hand, with a budget in minutes), uploads the results and
opens or comments on a "Nightly simulation failed" issue. Release-profile
seeds last about 70 ms each; replay them with `--release`. A job still
running after five minutes is killed and the seed it was on (traced by
`SIM_TRACE_SEEDS=1`) is reported as a seed that does not end.

Generated mutants (`cargo install cargo-mutants`, configured in
`.cargo/mutants.toml`) measure the ordinary tests the way the planted bugs
measure the simulation. Use the `mutants` profile, without which the
simulations make a run last days, and test the engine with the simulation:

```sh
cargo mutants --profile mutants -p streaming-gossip -p streaming-kv-redis
cargo mutants --profile mutants -f 'crates/streaming-runtime/**' \
    --test-package streaming-runtime,streaming-sim --timeout 180
cargo mutants -p streaming-core -p streaming-api -p streaming-warp10 -p streaming-pulsar -p streaming
```
`scripts/e2e-pulsar-redis.sh` runs real Pulsar + Redis + two workers with a
`kill -9`; `PULSAR_E2E_URL=pulsar://... cargo test -p streaming-pulsar`.

The harness (`streaming-sim/src/lib.rs`):

- **Faults**: reboots (Moonpool attrition); KV commands lost, applied without
  reply, applied *after their sender was told they failed* (even after its
  death), stalled for seconds; source redeliveries and cut batches; sinks
  failing half-way, and not confirming a batch in each of the three ways a
  sink has (saying so, asking for the next one, stopping); drifting worker
  clocks.
- **Operators**: the source, the sinks and the transformers of the simulated
  graph are written on the SDK traits and found through the registry like
  any application's; their world reaches them through `Shared`. The source
  reads its topic in one of two ways (`Scenario::piped`, drawn in random
  configurations): handing its partition to the connector the engine drives
  (`ChaosSource`, as before the traits), or pushing into its pipe, with the
  same faults.
- **Checks**: every event counted exactly once on four paths (windowed count,
  stateless, diamond, second-level aggregation); committed history never goes
  backwards nor loses its head (`Auditor`, every 100 ms); nothing acknowledged before being
  committed; no commit into an emitted window (checked the instant a commit
  lands); no window state or orphan log left after settling; logs stay small;
  a commit under the plan in force comes from a binary that declares the
  description this plan was computed from (also checked as it lands).
- **Scenarios**: twenty-seven hand-written ones (five of them with a source
  that pushes) plus random configurations
  (`Scenario::random`). A failing run prints `[VIOLATION] seed=...` and a
  dump of the cluster state. With `Scenario::deploy` the workers are
  instances of an application: they publish the graph of their release when
  they start, a `Deployer` replaces the release on one worker after the other
  while events flow (a new one or an earlier one again; a worker that crashes
  before its turn comes back with the old one), then a deployment fails (an
  instance of a release never seen publishes and dies, an index entry of the
  release in place landing after it; the release in place must take the
  graph back), and the graph leaves the code before it is deleted. Every run ends with an index entry written back by
  someone who had read it before the deletion, at once or just as the purge
  has removed the first seal, and with one log closed again by a late
  controller once the purge is over (the sweep of point 10 must remove it).
  From the end of the purge, anything written into the graph but such a
  seal (`p:` fields only) is a violation: the sweep would otherwise hide a
  slice still running (`DeleteWithoutClosing`, `RunWhatIsBeingDeleted`).
- Moonpool specifics: the run stops when *any* workload returns, so auxiliary
  workloads wait on `ctx.shutdown()`; without pinned seeds each run explores
  new ones; a "did not settle" may be a configuration too slow for the
  900 s timeout rather than a deadlock — look at whether progress is made.
- Moonpool 0.8.0 reseeds the *global* simulation RNG with the iteration's
  seed every time a process restarts (`handle_restart` builds
  `SimProviders::new(.., seed, ..)`): all randomness starts over after each
  reboot, and a rebooted worker draws again owner tokens it drew before (in
  about two runs out of three). This is the only path that exercises two
  instances of a slice sharing a token; fixing the reseed without injecting
  such reuse on purpose would leave that precaution untested. Open.
- `Scenario::random` lowers what it drew when the run could not end in
  time. A slow sink is paid once per write, a window reaching its sink
  seven keys at a time (`CHUNK_SIZE`): counted once per epoch only, a draw
  with 200 keys, 28 windows and a sink at three seconds did not settle on
  one seed in three (`SIM_CONFIG_SEED=1794525933778258523`, on the previous
  harness as well).
- Simulated time only passes when every task waits. A fault that answers at
  once must therefore be bounded, or a seed never ends: the simulated broker
  rewound at every read, redelivered more than it delivered when asked for a
  single record (an engine one record short of a full batch, lingering for
  the last), and the engine read redeliveries for ever at the same instant.
  It now rewinds only once it has delivered again what it rewound over
  (`may_rewind`). Seed 5278357632631441426 of the second scenario of the
  bench, with `SIM_PIPED=1`, lasted over seven minutes before that; the
  handed reader did the same.

Last validated state (purge sweep): the hand-written suite at 100 seeds per
scenario, 30 random configurations at five seeds, and the eighteen planted
bugs over 80 runs each (ten bench scenarios, one of them with the tightest
gate, `max_pending_windows: 1`): `ForgetPurgedSlices`, `DeleteWithoutClosing`,
`DeleteByTheIndexEntry` and `AckBeforeCommit` in every run;
`CommitBelowTheSeal` in 8, `ForgetAtOnce` in 7 and `JoinWithoutWatermark` in
2, which stay the least noticed.

Before that (SDK traits): the hand-written suite at 200 seeds per
scenario and 40 random configurations at ten seeds; 2,700 runs of the bench's scenarios
without a planted bug, half of them through a source that pushes, none
lasting more than a fraction of a second; the seventeen planted bugs noticed
one at a time over 72 runs each, from `ForgetAtOnce` (2 runs),
`JoinWithoutWatermark` (5) and `CommitBelowTheSeal` (6) to
`AckBeforeCommit`, `DeleteByTheIndexEntry` and `DeleteWithoutClosing` (all
72), `ConfirmWithoutTheSink` in 53 of 72 through the handed connector and
55 of 72 through a source that pushes. The simulated operators having moved
to the SDK traits and the broker's redeliveries being bounded, the six least
noticed were run on the previous harness too, at the same rates
(`ForgetAtOnce`: 15 runs of 256 before, 12 of 288 after). The seventeen
were noticed with `SIM_PIPED=1` as well, at the same rates
(`CloseWithoutClaim`: 16 runs of 72 both ways).

Before that (declarations): 3,000 runs of the two deployment
scenarios, the hand-written suite, 680 random configurations, and the four
planted bugs of the declarations noticed one at a time (`RunAnotherDeclaration`
and `AsideByTheIndexEntry` in nearly every deployment run,
`DeleteByTheIndexEntry` in every run, `RunWhatIsBeingDeleted` in one run in
six).

## Defects the simulation found (do not bring them back)

Trimmed commit fields revived by a stale worker; a stale emitter reading
state being deleted; controller taking read errors for missing documents; no
output watermark on emitters; orphan window hashes; a slice needing thirty
flawless store calls in a row to start; index cleanup needing one emitter
instance to live for `gc_grace`; a worker electing itself when it could not
read the members; takeover never progressing under short-lived instances; an
unplannable graph retiring all its slices; purging a retired slice racing
with its live instances; a half-done purge leaving an unreadable log; a deleted graph recreated by
writes landing after its purge (logs, seals and plan came back); a retired
slice never purged under faults (the purge asked every node, not only the
aggregates, and needed some twenty flawless store calls in a row); two
descriptions stored under one version by two publications, sharing a plan
computed for one of them; a deletion undone by an index entry written back
from a read older than it (the graph stayed, stopped, for ever); workers
saying they ran a graph they refused, because a late index entry named their
declaration over a description that did not (the binary waiting for them to
step aside waited for ever, and the graph ran nowhere); workers running a
graph being deleted again because its index entry was written back, and
committing into logs already purged; a source slice started just before the
instance it replaced committed and acknowledged the last records of a
partition, waiting for ever for records that would never come to it, its
watermark holding every window back (it now looks at its log when records
lie beyond what it read and none comes; held by
`a_slice_behind_what_another_instance_acknowledged_starts_again` in
`streaming-runtime/tests/engine.rs`, the simulation meeting it about once in
three thousand runs, too seldom for a planted bug); a retired
slice closed between an instance's commit and its acknowledgement (the
commit, valid when written, was voided by the closing: lost had it carried
records); a source slice lingering for a full batch behind a
source that redelivered faster than it moved on, polling for ever without
ever awaiting the clock (the simulation's time stood still: a seed that does
not end; the slice now stops lingering when a poll brings nothing new); an
instance taking for its own a commit that the previous instance of its
worker, dead, had sent under the same owner token and that landed late
(`try_commit` went by the owner alone: the new instance acknowledged one
record beyond what the log held; it now recognises its own record byte for
byte, held by `an_instance_that_drew_the_token_of_another_is_fenced` in
`state.rs`); seals left by a deleted graph (`the deleted graph left 5 keys`,
seen once on a random configuration): a controller that had read the plan
before the purge removed it closed the logs again afterwards, and nothing
went by them any more. The mark now keeps the list and sweeps it; the
first versions of that fix lost the list when an index entry written back
had the graph purged a second time, without its plan; needed several
flawless store calls in a row to sweep; and swept by a mark read before the
purge, removing the list it had just written.

## The HTTP API and its OpenAPI document

Every handler of `streaming-api/src/routes.rs` carries
`#[route(METHOD, "/path", tag = "...")]` and is listed once in `api()`; the
router and `GET /api/v1/openapi.json` both come from that list, so they
cannot diverge. The attribute generates, next to the function, a module of
the same name whose `route()` returns the handler with its description
(`use super::*` inside, hence handlers live at module level). Descriptions
are type-driven: each argument implements `OperationInput`, the return type
`OperationOutput` (`ApiResult<T>` resolves through the alias). A handler must
therefore say what it reads and returns in its prototype: no `Bytes`,
`RawQuery`, `serde_json::Value` or opaque `Response` (the owner agreed to
this). `JsonBody<T>` and `QueryParams<T>` replace axum's `Json` and `Query`
on input to keep the error contract and to accept bodies without a
`Content-Type`. Schemas come from `schemars` 1.x: a type crossing the API
derives `schemars::JsonSchema`; hand-serialized types (ids, `SliceId`,
`Position`, `TypeTag`, durations) have hand-written schemas that must follow
their `Serialize` impl. `tests/api.rs` validates every answer against the
document, so a drift fails there. After changing the API, regenerate the
committed copy: `UPDATE_OPENAPI=1 cargo test -p streaming-api --test openapi`.
The version is 3.2.0; the document uses nothing 3.2-specific and validates
as 3.1 too.

**Errors** are `{"message", "code", "context"}` (the owner's rule). `code` is
a constant from `streaming-api/src/error.rs`; `message` is for people and is
*always* its template rendered with `context`, so that a client can rewrite
it in another language from `code` + `context` alone (the UI does, in French,
from `ui/assets/js/errors-fr.js`; a test checks it knows every code with the
same placeholders). Never echo internals or configuration values (security);
log them. A new error case gets a new code, never a reworded old one.

## Running and the UI

`cargo run --release -p streaming --example demo -- standalone` starts
three in-memory workers, two demo graphs (`acme/count-per-key`,
`globex/clicks`) and the UI on `http://127.0.0.1:8080`. Views: overview,
cluster, catalogue, graph (topology, metrics, bottleneck, results; the side
panel of a node edits the fields its operator exposes, built from the
catalogue's `ParamSpec`s, a `choices` list becoming a drop-down, `min` and
`max` bounding a number and being said under the field, a refusal of the API
that names a field being shown on it), and **API**
(`#/api`), a viewer of `/api/v1/openapi.json` (operations by tag, schema
trees, a form to try GET operations). Every connector describes
its parameters; a test per connector checks the descriptions against the
parser (names accepted, required flags, defaults, choices).

Observability vocabulary: an actor is *busy*, *idle* or *backpressured*;
`waiting` is the part of busy spent inside a sink's write. A node shows the
worst busy and the worst backpressure over its instances. The bottleneck
says `limitedBy`: `destination`, `work` or `store`.

After changing anything under `crates/streaming-api/ui/`, rebuild the
application (the UI is embedded) and check it in a real browser.

## The project page

`docs/index.html` is the project's page for GitHub Pages (served from `/docs`
on `main`, `docs/.nojekyll` keeping the folder as it is). One self-contained
page in English, in the Nocturne palette of the UI, with the Inter font of
the UI copied under `docs/assets/fonts` and a screenshot of the demo in
`docs/assets/img`. Its figures (scenarios, planted bugs, crates) and its
limits come from this file and the README: change them together. It must
never present the project as ready for production.

`docs/operators.md` documents the kinds of operators, their contracts and the
shipped catalogue (references, types, parameters, delivery). Only what is on
`main` is described as existing; work in progress goes under *Not yet*.
Adding, renaming or changing the parameters of a stock operator means
updating its table.

## Known limits and open questions

- **What the API refuses of a configuration, and what it still lets
  through.** `PATCH .../nodes/{node}/config` refuses a value its parameter
  does not describe (`ParamSpec::refuses`: kind, choices, and the bounds of
  `#[param(min, max)]`, which the catalogue now carries, an integer narrower
  than 64 bits being bounded by its type), each with its own error code,
  then asks the operator itself through `ApiState::accepts`
  (`Registry::accepts`, hence `check = ...`), which `App` gives. Three things
  remain. An API served without the application's code (`accepts: None`)
  only goes by the descriptions. The operator asked is the one of the binary
  serving the request: during a deployment it may not be the one of the
  description in force, and an operator this binary does not have is
  accepted. And a publication only leaves out the stored fields a
  *description* refuses (`Publication::ignored_overrides`, said by the
  worker): a stored configuration that a newer `check` refuses is still laid
  over the code's, and the slices of the node then fail to start, saying
  why, until the field is set again.
- **A source that pushes acknowledges late, and answers for its end.** The
  engine does not wait for it after a commit (point 3 of the SDK layer): the
  last acknowledgement before a slice stops may never reach the broker, which
  then redelivers, and the engine drops what it already processed. And the
  engine believes what the source says of the end of its partition (point 4).
  A source on a real broker should be built on a connector
  (`SourcePipe::read`), as Pulsar is, until the pipe carries both exchanges
  as questions the engine waits on.
- `SourcePipe::read` and `Pipe` rely on things the compiler does not check:
  the connector handed a partition must give values of the source's `Out`
  (a mismatch is refused by the first operator downstream, at run time), and
  `map`, `filter` and `key_by` asked of a `Pipe<Keyed<_>>` or
  `Pipe<Windows<_>>` compile, and are refused when the operator is
  registered (the markers are recognized by their type name).
- A tuple structure, a generic one and an enumeration cannot derive
  `Operator`; an operator with generic behaviour implements its traits for
  several types instead (`impl<T> Sink<T>`).

- **No algorithm splits a node into several actors.** Parallelism follows
  the source partitions; the controller only moves whole slices off a worker
  above 85 % CPU or memory. Rule agreed for the day it exists: scale out a
  node limited by *work*; for one limited by its *destination*, more actors
  only help if the destination takes more concurrent writes.
- Events a new partition receives before the pipeline reads it may be late.
  The simulation checks they are never counted twice, but cannot yet tell a
  late drop from a silent loss (a "late records" side output would fix both).
- A partition removed then re-added under the same index resumes the old
  state.
- `replan_interval` is a `WorkerConfig` field without a CLI flag.
- A worker notices a deletion when the cluster epoch changes, or when it
  reads the documents again on its own (every thirty ticks). The purge
  starts two `gc_grace` after the logs are closed, whatever the tick: with a
  `gc_grace` under fifteen ticks, a worker that missed the epoch change may
  still run a slice when its seal goes, and commit into a log the purge has
  passed. The defaults (one second, thirty seconds) leave a margin of two;
  nothing checks the configuration.
- The `warp10` crate (crates.io, 3.0.0) was evaluated to replace
  `streaming-warp10` and not adopted. Its client is `reqwest` (tokio sockets,
  not the Moonpool providers), so it cannot run in simulation, and `reqwest`
  is a mandatory dependency even for the encoding alone. Its encoding loses
  what this crate guarantees: a double `3.0` is written `3` (read back as a
  long), NaN is written as is, string values are not escaped (a `'` or a
  newline in a value injects lines), empty classes and empty label values
  (which delete a label) pass, the time unit is fixed to microseconds, and
  there is no parser for the fake ingress. Revisit if upstream fixes the
  encoding and puts the HTTP client behind a feature.
- The Pulsar sink is at-least-once (emit id in a property); messages it
  writes carry a wrong publish time (Magnetar's moonpool engine clock).
- **Tenants are declared by the code** and created by the first worker that
  publishes them (one `HSETNX` on `cluster:tenants`, never overwritten: a
  display name changed in the code keeps the stored one, which the worker
  says). `PUT /api/v1/tenants/{tenant}` still creates one the code does not
  declare. Only publication checks a graph's tenant; the runtime never
  reads `cluster:tenants`. There is no tenant deletion or renaming yet.
- **Publication** (`streaming_api::control::publish`) keeps the code's
  description in `t:..:g:..:code`, lays the API's fields
  (`t:..:g:..:overrides`, per node) over it, and stores the result as a new
  version only when it differs. A field the operator no longer exposes, or
  whose stored value its parameter no longer takes, is not applied (the
  worker says so). A graph in the store but not in the code
  becomes an *orphan* (`GraphIndexEntry.orphan`): stopped, state kept, every
  write refused but `DELETE`, which is refused for the others. The owner
  confirmed this over deleting it at once (2026-11-12): deleting a state
  cannot be undone. An orphan the code declares again runs again on its
  state; a graph being deleted when the code brings it back is published
  once the deletion is over, and starts empty.
- **Declarations, what is left open.** The fingerprint covers what the code
  *declares*, not what its operators *do*: two binaries that differ only by
  the body of a closure have the same fingerprint, and both run the graph
  during the deployment. Going back to an earlier declaration waits for the
  workers of the replaced binary to be gone (their heartbeat to time out):
  a rollback takes over a few seconds after the last of them stops, not at
  once. A change through the API racing with a publication may store a
  description laid over the previous code; the workers notice within ten
  seconds and publish again, at the price of one more version. Several
  workers of a new binary starting at the same instant may each store the
  description (as many versions, the same content). The list of
  declarations is never trimmed (one small field per deployment that changes
  the graph). A worker of an older binary that comes back alone, every newer
  one being dead, declares again a graph the newer code had dropped: it is
  its right, and it recreates, empty, one that was being deleted.
- Never run against a real Materia KV or a real Warp 10. No quotas, fair
  scheduling or API authentication.
- The owner asked how to design interfaces with comments on components: a
  Design canvas artefact built on Nocturne was offered, not done.
