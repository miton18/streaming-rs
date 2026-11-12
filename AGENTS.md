# streaming-rs — handover for agents

A Rust streaming framework, "like Flink but simpler", built for one first use
case: **Pulsar source → 5-minute windows → count per key → flush elsewhere
(Pulsar or Warp 10)**, exactly-once, with state in a Redis-protocol store
(Materia KV). Everything is simulated deterministically with Moonpool.

Read `README.md` for the user-facing description and `docs/http-api.md` for
the HTTP contract. This file holds what they do not: the decisions, the
invariants, how to verify, and what is still open.

**Status.** `main` builds, lints and passes every test. A standalone
demonstrator (`streamd standalone`) runs on the owner's workstation. Nothing
is deployed in production; the on-disk formats may still change freely.

## Working with the owner

- Answer in **French**. Code, comments, commit messages, `README.md` and this
  file are in English; the web UI is in French.
- Work is pushed **directly to `main`** (the repository started empty). No
  pull request unless asked. No licence is declared yet. These three choices
  were stated to the owner and never explicitly confirmed: ask before
  changing them.
- The owner's standard: *"if the whole simulation passes on a project this
  young, it is not severe enough"*. A green run is not an argument. Measure
  the harness (mutation test), then trust it.
- Error messages are for people: never the `Debug` form of a value in a log or
  on stderr (`streamd` reports fatal errors as `error: <Display>`).
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

## Layout

| Crate | Role |
|---|---|
| `streaming-core` | Ids, time, `Kv` trait (9 commands) + `MemKv`, operator traits, `Registry`/`Catalog` (+ `ParamSpec`), `GraphSpec`/`Topology`, KV key layout (`keys`), control-plane documents (`control`) |
| `streaming-runtime` | The engine: `slice.rs` (actors, epochs, commits), `state.rs` (commit log, seal, window state), `worker.rs`, `controller.rs`, `metrics.rs`, `memlog.rs`, `inspect.rs`, `mutation.rs` |
| `streaming-sim` | Moonpool harness: fault-injecting KV/source/sinks, `Scenario`, `Driver`, `Operator`, `Auditor`; `tests/exactly_once.rs`, `tests/mutants.rs` |
| `streaming-gossip` | Scuttlebutt membership, simulable |
| `streaming-kv-redis` | `RedisKv` (RESP client, TLS feature), conformance kit |
| `streaming-warp10` | `std/warp10-sink@1` |
| `streaming-pulsar` | `std/pulsar-source@1`, `std/pulsar-sink@1` on Magnetar |
| `streaming-api` | axum API, status aggregation, embedded UI in `ui/` (vanilla JS) |
| `streaming-openapi` (+ `-macros`) | `#[route(GET, "/path")]` on a handler: registers it and derives its OpenAPI 3.2 operation from the signature |
| `streamd` | The binary: `standalone` (demo cluster in one process) and `worker` |

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
     then closes its log (`p:<generation>` in the seal hash, older-plan
     commits are void) and deletes its state two `gc_grace` later.
   - A graph that cannot be planned keeps the previous plan's bookkeeping.
10. **Control plane.** Not knowing who is alive is never being alone
    (`Membership::alive` returns `None` on a failed read). Read errors are
    never taken for absent documents.

`streaming_runtime::mutation` holds ten **planted bugs**, one per precaution
above. When you add a precaution, add its mutant and make the simulation
notice it.

## Verifying

```sh
cargo +nightly fmt --all                                  # rustfmt.toml uses nightly-only options
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p streaming-sim --features mutants --all-targets -- -D warnings
cargo test --workspace                                   # every crate, a few minutes
SIM_ITERATIONS=200 SIM_CONFIGS=40 cargo test --release -p streaming-sim --test exactly_once
SIM_ITERATIONS=8 cargo test --release -p streaming-sim --features mutants --test mutants -- --nocapture
SIM_SEEDS=<seed> cargo test -p streaming-sim --test exactly_once <test> -- --nocapture   # replay
SIM_CONFIG_SEED=<n> cargo test -p streaming-sim --test exactly_once exactly_once_in_random_configurations
SIM_REPORT=1 ...                                         # print coverage of the assert_sometimes
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
cargo mutants -p streaming-core -p streaming-api -p streaming-warp10 -p streaming-pulsar -p streamd
```
`scripts/e2e-pulsar-redis.sh` runs real Pulsar + Redis + two workers with a
`kill -9`; `PULSAR_E2E_URL=pulsar://... cargo test -p streaming-pulsar`.

The harness (`streaming-sim/src/lib.rs`):

- **Faults**: reboots (Moonpool attrition); KV commands lost, applied without
  reply, applied *after their sender was told they failed* (even after its
  death), stalled for seconds; source redeliveries and cut batches; sinks
  failing half-way; drifting worker clocks.
- **Checks**: every event counted exactly once on four paths (windowed count,
  stateless, diamond, second-level aggregation); committed history never goes
  backwards (`Auditor`, every 100 ms); nothing acknowledged before being
  committed; no commit into an emitted window (checked the instant a commit
  lands); no window state or orphan log left after settling; logs stay small.
- **Scenarios**: nineteen hand-written ones plus random configurations
  (`Scenario::random`). A failing run prints `[VIOLATION] seed=...` and a
  dump of the cluster state.
- Moonpool specifics: the run stops when *any* workload returns, so auxiliary
  workloads wait on `ctx.shutdown()`; without pinned seeds each run explores
  new ones; a "did not settle" may be a configuration too slow for the
  900 s timeout rather than a deadlock — look at whether progress is made.

Last validated state: 3,800 runs on the hand-written scenarios, 35 of 36
random configurations (the failure was an infeasible draw, the generator was
fixed), ten of ten mutants noticed.

## Defects the simulation found (do not bring them back)

Trimmed commit fields revived by a stale worker; a stale emitter reading
state being deleted; controller taking read errors for missing documents; no
output watermark on emitters; orphan window hashes; a slice needing thirty
flawless store calls in a row to start; index cleanup needing one emitter
instance to live for `gc_grace`; a worker electing itself when it could not
read the members; takeover never progressing under short-lived instances; an
unplannable graph retiring all its slices; purging a retired slice racing
with its live instances; a half-done purge leaving an unreadable log.

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
on input to keep `{"error": ...}` answers and to accept bodies without a
`Content-Type`. Schemas come from `schemars` 1.x: a type crossing the API
derives `schemars::JsonSchema`; hand-serialized types (ids, `SliceId`,
`Position`, `TypeTag`, durations) have hand-written schemas that must follow
their `Serialize` impl. `tests/api.rs` validates every answer against the
document, so a drift fails there. After changing the API, regenerate the
committed copy: `UPDATE_OPENAPI=1 cargo test -p streaming-api --test openapi`.
The version is 3.2.0; the document uses nothing 3.2-specific and validates
as 3.1 too.

## Running and the UI

`cargo run --release -p streamd -- standalone` starts three in-memory
workers, two demo graphs (`acme/count-per-key`, `globex/clicks`) and the UI
on `http://127.0.0.1:8080`. Views: overview, cluster, catalogue, graph
(topology, metrics, bottleneck, results), and **Concevoir** (`#/design`), the
pipeline designer built from the catalogue's `ParamSpec`s (a `choices` list
becomes a drop-down, the `json` kind a JSON field). Every connector describes
its parameters; a test per connector checks the descriptions against the
parser (names accepted, required flags, defaults, choices).

Observability vocabulary: an actor is *busy*, *idle* or *backpressured*;
`waiting` is the part of busy spent inside a sink's write. A node shows the
worst busy and the worst backpressure over its instances. The bottleneck
says `limited_by`: `destination`, `work` or `store`.

After changing anything under `crates/streaming-api/ui/`, rebuild `streamd`
(the UI is embedded) and check it in a real browser.

## Known limits and open questions

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
- The Pulsar sink is at-least-once (emit id in a property); messages it
  writes carry a wrong publish time (Magnetar's moonpool engine clock).
- Never run against a real Materia KV or a real Warp 10. No quotas, fair
  scheduling or API authentication.
- The owner asked how to design interfaces with comments on components: a
  Design canvas artefact built on Nocturne was offered, not done.
