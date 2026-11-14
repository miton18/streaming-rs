# Operators: sources, transformers and sinks

Everything a graph does between reading and writing is an **operator**: a Rust
structure that names a thing and carries its configuration, plus one trait
saying what the thing does. This document explains the kinds of operators,
how to write each one, and lists the operators the project ships.

It describes what is on `main`. What is being built and not merged yet is
listed at the end, under [Not yet](#not-yet).

- [The model](#the-model)
- [Declaring an operator](#declaring-an-operator)
- [Sources](#sources)
- [Transformers](#transformers)
- [Sinks](#sinks)
- [Building a graph](#building-a-graph)
- [Catalogue](#catalogue)
- [Not yet](#not-yet)

## The model

A graph is a DAG of nodes. Each node runs one operator, and is exactly one of:

| Kind | Trait to implement | Takes | Gives |
|---|---|---|---|
| source | `Source` | a partition to read | `T` |
| stateless step | `StatelessTransformer` or `StatelessFilterMap` | `A` | `B` (or nothing) |
| any transformer | `Transformer<In>` | `Pipe<In>` | `Pipe<Out>` |
| sink | `Sink<In>` | batches of `In` | nothing |

A transformer node is always one of four things, and only one:

1. **maps and filters** (any number in a row, fused into one node);
2. **a `key_by`**, which gives `Keyed<T>`;
3. **a window**, which takes `Keyed<T>` and gives `Windows<T>`;
4. **an aggregate**, which takes `Windows<T>` and gives `Windowed<A>`.

The types enforce the order, so a window on a stream without a key does not
compile:

```text
Source --> T --map/filter--> T' --key_by--> Keyed<T'> --window--> Windows<T'> --aggregate--> Windowed<A> --> Sink
```

`Keyed<T>` and `Windows<T>` exist for the compiler only: the values that
flow are `T`, with their key and their windows carried beside them.

Parallelism follows the source. A source node runs once per partition, and
everything downstream up to the first aggregate runs in the same *slice*
(`src.<node>.<partition>`). Each aggregate has one emitting slice
(`agg.<node>`), which merges the partitions and runs everything after it.
A slice is the unit of placement and of exactly-once; the README explains
the protocol.

## Declaring an operator

The structure **is** the configuration. `#[derive(Operator)]` gives it:

- a reference, `#[operator("acme/thing@1")]`, under which the store, the API
  and the catalogue name it. Bump the version when its meaning changes;
- a one-line description for the catalogue, taken from its doc comment;
- its **parameters**: every field marked `#[param]` may be read and changed
  through the API and the web UI, which restarts the node. A field without
  `#[param]` is fixed by the code and invisible to them.

```rust
/// Keeps the events of one action.
#[derive(Debug, Clone, Operator)]
#[operator("acme/only-action@1", check = Self::check_action)]
pub struct OnlyAction {
    /// The action to keep.
    #[param(default = "click")]
    pub action: String,
    /// Events per second above which the node warns.
    #[param(default = 1000, min = 1, max = 100000)]
    pub warn_above: u64,
}
```

`#[param(default = ..., min = ..., max = ...)]` sets the default and the
bounds the API enforces. `check = path` names a function that refuses what
bounds cannot express; it runs when the application starts and before the
API stores a change.

A parameter can be an integer, a float, `bool`, `String`, `Duration`
(written `"5m"`, `"250ms"`), `Secret` (the text, or `{"secret": "name"}`,
read from the environment variable `STREAMING_SECRET__<TENANT>__<NAME>` and
revealed with `ctx.reveal`), `serde_json::Value`, an `Option` of these, or an
enumeration deriving `Param` (shown as a list of choices).

The stored configuration of a node is its structure as JSON: camelCase
names, unknown fields refused. Changing how an operator serializes changes
the declaration of every graph naming it.

Three shapes cannot derive `Operator`: a tuple structure, a generic
structure and an enumeration. An operator that works on several types
implements its trait for several types instead (`impl<T> Sink<T>`).

## Sources

A source is started once per partition, with a pipe to push into.

```rust
impl Source for Generator {
    type Out = Vec<u8>;

    /// How many partitions there are now. Asked again every
    /// `replan_interval`: a change adds or retires slices.
    async fn partitions<P: Providers>(&self, ctx: &Context<P>) -> Result<u32, OpError> {
        Ok(self.partitions)
    }

    /// Push the records of `partition` that come after `after`.
    async fn start<P: Providers>(
        &self,
        ctx: &Context<P>,
        partition: u32,
        after: Option<Position>,
        out: SourcePipe<Vec<u8>>,
    ) -> Result<(), OpError> {
        let mut next = after.map_or(0, |position| position.2 + 1);
        loop {
            out.send(Position(0, 0, next), ctx.clock.now_ms(), event(next)).await?;
            next += 1;
        }
    }
}
```

What the `SourcePipe` offers, and what it expects:

| Call | Meaning |
|---|---|
| `send(position, event_time, value)` | Push one record. Waits while downstream is full: this is the backpressure. |
| `committed()` / `take_committed()` | The latest position the engine committed, to acknowledge to a broker. A source learns of a commit after it happened. |
| `end(position)` | Where the partition ends now, so the engine can tell an idle partition from a reader that lags. Never before a record already pushed. |
| `read(&connector, partition, after)` | Hand the partition to an existing `SourceConnector`, which the engine then drives directly. |
| `room()`, `closed()` | How much the pipe takes now; wait until the node stops. |

`after` is the last position the engine committed: a source resumes right
behind it, so a record is never counted twice. Returning from `start`
while the pipe is open is a failure, and the partition is started again
after a pause.

A source on a real broker should be built on a connector (`out.read`), as
the Pulsar source is. A source that pushes acknowledges late (the last
acknowledgement before a slice stops may never reach the broker, which then
redelivers), and the engine believes what it says of the end of its
partition.

## Transformers

### Stateless: a function of A to B

When a step only turns one element into another, implement one function and
nothing else:

```rust
#[derive(Debug, Clone, Operator)]
#[operator("acme/scale@1")]
pub struct Scale {
    #[param(default = 2, min = 1, max = 1000)]
    pub factor: u64,
}

impl StatelessTransformer<u64> for Scale {
    type Out = u64;
    fn apply(&self, value: &u64) -> u64 { value * self.factor }
}

let scaled = graph.map(numbers, "scale", Scale { factor: 3 });
```

`StatelessFilterMap` is the same, but `apply` returns `Option<Out>`, and
`None` drops the element:

```rust
impl StatelessFilterMap<String> for Parse {
    type Out = u64;
    fn apply(&self, text: &String) -> Option<u64> { text.parse().ok() }
}

let numbers = graph.filter_map(texts, "parse", Parse);
```

`json_decoder!` declares one decoding JSON, from `Vec<u8>` or `String`, into a
type of the application's, under a reference the application names; what
does not decode is dropped. The demonstration decodes its events this way:

```rust
json_decoder! {
    /// Decodes a JSON event.
    pub struct Decode("demo/decode@1") -> Event;
}

let decoded = graph.filter_map(events, "decode", Decode);
```

A generic `JsonDecoder<T>` would have one reference for every `T`: the
registry keeps one factory per reference and input type, so two graphs
decoding into two types would share one.

The operator must be `Clone`. The node is an ordinary map, stored and shown
under the operator's own reference and configuration.

### Transformer: a pipe in, a pipe out

For anything else, `Transformer<In>` receives a `Pipe<In>` and says what
happens to it:

```rust
impl Transformer<Event> for ByUser {
    type Out = Keyed<Event>;
    fn transform(&self, input: Pipe<Event>) -> Pipe<Keyed<Event>> {
        input.key_by(|event| event.user.clone())
    }
}
```

The methods of `Pipe`:

| On | Method | Gives | Notes |
|---|---|---|---|
| `Pipe<T>` | `map(f: &T -> O)` | `Pipe<O>` | |
| `Pipe<T>` | `filter(f: &T -> bool)` | `Pipe<T>` | |
| `Pipe<T>` | `filter_map(f: &T -> Option<O>)` | `Pipe<O>` | |
| `Pipe<T>` | `flat_map(f: (&T, &mut Vec<O>))` | `Pipe<O>` | zero, one or several elements |
| `Pipe<T>` | `key_by(f: &T -> K)` | `Pipe<Keyed<T>>` | `K: Serialize` |
| `Pipe<Keyed<T>>` | `window(lateness, assign)` | `Pipe<Windows<T>>` | `assign(ts, &mut Vec<Window>)` |
| `Pipe<Keyed<T>>` | `window_with(assigner)` | `Pipe<Windows<T>>` | a `WindowAssigner` |
| `Pipe<Windows<T>>` | `aggregate(init, add, merge)` | `Pipe<Windowed<A>>` | `A` is stored, so `Serialize` |
| `Pipe<Windows<T>>` | `aggregate_with(aggregator)` | `Pipe<Windowed<Out>>` | an `Aggregator` |

The first four may follow one another in one transformer. A `key_by`, a
window and an aggregate are each a node of their own: a transformer that
chains two of them is refused when it is registered, with a message saying
why. What a transformer is must not depend on its configuration.

### Windows

A window assigns each keyed element to the event-time windows that contain
its timestamp. It keeps no state.

```rust
pub trait WindowAssigner: Send + Sync + 'static {
    /// Push the windows containing `ts` into `out`.
    fn assign(&self, ts: Timestamp, out: &mut Vec<Window>);
    /// How long after its end a window still accepts records.
    fn allowed_lateness(&self) -> Duration { Duration::ZERO }
}
```

A window closes when the watermark (the highest event time seen, minus the
graph's `max_out_of_orderness`) passes its end plus its allowed lateness. A
record arriving for a closed window is dropped and counted.

### Aggregates

An aggregate keeps one state per key and per window, and emits it when the
window closes, as a `Windowed<A> { window, key, value }`. `merge` is
required: the state of one key may be spread over several partitions, and
the emitting slice merges them.

```rust
input.aggregate(
    || 0_u64,                       // the state of one key in one window
    |count, _element| *count += 1,  // an element comes
    |count, other| *count += other, // two partitions each held a part
)
```

`Aggregator` does the same when the result differs from the state (an
average kept as sum and count, say): `init`, `add`, `merge` and `finish`.
The state is committed to the store with the source position, in one
command: this is what makes the result exactly-once. Keep it small (a store
value is limited to 5 MB on Materia KV).

## Sinks

A sink receives a pipe of batches and confirms each one.

```rust
impl Sink<Event> for Archive {
    async fn start<P: Providers>(&self, ctx: &Context<P>, mut input: SinkPipe<Event>) -> Result<(), OpError> {
        while let Some(batch) = input.next().await {
            for element in batch.iter() {
                // element.id, element.value, element.ts, element.key, element.origin
            }
            batch.done();               // or batch.failed("why")
        }
        Ok(())
    }
}
```

A batch is written only once its sink said `done()`. If the sink says it
failed, asks for the next batch, returns, or the worker dies, the batch
comes back with the **same emit ids**. The destination must therefore be
idempotent on `element.id` for the result to be exactly-once: write it as a
row key, a message property, a deduplication id.

A sink built on an existing `SinkConnector` hands it the batches with
`input.write_with(&connector)`, as the Warp 10 and Pulsar sinks do.
`impl<T> Sink<T> for Archive` makes a sink of any type.

### Split: one branch per element

```rust
impl Split<Event> for Check {
    fn branch(&self, event: &Event) -> usize { usize::from(event.amount < 0.0) }
}

let [valid, invalid] = graph.split(decoded, "check", Check, ["valid", "invalid"]);
```

`branch` returns an index into the names given to `split`; an index with no
branch drops the element. The split node runs the operator once per element
and wraps it in a `Routed<T>` (its branch and an `Arc` of the element, never
copied); each `std/branch@1` node keeps its own and unwraps them. The routing
and its parameters live in one node: changing them through the API cannot
make two branches disagree. Every branch carries the type of what was split.

## Building a graph

`GraphBuilder` connects typed nodes and registers the operators it names:

| Method | Adds |
|---|---|
| `source(id, source)` | a source node; gives `Node<S::Out>` |
| `map(upstream, id, op)` | a `StatelessTransformer` |
| `filter_map(upstream, id, op)` | a `StatelessFilterMap` |
| `union(inputs, id)` | a `std/union@1` node fed by every node of `inputs`, all of one type; gives that type |
| `split(upstream, id, op, [names])` | a node running a `Split` once per element, and one `std/branch@1` node per name, `<id>-<name>`; gives one `Node` per branch, of the type of `upstream` |
| `then(upstream, id, transformer)` | any `Transformer` |
| `sink(upstream, id, sink)` | a sink |
| `edge(from, to)` | one more edge: a node fed by several others (types checked when the application starts, not by the compiler) |
| `node(id, operator)` | an operator registered by hand, untyped (`.typed::<T>()`) |
| `settings(GraphSettings { .. })` | watermark, epochs, backpressure and garbage collection settings |

Each method only accepts an operator written for what flows out of the node
it is connected to. `App::new().tenant(..).graph(builder).main()` checks
every graph when the binary starts and refuses to run otherwise.

## Catalogue

The operators the project ships. *In* and *out* are the types a node takes
and gives; *any* means the operator is registered for every type.

### Sources

| Reference | Crate (feature) | Out | Parameters |
|---|---|---|---|
| `std/pulsar-source@1` | `streaming-pulsar` (`pulsar`) | `PulsarMessage` | `serviceUrl`, `topic`, `subscription` (required); `token` (secret), `tlsTrustCertsFile`, `eventTime` (`publish` \| `event`, default `publish`), `initialPosition` (`earliest` \| `latest`, default `earliest`), `receiverQueueSize` (default 1000) |

`PulsarMessage` carries `key`, `payload`, `properties`, `publish_time_ms`,
`event_time_ms`, `topic` and the partition. One slice per partition of the
topic, through a durable Failover subscription.

### Transformers

| Reference | Crate (feature) | In → Out | Parameters |
|---|---|---|---|
| `std/identity@1` | `streaming-core` | any → same | none |
| `std/union@1` | `streaming-core` | any → same, from several inputs | none |
| `std/flatten@1` | `streaming-core` | `Vec<T>` → `T` | none; one element at a time, in order (`T: Clone`) |
| `std/branch@1` | `streaming-core` | `Routed<T>` → `T` | `branch`, fixed by the code; added by `GraphBuilder::split`, registered for its type only |
| `std/tumbling@1` | `streaming-core` | `Keyed<T>` → `Windows<T>` | `size` (≥ 1 ms), `allowedLateness` (default `0s`) |
| `std/sliding@1` | `streaming-core` | `Keyed<T>` → `Windows<T>` | `size`, `slide` (≤ `size`), `allowedLateness` |
| `std/count@1` | `streaming-core` | `Windows<T>` → `Windowed<u64>` | none |
| `std/count-to-json@1` | `streaming` | `Windowed<u64>` → JSON | none; gives `{key, count, windowStart, windowEnd}` |
| `std/count-to-gts@1` | `streaming-warp10` (`warp10`) | `Windowed<u64>` → `Gts` | `class` (required), `label` (default `key`) |
| `std/pulsar-key@1` | `streaming-pulsar` (`pulsar`) | `PulsarMessage` → `Keyed<PulsarMessage>` | none; keys by the message key |
| `std/count-to-pulsar@1` | `streaming-pulsar` (`pulsar`) | `Windowed<u64>` → `PulsarOutput` | none; JSON payload, key as message key |

### Sinks

| Reference | Crate (feature) | In | Parameters | Delivery |
|---|---|---|---|---|
| `std/warp10-sink@1` | `streaming-warp10` (`warp10`) | `Gts` | `endpoint`, `token` (secret, required); `timeUnit` (`ms` \| `us` \| `ns`, default `us`), `timeout` (default `10s`), `maxBatchBytes` (default 1 MiB) | idempotent by nature: a point written twice is written once |
| `std/pulsar-sink@1` | `streaming-pulsar` (`pulsar`) | `PulsarOutput` | `serviceUrl`, `topic` (required); `token`, `tlsTrustCertsFile`, `producerName`, `sendTimeoutMs` (default 30000), `brokerDedup` | at-least-once, emit id in a message property; effectively-once with `brokerDedup` and broker deduplication |
| `std/kv-sink@1` | `streaming-runtime` | JSON | `retain` (default 500) | idempotent: a row per emit id; keeps the latest rows only, for inspection (the UI's *Résultats* tab) |

`https://` towards Warp 10 needs the `tls` feature (`pulsar+ssl://` does not:
the Magnetar client always carries its TLS). The feature in parentheses is the
one of `streaming` that registers the operator (none is on by default).

## Not yet

Being built in other threads of work, and not on `main`:

- **`std/batch@1`**: gather elements into a `Vec<A>` until a maximum wait
  (required) or an optional maximum size, beside the event-time windows.

Not started: a dedicated output for late records or for what a decoder
refuses, session windows, joins, a keyed process function with timers,
and standard aggregates beyond `count`.
