//! End-to-end run of the engine on tokio with an in-memory KV: one worker,
//! one graph (source -> key_by -> window -> count -> two sinks), real time.

use async_trait::async_trait;
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use streaming_core::{
    Clock, DynKv, Emit, GraphBuilder, GraphSpec, Kv, KvError, MemKv, NoSecrets, Position, Registry, SinkConnector,
    SinkError, SliceKey, Split, SystemClock, TypeTag, Window, Windowed, WorkerId,
    control::{GraphIndexEntry, SliceSnapshot, WorkerSnapshot},
    datum, keys,
};
use streaming_runtime::{NoLoad, RuntimeEnv, Worker, WorkerConfig, builtin, inspect::slice_head, memlog::MemLog};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
struct Event {
    key: String,
}

/// Number of writes and last value of one emit id.
type Seen = (u32, Option<Windowed<u64>>);

/// Remembers every element by emit id and how many times it was written.
#[derive(Default)]
struct Collect {
    seen: Mutex<BTreeMap<String, Seen>>,
}

#[async_trait]
impl SinkConnector for Collect {
    async fn write(&self, batch: &[Emit]) -> Result<(), SinkError> {
        let mut seen = self.seen.lock();
        for emit in batch {
            let entry = seen.entry(emit.id.to_string()).or_insert((0, None));
            entry.0 += 1;
            entry.1 = emit.value.downcast_ref::<Windowed<u64>>().cloned();
        }
        Ok(())
    }
}

fn spec() -> GraphSpec { spec_idle_after("200ms") }

fn spec_idle_after(idle_timeout: &str) -> GraphSpec {
    serde_json::from_value(serde_json::json!({
        "tenant": "acme",
        "graph": "count-per-key",
        "version": 1,
        "settings": {
            "maxOutOfOrderness": "50ms",
            "idleTimeout": idle_timeout,
            "linger": "20ms",
            "flushInterval": "50ms",
            "gcGrace": "200ms",
            "maxBatch": 64,
            "chunkSize": 16
        },
        "nodes": [
            {"id": "events", "op": "test/log@1"},
            {"id": "by-key", "op": "test/key@1"},
            {"id": "win", "op": "std/tumbling@1", "config": {"size": "500ms"}},
            {"id": "count", "op": "std/count@1"},
            {"id": "counts", "op": "test/collect@1"},
            {"id": "raw", "op": "test/raw@1"}
        ],
        "edges": [
            ["events", "by-key"], ["by-key", "win"], ["win", "count"], ["count", "counts"],
            ["events", "raw"]
        ]
    }))
    .unwrap()
}

async fn submit(kv: &DynKv, spec: &GraphSpec) {
    let graph = spec.key();
    kv.set_json(&keys::graph_spec(&graph), spec).await.unwrap();
    kv.hset_json(
        keys::CLUSTER_GRAPHS,
        &graph.to_string(),
        &GraphIndexEntry {
            tenant: spec.tenant.clone(),
            graph: spec.graph.clone(),
            version: spec.version,
            updated_ms: 0,
            deleting: false,
            orphan: false,
            fingerprint: None,
        },
    )
    .await
    .unwrap();
    kv.incr(keys::CLUSTER_EPOCH).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn counts_every_record_exactly_once() {
    let providers = TokioProviders::new();
    let store = MemKv::shared();
    let kv: DynKv = store.clone();
    let log = MemLog::with_retention(3, true);
    let counts = Arc::new(Collect::default());
    let raw = Arc::new(Collect::default());

    let mut registry = Registry::<TokioProviders>::standard();
    builtin::register(&mut registry);
    {
        let log = log.clone();
        registry.register_source::<Event>("test/log@1", "", move |context, _| {
            Ok(log.source(context.providers.clone()))
        });
    }
    registry.register_key("test/key@1", "", |event: &Event| event.key.clone());
    for (op, sink) in [("test/collect@1", counts.clone()), ("test/raw@1", raw.clone())] {
        registry.register_sink(op, "", TypeTag::Any, move |_, _| {
            Ok(sink.clone() as Arc<dyn SinkConnector>)
        });
    }

    let mut config = WorkerConfig::new(WorkerId::new("worker-a").unwrap());
    config.tick = Duration::from_millis(50);
    config.controller_grace = Duration::ZERO;
    let worker = Worker::new(
        RuntimeEnv {
            shared: Arc::default(),
            providers,
            kv: kv.clone(),
            clock: Arc::new(SystemClock),
            secrets: Arc::new(NoSecrets),
            registry: Arc::new(registry),
        },
        config,
    )
    .with_load_probe(Arc::new(NoLoad));
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(worker.run(shutdown.clone()));

    submit(&kv, &spec()).await;

    // Produce over about one second so that several windows are touched.
    let clock = SystemClock;
    let mut expected: BTreeMap<(Window, String), u64> = BTreeMap::new();
    let total = 600_u64;
    for i in 0..total {
        let key = format!("user-{}", i % 7);
        let ts = clock.now_ms();
        let start = ts.div_euclid(500) * 500;
        *expected
            .entry((Window::new(start, start + 500), key.clone()))
            .or_default() += 1;
        log.append((i % 3) as u32, ts, datum(Event { key }));
        if i % 20 == 19 {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }

    // Every window closes once the partitions have been idle for a while.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let emitted: u64 = counts
            .seen
            .lock()
            .values()
            .filter_map(|(_, value)| value.as_ref().map(|w| w.value))
            .sum();
        if emitted == total && raw.seen.lock().len() as u64 == total {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {emitted}/{total} counted, {}/{total} raw",
            raw.seen.lock().len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let mut actual: BTreeMap<(Window, String), u64> = BTreeMap::new();
    for (_, value) in counts.seen.lock().values() {
        let windowed = value.as_ref().unwrap();
        let previous = actual.insert((windowed.window, windowed.key::<String>().unwrap()), windowed.value);
        assert!(previous.is_none(), "a (window, key) was emitted under two ids");
    }
    assert_eq!(actual, expected);

    // The worker reports what it runs, and released what it acknowledged.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let snapshot: WorkerSnapshot = kv
        .get_json(&keys::worker_metrics(&WorkerId::new("worker-a").unwrap()))
        .await
        .unwrap()
        .unwrap();
    let slices: Vec<&SliceSnapshot> = snapshot.slices.iter().collect();
    assert_eq!(slices.len(), 4, "three source partitions and one emitter");
    assert!(slices.iter().all(|slice| slice.epoch > 0 && slice.fenced == 0));
    assert!(snapshot.actors.iter().any(|actor| actor.node.as_str() == "count"));
    assert_eq!((0..3).map(|p| log.retained(p)).sum::<usize>(), 0);

    // Emitted windows are garbage-collected from the KV: their state at once,
    // their index entries once `gc_grace` has passed.
    let leftovers = || -> Vec<String> {
        store
            .keys()
            .into_iter()
            .filter(|key| key.contains(":w:") || key.contains(":idx:"))
            .collect()
    };
    for _ in 0..100 {
        if leftovers().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(leftovers().is_empty(), "window state left behind: {:?}", leftovers());

    shutdown.cancel();
    running.await.unwrap();
}

/// A store that answers, for the hashes it was given, with what they held
/// earlier, until told to stop: what an instance sees when it reads a commit
/// log just before another instance writes its last commit to it.
struct Behind {
    store: Arc<MemKv>,
    earlier: Mutex<BTreeMap<String, Fields>>,
}

/// The fields of a hash.
type Fields = Vec<(String, Vec<u8>)>;

#[async_trait]
impl Kv for Behind {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> { self.store.get(key).await }
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), KvError> { self.store.set(key, value).await }
    async fn del(&self, keys: Vec<String>) -> Result<(), KvError> { self.store.del(keys).await }
    async fn incr(&self, key: &str) -> Result<i64, KvError> { self.store.incr(key).await }
    async fn hsetnx(&self, key: &str, field: &str, value: Vec<u8>) -> Result<bool, KvError> {
        self.store.hsetnx(key, field, value).await
    }
    async fn hset(&self, key: &str, entries: Vec<(String, Vec<u8>)>) -> Result<(), KvError> {
        self.store.hset(key, entries).await
    }
    async fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>, KvError> {
        self.store.hget(key, field).await
    }
    async fn hgetall(&self, key: &str) -> Result<Fields, KvError> {
        if let Some(fields) = self.earlier.lock().get(key) {
            return Ok(fields.clone());
        }
        self.store.hgetall(key).await
    }
    async fn hdel(&self, key: &str, fields: Vec<String>) -> Result<(), KvError> { self.store.hdel(key, fields).await }
}

fn registry(log: &Arc<MemLog>, counts: &Arc<Collect>, raw: &Arc<Collect>) -> Registry<TokioProviders> {
    let mut registry = Registry::<TokioProviders>::standard();
    builtin::register(&mut registry);
    {
        let log = log.clone();
        registry.register_source::<Event>("test/log@1", "", move |context, _| {
            Ok(log.source(context.providers.clone()))
        });
    }
    registry.register_key("test/key@1", "", |event: &Event| event.key.clone());
    for (op, sink) in [("test/collect@1", counts.clone()), ("test/raw@1", raw.clone())] {
        registry.register_sink(op, "", TypeTag::Any, move |_, _| {
            Ok(sink.clone() as Arc<dyn SinkConnector>)
        });
    }
    registry
}

fn start(kv: DynKv, registry: Registry<TokioProviders>) -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let mut config = WorkerConfig::new(WorkerId::new("worker-a").unwrap());
    config.tick = Duration::from_millis(50);
    config.controller_grace = Duration::ZERO;
    config.restart_backoff = Duration::from_millis(100);
    let worker = Worker::new(
        RuntimeEnv {
            shared: Arc::default(),
            providers: TokioProviders::new(),
            kv,
            clock: Arc::new(SystemClock),
            secrets: Arc::new(NoSecrets),
            registry: Arc::new(registry),
        },
        config,
    )
    .with_load_probe(Arc::new(NoLoad));
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(worker.run(shutdown.clone()));
    (shutdown, running)
}

/// An instance reads the commit log of its slice; the instance it replaces
/// then commits and acknowledges the last records of the partition, which
/// says nothing more. The new instance waits for records that will never
/// come to it, behind a watermark that holds every window back: it must find
/// out from the log that the slice went on without it, and start again.
#[tokio::test(flavor = "current_thread")]
async fn a_slice_behind_what_another_instance_acknowledged_starts_again() {
    let store = MemKv::shared();
    let kv: DynKv = store.clone();
    let log = MemLog::with_retention(1, true);
    let counts = Arc::new(Collect::default());
    let raw = Arc::new(Collect::default());
    let slice: SliceKey = "acme/count-per-key/src.events.0".parse().unwrap();
    let counted = || -> u64 {
        counts
            .seen
            .lock()
            .values()
            .filter_map(|(_, value)| value.as_ref().map(|windowed| windowed.value))
            .sum()
    };
    let committed = |upto: Position| {
        let store = store.clone();
        let slice = slice.clone();
        async move {
            for _ in 0..200 {
                let head = slice_head(store.as_ref(), &slice).await.unwrap();
                if head.and_then(|head| head.position) == Some(upto) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("the slice did not commit up to {upto:?}");
        }
    };

    // Long enough an idle timeout for the first instance to be stopped
    // before it closes the window itself.
    let (shutdown, running) = start(kv.clone(), registry(&log, &counts, &raw));
    submit(&kv, &spec_idle_after("3s")).await;
    let ts = SystemClock.now_ms();
    let mut last = None;
    for _ in 0..5 {
        last = Some(log.append(0, ts, datum(Event { key: "user".into() })));
    }
    let first = last.unwrap();
    committed(first).await;
    let mut earlier = BTreeMap::new();
    for key in [keys::slice_log(&slice), keys::slice_seal(&slice)] {
        earlier.insert(key.clone(), store.hgetall(&key).await.unwrap());
    }
    for _ in 0..5 {
        last = Some(log.append(0, ts, datum(Event { key: "user".into() })));
    }
    committed(last.unwrap()).await;
    for _ in 0..200 {
        if log.retained(0) == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(log.retained(0), 0, "the last records were not acknowledged");
    shutdown.cancel();
    running.await.unwrap();
    assert_eq!(counted(), 0, "the first instance closed the window itself");

    // The next instance reads the log as it was after the first records.
    let behind = Arc::new(Behind {
        store: store.clone(),
        earlier: Mutex::new(earlier),
    });
    let (shutdown, running) = start(behind.clone(), registry(&log, &counts, &raw));
    let worker = WorkerId::new("worker-a").unwrap();
    let mut started = false;
    for _ in 0..300 {
        let snapshot: Option<WorkerSnapshot> = kv.get_json(&keys::worker_metrics(&worker)).await.unwrap();
        started = snapshot.is_some_and(|snapshot| {
            snapshot
                .slices
                .iter()
                .any(|running| running.slice == slice.slice && running.position == Some(first))
        });
        if started {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(started, "the second instance did not start behind the first");
    behind.earlier.lock().clear();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while counted() != 10 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the slice stayed behind what was acknowledged: {} of 10 counted",
            counted()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    shutdown.cancel();
    running.await.unwrap();
}

/// A numbered event.
#[derive(Debug, Clone)]
struct Numbered(u64);

/// Stands for `test/numbers@1`, registered by hand.
#[derive(Debug, Clone, streaming_core::Operator)]
#[operator("test/numbers@1")]
struct Numbers;

/// Stands for a sink registered by hand, `test/all@1` or `test/odd@1`.
#[derive(Debug, Clone, streaming_core::Operator)]
#[operator("test/all@1")]
struct All;

#[derive(Debug, Clone, streaming_core::Operator)]
#[operator("test/odd@1")]
struct Odd;

/// Sends odd numbers to the second branch.
#[derive(Debug, Clone, streaming_core::Operator)]
#[operator("test/parity@1")]
struct Parity;

impl Split<Numbered> for Parity {
    fn branch(&self, input: &Numbered) -> usize { (input.0 % 2) as usize }
}

/// How many times each number was written, by emit id.
#[derive(Default)]
struct Written {
    seen: Mutex<BTreeMap<String, u64>>,
}

#[async_trait]
impl SinkConnector for Written {
    async fn write(&self, batch: &[Emit]) -> Result<(), SinkError> {
        let mut seen = self.seen.lock();
        for emit in batch {
            let Some(number) = emit.value.downcast_ref::<Numbered>() else {
                return Err(SinkError("not a number".into()));
            };
            seen.insert(emit.id.to_string(), number.0);
        }
        Ok(())
    }
}

/// A split sends each element to exactly one branch, and a union of the
/// branches gives back every element once.
#[tokio::test(flavor = "current_thread")]
async fn a_split_routes_each_element_once_and_a_union_brings_them_back() {
    let store = MemKv::shared();
    let kv: DynKv = store.clone();
    let log = MemLog::with_retention(3, true);
    let all = Arc::new(Written::default());
    let odd = Arc::new(Written::default());

    let mut graph = GraphBuilder::<TokioProviders>::new("acme", "split-union").settings(spec().settings);
    let numbers = graph.node("numbers", Numbers).typed::<Numbered>();
    let [even_numbers, odd_numbers] = graph.split(numbers, "parity", Parity, ["even", "odd"]);
    let together = graph.union([even_numbers, odd_numbers], "together");
    let to_all = graph.node("all", All);
    graph.edge(together, to_all);
    let to_odd = graph.node("odd", Odd);
    graph.edge(odd_numbers, to_odd);
    let (mut spec, operators) = graph.into_parts().unwrap();
    spec.version = 1;

    let mut registry = Registry::<TokioProviders>::standard();
    builtin::register(&mut registry);
    {
        let log = log.clone();
        registry.register_source::<Numbered>("test/numbers@1", "", move |context, _| {
            Ok(log.source(context.providers.clone()))
        });
    }
    for (op, sink) in [("test/all@1", all.clone()), ("test/odd@1", odd.clone())] {
        registry.register_sink(op, "", TypeTag::of::<Numbered>(), move |_, _| {
            Ok(sink.clone() as Arc<dyn SinkConnector>)
        });
    }
    assert!(operators.register(&mut registry).is_empty());
    let nodes: Vec<&str> = spec.nodes.iter().map(|node| node.id.as_str()).collect();
    assert_eq!(
        nodes,
        [
            "numbers",
            "parity",
            "parity-even",
            "parity-odd",
            "together",
            "all",
            "odd"
        ]
    );

    let (shutdown, running) = start(kv.clone(), registry);
    submit(&kv, &spec).await;
    let total = 300_u64;
    for n in 0..total {
        log.append((n % 3) as u32, SystemClock.now_ms(), datum(Numbered(n)));
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while all.seen.lock().len() < total as usize || odd.seen.lock().len() < (total / 2) as usize {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} of {total} brought back, {} of {} odd",
            all.seen.lock().len(),
            odd.seen.lock().len(),
            total / 2
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut everything: Vec<u64> = all.seen.lock().values().copied().collect();
    everything.sort_unstable();
    assert_eq!(
        everything,
        (0..total).collect::<Vec<_>>(),
        "each number once through the union"
    );
    let mut odds: Vec<u64> = odd.seen.lock().values().copied().collect();
    odds.sort_unstable();
    assert_eq!(odds, (0..total).filter(|n| n % 2 == 1).collect::<Vec<_>>());
    shutdown.cancel();
    running.await.unwrap();
}

/// Stands for `test/more-numbers@1`, a second source registered by hand.
#[derive(Debug, Clone, streaming_core::Operator)]
#[operator("test/more-numbers@1")]
struct MoreNumbers;

/// A union of two sources: each partition of each source is a slice of its
/// own, and every element reaches the sink once.
#[tokio::test(flavor = "current_thread")]
async fn a_union_of_two_sources_writes_each_element_once() {
    let kv: DynKv = MemKv::shared();
    let (first, second) = (MemLog::with_retention(2, true), MemLog::with_retention(3, true));
    let all = Arc::new(Written::default());

    let mut graph = GraphBuilder::<TokioProviders>::new("acme", "two-sources").settings(spec().settings);
    let numbers = graph.node("numbers", Numbers).typed::<Numbered>();
    let more = graph.node("more", MoreNumbers).typed::<Numbered>();
    let together = graph.union([numbers, more], "together");
    let to_all = graph.node("all", All);
    graph.edge(together, to_all);
    let (mut spec, operators) = graph.into_parts().unwrap();
    spec.version = 1;

    let mut registry = Registry::<TokioProviders>::standard();
    builtin::register(&mut registry);
    for (op, log) in [
        ("test/numbers@1", first.clone()),
        ("test/more-numbers@1", second.clone()),
    ] {
        registry.register_source::<Numbered>(op, "", move |context, _| Ok(log.source(context.providers.clone())));
    }
    let sink = all.clone();
    registry.register_sink("test/all@1", "", TypeTag::of::<Numbered>(), move |_, _| {
        Ok(sink.clone() as Arc<dyn SinkConnector>)
    });
    assert!(operators.register(&mut registry).is_empty());

    let (shutdown, running) = start(kv.clone(), registry);
    submit(&kv, &spec).await;
    let total = 400_u64;
    for n in 0..total {
        let (log, partitions) = if n % 2 == 0 { (&first, 2) } else { (&second, 3) };
        log.append((n % partitions) as u32, SystemClock.now_ms(), datum(Numbered(n)));
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while all.seen.lock().len() < total as usize {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} of {total} written",
            all.seen.lock().len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut everything: Vec<u64> = all.seen.lock().values().copied().collect();
    everything.sort_unstable();
    assert_eq!(everything, (0..total).collect::<Vec<_>>());
    shutdown.cancel();
    running.await.unwrap();
}

/// A number that can be collected into a window's state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Collected(u64);

/// Stands for `test/collected@1` and `test/batches@1`, registered by hand.
#[derive(Debug, Clone, streaming_core::Operator)]
#[operator("test/collected@1")]
struct CollectedNumbers;

#[derive(Debug, Clone, streaming_core::Operator)]
#[operator("test/batches@1")]
struct Batches;

/// Every batch written, by emit id.
#[derive(Default)]
struct WrittenBatches {
    seen: Mutex<BTreeMap<String, Vec<u64>>>,
}

#[async_trait]
impl SinkConnector for WrittenBatches {
    async fn write(&self, batch: &[Emit]) -> Result<(), SinkError> {
        let mut seen = self.seen.lock();
        for emit in batch {
            let Some(numbers) = emit.value.downcast_ref::<Vec<Collected>>() else {
                return Err(SinkError("not a batch".into()));
            };
            seen.insert(emit.id.to_string(), numbers.iter().map(|number| number.0).collect());
        }
        Ok(())
    }
}

/// A batch gathers the elements of every partition over a window, in
/// pieces no larger than asked, each element in exactly one of them.
#[tokio::test(flavor = "current_thread")]
async fn a_batch_gathers_every_element_once_in_pieces_no_larger_than_asked() {
    let kv: DynKv = MemKv::shared();
    let log = MemLog::with_retention(3, true);
    let batches = Arc::new(WrittenBatches::default());

    let mut graph = GraphBuilder::<TokioProviders>::new("acme", "batches").settings(spec().settings);
    let numbers = graph.node("numbers", CollectedNumbers).typed::<Collected>();
    let batched = graph.batch(numbers, "batch", Duration::from_millis(500), Some(7));
    let to_sink = graph.node("out", Batches);
    graph.edge(batched, to_sink);
    let (mut spec, operators) = graph.into_parts().unwrap();
    spec.version = 1;
    let nodes: Vec<&str> = spec.nodes.iter().map(|node| node.id.as_str()).collect();
    assert_eq!(
        nodes,
        ["numbers", "batch-key", "batch-window", "batch-collect", "batch", "out"]
    );

    let mut registry = Registry::<TokioProviders>::standard();
    builtin::register(&mut registry);
    {
        let log = log.clone();
        registry.register_source::<Collected>("test/collected@1", "", move |context, _| {
            Ok(log.source(context.providers.clone()))
        });
    }
    let sink = batches.clone();
    registry.register_sink("test/batches@1", "", TypeTag::of::<Vec<Collected>>(), move |_, _| {
        Ok(sink.clone() as Arc<dyn SinkConnector>)
    });
    assert!(operators.register(&mut registry).is_empty());

    let (shutdown, running) = start(kv.clone(), registry);
    submit(&kv, &spec).await;
    let total = 300_u64;
    for n in 0..total {
        log.append((n % 3) as u32, SystemClock.now_ms(), datum(Collected(n)));
        if n % 30 == 29 {
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    let written = || -> usize { batches.seen.lock().values().map(Vec::len).sum() };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while written() < total as usize {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} of {total} batched",
            written()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let seen = batches.seen.lock().clone();
    assert!(
        seen.values().all(|batch| !batch.is_empty() && batch.len() <= 7),
        "a batch larger than asked"
    );
    let mut everything: Vec<u64> = seen.values().flatten().copied().collect();
    everything.sort_unstable();
    assert_eq!(
        everything,
        (0..total).collect::<Vec<_>>(),
        "each number in exactly one batch"
    );
    shutdown.cancel();
    running.await.unwrap();
}
