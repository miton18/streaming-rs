//! End-to-end run of the engine on tokio with an in-memory KV: one worker,
//! one graph (source -> key_by -> window -> count -> two sinks), real time.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use streaming_core::{
    Clock, DynKv, Emit, GraphSpec, MemKv, NoSecrets, Registry, SinkConnector, SinkError, SystemClock, TypeTag, Window,
    Windowed, WorkerId,
    control::{GraphIndexEntry, SliceSnapshot, WorkerSnapshot},
    datum, keys,
};
use streaming_runtime::{NoLoad, RuntimeEnv, Worker, WorkerConfig, builtin, memlog::MemLog};
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

fn spec() -> GraphSpec {
    serde_json::from_value(serde_json::json!({
        "tenant": "acme",
        "graph": "count-per-key",
        "version": 1,
        "settings": {
            "maxOutOfOrderness": "50ms",
            "idleTimeout": "200ms",
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
    builtin::register(&mut registry, kv.clone());
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
