//! What the control plane and the destinations make a running graph do, on
//! tokio with an in-memory KV: backpressure carried across an aggregate,
//! pausing a graph or a node, draining.

use async_trait::async_trait;
use moonpool_core::TokioProviders;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use streaming_core::{
    Clock, DynKv, Emit, GraphKey, GraphSpec, MemKv, NoSecrets, NodeId, Position, Registry, SinkConnector, SinkError,
    SliceKey, SystemClock, TypeTag, WorkerId,
    control::{DesiredState, GraphIndexEntry, RunState, WorkerSnapshot},
    datum, keys,
};
use streaming_runtime::{NoLoad, RuntimeEnv, Worker, WorkerConfig, inspect::slice_head, memlog::MemLog};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
struct Event {
    key: String,
}

/// Counts what it is handed; while `blocked`, never answers.
#[derive(Default)]
struct Gate {
    blocked: AtomicBool,
    written: AtomicU64,
    batches: parking_lot::Mutex<Vec<usize>>,
}

#[async_trait]
impl SinkConnector for Gate {
    async fn write(&self, batch: &[Emit]) -> Result<(), SinkError> {
        while self.blocked.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.written.fetch_add(batch.len() as u64, Ordering::Relaxed);
        self.batches.lock().push(batch.len());
        Ok(())
    }
}

struct Cluster {
    kv: DynKv,
    log: Arc<MemLog>,
    counts: Arc<Gate>,
    raw: Arc<Gate>,
    graph: GraphKey,
    shutdown: CancellationToken,
    running: tokio::task::JoinHandle<()>,
}

impl Cluster {
    async fn start(settings: serde_json::Value) -> Self {
        Self::with_graph(
            settings,
            serde_json::json!([
                {"id": "events", "op": "test/log@1"},
                {"id": "by-key", "op": "test/key@1"},
                {"id": "win", "op": "std/tumbling@1", "config": {"size": "100ms"}},
                {"id": "count", "op": "std/count@1"},
                {"id": "counts", "op": "test/counts@1"},
                {"id": "raw", "op": "test/raw@1"}
            ]),
            serde_json::json!([
                ["events", "by-key"],
                ["by-key", "win"],
                ["win", "count"],
                ["count", "counts"],
                ["events", "raw"]
            ]),
        )
        .await
    }

    async fn with_graph(settings: serde_json::Value, nodes: serde_json::Value, edges: serde_json::Value) -> Self {
        let kv: DynKv = MemKv::shared();
        let log = MemLog::with_retention(1, true);
        let counts = Arc::new(Gate::default());
        let raw = Arc::new(Gate::default());
        let mut registry = Registry::<TokioProviders>::standard();
        {
            let log = log.clone();
            registry.register_source::<Event>("test/log@1", "", move |context, _| {
                Ok(log.source(context.providers.clone()))
            });
        }
        registry.register_key("test/key@1", "", |event: &Event| event.key.clone());
        for (op, sink) in [("test/counts@1", counts.clone()), ("test/raw@1", raw.clone())] {
            registry.register_sink(op, "", TypeTag::Any, move |_, _| {
                Ok(sink.clone() as Arc<dyn SinkConnector>)
            });
        }
        let mut config = WorkerConfig::new(WorkerId::new("worker-a").unwrap());
        config.tick = Duration::from_millis(20);
        config.controller_grace = Duration::ZERO;
        let worker = Worker::new(
            RuntimeEnv {
                providers: TokioProviders::new(),
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

        let spec: GraphSpec = serde_json::from_value(serde_json::json!({
            "tenant": "acme",
            "graph": "g",
            "version": 1,
            "settings": settings,
            "nodes": nodes,
            "edges": edges
        }))
        .unwrap();
        let graph = spec.key();
        kv.set_json(&keys::graph_spec(&graph), &spec).await.unwrap();
        kv.hset_json(
            keys::CLUSTER_GRAPHS,
            &graph.to_string(),
            &GraphIndexEntry {
                tenant: spec.tenant.clone(),
                graph: spec.graph.clone(),
                version: 1,
                updated_ms: 0,
                deleting: false,
                orphan: false,
            },
        )
        .await
        .unwrap();
        kv.incr(keys::CLUSTER_EPOCH).await.unwrap();
        Self {
            kv,
            log,
            counts,
            raw,
            graph,
            shutdown,
            running,
        }
    }

    /// Append `n` events, stamped `spacing_ms` apart, ending now.
    fn produce(&self, n: u64, spacing_ms: i64) -> Position {
        let now = SystemClock.now_ms();
        let mut last = Position::default();
        for i in 0..n {
            let ts = now - (n - i) as i64 * spacing_ms;
            last = self.log.append(
                0,
                ts,
                datum(Event {
                    key: format!("k{}", i % 3),
                }),
            );
        }
        last
    }

    /// Once a first event went through, so that the slice is running and
    /// reading: what is produced next arrives while it reads.
    async fn warm_up(&self) {
        self.produce(1, 1);
        eventually("the slice runs", async || self.raw.written.load(Ordering::Relaxed) == 1).await;
        // Past the linger of that first epoch.
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.raw.batches.lock().clear();
    }

    async fn committed(&self) -> Option<Position> {
        let slice: SliceKey = "acme/g/src.events.0".parse().unwrap();
        slice_head(self.kv.as_ref(), &slice)
            .await
            .unwrap()
            .and_then(|head| head.position)
    }

    async fn desire(&self, state: RunState, paused: &[&str]) {
        let desired = DesiredState {
            state,
            paused_nodes: paused.iter().map(|n| NodeId::new(*n).unwrap()).collect(),
            updated_ms: SystemClock.now_ms(),
        };
        self.kv
            .set_json(&keys::graph_desired(&self.graph), &desired)
            .await
            .unwrap();
        self.kv.incr(keys::CLUSTER_EPOCH).await.unwrap();
    }

    async fn snapshot(&self) -> Option<WorkerSnapshot> {
        self.kv
            .get_json(&keys::worker_metrics(&WorkerId::new("worker-a").unwrap()))
            .await
            .unwrap()
    }

    async fn stop(self) {
        self.shutdown.cancel();
        self.running.await.unwrap();
    }
}

async fn eventually(what: &str, mut done: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !done().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The emitter cannot hand anything to its sink: the slice feeding the
/// aggregate stops reading once `max_pending_windows` windows are closed and
/// not emitted, although nothing blocks it along its own edges.
#[tokio::test(flavor = "current_thread")]
async fn backpressure_crosses_an_aggregate() {
    let cluster = Cluster::start(serde_json::json!({
        "maxOutOfOrderness": "0ms",
        "linger": "5ms",
        "maxBatch": 10,
        "maxPendingWindows": 2,
        "flushInterval": "20ms"
    }))
    .await;
    cluster.counts.blocked.store(true, Ordering::Relaxed);
    // 2,000 events over 200 windows of 100 ms.
    let end = cluster.produce(2_000, 10);
    eventually("the source throttles", async || {
        cluster
            .snapshot()
            .await
            .is_some_and(|s| s.slices.iter().any(|slice| slice.throttled > 0))
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let held = cluster.committed().await;
    assert!(held < Some(end), "read everything: {held:?}");
    // About two windows of ten events each, plus what an epoch carries.
    assert!(held.is_some_and(|at| at.1 <= 200), "read too far ahead: {held:?}");
    cluster.counts.blocked.store(false, Ordering::Relaxed);
    eventually("everything is read once emission resumes", async || {
        cluster.committed().await == Some(end)
    })
    .await;
    cluster.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_paused_graph_reads_nothing_and_resumes() {
    let cluster = Cluster::start(serde_json::json!({"linger": "5ms"})).await;
    let first = cluster.produce(10, 1);
    eventually("the first events are read", async || {
        cluster.committed().await == Some(first)
    })
    .await;
    cluster.desire(RunState::Paused, &[]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = cluster.raw.written.load(Ordering::Relaxed);
    let second = cluster.produce(10, 1);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(cluster.committed().await, Some(first));
    assert_eq!(cluster.raw.written.load(Ordering::Relaxed), before);
    cluster.desire(RunState::Running, &[]).await;
    eventually("reading resumes", async || cluster.committed().await == Some(second)).await;
    cluster.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_paused_node_holds_its_input() {
    let cluster = Cluster::start(serde_json::json!({"linger": "5ms"})).await;
    cluster.desire(RunState::Running, &["raw"]).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    cluster.produce(10, 1);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(cluster.raw.written.load(Ordering::Relaxed), 0);
    cluster.desire(RunState::Running, &[]).await;
    eventually("the node resumes", async || {
        cluster.raw.written.load(Ordering::Relaxed) == 10
    })
    .await;
    cluster.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_drained_graph_stops_reading() {
    let cluster = Cluster::start(serde_json::json!({"linger": "5ms"})).await;
    let first = cluster.produce(10, 1);
    eventually("the first events are read", async || {
        cluster.committed().await == Some(first)
    })
    .await;
    cluster.desire(RunState::Draining, &[]).await;
    // A drained slice ends and the worker forgets it: the snapshot shows
    // nothing running any more (stopped slices, or none at all).
    eventually("every slice stops", async || {
        cluster.snapshot().await.is_some_and(|s| {
            s.slices
                .iter()
                .all(|slice| slice.state == streaming_core::control::ActorState::Stopped)
        })
    })
    .await;
    cluster.produce(10, 1);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(cluster.committed().await, Some(first));
    cluster.stop().await;
}

/// One epoch costs one commit: once a first event is in, the source keeps
/// reading for `linger` to fill its batch.
#[tokio::test(flavor = "current_thread")]
async fn an_epoch_lingers_to_fill_its_batch() {
    let cluster = Cluster::start(serde_json::json!({"linger": "400ms"})).await;
    cluster.warm_up().await;
    cluster.produce(3, 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.produce(3, 1);
    eventually("every event is written", async || {
        cluster.raw.written.load(Ordering::Relaxed) == 7
    })
    .await;
    assert_eq!(*cluster.raw.batches.lock(), [6]);
    cluster.stop().await;
}

/// No epoch holds more than `max_batch` events.
#[tokio::test(flavor = "current_thread")]
async fn an_epoch_holds_at_most_max_batch_events() {
    let cluster = Cluster::start(serde_json::json!({"linger": "200ms", "maxBatch": 4})).await;
    cluster.warm_up().await;
    cluster.produce(1, 1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    cluster.produce(9, 1);
    eventually("every event is written", async || {
        cluster.raw.written.load(Ordering::Relaxed) == 11
    })
    .await;
    let batches = cluster.raw.batches.lock().clone();
    assert!(batches.iter().all(|&n| n <= 4), "{batches:?}");
    cluster.stop().await;
}
