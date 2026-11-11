//! Operators and graphs of the demonstration cluster.
//!
//! | reference | kind | types | configuration |
//! |---|---|---|---|
//! | `demo/generator@1` | source | `Vec<u8>` (JSON) | `{"partitions": 4, "rate_per_sec": 2000, "keys": 50}` |
//! | `demo/decode@1` | map | `Vec<u8>` -> `Event` | |
//! | `demo/by-user@1` | key_by | `Event` | |
//! | `demo/slow-sink@1` | sink | any | `{"latency_ms": 0}` |

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_core::{Providers, RandomProvider, TaskProvider, TimeProvider};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use streaming_core::{
    Clock, Emit, GraphSpec, OpError, Registry, SinkConnector, SinkError, SourceConnector,
    SystemClock, TypeTag, datum,
};
use streaming_runtime::memlog::MemLog;

/// What the generator produces, once decoded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Who.
    pub user: String,
    /// What.
    pub action: String,
    /// How much.
    pub amount: f64,
}

/// Records a partition may hold unread before the generator stops feeding it.
const BACKLOG_CAP: usize = 200_000;

struct Generator {
    log: Arc<MemLog>,
    rate_per_sec: AtomicU64,
    keys: AtomicU64,
    started: AtomicBool,
    dropped: AtomicU64,
}

/// The in-process topics behind the `demo/generator@1` sources of a process.
#[derive(Default)]
pub struct Generators {
    topics: Mutex<HashMap<String, Arc<Generator>>>,
}

fn default_partitions() -> u32 {
    4
}
fn default_rate() -> u64 {
    1_000
}
fn default_keys() -> u64 {
    50
}

#[derive(Debug, Deserialize)]
struct GeneratorConfig {
    #[serde(default = "default_partitions")]
    partitions: u32,
    #[serde(default = "default_rate")]
    rate_per_sec: u64,
    #[serde(default = "default_keys")]
    keys: u64,
}

const ACTIONS: [&str; 4] = ["view", "click", "cart", "purchase"];

async fn generate<P: Providers>(providers: P, generator: Arc<Generator>) {
    let clock = SystemClock;
    let tick = Duration::from_millis(50);
    let mut carry = 0.0_f64;
    let mut sequence = 0_u64;
    loop {
        let _ = providers.time().sleep(tick).await;
        let rate = generator.rate_per_sec.load(Ordering::Relaxed) as f64;
        carry += rate * tick.as_secs_f64();
        let count = carry.floor() as u64;
        carry -= count as f64;
        let keys = generator.keys.load(Ordering::Relaxed).max(1);
        let partitions = generator.log.partitions();
        let now = clock.now_ms();
        for _ in 0..count {
            // A few users account for most of the traffic.
            let skew: f64 = providers.random().random_ratio();
            let user = (skew * skew * keys as f64) as u64;
            let partition = (user % u64::from(partitions)) as u32;
            if generator.log.retained(partition) >= BACKLOG_CAP {
                generator.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            sequence += 1;
            let event = Event {
                user: format!("user-{user:03}"),
                action: ACTIONS[(sequence % ACTIONS.len() as u64) as usize].to_owned(),
                amount: (providers.random().random_range(100..10_000_u32) as f64) / 100.0,
            };
            if let Ok(bytes) = serde_json::to_vec(&event) {
                generator.log.append(partition, now, datum(bytes));
            }
        }
    }
}

struct SlowSink<P: Providers> {
    providers: P,
    latency: Duration,
    written: AtomicU64,
}

#[async_trait]
impl<P: Providers> SinkConnector for SlowSink<P> {
    async fn write(&self, batch: &[Emit]) -> Result<(), SinkError> {
        if !self.latency.is_zero() {
            let _ = self.providers.time().sleep(self.latency).await;
        }
        self.written
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct SlowSinkConfig {
    #[serde(default)]
    latency_ms: u64,
}

fn parse<C: for<'de> Deserialize<'de>>(config: &Value) -> Result<C, OpError> {
    let config = if config.is_null() {
        json!({})
    } else {
        config.clone()
    };
    serde_json::from_value(config).map_err(|error| OpError::Config(error.to_string()))
}

/// Register the demonstration operators.
pub fn register<P: Providers>(registry: &mut Registry<P>, generators: Arc<Generators>) {
    registry.register_source::<Vec<u8>>(
        "demo/generator@1",
        "Synthetic events at a configurable rate, as JSON bytes.",
        move |context, config| {
            let config: GeneratorConfig = parse(config)?;
            if config.partitions == 0 || config.partitions > 64 {
                return Err(OpError::Config(
                    "`partitions` must be between 1 and 64".into(),
                ));
            }
            if config.rate_per_sec > 200_000 {
                return Err(OpError::Config(
                    "`rate_per_sec` must be at most 200000".into(),
                ));
            }
            let topic = format!(
                "{}/{}/{}/{}",
                context.tenant, context.graph, context.node, config.partitions
            );
            let generator = generators
                .topics
                .lock()
                .entry(topic.clone())
                .or_insert_with(|| {
                    Arc::new(Generator {
                        log: MemLog::with_retention(config.partitions, true),
                        rate_per_sec: AtomicU64::new(0),
                        keys: AtomicU64::new(1),
                        started: AtomicBool::new(false),
                        dropped: AtomicU64::new(0),
                    })
                })
                .clone();
            generator
                .rate_per_sec
                .store(config.rate_per_sec, Ordering::Relaxed);
            generator.keys.store(config.keys.max(1), Ordering::Relaxed);
            if !generator.started.swap(true, Ordering::Relaxed) {
                drop(context.providers.task().spawn_task(
                    &format!("generator/{topic}"),
                    generate(context.providers.clone(), generator.clone()),
                ));
            }
            Ok(generator.log.source(context.providers.clone()) as Arc<dyn SourceConnector>)
        },
    );
    registry.register_map(
        "demo/decode@1",
        "Decodes a JSON event.",
        |bytes: &Vec<u8>| serde_json::from_slice::<Event>(bytes).ok(),
    );
    registry.register_key(
        "demo/by-user@1",
        "Groups events by user.",
        |event: &Event| event.user.clone(),
    );
    registry.register_sink(
        "demo/slow-sink@1",
        "Discards what it receives after a configurable delay per batch.",
        TypeTag::Any,
        |context, config| {
            let config: SlowSinkConfig = parse(config)?;
            if config.latency_ms > 60_000 {
                return Err(OpError::Config("`latency_ms` must be at most 60000".into()));
            }
            Ok(Arc::new(SlowSink {
                providers: context.providers.clone(),
                latency: Duration::from_millis(config.latency_ms),
                written: AtomicU64::new(0),
            }) as Arc<dyn SinkConnector>)
        },
    );
}

/// The graphs submitted when the demonstration cluster starts.
#[must_use]
pub fn graphs() -> Vec<GraphSpec> {
    let settings = json!({
        "max_out_of_orderness": "500ms",
        "idle_timeout": "3s",
        "linger": "100ms",
        "flush_interval": "500ms",
        "max_batch": 500,
        "chunk_size": 100,
        "mailbox_capacity": 4,
        // Low enough for a slow sink behind the aggregate to hold the source
        // back within half a minute.
        "max_pending_windows": 3
    });
    let specs = [
        json!({
            "tenant": "acme",
            "graph": "count-per-key",
            "settings": settings,
            "nodes": [
                {"id": "events", "op": "demo/generator@1",
                 "config": {"partitions": 4, "rate_per_sec": 2000, "keys": 50}},
                {"id": "decode", "op": "demo/decode@1"},
                {"id": "by-user", "op": "demo/by-user@1"},
                {"id": "window", "op": "std/tumbling@1", "config": {"size": "10s"}},
                {"id": "count", "op": "std/count@1"},
                {"id": "to-row", "op": "std/count-to-json@1"},
                {"id": "results", "op": "std/kv-sink@1", "config": {"retain": 400}},
                {"id": "to-gts", "op": "std/count-to-gts@1",
                 "config": {"class": "events.count", "label": "user"}},
                {"id": "warp10", "op": "demo/slow-sink@1", "config": {"latency_ms": 0}},
                {"id": "archive", "op": "demo/slow-sink@1", "config": {"latency_ms": 0}}
            ],
            "edges": [
                ["events", "decode"], ["decode", "by-user"], ["by-user", "window"],
                ["window", "count"], ["count", "to-row"], ["to-row", "results"],
                ["count", "to-gts"], ["to-gts", "warp10"],
                ["decode", "archive"]
            ]
        }),
        json!({
            "tenant": "globex",
            "graph": "clicks",
            "settings": settings,
            "nodes": [
                {"id": "clicks", "op": "demo/generator@1",
                 "config": {"partitions": 2, "rate_per_sec": 300, "keys": 12}},
                {"id": "decode", "op": "demo/decode@1"},
                {"id": "by-user", "op": "demo/by-user@1"},
                {"id": "sliding", "op": "std/sliding@1", "config": {"size": "20s", "slide": "5s"}},
                {"id": "count", "op": "std/count@1"},
                {"id": "to-row", "op": "std/count-to-json@1"},
                {"id": "results", "op": "std/kv-sink@1", "config": {"retain": 200}}
            ],
            "edges": [
                ["clicks", "decode"], ["decode", "by-user"], ["by-user", "sliding"],
                ["sliding", "count"], ["count", "to-row"], ["to-row", "results"]
            ]
        }),
    ];
    specs
        .into_iter()
        .map(|spec| {
            serde_json::from_value(spec).unwrap_or_else(|error| panic!("demo graph: {error}"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use moonpool_core::TokioProviders;
    use streaming_core::Topology;

    #[test]
    fn demo_graphs_are_valid() {
        let mut registry = Registry::<TokioProviders>::standard();
        streaming_runtime::builtin::register(&mut registry, streaming_core::MemKv::shared());
        streaming_warp10::register(&mut registry);
        crate::stock::register(&mut registry);
        register(&mut registry, Arc::new(Generators::default()));
        let catalog = registry.catalog();
        for spec in graphs() {
            let topology = Topology::build(&spec, &catalog)
                .unwrap_or_else(|error| panic!("{}: {error}", spec.key()));
            assert!(
                !topology
                    .nodes_of(streaming_core::OpKind::Aggregate)
                    .is_empty()
            );
        }
    }
}
