//! Operators and graphs of the demonstration application.
//!
//! | reference | kind | types | configuration |
//! |---|---|---|---|
//! | `demo/generator@1` | source | `Vec<u8>` (JSON) | `{"partitions": 4, "ratePerSec": 2000, "keys": 50}` |
//! | `demo/decode@1` | map | `Vec<u8>` -> `Event` | |
//! | `demo/by-user@1` | key_by | `Event` | |
//! | `demo/slow-sink@1` | sink | any | `{"latencyMs": 0}` |

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use streaming::{
    App,
    core::{
        Clock, Emit, GraphBuilder, GraphSettings, OpError, Operator, Registry, SinkConnector, SinkError,
        SourceConnector, SystemClock, TypeTag, datum,
        stdops::{Count, Sliding, Tumbling},
    },
    moonpool_core::{Providers, RandomProvider, TaskProvider, TimeProvider},
    runtime::{builtin::KvSink, memlog::MemLog},
    stock::{CountToGts, CountToJson},
};

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

struct Topic {
    log: Arc<MemLog>,
    rate_per_sec: AtomicU64,
    keys: AtomicU64,
    started: AtomicBool,
    dropped: AtomicU64,
}

/// The in-process topics behind the `demo/generator@1` sources of a process.
#[derive(Default)]
pub struct Generators {
    topics: Mutex<HashMap<String, Arc<Topic>>>,
}

fn default_partitions() -> u32 { 4 }
fn default_rate() -> u64 { 2_000 }
fn default_keys() -> u64 { 50 }

/// `demo/generator@1`: synthetic events, as JSON bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Generator {
    /// Number of partitions, from 1 to 64.
    #[serde(default = "default_partitions")]
    pub partitions: u32,
    /// Events per second, at most 200,000.
    #[serde(default = "default_rate")]
    pub rate_per_sec: u64,
    /// Number of distinct users.
    #[serde(default = "default_keys")]
    pub keys: u64,
}

impl Operator for Generator {
    const OP: &'static str = "demo/generator@1";
}

/// `demo/decode@1`: decodes the JSON bytes of a generator into an [`Event`].
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Decode;

impl Operator for Decode {
    const OP: &'static str = "demo/decode@1";
}

/// `demo/by-user@1`: groups events by user.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct ByUser;

impl Operator for ByUser {
    const OP: &'static str = "demo/by-user@1";
}

/// `demo/slow-sink@1`: discards what it receives after a delay per batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SlowSink {
    /// Delay of each write, in milliseconds, at most 60,000.
    #[serde(default)]
    pub latency_ms: u64,
}

impl Operator for SlowSink {
    const OP: &'static str = "demo/slow-sink@1";
}

const ACTIONS: [&str; 4] = ["view", "click", "cart", "purchase"];

async fn generate<P: Providers>(providers: P, generator: Arc<Topic>) {
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

struct SlowSinkConnector<P: Providers> {
    providers: P,
    latency: Duration,
    written: AtomicU64,
}

#[async_trait]
impl<P: Providers> SinkConnector for SlowSinkConnector<P> {
    async fn write(&self, batch: &[Emit]) -> Result<(), SinkError> {
        if !self.latency.is_zero() {
            let _ = self.providers.time().sleep(self.latency).await;
        }
        self.written.fetch_add(batch.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}

fn parse<C: for<'de> Deserialize<'de>>(config: &Value) -> Result<C, OpError> {
    let config = if config.is_null() { json!({}) } else { config.clone() };
    serde_json::from_value(config).map_err(|error| OpError::Config(error.to_string()))
}

fn check_generator(config: &Generator) -> Result<(), OpError> {
    if config.partitions == 0 || config.partitions > 64 {
        return Err(OpError::Config("`partitions` must be between 1 and 64".into()));
    }
    if config.rate_per_sec > 200_000 {
        return Err(OpError::Config("`ratePerSec` must be at most 200000".into()));
    }
    Ok(())
}

fn check_slow_sink(config: &SlowSink) -> Result<(), OpError> {
    if config.latency_ms > 60_000 {
        return Err(OpError::Config("`latencyMs` must be at most 60000".into()));
    }
    Ok(())
}

/// Register the demonstration operators.
pub fn register<P: Providers>(registry: &mut Registry<P>, generators: Arc<Generators>) {
    registry.register_source::<Vec<u8>>(
        Generator::OP,
        "Synthetic events at a configurable rate, as JSON bytes.",
        move |context, config| {
            let config: Generator = parse(config)?;
            check_generator(&config)?;
            let topic = format!(
                "{}/{}/{}/{}",
                context.tenant, context.graph, context.node, config.partitions
            );
            let generator = generators
                .topics
                .lock()
                .entry(topic.clone())
                .or_insert_with(|| {
                    Arc::new(Topic {
                        log: MemLog::with_retention(config.partitions, true),
                        rate_per_sec: AtomicU64::new(0),
                        keys: AtomicU64::new(1),
                        started: AtomicBool::new(false),
                        dropped: AtomicU64::new(0),
                    })
                })
                .clone();
            generator.rate_per_sec.store(config.rate_per_sec, Ordering::Relaxed);
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
    registry.register_map(Decode::OP, "Decodes a JSON event.", |bytes: &Vec<u8>| {
        serde_json::from_slice::<Event>(bytes).ok()
    });
    registry.register_key(ByUser::OP, "Groups events by user.", |event: &Event| event.user.clone());
    registry.register_sink(
        SlowSink::OP,
        "Discards what it receives after a configurable delay per batch.",
        TypeTag::Any,
        |context, config| {
            let config: SlowSink = parse(config)?;
            check_slow_sink(&config)?;
            Ok(Arc::new(SlowSinkConnector {
                providers: context.providers.clone(),
                latency: Duration::from_millis(config.latency_ms),
                written: AtomicU64::new(0),
            }) as Arc<dyn SinkConnector>)
        },
    );
    registry.with_params(
        Generator::OP,
        [
            streaming_core::ParamSpec::new("partitions", "integer", "Number of partitions.").default(4),
            streaming_core::ParamSpec::new("ratePerSec", "integer", "Events per second.").default(2000),
            streaming_core::ParamSpec::new("keys", "integer", "Number of distinct users.").default(50),
        ],
    );
    registry.with_params(
        SlowSink::OP,
        [streaming_core::ParamSpec::new("latencyMs", "integer", "Delay of each write, in milliseconds.").default(0)],
    );
}

/// The demonstration application: two tenants, a graph each, fed by
/// in-process generators.
#[must_use]
pub fn app() -> App {
    let settings = GraphSettings {
        max_out_of_orderness: Duration::from_millis(500),
        idle_timeout: Duration::from_secs(3),
        linger: Duration::from_millis(100),
        flush_interval: Duration::from_millis(500),
        max_batch: 500,
        chunk_size: 100,
        mailbox_capacity: 4,
        // Low enough for a slow sink behind the aggregate to hold the source
        // back within half a minute.
        max_pending_windows: 3,
        ..GraphSettings::default()
    };
    // One set of topics per process, shared by the workers it runs.
    let generators = Arc::new(Generators::default());
    App::new()
        .tenant("acme", "Acme Corporation")
        .tenant("globex", "Globex")
        .operators(move |registry| register(registry, generators.clone()))
        .graph(count_per_key(settings.clone()))
        .graph(clicks(settings))
}

/// Events counted per user over 10-second windows, the counts kept for the
/// API and sent to a stand-in for Warp 10, the decoded events archived.
fn count_per_key(settings: GraphSettings) -> GraphBuilder {
    let mut graph = GraphBuilder::new("acme", "count-per-key").settings(settings);
    let events = graph.node(
        "events",
        Generator {
            partitions: 4,
            rate_per_sec: 2_000,
            keys: 50,
        },
    );
    let decode = graph.then(events, "decode", Decode);
    let by_user = graph.then(decode, "by-user", ByUser);
    let window = graph.then(by_user, "window", Tumbling::of(Duration::from_secs(10)));
    let count = graph.then(window, "count", Count);
    let to_row = graph.then(count, "to-row", CountToJson);
    graph.then(to_row, "results", KvSink { retain: 400 });
    let to_gts = graph.then(count, "to-gts", CountToGts::new("events.count").label("user"));
    graph.then(to_gts, "warp10", SlowSink::default());
    graph.then(decode, "archive", SlowSink::default());
    graph
}

/// Clicks counted per user over 20-second windows sliding every 5 seconds.
fn clicks(settings: GraphSettings) -> GraphBuilder {
    let mut graph = GraphBuilder::new("globex", "clicks").settings(settings);
    let clicks = graph.node(
        "clicks",
        Generator {
            partitions: 2,
            rate_per_sec: 300,
            keys: 12,
        },
    );
    let decode = graph.then(clicks, "decode", Decode);
    let by_user = graph.then(decode, "by-user", ByUser);
    let sliding = graph.then(
        by_user,
        "sliding",
        Sliding::of(Duration::from_secs(20), Duration::from_secs(5)),
    );
    let count = graph.then(sliding, "count", Count);
    let to_row = graph.then(count, "to-row", CountToJson);
    graph.then(to_row, "results", KvSink { retain: 200 });
    graph
}

#[cfg(test)]
mod tests {
    use super::*;
    use moonpool_core::TokioProviders;
    use streaming_core::{OpFactory, SinkConnector, TenantId};

    #[test]
    fn the_demo_application_is_valid() {
        let app = super::app();
        app.check().unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(app.graphs().len(), 2);
    }

    #[test]
    fn the_demo_has_two_graphs() {
        let keys: Vec<String> = app().graphs().iter().map(|spec| spec.key().to_string()).collect();
        assert_eq!(keys, ["acme/count-per-key", "globex/clicks"]);
    }

    #[test]
    fn the_described_defaults_are_the_parsed_ones() {
        let parsed: Generator = parse(&Value::Null).unwrap();
        assert_eq!(
            parsed,
            Generator {
                partitions: 4,
                rate_per_sec: 2_000,
                keys: 50,
            }
        );
        let mut registry = Registry::<TokioProviders>::standard();
        register(&mut registry, Arc::new(Generators::default()));
        let catalog = registry.catalog();
        let described = |op: &str| -> serde_json::Map<String, Value> {
            let (_, descriptor) = catalog.ops.iter().find(|(name, _)| name.as_str() == op).unwrap();
            descriptor
                .params
                .iter()
                .filter_map(|param| Some((param.name.clone(), param.default.clone()?)))
                .collect()
        };
        assert_eq!(
            Value::Object(described(Generator::OP)),
            serde_json::to_value(parsed).unwrap()
        );
        let parsed: SlowSink = parse(&json!({})).unwrap();
        assert_eq!(
            Value::Object(described(SlowSink::OP)),
            serde_json::to_value(parsed).unwrap()
        );
    }

    #[test]
    fn configurations_are_bounded() {
        let generator = |partitions, rate_per_sec| Generator {
            partitions,
            rate_per_sec,
            keys: 1,
        };
        assert!(check_generator(&generator(1, 0)).is_ok());
        assert!(check_generator(&generator(64, 200_000)).is_ok());
        assert!(check_generator(&generator(0, 10)).is_err());
        assert!(check_generator(&generator(65, 10)).is_err());
        assert!(check_generator(&generator(4, 200_001)).is_err());
        assert!(check_slow_sink(&SlowSink { latency_ms: 60_000 }).is_ok());
        assert!(check_slow_sink(&SlowSink { latency_ms: 60_001 }).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_slow_sink_waits_then_counts() {
        let sink = SlowSinkConnector {
            providers: TokioProviders::new(),
            latency: Duration::from_millis(80),
            written: AtomicU64::new(0),
        };
        let started = std::time::Instant::now();
        sink.write(&[]).await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(80));
        assert_eq!(sink.written.load(Ordering::Relaxed), 0);
        let fast = SlowSinkConnector {
            providers: TokioProviders::new(),
            latency: Duration::ZERO,
            written: AtomicU64::new(0),
        };
        let started = std::time::Instant::now();
        fast.write(&[]).await.unwrap();
        assert!(started.elapsed() < Duration::from_millis(50));
    }

    /// The generator keeps its rate, spreads users over partitions by their
    /// number, and cycles through the actions.
    #[tokio::test(flavor = "current_thread")]
    async fn the_generator_keeps_its_rate_and_spreads_users() {
        let topic = Arc::new(Topic {
            log: MemLog::with_retention(3, true),
            rate_per_sec: AtomicU64::new(100),
            keys: AtomicU64::new(7),
            started: AtomicBool::new(true),
            dropped: AtomicU64::new(0),
        });
        let running = tokio::spawn(generate(TokioProviders::new(), topic.clone()));
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        running.abort();
        let mut events = Vec::new();
        for partition in 0..3 {
            for record in topic.log.read(partition, 0, usize::MAX) {
                let bytes = record.value.downcast_ref::<Vec<u8>>().unwrap();
                let event: Event = serde_json::from_slice(bytes).unwrap();
                let user: u64 = event.user.strip_prefix("user-").unwrap().parse().unwrap();
                assert!(user < 7, "{}", event.user);
                assert_eq!(event.user, format!("user-{user:03}"));
                assert_eq!(user % 3, u64::from(partition));
                assert!((1.0..100.0).contains(&event.amount), "{}", event.amount);
                events.push(event);
            }
        }
        // About a hundred a second, never more than the rate allows.
        assert!((70..=100).contains(&events.len()), "{} events", events.len());
        // Spread over the users, skewed towards the first ones.
        let users: std::collections::BTreeSet<&str> = events.iter().map(|event| event.user.as_str()).collect();
        assert!(users.len() >= 4, "{users:?}");
        let first = events.iter().filter(|event| event.user == "user-000").count();
        let last = events.iter().filter(|event| event.user == "user-006").count();
        assert!(first > last, "{first} for the first user, {last} for the last");
        let mut actions: Vec<&str> = events.iter().map(|event| event.action.as_str()).collect();
        actions.sort_unstable();
        actions.dedup();
        assert_eq!(actions, ["cart", "click", "purchase", "view"]);
        assert_eq!(topic.dropped.load(Ordering::Relaxed), 0);
    }

    /// The settings the demonstration relies on: short windows of latency,
    /// and a slow sink holding the source back within half a minute.
    #[test]
    fn the_demo_settings_are_tuned_for_a_live_display() {
        for spec in app().graphs() {
            let settings = &spec.settings;
            assert_eq!(settings.max_out_of_orderness, Duration::from_millis(500));
            assert_eq!(settings.idle_timeout, Duration::from_secs(3));
            assert_eq!(settings.linger, Duration::from_millis(100));
            assert_eq!(settings.flush_interval, Duration::from_millis(500));
            assert_eq!(
                (settings.max_batch, settings.chunk_size, settings.mailbox_capacity),
                (500, 100, 4)
            );
            assert_eq!(settings.max_pending_windows, 3);
        }
    }

    /// Two sources on the same topic share one generator, started once.
    #[tokio::test(flavor = "current_thread")]
    async fn a_generator_is_shared_and_started_once() {
        let generators = Arc::new(Generators::default());
        let mut registry = Registry::<TokioProviders>::standard();
        register(&mut registry, generators.clone());
        let Some((_, OpFactory::Source(factory))) = registry.get(&streaming_core::OpRef::new(Generator::OP).unwrap())
        else {
            panic!("the generator is not a source");
        };
        let context = streaming_core::OpContext {
            providers: TokioProviders::new(),
            clock: Arc::new(SystemClock),
            secrets: Arc::new(streaming_core::StaticSecrets::new()),
            tenant: TenantId::new("acme").unwrap(),
            graph: streaming_core::GraphId::new("g").unwrap(),
            node: streaming_core::NodeId::new("events").unwrap(),
        };
        let config = json!({"partitions": 1, "ratePerSec": 200, "keys": 3});
        factory(&context, &config).unwrap();
        factory(&context, &config).unwrap();
        assert!(factory(&context, &json!({"partitions": 0})).is_err());
        assert_eq!(generators.topics.lock().len(), 1);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let topic = generators.topics.lock().values().next().unwrap().clone();
        // Started once: about a hundred events, not twice as many.
        let produced = topic.log.retained(0);
        assert!((50..=100).contains(&produced), "{produced} events");
    }
}
