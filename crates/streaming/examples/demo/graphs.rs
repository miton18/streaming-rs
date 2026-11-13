//! Operators and graphs of the demonstration application.
//!
//! | reference | kind | types | configuration |
//! |---|---|---|---|
//! | `demo/generator@1` | source | `Vec<u8>` (JSON) | `{"partitions": 4, "ratePerSec": 2000, "keys": 50}` |
//! | `demo/decode@1` | map | `Vec<u8>` -> `Event` | |
//! | `demo/by-user@1` | key_by | `Event` | |
//! | `demo/slow-sink@1` | sink | any | `{"latencyMs": 0}` |
//!
//! Each operator is a structure whose fields are its configuration, and one
//! trait saying what it does: [`Source`], [`Transformer`] or [`Sink`]. A
//! graph registers the operators it names.

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
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
        Clock, Context, GraphBuilder, GraphSettings, Keyed, OpError, Operator, Pipe, Position, Sink, SinkPipe, Source,
        SourcePipe, StatelessFilterMap, SystemClock, Transformer, datum,
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
/// How long a source with nothing to read waits before it looks again at
/// what the engine committed.
const IDLE_WAIT: Duration = Duration::from_millis(100);

struct Topic {
    log: Arc<MemLog>,
    rate_per_sec: AtomicU64,
    keys: AtomicU64,
    started: AtomicBool,
    dropped: AtomicU64,
}

/// The in-process topics behind the `demo/generator@1` sources of a process:
/// one per node, whichever worker of the process runs its partitions.
#[derive(Default)]
pub struct Generators {
    topics: Mutex<HashMap<String, Arc<Topic>>>,
}

/// Synthetic events at a configurable rate, as JSON bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Operator)]
#[operator("demo/generator@1")]
pub struct Generator {
    /// Number of partitions.
    #[param(default = 4, min = 1, max = 64)]
    pub partitions: u32,
    /// Events per second.
    #[param(default = 2000, max = 200000)]
    pub rate_per_sec: u64,
    /// Number of distinct users.
    #[param(default = 50)]
    pub keys: u64,
}

impl Generator {
    /// The topic of the node, created and fed on first use, at the rate this
    /// configuration asks for.
    fn topic<P: Providers>(&self, ctx: &Context<P>) -> Arc<Topic> {
        let name = format!("{}/{}/{}/{}", ctx.tenant, ctx.graph, ctx.node, self.partitions);
        let topic = ctx
            .shared::<Generators>()
            .topics
            .lock()
            .entry(name.clone())
            .or_insert_with(|| {
                Arc::new(Topic {
                    log: MemLog::with_retention(self.partitions, true),
                    rate_per_sec: AtomicU64::new(0),
                    keys: AtomicU64::new(1),
                    started: AtomicBool::new(false),
                    dropped: AtomicU64::new(0),
                })
            })
            .clone();
        topic.rate_per_sec.store(self.rate_per_sec, Ordering::Relaxed);
        topic.keys.store(self.keys.max(1), Ordering::Relaxed);
        if !topic.started.swap(true, Ordering::Relaxed) {
            drop(ctx.providers.task().spawn_task(
                &format!("generator/{name}"),
                generate(ctx.providers.clone(), topic.clone()),
            ));
        }
        topic
    }
}

impl Source for Generator {
    type Out = Vec<u8>;

    async fn partitions<P: Providers>(&self, ctx: &Context<P>) -> Result<u32, OpError> {
        Ok(self.topic(ctx).log.partitions())
    }

    async fn start<P: Providers>(
        &self,
        ctx: &Context<P>,
        partition: u32,
        after: Option<Position>,
        out: SourcePipe<Vec<u8>>,
    ) -> Result<(), OpError> {
        let topic = self.topic(ctx);
        let mut next = after.map_or(0, |position| position.1);
        loop {
            // What the engine committed will never be read again.
            if let Some(position) = out.take_committed() {
                topic.log.ack(partition, position);
            }
            // Asked for before reading, so that no append is missed.
            let appended = topic.log.appended();
            let records = topic.log.read(partition, next, out.room());
            out.end(topic.log.end(partition));
            if records.is_empty() {
                let _ = ctx.providers.time().timeout(IDLE_WAIT, appended).await;
                continue;
            }
            for record in records {
                next = record.position.1;
                let Some(bytes) = record.value.downcast_ref::<Vec<u8>>() else {
                    continue;
                };
                // Waits while downstream is full.
                out.send(record.position, record.event_time, bytes.clone()).await?;
            }
        }
    }
}

/// Decodes a JSON event.
#[derive(Debug, Clone, Copy, Default, Operator)]
#[operator("demo/decode@1")]
pub struct Decode;

impl StatelessFilterMap<Vec<u8>> for Decode {
    type Out = Event;

    fn apply(&self, bytes: &Vec<u8>) -> Option<Event> { serde_json::from_slice(bytes).ok() }
}

/// Groups events by user.
#[derive(Debug, Clone, Copy, Default, Operator)]
#[operator("demo/by-user@1")]
pub struct ByUser;

impl Transformer<Event> for ByUser {
    type Out = Keyed<Event>;

    fn transform(&self, input: Pipe<Event>) -> Pipe<Keyed<Event>> { input.key_by(|event| event.user.clone()) }
}

/// Discards what it receives after a configurable delay per batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Operator)]
#[operator("demo/slow-sink@1")]
pub struct SlowSink {
    /// Delay of each write, in milliseconds.
    #[param(default = 0, max = 60000)]
    pub latency_ms: u64,
}

impl<T: Any + Send + Sync> Sink<T> for SlowSink {
    async fn start<P: Providers>(&self, ctx: &Context<P>, mut input: SinkPipe<T>) -> Result<(), OpError> {
        while let Some(batch) = input.next().await {
            if self.latency_ms > 0 {
                ctx.sleep(Duration::from_millis(self.latency_ms)).await;
            }
            batch.done();
        }
        Ok(())
    }
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
    App::new()
        .tenant("acme", "Acme Corporation")
        .tenant("globex", "Globex")
        .graph(count_per_key(settings.clone()))
        .graph(clicks(settings))
}

/// Events counted per user over 10-second windows, the counts kept for the
/// API and sent to a stand-in for Warp 10, the decoded events archived.
fn count_per_key(settings: GraphSettings) -> GraphBuilder {
    let mut graph = GraphBuilder::new("acme", "count-per-key").settings(settings);
    let events = graph.source(
        "events",
        Generator {
            partitions: 4,
            rate_per_sec: 2_000,
            keys: 50,
        },
    );
    let decoded = graph.filter_map(events, "decode", Decode);
    let by_user = graph.then(decoded, "by-user", ByUser);
    let windows = graph.then(by_user, "window", Tumbling::of(Duration::from_secs(10)));
    let counts = graph.then(windows, "count", Count);
    let rows = graph.then(counts, "to-row", CountToJson);
    graph.sink(rows, "results", KvSink { retain: 400 });
    let points = graph.then(counts, "to-gts", CountToGts::new("events.count").label("user"));
    graph.sink(points, "warp10", SlowSink::default());
    graph.sink(decoded, "archive", SlowSink::default());
    graph
}

/// Clicks counted per user over 20-second windows sliding every 5 seconds.
fn clicks(settings: GraphSettings) -> GraphBuilder {
    let mut graph = GraphBuilder::new("globex", "clicks").settings(settings);
    let clicks = graph.source(
        "clicks",
        Generator {
            partitions: 2,
            rate_per_sec: 300,
            keys: 12,
        },
    );
    let decoded = graph.filter_map(clicks, "decode", Decode);
    let by_user = graph.then(decoded, "by-user", ByUser);
    let windows = graph.then(
        by_user,
        "sliding",
        Sliding::of(Duration::from_secs(20), Duration::from_secs(5)),
    );
    let counts = graph.then(windows, "count", Count);
    let rows = graph.then(counts, "to-row", CountToJson);
    graph.sink(rows, "results", KvSink { retain: 200 });
    graph
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use streaming::{
        core::{
            Emit, EmitId, GraphId, MemKv, NodeId, OpContext, OpFactory, OpRef, Origin, Registry, SinkConnector,
            SliceId, SourceConnector, StaticSecrets, TenantId,
        },
        moonpool_core::TokioProviders,
    };

    fn context() -> OpContext<TokioProviders> {
        OpContext {
            providers: TokioProviders::new(),
            clock: Arc::new(SystemClock),
            secrets: Arc::new(StaticSecrets::new()),
            kv: MemKv::shared(),
            shared: Arc::default(),
            tenant: TenantId::new("acme").unwrap(),
            graph: GraphId::new("g").unwrap(),
            node: NodeId::new("events").unwrap(),
        }
    }

    fn source(context: &OpContext<TokioProviders>, config: &Value) -> Result<Arc<dyn SourceConnector>, OpError> {
        let mut registry = Registry::<TokioProviders>::empty();
        registry.source::<Generator>();
        let Some((_, OpFactory::Source(factory))) = registry.get(&OpRef::new(Generator::OP).unwrap()) else {
            panic!("the generator is not a source");
        };
        factory(context, config)
    }

    fn sink(config: &Value) -> Result<Arc<dyn SinkConnector>, OpError> {
        let mut registry = Registry::<TokioProviders>::empty();
        registry.sink::<Event, SlowSink>();
        let Some((_, OpFactory::Sink(factory))) = registry.get(&OpRef::new(SlowSink::OP).unwrap()) else {
            panic!("the slow sink is not a sink");
        };
        factory(&context(), config)
    }

    fn emit(index: u64) -> Emit {
        let slice = SliceId::Source {
            node: NodeId::new("events").unwrap(),
            partition: 0,
        };
        let origin = Origin::source(Position(0, index, 0));
        Emit {
            id: EmitId::new(&slice, &origin),
            origin,
            ts: 0,
            key: None,
            value: datum(Event {
                user: "user-000".into(),
                action: "view".into(),
                amount: 1.0,
            }),
        }
    }

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

    /// Naming an operator in a graph is enough for the application to know
    /// it, with what the catalogue shows of it.
    #[test]
    fn the_graphs_register_their_operators() {
        let catalog = app().registry().catalog();
        let described = |op: &str| {
            catalog
                .get(&OpRef::new(op).unwrap())
                .unwrap_or_else(|| panic!("{op} is unknown"))
        };
        assert_eq!(
            described(Generator::OP).description,
            "Synthetic events at a configurable rate, as JSON bytes."
        );
        assert_eq!(described(Decode::OP).description, "Decodes a JSON event.");
        assert_eq!(described(ByUser::OP).description, "Groups events by user.");
        // One sink for two types of elements: the points and the events.
        assert_eq!(described(SlowSink::OP).signatures.len(), 2);
        let names = |op: &str| -> Vec<String> { described(op).params.iter().map(|param| param.name.clone()).collect() };
        assert_eq!(names(Generator::OP), ["partitions", "ratePerSec", "keys"]);
        assert_eq!(names(SlowSink::OP), ["latencyMs"]);
    }

    #[test]
    fn the_described_defaults_are_the_parsed_ones() {
        let defaults = |params: Vec<streaming::core::ParamSpec>| -> Value {
            Value::Object(
                params
                    .into_iter()
                    .filter_map(|param| Some((param.name, param.default?)))
                    .collect(),
            )
        };
        let parsed: Generator = serde_json::from_value(json!({})).unwrap();
        assert_eq!(
            parsed,
            Generator {
                partitions: 4,
                rate_per_sec: 2_000,
                keys: 50,
            }
        );
        assert_eq!(defaults(Generator::params()), serde_json::to_value(parsed).unwrap());
        let parsed: SlowSink = serde_json::from_value(json!({})).unwrap();
        assert_eq!(parsed, SlowSink::default());
        assert_eq!(defaults(SlowSink::params()), serde_json::to_value(parsed).unwrap());
    }

    #[test]
    fn configurations_are_bounded() {
        let generator = |partitions, rate_per_sec| Generator {
            partitions,
            rate_per_sec,
            keys: 1,
        };
        assert!(generator(1, 0).check().is_ok());
        assert!(generator(64, 200_000).check().is_ok());
        for (refused, why) in [
            (generator(0, 10), "`partitions` must be between 1 and 64"),
            (generator(65, 10), "`partitions` must be between 1 and 64"),
            (generator(4, 200_001), "`ratePerSec` must be at most 200000"),
        ] {
            let error = refused.check().unwrap_err();
            assert_eq!(error.to_string(), format!("invalid operator configuration: {why}"));
        }
        assert!(SlowSink { latency_ms: 60_000 }.check().is_ok());
        assert!(SlowSink { latency_ms: 60_001 }.check().is_err());
        // A node is refused before it starts.
        assert!(source(&context(), &json!({"partitions": 0})).is_err());
        assert!(sink(&json!({"latencyMs": 60_001})).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_slow_sink_waits_then_confirms() {
        let slow = sink(&json!({"latencyMs": 80})).unwrap();
        for index in 1..=2 {
            let started = std::time::Instant::now();
            slow.write(&[emit(index)]).await.unwrap();
            assert!(started.elapsed() >= Duration::from_millis(80));
        }
        let fast = sink(&Value::Null).unwrap();
        let started = std::time::Instant::now();
        fast.write(&[emit(1), emit(2)]).await.unwrap();
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
        let context = context();
        let config = json!({"partitions": 1, "ratePerSec": 200, "keys": 3});
        for _ in 0..2 {
            assert_eq!(source(&context, &config).unwrap().partitions().await.unwrap(), 1);
        }
        let generators = context.shared::<Generators>();
        assert_eq!(generators.topics.lock().len(), 1);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let topic = generators.topics.lock().values().next().unwrap().clone();
        // Started once: about a hundred events, not twice as many.
        let produced = topic.log.retained(0);
        assert!((50..=100).contains(&produced), "{produced} events");
    }

    /// The source pushes what its topic holds after the position it is given,
    /// in order, lets go of what the engine committed, and says where the
    /// partition ends.
    #[tokio::test(flavor = "current_thread")]
    async fn the_source_reads_its_topic_and_releases_what_is_committed() {
        let context = context();
        let config = json!({"partitions": 1, "ratePerSec": 0});
        let connector = source(&context, &config).unwrap();
        assert_eq!(connector.partitions().await.unwrap(), 1);
        let topic = context
            .shared::<Generators>()
            .topics
            .lock()
            .values()
            .next()
            .unwrap()
            .clone();
        for index in 0..10_u8 {
            topic.log.append(0, i64::from(index), datum(vec![index]));
        }
        let mut reader = connector.open(0, Some(Position(0, 3, 0))).await.unwrap();
        let mut read = Vec::new();
        while read.len() < 7 {
            // Never more than asked for: this is how backpressure reaches the source.
            let records = reader.poll(4, Duration::from_secs(5)).await.unwrap();
            assert!(!records.is_empty() && records.len() <= 4, "{} records", records.len());
            read.extend(records);
        }
        let positions: Vec<u64> = read.iter().map(|record| record.position.1).collect();
        assert_eq!(positions, [4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(read[0].value.downcast_ref::<Vec<u8>>().unwrap(), &vec![3_u8]);
        assert_eq!(read[0].event_time, 3);
        assert_eq!(reader.end_position().await.unwrap(), Some(Position(0, 10, 0)));
        // Nothing more: the read waits, then comes back empty.
        assert!(reader.poll(4, Duration::from_millis(50)).await.unwrap().is_empty());

        assert_eq!(topic.log.retained(0), 10);
        reader.ack(Position(0, 6, 0)).await.unwrap();
        assert!(reader.poll(4, Duration::from_millis(250)).await.unwrap().is_empty());
        assert_eq!(topic.log.retained(0), 4, "what is committed is released");

        // What comes later is read by the same reader, and moves the end.
        topic.log.append(0, 99, datum(vec![42_u8]));
        let records = reader.poll(4, Duration::from_secs(5)).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(reader.end_position().await.unwrap(), Some(Position(0, 11, 0)));
    }
}
