//! The pipes of sources and sinks, as the engine drives them: through the
//! connectors a registry builds for a [`Source`] and a [`Sink`].

use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use streaming_core::{
    Context, Emit, EmitId, GraphBuilder, GraphId, Keyed, MemKv, NodeId, OpContext, OpError, OpFactory, OpRef, Operator,
    Origin, PartitionReader, Pipe, Position, Registry, Sink, SinkConnector, SinkError, SinkPipe, SliceId, Source,
    SourceConnector, SourceError, SourcePipe, SourceRecord, Stateless, StatelessFilter, StatelessFilterMap,
    StatelessTransformer, StaticSecrets, SystemClock, TenantId, Transformer, Windows, datum,
    moonpool_core::{Providers, TokioProviders},
};

/// What the operators under test tell the tests.
#[derive(Default)]
struct Seen {
    pushed: AtomicU64,
    starts: AtomicU64,
    stopped: AtomicBool,
    committed: Mutex<Vec<Position>>,
    written: Mutex<Vec<(String, u64)>>,
    batches: Mutex<Vec<usize>>,
}

fn context() -> OpContext<TokioProviders> {
    OpContext {
        providers: TokioProviders::new(),
        clock: Arc::new(SystemClock),
        secrets: Arc::new(StaticSecrets::new()),
        kv: MemKv::shared(),
        shared: Arc::default(),
        tenant: TenantId::new("acme").unwrap(),
        graph: GraphId::new("g").unwrap(),
        node: NodeId::new("n").unwrap(),
    }
}

fn source<S: Source>(context: &OpContext<TokioProviders>, config: Value) -> Arc<dyn SourceConnector> {
    let mut registry = Registry::<TokioProviders>::empty();
    registry.source::<S>();
    let Some((_, OpFactory::Source(factory))) = registry.get(&OpRef::new(S::OP).unwrap()) else {
        panic!("{} is not a source", S::OP);
    };
    factory(context, &config).unwrap_or_else(|error| panic!("{error}"))
}

fn sink<S: Sink<u64>>(context: &OpContext<TokioProviders>, config: Value) -> Arc<dyn SinkConnector> {
    let mut registry = Registry::<TokioProviders>::empty();
    registry.sink::<u64, S>();
    let Some((_, OpFactory::Sink(factory))) = registry.get(&OpRef::new(S::OP).unwrap()) else {
        panic!("{} is not a sink", S::OP);
    };
    factory(context, &config).unwrap_or_else(|error| panic!("{error}"))
}

fn position(offset: u64) -> Position { Position(0, offset, 0) }

fn offsets(records: &[SourceRecord]) -> Vec<u64> { records.iter().map(|record| record.position.1).collect() }

const SOON: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

/// Pushes the offsets after the position it is given, up to `upto`, then
/// does what `then` says.
#[derive(Operator)]
#[operator("test/counter@1")]
struct Counter {
    #[param(default = 1000000)]
    upto: u64,
    /// `wait`, `stop`, `fail`, or `end:<offset>` to say where the partition
    /// ends before waiting.
    #[param(default = "wait")]
    then: String,
}

impl Source for Counter {
    type Out = u64;

    async fn partitions<P: Providers>(&self, _: &Context<P>) -> Result<u32, OpError> { Ok(3) }

    async fn start<P: Providers>(
        &self,
        ctx: &Context<P>,
        _: u32,
        after: Option<Position>,
        out: SourcePipe<u64>,
    ) -> Result<(), OpError> {
        let seen = ctx.shared::<Seen>();
        seen.starts.fetch_add(1, Ordering::Relaxed);
        struct Stopped(Arc<Seen>);
        impl Drop for Stopped {
            fn drop(&mut self) { self.0.stopped.store(true, Ordering::Relaxed); }
        }
        let _stopped = Stopped(seen.clone());
        for offset in after.map_or(0, |position| position.1) + 1..=self.upto {
            out.send(position(offset), offset as i64 * 10, offset).await?;
            seen.pushed.fetch_add(1, Ordering::Relaxed);
        }
        match self.then.as_str() {
            "stop" => return Ok(()),
            "fail" => return Err(OpError::Failed("the broker is gone".into())),
            "wait" => {}
            end => out.end(end.strip_prefix("end:").and_then(|end| end.parse().ok()).map(position)),
        }
        loop {
            let committed = out.committed().await?;
            seen.committed.lock().push(committed);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_source_pushes_in_order_and_no_faster_than_it_is_read() {
    let context = context();
    let connector = source::<Counter>(&context, Value::Null);
    assert_eq!(connector.partitions().await.unwrap(), 3);
    let seen = context.shared::<Seen>();
    let mut reader = connector.open(0, Some(position(40))).await.unwrap();
    // Nothing runs before the engine reads.
    assert_eq!(seen.starts.load(Ordering::Relaxed), 0);
    let mut read = Vec::new();
    for max in [3, 1, 5, 2] {
        let records = reader.poll(max, SOON).await.unwrap();
        assert!(
            !records.is_empty() && records.len() <= max,
            "{} for {max}",
            records.len()
        );
        read.extend(offsets(&records));
        // The source is never more than one read ahead of the engine.
        let pushed = seen.pushed.load(Ordering::Relaxed);
        assert!(
            pushed <= read.len() as u64 + max as u64,
            "{pushed} pushed, {} read",
            read.len()
        );
    }
    assert_eq!(read, (41..41 + read.len() as u64).collect::<Vec<_>>());
    assert_eq!(seen.starts.load(Ordering::Relaxed), 1);
    // Records carry what the source said of them.
    let records = reader.poll(1, SOON).await.unwrap();
    assert_eq!(records[0].event_time, records[0].position.1 as i64 * 10);
    assert_eq!(records[0].value.downcast_ref::<u64>(), Some(&records[0].position.1));
    // Without a read, the source does not move.
    let before = seen.pushed.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(seen.pushed.load(Ordering::Relaxed), before);

    // Dropping the reader stops the source.
    assert!(!seen.stopped.load(Ordering::Relaxed));
    drop(reader);
    assert!(seen.stopped.load(Ordering::Relaxed));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_read_waits_as_long_as_asked_for_a_source_with_nothing_to_say() {
    let connector = source::<Counter>(&context(), json!({"upto": 2}));
    let mut reader = connector.open(0, None).await.unwrap();
    assert_eq!(offsets(&reader.poll(10, SOON).await.unwrap()), [1, 2]);
    let started = tokio::time::Instant::now();
    assert!(reader.poll(10, Duration::from_millis(300)).await.unwrap().is_empty());
    assert_eq!(started.elapsed(), Duration::from_millis(300));
}

#[tokio::test(flavor = "current_thread")]
async fn a_source_learns_what_the_engine_committed() {
    let context = context();
    let connector = source::<Counter>(&context, json!({"upto": 3}));
    let seen = context.shared::<Seen>();
    let mut reader = connector.open(0, None).await.unwrap();
    assert_eq!(reader.poll(10, SOON).await.unwrap().len(), 3);
    assert!(seen.committed.lock().is_empty());
    reader.ack(position(2)).await.unwrap();
    assert_eq!(*seen.committed.lock(), [position(2)]);
    reader.ack(position(3)).await.unwrap();
    assert_eq!(*seen.committed.lock(), [position(2), position(3)]);
}

/// Pushes one record, then only looks at what is committed when it is read
/// again: the commits it missed in between come as one, the latest.
#[derive(Operator)]
#[operator("test/busy@1")]
struct Busy;

impl Source for Busy {
    type Out = u64;

    async fn partitions<P: Providers>(&self, _: &Context<P>) -> Result<u32, OpError> { Ok(1) }

    async fn start<P: Providers>(
        &self,
        ctx: &Context<P>,
        _: u32,
        _: Option<Position>,
        out: SourcePipe<u64>,
    ) -> Result<(), OpError> {
        let seen = ctx.shared::<Seen>();
        for offset in 1.. {
            out.send(position(offset), 0, offset).await?;
            seen.committed.lock().extend(out.take_committed());
        }
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn commits_a_source_did_not_look_at_come_as_the_latest() {
    let context = context();
    let connector = source::<Busy>(&context, Value::Null);
    let seen = context.shared::<Seen>();
    let mut reader = connector.open(0, None).await.unwrap();
    assert_eq!(reader.poll(1, SOON).await.unwrap().len(), 1);
    // The source has room for one record, and looks once.
    reader.ack(position(4)).await.unwrap();
    assert_eq!(*seen.committed.lock(), [position(4)]);
    // Then it waits for room: it sees neither of these two as they come.
    reader.ack(position(7)).await.unwrap();
    reader.ack(position(9)).await.unwrap();
    assert_eq!(*seen.committed.lock(), [position(4)]);
    assert_eq!(reader.poll(1, SOON).await.unwrap().len(), 1);
    assert_eq!(reader.poll(1, SOON).await.unwrap().len(), 1);
    assert_eq!(*seen.committed.lock(), [position(4), position(9)]);
}

#[tokio::test(flavor = "current_thread")]
async fn the_end_of_a_partition_is_what_the_source_says_and_never_before_what_it_pushed() {
    // A source that says nothing: the last record it pushed stands for the end.
    let connector = source::<Counter>(&context(), json!({"upto": 2}));
    let mut reader = connector.open(0, None).await.unwrap();
    assert_eq!(
        reader.end_position().await.unwrap(),
        Some(position(1)),
        "one record fits before the first read"
    );
    assert_eq!(reader.poll(10, SOON).await.unwrap().len(), 2);
    assert_eq!(reader.end_position().await.unwrap(), Some(position(2)));

    // A source that knows records it has not pushed lie ahead.
    let connector = source::<Counter>(&context(), json!({"upto": 2, "then": "end:9"}));
    let mut reader = connector.open(0, None).await.unwrap();
    assert_eq!(reader.poll(10, SOON).await.unwrap().len(), 2);
    assert_eq!(reader.end_position().await.unwrap(), Some(position(9)));

    // An end that lies before what was pushed since does not hide it.
    let connector = source::<Counter>(&context(), json!({"upto": 4, "then": "end:1"}));
    let mut reader = connector.open(0, None).await.unwrap();
    assert_eq!(reader.poll(10, SOON).await.unwrap().len(), 4);
    assert_eq!(reader.end_position().await.unwrap(), Some(position(4)));

    // An empty partition has no end.
    let connector = source::<Counter>(&context(), json!({"upto": 0, "then": "end:none"}));
    let mut reader = connector.open(0, None).await.unwrap();
    assert!(reader.poll(10, Duration::from_millis(20)).await.unwrap().is_empty());
    assert_eq!(reader.end_position().await.unwrap(), None);
}

#[tokio::test(flavor = "current_thread")]
async fn a_source_that_stops_is_said_after_what_it_pushed() {
    for (then, why) in [
        ("fail", "the broker is gone"),
        ("stop", "the source stopped while its pipe was open"),
    ] {
        let connector = source::<Counter>(&context(), json!({"upto": 5, "then": then}));
        let mut reader = connector.open(0, None).await.unwrap();
        let mut read = Vec::new();
        let error = loop {
            match reader.poll(2, SOON).await {
                Ok(records) => read.extend(offsets(&records)),
                Err(error) => break error,
            }
        };
        assert_eq!(read, [1, 2, 3, 4, 5], "{then}");
        assert!(error.0.contains(why), "{then}: {error}");
        // It stays said: the engine opens the partition again.
        assert!(reader.poll(2, SOON).await.is_err());
    }
}

/// A connector that exists already: three records, and a note of what it is
/// asked.
struct Memory {
    seen: Arc<Seen>,
    broken: bool,
}

#[async_trait]
impl SourceConnector for Memory {
    async fn partitions(&self) -> Result<u32, SourceError> { Ok(7) }

    async fn open(&self, _: u32, after: Option<Position>) -> Result<Box<dyn PartitionReader>, SourceError> {
        if self.broken {
            return Err(SourceError("no such partition".into()));
        }
        Ok(Box::new(MemoryReader {
            seen: self.seen.clone(),
            next: after.map_or(0, |position| position.1) + 1,
        }))
    }
}

struct MemoryReader {
    seen: Arc<Seen>,
    next: u64,
}

#[async_trait]
impl PartitionReader for MemoryReader {
    async fn poll(&mut self, max: usize, _: Duration) -> Result<Vec<SourceRecord>, SourceError> {
        let records: Vec<SourceRecord> = (self.next..=3)
            .take(max)
            .map(|offset| SourceRecord {
                position: position(offset),
                event_time: 0,
                value: datum(offset),
            })
            .collect();
        self.next += records.len() as u64;
        Ok(records)
    }

    async fn ack(&mut self, upto: Position) -> Result<(), SourceError> {
        self.seen.committed.lock().push(upto);
        if upto == position(3) {
            return Err(SourceError("the acknowledgement was lost".into()));
        }
        Ok(())
    }

    async fn end_position(&mut self) -> Result<Option<Position>, SourceError> { Ok(Some(position(3))) }
}

/// A source built on [`Memory`].
#[derive(Operator)]
#[operator("test/handed@1")]
struct Handed {
    #[param(default = false)]
    broken: bool,
}

impl Source for Handed {
    type Out = u64;

    async fn partitions<P: Providers>(&self, ctx: &Context<P>) -> Result<u32, OpError> {
        let memory = Memory {
            seen: ctx.shared::<Seen>(),
            broken: self.broken,
        };
        memory.partitions().await.map_err(|error| OpError::Failed(error.0))
    }

    async fn start<P: Providers>(
        &self,
        ctx: &Context<P>,
        partition: u32,
        after: Option<Position>,
        out: SourcePipe<u64>,
    ) -> Result<(), OpError> {
        let memory = Memory {
            seen: ctx.shared::<Seen>(),
            broken: self.broken,
        };
        out.read(&memory, partition, after).await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_source_hands_its_partition_to_a_connector() {
    let context = context();
    let connector = source::<Handed>(&context, Value::Null);
    assert_eq!(connector.partitions().await.unwrap(), 7);
    let seen = context.shared::<Seen>();
    let mut reader = connector.open(0, Some(position(1))).await.unwrap();
    // The very first read is already the connector's.
    assert_eq!(offsets(&reader.poll(1, SOON).await.unwrap()), [2]);
    assert_eq!(offsets(&reader.poll(5, SOON).await.unwrap()), [3]);
    assert!(reader.poll(5, SOON).await.unwrap().is_empty());
    // So are the acknowledgements, their failures included, and the end.
    reader.ack(position(2)).await.unwrap();
    assert_eq!(*seen.committed.lock(), [position(2)]);
    let lost = reader.ack(position(3)).await.unwrap_err();
    assert_eq!(lost.0, "the acknowledgement was lost");
    assert_eq!(reader.end_position().await.unwrap(), Some(position(3)));

    // The acknowledgement and the end go to the connector even when they
    // are the first thing asked.
    let mut reader = connector.open(0, None).await.unwrap();
    assert_eq!(reader.end_position().await.unwrap(), Some(position(3)));
    let mut reader = connector.open(0, None).await.unwrap();
    reader.ack(position(1)).await.unwrap();
    assert_eq!(seen.committed.lock().last(), Some(&position(1)));

    // A partition the connector cannot open is said by the first read.
    let broken = source::<Handed>(&context, json!({"broken": true}));
    let mut reader = broken.open(0, None).await.unwrap();
    let Err(error) = reader.poll(1, SOON).await else {
        panic!("the partition cannot be opened");
    };
    assert!(error.0.contains("no such partition"), "{error}");
}

// ---------------------------------------------------------------------------
// Sinks
// ---------------------------------------------------------------------------

fn emit(value: u64) -> Emit {
    let slice = SliceId::Source {
        node: NodeId::new("events").unwrap(),
        partition: 0,
    };
    let origin = Origin::source(position(value));
    Emit {
        id: EmitId::new(&slice, &origin),
        origin,
        ts: 0,
        key: None,
        value: datum(value),
    }
}

fn batch(values: std::ops::RangeInclusive<u64>) -> Vec<Emit> { values.map(emit).collect() }

/// Writes what it receives into [`Seen`]; the first value of a batch says
/// what becomes of it.
#[derive(Operator)]
#[operator("test/recorder@1")]
struct Recorder;

const FAIL: u64 = 100;
const SKIP: u64 = 200;
const STOP: u64 = 300;
const BREAK: u64 = 400;
const DONE_THEN_STOP: u64 = 500;
const HANG: u64 = 600;

impl Sink<u64> for Recorder {
    async fn start<P: Providers>(&self, ctx: &Context<P>, mut input: SinkPipe<u64>) -> Result<(), OpError> {
        let seen = ctx.shared::<Seen>();
        seen.starts.fetch_add(1, Ordering::Relaxed);
        while let Some(batch) = input.next().await {
            seen.batches.lock().push(batch.len());
            let first = batch.iter().next().map_or(0, |element| *element.value);
            match first {
                FAIL => {
                    batch.failed("the destination refused");
                    continue;
                }
                SKIP => continue,
                STOP => return Ok(()),
                BREAK => return Err(OpError::Failed("the destination is gone".into())),
                HANG => std::future::pending::<()>().await,
                _ => {}
            }
            for element in batch.iter() {
                seen.written
                    .lock()
                    .push((element.id.as_str().to_owned(), *element.value));
            }
            batch.done();
            if first == DONE_THEN_STOP {
                return Ok(());
            }
        }
        seen.stopped.store(true, Ordering::Relaxed);
        Ok(())
    }
}

fn written(seen: &Seen) -> Vec<u64> { seen.written.lock().iter().map(|(_, value)| *value).collect() }

#[tokio::test(flavor = "current_thread")]
async fn a_batch_is_written_once_its_sink_says_so() {
    let context = context();
    let connector = sink::<Recorder>(&context, Value::Null);
    let seen = context.shared::<Seen>();
    // Nothing to write: the sink is not even started.
    connector.write(&[]).await.unwrap();
    assert_eq!(seen.starts.load(Ordering::Relaxed), 0);

    connector.write(&batch(1..=3)).await.unwrap();
    connector.write(&batch(4..=4)).await.unwrap();
    assert_eq!(written(&seen), [1, 2, 3, 4]);
    // Elements come with their emit ids, and one sink serves every batch.
    assert_eq!(seen.written.lock()[0].0, emit(1).id.as_str());
    assert_eq!(seen.starts.load(Ordering::Relaxed), 1);
    assert_eq!(*seen.batches.lock(), [3, 1]);
}

#[tokio::test(flavor = "current_thread")]
async fn a_batch_its_sink_did_not_confirm_is_not_written() {
    let context = context();
    let connector = sink::<Recorder>(&context, Value::Null);
    let seen = context.shared::<Seen>();
    let refused = |first: u64| {
        let connector = connector.clone();
        async move { connector.write(&batch(first..=first + 1)).await.unwrap_err().0 }
    };

    // The sink says it failed, or asks for the next batch: it goes on, and
    // is given what the engine writes next.
    assert_eq!(refused(FAIL).await, "the destination refused");
    assert_eq!(
        refused(SKIP).await,
        "the sink asked for the next batch without confirming this one"
    );
    connector.write(&batch(1..=2)).await.unwrap();
    assert_eq!(seen.starts.load(Ordering::Relaxed), 1);

    // The sink stops: it is started again for the next batch.
    assert_eq!(refused(STOP).await, "the sink stopped before confirming the batch");
    assert!(refused(BREAK).await.contains("the destination is gone"));
    connector.write(&batch(3..=4)).await.unwrap();
    assert_eq!(seen.starts.load(Ordering::Relaxed), 3);

    // Only what was confirmed is written.
    assert_eq!(written(&seen), [1, 2, 3, 4]);
    assert_eq!(seen.batches.lock().len(), 6);
}

#[tokio::test(flavor = "current_thread")]
async fn a_sink_that_stops_after_confirming_is_started_again() {
    let context = context();
    let connector = sink::<Recorder>(&context, Value::Null);
    let seen = context.shared::<Seen>();
    connector.write(&batch(DONE_THEN_STOP..=DONE_THEN_STOP)).await.unwrap();
    connector.write(&batch(1..=1)).await.unwrap();
    assert_eq!(written(&seen), [DONE_THEN_STOP, 1]);
    assert_eq!(seen.starts.load(Ordering::Relaxed), 2);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_write_the_engine_gives_up_starts_the_sink_again() {
    let context = context();
    let connector = sink::<Recorder>(&context, Value::Null);
    let seen = context.shared::<Seen>();
    let hung = batch(HANG..=HANG);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), connector.write(&hung))
            .await
            .is_err()
    );
    connector.write(&batch(1..=1)).await.unwrap();
    assert_eq!(seen.starts.load(Ordering::Relaxed), 2);
    assert_eq!(written(&seen), [1]);
}

#[tokio::test(flavor = "current_thread")]
async fn a_sink_is_not_given_what_it_was_not_written_for() {
    let context = context();
    let connector = sink::<Recorder>(&context, Value::Null);
    let mut wrong = emit(1);
    wrong.value = datum("text".to_owned());
    let error = connector.write(&[emit(2), wrong]).await.unwrap_err();
    assert!(error.0.contains("u64"), "{error}");
    assert_eq!(context.shared::<Seen>().starts.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn the_pipe_of_a_sink_ends_when_its_node_stops() {
    let context = context();
    let connector = sink::<Recorder>(&context, Value::Null);
    let seen = context.shared::<Seen>();
    connector.write(&batch(1..=1)).await.unwrap();
    assert!(!seen.stopped.load(Ordering::Relaxed));
    drop(connector);
    // The sink is not run any more: nothing of it is left waiting.
    assert_eq!(seen.starts.load(Ordering::Relaxed), 1);
}

/// A connector that exists already: refuses the batches starting with
/// [`FAIL`].
struct Outbox(Arc<Seen>);

#[async_trait]
impl SinkConnector for Outbox {
    async fn write(&self, batch: &[Emit]) -> Result<(), SinkError> {
        let values: Vec<u64> = batch
            .iter()
            .filter_map(|emit| emit.value.downcast_ref::<u64>().copied())
            .collect();
        if values.first() == Some(&FAIL) {
            return Err(SinkError("the outbox is full".into()));
        }
        self.0.written.lock().extend(
            batch
                .iter()
                .zip(values)
                .map(|(emit, value)| (emit.id.as_str().to_owned(), value)),
        );
        Ok(())
    }
}

/// A sink built on [`Outbox`].
#[derive(Operator)]
#[operator("test/posted@1")]
struct Posted;

impl Sink<u64> for Posted {
    async fn start<P: Providers>(&self, ctx: &Context<P>, mut input: SinkPipe<u64>) -> Result<(), OpError> {
        let outbox = Outbox(ctx.shared::<Seen>());
        input.write_with(&outbox).await;
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_sink_hands_its_batches_to_a_connector() {
    let context = context();
    let connector = sink::<Posted>(&context, Value::Null);
    connector.write(&batch(1..=2)).await.unwrap();
    let error = connector.write(&batch(FAIL..=FAIL)).await.unwrap_err();
    assert_eq!(error.0, "the outbox is full");
    connector.write(&batch(3..=3)).await.unwrap();
    assert_eq!(written(&context.shared::<Seen>()), [1, 2, 3]);
}

// ---------------------------------------------------------------------------
// Transformers
// ---------------------------------------------------------------------------

/// Several stateless steps in a row are one node.
#[derive(Operator)]
#[operator("test/odd-halves@1")]
struct OddHalves;

impl Transformer<u64> for OddHalves {
    type Out = String;

    fn transform(&self, input: Pipe<u64>) -> Pipe<String> {
        input
            .filter(|value| value % 2 == 0)
            .map(|value| value / 2)
            .flat_map(|half, out| out.extend(std::iter::repeat_n(*half, 2)))
            .filter_map(|half| (half % 2 == 1).then(|| format!("#{half}")))
    }
}

#[test]
fn stateless_steps_in_a_row_are_one_map() {
    let mut registry = Registry::<TokioProviders>::empty();
    registry.transformer(&OddHalves).unwrap();
    let (descriptor, OpFactory::Map(factory)) = registry.get(&OpRef::new(OddHalves::OP).unwrap()).unwrap() else {
        panic!("not a map");
    };
    assert_eq!(descriptor.input.to_string(), "u64");
    assert!(descriptor.output.to_string().contains("String"));
    let map = factory(&Value::Null).unwrap();
    let mut out = Vec::new();
    for value in 1..=6_u64 {
        map.apply(&datum(value), &mut out).unwrap();
    }
    let texts: Vec<&String> = out
        .iter()
        .map(|value| value.downcast_ref::<String>().unwrap())
        .collect();
    assert_eq!(texts, ["#1", "#1", "#3", "#3"]);
    assert!(map.apply(&datum("text".to_owned()), &mut out).is_err());
}

/// Two nodes in one: refused.
#[derive(Operator)]
#[operator("test/keyed-windows@1")]
struct KeyedWindows;

impl Transformer<u64> for KeyedWindows {
    type Out = Windows<u64>;

    fn transform(&self, input: Pipe<u64>) -> Pipe<Windows<u64>> {
        input.key_by(|value| *value).window(Duration::ZERO, |_, _| {})
    }
}

/// A map asked of keyed elements: refused.
#[derive(Operator)]
#[operator("test/map-of-keyed@1")]
struct MapOfKeyed;

impl Transformer<Keyed<u64>> for MapOfKeyed {
    type Out = u64;

    fn transform(&self, input: Pipe<Keyed<u64>>) -> Pipe<u64> { input.map(|_| 0) }
}

#[test]
fn a_transformer_that_is_not_one_node_is_refused_in_words() {
    let mut registry = Registry::<TokioProviders>::empty();
    let Err(error) = registry.transformer(&KeyedWindows).map(drop) else {
        panic!("two nodes in one must be refused");
    };
    assert!(
        error.to_string().contains("`test/keyed-windows@1` does several things"),
        "{error}"
    );
    let Err(error) = registry.transformer(&MapOfKeyed).map(drop) else {
        panic!("a map of keyed elements must be refused");
    };
    assert_eq!(
        error.to_string(),
        "invalid operator configuration: `test/map-of-keyed@1`: `map` is asked of a pipe that carries keyed \
         elements, which only take a window"
    );
    assert!(registry.catalog().ops.is_empty());
}

/// Multiplies by a factor the API may change: a function, nothing more.
#[derive(Debug, Clone, Operator)]
#[operator("test/scale@1")]
struct Scale {
    /// Factor.
    #[param(default = 2, min = 1, max = 1000)]
    factor: u64,
}

impl StatelessTransformer<u64> for Scale {
    type Out = String;

    fn apply(&self, value: &u64) -> String { format!("{}", value * self.factor) }
}

#[test]
fn a_stateless_transformer_is_a_map_under_its_own_name() {
    let mut registry = Registry::<TokioProviders>::empty();
    registry.transformer(&Stateless(Scale { factor: 2 })).unwrap();
    let (descriptor, OpFactory::Map(factory)) = registry.get(&OpRef::new(Scale::OP).unwrap()).unwrap() else {
        panic!("not a map");
    };
    assert_eq!(descriptor.params.len(), 1);
    let map = factory(&json!({"factor": 7})).unwrap();
    let mut out = Vec::new();
    map.apply(&datum(3_u64), &mut out).unwrap();
    assert_eq!(out[0].downcast_ref::<String>().unwrap(), "21");
    assert!(factory(&json!({"factor": 0})).is_err());
}

/// Parses numbers, dropping what is not one.
#[derive(Debug, Clone, Operator)]
#[operator("test/parse@1")]
struct Parse;

impl StatelessFilterMap<String> for Parse {
    type Out = u64;

    fn apply(&self, text: &String) -> Option<u64> { text.parse().ok() }
}

#[test]
fn a_stateless_filter_map_drops_what_it_returns_none_for() {
    let mut registry = Registry::<TokioProviders>::empty();
    registry.transformer(&StatelessFilter(Parse)).unwrap();
    let (_, OpFactory::Map(factory)) = registry.get(&OpRef::new(Parse::OP).unwrap()).unwrap() else {
        panic!("not a map");
    };
    let map = factory(&json!(null)).unwrap();
    let mut out = Vec::new();
    map.apply(&datum("12".to_owned()), &mut out).unwrap();
    map.apply(&datum("twelve".to_owned()), &mut out).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(*out[0].downcast_ref::<u64>().unwrap(), 12);
}

#[test]
fn a_graph_names_a_stateless_transformer_by_its_own_reference() {
    let mut graph = GraphBuilder::<TokioProviders>::new("acme", "stateless");
    let texts = graph.node("texts", Parse).typed::<String>();
    let numbers = graph.filter_map(texts, "parse", Parse);
    graph.map(numbers, "scale", Scale { factor: 3 });
    let spec = graph.build().unwrap();
    let ops: Vec<_> = spec
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node.op.to_string()))
        .collect();
    assert_eq!(
        ops,
        [
            ("texts", "test/parse@1".to_owned()),
            ("parse", "test/parse@1".to_owned()),
            ("scale", "test/scale@1".to_owned())
        ]
    );
    assert_eq!(spec.nodes[2].config, json!({"factor": 3}));
}

#[allow(dead_code)]
fn a_graph_maps_with_a_stateless_transformer(
    graph: &mut GraphBuilder,
    numbers: streaming_core::Node<u64>,
) -> streaming_core::Node<String> {
    graph.map(numbers, "scale", Scale { factor: 3 })
}
