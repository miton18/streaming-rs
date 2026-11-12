//! Unit tests of the pieces of a slice that the simulation cannot tell apart
//! from their mutants: retries, and the proof that copying out still writes
//! for the owner of the log.

use super::{ActorCtx, Control, Exit, Gate, MAX_BACKOFF, MIN_BACKOFF, Retry, Root, bump, copy_out};
use crate::{
    RuntimeEnv,
    mailbox::Outputs,
    metrics::{ActorIdent, ActorMetrics, Meter, SliceMetrics, ts_key},
    state::{self, CommitRecord, NodeChanges, StateEntry, StateError, WindowChanges},
};
use async_trait::async_trait;
use moonpool_core::{Providers, TimeProvider, TokioProviders};
use parking_lot::Mutex;
use std::{
    collections::{BTreeSet, VecDeque},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use streaming_core::{
    DynKv, GraphId, Key, Kv, KvError, MemKv, NoSecrets, NodeId, OpRef, Position, Registry, SystemClock, TenantId,
    Window, control::ActorKind, keys, spec::GraphSettings,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

/// Refuses every write to window hashes as too large; the rest goes through.
struct Refusing(Arc<MemKv>);

#[async_trait]
impl Kv for Refusing {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> { self.0.get(key).await }
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), KvError> { self.0.set(key, value).await }
    async fn del(&self, keys: Vec<String>) -> Result<(), KvError> { self.0.del(keys).await }
    async fn incr(&self, key: &str) -> Result<i64, KvError> { self.0.incr(key).await }
    async fn hsetnx(&self, key: &str, field: &str, value: Vec<u8>) -> Result<bool, KvError> {
        self.0.hsetnx(key, field, value).await
    }
    async fn hset(&self, key: &str, entries: Vec<(String, Vec<u8>)>) -> Result<(), KvError> {
        if key.contains(":w:") {
            return Err(KvError::TooLarge {
                size: 6 << 20,
                limit: 5 << 20,
            });
        }
        self.0.hset(key, entries).await
    }
    async fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>, KvError> { self.0.hget(key, field).await }
    async fn hgetall(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>, KvError> { self.0.hgetall(key).await }
    async fn hdel(&self, key: &str, fields: Vec<String>) -> Result<(), KvError> { self.0.hdel(key, fields).await }
}

fn node() -> NodeId { NodeId::new("count").unwrap() }

fn window() -> Window { Window::new(0, 10_000) }

fn ctx(kv: DynKv, gc_grace: Duration) -> ActorCtx<TokioProviders> {
    let providers = TokioProviders::new();
    let key: streaming_core::SliceKey = "acme/g/src.events.0".parse().unwrap();
    let metrics = ActorMetrics::new(
        ActorIdent {
            tenant: TenantId::new("acme").unwrap(),
            graph: GraphId::new("g").unwrap(),
            slice: key.slice.clone(),
            node: node(),
            kind: ActorKind::Aggregate,
            op: OpRef::new("std/count@1").unwrap(),
        },
        providers.time().now(),
    );
    let (_, control) = watch::channel(Control::default());
    ActorCtx {
        meter: Meter::new(&providers, metrics),
        env: Arc::new(RuntimeEnv {
            providers,
            kv,
            clock: Arc::new(SystemClock),
            secrets: Arc::new(NoSecrets),
            registry: Arc::new(Registry::standard()),
        }),
        key,
        node: node(),
        node_hash: 0,
        control,
        settings: GraphSettings {
            gc_grace,
            ..GraphSettings::default()
        },
    }
}

fn record(epoch: u64, owner: &str, name: &str) -> CommitRecord {
    CommitRecord {
        epoch,
        owner: owner.to_owned(),
        position: Some(Position(0, epoch, 0)),
        watermark: 0,
        plan: 0,
        changes: vec![NodeChanges {
            node: node(),
            windows: vec![WindowChanges {
                window: window(),
                entries: vec![StateEntry {
                    key: Key::encode(&name.to_owned()).unwrap(),
                    acc: vec![epoch as u8],
                    prev_epoch: 0,
                }],
            }],
        }],
    }
}

async fn commit(kv: &dyn Kv, ctx: &ActorCtx<TokioProviders>, record: &CommitRecord) {
    state::try_commit(kv, &ctx.key, record).await.unwrap();
}

/// Fields of the window hash, sorted.
async fn copied(kv: &dyn Kv, ctx: &ActorCtx<TokioProviders>) -> Vec<String> {
    let mut fields: Vec<String> = kv
        .hgetall(&keys::slice_window(&ctx.key, &node(), &window()))
        .await
        .unwrap()
        .into_iter()
        .map(|(field, _)| field)
        .collect();
    fields.sort();
    fields
}

async fn live(kv: &dyn Kv, ctx: &ActorCtx<TokioProviders>) -> Vec<u64> {
    state::read_log(kv, &ctx.key)
        .await
        .unwrap()
        .records
        .iter()
        .map(|record| record.epoch)
        .collect()
}

fn field(name: &str, epoch: u64) -> String { keys::state_field(&Key::encode(&name.to_owned()).unwrap(), epoch) }

#[test]
fn backoff_doubles_up_to_its_ceiling() {
    let mut backoff = MIN_BACKOFF;
    assert_eq!(bump(&mut backoff), MIN_BACKOFF);
    assert_eq!(backoff, 2 * MIN_BACKOFF);
    assert_eq!(bump(&mut backoff), 2 * MIN_BACKOFF);
    assert_eq!(backoff, 4 * MIN_BACKOFF);
    let mut backoff = MAX_BACKOFF;
    assert_eq!(bump(&mut backoff), MAX_BACKOFF);
    assert_eq!(backoff, MAX_BACKOFF);
}

#[test]
fn only_a_value_too_large_cannot_be_cured_by_repeating() {
    let too_large = KvError::TooLarge { size: 2, limit: 1 };
    assert!(!too_large.transient());
    assert!(KvError::Unavailable("down".into()).transient());
    assert!(KvError::Protocol("odd".into()).transient());
    assert!(!StateError::Kv(too_large).transient());
    assert!(StateError::Kv(KvError::Unavailable("down".into())).transient());
    let corrupt = StateError::Corrupt {
        slice: "s".into(),
        reason: "r".into(),
    };
    assert!(!corrupt.transient());
}

/// The owner copies every record out, and empties the inherited ones (below
/// its head) as soon as each is copied; its own head stays.
#[tokio::test(flavor = "current_thread")]
async fn inherited_records_are_copied_out_then_emptied() {
    let kv: DynKv = MemKv::shared();
    let ctx = ctx(kv.clone(), Duration::from_secs(60));
    let (old, ours) = (record(1, "old", "a"), record(2, "me", "b"));
    commit(kv.as_ref(), &ctx, &old).await;
    commit(kv.as_ref(), &ctx, &ours).await;
    let mut proven = ctx.now();
    let done = copy_out(&ctx, 2, "me", &[&old, &ours], &mut proven).await.unwrap();
    assert!(done);
    assert_eq!(copied(kv.as_ref(), &ctx).await, [field("a", 1), field("b", 2)]);
    assert_eq!(live(kv.as_ref(), &ctx).await, [2]);
}

/// A proof older than `gc_grace / 4` is renewed by reading the log: an
/// instance that lost the slice meanwhile writes nothing.
#[tokio::test(flavor = "current_thread")]
async fn a_stale_proof_is_renewed_and_a_lost_slice_writes_nothing() {
    let kv: DynKv = MemKv::shared();
    let ctx = ctx(kv.clone(), Duration::from_millis(400));
    let (ours, theirs) = (record(1, "me", "a"), record(2, "other", "b"));
    commit(kv.as_ref(), &ctx, &ours).await;
    commit(kv.as_ref(), &ctx, &theirs).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Older than 100 ms, not older than 1.6 s.
    let mut proven = ctx.now() - Duration::from_millis(150);
    let done = copy_out(&ctx, 1, "me", &[&ours], &mut proven).await.unwrap();
    assert!(!done);
    assert!(copied(kv.as_ref(), &ctx).await.is_empty());

    // Same head epoch, another owner: lost as well.
    let kv: DynKv = MemKv::shared();
    let ctx = self::ctx(kv.clone(), Duration::from_millis(400));
    let theirs = record(1, "other", "a");
    commit(kv.as_ref(), &ctx, &theirs).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut proven = ctx.now() - Duration::from_millis(150);
    let done = copy_out(&ctx, 1, "me", &[&theirs], &mut proven).await.unwrap();
    assert!(!done);
    assert!(copied(kv.as_ref(), &ctx).await.is_empty());
}

/// Renewing the proof also tells which inherited records somebody else
/// finished and emptied meanwhile: those are not copied again.
#[tokio::test(flavor = "current_thread")]
async fn records_emptied_meanwhile_are_skipped() {
    let kv: DynKv = MemKv::shared();
    let ctx = ctx(kv.clone(), Duration::from_millis(400));
    let (old, ours) = (record(1, "old", "a"), record(2, "me", "b"));
    commit(kv.as_ref(), &ctx, &old).await;
    commit(kv.as_ref(), &ctx, &ours).await;
    state::tombstone(kv.as_ref(), &ctx.key, &[1]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut proven = ctx.now() - Duration::from_millis(150);
    let asked = ctx.now();
    let done = copy_out(&ctx, 2, "me", &[&old, &ours], &mut proven).await.unwrap();
    assert!(done);
    assert!(proven >= asked);
    assert_eq!(copied(kv.as_ref(), &ctx).await, [field("b", 2)]);
}

/// An error that repeating cannot cure is returned, not retried forever.
#[tokio::test(flavor = "current_thread")]
async fn a_write_too_large_fails_the_copy() {
    let memory = Arc::new(MemKv::default());
    let kv: DynKv = Arc::new(Refusing(memory));
    let ctx = ctx(kv.clone(), Duration::from_secs(60));
    let ours = record(1, "me", "a");
    commit(kv.as_ref(), &ctx, &ours).await;
    let mut proven = ctx.now();
    let records = [&ours];
    let copy = copy_out(&ctx, 1, "me", &records, &mut proven);
    let result = tokio::time::timeout(Duration::from_secs(5), copy)
        .await
        .expect("the copy gave up");
    assert!(result.is_err());
}

/// A root whose only aggregate has closed `windows`, under a limit of `limit`
/// pending windows.
fn root(kv: DynKv, limit: usize, windows: &[Window]) -> Root<TokioProviders> {
    let mut ctx = ctx(kv, Duration::from_secs(60));
    ctx.settings.max_pending_windows = limit;
    let (commits, _) = mpsc::unbounded_channel();
    let (_, committed) = watch::channel(0);
    Root {
        ctx,
        outputs: Outputs::new(Vec::new()),
        commits,
        committed,
        in_flight: VecDeque::new(),
        epoch: 0,
        slice: SliceMetrics::new(),
        exit: Exit {
            slot: Arc::new(Mutex::new(None)),
            stop: CancellationToken::new(),
        },
        stateful: true,
        owner: "me".into(),
        start_epoch: 0,
        gates: vec![Gate {
            emitter: "acme/g/agg.count".parse().unwrap(),
            closed: Arc::new(Mutex::new(windows.iter().copied().collect::<BTreeSet<_>>())),
        }],
    }
}

#[tokio::test]
async fn a_slice_holds_back_past_its_pending_windows_only() {
    let kv: DynKv = Arc::new(MemKv::new());
    let windows = [
        Window::new(0, 10_000),
        Window::new(10_000, 20_000),
        Window::new(20_000, 30_000),
    ];
    // The emitter has emitted the first window: two remain, as many as allowed.
    let emitter: streaming_core::SliceKey = "acme/g/agg.count".parse().unwrap();
    let mut emitted = record(1, "emitter", "a");
    emitted.position = Some(Position(ts_key(10_000), ts_key(0), u64::MAX));
    emitted.changes.clear();
    state::try_commit(kv.as_ref(), &emitter, &emitted).await.unwrap();

    let mut within = root(kv.clone(), 2, &windows);
    assert!(!within.throttled().await);
    assert_eq!(within.gates[0].closed.lock().len(), 2, "emitted windows are forgotten");
    assert_eq!(within.slice.throttled.load(Ordering::Relaxed), 0);

    let mut beyond = root(kv.clone(), 1, &windows);
    assert!(beyond.throttled().await);
    assert_eq!(beyond.slice.throttled.load(Ordering::Relaxed), 1);

    let mut unlimited = root(kv, 0, &windows);
    assert!(!unlimited.throttled().await);
}
