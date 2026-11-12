//! Cluster membership over gossip, Scuttlebutt anti-entropy (à la Cassandra /
//! chitchat), written against [`moonpool_core::Providers`] so that the exact
//! same code runs in production (`TokioProviders`) and under the Moonpool
//! deterministic simulator (`SimProviders`).
//!
//! * [`state`]: the pure protocol state machine (digests, deltas, failure
//!   detector, tombstones). No I/O.
//! * [`wire`]: frame format and message types.
//! * [`Gossip`]: the thin I/O shell: a TCP server answering exchanges and a
//!   round loop initiating them, both spawned through the task provider.
//!
//! # Lifecycle and cancellation
//!
//! Every task spawned by a node is wrapped so that it is dropped at its
//! current await point as soon as the `shutdown` token given to
//! [`Gossip::start`] is cancelled: after cancellation no task of the node makes
//! any further progress (no frame sent, no state change). Under the simulator,
//! a crashed `Process` only has its main future aborted, so it must tie the
//! token to that future, e.g. by holding [`Gossip::drop_guard`] (or
//! `shutdown.clone().drop_guard()`) inside `Process::run`.
//!
//! # Known limitations
//!
//! * Failure detection is a fixed-timeout detector (no phi accrual). A crashed
//!   node is seen dead `dead_after` after the *last time an observer saw its
//!   heartbeat increase*, and that last heartbeat may still be spreading after
//!   the crash: detection latency is `dead_after` + dissemination delay.
//! * A node learnt for the first time is reported not alive until one more
//!   heartbeat increase reaches us; a higher generation is taken as proof of
//!   life (a stale higher generation of a node that died again is shown alive
//!   for up to `dead_after`).
//! * The `(generation, heartbeat)` of forgotten nodes is remembered for
//!   `forget_after`; a stale copy surviving longer than that can bring the node
//!   back as *not alive* until it is forgotten again (never as alive).
//! * Every node must use the same [`Limits`]: a node rejects node states that
//!   violate its own limits.
//! * Generations must increase across restarts of a node id; choosing them
//!   (e.g. wall-clock milliseconds at startup) is the caller's job. Two
//!   processes running concurrently with the same node id are not detected:
//!   the higher generation wins everywhere except on the lower one itself.
//! * The digest is not bounded by the delta budget: it grows linearly with the
//!   number of nodes (capped by `Limits::max_nodes`, ~45 bytes per node for
//!   short ids).
//! * One TCP connection per exchange, no authentication or encryption, no
//!   graceful leave (a stopped node is detected like a crashed one).

pub mod config;
pub mod error;
pub mod state;
pub mod wire;

pub use config::{GossipConfig, Limits};
pub use error::{GossipError, WireError};
use moonpool_core::{NetworkProvider, Providers, RandomProvider, TaskProvider, TcpListenerTrait, TimeProvider};
use parking_lot::Mutex;
use state::{ClusterState, StateConfig, TargetRng};
pub use state::{Member, NodeView};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;
use tokio_util::sync::{CancellationToken, DropGuard};
use wire::{Delta, Message};

/// Observability counters of a node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GossipStats {
    /// Gossip rounds run.
    pub rounds: u64,
    /// Exchanges initiated (connect attempts).
    pub exchanges_initiated: u64,
    /// Initiated exchanges that completed.
    pub exchanges_completed: u64,
    /// Initiated exchanges that failed after connecting (I/O, protocol, timeout).
    pub exchanges_failed: u64,
    /// Connect attempts that failed or timed out.
    pub connect_failures: u64,
    /// Timeouts (connect or exchange, both sides).
    pub timeouts: u64,
    /// Inbound exchanges served to completion.
    pub inbound_completed: u64,
    /// Inbound connections refused because of `max_inbound_exchanges`.
    pub inbound_refused: u64,
    /// Frames written.
    pub frames_sent: u64,
    /// Frames read.
    pub frames_received: u64,
    /// Bytes written (frames, headers included).
    pub bytes_sent: u64,
    /// Bytes read (frames, headers included).
    pub bytes_received: u64,
    /// Frames rejected (oversize, checksum, malformed, unexpected).
    pub frames_rejected: u64,
    /// Exchanges refused because of a cluster id mismatch (both sides).
    pub cluster_mismatches: u64,
    /// Encoded size of the last digest sent.
    pub last_digest_bytes: u64,
    /// Largest digest sent.
    pub max_digest_bytes: u64,
    /// Encoded size of the last delta sent.
    pub last_delta_bytes: u64,
    /// Largest delta sent.
    pub max_delta_bytes: u64,
    /// Deltas truncated by the byte budget.
    pub deltas_truncated: u64,
    /// Node resets sent.
    pub resets_sent: u64,
    /// Incoming node deltas rejected.
    pub node_deltas_rejected: u64,
    /// Nodes replaced by a higher generation.
    pub generation_bumps: u64,
    /// Dead nodes seen alive again.
    pub nodes_revived: u64,
    /// Alive -> dead transitions.
    pub nodes_marked_dead: u64,
    /// Nodes forgotten.
    pub nodes_forgotten: u64,
    /// Nodes discovered.
    pub nodes_discovered: u64,
    /// Tombstones garbage-collected.
    pub tombstones_gc: u64,
}

#[derive(Default)]
struct Counters {
    rounds: AtomicU64,
    exchanges_initiated: AtomicU64,
    exchanges_completed: AtomicU64,
    exchanges_failed: AtomicU64,
    connect_failures: AtomicU64,
    timeouts: AtomicU64,
    inbound_completed: AtomicU64,
    inbound_refused: AtomicU64,
    frames_sent: AtomicU64,
    frames_received: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    frames_rejected: AtomicU64,
    cluster_mismatches: AtomicU64,
    last_digest_bytes: AtomicU64,
    max_digest_bytes: AtomicU64,
    last_delta_bytes: AtomicU64,
    max_delta_bytes: AtomicU64,
}

fn inc(c: &AtomicU64) { c.fetch_add(1, Ordering::Relaxed); }

struct Inner<P: Providers> {
    providers: P,
    config: GossipConfig,
    state: Mutex<ClusterState>,
    changes: watch::Sender<u64>,
    counters: Counters,
    shutdown: CancellationToken,
    inbound: AtomicUsize,
}

/// Handle on a running gossip node. Cheap to clone.
pub struct Gossip<P: Providers> {
    inner: Arc<Inner<P>>,
}

impl<P: Providers> Clone for Gossip<P> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<P: Providers> std::fmt::Debug for Gossip<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gossip")
            .field("node_id", &self.inner.config.node_id)
            .field("generation", &self.inner.config.generation)
            .finish_non_exhaustive()
    }
}

/// Spawn `fut` through the task provider, dropped as soon as `token` is
/// cancelled (biased: cancellation wins over any ready branch).
fn spawn_guarded<P, F>(providers: &P, token: &CancellationToken, name: &str, fut: F)
where
    P: Providers,
    F: Future<Output = ()> + Send + 'static,
{
    let token = token.clone();
    // Detached: dropping the join handle does not cancel the task.
    drop(providers.task().spawn_task(name, async move {
        moonpool_core::select! {
            biased;
            () = token.cancelled() => {}
            () = fut => {}
        }
    }));
}

struct ProviderRng<'a, P: Providers>(&'a P);

impl<P: Providers> TargetRng for ProviderRng<'_, P> {
    fn index(&mut self, n: usize) -> usize { self.0.random().random_range(0..n) }
    fn chance(&mut self, p: f64) -> bool { p > 0.0 && self.0.random().random_bool(p.min(1.0)) }
}

impl<P: Providers> Gossip<P> {
    /// Validate the config, bind `listen_addr`, and spawn the server and
    /// round loops through the task provider. Cancelling `shutdown` stops
    /// every task of this node.
    ///
    /// # Errors
    /// Invalid configuration or bind failure.
    pub async fn start(providers: P, config: GossipConfig, shutdown: CancellationToken) -> Result<Self, GossipError> {
        config.validate()?;
        let listener = providers.network().bind(&config.listen_addr).await.map_err(|e| {
            let addr = config.listen_addr.clone();
            if e.kind() == std::io::ErrorKind::AddrInUse {
                GossipError::AddrInUse { addr }
            } else {
                GossipError::Bind {
                    addr,
                    reason: e.to_string(),
                }
            }
        })?;
        let now = providers.time().timer();
        let state = ClusterState::new(
            &config.node_id,
            config.generation,
            &config.advertise_addr,
            StateConfig::from(&config),
            now,
        );
        let (changes, _) = watch::channel(0u64);
        let inner = Arc::new(Inner {
            providers: providers.clone(),
            config,
            state: Mutex::new(state),
            changes,
            counters: Counters::default(),
            shutdown: shutdown.clone(),
            inbound: AtomicUsize::new(0),
        });
        spawn_guarded(
            &providers,
            &shutdown,
            "gossip-server",
            server_loop(Arc::clone(&inner), listener),
        );
        spawn_guarded(&providers, &shutdown, "gossip-rounds", round_loop(Arc::clone(&inner)));
        tracing::debug!(
            node = %inner.config.node_id,
            generation = inner.config.generation,
            addr = %inner.config.advertise_addr,
            "gossip started"
        );
        Ok(Self { inner })
    }

    /// Publish / overwrite one of our own keys.
    ///
    /// # Errors
    /// Key / value / key-count limit violations.
    pub fn set(&self, key: &str, value: &str) -> Result<(), GossipError> {
        let now = self.inner.providers.time().timer();
        let changed = self.inner.state.lock().set_local(key, value, now)?;
        if changed {
            self.inner.bump();
        }
        Ok(())
    }

    /// Delete one of our own keys (no-op when absent).
    pub fn remove(&self, key: &str) {
        let now = self.inner.providers.time().timer();
        if self.inner.state.lock().remove_local(key, now) {
            self.inner.bump();
        }
    }

    /// Our node id.
    #[must_use]
    pub fn node_id(&self) -> &str { &self.inner.config.node_id }

    /// Our generation.
    #[must_use]
    pub fn generation(&self) -> u64 { self.inner.config.generation }

    /// Our advertise address.
    #[must_use]
    pub fn advertise_addr(&self) -> &str { &self.inner.config.advertise_addr }

    /// The configuration in use.
    #[must_use]
    pub fn config(&self) -> &GossipConfig { &self.inner.config }

    /// Every known node, including self, sorted by node id.
    #[must_use]
    pub fn members(&self) -> Vec<Member> { self.inner.state.lock().members() }

    /// Alive nodes, including self, sorted by node id.
    #[must_use]
    pub fn live_members(&self) -> Vec<Member> {
        let mut m = self.members();
        m.retain(|m| m.alive);
        m
    }

    /// Internal replicated state of one node (debugging / tests).
    #[must_use]
    pub fn node_view(&self, node_id: &str) -> Option<NodeView> { self.inner.state.lock().node_view(node_id) }

    /// Bumps whenever membership or any value changes.
    #[must_use]
    pub fn changes(&self) -> watch::Receiver<u64> { self.inner.changes.subscribe() }

    /// Current counters.
    #[must_use]
    pub fn stats(&self) -> GossipStats {
        let c = &self.inner.counters;
        let r = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let s = self.inner.state.lock().counters().clone();
        GossipStats {
            rounds: r(&c.rounds),
            exchanges_initiated: r(&c.exchanges_initiated),
            exchanges_completed: r(&c.exchanges_completed),
            exchanges_failed: r(&c.exchanges_failed),
            connect_failures: r(&c.connect_failures),
            timeouts: r(&c.timeouts),
            inbound_completed: r(&c.inbound_completed),
            inbound_refused: r(&c.inbound_refused),
            frames_sent: r(&c.frames_sent),
            frames_received: r(&c.frames_received),
            bytes_sent: r(&c.bytes_sent),
            bytes_received: r(&c.bytes_received),
            frames_rejected: r(&c.frames_rejected),
            cluster_mismatches: r(&c.cluster_mismatches),
            last_digest_bytes: r(&c.last_digest_bytes),
            max_digest_bytes: r(&c.max_digest_bytes),
            last_delta_bytes: r(&c.last_delta_bytes),
            max_delta_bytes: r(&c.max_delta_bytes),
            deltas_truncated: s.deltas_truncated,
            resets_sent: s.resets_sent,
            node_deltas_rejected: s.node_deltas_rejected,
            generation_bumps: s.generation_bumps,
            nodes_revived: s.nodes_revived,
            nodes_marked_dead: s.nodes_marked_dead,
            nodes_forgotten: s.nodes_forgotten,
            nodes_discovered: s.nodes_discovered,
            tombstones_gc: s.tombstones_gc,
        }
    }

    /// The shutdown token of this node.
    #[must_use]
    pub fn shutdown_token(&self) -> &CancellationToken { &self.inner.shutdown }

    /// Stop every task of this node.
    pub fn shutdown(&self) { self.inner.shutdown.cancel(); }

    /// A guard that shuts the node down when dropped. Hold it in the future
    /// that owns the node (e.g. a simulated `Process::run`) so that aborting
    /// that future stops the node's tasks.
    #[must_use]
    pub fn drop_guard(&self) -> DropGuard { self.inner.shutdown.clone().drop_guard() }
}

impl<P: Providers> Inner<P> {
    fn bump(&self) { self.changes.send_modify(|v| *v = v.wrapping_add(1)); }

    fn apply(&self, delta: Delta) {
        let now = self.providers.time().timer();
        let changed = self.state.lock().apply_delta(delta, now);
        if changed {
            self.bump();
        }
    }

    fn record_digest(&self, bytes: usize) {
        let b = bytes as u64;
        self.counters.last_digest_bytes.store(b, Ordering::Relaxed);
        self.counters.max_digest_bytes.fetch_max(b, Ordering::Relaxed);
    }

    fn record_delta(&self, bytes: usize) {
        let b = bytes as u64;
        self.counters.last_delta_bytes.store(b, Ordering::Relaxed);
        self.counters.max_delta_bytes.fetch_max(b, Ordering::Relaxed);
    }

    async fn send<S: futures::io::AsyncWrite + Unpin + Send>(&self, s: &mut S, msg: &Message) -> Result<(), WireError> {
        let n = wire::write_frame(s, msg).await?;
        inc(&self.counters.frames_sent);
        self.counters.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
        Ok(())
    }

    async fn recv<S: futures::io::AsyncRead + Unpin + Send>(&self, s: &mut S) -> Result<Message, WireError> {
        match wire::read_frame(s, self.config.max_frame_bytes).await {
            Ok((msg, n)) => {
                inc(&self.counters.frames_received);
                self.counters.bytes_received.fetch_add(n as u64, Ordering::Relaxed);
                Ok(msg)
            }
            Err(WireError::Io(e)) => Err(WireError::Io(e)),
            Err(e) => {
                inc(&self.counters.frames_rejected);
                Err(e)
            }
        }
    }

    fn tie_break(&self) -> u64 { self.providers.random().random_range(0..u64::MAX) }

    /// Digest + delta for a peer digest, sizes recorded.
    fn answer(&self, peer: &wire::Digest) -> Result<(wire::Digest, Delta), WireError> {
        let tie = self.tie_break();
        let mut st = self.state.lock();
        st.validate_digest(peer).map_err(WireError::Protocol)?;
        let digest = st.digest();
        let (delta, info) = st.compute_delta(peer, self.config.delta_budget_bytes, tie);
        drop(st);
        self.record_digest(wire::digest_size(&digest));
        self.record_delta(info.bytes);
        Ok((digest, delta))
    }

    fn protocol_error(&self, what: &str) -> WireError {
        inc(&self.counters.frames_rejected);
        WireError::Protocol(what.to_string())
    }
}

async fn round_loop<P: Providers>(inner: Arc<Inner<P>>) {
    let interval = inner.config.gossip_interval;
    // Random initial offset so that nodes started together do not gossip in lockstep.
    let jitter_us = u64::try_from(interval.as_micros()).unwrap_or(u64::MAX).max(1);
    let first = Duration::from_micros(inner.providers.random().random_range(0..jitter_us));
    if inner.providers.time().sleep(first).await.is_err() {
        return;
    }
    loop {
        inc(&inner.counters.rounds);
        let now = inner.providers.time().timer();
        let (live, dead, changed) = {
            let mut st = inner.state.lock();
            st.beat();
            let changed = st.tick(now);
            let (live, dead) = st.peer_addrs();
            (live, dead, changed)
        };
        if changed {
            inner.bump();
        }
        let targets = state::select_targets(
            &live,
            &dead,
            &inner.config.seeds,
            &inner.config.advertise_addr,
            inner.config.gossip_fanout,
            inner.config.seed_gossip_probability,
            &mut ProviderRng(&inner.providers),
        );
        for target in targets {
            spawn_guarded(
                &inner.providers,
                &inner.shutdown,
                "gossip-exchange",
                exchange(Arc::clone(&inner), target),
            );
        }
        if inner.providers.time().sleep(interval).await.is_err() {
            return;
        }
    }
}

async fn exchange<P: Providers>(inner: Arc<Inner<P>>, addr: String) {
    inc(&inner.counters.exchanges_initiated);
    let time = inner.providers.time().clone();
    let mut stream = match time
        .timeout(inner.config.connect_timeout, inner.providers.network().connect(&addr))
        .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            tracing::trace!(%addr, error = %e, "connect failed");
            inc(&inner.counters.connect_failures);
            return;
        }
        Err(_) => {
            tracing::trace!(%addr, "connect timed out");
            inc(&inner.counters.connect_failures);
            inc(&inner.counters.timeouts);
            return;
        }
    };
    match time
        .timeout(inner.config.exchange_timeout, initiate(&inner, &mut stream))
        .await
    {
        Ok(Ok(())) => inc(&inner.counters.exchanges_completed),
        Ok(Err(e)) => {
            tracing::trace!(%addr, error = %e, "exchange failed");
            inc(&inner.counters.exchanges_failed);
        }
        Err(_) => {
            inc(&inner.counters.exchanges_failed);
            inc(&inner.counters.timeouts);
        }
    }
}

async fn initiate<P: Providers, S>(inner: &Inner<P>, stream: &mut S) -> Result<(), WireError>
where
    S: futures::io::AsyncRead + futures::io::AsyncWrite + Unpin + Send,
{
    let digest = inner.state.lock().digest();
    inner.record_digest(wire::digest_size(&digest));
    inner
        .send(
            stream,
            &Message::Syn {
                cluster_id: inner.config.cluster_id.clone(),
                digest,
            },
        )
        .await?;
    let (peer_digest, delta) = match inner.recv(stream).await? {
        Message::SynAck { digest, delta } => (digest, delta),
        Message::BadCluster { cluster_id } => {
            inc(&inner.counters.cluster_mismatches);
            return Err(WireError::Protocol(format!("peer is in cluster {cluster_id:?}")));
        }
        _ => return Err(inner.protocol_error("expected SynAck")),
    };
    inner.apply(delta);
    let (_, ack) = inner.answer(&peer_digest)?;
    inner.send(stream, &Message::Ack { delta: ack }).await?;
    Ok(())
}

struct InboundSlot<P: Providers>(Arc<Inner<P>>);

impl<P: Providers> Drop for InboundSlot<P> {
    fn drop(&mut self) { self.0.inbound.fetch_sub(1, Ordering::Relaxed); }
}

async fn server_loop<P: Providers>(inner: Arc<Inner<P>>, listener: <P::Network as NetworkProvider>::TcpListener) {
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                if inner.inbound.fetch_add(1, Ordering::Relaxed) >= inner.config.max_inbound_exchanges {
                    inner.inbound.fetch_sub(1, Ordering::Relaxed);
                    inc(&inner.counters.inbound_refused);
                    drop(stream);
                    continue;
                }
                let slot = InboundSlot(Arc::clone(&inner));
                let inner2 = Arc::clone(&inner);
                spawn_guarded(&inner.providers, &inner.shutdown, "gossip-inbound", async move {
                    let _slot = slot;
                    serve(inner2, stream).await;
                });
            }
            Err(e) => {
                tracing::debug!(error = %e, "accept failed");
                if inner.providers.time().sleep(Duration::from_millis(10)).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn serve<P: Providers, S>(inner: Arc<Inner<P>>, mut stream: S)
where
    S: futures::io::AsyncRead + futures::io::AsyncWrite + Unpin + Send,
{
    let time = inner.providers.time().clone();
    match time
        .timeout(inner.config.exchange_timeout, respond(&inner, &mut stream))
        .await
    {
        Ok(Ok(true)) => inc(&inner.counters.inbound_completed),
        Ok(Ok(false)) => {}
        Ok(Err(e)) => tracing::trace!(error = %e, "inbound exchange failed"),
        Err(_) => inc(&inner.counters.timeouts),
    }
}

/// Serve one exchange; `Ok(false)` when refused for a cluster mismatch.
async fn respond<P: Providers, S>(inner: &Inner<P>, stream: &mut S) -> Result<bool, WireError>
where
    S: futures::io::AsyncRead + futures::io::AsyncWrite + Unpin + Send,
{
    let (cluster_id, peer_digest) = match inner.recv(stream).await? {
        Message::Syn { cluster_id, digest } => (cluster_id, digest),
        _ => return Err(inner.protocol_error("expected Syn")),
    };
    if cluster_id != inner.config.cluster_id {
        inc(&inner.counters.cluster_mismatches);
        inner
            .send(
                stream,
                &Message::BadCluster {
                    cluster_id: inner.config.cluster_id.clone(),
                },
            )
            .await?;
        return Ok(false);
    }
    let (digest, delta) = inner.answer(&peer_digest)?;
    inner.send(stream, &Message::SynAck { digest, delta }).await?;
    let delta = match inner.recv(stream).await? {
        Message::Ack { delta } => delta,
        _ => return Err(inner.protocol_error("expected Ack")),
    };
    inner.apply(delta);
    Ok(true)
}
