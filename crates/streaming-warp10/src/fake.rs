//! A fake Warp 10 ingress, for tests and simulations.
//!
//! [`FakeWarp10`] is a handle on shared state (an `Arc`): the points it holds
//! and its fault plan survive restarts of the task serving it, which is what a
//! crashing simulated `Process` needs. [`FakeWarp10::serve`] runs it over any
//! [`NetworkProvider`].
//!
//! What it implements, of `POST /api/v0/update`:
//!
//! - the token (`X-Warp10-Token`) is checked: `401` otherwise;
//! - the body is parsed as GTS input format lines ([`crate::gts::parse_line`]);
//!   blank lines and `#` comments are skipped. Real Warp 10 answers `500` on a
//!   malformed line, and may already have stored the lines before it; the fake
//!   answers `400` and stores **nothing** of that request;
//! - points are stored keyed by (class, sorted labels, timestamp): writing the
//!   same key again overwrites, as in Warp 10.
//!
//! Faults are queued with [`FakeWarp10::push_fault`] and consumed one per
//! well-formed, authenticated request, in order.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::sync::Arc;

use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TimeProvider};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::gts::{GtsValue, parse_line};
use crate::http::{HttpError, Limits, ReadError, build_response, header, read_request};

/// Identity of a stored point. The timestamp is the raw one received, in the
/// platform unit of the sender.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PointKey {
    /// Class name.
    pub class: String,
    /// Labels (sorted).
    pub labels: BTreeMap<String, String>,
    /// Timestamp as received.
    pub ts: i64,
}

impl PointKey {
    /// Build a key.
    #[must_use]
    pub fn new<K: Into<String>, V: Into<String>>(
        class: impl Into<String>,
        labels: impl IntoIterator<Item = (K, V)>,
        ts: i64,
    ) -> Self {
        Self {
            class: class.into(),
            labels: labels
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            ts,
        }
    }
}

/// A failure to inject into one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Answer with this status instead of storing anything (e.g. 503).
    Status(u16),
    /// Read the body, close the connection without answering and without
    /// storing: the request is lost, the client cannot know.
    DropBeforeCommit,
    /// Read the body, store the points, close the connection without
    /// answering: the "outcome unknown" case where the write did happen.
    DropAfterCommit,
}

/// Counters, for assertions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FakeStats {
    /// Requests fully read.
    pub requests: u64,
    /// Requests answered 200.
    pub accepted: u64,
    /// Points stored (counting overwrites).
    pub points_written: u64,
    /// Requests refused with 401.
    pub unauthorized: u64,
    /// Requests refused with 400 (malformed body).
    pub malformed: u64,
    /// Requests refused with 404 or 405.
    pub wrong_route: u64,
    /// `Fault::Status` applied.
    pub status_faults: u64,
    /// `Fault::DropBeforeCommit` applied.
    pub drops_before_commit: u64,
    /// `Fault::DropAfterCommit` applied.
    pub drops_after_commit: u64,
}

struct Inner {
    token: String,
    points: BTreeMap<PointKey, GtsValue>,
    faults: VecDeque<Fault>,
    stats: FakeStats,
}

/// The fake ingress. Cloning gives another handle on the same state.
#[derive(Clone)]
pub struct FakeWarp10 {
    inner: Arc<Mutex<Inner>>,
}

enum Verdict {
    Respond(u16, String),
    Drop,
}

impl FakeWarp10 {
    /// A fake accepting writes made with `token`.
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                token: token.into(),
                points: BTreeMap::new(),
                faults: VecDeque::new(),
                stats: FakeStats::default(),
            })),
        }
    }

    /// Queue a fault for an upcoming request.
    pub fn push_fault(&self, fault: Fault) {
        self.inner.lock().faults.push_back(fault);
    }

    /// Fail the next `n` requests with `status`.
    pub fn fail_next(&self, n: usize, status: u16) {
        for _ in 0..n {
            self.push_fault(Fault::Status(status));
        }
    }

    /// For the next `n` requests: store the points, then close the connection
    /// without answering.
    pub fn drop_after_body_next(&self, n: usize) {
        for _ in 0..n {
            self.push_fault(Fault::DropAfterCommit);
        }
    }

    /// For the next `n` requests: close the connection without storing nor
    /// answering.
    pub fn drop_before_commit_next(&self, n: usize) {
        for _ in 0..n {
            self.push_fault(Fault::DropBeforeCommit);
        }
    }

    /// Forget everything: points, queued faults and counters (the token stays).
    pub fn reset(&self) {
        let mut inner = self.inner.lock();
        inner.points.clear();
        inner.faults.clear();
        inner.stats = FakeStats::default();
    }

    /// Number of faults still queued.
    #[must_use]
    pub fn pending_faults(&self) -> usize {
        self.inner.lock().faults.len()
    }

    /// Everything stored.
    #[must_use]
    pub fn points(&self) -> BTreeMap<PointKey, GtsValue> {
        self.inner.lock().points.clone()
    }

    /// One stored value.
    #[must_use]
    pub fn get(&self, key: &PointKey) -> Option<GtsValue> {
        self.inner.lock().points.get(key).cloned()
    }

    /// Number of stored points.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().points.len()
    }

    /// Whether nothing is stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.lock().points.is_empty()
    }

    /// Counters.
    #[must_use]
    pub fn stats(&self) -> FakeStats {
        self.inner.lock().stats
    }

    /// Decide what to do with a request. Pure with respect to the network, so
    /// it can be unit-tested.
    fn handle(&self, method: &str, target: &str, token: Option<&str>, body: &[u8]) -> Verdict {
        let mut inner = self.inner.lock();
        inner.stats.requests += 1;
        let path = target.split('?').next().unwrap_or(target);
        if path != "/api/v0/update" {
            inner.stats.wrong_route += 1;
            return Verdict::Respond(404, format!("no such route {path}"));
        }
        if method != "POST" {
            inner.stats.wrong_route += 1;
            return Verdict::Respond(405, "use POST".into());
        }
        if token != Some(inner.token.as_str()) {
            inner.stats.unauthorized += 1;
            return Verdict::Respond(401, "invalid write token".into());
        }
        let parsed = match parse_body(body) {
            Ok(parsed) => parsed,
            Err(message) => {
                inner.stats.malformed += 1;
                return Verdict::Respond(400, message);
            }
        };
        let commit = |inner: &mut Inner, parsed: Vec<(PointKey, GtsValue)>| {
            inner.stats.points_written += parsed.len() as u64;
            for (key, value) in parsed {
                inner.points.insert(key, value);
            }
        };
        match inner.faults.pop_front() {
            Some(Fault::Status(status)) => {
                inner.stats.status_faults += 1;
                Verdict::Respond(status, "injected failure".into())
            }
            Some(Fault::DropBeforeCommit) => {
                inner.stats.drops_before_commit += 1;
                Verdict::Drop
            }
            Some(Fault::DropAfterCommit) => {
                inner.stats.drops_after_commit += 1;
                commit(&mut inner, parsed);
                Verdict::Drop
            }
            None => {
                inner.stats.accepted += 1;
                commit(&mut inner, parsed);
                Verdict::Respond(200, String::new())
            }
        }
    }

    /// Serve one connection: read a request, answer it (or drop the
    /// connection), close. Works on any byte stream, so TLS can be layered by
    /// the caller.
    pub async fn serve_stream<S>(&self, mut stream: S)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let request = match read_request(&mut stream, Limits::REQUEST).await {
            Ok(request) => request,
            Err(ReadError::Http(e @ (HttpError::Malformed(_) | HttpError::HeadTooLarge(_)))) => {
                let _ = stream.write_all(&build_response(400, &e.to_string())).await;
                let _ = stream.close().await;
                return;
            }
            Err(_) => return,
        };
        if request.truncated {
            let _ = stream
                .write_all(&build_response(413, "body too large"))
                .await;
            let _ = stream.close().await;
            return;
        }
        let token = header(&request.headers, "x-warp10-token");
        match self.handle(&request.method, &request.target, token, &request.body) {
            Verdict::Drop => {}
            Verdict::Respond(status, body) => {
                if stream
                    .write_all(&build_response(status, &body))
                    .await
                    .is_ok()
                {
                    let _ = stream.flush().await;
                }
                let _ = stream.close().await;
            }
        }
    }

    /// Bind `addr` and serve until `shutdown` is cancelled.
    pub async fn serve<P: Providers>(
        &self,
        providers: P,
        addr: &str,
        shutdown: CancellationToken,
    ) -> io::Result<()> {
        let listener = providers.network().bind(addr).await?;
        self.serve_listener(providers, listener, shutdown).await
    }

    /// Serve on an already bound listener until `shutdown` is cancelled.
    ///
    /// Connections are served by spawned tasks. They hold a child of
    /// `shutdown` that is cancelled when this future is dropped as well (a
    /// simulated process crash only drops its main future: tasks spawned
    /// through the task provider would otherwise outlive it).
    pub async fn serve_listener<P: Providers>(
        &self,
        providers: P,
        listener: <P::Network as NetworkProvider>::TcpListener,
        shutdown: CancellationToken,
    ) -> io::Result<()> {
        let connections = shutdown.child_token();
        let _stop_connections = connections.clone().drop_guard();
        loop {
            let accepted = moonpool_core::select! {
                biased;
                accepted = listener.accept() => accepted,
                () = shutdown.cancelled() => return Ok(()),
            };
            match accepted {
                Ok((stream, _peer)) => {
                    let (fake, token) = (self.clone(), connections.clone());
                    // Detached on purpose: ends with the connection or the token.
                    drop(providers.task().spawn_task("warp10-fake-conn", async move {
                        moonpool_core::select! {
                            biased;
                            () = fake.serve_stream(stream) => {}
                            () = token.cancelled() => {}
                        }
                    }));
                }
                Err(e) => {
                    tracing::debug!("fake warp10: accept failed: {e}");
                    // Do not spin on a persistent error.
                    let _ = providers
                        .time()
                        .sleep(std::time::Duration::from_millis(10))
                        .await;
                }
            }
        }
    }
}

fn parse_body(body: &[u8]) -> Result<Vec<(PointKey, GtsValue)>, String> {
    let text = std::str::from_utf8(body).map_err(|_| "body is not UTF-8".to_owned())?;
    let mut out = Vec::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim_end_matches('\r');
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let parsed = parse_line(line).map_err(|e| format!("line {}: {e}", number + 1))?;
        out.push((
            PointKey {
                class: parsed.class,
                labels: parsed.labels,
                ts: parsed.ts,
            },
            parsed.value,
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(fake: &FakeWarp10, token: Option<&str>, body: &str) -> (u16, String) {
        match fake.handle("POST", "/api/v0/update", token, body.as_bytes()) {
            Verdict::Respond(s, b) => (s, b),
            Verdict::Drop => (0, String::new()),
        }
    }

    #[test]
    fn stores_and_overwrites_by_series_and_timestamp() {
        let fake = FakeWarp10::new("t");
        assert_eq!(run(&fake, Some("t"), "1// m{a=b} 1\n2// m{a=b} 2\n").0, 200);
        assert_eq!(fake.len(), 2);
        assert_eq!(
            run(&fake, Some("t"), "1// m{a=b} 10\n1// m{a=c} 3\n").0,
            200
        );
        assert_eq!(fake.len(), 3);
        assert_eq!(
            fake.get(&PointKey::new("m", [("a", "b")], 1)),
            Some(GtsValue::Long(10))
        );
        assert_eq!(fake.stats().points_written, 4);
    }

    #[test]
    fn auth_route_and_malformed() {
        let fake = FakeWarp10::new("t");
        assert_eq!(run(&fake, None, "1// m{} 1\n").0, 401);
        assert_eq!(run(&fake, Some("x"), "1// m{} 1\n").0, 401);
        assert!(matches!(
            fake.handle("GET", "/api/v0/update", Some("t"), b""),
            Verdict::Respond(405, _)
        ));
        assert!(fake.is_empty());
        // malformed: nothing of the request is stored
        let (status, message) = run(&fake, Some("t"), "1// m{} 1\nbogus\n");
        assert_eq!(status, 400);
        assert!(message.contains("line 2"), "{message}");
        assert!(fake.is_empty());
        // blank lines and comments are fine
        assert_eq!(run(&fake, Some("t"), "\n# c\n1// m{} 1\r\n").0, 200);
        assert_eq!(fake.len(), 1);
    }

    #[test]
    fn faults_are_consumed_in_order() {
        let fake = FakeWarp10::new("t");
        fake.fail_next(1, 503);
        fake.drop_after_body_next(1);
        fake.drop_before_commit_next(1);
        assert_eq!(run(&fake, Some("t"), "1// m{} 1\n").0, 503);
        assert!(fake.is_empty());
        assert_eq!(run(&fake, Some("t"), "1// m{} 1\n").0, 0);
        assert_eq!(fake.len(), 1, "drop after commit stores");
        assert_eq!(run(&fake, Some("t"), "2// m{} 2\n").0, 0);
        assert_eq!(fake.len(), 1, "drop before commit does not");
        assert_eq!(fake.pending_faults(), 0);
        assert_eq!(run(&fake, Some("t"), "2// m{} 2\n").0, 200);
        let s = fake.stats();
        assert_eq!(
            (
                s.status_faults,
                s.drops_after_commit,
                s.drops_before_commit,
                s.accepted
            ),
            (1, 1, 1, 1)
        );
    }
}
