//! An in-process fake Redis server for simulations and tests.
//!
//! [`FakeRedis`] implements exactly the commands the client uses (`GET`,
//! `SET`, `DEL`, `INCR`, `HSETNX`, `HSET`, `HGET`, `HGETALL`, `HSCAN`, `HDEL`,
//! `AUTH`, `PING`) with Redis semantics: a key holds a string or a hash,
//! wrong-type access is `-WRONGTYPE`, a hash losing its last field
//! disappears, every command is atomic. `HSCAN` has real cursor paging over
//! insertion order, is stable under concurrent modification, and
//! occasionally answers an empty page with a non-zero cursor like the real
//! server may. It also enforces the Materia KV limits that shape the client
//! (at most 100 keys per `DEL`, values up to 5 MiB by default).
//!
//! The data lives behind an `Arc` shared by every clone, so it survives the
//! server task being crashed and restarted: boot a fresh
//! `fake.clone().serve(..)` and the old state is still there.

use crate::resp::{Decoder, Value, encode_value};
use bytes::Bytes;
use futures::{
    StreamExt,
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    stream::FuturesUnordered,
};
use moonpool_core::{NetworkProvider, Providers, TcpListenerTrait, TimeProvider, select};
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

/// `DEL` key limit enforced like Materia KV.
pub const FAKE_MAX_KEYS_PER_DEL: usize = 100;

#[derive(Debug, Default, Clone)]
struct Hash {
    next_seq: u64,
    by_seq: BTreeMap<u64, (Vec<u8>, Vec<u8>)>,
    index: HashMap<Vec<u8>, u64>,
}

impl Hash {
    /// Returns true when the field is new.
    fn set(&mut self, field: &[u8], value: &[u8]) -> bool {
        if let Some(seq) = self.index.get(field) {
            if let Some(slot) = self.by_seq.get_mut(seq) {
                slot.1 = value.to_vec();
            }
            false
        } else {
            self.next_seq += 1;
            self.index.insert(field.to_vec(), self.next_seq);
            self.by_seq.insert(self.next_seq, (field.to_vec(), value.to_vec()));
            true
        }
    }

    fn get(&self, field: &[u8]) -> Option<&Vec<u8>> {
        self.index
            .get(field)
            .and_then(|seq| self.by_seq.get(seq))
            .map(|(_, v)| v)
    }

    fn remove(&mut self, field: &[u8]) -> bool {
        match self.index.remove(field) {
            Some(seq) => {
                self.by_seq.remove(&seq);
                true
            }
            None => false,
        }
    }

    fn len(&self) -> usize {
        self.index.len()
    }
}

#[derive(Debug, Clone)]
enum Entry {
    Str(Vec<u8>),
    Hash(Hash),
}

#[derive(Default)]
struct Shared {
    store: Mutex<HashMap<Vec<u8>, Entry>>,
    stats: Mutex<BTreeMap<String, u64>>,
    scan_calls: AtomicU64,
    connections: AtomicU64,
    auth: Option<(Option<String>, String)>,
    max_value_bytes: Option<usize>,
}

/// The fake server. Cloning shares the data.
#[derive(Clone, Default)]
pub struct FakeRedis {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for FakeRedis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeRedis")
            .field("keys", &self.shared.store.lock().len())
            .finish_non_exhaustive()
    }
}

fn err(msg: &str) -> Value {
    Value::Error(msg.to_owned())
}

fn wrong_type() -> Value {
    err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn arity(cmd: &str) -> Value {
    Value::Error(format!(
        "ERR wrong number of arguments for '{}' command",
        cmd.to_ascii_lowercase()
    ))
}

fn bulk(b: &[u8]) -> Value {
    Value::Bulk(Bytes::copy_from_slice(b))
}

/// Strict Redis integer parsing (`string2ll`): no `+`, no leading zeros.
fn parse_i64(raw: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(raw).ok()?;
    let n: i64 = text.parse().ok()?;
    (n.to_string() == text).then_some(n)
}

/// Per-connection state.
struct Session {
    authed: bool,
}

impl FakeRedis {
    /// An empty server without authentication.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty server requiring `AUTH` (`username` `None` is the default
    /// user, `AUTH <password>`).
    #[must_use]
    pub fn with_password(username: Option<&str>, password: &str) -> Self {
        Self {
            shared: Arc::new(Shared {
                auth: Some((username.map(str::to_owned), password.to_owned())),
                ..Shared::default()
            }),
        }
    }

    /// An empty server refusing values above `limit` bytes (default 5 MiB).
    #[must_use]
    pub fn with_max_value_bytes(limit: usize) -> Self {
        Self {
            shared: Arc::new(Shared {
                max_value_bytes: Some(limit),
                ..Shared::default()
            }),
        }
    }

    /// How many times a command (upper case, e.g. `"HSCAN"`) was received.
    #[must_use]
    pub fn command_count(&self, name: &str) -> u64 {
        self.shared.stats.lock().get(name).copied().unwrap_or(0)
    }

    /// Number of connections served so far (including closed ones).
    #[must_use]
    pub fn connections_accepted(&self) -> u64 {
        self.shared.connections.load(Ordering::Relaxed)
    }

    /// Number of keys stored.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.shared.store.lock().len()
    }

    /// Direct read of a string key, bypassing the network (for invariants).
    #[must_use]
    pub fn peek_string(&self, key: &str) -> Option<Vec<u8>> {
        match self.shared.store.lock().get(key.as_bytes()) {
            Some(Entry::Str(v)) => Some(v.clone()),
            _ => None,
        }
    }

    /// Direct read of a hash field, bypassing the network (for invariants).
    #[must_use]
    pub fn peek_field(&self, key: &str, field: &str) -> Option<Vec<u8>> {
        match self.shared.store.lock().get(key.as_bytes()) {
            Some(Entry::Hash(h)) => h.get(field.as_bytes()).cloned(),
            _ => None,
        }
    }

    fn max_value(&self) -> usize {
        self.shared.max_value_bytes.unwrap_or(5 * 1024 * 1024)
    }

    /// Execute one command. Atomic: the store lock is held for the whole
    /// command and no `await` happens inside.
    fn execute(&self, session: &mut Session, args: &[Bytes]) -> Value {
        let Some(name) = args.first() else {
            return err("ERR empty command");
        };
        let name = String::from_utf8_lossy(name).to_ascii_uppercase();
        *self.shared.stats.lock().entry(name.clone()).or_insert(0) += 1;
        let a = &args[1..];

        if name == "AUTH" {
            return self.auth(session, a);
        }
        if !session.authed {
            return err("NOAUTH Authentication required.");
        }
        let mut store = self.shared.store.lock();
        match name.as_str() {
            "PING" => match a.len() {
                0 => Value::Simple("PONG".into()),
                1 => Value::Bulk(a[0].clone()),
                _ => arity(&name),
            },
            "GET" => {
                if a.len() != 1 {
                    return arity(&name);
                }
                match store.get(&a[0][..]) {
                    None => Value::Null,
                    Some(Entry::Str(v)) => bulk(v),
                    Some(Entry::Hash(_)) => wrong_type(),
                }
            }
            "SET" => {
                if a.len() != 2 {
                    return if a.len() < 2 {
                        arity(&name)
                    } else {
                        err("ERR syntax error")
                    };
                }
                if a[1].len() > self.max_value() {
                    return err("ERR value is too large");
                }
                store.insert(a[0].to_vec(), Entry::Str(a[1].to_vec()));
                Value::Simple("OK".into())
            }
            "DEL" => {
                if a.is_empty() {
                    return arity(&name);
                }
                if a.len() > FAKE_MAX_KEYS_PER_DEL {
                    return err("ERR too many keys in DEL (max 100)");
                }
                let removed = a.iter().filter(|k| store.remove(&k[..]).is_some()).count();
                Value::Integer(removed as i64)
            }
            "INCR" => {
                if a.len() != 1 {
                    return arity(&name);
                }
                let current = match store.get(&a[0][..]) {
                    None => 0,
                    Some(Entry::Str(v)) => match parse_i64(v) {
                        Some(n) => n,
                        None => return err("ERR value is not an integer or out of range"),
                    },
                    Some(Entry::Hash(_)) => return wrong_type(),
                };
                let Some(next) = current.checked_add(1) else {
                    return err("ERR increment or decrement would overflow");
                };
                store.insert(a[0].to_vec(), Entry::Str(next.to_string().into_bytes()));
                Value::Integer(next)
            }
            "HSETNX" => {
                if a.len() != 3 {
                    return arity(&name);
                }
                if a[2].len() > self.max_value() {
                    return err("ERR value is too large");
                }
                match store.get_mut(&a[0][..]) {
                    Some(Entry::Str(_)) => wrong_type(),
                    Some(Entry::Hash(h)) => {
                        if h.get(&a[1]).is_some() {
                            Value::Integer(0)
                        } else {
                            h.set(&a[1], &a[2]);
                            Value::Integer(1)
                        }
                    }
                    None => {
                        let mut h = Hash::default();
                        h.set(&a[1], &a[2]);
                        store.insert(a[0].to_vec(), Entry::Hash(h));
                        Value::Integer(1)
                    }
                }
            }
            "HSET" => {
                if a.len() < 3 || a.len().is_multiple_of(2) {
                    return arity(&name);
                }
                if a[1..].chunks(2).any(|p| p[1].len() > self.max_value()) {
                    return err("ERR value is too large");
                }
                let entry = store
                    .entry(a[0].to_vec())
                    .or_insert_with(|| Entry::Hash(Hash::default()));
                let Entry::Hash(h) = entry else {
                    return wrong_type();
                };
                let created = a[1..].chunks(2).filter(|p| h.set(&p[0], &p[1])).count();
                Value::Integer(created as i64)
            }
            "HGET" => {
                if a.len() != 2 {
                    return arity(&name);
                }
                match store.get(&a[0][..]) {
                    None => Value::Null,
                    Some(Entry::Str(_)) => wrong_type(),
                    Some(Entry::Hash(h)) => h.get(&a[1]).map_or(Value::Null, |v| bulk(v)),
                }
            }
            "HGETALL" => {
                if a.len() != 1 {
                    return arity(&name);
                }
                match store.get(&a[0][..]) {
                    None => Value::Array(vec![]),
                    Some(Entry::Str(_)) => wrong_type(),
                    Some(Entry::Hash(h)) => {
                        Value::Array(h.by_seq.values().flat_map(|(f, v)| [bulk(f), bulk(v)]).collect())
                    }
                }
            }
            "HDEL" => {
                if a.len() < 2 {
                    return arity(&name);
                }
                let (removed, now_empty) = match store.get_mut(&a[0][..]) {
                    None => return Value::Integer(0),
                    Some(Entry::Str(_)) => return wrong_type(),
                    Some(Entry::Hash(h)) => {
                        let removed = a[1..].iter().filter(|f| h.remove(f)).count();
                        (removed, h.len() == 0)
                    }
                };
                if now_empty {
                    store.remove(&a[0][..]);
                }
                Value::Integer(removed as i64)
            }
            "HSCAN" => {
                if a.len() < 2 {
                    return arity(&name);
                }
                let Some(cursor) = std::str::from_utf8(&a[1]).ok().and_then(|c| c.parse::<u64>().ok()) else {
                    return err("ERR invalid cursor");
                };
                let mut count = 10usize;
                let mut opts = a[2..].iter();
                while let Some(opt) = opts.next() {
                    if opt.eq_ignore_ascii_case(b"COUNT") {
                        match opts.next().and_then(|n| parse_i64(n)) {
                            Some(n) if n >= 1 => count = n as usize,
                            Some(_) => return err("ERR syntax error"),
                            None => return err("ERR value is not an integer or out of range"),
                        }
                    } else {
                        return err("ERR syntax error");
                    }
                }
                match store.get(&a[0][..]) {
                    None => Value::Array(vec![bulk(b"0"), Value::Array(vec![])]),
                    Some(Entry::Str(_)) => wrong_type(),
                    Some(Entry::Hash(h)) => {
                        // Like the real server, sometimes answer an empty
                        // page and keep the cursor.
                        let call = self.shared.scan_calls.fetch_add(1, Ordering::Relaxed);
                        if call % 5 == 4 && cursor != 0 {
                            return Value::Array(vec![bulk(cursor.to_string().as_bytes()), Value::Array(vec![])]);
                        }
                        let mut flat = Vec::new();
                        let mut last = None;
                        let mut more = false;
                        // Sequence numbers start at 1: cursor c resumes at seq >= c.
                        for (taken, (seq, (f, v))) in h.by_seq.range(cursor..).enumerate() {
                            if taken == count {
                                more = true;
                                break;
                            }
                            flat.push(bulk(f));
                            flat.push(bulk(v));
                            last = Some(*seq);
                        }
                        let next = match (more, last) {
                            (true, Some(seq)) => seq + 1,
                            _ => 0,
                        };
                        Value::Array(vec![bulk(next.to_string().as_bytes()), Value::Array(flat)])
                    }
                }
            }
            _ => Value::Error(format!("ERR unknown command '{name}'")),
        }
    }

    fn auth(&self, session: &mut Session, a: &[Bytes]) -> Value {
        let Some((user, password)) = &self.shared.auth else {
            return err(
                "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
            );
        };
        let (given_user, given_pass) = match a {
            [p] => (None, &p[..]),
            [u, p] => (Some(&u[..]), &p[..]),
            _ => return arity("AUTH"),
        };
        let user_ok = match (user, given_user) {
            (None, None) => true,
            (None, Some(u)) => u == b"default",
            (Some(want), Some(got)) => want.as_bytes() == got,
            (Some(_), None) => false,
        };
        if user_ok && password.as_bytes() == given_pass {
            session.authed = true;
            Value::Simple("OK".into())
        } else {
            err("WRONGPASS invalid username-password pair or user is disabled.")
        }
    }

    /// Serve one connection until it closes or breaks. Public so tests can
    /// run the fake behind another transport (for example TLS).
    pub async fn serve_stream<S>(&self, mut stream: S)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        self.shared.connections.fetch_add(1, Ordering::Relaxed);
        let mut session = Session {
            authed: self.shared.auth.is_none(),
        };
        let mut decoder = Decoder::new();
        let mut buf = vec![0u8; 16 * 1024];
        let mut out = Vec::new();
        loop {
            loop {
                match decoder.next_value() {
                    Ok(Some(Value::Array(items))) => {
                        let args: Option<Vec<Bytes>> = items
                            .into_iter()
                            .map(|v| match v {
                                Value::Bulk(b) => Some(b),
                                _ => None,
                            })
                            .collect();
                        let reply = match args {
                            Some(args) if !args.is_empty() => self.execute(&mut session, &args),
                            _ => err("ERR Protocol error: expected array of bulk strings"),
                        };
                        out.clear();
                        encode_value(&reply, &mut out);
                        if stream.write_all(&out).await.is_err() || stream.flush().await.is_err() {
                            return;
                        }
                    }
                    Ok(Some(_)) | Err(_) => {
                        out.clear();
                        encode_value(&err("ERR Protocol error"), &mut out);
                        let _ = stream.write_all(&out).await;
                        let _ = stream.flush().await;
                        return;
                    }
                    Ok(None) => break,
                }
            }
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => decoder.feed(&buf[..n]),
            }
        }
    }

    /// Bind `addr` through the provider and serve connections until
    /// `shutdown` is cancelled (or the future is dropped, which is how a
    /// simulated crash kills it). Connections are driven by this very
    /// future, no task is spawned, so dropping it also drops every client
    /// connection.
    ///
    /// # Errors
    /// Binding `addr` failed.
    pub async fn serve<P: Providers>(self, providers: P, addr: &str, shutdown: CancellationToken) -> io::Result<()> {
        let listener = providers.network().bind(addr).await?;
        type Conn = Pin<Box<dyn Future<Output = ()> + Send>>;
        let mut conns: FuturesUnordered<Conn> = FuturesUnordered::new();
        loop {
            select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => match accepted {
                    Ok((stream, _peer)) => {
                        let this = self.clone();
                        conns.push(Box::pin(async move { this.serve_stream(stream).await }));
                    }
                    Err(e) => {
                        tracing::debug!("fake redis accept error: {e}");
                        let _ = providers.time().sleep(Duration::from_millis(1)).await;
                    }
                },
                Some(()) = conns.next(), if !conns.is_empty() => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(fake: &FakeRedis, session: &mut Session, cmd: &[&str]) -> Value {
        let args: Vec<Bytes> = cmd.iter().map(|s| Bytes::copy_from_slice(s.as_bytes())).collect();
        fake.execute(session, &args)
    }

    fn fresh() -> (FakeRedis, Session) {
        (FakeRedis::new(), Session { authed: true })
    }

    #[test]
    fn strings_and_wrong_type() {
        let (f, mut s) = fresh();
        assert_eq!(run(&f, &mut s, &["SET", "a", "1"]), Value::Simple("OK".into()));
        assert_eq!(run(&f, &mut s, &["INCR", "a"]), Value::Integer(2));
        assert_eq!(run(&f, &mut s, &["HSET", "h", "f", "v"]), Value::Integer(1));
        assert!(matches!(run(&f, &mut s, &["GET", "h"]), Value::Error(m) if m.starts_with("WRONGTYPE")));
        assert!(matches!(run(&f, &mut s, &["HGET", "a", "f"]), Value::Error(m) if m.starts_with("WRONGTYPE")));
        assert!(matches!(run(&f, &mut s, &["INCR", "h"]), Value::Error(m) if m.starts_with("WRONGTYPE")));
        run(&f, &mut s, &["SET", "t", "abc"]);
        assert!(matches!(run(&f, &mut s, &["INCR", "t"]), Value::Error(m) if m.contains("not an integer")));
        run(&f, &mut s, &["SET", "p", "+5"]);
        assert!(matches!(run(&f, &mut s, &["INCR", "p"]), Value::Error(_)));
        run(&f, &mut s, &["SET", "m", "9223372036854775807"]);
        assert!(matches!(run(&f, &mut s, &["INCR", "m"]), Value::Error(m) if m.contains("overflow")));
    }

    #[test]
    fn hash_lifecycle_and_hsetnx() {
        let (f, mut s) = fresh();
        assert_eq!(run(&f, &mut s, &["HSETNX", "h", "f", "1"]), Value::Integer(1));
        assert_eq!(run(&f, &mut s, &["HSETNX", "h", "f", "2"]), Value::Integer(0));
        assert_eq!(f.peek_field("h", "f"), Some(b"1".to_vec()));
        assert_eq!(run(&f, &mut s, &["HSET", "h", "f", "3", "g", "4"]), Value::Integer(1));
        assert_eq!(run(&f, &mut s, &["HDEL", "h", "f", "nope"]), Value::Integer(1));
        assert_eq!(f.key_count(), 1);
        assert_eq!(run(&f, &mut s, &["HDEL", "h", "g"]), Value::Integer(1));
        assert_eq!(f.key_count(), 0, "hash disappears with its last field");
        assert_eq!(run(&f, &mut s, &["GET", "h"]), Value::Null);
    }

    #[test]
    fn hscan_pages_cover_everything_exactly_once() {
        let (f, mut s) = fresh();
        for i in 0..1000 {
            run(&f, &mut s, &["HSET", "h", &format!("f{i}"), "v"]);
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut cursor = "0".to_owned();
        let mut pages = 0;
        loop {
            let Value::Array(parts) = run(&f, &mut s, &["HSCAN", "h", &cursor, "COUNT", "37"]) else {
                panic!()
            };
            let Value::Bulk(next) = &parts[0] else { panic!() };
            let Value::Array(flat) = &parts[1] else { panic!() };
            for pair in flat.chunks(2) {
                let Value::Bulk(field) = &pair[0] else { panic!() };
                assert!(seen.insert(field.clone()), "duplicate field in one scan");
            }
            pages += 1;
            cursor = String::from_utf8(next.to_vec()).unwrap();
            if cursor == "0" {
                break;
            }
        }
        assert_eq!(seen.len(), 1000);
        assert!(pages >= 1000 / 37);
    }

    #[test]
    fn scan_is_stable_under_deletion_and_insertion() {
        let (f, mut s) = fresh();
        for i in 0..100 {
            run(&f, &mut s, &["HSET", "h", &format!("f{i}"), "v"]);
        }
        let Value::Array(parts) = run(&f, &mut s, &["HSCAN", "h", "0", "COUNT", "10"]) else {
            panic!()
        };
        let Value::Bulk(next) = &parts[0] else { panic!() };
        let cursor = String::from_utf8(next.to_vec()).unwrap();
        // Delete everything seen so far and add a new field mid-scan.
        for i in 0..10 {
            run(&f, &mut s, &["HDEL", "h", &format!("f{i}")]);
        }
        run(&f, &mut s, &["HSET", "h", "late", "v"]);
        let mut total = 0;
        let mut cursor = cursor;
        loop {
            let Value::Array(parts) = run(&f, &mut s, &["HSCAN", "h", &cursor, "COUNT", "10"]) else {
                panic!()
            };
            let Value::Bulk(next) = &parts[0] else { panic!() };
            let Value::Array(flat) = &parts[1] else { panic!() };
            total += flat.len() / 2;
            cursor = String::from_utf8(next.to_vec()).unwrap();
            if cursor == "0" {
                break;
            }
        }
        assert_eq!(total, 91, "the 90 untouched fields plus the late one");
    }

    #[test]
    fn del_limit_and_auth() {
        let (f, mut s) = fresh();
        let mut cmd = vec!["DEL"];
        let keys: Vec<String> = (0..101).map(|i| format!("k{i}")).collect();
        cmd.extend(keys.iter().map(String::as_str));
        assert!(matches!(run(&f, &mut s, &cmd), Value::Error(_)));
        cmd.truncate(101);
        assert_eq!(run(&f, &mut s, &cmd), Value::Integer(0));

        let f = FakeRedis::with_password(None, "pw");
        let mut s = Session { authed: false };
        assert!(matches!(run(&f, &mut s, &["GET", "a"]), Value::Error(m) if m.starts_with("NOAUTH")));
        assert!(matches!(run(&f, &mut s, &["AUTH", "bad"]), Value::Error(m) if m.starts_with("WRONGPASS")));
        assert_eq!(run(&f, &mut s, &["AUTH", "pw"]), Value::Simple("OK".into()));
        assert_eq!(run(&f, &mut s, &["GET", "a"]), Value::Null);
    }
}
