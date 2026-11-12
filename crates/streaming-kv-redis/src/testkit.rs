//! Reusable conformance suite for [`Kv`] implementations.
//!
//! [`conformance`] exercises every trait method and the semantics documented
//! in `streaming_core::kv`. It is run against `MemKv`, against `RedisKv` over
//! a real Redis server and over [`crate::FakeRedis`], and can be pointed at
//! any other implementation. It uses keys under the `conf:` prefix and
//! removes them at the end.

use std::collections::BTreeMap;
use streaming_core::{Kv, KvError};

macro_rules! ensure {
    ($cond:expr, $($msg:tt)+) => {
        if !$cond {
            return Err(format!($($msg)+));
        }
    };
}

fn is_protocol<T: std::fmt::Debug>(what: &str, r: Result<T, KvError>) -> Result<(), String> {
    match r {
        Err(KvError::Protocol(_)) => Ok(()),
        other => Err(format!("{what}: expected a protocol error, got {other:?}")),
    }
}

fn ok<T>(what: &str, r: Result<T, KvError>) -> Result<T, String> { r.map_err(|e| format!("{what}: {e}")) }

fn key(name: &str) -> String { format!("conf:{name}") }

/// Run the whole suite. `Err` describes the first violated expectation.
///
/// # Errors
/// A description of the first expectation the store violated.
pub async fn conformance(kv: &dyn Kv) -> Result<(), String> {
    cleanup(kv).await?;
    let result = run(kv).await;
    let cleaned = cleanup(kv).await;
    result.and(cleaned)
}

/// `INCR` at `i64::MAX` must be a protocol error (Redis semantics), never a
/// wrap-around or a panic. Separate from [`conformance`] because
/// `streaming_core::MemKv` currently overflows (panics in debug builds).
///
/// # Errors
/// A description of the violated expectation.
pub async fn conformance_incr_overflow(kv: &dyn Kv) -> Result<(), String> {
    let n = key("n");
    ok("set max", kv.set(&n, i64::MAX.to_string().into_bytes()).await)?;
    let r = is_protocol("incr overflow", kv.incr(&n).await);
    ok("cleanup", kv.del(vec![n]).await)?;
    r
}

async fn cleanup(kv: &dyn Kv) -> Result<(), String> {
    let names = [
        "s", "bin", "empty", "big", "n", "h", "hb", "many", "wt-str", "wt-hash", "gone", "nx", "dup", "noop",
    ];
    let mut keys: Vec<String> = names.iter().map(|n| key(n)).collect();
    keys.extend((0..250).map(|i| key(&format!("bulk{i}"))));
    ok("cleanup del", kv.del(keys).await)
}

#[allow(clippy::too_many_lines)]
async fn run(kv: &dyn Kv) -> Result<(), String> {
    // ---- strings ---------------------------------------------------------
    let s = key("s");
    ensure!(
        ok("get missing", kv.get(&s).await)?.is_none(),
        "missing key must be None"
    );
    ok("set", kv.set(&s, b"hello".to_vec()).await)?;
    ensure!(ok("get", kv.get(&s).await)? == Some(b"hello".to_vec()), "get after set");
    ok("overwrite", kv.set(&s, b"world".to_vec()).await)?;
    ensure!(
        ok("get", kv.get(&s).await)? == Some(b"world".to_vec()),
        "set overwrites"
    );

    // Binary safety: every byte value, CRLF, NUL, RESP look-alikes.
    let mut bin: Vec<u8> = (0..=255u8).collect();
    bin.extend_from_slice(b"\r\n$5\r\nabcde\r\n*1\r\n+OK\r\n\0\0\r\n");
    ok("set bin", kv.set(&key("bin"), bin.clone()).await)?;
    ensure!(
        ok("get bin", kv.get(&key("bin")).await)? == Some(bin.clone()),
        "binary value (CRLF, NUL, all bytes) must round trip"
    );
    ok("set empty", kv.set(&key("empty"), Vec::new()).await)?;
    ensure!(
        ok("get empty", kv.get(&key("empty")).await)? == Some(Vec::new()),
        "an empty value is Some(empty), not None"
    );
    let big: Vec<u8> = (0..1_500_000u32).map(|i| (i % 251) as u8).collect();
    ok("set big", kv.set(&key("big"), big.clone()).await)?;
    ensure!(
        ok("get big", kv.get(&key("big")).await)? == Some(big),
        "1.5 MB value must round trip"
    );

    // ---- del -------------------------------------------------------------
    ok("del nothing", kv.del(Vec::new()).await)?;
    ok("del missing", kv.del(vec![key("never-existed")]).await)?;
    ok(
        "del mixed",
        kv.del(vec![s.clone(), key("never-existed"), key("bin")]).await,
    )?;
    ensure!(ok("get deleted", kv.get(&s).await)?.is_none(), "del removes the key");
    ensure!(
        ok("get deleted", kv.get(&key("bin")).await)?.is_none(),
        "del removes every key"
    );
    // More keys than one Materia DEL accepts: the client must chunk.
    let many: Vec<String> = (0..250).map(|i| key(&format!("bulk{i}"))).collect();
    for k in &many {
        ok("set bulk", kv.set(k, b"x".to_vec()).await)?;
    }
    ok("del 250", kv.del(many.clone()).await)?;
    for k in &many {
        ensure!(
            ok("get bulk", kv.get(k).await)?.is_none(),
            "{k} must be deleted by a 250 keys del"
        );
    }

    // ---- incr ------------------------------------------------------------
    let n = key("n");
    ensure!(ok("incr 1", kv.incr(&n).await)? == 1, "incr of a missing key is 1");
    ensure!(ok("incr 2", kv.incr(&n).await)? == 2, "incr again is 2");
    ensure!(
        ok("get counter", kv.get(&n).await)? == Some(b"2".to_vec()),
        "counter is stored as decimal text"
    );
    ok("set 41", kv.set(&n, b"41".to_vec()).await)?;
    ensure!(
        ok("incr 42", kv.incr(&n).await)? == 42,
        "incr continues from a string integer"
    );
    ok("set neg", kv.set(&n, b"-5".to_vec()).await)?;
    ensure!(ok("incr neg", kv.incr(&n).await)? == -4, "incr of a negative number");
    ok("set text", kv.set(&n, b"abc".to_vec()).await)?;
    is_protocol("incr of text", kv.incr(&n).await)?;
    ensure!(
        ok("still usable", kv.get(&n).await)? == Some(b"abc".to_vec()),
        "a failed incr leaves the value"
    );

    // ---- hsetnx: the compare-and-set --------------------------------------
    let nx = key("nx");
    ensure!(
        ok("hsetnx new", kv.hsetnx(&nx, "c:1", b"first".to_vec()).await)?,
        "hsetnx creating a field returns true"
    );
    ensure!(
        !ok("hsetnx dup", kv.hsetnx(&nx, "c:1", b"second".to_vec()).await)?,
        "hsetnx on an existing field returns false"
    );
    ensure!(
        ok("hget", kv.hget(&nx, "c:1").await)? == Some(b"first".to_vec()),
        "a refused hsetnx leaves the value untouched"
    );
    ensure!(
        ok("hsetnx other field", kv.hsetnx(&nx, "c:2", b"v".to_vec()).await)?,
        "another field is created"
    );
    ensure!(
        ok("hsetnx empty value", kv.hsetnx(&nx, "e", Vec::new()).await)?,
        "empty value is created"
    );
    ensure!(
        !ok("hsetnx empty exists", kv.hsetnx(&nx, "e", b"x".to_vec()).await)?,
        "an empty value still counts as existing"
    );
    ok(
        "hset overwrites",
        kv.hset(&nx, vec![("c:1".into(), b"third".to_vec())]).await,
    )?;
    ensure!(
        !ok("hsetnx after hset", kv.hsetnx(&nx, "c:1", b"fourth".to_vec()).await)?,
        "hsetnx still false after hset"
    );
    ok("hdel field", kv.hdel(&nx, vec!["c:1".into()]).await)?;
    ensure!(
        ok("hsetnx after hdel", kv.hsetnx(&nx, "c:1", b"fifth".to_vec()).await)?,
        "hsetnx true again once the field is deleted"
    );

    // ---- hset / hget / hgetall / hdel --------------------------------------
    let h = key("h");
    ensure!(
        ok("hget missing hash", kv.hget(&h, "f").await)?.is_none(),
        "hget on a missing key is None"
    );
    ensure!(
        ok("hgetall missing", kv.hgetall(&h).await)?.is_empty(),
        "hgetall on a missing key is empty"
    );
    ok("hdel missing hash", kv.hdel(&h, vec!["f".into()]).await)?;
    ok("hset empty", kv.hset(&key("noop"), Vec::new()).await)?;
    ensure!(
        ok("get after empty hset", kv.get(&key("noop")).await)?.is_none(),
        "hset with no entries must not create a key"
    );
    ok(
        "hset",
        kv.hset(&h, vec![("a".into(), b"1".to_vec()), ("b".into(), b"2".to_vec())])
            .await,
    )?;
    ok(
        "hset overwrite",
        kv.hset(&h, vec![("a".into(), b"one".to_vec()), ("c".into(), b"3".to_vec())])
            .await,
    )?;
    ensure!(
        ok("hget a", kv.hget(&h, "a").await)? == Some(b"one".to_vec()),
        "hset overwrites existing fields"
    );
    ensure!(
        ok("hget nope", kv.hget(&h, "nope").await)?.is_none(),
        "missing field is None"
    );
    let mut all = ok("hgetall", kv.hgetall(&h).await)?;
    all.sort();
    ensure!(
        all == vec![
            ("a".to_owned(), b"one".to_vec()),
            ("b".to_owned(), b"2".to_vec()),
            ("c".to_owned(), b"3".to_vec())
        ],
        "hgetall content: {all:?}"
    );
    // The same field twice in one call: the last value wins.
    ok(
        "hset dup",
        kv.hset(
            &key("dup"),
            vec![("f".into(), b"1".to_vec()), ("f".into(), b"2".to_vec())],
        )
        .await,
    )?;
    ensure!(
        ok("hget dup", kv.hget(&key("dup"), "f").await)? == Some(b"2".to_vec()),
        "last duplicate wins"
    );

    // Binary values and awkward field names.
    let hb = key("hb");
    let weird: Vec<(String, Vec<u8>)> = vec![
        ("with space".into(), b"a\r\nb".to_vec()),
        ("crlf\r\nfield".into(), vec![0, 0, 0]),
        ("nul\0field".into(), (0..=255u8).collect()),
        ("héllo wörld ✓".into(), Vec::new()),
        (String::new(), b"empty field name".to_vec()),
    ];
    ok("hset weird", kv.hset(&hb, weird.clone()).await)?;
    let mut got = ok("hgetall weird", kv.hgetall(&hb).await)?;
    got.sort();
    let mut want = weird.clone();
    want.sort();
    ensure!(got == want, "binary values and odd field names must round trip");
    for (f, v) in &weird {
        ensure!(
            ok("hget weird", kv.hget(&hb, f).await)?.as_ref() == Some(v),
            "hget of {f:?}"
        );
    }

    // A hash losing its last field disappears (so the key is free again).
    ok("hdel some", kv.hdel(&h, vec!["a".into(), "zzz".into()]).await)?;
    ensure!(
        ok("hgetall after hdel", kv.hgetall(&h).await)?.len() == 2,
        "hdel removes only named fields"
    );
    ok("hdel rest", kv.hdel(&h, vec!["b".into(), "c".into()]).await)?;
    ensure!(
        ok("get after the hash is gone", kv.get(&h).await)?.is_none(),
        "a hash that lost its last field must disappear (get must not be a wrong-type error)"
    );
    ensure!(
        ok("incr on vanished hash", kv.incr(&h).await)? == 1,
        "key reusable as a string"
    );
    ok("hdel nothing", kv.hdel(&nx, Vec::new()).await)?;

    // ---- big hash: HSCAN paging, chunked HSET/HDEL ---------------------------
    let many = key("many");
    let total = 5000usize;
    let entries: Vec<(String, Vec<u8>)> = (0..total)
        .map(|i| (format!("field-{i:05}"), format!("value-{i}\r\n").into_bytes()))
        .collect();
    ok("hset 5000", kv.hset(&many, entries.clone()).await)?;
    let all = ok("hgetall 5000", kv.hgetall(&many).await)?;
    ensure!(all.len() == total, "hgetall returned {} of {total} fields", all.len());
    let map: BTreeMap<_, _> = all.into_iter().collect();
    ensure!(map.len() == total, "hgetall returned duplicate fields");
    for (f, v) in &entries {
        ensure!(map.get(f) == Some(v), "wrong value for {f}");
    }
    let (gone, kept) = entries.split_at(4990);
    ok(
        "hdel 4990",
        kv.hdel(&many, gone.iter().map(|(f, _)| f.clone()).collect()).await,
    )?;
    ensure!(
        ok("hgetall rest", kv.hgetall(&many).await)?.len() == 10,
        "chunked hdel must remove everything asked"
    );
    ok(
        "hdel last",
        kv.hdel(&many, kept.iter().map(|(f, _)| f.clone()).collect()).await,
    )?;
    ensure!(
        ok("get emptied", kv.get(&many).await)?.is_none(),
        "emptied big hash disappears"
    );

    // ---- wrong type ----------------------------------------------------------
    let ws = key("wt-str");
    let wh = key("wt-hash");
    ok("set", kv.set(&ws, b"x".to_vec()).await)?;
    ok("hset", kv.hset(&wh, vec![("f".into(), b"v".to_vec())]).await)?;
    is_protocol("hget on string", kv.hget(&ws, "f").await)?;
    is_protocol("hgetall on string", kv.hgetall(&ws).await)?;
    is_protocol("hset on string", kv.hset(&ws, vec![("f".into(), b"v".to_vec())]).await)?;
    is_protocol("hsetnx on string", kv.hsetnx(&ws, "f", b"v".to_vec()).await)?;
    is_protocol("hdel on string", kv.hdel(&ws, vec!["f".into()]).await)?;
    is_protocol("get on hash", kv.get(&wh).await)?;
    is_protocol("incr on hash", kv.incr(&wh).await)?;
    // set replaces whatever the key held.
    ok("set over hash", kv.set(&wh, b"now a string".to_vec()).await)?;
    ensure!(
        ok("get", kv.get(&wh).await)? == Some(b"now a string".to_vec()),
        "set replaces a hash"
    );
    // The store is still healthy after the errors above.
    ensure!(
        ok("get after errors", kv.get(&ws).await)? == Some(b"x".to_vec()),
        "usable after protocol errors"
    );
    Ok(())
}
