//! Client behaviour that the generic conformance suite does not cover:
//! limits, chunking, pooling, failure mapping.

mod common;

use moonpool_core::TokioProviders;
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use streaming_core::{Kv, KvError};
use streaming_kv_redis::{FakeRedis, RedisConfig, RedisKv};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn cfg(addr: &str) -> RedisConfig {
    RedisConfig {
        addr: addr.to_owned(),
        command_timeout: Duration::from_millis(500),
        connect_timeout: Duration::from_millis(500),
        ..RedisConfig::default()
    }
}

#[tokio::test]
async fn hset_hdel_del_are_chunked_only_above_the_threshold() {
    let fake = FakeRedis::new();
    let (addr, _t) = common::start_fake(fake.clone()).await;
    let kv = RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            scan_count: 50,
            ..cfg(&addr)
        },
    );
    let entries = |n: usize| -> Vec<(String, Vec<u8>)> { (0..n).map(|i| (format!("f{i}"), b"v".to_vec())).collect() };

    // Exactly the default threshold (1000 fields): one single command.
    kv.hset("h1", entries(1000)).await.unwrap();
    assert_eq!(fake.command_count("HSET"), 1);
    // Above: split.
    kv.hset("h2", entries(2500)).await.unwrap();
    assert_eq!(fake.command_count("HSET"), 1 + 3);

    // hgetall pages with HSCAN COUNT 50: at least 20 pages for 1000 fields.
    let all = kv.hgetall("h1").await.unwrap();
    assert_eq!(all.len(), 1000);
    assert!(fake.command_count("HSCAN") >= 20);

    kv.hdel("h2", entries(2500).into_iter().map(|(f, _)| f).collect())
        .await
        .unwrap();
    assert_eq!(fake.command_count("HDEL"), 3);
    assert_eq!(fake.key_count(), 1, "h2 vanished once emptied");

    // DEL: 100 keys at most per command (the fake refuses more, like Materia).
    let keys: Vec<String> = (0..100).map(|i| format!("k{i}")).collect();
    kv.del(keys).await.unwrap();
    assert_eq!(fake.command_count("DEL"), 1);
    let keys: Vec<String> = (0..101).map(|i| format!("k{i}")).collect();
    kv.del(keys).await.unwrap();
    assert_eq!(fake.command_count("DEL"), 3);
    kv.del(Vec::new()).await.unwrap();
    assert_eq!(fake.command_count("DEL"), 3, "empty del sends nothing");
}

#[tokio::test]
async fn hset_is_split_above_eight_mebibytes() {
    let fake = FakeRedis::new();
    let (addr, _t) = common::start_fake(fake.clone()).await;
    let kv = RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            command_timeout: Duration::from_secs(10),
            ..cfg(&addr)
        },
    );
    const MIB: usize = 1024 * 1024;
    // Field name plus value: exactly 8 MiB in two entries, one command.
    let half = || vec![7u8; 4 * MIB - 1];
    kv.hset("h1", vec![("a".into(), half()), ("b".into(), half())])
        .await
        .unwrap();
    assert_eq!(fake.command_count("HSET"), 1);
    // One byte more, and the second entry goes into a command of its own.
    let mut more = half();
    more.push(7);
    kv.hset("h2", vec![("a".into(), half()), ("b".into(), more)])
        .await
        .unwrap();
    assert_eq!(fake.command_count("HSET"), 3);
    // Three 3 MiB entries: two fit in the first command, one in the next.
    let third = || vec![1u8; 3 * MIB];
    kv.hset(
        "h3",
        vec![("a".into(), third()), ("b".into(), third()), ("c".into(), third())],
    )
    .await
    .unwrap();
    assert_eq!(fake.command_count("HSET"), 5);
    for (key, field, len) in [("h1", "b", 4 * MIB - 1), ("h2", "b", 4 * MIB), ("h3", "c", 3 * MIB)] {
        assert_eq!(fake.peek_field(key, field).map(|v| v.len()), Some(len));
    }
    // Keys of exactly the limit are accepted.
    assert_eq!(kv.get(&"k".repeat(8192)).await, Ok(None));
}

#[tokio::test]
async fn too_large_values_are_refused_before_sending() {
    let fake = FakeRedis::new();
    let (addr, _t) = common::start_fake(fake.clone()).await;
    let base = fake.connections_accepted();
    let kv = RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            max_value_bytes: 1024,
            ..cfg(&addr)
        },
    );
    let big = vec![0u8; 1025];
    let expect = KvError::TooLarge {
        size: 1025,
        limit: 1024,
    };
    assert_eq!(kv.set("k", big.clone()).await, Err(expect.clone()));
    assert_eq!(kv.hsetnx("h", "f", big.clone()).await, Err(expect.clone()));
    assert_eq!(
        kv.hset("h", vec![("a".into(), b"ok".to_vec()), ("b".into(), big)])
            .await,
        Err(expect)
    );
    assert!(matches!(
        kv.get(&"k".repeat(9000)).await,
        Err(KvError::TooLarge {
            size: 9000,
            limit: 8192
        })
    ));
    assert_eq!(
        fake.command_count("SET") + fake.command_count("HSET") + fake.command_count("HSETNX"),
        0
    );
    assert_eq!(fake.connections_accepted(), base, "nothing was even connected");
    // Exactly at the limit is fine.
    kv.set("k", vec![1u8; 1024]).await.unwrap();
}

#[tokio::test]
async fn protocol_errors_keep_the_connection() {
    let fake = FakeRedis::new();
    let (addr, _t) = common::start_fake(fake.clone()).await;
    let base = fake.connections_accepted();
    let kv = RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            connections: 1,
            ..cfg(&addr)
        },
    );
    kv.set("s", b"x".to_vec()).await.unwrap();
    for _ in 0..5 {
        assert!(matches!(kv.hget("s", "f").await, Err(KvError::Protocol(_))));
        assert_eq!(kv.get("s").await.unwrap(), Some(b"x".to_vec()));
    }
    assert_eq!(
        fake.connections_accepted() - base,
        1,
        "error replies must not reset the connection"
    );
}

#[tokio::test]
async fn concurrent_commands_share_a_bounded_pool() {
    let fake = FakeRedis::new();
    let (addr, _t) = common::start_fake(fake.clone()).await;
    let base = fake.connections_accepted();
    let kv = Arc::new(RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            connections: 3,
            ..cfg(&addr)
        },
    ));
    let mut tasks = Vec::new();
    for _ in 0..40 {
        let kv = kv.clone();
        tasks.push(tokio::spawn(async move { kv.incr("counter").await.unwrap() }));
    }
    let mut seen = HashSet::new();
    for t in tasks {
        assert!(seen.insert(t.await.unwrap()), "incr returned a value twice");
    }
    assert_eq!(seen.len(), 40);
    assert_eq!(kv.get("counter").await.unwrap(), Some(b"40".to_vec()));
    assert!(
        fake.connections_accepted() - base <= 3,
        "{} connections",
        fake.connections_accepted() - base
    );
}

#[tokio::test]
async fn connection_refused_is_unavailable() {
    let kv = RedisKv::new(
        TokioProviders::new(),
        cfg(&format!("127.0.0.1:{}", common::free_port())),
    );
    assert!(matches!(kv.get("k").await, Err(KvError::Unavailable(_))));
    assert!(matches!(kv.ping().await, Err(KvError::Unavailable(_))));
}

#[tokio::test]
async fn a_silent_server_times_out_and_the_pool_recovers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = accepted.clone();
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            held.push(sock); // accept, read nothing, say nothing
        }
    });
    let kv = RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            connections: 1,
            command_timeout: Duration::from_millis(200),
            ..cfg(&addr)
        },
    );
    for _ in 0..3 {
        let started = Instant::now();
        let r = kv.incr("c").await;
        assert!(
            matches!(r, Err(KvError::Unavailable(ref m)) if m.contains("timed out")),
            "{r:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }
    // The timed-out connection is dropped (never reused), the permit is
    // released: each attempt opened a fresh connection.
    assert_eq!(accepted.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn a_garbage_reply_drops_the_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = accepted.clone();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut buf = [0u8; 256];
                while let Ok(n) = sock.read(&mut buf).await {
                    if n == 0 || sock.write_all(b"this is not RESP\r\n").await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    let kv = RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            connections: 1,
            ..cfg(&addr)
        },
    );
    for _ in 0..3 {
        assert!(matches!(kv.get("k").await, Err(KvError::Unavailable(_))));
    }
    assert_eq!(accepted.load(Ordering::SeqCst), 3);
    // A well-formed reply of the wrong type is a protocol error, also dropped.
}

#[tokio::test]
async fn a_server_closing_idle_connections_is_not_an_error() {
    // Answers exactly one PING per connection, then closes it.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0u8; 256];
                if sock.read(&mut buf).await.is_ok_and(|n| n > 0) {
                    let _ = sock.write_all(b"+PONG\r\n").await;
                }
            });
        }
    });
    let kv = RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            connections: 1,
            ..cfg(&addr)
        },
    );
    for _ in 0..4 {
        kv.ping()
            .await
            .expect("stale idle connection must be replaced, not surfaced");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(not(feature = "tls"))]
#[tokio::test]
async fn tls_without_the_feature_is_a_clear_configuration_error() {
    let kv = RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            tls: true,
            ..cfg("127.0.0.1:1")
        },
    );
    let err = kv.get("k").await.unwrap_err();
    assert!(
        matches!(&err, KvError::Protocol(m) if m.contains("`tls` cargo feature")),
        "{err:?}"
    );
    assert!(RedisConfig::from_url("rediss://:t@h:1").is_err());
}

/// A server answering each command with `reply(name, nth call of that name)`.
async fn scripted(reply: fn(&str, usize) -> Vec<u8>) -> String {
    use std::collections::HashMap;
    use streaming_kv_redis::resp::{Decoder, Value};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let calls = Arc::new(parking_lot::Mutex::new(HashMap::<String, usize>::new()));
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let calls = calls.clone();
            tokio::spawn(async move {
                let mut decoder = Decoder::new();
                let mut buf = [0u8; 4096];
                while let Ok(n) = sock.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                    decoder.feed(&buf[..n]);
                    while let Ok(Some(Value::Array(args))) = decoder.next_value() {
                        let Some(Value::Bulk(name)) = args.first() else {
                            return;
                        };
                        let name = String::from_utf8_lossy(name).to_uppercase();
                        let nth = {
                            let mut calls = calls.lock();
                            let n = calls.entry(name.clone()).or_default();
                            *n += 1;
                            *n
                        };
                        if sock.write_all(&reply(&name, nth)).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    });
    addr
}

#[tokio::test]
async fn unexpected_simple_replies_are_protocol_errors() {
    let addr = scripted(|name, _| match name {
        "PING" => b"+PANG\r\n".to_vec(),
        "SET" => b"+QUEUED\r\n".to_vec(),
        _ => b":1\r\n".to_vec(),
    })
    .await;
    let kv = RedisKv::new(TokioProviders::new(), cfg(&addr));
    assert!(matches!(kv.ping().await, Err(KvError::Protocol(_))));
    assert!(matches!(kv.set("k", b"v".to_vec()).await, Err(KvError::Protocol(_))));
    // DEL answers an integer, so SET must not take one for its +OK.
    let addr = scripted(|_, _| b":1\r\n".to_vec()).await;
    let kv = RedisKv::new(TokioProviders::new(), cfg(&addr));
    assert!(matches!(kv.set("k", b"v".to_vec()).await, Err(KvError::Protocol(_))));
}

#[tokio::test]
async fn auth_must_answer_ok() {
    let addr = scripted(|name, _| match name {
        "AUTH" => b"+NOPE\r\n".to_vec(),
        _ => b"+PONG\r\n".to_vec(),
    })
    .await;
    let kv = RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            password: Some("tok".into()),
            ..cfg(&addr)
        },
    );
    let err = kv.ping().await.unwrap_err();
    assert!(matches!(&err, KvError::Protocol(m) if m.contains("AUTH")), "{err}");
}

#[tokio::test]
async fn malformed_hscan_cursors_are_refused() {
    // The first page carries `cursor`, the second ends the scan.
    fn reply(cursor: &str, nth: usize) -> Vec<u8> {
        let cursor = if nth == 1 { cursor } else { "0" };
        format!("*2\r\n${}\r\n{cursor}\r\n*0\r\n", cursor.len()).into_bytes()
    }
    for addr in [
        scripted(|_, nth| reply("", nth)).await,
        scripted(|_, nth| reply("123456789012345678901", nth)).await,
        scripted(|_, nth| reply("12a", nth)).await,
    ] {
        let kv = RedisKv::new(TokioProviders::new(), cfg(&addr));
        let got = tokio::time::timeout(Duration::from_secs(5), kv.hgetall("h"))
            .await
            .expect("no endless scan");
        assert!(matches!(got, Err(KvError::Protocol(_))), "{got:?}");
    }
    // Twenty digits is a valid cursor.
    let addr = scripted(|_, nth| reply("12345678901234567890", nth)).await;
    let kv = RedisKv::new(TokioProviders::new(), cfg(&addr));
    assert_eq!(kv.hgetall("h").await, Ok(Vec::new()));
}

#[tokio::test]
async fn hset_honours_the_field_limit_exactly() {
    let fake = FakeRedis::new();
    let (addr, _t) = common::start_fake(fake.clone()).await;
    let kv = RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            max_fields_per_command: 2,
            ..cfg(&addr)
        },
    );
    let entries = |n: usize| -> Vec<(String, Vec<u8>)> { (0..n).map(|i| (format!("f{i}"), b"v".to_vec())).collect() };
    kv.hset("h", entries(2)).await.unwrap();
    assert_eq!(fake.command_count("HSET"), 1);
    kv.hset("h", entries(3)).await.unwrap();
    assert_eq!(fake.command_count("HSET"), 3);
}

#[tokio::test]
async fn a_single_entry_above_eight_mebibytes_is_sent_alone() {
    const MIB: usize = 1024 * 1024;
    let fake = FakeRedis::with_max_value_bytes(16 * MIB);
    let (addr, _t) = common::start_fake(fake.clone()).await;
    let kv = RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            max_value_bytes: 16 * MIB,
            command_timeout: Duration::from_secs(10),
            ..cfg(&addr)
        },
    );
    let set = kv.hset(
        "h",
        vec![("a".into(), vec![1; 9 * MIB]), ("b".into(), vec![2; 9 * MIB])],
    );
    tokio::time::timeout(Duration::from_secs(30), set)
        .await
        .expect("no endless split")
        .unwrap();
    assert_eq!(fake.command_count("HSET"), 2);
    assert_eq!(fake.peek_field("h", "b").map(|v| v.len()), Some(9 * MIB));
}

#[test]
fn debug_shows_the_configuration_only() {
    let kv = RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            password: Some("topsecret".into()),
            ..RedisConfig::default()
        },
    );
    let shown = format!("{kv:?}");
    assert!(shown.starts_with("RedisKv { config: RedisConfig {"), "{shown}");
    assert!(!shown.contains("topsecret"), "{shown}");
}
