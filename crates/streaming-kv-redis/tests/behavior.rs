//! Client behaviour that the generic conformance suite does not cover:
//! limits, chunking, pooling, failure mapping.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use moonpool_core::TokioProviders;
use streaming_core::{Kv, KvError};
use streaming_kv_redis::{FakeRedis, RedisConfig, RedisKv};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

async fn start_fake(fake: FakeRedis) -> (String, CancellationToken) {
    let addr = format!("127.0.0.1:{}", common::free_port());
    let token = CancellationToken::new();
    let (a, t) = (addr.clone(), token.clone());
    tokio::spawn(async move {
        fake.serve(TokioProviders::new(), &a, t)
            .await
            .expect("serve");
    });
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            // Let the readiness probe's own connection be counted and closed.
            tokio::time::sleep(Duration::from_millis(50)).await;
            return (addr, token);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("fake did not start");
}

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
    let (addr, _t) = start_fake(fake.clone()).await;
    let kv = RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            scan_count: 50,
            ..cfg(&addr)
        },
    );
    let entries = |n: usize| -> Vec<(String, Vec<u8>)> {
        (0..n).map(|i| (format!("f{i}"), b"v".to_vec())).collect()
    };

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
async fn too_large_values_are_refused_before_sending() {
    let fake = FakeRedis::new();
    let (addr, _t) = start_fake(fake.clone()).await;
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
    assert_eq!(
        fake.connections_accepted(),
        base,
        "nothing was even connected"
    );
    // Exactly at the limit is fine.
    kv.set("k", vec![1u8; 1024]).await.unwrap();
}

#[tokio::test]
async fn protocol_errors_keep_the_connection() {
    let fake = FakeRedis::new();
    let (addr, _t) = start_fake(fake.clone()).await;
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
    let (addr, _t) = start_fake(fake.clone()).await;
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
        tasks.push(tokio::spawn(
            async move { kv.incr("counter").await.unwrap() },
        ));
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
