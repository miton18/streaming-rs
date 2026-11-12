//! The conformance suite against `MemKv`, `FakeRedis` and a real redis-server.

mod common;

use moonpool_core::TokioProviders;
use std::sync::Arc;
use streaming_core::{Kv, KvError, MemKv};
use streaming_kv_redis::{
    FakeRedis, RedisConfig, RedisKv,
    testkit::{conformance, conformance_incr_overflow},
};
use tokio_util::sync::CancellationToken;

fn config(addr: &str) -> RedisConfig {
    RedisConfig {
        addr: addr.to_owned(),
        ..RedisConfig::default()
    }
}

/// Serve a `FakeRedis` on a free local port through the tokio providers.
async fn start_fake(fake: FakeRedis) -> (String, CancellationToken) {
    let addr = format!("127.0.0.1:{}", common::free_port());
    let token = CancellationToken::new();
    let (a, t) = (addr.clone(), token.clone());
    tokio::spawn(async move {
        fake.serve(TokioProviders::new(), &a, t).await.expect("serve");
    });
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return (addr, token);
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("fake did not start");
}

#[tokio::test]
async fn conformance_mem_kv() {
    conformance(&MemKv::new()).await.expect("MemKv conforms");
}

#[tokio::test]
async fn conformance_fake_redis() {
    let fake = FakeRedis::new();
    let (addr, token) = start_fake(fake.clone()).await;
    let kv = RedisKv::new(TokioProviders::new(), config(&addr));
    kv.ping().await.expect("ping");
    conformance(&kv).await.expect("RedisKv over FakeRedis conforms");
    conformance_incr_overflow(&kv).await.expect("incr overflow");
    // HSCAN really paged (5000 fields, COUNT 500) and DEL really chunked.
    assert!(
        fake.command_count("HSCAN") >= 10,
        "hscan calls: {}",
        fake.command_count("HSCAN")
    );
    assert_eq!(fake.command_count("HGETALL"), 0, "hgetall must page with HSCAN");
    token.cancel();
}

#[tokio::test]
async fn conformance_real_redis() {
    let Some(server) = common::start_redis(&[]).await else {
        return;
    };
    let kv = RedisKv::new(TokioProviders::new(), config(&server.addr));
    kv.ping().await.expect("ping");
    conformance(&kv).await.expect("RedisKv over real redis conforms");
    conformance_incr_overflow(&kv).await.expect("incr overflow");
}

#[tokio::test]
async fn conformance_real_redis_small_chunks() {
    let Some(server) = common::start_redis(&[]).await else {
        return;
    };
    let cfg = RedisConfig {
        scan_count: 7,
        max_fields_per_command: 13,
        max_keys_per_del: 5,
        connections: 1,
        ..config(&server.addr)
    };
    let kv = RedisKv::new(TokioProviders::new(), cfg);
    conformance(&kv).await.expect("conforms with tiny chunk sizes");
}

#[tokio::test]
async fn real_redis_with_password() {
    let Some(server) = common::start_redis(&["--requirepass", "s3cr3t"]).await else {
        return;
    };

    // Right password.
    let cfg = RedisConfig {
        password: Some("s3cr3t".into()),
        ..config(&server.addr)
    };
    let kv = RedisKv::new(TokioProviders::new(), cfg);
    kv.ping().await.expect("authenticated ping");
    conformance(&kv).await.expect("conforms behind AUTH");

    // Default user named explicitly.
    let cfg = RedisConfig {
        username: Some("default".into()),
        password: Some("s3cr3t".into()),
        ..config(&server.addr)
    };
    RedisKv::new(TokioProviders::new(), cfg)
        .ping()
        .await
        .expect("AUTH default pw");

    // Credentials from a URL, password-only form.
    let url = format!("redis://:s3cr3t@{}", server.addr);
    let kv = RedisKv::new(TokioProviders::new(), RedisConfig::from_url(&url).unwrap());
    kv.ping().await.expect("ping via url");

    // Wrong password: a protocol error, not "unavailable".
    let cfg = RedisConfig {
        password: Some("nope".into()),
        ..config(&server.addr)
    };
    let err = RedisKv::new(TokioProviders::new(), cfg).ping().await.unwrap_err();
    assert!(matches!(err, KvError::Protocol(ref m) if m.contains("AUTH")), "{err:?}");

    // No credentials at all: NOAUTH is a protocol error too.
    let err = RedisKv::new(TokioProviders::new(), config(&server.addr))
        .get("k")
        .await
        .unwrap_err();
    assert!(matches!(err, KvError::Protocol(_)), "{err:?}");
}

#[tokio::test]
async fn real_redis_hash_of_several_thousand_fields_pages() {
    let Some(server) = common::start_redis(&[]).await else {
        return;
    };
    let kv = Arc::new(RedisKv::new(
        TokioProviders::new(),
        RedisConfig {
            scan_count: 100,
            ..config(&server.addr)
        },
    ));
    let entries: Vec<(String, Vec<u8>)> = (0..7500)
        .map(|i| (format!("f{i}"), vec![(i % 256) as u8; 1 + i % 40]))
        .collect();
    kv.hset("big", entries.clone()).await.unwrap();
    let mut got = kv.hgetall("big").await.unwrap();
    got.sort();
    let mut want = entries;
    want.sort();
    assert_eq!(got, want);
}
