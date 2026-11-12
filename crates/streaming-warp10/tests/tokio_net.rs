//! `Warp10Writer<TokioProviders>` against the fake ingress over real localhost
//! TCP, plus a few hand-written misbehaving servers.

use moonpool_core::{NetworkProvider, Providers, TcpListenerTrait, TokioProviders};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use streaming_core::{
    Emit, EmitId, GraphId, NodeId, OpFactory, OpRef, Origin, Position, Registry, SinkConnector, SliceId, StaticSecrets,
    SystemClock, TenantId, datum,
};
use streaming_warp10::{
    Gts, GtsValue, Warp10Config, Warp10Writer,
    fake::{FakeWarp10, PointKey},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;

const TOKEN: &str = "write-token-123";

fn emit(n: u64, gts: Gts) -> Emit {
    let origin = Origin::source(Position(0, n, 0));
    let slice = SliceId::Source {
        node: NodeId::new("n").unwrap(),
        partition: 0,
    };
    Emit {
        id: EmitId::new(&slice, &origin),
        origin,
        ts: 0,
        key: None,
        value: datum(gts),
    }
}

fn batch(n: u64) -> Vec<Emit> {
    (0..n)
        .map(|i| {
            emit(
                i,
                Gts::new("test.metric")
                    .label("host", format!("h{}", i % 3))
                    .label("zone", "eu west")
                    .at(1_700_000_000_000 + i64::try_from(i).unwrap())
                    .value(i64::try_from(i).unwrap() * 10),
            )
        })
        .collect()
}

fn expected(n: u64) -> BTreeMap<PointKey, GtsValue> {
    (0..n)
        .map(|i| {
            (
                PointKey::new(
                    "test.metric",
                    [
                        ("host".to_owned(), format!("h{}", i % 3)),
                        ("zone".to_owned(), "eu west".to_owned()),
                    ],
                    (1_700_000_000_000 + i64::try_from(i).unwrap()) * 1000,
                ),
                GtsValue::Long(i64::try_from(i).unwrap() * 10),
            )
        })
        .collect()
}

struct Server {
    fake: FakeWarp10,
    port: u16,
    shutdown: CancellationToken,
}

impl Drop for Server {
    fn drop(&mut self) { self.shutdown.cancel(); }
}

async fn start() -> Server {
    let providers = TokioProviders::new();
    let fake = FakeWarp10::new(TOKEN);
    let listener = providers.network().bind("127.0.0.1:0").await.unwrap();
    let port: u16 = listener
        .local_addr()
        .unwrap()
        .rsplit(':')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let shutdown = CancellationToken::new();
    let (f, s) = (fake.clone(), shutdown.clone());
    tokio::spawn(async move {
        let _ = f.serve_listener(providers, listener, s).await;
    });
    Server { fake, port, shutdown }
}

fn sink_for(port: u16, token: &str, tweak: impl FnOnce(&mut Warp10Config)) -> Warp10Writer<TokioProviders> {
    let mut config = Warp10Config::new(&format!("http://127.0.0.1:{port}/api/v0/update"), token).unwrap();
    config.timeout = Duration::from_secs(5);
    tweak(&mut config);
    Warp10Writer::new(TokioProviders::new(), config).unwrap()
}

#[tokio::test]
async fn happy_path() {
    let server = start().await;
    let sink = sink_for(server.port, TOKEN, |_| {});
    sink.write(&batch(7)).await.unwrap();
    assert_eq!(server.fake.points(), expected(7));
    // empty batch: nothing sent
    let before = server.fake.stats().requests;
    sink.write(&[]).await.unwrap();
    assert_eq!(server.fake.stats().requests, before);
}

#[tokio::test]
async fn time_unit_is_applied() {
    let server = start().await;
    let sink = sink_for(server.port, TOKEN, |c| c.time_unit = streaming_warp10::TimeUnit::Ns);
    sink.write(&[emit(0, Gts::new("m").at(5).value(1.0))]).await.unwrap();
    assert_eq!(
        server
            .fake
            .get(&PointKey::new("m", Vec::<(String, String)>::new(), 5_000_000)),
        Some(GtsValue::Double(1.0))
    );
}

#[tokio::test]
async fn bad_token_is_a_401_that_does_not_leak_the_token() {
    let server = start().await;
    let sink = sink_for(server.port, "wrong-token-xyz", |_| {});
    let err = sink.write(&batch(2)).await.unwrap_err();
    assert!(err.0.contains("401"), "{err}");
    assert!(err.0.contains("invalid write token"), "{err}");
    assert!(!err.0.contains("wrong-token-xyz"), "{err}");
    assert!(server.fake.is_empty());
    assert_eq!(server.fake.stats().unauthorized, 1);
}

#[tokio::test]
async fn http_503_then_retry() {
    let server = start().await;
    server.fake.fail_next(2, 503);
    let sink = sink_for(server.port, TOKEN, |_| {});
    let b = batch(5);
    for _ in 0..2 {
        let err = sink.write(&b).await.unwrap_err();
        assert!(err.0.contains("503") && err.0.contains("injected failure"), "{err}");
        assert!(server.fake.is_empty());
    }
    sink.write(&b).await.unwrap();
    assert_eq!(server.fake.points(), expected(5));
}

#[tokio::test]
async fn connection_dropped_after_the_body_then_retry_is_idempotent() {
    let server = start().await;
    server.fake.drop_after_body_next(1);
    let sink = sink_for(server.port, TOKEN, |_| {});
    let b = batch(6);
    let err = sink.write(&b).await.unwrap_err();
    assert!(err.0.contains("reading the response"), "{err}");
    // The outcome was unknown, and in fact the points were stored already.
    assert_eq!(server.fake.points(), expected(6));
    sink.write(&b).await.unwrap();
    sink.write(&b).await.unwrap();
    assert_eq!(server.fake.points(), expected(6), "exactly the batch");
    assert_eq!(server.fake.stats().points_written, 18);
}

#[tokio::test]
async fn connection_dropped_before_commit_then_retry() {
    let server = start().await;
    server.fake.drop_before_commit_next(1);
    let sink = sink_for(server.port, TOKEN, |_| {});
    let b = batch(4);
    assert!(sink.write(&b).await.is_err());
    assert!(server.fake.is_empty());
    sink.write(&b).await.unwrap();
    assert_eq!(server.fake.points(), expected(4));
}

#[tokio::test]
async fn overwrite_semantics_last_write_wins() {
    let server = start().await;
    let sink = sink_for(server.port, TOKEN, |_| {});
    sink.write(&[emit(0, Gts::new("m").at(1).value(1))]).await.unwrap();
    sink.write(&[emit(0, Gts::new("m").at(1).value("now a string"))])
        .await
        .unwrap();
    assert_eq!(server.fake.len(), 1);
    assert_eq!(
        server
            .fake
            .get(&PointKey::new("m", Vec::<(String, String)>::new(), 1000)),
        Some(GtsValue::Text("now a string".into()))
    );
}

#[tokio::test]
async fn large_batches_are_split_into_several_requests() {
    let server = start().await;
    let sink = sink_for(server.port, TOKEN, |c| c.max_batch_bytes = 200);
    let b = batch(40);
    sink.write(&b).await.unwrap();
    let stats = server.fake.stats();
    assert!(stats.requests >= 10, "{stats:?}");
    assert_eq!(stats.accepted, stats.requests);
    assert_eq!(server.fake.points(), expected(40));

    // A failure in the middle fails the whole write; the retry completes it.
    let server = start().await;
    let sink = sink_for(server.port, TOKEN, |c| c.max_batch_bytes = 200);
    server.fake.fail_next(1, 500);
    assert!(sink.write(&b).await.is_err());
    sink.write(&b).await.unwrap();
    assert_eq!(server.fake.points(), expected(40));
}

#[tokio::test]
async fn elements_that_are_not_gts_are_refused_without_any_request() {
    let server = start().await;
    let sink = sink_for(server.port, TOKEN, |_| {});
    let mut b = batch(2);
    b.push(Emit {
        value: datum(42_u32),
        ..emit(9, Gts::new("x"))
    });
    let err = sink.write(&b).await.unwrap_err();
    assert!(err.0.contains("not a Gts"), "{err}");
    assert_eq!(server.fake.stats().requests, 0);
}

/// A server answering every connection with `reply(request_bytes_len)`.
async fn raw_server<F, Fut>(handler: F) -> u16
where
    F: Fn(tokio::net::TcpStream) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handler = Arc::new(handler);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let handler = handler.clone();
            tokio::spawn(async move { handler(stream).await });
        }
    });
    port
}

async fn read_request_head(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut buf = vec![0u8; 8192];
    let n = stream.read(&mut buf).await.unwrap_or(0);
    buf.truncate(n);
    buf
}

#[tokio::test]
async fn a_server_that_never_answers_times_out() {
    let port = raw_server(|mut stream| async move {
        let _ = read_request_head(&mut stream).await;
        tokio::time::sleep(Duration::from_secs(30)).await;
    })
    .await;
    let sink = sink_for(port, TOKEN, |c| c.timeout = Duration::from_millis(200));
    let started = std::time::Instant::now();
    let err = sink.write(&batch(1)).await.unwrap_err();
    assert!(err.0.contains("timed out"), "{err}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn connection_refused_is_an_error() {
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };
    let sink = sink_for(port, TOKEN, |_| {});
    let err = sink.write(&batch(1)).await.unwrap_err();
    assert!(err.0.contains("connect"), "{err}");
}

#[tokio::test]
async fn fragmented_200_and_chunked_error_bodies() {
    // 200 dribbled one byte at a time
    let port = raw_server(|mut stream| async move {
        let _ = read_request_head(&mut stream).await;
        for b in b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n" {
            stream.write_all(&[*b]).await.unwrap();
            stream.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    sink_for(port, TOKEN, |_| {}).write(&batch(1)).await.unwrap();

    // 500 with a chunked body, and a body that echoes the token
    let port = raw_server(|mut stream| async move {
        let _ = read_request_head(&mut stream).await;
        let body = format!("bad request for {TOKEN}\nline two");
        let chunked = format!(
            "HTTP/1.1 500 Internal Server Error\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
            body.len(),
            body
        );
        stream.write_all(chunked.as_bytes()).await.unwrap();
    })
    .await;
    let err = sink_for(port, TOKEN, |_| {}).write(&batch(1)).await.unwrap_err();
    assert!(err.0.contains("500") && err.0.contains("line two"), "{err}");
    assert!(!err.0.contains(TOKEN), "{err}");

    // 200 is the only success: 204 is an error
    let port = raw_server(|mut stream| async move {
        let _ = read_request_head(&mut stream).await;
        stream.write_all(b"HTTP/1.1 204 No Content\r\n\r\n").await.unwrap();
    })
    .await;
    let err = sink_for(port, TOKEN, |_| {}).write(&batch(1)).await.unwrap_err();
    assert!(err.0.contains("204"), "{err}");

    // garbage
    let port = raw_server(|mut stream| async move {
        let _ = read_request_head(&mut stream).await;
        stream.write_all(b"not http at all\r\n\r\n").await.unwrap();
    })
    .await;
    assert!(sink_for(port, TOKEN, |_| {}).write(&batch(1)).await.is_err());
}

#[tokio::test]
async fn request_carries_token_content_type_and_text_body() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let port = raw_server(move |mut stream| {
        let tx = tx.clone();
        async move {
            let head = read_request_head(&mut stream).await;
            tx.send(String::from_utf8_lossy(&head).into_owned()).unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
        }
    })
    .await;
    sink_for(port, TOKEN, |_| {})
        .write(&[emit(0, Gts::new("a b").label("k", "v").at(1).value(true))])
        .await
        .unwrap();
    let raw = rx.recv().await.unwrap();
    assert!(raw.starts_with("POST /api/v0/update HTTP/1.1\r\n"), "{raw}");
    assert!(raw.contains(&format!("Host: 127.0.0.1:{port}\r\n")), "{raw}");
    assert!(raw.contains(&format!("X-Warp10-Token: {TOKEN}\r\n")), "{raw}");
    assert!(raw.contains("Content-Type: text/plain\r\n"), "{raw}");
    assert!(raw.ends_with("\r\n\r\n1000// a%20b{k=v} T\n"), "{raw}");
}

#[tokio::test]
async fn registered_in_the_registry_and_builds_from_json() {
    let mut registry: Registry<TokioProviders> = Registry::empty();
    streaming_warp10::register(&mut registry);
    let op = OpRef::new("std/warp10-sink@1").unwrap();
    let (descriptor, factory) = registry.get(&op).expect("registered");
    assert_eq!(descriptor.input, streaming_core::TypeTag::of::<Gts>());
    let OpFactory::Sink(factory) = factory else {
        panic!("not a sink")
    };

    let server = start().await;
    let tenant = TenantId::new("acme").unwrap();
    let context = streaming_core::OpContext {
        providers: TokioProviders::new(),
        clock: Arc::new(SystemClock),
        secrets: Arc::new(StaticSecrets::new().with(&tenant, "warp-write", TOKEN)),
        kv: streaming_core::MemKv::shared(),
        shared: Arc::default(),
        tenant,
        graph: GraphId::new("g").unwrap(),
        node: NodeId::new("out").unwrap(),
    };
    let sink = factory(
        &context,
        &json!({
            "endpoint": format!("http://127.0.0.1:{}/api/v0/update", server.port),
            "token": {"secret": "warp-write"},
            "timeUnit": "us", "timeout": "5s", "maxBatchBytes": 4096
        }),
    )
    .expect("sink builds");
    sink.write(&batch(3)).await.unwrap();
    assert_eq!(server.fake.points(), expected(3));

    // A retried batch goes through the same sink, and leaves the same points.
    sink.write(&batch(3)).await.unwrap();
    assert_eq!(server.fake.points(), expected(3));

    // A configuration that cannot run is refused before anything starts.
    assert!(factory(&context, &json!({"endpoint": "http://h/"})).is_err());
    assert!(factory(&context, &json!({"endpoint": "ftp://h/", "token": "t"})).is_err());
    // So is one that names a secret its tenant does not have.
    let Err(error) = factory(&context, &json!({"endpoint": "http://h/", "token": {"secret": "nope"}})) else {
        panic!("a missing secret must be refused");
    };
    assert_eq!(
        error.to_string(),
        "invalid operator configuration: std/warp10-sink@1: secret `nope` is not defined for tenant `acme`"
    );
}
