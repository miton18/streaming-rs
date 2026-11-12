//! Smoke test: a real listener and a real HTTP client.

use std::sync::Arc;
use streaming_api::{ApiConfig, ApiState, serve};
use streaming_core::{Clock, MemKv, SystemClock};

#[tokio::test]
async fn serves_over_tcp_and_shuts_down() {
    let kv = MemKv::shared();
    let state = ApiState {
        kv,
        clock: Arc::new(SystemClock) as Arc<dyn Clock>,
        kv_stats: None,
        config: ApiConfig::default(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, state, async move {
        let _ = stopped.await;
    }));

    let client = reqwest::Client::new();
    let health = client.get(format!("http://{addr}/healthz")).send().await.unwrap();
    assert_eq!(health.status(), 200);
    assert_eq!(health.text().await.unwrap(), "ok");

    let cluster: serde_json::Value = client
        .get(format!("http://{addr}/api/v1/cluster"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cluster["epoch"], 0);
    assert_eq!(cluster["workers"], serde_json::json!([]));
    assert_eq!(cluster["kv"], serde_json::Value::Null);
    assert!(cluster["nowMs"].as_i64().unwrap() > 1_600_000_000_000);

    let index = client.get(format!("http://{addr}/")).send().await.unwrap();
    assert_eq!(index.status(), 200);
    assert!(
        index.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/html")
    );

    stop.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .expect("graceful shutdown")
        .unwrap()
        .unwrap();
}
