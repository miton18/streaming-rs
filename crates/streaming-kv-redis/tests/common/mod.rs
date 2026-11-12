#![allow(dead_code)]
//! Helpers shared by the integration tests: free ports and a real redis-server.

use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    sync::Mutex,
    time::Duration,
};

/// A port nobody listens on and that no other test of this process got.
pub fn free_port() -> u16 {
    static HANDED_OUT: Mutex<Vec<u16>> = Mutex::new(Vec::new());
    loop {
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("bind port 0")
            .local_addr()
            .expect("local addr")
            .port();
        let mut taken = HANDED_OUT.lock().expect("lock");
        if !taken.contains(&port) {
            taken.push(port);
            return port;
        }
    }
}

/// A real `redis-server` child process, killed on drop.
pub struct RealRedis {
    child: Child,
    pub addr: String,
}

impl Drop for RealRedis {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn redis_server_available() -> bool {
    Command::new("redis-server")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Start redis-server on a free port (optionally `--requirepass`), wait until
/// it accepts connections. `None` (after printing a notice) when the binary
/// is not installed.
pub async fn start_redis(extra: &[&str]) -> Option<RealRedis> {
    if !redis_server_available() {
        eprintln!("NOTICE: redis-server binary not found, skipping real-redis test");
        return None;
    }
    let port = free_port();
    let child = Command::new("redis-server")
        .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
        .args(["--save", "", "--appendonly", "no"])
        .args(extra)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn redis-server");
    let addr = format!("127.0.0.1:{port}");
    let guard = RealRedis {
        child,
        addr: addr.clone(),
    };
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return Some(guard);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("redis-server did not start listening on {addr}");
}

/// Serve a `FakeRedis` on a free local port through the tokio providers.
///
/// Another process may take the port between [`free_port`] and the bind:
/// a failed bind is retried on another port.
pub async fn start_fake(fake: streaming_kv_redis::FakeRedis) -> (String, tokio_util::sync::CancellationToken) {
    for _ in 0..20 {
        let addr = format!("127.0.0.1:{}", free_port());
        let token = tokio_util::sync::CancellationToken::new();
        let (a, t, f) = (addr.clone(), token.clone(), fake.clone());
        let server = tokio::spawn(async move { f.serve(moonpool_core::TokioProviders::new(), &a, t).await });
        // The bind happens on the first poll of the task.
        tokio::time::sleep(Duration::from_millis(20)).await;
        if server.is_finished() {
            continue;
        }
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                // Let the readiness probe's own connection be counted and closed.
                tokio::time::sleep(Duration::from_millis(50)).await;
                return (addr, token);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    panic!("fake did not start");
}
