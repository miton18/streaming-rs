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
    // Another process may take the port between [`free_port`] and the
    // server's bind: the server then exits, and starts again on another port.
    for _ in 0..20 {
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
        let mut guard = RealRedis {
            child,
            addr: addr.clone(),
        };
        for _ in 0..200 {
            if guard.child.try_wait().expect("poll redis-server").is_some() {
                break;
            }
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                return Some(guard);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    panic!("redis-server did not start listening");
}

/// Serve a `FakeRedis` on a local port the system picks.
///
/// The listener is bound before its port is known, so no other process can
/// take the port in between, as it could with [`free_port`]. Cancelling the
/// token closes the listener and every connection.
pub async fn start_fake(fake: streaming_kv_redis::FakeRedis) -> (String, tokio_util::sync::CancellationToken) {
    use tokio_util::compat::TokioAsyncReadCompatExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a local port");
    let addr = listener.local_addr().expect("local addr").to_string();
    let token = tokio_util::sync::CancellationToken::new();
    let stop = token.clone();
    tokio::spawn(async move {
        // Dropping the set aborts the connections it drives.
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                accepted = listener.accept() => {
                    if let Ok((stream, _peer)) = accepted {
                        let fake = fake.clone();
                        connections.spawn(async move { fake.serve_stream(stream.compat()).await });
                    }
                }
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    (addr, token)
}
