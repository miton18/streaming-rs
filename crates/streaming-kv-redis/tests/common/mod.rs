#![allow(dead_code)]
//! Helpers shared by the integration tests: free ports and a real redis-server.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::Duration;

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
