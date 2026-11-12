//! An application binary itself: what the demonstration does when it is
//! run. The test binary is its own subject: run with `STREAMING_CLI_CHILD`
//! set, it is the demonstration application (`examples/demo`).
//!
//! `harness = false`: `main` below runs the tests one after the other.

#[path = "../examples/demo/graphs.rs"]
#[allow(dead_code, unused_imports)]
mod graphs;

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    process::{Child, ChildStderr, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

/// A command running this binary as the demonstration application.
fn app() -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.env("STREAMING_CLI_CHILD", "1");
    command
}

/// The lines a child writes on stderr, as they come.
fn lines(stderr: ChildStderr) -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

/// Lines up to the first one containing `needle`, or a panic after 30 s.
fn until(rx: &mpsc::Receiver<String>, needle: &str) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut seen = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok(line) => {
                let found = line.contains(needle);
                seen.push(line);
                if found {
                    return seen;
                }
            }
            Err(_) => panic!("no `{needle}` on stderr, got {seen:?}"),
        }
    }
}

fn get(address: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(address).unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).unwrap();
    answer
}

fn interrupt(child: &Child) {
    let status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}

fn wait(mut child: Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("streamd did not stop");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn standalone(extra: &[&str], path: &str) -> (Child, mpsc::Receiver<String>) {
    let mut child = app()
        .args([
            "standalone",
            "--listen",
            "127.0.0.1:0",
            "--workers",
            "1",
            "--gossip-port",
            "0",
        ])
        .args(extra)
        .env("PATH", path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let rx = lines(child.stderr.take().unwrap());
    (child, rx)
}

/// The demonstration cluster submits its graphs, serves the API until it is
/// interrupted, then stops cleanly.
fn standalone_serves_the_demo_until_interrupted() {
    let (child, rx) = standalone(&["--no-open"], "");
    let seen = until(&rx, "Ctrl-C to stop");
    assert!(
        seen.iter()
            .any(|line| line.contains("graph acme/count-per-key published")),
        "{seen:?}"
    );
    assert!(
        seen.iter().any(|line| line.contains("graph globex/clicks published")),
        "{seen:?}"
    );
    let url = seen
        .iter()
        .find_map(|line| line.split("web UI and API: http://").nth(1))
        .expect("the address is said");
    let address = url.trim_end_matches('/').to_owned();
    let answer = get(&address, "/api/v1/tenants/acme/graphs");
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
    assert!(answer.contains("count-per-key"), "{answer}");
    // Still running a moment later: it waits for the signal.
    thread::sleep(Duration::from_millis(300));
    interrupt(&child);
    let rest = until(&rx, "stopping...");
    let status = wait(child);
    assert!(status.success(), "{status}");
    let all: Vec<&String> = seen.iter().chain(&rest).collect();
    assert!(
        !all.iter().any(|line| line.contains("could not open a browser")),
        "{all:?}"
    );
}

/// Without `--no-open`, a browser is opened, or the user is told to open the
/// address themselves.
fn standalone_opens_a_browser_or_says_so() {
    let (child, rx) = standalone(&[], "");
    let seen = until(&rx, "could not open a browser");
    assert!(seen.iter().any(|line| line.contains("Ctrl-C to stop")));
    interrupt(&child);
    assert!(wait(child).success());

    let bin = std::env::temp_dir().join(format!("streamd-browser-{}", std::process::id()));
    std::fs::create_dir_all(&bin).unwrap();
    let opener = bin.join("xdg-open");
    std::fs::write(&opener, "#!/bin/sh\nexit 0\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&opener, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let (child, rx) = standalone(&[], bin.to_str().unwrap());
    let seen = until(&rx, "Ctrl-C to stop");
    thread::sleep(Duration::from_millis(300));
    interrupt(&child);
    let rest = until(&rx, "stopping...");
    assert!(wait(child).success());
    let all: Vec<&String> = seen.iter().chain(&rest).collect();
    assert!(
        !all.iter().any(|line| line.contains("could not open a browser")),
        "{all:?}"
    );
    let _ = std::fs::remove_dir_all(&bin);
}

/// A worker that cannot reach its store says why, in words, and fails.
fn a_worker_without_a_store_fails_in_words() {
    for kv in ["nope://somewhere", "redis://127.0.0.1:1"] {
        let output = app()
            .args(["worker", "--id", "w1", "--kv", kv])
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.starts_with("error: "), "{stderr}");
        assert!(!stderr.contains("Error {") && !stderr.contains("Kind("), "{stderr}");
    }
}

/// An address that cannot be listened on is refused in words.
fn standalone_says_why_it_cannot_listen() {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = taken.local_addr().unwrap().to_string();
    let output = app()
        .args(["standalone", "--listen", &address, "--no-open", "--gossip-port", "0"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(stderr.trim(), format!("error: the address {address} is already in use"));
}

/// Given a gossip port, the workers gossip on it; given `0`, they do not.
fn standalone_gossips_on_the_given_port() {
    let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = free.local_addr().unwrap().port();
    drop(free);
    let mut child = app()
        .args(["standalone", "--listen", "127.0.0.1:0", "--workers", "1", "--no-open"])
        .args(["--gossip-port", &port.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let rx = lines(child.stderr.take().unwrap());
    until(&rx, "Ctrl-C to stop");
    let taken = std::net::TcpListener::bind(("127.0.0.1", port));
    assert!(taken.is_err(), "nothing listens on the gossip port {port}");
    interrupt(&child);
    assert!(wait(child).success());
}

fn main() -> std::process::ExitCode {
    if std::env::var_os("STREAMING_CLI_CHILD").is_some() {
        return graphs::app().main();
    }
    let tests: &[(&str, fn())] = &[
        (
            "standalone_serves_the_demo_until_interrupted",
            standalone_serves_the_demo_until_interrupted,
        ),
        (
            "standalone_opens_a_browser_or_says_so",
            standalone_opens_a_browser_or_says_so,
        ),
        (
            "a_worker_without_a_store_fails_in_words",
            a_worker_without_a_store_fails_in_words,
        ),
        (
            "standalone_says_why_it_cannot_listen",
            standalone_says_why_it_cannot_listen,
        ),
        (
            "standalone_gossips_on_the_given_port",
            standalone_gossips_on_the_given_port,
        ),
    ];
    let filter: Vec<String> = std::env::args().skip(1).filter(|arg| !arg.starts_with('-')).collect();
    let mut failed = 0;
    for (name, test) in tests {
        if !filter.is_empty() && !filter.iter().any(|wanted| name.contains(wanted.as_str())) {
            continue;
        }
        let outcome = std::panic::catch_unwind(test);
        println!("test {name} ... {}", if outcome.is_ok() { "ok" } else { "FAILED" });
        failed += usize::from(outcome.is_err());
    }
    println!(
        "test result: {}. {failed} failed",
        if failed == 0 { "ok" } else { "FAILED" }
    );
    if failed == 0 {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::FAILURE
    }
}
