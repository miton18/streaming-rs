//! `streamd`: the streaming-rs daemon.
//!
//! - `streamd standalone` runs a whole cluster in one process: an in-memory
//!   KV, a few workers, demonstration graphs fed by a generator, the HTTP API
//!   and the web UI. Nothing to install, nothing persisted.
//! - `streamd worker` runs one production worker against a Redis-protocol KV
//!   (Redis or Materia KV), with the stock connectors.

mod demo;
mod stock;

use std::{
    io::ErrorKind,
    net::SocketAddr,
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};

use clap::{Parser, Subcommand};
use moonpool_core::TokioProviders;
use streaming_api::{ApiConfig, ApiState, control};
use streaming_core::{Clock, DynKv, EnvSecrets, MemKv, Registry, SecretProvider, SystemClock, WorkerId};
use streaming_kv_redis::{RedisConfig, RedisKv};
use streaming_runtime::{GossipConfig, GossipMembership, RuntimeEnv, Worker, WorkerConfig, builtin};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(name = "streamd", version, about = "The streaming-rs daemon")]
struct Cli {
    #[command(subcommand)]
    command: Mode,
}

#[derive(Subcommand)]
enum Mode {
    /// Run a demonstration cluster in this process, with the web UI.
    Standalone {
        /// Address of the HTTP API and web UI.
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: SocketAddr,
        /// Number of workers to run in the process.
        #[arg(long, default_value_t = 3)]
        workers: u32,
        /// Do not try to open a browser.
        #[arg(long)]
        no_open: bool,
        /// First local port of the workers' gossip (one port per worker).
        /// `0` keeps membership in the KV instead.
        #[arg(long, default_value_t = 17946)]
        gossip_port: u16,
    },
    /// Run one worker against a Redis-protocol KV (Redis, Materia KV).
    Worker {
        /// Unique id of this worker in the cluster.
        #[arg(long)]
        id: String,
        /// KV URL: `redis://[user:password@]host:port` or `rediss://` for TLS.
        #[arg(long, env = "STREAMD_KV")]
        kv: String,
        /// Also serve the HTTP API and web UI on this address.
        #[arg(long)]
        listen: Option<SocketAddr>,
        /// Address the gossip listens on. Without it, membership goes through
        /// heartbeats in the KV.
        #[arg(long)]
        gossip: Option<String>,
        /// Address other workers reach this worker's gossip at (defaults to `--gossip`).
        #[arg(long)]
        gossip_advertise: Option<String>,
        /// Gossip addresses of workers already in the cluster.
        #[arg(long, value_delimiter = ',')]
        seeds: Vec<String>,
        /// Name of the cluster; gossip from another cluster is refused.
        #[arg(long, default_value = "streaming-rs")]
        cluster: String,
    },
}

type AnyError = Box<dyn std::error::Error + Send + Sync>;

fn stock_registry(kv: &DynKv) -> Registry<TokioProviders> {
    let mut registry = Registry::<TokioProviders>::standard();
    builtin::register(&mut registry, kv.clone());
    streaming_warp10::register(&mut registry);
    stock::register(&mut registry);
    registry
}

fn environment(
    kv: DynKv,
    registry: Registry<TokioProviders>,
    secrets: Arc<dyn SecretProvider>,
) -> RuntimeEnv<TokioProviders> {
    RuntimeEnv {
        providers: TokioProviders::new(),
        kv,
        clock: Arc::new(SystemClock),
        secrets,
        registry: Arc::new(registry),
    }
}

/// What to tell someone when `address` cannot be listened on. An operating
/// system error code is not a message.
fn listen_error(address: SocketAddr, error: &std::io::Error) -> String {
    match error.kind() {
        ErrorKind::AddrInUse => format!("the address {address} is already in use"),
        ErrorKind::AddrNotAvailable => {
            format!("the address {address} does not belong to this machine")
        }
        ErrorKind::PermissionDenied => {
            format!("this user is not allowed to listen on the address {address}")
        }
        _ => format!("cannot listen on the address {address}: {error}"),
    }
}

/// Open the HTTP listener, or say in plain words why not.
async fn listen_on(address: SocketAddr) -> Result<TcpListener, AnyError> {
    TcpListener::bind(address)
        .await
        .map_err(|error| listen_error(address, &error).into())
}

async fn wait_for_signal(shutdown: CancellationToken) {
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("\nstopping...");
    shutdown.cancel();
}

/// Try to open `url` in the user's browser. Best effort, never fatal.
fn open_browser(url: &str) -> bool {
    let wsl = std::env::var_os("WSL_DISTRO_NAME").is_some()
        || std::fs::read_to_string("/proc/version").is_ok_and(|version| version.to_lowercase().contains("microsoft"));
    let mut candidates: Vec<(&str, Vec<&str>)> = Vec::new();
    if wsl {
        candidates.push(("wslview", vec![url]));
        candidates.push(("explorer.exe", vec![url]));
        candidates.push(("cmd.exe", vec!["/c", "start", "", url]));
    }
    if cfg!(target_os = "macos") {
        candidates.push(("open", vec![url]));
    } else if cfg!(target_os = "windows") {
        candidates.push(("cmd", vec!["/C", "start", "", url]));
    } else {
        candidates.push(("xdg-open", vec![url]));
    }
    candidates.into_iter().any(|(program, arguments)| {
        Command::new(program)
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .is_ok()
    })
}

/// A gossip configuration for one worker.
fn gossip_config(cluster: &str, worker: &WorkerId, listen: &str, advertise: &str, seeds: Vec<String>) -> GossipConfig {
    let generation = SystemClock.now_ms().max(0) as u64;
    let mut config = GossipConfig::new(cluster, worker.as_str(), generation, listen);
    config.advertise_addr = advertise.to_owned();
    config.seeds = seeds;
    config
}

async fn standalone(listen: SocketAddr, workers: u32, no_open: bool, gossip_port: u16) -> Result<(), AnyError> {
    let store = MemKv::shared();
    let kv: DynKv = store.clone();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let shutdown = CancellationToken::new();
    let generators = Arc::new(demo::Generators::default());
    let listener = listen_on(listen).await?;
    let address = listener.local_addr()?;

    let mut tasks = Vec::new();
    for index in 1..=workers.max(1) {
        let mut registry = stock_registry(&kv);
        demo::register(&mut registry, generators.clone());
        let id = WorkerId::new(format!("worker-{index}"))?;
        let mut config = WorkerConfig::new(id.clone());
        config.api_addr = Some(address.to_string());
        let mut worker = Worker::new(environment(kv.clone(), registry, Arc::new(EnvSecrets)), config);
        if gossip_port != 0 {
            // The in-process workers gossip over loopback, like separate
            // machines would: the first one is everybody's seed.
            let port = |index: u32| format!("127.0.0.1:{}", u32::from(gossip_port) + index - 1);
            let config = gossip_config("standalone", &id, &port(index), &port(index), vec![port(1)]);
            match GossipMembership::join(TokioProviders::new(), config, kv.clone()).await {
                Ok(membership) => worker = worker.with_membership(Arc::new(membership)),
                Err(error) => eprintln!("  gossip unavailable for {id} ({error}): using KV heartbeats instead"),
            }
        }
        tasks.push(tokio::spawn(worker.run(shutdown.clone())));
    }

    let state = ApiState {
        kv: kv.clone(),
        clock: clock.clone(),
        kv_stats: Some(Arc::new(move || {
            let stats = store.stats();
            serde_json::json!({
                "keys": stats.keys,
                "bytes": stats.bytes,
                "reads": stats.reads,
                "writes": stats.writes,
            })
        })),
        config: ApiConfig::default(),
    };
    let api = tokio::spawn(streaming_api::serve(
        listener,
        state,
        shutdown.clone().cancelled_owned(),
    ));

    // Workers publish their operators on their first tick: submit the
    // demonstration graphs once the catalog is there.
    let mut submitted = false;
    for _ in 0..100 {
        if !control::load_catalog(kv.as_ref()).await?.ops.is_empty() {
            for (tenant, display_name) in demo::tenants() {
                control::create_tenant(kv.as_ref(), clock.as_ref(), tenant, Some(display_name.to_owned())).await?;
            }
            for spec in demo::graphs() {
                let graph = spec.key();
                let version = control::apply_graph(kv.as_ref(), clock.as_ref(), spec).await?;
                eprintln!("  graph {graph} submitted (version {version})");
            }
            submitted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !submitted {
        return Err("the workers did not publish their operator catalog".into());
    }

    let url = format!("http://{address}/");
    eprintln!("\n  streaming-rs standalone: {workers} workers, in-memory KV");
    eprintln!("  web UI and API: {url}");
    eprintln!("  Ctrl-C to stop\n");
    if !no_open && !open_browser(&url) {
        eprintln!("  (could not open a browser: open the address above yourself)");
    }

    wait_for_signal(shutdown).await;
    for task in tasks {
        let _ = task.await;
    }
    let _ = api.await;
    Ok(())
}

struct GossipOptions {
    listen: Option<String>,
    advertise: Option<String>,
    seeds: Vec<String>,
    cluster: String,
}

async fn worker(id: String, kv_url: String, listen: Option<SocketAddr>, gossip: GossipOptions) -> Result<(), AnyError> {
    let config = RedisConfig::from_url(&kv_url)?;
    let redis = RedisKv::new(TokioProviders::new(), config);
    redis.ping().await?;
    let kv: DynKv = Arc::new(redis);
    let shutdown = CancellationToken::new();
    let mut worker_config = WorkerConfig::new(WorkerId::new(id)?);

    let mut api = None;
    if let Some(listen) = listen {
        let listener = listen_on(listen).await?;
        let address = listener.local_addr()?;
        worker_config.api_addr = Some(address.to_string());
        eprintln!("  web UI and API: http://{address}/");
        api = Some(tokio::spawn(streaming_api::serve(
            listener,
            ApiState {
                kv: kv.clone(),
                clock: Arc::new(SystemClock),
                kv_stats: None,
                config: ApiConfig::default(),
            },
            shutdown.clone().cancelled_owned(),
        )));
    }

    let worker_id = worker_config.worker.clone();
    let mut worker = Worker::new(
        environment(kv.clone(), stock_registry(&kv), Arc::new(EnvSecrets)),
        worker_config,
    );
    if let Some(gossip_listen) = gossip.listen {
        let advertise = gossip.advertise.unwrap_or_else(|| gossip_listen.clone());
        let config = gossip_config(&gossip.cluster, &worker_id, &gossip_listen, &advertise, gossip.seeds);
        let membership = GossipMembership::join(TokioProviders::new(), config, kv.clone()).await?;
        eprintln!("  gossip: listening on {gossip_listen}, advertised as {advertise}");
        worker = worker.with_membership(Arc::new(membership));
    }
    let running = tokio::spawn(worker.run(shutdown.clone()));
    wait_for_signal(shutdown).await;
    let _ = running.await;
    if let Some(api) = api {
        let _ = api.await;
    }
    Ok(())
}

fn main() -> std::process::ExitCode {
    // Errors are for people: said in words, on one line, never as the debug
    // form of a value.
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), AnyError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        match cli.command {
            Mode::Standalone {
                listen,
                workers,
                no_open,
                gossip_port,
            } => standalone(listen, workers, no_open, gossip_port).await,
            Mode::Worker {
                id,
                kv,
                listen,
                gossip,
                gossip_advertise,
                seeds,
                cluster,
            } => {
                let gossip = GossipOptions {
                    listen: gossip,
                    advertise: gossip_advertise,
                    seeds,
                    cluster,
                };
                worker(id, kv, listen, gossip).await
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_in_use_is_said_in_words() {
        let address: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let taken = std::io::Error::from(std::io::ErrorKind::AddrInUse);
        assert_eq!(
            listen_error(address, &taken),
            "the address 127.0.0.1:8080 is already in use"
        );
        let other = std::io::Error::other("boom");
        assert_eq!(
            listen_error(address, &other),
            "cannot listen on the address 127.0.0.1:8080: boom"
        );
    }
}
