//! streaming-rs, the SDK: a cluster's graphs are Rust code.
//!
//! An application declares its tenants and its graphs, then hands control to
//! [`App::main`], which turns the binary into a worker. An operator is a
//! structure: its name and its fields are its reference and its
//! configuration ([`core::Operator`]), and what it does is a trait, a
//! [`core::Source`], a [`core::Sink`], or a [`core::Transformer`] for
//! everything in between. Naming an operator in a graph registers it.
//!
//! ```no_run
//! use std::time::Duration;
//! use streaming::{
//!     App,
//!     core::{GraphBuilder, Keyed, Operator, Pipe, Transformer, stdops::{Count, Tumbling}},
//!     runtime::builtin::KvSink,
//!     stock::CountToJson,
//! };
//! # use streaming::core::{Context, OpError, Position, Source, SourcePipe, moonpool_core::Providers};
//! # #[derive(Debug, Clone, Operator)]
//! # #[operator("acme/words@1")]
//! # struct Words;
//! # impl Source for Words {
//! #     type Out = String;
//! #     async fn partitions<P: Providers>(&self, _: &Context<P>) -> Result<u32, OpError> { Ok(1) }
//! #     async fn start<P: Providers>(&self, _: &Context<P>, _: u32, _: Option<Position>, out: SourcePipe<String>) -> Result<(), OpError> {
//! #         out.closed().await;
//! #         Ok(())
//! #     }
//! # }
//!
//! /// Groups words by their first letter.
//! #[derive(Debug, Clone, Operator)]
//! #[operator("acme/by-initial@1")]
//! struct ByInitial;
//!
//! impl Transformer<String> for ByInitial {
//!     type Out = Keyed<String>;
//!
//!     fn transform(&self, input: Pipe<String>) -> Pipe<Keyed<String>> { input.key_by(|word| word.chars().next()) }
//! }
//!
//! fn words_per_initial() -> GraphBuilder {
//!     let mut graph = GraphBuilder::new("acme", "words-per-initial");
//!     let words = graph.source("words", Words);
//!     let by_initial = graph.then(words, "by-initial", ByInitial);
//!     let window = graph.then(by_initial, "window", Tumbling::of(Duration::from_secs(300)));
//!     let count = graph.then(window, "count", Count);
//!     let rows = graph.then(count, "rows", CountToJson);
//!     graph.sink(rows, "results", KvSink::default());
//!     graph
//! }
//!
//! fn main() -> std::process::ExitCode {
//!     App::new()
//!         .tenant("acme", "Acme Corporation")
//!         .graph(words_per_initial())
//!         .main()
//! }
//! ```
//!
//! The code is the truth: every worker runs the same binary and, when it
//! starts, publishes the tenants and graphs of the code into the store, where
//! the controller, the other workers and the HTTP API read them. A graph the
//! store holds but the code no longer does is stopped, its state kept until
//! it is deleted through the API. While a deployment is in progress two
//! binaries run side by side: each graph remembers the declarations it has
//! had, and a binary the store has moved on from steps aside from that graph,
//! neither publishing nor running it, as long as a worker of another binary
//! is alive. The store keeps the
//! graphs' state, and the configuration fields changed through the API
//! (`PATCH .../nodes/{node}/config`), which only the fields an operator
//! describes among its parameters can be.
//!
//! The binary takes two commands:
//!
//! - `standalone` runs a whole cluster in one process: an in-memory store, a
//!   few workers, the HTTP API and the web UI. Nothing to install, nothing
//!   persisted.
//! - `worker` runs one worker against a Redis-protocol store (Redis or
//!   Materia KV), and the HTTP API and web UI with `--listen`.
//!
//! The connectors and the stock operators of [`stock`] come with every
//! application.

pub mod stock;

use clap::{Parser, Subcommand};
pub use moonpool_core;
use moonpool_core::TokioProviders;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::ErrorKind,
    net::SocketAddr,
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};
pub use streaming_api as api;
use streaming_api::{
    ApiConfig, ApiState,
    control::{self, ConfigCheck, IgnoredOverride, Publisher},
};
pub use streaming_core as core;
use streaming_core::{
    Clock, DynKv, EnvSecrets, GraphBuilder, GraphKey, GraphSpec, Kv, MemKv, OpError, Operators, Registry,
    SecretProvider, Shared, SystemClock, TenantId, WorkerId,
};
use streaming_kv_redis::{RedisConfig, RedisKv};
#[cfg(feature = "pulsar")]
pub use streaming_pulsar as pulsar;
pub use streaming_runtime as runtime;
use streaming_runtime::{GossipConfig, GossipMembership, RuntimeEnv, Worker, WorkerConfig, builtin};
pub use streaming_warp10 as warp10;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// The registry the operators of an application are registered in.
pub type AppRegistry = Registry<TokioProviders>;

type Registered = Arc<dyn Fn(&mut AppRegistry) + Send + Sync>;

/// How often a worker checks that the store still holds what its code
/// declares, to publish again when it does not.
const REPUBLISH_INTERVAL: Duration = Duration::from_secs(10);

/// A fatal error of an application, said in words.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// The tenants, operators and graphs of a cluster, and what runs them.
#[derive(Default)]
pub struct App {
    tenants: Vec<(TenantId, Option<String>)>,
    operators: Vec<Registered>,
    /// The operators the nodes of each graph name.
    named: Vec<(GraphKey, Operators)>,
    graphs: Vec<GraphSpec>,
    problems: Vec<String>,
    /// What the operators of this process share, whatever the worker.
    shared: Arc<Shared>,
}

impl App {
    /// An application with no tenant, no graph, and the stock operators.
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Declare a tenant, with the name people see. Every graph belongs to a
    /// declared tenant.
    #[must_use]
    pub fn tenant(mut self, tenant: &str, display_name: &str) -> Self {
        match TenantId::new(tenant) {
            Ok(id) if self.tenants.iter().any(|(known, _)| known == &id) => {
                self.problems.push(format!("the tenant `{id}` is declared twice"));
            }
            Ok(id) => self.tenants.push((id, Some(display_name.to_owned()))),
            Err(error) => self.problems.push(format!("tenant `{tenant}`: {error}")),
        }
        self
    }

    /// Register operators by hand: those no graph names, or built at a lower
    /// level than [`streaming_core::Source`], [`streaming_core::Sink`] and
    /// [`streaming_core::Transformer`], which a graph registers by naming
    /// them. `register` is called once for every worker the process runs, on
    /// a registry that already holds the stock operators.
    #[must_use]
    pub fn operators(mut self, register: impl Fn(&mut AppRegistry) + Send + Sync + 'static) -> Self {
        self.operators.push(Arc::new(register));
        self
    }

    /// Add a graph, and the operators its nodes name.
    #[must_use]
    pub fn graph(mut self, graph: GraphBuilder) -> Self {
        match graph.into_parts() {
            Ok((spec, _)) if self.graphs.iter().any(|known| known.key() == spec.key()) => {
                self.problems
                    .push(format!("the graph `{}` is declared twice", spec.key()));
            }
            Ok((spec, operators)) => {
                self.named.push((spec.key(), operators));
                self.graphs.push(spec);
            }
            Err(error) => self.problems.push(format!("a graph is invalid: {error}")),
        }
        self
    }

    /// The graphs of the application, as the store keeps them.
    #[must_use]
    pub fn graphs(&self) -> &[GraphSpec] { &self.graphs }

    /// The operators of the application, stock ones included: those
    /// registered by hand, then those its graphs name.
    #[must_use]
    pub fn registry(&self) -> AppRegistry { self.registered().0 }

    /// The registry, and the operators of the graphs that cannot be
    /// registered, said in words.
    fn registered(&self) -> (AppRegistry, Vec<String>) {
        let mut registry = AppRegistry::standard();
        builtin::register(&mut registry);
        streaming_warp10::register(&mut registry);
        stock::register(&mut registry);
        for register in &self.operators {
            register(&mut registry);
        }
        let mut problems = Vec::new();
        for (graph, operators) in &self.named {
            for (node, error) in operators.register(&mut registry) {
                problems.push(format!("node `{node}` of the graph `{graph}`: {error}"));
            }
        }
        (registry, problems)
    }

    /// What the API asks before it stores a configuration: whether the
    /// operator takes it, for the reasons [`App::check`] goes by. An operator
    /// this binary does not have belongs to another one, which is its judge.
    fn config_check(&self) -> ConfigCheck {
        let registry = Arc::new(self.registry());
        Arc::new(move |op, fed, config| match registry.get_for(op, fed) {
            Some(_) => registry.accepts(op, fed, config).map_err(|error| match error {
                // The answer already says a configuration is refused, and
                // names the node: the operator's own words are enough.
                OpError::Config(why) => why.strip_prefix(&format!("{op}: ")).unwrap_or(&why).to_owned(),
                other => other.to_string(),
            }),
            None => Ok(()),
        })
    }

    /// Why the application cannot run: a declaration that is invalid, or a
    /// graph that does not fit its operators.
    ///
    /// # Errors
    ///
    /// The problems found, one per line.
    pub fn check(&self) -> Result<(), Error> {
        let mut problems = self.problems.clone();
        let declared: BTreeSet<&TenantId> = self.tenants.iter().map(|(tenant, _)| tenant).collect();
        let (registry, unregistered) = self.registered();
        problems.extend(unregistered);
        let catalog = registry.catalog();
        for spec in &self.graphs {
            if !declared.contains(&spec.tenant) {
                problems.push(format!(
                    "the graph `{}` belongs to the tenant `{}`, which is not declared",
                    spec.key(),
                    spec.tenant
                ));
            }
            let topology = match streaming_core::Topology::build(spec, &catalog) {
                Ok(topology) => topology,
                Err(error) => {
                    problems.push(format!("the graph `{}` is invalid: {error}", spec.key()));
                    continue;
                }
            };
            // What the code stored is what the operators accept. What a
            // source or a sink needs the outside world for (a secret, a
            // broker) is found when its instances start.
            for node in &spec.nodes {
                let fed = topology
                    .inputs
                    .get(&node.id)
                    .and_then(|inputs| inputs.first())
                    .and_then(|input| topology.types.get(input));
                let accepted = registry.accepts(&node.op, fed, &node.config);
                if let Err(error) = accepted {
                    problems.push(format!("node `{}` of the graph `{}`: {error}", node.id, spec.key()));
                }
            }
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems.join("\n").into())
        }
    }

    /// The graphs of the code with the fingerprint of their declaration:
    /// what a worker of this binary runs, and nothing else.
    fn declared(&self) -> BTreeMap<GraphKey, String> {
        self.graphs
            .iter()
            .map(|graph| (graph.key(), graph.declaration_fingerprint()))
            .collect()
    }

    /// Make the store describe the tenants and graphs of the code, and say
    /// on stderr what changed (`again`: only what changed since the previous
    /// publication of this process).
    async fn publish(&self, kv: &dyn Kv, clock: &dyn Clock, publisher: &Publisher, again: bool) -> Result<(), Error> {
        let catalog = self.registry().catalog();
        let publication = control::publish(kv, clock, &catalog, &self.tenants, &self.graphs, publisher).await?;
        for tenant in &publication.created_tenants {
            eprintln!("  tenant {tenant} created");
        }
        for (graph, version) in &publication.updated {
            eprintln!("  graph {graph} published (version {version})");
        }
        for (graph, version) in &publication.restored {
            eprintln!("  graph {graph} is back in the code: running again on its state (version {version})");
        }
        for graph in &publication.orphaned {
            eprintln!(
                "  graph {graph} is no longer in the code: stopped, its state is kept until it is deleted through the API"
            );
        }
        if again {
            return Ok(());
        }
        for tenant in &publication.renamed_tenants {
            eprintln!("  tenant {tenant} is stored with another display name, which is kept");
        }
        for graph in &publication.unchanged {
            eprintln!("  graph {graph} unchanged");
        }
        for graph in &publication.orphans {
            eprintln!(
                "  graph {graph} is not in the code: stopped, its state is kept until it is deleted through the API"
            );
        }
        for graph in &publication.still_deleting {
            eprintln!("  graph {graph} is still being deleted: it is published once it is gone");
        }
        for graph in &publication.aside {
            eprintln!(
                "  graph {graph}: workers of another binary run it otherwise than this code declares it. \
                 Stepping aside from it until every worker runs this binary"
            );
        }
        for IgnoredOverride {
            graph,
            node,
            field,
            refusal,
        } in &publication.ignored_overrides
        {
            match refusal {
                None => eprintln!(
                    "  graph {graph}: node {node} no longer exposes `{field}`, its value set through the API is not \
                     applied"
                ),
                Some(refusal) => eprintln!(
                    "  graph {graph}: node {node} no longer takes the value set through the API for `{field}` \
                     ({refusal}), it is not applied"
                ),
            }
        }
        Ok(())
    }

    /// Publish again whenever the store no longer holds what the code
    /// declares: this binary stepped aside from a graph while another one
    /// was running, a graph was being deleted, or a concurrent write got in
    /// the way. Looks every [`REPUBLISH_INTERVAL`], until `shutdown`.
    async fn keep_published(
        self: Arc<Self>,
        kv: DynKv,
        clock: Arc<dyn Clock>,
        publisher: Publisher,
        shutdown: CancellationToken,
    ) {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(REPUBLISH_INTERVAL) => {}
            }
            match control::out_of_date(kv.as_ref(), &self.graphs).await {
                Ok(false) => {}
                Ok(true) => {
                    if let Err(error) = self.publish(kv.as_ref(), clock.as_ref(), &publisher, true).await {
                        tracing::warn!(%error, "cannot publish the graphs of the code");
                    }
                }
                Err(error) => tracing::warn!(%error, "cannot read the graph index"),
            }
        }
    }

    /// Parse the command line, run what it asks for, and turn a failure into
    /// a message on stderr. Meant to be the whole `main` of a binary.
    #[must_use]
    pub fn main(self) -> std::process::ExitCode {
        // Errors are for people: said in words, on one line, never as the debug
        // form of a value.
        match self.main_inner() {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("error: {error}");
                std::process::ExitCode::FAILURE
            }
        }
    }

    fn main_inner(self) -> Result<(), Error> {
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
        runtime.block_on(self.run(cli.command))
    }

    /// Run what `mode` asks for, until Ctrl-C.
    ///
    /// # Errors
    ///
    /// Why the application could not start or stopped.
    pub async fn run(self, mode: Mode) -> Result<(), Error> {
        self.check()?;
        let app = Arc::new(self);
        match mode {
            Mode::Standalone {
                listen,
                workers,
                no_open,
                gossip_port,
            } => standalone(app, listen, workers, no_open, gossip_port).await,
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
                worker(app, id, kv, listen, gossip).await
            }
        }
    }
}

#[derive(Parser)]
#[command(version, about = "A streaming-rs application")]
struct Cli {
    #[command(subcommand)]
    command: Mode,
}

/// What an application binary is asked to run.
#[derive(Subcommand)]
pub enum Mode {
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

fn environment(app: &App, kv: DynKv, secrets: Arc<dyn SecretProvider>) -> RuntimeEnv<TokioProviders> {
    let registry = app.registry();
    RuntimeEnv {
        shared: app.shared.clone(),
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
async fn listen_on(address: SocketAddr) -> Result<TcpListener, Error> {
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

async fn standalone(
    app: Arc<App>,
    listen: SocketAddr,
    workers: u32,
    no_open: bool,
    gossip_port: u16,
) -> Result<(), Error> {
    let store = MemKv::shared();
    let kv: DynKv = store.clone();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let shutdown = CancellationToken::new();
    let listener = listen_on(listen).await?;
    let address = listener.local_addr()?;
    let ids = (1..=workers.max(1))
        .map(|index| WorkerId::new(format!("worker-{index}")))
        .collect::<Result<Vec<_>, _>>()?;
    let publisher = Publisher {
        workers: ids.clone(),
        ..Publisher::default()
    };
    app.publish(kv.as_ref(), clock.as_ref(), &publisher, false).await?;

    let mut tasks = vec![tokio::spawn(app.clone().keep_published(
        kv.clone(),
        clock.clone(),
        publisher,
        shutdown.clone(),
    ))];
    for (index, id) in (1..).zip(ids) {
        let mut config = WorkerConfig::new(id.clone());
        config.api_addr = Some(address.to_string());
        config.declared = Some(app.declared());
        let mut worker = Worker::new(environment(&app, kv.clone(), Arc::new(EnvSecrets)), config);
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
        accepts: Some(app.config_check()),
    };
    let api = tokio::spawn(streaming_api::serve(
        listener,
        state,
        shutdown.clone().cancelled_owned(),
    ));

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

async fn worker(
    app: Arc<App>,
    id: String,
    kv_url: String,
    listen: Option<SocketAddr>,
    gossip: GossipOptions,
) -> Result<(), Error> {
    let config = RedisConfig::from_url(&kv_url)?;
    let redis = RedisKv::new(TokioProviders::new(), config);
    redis.ping().await?;
    let kv: DynKv = Arc::new(redis);
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let shutdown = CancellationToken::new();
    let mut worker_config = WorkerConfig::new(WorkerId::new(id)?);
    worker_config.declared = Some(app.declared());
    let publisher = Publisher {
        workers: vec![worker_config.worker.clone()],
        member_timeout: worker_config.member_timeout,
    };
    app.publish(kv.as_ref(), clock.as_ref(), &publisher, false).await?;
    let republishing = tokio::spawn(
        app.clone()
            .keep_published(kv.clone(), clock.clone(), publisher, shutdown.clone()),
    );

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
                clock,
                kv_stats: None,
                config: ApiConfig::default(),
                accepts: Some(app.config_check()),
            },
            shutdown.clone().cancelled_owned(),
        )));
    }

    let worker_id = worker_config.worker.clone();
    let mut worker = Worker::new(environment(&app, kv.clone(), Arc::new(EnvSecrets)), worker_config);
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
    let _ = republishing.await;
    if let Some(api) = api {
        let _ = api.await;
    }
    Ok(())
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

    fn graph(tenant: &str, name: &str, window: &str) -> GraphBuilder {
        use streaming_core::{
            Keyed,
            stdops::{Count, Identity, Tumbling},
        };
        let mut graph = GraphBuilder::new(tenant, name);
        // `std/identity@1` stands for a source and a key: only the wiring
        // and the configurations are looked at here.
        let events = graph.node("events", Identity).typed::<Keyed<u8>>();
        let windows = graph.then(events, window, Tumbling::of(std::time::Duration::from_secs(5)));
        graph.then(windows, "count", Count);
        graph
    }

    #[test]
    fn the_api_asks_the_operators_of_the_application() {
        let accepts = App::new().config_check();
        let op = |name: &str| streaming_core::OpRef::new(name).unwrap();
        let sliding = |slide: &str| serde_json::json!({"size": "10s", "slide": slide});
        assert_eq!(accepts(&op("std/sliding@1"), None, &sliding("5s")), Ok(()));
        // What no description of a parameter tells: two fields that do not
        // go together.
        assert_eq!(
            accepts(&op("std/sliding@1"), None, &sliding("20s")),
            Err("`slide` must not exceed `size`".to_owned())
        );
        // An operator of another binary is not this one's to judge.
        assert_eq!(accepts(&op("other/unknown@1"), None, &sliding("20s")), Ok(()));
    }

    fn problems(app: &App) -> String { app.check().expect_err("the application is refused").to_string() }

    #[test]
    fn an_application_says_everything_that_is_wrong_with_it() {
        let app = App::new()
            .tenant("acme", "Acme")
            .tenant("acme", "Acme again")
            .tenant("bad tenant", "Bad")
            .graph(graph("acme", "g", "window"))
            .graph(graph("acme", "g", "window"))
            .graph(graph("globex", "h", "window"))
            .graph(graph("acme", "i", "bad id"));
        let problems = problems(&app);
        let lines: Vec<&str> = problems.lines().collect();
        assert_eq!(lines.len(), 7, "{problems}");
        assert_eq!(lines[0], "the tenant `acme` is declared twice");
        assert!(lines[1].starts_with("tenant `bad tenant`: "), "{problems}");
        assert_eq!(lines[2], "the graph `acme/g` is declared twice");
        assert!(lines[3].starts_with("a graph is invalid: "), "{problems}");
        // The wiring of the stand-in graphs is wrong, and said per graph.
        assert!(lines[4].starts_with("the graph `acme/g` is invalid: "), "{problems}");
        assert_eq!(
            lines[5],
            "the graph `globex/h` belongs to the tenant `globex`, which is not declared"
        );
        assert!(lines[6].starts_with("the graph `globex/h` is invalid: "), "{problems}");
        assert_eq!(app.graphs().len(), 2);
    }

    mod standins {
        use streaming_core::{
            Context, Keyed, OpError, Operator, Pipe, Position, Sink, SinkPipe, Source, SourcePipe, Transformer,
            Windowed, Windows, moonpool_core::Providers,
        };

        /// What a configuration that cannot run is refused with.
        fn refuse<T>(_: &T) -> Result<(), OpError> { Err(OpError::Config("refused".into())) }

        /// Stands for a source.
        #[derive(Operator)]
        #[operator("test/events@1")]
        pub struct Events;

        /// Stands for a source whose configuration is refused.
        #[derive(Operator)]
        #[operator("test/refused-events@1", check = refuse)]
        pub struct RefusedEvents;

        macro_rules! sources {
            ($($source:ty),*) => {$(
                impl Source for $source {
                    type Out = u8;

                    async fn partitions<P: Providers>(&self, _: &Context<P>) -> Result<u32, OpError> { Ok(1) }

                    async fn start<P: Providers>(
                        &self,
                        _: &Context<P>,
                        _: u32,
                        _: Option<Position>,
                        out: SourcePipe<u8>,
                    ) -> Result<(), OpError> {
                        out.closed().await;
                        Ok(())
                    }
                }
            )*};
        }
        sources!(Events, RefusedEvents);

        /// Stands for a map whose configuration is refused.
        #[derive(Operator)]
        #[operator("test/refused-map@1", check = refuse)]
        pub struct RefusedMap;

        impl Transformer<u8> for RefusedMap {
            type Out = u8;

            fn transform(&self, input: Pipe<u8>) -> Pipe<u8> { input.map(|value| *value) }
        }

        /// Stands for a key extractor.
        #[derive(Operator)]
        #[operator("test/key@1")]
        pub struct ByValue;

        /// Stands for a key extractor whose configuration is refused.
        #[derive(Operator)]
        #[operator("test/refused-key@1", check = refuse)]
        pub struct RefusedKey;

        macro_rules! keys {
            ($($key:ty),*) => {$(
                impl Transformer<u8> for $key {
                    type Out = Keyed<u8>;

                    fn transform(&self, input: Pipe<u8>) -> Pipe<Keyed<u8>> { input.key_by(|value| *value) }
                }
            )*};
        }
        keys!(ByValue, RefusedKey);

        /// Stands for a window whose configuration is refused.
        #[derive(Operator)]
        #[operator("test/refused-window@1", check = refuse)]
        pub struct RefusedWindow;

        impl Transformer<Keyed<u8>> for RefusedWindow {
            type Out = Windows<u8>;

            fn transform(&self, input: Pipe<Keyed<u8>>) -> Pipe<Windows<u8>> {
                input.window(std::time::Duration::ZERO, |_, _| {})
            }
        }

        /// Stands for an aggregate whose configuration is refused.
        #[derive(Operator)]
        #[operator("test/refused-count@1", check = refuse)]
        pub struct RefusedCount;

        impl Transformer<Windows<u8>> for RefusedCount {
            type Out = Windowed<u64>;

            fn transform(&self, input: Pipe<Windows<u8>>) -> Pipe<Windowed<u64>> {
                input.aggregate(|| 0_u64, |count, _| *count += 1, |count, other| *count += other)
            }
        }

        /// Stands for a sink.
        #[derive(Operator)]
        #[operator("test/out@1")]
        pub struct Out;

        /// Stands for a sink whose configuration is refused.
        #[derive(Operator)]
        #[operator("test/refused-out@1", check = refuse)]
        pub struct RefusedOut;

        macro_rules! sinks {
            ($($sink:ty),*) => {$(
                impl Sink<Windowed<u64>> for $sink {
                    async fn start<P: Providers>(
                        &self,
                        _: &Context<P>,
                        mut input: SinkPipe<Windowed<u64>>,
                    ) -> Result<(), OpError> {
                        while let Some(batch) = input.next().await {
                            batch.done();
                        }
                        Ok(())
                    }
                }
            )*};
        }
        sinks!(Out, RefusedOut);
    }

    /// A valid graph in which the node `refused`, of kind `kind`, has a
    /// configuration its operator refuses.
    fn refused_graph(kind: streaming_core::OpKind) -> GraphBuilder {
        use standins::*;
        use streaming_core::{
            OpKind,
            stdops::{Count, Tumbling},
        };
        let mut graph = GraphBuilder::new("acme", "g");
        let events = if kind == OpKind::Source {
            graph.source("refused", RefusedEvents)
        } else {
            graph.source("events", Events)
        };
        let fed = if kind == OpKind::Map {
            graph.then(events, "refused", RefusedMap)
        } else {
            events
        };
        let keyed = if kind == OpKind::KeyBy {
            graph.then(fed, "refused", RefusedKey)
        } else {
            graph.then(fed, "key", ByValue)
        };
        let windows = if kind == OpKind::Window {
            graph.then(keyed, "refused", RefusedWindow)
        } else {
            graph.then(keyed, "window", Tumbling::of(std::time::Duration::from_secs(5)))
        };
        let counts = if kind == OpKind::Aggregate {
            graph.then(windows, "refused", RefusedCount)
        } else {
            graph.then(windows, "count", Count)
        };
        if kind == OpKind::Sink {
            graph.sink(counts, "refused", RefusedOut);
        } else {
            graph.sink(counts, "out", Out);
        }
        graph
    }

    /// An application is refused when it starts, whatever the kind of the
    /// operator that refuses the configuration its code gives it.
    #[test]
    fn every_kind_of_operator_checks_its_configuration() {
        use streaming_core::OpKind;
        let kinds = [
            (OpKind::Source, "test/refused-events@1"),
            (OpKind::Map, "test/refused-map@1"),
            (OpKind::KeyBy, "test/refused-key@1"),
            (OpKind::Window, "test/refused-window@1"),
            (OpKind::Aggregate, "test/refused-count@1"),
            (OpKind::Sink, "test/refused-out@1"),
        ];
        for (kind, op) in kinds {
            let app = App::new().tenant("acme", "Acme").graph(refused_graph(kind));
            assert_eq!(
                problems(&app),
                format!("node `refused` of the graph `acme/g`: invalid operator configuration: {op}: refused"),
                "{kind:?}"
            );
        }
        // The same graphs are accepted once nothing refuses.
        let mut graph = GraphBuilder::new("acme", "g");
        let events = graph.source("events", standins::Events);
        let keyed = graph.then(events, "key", standins::ByValue);
        let windows = graph.then(
            keyed,
            "window",
            streaming_core::stdops::Tumbling::of(std::time::Duration::from_secs(5)),
        );
        let counts = graph.then(windows, "count", streaming_core::stdops::Count);
        graph.sink(counts, "out", standins::Out);
        App::new()
            .tenant("acme", "Acme")
            .graph(graph)
            .check()
            .unwrap_or_else(|error| panic!("{error}"));
    }

    /// An operator registered by hand, below the traits, is still there, and
    /// a graph may name it without saying what flows.
    #[test]
    fn operators_are_still_registered_by_hand() {
        use streaming_core::{
            Operator,
            stdops::{Count, Tumbling},
        };
        #[derive(Operator)]
        #[operator("test/by-hand@1")]
        struct ByHand;
        let mut graph = GraphBuilder::new("acme", "g");
        let events = graph.source("events", standins::Events);
        let keyed = graph.node("key", ByHand).typed::<streaming_core::Keyed<u8>>();
        graph.edge(events, keyed);
        let windows = graph.then(keyed, "window", Tumbling::of(std::time::Duration::from_secs(5)));
        let counts = graph.then(windows, "count", Count);
        graph.sink(counts, "out", standins::Out);
        let app = App::new().tenant("acme", "Acme").graph(graph);
        assert!(problems(&app).contains("test/by-hand@1"), "{}", problems(&app));
        let app = app.operators(|registry| {
            registry.register_key(ByHand::OP, "By value.", |value: &u8| *value);
        });
        app.check().unwrap_or_else(|error| panic!("{error}"));
    }

    #[test]
    fn an_application_without_graphs_is_valid() {
        App::new().tenant("acme", "Acme").check().unwrap();
        assert!(App::new().registry().catalog().ops.len() >= 8);
    }

    #[test]
    fn every_listening_failure_is_said_in_words() {
        let address: SocketAddr = "10.1.2.3:80".parse().unwrap();
        let said = |kind: std::io::ErrorKind| listen_error(address, &std::io::Error::from(kind));
        assert_eq!(
            said(std::io::ErrorKind::AddrNotAvailable),
            "the address 10.1.2.3:80 does not belong to this machine"
        );
        assert_eq!(
            said(std::io::ErrorKind::PermissionDenied),
            "this user is not allowed to listen on the address 10.1.2.3:80"
        );
    }
}
