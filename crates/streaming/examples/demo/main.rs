//! The demonstration application of streaming-rs: two tenants, `acme` and
//! `globex`, a graph each, fed by in-process generators.
//!
//! ```sh
//! cargo run --release -p streaming --example demo -- standalone
//! ```
//!
//! An application of your own is the same `main`: declare tenants,
//! operators and graphs, then call [`App::main`](streaming::App::main).

mod graphs;

fn main() -> std::process::ExitCode { graphs::app().main() }
