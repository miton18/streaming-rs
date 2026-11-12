//! Structured task spawning.
//!
//! Every task of a worker is spawned through a [`TaskScope`]. A scope carries
//! a kill token; once the token is cancelled, none of its tasks makes any
//! further progress: their futures are dropped at whatever await point they
//! were suspended on. This gives a worker crash-like stop semantics in
//! production (shutdown) and under simulation, where killing a process only
//! aborts its main task.

use moonpool_core::{Providers, TaskProvider};
use std::future::Future;
use tokio_util::sync::CancellationToken;

/// A set of tasks sharing a kill token.
pub struct TaskScope<P: Providers> {
    providers: P,
    kill: CancellationToken,
}

impl<P: Providers> Clone for TaskScope<P> {
    fn clone(&self) -> Self {
        Self {
            providers: self.providers.clone(),
            kill: self.kill.clone(),
        }
    }
}

impl<P: Providers> TaskScope<P> {
    /// A scope killed when `kill` is cancelled.
    pub fn new(providers: P, kill: CancellationToken) -> Self { Self { providers, kill } }

    /// A nested scope: killed with its parent, or on its own.
    #[must_use]
    pub fn child(&self) -> Self {
        Self {
            providers: self.providers.clone(),
            kill: self.kill.child_token(),
        }
    }

    /// The kill token of this scope.
    #[must_use]
    pub fn token(&self) -> &CancellationToken { &self.kill }

    /// The providers tasks are spawned with.
    #[must_use]
    pub fn providers(&self) -> &P { &self.providers }

    /// Spawn a task that stops at its next await point once the scope is killed.
    pub fn spawn<F>(&self, name: &str, future: F) -> <P::Task as TaskProvider>::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let kill = self.kill.clone();
        self.providers.task().spawn_task(name, async move {
            moonpool_core::select! {
                biased;
                () = kill.cancelled() => {}
                () = future => {}
            }
        })
    }
}
