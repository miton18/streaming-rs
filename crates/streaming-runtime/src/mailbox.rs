//! Edges between actors.
//!
//! An edge is a bounded queue of chunks. A full edge suspends its producer,
//! which is how backpressure travels from a slow actor up to the source.
//!
//! Besides records, edges carry *barriers*. A barrier closes an epoch: every
//! record sent before it belongs to the epoch, and once every terminal actor
//! of a slice has seen the barrier, the epoch can be committed atomically.

use futures::future::select_all;
use moonpool_core::Providers;
use streaming_core::{Record, Timestamp};
use tokio::sync::mpsc;

use crate::metrics::Meter;

/// End-of-epoch marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Barrier {
    /// The epoch being closed.
    pub epoch: u64,
    /// Watermark of the slice at the end of the epoch.
    pub watermark: Timestamp,
}

/// What travels on an edge.
#[derive(Debug, Clone)]
pub(crate) enum Msg {
    Records(Vec<Record>),
    Barrier(Barrier),
}

/// What an actor gets from its inputs.
pub(crate) enum Input {
    Records(Vec<Record>),
    /// Every input edge has delivered the barrier of the same epoch.
    Barrier(Barrier),
}

/// The input edges of an actor, with barrier alignment: once an edge has
/// delivered the barrier of an epoch it is not read again until all the other
/// edges have delivered it too.
pub(crate) struct Inputs {
    edges: Vec<mpsc::Receiver<Msg>>,
    aligned: Vec<bool>,
    barrier: Option<Barrier>,
}

impl Inputs {
    pub(crate) fn new(edges: Vec<mpsc::Receiver<Msg>>) -> Self {
        let aligned = vec![false; edges.len()];
        Self {
            edges,
            aligned,
            barrier: None,
        }
    }

    /// Next batch of records, or the next barrier once aligned. Never returns
    /// when the producers are gone: the slice is then being torn down and this
    /// future is about to be dropped.
    pub(crate) async fn next(&mut self) -> Input {
        loop {
            let (index, message) = {
                let mut pending: Vec<_> = self
                    .edges
                    .iter_mut()
                    .enumerate()
                    .filter(|(index, _)| !self.aligned[*index])
                    .map(|(index, edge)| Box::pin(async move { (index, edge.recv().await) }))
                    .collect();
                match pending.len() {
                    0 => return futures::future::pending().await,
                    1 => pending.remove(0).await,
                    _ => select_all(pending).await.0,
                }
            };
            match message {
                None => return futures::future::pending().await,
                Some(Msg::Records(records)) => return Input::Records(records),
                Some(Msg::Barrier(barrier)) => {
                    self.aligned[index] = true;
                    self.barrier.get_or_insert(barrier);
                    if self.aligned.iter().all(|aligned| *aligned) {
                        self.aligned.fill(false);
                        if let Some(barrier) = self.barrier.take() {
                            return Input::Barrier(barrier);
                        }
                    }
                }
            }
        }
    }
}

/// The output edges of an actor.
pub(crate) struct Outputs {
    edges: Vec<mpsc::Sender<Msg>>,
}

impl Outputs {
    pub(crate) fn new(edges: Vec<mpsc::Sender<Msg>>) -> Self {
        Self { edges }
    }

    /// Send to every output, waiting for room. Time spent waiting is accounted
    /// as backpressure; the meter is left in the busy phase.
    pub(crate) async fn send<P: Providers>(&self, meter: &Meter<P>, message: Msg) {
        let Some((last, others)) = self.edges.split_last() else {
            return;
        };
        meter.blocked();
        for edge in others {
            if edge.send(message.clone()).await.is_err() {
                return futures::future::pending().await;
            }
        }
        if last.send(message).await.is_err() {
            return futures::future::pending().await;
        }
        meter.busy();
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;

    use super::*;

    fn barrier(epoch: u64) -> Msg {
        Msg::Barrier(Barrier { epoch, watermark: 0 })
    }

    #[test]
    fn barriers_are_aligned_across_inputs() {
        let (a_tx, a_rx) = mpsc::channel(8);
        let (b_tx, b_rx) = mpsc::channel(8);
        let mut inputs = Inputs::new(vec![a_rx, b_rx]);

        a_tx.try_send(barrier(1)).unwrap();
        // Records of the next epoch on the already-aligned edge must wait.
        a_tx.try_send(Msg::Records(Vec::new())).unwrap();
        assert!(inputs.next().now_or_never().is_none(), "barrier must wait for edge b");

        b_tx.try_send(Msg::Records(Vec::new())).unwrap();
        assert!(matches!(inputs.next().now_or_never(), Some(Input::Records(_))));
        b_tx.try_send(barrier(1)).unwrap();
        assert!(matches!(
            inputs.next().now_or_never(),
            Some(Input::Barrier(Barrier { epoch: 1, .. }))
        ));
        // Edge a is readable again.
        assert!(matches!(inputs.next().now_or_never(), Some(Input::Records(_))));
    }
}
