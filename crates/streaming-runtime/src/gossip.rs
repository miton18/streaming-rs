//! Membership over gossip.
//!
//! Each worker gossips one short value: its [`MemberInfo`] (identity, load,
//! actor counts), a couple of hundred bytes. Everything heavier (graph
//! descriptions, placement, metrics) lives in the KV, so a node joining or
//! rejoining the cluster resynchronises in a few exchanges whatever the
//! number of graphs.

use async_trait::async_trait;
use moonpool_core::Providers;
use streaming_core::control::MemberInfo;
use streaming_core::{DynKv, Timestamp, WorkerId, keys};
use streaming_gossip::Gossip;
pub use streaming_gossip::{GossipConfig, GossipError};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::worker::Membership;

/// The gossip key under which a worker publishes its [`MemberInfo`].
const MEMBER_KEY: &str = "member";

/// Membership backed by the Scuttlebutt gossip of `streaming-gossip`.
///
/// Liveness comes from the gossip failure detector. The same information is
/// mirrored into the `cluster:members` hash of the KV, because the HTTP API
/// only reads the KV.
pub struct GossipMembership<P: Providers> {
    gossip: Gossip<P>,
    kv: DynKv,
    /// Stops the gossip tasks when the membership is dropped, so that a worker
    /// that goes away takes its gossip node with it.
    _stop: DropGuard,
}

impl<P: Providers> GossipMembership<P> {
    /// Start a gossip node and join the cluster through its seeds.
    pub async fn join(providers: P, config: GossipConfig, kv: DynKv) -> Result<Self, GossipError> {
        let gossip = Gossip::start(providers, config, CancellationToken::new()).await?;
        let stop = gossip.drop_guard();
        Ok(Self {
            gossip,
            kv,
            _stop: stop,
        })
    }

    /// The underlying gossip node, for inspection.
    pub fn gossip(&self) -> &Gossip<P> {
        &self.gossip
    }
}

#[async_trait]
impl<P: Providers> Membership for GossipMembership<P> {
    async fn announce(&self, me: &MemberInfo) {
        match serde_json::to_string(me) {
            Ok(json) => {
                if let Err(error) = self.gossip.set(MEMBER_KEY, &json) {
                    tracing::warn!(%error, worker = %me.worker, "cannot gossip membership");
                }
            }
            Err(error) => tracing::warn!(%error, "cannot encode membership"),
        }
        if let Err(error) = self
            .kv
            .hset_json(keys::CLUSTER_MEMBERS, me.worker.as_str(), me)
            .await
        {
            tracing::debug!(%error, worker = %me.worker, "cannot mirror membership into the KV");
        }
    }

    async fn alive(&self, _now_ms: Timestamp, _timeout_ms: i64) -> Vec<MemberInfo> {
        self.gossip
            .live_members()
            .iter()
            .filter_map(|member| member.values.get(MEMBER_KEY))
            .filter_map(|json| serde_json::from_str::<MemberInfo>(json).ok())
            .collect()
    }

    async fn leave(&self, worker: &WorkerId) {
        self.gossip.shutdown();
        let _ = self
            .kv
            .hdel(keys::CLUSTER_MEMBERS, vec![worker.to_string()])
            .await;
    }
}
