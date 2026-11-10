//! Shard-leader upkeep: catch learners up, promote them once verified, and
//! report the measured in-sync replica set to the controller.

use crate::append::ReplicationClient;
use broker_raft_meta::{ControllerCommand, ShardPlacement};
use std::collections::BTreeSet;
use tracing::{info, warn};
use uuid::Uuid;

/// Failed pushes in a row before an in-sync replica is reported out of sync.
const ISR_SHRINK_FAILURES: u64 = 3;
/// Time without a successful push before an in-sync replica is reported out.
const ISR_SHRINK_AFTER_MS: u64 = 10_000;

/// What the leader knows about one peer's copy of a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerSync {
    /// Repair confirmed every stream; safe to count as in sync.
    InSync,
    /// Out of sync long enough to report.
    Lagging,
    /// No evidence either way (no traffic yet, or a brief blip).
    Unknown,
}

/// Desired ISR given the current one and each replica's measured state. The
/// leader is always included; replicas move only on decisive evidence.
pub(crate) fn desired_isr(
    placement: &ShardPlacement,
    leader: Uuid,
    peer: impl Fn(Uuid) -> PeerSync,
) -> Vec<Uuid> {
    let mut isr = vec![leader];
    for replica in placement
        .replicas
        .iter()
        .copied()
        .filter(|id| *id != leader)
    {
        let keep = match peer(replica) {
            PeerSync::InSync => true,
            PeerSync::Lagging => false,
            PeerSync::Unknown => placement.isr.contains(&replica),
        };
        if keep {
            isr.push(replica);
        }
    }
    isr
}

fn same_members(a: &[Uuid], b: &[Uuid]) -> bool {
    a.iter().collect::<BTreeSet<_>>() == b.iter().collect::<BTreeSet<_>>()
}

impl ReplicationClient {
    /// Sync state of `peer` for `shard` across all of the shard's streams.
    pub(crate) fn peer_sync(&self, streams: &[String], shard: u32, peer: Uuid) -> PeerSync {
        let progress = self.replica_progress();
        let now = crate::append::now_ms();
        let mut confirmed = 0usize;
        let mut lagging = false;
        for stream in streams {
            if self.repair_in_progress(stream, shard, peer) {
                return PeerSync::Unknown;
            }
            let Some(entry) = progress.iter().find(|entry| {
                entry.partition == shard && entry.peer_id == peer && entry.topic == *stream
            }) else {
                continue;
            };
            if entry.in_sync {
                confirmed += 1;
            } else if entry.consecutive_failures >= ISR_SHRINK_FAILURES
                || now.saturating_sub(entry.last_success_ms.unwrap_or(0)) > ISR_SHRINK_AFTER_MS
            {
                lagging = true;
            }
        }
        if lagging {
            PeerSync::Lagging
        } else if !streams.is_empty() && confirmed == streams.len() {
            PeerSync::InSync
        } else {
            PeerSync::Unknown
        }
    }

    /// Whether repair has verified that `peer` holds every stream of `shard`
    /// (streams with no records need nothing copied).
    pub(crate) fn learner_caught_up(&self, streams: &[String], shard: u32, peer: Uuid) -> bool {
        let Some(source) = self.repair.source.get() else {
            return false;
        };
        let progress = self.replica_progress();
        streams.iter().all(|stream| {
            if self.repair_in_progress(stream, shard, peer) {
                return false;
            }
            let end = source.log_end(stream, shard).unwrap_or(u64::MAX);
            end == 0
                || progress.iter().any(|entry| {
                    entry.partition == shard
                        && entry.peer_id == peer
                        && entry.topic == *stream
                        && entry.in_sync
                        && entry.durable_hwm >= end
                })
        })
    }

    /// Run on the cluster health tick. For each shard this node leads: wake
    /// repair of lagging peers, catch learners up and promote them once
    /// verified, and commit the measured ISR when it changes.
    pub async fn maintain_led_shards(&self) {
        self.wake_repairs();
        let Some(runtime) = self.runtime.clone() else {
            return;
        };
        let Some(controller) = runtime.controller() else {
            return;
        };
        let Some(source) = self.repair.source.get().cloned() else {
            return;
        };
        let node_id = self.cluster.node_id;
        let state = controller.state();
        for placement in state.shards.values() {
            let shard = placement.shard;
            if placement.leader_id != node_id || !runtime.is_leader_for_shard(shard) {
                continue;
            }
            let streams = source.streams(shard);
            let targets = runtime.replication_targets(shard);
            for learner in &targets.learners {
                if self.learner_caught_up(&streams, shard, learner.id) {
                    self.promote_learner(placement, learner.id).await;
                    continue;
                }
                for stream in &streams {
                    if !self.repair_in_progress(stream, shard, learner.id) {
                        self.schedule_repair(stream, shard, learner.clone());
                    }
                }
            }
            let isr = desired_isr(placement, node_id, |peer| {
                self.peer_sync(&streams, shard, peer)
            });
            if same_members(&isr, &placement.isr) {
                continue;
            }
            let committed_hwm = if streams.len() == 1 && streams[0].is_empty() {
                source.log_end("", shard).unwrap_or(0)
            } else {
                0
            };
            match controller
                .submit(ControllerCommand::UpdateIsr {
                    shard,
                    leader_id: node_id,
                    leader_term: placement.leader_term,
                    isr: isr.clone(),
                    committed_hwm,
                })
                .await
            {
                Ok(_) => info!(shard, ?isr, previous = ?placement.isr, "committed measured ISR"),
                Err(error) => warn!(shard, %error, "failed to commit measured ISR"),
            }
        }
    }

    async fn promote_learner(&self, placement: &ShardPlacement, learner: Uuid) {
        let Some(controller) = self
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.controller())
        else {
            return;
        };
        let shard = placement.shard;
        if let Err(error) = controller
            .submit(ControllerCommand::PromoteLearner {
                shard,
                node_id: learner,
                leader_id: self.cluster.node_id,
                leader_term: placement.leader_term,
            })
            .await
        {
            warn!(shard, %learner, %error, "learner promotion rejected");
            return;
        }
        info!(shard, %learner, "learner caught up and promoted");
        let state = controller.state();
        let finished = state.rebalance.get(&shard).is_some_and(|op| {
            state
                .shards
                .get(&shard)
                .is_some_and(|placement| op.adding.iter().all(|id| placement.replicas.contains(id)))
        });
        if finished {
            if let Err(error) = controller
                .submit(ControllerCommand::CompleteRebalance { shard })
                .await
            {
                warn!(shard, %error, "failed to complete rebalance after promotion");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placement(leader: Uuid, replicas: &[Uuid], isr: &[Uuid]) -> ShardPlacement {
        ShardPlacement {
            shard: 0,
            leader_id: leader,
            leader_term: 1,
            replicas: replicas.to_vec(),
            learners: Vec::new(),
            isr: isr.to_vec(),
            committed_hwm: 0,
        }
    }

    #[test]
    fn isr_moves_only_on_decisive_evidence() {
        let ids: Vec<_> = (0..3).map(|_| Uuid::new_v4()).collect();
        let current = placement(ids[0], &ids, &ids);
        let unknown = desired_isr(&current, ids[0], |_| PeerSync::Unknown);
        assert_eq!(unknown, ids);
        let lagging = desired_isr(&current, ids[0], |peer| {
            if peer == ids[2] {
                PeerSync::Lagging
            } else {
                PeerSync::Unknown
            }
        });
        assert_eq!(lagging, vec![ids[0], ids[1]]);
        let shrunk = placement(ids[0], &ids, &ids[..2]);
        assert_eq!(
            desired_isr(&shrunk, ids[0], |_| PeerSync::Unknown),
            vec![ids[0], ids[1]],
            "an out-of-sync replica returns only after repair confirms it"
        );
        assert_eq!(desired_isr(&shrunk, ids[0], |_| PeerSync::InSync), ids);
    }
}
