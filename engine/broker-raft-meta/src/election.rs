//! Deterministic shard leader election with ring failover (CP7b / HA M2).
//!
//! Preferred leader is `shard % node_count`. If that node is unhealthy, walk the ring
//! to the next alive peer. If **no** peer is alive, return `None` (fail closed) —
//! do not fall back to a preferred dead node under partition ambiguity.
//!
//! The ring is only used without the OpenRaft controller. With it, leaders are
//! chosen from the shard's in-sync replicas (`choose_shard_leader`).

use crate::cluster::NodeConfig;
use crate::controller::ShardPlacement;
use uuid::Uuid;

pub const DEFAULT_PEER_TTL_MS: i64 = 10_000;

/// Walk the ring from the preferred index; return the first alive node id.
/// Returns `None` when the ring has no alive members (fail closed).
pub fn elect_shard_leader(
    nodes: &[NodeConfig],
    shard: u32,
    is_alive: impl Fn(Uuid) -> bool,
) -> Option<Uuid> {
    if nodes.is_empty() {
        return None;
    }
    let n = nodes.len();
    let start = (shard as usize) % n;
    for step in 0..n {
        let idx = (start + step) % n;
        let id = nodes[idx].id;
        if is_alive(id) {
            return Some(id);
        }
    }
    None
}

/// Members eligible to lead a shard: its in-sync replicas. Only these are
/// known to hold every acknowledged write.
pub fn eligible_shard_leaders(placement: &ShardPlacement) -> &[Uuid] {
    &placement.isr
}

/// Controller placement choice for a shard. Keeps the current leader while it
/// is alive and in sync (moving leadership fences the shard until catch-up),
/// otherwise walks the in-sync replicas from the shard's preferred position.
/// Nodes outside the ISR (drained, lagging, never-replicated) are never
/// chosen; with no live in-sync replica the result is `None` (fail closed).
pub fn choose_shard_leader(
    placement: &ShardPlacement,
    is_alive: impl Fn(Uuid) -> bool,
) -> Option<Uuid> {
    let eligible = eligible_shard_leaders(placement);
    if eligible.contains(&placement.leader_id) && is_alive(placement.leader_id) {
        return Some(placement.leader_id);
    }
    let n = eligible.len();
    if n == 0 {
        return None;
    }
    let start = (placement.shard as usize) % n;
    (0..n)
        .map(|step| eligible[(start + step) % n])
        .find(|id| is_alive(*id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn nodes(ids: &[Uuid]) -> Vec<NodeConfig> {
        ids.iter()
            .enumerate()
            .map(|(i, id)| NodeConfig {
                internal_url: None,
                id: *id,
                addr: format!("http://n{i}:8080"),
            })
            .collect()
    }

    #[test]
    fn prefers_static_leader_when_healthy() {
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        let list = nodes(&ids);
        let alive = |_| true;
        assert_eq!(elect_shard_leader(&list, 0, alive), Some(ids[0]));
        assert_eq!(elect_shard_leader(&list, 1, alive), Some(ids[1]));
        assert_eq!(elect_shard_leader(&list, 2, alive), Some(ids[2]));
    }

    #[test]
    fn fails_over_to_next_alive() {
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        let list = nodes(&ids);
        let dead_middle = ids[1];
        let alive = |id: Uuid| id != dead_middle;
        assert_eq!(elect_shard_leader(&list, 1, alive), Some(ids[2]));
        assert_eq!(elect_shard_leader(&list, 0, alive), Some(ids[0]));
    }

    #[test]
    fn fails_over_when_preferred_dead() {
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        let list = nodes(&ids);
        let dead = ids[0];
        let alive = |id: Uuid| id != dead;
        assert_eq!(elect_shard_leader(&list, 0, alive), Some(ids[1]));
        assert_eq!(elect_shard_leader(&list, 3, alive), Some(ids[1]));
    }

    fn placement(leader: Uuid, replicas: &[Uuid], isr: &[Uuid]) -> ShardPlacement {
        ShardPlacement {
            shard: 1,
            leader_id: leader,
            leader_term: 3,
            replicas: replicas.to_vec(),
            learners: Vec::new(),
            isr: isr.to_vec(),
        }
    }

    #[test]
    fn shard_leader_comes_from_live_isr_only() {
        let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
        // ids[3] is alive and first in the static ring but holds no replica.
        let current = placement(ids[0], &ids[..3], &ids[..3]);
        assert_eq!(choose_shard_leader(&current, |_| true), Some(ids[0]));
        let dead_leader = |id: Uuid| id != ids[0];
        assert_eq!(choose_shard_leader(&current, dead_leader), Some(ids[1]));

        // A lagging replica outside the ISR is not eligible even when alive.
        let lagging = placement(ids[0], &ids[..3], &[ids[0], ids[1]]);
        assert_eq!(choose_shard_leader(&lagging, dead_leader), Some(ids[1]));
        assert_eq!(
            choose_shard_leader(&lagging, |id| id == ids[2] || id == ids[3]),
            None
        );
    }

    #[test]
    fn drained_node_does_not_regain_leadership() {
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        // After draining ids[0] it is out of replicas and ISR; leader moved.
        let drained = placement(ids[1], &ids[1..], &ids[1..]);
        assert_eq!(choose_shard_leader(&drained, |_| true), Some(ids[1]));
        assert_eq!(
            choose_shard_leader(&drained, |id| id != ids[1]),
            Some(ids[2])
        );
    }

    #[test]
    fn fail_closed_when_no_peer_alive() {
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        let list = nodes(&ids);
        let alive = |_| false;
        assert_eq!(elect_shard_leader(&list, 0, alive), None);
    }
}
