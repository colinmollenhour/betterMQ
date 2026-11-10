//! CP7b: the health-based placement recommendation moves shard leadership off
//! an unhealthy broker. Production commits `desired_leader_for_shard` through
//! OpenRaft (cluster health monitor); end-to-end failover on `open_with_raft`
//! runtimes is covered by `src/failover_tests.rs`.

use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
use uuid::Uuid;

fn three_node_runtime(this: usize) -> (ClusterRuntime, Vec<Uuid>) {
    let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
    let nodes: Vec<NodeConfig> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| NodeConfig {
            internal_url: None,
            id: *id,
            addr: format!("http://broker{i}:8080"),
        })
        .collect();
    let cfg = ClusterConfig {
        cluster_id: Uuid::new_v4(),
        nodes,
        node_id: ids[this],
        generation: 1,
        hash_version: 1,
    };
    (ClusterRuntime::from_config_only(cfg), ids)
}

#[test]
fn all_healthy_keeps_static_shard_owners() {
    let (rt0, ids) = three_node_runtime(0);
    let now = chrono::Utc::now().timestamp_millis();
    rt0.record_self_alive(now);
    for id in &ids {
        rt0.record_peer_alive(*id, now);
    }
    for shard in 0..6 {
        assert_eq!(
            rt0.desired_leader_for_shard(shard),
            Some(ids[shard as usize % 3])
        );
    }
}

#[test]
fn broker1_down_shard1_moves_to_broker2() {
    let (rt2, ids) = three_node_runtime(2);
    let now = chrono::Utc::now().timestamp_millis();
    rt2.record_self_alive(now);
    rt2.record_peer_alive(ids[0], now);
    // broker1 (ids[1]) not healthy — preferred leader for shard 1
    assert_eq!(rt2.desired_leader_for_shard(1), Some(ids[2]));
    assert_eq!(rt2.desired_leader_for_shard(0), Some(ids[0]));
}

#[test]
fn isolated_node_recommends_no_leader() {
    let (rt, _ids) = three_node_runtime(2);
    let now = chrono::Utc::now().timestamp_millis();
    rt.record_self_alive(now);
    for shard in 0..4 {
        assert_eq!(rt.desired_leader_for_shard(shard), None);
    }
}

#[test]
fn stale_peer_observation_expires_from_the_recommendation() {
    let (rt0, ids) = three_node_runtime(0);
    let now = chrono::Utc::now().timestamp_millis();
    rt0.record_self_alive(now);
    rt0.record_peer_alive(ids[2], now);
    rt0.record_peer_alive(ids[1], now - broker_raft_meta::DEFAULT_PEER_TTL_MS - 1);
    assert_eq!(rt0.desired_leader_for_shard(1), Some(ids[2]));
}
