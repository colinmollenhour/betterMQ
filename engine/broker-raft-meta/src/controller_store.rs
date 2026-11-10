//! Per-entity encoding of the controller state in the state-machine data
//! column family.
//!
//! Each shard placement, fan-out entry, catalog record and operation receipt,
//! GC watermark and rebalance operation is stored under its own key, next to
//! one small `meta` record. Applying a command rewrites only the keys it touched, so write
//! volume tracks the size of the change rather than the size of the state.

use crate::cluster::SchedulerLease;
use crate::controller::{ControllerNode, ControllerState, NodeId, StateKey};
use crate::placement::DataNodeRecord;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

pub(crate) const PREFIX: &str = "bettermq/controller/v2/";
const META: &str = "meta";
const SHARD: &str = "shard/";
const FANOUT: &str = "fanout/";
const CATALOG: &str = "catalog/";
const GC: &str = "gc/";
const REBALANCE: &str = "rebalance/";
const CATALOG_OPERATION: &str = "catalog-op/";
/// `log-gc/<partition>/<namespace>`: the partition first, because namespaces
/// may contain `/`.
const LOG_GC: &str = "log-gc/";

#[derive(Serialize)]
struct MetaRef<'a> {
    version: u32,
    cluster_id: Uuid,
    membership_generation: u64,
    nodes: &'a BTreeMap<NodeId, ControllerNode>,
    catalog_epoch: u64,
    scheduler: &'a Option<SchedulerLease>,
    data_nodes: &'a BTreeMap<Uuid, DataNodeRecord>,
    controller_voters: &'a Vec<Uuid>,
    replication_factor: u32,
    min_isr: u32,
    catalog_authoritative: bool,
    catalog_frozen: bool,
}

#[derive(Deserialize)]
struct Meta {
    version: u32,
    cluster_id: Uuid,
    membership_generation: u64,
    nodes: BTreeMap<NodeId, ControllerNode>,
    catalog_epoch: u64,
    scheduler: Option<SchedulerLease>,
    #[serde(default)]
    data_nodes: BTreeMap<Uuid, DataNodeRecord>,
    #[serde(default)]
    controller_voters: Vec<Uuid>,
    #[serde(default)]
    replication_factor: u32,
    #[serde(default)]
    min_isr: u32,
    #[serde(default)]
    catalog_authoritative: bool,
    #[serde(default)]
    catalog_frozen: bool,
}

impl StateKey {
    pub(crate) fn encode(&self) -> String {
        match self {
            Self::Meta => format!("{PREFIX}{META}"),
            Self::Shard(shard) => format!("{PREFIX}{SHARD}{shard:010}"),
            Self::Fanout(id) => format!("{PREFIX}{FANOUT}{id}"),
            Self::Catalog(key) => format!("{PREFIX}{CATALOG}{key}"),
            Self::GcWatermark(shard) => format!("{PREFIX}{GC}{shard:010}"),
            Self::Rebalance(shard) => format!("{PREFIX}{REBALANCE}{shard:010}"),
            Self::CatalogOperation(id) => format!("{PREFIX}{CATALOG_OPERATION}{id}"),
            Self::LogGcWatermark {
                namespace,
                partition,
            } => format!("{PREFIX}{LOG_GC}{partition:010}/{namespace}"),
        }
    }

    fn decode(key: &str) -> Option<Self> {
        let rest = key.strip_prefix(PREFIX)?;
        if rest == META {
            return Some(Self::Meta);
        }
        if let Some(shard) = rest.strip_prefix(SHARD) {
            return shard.parse().ok().map(Self::Shard);
        }
        if let Some(id) = rest.strip_prefix(FANOUT) {
            return id.parse().ok().map(Self::Fanout);
        }
        if let Some(key) = rest.strip_prefix(CATALOG) {
            return Some(Self::Catalog(key.to_string()));
        }
        if let Some(shard) = rest.strip_prefix(GC) {
            return shard.parse().ok().map(Self::GcWatermark);
        }
        if let Some(shard) = rest.strip_prefix(REBALANCE) {
            return shard.parse().ok().map(Self::Rebalance);
        }
        if let Some(id) = rest.strip_prefix(CATALOG_OPERATION) {
            return id.parse().ok().map(Self::CatalogOperation);
        }
        if let Some(rest) = rest.strip_prefix(LOG_GC) {
            let (partition, namespace) = rest.split_once('/')?;
            return Some(Self::LogGcWatermark {
                namespace: namespace.to_string(),
                partition: partition.parse().ok()?,
            });
        }
        None
    }
}

/// Encoded value of one entity, or `None` when it no longer exists and its key
/// must be deleted.
pub(crate) fn encode_entity(
    state: &ControllerState,
    key: &StateKey,
) -> Result<Option<Vec<u8>>, serde_json::Error> {
    match key {
        StateKey::Meta => serde_json::to_vec(&MetaRef {
            version: state.version,
            cluster_id: state.cluster_id,
            membership_generation: state.membership_generation,
            nodes: &state.nodes,
            catalog_epoch: state.catalog_epoch,
            scheduler: &state.scheduler,
            data_nodes: &state.data_nodes,
            controller_voters: &state.controller_voters,
            replication_factor: state.replication_factor,
            min_isr: state.min_isr,
            catalog_authoritative: state.catalog_authoritative,
            catalog_frozen: state.catalog_frozen,
        })
        .map(Some),
        StateKey::Shard(shard) => state.shards.get(shard).map(serde_json::to_vec).transpose(),
        StateKey::Fanout(id) => state
            .fanout_outbox
            .get(id)
            .map(serde_json::to_vec)
            .transpose(),
        StateKey::Catalog(key) => state
            .catalog_records
            .get(key)
            .map(serde_json::to_vec)
            .transpose(),
        StateKey::GcWatermark(shard) => state
            .gc_watermarks
            .get(shard)
            .map(serde_json::to_vec)
            .transpose(),
        StateKey::Rebalance(shard) => state
            .rebalance
            .get(shard)
            .map(serde_json::to_vec)
            .transpose(),
        StateKey::CatalogOperation(id) => state
            .catalog_operations
            .get(id)
            .map(serde_json::to_vec)
            .transpose(),
        StateKey::LogGcWatermark {
            namespace,
            partition,
        } => state
            .log_gc_watermarks
            .get(namespace)
            .and_then(|partitions| partitions.get(partition))
            .map(serde_json::to_vec)
            .transpose(),
    }
}

/// Every entity key present in `state`.
pub(crate) fn all_keys(state: &ControllerState) -> Vec<StateKey> {
    let mut keys = vec![StateKey::Meta];
    keys.extend(state.shards.keys().copied().map(StateKey::Shard));
    keys.extend(state.fanout_outbox.keys().copied().map(StateKey::Fanout));
    keys.extend(state.catalog_records.keys().cloned().map(StateKey::Catalog));
    keys.extend(
        state
            .gc_watermarks
            .keys()
            .copied()
            .map(StateKey::GcWatermark),
    );
    keys.extend(state.rebalance.keys().copied().map(StateKey::Rebalance));
    keys.extend(
        state
            .catalog_operations
            .keys()
            .copied()
            .map(StateKey::CatalogOperation),
    );
    for (namespace, partitions) in &state.log_gc_watermarks {
        keys.extend(partitions.keys().map(|partition| StateKey::LogGcWatermark {
            namespace: namespace.clone(),
            partition: *partition,
        }));
    }
    keys
}

/// Rebuild the controller state from its stored entities. `Ok(None)` when no
/// controller state has been committed.
pub(crate) fn decode_state(
    entries: impl IntoIterator<Item = (String, Vec<u8>)>,
) -> Result<Option<ControllerState>, String> {
    let mut meta: Option<Meta> = None;
    let mut shards = HashMap::new();
    let mut fanout_outbox = BTreeMap::new();
    let mut catalog_records = BTreeMap::new();
    let mut gc_watermarks = HashMap::new();
    let mut rebalance = BTreeMap::new();
    let mut catalog_operations = BTreeMap::new();
    let mut log_gc_watermarks: BTreeMap<String, BTreeMap<u32, u64>> = BTreeMap::new();
    let mut entities = 0usize;
    for (key, value) in entries {
        let Some(state_key) = StateKey::decode(&key) else {
            return Err(format!("unrecognised controller state key {key}"));
        };
        let error = |error: serde_json::Error| format!("controller state key {key}: {error}");
        entities += 1;
        match state_key {
            StateKey::Meta => meta = Some(serde_json::from_slice(&value).map_err(error)?),
            StateKey::Shard(shard) => {
                shards.insert(shard, serde_json::from_slice(&value).map_err(error)?);
            }
            StateKey::Fanout(id) => {
                fanout_outbox.insert(id, serde_json::from_slice(&value).map_err(error)?);
            }
            StateKey::Catalog(catalog_key) => {
                catalog_records.insert(catalog_key, serde_json::from_slice(&value).map_err(error)?);
            }
            StateKey::GcWatermark(shard) => {
                gc_watermarks.insert(shard, serde_json::from_slice(&value).map_err(error)?);
            }
            StateKey::Rebalance(shard) => {
                rebalance.insert(shard, serde_json::from_slice(&value).map_err(error)?);
            }
            StateKey::CatalogOperation(id) => {
                catalog_operations.insert(id, serde_json::from_slice(&value).map_err(error)?);
            }
            StateKey::LogGcWatermark {
                namespace,
                partition,
            } => {
                log_gc_watermarks
                    .entry(namespace)
                    .or_default()
                    .insert(partition, serde_json::from_slice(&value).map_err(error)?);
            }
        }
    }
    let Some(meta) = meta else {
        return if entities == 0 {
            Ok(None)
        } else {
            Err("controller state entities exist without a meta record".into())
        };
    };
    Ok(Some(ControllerState {
        version: meta.version,
        cluster_id: meta.cluster_id,
        membership_generation: meta.membership_generation,
        nodes: meta.nodes,
        shards,
        catalog_epoch: meta.catalog_epoch,
        scheduler: meta.scheduler,
        fanout_outbox,
        data_nodes: meta.data_nodes,
        controller_voters: meta.controller_voters,
        replication_factor: meta.replication_factor,
        min_isr: meta.min_isr,
        catalog_records,
        gc_watermarks,
        rebalance,
        catalog_authoritative: meta.catalog_authoritative,
        catalog_frozen: meta.catalog_frozen,
        catalog_operations,
        log_gc_watermarks,
    }))
}
