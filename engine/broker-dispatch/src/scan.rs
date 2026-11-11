//! Incremental pending-work index for backfill and fleet claims.
//!
//! Each `(topic, partition)` keeps a *floor* (every offset below it is
//! delivered or dead-lettered), a *scanned-to* position, and the set of
//! offsets in `[floor, scanned_to)` that are still pending. A refresh only
//! reads records past `scanned_to`, bounded by a record budget, so steady-state
//! backfill and claims cost O(new records) instead of O(retained history).
//! Completions remove entries directly ([`DispatchScan::mark_done`]); a
//! periodic recheck catches completions made by other processes.
//!
//! Floors persist in `dispatch-scan-floors.json` under the data dir so a
//! restart resumes from the last floor. A floor is only ever a lower bound of
//! finished work: losing or lowering it costs a rescan, never a skipped message.

use crate::flow_control::FlowKey;
use broker_partition::{Broker, BrokerError};
use broker_storage::StoredMessage;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Most pending offsets tracked per `(topic, partition)`. Scanning pauses
/// (backpressure) until delivery drains the index below this.
pub(crate) const PENDING_CAP: usize = 4_096;
/// Re-check tracked entries against durable completion state this often.
const RECHECK_EVERY: Duration = Duration::from_secs(30);
/// Persist advanced floors at most this often.
const PERSIST_EVERY: Duration = Duration::from_secs(5);
/// Records read per storage call while extending a scan.
const READ_CHUNK: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingEntry {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub message_id: Uuid,
    pub cursor_key: String,
    pub tenant_id: String,
    pub published_at_ms: i64,
    /// Strict-FIFO flow key: retries of earlier offsets with this key fence it.
    pub fifo_key: Option<String>,
    pub queue_id: Option<Uuid>,
    pub group_member_id: Option<Uuid>,
    pub host_key: Option<String>,
    /// Flow lane and limits frozen on the message, when it is flow-controlled.
    pub flow: Option<FlowAdmission>,
}

/// Flow-control lane of a pending message (same key the local FlowController uses).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlowAdmission {
    pub key: FlowKey,
    pub parallelism: u32,
    pub rate: u32,
    pub period_secs: u64,
}

impl FlowAdmission {
    pub(crate) fn for_message(msg: &StoredMessage) -> Option<Self> {
        if !broker_partition::delivery_uses_flow_control(msg) {
            return None;
        }
        let limits = broker_partition::ResolvedFlow::for_delivery(&msg.routing_key, msg);
        Some(Self {
            key: FlowKey {
                endpoint_id: broker_partition::flow_lane_owner(msg),
                flow_key: limits.key,
            },
            parallelism: limits.parallelism.max(1),
            rate: limits.rate,
            period_secs: limits.period_secs.max(1),
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct PendingView {
    pub entries: Vec<Arc<PendingEntry>>,
    /// Every committed record has been examined.
    pub caught_up: bool,
}

struct PartitionScan {
    floor: u64,
    scanned_to: u64,
    pending: BTreeMap<u64, Arc<PendingEntry>>,
    fifo_offsets: HashMap<(String, String), BTreeSet<u64>>,
    last_recheck: Instant,
}

impl PartitionScan {
    fn starting_at(floor: u64) -> Self {
        Self {
            floor,
            scanned_to: floor,
            pending: BTreeMap::new(),
            fifo_offsets: HashMap::new(),
            last_recheck: Instant::now(),
        }
    }

    fn index_fifo(&mut self, entry: &PendingEntry) {
        if let Some(flow) = &entry.fifo_key {
            self.fifo_offsets
                .entry((entry.cursor_key.clone(), flow.clone()))
                .or_default()
                .insert(entry.offset);
        }
    }

    fn remove(&mut self, offset: u64) {
        if let Some(entry) = self.pending.remove(&offset) {
            if let Some(flow) = &entry.fifo_key {
                let key = (entry.cursor_key.clone(), flow.clone());
                if let Some(offsets) = self.fifo_offsets.get_mut(&key) {
                    offsets.remove(&offset);
                    if offsets.is_empty() {
                        self.fifo_offsets.remove(&key);
                    }
                }
            }
        }
    }
}

type ScanKey = (String, u32);

#[derive(Debug, Default, Serialize, Deserialize)]
struct FloorFile {
    floors: Vec<FloorRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FloorRecord {
    topic: String,
    partition: u32,
    floor: u64,
}

struct FloorStore {
    path: Option<PathBuf>,
    floors: HashMap<ScanKey, u64>,
    dirty: bool,
    last_persist: Instant,
}

impl FloorStore {
    fn open(data_dir: Option<&Path>) -> Self {
        let path = data_dir.map(|dir| dir.join("dispatch-scan-floors.json"));
        let mut floors = HashMap::new();
        if let Some(path) = path.as_ref().filter(|p| p.exists()) {
            match std::fs::read(path)
                .map_err(|e| e.to_string())
                .and_then(|bytes| {
                    serde_json::from_slice::<FloorFile>(&bytes).map_err(|e| e.to_string())
                }) {
                Ok(file) => {
                    for record in file.floors {
                        floors.insert((record.topic, record.partition), record.floor);
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, path = %path.display(), "ignoring unreadable dispatch scan floors; rescanning");
                }
            }
        }
        Self {
            path,
            floors,
            dirty: false,
            last_persist: Instant::now(),
        }
    }

    fn get(&self, key: &ScanKey) -> u64 {
        self.floors.get(key).copied().unwrap_or(0)
    }

    fn set(&mut self, key: ScanKey, floor: u64) {
        if self.floors.get(&key) != Some(&floor) {
            self.floors.insert(key, floor);
            self.dirty = true;
        }
    }

    fn remove_partitions(&mut self, partitions: &[u32]) {
        let before = self.floors.len();
        self.floors.retain(|(_, p), _| !partitions.contains(p));
        if self.floors.len() != before {
            self.dirty = true;
            self.last_persist = Instant::now() - PERSIST_EVERY;
        }
    }

    fn maybe_persist(&mut self) {
        if !self.dirty || self.last_persist.elapsed() < PERSIST_EVERY {
            return;
        }
        self.last_persist = Instant::now();
        let Some(path) = &self.path else {
            self.dirty = false;
            return;
        };
        let mut floors: Vec<FloorRecord> = self
            .floors
            .iter()
            .map(|((topic, partition), floor)| FloorRecord {
                topic: topic.clone(),
                partition: *partition,
                floor: *floor,
            })
            .collect();
        floors.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
        let write = serde_json::to_vec(&FloorFile { floors })
            .map_err(std::io::Error::other)
            .and_then(|bytes| broker_storage::atomic_write_file(path, &bytes));
        match write {
            Ok(()) => self.dirty = false,
            Err(error) => tracing::warn!(%error, "persist dispatch scan floors failed"),
        }
    }
}

#[derive(Clone)]
pub(crate) struct DispatchScan {
    partitions: Arc<Mutex<HashMap<ScanKey, Arc<Mutex<PartitionScan>>>>>,
    floors: Arc<Mutex<FloorStore>>,
}

/// Tenant used for dispatch cursors of `msg` (matches delivery and completion).
pub(crate) fn dispatch_tenant(broker: &Broker, msg: &StoredMessage) -> String {
    if msg.tenant_id.is_empty() {
        broker.tenant()
    } else {
        msg.tenant_id.clone()
    }
}

/// Lane cursors looked up once per refresh.
struct CursorCache<'a> {
    broker: &'a Broker,
    partition: u32,
    cursors: HashMap<(String, String), u64>,
}

impl<'a> CursorCache<'a> {
    fn new(broker: &'a Broker, partition: u32) -> Self {
        Self {
            broker,
            partition,
            cursors: HashMap::new(),
        }
    }

    fn is_done(
        &mut self,
        topic: &str,
        tenant: &str,
        cursor_key: &str,
        offset: u64,
        message_id: Uuid,
    ) -> bool {
        let partition = self.partition;
        let broker = self.broker;
        let cursor = *self
            .cursors
            .entry((tenant.to_string(), cursor_key.to_string()))
            .or_insert_with(|| {
                broker
                    .dispatch_offset(tenant, cursor_key, partition)
                    .unwrap_or(0)
            });
        offset < cursor
            || broker
                .is_delivery_complete(topic, tenant, cursor_key, partition, offset, message_id)
                .unwrap_or(false)
    }
}

impl DispatchScan {
    pub(crate) fn open(data_dir: Option<&Path>) -> Self {
        Self {
            partitions: Arc::new(Mutex::new(HashMap::new())),
            floors: Arc::new(Mutex::new(FloorStore::open(data_dir))),
        }
    }

    fn partition_scan(&self, topic: &str, partition: u32) -> Arc<Mutex<PartitionScan>> {
        let key = (topic.to_string(), partition);
        let mut map = self.partitions.lock();
        map.entry(key.clone())
            .or_insert_with(|| {
                Arc::new(Mutex::new(PartitionScan::starting_at(
                    self.floors.lock().get(&key),
                )))
            })
            .clone()
    }

    /// Bring the index for `(topic, partition)` up to date, reading at most
    /// `record_budget` new records, and return the pending offsets in order.
    pub(crate) fn refresh(
        &self,
        broker: &Broker,
        topic: &str,
        partition: u32,
        record_budget: usize,
    ) -> Result<PendingView, BrokerError> {
        // Only quorum-visible records are indexed (dispatch gate).
        let hwm = broker.dispatch_hwm(topic, partition)?;
        let scan = self.partition_scan(topic, partition);
        let mut scan = scan.lock();
        if hwm < scan.scanned_to {
            // The log no longer reaches what we scanned (rewritten during
            // catch-up or replaced): start over.
            *scan = PartitionScan::starting_at(0);
        }
        let mut cursors = CursorCache::new(broker, partition);
        if scan.last_recheck.elapsed() >= RECHECK_EVERY {
            scan.pending.retain(|_, entry| {
                !cursors.is_done(
                    &entry.topic,
                    &entry.tenant_id,
                    &entry.cursor_key,
                    entry.offset,
                    entry.message_id,
                )
            });
            scan.fifo_offsets.clear();
            let entries: Vec<_> = scan.pending.values().cloned().collect();
            for entry in entries {
                scan.index_fifo(&entry);
            }
            scan.last_recheck = Instant::now();
        }

        let mut budget = record_budget;
        while scan.scanned_to < hwm && budget > 0 && scan.pending.len() < PENDING_CAP {
            let chunk = budget.min(READ_CHUNK).min(PENDING_CAP - scan.pending.len());
            let (messages, next) =
                broker.scan_topic_metadata(topic, partition, scan.scanned_to, chunk)?;
            if next <= scan.scanned_to {
                // Nothing readable at or after this offset below the watermark.
                scan.scanned_to = hwm;
                break;
            }
            budget = budget.saturating_sub(chunk);
            let mut stopped_at = None;
            for msg in messages {
                if msg.offset >= hwm {
                    stopped_at = Some(msg.offset);
                    break;
                }
                let cursor_key = broker_partition::flow_lane_owner(&msg).to_string();
                let tenant_id = dispatch_tenant(broker, &msg);
                if !cursors.is_done(topic, &tenant_id, &cursor_key, msg.offset, msg.id) {
                    let entry = Arc::new(PendingEntry {
                        topic: topic.to_string(),
                        partition,
                        offset: msg.offset,
                        message_id: msg.id,
                        cursor_key,
                        tenant_id,
                        published_at_ms: msg.published_at_ms,
                        fifo_key: crate::worker::DispatchEngine::fifo_retry_key(&msg),
                        queue_id: msg.queue_id,
                        group_member_id: msg.group_member_id,
                        host_key: msg
                            .destination_url
                            .as_deref()
                            .and_then(crate::host_blocker::HostBlocker::host_key),
                        flow: FlowAdmission::for_message(&msg),
                    });
                    scan.index_fifo(&entry);
                    scan.pending.insert(msg.offset, entry);
                }
            }
            scan.scanned_to = stopped_at.unwrap_or(next).min(hwm).max(scan.scanned_to);
        }

        let floor = scan
            .pending
            .keys()
            .next()
            .copied()
            .unwrap_or(scan.scanned_to);
        scan.floor = floor;
        let caught_up = scan.scanned_to >= hwm;
        let entries = scan.pending.values().cloned().collect();
        drop(scan);
        let mut floors = self.floors.lock();
        floors.set((topic.to_string(), partition), floor);
        floors.maybe_persist();
        Ok(PendingView { entries, caught_up })
    }

    /// Forget a pending offset once it is delivered or dead-lettered.
    pub(crate) fn mark_done(&self, topic: &str, partition: u32, offset: u64) {
        let scan = self
            .partitions
            .lock()
            .get(&(topic.to_string(), partition))
            .cloned();
        if let Some(scan) = scan {
            scan.lock().remove(offset);
        }
    }

    /// A bounded admission queue may drop an earlier live job. Later arrivals
    /// must not overtake that durable predecessor, even if hydration tasks
    /// finish out of order or the earlier job has not reached a flow waitlist.
    pub(crate) fn is_fifo_head(
        &self,
        broker: &Broker,
        msg: &StoredMessage,
    ) -> Result<bool, BrokerError> {
        let Some(flow) = crate::worker::DispatchEngine::fifo_retry_key(msg) else {
            return Ok(true);
        };
        let scan = self.partition_scan(&msg.topic, msg.partition);
        if scan.lock().scanned_to <= msg.offset {
            self.refresh(broker, &msg.topic, msg.partition, PENDING_CAP)?;
        }
        let scan = scan.lock();
        if scan.scanned_to <= msg.offset {
            return Ok(false);
        }
        let lane = broker_partition::flow_lane_owner(msg).to_string();
        Ok(scan
            .fifo_offsets
            .get(&(lane, flow))
            .and_then(|offsets| offsets.first())
            .is_none_or(|head| *head >= msg.offset))
    }

    /// Drop all scan state for `partitions` (e.g. after leadership moved and
    /// the local log was caught up from peers).
    pub(crate) fn reset_partitions(&self, partitions: &[u32]) {
        self.partitions
            .lock()
            .retain(|(_, p), _| !partitions.contains(p));
        let mut floors = self.floors.lock();
        floors.remove_partitions(partitions);
        floors.maybe_persist();
    }

    #[cfg(test)]
    fn floor(&self, topic: &str, partition: u32) -> u64 {
        self.floors.lock().get(&(topic.to_string(), partition))
    }

    #[cfg(test)]
    fn flush(&self) {
        let mut floors = self.floors.lock();
        floors.last_persist = Instant::now() - PERSIST_EVERY;
        floors.maybe_persist();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_partition::{BrokerConfig, PublishRequest};

    fn publish(broker: &Broker, body: &str) -> (String, u32, u64) {
        let response = broker
            .publish(PublishRequest {
                topic: String::new(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: "scan".into(),
                payload: body.into(),
                routing_id: None,
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: None,
                flow_id: None,
                url: Some("https://example.com/hook".into()),
                secret: Some("secret".into()),
                destination: None,
                flow: None,
                parallelism: None,
                max_retries: None,
                retry_backoff: None,
                method: None,
                headers: None,
                sign: None,
                request: None,
            })
            .unwrap();
        (
            response.topic,
            response.partition.unwrap(),
            response.offset.unwrap(),
        )
    }

    #[test]
    fn refresh_reads_only_new_records_and_floor_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let mut offsets = Vec::new();
        for i in 0..10 {
            offsets.push(publish(&broker, &format!("m{i}")));
        }
        broker.flush_wal().unwrap();
        let (topic, partition, _) = offsets[0].clone();
        let tenant = broker.tenant();
        let scan = DispatchScan::open(Some(dir.path()));

        // A tiny budget pages through the backlog across refreshes.
        let first = scan.refresh(&broker, &topic, partition, 4).unwrap();
        assert!(!first.caught_up);
        assert_eq!(first.entries.len(), 4);
        let mut view = first;
        while !view.caught_up {
            view = scan.refresh(&broker, &topic, partition, 4).unwrap();
        }
        assert_eq!(view.entries.len(), 10);

        // Deliver the first seven; the floor moves to the first pending offset.
        let lane = view.entries[0].cursor_key.clone();
        for entry in &view.entries[..7] {
            broker
                .mark_dispatch_complete(&tenant, &lane, partition, entry.offset)
                .unwrap();
            scan.mark_done(&topic, partition, entry.offset);
        }
        let view = scan.refresh(&broker, &topic, partition, 4).unwrap();
        assert_eq!(view.entries.len(), 3);
        assert_eq!(scan.floor(&topic, partition), offsets[7].2);
        scan.flush();

        // After a restart the scan resumes at the persisted floor.
        let reopened = DispatchScan::open(Some(dir.path()));
        let view = reopened.refresh(&broker, &topic, partition, 3).unwrap();
        assert!(
            view.caught_up,
            "only the three records above the floor are read"
        );
        assert_eq!(
            view.entries.iter().map(|e| e.offset).collect::<Vec<_>>(),
            offsets[7..].iter().map(|o| o.2).collect::<Vec<_>>()
        );
    }

    #[test]
    fn completions_by_other_processes_are_rechecked() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let (topic, partition, offset) = publish(&broker, "x");
        broker.flush_wal().unwrap();
        let scan = DispatchScan::open(None);
        let view = scan.refresh(&broker, &topic, partition, 64).unwrap();
        assert_eq!(view.entries.len(), 1);
        let lane = view.entries[0].cursor_key.clone();
        broker
            .set_dispatch_offset(&broker.tenant(), &lane, partition, offset + 1)
            .unwrap();
        scan.partition_scan(&topic, partition).lock().last_recheck = Instant::now() - RECHECK_EVERY;
        let view = scan.refresh(&broker, &topic, partition, 64).unwrap();
        assert!(view.entries.is_empty());
        assert_eq!(scan.floor(&topic, partition), offset + 1);
    }

    #[test]
    fn reset_drops_floors_for_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let (topic, partition, offset) = publish(&broker, "x");
        broker.flush_wal().unwrap();
        let scan = DispatchScan::open(None);
        let view = scan.refresh(&broker, &topic, partition, 64).unwrap();
        let lane = view.entries[0].cursor_key.clone();
        broker
            .mark_dispatch_complete(&broker.tenant(), &lane, partition, offset)
            .unwrap();
        scan.mark_done(&topic, partition, offset);
        scan.refresh(&broker, &topic, partition, 64).unwrap();
        assert_eq!(scan.floor(&topic, partition), offset + 1);
        scan.reset_partitions(&[partition]);
        assert_eq!(scan.floor(&topic, partition), 0);
    }
}
