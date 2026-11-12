//! Leader-driven follower repair and follower-side truncation fence.
//!
//! The hot path pushes each epoch to each peer once. When a push fails, is
//! rejected with a position conflict, or is acknowledged short, the leader
//! schedules one repair task per (stream, shard, peer). The task probes the
//! follower's log end, verifies that the follower's log agrees with the
//! leader's below that point (truncating a divergent tail when it does not),
//! then sends the missing range in order, awaiting each acknowledgement.
//! Only a repair that reaches the leader's log end marks the peer in sync
//! again; until then its hot-path acknowledgements do not count toward
//! quorum.

use crate::append::{
    ReplicateBatchAck, ReplicateBatchRequest, ReplicateCatchUpRequest, ReplicateError,
    ReplicationClient,
};
use broker_raft_meta::NodeConfig;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

/// Records per repair page.
const REPAIR_PAGE_RECORDS: usize = 1024;
/// Pages one repair run may send before giving up (the periodic wake retries).
const REPAIR_MAX_PAGES: usize = 100_000;
/// Failed repair runs retried before the task exits until the next wake.
const REPAIR_RETRIES: u32 = 5;

/// Body of a `409 Conflict` from `/internal/v1/replicate/batch`: the epoch did
/// not start at the follower's log end.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaPositionConflict {
    pub node_id: Uuid,
    pub partition: u32,
    pub leader_epoch: u64,
    /// The follower's log end (next offset it would accept).
    pub log_end: u64,
    /// First offset of the rejected epoch.
    pub want: u64,
}

/// `POST /internal/v1/replicate/truncate`: drop the follower's records at or
/// above `to_offset`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicateTruncateRequest {
    pub tenant_id: String,
    /// Replication stream (V1 topic, or empty for a V2 physical shard).
    pub topic: String,
    pub partition: u32,
    pub leader_id: Uuid,
    pub leader_epoch: u64,
    pub to_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicateTruncateResponse {
    pub node_id: Uuid,
    pub partition: u32,
    pub log_end: u64,
}

/// The leader's local log, as seen by repair.
pub trait ReplicaLogSource: Send + Sync + 'static {
    fn tenant(&self) -> String;
    fn shards(&self) -> Vec<u32> {
        Vec::new()
    }
    /// Replication streams that exist on `shard`: the V1 topics with data on
    /// that partition, or the single empty stream of a V2 physical shard.
    fn streams(&self, shard: u32) -> Vec<String>;
    /// Exclusive end of the local log, including records not yet fsynced.
    fn log_end(&self, stream: &str, shard: u32) -> Result<u64, String>;
    /// Encoded frames from `from` (ascending; purged offsets are absent).
    fn frames(
        &self,
        stream: &str,
        shard: u32,
        from: u64,
        max: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>, String>;
}

pub(crate) type RepairKey = (String, u32, Uuid);

#[derive(Default)]
pub(crate) struct RepairRegistry {
    pub(crate) source: OnceLock<Arc<dyn ReplicaLogSource>>,
    /// Running repairs; the flag asks the running task for another pass.
    active: Mutex<HashMap<RepairKey, bool>>,
}

impl RepairRegistry {
    pub(crate) fn is_active(&self, key: &RepairKey) -> bool {
        self.active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(key)
    }
}

enum BatchResult {
    Ack(ReplicateBatchAck),
    Conflict(ReplicaPositionConflict),
}

fn frame_id(bytes: &[u8]) -> Option<Uuid> {
    broker_proto::decode_frame(std::io::Cursor::new(bytes))
        .ok()
        .map(|(header, _)| header.id)
}

impl ReplicationClient {
    /// Give repair access to the local log. Without it, repair is disabled.
    pub fn set_log_source(&self, source: Arc<dyn ReplicaLogSource>) {
        let _ = self.repair.source.set(source);
    }

    pub fn repair_in_progress(&self, stream: &str, shard: u32, peer: Uuid) -> bool {
        self.repair.is_active(&(stream.to_string(), shard, peer))
    }

    /// Start (or re-run) repair of one stream on one peer. Single-flight per
    /// key: a request while a repair runs makes it take one more pass.
    pub fn schedule_repair(&self, stream: &str, shard: u32, peer: NodeConfig) {
        if self.repair.source.get().is_none() || tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let key: RepairKey = (stream.to_string(), shard, peer.id);
        {
            let mut active = self
                .repair
                .active
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(rerun) = active.get_mut(&key) {
                *rerun = true;
                return;
            }
            active.insert(key.clone(), false);
        }
        let client = self.clone();
        tokio::spawn(async move {
            let mut failures = 0u32;
            loop {
                let result = client.repair_once(&key.0, key.1, &peer).await;
                match &result {
                    Ok(_) => failures = 0,
                    Err(error) => {
                        failures += 1;
                        warn!(
                            peer = %peer.addr,
                            stream = %key.0,
                            shard = key.1,
                            %error,
                            "replica repair failed; peer stays out of sync"
                        );
                    }
                }
                let again = {
                    let mut active = client
                        .repair
                        .active
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let rerun = active.get(&key).copied().unwrap_or(false);
                    let retry = result.is_err() && failures < REPAIR_RETRIES;
                    if rerun || retry {
                        active.insert(key.clone(), false);
                        true
                    } else {
                        active.remove(&key);
                        false
                    }
                };
                if !again {
                    break;
                }
                if result.is_err() {
                    let backoff = 200u64.saturating_mul(1 << failures.min(5)).min(5_000);
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                }
            }
        });
    }

    /// Safety net for peers that missed pushes without producing a failure
    /// (for example while down): re-run repair for every out-of-sync stream
    /// of a shard this node leads.
    pub fn wake_repairs(&self) {
        let Some(runtime) = self.runtime.clone() else {
            return;
        };
        let Some(source) = self.repair.source.get() else {
            return;
        };
        let progress = self.replica_progress();
        // Unknown peers must be discovered too; absence is not evidence of an
        // in-sync prefix and otherwise an idle shard would never repair them.
        let shards: std::collections::BTreeSet<_> = source
            .shards()
            .into_iter()
            .chain(progress.iter().map(|entry| entry.partition))
            .chain(runtime.controller().into_iter().flat_map(|controller| {
                controller
                    .state()
                    .shards
                    .keys()
                    .copied()
                    .collect::<Vec<_>>()
            }))
            .collect();
        for shard in shards {
            if !runtime.is_recovery_leader_for_shard(shard) {
                continue;
            }
            for stream in source.streams(shard) {
                let Ok(end) = source.log_end(&stream, shard) else {
                    continue;
                };
                if end == 0 {
                    continue;
                }
                let targets = self.targets_for_shard(shard);
                for peer in targets.replicas.into_iter().chain(targets.learners) {
                    if !progress.iter().any(|entry| {
                        entry.partition == shard
                            && entry.topic == stream
                            && entry.peer_id == peer.id
                            && entry.in_sync
                            && entry.durable_hwm >= end
                    }) {
                        self.schedule_repair(&stream, shard, peer);
                    }
                }
            }
        }
    }

    /// One repair pass. `Ok(Some(end))` when the peer holds the leader's log
    /// through `end`; `Ok(None)` when this node no longer leads the shard.
    async fn repair_once(
        &self,
        stream: &str,
        shard: u32,
        peer: &NodeConfig,
    ) -> Result<Option<u64>, ReplicateError> {
        let Some(source) = self.repair.source.get().cloned() else {
            return Ok(None);
        };
        let Some(runtime) = self.runtime.clone() else {
            return Ok(None);
        };
        let epoch = runtime.shard_generation(shard);
        let still_leader = || {
            runtime.is_recovery_leader_for_shard(shard) && runtime.shard_generation(shard) == epoch
        };
        if !still_leader() {
            return Ok(None);
        }
        // Invalidate old maximum HWMs before probing or truncating. Every
        // acknowledgement below is tied to this exact repair revision.
        let session = self.begin_repair_progress(stream, shard, peer.id, epoch)?;
        let tenant = source.tenant();
        let mut follower_end = self.probe_follower(peer, &tenant, stream, shard).await?;
        let mut prefix_verified = false;
        for _ in 0..REPAIR_MAX_PAGES {
            if !still_leader() {
                return Ok(None);
            }
            let leader_end = source
                .log_end(stream, shard)
                .map_err(ReplicateError::InvalidEpoch)?;
            if !prefix_verified {
                let common = self
                    .common_prefix(
                        source.as_ref(),
                        peer,
                        &tenant,
                        stream,
                        shard,
                        follower_end,
                        leader_end,
                    )
                    .await?;
                if !still_leader() || !self.progress_fence_current(stream, shard, peer.id, session)
                {
                    return Ok(None);
                }
                if common < follower_end {
                    follower_end = self
                        .truncate_follower(peer, &tenant, stream, shard, epoch, common)
                        .await?;
                    if follower_end > common {
                        return Err(ReplicateError::InvalidEpoch(format!(
                            "follower log end {follower_end} is still above {common} after truncation"
                        )));
                    }
                }
                prefix_verified = true;
            }
            if follower_end >= leader_end {
                if self.mark_repaired(stream, shard, peer.id, leader_end, follower_end, session) {
                    info!(peer = %peer.addr, stream, shard, log_end = leader_end, "replica repaired");
                    return Ok(Some(follower_end));
                }
                continue;
            }
            let frames = source
                .frames(stream, shard, follower_end, REPAIR_PAGE_RECORDS)
                .map_err(ReplicateError::InvalidEpoch)?;
            let mut page = Vec::new();
            let mut bytes = 0usize;
            for (offset, frame) in frames {
                if offset != follower_end + page.len() as u64 {
                    break;
                }
                if !page.is_empty() && bytes.saturating_add(frame.len() + 64) > self.max_epoch_bytes
                {
                    break;
                }
                bytes = bytes.saturating_add(frame.len() + 64);
                page.push(frame);
            }
            if page.is_empty() {
                return Err(ReplicateError::InvalidEpoch(format!(
                    "leader log has no record at offset {follower_end} (purged or garbage-collected); the replica must be reseeded"
                )));
            }
            let request = ReplicateBatchRequest::new(
                tenant.clone(),
                stream.to_string(),
                shard,
                self.cluster.node_id,
                epoch,
                follower_end,
                page,
            )?;
            let sent_end = request.end_offset()?;
            match self.send_repair_batch(peer, request).await? {
                BatchResult::Ack(ack) => {
                    if !still_leader()
                        || !self.progress_fence_current(stream, shard, peer.id, session)
                    {
                        return Ok(None);
                    }
                    if ack.durable_hwm < sent_end {
                        return Err(ReplicateError::InvalidEpoch(format!(
                            "follower acknowledged {} below the repaired range end {sent_end}",
                            ack.durable_hwm
                        )));
                    }
                    // Only the range we sent is proven, never a follower's
                    // potentially divergent larger reported maximum.
                    follower_end = sent_end;
                    self.record_repair_progress(
                        stream,
                        shard,
                        peer.id,
                        leader_end,
                        follower_end,
                        session,
                    );
                }
                BatchResult::Conflict(conflict) => {
                    follower_end = conflict.log_end;
                    prefix_verified = false;
                }
            }
        }
        Err(ReplicateError::InvalidEpoch(
            "replica repair did not converge".into(),
        ))
    }

    /// Follower's durable log end, via the catch-up endpoint (a request from
    /// `u64::MAX` returns no frames, only the follower's durable HWM).
    async fn probe_follower(
        &self,
        peer: &NodeConfig,
        tenant: &str,
        stream: &str,
        shard: u32,
    ) -> Result<u64, ReplicateError> {
        let range = self
            .catch_up_from(
                peer.internal_base(),
                &ReplicateCatchUpRequest {
                    tenant_id: tenant.to_string(),
                    topic: stream.to_string(),
                    partition: shard,
                    from_offset: u64::MAX,
                    max_records: 1,
                },
            )
            .await?;
        Ok(range.committed_hwm)
    }

    async fn follower_ids(
        &self,
        peer: &NodeConfig,
        tenant: &str,
        stream: &str,
        shard: u32,
        from: u64,
        count: u64,
    ) -> Result<BTreeMap<u64, Uuid>, ReplicateError> {
        let mut ids = BTreeMap::new();
        let mut cursor = from;
        let end = from.saturating_add(count);
        while cursor < end {
            let range = self
                .catch_up_from(
                    peer.internal_base(),
                    &ReplicateCatchUpRequest {
                        tenant_id: tenant.to_string(),
                        topic: stream.to_string(),
                        partition: shard,
                        from_offset: cursor,
                        max_records: (end - cursor).min(4096) as usize,
                    },
                )
                .await?;
            let Some(last) = range.frames.last().map(|frame| frame.offset) else {
                break;
            };
            for frame in range.frames {
                if frame.offset < end {
                    if let Some(id) = frame_id(&frame.bytes) {
                        ids.insert(frame.offset, id);
                    }
                }
            }
            cursor = last.saturating_add(1);
        }
        Ok(ids)
    }

    /// Highest offset `c <= min(follower_end, leader_end)` such that both logs
    /// agree on every comparable record below `c`. Records missing on either
    /// side (purged) are skipped rather than treated as conflicts.
    #[allow(clippy::too_many_arguments)]
    async fn common_prefix(
        &self,
        source: &dyn ReplicaLogSource,
        peer: &NodeConfig,
        tenant: &str,
        stream: &str,
        shard: u32,
        follower_end: u64,
        leader_end: u64,
    ) -> Result<u64, ReplicateError> {
        let top = follower_end.min(leader_end);
        let mut cursor = 0;
        // A matching tail record is not proof of a matching prefix: a failed
        // repair may have appended the same later UUID after divergent middle
        // records. Inspect every retained comparable record, in bounded pages.
        // This costs a full retained-prefix scan when repairing, not on ingest.
        for _ in 0..REPAIR_MAX_PAGES {
            if cursor >= top {
                return Ok(top);
            }
            let leader: BTreeMap<u64, Uuid> = source
                .frames(stream, shard, cursor, REPAIR_PAGE_RECORDS)
                .map_err(ReplicateError::InvalidEpoch)?
                .into_iter()
                .take_while(|(offset, _)| *offset < top)
                .map(|(offset, bytes)| {
                    frame_id(&bytes).map(|id| (offset, id)).ok_or_else(|| {
                        ReplicateError::InvalidEpoch(
                            "invalid local frame during prefix verification".into(),
                        )
                    })
                })
                .collect::<Result<_, _>>()?;
            let (Some((&first, _)), Some((&last, _))) =
                (leader.first_key_value(), leader.last_key_value())
            else {
                return Ok(top);
            };
            if first < cursor {
                return Err(ReplicateError::InvalidEpoch(
                    "nonadvancing prefix scan".into(),
                ));
            }
            let follower = self
                .follower_ids(
                    peer,
                    tenant,
                    stream,
                    shard,
                    first,
                    last.saturating_add(1) - first,
                )
                .await?;
            for (offset, leader_id) in leader {
                if follower.get(&offset).is_some_and(|id| *id != leader_id) {
                    return Ok(offset);
                }
            }
            cursor = last.saturating_add(1);
        }
        Err(ReplicateError::InvalidEpoch(
            "prefix verification exceeded its bounded page budget; refusing unverified progress"
                .into(),
        ))
    }

    async fn truncate_follower(
        &self,
        peer: &NodeConfig,
        tenant: &str,
        stream: &str,
        shard: u32,
        epoch: u64,
        to_offset: u64,
    ) -> Result<u64, ReplicateError> {
        warn!(peer = %peer.addr, stream, shard, to_offset, "truncating divergent follower tail");
        let url = format!("{}/internal/v1/replicate/truncate", peer.internal_base());
        let request = ReplicateTruncateRequest {
            tenant_id: tenant.to_string(),
            topic: stream.to_string(),
            partition: shard,
            leader_id: self.cluster.node_id,
            leader_epoch: epoch,
            to_offset,
        };
        let response = Self::apply_cluster_secret(self.http.post(&url).json(&request))
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(ReplicateError::PeerRejected {
                peer: peer.addr.clone(),
                status: response.status(),
                detail: response.text().await.unwrap_or_default(),
            });
        }
        let response: ReplicateTruncateResponse = response.json().await?;
        if response.node_id != peer.id || response.partition != shard {
            return Err(ReplicateError::InvalidEpoch(
                "truncate response from an unexpected peer".into(),
            ));
        }
        Ok(response.log_end)
    }

    async fn send_repair_batch(
        &self,
        peer: &NodeConfig,
        request: ReplicateBatchRequest,
    ) -> Result<BatchResult, ReplicateError> {
        let encoded = request.encode_binary()?;
        let permits = u32::try_from(encoded.len()).map_err(|_| ReplicateError::EpochTooLarge {
            bytes: encoded.len(),
            limit: u32::MAX as usize,
        })?;
        let _permit = Arc::clone(&self.in_flight)
            .acquire_many_owned(permits)
            .await
            .map_err(|_| ReplicateError::Closed)?;
        let url = format!("{}/internal/v1/replicate/batch", peer.internal_base());
        let response = Self::apply_cluster_secret(
            self.http
                .post(&url)
                .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                .body(encoded),
        )
        .send()
        .await?;
        let status = response.status();
        if status == reqwest::StatusCode::CONFLICT {
            if let Ok(conflict) = response.json::<ReplicaPositionConflict>().await {
                if conflict.node_id == peer.id && conflict.partition == request.partition {
                    return Ok(BatchResult::Conflict(conflict));
                }
            }
            return Err(ReplicateError::PeerRejected {
                peer: peer.addr.clone(),
                status,
                detail: "unparseable position conflict".into(),
            });
        }
        if !status.is_success() {
            return Err(ReplicateError::PeerRejected {
                peer: peer.addr.clone(),
                status,
                detail: response.text().await.unwrap_or_default(),
            });
        }
        let ack: ReplicateBatchAck = response.json().await?;
        if ack.node_id != peer.id
            || ack.partition != request.partition
            || ack.leader_epoch != request.leader_epoch
        {
            return Err(ReplicateError::InvalidEpoch(
                "repair ack from an unexpected peer or epoch".into(),
            ));
        }
        Ok(BatchResult::Ack(ack))
    }
}

/// Follower-side fence for truncation requests. A follower never truncates
/// below what it acknowledged unless the request comes from a newer leader
/// epoch than the one it acknowledged under. The epoch is persisted whenever
/// it changes; the acknowledged HWM is kept in memory and, after a restart,
/// conservatively taken to be the follower's whole durable log.
#[derive(Default)]
pub struct FollowerFence {
    path: OnceLock<PathBuf>,
    state: Mutex<FenceState>,
}

#[derive(Default)]
struct FenceState {
    /// Persisted: last epoch acknowledged per stream/partition.
    epochs: BTreeMap<String, u64>,
    /// In memory: (epoch, highest acknowledged durable HWM) since start.
    acked: HashMap<String, (u64, u64)>,
}

#[derive(Serialize, Deserialize, Default)]
struct FenceFile {
    epochs: BTreeMap<String, u64>,
}

fn fence_key(stream: &str, partition: u32) -> String {
    format!("{stream}/{partition}")
}

impl FollowerFence {
    /// Persist the fence under `dir` and load what an earlier run recorded.
    pub fn open_dir(&self, dir: impl Into<PathBuf>) -> std::io::Result<()> {
        let path = dir.into().join("replica-fence.json");
        if path.exists() {
            let file: FenceFile = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            self.lock().epochs = file.epochs;
        }
        let _ = self.path.set(path);
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FenceState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Record an acknowledgement sent to the leader of `epoch`.
    pub fn record_ack(
        &self,
        stream: &str,
        partition: u32,
        epoch: u64,
        durable_hwm: u64,
    ) -> std::io::Result<()> {
        self.record_ack_checked(stream, partition, epoch, durable_hwm, || Ok(()))
    }

    /// Validate the original WAL incarnation under the same lock that protects
    /// truncation. A cut cannot slip between validation and the durable ACK.
    pub fn record_ack_checked(
        &self,
        stream: &str,
        partition: u32,
        epoch: u64,
        durable_hwm: u64,
        validate: impl FnOnce() -> Result<(), String>,
    ) -> std::io::Result<()> {
        let key = fence_key(stream, partition);
        let mut state = self.lock();
        validate().map_err(std::io::Error::other)?;
        if state.epochs.get(&key).is_some_and(|stored| *stored > epoch)
            || state
                .acked
                .get(&key)
                .is_some_and(|(stored, _)| *stored > epoch)
        {
            return Err(std::io::Error::other(
                "replica ACK belongs to an obsolete leader epoch",
            ));
        }
        let acked = state.acked.entry(key.clone()).or_insert((epoch, 0));
        if epoch > acked.0 {
            *acked = (epoch, durable_hwm);
        } else if epoch == acked.0 {
            acked.1 = acked.1.max(durable_hwm);
        }
        if state
            .epochs
            .get(&key)
            .is_some_and(|stored| *stored >= epoch)
        {
            return Ok(());
        }
        let Some(path) = self.path.get() else {
            state.epochs.insert(key, epoch);
            return Ok(());
        };
        let mut epochs = state.epochs.clone();
        epochs.insert(key.clone(), epoch);
        let bytes = serde_json::to_vec(&FenceFile { epochs }).map_err(std::io::Error::other)?;
        let tmp = path.with_extension("json.tmp");
        {
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        // Do not let a failed persist turn the next retry into an in-memory
        // no-op. Only a fully synced fence may satisfy the fast path above.
        state.epochs.insert(key, epoch);
        Ok(())
    }

    /// Whether a leader of `leader_epoch` may cut this follower's log at
    /// `to_offset`, given its current durable end `local_end`.
    pub fn check_truncate(
        &self,
        stream: &str,
        partition: u32,
        leader_epoch: u64,
        to_offset: u64,
        local_end: u64,
    ) -> Result<(), String> {
        let key = fence_key(stream, partition);
        let state = self.lock();
        Self::check_truncate_locked(
            &state,
            &key,
            stream,
            partition,
            leader_epoch,
            to_offset,
            local_end,
        )
    }

    /// Keep the ACK/truncate fence locked through the actual WAL transaction,
    /// not merely its preflight check.
    #[allow(clippy::too_many_arguments)]
    pub fn with_truncate<T, E>(
        &self,
        stream: &str,
        partition: u32,
        leader_epoch: u64,
        to_offset: u64,
        local_end: u64,
        cut: impl FnOnce() -> Result<T, E>,
    ) -> Result<Result<T, E>, String> {
        let state = self.lock();
        Self::check_truncate_locked(
            &state,
            &fence_key(stream, partition),
            stream,
            partition,
            leader_epoch,
            to_offset,
            local_end,
        )?;
        Ok(cut())
    }

    fn check_truncate_locked(
        state: &FenceState,
        key: &str,
        stream: &str,
        partition: u32,
        leader_epoch: u64,
        to_offset: u64,
        local_end: u64,
    ) -> Result<(), String> {
        let (epoch, acked_hwm) = match (state.acked.get(key), state.epochs.get(key)) {
            (Some(acked), _) => *acked,
            (None, Some(epoch)) => (*epoch, local_end),
            (None, None) => return Ok(()),
        };
        if to_offset >= acked_hwm || leader_epoch > epoch {
            return Ok(());
        }
        Err(format!(
            "refusing to truncate {stream}/{partition} to {to_offset}: offsets below {acked_hwm} were acknowledged under leader epoch {epoch} and the request is from epoch {leader_epoch}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::append::{ProgressUpdate, ReplicateCatchUpRange};
    use crate::CatchUpFrame;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use axum::routing::post;
    use axum::{Json, Router};
    use broker_proto::LogRecord;
    use broker_raft_meta::{ClusterConfig, ClusterRuntime};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn frame(id: Uuid) -> Vec<u8> {
        let record = LogRecord {
            id,
            tenant_id: "default".into(),
            topic: "orders".into(),
            routing_key: "rk".into(),
            idempotency_key: None,
            published_at_ms: 0,
            priority: 5,
            flow_parallelism: None,
            flow_key: None,
            flow_rate: None,
            flow_period_secs: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            flow_profile_id: None,
            destination_url: None,
            destination_secret: None,
            max_retries: 0,
            retry_backoff: None,
            http_method: None,
            http_headers_json: None,
            http_sign: None,
            payload_ref_json: None,
            dedup_inserted_at_ms: None,
        };
        broker_proto::encode_frame_vec(&record, b"payload").unwrap()
    }

    /// In-memory follower speaking the follower side of the protocol.
    struct Follower {
        id: Uuid,
        log: Mutex<Vec<Vec<u8>>>,
        batches: AtomicUsize,
        truncations: AtomicUsize,
        /// Batches to reject as if the follower's placement view were stale.
        reject_batches: AtomicUsize,
    }

    impl Follower {
        fn ids(&self) -> Vec<Uuid> {
            self.log
                .lock()
                .unwrap()
                .iter()
                .map(|bytes| frame_id(bytes).unwrap())
                .collect()
        }
    }

    async fn batch(State(follower): State<Arc<Follower>>, body: Bytes) -> Response {
        follower.batches.fetch_add(1, Ordering::SeqCst);
        if follower
            .reject_batches
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            return (
                StatusCode::BAD_REQUEST,
                "replication leader is not elected for shard 0",
            )
                .into_response();
        }
        let request = ReplicateBatchRequest::decode_binary(&body).unwrap();
        let mut log = follower.log.lock().unwrap();
        for (index, bytes) in request.frames.iter().enumerate() {
            let offset = request.first_offset as usize + index;
            let conflict = || {
                (
                    StatusCode::CONFLICT,
                    Json(ReplicaPositionConflict {
                        node_id: follower.id,
                        partition: request.partition,
                        leader_epoch: request.leader_epoch,
                        log_end: log.len() as u64,
                        want: offset as u64,
                    }),
                )
                    .into_response()
            };
            match offset.cmp(&log.len()) {
                std::cmp::Ordering::Less if frame_id(&log[offset]) == frame_id(bytes) => {}
                std::cmp::Ordering::Equal => log.push(bytes.clone()),
                _ => return conflict(),
            }
        }
        Json(ReplicateBatchAck {
            node_id: follower.id,
            partition: request.partition,
            leader_epoch: request.leader_epoch,
            durable_hwm: log.len() as u64,
        })
        .into_response()
    }

    async fn catch_up(
        State(follower): State<Arc<Follower>>,
        Json(request): Json<ReplicateCatchUpRequest>,
    ) -> Response {
        let log = follower.log.lock().unwrap();
        let end = log.len() as u64;
        let from = request.from_offset.min(end);
        let to = from.saturating_add(request.max_records as u64).min(end);
        let range = ReplicateCatchUpRange {
            partition: request.partition,
            leader_generation: 0,
            committed_hwm: end,
            base_offset: 0,
            frames: (from..to)
                .map(|offset| CatchUpFrame {
                    offset,
                    bytes: log[offset as usize].clone(),
                })
                .collect(),
        };
        (
            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
            range.encode_binary().unwrap(),
        )
            .into_response()
    }

    async fn truncate(
        State(follower): State<Arc<Follower>>,
        Json(request): Json<ReplicateTruncateRequest>,
    ) -> Json<ReplicateTruncateResponse> {
        follower.truncations.fetch_add(1, Ordering::SeqCst);
        let mut log = follower.log.lock().unwrap();
        log.truncate(request.to_offset as usize);
        Json(ReplicateTruncateResponse {
            node_id: follower.id,
            partition: request.partition,
            log_end: log.len() as u64,
        })
    }

    struct LeaderLog(Mutex<Vec<Vec<u8>>>);

    impl ReplicaLogSource for LeaderLog {
        fn shards(&self) -> Vec<u32> {
            vec![0]
        }
        fn tenant(&self) -> String {
            "default".into()
        }
        fn streams(&self, _shard: u32) -> Vec<String> {
            vec!["orders".into()]
        }
        fn log_end(&self, _stream: &str, _shard: u32) -> Result<u64, String> {
            Ok(self.0.lock().unwrap().len() as u64)
        }
        fn frames(
            &self,
            _stream: &str,
            _shard: u32,
            from: u64,
            max: usize,
        ) -> Result<Vec<(u64, Vec<u8>)>, String> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .iter()
                .enumerate()
                .skip(from as usize)
                .take(max)
                .map(|(offset, bytes)| (offset as u64, bytes.clone()))
                .collect())
        }
    }

    struct Harness {
        client: ReplicationClient,
        follower: Arc<Follower>,
        peer: NodeConfig,
        leader_log: Arc<LeaderLog>,
    }

    async fn harness(leader: Vec<Vec<u8>>, follower_log: Vec<Vec<u8>>) -> Harness {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let (leader_id, follower_id) = (Uuid::new_v4(), Uuid::new_v4());
        let follower = Arc::new(Follower {
            id: follower_id,
            log: Mutex::new(follower_log),
            batches: AtomicUsize::new(0),
            truncations: AtomicUsize::new(0),
            reject_batches: AtomicUsize::new(0),
        });
        let app = Router::new()
            .route("/internal/v1/replicate/batch", post(batch))
            .route("/internal/v1/replicate/catch-up", post(catch_up))
            .route("/internal/v1/replicate/truncate", post(truncate))
            .with_state(Arc::clone(&follower));
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let peer = NodeConfig {
            id: follower_id,
            addr,
            internal_url: None,
        };
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![
                NodeConfig {
                    id: leader_id,
                    addr: "http://leader.invalid".into(),
                    internal_url: None,
                },
                peer.clone(),
            ],
            node_id: leader_id,
            generation: 1,
            hash_version: 1,
        };
        let runtime = ClusterRuntime::from_config_only(config.clone());
        let now = chrono_now();
        runtime.record_self_alive(now);
        runtime.record_peer_alive(follower_id, now);
        assert!(runtime.is_leader_for_shard(0));
        let client = ReplicationClient::new(config).with_runtime(runtime);
        let leader_log = Arc::new(LeaderLog(Mutex::new(leader)));
        client.set_log_source(leader_log.clone());
        Harness {
            client,
            follower,
            peer,
            leader_log,
        }
    }

    fn chrono_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    async fn settle(harness: &Harness) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while harness
                .client
                .repair_in_progress("orders", 0, harness.peer.id)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("repair finished");
    }

    fn in_sync(harness: &Harness) -> bool {
        harness
            .client
            .replica_progress()
            .iter()
            .any(|entry| entry.peer_id == harness.peer.id && entry.in_sync)
    }

    fn ids(log: &[Vec<u8>]) -> Vec<Uuid> {
        log.iter().map(|bytes| frame_id(bytes).unwrap()).collect()
    }

    #[tokio::test]
    async fn elected_leader_repairs_followers_while_takeover_ingest_is_fenced() {
        let leader: Vec<_> = (0..8).map(|_| frame(Uuid::new_v4())).collect();
        let harness = harness(leader.clone(), leader[..3].to_vec()).await;
        let runtime = harness.client.runtime.as_ref().unwrap();
        let epoch = Uuid::new_v4();
        runtime.begin_shard_recovery(0, epoch, runtime.shard_generation(0));
        assert!(
            !runtime.is_recovery_leader_for_shard(0),
            "repair cannot race catch-up truncation"
        );
        assert!(runtime.mark_shard_verifying(0, epoch, runtime.shard_generation(0)));
        assert!(!runtime.is_leader_for_shard(0));
        assert!(runtime.is_recovery_leader_for_shard(0));
        harness
            .client
            .schedule_repair("orders", 0, harness.peer.clone());
        settle(&harness).await;
        assert_eq!(harness.follower.ids(), ids(&leader));
        assert!(in_sync(&harness));
        assert!(
            !runtime.shard_ready(0),
            "repair itself must not bypass the takeover barrier"
        );
    }

    #[tokio::test]
    async fn out_of_order_epoch_converges_without_leadership_change() {
        let leader: Vec<_> = (0..10).map(|_| frame(Uuid::new_v4())).collect();
        let harness = harness(leader.clone(), leader[..5].to_vec()).await;
        // Epoch 7..10 arrives while 5..7 never did: the follower rejects it.
        let request = ReplicateBatchRequest::new(
            "default".into(),
            "orders".into(),
            0,
            harness.client.cluster.node_id,
            0,
            7,
            leader[7..].to_vec(),
        )
        .unwrap();
        harness.client.replicate_batch(request, 10).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        settle(&harness).await;
        assert_eq!(harness.follower.ids(), ids(&leader));
        assert!(in_sync(&harness));
        assert!(harness
            .client
            .counts_toward_quorum("orders", 0, harness.peer.id));
    }

    #[tokio::test]
    async fn batch_rejected_during_placement_change_converges_via_repair() {
        let leader: Vec<_> = (0..8).map(|_| frame(Uuid::new_v4())).collect();
        let harness = harness(leader.clone(), leader[..4].to_vec()).await;
        // The follower's controller view is settling: it rejects the hot push
        // and the first repair page, then accepts.
        harness.follower.reject_batches.store(2, Ordering::SeqCst);
        let request = ReplicateBatchRequest::new(
            "default".into(),
            "orders".into(),
            0,
            harness.client.cluster.node_id,
            0,
            4,
            leader[4..].to_vec(),
        )
        .unwrap();
        harness.client.replicate_batch(request, 8).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        settle(&harness).await;
        assert_eq!(harness.follower.ids(), ids(&leader));
        assert!(in_sync(&harness));
    }

    #[tokio::test]
    async fn timed_out_push_to_a_durable_follower_repairs_as_a_no_op() {
        let leader: Vec<_> = (0..6).map(|_| frame(Uuid::new_v4())).collect();
        let harness = harness(leader.clone(), leader.clone()).await;
        crate::append::update_progress(
            &harness.client.progress,
            ProgressUpdate {
                fence: Default::default(),
                stream: "orders",
                partition: 0,
                peer_id: harness.peer.id,
                leader_hwm: 6,
                durable_hwm: 0,
                success: false,
                may_mark_in_sync: true,
                log_end: None,
            },
        );
        assert!(!harness
            .client
            .counts_toward_quorum("orders", 0, harness.peer.id));
        harness
            .client
            .schedule_repair("orders", 0, harness.peer.clone());
        settle(&harness).await;
        assert_eq!(harness.follower.batches.load(Ordering::SeqCst), 0);
        assert_eq!(harness.follower.truncations.load(Ordering::SeqCst), 0);
        assert!(in_sync(&harness));
    }

    #[tokio::test]
    async fn divergent_follower_tail_is_truncated_then_repaired() {
        let leader: Vec<_> = (0..10).map(|_| frame(Uuid::new_v4())).collect();
        // Offsets 6..8 came from a deposed leader and were never committed.
        let mut stale = leader[..6].to_vec();
        stale.extend((0..2).map(|_| frame(Uuid::new_v4())));
        let harness = harness(leader[..8].to_vec(), stale).await;
        harness
            .client
            .schedule_repair("orders", 0, harness.peer.clone());
        settle(&harness).await;
        assert_eq!(harness.follower.truncations.load(Ordering::SeqCst), 1);
        assert_eq!(harness.follower.ids(), ids(&leader[..8]));
        // New leader writes after the repair keep flowing in order.
        harness
            .leader_log
            .0
            .lock()
            .unwrap()
            .extend(leader[8..].iter().cloned());
        harness
            .client
            .schedule_repair("orders", 0, harness.peer.clone());
        settle(&harness).await;
        assert_eq!(harness.follower.ids(), ids(&leader));
        assert!(in_sync(&harness));
    }

    #[tokio::test]
    async fn matching_tail_does_not_hide_a_divergent_middle_record() {
        let leader: Vec<_> = (0..10).map(|_| frame(Uuid::new_v4())).collect();
        let mut follower = leader.clone();
        follower[3] = frame(Uuid::new_v4());
        let harness = harness(leader.clone(), follower).await;
        harness
            .client
            .schedule_repair("orders", 0, harness.peer.clone());
        settle(&harness).await;
        assert_eq!(harness.follower.ids(), ids(&leader));
        assert_eq!(harness.follower.truncations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unknown_idle_peer_is_not_presumed_in_sync_and_is_discovered() {
        let leader: Vec<_> = (0..3).map(|_| frame(Uuid::new_v4())).collect();
        let harness = harness(leader.clone(), Vec::new()).await;
        assert!(!harness
            .client
            .counts_toward_quorum("orders", 0, harness.peer.id));
        harness.client.wake_repairs();
        settle(&harness).await;
        assert_eq!(harness.follower.ids(), ids(&leader));
        assert!(in_sync(&harness));
    }

    #[tokio::test]
    async fn new_repair_revision_rejects_late_maximum_and_completion_from_old_session() {
        let leader: Vec<_> = (0..3).map(|_| frame(Uuid::new_v4())).collect();
        let harness = harness(leader.clone(), leader).await;
        let old = harness
            .client
            .begin_repair_progress("orders", 0, harness.peer.id, 0)
            .unwrap();
        harness
            .client
            .record_repair_progress("orders", 0, harness.peer.id, 100, 100, old);
        let current = harness
            .client
            .begin_repair_progress("orders", 0, harness.peer.id, 0)
            .unwrap();
        assert_ne!(old, current);
        assert_eq!(harness.client.replica_progress()[0].durable_hwm, 0);
        assert!(!harness
            .client
            .mark_repaired("orders", 0, harness.peer.id, 100, 100, old));
        harness
            .client
            .record_repair_progress("orders", 0, harness.peer.id, 100, 100, old);
        assert!(harness
            .client
            .mark_repaired("orders", 0, harness.peer.id, 3, 3, current));
        let entry = &harness.client.replica_progress()[0];
        assert_eq!(entry.durable_hwm, 3);
        assert_eq!(entry.leader_hwm, 3);
        // Even a numerically identical leader term has a new recovery identity.
        let runtime = harness.client.runtime.as_ref().unwrap();
        runtime.begin_shard_recovery(0, Uuid::new_v4(), 0);
        assert_eq!(harness.client.replica_progress()[0].durable_hwm, 0);
        assert!(!harness
            .client
            .mark_repaired("orders", 0, harness.peer.id, 100, 100, current));
    }

    #[test]
    fn follower_cut_serializes_with_original_incarnation_ack_validation() {
        let fence = Arc::new(FollowerFence::default());
        let incarnation = Arc::new(AtomicUsize::new(0));
        let (started, cutting) = std::sync::mpsc::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let cut_fence = fence.clone();
        let cut_incarnation = incarnation.clone();
        let cut = std::thread::spawn(move || {
            cut_fence
                .with_truncate("orders", 0, 2, 0, 1, || {
                    started.send(()).unwrap();
                    blocked.recv().unwrap();
                    cut_incarnation.store(1, Ordering::SeqCst);
                    Ok::<_, ()>(())
                })
                .unwrap()
                .unwrap();
        });
        cutting.recv().unwrap();
        let ack_fence = fence.clone();
        let ack = std::thread::spawn(move || {
            ack_fence.record_ack_checked("orders", 0, 2, 1, || {
                if incarnation.load(Ordering::SeqCst) == 0 {
                    Ok(())
                } else {
                    Err("WAL rewritten".into())
                }
            })
        });
        release.send(()).unwrap();
        cut.join().unwrap();
        assert!(ack.join().unwrap().is_err(), "an ACK after the cut must validate the old append incarnation, not adopt the replacement");
        fence.record_ack("orders", 0, 2, 1).unwrap();
        assert!(fence
            .with_truncate("orders", 0, 2, 0, 1, || Ok::<_, ()>(()))
            .is_err());
    }

    #[test]
    fn failed_fence_persistence_cannot_make_the_retry_skip_its_durability_barrier() {
        let dir = tempfile::tempdir().unwrap();
        let fence = FollowerFence::default();
        fence.open_dir(dir.path()).unwrap();
        std::fs::create_dir(dir.path().join("replica-fence.json.tmp")).unwrap();
        for _ in 0..2 {
            assert!(fence.record_ack("orders", 0, 7, 1).is_err());
        }
        assert!(!dir.path().join("replica-fence.json").exists());
        std::fs::remove_dir(dir.path().join("replica-fence.json.tmp")).unwrap();
        fence.record_ack("orders", 0, 7, 1).unwrap();
        let restarted = FollowerFence::default();
        restarted.open_dir(dir.path()).unwrap();
        assert!(restarted.check_truncate("orders", 0, 7, 0, 1).is_err());
        assert!(restarted.record_ack("orders", 0, 6, 1).is_err());
    }

    #[tokio::test]
    async fn learner_counts_as_caught_up_only_after_repair_copies_everything() {
        let leader: Vec<_> = (0..5).map(|_| frame(Uuid::new_v4())).collect();
        let harness = harness(leader.clone(), Vec::new()).await;
        let streams = vec!["orders".to_string()];
        assert!(!harness
            .client
            .learner_caught_up(&streams, 0, harness.peer.id));
        // A learner's hot-path acks never mark it in sync.
        crate::append::update_progress(
            &harness.client.progress,
            ProgressUpdate {
                fence: Default::default(),
                stream: "orders",
                partition: 0,
                peer_id: harness.peer.id,
                leader_hwm: 5,
                durable_hwm: 5,
                success: true,
                may_mark_in_sync: false,
                log_end: None,
            },
        );
        assert!(!harness
            .client
            .learner_caught_up(&streams, 0, harness.peer.id));
        harness
            .client
            .schedule_repair("orders", 0, harness.peer.clone());
        settle(&harness).await;
        assert_eq!(harness.follower.ids(), ids(&leader));
        assert!(harness
            .client
            .learner_caught_up(&streams, 0, harness.peer.id));
        // New records on the leader make it lag again until repaired.
        harness
            .leader_log
            .0
            .lock()
            .unwrap()
            .push(frame(Uuid::new_v4()));
        assert!(!harness
            .client
            .learner_caught_up(&streams, 0, harness.peer.id));
    }

    #[test]
    fn follower_fence_refuses_truncating_acked_data_within_an_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let fence = FollowerFence::default();
        fence.open_dir(dir.path()).unwrap();
        fence.record_ack("", 3, 4, 100).unwrap();
        assert!(fence.check_truncate("", 3, 4, 90, 100).is_err());
        assert!(fence.check_truncate("", 3, 4, 100, 120).is_ok());
        assert!(fence.check_truncate("", 3, 5, 90, 100).is_ok());
        // After a restart the acknowledged HWM is unknown: the whole durable
        // log is treated as acknowledged under the persisted epoch.
        let restarted = FollowerFence::default();
        restarted.open_dir(dir.path()).unwrap();
        assert!(restarted.check_truncate("", 3, 4, 110, 120).is_err());
        assert!(restarted.check_truncate("", 3, 5, 0, 120).is_ok());
        assert!(restarted.check_truncate("", 9, 1, 0, 120).is_ok());
    }
}
