//! Persistent-client, bounded binary replication of append-only log epochs.

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use broker_raft_meta::ClusterConfig;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::{debug, warn};
use uuid::Uuid;

type SharedReplicaProgress = Arc<Mutex<HashMap<(String, u32, Uuid), ReplicaProgress>>>;

/// Progress for a stream that has seen no replication traffic for this long
/// is dropped (deleted topics, departed peers).
const PROGRESS_RETENTION_MS: u64 = 60 * 60 * 1000;

const EPOCH_MAGIC: [u8; 4] = *b"BMQE";
const EPOCH_VERSION: u16 = 1;
const FIXED_HEADER_LEN: usize = 64;
const DEFAULT_MAX_EPOCH_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_IN_FLIGHT_BYTES: usize = 64 * 1024 * 1024;
const MAX_DECODE_EPOCH_BYTES: usize = 64 * 1024 * 1024;
const MAX_EPOCH_RECORDS: usize = 65_536;
const CATCHUP_MAGIC: [u8; 4] = *b"BMQC";
const CATCHUP_VERSION: u16 = 1;
const CATCHUP_HEADER_LEN: usize = 32;
/// v2 appends the source's retained `base_offset`; only sent when non-zero so
/// peers that never collected a prefix still speak v1.
const CATCHUP_VERSION_BASE: u16 = 2;
const CATCHUP_HEADER_LEN_BASE: usize = 40;
const MAX_CATCHUP_RECORDS: usize = 4096;

/// Bytes an epoch header may spend on its tenant and topic (both are
/// length-prefixed with a u16).
const EPOCH_NAME_BUDGET: usize = 2 * u16::MAX as usize;
/// Fixed bytes of one broker-proto frame around its header and payload:
/// magic, header length, payload length and CRC.
const FRAME_FIXED_BYTES: usize = 16;
/// Per-frame length prefix inside an epoch.
const EPOCH_FRAME_PREFIX: usize = 4;

/// Replication epoch cap (`BETTERMQ_MAX_REPLICATION_EPOCH_BYTES`, 16 MiB).
pub fn configured_max_epoch_bytes() -> usize {
    env_bytes(
        "BETTERMQ_MAX_REPLICATION_EPOCH_BYTES",
        DEFAULT_MAX_EPOCH_BYTES,
    )
    .min(Semaphore::MAX_PERMITS)
}

/// Replication in-flight cap (`BETTERMQ_MAX_REPLICATION_IN_FLIGHT_BYTES`).
fn configured_max_in_flight_bytes() -> usize {
    env_bytes(
        "BETTERMQ_MAX_REPLICATION_IN_FLIGHT_BYTES",
        DEFAULT_MAX_IN_FLIGHT_BYTES,
    )
    .min(Semaphore::MAX_PERMITS)
}

/// Bytes of every cell body budget reserved for the dead-letter envelope.
/// A DLQ record wraps the original body with metadata (source queue, message
/// id, destination, reason, headers), and it must still fit one replication
/// epoch, so any body accepted at ingest stays dead-letterable. The DLQ
/// writer must keep its envelope overhead within this allowance.
pub const DLQ_ENVELOPE_ALLOWANCE: usize = 64 * 1024;

/// Body-size policy of a local-WAL multi-node cell. Bodies travel inline in
/// their WAL frame, so a worst-case frame (largest header) must fit one
/// replication epoch, which must fit the receiver's decode cap and the
/// internal HTTP body limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellBodyLimits {
    pub max_epoch_bytes: usize,
    /// Largest frame payload one epoch can carry with a worst-case header
    /// (what a DLQ record may use).
    pub max_frame_body: usize,
    /// Largest message body (decoded bytes) the cell accepts at ingest:
    /// `max_frame_body - DLQ_ENVELOPE_ALLOWANCE`.
    pub max_inline_body: usize,
}

/// The single source of the cell body limit (`cell_max_inline_body`).
/// Errors when the configuration cannot replicate safely; cell nodes must
/// refuse to start then.
pub fn cell_body_limits(
    internal_body_limit: usize,
    frame_payload_limit: usize,
) -> Result<CellBodyLimits, String> {
    cell_body_limits_for(
        configured_max_epoch_bytes(),
        internal_body_limit,
        frame_payload_limit,
    )
}

pub fn cell_body_limits_for(
    max_epoch_bytes: usize,
    internal_body_limit: usize,
    frame_payload_limit: usize,
) -> Result<CellBodyLimits, String> {
    if max_epoch_bytes > MAX_DECODE_EPOCH_BYTES {
        return Err(format!(
            "BETTERMQ_MAX_REPLICATION_EPOCH_BYTES={max_epoch_bytes} exceeds the replication decode cap of {MAX_DECODE_EPOCH_BYTES} bytes"
        ));
    }
    if max_epoch_bytes > internal_body_limit {
        return Err(format!(
            "BETTERMQ_MAX_REPLICATION_EPOCH_BYTES={max_epoch_bytes} exceeds the internal HTTP body limit of {internal_body_limit} bytes (BETTERMQ_MAX_HTTP_BODY_BYTES)"
        ));
    }
    let overhead = FIXED_HEADER_LEN
        + EPOCH_NAME_BUDGET
        + EPOCH_FRAME_PREFIX
        + FRAME_FIXED_BYTES
        + broker_proto::record::MAX_FRAME_HEADER_BYTES;
    let max_frame_body = max_epoch_bytes
        .saturating_sub(overhead)
        .min(frame_payload_limit);
    let max_inline_body = max_frame_body
        .checked_sub(DLQ_ENVELOPE_ALLOWANCE)
        .filter(|limit| *limit > 0)
        .ok_or_else(|| {
            format!(
                "BETTERMQ_MAX_REPLICATION_EPOCH_BYTES={max_epoch_bytes} leaves no room for a message body (needs more than {} bytes of frame, epoch and DLQ-envelope overhead)",
                overhead + DLQ_ENVELOPE_ALLOWANCE
            )
        })?;
    Ok(CellBodyLimits {
        max_epoch_bytes,
        max_frame_body,
        max_inline_body,
    })
}

/// Split consecutive frames into epochs that each encode within
/// `max_epoch_bytes` and [`MAX_EPOCH_RECORDS`]. Every range holds at least one
/// frame; a single frame larger than the budget gets a range of its own (and
/// is rejected by the sender), which pre-append body checks prevent.
pub fn epoch_frame_ranges(
    frame_lens: &[usize],
    tenant_len: usize,
    topic_len: usize,
    max_epoch_bytes: usize,
) -> Vec<std::ops::Range<usize>> {
    let header = FIXED_HEADER_LEN + tenant_len + topic_len;
    let mut ranges = Vec::new();
    let mut start = 0usize;
    let mut bytes = header;
    for (index, len) in frame_lens.iter().enumerate() {
        let cost = EPOCH_FRAME_PREFIX + len;
        let count = index - start;
        if count > 0 && (bytes + cost > max_epoch_bytes || count >= MAX_EPOCH_RECORDS) {
            ranges.push(start..index);
            start = index;
            bytes = header;
        }
        bytes += cost;
    }
    if start < frame_lens.len() {
        ranges.push(start..frame_lens.len());
    }
    ranges
}

/// Byte budget for frames in one catch-up page (stays below the decoder cap).
pub fn max_catchup_page_bytes() -> usize {
    MAX_DECODE_EPOCH_BYTES - CATCHUP_HEADER_LEN_BASE - MAX_CATCHUP_RECORDS * 12
}

#[derive(Debug, Error)]
pub enum ReplicateError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("quorum not reached: {acked}/{quorum}")]
    QuorumNotReached { acked: usize, quorum: usize },
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid replication epoch: {0}")]
    InvalidEpoch(String),
    #[error("replication epoch is too large: {bytes} bytes (limit {limit})")]
    EpochTooLarge { bytes: usize, limit: usize },
    #[error("peer {peer} rejected replication: {status} {detail}")]
    PeerRejected {
        peer: String,
        status: reqwest::StatusCode,
        detail: String,
    },
    #[error("local WAL is not durable through offset {required}: durable hwm is {actual}")]
    LocalNotDurable { required: u64, actual: u64 },
    #[error("replication in-flight byte semaphore closed")]
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateAppendRequest {
    pub tenant_id: String,
    pub topic: String,
    pub partition: u32,
    /// Leader-assigned log offset for this frame (quorum-atomic agreement).
    #[serde(default)]
    pub offset: u64,
    /// Base64-encoded partition log frame (magic + header + payload + crc).
    pub frame_b64: String,
    pub leader_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicateBatchRequest {
    pub tenant_id: String,
    /// Logical topic for a V1/single-topic epoch. Empty for a V2 physical-shard
    /// epoch whose frames carry multiple logical topics.
    pub topic: String,
    pub partition: u32,
    pub leader_id: Uuid,
    pub leader_epoch: u64,
    pub first_offset: u64,
    pub record_count: u32,
    /// Raw broker-proto frames. The primary transport encodes these without base64.
    pub frames: Vec<Vec<u8>>,
    pub batch_crc: u32,
}

impl ReplicateBatchRequest {
    pub fn new(
        tenant_id: String,
        topic: String,
        partition: u32,
        leader_id: Uuid,
        leader_epoch: u64,
        first_offset: u64,
        frames: Vec<Vec<u8>>,
    ) -> Result<Self, ReplicateError> {
        let record_count = u32::try_from(frames.len())
            .map_err(|_| ReplicateError::InvalidEpoch("too many frames".into()))?;
        if record_count == 0 {
            return Err(ReplicateError::InvalidEpoch(
                "an epoch must contain at least one frame".into(),
            ));
        }
        let batch_crc = crc_for_frames(&frames);
        let request = Self {
            tenant_id,
            topic,
            partition,
            leader_id,
            leader_epoch,
            first_offset,
            record_count,
            frames,
            batch_crc,
        };
        request.validate()?;
        Ok(request)
    }

    pub fn last_offset(&self) -> Result<u64, ReplicateError> {
        self.first_offset
            .checked_add(u64::from(self.record_count) - 1)
            .ok_or_else(|| ReplicateError::InvalidEpoch("offset range overflow".into()))
    }

    pub fn end_offset(&self) -> Result<u64, ReplicateError> {
        self.last_offset()?
            .checked_add(1)
            .ok_or_else(|| ReplicateError::InvalidEpoch("offset range overflow".into()))
    }

    pub fn validate(&self) -> Result<(), ReplicateError> {
        if self.leader_epoch == u64::MAX {
            return Err(ReplicateError::InvalidEpoch(
                "leader epoch u64::MAX is reserved".into(),
            ));
        }
        if self.record_count == 0
            || self.record_count as usize != self.frames.len()
            || self.frames.len() > MAX_EPOCH_RECORDS
        {
            return Err(ReplicateError::InvalidEpoch(format!(
                "invalid record count {} for {} frames (limit {})",
                self.record_count,
                self.frames.len(),
                MAX_EPOCH_RECORDS
            )));
        }
        self.end_offset()?;
        let actual_crc = crc_for_frames(&self.frames);
        if actual_crc != self.batch_crc {
            return Err(ReplicateError::InvalidEpoch(format!(
                "batch crc mismatch: declared {}, computed {}",
                self.batch_crc, actual_crc
            )));
        }
        Ok(())
    }

    /// Compact epoch body:
    /// fixed header | tenant | topic | repeated(frame_len | frame).
    pub fn binary_len(&self) -> Result<usize, ReplicateError> {
        self.validate()?;
        let tenant = self.tenant_id.as_bytes();
        let topic = self.topic.as_bytes();
        u16::try_from(tenant.len())
            .map_err(|_| ReplicateError::InvalidEpoch("tenant id is too long".into()))?;
        u16::try_from(topic.len())
            .map_err(|_| ReplicateError::InvalidEpoch("topic is too long".into()))?;
        let payload_len = self.frames.iter().try_fold(0usize, |total, frame| {
            total
                .checked_add(4)
                .and_then(|n| n.checked_add(frame.len()))
                .ok_or_else(|| ReplicateError::InvalidEpoch("epoch length overflow".into()))
        })?;
        FIXED_HEADER_LEN
            .checked_add(tenant.len())
            .and_then(|n| n.checked_add(topic.len()))
            .and_then(|n| n.checked_add(payload_len))
            .ok_or_else(|| ReplicateError::InvalidEpoch("epoch length overflow".into()))
    }

    pub fn encode_binary(&self) -> Result<Vec<u8>, ReplicateError> {
        let total_len = self.binary_len()?;
        let tenant = self.tenant_id.as_bytes();
        let topic = self.topic.as_bytes();
        let tenant_len = tenant.len() as u16;
        let topic_len = topic.len() as u16;
        let payload_len = total_len - FIXED_HEADER_LEN - tenant.len() - topic.len();
        let mut out = Vec::with_capacity(total_len);
        out.extend_from_slice(&EPOCH_MAGIC);
        out.extend_from_slice(&EPOCH_VERSION.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(self.leader_id.as_bytes());
        out.extend_from_slice(&self.partition.to_be_bytes());
        out.extend_from_slice(&self.leader_epoch.to_be_bytes());
        out.extend_from_slice(&self.first_offset.to_be_bytes());
        out.extend_from_slice(&self.record_count.to_be_bytes());
        out.extend_from_slice(&tenant_len.to_be_bytes());
        out.extend_from_slice(&topic_len.to_be_bytes());
        out.extend_from_slice(&(payload_len as u64).to_be_bytes());
        out.extend_from_slice(&self.batch_crc.to_be_bytes());
        out.extend_from_slice(tenant);
        out.extend_from_slice(topic);
        for frame in &self.frames {
            let len = u32::try_from(frame.len())
                .map_err(|_| ReplicateError::InvalidEpoch("frame is too large".into()))?;
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(frame);
        }
        debug_assert_eq!(out.len(), total_len);
        Ok(out)
    }

    pub fn decode_binary(body: &[u8]) -> Result<Self, ReplicateError> {
        if body.len() > MAX_DECODE_EPOCH_BYTES {
            return Err(ReplicateError::EpochTooLarge {
                bytes: body.len(),
                limit: MAX_DECODE_EPOCH_BYTES,
            });
        }
        if body.len() < FIXED_HEADER_LEN {
            return Err(ReplicateError::InvalidEpoch(
                "truncated epoch header".into(),
            ));
        }
        if body[..4] != EPOCH_MAGIC {
            return Err(ReplicateError::InvalidEpoch("invalid epoch magic".into()));
        }
        let version = read_u16(body, 4)?;
        if version != EPOCH_VERSION {
            return Err(ReplicateError::InvalidEpoch(format!(
                "unsupported epoch version {version}"
            )));
        }
        let leader_id = Uuid::from_slice(&body[8..24])
            .map_err(|e| ReplicateError::InvalidEpoch(format!("invalid leader id: {e}")))?;
        let partition = read_u32(body, 24)?;
        let leader_epoch = read_u64(body, 28)?;
        let first_offset = read_u64(body, 36)?;
        let record_count = read_u32(body, 44)?;
        if record_count == 0 || record_count as usize > MAX_EPOCH_RECORDS {
            return Err(ReplicateError::InvalidEpoch(format!(
                "record count {record_count} is outside 1..={MAX_EPOCH_RECORDS}"
            )));
        }
        let tenant_len = read_u16(body, 48)? as usize;
        let topic_len = read_u16(body, 50)? as usize;
        let payload_len = usize::try_from(read_u64(body, 52)?)
            .map_err(|_| ReplicateError::InvalidEpoch("payload length overflow".into()))?;
        let batch_crc = read_u32(body, 60)?;
        let strings_end = FIXED_HEADER_LEN
            .checked_add(tenant_len)
            .and_then(|n| n.checked_add(topic_len))
            .ok_or_else(|| ReplicateError::InvalidEpoch("epoch length overflow".into()))?;
        let expected_len = strings_end
            .checked_add(payload_len)
            .ok_or_else(|| ReplicateError::InvalidEpoch("epoch length overflow".into()))?;
        if expected_len != body.len() {
            return Err(ReplicateError::InvalidEpoch(format!(
                "declared epoch length {expected_len} does not match body length {}",
                body.len()
            )));
        }
        let tenant_id = std::str::from_utf8(&body[FIXED_HEADER_LEN..FIXED_HEADER_LEN + tenant_len])
            .map_err(|e| ReplicateError::InvalidEpoch(format!("tenant id is not utf-8: {e}")))?
            .to_owned();
        let topic_start = FIXED_HEADER_LEN + tenant_len;
        let topic = std::str::from_utf8(&body[topic_start..strings_end])
            .map_err(|e| ReplicateError::InvalidEpoch(format!("topic is not utf-8: {e}")))?
            .to_owned();
        let mut cursor = strings_end;
        let mut frames = Vec::with_capacity(record_count as usize);
        for _ in 0..record_count {
            let frame_len = read_u32(body, cursor)? as usize;
            cursor = cursor
                .checked_add(4)
                .ok_or_else(|| ReplicateError::InvalidEpoch("frame offset overflow".into()))?;
            let end = cursor
                .checked_add(frame_len)
                .ok_or_else(|| ReplicateError::InvalidEpoch("frame length overflow".into()))?;
            if end > body.len() {
                return Err(ReplicateError::InvalidEpoch("truncated frame".into()));
            }
            frames.push(body[cursor..end].to_vec());
            cursor = end;
        }
        if cursor != body.len() {
            return Err(ReplicateError::InvalidEpoch(
                "trailing bytes after declared frames".into(),
            ));
        }
        let request = Self {
            tenant_id,
            topic,
            partition,
            leader_id,
            leader_epoch,
            first_offset,
            record_count,
            frames,
            batch_crc,
        };
        request.validate()?;
        Ok(request)
    }
}

fn read_u16(body: &[u8], at: usize) -> Result<u16, ReplicateError> {
    let bytes = body
        .get(at..at + 2)
        .ok_or_else(|| ReplicateError::InvalidEpoch("truncated integer".into()))?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u32(body: &[u8], at: usize) -> Result<u32, ReplicateError> {
    let bytes = body
        .get(at..at + 4)
        .ok_or_else(|| ReplicateError::InvalidEpoch("truncated integer".into()))?;
    Ok(u32::from_be_bytes(
        bytes.try_into().expect("four byte slice"),
    ))
}

fn read_u64(body: &[u8], at: usize) -> Result<u64, ReplicateError> {
    let bytes = body
        .get(at..at + 8)
        .ok_or_else(|| ReplicateError::InvalidEpoch("truncated integer".into()))?;
    Ok(u64::from_be_bytes(
        bytes.try_into().expect("eight byte slice"),
    ))
}

fn crc_for_frames(frames: &[Vec<u8>]) -> u32 {
    let mut crc = crc32fast::Hasher::new();
    for frame in frames {
        crc.update(frame);
    }
    crc.finalize()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicateBatchAck {
    pub node_id: Uuid,
    pub partition: u32,
    pub leader_epoch: u64,
    /// Exclusive durable high watermark.
    pub durable_hwm: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateOutcome {
    pub durable_acks: usize,
    pub quorum: usize,
    pub committed_hwm: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplicaProgress {
    pub peer_id: Uuid,
    /// Replication stream: the logical topic of a V1 partition log, or empty
    /// for a V2 physical shard (whose log carries many topics).
    pub topic: String,
    pub partition: u32,
    pub durable_hwm: u64,
    pub leader_hwm: u64,
    pub lag: u64,
    pub in_sync: bool,
    pub last_success_ms: Option<u64>,
    pub consecutive_failures: u64,
    /// Last time this stream was updated (success or failure).
    pub last_update_ms: u64,
    /// Follower log end reported by its last position conflict.
    pub log_end: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateCatchUpRequest {
    pub tenant_id: String,
    pub topic: String,
    pub partition: u32,
    pub from_offset: u64,
    #[serde(default = "default_catchup_max")]
    pub max_records: usize,
}

fn default_catchup_max() -> usize {
    512
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateCatchUpResponse {
    pub frames: Vec<ReplicateAppendRequest>,
    pub committed_hwm: u64,
    #[serde(default)]
    pub base_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatchUpFrame {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateCatchUpRange {
    pub partition: u32,
    pub leader_generation: u64,
    pub committed_hwm: u64,
    /// First offset the source still retains; frames below it were
    /// collected by retention and cannot be served.
    pub base_offset: u64,
    pub frames: Vec<CatchUpFrame>,
}

impl ReplicateCatchUpRange {
    pub fn encode_binary(&self) -> Result<Vec<u8>, ReplicateError> {
        if self.frames.len() > MAX_CATCHUP_RECORDS {
            return Err(ReplicateError::InvalidEpoch(
                "catch-up range has too many records".into(),
            ));
        }
        let (version, header_len) = if self.base_offset > 0 {
            (CATCHUP_VERSION_BASE, CATCHUP_HEADER_LEN_BASE)
        } else {
            (CATCHUP_VERSION, CATCHUP_HEADER_LEN)
        };
        let total = self.frames.iter().try_fold(header_len, |size, frame| {
            size.checked_add(12)
                .and_then(|value| value.checked_add(frame.bytes.len()))
                .ok_or_else(|| ReplicateError::InvalidEpoch("catch-up length overflow".into()))
        })?;
        if total > MAX_DECODE_EPOCH_BYTES {
            return Err(ReplicateError::EpochTooLarge {
                bytes: total,
                limit: MAX_DECODE_EPOCH_BYTES,
            });
        }
        let mut output = Vec::with_capacity(total);
        output.extend_from_slice(&CATCHUP_MAGIC);
        output.extend_from_slice(&version.to_be_bytes());
        output.extend_from_slice(&0u16.to_be_bytes());
        output.extend_from_slice(&self.partition.to_be_bytes());
        output.extend_from_slice(&self.leader_generation.to_be_bytes());
        output.extend_from_slice(&self.committed_hwm.to_be_bytes());
        output.extend_from_slice(&(self.frames.len() as u32).to_be_bytes());
        if version == CATCHUP_VERSION_BASE {
            output.extend_from_slice(&self.base_offset.to_be_bytes());
        }
        for frame in &self.frames {
            let len = u32::try_from(frame.bytes.len())
                .map_err(|_| ReplicateError::InvalidEpoch("catch-up frame too large".into()))?;
            output.extend_from_slice(&frame.offset.to_be_bytes());
            output.extend_from_slice(&len.to_be_bytes());
            output.extend_from_slice(&frame.bytes);
        }
        Ok(output)
    }

    pub fn decode_binary(body: &[u8]) -> Result<Self, ReplicateError> {
        if body.len() > MAX_DECODE_EPOCH_BYTES {
            return Err(ReplicateError::EpochTooLarge {
                bytes: body.len(),
                limit: MAX_DECODE_EPOCH_BYTES,
            });
        }
        if body.len() < CATCHUP_HEADER_LEN || body[..4] != CATCHUP_MAGIC {
            return Err(ReplicateError::InvalidEpoch(
                "invalid catch-up range header".into(),
            ));
        }
        let (header_len, base_offset) = match read_u16(body, 4)? {
            CATCHUP_VERSION => (CATCHUP_HEADER_LEN, 0),
            CATCHUP_VERSION_BASE if body.len() >= CATCHUP_HEADER_LEN_BASE => {
                (CATCHUP_HEADER_LEN_BASE, read_u64(body, 32)?)
            }
            _ => {
                return Err(ReplicateError::InvalidEpoch(
                    "unsupported catch-up range version".into(),
                ))
            }
        };
        let partition = read_u32(body, 8)?;
        let leader_generation = read_u64(body, 12)?;
        let committed_hwm = read_u64(body, 20)?;
        let count = read_u32(body, 28)? as usize;
        if count > MAX_CATCHUP_RECORDS {
            return Err(ReplicateError::InvalidEpoch(
                "catch-up record count exceeds limit".into(),
            ));
        }
        let mut cursor = header_len;
        let mut frames = Vec::with_capacity(count);
        for _ in 0..count {
            let offset = read_u64(body, cursor)?;
            let len = read_u32(body, cursor + 8)? as usize;
            cursor = cursor
                .checked_add(12)
                .ok_or_else(|| ReplicateError::InvalidEpoch("catch-up offset overflow".into()))?;
            let end = cursor
                .checked_add(len)
                .filter(|end| *end <= body.len())
                .ok_or_else(|| ReplicateError::InvalidEpoch("truncated catch-up frame".into()))?;
            frames.push(CatchUpFrame {
                offset,
                bytes: body[cursor..end].to_vec(),
            });
            cursor = end;
        }
        if cursor != body.len() {
            return Err(ReplicateError::InvalidEpoch(
                "trailing catch-up range bytes".into(),
            ));
        }
        Ok(Self {
            partition,
            leader_generation,
            committed_hwm,
            base_offset,
            frames,
        })
    }

    fn from_legacy(response: ReplicateCatchUpResponse) -> Result<Self, ReplicateError> {
        let partition = response
            .frames
            .first()
            .map(|frame| frame.partition)
            .unwrap_or(0);
        let leader_generation = response
            .frames
            .first()
            .map(|frame| frame.leader_generation)
            .unwrap_or(0);
        let frames = response
            .frames
            .into_iter()
            .map(|frame| {
                B64.decode(frame.frame_b64)
                    .map(|bytes| CatchUpFrame {
                        offset: frame.offset,
                        bytes,
                    })
                    .map_err(|error| {
                        ReplicateError::InvalidEpoch(format!(
                            "invalid legacy catch-up base64: {error}"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            partition,
            leader_generation,
            committed_hwm: response.committed_hwm,
            base_offset: response.base_offset,
            frames,
        })
    }
}

#[derive(Clone)]
pub struct ReplicationClient {
    pub(crate) http: reqwest::Client,
    pub(crate) cluster: ClusterConfig,
    pub(crate) in_flight: Arc<Semaphore>,
    pub(crate) max_epoch_bytes: usize,
    pub(crate) progress: SharedReplicaProgress,
    telemetry: Arc<crate::telemetry::ReplicationTelemetry>,
    pub(crate) runtime: Option<broker_raft_meta::ClusterRuntime>,
    physical_shards: bool,
    pub(crate) repair: Arc<crate::repair::RepairRegistry>,
    fence: Arc<crate::repair::FollowerFence>,
}

impl ReplicationClient {
    pub fn new(cluster: ClusterConfig) -> Self {
        let max_epoch_bytes = configured_max_epoch_bytes();
        let max_in_flight = configured_max_in_flight_bytes()
            .max(max_epoch_bytes)
            .min(Semaphore::MAX_PERMITS);
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
            cluster,
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
            max_epoch_bytes,
            progress: Arc::new(Mutex::new(HashMap::new())),
            telemetry: Arc::new(crate::telemetry::ReplicationTelemetry::default()),
            runtime: None,
            physical_shards: false,
            repair: Arc::default(),
            fence: Arc::default(),
        }
    }

    /// Follower-side truncation fence for this node's replicas.
    pub fn follower_fence(&self) -> &crate::repair::FollowerFence {
        &self.fence
    }

    /// V2 layouts keep one log per physical shard, so progress is tracked per
    /// shard and peer rather than per logical topic.
    pub fn with_physical_shards(mut self, physical: bool) -> Self {
        self.physical_shards = physical;
        self
    }

    /// Progress stream for an epoch of `topic`.
    pub fn progress_stream(&self, topic: &str) -> String {
        if self.physical_shards {
            String::new()
        } else {
            topic.to_string()
        }
    }

    /// Encoded-size cap of one replication epoch.
    pub fn max_epoch_bytes(&self) -> usize {
        self.max_epoch_bytes
    }

    pub fn with_runtime(mut self, runtime: broker_raft_meta::ClusterRuntime) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub(crate) fn targets_for_shard(
        &self,
        shard: u32,
    ) -> (Vec<broker_raft_meta::NodeConfig>, usize) {
        if let Some(runtime) = &self.runtime {
            return runtime.replication_targets(shard);
        }
        (self.cluster.peer_nodes(), self.cluster.quorum_size())
    }

    pub fn with_config(cluster: ClusterConfig) -> Self {
        Self::new(cluster)
    }

    /// Leader: parallel fan-out frame to peers; require quorum acks (including self).
    pub async fn replicate_append(
        &self,
        tenant_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
        frame: &[u8],
        leader_generation: u64,
    ) -> Result<(), ReplicateError> {
        if self.cluster.node_count() <= 1 {
            return Ok(());
        }

        let req = ReplicateBatchRequest::new(
            tenant_id.to_string(),
            topic.to_string(),
            partition,
            self.cluster.node_id,
            leader_generation,
            offset,
            vec![frame.to_vec()],
        )?;
        self.replicate_batch(req, offset.saturating_add(1))
            .await
            .map(|_| ())
    }

    pub(crate) fn apply_cluster_secret(req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Ok(secret) = std::env::var("BETTERMQ_CLUSTER_SECRET") {
            if !secret.trim().is_empty() {
                return req.header("x-bettermq-cluster-secret", secret);
            }
        }
        req
    }

    /// Replicate a contiguous epoch batch. `local_durable_hwm` is an exclusive
    /// WAL high watermark obtained only after local fsync. Returns after a
    /// durable majority; remaining peer RPCs continue in the background.
    pub async fn replicate_batch(
        &self,
        req: ReplicateBatchRequest,
        local_durable_hwm: u64,
    ) -> Result<ReplicateOutcome, ReplicateError> {
        self.replicate_batch_with_local(req, async move { Ok(local_durable_hwm) })
            .await
    }

    /// Fan out replica RPCs while `local_durable` is still in flight. `202`
    /// still requires local fsync plus minISR (self counts after local
    /// durability). minISR stays 2 on RF=3 — do not wait for three copies.
    pub async fn replicate_batch_with_local<F>(
        &self,
        req: ReplicateBatchRequest,
        local_durable: F,
    ) -> Result<ReplicateOutcome, ReplicateError>
    where
        F: std::future::Future<Output = Result<u64, ReplicateError>>,
    {
        req.validate()?;
        let required_hwm = req.end_offset()?;
        let quorum_started = Instant::now();
        if self.cluster.node_count() <= 1 {
            let local_durable_hwm = local_durable.await?;
            if local_durable_hwm < required_hwm {
                return Err(ReplicateError::LocalNotDurable {
                    required: required_hwm,
                    actual: local_durable_hwm,
                });
            }
            self.telemetry
                .record_quorum(req.partition, quorum_started.elapsed(), 1, 1, true);
            return Ok(ReplicateOutcome {
                durable_acks: 1,
                quorum: 1,
                committed_hwm: required_hwm,
            });
        }
        let encoded_len = req.binary_len()?;
        if encoded_len > self.max_epoch_bytes {
            return Err(ReplicateError::EpochTooLarge {
                bytes: encoded_len,
                limit: self.max_epoch_bytes,
            });
        }
        let permits = u32::try_from(encoded_len).map_err(|_| ReplicateError::EpochTooLarge {
            bytes: encoded_len,
            limit: u32::MAX as usize,
        })?;
        let epoch_permit = Arc::new(
            Arc::clone(&self.in_flight)
                .acquire_many_owned(permits)
                .await
                .map_err(|_| ReplicateError::Closed)?,
        );
        let encoded = bytes::Bytes::from(req.encode_binary()?);
        let (peers, quorum) = self.targets_for_shard(req.partition);
        let stream = self.progress_stream(&req.topic);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<bool>(peers.len().max(1));
        for peer in peers {
            let url = format!("{}/internal/v1/replicate/batch", peer.internal_base());
            let http = self.http.clone();
            let body = encoded.clone();
            let tx = tx.clone();
            let epoch_permit = Arc::clone(&epoch_permit);
            let client = self.clone();
            let stream = stream.clone();
            let partition = req.partition;
            let leader_epoch = req.leader_epoch;
            // A peer that is out of sync or under repair still receives the
            // push, but only a completed repair makes it count again.
            let counted = self.counts_toward_quorum(&stream, partition, peer.id);
            tokio::spawn(async move {
                let peer_id = peer.id;
                let builder = Self::apply_cluster_secret(
                    http.post(&url)
                        .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                        .body(body),
                );
                let outcome = match builder.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        match resp.json::<ReplicateBatchAck>().await {
                            Ok(response)
                                if response.node_id == peer_id
                                    && response.partition == partition
                                    && response.leader_epoch == leader_epoch
                                    && response.durable_hwm >= required_hwm =>
                            {
                                PushOutcome::Durable(response.durable_hwm)
                            }
                            Ok(response) => {
                                warn!(
                                    expected_peer = %peer_id,
                                    actual_peer = %response.node_id,
                                    expected_hwm = required_hwm,
                                    actual_hwm = response.durable_hwm,
                                    "replicate peer returned non-durable or mismatched ack"
                                );
                                PushOutcome::Short(response.durable_hwm)
                            }
                            Err(e) => {
                                warn!(error = %e, url = %url, "replicate peer returned invalid ack");
                                PushOutcome::Failed
                            }
                        }
                    }
                    Ok(resp) if resp.status() == reqwest::StatusCode::CONFLICT => {
                        match resp.json::<crate::repair::ReplicaPositionConflict>().await {
                            Ok(conflict) if conflict.node_id == peer_id => {
                                debug!(
                                    url = %url,
                                    log_end = conflict.log_end,
                                    want = conflict.want,
                                    "replicate peer is at a different position; scheduling repair"
                                );
                                PushOutcome::Conflict(conflict.log_end)
                            }
                            _ => PushOutcome::Failed,
                        }
                    }
                    Ok(resp) => {
                        warn!(status = %resp.status(), url = %url, "replicate batch peer rejected");
                        PushOutcome::Failed
                    }
                    Err(e) => {
                        warn!(error = %e, url = %url, "replicate batch peer failed");
                        PushOutcome::Failed
                    }
                };
                let durable = matches!(outcome, PushOutcome::Durable(_));
                let (durable_hwm, log_end) = match outcome {
                    PushOutcome::Durable(hwm) | PushOutcome::Short(hwm) => (hwm, None),
                    PushOutcome::Conflict(log_end) => (0, Some(log_end)),
                    PushOutcome::Failed => (0, None),
                };
                update_progress(
                    &client.progress,
                    ProgressUpdate {
                        stream: &stream,
                        partition,
                        peer_id,
                        leader_hwm: required_hwm,
                        durable_hwm,
                        success: durable,
                        may_mark_in_sync: counted,
                        log_end,
                    },
                );
                if !durable {
                    client.schedule_repair(&stream, partition, peer);
                }
                drop(epoch_permit);
                let _ = tx.send(durable && counted).await;
            });
        }
        drop(tx);
        let local_durable_hwm = local_durable.await?;
        if local_durable_hwm < required_hwm {
            return Err(ReplicateError::LocalNotDurable {
                required: required_hwm,
                actual: local_durable_hwm,
            });
        }
        let mut acked = 1usize;
        while acked < quorum {
            match rx.recv().await {
                Some(true) => acked += 1,
                Some(false) => {}
                None => break,
            }
        }
        debug!(
            acked,
            quorum,
            topic = %req.topic,
            partition = req.partition,
            first_offset = req.first_offset,
            record_count = req.record_count,
            "replicate durable quorum"
        );
        if acked >= quorum {
            self.telemetry.record_quorum(
                req.partition,
                quorum_started.elapsed(),
                acked,
                quorum,
                true,
            );
            Ok(ReplicateOutcome {
                durable_acks: acked,
                quorum,
                committed_hwm: required_hwm,
            })
        } else {
            self.telemetry.record_quorum(
                req.partition,
                quorum_started.elapsed(),
                acked,
                quorum,
                false,
            );
            Err(ReplicateError::QuorumNotReached { acked, quorum })
        }
    }

    pub fn replica_progress(&self) -> Vec<ReplicaProgress> {
        let mut all = self
            .progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let cutoff = now_ms().saturating_sub(PROGRESS_RETENTION_MS);
        all.retain(|_, entry| entry.last_update_ms >= cutoff);
        let mut progress: Vec<_> = all.values().cloned().collect();
        drop(all);
        progress.sort_by_key(|p| (p.partition, p.topic.clone(), p.peer_id));
        progress
    }

    /// ISR and lag telemetry. Only peers that are current replication targets
    /// of a shard count toward its ISR.
    pub fn telemetry_snapshot(&self) -> crate::telemetry::ReplicationTelemetrySnapshot {
        let mut progress = self.replica_progress();
        if self.runtime.is_some() {
            let mut targets: HashMap<u32, Vec<Uuid>> = HashMap::new();
            progress.retain(|entry| {
                targets
                    .entry(entry.partition)
                    .or_insert_with(|| {
                        self.targets_for_shard(entry.partition)
                            .0
                            .into_iter()
                            .map(|peer| peer.id)
                            .collect()
                    })
                    .contains(&entry.peer_id)
            });
        }
        self.telemetry.snapshot(&progress)
    }

    pub async fn catch_up_from(
        &self,
        peer: &str,
        body: &ReplicateCatchUpRequest,
    ) -> Result<ReplicateCatchUpRange, ReplicateError> {
        let url = format!(
            "{}/internal/v1/replicate/catch-up",
            peer.trim_end_matches('/')
        );
        let resp = Self::apply_cluster_secret(self.http.post(&url).json(body))
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let detail = resp.text().await.unwrap_or_default();
            return Err(ReplicateError::PeerRejected {
                peer: peer.to_string(),
                status,
                detail,
            });
        }
        let binary = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/octet-stream"));
        if resp
            .content_length()
            .is_some_and(|length| length > MAX_DECODE_EPOCH_BYTES as u64)
        {
            return Err(ReplicateError::EpochTooLarge {
                bytes: resp.content_length().unwrap_or_default() as usize,
                limit: MAX_DECODE_EPOCH_BYTES,
            });
        }
        let mut stream = resp.bytes_stream();
        let mut bytes = bytes::BytesMut::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if bytes.len().saturating_add(chunk.len()) > MAX_DECODE_EPOCH_BYTES {
                return Err(ReplicateError::EpochTooLarge {
                    bytes: bytes.len().saturating_add(chunk.len()),
                    limit: MAX_DECODE_EPOCH_BYTES,
                });
            }
            bytes.extend_from_slice(&chunk);
        }
        if binary {
            ReplicateCatchUpRange::decode_binary(&bytes)
        } else {
            let legacy = serde_json::from_slice::<ReplicateCatchUpResponse>(&bytes)?;
            ReplicateCatchUpRange::from_legacy(legacy)
        }
    }
}

fn env_bytes(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &usize| *value > 0)
        .unwrap_or(default)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

enum PushOutcome {
    Durable(u64),
    Short(u64),
    Conflict(u64),
    Failed,
}

pub(crate) struct ProgressUpdate<'a> {
    pub stream: &'a str,
    pub partition: u32,
    pub peer_id: Uuid,
    pub leader_hwm: u64,
    pub durable_hwm: u64,
    pub success: bool,
    /// A hot-path ack only keeps an in-sync peer in sync; turning an
    /// out-of-sync peer back in sync is reserved for a completed repair.
    pub may_mark_in_sync: bool,
    pub log_end: Option<u64>,
}

pub(crate) fn update_progress(
    progress: &Mutex<HashMap<(String, u32, Uuid), ReplicaProgress>>,
    update: ProgressUpdate<'_>,
) -> bool {
    let ProgressUpdate {
        stream,
        partition,
        peer_id,
        leader_hwm,
        durable_hwm,
        success,
        may_mark_in_sync,
        log_end,
    } = update;
    let key = (stream.to_string(), partition, peer_id);
    let mut all = progress
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = all.entry(key).or_insert_with(|| ReplicaProgress {
        peer_id,
        topic: stream.to_string(),
        partition,
        durable_hwm: 0,
        leader_hwm,
        lag: leader_hwm,
        // A peer is presumed in sync until the first failed push.
        in_sync: true,
        last_success_ms: None,
        consecutive_failures: 0,
        last_update_ms: 0,
        log_end: None,
    });
    let was_in_sync = entry.in_sync;
    entry.last_update_ms = now_ms();
    // Concurrent epochs can complete out of order; an older epoch's ack must
    // not hide a failure of a newer one, so the leader frontier only grows.
    entry.leader_hwm = entry.leader_hwm.max(leader_hwm);
    if success {
        entry.durable_hwm = entry.durable_hwm.max(durable_hwm);
        entry.last_success_ms = Some(now_ms());
        entry.consecutive_failures = 0;
        entry.log_end = None;
    } else {
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
        if log_end.is_some() {
            entry.log_end = log_end;
        }
    }
    entry.lag = entry.leader_hwm.saturating_sub(entry.durable_hwm);
    entry.in_sync =
        success && may_mark_in_sync && was_in_sync && entry.durable_hwm >= entry.leader_hwm;
    entry.in_sync
}

impl ReplicationClient {
    /// Whether a durable ack from `peer` counts toward quorum right now.
    pub(crate) fn counts_toward_quorum(&self, stream: &str, partition: u32, peer: Uuid) -> bool {
        let key = (stream.to_string(), partition, peer);
        if self.repair.is_active(&key) {
            return false;
        }
        self.progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&key)
            .is_none_or(|entry| entry.in_sync)
    }

    pub(crate) fn record_repair_progress(
        &self,
        stream: &str,
        partition: u32,
        peer_id: Uuid,
        leader_hwm: u64,
        durable_hwm: u64,
    ) {
        update_progress(
            &self.progress,
            ProgressUpdate {
                stream,
                partition,
                peer_id,
                leader_hwm,
                durable_hwm,
                success: true,
                may_mark_in_sync: false,
                log_end: None,
            },
        );
    }

    /// Mark a peer in sync after repair verified it holds the leader's log
    /// through `leader_end`. Returns false if pushes have moved the leader
    /// frontier beyond what was verified, so repair must continue.
    pub(crate) fn mark_repaired(
        &self,
        stream: &str,
        partition: u32,
        peer_id: Uuid,
        leader_end: u64,
        durable_hwm: u64,
    ) -> bool {
        let mut all = self
            .progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = all
            .entry((stream.to_string(), partition, peer_id))
            .or_insert_with(|| ReplicaProgress {
                peer_id,
                topic: stream.to_string(),
                partition,
                durable_hwm: 0,
                leader_hwm: leader_end,
                lag: leader_end,
                in_sync: false,
                last_success_ms: None,
                consecutive_failures: 0,
                last_update_ms: 0,
                log_end: None,
            });
        let now = now_ms();
        entry.last_update_ms = now;
        entry.last_success_ms = Some(now);
        entry.consecutive_failures = 0;
        entry.log_end = None;
        // Repair may have truncated the follower, so its durable HWM is set,
        // not maxed.
        entry.durable_hwm = durable_hwm;
        entry.leader_hwm = entry.leader_hwm.max(leader_end);
        entry.lag = entry.leader_hwm.saturating_sub(entry.durable_hwm);
        entry.in_sync = entry.durable_hwm >= entry.leader_hwm;
        entry.in_sync
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(frames: Vec<Vec<u8>>) -> ReplicateBatchRequest {
        ReplicateBatchRequest::new(
            "tenant".into(),
            "jobs".into(),
            7,
            Uuid::new_v4(),
            11,
            42,
            frames,
        )
        .unwrap()
    }

    #[test]
    fn binary_epoch_round_trip_preserves_raw_frames() {
        let req = request(vec![b"\0binary-one\xff".to_vec(), b"two".to_vec()]);
        let encoded = req.encode_binary().unwrap();
        assert!(!encoded.windows(4).any(|bytes| bytes == b"AA=="));
        assert_eq!(ReplicateBatchRequest::decode_binary(&encoded).unwrap(), req);
    }

    #[test]
    fn binary_epoch_rejects_crc_and_count_corruption() {
        let req = request(vec![b"one".to_vec(), b"two".to_vec()]);
        let mut crc_corrupt = req.encode_binary().unwrap();
        *crc_corrupt.last_mut().unwrap() ^= 0x40;
        assert!(matches!(
            ReplicateBatchRequest::decode_binary(&crc_corrupt),
            Err(ReplicateError::InvalidEpoch(_))
        ));

        let mut count_corrupt = req.encode_binary().unwrap();
        count_corrupt[44..48].copy_from_slice(&3u32.to_be_bytes());
        assert!(matches!(
            ReplicateBatchRequest::decode_binary(&count_corrupt),
            Err(ReplicateError::InvalidEpoch(_))
        ));
    }

    #[test]
    fn binary_catch_up_range_round_trips_without_base64() {
        let range = ReplicateCatchUpRange {
            partition: 7,
            leader_generation: 12,
            committed_hwm: 45,
            base_offset: 0,
            frames: vec![
                CatchUpFrame {
                    offset: 42,
                    bytes: b"\0raw\xff".to_vec(),
                },
                CatchUpFrame {
                    offset: 43,
                    bytes: b"next".to_vec(),
                },
            ],
        };
        let encoded = range.encode_binary().unwrap();
        assert_eq!(&encoded[..4], &CATCHUP_MAGIC);
        assert_eq!(
            ReplicateCatchUpRange::decode_binary(&encoded).unwrap(),
            range
        );
    }

    #[test]
    fn cell_inline_limit_leaves_room_for_a_worst_case_frame() {
        let limits = cell_body_limits_for(16 * 1024 * 1024, 64 * 1024 * 1024, usize::MAX).unwrap();
        // Worst case: largest header and longest tenant/topic names.
        let epoch = FIXED_HEADER_LEN
            + EPOCH_NAME_BUDGET
            + EPOCH_FRAME_PREFIX
            + FRAME_FIXED_BYTES
            + broker_proto::record::MAX_FRAME_HEADER_BYTES
            + limits.max_frame_body;
        assert_eq!(epoch, limits.max_epoch_bytes);
        assert!(limits.max_inline_body > 14 * 1024 * 1024);
        // The frame payload cap also binds, and the DLQ allowance still holds.
        let capped = cell_body_limits_for(16 * 1024 * 1024, 64 * 1024 * 1024, 100_000).unwrap();
        assert_eq!(capped.max_frame_body, 100_000);
        assert_eq!(capped.max_inline_body, 100_000 - DLQ_ENVELOPE_ALLOWANCE);
        assert!(cell_body_limits_for(16 * 1024 * 1024, 64 * 1024 * 1024, 1_000).is_err());
        // Unsafe configurations refuse.
        assert!(cell_body_limits_for(MAX_DECODE_EPOCH_BYTES + 1, usize::MAX, usize::MAX).is_err());
        assert!(cell_body_limits_for(16 * 1024 * 1024, 8 * 1024 * 1024, usize::MAX).is_err());
        assert!(cell_body_limits_for(1024 * 1024, usize::MAX, usize::MAX).is_err());
    }

    #[test]
    fn every_accepted_body_stays_dead_letterable() {
        for epoch in [2 * 1024 * 1024, 16 * 1024 * 1024, MAX_DECODE_EPOCH_BYTES] {
            for frame_cap in [usize::MAX, 4 * 1024 * 1024] {
                let Ok(limits) = cell_body_limits_for(epoch, usize::MAX, frame_cap) else {
                    continue;
                };
                // A body at the ingest limit, wrapped in a DLQ envelope of up
                // to the allowance, still fits the epoch-derived frame limit.
                assert!(
                    limits.max_inline_body + DLQ_ENVELOPE_ALLOWANCE <= limits.max_frame_body,
                    "{limits:?}"
                );
                let dlq_frame = FIXED_HEADER_LEN
                    + EPOCH_NAME_BUDGET
                    + EPOCH_FRAME_PREFIX
                    + FRAME_FIXED_BYTES
                    + broker_proto::record::MAX_FRAME_HEADER_BYTES
                    + limits.max_inline_body
                    + DLQ_ENVELOPE_ALLOWANCE;
                assert!(dlq_frame <= limits.max_epoch_bytes);
            }
        }
    }

    #[test]
    fn epoch_ranges_respect_byte_budget_and_keep_order() {
        let lens = vec![400, 400, 400, 900, 10, 10];
        let budget = FIXED_HEADER_LEN + 3 + 4 + (EPOCH_FRAME_PREFIX + 400) * 2;
        let ranges = epoch_frame_ranges(&lens, 3, 4, budget);
        assert_eq!(ranges, vec![0..2, 2..3, 3..4, 4..6]);
        for range in &ranges {
            let bytes: usize = FIXED_HEADER_LEN
                + 7
                + lens[range.clone()]
                    .iter()
                    .map(|len| EPOCH_FRAME_PREFIX + len)
                    .sum::<usize>();
            assert!(bytes <= budget || range.len() == 1);
        }
        assert!(epoch_frame_ranges(&[], 0, 0, budget).is_empty());
        let many = vec![1usize; MAX_EPOCH_RECORDS + 5];
        let ranges = epoch_frame_ranges(&many, 0, 0, usize::MAX);
        assert_eq!(
            ranges,
            vec![
                0..MAX_EPOCH_RECORDS,
                MAX_EPOCH_RECORDS..MAX_EPOCH_RECORDS + 5
            ]
        );
    }

    #[test]
    fn split_epochs_of_large_inline_frames_always_encode_within_the_cap() {
        let max_epoch = 16 * 1024 * 1024;
        let limits = cell_body_limits_for(max_epoch, usize::MAX, usize::MAX).unwrap();
        // Bodies from 200 KiB up to the cell limit, in a mixed order.
        let sizes: Vec<usize> = (0..40)
            .map(|index| {
                let span = limits.max_inline_body - 200 * 1024;
                200 * 1024 + (index * 7_919 * 4_099) % span
            })
            .chain([limits.max_inline_body, 200 * 1024, limits.max_inline_body])
            .collect();
        let frames: Vec<Vec<u8>> = sizes
            .iter()
            .map(|size| vec![0u8; FRAME_FIXED_BYTES + 2048 + size])
            .collect();
        let lens: Vec<usize> = frames.iter().map(Vec::len).collect();
        let tenant = "default".to_string();
        let topic = "jobs".to_string();
        let ranges = epoch_frame_ranges(&lens, tenant.len(), topic.len(), max_epoch);
        let mut next = 0usize;
        for range in ranges {
            assert_eq!(range.start, next, "ranges keep offset order");
            next = range.end;
            let request = ReplicateBatchRequest::new(
                tenant.clone(),
                topic.clone(),
                0,
                Uuid::nil(),
                1,
                range.start as u64,
                frames[range].to_vec(),
            )
            .unwrap();
            assert!(request.binary_len().unwrap() <= max_epoch);
        }
        assert_eq!(next, frames.len());
    }

    #[test]
    fn catch_up_range_carries_the_retained_base() {
        let range = ReplicateCatchUpRange {
            partition: 2,
            leader_generation: 3,
            committed_hwm: 900,
            base_offset: 512,
            frames: vec![CatchUpFrame {
                offset: 512,
                bytes: b"first retained".to_vec(),
            }],
        };
        let encoded = range.encode_binary().unwrap();
        assert_eq!(read_u16(&encoded, 4).unwrap(), CATCHUP_VERSION_BASE);
        assert_eq!(
            ReplicateCatchUpRange::decode_binary(&encoded).unwrap(),
            range
        );
        // Without a collected prefix the v1 wire format is unchanged.
        let v1 = ReplicateCatchUpRange {
            base_offset: 0,
            ..range
        };
        let encoded = v1.encode_binary().unwrap();
        assert_eq!(read_u16(&encoded, 4).unwrap(), CATCHUP_VERSION);
        assert_eq!(ReplicateCatchUpRange::decode_binary(&encoded).unwrap(), v1);
    }

    #[test]
    fn legacy_catch_up_json_remains_decodable() {
        let legacy = ReplicateCatchUpResponse {
            committed_hwm: 2,
            base_offset: 0,
            frames: vec![ReplicateAppendRequest {
                tenant_id: "tenant".into(),
                topic: "jobs".into(),
                partition: 3,
                offset: 1,
                frame_b64: B64.encode(b"frame"),
                leader_generation: 9,
            }],
        };
        let decoded = ReplicateCatchUpRange::from_legacy(legacy).unwrap();
        assert_eq!(decoded.partition, 3);
        assert_eq!(decoded.frames[0].bytes, b"frame");
    }

    #[test]
    fn binary_catch_up_rejects_record_count_over_limit() {
        let range = ReplicateCatchUpRange {
            partition: 1,
            leader_generation: 1,
            committed_hwm: 0,
            base_offset: 0,
            frames: Vec::new(),
        };
        let mut encoded = range.encode_binary().unwrap();
        encoded[28..32].copy_from_slice(&((MAX_CATCHUP_RECORDS + 1) as u32).to_be_bytes());
        assert!(matches!(
            ReplicateCatchUpRange::decode_binary(&encoded),
            Err(ReplicateError::InvalidEpoch(message)) if message.contains("count")
        ));
    }

    #[test]
    fn v2_progress_is_per_physical_shard_and_out_of_order_acks_do_not_hide_lag() {
        let peer = Uuid::new_v4();
        let client = ReplicationClient::new(ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: Vec::new(),
            node_id: Uuid::new_v4(),
            generation: 1,
            hash_version: 1,
        })
        .with_physical_shards(true);
        // Two logical topics routed to physical shard 4 share one stream.
        let push = |stream: &str, leader_hwm: u64, durable_hwm: u64, success: bool| {
            update_progress(
                &client.progress,
                ProgressUpdate {
                    stream,
                    partition: 4,
                    peer_id: peer,
                    leader_hwm,
                    durable_hwm,
                    success,
                    may_mark_in_sync: true,
                    log_end: None,
                },
            )
        };
        for topic in ["orders", "billing"] {
            push(&client.progress_stream(topic), 10, 10, true);
        }
        assert_eq!(client.replica_progress().len(), 1);
        assert_eq!(client.telemetry_snapshot().shards[0].isr_members, 2);

        // Epoch ending at 20 fails, then the older epoch ending at 15 acks.
        let stream = client.progress_stream("orders");
        push(&stream, 20, 0, false);
        push(&stream, 15, 15, true);
        let progress = client.replica_progress();
        assert!(!progress[0].in_sync);
        assert_eq!(progress[0].lag, 5);
        assert_eq!(client.telemetry_snapshot().shards[0].isr_members, 1);
        // Even a full ack does not restore it: only a completed repair does.
        assert!(!push(&stream, 20, 20, true));
        assert!(client.mark_repaired(&stream, 4, peer, 20, 20));
        assert_eq!(client.telemetry_snapshot().shards[0].isr_members, 2);

        let v1 = ReplicationClient::new(client.cluster.clone());
        assert_eq!(v1.progress_stream("orders"), "orders");
    }

    #[tokio::test]
    async fn single_node_quorum_updates_native_telemetry() {
        let node_id = Uuid::new_v4();
        let client = ReplicationClient::new(ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![broker_raft_meta::NodeConfig {
                internal_url: None,
                id: node_id,
                addr: "http://127.0.0.1:8080".into(),
            }],
            node_id,
            generation: 1,
            hash_version: 1,
        });
        let request = request(vec![b"frame".to_vec()]);
        let required_hwm = request.end_offset().unwrap();
        let outcome = client.replicate_batch(request, required_hwm).await.unwrap();
        assert_eq!(outcome.durable_acks, 1);
        let snapshot = client.telemetry_snapshot();
        let shard = snapshot.shards.iter().find(|item| item.shard == 7).unwrap();
        assert_eq!(shard.quorum_requests, 1);
        assert_eq!(shard.quorum_failures, 0);
        assert_eq!(shard.last_durable_acks, 1);
        assert_eq!(shard.isr_members, 1);
    }

    #[tokio::test]
    async fn replication_calls_private_url_and_falls_back_for_legacy_members() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for private in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let leader = Uuid::new_v4();
            let follower = Uuid::new_v4();
            let req = request(vec![b"frame".to_vec()]);
            let hwm = req.end_offset().unwrap();
            let ack = serde_json::to_string(&ReplicateBatchAck {
                node_id: follower,
                partition: req.partition,
                leader_epoch: req.leader_epoch,
                durable_hwm: hwm,
            })
            .unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut received = Vec::new();
                loop {
                    let mut chunk = [0; 4096];
                    let count = socket.read(&mut chunk).await.unwrap();
                    assert!(count > 0);
                    received.extend_from_slice(&chunk[..count]);
                    if let Some(end) = received.windows(4).position(|s| s == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&received[..end]);
                        assert!(headers.starts_with("POST /internal/v1/replicate/batch "));
                        let size = headers
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        if received.len() >= end + 4 + size {
                            break;
                        }
                    }
                }
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{ack}", ack.len()).as_bytes()).await.unwrap();
            });
            let client = ReplicationClient::new(ClusterConfig {
                cluster_id: Uuid::new_v4(),
                node_id: leader,
                generation: 1,
                hash_version: 1,
                nodes: vec![
                    broker_raft_meta::NodeConfig {
                        id: leader,
                        addr: "http://127.0.0.1:1".into(),
                        internal_url: None,
                    },
                    broker_raft_meta::NodeConfig {
                        id: follower,
                        addr: if private {
                            "http://127.0.0.1:1".into()
                        } else {
                            base.clone()
                        },
                        internal_url: private.then_some(base),
                    },
                ],
            });
            let result =
                tokio::time::timeout(Duration::from_secs(3), client.replicate_batch(req, hwm))
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(result.durable_acks, 2);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn local_fsync_overlaps_replica_fanout() {
        let leader = Uuid::new_v4();
        let follower = Uuid::new_v4();
        let client = ReplicationClient::new(ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![
                broker_raft_meta::NodeConfig {
                    internal_url: None,
                    id: leader,
                    addr: "http://127.0.0.1:1".into(),
                },
                broker_raft_meta::NodeConfig {
                    internal_url: None,
                    id: follower,
                    addr: "http://127.0.0.1:1".into(),
                },
            ],
            node_id: leader,
            generation: 1,
            hash_version: 1,
        });
        let request = request(vec![b"frame".to_vec()]);
        let required_hwm = request.end_offset().unwrap();
        let started = Instant::now();
        let err = client
            .replicate_batch_with_local(request, async move {
                tokio::time::sleep(Duration::from_millis(40)).await;
                Ok(required_hwm)
            })
            .await
            .expect_err("unreachable follower cannot form minISR");
        assert!(matches!(
            err,
            ReplicateError::QuorumNotReached { acked: 1, .. }
        ));
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(400),
            "fan-out must not wait for local fsync before connecting: {elapsed:?}"
        );
    }
}
