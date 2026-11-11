//! Durable log record encoding (length-prefixed, CRC32-checked).

use crate::retry::RetryBackoff;
use crate::RECORD_MAGIC;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use thiserror::Error;
use uuid::Uuid;

/// Max bincode header size accepted when decoding a frame.
pub const MAX_FRAME_HEADER_BYTES: usize = 1024 * 1024;
/// Default max payload size (aligned with the HTTP body cap).
pub const DEFAULT_MAX_FRAME_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

fn configured_http_body_bytes() -> Option<usize> {
    std::env::var("BETTERMQ_MAX_HTTP_BODY_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n: &usize| *n > 0)
}

/// The client ingest limit (`BETTERMQ_MAX_HTTP_BODY_BYTES`, default 64 MiB).
/// The HTTP layer enforces it on fresh requests; the frame codec does not,
/// see [`max_frame_decode_payload_bytes`].
pub fn max_frame_payload_bytes() -> usize {
    configured_http_body_bytes().unwrap_or(DEFAULT_MAX_FRAME_PAYLOAD_BYTES)
}

/// Largest payload a frame may carry, for encoding and decoding.
///
/// Frames in the WAL, or arriving from a peer, were accepted under whatever
/// ingest limit applied when they were written, and internal republishes
/// (dead-lettering, schedules) re-encode bodies that were already accepted.
/// Lowering `BETTERMQ_MAX_HTTP_BODY_BYTES` must not make any of them
/// unreadable or impossible to move, so the codec uses the format default
/// unless the configured limit is larger.
pub fn max_frame_decode_payload_bytes() -> usize {
    decode_payload_limit(configured_http_body_bytes())
}

fn decode_payload_limit(configured: Option<usize>) -> usize {
    configured.unwrap_or(0).max(DEFAULT_MAX_FRAME_PAYLOAD_BYTES)
}

#[derive(Debug, Error)]
pub enum RecordError {
    #[error("invalid magic")]
    InvalidMagic,
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("frame too large: header={header_len} payload={payload_len}")]
    FrameTooLarge {
        header_len: usize,
        payload_len: usize,
    },
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("encode error: {0}")]
    Encode(#[from] bincode_next::error::EncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] bincode_next::error::DecodeError),
}

fn default_priority() -> u8 {
    5
}

fn default_max_retries() -> u32 {
    3
}

/// Metadata stored in the log before payload bytes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogRecord {
    pub id: Uuid,
    pub tenant_id: String,
    pub topic: String,
    pub routing_key: String,
    pub idempotency_key: Option<String>,
    pub published_at_ms: i64,
    /// 0 (lowest) .. 9 (highest). Used for webhook ordering when parallelism = 1.
    #[serde(default = "default_priority")]
    pub priority: u8,
    /// Per-message flow control override (parallel in-flight per routing key).
    #[serde(default)]
    pub flow_parallelism: Option<u32>,
    #[serde(default)]
    pub flow_key: Option<String>,
    #[serde(default)]
    pub flow_rate: Option<u32>,
    #[serde(default)]
    pub flow_period_secs: Option<u64>,
    /// Queue used at enqueue; destination URL/secret are fixed for this message.
    #[serde(default)]
    pub queue_id: Option<Uuid>,
    /// Fan-out group (one publish → many destinations).
    #[serde(default)]
    pub group_id: Option<Uuid>,
    #[serde(default)]
    pub group_member_id: Option<Uuid>,
    #[serde(default)]
    pub flow_profile_id: Option<Uuid>,
    #[serde(default)]
    pub destination_url: Option<String>,
    #[serde(default)]
    pub destination_secret: Option<String>,
    /// Extra delivery attempts after the first failure (`0` = single try, no retry).
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Backoff between attempts; when absent at read time, dispatch uses broker defaults.
    #[serde(default)]
    pub retry_backoff: Option<RetryBackoff>,
    /// Outbound HTTP method (default POST). Snapshotted at enqueue.
    #[serde(default)]
    pub http_method: Option<String>,
    /// JSON object of request headers. Snapshotted at enqueue.
    #[serde(default)]
    pub http_headers_json: Option<String>,
    /// Add BetterMQ HMAC signature headers when delivering.
    #[serde(default)]
    pub http_sign: Option<bool>,
    /// JSON `PayloadRef` when body stored in object store (CP3b).
    #[serde(default)]
    pub payload_ref_json: Option<String>,
}

/// Pre-priority on-disk header (read compatibility).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LogRecordLegacy {
    pub id: Uuid,
    pub tenant_id: String,
    pub topic: String,
    pub routing_key: String,
    pub idempotency_key: Option<String>,
    pub published_at_ms: i64,
}

impl LogRecord {
    pub fn decode_bytes(bytes: &[u8]) -> Result<Self, bincode_next::error::DecodeError> {
        match crate::codec::decode::<Self>(bytes) {
            Ok(r) => Ok(r),
            Err(_) => {
                let leg: LogRecordLegacy = crate::codec::decode(bytes)?;
                Ok(Self {
                    id: leg.id,
                    tenant_id: leg.tenant_id,
                    topic: leg.topic,
                    routing_key: leg.routing_key,
                    idempotency_key: leg.idempotency_key,
                    published_at_ms: leg.published_at_ms,
                    priority: default_priority(),
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
                    max_retries: default_max_retries(),
                    retry_backoff: None,
                    http_method: None,
                    http_headers_json: None,
                    http_sign: None,
                    payload_ref_json: None,
                })
            }
        }
    }
}

/// Full message returned to consumers (includes assigned offset and payload).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredMessage {
    pub id: Uuid,
    pub tenant_id: String,
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub routing_key: String,
    /// WAL identity needed by replica catch-up and takeover dedup recovery.
    /// Internal only: preserve the existing consumer JSON representation.
    #[serde(skip)]
    pub idempotency_key: Option<String>,
    pub payload: Vec<u8>,
    pub published_at_ms: i64,
    pub priority: u8,
    pub flow_parallelism: Option<u32>,
    pub flow_key: Option<String>,
    pub flow_rate: Option<u32>,
    pub flow_period_secs: Option<u64>,
    pub queue_id: Option<Uuid>,
    pub group_id: Option<Uuid>,
    pub group_member_id: Option<Uuid>,
    pub flow_profile_id: Option<Uuid>,
    pub destination_url: Option<String>,
    pub destination_secret: Option<String>,
    pub max_retries: u32,
    pub retry_backoff: Option<RetryBackoff>,
    pub http_method: Option<String>,
    pub http_headers_json: Option<String>,
    pub http_sign: Option<bool>,
    /// JSON `PayloadRef` when body is stored in object storage (CP3b).
    #[serde(default)]
    pub payload_ref_json: Option<String>,
}

/// Encode a frame into a new buffer (replication / batch append).
pub fn encode_frame_vec(header: &LogRecord, payload: &[u8]) -> Result<Vec<u8>, RecordError> {
    let mut buf = Vec::new();
    encode_frame(header, payload, &mut buf)?;
    Ok(buf)
}

/// CRC32 of concatenated frames (commit-epoch / replication batch).
pub fn batch_crc(frames: &[u8]) -> u32 {
    crc32fast::hash(frames)
}

/// On-disk frame: magic | header_len | payload_len | header | payload | crc32
pub fn encode_frame(
    header: &LogRecord,
    payload: &[u8],
    writer: &mut impl Write,
) -> Result<(), RecordError> {
    let header_bytes = crate::codec::encode(header)?;
    if header_bytes.len() > MAX_FRAME_HEADER_BYTES
        || payload.len() > max_frame_decode_payload_bytes()
    {
        return Err(RecordError::FrameTooLarge {
            header_len: header_bytes.len(),
            payload_len: payload.len(),
        });
    }
    let mut prefix = [0; 12];
    prefix[..4].copy_from_slice(&RECORD_MAGIC);
    prefix[4..8].copy_from_slice(&(header_bytes.len() as u32).to_be_bytes());
    prefix[8..].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    let mut crc = crc32fast::Hasher::new();
    crc.update(&prefix);
    crc.update(&header_bytes);
    crc.update(payload);
    let checksum = crc.finalize();
    writer.write_all(&prefix)?;
    writer.write_all(&header_bytes)?;
    writer.write_all(payload)?;
    writer.write_all(&checksum.to_be_bytes())?;
    Ok(())
}

pub fn decode_frame(reader: impl Read) -> Result<(LogRecord, Vec<u8>), RecordError> {
    decode_frame_inner(reader, true)
}

/// Decode and verify a complete frame without retaining its body. Dispatch
/// indexes and cursor scans need metadata, not a second in-memory message log.
pub fn decode_frame_metadata(reader: impl Read) -> Result<LogRecord, RecordError> {
    decode_frame_inner(reader, false).map(|(header, _)| header)
}

fn decode_frame_inner(
    mut reader: impl Read,
    keep_payload: bool,
) -> Result<(LogRecord, Vec<u8>), RecordError> {
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;
    if magic != RECORD_MAGIC {
        return Err(RecordError::InvalidMagic);
    }

    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let header_len = u32::from_be_bytes(len_buf) as usize;
    reader.read_exact(&mut len_buf)?;
    let payload_len = u32::from_be_bytes(len_buf) as usize;
    if header_len > MAX_FRAME_HEADER_BYTES || payload_len > max_frame_decode_payload_bytes() {
        return Err(RecordError::FrameTooLarge {
            header_len,
            payload_len,
        });
    }

    let header_bytes = read_exact_len(&mut reader, header_len)?;
    let mut crc = crc32fast::Hasher::new();
    crc.update(&RECORD_MAGIC);
    crc.update(&(header_len as u32).to_be_bytes());
    crc.update(&(payload_len as u32).to_be_bytes());
    crc.update(&header_bytes);
    let payload = if keep_payload {
        let payload = read_exact_len(&mut reader, payload_len)?;
        crc.update(&payload);
        payload
    } else {
        let mut remaining = payload_len;
        let mut scratch = [0; 8192];
        while remaining > 0 {
            let count = remaining.min(scratch.len());
            reader.read_exact(&mut scratch[..count])?;
            crc.update(&scratch[..count]);
            remaining -= count;
        }
        Vec::new()
    };

    let mut checksum_bytes = [0u8; 4];
    reader.read_exact(&mut checksum_bytes)?;
    let expected = u32::from_be_bytes(checksum_bytes);

    if crc.finalize() != expected {
        return Err(RecordError::ChecksumMismatch);
    }

    let header = LogRecord::decode_bytes(&header_bytes)?;
    Ok((header, payload))
}

/// Read exactly `len` bytes, growing the buffer with the data actually read
/// so a corrupt length field cannot force a huge up-front allocation.
fn read_exact_len(reader: &mut impl Read, len: usize) -> Result<Vec<u8>, RecordError> {
    let mut buf = Vec::with_capacity(len.min(1024 * 1024));
    reader.take(len as u64).read_to_end(&mut buf)?;
    if buf.len() != len {
        return Err(RecordError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "frame truncated",
        )));
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_header() -> LogRecord {
        LogRecord {
            id: Uuid::new_v4(),
            tenant_id: "default".into(),
            topic: "orders".into(),
            routing_key: "rk1".into(),
            idempotency_key: Some("idem-1".into()),
            published_at_ms: 1_700_000_000_000,
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
        }
    }

    #[test]
    fn roundtrip_frame() {
        let header = sample_header();
        let payload = b"hello".to_vec();
        let mut buf = Vec::new();
        encode_frame(&header, &payload, &mut buf).unwrap();
        let (h, p) = decode_frame(std::io::Cursor::new(buf)).unwrap();
        assert_eq!(h, header);
        assert_eq!(p, payload);
    }

    #[test]
    fn lowered_ingest_limit_does_not_restrict_the_codec() {
        let payload = vec![7u8; 256 * 1024];
        // Neither encode nor decode reads a lowered value, so this cannot
        // affect other tests running concurrently.
        std::env::set_var("BETTERMQ_MAX_HTTP_BODY_BYTES", "1024");
        let mut buf = Vec::new();
        let encoded = encode_frame(&sample_header(), &payload, &mut buf);
        let decoded = encoded
            .as_ref()
            .ok()
            .map(|()| decode_frame(std::io::Cursor::new(&buf)));
        let ingest_limit = max_frame_payload_bytes();
        std::env::remove_var("BETTERMQ_MAX_HTTP_BODY_BYTES");
        encoded.unwrap();
        assert_eq!(decoded.unwrap().unwrap().1, payload);
        assert_eq!(ingest_limit, 1024);
    }

    #[test]
    fn decode_limit_never_drops_below_the_format_default() {
        assert_eq!(decode_payload_limit(None), DEFAULT_MAX_FRAME_PAYLOAD_BYTES);
        assert_eq!(
            decode_payload_limit(Some(128 * 1024)),
            DEFAULT_MAX_FRAME_PAYLOAD_BYTES
        );
        assert_eq!(
            decode_payload_limit(Some(256 * 1024 * 1024)),
            256 * 1024 * 1024
        );
    }

    #[test]
    fn truncated_frame_is_unexpected_eof() {
        let mut buf = Vec::new();
        encode_frame(&sample_header(), b"payload", &mut buf).unwrap();
        buf.truncate(buf.len() - 6);
        let err = decode_frame(std::io::Cursor::new(buf)).unwrap_err();
        assert!(matches!(err, RecordError::Io(e) if e.kind() == io::ErrorKind::UnexpectedEof));
    }

    #[test]
    fn metadata_decode_checks_the_entire_body_and_preserves_frame_boundaries() {
        let header = sample_header();
        let payload = vec![42; 32 * 1024 + 3];
        let frame = encode_frame_vec(&header, &payload).unwrap();
        let mut both = frame.clone();
        both.extend_from_slice(&frame);
        let mut reader = std::io::Cursor::new(both);
        assert_eq!(decode_frame_metadata(&mut reader).unwrap(), header);
        assert_eq!(reader.position(), frame.len() as u64);
        assert_eq!(decode_frame(&mut reader).unwrap().1, payload);
        let mut damaged = frame.clone();
        damaged[frame.len() - 5] ^= 1;
        assert!(matches!(
            decode_frame_metadata(damaged.as_slice()),
            Err(RecordError::ChecksumMismatch)
        ));
        assert!(matches!(
            decode_frame_metadata(&frame[..frame.len() - 5]),
            Err(RecordError::Io(_))
        ));
    }

    #[test]
    fn decode_rejects_oversize_lengths() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&RECORD_MAGIC);
        buf.extend_from_slice(&u32::MAX.to_be_bytes());
        buf.extend_from_slice(&u32::MAX.to_be_bytes());
        let err = decode_frame(std::io::Cursor::new(buf)).unwrap_err();
        assert!(matches!(err, RecordError::FrameTooLarge { .. }));
    }
}
