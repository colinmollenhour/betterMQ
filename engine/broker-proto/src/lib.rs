//! Wire format and shared domain types for BetterMQ.

mod atomic_file;
pub mod codec;
pub use atomic_file::write_secret_atomic;
pub mod epoch;
pub const SLATE_SINGLE_BROKER: &str =
    "Slate storage supports a single broker; multi-broker Slate is unsupported";
/// Documented minimum client retry window; deployments may retain keys longer.
pub const DEFAULT_DEDUP_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;
pub mod paths;
pub mod record;
pub mod recover;
pub mod retry;
pub mod stable_hash;

pub const PROTOCOL_VERSION: u32 = 1;
pub const RECORD_MAGIC: [u8; 4] = *b"SBK1";

pub use codec::{decode, encode};
pub use epoch::{
    decode_epoch, encode_epoch, EpochError, EpochHeader, EPOCH_FORMAT_V2, EPOCH_HEADER_BYTES,
    EPOCH_MAGIC,
};
pub use paths::{
    join_under_root, sanitize_path_segment, validate_flow_key, validate_queue_name,
    PathSegmentError,
};
pub use record::{
    batch_crc, decode_frame, encode_frame, encode_frame_vec, max_frame_decode_payload_bytes,
    LogRecord, RecordError, StoredMessage,
};
pub use recover::allow_empty_metadata_recovery;
pub use retry::{RetryBackoff, RetryBackoffKind, RetryDefaults};
pub use stable_hash::{
    stable_node_id, stable_partition, stable_physical_shard, HASH_VERSION, HASH_VERSION_V2,
};
