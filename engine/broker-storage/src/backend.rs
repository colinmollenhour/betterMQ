//! Local WAL vs SlateDB partition backend.

use crate::log::{LogError, PartitionLog, PartitionLogConfig};
use broker_proto::{LogRecord, StoredMessage};
#[cfg(feature = "slate")]
use object_store::ObjectStore;
#[cfg(feature = "slate")]
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    Local,
    Slate,
}

pub use broker_proto::SLATE_SINGLE_BROKER;

impl StorageMode {
    pub fn from_env() -> Self {
        match std::env::var("BETTERMQ_STORAGE")
            .unwrap_or_else(|_| "local".into())
            .to_lowercase()
            .as_str()
        {
            "slate" | "s3" => StorageMode::Slate,
            _ => StorageMode::Local,
        }
    }
}

pub enum PartitionBackend {
    Local(Box<PartitionLog>),
    #[cfg(feature = "slate")]
    Slate(crate::slate_log::SlatePartitionLog),
}

impl PartitionBackend {
    pub fn open_local(
        dir: impl AsRef<std::path::Path>,
        config: PartitionLogConfig,
    ) -> Result<Self, LogError> {
        Ok(Self::Local(Box::new(PartitionLog::open(dir, config)?)))
    }

    pub fn open_local_for_shard(
        dir: impl AsRef<std::path::Path>,
        config: PartitionLogConfig,
        shard_id: u32,
        wal_format: u16,
    ) -> Result<Self, LogError> {
        Ok(Self::Local(Box::new(PartitionLog::open_for_shard(
            dir, config, shard_id, wal_format,
        )?)))
    }

    #[cfg(feature = "slate")]
    pub fn open_slate(
        db_path: String,
        local_cache_dir: impl AsRef<std::path::Path>,
        object_store: Arc<dyn ObjectStore>,
        partition: u32,
        config: PartitionLogConfig,
    ) -> Result<Self, LogError> {
        Ok(Self::Slate(crate::slate_log::SlatePartitionLog::open(
            db_path,
            local_cache_dir,
            object_store,
            partition,
            config,
        )?))
    }

    pub fn append(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        self.append_fenced(partition, header, payload, None)
    }

    /// Slate: optional fence generation stamped into the durable batch (HA M3).
    pub fn append_fenced(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
        fence_generation: Option<u64>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        match self {
            Self::Local(log) => log.append_fenced(partition, header, payload, fence_generation),
            #[cfg(feature = "slate")]
            Self::Slate(log) => log.append_fenced(partition, header, payload, fence_generation),
        }
    }

    pub fn append_raw_frame(
        &mut self,
        partition: u32,
        frame: &[u8],
        expected_offset: Option<u64>,
    ) -> Result<StoredMessage, LogError> {
        match self {
            Self::Local(log) => log.append_raw_frame(partition, frame, expected_offset),
            #[cfg(feature = "slate")]
            Self::Slate(log) => log.append_raw_frame(partition, frame, expected_offset),
        }
    }

    pub fn read_range(
        &self,
        partition: u32,
        offset: u64,
        max: usize,
    ) -> Result<Vec<StoredMessage>, LogError> {
        match self {
            Self::Local(log) => log.read_range(partition, offset, max),
            #[cfg(feature = "slate")]
            Self::Slate(log) => log.read_range(partition, offset, max),
        }
    }

    pub fn high_watermark(&self) -> u64 {
        match self {
            Self::Local(log) => log.high_watermark(),
            #[cfg(feature = "slate")]
            Self::Slate(log) => log.high_watermark(),
        }
    }

    /// Internal metadata scan: bodies are deliberately omitted, never delivered.
    pub fn read_range_metadata(
        &self,
        partition: u32,
        offset: u64,
        max: usize,
    ) -> Result<Vec<StoredMessage>, LogError> {
        match self {
            Self::Local(log) => log.read_range_metadata(partition, offset, max),
            #[cfg(feature = "slate")]
            Self::Slate(log) => {
                let mut messages = log.read_range(partition, offset, max)?;
                for message in &mut messages {
                    message.payload = Vec::new();
                }
                Ok(messages)
            }
        }
    }

    pub fn committed_hwm(&self) -> u64 {
        match self {
            Self::Local(log) => log.committed_hwm(),
            #[cfg(feature = "slate")]
            Self::Slate(log) => log.high_watermark(),
        }
    }

    pub fn read_raw_metadata(
        &self,
        partition: u32,
        offset: u64,
        max: usize,
    ) -> Result<Vec<StoredMessage>, LogError> {
        match self {
            Self::Local(log) => log.read_raw_metadata(partition, offset, max),
            #[cfg(feature = "slate")]
            Self::Slate(_) => self.read_range_metadata(partition, offset, max),
        }
    }

    pub fn is_dirty(&self) -> bool {
        match self {
            Self::Local(log) => log.is_dirty(),
            #[cfg(feature = "slate")]
            Self::Slate(_) => false,
        }
    }

    pub fn fsync_mode(&self) -> crate::log::FsyncMode {
        match self {
            Self::Local(log) => log.fsync_mode(),
            #[cfg(feature = "slate")]
            Self::Slate(_) => crate::log::FsyncMode::Always,
        }
    }

    pub fn append_batch(
        &mut self,
        partition: u32,
        items: Vec<(broker_proto::LogRecord, Vec<u8>)>,
        fence_generation: Option<u64>,
    ) -> Result<Vec<(StoredMessage, Vec<u8>)>, LogError> {
        match self {
            Self::Local(log) => log.append_batch(partition, items, fence_generation),
            #[cfg(feature = "slate")]
            Self::Slate(log) => {
                let mut out = Vec::with_capacity(items.len());
                for (header, payload) in items {
                    out.push(log.append_fenced(partition, header, payload, fence_generation)?);
                }
                Ok(out)
            }
        }
    }

    pub fn purge_offset(&mut self, offset: u64) -> Result<bool, LogError> {
        Ok(self.purge_offsets(&[offset])? == 1)
    }

    /// Tombstone records durably; returns how many live records were purged.
    pub fn purge_offsets(&mut self, offsets: &[u64]) -> Result<usize, LogError> {
        match self {
            Self::Local(log) => log.purge_offsets(offsets),
            #[cfg(feature = "slate")]
            Self::Slate(log) => Ok(offsets
                .iter()
                .filter(|offset| log.purge_offset(**offset))
                .count()),
        }
    }

    /// Follower reconciliation; see [`PartitionLog::truncate_to`].
    pub fn truncate_to(&mut self, offset: u64, generation: u64) -> Result<u64, LogError> {
        match self {
            Self::Local(log) => log.truncate_to(offset, generation),
            #[cfg(feature = "slate")]
            Self::Slate(_) => Err(LogError::TruncateUnsupported(
                "slate logs are not replicated".into(),
            )),
        }
    }

    pub fn gc_sealed_below(&mut self, watermark: u64) -> Result<usize, LogError> {
        match self {
            Self::Local(log) => log.gc_sealed_below(watermark),
            #[cfg(feature = "slate")]
            Self::Slate(_) => Ok(0),
        }
    }

    /// First retained offset; records below it were collected.
    pub fn base_offset(&self) -> u64 {
        match self {
            Self::Local(log) => log.base_offset(),
            #[cfg(feature = "slate")]
            Self::Slate(_) => 0,
        }
    }

    /// Exclusive end of the prefix `gc_sealed_below(watermark)` would delete.
    pub fn gc_boundary(&self, watermark: u64) -> u64 {
        match self {
            Self::Local(log) => log.gc_boundary(watermark),
            #[cfg(feature = "slate")]
            Self::Slate(_) => 0,
        }
    }

    /// WAL bytes held on local disk.
    pub fn retained_bytes(&self) -> u64 {
        match self {
            Self::Local(log) => log.retained_bytes(),
            #[cfg(feature = "slate")]
            Self::Slate(_) => 0,
        }
    }

    /// Continue an empty or fully-below-`base` log at `base`.
    pub fn reset_base(&mut self, base: u64) -> Result<(), LogError> {
        match self {
            Self::Local(log) => log.reset_base(base),
            #[cfg(feature = "slate")]
            Self::Slate(_) => Err(LogError::Slate(
                "reset_base is not supported for Slate storage".into(),
            )),
        }
    }

    pub fn sync(&mut self) -> Result<(), LogError> {
        match self {
            Self::Local(log) => log.sync(),
            #[cfg(feature = "slate")]
            Self::Slate(log) => log.sync(),
        }
    }

    pub fn flush_if_due(&mut self) -> Result<(), LogError> {
        match self {
            Self::Local(log) => log.flush_if_due(),
            #[cfg(feature = "slate")]
            Self::Slate(_) => Ok(()),
        }
    }

    pub fn flush_count(&self) -> u64 {
        match self {
            Self::Local(log) => log.flush_count(),
            #[cfg(feature = "slate")]
            Self::Slate(_) => 0,
        }
    }

    /// Deterministic fault injection used by durability tests.
    pub fn inject_fsync_failure(&mut self) {
        match self {
            Self::Local(log) => log.inject_fsync_failure(),
            #[cfg(feature = "slate")]
            Self::Slate(_) => {}
        }
    }
}
