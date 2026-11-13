//! Idempotency dedup and push dispatch cursors (local RocksDB, shared-meta file, or Slate).

use crate::shared_indexes::{SharedIndexError, SharedMetadataStore};
#[cfg(feature = "slate")]
use crate::slate_indexes::{SlateIndexError, SlateMetadataStore};
use rocksdb::{Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use thiserror::Error;
use uuid::Uuid;

mod group_commit;
mod reverse_index;
pub use group_commit::MetadataWriteReceipt;
use group_commit::MetadataWriter;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("rocksdb error: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("encode error: {0}")]
    Encode(#[from] bincode_next::error::EncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] bincode_next::error::DecodeError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("shared index error: {0}")]
    Shared(#[from] SharedIndexError),
    #[error("metadata JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("metadata outcome uncertain: {0}")]
    Uncertain(String),
    #[error("dedup identity changed before conditional commit")]
    DedupConflict,
    #[error("metadata ownership validation failed: {0}")]
    Validation(String),
    #[cfg(feature = "slate")]
    #[error("slate index error: {0}")]
    Slate(#[from] SlateIndexError),
}

pub type MetadataValidator = std::sync::Arc<dyn Fn() -> Result<(), IndexError> + Send + Sync>;

impl IndexError {
    pub fn is_dedup_conflict(&self) -> bool {
        matches!(
            self,
            Self::DedupConflict | Self::Shared(SharedIndexError::DedupConflict)
        )
    }
    pub fn is_conditional_failure(&self) -> bool {
        self.is_dedup_conflict()
            || matches!(
                self,
                Self::Validation(_) | Self::Shared(SharedIndexError::Validation(_))
            )
    }
}

#[derive(Default)]
pub struct MetadataWriteBatch {
    operations: Vec<MetadataOperation>,
    validators: Vec<MetadataValidator>,
}

impl std::fmt::Debug for MetadataWriteBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataWriteBatch")
            .field("operations", &self.operations)
            .field("validators", &self.validators.len())
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum MetadataOperation {
    DeleteDedupIf {
        tenant_id: String,
        key: String,
        expected: DedupEntry,
    },
    ComparePutDedup {
        tenant_id: String,
        key: String,
        entry: DedupEntry,
        expected: Option<Uuid>,
    },
    PutDedup {
        tenant_id: String,
        key: String,
        entry: DedupEntry,
    },
    DeleteDedup {
        tenant_id: String,
        key: String,
    },
    SetDispatchOffset {
        tenant_id: String,
        subscription_id: String,
        partition: u32,
        next_offset: u64,
    },
    MarkDispatchComplete {
        tenant_id: String,
        subscription_id: String,
        partition: u32,
        offset: u64,
        /// The delivered record, so a later reader can tell whether the marker
        /// belongs to the record now stored at `offset`. `None` = legacy marker.
        message_id: Option<Uuid>,
    },
}

impl MetadataWriteBatch {
    pub fn validate_with(&mut self, validator: MetadataValidator) {
        self.validators.push(validator);
    }

    pub fn compare_put_dedup(
        &mut self,
        tenant_id: impl Into<String>,
        key: impl Into<String>,
        entry: DedupEntry,
        expected: Option<Uuid>,
    ) {
        self.operations.push(MetadataOperation::ComparePutDedup {
            tenant_id: tenant_id.into(),
            key: key.into(),
            entry,
            expected,
        });
    }
    pub fn delete_dedup_if(
        &mut self,
        tenant_id: impl Into<String>,
        key: impl Into<String>,
        expected: DedupEntry,
    ) {
        self.operations.push(MetadataOperation::DeleteDedupIf {
            tenant_id: tenant_id.into(),
            key: key.into(),
            expected,
        });
    }
    fn estimated_bytes(&self) -> usize {
        self.operations.iter().fold(0usize, |bytes, operation| {
            let strings = match operation {
                MetadataOperation::PutDedup {
                    tenant_id,
                    key,
                    entry,
                }
                | MetadataOperation::ComparePutDedup {
                    tenant_id,
                    key,
                    entry,
                    ..
                } => tenant_id
                    .len()
                    .saturating_add(key.len())
                    .saturating_add(entry.topic.len()),
                MetadataOperation::DeleteDedup { tenant_id, key }
                | MetadataOperation::DeleteDedupIf { tenant_id, key, .. } => {
                    tenant_id.len().saturating_add(key.len())
                }
                MetadataOperation::SetDispatchOffset {
                    tenant_id,
                    subscription_id,
                    ..
                }
                | MetadataOperation::MarkDispatchComplete {
                    tenant_id,
                    subscription_id,
                    ..
                } => tenant_id.len().saturating_add(subscription_id.len()),
            };
            bytes.saturating_add(strings).saturating_add(128)
        })
    }

    pub fn put_dedup(
        &mut self,
        tenant_id: impl Into<String>,
        key: impl Into<String>,
        entry: DedupEntry,
    ) {
        self.operations.push(MetadataOperation::PutDedup {
            tenant_id: tenant_id.into(),
            key: key.into(),
            entry,
        });
    }

    pub fn delete_dedup(&mut self, tenant_id: impl Into<String>, key: impl Into<String>) {
        self.operations.push(MetadataOperation::DeleteDedup {
            tenant_id: tenant_id.into(),
            key: key.into(),
        });
    }

    pub fn set_dispatch_offset(
        &mut self,
        tenant_id: impl Into<String>,
        subscription_id: impl Into<String>,
        partition: u32,
        next_offset: u64,
    ) {
        self.operations.push(MetadataOperation::SetDispatchOffset {
            tenant_id: tenant_id.into(),
            subscription_id: subscription_id.into(),
            partition,
            next_offset,
        });
    }

    pub fn mark_dispatch_complete(
        &mut self,
        tenant_id: impl Into<String>,
        subscription_id: impl Into<String>,
        partition: u32,
        offset: u64,
    ) {
        self.operations
            .push(MetadataOperation::MarkDispatchComplete {
                tenant_id: tenant_id.into(),
                subscription_id: subscription_id.into(),
                partition,
                offset,
                message_id: None,
            });
    }

    /// [`Self::mark_dispatch_complete`] recording the delivered message id.
    pub fn mark_dispatch_complete_for(
        &mut self,
        tenant_id: impl Into<String>,
        subscription_id: impl Into<String>,
        partition: u32,
        offset: u64,
        message_id: Uuid,
    ) {
        self.operations
            .push(MetadataOperation::MarkDispatchComplete {
                tenant_id: tenant_id.into(),
                subscription_id: subscription_id.into(),
                partition,
                offset,
                message_id: Some(message_id),
            });
    }
}

/// Durable completion marker of one dispatch offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchCompletion {
    Missing,
    /// Written before markers carried the message id (or by a backend that
    /// does not store it): the offset is complete, identity unknown.
    Legacy,
    /// Completed delivery of this message id.
    Message(Uuid),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupEntry {
    pub message_id: Uuid,
    pub offset: u64,
    pub partition: u32,
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub inserted_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupRecord {
    pub tenant_id: String,
    pub key: String,
    pub entry: DedupEntry,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupPage {
    pub records: Vec<DedupRecord>,
    pub next: Option<(String, String)>,
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
pub(crate) fn dedup_retention_ms() -> Result<u64, IndexError> {
    let seconds = match std::env::var("BETTERMQ_DEDUP_RETENTION_SECS") {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?,
        Err(std::env::VarError::NotPresent) => broker_proto::DEFAULT_DEDUP_RETENTION_SECS,
        Err(error) => {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, error).into())
        }
    };
    if seconds < broker_proto::DEFAULT_DEDUP_RETENTION_SECS {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "dedup_retention must cover the documented 7-day client retry window",
        )
        .into());
    }
    seconds.checked_mul(1000).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "dedup_retention overflow").into()
    })
}

impl DedupEntry {
    pub fn same_identity(&self, other: &Self) -> bool {
        self.message_id == other.message_id
            && self.offset == other.offset
            && self.partition == other.partition
            && self.topic == other.topic
    }
    pub fn new(message_id: Uuid, offset: u64, partition: u32, topic: String) -> Self {
        Self {
            message_id,
            offset,
            partition,
            topic,
            inserted_at_ms: now_ms(),
        }
    }
    pub(crate) fn expired(&self, now: u64, retention: u64) -> bool {
        now.saturating_sub(self.inserted_at_ms) >= retention
    }
    pub(crate) fn encoded(&self) -> Result<Vec<u8>, IndexError> {
        let mut out = b"BMQDEDUP2\n".to_vec();
        out.extend(serde_json::to_vec(self)?);
        Ok(out)
    }
    pub(crate) fn decoded(bytes: &[u8], migration_ms: u64) -> Result<Self, IndexError> {
        // Legacy UUID + u64 + u32 bincode is exactly 36 bytes, even when its
        // UUID happens to begin with the new magic. Never reinterpret it.
        if bytes.len() != 36 {
            if let Some(json) = bytes.strip_prefix(b"BMQDEDUP2\n") {
                return Ok(serde_json::from_slice(json)?);
            }
        }
        #[derive(Deserialize)]
        struct Legacy {
            message_id: Uuid,
            offset: u64,
            partition: u32,
        }
        let (old, used): (Legacy, usize) =
            bincode_next::serde::decode_from_slice(bytes, bincode_next::config::legacy())?;
        if used != bytes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unrecognized dedup value version",
            )
            .into());
        }
        Ok(Self {
            message_id: old.message_id,
            offset: old.offset,
            partition: old.partition,
            topic: String::new(),
            inserted_at_ms: migration_ms,
        })
    }
}

struct RocksControl {
    mutation: Mutex<()>,
    sweep_cursor: Mutex<Vec<u8>>,
    operations: AtomicU64,
    migration_ms: u64,
    reverse_ready: std::sync::atomic::AtomicBool,
}

enum MetaBackend {
    Rocks {
        db: std::sync::Arc<DB>,
        sync_writes: std::sync::Arc<WriteOptions>,
        control: std::sync::Arc<RocksControl>,
    },
    Shared(Box<SharedMetadataStore>),
    #[cfg(feature = "slate")]
    Slate(SlateMetadataStore),
}

/// Metadata for a broker: local RocksDB, flock shared-meta, or SlateDB on object store.
pub struct MetadataStore {
    backend: MetaBackend,
    retention_ms: u64,
    writer: Option<std::sync::Arc<MetadataWriter>>,
}

impl MetadataStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, IndexError> {
        let retention_ms = dedup_retention_ms()?;
        if let Ok(shared) = std::env::var("BETTERMQ_SHARED_META_DIR") {
            let shared = shared.trim();
            if !shared.is_empty() {
                let local = path.as_ref();
                std::fs::create_dir_all(local)?;
                let identity = local.join("shared-store-id");
                if identity.exists() && !Path::new(shared).join("meta-manifest.json").is_file() {
                    return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "shared metadata manifest missing; check the shared mount, do not initialize an empty replacement").into());
                }
                let store = SharedMetadataStore::open(shared)?;
                if identity.exists() {
                    if std::fs::read_to_string(&identity)?.trim() != store.identity().to_string() {
                        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "shared metadata identity changed; check the shared mount and restore its original volume").into());
                    }
                } else {
                    crate::shared_indexes::durable_replace(
                        &identity,
                        store.identity().to_string().as_bytes(),
                    )?;
                }
                return Ok(Self {
                    backend: MetaBackend::Shared(Box::new(store)),
                    retention_ms,
                    writer: None,
                });
            }
        }
        Self::open_local(path)
    }

    /// Explicit node-local index, independent of a configured shared cursor
    /// store. Replica WAL identities must never be invalidated by another
    /// replica's divergent local tail.
    pub fn open_local(path: impl AsRef<Path>) -> Result<Self, IndexError> {
        let retention_ms = dedup_retention_ms()?;
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // This is an index, not the message log. RocksDB's 64 MiB default
        // memtable also preallocates a ~70 MiB WAL for an almost empty broker.
        opts.set_write_buffer_size(4 * 1024 * 1024);
        opts.set_max_write_buffer_number(2);
        let db = DB::open(&opts, path)?;
        // This metadata is not rebuilt from the message WAL. Each successful
        // mutation must survive a host crash: dedup precedes the enqueue reply,
        // and dispatch cursors/completions precede their acknowledgement. Batch
        // callers amortize this barrier across one atomic metadata epoch.
        let mut sync_writes = WriteOptions::default();
        sync_writes.set_sync(true);
        Self::migrate_completion_keys(&db, &sync_writes)?;
        let migration_ms = if let Some(bytes) = db.get(b"_meta/dedup-migration-ms")? {
            u64::from_le_bytes(bytes.try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid dedup migration epoch",
                )
            })?)
        } else {
            let now = now_ms();
            db.put_opt(b"_meta/dedup-migration-ms", now.to_le_bytes(), &sync_writes)?;
            now
        };
        let db = std::sync::Arc::new(db);
        let sync_writes = std::sync::Arc::new(sync_writes);
        let control = std::sync::Arc::new(RocksControl {
            mutation: Mutex::new(()),
            sweep_cursor: Mutex::new(Vec::new()),
            operations: AtomicU64::new(0),
            migration_ms,
            reverse_ready: std::sync::atomic::AtomicBool::new(false),
        });
        let direct = Self {
            backend: MetaBackend::Rocks {
                db: db.clone(),
                sync_writes: sync_writes.clone(),
                control: control.clone(),
            },
            retention_ms,
            writer: None,
        };
        let writer = MetadataWriter::start(direct)?;
        Ok(Self {
            backend: MetaBackend::Rocks {
                db,
                sync_writes,
                control,
            },
            retention_ms,
            writer: Some(writer),
        })
    }

    pub fn is_shared(&self) -> bool {
        matches!(self.backend, MetaBackend::Shared(_))
    }

    pub fn try_lock_producers(
        &self,
        tenant: &str,
        keys: &[String],
    ) -> Result<Option<crate::ProducerLocks>, IndexError> {
        self.check_ready()?;
        match &self.backend {
            MetaBackend::Shared(store) => Ok(store.try_lock_producers(tenant, keys)?),
            _ => Err(IndexError::Validation(
                "global producer admission requires shared metadata".into(),
            )),
        }
    }

    /// Dedup + dispatch cursors on the same object store as Slate messages (HA M3).
    #[cfg(feature = "slate")]
    pub fn open_slate(
        object_store: std::sync::Arc<dyn object_store::ObjectStore>,
        local_cache_dir: impl AsRef<Path>,
    ) -> Result<Self, IndexError> {
        let store = SlateMetadataStore::open(object_store, local_cache_dir)?;
        Ok(Self {
            backend: MetaBackend::Slate(store),
            retention_ms: dedup_retention_ms()?,
            writer: None,
        })
    }

    pub fn is_ready(&self) -> bool {
        if self
            .writer
            .as_ref()
            .is_some_and(|writer| writer.check_ready().is_err())
        {
            return false;
        }
        match &self.backend {
            #[cfg(feature = "slate")]
            MetaBackend::Slate(store) => store.is_ready(),
            MetaBackend::Shared(store) => store.check_ready().is_ok(),
            _ => true,
        }
    }

    pub fn check_ready(&self) -> Result<(), IndexError> {
        if let Some(writer) = &self.writer {
            writer.check_ready()?;
        }
        if let MetaBackend::Shared(store) = &self.backend {
            store.check_ready()?;
        }
        #[cfg(feature = "slate")]
        if let MetaBackend::Slate(store) = &self.backend {
            store.check_ready()?;
        }
        Ok(())
    }

    /// Atomically apply one metadata epoch on RocksDB.
    ///
    /// Shared metadata appends the entire batch under one lock and one fdatasync.
    /// Slate remains a single-broker compatibility backend.
    pub fn write_batch(&self, batch: MetadataWriteBatch) -> Result<(), IndexError> {
        if batch.operations.is_empty() {
            return Ok(());
        }
        if let Some(writer) = &self.writer {
            return writer.write_sync(batch);
        }
        self.write_batch_direct(batch, 1)
    }

    /// Local RocksDB metadata supports bounded asynchronous group admission.
    /// Other backends retain their synchronous compatibility path.
    pub fn supports_group_commit(&self) -> bool {
        self.writer.is_some()
    }

    /// Queue one atomic caller batch and transfer cancellation-sensitive guards
    /// into the writer. The receipt retains them through caller bookkeeping.
    pub async fn write_batch_grouped<G: Send + 'static>(
        &self,
        batch: MetadataWriteBatch,
        held: G,
    ) -> Result<MetadataWriteReceipt, IndexError> {
        let writer = self.writer.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "metadata backend has no group writer",
            )
        })?;
        writer.write(batch, Box::new(held)).await
    }

    /// Batched local write epochs since open, excluding maintenance barriers.
    pub fn group_commit_epochs(&self) -> Option<u64> {
        self.writer.as_ref().map(|writer| writer.epoch_count())
    }

    fn write_batch_direct(
        &self,
        batch: MetadataWriteBatch,
        logical_requests: u64,
    ) -> Result<(), IndexError> {
        if batch.operations.is_empty() {
            return Ok(());
        }
        match &self.backend {
            MetaBackend::Rocks {
                db,
                sync_writes,
                control,
            } => {
                let _mutation = control
                    .mutation
                    .lock()
                    .map_err(|_| std::io::Error::other("metadata mutation lock poisoned"))?;
                for validate in &batch.validators {
                    validate()?;
                }
                let mut rocks_batch = WriteBatch::default();
                let mut dedup = std::collections::HashMap::<Vec<u8>, Option<DedupEntry>>::new();
                let mut cursors = std::collections::HashMap::<Vec<u8>, u64>::new();
                let mut completions = Vec::new();
                for operation in batch.operations {
                    match operation {
                        MetadataOperation::DeleteDedupIf {
                            tenant_id,
                            key,
                            expected,
                        } => {
                            let key = Self::dedup_key(&tenant_id, &key);
                            let current = match dedup.get(&key) {
                                Some(entry) => entry.clone(),
                                None => db
                                    .get(&key)?
                                    .map(|bytes| DedupEntry::decoded(&bytes, control.migration_ms))
                                    .transpose()?,
                            };
                            if current
                                .as_ref()
                                .is_some_and(|entry| entry.same_identity(&expected))
                            {
                                rocks_batch.delete(&key);
                                dedup.insert(key, None);
                            }
                        }
                        MetadataOperation::ComparePutDedup {
                            tenant_id,
                            key,
                            mut entry,
                            expected,
                        } => {
                            let retention = self.retention_ms;
                            let key = Self::dedup_key(&tenant_id, &key);
                            let current = match dedup.get(&key) {
                                Some(entry) => entry.clone(),
                                None => db
                                    .get(&key)?
                                    .map(|bytes| DedupEntry::decoded(&bytes, control.migration_ms))
                                    .transpose()?
                                    .filter(|entry| !entry.expired(now_ms(), retention)),
                            };
                            if current
                                .as_ref()
                                .is_some_and(|current| current.same_identity(&entry))
                            {
                                continue;
                            }
                            if current.as_ref().map(|entry| entry.message_id) != expected {
                                return Err(IndexError::DedupConflict);
                            }
                            if entry.inserted_at_ms == 0 {
                                entry.inserted_at_ms = now_ms();
                            }
                            rocks_batch.put(&key, entry.encoded()?);
                            dedup.insert(key, Some(entry));
                        }
                        MetadataOperation::PutDedup {
                            tenant_id,
                            key,
                            mut entry,
                        } => {
                            if entry.inserted_at_ms == 0 {
                                entry.inserted_at_ms = now_ms();
                            }
                            let key = Self::dedup_key(&tenant_id, &key);
                            rocks_batch.put(&key, entry.encoded()?);
                            dedup.insert(key, Some(entry));
                        }
                        MetadataOperation::DeleteDedup { tenant_id, key } => {
                            let key = Self::dedup_key(&tenant_id, &key);
                            rocks_batch.delete(&key);
                            dedup.insert(key, None);
                        }
                        MetadataOperation::SetDispatchOffset {
                            tenant_id,
                            subscription_id,
                            partition,
                            next_offset,
                        } => {
                            let key = Self::dispatch_key(&tenant_id, &subscription_id, partition);
                            let current = match cursors.get(&key) {
                                Some(value) => *value,
                                None => db
                                    .get(&key)?
                                    .map(|bytes| {
                                        u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]))
                                    })
                                    .unwrap_or(0),
                            };
                            let next = current.max(next_offset);
                            if next == current {
                                continue;
                            }
                            cursors.insert(key.clone(), next);
                            rocks_batch.put(key, next.to_le_bytes());
                            let prefix =
                                Self::complete_prefix(&tenant_id, &subscription_id, partition);
                            let end = Self::ordered_complete_key(
                                &tenant_id,
                                &subscription_id,
                                partition,
                                next,
                            );
                            for item in db.prefix_iterator(&prefix) {
                                let (key, _) = item?;
                                if !key.starts_with(&prefix) || key.as_ref() >= end.as_slice() {
                                    break;
                                }
                                let offset = Self::ordered_offset(&key, prefix.len())?;
                                rocks_batch.delete(Self::complete_key(
                                    &tenant_id,
                                    &subscription_id,
                                    partition,
                                    offset,
                                ));
                                rocks_batch.delete(key);
                            }
                        }
                        MetadataOperation::MarkDispatchComplete {
                            tenant_id,
                            subscription_id,
                            partition,
                            offset,
                            message_id,
                        } => completions.push((
                            tenant_id,
                            subscription_id,
                            partition,
                            offset,
                            message_id,
                        )),
                    }
                }
                for (tenant, sub, partition, offset, message_id) in completions {
                    let cursor_key = Self::dispatch_key(&tenant, &sub, partition);
                    let cursor = match cursors.get(&cursor_key) {
                        Some(value) => *value,
                        None => db
                            .get(cursor_key)?
                            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])))
                            .unwrap_or(0),
                    };
                    if offset >= cursor {
                        for key in [
                            Self::complete_key(&tenant, &sub, partition, offset),
                            Self::ordered_complete_key(&tenant, &sub, partition, offset),
                        ] {
                            match message_id {
                                Some(id) => rocks_batch.put(key, id.as_bytes()),
                                None => rocks_batch.put(key, [1u8]),
                            }
                        }
                    }
                }
                // Derived UUID lookups share primary writer ordering. A lazy
                // initial build must not miss keys inserted afterwards.
                if control.reverse_ready.load(Ordering::Acquire) {
                    for (key, entry) in &dedup {
                        if let Some(value) = db.get(key)? {
                            let old = DedupEntry::decoded(&value, control.migration_ms)?;
                            rocks_batch.delete(Self::reverse_key(old.message_id, key));
                        }
                        if let Some(entry) = entry {
                            rocks_batch.put(Self::reverse_key(entry.message_id, key), []);
                        }
                    }
                }
                db.write_opt(rocks_batch, sync_writes)?;
                // The primary batch is already durable. Only a storage failure in
                // the follow-up sweep makes the store's state uncertain; a stale
                // or undecodable row must not fence the writer or fail callers
                // whose writes committed.
                match self.sweep_rocks(db, sync_writes, control, logical_requests) {
                    Err(error @ IndexError::Rocks(_)) => {
                        Err(IndexError::Uncertain(error.to_string()))
                    }
                    Err(error) => {
                        tracing::warn!(%error, "metadata expiry sweep skipped");
                        Ok(())
                    }
                    Ok(()) => Ok(()),
                }
            }
            MetaBackend::Shared(store) => {
                store.apply_batch_validated(batch.operations, batch.validators)?;
                Ok(())
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(_) => {
                for validate in &batch.validators {
                    validate()?;
                }
                for operation in batch.operations {
                    match operation {
                        MetadataOperation::DeleteDedupIf {
                            tenant_id,
                            key,
                            expected,
                        } => {
                            if self
                                .get_dedup(&tenant_id, &key)?
                                .is_some_and(|entry| entry.same_identity(&expected))
                            {
                                self.delete_dedup(&tenant_id, &key)?;
                            }
                        }
                        MetadataOperation::ComparePutDedup {
                            tenant_id,
                            key,
                            entry,
                            expected,
                        } => {
                            let current = self.get_dedup(&tenant_id, &key)?;
                            if current
                                .as_ref()
                                .is_some_and(|current| current.same_identity(&entry))
                            {
                                continue;
                            }
                            if current.as_ref().map(|entry| entry.message_id) != expected {
                                return Err(IndexError::DedupConflict);
                            }
                            self.put_dedup(&tenant_id, &key, &entry)?;
                        }
                        MetadataOperation::PutDedup {
                            tenant_id,
                            key,
                            entry,
                        } => self.put_dedup(&tenant_id, &key, &entry)?,
                        MetadataOperation::DeleteDedup { tenant_id, key } => {
                            self.delete_dedup(&tenant_id, &key)?
                        }
                        MetadataOperation::SetDispatchOffset {
                            tenant_id,
                            subscription_id,
                            partition,
                            next_offset,
                        } => self.set_dispatch_offset(
                            &tenant_id,
                            &subscription_id,
                            partition,
                            next_offset,
                        )?,
                        // Slate cells share one object-store log, so offsets are
                        // never reused across leaders; markers stay identity-less.
                        MetadataOperation::MarkDispatchComplete {
                            tenant_id,
                            subscription_id,
                            partition,
                            offset,
                            ..
                        } => self.mark_dispatch_complete(
                            &tenant_id,
                            &subscription_id,
                            partition,
                            offset,
                        )?,
                    }
                }
                Ok(())
            }
        }
    }

    fn dedup_key(tenant_id: &str, idempotency_key: &str) -> Vec<u8> {
        format!("dedup:{tenant_id}:{idempotency_key}").into_bytes()
    }

    /// Bounded ordered scan of the authoritative TTL index, including keys
    /// whose WAL frames have already been collected. Cursor counts scanned
    /// rows, so expired entries cannot create a nonadvancing empty page.
    pub fn dedup_page(&self, after: Option<(String, String)>) -> Result<DedupPage, IndexError> {
        self.check_ready()?;
        match &self.backend {
            MetaBackend::Shared(store) => Ok(store.dedup_page(after)?),
            #[cfg(feature = "slate")]
            MetaBackend::Slate(_) => Err(IndexError::Validation(
                "local-WAL dedup export is not supported by Slate".into(),
            )),
            MetaBackend::Rocks { db, control, .. } => {
                let start = after
                    .as_ref()
                    .map(|(tenant, key)| Self::dedup_key(tenant, key))
                    .unwrap_or_else(|| b"dedup:".to_vec());
                let mut records = Vec::new();
                let mut count = 0usize;
                let mut bytes = 0usize;
                for row in db.iterator(rocksdb::IteratorMode::From(
                    &start,
                    rocksdb::Direction::Forward,
                )) {
                    let (key, value) = row?;
                    if !key.starts_with(b"dedup:") {
                        break;
                    }
                    if after.is_some() && key.as_ref() <= start.as_slice() {
                        continue;
                    }
                    let key_text = std::str::from_utf8(&key[6..]).map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error)
                    })?;
                    let (tenant, producer_key) = key_text.split_once(':').ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, "malformed dedup key")
                    })?;
                    let next = Some((tenant.to_owned(), producer_key.to_owned()));
                    count += 1;
                    bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
                    let entry = DedupEntry::decoded(&value, control.migration_ms)?;
                    if !entry.expired(now_ms(), self.retention_ms) {
                        records.push(DedupRecord {
                            tenant_id: tenant.to_owned(),
                            key: producer_key.to_owned(),
                            entry,
                        });
                    }
                    if count >= 256 || bytes >= 4 * 1024 * 1024 {
                        return Ok(DedupPage { records, next });
                    }
                }
                Ok(DedupPage {
                    records,
                    next: None,
                })
            }
        }
    }

    pub fn get_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<DedupEntry>, IndexError> {
        self.check_ready()?;
        match &self.backend {
            MetaBackend::Shared(s) => Ok(s.get_dedup(tenant_id, idempotency_key)?),
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => Ok(s
                .get_dedup(tenant_id, idempotency_key)?
                .filter(|entry| !entry.expired(now_ms(), self.retention_ms))),
            MetaBackend::Rocks {
                db,
                sync_writes,
                control,
            } => {
                let key = Self::dedup_key(tenant_id, idempotency_key);
                match db.get(&key)? {
                    Some(bytes) => {
                        let entry = DedupEntry::decoded(&bytes, control.migration_ms)?;
                        if entry.expired(now_ms(), self.retention_ms) {
                            if let Some(writer) = &self.writer {
                                let tenant_id = tenant_id.to_owned();
                                let idempotency_key = idempotency_key.to_owned();
                                return writer.exclusive(move |store| {
                                    store.get_dedup(&tenant_id, &idempotency_key)
                                });
                            }
                            // Healthy lookups never wait for an unrelated
                            // delivery's fsync. Only expiration mutates; reread
                            // under the lock so it cannot delete a fresh retry.
                            let _mutation = control
                                .mutation
                                .lock()
                                .map_err(|_| std::io::Error::other("metadata lock poisoned"))?;
                            match db.get(&key)? {
                                Some(bytes) => {
                                    let current =
                                        DedupEntry::decoded(&bytes, control.migration_ms)?;
                                    if current.expired(now_ms(), self.retention_ms) {
                                        let mut batch = WriteBatch::default();
                                        batch.delete(Self::reverse_key(current.message_id, &key));
                                        batch.delete(key);
                                        db.write_opt(batch, sync_writes)?;
                                        Ok(None)
                                    } else {
                                        Ok(Some(current))
                                    }
                                }
                                None => Ok(None),
                            }
                        } else {
                            Ok(Some(entry))
                        }
                    }
                    None => Ok(None),
                }
            }
        }
    }

    pub fn confirmed_dedup(
        &self,
        tenant: &str,
        key: &str,
    ) -> Result<Option<DedupEntry>, IndexError> {
        self.check_ready()?;
        match &self.backend {
            MetaBackend::Shared(store) => Ok(store.confirmed_dedup(tenant, key)?),
            _ => self.get_dedup(tenant, key),
        }
    }

    pub fn put_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
        entry: &DedupEntry,
    ) -> Result<(), IndexError> {
        let mut entry = entry.clone();
        if entry.inserted_at_ms == 0 {
            entry.inserted_at_ms = now_ms();
        }
        match &self.backend {
            MetaBackend::Shared(s) => {
                s.put_dedup(tenant_id, idempotency_key, &entry)?;
                Ok(())
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => {
                s.put_dedup(tenant_id, idempotency_key, &entry)?;
                Ok(())
            }
            MetaBackend::Rocks { .. } => {
                let mut batch = MetadataWriteBatch::default();
                batch.put_dedup(tenant_id, idempotency_key, entry);
                self.write_batch(batch)
            }
        }
    }

    pub fn delete_dedup(&self, tenant_id: &str, idempotency_key: &str) -> Result<(), IndexError> {
        match &self.backend {
            MetaBackend::Shared(s) => {
                s.delete_dedup(tenant_id, idempotency_key)?;
                Ok(())
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => {
                s.delete_dedup(tenant_id, idempotency_key)?;
                Ok(())
            }
            MetaBackend::Rocks { .. } => {
                let mut batch = MetadataWriteBatch::default();
                batch.delete_dedup(tenant_id, idempotency_key);
                self.write_batch(batch)
            }
        }
    }

    fn dispatch_key(tenant_id: &str, subscription_id: &str, partition: u32) -> Vec<u8> {
        format!("disp:{tenant_id}:{subscription_id}:p{partition}").into_bytes()
    }

    pub fn dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
    ) -> Result<u64, IndexError> {
        self.check_ready()?;
        match &self.backend {
            MetaBackend::Shared(s) => {
                Ok(s.dispatch_offset(tenant_id, subscription_id, partition)?)
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => {
                Ok(s.dispatch_offset(tenant_id, subscription_id, partition)?)
            }
            MetaBackend::Rocks { db, .. } => {
                let key = Self::dispatch_key(tenant_id, subscription_id, partition);
                match db.get(key)? {
                    Some(bytes) => Ok(u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]))),
                    None => Ok(0),
                }
            }
        }
    }

    /// Advance a lane cursor. Cursors are monotonic: a value at or below the
    /// stored cursor is ignored, so a stale concurrent completion can never
    /// move it backwards (and the completion markers it covered are gone).
    pub fn set_dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        next_offset: u64,
    ) -> Result<(), IndexError> {
        match &self.backend {
            MetaBackend::Shared(s) => {
                s.set_dispatch_offset(tenant_id, subscription_id, partition, next_offset)?;
                Ok(())
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => {
                s.set_dispatch_offset(tenant_id, subscription_id, partition, next_offset)?;
                Ok(())
            }
            MetaBackend::Rocks { .. } => {
                let mut batch = MetadataWriteBatch::default();
                batch.set_dispatch_offset(tenant_id, subscription_id, partition, next_offset);
                self.write_batch(batch)
            }
        }
    }

    fn complete_key(
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Vec<u8> {
        format!("dcomp:{tenant_id}:{subscription_id}:p{partition}:{offset}").into_bytes()
    }

    fn ordered_complete_key(
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Vec<u8> {
        format!("dcomp2:{tenant_id}:{subscription_id}:p{partition}:{offset:020}").into_bytes()
    }

    fn complete_prefix(tenant_id: &str, subscription_id: &str, partition: u32) -> Vec<u8> {
        format!("dcomp2:{tenant_id}:{subscription_id}:p{partition}:").into_bytes()
    }

    fn ordered_offset(key: &[u8], prefix_len: usize) -> Result<u64, IndexError> {
        std::str::from_utf8(&key[prefix_len..])
            .ok()
            .and_then(|offset| offset.parse().ok())
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid ordered completion key",
                )
                .into()
            })
    }

    /// Decimal authoritative keys sort lexically (10 before 2). Build an
    /// ordered, derived cleanup index in bounded atomic batches, preserving the
    /// original markers so older binaries still understand every completion.
    /// Rebuild on every open to incorporate writes made by an older binary.
    fn migrate_completion_keys(db: &DB, options: &WriteOptions) -> Result<(), IndexError> {
        let mut batch = WriteBatch::default();
        let mut count = 0;
        for item in db.prefix_iterator(b"dcomp:") {
            let (key, value) = item?;
            let Some(rest) = key.strip_prefix(b"dcomp:") else {
                break;
            };
            let rest = std::str::from_utf8(rest)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            let (lane, offset) = rest.rsplit_once(':').ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid completion key")
            })?;
            let offset: u64 = offset
                .parse()
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            batch.put(format!("dcomp2:{lane}:{offset:020}"), value);
            count += 1;
            if count % 512 == 0 {
                db.write_opt(std::mem::take(&mut batch), options)?;
            }
        }
        if !batch.is_empty() {
            db.write_opt(batch, options)?;
        }
        Ok(())
    }

    pub fn mark_dispatch_complete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), IndexError> {
        match &self.backend {
            MetaBackend::Shared(s) => {
                s.mark_dispatch_complete(tenant_id, subscription_id, partition, offset)?;
                Ok(())
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => {
                s.mark_dispatch_complete(tenant_id, subscription_id, partition, offset)?;
                Ok(())
            }
            MetaBackend::Rocks { .. } => {
                let mut batch = MetadataWriteBatch::default();
                batch.mark_dispatch_complete(tenant_id, subscription_id, partition, offset);
                self.write_batch(batch)
            }
        }
    }

    /// Mark `offset` complete for the delivery of `message_id`, so a later
    /// reader can tell whether the marker belongs to the record now stored at
    /// that offset.
    pub fn mark_dispatch_complete_for(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
        message_id: Uuid,
    ) -> Result<(), IndexError> {
        let mut batch = MetadataWriteBatch::default();
        batch.mark_dispatch_complete_for(tenant_id, subscription_id, partition, offset, message_id);
        self.write_batch(batch)
    }

    /// The completion marker at `offset` (ignores the lane cursor).
    pub fn dispatch_completion(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<DispatchCompletion, IndexError> {
        self.check_ready()?;
        match &self.backend {
            MetaBackend::Shared(s) => {
                Ok(s.dispatch_completion(tenant_id, subscription_id, partition, offset)?)
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => Ok(
                if s.is_dispatch_complete(tenant_id, subscription_id, partition, offset)? {
                    DispatchCompletion::Legacy
                } else {
                    DispatchCompletion::Missing
                },
            ),
            MetaBackend::Rocks { db, .. } => {
                let key = Self::complete_key(tenant_id, subscription_id, partition, offset);
                Ok(match db.get(key)? {
                    None => DispatchCompletion::Missing,
                    Some(bytes) => Uuid::from_slice(&bytes)
                        .map(DispatchCompletion::Message)
                        .unwrap_or(DispatchCompletion::Legacy),
                })
            }
        }
    }

    /// Drop a completion marker that belongs to a different record.
    pub fn clear_dispatch_complete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), IndexError> {
        if let Some(writer) = &self.writer {
            let tenant = tenant_id.to_owned();
            let subscription = subscription_id.to_owned();
            return writer.exclusive(move |store| {
                store.clear_dispatch_complete(&tenant, &subscription, partition, offset)
            });
        }
        match &self.backend {
            MetaBackend::Shared(s) => {
                s.clear_dispatch_complete(tenant_id, subscription_id, partition, offset)?;
                Ok(())
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(_) => Ok(()),
            MetaBackend::Rocks {
                db,
                sync_writes,
                control,
            } => {
                let _mutation = control
                    .mutation
                    .lock()
                    .map_err(|_| std::io::Error::other("metadata mutation lock poisoned"))?;
                let key = Self::complete_key(tenant_id, subscription_id, partition, offset);
                let mut batch = WriteBatch::default();
                batch.delete(key);
                batch.delete(Self::ordered_complete_key(
                    tenant_id,
                    subscription_id,
                    partition,
                    offset,
                ));
                db.write_opt(batch, sync_writes)?;
                Ok(())
            }
        }
    }

    /// Delete only the identity observed stale by the caller. A queued cleanup
    /// must not erase a newer, correct completion written before it executes.
    pub fn clear_dispatch_complete_for(
        &self,
        tenant: &str,
        subscription: &str,
        partition: u32,
        offset: u64,
        stale_id: Uuid,
    ) -> Result<bool, IndexError> {
        if let Some(writer) = &self.writer {
            let tenant = tenant.to_owned();
            let subscription = subscription.to_owned();
            return writer.exclusive(move |store| {
                store.clear_dispatch_complete_for(
                    &tenant,
                    &subscription,
                    partition,
                    offset,
                    stale_id,
                )
            });
        }
        match &self.backend {
            MetaBackend::Shared(store) => Ok(store.clear_dispatch_complete_for(
                tenant,
                subscription,
                partition,
                offset,
                stale_id,
            )?),
            #[cfg(feature = "slate")]
            MetaBackend::Slate(_) => Ok(false),
            MetaBackend::Rocks {
                db,
                sync_writes,
                control,
            } => {
                let _mutation = control
                    .mutation
                    .lock()
                    .map_err(|_| std::io::Error::other("metadata mutation lock poisoned"))?;
                let key = Self::complete_key(tenant, subscription, partition, offset);
                if db.get(&key)?.as_deref() != Some(stale_id.as_bytes().as_slice()) {
                    return Ok(false);
                }
                let mut batch = WriteBatch::default();
                batch.delete(key);
                batch.delete(Self::ordered_complete_key(
                    tenant,
                    subscription,
                    partition,
                    offset,
                ));
                db.write_opt(batch, sync_writes)?;
                Ok(true)
            }
        }
    }

    /// Every lane cursor of `partition` as `(tenant, lane, next_offset)`.
    pub fn dispatch_offsets_for_partition(
        &self,
        partition: u32,
    ) -> Result<Vec<(String, String, u64)>, IndexError> {
        Ok(self
            .dispatch_offsets()?
            .into_iter()
            .filter(|(_, _, p, _)| *p == partition)
            .map(|(tenant, lane, _, offset)| (tenant, lane, offset))
            .collect())
    }

    /// Every lane cursor as `(tenant, lane, partition, next_offset)`. Slate
    /// stores cursors in the shared object store, where every node already
    /// sees them, so it reports none.
    pub fn dispatch_offsets(&self) -> Result<Vec<(String, String, u32, u64)>, IndexError> {
        self.check_ready()?;
        match &self.backend {
            MetaBackend::Shared(s) => Ok(s.dispatch_offsets()?),
            #[cfg(feature = "slate")]
            MetaBackend::Slate(_) => Ok(Vec::new()),
            MetaBackend::Rocks { db, .. } => {
                let parse = |key: &str| -> Option<(String, String, u32)> {
                    let (rest, partition) = key.rsplit_once(":p")?;
                    let partition = partition.parse().ok()?;
                    let (tenant, lane) = rest.rsplit_once(':')?;
                    Some((tenant.to_string(), lane.to_string(), partition))
                };
                let prefix = b"disp:";
                let mut out = Vec::new();
                for item in db.prefix_iterator(prefix) {
                    let (key, value) = item?;
                    if !key.starts_with(prefix) {
                        break;
                    }
                    let Ok(key) = std::str::from_utf8(&key[prefix.len()..]) else {
                        continue;
                    };
                    if let Some((tenant, lane, partition)) = parse(key) {
                        let offset =
                            u64::from_le_bytes(value.as_ref().try_into().unwrap_or([0; 8]));
                        out.push((tenant, lane, partition, offset));
                    }
                }
                Ok(out)
            }
        }
    }

    pub fn is_dispatch_complete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, IndexError> {
        if offset < self.dispatch_offset(tenant_id, subscription_id, partition)? {
            return Ok(true);
        }
        match &self.backend {
            MetaBackend::Shared(s) => {
                Ok(s.is_dispatch_complete(tenant_id, subscription_id, partition, offset)?)
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => {
                Ok(s.is_dispatch_complete(tenant_id, subscription_id, partition, offset)?)
            }
            MetaBackend::Rocks { db, .. } => {
                let key = Self::complete_key(tenant_id, subscription_id, partition, offset);
                Ok(db.get(key)?.is_some())
            }
        }
    }

    /// Additional RocksDB WAL barrier for shutdown/checkpoints. Normal writes
    /// are already synchronous. No-op for shared-file / slate.
    pub fn flush_epoch(&self) -> Result<(), IndexError> {
        if let Some(writer) = &self.writer {
            return writer.exclusive(Self::flush_epoch);
        }
        match &self.backend {
            MetaBackend::Rocks { db, .. } => {
                db.flush_wal(true)?;
                Ok(())
            }
            // Every shared epoch already completed fdatasync before success.
            MetaBackend::Shared(store) => {
                store.check_ready()?;
                Ok(())
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(_) => Ok(()),
        }
    }

    fn sweep_rocks(
        &self,
        db: &DB,
        options: &WriteOptions,
        control: &RocksControl,
        logical_requests: u64,
    ) -> Result<(), IndexError> {
        let before = control
            .operations
            .fetch_add(logical_requests, Ordering::Relaxed);
        if before / 256 == before.saturating_add(logical_requests) / 256 {
            return Ok(());
        }
        let mut cursor = control
            .sweep_cursor
            .lock()
            .map_err(|_| std::io::Error::other("metadata sweep lock poisoned"))?;
        let start = if cursor.is_empty() {
            b"dedup:".to_vec()
        } else {
            cursor.clone()
        };
        let mut batch = WriteBatch::default();
        let mut visited = 0;
        for item in db
            .iterator(rocksdb::IteratorMode::From(
                &start,
                rocksdb::Direction::Forward,
            ))
            .take(512)
        {
            let (key, value) = item?;
            if !key.starts_with(b"dedup:") {
                break;
            }
            let entry = DedupEntry::decoded(&value, control.migration_ms)?;
            if entry.expired(now_ms(), self.retention_ms) {
                batch.delete(Self::reverse_key(entry.message_id, &key));
                batch.delete(&key);
            } else if value.len() == 36 {
                batch.put(&key, entry.encoded()?);
            }
            *cursor = key.to_vec();
            cursor.push(0);
            visited += 1;
        }
        if visited < 512 {
            cursor.clear();
        }
        if !batch.is_empty() {
            db.write_opt(batch, options)?;
        }
        Ok(())
    }

    /// Explicit rewind only while dispatch is fenced; normal writes use max.
    pub fn reset_dispatch_offset(
        &self,
        tenant: &str,
        sub: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), IndexError> {
        if let Some(writer) = &self.writer {
            let tenant = tenant.to_owned();
            let sub = sub.to_owned();
            return writer.exclusive(move |store| {
                store.reset_dispatch_offset(&tenant, &sub, partition, offset)
            });
        }
        match &self.backend {
            MetaBackend::Shared(store) => {
                store.reset_dispatch_offset(tenant, sub, partition, offset)?;
                Ok(())
            }
            MetaBackend::Rocks {
                db,
                sync_writes,
                control,
            } => {
                let _mutation = control
                    .mutation
                    .lock()
                    .map_err(|_| std::io::Error::other("metadata lock poisoned"))?;
                let mut batch = WriteBatch::default();
                batch.put(
                    Self::dispatch_key(tenant, sub, partition),
                    offset.to_le_bytes(),
                );
                let prefix = Self::complete_prefix(tenant, sub, partition);
                for item in db.prefix_iterator(&prefix) {
                    let (key, _) = item?;
                    if !key.starts_with(&prefix) {
                        break;
                    }
                    let completed = Self::ordered_offset(&key, prefix.len())?;
                    batch.delete(Self::complete_key(tenant, sub, partition, completed));
                    batch.delete(key);
                }
                db.write_opt(batch, sync_writes)?;
                Ok(())
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Slate dispatch rewind is unsupported",
            )
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn conditional_cleanup_cannot_delete_a_newer_completion_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = super::MetadataStore::open(dir.path()).unwrap();
        let old = uuid::Uuid::new_v4();
        let current = uuid::Uuid::new_v4();
        store
            .mark_dispatch_complete_for("tenant", "lane", 0, 5, old)
            .unwrap();
        assert_eq!(
            store.dispatch_completion("tenant", "lane", 0, 5).unwrap(),
            super::DispatchCompletion::Message(old)
        );
        store
            .mark_dispatch_complete_for("tenant", "lane", 0, 5, current)
            .unwrap();
        assert!(!store
            .clear_dispatch_complete_for("tenant", "lane", 0, 5, old)
            .unwrap());
        assert_eq!(
            store.dispatch_completion("tenant", "lane", 0, 5).unwrap(),
            super::DispatchCompletion::Message(current)
        );
        assert!(store
            .clear_dispatch_complete_for("tenant", "lane", 0, 5, current)
            .unwrap());
        assert_eq!(
            store.dispatch_completion("tenant", "lane", 0, 5).unwrap(),
            super::DispatchCompletion::Missing
        );
    }

    #[test]
    fn shared_conditional_cleanup_preserves_a_newer_completion_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = super::SharedMetadataStore::open(dir.path()).unwrap();
        let old = uuid::Uuid::new_v4();
        let current = uuid::Uuid::new_v4();
        for id in [old, current] {
            let mut batch = super::MetadataWriteBatch::default();
            batch.mark_dispatch_complete_for("tenant", "lane", 0, 5, id);
            store.apply_batch(batch.operations).unwrap();
        }
        assert!(!store
            .clear_dispatch_complete_for("tenant", "lane", 0, 5, old)
            .unwrap());
        assert_eq!(
            store.dispatch_completion("tenant", "lane", 0, 5).unwrap(),
            super::DispatchCompletion::Message(current)
        );
        assert!(store
            .clear_dispatch_complete_for("tenant", "lane", 0, 5, current)
            .unwrap());
        assert_eq!(
            store.dispatch_completion("tenant", "lane", 0, 5).unwrap(),
            super::DispatchCompletion::Missing
        );
    }

    #[test]
    fn synchronous_metadata_is_reentrant_inside_a_futures_executor() {
        let dir = tempfile::tempdir().unwrap();
        let store = super::MetadataStore::open(dir.path()).unwrap();
        futures::executor::block_on(async {
            let mut expired = super::DedupEntry::new(uuid::Uuid::new_v4(), 0, 0, "topic".into());
            expired.inserted_at_ms = 1;
            store.put_dedup("tenant", "expired", &expired).unwrap();
            assert!(store.get_dedup("tenant", "expired").unwrap().is_none());
            store.set_dispatch_offset("tenant", "lane", 0, 10).unwrap();
            store.reset_dispatch_offset("tenant", "lane", 0, 2).unwrap();
            store.flush_epoch().unwrap();
        });
        assert_eq!(store.dispatch_offset("tenant", "lane", 0).unwrap(), 2);
    }

    #[tokio::test]
    async fn synchronous_metadata_does_not_exhaust_tokio_cooperative_budget() {
        const CHILD: &str = "BETTERMQ_TEST_SYNC_META_CHILD";
        if let Some(path) = std::env::var_os(CHILD) {
            let store = super::MetadataStore::open(std::path::PathBuf::from(path)).unwrap();
            // No yield: a nested channel bridge must not consume this task's
            // budget and then defer its only wake until this poll returns.
            for next in 1..300 {
                store
                    .set_dispatch_offset("tenant", "lane", 0, next)
                    .unwrap();
                if next % 8 == 0 {
                    store.flush_epoch().unwrap();
                }
            }
            assert_eq!(store.dispatch_offset("tenant", "lane", 0).unwrap(), 299);
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "indexes::tests::synchronous_metadata_does_not_exhaust_tokio_cooperative_budget",
                "--nocapture",
            ])
            .env(CHILD, dir.path())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let status = tokio::time::timeout(std::time::Duration::from_secs(10), child.wait())
            .await
            .expect("synchronous metadata exhausted its enclosing Tokio task's budget")
            .unwrap();
        assert!(status.success());
    }

    #[tokio::test]
    async fn prewrite_validation_failure_isolated_from_other_grouped_producer_keys() {
        use futures::stream::{FuturesUnordered, StreamExt};
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(super::MetadataStore::open(dir.path()).unwrap());
        if let super::MetaBackend::Rocks { db, .. } = &store.backend {
            let mut malformed = super::MetadataStore::complete_prefix("tenant", "corrupt", 0);
            malformed.push(0); // an ordered offset must have exactly eight bytes
            db.put(malformed, [1u8]).unwrap();
        } else {
            panic!("test requires local metadata");
        }
        let writer = store.writer.as_ref().unwrap().clone();
        let (started, start) = tokio::sync::oneshot::channel();
        let (release, gate) = std::sync::mpsc::sync_channel(1);
        *writer.gate.lock().unwrap() = Some((started, gate));
        let first_store = store.clone();
        let first = tokio::spawn(async move {
            let mut batch = super::MetadataWriteBatch::default();
            batch.set_dispatch_offset("tenant", "first", 0, 1);
            first_store.write_batch_grouped(batch, ()).await
        });
        start.await.unwrap();
        let mut pending = FuturesUnordered::new();
        let ids = [
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
        ];
        for (index, id) in ids.into_iter().enumerate() {
            let store = store.clone();
            pending.push(async move {
                let mut batch = super::MetadataWriteBatch::default();
                batch.put_dedup(
                    "tenant",
                    format!("key-{index}"),
                    super::DedupEntry::new(id, index as u64, 0, "topic".into()),
                );
                if index == 1 {
                    batch.set_dispatch_offset("tenant", "corrupt", 0, 2);
                }
                (index, store.write_batch_grouped(batch, ()).await)
            });
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), pending.next())
                .await
                .is_err()
        );
        assert_eq!(writer.available_queue_slots(), 256 - 3);
        release.send(()).unwrap();
        first.await.unwrap().unwrap();
        while let Some((index, result)) = pending.next().await {
            assert_eq!(
                result.is_ok(),
                index != 1,
                "only the malformed caller must fail"
            );
        }
        assert!(
            store.is_ready(),
            "a prewrite validation error is not an uncertain fsync"
        );
        for (index, id) in ids.into_iter().enumerate() {
            let entry = store.get_dedup("tenant", &format!("key-{index}")).unwrap();
            assert_eq!(
                entry.map(|entry| entry.message_id),
                (index != 1).then_some(id)
            );
        }
        drop(writer);
        drop(store);
        let reopened = super::MetadataStore::open(dir.path()).unwrap();
        assert_eq!(
            reopened
                .get_dedup("tenant", "key-0")
                .unwrap()
                .unwrap()
                .message_id,
            ids[0]
        );
        assert_eq!(
            reopened
                .get_dedup("tenant", "key-2")
                .unwrap()
                .unwrap()
                .message_id,
            ids[2]
        );
    }

    #[tokio::test]
    async fn uncertain_group_sync_failure_fences_reads_and_all_queued_acknowledgements() {
        use futures::stream::{FuturesUnordered, StreamExt};
        use std::sync::{atomic::Ordering, Arc};
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(super::MetadataStore::open(dir.path()).unwrap());
        let seed = super::DedupEntry::new(uuid::Uuid::new_v4(), 0, 0, "topic".into());
        store.put_dedup("tenant", "seed", &seed).unwrap();
        let writer = store.writer.as_ref().unwrap().clone();
        let (started, start) = tokio::sync::oneshot::channel();
        let (release, gate) = std::sync::mpsc::sync_channel(1);
        *writer.gate.lock().unwrap() = Some((started, gate));
        let first_store = store.clone();
        let first = tokio::spawn(async move {
            let mut batch = super::MetadataWriteBatch::default();
            batch.set_dispatch_offset("tenant", "first", 0, 1);
            first_store.write_batch_grouped(batch, ()).await
        });
        start.await.unwrap();
        let mut pending = FuturesUnordered::new();
        for index in 0..32 {
            let store = store.clone();
            pending.push(async move {
                let mut batch = super::MetadataWriteBatch::default();
                batch.set_dispatch_offset("tenant", "queued", 0, index);
                store.write_batch_grouped(batch, ()).await
            });
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), pending.next())
                .await
                .is_err()
        );
        writer.fail_next_sync.store(true, Ordering::SeqCst);
        release.send(()).unwrap();
        assert!(first
            .await
            .unwrap()
            .err()
            .unwrap()
            .to_string()
            .contains("uncertain"));
        while let Some(result) = pending.next().await {
            assert!(result.is_err());
        }
        assert!(!store.is_ready());
        assert!(store.get_dedup("tenant", "seed").is_err());
        assert!(store.dispatch_offset("tenant", "first", 0).is_err());
        assert!(store.flush_epoch().is_err());
        drop(writer);
        drop(store);
        let recovered = super::MetadataStore::open(dir.path()).unwrap();
        assert!(recovered.is_ready());
        assert_eq!(
            recovered
                .get_dedup("tenant", "seed")
                .unwrap()
                .unwrap()
                .message_id,
            seed.message_id
        );
        assert_eq!(recovered.dispatch_offset("tenant", "first", 0).unwrap(), 0);
    }

    #[tokio::test]
    async fn grouped_writes_bound_admission_and_keep_cancelled_ownership_until_durable() {
        use futures::stream::{FuturesUnordered, StreamExt};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(super::MetadataStore::open(dir.path()).unwrap());
        let entry = super::DedupEntry::new(uuid::Uuid::new_v4(), 0, 0, "topic".into());
        store.put_dedup("tenant", "healthy", &entry).unwrap();
        let writer = store.writer.as_ref().unwrap().clone();
        let epochs = writer.epoch_count();
        let dropped = Arc::new(AtomicUsize::new(0));
        struct Guard(Arc<AtomicUsize>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (started, start) = tokio::sync::oneshot::channel();
        let (release, gate) = std::sync::mpsc::sync_channel(1);
        *writer.gate.lock().unwrap() = Some((started, gate));
        let first_store = store.clone();
        let owned = Guard(dropped.clone());
        let first = tokio::spawn(async move {
            let mut batch = super::MetadataWriteBatch::default();
            batch.set_dispatch_offset("tenant", "cancelled", 0, 1);
            first_store.write_batch_grouped(batch, owned).await
        });
        start.await.unwrap();
        first.abort();
        assert!(first.await.err().unwrap().is_cancelled());
        let mut pending = FuturesUnordered::new();
        for index in 0..400 {
            let store = store.clone();
            pending.push(async move {
                let mut batch = super::MetadataWriteBatch::default();
                batch.mark_dispatch_complete_for("tenant", "lane", 0, index, uuid::Uuid::new_v4());
                batch.set_dispatch_offset("tenant", "lane", 0, index);
                store.write_batch_grouped(batch, ()).await
            });
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::select! {
                _ = pending.next() => panic!("no write may finish before the durability gate"),
                _ = async {
                    while writer.available_queue_slots() != 0 { tokio::task::yield_now().await; }
                } => {},
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), pending.next())
                .await
                .is_err(),
            "queue admission and durable replies must not block the runtime"
        );
        assert_eq!(
            writer.available_queue_slots(),
            0,
            "the bounded queue applies async backpressure"
        );
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            0,
            "cancelled callers cannot release queued guards"
        );
        assert_eq!(
            store
                .get_dedup("tenant", "healthy")
                .unwrap()
                .unwrap()
                .message_id,
            entry.message_id,
            "healthy reads do not wait on the writer's fsync"
        );
        release.send(()).unwrap();
        while let Some(result) = pending.next().await {
            drop(result.unwrap());
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(store.dispatch_offset("tenant", "lane", 0).unwrap(), 399);
        assert_eq!(
            store.dispatch_completion("tenant", "lane", 0, 398).unwrap(),
            super::DispatchCompletion::Missing,
            "a later epoch must not resurrect a marker below the final cursor"
        );
        assert!(
            writer.epoch_count() - epochs < 100,
            "ready caller batches must share durable barriers"
        );
        drop(writer);
        drop(store);
        let reopened = super::MetadataStore::open(dir.path()).unwrap();
        assert_eq!(
            reopened.dispatch_offset("tenant", "cancelled", 0).unwrap(),
            1
        );
        assert_eq!(reopened.dispatch_offset("tenant", "lane", 0).unwrap(), 399);
    }

    use super::*;
    use tempfile::tempdir;

    fn entry(offset: u64) -> DedupEntry {
        DedupEntry::new(Uuid::new_v4(), offset, 2, "jobs".into())
    }

    #[test]
    fn ordered_completion_index_preserves_identity_numeric_order_and_rollback() {
        let dir = tempdir().unwrap();
        let id = Uuid::new_v4();
        {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            let db = DB::open(&opts, dir.path()).unwrap();
            for offset in [0, 2, 9, 10, 99, 100, u64::MAX] {
                db.put(format!("dcomp:tenant:lane:p0:{offset}"), id.as_bytes())
                    .unwrap();
            }
            db.put("dcomp:tenant:other:p0:2", [1]).unwrap();
        }
        let store = MetadataStore::open(dir.path()).unwrap();
        if let MetaBackend::Rocks { db, .. } = &store.backend {
            assert!(
                db.get(b"dcomp:tenant:lane:p0:100").unwrap().is_some(),
                "an older binary must still understand the authoritative markers"
            );
        }
        for offset in [0, 2, 9, 10, 99, 100, u64::MAX] {
            assert_eq!(
                store
                    .dispatch_completion("tenant", "lane", 0, offset)
                    .unwrap(),
                DispatchCompletion::Message(id)
            );
        }
        store.set_dispatch_offset("tenant", "lane", 0, 10).unwrap();
        for offset in [0, 2, 9] {
            assert_eq!(
                store
                    .dispatch_completion("tenant", "lane", 0, offset)
                    .unwrap(),
                DispatchCompletion::Missing
            );
        }
        for offset in [10, 99, 100, u64::MAX] {
            assert_eq!(
                store
                    .dispatch_completion("tenant", "lane", 0, offset)
                    .unwrap(),
                DispatchCompletion::Message(id)
            );
        }
        assert_eq!(
            store.dispatch_completion("tenant", "other", 0, 2).unwrap(),
            DispatchCompletion::Legacy
        );
        drop(store);
        {
            // An older binary knows nothing about the ordered index.
            let db = DB::open(&Options::default(), dir.path()).unwrap();
            db.put(b"dcomp:tenant:lane:p0:200", id.as_bytes()).unwrap();
            db.delete(b"dcomp:tenant:lane:p0:100").unwrap();
        }
        let store = MetadataStore::open(dir.path()).unwrap();
        assert_eq!(
            store.dispatch_completion("tenant", "lane", 0, 100).unwrap(),
            DispatchCompletion::Missing
        );
        assert_eq!(
            store.dispatch_completion("tenant", "lane", 0, 200).unwrap(),
            DispatchCompletion::Message(id)
        );
        store.set_dispatch_offset("tenant", "lane", 0, 201).unwrap();
        assert_eq!(
            store.dispatch_completion("tenant", "lane", 0, 200).unwrap(),
            DispatchCompletion::Missing
        );
    }

    #[test]
    fn versioned_dedup_reads_legacy_values_without_magic_collision() {
        #[derive(Serialize)]
        struct Legacy {
            message_id: Uuid,
            offset: u64,
            partition: u32,
        }
        let old = Legacy {
            message_id: Uuid::new_v4(),
            offset: 9,
            partition: 2,
        };
        let mut bytes = broker_proto::encode(&old).unwrap();
        assert_eq!(bytes.len(), 36);
        let decoded = DedupEntry::decoded(&bytes, 123).unwrap();
        assert_eq!(decoded.message_id, old.message_id);
        assert_eq!(decoded.inserted_at_ms, 123);
        assert!(decoded.topic.is_empty());
        // UUID serialization includes its bincode length; even a misleading
        // prefix must select the legacy format by its exact length.
        bytes[..10].copy_from_slice(b"BMQDEDUP2\n");
        assert!(DedupEntry::decoded(&bytes, 123).is_err());
        let current = entry(19);
        let decoded = DedupEntry::decoded(&current.encoded().unwrap(), 1).unwrap();
        assert_eq!(decoded.message_id, current.message_id);
        assert_eq!(decoded.topic, "jobs");
    }

    #[test]
    fn shared_identity_refuses_missing_and_replacement_mounts() {
        const DIR: &str = "BETTERMQ_TEST_SHARED_IDENTITY_DIR";
        if let Some(dir) = std::env::var_os(DIR) {
            let dir = std::path::PathBuf::from(dir);
            let shared = dir.join("shared");
            let local = dir.join("local");
            drop(MetadataStore::open(&local).unwrap());
            std::fs::rename(&shared, dir.join("original-shared")).unwrap();
            std::fs::create_dir(&shared).unwrap();
            assert!(MetadataStore::open(&local).is_err());
            assert!(!shared.join("meta-manifest.json").exists());
            drop(SharedMetadataStore::open(&shared).unwrap());
            assert!(MetadataStore::open(&local).is_err());
            return;
        }
        let dir = tempdir().unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "indexes::tests::shared_identity_refuses_missing_and_replacement_mounts",
            ])
            .env(DIR, dir.path())
            .env("BETTERMQ_SHARED_META_DIR", dir.path().join("shared"))
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    #[test]
    fn rocks_expiry_sweep_and_legacy_grace_are_durable() {
        #[derive(Serialize)]
        struct Legacy {
            message_id: Uuid,
            offset: u64,
            partition: u32,
        }
        let dir = tempdir().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        let MetaBackend::Rocks {
            db,
            sync_writes,
            control,
        } = &store.backend
        else {
            panic!("expected local backend");
        };
        let mut old = entry(1);
        old.inserted_at_ms = now_ms() - store.retention_ms - 1;
        db.put_opt(
            MetadataStore::dedup_key("t", "expired"),
            old.encoded().unwrap(),
            sync_writes,
        )
        .unwrap();
        db.put_opt(
            MetadataStore::dedup_key("t", "legacy"),
            broker_proto::encode(&Legacy {
                message_id: old.message_id,
                offset: 7,
                partition: 2,
            })
            .unwrap(),
            sync_writes,
        )
        .unwrap();
        let migration_ms = control.migration_ms;
        control.operations.store(255, Ordering::Relaxed);
        store.put_dedup("t", "live", &entry(2)).unwrap();
        assert!(db
            .get(MetadataStore::dedup_key("t", "expired"))
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .get_dedup("t", "legacy")
                .unwrap()
                .unwrap()
                .inserted_at_ms,
            migration_ms
        );
        assert!(db
            .get(MetadataStore::dedup_key("t", "legacy"))
            .unwrap()
            .unwrap()
            .starts_with(b"BMQDEDUP2\n"));
        drop(store);
        let store = MetadataStore::open(dir.path()).unwrap();
        assert_eq!(
            store
                .get_dedup("t", "legacy")
                .unwrap()
                .unwrap()
                .inserted_at_ms,
            migration_ms
        );
        assert!(store.get_dedup("t", "expired").unwrap().is_none());
    }

    #[test]
    fn rocks_batch_prunes_earlier_completions_and_never_regresses() {
        let dir = tempdir().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        let mut batch = MetadataWriteBatch::default();
        batch.mark_dispatch_complete("t", "s", 0, 2);
        batch.set_dispatch_offset("t", "s", 0, 5);
        batch.set_dispatch_offset("t", "s", 0, 3);
        batch.mark_dispatch_complete("t", "s", 0, 4);
        batch.mark_dispatch_complete("t", "s", 0, 8);
        store.write_batch(batch).unwrap();
        assert_eq!(store.dispatch_offset("t", "s", 0).unwrap(), 5);
        let MetaBackend::Rocks { db, .. } = &store.backend else {
            panic!("expected local backend");
        };
        assert!(db
            .get(MetadataStore::complete_key("t", "s", 0, 2))
            .unwrap()
            .is_none());
        assert!(db
            .get(MetadataStore::complete_key("t", "s", 0, 4))
            .unwrap()
            .is_none());
        assert!(db
            .get(MetadataStore::complete_key("t", "s", 0, 8))
            .unwrap()
            .is_some());
        store.reset_dispatch_offset("t", "s", 0, 1).unwrap();
        assert!(!store.is_dispatch_complete("t", "s", 0, 8).unwrap());
    }

    #[test]
    fn metadata_survives_exit_without_flush_or_drop() {
        const CHILD_DIR: &str = "BETTERMQ_TEST_METADATA_CRASH_DIR";
        if let Some(dir) = std::env::var_os(CHILD_DIR) {
            let store = MetadataStore::open(dir).unwrap();
            store.put_dedup("tenant", "individual", &entry(10)).unwrap();
            store
                .mark_dispatch_complete("tenant", "queue", 2, 12)
                .unwrap();
            store.set_dispatch_offset("tenant", "queue", 2, 7).unwrap();
            let mut batch = MetadataWriteBatch::default();
            batch.put_dedup("tenant", "batched", entry(11));
            batch.set_dispatch_offset("tenant", "batch-queue", 2, 9);
            batch.mark_dispatch_complete("tenant", "batch-queue", 2, 13);
            store.write_batch(batch).unwrap();
            // Deliberately skip destructors and flush_epoch, like a process
            // crash immediately after successful acknowledgements.
            std::process::exit(0);
        }
        let dir = tempdir().unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "indexes::tests::metadata_survives_exit_without_flush_or_drop",
            ])
            .env(CHILD_DIR, dir.path())
            .env_remove("BETTERMQ_SHARED_META_DIR")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let store = MetadataStore::open(dir.path()).unwrap();
        assert_eq!(
            store
                .get_dedup("tenant", "individual")
                .unwrap()
                .unwrap()
                .offset,
            10
        );
        assert_eq!(
            store
                .get_dedup("tenant", "batched")
                .unwrap()
                .unwrap()
                .offset,
            11
        );
        assert_eq!(store.dispatch_offset("tenant", "queue", 2).unwrap(), 7);
        assert_eq!(
            store.dispatch_offset("tenant", "batch-queue", 2).unwrap(),
            9
        );
        assert!(store
            .is_dispatch_complete("tenant", "queue", 2, 12)
            .unwrap());
        assert!(store
            .is_dispatch_complete("tenant", "batch-queue", 2, 13)
            .unwrap());
    }

    #[test]
    fn metadata_write_batch_commits_epoch_together() {
        let dir = tempdir().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        let mut batch = MetadataWriteBatch::default();
        batch.put_dedup("tenant", "a", entry(10));
        batch.put_dedup("tenant", "b", entry(11));
        batch.set_dispatch_offset("tenant", "queue", 2, 7);
        store.write_batch(batch).unwrap();

        assert_eq!(store.get_dedup("tenant", "a").unwrap().unwrap().offset, 10);
        assert_eq!(store.get_dedup("tenant", "b").unwrap().unwrap().offset, 11);
        assert_eq!(store.dispatch_offset("tenant", "queue", 2).unwrap(), 7);
    }

    #[test]
    fn advancing_cursor_compacts_covered_completion_gaps() {
        let dir = tempdir().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        store
            .mark_dispatch_complete("tenant", "queue", 0, 1)
            .unwrap();
        store
            .mark_dispatch_complete("tenant", "queue", 0, 3)
            .unwrap();
        store.set_dispatch_offset("tenant", "queue", 0, 2).unwrap();

        assert!(store.is_dispatch_complete("tenant", "queue", 0, 1).unwrap());
        assert!(store.is_dispatch_complete("tenant", "queue", 0, 3).unwrap());
        assert_eq!(store.dispatch_offset("tenant", "queue", 0).unwrap(), 2);
    }

    #[test]
    fn dispatch_cursor_never_moves_backwards() {
        let dir = tempdir().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        store
            .mark_dispatch_complete("tenant", "queue", 0, 25)
            .unwrap();
        store.set_dispatch_offset("tenant", "queue", 0, 20).unwrap();
        // A stale completion computed from an older cursor.
        store.set_dispatch_offset("tenant", "queue", 0, 11).unwrap();
        assert_eq!(store.dispatch_offset("tenant", "queue", 0).unwrap(), 20);
        assert!(store
            .is_dispatch_complete("tenant", "queue", 0, 25)
            .unwrap());

        let mut batch = MetadataWriteBatch::default();
        batch.set_dispatch_offset("tenant", "queue", 0, 5);
        store.write_batch(batch).unwrap();
        assert_eq!(store.dispatch_offset("tenant", "queue", 0).unwrap(), 20);
    }

    #[test]
    fn completion_markers_carry_message_identity() {
        let dir = tempdir().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        let id = Uuid::new_v4();
        store
            .mark_dispatch_complete_for("tenant", "queue", 1, 9, id)
            .unwrap();
        store
            .mark_dispatch_complete("tenant", "queue", 1, 10)
            .unwrap();
        assert_eq!(
            store.dispatch_completion("tenant", "queue", 1, 9).unwrap(),
            DispatchCompletion::Message(id)
        );
        assert_eq!(
            store.dispatch_completion("tenant", "queue", 1, 10).unwrap(),
            DispatchCompletion::Legacy
        );
        store
            .clear_dispatch_complete("tenant", "queue", 1, 9)
            .unwrap();
        assert_eq!(
            store.dispatch_completion("tenant", "queue", 1, 9).unwrap(),
            DispatchCompletion::Missing
        );

        store.set_dispatch_offset("tenant", "queue", 1, 40).unwrap();
        store.set_dispatch_offset("ten:ant", "other", 1, 7).unwrap();
        store.set_dispatch_offset("tenant", "queue", 2, 5).unwrap();
        let mut cursors = store.dispatch_offsets_for_partition(1).unwrap();
        cursors.sort();
        assert_eq!(
            cursors,
            vec![
                ("ten:ant".into(), "other".into(), 7),
                ("tenant".into(), "queue".into(), 40)
            ]
        );
        store
            .reset_dispatch_offset("tenant", "queue", 1, 12)
            .unwrap();
        assert_eq!(store.dispatch_offset("tenant", "queue", 1).unwrap(), 12);
    }
}
