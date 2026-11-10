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
    #[cfg(feature = "slate")]
    #[error("slate index error: {0}")]
    Slate(#[from] SlateIndexError),
}

#[derive(Debug, Default)]
pub struct MetadataWriteBatch {
    operations: Vec<MetadataOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum MetadataOperation {
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
    },
}

impl MetadataWriteBatch {
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
            });
    }
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
}

enum MetaBackend {
    Rocks {
        db: DB,
        sync_writes: WriteOptions,
        control: RocksControl,
    },
    Shared(SharedMetadataStore),
    #[cfg(feature = "slate")]
    Slate(SlateMetadataStore),
}

/// Metadata for a broker: local RocksDB, flock shared-meta, or SlateDB on object store.
pub struct MetadataStore {
    backend: MetaBackend,
    retention_ms: u64,
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
                    backend: MetaBackend::Shared(store),
                    retention_ms,
                });
            }
        }
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;
        // This metadata is not rebuilt from the message WAL. Each successful
        // mutation must survive a host crash: dedup precedes the enqueue reply,
        // and dispatch cursors/completions precede their acknowledgement. Batch
        // callers amortize this barrier across one atomic metadata epoch.
        let mut sync_writes = WriteOptions::default();
        sync_writes.set_sync(true);
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
        Ok(Self {
            backend: MetaBackend::Rocks {
                db,
                sync_writes,
                control: RocksControl {
                    mutation: Mutex::new(()),
                    sweep_cursor: Mutex::new(Vec::new()),
                    operations: AtomicU64::new(0),
                    migration_ms,
                },
            },
            retention_ms,
        })
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
        })
    }

    pub fn is_ready(&self) -> bool {
        match &self.backend {
            #[cfg(feature = "slate")]
            MetaBackend::Slate(store) => store.is_ready(),
            MetaBackend::Shared(store) => store.check_ready().is_ok(),
            _ => true,
        }
    }

    pub fn check_ready(&self) -> Result<(), IndexError> {
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
                let mut rocks_batch = WriteBatch::default();
                let mut cursors = std::collections::HashMap::<Vec<u8>, u64>::new();
                let mut completions = Vec::new();
                for operation in batch.operations {
                    match operation {
                        MetadataOperation::PutDedup {
                            tenant_id,
                            key,
                            mut entry,
                        } => rocks_batch.put(Self::dedup_key(&tenant_id, &key), {
                            if entry.inserted_at_ms == 0 {
                                entry.inserted_at_ms = now_ms();
                            }
                            entry.encoded()?
                        }),
                        MetadataOperation::DeleteDedup { tenant_id, key } => {
                            rocks_batch.delete(Self::dedup_key(&tenant_id, &key))
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
                            cursors.insert(key.clone(), next);
                            rocks_batch.put(key, next.to_le_bytes());
                            let prefix =
                                Self::complete_prefix(&tenant_id, &subscription_id, partition);
                            for item in db.prefix_iterator(&prefix) {
                                let (key, _) = item?;
                                if !key.starts_with(&prefix) {
                                    break;
                                }
                                if std::str::from_utf8(&key[prefix.len()..])
                                    .ok()
                                    .and_then(|offset| offset.parse::<u64>().ok())
                                    .is_some_and(|offset| offset < next)
                                {
                                    rocks_batch.delete(key);
                                }
                            }
                        }
                        MetadataOperation::MarkDispatchComplete {
                            tenant_id,
                            subscription_id,
                            partition,
                            offset,
                        } => completions.push((tenant_id, subscription_id, partition, offset)),
                    }
                }
                for (tenant, sub, partition, offset) in completions {
                    let cursor_key = Self::dispatch_key(&tenant, &sub, partition);
                    let cursor = match cursors.get(&cursor_key) {
                        Some(value) => *value,
                        None => db
                            .get(cursor_key)?
                            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])))
                            .unwrap_or(0),
                    };
                    if offset >= cursor {
                        rocks_batch
                            .put(Self::complete_key(&tenant, &sub, partition, offset), [1u8]);
                    }
                }
                db.write_opt(rocks_batch, sync_writes)?;
                self.sweep_rocks(db, sync_writes, control)?;
                Ok(())
            }
            MetaBackend::Shared(store) => {
                store.apply_batch(batch.operations)?;
                Ok(())
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(_) => {
                for operation in batch.operations {
                    match operation {
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
                        MetadataOperation::MarkDispatchComplete {
                            tenant_id,
                            subscription_id,
                            partition,
                            offset,
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

    pub fn get_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<DedupEntry>, IndexError> {
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
                let _mutation = control
                    .mutation
                    .lock()
                    .map_err(|_| std::io::Error::other("metadata lock poisoned"))?;
                let key = Self::dedup_key(tenant_id, idempotency_key);
                match db.get(&key)? {
                    Some(bytes) => {
                        let entry = DedupEntry::decoded(&bytes, control.migration_ms)?;
                        if entry.expired(now_ms(), self.retention_ms) {
                            db.delete_opt(key, sync_writes)?;
                            Ok(None)
                        } else {
                            Ok(Some(entry))
                        }
                    }
                    None => Ok(None),
                }
            }
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

    fn complete_prefix(tenant_id: &str, subscription_id: &str, partition: u32) -> Vec<u8> {
        format!("dcomp:{tenant_id}:{subscription_id}:p{partition}:").into_bytes()
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
    ) -> Result<(), IndexError> {
        if (control.operations.fetch_add(1, Ordering::Relaxed) + 1) % 256 != 0 {
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
    use super::*;
    use tempfile::tempdir;

    fn entry(offset: u64) -> DedupEntry {
        DedupEntry::new(Uuid::new_v4(), offset, 2, "jobs".into())
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
}
