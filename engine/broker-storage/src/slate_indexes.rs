//! Single-broker SlateDB metadata with exclusive object-store admission and
//! terminal fencing. Never reopen a fenced database automatically.

use crate::indexes::DedupEntry;
use crate::slate_log::block_on_slate;
use crate::slate_owner::{self, SlateOwner};
use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::config::WriteOptions;
use slatedb::{Db, WriteBatch};
use std::future::Future;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SlateIndexError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("slate: {0}")]
    Slate(String),
    #[error("Slate metadata is fenced; this process is permanently not-ready; stop it before explicit takeover")]
    Fenced,
    #[error("Slate is owned by node {owner} since {claimed_at}; stop that broker, then set BETTERMQ_SLATE_TAKEOVER={owner} for an explicit CAS takeover")]
    OwnedByOtherNode { owner: uuid::Uuid, claimed_at: u64 },
    #[error("encode error: {0}")]
    Encode(#[from] bincode_next::error::EncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] bincode_next::error::DecodeError),
}

const INDEX_DB_PATH: &str = "bettermq/_indexes";

pub struct SlateMetadataStore {
    db: Arc<Db>,
    poisoned: AtomicBool,
    _owner: SlateOwner,
}

impl SlateMetadataStore {
    pub fn open(
        object_store: Arc<dyn ObjectStore>,
        data_dir: impl AsRef<Path>,
    ) -> Result<Self, SlateIndexError> {
        let node_id = slate_owner::stable_node_id(data_dir.as_ref())?;
        let takeover = std::env::var("BETTERMQ_SLATE_TAKEOVER")
            .ok()
            .map(|value| {
                uuid::Uuid::parse_str(&value).map_err(|error| {
                    SlateIndexError::Slate(format!("invalid BETTERMQ_SLATE_TAKEOVER: {error}"))
                })
            })
            .transpose()?;
        Self::open_as(object_store, node_id, takeover)
    }

    fn open_as(
        store: Arc<dyn ObjectStore>,
        node_id: uuid::Uuid,
        takeover: Option<uuid::Uuid>,
    ) -> Result<Self, SlateIndexError> {
        let owner_store = store.clone();
        let owner =
            block_on_slate(
                async move { slate_owner::claim(owner_store, node_id, takeover).await },
            )?;
        let db = block_on_slate(Db::open(INDEX_DB_PATH, store))
            .map_err(|error| SlateIndexError::Slate(error.to_string()))?;
        Ok(Self {
            db: Arc::new(db),
            poisoned: AtomicBool::new(false),
            _owner: owner,
        })
    }

    pub fn is_ready(&self) -> bool {
        !self.poisoned.load(Ordering::Acquire)
    }

    pub fn check_ready(&self) -> Result<(), SlateIndexError> {
        if self.is_ready() {
            Ok(())
        } else {
            Err(SlateIndexError::Fenced)
        }
    }

    fn io<F, T>(&self, operation: impl FnOnce(Arc<Db>) -> F) -> Result<T, SlateIndexError>
    where
        F: Future<Output = Result<T, slatedb::Error>> + Send + 'static,
        T: Send + 'static,
    {
        self.check_ready()?;
        match block_on_slate(operation(self.db.clone())) {
            Ok(value) => {
                self.check_ready()?;
                Ok(value)
            }
            Err(error)
                if error.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced) =>
            {
                self.poisoned.store(true, Ordering::Release);
                Err(SlateIndexError::Fenced)
            }
            Err(error) => Err(SlateIndexError::Slate(error.to_string())),
        }
    }

    fn write(&self, batch: WriteBatch) -> Result<(), SlateIndexError> {
        self.io(|db| async move {
            db.write_with_options(
                batch,
                &WriteOptions {
                    await_durable: true,
                    seqnum: 0,
                },
            )
            .await?;
            db.flush().await?;
            Ok(())
        })
    }

    fn dedup_key(tenant: &str, key: &str) -> Vec<u8> {
        format!("dedup:{tenant}:{key}").into_bytes()
    }
    fn dispatch_key(tenant: &str, subscription: &str, partition: u32) -> Vec<u8> {
        format!("disp:{tenant}:{subscription}:p{partition}").into_bytes()
    }
    fn complete_key(tenant: &str, subscription: &str, partition: u32, offset: u64) -> Vec<u8> {
        format!("dcomp:{tenant}:{subscription}:p{partition}:{offset}").into_bytes()
    }

    pub fn get_dedup(
        &self,
        tenant: &str,
        key: &str,
    ) -> Result<Option<DedupEntry>, SlateIndexError> {
        let key = Self::dedup_key(tenant, key);
        self.io(|db| async move { db.get(key).await })?
            .map(|bytes| broker_proto::decode(&bytes).map_err(Into::into))
            .transpose()
    }

    pub fn put_dedup(
        &self,
        tenant: &str,
        key: &str,
        entry: &DedupEntry,
    ) -> Result<(), SlateIndexError> {
        self.check_ready()?;
        let mut batch = WriteBatch::new();
        batch.put_bytes(
            Bytes::from(Self::dedup_key(tenant, key)),
            Bytes::from(broker_proto::encode(entry)?),
        );
        self.write(batch)
    }

    pub fn delete_dedup(&self, tenant: &str, key: &str) -> Result<(), SlateIndexError> {
        let mut batch = WriteBatch::new();
        batch.delete(Self::dedup_key(tenant, key));
        self.write(batch)
    }

    pub fn dispatch_offset(
        &self,
        tenant: &str,
        subscription: &str,
        partition: u32,
    ) -> Result<u64, SlateIndexError> {
        let key = Self::dispatch_key(tenant, subscription, partition);
        Ok(self
            .io(|db| async move { db.get(key).await })?
            .and_then(|bytes| bytes.as_ref().try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0))
    }

    pub fn set_dispatch_offset(
        &self,
        tenant: &str,
        subscription: &str,
        partition: u32,
        next_offset: u64,
    ) -> Result<(), SlateIndexError> {
        let current = self.dispatch_offset(tenant, subscription, partition)?;
        let mut batch = WriteBatch::new();
        batch.put_bytes(
            Bytes::from(Self::dispatch_key(tenant, subscription, partition)),
            Bytes::copy_from_slice(&next_offset.to_le_bytes()),
        );
        for offset in current..next_offset {
            batch.delete(Self::complete_key(tenant, subscription, partition, offset));
        }
        self.write(batch)
    }

    pub fn mark_dispatch_complete(
        &self,
        tenant: &str,
        subscription: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), SlateIndexError> {
        let mut batch = WriteBatch::new();
        batch.put_bytes(
            Bytes::from(Self::complete_key(tenant, subscription, partition, offset)),
            Bytes::from_static(&[1]),
        );
        self.write(batch)
    }

    pub fn is_dispatch_complete(
        &self,
        tenant: &str,
        subscription: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, SlateIndexError> {
        let key = Self::complete_key(tenant, subscription, partition, offset);
        Ok(self.io(|db| async move { db.get(key).await })?.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn close(store: SlateMetadataStore) {
        block_on_slate(async move { store.db.close().await }).unwrap();
    }

    #[test]
    fn owner_restart_takeover_and_terminal_fence() {
        let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let old = SlateMetadataStore::open_as(objects.clone(), first, None).unwrap();
        assert!(matches!(
            SlateMetadataStore::open_as(objects.clone(), second, None),
            Err(SlateIndexError::OwnedByOtherNode { .. })
        ));
        assert!(
            SlateMetadataStore::open_as(objects.clone(), second, Some(Uuid::new_v4())).is_err()
        );
        old.set_dispatch_offset("t", "s", 0, 9).unwrap();
        let new = SlateMetadataStore::open_as(objects.clone(), second, Some(first)).unwrap();
        assert_eq!(new.dispatch_offset("t", "s", 0).unwrap(), 9);
        assert!(matches!(
            old.set_dispatch_offset("t", "s", 0, 10),
            Err(SlateIndexError::Fenced)
        ));
        assert!(!old.is_ready());
        assert!(matches!(
            old.get_dedup("t", "k"),
            Err(SlateIndexError::Fenced)
        ));
        // A stale takeover setting cannot steal back from the new identity.
        assert!(SlateMetadataStore::open_as(objects.clone(), first, Some(first)).is_err());
        close(new);
        let restarted = SlateMetadataStore::open_as(objects, second, None).unwrap();
        assert_eq!(restarted.dispatch_offset("t", "s", 0).unwrap(), 9);
        close(restarted);
    }

    #[test]
    fn concurrent_distinct_nodes_have_one_winner_without_fencing_it() {
        let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let store = objects.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    SlateMetadataStore::open_as(store, Uuid::new_v4(), None)
                })
            })
            .collect();
        let mut winners: Vec<_> = handles
            .into_iter()
            .filter_map(|handle| handle.join().unwrap().ok())
            .collect();
        assert_eq!(winners.len(), 1);
        let winner = winners.remove(0);
        winner.set_dispatch_offset("t", "s", 0, 1).unwrap();
        close(winner);
    }
}
