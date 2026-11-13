//! Optional, derived lookup for pre-key-header WALs. Normal publishing does not
//! pay the build or memory cost. The primary TTL row remains authoritative.

use super::*;

impl MetadataStore {
    fn reverse_prefix(id: Uuid) -> Vec<u8> {
        let mut key = b"dedup-id:".to_vec();
        key.extend_from_slice(id.as_bytes());
        key
    }

    pub(super) fn reverse_key(id: Uuid, primary: &[u8]) -> Vec<u8> {
        let mut key = Self::reverse_prefix(id);
        key.extend_from_slice(primary);
        key
    }

    /// Build on the native writer, then maintain atomically with every primary
    /// mutation. Rebuilding after open is bounded in memory and discards any
    /// obsolete derived format left by an older binary.
    fn ensure_reverse_index(&self) -> Result<(), IndexError> {
        let MetaBackend::Rocks {
            db,
            sync_writes,
            control,
        } = &self.backend
        else {
            return Ok(());
        };
        if control.reverse_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Some(writer) = &self.writer {
            return writer.exclusive(|store| store.ensure_reverse_index());
        }
        let _mutation = control
            .mutation
            .lock()
            .map_err(|_| std::io::Error::other("metadata mutation lock poisoned"))?;
        if control.reverse_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        batch.delete_range(b"dedup-id:", b"dedup-id;");
        db.write_opt(batch, sync_writes)?;
        let mut batch = WriteBatch::default();
        let mut count = 0usize;
        for row in db.prefix_iterator(b"dedup:") {
            let (key, value) = row?;
            if !key.starts_with(b"dedup:") {
                break;
            }
            let entry = DedupEntry::decoded(&value, control.migration_ms)?;
            if !entry.expired(now_ms(), self.retention_ms) {
                batch.put(Self::reverse_key(entry.message_id, &key), []);
            }
            count += 1;
            if count == 256 {
                db.write_opt(std::mem::take(&mut batch), sync_writes)?;
                count = 0;
            }
        }
        if !batch.is_empty() {
            db.write_opt(batch, sync_writes)?;
        }
        control.reverse_ready.store(true, Ordering::Release);
        Ok(())
    }

    /// Read-only primary validation: export must not renew TTL or mutate the
    /// frozen legacy shared producer index.
    pub fn dedup_for_message(&self, id: Uuid) -> Result<Vec<DedupRecord>, IndexError> {
        self.check_ready()?;
        match &self.backend {
            MetaBackend::Shared(store) => Ok(store.dedup_for_message(id)?),
            #[cfg(feature = "slate")]
            MetaBackend::Slate(_) => Err(IndexError::Validation(
                "local-WAL identity export is not supported by Slate".into(),
            )),
            MetaBackend::Rocks { db, control, .. } => {
                self.ensure_reverse_index()?;
                let prefix = Self::reverse_prefix(id);
                let mut out = Vec::new();
                for row in db.prefix_iterator(&prefix) {
                    let (key, _) = row?;
                    if !key.starts_with(&prefix) {
                        break;
                    }
                    let primary = &key[prefix.len()..];
                    let Some(value) = db.get(primary)? else {
                        continue;
                    };
                    let entry = DedupEntry::decoded(&value, control.migration_ms)?;
                    if entry.message_id != id || entry.expired(now_ms(), self.retention_ms) {
                        continue;
                    }
                    let text = std::str::from_utf8(primary).map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error)
                    })?;
                    let (tenant, key) = text
                        .strip_prefix("dedup:")
                        .and_then(|text| text.split_once(':'))
                        .ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "malformed dedup key",
                            )
                        })?;
                    out.push(DedupRecord {
                        tenant_id: tenant.into(),
                        key: key.into(),
                        entry,
                    });
                }
                Ok(out)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exercise(store: &MetadataStore) {
        let a = DedupEntry::new(Uuid::new_v4(), 0, 0, "a".into());
        let b = DedupEntry::new(Uuid::new_v4(), 1, 0, "b".into());
        store.put_dedup("tenant", "key", &a).unwrap();
        assert_eq!(store.dedup_for_message(a.message_id).unwrap()[0].key, "key");
        let mut batch = MetadataWriteBatch::default();
        batch.compare_put_dedup("tenant", "key", b.clone(), Some(a.message_id));
        store.write_batch(batch).unwrap();
        assert!(store.dedup_for_message(a.message_id).unwrap().is_empty());
        assert_eq!(
            store.dedup_for_message(b.message_id).unwrap()[0]
                .entry
                .offset,
            1
        );
        let mut wrong_coordinates = b.clone();
        wrong_coordinates.offset = 99;
        let mut batch = MetadataWriteBatch::default();
        batch.put_dedup("tenant", "atomic-not-written", a.clone());
        batch.compare_put_dedup("tenant", "key", wrong_coordinates.clone(), None);
        let conflict = store.write_batch(batch).unwrap_err();
        assert!(
            conflict.is_dedup_conflict(),
            "UUID equality alone is not full identity equality"
        );
        assert!(store.is_ready());
        assert!(store
            .get_dedup("tenant", "atomic-not-written")
            .unwrap()
            .is_none());
        let mut batch = MetadataWriteBatch::default();
        batch.delete_dedup_if("tenant", "key", wrong_coordinates);
        store.write_batch(batch).unwrap();
        assert_eq!(
            store.dedup_for_message(b.message_id).unwrap()[0]
                .entry
                .offset,
            1
        );
        let mut batch = MetadataWriteBatch::default();
        batch.validate_with(std::sync::Arc::new(|| {
            Err(IndexError::Validation("stale original owner".into()))
        }));
        batch.delete_dedup("tenant", "key");
        assert!(store
            .write_batch(batch)
            .unwrap_err()
            .is_conditional_failure());
        assert!(store.is_ready());
        assert_eq!(store.dedup_for_message(b.message_id).unwrap().len(), 1);
        let mut batch = MetadataWriteBatch::default();
        batch.delete_dedup_if("tenant", "key", a.clone());
        store.write_batch(batch).unwrap();
        assert_eq!(store.dedup_for_message(b.message_id).unwrap().len(), 1);
        let mut batch = MetadataWriteBatch::default();
        batch.delete_dedup_if("tenant", "key", b.clone());
        store.write_batch(batch).unwrap();
        assert!(store.dedup_for_message(b.message_id).unwrap().is_empty());
        store.put_dedup("tenant", "later", &a).unwrap();
        assert_eq!(
            store.dedup_for_message(a.message_id).unwrap()[0].key,
            "later"
        );
        let mut expired = b;
        expired.inserted_at_ms = 1;
        store.put_dedup("tenant", "expired", &expired).unwrap();
        assert!(store
            .dedup_for_message(expired.message_id)
            .unwrap()
            .is_empty());
        store.delete_dedup("tenant", "later").unwrap();
        assert!(store.dedup_for_message(a.message_id).unwrap().is_empty());
    }

    #[test]
    fn rocks_reverse_index_tracks_later_puts_cas_deletes_expiry_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            exercise(&MetadataStore::open_local(dir.path()).unwrap());
        }
        exercise(&MetadataStore::open_local(dir.path()).unwrap());
    }

    #[test]
    fn shared_reverse_index_tracks_primary_mutations_and_snapshot_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let store = MetadataStore {
            backend: MetaBackend::Shared(Box::new(SharedMetadataStore::open(dir.path()).unwrap())),
            retention_ms: dedup_retention_ms().unwrap(),
            writer: None,
        };
        exercise(&store);
        let other = SharedMetadataStore::open(dir.path()).unwrap();
        let a = DedupEntry::new(Uuid::new_v4(), 9, 0, "a".into());
        other.put_dedup("tenant", "external", &a).unwrap();
        assert_eq!(
            store.dedup_for_message(a.message_id).unwrap()[0].key,
            "external"
        );
        other.delete_dedup("tenant", "external").unwrap();
        assert!(store.dedup_for_message(a.message_id).unwrap().is_empty());
    }
}
