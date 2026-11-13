//! Shared delivery cursors are independent of each local WAL's producer keys.
//! Legacy global rows are read-only evidence, never authority to trim a replica.

use super::*;

impl Broker {
    fn has_legacy_shared_identities(&self) -> bool {
        !Arc::ptr_eq(&self.inner.metadata, &self.inner.identity_metadata)
            && self.inner.metadata.is_shared()
    }

    fn identity_matches_message(
        &self,
        row: &broker_storage::DedupRecord,
        message: &StoredMessage,
    ) -> bool {
        row.tenant_id != super::identity_gc::CHECKPOINT_TENANT
            && row.tenant_id == message.tenant_id
            && row.entry.message_id == message.id
            && row.entry.partition == message.partition
            && row.entry.offset == message.offset
            && (row.entry.topic == message.topic || row.entry.topic.is_empty())
            && message
                .idempotency_key
                .as_ref()
                .is_none_or(|key| key == &row.key)
    }

    /// Catch-up may export before migration; consult both indexes, and validate
    /// all coordinates against the actual source record, not just its UUID.
    pub(super) fn restore_replication_key(
        &self,
        message: &mut StoredMessage,
    ) -> Result<(), BrokerError> {
        if message.idempotency_key.is_some() {
            return Ok(());
        }
        let mut rows = self.inner.identity_metadata.dedup_for_message(message.id)?;
        if self.has_legacy_shared_identities() {
            rows.extend(self.inner.metadata.dedup_for_message(message.id)?);
        }
        let mut key = None;
        let mut timestamp = None;
        for row in rows
            .into_iter()
            .filter(|row| self.identity_matches_message(row, message))
        {
            if key.as_ref().is_some_and(|key| key != &row.key) {
                return Err(broker_storage::IndexError::Validation(
                    "ambiguous legacy producer keys for one WAL record".into(),
                )
                .into());
            }
            timestamp = Some(timestamp.map_or(row.entry.inserted_at_ms, |old: u64| {
                old.min(row.entry.inserted_at_ms)
            }));
            key = Some(row.key);
        }
        message.dedup_inserted_at_ms = timestamp;
        message.idempotency_key = key;
        Ok(())
    }

    /// Once per recovery/indexing pass, not once per WAL page. Never import
    /// global GC checkpoints: only this replica can certify its deleted prefix.
    pub fn seed_legacy_replica_identities(
        &self,
        log: &LogId,
        target: u64,
        validate: broker_storage::MetadataValidator,
    ) -> Result<(), BrokerError> {
        if !self.has_legacy_shared_identities() {
            return Ok(());
        }
        let handle = self.shard_handle(&log.namespace, log.partition)?;
        let mut cursor = None;
        loop {
            validate()?;
            let page = self.inner.metadata.dedup_page(cursor)?;
            let mut batch = MetadataWriteBatch::default();
            batch.validate_with(validate.clone());
            for mut row in page.records {
                if row.tenant_id == super::identity_gc::CHECKPOINT_TENANT
                    || row.entry.partition != log.partition
                    || row.entry.offset >= target
                {
                    continue;
                }
                if !row.entry.topic.is_empty()
                    && self.log_id(&row.entry.topic, row.entry.partition) != *log
                {
                    continue;
                }
                let Some(message) = handle
                    .read_raw_metadata(log.partition, row.entry.offset, 1)?
                    .into_iter()
                    .next()
                else {
                    continue;
                };
                if !self.identity_matches_message(&row, &message) {
                    continue;
                }
                let existing = self
                    .inner
                    .identity_metadata
                    .get_dedup(&row.tenant_id, &row.key)?;
                if let Some(existing) = &existing {
                    if existing.message_id == row.entry.message_id
                        || (existing.inserted_at_ms > row.entry.inserted_at_ms
                            && matches!(
                                self.verify_dedup_hit_with_visibility(
                                    &row.tenant_id,
                                    existing,
                                    false
                                )?,
                                DedupVerification::Verified(_)
                            ))
                    {
                        continue;
                    }
                }
                row.entry.topic = message.topic;
                batch.compare_put_dedup(
                    row.tenant_id,
                    row.key,
                    row.entry,
                    existing.map(|entry| entry.message_id),
                );
            }
            self.inner.identity_metadata.write_batch(batch)?;
            let Some(next) = page.next else {
                break;
            };
            cursor = Some(next);
        }
        validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(path: &std::path::Path, split: bool) -> BrokerConfig {
        let mut config = BrokerConfig::new(path.to_owned());
        config.partitions = 1;
        config.local_replica_identities = split;
        config
    }

    fn request(key: &str) -> PublishRequest {
        serde_json::from_value(serde_json::json!({"payload":"body", "routing_key":"one", "url":"https://example.com/hook", "secret":"secret", "idempotency_key":key})).unwrap()
    }

    #[test]
    fn shared_replica_trim_and_legacy_export_are_local_and_restart_safe() {
        const CHILD: &str = "BETTERMQ_TEST_LOCAL_IDENTITY_SPLIT";
        if std::env::var_os(CHILD).is_none() {
            let shared = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "broker::identity_migration::tests::shared_replica_trim_and_legacy_export_are_local_and_restart_safe", "--nocapture"])
                .env(CHILD, "1").env("BETTERMQ_SHARED_META_DIR", shared.path()).output().unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        let legacy_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();
        let export_dir = tempfile::tempdir().unwrap();
        let legacy = Broker::open(config(legacy_dir.path(), false)).unwrap();
        let original = legacy.publish(request("canonical-A")).unwrap();
        legacy.flush_wal().unwrap();
        let mut original_entry = legacy
            .inner
            .metadata
            .get_dedup(&legacy.tenant(), "canonical-A")
            .unwrap()
            .unwrap();
        original_entry.inserted_at_ms -= 1000;
        legacy
            .inner
            .metadata
            .put_dedup(&legacy.tenant(), "canonical-A", &original_entry)
            .unwrap();

        // F's divergent X shares A's offset, but has no authority to delete A.
        {
            let follower = Broker::open(config(follower_dir.path(), true)).unwrap();
            assert!(!Arc::ptr_eq(
                &follower.inner.metadata,
                &follower.inner.identity_metadata
            ));
            let divergent = follower.publish(request("divergent-X")).unwrap();
            assert_eq!(original.offset, divergent.offset);
            follower.flush_wal().unwrap();
            follower.inner.log_epochs[0]
                .trim_stop
                .store(3, std::sync::atomic::Ordering::SeqCst);
            assert!(follower
                .truncate_replica_log(DIRECT_TOPIC, 0, 0, 1)
                .is_err());
        }
        let follower = Broker::open(config(follower_dir.path(), true)).unwrap();
        assert!(follower
            .inner
            .identity_metadata
            .get_dedup(&follower.tenant(), "divergent-X")
            .unwrap()
            .is_none());
        assert_eq!(
            legacy
                .inner
                .metadata
                .get_dedup(&legacy.tenant(), "canonical-A")
                .unwrap()
                .unwrap()
                .message_id,
            original.message_id.unwrap()
        );

        let exporter = Broker::open(config(export_dir.path(), true)).unwrap();
        // Enable the local reverse view while empty: migration must update it.
        assert!(exporter
            .inner
            .identity_metadata
            .dedup_for_message(original.message_id.unwrap())
            .unwrap()
            .is_empty());
        for key in ["canonical-A", "later-B"] {
            let response = if key == "canonical-A" {
                original.clone()
            } else {
                legacy.publish(request(key)).unwrap()
            };
            legacy.flush_wal().unwrap();
            let mut message = legacy
                .read_message(DIRECT_TOPIC, 0, response.offset.unwrap())
                .unwrap();
            message.idempotency_key = None; // historical catch-up stripped it
            let frame = encode_replication_frame(&message).unwrap();
            exporter
                .append_replicated_frame(DIRECT_TOPIC, 0, &frame, response.offset)
                .unwrap();
            exporter.flush_wal().unwrap();
            // Both first export and later insertion use legacy Shared evidence.
            let exported = exporter
                .namespace_replication_frames(
                    DIRECT_TOPIC,
                    0,
                    response.offset.unwrap(),
                    1,
                    usize::MAX,
                )
                .unwrap();
            let (header, _) =
                broker_proto::decode_frame(std::io::Cursor::new(&exported[0].1)).unwrap();
            assert_eq!(header.idempotency_key.as_deref(), Some(key));
            let stored = legacy
                .inner
                .metadata
                .get_dedup(&legacy.tenant(), key)
                .unwrap()
                .unwrap();
            assert_eq!(header.dedup_inserted_at_ms, Some(stored.inserted_at_ms));
            exporter
                .seed_legacy_replica_identities(
                    &exporter.log_id(DIRECT_TOPIC, 0),
                    response.offset.unwrap() + 1,
                    exporter.log_metadata_validator(0).unwrap(),
                )
                .unwrap();
            let local = exporter
                .inner
                .identity_metadata
                .get_dedup(&exporter.tenant(), key)
                .unwrap()
                .unwrap();
            assert_eq!(local.inserted_at_ms, stored.inserted_at_ms);
            assert_eq!(
                exporter
                    .inner
                    .identity_metadata
                    .dedup_for_message(header.id)
                    .unwrap()[0]
                    .key,
                key
            );
            // Original publication time and the migrated retry TTL are distinct.
            assert_eq!(header.published_at_ms, message.published_at_ms);
            let local_frames = exporter
                .replication_frames(DIRECT_TOPIC, 0, response.offset.unwrap(), 1)
                .unwrap();
            assert_eq!(local_frames, exported);
        }
        assert!(legacy
            .inner
            .metadata
            .get_dedup(&legacy.tenant(), "divergent-X")
            .unwrap()
            .is_none());
    }
}
