//! Local identity barriers complement the existing all-replica payload floor.

use super::{Broker, BrokerError, GcLease, LogId};
use broker_storage::{DedupEntry, MetadataWriteBatch};
use std::sync::atomic::Ordering;
use uuid::Uuid;

pub(super) const CHECKPOINT_TENANT: &str = "__bettermq_identity_gc_v1";

impl Broker {
    fn gc_identity_key(&self, log: &LogId) -> Result<String, BrokerError> {
        serde_json::to_string(&(self.tenant(), log))
            .map_err(|error| BrokerError::InvalidConfig(error.to_string()))
    }

    /// A recreated metadata index cannot certify an already-collected prefix.
    /// The checkpoint is in the same store as producer keys, not a sidecar.
    pub fn check_gc_identity_base(&self, log: &LogId) -> Result<(), BrokerError> {
        let base = self.log_retention_status(log, 0)?.base_offset;
        self.check_gc_identity_coverage(log, base)
    }

    pub fn check_gc_identity_coverage(&self, log: &LogId, base: u64) -> Result<(), BrokerError> {
        if base == 0 {
            return Ok(());
        }
        let checkpoint = self
            .inner
            .identity_metadata
            .get_dedup(CHECKPOINT_TENANT, &self.gc_identity_key(log)?)?;
        if !checkpoint.is_some_and(|entry| {
            entry.offset >= base && entry.partition == log.partition && entry.topic == log.namespace
        }) {
            return Err(broker_storage::IndexError::Validation("collected WAL prefix has unknown producer-identity coverage; restore the original metadata index before recovery or GC".into()).into());
        }
        Ok(())
    }

    pub(crate) fn begin_identity_gc(
        &self,
        log: &LogId,
        watermark: u64,
    ) -> Result<GcLease<'_>, BrokerError> {
        self.inner.identity_metadata.check_ready()?;
        let state = self
            .inner
            .log_epochs
            .get(log.partition as usize)
            .ok_or_else(|| BrokerError::InvalidConfig("invalid GC partition".into()))?;
        let admission = state.admission.lock();
        let validate = self.log_metadata_validator(log.partition)?;
        let pending = state
            .pending_metadata
            .lock()
            .keys()
            .next()
            .copied()
            .unwrap_or(u64::MAX);
        let target = watermark.min(pending).min(self.log_committed_hwm(log)?);
        let base = self.log_retention_status(log, 0)?.base_offset;
        if target <= base {
            return Ok(GcLease {
                _admission: admission,
                target,
            });
        }
        self.check_gc_identity_base(log)?;
        let key = self.gc_identity_key(log)?;
        if !state.gc_indexed.lock().contains(&key) {
            let revision = state.dirty_revision.load(Ordering::Acquire);
            let committed = self.log_committed_hwm(log)?;
            self.seed_legacy_replica_identities(log, committed, validate.clone())?;
            let mut next = base;
            while next < committed {
                next = self.recover_dedup_page(
                    &log.namespace,
                    log.partition,
                    next,
                    committed,
                    Some(validate.clone()),
                )?;
            }
            validate()?;
            if state.dirty_revision.load(Ordering::Acquire) != revision {
                return Err(broker_storage::IndexError::Validation(
                    "identity work failed during GC reindex; retry after rebuilding".into(),
                )
                .into());
            }
            state.gc_indexed.lock().insert(key.clone());
        }
        // Receipt destructors never take admission, avoiding native-writer
        // re-entry/deadlock while this checked durability barrier is awaited.
        self.inner.identity_metadata.flush_epoch()?;
        validate()?;
        let mut checkpoint =
            DedupEntry::new(Uuid::new_v4(), target, log.partition, log.namespace.clone());
        checkpoint.inserted_at_ms = u64::MAX; // reserved, non-expiring internal row
        let mut batch = MetadataWriteBatch::default();
        batch.validate_with(validate);
        batch.put_dedup(CHECKPOINT_TENANT, key, checkpoint);
        self.inner.identity_metadata.write_batch(batch)?;
        Ok(GcLease {
            _admission: admission,
            target,
        })
    }

    pub(super) fn clamp_gc_identity_checkpoint(
        &self,
        log: &LogId,
        cut: u64,
        incarnation: Uuid,
    ) -> Result<(), BrokerError> {
        let key = self.gc_identity_key(log)?;
        if let Some(mut checkpoint) = self
            .inner
            .identity_metadata
            .get_dedup(CHECKPOINT_TENANT, &key)?
        {
            checkpoint.offset = checkpoint.offset.min(cut);
            checkpoint.message_id = incarnation;
            self.inner
                .identity_metadata
                .put_dedup(CHECKPOINT_TENANT, &key, &checkpoint)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BrokerConfig, PublishRequest, DIRECT_TOPIC};

    fn request(key: Option<&str>) -> PublishRequest {
        serde_json::from_value(serde_json::json!({"payload":"body", "routing_key":"one", "url":"https://example.com/hook", "secret":"secret", "idempotency_key":key})).unwrap()
    }

    fn broker(dir: &std::path::Path) -> Broker {
        let mut config = BrokerConfig::new(dir.to_owned());
        config.partitions = 1;
        config.log.segment_max_bytes = 1;
        Broker::open(config).unwrap()
    }

    fn deferred(broker: &Broker, key: &str) -> (crate::PublishResponse, crate::PendingDedup) {
        let prepared = broker
            .prepare_publish_batch(vec![request(Some(key))])
            .unwrap();
        broker.publish_prepared(prepared, false).unwrap()
    }

    #[test]
    fn pending_and_abandoned_key_work_cannot_escape_local_gc() {
        let dir = tempfile::tempdir().unwrap();
        let broker = broker(dir.path());
        broker.publish(request(None)).unwrap();
        broker.flush_wal().unwrap();
        let log = broker.log_id(DIRECT_TOPIC, 0);
        drop(broker.begin_identity_gc(&log, 1).unwrap()); // warm the indexed cache
        let (original, pending) = deferred(&broker, "pending-key");
        broker.publish(request(None)).unwrap();
        broker.flush_wal().unwrap();
        let pinned = broker.retention().collect(&broker, &log, 3).unwrap();
        assert!(
            pinned.base_offset <= original.offset.unwrap(),
            "a still-owned key pins its source frame"
        );
        drop(pending); // healthy prewrite failure/cancellation, not success
        broker.retention().collect(&broker, &log, 3).unwrap();
        assert!(
            broker.log_retention_status(&log, 0).unwrap().base_offset > original.offset.unwrap()
        );
        let duplicate = broker.publish(request(Some("pending-key"))).unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(
            duplicate.message_id, original.message_id,
            "sticky dirty state must force reindex before deleting the last frame"
        );
    }

    #[test]
    fn raw_catchup_append_invalidates_an_already_warm_gc_identity_cache() {
        let source_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();
        let source = broker(source_dir.path());
        source.publish(request(None)).unwrap();
        let original = source.publish(request(Some("raw-catchup-key"))).unwrap();
        source.publish(request(None)).unwrap();
        source.flush_wal().unwrap();
        let frames = source
            .namespace_replication_frames(DIRECT_TOPIC, 0, 0, 3, usize::MAX)
            .unwrap();
        let follower = broker(follower_dir.path());
        follower
            .append_replicated_frame(DIRECT_TOPIC, 0, &frames[0].1, Some(0))
            .unwrap();
        follower.flush_wal().unwrap();
        let log = follower.log_id(DIRECT_TOPIC, 0);
        drop(follower.begin_identity_gc(&log, 1).unwrap());
        for (offset, frame) in &frames[1..] {
            follower
                .append_replicated_frame(DIRECT_TOPIC, 0, frame, Some(*offset))
                .unwrap();
        }
        follower.flush_wal().unwrap();
        follower.retention().collect(&follower, &log, 3).unwrap();
        assert!(
            follower.log_retention_status(&log, 0).unwrap().base_offset > original.offset.unwrap()
        );
        let retry = follower.publish(request(Some("raw-catchup-key"))).unwrap();
        assert!(retry.duplicate, "a catch-up append with no pending metadata receipt must dirty cached GC coverage before its frame can be collected");
        assert_eq!(retry.message_id, original.message_id);
    }

    #[test]
    fn startup_gc_reindexes_purged_headers_before_payload_collection() {
        let dir = tempfile::tempdir().unwrap();
        let original;
        {
            let broker = broker(dir.path());
            let (response, pending) = deferred(&broker, "purged-key");
            original = response;
            drop(pending);
            assert!(broker.purge_message(DIRECT_TOPIC, 0, 0).unwrap());
            broker.publish(request(None)).unwrap();
            broker.flush_wal().unwrap();
        }
        let broker = broker(dir.path());
        let log = broker.log_id(DIRECT_TOPIC, 0);
        broker.retention().collect(&broker, &log, 2).unwrap();
        assert!(broker.log_retention_status(&log, 0).unwrap().base_offset >= 1);
        let retry = broker.publish(request(Some("purged-key"))).unwrap();
        assert!(retry.duplicate);
        assert_eq!(retry.message_id, original.message_id);
    }

    #[test]
    fn metadata_recreation_cannot_certify_a_previously_collected_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let mut broker = broker(dir.path());
        broker.publish(request(Some("old-key"))).unwrap();
        broker.flush_wal().unwrap();
        let log = broker.log_id(DIRECT_TOPIC, 0);
        broker.retention().collect(&broker, &log, 1).unwrap();
        assert!(broker.log_retention_status(&log, 0).unwrap().base_offset > 0);
        let replacement = tempfile::tempdir().unwrap();
        let metadata = broker_storage::MetadataStore::open(replacement.path()).unwrap();
        std::sync::Arc::get_mut(&mut broker.inner)
            .unwrap()
            .identity_metadata = std::sync::Arc::new(metadata);
        assert!(broker.check_gc_identity_base(&log).is_err());
        broker.publish(request(Some("new-key"))).unwrap();
        broker.flush_wal().unwrap();
        assert!(broker.retention().collect(&broker, &log, 2).is_err());
        assert_eq!(broker.log_retention_status(&log, 0).unwrap().base_offset, 1);
    }
}
