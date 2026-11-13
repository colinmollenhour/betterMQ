//! A committed, tenant-wide directory is distinct from replica-local indexes.
//! Only globally visible identities enter it; local cuts never invalidate it.
//! Fixed shared-file admission stripes serialize concurrent cross-shard keys.

use super::*;

impl Broker {
    pub fn shared_producer_admission(&self) -> bool {
        self.inner.metadata.is_shared()
            && !Arc::ptr_eq(&self.inner.metadata, &self.inner.identity_metadata)
    }

    fn committed_producer_tenant(tenant: &str) -> String {
        // A real WAL tenant cannot contain NUL (it is a directory component).
        // Keep internal committed evidence separate from frozen legacy rows.
        format!("\0bettermq-committed-producers-v1\0{tenant}")
    }

    /// Acquire only on the actual WAL leader, before local key reservations.
    /// All batch stripes are sorted/deduplicated by storage. Busy attempts drop
    /// every partial lock and return their native I/O permit before waiting.
    pub async fn lock_shared_producers(
        &self,
        keys: Vec<String>,
    ) -> Result<Option<Arc<broker_storage::ProducerLocks>>, BrokerError> {
        if !self.shared_producer_admission() || keys.is_empty() {
            return Ok(None);
        }
        let keys = Arc::new(keys);
        let tenant = self.tenant();
        let deadline = tokio::time::Instant::now() + DEDUP_IN_FLIGHT_WAIT;
        loop {
            let store = self.inner.metadata.clone();
            let tenant = tenant.clone();
            let keys = keys.clone();
            let locks = crate::run_blocking_io(move || store.try_lock_producers(&tenant, &keys))
                .await
                .map_err(blocking_task_error)??;
            if let Some(locks) = locks {
                return Ok(Some(Arc::new(locks)));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(BrokerError::IdempotencyInFlight(
                    "shared producer admission stripe".into(),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// Directory rows are written only after contiguous quorum visibility.
    /// They remain valid when a retry reaches a different shard/replica set.
    pub async fn shared_committed_publish(
        &self,
        key: &str,
    ) -> Result<Option<PublishResponse>, BrokerError> {
        if !self.shared_producer_admission() {
            return Ok(None);
        }
        let store = self.inner.metadata.clone();
        let tenant = Self::committed_producer_tenant(&self.tenant());
        let key = key.to_owned();
        let entry = crate::run_blocking_io(move || store.confirmed_dedup(&tenant, &key))
            .await
            .map_err(blocking_task_error)??;
        Ok(entry.map(|entry| PublishResponse {
            message_id: Some(entry.message_id),
            topic: entry.topic,
            partition: Some(entry.partition),
            offset: Some(entry.offset),
            duplicate: true,
            scheduled: None,
            commit_epoch: Some(entry.offset.saturating_add(1)),
            replication_frame: None,
            publish_fence: None,
        }))
    }

    pub async fn legacy_shared_publish_hint(
        &self,
        key: &str,
    ) -> Result<Option<DedupEntry>, BrokerError> {
        if !self.shared_producer_admission() {
            return Ok(None);
        }
        let store = self.inner.metadata.clone();
        let tenant = self.tenant();
        let key = key.to_owned();
        crate::run_blocking_io(move || store.get_dedup(&tenant, &key))
            .await
            .map_err(blocking_task_error)?
            .map_err(BrokerError::from)
    }

    /// Read-only resolution, called only by the serving canonical WAL leader.
    /// Unverified or unknown collected coverage is never classified as absent.
    pub fn verify_legacy_shared_publish(
        &self,
        key: &str,
        entry: &DedupEntry,
    ) -> Result<Option<DedupEntry>, BrokerError> {
        let (_, authority) = self.require_shard_fence(entry.partition)?;
        let result = match self.verify_dedup_hit(&self.tenant(), entry)? {
            DedupVerification::Unverified => Err(BrokerError::DedupQuorumPending(
                "legacy producer identity".into(),
            )),
            DedupVerification::Stale => Ok(None),
            DedupVerification::Verified(topic) if !topic.is_empty() => {
                self.check_gc_identity_base(&self.log_id(&topic, entry.partition))?;
                let handle = self.shard_handle(&topic, entry.partition)?;
                let mut entry = entry.clone();
                entry.topic = topic;
                if entry.offset < handle.base_offset()? {
                    if !self
                        .inner
                        .identity_metadata
                        .get_dedup(&self.tenant(), key)?
                        .is_some_and(|local| local.same_identity(&entry))
                    {
                        return Err(broker_storage::IndexError::Validation("legacy producer key below the collected base has no matching local identity coverage".into()).into());
                    }
                } else if !handle
                    .read_raw_metadata(entry.partition, entry.offset, 1)?
                    .iter()
                    .any(|message| {
                        message.offset == entry.offset
                            && message.id == entry.message_id
                            && message.tenant_id == self.tenant()
                            && message
                                .idempotency_key
                                .as_ref()
                                .is_none_or(|stored| stored == key)
                    })
                {
                    return Err(broker_storage::IndexError::Validation(
                        "legacy producer key does not match its canonical raw WAL identity".into(),
                    )
                    .into());
                }
                Ok(Some(entry))
            }
            DedupVerification::Verified(_) => Err(broker_storage::IndexError::Validation(
                "collected legacy producer identity has no provable original topic".into(),
            )
            .into()),
        };
        if let Some(authority) = authority {
            authority()?;
        }
        result
    }

    /// Promote read-only legacy evidence only after the canonical source proved
    /// it quorum-visible. This does not mutate or delete the original row.
    pub async fn adopt_shared_publish(
        &self,
        key: &str,
        entry: DedupEntry,
        locks: Option<Arc<broker_storage::ProducerLocks>>,
    ) -> Result<Option<PublishResponse>, BrokerError> {
        let locks = locks.ok_or_else(|| {
            BrokerError::InvalidConfig("legacy producer adoption requires owned admission".into())
        })?;
        let store = self.inner.metadata.clone();
        let tenant = self.tenant();
        let owned_key = key.to_owned();
        let found = crate::run_blocking_io(move || {
            let _locks = locks;
            let Some(original) = store.get_dedup(&tenant, &owned_key)? else {
                return Ok::<_, BrokerError>(false);
            };
            if original.message_id != entry.message_id
                || original.offset != entry.offset
                || original.partition != entry.partition
                || (!original.topic.is_empty() && original.topic != entry.topic)
            {
                return Err(broker_storage::IndexError::DedupConflict.into());
            }
            let mut batch = MetadataWriteBatch::default();
            batch.compare_put_dedup(
                Self::committed_producer_tenant(&tenant),
                owned_key,
                entry,
                None,
            );
            store.write_batch(batch)?;
            Ok(true)
        })
        .await
        .map_err(blocking_task_error)??;
        if found {
            self.shared_committed_publish(key).await
        } else {
            Ok(None)
        }
    }

    pub async fn commit_shared_publish(
        &self,
        key: &str,
        response: &PublishResponse,
        locks: Option<Arc<broker_storage::ProducerLocks>>,
    ) -> Result<(), BrokerError> {
        self.commit_shared_publishes(&[(key, response)], locks)
            .await
    }

    /// One directory durability epoch for an admitted batch, without copying
    /// any response's payload-sized replication frame.
    pub async fn commit_shared_publishes(
        &self,
        publishes: &[(&str, &PublishResponse)],
        locks: Option<Arc<broker_storage::ProducerLocks>>,
    ) -> Result<(), BrokerError> {
        if !self.shared_producer_admission() {
            return Ok(());
        }
        let locks = locks.ok_or_else(|| {
            BrokerError::InvalidConfig("shared producer commit requires owned admission".into())
        })?;
        let publishes: Vec<_> = publishes
            .iter()
            .filter_map(|(key, response)| {
                let (Some(id), Some(partition), Some(offset)) =
                    (response.message_id, response.partition, response.offset)
                else {
                    return None;
                };
                Some((
                    (*key).to_owned(),
                    id,
                    partition,
                    offset,
                    response.topic.clone(),
                    response.publish_fence.clone(),
                ))
            })
            .collect();
        let broker = self.clone();
        let tenant = self.tenant();
        crate::run_blocking_io(move || {
            let _locks = locks; // native persistence must outlive cancellation
            let mut batch = MetadataWriteBatch::default();
            for (key, id, partition, offset, topic, fence) in publishes {
                if let Some(fence) = &fence {
                    fence.validate()?;
                }
                if broker.quorum_hwm(&topic, partition)? <= offset {
                    return Err(BrokerError::DedupQuorumPending(key));
                }
                let entry = broker
                    .inner
                    .identity_metadata
                    .get_dedup(&tenant, &key)?
                    .filter(|entry| {
                        entry.message_id == id
                            && entry.partition == partition
                            && entry.offset == offset
                            && entry.topic == topic
                    })
                    .ok_or_else(|| {
                        broker_storage::IndexError::Validation(
                            "committed producer identity is missing from its replica index".into(),
                        )
                    })?;
                if let Some(fence) = fence {
                    batch.validate_with(fence.validate);
                }
                batch.compare_put_dedup(Self::committed_producer_tenant(&tenant), key, entry, None);
            }
            broker.inner.metadata.write_batch(batch)?;
            Ok(())
        })
        .await
        .map_err(blocking_task_error)?
    }
}
