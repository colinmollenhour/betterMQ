//! Crash-safe WAL-first tail rewrites. An unfinished cut remains fenced until
//! its conditional identity cleanup is durable, including across process exit.

use super::{Broker, BrokerError};
use broker_storage::{DedupRecord, MetadataWriteBatch};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const MAX_ROW_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
enum Phase {
    Prepared,
    Cut,
    Done,
}

#[derive(Serialize, Deserialize)]
struct Intent {
    version: u16,
    id: Uuid,
    tenant: String,
    topic: String,
    partition: u32,
    cut: u64,
    old_end: u64,
    generation: u64,
    bytes: u64,
    checksum: u32,
    records: u64,
    phase: Phase,
}

fn io(error: impl ToString) -> BrokerError {
    broker_storage::LogError::Io(std::io::Error::other(error.to_string())).into()
}

fn sync_dir(dir: &Path) -> Result<(), BrokerError> {
    File::open(dir).and_then(|file| file.sync_all()).map_err(io)
}

impl Broker {
    fn trim_fault(&self, partition: u32, stage: u64) -> Result<(), BrokerError> {
        #[cfg(test)]
        if self.inner.log_epochs[partition as usize]
            .trim_stop
            .load(std::sync::atomic::Ordering::SeqCst)
            == stage
        {
            return Err(io(format!("injected trim interruption at stage {stage}")));
        }
        let _ = (partition, stage);
        Ok(())
    }
    fn rewrite_dir(&self) -> PathBuf {
        self.config().data_dir.join("log-rewrites")
    }
    fn intent_path(&self, id: Uuid) -> PathBuf {
        self.rewrite_dir().join(format!("{id}.intent.json"))
    }
    fn inventory_path(&self, id: Uuid) -> PathBuf {
        self.rewrite_dir().join(format!("{id}.keys.jsonl"))
    }

    fn save_intent(&self, intent: &Intent) -> Result<(), BrokerError> {
        broker_storage::atomic_write_file(
            &self.intent_path(intent.id),
            &serde_json::to_vec(intent).map_err(io)?,
        )
        .map_err(io)
    }

    fn cleanup_trim_keys(&self, intent: &Intent) -> Result<(), BrokerError> {
        let path = self.inventory_path(intent.id);
        let mut file = File::open(&path).map_err(io)?;
        if file.metadata().map_err(io)?.len() != intent.bytes {
            return Err(io("trim inventory length changed"));
        }
        let mut checksum = crc32fast::Hasher::new();
        let mut buf = [0u8; 64 * 1024];
        loop {
            let read = file.read(&mut buf).map_err(io)?;
            if read == 0 {
                break;
            }
            checksum.update(&buf[..read]);
        }
        if checksum.finalize() != intent.checksum {
            return Err(io("trim inventory checksum mismatch"));
        }
        let mut reader = BufReader::new(File::open(&path).map_err(io)?);
        let mut count = 0u64;
        let mut batch = MetadataWriteBatch::default();
        loop {
            let mut line = Vec::new();
            let read = Read::by_ref(&mut reader)
                .take(MAX_ROW_BYTES + 1)
                .read_until(b'\n', &mut line)
                .map_err(io)?;
            if read == 0 {
                break;
            }
            if read as u64 > MAX_ROW_BYTES || line.last() != Some(&b'\n') {
                return Err(io("invalid trim inventory row"));
            }
            let row: DedupRecord = serde_json::from_slice(&line).map_err(io)?;
            if row.entry.partition != intent.partition
                || row.entry.offset < intent.cut
                || row.entry.offset >= intent.old_end
            {
                return Err(io("trim inventory identity outside cut"));
            }
            batch.delete_dedup_if(row.tenant_id, row.key, row.entry);
            count += 1;
            if count % 256 == 0 {
                self.inner
                    .identity_metadata
                    .write_batch(std::mem::take(&mut batch))?;
            }
        }
        if count != intent.records {
            return Err(io("trim inventory row count changed"));
        }
        self.inner.identity_metadata.write_batch(batch)?;
        Ok(())
    }

    fn finish_trim(&self, intent: &mut Intent) -> Result<(), BrokerError> {
        self.cleanup_trim_keys(intent)?;
        self.clamp_gc_identity_checkpoint(
            &self.log_id(&intent.topic, intent.partition),
            intent.cut,
            intent.id,
        )?;
        self.trim_fault(intent.partition, 4)?;
        intent.phase = Phase::Done;
        self.save_intent(intent)?;
        self.trim_fault(intent.partition, 5)?;
        self.remove_trim_files(intent)
    }

    fn remove_trim_files(&self, intent: &Intent) -> Result<(), BrokerError> {
        // Remove the durable Done/aborted intent first. A resurrected Done
        // record is harmless; a live intent never loses its required inventory.
        match std::fs::remove_file(self.intent_path(intent.id)) {
            Ok(()) => sync_dir(&self.rewrite_dir())?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io(error)),
        }
        match std::fs::remove_file(self.inventory_path(intent.id)) {
            Ok(()) => sync_dir(&self.rewrite_dir())?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io(error)),
        }
        Ok(())
    }

    pub(super) fn rewrite_log_tail(
        &self,
        topic: &str,
        partition: u32,
        cut: u64,
        generation: u64,
    ) -> Result<u64, BrokerError> {
        let mut fence = self.begin_log_rewrite(partition)?;
        // Do not wait for outstanding quorum RPCs, only metadata intents which
        // already passed their pure incarnation guard before the epoch bump.
        self.inner.identity_metadata.flush_epoch()?;
        let handle = self.shard_handle(topic, partition)?;
        let old_end = handle.high_watermark()?;
        if cut >= old_end {
            return Ok(old_end);
        }
        std::fs::create_dir_all(self.rewrite_dir()).map_err(io)?;
        sync_dir(&self.config().data_dir)?;
        let id = Uuid::new_v4();
        let path = self.inventory_path(id);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(io)?;
        broker_storage::set_secret_file_mode(&path);
        let mut file = BufWriter::new(file);
        let mut checksum = crc32fast::Hasher::new();
        let mut bytes = 0u64;
        let mut records = 0u64;
        let mut cursor = None;
        loop {
            let page = self.inner.identity_metadata.dedup_page(cursor)?;
            for row in page.records {
                if row.tenant_id == super::identity_gc::CHECKPOINT_TENANT {
                    continue;
                }
                let entry = &row.entry;
                if entry.partition != partition || entry.offset < cut || entry.offset >= old_end {
                    continue;
                }
                if !self.layout().is_physical() {
                    if row.tenant_id != self.tenant() {
                        continue;
                    }
                    if entry.topic.is_empty() {
                        let current = handle.read_raw_metadata(partition, entry.offset, 1)?;
                        if !current.iter().any(|message| {
                            message.offset == entry.offset && message.id == entry.message_id
                        }) {
                            return Err(io(
                                "cannot safely classify a legacy topic-less trim identity",
                            ));
                        }
                    } else if entry.topic != topic {
                        continue;
                    }
                }
                let mut line = serde_json::to_vec(&row).map_err(io)?;
                line.push(b'\n');
                if line.len() as u64 > MAX_ROW_BYTES {
                    return Err(io("trim identity exceeds the bounded inventory row size"));
                }
                file.write_all(&line).map_err(io)?;
                checksum.update(&line);
                bytes += line.len() as u64;
                records += 1;
            }
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        file.flush().map_err(io)?;
        file.get_ref().sync_all().map_err(io)?;
        sync_dir(&self.rewrite_dir())?;
        let mut intent = Intent {
            version: 1,
            id,
            tenant: self.tenant(),
            topic: topic.to_owned(),
            partition,
            cut,
            old_end,
            generation,
            bytes,
            checksum: checksum.finalize(),
            records,
            phase: Phase::Prepared,
        };
        self.save_intent(&intent)?;
        fence.release = false;
        self.trim_fault(partition, 1)?;
        handle.truncate_to(cut, generation)?;
        self.trim_fault(partition, 2)?;
        intent.phase = Phase::Cut;
        self.save_intent(&intent)?;
        self.trim_fault(partition, 3)?;
        self.finish_trim(&mut intent)?;
        fence.release = true;
        Ok(handle.high_watermark()?)
    }

    pub(super) fn recover_log_rewrites(&self) -> Result<(), BrokerError> {
        let dir = self.rewrite_dir();
        let paths = match std::fs::read_dir(&dir) {
            Ok(paths) => paths,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(io(error)),
        };
        for path in paths {
            let path = path.map_err(io)?.path();
            if !path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".intent.json"))
            {
                continue;
            }
            if path.metadata().map_err(io)?.len() > 64 * 1024 {
                return Err(io("trim intent is too large"));
            }
            let mut intent: Intent =
                serde_json::from_slice(&std::fs::read(&path).map_err(io)?).map_err(io)?;
            if intent.version != 1
                || self.intent_path(intent.id) != path
                || intent.partition >= self.shard_count()
            {
                return Err(io("invalid trim intent identity"));
            }
            if intent.phase == Phase::Done {
                self.remove_trim_files(&intent)?;
                continue;
            }
            let mut fence = self.begin_log_rewrite(intent.partition)?;
            fence.release = false;
            // Boot has no controller authority yet. Never reapply a pending
            // cut: abort if untouched, finish cleanup if the cut is durable.
            let end = crate::tenant_scope::with_tenant(&intent.tenant, || {
                self.shard_handle(&intent.topic, intent.partition)
            })?
            .high_watermark()?;
            if intent.phase == Phase::Prepared && end == intent.old_end && end != intent.cut {
                intent.phase = Phase::Done;
                self.save_intent(&intent)?;
                self.remove_trim_files(&intent)?;
            } else if end == intent.cut {
                self.finish_trim(&mut intent)?;
            } else {
                return Err(io(
                    "unfinished trim does not match the recovered WAL end; refusing offset reuse",
                ));
            }
            fence.release = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BrokerConfig, PublishRequest, DIRECT_TOPIC};

    fn request(key: &str) -> PublishRequest {
        serde_json::from_value(serde_json::json!({"payload":"original", "routing_key":"one", "url":"https://example.com/hook", "secret":"secret", "idempotency_key":key})).unwrap()
    }

    #[test]
    fn interrupted_wal_first_trim_recovers_before_key_rebuild_or_offset_reuse() {
        for stage in 1..=5 {
            let dir = tempfile::tempdir().unwrap();
            let config = BrokerConfig::new(dir.path().to_path_buf());
            let broker = Broker::open(config.clone()).unwrap();
            let original = broker.publish(request("trimmed-key")).unwrap();
            broker.flush_wal().unwrap();
            let partition = original.partition.unwrap();
            broker.inner.log_epochs[partition as usize]
                .trim_stop
                .store(stage, std::sync::atomic::Ordering::SeqCst);
            let error = broker
                .truncate_replica_log(DIRECT_TOPIC, partition, 0, 7)
                .unwrap_err();
            assert!(
                error.to_string().contains("injected"),
                "stage {stage}: {error}"
            );
            assert!(broker.publish(request("must-stay-fenced")).is_err());
            drop(broker);
            let broker = Broker::open(config.clone()).unwrap();
            let retried = broker.publish(request("trimmed-key")).unwrap();
            if stage == 1 {
                assert!(retried.duplicate);
                assert_eq!(retried.message_id, original.message_id);
            } else {
                assert!(!retried.duplicate);
                assert_ne!(retried.message_id, original.message_id);
                assert_eq!(retried.offset, Some(0));
            }
            broker.flush_wal().unwrap();
            drop(broker);
            let reopened = Broker::open(config).unwrap();
            assert_eq!(
                reopened.publish(request("trimmed-key")).unwrap().message_id,
                retried.message_id,
                "old trim must never cut replacement records at stage {stage}"
            );
        }
    }

    #[test]
    fn trim_invalidates_failed_keys_before_later_gc_can_trust_a_phantom() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        broker.enable_quorum_gating();
        let original = broker.publish(request("abandoned-A")).unwrap();
        broker.flush_wal().unwrap();
        let partition = original.partition.unwrap();
        broker
            .truncate_replica_log(DIRECT_TOPIC, partition, 0, 8)
            .unwrap();
        assert!(broker
            .inner
            .metadata
            .get_dedup(&broker.tenant(), "abandoned-A")
            .unwrap()
            .is_none());
        let replacement = broker.publish(request("replacement-B")).unwrap();
        broker.flush_wal().unwrap();
        broker
            .record_quorum_commit(DIRECT_TOPIC, partition, 0, 1)
            .unwrap();
        let retry = broker.publish(request("abandoned-A")).unwrap();
        assert!(!retry.duplicate);
        assert_ne!(retry.message_id, original.message_id);
        assert_ne!(retry.message_id, replacement.message_id);
    }
}
