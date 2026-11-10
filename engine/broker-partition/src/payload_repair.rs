//! Legacy payload-blob references in local-WAL cells.
//!
//! Before inline-only bodies, a large body in a multi-node local-WAL cell was
//! written only to the accepting node's `payload-blobs`, and just a reference
//! was replicated. A node that becomes shard leader finds those references
//! (`Broker::damaged_payload_refs`) and repairs the missing or corrupt blobs
//! from peers before the shard serves. References that cannot be repaired
//! stay deferred at delivery (`hydrate_failed`), never dead-lettered or sent
//! with an empty body.

use crate::broker::{Broker, BrokerError, LogId};
use broker_payload::{PayloadError, PayloadRef};
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::OnceLock;

const SCAN_BATCH: usize = 512;

impl Broker {
    /// Payload references in `log` whose local blob is missing or does not
    /// match the reference. Returns `(references scanned, damaged)`.
    pub fn damaged_payload_refs(
        &self,
        log: &LogId,
    ) -> Result<(usize, Vec<PayloadRef>), BrokerError> {
        let mut scanned = 0usize;
        let mut damaged = Vec::new();
        let mut cursor = 0u64;
        loop {
            let batch = self.read_log(log, cursor, SCAN_BATCH)?;
            let Some(last) = batch.last().map(|message| message.offset) else {
                break;
            };
            cursor = last.saturating_add(1);
            for message in &batch {
                // A non-empty inline payload never needs the blob.
                if !message.payload.is_empty() {
                    continue;
                }
                let Some(reference) = crate::retention::payload_reference(message) else {
                    continue;
                };
                scanned += 1;
                match self.blob_store().verify_blob(&reference) {
                    Ok(()) => {}
                    Err(PayloadError::Io(error)) => return Err(PayloadError::Io(error).into()),
                    Err(_) => damaged.push(reference),
                }
            }
        }
        Ok((scanned, damaged))
    }

    /// Install blob bytes fetched from a peer after verifying them.
    pub fn install_repaired_blob(
        &self,
        reference: &PayloadRef,
        data: &[u8],
    ) -> Result<(), BrokerError> {
        Ok(self.blob_store().install_blob(reference, data)?)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PayloadRepairSnapshot {
    /// Unrepaired legacy references per shard (leader view).
    pub unresolved_by_shard: BTreeMap<u32, u64>,
    pub repaired_total: u64,
    /// Repaired references without a sha256, verified by size only.
    pub unhashed_total: u64,
    pub failures_total: u64,
}

impl PayloadRepairSnapshot {
    pub fn unresolved(&self) -> u64 {
        self.unresolved_by_shard.values().sum()
    }
}

fn telemetry() -> &'static Mutex<PayloadRepairSnapshot> {
    static TELEMETRY: OnceLock<Mutex<PayloadRepairSnapshot>> = OnceLock::new();
    TELEMETRY.get_or_init(Default::default)
}

pub fn payload_repair_snapshot() -> PayloadRepairSnapshot {
    telemetry().lock().clone()
}

/// Record one repair pass over `shard`.
pub fn record_payload_repair(shard: u32, unresolved: u64, repaired: u64, unhashed: u64) {
    let mut snapshot = telemetry().lock();
    if unresolved == 0 {
        snapshot.unresolved_by_shard.remove(&shard);
    } else {
        snapshot.unresolved_by_shard.insert(shard, unresolved);
    }
    snapshot.repaired_total += repaired;
    snapshot.unhashed_total += unhashed;
    snapshot.failures_total += unresolved;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::{BrokerConfig, PublishRequest};
    use crate::topic::DIRECT_TOPIC;

    fn request(payload: String) -> PublishRequest {
        PublishRequest {
            topic: String::new(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "lane".into(),
            payload,
            payload_encoding: None,
            idempotency_key: None,
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: Some("http://127.0.0.1:9/h".into()),
            secret: Some("s".into()),
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
            routing_id: None,
        }
    }

    #[test]
    fn missing_and_corrupt_legacy_blobs_are_found_and_repairable() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = BrokerConfig::new(dir.path().to_path_buf());
        config.partitions = 1;
        let broker = Broker::open(config).unwrap();
        // A legacy (pre-inline) record: body in a node-local blob.
        let body = "L".repeat(broker_payload::inline_max_bytes() + 10);
        broker.publish(request(body.clone())).unwrap();
        broker.publish(request("inline".into())).unwrap();
        let log = broker.log_id(DIRECT_TOPIC, 0);
        let (scanned, damaged) = broker.damaged_payload_refs(&log).unwrap();
        assert_eq!((scanned, damaged.len()), (1, 0));

        let message = broker
            .read_log(&log, 0, 10)
            .unwrap()
            .into_iter()
            .find(|message| message.payload_ref_json.is_some())
            .unwrap();
        let reference = crate::retention::payload_reference(&message).unwrap();
        let bytes = broker
            .blob_store()
            .read_local_blob(&reference.tenant_id, reference.message_id)
            .unwrap();
        let path = broker_payload::PayloadRef::fs_path(
            &dir.path().join("payload-blobs"),
            &reference.bucket_key,
        )
        .unwrap();

        std::fs::remove_file(&path).unwrap();
        let (_, damaged) = broker.damaged_payload_refs(&log).unwrap();
        assert_eq!(damaged, vec![reference.clone()]);
        broker.install_repaired_blob(&reference, &bytes).unwrap();
        assert!(broker.damaged_payload_refs(&log).unwrap().1.is_empty());

        std::fs::write(&path, b"corrupt").unwrap();
        assert_eq!(broker.damaged_payload_refs(&log).unwrap().1.len(), 1);
        // Peer bytes that do not match the reference are refused.
        assert!(broker
            .install_repaired_blob(&reference, b"corrupt")
            .is_err());
        broker.install_repaired_blob(&reference, &bytes).unwrap();
        let mut hydrated = message.clone();
        broker.hydrate_message_payload(&mut hydrated).unwrap();
        assert_eq!(hydrated.payload, body.as_bytes());
    }

    #[test]
    fn repair_telemetry_tracks_unresolved_per_shard() {
        record_payload_repair(9_001, 2, 1, 1);
        assert_eq!(payload_repair_snapshot().unresolved_by_shard[&9_001], 2);
        record_payload_repair(9_001, 0, 2, 0);
        assert!(!payload_repair_snapshot()
            .unresolved_by_shard
            .contains_key(&9_001));
    }
}
