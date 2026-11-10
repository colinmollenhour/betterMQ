//! Durable SlateDB partition log on object storage (HA M3).

#[cfg(feature = "slate")]
use crate::log::PartitionLogConfig;
#[cfg(feature = "slate")]
use crate::LogError;
#[cfg(feature = "slate")]
use broker_proto::record::{decode_frame, encode_frame};
#[cfg(feature = "slate")]
use broker_proto::{LogRecord, StoredMessage};
#[cfg(feature = "slate")]
use bytes::Bytes;
#[cfg(feature = "slate")]
use object_store::ObjectStore;
#[cfg(feature = "slate")]
use slatedb::config::WriteOptions;
#[cfg(feature = "slate")]
use slatedb::{Db, WriteBatch};
#[cfg(feature = "slate")]
use std::future::Future;
#[cfg(feature = "slate")]
use std::path::Path;
#[cfg(feature = "slate")]
use std::sync::{Arc, OnceLock};
#[cfg(feature = "slate")]
use tokio::runtime::{Handle, Runtime};

/// Dedicated runtime for SlateDB — never `block_on` the server's Tokio pool (deadlocks HTTP).
#[cfg(feature = "slate")]
fn slate_runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("slate-io")
            .enable_all()
            .build()
            .expect("slate tokio runtime")
    })
}

/// Run SlateDB async IO from sync partition APIs (called under `#[tokio::main]`).
#[cfg(feature = "slate")]
pub(crate) fn block_on_slate<F, T>(future: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let rt = slate_runtime();
    if Handle::try_current().is_ok() {
        std::thread::scope(|scope| scope.spawn(|| rt.block_on(future)).join().unwrap())
    } else {
        rt.block_on(future)
    }
}

#[cfg(feature = "slate")]
fn frame_key(offset: u64) -> Vec<u8> {
    format!("fr/{offset:020}").into_bytes()
}

#[cfg(feature = "slate")]
const META_NEXT_OFFSET: &[u8] = b"meta/next_offset";
#[cfg(feature = "slate")]
const META_LEADER_GEN: &[u8] = b"meta/leader_generation";

#[cfg(feature = "slate")]
pub fn slate_topics(store: Arc<dyn ObjectStore>, tenant: &str) -> Result<Vec<String>, LogError> {
    let tenant = broker_proto::sanitize_path_segment(tenant)
        .map_err(|error| LogError::Slate(error.to_string()))?;
    let prefix = object_store::path::Path::from(format!("bettermq/{tenant}/topics"));
    block_on_slate(async move {
        let listing = store
            .list_with_delimiter(Some(&prefix))
            .await
            .map_err(|error| LogError::Slate(error.to_string()))?;
        Ok(listing
            .common_prefixes
            .into_iter()
            .filter_map(|path| path.parts().last().map(|part| part.as_ref().to_string()))
            .collect())
    })
}

/// Stable object-store prefix shared by every broker in a cluster (not per-node `data_dir`).
#[cfg(feature = "slate")]
pub fn slate_db_path(
    tenant_id: &str,
    topic: &str,
    partition: u32,
) -> Result<String, broker_proto::PathSegmentError> {
    let tenant = broker_proto::sanitize_path_segment(tenant_id)?;
    let topic = broker_proto::sanitize_path_segment(topic)?;
    Ok(format!("bettermq/{tenant}/topics/{topic}/p{partition}"))
}

#[cfg(feature = "slate")]
fn durable_write_opts() -> WriteOptions {
    WriteOptions {
        await_durable: true,
        seqnum: 0,
    }
}

#[cfg(feature = "slate")]
pub struct SlatePartitionLog {
    db: Arc<Db>,
    next_offset: u64,
    leader_generation: u64,
    _config: PartitionLogConfig,
}

#[cfg(feature = "slate")]
impl SlatePartitionLog {
    pub fn open(
        db_path: String,
        local_cache_dir: impl AsRef<Path>,
        object_store: Arc<dyn ObjectStore>,
        partition: u32,
        config: PartitionLogConfig,
    ) -> Result<Self, LogError> {
        let _ = std::fs::create_dir_all(local_cache_dir.as_ref());
        let db =
            Arc::new(block_on_slate(Db::open(db_path.clone(), object_store)).map_err(slate_err)?);

        let next_offset = Self::recover_next_offset(Arc::clone(&db), partition)?;
        let leader_generation = Self::read_leader_generation(Arc::clone(&db))?;

        Ok(Self {
            db,
            next_offset,
            leader_generation,
            _config: config,
        })
    }

    fn read_leader_generation(db: Arc<Db>) -> Result<u64, LogError> {
        block_on_slate(async move {
            match db.get(META_LEADER_GEN).await.map_err(slate_err)? {
                Some(bytes) if bytes.len() >= 8 => {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&bytes[..8]);
                    Ok(u64::from_be_bytes(buf))
                }
                _ => Ok(0u64),
            }
        })
    }

    /// The offset and its frame are committed atomically in the same WriteBatch.
    /// Recovery must not probe historical offsets (which may also be purged).
    fn recover_next_offset(db: Arc<Db>, _partition: u32) -> Result<u64, LogError> {
        block_on_slate(async move {
            match db.get(META_NEXT_OFFSET).await.map_err(slate_err)? {
                Some(bytes) => {
                    let buf: [u8; 8] = bytes.as_ref().try_into().map_err(|_| {
                        LogError::Slate("invalid durable next_offset metadata".into())
                    })?;
                    Ok(u64::from_be_bytes(buf))
                }
                None => {
                    // Only an empty database may have no offset. Failing closed
                    // on inconsistent metadata prevents overwriting old frames.
                    let mut frames = db.scan_prefix(b"fr/").await.map_err(slate_err)?;
                    if frames.next().await.map_err(slate_err)?.is_some() {
                        return Err(LogError::Slate(
                            "frame data exists without durable next_offset metadata".into(),
                        ));
                    }
                    Ok(0)
                }
            }
        })
    }

    /// Reject stale leaders against the durable generation, not only the in-memory copy.
    pub fn require_fence(&mut self, generation: u64) -> Result<(), LogError> {
        let stored = Self::read_leader_generation(Arc::clone(&self.db))?;
        self.leader_generation = stored;
        if generation < stored {
            return Err(LogError::StaleFence {
                ours: generation,
                stored,
            });
        }
        Ok(())
    }

    pub fn append(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        let mut frame = Vec::new();
        encode_frame(&header, &payload, &mut frame)?;
        self.append_frame(partition, header, payload, frame, None)
    }

    /// Append with an optional leadership fence token (HA M3).
    pub fn append_fenced(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
        fence_generation: Option<u64>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        if let Some(gen) = fence_generation {
            self.require_fence(gen)?;
        }
        let mut frame = Vec::new();
        encode_frame(&header, &payload, &mut frame)?;
        self.append_frame(partition, header, payload, frame, fence_generation)
    }

    fn append_frame(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
        frame: Vec<u8>,
        fence_generation: Option<u64>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        let offset = self.next_offset;
        let next = offset + 1;
        let db = Arc::clone(&self.db);
        let key = frame_key(offset);
        let frame_bytes = Bytes::from(frame.clone());
        let next_bytes = next.to_be_bytes();
        let stamp_gen = fence_generation.filter(|g| *g > self.leader_generation);
        let fence_check = fence_generation;
        let gen_bytes = stamp_gen.map(|g| g.to_be_bytes());

        block_on_slate(async move {
            if let Some(gen) = fence_check {
                let stored = match db.get(META_LEADER_GEN).await.map_err(slate_err)? {
                    Some(bytes) if bytes.len() >= 8 => {
                        let mut buf = [0u8; 8];
                        buf.copy_from_slice(&bytes[..8]);
                        u64::from_be_bytes(buf)
                    }
                    _ => 0u64,
                };
                if gen < stored {
                    return Err(LogError::StaleFence { ours: gen, stored });
                }
            }
            let mut batch = WriteBatch::new();
            batch.put_bytes(Bytes::from(key), frame_bytes);
            batch.put_bytes(
                Bytes::copy_from_slice(META_NEXT_OFFSET),
                Bytes::copy_from_slice(&next_bytes),
            );
            if let Some(gb) = gen_bytes {
                batch.put_bytes(
                    Bytes::copy_from_slice(META_LEADER_GEN),
                    Bytes::copy_from_slice(&gb),
                );
            }
            let opts = durable_write_opts();
            let _h = db
                .write_with_options(batch, &opts)
                .await
                .map_err(slate_err)?;
            // Explicit flush so ACK is only after object-store durability.
            db.flush().await.map_err(slate_err)?;
            Ok::<(), LogError>(())
        })?;

        self.next_offset = next;
        if let Some(g) = stamp_gen {
            self.leader_generation = g;
        }
        let stored = stored_from_header(header, partition, offset, payload);
        Ok((stored, frame))
    }

    pub fn append_raw_frame(
        &mut self,
        partition: u32,
        frame: &[u8],
        expected_offset: Option<u64>,
    ) -> Result<StoredMessage, LogError> {
        if let Some(want) = expected_offset {
            let have = self.next_offset;
            if have != want {
                return Err(LogError::OffsetMismatch { have, want });
            }
        }
        let (header, payload) = {
            let mut cursor = std::io::Cursor::new(frame);
            decode_frame(&mut cursor)?
        };
        let (stored, _) = self.append_frame(partition, header, payload, frame.to_vec(), None)?;
        Ok(stored)
    }

    pub fn read_range(
        &self,
        partition: u32,
        offset: u64,
        max_messages: usize,
    ) -> Result<Vec<StoredMessage>, LogError> {
        if max_messages == 0 || offset >= self.next_offset {
            return Ok(Vec::new());
        }
        let db = Arc::clone(&self.db);
        let end = self.next_offset;
        block_on_slate(async move {
            let mut out = Vec::with_capacity(max_messages.min(256));
            // Range iteration skips deleted frames and counts live messages,
            // just like the local WAL, without probing every purged offset.
            let mut frames = db
                .scan(frame_key(offset)..frame_key(end))
                .await
                .map_err(slate_err)?;
            while out.len() < max_messages {
                let Some(frame) = frames.next().await.map_err(slate_err)? else {
                    break;
                };
                let off = std::str::from_utf8(&frame.key)
                    .ok()
                    .and_then(|key| key.strip_prefix("fr/"))
                    .and_then(|key| key.parse::<u64>().ok())
                    .ok_or_else(|| LogError::Slate("invalid frame offset key".into()))?;
                let mut cursor = std::io::Cursor::new(frame.value.as_ref());
                let (header, payload) = decode_frame(&mut cursor)?;
                out.push(stored_from_header(header, partition, off, payload));
            }
            Ok(out)
        })
    }

    pub fn high_watermark(&self) -> u64 {
        self.next_offset
    }

    pub fn purge_offset(&mut self, offset: u64) -> bool {
        if offset >= self.next_offset {
            return false;
        }
        let db = Arc::clone(&self.db);
        let key = frame_key(offset);
        block_on_slate(async move {
            let opts = durable_write_opts();
            let mut batch = WriteBatch::new();
            batch.delete(key);
            let _h = db
                .write_with_options(batch, &opts)
                .await
                .map_err(slate_err)?;
            db.flush().await.map_err(slate_err)?;
            Ok::<(), LogError>(())
        })
        .is_ok()
    }

    pub fn sync(&mut self) -> Result<(), LogError> {
        let db = Arc::clone(&self.db);
        let next = self.next_offset.to_be_bytes();
        block_on_slate(async move {
            let mut batch = WriteBatch::new();
            batch.put_bytes(
                Bytes::copy_from_slice(META_NEXT_OFFSET),
                Bytes::copy_from_slice(&next),
            );
            let opts = durable_write_opts();
            let _h = db
                .write_with_options(batch, &opts)
                .await
                .map_err(slate_err)?;
            db.flush().await.map_err(slate_err)?;
            Ok::<(), LogError>(())
        })
    }
}

#[cfg(feature = "slate")]
fn stored_from_header(
    header: LogRecord,
    partition: u32,
    offset: u64,
    payload: Vec<u8>,
) -> StoredMessage {
    StoredMessage {
        id: header.id,
        tenant_id: header.tenant_id,
        topic: header.topic,
        partition,
        offset,
        routing_key: header.routing_key,
        payload,
        published_at_ms: header.published_at_ms,
        priority: header.priority,
        flow_parallelism: header.flow_parallelism,
        flow_key: header.flow_key,
        flow_rate: header.flow_rate,
        flow_period_secs: header.flow_period_secs,
        queue_id: header.queue_id,
        group_id: header.group_id,
        group_member_id: header.group_member_id,
        flow_profile_id: header.flow_profile_id,
        destination_url: header.destination_url,
        destination_secret: header.destination_secret,
        max_retries: header.max_retries,
        retry_backoff: header.retry_backoff.clone(),
        http_method: header.http_method,
        http_headers_json: header.http_headers_json,
        http_sign: header.http_sign,
        payload_ref_json: header.payload_ref_json,
    }
}

#[cfg(feature = "slate")]
fn slate_err(e: slatedb::Error) -> LogError {
    LogError::Slate(e.to_string())
}

#[cfg(all(test, feature = "slate"))]
mod tests {
    use super::*;

    #[test]
    fn local_and_slate_reads_skip_purges_after_reopen() {
        for slate in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
            let open = || {
                if slate {
                    crate::PartitionBackend::open_slate(
                        "purge-parity".into(),
                        dir.path(),
                        store.clone(),
                        0,
                        PartitionLogConfig::default(),
                    )
                    .unwrap()
                } else {
                    crate::PartitionBackend::open_local(dir.path(), PartitionLogConfig::default())
                        .unwrap()
                }
            };
            let close = |backend: crate::PartitionBackend| {
                if let crate::PartitionBackend::Slate(log) = backend {
                    block_on_slate(async move { log.db.close().await }).unwrap();
                }
            };
            let mut backend = open();
            for n in 0..4u8 {
                let header: LogRecord = serde_json::from_value(serde_json::json!({
                    "id": uuid::Uuid::new_v4(), "tenant_id": "tenant", "topic": "dlq",
                    "routing_key": "", "idempotency_key": null, "published_at_ms": 0
                }))
                .unwrap();
                backend.append(0, header, vec![n]).unwrap();
            }
            assert!(backend.purge_offset(0).unwrap());
            assert!(backend.purge_offset(2).unwrap());
            backend.sync().unwrap();
            close(backend);
            let mut backend = open();
            assert_eq!(backend.high_watermark(), 4);
            let messages = backend.read_range(0, 0, 2).unwrap();
            assert_eq!(
                messages.iter().map(|m| m.offset).collect::<Vec<_>>(),
                vec![1, 3]
            );
            assert_eq!(backend.read_range(0, 2, 1).unwrap()[0].payload, vec![3]);
            assert!(backend.read_range(0, 0, 0).unwrap().is_empty());
            assert!(backend.read_range(0, u64::MAX, 10).unwrap().is_empty());
            assert!(backend.purge_offset(1).unwrap());
            assert!(backend.purge_offset(3).unwrap());
            assert!(backend.read_range(0, 0, 10).unwrap().is_empty());
            close(backend);
            let backend = open();
            assert_eq!(backend.high_watermark(), 4);
            assert!(backend.read_range(0, 0, 10).unwrap().is_empty());
            close(backend);
        }
    }

    #[test]
    fn recovery_uses_atomic_offset_independent_of_history_size() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(block_on_slate(Db::open("offset-recovery", store)).unwrap());
        let writer = db.clone();
        block_on_slate(async move {
            writer
                .put(META_NEXT_OFFSET, u64::MAX.to_be_bytes())
                .await
                .unwrap();
        });
        // A history scan up to this value would never finish. Purged frames do
        // not affect the monotonically increasing durable offset.
        assert_eq!(
            SlatePartitionLog::recover_next_offset(db.clone(), 0).unwrap(),
            u64::MAX
        );
        block_on_slate(async move { db.close().await }).unwrap();
    }

    #[test]
    fn recovery_rejects_missing_or_malformed_metadata_with_frames() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(block_on_slate(Db::open("bad-offset", store)).unwrap());
        assert_eq!(
            SlatePartitionLog::recover_next_offset(db.clone(), 0).unwrap(),
            0
        );
        let writer = db.clone();
        block_on_slate(async move { writer.put(frame_key(0), b"frame").await }).unwrap();
        assert!(SlatePartitionLog::recover_next_offset(db.clone(), 0).is_err());
        let writer = db.clone();
        block_on_slate(async move { writer.put(META_NEXT_OFFSET, b"bad").await }).unwrap();
        assert!(SlatePartitionLog::recover_next_offset(db.clone(), 0).is_err());
        block_on_slate(async move { db.close().await }).unwrap();
    }
}
