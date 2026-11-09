//! Append-only partition log: active WAL + sealed segments.

use crate::manifest::{WalManifest, WAL_FORMAT_V1, WAL_FORMAT_V2};
use crate::meta::LogMeta;
use broker_proto::epoch::{
    decode_epoch, encode_epoch, EpochError, EpochHeader, EPOCH_HEADER_BYTES,
};
use broker_proto::record::{decode_frame, encode_frame, RecordError};
use broker_proto::{LogRecord, StoredMessage};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Cursor, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::info;

const WAL_FILE: &str = "active.wal";
const SEGMENTS_DIR: &str = "segments";
const DEFAULT_GROUP_INTERVAL_MS: u64 = 10;
const DEFAULT_SPARSE_INDEX_STRIDE: u64 = 128;
const DEFAULT_READER_CACHE_CAPACITY: usize = 64;

/// How often the local WAL is forced to stable storage.
///
/// Override with `BETTERMQ_FSYNC=always|group|os` (default `group`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    /// `sync_all` + persist `meta.json` after every record.
    Always,
    /// Write without fsync; flush WAL + meta together on a timer, request end, roll, or drop.
    Group,
    /// No explicit fsync (OS page cache). Fastest; a crash can lose recent writes.
    Os,
}

impl FsyncMode {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "always" | "every" | "record" => Some(Self::Always),
            "group" | "batch" => Some(Self::Group),
            "os" | "never" | "none" => Some(Self::Os),
            _ => None,
        }
    }

    pub fn from_env() -> Self {
        match std::env::var("BETTERMQ_FSYNC") {
            Ok(raw) if !raw.trim().is_empty() => Self::parse(&raw).unwrap_or_else(|| {
                tracing::warn!(
                    value = %raw,
                    "invalid BETTERMQ_FSYNC (use always|group|os); defaulting to group"
                );
                Self::Group
            }),
            _ => Self::Group,
        }
    }
}

fn group_interval_from_env() -> Duration {
    let ms = std::env::var("BETTERMQ_FSYNC_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_GROUP_INTERVAL_MS)
        .clamp(1, 60_000);
    Duration::from_millis(ms)
}

#[derive(Debug, Clone)]
pub struct PartitionLogConfig {
    /// Roll WAL into a segment when it exceeds this many bytes.
    pub segment_max_bytes: u64,
    pub fsync: FsyncMode,
    /// Max delay before a group flush (used when `fsync` is [`FsyncMode::Group`]).
    pub group_interval: Duration,
}

impl Default for PartitionLogConfig {
    fn default() -> Self {
        Self {
            segment_max_bytes: 8 * 1024 * 1024,
            fsync: FsyncMode::Group,
            group_interval: Duration::from_millis(DEFAULT_GROUP_INTERVAL_MS),
        }
    }
}

impl PartitionLogConfig {
    pub fn from_env() -> Self {
        Self {
            segment_max_bytes: 8 * 1024 * 1024,
            fsync: FsyncMode::from_env(),
            group_interval: group_interval_from_env(),
        }
    }
}

#[derive(Debug, Clone)]
struct LogPosition {
    path: PathBuf,
    byte_offset: u64,
}

#[derive(Debug, Clone)]
struct SparsePoint {
    offset: u64,
    byte_offset: u64,
}

#[derive(Debug, Clone)]
struct SegmentIndex {
    path: PathBuf,
    first_offset: u64,
    last_offset: u64,
    points: Vec<SparsePoint>,
}

struct ReaderCache {
    files: HashMap<PathBuf, File>,
    order: VecDeque<PathBuf>,
    capacity: usize,
}

impl ReaderCache {
    fn new() -> Self {
        let capacity = std::env::var("BETTERMQ_SEGMENT_READER_CACHE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_READER_CACHE_CAPACITY)
            .clamp(1, 4096);
        Self {
            files: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn clone_reader(&mut self, path: &Path) -> io::Result<File> {
        if !self.files.contains_key(path) {
            while self.files.len() >= self.capacity {
                if let Some(evicted) = self.order.pop_front() {
                    self.files.remove(&evicted);
                }
            }
            self.files.insert(path.to_path_buf(), File::open(path)?);
        }
        self.order.retain(|cached| cached != path);
        self.order.push_back(path.to_path_buf());
        self.files.get(path).expect("reader inserted").try_clone()
    }

    fn remove(&mut self, path: &Path) {
        self.files.remove(path);
        self.order.retain(|cached| cached != path);
    }
}

#[derive(Debug, Error)]
pub enum LogError {
    #[error("record error: {0}")]
    Record(#[from] RecordError),
    #[error("epoch error: {0}")]
    Epoch(#[from] EpochError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("offset {0} not found")]
    OffsetNotFound(u64),
    #[error("offset mismatch: have next={have}, want={want}")]
    OffsetMismatch { have: u64, want: u64 },
    #[error("stale leader fence: our generation {ours} < stored {stored}")]
    StaleFence { ours: u64, stored: u64 },
    #[error("corrupt segment {path} at offset {offset}: {source}")]
    CorruptSegment {
        path: String,
        offset: u64,
        source: RecordError,
    },
    #[error("slate: {0}")]
    Slate(String),
    #[error("partition log fenced until reopened from disk: {0}")]
    Fenced(String),
}

#[cfg(test)]
#[derive(Default)]
struct TestFaults {
    /// Write this many bytes of the next WAL write, then fail it.
    short_write: Option<usize>,
    /// Fail the rollback that follows a failed WAL write.
    fail_rollback: bool,
}

/// Durable log for one partition directory.
pub struct PartitionLog {
    dir: PathBuf,
    config: PartitionLogConfig,
    meta: LogMeta,
    manifest: WalManifest,
    wal: File,
    wal_size: u64,
    /// Full index is bounded to the active WAL only.
    index: BTreeMap<u64, LogPosition>,
    sealed_indexes: Vec<SegmentIndex>,
    active_base_offset: u64,
    /// Open readers use a bounded LRU; clones have independent cursors.
    reader_cache: Mutex<ReaderCache>,
    dirty: bool,
    last_flush: Instant,
    /// Last offset exclusive that has been fsynced (commit high watermark).
    committed_hwm: u64,
    flush_count: u64,
    fail_next_sync: bool,
    /// Set once the WAL on disk may no longer match the in-memory state.
    /// Every later write or durability barrier fails until a reopen rebuilds
    /// the log from disk.
    fenced: Option<String>,
    telemetry: crate::telemetry::StorageTelemetryHandle,
    #[cfg(test)]
    faults: TestFaults,
}

impl PartitionLog {
    pub fn open(dir: impl AsRef<Path>, config: PartitionLogConfig) -> Result<Self, LogError> {
        Self::open_for_shard(dir, config, 0, WAL_FORMAT_V2)
    }

    pub fn open_for_shard(
        dir: impl AsRef<Path>,
        config: PartitionLogConfig,
        shard_id: u32,
        requested_format: u16,
    ) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        std::fs::create_dir_all(dir.join(SEGMENTS_DIR))?;
        let manifest = WalManifest::load_or_create(&dir, requested_format, shard_id)?;

        let meta = match LogMeta::load(&dir) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => LogMeta::default(),
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                if broker_proto::allow_empty_metadata_recovery() {
                    LogMeta::default()
                } else {
                    return Err(LogError::Io(e));
                }
            }
            Err(e) => return Err(e.into()),
        };
        let wal_path = dir.join(WAL_FILE);
        let wal = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;

        let mut log = Self {
            dir: dir.clone(),
            config,
            meta,
            manifest,
            wal,
            wal_size: 0,
            index: BTreeMap::new(),
            sealed_indexes: Vec::new(),
            active_base_offset: 0,
            reader_cache: Mutex::new(ReaderCache::new()),
            dirty: false,
            last_flush: Instant::now(),
            committed_hwm: 0,
            flush_count: 0,
            fail_next_sync: false,
            fenced: None,
            telemetry: crate::telemetry::register_shard(shard_id),
            #[cfg(test)]
            faults: TestFaults::default(),
        };

        log.rebuild_from_disk()?;
        log.committed_hwm = log.meta.next_offset;

        info!(
            dir = %dir.display(),
            next_offset = log.meta.next_offset,
            records = log.index.len(),
            wal_format = log.manifest.wal_format_version,
            "partition log opened"
        );

        Ok(log)
    }

    fn rebuild_from_disk(&mut self) -> Result<(), LogError> {
        self.index.clear();
        self.sealed_indexes.clear();
        let leftovers = self.remove_segments_below_base()?;
        if leftovers > 0 {
            tracing::warn!(
                dir = %self.dir.display(),
                leftovers,
                base_segment = self.meta.base_segment,
                "removed sealed segments left behind by an interrupted GC"
            );
        }

        let mut segment_files: Vec<PathBuf> = std::fs::read_dir(self.dir.join(SEGMENTS_DIR))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file())
            .collect();
        segment_files.sort();

        // A crash between the roll rename and the meta checkpoint, a failed
        // checkpoint, or a reset meta.json leaves a stale counter. Never hand
        // out a roll number that already names a sealed segment on disk.
        if let Some(highest) = segment_files.iter().filter_map(|p| segment_index(p)).max() {
            self.meta.segment_roll_count =
                self.meta.segment_roll_count.max(highest.saturating_add(1));
        }
        self.meta.segment_roll_count = self.meta.segment_roll_count.max(self.meta.base_segment);
        if let Some(lowest) = segment_files.iter().filter_map(|p| segment_index(p)).min() {
            if lowest > self.meta.base_segment {
                self.recover_missing_base(&segment_files, lowest)?;
            }
        }

        // Offsets are absolute: a GC'd prefix must not renumber survivors.
        let mut next = self.meta.base_offset;
        for path in &segment_files {
            let first = next;
            next = self.scan_file(path, next, false)?;
            if next > first {
                self.sealed_indexes
                    .push(self.build_sparse_index(path, first, next)?);
                self.index
                    .retain(|offset, _| *offset < first || *offset >= next);
            }
        }

        self.active_base_offset = next;
        let wal_path = self.dir.join(WAL_FILE);
        next = self.scan_file(&wal_path, next, true)?;

        self.wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        self.meta.next_offset = next;
        self.meta.save(&self.dir)?;
        Ok(())
    }

    /// The lowest sealed segment on disk is newer than `meta.base_segment`,
    /// so a prefix was deleted without a matching base (meta.json lost or
    /// reset, or a GC from before the base was persisted). V2 epochs carry
    /// absolute offsets, so the base is read back from the first one. V1
    /// frames carry none; guessing would silently renumber records.
    fn recover_missing_base(
        &mut self,
        segment_files: &[PathBuf],
        lowest: u64,
    ) -> Result<(), LogError> {
        let Some(first_path) = segment_files
            .iter()
            .find(|path| segment_index(path) == Some(lowest))
        else {
            return Ok(());
        };
        if self.manifest.wal_format_version != WAL_FORMAT_V2 {
            return Err(LogError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "sealed segments before {} are missing and V1 frames carry no offsets; \
                     restore meta.json (base_offset) or the missing segments",
                    first_path.display()
                ),
            )));
        }
        let (epoch, _) = decode_epoch(&mut BufReader::new(File::open(first_path)?))?;
        if epoch.first_offset < self.meta.base_offset {
            return Err(LogError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} starts at offset {} below the retained base {}",
                    first_path.display(),
                    epoch.first_offset,
                    self.meta.base_offset
                ),
            )));
        }
        tracing::warn!(
            dir = %self.dir.display(),
            base_offset = epoch.first_offset,
            base_segment = lowest,
            "sealed-segment prefix missing from meta; recovered base from the first epoch"
        );
        self.meta.base_offset = epoch.first_offset;
        self.meta.base_segment = lowest;
        Ok(())
    }

    /// Delete sealed segments numbered below `meta.base_segment`. A GC or
    /// [`Self::reset_base`] persisted the new base first and then failed or
    /// crashed before unlinking them.
    fn remove_segments_below_base(&mut self) -> Result<usize, LogError> {
        let base = self.meta.base_segment;
        if base == 0 {
            return Ok(0);
        }
        let segments = self.dir.join(SEGMENTS_DIR);
        let mut removed = 0usize;
        for entry in std::fs::read_dir(&segments)? {
            let path = entry?.path();
            if segment_index(&path).is_none_or(|index| index >= base) {
                continue;
            }
            if let Ok(mut cache) = self.reader_cache.lock() {
                cache.remove(&path);
            }
            match std::fs::remove_file(&path) {
                Ok(()) => removed += 1,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        if removed > 0 {
            File::open(&segments)?.sync_all()?;
        }
        Ok(removed)
    }
    fn build_sparse_index(
        &self,
        path: &Path,
        first_offset: u64,
        last_offset: u64,
    ) -> Result<SegmentIndex, LogError> {
        let mut points = Vec::new();
        let mut file = BufReader::new(File::open(path)?);
        match self.manifest.wal_format_version {
            WAL_FORMAT_V1 => {
                let mut offset = first_offset;
                while offset < last_offset {
                    let byte_offset = file.stream_position()?;
                    decode_frame(&mut file)?;
                    if (offset - first_offset) % DEFAULT_SPARSE_INDEX_STRIDE == 0 {
                        points.push(SparsePoint {
                            offset,
                            byte_offset,
                        });
                    }
                    offset += 1;
                }
            }
            WAL_FORMAT_V2 => {
                while file.stream_position()? < std::fs::metadata(path)?.len() {
                    let epoch_start = file.stream_position()?;
                    let (epoch, _) = decode_epoch(&mut file)?;
                    for offset in epoch.first_offset..epoch.committed_hwm {
                        if (offset - first_offset) % DEFAULT_SPARSE_INDEX_STRIDE == 0 {
                            points.push(SparsePoint {
                                offset,
                                byte_offset: epoch_start,
                            });
                        }
                    }
                }
            }
            version => {
                return Err(LogError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported WAL format {version}"),
                )))
            }
        }
        if points.is_empty() {
            points.push(SparsePoint {
                offset: first_offset,
                byte_offset: 0,
            });
        }
        Ok(SegmentIndex {
            path: path.to_path_buf(),
            first_offset,
            last_offset,
            points,
        })
    }

    fn scan_file(
        &mut self,
        path: &Path,
        next_offset: u64,
        truncate_torn_tail: bool,
    ) -> Result<u64, LogError> {
        match self.manifest.wal_format_version {
            WAL_FORMAT_V1 => self.scan_v1_file(path, next_offset, truncate_torn_tail),
            WAL_FORMAT_V2 => self.scan_v2_file(path, next_offset, truncate_torn_tail),
            version => Err(LogError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported WAL format {version}"),
            ))),
        }
    }

    fn scan_v1_file(
        &mut self,
        path: &Path,
        mut next_offset: u64,
        truncate_torn_tail: bool,
    ) -> Result<u64, LogError> {
        let len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        if len == 0 {
            return Ok(next_offset);
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut byte_pos = 0u64;

        while byte_pos < len {
            let frame_start = byte_pos;
            match decode_frame(&mut reader) {
                Ok((_header, _payload)) => {
                    if !self.meta.purged_offsets.contains(&next_offset) {
                        self.index.insert(
                            next_offset,
                            LogPosition {
                                path: path.to_path_buf(),
                                byte_offset: frame_start,
                            },
                        );
                    }
                    next_offset += 1;
                    byte_pos = reader.stream_position()?;
                }
                Err(RecordError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    if truncate_torn_tail {
                        truncate_file(path, frame_start)?;
                    }
                    break;
                }
                Err(e) => {
                    let pos = reader.stream_position().unwrap_or(byte_pos);
                    if pos < len {
                        quarantine_corrupt_file(path);
                        return Err(LogError::CorruptSegment {
                            path: path.display().to_string(),
                            offset: next_offset,
                            source: e,
                        });
                    }
                    break;
                }
            }
        }
        Ok(next_offset)
    }

    fn scan_v2_file(
        &mut self,
        path: &Path,
        mut next_offset: u64,
        truncate_torn_tail: bool,
    ) -> Result<u64, LogError> {
        let len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        if len == 0 {
            return Ok(next_offset);
        }
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut byte_pos = 0u64;

        while byte_pos < len {
            let epoch_start = byte_pos;
            let (epoch, body) = match decode_epoch(&mut reader) {
                Ok(decoded) => decoded,
                Err(EpochError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    if truncate_torn_tail {
                        truncate_file(path, epoch_start)?;
                        break;
                    }
                    return Err(LogError::Epoch(EpochError::Io(e)));
                }
                Err(e) => {
                    if truncate_torn_tail
                        && len.saturating_sub(epoch_start) < EPOCH_HEADER_BYTES as u64
                    {
                        truncate_file(path, epoch_start)?;
                        break;
                    }
                    quarantine_corrupt_file(path);
                    return Err(LogError::Epoch(e));
                }
            };
            if epoch.shard_id != self.manifest.shard_id
                || epoch.first_offset != next_offset
                || epoch.committed_hwm
                    != epoch.first_offset.saturating_add(epoch.record_count as u64)
            {
                quarantine_corrupt_file(path);
                return Err(LogError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "invalid epoch: shard={} first={} count={} hwm={}",
                        epoch.shard_id, epoch.first_offset, epoch.record_count, epoch.committed_hwm
                    ),
                )));
            }

            let mut frames = Cursor::new(body.as_slice());
            for _ in 0..epoch.record_count {
                let frame_start = frames.position();
                decode_frame(&mut frames).map_err(|source| LogError::CorruptSegment {
                    path: path.display().to_string(),
                    offset: next_offset,
                    source,
                })?;
                if !self.meta.purged_offsets.contains(&next_offset) {
                    self.index.insert(
                        next_offset,
                        LogPosition {
                            path: path.to_path_buf(),
                            byte_offset: epoch_start + EPOCH_HEADER_BYTES as u64 + frame_start,
                        },
                    );
                }
                next_offset += 1;
            }
            if frames.position() != body.len() as u64 || next_offset != epoch.committed_hwm {
                quarantine_corrupt_file(path);
                return Err(LogError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "epoch record count/length mismatch",
                )));
            }
            byte_pos = reader.stream_position()?;
        }
        Ok(next_offset)
    }

    fn apply_fence(&mut self, fence_generation: Option<u64>) -> Result<(), LogError> {
        let Some(gen) = fence_generation else {
            return Ok(());
        };
        if gen < self.meta.leader_generation {
            return Err(LogError::StaleFence {
                ours: gen,
                stored: self.meta.leader_generation,
            });
        }
        if gen > self.meta.leader_generation {
            self.meta.leader_generation = gen;
            self.dirty = true;
        }
        Ok(())
    }

    /// Why the log refuses writes until it is reopened, if it does.
    pub fn fenced_reason(&self) -> Option<&str> {
        self.fenced.as_deref()
    }

    fn ensure_writable(&self) -> Result<(), LogError> {
        match &self.fenced {
            Some(reason) => Err(LogError::Fenced(reason.clone())),
            None => Ok(()),
        }
    }

    fn fence(&mut self, reason: String) {
        if self.fenced.is_some() {
            return;
        }
        tracing::error!(
            dir = %self.dir.display(),
            %reason,
            "partition log fenced; writes refused until it is reopened from disk"
        );
        self.telemetry.record_fence();
        self.fenced = Some(reason);
    }

    /// Append `bytes` at the end of the WAL (`self.wal_size`). If the write
    /// fails, any partially written bytes are cut off again so a torn record
    /// never sits in front of later ones; if that cannot be done durably the
    /// log is fenced.
    fn write_wal(&mut self, bytes: &[u8]) -> Result<(), LogError> {
        let Err(error) = self.write_wal_bytes(bytes) else {
            return Ok(());
        };
        if let Err(rollback) = self.truncate_wal_to_size() {
            self.fence(format!(
                "WAL write failed ({error}) and the partial record could not be removed ({rollback})"
            ));
        }
        Err(error.into())
    }

    fn write_wal_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        use std::io::Write;
        #[cfg(test)]
        if let Some(limit) = self.faults.short_write.take() {
            self.wal.write_all(&bytes[..limit.min(bytes.len())])?;
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "injected short write",
            ));
        }
        self.wal.write_all(bytes)
    }

    fn truncate_wal_to_size(&mut self) -> io::Result<()> {
        #[cfg(test)]
        if std::mem::take(&mut self.faults.fail_rollback) {
            return Err(io::Error::other("injected rollback failure"));
        }
        self.wal.set_len(self.wal_size)?;
        // Always durable, even in `os` mode: a torn record that reappears
        // after a crash would hide every record written after it.
        self.wal.sync_data()
    }

    /// Append a pre-encoded broker-proto frame (follower replication).
    /// When `expected_offset` is set, require `meta.next_offset == expected` (or
    /// idempotent success if that offset is already present).
    pub fn append_raw_frame(
        &mut self,
        partition: u32,
        frame: &[u8],
        expected_offset: Option<u64>,
    ) -> Result<StoredMessage, LogError> {
        self.ensure_writable()?;
        let (header, payload) = {
            let mut cursor = std::io::Cursor::new(frame);
            decode_frame(&mut cursor)?
        };

        if let Some(want) = expected_offset {
            let have = self.meta.next_offset;
            if have > want {
                // Idempotent retry: already applied this offset.
                if let Some(existing) = self.read_range(partition, want, 1)?.into_iter().next() {
                    if existing.offset == want && existing.id == header.id {
                        return Ok(existing);
                    }
                }
                return Err(LogError::OffsetMismatch { have, want });
            }
            if have < want {
                return Err(LogError::OffsetMismatch { have, want });
            }
        }

        if self.manifest.wal_format_version == WAL_FORMAT_V2 {
            let mut appended = self.append_batch(partition, vec![(header, payload)], None)?;
            return appended.pop().map(|(stored, _)| stored).ok_or_else(|| {
                LogError::Io(io::Error::other(
                    "empty result from single-record epoch append",
                ))
            });
        }

        let offset = self.meta.next_offset;
        let byte_offset = self.wal_size;
        let wal_path = self.dir.join(WAL_FILE);

        self.write_wal(frame)?;

        self.index.insert(
            offset,
            LogPosition {
                path: wal_path,
                byte_offset,
            },
        );
        self.wal_size += frame.len() as u64;
        self.meta.next_offset = offset + 1;
        self.after_write()?;

        if self.wal_size >= self.config.segment_max_bytes {
            self.roll_segment()?;
        }

        Ok(StoredMessage {
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
            flow_key: header.flow_key.clone(),
            flow_rate: header.flow_rate,
            flow_period_secs: header.flow_period_secs,
            queue_id: header.queue_id,
            group_id: header.group_id,
            group_member_id: header.group_member_id,
            flow_profile_id: header.flow_profile_id,
            destination_url: header.destination_url.clone(),
            destination_secret: header.destination_secret.clone(),
            max_retries: header.max_retries,
            retry_backoff: header.retry_backoff.clone(),
            http_method: header.http_method.clone(),
            http_headers_json: header.http_headers_json.clone(),
            http_sign: header.http_sign,
            payload_ref_json: header.payload_ref_json.clone(),
        })
    }

    pub fn append(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        self.append_fenced(partition, header, payload, None)
    }

    pub fn append_fenced(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
        fence_generation: Option<u64>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        self.ensure_writable()?;
        self.apply_fence(fence_generation)?;
        if self.manifest.wal_format_version == WAL_FORMAT_V2 {
            return self
                .append_batch(partition, vec![(header, payload)], fence_generation)?
                .pop()
                .ok_or_else(|| {
                    LogError::Io(io::Error::other(
                        "empty result from single-record epoch append",
                    ))
                });
        }
        let offset = self.meta.next_offset;
        let byte_offset = self.wal_size;
        let wal_path = self.dir.join(WAL_FILE);

        let mut frame = Vec::new();
        encode_frame(&header, &payload, &mut frame)?;
        self.write_wal(&frame)?;

        self.index.insert(
            offset,
            LogPosition {
                path: wal_path,
                byte_offset,
            },
        );
        self.wal_size += frame.len() as u64;
        self.meta.next_offset = offset + 1;
        self.after_write()?;

        if self.wal_size >= self.config.segment_max_bytes {
            self.roll_segment()?;
        }

        let stored = StoredMessage {
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
            flow_key: header.flow_key.clone(),
            flow_rate: header.flow_rate,
            flow_period_secs: header.flow_period_secs,
            queue_id: header.queue_id,
            group_id: header.group_id,
            group_member_id: header.group_member_id,
            flow_profile_id: header.flow_profile_id,
            destination_url: header.destination_url.clone(),
            destination_secret: header.destination_secret.clone(),
            max_retries: header.max_retries,
            retry_backoff: header.retry_backoff.clone(),
            http_method: header.http_method.clone(),
            http_headers_json: header.http_headers_json.clone(),
            http_sign: header.http_sign,
            payload_ref_json: header.payload_ref_json.clone(),
        };
        Ok((stored, frame))
    }

    /// Append many records with one `after_write` (one dirty epoch).
    pub fn append_batch(
        &mut self,
        partition: u32,
        items: Vec<(LogRecord, Vec<u8>)>,
        fence_generation: Option<u64>,
    ) -> Result<Vec<(StoredMessage, Vec<u8>)>, LogError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_writable()?;
        self.apply_fence(fence_generation)?;
        let mut out = Vec::with_capacity(items.len());
        // Positions are installed only once the bytes are in the WAL.
        let mut positions = Vec::with_capacity(items.len());
        let mut buf = Vec::new();
        let wal_path = self.dir.join(WAL_FILE);
        let first_offset = self.meta.next_offset;
        let epoch_prefix = if self.manifest.wal_format_version == WAL_FORMAT_V2 {
            EPOCH_HEADER_BYTES as u64
        } else {
            0
        };
        let mut byte_offset = self.wal_size + epoch_prefix;
        let mut offset = first_offset;
        for (header, payload) in items {
            let frame_start = buf.len();
            encode_frame(&header, &payload, &mut buf)?;
            let frame = buf[frame_start..].to_vec();
            positions.push((
                offset,
                LogPosition {
                    path: wal_path.clone(),
                    byte_offset,
                },
            ));
            byte_offset += frame.len() as u64;
            let stored = StoredMessage {
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
                flow_key: header.flow_key.clone(),
                flow_rate: header.flow_rate,
                flow_period_secs: header.flow_period_secs,
                queue_id: header.queue_id,
                group_id: header.group_id,
                group_member_id: header.group_member_id,
                flow_profile_id: header.flow_profile_id,
                destination_url: header.destination_url.clone(),
                destination_secret: header.destination_secret.clone(),
                max_retries: header.max_retries,
                retry_backoff: header.retry_backoff.clone(),
                http_method: header.http_method.clone(),
                http_headers_json: header.http_headers_json.clone(),
                http_sign: header.http_sign,
                payload_ref_json: header.payload_ref_json.clone(),
            };
            offset += 1;
            out.push((stored, frame));
        }
        let epoch_bytes = if self.manifest.wal_format_version == WAL_FORMAT_V2 {
            let epoch = EpochHeader::v2(
                self.manifest.shard_id,
                self.meta.leader_generation,
                first_offset,
                out.len() as u32,
                &buf,
            );
            let mut encoded = Vec::with_capacity(EPOCH_HEADER_BYTES + buf.len());
            encode_epoch(&epoch, &buf, &mut encoded)?;
            self.write_wal(&encoded)?;
            self.wal_size = self.wal_size.saturating_add(encoded.len() as u64);
            encoded.len() as u64
        } else {
            self.write_wal(&buf)?;
            self.wal_size = byte_offset;
            buf.len() as u64
        };
        self.index.extend(positions);
        self.meta.next_offset = offset;
        self.after_write()?;
        self.telemetry
            .record_epoch(self.meta.leader_generation, out.len() as u64, epoch_bytes);
        if self.wal_size >= self.config.segment_max_bytes {
            self.roll_segment()?;
        }
        Ok(out)
    }

    fn roll_segment(&mut self) -> Result<(), LogError> {
        if self.wal_size == 0 {
            return Ok(());
        }

        let roll = self.meta.segment_roll_count;
        let seg_path = self.dir.join(SEGMENTS_DIR).join(segment_file_name(roll));
        let wal_path = self.dir.join(WAL_FILE);
        if std::fs::symlink_metadata(&seg_path).is_ok() {
            // Checked before the WAL handle is swapped so the log stays usable.
            return Err(segment_exists_error(&seg_path).into());
        }

        if self.config.fsync != FsyncMode::Os {
            let started = Instant::now();
            let pending_records = self.meta.next_offset.saturating_sub(self.committed_hwm);
            if let Err(error) = self.wal.sync_all() {
                self.telemetry
                    .record_fsync(pending_records, started.elapsed(), false);
                return Err(error.into());
            }
            self.telemetry
                .record_fsync(pending_records, started.elapsed(), true);
        }

        // Close the WAL handle before rename. Do not truncate active.wal first.
        let placeholder = self.dir.join(".wal_fd_placeholder");
        let _closed = std::mem::replace(
            &mut self.wal,
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&placeholder)?,
        );

        if let Err(error) = rename_no_replace(&wal_path, &seg_path) {
            // active.wal is still in place: reattach to it so later appends do
            // not land in the placeholder.
            self.wal = OpenOptions::new().append(true).read(true).open(&wal_path)?;
            let _ = std::fs::remove_file(&placeholder);
            return Err(error.into());
        }
        if let Ok(mut cache) = self.reader_cache.lock() {
            cache.remove(&wal_path);
            cache.remove(&seg_path);
        }
        if self.config.fsync != FsyncMode::Os {
            // Durability of the segment rename (parent dir entry).
            if let Ok(dir) = std::fs::File::open(self.dir.join(SEGMENTS_DIR)) {
                let _ = dir.sync_all();
            }
            if let Ok(dir) = std::fs::File::open(&self.dir) {
                let _ = dir.sync_all();
            }
        }
        let _ = std::fs::remove_file(&placeholder);

        // The segment is sealed on disk from here on. Bring the in-memory
        // state in line before anything else can fail, so an error below
        // never leaves the index pointing at the renamed active.wal or the
        // write handle on the deleted placeholder.
        self.meta.segment_roll_count = roll + 1;
        self.dirty = false;
        self.last_flush = Instant::now();
        self.committed_hwm = self.meta.next_offset;
        let sparse = if self.meta.next_offset > self.active_base_offset {
            Some(self.build_sparse_index(&seg_path, self.active_base_offset, self.meta.next_offset))
        } else {
            None
        };
        let sparse_error = match sparse {
            Some(Ok(segment)) => {
                self.sealed_indexes.push(segment);
                self.index.retain(|_, pos| pos.path != wal_path);
                None
            }
            Some(Err(error)) => {
                // Keep the exact positions; the bytes moved with the rename.
                for pos in self.index.values_mut() {
                    if pos.path == wal_path {
                        pos.path = seg_path.clone();
                    }
                }
                Some(error)
            }
            None => None,
        };
        self.active_base_offset = self.meta.next_offset;
        self.wal_size = 0;
        self.wal = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;
        crate::archive::enqueue_sealed_segment(&seg_path);
        // meta.json is a checkpoint: open reconciles the roll counter with
        // the segment files, so a failed save here cannot cause an overwrite.
        self.meta
            .save_with(&self.dir, self.config.fsync != FsyncMode::Os)?;
        match sparse_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub fn read_range(
        &self,
        partition: u32,
        start_offset: u64,
        max_messages: usize,
    ) -> Result<Vec<StoredMessage>, LogError> {
        let mut out = Vec::with_capacity(max_messages.min(256));
        for segment in self
            .sealed_indexes
            .iter()
            .filter(|segment| segment.last_offset > start_offset)
        {
            self.read_sparse_segment(segment, partition, start_offset, max_messages, &mut out)?;
            if out.len() >= max_messages {
                return Ok(out);
            }
        }
        for (&offset, pos) in self.index.range(start_offset..) {
            if out.len() >= max_messages {
                break;
            }
            let (header, payload) = self.read_at(pos)?;
            out.push(stored_message(header, payload, partition, offset));
        }
        Ok(out)
    }

    fn read_sparse_segment(
        &self,
        segment: &SegmentIndex,
        partition: u32,
        start_offset: u64,
        max_messages: usize,
        out: &mut Vec<StoredMessage>,
    ) -> Result<(), LogError> {
        let point = segment
            .points
            .iter()
            .rev()
            .find(|point| point.offset <= start_offset)
            .unwrap_or(&segment.points[0]);
        let mut file = self.cached_reader(&segment.path)?;
        file.seek(SeekFrom::Start(point.byte_offset))?;
        match self.manifest.wal_format_version {
            WAL_FORMAT_V1 => {
                let mut offset = point.offset;
                while offset < segment.last_offset && out.len() < max_messages {
                    let (header, payload) = decode_frame(&mut file)?;
                    if offset >= start_offset && !self.meta.purged_offsets.contains(&offset) {
                        out.push(stored_message(header, payload, partition, offset));
                    }
                    offset += 1;
                }
            }
            WAL_FORMAT_V2 => {
                while out.len() < max_messages {
                    let position = file.stream_position()?;
                    if position >= std::fs::metadata(&segment.path)?.len() {
                        break;
                    }
                    let (epoch, body) = decode_epoch(&mut file)?;
                    if epoch.first_offset >= segment.last_offset {
                        break;
                    }
                    let mut frames = Cursor::new(body);
                    for offset in epoch.first_offset..epoch.committed_hwm {
                        let (header, payload) = decode_frame(&mut frames)?;
                        if offset >= start_offset
                            && offset < segment.last_offset
                            && !self.meta.purged_offsets.contains(&offset)
                        {
                            out.push(stored_message(header, payload, partition, offset));
                            if out.len() >= max_messages {
                                break;
                            }
                        }
                    }
                }
            }
            _ => unreachable!("manifest version validated at open"),
        }
        Ok(())
    }

    pub fn high_watermark(&self) -> u64 {
        self.meta.next_offset
    }

    /// Exclusive offset of the last durable (fsynced) record.
    pub fn committed_hwm(&self) -> u64 {
        self.committed_hwm
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn fsync_mode(&self) -> FsyncMode {
        self.config.fsync
    }

    pub fn flush_count(&self) -> u64 {
        self.flush_count
    }

    pub fn resident_index_entries(&self) -> usize {
        self.index.len()
            + self
                .sealed_indexes
                .iter()
                .map(|segment| segment.points.len())
                .sum::<usize>()
    }

    /// Fail the next durability barrier before advancing the committed HWM.
    pub fn inject_fsync_failure(&mut self) {
        self.fail_next_sync = true;
    }

    /// First offset still retained (everything below was collected).
    pub fn base_offset(&self) -> u64 {
        self.meta.base_offset
    }

    /// Delete the leading sealed segments that end at or below `watermark`
    /// (exclusive offset). The active WAL is never deleted. Watermark only
    /// moves forward.
    ///
    /// The new retained base is fsynced to meta.json before any file is
    /// unlinked, so a crash leaves deletable leftovers (removed on the next
    /// open) and never renumbered records.
    pub fn gc_sealed_below(&mut self, watermark: u64) -> Result<usize, LogError> {
        self.ensure_writable()?;
        let current = self.meta.gc_watermark;
        if watermark < current {
            return Err(LogError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("gc watermark {watermark} is below durable {current}"),
            )));
        }
        let condemned = self
            .sealed_indexes
            .iter()
            .take_while(|segment| segment.last_offset <= watermark)
            .count();
        let previous = self.meta.clone();
        self.meta.gc_watermark = watermark;
        if condemned > 0 {
            let last = &self.sealed_indexes[condemned - 1];
            let last_roll = segment_index(&last.path).ok_or_else(|| {
                LogError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("non-canonical sealed segment name {}", last.path.display()),
                ))
            })?;
            self.set_base(last.last_offset, last_roll + 1);
        }
        if let Err(error) = self.meta.save_with(&self.dir, true) {
            self.meta = previous;
            return Err(error.into());
        }

        let doomed: Vec<SegmentIndex> = self.sealed_indexes.drain(..condemned).collect();
        self.index
            .retain(|_, pos| !doomed.iter().any(|seg| seg.path == pos.path));
        let mut deleted = 0usize;
        for segment in &doomed {
            if let Ok(mut cache) = self.reader_cache.lock() {
                cache.remove(&segment.path);
            }
            match std::fs::remove_file(&segment.path) {
                Ok(()) => deleted += 1,
                Err(err) if err.kind() == io::ErrorKind::NotFound => deleted += 1,
                // The base is already durable: the rest is removed on the
                // next GC or open.
                Err(err) => return Err(err.into()),
            }
        }
        // Also sweeps leftovers from an earlier interrupted GC.
        self.remove_segments_below_base()?;
        Ok(deleted)
    }

    /// Discard a log that holds nothing at or above `base` and continue at
    /// `base`, for a follower whose leader has already collected the prefix
    /// it is missing. Fails if any local record is at or above `base`.
    pub fn reset_base(&mut self, base: u64) -> Result<(), LogError> {
        self.ensure_writable()?;
        if self.meta.next_offset > base {
            return Err(LogError::OffsetMismatch {
                have: self.meta.next_offset,
                want: base,
            });
        }
        // Seal the active WAL first so every local record lives in a segment
        // below the new base segment; a crash at any later point leaves only
        // leftovers, never old records numbered from the new base.
        self.roll_segment()?;
        let previous = self.meta.clone();
        let roll = self.meta.segment_roll_count;
        self.set_base(base, roll);
        self.meta.next_offset = base;
        self.meta.gc_watermark = self.meta.gc_watermark.max(base);
        if let Err(error) = self.meta.save_with(&self.dir, true) {
            self.meta = previous;
            return Err(error.into());
        }
        for segment in self.sealed_indexes.drain(..) {
            if let Ok(mut cache) = self.reader_cache.lock() {
                cache.remove(&segment.path);
            }
        }
        self.index.clear();
        self.active_base_offset = base;
        self.committed_hwm = base;
        self.remove_segments_below_base()?;
        Ok(())
    }

    fn set_base(&mut self, base_offset: u64, base_segment: u64) {
        self.meta.base_offset = self.meta.base_offset.max(base_offset);
        self.meta.base_segment = self.meta.base_segment.max(base_segment);
        // Tombstones below the base refer to records that no longer exist.
        self.meta.purged_offsets = self.meta.purged_offsets.split_off(&self.meta.base_offset);
    }

    /// Tombstone a record: drop from the index and persist the offset in [`LogMeta`].
    /// WAL/segment bytes are not rewritten; [`Self::rebuild_from_disk`] skips purged offsets.
    pub fn purge_offset(&mut self, offset: u64) -> bool {
        if self.meta.purged_offsets.contains(&offset) {
            return false;
        }
        if offset >= self.meta.next_offset {
            return false;
        }
        let removed = self.index.remove(&offset).is_some()
            || self
                .sealed_indexes
                .iter()
                .any(|segment| offset >= segment.first_offset && offset < segment.last_offset);
        if removed {
            self.meta.purged_offsets.insert(offset);
            self.dirty = true;
            if self.config.fsync == FsyncMode::Always {
                if let Err(e) = self.flush_durable() {
                    tracing::warn!(error = %e, offset, "failed to persist purged offset");
                }
            }
        }
        removed
    }

    fn read_at(&self, pos: &LogPosition) -> Result<(LogRecord, Vec<u8>), LogError> {
        let mut file = self.cached_reader(&pos.path)?;
        file.seek(SeekFrom::Start(pos.byte_offset))?;
        decode_frame(&mut file).map_err(LogError::from)
    }

    fn cached_reader(&self, path: &Path) -> Result<File, LogError> {
        let mut cache = self
            .reader_cache
            .lock()
            .map_err(|_| LogError::Io(io::Error::other("segment reader cache poisoned")))?;
        Ok(cache.clone_reader(path)?)
    }

    fn after_write(&mut self) -> Result<(), LogError> {
        self.dirty = true;
        match self.config.fsync {
            FsyncMode::Always => self.flush_durable(),
            // Group/os: persist at HTTP request end, the background timer, roll, or drop.
            // Flushing here when `group_interval` elapsed re-enters fsync on the
            // ingest path and collapses back to ~one fsync per message.
            FsyncMode::Group | FsyncMode::Os => Ok(()),
        }
    }

    /// Persist WAL if there are unflushed writes. `meta.json` is a checkpoint,
    /// not the commit path — recovery reconstructs `next_offset` from the WAL.
    pub fn flush_durable(&mut self) -> Result<(), LogError> {
        if !self.dirty {
            return Ok(());
        }
        let started = Instant::now();
        let pending_records = self.meta.next_offset.saturating_sub(self.committed_hwm);
        if self.fail_next_sync {
            self.fail_next_sync = false;
            self.telemetry
                .record_fsync(pending_records, started.elapsed(), false);
            return Err(LogError::Io(io::Error::other("injected fsync failure")));
        }
        let persist_sync = self.config.fsync != FsyncMode::Os;
        if persist_sync {
            if let Err(error) = self.wal.sync_data() {
                self.telemetry
                    .record_fsync(pending_records, started.elapsed(), false);
                return Err(error.into());
            }
        }
        self.committed_hwm = self.meta.next_offset;
        self.flush_count = self.flush_count.saturating_add(1);
        let checkpoint = self.config.fsync == FsyncMode::Always || self.flush_count % 32 == 0;
        if checkpoint {
            if let Err(error) = self.meta.save_with(&self.dir, persist_sync) {
                if persist_sync {
                    self.telemetry
                        .record_fsync(pending_records, started.elapsed(), false);
                }
                return Err(error.into());
            }
        }
        self.dirty = false;
        self.last_flush = Instant::now();
        if persist_sync {
            self.telemetry
                .record_fsync(pending_records, started.elapsed(), true);
        }
        Ok(())
    }

    pub fn flush_if_due(&mut self) -> Result<(), LogError> {
        if self.config.fsync != FsyncMode::Group || !self.dirty {
            return Ok(());
        }
        if self.last_flush.elapsed() >= self.config.group_interval {
            self.flush_durable()
        } else {
            Ok(())
        }
    }

    pub fn sync(&mut self) -> Result<(), LogError> {
        self.flush_durable()
    }
}

impl Drop for PartitionLog {
    fn drop(&mut self) {
        if let Err(e) = self.flush_durable() {
            tracing::warn!(error = %e, "wal flush on drop failed");
        }
        let persist_sync = self.config.fsync != FsyncMode::Os;
        if let Err(e) = self.meta.save_with(&self.dir, persist_sync) {
            tracing::warn!(error = %e, "meta checkpoint on drop failed");
        }
    }
}

fn quarantine_corrupt_file(path: &Path) {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let dest = path.with_extension(format!("corrupt.{ts}"));
    match std::fs::rename(path, &dest) {
        Ok(()) => tracing::error!(
            from = %path.display(),
            to = %dest.display(),
            "quarantined corrupt WAL/segment; refusing to skip later frames"
        ),
        Err(e) => tracing::error!(
            file = %path.display(),
            error = %e,
            "could not quarantine corrupt WAL/segment"
        ),
    }
}

fn truncate_file(path: &Path, len: u64) -> io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len)?;
    file.sync_data()?;
    Ok(())
}

fn segment_file_name(roll: u64) -> String {
    format!("segment-{roll:06}.log")
}

/// Roll number of a canonical sealed segment name (`segment-NNNNNN.log`).
fn segment_index(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let digits = name.strip_prefix("segment-")?.strip_suffix(".log")?;
    if digits.len() < 6 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn segment_exists_error(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "refusing to roll onto existing sealed segment {}",
            path.display()
        ),
    )
}

/// Rename that fails with `AlreadyExists` instead of replacing `to`.
///
/// Uses `renameat2(RENAME_NOREPLACE)` on Linux. Filesystems without it fall
/// back to an existence check, which is race-free here because only the
/// owning shard actor writes this directory.
fn rename_no_replace(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        const RENAME_NOREPLACE: libc::c_uint = 1;
        let from_c = CString::new(from.as_os_str().as_bytes())?;
        let to_c = CString::new(to.as_os_str().as_bytes())?;
        // SAFETY: both paths are valid NUL-terminated strings that outlive the call.
        let rc = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                from_c.as_ptr(),
                libc::AT_FDCWD,
                to_c.as_ptr(),
                RENAME_NOREPLACE,
            )
        };
        if rc == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EEXIST) => return Err(segment_exists_error(to)),
            Some(libc::EINVAL) | Some(libc::ENOSYS) | Some(libc::EOPNOTSUPP) => {}
            _ => return Err(error),
        }
    }
    if std::fs::symlink_metadata(to).is_ok() {
        return Err(segment_exists_error(to));
    }
    std::fs::rename(from, to)
}

fn stored_message(
    header: LogRecord,
    payload: Vec<u8>,
    partition: u32,
    offset: u64,
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
        retry_backoff: header.retry_backoff,
        http_method: header.http_method,
        http_headers_json: header.http_headers_json,
        http_sign: header.http_sign,
        payload_ref_json: header.payload_ref_json,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn sample_record(topic: &str) -> LogRecord {
        LogRecord {
            id: Uuid::new_v4(),
            tenant_id: "default".into(),
            topic: topic.into(),
            routing_key: "rk".into(),
            idempotency_key: None,
            published_at_ms: 0,
            priority: 5,
            flow_parallelism: None,
            flow_key: None,
            flow_rate: None,
            flow_period_secs: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            flow_profile_id: None,
            destination_url: None,
            destination_secret: None,
            max_retries: 0,
            retry_backoff: None,
            http_method: None,
            http_headers_json: None,
            http_sign: None,
            payload_ref_json: None,
        }
    }

    #[test]
    fn append_and_read_back() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
        log.append(0, sample_record("t"), b"two".to_vec()).unwrap();

        let msgs = log.read_range(0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].offset, 0);
        assert_eq!(msgs[1].payload, b"two");
    }

    #[test]
    fn reopen_recovers() {
        let dir = tempdir().unwrap();
        {
            let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
            for i in 0..5u8 {
                log.append(0, sample_record("t"), vec![i]).unwrap();
            }
            log.sync().unwrap();
        }
        let log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        let msgs = log.read_range(0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 5);
        assert_eq!(log.high_watermark(), 5);
    }

    #[test]
    fn purge_offset_removes_from_reads() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
        assert!(log.purge_offset(0));
        let msgs = log.read_range(0, 0, 10).unwrap();
        assert!(msgs.is_empty());
    }

    #[test]
    fn purge_offset_survives_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
            log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
            log.append(0, sample_record("t"), b"two".to_vec()).unwrap();
            assert!(log.purge_offset(0));
            log.sync().unwrap();
        }
        let log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        let msgs = log.read_range(0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].offset, 1);
        assert_eq!(msgs[0].payload, b"two");
    }

    #[test]
    fn segment_roll_preserves_records() {
        let dir = tempdir().unwrap();
        let cfg = PartitionLogConfig {
            segment_max_bytes: 256,
            fsync: FsyncMode::Always,
            group_interval: Duration::from_millis(10),
        };
        let mut log = PartitionLog::open(dir.path(), cfg.clone()).unwrap();
        for i in 0..20u8 {
            log.append(0, sample_record("t"), vec![i; 32]).unwrap();
        }
        let log = PartitionLog::open(dir.path(), cfg).unwrap();
        let msgs = log.read_range(0, 0, 50).unwrap();
        assert_eq!(msgs.len(), 20);
    }

    fn small_segment_cfg() -> PartitionLogConfig {
        PartitionLogConfig {
            segment_max_bytes: 256,
            fsync: FsyncMode::Always,
            group_interval: Duration::from_millis(10),
        }
    }

    fn segment_names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir.join(SEGMENTS_DIR))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    fn append_until_roll(log: &mut PartitionLog, payload: u8) -> u64 {
        let rolls = log.meta.segment_roll_count;
        let mut appended = 0;
        while log.meta.segment_roll_count == rolls {
            log.append(0, sample_record("t"), vec![payload; 32])
                .unwrap();
            appended += 1;
        }
        appended
    }

    fn assert_payloads(log: &PartitionLog, expected: &[u8]) {
        let msgs = log.read_range(0, 0, 1000).unwrap();
        let got: Vec<u8> = msgs.iter().map(|m| m.payload[0]).collect();
        assert_eq!(got, expected);
        for (index, msg) in msgs.iter().enumerate() {
            assert_eq!(msg.offset, index as u64);
        }
    }

    fn reopen_after_stale_roll_counter_keeps_sealed_segments(format: u16, tamper: impl Fn(&Path)) {
        let dir = tempdir().unwrap();
        let mut expected = Vec::new();
        {
            let mut log =
                PartitionLog::open_for_shard(dir.path(), small_segment_cfg(), 0, format).unwrap();
            let first = append_until_roll(&mut log, 1);
            expected.extend(std::iter::repeat_n(1u8, first as usize));
        }
        assert_eq!(segment_names(dir.path()), vec!["segment-000000.log"]);
        tamper(dir.path());

        {
            let mut log =
                PartitionLog::open_for_shard(dir.path(), small_segment_cfg(), 0, format).unwrap();
            assert_eq!(log.meta.segment_roll_count, 1);
            let second = append_until_roll(&mut log, 2);
            expected.extend(std::iter::repeat_n(2u8, second as usize));
            assert_payloads(&log, &expected);
        }
        assert_eq!(
            segment_names(dir.path()),
            vec!["segment-000000.log", "segment-000001.log"]
        );
        let log = PartitionLog::open_for_shard(dir.path(), small_segment_cfg(), 0, format).unwrap();
        assert_payloads(&log, &expected);
    }

    fn rewind_roll_counter(dir: &Path) {
        // Crash after the rename but before the incremented counter was saved.
        let mut meta = LogMeta::load(dir).unwrap();
        meta.segment_roll_count -= 1;
        meta.save(dir).unwrap();
    }

    #[test]
    fn stale_roll_counter_after_crash_does_not_overwrite_segment() {
        for format in [WAL_FORMAT_V1, WAL_FORMAT_V2] {
            reopen_after_stale_roll_counter_keeps_sealed_segments(format, rewind_roll_counter);
        }
    }

    #[test]
    fn missing_meta_does_not_overwrite_segment() {
        for format in [WAL_FORMAT_V1, WAL_FORMAT_V2] {
            reopen_after_stale_roll_counter_keeps_sealed_segments(format, |dir| {
                std::fs::remove_file(LogMeta::path(dir)).unwrap();
            });
        }
    }

    #[test]
    fn roll_refuses_to_replace_existing_segment_and_log_stays_usable() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), small_segment_cfg()).unwrap();
        let foreign = dir.path().join(SEGMENTS_DIR).join("segment-000000.log");
        std::fs::write(&foreign, b"not ours").unwrap();

        let mut appended = 0u8;
        let err = loop {
            match log.append(0, sample_record("t"), vec![appended; 32]) {
                Ok(_) => appended += 1,
                Err(err) => break err,
            }
        };
        assert!(
            matches!(&err, LogError::Io(e) if e.kind() == io::ErrorKind::AlreadyExists),
            "unexpected error {err}"
        );
        assert_eq!(std::fs::read(&foreign).unwrap(), b"not ours");
        assert!(dir.path().join(WAL_FILE).exists());
        // The record that triggered the roll is in active.wal and later
        // appends still land there, not in a placeholder.
        let more = log.append(0, sample_record("t"), vec![0xee; 32]);
        assert!(more.is_err(), "roll is retried and refused again");
        let msgs = log.read_range(0, 0, 1000).unwrap();
        assert_eq!(msgs.len(), appended as usize + 2);
        assert_eq!(msgs.last().unwrap().payload, vec![0xee; 32]);
    }

    #[test]
    fn rename_no_replace_keeps_existing_target() {
        let dir = tempdir().unwrap();
        let from = dir.path().join("from");
        let to = dir.path().join("to");
        std::fs::write(&from, b"new").unwrap();
        std::fs::write(&to, b"old").unwrap();
        let err = rename_no_replace(&from, &to).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&to).unwrap(), b"old");
        assert_eq!(std::fs::read(&from).unwrap(), b"new");
        std::fs::remove_file(&to).unwrap();
        rename_no_replace(&from, &to).unwrap();
        assert_eq!(std::fs::read(&to).unwrap(), b"new");
        assert!(!from.exists());
    }

    #[test]
    fn segment_index_accepts_only_canonical_names() {
        assert_eq!(segment_index(Path::new("segment-000007.log")), Some(7));
        assert_eq!(
            segment_index(Path::new("segment-1234567.log")),
            Some(1_234_567)
        );
        assert_eq!(
            segment_index(Path::new("segment-000007.log.archive.pending")),
            None
        );
        assert_eq!(segment_index(Path::new("segment-000007.corrupt.1")), None);
        assert_eq!(segment_index(Path::new("segment-7.log")), None);
        assert_eq!(segment_index(Path::new("segment-00000x.log")), None);
    }

    #[test]
    fn sealed_segment_index_is_sparse_and_readable() {
        let dir = tempdir().unwrap();
        let cfg = PartitionLogConfig {
            segment_max_bytes: 1024,
            fsync: FsyncMode::Group,
            group_interval: Duration::from_secs(60),
        };
        let mut log =
            PartitionLog::open_for_shard(dir.path(), cfg.clone(), 4, WAL_FORMAT_V2).unwrap();
        let items = (0..1000)
            .map(|index| (sample_record("sparse"), vec![(index % 251) as u8; 32]))
            .collect();
        log.append_batch(4, items, None).unwrap();
        assert!(
            log.resident_index_entries() < 20,
            "sealed segment should retain sparse anchors, not every record"
        );
        let messages = log.read_range(4, 777, 25).unwrap();
        assert_eq!(messages.len(), 25);
        assert_eq!(messages[0].offset, 777);
        assert_eq!(messages[24].offset, 801);
        log.sync().unwrap();
        drop(log);

        let recovered = PartitionLog::open_for_shard(dir.path(), cfg, 4, WAL_FORMAT_V2).unwrap();
        assert!(recovered.resident_index_entries() < 20);
        assert_eq!(recovered.read_range(4, 995, 10).unwrap().len(), 5);
    }

    #[test]
    fn stale_fence_is_rejected() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        log.append_fenced(0, sample_record("t"), b"a".to_vec(), Some(3))
            .unwrap();
        let err = log
            .append_fenced(0, sample_record("t"), b"b".to_vec(), Some(2))
            .unwrap_err();
        assert!(matches!(err, LogError::StaleFence { ours: 2, stored: 3 }));
    }

    #[test]
    fn mid_file_checksum_does_not_skip_later_frames() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
        log.append(0, sample_record("t"), b"two".to_vec()).unwrap();
        log.sync().unwrap();
        drop(log);

        let wal = dir.path().join("active.wal");
        let mut bytes = std::fs::read(&wal).unwrap();
        // Flip a byte in the first frame body so the enclosing epoch CRC fails
        // while a second record remains.
        if bytes.len() > EPOCH_HEADER_BYTES + 16 {
            bytes[EPOCH_HEADER_BYTES + 16] ^= 0xff;
        }
        std::fs::write(&wal, &bytes).unwrap();

        match PartitionLog::open(dir.path(), PartitionLogConfig::default()) {
            Err(LogError::Epoch(EpochError::ChecksumMismatch)) => {}
            Err(e) => panic!("expected epoch checksum failure, got {e}"),
            Ok(_) => panic!("expected epoch checksum failure, log opened"),
        }
    }

    fn group_cfg(interval: Duration) -> PartitionLogConfig {
        PartitionLogConfig {
            segment_max_bytes: 8 * 1024 * 1024,
            fsync: FsyncMode::Group,
            group_interval: interval,
        }
    }

    #[test]
    fn fsync_mode_parse() {
        assert_eq!(FsyncMode::parse("always"), Some(FsyncMode::Always));
        assert_eq!(FsyncMode::parse("GROUP"), Some(FsyncMode::Group));
        assert_eq!(FsyncMode::parse("os"), Some(FsyncMode::Os));
        assert_eq!(FsyncMode::parse("nope"), None);
    }

    #[test]
    fn group_commit_defers_meta_until_flush() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), group_cfg(Duration::from_secs(60))).unwrap();
        let meta_path = dir.path().join("meta.json");
        let meta_after_open = std::fs::read(&meta_path).unwrap();
        for i in 0..50u8 {
            log.append(0, sample_record("t"), vec![i]).unwrap();
        }
        let meta_after_appends = std::fs::read(&meta_path).unwrap();
        assert_eq!(
            meta_after_open, meta_after_appends,
            "group commit must not rewrite meta.json per record"
        );
        assert_eq!(log.committed_hwm(), 0);
        log.sync().unwrap();
        assert_eq!(log.committed_hwm(), 50);
        assert_eq!(log.high_watermark(), 50);
        drop(log);
        let log = PartitionLog::open(dir.path(), group_cfg(Duration::from_secs(60))).unwrap();
        assert_eq!(log.high_watermark(), 50);
        assert_eq!(log.read_range(0, 0, 100).unwrap().len(), 50);
    }

    #[test]
    fn group_commit_unflushed_tail_may_disappear() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), group_cfg(Duration::from_secs(60))).unwrap();
        log.append(0, sample_record("t"), b"lost".to_vec()).unwrap();
        std::mem::forget(log);
        let recovered = PartitionLog::open(dir.path(), group_cfg(Duration::from_secs(60))).unwrap();
        assert!(recovered.high_watermark() <= 1);
    }

    #[test]
    fn always_mode_persists_meta_per_record() {
        let dir = tempdir().unwrap();
        let cfg = PartitionLogConfig {
            segment_max_bytes: 8 * 1024 * 1024,
            fsync: FsyncMode::Always,
            group_interval: Duration::from_millis(10),
        };
        let mut log = PartitionLog::open(dir.path(), cfg).unwrap();
        log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
        let meta = LogMeta::load(dir.path()).unwrap();
        assert_eq!(meta.next_offset, 1);
    }

    #[test]
    fn v2_batch_uses_epoch_frame_and_recovers() {
        let dir = tempdir().unwrap();
        {
            let mut log = PartitionLog::open_for_shard(
                dir.path(),
                group_cfg(Duration::from_secs(60)),
                9,
                WAL_FORMAT_V2,
            )
            .unwrap();
            log.append_batch(
                9,
                vec![
                    (sample_record("a"), b"one".to_vec()),
                    (sample_record("b"), b"two".to_vec()),
                ],
                Some(4),
            )
            .unwrap();
            log.sync().unwrap();
        }
        let bytes = std::fs::read(dir.path().join(WAL_FILE)).unwrap();
        assert_eq!(&bytes[..4], &broker_proto::EPOCH_MAGIC);

        let log = PartitionLog::open_for_shard(
            dir.path(),
            group_cfg(Duration::from_secs(60)),
            9,
            WAL_FORMAT_V2,
        )
        .unwrap();
        assert_eq!(log.high_watermark(), 2);
        assert_eq!(log.read_range(9, 0, 10).unwrap().len(), 2);
    }

    #[test]
    fn pre_manifest_v1_remains_readable() {
        let dir = tempdir().unwrap();
        {
            let mut log = PartitionLog::open_for_shard(
                dir.path(),
                PartitionLogConfig::default(),
                2,
                WAL_FORMAT_V1,
            )
            .unwrap();
            log.append(2, sample_record("legacy"), b"v1".to_vec())
                .unwrap();
            log.sync().unwrap();
        }
        std::fs::remove_file(WalManifest::path(dir.path())).unwrap();

        let log = PartitionLog::open_for_shard(
            dir.path(),
            PartitionLogConfig::default(),
            2,
            WAL_FORMAT_V2,
        )
        .unwrap();
        assert_eq!(log.manifest.wal_format_version, WAL_FORMAT_V1);
        assert_eq!(log.read_range(2, 0, 1).unwrap()[0].payload, b"v1");
    }

    #[test]
    fn recovery_truncates_incomplete_v2_epoch_tail() {
        let dir = tempdir().unwrap();
        {
            let mut log = PartitionLog::open_for_shard(
                dir.path(),
                PartitionLogConfig::default(),
                3,
                WAL_FORMAT_V2,
            )
            .unwrap();
            log.append(3, sample_record("t"), b"committed".to_vec())
                .unwrap();
            log.sync().unwrap();
        }
        let wal = dir.path().join(WAL_FILE);
        let valid_len = std::fs::metadata(&wal).unwrap().len();
        use std::io::Write;
        OpenOptions::new()
            .append(true)
            .open(&wal)
            .unwrap()
            .write_all(&broker_proto::EPOCH_MAGIC)
            .unwrap();

        let log = PartitionLog::open_for_shard(
            dir.path(),
            PartitionLogConfig::default(),
            3,
            WAL_FORMAT_V2,
        )
        .unwrap();
        assert_eq!(log.high_watermark(), 1);
        assert_eq!(std::fs::metadata(&wal).unwrap().len(), valid_len);
    }

    #[test]
    fn injected_fsync_failure_does_not_advance_hwm() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), group_cfg(Duration::from_secs(60))).unwrap();
        log.append(0, sample_record("t"), b"value".to_vec())
            .unwrap();
        log.inject_fsync_failure();
        assert!(log.sync().unwrap_err().to_string().contains("injected"));
        assert_eq!(log.committed_hwm(), 0);
        assert!(log.is_dirty());
        log.sync().unwrap();
        assert_eq!(log.committed_hwm(), 1);
    }

    fn wal_len(dir: &Path) -> u64 {
        std::fs::metadata(dir.join(WAL_FILE)).unwrap().len()
    }

    fn assert_payload_sequence(log: &PartitionLog, expected: &[&[u8]]) {
        let msgs = log.read_range(0, 0, 100).unwrap();
        let got: Vec<&[u8]> = msgs.iter().map(|m| m.payload.as_slice()).collect();
        assert_eq!(got, expected);
        for (index, msg) in msgs.iter().enumerate() {
            assert_eq!(msg.offset, index as u64);
        }
    }

    #[test]
    fn short_wal_write_is_rolled_back_before_later_appends() {
        for format in [WAL_FORMAT_V1, WAL_FORMAT_V2] {
            let dir = tempdir().unwrap();
            let mut log = PartitionLog::open_for_shard(
                dir.path(),
                group_cfg(Duration::from_secs(60)),
                0,
                format,
            )
            .unwrap();
            log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
            log.append(0, sample_record("t"), b"two".to_vec()).unwrap();
            let good_len = wal_len(dir.path());

            log.faults.short_write = Some(10);
            let err = log
                .append_batch(
                    0,
                    vec![
                        (sample_record("t"), b"lost-a".to_vec()),
                        (sample_record("t"), b"lost-b".to_vec()),
                    ],
                    None,
                )
                .unwrap_err();
            assert!(err.to_string().contains("injected short write"), "{err}");
            assert_eq!(wal_len(dir.path()), good_len, "partial bytes left behind");
            assert_eq!(log.high_watermark(), 2);
            assert!(log.read_range(0, 2, 10).unwrap().is_empty());

            let (third, _) = log
                .append(0, sample_record("t"), b"three".to_vec())
                .unwrap();
            assert_eq!(third.offset, 2);
            assert_payload_sequence(&log, &[b"one", b"two", b"three"]);
            log.sync().unwrap();
            drop(log);

            let log = PartitionLog::open_for_shard(
                dir.path(),
                group_cfg(Duration::from_secs(60)),
                0,
                format,
            )
            .unwrap();
            assert_payload_sequence(&log, &[b"one", b"two", b"three"]);
        }
    }

    #[test]
    fn short_write_on_single_v1_append_and_raw_frame_is_rolled_back() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open_for_shard(
            dir.path(),
            group_cfg(Duration::from_secs(60)),
            0,
            WAL_FORMAT_V1,
        )
        .unwrap();
        log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
        log.faults.short_write = Some(5);
        assert!(log.append(0, sample_record("t"), b"lost".to_vec()).is_err());
        let mut frame = Vec::new();
        encode_frame(&sample_record("t"), b"lost-raw", &mut frame).unwrap();
        log.faults.short_write = Some(5);
        assert!(log.append_raw_frame(0, &frame, Some(1)).is_err());
        log.append(0, sample_record("t"), b"two".to_vec()).unwrap();
        assert_payload_sequence(&log, &[b"one", b"two"]);
        log.sync().unwrap();
        drop(log);
        let log = PartitionLog::open_for_shard(
            dir.path(),
            group_cfg(Duration::from_secs(60)),
            0,
            WAL_FORMAT_V1,
        )
        .unwrap();
        assert_payload_sequence(&log, &[b"one", b"two"]);
    }

    #[test]
    fn failed_rollback_fences_the_log_until_reopen() {
        for format in [WAL_FORMAT_V1, WAL_FORMAT_V2] {
            let dir = tempdir().unwrap();
            let mut log = PartitionLog::open_for_shard(
                dir.path(),
                group_cfg(Duration::from_secs(60)),
                0,
                format,
            )
            .unwrap();
            log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
            log.sync().unwrap();
            log.faults.short_write = Some(10);
            log.faults.fail_rollback = true;
            assert!(log.append(0, sample_record("t"), b"torn".to_vec()).is_err());
            assert!(log.fenced_reason().is_some());
            assert!(matches!(
                log.append(0, sample_record("t"), b"refused".to_vec()),
                Err(LogError::Fenced(_))
            ));
            assert!(matches!(log.gc_sealed_below(1), Err(LogError::Fenced(_))));
            assert_payload_sequence(&log, &[b"one"]);
            drop(log);

            // Recovery drops the torn tail and the log is writable again.
            let mut log = PartitionLog::open_for_shard(
                dir.path(),
                group_cfg(Duration::from_secs(60)),
                0,
                format,
            )
            .unwrap();
            assert!(log.fenced_reason().is_none());
            assert_payload_sequence(&log, &[b"one"]);
            log.append(0, sample_record("t"), b"two".to_vec()).unwrap();
            assert_payload_sequence(&log, &[b"one", b"two"]);
        }
    }

    fn gc_cfg() -> PartitionLogConfig {
        PartitionLogConfig {
            segment_max_bytes: 1024,
            fsync: FsyncMode::Group,
            group_interval: Duration::from_secs(60),
        }
    }

    fn offset_payload(offset: u64) -> Vec<u8> {
        vec![(offset % 251) as u8; 32]
    }

    fn open_gc_log(dir: &Path, format: u16) -> PartitionLog {
        PartitionLog::open_for_shard(dir, gc_cfg(), 0, format).unwrap()
    }

    /// Append until at least `sealed` segments exist and the active WAL holds
    /// `active_records` records.
    fn fill_gc_log(log: &mut PartitionLog, sealed: usize, active_records: u64) {
        while log.sealed_indexes.len() < sealed {
            let offset = log.high_watermark();
            log.append(0, sample_record("gc"), offset_payload(offset))
                .unwrap();
        }
        for _ in 0..active_records {
            let offset = log.high_watermark();
            log.append(0, sample_record("gc"), offset_payload(offset))
                .unwrap();
        }
        assert_eq!(log.sealed_indexes.len(), sealed, "active WAL rolled");
        log.sync().unwrap();
    }

    fn assert_retained(log: &PartitionLog, base: u64, next: u64) {
        assert_eq!(log.base_offset(), base);
        assert_eq!(log.high_watermark(), next);
        let msgs = log.read_range(0, 0, 10_000).unwrap();
        let offsets: Vec<u64> = msgs.iter().map(|m| m.offset).collect();
        assert_eq!(offsets, (base..next).collect::<Vec<_>>());
        for msg in &msgs {
            assert_eq!(
                msg.payload,
                offset_payload(msg.offset),
                "offset {}",
                msg.offset
            );
        }
    }

    #[test]
    fn partial_prefix_gc_survives_reopen() {
        for format in [WAL_FORMAT_V1, WAL_FORMAT_V2] {
            let dir = tempdir().unwrap();
            let mut log = open_gc_log(dir.path(), format);
            fill_gc_log(&mut log, 4, 1);
            let base = log.sealed_indexes[1].last_offset;
            let next = log.high_watermark();
            assert_eq!(log.gc_sealed_below(base).unwrap(), 2);
            assert_retained(&log, base, next);
            drop(log);

            let mut log = open_gc_log(dir.path(), format);
            assert_retained(&log, base, next);
            let appended = log
                .append(0, sample_record("gc"), offset_payload(next))
                .unwrap();
            assert_eq!(appended.0.offset, next);
            log.sync().unwrap();
            drop(log);
            assert_retained(&open_gc_log(dir.path(), format), base, next + 1);
        }
    }

    #[test]
    fn full_prefix_gc_survives_reopen_with_and_without_active_records() {
        for format in [WAL_FORMAT_V1, WAL_FORMAT_V2] {
            for active_records in [0, 2] {
                let dir = tempdir().unwrap();
                let mut log = open_gc_log(dir.path(), format);
                fill_gc_log(&mut log, 3, active_records);
                let base = log.active_base_offset;
                let next = log.high_watermark();
                assert_eq!(log.gc_sealed_below(next).unwrap(), 3);
                assert!(segment_names(dir.path()).is_empty());
                drop(log);

                let mut log = open_gc_log(dir.path(), format);
                assert_retained(&log, base, next);
                let appended = log
                    .append(0, sample_record("gc"), offset_payload(next))
                    .unwrap();
                assert_eq!(appended.0.offset, next, "offsets are never reused");
                log.sync().unwrap();
                drop(log);
                assert_retained(&open_gc_log(dir.path(), format), base, next + 1);
            }
        }
    }

    #[test]
    fn crash_between_gc_meta_write_and_unlink_leaves_only_leftovers() {
        for format in [WAL_FORMAT_V1, WAL_FORMAT_V2] {
            let dir = tempdir().unwrap();
            let mut log = open_gc_log(dir.path(), format);
            fill_gc_log(&mut log, 3, 1);
            let doomed: Vec<(PathBuf, Vec<u8>)> = log.sealed_indexes[..2]
                .iter()
                .map(|seg| (seg.path.clone(), std::fs::read(&seg.path).unwrap()))
                .collect();
            let base = log.sealed_indexes[1].last_offset;
            let next = log.high_watermark();
            log.gc_sealed_below(base).unwrap();
            drop(log);
            // Meta with the new base is durable; the unlinks never happened.
            for (path, bytes) in &doomed {
                std::fs::write(path, bytes).unwrap();
            }

            let log = open_gc_log(dir.path(), format);
            assert_retained(&log, base, next);
            for (path, _) in &doomed {
                assert!(!path.exists(), "leftover {} not removed", path.display());
            }
        }
    }

    #[test]
    fn failed_gc_meta_write_deletes_nothing() {
        let dir = tempdir().unwrap();
        let mut log = open_gc_log(dir.path(), WAL_FORMAT_V2);
        fill_gc_log(&mut log, 2, 0);
        let before = segment_names(dir.path());
        // meta.json.tmp as a directory makes the atomic meta write fail.
        std::fs::create_dir(dir.path().join("meta.json.tmp")).unwrap();
        let next = log.high_watermark();
        assert!(log.gc_sealed_below(next).is_err());
        assert_eq!(segment_names(dir.path()), before);
        assert_eq!(log.base_offset(), 0);
        assert_retained(&log, 0, next);
    }

    #[test]
    fn gc_prunes_purge_tombstones_below_base() {
        let dir = tempdir().unwrap();
        let mut log = open_gc_log(dir.path(), WAL_FORMAT_V2);
        fill_gc_log(&mut log, 2, 1);
        let base = log.sealed_indexes[0].last_offset;
        assert!(log.purge_offset(0));
        assert!(log.purge_offset(base));
        log.gc_sealed_below(base).unwrap();
        let purged: Vec<u64> = log.meta.purged_offsets.iter().copied().collect();
        assert_eq!(purged, vec![base]);
        drop(log);
        let log = open_gc_log(dir.path(), WAL_FORMAT_V2);
        assert_eq!(log.read_range(0, 0, 1).unwrap()[0].offset, base + 1);
    }

    #[test]
    fn missing_meta_after_gc_recovers_v2_base_and_refuses_v1() {
        for format in [WAL_FORMAT_V1, WAL_FORMAT_V2] {
            let dir = tempdir().unwrap();
            let mut log = open_gc_log(dir.path(), format);
            fill_gc_log(&mut log, 3, 1);
            let base = log.sealed_indexes[0].last_offset;
            let next = log.high_watermark();
            log.gc_sealed_below(base).unwrap();
            drop(log);
            std::fs::remove_file(LogMeta::path(dir.path())).unwrap();

            let reopened = PartitionLog::open_for_shard(dir.path(), gc_cfg(), 0, format);
            if format == WAL_FORMAT_V2 {
                assert_retained(&reopened.unwrap(), base, next);
            } else {
                assert!(reopened.is_err(), "V1 must not renumber after a lost base");
            }
        }
    }

    #[test]
    fn reset_base_discards_a_fully_below_base_log() {
        for format in [WAL_FORMAT_V1, WAL_FORMAT_V2] {
            let dir = tempdir().unwrap();
            let mut log = open_gc_log(dir.path(), format);
            fill_gc_log(&mut log, 1, 2);
            let next = log.high_watermark();
            assert!(matches!(
                log.reset_base(next - 1),
                Err(LogError::OffsetMismatch { .. })
            ));

            log.reset_base(500).unwrap();
            assert_retained(&log, 500, 500);
            assert!(segment_names(dir.path()).is_empty());
            let mut frame = Vec::new();
            encode_frame(&sample_record("gc"), &offset_payload(500), &mut frame).unwrap();
            assert_eq!(
                log.append_raw_frame(0, &frame, Some(500)).unwrap().offset,
                500
            );
            log.sync().unwrap();
            drop(log);
            assert_retained(&open_gc_log(dir.path(), format), 500, 501);
        }
    }

    #[test]
    fn reset_base_on_empty_log_survives_reopen() {
        let dir = tempdir().unwrap();
        let mut log = open_gc_log(dir.path(), WAL_FORMAT_V2);
        log.reset_base(42).unwrap();
        drop(log);
        let log = open_gc_log(dir.path(), WAL_FORMAT_V2);
        assert_retained(&log, 42, 42);
    }
}
