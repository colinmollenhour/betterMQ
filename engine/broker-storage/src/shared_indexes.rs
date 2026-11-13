//! Incremental shared-volume metadata. A stable flock protects an append-only
//! CRC32C journal and generation-swapped snapshots. The shared volume remains
//! a single failure domain; each batch still pays a cell-wide lock and fsync.

use crate::flock::FileLock;
use crate::indexes::{DedupEntry, MetadataOperation};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Mutex, MutexGuard,
};
use std::time::{Duration, Instant};
use thiserror::Error;
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"BMQMETA1";
const HEADER: usize = 20;
const MAX_RECORD: usize = 16 * 1024 * 1024;
const MAX_SNAPSHOT: u64 = 256 * 1024 * 1024;
type DedupKey = (String, String);
type CursorKey = (String, String, u32);
const COMPLETION_BYTES: usize = 32;
/// Extra allowance for a completion marker that records its message id.
const COMPLETION_ID_BYTES: usize = 24;

struct CursorProjection {
    cursor: Option<u64>,
    generation: Option<u64>,
    minimum: u64,
    existing_count: usize,
    completed_present: bool,
    cleared: bool,
    added: BTreeSet<u64>,
}
impl CursorProjection {
    fn size(&self, key: &CursorKey) -> (usize, usize) {
        let completions = self.existing_count + self.added.len();
        (
            usize::from(self.cursor.is_some()) + completions,
            Index::cursor_weight(key)
                * (usize::from(self.cursor.is_some())
                    + usize::from(self.completed_present)
                    + usize::from(self.generation.is_some()))
                + completions * COMPLETION_BYTES,
        )
    }
}

#[derive(Debug, Error)]
pub enum SharedIndexError {
    #[error("shared metadata io: {0}")]
    Io(#[from] std::io::Error),
    #[error("shared metadata encoding: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("corrupt shared metadata; restore its backup or explicitly set BETTERMQ_METADATA_RECOVER=empty")]
    Corrupt,
    #[error("shared metadata lock deadline exceeded; retry after the storage recovers")]
    Timeout,
    #[error("shared metadata has uncertain persistence and is fenced until restart")]
    Poisoned,
    #[error(
        "shared metadata capacity exceeded; increase its configured limit or retention capacity"
    )]
    Capacity,
    #[error("shared metadata configuration: {0}")]
    Configuration(String),
    #[error("dedup identity changed before conditional commit")]
    DedupConflict,
    #[error("shared metadata ownership validation failed: {0}")]
    Validation(String),
}

#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    generation: u64,
    store_id: Uuid,
    snapshot: String,
}

#[derive(Clone, Serialize, Deserialize)]
enum Operation {
    PutDedup(DedupKey, DedupEntry),
    DeleteDedup(DedupKey),
    SetDispatchOffset(CursorKey, u64),
    MarkDispatchComplete(CursorKey, u64),
    ResetDispatchOffset(CursorKey, u64, u64),
    /// Completion of the delivery of a specific message id.
    MarkDispatchCompleteFor(CursorKey, u64, Uuid),
    /// Drop a completion marker that belongs to a different record.
    ClearDispatchComplete(CursorKey, u64),
}

impl TryFrom<MetadataOperation> for Operation {
    type Error = SharedIndexError;
    fn try_from(value: MetadataOperation) -> Result<Self, Self::Error> {
        Ok(match value {
            MetadataOperation::ComparePutDedup { .. } | MetadataOperation::DeleteDedupIf { .. } => {
                return Err(SharedIndexError::Configuration(
                    "conditional dedup operation was not evaluated".into(),
                ))
            }
            MetadataOperation::PutDedup {
                tenant_id,
                key,
                entry,
            } => Self::PutDedup((tenant_id, key), entry),
            MetadataOperation::DeleteDedup { tenant_id, key } => {
                Self::DeleteDedup((tenant_id, key))
            }
            MetadataOperation::SetDispatchOffset {
                tenant_id,
                subscription_id,
                partition,
                next_offset,
            } => Self::SetDispatchOffset((tenant_id, subscription_id, partition), next_offset),
            MetadataOperation::MarkDispatchComplete {
                tenant_id,
                subscription_id,
                partition,
                offset,
                message_id: None,
            } => Self::MarkDispatchComplete((tenant_id, subscription_id, partition), offset),
            MetadataOperation::MarkDispatchComplete {
                tenant_id,
                subscription_id,
                partition,
                offset,
                message_id: Some(id),
            } => Self::MarkDispatchCompleteFor((tenant_id, subscription_id, partition), offset, id),
        })
    }
}

#[derive(Default, Serialize, Deserialize)]
struct Snapshot {
    dedup: Vec<(DedupKey, DedupEntry)>,
    dispatch: Vec<(CursorKey, u64)>,
    completed: Vec<(CursorKey, BTreeSet<u64>)>,
    #[serde(default)]
    generations: Vec<(CursorKey, u64)>,
    /// Message ids of identity-aware completion markers (a subset of `completed`).
    #[serde(default)]
    completed_ids: Vec<(CursorKey, Vec<(u64, Uuid)>)>,
}

#[derive(Default)]
struct Index {
    dedup: std::collections::BTreeMap<DedupKey, DedupEntry>,
    // Enabled only for stripped-header legacy export, not ordinary cursors.
    reverse: Option<HashMap<Uuid, BTreeSet<DedupKey>>>,
    dispatch: HashMap<CursorKey, u64>,
    completed: HashMap<CursorKey, BTreeSet<u64>>,
    completed_ids: HashMap<CursorKey, BTreeMap<u64, Uuid>>,
    generations: HashMap<CursorKey, u64>,
    bytes: usize,
    entry_count: usize,
}

impl Index {
    fn entries(&self) -> usize {
        self.entry_count
    }
    fn weight(key: &DedupKey, entry: &DedupEntry) -> usize {
        // Conservative allowance covers JSON escaping and in-memory overhead.
        (key.0.len() + key.1.len() + entry.topic.len()) * 6 + 256
    }
    fn cursor_weight(key: &CursorKey) -> usize {
        (key.0.len() + key.1.len()) * 6 + 128
    }
    fn apply(&mut self, op: Operation) {
        match op {
            Operation::PutDedup(key, entry) => {
                if let Some(old) = self.dedup.remove(&key) {
                    self.remove_reverse(old.message_id, &key);
                    self.bytes = self.bytes.saturating_sub(Self::weight(&key, &old));
                } else {
                    self.entry_count += 1;
                }
                self.bytes += Self::weight(&key, &entry);
                if let Some(reverse) = &mut self.reverse {
                    reverse
                        .entry(entry.message_id)
                        .or_default()
                        .insert(key.clone());
                }
                self.dedup.insert(key, entry);
            }
            Operation::DeleteDedup(key) => {
                if let Some(old) = self.dedup.remove(&key) {
                    self.remove_reverse(old.message_id, &key);
                    self.bytes = self.bytes.saturating_sub(Self::weight(&key, &old));
                    self.entry_count -= 1;
                }
            }
            Operation::SetDispatchOffset(key, value) => {
                let cursor = self.dispatch.entry(key.clone()).or_insert_with(|| {
                    self.bytes += Self::cursor_weight(&key);
                    self.entry_count += 1;
                    0
                });
                *cursor = (*cursor).max(value);
                let floor = *cursor;
                if let Some(completed) = self.completed.get_mut(&key) {
                    let retained = completed.split_off(&floor);
                    self.bytes = self
                        .bytes
                        .saturating_sub(completed.len() * COMPLETION_BYTES);
                    self.entry_count -= completed.len();
                    *completed = retained;
                }
                self.drop_ids_below(&key, floor);
            }
            Operation::MarkDispatchComplete(key, offset) => {
                if offset >= self.dispatch.get(&key).copied().unwrap_or(0) {
                    let set = self.completed.entry(key.clone()).or_insert_with(|| {
                        self.bytes += Self::cursor_weight(&key);
                        BTreeSet::new()
                    });
                    if set.insert(offset) {
                        self.bytes += COMPLETION_BYTES;
                        self.entry_count += 1;
                    }
                }
            }
            Operation::MarkDispatchCompleteFor(key, offset, id) => {
                if offset >= self.dispatch.get(&key).copied().unwrap_or(0) {
                    self.apply(Operation::MarkDispatchComplete(key.clone(), offset));
                    if self
                        .completed_ids
                        .entry(key)
                        .or_default()
                        .insert(offset, id)
                        .is_none()
                    {
                        self.bytes += COMPLETION_ID_BYTES;
                    }
                }
            }
            Operation::ClearDispatchComplete(key, offset) => {
                if let Some(set) = self.completed.get_mut(&key) {
                    if set.remove(&offset) {
                        self.bytes = self.bytes.saturating_sub(COMPLETION_BYTES);
                        self.entry_count -= 1;
                    }
                }
                if let Some(ids) = self.completed_ids.get_mut(&key) {
                    if ids.remove(&offset).is_some() {
                        self.bytes = self.bytes.saturating_sub(COMPLETION_ID_BYTES);
                    }
                    if ids.is_empty() {
                        self.completed_ids.remove(&key);
                    }
                }
            }
            Operation::ResetDispatchOffset(key, offset, generation) => {
                self.drop_ids_below(&key, u64::MAX);
                if self.dispatch.insert(key.clone(), offset).is_none() {
                    self.entry_count += 1;
                    self.bytes += Self::cursor_weight(&key);
                }
                if let Some(old) = self.completed.remove(&key) {
                    self.bytes = self
                        .bytes
                        .saturating_sub(old.len() * COMPLETION_BYTES + Self::cursor_weight(&key));
                    self.entry_count -= old.len();
                }
                if self.generations.insert(key.clone(), generation).is_none() {
                    self.bytes += Self::cursor_weight(&key);
                }
            }
        }
    }
    fn drop_ids_below(&mut self, key: &CursorKey, floor: u64) {
        if let Some(ids) = self.completed_ids.get_mut(key) {
            let retained = ids.split_off(&floor);
            self.bytes = self.bytes.saturating_sub(ids.len() * COMPLETION_ID_BYTES);
            *ids = retained;
            if ids.is_empty() {
                self.completed_ids.remove(key);
            }
        }
    }

    fn remove_reverse(&mut self, id: Uuid, key: &DedupKey) {
        if let Some(reverse) = &mut self.reverse {
            if let Some(keys) = reverse.get_mut(&id) {
                keys.remove(key);
                if keys.is_empty() {
                    reverse.remove(&id);
                }
            }
        }
    }
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            dedup: self
                .dedup
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            dispatch: self
                .dispatch
                .iter()
                .map(|(key, value)| (key.clone(), *value))
                .collect(),
            completed: self
                .completed
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            generations: self
                .generations
                .iter()
                .map(|(key, value)| (key.clone(), *value))
                .collect(),
            completed_ids: self
                .completed_ids
                .iter()
                .map(|(key, ids)| (key.clone(), ids.iter().map(|(o, id)| (*o, *id)).collect()))
                .collect(),
        }
    }
    fn from_snapshot(snapshot: Snapshot) -> Self {
        let mut index = Self::default();
        for (key, entry) in snapshot.dedup {
            index.apply(Operation::PutDedup(key, entry));
        }
        for (key, value) in snapshot.dispatch {
            index.apply(Operation::SetDispatchOffset(key, value));
        }
        for (key, values) in snapshot.completed {
            index.completed.entry(key.clone()).or_default();
            for value in values {
                index.apply(Operation::MarkDispatchComplete(key.clone(), value));
            }
        }
        for (key, ids) in snapshot.completed_ids {
            for (offset, id) in ids {
                index.apply(Operation::MarkDispatchCompleteFor(key.clone(), offset, id));
            }
        }
        index.generations = snapshot.generations.into_iter().collect();
        index.recount();
        index
    }
    fn expire(&mut self, retention: u64) {
        let now = crate::indexes::now_ms();
        // Expiration bypasses apply(); rebuild only this optional producer
        // view, never on ordinary cursor/completion updates.
        self.reverse = None;
        self.dedup.retain(|_, entry| !entry.expired(now, retention));
        self.recount();
    }

    // Track only this batch's changes. Never clone the existing completion-gap
    // set on each new completion: that would reintroduce quadratic hot paths.
    fn projected(&self, ops: &[Operation]) -> Result<(usize, usize), SharedIndexError> {
        let mut dedup = HashMap::<DedupKey, Option<DedupEntry>>::new();
        let mut cursors = HashMap::<CursorKey, CursorProjection>::new();
        let (mut entries, mut bytes) = (self.entries(), self.bytes);
        for op in ops {
            match op {
                Operation::PutDedup(key, _) | Operation::DeleteDedup(key) => {
                    let value = dedup
                        .entry(key.clone())
                        .or_insert_with(|| self.dedup.get(key).cloned());
                    if let Some(old) = value.take() {
                        entries -= 1;
                        bytes -= Self::weight(key, &old);
                    }
                    if let Operation::PutDedup(_, entry) = op {
                        entries += 1;
                        bytes += Self::weight(key, entry);
                        *value = Some(entry.clone());
                    }
                }
                // Clearing a marker only frees space; ignoring it keeps the
                // projection an upper bound.
                Operation::ClearDispatchComplete(..) => {}
                Operation::SetDispatchOffset(key, _)
                | Operation::MarkDispatchComplete(key, _)
                | Operation::MarkDispatchCompleteFor(key, _, _)
                | Operation::ResetDispatchOffset(key, _, _) => {
                    let state = cursors
                        .entry(key.clone())
                        .or_insert_with(|| CursorProjection {
                            cursor: self.dispatch.get(key).copied(),
                            generation: self.generations.get(key).copied(),
                            minimum: self.dispatch.get(key).copied().unwrap_or(0),
                            existing_count: self.completed.get(key).map_or(0, BTreeSet::len),
                            completed_present: self.completed.contains_key(key),
                            cleared: false,
                            added: BTreeSet::new(),
                        });
                    let before = state.size(key);
                    match op {
                        Operation::SetDispatchOffset(_, offset) => {
                            let next = state.cursor.unwrap_or(0).max(*offset);
                            if !state.cleared && next > state.minimum {
                                state.existing_count -= self
                                    .completed
                                    .get(key)
                                    .map_or(0, |set| set.range(state.minimum..next).count());
                            }
                            state.minimum = next;
                            state.cursor = Some(next);
                            state.added = state.added.split_off(&next);
                        }
                        Operation::MarkDispatchComplete(_, offset)
                        | Operation::MarkDispatchCompleteFor(_, offset, _) => {
                            if *offset >= state.cursor.unwrap_or(0) {
                                if matches!(op, Operation::MarkDispatchCompleteFor(..)) {
                                    bytes += COMPLETION_ID_BYTES;
                                }
                                state.completed_present = true;
                                if state.cleared
                                    || !self
                                        .completed
                                        .get(key)
                                        .is_some_and(|set| set.contains(offset))
                                {
                                    state.added.insert(*offset);
                                }
                            }
                        }
                        Operation::ResetDispatchOffset(_, offset, generation) => {
                            if state.generation.unwrap_or(0).checked_add(1) != Some(*generation) {
                                return Err(SharedIndexError::Corrupt);
                            }
                            state.cursor = Some(*offset);
                            state.generation = Some(*generation);
                            state.minimum = *offset;
                            state.cleared = true;
                            state.existing_count = 0;
                            state.completed_present = false;
                            state.added.clear();
                        }
                        _ => unreachable!(),
                    }
                    let after = state.size(key);
                    entries = entries - before.0 + after.0;
                    bytes = bytes - before.1 + after.1;
                }
            }
        }
        Ok((entries, bytes))
    }

    fn recount(&mut self) {
        self.entry_count = self.dedup.len()
            + self.dispatch.len()
            + self.completed.values().map(BTreeSet::len).sum::<usize>();
        self.bytes = self
            .dedup
            .iter()
            .map(|(key, entry)| Self::weight(key, entry))
            .sum::<usize>()
            + self.dispatch.keys().map(Self::cursor_weight).sum::<usize>()
            + self
                .completed
                .iter()
                .map(|(key, set)| Self::cursor_weight(key) + set.len() * COMPLETION_BYTES)
                .sum::<usize>()
            + self
                .generations
                .keys()
                .map(Self::cursor_weight)
                .sum::<usize>()
            + self
                .completed_ids
                .values()
                .map(|ids| ids.len() * COMPLETION_ID_BYTES)
                .sum::<usize>();
    }

    fn validate(&self, op: &Operation) -> Result<(), SharedIndexError> {
        if let Operation::ResetDispatchOffset(key, _, generation) = op {
            if self
                .generations
                .get(key)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                != Some(*generation)
            {
                return Err(SharedIndexError::Corrupt);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct Cache {
    generation: u64,
    applied: u64,
    index: Index,
}

pub struct SharedMetadataStore {
    dir: PathBuf,
    id: Uuid,
    cache: Mutex<Cache>,
    poisoned: AtomicBool,
    retention: u64,
    timeout: Duration,
    max_entries: usize,
    max_bytes: usize,
    compact_bytes: u64,
    sync_count: AtomicU64,
}

fn number(name: &str, default: u64) -> Result<u64, SharedIndexError> {
    let value = match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|error| SharedIndexError::Configuration(format!("invalid {name}: {error}")))?,
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => {
            return Err(SharedIndexError::Configuration(format!(
                "invalid {name}: {error}"
            )))
        }
    };
    if value == 0 {
        return Err(SharedIndexError::Configuration(format!(
            "{name} must be positive"
        )));
    }
    Ok(value)
}
fn sync_dir(path: &Path) -> Result<(), SharedIndexError> {
    injected_io("sync-directory")?;
    File::open(path)?.sync_all()?;
    Ok(())
}

pub(crate) fn durable_replace(path: &Path, bytes: &[u8]) -> Result<(), SharedIndexError> {
    let parent = path.parent().ok_or(SharedIndexError::Corrupt)?;
    let tmp = parent.join(format!(".metadata-write-{}", Uuid::new_v4()));
    let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)?;
        sync_dir(parent)
    })();
    let _ = std::fs::remove_file(tmp);
    result
}

fn frame(payload: &[u8]) -> Result<Vec<u8>, SharedIndexError> {
    if payload.len() > MAX_RECORD {
        return Err(SharedIndexError::Capacity);
    }
    let mut out = Vec::with_capacity(HEADER + payload.len() + 4);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(&out).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    Ok(out)
}

fn snapshot_bytes(index: &Index) -> Result<Vec<u8>, SharedIndexError> {
    let payload = serde_json::to_vec(&index.snapshot())?;
    if payload.len() as u64 > MAX_SNAPSHOT {
        return Err(SharedIndexError::Capacity);
    }
    let mut out = b"BMQSNAP1".to_vec();
    out.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
    out.extend(payload);
    Ok(out)
}

fn load_snapshot(path: &Path) -> Result<Index, SharedIndexError> {
    if path.metadata()?.len() > MAX_SNAPSHOT + 12 {
        return Err(SharedIndexError::Capacity);
    }
    let bytes = std::fs::read(path)?;
    if bytes.len() < 12
        || &bytes[..8] != b"BMQSNAP1"
        || crc32c::crc32c(&bytes[12..]) != u32::from_le_bytes(bytes[8..12].try_into().unwrap())
    {
        return Err(SharedIndexError::Corrupt);
    }
    Ok(Index::from_snapshot(
        serde_json::from_slice(&bytes[12..]).map_err(|_| SharedIndexError::Corrupt)?,
    ))
}

#[cfg(test)]
fn fault(point: &str) {
    if std::env::var("BETTERMQ_TEST_SHARED_CRASH").as_deref() == Ok(point) {
        std::process::exit(73);
    }
}
#[cfg(not(test))]
fn fault(_: &str) {}

#[cfg(test)]
fn injected_io(point: &str) -> std::io::Result<()> {
    if std::env::var("BETTERMQ_TEST_SHARED_IO_ERROR").as_deref() == Ok(point) {
        return Err(std::io::Error::other("injected durability failure"));
    }
    Ok(())
}
#[cfg(not(test))]
fn injected_io(_: &str) -> std::io::Result<()> {
    Ok(())
}

impl SharedMetadataStore {
    pub(crate) fn try_lock_producers(
        &self,
        tenant: &str,
        keys: &[String],
    ) -> Result<Option<crate::ProducerLocks>, SharedIndexError> {
        self.check_ready()?;
        crate::producer_locks::try_lock(&self.dir, tenant, keys)
    }
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, SharedIndexError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let mut store = Self {
            dir,
            id: Uuid::nil(),
            cache: Mutex::new(Cache::default()),
            poisoned: AtomicBool::new(false),
            retention: crate::indexes::dedup_retention_ms()
                .map_err(|error| SharedIndexError::Configuration(error.to_string()))?,
            timeout: Duration::from_millis(number("BETTERMQ_SHARED_META_LOCK_TIMEOUT_MS", 2000)?),
            max_entries: number("BETTERMQ_SHARED_META_MAX_ENTRIES", 1_000_000)? as usize,
            max_bytes: number("BETTERMQ_SHARED_META_MAX_BYTES", MAX_SNAPSHOT)? as usize,
            compact_bytes: number("BETTERMQ_SHARED_META_COMPACT_BYTES", 32 * 1024 * 1024)?,
            sync_count: AtomicU64::new(0),
        };
        if store.max_bytes as u64 > MAX_SNAPSHOT {
            return Err(SharedIndexError::Configuration(
                "shared metadata max bytes exceeds the 256 MiB snapshot bound".into(),
            ));
        }
        if store.compact_bytes > MAX_SNAPSHOT {
            return Err(SharedIndexError::Configuration(
                "shared metadata compaction threshold exceeds the 256 MiB replay bound".into(),
            ));
        }
        if store.timeout > Duration::from_secs(60) {
            return Err(SharedIndexError::Configuration(
                "shared metadata lock timeout exceeds 60 seconds".into(),
            ));
        }
        if let Some(parent) = store
            .dir
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            sync_dir(parent)?;
        }
        let _lock = store.lock_file()?;
        store.probe_filesystem()?;
        if !store.manifest_path().exists() {
            let index = store.import_legacy()?;
            store.publish_generation(1, Uuid::new_v4(), &index)?;
        }
        if !store.dir.join("indexes.json").is_dir() {
            return Err(SharedIndexError::Configuration(
                "legacy-writer upgrade barrier is missing; stop legacy writers before starting"
                    .into(),
            ));
        }
        let manifest = store.manifest()?;
        store.id = manifest.store_id;
        let mut cache = Cache::default();
        if let Err(error) = store.refresh(&mut cache) {
            if !matches!(error, SharedIndexError::Corrupt)
                || !broker_proto::allow_empty_metadata_recovery()
            {
                return Err(error);
            }
            store.publish_generation(
                manifest
                    .generation
                    .checked_add(1)
                    .ok_or(SharedIndexError::Corrupt)?,
                store.id,
                &Index::default(),
            )?;
            store.refresh(&mut cache)?;
        }
        *store
            .cache
            .get_mut()
            .map_err(|_| SharedIndexError::Poisoned)? = cache;
        Ok(store)
    }
    pub fn identity(&self) -> Uuid {
        self.id
    }
    fn manifest_path(&self) -> PathBuf {
        self.dir.join("meta-manifest.json")
    }
    fn log_path(&self, generation: u64) -> PathBuf {
        self.dir.join(format!("indexes.{generation}.log"))
    }
    fn manifest(&self) -> Result<Manifest, SharedIndexError> {
        let path = self.manifest_path();
        if path.metadata()?.len() > 4096 {
            return Err(SharedIndexError::Corrupt);
        }
        let manifest: Manifest =
            serde_json::from_slice(&std::fs::read(path)?).map_err(|_| SharedIndexError::Corrupt)?;
        if manifest.version != 1
            || manifest.generation == 0
            || manifest.snapshot != format!("indexes.{}.snap", manifest.generation)
            || (self.id != Uuid::nil() && manifest.store_id != self.id)
        {
            return Err(SharedIndexError::Corrupt);
        }
        Ok(manifest)
    }
    fn lock_file(&self) -> Result<FileLock, SharedIndexError> {
        let started = Instant::now();
        let result = FileLock::exclusive_timeout(&self.dir.join("indexes.json"), self.timeout);
        crate::shared_meta_metrics::lock_wait(started.elapsed());
        result.map_err(|error| {
            if error.kind() == std::io::ErrorKind::TimedOut {
                SharedIndexError::Timeout
            } else {
                error.into()
            }
        })
    }
    fn local_cache(&self) -> Result<MutexGuard<'_, Cache>, SharedIndexError> {
        let deadline = Instant::now() + self.timeout;
        loop {
            match self.cache.try_lock() {
                Ok(cache) => return Ok(cache),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(SharedIndexError::Poisoned)
                }
                Err(std::sync::TryLockError::WouldBlock) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(2))
                }
                Err(_) => return Err(SharedIndexError::Timeout),
            }
        }
    }
    fn locked<R>(
        &self,
        operation: impl FnOnce(&mut Cache) -> Result<R, SharedIndexError>,
    ) -> Result<R, SharedIndexError> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(SharedIndexError::Poisoned);
        }
        let result = (|| {
            let mut cache = self.local_cache()?;
            if self.poisoned.load(Ordering::Acquire) {
                return Err(SharedIndexError::Poisoned);
            }
            let result = (|| {
                let _lock = self.lock_file()?;
                self.refresh(&mut cache)?;
                operation(&mut cache)
            })();
            // Fence before releasing the process-local mutex, so a waiting
            // caller cannot use partially refreshed or uncertain state.
            if matches!(
                result,
                Err(SharedIndexError::Corrupt
                    | SharedIndexError::Poisoned
                    | SharedIndexError::Io(_)
                    | SharedIndexError::Serde(_))
            ) {
                self.poisoned.store(true, Ordering::Release);
            }
            result
        })();
        if result.is_err() {
            crate::shared_meta_metrics::error();
        }
        if matches!(
            result,
            Err(SharedIndexError::Corrupt
                | SharedIndexError::Poisoned
                | SharedIndexError::Io(_)
                | SharedIndexError::Serde(_))
        ) {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }
    pub fn check_ready(&self) -> Result<(), SharedIndexError> {
        self.locked(|_| Ok(()))
    }
    fn refresh(&self, cache: &mut Cache) -> Result<(), SharedIndexError> {
        if !self.dir.join("indexes.json").is_dir() {
            return Err(SharedIndexError::Corrupt);
        }
        let manifest = self.manifest()?;
        if manifest.generation < cache.generation {
            return Err(SharedIndexError::Corrupt);
        }
        if manifest.generation != cache.generation {
            cache.index = load_snapshot(&self.dir.join(&manifest.snapshot))?;
            cache.generation = manifest.generation;
            cache.applied = 0;
        }
        if cache.index.entries() > self.max_entries || cache.index.bytes > self.max_bytes {
            return Err(SharedIndexError::Capacity);
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.log_path(cache.generation))?;
        let end = file.metadata()?.len();
        if end < cache.applied {
            return Err(SharedIndexError::Corrupt);
        }
        file.seek(SeekFrom::Start(cache.applied))?;
        while cache.applied < end {
            let start = cache.applied;
            if end - start < HEADER as u64 {
                self.truncate_tail(&file, start)?;
                break;
            }
            let mut header = [0u8; HEADER];
            file.read_exact(&mut header)?;
            if &header[..8] != MAGIC
                || crc32c::crc32c(&header[..12])
                    != u32::from_le_bytes(header[12..16].try_into().unwrap())
            {
                return Err(SharedIndexError::Corrupt);
            }
            let len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
            if len > MAX_RECORD {
                return Err(SharedIndexError::Corrupt);
            }
            let next = start + HEADER as u64 + len as u64 + 4;
            if next > end {
                self.truncate_tail(&file, start)?;
                break;
            }
            let mut payload = vec![0; len];
            file.read_exact(&mut payload)?;
            let mut footer = [0; 4];
            file.read_exact(&mut footer)?;
            if u32::from_le_bytes(footer) as usize != len
                || crc32c::crc32c(&payload)
                    != u32::from_le_bytes(header[16..20].try_into().unwrap())
            {
                return Err(SharedIndexError::Corrupt);
            }
            let ops: Vec<Operation> =
                serde_json::from_slice(&payload).map_err(|_| SharedIndexError::Corrupt)?;
            for op in ops {
                cache.index.validate(&op)?;
                cache.index.apply(op);
            }
            if cache.index.entries() > self.max_entries || cache.index.bytes > self.max_bytes {
                return Err(SharedIndexError::Capacity);
            }
            cache.applied = next;
            crate::shared_meta_metrics::replay(next - start);
        }
        Ok(())
    }
    fn truncate_tail(&self, file: &File, length: u64) -> Result<(), SharedIndexError> {
        file.set_len(length)?;
        file.sync_data()?;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn apply_batch(&self, ops: Vec<MetadataOperation>) -> Result<(), SharedIndexError> {
        self.apply_batch_validated(ops, Vec::new())
    }
    pub(crate) fn apply_batch_validated(
        &self,
        ops: Vec<MetadataOperation>,
        validators: Vec<crate::indexes::MetadataValidator>,
    ) -> Result<(), SharedIndexError> {
        self.locked(|cache| {
            for validate in validators {
                validate().map_err(|error| SharedIndexError::Validation(error.to_string()))?;
            }
            let mut dedup = HashMap::<DedupKey, Option<DedupEntry>>::new();
            let mut converted = Vec::with_capacity(ops.len());
            for op in ops {
                match op {
                    MetadataOperation::DeleteDedupIf {
                        tenant_id,
                        key,
                        expected,
                    } => {
                        let key = (tenant_id, key);
                        let current = dedup
                            .get(&key)
                            .cloned()
                            .unwrap_or_else(|| cache.index.dedup.get(&key).cloned());
                        if current.is_some_and(|entry| entry.same_identity(&expected)) {
                            dedup.insert(key.clone(), None);
                            converted.push(Operation::DeleteDedup(key));
                        }
                    }
                    MetadataOperation::ComparePutDedup {
                        tenant_id,
                        key,
                        mut entry,
                        expected,
                    } => {
                        let key = (tenant_id, key);
                        let current = dedup
                            .get(&key)
                            .cloned()
                            .unwrap_or_else(|| cache.index.dedup.get(&key).cloned())
                            .filter(|entry| {
                                !entry.expired(crate::indexes::now_ms(), self.retention)
                            });
                        if current
                            .as_ref()
                            .is_some_and(|current| current.same_identity(&entry))
                        {
                            continue;
                        }
                        if current.as_ref().map(|entry| entry.message_id) != expected {
                            return Err(SharedIndexError::DedupConflict);
                        }
                        if entry.inserted_at_ms == 0 {
                            entry.inserted_at_ms = crate::indexes::now_ms();
                        }
                        dedup.insert(key.clone(), Some(entry.clone()));
                        converted.push(Operation::PutDedup(key, entry));
                    }
                    MetadataOperation::PutDedup {
                        tenant_id,
                        key,
                        mut entry,
                    } => {
                        if entry.inserted_at_ms == 0 {
                            entry.inserted_at_ms = crate::indexes::now_ms();
                        }
                        let key = (tenant_id, key);
                        dedup.insert(key.clone(), Some(entry.clone()));
                        converted.push(Operation::PutDedup(key, entry));
                    }
                    MetadataOperation::DeleteDedup { tenant_id, key } => {
                        let key = (tenant_id, key);
                        dedup.insert(key.clone(), None);
                        converted.push(Operation::DeleteDedup(key));
                    }
                    op => converted.push(op.try_into()?),
                }
            }
            if converted.is_empty() {
                return Ok(());
            }
            self.commit_locked(cache, converted)
        })
    }
    fn commit(&self, mut ops: Vec<Operation>) -> Result<(), SharedIndexError> {
        if ops.is_empty() {
            return self.check_ready();
        }
        for op in &mut ops {
            if let Operation::PutDedup(_, entry) = op {
                if entry.inserted_at_ms == 0 {
                    entry.inserted_at_ms = crate::indexes::now_ms();
                }
            }
        }
        self.locked(|cache| self.commit_locked(cache, ops))
    }
    fn commit_locked(
        &self,
        cache: &mut Cache,
        ops: Vec<Operation>,
    ) -> Result<(), SharedIndexError> {
        let payload = serde_json::to_vec(&ops)?;
        let encoded = frame(&payload)?;
        let mut projected = cache.index.projected(&ops)?;
        if projected.0 > self.max_entries || projected.1 > self.max_bytes {
            self.compact_locked(cache)?;
            projected = cache.index.projected(&ops)?;
        }
        if projected.0 > self.max_entries || projected.1 > self.max_bytes {
            return Err(SharedIndexError::Capacity);
        }
        let mut file = OpenOptions::new()
            .append(true)
            .open(self.log_path(cache.generation))?;
        fault("append-before-write");
        let started = Instant::now();
        if let Err(error) = file.write_all(&encoded).and_then(|_| {
            fault("append-after-write");
            injected_io("sync-append")?;
            file.sync_data()
        }) {
            self.poisoned.store(true, Ordering::Release);
            return Err(error.into());
        }
        self.sync_count.fetch_add(1, Ordering::Relaxed);
        crate::shared_meta_metrics::fsync(started.elapsed());
        fault("append-after-sync");
        for op in ops {
            cache.index.apply(op);
        }
        cache.applied += encoded.len() as u64;
        fault("append-after-publish");
        let live = (cache.index.bytes as u64).max(64 * 1024);
        if cache.applied >= self.compact_bytes || cache.applied > live.saturating_mul(4) {
            self.compact_locked(cache)?;
        }
        Ok(())
    }
    fn publish_generation(
        &self,
        generation: u64,
        id: Uuid,
        index: &Index,
    ) -> Result<(), SharedIndexError> {
        let snapshot = format!("indexes.{generation}.snap");
        durable_replace(&self.dir.join(&snapshot), &snapshot_bytes(index)?)?;
        fault("snapshot-synced");
        durable_replace(&self.log_path(generation), &[])?;
        fault("new-log-synced");
        let manifest = Manifest {
            version: 1,
            generation,
            store_id: id,
            snapshot,
        };
        durable_replace(&self.manifest_path(), &serde_json::to_vec(&manifest)?)?;
        fault("generation-swapped");
        Ok(())
    }
    fn compact_locked(&self, cache: &mut Cache) -> Result<(), SharedIndexError> {
        let previous = cache.index.dedup.len();
        cache.index.expire(self.retention);
        let old = cache.generation;
        let next = old.checked_add(1).ok_or(SharedIndexError::Corrupt)?;
        self.publish_generation(next, self.id, &cache.index)?;
        cache.generation = next;
        cache.applied = 0;
        crate::shared_meta_metrics::compaction();
        crate::shared_meta_metrics::expired((previous - cache.index.dedup.len()) as u64);
        for path in [
            self.log_path(old),
            self.dir.join(format!("indexes.{old}.snap")),
        ] {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        sync_dir(&self.dir)?;
        fault("old-generation-deleted");
        Ok(())
    }
    fn probe_filesystem(&self) -> Result<(), SharedIndexError> {
        let second = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.dir.join("indexes.json.lock"))?;
        match second.try_lock_exclusive() {
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Ok(()) => {
                FileExt::unlock(&second)?;
                return Err(SharedIndexError::Configuration(
                    "filesystem does not enforce exclusive flock".into(),
                ));
            }
            Err(error) => return Err(error.into()),
        }
        let path = self.dir.join(format!(".metadata-probe-{}", Uuid::new_v4()));
        let result = (|| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)?;
            file.write_all(b"durable shared metadata")?;
            file.sync_all()?;
            if std::fs::read(&path)? != b"durable shared metadata" {
                return Err(SharedIndexError::Corrupt);
            }
            sync_dir(&self.dir)
        })();
        let _ = std::fs::remove_file(path);
        result
    }
    fn import_legacy(&self) -> Result<Index, SharedIndexError> {
        let legacy = self.dir.join("indexes.json");
        let backup = self.dir.join("indexes.json.migrated");
        let swap = self.dir.join(".indexes-migration.swap");
        let source = if legacy.is_file() {
            Some(legacy.clone())
        } else if backup.is_file() {
            Some(backup.clone())
        } else if swap.is_file() {
            Some(swap.clone())
        } else {
            None
        };
        let index = if let Some(path) = source {
            if path.metadata()?.len() > MAX_SNAPSHOT {
                return Err(SharedIndexError::Capacity);
            }
            match serde_json::from_slice::<Legacy>(&std::fs::read(&path)?) {
                Ok(value) => value.into_index()?,
                Err(_) if broker_proto::allow_empty_metadata_recovery() => Index::default(),
                Err(_) => return Err(SharedIndexError::Corrupt),
            }
        } else {
            Index::default()
        };
        if legacy.is_file() {
            if !swap.exists() {
                std::fs::create_dir(&swap)?;
            }
            exchange_legacy(&legacy, &swap)?;
            sync_dir(&self.dir)?;
        }
        if swap.is_file() {
            if backup.exists() {
                return Err(SharedIndexError::Corrupt);
            }
            std::fs::rename(&swap, &backup)?;
            sync_dir(&self.dir)?;
        }
        if !legacy.exists() {
            std::fs::create_dir(&legacy)?;
            sync_dir(&self.dir)?;
        }
        if !legacy.is_dir() {
            return Err(SharedIndexError::Corrupt);
        }
        fault("legacy-blocked");
        Ok(index)
    }
    pub fn get_dedup(
        &self,
        tenant: &str,
        key: &str,
    ) -> Result<Option<DedupEntry>, SharedIndexError> {
        self.locked(|cache| {
            Ok(cache
                .index
                .dedup
                .get(&(tenant.into(), key.into()))
                .filter(|entry| !entry.expired(crate::indexes::now_ms(), self.retention))
                .cloned())
        })
    }

    /// A different process may have observed an uncertain append before its
    /// fdatasync failed. A committed-directory hit must establish its own
    /// durability barrier before acknowledging that row, not merely read it.
    pub(crate) fn confirmed_dedup(
        &self,
        tenant: &str,
        key: &str,
    ) -> Result<Option<DedupEntry>, SharedIndexError> {
        self.locked(|cache| {
            let entry = cache
                .index
                .dedup
                .get(&(tenant.into(), key.into()))
                .filter(|entry| !entry.expired(crate::indexes::now_ms(), self.retention))
                .cloned();
            if entry.is_some() {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(self.log_path(cache.generation))?
                    .sync_data()?;
                sync_dir(&self.dir)?;
            }
            Ok(entry)
        })
    }

    pub fn dedup_page(
        &self,
        after: Option<(String, String)>,
    ) -> Result<crate::indexes::DedupPage, SharedIndexError> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.locked(|cache| {
            let mut records = Vec::new();
            let mut count = 0usize;
            let mut bytes = 0usize;
            for ((tenant, key), entry) in cache
                .index
                .dedup
                .range((after.map(Excluded).unwrap_or(Unbounded), Unbounded))
            {
                let next = Some((tenant.clone(), key.clone()));
                count += 1;
                bytes = bytes
                    .saturating_add(tenant.len())
                    .saturating_add(key.len())
                    .saturating_add(entry.topic.len())
                    .saturating_add(128);
                if !entry.expired(crate::indexes::now_ms(), self.retention) {
                    records.push(crate::indexes::DedupRecord {
                        tenant_id: tenant.clone(),
                        key: key.clone(),
                        entry: entry.clone(),
                    });
                }
                if count >= 256 || bytes >= 4 * 1024 * 1024 {
                    return Ok(crate::indexes::DedupPage { records, next });
                }
            }
            Ok(crate::indexes::DedupPage {
                records,
                next: None,
            })
        })
    }
    pub fn put_dedup(
        &self,
        tenant: &str,
        key: &str,
        entry: &DedupEntry,
    ) -> Result<(), SharedIndexError> {
        self.commit(vec![Operation::PutDedup(
            (tenant.into(), key.into()),
            entry.clone(),
        )])
    }

    pub fn dedup_for_message(
        &self,
        id: Uuid,
    ) -> Result<Vec<crate::indexes::DedupRecord>, SharedIndexError> {
        self.locked(|cache| {
            if cache.index.reverse.is_none() {
                let mut reverse = HashMap::<Uuid, BTreeSet<DedupKey>>::new();
                for (key, entry) in &cache.index.dedup {
                    reverse
                        .entry(entry.message_id)
                        .or_default()
                        .insert(key.clone());
                }
                cache.index.reverse = Some(reverse);
            }
            let mut records = Vec::new();
            if let Some(keys) = cache
                .index
                .reverse
                .as_ref()
                .and_then(|reverse| reverse.get(&id))
            {
                for key in keys {
                    if let Some(entry) = cache.index.dedup.get(key).filter(|entry| {
                        entry.message_id == id
                            && !entry.expired(crate::indexes::now_ms(), self.retention)
                    }) {
                        records.push(crate::indexes::DedupRecord {
                            tenant_id: key.0.clone(),
                            key: key.1.clone(),
                            entry: entry.clone(),
                        });
                    }
                }
            }
            Ok(records)
        })
    }
    pub fn delete_dedup(&self, tenant: &str, key: &str) -> Result<(), SharedIndexError> {
        self.commit(vec![Operation::DeleteDedup((tenant.into(), key.into()))])
    }
    pub fn dispatch_offset(
        &self,
        tenant: &str,
        sub: &str,
        partition: u32,
    ) -> Result<u64, SharedIndexError> {
        self.locked(|cache| {
            Ok(cache
                .index
                .dispatch
                .get(&(tenant.into(), sub.into(), partition))
                .copied()
                .unwrap_or(0))
        })
    }
    pub fn set_dispatch_offset(
        &self,
        tenant: &str,
        sub: &str,
        partition: u32,
        value: u64,
    ) -> Result<(), SharedIndexError> {
        self.commit(vec![Operation::SetDispatchOffset(
            (tenant.into(), sub.into(), partition),
            value,
        )])
    }
    /// Explicit rewind only while the shard is fenced. Normal advancement never regresses.
    pub fn reset_dispatch_offset(
        &self,
        tenant: &str,
        sub: &str,
        partition: u32,
        value: u64,
    ) -> Result<(), SharedIndexError> {
        let key = (tenant.into(), sub.into(), partition);
        self.locked(|cache| {
            let generation = cache
                .index
                .generations
                .get(&key)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(SharedIndexError::Corrupt)?;
            self.commit_locked(
                cache,
                vec![Operation::ResetDispatchOffset(key, value, generation)],
            )
        })
    }
    pub fn mark_dispatch_complete(
        &self,
        tenant: &str,
        sub: &str,
        partition: u32,
        value: u64,
    ) -> Result<(), SharedIndexError> {
        self.commit(vec![Operation::MarkDispatchComplete(
            (tenant.into(), sub.into(), partition),
            value,
        )])
    }
    pub fn clear_dispatch_complete(
        &self,
        tenant: &str,
        sub: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), SharedIndexError> {
        self.commit(vec![Operation::ClearDispatchComplete(
            (tenant.into(), sub.into(), partition),
            offset,
        )])
    }
    /// Clear only an exact stale identity under the shared writer lock.
    pub fn clear_dispatch_complete_for(
        &self,
        tenant: &str,
        sub: &str,
        partition: u32,
        offset: u64,
        stale_id: Uuid,
    ) -> Result<bool, SharedIndexError> {
        let key = (tenant.into(), sub.into(), partition);
        self.locked(|cache| {
            if cache
                .index
                .completed_ids
                .get(&key)
                .and_then(|ids| ids.get(&offset))
                != Some(&stale_id)
            {
                return Ok(false);
            }
            self.commit_locked(cache, vec![Operation::ClearDispatchComplete(key, offset)])?;
            Ok(true)
        })
    }

    /// The completion marker at `offset` (ignores the lane cursor).
    pub fn dispatch_completion(
        &self,
        tenant: &str,
        sub: &str,
        partition: u32,
        offset: u64,
    ) -> Result<crate::indexes::DispatchCompletion, SharedIndexError> {
        use crate::indexes::DispatchCompletion;
        let key = (tenant.into(), sub.into(), partition);
        self.locked(|cache| {
            if let Some(id) = cache
                .index
                .completed_ids
                .get(&key)
                .and_then(|ids| ids.get(&offset))
            {
                return Ok(DispatchCompletion::Message(*id));
            }
            Ok(
                if cache
                    .index
                    .completed
                    .get(&key)
                    .is_some_and(|set| set.contains(&offset))
                {
                    DispatchCompletion::Legacy
                } else {
                    DispatchCompletion::Missing
                },
            )
        })
    }
    /// Every lane cursor as `(tenant, lane, partition, next_offset)`.
    pub fn dispatch_offsets(&self) -> Result<Vec<(String, String, u32, u64)>, SharedIndexError> {
        self.locked(|cache| {
            Ok(cache
                .index
                .dispatch
                .iter()
                .map(|((tenant, lane, partition), offset)| {
                    (tenant.clone(), lane.clone(), *partition, *offset)
                })
                .collect())
        })
    }
    pub fn is_dispatch_complete(
        &self,
        tenant: &str,
        sub: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, SharedIndexError> {
        let key = (tenant.into(), sub.into(), partition);
        self.locked(|cache| {
            Ok(
                offset < cache.index.dispatch.get(&key).copied().unwrap_or(0)
                    || cache
                        .index
                        .completed
                        .get(&key)
                        .is_some_and(|set| set.contains(&offset)),
            )
        })
    }
}

#[derive(Deserialize)]
struct Legacy {
    #[serde(default)]
    dedup: HashMap<String, DedupEntry>,
    #[serde(default)]
    dispatch: HashMap<String, u64>,
    #[serde(default)]
    completed: HashMap<String, Vec<u64>>,
}
impl Legacy {
    fn into_index(self) -> Result<Index, SharedIndexError> {
        let mut index = Index::default();
        let now = crate::indexes::now_ms();
        for (key, mut value) in self.dedup {
            let (tenant, key) = key.split_once(':').ok_or(SharedIndexError::Corrupt)?;
            value.inserted_at_ms = now;
            index.apply(Operation::PutDedup((tenant.into(), key.into()), value));
        }
        for (key, value) in self.dispatch {
            index.apply(Operation::SetDispatchOffset(legacy_cursor(&key)?, value));
        }
        for (key, values) in self.completed {
            let key = legacy_cursor(&key)?;
            for value in values {
                index.apply(Operation::MarkDispatchComplete(key.clone(), value));
            }
        }
        Ok(index)
    }
}
fn legacy_cursor(key: &str) -> Result<CursorKey, SharedIndexError> {
    let (prefix, partition) = key.rsplit_once(":p").ok_or(SharedIndexError::Corrupt)?;
    let (tenant, sub) = prefix.split_once(':').ok_or(SharedIndexError::Corrupt)?;
    Ok((
        tenant.into(),
        sub.into(),
        partition.parse().map_err(|_| SharedIndexError::Corrupt)?,
    ))
}
#[cfg(target_os = "linux")]
fn exchange_legacy(legacy: &Path, swap: &Path) -> Result<(), SharedIndexError> {
    use std::os::unix::ffi::OsStrExt;
    let first = std::ffi::CString::new(legacy.as_os_str().as_bytes())
        .map_err(|_| SharedIndexError::Corrupt)?;
    let second = std::ffi::CString::new(swap.as_os_str().as_bytes())
        .map_err(|_| SharedIndexError::Corrupt)?;
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            first.as_ptr(),
            libc::AT_FDCWD,
            second.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(SharedIndexError::Configuration(format!(
            "legacy import needs atomic rename exchange and a coordinated restart: {}",
            std::io::Error::last_os_error()
        )))
    }
}
#[cfg(not(target_os = "linux"))]
fn exchange_legacy(_: &Path, _: &Path) -> Result<(), SharedIndexError> {
    Err(SharedIndexError::Configuration(
        "legacy migration requires a coordinated Linux upgrade".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn entry(offset: u64) -> DedupEntry {
        DedupEntry::new(Uuid::from_u128(42), offset, 0, "jobs".into())
    }
    fn compact(store: &SharedMetadataStore) {
        store.locked(|cache| store.compact_locked(cache)).unwrap();
    }
    fn child(dir: &Path, action: &str, point: &str) -> std::process::Command {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "shared_indexes::tests::process_worker"])
            .env("BETTERMQ_TEST_SHARED_DIR", dir)
            .env("BETTERMQ_TEST_SHARED_ACTION", action)
            .env("BETTERMQ_TEST_SHARED_CRASH", point);
        command
    }

    #[test]
    fn process_worker() {
        let Some(dir) = std::env::var_os("BETTERMQ_TEST_SHARED_DIR") else {
            return;
        };
        if std::env::var("BETTERMQ_TEST_SHARED_ACTION").as_deref() == Ok("expect-capacity") {
            assert!(matches!(
                SharedMetadataStore::open(dir),
                Err(SharedIndexError::Capacity)
            ));
            return;
        }
        let store = SharedMetadataStore::open(dir).unwrap();
        match std::env::var("BETTERMQ_TEST_SHARED_ACTION")
            .unwrap()
            .as_str()
        {
            "append" => store.put_dedup("t", "crash", &entry(9)).unwrap(),
            "compact" => compact(&store),
            "import" => {}
            "io-error" | "directory-error" => {
                let directory =
                    std::env::var("BETTERMQ_TEST_SHARED_ACTION").unwrap() == "directory-error";
                std::env::set_var(
                    "BETTERMQ_TEST_SHARED_IO_ERROR",
                    if directory {
                        "sync-directory"
                    } else {
                        "sync-append"
                    },
                );
                let result = if directory {
                    store.locked(|cache| store.compact_locked(cache))
                } else {
                    store.put_dedup("t", "uncertain", &entry(8))
                };
                assert!(matches!(result, Err(SharedIndexError::Io(_))));
                assert!(matches!(
                    store.check_ready(),
                    Err(SharedIndexError::Poisoned)
                ));
                std::env::remove_var("BETTERMQ_TEST_SHARED_IO_ERROR");
            }
            id => {
                for i in 0..50 {
                    store
                        .put_dedup("t", &format!("{id}-{i}"), &entry(i))
                        .unwrap();
                    store.set_dispatch_offset("t", "s", 0, i).unwrap();
                }
            }
        }
    }

    #[test]
    fn instances_tail_only_new_bytes_and_reload_generations() {
        let dir = tempdir().unwrap();
        let first = SharedMetadataStore::open(dir.path()).unwrap();
        let second = SharedMetadataStore::open(dir.path()).unwrap();
        first.put_dedup("t", "a", &entry(1)).unwrap();
        assert_eq!(second.get_dedup("t", "a").unwrap().unwrap().offset, 1);
        let applied = second.cache.lock().unwrap().applied;
        second.get_dedup("t", "a").unwrap();
        assert_eq!(second.cache.lock().unwrap().applied, applied);
        second.put_dedup("t", "b", &entry(2)).unwrap();
        assert_eq!(first.get_dedup("t", "b").unwrap().unwrap().offset, 2);
        compact(&second);
        assert!(first.get_dedup("t", "a").unwrap().is_some());
        assert_eq!(first.cache.lock().unwrap().generation, 2);
        assert_eq!(first.cache.lock().unwrap().applied, 0);
        first.put_dedup("t", "c", &entry(3)).unwrap();
        assert!(second.get_dedup("t", "c").unwrap().is_some());
    }

    #[test]
    fn concurrent_processes_preserve_all_writes() {
        let dir = tempdir().unwrap();
        let store = SharedMetadataStore::open(dir.path()).unwrap();
        let mut first = child(dir.path(), "first", "").spawn().unwrap();
        let mut second = child(dir.path(), "second", "").spawn().unwrap();
        assert!(first.wait().unwrap().success());
        assert!(second.wait().unwrap().success());
        for id in ["first", "second"] {
            for i in 0..50 {
                assert_eq!(
                    store
                        .get_dedup("t", &format!("{id}-{i}"))
                        .unwrap()
                        .unwrap()
                        .offset,
                    i
                );
            }
        }
        assert_eq!(store.dispatch_offset("t", "s", 0).unwrap(), 49);
    }

    #[test]
    fn batch_has_one_sync_and_exact_capacity_with_monotonic_cursors() {
        let dir = tempdir().unwrap();
        let mut store = SharedMetadataStore::open(dir.path()).unwrap();
        store.max_entries = 2;
        let key = ("t".into(), "s".into(), 0);
        store
            .commit(vec![
                Operation::MarkDispatchComplete(key.clone(), 2),
                Operation::SetDispatchOffset(key.clone(), 5),
                Operation::SetDispatchOffset(key.clone(), 3),
                Operation::MarkDispatchComplete(key.clone(), 1),
                Operation::MarkDispatchComplete(key.clone(), 8),
                Operation::MarkDispatchComplete(key.clone(), 8),
            ])
            .unwrap();
        assert_eq!(store.sync_count.load(Ordering::Relaxed), 1);
        assert_eq!(store.dispatch_offset("t", "s", 0).unwrap(), 5);
        assert_eq!(store.cache.lock().unwrap().index.entries(), 2);
        assert!(matches!(
            store.put_dedup("t", "overflow", &entry(0)),
            Err(SharedIndexError::Capacity)
        ));
        assert!(store.check_ready().is_ok());
        store.reset_dispatch_offset("t", "s", 0, 1).unwrap();
        assert!(!store.is_dispatch_complete("t", "s", 0, 8).unwrap());
        store.set_dispatch_offset("t", "s", 0, 0).unwrap();
        assert_eq!(store.dispatch_offset("t", "s", 0).unwrap(), 1);
        drop(store);
        let store = SharedMetadataStore::open(dir.path()).unwrap();
        assert_eq!(store.cache.lock().unwrap().index.generations[&key], 1);
    }

    #[test]
    fn expiry_survives_restart_and_compaction_drops_history() {
        let dir = tempdir().unwrap();
        let store = SharedMetadataStore::open(dir.path()).unwrap();
        let mut expired = entry(1);
        expired.inserted_at_ms = crate::indexes::now_ms() - store.retention - 1;
        store.put_dedup("t", "old", &expired).unwrap();
        store.put_dedup("t", "live", &entry(2)).unwrap();
        assert!(store.get_dedup("t", "old").unwrap().is_none());
        drop(store);
        let store = SharedMetadataStore::open(dir.path()).unwrap();
        assert!(store.get_dedup("t", "old").unwrap().is_none());
        compact(&store);
        assert_eq!(store.cache.lock().unwrap().index.dedup.len(), 1);
        assert!(store.get_dedup("t", "live").unwrap().is_some());
    }

    #[test]
    fn legacy_import_barrier_and_interrupted_migration_preserve_original() {
        for point in [
            "",
            "legacy-blocked",
            "snapshot-synced",
            "new-log-synced",
            "generation-swapped",
        ] {
            let dir = tempdir().unwrap();
            let json = serde_json::json!({"dedup": {"t:old:key": {"message_id": Uuid::from_u128(42), "offset": 9, "partition": 0}}, "dispatch": {"t:s:p0": 5}, "completed": {"t:s:p0": [3, 8]}});
            let original = serde_json::to_vec(&json).unwrap();
            std::fs::write(dir.path().join("indexes.json"), &original).unwrap();
            let result = child(dir.path(), "import", point).output().unwrap();
            assert_eq!(
                result.status.code(),
                Some(if point.is_empty() { 0 } else { 73 }),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            let store = SharedMetadataStore::open(dir.path()).unwrap();
            let old = store.get_dedup("t", "old:key").unwrap().unwrap();
            assert_eq!(old.offset, 9);
            assert!(old.inserted_at_ms > 0);
            assert!(old.topic.is_empty());
            assert_eq!(store.dispatch_offset("t", "s", 0).unwrap(), 5);
            assert!(store.is_dispatch_complete("t", "s", 0, 8).unwrap());
            assert_eq!(
                std::fs::read(dir.path().join("indexes.json.migrated")).unwrap(),
                original
            );
            assert!(std::fs::read(dir.path().join("indexes.json")).is_err());
            // A legacy writer's atomic rename cannot replace the directory.
            let tmp = dir.path().join("legacy-write");
            std::fs::write(&tmp, b"{}").unwrap();
            assert!(std::fs::rename(tmp, dir.path().join("indexes.json")).is_err());
        }
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("indexes.json"), b"bad json").unwrap();
        assert!(matches!(
            SharedMetadataStore::open(dir.path()),
            Err(SharedIndexError::Corrupt)
        ));
        assert!(!dir.path().join("meta-manifest.json").exists());
    }

    #[test]
    fn crash_each_append_and_compaction_stage_preserves_durable_batches() {
        for point in [
            "append-before-write",
            "append-after-write",
            "append-after-sync",
            "append-after-publish",
            "snapshot-synced",
            "new-log-synced",
            "generation-swapped",
            "old-generation-deleted",
        ] {
            let dir = tempdir().unwrap();
            let store = SharedMetadataStore::open(dir.path()).unwrap();
            store.put_dedup("t", "baseline", &entry(1)).unwrap();
            let action = if point.starts_with("append") {
                "append"
            } else {
                "compact"
            };
            let result = child(dir.path(), action, point).output().unwrap();
            assert_eq!(
                result.status.code(),
                Some(73),
                "{point}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            let reopened = SharedMetadataStore::open(dir.path()).unwrap();
            assert_eq!(
                reopened.get_dedup("t", "baseline").unwrap().unwrap().offset,
                1
            );
            if matches!(point, "append-after-sync" | "append-after-publish") {
                assert_eq!(
                    reopened
                        .get_dedup("t", "crash")
                        .unwrap()
                        .unwrap()
                        .message_id,
                    Uuid::from_u128(42)
                );
            }
            if point == "append-before-write" {
                assert!(reopened.get_dedup("t", "crash").unwrap().is_none());
            }
            compact(&reopened);
            assert!(store.get_dedup("t", "baseline").unwrap().is_some());
        }
    }

    #[test]
    fn torn_eof_is_truncated_but_complete_crc_corruption_is_fatal() {
        let encoded = frame(
            &serde_json::to_vec(&vec![Operation::PutDedup(
                ("t".into(), "partial".into()),
                entry(2),
            )])
            .unwrap(),
        )
        .unwrap();
        for cut in [1, HEADER - 1, HEADER, encoded.len() - 1] {
            let dir = tempdir().unwrap();
            let store = SharedMetadataStore::open(dir.path()).unwrap();
            store.put_dedup("t", "baseline", &entry(1)).unwrap();
            let log = store.log_path(1);
            let original = log.metadata().unwrap().len();
            OpenOptions::new()
                .append(true)
                .open(&log)
                .unwrap()
                .write_all(&encoded[..cut])
                .unwrap();
            let reopened = SharedMetadataStore::open(dir.path()).unwrap();
            assert_eq!(log.metadata().unwrap().len(), original);
            assert!(reopened.get_dedup("t", "partial").unwrap().is_none());
        }
        for mid_file in [false, true] {
            let dir = tempdir().unwrap();
            let store = SharedMetadataStore::open(dir.path()).unwrap();
            store.put_dedup("t", "baseline", &entry(1)).unwrap();
            let log = store.log_path(1);
            let mut bytes = std::fs::read(&log).unwrap();
            bytes[HEADER + 5] ^= 1;
            if mid_file {
                bytes.extend_from_slice(&encoded);
            }
            std::fs::write(log, bytes).unwrap();
            assert!(matches!(
                SharedMetadataStore::open(dir.path()),
                Err(SharedIndexError::Corrupt)
            ));
        }
    }

    #[test]
    fn bounded_lock_wait_and_storage_failure_fail_readiness_closed() {
        let dir = tempdir().unwrap();
        let mut store = SharedMetadataStore::open(dir.path()).unwrap();
        store.timeout = Duration::from_millis(30);
        let lock = store.lock_file().unwrap();
        let start = Instant::now();
        assert!(matches!(
            store.check_ready(),
            Err(SharedIndexError::Timeout)
        ));
        assert!(start.elapsed() < Duration::from_secs(1));
        drop(lock);
        assert!(store.check_ready().is_ok());
        std::fs::remove_file(store.log_path(1)).unwrap();
        assert!(matches!(store.check_ready(), Err(SharedIndexError::Io(_))));
        assert!(matches!(
            store.check_ready(),
            Err(SharedIndexError::Poisoned)
        ));
    }

    #[test]
    fn failed_sync_never_reports_success_and_fences_until_restart() {
        for action in ["io-error", "directory-error"] {
            let dir = tempdir().unwrap();
            let store = SharedMetadataStore::open(dir.path()).unwrap();
            store.put_dedup("t", "baseline", &entry(1)).unwrap();
            let result = child(dir.path(), action, "").output().unwrap();
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            let recovered = SharedMetadataStore::open(dir.path()).unwrap();
            assert!(recovered.get_dedup("t", "baseline").unwrap().is_some());
        }
    }

    #[test]
    fn projected_batch_accounting_matches_applied_state_without_history_clones() {
        let mut index = Index::default();
        let mut random = 7u64;
        for _ in 0..200 {
            let mut expected = Index::from_snapshot(index.snapshot());
            let mut ops = Vec::new();
            for _ in 0..20 {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                let cursor = ("t".into(), "s".into(), ((random >> 16) % 3) as u32);
                let key = ("t".into(), ((random >> 8) % 5).to_string());
                let offset = (random >> 32) % 50;
                let op = match random % 5 {
                    0 => Operation::PutDedup(key, entry(offset)),
                    1 => Operation::DeleteDedup(key),
                    2 => Operation::SetDispatchOffset(cursor, offset),
                    3 => Operation::MarkDispatchComplete(cursor, offset),
                    _ => {
                        let generation =
                            expected.generations.get(&cursor).copied().unwrap_or(0) + 1;
                        Operation::ResetDispatchOffset(cursor, offset, generation)
                    }
                };
                expected.validate(&op).unwrap();
                expected.apply(op.clone());
                ops.push(op);
            }
            assert_eq!(
                index.projected(&ops).unwrap(),
                (expected.entries(), expected.bytes)
            );
            let counted = Index::from_snapshot(expected.snapshot());
            assert_eq!(
                (expected.entries(), expected.bytes),
                (counted.entries(), counted.bytes)
            );
            index = expected;
        }
    }

    #[test]
    fn startup_enforces_capacity_for_snapshots_not_only_replayed_appends() {
        let dir = tempdir().unwrap();
        let store = SharedMetadataStore::open(dir.path()).unwrap();
        store.put_dedup("t", "a", &entry(1)).unwrap();
        store.put_dedup("t", "b", &entry(2)).unwrap();
        compact(&store);
        let result = child(dir.path(), "expect-capacity", "")
            .env("BETTERMQ_SHARED_META_MAX_ENTRIES", "1")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    #[test]
    #[ignore = "local shared-volume metadata microbenchmark; not an RF=3 qualification"]
    fn metadata_microbenchmark() {
        let dir = match std::env::var_os("BETTERMQ_TEST_BENCH_DIR") {
            Some(path) => tempfile::tempdir_in(path).unwrap(),
            None => tempdir().unwrap(),
        };
        let store = SharedMetadataStore::open(dir.path()).unwrap();
        for size in [1, 64] {
            let start = Instant::now();
            let mut latencies = Vec::new();
            for batch in 0..500 {
                let ops = (0..size)
                    .map(|index| {
                        Operation::PutDedup(
                            ("bench".into(), format!("{size}-{batch}-{index}")),
                            entry(index),
                        )
                    })
                    .collect();
                let before = Instant::now();
                store.commit(ops).unwrap();
                latencies.push(before.elapsed().as_micros());
            }
            let elapsed = start.elapsed();
            latencies.sort_unstable();
            eprintln!("shared metadata: batch={size}, records={}, records/s={:.0}, batch_p99_us={}, elapsed_ms={}, filesystem={}", size * 500, (size * 500) as f64 / elapsed.as_secs_f64(), latencies[494], elapsed.as_millis(), dir.path().display());
        }
    }

    #[test]
    fn cached_reads_observe_writes_from_another_process_handle() {
        let dir = tempfile::tempdir().unwrap();
        let a = SharedMetadataStore::open(dir.path()).unwrap();
        let b = SharedMetadataStore::open(dir.path()).unwrap();
        a.set_dispatch_offset("tenant", "lane", 0, 5).unwrap();
        assert_eq!(b.dispatch_offset("tenant", "lane", 0).unwrap(), 5);
        assert_eq!(b.dispatch_offset("tenant", "lane", 0).unwrap(), 5);
        // `a` serves from its cache; a write through `b` must invalidate it.
        b.set_dispatch_offset("tenant", "lane", 0, 9).unwrap();
        assert_eq!(a.dispatch_offset("tenant", "lane", 0).unwrap(), 9);
        a.put_dedup(
            "tenant",
            "key",
            &DedupEntry::new(uuid::Uuid::nil(), 1, 0, String::new()),
        )
        .unwrap();
        assert_eq!(b.get_dedup("tenant", "key").unwrap().unwrap().offset, 1);
        assert_eq!(b.dispatch_offset("tenant", "lane", 0).unwrap(), 9);
    }

    #[test]
    fn shared_completion_markers_carry_identity_and_survive_reload() {
        use crate::indexes::DispatchCompletion;
        let dir = tempfile::tempdir().unwrap();
        let store = SharedMetadataStore::open(dir.path()).unwrap();
        let id = uuid::Uuid::new_v4();
        store
            .apply_batch(vec![MetadataOperation::MarkDispatchComplete {
                tenant_id: "tenant".into(),
                subscription_id: "lane".into(),
                partition: 0,
                offset: 4,
                message_id: Some(id),
            }])
            .unwrap();
        store
            .mark_dispatch_complete("tenant", "lane", 0, 5)
            .unwrap();
        let reopened = SharedMetadataStore::open(dir.path()).unwrap();
        assert_eq!(
            reopened
                .dispatch_completion("tenant", "lane", 0, 4)
                .unwrap(),
            DispatchCompletion::Message(id)
        );
        assert_eq!(
            reopened
                .dispatch_completion("tenant", "lane", 0, 5)
                .unwrap(),
            DispatchCompletion::Legacy
        );
        reopened
            .set_dispatch_offset("tenant", "lane", 0, 6)
            .unwrap();
        assert_eq!(
            reopened
                .dispatch_completion("tenant", "lane", 0, 4)
                .unwrap(),
            DispatchCompletion::Missing
        );
        reopened
            .reset_dispatch_offset("tenant", "lane", 0, 2)
            .unwrap();
        assert_eq!(reopened.dispatch_offset("tenant", "lane", 0).unwrap(), 2);
        assert_eq!(
            reopened.dispatch_offsets().unwrap(),
            vec![("tenant".to_string(), "lane".to_string(), 0, 2)]
        );
    }

    #[test]
    fn shared_cursor_never_moves_backwards() {
        let dir = tempfile::tempdir().unwrap();
        let store = SharedMetadataStore::open(dir.path()).unwrap();
        store
            .mark_dispatch_complete("tenant", "lane", 0, 25)
            .unwrap();
        store.set_dispatch_offset("tenant", "lane", 0, 20).unwrap();
        store.set_dispatch_offset("tenant", "lane", 0, 11).unwrap();
        assert_eq!(store.dispatch_offset("tenant", "lane", 0).unwrap(), 20);
        assert!(store.is_dispatch_complete("tenant", "lane", 0, 25).unwrap());
    }
}
