//! Delayed enqueue (`delay`) and recurring cron schedules.

mod cron;
mod legacy;
mod persist;
mod retry;

use broker_storage::{FileLock, JsonlJournal};
use chrono::Utc;
pub use cron::{normalize_cron, CronError, CronJob, CronRegistry, ScheduleKind};
pub use legacy::{legacy_import_totals, LegacyImport, LegacyImportTotals};
use parking_lot::Mutex;
use persist::{load_json_with_recovery, persist_json_atomic, JsonLoadSource, MetadataLoadError};
pub use retry::{
    fire_backoff_ms, FireFailure, FireRetry, FIRE_RETRY_BASE_MS, FIRE_RETRY_MAX_MS,
    PERMANENT_FAILURE_ATTEMPTS,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ScheduleError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("delayed job not found: {0}")]
    NotFound(Uuid),
    #[error("unsupported delayed schedule version: {0}")]
    UnsupportedVersion(u16),
    #[error(transparent)]
    MetadataLoad(#[from] MetadataLoadError),
    #[error(
        "schedule journal {0} is in use by another process; every broker needs its own data dir"
    )]
    Locked(PathBuf),
    #[error("delayed backlog is full ({jobs} jobs, {bytes} bytes pending); retry later")]
    BacklogFull { jobs: usize, bytes: usize },
    #[error("delay of {0} ms is out of range")]
    InvalidDelay(u64),
}

fn deserialize_flexible_payload<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::String(s) => Ok(s),
        other => serde_json::to_string(&other).map_err(serde::de::Error::custom),
    }
}

/// Payload waiting for delayed ingest (mirrors publish body fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledPublishRequest {
    pub topic: String,
    #[serde(default)]
    pub routing_key: String,
    #[serde(deserialize_with = "deserialize_flexible_payload")]
    pub payload: String,
    #[serde(default)]
    pub payload_encoding: Option<String>,
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub priority: Option<u8>,
    #[serde(default)]
    pub parallelism: Option<u32>,
    #[serde(default)]
    pub flow_id: Option<uuid::Uuid>,
    #[serde(default)]
    pub queue_id: Option<uuid::Uuid>,
    #[serde(default)]
    pub destination: Option<broker_partition::DestinationSnapshot>,
    /// Legacy — prefer `flow_id`.
    #[serde(default)]
    pub flow: Option<broker_partition::FlowSpec>,
    #[serde(default)]
    pub max_retries: Option<u32>,
    #[serde(default)]
    pub retry_backoff: Option<broker_proto::RetryBackoff>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(
        default,
        deserialize_with = "broker_partition::http_delivery::deserialize_optional_headers"
    )]
    pub headers: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    pub sign: Option<bool>,
    #[serde(default)]
    pub request: Option<broker_partition::HttpDeliveryInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledPublish {
    pub id: Uuid,
    pub deliver_at_ms: i64,
    pub request: ScheduledPublishRequest,
}

/// Payload-free inspection metadata, ordered by the stable delivery key.
#[derive(Debug, Clone, Serialize)]
pub struct ScheduledSummary {
    pub id: Uuid,
    pub deliver_at_ms: i64,
    pub topic: String,
    pub routing_key: String,
}

/// A pending delayed job as persisted in snapshots and journal records.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueuedJob {
    deliver_at_ms: i64,
    id: Uuid,
    request: ScheduledPublishRequest,
    /// In-memory retry gate after a failed fire; not persisted, so a restart
    /// retries once immediately and then backs off again.
    #[serde(skip)]
    not_before_ms: i64,
}

impl QueuedJob {
    fn due_at_ms(&self) -> i64 {
        self.deliver_at_ms.max(self.not_before_ms)
    }

    /// Approximate memory held for the job, for the backlog byte cap.
    fn size_bytes(&self) -> usize {
        self.request.payload.len() + self.request.topic.len() + self.request.routing_key.len()
    }

    fn summary(&self) -> ScheduledSummary {
        ScheduledSummary {
            id: self.id,
            deliver_at_ms: self.deliver_at_ms,
            topic: self.request.topic.clone(),
            routing_key: self.request.routing_key.clone(),
        }
    }

    fn to_public(&self) -> ScheduledPublish {
        ScheduledPublish {
            id: self.id,
            deliver_at_ms: self.deliver_at_ms,
            request: self.request.clone(),
        }
    }
}

/// Backlog limits for delayed jobs, checked before a delayed enqueue is
/// acknowledged. Jobs are held in memory until they fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduleLimits {
    pub max_jobs: usize,
    pub max_bytes: usize,
}

impl ScheduleLimits {
    pub const DEFAULT_MAX_JOBS: usize = 1_000_000;
    pub const DEFAULT_MAX_BYTES: usize = 1 << 30;

    /// `BETTERMQ_SCHEDULE_MAX_PENDING` / `BETTERMQ_SCHEDULE_MAX_PENDING_BYTES`.
    pub fn from_env() -> Self {
        let read = |name: &str, default: usize| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.trim().parse::<usize>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(default)
        };
        Self {
            max_jobs: read("BETTERMQ_SCHEDULE_MAX_PENDING", Self::DEFAULT_MAX_JOBS),
            max_bytes: read(
                "BETTERMQ_SCHEDULE_MAX_PENDING_BYTES",
                Self::DEFAULT_MAX_BYTES,
            ),
        }
    }
}

impl Default for ScheduleLimits {
    fn default() -> Self {
        Self {
            max_jobs: Self::DEFAULT_MAX_JOBS,
            max_bytes: Self::DEFAULT_MAX_BYTES,
        }
    }
}

#[derive(Clone)]
pub struct ScheduleQueue {
    path: PathBuf,
    journal: JsonlJournal,
    state: Arc<Mutex<ScheduleState>>,
    limits: ScheduleLimits,
    /// Exclusive ownership of the journal for the life of the queue.
    _lock: Arc<FileLock>,
}

/// Jobs live in `jobs`; `due` orders `(due_at_ms, id)` keys and may hold stale
/// keys (cancelled or rescheduled jobs) that `pop_due` skips, so every
/// operation is O(log n) instead of draining and rebuilding a heap.
#[derive(Default)]
struct ScheduleState {
    jobs: HashMap<Uuid, QueuedJob>,
    due: BinaryHeap<Reverse<(i64, Uuid)>>,
    in_flight: HashSet<Uuid>,
    /// Payload-free rows by `(deliver_at_ms, id)` for paged inspection.
    listing: BTreeMap<(i64, Uuid), ScheduledSummary>,
    /// Consecutive failed fires per job (in memory only).
    attempts: HashMap<Uuid, u32>,
    bytes: usize,
    sequence: u64,
    journal_ops: usize,
    compacting: bool,
}

impl ScheduleState {
    fn from_jobs(jobs: HashMap<Uuid, QueuedJob>) -> Self {
        let due = jobs
            .values()
            .map(|job| Reverse((job.due_at_ms(), job.id)))
            .collect::<Vec<_>>()
            .into();
        let bytes = jobs.values().map(QueuedJob::size_bytes).sum();
        let listing = jobs
            .values()
            .map(|job| ((job.deliver_at_ms, job.id), job.summary()))
            .collect();
        Self {
            jobs,
            due,
            listing,
            bytes,
            ..Self::default()
        }
    }

    fn insert(&mut self, job: QueuedJob) {
        if let Some(previous) = self.jobs.remove(&job.id) {
            self.bytes -= previous.size_bytes();
            self.listing.remove(&(previous.deliver_at_ms, previous.id));
        }
        self.bytes += job.size_bytes();
        self.due.push(Reverse((job.due_at_ms(), job.id)));
        self.listing
            .insert((job.deliver_at_ms, job.id), job.summary());
        self.jobs.insert(job.id, job);
    }

    fn remove(&mut self, id: &Uuid) -> Option<QueuedJob> {
        self.in_flight.remove(id);
        self.attempts.remove(id);
        let removed = self.jobs.remove(id)?;
        self.bytes -= removed.size_bytes();
        self.listing.remove(&(removed.deliver_at_ms, removed.id));
        // Bound the stale keys left behind by cancels.
        if self.due.len() > 2 * self.jobs.len() + 1024 {
            let in_flight = &self.in_flight;
            self.due = self
                .jobs
                .values()
                .filter(|job| !in_flight.contains(&job.id))
                .map(|job| Reverse((job.due_at_ms(), job.id)))
                .collect::<Vec<_>>()
                .into();
        }
        Some(removed)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)] // Persisted operations are infrequent and mirror heap records.
enum JournalOp {
    Add { item: QueuedJob },
    Remove { id: Uuid },
}

#[derive(Debug, Serialize, Deserialize)]
struct JournalRecord {
    version: u16,
    sequence: u64,
    op: JournalOp,
}

#[derive(Debug, Serialize, Deserialize)]
struct ScheduleSnapshot {
    version: u16,
    sequence: u64,
    items: Vec<QueuedJob>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum ScheduleSnapshotCompat {
    Current(ScheduleSnapshot),
    Legacy(Vec<QueuedJob>),
}

const SCHEDULE_STATE_VERSION: u16 = 1;
const JOURNAL_COMPACT_OPS: usize = 1024;

/// Parse one journal line: a versioned record, or a bare op from early releases.
fn parse_journal_line(line: &[u8], sequence: u64) -> Result<JournalRecord, ScheduleError> {
    match serde_json::from_slice::<JournalRecord>(line) {
        Ok(record) => Ok(record),
        Err(record_error) => match serde_json::from_slice::<JournalOp>(line) {
            Ok(op) => Ok(JournalRecord {
                version: SCHEDULE_STATE_VERSION,
                sequence: sequence.saturating_add(1),
                op,
            }),
            Err(_) => Err(ScheduleError::Serde(record_error)),
        },
    }
}

fn snapshot_items(
    snapshot: ScheduleSnapshotCompat,
) -> Result<(Vec<QueuedJob>, u64), ScheduleError> {
    match snapshot {
        ScheduleSnapshotCompat::Current(snapshot) => {
            if snapshot.version != SCHEDULE_STATE_VERSION {
                return Err(ScheduleError::UnsupportedVersion(snapshot.version));
            }
            Ok((snapshot.items, snapshot.sequence))
        }
        ScheduleSnapshotCompat::Legacy(items) => Ok((items, 0)),
    }
}

/// Take the journal's exclusive lock (`<journal>.lock`) or fail: two processes
/// appending to one journal with separate in-memory state lose jobs.
pub(crate) fn lock_journal(journal: &Path) -> std::io::Result<Option<FileLock>> {
    FileLock::try_exclusive(journal)
}

impl ScheduleQueue {
    /// Open the node-local delayed queue in `data_dir`. Never shared between
    /// processes: the journal is locked exclusively for the queue's lifetime.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, ScheduleError> {
        Self::open_with_limits(data_dir, ScheduleLimits::from_env())
    }

    pub fn open_with_limits(
        data_dir: impl AsRef<Path>,
        limits: ScheduleLimits,
    ) -> Result<Self, ScheduleError> {
        let path = data_dir.as_ref().join("schedule.json");
        let journal_path = path.with_extension("journal");
        let lock = lock_journal(&journal_path)?
            .ok_or_else(|| ScheduleError::Locked(journal_path.clone()))?;
        let journal = JsonlJournal::new(journal_path);
        let loaded = load_json_with_recovery(&path, || ScheduleSnapshotCompat::Legacy(Vec::new()))?;
        let (items, mut sequence) = snapshot_items(loaded.value)?;
        if !matches!(
            loaded.source,
            JsonLoadSource::Main | JsonLoadSource::Missing
        ) {
            info!(
                file = %path.display(),
                source = ?loaded.source,
                count = items.len(),
                "schedule queue recovered after metadata read failure"
            );
        }
        // Replay into a map and build the due queue once at the end.
        let mut jobs: HashMap<Uuid, QueuedJob> =
            items.into_iter().map(|item| (item.id, item)).collect();
        let mut journal_ops = 0usize;
        for line in journal.read_lines()? {
            let record = parse_journal_line(&line, sequence)?;
            if record.version != SCHEDULE_STATE_VERSION {
                return Err(ScheduleError::UnsupportedVersion(record.version));
            }
            if record.sequence <= sequence {
                continue;
            }
            sequence = record.sequence;
            journal_ops += 1;
            match record.op {
                JournalOp::Add { item } => {
                    jobs.insert(item.id, item);
                }
                JournalOp::Remove { id } => {
                    jobs.remove(&id);
                }
            }
        }
        let mut state = ScheduleState::from_jobs(jobs);
        state.sequence = sequence;
        state.journal_ops = journal_ops;
        Ok(Self {
            path,
            journal,
            state: Arc::new(Mutex::new(state)),
            limits,
            _lock: Arc::new(lock),
        })
    }

    /// Import delayed jobs that older releases stored under the shared meta
    /// dir. Run it only on the node that fires schedules (the scheduler
    /// holder): imported jobs become this node's local jobs.
    ///
    /// The live shared files are renamed aside before they are read
    /// (`schedule.json.importing-<ts>`, then `.imported-<ts>` once done), so a
    /// node still on an older release that appends later writes a new file
    /// for the next pass. Shared files are never truncated or rewritten. Ids
    /// and deadlines are preserved; a ledger beside the local journal keeps
    /// re-scans and interrupted passes from importing a job twice.
    pub fn import_legacy_shared(&self, shared_dir: &Path) -> Result<LegacyImport, ScheduleError> {
        let mut report = LegacyImport::default();
        if !shared_dir.is_dir() || legacy::is_same_dir(shared_dir, &self.path) {
            return Ok(report);
        }
        let Some(_import_lock) = FileLock::try_exclusive(&shared_dir.join("schedule-import"))?
        else {
            return Ok(report);
        };
        let groups = legacy::claim(shared_dir, "schedule.json", "schedule.journal")?;
        if groups.is_empty() {
            return Ok(report);
        }
        // Shared journals had several writers with colliding sequence numbers,
        // so apply records in file order instead of filtering by sequence.
        let mut found: HashMap<Uuid, QueuedJob> = HashMap::new();
        for group in &groups {
            if let Some(snapshot) = &group.snapshot {
                let parsed: ScheduleSnapshotCompat =
                    serde_json::from_slice(&std::fs::read(snapshot)?)?;
                for item in snapshot_items(parsed)?.0 {
                    found.insert(item.id, item);
                }
            }
            if let Some(journal) = &group.journal {
                for line in JsonlJournal::new(journal).read_lines_readonly()? {
                    let record = parse_journal_line(&line, 0)?;
                    if record.version != SCHEDULE_STATE_VERSION {
                        return Err(ScheduleError::UnsupportedVersion(record.version));
                    }
                    match record.op {
                        JournalOp::Add { item } => {
                            found.insert(item.id, item);
                        }
                        JournalOp::Remove { id } => {
                            found.remove(&id);
                        }
                    }
                }
            }
            report.sources.extend(group.files().cloned());
        }
        let mut ledger = legacy::Ledger::load(self.path.with_extension("imported.json"))?;
        {
            let mut state = self.state.lock();
            let mut items: Vec<_> = found.into_values().collect();
            items.sort_by_key(|item| (item.deliver_at_ms, item.id));
            for mut item in items {
                // Imported jobs were already acknowledged: no backlog cap here.
                let local = state.jobs.contains_key(&item.id);
                if local || ledger.contains(&item.id) {
                    report.skipped += 1;
                    ledger.insert(item.id);
                    continue;
                }
                item.not_before_ms = 0;
                self.append_op(&mut state, JournalOp::Add { item: item.clone() })?;
                ledger.insert(item.id);
                state.insert(item);
                state.journal_ops += 1;
                report.imported += 1;
            }
        }
        self.compact_if_needed();
        ledger.save()?;
        legacy::finish(&groups)?;
        legacy::record_imported_delayed(report.imported);
        info!(
            shared_dir = %shared_dir.display(),
            imported = report.imported,
            skipped = report.skipped,
            files = report.sources.len(),
            "imported legacy shared delayed jobs"
        );
        Ok(report)
    }

    pub fn schedule(
        &self,
        request: ScheduledPublishRequest,
        delay_ms: u64,
    ) -> Result<ScheduledPublish, ScheduleError> {
        let deliver_at_ms = i64::try_from(delay_ms)
            .ok()
            .and_then(|delay| Utc::now().timestamp_millis().checked_add(delay))
            .ok_or(ScheduleError::InvalidDelay(delay_ms))?;
        let id = Uuid::new_v4();
        let item = QueuedJob {
            deliver_at_ms,
            id,
            request: request.clone(),
            not_before_ms: 0,
        };
        {
            let mut state = self.state.lock();
            if state.jobs.len() >= self.limits.max_jobs
                || state.bytes.saturating_add(item.size_bytes()) > self.limits.max_bytes
            {
                return Err(ScheduleError::BacklogFull {
                    jobs: state.jobs.len(),
                    bytes: state.bytes,
                });
            }
            self.append_op(&mut state, JournalOp::Add { item: item.clone() })?;
            state.insert(item);
            state.journal_ops += 1;
        }
        self.compact_if_needed();
        Ok(ScheduledPublish {
            id,
            deliver_at_ms,
            request,
        })
    }

    pub fn list(&self) -> Vec<ScheduledPublish> {
        let state = self.state.lock();
        state.jobs.values().map(QueuedJob::to_public).collect()
    }

    /// Jobs waiting to fire, including ones being fired now.
    pub fn count(&self) -> usize {
        self.state.lock().jobs.len()
    }

    /// At most 1000 metadata rows, exclusively after the supplied stable key.
    /// Includes due jobs in flight until their durable completion.
    pub fn page(&self, after: Option<(i64, Uuid)>, limit: usize) -> Vec<ScheduledSummary> {
        use std::ops::Bound::{Excluded, Unbounded};
        let state = self.state.lock();
        let lower = after.map_or(Unbounded, Excluded);
        state
            .listing
            .range((lower, Unbounded))
            .take(limit.min(1000))
            .map(|(_, item)| item.clone())
            .collect()
    }

    pub fn cancel(&self, id: Uuid) -> Result<ScheduledPublish, ScheduleError> {
        let removed = {
            let mut state = self.state.lock();
            if !state.jobs.contains_key(&id) {
                return Err(ScheduleError::NotFound(id));
            }
            self.append_op(&mut state, JournalOp::Remove { id })?;
            state.journal_ops += 1;
            state.remove(&id).expect("checked above")
        };
        self.compact_if_needed();
        Ok(removed.to_public())
    }

    /// Pop at most `limit` due jobs **without** persisting, so one tick does
    /// bounded work. Persist only via [`Self::complete`] after a successful
    /// fire so a crash before publish still reloads the job (at-least-once).
    pub fn pop_due(&self, now_ms: i64, limit: usize) -> Vec<ScheduledPublish> {
        let mut guard = self.state.lock();
        let state = &mut *guard;
        let mut popped = Vec::new();
        while popped.len() < limit {
            let Some(&Reverse((due_at_ms, id))) = state.due.peek() else {
                break;
            };
            if due_at_ms > now_ms {
                break;
            }
            state.due.pop();
            let Some(job) = state.jobs.get(&id) else {
                continue; // cancelled or completed
            };
            if state.in_flight.contains(&id) || job.due_at_ms() != due_at_ms {
                continue; // stale key from before a reschedule
            }
            state.in_flight.insert(id);
            popped.push(job.to_public());
        }
        popped
    }

    /// Remove a fired job from durable storage after successful publish.
    pub fn complete(&self, id: Uuid) -> Result<(), ScheduleError> {
        {
            let mut state = self.state.lock();
            self.append_op(&mut state, JournalOp::Remove { id })?;
            state.remove(&id);
            state.journal_ops += 1;
        }
        self.compact_if_needed();
        Ok(())
    }

    /// Remove several fired jobs with one journal append and one fsync.
    pub fn complete_many(&self, ids: &[Uuid]) -> Result<(), ScheduleError> {
        if ids.is_empty() {
            return Ok(());
        }
        {
            let mut state = self.state.lock();
            self.append_ops(
                &mut state,
                ids.iter().map(|id| JournalOp::Remove { id: *id }).collect(),
            )?;
            for id in ids {
                state.remove(id);
            }
            state.journal_ops += ids.len();
        }
        self.compact_if_needed();
        Ok(())
    }

    /// Record a failed fire of a job returned by [`Self::pop_due`].
    ///
    /// On [`FireRetry::Retry`] the job goes back into the queue but is not due
    /// again until the backoff elapses; its durable Add stays in the journal.
    /// On [`FireRetry::GiveUp`] (a permanent failure that kept failing) the
    /// job is removed durably, like [`Self::complete`].
    pub fn fail(
        &self,
        job: ScheduledPublish,
        failure: FireFailure,
        now_ms: i64,
    ) -> Result<FireRetry, ScheduleError> {
        let mut guard = self.state.lock();
        let state = &mut *guard;
        let attempt = state
            .attempts
            .get(&job.id)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        let decision = retry::decide(attempt, failure, now_ms);
        let retry_at_ms = match decision {
            FireRetry::Retry { retry_at_ms, .. } => retry_at_ms,
            FireRetry::GiveUp { .. } => {
                match self.append_op(state, JournalOp::Remove { id: job.id }) {
                    Ok(()) => {
                        state.remove(&job.id);
                        state.journal_ops += 1;
                        drop(guard);
                        self.compact_if_needed();
                        return Ok(decision);
                    }
                    Err(error) => {
                        // Keep the job and retry later rather than losing track of it.
                        Self::push_retry(state, job.id, attempt, now_ms + FIRE_RETRY_MAX_MS);
                        return Err(error);
                    }
                }
            }
        };
        Self::push_retry(state, job.id, attempt, retry_at_ms);
        Ok(decision)
    }

    fn push_retry(state: &mut ScheduleState, id: Uuid, attempt: u32, at_ms: i64) {
        if !state.in_flight.remove(&id) {
            return; // cancelled while the fire was in progress
        }
        let Some(job) = state.jobs.get_mut(&id) else {
            return;
        };
        job.not_before_ms = at_ms;
        let due_at_ms = job.due_at_ms();
        state.attempts.insert(id, attempt);
        state.due.push(Reverse((due_at_ms, id)));
    }

    fn append_op(&self, state: &mut ScheduleState, op: JournalOp) -> Result<(), ScheduleError> {
        self.append_ops(state, vec![op])
    }

    /// Append records in one write followed by one fsync. A crash can tear
    /// only the final line, which the next append or `open` cuts off.
    fn append_ops(
        &self,
        state: &mut ScheduleState,
        ops: Vec<JournalOp>,
    ) -> Result<(), ScheduleError> {
        let mut sequence = state.sequence;
        let records: Vec<_> = ops
            .into_iter()
            .map(|op| {
                sequence = sequence.saturating_add(1);
                JournalRecord {
                    version: SCHEDULE_STATE_VERSION,
                    sequence,
                    op,
                }
            })
            .collect();
        self.journal.append_all(&records)?;
        state.sequence = sequence;
        Ok(())
    }

    /// Fold the journal into a snapshot every [`JOURNAL_COMPACT_OPS`] ops.
    ///
    /// Only cloning the jobs happens under the state lock; encoding and the
    /// fsynced snapshot write run outside it, so ingest and firing continue.
    /// The journal is then rotated under the lock, keeping only records newer
    /// than the snapshot's sequence: appends made while the snapshot was being
    /// written are never truncated away.
    fn compact_if_needed(&self) {
        let (items, sequence) = {
            let mut state = self.state.lock();
            if state.journal_ops < JOURNAL_COMPACT_OPS || state.compacting {
                return;
            }
            state.compacting = true;
            (
                state.jobs.values().cloned().collect::<Vec<_>>(),
                state.sequence,
            )
        };
        let written = self.write_snapshot(items, sequence);
        let mut state = self.state.lock();
        state.compacting = false;
        if let Err(error) = written {
            tracing::warn!(%error, "schedule journal compaction snapshot failed");
            return;
        }
        match self.rotate_journal(sequence) {
            Ok(kept) => state.journal_ops = kept,
            Err(error) => tracing::warn!(%error, "schedule journal rotation failed"),
        }
    }

    fn write_snapshot(&self, items: Vec<QueuedJob>, sequence: u64) -> Result<(), ScheduleError> {
        let bytes = serde_json::to_vec(&ScheduleSnapshot {
            version: SCHEDULE_STATE_VERSION,
            sequence,
            items,
        })?;
        persist_json_atomic(&self.path, &bytes)?;
        broker_storage::set_secret_file_mode(&self.path);
        Ok(())
    }

    /// Drop journal records the snapshot at `covered` already contains.
    /// Caller holds the state lock, so no append races the rewrite. Bare
    /// legacy ops carry no sequence; they predate this process and were
    /// replayed into the snapshot, so they are dropped too.
    fn rotate_journal(&self, covered: u64) -> Result<usize, ScheduleError> {
        let keep: Vec<_> = self
            .journal
            .read_lines()?
            .into_iter()
            .filter(|line| {
                serde_json::from_slice::<JournalRecord>(line)
                    .is_ok_and(|record| record.sequence > covered)
            })
            .collect();
        self.journal.rewrite(&keep)?;
        Ok(keep.len())
    }
}

#[cfg(test)]
mod schedule_tests {
    use super::*;
    use crate::persist::env_test_lock::LOCK;
    use tempfile::tempdir;

    fn request() -> ScheduledPublishRequest {
        ScheduledPublishRequest {
            topic: "__direct".into(),
            routing_key: "rk".into(),
            payload: "body".into(),
            payload_encoding: None,
            idempotency_key: Some("scheduled-test".into()),
            priority: None,
            parallelism: None,
            flow_id: None,
            queue_id: None,
            destination: None,
            flow: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        }
    }

    #[test]
    fn metadata_pages_and_counts_follow_cancel_requeue_complete_and_reopen() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        for _ in 0..5 {
            queue.schedule(request(), 0).unwrap();
        }
        assert_eq!(queue.count(), 5);
        let first = queue.page(None, 2);
        assert_eq!(first.len(), 2);
        let last = first.last().unwrap();
        let second = queue.page(Some((last.deliver_at_ms, last.id)), 2);
        assert_eq!(second.len(), 2);
        assert!(second
            .iter()
            .all(|item| !first.iter().any(|previous| item.id == previous.id)));
        assert!(serde_json::to_value(&first).unwrap()[0]
            .get("payload")
            .is_none());
        queue.cancel(first[0].id).unwrap();
        assert_eq!(queue.count(), 4);
        let mut due = queue.pop_due(i64::MAX, usize::MAX);
        assert_eq!(queue.count(), 4, "in-flight jobs remain visible");
        queue
            .fail(due.pop().unwrap(), FireFailure::Transient, 0)
            .unwrap();
        queue.complete(due.pop().unwrap().id).unwrap();
        assert_eq!(queue.count(), 3);
        drop(queue);
        let reopened = ScheduleQueue::open(dir.path()).unwrap();
        assert_eq!(reopened.count(), 3);
        assert_eq!(reopened.page(None, usize::MAX).len(), 3);
    }

    #[test]
    fn failed_schedule_does_not_increment_count() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        std::fs::create_dir(dir.path().join("schedule.journal")).unwrap();
        assert!(queue.schedule(request(), 0).is_err());
        assert_eq!(queue.count(), 0);
        assert!(queue.page(None, 10).is_empty());
    }

    #[test]
    fn open_fails_on_empty_corrupt_schedule_json_without_recovery_env() {
        let _guard = LOCK.lock().unwrap();
        unsafe { std::env::remove_var("BETTERMQ_METADATA_RECOVER") };
        let dir = tempdir().unwrap();
        let path = dir.path().join("schedule.json");
        std::fs::write(&path, b"").unwrap();
        assert!(ScheduleQueue::open(dir.path()).is_err());
    }

    #[test]
    fn open_recovers_empty_schedule_json_when_recovery_env_set() {
        let _guard = LOCK.lock().unwrap();
        unsafe { std::env::set_var("BETTERMQ_METADATA_RECOVER", "empty") };
        let dir = tempdir().unwrap();
        let path = dir.path().join("schedule.json");
        std::fs::write(&path, b"").unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        unsafe { std::env::remove_var("BETTERMQ_METADATA_RECOVER") };
        assert!(queue.list().is_empty());
    }

    #[test]
    fn append_journal_survives_requeue_and_completion() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        let scheduled = queue.schedule(request(), 0).unwrap();
        assert!(dir.path().join("schedule.journal").exists());
        drop(queue);

        let queue = ScheduleQueue::open(dir.path()).unwrap();
        assert_eq!(queue.list().len(), 1);
        let pending = queue.pop_due(i64::MAX, usize::MAX).pop().unwrap();
        queue.fail(pending, FireFailure::Transient, 0).unwrap();
        drop(queue);

        let queue = ScheduleQueue::open(dir.path()).unwrap();
        assert_eq!(queue.list().len(), 1);
        let pending = queue.pop_due(i64::MAX, usize::MAX).pop().unwrap();
        assert_eq!(pending.id, scheduled.id);
        queue.complete(pending.id).unwrap();
        drop(queue);

        let queue = ScheduleQueue::open(dir.path()).unwrap();
        assert!(queue.list().is_empty());
    }

    #[test]
    fn complete_many_removes_a_batch_in_one_append() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        let ids: Vec<_> = (0..5)
            .map(|_| queue.schedule(request(), 0).unwrap().id)
            .collect();
        let due = queue.pop_due(i64::MAX, usize::MAX);
        assert_eq!(due.len(), 5);
        let journal = dir.path().join("schedule.journal");
        let before = std::fs::read(&journal).unwrap().len();

        queue.complete_many(&ids[..3]).unwrap();
        let appended = std::fs::read(&journal).unwrap()[before..].to_vec();
        assert_eq!(appended.iter().filter(|b| **b == b'\n').count(), 3);
        assert_eq!(queue.list().len(), 2, "remaining jobs stay in flight");
        drop(queue);

        let queue = ScheduleQueue::open(dir.path()).unwrap();
        let mut left: Vec<_> = queue.list().into_iter().map(|job| job.id).collect();
        left.sort();
        let mut expected = ids[3..].to_vec();
        expected.sort();
        assert_eq!(left, expected);
        queue.complete_many(&[]).unwrap();
    }

    #[test]
    fn failed_fire_backs_off_exponentially() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        let scheduled = queue.schedule(request(), 0).unwrap();
        let now = scheduled.deliver_at_ms + 1;

        let pending = queue.pop_due(now, usize::MAX).pop().unwrap();
        assert_eq!(
            queue.fail(pending, FireFailure::Transient, now).unwrap(),
            FireRetry::Retry {
                attempt: 1,
                retry_at_ms: now + FIRE_RETRY_BASE_MS
            }
        );
        assert!(queue
            .pop_due(now + FIRE_RETRY_BASE_MS - 1, usize::MAX)
            .is_empty());
        let listed = queue.list();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].deliver_at_ms, scheduled.deliver_at_ms);

        let retry_at = now + FIRE_RETRY_BASE_MS;
        let pending = queue.pop_due(retry_at, usize::MAX).pop().unwrap();
        assert_eq!(pending.deliver_at_ms, scheduled.deliver_at_ms);
        assert_eq!(
            queue
                .fail(pending, FireFailure::Transient, retry_at)
                .unwrap(),
            FireRetry::Retry {
                attempt: 2,
                retry_at_ms: retry_at + 2 * FIRE_RETRY_BASE_MS
            }
        );

        let pending = queue.pop_due(i64::MAX, usize::MAX).pop().unwrap();
        queue.complete(pending.id).unwrap();
        assert!(queue.list().is_empty());
    }

    #[test]
    fn permanent_failure_gives_up_and_removes_job_durably() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        queue.schedule(request(), 0).unwrap();
        let mut outcome = None;
        for _ in 0..PERMANENT_FAILURE_ATTEMPTS {
            let pending = queue.pop_due(i64::MAX, usize::MAX).pop().unwrap();
            outcome = Some(queue.fail(pending, FireFailure::Permanent, 0).unwrap());
        }
        assert_eq!(
            outcome,
            Some(FireRetry::GiveUp {
                attempt: PERMANENT_FAILURE_ATTEMPTS
            })
        );
        assert!(queue.list().is_empty());
        assert!(queue.pop_due(i64::MAX, usize::MAX).is_empty());
        drop(queue);
        assert!(ScheduleQueue::open(dir.path()).unwrap().list().is_empty());
    }

    #[test]
    fn failed_fire_of_cancelled_job_is_not_requeued() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        queue.schedule(request(), 0).unwrap();
        let pending = queue.pop_due(i64::MAX, usize::MAX).pop().unwrap();
        queue.cancel(pending.id).unwrap();
        queue.fail(pending, FireFailure::Transient, 0).unwrap();
        assert!(queue.list().is_empty());
    }

    #[test]
    fn append_after_torn_journal_tail_survives_restart() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        let first = queue.schedule(request(), 60_000).unwrap();
        drop(queue);
        let journal = dir.path().join("schedule.journal");
        let mut bytes = std::fs::read(&journal).unwrap();
        bytes.extend_from_slice(br#"{"version":1,"sequence":2,"op":{"op":"ad"#);
        std::fs::write(&journal, bytes).unwrap();

        let queue = ScheduleQueue::open(dir.path()).unwrap();
        let second = queue.schedule(request(), 60_000).unwrap();
        drop(queue);

        let mut ids: Vec<_> = ScheduleQueue::open(dir.path())
            .unwrap()
            .list()
            .into_iter()
            .map(|job| job.id)
            .collect();
        ids.sort();
        let mut expected = vec![first.id, second.id];
        expected.sort();
        assert_eq!(ids, expected);
    }

    #[test]
    fn second_open_of_same_journal_is_refused() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        let clone = queue.clone();
        assert!(matches!(
            ScheduleQueue::open(dir.path()),
            Err(ScheduleError::Locked(_))
        ));
        drop(queue);
        assert!(
            matches!(
                ScheduleQueue::open(dir.path()),
                Err(ScheduleError::Locked(_))
            ),
            "a live clone still owns the journal"
        );
        drop(clone);
        ScheduleQueue::open(dir.path()).unwrap();
    }

    #[test]
    fn shared_meta_dir_is_ignored() {
        let _guard = LOCK.lock().unwrap();
        let local = tempdir().unwrap();
        let shared = tempdir().unwrap();
        unsafe { std::env::set_var("BETTERMQ_SHARED_META_DIR", shared.path()) };
        let opened = ScheduleQueue::open(local.path());
        unsafe { std::env::remove_var("BETTERMQ_SHARED_META_DIR") };
        opened.unwrap().schedule(request(), 60_000).unwrap();
        assert!(local.path().join("schedule.journal").exists());
        assert!(!shared.path().join("schedule.journal").exists());
    }

    /// Append `queue`'s journal lines to `shared`'s journal, as a second
    /// legacy writer with its own (colliding) sequence numbers would.
    fn append_journal(from: &Path, to: &Path) {
        let lines = std::fs::read(from.join("schedule.journal")).unwrap();
        let mut existing = std::fs::read(to.join("schedule.journal")).unwrap_or_default();
        existing.extend_from_slice(&lines);
        std::fs::write(to.join("schedule.journal"), existing).unwrap();
    }

    fn ids(queue: &ScheduleQueue) -> Vec<Uuid> {
        let mut ids: Vec<_> = queue.list().into_iter().map(|job| job.id).collect();
        ids.sort();
        ids
    }

    fn sorted(mut ids: Vec<Uuid>) -> Vec<Uuid> {
        ids.sort();
        ids
    }

    #[test]
    fn legacy_shared_import_preserves_jobs_and_is_idempotent() {
        let shared = tempdir().unwrap();
        let other = tempdir().unwrap();
        let local = tempdir().unwrap();

        // Two legacy writers on one shared journal, sequences colliding.
        let writer = ScheduleQueue::open(shared.path()).unwrap();
        let a = writer.schedule(request(), 60_000).unwrap();
        let b = writer.schedule(request(), 120_000).unwrap();
        writer.cancel(a.id).unwrap();
        drop(writer);
        let second = ScheduleQueue::open(other.path()).unwrap();
        let c = second.schedule(request(), 30_000).unwrap();
        drop(second);
        append_journal(other.path(), shared.path());

        let queue = ScheduleQueue::open(local.path()).unwrap();
        let report = queue.import_legacy_shared(shared.path()).unwrap();
        assert_eq!(report.imported, 2);
        assert_eq!(ids(&queue), sorted(vec![b.id, c.id]));
        let listed = queue.list();
        let imported_b = listed.iter().find(|job| job.id == b.id).unwrap();
        assert_eq!(imported_b.deliver_at_ms, b.deliver_at_ms);
        assert!(!shared.path().join("schedule.journal").exists());
        assert!(std::fs::read_dir(shared.path()).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("schedule.journal.imported-")));

        // Re-scan: nothing new.
        assert_eq!(
            queue.import_legacy_shared(shared.path()).unwrap().imported,
            0
        );

        // A late append from an old node (re-adding b, plus a new d) after b
        // already fired here: only d is imported.
        let job = queue.pop_due(i64::MAX, usize::MAX);
        for fired in job {
            if fired.id == b.id {
                queue.complete(fired.id).unwrap();
            } else {
                queue.fail(fired, FireFailure::Transient, 0).unwrap();
            }
        }
        let late = tempdir().unwrap();
        let old_node = ScheduleQueue::open(late.path()).unwrap();
        let d = old_node.schedule(request(), 10_000).unwrap();
        drop(old_node);
        std::fs::copy(
            late.path().join("schedule.journal"),
            shared.path().join("schedule.journal"),
        )
        .unwrap();
        let mut bytes = std::fs::read(shared.path().join("schedule.journal")).unwrap();
        bytes.extend_from_slice(
            &serde_json::to_vec(&JournalRecord {
                version: SCHEDULE_STATE_VERSION,
                sequence: 1,
                op: JournalOp::Add {
                    item: QueuedJob {
                        deliver_at_ms: b.deliver_at_ms,
                        id: b.id,
                        request: request(),
                        not_before_ms: 0,
                    },
                },
            })
            .unwrap(),
        );
        bytes.push(b'\n');
        std::fs::write(shared.path().join("schedule.journal"), bytes).unwrap();

        let report = queue.import_legacy_shared(shared.path()).unwrap();
        assert_eq!(report.imported, 1);
        assert_eq!(report.skipped, 1);
        drop(queue);
        let reopened = ScheduleQueue::open(local.path()).unwrap();
        assert_eq!(ids(&reopened), sorted(vec![c.id, d.id]));
    }

    #[test]
    fn interrupted_legacy_import_resumes_from_claimed_files() {
        let shared = tempdir().unwrap();
        let local = tempdir().unwrap();
        let writer = ScheduleQueue::open(shared.path()).unwrap();
        let a = writer.schedule(request(), 60_000).unwrap();
        drop(writer);
        // Crash right after the claim rename, before anything was imported.
        std::fs::rename(
            shared.path().join("schedule.journal"),
            shared
                .path()
                .join("schedule.journal.importing-20260101T000000.000000000Z"),
        )
        .unwrap();

        let queue = ScheduleQueue::open(local.path()).unwrap();
        assert_eq!(
            queue.import_legacy_shared(shared.path()).unwrap().imported,
            1
        );
        assert_eq!(ids(&queue), vec![a.id]);
        assert!(shared
            .path()
            .join("schedule.journal.imported-20260101T000000.000000000Z")
            .exists());
    }

    #[test]
    fn legacy_import_from_own_data_dir_is_a_no_op() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        queue.schedule(request(), 60_000).unwrap();
        assert_eq!(
            queue.import_legacy_shared(dir.path()).unwrap(),
            LegacyImport::default()
        );
        assert!(dir.path().join("schedule.journal").exists());
    }

    #[test]
    fn replay_applies_adds_and_removes_by_id() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        let mut kept = Vec::new();
        for index in 0..200 {
            let job = queue.schedule(request(), 60_000 + index).unwrap();
            if index % 3 == 0 {
                queue.cancel(job.id).unwrap();
            } else {
                kept.push(job.id);
            }
        }
        drop(queue);
        let reopened = ScheduleQueue::open(dir.path()).unwrap();
        assert_eq!(ids(&reopened), sorted(kept));
        assert_eq!(reopened.count(), reopened.list().len());
    }

    #[test]
    fn compaction_keeps_appends_made_while_the_snapshot_was_written() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        let first = queue.schedule(request(), 60_000).unwrap();
        // Snapshot taken here (as compact_if_needed does under the lock) ...
        let (items, sequence) = {
            let state = queue.state.lock();
            (
                state.jobs.values().cloned().collect::<Vec<_>>(),
                state.sequence,
            )
        };
        // ... an append lands while it is being written outside the lock ...
        let second = queue.schedule(request(), 60_000).unwrap();
        queue.write_snapshot(items, sequence).unwrap();
        // ... and rotation keeps it.
        {
            let _state = queue.state.lock();
            assert_eq!(queue.rotate_journal(sequence).unwrap(), 1);
        }
        drop(queue);
        let reopened = ScheduleQueue::open(dir.path()).unwrap();
        assert_eq!(ids(&reopened), sorted(vec![first.id, second.id]));
    }

    #[test]
    fn threshold_compaction_snapshots_and_rotates() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        let first = queue.schedule(request(), 60_000).unwrap();
        queue.state.lock().journal_ops = JOURNAL_COMPACT_OPS - 1;
        let second = queue.schedule(request(), 60_000).unwrap();
        assert_eq!(queue.state.lock().journal_ops, 0);
        assert!(dir.path().join("schedule.json").exists());
        assert_eq!(
            std::fs::metadata(dir.path().join("schedule.journal"))
                .unwrap()
                .len(),
            0
        );
        let third = queue.schedule(request(), 60_000).unwrap();
        drop(queue);
        let reopened = ScheduleQueue::open(dir.path()).unwrap();
        assert_eq!(ids(&reopened), sorted(vec![first.id, second.id, third.id]));
    }

    #[test]
    fn backlog_cap_rejects_before_anything_is_written() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open_with_limits(
            dir.path(),
            ScheduleLimits {
                max_jobs: 2,
                max_bytes: usize::MAX,
            },
        )
        .unwrap();
        queue.schedule(request(), 60_000).unwrap();
        queue.schedule(request(), 60_000).unwrap();
        let journal_len = std::fs::metadata(dir.path().join("schedule.journal"))
            .unwrap()
            .len();
        assert!(matches!(
            queue.schedule(request(), 60_000),
            Err(ScheduleError::BacklogFull { jobs: 2, .. })
        ));
        assert_eq!(
            std::fs::metadata(dir.path().join("schedule.journal"))
                .unwrap()
                .len(),
            journal_len
        );

        let small = tempdir().unwrap();
        let queue = ScheduleQueue::open_with_limits(
            small.path(),
            ScheduleLimits {
                max_jobs: usize::MAX,
                max_bytes: 4,
            },
        )
        .unwrap();
        assert!(matches!(
            queue.schedule(request(), 60_000),
            Err(ScheduleError::BacklogFull { .. })
        ));
    }

    #[test]
    fn out_of_range_delay_is_rejected() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        assert!(matches!(
            queue.schedule(request(), u64::MAX),
            Err(ScheduleError::InvalidDelay(u64::MAX))
        ));
        assert!(matches!(
            queue.schedule(request(), i64::MAX as u64),
            Err(ScheduleError::InvalidDelay(_))
        ));
        assert_eq!(queue.count(), 0);
    }

    #[test]
    fn pop_due_is_bounded_and_skips_cancelled_jobs() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        let jobs: Vec<_> = (0..5)
            .map(|_| queue.schedule(request(), 0).unwrap())
            .collect();
        queue.cancel(jobs[0].id).unwrap();
        let now = i64::MAX;
        assert_eq!(queue.pop_due(now, 2).len(), 2);
        assert_eq!(queue.pop_due(now, 2).len(), 2);
        assert!(queue.pop_due(now, 2).is_empty());
        assert_eq!(queue.count(), 4, "popped jobs stay pending until completed");
    }

    #[test]
    fn unsupported_schedule_journal_version_fails_closed() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("schedule.json"), b"[]").unwrap();
        std::fs::write(
            dir.path().join("schedule.journal"),
            br#"{"version":99,"sequence":1,"op":{"op":"remove","id":"00000000-0000-0000-0000-000000000000"}}"#,
        )
        .unwrap();
        assert!(matches!(
            ScheduleQueue::open(dir.path()),
            Err(ScheduleError::UnsupportedVersion(99))
        ));
    }
}
