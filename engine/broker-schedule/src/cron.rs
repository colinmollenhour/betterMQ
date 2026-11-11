//! Recurring schedules: cron expression or fixed interval, file-backed.

use crate::legacy::{self, LegacyImport};
use crate::persist::{
    load_json_with_recovery, persist_json_atomic, JsonLoadSource, MetadataLoadError,
};
use crate::retry::{self, FireFailure, FireRetry};
use crate::ScheduledPublishRequest;
use broker_storage::{FileLock, JsonlJournal};
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum CronError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid cron expression: {0}")]
    InvalidExpression(String),
    #[error("invalid schedule: {0}")]
    InvalidSchedule(String),
    #[error("cron job not found: {0}")]
    NotFound(Uuid),
    #[error(transparent)]
    MetadataLoad(#[from] MetadataLoadError),
    #[error("cron journal {0} is in use by another process; every broker needs its own data dir")]
    Locked(PathBuf),
}

/// Either a cron pattern or a fixed interval between runs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScheduleKind {
    Cron { cron: String },
    Interval { every_seconds: u64 },
}

impl ScheduleKind {
    pub fn from_cron(expr: impl Into<String>) -> Self {
        Self::Cron { cron: expr.into() }
    }

    pub fn from_interval(every_seconds: u64) -> Result<Self, CronError> {
        if every_seconds == 0 {
            return Err(CronError::InvalidSchedule(
                "every_seconds must be >= 1".into(),
            ));
        }
        Ok(Self::Interval { every_seconds })
    }

    fn label(&self) -> String {
        match self {
            Self::Cron { cron } => cron.clone(),
            Self::Interval { every_seconds } => format!("every {every_seconds}s"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronJob {
    pub id: Uuid,
    /// Display / legacy: cron expr or `every Ns` for intervals.
    pub cron: String,
    #[serde(default)]
    pub every_seconds: Option<u64>,
    pub paused: bool,
    #[serde(default)]
    pub next_run_at_ms: i64,
    pub created_at_ms: i64,
    #[serde(default)]
    pub last_run_at_ms: Option<i64>,
    /// Last catalog mutation time (ms since epoch) for cluster LWW merge (CP6c).
    #[serde(default)]
    pub updated_at_ms: i64,
    pub request: ScheduledPublishRequest,
}

impl CronJob {
    pub fn schedule_kind(&self) -> ScheduleKind {
        if let Some(secs) = self.every_seconds {
            ScheduleKind::Interval {
                every_seconds: secs,
            }
        } else {
            ScheduleKind::Cron {
                cron: self.cron.clone(),
            }
        }
    }

    /// Publish idempotency key for one firing of this job, as returned by
    /// [`CronRegistry::pop_due`] (whose `next_run_at_ms` is the scheduled slot
    /// being fired, before advancing).
    ///
    /// Keyed on the slot rather than the tick time, so a retry after a failed
    /// publish, a replay after a crash, or a second scheduler firing the same
    /// slot all dedupe to one message. Dedup entries never expire, so the key
    /// must differ per slot: a user-supplied `idempotency_key` is kept only as
    /// a suffix, since reusing it verbatim would suppress every later firing.
    pub fn fire_idempotency_key(&self) -> String {
        let fire_ts = self.next_run_at_ms;
        match self.request.idempotency_key.as_deref() {
            Some(user_key) => format!("cron:{}:{}:{}", self.id, fire_ts, user_key),
            None => format!("cron:{}:{}", self.id, fire_ts),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CronFile {
    #[serde(default)]
    version: u16,
    #[serde(default)]
    sequence: u64,
    jobs: Vec<CronJob>,
}

#[derive(Clone)]
pub struct CronRegistry {
    path: PathBuf,
    journal: JsonlJournal,
    inner: Arc<parking_lot::Mutex<CronState>>,
    /// Exclusive ownership of the journal for the life of the registry.
    _lock: Arc<FileLock>,
}

struct CronState {
    file: CronFile,
    sequence: u64,
    journal_ops: usize,
    /// Failed-fire backoff per job (in memory only).
    retries: HashMap<Uuid, CronRetry>,
    /// Tentative occurrences popped but not yet committed: job id → fire
    /// time. Kept out of `file` so snapshots hold committed positions only.
    firing: HashMap<Uuid, i64>,
    /// Published occurrences whose completion barrier needs another attempt.
    completion_retries: std::collections::HashSet<Uuid>,
}

#[derive(Debug, Clone, Copy)]
struct CronRetry {
    attempts: u32,
    not_before_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)] // Persisted commands are infrequent and mirror the snapshot schema.
enum CronCommand {
    Upsert { job: CronJob },
    Delete { id: Uuid },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CronJournalRecord {
    version: u16,
    sequence: u64,
    command: CronCommand,
}

const CRON_JOURNAL_VERSION: u16 = 1;
const CRON_COMPACT_OPS: usize = 1024;

fn apply_command(file: &mut CronFile, command: CronCommand) {
    match command {
        CronCommand::Upsert { job } => {
            if let Some(slot) = file.jobs.iter_mut().find(|existing| existing.id == job.id) {
                *slot = job;
            } else {
                file.jobs.push(job);
            }
        }
        CronCommand::Delete { id } => file.jobs.retain(|job| job.id != id),
    }
}

impl CronRegistry {
    /// Open the node-local cron registry in `data_dir`. Never shared between
    /// processes: the journal is locked exclusively for the registry's lifetime.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, CronError> {
        let path = data_dir.as_ref().join("crons.json");
        let journal_path = path.with_extension("journal");
        let lock = crate::lock_journal(&journal_path)?
            .ok_or_else(|| CronError::Locked(journal_path.clone()))?;
        let journal = JsonlJournal::new(journal_path);
        let mut loaded = load_json_with_recovery(&path, || CronFile {
            version: CRON_JOURNAL_VERSION,
            sequence: 0,
            jobs: Vec::new(),
        })?;
        if !matches!(
            loaded.source,
            JsonLoadSource::Main | JsonLoadSource::Missing
        ) {
            info!(
                file = %path.display(),
                source = ?loaded.source,
                count = loaded.value.jobs.len(),
                "cron registry recovered after metadata read failure"
            );
        }
        if loaded.value.version != 0 && loaded.value.version != CRON_JOURNAL_VERSION {
            return Err(CronError::InvalidSchedule(format!(
                "unsupported cron snapshot version {}",
                loaded.value.version
            )));
        }
        loaded.value.version = CRON_JOURNAL_VERSION;
        let mut sequence = loaded.value.sequence;
        let mut journal_ops = 0usize;
        for line in journal.read_lines()? {
            let record = serde_json::from_slice::<CronJournalRecord>(&line)?;
            if record.version != CRON_JOURNAL_VERSION {
                return Err(CronError::InvalidSchedule(format!(
                    "unsupported cron journal version {}",
                    record.version
                )));
            }
            if record.sequence <= sequence {
                continue;
            }
            sequence = sequence.max(record.sequence);
            journal_ops += 1;
            apply_command(&mut loaded.value, record.command);
        }
        loaded.value.sequence = sequence;
        Ok(Self {
            path,
            journal,
            inner: Arc::new(parking_lot::Mutex::new(CronState {
                file: loaded.value,
                sequence,
                journal_ops,
                retries: HashMap::new(),
                firing: HashMap::new(),
                completion_retries: Default::default(),
            })),
            _lock: Arc::new(lock),
        })
    }

    /// Import cron jobs that older releases stored under the shared meta dir.
    ///
    /// Every node runs this, so whichever node holds the scheduler lease has
    /// the crons. Shared files are only read, never renamed, truncated or
    /// rewritten (other nodes import from them too). Jobs already local or
    /// recorded in this node's import ledger are skipped, so re-scans never
    /// resurrect a cron deleted here.
    pub fn import_legacy_shared(&self, shared_dir: &Path) -> Result<LegacyImport, CronError> {
        let mut report = LegacyImport::default();
        if !shared_dir.is_dir() || legacy::is_same_dir(shared_dir, &self.path) {
            return Ok(report);
        }
        let snapshot = shared_dir.join("crons.json");
        let journal = shared_dir.join("crons.journal");
        let mut found = CronFile {
            version: CRON_JOURNAL_VERSION,
            sequence: 0,
            jobs: Vec::new(),
        };
        if snapshot.exists() {
            found = serde_json::from_slice(&std::fs::read(&snapshot)?)?;
            report.sources.push(snapshot);
        }
        if journal.exists() {
            // Several writers shared this journal with colliding sequence
            // numbers: apply in file order instead of filtering by sequence.
            for line in JsonlJournal::new(&journal).read_lines_readonly()? {
                let record = serde_json::from_slice::<CronJournalRecord>(&line)?;
                if record.version != CRON_JOURNAL_VERSION {
                    return Err(CronError::InvalidSchedule(format!(
                        "unsupported cron journal version {}",
                        record.version
                    )));
                }
                apply_command(&mut found, record.command);
            }
            report.sources.push(journal);
        }
        if found.jobs.is_empty() {
            return Ok(report);
        }
        let mut ledger = legacy::Ledger::load(self.path.with_extension("imported.json"))?;
        let mut ledger_changed = false;
        {
            let mut state = self.inner.lock();
            for job in found.jobs {
                let local = state.file.jobs.iter().any(|existing| existing.id == job.id);
                if local || ledger.contains(&job.id) {
                    ledger_changed |= ledger.insert(job.id);
                    report.skipped += 1;
                    continue;
                }
                self.append_command(&mut state, CronCommand::Upsert { job: job.clone() })?;
                ledger_changed |= ledger.insert(job.id);
                apply_command(&mut state.file, CronCommand::Upsert { job });
                report.imported += 1;
            }
            self.maybe_compact(&mut state);
        }
        if ledger_changed {
            ledger.save()?;
        }
        legacy::record_imported_crons(report.imported);
        if report.imported > 0 {
            info!(
                shared_dir = %shared_dir.display(),
                imported = report.imported,
                skipped = report.skipped,
                "imported legacy shared cron jobs"
            );
        }
        Ok(report)
    }

    pub fn create(
        &self,
        cron_expr: &str,
        request: ScheduledPublishRequest,
    ) -> Result<CronJob, CronError> {
        self.create_with_kind(ScheduleKind::from_cron(cron_expr), request)
    }

    pub fn create_with_kind(
        &self,
        kind: ScheduleKind,
        request: ScheduledPublishRequest,
    ) -> Result<CronJob, CronError> {
        let job = Self::prepare_with_kind(kind, request)?;
        self.upsert(job.clone())?;
        Ok(job)
    }

    /// Build and validate a definition without persisting or scheduling it.
    pub fn prepare_with_kind(
        kind: ScheduleKind,
        request: ScheduledPublishRequest,
    ) -> Result<CronJob, CronError> {
        let now = Utc::now().timestamp_millis();
        let (cron, every_seconds) = match &kind {
            ScheduleKind::Cron { cron } => {
                parse_schedule(&cron_fields_for_parser(cron)?)?;
                (cron.clone(), None)
            }
            ScheduleKind::Interval { every_seconds } => {
                ScheduleKind::from_interval(*every_seconds)?;
                (kind.label(), Some(*every_seconds))
            }
        };

        let next_run_at_ms = next_run_for_job(&cron, every_seconds, now)?;

        let job = CronJob {
            id: Uuid::new_v4(),
            cron,
            every_seconds,
            paused: false,
            next_run_at_ms,
            created_at_ms: now,
            last_run_at_ms: None,
            updated_at_ms: now,
            request,
        };

        Ok(job)
    }

    /// Controller definitions contain no execution progress. Replaying one must
    /// not rewind a committed fire, even after an apply/checkpoint crash.
    pub fn apply_authoritative(
        &self,
        id: Uuid,
        definition: Option<CronJob>,
    ) -> Result<(), CronError> {
        let mut state = self.inner.lock();
        if definition.is_none() && !state.file.jobs.iter().any(|job| job.id == id) {
            return Ok(());
        }
        let command = if let Some(mut job) = definition {
            next_run_for_job(&job.cron, job.every_seconds, Utc::now().timestamp_millis())?;
            if let Some(existing) = state.file.jobs.iter().find(|j| j.id == id) {
                job.last_run_at_ms = existing.last_run_at_ms;
                job.next_run_at_ms = if existing.cron != job.cron
                    || existing.every_seconds != job.every_seconds
                    || (existing.paused && !job.paused)
                {
                    next_run_for_job(&job.cron, job.every_seconds, Utc::now().timestamp_millis())?
                } else {
                    existing.next_run_at_ms
                };
            } else {
                job.last_run_at_ms = None;
                job.next_run_at_ms =
                    next_run_for_job(&job.cron, job.every_seconds, Utc::now().timestamp_millis())?;
            }
            CronCommand::Upsert { job }
        } else {
            CronCommand::Delete { id }
        };
        self.append_command(&mut state, command.clone())?;
        apply_command(&mut state.file, command);
        self.maybe_compact(&mut state);
        Ok(())
    }

    pub fn get(&self, id: Uuid) -> Result<CronJob, CronError> {
        let state = self.inner.lock();
        state
            .file
            .jobs
            .iter()
            .find(|j| j.id == id)
            .cloned()
            .ok_or(CronError::NotFound(id))
    }

    pub fn list(&self) -> Vec<CronJob> {
        self.inner.lock().file.jobs.clone()
    }

    pub fn count(&self) -> usize {
        self.inner.lock().file.jobs.len()
    }

    pub fn pause(&self, id: Uuid) -> Result<CronJob, CronError> {
        self.set_paused(id, true)
    }

    pub fn resume(&self, id: Uuid) -> Result<CronJob, CronError> {
        let job = self.set_paused(id, false)?;
        let now = Utc::now().timestamp_millis();
        let next = next_run_for_job(&job.cron, job.every_seconds, now)?;
        self.update_next_run(id, next)
    }

    pub fn delete(&self, id: Uuid) -> Result<CronJob, CronError> {
        let mut state = self.inner.lock();
        let pos = state
            .file
            .jobs
            .iter()
            .position(|j| j.id == id)
            .ok_or(CronError::NotFound(id))?;
        let removed = state.file.jobs[pos].clone();
        self.append_command(&mut state, CronCommand::Delete { id })?;
        apply_command(&mut state.file, CronCommand::Delete { id });
        state.retries.remove(&id);
        state.firing.remove(&id);
        state.completion_retries.remove(&id);
        self.maybe_compact(&mut state);
        Ok(removed)
    }

    /// Insert or replace by `id` (cluster catalog sync, LWW).
    pub fn upsert(&self, mut job: CronJob) -> Result<(), CronError> {
        if job.updated_at_ms == 0 {
            job.updated_at_ms = Utc::now().timestamp_millis();
        }
        let mut state = self.inner.lock();
        if let Some(pos) = state.file.jobs.iter().position(|j| j.id == job.id) {
            if state.file.jobs[pos].updated_at_ms > job.updated_at_ms {
                return Ok(());
            }
        }
        self.append_command(&mut state, CronCommand::Upsert { job: job.clone() })?;
        apply_command(&mut state.file, CronCommand::Upsert { job });
        self.maybe_compact(&mut state);
        Ok(())
    }

    /// Jobs due now. Committed state is left untouched: the firing is held
    /// as a tentative occurrence until [`Self::commit_fire`] (after a
    /// successful publish) or [`Self::fail_fire`], so a compaction snapshot in
    /// between only ever contains committed positions. Jobs already firing or
    /// backing off after a failed fire are skipped. Each returned job is the
    /// committed snapshot (its `next_run_at_ms` is the slot being fired)
    /// stamped with `last_run_at_ms = now_ms`.
    pub fn pop_due(&self, now_ms: i64) -> Vec<CronJob> {
        let mut guard = self.inner.lock();
        let state = &mut *guard;
        let mut due = Vec::new();
        for job in &state.file.jobs {
            if job.paused || job.next_run_at_ms > now_ms || state.firing.contains_key(&job.id) {
                continue;
            }
            if state
                .retries
                .get(&job.id)
                .is_some_and(|retry| retry.not_before_ms > now_ms)
            {
                continue;
            }
            state.firing.insert(job.id, now_ms);
            let mut fired = job.clone();
            fired.last_run_at_ms = Some(now_ms);
            due.push(fired);
        }
        due
    }

    /// Commit a successful firing: advance `next_run_at_ms` past the fire time
    /// and persist it.
    ///
    /// Bumps `updated_at_ms` so the advanced position wins catalog LWW merges
    /// over stale peer copies that still hold the fired slot.
    pub fn commit_fire(&self, id: Uuid) -> Result<(), CronError> {
        let mut state = self.inner.lock();
        let mut job = state
            .file
            .jobs
            .iter()
            .find(|job| job.id == id)
            .cloned()
            .ok_or(CronError::NotFound(id))?;
        let fired_at_ms = state
            .firing
            .get(&id)
            .copied()
            .unwrap_or_else(|| Utc::now().timestamp_millis());
        match next_run_for_job(&job.cron, job.every_seconds, fired_at_ms) {
            Ok(next) => job.next_run_at_ms = next,
            Err(error) => {
                // No later run exists: stop instead of refiring every tick.
                tracing::warn!(cron_id = %id, %error, "cron has no upcoming run; pausing it");
                job.paused = true;
            }
        }
        job.last_run_at_ms = Some(fired_at_ms);
        job.updated_at_ms = Utc::now()
            .timestamp_millis()
            .max(job.updated_at_ms.saturating_add(1));
        self.append_command(&mut state, CronCommand::Upsert { job: job.clone() })?;
        apply_command(&mut state.file, CronCommand::Upsert { job });
        state.firing.remove(&id);
        state.completion_retries.remove(&id);
        state.retries.remove(&id);
        self.maybe_compact(&mut state);
        Ok(())
    }

    /// Commit a bounded batch of successful firings with one journal barrier.
    /// Tentative positions remain untouched until that barrier succeeds, so a
    /// crash or failed append can only replay the same deduplicated occurrence.
    pub fn commit_fires(&self, ids: &[Uuid]) -> Result<(), CronError> {
        let mut state = self.inner.lock();
        let ids: std::collections::HashSet<_> = ids.iter().copied().collect();
        state.completion_retries.extend(ids.iter().copied());
        let now = Utc::now().timestamp_millis();
        let mut updates = Vec::new();
        for (position, job) in state.file.jobs.iter().enumerate() {
            if !ids.contains(&job.id) {
                continue;
            }
            let Some(&fired_at_ms) = state.firing.get(&job.id) else {
                continue;
            };
            let mut job = job.clone();
            match next_run_for_job(&job.cron, job.every_seconds, fired_at_ms) {
                Ok(next) => job.next_run_at_ms = next,
                Err(error) => {
                    tracing::warn!(cron_id = %job.id, %error, "cron has no upcoming run; pausing it");
                    job.paused = true;
                }
            }
            job.last_run_at_ms = Some(fired_at_ms);
            job.updated_at_ms = now.max(job.updated_at_ms.saturating_add(1));
            updates.push((position, job));
        }
        let records: Vec<_> = updates
            .iter()
            .enumerate()
            .map(|(index, (_, job))| CronJournalRecord {
                version: CRON_JOURNAL_VERSION,
                sequence: state.sequence.saturating_add(index as u64 + 1),
                command: CronCommand::Upsert { job: job.clone() },
            })
            .collect();
        self.journal.append_all(&records)?;
        if let Some(last) = records.last() {
            state.sequence = last.sequence;
            state.file.sequence = last.sequence;
            state.journal_ops += records.len();
        }
        for (position, job) in updates {
            state.firing.remove(&job.id);
            state.retries.remove(&job.id);
            state.file.jobs[position] = job;
        }
        for id in ids {
            state.completion_retries.remove(&id);
        }
        self.maybe_compact(&mut state);
        Ok(())
    }

    /// Retry completion persistence without republishing successful occurrences.
    /// A poisoned journal still requires restart; known-safe transient failures
    /// can recover on a later scheduler tick in the same process.
    pub fn retry_pending_completions(&self, limit: usize) -> Result<usize, CronError> {
        let ids: Vec<_> = self
            .inner
            .lock()
            .completion_retries
            .iter()
            .take(limit)
            .copied()
            .collect();
        if ids.is_empty() {
            return Ok(0);
        }
        self.commit_fires(&ids)?;
        Ok(ids.len())
    }

    /// Record a failed publish of a job returned by [`Self::pop_due`].
    ///
    /// On [`FireRetry::Retry`] the tentative occurrence is dropped (nothing
    /// durable changed) and the slot is retried after an exponential backoff.
    /// On [`FireRetry::GiveUp`] (a permanent failure that kept failing) the
    /// occurrence is skipped: it is committed as fired and the job keeps its
    /// schedule.
    pub fn fail_fire(
        &self,
        job: &CronJob,
        failure: FireFailure,
        now_ms: i64,
    ) -> Result<FireRetry, CronError> {
        let decision = {
            let mut state = self.inner.lock();
            let attempt = state
                .retries
                .get(&job.id)
                .map_or(0, |retry| retry.attempts)
                .saturating_add(1);
            let decision = retry::decide(attempt, failure, now_ms);
            if let FireRetry::Retry { retry_at_ms, .. } = decision {
                state.firing.remove(&job.id);
                state.retries.insert(
                    job.id,
                    CronRetry {
                        attempts: attempt,
                        not_before_ms: retry_at_ms,
                    },
                );
            }
            decision
        };
        if let FireRetry::GiveUp { .. } = decision {
            self.commit_fire(job.id)?;
        }
        Ok(decision)
    }

    fn set_paused(&self, id: Uuid, paused: bool) -> Result<CronJob, CronError> {
        let mut state = self.inner.lock();
        let mut job = state
            .file
            .jobs
            .iter()
            .find(|j| j.id == id)
            .cloned()
            .ok_or(CronError::NotFound(id))?;
        job.paused = paused;
        let out = job.clone();
        self.append_command(&mut state, CronCommand::Upsert { job: job.clone() })?;
        apply_command(&mut state.file, CronCommand::Upsert { job });
        self.maybe_compact(&mut state);
        Ok(out)
    }

    fn update_next_run(&self, id: Uuid, next_run_at_ms: i64) -> Result<CronJob, CronError> {
        let mut state = self.inner.lock();
        let mut job = state
            .file
            .jobs
            .iter()
            .find(|j| j.id == id)
            .cloned()
            .ok_or(CronError::NotFound(id))?;
        job.next_run_at_ms = next_run_at_ms;
        let out = job.clone();
        self.append_command(&mut state, CronCommand::Upsert { job: job.clone() })?;
        apply_command(&mut state.file, CronCommand::Upsert { job });
        self.maybe_compact(&mut state);
        Ok(out)
    }

    fn append_command(&self, state: &mut CronState, command: CronCommand) -> Result<(), CronError> {
        let sequence = state.sequence.saturating_add(1);
        let record = CronJournalRecord {
            version: CRON_JOURNAL_VERSION,
            sequence,
            command,
        };
        self.journal.append(&record)?;
        state.sequence = sequence;
        state.file.sequence = sequence;
        state.journal_ops += 1;
        Ok(())
    }

    fn maybe_compact(&self, state: &mut CronState) {
        if state.journal_ops < CRON_COMPACT_OPS {
            return;
        }
        let Ok(bytes) = serde_json::to_vec_pretty(&state.file) else {
            return;
        };
        if persist_json_atomic(&self.path, &bytes).is_err() {
            return;
        }
        broker_storage::set_secret_file_mode(&self.path);
        match self.journal.reset() {
            Ok(()) => state.journal_ops = 0,
            Err(error) => tracing::warn!(%error, "cron journal compaction failed"),
        }
    }
}

fn next_run_for_job(
    cron: &str,
    every_seconds: Option<u64>,
    after_ms: i64,
) -> Result<i64, CronError> {
    if let Some(secs) = every_seconds {
        let step_ms = i64::try_from(secs)
            .ok()
            .and_then(|seconds| seconds.checked_mul(1000))
            .filter(|step| *step > 0)
            .ok_or_else(|| {
                CronError::InvalidSchedule(
                    "interval is outside the representable timestamp range".into(),
                )
            })?;
        return after_ms.checked_add(step_ms).ok_or_else(|| {
            CronError::InvalidSchedule("interval has no representable next run".into())
        });
    }
    next_run_after_cron(cron, after_ms)
}

/// Split an optional `CRON_TZ=` / `TZ=` prefix from a crontab expression.
/// Default timezone is UTC.
fn split_cron_timezone(expr: &str) -> Result<(chrono_tz::Tz, &str), CronError> {
    let expr = expr.trim();
    let rest = if let Some(rest) = expr
        .strip_prefix("CRON_TZ=")
        .or_else(|| expr.strip_prefix("cron_tz="))
    {
        rest
    } else if let Some(rest) = expr
        .strip_prefix("TZ=")
        .or_else(|| expr.strip_prefix("tz="))
    {
        rest
    } else {
        return Ok((chrono_tz::Tz::UTC, expr));
    };
    let (name, fields) = rest.split_once(char::is_whitespace).ok_or_else(|| {
        CronError::InvalidExpression(
            "CRON_TZ requires a timezone and a cron expression, e.g. CRON_TZ=America/New_York */5 * * * *".into(),
        )
    })?;
    let tz = name.parse::<chrono_tz::Tz>().map_err(|_| {
        CronError::InvalidExpression(format!(
            "unknown timezone {name}; use an IANA name like America/New_York (default is UTC)"
        ))
    })?;
    let fields = fields.trim();
    if fields.is_empty() {
        return Err(CronError::InvalidExpression(
            "cron expression missing after timezone".into(),
        ));
    }
    Ok((tz, fields))
}

/// Accept 5-field (`min hour dom month dow`) or 6-field (`sec min hour dom month dow`) cron.
/// Strips an optional `CRON_TZ=` prefix first.
pub fn normalize_cron(expr: &str) -> String {
    let fields = split_cron_timezone(expr)
        .map(|(_, fields)| fields.to_string())
        .unwrap_or_else(|_| expr.trim().to_string());
    normalize_cron_fields(&fields)
}

fn normalize_cron_fields(expr: &str) -> String {
    let parts: Vec<&str> = expr.split_whitespace().collect();
    match parts.len() {
        5 => format!(
            "0 {} {} {} {} {}",
            parts[0], parts[1], parts[2], parts[3], parts[4]
        ),
        _ => expr.to_string(),
    }
}

fn cron_fields_for_parser(expr: &str) -> Result<String, CronError> {
    let (_, fields) = split_cron_timezone(expr)?;
    Ok(normalize_cron_fields(fields))
}

fn parse_schedule(expr: &str) -> Result<Schedule, CronError> {
    Schedule::from_str(expr).map_err(|e| CronError::InvalidExpression(e.to_string()))
}

fn next_run_after_cron(expr: &str, after_ms: i64) -> Result<i64, CronError> {
    let (tz, fields) = split_cron_timezone(expr)?;
    let schedule = parse_schedule(&normalize_cron_fields(fields))?;
    let after_utc: DateTime<Utc> =
        DateTime::from_timestamp_millis(after_ms).unwrap_or_else(Utc::now);
    let after_local = after_utc.with_timezone(&tz);
    schedule
        .after(&after_local)
        .next()
        .map(|dt| dt.with_timezone(&Utc).timestamp_millis())
        .ok_or_else(|| CronError::InvalidExpression("no upcoming run".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_definition_replay_preserves_execution_progress() {
        let dir = tempfile::tempdir().unwrap();
        let registry = CronRegistry::open(dir.path()).unwrap();
        let definition = CronRegistry::prepare_with_kind(
            ScheduleKind::Interval { every_seconds: 1 },
            sample_request(),
        )
        .unwrap();
        assert!(registry.list().is_empty());
        registry
            .apply_authoritative(definition.id, Some(definition.clone()))
            .unwrap();
        registry.pop_due(definition.next_run_at_ms + 2000);
        registry.commit_fire(definition.id).unwrap();
        let before = registry.get(definition.id).unwrap();
        registry
            .apply_authoritative(definition.id, Some(definition.clone()))
            .unwrap();
        let after = registry.get(definition.id).unwrap();
        assert_eq!(after.next_run_at_ms, before.next_run_at_ms);
        assert_eq!(after.last_run_at_ms, before.last_run_at_ms);
    }
    use std::env::temp_dir;

    fn sample_request() -> ScheduledPublishRequest {
        ScheduledPublishRequest {
            topic: "q".into(),
            routing_key: "k".into(),
            payload: "x".into(),
            payload_encoding: None,
            idempotency_key: None,
            priority: None,
            parallelism: None,
            flow_id: None,
            queue_id: None,
            flow: None,
            destination: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        }
    }

    #[test]
    fn cron_tz_prefix_shifts_next_run() {
        let after = chrono::DateTime::parse_from_rfc3339("2024-01-15T12:00:00Z")
            .unwrap()
            .timestamp_millis();
        let utc = next_run_after_cron("0 9 * * *", after).unwrap();
        assert_eq!(
            utc,
            chrono::DateTime::parse_from_rfc3339("2024-01-16T09:00:00Z")
                .unwrap()
                .timestamp_millis()
        );
        let ny = next_run_after_cron("CRON_TZ=America/New_York 0 9 * * *", after).unwrap();
        assert_eq!(
            ny,
            chrono::DateTime::parse_from_rfc3339("2024-01-15T14:00:00Z")
                .unwrap()
                .timestamp_millis(),
            "09:00 EST is 14:00 UTC in January"
        );
    }

    #[test]
    fn unknown_cron_timezone_is_rejected() {
        assert!(next_run_after_cron("CRON_TZ=Not/AZone */5 * * * *", 0).is_err());
    }

    #[test]
    fn normalize_cron_strips_timezone_prefix() {
        assert_eq!(
            normalize_cron("CRON_TZ=America/New_York */5 * * * *"),
            "0 */5 * * * *"
        );
        assert_eq!(normalize_cron("*/5 * * * *"), "0 */5 * * * *");
    }

    #[test]
    fn create_pause_resume_delete() {
        let dir = temp_dir().join(format!("bettermq-cron-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        let job = reg.create("0 9 * * *", sample_request()).unwrap();
        assert!(!job.paused);
        assert!(job.every_seconds.is_none());
        reg.pause(job.id).unwrap();
        assert!(reg.get(job.id).unwrap().paused);
        reg.resume(job.id).unwrap();
        assert!(!reg.get(job.id).unwrap().paused);
        reg.delete(job.id).unwrap();
        assert!(reg.get(job.id).is_err());
    }

    #[test]
    fn interval_schedule_advances() {
        let dir = temp_dir().join(format!("bettermq-interval-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        let job = reg
            .create_with_kind(
                ScheduleKind::Interval { every_seconds: 10 },
                sample_request(),
            )
            .unwrap();
        assert_eq!(job.every_seconds, Some(10));
        assert_eq!(job.cron, "every 10s");

        let now = job.created_at_ms;
        let due = reg.pop_due(now + 10_000);
        assert_eq!(due.len(), 1);
        assert_eq!(
            reg.get(job.id).unwrap().next_run_at_ms,
            now + 10_000,
            "popping alone does not advance the committed position"
        );
        assert!(reg.pop_due(now + 10_000).is_empty(), "already firing");
        reg.commit_fire(job.id).unwrap();
        let updated = reg.get(job.id).unwrap();
        assert_eq!(updated.next_run_at_ms, now + 20_000);
    }

    #[test]
    fn fire_idempotency_key_differs_per_firing_even_with_user_key() {
        let dir = temp_dir().join(format!("bettermq-cron-key-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        let mut request = sample_request();
        request.idempotency_key = Some("user-key".into());
        let job = reg
            .create_with_kind(ScheduleKind::Interval { every_seconds: 10 }, request)
            .unwrap();

        let first = reg.pop_due(job.created_at_ms + 10_000).pop().unwrap();
        reg.commit_fire(first.id).unwrap();
        let second = reg.pop_due(job.created_at_ms + 20_000).pop().unwrap();

        let first_key = first.fire_idempotency_key();
        let second_key = second.fire_idempotency_key();
        assert_ne!(first_key, second_key);
        assert!(first_key.starts_with(&format!("cron:{}:", job.id)));
        assert!(first_key.ends_with(":user-key"));
        assert!(second_key.ends_with(":user-key"));

        let plain = reg
            .create_with_kind(
                ScheduleKind::Interval { every_seconds: 10 },
                sample_request(),
            )
            .unwrap();
        let fired = reg
            .pop_due(plain.created_at_ms + 10_000)
            .into_iter()
            .find(|due| due.id == plain.id)
            .unwrap();
        assert_eq!(
            fired.fire_idempotency_key(),
            format!("cron:{}:{}", plain.id, plain.created_at_ms + 10_000)
        );
    }

    #[test]
    fn failed_cron_fire_backs_off_then_gives_up_on_permanent_failure() {
        let dir = temp_dir().join(format!("bettermq-cron-retry-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        let job = reg
            .create_with_kind(
                ScheduleKind::Interval { every_seconds: 10 },
                sample_request(),
            )
            .unwrap();
        let slot = job.next_run_at_ms;
        let now = slot + 1;

        let fired = reg.pop_due(now).pop().unwrap();
        assert_eq!(
            reg.fail_fire(&fired, FireFailure::Transient, now).unwrap(),
            FireRetry::Retry {
                attempt: 1,
                retry_at_ms: now + crate::FIRE_RETRY_BASE_MS
            }
        );
        assert_eq!(reg.get(job.id).unwrap().next_run_at_ms, slot);
        assert!(reg.pop_due(now + crate::FIRE_RETRY_BASE_MS - 1).is_empty());

        let mut at = now + crate::FIRE_RETRY_BASE_MS;
        let mut outcome = None;
        for _ in 1..crate::PERMANENT_FAILURE_ATTEMPTS {
            let fired = reg.pop_due(at).pop().expect("due after backoff");
            let decision = reg.fail_fire(&fired, FireFailure::Permanent, at).unwrap();
            if let FireRetry::Retry { retry_at_ms, .. } = decision {
                at = retry_at_ms;
            }
            outcome = Some(decision);
        }
        assert_eq!(
            outcome,
            Some(FireRetry::GiveUp {
                attempt: crate::PERMANENT_FAILURE_ATTEMPTS
            })
        );
        // The occurrence is skipped but the cron keeps its schedule.
        let skipped = reg.get(job.id).unwrap();
        assert!(skipped.next_run_at_ms > at);
        assert!(!skipped.paused);
        let next = reg.pop_due(skipped.next_run_at_ms).pop().unwrap();
        assert!(matches!(
            reg.fail_fire(&next, FireFailure::Transient, skipped.next_run_at_ms)
                .unwrap(),
            FireRetry::Retry { attempt: 1, .. }
        ));
    }

    #[test]
    fn retried_fire_reuses_the_slot_idempotency_key() {
        let dir = temp_dir().join(format!("bettermq-cron-slot-key-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        let job = reg
            .create_with_kind(
                ScheduleKind::Interval { every_seconds: 10 },
                sample_request(),
            )
            .unwrap();
        let slot = job.next_run_at_ms;

        let first = reg.pop_due(slot + 7).pop().unwrap();
        let retry_at = match reg.fail_fire(&first, FireFailure::Transient, slot + 7) {
            Ok(FireRetry::Retry { retry_at_ms, .. }) => retry_at_ms,
            other => panic!("unexpected {other:?}"),
        };
        let retried = reg.pop_due(retry_at + 123).pop().unwrap();
        assert_eq!(first.fire_idempotency_key(), retried.fire_idempotency_key());
        assert_eq!(
            retried.fire_idempotency_key(),
            format!("cron:{}:{}", job.id, slot)
        );

        // Crash between publish and commit: the reopened registry refires the
        // same slot under the same key.
        drop(reg);
        let reopened = CronRegistry::open(&dir).unwrap();
        let replayed = reopened.pop_due(retry_at + 999).pop().unwrap();
        assert_eq!(
            replayed.fire_idempotency_key(),
            first.fire_idempotency_key()
        );
    }

    #[test]
    fn commit_fire_bumps_updated_at_so_stale_catalog_copies_lose() {
        let dir = temp_dir().join(format!("bettermq-cron-bump-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        let job = reg
            .create_with_kind(
                ScheduleKind::Interval { every_seconds: 10 },
                sample_request(),
            )
            .unwrap();
        let mut stale = job.clone();
        stale.updated_at_ms -= 1;
        reg.pop_due(job.next_run_at_ms).pop().unwrap();
        reg.commit_fire(job.id).unwrap();
        let advanced = reg.get(job.id).unwrap();
        assert!(advanced.updated_at_ms > stale.updated_at_ms);

        reg.upsert(stale.clone()).unwrap();
        let after = reg.get(job.id).unwrap();
        assert_eq!(after.next_run_at_ms, advanced.next_run_at_ms);
        assert!(reg.pop_due(stale.next_run_at_ms).is_empty());
    }

    #[test]
    fn compaction_mid_batch_does_not_persist_unpublished_advances() {
        let dir = temp_dir().join(format!("bettermq-cron-m46-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        let first = reg
            .create_with_kind(
                ScheduleKind::Interval { every_seconds: 10 },
                sample_request(),
            )
            .unwrap();
        let second = reg
            .create_with_kind(
                ScheduleKind::Interval { every_seconds: 10 },
                sample_request(),
            )
            .unwrap();
        let now = first.next_run_at_ms.max(second.next_run_at_ms);
        let due = reg.pop_due(now);
        assert_eq!(due.len(), 2);

        // Committing the first fire crosses the compaction threshold.
        reg.inner.lock().journal_ops = CRON_COMPACT_OPS - 1;
        reg.commit_fire(first.id).unwrap();
        assert_eq!(reg.inner.lock().journal_ops, 0, "compacted");
        let snapshot: CronFile =
            serde_json::from_slice(&std::fs::read(dir.join("crons.json")).unwrap()).unwrap();
        let persisted = snapshot
            .jobs
            .iter()
            .find(|job| job.id == second.id)
            .unwrap();
        assert_eq!(persisted.next_run_at_ms, second.next_run_at_ms);

        // Crash before the second publish: it is still due, same slot.
        drop(reg);
        let reopened = CronRegistry::open(&dir).unwrap();
        let refired = reopened.pop_due(now);
        assert_eq!(refired.len(), 1);
        assert_eq!(refired[0].id, second.id);
        let original = due.iter().find(|job| job.id == second.id).unwrap();
        assert_eq!(
            refired[0].fire_idempotency_key(),
            original.fire_idempotency_key()
        );
        assert!(reopened.get(first.id).unwrap().next_run_at_ms > now);
    }

    #[test]
    fn failed_fire_writes_nothing_to_the_journal() {
        let dir = temp_dir().join(format!("bettermq-cron-nofsync-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        let job = reg
            .create_with_kind(
                ScheduleKind::Interval { every_seconds: 10 },
                sample_request(),
            )
            .unwrap();
        let before = std::fs::read(dir.join("crons.journal")).unwrap();
        let fired = reg.pop_due(job.next_run_at_ms).pop().unwrap();
        reg.fail_fire(&fired, FireFailure::Transient, job.next_run_at_ms)
            .unwrap();
        assert_eq!(std::fs::read(dir.join("crons.journal")).unwrap(), before);
    }

    #[test]
    fn bulk_completions_preserve_unpublished_occurrences_through_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let reg = CronRegistry::open(dir.path()).unwrap();
        let jobs: Vec<_> = (0..8)
            .map(|_| {
                reg.create_with_kind(
                    ScheduleKind::Interval { every_seconds: 10 },
                    sample_request(),
                )
                .unwrap()
            })
            .collect();
        let now = jobs.iter().map(|job| job.next_run_at_ms).max().unwrap();
        let due = reg.pop_due(now);
        reg.inner.lock().journal_ops = CRON_COMPACT_OPS - 1;
        reg.commit_fires(&jobs[..5].iter().map(|job| job.id).collect::<Vec<_>>())
            .unwrap();
        assert_eq!(reg.inner.lock().journal_ops, 0);
        drop(reg);
        let reg = CronRegistry::open(dir.path()).unwrap();
        let refired = reg.pop_due(now);
        assert_eq!(refired.len(), 3);
        for job in refired {
            let original = due.iter().find(|original| original.id == job.id).unwrap();
            assert_eq!(job.fire_idempotency_key(), original.fire_idempotency_key());
        }
        assert!(next_run_for_job("", Some(u64::MAX), now).is_err());
        assert!(next_run_for_job("", Some(0), now).is_err());
    }

    #[test]
    fn failed_bulk_append_cannot_publish_tentative_positions_in_memory() {
        let dir = tempfile::tempdir().unwrap();
        let reg = CronRegistry::open(dir.path()).unwrap();
        let job = reg
            .create_with_kind(
                ScheduleKind::Interval { every_seconds: 10 },
                sample_request(),
            )
            .unwrap();
        reg.pop_due(job.next_run_at_ms);
        let journal = dir.path().join("crons.journal");
        std::fs::rename(&journal, journal.with_extension("saved")).unwrap();
        std::fs::create_dir(&journal).unwrap();
        assert!(reg.commit_fires(&[job.id]).is_err());
        assert_eq!(reg.get(job.id).unwrap().next_run_at_ms, job.next_run_at_ms);
        assert!(reg.inner.lock().firing.contains_key(&job.id));
        assert_eq!(reg.retry_pending_completions(0).unwrap(), 0);
        std::fs::remove_dir(&journal).unwrap();
        std::fs::rename(journal.with_extension("saved"), &journal).unwrap();
        assert_eq!(reg.retry_pending_completions(256).unwrap(), 1);
        assert!(reg.inner.lock().completion_retries.is_empty());
        assert!(!reg.inner.lock().firing.contains_key(&job.id));
        let next = reg.get(job.id).unwrap().next_run_at_ms;
        assert!(next > job.next_run_at_ms);
        assert!(reg.pop_due(job.next_run_at_ms).is_empty());
        let due = reg.pop_due(next);
        assert_eq!(due.len(), 1, "later occurrences recover without restarting");
        assert_ne!(due[0].fire_idempotency_key(), job.fire_idempotency_key());
    }

    #[test]
    fn journal_replays_committed_fire_after_restart() {
        let dir = temp_dir().join(format!("bettermq-cron-restart-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        let job = reg
            .create_with_kind(
                ScheduleKind::Interval { every_seconds: 10 },
                sample_request(),
            )
            .unwrap();
        let due = reg.pop_due(job.created_at_ms + 10_000);
        assert_eq!(due.len(), 1);
        reg.commit_fire(job.id).unwrap();
        drop(reg);

        let reopened = CronRegistry::open(&dir).unwrap();
        let restored = reopened.get(job.id).unwrap();
        assert_eq!(restored.next_run_at_ms, job.created_at_ms + 20_000);
        assert_eq!(restored.last_run_at_ms, Some(job.created_at_ms + 10_000));
        assert!(dir.join("crons.journal").exists());
    }

    #[test]
    fn append_after_torn_journal_tail_survives_restart() {
        let dir = temp_dir().join(format!("bettermq-cron-torn-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        let first = reg.create("0 9 * * *", sample_request()).unwrap();
        drop(reg);
        let journal = dir.join("crons.journal");
        let mut bytes = std::fs::read(&journal).unwrap();
        bytes.extend_from_slice(br#"{"version":1,"sequence":2,"comm"#);
        std::fs::write(&journal, bytes).unwrap();

        let reg = CronRegistry::open(&dir).unwrap();
        let second = reg.create("0 10 * * *", sample_request()).unwrap();
        drop(reg);

        let reopened = CronRegistry::open(&dir).unwrap();
        assert!(reopened.get(first.id).is_ok());
        assert!(reopened.get(second.id).is_ok());
    }

    #[test]
    fn second_open_of_same_cron_journal_is_refused() {
        let dir = temp_dir().join(format!("bettermq-cron-lock-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        assert!(matches!(
            CronRegistry::open(&dir),
            Err(CronError::Locked(_))
        ));
        drop(reg);
        CronRegistry::open(&dir).unwrap();
    }

    #[test]
    fn legacy_shared_crons_import_on_every_node_without_resurrection() {
        let base = temp_dir().join(format!("bettermq-cron-legacy-{}", Uuid::new_v4()));
        let shared = base.join("shared");
        std::fs::create_dir_all(&shared).unwrap();
        let writer = CronRegistry::open(&shared).unwrap();
        let kept = writer.create("0 9 * * *", sample_request()).unwrap();
        let gone = writer.create("0 10 * * *", sample_request()).unwrap();
        writer.delete(gone.id).unwrap();
        drop(writer);
        let shared_bytes = std::fs::read(shared.join("crons.journal")).unwrap();

        for node in ["n1", "n2"] {
            let dir = base.join(node);
            std::fs::create_dir_all(&dir).unwrap();
            let reg = CronRegistry::open(&dir).unwrap();
            let report = reg.import_legacy_shared(&shared).unwrap();
            assert_eq!(report.imported, 1);
            let imported = reg.get(kept.id).unwrap();
            assert_eq!(imported.next_run_at_ms, kept.next_run_at_ms);
            assert!(reg.get(gone.id).is_err());

            // Deleted locally: a re-scan or restart must not bring it back.
            reg.delete(kept.id).unwrap();
            assert_eq!(reg.import_legacy_shared(&shared).unwrap().imported, 0);
            drop(reg);
            let reopened = CronRegistry::open(&dir).unwrap();
            assert_eq!(reopened.import_legacy_shared(&shared).unwrap().imported, 0);
            assert!(reopened.get(kept.id).is_err());
        }
        assert_eq!(
            std::fs::read(shared.join("crons.journal")).unwrap(),
            shared_bytes,
            "shared files are read-only"
        );
    }

    #[test]
    fn unsupported_journal_version_fails_closed() {
        let dir = temp_dir().join(format!("bettermq-cron-version-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("crons.json"), br#"{"jobs":[]}"#).unwrap();
        std::fs::write(
            dir.join("crons.journal"),
            br#"{"version":99,"sequence":1,"command":{"command":"delete","id":"00000000-0000-0000-0000-000000000000"}}"#,
        )
        .unwrap();
        assert!(matches!(
            CronRegistry::open(&dir),
            Err(CronError::InvalidSchedule(message))
                if message.contains("unsupported cron journal version")
        ));
    }
}
