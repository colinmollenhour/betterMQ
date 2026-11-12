//! Durable, versioned fan-out acceptance and per-member outcomes.

use broker_partition::{PublishRequest, PublishResponse};
use broker_raft_meta::ControllerCommand;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use thiserror::Error;
use uuid::Uuid;

mod writer;

const FANOUT_STATE_VERSION: u16 = 1;
const FANOUT_COMPACT_OPS: usize = 1024;
const FANOUT_COMPACT_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum FanoutStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("unsupported fanout state version: {0}")]
    UnsupportedVersion(u16),
    #[error("fanout command not found: {0}")]
    NotFound(Uuid),
    #[error("fanout authority: {0}")]
    Authority(String),
    #[error(
        "fanout command is too large for the cluster controller: {bytes} bytes (limit {limit})"
    )]
    TooLarge { bytes: usize, limit: usize },
    #[error("fanout outbox is full; retry later")]
    OutboxFull,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanoutMemberCommand {
    pub member_id: Uuid,
    #[serde(with = "member_request")]
    pub request: PublishRequest,
}

mod member_request {
    use super::PublishRequest;
    use serde::{Deserialize, Serialize};

    pub fn serialize<S: serde::Serializer>(
        request: &PublishRequest,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut value = serde_json::to_value(request).map_err(serde::ser::Error::custom)?;
        if request.payload.is_empty() {
            value
                .as_object_mut()
                .expect("request object")
                .remove("payload");
        }
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<PublishRequest, D::Error> {
        let mut value = serde_json::Value::deserialize(deserializer)?;
        if let Some(object) = value.as_object_mut() {
            object.entry("payload").or_insert_with(|| "".into());
        }
        serde_json::from_value(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanoutMemberOutcome {
    #[serde(default)]
    pub response: Option<PublishResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub attempts: u32,
    #[serde(default)]
    pub next_attempt_at_ms: Option<i64>,
    #[serde(default)]
    pub terminal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanoutCommand {
    pub id: Uuid,
    #[serde(default)]
    pub acceptance_hash: String,
    #[serde(default)]
    pub tenant_id: String,
    pub group_id: Uuid,
    pub created_at_ms: i64,
    #[serde(default)]
    pub updated_at_ms: i64,
    /// New commands keep one body; absent on legacy per-member commands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_payload: Option<std::sync::Arc<String>>,
    pub members: Vec<FanoutMemberCommand>,
    #[serde(default)]
    pub outcomes: HashMap<Uuid, FanoutMemberOutcome>,
    #[serde(default)]
    pub completed: bool,
    #[serde(default)]
    pub completed_at_ms: Option<i64>,
}

impl FanoutCommand {
    /// The accepted timestamp and original delay form a durable absolute deadline.
    pub(crate) fn request_for_attempt(&self, member: &FanoutMemberCommand) -> PublishRequest {
        let mut request = member.request.clone();
        if let Some(payload) = &self.shared_payload {
            request.payload = payload.as_ref().clone();
        }
        request.delay_ms = request.delay_ms.map(|delay| {
            let deadline = self
                .created_at_ms
                .saturating_add(i64::try_from(delay).unwrap_or(i64::MAX));
            deadline
                .saturating_sub(chrono::Utc::now().timestamp_millis())
                .max(0) as u64
        });
        request
    }
}

/// Member outcomes replicated by each controller update; the accepted command
/// itself is never resent. (The accepted command already stores a group's
/// shared body once, in `FanoutCommand::shared_payload`.)
#[derive(Serialize, Deserialize)]
struct FanoutProgress {
    #[serde(default)]
    outcomes: HashMap<Uuid, FanoutMemberOutcome>,
    #[serde(default)]
    completed: bool,
    #[serde(default)]
    completed_at_ms: Option<i64>,
    #[serde(default)]
    updated_at_ms: i64,
}

fn encode_accepted(command: &FanoutCommand) -> Result<String, serde_json::Error> {
    serde_json::to_string(command)
}

fn encode_progress(command: &FanoutCommand) -> Result<String, serde_json::Error> {
    serde_json::to_string(&FanoutProgress {
        outcomes: command.outcomes.clone(),
        completed: command.completed,
        completed_at_ms: command.completed_at_ms,
        updated_at_ms: command.updated_at_ms,
    })
}

/// Rebuild the full fan-out command from its controller outbox entry.
pub fn authoritative_command(
    entry: &broker_raft_meta::FanoutOutboxEntry,
) -> Result<FanoutCommand, serde_json::Error> {
    let mut command: FanoutCommand = serde_json::from_str(&entry.payload_json)?;
    if !entry.progress_json.is_empty() {
        let progress: FanoutProgress = serde_json::from_str(&entry.progress_json)?;
        command.outcomes = progress.outcomes;
        command.completed = progress.completed;
        command.completed_at_ms = progress.completed_at_ms;
        command.updated_at_ms = progress.updated_at_ms;
    }
    Ok(command)
}

fn controller_error(error: broker_raft_meta::ControllerError) -> FanoutStoreError {
    let message = error.to_string();
    if message.contains("fanout outbox is full") {
        FanoutStoreError::OutboxFull
    } else {
        FanoutStoreError::Authority(message)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct FanoutSnapshot {
    #[serde(default)]
    sequence: u64,
    #[serde(default)]
    commands: HashMap<Uuid, FanoutCommand>,
    #[serde(skip)]
    journal_ops: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum FanoutEvent {
    Accept {
        fanout: FanoutCommand,
    },
    RecordResult {
        fanout_id: Uuid,
        member_id: Uuid,
        outcome: FanoutMemberOutcome,
        #[serde(default)]
        updated_at_ms: i64,
    },
    Replace {
        fanout: FanoutCommand,
    },
    GarbageCollect {
        ids: Vec<Uuid>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FanoutJournalRecord {
    version: u16,
    sequence: u64,
    event: FanoutEvent,
}

#[derive(Debug, Clone)]
pub struct FanoutStore {
    snapshot_path: PathBuf,
    journal_path: PathBuf,
}

#[derive(Default)]
struct CachedSnapshot {
    state: FanoutSnapshot,
    offset: u64,
    snapshot_stamp: Option<(u64, std::time::SystemTime)>,
    initialized: bool,
    poisoned: bool,
    #[cfg(test)]
    gate: Option<(
        tokio::sync::oneshot::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
    #[cfg(test)]
    fail_next_sync: bool,
    #[cfg(test)]
    commit_epochs: u64,
}

static FANOUT_CACHE: OnceLock<parking_lot::Mutex<HashMap<PathBuf, CachedSnapshot>>> =
    OnceLock::new();
static FANOUT_WRITER: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

static ACTIVE_FANOUTS: OnceLock<parking_lot::Mutex<HashSet<Uuid>>> = OnceLock::new();

pub(crate) struct FanoutExecutionGuard {
    id: Uuid,
}

impl Drop for FanoutExecutionGuard {
    fn drop(&mut self) {
        ACTIVE_FANOUTS
            .get_or_init(Default::default)
            .lock()
            .remove(&self.id);
    }
}

pub(crate) fn try_begin_execution(id: Uuid) -> Option<FanoutExecutionGuard> {
    let acquired = ACTIVE_FANOUTS
        .get_or_init(Default::default)
        .lock()
        .insert(id);
    // Construct a guard only for an acquired entry, and after releasing the
    // registry lock. Eager `then_some` constructed/dropped a losing guard under
    // that same lock, deadlocking live publishing against outbox replay.
    acquired.then(|| FanoutExecutionGuard { id })
}

impl FanoutStore {
    #[cfg(test)]
    pub(crate) fn evict_cached_state(&self) {
        FANOUT_CACHE
            .get_or_init(Default::default)
            .lock()
            .remove(&self.journal_path);
    }

    /// Keep flock, serialization and fsync off runtime workers. Admission bounds
    /// the blocking writer queue; dropping a caller does not cancel an active write.
    async fn blocking<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Self) -> Result<T, FanoutStoreError> + Send + 'static,
    ) -> Result<T, FanoutStoreError> {
        let permit = FANOUT_WRITER
            .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| FanoutStoreError::Authority(error.to_string()))?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(store)
        })
        .await
        .map_err(|error| FanoutStoreError::Authority(error.to_string()))?
    }

    pub async fn get_async(&self, id: Uuid) -> Result<Option<FanoutCommand>, FanoutStoreError> {
        match writer::submit(self, writer::Operation::Get(id)).await? {
            writer::Outcome::Found(command) => Ok(command),
            _ => unreachable!("fanout get reply"),
        }
    }
    pub async fn accept_async(
        &self,
        command: FanoutCommand,
    ) -> Result<(FanoutCommand, bool), FanoutStoreError> {
        match writer::submit(self, writer::Operation::Accept(command)).await? {
            writer::Outcome::Accepted(command, inserted) => Ok((command, inserted)),
            _ => unreachable!("fanout accept reply"),
        }
    }
    pub async fn replace_async(
        &self,
        command: FanoutCommand,
    ) -> Result<FanoutCommand, FanoutStoreError> {
        match writer::submit(self, writer::Operation::Replace(command)).await? {
            writer::Outcome::Updated(command) => Ok(command),
            _ => unreachable!("fanout replace reply"),
        }
    }
    pub async fn record_results_async(
        &self,
        id: Uuid,
        results: Vec<(Uuid, FanoutMemberOutcome)>,
    ) -> Result<FanoutCommand, FanoutStoreError> {
        match writer::submit(self, writer::Operation::Results(id, results)).await? {
            writer::Outcome::Updated(command) => Ok(command),
            _ => unreachable!("fanout results reply"),
        }
    }

    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, FanoutStoreError> {
        let dir = std::env::var("BETTERMQ_SHARED_META_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.as_ref().to_path_buf());
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            snapshot_path: dir.join("fanout-state.json"),
            journal_path: dir.join("fanout-state.journal"),
        })
    }

    #[cfg(test)]
    pub fn accept(&self, fanout: FanoutCommand) -> Result<(FanoutCommand, bool), FanoutStoreError> {
        match self
            .execute_group(vec![writer::Operation::Accept(fanout)])?
            .remove(0)?
        {
            writer::Outcome::Accepted(command, inserted) => Ok((command, inserted)),
            _ => unreachable!("fanout accept reply"),
        }
    }

    #[cfg(test)]
    pub fn record_result(
        &self,
        fanout_id: Uuid,
        member_id: Uuid,
        outcome: FanoutMemberOutcome,
    ) -> Result<FanoutCommand, FanoutStoreError> {
        self.record_results(fanout_id, vec![(member_id, outcome)])
    }

    #[cfg(test)]
    pub fn record_results(
        &self,
        id: Uuid,
        results: Vec<(Uuid, FanoutMemberOutcome)>,
    ) -> Result<FanoutCommand, FanoutStoreError> {
        match self
            .execute_group(vec![writer::Operation::Results(id, results)])?
            .remove(0)?
        {
            writer::Outcome::Updated(command) => Ok(command),
            _ => unreachable!("fanout results reply"),
        }
    }

    pub fn pending(&self, limit: usize) -> Result<Vec<FanoutCommand>, FanoutStoreError> {
        let _lock = broker_storage::FileLock::exclusive(&self.journal_path)?;
        let mut cache = FANOUT_CACHE.get_or_init(Default::default).lock();
        let state = self.load_state(cache.entry(self.journal_path.clone()).or_default())?;
        let mut commands: Vec<_> = state
            .commands
            .values()
            .filter(|command| !command.completed)
            .collect();
        commands.sort_by_key(|command| (command.updated_at_ms, command.created_at_ms, command.id));
        commands.truncate(limit);
        Ok(commands.into_iter().cloned().collect())
    }

    pub fn garbage_collect(
        &self,
        now_ms: i64,
        retention_ms: i64,
        max_completed: usize,
    ) -> Result<usize, FanoutStoreError> {
        let (removed, _) = self.mutate(|state| {
            let cutoff = now_ms.saturating_sub(retention_ms.max(0));
            let mut completed: Vec<_> = state
                .commands
                .values()
                .filter(|command| command.completed)
                .collect();
            completed.sort_by_key(|command| {
                Reverse((
                    command.completed_at_ms.unwrap_or(command.updated_at_ms),
                    command.id,
                ))
            });
            let ids: Vec<_> = completed
                .into_iter()
                .enumerate()
                .filter(|(index, command)| {
                    *index >= max_completed
                        || command.completed_at_ms.unwrap_or(command.updated_at_ms) <= cutoff
                })
                .map(|(_, command)| command.id)
                .collect();
            let count = ids.len();
            Ok((
                count,
                (!ids.is_empty()).then_some(FanoutEvent::GarbageCollect { ids }),
            ))
        })?;
        Ok(removed)
    }

    #[cfg(test)]
    pub fn get(&self, fanout_id: Uuid) -> Result<Option<FanoutCommand>, FanoutStoreError> {
        match self
            .execute_group(vec![writer::Operation::Get(fanout_id)])?
            .remove(0)?
        {
            writer::Outcome::Found(command) => Ok(command),
            _ => unreachable!("fanout get reply"),
        }
    }

    fn mutate<T>(
        &self,
        mutation: impl FnOnce(&FanoutSnapshot) -> Result<(T, Option<FanoutEvent>), FanoutStoreError>,
    ) -> Result<(T, bool), FanoutStoreError> {
        let _lock = broker_storage::FileLock::exclusive(&self.journal_path)?;
        let mut cache = FANOUT_CACHE.get_or_init(Default::default).lock();
        let cached = cache.entry(self.journal_path.clone()).or_default();
        let state = self.load_state(cached)?;
        let (result, event) = mutation(state)?;
        let Some(event) = event else {
            return Ok((result, false));
        };
        self.append_events(cached, vec![event])?;
        Ok((result, true))
    }

    fn snapshot_stamp(&self) -> Result<Option<(u64, std::time::SystemTime)>, FanoutStoreError> {
        match self.snapshot_path.metadata() {
            Ok(metadata) => Ok(Some((metadata.len(), metadata.modified()?))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn load_state<'a>(
        &self,
        cached: &'a mut CachedSnapshot,
    ) -> Result<&'a mut FanoutSnapshot, FanoutStoreError> {
        if cached.poisoned {
            return Err(FanoutStoreError::Authority(
                "fanout journal is fenced after an uncertain failure; restart to recover".into(),
            ));
        }
        let stamp = self.snapshot_stamp()?;
        let journal_len = self
            .journal_path
            .metadata()
            .map(|meta| meta.len())
            .unwrap_or(0);
        let reloading =
            !cached.initialized || cached.snapshot_stamp != stamp || journal_len < cached.offset;
        if reloading {
            cached.state = if stamp.is_some() {
                serde_json::from_slice(&std::fs::read(&self.snapshot_path)?)?
            } else {
                FanoutSnapshot::default()
            };
            cached.offset = 0;
            cached.snapshot_stamp = stamp;
            cached.initialized = true;
        }
        if !self.journal_path.exists() {
            return Ok(&mut cached.state);
        }
        let mut file = std::fs::File::open(&self.journal_path)?;
        if reloading {
            // Establish durability of recovered complete records before they
            // can turn a previously failed acceptance into a successful retry.
            file.sync_data()?;
        }
        file.seek(SeekFrom::Start(cached.offset))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let state = &mut cached.state;
        let lines: Vec<&[u8]> = bytes.split(|byte| *byte == b'\n').collect();
        let mut boundary = cached.offset as usize;
        for (index, line) in lines.iter().enumerate() {
            if line.iter().all(|byte| byte.is_ascii_whitespace()) {
                boundary += line.len() + usize::from(index + 1 < lines.len());
                continue;
            }
            let record = match serde_json::from_slice::<FanoutJournalRecord>(line) {
                Ok(record) => record,
                Err(error) if index + 1 == lines.len() => {
                    tracing::warn!(%error, "truncating torn fanout journal tail");
                    let file = OpenOptions::new().write(true).open(&self.journal_path)?;
                    file.set_len(boundary as u64)?;
                    file.sync_data()?;
                    break;
                }
                Err(error) => return Err(FanoutStoreError::Serde(error)),
            };
            if record.version != FANOUT_STATE_VERSION {
                return Err(FanoutStoreError::UnsupportedVersion(record.version));
            }
            boundary += line.len() + 1;
            // A complete JSON record can survive without its newline. Preserve it,
            // but restore the delimiter before a subsequent append.
            if index + 1 == lines.len() {
                let mut file = OpenOptions::new().append(true).open(&self.journal_path)?;
                file.write_all(b"\n")?;
                file.sync_data()?;
            }
            if record.sequence <= state.sequence {
                continue;
            }
            state.sequence = record.sequence;
            apply_event(state, record.event);
            state.journal_ops += 1;
        }
        cached.offset = self.journal_path.metadata()?.len();
        Ok(state)
    }

    fn maybe_compact(&self, state: &mut FanoutSnapshot) -> Result<(), FanoutStoreError> {
        let journal_bytes = self
            .journal_path
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if state.journal_ops < FANOUT_COMPACT_OPS && journal_bytes < FANOUT_COMPACT_BYTES {
            return Ok(());
        }
        let bytes = serde_json::to_vec(state)?;
        broker_storage::atomic_write_file(&self.snapshot_path, &bytes)?;
        broker_storage::set_secret_file_mode(&self.snapshot_path);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.journal_path)?;
        file.sync_data()?;
        state.journal_ops = 0;
        Ok(())
    }
}

pub async fn accept_authoritative(
    state: &crate::AppState,
    store: &FanoutStore,
    command: FanoutCommand,
) -> Result<FanoutCommand, FanoutStoreError> {
    let Some(controller) = state
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    else {
        return store.accept_async(command).await.map(|accepted| accepted.0);
    };
    if let Some(existing) = controller.state().fanout_outbox.get(&command.id) {
        if existing.tenant_id != command.tenant_id {
            return Err(FanoutStoreError::Authority(
                "fanout id belongs to another tenant".into(),
            ));
        }
        let authoritative = authoritative_command(existing)?;
        store.replace_async(authoritative.clone()).await?;
        return Ok(authoritative);
    }
    let payload_json = encode_accepted(&command)?;
    if payload_json.len() > broker_raft_meta::FANOUT_MAX_ENTRY_BYTES {
        return Err(FanoutStoreError::TooLarge {
            bytes: payload_json.len(),
            limit: broker_raft_meta::FANOUT_MAX_ENTRY_BYTES,
        });
    }
    let committed = controller
        .submit(ControllerCommand::AcceptFanout {
            id: command.id,
            tenant_id: command.tenant_id.clone(),
            acceptance_hash: command.acceptance_hash.clone(),
            payload_json,
            created_at_ms: command.created_at_ms,
            controller_term: 0,
        })
        .await
        .map_err(controller_error)?;
    let entry = committed
        .fanout_outbox
        .get(&command.id)
        .ok_or(FanoutStoreError::NotFound(command.id))?;
    let authoritative = authoritative_command(entry)?;
    store.replace_async(authoritative.clone()).await?;
    Ok(authoritative)
}

pub async fn claim_authority(
    state: &crate::AppState,
    store: &FanoutStore,
    fanout_id: Uuid,
) -> Result<Option<FanoutCommand>, FanoutStoreError> {
    let Some(cluster) = state.cluster.as_ref() else {
        return store.get_async(fanout_id).await;
    };
    let Some(controller) = cluster.runtime.controller() else {
        return store.get_async(fanout_id).await;
    };
    let snapshot = controller.state();
    let entry = snapshot
        .fanout_outbox
        .get(&fanout_id)
        .ok_or(FanoutStoreError::NotFound(fanout_id))?;
    if entry.completed {
        let command = authoritative_command(entry)?;
        store.replace_async(command.clone()).await?;
        return Ok(Some(command));
    }
    let owner_id = cluster.runtime.config().node_id;
    let committed = match controller
        .submit(ControllerCommand::ClaimFanout {
            id: fanout_id,
            owner_id,
            expected_version: entry.version,
            now_ms: 0,
            ttl_ms: 120_000,
            controller_term: 0,
        })
        .await
    {
        Ok(state) => state,
        Err(error) if error.to_string().contains("leased by another worker") => return Ok(None),
        Err(error) if error.to_string().contains("version fence failed") => return Ok(None),
        Err(error) => return Err(FanoutStoreError::Authority(error.to_string())),
    };
    let entry = committed
        .fanout_outbox
        .get(&fanout_id)
        .ok_or(FanoutStoreError::NotFound(fanout_id))?;
    if entry.owner_id != Some(owner_id) {
        return Ok(None);
    }
    let command = authoritative_command(entry)?;
    store.replace_async(command.clone()).await?;
    Ok(Some(command))
}

pub async fn update_authoritative(
    state: &crate::AppState,
    store: &FanoutStore,
    command: FanoutCommand,
) -> Result<FanoutCommand, FanoutStoreError> {
    let Some(cluster) = state.cluster.as_ref() else {
        return Ok(command);
    };
    let Some(controller) = cluster.runtime.controller() else {
        return Ok(command);
    };
    let owner_id = cluster.runtime.config().node_id;
    let entry = controller
        .state()
        .fanout_outbox
        .get(&command.id)
        .cloned()
        .ok_or(FanoutStoreError::NotFound(command.id))?;
    if entry.owner_id != Some(owner_id) {
        return Err(FanoutStoreError::Authority(
            "fanout ownership changed before update".into(),
        ));
    }
    let committed = controller
        .submit(ControllerCommand::UpdateFanout {
            id: command.id,
            owner_id,
            expected_version: entry.version,
            progress_json: encode_progress(&command)?,
            completed: command.completed,
            now_ms: 0,
            controller_term: 0,
        })
        .await
        .map_err(|error| FanoutStoreError::Authority(error.to_string()))?;
    let entry = committed
        .fanout_outbox
        .get(&command.id)
        .ok_or(FanoutStoreError::NotFound(command.id))?;
    let authoritative = authoritative_command(entry)?;
    store.replace_async(authoritative.clone()).await?;
    Ok(authoritative)
}

pub async fn pending_authoritative(
    state: &crate::AppState,
    store: &FanoutStore,
    limit: usize,
) -> Result<Vec<FanoutCommand>, FanoutStoreError> {
    let Some(controller) = state
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    else {
        return store.blocking(move |store| store.pending(limit)).await;
    };
    let mut pending: Vec<FanoutCommand> = Vec::new();
    for entry in controller.state().fanout_outbox.values() {
        if entry.completed {
            continue;
        }
        pending.push(authoritative_command(entry)?);
    }
    pending.sort_by_key(|command| (command.updated_at_ms, command.created_at_ms, command.id));
    pending.truncate(limit);
    Ok(pending)
}

pub async fn garbage_collect_authoritative(
    state: &crate::AppState,
    store: &FanoutStore,
    now_ms: i64,
    retention_ms: i64,
    max_completed: usize,
) -> Result<usize, FanoutStoreError> {
    let local_removed = store
        .blocking(move |store| store.garbage_collect(now_ms, retention_ms, max_completed))
        .await?;
    let Some(controller) = state
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    else {
        return Ok(local_removed);
    };
    let cutoff = now_ms.saturating_sub(retention_ms.max(0));
    let snapshot = controller.state();
    let mut completed: Vec<_> = snapshot
        .fanout_outbox
        .values()
        .filter(|entry| entry.completed)
        .collect();
    completed.sort_by_key(|entry| Reverse((entry.updated_at_ms, entry.id)));
    let ids: Vec<_> = completed
        .into_iter()
        .enumerate()
        .filter(|(index, entry)| *index >= max_completed || entry.updated_at_ms <= cutoff)
        .map(|(_, entry)| entry.id)
        .collect();
    if ids.is_empty() {
        return Ok(local_removed);
    }
    controller
        .submit(ControllerCommand::GarbageCollectFanout {
            ids: ids.clone(),
            completed_before_ms: cutoff,
            controller_term: 0,
        })
        .await
        .map_err(|error| FanoutStoreError::Authority(error.to_string()))?;
    Ok(local_removed.saturating_add(ids.len()))
}

fn apply_event(state: &mut FanoutSnapshot, event: FanoutEvent) {
    match event {
        FanoutEvent::Accept { mut fanout } => {
            strip_replication_frames(&mut fanout);
            state.commands.entry(fanout.id).or_insert(fanout);
        }
        FanoutEvent::RecordResult {
            fanout_id,
            member_id,
            mut outcome,
            updated_at_ms,
        } => {
            if let Some(response) = &mut outcome.response {
                response.replication_frame = None;
            }
            if let Some(fanout) = state.commands.get_mut(&fanout_id) {
                fanout.outcomes.insert(member_id, outcome);
                fanout.updated_at_ms = updated_at_ms;
                fanout.completed = fanout.members.iter().all(|member| {
                    fanout
                        .outcomes
                        .get(&member.member_id)
                        .is_some_and(|outcome| outcome.response.is_some() || outcome.terminal)
                });
                if fanout.completed && fanout.completed_at_ms.is_none() {
                    fanout.completed_at_ms = Some(fanout.updated_at_ms);
                }
            }
        }
        FanoutEvent::Replace { mut fanout } => {
            strip_replication_frames(&mut fanout);
            state.commands.insert(fanout.id, fanout);
        }
        FanoutEvent::GarbageCollect { ids } => {
            state.commands.retain(|id, _| !ids.contains(id));
        }
    }
}

fn strip_replication_frames(command: &mut FanoutCommand) {
    for outcome in command.outcomes.values_mut() {
        if let Some(response) = &mut outcome.response {
            response.replication_frame = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execution_conflicts_do_not_drop_the_owners_guard_or_deadlock() {
        const CHILD: &str = "BETTERMQ_TEST_FANOUT_GUARD_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let id = Uuid::new_v4();
            let guard = try_begin_execution(id).unwrap();
            for _ in 0..16 {
                assert!(try_begin_execution(id).is_none());
            }
            let other = try_begin_execution(Uuid::new_v4()).unwrap();
            drop(other);
            assert!(try_begin_execution(id).is_none());
            drop(guard);
            let reacquired = try_begin_execution(id).unwrap();
            assert!(try_begin_execution(id).is_none());
            drop(reacquired);
            return;
        }

        // Isolate a regression's synchronous deadlock so the timeout can stop
        // it without wedging every test that shares the process-wide registry.
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "fanout::tests::execution_conflicts_do_not_drop_the_owners_guard_or_deadlock",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let status = tokio::time::timeout(std::time::Duration::from_secs(10), child.wait())
            .await
            .expect("conflicting fanout execution deadlocked")
            .unwrap();
        assert!(status.success());
    }

    fn request() -> PublishRequest {
        PublishRequest {
            routing_id: None,
            topic: "__direct".into(),
            queue_id: None,
            group_id: None,
            group_member_id: Some(Uuid::new_v4()),
            routing_key: "rk".into(),
            payload: "body".into(),
            payload_encoding: None,
            idempotency_key: Some("fanout-member".into()),
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: Some("https://example.com/hook".into()),
            secret: Some("secret".into()),
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        }
    }

    #[test]
    fn accepted_command_and_member_result_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let store = FanoutStore::open(dir.path()).unwrap();
        let fanout_id = Uuid::new_v4();
        let member_id = Uuid::new_v4();
        let command = FanoutCommand {
            id: fanout_id,
            acceptance_hash: "test".into(),
            tenant_id: "tenant-a".into(),
            group_id: Uuid::new_v4(),
            created_at_ms: 1,
            updated_at_ms: 1,
            shared_payload: None,
            members: vec![FanoutMemberCommand {
                member_id,
                request: request(),
            }],
            outcomes: HashMap::new(),
            completed: false,
            completed_at_ms: None,
        };
        assert!(store.accept(command.clone()).unwrap().1);
        // Crash during the next journal write, then recover and mutate again.
        let mut journal = OpenOptions::new()
            .append(true)
            .open(&store.journal_path)
            .unwrap();
        journal
            .write_all(br#"{"version":1,"sequence":2,"event":{"#)
            .unwrap();
        journal.sync_data().unwrap();
        let store = FanoutStore::open(dir.path()).unwrap();
        assert!(store.get(fanout_id).unwrap().is_some());
        store
            .record_result(
                fanout_id,
                member_id,
                FanoutMemberOutcome {
                    response: Some(PublishResponse {
                        publish_fence: None,
                        message_id: Some(Uuid::new_v4()),
                        topic: "__direct".into(),
                        partition: Some(0),
                        offset: Some(7),
                        duplicate: false,
                        scheduled: None,
                        commit_epoch: Some(8),
                        replication_frame: Some(vec![42; 128 * 1024]),
                    }),
                    error: None,
                    attempts: 1,
                    next_attempt_at_ms: None,
                    terminal: false,
                },
            )
            .unwrap();
        assert!(
            store.get(fanout_id).unwrap().unwrap().outcomes[&member_id]
                .response
                .as_ref()
                .unwrap()
                .replication_frame
                .is_none(),
            "cached fan-out outcomes must not retain payload-sized replication frames"
        );
        store.evict_cached_state();
        drop(store);

        let reopened = FanoutStore::open(dir.path()).unwrap();
        let restored = reopened.get(fanout_id).unwrap().unwrap();
        assert!(restored.completed);
        assert_eq!(restored.outcomes[&member_id].attempts, 1);
        let (_, inserted) = reopened.accept(command).unwrap();
        assert!(!inserted);
    }

    #[test]
    fn controller_entry_stores_shared_body_once_and_outcomes_separately() {
        // Group publishes keep one shared body; members carry empty payloads.
        let members: Vec<_> = (0..8)
            .map(|_| FanoutMemberCommand {
                member_id: Uuid::new_v4(),
                request: PublishRequest {
                    payload: String::new(),
                    ..request()
                },
            })
            .collect();
        let mut command = FanoutCommand {
            id: Uuid::new_v4(),
            acceptance_hash: "hash".into(),
            tenant_id: "tenant".into(),
            group_id: Uuid::new_v4(),
            created_at_ms: 1,
            updated_at_ms: 1,
            shared_payload: Some(std::sync::Arc::new("p".repeat(64 * 1024))),
            members,
            outcomes: HashMap::new(),
            completed: false,
            completed_at_ms: None,
        };
        let payload_json = encode_accepted(&command).unwrap();
        assert!(payload_json.len() < 2 * 64 * 1024, "body stored once");
        command.outcomes.insert(
            command.members[0].member_id,
            FanoutMemberOutcome {
                response: None,
                error: Some("boom".into()),
                attempts: 2,
                next_attempt_at_ms: Some(9),
                terminal: false,
            },
        );
        command.updated_at_ms = 5;
        let progress_json = encode_progress(&command).unwrap();
        assert!(progress_json.len() < 1024, "updates never resend the body");
        let entry = broker_raft_meta::FanoutOutboxEntry {
            id: command.id,
            tenant_id: command.tenant_id.clone(),
            acceptance_hash: command.acceptance_hash.clone(),
            version: 3,
            authority_term: 1,
            owner_id: None,
            lease_expires_at_ms: 0,
            payload_json: payload_json.into(),
            progress_json,
            completed: false,
            created_at_ms: 1,
            updated_at_ms: 5,
        };
        let restored = authoritative_command(&entry).unwrap();
        assert_eq!(
            serde_json::to_value(&restored).unwrap(),
            serde_json::to_value(&command).unwrap()
        );

        // Entries written before outcomes were split carry the whole command.
        let legacy = broker_raft_meta::FanoutOutboxEntry {
            payload_json: serde_json::to_string(&command).unwrap().into(),
            progress_json: String::new(),
            ..entry
        };
        assert_eq!(
            serde_json::to_value(authoritative_command(&legacy).unwrap()).unwrap(),
            serde_json::to_value(&command).unwrap()
        );
    }

    #[test]
    fn unknown_journal_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("fanout-state.journal"),
            br#"{"version":99,"sequence":1,"event":{"command":"record_result","fanout_id":"00000000-0000-0000-0000-000000000000","member_id":"00000000-0000-0000-0000-000000000000","outcome":{"attempts":1}}}"#,
        )
        .unwrap();
        let store = FanoutStore::open(dir.path()).unwrap();
        assert!(matches!(
            store.get(Uuid::nil()),
            Err(FanoutStoreError::UnsupportedVersion(99))
        ));
    }

    #[test]
    fn malformed_interior_journal_record_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("fanout-state.journal"), b"{torn\n{}\n").unwrap();
        let store = FanoutStore::open(dir.path()).unwrap();
        assert!(matches!(
            store.get(Uuid::nil()),
            Err(FanoutStoreError::Serde(_))
        ));
    }

    #[tokio::test]
    async fn cached_reader_observes_external_append_and_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let store = FanoutStore::open(dir.path()).unwrap();
        let member_id = Uuid::new_v4();
        let command = FanoutCommand {
            id: Uuid::new_v4(),
            acceptance_hash: "external".into(),
            tenant_id: "tenant".into(),
            group_id: Uuid::new_v4(),
            created_at_ms: 1,
            updated_at_ms: 1,
            shared_payload: None,
            members: vec![FanoutMemberCommand {
                member_id,
                request: request(),
            }],
            outcomes: HashMap::new(),
            completed: false,
            completed_at_ms: None,
        };
        store.accept_async(command.clone()).await.unwrap();
        assert!(
            !store
                .get_async(command.id)
                .await
                .unwrap()
                .unwrap()
                .completed
        );
        // Another process appends under the same cross-process lock.
        {
            let _lock = broker_storage::FileLock::exclusive(&store.journal_path).unwrap();
            let record = FanoutJournalRecord {
                version: 1,
                sequence: 2,
                event: FanoutEvent::RecordResult {
                    fanout_id: command.id,
                    member_id,
                    outcome: FanoutMemberOutcome {
                        response: None,
                        error: Some("terminal".into()),
                        attempts: 1,
                        next_attempt_at_ms: None,
                        terminal: true,
                    },
                    updated_at_ms: 2,
                },
            };
            let mut file = OpenOptions::new()
                .append(true)
                .open(&store.journal_path)
                .unwrap();
            serde_json::to_writer(&mut file, &record).unwrap();
            file.write_all(b"\n").unwrap();
            file.sync_data().unwrap();
        }
        let restored = store.get_async(command.id).await.unwrap().unwrap();
        assert!(restored.completed);
        {
            let _lock = broker_storage::FileLock::exclusive(&store.journal_path).unwrap();
            let snapshot = FanoutSnapshot {
                sequence: 2,
                commands: HashMap::from([(command.id, restored)]),
                journal_ops: 0,
            };
            broker_storage::atomic_write_file(
                &store.snapshot_path,
                &serde_json::to_vec(&snapshot).unwrap(),
            )
            .unwrap();
            let file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&store.journal_path)
                .unwrap();
            file.sync_data().unwrap();
        }
        assert!(
            store
                .get_async(command.id)
                .await
                .unwrap()
                .unwrap()
                .completed
        );
    }

    #[test]
    fn pending_replay_and_completed_retention_are_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let store = FanoutStore::open(dir.path()).unwrap();
        let member_id = Uuid::new_v4();
        let fanout = FanoutCommand {
            id: Uuid::new_v4(),
            acceptance_hash: "test".into(),
            tenant_id: "tenant".into(),
            group_id: Uuid::new_v4(),
            created_at_ms: 1,
            updated_at_ms: 1,
            shared_payload: None,
            members: vec![FanoutMemberCommand {
                member_id,
                request: request(),
            }],
            outcomes: HashMap::new(),
            completed: false,
            completed_at_ms: None,
        };
        store.accept(fanout.clone()).unwrap();
        assert_eq!(store.pending(10).unwrap().len(), 1);
        store
            .record_result(
                fanout.id,
                member_id,
                FanoutMemberOutcome {
                    response: Some(PublishResponse {
                        publish_fence: None,
                        message_id: Some(Uuid::new_v4()),
                        topic: "__direct".into(),
                        partition: Some(0),
                        offset: Some(1),
                        duplicate: false,
                        scheduled: None,
                        commit_epoch: Some(2),
                        replication_frame: None,
                    }),
                    error: None,
                    attempts: 1,
                    next_attempt_at_ms: None,
                    terminal: false,
                },
            )
            .unwrap();
        assert!(store.pending(10).unwrap().is_empty());
        assert_eq!(store.garbage_collect(i64::MAX, 0, 0).unwrap(), 1);
        assert!(store.get(fanout.id).unwrap().is_none());
    }
}
