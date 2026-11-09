//! Tokio RocksDB Raft store, adapted from `openraft-rocksstore` 0.9.21
//! (MIT OR Apache-2.0). The upstream example depends on unmaintained `async-std`
//! only for `RwLock`; this copy uses `tokio::sync::RwLock` instead.

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use openraft::storage::LogState;
use openraft::storage::Snapshot;
use openraft::AnyError;
use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftStorage;
use openraft::RaftTypeConfig;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::TokioRuntime;
use openraft::Vote;
use rocksdb::ColumnFamily;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::Direction;
use rocksdb::Options;
use rocksdb::WriteBatch;
use rocksdb::WriteOptions;
use rocksdb::DB;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::controller::{
    apply_replicated, ControllerCommand, ControllerRejection, ControllerState, STATE_KEY,
};

pub type RocksNodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for `MemStore`.
    pub TypeConfig:
        D = RocksRequest,
        R = RocksResponse,
        NodeId = RocksNodeId,
        Node = BasicNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime
);

/// Replicated state-machine requests.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RocksRequest {
    /// Legacy full-value write. Older controllers replicated the whole
    /// controller state this way; entries still in the log are applied as-is.
    Set { key: String, value: String },
    /// Install the initial controller state. A no-op once any controller
    /// state has been committed, so racing bootstraps cannot overwrite it.
    ControllerBootstrap { state: Box<ControllerState> },
    /// One controller command, applied deterministically to the replicated
    /// controller state by every node's state machine.
    Controller { command: ControllerCommand },
}

/// Result of applying one entry.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RocksResponse {
    pub value: Option<String>,
    /// Set when a controller command was rejected by the state machine. The
    /// entry is still applied (as a no-op) so every replica agrees.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejected: Option<ControllerRejection>,
}

impl RocksResponse {
    fn empty() -> Self {
        Self {
            value: None,
            rejected: None,
        }
    }
}

/// Controller state staged while one batch of entries is applied, so later
/// entries in the batch see the effects of earlier ones before the batch is
/// committed.
#[derive(Default)]
struct StagedController {
    loaded: bool,
    state: Option<ControllerState>,
    dirty: bool,
    staged_json: Option<String>,
}

impl StagedController {
    fn state(&mut self, sm: &RocksStateMachine) -> StorageResult<&mut Option<ControllerState>> {
        if !self.loaded {
            let json = match self.staged_json.take() {
                Some(json) => Some(json),
                None => sm.get(STATE_KEY)?,
            };
            self.state = json
                .map(|json| serde_json::from_str(&json).map_err(sm_r_err))
                .transpose()?;
            self.loaded = true;
        }
        Ok(&mut self.state)
    }

    fn overwritten(&mut self, json: String) {
        *self = Self {
            staged_json: Some(json),
            ..Self::default()
        };
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RocksSnapshot {
    pub meta: SnapshotMeta<RocksNodeId, BasicNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/**
 * Here defines a state machine of the raft, this state represents a copy of the data
 * between each node. Note that we are using `serde` to serialize the `data`, which has
 * a implementation to be serialized. Note that for this test we set both the key and
 * value as String, but you could set any type of value that has the serialization impl.
 */
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SerializableRocksStateMachine {
    pub last_applied_log: Option<LogId<RocksNodeId>>,

    pub last_membership: StoredMembership<RocksNodeId, BasicNode>,

    /// Application data.
    pub data: BTreeMap<String, String>,
}

impl From<&RocksStateMachine> for SerializableRocksStateMachine {
    fn from(state: &RocksStateMachine) -> Self {
        let mut data = BTreeMap::new();

        let it = state
            .db
            .iterator_cf(state.cf_sm_data(), rocksdb::IteratorMode::Start);

        for item in it {
            let (key, value) = item.expect("invalid kv record");

            let key: &[u8] = &key;
            let value: &[u8] = &value;
            data.insert(
                String::from_utf8(key.to_vec()).expect("invalid key"),
                String::from_utf8(value.to_vec()).expect("invalid data"),
            );
        }
        Self {
            last_applied_log: state.get_last_applied_log().expect("last_applied_log"),
            last_membership: state.get_last_membership().expect("last_membership"),
            data,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RocksStateMachine {
    /// Application data.
    pub db: Arc<DB>,
}

fn sm_r_err<E: Error + 'static>(e: E) -> StorageError<RocksNodeId> {
    StorageIOError::read_state_machine(&e).into()
}
fn sm_w_err<E: Error + 'static>(e: E) -> StorageError<RocksNodeId> {
    StorageIOError::write_state_machine(&e).into()
}

impl RocksStateMachine {
    fn cf_sm_meta(&self) -> &ColumnFamily {
        self.db.cf_handle("sm_meta").unwrap()
    }

    fn cf_sm_data(&self) -> &ColumnFamily {
        self.db.cf_handle("sm_data").unwrap()
    }

    fn get_last_membership(&self) -> StorageResult<StoredMembership<RocksNodeId, BasicNode>> {
        self.db
            .get_cf(self.cf_sm_meta(), "last_membership".as_bytes())
            .map_err(sm_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .unwrap_or_else(|| Ok(StoredMembership::default()))
            })
    }

    fn stage_last_membership(
        &self,
        batch: &mut WriteBatch,
        membership: &StoredMembership<RocksNodeId, BasicNode>,
    ) -> StorageResult<()> {
        batch.put_cf(
            self.cf_sm_meta(),
            "last_membership".as_bytes(),
            serde_json::to_vec(membership).map_err(sm_w_err)?,
        );
        Ok(())
    }

    fn get_last_applied_log(&self) -> StorageResult<Option<LogId<RocksNodeId>>> {
        self.db
            .get_cf(self.cf_sm_meta(), "last_applied_log".as_bytes())
            .map_err(sm_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .transpose()
            })
    }

    fn stage_last_applied_log(
        &self,
        batch: &mut WriteBatch,
        log_id: &LogId<RocksNodeId>,
    ) -> StorageResult<()> {
        batch.put_cf(
            self.cf_sm_meta(),
            "last_applied_log".as_bytes(),
            serde_json::to_vec(log_id).map_err(sm_w_err)?,
        );
        Ok(())
    }

    /// Commit staged state-machine writes as one atomic, WAL-synced batch, so
    /// the applied log id never lands without the data and membership it
    /// covers (or the other way round).
    fn commit(&self, batch: WriteBatch) -> StorageResult<()> {
        let mut options = WriteOptions::default();
        options.set_sync(true);
        self.db.write_opt(batch, &options).map_err(sm_w_err)
    }

    /// Replace the whole state machine with a snapshot in one atomic batch.
    /// Keys that are absent from the snapshot are removed rather than left
    /// behind from the previous state.
    fn replace_with(&self, sm: SerializableRocksStateMachine) -> StorageResult<()> {
        let mut batch = WriteBatch::default();
        for item in self
            .db
            .iterator_cf(self.cf_sm_data(), rocksdb::IteratorMode::Start)
        {
            let (key, _) = item.map_err(sm_r_err)?;
            batch.delete_cf(self.cf_sm_data(), key);
        }
        for (key, value) in &sm.data {
            batch.put_cf(self.cf_sm_data(), key.as_bytes(), value.as_bytes());
        }
        match &sm.last_applied_log {
            Some(log_id) => self.stage_last_applied_log(&mut batch, log_id)?,
            None => batch.delete_cf(self.cf_sm_meta(), "last_applied_log".as_bytes()),
        }
        self.stage_last_membership(&mut batch, &sm.last_membership)?;
        self.commit(batch)
    }

    fn new(db: Arc<DB>) -> RocksStateMachine {
        Self { db }
    }

    pub fn get(&self, key: &str) -> StorageResult<Option<String>> {
        let key = key.as_bytes();
        self.db
            .get_cf(self.cf_sm_data(), key)
            .map(|value| value.map(|v| String::from_utf8(v).expect("invalid data")))
            .map_err(|e| StorageIOError::read(&e).into())
    }
}

#[derive(Debug)]
pub struct RocksStore {
    db: Arc<DB>,

    /// The Raft state machine.
    pub state_machine: RwLock<RocksStateMachine>,

    /// Test-only fault injection: fail the next state-machine commit after
    /// every write of the batch has been staged.
    #[cfg(test)]
    fail_next_commit: std::sync::atomic::AtomicBool,
}
type StorageResult<T> = Result<T, StorageError<RocksNodeId>>;

/// converts an id to a byte vector for storing in the database.
/// Note that we're using big endian encoding to ensure correct sorting of keys
fn id_to_bin(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id).unwrap();
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

/// Meta data of a raft-store.
///
/// In raft, except logs and state machine, the store also has to store several piece of metadata.
/// This sub mod defines the key-value pairs of these metadata.
mod meta {
    use openraft::ErrorSubject;
    use openraft::LogId;

    use super::RocksNodeId;
    use super::RocksSnapshot;

    /// Defines metadata key and value
    pub(crate) trait StoreMeta {
        /// The key used to store in rocksdb
        const KEY: &'static str;

        /// The type of the value to store
        type Value: serde::Serialize + serde::de::DeserializeOwned;

        /// The subject this meta belongs to, and will be embedded into the returned storage error.
        fn subject(v: Option<&Self::Value>) -> ErrorSubject<RocksNodeId>;
    }

    pub(crate) struct LastPurged {}
    pub(crate) struct SnapshotIndex {}
    pub(crate) struct Vote {}
    pub(crate) struct Snapshot {}

    impl StoreMeta for LastPurged {
        const KEY: &'static str = "last_purged_log_id";
        type Value = LogId<u64>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<RocksNodeId> {
            ErrorSubject::Store
        }
    }
    impl StoreMeta for SnapshotIndex {
        const KEY: &'static str = "snapshot_index";
        type Value = u64;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<RocksNodeId> {
            ErrorSubject::Store
        }
    }
    impl StoreMeta for Vote {
        const KEY: &'static str = "vote";
        type Value = openraft::Vote<RocksNodeId>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<RocksNodeId> {
            ErrorSubject::Vote
        }
    }
    impl StoreMeta for Snapshot {
        const KEY: &'static str = "snapshot";
        type Value = RocksSnapshot;

        fn subject(v: Option<&Self::Value>) -> ErrorSubject<RocksNodeId> {
            ErrorSubject::Snapshot(Some(v.unwrap().meta.signature()))
        }
    }
}

impl RocksStore {
    fn cf_meta(&self) -> &ColumnFamily {
        self.db.cf_handle("meta").unwrap()
    }

    fn cf_logs(&self) -> &ColumnFamily {
        self.db.cf_handle("logs").unwrap()
    }

    /// Get a store metadata.
    ///
    /// It returns `None` if the store does not have such a metadata stored.
    fn get_meta<M: meta::StoreMeta>(&self) -> Result<Option<M::Value>, StorageError<RocksNodeId>> {
        let v = self.db.get_cf(self.cf_meta(), M::KEY).map_err(|e| {
            StorageIOError::new(M::subject(None), ErrorVerb::Read, AnyError::new(&e))
        })?;

        let t = match v {
            None => None,
            Some(bytes) => Some(serde_json::from_slice(&bytes).map_err(|e| {
                StorageIOError::new(M::subject(None), ErrorVerb::Read, AnyError::new(&e))
            })?),
        };
        Ok(t)
    }

    /// Save a store metadata.
    fn put_meta<M: meta::StoreMeta>(
        &self,
        value: &M::Value,
    ) -> Result<(), StorageError<RocksNodeId>> {
        let json_value = serde_json::to_vec(value).map_err(|e| {
            StorageIOError::new(M::subject(Some(value)), ErrorVerb::Write, AnyError::new(&e))
        })?;

        self.db
            .put_cf(self.cf_meta(), M::KEY, json_value)
            .map_err(|e| {
                StorageIOError::new(M::subject(Some(value)), ErrorVerb::Write, AnyError::new(&e))
            })?;

        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for Arc<RocksStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        };

        let mut res = Vec::new();

        let it = self.db.iterator_cf(
            self.cf_logs(),
            rocksdb::IteratorMode::From(&start, Direction::Forward),
        );
        for item_res in it {
            let (id, val) = item_res.map_err(read_logs_err)?;

            let id = bin_to_id(&id);
            if !range.contains(&id) {
                break;
            }

            let entry: Entry<_> = serde_json::from_slice(&val).map_err(read_logs_err)?;

            assert_eq!(id, entry.log_id.index);

            res.push(entry);
        }
        Ok(res)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<RocksStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<RocksNodeId>> {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // Serialize the data of the state machine.
            let state_machine =
                SerializableRocksStateMachine::from(&*self.state_machine.read().await);
            data = serde_json::to_vec(&state_machine)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership;
        }

        // TODO: we probably want this to be atomic.
        let snapshot_idx: u64 = self.get_meta::<meta::SnapshotIndex>()?.unwrap_or_default() + 1;
        self.put_meta::<meta::SnapshotIndex>(&snapshot_idx)?;

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = RocksSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        self.put_meta::<meta::Snapshot>(&snapshot)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<TypeConfig> for Arc<RocksStore> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let last = self
            .db
            .iterator_cf(self.cf_logs(), rocksdb::IteratorMode::End)
            .next();

        let last_log_id = match last {
            None => None,
            Some(res) => {
                let (_log_index, entry_bytes) = res.map_err(read_logs_err)?;
                let ent = serde_json::from_slice::<Entry<TypeConfig>>(&entry_bytes)
                    .map_err(read_logs_err)?;
                Some(ent.log_id)
            }
        };

        let last_purged_log_id = self.get_meta::<meta::LastPurged>()?;

        let last_log_id = match last_log_id {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(
        &mut self,
        vote: &Vote<RocksNodeId>,
    ) -> Result<(), StorageError<RocksNodeId>> {
        self.put_meta::<meta::Vote>(vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RocksNodeId>>, StorageError<RocksNodeId>> {
        self.get_meta::<meta::Vote>()
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log<I>(&mut self, entries: I) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        for entry in entries {
            let id = id_to_bin(entry.log_id.index);
            assert_eq!(bin_to_id(&id), entry.log_id.index);
            self.db
                .put_cf(
                    self.cf_logs(),
                    id,
                    serde_json::to_vec(&entry).map_err(|e| StorageIOError::write_logs(&e))?,
                )
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<RocksNodeId>,
    ) -> StorageResult<()> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let from = id_to_bin(log_id.index);
        let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff);
        self.db
            .delete_range_cf(self.cf_logs(), &from, &to)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<RocksNodeId>,
    ) -> Result<(), StorageError<RocksNodeId>> {
        tracing::debug!("delete_log: [0, {:?}]", log_id);

        self.put_meta::<meta::LastPurged>(&log_id)?;

        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index + 1);
        self.db
            .delete_range_cf(self.cf_logs(), &from, &to)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<RocksNodeId>>,
            StoredMembership<RocksNodeId, BasicNode>,
        ),
        StorageError<RocksNodeId>,
    > {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.get_last_applied_log()?,
            state_machine.get_last_membership()?,
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<RocksResponse>, StorageError<RocksNodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let sm = self.state_machine.write().await;
        let mut batch = WriteBatch::default();
        let mut controller = StagedController::default();

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.stage_last_applied_log(&mut batch, &entry.log_id)?;

            match entry.payload {
                EntryPayload::Blank => res.push(RocksResponse::empty()),
                EntryPayload::Normal(ref req) => match req {
                    RocksRequest::Set { key, value } => {
                        batch.put_cf(sm.cf_sm_data(), key.as_bytes(), value.as_bytes());
                        if key == STATE_KEY {
                            controller.overwritten(value.clone());
                        }
                        res.push(RocksResponse {
                            value: Some(value.clone()),
                            rejected: None,
                        })
                    }
                    RocksRequest::ControllerBootstrap { state } => {
                        let current = controller.state(&sm)?;
                        if current.is_none() {
                            *current = Some(state.as_ref().clone());
                            controller.dirty = true;
                        }
                        res.push(RocksResponse::empty())
                    }
                    RocksRequest::Controller { command } => {
                        let rejected = match controller.state(&sm)? {
                            None => Some(ControllerRejection::InvalidCommand(
                                "controller state is not bootstrapped".into(),
                            )),
                            Some(state) => {
                                match apply_replicated(
                                    state,
                                    command.clone(),
                                    entry.log_id.leader_id.term,
                                ) {
                                    Ok(()) => {
                                        controller.dirty = true;
                                        None
                                    }
                                    Err(error) => Some(ControllerRejection::from(error)),
                                }
                            }
                        };
                        res.push(RocksResponse {
                            value: None,
                            rejected,
                        })
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.stage_last_membership(
                        &mut batch,
                        &StoredMembership::new(Some(entry.log_id), mem.clone()),
                    )?;
                    res.push(RocksResponse::empty())
                }
            };
        }
        if controller.dirty {
            if let Some(state) = &controller.state {
                let value = serde_json::to_string(state).map_err(sm_w_err)?;
                batch.put_cf(sm.cf_sm_data(), STATE_KEY.as_bytes(), value.as_bytes());
            }
        }
        #[cfg(test)]
        if self
            .fail_next_commit
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StorageIOError::write_state_machine(AnyError::error(
                "injected crash before state-machine commit",
            ))
            .into());
        }
        sm.commit(batch)?;
        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<RocksNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<RocksNodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<RocksNodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = RocksSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let updated_state_machine: SerializableRocksStateMachine =
                serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                    StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e)
                })?;
            let state_machine = self.state_machine.write().await;
            state_machine.replace_with(updated_state_machine)?;
        }

        self.put_meta::<meta::Snapshot>(&new_snapshot)?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RocksNodeId>> {
        let curr_snap = self.get_meta::<meta::Snapshot>()?;

        match curr_snap {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

impl RocksStore {
    pub async fn new<P: AsRef<Path>>(db_path: P) -> Arc<RocksStore> {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let meta = ColumnFamilyDescriptor::new("meta", Options::default());
        let sm_meta = ColumnFamilyDescriptor::new("sm_meta", Options::default());
        let sm_data = ColumnFamilyDescriptor::new("sm_data", Options::default());
        let logs = ColumnFamilyDescriptor::new("logs", Options::default());

        let db =
            DB::open_cf_descriptors(&db_opts, db_path, vec![meta, sm_meta, sm_data, logs]).unwrap();

        let db = Arc::new(db);
        let state_machine = RwLock::new(RocksStateMachine::new(db.clone()));
        Arc::new(RocksStore {
            db,
            state_machine,
            #[cfg(test)]
            fail_next_commit: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

fn read_logs_err(e: impl Error + 'static) -> StorageError<RocksNodeId> {
    StorageError::IO {
        source: StorageIOError::read_logs(&e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::CommittedLeaderId;
    use openraft::Membership;
    use std::collections::BTreeSet;

    fn entry(index: u64, payload: EntryPayload<TypeConfig>) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload,
        }
    }

    fn set(index: u64, value: &str) -> Entry<TypeConfig> {
        entry(
            index,
            EntryPayload::Normal(RocksRequest::Set {
                key: "k".into(),
                value: value.into(),
            }),
        )
    }

    #[tokio::test]
    async fn crash_before_commit_leaves_applied_index_and_data_consistent() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = RocksStore::new(dir.path()).await;
            store
                .apply_to_state_machine(&[set(1, "one")])
                .await
                .unwrap();
            store
                .fail_next_commit
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let membership = Membership::<RocksNodeId, BasicNode>::new(
                vec![BTreeSet::from([1])],
                BTreeMap::from([(1, BasicNode::new("http://n1"))]),
            );
            assert!(store
                .apply_to_state_machine(&[
                    set(2, "two"),
                    entry(3, EntryPayload::Membership(membership))
                ])
                .await
                .is_err());
        }
        let mut reopened = RocksStore::new(dir.path()).await;
        let (applied, membership) = reopened.last_applied_state().await.unwrap();
        assert_eq!(applied.map(|log_id| log_id.index), Some(1));
        assert_eq!(membership.log_id(), &None);
        let value = reopened.state_machine.read().await.get("k").unwrap();
        assert_eq!(value.as_deref(), Some("one"));
    }

    #[tokio::test]
    async fn installed_snapshot_replaces_previous_state_machine_keys() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = RocksStore::new(dir.path()).await;
        store
            .apply_to_state_machine(&[entry(
                1,
                EntryPayload::Normal(RocksRequest::Set {
                    key: "stale".into(),
                    value: "old".into(),
                }),
            )])
            .await
            .unwrap();
        let snapshot = SerializableRocksStateMachine {
            last_applied_log: Some(LogId::new(CommittedLeaderId::new(2, 1), 9)),
            last_membership: StoredMembership::default(),
            data: BTreeMap::from([("fresh".to_string(), "new".to_string())]),
        };
        let meta = SnapshotMeta {
            last_log_id: snapshot.last_applied_log,
            last_membership: StoredMembership::default(),
            snapshot_id: "test".into(),
        };
        store
            .install_snapshot(
                &meta,
                Box::new(Cursor::new(serde_json::to_vec(&snapshot).unwrap())),
            )
            .await
            .unwrap();
        let sm = store.state_machine.read().await;
        assert_eq!(sm.get("stale").unwrap(), None);
        assert_eq!(sm.get("fresh").unwrap().as_deref(), Some("new"));
        assert_eq!(
            sm.get_last_applied_log().unwrap().map(|id| id.index),
            Some(9)
        );
    }
}
