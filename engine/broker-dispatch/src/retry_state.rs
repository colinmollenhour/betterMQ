use broker_storage::JsonlJournal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

const RETRY_STATE_VERSION: u16 = 1;
const RETRY_COMPACT_OPS: usize = 1024;

#[derive(Debug, Error)]
pub(crate) enum RetryStateError {
    #[error("retry state io: {0}")]
    Io(#[from] std::io::Error),
    #[error("retry state serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("unsupported retry state version: {0}")]
    UnsupportedVersion(u16),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct RetryKey {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub message_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RetryRecord {
    pub key: RetryKey,
    pub cursor_key: String,
    #[serde(default)]
    pub fifo_key: Option<String>,
    pub attempts: u32,
    pub due_at_ms: i64,
    pub last_error: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RetrySnapshot {
    #[serde(default)]
    sequence: u64,
    #[serde(default, with = "snapshot_entries")]
    records: HashMap<RetryKey, RetryRecord>,
    #[serde(skip)]
    journal_ops: usize,
    #[serde(skip)]
    fifo_index: HashMap<(String, String, u32), BTreeMap<u64, RetryKey>>,
}

// JSON object keys cannot represent RetryKey. Keep the in-memory index, but
// persist entries as an array. Accept old empty-object snapshots on upgrade.
pub(crate) mod snapshot_entries {
    use super::RetryKey;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::HashMap;

    pub fn serialize<S: Serializer, T: Serialize>(
        entries: &HashMap<RetryKey, T>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        entries.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
        deserializer: D,
    ) -> Result<HashMap<RetryKey, T>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Entries<T> {
            Array(Vec<(RetryKey, T)>),
            Legacy(HashMap<RetryKey, T>),
        }
        Ok(match Entries::deserialize(deserializer)? {
            Entries::Array(entries) => entries.into_iter().collect(),
            Entries::Legacy(entries) => entries,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum RetryEvent {
    Upsert { record: RetryRecord },
    Remove { key: RetryKey },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryJournalRecord {
    version: u16,
    sequence: u64,
    event: RetryEvent,
}

#[derive(Clone)]
pub(crate) struct RetryState {
    snapshot_path: PathBuf,
    journal: JsonlJournal,
    state: Arc<parking_lot::Mutex<RetrySnapshot>>,
    io_lock: Arc<parking_lot::Mutex<()>>,
}

impl RetryState {
    pub(crate) fn open(data_dir: &Path) -> Result<Self, RetryStateError> {
        let dir = data_dir.to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let snapshot_path = dir.join("dispatch-retries.json");
        let journal_path = dir.join("dispatch-retries.journal");
        let mut state = if snapshot_path.exists() {
            serde_json::from_slice(&std::fs::read(&snapshot_path)?)?
        } else {
            RetrySnapshot::default()
        };
        rebuild_index(&mut state);
        let journal = JsonlJournal::new(journal_path);
        for line in journal.read_lines()? {
            let record = serde_json::from_slice::<RetryJournalRecord>(&line)?;
            if record.version != RETRY_STATE_VERSION {
                return Err(RetryStateError::UnsupportedVersion(record.version));
            }
            if record.sequence <= state.sequence {
                continue;
            }
            state.sequence = record.sequence;
            apply_event(&mut state, record.event);
            state.journal_ops += 1;
        }
        Ok(Self {
            snapshot_path,
            journal,
            state: Arc::new(parking_lot::Mutex::new(state)),
            io_lock: Arc::new(parking_lot::Mutex::new(())),
        })
    }

    pub(crate) fn get(&self, key: &RetryKey) -> Option<RetryRecord> {
        self.state.lock().records.get(key).cloned()
    }

    pub(crate) fn pending(&self) -> Vec<RetryRecord> {
        self.state.lock().records.values().cloned().collect()
    }

    pub(crate) fn blocking_retry(
        &self,
        cursor_key: &str,
        flow_key: &str,
        partition: u32,
        offset: u64,
    ) -> Option<RetryRecord> {
        let state = self.state.lock();
        let (_, key) = state
            .fifo_index
            .get(&(cursor_key.into(), flow_key.into(), partition))?
            .range(..=offset)
            .next()?;
        state.records.get(key).cloned()
    }

    pub(crate) fn schedule(&self, record: RetryRecord) -> Result<(), RetryStateError> {
        let _guard = self.io_lock.lock();
        self.append(RetryEvent::Upsert {
            record: record.clone(),
        })?;
        apply_event(&mut self.state.lock(), RetryEvent::Upsert { record });
        self.maybe_compact()
    }

    pub(crate) fn clear(&self, key: &RetryKey) -> Result<(), RetryStateError> {
        let _guard = self.io_lock.lock();
        if !self.state.lock().records.contains_key(key) {
            return Ok(());
        }
        self.append(RetryEvent::Remove { key: key.clone() })?;
        apply_event(
            &mut self.state.lock(),
            RetryEvent::Remove { key: key.clone() },
        );
        self.maybe_compact()
    }

    /// Clear any retry record for `(topic, partition, offset)`.
    pub(crate) fn clear_offset(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), RetryStateError> {
        let keys: Vec<RetryKey> = self
            .state
            .lock()
            .records
            .keys()
            .filter(|key| key.topic == topic && key.partition == partition && key.offset == offset)
            .cloned()
            .collect();
        for key in keys {
            self.clear(&key)?;
        }
        Ok(())
    }

    fn append(&self, event: RetryEvent) -> Result<(), RetryStateError> {
        let sequence = self.state.lock().sequence.saturating_add(1);
        self.journal.append(&RetryJournalRecord {
            version: RETRY_STATE_VERSION,
            sequence,
            event,
        })?;
        let mut state = self.state.lock();
        state.sequence = sequence;
        state.journal_ops += 1;
        Ok(())
    }

    fn maybe_compact(&self) -> Result<(), RetryStateError> {
        let mut state = self.state.lock();
        if state.journal_ops < RETRY_COMPACT_OPS {
            return Ok(());
        }
        let bytes = serde_json::to_vec(&*state)?;
        broker_storage::atomic_write_file(&self.snapshot_path, &bytes)?;
        broker_storage::set_secret_file_mode(&self.snapshot_path);
        self.journal.reset()?;
        state.journal_ops = 0;
        Ok(())
    }
}

fn apply_event(state: &mut RetrySnapshot, event: RetryEvent) {
    let key = match &event {
        RetryEvent::Upsert { record } => &record.key,
        RetryEvent::Remove { key } => key,
    };
    if let Some(old) = state.records.get(key) {
        if let Some(flow) = &old.fifo_key {
            let lane = (old.cursor_key.clone(), flow.clone(), key.partition);
            if let Some(offsets) = state.fifo_index.get_mut(&lane) {
                offsets.remove(&key.offset);
                if offsets.is_empty() {
                    state.fifo_index.remove(&lane);
                }
            }
        }
    }
    match event {
        RetryEvent::Upsert { record } => {
            index_record(&mut state.fifo_index, &record);
            state.records.insert(record.key.clone(), record);
        }
        RetryEvent::Remove { key } => {
            state.records.remove(&key);
        }
    }
}

fn index_record(
    index: &mut HashMap<(String, String, u32), BTreeMap<u64, RetryKey>>,
    record: &RetryRecord,
) {
    if let Some(flow) = &record.fifo_key {
        index
            .entry((
                record.cursor_key.clone(),
                flow.clone(),
                record.key.partition,
            ))
            .or_default()
            .insert(record.key.offset, record.key.clone());
    }
}

fn rebuild_index(state: &mut RetrySnapshot) {
    for record in state.records.values() {
        index_record(&mut state.fifo_index, record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_deadline_and_attempt_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let key = RetryKey {
            topic: "orders".into(),
            partition: 2,
            offset: 9,
            message_id: Uuid::new_v4(),
        };
        let state = RetryState::open(dir.path()).unwrap();
        state
            .schedule(RetryRecord {
                key: key.clone(),
                cursor_key: "lane".into(),
                fifo_key: Some("fifo".into()),
                attempts: 2,
                due_at_ms: 42,
                last_error: "HTTP 503".into(),
            })
            .unwrap();
        drop(state);

        let reopened = RetryState::open(dir.path()).unwrap();
        let restored = reopened.get(&key).unwrap();
        assert_eq!(restored.attempts, 2);
        assert_eq!(restored.due_at_ms, 42);
        assert!(reopened.blocking_retry("lane", "fifo", 2, 10).is_some());
        assert!(reopened.blocking_retry("lane", "other", 2, 10).is_none());
        reopened.clear(&key).unwrap();
        assert!(reopened.blocking_retry("lane", "fifo", 2, 10).is_none());
        drop(reopened);
        assert!(RetryState::open(dir.path()).unwrap().get(&key).is_none());
    }

    #[test]
    fn nonempty_compaction_survives_restart_and_completion() {
        let dir = tempfile::tempdir().unwrap();
        let key = RetryKey {
            topic: "orders".into(),
            partition: 0,
            offset: 1,
            message_id: Uuid::new_v4(),
        };
        let state = RetryState::open(dir.path()).unwrap();
        for attempts in 0..=RETRY_COMPACT_OPS as u32 {
            state
                .schedule(RetryRecord {
                    key: key.clone(),
                    cursor_key: "lane".into(),
                    fifo_key: None,
                    attempts,
                    due_at_ms: 42,
                    last_error: "HTTP 503".into(),
                })
                .unwrap();
        }
        drop(state);
        let state = RetryState::open(dir.path()).unwrap();
        assert_eq!(state.get(&key).unwrap().attempts, RETRY_COMPACT_OPS as u32);
        state.clear(&key).unwrap();
        drop(state);
        assert!(RetryState::open(dir.path()).unwrap().pending().is_empty());
    }

    #[test]
    fn legacy_empty_snapshot_is_readable() {
        let snapshot: RetrySnapshot =
            serde_json::from_str(r#"{"sequence":1,"records":{}}"#).unwrap();
        assert!(snapshot.records.is_empty());
    }

    #[test]
    fn append_after_torn_tail_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let record = |offset| RetryRecord {
            key: RetryKey {
                topic: "orders".into(),
                partition: 0,
                offset,
                message_id: Uuid::new_v4(),
            },
            cursor_key: "lane".into(),
            fifo_key: None,
            attempts: 1,
            due_at_ms: 1,
            last_error: "HTTP 503".into(),
        };
        let first = record(1);
        let state = RetryState::open(dir.path()).unwrap();
        state.schedule(first.clone()).unwrap();
        drop(state);
        let journal = dir.path().join("dispatch-retries.journal");
        let mut bytes = std::fs::read(&journal).unwrap();
        bytes.extend_from_slice(br#"{"version":1,"seq"#);
        std::fs::write(&journal, bytes).unwrap();

        let state = RetryState::open(dir.path()).unwrap();
        let second = record(2);
        state.schedule(second.clone()).unwrap();
        drop(state);

        let reopened = RetryState::open(dir.path()).unwrap();
        assert!(reopened.get(&first.key).is_some());
        assert!(reopened.get(&second.key).is_some());
    }
}
