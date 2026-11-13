use crate::retry_state::RetryKey;
use broker_storage::JsonlJournal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

const LIFECYCLE_VERSION: u16 = 1;
const COMPACT_OPS: usize = 1024;

#[derive(Debug, Error)]
pub(crate) enum LifecycleError {
    #[error("lifecycle io: {0}")]
    Io(#[from] std::io::Error),
    #[error("lifecycle serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("unsupported lifecycle version: {0}")]
    UnsupportedVersion(u16),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DlqPhase {
    Prepared,
    DlqCommitted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DlqTransition {
    pub source: RetryKey,
    pub cursor_key: String,
    pub reason: String,
    pub phase: DlqPhase,
    pub updated_at_ms: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Snapshot {
    #[serde(default)]
    sequence: u64,
    #[serde(default, with = "crate::retry_state::snapshot_entries")]
    transitions: HashMap<RetryKey, DlqTransition>,
    #[serde(skip)]
    journal_ops: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum Event {
    Upsert { transition: DlqTransition },
    Remove { source: RetryKey },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    version: u16,
    sequence: u64,
    event: Event,
}

#[derive(Clone)]
pub(crate) struct LifecycleStore {
    snapshot_path: PathBuf,
    journal: JsonlJournal,
    state: Arc<parking_lot::Mutex<Snapshot>>,
    io_lock: Arc<parking_lot::Mutex<()>>,
}

impl LifecycleStore {
    pub(crate) fn open(data_dir: &Path) -> Result<Self, LifecycleError> {
        let dir = data_dir.to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let snapshot_path = dir.join("dispatch-lifecycle.json");
        let journal_path = dir.join("dispatch-lifecycle.journal");
        let mut state = if snapshot_path.exists() {
            serde_json::from_slice(&std::fs::read(&snapshot_path)?)?
        } else {
            Snapshot::default()
        };
        let journal = JsonlJournal::new(journal_path);
        for line in journal.read_lines()? {
            let record = serde_json::from_slice::<Record>(&line)?;
            if record.version != LIFECYCLE_VERSION {
                return Err(LifecycleError::UnsupportedVersion(record.version));
            }
            if record.sequence <= state.sequence {
                continue;
            }
            state.sequence = record.sequence;
            apply(&mut state, record.event);
            state.journal_ops += 1;
        }
        Ok(Self {
            snapshot_path,
            journal,
            state: Arc::new(parking_lot::Mutex::new(state)),
            io_lock: Arc::new(parking_lot::Mutex::new(())),
        })
    }

    pub(crate) fn pending(&self) -> Vec<DlqTransition> {
        self.state.lock().transitions.values().cloned().collect()
    }

    pub(crate) async fn prepare_async(
        &self,
        transition: DlqTransition,
    ) -> Result<(), LifecycleError> {
        let store = self.clone();
        broker_partition::run_blocking_io(move || store.prepare(transition))
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?
    }

    pub(crate) async fn mark_dlq_committed_async(
        &self,
        source: &RetryKey,
    ) -> Result<(), LifecycleError> {
        let store = self.clone();
        let source = source.clone();
        broker_partition::run_blocking_io(move || store.mark_dlq_committed(&source))
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?
    }

    pub(crate) async fn clear_async(&self, source: &RetryKey) -> Result<(), LifecycleError> {
        {
            if let Some(_guard) = self.io_lock.try_lock() {
                if !self.state.lock().transitions.contains_key(source) {
                    return Ok(());
                }
            }
        }
        let store = self.clone();
        let source = source.clone();
        broker_partition::run_blocking_io(move || store.clear(&source))
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?
    }

    pub(crate) fn prepare(&self, mut transition: DlqTransition) -> Result<(), LifecycleError> {
        let _guard = self.io_lock.lock();
        if self
            .state
            .lock()
            .transitions
            .get(&transition.source)
            .is_some_and(|existing| existing.phase == DlqPhase::DlqCommitted)
        {
            transition.phase = DlqPhase::DlqCommitted;
        }
        self.write(Event::Upsert {
            transition: transition.clone(),
        })?;
        self.state
            .lock()
            .transitions
            .insert(transition.source.clone(), transition);
        self.maybe_compact()
    }

    pub(crate) fn mark_dlq_committed(&self, source: &RetryKey) -> Result<(), LifecycleError> {
        let Some(mut transition) = self.state.lock().transitions.get(source).cloned() else {
            return Ok(());
        };
        transition.phase = DlqPhase::DlqCommitted;
        transition.updated_at_ms = chrono::Utc::now().timestamp_millis();
        self.prepare(transition)
    }

    pub(crate) fn clear(&self, source: &RetryKey) -> Result<(), LifecycleError> {
        let _guard = self.io_lock.lock();
        if !self.state.lock().transitions.contains_key(source) {
            return Ok(());
        }
        self.write(Event::Remove {
            source: source.clone(),
        })?;
        self.state.lock().transitions.remove(source);
        self.maybe_compact()
    }

    fn write(&self, event: Event) -> Result<(), LifecycleError> {
        let sequence = self.state.lock().sequence.saturating_add(1);
        self.journal.append(&Record {
            version: LIFECYCLE_VERSION,
            sequence,
            event,
        })?;
        let mut state = self.state.lock();
        state.sequence = sequence;
        state.journal_ops += 1;
        Ok(())
    }

    fn maybe_compact(&self) -> Result<(), LifecycleError> {
        // io_lock still serializes writers, but cached readers must not wait
        // for encoding, snapshot persistence or journal rotation.
        let snapshot = {
            let state = self.state.lock();
            if state.journal_ops < COMPACT_OPS {
                return Ok(());
            }
            Snapshot {
                sequence: state.sequence,
                transitions: state.transitions.clone(),
                ..Default::default()
            }
        };
        broker_storage::atomic_write_file(&self.snapshot_path, &serde_json::to_vec(&snapshot)?)?;
        broker_storage::set_secret_file_mode(&self.snapshot_path);
        self.journal.reset()?;
        self.state.lock().journal_ops = 0;
        Ok(())
    }
}

fn apply(state: &mut Snapshot, event: Event) {
    match event {
        Event::Upsert { transition } => {
            state
                .transitions
                .insert(transition.source.clone(), transition);
        }
        Event::Remove { source } => {
            state.transitions.remove(&source);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn async_cleanup_waits_for_preparation_without_blocking_the_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let store = LifecycleStore::open(dir.path()).unwrap();
        let transition = transition();
        let source = transition.source.clone();
        let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let writer = store.clone();
        let thread = std::thread::spawn(move || {
            let _guard = writer.io_lock.lock();
            locked_tx.send(()).unwrap();
            let released = release_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .is_ok();
            if released {
                writer
                    .write(Event::Upsert {
                        transition: transition.clone(),
                    })
                    .unwrap();
                writer
                    .state
                    .lock()
                    .transitions
                    .insert(transition.source.clone(), transition);
            }
            released
        });
        locked_rx.recv().unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut clear = Box::pin(store.clear_async(&source));
            assert!(tokio::time::timeout(std::time::Duration::from_millis(10), &mut clear).await.is_err(),
                "cleanup must await an in-progress preparation without blocking the runtime or returning early");
            release_tx.send(()).unwrap();
            clear.await.unwrap();
        });
        assert!(
            thread.join().unwrap(),
            "lifecycle cleanup blocked the runtime"
        );
        assert!(store.pending().is_empty());
        assert!(LifecycleStore::open(dir.path())
            .unwrap()
            .pending()
            .is_empty());
    }

    fn transition() -> DlqTransition {
        DlqTransition {
            source: RetryKey {
                topic: "orders".into(),
                partition: 0,
                offset: 7,
                message_id: Uuid::new_v4(),
            },
            cursor_key: "lane".into(),
            reason: "exhausted".into(),
            phase: DlqPhase::Prepared,
            updated_at_ms: 1,
        }
    }

    #[test]
    fn every_dlq_boundary_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let transition = transition();
        let source = transition.source.clone();
        let store = LifecycleStore::open(dir.path()).unwrap();
        store.prepare(transition).unwrap();
        drop(store);

        let store = LifecycleStore::open(dir.path()).unwrap();
        assert_eq!(store.pending()[0].phase, DlqPhase::Prepared);
        store.mark_dlq_committed(&source).unwrap();
        drop(store);

        let store = LifecycleStore::open(dir.path()).unwrap();
        assert_eq!(store.pending()[0].phase, DlqPhase::DlqCommitted);
        store.clear(&source).unwrap();
        drop(store);

        assert!(LifecycleStore::open(dir.path())
            .unwrap()
            .pending()
            .is_empty());
    }

    #[test]
    fn nonempty_compaction_survives_restart_and_completion() {
        let dir = tempfile::tempdir().unwrap();
        let transition = transition();
        let source = transition.source.clone();
        let store = LifecycleStore::open(dir.path()).unwrap();
        for _ in 0..=COMPACT_OPS {
            store.prepare(transition.clone()).unwrap();
        }
        drop(store);
        let store = LifecycleStore::open(dir.path()).unwrap();
        assert_eq!(store.pending()[0].source, source);
        store.mark_dlq_committed(&source).unwrap();
        drop(store);
        let store = LifecycleStore::open(dir.path()).unwrap();
        assert_eq!(store.pending()[0].phase, DlqPhase::DlqCommitted);
        store.clear(&source).unwrap();
        drop(store);
        assert!(LifecycleStore::open(dir.path())
            .unwrap()
            .pending()
            .is_empty());
    }

    #[test]
    fn concurrent_prepares_cannot_compact_unapplied_journal_entries() {
        let dir = tempfile::tempdir().unwrap();
        let store = LifecycleStore::open(dir.path()).unwrap();
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let store = store.clone();
                scope.spawn(move || {
                    for offset in 0..129 {
                        let mut entry = transition();
                        entry.source.offset = worker * 129 + offset;
                        store.prepare(entry).unwrap();
                    }
                });
            }
        });
        assert_eq!(store.pending().len(), 1032);
        drop(store);
        assert_eq!(
            LifecycleStore::open(dir.path()).unwrap().pending().len(),
            1032
        );
    }

    #[test]
    fn append_after_torn_tail_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let first = transition();
        let store = LifecycleStore::open(dir.path()).unwrap();
        store.prepare(first.clone()).unwrap();
        drop(store);
        let journal = dir.path().join("dispatch-lifecycle.journal");
        let mut bytes = std::fs::read(&journal).unwrap();
        bytes.extend_from_slice(br#"{"version":1,"sequ"#);
        std::fs::write(&journal, bytes).unwrap();

        let store = LifecycleStore::open(dir.path()).unwrap();
        let second = transition();
        store.prepare(second.clone()).unwrap();
        drop(store);

        let reopened = LifecycleStore::open(dir.path()).unwrap();
        let mut sources: Vec<_> = reopened
            .pending()
            .into_iter()
            .map(|transition| transition.source.message_id)
            .collect();
        sources.sort();
        let mut expected = vec![first.source.message_id, second.source.message_id];
        expected.sort();
        assert_eq!(sources, expected);
    }
}
