use broker_storage::JsonlJournal;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

pub(crate) fn legacy_shared_path(data_dir: &Path, name: &str) -> std::io::Result<Option<PathBuf>> {
    let Ok(shared) = std::env::var("BETTERMQ_SHARED_META_DIR") else {
        return Ok(None);
    };
    if shared.trim().is_empty() {
        return Ok(None);
    }
    let dir = match PathBuf::from(shared).canonicalize() {
        Ok(dir) => dir,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if dir.canonicalize().ok() == data_dir.canonicalize().ok() {
        return Ok(None);
    }
    let path = dir.join(name);
    Ok((path.try_exists()? || path.with_extension("journal").try_exists()?).then_some(path))
}

#[derive(Debug, Error)]
pub(crate) enum CatalogJournalError {
    #[error("catalog journal io: {0}")]
    Io(#[from] std::io::Error),
    #[error("catalog journal serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("unsupported catalog journal version {saw}, expected {expected}")]
    UnsupportedVersion { saw: u16, expected: u16 },
}

#[derive(Debug, Serialize, Deserialize)]
struct JournalRecord<C> {
    version: u16,
    sequence: u64,
    command: C,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogJournal {
    journal: JsonlJournal,
    version: u16,
}

impl CatalogJournal {
    pub(crate) fn new(snapshot_path: &Path, version: u16) -> Self {
        Self {
            journal: JsonlJournal::new(snapshot_path.with_extension("journal")),
            version,
        }
    }

    pub(crate) fn path(&self) -> &Path {
        self.journal.path()
    }

    pub(crate) fn replay<C>(
        &self,
        snapshot_sequence: u64,
        mut apply: impl FnMut(C),
    ) -> Result<(u64, usize), CatalogJournalError>
    where
        C: DeserializeOwned,
    {
        let mut sequence = snapshot_sequence;
        let mut operations = 0usize;
        for line in self.journal.read_lines()? {
            let record = serde_json::from_slice::<JournalRecord<C>>(&line)?;
            if record.version != self.version {
                return Err(CatalogJournalError::UnsupportedVersion {
                    saw: record.version,
                    expected: self.version,
                });
            }
            if record.sequence <= sequence {
                continue;
            }
            sequence = record.sequence;
            operations += 1;
            apply(record.command);
        }
        Ok((sequence, operations))
    }

    /// Append one record. Callers must hold the catalog's exclusive writer
    /// lock: [`JsonlJournal::append`] repairs the tail to a record boundary
    /// first, so a fragment left by a crash (or a failed earlier append) is
    /// never glued onto the new record.
    pub(crate) fn append<C>(&self, sequence: u64, command: &C) -> Result<(), CatalogJournalError>
    where
        C: Serialize,
    {
        self.journal.append(&JournalRecord {
            version: self.version,
            sequence,
            command,
        })?;
        Ok(())
    }

    pub(crate) fn compact<T>(
        &self,
        snapshot_path: &Path,
        snapshot: &T,
    ) -> Result<(), CatalogJournalError>
    where
        T: Serialize,
    {
        let bytes = serde_json::to_vec_pretty(snapshot)?;
        broker_storage::atomic_write_file(snapshot_path, &bytes)?;
        broker_storage::set_secret_file_mode(snapshot_path);
        self.journal.reset()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct Command(u64);

    #[test]
    fn replay_skips_snapshot_covered_records_and_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("catalog.json");
        let journal = CatalogJournal::new(&snapshot, 1);
        journal.append(1, &Command(1)).unwrap();
        journal.append(2, &Command(2)).unwrap();
        let mut file = OpenOptions::new()
            .append(true)
            .open(snapshot.with_extension("journal"))
            .unwrap();
        file.write_all(br#"{"version":1"#).unwrap();
        file.sync_data().unwrap();

        let mut applied = Vec::new();
        let (sequence, operations) = journal
            .replay(1, |command: Command| applied.push(command.0))
            .unwrap();
        assert_eq!(sequence, 2);
        assert_eq!(operations, 1);
        assert_eq!(applied, vec![2]);
    }

    fn replay_all(journal: &CatalogJournal) -> Vec<u64> {
        let mut applied = Vec::new();
        journal
            .replay(0, |command: Command| applied.push(command.0))
            .unwrap();
        applied
    }

    #[test]
    fn append_after_torn_tail_truncates_fragment_and_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("catalog.json");
        let journal = CatalogJournal::new(&snapshot, 1);
        journal.append(1, &Command(1)).unwrap();
        let path = snapshot.with_extension("journal");
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(br#"{"version":1,"sequence":2,"comm"#)
            .unwrap();
        drop(file);
        assert_eq!(replay_all(&journal), vec![1]);

        journal.append(2, &Command(2)).unwrap();
        // A fresh handle models a restart after the post-recovery write.
        let reopened = CatalogJournal::new(&snapshot, 1);
        assert_eq!(replay_all(&reopened), vec![1, 2]);
        reopened.append(3, &Command(3)).unwrap();
        assert_eq!(replay_all(&reopened), vec![1, 2, 3]);
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 3);
    }

    #[test]
    fn append_keeps_complete_final_record_missing_its_newline() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("catalog.json");
        let journal = CatalogJournal::new(&snapshot, 1);
        journal.append(1, &Command(1)).unwrap();
        let path = snapshot.with_extension("journal");
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(br#"{"version":1,"sequence":2,"command":2}"#)
            .unwrap();
        drop(file);
        // replay already applies the unterminated record, so it must survive.
        assert_eq!(replay_all(&journal), vec![1, 2]);

        journal.append(3, &Command(3)).unwrap();
        assert_eq!(
            replay_all(&CatalogJournal::new(&snapshot, 1)),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn append_repairs_journal_that_is_only_a_fragment() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("catalog.json");
        let path = snapshot.with_extension("journal");
        std::fs::write(&path, br#"{"vers"#).unwrap();
        let journal = CatalogJournal::new(&snapshot, 1);
        assert!(replay_all(&journal).is_empty());
        journal.append(1, &Command(1)).unwrap();
        assert_eq!(replay_all(&CatalogJournal::new(&snapshot, 1)), vec![1]);
    }

    #[test]
    fn append_after_torn_tail_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("catalog.json");
        let journal = CatalogJournal::new(&snapshot, 1);
        journal.append(1, &Command(1)).unwrap();
        let mut file = OpenOptions::new()
            .append(true)
            .open(snapshot.with_extension("journal"))
            .unwrap();
        file.write_all(br#"{"version":1,"seq"#).unwrap();
        drop(file);

        journal.replay(0, |_: Command| {}).unwrap();
        journal.append(2, &Command(2)).unwrap();

        let mut applied = Vec::new();
        let (sequence, _) = CatalogJournal::new(&snapshot, 1)
            .replay(0, |command: Command| applied.push(command.0))
            .unwrap();
        assert_eq!(sequence, 2);
        assert_eq!(applied, vec![1, 2]);
    }
}
