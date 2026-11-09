use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
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
    path: PathBuf,
    version: u16,
}

impl CatalogJournal {
    pub(crate) fn new(snapshot_path: &Path, version: u16) -> Self {
        Self {
            path: snapshot_path.with_extension("journal"),
            version,
        }
    }

    pub(crate) fn replay<C>(
        &self,
        snapshot_sequence: u64,
        mut apply: impl FnMut(C),
    ) -> Result<(u64, usize), CatalogJournalError>
    where
        C: DeserializeOwned,
    {
        if !self.path.exists() {
            return Ok((snapshot_sequence, 0));
        }
        let bytes = std::fs::read(&self.path)?;
        let lines: Vec<&[u8]> = bytes.split(|byte| *byte == b'\n').collect();
        let mut sequence = snapshot_sequence;
        let mut operations = 0usize;
        for (index, line) in lines.iter().enumerate() {
            if line.iter().all(|byte| byte.is_ascii_whitespace()) {
                continue;
            }
            let record = match serde_json::from_slice::<JournalRecord<C>>(line) {
                Ok(record) => record,
                Err(error) if index + 1 == lines.len() => {
                    tracing::warn!(%error, path = %self.path.display(), "ignoring torn catalog journal tail");
                    break;
                }
                Err(error) => return Err(error.into()),
            };
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
    /// lock: the journal tail is repaired to a record boundary first, so a
    /// fragment left by a crash (or a failed earlier append) is never glued
    /// onto the new record.
    pub(crate) fn append<C>(&self, sequence: u64, command: &C) -> Result<(), CatalogJournalError>
    where
        C: Serialize + DeserializeOwned,
    {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut line = serde_json::to_vec(&JournalRecord {
            version: self.version,
            sequence,
            command,
        })?;
        line.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.path)?;
        let end = self.repair_tail::<C>(&mut file)?;
        file.seek(SeekFrom::Start(end))?;
        if let Err(error) = file.write_all(&line).and_then(|()| file.sync_data()) {
            // Roll a partial write back so the journal still ends on a boundary.
            let _ = file.set_len(end).and_then(|()| file.sync_data());
            return Err(error.into());
        }
        broker_storage::set_secret_file_mode(&self.path);
        Ok(())
    }

    /// Make the journal end on a newline-terminated record boundary and return
    /// the resulting length. A final line that is a complete record (newline
    /// lost) is kept, because `replay` already applies it; anything else after
    /// the last newline is a torn write and is truncated durably.
    fn repair_tail<C>(&self, file: &mut File) -> Result<u64, CatalogJournalError>
    where
        C: DeserializeOwned,
    {
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(0);
        }
        let mut last = [0u8; 1];
        file.seek(SeekFrom::Start(len - 1))?;
        file.read_exact(&mut last)?;
        if last[0] == b'\n' {
            return Ok(len);
        }
        let mut bytes = Vec::with_capacity(len as usize);
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut bytes)?;
        let tail_start = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        let tail = &bytes[tail_start..];
        let complete = !tail.iter().all(|byte| byte.is_ascii_whitespace())
            && serde_json::from_slice::<JournalRecord<C>>(tail).is_ok();
        if complete {
            file.seek(SeekFrom::Start(len))?;
            file.write_all(b"\n")?;
            file.sync_data()?;
            return Ok(len + 1);
        }
        tracing::warn!(
            path = %self.path.display(),
            discarded_bytes = tail.len(),
            "truncating torn catalog journal tail before append"
        );
        file.set_len(tail_start as u64)?;
        file.sync_data()?;
        Ok(tail_start as u64)
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
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        file.sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
