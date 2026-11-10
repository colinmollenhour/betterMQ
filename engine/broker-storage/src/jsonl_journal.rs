//! Append-only JSON-lines journal shared by the metadata stores (catalogs,
//! schedules, crons, dispatch retry and lifecycle state).
//!
//! Each record is one JSON document followed by `\n`, written with a single
//! `write_all` and `sync_data`. A crash mid-append leaves at most one
//! unterminated fragment at the end of the file; [`JsonlJournal::read_lines`]
//! and [`JsonlJournal::append`] both cut that fragment off first, so a later
//! record is never glued onto it. A failed write is rolled back to the
//! previous record boundary; when that rollback or an fsync fails, the bytes
//! on disk are unknown and the journal refuses further appends until it is
//! re-read or reset.

use crate::meta::set_secret_file_mode;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct JsonlJournal {
    path: PathBuf,
    poisoned: Arc<AtomicBool>,
}

impl JsonlJournal {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            poisoned: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read every complete record line, skipping blank lines.
    ///
    /// An unterminated last line is the remains of a torn append. When it is
    /// not a complete JSON document it is truncated off the file (and the
    /// truncation is synced) so the next append starts on a clean line. A
    /// complete but unterminated document (written by older builds that sent
    /// the newline separately) is kept and terminated instead. Malformed
    /// *terminated* lines are returned as-is: they are corruption, not a torn
    /// write, and callers must fail on them.
    pub fn read_lines(&self) -> std::io::Result<Vec<Vec<u8>>> {
        let Some(bytes) = self.read_file()? else {
            return Ok(Vec::new());
        };
        let (mut lines, complete_len, tail) = split_records(&bytes);
        match tail {
            Tail::None => {}
            Tail::Complete(record) => {
                let mut file = OpenOptions::new().append(true).open(&self.path)?;
                file.write_all(b"\n")?;
                file.sync_data()?;
                lines.push(record);
            }
            Tail::Torn(torn_bytes) => {
                tracing::warn!(
                    path = %self.path.display(),
                    torn_bytes,
                    "truncating torn journal tail"
                );
                let file = OpenOptions::new().write(true).open(&self.path)?;
                file.set_len(complete_len as u64)?;
                file.sync_all()?;
            }
        }
        self.poisoned.store(false, Ordering::SeqCst);
        Ok(lines)
    }

    /// Read every complete record line without modifying the file (for
    /// journals owned by another process). A torn unterminated tail is
    /// ignored; a complete unterminated document is returned.
    pub fn read_lines_readonly(&self) -> std::io::Result<Vec<Vec<u8>>> {
        let Some(bytes) = self.read_file()? else {
            return Ok(Vec::new());
        };
        let (mut lines, _, tail) = split_records(&bytes);
        if let Tail::Complete(record) = tail {
            lines.push(record);
        }
        Ok(lines)
    }

    fn read_file(&self) -> std::io::Result<Option<Vec<u8>>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// True after an append whose write or fsync failed.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::SeqCst)
    }

    /// Append one record and its newline in a single write, then fsync.
    ///
    /// The tail is repaired to a record boundary first (callers hold the
    /// journal's writer lock), so this does not depend on an earlier
    /// [`Self::read_lines`]. A failed write is truncated back off. A failed
    /// fsync, or a write that cannot be rolled back, poisons the journal: the
    /// record may be partly or wholly on disk, so later appends fail until
    /// [`Self::read_lines`] repairs the tail or [`Self::reset`] empties the file.
    pub fn append<T: Serialize>(&self, record: &T) -> std::io::Result<()> {
        self.append_all(std::slice::from_ref(record))
    }

    /// [`Self::append`] for several records: one write and one fsync. A crash
    /// tears at most the final line.
    pub fn append_all<T: Serialize>(&self, records: &[T]) -> std::io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        if self.is_poisoned() {
            return Err(std::io::Error::other(format!(
                "journal {} is poisoned by an earlier failed append; restart to repair it",
                self.path.display()
            )));
        }
        let mut line = Vec::new();
        for record in records {
            serde_json::to_writer(&mut line, record)?;
            line.push(b'\n');
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.path)?;
        let end = self.repair_tail(&mut file)?;
        file.seek(SeekFrom::Start(end))?;
        if let Err(error) = file.write_all(&line) {
            if file.set_len(end).and_then(|()| file.sync_data()).is_err() {
                self.poisoned.store(true, Ordering::SeqCst);
            }
            return Err(error);
        }
        if let Err(error) = file.sync_data() {
            self.poisoned.store(true, Ordering::SeqCst);
            return Err(error);
        }
        set_secret_file_mode(&self.path);
        Ok(())
    }

    /// Make the file end on a record boundary and return its length. Costs
    /// one 1-byte read when the file already ends with a newline.
    fn repair_tail(&self, file: &mut File) -> std::io::Result<u64> {
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
        let (_, complete_len, tail) = split_records(&bytes);
        match tail {
            Tail::None => Ok(len),
            Tail::Complete(_) => {
                file.seek(SeekFrom::Start(len))?;
                file.write_all(b"\n")?;
                file.sync_data()?;
                Ok(len + 1)
            }
            Tail::Torn(torn_bytes) => {
                tracing::warn!(
                    path = %self.path.display(),
                    torn_bytes,
                    "truncating torn journal tail before append"
                );
                file.set_len(complete_len as u64)?;
                file.sync_all()?;
                Ok(complete_len as u64)
            }
        }
    }

    /// Empty the journal after its records were folded into a snapshot.
    pub fn reset(&self) -> std::io::Result<()> {
        let file: File = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        file.sync_data()?;
        self.poisoned.store(false, Ordering::SeqCst);
        Ok(())
    }
}

enum Tail {
    None,
    /// Unterminated but a complete JSON document.
    Complete(Vec<u8>),
    /// Unterminated fragment of this many bytes.
    Torn(usize),
}

/// Split into newline-terminated, non-blank lines plus the trailing bytes
/// after the last newline.
fn split_records(bytes: &[u8]) -> (Vec<Vec<u8>>, usize, Tail) {
    let complete_len = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    let lines = bytes[..complete_len]
        .split(|byte| *byte == b'\n')
        .filter(|line| !is_blank(line))
        .map(<[u8]>::to_vec)
        .collect();
    let rest = &bytes[complete_len..];
    let tail = if is_blank(rest) {
        Tail::None
    } else if serde_json::from_slice::<serde::de::IgnoredAny>(rest).is_ok() {
        Tail::Complete(rest.to_vec())
    } else {
        Tail::Torn(rest.len())
    };
    (lines, complete_len, tail)
}

fn is_blank(line: &[u8]) -> bool {
    line.iter().all(|byte| byte.is_ascii_whitespace())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, serde::Deserialize, Debug, PartialEq)]
    struct Rec {
        n: u32,
    }

    fn parse(lines: &[Vec<u8>]) -> Vec<Rec> {
        lines
            .iter()
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect()
    }

    #[test]
    fn missing_file_reads_empty() {
        let dir = tempfile::tempdir().unwrap();
        let journal = JsonlJournal::new(dir.path().join("x.journal"));
        assert!(journal.read_lines().unwrap().is_empty());
    }

    #[test]
    fn append_after_torn_tail_then_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.journal");
        let journal = JsonlJournal::new(&path);
        journal.append(&Rec { n: 1 }).unwrap();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(br#"{"n":"#).unwrap();
        drop(file);

        assert_eq!(parse(&journal.read_lines().unwrap()), vec![Rec { n: 1 }]);
        journal.append(&Rec { n: 2 }).unwrap();
        assert_eq!(
            parse(&JsonlJournal::new(&path).read_lines().unwrap()),
            vec![Rec { n: 1 }, Rec { n: 2 }]
        );
    }

    #[test]
    fn append_repairs_a_torn_tail_without_a_prior_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.journal");
        std::fs::write(&path, b"{\"n\":1}\n{\"n\":").unwrap();
        let journal = JsonlJournal::new(&path);
        journal.append(&Rec { n: 2 }).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"{\"n\":1}\n{\"n\":2}\n");

        std::fs::write(&path, b"{\"n\":1}").unwrap();
        journal.append(&Rec { n: 2 }).unwrap();
        assert_eq!(
            parse(&journal.read_lines().unwrap()),
            vec![Rec { n: 1 }, Rec { n: 2 }]
        );
    }

    #[test]
    fn append_all_writes_every_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.journal");
        let journal = JsonlJournal::new(&path);
        journal.append_all(&[Rec { n: 1 }, Rec { n: 2 }]).unwrap();
        journal.append_all::<Rec>(&[]).unwrap();
        assert_eq!(
            parse(&journal.read_lines().unwrap()),
            vec![Rec { n: 1 }, Rec { n: 2 }]
        );
    }

    #[test]
    fn complete_unterminated_record_is_kept_and_terminated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.journal");
        std::fs::write(&path, b"{\"n\":1}\n{\"n\":2}").unwrap();
        let journal = JsonlJournal::new(&path);
        assert_eq!(
            parse(&journal.read_lines().unwrap()),
            vec![Rec { n: 1 }, Rec { n: 2 }]
        );
        journal.append(&Rec { n: 3 }).unwrap();
        assert_eq!(
            parse(&journal.read_lines().unwrap()),
            vec![Rec { n: 1 }, Rec { n: 2 }, Rec { n: 3 }]
        );
    }

    #[test]
    fn readonly_read_leaves_torn_tail_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.journal");
        std::fs::write(&path, b"{\"n\":1}\n{\"n\"").unwrap();
        let journal = JsonlJournal::new(&path);
        assert_eq!(
            parse(&journal.read_lines_readonly().unwrap()),
            vec![Rec { n: 1 }]
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"{\"n\":1}\n{\"n\"");
    }

    #[test]
    fn zero_filled_tail_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.journal");
        std::fs::write(&path, b"{\"n\":1}\n\0\0\0\0").unwrap();
        let journal = JsonlJournal::new(&path);
        assert_eq!(parse(&journal.read_lines().unwrap()), vec![Rec { n: 1 }]);
        assert_eq!(std::fs::read(&path).unwrap(), b"{\"n\":1}\n");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn failed_append_poisons_later_appends() {
        // /dev/full accepts open() but every write fails with ENOSPC.
        let journal = JsonlJournal::new("/dev/full");
        assert!(journal.append(&Rec { n: 1 }).is_err());
        assert!(journal.is_poisoned());
        let error = journal.append(&Rec { n: 2 }).unwrap_err();
        assert!(error.to_string().contains("poisoned"));
        assert!(
            journal.clone().is_poisoned(),
            "clones share the poison flag"
        );
    }

    #[test]
    fn reset_empties_file() {
        let dir = tempfile::tempdir().unwrap();
        let journal = JsonlJournal::new(dir.path().join("x.journal"));
        journal.append(&Rec { n: 1 }).unwrap();
        journal.reset().unwrap();
        assert!(journal.read_lines().unwrap().is_empty());
    }
}
