//! Exclusive file locks for shared-meta CAS updates (HA M2).

use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::time::SystemTime;

/// RAII lock on `{path}.lock` (created if missing).
pub struct FileLock {
    file: File,
}

impl FileLock {
    /// Bounded wait for metadata hot paths. Never recreate a missing mount.
    pub fn exclusive_timeout(path: &Path, timeout: std::time::Duration) -> std::io::Result<Self> {
        let lock_path = Path::new(&format!("{}.lock", path.display())).to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match file.try_lock_exclusive() {
                Ok(()) => return Ok(Self { file }),
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(2))
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "shared metadata flock deadline exceeded",
                    ))
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn open(path: &Path) -> std::io::Result<File> {
        let lock_path = Path::new(&format!("{}.lock", path.display())).to_path_buf();
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
    }

    pub fn exclusive(path: &Path) -> std::io::Result<Self> {
        let file = Self::open(path)?;
        file.lock_exclusive()?;
        Ok(Self { file })
    }

    /// Shared lock for readers that must not observe a writer's multi-file
    /// update half done. Never take it while this process holds `exclusive`
    /// on the same path: flock locks are per open file, so that self-deadlocks.
    pub fn shared(path: &Path) -> std::io::Result<Self> {
        let file = Self::open(path)?;
        file.lock_shared()?;
        Ok(Self { file })
    }

    /// Like [`Self::exclusive`] but returns `Ok(None)` instead of waiting
    /// when another process (or open file) already holds the lock.
    pub fn try_exclusive(path: &Path) -> std::io::Result<Option<Self>> {
        let file = Self::open(path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(Self { file })),
            Err(error) if error.kind() == fs2::lock_contended_error().kind() => Ok(None),
            Err(error) => Err(error),
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Cheap change detector for a metadata file: size, mtime and (on Unix)
/// device, inode and ctime. Atomic replacement changes the inode, appends
/// change the size, and any write changes ctime, so an unchanged fingerprint
/// lets callers reuse a parsed in-memory copy instead of re-reading the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileFingerprint(Option<FileStamp>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileStamp {
    len: u64,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    #[cfg(unix)]
    ctime: (i64, i64),
}

impl FileFingerprint {
    /// Fingerprint `path`; a missing file is a valid (distinct) state.
    pub fn of(path: &Path) -> std::io::Result<Self> {
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Self(None)),
            Err(error) => return Err(error),
        };
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Ok(Self(Some(FileStamp {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            dev: metadata.dev(),
            #[cfg(unix)]
            ino: metadata.ino(),
            #[cfg(unix)]
            ctime: (metadata.ctime(), metadata.ctime_nsec()),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_tracks_appends_and_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.json");
        let missing = FileFingerprint::of(&path).unwrap();
        std::fs::write(&path, b"one").unwrap();
        let written = FileFingerprint::of(&path).unwrap();
        assert_ne!(missing, written);
        assert_eq!(written, FileFingerprint::of(&path).unwrap());

        let replacement = dir.path().join("catalog.tmp");
        std::fs::write(&replacement, b"two").unwrap();
        std::fs::rename(&replacement, &path).unwrap();
        assert_ne!(written, FileFingerprint::of(&path).unwrap());
    }

    #[test]
    fn shared_locks_coexist() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.json");
        let first = FileLock::shared(&path).unwrap();
        let second = FileLock::shared(&path).unwrap();
        drop((first, second));
        FileLock::exclusive(&path).unwrap();
    }
}
