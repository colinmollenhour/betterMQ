//! Exclusive file locks for shared-meta CAS updates (HA M2).

use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::path::Path;

/// RAII exclusive lock on `{path}.lock` (created if missing).
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
    pub fn exclusive(path: &Path) -> std::io::Result<Self> {
        let lock_path = Path::new(&format!("{}.lock", path.display())).to_path_buf();
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        file.lock_exclusive()?;
        Ok(Self { file })
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}
