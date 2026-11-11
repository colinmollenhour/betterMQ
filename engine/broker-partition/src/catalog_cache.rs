//! Parsed catalog (snapshot + journal) kept in memory for hot-path lookups.
//!
//! Every lookup used to re-read the snapshot and replay the journal. The cache
//! revalidates with two `stat` calls instead, and reloads only when either
//! file changed (another process on a shared meta dir, or compaction).
//! Reloads run under the catalog's shared file lock, so a reader can never pair
//! a freshly compacted snapshot with the journal that compaction is about to
//! truncate.

use broker_storage::{FileFingerprint, FileLock};
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogFingerprint {
    snapshot: FileFingerprint,
    journal: FileFingerprint,
}

struct CachedCatalog<F> {
    fingerprint: CatalogFingerprint,
    state: Arc<F>,
    journal_ops: usize,
}

pub(crate) struct CatalogCache<F> {
    snapshot_path: PathBuf,
    journal_path: PathBuf,
    entry: RwLock<Option<CachedCatalog<F>>>,
}

impl<F> CatalogCache<F> {
    pub(crate) fn new(snapshot_path: &Path, journal_path: &Path) -> Self {
        Self {
            snapshot_path: snapshot_path.to_path_buf(),
            journal_path: journal_path.to_path_buf(),
            entry: RwLock::new(None),
        }
    }

    fn fingerprint(&self) -> std::io::Result<CatalogFingerprint> {
        Ok(CatalogFingerprint {
            snapshot: FileFingerprint::of(&self.snapshot_path)?,
            journal: FileFingerprint::of(&self.journal_path)?,
        })
    }

    fn cached(&self, fingerprint: &CatalogFingerprint) -> Option<(Arc<F>, usize)> {
        self.entry
            .read()
            .as_ref()
            .filter(|entry| &entry.fingerprint == fingerprint)
            .map(|entry| (Arc::clone(&entry.state), entry.journal_ops))
    }

    /// Reader path: no file lock unless the on-disk catalog changed.
    pub(crate) fn read<E>(&self, load: impl FnOnce() -> Result<(F, usize), E>) -> Result<Arc<F>, E>
    where
        E: From<std::io::Error>,
    {
        if let Some((state, _)) = self.cached(&self.fingerprint()?) {
            return Ok(state);
        }
        let _lock = FileLock::shared(&self.snapshot_path)?;
        self.reload(load).map(|(state, _)| state)
    }

    /// Writer path: the caller already holds the exclusive catalog lock.
    pub(crate) fn read_locked<E>(
        &self,
        load: impl FnOnce() -> Result<(F, usize), E>,
    ) -> Result<(Arc<F>, usize), E>
    where
        E: From<std::io::Error>,
    {
        self.reload(load)
    }

    fn reload<E>(&self, load: impl FnOnce() -> Result<(F, usize), E>) -> Result<(Arc<F>, usize), E>
    where
        E: From<std::io::Error>,
    {
        let fingerprint = self.fingerprint()?;
        if let Some(hit) = self.cached(&fingerprint) {
            return Ok(hit);
        }
        let (state, journal_ops) = load()?;
        let state = Arc::new(state);
        *self.entry.write() = Some(CachedCatalog {
            fingerprint,
            state: Arc::clone(&state),
            journal_ops,
        });
        Ok((state, journal_ops))
    }

    /// Publish the post-mutation state. Call while still holding the
    /// exclusive lock, after the journal append (and any compaction). The
    /// mutation is already durable, so a failed `stat` only drops the cache.
    pub(crate) fn store(&self, state: F, journal_ops: usize) {
        *self.entry.write() = self.fingerprint().ok().map(|fingerprint| CachedCatalog {
            fingerprint,
            state: Arc::new(state),
            journal_ops,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn load_counted<'a>(
        path: &Path,
        loads: &'a AtomicUsize,
    ) -> impl FnOnce() -> Result<(String, usize), std::io::Error> + 'a {
        let path = path.to_path_buf();
        move || {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok((std::fs::read_to_string(path)?, 0))
        }
    }

    #[test]
    fn unchanged_files_are_served_from_memory() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("catalog.json");
        let journal = dir.path().join("catalog.journal");
        std::fs::write(&snapshot, "v1").unwrap();
        let cache = CatalogCache::new(&snapshot, &journal);
        let loads = AtomicUsize::new(0);
        for _ in 0..10 {
            assert_eq!(*cache.read(load_counted(&snapshot, &loads)).unwrap(), "v1");
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);

        // Another process appending to the journal invalidates the copy.
        std::fs::write(&journal, "op\n").unwrap();
        cache.read(load_counted(&snapshot, &loads)).unwrap();
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn reload_waits_for_an_in_progress_writer() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("catalog.json");
        let journal = dir.path().join("catalog.journal");
        std::fs::write(&snapshot, "old").unwrap();
        let cache = Arc::new(CatalogCache::new(&snapshot, &journal));
        let loads = AtomicUsize::new(0);
        cache.read(load_counted(&snapshot, &loads)).unwrap();

        // A writer mid-compaction: snapshot replaced, journal not yet truncated.
        let writer = FileLock::exclusive(&snapshot).unwrap();
        std::fs::write(&snapshot, "new").unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let reader = {
            let cache = Arc::clone(&cache);
            let snapshot = snapshot.clone();
            std::thread::spawn(move || {
                let loads = AtomicUsize::new(0);
                let value = cache.read(load_counted(&snapshot, &loads)).unwrap();
                done_tx.send(value.to_string()).unwrap();
            })
        };
        assert!(done_rx
            .recv_timeout(std::time::Duration::from_millis(200))
            .is_err());
        drop(writer);
        assert_eq!(
            done_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap(),
            "new"
        );
        reader.join().unwrap();
    }
}
