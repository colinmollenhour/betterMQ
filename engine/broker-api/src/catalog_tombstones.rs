//! Tombstones for catalog deletes (CP6c) — prevents resurrection on union-merge.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn corrupt_tombstones_are_not_an_empty_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let store = CatalogTombstones::open(dir.path()).unwrap();
        std::fs::write(dir.path().join("catalog-tombstones.json"), b"not json").unwrap();
        assert!(store.snapshot().is_err());
        assert!(CatalogTombstones::open(dir.path()).is_err());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CatalogKind {
    Unknown,
    Flow,
    Queue,
    Group,
    GroupMember,
    Cron,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogTombstone {
    pub id: Uuid,
    pub kind: CatalogKind,
    pub deleted_at_ms: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct TombstoneFile {
    #[serde(default)]
    tombstones: Vec<CatalogTombstone>,
}

#[derive(Clone)]
pub struct CatalogTombstones {
    path: PathBuf,
}

fn meta_file_path(data_dir: &Path, name: &str) -> PathBuf {
    data_dir.join(name)
}

impl CatalogTombstones {
    pub fn legacy_shared_snapshot(data_dir: &Path) -> std::io::Result<HashMap<Uuid, i64>> {
        let Some(shared) = std::env::var("BETTERMQ_SHARED_META_DIR")
            .ok()
            .filter(|s| !s.trim().is_empty())
        else {
            return Ok(HashMap::new());
        };
        let path = PathBuf::from(shared).join("catalog-tombstones.json");
        if path.parent().and_then(|p| p.canonicalize().ok()) == data_dir.canonicalize().ok()
            || !path.try_exists()?
        {
            return Ok(HashMap::new());
        }
        Self { path }.snapshot()
    }
    pub fn open(data_dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = meta_file_path(data_dir.as_ref(), "catalog-tombstones.json");
        if !path.exists() {
            let file = TombstoneFile::default();
            std::fs::write(&path, serde_json::to_vec_pretty(&file)?)?;
        }
        let store = Self { path };
        store.load()?;
        Ok(store)
    }

    fn load(&self) -> std::io::Result<TombstoneFile> {
        let bytes = std::fs::read(&self.path)?;
        match serde_json::from_slice(&bytes) {
            Ok(v) => Ok(v),
            Err(_) if broker_proto::allow_empty_metadata_recovery() => Ok(TombstoneFile::default()),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        }
    }

    fn persist_unlocked(&self, file: &TombstoneFile) -> std::io::Result<()> {
        let bytes = serde_json::to_vec_pretty(file)?;
        broker_storage::atomic_write_file(&self.path, &bytes)
    }

    fn mutate<T>(
        &self,
        f: impl FnOnce(&mut TombstoneFile) -> std::io::Result<T>,
    ) -> std::io::Result<T> {
        let _lock = broker_storage::FileLock::exclusive(&self.path)?;
        let mut file = self.load()?;
        let out = f(&mut file)?;
        self.persist_unlocked(&file)?;
        Ok(out)
    }

    pub fn record(&self, id: Uuid, kind: CatalogKind) -> std::io::Result<()> {
        self.mutate(|file| {
            let now = Utc::now().timestamp_millis();
            if let Some(existing) = file.tombstones.iter_mut().find(|t| t.id == id) {
                existing.deleted_at_ms = now;
                existing.kind = kind;
            } else {
                file.tombstones.push(CatalogTombstone {
                    id,
                    kind,
                    deleted_at_ms: now,
                });
            }
            Ok(())
        })
    }

    pub fn is_deleted(&self, id: Uuid) -> std::io::Result<bool> {
        self.load().map(|f| f.tombstones.iter().any(|t| t.id == id))
    }

    pub fn deleted_at(&self, id: Uuid) -> std::io::Result<Option<i64>> {
        Ok(self
            .load()?
            .tombstones
            .iter()
            .find(|t| t.id == id)
            .map(|t| t.deleted_at_ms))
    }

    pub fn snapshot(&self) -> std::io::Result<HashMap<Uuid, i64>> {
        self.load().map(|f| {
            f.tombstones
                .into_iter()
                .map(|t| (t.id, t.deleted_at_ms))
                .collect()
        })
    }

    pub fn merge_remote(&self, remote: &HashMap<Uuid, i64>) -> std::io::Result<()> {
        if remote.is_empty() {
            return Ok(());
        }
        self.mutate(|file| {
            let mut changed = false;
            for (id, deleted_at_ms) in remote {
                if let Some(existing) = file.tombstones.iter_mut().find(|t| t.id == *id) {
                    if *deleted_at_ms > existing.deleted_at_ms {
                        existing.deleted_at_ms = *deleted_at_ms;
                        changed = true;
                    }
                } else {
                    file.tombstones.push(CatalogTombstone {
                        id: *id,
                        kind: CatalogKind::Unknown,
                        deleted_at_ms: *deleted_at_ms,
                    });
                    changed = true;
                }
            }
            let _ = changed;
            Ok(())
        })
    }
}
