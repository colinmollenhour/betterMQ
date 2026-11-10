//! One-way import of schedule/cron state that older releases kept under
//! `BETTERMQ_SHARED_META_DIR`.
//!
//! Journals are node-local now. Shared files are only ever read and renamed,
//! never truncated or compacted, because nodes still running an older release
//! may keep appending to them during a rolling upgrade.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

static IMPORTED_DELAYED: AtomicU64 = AtomicU64::new(0);
static IMPORTED_CRONS: AtomicU64 = AtomicU64::new(0);

const IMPORTING: &str = ".importing-";
const IMPORTED: &str = ".imported-";

/// Result of one legacy import pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacyImport {
    /// Jobs added to this node's local state.
    pub imported: usize,
    /// Jobs skipped because they were already local or imported before.
    pub skipped: usize,
    /// Shared files read in this pass.
    pub sources: Vec<PathBuf>,
}

/// Totals since process start, for metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LegacyImportTotals {
    pub delayed: u64,
    pub crons: u64,
}

pub fn legacy_import_totals() -> LegacyImportTotals {
    LegacyImportTotals {
        delayed: IMPORTED_DELAYED.load(Ordering::Relaxed),
        crons: IMPORTED_CRONS.load(Ordering::Relaxed),
    }
}

pub(crate) fn record_imported_delayed(count: usize) {
    IMPORTED_DELAYED.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn record_imported_crons(count: usize) {
    IMPORTED_CRONS.fetch_add(count as u64, Ordering::Relaxed);
}

/// True when `shared_dir` is the node's own data dir (nothing to import).
pub(crate) fn is_same_dir(shared_dir: &Path, local_file: &Path) -> bool {
    let Some(local_dir) = local_file.parent() else {
        return false;
    };
    match (shared_dir.canonicalize(), local_dir.canonicalize()) {
        (Ok(shared), Ok(local)) => shared == local,
        _ => false,
    }
}

/// Ids already imported into this node, persisted beside the local state so a
/// re-scan, an interrupted import or a later restart never imports twice.
#[derive(Debug)]
pub(crate) struct Ledger {
    path: PathBuf,
    ids: BTreeSet<Uuid>,
}

#[derive(Serialize, Deserialize)]
struct LedgerFile {
    ids: BTreeSet<Uuid>,
}

impl Ledger {
    pub(crate) fn load(path: PathBuf) -> std::io::Result<Self> {
        let ids = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<LedgerFile>(&bytes)?.ids,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeSet::new(),
            Err(error) => return Err(error),
        };
        Ok(Self { path, ids })
    }

    pub(crate) fn contains(&self, id: &Uuid) -> bool {
        self.ids.contains(id)
    }

    pub(crate) fn insert(&mut self, id: Uuid) -> bool {
        self.ids.insert(id)
    }

    pub(crate) fn save(&self) -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&LedgerFile {
            ids: self.ids.clone(),
        })?;
        broker_storage::atomic_write_file(&self.path, &bytes)?;
        broker_storage::set_secret_file_mode(&self.path);
        Ok(())
    }
}

/// Shared snapshot + journal pair claimed by one import pass.
#[derive(Debug)]
pub(crate) struct SourceGroup {
    tag: String,
    pub(crate) snapshot: Option<PathBuf>,
    pub(crate) journal: Option<PathBuf>,
}

impl SourceGroup {
    pub(crate) fn files(&self) -> impl Iterator<Item = &PathBuf> {
        self.snapshot.iter().chain(self.journal.iter())
    }
}

/// Move the live shared files aside (`<name>.importing-<ts>`) so writers from
/// older nodes start fresh files, then return every claimed but unfinished
/// group, oldest first. Renaming before reading means no append can land in a
/// file after it was read.
pub(crate) fn claim(
    shared_dir: &Path,
    snapshot_name: &str,
    journal_name: &str,
) -> std::io::Result<Vec<SourceGroup>> {
    let tag = chrono::Utc::now().format("%Y%m%dT%H%M%S%.9fZ").to_string();
    for name in [snapshot_name, journal_name] {
        let live = shared_dir.join(name);
        if live.exists() {
            let claimed = shared_dir.join(format!("{name}{IMPORTING}{tag}"));
            std::fs::rename(&live, &claimed)?;
        }
    }
    let mut tags = BTreeSet::new();
    for entry in std::fs::read_dir(shared_dir)? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        for prefix in [snapshot_name, journal_name] {
            if let Some(tag) = name
                .strip_prefix(prefix)
                .and_then(|rest| rest.strip_prefix(IMPORTING))
            {
                tags.insert(tag.to_string());
            }
        }
    }
    Ok(tags
        .into_iter()
        .map(|tag| {
            let existing = |name: &str| {
                let path = shared_dir.join(format!("{name}{IMPORTING}{tag}"));
                path.exists().then_some(path)
            };
            SourceGroup {
                snapshot: existing(snapshot_name),
                journal: existing(journal_name),
                tag,
            }
        })
        .collect())
}

/// Mark claimed groups as done (`<name>.imported-<ts>`). Kept for recovery,
/// never deleted.
pub(crate) fn finish(groups: &[SourceGroup]) -> std::io::Result<()> {
    for group in groups {
        for file in group.files() {
            let name = file.file_name().unwrap_or_default().to_string_lossy();
            let done = name.replace(
                &format!("{IMPORTING}{}", group.tag),
                &format!("{IMPORTED}{}", group.tag),
            );
            std::fs::rename(file, file.with_file_name(done))?;
        }
    }
    Ok(())
}
