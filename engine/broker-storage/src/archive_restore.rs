//! Validate and publish an entire restored WAL tree, never a partial shard.

use crate::archive::{ArchiveError, ArchiveManifest};
use crate::archive_checkpoint::{segment_number, validate_segment};
use crate::{LogMeta, PartitionLog, PartitionLogConfig, WalManifest};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub(crate) struct RestoreBatch {
    root: PathBuf,
    destination: PathBuf,
    groups: BTreeMap<PathBuf, Vec<ArchiveManifest>>,
}

fn invalid(message: impl Into<String>) -> ArchiveError {
    ArchiveError::Manifest(message.into())
}

impl RestoreBatch {
    pub(crate) fn new(
        destination: &Path,
        manifests: Vec<ArchiveManifest>,
    ) -> Result<Self, ArchiveError> {
        if destination.exists() && destination.read_dir()?.next().is_some() {
            return Err(invalid(
                "restore destination must be a new or empty directory",
            ));
        }
        let mut groups: BTreeMap<PathBuf, Vec<ArchiveManifest>> = BTreeMap::new();
        for manifest in manifests {
            let key = crate::archive::safe_archive_key(&manifest.segment)?;
            let parent = key
                .parent()
                .filter(|parent| parent.file_name().is_some_and(|name| name == "segments"))
                .and_then(Path::parent)
                .ok_or_else(|| {
                    invalid("archived segment is not inside a shard's segments directory")
                })?;
            segment_number(&key)?;
            if manifest.version != 2 || manifest.checkpoint.is_none() {
                return Err(invalid("archive has no versioned WAL recovery checkpoint; legacy archives are scrub-only"));
            }
            groups.entry(parent.into()).or_default().push(manifest);
        }
        for manifests in groups.values_mut() {
            manifests.sort_by_key(|manifest| {
                manifest
                    .checkpoint
                    .as_ref()
                    .expect("checked checkpoint")
                    .first_offset
            });
            let first = manifests[0]
                .checkpoint
                .as_ref()
                .expect("checked checkpoint");
            if first.first_offset != first.meta.base_offset {
                return Err(invalid(
                    "archive is missing the start of its retained WAL chain",
                ));
            }
            let mut next = first.first_offset;
            for manifest in manifests.iter() {
                let checkpoint = manifest.checkpoint.as_ref().expect("checked checkpoint");
                if checkpoint.wal_manifest != first.wal_manifest
                    || checkpoint.first_offset != next
                    || checkpoint.next_offset <= next
                {
                    return Err(invalid(
                        "archive segments are not one consistent contiguous WAL chain",
                    ));
                }
                next = checkpoint.next_offset;
            }
        }
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let root = parent.join(format!(".bettermq-restore-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root)?;
        Ok(Self {
            root,
            destination: destination.into(),
            groups,
        })
    }

    pub(crate) fn target(&self, manifest: &ArchiveManifest) -> Result<PathBuf, ArchiveError> {
        let path = self
            .root
            .join(crate::archive::safe_archive_key(&manifest.segment)?);
        std::fs::create_dir_all(path.parent().expect("segment parent"))?;
        Ok(path)
    }

    pub(crate) fn publish(self) -> Result<(), ArchiveError> {
        for (shard, manifests) in &self.groups {
            let first = manifests[0]
                .checkpoint
                .as_ref()
                .expect("checked checkpoint");
            let last = manifests
                .last()
                .expect("nonempty chain")
                .checkpoint
                .as_ref()
                .expect("checked checkpoint");
            let mut meta: LogMeta = last.meta.clone();
            meta.base_offset = first.first_offset;
            meta.base_segment = segment_number(Path::new(&manifests[0].segment))?;
            meta.next_offset = last.next_offset;
            meta.gc_watermark = first.first_offset;
            meta.purged_offsets.clear();
            for manifest in manifests {
                let checkpoint = manifest.checkpoint.as_ref().expect("checked checkpoint");
                let end = validate_segment(
                    &self.root.join(&manifest.segment),
                    &checkpoint.wal_manifest,
                    checkpoint.first_offset,
                )?;
                if end != checkpoint.next_offset {
                    return Err(invalid(
                        "archived segment does not match its checkpoint offset range",
                    ));
                }
                let roll = segment_number(Path::new(&manifest.segment))?
                    .checked_add(1)
                    .ok_or_else(|| invalid("segment sequence overflow"))?;
                meta.segment_roll_count = meta.segment_roll_count.max(roll);
                meta.purged_offsets.extend(
                    checkpoint
                        .meta
                        .purged_offsets
                        .range(first.first_offset..last.next_offset)
                        .copied(),
                );
            }
            let dir = self.root.join(shard);
            crate::atomic_write_file(
                &WalManifest::path(&dir),
                &serde_json::to_vec(&first.wal_manifest)
                    .map_err(|error| invalid(error.to_string()))?,
            )?;
            meta.save(&dir)?;
            // Open with the real recovery path before publishing the directory.
            let log = PartitionLog::open_for_shard(
                &dir,
                PartitionLogConfig::default(),
                first.wal_manifest.shard_id,
                first.wal_manifest.wal_format_version,
            )
            .map_err(|error| invalid(format!("restored shard cannot open: {error}")))?;
            if log.high_watermark() != last.next_offset {
                return Err(invalid(
                    "restored shard high watermark differs from archive checkpoint",
                ));
            }
        }
        sync_directories(&self.root)?;
        if self.destination.exists() {
            // Only removes an empty destination; never replaces existing data.
            std::fs::remove_dir(&self.destination)?;
        }
        std::fs::rename(&self.root, &self.destination)?;
        let parent = self
            .destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    }
}

fn sync_directories(path: &Path) -> std::io::Result<()> {
    for entry in path.read_dir()? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_directories(&entry.path())?;
        }
    }
    std::fs::File::open(path)?.sync_all()
}

impl Drop for RestoreBatch {
    fn drop(&mut self) {
        // This exclusively owned UUID directory is never the caller's data dir.
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
