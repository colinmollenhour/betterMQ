//! Validate and publish an entire restored WAL tree, never a partial shard.

use crate::archive::{ArchiveError, ArchiveManifest};
use crate::archive_checkpoint::{segment_number, validate_segment};
use crate::{ArchiveSource, LogMeta, OffsetRanges, PartitionLog, PartitionLogConfig, WalManifest};
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

fn restore_key(manifest: &ArchiveManifest) -> Result<PathBuf, ArchiveError> {
    crate::archive::safe_archive_key(
        manifest
            .restore_path
            .as_deref()
            .ok_or_else(|| invalid("archive has no original WAL path"))?,
    )
}

impl RestoreBatch {
    pub(crate) fn new(
        destination: &Path,
        manifests: Vec<ArchiveManifest>,
        source: Option<&ArchiveSource>,
    ) -> Result<Self, ArchiveError> {
        if destination.exists() && destination.read_dir()?.next().is_some() {
            return Err(invalid(
                "restore destination must be a new or empty directory",
            ));
        }
        let mut groups: BTreeMap<PathBuf, Vec<ArchiveManifest>> = BTreeMap::new();
        for manifest in manifests {
            if manifest.version != 3 || manifest.checkpoint.is_none() || manifest.source.is_none() {
                return Err(invalid("archive has no namespaced WAL recovery checkpoint; legacy archives are scrub-only"));
            }
            let source = source.ok_or_else(|| {
                invalid("restore requires an explicit archive source; use --source CLUSTER/NODE")
            })?;
            if manifest.source.as_ref() != Some(source) {
                return Err(invalid(
                    "archive manifest does not belong to the selected source",
                ));
            }
            let key = restore_key(&manifest)?;
            if crate::archive::safe_archive_key(&manifest.segment)?
                != source.object_key(&key, &manifest.sha256)?
            {
                return Err(invalid(
                    "archive object key does not match its source and checksum identity",
                ));
            }
            let parent = key
                .parent()
                .filter(|parent| parent.file_name().is_some_and(|name| name == "segments"))
                .and_then(Path::parent)
                .ok_or_else(|| {
                    invalid("archived segment is not inside a shard's segments directory")
                })?;
            segment_number(&key)?;
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
        let path = self.root.join(restore_key(manifest)?);
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
            meta.base_segment = segment_number(&restore_key(&manifests[0])?)?;
            meta.next_offset = last.next_offset;
            meta.gc_watermark = first.first_offset;
            meta.purged_offsets = OffsetRanges::default();
            for manifest in manifests {
                let checkpoint = manifest.checkpoint.as_ref().expect("checked checkpoint");
                let end = validate_segment(
                    &self.root.join(restore_key(manifest)?),
                    &checkpoint.wal_manifest,
                    checkpoint.first_offset,
                )?;
                if end != checkpoint.next_offset {
                    return Err(invalid(
                        "archived segment does not match its checkpoint offset range",
                    ));
                }
                let roll = segment_number(&restore_key(manifest)?)?
                    .checked_add(1)
                    .ok_or_else(|| invalid("segment sequence overflow"))?;
                meta.segment_roll_count = meta.segment_roll_count.max(roll);
                for (start, end) in checkpoint.meta.purged_offsets.ranges() {
                    meta.purged_offsets
                        .insert_range(start.max(first.first_offset), end.min(last.next_offset));
                }
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
