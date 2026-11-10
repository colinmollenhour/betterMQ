//! Recovery metadata and strict validation for archived sealed WAL segments.

use crate::archive::ArchiveError;
use crate::{LogMeta, WalManifest, WAL_FORMAT_V1, WAL_FORMAT_V2};
use broker_proto::{decode_epoch, record::decode_frame};
use serde::{Deserialize, Serialize};
use std::io::{BufReader, Cursor, Seek};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveCheckpoint {
    pub wal_manifest: WalManifest,
    pub meta: LogMeta,
    pub first_offset: u64,
    pub next_offset: u64,
}

fn invalid(message: impl Into<String>) -> ArchiveError {
    ArchiveError::Manifest(message.into())
}

pub(crate) fn segment_number(path: &Path) -> Result<u64, ArchiveError> {
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("segment-"))
        .and_then(|name| name.strip_suffix(".log"))
        .and_then(|number| number.parse().ok())
        .ok_or_else(|| invalid(format!("invalid segment filename {}", path.display())))
}

pub(crate) fn checkpoint_path(segment: &Path) -> Result<PathBuf, ArchiveError> {
    let shard = segment
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| invalid("segment has no shard directory"))?;
    Ok(shard.join("archive-checkpoints").join(format!(
        "{}.json",
        segment
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid("non-UTF8 segment filename"))?
    )))
}

pub(crate) fn save_checkpoint(
    segment: &Path,
    checkpoint: &ArchiveCheckpoint,
) -> Result<(), ArchiveError> {
    let path = checkpoint_path(segment)?;
    std::fs::create_dir_all(path.parent().expect("checkpoint parent"))?;
    crate::atomic_write_file(
        &path,
        &serde_json::to_vec(checkpoint).map_err(|error| invalid(error.to_string()))?,
    )?;
    Ok(())
}

/// Older pending jobs predate per-roll snapshots. Recover their range once from
/// the retained sealed chain (never from a concurrently changing active WAL).
pub(crate) fn load_checkpoint(segment: &Path) -> Result<Option<ArchiveCheckpoint>, ArchiveError> {
    let checkpoint = checkpoint_path(segment)?;
    if checkpoint.exists() {
        return serde_json::from_slice(&std::fs::read(checkpoint)?)
            .map(Some)
            .map_err(|error| invalid(error.to_string()));
    }
    let shard = segment
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| invalid("missing shard directory"))?;
    let wal_path = WalManifest::path(shard);
    if !wal_path.exists() {
        // Pre-checkpoint archives remain useful for checksumming, but cannot
        // claim to be a recoverable WAL. Restore explicitly refuses these.
        return Ok(None);
    }
    let wal_manifest: WalManifest = serde_json::from_slice(&std::fs::read(wal_path)?)
        .map_err(|error| invalid(error.to_string()))?;
    let meta = LogMeta::load(shard)?;
    let wanted = segment_number(segment)?;
    if wanted < meta.base_segment {
        return Err(invalid("segment is below the retained source base"));
    }
    let mut segments: Vec<_> = std::fs::read_dir(shard.join("segments"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter_map(|path| segment_number(&path).ok().map(|number| (number, path)))
        .filter(|(number, _)| *number >= meta.base_segment && *number <= wanted)
        .collect();
    segments.sort_by_key(|(number, _)| *number);
    let mut next = meta.base_offset;
    let mut number = meta.base_segment;
    for (current, path) in segments {
        if current != number {
            return Err(invalid("missing segment in source recovery chain"));
        }
        let first_offset = next;
        next = validate_segment(&path, &wal_manifest, first_offset)?;
        if current == wanted {
            if LogMeta::load(shard)?.base_offset != meta.base_offset {
                return Err(invalid(
                    "source base changed while capturing archive checkpoint; retry",
                ));
            }
            let snapshot = ArchiveCheckpoint {
                wal_manifest,
                meta,
                first_offset,
                next_offset: next,
            };
            save_checkpoint(segment, &snapshot)?;
            return Ok(Some(snapshot));
        }
        number = number
            .checked_add(1)
            .ok_or_else(|| invalid("segment number overflow"))?;
    }
    Err(invalid("source segment not found in retained chain"))
}

/// Check frame/epoch integrity and the complete exclusive offset range. Unlike
/// normal active-WAL recovery, a sealed archive may never truncate a torn tail.
pub(crate) fn validate_segment(
    path: &Path,
    manifest: &WalManifest,
    first: u64,
) -> Result<u64, ArchiveError> {
    let mut reader = BufReader::new(std::fs::File::open(path)?);
    let length = std::fs::metadata(path)?.len();
    let mut next = first;
    while reader.stream_position()? < length {
        match manifest.wal_format_version {
            WAL_FORMAT_V1 => {
                decode_frame(&mut reader)
                    .map_err(|error| invalid(format!("{}: {error}", path.display())))?;
                next = next
                    .checked_add(1)
                    .ok_or_else(|| invalid("offset overflow"))?;
            }
            WAL_FORMAT_V2 => {
                let (epoch, body) = decode_epoch(&mut reader)
                    .map_err(|error| invalid(format!("{}: {error}", path.display())))?;
                if epoch.shard_id != manifest.shard_id
                    || epoch.first_offset != next
                    || epoch.record_count == 0
                    || next.checked_add(epoch.record_count as u64) != Some(epoch.committed_hwm)
                {
                    return Err(invalid(format!(
                        "noncontiguous or mismatched epoch in {}",
                        path.display()
                    )));
                }
                let mut frames = Cursor::new(&body);
                for _ in 0..epoch.record_count {
                    decode_frame(&mut frames).map_err(|error| invalid(error.to_string()))?;
                }
                if frames.position() != body.len() as u64 {
                    return Err(invalid("epoch frame count/length mismatch"));
                }
                next = epoch.committed_hwm;
            }
            _ => return Err(invalid("unsupported archived WAL format")),
        }
    }
    if next == first {
        return Err(invalid("empty sealed segment"));
    }
    Ok(next)
}
