use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Restrict a file that may contain secrets (tokens, webhook URLs, hashes).
pub fn set_secret_file_mode(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    let _ = path;
}

/// Atomic durable write: temp → fsync → rename → parent-dir fsync.
pub fn atomic_write_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    atomic_write_file_with(path, bytes, true)
}

/// Atomic replace. When `fsync` is false, skip `sync_all` (OS page cache only).
pub fn atomic_write_file_with(path: &Path, bytes: &[u8], fsync: bool) -> std::io::Result<()> {
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        if fsync {
            f.sync_all()?;
        }
    }
    std::fs::rename(&tmp, path)?;
    if fsync {
        if let Some(parent) = path.parent() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogMeta {
    pub next_offset: u64,
    pub segment_roll_count: u64,
    /// Offsets tombstoned by purge (local log bytes are not rewritten),
    /// stored as merged ranges so bulk purges stay small.
    #[serde(default, rename = "purged_ranges")]
    pub purged_offsets: OffsetRanges,
    /// Tombstones in the pre-range format. Merged into `purged_offsets` on
    /// load and never written back.
    #[serde(default, rename = "purged_offsets", skip_serializing)]
    legacy_purged_offsets: BTreeSet<u64>,
    /// Last accepted leadership fence for this partition (local WAL).
    #[serde(default)]
    pub leader_generation: u64,
    /// Highest offset whose sealed segments may be physically deleted.
    #[serde(default)]
    pub gc_watermark: u64,
    /// First offset still retained by this log. Everything below it was
    /// removed by GC (or never copied here, see `PartitionLog::reset_base`),
    /// and recovery numbers the surviving records from here instead of 0.
    #[serde(default)]
    pub base_offset: u64,
    /// Roll number of the first retained sealed segment. Segment files with a
    /// lower number are deletion leftovers and are removed on open.
    #[serde(default)]
    pub base_segment: u64,
    /// Exclusive offset known to be fsynced to the WAL. Advanced only after a
    /// successful sync, so it is a lower bound of what recovery must find;
    /// opening with fewer records fails instead of reusing their offsets.
    #[serde(default)]
    pub durable_offset: u64,
}

impl LogMeta {
    pub fn path(partition_dir: &Path) -> PathBuf {
        partition_dir.join("meta.json")
    }

    pub fn load(partition_dir: &Path) -> std::io::Result<Self> {
        let path = Self::path(partition_dir);
        if !path.exists() {
            return Ok(Self::default());
        }
        let bytes = std::fs::read(path)?;
        let mut meta: Self = serde_json::from_slice(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        for offset in std::mem::take(&mut meta.legacy_purged_offsets) {
            meta.purged_offsets.insert(offset);
        }
        Ok(meta)
    }

    pub fn save(&self, partition_dir: &Path) -> std::io::Result<()> {
        self.save_with(partition_dir, true)
    }

    pub fn save_with(&self, partition_dir: &Path, fsync: bool) -> std::io::Result<()> {
        let path = Self::path(partition_dir);
        let bytes = serde_json::to_vec(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        atomic_write_file_with(&path, &bytes, fsync)
    }
}

/// Set of offsets kept as disjoint, non-adjacent `[start, end)` ranges.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OffsetRanges {
    ranges: BTreeMap<u64, u64>,
}

impl OffsetRanges {
    pub fn contains(&self, offset: u64) -> bool {
        self.ranges
            .range(..=offset)
            .next_back()
            .is_some_and(|(_, &end)| offset < end)
    }

    /// Add `offset`; returns false if it was already present.
    pub fn insert(&mut self, offset: u64) -> bool {
        if self.contains(offset) {
            return false;
        }
        self.insert_range(offset, offset.saturating_add(1));
        true
    }

    /// Add every offset in `[start, end)`, merging touching ranges.
    pub fn insert_range(&mut self, mut start: u64, mut end: u64) {
        if start >= end {
            return;
        }
        if let Some((&previous_start, &previous_end)) = self.ranges.range(..=start).next_back() {
            if previous_end >= start {
                start = previous_start;
                end = end.max(previous_end);
                self.ranges.remove(&previous_start);
            }
        }
        while let Some((&next_start, &next_end)) = self.ranges.range(start..=end).next() {
            end = end.max(next_end);
            self.ranges.remove(&next_start);
        }
        self.ranges.insert(start, end);
    }

    /// Drop every offset below `base`.
    pub fn remove_below(&mut self, base: u64) {
        let mut kept = self.ranges.split_off(&base);
        if let Some((_, &end)) = self.ranges.iter().next_back() {
            if end > base {
                kept.insert(base, end);
            }
        }
        self.ranges = kept;
    }

    /// Drop every offset at or above `end`.
    pub fn remove_from(&mut self, end: u64) {
        self.ranges.split_off(&end);
        if let Some((&start, &range_end)) = self.ranges.iter().next_back() {
            if range_end > end {
                self.ranges.insert(start, end);
            }
        }
    }

    /// End of the range containing `offset`, if any.
    pub fn run_end_from(&self, offset: u64) -> Option<u64> {
        self.ranges
            .range(..=offset)
            .next_back()
            .filter(|(_, &end)| offset < end)
            .map(|(_, &end)| end)
    }

    /// Number of offsets in the set.
    pub fn len(&self) -> u64 {
        self.ranges.iter().map(|(start, end)| end - start).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn ranges(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.ranges.iter().map(|(&start, &end)| (start, end))
    }
}

impl Serialize for OffsetRanges {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.ranges())
    }
}

impl<'de> Deserialize<'de> for OffsetRanges {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let pairs = Vec::<(u64, u64)>::deserialize(deserializer)?;
        let mut set = Self::default();
        for (start, end) in pairs {
            if start >= end {
                return Err(serde::de::Error::custom(format!(
                    "empty purged range [{start}, {end})"
                )));
            }
            set.insert_range(start, end);
        }
        Ok(set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_ranges_merge_split_and_prune() {
        let mut set = OffsetRanges::default();
        for offset in [5, 3, 4, 10, 6, 11] {
            assert!(set.insert(offset));
        }
        assert!(!set.insert(4));
        assert_eq!(set.ranges().collect::<Vec<_>>(), vec![(3, 7), (10, 12)]);
        assert_eq!(set.len(), 6);
        assert!(set.contains(6) && !set.contains(7) && !set.contains(2));
        assert_eq!(set.run_end_from(4), Some(7));
        let mut cut = set.clone();
        cut.remove_from(5);
        assert_eq!(cut.ranges().collect::<Vec<_>>(), vec![(3, 5)]);
        cut.remove_from(3);
        assert!(cut.is_empty());
        assert_eq!(set.run_end_from(7), None);
        set.remove_below(6);
        assert_eq!(set.ranges().collect::<Vec<_>>(), vec![(6, 7), (10, 12)]);
        set.insert_range(0, 1_000_000_000);
        assert_eq!(set.ranges().collect::<Vec<_>>(), vec![(0, 1_000_000_000)]);
    }

    #[test]
    fn legacy_purged_offsets_load_as_ranges_and_save_compactly() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            LogMeta::path(dir.path()),
            br#"{"next_offset":9,"segment_roll_count":0,"purged_offsets":[1,2,3,7]}"#,
        )
        .unwrap();
        let meta = LogMeta::load(dir.path()).unwrap();
        assert_eq!(
            meta.purged_offsets.ranges().collect::<Vec<_>>(),
            vec![(1, 4), (7, 8)]
        );
        meta.save(dir.path()).unwrap();
        let raw = std::fs::read_to_string(LogMeta::path(dir.path())).unwrap();
        assert!(raw.contains(r#""purged_ranges":[[1,4],[7,8]]"#), "{raw}");
        assert!(!raw.contains(r#""purged_offsets""#), "{raw}");
        let reloaded = LogMeta::load(dir.path()).unwrap();
        assert_eq!(reloaded.purged_offsets, meta.purged_offsets);
    }
}
