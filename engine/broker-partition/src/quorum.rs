//! Quorum visibility watermark: the end of the contiguous log prefix that is
//! proven durable on a replication quorum (local fsync + minISR acks).
//!
//! Dispatch never delivers past it in a multi-node local-WAL cell, so a record
//! whose replication failed (the producer got a 5xx) is not delivered and its
//! offset cannot be marked complete before it is known to survive failover.
//!
//! The watermark only covers a *contiguous* prefix: epochs whose quorum
//! succeeds out of order are held as proven ranges and merged once the gap
//! below them is proven, so concurrent successes never bridge an unproven
//! gap. It is persisted (atomic write, fsync) before it is exposed, and lives
//! next to the log it describes, so wiping the log also wipes the watermark.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const FILE_NAME: &str = "quorum-hwm";

pub struct QuorumWatermark {
    path: PathBuf,
    exposed: AtomicU64,
    /// Legacy (identity-less) completion markers are trusted only below this.
    trusted_legacy_below: AtomicU64,
    state: parking_lot::Mutex<QuorumState>,
}

struct QuorumState {
    hwm: u64,
    /// Proven `[start, end)` ranges above `hwm`, keyed by start.
    proven: BTreeMap<u64, u64>,
}

impl QuorumWatermark {
    /// Load the persisted watermark for the log stored in `log_dir`; a cell
    /// that has none yet (upgrade, fresh replica) starts at 0.
    pub fn open(log_dir: &Path) -> std::io::Result<Self> {
        let path = log_dir.join(FILE_NAME);
        let hwm = match std::fs::read(&path) {
            Ok(bytes) => bytes
                .get(..8)
                .and_then(|b| b.try_into().ok())
                .map(u64::from_le_bytes)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("corrupt {}", path.display()),
                    )
                })?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error),
        };
        Ok(Self {
            path,
            exposed: AtomicU64::new(hwm),
            trusted_legacy_below: AtomicU64::new(hwm),
            state: parking_lot::Mutex::new(QuorumState {
                hwm,
                proven: BTreeMap::new(),
            }),
        })
    }

    pub fn get(&self) -> u64 {
        self.exposed.load(Ordering::Acquire)
    }

    pub fn trusted_legacy_below(&self) -> u64 {
        self.trusted_legacy_below.load(Ordering::Acquire)
    }

    /// Freeze the legacy-marker trust point at the current watermark (called
    /// when this node takes over a shard, before it verifies the tail).
    pub fn freeze_legacy_trust(&self) {
        self.trusted_legacy_below
            .store(self.get(), Ordering::Release);
    }

    /// Record that `[start, end)` is durable on a quorum. Returns the
    /// (possibly unchanged) contiguous watermark after persisting it.
    pub fn record(&self, start: u64, end: u64) -> std::io::Result<u64> {
        if end <= start {
            return Ok(self.get());
        }
        let mut state = self.state.lock();
        if end <= state.hwm {
            return Ok(state.hwm);
        }
        let entry = state.proven.entry(start).or_insert(end);
        *entry = (*entry).max(end);
        let mut next = state.hwm;
        while let Some((&range_start, &range_end)) = state.proven.iter().next() {
            if range_start > next {
                break;
            }
            next = next.max(range_end);
            state.proven.remove(&range_start);
        }
        if next > state.hwm {
            broker_storage::atomic_write_file(&self.path, &next.to_le_bytes())?;
            state.hwm = next;
            self.exposed.store(next, Ordering::Release);
        }
        Ok(state.hwm)
    }

    /// Proven ranges waiting for a gap below them (observability).
    pub fn pending_ranges(&self) -> usize {
        self.state.lock().proven.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn out_of_order_quorum_never_bridges_a_gap() {
        let dir = tempfile::tempdir().unwrap();
        let wm = QuorumWatermark::open(dir.path()).unwrap();
        assert_eq!(wm.record(0, 5).unwrap(), 5);
        // [5, 8) failed or is still in flight; [8, 12) succeeds first.
        assert_eq!(wm.record(8, 12).unwrap(), 5);
        assert_eq!(wm.get(), 5);
        assert_eq!(wm.pending_ranges(), 1);
        assert_eq!(wm.record(5, 8).unwrap(), 12);
        assert_eq!(wm.pending_ranges(), 0);
        // Stale and overlapping reports are harmless.
        assert_eq!(wm.record(2, 4).unwrap(), 12);
        assert_eq!(wm.record(10, 14).unwrap(), 14);
    }

    #[test]
    fn watermark_is_persisted_before_exposure_and_reloaded() {
        let dir = tempfile::tempdir().unwrap();
        let wm = QuorumWatermark::open(dir.path()).unwrap();
        assert_eq!(wm.get(), 0, "a cell without a watermark starts at 0");
        wm.record(0, 7).unwrap();
        drop(wm);
        let reopened = QuorumWatermark::open(dir.path()).unwrap();
        assert_eq!(reopened.get(), 7);
        assert_eq!(reopened.trusted_legacy_below(), 7);
    }
}
