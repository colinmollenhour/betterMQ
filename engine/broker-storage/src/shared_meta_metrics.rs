//! Bounded, process-wide shared metadata telemetry (no tenant or key labels).
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Default)]
struct Histogram {
    count: AtomicU64,
    micros: AtomicU64,
    buckets: [AtomicU64; 10],
}
impl Histogram {
    fn record(&self, elapsed: Duration) {
        let value = elapsed.as_micros().min(u64::MAX as u128) as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.micros.fetch_add(value, Ordering::Relaxed);
        if let Some(bucket) = crate::FSYNC_BUCKET_US
            .iter()
            .position(|edge| value <= *edge)
        {
            self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        }
    }
    fn snapshot(&self) -> SharedMetadataHistogram {
        SharedMetadataHistogram {
            count: self.count.load(Ordering::Relaxed),
            sum_us: self.micros.load(Ordering::Relaxed),
            buckets: std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed)),
        }
    }
}
#[derive(Debug, Clone)]
pub struct SharedMetadataHistogram {
    pub count: u64,
    pub sum_us: u64,
    pub buckets: [u64; 10],
}
#[derive(Debug, Clone)]
pub struct SharedMetadataTelemetry {
    pub lock_wait: SharedMetadataHistogram,
    pub fsync: SharedMetadataHistogram,
    pub replay_bytes: u64,
    pub compactions: u64,
    pub errors: u64,
    pub expired_entries: u64,
    pub legacy_unverifiable_dedup_hits: u64,
}
static LOCK: std::sync::OnceLock<Histogram> = std::sync::OnceLock::new();
static SYNC: std::sync::OnceLock<Histogram> = std::sync::OnceLock::new();
static REPLAY: AtomicU64 = AtomicU64::new(0);
static COMPACTIONS: AtomicU64 = AtomicU64::new(0);
static ERRORS: AtomicU64 = AtomicU64::new(0);
static EXPIRED: AtomicU64 = AtomicU64::new(0);
static LEGACY_UNVERIFIABLE: AtomicU64 = AtomicU64::new(0);
pub(crate) fn lock_wait(elapsed: Duration) {
    LOCK.get_or_init(Histogram::default).record(elapsed);
}
pub(crate) fn fsync(elapsed: Duration) {
    SYNC.get_or_init(Histogram::default).record(elapsed);
}
pub(crate) fn replay(bytes: u64) {
    REPLAY.fetch_add(bytes, Ordering::Relaxed);
}
pub(crate) fn compaction() {
    COMPACTIONS.fetch_add(1, Ordering::Relaxed);
}
pub(crate) fn error() {
    ERRORS.fetch_add(1, Ordering::Relaxed);
}
pub fn shared_metadata_telemetry() -> SharedMetadataTelemetry {
    SharedMetadataTelemetry {
        lock_wait: LOCK.get_or_init(Histogram::default).snapshot(),
        fsync: SYNC.get_or_init(Histogram::default).snapshot(),
        replay_bytes: REPLAY.load(Ordering::Relaxed),
        compactions: COMPACTIONS.load(Ordering::Relaxed),
        errors: ERRORS.load(Ordering::Relaxed),
        expired_entries: EXPIRED.load(Ordering::Relaxed),
        legacy_unverifiable_dedup_hits: LEGACY_UNVERIFIABLE.load(Ordering::Relaxed),
    }
}
pub(crate) fn expired(entries: u64) {
    EXPIRED.fetch_add(entries, Ordering::Relaxed);
}
pub fn record_legacy_unverifiable_dedup_hit() {
    LEGACY_UNVERIFIABLE.fetch_add(1, Ordering::Relaxed);
}
