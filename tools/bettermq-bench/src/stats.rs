//! Lock-sharded latency recorders and the shared counters every component
//! (load generator, sink, recorder) reads and writes.

use hdrhistogram::Histogram;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

const SHARDS: usize = 16;
/// Two hours in microseconds; far above anything we expect to record.
const MAX_US: u64 = 7_200_000_000;

pub fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, MAX_US, 3).expect("static histogram bounds are valid")
}

/// Latency recorder (microseconds). Writers pick a shard round-robin so
/// hot paths rarely contend; `take` drains all shards into one histogram.
pub struct Lat {
    shards: Vec<Mutex<Histogram<u64>>>,
    next: AtomicUsize,
}

impl Default for Lat {
    fn default() -> Self {
        Self::new()
    }
}

impl Lat {
    pub fn new() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(new_hist())).collect(),
            next: AtomicUsize::new(0),
        }
    }

    pub fn record_us(&self, us: u64) {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % SHARDS;
        let _ = self.shards[i].lock().record(us.clamp(1, MAX_US));
    }

    pub fn record(&self, d: std::time::Duration) {
        self.record_us(d.as_micros().min(u128::from(MAX_US)) as u64);
    }

    /// Merge every shard without resetting (cumulative view).
    pub fn peek(&self) -> Histogram<u64> {
        let mut out = new_hist();
        for shard in &self.shards {
            let _ = out.add(&*shard.lock());
        }
        out
    }

    /// Merge and reset every shard.
    pub fn take(&self) -> Histogram<u64> {
        let mut out = new_hist();
        for shard in &self.shards {
            let mut guard = shard.lock();
            let _ = out.add(&*guard);
            guard.reset();
        }
        out
    }
}

pub fn q_ms(h: &Histogram<u64>, q: f64) -> Option<f64> {
    if h.is_empty() {
        None
    } else {
        Some(h.value_at_quantile(q) as f64 / 1000.0)
    }
}

pub fn max_ms(h: &Histogram<u64>) -> Option<f64> {
    if h.is_empty() {
        None
    } else {
        Some(h.max() as f64 / 1000.0)
    }
}

/// Shared monotonic clock; message timestamps are microseconds on it.
#[derive(Clone, Copy)]
pub struct Clock {
    epoch: Instant,
}

impl Clock {
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }

    pub fn now_us(&self) -> u64 {
        self.epoch.elapsed().as_micros() as u64
    }

    pub fn us_at(&self, at: Instant) -> u64 {
        at.saturating_duration_since(self.epoch).as_micros() as u64
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
pub struct Counters {
    // load generator
    pub offered: AtomicU64,
    pub sent: AtomicU64,
    /// HTTP requests issued (a batch request carries many messages).
    pub requests: AtomicU64,
    pub acked: AtomicU64,
    pub rejected: AtomicU64,
    pub failed: AtomicU64,
    pub dropped: AtomicU64,
    pub duplicates_acked: AtomicU64,
    pub http_429: AtomicU64,
    pub http_503: AtomicU64,
    pub http_other_err: AtomicU64,
    pub transport_err: AtomicU64,
    pub payload_bytes: AtomicU64,
    pub wire_bytes: AtomicU64,
    pub client_inflight: AtomicI64,
    // sink
    pub sink_recv: AtomicU64,
    pub sink_ok: AtomicU64,
    pub sink_fail_transient: AtomicU64,
    pub sink_fail_permanent: AtomicU64,
    pub sink_dups: AtomicU64,
    pub sink_malformed: AtomicU64,
    pub sink_inflight: AtomicI64,
    pub cron_fires: AtomicU64,
}

pub fn ld(a: &AtomicU64) -> u64 {
    a.load(Ordering::Relaxed)
}

/// The histograms drained once per recorder tick.
#[derive(Default)]
pub struct Lats {
    /// ACK latency from the intended send time (coordinated-omission safe).
    pub ack: Lat,
    /// ACK latency from the actual send time (what a lone client would see).
    pub service: Lat,
    /// Actual send time minus intended send time: generator lag.
    pub gen_lag: Lat,
    /// Delivery latency (arrival at sink minus due time), first-attempt successes.
    pub delivery: Lat,
    /// Delivery latency for messages that needed retries.
    pub delivery_retried: Lat,
    /// Cron fire lateness (arrival minus scheduled instant).
    pub cron_jitter: Lat,
}

#[derive(Default)]
pub struct Shared {
    pub c: Counters,
    pub l: Lats,
    /// Distinct ingest failures: `"<status> <body prefix>"` -> count.
    pub errors: Mutex<std::collections::BTreeMap<String, u64>>,
}

impl Shared {
    pub fn note_error(&self, what: String) {
        let mut errors = self.errors.lock();
        if errors.len() < 24 || errors.contains_key(&what) {
            *errors.entry(what).or_default() += 1;
        }
    }
}
