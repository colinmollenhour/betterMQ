//! Bounded native dispatch telemetry.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

pub const MAX_DISPATCH_TELEMETRY_SHARDS: usize = 4096;
pub const MAX_DISPATCH_CURSOR_LANES: usize = 4096;
pub const DELIVERY_OUTCOME_CLASSES: [&str; 7] = [
    "2xx",
    "3xx",
    "4xx",
    "5xx",
    "transport_error",
    "policy_block",
    "other",
];
pub const WEBHOOK_DURATION_BUCKETS: [f64; 16] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 120.0, 900.0, 3600.0,
    43200.0,
];

#[derive(Default)]
struct ShardCounters {
    completions: AtomicU64,
    cursor_advanced_records: AtomicU64,
}

#[derive(Default)]
pub(crate) struct DispatchTelemetry {
    committed_ready_ranges: AtomicU64,
    committed_ready_records: AtomicU64,
    queue_depth: AtomicU64,
    queue_rejected: AtomicU64,
    deferred_jobs: AtomicU64,
    defer_rejected: AtomicU64,
    retries_scheduled: AtomicU64,
    retries_due: AtomicU64,
    retries_exhausted: AtomicU64,
    dlq_prepared: AtomicU64,
    dlq_committed: AtomicU64,
    dlq_failures: AtomicU64,
    drain_started: AtomicU64,
    drain_completed: AtomicU64,
    drain_timeouts: AtomicU64,
    last_drain_duration_ms: AtomicU64,
    draining: AtomicBool,
    shards: Box<[ShardCounters]>,
    cursor_lanes: Mutex<BTreeMap<(u32, String), (u64, u64)>>,
    dropped_cursor_lanes: AtomicU64,
    delivery_outcomes: [AtomicU64; 7],
    webhook_buckets: [AtomicU64; 16],
    webhook_duration_sum_us: AtomicU64,
    flow_errors: AtomicU64,
    pending_heads: Mutex<BTreeMap<(u32, String), (u64, i64)>>,
}

impl DispatchTelemetry {
    pub(crate) fn new() -> Self {
        Self {
            shards: (0..MAX_DISPATCH_TELEMETRY_SHARDS)
                .map(|_| ShardCounters::default())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            ..Self::default()
        }
    }

    pub(crate) fn record_committed_range(&self, records: u64) {
        self.committed_ready_ranges.fetch_add(1, Ordering::Relaxed);
        self.committed_ready_records
            .fetch_add(records, Ordering::Relaxed);
    }

    pub(crate) fn record_http_outcome(&self, outcome: usize, elapsed: std::time::Duration) {
        self.delivery_outcomes[outcome].fetch_add(1, Ordering::Relaxed);
        self.webhook_duration_sum_us.fetch_add(
            elapsed.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        if let Some(bucket) = WEBHOOK_DURATION_BUCKETS
            .iter()
            .position(|limit| elapsed.as_secs_f64() <= *limit)
        {
            self.webhook_buckets[bucket].fetch_add(1, Ordering::Release);
        }
    }

    pub(crate) fn record_flow_error(&self) {
        self.flow_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_pending_age(
        &self,
        shard: u32,
        lane: &str,
        offset: u64,
        published_at_ms: i64,
    ) {
        let lanes = self
            .cursor_lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if lanes
            .get(&(shard, lane.to_owned()))
            .is_none_or(|(cursor, _)| offset < *cursor)
        {
            return;
        }
        let mut heads = self
            .pending_heads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = (shard, lane.to_owned());
        if let Some(head) = heads.get_mut(&key) {
            if offset < head.0 {
                *head = (offset, published_at_ms);
            }
        } else if heads.len() < MAX_DISPATCH_CURSOR_LANES && (shard as usize) < self.shards.len() {
            heads.insert(key, (offset, published_at_ms));
        }
    }

    pub(crate) fn advance_pending_head(
        &self,
        shard: u32,
        lane: &str,
        cursor: u64,
        next: Option<(u64, i64)>,
    ) {
        let lanes = self
            .cursor_lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let cursor = lanes
            .get(&(shard, lane.to_owned()))
            .map_or(cursor, |(current, _)| cursor.max(*current));
        let next = next.filter(|head| head.0 >= cursor);
        let mut heads = self
            .pending_heads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = (shard, lane.to_owned());
        if heads.get(&key).is_some_and(|head| head.0 < cursor) {
            heads.remove(&key);
        }
        if let Some(next) = next {
            if let Some(head) = heads.get_mut(&key) {
                if next.0 < head.0 {
                    *head = next;
                }
            } else if heads.len() < MAX_DISPATCH_CURSOR_LANES {
                heads.insert(key, next);
            }
        }
    }

    pub(crate) fn record_queue_enqueue(&self) {
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_queue_dequeue(&self) {
        self.queue_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();
    }

    pub(crate) fn record_queue_rejected(&self) {
        self.queue_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_deferred(&self) {
        self.deferred_jobs.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_defer_rejected(&self) {
        self.defer_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry_scheduled(&self) {
        self.retries_scheduled.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry_due(&self) {
        self.retries_due.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry_exhausted(&self) {
        self.retries_exhausted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dlq_prepared(&self) {
        self.dlq_prepared.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dlq_committed(&self) {
        self.dlq_committed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dlq_failure(&self) {
        self.dlq_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_drain_started(&self) {
        self.draining.store(true, Ordering::Release);
        self.drain_started.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_drain_finished(&self, elapsed_ms: u64, completed: bool) {
        self.last_drain_duration_ms
            .store(elapsed_ms, Ordering::Relaxed);
        if completed {
            self.drain_completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.drain_timeouts.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_stopped(&self) {
        self.draining.store(true, Ordering::Release);
    }

    pub(crate) fn record_completion(
        &self,
        shard: u32,
        lane: &str,
        old_cursor: u64,
        new_cursor: u64,
        committed_hwm: u64,
        newly_completed: bool,
    ) {
        let Some(counters) = self.shards.get(shard as usize) else {
            self.dropped_cursor_lanes.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if newly_completed {
            counters.completions.fetch_add(1, Ordering::Relaxed);
        }
        counters
            .cursor_advanced_records
            .fetch_add(new_cursor.saturating_sub(old_cursor), Ordering::Relaxed);
        self.record_pending(shard, lane, new_cursor, committed_hwm);
    }

    pub(crate) fn record_pending(&self, shard: u32, lane: &str, cursor: u64, committed_hwm: u64) {
        if shard as usize >= self.shards.len() {
            self.dropped_cursor_lanes.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut lanes = self
            .cursor_lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = (shard, lane.to_string());
        if lanes.contains_key(&key) || lanes.len() < MAX_DISPATCH_CURSOR_LANES {
            let entry = lanes.entry(key).or_insert((cursor, committed_hwm));
            entry.0 = entry.0.max(cursor);
            entry.1 = entry.1.max(committed_hwm);
        } else {
            self.dropped_cursor_lanes.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn snapshot(&self) -> DispatchCounterSnapshot {
        let lanes = self
            .cursor_lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut aggregate: BTreeMap<u32, (u64, u64, u64, u64)> = BTreeMap::new();
        for ((shard, _), (cursor, hwm)) in lanes.iter() {
            let value = aggregate.entry(*shard).or_insert((u64::MAX, 0, 0, 0));
            value.0 = value.0.min(*cursor);
            value.1 = value.1.max(*cursor);
            value.2 = value.2.saturating_add(hwm.saturating_sub(*cursor));
            value.3 = value.3.saturating_add(1);
        }
        for (shard, counters) in self.shards.iter().enumerate() {
            let completions = counters.completions.load(Ordering::Relaxed);
            let advanced = counters.cursor_advanced_records.load(Ordering::Relaxed);
            if completions > 0 || advanced > 0 {
                aggregate.entry(shard as u32).or_insert((0, 0, 0, 0));
            }
        }
        let now = chrono::Utc::now().timestamp_millis();
        let heads = self
            .pending_heads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut oldest_ages = BTreeMap::<u32, u64>::new();
        for ((shard, _), (_, published)) in heads.iter() {
            let age = now.saturating_sub(*published).max(0) as u64;
            let oldest = oldest_ages.entry(*shard).or_default();
            *oldest = (*oldest).max(age);
        }
        // Snapshot finite buckets before outcome/count totals. Acquiring each
        // bucket publishes its preceding outcome increment, so +Inf/count can
        // never lag behind an observed finite bucket during concurrent updates.
        let duration_buckets =
            std::array::from_fn(|index| self.webhook_buckets[index].load(Ordering::Acquire));
        let outcomes =
            std::array::from_fn(|index| self.delivery_outcomes[index].load(Ordering::Relaxed));
        DispatchCounterSnapshot {
            delivery: DeliveryTelemetrySnapshot {
                outcomes,
                duration_buckets,
                duration_sum_us: self.webhook_duration_sum_us.load(Ordering::Relaxed),
                flow_errors: self.flow_errors.load(Ordering::Relaxed),
            },
            committed_ready_ranges: self.committed_ready_ranges.load(Ordering::Relaxed),
            committed_ready_records: self.committed_ready_records.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_rejected: self.queue_rejected.load(Ordering::Relaxed),
            deferred_jobs: self.deferred_jobs.load(Ordering::Relaxed),
            defer_rejected: self.defer_rejected.load(Ordering::Relaxed),
            retries_scheduled: self.retries_scheduled.load(Ordering::Relaxed),
            retries_due: self.retries_due.load(Ordering::Relaxed),
            retries_exhausted: self.retries_exhausted.load(Ordering::Relaxed),
            dlq_prepared: self.dlq_prepared.load(Ordering::Relaxed),
            dlq_committed: self.dlq_committed.load(Ordering::Relaxed),
            dlq_failures: self.dlq_failures.load(Ordering::Relaxed),
            drain_started: self.drain_started.load(Ordering::Relaxed),
            drain_completed: self.drain_completed.load(Ordering::Relaxed),
            drain_timeouts: self.drain_timeouts.load(Ordering::Relaxed),
            last_drain_duration_ms: self.last_drain_duration_ms.load(Ordering::Relaxed),
            draining: self.draining.load(Ordering::Acquire),
            shards: aggregate
                .into_iter()
                .map(
                    |(shard, (cursor_min, cursor_max, gap_records, tracked_lanes))| {
                        let counters = &self.shards[shard as usize];
                        DispatchShardTelemetrySnapshot {
                            shard,
                            completions: counters.completions.load(Ordering::Relaxed),
                            cursor_advanced_records: counters
                                .cursor_advanced_records
                                .load(Ordering::Relaxed),
                            cursor_min_offset: cursor_min,
                            cursor_max_offset: cursor_max,
                            completion_gap_records: gap_records,
                            tracked_cursor_lanes: tracked_lanes,
                            oldest_pending_age_ms: oldest_ages.get(&shard).copied().unwrap_or(0),
                        }
                    },
                )
                .collect(),
            dropped_cursor_lanes: self.dropped_cursor_lanes.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct DispatchCounterSnapshot {
    pub delivery: DeliveryTelemetrySnapshot,
    pub committed_ready_ranges: u64,
    pub committed_ready_records: u64,
    pub queue_depth: u64,
    pub queue_rejected: u64,
    pub deferred_jobs: u64,
    pub defer_rejected: u64,
    pub retries_scheduled: u64,
    pub retries_due: u64,
    pub retries_exhausted: u64,
    pub dlq_prepared: u64,
    pub dlq_committed: u64,
    pub dlq_failures: u64,
    pub drain_started: u64,
    pub drain_completed: u64,
    pub drain_timeouts: u64,
    pub last_drain_duration_ms: u64,
    pub draining: bool,
    pub shards: Vec<DispatchShardTelemetrySnapshot>,
    pub dropped_cursor_lanes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchShardTelemetrySnapshot {
    pub shard: u32,
    pub completions: u64,
    pub cursor_advanced_records: u64,
    pub cursor_min_offset: u64,
    pub cursor_max_offset: u64,
    pub completion_gap_records: u64,
    pub tracked_cursor_lanes: u64,
    pub oldest_pending_age_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeliveryTelemetrySnapshot {
    pub outcomes: [u64; 7],
    pub duration_buckets: [u64; 16],
    pub duration_sum_us: u64,
    pub flow_errors: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPressureSnapshot {
    pub tracked_hosts: u64,
    pub blocked_hosts: u64,
    pub hosts_with_failures: u64,
    pub current_failures: u64,
    pub max_host_failures: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TenantFairnessSnapshot {
    pub tracked_tenants: u64,
    pub configured_weights: u64,
    pub soft_limit: u64,
    pub saturation_ratio: f64,
    pub saturated: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DispatchTelemetrySnapshot {
    pub delivery: DeliveryTelemetrySnapshot,
    pub committed_ready_ranges: u64,
    pub committed_ready_records: u64,
    pub queue_depth: u64,
    pub queue_rejected: u64,
    pub deferred_jobs: u64,
    pub defer_rejected: u64,
    pub in_flight_deliveries: u64,
    pub active_http_deliveries: u64,
    pub retries_pending: u64,
    pub retries_pending_due: u64,
    pub retries_scheduled: u64,
    pub retries_due: u64,
    pub retries_exhausted: u64,
    pub dlq_pending_prepared: u64,
    pub dlq_pending_committed: u64,
    pub dlq_prepared: u64,
    pub dlq_committed: u64,
    pub dlq_failures: u64,
    pub drain_started: u64,
    pub drain_completed: u64,
    pub drain_timeouts: u64,
    pub last_drain_duration_ms: u64,
    pub draining: bool,
    pub shards: Vec<DispatchShardTelemetrySnapshot>,
    pub dropped_cursor_lanes: u64,
    pub host_pressure: HostPressureSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcomes_histograms_and_stalled_pending_age_are_observable() {
        let telemetry = DispatchTelemetry::new();
        telemetry.record_http_outcome(0, std::time::Duration::from_millis(2));
        telemetry.record_http_outcome(3, std::time::Duration::from_secs(2));
        telemetry.record_http_outcome(4, std::time::Duration::from_secs(50_000));
        telemetry.record_flow_error();
        telemetry.record_pending(0, "lane", 0, 2);
        telemetry.record_pending_age(0, "lane", 0, chrono::Utc::now().timestamp_millis() - 1_000);
        let first = telemetry.snapshot();
        assert_eq!(first.delivery.outcomes, [1, 0, 0, 1, 1, 0, 0]);
        assert_eq!(first.delivery.duration_buckets.iter().sum::<u64>(), 2);
        assert_eq!(first.delivery.flow_errors, 1);
        assert!(first.shards[0].oldest_pending_age_ms >= 1_000);
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(
            telemetry.snapshot().shards[0].oldest_pending_age_ms
                > first.shards[0].oldest_pending_age_ms
        );
        telemetry.record_completion(0, "lane", 0, 2, 2, true);
        telemetry.advance_pending_head(0, "lane", 2, None);
        telemetry.record_pending_age(0, "lane", 0, 0);
        assert_eq!(telemetry.snapshot().shards[0].oldest_pending_age_ms, 0);
    }

    #[test]
    fn counters_and_cursor_gaps_are_aggregated_without_lane_labels() {
        let telemetry = DispatchTelemetry::new();
        telemetry.record_committed_range(4);
        telemetry.record_queue_enqueue();
        telemetry.record_queue_dequeue();
        telemetry.record_deferred();
        telemetry.record_defer_rejected();
        telemetry.record_retry_scheduled();
        telemetry.record_retry_due();
        telemetry.record_completion(3, "private-lane-a", 10, 12, 20, true);
        telemetry.record_completion(3, "private-lane-b", 5, 7, 9, true);
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.committed_ready_ranges, 1);
        assert_eq!(snapshot.committed_ready_records, 4);
        assert_eq!(snapshot.queue_depth, 0);
        assert_eq!(snapshot.deferred_jobs, 1);
        assert_eq!(snapshot.defer_rejected, 1);
        assert_eq!(snapshot.retries_scheduled, 1);
        assert_eq!(snapshot.retries_due, 1);
        let shard = &snapshot.shards[0];
        assert_eq!(shard.shard, 3);
        assert_eq!(shard.completions, 2);
        assert_eq!(shard.cursor_advanced_records, 4);
        assert_eq!(shard.cursor_min_offset, 7);
        assert_eq!(shard.cursor_max_offset, 12);
        assert_eq!(shard.completion_gap_records, 10);
        assert_eq!(shard.tracked_cursor_lanes, 2);
    }

    #[test]
    fn committed_work_grows_gap_without_any_completion() {
        let telemetry = DispatchTelemetry::new();
        telemetry.record_completion(0, "lane", 0, 1, 1, true);
        telemetry.record_pending(0, "lane", 1, 5);
        assert_eq!(telemetry.snapshot().shards[0].completion_gap_records, 4);
        telemetry.record_pending(1, "new-lane", 0, 3);
        assert_eq!(telemetry.snapshot().shards[1].completion_gap_records, 3);
        telemetry.record_completion(0, "lane", 0, 1, 1, false);
        assert_eq!(
            telemetry.snapshot().shards[0].completion_gap_records,
            4,
            "stale completion must not roll HWM back"
        );
    }
}
