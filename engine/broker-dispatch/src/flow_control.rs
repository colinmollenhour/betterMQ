//! Flow control: key, parallelism, rate/period, waitlist.

use broker_partition::ResolvedFlow;
use broker_storage::StoredMessage;
use chrono::Utc;
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::memory_guard::MemoryGuard;
use crate::worker::DispatchEngine;

static ENQUEUE_SEQ: AtomicU64 = AtomicU64::new(0);

#[cfg(not(test))]
const IDLE_LANE_TTL: std::time::Duration = std::time::Duration::from_secs(60);
#[cfg(test)]
const IDLE_LANE_TTL: std::time::Duration = std::time::Duration::from_millis(100);

type LaneMap = Arc<Mutex<HashMap<FlowKey, Arc<FlowLane>>>>;
type Drainers = Arc<parking_lot::Mutex<HashMap<FlowKey, JoinHandle<()>>>>;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct FlowKey {
    pub endpoint_id: Uuid,
    pub flow_key: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FlowControlInfo {
    pub flow_key: String,
    pub endpoint_id: Uuid,
    pub wait_list_size: usize,
    pub parallelism_max: u32,
    pub parallelism_count: u32,
    pub rate_max: u32,
    pub rate_count: u32,
    pub rate_period_secs: u64,
    pub rate_period_start_ms: i64,
    pub paused: bool,
    pub pinned_parallelism: bool,
    pub pinned_rate: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct GlobalParallelismInfo {
    pub parallelism_max: Option<u32>,
    pub parallelism_count: u32,
}

#[derive(Debug, Clone)]
struct PinnedLimits {
    parallelism: Option<u32>,
    rate: Option<u32>,
    period_secs: Option<u64>,
}

struct FlowWork {
    engine: DispatchEngine,
    msg: Arc<StoredMessage>,
    backfill: bool,
    _pending_permit: tokio::sync::OwnedSemaphorePermit,
    _ownership: Option<crate::worker::DeliveryOwnership>,
}

struct OrderedWork {
    backfill: bool,
    priority: u8,
    published_at_ms: i64,
    partition: u32,
    offset: u64,
    seq: u64,
    work: FlowWork,
}

type FifoOrder = (i64, u32, u64, u64);
type PriorityOrder = (bool, u8, Reverse<u64>, Reverse<u64>);

#[derive(Default)]
struct WorkQueue {
    items: HashMap<u64, OrderedWork>,
    fifo: BTreeSet<FifoOrder>,
    priority: BTreeSet<PriorityOrder>,
}

impl WorkQueue {
    fn len(&self) -> usize {
        self.items.len()
    }

    fn push(&mut self, item: OrderedWork) {
        self.fifo.insert(fifo_order(&item));
        self.priority.insert(priority_order(&item));
        self.items.insert(item.seq, item);
    }

    fn pop(&mut self, strict_fifo: bool) -> Option<OrderedWork> {
        let sequence = if strict_fifo {
            self.fifo.first().map(|key| key.3)
        } else {
            self.priority.last().map(|key| key.3 .0)
        }?;
        let item = self.items.remove(&sequence)?;
        self.fifo.remove(&fifo_order(&item));
        self.priority.remove(&priority_order(&item));
        Some(item)
    }
}

fn fifo_order(item: &OrderedWork) -> FifoOrder {
    (item.published_at_ms, item.partition, item.offset, item.seq)
}

fn priority_order(item: &OrderedWork) -> PriorityOrder {
    (
        !item.backfill,
        item.priority,
        Reverse(item.offset),
        Reverse(item.seq),
    )
}

struct RateWindow {
    max: u32,
    period_secs: u64,
    period_start_ms: i64,
    count: u32,
}

impl RateWindow {
    fn new(max: u32, period_secs: u64) -> Self {
        Self {
            max,
            period_secs,
            period_start_ms: Utc::now().timestamp_millis(),
            count: 0,
        }
    }

    fn try_acquire(&mut self, now_ms: i64) -> bool {
        if self.max == 0 {
            return true;
        }
        let period_ms = (self.period_secs * 1000) as i64;
        if now_ms.saturating_sub(self.period_start_ms) >= period_ms {
            self.period_start_ms = now_ms;
            self.count = 0;
        }
        if self.count < self.max {
            self.count += 1;
            true
        } else {
            false
        }
    }

    fn reset(&mut self, now_ms: i64) {
        self.period_start_ms = now_ms;
        self.count = 0;
    }
}

struct FlowLane {
    limits: Mutex<ResolvedFlow>,
    waitlist: Mutex<WorkQueue>,
    active: AtomicU32,
    rate: Mutex<RateWindow>,
    paused: Mutex<bool>,
    pinned: Mutex<PinnedLimits>,
    members: Mutex<HashSet<(String, u32, u64)>>,
    notify: Notify,
}

#[derive(Clone)]
pub struct FlowController {
    lanes: LaneMap,
    global_max_parallelism: Option<u32>,
    global_capacity: Option<Arc<Semaphore>>,
    global_active: Arc<AtomicU32>,
    memory_guard: Arc<MemoryGuard>,
    stop: Arc<AtomicBool>,
    drainers: Drainers,
    pending_capacity: Arc<Semaphore>,
}

impl FlowController {
    pub fn new(global_max_parallelism: Option<u32>, memory_guard: Arc<MemoryGuard>) -> Self {
        Self {
            lanes: Arc::new(Mutex::new(HashMap::new())),
            global_max_parallelism,
            global_capacity: global_max_parallelism
                .map(|max| Arc::new(Semaphore::new(max as usize))),
            global_active: Arc::new(AtomicU32::new(0)),
            memory_guard,
            stop: Arc::new(AtomicBool::new(false)),
            drainers: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            pending_capacity: Arc::new(Semaphore::new(
                std::env::var("BETTERMQ_DISPATCH_QUEUE_CAP")
                    .ok()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(4096)
                    .clamp(1, Semaphore::MAX_PERMITS),
            )),
        }
    }

    pub fn take_drainer_tasks(&self) -> Vec<JoinHandle<()>> {
        self.stop.store(true, AtomicOrdering::Release);
        std::mem::take(&mut *self.drainers.lock())
            .into_values()
            .collect()
    }

    pub(crate) async fn discard_pending(&self) {
        self.lanes.lock().await.clear();
    }

    pub async fn submit(
        &self,
        engine: DispatchEngine,
        lane_owner: Uuid,
        msg: StoredMessage,
        limits: ResolvedFlow,
        backfill: bool,
    ) {
        self.submit_work(engine, lane_owner, msg, limits, backfill, None)
            .await;
    }

    pub(crate) async fn submit_owned(
        &self,
        engine: DispatchEngine,
        lane_owner: Uuid,
        msg: StoredMessage,
        limits: ResolvedFlow,
        ownership: Option<crate::worker::DeliveryOwnership>,
    ) {
        self.submit_work(engine, lane_owner, msg, limits, false, ownership)
            .await;
    }

    async fn submit_work(
        &self,
        engine: DispatchEngine,
        lane_owner: Uuid,
        msg: StoredMessage,
        limits: ResolvedFlow,
        backfill: bool,
        ownership: Option<crate::worker::DeliveryOwnership>,
    ) {
        let flow_key = limits.key.clone();
        // Bound aggregate hydrated flow work, not just each individual key's
        // waitlist. Reject before allocating a new lane; durable backfill retries.
        let Ok(pending_permit) = self.pending_capacity.clone().try_acquire_owned() else {
            engine.record_flow_rejected();
            return;
        };
        let key = FlowKey {
            endpoint_id: lane_owner,
            flow_key: flow_key.clone(),
        };
        let lane = self.get_or_create_lane(key, limits).await;
        let member = (msg.topic.clone(), msg.partition, msg.offset);
        let mut members = lane.members.lock().await;
        if members.contains(&member) {
            return;
        }
        let work = FlowWork {
            engine,
            msg: Arc::new(msg),
            backfill,
            _pending_permit: pending_permit,
            _ownership: ownership,
        };
        let mut heap = lane.waitlist.lock().await;
        let cap = std::env::var("BETTERMQ_FLOW_WAITLIST_CAP")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8192)
            .max(1);
        if heap.len() >= cap {
            tracing::warn!(
                endpoint_id = %lane_owner,
                flow_key = %flow_key,
                cap,
                "flow waitlist full; dropping work (backfill will retry)"
            );
            return;
        }
        heap.push(OrderedWork {
            backfill: work.backfill,
            priority: work.msg.priority,
            published_at_ms: work.msg.published_at_ms,
            partition: work.msg.partition,
            offset: work.msg.offset,
            seq: ENQUEUE_SEQ.fetch_add(1, AtomicOrdering::Relaxed),
            work,
        });
        members.insert(member);
        lane.notify.notify_one();
    }

    pub async fn get(&self, endpoint_id: Uuid, flow_key: &str) -> Option<FlowControlInfo> {
        let lane = self.lane_ref(endpoint_id, flow_key).await?;
        Some(self.info_from_lane(&lane).await)
    }

    pub async fn list_keys(&self, endpoint_id: Uuid) -> Vec<FlowControlInfo> {
        let lanes: Vec<_> = self
            .lanes
            .lock()
            .await
            .iter()
            .filter(|(key, _)| key.endpoint_id == endpoint_id)
            .map(|(_, lane)| lane.clone())
            .collect();
        let mut out = Vec::new();
        for lane in lanes {
            out.push(self.info_from_lane(&lane).await);
        }
        out
    }

    pub async fn global_parallelism(&self) -> GlobalParallelismInfo {
        GlobalParallelismInfo {
            parallelism_max: self.global_max_parallelism,
            parallelism_count: self.global_active.load(AtomicOrdering::Relaxed),
        }
    }

    pub async fn pending_count(&self) -> usize {
        let lanes: Vec<_> = self.lanes.lock().await.values().cloned().collect();
        let mut pending = 0usize;
        for lane in lanes {
            pending = pending
                .saturating_add(lane.waitlist.lock().await.len())
                .saturating_add(lane.active.load(AtomicOrdering::Acquire) as usize);
        }
        pending
    }

    /// Create the in-memory lane if missing (so pause/pin work before first delivery).
    pub async fn ensure_lane(&self, endpoint_id: Uuid, flow_key: &str, limits: ResolvedFlow) {
        let _ = self
            .get_or_create_lane(
                FlowKey {
                    endpoint_id,
                    flow_key: flow_key.to_string(),
                },
                limits,
            )
            .await;
    }

    pub async fn pause(&self, endpoint_id: Uuid, flow_key: &str) -> bool {
        if let Some(lane) = self.lane_ref(endpoint_id, flow_key).await {
            *lane.paused.lock().await = true;
            return true;
        }
        false
    }

    pub async fn resume(&self, endpoint_id: Uuid, flow_key: &str) -> bool {
        if let Some(lane) = self.lane_ref(endpoint_id, flow_key).await {
            *lane.paused.lock().await = false;
            lane.notify.notify_one();
            return true;
        }
        false
    }

    pub async fn pin(
        &self,
        endpoint_id: Uuid,
        flow_key: &str,
        parallelism: Option<u32>,
        rate: Option<u32>,
        period_secs: Option<u64>,
    ) -> bool {
        if let Some(lane) = self.lane_ref(endpoint_id, flow_key).await {
            let mut pin = lane.pinned.lock().await;
            if parallelism.is_some() {
                pin.parallelism = parallelism;
            }
            if rate.is_some() {
                pin.rate = rate;
            }
            if period_secs.is_some() {
                pin.period_secs = period_secs;
            }
            drop(pin);
            self.apply_pins(&lane).await;
            lane.notify.notify_one();
            return true;
        }
        false
    }

    pub async fn unpin(
        &self,
        endpoint_id: Uuid,
        flow_key: &str,
        parallelism: bool,
        rate: bool,
    ) -> bool {
        if let Some(lane) = self.lane_ref(endpoint_id, flow_key).await {
            let mut pin = lane.pinned.lock().await;
            if parallelism {
                pin.parallelism = None;
            }
            if rate {
                pin.rate = None;
            }
            drop(pin);
            lane.notify.notify_one();
            return true;
        }
        false
    }

    pub async fn reset_rate(&self, endpoint_id: Uuid, flow_key: &str) -> bool {
        if let Some(lane) = self.lane_ref(endpoint_id, flow_key).await {
            lane.rate.lock().await.reset(Utc::now().timestamp_millis());
            lane.notify.notify_one();
            return true;
        }
        false
    }

    async fn info_from_lane(&self, lane: &FlowLane) -> FlowControlInfo {
        // Same lock order as apply_pins: pinned -> limits -> rate.
        let pin = lane.pinned.lock().await;
        let lim = lane.limits.lock().await;
        let rate = lane.rate.lock().await;
        FlowControlInfo {
            flow_key: lim.key.clone(),
            endpoint_id: Uuid::nil(), // filled by caller if needed
            wait_list_size: lane.waitlist.lock().await.len(),
            parallelism_max: lim.parallelism,
            parallelism_count: lane.active.load(AtomicOrdering::Relaxed),
            rate_max: lim.rate,
            rate_count: rate.count,
            rate_period_secs: lim.period_secs,
            rate_period_start_ms: rate.period_start_ms,
            paused: *lane.paused.lock().await,
            pinned_parallelism: pin.parallelism.is_some(),
            pinned_rate: pin.rate.is_some(),
        }
    }

    async fn lane_ref(&self, endpoint_id: Uuid, flow_key: &str) -> Option<Arc<FlowLane>> {
        let lanes = self.lanes.lock().await;
        lanes
            .get(&FlowKey {
                endpoint_id,
                flow_key: flow_key.to_string(),
            })
            .cloned()
    }

    async fn get_or_create_lane(&self, key: FlowKey, limits: ResolvedFlow) -> Arc<FlowLane> {
        let mut map = self.lanes.lock().await;
        if let Some(existing) = map.get(&key).cloned() {
            drop(map);
            // Update limits in place — never replace the lane (that orphans the waitlist
            // and leaks a drainer task parked on the old Notify).
            {
                let mut el = existing.limits.lock().await;
                el.parallelism = limits.parallelism;
                el.rate = limits.rate;
                el.period_secs = limits.period_secs;
            }
            {
                let mut rate = existing.rate.lock().await;
                rate.max = limits.rate;
                rate.period_secs = limits.period_secs.max(1);
            }
            existing.notify.notify_one();
            self.apply_pins(&existing).await;
            return existing;
        }
        let lane = Arc::new(FlowLane {
            limits: Mutex::new(limits.clone()),
            waitlist: Mutex::new(WorkQueue::default()),
            active: AtomicU32::new(0),
            rate: Mutex::new(RateWindow::new(limits.rate, limits.period_secs)),
            paused: Mutex::new(false),
            pinned: Mutex::new(PinnedLimits {
                parallelism: None,
                rate: None,
                period_secs: None,
            }),
            members: Mutex::new(HashSet::new()),
            notify: Notify::new(),
        });
        if !self.stop.load(AtomicOrdering::Acquire) {
            spawn_lane_drainer(
                lane.clone(),
                (key.clone(), self.lanes.clone()),
                self.global_capacity.clone(),
                self.global_active.clone(),
                self.memory_guard.clone(),
                self.stop.clone(),
                self.drainers.clone(),
            );
        }
        map.insert(key, lane.clone());
        lane
    }

    async fn apply_pins(&self, lane: &FlowLane) {
        let pin = lane.pinned.lock().await;
        let mut lim = lane.limits.lock().await;
        if let Some(p) = pin.parallelism {
            lim.parallelism = p.max(1);
        }
        if let Some(r) = pin.rate {
            lim.rate = r;
            lane.rate.lock().await.max = r;
        }
        if let Some(period) = pin.period_secs {
            lim.period_secs = period.max(1);
            lane.rate.lock().await.period_secs = period.max(1);
        }
    }
}

fn spawn_lane_drainer(
    lane: Arc<FlowLane>,
    registry: (FlowKey, LaneMap),
    global_capacity: Option<Arc<Semaphore>>,
    global_active: Arc<AtomicU32>,
    memory_guard: Arc<MemoryGuard>,
    stop: Arc<AtomicBool>,
    drainers: Drainers,
) {
    let (key, lanes) = registry;
    let task_key = key.clone();
    let task_drainers = drainers.clone();
    let handle = tokio::spawn(async move {
        loop {
            if stop.load(AtomicOrdering::Acquire) {
                break;
            }
            if memory_guard.is_critical() {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if stop.load(AtomicOrdering::Acquire) {
                    break;
                }
                lane.notify.notify_one();
                continue;
            }

            if *lane.paused.lock().await {
                if stop.load(AtomicOrdering::Acquire) {
                    break;
                }
                lane.notify.notified().await;
                continue;
            }

            let parallelism = lane.limits.lock().await.parallelism;
            let active = lane.active.load(AtomicOrdering::Relaxed);
            if active >= parallelism {
                lane.notify.notified().await;
                continue;
            }

            let now_ms = Utc::now().timestamp_millis();
            if !lane.rate.lock().await.try_acquire(now_ms) {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                if stop.load(AtomicOrdering::Acquire) {
                    break;
                }
                lane.notify.notify_one();
                continue;
            }

            let next = {
                let mut heap = lane.waitlist.lock().await;
                heap.pop(parallelism == 1)
            };
            let Some(ordered) = next else {
                if tokio::time::timeout(IDLE_LANE_TTL, lane.notify.notified())
                    .await
                    .is_err()
                    && evict_idle_lane(&lanes, &task_key, &lane, &task_drainers).await
                {
                    break;
                }
                continue;
            };

            // Semaphore waiters are global and FIFO; releasing one slot wakes
            // another lane without requiring a lane-specific submission event.
            let global_permit = if let Some(capacity) = &global_capacity {
                match capacity.clone().acquire_owned().await {
                    Ok(permit) => Some(permit),
                    Err(_) => break,
                }
            } else {
                None
            };

            // Operator controls may have changed while global capacity was
            // unavailable. Requeue reserved work if a lane was paused meanwhile.
            if *lane.paused.lock().await
                || memory_guard.is_critical()
                || stop.load(AtomicOrdering::Acquire)
            {
                lane.waitlist.lock().await.push(ordered);
                continue;
            }
            let parallelism = lane.limits.lock().await.parallelism;
            if lane.active.load(AtomicOrdering::Relaxed) >= parallelism {
                lane.waitlist.lock().await.push(ordered);
                continue;
            }

            lane.active.fetch_add(1, AtomicOrdering::Relaxed);
            global_active.fetch_add(1, AtomicOrdering::Relaxed);

            let lane2 = lane.clone();
            let global_active2 = global_active.clone();
            tokio::spawn(async move {
                let _global_permit = global_permit;
                let member = (
                    ordered.work.msg.topic.clone(),
                    ordered.work.msg.partition,
                    ordered.work.msg.offset,
                );
                ordered
                    .work
                    .engine
                    .deliver_message(&ordered.work.msg)
                    .await
                    .ok();
                lane2.members.lock().await.remove(&member);
                lane2.active.fetch_sub(1, AtomicOrdering::Relaxed);
                global_active2.fetch_sub(1, AtomicOrdering::Relaxed);
                lane2.notify.notify_one();
            });
        }
        let map = lanes.lock().await;
        if map
            .get(&task_key)
            .is_some_and(|current| Arc::ptr_eq(current, &lane))
        {
            task_drainers.lock().remove(&task_key);
        }
    });
    drainers.lock().insert(key, handle);
}

async fn evict_idle_lane(
    lanes: &LaneMap,
    key: &FlowKey,
    lane: &Arc<FlowLane>,
    drainers: &Drainers,
) -> bool {
    let mut map = lanes.lock().await;
    // Map + drainer are the only owners: no concurrent submit/control operation
    // can still enqueue onto this lane after it has been detached from the map.
    if Arc::strong_count(lane) != 2 || lane.active.load(AtomicOrdering::Acquire) != 0 {
        return false;
    }
    let (Ok(waitlist), Ok(paused), Ok(pinned)) = (
        lane.waitlist.try_lock(),
        lane.paused.try_lock(),
        lane.pinned.try_lock(),
    ) else {
        return false;
    };
    if waitlist.len() != 0
        || *paused
        || pinned.parallelism.is_some()
        || pinned.rate.is_some()
        || pinned.period_secs.is_some()
    {
        return false;
    }
    map.remove(key);
    // Detach the old handle while still holding the registry lock, before a new
    // submission can install a replacement lane/drainer with the same key.
    drainers.lock().remove(key);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_guard::MemoryGuardConfig;
    use std::time::Duration;

    #[tokio::test]
    async fn aggregate_flow_capacity_rejects_before_allocating_another_lane() {
        let dir = tempfile::tempdir().unwrap();
        let broker = broker_partition::Broker::open(broker_partition::BrokerConfig::new(
            dir.path().to_path_buf(),
        ))
        .unwrap();
        let response = broker
            .publish(
                serde_json::from_value(serde_json::json!({
                    "url": "https://example.com/hook", "secret": "secret", "payload": "test"
                }))
                .unwrap(),
            )
            .unwrap();
        broker.flush_wal().unwrap();
        let msg = broker
            .read_message(
                &response.topic,
                response.partition.unwrap(),
                response.offset.unwrap(),
            )
            .unwrap();
        let mut engine = DispatchEngine::new_broker_only(broker, crate::DispatchConfig::default());
        engine.flow.pending_capacity = Arc::new(Semaphore::new(1));
        let owner = Uuid::new_v4();
        let limits = ResolvedFlow {
            key: "a".into(),
            parallelism: 1,
            rate: 0,
            period_secs: 1,
        };
        engine.flow.ensure_lane(owner, "a", limits.clone()).await;
        engine.flow.pause(owner, "a").await;
        engine
            .flow
            .submit(engine.clone(), owner, msg.clone(), limits.clone(), false)
            .await;
        engine
            .flow
            .submit(engine.clone(), Uuid::new_v4(), msg, limits, false)
            .await;
        assert_eq!(engine.flow.lanes.lock().await.len(), 1);
        assert_eq!(engine.flow.pending_count().await, 1);
        assert_eq!(engine.telemetry_snapshot().queue_rejected, 1);
        engine.shutdown_background().await;
        assert_eq!(engine.flow.pending_capacity.available_permits(), 1);
    }

    #[tokio::test]
    async fn idle_lanes_and_their_drainers_are_reclaimed() {
        let controller = FlowController::new(
            None,
            Arc::new(MemoryGuard::new(MemoryGuardConfig::default())),
        );
        let owner = Uuid::new_v4();
        for i in 0..100 {
            let key = i.to_string();
            controller
                .ensure_lane(
                    owner,
                    &key,
                    ResolvedFlow {
                        key: key.clone(),
                        parallelism: 1,
                        rate: 0,
                        period_secs: 1,
                    },
                )
                .await;
        }
        controller.pause(owner, "0").await;
        controller.pin(owner, "1", Some(1), None, None).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while controller.lanes.lock().await.len() != 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(controller.drainers.lock().len(), 2);
        for task in controller.take_drainer_tasks() {
            task.abort();
        }
    }

    #[tokio::test]
    async fn operator_controls_do_not_deadlock() {
        let controller = FlowController::new(
            None,
            Arc::new(MemoryGuard::new(MemoryGuardConfig::default())),
        );
        let owner = Uuid::new_v4();
        tokio::time::timeout(Duration::from_secs(1), async {
            controller
                .ensure_lane(
                    owner,
                    "key",
                    ResolvedFlow {
                        key: "key".into(),
                        parallelism: 1,
                        rate: 0,
                        period_secs: 1,
                    },
                )
                .await;
            assert!(
                controller
                    .pin(owner, "key", Some(2), Some(3), Some(4))
                    .await
            );
            let info = controller.get(owner, "key").await.unwrap();
            assert_eq!(info.parallelism_max, 2);
            assert_eq!(info.rate_max, 3);
            assert_eq!(controller.list_keys(owner).await.len(), 1);
            assert!(controller.unpin(owner, "key", true, true).await);
            assert!(
                !controller
                    .get(owner, "key")
                    .await
                    .unwrap()
                    .pinned_parallelism
            );
            assert!(controller.pause(owner, "key").await);
            assert!(controller.resume(owner, "key").await);
        })
        .await
        .expect("operator controls must finish");
        for task in controller.take_drainer_tasks() {
            task.abort();
        }
    }

    #[tokio::test]
    async fn inspection_does_not_hold_global_map_while_waiting_on_lane() {
        let controller = FlowController::new(
            None,
            Arc::new(MemoryGuard::new(MemoryGuardConfig::default())),
        );
        let owner = Uuid::new_v4();
        let limits = ResolvedFlow {
            key: "a".into(),
            parallelism: 1,
            rate: 0,
            period_secs: 1,
        };
        controller.ensure_lane(owner, "a", limits.clone()).await;
        let lane = controller.lane_ref(owner, "a").await.unwrap();
        let guard = lane.pinned.lock().await;
        let other = controller.clone();
        let inspection = tokio::spawn(async move { other.get(owner, "a").await });
        tokio::task::yield_now().await;
        tokio::time::timeout(
            Duration::from_secs(1),
            controller.ensure_lane(owner, "b", limits),
        )
        .await
        .expect("unrelated lanes must remain available");
        drop(guard);
        inspection.await.unwrap();
        for task in controller.take_drainer_tasks() {
            task.abort();
        }
    }
}
