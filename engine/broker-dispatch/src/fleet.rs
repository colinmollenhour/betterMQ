//! Dispatch fleet (`--dispatch-fleet`): deliver jobs claimed from broker lease APIs.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use broker_storage::StoredMessage;
use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

use crate::host_blocker::HostBlocker;
use crate::lease::{ClaimedJob, LeaseClient, MAX_CLAIM_JOBS};
use crate::memory_guard::MemoryGuard;
use crate::outbound::{apply_to_reqwest, build_outbound};
use crate::telemetry::DispatchTelemetry;
use crate::worker::{discard_body_capped, DispatchConfig, DispatchError};

/// Retry delay when a claim arrived without its body (matches the local path).
const PAYLOAD_RETRY_MS: u64 = 10_000;

/// Heartbeat cadence: a third of the 30 s lease TTL the client requests.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Default jobs requested per claim (`BETTERMQ_FLEET_CLAIM_BATCH`).
pub const DEFAULT_CLAIM_BATCH: usize = 16;

/// Default wait after every broker answered a claim with no jobs.
pub const DEFAULT_IDLE_POLL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone)]
pub struct FleetOptions {
    /// Jobs in flight per fleet process (`BETTERMQ_FLEET_CONCURRENCY`).
    pub concurrency: usize,
    /// Most jobs requested by one claim; each claim asks for
    /// `min(free permits, claim_batch)`.
    pub claim_batch: usize,
    /// Back-off after a full round of brokers returned no jobs.
    pub idle_poll: Duration,
    /// How often each held lease is renewed while its job is in flight.
    pub heartbeat_interval: Duration,
    /// Called for every failed claim (process metrics live above this crate).
    pub on_claim_failure: Option<fn()>,
}

impl FleetOptions {
    pub fn from_env() -> Self {
        Self {
            concurrency: crate::lease::fleet_concurrency(),
            claim_batch: std::env::var("BETTERMQ_FLEET_CLAIM_BATCH")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_CLAIM_BATCH),
            idle_poll: DEFAULT_IDLE_POLL,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            on_claim_failure: None,
        }
    }
}

/// A fleet worker reports ready while a claim succeeded this recently.
pub const FLEET_READY_WINDOW: Duration = Duration::from_secs(60);

/// Storage-free webhook delivery for fleet workers. Holds no WAL, metadata or
/// retry state: the broker that leased a job owns its cursor and attempts.
#[derive(Clone)]
pub struct FleetDelivery {
    inner: Arc<FleetDeliveryInner>,
}

struct FleetDeliveryInner {
    config: DispatchConfig,
    client: reqwest::Client,
    long_client: reqwest::Client,
    host_blocker: Arc<HostBlocker>,
    memory_guard: Arc<MemoryGuard>,
    telemetry: DispatchTelemetry,
    active_http: AtomicU64,
    monitor: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

struct ActiveHttp<'a>(&'a AtomicU64);

impl Drop for ActiveHttp<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl FleetDelivery {
    pub fn new(config: DispatchConfig) -> Self {
        let long_secs =
            crate::lease::long_wait_tier_secs().unwrap_or(config.long_http_timeout_secs);
        // Same egress policy as local delivery: addresses are checked when a
        // connection is established, so pooled connections stay safe.
        let client = |secs: u64| {
            reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(secs))
                .redirect(reqwest::redirect::Policy::none())
                .dns_resolver(Arc::new(crate::egress::PolicyResolver))
                .no_proxy()
                .build()
                .expect("reqwest client")
        };
        let memory_guard = Arc::new(MemoryGuard::new(config.memory_guard.clone()));
        let monitor = memory_guard.spawn_monitor();
        Self {
            inner: Arc::new(FleetDeliveryInner {
                client: client(config.http_timeout_secs),
                long_client: client(long_secs),
                host_blocker: Arc::new(HostBlocker::new(config.host_blocker.clone())),
                memory_guard,
                telemetry: DispatchTelemetry::new(),
                active_http: AtomicU64::new(0),
                monitor: parking_lot::Mutex::new(monitor),
                config,
            }),
        }
    }

    /// Webhook calls currently in flight.
    pub fn active_deliveries(&self) -> u64 {
        self.inner.active_http.load(Ordering::Acquire)
    }

    /// Stop the memory monitor (call once on shutdown).
    pub fn shutdown(&self) {
        self.inner.memory_guard.stop();
        if let Some(handle) = self.inner.monitor.lock().take() {
            handle.abort();
        }
    }

    /// Push one leased message over HTTP. The caller completes or fails the
    /// lease on the broker, which owns the attempt count; `prior_attempts` is
    /// the count the broker reported on the claim.
    pub async fn push(
        &self,
        msg: &StoredMessage,
        committed_hwm: u64,
        prior_attempts: u32,
    ) -> Result<(), DispatchError> {
        let inner = &*self.inner;
        inner.active_http.fetch_add(1, Ordering::AcqRel);
        let _active = ActiveHttp(&inner.active_http);
        inner.memory_guard.wait_below_limit().await;
        if msg.offset >= committed_hwm {
            return Err(DispatchError::Uncommitted);
        }
        // Blob-backed bodies arrive from the claim with an empty inline payload;
        // the caller must load them first or the destination gets an empty body.
        if msg.payload_ref_json.is_some() && msg.payload.is_empty() {
            return Err(DispatchError::PayloadUnresolved);
        }
        let url = msg
            .destination_url
            .as_deref()
            .filter(|u| !u.is_empty())
            .ok_or(DispatchError::NoDestination)?;
        if inner.host_blocker.is_blocked_async(url).await {
            return Err(DispatchError::HostBlocked);
        }
        let outbound = build_outbound(msg);
        let long = msg.payload.len() >= inner.config.long_payload_threshold_bytes;
        if prior_attempts > 0 {
            inner.telemetry.record_retry_due();
        }
        crate::egress::validate_destination_url(url).map_err(|error| {
            inner.telemetry.record_http_outcome(5, Duration::ZERO);
            DispatchError::Egress(error.to_string())
        })?;
        let http = if long {
            &inner.long_client
        } else {
            &inner.client
        };
        let started = std::time::Instant::now();
        let response = apply_to_reqwest(http, url, outbound).send().await;
        crate::worker::record_http_result(&inner.telemetry, &response, started.elapsed());
        let retry_after_ms;
        let failure = match response {
            Ok(response) if response.status().is_success() => {
                inner.host_blocker.record_success_async(url).await;
                discard_body_capped(response).await;
                return Ok(());
            }
            Ok(response) => {
                let code = response.status().as_u16();
                retry_after_ms = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(|secs| secs.saturating_mul(1000));
                discard_body_capped(response).await;
                if inner.config.non_retry_status_codes.contains(&code) {
                    return Err(DispatchError::NonRetryable(format!("HTTP {code}")));
                }
                inner.host_blocker.record_transport_failure_async(url).await;
                format!("HTTP {code}")
            }
            Err(error) if crate::egress::is_policy_failure(&error) => {
                return Err(DispatchError::Egress(
                    "destination blocked by egress policy".into(),
                ));
            }
            Err(error) => {
                let error = error.without_url();
                retry_after_ms = None;
                inner.host_blocker.record_transport_failure_async(url).await;
                format!("transport error: {error}")
            }
        };
        let attempts = prior_attempts.saturating_add(1);
        if attempts > msg.max_retries {
            inner.telemetry.record_retry_exhausted();
            return Err(DispatchError::RetryExhausted(format!(
                "{failure}; exhausted {} delivery attempt(s)",
                msg.max_retries.saturating_add(1)
            )));
        }
        let backoff_cfg = msg
            .retry_backoff
            .as_ref()
            .unwrap_or(&inner.config.retry_defaults.backoff);
        let wait_ms = retry_after_ms
            .unwrap_or_else(|| backoff_cfg.delay_ms(attempts))
            .min(backoff_cfg.max_ms)
            .max(1);
        inner.telemetry.record_retry_scheduled();
        Err(DispatchError::RetryDeferred {
            retry_after_ms: wait_ms,
            reason: failure,
        })
    }
}

/// Liveness and drain state of a running fleet loop.
#[derive(Debug, Clone, Serialize)]
pub struct FleetStatus {
    pub running: bool,
    pub draining: bool,
    pub ready: bool,
    pub in_flight_jobs: usize,
    /// Unix ms of the last claim any broker answered; `null` before the first.
    pub last_claim_ok_ms: Option<i64>,
}

struct FleetShared {
    draining: AtomicBool,
    last_claim_ok_ms: AtomicI64,
    sem: Arc<Semaphore>,
    concurrency: usize,
    task: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

/// Handle to the fleet loop: readiness, graceful drain and shutdown.
#[derive(Clone)]
pub struct FleetHandle {
    shared: Arc<FleetShared>,
}

impl FleetHandle {
    pub fn status(&self) -> FleetStatus {
        let shared = &*self.shared;
        let running = shared
            .task
            .lock()
            .as_ref()
            .is_some_and(|task| !task.is_finished());
        let draining = shared.draining.load(Ordering::Acquire);
        let last = shared.last_claim_ok_ms.load(Ordering::Acquire);
        let last_claim_ok_ms = (last > 0).then_some(last);
        let fresh = last_claim_ok_ms.is_some_and(|at| {
            chrono::Utc::now().timestamp_millis().saturating_sub(at)
                <= FLEET_READY_WINDOW.as_millis() as i64
        });
        FleetStatus {
            running,
            draining,
            ready: running && !draining && fresh,
            in_flight_jobs: shared
                .concurrency
                .saturating_sub(shared.sem.available_permits()),
            last_claim_ok_ms,
        }
    }

    /// Stop claiming new jobs; deliveries already claimed keep running.
    pub fn begin_drain(&self) {
        self.shared.draining.store(true, Ordering::Release);
    }

    /// Stop claiming, wait up to `timeout` for in-flight jobs to finish and
    /// report to their broker, then stop the loop. Returns true when every
    /// in-flight job finished; leases of the others expire on the broker and
    /// are offered again.
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.begin_drain();
        let shared = &*self.shared;
        let permits = u32::try_from(shared.concurrency).unwrap_or(u32::MAX);
        let drained = tokio::time::timeout(timeout, shared.sem.acquire_many(permits))
            .await
            .is_ok_and(|permit| permit.is_ok());
        self.abort();
        drained
    }

    /// Stop the claim loop immediately.
    pub fn abort(&self) {
        if let Some(task) = self.shared.task.lock().take() {
            task.abort();
        }
    }
}

/// Claim jobs from every broker in `client` and deliver them until shut down.
///
/// The loop keeps claiming while delivery permits are free and brokers return
/// work; it only waits `idle_poll` once a whole round of brokers came back
/// empty. Each claim asks for as many jobs as there are free permits (capped
/// by `claim_batch`), and every claimed job runs in its own task that
/// heartbeats its lease from the moment it is claimed.
pub fn spawn_fleet(
    delivery: FleetDelivery,
    client: LeaseClient,
    options: FleetOptions,
) -> FleetHandle {
    let concurrency = options.concurrency.max(1);
    let sem = Arc::new(Semaphore::new(concurrency));
    let shared = Arc::new(FleetShared {
        draining: AtomicBool::new(false),
        last_claim_ok_ms: AtomicI64::new(0),
        sem: sem.clone(),
        concurrency,
        task: parking_lot::Mutex::new(None),
    });
    let handle = FleetHandle {
        shared: shared.clone(),
    };
    let claim_batch = options.claim_batch.clamp(1, MAX_CLAIM_JOBS);
    let task = tokio::spawn(async move {
        let mut brokers: Vec<BrokerHealth> = client
            .broker_urls()
            .iter()
            .map(|url| BrokerHealth::new(url.clone()))
            .collect();
        let mut rr = 0usize;
        let mut empty_streak = 0usize;
        loop {
            if brokers.is_empty() || shared.draining.load(Ordering::Acquire) {
                tokio::time::sleep(options.idle_poll).await;
                continue;
            }
            if empty_streak >= brokers.len() {
                empty_streak = 0;
                tokio::time::sleep(options.idle_poll).await;
            }
            // Skip brokers that recently failed; wait only if all of them did.
            let now = Instant::now();
            let Some(index) = (0..brokers.len())
                .map(|step| (rr + 1 + step) % brokers.len())
                .find(|&i| brokers[i].retry_at <= now)
            else {
                let next = brokers.iter().map(|b| b.retry_at).min().unwrap_or(now);
                tokio::time::sleep_until(next.into()).await;
                continue;
            };
            rr = index;
            let Ok(first) = sem.clone().acquire_owned().await else {
                break;
            };
            let mut permits = vec![first];
            while permits.len() < claim_batch {
                match sem.clone().try_acquire_owned() {
                    Ok(permit) => permits.push(permit),
                    Err(_) => break,
                }
            }
            let broker = &mut brokers[index];
            let claimed =
                claim_and_deliver(&delivery, &client, &broker.url, permits, &options).await;
            if claimed.is_ok() {
                shared
                    .last_claim_ok_ms
                    .store(chrono::Utc::now().timestamp_millis(), Ordering::Release);
            }
            match claimed {
                Ok(0) => {
                    broker.record_success();
                    empty_streak += 1;
                }
                Ok(_) => {
                    broker.record_success();
                    empty_streak = 0;
                }
                Err(error) => {
                    if let Some(hook) = options.on_claim_failure {
                        hook();
                    }
                    broker.record_failure(&error);
                    empty_streak += 1;
                }
            }
        }
    });
    *handle.shared.task.lock() = Some(task);
    handle
}

const BROKER_BACKOFF_MIN: Duration = Duration::from_secs(1);
const BROKER_BACKOFF_MAX: Duration = Duration::from_secs(60);
const CLAIM_FAILURE_WARN_EVERY: Duration = Duration::from_secs(60);

/// Per-broker claim back-off and rate-limited failure logging.
struct BrokerHealth {
    url: String,
    retry_at: Instant,
    failures: u32,
    last_warn: Option<Instant>,
}

impl BrokerHealth {
    fn new(url: String) -> Self {
        Self {
            url,
            retry_at: Instant::now(),
            failures: 0,
            last_warn: None,
        }
    }

    fn record_success(&mut self) {
        if self.failures > 0 {
            tracing::info!(broker = %self.url, failures = self.failures, "fleet claims recovered");
        }
        self.failures = 0;
        self.retry_at = Instant::now();
    }

    fn record_failure(&mut self, error: &str) {
        self.failures = self.failures.saturating_add(1);
        let exponent = self.failures.saturating_sub(1).min(5);
        let backoff = (BROKER_BACKOFF_MIN * 2u32.pow(exponent)).min(BROKER_BACKOFF_MAX);
        let now = Instant::now();
        self.retry_at = now + backoff;
        let warn = self
            .last_warn
            .is_none_or(|at| now.duration_since(at) >= CLAIM_FAILURE_WARN_EVERY);
        if warn {
            self.last_warn = Some(now);
            tracing::warn!(
                broker = %self.url,
                %error,
                failures = self.failures,
                backoff_ms = backoff.as_millis() as u64,
                "fleet claim failed; skipping broker until back-off expires"
            );
        } else {
            tracing::debug!(broker = %self.url, %error, failures = self.failures, "fleet claim failed");
        }
    }
}

/// Claim up to `permits.len()` jobs and start one delivery task per job.
/// Returns how many jobs were claimed; unused permits are released.
async fn claim_and_deliver(
    delivery: &FleetDelivery,
    client: &LeaseClient,
    broker: &str,
    mut permits: Vec<OwnedSemaphorePermit>,
    options: &FleetOptions,
) -> Result<usize, String> {
    let claimed = client.claim(broker, permits.len()).await?;
    let count = claimed.jobs.len();
    for job in claimed.jobs {
        let Some(permit) = permits.pop() else {
            // The broker returned more jobs than requested; let the extra
            // leases expire rather than deliver them without a permit.
            tracing::warn!(%broker, lease_id = %job.lease_id, "fleet claim exceeded max_jobs");
            continue;
        };
        let delivery = delivery.clone();
        let client = client.clone();
        let broker = broker.to_string();
        let heartbeat_interval = options.heartbeat_interval;
        tokio::spawn(async move {
            let _permit = permit;
            deliver_claimed_job(&delivery, &client, &broker, job, heartbeat_interval).await;
        });
    }
    Ok(count)
}

/// Deliver one claimed job and report the outcome to the broker that leased it.
pub async fn deliver_claimed_job(
    delivery: &FleetDelivery,
    client: &LeaseClient,
    broker: &str,
    job: ClaimedJob,
    heartbeat_interval: Duration,
) {
    let Some(msg) = job.message.clone() else {
        let _ = client
            .fail(broker, &job, "missing message envelope", true, 0, false)
            .await;
        return;
    };
    let hb = client.clone();
    let broker_hb = broker.to_string();
    let lease_id = job.lease_id;
    let heartbeat = tokio::spawn(async move {
        let mut tick = tokio::time::interval(heartbeat_interval);
        loop {
            tick.tick().await;
            if hb.heartbeat(&broker_hb, lease_id).await.is_err() {
                break;
            }
        }
    });

    let result = delivery.push(&msg, job.committed_hwm, job.attempts).await;
    heartbeat.abort();
    match result {
        Ok(()) => {
            if let Err(e) = client.complete(broker, &job).await {
                tracing::warn!(error = %e, "fleet complete failed");
            }
        }
        Err(e) => {
            let dead = matches!(
                &e,
                DispatchError::NoDestination
                    | DispatchError::Egress(_)
                    | DispatchError::NonRetryable(_)
                    | DispatchError::RetryExhausted(_)
            );
            // Only a real webhook attempt spends the broker-side retry budget.
            let counts_attempt = matches!(&e, DispatchError::RetryDeferred { .. });
            let retry_after_ms = match &e {
                DispatchError::RetryDeferred { retry_after_ms, .. } => *retry_after_ms,
                DispatchError::HostBlocked => 30_000,
                DispatchError::Uncommitted => 250,
                DispatchError::PayloadUnresolved => PAYLOAD_RETRY_MS,
                _ => 1_000,
            };
            let _ = client
                .fail(
                    broker,
                    &job,
                    &e.to_string(),
                    dead,
                    retry_after_ms,
                    counts_attempt,
                )
                .await;
        }
    }
}
