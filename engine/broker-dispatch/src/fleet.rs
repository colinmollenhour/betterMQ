//! Dispatch fleet (`--dispatch-fleet`): deliver jobs claimed from broker lease APIs.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

use crate::lease::{ClaimedJob, LeaseClient, MAX_CLAIM_JOBS};
use crate::worker::{DispatchEngine, DispatchError};

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
        }
    }
}

/// Claim jobs from every broker in `client` and deliver them until aborted.
///
/// The loop keeps claiming while delivery permits are free and brokers return
/// work; it only waits `idle_poll` once a whole round of brokers came back
/// empty. Each claim asks for as many jobs as there are free permits (capped
/// by `claim_batch`), and every claimed job runs in its own task that
/// heartbeats its lease from the moment it is claimed.
pub fn spawn_fleet(
    dispatch: DispatchEngine,
    client: LeaseClient,
    options: FleetOptions,
) -> JoinHandle<()> {
    let sem = Arc::new(Semaphore::new(options.concurrency.max(1)));
    let claim_batch = options.claim_batch.clamp(1, MAX_CLAIM_JOBS);
    tokio::spawn(async move {
        let mut brokers: Vec<BrokerHealth> = client
            .broker_urls()
            .iter()
            .map(|url| BrokerHealth::new(url.clone()))
            .collect();
        let mut rr = 0usize;
        let mut empty_streak = 0usize;
        loop {
            if brokers.is_empty() || dispatch.is_draining() {
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
            match claim_and_deliver(&dispatch, &client, &broker.url, permits, &options).await {
                Ok(0) => {
                    broker.record_success();
                    empty_streak += 1;
                }
                Ok(_) => {
                    broker.record_success();
                    empty_streak = 0;
                }
                Err(error) => {
                    broker.record_failure(&error);
                    empty_streak += 1;
                }
            }
        }
    })
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
    dispatch: &DispatchEngine,
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
        let dispatch = dispatch.clone();
        let client = client.clone();
        let broker = broker.to_string();
        let heartbeat_interval = options.heartbeat_interval;
        tokio::spawn(async move {
            let _permit = permit;
            deliver_claimed_job(&dispatch, &client, &broker, job, heartbeat_interval).await;
        });
    }
    Ok(count)
}

/// Deliver one claimed job and report the outcome to the broker that leased it.
pub async fn deliver_claimed_job(
    dispatch: &DispatchEngine,
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

    let result = dispatch
        .push_http_only(&msg, job.committed_hwm, job.attempts)
        .await;
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
