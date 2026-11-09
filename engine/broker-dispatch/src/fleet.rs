//! Dispatch fleet (`--dispatch-fleet`): deliver jobs claimed from broker lease APIs.

use std::sync::Arc;
use std::time::Duration;

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
        let mut rr = 0usize;
        let mut empty_streak = 0usize;
        loop {
            let urls = client.broker_urls();
            if urls.is_empty() || dispatch.is_draining() {
                tokio::time::sleep(options.idle_poll).await;
                continue;
            }
            if empty_streak >= urls.len() {
                empty_streak = 0;
                tokio::time::sleep(options.idle_poll).await;
            }
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
            rr = (rr + 1) % urls.len();
            let broker = urls[rr].clone();
            let claimed = claim_and_deliver(&dispatch, &client, &broker, permits, &options).await;
            if claimed == 0 {
                empty_streak += 1;
            } else {
                empty_streak = 0;
            }
        }
    })
}

/// Claim up to `permits.len()` jobs and start one delivery task per job.
/// Returns how many jobs were claimed; unused permits are released.
async fn claim_and_deliver(
    dispatch: &DispatchEngine,
    client: &LeaseClient,
    broker: &str,
    mut permits: Vec<OwnedSemaphorePermit>,
    options: &FleetOptions,
) -> usize {
    let claimed = match client.claim(broker, permits.len()).await {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(error = %e, %broker, "fleet claim");
            return 0;
        }
    };
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
    count
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
            .fail(broker, &job, "missing message envelope", true, 0)
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

    let result = dispatch.push_http_only(&msg, job.committed_hwm).await;
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
            let retry_after_ms = match &e {
                DispatchError::RetryDeferred { retry_after_ms, .. } => *retry_after_ms,
                DispatchError::HostBlocked => 30_000,
                DispatchError::Uncommitted => 250,
                DispatchError::PayloadUnresolved => PAYLOAD_RETRY_MS,
                _ => 1_000,
            };
            let _ = client
                .fail(broker, &job, &e.to_string(), dead, retry_after_ms)
                .await;
        }
    }
}
