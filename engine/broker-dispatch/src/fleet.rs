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

#[derive(Debug, Clone)]
pub struct FleetOptions {
    /// Jobs in flight per fleet process (`BETTERMQ_FLEET_CONCURRENCY`).
    pub concurrency: usize,
    /// How often each held lease is renewed while its job is in flight.
    pub heartbeat_interval: Duration,
}

impl FleetOptions {
    pub fn from_env() -> Self {
        Self {
            concurrency: crate::lease::fleet_concurrency(),
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
        }
    }
}

/// Claim jobs from every broker in `client` and deliver them until aborted.
///
/// Each claim asks for as many jobs as there are free delivery permits, and
/// every claimed job runs in its own task that heartbeats its lease from the
/// moment it is claimed, so no lease sits idle waiting behind another delivery.
pub fn spawn_fleet(
    dispatch: DispatchEngine,
    client: LeaseClient,
    options: FleetOptions,
) -> JoinHandle<()> {
    let sem = Arc::new(Semaphore::new(options.concurrency.max(1)));
    tokio::spawn(async move {
        let mut rr = 0usize;
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        loop {
            interval.tick().await;
            let urls = client.broker_urls();
            if urls.is_empty() {
                continue;
            }
            rr = (rr + 1) % urls.len();
            let broker = urls[rr].clone();
            let Ok(first) = sem.clone().acquire_owned().await else {
                break;
            };
            let mut permits = vec![first];
            while permits.len() < MAX_CLAIM_JOBS {
                match sem.clone().try_acquire_owned() {
                    Ok(permit) => permits.push(permit),
                    Err(_) => break,
                }
            }
            let dispatch = dispatch.clone();
            let client = client.clone();
            let options = options.clone();
            tokio::spawn(async move {
                if dispatch.is_draining() {
                    return;
                }
                claim_and_deliver(&dispatch, &client, &broker, permits, &options).await;
            });
        }
    })
}

async fn claim_and_deliver(
    dispatch: &DispatchEngine,
    client: &LeaseClient,
    broker: &str,
    mut permits: Vec<OwnedSemaphorePermit>,
    options: &FleetOptions,
) {
    let claimed = match client.claim(broker, permits.len()).await {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(error = %e, %broker, "fleet claim");
            return;
        }
    };
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
