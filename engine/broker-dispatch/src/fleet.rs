//! Dispatch fleet (`--dispatch-fleet`): deliver jobs claimed from broker lease APIs.

use std::time::Duration;

use crate::lease::{ClaimedJob, LeaseClient};
use crate::worker::{DispatchEngine, DispatchError};

/// Retry delay when a claim arrived without its body (matches the local path).
const PAYLOAD_RETRY_MS: u64 = 10_000;

/// Deliver one claimed job and report the outcome to the broker that leased it.
pub async fn deliver_claimed_job(
    dispatch: &DispatchEngine,
    client: &LeaseClient,
    broker: &str,
    job: ClaimedJob,
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
        let mut tick = tokio::time::interval(Duration::from_secs(10));
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
