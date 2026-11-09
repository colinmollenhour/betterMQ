//! Retry policy for scheduled publishes (delayed jobs and cron firings) that fail.

use broker_partition::{BrokerError, FlowProfileError};

/// First retry delay after a failed scheduled publish.
pub const FIRE_RETRY_BASE_MS: i64 = 1_000;
/// Upper bound on the delay between retries of one scheduled publish.
pub const FIRE_RETRY_MAX_MS: i64 = 60_000;
/// Attempts before a permanent failure is abandoned. A few retries ride out
/// catalog propagation lag (a queue or flow created on another node moments ago).
pub const PERMANENT_FAILURE_ATTEMPTS: u32 = 5;

/// Why a scheduled publish failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FireFailure {
    /// May succeed later: no quorum, shard leader unreachable, storage error.
    Transient,
    /// Cannot succeed as written: the flow or queue is gone, or the request is invalid.
    Permanent,
}

impl FireFailure {
    pub fn from_broker_error(error: &BrokerError) -> Self {
        match error {
            BrokerError::QueueNotFound(_)
            | BrokerError::FlowProfileNotFound(_)
            | BrokerError::FlowProfile(FlowProfileError::NotFound(_))
            | BrokerError::InvalidName(_)
            | BrokerError::InvalidConfig(_) => Self::Permanent,
            _ => Self::Transient,
        }
    }
}

/// What happens to a scheduled publish after a failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FireRetry {
    /// Retry no earlier than `retry_at_ms`. `attempt` counts failures so far.
    Retry { attempt: u32, retry_at_ms: i64 },
    /// Stop retrying after `attempt` failures: a delayed job is dropped, a cron
    /// skips this occurrence and keeps its schedule.
    GiveUp { attempt: u32 },
}

/// Exponential backoff: 1 s, 2 s, 4 s, ... capped at [`FIRE_RETRY_MAX_MS`].
pub fn fire_backoff_ms(attempt: u32) -> i64 {
    let exponent = attempt.saturating_sub(1).min(32);
    FIRE_RETRY_BASE_MS
        .saturating_mul(1_i64 << exponent)
        .min(FIRE_RETRY_MAX_MS)
}

pub(crate) fn decide(attempt: u32, failure: FireFailure, now_ms: i64) -> FireRetry {
    if failure == FireFailure::Permanent && attempt >= PERMANENT_FAILURE_ATTEMPTS {
        return FireRetry::GiveUp { attempt };
    }
    FireRetry::Retry {
        attempt,
        retry_at_ms: now_ms.saturating_add(fire_backoff_ms(attempt)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        assert_eq!(fire_backoff_ms(1), 1_000);
        assert_eq!(fire_backoff_ms(2), 2_000);
        assert_eq!(fire_backoff_ms(3), 4_000);
        assert_eq!(fire_backoff_ms(7), 60_000);
        assert_eq!(fire_backoff_ms(u32::MAX), 60_000);
    }

    #[test]
    fn transient_failures_retry_forever() {
        assert_eq!(
            decide(1_000, FireFailure::Transient, 10),
            FireRetry::Retry {
                attempt: 1_000,
                retry_at_ms: 60_010
            }
        );
    }

    #[test]
    fn permanent_failures_give_up_after_cap() {
        assert!(matches!(
            decide(PERMANENT_FAILURE_ATTEMPTS - 1, FireFailure::Permanent, 0),
            FireRetry::Retry { .. }
        ));
        assert_eq!(
            decide(PERMANENT_FAILURE_ATTEMPTS, FireFailure::Permanent, 0),
            FireRetry::GiveUp {
                attempt: PERMANENT_FAILURE_ATTEMPTS
            }
        );
    }

    #[test]
    fn classifies_missing_catalog_entries_as_permanent() {
        let flow = uuid::Uuid::new_v4();
        assert_eq!(
            FireFailure::from_broker_error(&BrokerError::FlowProfileNotFound(flow)),
            FireFailure::Permanent
        );
        assert_eq!(
            FireFailure::from_broker_error(&BrokerError::QueueNotFound("q".into())),
            FireFailure::Permanent
        );
        assert_eq!(
            FireFailure::from_broker_error(&BrokerError::NotShardLeader(0)),
            FireFailure::Transient
        );
    }
}
