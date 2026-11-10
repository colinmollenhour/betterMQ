use crate::AppState;
use axum::{
    extract::{Extension, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use broker_partition::{
    BrokerError, CreateSubscriptionRequest, CreateSubscriptionResponse, DestinationSnapshot,
    FlowProfileError, PublishRequest, PublishResponse,
};
use std::sync::Arc;
use tracing::info;

pub(crate) async fn publish(
    State(state): State<Arc<AppState>>,
    ingest: Option<Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<Extension<broker_control_plane::PlanLimits>>,
    Json(req): Json<PublishRequest>,
) -> Result<(StatusCode, Json<PublishResponse>), ApiError> {
    #[cfg(feature = "cloud")]
    if state.uses_cloud_auth() {
        if let Some(plan) = plan.as_ref().map(|e| &e.0) {
            if req.payload.len() as u64 > plan.max_message_bytes {
                return crate::metrics::record_single_outcome(Err(ApiError::BadRequest(format!(
                    "message exceeds plan limit of {} bytes",
                    plan.max_message_bytes
                ))));
            }
        }
    }
    let meter = crate::metering::ingest_meter(ingest.map(|e| e.0), req.payload.len());
    let resp = crate::ingest::submit_one(&state, req, meter).await?;
    if resp.scheduled.is_none() {
        crate::ingest::notify_dispatch(&state, &resp);
    }
    if resp.duplicate {
        crate::metrics::record_duplicate();
    } else if resp.scheduled.is_none() {
        crate::metrics::record_accepted();
    }
    let status = crate::ingest::status_for(&resp);
    Ok((status, Json(resp)))
}

pub(crate) async fn create_subscription(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateSubscriptionRequest>,
) -> Result<(StatusCode, Json<CreateSubscriptionResponse>), ApiError> {
    validate_destination_url_str(&req.url)?;
    let resp = if state.cluster.is_some() {
        let mutation = crate::catalog_applier::begin(&state)?;
        let kind = broker_raft_meta::catalog::CatalogKind::Queue;
        let sub = state
            .broker
            .prepare_subscription(req, mutation.id(&state.broker.tenant(), kind))?;
        mutation
            .commit(&state, sub.tenant_id.clone(), kind, sub.id, &sub, false)
            .await?;
        CreateSubscriptionResponse {
            id: sub.id,
            topic: sub.topic,
            url: sub.url,
            parallelism: sub.parallelism,
            default_max_retries: sub.default_max_retries,
        }
    } else {
        state.broker.create_subscription(req)?
    };
    info!(queue_id = %resp.id, queue = %resp.topic, url = %resp.url, "queue ready");
    Ok((StatusCode::CREATED, Json(resp)))
}

fn validate_destination_url_str(url: &str) -> Result<(), ApiError> {
    broker_dispatch::validate_destination_url(url).map_err(|e| ApiError::BadRequest(e.to_string()))
}

/// Public for catalog apply / other ingest paths.
pub fn validate_destination_url_str_pub(url: &str) -> Result<(), ApiError> {
    validate_destination_url_str(url)
}

fn validate_publish_destinations(req: &PublishRequest) -> Result<(), ApiError> {
    if let Some(url) = req.url.as_deref().filter(|u| !u.trim().is_empty()) {
        validate_destination_url_str(url)?;
    }
    if let Some(dest) = req.destination.as_ref() {
        validate_destination_url_str(&dest.url)?;
    }
    Ok(())
}

/// Public for batch / gateway ingest paths.
pub fn validate_publish_destinations_pub(req: &PublishRequest) -> Result<(), ApiError> {
    validate_publish_destinations(req)
}

/// Frozen destination for delayed enqueue or publish (queue, inline URL, or snapshot).
pub(crate) async fn snapshot_destination(
    state: &Arc<AppState>,
    req: &PublishRequest,
) -> Result<DestinationSnapshot, ApiError> {
    if let Some(dest) = req.destination.as_ref() {
        return Ok(dest.clone());
    }
    if let (Some(url), Some(secret)) = (req.url.as_ref(), req.secret.as_ref()) {
        return Ok(DestinationSnapshot {
            queue_id: req.queue_id,
            url: url.clone(),
            secret: secret.clone(),
        });
    }
    resolve_destination_with_repair(state, req.queue_id, &req.topic).await
}

pub(crate) async fn resolve_destination_with_repair(
    state: &Arc<AppState>,
    queue_id: Option<uuid::Uuid>,
    queue_name: &str,
) -> Result<DestinationSnapshot, ApiError> {
    match resolve_destination(&state.broker, queue_id, queue_name) {
        Ok(d) => Ok(d),
        Err(ApiError::Broker(BrokerError::QueueNotFound(_))) if state.cluster.is_some() => {
            crate::cluster::sync_catalog_from_peers(state).await;
            resolve_destination(&state.broker, queue_id, queue_name)
        }
        Err(e) => Err(e),
    }
}

pub(crate) fn resolve_destination(
    broker: &broker_partition::Broker,
    queue_id: Option<uuid::Uuid>,
    queue_name: &str,
) -> Result<DestinationSnapshot, ApiError> {
    let q = if let Some(id) = queue_id {
        broker
            .get_queue_by_id(id)?
            .ok_or_else(|| ApiError::Broker(BrokerError::QueueNotFound(id.to_string())))?
    } else {
        broker
            .get_queue(queue_name)?
            .ok_or_else(|| ApiError::Broker(BrokerError::QueueNotFound(queue_name.to_string())))?
    };
    Ok(DestinationSnapshot {
        queue_id: Some(q.id),
        url: q.url,
        secret: q.secret,
    })
}

#[derive(Debug)]
pub enum ApiError {
    Broker(BrokerError),
    BadRequest(String),
    Unauthorized(String),
    NotFound(String),
    Conflict(String),
    Unavailable(String),
    ReplicationFailed(String),
    /// Preserve a leader's rejection, including back-pressure headers.
    Upstream {
        status: StatusCode,
        retry_after: Option<axum::http::HeaderValue>,
        body: axum::body::Bytes,
    },
    Overloaded {
        retry_after_ms: u64,
        message: String,
    },
    TooManyRequests {
        retry_after_ms: u64,
        message: String,
    },
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Broker(error) => write!(formatter, "{error}"),
            Self::BadRequest(message)
            | Self::Unauthorized(message)
            | Self::NotFound(message)
            | Self::Conflict(message)
            | Self::Unavailable(message)
            | Self::ReplicationFailed(message) => formatter.write_str(message),
            Self::Overloaded { message, .. } | Self::TooManyRequests { message, .. } => {
                formatter.write_str(message)
            }
            Self::Upstream { status, body, .. } => {
                match serde_json::from_slice::<serde_json::Value>(body)
                    .ok()
                    .and_then(|value| value["error"].as_str().map(str::to_string))
                {
                    Some(message) => formatter.write_str(&message),
                    None => write!(formatter, "shard leader returned {status}"),
                }
            }
        }
    }
}

impl ApiError {
    /// Stable machine-readable classification for multi-status ingest results.
    pub(crate) fn failure(&self) -> crate::ingest::IngestFailure {
        let (code, retryable, retry_after_ms) = match self {
            Self::BadRequest(_) => ("invalid_request", false, None),
            Self::Unauthorized(_) => ("unauthorized", false, None),
            Self::NotFound(_) => ("not_found", false, None),
            Self::Conflict(_) => ("conflict", false, None),
            Self::Unavailable(_) => ("unavailable", true, None),
            Self::ReplicationFailed(_) => ("replication_failed", true, None),
            Self::Overloaded { retry_after_ms, .. } => ("saturated", true, Some(*retry_after_ms)),
            Self::TooManyRequests { retry_after_ms, .. } => {
                ("tenant_saturated", true, Some(*retry_after_ms))
            }
            Self::Upstream {
                status,
                retry_after,
                body,
            } => {
                let code = serde_json::from_slice::<serde_json::Value>(body)
                    .ok()
                    .and_then(|value| value["code"].as_str().map(str::to_string))
                    .unwrap_or_else(|| "upstream_error".into());
                return crate::ingest::IngestFailure {
                    code,
                    retryable: status.is_server_error() || *status == StatusCode::TOO_MANY_REQUESTS,
                    retry_after_ms: retry_after
                        .as_ref()
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.trim().parse::<u64>().ok())
                        .map(|seconds| seconds.saturating_mul(1_000)),
                };
            }
            Self::Broker(BrokerError::QueueNotFound(_) | BrokerError::FlowProfileNotFound(_)) => {
                ("not_found", false, None)
            }
            Self::Broker(BrokerError::InvalidName(_) | BrokerError::InvalidConfig(_)) => {
                ("invalid_request", false, None)
            }
            Self::Broker(BrokerError::NotShardLeader(_)) => ("not_shard_leader", true, None),
            Self::Broker(_) => ("internal_error", true, None),
        };
        crate::ingest::IngestFailure {
            code: code.into(),
            retryable,
            retry_after_ms,
        }
    }
}

impl From<BrokerError> for ApiError {
    fn from(e: BrokerError) -> Self {
        match e {
            BrokerError::FlowProfile(FlowProfileError::Duplicate(id)) => {
                Self::Conflict(format!("duplicate flow {id}"))
            }
            other => Self::Broker(other),
        }
    }
}

impl From<broker_schedule::ScheduleError> for ApiError {
    fn from(e: broker_schedule::ScheduleError) -> Self {
        Self::Broker(BrokerError::Storage(broker_storage::LogError::Io(
            std::io::Error::other(e.to_string()),
        )))
    }
}

pub(crate) fn retry_after_seconds(retry_after_ms: u64) -> u64 {
    retry_after_ms
        .saturating_add(999)
        .saturating_div(1_000)
        .max(1)
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        match self {
            ApiError::Upstream {
                status,
                retry_after,
                body,
            } => {
                let mut response =
                    (status, [("content-type", "application/json")], body).into_response();
                if let Some(value) = retry_after {
                    response.headers_mut().insert("retry-after", value);
                }
                response
            }
            ApiError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            ApiError::Unauthorized(msg) => (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            ApiError::NotFound(msg) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            ApiError::Conflict(msg) => (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            ApiError::Unavailable(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": msg, "code": "not_shard_leader" })),
            )
                .into_response(),
            ApiError::ReplicationFailed(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            ApiError::Overloaded {
                retry_after_ms,
                message,
            } => {
                let mut resp = (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({ "error": message, "code": "saturated" })),
                )
                    .into_response();
                if let Ok(v) = axum::http::HeaderValue::from_str(
                    &retry_after_seconds(retry_after_ms).to_string(),
                ) {
                    resp.headers_mut().insert("retry-after", v);
                }
                resp
            }
            ApiError::TooManyRequests {
                retry_after_ms,
                message,
            } => {
                let mut resp = (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(serde_json::json!({ "error": message, "code": "quota" })),
                )
                    .into_response();
                if let Ok(v) = axum::http::HeaderValue::from_str(
                    &retry_after_seconds(retry_after_ms).to_string(),
                ) {
                    resp.headers_mut().insert("retry-after", v);
                }
                resp
            }
            ApiError::Broker(BrokerError::Payload(
                broker_partition::PayloadError::TooLargeForCell { size, limit },
            )) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(serde_json::json!({
                    "error": "payload_too_large_for_cell",
                    "code": "payload_too_large_for_cell",
                    "message": format!(
                        "message body is {size} bytes; this replicated cell stores bodies inline and accepts at most {limit} bytes"
                    ),
                    "limit": limit,
                    "size": size,
                })),
            )
                .into_response(),
            ApiError::Broker(BrokerError::IdempotencyInFlight(key)) => {
                let mut resp = (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({
                        "error": format!(
                            "idempotency key {key} is still being published by another request; retry shortly"
                        ),
                        "code": "idempotency_in_flight",
                    })),
                )
                    .into_response();
                resp.headers_mut()
                    .insert("retry-after", axum::http::HeaderValue::from_static("1"));
                resp
            }
            ApiError::Broker(e) => {
                let (status, code, msg) = match &e {
                    BrokerError::Index(_) => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "metadata_unavailable",
                        "metadata unavailable; retry after storage recovers".to_string(),
                    ),
                    BrokerError::QueueNotFound(_) | BrokerError::FlowProfileNotFound(_) => {
                        (StatusCode::NOT_FOUND, "not_found", e.to_string())
                    }
                    BrokerError::NotShardLeader(_) => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "not_shard_leader",
                        "not shard leader".to_string(),
                    ),
                    BrokerError::InvalidName(_) | BrokerError::InvalidConfig(_) => {
                        (StatusCode::BAD_REQUEST, "invalid_request", e.to_string())
                    }
                    other => {
                        tracing::error!(error = %other, "broker error");
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "internal_error",
                            "internal error".to_string(),
                        )
                    }
                };
                let body = serde_json::json!({ "error": msg, "code": code });
                (status, Json(body)).into_response()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header::RETRY_AFTER;

    #[test]
    fn per_item_retry_classification_is_stable() {
        let bad = ApiError::BadRequest("bad body".into());
        assert!(!bad.failure().retryable);
        assert_eq!(bad.failure().code, "invalid_request");
        let busy = ApiError::Overloaded {
            retry_after_ms: 1500,
            message: "busy".into(),
        };
        let value = serde_json::to_value(busy.failure()).unwrap();
        assert_eq!(value["code"], "saturated");
        assert_eq!(value["retryable"], true);
        assert_eq!(value["retry_after_ms"], 1500);
        assert!(
            ApiError::ReplicationFailed("quorum".into())
                .failure()
                .retryable
        );
        assert!(ApiError::Unavailable("fsync".into()).failure().retryable);
    }

    #[test]
    fn upstream_failure_keeps_leader_code_and_retry_after() {
        let saturated = ApiError::Upstream {
            status: StatusCode::SERVICE_UNAVAILABLE,
            retry_after: Some(axum::http::HeaderValue::from_static("7")),
            body: axum::body::Bytes::from_static(br#"{"error":"busy","code":"saturated"}"#),
        }
        .failure();
        assert_eq!(saturated.code, "saturated");
        assert!(saturated.retryable);
        assert_eq!(saturated.retry_after_ms, Some(7_000));
        let rejected = ApiError::Upstream {
            status: StatusCode::BAD_REQUEST,
            retry_after: None,
            body: axum::body::Bytes::from_static(b"not json"),
        }
        .failure();
        assert_eq!(rejected.code, "upstream_error");
        assert!(!rejected.retryable);
        assert_eq!(rejected.retry_after_ms, None);
    }

    #[test]
    fn metadata_failure_returns_retryable_503() {
        let response = ApiError::from(BrokerError::Index(broker_storage::IndexError::Io(
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "metadata lock deadline exceeded",
            ),
        )))
        .into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn oversize_cell_body_is_413_with_the_limit() {
        let response = ApiError::Broker(BrokerError::Payload(
            broker_partition::PayloadError::TooLargeForCell {
                size: 20,
                limit: 10,
            },
        ))
        .into_response();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn in_flight_idempotency_key_is_retryable_conflict() {
        let response =
            ApiError::Broker(BrokerError::IdempotencyInFlight("k".into())).into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(response.headers().get(RETRY_AFTER).unwrap(), "1");
    }

    #[test]
    fn retry_after_is_whole_seconds_not_milliseconds() {
        assert_eq!(retry_after_seconds(1), 1);
        assert_eq!(retry_after_seconds(1_000), 1);
        assert_eq!(retry_after_seconds(1_001), 2);

        let response = ApiError::Overloaded {
            retry_after_ms: 1_500,
            message: "busy".into(),
        }
        .into_response();
        assert_eq!(response.headers().get(RETRY_AFTER).unwrap(), "2");
    }

    #[test]
    fn tenant_saturation_is_429_and_system_saturation_is_503() {
        let tenant = ApiError::TooManyRequests {
            retry_after_ms: 1_000,
            message: "tenant".into(),
        }
        .into_response();
        let system = ApiError::Overloaded {
            retry_after_ms: 1_000,
            message: "system".into(),
        }
        .into_response();
        assert_eq!(tenant.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(system.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
