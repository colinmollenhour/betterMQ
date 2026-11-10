//! Cell admission: reject quickly with 429/503 rather than unbounded tails.

use crate::routes::ApiError;
use crate::AppState;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use uuid::Uuid;

fn is_body_ingest(request: &axum::extract::Request) -> bool {
    request.method() == axum::http::Method::POST
        && (matches!(
            request.uri().path(),
            "/v1/enqueue"
                | "/v1/publish"
                | "/v1/enqueue/batch"
                | "/v1/ingest/batch"
                | "/v1/ingest/ndjson"
                | "/v1/gateway/enqueue"
        ) || (request.uri().path().starts_with("/v1/queues/")
            && request.uri().path().ends_with("/enqueue")))
}

pub(crate) async fn buffer_ingest_body(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<AppState>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    buffer_body(Some(&state), request, next, crate::http_body_limit_bytes()).await
}

pub(crate) async fn buffer_gateway_body(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    buffer_body(None, request, next, crate::ingest::MAX_BATCH_BYTES).await
}

async fn buffer_body(
    state: Option<&AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
    limit: usize,
) -> axum::response::Response {
    use axum::{
        http::{header::CONTENT_LENGTH, StatusCode},
        response::IntoResponse,
    };
    if !is_body_ingest(&request) {
        return next.run(request).await;
    }
    let declared = match request.headers().get(CONTENT_LENGTH) {
        Some(value) => match value
            .to_str()
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
        {
            Some(value) if value <= limit => value,
            _ => {
                return (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "invalid or oversized Content-Length",
                )
                    .into_response()
            }
        },
        None => limit,
    };
    let tenant = request
        .extensions()
        .get::<crate::metering::IngestAuth>()
        .map(|auth| auth.tenant_id);
    let permit = match state {
        Some(state) => check_admission(state, tenant, declared),
        None => check_gateway_admission(tenant, declared),
    };
    let _permit = match permit {
        Ok(permit) => permit,
        Err(error) => {
            crate::metrics::record_rejected();
            return error.into_response();
        }
    };
    let (parts, body) = request.into_parts();
    let timeout_ms = env_limit("BETTERMQ_INGEST_BODY_TIMEOUT_MS", 30_000).min(120_000);
    let bytes = match read_reserved_body(body, declared, timeout_ms).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                "body exceeds reserved byte budget or could not be read",
            )
                .into_response()
        }
        Err(_) => {
            return (StatusCode::REQUEST_TIMEOUT, "ingest body read timed out").into_response()
        }
    };
    // Keep the transport reservation through parsing and commit. Canonical
    // admission additionally accounts for decoded records and fan-out expansion.
    next.run(axum::extract::Request::from_parts(
        parts,
        axum::body::Body::from(bytes),
    ))
    .await
}

async fn read_reserved_body(
    body: axum::body::Body,
    limit: usize,
    timeout_ms: u64,
) -> Result<Result<axum::body::Bytes, axum::Error>, tokio::time::error::Elapsed> {
    tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        axum::body::to_bytes(body, limit),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        extract::Request,
        http::StatusCode,
        middleware::{from_fn, Next},
        routing::post,
        Router,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn body_budget_is_checked_before_reading_and_enforced_for_chunks() {
        let app = Router::new()
            .route("/v1/enqueue", post(|| async { StatusCode::ACCEPTED }))
            .layer(from_fn(|request: Request, next: Next| async move {
                buffer_body(None, request, next, 8).await
            }));
        let unread = Body::from_stream(futures_util::stream::poll_fn(
            |_| -> std::task::Poll<Option<Result<axum::body::Bytes, std::io::Error>>> {
                panic!("oversized body must not be read")
            },
        ));
        let response = app
            .clone()
            .oneshot(
                Request::post("/v1/enqueue")
                    .header("content-length", "9")
                    .body(unread)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let response = app
            .oneshot(
                Request::post("/v1/enqueue")
                    .body(Body::from("123456789"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn stalled_upload_has_a_deadline() {
        let body = Body::from_stream(futures_util::stream::pending::<
            Result<axum::body::Bytes, std::io::Error>,
        >());
        assert!(read_reserved_body(body, 8, 1).await.is_err());
    }

    #[test]
    fn enormous_length_is_rejected_without_overflowing_admission() {
        assert!(reserve_admission(Some(Uuid::new_v4()), usize::MAX).is_err());
    }
}

fn env_limit(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
        .max(1)
}

fn max_in_flight() -> u64 {
    env_limit("BETTERMQ_INGEST_MAX_IN_FLIGHT", 4096)
}

fn max_queued_bytes() -> u64 {
    env_limit("BETTERMQ_INGEST_MAX_QUEUED_BYTES", 64 * 1024 * 1024)
}

fn max_tenant_records() -> u64 {
    env_limit("BETTERMQ_INGEST_MAX_TENANT_RECORDS", max_in_flight())
}

fn max_tenant_bytes() -> u64 {
    env_limit("BETTERMQ_INGEST_MAX_TENANT_BYTES", max_queued_bytes())
}

#[derive(Default)]
struct TenantQueued {
    records: u64,
    bytes: u64,
}

static ACCEPTING: AtomicBool = AtomicBool::new(true);
static QUEUED_RECORDS: AtomicU64 = AtomicU64::new(0);
static QUEUED_BYTES: AtomicU64 = AtomicU64::new(0);
static TENANTS: OnceLock<Mutex<HashMap<Uuid, TenantQueued>>> = OnceLock::new();

pub struct InFlightGuard {
    tenant_id: Uuid,
    bytes: u64,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        QUEUED_RECORDS.fetch_sub(1, Ordering::Relaxed);
        QUEUED_BYTES.fetch_sub(self.bytes, Ordering::Relaxed);
        let mut tenants = TENANTS.get_or_init(Default::default).lock();
        if let Some(queued) = tenants.get_mut(&self.tenant_id) {
            queued.records = queued.records.saturating_sub(1);
            queued.bytes = queued.bytes.saturating_sub(self.bytes);
            if queued.records == 0 {
                tenants.remove(&self.tenant_id);
            }
        }
    }
}

/// Stop accepting new ingest before graceful HTTP drain begins.
pub fn begin_shutdown() {
    ACCEPTING.store(false, Ordering::Release);
}

pub(crate) fn is_shutting_down() -> bool {
    !ACCEPTING.load(Ordering::Acquire)
}

pub fn check_admission(
    state: &AppState,
    tenant_id: Option<Uuid>,
    body_bytes: usize,
) -> Result<InFlightGuard, ApiError> {
    if !ACCEPTING.load(Ordering::Acquire) {
        return Err(ApiError::Overloaded {
            retry_after_ms: 1_000,
            message: "broker is draining".into(),
        });
    }
    if broker_storage::archive_admission_blocked() {
        let lag = broker_storage::archive_lag_status();
        return Err(ApiError::Overloaded {
            retry_after_ms: 1_000,
            message: format!(
                "archive lag saturated: {} queued / {} failed segments, {} queued bytes (oldest {} ms)",
                lag.queued_records, lag.failed_records, lag.queued_bytes, lag.oldest_age_ms
            ),
        });
    }
    if state.cluster.as_ref().is_some_and(|c| !c.catalog_ready()) {
        return Err(ApiError::Unavailable(
            "catalog cache is dirty or behind the committed revision".into(),
        ));
    }
    if state.dispatch.memory_guard().is_critical() {
        return Err(ApiError::Overloaded {
            retry_after_ms: 1_000,
            message: "memory critical; retry shortly".into(),
        });
    }
    reserve_admission(tenant_id, body_bytes)
}

pub(crate) fn check_gateway_admission(
    tenant_id: Option<Uuid>,
    body_bytes: usize,
) -> Result<InFlightGuard, ApiError> {
    if !ACCEPTING.load(Ordering::Acquire) {
        return Err(ApiError::Overloaded {
            retry_after_ms: 1_000,
            message: "gateway is draining".into(),
        });
    }
    reserve_admission(tenant_id, body_bytes)
}

fn reserve_admission(
    tenant_id: Option<Uuid>,
    body_bytes: usize,
) -> Result<InFlightGuard, ApiError> {
    let bytes = body_bytes as u64;
    let records = QUEUED_RECORDS.fetch_add(1, Ordering::AcqRel) + 1;
    let byte_limit = max_queued_bytes();
    if records > max_in_flight() {
        QUEUED_RECORDS.fetch_sub(1, Ordering::Relaxed);
        return Err(ApiError::Overloaded {
            retry_after_ms: 1_000,
            message: "broker ingest queue saturated".into(),
        });
    }
    if QUEUED_BYTES
        .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |queued| {
            queued
                .checked_add(bytes)
                .filter(|total| *total <= byte_limit)
        })
        .is_err()
    {
        QUEUED_RECORDS.fetch_sub(1, Ordering::Relaxed);
        return Err(ApiError::Overloaded {
            retry_after_ms: 1_000,
            message: "broker ingest byte budget saturated".into(),
        });
    }

    // Cloud callers get their authenticated tenant. Self-host uses one local
    // tenant, so its tenant limits default to the process limits.
    let tenant_id = tenant_id.unwrap_or_else(Uuid::nil);
    let mut tenants = TENANTS.get_or_init(Default::default).lock();
    let tenant = tenants.entry(tenant_id).or_default();
    if tenant.records + 1 > max_tenant_records()
        || tenant
            .bytes
            .checked_add(bytes)
            .is_none_or(|total| total > max_tenant_bytes())
    {
        if tenant.records == 0 {
            tenants.remove(&tenant_id);
        }
        drop(tenants);
        QUEUED_RECORDS.fetch_sub(1, Ordering::Relaxed);
        QUEUED_BYTES.fetch_sub(bytes, Ordering::Relaxed);
        return Err(ApiError::TooManyRequests {
            retry_after_ms: 1_000,
            message: "tenant ingest queue saturated".into(),
        });
    }
    tenant.records += 1;
    tenant.bytes += bytes;
    drop(tenants);

    Ok(InFlightGuard { tenant_id, bytes })
}
