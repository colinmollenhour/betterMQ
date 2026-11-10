//! Append-only audit trail of `/admin/v1` mutations, served by
//! `GET /admin/v1/audit`.
//!
//! Every non-read admin request is recorded after it completes (including
//! rejected and unauthenticated attempts) with the caller, the path, the
//! request parameters (secrets redacted) and the response status. Records go
//! to `admin-audit.jsonl` next to the cell registry (rotated to
//! `admin-audit.jsonl.1` past 8 MiB) and the newest are kept in memory.

use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::io::{BufRead, Write};
use std::path::PathBuf;

const KEEP_IN_MEMORY: usize = 1_000;
const ROTATE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_AUDITED_BODY: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminAuditEvent {
    pub at_ms: i64,
    /// `bearer:<sha256 prefix>` of the presented token, or `anonymous`.
    pub caller: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forwarded_for: Option<String>,
    pub method: String,
    pub path: String,
    pub params: serde_json::Value,
    pub status: u16,
}

pub struct AdminAuditLog {
    path: Option<PathBuf>,
    recent: Mutex<VecDeque<AdminAuditEvent>>,
}

impl AdminAuditLog {
    /// Open (or create) the log at `path`; `None` keeps events in memory only.
    pub fn open(path: Option<PathBuf>) -> Self {
        let mut recent = VecDeque::new();
        if let Some(file) = path
            .as_ref()
            .and_then(|path| std::fs::File::open(path).ok())
        {
            for line in std::io::BufReader::new(file)
                .split(b'\n')
                .map_while(Result::ok)
            {
                if let Ok(event) = serde_json::from_slice::<AdminAuditEvent>(&line) {
                    recent.push_back(event);
                    if recent.len() > KEEP_IN_MEMORY {
                        recent.pop_front();
                    }
                }
            }
        }
        Self {
            path,
            recent: Mutex::new(recent),
        }
    }

    pub fn record(&self, event: AdminAuditEvent) {
        tracing::info!(
            target: "bettermq::admin_audit",
            caller = %event.caller,
            method = %event.method,
            path = %event.path,
            status = event.status,
            params = %event.params,
            "admin mutation"
        );
        let mut recent = self.recent.lock();
        if let Some(path) = &self.path {
            if let Err(error) = append_line(path, &event) {
                tracing::error!(%error, path = %path.display(), "admin audit write failed");
            }
        }
        recent.push_back(event);
        if recent.len() > KEEP_IN_MEMORY {
            recent.pop_front();
        }
    }

    /// Newest first.
    pub fn recent(&self, limit: usize) -> Vec<AdminAuditEvent> {
        self.recent
            .lock()
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }
}

fn append_line(path: &PathBuf, event: &AdminAuditEvent) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if std::fs::metadata(path).is_ok_and(|metadata| metadata.len() >= ROTATE_BYTES) {
        std::fs::rename(path, PathBuf::from(format!("{}.1", path.display())))?;
    }
    let mut line = serde_json::to_vec(event).map_err(std::io::Error::other)?;
    line.push(b'\n');
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(&line)?;
    file.sync_data()?;
    broker_storage::set_secret_file_mode(path);
    Ok(())
}

fn caller_identity(headers: &HeaderMap) -> String {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|token| !token.is_empty())
        .map(|token| {
            let digest = Sha256::digest(token.as_bytes());
            format!("bearer:{}", &hex::encode(digest)[..12])
        })
        .unwrap_or_else(|| "anonymous".into())
}

fn is_sensitive(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    ["secret", "password", "token", "credential"]
        .iter()
        .any(|needle| key.contains(needle))
}

fn redact(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => map
            .into_iter()
            .map(|(key, value)| {
                if is_sensitive(&key) {
                    (key, serde_json::Value::String("[redacted]".into()))
                } else {
                    (key, redact(value))
                }
            })
            .collect::<serde_json::Map<_, _>>()
            .into(),
        serde_json::Value::Array(items) => items.into_iter().map(redact).collect(),
        other => other,
    }
}

/// Middleware recording every non-read request on the admin router.
pub async fn audit_admin_mutations(
    State(audit): State<std::sync::Arc<AdminAuditLog>>,
    request: Request,
    next: Next,
) -> Response {
    if matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) || !request.uri().path().starts_with("/admin/")
    {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_AUDITED_BODY).await {
        Ok(bytes) => bytes,
        Err(_) => {
            let response = (
                StatusCode::PAYLOAD_TOO_LARGE,
                "admin request body too large",
            )
                .into_response();
            audit.record(AdminAuditEvent {
                at_ms: chrono::Utc::now().timestamp_millis(),
                caller: caller_identity(&parts.headers),
                forwarded_for: forwarded_for(&parts.headers),
                method: parts.method.to_string(),
                path: parts.uri.path().to_string(),
                params: serde_json::Value::Null,
                status: response.status().as_u16(),
            });
            return response;
        }
    };
    let params = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .map(redact)
            .unwrap_or_else(|_| serde_json::json!({ "unparsedBytes": bytes.len() }))
    };
    let caller = caller_identity(&parts.headers);
    let forwarded_for = forwarded_for(&parts.headers);
    let method = parts.method.to_string();
    let path = parts.uri.path().to_string();
    let response = next
        .run(Request::from_parts(parts, Body::from(bytes)))
        .await;
    audit.record(AdminAuditEvent {
        at_ms: chrono::Utc::now().timestamp_millis(),
        caller,
        forwarded_for,
        method,
        path,
        params,
        status: response.status().as_u16(),
    });
    response
}

fn forwarded_for(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.chars().take(256).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(path: &str) -> AdminAuditEvent {
        AdminAuditEvent {
            at_ms: 1,
            caller: "anonymous".into(),
            forwarded_for: None,
            method: "POST".into(),
            path: path.into(),
            params: serde_json::Value::Null,
            status: 200,
        }
    }

    #[test]
    fn events_persist_and_reload_newest_first() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("admin-audit.jsonl");
        let log = AdminAuditLog::open(Some(path.clone()));
        log.record(event("/admin/v1/a"));
        log.record(event("/admin/v1/b"));
        let reopened = AdminAuditLog::open(Some(path));
        let paths: Vec<_> = reopened
            .recent(10)
            .into_iter()
            .map(|event| event.path)
            .collect();
        assert_eq!(paths, vec!["/admin/v1/b", "/admin/v1/a"]);
        assert_eq!(reopened.recent(1).len(), 1);
    }

    #[test]
    fn secrets_are_redacted_recursively() {
        let value = redact(serde_json::json!({
            "url": "http://n1",
            "clusterSecret": "s3cr3t",
            "nested": [{ "api_token": "t", "watermark": 5 }],
        }));
        assert_eq!(value["url"], "http://n1");
        assert_eq!(value["clusterSecret"], "[redacted]");
        assert_eq!(value["nested"][0]["api_token"], "[redacted]");
        assert_eq!(value["nested"][0]["watermark"], 5);
    }

    #[test]
    fn caller_is_a_token_fingerprint_never_the_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer sk_local_abc".parse().unwrap(),
        );
        let caller = caller_identity(&headers);
        assert!(caller.starts_with("bearer:"));
        assert!(!caller.contains("sk_local_abc"));
        assert_eq!(caller_identity(&HeaderMap::new()), "anonymous");
    }
}
