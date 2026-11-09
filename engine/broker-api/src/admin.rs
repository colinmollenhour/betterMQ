//! Versioned `/admin/v1` management API shared by embedded and standalone panel.

use crate::auth;
use crate::ops;
use crate::AppState;
use axum::{
    extract::{Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use broker_config::stable_node_id;
use broker_raft_meta::{replication_policy, ControllerCommand, DataNodeRecord};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const ADMIN_API_VERSION: &str = "v1";

#[derive(Clone)]
pub struct AdminState {
    pub local: Option<Arc<AppState>>,
    pub remote_controller: Option<String>,
    pub cell_registry: Arc<parking_lot::RwLock<CellRegistry>>,
    pub cluster_secret: Option<String>,
    pub registry_path: Option<PathBuf>,
}

impl AdminState {
    pub fn local(state: Arc<AppState>) -> Self {
        Self::local_with_registry(state, None, None)
    }

    pub fn local_with_registry(
        state: Arc<AppState>,
        registry_path: Option<PathBuf>,
        controller_url: Option<String>,
    ) -> Self {
        let (mut registry, registry_path) = match registry_path {
            Some(path) => {
                let (registry, persist) = CellRegistry::load_for_startup(&path);
                (registry, persist.then_some(path))
            }
            None => (CellRegistry::default(), None),
        };
        if let Some(url) = controller_url.as_deref() {
            ensure_local_cell(&mut registry, &state, url);
        }
        if let Some(path) = registry_path.as_ref() {
            if let Err(error) = registry.save(path) {
                tracing::warn!(%error, path = %path.display(), "cell registry save failed");
            }
        }
        Self {
            local: Some(state),
            remote_controller: None,
            cell_registry: Arc::new(parking_lot::RwLock::new(registry)),
            cluster_secret: crate::cluster_auth::cluster_secret(),
            registry_path,
        }
    }

    pub fn remote(controller: String, registry: CellRegistry) -> Self {
        Self::remote_with_path(Some(controller), registry, None)
    }

    /// Standalone panel. Direct `/admin/v1` reads and mutations are proxied to
    /// `controller` (registered as the default cell; its `adminToken` is the
    /// credential used). Without a controller they return 503.
    pub fn remote_with_path(
        controller: Option<String>,
        mut registry: CellRegistry,
        registry_path: Option<PathBuf>,
    ) -> Self {
        let controller = controller
            .map(|c| c.trim().trim_end_matches('/').to_string())
            .filter(|c| !c.is_empty());
        if let Some(url) = controller.as_deref() {
            ensure_controller_cell(&mut registry, url);
        }
        if let Some(path) = registry_path.as_ref() {
            if let Err(error) = registry.save(path) {
                tracing::warn!(%error, path = %path.display(), "cell registry save failed");
            }
        }
        Self {
            local: None,
            remote_controller: controller,
            cell_registry: Arc::new(parking_lot::RwLock::new(registry)),
            cluster_secret: crate::cluster_auth::cluster_secret(),
            registry_path,
        }
    }

    fn persist_registry(&self, registry: &CellRegistry) {
        if let Some(path) = &self.registry_path {
            if let Err(error) = registry.save(path) {
                tracing::warn!(%error, "cell registry save failed");
            }
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CellRegistry {
    pub cells: Vec<CellRecord>,
    #[serde(default)]
    pub members: Vec<CellMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CellRecord {
    pub id: String,
    pub region: String,
    pub controller_url: String,
    #[serde(default)]
    pub label: Option<String>,
    /// The cell's own `sk_local_` admin token, used for federated admin calls
    /// to `controller_url`. Persisted (secret file mode), never returned by the API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_token: Option<String>,
}

/// API view of a [`CellRecord`]: the admin token is replaced by a flag.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CellView {
    pub id: String,
    pub region: String,
    pub controller_url: String,
    pub label: Option<String>,
    pub admin_token_configured: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CellRegistryView {
    pub cells: Vec<CellView>,
    pub members: Vec<CellMember>,
}

impl From<&CellRegistry> for CellRegistryView {
    fn from(registry: &CellRegistry) -> Self {
        Self {
            cells: registry
                .cells
                .iter()
                .map(|c| CellView {
                    id: c.id.clone(),
                    region: c.region.clone(),
                    controller_url: c.controller_url.clone(),
                    label: c.label.clone(),
                    admin_token_configured: c.admin_token.is_some(),
                })
                .collect(),
            members: registry.members.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellMember {
    pub profile: String,
    pub reach_url: String,
    pub advertise_url: String,
    pub name: String,
    pub cell_id: String,
}

impl CellRegistry {
    /// Strict load: a missing file is an empty registry; an unreadable or
    /// unparsable file is an error.
    pub fn load(path: &std::path::Path) -> std::io::Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }

    /// Startup load that never destroys a damaged registry. A corrupt or
    /// unreadable file is moved aside to `<file>.corrupt-<unix ms>` and an
    /// empty registry is used. If it cannot be moved aside, the registry runs
    /// in memory only (`false`): nothing is written over the original.
    pub fn load_for_startup(path: &std::path::Path) -> (Self, bool) {
        let error = match Self::load(path) {
            Ok(registry) => return (registry, true),
            Err(error) => error,
        };
        let mut quarantine = path.as_os_str().to_os_string();
        quarantine.push(format!(
            ".corrupt-{}",
            chrono::Utc::now().timestamp_millis()
        ));
        let quarantine = PathBuf::from(quarantine);
        match std::fs::rename(path, &quarantine) {
            Ok(()) => {
                tracing::error!(
                    %error,
                    path = %path.display(),
                    saved_as = %quarantine.display(),
                    "cell registry unreadable; moved aside and starting with an empty registry"
                );
                (Self::default(), true)
            }
            Err(rename_error) => {
                tracing::error!(
                    %error,
                    %rename_error,
                    path = %path.display(),
                    "cell registry unreadable and could not be moved aside; registry changes will not be saved"
                );
                (Self::default(), false)
            }
        }
    }

    /// Durable replace: unique 0600 temp file → fsync → rename → dir fsync, so
    /// a crash or full disk never leaves a truncated registry (it holds
    /// per-cell admin tokens, hence the mode).
    pub fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        use std::io::Write;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."));
        std::fs::create_dir_all(parent)?;
        let bytes = serde_json::to_vec_pretty(self)?;
        let tmp = parent.join(format!(
            ".{}.{}.{}.tmp",
            path.file_name().unwrap_or_default().to_string_lossy(),
            std::process::id(),
            Uuid::new_v4().simple()
        ));
        let written = (|| {
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(&tmp, path)
        })();
        if written.is_err() {
            let _ = std::fs::remove_file(&tmp);
            return written;
        }
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
        Ok(())
    }

    pub fn upsert_member(&mut self, member: CellMember) {
        if let Some(existing) = self.members.iter_mut().find(|m| {
            m.cell_id == member.cell_id
                && (m.name == member.name
                    || m.reach_url == member.reach_url
                    || m.advertise_url == member.advertise_url)
        }) {
            *existing = member;
        } else {
            self.members.push(member);
        }
    }
}

fn same_url(a: &str, b: &str) -> bool {
    a.trim().trim_end_matches('/') == b.trim().trim_end_matches('/')
}

fn ensure_controller_cell(registry: &mut CellRegistry, controller_url: &str) {
    if registry
        .cells
        .iter()
        .any(|c| same_url(&c.controller_url, controller_url))
    {
        return;
    }
    let id = if registry.cells.iter().any(|c| c.id == "default") {
        format!("default-{}", registry.cells.len())
    } else {
        "default".into()
    };
    registry.cells.push(CellRecord {
        id,
        region: std::env::var("BETTERMQ_CELL_REGION").unwrap_or_else(|_| "default".into()),
        controller_url: controller_url.to_string(),
        label: Some(std::env::var("BETTERMQ_CELL_LABEL").unwrap_or_else(|_| "controller".into())),
        admin_token: None,
    });
}

fn ensure_local_cell(registry: &mut CellRegistry, local: &AppState, controller_url: &str) {
    let id = local
        .cluster
        .as_ref()
        .map(|c| c.runtime.config().cluster_id.to_string())
        .unwrap_or_else(|| "local".into());
    if registry
        .cells
        .iter()
        .any(|c| c.id == id || c.id == "local" || c.controller_url == controller_url)
    {
        return;
    }
    registry.cells.push(CellRecord {
        id,
        region: std::env::var("BETTERMQ_CELL_REGION").unwrap_or_else(|_| "local".into()),
        controller_url: controller_url.trim_end_matches('/').to_string(),
        label: Some(std::env::var("BETTERMQ_CELL_LABEL").unwrap_or_else(|_| "local".into())),
        admin_token: None,
    });
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminErrorBody {
    pub error: String,
    pub code: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterOverview {
    pub api_version: &'static str,
    pub ready: bool,
    pub cluster_healthy: bool,
    pub node_count: usize,
    pub replication_factor: u32,
    pub min_isr: u32,
    pub catalog_epoch: u64,
    pub local: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartialResult<T> {
    pub ok: Vec<CellOk<T>>,
    pub failed: Vec<CellFailure>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CellOk<T> {
    pub cell: String,
    pub data: T,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CellFailure {
    pub cell: String,
    pub error: String,
}

#[derive(Debug, Deserialize)]
pub struct AuditQuery {
    #[serde(default)]
    #[allow(dead_code)]
    pub limit: Option<usize>,
}

pub fn router(state: AdminState) -> Router {
    // Every route except health sits behind `admin_auth`, so a new route cannot
    // forget the check.
    let protected = Router::new()
        .route("/admin/v1/cluster", get(cluster_overview))
        .route("/admin/v1/nodes", get(list_nodes))
        .route("/admin/v1/shards", get(list_shards))
        .route("/admin/v1/catalog", get(catalog_snapshot))
        .route("/admin/v1/metrics", get(admin_metrics))
        .route("/admin/v1/readyz", get(admin_readyz))
        .route("/admin/v1/audit", get(audit_log))
        .route("/admin/v1/cells", get(list_cells).post(upsert_cell))
        .route("/admin/v1/cells/{id}/members", get(list_cell_members))
        .route("/admin/v1/cells/query/{kind}", get(federated_query))
        .route("/admin/v1/attach", post(attach_node))
        .route("/admin/v1/probe", post(probe_node))
        .route("/admin/v1/expand-rf", post(expand_rf))
        .route("/admin/v1/rebalance", post(start_rebalance))
        .route("/admin/v1/nodes/drain", post(drain_node))
        .route("/admin/v1/controller/voters", post(controller_voter))
        .route("/admin/v1/gc/{shard}", post(advance_gc))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            admin_auth,
        ));
    Router::new()
        .route("/admin/v1/health", get(admin_health))
        .merge(protected)
        .with_state(state)
}

async fn admin_auth(
    State(state): State<AdminState>,
    req: Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(req.headers(), &state)?;
    Ok(next.run(req).await)
}

/// Fail closed: a caller is an admin only when it presents a credential that
/// was affirmatively configured. Embedded nodes accept the local API token;
/// every mode accepts the cluster secret (it already controls `/internal/v1`).
/// An unconfigured node or a standalone panel without a secret rejects
/// everyone; only `/v1/local-auth/setup` is reachable before setup.
fn require_admin(
    headers: &HeaderMap,
    state: &AdminState,
) -> Result<(), (StatusCode, Json<AdminErrorBody>)> {
    if auth::insecure_no_auth() {
        return Ok(());
    }
    let token = bearer(headers);
    if let (Some(token), Some(secret)) = (token, state.cluster_secret.as_deref()) {
        if subtle_eq(token, secret) {
            return Ok(());
        }
    }
    if let Some(local) = &state.local {
        if let Some(store) = &local.local_auth {
            if !store.is_configured() {
                return Err(deny(
                    "local auth not configured — set a panel password at /panel/ first",
                    "auth_not_configured",
                ));
            }
            if token.is_some_and(|t| store.authorize_admin(t)) {
                return Ok(());
            }
        }
        return Err(deny("admin authentication required", "unauthorized"));
    }
    if state.cluster_secret.is_none() {
        return Err(deny(
            "standalone panel requires BETTERMQ_CLUSTER_SECRET",
            "auth_not_configured",
        ));
    }
    Err(deny("admin authentication required", "unauthorized"))
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

fn subtle_eq(a: &str, b: &str) -> bool {
    use subtle::ConstantTimeEq;
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

fn deny(error: &str, code: &'static str) -> (StatusCode, Json<AdminErrorBody>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(AdminErrorBody {
            error: error.into(),
            code,
        }),
    )
}

async fn admin_health(State(state): State<AdminState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "apiVersion": ADMIN_API_VERSION,
        "mode": if state.local.is_some() { "embedded" } else { "standalone" },
        "cells": state.cell_registry.read().cells.len(),
    }))
}

async fn cluster_overview(
    State(state): State<AdminState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    if let Some(local) = &state.local {
        let ready = ops::ready_snapshot(local);
        let (node_count, catalog_epoch, rf, min_isr) = match &local.cluster {
            Some(cluster) => {
                let cfg = cluster.runtime.config();
                let snapshot = cluster.runtime.controller().map(|c| c.state());
                (
                    cfg.node_count(),
                    snapshot.as_ref().map(|s| s.catalog_epoch).unwrap_or(0),
                    snapshot
                        .as_ref()
                        .map(|s| s.replication_factor)
                        .filter(|v| *v > 0)
                        .unwrap_or_else(|| replication_policy(cfg.node_count()).0),
                    snapshot
                        .as_ref()
                        .map(|s| s.min_isr)
                        .filter(|v| *v > 0)
                        .unwrap_or_else(|| replication_policy(cfg.node_count()).1),
                )
            }
            None => (1, 0, 1, 1),
        };
        return Ok(Json(to_json(ClusterOverview {
            api_version: ADMIN_API_VERSION,
            ready: ready.ready,
            cluster_healthy: ready.cluster_healthy,
            node_count,
            replication_factor: rf,
            min_isr,
            catalog_epoch,
            local: true,
        })));
    }
    let mut overview = proxy_controller(&state, reqwest::Method::GET, "cluster", None).await?;
    if let Some(map) = overview.as_object_mut() {
        map.insert("local".into(), serde_json::Value::Bool(false));
    }
    Ok(Json(overview))
}

fn to_json<T: Serialize>(value: T) -> serde_json::Value {
    serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
}

fn admin_error(
    status: StatusCode,
    code: &'static str,
    error: impl Into<String>,
) -> (StatusCode, Json<AdminErrorBody>) {
    (
        status,
        Json(AdminErrorBody {
            error: error.into(),
            code,
        }),
    )
}

#[cfg(not(test))]
const CONTROLLER_MUTATION_TIMEOUT: Duration = Duration::from_secs(60);
#[cfg(test)]
const CONTROLLER_MUTATION_TIMEOUT: Duration = Duration::from_secs(2);

/// Standalone panel: forward an `/admin/v1/{path}` call to the configured
/// controller with the controller cell's `adminToken`. Fails explicitly
/// (503/502) instead of inventing a healthy, empty answer.
async fn proxy_controller(
    state: &AdminState,
    method: reqwest::Method,
    path: &str,
    body: Option<serde_json::Value>,
) -> Result<serde_json::Value, (StatusCode, Json<AdminErrorBody>)> {
    let Some(controller) = state.remote_controller.as_deref() else {
        return Err(admin_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "no_controller",
            "no controller configured — start the panel with --controller <cell admin URL>",
        ));
    };
    let token = state
        .cell_registry
        .read()
        .cells
        .iter()
        .find(|c| same_url(&c.controller_url, controller))
        .and_then(|c| c.admin_token.clone());
    let Some(token) = token else {
        return Err(admin_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "cell_credential_missing",
            format!("cell credential not configured for controller {controller} — set its adminToken in Cells"),
        ));
    };
    let timeout = if method == reqwest::Method::GET {
        FEDERATION_TIMEOUT
    } else {
        CONTROLLER_MUTATION_TIMEOUT
    };
    let client = reqwest::Client::builder()
        .connect_timeout(FEDERATION_CONNECT_TIMEOUT)
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| admin_error(StatusCode::BAD_GATEWAY, "client", e.to_string()))?;
    let mut request = client
        .request(method, format!("{controller}/admin/v1/{path}"))
        .bearer_auth(token);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let resp = request.send().await.map_err(|e| {
        admin_error(
            StatusCode::BAD_GATEWAY,
            "upstream_unavailable",
            format!("controller {controller} unreachable: {e}"),
        )
    })?;
    let status = resp.status();
    let value: Option<serde_json::Value> = resp.json().await.ok();
    if status.is_success() {
        return value.ok_or_else(|| {
            admin_error(
                StatusCode::BAD_GATEWAY,
                "upstream_invalid",
                format!("controller {controller} returned a non-JSON body"),
            )
        });
    }
    let message = value
        .as_ref()
        .and_then(|v| v.get("error"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| format!("controller returned {status}"));
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        // Not the panel user's credential that failed: do not answer 401.
        return Err(admin_error(
            StatusCode::BAD_GATEWAY,
            "upstream_unauthorized",
            format!("controller rejected the cell credential: {message}"),
        ));
    }
    Err(admin_error(
        StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
        "upstream_rejected",
        message,
    ))
}

async fn list_nodes(
    State(state): State<AdminState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    if let Some(local) = &state.local {
        return Ok(Json(nodes_from_local(local)));
    }
    proxy_controller(&state, reqwest::Method::GET, "nodes", None)
        .await
        .map(Json)
}

async fn list_shards(
    State(state): State<AdminState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    if let Some(local) = &state.local {
        return Ok(Json(shards_from_local(local)));
    }
    proxy_controller(&state, reqwest::Method::GET, "shards", None)
        .await
        .map(Json)
}

async fn catalog_snapshot(
    State(state): State<AdminState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    if let Some(local) = &state.local {
        if let Some(cluster) = &local.cluster {
            if let Some(controller) = cluster.runtime.controller() {
                let snap = controller.state();
                return Ok(Json(serde_json::json!({
                    "epoch": snap.catalog_epoch,
                    "records": snap.catalog_records,
                    "authoritative": true,
                })));
            }
        }
        return Ok(Json(serde_json::json!({
            "epoch": 0,
            "records": {},
            "authoritative": false,
        })));
    }
    proxy_controller(&state, reqwest::Method::GET, "catalog", None)
        .await
        .map(Json)
}

async fn admin_metrics(
    State(state): State<AdminState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    if let Some(local) = &state.local {
        return Ok(Json(to_json(ops::metrics_snapshot(local))));
    }
    proxy_controller(&state, reqwest::Method::GET, "metrics", None)
        .await
        .map(Json)
}

async fn admin_readyz(
    State(state): State<AdminState>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<AdminErrorBody>)> {
    if let Some(local) = &state.local {
        let body = ops::ready_snapshot(local);
        let status = if body.ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        return Ok((status, Json(to_json(body))));
    }
    proxy_controller(&state, reqwest::Method::GET, "readyz", None)
        .await
        .map(|body| (StatusCode::OK, Json(body)))
}

async fn audit_log(
    Query(_q): Query<AuditQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    Ok(Json(serde_json::json!({ "events": [] })))
}

async fn list_cells(
    State(state): State<AdminState>,
) -> Result<Json<CellRegistryView>, (StatusCode, Json<AdminErrorBody>)> {
    Ok(Json(CellRegistryView::from(&*state.cell_registry.read())))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpsertCell {
    id: String,
    region: String,
    controller_url: String,
    label: Option<String>,
    /// Omitted or blank on update keeps the saved token.
    #[serde(default)]
    admin_token: Option<String>,
}

const LOCAL_TOKEN_PREFIX: &str = "sk_local_";

/// Cells served from this process's own state (no credential needed).
fn is_local_cell(state: &AdminState, id: &str) -> bool {
    state.local.as_ref().is_some_and(|local| {
        id == "local"
            || local
                .cluster
                .as_ref()
                .is_some_and(|c| c.runtime.config().cluster_id.to_string() == id)
    })
}

fn bad_cell(error: &str) -> (StatusCode, Json<AdminErrorBody>) {
    (
        StatusCode::BAD_REQUEST,
        Json(AdminErrorBody {
            error: error.into(),
            code: "bad_cell",
        }),
    )
}

async fn upsert_cell(
    State(state): State<AdminState>,
    Json(body): Json<UpsertCell>,
) -> Result<Json<CellRegistryView>, (StatusCode, Json<AdminErrorBody>)> {
    let id = body.id.trim().to_string();
    let region = body.region.trim().to_string();
    if id.is_empty() || region.is_empty() {
        return Err(bad_cell("cell id and region are required"));
    }
    let admin_token = body
        .admin_token
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_string);
    if admin_token
        .as_deref()
        .is_some_and(|t| !t.starts_with(LOCAL_TOKEN_PREFIX) || t.len() > 256)
    {
        return Err(bad_cell(
            "adminToken must be the cell's sk_local_… API token",
        ));
    }
    let controller_url = validate_attach_url(&body.controller_url).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error,
                code: "bad_url",
            }),
        )
    })?;
    let mut registry = state.cell_registry.write();
    if let Some(existing) = registry.cells.iter_mut().find(|c| c.id == id) {
        existing.region = region;
        existing.controller_url = controller_url;
        existing.label = body.label;
        if admin_token.is_some() {
            existing.admin_token = admin_token;
        }
    } else {
        if admin_token.is_none() && !is_local_cell(&state, &id) {
            return Err(bad_cell(
                "adminToken (the cell's sk_local_… API token) is required to register a cell",
            ));
        }
        registry.cells.push(CellRecord {
            id,
            region,
            controller_url,
            label: body.label,
            admin_token,
        });
    }
    state.persist_registry(&registry);
    Ok(Json(CellRegistryView::from(&*registry)))
}

async fn federated_query(
    State(state): State<AdminState>,
    Path(kind): Path<String>,
) -> Result<Json<PartialResult<serde_json::Value>>, (StatusCode, Json<AdminErrorBody>)> {
    let cells = state.cell_registry.read().cells.clone();
    if cells.is_empty() {
        if let Some(local) = &state.local {
            return Ok(Json(PartialResult {
                ok: vec![CellOk {
                    cell: "local".into(),
                    data: local_kind_data(local, &kind),
                }],
                failed: vec![],
            }));
        }
    }
    let client = federation_client().map_err(|err| {
        (
            StatusCode::BAD_GATEWAY,
            Json(AdminErrorBody {
                error: err.to_string(),
                code: "client",
            }),
        )
    })?;
    let path = match kind.as_str() {
        "shards" => "shards",
        "nodes" => "nodes",
        "catalog" => "catalog",
        _ => "cluster",
    };
    // Cells are queried concurrently (bounded) and each has its own deadline,
    // so one slow or hung cell cannot hide the healthy ones.
    let results: Vec<Result<CellOk<serde_json::Value>, CellFailure>> =
        futures_util::stream::iter(cells.into_iter().map(|cell| {
            let client = client.clone();
            let state = state.clone();
            let kind = kind.clone();
            async move { query_cell(&client, &state, cell, &kind, path).await }
        }))
        .buffered(FEDERATION_CONCURRENCY)
        .collect()
        .await;
    let mut ok = Vec::new();
    let mut failed = Vec::new();
    for result in results {
        match result {
            Ok(cell) => ok.push(cell),
            Err(failure) => failed.push(failure),
        }
    }
    Ok(Json(PartialResult { ok, failed }))
}

const FEDERATION_CONCURRENCY: usize = 8;
const FEDERATION_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(not(test))]
const FEDERATION_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const FEDERATION_TIMEOUT: Duration = Duration::from_millis(500);

/// HTTP client for calls to registered cells: bounded connect and whole-request
/// deadlines, and no redirects (a cell must not bounce the request elsewhere).
fn federation_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(FEDERATION_CONNECT_TIMEOUT)
        .timeout(FEDERATION_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

async fn query_cell(
    client: &reqwest::Client,
    state: &AdminState,
    cell: CellRecord,
    kind: &str,
    path: &str,
) -> Result<CellOk<serde_json::Value>, CellFailure> {
    if let Some(local) = state
        .local
        .as_ref()
        .filter(|_| is_local_cell(state, &cell.id))
    {
        return Ok(CellOk {
            cell: cell.id,
            data: local_kind_data(local, kind),
        });
    }
    // Registry URLs are operator-supplied, not proven peers: authenticate with
    // the cell's own credential only, never the caller's bearer or the
    // cluster secret.
    let Some(token) = cell.admin_token.as_deref() else {
        return Err(CellFailure {
            cell: cell.id,
            error: "cell credential not configured".into(),
        });
    };
    let url = format!(
        "{}/admin/v1/{path}",
        cell.controller_url.trim_end_matches('/')
    );
    let failure = |error: String| CellFailure {
        cell: cell.id.clone(),
        error,
    };
    let describe = |err: reqwest::Error| {
        if err.is_timeout() {
            format!("timed out after {}s", FEDERATION_TIMEOUT.as_secs_f32())
        } else {
            err.to_string()
        }
    };
    let resp = client
        .get(url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| failure(describe(e)))?;
    if !resp.status().is_success() {
        return Err(failure(format!("status {}", resp.status())));
    }
    let data = resp.json().await.map_err(|e| failure(describe(e)))?;
    Ok(CellOk {
        cell: cell.id.clone(),
        data,
    })
}

fn cluster_from_local(local: &AppState) -> serde_json::Value {
    let (node_count, rf, min_isr) = match &local.cluster {
        Some(cluster) => {
            let n = cluster.runtime.config().node_count();
            let snap = cluster.runtime.controller().map(|c| c.state());
            (
                n,
                snap.as_ref()
                    .map(|s| s.replication_factor)
                    .filter(|v| *v > 0)
                    .unwrap_or_else(|| replication_policy(n).0),
                snap.as_ref()
                    .map(|s| s.min_isr)
                    .filter(|v| *v > 0)
                    .unwrap_or_else(|| replication_policy(n).1),
            )
        }
        None => (1, 1, 1),
    };
    serde_json::json!({
        "nodeCount": node_count,
        "replicationFactor": rf,
        "minIsr": min_isr,
        "ready": ops::ready_snapshot(local).ready,
    })
}

fn nodes_from_local(local: &AppState) -> serde_json::Value {
    let Some(cluster) = &local.cluster else {
        return serde_json::json!({ "nodes": [] });
    };
    let cfg = cluster.runtime.config();
    let now = chrono::Utc::now().timestamp_millis();
    let nodes: Vec<_> = cfg
        .nodes
        .iter()
        .map(|n| {
            let voter = cluster
                .runtime
                .controller()
                .map(|c| c.state().controller_voters.contains(&n.id))
                .unwrap_or(false);
            serde_json::json!({
                "id": n.id,
                "addr": n.addr,
                "alive": cluster.runtime.is_peer_alive(n.id, now),
                "self": n.id == cfg.node_id,
                "controllerVoter": voter,
            })
        })
        .collect();
    serde_json::json!({ "nodes": nodes })
}

fn shards_from_local(local: &AppState) -> serde_json::Value {
    let Some(controller) = local
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    else {
        return serde_json::json!({ "shards": [] });
    };
    let shards: Vec<_> = controller
        .state()
        .shards
        .values()
        .map(|p| {
            serde_json::json!({
                "shard": p.shard,
                "leaderId": p.leader_id,
                "leaderTerm": p.leader_term,
                "replicas": p.replicas,
                "learners": p.learners,
                "isr": p.isr,
            })
        })
        .collect();
    serde_json::json!({ "shards": shards })
}

fn catalog_from_local(local: &AppState) -> serde_json::Value {
    if let Some(controller) = local
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    {
        let snap = controller.state();
        return serde_json::json!({
            "epoch": snap.catalog_epoch,
            "records": snap.catalog_records,
            "authoritative": true,
        });
    }
    serde_json::json!({
        "epoch": 0,
        "records": {},
        "authoritative": false,
    })
}

fn local_kind_data(local: &AppState, kind: &str) -> serde_json::Value {
    match kind {
        "nodes" => nodes_from_local(local),
        "shards" => shards_from_local(local),
        "catalog" => catalog_from_local(local),
        _ => cluster_from_local(local),
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RebalanceRequest {
    shard: u32,
    add: Option<Uuid>,
    remove: Option<Uuid>,
}

async fn start_rebalance(
    State(state): State<AdminState>,
    Json(body): Json<RebalanceRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    if state.local.is_none() {
        return proxy_controller(
            &state,
            reqwest::Method::POST,
            "rebalance",
            Some(to_json(&body)),
        )
        .await
        .map(Json);
    }
    if let Some(local) = &state.local {
        if let Some(cluster) = &local.cluster {
            if let Some(controller) = cluster.runtime.controller() {
                let cmd = broker_raft_meta::ControllerCommand::BeginRebalance {
                    shard: body.shard,
                    add: body.add,
                    remove: body.remove,
                };
                match controller.submit(cmd).await {
                    Ok(snap) => {
                        return Ok(Json(serde_json::json!({
                            "ok": true,
                            "shard": snap.shards.get(&body.shard),
                        })))
                    }
                    Err(err) => {
                        return Err((
                            StatusCode::CONFLICT,
                            Json(AdminErrorBody {
                                error: err.to_string(),
                                code: "rebalance_rejected",
                            }),
                        ))
                    }
                }
            }
        }
    }
    Err((
        StatusCode::BAD_REQUEST,
        Json(AdminErrorBody {
            error: "rebalance requires a local controller".into(),
            code: "no_controller",
        }),
    ))
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GcRequest {
    watermark: u64,
}

async fn advance_gc(
    State(state): State<AdminState>,
    Path(shard): Path<u32>,
    Json(body): Json<GcRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    let Some(local) = &state.local else {
        let path = format!("gc/{shard}");
        return proxy_controller(&state, reqwest::Method::POST, &path, Some(to_json(&body)))
            .await
            .map(Json);
    };
    if let Some(cluster) = &local.cluster {
        if let Some(controller) = cluster.runtime.controller() {
            let _ = controller
                .submit(broker_raft_meta::ControllerCommand::SetGcWatermark {
                    shard,
                    watermark: body.watermark,
                })
                .await
                .map_err(|err| {
                    (
                        StatusCode::CONFLICT,
                        Json(AdminErrorBody {
                            error: err.to_string(),
                            code: "gc_rejected",
                        }),
                    )
                })?;
        }
    }
    let deleted = local
        .broker
        .gc_sealed_below(shard, body.watermark)
        .unwrap_or(0);
    Ok(Json(serde_json::json!({
        "shard": shard,
        "watermark": body.watermark,
        "deletedSegments": deleted,
    })))
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct DrainNodeRequest {
    node_id: Uuid,
}

async fn drain_node(
    State(state): State<AdminState>,
    Json(body): Json<DrainNodeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    if state.local.is_none() {
        return proxy_controller(
            &state,
            reqwest::Method::POST,
            "nodes/drain",
            Some(to_json(&body)),
        )
        .await
        .map(Json);
    }
    let Some(local) = &state.local else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "drain requires a local controller".into(),
                code: "no_controller",
            }),
        ));
    };
    let Some(cluster) = &local.cluster else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "cluster mode not enabled".into(),
                code: "no_cluster",
            }),
        ));
    };
    let Some(controller) = cluster.runtime.controller() else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "drain requires a local controller".into(),
                code: "no_controller",
            }),
        ));
    };
    match controller.drain_data_node(body.node_id).await {
        Ok(report) => Ok(Json(serde_json::json!({
            "ok": true,
            "nodeId": report.node_id,
            "shards": report.shards,
            "leadershipTransfers": report.leadership_transfers,
        }))),
        Err(err) => Err((
            StatusCode::CONFLICT,
            Json(AdminErrorBody {
                error: err.to_string(),
                code: "drain_rejected",
            }),
        )),
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ControllerVoterRequest {
    node_id: Uuid,
    action: String,
}

async fn controller_voter(
    State(state): State<AdminState>,
    Json(body): Json<ControllerVoterRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    if state.local.is_none() {
        return proxy_controller(
            &state,
            reqwest::Method::POST,
            "controller/voters",
            Some(to_json(&body)),
        )
        .await
        .map(Json);
    }
    let Some(local) = &state.local else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "controller voter changes require a local controller".into(),
                code: "no_controller",
            }),
        ));
    };
    let Some(controller) = local
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "controller voter changes require a local controller".into(),
                code: "no_controller",
            }),
        ));
    };
    let command = match body.action.as_str() {
        "add" => broker_raft_meta::ControllerCommand::AddControllerVoter {
            node_id: body.node_id,
        },
        "remove" => broker_raft_meta::ControllerCommand::RemoveControllerVoter {
            node_id: body.node_id,
        },
        other => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(AdminErrorBody {
                    error: format!("action must be add or remove, got {other}"),
                    code: "bad_action",
                }),
            ))
        }
    };
    match controller.submit(command).await {
        Ok(snap) => Ok(Json(serde_json::json!({
            "ok": true,
            "controllerVoters": snap.controller_voters,
        }))),
        Err(err) => Err((
            StatusCode::CONFLICT,
            Json(AdminErrorBody {
                error: err.to_string(),
                code: "voter_rejected",
            }),
        )),
    }
}

const DATA_PROFILES: &[&str] = &["all", "broker"];
const FLEET_PROFILES: &[&str] = &["controller", "dispatch", "gateway", "panel"];

fn is_data_profile(profile: &str) -> bool {
    DATA_PROFILES.contains(&profile)
}

fn is_known_profile(profile: &str) -> bool {
    is_data_profile(profile) || FLEET_PROFILES.contains(&profile)
}

pub fn validate_attach_url(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    let parsed = reqwest::Url::parse(trimmed).map_err(|e| format!("invalid url: {e}"))?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err("url must be http or https".into());
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("url must not contain credentials".into());
    }
    if let Some(host) = parsed.host_str() {
        let lower = host.to_ascii_lowercase();
        if lower == "metadata"
            || lower == "metadata.google.internal"
            || lower.ends_with(".metadata.google.internal")
        {
            return Err("url host is not allowed".into());
        }
        if let Ok(ip) = host.trim_matches(['[', ']']).parse::<std::net::IpAddr>() {
            let blocked_v4 = |v4: std::net::Ipv4Addr| v4.is_unspecified() || v4.is_link_local();
            let blocked = match ip {
                std::net::IpAddr::V4(v4) => blocked_v4(v4),
                std::net::IpAddr::V6(v6) => {
                    v6.is_unspecified()
                        || (v6.segments()[0] & 0xffc0) == 0xfe80
                        || v6.to_ipv4_mapped().is_some_and(blocked_v4)
                }
            };
            if blocked {
                return Err("url host is not allowed".into());
            }
        }
    } else {
        return Err("url host is required".into());
    }
    Ok(trimmed.to_string())
}

async fn list_cell_members(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    let registry = state.cell_registry.read().clone();
    let members: Vec<_> = registry
        .members
        .into_iter()
        .filter(|m| m.cell_id == id)
        .collect();
    let is_controller_cell = registry.cells.iter().any(|c| {
        c.id == id
            && state
                .remote_controller
                .as_deref()
                .is_some_and(|url| same_url(&c.controller_url, url))
    });
    let brokers = if let Some(local) = &state.local {
        nodes_from_local(local)
    } else if is_controller_cell {
        proxy_controller(&state, reqwest::Method::GET, "nodes", None).await?
    } else {
        serde_json::json!({ "nodes": [] })
    };
    Ok(Json(serde_json::json!({
        "cellId": id,
        "brokers": brokers.get("nodes").cloned().unwrap_or(serde_json::json!([])),
        "members": members,
    })))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProbeRequest {
    reach_url: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AttachRequest {
    profile: String,
    reach_url: String,
    advertise_url: String,
    node_name: String,
    #[serde(default)]
    join_token: Option<String>,
    #[serde(default)]
    seed_url: Option<String>,
    #[serde(default)]
    cell_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExpandRfRequest {
    #[serde(default)]
    node_name: Option<String>,
    #[serde(default)]
    advertise_url: Option<String>,
}

async fn probe_node(
    Json(body): Json<ProbeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    let reach = validate_attach_url(&body.reach_url).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error,
                code: "bad_url",
            }),
        )
    })?;
    Ok(Json(probe_reach(&reach).await))
}

async fn probe_reach(reach: &str) -> serde_json::Value {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
    {
        Ok(c) => c,
        Err(err) => {
            return serde_json::json!({
                "ok": false,
                "reachUrl": reach,
                "error": err.to_string(),
                "code": "client",
            })
        }
    };
    match client.get(format!("{reach}/healthz")).send().await {
        Ok(resp) if resp.status().is_success() => {
            let ready = client
                .get(format!("{reach}/readyz"))
                .send()
                .await
                .ok()
                .and_then(|r| r.status().is_success().then_some(true))
                .unwrap_or(false);
            let admin = match client.get(format!("{reach}/admin/v1/health")).send().await {
                Ok(resp) if resp.status().is_success() => {
                    resp.json::<serde_json::Value>().await.ok()
                }
                _ => None,
            };
            let gateway = client
                .get(format!("{reach}/v1/gateway/status"))
                .send()
                .await
                .ok()
                .and_then(|r| r.status().is_success().then_some(true))
                .unwrap_or(false);
            let profile_hint = if gateway {
                "gateway"
            } else if admin
                .as_ref()
                .and_then(|v| v.get("mode"))
                .and_then(|v| v.as_str())
                == Some("standalone")
            {
                "panel"
            } else {
                "broker"
            };
            serde_json::json!({
                "ok": true,
                "reachUrl": reach,
                "ready": ready,
                "profileHint": profile_hint,
                "admin": admin,
            })
        }
        Ok(resp) => serde_json::json!({
            "ok": false,
            "reachUrl": reach,
            "error": format!("HTTP {}", resp.status()),
            "code": if resp.status() == StatusCode::NOT_FOUND { "not_found" } else { "http" },
        }),
        Err(err) => {
            let code = if err.is_connect() {
                "connection_refused"
            } else if err.is_timeout() {
                "timeout"
            } else if err.to_string().contains("dns") {
                "dns"
            } else {
                "network"
            };
            serde_json::json!({
                "ok": false,
                "reachUrl": reach,
                "error": err.to_string(),
                "code": code,
            })
        }
    }
}

async fn attach_node(
    State(state): State<AdminState>,
    Json(body): Json<AttachRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    let profile = body.profile.trim().to_ascii_lowercase();
    if !is_known_profile(&profile) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: format!("unknown profile '{profile}'"),
                code: "bad_profile",
            }),
        ));
    }
    let reach = validate_attach_url(&body.reach_url).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error,
                code: "bad_url",
            }),
        )
    })?;
    let advertise = validate_attach_url(&body.advertise_url).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error,
                code: "bad_url",
            }),
        )
    })?;
    let name = body.node_name.trim();
    if name.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "node name is required".into(),
                code: "bad_name",
            }),
        ));
    }
    let probe = probe_reach(&reach).await;
    if probe.get("ok") != Some(&serde_json::Value::Bool(true)) {
        let error = probe
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("target not reachable")
            .to_string();
        let code = probe
            .get("code")
            .and_then(|v| v.as_str())
            .unwrap_or("unreachable");
        return Err((
            StatusCode::BAD_GATEWAY,
            Json(AdminErrorBody {
                error,
                code: match code {
                    "connection_refused" => "connection_refused",
                    "timeout" => "timeout",
                    "dns" => "dns",
                    "not_found" => "not_found",
                    "http" => "http",
                    _ => "unreachable",
                },
            }),
        ));
    }

    let cell_id = body
        .cell_id
        .clone()
        .or_else(|| {
            state.local.as_ref().and_then(|local| {
                local
                    .cluster
                    .as_ref()
                    .map(|c| c.runtime.config().cluster_id.to_string())
            })
        })
        .unwrap_or_else(|| "local".into());

    if is_data_profile(&profile) {
        let token = body.join_token.as_deref().unwrap_or("").trim();
        if token.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(AdminErrorBody {
                    error: "join token is required for all/broker".into(),
                    code: "join_token_required",
                }),
            ));
        }
        let seed = body
            .seed_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.trim_end_matches('/').to_string())
            .or_else(|| {
                state.local.as_ref().and_then(|local| {
                    local.cluster.as_ref().and_then(|c| {
                        c.runtime
                            .config()
                            .nodes
                            .first()
                            .map(|n| n.addr.trim_end_matches('/').to_string())
                    })
                })
            })
            .ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(AdminErrorBody {
                        error: "seed url is required".into(),
                        code: "seed_required",
                    }),
                )
            })?;
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .map_err(|err| {
                (
                    StatusCode::BAD_GATEWAY,
                    Json(AdminErrorBody {
                        error: err.to_string(),
                        code: "client",
                    }),
                )
            })?;
        let Some(secret) = state.cluster_secret.as_deref() else {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(AdminErrorBody {
                    error: "attaching a broker requires BETTERMQ_CLUSTER_SECRET".into(),
                    code: "cluster_secret_required",
                }),
            ));
        };
        let enroll_body = serde_json::to_vec(&serde_json::json!({
            "seed_url": seed,
            "join_token": token,
            "public_url": advertise,
            "node_name": name,
        }))
        .unwrap_or_default();
        // The target proves the caller holds the cluster secret from this HMAC;
        // the secret itself never goes to the (operator-typed) reach URL.
        let proof = crate::cluster_auth::sign_enroll(
            secret,
            chrono::Utc::now().timestamp_millis(),
            &enroll_body,
        );
        let enroll_url = format!("{reach}/v1/infra/cluster/enroll");
        let resp = client
            .post(&enroll_url)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .header(crate::cluster_auth::ENROLL_PROOF_HEADER, proof)
            .body(enroll_body)
            .send()
            .await
            .map_err(|err| {
                let code = if err.is_connect() {
                    "connection_refused"
                } else {
                    "network"
                };
                (
                    StatusCode::BAD_GATEWAY,
                    Json(AdminErrorBody {
                        error: format!("enroll failed: {err}"),
                        code,
                    }),
                )
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            let code = if status == StatusCode::NOT_FOUND {
                "not_found"
            } else if status == StatusCode::UNAUTHORIZED {
                "unauthorized"
            } else {
                "enroll_rejected"
            };
            return Err((
                StatusCode::BAD_GATEWAY,
                Json(AdminErrorBody {
                    error: if body.is_empty() {
                        format!("enroll rejected: HTTP {status}")
                    } else {
                        body
                    },
                    code,
                }),
            ));
        }
        let enroll: serde_json::Value = resp.json().await.unwrap_or_default();
        let expand = if let Some(local) = &state.local {
            match expand_replication(local, name, &advertise).await {
                Ok(v) => v,
                Err(err) => serde_json::json!({ "ok": false, "error": err }),
            }
        } else {
            serde_json::json!({ "ok": false, "error": "expand requires a local controller" })
        };
        return Ok(Json(serde_json::json!({
            "ok": true,
            "profile": profile,
            "joined": true,
            "enroll": enroll,
            "expand": expand,
            "probe": probe,
        })));
    }

    let member = CellMember {
        profile: profile.clone(),
        reach_url: reach,
        advertise_url: advertise,
        name: name.to_string(),
        cell_id,
    };
    {
        let mut registry = state.cell_registry.write();
        registry.upsert_member(member.clone());
        state.persist_registry(&registry);
    }
    Ok(Json(serde_json::json!({
        "ok": true,
        "profile": profile,
        "joined": false,
        "registered": true,
        "member": member,
        "probe": probe,
        "warning": if profile == "controller" {
            Some("controller Raft voter membership is not automated; this node is registered for health only")
        } else {
            None
        },
    })))
}

async fn expand_rf(
    State(state): State<AdminState>,
    Json(body): Json<ExpandRfRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    if state.local.is_none() {
        return proxy_controller(
            &state,
            reqwest::Method::POST,
            "expand-rf",
            Some(to_json(&body)),
        )
        .await
        .map(Json);
    }
    let Some(local) = &state.local else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "expand requires a local controller".into(),
                code: "no_controller",
            }),
        ));
    };
    let name = body.node_name.as_deref().unwrap_or("");
    let addr = body.advertise_url.as_deref().unwrap_or("");
    expand_replication(local, name, addr)
        .await
        .map(Json)
        .map_err(|error| {
            (
                StatusCode::CONFLICT,
                Json(AdminErrorBody {
                    error,
                    code: "expand_rejected",
                }),
            )
        })
}

async fn expand_replication(
    local: &AppState,
    node_name: &str,
    advertise_url: &str,
) -> Result<serde_json::Value, String> {
    let Some(controller) = local
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    else {
        return Err("no local controller".into());
    };
    let addr = if advertise_url.is_empty() {
        None
    } else {
        Some(validate_attach_url(advertise_url)?)
    };
    // Enrollment URLs stay public; controller/data-node transport is private.
    let addr = broker_config::load_managed_config(&local.broker.config().data_dir)
        .ok()
        .flatten()
        .and_then(|cfg| cfg.cluster)
        .and_then(|cluster| cluster.nodes.into_iter().find(|n| n.name == node_name))
        .map(|node| node.internal_base().to_owned())
        .or(addr);
    if !node_name.is_empty() {
        if let Some(addr) = addr.clone() {
            let id = stable_node_id(node_name);
            controller
                .submit(ControllerCommand::RegisterDataNode {
                    node: DataNodeRecord {
                        id,
                        addr,
                        rack: None,
                        region: None,
                        broker: true,
                        controller_voter: false,
                    },
                })
                .await
                .map_err(|e| e.to_string())?;
        }
    }
    let snap = controller.state();
    let data_count = snap.data_nodes.values().filter(|n| n.broker).count();
    let mut actions = Vec::new();
    if data_count >= 3 && snap.replication_factor < 3 {
        controller
            .submit(ControllerCommand::SetReplicationPolicy {
                factor: 3,
                min_isr: 2,
            })
            .await
            .map_err(|e| e.to_string())?;
        actions.push("set_rf_3".to_string());
    }
    let snap = controller.state();
    let factor = snap.replication_factor.max(1) as usize;
    if data_count >= 3 && factor >= 3 {
        if let Some(new_id) = (!node_name.is_empty()).then(|| stable_node_id(node_name)) {
            let shards: Vec<u32> = snap.shards.keys().copied().collect();
            for shard in shards {
                let Some(placement) = controller.state().shards.get(&shard).cloned() else {
                    continue;
                };
                if placement.replicas.len() >= factor {
                    continue;
                }
                if placement.replicas.contains(&new_id) {
                    continue;
                }
                if !placement.learners.contains(&new_id) {
                    controller
                        .submit(ControllerCommand::BeginRebalance {
                            shard,
                            add: Some(new_id),
                            remove: None,
                        })
                        .await
                        .map_err(|e| e.to_string())?;
                }
                controller
                    .submit(ControllerCommand::PromoteLearner {
                        shard,
                        node_id: new_id,
                    })
                    .await
                    .map_err(|e| e.to_string())?;
                controller
                    .submit(ControllerCommand::CompleteRebalance { shard })
                    .await
                    .map_err(|e| e.to_string())?;
                actions.push(format!("shard_{shard}_add"));
            }
        }
    }
    let snap = controller.state();
    let ha_ready = snap.replication_factor >= 3
        && snap
            .shards
            .values()
            .all(|s| s.isr.len() >= snap.min_isr.max(2) as usize);
    Ok(serde_json::json!({
        "ok": true,
        "dataNodes": data_count,
        "replicationFactor": snap.replication_factor,
        "minIsr": snap.min_isr,
        "haReady": ha_ready,
        "actions": actions,
        "warning": if data_count == 2 {
            Some("RF=2 / minISR=2 — both nodes must be up. Add a third broker for HA.")
        } else if data_count >= 4 && snap.replication_factor == 3 {
            Some("RF stays 3. Extra brokers add shard capacity, not a fourth copy.")
        } else if data_count >= 3 && !ha_ready {
            Some("Expanding replicas to RF=3…")
        } else {
            None
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_registry_survives_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cell-registry.json");
        let mut registry = CellRegistry::default();
        registry.cells.push(CellRecord {
            id: "us-east".into(),
            region: "us-east".into(),
            controller_url: "http://broker1:8080".into(),
            label: Some("prod".into()),
            admin_token: Some("sk_local_prod".into()),
        });
        registry.upsert_member(CellMember {
            profile: "dispatch".into(),
            reach_url: "http://dispatch1:8080".into(),
            advertise_url: "http://dispatch1:8080".into(),
            name: "dispatch1".into(),
            cell_id: "us-east".into(),
        });
        registry.save(&path).unwrap();
        let loaded = CellRegistry::load(&path).unwrap();
        assert_eq!(loaded.cells.len(), 1);
        assert_eq!(loaded.cells[0].id, "us-east");
        assert_eq!(
            loaded.cells[0].admin_token.as_deref(),
            Some("sk_local_prod")
        );
        assert_eq!(loaded.members.len(), 1);
        assert_eq!(loaded.members[0].profile, "dispatch");
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty());
    }

    #[test]
    fn missing_registry_is_empty_and_writable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cell-registry.json");
        let (registry, persist) = CellRegistry::load_for_startup(&path);
        assert!(registry.cells.is_empty());
        assert!(persist);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn corrupt_registry_is_preserved_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cell-registry.json");
        std::fs::write(&path, b"{\"cells\": [{\"id\": \"eu\", \"regi").unwrap();
        assert!(CellRegistry::load(&path).is_err());
        let mut admin = AdminState::remote_with_path(
            Some("http://broker1:8080".into()),
            CellRegistry::load_for_startup(&path).0,
            Some(path.clone()),
        );
        admin.cluster_secret = None;
        let saved: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.to_string_lossy().contains(".corrupt-"))
            .collect();
        assert_eq!(saved.len(), 1, "damaged file kept for recovery");
        assert_eq!(
            std::fs::read(&saved[0]).unwrap(),
            b"{\"cells\": [{\"id\": \"eu\", \"regi"
        );
        let fresh = CellRegistry::load(&path).unwrap();
        assert_eq!(fresh.cells.len(), 1, "only the default controller cell");
    }

    #[test]
    fn attach_url_allows_loopback_and_rfc1918() {
        assert!(validate_attach_url("http://127.0.0.1:8082").is_ok());
        assert!(validate_attach_url("http://broker2:8080").is_ok());
        assert!(validate_attach_url("http://10.0.0.5:8090").is_ok());
        assert!(validate_attach_url("http://169.254.169.254/").is_err());
        assert!(validate_attach_url("http://[::ffff:169.254.169.254]/").is_err());
        assert!(validate_attach_url("http://[fe80::1]:8080").is_err());
        assert!(validate_attach_url("http://metadata.google.internal/").is_err());
        assert!(validate_attach_url("http://user:pass@broker2:8080").is_err());
        assert!(validate_attach_url("ftp://broker2:8080").is_err());
    }

    #[test]
    fn fleet_profiles_are_not_data_nodes() {
        assert!(is_data_profile("all"));
        assert!(is_data_profile("broker"));
        assert!(!is_data_profile("dispatch"));
        assert!(!is_data_profile("gateway"));
        assert!(!is_data_profile("panel"));
        assert!(!is_data_profile("controller"));
    }
}
