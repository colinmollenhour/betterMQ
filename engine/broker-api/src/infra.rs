//! Panel infrastructure manager — storage, cluster, join tokens (no hand-edited JSON).

use crate::cluster::build_cluster_status;
use crate::routes::ApiError;
use crate::AppState;
use axum::{
    extract::{Query, State},
    Json,
};
use broker_config::{
    config_to_view, ensure_cluster_config, load_managed_config, managed_config_path,
    merge_s3_update, save_managed_config, stable_node_id, BetterMqConfig, BetterMqConfigView,
    ClusterConfigSection, ClusterNode, StorageConfig, REDACTED_SECRET,
};
use broker_raft_meta::{ClusterConfig, ClusterRuntime};
use broker_storage::{test_s3_connection, S3ConnectionConfig, StorageMode};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

const JOIN_TOKEN_TTL_MS: i64 = 60 * 60 * 1000;

#[derive(Debug, Serialize)]
pub struct InfraStatusResponse {
    pub managed_config: bool,
    pub needs_restart: bool,
    pub active_storage: &'static str,
    pub pending_storage: Option<&'static str>,
    pub cluster: crate::cluster::ClusterStatusResponse,
    pub join_token_active: bool,
    pub is_cluster_seed: bool,
    pub public_url: String,
    pub node_name: String,
}

#[derive(Debug, Deserialize)]
pub struct ClusterRemoveNodeRequest {
    pub node_name: String,
}

#[derive(Debug, Serialize)]
pub struct ClusterRemoveNodeResponse {
    pub removed: bool,
    pub node_count: usize,
    pub needs_restart: bool,
    pub peers_notified: usize,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct ApplyMembershipResponse {
    pub ok: bool,
    pub needs_restart: bool,
}

#[derive(Debug, Deserialize)]
pub struct NodeUpdateRequest {
    pub name: String,
    pub public_url: String,
    #[serde(default)]
    pub internal_url: Option<String>,
    #[serde(default)]
    pub listen: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct NodeUpdateResponse {
    pub config: BetterMqConfigView,
    pub needs_restart: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMembershipResponse {
    pub nodes: Vec<ClusterNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeUpdateRequest {
    pub node_name: String,
    pub public_url: String,
    #[serde(default)]
    pub internal_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct StorageUpdateRequest {
    pub mode: String,
    #[serde(default)]
    pub s3: Option<broker_config::S3Config>,
}

#[derive(Debug, Serialize)]
pub struct StorageUpdateResponse {
    pub saved: bool,
    pub needs_restart: bool,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct S3TestRequest {
    pub endpoint: String,
    pub bucket: String,
    #[serde(default)]
    pub access_key: String,
    #[serde(default)]
    pub secret_key: String,
    #[serde(default = "default_region")]
    pub region: String,
}

fn default_region() -> String {
    "auto".into()
}

#[derive(Debug, Serialize)]
pub struct S3TestResponse {
    pub ok: bool,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct ClusterCreateResponse {
    pub join_token: String,
    pub expires_at_ms: i64,
    pub needs_restart: bool,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct ClusterJoinRequest {
    pub seed_url: String,
    pub join_token: String,
    pub public_url: String,
    #[serde(default)]
    pub internal_url: Option<String>,
    pub node_name: String,
}

#[derive(Debug, Serialize)]
pub struct ClusterJoinResponse {
    pub joined: bool,
    pub node_count: usize,
    pub needs_restart: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterRegisterRequest {
    pub join_token: String,
    pub public_url: String,
    #[serde(default)]
    pub internal_url: Option<String>,
    pub node_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterRegisterResponse {
    pub cluster_id: Uuid,
    pub nodes: Vec<ClusterNodeInfo>,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeInfo {
    pub name: String,
    pub public_url: String,
    #[serde(default)]
    pub internal_url: Option<String>,
    pub id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct BootstrapQuery {
    /// Prefer `Authorization: Bearer <join_token>` or `x-bettermq-join-token`.
    /// Query `token` is accepted for older joiners but should be avoided (logs/referrers).
    #[serde(default)]
    pub token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapResponse {
    pub cluster_id: Uuid,
    pub generation: u64,
    pub nodes: Vec<ClusterNodeInfo>,
}

#[derive(Debug, Deserialize)]
pub struct PeerTestRequest {
    pub url: String,
}

#[derive(Debug, Serialize)]
pub struct PeerTestResponse {
    pub ok: bool,
    pub message: String,
    pub version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct JoinTokenFile {
    token: String,
    expires_at_ms: i64,
    cluster_id: Uuid,
}

fn data_dir(state: &AppState) -> PathBuf {
    state.broker.config().data_dir.clone()
}

fn active_storage_mode(state: &AppState) -> StorageMode {
    state.broker.config().storage
}

fn storage_label(mode: StorageMode) -> &'static str {
    match mode {
        StorageMode::Local => "local",
        StorageMode::Slate => "slate",
    }
}

fn pending_storage_label(cfg: &BetterMqConfig, active: StorageMode) -> Option<&'static str> {
    let pending = match &cfg.storage {
        StorageConfig::Local => StorageMode::Local,
        StorageConfig::Slate { .. } => StorageMode::Slate,
    };
    if pending != active {
        Some(storage_label(pending))
    } else {
        None
    }
}

// An emptiness check is insufficient: new writes may arrive before restart.
// A live API must never point the next process at a different message store.
fn ensure_same_storage_location(
    previous: &StorageConfig,
    next: &StorageConfig,
    active: StorageMode,
) -> Result<(), ApiError> {
    let unchanged = match (previous, next, active) {
        (StorageConfig::Local, StorageConfig::Local, StorageMode::Local) => true,
        (StorageConfig::Slate { s3: a }, StorageConfig::Slate { s3: b }, StorageMode::Slate) => {
            a.endpoint == b.endpoint
                && a.bucket == b.bucket
                && a.payload_bucket == b.payload_bucket
                && a.region == b.region
        }
        _ => false,
    };
    if !unchanged {
        return Err(ApiError::Conflict("Live storage backend/location changes are disabled. Stop ingestion and perform a verified offline migration, or start a new deployment; no messages are migrated by this API.".into()));
    }
    Ok(())
}

/// Keep `cluster.nodes` in sync when the operator changes this node's public URL in the panel.
fn patch_cluster_node_entry(cfg: &mut BetterMqConfig, previous_name: &str) {
    let Some(cluster) = cfg.cluster.as_mut() else {
        return;
    };
    if !cluster.enabled {
        return;
    }
    let name = cfg.node.name.clone();
    let url = cfg.node.public_url.clone();
    for n in cluster.nodes.iter_mut() {
        if n.name == previous_name || n.name == name {
            n.name = name;
            n.public_url = url;
            n.internal_url = cfg
                .node
                .internal_url
                .clone()
                .or_else(|| n.internal_url.clone());
            return;
        }
    }
}

fn urls_match(a: &str, b: &str) -> bool {
    let norm = |s: &str| {
        s.trim()
            .trim_end_matches('/')
            .replace("http://", "")
            .replace("https://", "")
            .to_lowercase()
    };
    norm(a) == norm(b)
}

fn resolve_cluster_membership(
    cfg: &BetterMqConfig,
    data_dir: &std::path::Path,
) -> Result<Vec<ClusterNode>, ApiError> {
    let mut nodes = cfg
        .cluster
        .as_ref()
        .map(|c| c.nodes.clone())
        .unwrap_or_default();
    if let Ok(rt) = ClusterRuntime::load_config(data_dir) {
        for peer in &rt.nodes {
            if let Some(n) = nodes.iter_mut().find(|n| {
                stable_node_id(&n.name) == peer.id || urls_match(&n.public_url, &peer.addr)
            }) {
                if !urls_match(&n.public_url, &peer.addr) {
                    n.public_url = peer.addr.clone();
                }
                n.internal_url = n.internal_url.clone().or_else(|| peer.internal_url.clone());
                continue;
            }
            nodes.push(ClusterNode {
                internal_url: peer.internal_url.clone(),
                name: peer.addr.clone(),
                public_url: peer.addr.clone(),
            });
        }
    }
    Ok(nodes)
}

fn find_cluster_node_index(
    nodes: &[ClusterNode],
    runtime: &[broker_raft_meta::NodeConfig],
    key: &str,
) -> Option<usize> {
    if let Some(i) = nodes
        .iter()
        .position(|n| n.name == key || urls_match(&n.public_url, key))
    {
        return Some(i);
    }
    let key_id = stable_node_id(key);
    let peer = runtime
        .iter()
        .find(|p| p.id == key_id || urls_match(&p.addr, key))?;
    nodes
        .iter()
        .position(|n| stable_node_id(&n.name) == peer.id || urls_match(&n.public_url, &peer.addr))
}

fn is_cluster_seed(cfg: &BetterMqConfig) -> bool {
    cfg.cluster
        .as_ref()
        .and_then(|c| c.nodes.first())
        .map(|n| n.name == cfg.node.name)
        .unwrap_or(false)
}

fn enrich_cluster_node_names(
    status: &mut crate::cluster::ClusterStatusResponse,
    cfg: &BetterMqConfig,
    data_dir: &std::path::Path,
) {
    let Ok(membership) = resolve_cluster_membership(cfg, data_dir) else {
        return;
    };
    for node in &mut status.nodes {
        if let Some(n) = membership
            .iter()
            .find(|n| stable_node_id(&n.name) == node.id || urls_match(&n.public_url, &node.addr))
        {
            node.name = n.name.clone();
        }
    }
}

/// Push saved membership to every other broker so operators do not need manual sync.
async fn propagate_membership_to_peers(
    cfg: &BetterMqConfig,
    skip_name: Option<&str>,
) -> (usize, Vec<String>) {
    let Some(cluster) = cfg.cluster.as_ref() else {
        return (0, Vec::new());
    };
    if !cluster.enabled {
        return (0, Vec::new());
    }
    let nodes = cluster.nodes.clone();
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
    {
        Ok(c) => c,
        Err(e) => return (0, vec![e.to_string()]),
    };
    let mut notified = 0usize;
    let mut warnings = Vec::new();
    for peer in &cluster.nodes {
        if peer.name == cfg.node.name {
            continue;
        }
        if skip_name == Some(peer.name.as_str()) {
            continue;
        }
        let mut delivered = false;
        for base in crate::cluster::peer_fanout_urls(peer.internal_base()) {
            let url = format!(
                "{}/internal/v1/cluster/apply-membership",
                base.trim_end_matches('/')
            );
            match crate::cluster_auth::apply_cluster_secret(client.post(&url))
                .json(&ClusterMembershipResponse {
                    nodes: nodes.clone(),
                })
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    notified += 1;
                    delivered = true;
                    break;
                }
                Ok(resp) => {
                    warnings.push(format!("{} ({base}): HTTP {}", peer.name, resp.status()))
                }
                Err(e) => warnings.push(format!("{} ({base}): {e}", peer.name)),
            }
        }
        if !delivered {
            warnings.push(format!("{}: could not reach any fan-out URL", peer.name));
        }
    }
    (notified, warnings)
}

async fn push_node_url_to_seed(cfg: &BetterMqConfig) -> Result<(), String> {
    let Some(cluster) = cfg.cluster.as_ref() else {
        return Ok(());
    };
    if !cluster.enabled || cluster.nodes.is_empty() {
        return Ok(());
    }
    let seed = &cluster.nodes[0];
    if seed.name == cfg.node.name {
        return Ok(());
    }
    let seed_url = seed.internal_base();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .map_err(|e| e.to_string())?;
    crate::cluster_auth::apply_cluster_secret(
        client.post(format!("{seed_url}/internal/v1/cluster/update-node")),
    )
    .json(&ClusterNodeUpdateRequest {
        node_name: cfg.node.name.clone(),
        public_url: cfg.node.public_url.clone(),
        internal_url: cfg.node.internal_url.clone(),
    })
    .send()
    .await
    .map_err(|e| format!("cannot reach seed: {e}"))?
    .error_for_status()
    .map_err(|e| format!("seed rejected node update: {e}"))?;
    Ok(())
}

fn load_or_default_config(state: &AppState) -> Result<BetterMqConfig, ApiError> {
    let dir = data_dir(state);
    if let Some(cfg) = load_managed_config(&dir).map_err(io_err)? {
        return Ok(cfg);
    }
    let mut cfg = BetterMqConfig::template_single_local();
    broker_config::apply_node_env_overrides(&mut cfg);
    cfg.data_dir = dir;
    Ok(cfg)
}

fn join_token_path(dir: &std::path::Path) -> PathBuf {
    dir.join("infra").join("join-token.json")
}

fn read_join_token(dir: &std::path::Path) -> Result<Option<JoinTokenFile>, ApiError> {
    let path = join_token_path(dir);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&path).map_err(io_err)?;
    Ok(Some(serde_json::from_slice(&bytes).map_err(io_err)?))
}

fn write_join_token(dir: &std::path::Path, file: &JoinTokenFile) -> Result<(), ApiError> {
    let path = join_token_path(dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(io_err)?;
    }
    std::fs::write(&path, serde_json::to_vec_pretty(file).map_err(io_err)?).map_err(io_err)?;
    broker_storage::set_secret_file_mode(&path);
    Ok(())
}

fn validate_join_token(file: &JoinTokenFile, token: &str) -> bool {
    let now = Utc::now().timestamp_millis();
    if file.expires_at_ms <= now {
        return false;
    }
    use subtle::ConstantTimeEq;
    let a = token.as_bytes();
    let b = file.token.as_bytes();
    a.len() == b.len() && bool::from(a.ct_eq(b))
}

pub async fn infra_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<InfraStatusResponse>, ApiError> {
    let dir = data_dir(&state);
    let managed = managed_config_path(&dir).exists();
    let cfg = load_or_default_config(&state)?;
    let active = active_storage_mode(&state);
    let join = read_join_token(&dir)?;
    let join_active = join
        .as_ref()
        .map(|j| j.expires_at_ms > Utc::now().timestamp_millis())
        .unwrap_or(false);

    let mut cluster =
        build_cluster_status(state.cluster.as_ref(), state.broker.config().partitions);
    enrich_cluster_node_names(&mut cluster, &cfg, &dir);

    Ok(Json(InfraStatusResponse {
        managed_config: managed,
        needs_restart: pending_storage_label(&cfg, active).is_some(),
        active_storage: storage_label(active),
        pending_storage: pending_storage_label(&cfg, active),
        cluster,
        join_token_active: join_active,
        is_cluster_seed: is_cluster_seed(&cfg),
        public_url: cfg.node.public_url,
        node_name: cfg.node.name,
    }))
}

pub async fn infra_get_config(
    State(state): State<Arc<AppState>>,
) -> Result<Json<BetterMqConfigView>, ApiError> {
    let dir = data_dir(&state);
    let cfg = load_or_default_config(&state)?;
    Ok(Json(config_to_view(&cfg, &managed_config_path(&dir))))
}

pub async fn infra_update_node(
    State(state): State<Arc<AppState>>,
    Json(body): Json<NodeUpdateRequest>,
) -> Result<Json<NodeUpdateResponse>, ApiError> {
    if body.name.trim().is_empty() || body.public_url.trim().is_empty() {
        return Err(ApiError::BadRequest("name and public_url required".into()));
    }
    let dir = data_dir(&state);
    let mut cfg = load_or_default_config(&state)?;
    let previous_name = cfg.node.name.clone();
    cfg.node.name = body.name.trim().to_string();
    cfg.node.public_url = body.public_url.trim().trim_end_matches('/').to_string();
    if body.internal_url.is_some() {
        cfg.node.internal_url = body.internal_url;
    }
    if let Some(listen) = body.listen {
        cfg.node.listen = listen;
    }
    patch_cluster_node_entry(&mut cfg, &previous_name);
    cfg.validate()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    save_managed_config(&dir, &cfg).map_err(io_err)?;
    let in_cluster = cfg
        .cluster
        .as_ref()
        .map(|c| c.enabled && !c.nodes.is_empty())
        .unwrap_or(false);
    if in_cluster {
        ensure_cluster_config(&cfg, &dir).map_err(|e| ApiError::BadRequest(e.to_string()))?;
        if let Err(e) = push_node_url_to_seed(&cfg).await {
            return Err(ApiError::BadRequest(format!(
                "saved locally but seed update failed: {e}"
            )));
        }
    }
    let needs_restart = in_cluster;
    Ok(Json(NodeUpdateResponse {
        config: config_to_view(&cfg, &managed_config_path(&dir)),
        needs_restart,
        message: if needs_restart {
            "Node saved. Restart this broker so cluster peers use the new address.".into()
        } else {
            "Node settings saved.".into()
        },
    }))
}

pub async fn infra_update_storage(
    State(state): State<Arc<AppState>>,
    Json(body): Json<StorageUpdateRequest>,
) -> Result<Json<StorageUpdateResponse>, ApiError> {
    let dir = data_dir(&state);
    let mut cfg = load_or_default_config(&state)?;
    let existing_s3 = match &cfg.storage {
        StorageConfig::Slate { s3 } => Some(s3.clone()),
        _ => None,
    };

    let next_storage = match body.mode.to_lowercase().as_str() {
        "local" => StorageConfig::Local,
        "slate" | "s3" => {
            state
                .broker
                .layout()
                .validate_storage(broker_storage::StorageMode::Slate)
                .map_err(|error| ApiError::BadRequest(error.to_string()))?;
            let s3 = body
                .s3
                .ok_or_else(|| ApiError::BadRequest("s3 config required for slate mode".into()))?;
            StorageConfig::Slate {
                s3: merge_s3_update(existing_s3.as_ref(), s3)
                    .map_err(|e| ApiError::BadRequest(e.to_string()))?,
            }
        }
        other => {
            return Err(ApiError::BadRequest(format!(
                "unknown storage mode: {other}"
            )))
        }
    };
    let active = active_storage_mode(&state);
    ensure_same_storage_location(&cfg.storage, &next_storage, active)?;
    let credentials_changed = match (&cfg.storage, &next_storage) {
        (StorageConfig::Slate { s3: a }, StorageConfig::Slate { s3: b }) => {
            a.access_key != b.access_key || a.secret_key != b.secret_key
        }
        _ => false,
    };
    cfg.storage = next_storage;
    cfg.validate()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    save_managed_config(&dir, &cfg).map_err(io_err)?;

    let needs_restart = credentials_changed;
    Ok(Json(StorageUpdateResponse {
        saved: true,
        needs_restart,
        message: if needs_restart {
            "Storage credentials saved. Restart this broker to apply.".into()
        } else {
            "Storage settings saved.".into()
        },
    }))
}

pub async fn infra_test_storage(
    State(state): State<Arc<AppState>>,
    Json(body): Json<S3TestRequest>,
) -> Result<Json<S3TestResponse>, ApiError> {
    let mut secret = body.secret_key;
    if secret.is_empty() || secret == REDACTED_SECRET {
        secret = match load_or_default_config(&state)?.storage {
            StorageConfig::Slate { s3 } => s3.secret_key,
            StorageConfig::Local => {
                return Err(ApiError::BadRequest("secret_key required".into()));
            }
        };
    }
    let cfg = S3ConnectionConfig {
        endpoint: body.endpoint,
        bucket: body.bucket,
        access_key: body.access_key,
        secret_key: secret,
        region: body.region,
    };
    match test_s3_connection(&cfg).await {
        Ok(()) => Ok(Json(S3TestResponse {
            ok: true,
            message: "Connected to object storage successfully.".into(),
        })),
        Err(e) => Ok(Json(S3TestResponse {
            ok: false,
            message: e.to_string(),
        })),
    }
}

pub async fn infra_cluster_create(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ClusterCreateResponse>, ApiError> {
    if state.uses_cloud_auth() {
        return Err(ApiError::BadRequest(
            "cluster create via panel is for self-host mode only".into(),
        ));
    }
    let dir = data_dir(&state);
    let mut cfg = load_or_default_config(&state)?;
    let cluster_id = Uuid::new_v4();
    if state.cluster.is_some()
        || cfg
            .cluster
            .as_ref()
            .is_some_and(|c| c.id.is_some() || !c.nodes.is_empty())
        || dir.join("cluster-config.json").exists()
    {
        return Err(ApiError::Conflict(
            "cluster membership already exists; create cannot replace it".into(),
        ));
    }
    let token = format!("join_{}", Uuid::new_v4().simple());
    let expires = Utc::now().timestamp_millis() + JOIN_TOKEN_TTL_MS;

    cfg.cluster = Some(ClusterConfigSection {
        enabled: true,
        id: Some(cluster_id.to_string()),
        nodes: vec![ClusterNode {
            internal_url: cfg.node.internal_url.clone(),
            name: cfg.node.name.clone(),
            public_url: cfg.node.public_url.clone(),
        }],
        shared_meta_dir: std::env::var("BETTERMQ_SHARED_META_DIR")
            .ok()
            .map(PathBuf::from),
    });
    cfg.validate()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    save_managed_config(&dir, &cfg).map_err(io_err)?;
    ensure_cluster_config(&cfg, &dir).map_err(|e| ApiError::BadRequest(e.to_string()))?;

    write_join_token(
        &dir,
        &JoinTokenFile {
            token: token.clone(),
            expires_at_ms: expires,
            cluster_id,
        },
    )?;

    Ok(Json(ClusterCreateResponse {
        join_token: token,
        expires_at_ms: expires,
        needs_restart: false,
        message: "Cluster seed created. Share the join token with other servers, then join from their Infrastructure panel. Full HA activates after a second node joins and all brokers restart.".into(),
    }))
}

fn extract_join_token(
    headers: &axum::http::HeaderMap,
    query_token: Option<&str>,
) -> Result<String, ApiError> {
    if let Some(auth) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
    {
        let t = auth.trim();
        if !t.is_empty() {
            return Ok(t.to_string());
        }
    }
    if let Some(h) = headers
        .get("x-bettermq-join-token")
        .and_then(|v| v.to_str().ok())
    {
        let t = h.trim();
        if !t.is_empty() {
            return Ok(t.to_string());
        }
    }
    if let Some(t) = query_token.map(str::trim).filter(|s| !s.is_empty()) {
        let allow_query = matches!(
            std::env::var("BETTERMQ_JOIN_TOKEN_QUERY")
                .ok()
                .as_deref()
                .map(str::trim),
            Some("1") | Some("true") | Some("TRUE") | Some("yes")
        );
        if allow_query {
            return Ok(t.to_string());
        }
    }
    Err(ApiError::Unauthorized(
        "missing join token (use Authorization: Bearer or x-bettermq-join-token)".into(),
    ))
}

pub async fn infra_join_bootstrap(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Query(q): Query<BootstrapQuery>,
) -> Result<Json<BootstrapResponse>, ApiError> {
    let dir = data_dir(&state);
    let Some(file) = read_join_token(&dir)? else {
        return Err(ApiError::BadRequest(
            "no active join token on this node".into(),
        ));
    };
    let token = extract_join_token(&headers, q.token.as_deref())?;
    if !validate_join_token(&file, &token) {
        return Err(ApiError::BadRequest("invalid or expired join token".into()));
    }
    let cfg = load_or_default_config(&state)?;
    let cluster = cfg
        .cluster
        .as_ref()
        .ok_or_else(|| ApiError::BadRequest("this node is not a cluster seed".into()))?;
    let nodes = cluster
        .nodes
        .iter()
        .map(|n| ClusterNodeInfo {
            name: n.name.clone(),
            public_url: n.public_url.clone(),
            internal_url: n.internal_url.clone(),
            id: stable_node_id(&n.name),
        })
        .collect();
    let generation = ClusterRuntime::load_config(&dir)
        .map(|c| c.generation)
        .unwrap_or(1);
    Ok(Json(BootstrapResponse {
        cluster_id: file.cluster_id,
        generation,
        nodes,
    }))
}

pub async fn infra_cluster_register(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClusterRegisterRequest>,
) -> Result<Json<ClusterRegisterResponse>, ApiError> {
    let dir = data_dir(&state);
    let Some(file) = read_join_token(&dir)? else {
        return Err(ApiError::BadRequest("no active join token".into()));
    };
    if !validate_join_token(&file, &body.join_token) {
        return Err(ApiError::BadRequest("invalid or expired join token".into()));
    }
    let mut cfg = load_or_default_config(&state)?;
    let url = body.public_url.trim().trim_end_matches('/').to_string();
    let node_name = body.node_name.trim().to_string();
    {
        let cluster = cfg
            .cluster
            .as_mut()
            .ok_or_else(|| ApiError::BadRequest("not a cluster seed".into()))?;
        if cluster
            .nodes
            .iter()
            .any(|n| n.public_url == url || n.name == node_name)
        {
            return Err(ApiError::BadRequest("node already registered".into()));
        }
        cluster.nodes.push(ClusterNode {
            internal_url: body.internal_url.clone(),
            name: node_name.clone(),
            public_url: url,
        });
    }
    cfg.validate()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let nodes: Vec<ClusterNodeInfo> = cfg
        .cluster
        .as_ref()
        .map(|c| {
            c.nodes
                .iter()
                .map(|n| ClusterNodeInfo {
                    name: n.name.clone(),
                    public_url: n.public_url.clone(),
                    internal_url: n.internal_url.clone(),
                    id: stable_node_id(&n.name),
                })
                .collect()
        })
        .unwrap_or_default();
    save_managed_config(&dir, &cfg).map_err(io_err)?;
    ensure_cluster_config(&cfg, &dir).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let (notified, warnings) = propagate_membership_to_peers(&cfg, Some(node_name.as_str())).await;
    if !warnings.is_empty() {
        tracing::warn!(
            notified,
            warnings = ?warnings,
            "some peers did not accept membership push"
        );
    }
    // HA M1: do not push catalog to registrant public_url (exfiltration risk).
    // Joiner pulls catalog after proving membership (merge_catalog_from_peers).
    let generation = ClusterRuntime::load_config(&dir)
        .map(|c| c.generation)
        .unwrap_or(1);
    Ok(Json(ClusterRegisterResponse {
        cluster_id: file.cluster_id,
        nodes,
        generation,
    }))
}

pub async fn infra_cluster_join(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClusterJoinRequest>,
) -> Result<Json<ClusterJoinResponse>, ApiError> {
    if state.uses_cloud_auth() {
        return Err(ApiError::BadRequest(
            "cluster join via panel is for self-host mode only".into(),
        ));
    }
    let dir = data_dir(&state);
    let seed = body.seed_url.trim().trim_end_matches('/');
    // Prefer header over query string so join tokens are not written to access logs.
    let bootstrap_url = format!("{seed}/v1/infra/join/bootstrap");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let bootstrap: BootstrapResponse = client
        .get(&bootstrap_url)
        .header("x-bettermq-join-token", &body.join_token)
        .send()
        .await
        .map_err(|e| ApiError::BadRequest(format!("cannot reach seed: {e}")))?
        .error_for_status()
        .map_err(|e| ApiError::BadRequest(format!("seed rejected bootstrap: {e}")))?
        .json()
        .await
        .map_err(|e| ApiError::BadRequest(format!("invalid bootstrap response: {e}")))?;

    let public_url = body.public_url.trim().trim_end_matches('/').to_string();
    let local_cfg = load_or_default_config(&state)?;
    let internal_url = body.internal_url.clone().or(local_cfg.node.internal_url);
    let node_name = body.node_name.trim().to_string();
    let register_url = format!("{seed}/v1/infra/cluster/register");
    let registered: ClusterRegisterResponse = client
        .post(&register_url)
        .json(&ClusterRegisterRequest {
            internal_url: internal_url.clone(),
            join_token: body.join_token,
            public_url: public_url.clone(),
            node_name: node_name.clone(),
        })
        .send()
        .await
        .map_err(|e| ApiError::BadRequest(format!("register with seed failed: {e}")))?
        .error_for_status()
        .map_err(|e| ApiError::BadRequest(format!("seed rejected register: {e}")))?
        .json()
        .await
        .map_err(|e| ApiError::BadRequest(format!("invalid register response: {e}")))?;

    let nodes: Vec<ClusterNode> = registered
        .nodes
        .iter()
        .map(|n| ClusterNode {
            internal_url: n.internal_url.clone(),
            name: n.name.clone(),
            public_url: n.public_url.clone(),
        })
        .collect();

    let mut cfg = load_or_default_config(&state)?;
    cfg.node.name = node_name;
    cfg.node.public_url = public_url;
    cfg.node.internal_url = internal_url;
    cfg.cluster = Some(ClusterConfigSection {
        enabled: true,
        id: Some(bootstrap.cluster_id.to_string()),
        nodes: nodes.clone(),
        shared_meta_dir: std::env::var("BETTERMQ_SHARED_META_DIR")
            .ok()
            .map(PathBuf::from),
    });
    cfg.validate()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    save_managed_config(&dir, &cfg).map_err(io_err)?;
    ensure_cluster_config(&cfg, &dir).map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let peer_bases: Vec<String> = nodes.iter().map(|n| n.internal_base().to_owned()).collect();
    let seed_internal = nodes
        .first()
        .map(ClusterNode::internal_base)
        .unwrap_or(seed);
    if let Err(e) = crate::cluster::pull_auth_from_seed(&state, seed_internal).await {
        tracing::warn!(error = ?e, "auth pull from seed failed during join");
    }
    let catalog_note = match crate::cluster::merge_catalog_from_peers(&state, &peer_bases).await {
        Ok(snap) => format!(
            " Catalog synced ({} flows, {} queues, {} schedules).",
            snap.flows.len(),
            snap.queues.len(),
            snap.crons.len()
        ),
        Err(e) => {
            tracing::warn!(error = ?e, "catalog merge from peers failed during join");
            String::from(" Catalog sync failed — use Infrastructure → Re-sync after restart.")
        }
    };

    Ok(Json(ClusterJoinResponse {
        joined: true,
        node_count: nodes.len(),
        needs_restart: true,
        message: format!(
            "Joined cluster. Membership was pushed to other brokers automatically — restart every cluster node to apply.{catalog_note}"
        ),
    }))
}

/// Same as join, but authenticated by the seed join token (public). Used by the
/// panel attach proxy so a fresh broker does not need its own API token.
pub async fn infra_cluster_enroll(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClusterJoinRequest>,
) -> Result<Json<ClusterJoinResponse>, ApiError> {
    if state.dispatch_fleet {
        return Err(ApiError::BadRequest(
            "enroll is not supported on dispatch fleet or WAL-less processes".into(),
        ));
    }
    infra_cluster_join(State(state), Json(body)).await
}

pub async fn infra_cluster_remove_node(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClusterRemoveNodeRequest>,
) -> Result<Json<ClusterRemoveNodeResponse>, ApiError> {
    if state.uses_cloud_auth() {
        return Err(ApiError::BadRequest(
            "cluster remove via panel is for self-host mode only".into(),
        ));
    }
    let dir = data_dir(&state);
    let mut cfg = load_or_default_config(&state)?;
    if !is_cluster_seed(&cfg) {
        return Err(ApiError::BadRequest(
            "only the seed broker can remove other nodes".into(),
        ));
    }
    let key = body.node_name.trim();
    if key.is_empty() {
        return Err(ApiError::BadRequest("node_name required".into()));
    }
    if !cfg.cluster.as_ref().map(|c| c.enabled).unwrap_or(false) {
        return Err(ApiError::BadRequest("cluster not enabled".into()));
    }
    let runtime = ClusterRuntime::load_config(&dir)
        .map(|c| c.nodes)
        .unwrap_or_default();
    let mut membership = resolve_cluster_membership(&cfg, &dir)?;
    let idx = find_cluster_node_index(&membership, &runtime, key)
        .ok_or_else(|| ApiError::BadRequest(format!("unknown node: {key}")))?;
    let removed = membership.remove(idx);
    let removed_name = removed.name.clone();
    if removed.name == cfg.node.name || urls_match(&removed.public_url, &cfg.node.public_url) {
        return Err(ApiError::BadRequest(
            "cannot remove the seed node — remove followers first or reset this broker".into(),
        ));
    }
    let removed_id = runtime
        .iter()
        .find(|peer| {
            peer.id == stable_node_id(&removed.name) || urls_match(&peer.addr, &removed.public_url)
        })
        .map(|peer| peer.id)
        .unwrap_or_else(|| stable_node_id(&removed.name));
    if let Some(cluster) = &state.cluster {
        if let Some(controller) = cluster.runtime.controller() {
            controller
                .drain_data_node(removed_id)
                .await
                .map_err(|error| ApiError::Conflict(error.to_string()))?;
        }
    }
    let cluster = cfg
        .cluster
        .as_mut()
        .ok_or_else(|| ApiError::BadRequest("not in a cluster".into()))?;
    cluster.nodes = membership;
    cfg.validate()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    save_managed_config(&dir, &cfg).map_err(io_err)?;
    ensure_cluster_config(&cfg, &dir).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let (peers_notified, warnings) = propagate_membership_to_peers(&cfg, None).await;
    let warn_suffix = if warnings.is_empty() {
        String::new()
    } else {
        format!(
            " Some peers need a manual restart or re-sync: {}",
            warnings.join("; ")
        )
    };
    Ok(Json(ClusterRemoveNodeResponse {
        removed: true,
        node_count: cfg.cluster.as_ref().map(|c| c.nodes.len()).unwrap_or(0),
        needs_restart: true,
        peers_notified,
        message: format!(
            "Removed {removed_name} from the cluster. Other brokers were notified automatically.{warn_suffix}"
        ),
    }))
}

pub async fn infra_cluster_sync(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ClusterRegisterResponse>, ApiError> {
    let dir = data_dir(&state);
    let cfg = load_or_default_config(&state)?;
    let cluster = cfg
        .cluster
        .as_ref()
        .ok_or_else(|| ApiError::BadRequest("not in a cluster".into()))?;
    if cluster.nodes.is_empty() {
        return Err(ApiError::BadRequest("cluster has no nodes".into()));
    }
    let seed_url = cluster
        .nodes
        .first()
        .map(|n| n.internal_base().to_owned())
        .ok_or_else(|| ApiError::BadRequest("no seed node".into()))?;
    let seed_base = seed_url.trim().trim_end_matches('/');
    let client = reqwest::Client::new();
    let mut nodes: Vec<ClusterNode> = match crate::cluster_auth::apply_cluster_secret(
        client.get(format!("{seed_base}/internal/v1/cluster/membership")),
    )
    .send()
    .await
    {
        Ok(resp) if resp.status().is_success() => resp
            .json::<ClusterMembershipResponse>()
            .await
            .map(|m| m.nodes)
            .map_err(|e| ApiError::BadRequest(format!("invalid membership response: {e}")))?,
        _ => {
            let remote: ClusterConfig = crate::cluster_auth::apply_cluster_secret(
                client.get(format!("{seed_base}/internal/v1/cluster")),
            )
            .send()
            .await
            .map_err(|e| ApiError::BadRequest(format!("cannot reach seed: {e}")))?
            .error_for_status()
            .map_err(|e| ApiError::BadRequest(format!("seed error: {e}")))?
            .json()
            .await
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;
            remote
                .nodes
                .iter()
                .map(|n| {
                    let name = cluster
                        .nodes
                        .iter()
                        .find(|c| stable_node_id(&c.name) == n.id || c.public_url == n.addr)
                        .map(|c| c.name.clone())
                        .unwrap_or_else(|| n.addr.clone());
                    ClusterNode {
                        internal_url: n.internal_url.clone(),
                        name,
                        public_url: n.addr.clone(),
                    }
                })
                .collect()
        }
    };

    for n in &mut nodes {
        if n.name == cfg.node.name {
            n.name = cfg.node.name.clone();
            n.public_url = cfg.node.public_url.clone();
            n.internal_url = cfg
                .node
                .internal_url
                .clone()
                .or_else(|| n.internal_url.clone());
        }
    }

    let mut cfg = cfg;
    if let Some(c) = cfg.cluster.as_mut() {
        c.nodes = nodes.clone();
    }
    save_managed_config(&dir, &cfg).map_err(io_err)?;
    ensure_cluster_config(&cfg, &dir).map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let node_infos: Vec<ClusterNodeInfo> = nodes
        .iter()
        .map(|n| ClusterNodeInfo {
            name: n.name.clone(),
            public_url: n.public_url.clone(),
            internal_url: n.internal_url.clone(),
            id: stable_node_id(&n.name),
        })
        .collect();

    let runtime =
        ClusterRuntime::load_config(&dir).map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let peer_bases: Vec<String> = nodes.iter().map(|n| n.internal_base().to_owned()).collect();
    if let Err(e) = crate::cluster::pull_auth_from_seed(&state, seed_base).await {
        tracing::warn!(error = ?e, "auth pull from seed failed during sync");
    }
    if let Err(e) = crate::cluster::merge_catalog_from_peers(&state, &peer_bases).await {
        tracing::warn!(error = ?e, "catalog merge from peers failed during sync");
    }

    Ok(Json(ClusterRegisterResponse {
        cluster_id: runtime.cluster_id,
        generation: runtime.generation,
        nodes: node_infos,
    }))
}

/// Cluster membership from saved `bettermq.json` (not the in-memory runtime snapshot).
pub async fn internal_cluster_membership(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ClusterMembershipResponse>, ApiError> {
    let cfg = load_or_default_config(&state)?;
    let nodes = cfg
        .cluster
        .as_ref()
        .filter(|c| c.enabled)
        .map(|c| c.nodes.clone())
        .ok_or_else(|| ApiError::BadRequest("not in a cluster".into()))?;
    Ok(Json(ClusterMembershipResponse { nodes }))
}

/// Apply membership pushed from the seed (auto-sync).
pub async fn internal_cluster_apply_membership(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClusterMembershipResponse>,
) -> Result<Json<ApplyMembershipResponse>, ApiError> {
    let dir = data_dir(&state);
    let mut cfg = load_or_default_config(&state)?;
    let Some(cluster) = cfg.cluster.as_mut() else {
        return Err(ApiError::BadRequest("not in a cluster".into()));
    };
    if !cluster.enabled {
        return Err(ApiError::BadRequest("cluster not enabled".into()));
    }
    if body.nodes.is_empty() {
        return Err(ApiError::BadRequest("empty membership".into()));
    }
    let self_name = cfg.node.name.clone();
    cluster.nodes = body.nodes;
    patch_cluster_node_entry(&mut cfg, &self_name);
    if !cfg
        .cluster
        .as_ref()
        .map(|c| c.nodes.iter().any(|n| n.name == self_name))
        .unwrap_or(false)
    {
        return Err(ApiError::BadRequest(
            "membership update does not include this node".into(),
        ));
    }
    cfg.validate()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    save_managed_config(&dir, &cfg).map_err(io_err)?;
    ensure_cluster_config(&cfg, &dir).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let peer_bases: Vec<String> = cfg
        .cluster
        .as_ref()
        .map(|c| {
            c.nodes
                .iter()
                .map(|n| n.internal_base().to_owned())
                .collect()
        })
        .unwrap_or_default();
    if !peer_bases.is_empty() {
        if let Err(e) = crate::cluster::merge_catalog_from_peers(&state, &peer_bases).await {
            tracing::warn!(error = ?e, "catalog merge from peers failed on membership apply");
        }
    }
    Ok(Json(ApplyMembershipResponse {
        ok: true,
        needs_restart: true,
    }))
}

/// Follower pushes an updated public URL to the seed's saved cluster list.
pub async fn internal_cluster_update_node(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClusterNodeUpdateRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let dir = data_dir(&state);
    let mut cfg = load_or_default_config(&state)?;
    let Some(cluster) = cfg.cluster.as_mut() else {
        return Err(ApiError::BadRequest("not in a cluster".into()));
    };
    if !cluster.enabled {
        return Err(ApiError::BadRequest("cluster not enabled".into()));
    }
    let node_name = body.node_name.trim();
    let public_url = body.public_url.trim().trim_end_matches('/');
    if node_name.is_empty() || public_url.is_empty() {
        return Err(ApiError::BadRequest(
            "node_name and public_url required".into(),
        ));
    }
    let Some(entry) = cluster.nodes.iter_mut().find(|n| n.name == node_name) else {
        return Err(ApiError::BadRequest(format!(
            "unknown cluster node: {node_name}"
        )));
    };
    entry.public_url = public_url.to_string();
    if let Some(internal_url) = body.internal_url {
        entry.internal_url = Some(internal_url);
    }
    cfg.validate()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    save_managed_config(&dir, &cfg).map_err(io_err)?;
    ensure_cluster_config(&cfg, &dir).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn infra_test_peer(
    Json(body): Json<PeerTestRequest>,
) -> Result<Json<PeerTestResponse>, ApiError> {
    let base = body.url.trim().trim_end_matches('/');
    validate_peer_probe_url(base)?;
    let url = format!("{base}/healthz");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    match client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => {
            let version = resp
                .json::<serde_json::Value>()
                .await
                .ok()
                .and_then(|v| v.get("version").and_then(|x| x.as_str()).map(String::from));
            Ok(Json(PeerTestResponse {
                ok: true,
                message: "Peer is reachable.".into(),
                version,
            }))
        }
        Ok(resp) => Ok(Json(PeerTestResponse {
            ok: false,
            message: format!("HTTP {}", resp.status()),
            version: None,
        })),
        Err(e) => Ok(Json(PeerTestResponse {
            ok: false,
            message: e.to_string(),
            version: None,
        })),
    }
}

/// Reject probe URLs that target cloud metadata or obvious link-local addresses.
fn validate_peer_probe_url(raw: &str) -> Result<(), ApiError> {
    let parsed =
        reqwest::Url::parse(raw).map_err(|_| ApiError::BadRequest("invalid peer URL".into()))?;
    match parsed.scheme() {
        "http" | "https" => {}
        _ => {
            return Err(ApiError::BadRequest(
                "peer URL must be http or https".into(),
            ))
        }
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| ApiError::BadRequest("peer URL missing host".into()))?
        .to_ascii_lowercase();
    if host == "metadata.google.internal"
        || host == "metadata"
        || host.ends_with(".metadata.google.internal")
    {
        return Err(ApiError::BadRequest("peer URL host is not allowed".into()));
    }
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        if is_blocked_probe_ip(ip) {
            return Err(ApiError::BadRequest("peer URL host is not allowed".into()));
        }
    } else if host == "localhost" || host.ends_with(".localhost") {
        return Err(ApiError::BadRequest("peer URL host is not allowed".into()));
    }
    Ok(())
}

fn is_blocked_probe_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            // Allow RFC1918 so operators can test LAN peers; block loopback,
            // link-local (incl. cloud metadata 169.254.169.254), and unspecified.
            v4.is_loopback() || v4.is_link_local() || v4.is_unspecified()
        }
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback() || v6.is_unspecified() || (v6.segments()[0] & 0xffc0) == 0xfe80
            // link-local
        }
    }
}

fn io_err(e: impl std::error::Error) -> ApiError {
    ApiError::Broker(broker_partition::BrokerError::Storage(
        broker_storage::LogError::Io(std::io::Error::other(e.to_string())),
    ))
}

pub fn public_infra_routes() -> axum::Router<std::sync::Arc<AppState>> {
    use axum::routing::{get, post};
    axum::Router::new()
        .route("/v1/infra/join/bootstrap", get(infra_join_bootstrap))
        .route("/v1/infra/cluster/register", post(infra_cluster_register))
        .route("/v1/infra/cluster/enroll", post(infra_cluster_enroll))
}

pub fn protected_infra_routes() -> axum::Router<std::sync::Arc<AppState>> {
    use axum::routing::{get, post, put};
    axum::Router::new()
        .route("/v1/infra/status", get(infra_status))
        .route("/v1/infra/config", get(infra_get_config))
        .route("/v1/infra/node", put(infra_update_node))
        .route("/v1/infra/storage", put(infra_update_storage))
        .route("/v1/infra/storage/test", post(infra_test_storage))
        .route("/v1/infra/cluster/create", post(infra_cluster_create))
        .route("/v1/infra/cluster/join", post(infra_cluster_join))
        .route("/v1/infra/cluster/sync", post(infra_cluster_sync))
        .route("/v1/infra/cluster/test-peer", post(infra_test_peer))
        .route(
            "/v1/infra/cluster/remove-node",
            post(infra_cluster_remove_node),
        )
}

#[cfg(test)]
mod storage_location_tests {
    use super::*;

    #[test]
    fn only_same_store_credential_changes_are_allowed() {
        let s3 = broker_config::S3Config {
            endpoint: "https://s3.example.com".into(),
            bucket: "wal".into(),
            payload_bucket: Some("payload".into()),
            region: "auto".into(),
            access_key: "old-access".into(),
            secret_key: "old-secret".into(),
        };
        let previous = StorageConfig::Slate { s3: s3.clone() };
        let mut credentials = s3.clone();
        credentials.access_key = "new-access".into();
        credentials.secret_key = "new-secret".into();
        assert!(ensure_same_storage_location(
            &previous,
            &StorageConfig::Slate { s3: credentials },
            StorageMode::Slate
        )
        .is_ok());
        for field in ["endpoint", "bucket", "payload", "region"] {
            let mut changed = s3.clone();
            match field {
                "endpoint" => changed.endpoint = "https://other.example.com".into(),
                "bucket" => changed.bucket = "other".into(),
                "payload" => changed.payload_bucket = Some("other".into()),
                _ => changed.region = "other".into(),
            }
            assert!(matches!(
                ensure_same_storage_location(
                    &previous,
                    &StorageConfig::Slate { s3: changed },
                    StorageMode::Slate
                ),
                Err(ApiError::Conflict(_))
            ));
        }
        assert!(
            ensure_same_storage_location(&previous, &StorageConfig::Local, StorageMode::Slate)
                .is_err()
        );
        assert!(ensure_same_storage_location(&previous, &previous, StorageMode::Local).is_err());
    }
}
