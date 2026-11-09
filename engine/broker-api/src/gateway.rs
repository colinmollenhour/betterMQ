//! Stateless ingest gateway: authenticate, split by shard leader, forward concurrently.

use crate::batch::{BatchEnqueueRequest, BatchEnqueueResponse};
use crate::ingest::{IngestBatch, MAX_BATCH_BYTES, MAX_HT_BATCH_MESSAGES};
use crate::routes::ApiError;
use crate::AppState;
use axum::{
    body::Bytes,
    extract::{Request, State},
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use broker_partition::{PublishRequest, ScheduledInfo};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub(crate) struct GatewayQueueTopicsRequest {
    tenant_id: Option<Uuid>,
    queue_ids: Vec<Uuid>,
}

pub(crate) async fn internal_gateway_queue_topics(
    State(state): State<Arc<AppState>>,
    Json(request): Json<GatewayQueueTopicsRequest>,
) -> Result<Json<HashMap<Uuid, String>>, ApiError> {
    if request.queue_ids.len() > MAX_HT_BATCH_MESSAGES {
        return Err(ApiError::BadRequest(
            "too many queue routing lookups".into(),
        ));
    }
    let resolve = async {
        let mut topics = HashMap::new();
        for id in request.queue_ids {
            let queue = state.broker.get_queue_by_id(id)?.ok_or_else(|| {
                ApiError::Broker(broker_partition::BrokerError::QueueNotFound(id.to_string()))
            })?;
            topics.insert(id, queue.topic);
        }
        Ok(Json(topics))
    };
    match request.tenant_id {
        Some(tenant_id) => broker_partition::scope_tenant(tenant_id.to_string(), resolve).await,
        None => resolve.await,
    }
}

fn routing_queue_id(request: &PublishRequest) -> Option<Uuid> {
    if let Some(destination) = &request.destination {
        destination.queue_id
    } else if request.url.is_some() && request.secret.is_some() {
        None
    } else {
        request.queue_id
    }
}

#[derive(Clone)]
pub struct GatewayOnlyState {
    seeds: Arc<Vec<String>>,
    route_overrides: Arc<RwLock<HashMap<u32, CachedRoute>>>,
    shard_count: u32,
    layout_version: u32,
    tenant: Arc<String>,
    local_token: Option<Arc<String>>,
    #[cfg(feature = "cloud")]
    auth: Option<broker_control_plane::ApiKeyValidator>,
    #[cfg(feature = "cloud")]
    control_plane: Option<broker_control_plane::ControlPlanePool>,
}

const ROUTE_TTL: Duration = Duration::from_secs(30);

fn gateway_layout(shards: Option<&str>, version: Option<&str>) -> Result<(u32, u32), String> {
    let version = version
        .map(str::parse::<u32>)
        .transpose()
        .map_err(|_| "BETTERMQ_GATEWAY_LAYOUT_VERSION must be 1 or 2")?
        .unwrap_or(broker_partition::LAYOUT_V1);
    if !matches!(
        version,
        broker_partition::LAYOUT_V1 | broker_partition::LAYOUT_V2
    ) {
        return Err("BETTERMQ_GATEWAY_LAYOUT_VERSION must be 1 or 2".into());
    }
    let shards = shards
        .map(str::parse::<u32>)
        .transpose()
        .map_err(|_| "BETTERMQ_GATEWAY_SHARD_COUNT must be an integer in 1..=65536")?
        .unwrap_or(if version == broker_partition::LAYOUT_V2 {
            broker_partition::DEFAULT_V2_SHARDS
        } else {
            broker_partition::DEFAULT_PARTITIONS
        });
    if !(1..=65_536).contains(&shards) {
        return Err("BETTERMQ_GATEWAY_SHARD_COUNT must be in 1..=65536".into());
    }
    Ok((shards, version))
}

#[derive(Clone)]
struct CachedRoute {
    leader: String,
    generation: u64,
    learned_at: Instant,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct GatewayRoutes {
    routes: Vec<(u32, String, u64)>,
}

pub(crate) async fn internal_gateway_routes(
    State(state): State<Arc<AppState>>,
) -> Json<GatewayRoutes> {
    let routes = state
        .cluster
        .as_ref()
        .map(|cluster| {
            (0..state.broker.shard_count())
                .filter_map(|shard| {
                    cluster
                        .runtime
                        .leader_http_base_for_shard(shard)
                        .map(|leader| (shard, leader, cluster.runtime.shard_generation(shard)))
                })
                .collect()
        })
        .unwrap_or_default();
    Json(GatewayRoutes { routes })
}

impl GatewayOnlyState {
    pub fn from_env() -> Result<Self, String> {
        let seeds: Vec<_> = std::env::var("BETTERMQ_BROKER_URLS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.trim_end_matches('/').to_string())
            .collect();
        if seeds.is_empty() {
            return Err("BETTERMQ_BROKER_URLS must contain at least one broker URL".into());
        }
        let (shard_count, layout_version) = gateway_layout(
            std::env::var("BETTERMQ_GATEWAY_SHARD_COUNT")
                .ok()
                .as_deref(),
            std::env::var("BETTERMQ_GATEWAY_LAYOUT_VERSION")
                .ok()
                .as_deref(),
        )?;
        let tenant = std::env::var("BETTERMQ_GATEWAY_TENANT").unwrap_or_else(|_| "default".into());
        let local_token = std::env::var("BETTERMQ_GATEWAY_TOKEN")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(Arc::new);
        Ok(Self {
            seeds: Arc::new(seeds),
            route_overrides: Arc::new(RwLock::new(HashMap::new())),
            shard_count,
            layout_version,
            tenant: Arc::new(tenant),
            local_token,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        })
    }

    #[cfg(feature = "cloud")]
    pub fn with_cloud_auth(
        mut self,
        auth: broker_control_plane::ApiKeyValidator,
        control_plane: broker_control_plane::ControlPlanePool,
    ) -> Self {
        self.auth = Some(auth);
        self.control_plane = Some(control_plane);
        self
    }

    fn uses_cloud_auth(&self) -> bool {
        #[cfg(feature = "cloud")]
        {
            return self.auth.is_some();
        }
        #[cfg(not(feature = "cloud"))]
        {
            false
        }
    }

    fn assign_shard(&self, tenant: &str, topic: &str, routing_key: &str) -> u32 {
        if self.layout_version >= broker_partition::LAYOUT_V2 {
            broker_proto::stable_physical_shard(tenant, routing_key, self.shard_count)
        } else {
            broker_proto::stable_partition(tenant, topic, routing_key, self.shard_count)
        }
    }

    async fn route_for(&self, shard: u32) -> (String, u64) {
        let mut routes = self.route_overrides.write().await;
        if let Some(route) = routes.get(&shard) {
            if route.learned_at.elapsed() < ROUTE_TTL {
                return (route.leader.clone(), route.generation);
            }
        }
        routes.remove(&shard);
        (self.seeds[shard as usize % self.seeds.len()].clone(), 0)
    }

    async fn remember_route(&self, shard: u32, leader: String, generation: u64) {
        self.route_overrides
            .write()
            .await
            .entry(shard)
            .and_modify(|route| {
                if generation >= route.generation || route.learned_at.elapsed() >= ROUTE_TTL {
                    *route = CachedRoute {
                        leader: leader.clone(),
                        generation,
                        learned_at: Instant::now(),
                    };
                }
            })
            .or_insert(CachedRoute {
                leader,
                generation,
                learned_at: Instant::now(),
            });
    }

    async fn forget_route(&self, shard: u32, failed: &str) {
        let mut routes = self.route_overrides.write().await;
        if routes
            .get(&shard)
            .is_some_and(|route| route.leader == failed)
        {
            routes.remove(&shard);
        }
    }

    async fn refresh_routes(&self) {
        for seed in self.seeds.iter() {
            let response = crate::cluster_auth::apply_cluster_secret(
                gateway_http_client()
                    .get(format!("{seed}/internal/v1/gateway/routes"))
                    .timeout(Duration::from_secs(2)),
            )
            .send()
            .await;
            let Ok(response) = response else { continue };
            if !response.status().is_success() {
                continue;
            }
            let Ok(routes) = response.json::<GatewayRoutes>().await else {
                continue;
            };
            for (shard, leader, generation) in routes.routes {
                if shard < self.shard_count {
                    self.remember_route(shard, leader, generation).await;
                }
            }
            break;
        }
    }

    pub(crate) fn spawn_route_refresh(self: &Arc<Self>) {
        let state = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(ROUTE_TTL);
            loop {
                interval.tick().await;
                if crate::admission::is_shutting_down() {
                    break;
                }
                state.refresh_routes().await;
            }
        });
    }

    async fn healthy_brokers(&self) -> usize {
        let probes = self.seeds.iter().map(|seed| {
            // Probe serving readiness using the same listener and credentials as forwarding.
            let url = format!("{}/internal/v1/readyz", seed.trim_end_matches('/'));
            async move {
                crate::cluster_auth::apply_cluster_secret(
                    gateway_http_client()
                        .get(url)
                        .timeout(Duration::from_secs(2)),
                )
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            }
        });
        futures_util::future::join_all(probes)
            .await
            .into_iter()
            .filter(|healthy| *healthy)
            .count()
    }

    fn auth_configured(&self) -> bool {
        self.uses_cloud_auth() || self.local_token.is_some() || insecure_no_auth()
    }

    async fn canonicalize_topics(
        &self,
        batch: &mut IngestBatch,
        tenant_id: Option<Uuid>,
    ) -> Result<(), ApiError> {
        let queue_ids: HashSet<_> = batch
            .records
            .iter()
            .filter_map(|record| routing_queue_id(&record.req))
            .collect();
        let mut topics = HashMap::new();
        if !queue_ids.is_empty() {
            let request = GatewayQueueTopicsRequest {
                tenant_id,
                queue_ids: queue_ids.into_iter().collect(),
            };
            let mut resolved = false;
            let mut missing_queue = None;
            let mut failure = "cannot resolve queue routing topics from internal broker seeds";
            for seed in self.seeds.iter() {
                let response = crate::cluster_auth::apply_cluster_secret(
                    gateway_http_client()
                        .post(format!("{seed}/internal/v1/gateway/queue-topics"))
                        .timeout(Duration::from_secs(2))
                        .json(&request),
                )
                .send()
                .await;
                let Ok(response) = response else { continue };
                if !response.status().is_success() {
                    match response.status() {
                        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                            failure = "queue routing lookup rejected: cluster secret missing or mismatched";
                        }
                        StatusCode::NOT_FOUND => {
                            failure = "queue routing lookup route not found: BROKER_URLS must use internal listeners";
                            if let Ok(body) = response.json::<serde_json::Value>().await {
                                if body["code"] == "not_found" {
                                    missing_queue = body["error"].as_str().map(str::to_string);
                                }
                            }
                        }
                        _ => {}
                    }
                    continue;
                }
                if let Ok(map) = response.json::<HashMap<Uuid, String>>().await {
                    topics = map;
                    resolved = true;
                    break;
                }
            }
            if !resolved {
                return Err(missing_queue
                    .map_or_else(|| ApiError::Unavailable(failure.into()), ApiError::NotFound));
            }
        }
        for record in &mut batch.records {
            let request = &mut record.req;
            if broker_partition::is_dlq_topic(&request.topic) {
                continue;
            }
            if let Some(id) = routing_queue_id(request) {
                request.topic = topics.get(&id).cloned().ok_or_else(|| {
                    ApiError::Broker(broker_partition::BrokerError::QueueNotFound(id.to_string()))
                })?;
            } else if request.destination.is_some()
                || (request.url.is_some() && request.secret.is_some())
            {
                request.topic = broker_partition::DIRECT_TOPIC.into();
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct GatewayForwardItem {
    index: usize,
    request: PublishRequest,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct GatewayForwardRequest {
    shard: u32,
    leader_generation: u64,
    #[serde(default)]
    tenant_id: Option<Uuid>,
    items: Vec<GatewayForwardItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GatewayForwardResult {
    index: usize,
    accepted: bool,
    message_id: Option<Uuid>,
    topic: String,
    partition: Option<u32>,
    offset: Option<u64>,
    duplicate: bool,
    commit_epoch: Option<u64>,
    scheduled: Option<ScheduledInfo>,
    error: Option<String>,
    #[serde(flatten)]
    failure: Option<crate::ingest::IngestFailure>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GatewayForwardResponse {
    stale_leader: bool,
    leader: Option<String>,
    leader_generation: u64,
    results: Vec<GatewayForwardResult>,
}

fn gateway_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        let idle_per_host = std::env::var("BETTERMQ_GATEWAY_IDLE_CONNECTIONS_PER_HOST")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(16);
        reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(idle_per_host)
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("gateway pooled HTTP client")
    })
}

fn failed_results(
    items: &[GatewayForwardItem],
    error: impl Into<String>,
) -> Vec<GatewayForwardResult> {
    let error = error.into();
    items
        .iter()
        .map(|item| GatewayForwardResult {
            index: item.index,
            accepted: false,
            message_id: None,
            topic: item.request.topic.clone(),
            partition: None,
            offset: None,
            duplicate: false,
            commit_epoch: None,
            scheduled: None,
            error: Some(error.clone()),
            failure: Some(ApiError::Unavailable(error.clone()).failure()),
        })
        .collect()
}

fn failed_metadata(
    items: &[(usize, String)],
    error: impl Into<String>,
) -> Vec<GatewayForwardResult> {
    let error = error.into();
    items
        .iter()
        .map(|(index, topic)| GatewayForwardResult {
            index: *index,
            accepted: false,
            message_id: None,
            topic: topic.clone(),
            partition: None,
            offset: None,
            duplicate: false,
            commit_epoch: None,
            scheduled: None,
            error: Some(error.clone()),
            failure: Some(ApiError::Unavailable(error.clone()).failure()),
        })
        .collect()
}

async fn forward_shard(
    state: Arc<AppState>,
    shard: u32,
    items: Vec<GatewayForwardItem>,
    tenant_id: Option<Uuid>,
) -> Vec<GatewayForwardResult> {
    let Some(cluster) = state.cluster.as_ref() else {
        return failed_results(&items, "cluster mode not enabled");
    };
    let mut target = cluster.runtime.leader_http_base_for_shard(shard);
    let mut request = GatewayForwardRequest {
        shard,
        leader_generation: cluster.runtime.shard_generation(shard),
        tenant_id,
        items,
    };

    for attempt in 0..=1 {
        let Some(base) = target.clone() else {
            return failed_results(&request.items, format!("no leader for shard {shard}"));
        };
        let url = format!("{}/internal/v1/gateway/ingest", base.trim_end_matches('/'));
        let response = crate::cluster_auth::apply_cluster_secret(
            gateway_http_client().post(url).json(&request),
        )
        .send()
        .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                return failed_results(
                    &request.items,
                    format!("gateway forward failed for shard {shard}: {error}"),
                );
            }
        };
        let status = response.status();
        let decoded = response.json::<GatewayForwardResponse>().await;
        let decoded = match decoded {
            Ok(decoded) => decoded,
            Err(error) => {
                return failed_results(
                    &request.items,
                    format!("gateway leader response decode failed for shard {shard}: {error}"),
                );
            }
        };
        if status == StatusCode::CONFLICT && decoded.stale_leader && attempt == 0 {
            target = decoded
                .leader
                .or_else(|| cluster.runtime.leader_http_base_for_shard(shard));
            request.leader_generation = decoded
                .leader_generation
                .max(cluster.runtime.shard_generation(shard));
            continue;
        }
        if !status.is_success() {
            if !decoded.results.is_empty() {
                return decoded.results;
            }
            let detail = decoded
                .results
                .first()
                .and_then(|result| result.error.clone())
                .unwrap_or_else(|| format!("leader returned {status}"));
            return failed_results(&request.items, detail);
        }
        return decoded.results;
    }

    failed_results(
        &request.items,
        format!("stale leader retry exhausted for shard {shard}"),
    )
}

async fn forward_gateway_only_shard(
    state: Arc<GatewayOnlyState>,
    shard: u32,
    mut items: Vec<GatewayForwardItem>,
    tenant_id: Option<Uuid>,
) -> Vec<GatewayForwardResult> {
    let (mut target, mut generation) = state.route_for(shard).await;
    // A transport failure can have an unknown outcome. Protect internal retries
    // even when a producer did not provide an idempotency key.
    for item in &mut items {
        if item.request.idempotency_key.is_none() {
            item.request.idempotency_key = Some(format!("gateway:{}", Uuid::new_v4()));
        }
    }
    let mut request = GatewayForwardRequest {
        shard,
        leader_generation: generation,
        tenant_id,
        items,
    };
    let mut attempted = HashSet::new();
    // Cached leader -> alternate seed -> redirected leader, at most.
    for attempt in 0..=2 {
        attempted.insert(target.clone());
        let url = format!(
            "{}/internal/v1/gateway/ingest",
            target.trim_end_matches('/')
        );
        let response = crate::cluster_auth::apply_cluster_secret(
            gateway_http_client().post(url).json(&request),
        )
        .send()
        .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                state.forget_route(shard, &target).await;
                if attempt < 2 {
                    if let Some(seed) = state.seeds.iter().find(|seed| !attempted.contains(*seed)) {
                        target = seed.clone();
                        request.leader_generation = 0;
                        continue;
                    }
                }
                return failed_results(
                    &request.items,
                    format!("gateway forward failed for shard {shard}: {error}"),
                );
            }
        };
        let status = response.status();
        let decoded = match response.json::<GatewayForwardResponse>().await {
            Ok(decoded) => decoded,
            Err(error) => {
                return failed_results(
                    &request.items,
                    format!("gateway leader response decode failed for shard {shard}: {error}"),
                );
            }
        };
        if status == StatusCode::CONFLICT && decoded.stale_leader && attempt < 2 {
            if let Some(leader) = decoded.leader {
                target = leader;
            }
            generation = decoded.leader_generation;
            request.leader_generation = generation;
            state
                .remember_route(shard, target.clone(), generation)
                .await;
            continue;
        }
        if !status.is_success() {
            if !decoded.results.is_empty() {
                return decoded.results;
            }
            let detail = decoded
                .results
                .first()
                .and_then(|result| result.error.clone())
                .unwrap_or_else(|| format!("leader returned {status}"));
            return failed_results(&request.items, detail);
        }
        state
            .remember_route(shard, target.clone(), decoded.leader_generation)
            .await;
        return decoded.results;
    }
    failed_results(
        &request.items,
        format!("stale leader retry exhausted for shard {shard}"),
    )
}

fn aggregate_results(
    mut outcomes: Vec<GatewayForwardResult>,
    input_metadata: Vec<(usize, String)>,
) -> (StatusCode, Json<BatchEnqueueResponse>) {
    let seen: HashSet<_> = outcomes.iter().map(|outcome| outcome.index).collect();
    outcomes.extend(failed_metadata(
        &input_metadata
            .into_iter()
            .filter(|(index, _)| !seen.contains(index))
            .collect::<Vec<_>>(),
        "gateway shard forwarding task failed",
    ));
    let mut ids = Vec::new();
    let mut results = Vec::with_capacity(outcomes.len());
    let mut failed = 0usize;
    for outcome in outcomes {
        if outcome.accepted {
            if let Some(id) = outcome.message_id {
                ids.push(id);
            }
        } else {
            failed += 1;
        }
        results.push(crate::ingest::IngestItemResult {
            index: outcome.index,
            accepted: outcome.accepted,
            message_id: outcome.message_id,
            topic: outcome.topic,
            partition: outcome.partition,
            offset: outcome.offset,
            duplicate: outcome.duplicate,
            commit_epoch: outcome.commit_epoch,
            scheduled: outcome.scheduled,
            error: outcome.error,
            failure: outcome.failure.or_else(|| {
                (!outcome.accepted)
                    .then(|| ApiError::Unavailable("legacy gateway error".into()).failure())
            }),
        });
    }
    results.sort_by_key(|result| result.index);
    let accepted = results.len().saturating_sub(failed);
    (
        if failed > 0 {
            StatusCode::MULTI_STATUS
        } else {
            StatusCode::ACCEPTED
        },
        Json(BatchEnqueueResponse {
            accepted,
            failed,
            partial: accepted > 0 && failed > 0,
            message_ids: ids,
            results,
        }),
    )
}

async fn gateway_only_submit(
    state: Arc<GatewayOnlyState>,
    ingest: Option<crate::metering::IngestAuth>,
    body: BatchEnqueueRequest,
) -> Result<(StatusCode, Json<BatchEnqueueResponse>), ApiError> {
    if body.messages.len() > MAX_HT_BATCH_MESSAGES {
        return Err(ApiError::BadRequest(format!(
            "batch exceeds max size of {MAX_HT_BATCH_MESSAGES}"
        )));
    }
    let mut batch = IngestBatch::from_requests(body.messages)?;
    let tenant_name = ingest
        .map(|auth| auth.tenant_id.to_string())
        .unwrap_or_else(|| state.tenant.as_ref().clone());
    let tenant_id = ingest.map(|auth| auth.tenant_id);
    let mut guards = Vec::with_capacity(batch.records.len());
    for envelope in &batch.records {
        guards.push(crate::admission::check_gateway_admission(
            tenant_id,
            envelope.body_bytes,
        )?);
    }
    state.canonicalize_topics(&mut batch, tenant_id).await?;
    let mut groups: HashMap<u32, Vec<GatewayForwardItem>> = HashMap::new();
    let mut input_metadata = Vec::with_capacity(batch.records.len());
    for (index, envelope) in batch.records.into_iter().enumerate() {
        let shard = state.assign_shard(
            &tenant_name,
            &envelope.req.topic,
            &envelope.req.shard_routing_key(),
        );
        input_metadata.push((index, envelope.req.topic.clone()));
        groups.entry(shard).or_default().push(GatewayForwardItem {
            index,
            request: envelope.req,
        });
    }
    let mut tasks = JoinSet::new();
    for (shard, items) in groups {
        tasks.spawn(forward_gateway_only_shard(
            state.clone(),
            shard,
            items,
            tenant_id,
        ));
    }
    let mut outcomes = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(results) => outcomes.extend(results),
            Err(error) => tracing::warn!(%error, "gateway-only forwarding task failed"),
        }
    }
    drop(guards);
    Ok(aggregate_results(outcomes, input_metadata))
}

pub async fn gateway_only_enqueue(
    State(state): State<Arc<GatewayOnlyState>>,
    ingest: Option<axum::extract::Extension<crate::metering::IngestAuth>>,
    body: Bytes,
) -> Result<(StatusCode, Json<BatchEnqueueResponse>), ApiError> {
    if body.len() > MAX_BATCH_BYTES {
        return Err(ApiError::BadRequest(format!(
            "gateway batch exceeds max bytes of {MAX_BATCH_BYTES}"
        )));
    }
    let body: BatchEnqueueRequest = serde_json::from_slice(&body)
        .map_err(|error| ApiError::BadRequest(format!("invalid gateway batch: {error}")))?;
    gateway_only_submit(state, ingest.map(|value| value.0), body).await
}

pub async fn gateway_only_ndjson(
    State(state): State<Arc<GatewayOnlyState>>,
    ingest: Option<axum::extract::Extension<crate::metering::IngestAuth>>,
    request: Request,
) -> Result<(StatusCode, Json<BatchEnqueueResponse>), ApiError> {
    crate::batch::validate_ndjson_content_encoding(request.headers())?;
    let body = crate::batch::parse_ndjson_stream(request.into_body()).await?;
    gateway_only_submit(state, ingest.map(|value| value.0), body).await
}

pub async fn gateway_only_auth(
    State(state): State<Arc<GatewayOnlyState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    #[cfg(feature = "cloud")]
    let mut request = request;
    #[cfg(feature = "cloud")]
    if let Some(validator) = &state.auth {
        use broker_control_plane::AuthError;
        let token = bearer_token(&request)
            .ok_or(AuthError::Missing)
            .map_err(|error| ApiError::Unauthorized(error.to_string()))?;
        let context = validator
            .validate_bearer(token)
            .await
            .map_err(|error| ApiError::Unauthorized(error.to_string()))?;
        request
            .extensions_mut()
            .insert(crate::metering::IngestAuth {
                tenant_id: context.tenant_id,
            });
        request.extensions_mut().insert(context);
        return Ok(next.run(request).await);
    }
    if let Some(expected) = state.local_token.as_deref() {
        let presented = bearer_token(&request).unwrap_or_default();
        use subtle::ConstantTimeEq;
        if presented.len() != expected.len()
            || !bool::from(presented.as_bytes().ct_eq(expected.as_bytes()))
        {
            return Err(ApiError::Unauthorized("invalid gateway token".into()));
        }
        return Ok(next.run(request).await);
    }
    if insecure_no_auth() {
        return Ok(next.run(request).await);
    }
    Err(ApiError::Unauthorized(
        "gateway authentication required; set BETTERMQ_GATEWAY_TOKEN".into(),
    ))
}

fn bearer_token(request: &Request) -> Option<&str> {
    request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

fn insecure_no_auth() -> bool {
    matches!(
        std::env::var("BETTERMQ_INSECURE_NO_AUTH")
            .ok()
            .as_deref()
            .map(str::trim),
        Some("1") | Some("true") | Some("TRUE") | Some("yes")
    )
}

#[derive(Serialize)]
pub struct GatewayOnlyStatus {
    mode: &'static str,
    ready: bool,
    healthy_brokers: usize,
    configured_brokers: usize,
    shard_count: u32,
    layout_version: u32,
    draining: bool,
}

pub async fn gateway_only_ready(
    State(state): State<Arc<GatewayOnlyState>>,
) -> (StatusCode, Json<GatewayOnlyStatus>) {
    let healthy = state.healthy_brokers().await;
    let draining = crate::admission::is_shutting_down();
    let ready = state.auth_configured() && healthy > 0 && !draining;
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(GatewayOnlyStatus {
            mode: "gateway-only",
            ready,
            healthy_brokers: healthy,
            configured_brokers: state.seeds.len(),
            shard_count: state.shard_count,
            layout_version: state.layout_version,
            draining,
        }),
    )
}

pub async fn gateway_only_status(
    State(state): State<Arc<GatewayOnlyState>>,
) -> Json<GatewayOnlyStatus> {
    let healthy = state.healthy_brokers().await;
    Json(GatewayOnlyStatus {
        mode: "gateway-only",
        ready: state.auth_configured() && healthy > 0 && !crate::admission::is_shutting_down(),
        healthy_brokers: healthy,
        configured_brokers: state.seeds.len(),
        shard_count: state.shard_count,
        layout_version: state.layout_version,
        draining: crate::admission::is_shutting_down(),
    })
}

/// Edge gateway: split by physical shard and forward sub-batches to leaders.
pub async fn gateway_enqueue(
    State(state): State<Arc<AppState>>,
    ingest: Option<axum::extract::Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<
        axum::extract::Extension<broker_control_plane::PlanLimits>,
    >,
    Json(body): Json<BatchEnqueueRequest>,
) -> Result<(StatusCode, Json<BatchEnqueueResponse>), ApiError> {
    if body.messages.len() > MAX_HT_BATCH_MESSAGES {
        return Err(ApiError::BadRequest(format!(
            "batch exceeds max size of {MAX_HT_BATCH_MESSAGES}"
        )));
    }

    if state.cluster.is_none() {
        #[cfg(feature = "cloud")]
        return crate::batch::batch_enqueue(State(state), ingest, plan, Json(body)).await;
        #[cfg(not(feature = "cloud"))]
        return crate::batch::batch_enqueue(State(state), ingest, Json(body)).await;
    }

    #[cfg(feature = "cloud")]
    if state.uses_cloud_auth() {
        if let (Some(auth), Some(plan)) = (
            ingest.as_ref().map(|value| value.0),
            plan.as_ref().map(|value| &value.0),
        ) {
            crate::metering::check_cloud_batch_messages_cap(
                &state,
                auth.tenant_id,
                plan,
                body.messages.len(),
            )
            .await?;
            if let Some(request) = body
                .messages
                .iter()
                .find(|request| request.payload.len() as u64 > plan.max_message_bytes)
            {
                return Err(ApiError::BadRequest(format!(
                    "message of {} bytes exceeds plan limit of {} bytes",
                    request.payload.len(),
                    plan.max_message_bytes
                )));
            }
        }
    }

    let batch = IngestBatch::from_requests(body.messages)?;

    let mut by_shard: HashMap<u32, Vec<GatewayForwardItem>> = HashMap::new();
    let mut input_metadata = Vec::with_capacity(batch.records.len());
    for (index, mut env) in batch.records.into_iter().enumerate() {
        state.broker.canonicalize_routing(&mut env.req)?;
        let shard = state
            .broker
            .assign_shard(&env.req.topic, &env.req.shard_routing_key());
        input_metadata.push((index, env.req.topic.clone()));
        by_shard.entry(shard).or_default().push(GatewayForwardItem {
            index,
            request: env.req,
        });
    }

    let tenant_id = ingest.as_ref().map(|value| value.0.tenant_id);
    let mut joins = JoinSet::new();
    for (shard, items) in by_shard {
        joins.spawn(forward_shard(state.clone(), shard, items, tenant_id));
    }

    let mut outcomes = Vec::new();
    while let Some(joined) = joins.join_next().await {
        match joined {
            Ok(chunk) => outcomes.extend(chunk),
            Err(error) => tracing::warn!(%error, "gateway shard forwarding task failed"),
        }
    }
    let seen: HashSet<_> = outcomes.iter().map(|outcome| outcome.index).collect();
    outcomes.extend(failed_metadata(
        &input_metadata
            .into_iter()
            .filter(|(index, _)| !seen.contains(index))
            .collect::<Vec<_>>(),
        "gateway shard forwarding task failed",
    ));

    let mut ids = Vec::new();
    let mut results = Vec::new();
    let mut failed = 0usize;
    for outcome in outcomes {
        if outcome.accepted {
            if let Some(id) = outcome.message_id {
                ids.push(id);
            }
        } else {
            failed += 1;
        }
        results.push(crate::ingest::IngestItemResult {
            index: outcome.index,
            accepted: outcome.accepted,
            message_id: outcome.message_id,
            topic: outcome.topic,
            partition: outcome.partition,
            offset: outcome.offset,
            duplicate: outcome.duplicate,
            commit_epoch: outcome.commit_epoch,
            scheduled: outcome.scheduled,
            error: outcome.error,
            failure: outcome.failure.or_else(|| {
                (!outcome.accepted)
                    .then(|| ApiError::Unavailable("legacy gateway error".into()).failure())
            }),
        });
    }
    results.sort_by_key(|r| r.index);
    let accepted = results.len().saturating_sub(failed);
    let partial = accepted > 0 && failed > 0;
    Ok((
        if failed > 0 {
            StatusCode::MULTI_STATUS
        } else {
            StatusCode::ACCEPTED
        },
        Json(BatchEnqueueResponse {
            accepted,
            failed,
            partial,
            message_ids: ids,
            results,
        }),
    ))
}

/// Leader-side storage endpoint for a storage-free gateway. The public gateway
/// performs authentication and limits; this endpoint repeats canonical
/// validation/admission/delay handling before committing.
pub(crate) async fn internal_gateway_ingest(
    State(state): State<Arc<AppState>>,
    Json(body): Json<GatewayForwardRequest>,
) -> Response {
    let Some(cluster) = state.cluster.as_ref() else {
        return (
            StatusCode::CONFLICT,
            Json(GatewayForwardResponse {
                stale_leader: true,
                leader: None,
                leader_generation: 0,
                results: failed_results(&body.items, "cluster mode not enabled"),
            }),
        )
            .into_response();
    };
    let generation = cluster.runtime.shard_generation(body.shard);
    if body.leader_generation != generation || !cluster.runtime.is_leader_for_shard(body.shard) {
        return (
            StatusCode::CONFLICT,
            Json(GatewayForwardResponse {
                stale_leader: true,
                leader: cluster.runtime.leader_http_base_for_shard(body.shard),
                leader_generation: generation,
                results: Vec::new(),
            }),
        )
            .into_response();
    }
    if body.items.is_empty() || body.items.len() > MAX_HT_BATCH_MESSAGES {
        return (
            StatusCode::BAD_REQUEST,
            Json(GatewayForwardResponse {
                stale_leader: false,
                leader: None,
                leader_generation: generation,
                results: failed_results(&body.items, "invalid gateway batch size"),
            }),
        )
            .into_response();
    }
    if body.items.iter().any(|item| {
        state
            .broker
            .assign_shard(&item.request.topic, &item.request.shard_routing_key())
            != body.shard
    }) {
        return (
            StatusCode::BAD_REQUEST,
            Json(GatewayForwardResponse {
                stale_leader: false,
                leader: None,
                leader_generation: generation,
                results: failed_results(&body.items, "gateway batch contains the wrong shard"),
            }),
        )
            .into_response();
    }

    let metadata: Vec<_> = body
        .items
        .iter()
        .map(|item| (item.index, item.request.topic.clone()))
        .collect();
    let original_indexes: Vec<_> = metadata.iter().map(|(index, _)| *index).collect();
    let requests = body.items.into_iter().map(|item| item.request).collect();
    let batch = match IngestBatch::from_requests(requests) {
        Ok(batch) => batch,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(GatewayForwardResponse {
                    stale_leader: false,
                    leader: None,
                    leader_generation: generation,
                    results: failed_metadata(&metadata, error.to_string())
                        .into_iter()
                        .map(|mut result| {
                            result.failure = Some(error.failure());
                            result
                        })
                        .collect(),
                }),
            )
                .into_response();
        }
    };
    let ingest = body
        .tenant_id
        .map(|tenant_id| crate::metering::IngestAuth { tenant_id });
    let outcomes = crate::ingest::submit_batch(&state, ingest, batch).await;
    let mut results = Vec::with_capacity(outcomes.len());
    for outcome in outcomes {
        let index = original_indexes
            .get(outcome.index)
            .copied()
            .unwrap_or(outcome.index);
        match outcome.result {
            Ok(response) => {
                if response.scheduled.is_none() && !response.duplicate {
                    crate::ingest::notify_dispatch(&state, &response);
                    crate::metrics::record_accepted();
                } else if response.duplicate {
                    crate::metrics::record_duplicate();
                }
                results.push(GatewayForwardResult {
                    index,
                    accepted: true,
                    message_id: response.message_id,
                    topic: response.topic,
                    partition: response.partition,
                    offset: response.offset,
                    duplicate: response.duplicate,
                    commit_epoch: response.commit_epoch,
                    scheduled: response.scheduled,
                    error: None,
                    failure: None,
                });
            }
            Err(error) => {
                crate::metrics::record_rejected();
                results.push(GatewayForwardResult {
                    index,
                    accepted: false,
                    message_id: None,
                    topic: outcome.topic,
                    partition: None,
                    offset: None,
                    duplicate: false,
                    commit_epoch: None,
                    scheduled: None,
                    failure: Some(error.failure()),
                    error: Some(error.to_string()),
                });
            }
        }
    }
    results.sort_by_key(|result| result.index);
    (
        StatusCode::OK,
        Json(GatewayForwardResponse {
            stale_leader: false,
            leader: None,
            leader_generation: generation,
            results,
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_dispatch::{DispatchConfig, DispatchEngine, LeaseTable};
    use broker_partition::{Broker, BrokerConfig, PublishRequest};
    use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
    use broker_schedule::{CronRegistry, ScheduleQueue};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn gateway_defaults_match_new_brokers_and_reject_invalid_layouts() {
        assert_eq!(
            gateway_layout(None, None).unwrap(),
            (
                broker_partition::DEFAULT_PARTITIONS,
                broker_partition::LAYOUT_V1
            )
        );
        assert_eq!(
            gateway_layout(None, Some("2")).unwrap(),
            (
                broker_partition::DEFAULT_V2_SHARDS,
                broker_partition::LAYOUT_V2
            )
        );
        assert_eq!(gateway_layout(Some("8"), Some("1")).unwrap(), (8, 1));
        for (shards, version) in [
            (Some("0"), None),
            (Some("65537"), None),
            (Some("bad"), None),
            (None, Some("3")),
            (None, Some("bad")),
        ] {
            assert!(gateway_layout(shards, version).is_err());
        }
    }

    fn test_state(dir: &tempfile::TempDir) -> Arc<AppState> {
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        Arc::new(AppState {
            broker: broker.clone(),
            schedule: ScheduleQueue::open(dir.path()).unwrap(),
            crons: CronRegistry::open(dir.path()).unwrap(),
            dispatch: DispatchEngine::new_broker_only(broker, DispatchConfig::default()),
            leases: LeaseTable::new(),
            cluster: None,
            local_auth: None,
            fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
            catalog_tombstones: crate::catalog_tombstones::CatalogTombstones::open(dir.path())
                .unwrap(),
            dispatch_fleet: false,
            broker_only: false,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        })
    }

    #[tokio::test]
    async fn gateway_preserves_delay_through_canonical_ingest() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(&dir);
        let schedule = state.schedule.clone();
        let request = PublishRequest {
            routing_id: None,
            topic: String::new(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "delayed".into(),
            payload: "body".into(),
            payload_encoding: None,
            idempotency_key: Some("gateway-delay".into()),
            delay_ms: Some(5_000),
            priority: None,
            flow_id: None,
            url: Some("https://example.com/hook".into()),
            secret: Some("secret".into()),
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        };
        let (status, Json(response)) = gateway_enqueue(
            State(state),
            None,
            Json(BatchEnqueueRequest {
                messages: vec![request],
            }),
        )
        .await
        .unwrap();

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(response.accepted, 1);
        assert!(response.results[0].scheduled.is_some());
        assert_eq!(schedule.list().len(), 1);
    }

    #[test]
    fn gateway_client_is_process_wide_and_pooled() {
        assert!(std::ptr::eq(gateway_http_client(), gateway_http_client()));
    }

    #[test]
    fn failed_aggregation_preserves_original_indexes() {
        let failed = failed_metadata(
            &[(9, "nine".into()), (2, "two".into())],
            "leader unavailable",
        );
        assert_eq!(failed[0].index, 9);
        assert_eq!(failed[1].index, 2);
        assert!(failed.iter().all(|result| !result.accepted));
    }

    #[tokio::test]
    async fn internal_gateway_requires_cluster_leader() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(&dir);
        let response = internal_gateway_ingest(
            State(state),
            Json(GatewayForwardRequest {
                shard: 0,
                leader_generation: 1,
                tenant_id: None,
                items: vec![],
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn gateway_retries_stale_leader_once_and_preserves_index() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let retry_base = base.clone();
        let retry_attempts = attempts.clone();
        let app = axum::Router::new().route(
            "/internal/v1/gateway/ingest",
            axum::routing::post(move |Json(request): Json<GatewayForwardRequest>| {
                let retry_base = retry_base.clone();
                let retry_attempts = retry_attempts.clone();
                async move {
                    if retry_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        return (
                            StatusCode::CONFLICT,
                            Json(GatewayForwardResponse {
                                stale_leader: true,
                                leader: Some(retry_base),
                                leader_generation: request.leader_generation + 1,
                                results: Vec::new(),
                            }),
                        );
                    }
                    let results = request
                        .items
                        .into_iter()
                        .map(|item| GatewayForwardResult {
                            index: item.index,
                            accepted: true,
                            message_id: Some(Uuid::new_v4()),
                            topic: item.request.topic,
                            partition: Some(request.shard),
                            offset: Some(1),
                            duplicate: false,
                            commit_epoch: Some(2),
                            scheduled: None,
                            error: None,
                            failure: None,
                        })
                        .collect();
                    (
                        StatusCode::OK,
                        Json(GatewayForwardResponse {
                            stale_leader: false,
                            leader: None,
                            leader_generation: request.leader_generation,
                            results,
                        }),
                    )
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let runtime = ClusterRuntime::from_config_only(ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![
                NodeConfig {
                    internal_url: None,
                    id: local_id,
                    addr: "http://127.0.0.1:1".into(),
                },
                NodeConfig {
                    internal_url: None,
                    id: peer_id,
                    addr: base.clone(),
                },
            ],
            node_id: local_id,
            generation: 1,
            hash_version: 1,
        });
        let now = chrono::Utc::now().timestamp_millis();
        runtime.record_self_alive(now);
        runtime.record_peer_alive(peer_id, now);
        let shard = (0..256)
            .find(|shard| {
                runtime.leader_http_base_for_shard(*shard).as_deref() == Some(base.as_str())
            })
            .expect("peer leads at least one shard");

        let dir = tempfile::tempdir().unwrap();
        let base_state = test_state(&dir);
        let mut state = (*base_state).clone();
        state.cluster = Some(crate::Cluster::new(runtime));
        let request = PublishRequest {
            routing_id: None,
            topic: "jobs".into(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "key".into(),
            payload: "body".into(),
            payload_encoding: None,
            idempotency_key: Some("gateway-retry".into()),
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: Some("https://example.com/hook".into()),
            secret: Some("secret".into()),
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        };
        let results = forward_shard(
            Arc::new(state),
            shard,
            vec![GatewayForwardItem { index: 7, request }],
            None,
        )
        .await;
        server.abort();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 7);
        assert!(results[0].accepted);
    }

    #[tokio::test]
    async fn gateway_only_evicts_dead_leader_and_preserves_retry_identity() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_base = format!("http://{}", dead.local_addr().unwrap());
        drop(dead);
        let app = axum::Router::new().route(
            "/internal/v1/gateway/ingest",
            axum::routing::post(|Json(request): Json<GatewayForwardRequest>| async move {
                assert_eq!(
                    request.items[0].request.idempotency_key.as_deref(),
                    Some("keep-me")
                );
                assert!(request.items[0].request.routing_id.is_some());
                Json(GatewayForwardResponse {
                    stale_leader: false,
                    leader: None,
                    leader_generation: 2,
                    results: request
                        .items
                        .iter()
                        .map(|item| GatewayForwardResult {
                            index: item.index,
                            accepted: true,
                            message_id: Some(Uuid::new_v4()),
                            topic: item.request.topic.clone(),
                            partition: Some(request.shard),
                            offset: Some(0),
                            duplicate: false,
                            commit_epoch: Some(1),
                            scheduled: None,
                            error: None,
                            failure: None,
                        })
                        .collect(),
                })
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let state = Arc::new(GatewayOnlyState {
            seeds: Arc::new(vec![base.clone()]),
            route_overrides: Arc::new(RwLock::new(HashMap::new())),
            shard_count: 4,
            layout_version: broker_partition::LAYOUT_V1,
            tenant: Arc::new("default".into()),
            local_token: Some(Arc::new("token".into())),
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        });
        state.remember_route(0, dead_base.clone(), 1).await;
        let mut request: PublishRequest = serde_json::from_value(serde_json::json!({
            "payload":"x", "idempotency_key":"keep-me"
        }))
        .unwrap();
        request.ensure_routing_id();
        let results = forward_gateway_only_shard(
            state.clone(),
            0,
            vec![GatewayForwardItem { index: 9, request }],
            None,
        )
        .await;
        assert!(results[0].accepted);
        assert_eq!(results[0].index, 9);
        assert_eq!(state.route_for(0).await, (base.clone(), 2));
        state.route_overrides.write().await.insert(
            0,
            CachedRoute {
                leader: dead_base,
                generation: 1,
                learned_at: Instant::now() - ROUTE_TTL,
            },
        );
        assert_eq!(state.route_for(0).await, (base, 0));
        server.abort();
    }

    #[tokio::test]
    async fn gateway_only_json_enforces_wire_size_before_parse() {
        let state = Arc::new(GatewayOnlyState {
            seeds: Arc::new(vec!["http://127.0.0.1:9".into()]),
            route_overrides: Arc::new(RwLock::new(HashMap::new())),
            shard_count: 256,
            layout_version: broker_partition::LAYOUT_V2,
            tenant: Arc::new("default".into()),
            local_token: Some(Arc::new("token".into())),
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        });
        assert!(matches!(
            gateway_only_enqueue(
                State(state),
                None,
                Bytes::from(vec![b'x'; MAX_BATCH_BYTES + 1]),
            )
            .await,
            Err(ApiError::BadRequest(message)) if message.contains("max bytes")
        ));
    }
}
