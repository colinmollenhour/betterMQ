use anyhow::Context;
use axum::{response::Redirect, routing::get};
use broker_api::{
    admin_router, begin_shutdown, data_plane_router_no_spawn, enqueue_dispatch_after_publish,
    gateway_only_router, internal_router, open_setup_window, public_router, publish_with_cluster,
    spawn_cluster_catalog_sync, AdminState, ApiError, AppState, CatalogTombstones, Cluster,
    GatewayOnlyState,
};
use broker_cli::{
    Cli, ClusterCommands, ClusterInitArgs, ClusterJoinArgs, Commands, ConfigCommands,
    ConfigInitArgs, ConfigTemplate, ConfigValidateArgs, DoctorArgs, PanelArgs, ServeArgs,
    SupportBundleArgs,
};
use broker_config::{
    ensure_cluster_config, load_config, load_managed_config, managed_config_path,
    resolve_from_path, resolve_serve, write_config, BetterMqConfig, Component, PanelMode,
    ResolvedAuth, ResolvedServeSettings, ServeOverrides,
};
use broker_dispatch::{DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, PublishRequest};
use broker_raft_meta::ClusterRuntime;
use broker_schedule::{CronRegistry, ScheduleQueue};
use broker_storage::StorageMode;
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tower_http::{
    cors::{AllowOrigin, Any, CorsLayer},
    trace::TraceLayer,
};

mod cluster_health;
mod docs;
#[cfg(test)]
mod failover_tests;
mod panel;
mod quorum_repair;
mod security;
mod startup_banner;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match Cli::parse_args().command {
        Commands::Serve(args) => serve(args).await,
        Commands::Cluster { cmd } => match cmd {
            ClusterCommands::Init(a) => cluster_init(a),
            ClusterCommands::Join(a) => cluster_join(a).await,
        },
        Commands::Config { cmd } => match cmd {
            ConfigCommands::Init(a) => config_init(a),
            ConfigCommands::Validate(a) => config_validate(a),
            ConfigCommands::Schema => config_schema(),
        },
        Commands::Doctor(args) => doctor(args),
        Commands::SupportBundle(args) => support_bundle(args),
        Commands::Panel(args) => serve_panel(args).await,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    Text,
    Json,
}

impl LogFormat {
    /// `BETTERMQ_LOG_FORMAT`: `text` (default) or `json`.
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "text" => Some(Self::Text),
            "json" => Some(Self::Json),
            _ => None,
        }
    }
}

fn init_tracing() {
    let raw = std::env::var("BETTERMQ_LOG_FORMAT").unwrap_or_default();
    let format = LogFormat::parse(&raw);
    let registry = tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()));
    match format {
        Some(LogFormat::Json) => registry
            .with(tracing_subscriber::fmt::layer().json().flatten_event(true))
            .init(),
        Some(LogFormat::Text) | None => registry.with(tracing_subscriber::fmt::layer()).init(),
    }
    if format.is_none() {
        tracing::warn!(value = %raw, "invalid BETTERMQ_LOG_FORMAT (use text|json); using text");
    }
}

/// Request IDs plus a per-request span carrying them, so every log line a
/// handler emits can be correlated across gateway, broker and peer calls.
trait HttpObservability {
    fn with_http_observability(self) -> Self;
}

impl HttpObservability for axum::Router {
    fn with_http_observability(self) -> Self {
        self.layer(TraceLayer::new_for_http().make_span_with(
            |request: &axum::http::Request<axum::body::Body>| {
                let request_id = request
                    .headers()
                    .get(broker_api::request_id::REQUEST_ID_HEADER)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default();
                // Path only: query strings may carry credentials.
                tracing::info_span!(
                    "request",
                    method = %request.method(),
                    path = %request.uri().path(),
                    request_id = %request_id,
                )
            },
        ))
        .layer(axum::middleware::from_fn(
            broker_api::request_id::assign_request_id,
        ))
    }
}

fn config_init(args: ConfigInitArgs) -> anyhow::Result<()> {
    let cfg = match args.template {
        ConfigTemplate::Local => BetterMqConfig::template_single_local(),
        ConfigTemplate::Slate => BetterMqConfig::template_single_slate(),
        ConfigTemplate::Cluster => BetterMqConfig::template_cluster_local(),
        #[cfg(feature = "cloud")]
        ConfigTemplate::Cloud => BetterMqConfig::template_cloud(),
    };
    write_config(&args.output, &cfg).context("write bettermq.json")?;
    info!(path = %args.output.display(), template = ?args.template, "wrote config");
    Ok(())
}

fn config_validate(args: ConfigValidateArgs) -> anyhow::Result<()> {
    load_config(&args.config).context("invalid bettermq.json")?;
    info!(path = %args.config.display(), "config valid");
    Ok(())
}

fn config_schema() -> anyhow::Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&broker_config::json_schema())?
    );
    Ok(())
}

fn doctor(args: DoctorArgs) -> anyhow::Result<()> {
    let report = build_doctor_report(&args.data_dir, args.config.as_deref());
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        for check in report["checks"].as_array().into_iter().flatten() {
            println!(
                "{:<5} {:<18} {}",
                check["status"].as_str().unwrap_or("ERROR"),
                check["name"].as_str().unwrap_or("unknown"),
                check["detail"].as_str().unwrap_or("")
            );
        }
    }
    if report["ok"].as_bool() == Some(true) {
        Ok(())
    } else {
        anyhow::bail!("doctor found one or more errors")
    }
}

fn support_bundle(args: SupportBundleArgs) -> anyhow::Result<()> {
    let config_path = args.config.clone().or_else(|| {
        managed_config_path(&args.data_dir)
            .exists()
            .then(|| managed_config_path(&args.data_dir))
    });
    let mut config = config_path
        .as_deref()
        .and_then(|path| std::fs::read(path).ok())
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .unwrap_or(serde_json::Value::Null);
    broker_cli::redact_json(&mut config);

    let mut environment = serde_json::Map::new();
    for (key, value) in std::env::vars() {
        if key.starts_with("BETTERMQ_") || key.starts_with("AWS_") || key == "DATABASE_URL" {
            let value = if broker_cli::is_sensitive_key(&key) {
                "[REDACTED]".to_string()
            } else {
                value
            };
            environment.insert(key, serde_json::Value::String(value));
        }
    }
    let mut environment = serde_json::Value::Object(environment);
    broker_cli::redact_json(&mut environment);
    let bundle = serde_json::json!({
        "format_version": 1,
        "generated_at": Utc::now().to_rfc3339(),
        "bettermq_version": env!("CARGO_PKG_VERSION"),
        "protocol_version": broker_proto::PROTOCOL_VERSION,
        "platform": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH
        },
        "doctor": build_doctor_report(&args.data_dir, config_path.as_deref()),
        "config_path": config_path,
        "config": config,
        "environment": environment,
        "files": inventory_files(&args.data_dir, 10_000),
    });
    if let Some(parent) = args.output.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let part = args.output.with_extension("json.part");
    std::fs::write(&part, serde_json::to_vec_pretty(&bundle)?)?;
    broker_storage::set_secret_file_mode(&part);
    std::fs::rename(&part, &args.output)?;
    broker_storage::set_secret_file_mode(&args.output);
    println!("wrote redacted support bundle {}", args.output.display());
    Ok(())
}

fn build_doctor_report(
    data_dir: &std::path::Path,
    config: Option<&std::path::Path>,
) -> serde_json::Value {
    let mut checks = Vec::new();
    let mut ok = true;
    let mut add = |name: &str, status: &str, detail: String| {
        if status == "ERROR" {
            ok = false;
        }
        checks.push(serde_json::json!({"name": name, "status": status, "detail": detail}));
    };

    if !data_dir.is_dir() {
        add(
            "data-directory",
            "ERROR",
            format!(
                "{} does not exist or is not a directory",
                data_dir.display()
            ),
        );
    } else {
        let probe = data_dir.join(format!(".doctor-write-test-{}", std::process::id()));
        match std::fs::write(&probe, b"ok").and_then(|_| std::fs::remove_file(&probe)) {
            Ok(()) => add(
                "data-directory",
                "OK",
                format!("{} is readable and writable", data_dir.display()),
            ),
            Err(error) => add("data-directory", "ERROR", error.to_string()),
        }
    }

    let config_path = config.map(std::path::Path::to_path_buf).or_else(|| {
        managed_config_path(data_dir)
            .exists()
            .then(|| managed_config_path(data_dir))
    });
    match config_path {
        Some(path) => match load_config(&path) {
            Ok(cfg) => {
                add("config", "OK", format!("{} is compatible", path.display()));
                let profile = cfg
                    .components
                    .as_ref()
                    .and_then(|s| s.profile.clone())
                    .unwrap_or_else(|| "all".into());
                add(
                    "components",
                    "OK",
                    format!(
                        "schema v{} profile {profile} (V1 files resolve in memory, not rewritten)",
                        cfg.version
                    ),
                );
            }
            Err(error) => add("config", "ERROR", format!("{}: {error:#}", path.display())),
        },
        None => add(
            "config",
            "WARN",
            "no config file found; serve defaults or CLI flags may be intentional".into(),
        ),
    }

    let files = inventory_paths(data_dir, 10_000);
    let wal_files: Vec<_> = files
        .iter()
        .filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| {
                    name == "active.wal" || (name.starts_with("segment-") && name.ends_with(".log"))
                })
        })
        .collect();
    let mut frames = 0usize;
    let mut wal_error = None;
    for path in &wal_files {
        match verify_wal_frames(path) {
            Ok(count) => frames += count,
            Err(error) => {
                wal_error = Some(format!("{}: {error}", path.display()));
                break;
            }
        }
    }
    if let Some(error) = wal_error {
        add("wal-checksums", "ERROR", error);
    } else {
        add(
            "wal-checksums",
            "OK",
            format!(
                "{} WAL/segment files, {frames} valid frames",
                wal_files.len()
            ),
        );
    }

    let archive_files = std::env::var_os("BETTERMQ_ARCHIVE_DIR")
        .map(std::path::PathBuf::from)
        .map(|root| inventory_paths(&root, 10_000))
        .unwrap_or_default();
    let manifests: Vec<_> = archive_files
        .iter()
        .filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| name.ends_with(".manifest.json"))
        })
        .collect();
    let invalid_manifest = manifests.iter().find_map(|path| match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice::<serde_json::Value>(&bytes)
            .err()
            .map(|error| format!("{}: {error}", path.display())),
        Err(error) => Some(format!("{}: {error}", path.display())),
    });
    if let Some(error) = invalid_manifest {
        add("archive-manifests", "ERROR", error);
    } else {
        add(
            "archive-manifests",
            "OK",
            format!("{} manifests parsed", manifests.len()),
        );
    }

    serde_json::json!({"ok": ok, "checks": checks})
}

fn verify_wal_frames(path: &std::path::Path) -> Result<usize, String> {
    use std::io::BufRead;
    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let mut reader = std::io::BufReader::new(file);
    let manifest = path
        .parent()
        .into_iter()
        .flat_map(|parent| {
            [
                parent.to_path_buf(),
                parent.parent().unwrap_or(parent).to_path_buf(),
            ]
        })
        .map(|dir| broker_storage::WalManifest::path(&dir))
        .find(|candidate| candidate.exists())
        .and_then(|manifest| std::fs::read(manifest).ok())
        .and_then(|bytes| serde_json::from_slice::<broker_storage::WalManifest>(&bytes).ok());
    let mut count = 0usize;
    loop {
        if reader.fill_buf().map_err(|e| e.to_string())?.is_empty() {
            return Ok(count);
        }
        if manifest
            .as_ref()
            .is_some_and(|manifest| manifest.wal_format_version == broker_storage::WAL_FORMAT_V2)
        {
            let (epoch, body) =
                broker_proto::decode_epoch(&mut reader).map_err(|e| e.to_string())?;
            let mut body = std::io::Cursor::new(body);
            let mut epoch_records = 0u32;
            while body.position() < body.get_ref().len() as u64 {
                broker_proto::decode_frame(&mut body).map_err(|e| e.to_string())?;
                epoch_records += 1;
            }
            if epoch_records != epoch.record_count {
                return Err(format!(
                    "epoch declared {} records but contained {epoch_records}",
                    epoch.record_count
                ));
            }
            count += epoch_records as usize;
        } else {
            broker_proto::decode_frame(&mut reader).map_err(|e| e.to_string())?;
            count += 1;
        }
    }
}

fn inventory_paths(root: &std::path::Path, limit: usize) -> Vec<std::path::PathBuf> {
    fn visit(
        dir: &std::path::Path,
        depth: usize,
        limit: usize,
        output: &mut Vec<std::path::PathBuf>,
    ) {
        if depth > 16 || output.len() >= limit {
            return;
        }
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            if output.len() >= limit {
                return;
            }
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                visit(&path, depth + 1, limit, output);
            } else if file_type.is_file() {
                output.push(path);
            }
        }
    }
    let mut output = Vec::new();
    if root.is_dir() {
        visit(root, 0, limit, &mut output);
    }
    output
}

fn inventory_files(root: &std::path::Path, limit: usize) -> Vec<serde_json::Value> {
    inventory_paths(root, limit)
        .into_iter()
        .map(|path| {
            let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            let relative = path.strip_prefix(root).unwrap_or(&path);
            serde_json::json!({"path": relative, "bytes": bytes})
        })
        .collect()
}

fn cluster_init(_args: ClusterInitArgs) -> anyhow::Result<()> {
    anyhow::bail!("cluster init is retired: use bettermq config init --template cluster and named cluster.nodes for initial configuration, or Panel → Infrastructure to create a cluster; no files were changed")
}

async fn cluster_join(_args: ClusterJoinArgs) -> anyhow::Result<()> {
    anyhow::bail!("cluster join is retired: use Panel → Infrastructure enrollment (POST /v1/infra/cluster/enroll with a seed join token); copying seed membership does not enroll a node; no files were changed")
}

#[cfg(feature = "cloud")]
fn serve_database_url(args: &ServeArgs) -> Option<String> {
    args.database_url.clone()
}

#[cfg(not(feature = "cloud"))]
fn serve_database_url(_args: &ServeArgs) -> Option<String> {
    None
}

fn serve_overrides(args: &ServeArgs) -> ServeOverrides {
    ServeOverrides {
        config_path: args.config.clone(),
        listen: args.listen,
        port: args.port,
        data_dir: args.data_dir.clone(),
        cluster: args.cluster,
        database_url: serve_database_url(args),
        dispatch_fleet: if args.dispatch_fleet {
            Some(true)
        } else {
            None
        },
        broker_only: if args.broker_only { Some(true) } else { None },
        gateway_only: if args.gateway_only { Some(true) } else { None },
        panel_listen: args.panel_listen,
        admin_listen: args.admin_listen,
        internal_listen: args.internal_listen,
        profile: args.profile.clone(),
        components: args.components.clone(),
        no_panel: args.no_panel,
        standalone_panel: false,
        controller_url: None,
    }
}

async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    if let Some(url) = &args.internal_url {
        std::env::set_var("BETTERMQ_INTERNAL_URL", url);
    }
    let overrides = serve_overrides(&args);
    let data_dir = overrides
        .data_dir
        .clone()
        .or_else(|| args.data_dir.clone())
        .unwrap_or_else(|| std::path::PathBuf::from("./data"));
    let managed_path = managed_config_path(&data_dir);
    let config_path = args
        .config
        .clone()
        .or_else(|| managed_path.exists().then_some(managed_path));

    let file_cfg = if let Some(ref path) = config_path {
        Some(load_config(path).with_context(|| format!("load {}", path.display()))?)
    } else {
        None
    };

    let settings = if let Some(ref path) = config_path {
        resolve_from_path(path, &overrides)?
    } else {
        resolve_serve(file_cfg.as_ref(), &overrides)?
    };

    settings.apply_env();
    std::env::set_var(
        "BETTERMQ_PANEL_MODE",
        match settings.panel_mode {
            PanelMode::Embedded => "embedded",
            PanelMode::SeparateListener => "separate",
            PanelMode::Disabled => "disabled",
            PanelMode::Standalone => "standalone",
        },
    );

    if settings.gateway_only || (settings.has(Component::Gateway) && !settings.opens_local_storage)
    {
        if settings.broker_only || settings.dispatch_fleet {
            anyhow::bail!(
                "--gateway-only cannot be combined with --broker-only or --dispatch-fleet"
            );
        }
        return serve_gateway_only(&settings).await;
    }

    // The dispatch profile delivers for remote brokers and opens no storage;
    // it must be selected before the storage-free panel fallback.
    if settings.dispatch_fleet && !settings.opens_local_storage {
        return serve_dispatch_fleet(&settings).await;
    }

    if !settings.opens_local_storage {
        return serve_panel_from_settings(&settings).await;
    }

    if settings.storage == broker_config::StorageMode::Slate && settings.cluster_enabled {
        anyhow::bail!(broker_storage::SLATE_SINGLE_BROKER);
    }

    std::fs::create_dir_all(&settings.data_dir)
        .with_context(|| format!("create data dir {}", settings.data_dir.display()))?;

    if settings.cluster_enabled {
        let cluster_cfg = if let Some(c) = &file_cfg {
            c.clone()
        } else {
            load_managed_config(&settings.data_dir)
                .context("load managed config")?
                .context(
                    "cluster mode requires saved infrastructure config — use Panel → Infrastructure",
                )?
        };
        ensure_cluster_config(&cluster_cfg, &settings.data_dir)
            .context("write cluster-config.json")?;
    }
    // The archive namespace defaults to the durable cluster/node identities.
    broker_storage::start_archive_service().context("initialize sealed-segment archive")?;

    let postgres = matches!(&settings.auth, ResolvedAuth::Cloud { .. });
    if !postgres {
        let auth_path = std::env::var_os("BETTERMQ_LOCAL_AUTH_FILE")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| settings.data_dir.join("local-auth.json"));
        check_auth_directory(&auth_path)?;
    }
    let mut first_boot_task = None;

    #[cfg(feature = "cloud")]
    let (auth, local_auth, control_plane) = match &settings.auth {
        ResolvedAuth::Cloud { database_url } => {
            let cp = broker_control_plane::ControlPlanePool::connect(database_url.as_str())
                .await
                .context("connect control plane postgres")?;
            cp.migrate().await.context("migrate control plane")?;
            let auth = broker_control_plane::ApiKeyValidator::new(cp.clone());
            (Some(auth), None, Some(cp))
        }
        ResolvedAuth::Local { .. } => {
            let local = broker_local_auth::LocalAuthStore::open(&settings.data_dir)
                .context("open local auth")?;
            if local.is_configured() {
                info!("local API token auth enabled");
            } else {
                first_boot_task = start_first_boot_setup(&settings.data_dir);
            }
            (None, Some(Arc::new(local)), None)
        }
    };

    #[cfg(not(feature = "cloud"))]
    let local_auth = match &settings.auth {
        ResolvedAuth::Cloud { .. } => {
            anyhow::bail!("auth.mode \"cloud\" is not supported in the self-host build (use auth.mode \"local\")")
        }
        ResolvedAuth::Local { .. } => {
            let local = broker_local_auth::LocalAuthStore::open(&settings.data_dir)
                .context("open local auth")?;
            if local.is_configured() {
                info!("local API token auth enabled");
            } else {
                first_boot_task = start_first_boot_setup(&settings.data_dir);
            }
            Some(Arc::new(local))
        }
    };

    let mut broker_cfg = BrokerConfig::new(settings.data_dir.clone());
    broker_cfg.storage = match settings.storage {
        broker_config::StorageMode::Local => StorageMode::Local,
        broker_config::StorageMode::Slate => StorageMode::Slate,
    };
    broker_cfg.retry_defaults = settings.dispatch_retry.clone();
    let storage = broker_cfg.storage;
    let broker = Broker::open(broker_cfg).context("open broker storage")?;
    if !settings.cluster_enabled {
        broker
            .import_legacy_shared_catalog()
            .context("import standalone legacy catalog")?;
    }

    let cluster = if settings.cluster_enabled {
        let config =
            ClusterRuntime::load_config(&settings.data_dir).context("load cluster-config.json")?;
        let runtime =
            ClusterRuntime::open_with_raft(&settings.data_dir, config, broker.layout().shard_count)
                .await
                .context("open OpenRaft controller")?;
        let policy = runtime
            .controller()
            .context("controller unavailable")?
            .state();
        validate_replication_policy(
            &settings.replication,
            policy.replication_factor,
            policy.min_isr,
        )?;
        Some(Cluster::new(runtime).with_physical_shards(broker.layout().is_physical()))
    } else {
        None
    };
    // Multi-node local-WAL cells deliver only quorum-durable records (D2).
    // Single node and Slate keep the local committed watermark.
    if cluster
        .as_ref()
        .is_some_and(|c| c.runtime.config().node_count() > 1)
        && storage == StorageMode::Local
    {
        broker.enable_quorum_gating();
    }
    if let Some(ref c) = cluster {
        c.attach_local_log(broker.clone(), &settings.data_dir)
            .context("open replica fence")?;
        if c.runtime.config().node_count() > 1 && storage == StorageMode::Local {
            // Blobs would exist only on the accepting node: keep bodies inline
            // in the replicated WAL frame, bounded by the epoch cap (D3).
            let limits = broker_replication::cell_body_limits(
                broker_api::internal_body_limit_bytes(),
                broker_proto::record::max_frame_payload_bytes(),
            )
            .map_err(|reason| {
                anyhow::anyhow!("refusing to start a local-WAL cell node: {reason}")
            })?;
            broker.set_payload_policy(broker_partition::PayloadPolicy::InlineOnly {
                max_body: limits.max_inline_body,
            });
            info!(
                cell_max_inline_body = limits.max_inline_body,
                dlq_envelope_allowance = broker_replication::DLQ_ENVELOPE_ALLOWANCE,
                max_replication_epoch_bytes = limits.max_epoch_bytes,
                "local-WAL cell: message bodies stay inline in the replicated WAL; larger bodies get 413"
            );
        }
        let cluster = c.clone();
        broker.set_shard_leader_check(Arc::new(move |p| {
            if cluster.catalog_ready() && cluster.runtime.is_leader_for_shard(p) {
                Some(cluster.runtime.shard_generation(p))
            } else {
                None
            }
        }));
    }
    let schedule = ScheduleQueue::open(&settings.data_dir).context("open schedule queue")?;
    let crons = CronRegistry::open(&settings.data_dir).context("open cron registry")?;
    let catalog_tombstones =
        CatalogTombstones::open(&settings.data_dir).context("open catalog tombstones")?;

    let dispatch_cfg = dispatch_config(&settings);
    let leases = broker_dispatch::LeaseTable::new();
    let broker_only = settings.broker_only || settings.dispatch_fleet;
    let leader_check = cluster.as_ref().map(|c| {
        let cluster = c.clone();
        Arc::new(move |p| cluster.catalog_ready() && cluster.runtime.is_leader_for_shard(p))
            as Arc<dyn Fn(u32) -> bool + Send + Sync>
    });
    let dispatch = DispatchEngine::new_with_controls(
        broker.clone(),
        dispatch_cfg,
        !broker_only,
        Some(leases.clone()),
        leader_check,
    );
    // One bounded pass at boot; the backfill loop continues if work remains.
    let boot_backfill_more = !broker_only && dispatch.backfill_pending();

    let app_state = Arc::new(AppState {
        broker,
        schedule,
        crons,
        dispatch,
        leases,
        cluster,
        local_auth,
        fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
        catalog_tombstones,
        dispatch_fleet: settings.dispatch_fleet,
        broker_only: settings.broker_only,
        #[cfg(feature = "cloud")]
        auth,
        #[cfg(feature = "cloud")]
        control_plane,
    });

    // DLQ moves replicate to the source shard's quorum before completing.
    app_state
        .dispatch
        .set_dlq_durability(Arc::new(broker_api::ClusterDlqDurability::new(&app_state)));

    let mut bg_tasks = Vec::new();
    if let Some(task) = first_boot_task {
        bg_tasks.push(task);
    }
    if settings.cluster_enabled {
        if let Some(h) = spawn_cluster_catalog_sync(app_state.clone()) {
            bg_tasks.push(h);
        }
    }
    if let (Some(c), true) = (&app_state.cluster, app_state.broker.quorum_gating()) {
        bg_tasks.push(quorum_repair::spawn_quorum_repair(
            c.clone(),
            app_state.clone(),
        ));
        // With a shared metadata dir every node already sees the cursors.
        if settings.cluster_shared_meta_dir.is_none() {
            bg_tasks.push(quorum_repair::spawn_cursor_checkpoints(
                c.clone(),
                app_state.clone(),
            ));
        }
    }
    if let Some(ref c) = app_state.cluster {
        bg_tasks.push(cluster_health::spawn_cluster_health_monitor(
            c.clone(),
            app_state.dispatch.clone(),
            app_state.clone(),
        ));
    }

    if !settings.dispatch_fleet {
        bg_tasks.push(spawn_schedule_worker(app_state.clone()));
    }
    if app_state.broker.config().log.fsync == broker_storage::FsyncMode::Group {
        bg_tasks.push(spawn_wal_group_flusher(app_state.broker.clone()));
    }
    if !broker_only {
        bg_tasks.push(spawn_dispatch_backfill_loop(
            app_state.clone(),
            boot_backfill_more,
        ));
    }
    if let Some(task) = broker_api::spawn_retention_loop(app_state.clone()) {
        bg_tasks.push(task);
    }
    let fleet = if settings.dispatch_fleet {
        Some(start_fleet(&settings)?)
    } else {
        None
    };

    let cors = build_cors_layer();

    if settings.dispatch_fleet {
        info!("dispatch fleet mode: claiming from BETTERMQ_BROKER_URLS");
    }
    if settings.broker_only {
        info!("broker-only mode: lease API enabled, local delivery workers off");
    }

    let include_panel =
        settings.panel_mode != PanelMode::Disabled && settings.has(Component::Panel);
    let registry_path = settings.data_dir.join("cell-registry.json");
    let controller_url = settings
        .controller_url
        .clone()
        .unwrap_or_else(|| format!("http://{}", settings.listeners.admin));
    let admin_state = AdminState::local_with_registry(
        app_state.clone(),
        Some(registry_path),
        Some(controller_url),
    );
    let panel_and_admin = {
        let mut r = admin_router(admin_state);
        if include_panel {
            r = r
                .route("/panel", get(|| async { Redirect::permanent("/panel/") }))
                .nest("/panel/", panel::resolve_router());
        }
        r.merge(docs::router())
            .layer(axum::middleware::from_fn(security::api_security_headers))
            .layer(cors.clone())
            .with_http_observability()
    };

    // Assemble roles by socket, not by a second runtime collapse heuristic.
    // Each address is bound once, including partial splits and admin == internal.
    let mut addresses = vec![settings.listeners.public];
    for addr in [settings.listeners.internal, settings.listeners.admin] {
        if !addresses.contains(&addr) {
            addresses.push(addr);
        }
    }
    let mut bound = Vec::new();
    for addr in addresses {
        let mut app = if addr == settings.listeners.public {
            public_router((*app_state).clone())
        } else if addr == settings.listeners.admin {
            // The panel uses the data-plane API on its own origin.
            data_plane_router_no_spawn((*app_state).clone())
        } else {
            axum::Router::new()
        };
        if addr == settings.listeners.internal {
            app = app.merge(internal_router((*app_state).clone()));
        }
        if addr == settings.listeners.admin {
            app = app.merge(panel_and_admin.clone());
        }
        let app = app
            .layer(axum::middleware::from_fn(security::api_security_headers))
            .layer(cors.clone())
            .with_http_observability();
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("bind {addr}"))?;
        info!(%addr, "listen");
        bound.push((listener, app));
    }
    let (listener, app) = bound.remove(0);
    for (listener, app) in bound {
        bg_tasks.push(tokio::spawn(async move {
            let _ = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await;
        }));
    }

    startup_banner::print(&settings, storage);
    if postgres {
        info!("cloud auth enabled (postgres)");
    }

    let shutdown_deadline = shutdown_deadline_secs();
    let drain_notice = Arc::new(tokio::sync::Notify::new());
    let signal_notice = drain_notice.clone();
    let shutdown = async move {
        shutdown_signal().await;
        begin_shutdown();
        info!(
            timeout_secs = shutdown_deadline,
            "shutdown signal received; admission stopped, draining HTTP"
        );
        signal_notice.notify_one();
    };

    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown);
    let mut server = Box::pin(std::future::IntoFuture::into_future(server));
    tokio::select! {
        result = &mut server => {
            result.context("HTTP server exited with error")?;
        }
        _ = drain_notice.notified() => {
            if tokio::time::timeout(Duration::from_secs(shutdown_deadline), &mut server)
                .await
                .is_err()
            {
                tracing::warn!("HTTP drain deadline exceeded; closing remaining connections");
            }
        }
    }

    if let Some((fleet, delivery)) = &fleet {
        stop_fleet(fleet, delivery, shutdown_deadline).await;
    }
    if app_state
        .dispatch
        .drain(Duration::from_secs(shutdown_deadline))
        .await
    {
        info!("dispatch drain complete");
    } else {
        tracing::warn!("dispatch drain deadline exceeded; retry state remains durable");
    }

    let broker = app_state.broker.clone();
    match tokio::time::timeout(
        Duration::from_secs(shutdown_deadline),
        tokio::task::spawn_blocking(move || broker.flush_wal()),
    )
    .await
    {
        Ok(Ok(Ok(()))) => info!("WAL drain complete"),
        Ok(Ok(Err(error))) => tracing::warn!(%error, "WAL flush on shutdown failed"),
        Ok(Err(error)) => tracing::warn!(%error, "WAL flush task failed"),
        Err(_) => tracing::warn!("WAL flush deadline exceeded"),
    }
    abort_and_join(bg_tasks, Duration::from_secs(shutdown_deadline.min(5))).await;
    app_state.dispatch.shutdown_background().await;
    drop(app_state);
    // Let reqwest/hyper drop timers before #[tokio::main] tears down the runtime.
    tokio::task::yield_now().await;
    info!("dispatch, scheduler, and panel tasks stopped");

    Ok(())
}

fn check_auth_directory(auth_path: &std::path::Path) -> anyhow::Result<()> {
    let parent = auth_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    std::fs::create_dir_all(parent)
        .with_context(|| format!("create local auth directory {}", parent.display()))?;
    let probe = parent.join(format!(".auth-write-check-{}", Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(&probe).with_context(|| {
        format!(
            "local auth directory {} must be writable and on persistent storage (auth file {})",
            parent.display(),
            auth_path.display()
        )
    })?;
    drop(file);
    std::fs::remove_file(&probe).context("remove auth-directory write probe")?;
    Ok(())
}

fn validate_replication_policy(
    configured: &broker_config::ReplicationPolicyConfig,
    factor: u32,
    min_isr: u32,
) -> anyhow::Result<()> {
    if configured.factor != factor || configured.min_isr != min_isr {
        anyhow::bail!(
            "configured replication factor={} minISR={} differs from controller factor={factor} minISR={min_isr}; use the admin replication policy API and align bettermq.json before starting",
            configured.factor, configured.min_isr
        );
    }
    Ok(())
}

async fn serve_panel(args: PanelArgs) -> anyhow::Result<()> {
    let mut overrides = ServeOverrides {
        config_path: args.config.clone(),
        listen: Some(args.listen),
        standalone_panel: true,
        controller_url: args.controller.clone(),
        data_dir: args.data_dir.clone(),
        ..ServeOverrides::default()
    };
    overrides.panel_listen = Some(args.listen);
    let file_cfg = args
        .config
        .as_ref()
        .map(|path| load_config(path))
        .transpose()
        .context("load panel config")?;
    let mut settings = resolve_serve(file_cfg.as_ref(), &overrides)?;
    settings.listen = args.listen;
    settings.listeners.public = args.listen;
    settings.listeners.admin = args.listen;
    settings.listeners.internal = args.listen;
    settings.panel_mode = PanelMode::Standalone;
    settings.controller_url = args.controller.or(settings.controller_url);
    std::env::set_var("BETTERMQ_PANEL_MODE", "standalone");
    serve_panel_from_settings(&settings).await
}

async fn serve_panel_from_settings(settings: &ResolvedServeSettings) -> anyhow::Result<()> {
    let registry_path = settings.data_dir.join("cell-registry.json");
    let (registry, persist) = broker_api::CellRegistry::load_for_startup(&registry_path);
    if settings.controller_url.is_none() {
        tracing::warn!("standalone panel has no --controller; cell reads will return 503");
    }
    let state = AdminState::remote_with_path(
        settings.controller_url.clone(),
        registry,
        persist.then_some(registry_path),
    );
    let app = admin_router(state)
        .route("/panel", get(|| async { Redirect::permanent("/panel/") }))
        .nest("/panel/", panel::resolve_router())
        .merge(docs::router())
        .route("/healthz", get(|| async { "ok" }))
        .layer(axum::middleware::from_fn(security::api_security_headers))
        .layer(build_cors_layer())
        .with_http_observability();
    let addr = settings.listeners.admin;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind panel {}", addr))?;
    info!(
        listen = %addr,
        "standalone panel: no broker WAL, RocksDB, archive, or dispatch opened"
    );
    let shutdown = async {
        shutdown_signal().await;
        begin_shutdown();
        info!("panel shutdown signal received; draining HTTP");
    };
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
    .context("panel HTTP server exited")
}

async fn serve_gateway_only(settings: &ResolvedServeSettings) -> anyhow::Result<()> {
    let state = GatewayOnlyState::from_env().map_err(anyhow::Error::msg)?;
    #[cfg(feature = "cloud")]
    let state = match &settings.auth {
        ResolvedAuth::Cloud { database_url } => {
            let control_plane = broker_control_plane::ControlPlanePool::connect(database_url)
                .await
                .context("connect gateway control plane")?;
            let auth = broker_control_plane::ApiKeyValidator::new(control_plane.clone());
            state.with_cloud_auth(auth, control_plane)
        }
        ResolvedAuth::Local { .. } => state,
    };
    #[cfg(not(feature = "cloud"))]
    if matches!(&settings.auth, ResolvedAuth::Cloud { .. }) {
        anyhow::bail!("cloud auth is unavailable in this build");
    }

    let app = gateway_only_router(state)
        .merge(docs::router())
        .layer(axum::middleware::from_fn(security::api_security_headers))
        .layer(build_cors_layer())
        .with_http_observability();
    let listener = tokio::net::TcpListener::bind(settings.listen)
        .await
        .with_context(|| format!("bind {}", settings.listen))?;
    info!(
        listen = %settings.listen,
        "gateway-only mode: no local broker, WAL, index, scheduler, dispatch, or archive opened"
    );
    let shutdown = async {
        shutdown_signal().await;
        begin_shutdown();
        info!("gateway shutdown signal received; draining HTTP");
    };
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
    .context("gateway HTTP server exited")
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            tracing::warn!(%error, "SIGINT handler failed");
                        }
                    }
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::warn!(%error, "SIGTERM handler failed; waiting for SIGINT");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "shutdown signal handler failed");
    }
}

fn build_cors_layer() -> CorsLayer {
    // Default: same-origin panel needs no CORS. Set BETTERMQ_CORS_ORIGINS to a
    // comma-separated allowlist, or `*` only when you intentionally need it.
    let raw = std::env::var("BETTERMQ_CORS_ORIGINS").unwrap_or_default();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return CorsLayer::new();
    }
    if trimmed == "*" {
        return CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);
    }
    let origins: Vec<_> = trimmed
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse().ok())
        .collect();
    if origins.is_empty() {
        return CorsLayer::new();
    }
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods(Any)
        .allow_headers(Any)
}

fn start_first_boot_setup(data_dir: &std::path::Path) -> Option<JoinHandle<()>> {
    // Older builds wrote a one-time secret into the volume. That is unused now.
    let leftover = data_dir.join("setup-token.txt");
    if leftover.exists() {
        if let Err(e) = std::fs::remove_file(&leftover) {
            tracing::warn!(error = %e, "could not remove leftover setup-token.txt");
        }
    }

    if matches!(
        std::env::var("BETTERMQ_ALLOW_OPEN_SETUP")
            .ok()
            .as_deref()
            .map(str::trim),
        Some("1") | Some("true") | Some("TRUE") | Some("yes")
    ) {
        info!("local auth not configured — open setup allowed (BETTERMQ_ALLOW_OPEN_SETUP)");
        eprintln!(
            "Open /panel/ and set a password. Anyone who can reach this port can claim admin."
        );
        return None;
    }

    let secs: u64 = std::env::var("BETTERMQ_SETUP_WINDOW_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15 * 60);
    if secs == 0 {
        info!("local auth not configured — setup locked (BETTERMQ_SETUP_WINDOW_SECS=0)");
        eprintln!(
            "Setup is locked. Restart BetterMQ to open a setup window, or set BETTERMQ_ALLOW_OPEN_SETUP=1."
        );
        return None;
    }

    let code = open_setup_window(Duration::from_secs(secs));
    let mins = secs.div_ceil(60);
    info!(
        minutes = mins,
        "local auth not configured — setup window open (setup code printed to stderr)"
    );
    eprintln!("Set a panel password at /panel/ (open for {mins} minutes).");
    eprintln!("Setup code: {code}");
    eprintln!("The panel asks for this code; only someone who can read this log can claim admin.");
    eprintln!("After that, restart this process to open setup again.");
    Some(tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(secs)).await;
        eprintln!("Setup window closed. Restart BetterMQ to set a password.");
    }))
}

async fn abort_and_join(tasks: Vec<JoinHandle<()>>, timeout: Duration) {
    for task in &tasks {
        task.abort();
    }
    let joining = async {
        for task in tasks {
            let _ = task.await;
        }
    };
    if tokio::time::timeout(timeout, joining).await.is_err() {
        tracing::warn!("background task join deadline exceeded");
    }
}

/// Group-commit timer. Each flush walks every shard and fsyncs, so it runs on
/// the blocking pool; awaiting it before the next tick keeps cycles from
/// overlapping.
fn spawn_wal_group_flusher(broker: Broker) -> tokio::task::JoinHandle<()> {
    let interval = broker.config().log.group_interval;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let flushing = broker.clone();
            match tokio::task::spawn_blocking(move || flushing.flush_wal_if_due()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!(error = %e, "wal group flush failed"),
                Err(e) => tracing::warn!(error = %e, "wal group flush task failed"),
            }
        }
    })
}

/// Regular backfill cadence once the pending-work index has caught up.
const BACKFILL_INTERVAL: Duration = Duration::from_secs(30);
/// Delay before the next pass while a backlog is still being scanned.
const BACKFILL_CONTINUE: Duration = Duration::from_secs(1);

fn spawn_dispatch_backfill_loop(
    state: Arc<AppState>,
    mut more: bool,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let wait = if more {
                BACKFILL_CONTINUE
            } else {
                BACKFILL_INTERVAL
            };
            tokio::select! {
                _ = tokio::time::sleep(wait) => {},
                _ = state.dispatch.backfill_notified() => {
                    // Coalesce per-message notifications; never spin while a
                    // lane is full or let dispatch compete with every ACK.
                    tokio::time::sleep(Duration::from_millis(25)).await;
                },
            }
            let dispatch = state.dispatch.clone();
            // Storage scans and metadata lookups block; keep them off the
            // async workers that serve ingest and replication.
            more = tokio::task::spawn_blocking(move || dispatch.backfill_pending())
                .await
                .unwrap_or(false);
        }
    })
}

/// How long after start a node keeps re-scanning the legacy shared meta dir
/// for schedule state written by nodes still on an older release.
fn legacy_schedule_import_window() -> Duration {
    std::env::var("BETTERMQ_LEGACY_SCHEDULE_IMPORT_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(3_600))
}

const LEGACY_SCHEDULE_IMPORT_EVERY: Duration = Duration::from_secs(30);

/// Upper bound on delayed jobs fired per 200 ms tick, so a burst of due jobs
/// cannot starve the cron pass or the lease renewal.
const MAX_DELAYED_FIRES_PER_TICK: usize = 256;

/// Import pre-upgrade schedule state from `BETTERMQ_SHARED_META_DIR`: crons on
/// every node (only the scheduler holder fires them), delayed jobs only on the
/// scheduler holder, which then owns and fires them.
async fn import_legacy_schedule_state(
    state: &Arc<AppState>,
    shared_dir: std::path::PathBuf,
    scheduler_leader: bool,
) {
    let schedule = state.schedule.clone();
    let crons = state.crons.clone();
    let result = tokio::task::spawn_blocking(move || {
        let crons = crons.import_legacy_shared(&shared_dir);
        let delayed = scheduler_leader.then(|| schedule.import_legacy_shared(&shared_dir));
        (crons, delayed)
    })
    .await;
    match result {
        Ok((crons, delayed)) => {
            if let Err(error) = crons {
                tracing::warn!(%error, "legacy shared cron import failed; will retry");
            }
            if let Some(Err(error)) = delayed {
                tracing::warn!(%error, "legacy shared delayed-job import failed; will retry");
            }
        }
        Err(error) => tracing::warn!(%error, "legacy schedule import task failed"),
    }
}

fn spawn_schedule_worker(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(200));
        let legacy_shared_dir =
            std::env::var_os("BETTERMQ_SHARED_META_DIR").map(std::path::PathBuf::from);
        let import_until = tokio::time::Instant::now() + legacy_schedule_import_window();
        let mut next_import = tokio::time::Instant::now();
        loop {
            interval.tick().await;
            let scheduler_leader = match &state.cluster {
                None => true,
                Some(c) => {
                    if !c.catalog_ready() {
                        continue;
                    }
                    if let Err(error) = c.runtime.acquire_scheduler_leader(5_000).await {
                        broker_api::record_scheduler_failure(broker_api::SchedulerFailure::Lease);
                        tracing::warn!(%error, "OpenRaft scheduler lease acquisition failed");
                    }
                    c.runtime.is_scheduler_leader()
                }
            };
            if let Some(shared_dir) = &legacy_shared_dir {
                let tick = tokio::time::Instant::now();
                if tick >= next_import && tick < import_until {
                    next_import = tick + LEGACY_SCHEDULE_IMPORT_EVERY;
                    import_legacy_schedule_state(&state, shared_dir.clone(), scheduler_leader)
                        .await;
                }
            }

            let now = Utc::now().timestamp_millis();

            // Delayed jobs are node-local. In a cell no new ones are accepted,
            // so every node drains its own pre-upgrade jobs through the quorum
            // publish path (deduplicated by their `delay:<uuid>` key) rather
            // than waiting to win the scheduler lease.
            fire_due_schedules(
                &state,
                state.schedule.pop_due(now, MAX_DELAYED_FIRES_PER_TICK),
                now,
            )
            .await;
            if scheduler_leader {
                fire_due_crons(&state, now).await;
            }
        }
    })
}

/// Fire due crons. Only the scheduler holder calls this.
async fn fire_due_crons(state: &Arc<AppState>, now: i64) {
    let crons = state.crons.clone();
    match broker_partition::run_blocking_io(move || {
        crons.retry_pending_completions(SCHEDULE_COMPLETE_BATCH)
    })
    .await
    {
        Ok(Ok(_)) => {}
        result => {
            broker_api::record_scheduler_failure(broker_api::SchedulerFailure::Persist);
            tracing::warn!(?result, "cron completion retry failed");
        }
    }
    let mut due = state.crons.pop_due(now).into_iter().peekable();
    while due.peek().is_some() {
        let limit = Arc::new(tokio::sync::Semaphore::new(SCHEDULE_FIRE_CONCURRENCY));
        let mut firing = tokio::task::JoinSet::new();
        for job in due.by_ref().take(SCHEDULE_COMPLETE_BATCH) {
            let state = state.clone();
            let limit = limit.clone();
            firing.spawn(async move {
                let _permit = limit.acquire_owned().await;
                tracing::info!(cron_id = %job.id, queue = %job.request.topic, "cron tick");
                let mut req = job.request.clone();
                // Per-firing key: a user key alone would dedupe every later tick.
                req.idempotency_key = Some(job.fire_idempotency_key());
                let outcome = fire_scheduled_publish(&state, req).await;
                (job, outcome)
            });
        }
        let mut completed = Vec::new();
        while let Some(joined) = firing.join_next().await {
            match joined {
                Ok((job, Ok(()))) => completed.push(job.id),
                Ok((job, Err(failure))) => {
                    broker_api::record_scheduler_failure(broker_api::SchedulerFailure::CronFire);
                    match state.crons.fail_fire(&job, failure, now) {
                        Ok(broker_schedule::FireRetry::Retry {
                            attempt,
                            retry_at_ms,
                        }) => tracing::warn!(
                            cron_id = %job.id,
                            attempt,
                            retry_in_ms = retry_at_ms - now,
                            "cron fire failed; will retry"
                        ),
                        Ok(broker_schedule::FireRetry::GiveUp { attempt }) => tracing::error!(
                            cron_id = %job.id,
                            attempt,
                            "cron occurrence skipped: publish failed permanently"
                        ),
                        Err(e) => {
                            broker_api::record_scheduler_failure(
                                broker_api::SchedulerFailure::Persist,
                            );
                            tracing::warn!(error = %e, cron_id = %job.id, "cron fail_fire failed")
                        }
                    }
                }
                Err(error) => {
                    broker_api::record_scheduler_failure(broker_api::SchedulerFailure::CronFire);
                    tracing::warn!(%error, "cron fire task failed; durable occurrence retained for restart");
                }
            }
        }
        let crons = state.crons.clone();
        match tokio::task::spawn_blocking(move || crons.commit_fires(&completed)).await {
            Ok(Ok(())) => {}
            result => {
                broker_api::record_scheduler_failure(broker_api::SchedulerFailure::Persist);
                tracing::warn!(
                    ?result,
                    "cron completion batch failed; completion retained for a later tick or restart"
                );
            }
        }
    }
}

/// Delayed publishes fired concurrently per batch.
const SCHEDULE_FIRE_CONCURRENCY: usize = 32;
/// Completions persisted per journal append (and fsync). Bounds how many
/// already-published jobs a crash can make the scheduler fire again.
const SCHEDULE_COMPLETE_BATCH: usize = 256;

/// Fire due delayed publishes with bounded concurrency. Each batch's
/// successes are removed from the schedule journal in one append, on the
/// blocking pool; failures back off per job (see [`broker_schedule::FireRetry`]).
async fn fire_due_schedules(
    state: &Arc<AppState>,
    due: Vec<broker_schedule::ScheduledPublish>,
    now: i64,
) {
    let limit = Arc::new(tokio::sync::Semaphore::new(SCHEDULE_FIRE_CONCURRENCY));
    let mut due = due.into_iter().peekable();
    while due.peek().is_some() {
        let mut firing = tokio::task::JoinSet::new();
        for job in due.by_ref().take(SCHEDULE_COMPLETE_BATCH) {
            let state = state.clone();
            let limit = limit.clone();
            firing.spawn(async move {
                let _permit = limit.acquire_owned().await;
                let outcome = fire_scheduled_publish(&state, job.request.clone()).await;
                (job, outcome)
            });
        }
        let mut completed = Vec::new();
        while let Some(joined) = firing.join_next().await {
            match joined {
                Ok((job, Ok(()))) => completed.push(job.id),
                Ok((job, Err(failure))) => {
                    broker_api::record_scheduler_failure(broker_api::SchedulerFailure::DelayedFire);
                    let id = job.id;
                    let topic = job.request.topic.clone();
                    match state.schedule.fail(job, failure, now) {
                        Ok(broker_schedule::FireRetry::Retry {
                            attempt,
                            retry_at_ms,
                        }) => tracing::warn!(
                            scheduled_id = %id,
                            queue = %topic,
                            attempt,
                            retry_in_ms = retry_at_ms - now,
                            "delayed job fire failed; will retry"
                        ),
                        Ok(broker_schedule::FireRetry::GiveUp { attempt }) => tracing::error!(
                            scheduled_id = %id,
                            queue = %topic,
                            attempt,
                            "delayed job dropped: publish failed permanently"
                        ),
                        Err(e) => {
                            broker_api::record_scheduler_failure(
                                broker_api::SchedulerFailure::Persist,
                            );
                            tracing::warn!(
                                error = %e,
                                scheduled_id = %id,
                                "schedule retry bookkeeping after failed fire failed"
                            )
                        }
                    }
                }
                // The job stays in flight; its durable Add makes a restart fire it again.
                Err(e) => {
                    broker_api::record_scheduler_failure(broker_api::SchedulerFailure::DelayedFire);
                    tracing::warn!(error = %e, "scheduled fire task failed");
                }
            }
        }
        if completed.is_empty() {
            continue;
        }
        let schedule = state.schedule.clone();
        match tokio::task::spawn_blocking(move || schedule.complete_many(&completed)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                broker_api::record_scheduler_failure(broker_api::SchedulerFailure::Persist);
                tracing::warn!(error = %e, "schedule complete persist failed");
            }
            Err(e) => {
                broker_api::record_scheduler_failure(broker_api::SchedulerFailure::Persist);
                tracing::warn!(error = %e, "schedule complete task failed");
            }
        }
    }
}

/// `Ok` when the publish was accepted (including duplicates); otherwise how it failed.
async fn fire_scheduled_publish(
    state: &Arc<AppState>,
    pending: broker_schedule::ScheduledPublishRequest,
) -> Result<(), broker_schedule::FireFailure> {
    let req = PublishRequest {
        routing_id: None,
        topic: pending.topic,
        routing_key: pending.routing_key,
        payload: pending.payload,
        payload_encoding: pending.payload_encoding,
        idempotency_key: pending.idempotency_key,
        delay_ms: None,
        priority: pending.priority,
        flow_id: pending.flow_id,
        queue_id: pending.queue_id,
        group_id: None,
        group_member_id: None,
        destination: pending.destination,
        flow: pending.flow,
        parallelism: pending.parallelism,
        max_retries: pending.max_retries,
        retry_backoff: pending.retry_backoff.clone(),
        method: pending.method.clone(),
        headers: pending.headers.clone(),
        sign: pending.sign,
        request: pending.request.clone(),
        url: None,
        secret: None,
    };
    if state.cluster.is_some() {
        match publish_with_cluster(state, req, None).await {
            Ok(resp) => {
                if !resp.duplicate {
                    enqueue_dispatch_after_publish(state, &resp);
                }
                Ok(())
            }
            Err(e) => {
                tracing::warn!(error = ?e, "scheduled cluster publish failed");
                Err(match &e {
                    ApiError::Broker(error) => {
                        broker_schedule::FireFailure::from_broker_error(error)
                    }
                    // Forwarding and quorum errors are not specific enough to give up on.
                    _ => broker_schedule::FireFailure::Transient,
                })
            }
        }
    } else {
        match state.broker.publish_immediate(req) {
            Ok(resp) => {
                if let (Some(partition), Some(offset)) = (resp.partition, resp.offset) {
                    if let Err(error) = state
                        .broker
                        .wait_committed(&resp.topic, partition, offset)
                        .await
                    {
                        tracing::warn!(
                            %error,
                            topic = %resp.topic,
                            partition,
                            offset,
                            "scheduled enqueue commit failed"
                        );
                        return Err(broker_schedule::FireFailure::Transient);
                    }
                }
                if !resp.duplicate {
                    if let (Some(partition), Some(offset), Some(message_id)) =
                        (resp.partition, resp.offset, resp.message_id)
                    {
                        if !broker_partition::is_dlq_topic(&resp.topic) {
                            state.dispatch.enqueue(broker_dispatch::DeliveryJob::live(
                                resp.topic, partition, offset, message_id,
                            ));
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                tracing::warn!(error = %e, "scheduled enqueue failed");
                Err(broker_schedule::FireFailure::from_broker_error(&e))
            }
        }
    }
}

fn dispatch_config(settings: &ResolvedServeSettings) -> DispatchConfig {
    DispatchConfig {
        retry_defaults: settings.dispatch_retry.clone(),
        http_timeout_secs: settings.dispatch_http_timeout_secs,
        long_http_timeout_secs: settings.dispatch_long_http_timeout_secs,
        long_payload_threshold_bytes: 256 * 1024,
        ..DispatchConfig::default()
    }
}

fn shutdown_deadline_secs() -> u64 {
    std::env::var("BETTERMQ_SHUTDOWN_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30u64)
        .clamp(1, 300)
}

/// Start the fleet claim loop; fails when no broker URLs are configured.
fn start_fleet(
    settings: &ResolvedServeSettings,
) -> anyhow::Result<(broker_dispatch::FleetHandle, broker_dispatch::FleetDelivery)> {
    let client = broker_dispatch::LeaseClient::from_env().context(
        "dispatch fleet requires BETTERMQ_BROKER_URLS (comma-separated broker base URLs)",
    )?;
    let options = broker_dispatch::FleetOptions {
        on_claim_failure: Some(broker_api::record_fleet_claim_failure),
        ..broker_dispatch::FleetOptions::from_env()
    };
    info!(
        holder = %client.holder(),
        brokers = ?client.broker_urls(),
        concurrency = options.concurrency,
        claim_batch = options.claim_batch,
        "starting dispatch fleet workers"
    );
    let delivery = broker_dispatch::FleetDelivery::new(dispatch_config(settings));
    let handle = broker_dispatch::spawn_fleet(delivery.clone(), client, options);
    Ok((handle, delivery))
}

/// Stop claiming, let in-flight fleet jobs finish and report, then stop.
async fn stop_fleet(
    fleet: &broker_dispatch::FleetHandle,
    delivery: &broker_dispatch::FleetDelivery,
    deadline_secs: u64,
) {
    if fleet.shutdown(Duration::from_secs(deadline_secs)).await {
        info!("fleet drain complete");
    } else {
        tracing::warn!(
            in_flight = fleet.status().in_flight_jobs,
            "fleet drain deadline exceeded; unfinished leases expire and are redelivered"
        );
    }
    delivery.shutdown();
}

/// `--dispatch-fleet` / `--profile dispatch`: claim from remote brokers and
/// push webhooks. Opens no WAL, metadata, archive or retry state.
async fn serve_dispatch_fleet(settings: &ResolvedServeSettings) -> anyhow::Result<()> {
    let (fleet, delivery) = start_fleet(settings)?;
    let ready = fleet.clone();
    let live = fleet.clone();
    let app = axum::Router::new()
        // Liveness: the claim loop is still running.
        .route(
            "/healthz",
            get(move || {
                let running = live.status().running;
                async move {
                    if running {
                        (axum::http::StatusCode::OK, "ok")
                    } else {
                        (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            "fleet loop stopped",
                        )
                    }
                }
            }),
        )
        // Readiness: claiming and a broker answered within FLEET_READY_WINDOW.
        .route(
            "/readyz",
            get(move || {
                let status = ready.status();
                async move {
                    let code = if status.ready {
                        axum::http::StatusCode::OK
                    } else {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    };
                    (code, axum::Json(status))
                }
            }),
        )
        .layer(axum::middleware::from_fn(security::api_security_headers))
        .layer(TraceLayer::new_for_http());
    let listener = tokio::net::TcpListener::bind(settings.listen)
        .await
        .with_context(|| format!("bind {}", settings.listen))?;
    info!(
        listen = %settings.listen,
        "dispatch fleet mode: no broker WAL, RocksDB, archive, or retry state opened"
    );
    let deadline = shutdown_deadline_secs();
    let draining = fleet.clone();
    let shutdown = async move {
        shutdown_signal().await;
        begin_shutdown();
        // Stop claiming at once so /readyz turns unready while jobs finish.
        draining.begin_drain();
        info!("fleet shutdown signal received; draining in-flight jobs");
    };
    let served = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .context("fleet HTTP server exited");
    stop_fleet(&fleet, &delivery, deadline).await;
    served
}

#[cfg(test)]
mod bootstrap_tests {
    use super::*;

    #[test]
    fn checks_auth_parent_before_first_boot_setup() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("meta/local-auth.json");
        check_auth_directory(&path).unwrap();
        assert!(!path.exists());
        std::fs::write(root.path().join("file"), b"not a directory").unwrap();
        assert!(check_auth_directory(&root.path().join("file/local-auth.json")).is_err());
    }

    #[test]
    fn never_silently_accepts_a_weaker_controller_policy() {
        let policy = broker_config::ReplicationPolicyConfig {
            factor: 3,
            min_isr: 3,
        };
        assert!(validate_replication_policy(&policy, 3, 2).is_err());
        assert!(validate_replication_policy(&policy, 3, 3).is_ok());
        assert!(validate_replication_policy(&policy, 2, 2).is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn wal_group_flusher_commits_from_the_blocking_pool() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BrokerConfig::new(dir.path().to_path_buf());
        cfg.log.fsync = broker_storage::FsyncMode::Group;
        cfg.log.group_interval = Duration::from_millis(20);
        let broker = Broker::open(cfg).unwrap();
        let resp = broker
            .publish(PublishRequest {
                routing_id: None,
                topic: broker_partition::DIRECT_TOPIC.into(),
                routing_key: "k".into(),
                payload: "x".into(),
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: None,
                flow_id: None,
                queue_id: None,
                group_id: None,
                group_member_id: None,
                destination: None,
                flow: None,
                parallelism: None,
                max_retries: None,
                retry_backoff: None,
                method: None,
                headers: None,
                sign: None,
                request: None,
                url: Some("http://example.invalid/hook".into()),
                secret: Some("s".into()),
            })
            .unwrap();
        let shard = resp.partition.unwrap();
        assert_eq!(broker.committed_hwm(&resp.topic, shard).unwrap(), 0);

        let flusher = spawn_wal_group_flusher(broker.clone());
        tokio::time::timeout(Duration::from_secs(5), async {
            while broker.committed_hwm(&resp.topic, shard).unwrap() < 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("group flusher advances the commit HWM");
        flusher.abort();
    }

    fn local_state(dir: &tempfile::TempDir) -> Arc<AppState> {
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        Arc::new(AppState {
            schedule: ScheduleQueue::open(dir.path()).unwrap(),
            crons: CronRegistry::open(dir.path()).unwrap(),
            dispatch: DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default()),
            broker,
            leases: broker_dispatch::LeaseTable::new(),
            cluster: None,
            local_auth: None,
            fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
            catalog_tombstones: CatalogTombstones::open(dir.path()).unwrap(),
            dispatch_fleet: false,
            broker_only: true,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        })
    }

    fn scheduled(topic: &str) -> broker_schedule::ScheduledPublishRequest {
        broker_schedule::ScheduledPublishRequest {
            topic: topic.into(),
            routing_key: "rk".into(),
            payload: "body".into(),
            payload_encoding: None,
            idempotency_key: None,
            priority: None,
            parallelism: None,
            flow_id: None,
            queue_id: None,
            destination: None,
            flow: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn due_schedules_fire_in_batches_and_requeue_failures() {
        let dir = tempfile::tempdir().unwrap();
        let state = local_state(&dir);
        state
            .broker
            .create_subscription(broker_partition::CreateSubscriptionRequest {
                topic: "jobs".into(),
                url: "http://example.invalid/hook".into(),
                secret: "sec".into(),
                parallelism: None,
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();
        let jobs = SCHEDULE_COMPLETE_BATCH + 10;
        for _ in 0..jobs {
            state.schedule.schedule(scheduled("jobs"), 0).unwrap();
        }
        let missing = state
            .schedule
            .schedule(scheduled("no-such-queue"), 0)
            .unwrap();

        let now = Utc::now().timestamp_millis();
        fire_due_schedules(&state, state.schedule.pop_due(i64::MAX, usize::MAX), now).await;

        assert_eq!(
            state
                .broker
                .list_topic_messages("jobs", jobs * 2)
                .unwrap()
                .len(),
            jobs
        );
        let left = state.schedule.list();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].id, missing.id);
        // The failed job is back on the heap behind its backoff, not stuck in flight.
        assert!(state.schedule.pop_due(now, usize::MAX).is_empty());
        assert_eq!(state.schedule.pop_due(i64::MAX, usize::MAX).len(), 1);

        drop(state);
        let reopened = ScheduleQueue::open(dir.path()).unwrap();
        assert_eq!(reopened.list().len(), 1, "completions are durable");
    }

    #[test]
    fn log_format_parses_text_and_json_only() {
        assert_eq!(LogFormat::parse(""), Some(LogFormat::Text));
        assert_eq!(LogFormat::parse(" Text "), Some(LogFormat::Text));
        assert_eq!(LogFormat::parse("JSON"), Some(LogFormat::Json));
        assert_eq!(LogFormat::parse("yaml"), None);
    }

    #[tokio::test]
    async fn http_observability_assigns_and_echoes_request_ids() {
        use tower::ServiceExt;
        let app = axum::Router::new()
            .route("/ok", get(|| async { "ok" }))
            .with_http_observability();
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/ok")
                    .header("x-request-id", "from-gateway")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()["x-request-id"], "from-gateway");
    }
}

#[cfg(test)]
mod schedule_worker_tests {
    use super::*;

    fn one_node_cell_state(dir: &tempfile::TempDir) -> Arc<AppState> {
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let node = uuid::Uuid::new_v4();
        let config = broker_raft_meta::ClusterConfig {
            cluster_id: uuid::Uuid::new_v4(),
            nodes: vec![broker_raft_meta::NodeConfig {
                id: node,
                addr: "http://broker0:8080".into(),
                internal_url: None,
            }],
            node_id: node,
            generation: 1,
            hash_version: 1,
        };
        Arc::new(AppState {
            broker: broker.clone(),
            schedule: ScheduleQueue::open(dir.path()).unwrap(),
            crons: CronRegistry::open(dir.path()).unwrap(),
            dispatch: DispatchEngine::new_broker_only(broker, DispatchConfig::default()),
            leases: broker_dispatch::LeaseTable::new(),
            cluster: Some(Cluster::new(
                broker_raft_meta::ClusterRuntime::from_config_only(config),
            )),
            local_auth: None,
            fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
            catalog_tombstones: CatalogTombstones::open(dir.path()).unwrap(),
            dispatch_fleet: false,
            broker_only: true,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn legacy_delayed_job_fires_on_a_node_without_the_scheduler_lease() {
        let dir = tempfile::tempdir().unwrap();
        let state = one_node_cell_state(&dir);
        let cluster = state.cluster.as_ref().unwrap();
        assert!(!cluster.runtime.is_scheduler_leader());
        let scheduled = state
            .schedule
            .schedule(
                broker_schedule::ScheduledPublishRequest {
                    topic: broker_partition::DIRECT_TOPIC.into(),
                    routing_key: "legacy".into(),
                    payload: "body".into(),
                    payload_encoding: None,
                    idempotency_key: Some(format!("delay:{}", uuid::Uuid::new_v4())),
                    priority: None,
                    parallelism: None,
                    flow_id: None,
                    queue_id: None,
                    destination: Some(broker_partition::DestinationSnapshot {
                        queue_id: None,
                        url: "https://example.com/hook".into(),
                        secret: "secret".into(),
                    }),
                    flow: None,
                    max_retries: None,
                    retry_backoff: None,
                    method: None,
                    headers: None,
                    sign: None,
                    request: None,
                },
                0,
            )
            .unwrap();

        let now = scheduled.deliver_at_ms;
        fire_due_schedules(&state, state.schedule.pop_due(now, usize::MAX), now).await;

        assert_eq!(state.schedule.count(), 0, "fired and completed");
        let published = state
            .broker
            .list_topic_messages(broker_partition::DIRECT_TOPIC, 10)
            .unwrap();
        assert_eq!(published.len(), 1);
    }
}
