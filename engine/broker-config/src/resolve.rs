use crate::components::{
    resolve_component_set, resolve_listeners, resolve_panel_mode, Component, ComponentSet,
    PanelMode, ReplicationPolicyConfig, ResolvedListeners,
};
use crate::types::{
    parse_listen, AuthConfig, BetterMqConfig, ConfigError, S3Config, StorageConfig,
};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// CLI / env overrides applied on top of `bettermq.json`.
#[derive(Debug, Clone, Default)]
pub struct ServeOverrides {
    pub config_path: Option<PathBuf>,
    pub listen: Option<SocketAddr>,
    /// Shorthand for `0.0.0.0:{port}` when `--listen` is not set.
    pub port: Option<u16>,
    pub data_dir: Option<PathBuf>,
    pub cluster: Option<bool>,
    pub database_url: Option<String>,
    pub dispatch_fleet: Option<bool>,
    pub broker_only: Option<bool>,
    pub gateway_only: Option<bool>,
    pub panel_listen: Option<SocketAddr>,
    pub admin_listen: Option<SocketAddr>,
    pub internal_listen: Option<SocketAddr>,
    pub profile: Option<String>,
    pub components: Vec<String>,
    pub no_panel: bool,
    pub standalone_panel: bool,
    pub controller_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedServeSettings {
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub storage: StorageMode,
    pub s3: Option<S3Config>,
    pub auth: ResolvedAuth,
    pub cluster_enabled: bool,
    pub cluster_shared_meta_dir: Option<PathBuf>,
    pub dispatch_http_timeout_secs: u64,
    pub dispatch_long_http_timeout_secs: u64,
    pub dispatch_retry: broker_proto::RetryDefaults,
    pub dispatch_fleet: bool,
    pub broker_only: bool,
    pub gateway_only: bool,
    pub panel_listen: Option<SocketAddr>,
    pub components: ComponentSet,
    pub panel_mode: PanelMode,
    pub listeners: ResolvedListeners,
    pub replication: ReplicationPolicyConfig,
    pub controller_url: Option<String>,
    pub opens_local_storage: bool,
}

impl ResolvedServeSettings {
    pub fn has(&self, component: Component) -> bool {
        self.components.contains(component)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    Local,
    Slate,
}

#[derive(Debug, Clone)]
pub enum ResolvedAuth {
    Local { auth_file: Option<PathBuf> },
    Cloud { database_url: String },
}

impl ResolvedServeSettings {
    /// Set process env vars consumed by existing broker crates (backward compatible).
    pub fn apply_env(&self) {
        std::env::set_var("BETTERMQ_DATA_DIR", &self.data_dir);
        let storage = match self.storage {
            StorageMode::Local => "local",
            StorageMode::Slate => "slate",
        };
        std::env::set_var("BETTERMQ_STORAGE", storage);

        if let Some(s3) = &self.s3 {
            std::env::set_var("S3_ENDPOINT", &s3.endpoint);
            std::env::set_var("S3_BUCKET", &s3.bucket);
            std::env::set_var("S3_ACCESS_KEY", &s3.access_key);
            std::env::set_var("S3_SECRET_KEY", &s3.secret_key);
            std::env::set_var("S3_REGION", &s3.region);
            if let Some(pb) = &s3.payload_bucket {
                std::env::set_var("S3_PAYLOAD_BUCKET", pb);
            }
        }

        std::env::set_var(
            "BETTERMQ_HTTP_TIMEOUT_SECS",
            self.dispatch_http_timeout_secs.to_string(),
        );
        std::env::set_var(
            "BETTERMQ_LONG_HTTP_TIMEOUT_SECS",
            self.dispatch_long_http_timeout_secs.to_string(),
        );

        if let Some(dir) = &self.cluster_shared_meta_dir {
            std::env::set_var("BETTERMQ_SHARED_META_DIR", dir);
        }

        if let ResolvedAuth::Local {
            auth_file: Some(path),
        } = &self.auth
        {
            std::env::set_var("BETTERMQ_LOCAL_AUTH_FILE", path);
        }
    }
}

pub fn resolve_serve(
    file_cfg: Option<&BetterMqConfig>,
    overrides: &ServeOverrides,
) -> Result<ResolvedServeSettings, ConfigError> {
    resolve_serve_with_env(file_cfg, overrides, |key| std::env::var(key).ok())
}

fn resolve_serve_with_env(
    file_cfg: Option<&BetterMqConfig>,
    overrides: &ServeOverrides,
    env: impl Fn(&str) -> Option<String>,
) -> Result<ResolvedServeSettings, ConfigError> {
    let mut base = file_cfg
        .cloned()
        .unwrap_or_else(BetterMqConfig::template_single_local);
    apply_runtime_env(&mut base, &env)?;

    let configured_public = base
        .listeners
        .as_ref()
        .and_then(|s| s.public.as_deref())
        .map(parse_listen)
        .transpose()?;
    let listen = overrides
        .listen
        .or_else(|| {
            overrides
                .port
                .map(|port| SocketAddr::from(([0, 0, 0, 0], port)))
        })
        .or(configured_public)
        .or_else(|| parse_listen(&base.node.listen).ok())
        .unwrap_or_else(|| "0.0.0.0:8080".parse().expect("default listen"));

    let data_dir = overrides
        .data_dir
        .clone()
        .unwrap_or_else(|| base.data_dir.clone());

    let cluster_enabled = overrides.cluster.unwrap_or_else(|| base.cluster_enabled());

    let (storage, s3) = match &base.storage {
        StorageConfig::Local => (StorageMode::Local, None),
        StorageConfig::Slate { s3 } => (StorageMode::Slate, Some(s3.clone())),
    };

    let auth = match (&base.auth, &overrides.database_url) {
        (_, Some(url)) if !url.is_empty() => ResolvedAuth::Cloud {
            database_url: url.clone(),
        },
        (AuthConfig::Cloud { database_url }, _) => ResolvedAuth::Cloud {
            database_url: database_url.clone(),
        },
        (AuthConfig::Local { auth_file }, _) => ResolvedAuth::Local {
            auth_file: auth_file.clone(),
        },
    };

    let cluster_shared_meta_dir = base
        .cluster
        .as_ref()
        .and_then(|c| c.shared_meta_dir.clone());

    let components = resolve_component_set(
        &base,
        overrides.profile.as_deref(),
        &overrides.components,
        overrides.broker_only.unwrap_or(false),
        overrides.gateway_only.unwrap_or(false),
        overrides.dispatch_fleet.unwrap_or(false),
        overrides.standalone_panel,
    )?;
    let panel_mode = resolve_panel_mode(
        &base,
        overrides.panel_listen,
        overrides.no_panel,
        overrides.standalone_panel,
    )?;
    let mut panel_listen = overrides.panel_listen.or_else(|| {
        base.panel
            .as_ref()
            .and_then(|section| section.listen.as_deref())
            .and_then(|value| parse_listen(value).ok())
    });
    if panel_mode == PanelMode::Disabled {
        panel_listen = None;
    }
    let admin_listen = overrides.admin_listen.or_else(|| {
        base.listeners
            .as_ref()
            .and_then(|section| section.admin.as_deref())
            .and_then(|value| parse_listen(value).ok())
    });
    let internal_listen = overrides.internal_listen.or_else(|| {
        base.listeners
            .as_ref()
            .and_then(|section| section.internal.as_deref())
            .and_then(|value| parse_listen(value).ok())
    });
    let listeners = resolve_listeners(
        listen,
        admin_listen,
        internal_listen,
        panel_listen,
        panel_mode,
    );
    listeners.validate_exposure(components)?;
    let replication = base.replication.clone().unwrap_or_else(|| {
        if cluster_enabled {
            let count = base.cluster.as_ref().map_or(0, |c| c.nodes.len());
            let (factor, min_isr) = broker_raft_meta::replication_policy(count);
            ReplicationPolicyConfig { factor, min_isr }
        } else {
            ReplicationPolicyConfig {
                factor: 1,
                min_isr: 1,
            }
        }
    });
    let controller_url = overrides.controller_url.clone().or_else(|| {
        base.panel
            .as_ref()
            .and_then(|section| section.controller.clone())
    });

    Ok(ResolvedServeSettings {
        listen,
        data_dir,
        storage,
        s3,
        auth,
        cluster_enabled,
        cluster_shared_meta_dir,
        dispatch_http_timeout_secs: base.dispatch.http_timeout_secs,
        dispatch_long_http_timeout_secs: base.dispatch.long_http_timeout_secs,
        dispatch_retry: base.dispatch.retry.clone(),
        dispatch_fleet: overrides.dispatch_fleet.unwrap_or(false)
            || (components.contains(Component::Dispatch)
                && !components.contains(Component::Broker)),
        broker_only: overrides.broker_only.unwrap_or(false)
            || (components.contains(Component::Broker)
                && !components.contains(Component::Dispatch)),
        gateway_only: overrides.gateway_only.unwrap_or(false)
            || (components.contains(Component::Gateway) && !components.contains(Component::Broker)),
        panel_listen,
        components,
        panel_mode,
        listeners,
        replication,
        controller_url,
        opens_local_storage: components.requires_local_storage(),
    })
}

// Dispatch consumes typed values; storage crates still consume normalized S3 env.
fn apply_runtime_env(
    cfg: &mut BetterMqConfig,
    env: &impl Fn(&str) -> Option<String>,
) -> Result<(), ConfigError> {
    if let Some(mode) = env("BETTERMQ_STORAGE") {
        cfg.storage = match mode.as_str() {
            "local" | "rocksdb" | "wal" => StorageConfig::Local,
            "slate" => match &cfg.storage {
                StorageConfig::Slate { .. } => cfg.storage.clone(),
                StorageConfig::Local => StorageConfig::Slate {
                    s3: S3Config {
                        endpoint: String::new(),
                        bucket: String::new(),
                        payload_bucket: None,
                        access_key: String::new(),
                        secret_key: String::new(),
                        region: "auto".into(),
                    },
                },
            },
            _ => {
                return Err(ConfigError::Invalid(format!(
                    "invalid BETTERMQ_STORAGE '{mode}'"
                )))
            }
        };
    }
    if let StorageConfig::Slate { s3 } = &mut cfg.storage {
        for (key, field) in [
            ("S3_ENDPOINT", &mut s3.endpoint),
            ("S3_BUCKET", &mut s3.bucket),
            ("S3_ACCESS_KEY", &mut s3.access_key),
            ("S3_SECRET_KEY", &mut s3.secret_key),
            ("S3_REGION", &mut s3.region),
        ] {
            if let Some(value) = env(key) {
                *field = value;
            }
        }
        if let Some(value) = env("S3_PAYLOAD_BUCKET") {
            s3.payload_bucket = Some(value);
        }
        crate::types::validate_s3(s3)?;
    }
    for (key, field) in [
        (
            "BETTERMQ_HTTP_TIMEOUT_SECS",
            &mut cfg.dispatch.http_timeout_secs,
        ),
        (
            "BETTERMQ_LONG_HTTP_TIMEOUT_SECS",
            &mut cfg.dispatch.long_http_timeout_secs,
        ),
    ] {
        if let Some(value) = env(key) {
            *field =
                value.parse().ok().filter(|n| *n > 0).ok_or_else(|| {
                    ConfigError::Invalid(format!("{key} must be a positive integer"))
                })?;
        }
    }
    if let Some(value) = env("BETTERMQ_DEFAULT_MAX_RETRIES") {
        cfg.dispatch.retry.max_retries = value.parse().map_err(|_| {
            ConfigError::Invalid("BETTERMQ_DEFAULT_MAX_RETRIES must be an unsigned integer".into())
        })?;
    }
    Ok(())
}

pub fn resolve_from_path(
    path: &Path,
    overrides: &ServeOverrides,
) -> Result<ResolvedServeSettings, ConfigError> {
    let cfg = crate::file::load_config(path)?;
    resolve_serve(Some(&cfg), overrides)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::{Component, PanelMode};

    #[test]
    fn default_serve_opens_storage_with_embedded_panel() {
        let settings = resolve_serve(None, &ServeOverrides::default()).unwrap();
        assert!(settings.opens_local_storage);
        assert_eq!(settings.panel_mode, PanelMode::Embedded);
        assert!(settings.has(Component::Broker));
        assert!(settings.has(Component::Panel));
        assert_eq!(settings.replication.factor, 1);
    }

    #[test]
    fn panel_and_gateway_profiles_do_not_open_storage() {
        let panel = resolve_serve(
            None,
            &ServeOverrides {
                standalone_panel: true,
                ..ServeOverrides::default()
            },
        )
        .unwrap();
        assert!(!panel.opens_local_storage);
        assert_eq!(panel.panel_mode, PanelMode::Standalone);

        let gateway = resolve_serve(
            None,
            &ServeOverrides {
                gateway_only: Some(true),
                ..ServeOverrides::default()
            },
        )
        .unwrap();
        assert!(!gateway.opens_local_storage);
        assert!(gateway.gateway_only);
    }

    #[test]
    fn cluster_defaults_to_rf3() {
        let mut cfg = crate::types::BetterMqConfig::template_cluster_local();
        cfg.cluster.as_mut().unwrap().nodes.truncate(3);
        let settings = resolve_serve(
            Some(&cfg),
            &ServeOverrides {
                cluster: Some(true),
                ..ServeOverrides::default()
            },
        )
        .unwrap();
        assert_eq!(settings.replication.factor, 3);
        assert_eq!(settings.replication.min_isr, 2);
    }

    #[test]
    fn public_listener_overrides_legacy_node_listen_but_not_cli() {
        let cfg: BetterMqConfig = serde_json::from_value(serde_json::json!({
            "version": 2, "node": {"name": "test", "listen": "0.0.0.0:8080", "publicUrl": "http://localhost:9000"},
            "listeners": {"public": "127.0.0.1:9000"}
        })).unwrap();
        let resolved = resolve_serve(Some(&cfg), &ServeOverrides::default()).unwrap();
        assert_eq!(resolved.listen, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(resolved.listeners.public, resolved.listen);
        let resolved = resolve_serve(
            Some(&cfg),
            &ServeOverrides {
                port: Some(9100),
                ..ServeOverrides::default()
            },
        )
        .unwrap();
        assert_eq!(resolved.listen, "0.0.0.0:9100".parse().unwrap());
    }

    #[test]
    fn storage_and_dispatch_env_override_file_and_defaults() {
        let env = |key: &str| {
            match key {
                "BETTERMQ_STORAGE" => Some("slate"),
                "S3_ENDPOINT" => Some("http://object-store:9000"),
                "S3_BUCKET" => Some("messages"),
                "S3_PAYLOAD_BUCKET" => Some("payloads"),
                "S3_ACCESS_KEY" => Some("access"),
                "S3_SECRET_KEY" => Some("secret"),
                "BETTERMQ_HTTP_TIMEOUT_SECS" => Some("5"),
                "BETTERMQ_LONG_HTTP_TIMEOUT_SECS" => Some("60"),
                "BETTERMQ_DEFAULT_MAX_RETRIES" => Some("10"),
                _ => None,
            }
            .map(str::to_owned)
        };
        for cfg in [None, Some(BetterMqConfig::template_single_local())] {
            let resolved =
                resolve_serve_with_env(cfg.as_ref(), &ServeOverrides::default(), env).unwrap();
            assert_eq!(resolved.storage, StorageMode::Slate);
            assert_eq!(resolved.s3.unwrap().bucket, "messages");
            assert_eq!(resolved.dispatch_http_timeout_secs, 5);
            assert_eq!(resolved.dispatch_long_http_timeout_secs, 60);
            assert_eq!(resolved.dispatch_retry.max_retries, 10);
        }
    }

    #[test]
    fn invalid_runtime_env_fails_instead_of_silently_defaulting() {
        for (key, value) in [
            ("BETTERMQ_STORAGE", "typo"),
            ("BETTERMQ_STORAGE", "slate"),
            ("BETTERMQ_HTTP_TIMEOUT_SECS", "0"),
            ("BETTERMQ_DEFAULT_MAX_RETRIES", "no"),
        ] {
            assert!(
                resolve_serve_with_env(None, &ServeOverrides::default(), |k| (k == key)
                    .then(|| value.into()))
                .is_err()
            );
        }
    }
}
