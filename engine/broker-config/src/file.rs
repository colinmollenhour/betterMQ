use crate::types::{BetterMqConfig, ConfigError};
use std::path::Path;

/// Optional per-host overrides when the same cluster config file is mounted on every node.
pub fn apply_node_env_overrides(cfg: &mut BetterMqConfig) {
    if let Ok(url) = std::env::var("BETTERMQ_INTERNAL_URL") {
        if !url.trim().is_empty() {
            cfg.node.internal_url = Some(url.trim().trim_end_matches('/').to_owned());
        }
    }
    if let Ok(name) = std::env::var("BETTERMQ_NODE_NAME") {
        let name = name.trim();
        if !name.is_empty() {
            cfg.node.name = name.to_string();
        }
    }
    if let Ok(url) = std::env::var("BETTERMQ_NODE_PUBLIC_URL") {
        let url = url.trim();
        if !url.is_empty() {
            cfg.node.public_url = url.to_string();
        }
    }
    if let Some(cluster) = &mut cfg.cluster {
        if let Some(node) = cluster.nodes.iter_mut().find(|n| n.name == cfg.node.name) {
            node.internal_url = cfg
                .node
                .internal_url
                .clone()
                .or_else(|| node.internal_url.clone());
        }
    }
}

pub fn load_config(path: &Path) -> Result<BetterMqConfig, ConfigError> {
    let bytes = std::fs::read(path)?;
    let mut cfg: BetterMqConfig = serde_json::from_slice(&bytes)?;
    apply_node_env_overrides(&mut cfg);
    cfg.validate()?;
    Ok(cfg)
}

pub fn write_config(path: &Path, cfg: &BetterMqConfig) -> Result<(), ConfigError> {
    cfg.validate()?;
    let bytes = serde_json::to_vec_pretty(cfg)?;
    broker_proto::write_secret_atomic(path, &bytes)?;
    Ok(())
}
