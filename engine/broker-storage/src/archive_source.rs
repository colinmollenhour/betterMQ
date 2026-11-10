//! Stable source identities for sealed-segment archives.

use crate::archive::ArchiveError;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveSource {
    pub cluster_id: String,
    pub node_id: String,
}

impl ArchiveSource {
    pub fn new(cluster_id: String, node_id: String) -> Result<Self, ArchiveError> {
        for (name, value) in [("cluster", &cluster_id), ("node", &node_id)] {
            if value.is_empty()
                || value.len() > 128
                || value == "."
                || value == ".."
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
            {
                return Err(ArchiveError::Configuration(format!(
                    "archive {name} identity must be 1-128 ASCII letters, digits, '.', '-' or '_' (not '.' or '..')"
                )));
            }
        }
        Ok(Self {
            cluster_id,
            node_id,
        })
    }

    pub fn parse(value: &str) -> Result<Self, ArchiveError> {
        let (cluster, node) = value.split_once('/').ok_or_else(|| {
            ArchiveError::Configuration("archive source must be CLUSTER/NODE".into())
        })?;
        Self::new(cluster.into(), node.into())
    }

    pub(crate) fn namespace(&self) -> PathBuf {
        PathBuf::from("clusters")
            .join(&self.cluster_id)
            .join("nodes")
            .join(&self.node_id)
    }

    pub(crate) fn object_key(
        &self,
        relative: &Path,
        sha256: &str,
    ) -> Result<PathBuf, ArchiveError> {
        // Also validate deserialized identities before using them as paths.
        Self::new(self.cluster_id.clone(), self.node_id.clone())?;
        if sha256.len() != 64
            || !sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ArchiveError::Manifest("invalid SHA-256 identity".into()));
        }
        let relative = crate::archive::safe_archive_key(&relative.to_string_lossy())?;
        Ok(self.namespace().join("objects").join(sha256).join(relative))
    }

    pub(crate) fn from_env() -> Result<Self, ArchiveError> {
        let cluster = std::env::var("BETTERMQ_ARCHIVE_CLUSTER_ID").ok();
        let node = std::env::var("BETTERMQ_ARCHIVE_NODE_ID").ok();
        match (cluster, node) {
            (Some(cluster), Some(node)) => return Self::new(cluster, node),
            (None, None) => {}
            _ => {
                return Err(ArchiveError::Configuration(
                    "set both BETTERMQ_ARCHIVE_CLUSTER_ID and BETTERMQ_ARCHIVE_NODE_ID, or neither"
                        .into(),
                ))
            }
        }
        let root = std::env::var_os("BETTERMQ_DATA_DIR").map(PathBuf::from).ok_or_else(|| {
            ArchiveError::Configuration("archive source identity requires BETTERMQ_DATA_DIR or explicit cluster/node IDs".into())
        })?;
        let clustered = std::env::var("BETTERMQ_CLUSTER")
            .is_ok_and(|value| matches!(value.to_lowercase().as_str(), "true" | "1"));
        Self::configured(&root, clustered)
    }

    fn configured(root: &Path, clustered: bool) -> Result<Self, ArchiveError> {
        let config = root.join("cluster-config.json");
        if config.exists() {
            let source: Self =
                serde_json::from_slice(&std::fs::read(config)?).map_err(|error| {
                    ArchiveError::Configuration(format!("cluster archive identity: {error}"))
                })?;
            return Self::new(source.cluster_id, source.node_id);
        }
        if clustered {
            return Err(ArchiveError::Configuration(
                "cluster archiving requires cluster-config.json or explicit archive cluster/node IDs".into(),
            ));
        }
        Self::local(root)
    }

    fn local(root: &Path) -> Result<Self, ArchiveError> {
        std::fs::create_dir_all(root)?;
        let path = root.join("archive-source.json");
        let _lock = crate::flock::FileLock::exclusive(&path)?;
        if path.exists() {
            let source: Self = serde_json::from_slice(&std::fs::read(path)?)
                .map_err(|error| ArchiveError::Configuration(error.to_string()))?;
            return Self::new(source.cluster_id, source.node_id);
        }
        let source = Self::new(
            uuid::Uuid::new_v4().to_string(),
            uuid::Uuid::new_v4().to_string(),
        )?;
        crate::atomic_write_file(
            &path,
            &serde_json::to_vec(&source)
                .map_err(|error| ArchiveError::Configuration(error.to_string()))?,
        )?;
        Ok(source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_source_is_stable_and_isolated() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        assert_eq!(
            ArchiveSource::local(first.path()).unwrap(),
            ArchiveSource::local(first.path()).unwrap()
        );
        assert_ne!(
            ArchiveSource::local(first.path()).unwrap(),
            ArchiveSource::local(second.path()).unwrap()
        );
    }

    #[test]
    fn source_is_a_safe_explicit_namespace() {
        let source = ArchiveSource::parse("production/broker-1").unwrap();
        assert_eq!(
            source.namespace(),
            PathBuf::from("clusters/production/nodes/broker-1")
        );
        for invalid in ["", "one", "a/..", "../a", "a/b/c", "a/", "/b", "a/b\\c"] {
            assert!(ArchiveSource::parse(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn immutable_keys_include_source_and_content() {
        let first = ArchiveSource::parse("cluster/first").unwrap();
        let second = ArchiveSource::parse("cluster/second").unwrap();
        let segment = Path::new("shards/7/segments/segment-000001.log");
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        assert_ne!(
            first.object_key(segment, &a).unwrap(),
            second.object_key(segment, &a).unwrap()
        );
        assert_ne!(
            first.object_key(segment, &a).unwrap(),
            first.object_key(segment, &b).unwrap()
        );
        assert!(first.object_key(segment, "../bad").is_err());
        assert!(first.object_key(Path::new("../bad"), &a).is_err());
    }

    #[test]
    fn clustered_sources_require_and_reuse_durable_node_identity() {
        let root = tempfile::tempdir().unwrap();
        assert!(ArchiveSource::configured(root.path(), true).is_err());
        let expected = ArchiveSource::parse("configured-cluster/configured-node").unwrap();
        std::fs::write(
            root.path().join("cluster-config.json"),
            serde_json::to_vec(&expected).unwrap(),
        )
        .unwrap();
        assert_eq!(
            ArchiveSource::configured(root.path(), true).unwrap(),
            expected
        );
        std::fs::write(root.path().join("cluster-config.json"), b"corrupt").unwrap();
        assert!(ArchiveSource::configured(root.path(), true).is_err());
    }
}
