//! Exclusive admission before opening SlateDB. Markers are deliberately retained
//! on shutdown/crash; a different node needs an explicit compare-and-swap takeover.

use crate::slate_indexes::SlateIndexError;
use object_store::{path::Path as ObjectPath, ObjectStore, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

const OWNER_PATH: &str = "bettermq/_indexes.owner";

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SlateOwner {
    pub version: u32,
    pub node_id: Uuid,
    pub process_token: Uuid,
    pub claimed_at: u64,
}

pub(crate) fn stable_node_id(data_dir: &Path) -> Result<Uuid, SlateIndexError> {
    std::fs::create_dir_all(data_dir)?;
    let path = data_dir.join("slate-node-id");
    let read = || -> Result<Uuid, SlateIndexError> {
        Uuid::parse_str(std::fs::read_to_string(&path)?.trim())
            .map_err(|error| SlateIndexError::Slate(format!("invalid slate-node-id: {error}")))
    };
    if path.exists() {
        return read();
    }
    let node_id = Uuid::new_v4();
    let tmp = data_dir.join(format!(".slate-node-id-{}", Uuid::new_v4()));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)?;
    writeln!(file, "{node_id}")?;
    file.sync_all()?;
    drop(file);
    let result = std::fs::hard_link(&tmp, &path);
    let _ = std::fs::remove_file(&tmp);
    match result {
        Ok(()) => {
            std::fs::File::open(data_dir)?.sync_all()?;
            Ok(node_id)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => read(),
        Err(error) => Err(error.into()),
    }
}

pub(crate) async fn claim(
    store: Arc<dyn ObjectStore>,
    node_id: Uuid,
    takeover: Option<Uuid>,
) -> Result<SlateOwner, SlateIndexError> {
    let owner = SlateOwner {
        version: 1,
        node_id,
        process_token: Uuid::new_v4(),
        claimed_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64,
    };
    let bytes =
        serde_json::to_vec(&owner).map_err(|error| SlateIndexError::Slate(error.to_string()))?;
    let path = ObjectPath::from(OWNER_PATH);
    match store
        .put_opts(
            &path,
            bytes.clone().into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => return Ok(owner),
        Err(object_store::Error::AlreadyExists { .. }) => {}
        // Ambiguous/unsupported conditional writes must never open SlateDB.
        Err(error) => {
            return Err(SlateIndexError::Slate(format!(
                "cannot atomically claim Slate ownership: {error}"
            )))
        }
    }
    let result = store
        .get(&path)
        .await
        .map_err(|error| SlateIndexError::Slate(error.to_string()))?;
    let version = UpdateVersion {
        e_tag: result.meta.e_tag.clone(),
        version: result.meta.version.clone(),
    };
    let previous: SlateOwner = serde_json::from_slice(
        &result
            .bytes()
            .await
            .map_err(|error| SlateIndexError::Slate(error.to_string()))?,
    )
    .map_err(|error| SlateIndexError::Slate(format!("invalid Slate owner marker: {error}")))?;
    if previous.version != 1 || (previous.node_id != node_id && takeover != Some(previous.node_id))
    {
        return Err(SlateIndexError::OwnedByOtherNode {
            owner: previous.node_id,
            claimed_at: previous.claimed_at,
        });
    }
    if version.e_tag.is_none() {
        return Err(SlateIndexError::Slate(
            "Slate owner marker has no ETag; conditional takeover/restart is unsupported".into(),
        ));
    }
    store
        .put_opts(
            &path,
            bytes.into(),
            PutOptions {
                mode: PutMode::Update(version),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| {
            SlateIndexError::Slate(format!(
                "Slate ownership changed or CAS is unsupported; refusing to open: {error}"
            ))
        })?;
    if previous.node_id != node_id {
        tracing::warn!(previous_owner = %previous.node_id, new_owner = %node_id, "explicit Slate ownership takeover; stop the previous broker");
    }
    Ok(owner)
}
