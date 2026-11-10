//! Unified blob store + ingest helpers.

use crate::{fs::FsBlobStore, PayloadError, PayloadRef};
use std::path::Path;
use uuid::Uuid;

#[cfg(feature = "s3")]
use crate::s3::S3BlobStore;

/// Default inline threshold — bodies larger than this go to blob storage.
pub fn inline_max_bytes() -> usize {
    std::env::var("BETTERMQ_INLINE_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256 * 1024)
}

/// Hard cap for externally stored payloads, including streaming uploads.
pub fn max_blob_bytes() -> u64 {
    std::env::var("BETTERMQ_MAX_PAYLOAD_BLOB_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256 * 1024 * 1024)
}

#[derive(Clone)]
pub enum BlobStore {
    Fs(FsBlobStore),
    #[cfg(feature = "s3")]
    S3(S3BlobStore),
}

impl BlobStore {
    pub fn open_local(data_dir: impl AsRef<Path>) -> Result<Self, PayloadError> {
        Ok(Self::Fs(FsBlobStore::open(
            data_dir.as_ref().join("payload-blobs"),
        )?))
    }

    #[cfg(feature = "s3")]
    pub fn open_s3(store: std::sync::Arc<dyn object_store::ObjectStore>) -> Self {
        Self::S3(S3BlobStore::new(store))
    }

    pub fn put_blob(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        data: &[u8],
    ) -> Result<PayloadRef, PayloadError> {
        let limit = max_blob_bytes();
        if data.len() as u64 > limit {
            return Err(PayloadError::TooLarge {
                size: data.len() as u64,
                limit,
            });
        }
        match self {
            Self::Fs(fs) => fs.put_blob(tenant_id, message_id, data),
            #[cfg(feature = "s3")]
            Self::S3(s3) => s3.put_blob(tenant_id, message_id, data),
        }
    }

    pub fn put_blob_owned(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        data: Vec<u8>,
    ) -> Result<PayloadRef, PayloadError> {
        let limit = max_blob_bytes();
        if data.len() as u64 > limit {
            return Err(PayloadError::TooLarge {
                size: data.len() as u64,
                limit,
            });
        }
        match self {
            Self::Fs(fs) => fs.put_blob_reader(tenant_id, message_id, std::io::Cursor::new(data)),
            #[cfg(feature = "s3")]
            Self::S3(s3) => s3.put_blob_owned(tenant_id, message_id, data),
        }
    }

    pub fn get_blob(&self, reference: &PayloadRef) -> Result<Vec<u8>, PayloadError> {
        match self {
            Self::Fs(fs) => fs.get_blob(reference),
            #[cfg(feature = "s3")]
            Self::S3(s3) => s3.get_blob(reference),
        }
    }

    /// Raw bytes of a node-local blob by message identity (peer repair).
    pub fn read_local_blob(
        &self,
        tenant_id: &str,
        message_id: Uuid,
    ) -> Result<Vec<u8>, PayloadError> {
        match self {
            Self::Fs(fs) => fs.read_key(&PayloadRef::key_for(tenant_id, message_id)?),
            #[cfg(feature = "s3")]
            Self::S3(_) => Err(PayloadError::Store(
                "payload blobs are shared in object storage; nothing local to serve".into(),
            )),
        }
    }

    /// Check that the stored blob exists and matches its reference (size,
    /// and sha256 when the reference has one).
    pub fn verify_blob(&self, reference: &PayloadRef) -> Result<(), PayloadError> {
        let size = self.get_blob_to_writer(reference, &mut std::io::sink())?;
        if size != reference.size {
            return Err(PayloadError::Mismatch(format!(
                "{}: {size} bytes, reference says {}",
                reference.bucket_key, reference.size
            )));
        }
        Ok(())
    }

    /// Durably install bytes fetched from a peer for `reference`, after
    /// verifying them against it.
    pub fn install_blob(&self, reference: &PayloadRef, data: &[u8]) -> Result<(), PayloadError> {
        crate::fs::verify_bytes(reference, data)?;
        match self {
            Self::Fs(fs) => fs.install(reference, data),
            #[cfg(feature = "s3")]
            Self::S3(_) => Err(PayloadError::Store(
                "payload blobs are shared in object storage; nothing to repair".into(),
            )),
        }
    }

    /// Delete a blob that retention has proven unreferenced. Returns whether
    /// it existed; deleting an absent blob succeeds.
    pub fn delete_blob(&self, reference: &PayloadRef) -> Result<bool, PayloadError> {
        match self {
            Self::Fs(fs) => fs.delete_blob(reference),
            #[cfg(feature = "s3")]
            Self::S3(s3) => s3.delete_blob(reference),
        }
    }

    pub fn get_blob_to_writer<W: std::io::Write + Unpin>(
        &self,
        reference: &PayloadRef,
        writer: &mut W,
    ) -> Result<u64, PayloadError> {
        match self {
            Self::Fs(fs) => fs.get_blob_to_writer(reference, writer),
            #[cfg(feature = "s3")]
            Self::S3(s3) => s3.get_blob_to_writer(reference, writer),
        }
    }
}

/// Where message bodies may live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadPolicy {
    /// Bodies above [`inline_max_bytes`] go to the blob store (single node,
    /// or a blob store every replica shares).
    #[default]
    External,
    /// Local-WAL multi-node cell: blobs would exist only on the accepting
    /// node, so every body stays inline in its WAL frame (and therefore in
    /// replication and catch-up). Bodies above `max_body` are rejected.
    InlineOnly { max_body: usize },
}

impl PayloadPolicy {
    /// Reject a body before any append or blob write.
    pub fn check(&self, size: usize) -> Result<(), PayloadError> {
        match *self {
            Self::InlineOnly { max_body } if size > max_body => {
                Err(PayloadError::TooLargeForCell {
                    size: size as u64,
                    limit: max_body as u64,
                })
            }
            _ => Ok(()),
        }
    }
}

/// Split payload for log append: inline bytes + optional external ref JSON.
pub fn prepare_for_log(
    store: &BlobStore,
    tenant_id: &str,
    message_id: Uuid,
    payload: Vec<u8>,
) -> Result<(Vec<u8>, Option<String>), PayloadError> {
    prepare_for_log_with(
        store,
        PayloadPolicy::External,
        tenant_id,
        message_id,
        payload,
    )
}

/// [`prepare_for_log`] under an explicit [`PayloadPolicy`].
pub fn prepare_for_log_with(
    store: &BlobStore,
    policy: PayloadPolicy,
    tenant_id: &str,
    message_id: Uuid,
    payload: Vec<u8>,
) -> Result<(Vec<u8>, Option<String>), PayloadError> {
    policy.check(payload.len())?;
    if matches!(policy, PayloadPolicy::InlineOnly { .. }) {
        return Ok((payload, None));
    }
    let inline_max = inline_max_bytes();
    if payload.len() <= inline_max {
        return Ok((payload, None));
    }
    let reference = store.put_blob_owned(tenant_id, message_id, payload)?;
    let json = serde_json::to_string(&reference).map_err(|e| PayloadError::Store(e.to_string()))?;
    Ok((Vec::new(), Some(json)))
}

pub fn hydrate_payload(
    store: &BlobStore,
    payload: &mut Vec<u8>,
    payload_ref_json: Option<&str>,
) -> Result<(), PayloadError> {
    let Some(json) = payload_ref_json else {
        return Ok(());
    };
    if !payload.is_empty() {
        return Ok(());
    }
    let reference: PayloadRef =
        serde_json::from_str(json).map_err(|e| PayloadError::Store(e.to_string()))?;
    *payload = store.get_blob(&reference)?;
    Ok(())
}
