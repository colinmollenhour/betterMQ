//! Fixed, versioned producer-admission stripes on the coherent shared volume.
//! Lock inodes are never renamed/unlinked. Nonblocking acquisition lets async
//! callers wait without occupying the native I/O budget needed by the owner.

use crate::shared_indexes::SharedIndexError;
use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::path::Path;

const STRIPES: u16 = 4096;

pub struct ProducerLocks {
    // Closing the final file handle releases flock, including process exit.
    _files: Vec<File>,
}

fn stripe(tenant: &str, key: &str) -> u16 {
    let mut hash = Sha256::new();
    hash.update((tenant.len() as u64).to_be_bytes());
    hash.update(tenant.as_bytes());
    hash.update(key.as_bytes());
    let hash = hash.finalize();
    u16::from_be_bytes([hash[0], hash[1]]) % STRIPES
}

pub(crate) fn try_lock(
    dir: &Path,
    tenant: &str,
    keys: &[String],
) -> Result<Option<ProducerLocks>, SharedIndexError> {
    let mut stripes: Vec<_> = keys.iter().map(|key| stripe(tenant, key)).collect();
    stripes.sort_unstable();
    stripes.dedup();
    let root = dir.join("producer-admission-v1-4096");
    std::fs::create_dir_all(&root)?;
    let mut files = Vec::with_capacity(stripes.len());
    for stripe in stripes {
        let path = root.join(format!("{stripe:04x}.lock"));
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        crate::set_secret_file_mode(&path);
        match file.try_lock_exclusive() {
            Ok(()) => files.push(file),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(Some(ProducerLocks { _files: files }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn colliding_batch_keys_share_one_owned_inode_and_never_recursively_lock() {
        let dir = tempfile::tempdir().unwrap();
        let mut seen = std::collections::HashMap::new();
        let (first, collision) = (0..=STRIPES)
            .find_map(|n| {
                let key = format!("key-{n}");
                seen.insert(stripe("tenant", &key), key.clone())
                    .map(|previous| (previous, key))
            })
            .unwrap();
        let held = try_lock(dir.path(), "tenant", std::slice::from_ref(&first))
            .unwrap()
            .unwrap();
        assert!(
            try_lock(dir.path(), "tenant", std::slice::from_ref(&collision))
                .unwrap()
                .is_none()
        );
        drop(held);
        let held = try_lock(dir.path(), "tenant", &[first.clone(), collision, first])
            .unwrap()
            .unwrap();
        assert_eq!(held._files.len(), 1);
        drop(held);
        assert_eq!(
            std::fs::read_dir(dir.path().join("producer-admission-v1-4096"))
                .unwrap()
                .count(),
            1,
            "released lock inodes must remain, never be replaced/unlinked"
        );
    }

    #[test]
    fn busy_batch_drops_every_partial_lock_before_async_retry() {
        let dir = tempfile::tempdir().unwrap();
        let mut keys: Vec<_> = (0..100).map(|n| format!("key-{n}")).collect();
        keys.sort_by_key(|key| stripe("tenant", key));
        let (low, high) = (keys.first().unwrap(), keys.last().unwrap());
        assert_ne!(stripe("tenant", low), stripe("tenant", high));
        let high_held = try_lock(dir.path(), "tenant", std::slice::from_ref(high))
            .unwrap()
            .unwrap();
        assert!(try_lock(dir.path(), "tenant", &[high.clone(), low.clone()])
            .unwrap()
            .is_none());
        let low_held = try_lock(dir.path(), "tenant", std::slice::from_ref(low))
            .unwrap()
            .unwrap();
        drop(low_held);
        drop(high_held);
        assert!(try_lock(dir.path(), "tenant", &[low.clone(), high.clone()])
            .unwrap()
            .is_some());
    }
}
