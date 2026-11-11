//! Password-protected API token for standalone (non–control-plane) brokers.
//! Token plaintext is returned once at setup/regenerate; only hashes are stored.

use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use parking_lot::RwLock;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;

const FILE_NAME: &str = "local-auth.json";
const TOKEN_PREFIX: &str = "sk_local_";
/// Longest bearer value worth hashing; real tokens are 73 bytes.
const MAX_TOKEN_LEN: usize = 256;
/// How long an in-memory snapshot is trusted before the file is re-checked
/// (it may be shared with other processes through `BETTERMQ_LOCAL_AUTH_FILE`).
const DEFAULT_REFRESH: Duration = Duration::from_secs(1);

#[derive(Debug, Error)]
pub enum LocalAuthError {
    #[error("local auth already configured")]
    AlreadyConfigured,
    #[error("local auth not configured")]
    NotConfigured,
    #[error("invalid password")]
    InvalidPassword,
    #[error("invalid API token")]
    InvalidToken,
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Json(#[from] serde_json::Error),
    #[error("password hashing failed")]
    Hash,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Stored {
    password_hash: String,
    api_key_hash: String,
    /// Bumped on every regenerate; peers accept a strictly newer version.
    /// Files written before versioning read as 0.
    #[serde(default)]
    version: u64,
}

/// Password + API token hashes for cluster sync (never includes plaintext token).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthCredentials {
    pub password_hash: String,
    pub api_key_hash: String,
    /// Credential generation; see [`LocalAuthStore::apply_credentials`].
    #[serde(default)]
    pub version: u64,
}

/// Identity of the credentials file, to notice replacement by another process.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileStamp {
    modified: Option<SystemTime>,
    len: u64,
    #[cfg(unix)]
    ino: u64,
}

impl FileStamp {
    fn of(meta: &fs::Metadata) -> Self {
        Self {
            modified: meta.modified().ok(),
            len: meta.len(),
            #[cfg(unix)]
            ino: std::os::unix::fs::MetadataExt::ino(meta),
        }
    }
}

#[derive(Debug, Clone)]
enum Loaded {
    Missing,
    Ready(Arc<Stored>),
    Corrupt(String),
}

#[derive(Debug)]
struct Snapshot {
    loaded: Loaded,
    stamp: Option<FileStamp>,
    checked_at: Option<Instant>,
}

/// Credentials live in one small JSON file. Verification runs against an
/// in-memory copy (refreshed after our own writes, and at most every
/// `refresh` from disk), so per-request auth costs one SHA-256, not a file read.
pub struct LocalAuthStore {
    path: PathBuf,
    refresh: Duration,
    snapshot: RwLock<Snapshot>,
}

impl LocalAuthStore {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, LocalAuthError> {
        let path = std::env::var("BETTERMQ_LOCAL_AUTH_FILE")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.as_ref().join(FILE_NAME));
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        Ok(Self {
            path,
            refresh: DEFAULT_REFRESH,
            snapshot: RwLock::new(Snapshot {
                loaded: Loaded::Missing,
                stamp: None,
                checked_at: None,
            }),
        })
    }

    /// Current credentials, from memory when fresh, else re-checked on disk.
    fn current(&self) -> Loaded {
        {
            let snap = self.snapshot.read();
            if snap
                .checked_at
                .is_some_and(|at| at.elapsed() < self.refresh)
            {
                return snap.loaded.clone();
            }
        }
        let mut snap = self.snapshot.write();
        let stamp = match fs::metadata(&self.path) {
            Ok(meta) if meta.is_file() => Some(FileStamp::of(&meta)),
            _ => None,
        };
        let unchanged = snap.checked_at.is_some()
            && stamp == snap.stamp
            && !matches!(snap.loaded, Loaded::Corrupt(_));
        if !unchanged {
            snap.loaded = match &stamp {
                None => Loaded::Missing,
                Some(_) => match fs::read_to_string(&self.path)
                    .map_err(|e| e.to_string())
                    .and_then(|raw| serde_json::from_str::<Stored>(&raw).map_err(|e| e.to_string()))
                {
                    Ok(stored) => Loaded::Ready(Arc::new(stored)),
                    Err(error) => Loaded::Corrupt(error),
                },
            };
            snap.stamp = stamp;
        }
        snap.checked_at = Some(Instant::now());
        snap.loaded.clone()
    }

    /// Record credentials this process just wrote, so they apply immediately.
    fn remember(&self, stored: Stored) {
        let stamp = fs::metadata(&self.path).ok().map(|m| FileStamp::of(&m));
        let mut snap = self.snapshot.write();
        snap.loaded = Loaded::Ready(Arc::new(stored));
        snap.stamp = stamp;
        snap.checked_at = Some(Instant::now());
    }

    pub fn is_configured(&self) -> bool {
        !matches!(self.current(), Loaded::Missing)
    }

    /// Exclusive advisory lock on `<file>.lock`, serialising every
    /// check-then-write on the credentials file across threads and across
    /// processes that share it. Blocks; call from a blocking context.
    fn write_lock(&self) -> Result<fs::File, LocalAuthError> {
        let mut name = self.path.file_name().unwrap_or_default().to_os_string();
        name.push(".lock");
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.path.with_file_name(name))?;
        lock.lock()?;
        Ok(lock)
    }

    pub fn setup(&self, password: &str) -> Result<String, LocalAuthError> {
        if self.is_configured() {
            return Err(LocalAuthError::AlreadyConfigured);
        }
        validate_password(password)?;
        let token = generate_token();
        let stored = Stored {
            password_hash: hash_password(password)?,
            api_key_hash: hash_token(&token),
            version: 1,
        };
        // Concurrent first-boot claims: exactly one wins; the rest see the file.
        let _lock = self.write_lock()?;
        if self.path.is_file() {
            return Err(LocalAuthError::AlreadyConfigured);
        }
        write_atomic(&self.path, &stored)?;
        self.remember(stored);
        Ok(token)
    }

    pub fn regenerate(&self, password: &str) -> Result<String, LocalAuthError> {
        let checked = self.load()?;
        if !verify_password(password, &checked.password_hash)? {
            return Err(LocalAuthError::InvalidPassword);
        }
        let token = generate_token();
        let _lock = self.write_lock()?;
        // Re-read under the lock so a concurrent rotation or applied snapshot
        // is not overwritten with a stale version.
        let current = self.load()?;
        if !constant_time_eq(&current.password_hash, &checked.password_hash) {
            return Err(LocalAuthError::InvalidPassword);
        }
        let updated = Stored {
            password_hash: current.password_hash,
            api_key_hash: hash_token(&token),
            version: current.version.saturating_add(1),
        };
        write_atomic(&self.path, &updated)?;
        self.remember(updated);
        Ok(token)
    }

    pub fn verify_token(&self, token: &str) -> Result<bool, LocalAuthError> {
        if token.len() > MAX_TOKEN_LEN {
            return Ok(false);
        }
        match self.current() {
            Loaded::Missing => Ok(false),
            Loaded::Ready(stored) => Ok(constant_time_eq(&hash_token(token), &stored.api_key_hash)),
            Loaded::Corrupt(error) => Err(LocalAuthError::Io(std::io::Error::other(error))),
        }
    }

    /// Admin operations use the same local API token as the data plane.
    pub fn authorize_admin(&self, token: &str) -> bool {
        self.verify_token(token).unwrap_or(false)
    }

    pub fn export_credentials(&self) -> Result<Option<AuthCredentials>, LocalAuthError> {
        if !self.is_configured() {
            return Ok(None);
        }
        let stored = self.load()?;
        Ok(Some(AuthCredentials {
            password_hash: stored.password_hash,
            api_key_hash: stored.api_key_hash,
            version: stored.version,
        }))
    }

    /// Apply credentials from a cluster peer (same API token works on every
    /// node). Callers reach this only through cluster-secret-authenticated
    /// internal routes or a seed that proved the secret.
    ///
    /// - Unconfigured node: write once (join path).
    /// - Identical hashes: no-op (adopts a newer version number).
    /// - Different hashes with a strictly newer version: a rotation; replace.
    /// - Different hashes with the same or an older version: reject, so a stale
    ///   snapshot cannot restore a revoked token and peers cannot silently
    ///   overwrite each other.
    pub fn apply_credentials(&self, creds: &AuthCredentials) -> Result<(), LocalAuthError> {
        if creds.password_hash.trim().is_empty() || creds.api_key_hash.trim().is_empty() {
            return Err(LocalAuthError::InvalidToken);
        }
        let _lock = self.write_lock()?;
        if self.path.is_file() {
            let existing = self.load()?;
            let same_password = constant_time_eq(&existing.password_hash, &creds.password_hash);
            let same_api_key = constant_time_eq(&existing.api_key_hash, &creds.api_key_hash);
            if same_password && same_api_key && creds.version <= existing.version {
                return Ok(());
            }
            if !(same_password && same_api_key) && creds.version <= existing.version {
                return Err(LocalAuthError::AlreadyConfigured);
            }
        }
        let stored = Stored {
            password_hash: creds.password_hash.clone(),
            api_key_hash: creds.api_key_hash.clone(),
            version: creds.version,
        };
        write_atomic(&self.path, &stored)?;
        self.remember(stored);
        Ok(())
    }

    /// Fresh read from disk for mutations (never the cached copy).
    fn load(&self) -> Result<Stored, LocalAuthError> {
        if !self.path.is_file() {
            return Err(LocalAuthError::NotConfigured);
        }
        let raw = fs::read_to_string(&self.path)?;
        Ok(serde_json::from_str(&raw)?)
    }
}

fn validate_password(password: &str) -> Result<(), LocalAuthError> {
    if password.len() < 12 {
        return Err(LocalAuthError::InvalidPassword);
    }
    Ok(())
}

fn hash_password(password: &str) -> Result<String, LocalAuthError> {
    let salt = SaltString::generate(&mut rand::thread_rng());
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|_| LocalAuthError::Hash)
}

fn verify_password(password: &str, encoded: &str) -> Result<bool, LocalAuthError> {
    let parsed = PasswordHash::new(encoded).map_err(|_| LocalAuthError::Hash)?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok())
}

fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!("{TOKEN_PREFIX}{}", hex::encode(bytes))
}

fn hash_token(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    hex::encode(digest)
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// Durable replace: unique temp file (mode 0600 from creation) → fsync →
/// rename → directory fsync. A unique name keeps concurrent writers, including
/// other processes sharing the file, from truncating each other's temp file.
fn write_atomic(path: &Path, stored: &Stored) -> Result<(), LocalAuthError> {
    use std::io::Write;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let json = serde_json::to_string_pretty(stored)?;
    let mut suffix = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut suffix);
    let tmp = parent.join(format!(
        ".{}.{}.{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id(),
        hex::encode(suffix)
    ));
    let written = (|| -> Result<(), LocalAuthError> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut f = options.open(&tmp)?;
        f.write_all(json.as_bytes())?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, path)?;
        Ok(())
    })();
    if written.is_err() {
        let _ = fs::remove_file(&tmp);
        return written;
    }
    if let Ok(dir) = fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn setup_and_verify() {
        let dir = tempdir().unwrap();
        let store = LocalAuthStore::open(dir.path()).unwrap();
        let token = store.setup("password1234").unwrap();
        assert!(token.starts_with(TOKEN_PREFIX));
        assert!(store.verify_token(&token).unwrap());
        assert!(!store.verify_token("sk_local_deadbeef").unwrap());
    }

    #[test]
    fn concurrent_first_boot_setup_has_exactly_one_winner() {
        let dir = tempdir().unwrap();
        let results: Vec<_> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..6)
                .map(|_| {
                    let path = dir.path();
                    scope.spawn(move || {
                        // Separate instances model separate processes sharing the file.
                        let store = LocalAuthStore::open(path).unwrap();
                        store.setup("password1234")
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let winners: Vec<&String> = results.iter().filter_map(|r| r.as_ref().ok()).collect();
        assert_eq!(winners.len(), 1, "{results:?}");
        assert!(results
            .iter()
            .all(|r| r.is_ok() || matches!(r, Err(LocalAuthError::AlreadyConfigured))));
        let store = LocalAuthStore::open(dir.path()).unwrap();
        assert!(
            store.verify_token(winners[0]).unwrap(),
            "the issued token works"
        );
        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(dir.path().join(FILE_NAME))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[test]
    fn regenerate_rotates_token() {
        let dir = tempdir().unwrap();
        let store = LocalAuthStore::open(dir.path()).unwrap();
        let t1 = store.setup("password1234").unwrap();
        let t2 = store.regenerate("password1234").unwrap();
        assert_ne!(t1, t2);
        assert!(!store.verify_token(&t1).unwrap());
        assert!(store.verify_token(&t2).unwrap());
    }

    #[test]
    fn verify_uses_memory_and_rejects_oversized_tokens() {
        let dir = tempdir().unwrap();
        let store = LocalAuthStore::open(dir.path()).unwrap();
        let token = store.setup("password1234").unwrap();
        assert!(!store.verify_token(&"x".repeat(MAX_TOKEN_LEN + 1)).unwrap());
        // Unlinking the file is not seen until the refresh interval passes:
        // verification is served from memory, not a per-request file read.
        fs::remove_file(dir.path().join(FILE_NAME)).unwrap();
        assert!(store.verify_token(&token).unwrap());
    }

    #[test]
    fn picks_up_rotation_by_another_process_sharing_the_file() {
        let dir = tempdir().unwrap();
        let mut a = LocalAuthStore::open(dir.path()).unwrap();
        a.refresh = Duration::ZERO;
        let b = LocalAuthStore::open(dir.path()).unwrap();
        let t1 = a.setup("password1234").unwrap();
        assert!(b.verify_token(&t1).unwrap());
        let t2 = b.regenerate("password1234").unwrap();
        assert!(
            !a.verify_token(&t1).unwrap(),
            "revoked token must stop working"
        );
        assert!(a.verify_token(&t2).unwrap());
    }

    #[test]
    fn corrupt_file_is_an_error_not_an_open_door() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join(FILE_NAME), b"{ not json").unwrap();
        let store = LocalAuthStore::open(dir.path()).unwrap();
        assert!(store.is_configured());
        assert!(store.verify_token("sk_local_x").is_err());
    }

    #[test]
    fn apply_credentials_allows_first_write_and_blocks_takeover() {
        let dir = tempdir().unwrap();
        let store = LocalAuthStore::open(dir.path()).unwrap();
        let creds = AuthCredentials {
            password_hash: "argon2-hash".into(),
            api_key_hash: "sha256-hash".into(),
            version: 1,
        };
        store.apply_credentials(&creds).unwrap();
        store.apply_credentials(&creds).unwrap(); // idempotent
        let takeover = AuthCredentials {
            password_hash: "evil-password-hash".into(),
            api_key_hash: "evil-api-hash".into(),
            version: 1,
        };
        assert!(matches!(
            store.apply_credentials(&takeover),
            Err(LocalAuthError::AlreadyConfigured)
        ));
    }

    #[test]
    fn rotation_propagates_to_a_configured_peer_and_stale_snapshots_are_refused() {
        let origin_dir = tempdir().unwrap();
        let peer_dir = tempdir().unwrap();
        let origin = LocalAuthStore::open(origin_dir.path()).unwrap();
        let peer = LocalAuthStore::open(peer_dir.path()).unwrap();
        let t1 = origin.setup("password1234").unwrap();
        let v1 = origin.export_credentials().unwrap().unwrap();
        peer.apply_credentials(&v1).unwrap();
        assert!(peer.verify_token(&t1).unwrap());

        let t2 = origin.regenerate("password1234").unwrap();
        let v2 = origin.export_credentials().unwrap().unwrap();
        assert_eq!(v2.version, v1.version + 1);
        peer.apply_credentials(&v2).unwrap();
        assert!(
            !peer.verify_token(&t1).unwrap(),
            "old token revoked on peer"
        );
        assert!(peer.verify_token(&t2).unwrap());

        assert!(matches!(
            peer.apply_credentials(&v1),
            Err(LocalAuthError::AlreadyConfigured)
        ));
        assert!(peer.verify_token(&t2).unwrap());
    }

    #[test]
    fn unversioned_files_read_as_version_zero() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join(FILE_NAME),
            br#"{"password_hash":"p","api_key_hash":"a"}"#,
        )
        .unwrap();
        let store = LocalAuthStore::open(dir.path()).unwrap();
        assert_eq!(store.export_credentials().unwrap().unwrap().version, 0);
        store
            .apply_credentials(&AuthCredentials {
                password_hash: "p".into(),
                api_key_hash: "b".into(),
                version: 1,
            })
            .unwrap();
        assert_eq!(store.export_credentials().unwrap().unwrap().version, 1);
    }
}
