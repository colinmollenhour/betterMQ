//! Atomic JSON file I/O with corruption recovery (.tmp / .bak fallbacks).

use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonLoadSource {
    Main,
    Temp,
    Backup,
    /// File did not exist — fresh install, empty state is expected.
    Missing,
    /// Operator override after unrecoverable corruption (`BETTERMQ_METADATA_RECOVER=empty`).
    EmptyRecovery,
}

#[derive(Debug, Error)]
#[error(
    "metadata file corrupt with no recoverable backup: {path}; set BETTERMQ_METADATA_RECOVER=empty to start fresh"
)]
pub struct MetadataLoadError {
    pub path: PathBuf,
}

#[derive(Debug)]
pub struct JsonLoadOutcome<T> {
    pub value: T,
    pub source: JsonLoadSource,
}

/// When set to `empty`, allows starting with default state after all recovery paths fail.
pub fn allow_empty_metadata_recovery() -> bool {
    std::env::var("BETTERMQ_METADATA_RECOVER").is_ok_and(|v| v.eq_ignore_ascii_case("empty"))
}

fn temp_path(path: &Path) -> PathBuf {
    path.with_extension("json.tmp")
}

fn backup_path(path: &Path) -> PathBuf {
    path.with_extension("json.bak")
}

fn corrupt_quarantine_path(path: &Path) -> PathBuf {
    let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S%.3fZ");
    path.with_extension(format!("json.corrupt.{ts}"))
}

/// Quarantined copies of `path` (`<name>.json.corrupt.<ts>`).
fn quarantined_copies(path: &Path) -> Vec<PathBuf> {
    let (Some(dir), Some(name)) = (path.parent(), path.file_name()) else {
        return Vec::new();
    };
    let prefix = format!("{}.corrupt.", name.to_string_lossy());
    let Ok(entries) = fs::read_dir(if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    }) else {
        return Vec::new();
    };
    entries
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(&prefix))
        .map(|entry| entry.path())
        .collect()
}

fn try_parse<T: serde::de::DeserializeOwned>(bytes: &[u8], label: &str) -> Option<T> {
    if bytes.is_empty() || bytes.iter().all(|b| b.is_ascii_whitespace()) {
        warn!(file = label, "json file is empty");
        return None;
    }
    match serde_json::from_slice::<T>(bytes) {
        Ok(v) => Some(v),
        Err(e) => {
            warn!(file = label, error = %e, "json file is corrupt or invalid");
            None
        }
    }
}

/// Copy unreadable metadata aside for forensics. The main file is left in
/// place until a recovery succeeds, so a failed start cannot turn into a
/// "fresh install" on the next one.
fn quarantine_corrupt_file(path: &Path, bytes: &[u8]) {
    if quarantined_copies(path)
        .iter()
        .any(|copy| fs::read(copy).is_ok_and(|existing| existing == bytes))
    {
        warn!(
            file = %path.display(),
            "unreadable json already quarantined; will attempt recovery from .tmp / .bak"
        );
        return;
    }
    let quarantine = corrupt_quarantine_path(path);
    match fs::write(&quarantine, bytes) {
        Ok(()) => warn!(
            from = %path.display(),
            to = %quarantine.display(),
            "quarantined unreadable json; will attempt recovery from .tmp / .bak"
        ),
        Err(e) => warn!(
            file = %path.display(),
            error = %e,
            "could not quarantine corrupt json"
        ),
    }
}

/// Replace the main file with recovered bytes without rotating the (corrupt)
/// main file over a good `.bak`.
fn restore_main(path: &Path, bytes: &[u8]) {
    if let Err(error) = broker_storage::atomic_write_file(path, bytes) {
        warn!(file = %path.display(), %error, "could not rewrite recovered metadata file");
    }
}

/// Load JSON from `path`, falling back to `.json.tmp`, then `.json.bak`.
/// Returns `Err` if the file existed but no valid copy was found (unless `BETTERMQ_METADATA_RECOVER=empty`).
/// A missing file next to a quarantined copy is treated the same way, not as a fresh install.
pub fn load_json_with_recovery<T, F>(
    path: &Path,
    default: F,
) -> Result<JsonLoadOutcome<T>, MetadataLoadError>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
    F: FnOnce() -> T,
{
    if !path.exists() {
        let quarantined = quarantined_copies(path);
        if quarantined.is_empty() {
            return Ok(JsonLoadOutcome {
                value: default(),
                source: JsonLoadSource::Missing,
            });
        }
        warn!(
            file = %path.display(),
            quarantined = quarantined.len(),
            "metadata file missing but quarantined copies exist; not treating as a fresh install"
        );
    }

    let tmp = temp_path(path);
    let bak = backup_path(path);

    match fs::read(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Ok(bytes) => {
            if let Some(v) = try_parse(&bytes, &path.display().to_string()) {
                return Ok(JsonLoadOutcome {
                    value: v,
                    source: JsonLoadSource::Main,
                });
            }
            quarantine_corrupt_file(path, &bytes);
        }
        Err(e) => {
            warn!(
                file = %path.display(),
                error = %e,
                "could not read metadata file; will attempt recovery from .tmp / .bak"
            );
        }
    }

    if tmp.exists() {
        if let Ok(bytes) = fs::read(&tmp) {
            if let Some(v) = try_parse(&bytes, &tmp.display().to_string()) {
                info!(
                    file = %path.display(),
                    "recovered json metadata from .json.tmp"
                );
                restore_main(path, &bytes);
                return Ok(JsonLoadOutcome {
                    value: v,
                    source: JsonLoadSource::Temp,
                });
            }
        }
        warn!(file = %tmp.display(), "found .json.tmp but it is also unreadable");
    }

    if bak.exists() {
        if let Ok(bytes) = fs::read(&bak) {
            if let Some(v) = try_parse(&bytes, &bak.display().to_string()) {
                info!(
                    file = %path.display(),
                    "recovered json metadata from .json.bak"
                );
                restore_main(path, &bytes);
                return Ok(JsonLoadOutcome {
                    value: v,
                    source: JsonLoadSource::Backup,
                });
            }
        }
        warn!(file = %bak.display(), "found .json.bak but it is also unreadable");
    }

    if allow_empty_metadata_recovery() {
        warn!(
            file = %path.display(),
            "no valid json found; starting with empty state (BETTERMQ_METADATA_RECOVER=empty)"
        );
        let value = default();
        // Persist the operator's choice so later starts without the override
        // load the empty state instead of failing on the corrupt file again.
        match serde_json::to_vec(&value) {
            Ok(bytes) => restore_main(path, &bytes),
            Err(error) => warn!(file = %path.display(), %error, "could not encode empty metadata"),
        }
        return Ok(JsonLoadOutcome {
            value,
            source: JsonLoadSource::EmptyRecovery,
        });
    }

    Err(MetadataLoadError {
        path: path.to_path_buf(),
    })
}

/// Write `bytes` atomically: rotate `.bak` then temp → fsync → rename.
pub fn persist_json_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let bak = backup_path(path);
    if path.exists() {
        let _ = fs::copy(path, &bak);
    }

    broker_storage::atomic_write_file(path, bytes)
}

#[cfg(test)]
pub(crate) mod env_test_lock {
    use std::sync::Mutex;
    pub static LOCK: Mutex<()> = Mutex::new(());
}

#[cfg(test)]
mod tests {
    use super::env_test_lock::LOCK;
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
    struct Sample {
        items: Vec<u32>,
    }

    fn sample_path(dir: &Path) -> PathBuf {
        dir.join("schedule.json")
    }

    #[test]
    fn load_missing_file_uses_default() {
        let dir = TempDir::new().unwrap();
        let path = sample_path(dir.path());
        let out = load_json_with_recovery(&path, || Sample { items: vec![] }).unwrap();
        assert_eq!(out.source, JsonLoadSource::Missing);
        assert!(out.value.items.is_empty());
    }

    #[test]
    fn load_valid_main_file() {
        let dir = TempDir::new().unwrap();
        let path = sample_path(dir.path());
        persist_json_atomic(&path, br#"{"items":[1,2]}"#).unwrap();
        let out = load_json_with_recovery(&path, || Sample { items: vec![] }).unwrap();
        assert_eq!(out.source, JsonLoadSource::Main);
        assert_eq!(out.value.items, vec![1, 2]);
    }

    #[test]
    fn recovers_from_backup_when_main_empty() {
        let dir = TempDir::new().unwrap();
        let path = sample_path(dir.path());
        persist_json_atomic(&path, br#"{"items":[9]}"#).unwrap();
        // Second write rotates the first good snapshot into .json.bak
        persist_json_atomic(&path, br#"{"items":[9]}"#).unwrap();
        fs::write(&path, b"").unwrap();
        let out = load_json_with_recovery(&path, || Sample { items: vec![] }).unwrap();
        assert_eq!(out.source, JsonLoadSource::Backup);
        assert_eq!(out.value.items, vec![9]);
        let restored = fs::read_to_string(&path).unwrap();
        assert!(restored.contains("[9]"));
    }

    #[test]
    fn recovers_from_tmp_when_main_corrupt() {
        let dir = TempDir::new().unwrap();
        let path = sample_path(dir.path());
        let tmp = temp_path(&path);
        fs::write(&path, b"not json").unwrap();
        fs::write(&tmp, br#"{"items":[3]}"#).unwrap();
        let out = load_json_with_recovery(&path, || Sample { items: vec![] }).unwrap();
        assert_eq!(out.source, JsonLoadSource::Temp);
        assert_eq!(out.value.items, vec![3]);
    }

    #[test]
    fn fails_closed_when_no_fallback() {
        let _guard = LOCK.lock().unwrap();
        unsafe { std::env::remove_var("BETTERMQ_METADATA_RECOVER") };
        let dir = TempDir::new().unwrap();
        let path = sample_path(dir.path());
        fs::write(&path, b"{broken").unwrap();
        let err = load_json_with_recovery(&path, || Sample { items: vec![] })
            .expect_err("expected load to fail");
        assert_eq!(err.path, path);
        assert_eq!(fs::read(&path).unwrap(), b"{broken", "main file kept");
        assert_eq!(quarantined_copies(&path).len(), 1);

        // A restart (e.g. restart: unless-stopped) must fail closed again,
        // without piling up identical quarantine copies.
        load_json_with_recovery(&path, || Sample { items: vec![] })
            .expect_err("second load must also fail");
        assert_eq!(quarantined_copies(&path).len(), 1);
    }

    #[test]
    fn missing_main_next_to_quarantine_fails_closed() {
        let _guard = LOCK.lock().unwrap();
        unsafe { std::env::remove_var("BETTERMQ_METADATA_RECOVER") };
        let dir = TempDir::new().unwrap();
        let path = sample_path(dir.path());
        fs::write(
            path.with_extension("json.corrupt.20260101T000000.000Z"),
            b"{broken",
        )
        .unwrap();
        load_json_with_recovery(&path, || Sample { items: vec![] })
            .expect_err("missing main beside a quarantined copy is not a fresh install");

        let bak = backup_path(&path);
        fs::write(&bak, br#"{"items":[4]}"#).unwrap();
        let out = load_json_with_recovery(&path, || Sample { items: vec![] }).unwrap();
        assert_eq!(out.source, JsonLoadSource::Backup);
        assert_eq!(out.value.items, vec![4]);
    }

    #[test]
    fn backup_recovery_does_not_overwrite_good_backup() {
        let dir = TempDir::new().unwrap();
        let path = sample_path(dir.path());
        fs::write(&path, b"{broken").unwrap();
        fs::write(backup_path(&path), br#"{"items":[5]}"#).unwrap();
        let out = load_json_with_recovery(&path, || Sample { items: vec![] }).unwrap();
        assert_eq!(out.source, JsonLoadSource::Backup);
        assert_eq!(fs::read(backup_path(&path)).unwrap(), br#"{"items":[5]}"#);
        assert_eq!(fs::read(&path).unwrap(), br#"{"items":[5]}"#);
        assert_eq!(quarantined_copies(&path).len(), 1);
    }

    #[test]
    fn empty_recovery_env_allows_fresh_start() {
        let _guard = LOCK.lock().unwrap();
        unsafe { std::env::set_var("BETTERMQ_METADATA_RECOVER", "empty") };
        let dir = TempDir::new().unwrap();
        let path = sample_path(dir.path());
        fs::write(&path, b"{broken").unwrap();
        let out = load_json_with_recovery(&path, || Sample { items: vec![] }).unwrap();
        unsafe { std::env::remove_var("BETTERMQ_METADATA_RECOVER") };
        assert_eq!(out.source, JsonLoadSource::EmptyRecovery);
        assert!(out.value.items.is_empty());

        // The override is remembered: the next start loads the empty state.
        let out = load_json_with_recovery(&path, || Sample { items: vec![9] }).unwrap();
        assert_eq!(out.source, JsonLoadSource::Main);
        assert!(out.value.items.is_empty());
    }
}
