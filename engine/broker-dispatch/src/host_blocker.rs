//! Per-destination host circuit breaker (CP6a / CP6b).
//! When `BETTERMQ_SHARED_META_DIR` is set, block state is shared across brokers (Phase E).

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

#[derive(Debug, Clone)]
pub struct HostBlockerConfig {
    pub failures_before_block: u32,
    pub initial_cooldown_ms: u64,
    pub max_cooldown_ms: u64,
    pub multiplier: f64,
}

impl Default for HostBlockerConfig {
    fn default() -> Self {
        Self {
            failures_before_block: 3,
            initial_cooldown_ms: 30_000,
            max_cooldown_ms: 900_000,
            multiplier: 2.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HostState {
    failures: u32,
    /// Wall-clock ms since epoch when block ends (shared-durable).
    blocked_until_ms: Option<i64>,
    cooldown_ms: u64,
    #[serde(default)]
    updated_at_ns: u64,
    #[serde(default)]
    revision: uuid::Uuid,
    #[serde(default)]
    cleared: bool,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct SharedHostFile {
    hosts: HashMap<String, HostState>,
}

#[derive(Default)]
pub struct HostBlocker {
    cfg: HostBlockerConfig,
    hosts: Mutex<HashMap<String, HostState>>,
    shared_path: Option<PathBuf>,
    last_shared_load_ms: Mutex<i64>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn touch(state: &mut HostState) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64;
    state.updated_at_ns = now.max(state.updated_at_ns.saturating_add(1));
    state.revision = uuid::Uuid::new_v4();
}

fn merge_hosts(target: &mut HashMap<String, HostState>, incoming: HashMap<String, HostState>) {
    for (key, state) in incoming {
        let replace = target.get(&key).is_none_or(|old| {
            (state.updated_at_ns, state.revision) > (old.updated_at_ns, old.revision)
        });
        if replace {
            target.insert(key, state);
        }
    }
}

impl HostBlocker {
    pub fn shared_metadata_enabled(&self) -> bool {
        self.shared_path.is_some()
    }

    pub fn new(cfg: HostBlockerConfig) -> Self {
        let shared_path = std::env::var("BETTERMQ_SHARED_META_DIR")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|d| PathBuf::from(d).join("host_breaker.json"));
        Self::with_shared_path(cfg, shared_path)
    }

    fn with_shared_path(cfg: HostBlockerConfig, shared_path: Option<PathBuf>) -> Self {
        let hb = Self {
            cfg,
            hosts: Mutex::new(HashMap::new()),
            shared_path: shared_path.clone(),
            last_shared_load_ms: Mutex::new(0),
        };
        if let Some(ref path) = shared_path {
            hb.load_shared(path);
        }
        hb
    }

    fn load_shared(&self, path: &Path) {
        // Throttle missing/unreadable snapshots too; a healthy new cluster may
        // not create the first snapshot until its first breaker transition.
        *self.last_shared_load_ms.lock() = now_ms();
        let Ok(_lock) = broker_storage::FileLock::exclusive(path) else {
            return;
        };
        let Ok(mut f) = File::open(path) else {
            return;
        };
        let mut buf = String::new();
        if f.read_to_string(&mut buf).is_err() {
            return;
        }
        let Ok(file) = serde_json::from_str::<SharedHostFile>(&buf) else {
            return;
        };
        merge_hosts(&mut self.hosts.lock(), file.hosts);
        *self.last_shared_load_ms.lock() = now_ms();
    }

    fn maybe_reload_shared(&self) {
        let Some(ref path) = self.shared_path else {
            return;
        };
        let now = now_ms();
        if now.saturating_sub(*self.last_shared_load_ms.lock()) < 1_000 {
            return;
        }
        self.load_shared(path);
    }

    fn persist_shared(&self) {
        let Some(ref path) = self.shared_path else {
            return;
        };
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let Ok(_lock) = broker_storage::FileLock::exclusive(path) else {
            return;
        };
        let mut file = if path.exists() {
            match std::fs::read(path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<SharedHostFile>(&bytes).ok())
            {
                Some(file) => file,
                None => {
                    tracing::error!(
                        "cannot read shared host breaker state; refusing to overwrite it"
                    );
                    return;
                }
            }
        } else {
            SharedHostFile::default()
        };
        merge_hosts(&mut file.hosts, self.hosts.lock().clone());
        merge_hosts(&mut self.hosts.lock(), file.hosts.clone());
        let Ok(json) = serde_json::to_vec_pretty(&file) else {
            return;
        };
        if let Err(error) = broker_storage::atomic_write_file(path, &json) {
            tracing::error!(%error, "cannot persist shared host breaker state");
        }
    }

    pub fn host_key(url: &str) -> Option<String> {
        let parsed = Url::parse(url).ok()?;
        let host = parsed.host_str()?;
        let port = parsed
            .port()
            .unwrap_or(if parsed.scheme() == "https" { 443 } else { 80 });
        Some(format!("{}://{}:{}", parsed.scheme(), host, port))
    }

    pub fn is_blocked(&self, url: &str) -> bool {
        let Some(key) = Self::host_key(url) else {
            return false;
        };
        self.is_host_key_blocked(&key)
    }

    pub(crate) fn is_host_key_blocked(&self, key: &str) -> bool {
        self.maybe_reload_shared();
        let hosts = self.hosts.lock();
        let Some(state) = hosts.get(key) else {
            return false;
        };
        state.blocked_until_ms.is_some_and(|until| now_ms() < until)
    }

    pub async fn is_blocked_async(self: &std::sync::Arc<Self>, url: &str) -> bool {
        if self.shared_path.is_some()
            && now_ms().saturating_sub(*self.last_shared_load_ms.lock()) >= 1_000
        {
            let this = self.clone();
            let url = url.to_owned();
            return tokio::task::spawn_blocking(move || this.is_blocked(&url))
                .await
                .unwrap_or(true);
        }
        self.is_blocked(url)
    }

    pub fn record_success(&self, url: &str) {
        let Some(key) = Self::host_key(url) else {
            return;
        };
        if self.clear_host(&key) {
            self.persist_shared();
        }
    }

    pub fn record_transport_failure(&self, url: &str) {
        let Some(key) = Self::host_key(url) else {
            return;
        };
        {
            let mut hosts = self.hosts.lock();
            let state = hosts.entry(key.clone()).or_insert_with(|| HostState {
                failures: 0,
                blocked_until_ms: None,
                cooldown_ms: self.cfg.initial_cooldown_ms,
                updated_at_ns: 0,
                revision: uuid::Uuid::nil(),
                cleared: false,
            });
            state.cleared = false;
            state.failures = state.failures.saturating_add(1);
            // Requests already in flight when the circuit opened can all time
            // out together. They are not failed recovery probes: extending the
            // cooldown for each one turns a 30-second outage into 15 minutes.
            if state.blocked_until_ms.is_some_and(|until| now_ms() < until) {
                return;
            }
            if state.failures >= self.cfg.failures_before_block {
                let wait = state.cooldown_ms;
                state.blocked_until_ms = Some(now_ms() + wait as i64);
                state.cooldown_ms = ((state.cooldown_ms as f64) * self.cfg.multiplier)
                    .min(self.cfg.max_cooldown_ms as f64)
                    as u64;
                touch(state);
                tracing::warn!(
                    destination = %key,
                    cooldown_ms = wait,
                    "host blocked after transport failures"
                );
            } else {
                return;
            }
        }
        self.persist_shared();
    }

    /// Operator override — block a host immediately (URL or `scheme://host:port` key).
    pub fn block_manual(&self, host: &str, duration_ms: u64) -> String {
        let key = if host.contains("://") {
            Self::host_key(host).unwrap_or_else(|| host.trim().to_string())
        } else {
            host.trim().to_string()
        };
        let wait = duration_ms.max(1_000);
        let mut state = HostState {
            failures: self.cfg.failures_before_block,
            blocked_until_ms: Some(now_ms().saturating_add(wait.min(i64::MAX as u64) as i64)),
            cooldown_ms: duration_ms,
            updated_at_ns: self
                .hosts
                .lock()
                .get(&key)
                .map_or(0, |state| state.updated_at_ns),
            revision: uuid::Uuid::nil(),
            cleared: false,
        };
        touch(&mut state);
        self.hosts.lock().insert(key.clone(), state);
        tracing::warn!(host = %Self::host_key(&key).unwrap_or_else(|| "<invalid destination>".into()), cooldown_ms = wait, "host manually blocked");
        self.persist_shared();
        key
    }

    pub fn unblock(&self, host: &str) -> bool {
        let key = if host.contains("://") {
            Self::host_key(host).unwrap_or_else(|| host.to_string())
        } else {
            host.to_string()
        };
        self.maybe_reload_shared();
        let removed = self.clear_host(&key);
        if removed {
            self.persist_shared();
        }
        removed
    }

    fn clear_host(&self, key: &str) -> bool {
        let mut hosts = self.hosts.lock();
        if self.shared_path.is_none() {
            return hosts.remove(key).is_some();
        }
        let Some(state) = hosts.get_mut(key).filter(|state| !state.cleared) else {
            return false;
        };
        state.failures = 0;
        state.blocked_until_ms = None;
        state.cooldown_ms = self.cfg.initial_cooldown_ms;
        state.cleared = true;
        touch(state);
        true
    }

    pub async fn record_success_async(self: &std::sync::Arc<Self>, url: &str) {
        let changed = Self::host_key(url).is_some_and(|key| {
            self.hosts
                .lock()
                .get(&key)
                .is_some_and(|state| !state.cleared)
        });
        if !changed {
            return;
        }
        if self.shared_path.is_none() {
            self.record_success(url);
            return;
        }
        let this = self.clone();
        let url = url.to_owned();
        if let Err(error) = tokio::task::spawn_blocking(move || this.record_success(&url)).await {
            tracing::error!(%error, "shared breaker success update failed");
        }
    }

    pub async fn record_transport_failure_async(self: &std::sync::Arc<Self>, url: &str) {
        if self.shared_path.is_none() {
            self.record_transport_failure(url);
            return;
        }
        let this = self.clone();
        let url = url.to_owned();
        if let Err(error) =
            tokio::task::spawn_blocking(move || this.record_transport_failure(&url)).await
        {
            tracing::error!(%error, "shared breaker failure update failed");
        }
    }

    pub fn blocked_hosts(&self) -> Vec<(String, u64)> {
        self.maybe_reload_shared();
        let hosts = self.hosts.lock();
        let now = now_ms();
        hosts
            .iter()
            .filter_map(|(k, s)| {
                s.blocked_until_ms.and_then(|until| {
                    if until > now {
                        Some((k.clone(), (until - now) as u64))
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    pub fn telemetry_snapshot(&self) -> crate::telemetry::HostPressureSnapshot {
        self.maybe_reload_shared();
        let hosts = self.hosts.lock();
        let now = now_ms();
        let mut blocked_hosts = 0u64;
        let mut hosts_with_failures = 0u64;
        let mut current_failures = 0u64;
        let mut max_host_failures = 0u64;
        for state in hosts.values().filter(|state| !state.cleared) {
            blocked_hosts += u64::from(
                state
                    .blocked_until_ms
                    .is_some_and(|blocked_until| blocked_until > now),
            );
            hosts_with_failures += u64::from(state.failures > 0);
            current_failures = current_failures.saturating_add(u64::from(state.failures));
            max_host_failures = max_host_failures.max(u64::from(state.failures));
        }
        crate::telemetry::HostPressureSnapshot {
            tracked_hosts: hosts.values().filter(|state| !state.cleared).count() as u64,
            blocked_hosts,
            hosts_with_failures,
            current_failures,
            max_host_failures,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_updates_preserve_peer_blocks_and_clears() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("host_breaker.json");
        let a = HostBlocker::with_shared_path(HostBlockerConfig::default(), Some(path.clone()));
        let b = HostBlocker::with_shared_path(HostBlockerConfig::default(), Some(path.clone()));
        a.record_success("https://healthy.example/hook");
        assert!(
            !path.exists(),
            "healthy deliveries must not create shared snapshots"
        );
        a.block_manual("https://a.example", 60_000);
        b.block_manual("https://b.example", 60_000);
        a.load_shared(&path);
        assert!(a.is_blocked("https://a.example"));
        assert!(a.is_blocked("https://b.example"));
        a.unblock("https://a.example");
        b.block_manual("https://c.example", 60_000);
        a.load_shared(&path);
        b.load_shared(&path);
        assert!(
            !b.is_blocked("https://a.example"),
            "stale peer must not resurrect clears"
        );
        assert!(a.is_blocked("https://b.example"));
        assert!(a.is_blocked("https://c.example"));
        let bytes = std::fs::read(&path).unwrap();
        for _ in 0..100 {
            a.record_success("https://healthy.example/hook");
        }
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn blocks_after_failures() {
        let hb = HostBlocker::new(HostBlockerConfig {
            failures_before_block: 2,
            initial_cooldown_ms: 60_000,
            max_cooldown_ms: 60_000,
            multiplier: 2.0,
        });
        let url = "https://example.com/hook";
        assert!(!hb.is_blocked(url));
        hb.record_transport_failure(url);
        assert!(!hb.is_blocked(url));
        hb.record_transport_failure(url);
        assert!(hb.is_blocked(url));
        let snapshot = hb.telemetry_snapshot();
        assert_eq!(snapshot.tracked_hosts, 1);
        assert_eq!(snapshot.blocked_hosts, 1);
        assert_eq!(snapshot.current_failures, 2);
        hb.record_success(url);
        assert!(!hb.is_blocked(url));
        assert!(
            hb.hosts.lock().is_empty(),
            "local clears need no retained tombstone"
        );
    }

    #[test]
    fn concurrent_failures_do_not_extend_an_open_circuit() {
        let hb = HostBlocker::new(HostBlockerConfig::default());
        let url = "https://example.com/hook";
        for _ in 0..3 {
            hb.record_transport_failure(url);
        }
        let key = HostBlocker::host_key(url).unwrap();
        let opened = hb.hosts.lock()[&key].clone();
        for _ in 0..256 {
            hb.record_transport_failure(url);
        }
        let after = hb.hosts.lock()[&key].clone();
        assert_eq!(after.blocked_until_ms, opened.blocked_until_ms);
        assert_eq!(after.cooldown_ms, opened.cooldown_ms);
        hb.hosts.lock().get_mut(&key).unwrap().blocked_until_ms = Some(now_ms() - 1);
        hb.record_transport_failure(url);
        assert_eq!(hb.hosts.lock()[&key].cooldown_ms, opened.cooldown_ms * 2);
    }

    #[test]
    fn missing_shared_snapshot_is_reloaded_at_most_once_per_interval() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("host_breaker.json");
        let blocker = HostBlocker::with_shared_path(HostBlockerConfig::default(), Some(path));
        let loaded_at = *blocker.last_shared_load_ms.lock();
        assert!(loaded_at > 0);
        for _ in 0..100 {
            assert!(!blocker.is_blocked("https://healthy.example"));
        }
        assert_eq!(*blocker.last_shared_load_ms.lock(), loaded_at);
    }
}
