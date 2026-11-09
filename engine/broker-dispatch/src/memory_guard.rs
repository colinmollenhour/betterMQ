//! Pause dispatch fetches when process memory is high (CP6a).

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub struct MemoryGuardConfig {
    pub fetch_limit_percent: u8,
    pub limit_mb: Option<u64>,
}

impl Default for MemoryGuardConfig {
    fn default() -> Self {
        Self {
            fetch_limit_percent: 75,
            limit_mb: std::env::var("BETTERMQ_MEMORY_LIMIT_MB")
                .ok()
                .and_then(|s| s.parse().ok()),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessResourceStats {
    pub rss_mb: Option<u64>,
    pub cpu_percent: Option<f32>,
}

#[derive(Default)]
pub struct MemoryGuard {
    critical: AtomicBool,
    sample_unavailable: AtomicBool,
    stop: AtomicBool,
    stop_notify: Notify,
    cfg: MemoryGuardConfig,
}

impl MemoryGuard {
    pub fn new(cfg: MemoryGuardConfig) -> Self {
        Self {
            critical: AtomicBool::new(false),
            sample_unavailable: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            stop_notify: Notify::new(),
            cfg,
        }
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.stop_notify.notify_waiters();
    }

    /// Sample RSS while a memory limit is configured. Returns `None` when there is
    /// nothing to monitor so shutdown does not have a leftover `Interval`.
    pub fn spawn_monitor(self: &std::sync::Arc<Self>) -> Option<JoinHandle<()>> {
        self.cfg.limit_mb?;
        let guard = std::sync::Arc::clone(self);
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                if guard.stop.load(Ordering::Acquire) {
                    break;
                }
                tokio::select! {
                    _ = guard.stop_notify.notified() => break,
                    _ = interval.tick() => {
                        if guard.stop.load(Ordering::Acquire) {
                            break;
                        }
                        guard.sample();
                    }
                }
            }
        }))
    }

    fn sample(&self) {
        let Some(limit_mb) = self.cfg.limit_mb else {
            return;
        };
        let Some(rss_mb) = sample_process_resources().rss_mb else {
            if !self.sample_unavailable.swap(true, Ordering::Relaxed) {
                tracing::warn!("process RSS unavailable; retaining previous memory-pressure state");
            }
            return;
        };
        self.sample_unavailable.store(false, Ordering::Relaxed);
        let threshold = limit_mb.saturating_mul(self.cfg.fetch_limit_percent as u64) / 100;
        let is_critical = rss_mb >= threshold;
        let was = self.critical.swap(is_critical, Ordering::SeqCst);
        if is_critical && !was {
            tracing::error!(
                rss_mb,
                threshold_mb = threshold,
                limit_mb,
                "memory critical: pausing dispatch fetch"
            );
        } else if !is_critical && was {
            tracing::info!(rss_mb, "memory usage back to normal");
        }
    }

    pub fn limit_mb(&self) -> Option<u64> {
        self.cfg.limit_mb
    }

    pub fn fetch_limit_percent(&self) -> u8 {
        self.cfg.fetch_limit_percent
    }

    /// True when RSS is at or above the fetch pause threshold.
    pub fn is_critical(&self) -> bool {
        self.critical.load(Ordering::SeqCst)
    }

    pub async fn wait_below_limit(&self) {
        if !self.critical.load(Ordering::SeqCst) {
            return;
        }
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            if self.stop.load(Ordering::Acquire) {
                return;
            }
            tokio::select! {
                _ = self.stop_notify.notified() => return,
                _ = interval.tick() => {
                    if !self.critical.load(Ordering::SeqCst) {
                        return;
                    }
                }
            }
        }
    }
}

/// RSS + CPU for the broker process (best-effort; used by `/metrics` and the panel).
pub fn sample_process_resources() -> ProcessResourceStats {
    #[cfg(target_os = "linux")]
    {
        ProcessResourceStats {
            rss_mb: linux_rss_mb(),
            cpu_percent: None,
        }
    }
    #[cfg(not(target_os = "linux"))]
    sample_non_linux_resources()
}

#[cfg(not(target_os = "linux"))]
fn sample_non_linux_resources() -> ProcessResourceStats {
    #[cfg(unix)]
    {
        let pid = std::process::id();
        let out = std::process::Command::new("ps")
            .args(["-o", "rss=,%cpu=", "-p", &pid.to_string()])
            .output()
            .ok();
        if let Some(out) = out {
            if out.status.success() {
                let line = String::from_utf8_lossy(&out.stdout);
                if let Some(stats) = parse_ps_resources(&line) {
                    return stats;
                }
            }
        }
    }
    ProcessResourceStats::default()
}

#[cfg(any(all(unix, not(target_os = "linux")), test))]
fn parse_ps_resources(line: &str) -> Option<ProcessResourceStats> {
    let mut parts = line.split_whitespace();
    let rss_kb: u64 = parts.next()?.parse().ok()?;
    Some(ProcessResourceStats {
        rss_mb: Some(rss_kb / 1024),
        cpu_percent: parts.next().and_then(|value| value.parse().ok()),
    })
}

#[cfg(target_os = "linux")]
fn linux_rss_mb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(kb) = line.strip_prefix("VmRSS:") {
            let kb: u64 = kb.split_whitespace().next()?.parse().ok()?;
            return Some(kb / 1024);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_real_ps_columns_and_rejects_missing_rss() {
        let stats = parse_ps_resources("  91234  3.1\n").unwrap();
        assert_eq!(stats.rss_mb, Some(89));
        assert_eq!(stats.cpu_percent, Some(3.1));
        assert!(parse_ps_resources("RSS %CPU").is_none());
        assert!(parse_ps_resources("").is_none());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_reports_positive_rss_and_enforces_memory_limit() {
        assert!(sample_process_resources().rss_mb.unwrap() > 0);
        let guard = MemoryGuard::new(MemoryGuardConfig {
            fetch_limit_percent: 75,
            limit_mb: Some(1),
        });
        guard.sample();
        assert!(guard.is_critical());
    }
}
