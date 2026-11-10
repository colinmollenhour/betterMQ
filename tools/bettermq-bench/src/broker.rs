//! Spawns and supervises the broker under test as a child process.

use anyhow::{bail, Context};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

#[derive(Clone, Debug)]
pub struct BrokerCfg {
    pub bin: PathBuf,
    pub data_dir: PathBuf,
    pub log_path: PathBuf,
    pub fsync: String,
    pub rust_log: String,
    /// CPUs the broker may run on (`None` = unrestricted).
    pub cpus: Option<Vec<usize>>,
    pub extra_env: Vec<(String, String)>,
    /// Reuse this port (restarts must keep the same address); `None` picks a free one.
    pub port: Option<u16>,
}

pub struct Broker {
    cfg: BrokerCfg,
    child: Option<Child>,
    pub pid: i32,
    pub port: u16,
    pub base: String,
    pub token: Option<String>,
    pub version: String,
    /// Time from spawn to `/readyz` = 200.
    pub startup: Duration,
}

fn free_port() -> anyhow::Result<u16> {
    let l = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(l.local_addr()?.port())
}

impl Broker {
    /// Start the broker. Pass `token` when restarting on an existing data
    /// directory (the plaintext token cannot be recovered from disk).
    pub async fn start(
        cfg: BrokerCfg,
        http: &reqwest::Client,
        token: Option<String>,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&cfg.data_dir)?;
        let port = match cfg.port {
            Some(p) => p,
            None => free_port()?,
        };
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&cfg.log_path)
            .with_context(|| format!("open {}", cfg.log_path.display()))?;
        let mut cmd = Command::new(&cfg.bin);
        cmd.arg("serve")
            .arg("--data-dir")
            .arg(&cfg.data_dir)
            .arg("--listen")
            .arg(format!("127.0.0.1:{port}"))
            .env("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1")
            .env("BETTERMQ_FSYNC", &cfg.fsync)
            .env("RUST_LOG", &cfg.rust_log)
            .env("NO_COLOR", "1")
            .stdin(std::process::Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .kill_on_drop(true);
        for (k, v) in &cfg.extra_env {
            cmd.env(k, v);
        }
        #[cfg(target_os = "linux")]
        if let Some(cpus) = cfg.cpus.clone() {
            // SAFETY: runs between fork and exec; only calls the async-signal-safe
            // sched_setaffinity syscall on already-built plain data.
            unsafe {
                cmd.pre_exec(move || {
                    let mut set: libc::cpu_set_t = std::mem::zeroed();
                    libc::CPU_ZERO(&mut set);
                    for cpu in &cpus {
                        libc::CPU_SET(*cpu, &mut set);
                    }
                    if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
        let spawned = Instant::now();
        let child = cmd
            .spawn()
            .with_context(|| format!("spawn {}", cfg.bin.display()))?;
        let pid = child.id().map(|p| p as i32).unwrap_or(0);
        let base = format!("http://127.0.0.1:{port}");
        let mut broker = Self {
            cfg,
            child: Some(child),
            pid,
            port,
            base,
            token,
            version: String::new(),
            startup: Duration::ZERO,
        };
        broker.await_ready(http, spawned).await?;
        Ok(broker)
    }

    async fn await_ready(
        &mut self,
        http: &reqwest::Client,
        spawned: Instant,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(child) = self.child.as_mut() {
                if let Ok(Some(status)) = child.try_wait() {
                    bail!(
                        "broker exited during startup ({status}); see {}",
                        self.cfg.log_path.display()
                    );
                }
            }
            if let Ok(resp) = http.get(format!("{}/healthz", self.base)).send().await {
                if resp.status().is_success() {
                    if let Ok(v) = resp.json::<serde_json::Value>().await {
                        self.version = v["version"].as_str().unwrap_or("").to_string();
                    }
                    break;
                }
            }
            if Instant::now() > deadline {
                bail!("broker did not become healthy within 60s");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if self.token.is_none() {
            let resp = http
                .post(format!("{}/v1/local-auth/setup", self.base))
                .json(&serde_json::json!({"password": "bench-password-123"}))
                .send()
                .await?;
            if resp.status().is_success() {
                let v: serde_json::Value = resp.json().await?;
                self.token = v["token"].as_str().map(str::to_string);
            } else {
                bail!("local-auth setup failed: {}", resp.status());
            }
        }
        loop {
            let mut req = http.get(format!("{}/readyz", self.base));
            if let Some(t) = &self.token {
                req = req.bearer_auth(t);
            }
            if let Ok(resp) = req.send().await {
                if resp.status().is_success() {
                    break;
                }
            }
            if Instant::now() > deadline {
                bail!("broker did not become ready within 60s");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        self.startup = spawned.elapsed();
        Ok(())
    }

    /// SIGTERM and wait; returns how long graceful shutdown took.
    pub async fn stop_graceful(&mut self, timeout: Duration) -> anyhow::Result<Duration> {
        let Some(mut child) = self.child.take() else {
            return Ok(Duration::ZERO);
        };
        let started = Instant::now();
        // SAFETY: plain signal to our own child.
        unsafe {
            libc::kill(self.pid, libc::SIGTERM);
        }
        match tokio::time::timeout(timeout, child.wait()).await {
            Ok(_) => {}
            Err(_) => {
                let _ = child.kill().await;
                bail!("broker ignored SIGTERM for {timeout:?}; killed");
            }
        }
        self.pid = 0;
        Ok(started.elapsed())
    }

    /// SIGKILL, simulating a crash.
    pub async fn kill9(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill().await;
        }
        self.pid = 0;
    }

    pub fn cfg(&self) -> &BrokerCfg {
        &self.cfg
    }

    pub fn is_running(&mut self) -> bool {
        match self.child.as_mut() {
            Some(c) => matches!(c.try_wait(), Ok(None)),
            None => false,
        }
    }
}

/// Strip volatile parts (ids, numbers, timestamps) so repeated messages group.
fn normalise_log_line(line: &str) -> String {
    let body = match line.find(" WARN ").or_else(|| line.find(" ERROR ")) {
        Some(i) => line[i + 1..].trim_start(),
        None => line,
    };
    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() {
            while chars
                .peek()
                .is_some_and(|n| n.is_ascii_alphanumeric() || *n == '-')
            {
                chars.next();
            }
            out.push('N');
        } else if c.is_ascii_hexdigit() && chars.peek().is_some_and(|n| n.is_ascii_hexdigit()) {
            // part of an id such as `8a752f91-...`: swallow the whole token
            let mut len = 1;
            while chars
                .peek()
                .is_some_and(|n| n.is_ascii_hexdigit() || *n == '-')
            {
                chars.next();
                len += 1;
            }
            if len >= 8 {
                out.push_str("ID");
            } else {
                out.push(c);
            }
        } else {
            out.push(c);
        }
    }
    out.chars().take(200).collect()
}

/// Count WARN/ERROR lines, total bytes, and the most frequent messages.
pub fn log_stats(path: &Path) -> (u64, u64, u64, Vec<(String, u64)>) {
    let Ok(text) = std::fs::read_to_string(path) else {
        return (0, 0, 0, Vec::new());
    };
    let mut warn = 0;
    let mut err = 0;
    let mut groups: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for line in text.lines() {
        let is_err = line.contains(" ERROR ");
        if is_err {
            err += 1;
        } else if line.contains(" WARN ") {
            warn += 1;
        } else {
            continue;
        }
        *groups.entry(normalise_log_line(line)).or_default() += 1;
    }
    let mut top: Vec<(String, u64)> = groups.into_iter().collect();
    top.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    top.truncate(6);
    (warn, err, text.len() as u64, top)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalises_ids_and_numbers() {
        let a = normalise_log_line("2026-11-09T21:00:00.1Z  WARN broker_dispatch::worker: webhook non-success status=503 lane_owner=8a752f91-c4ca-408b-b106-e22781ac9b8f attempt=3 elapsed_ms=2581");
        let b = normalise_log_line("2026-11-09T21:00:09.9Z  WARN broker_dispatch::worker: webhook non-success status=503 lane_owner=0b1c2d3e-aaaa-bbbb-cccc-123456789abc attempt=1 elapsed_ms=9");
        assert_eq!(a, b);
        assert!(a.starts_with("WARN"));
    }
}
