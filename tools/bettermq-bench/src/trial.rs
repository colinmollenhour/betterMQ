//! One isolated trial: a fresh broker + data dir, sink, ledger, and recorder.
//! Scenarios drive it through phases and read back [`TrialResult`].

use crate::analysis;
use crate::api::{Api, QueueSpec};
use crate::broker::{log_stats, Broker, BrokerCfg};
use crate::loadgen::{Plan, Sender, SenderCfg};
use crate::model::*;
use crate::recorder::{PhaseStats, Recorder};
use crate::sink::{DestCfg, Sink};
use crate::stats::{Clock, Shared};
use crate::verify::Verifier;
use anyhow::Context;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Smoke,
    Standard,
    Full,
}

impl Profile {
    pub fn pick<T>(self, smoke: T, standard: T, full: T) -> T {
        match self {
            Self::Smoke => smoke,
            Self::Standard => standard,
            Self::Full => full,
        }
    }

    pub fn name(self) -> &'static str {
        self.pick("smoke", "standard", "full")
    }
}

pub struct RunCtx {
    pub broker_bin: PathBuf,
    /// Where broker data directories and logs live (put this on the disk under test).
    pub data_root: PathBuf,
    pub profile: Profile,
    pub seed: u64,
    pub fsync: String,
    pub rust_log: String,
    pub broker_cpus: Option<Vec<usize>>,
    pub slo: Slo,
    pub keep_data: bool,
    /// Multiplies every scenario's offered rates (for slower/faster machines).
    pub rate_scale: f64,
    /// Multiplies every scenario's durations.
    pub time_scale: f64,
    pub extra_env: Vec<(String, String)>,
}

impl RunCtx {
    pub fn secs(&self, base: f64) -> Duration {
        Duration::from_secs_f64((base * self.time_scale).max(1.0))
    }

    pub fn rate(&self, base: f64) -> f64 {
        (base * self.rate_scale).max(1.0)
    }
}

pub struct Trial {
    pub ctx: Arc<RunCtx>,
    pub index: u32,
    pub seed: u64,
    pub shared: Arc<Shared>,
    pub verifier: Arc<Verifier>,
    pub clock: Clock,
    pub sink: Sink,
    pub broker: Broker,
    pub api: Api,
    pub http: reqwest::Client,
    sender_client: reqwest::Client,
    pub recorder: Arc<Recorder>,
    pid: Arc<AtomicI32>,
    dir: PathBuf,
    token: Option<String>,
    pub metrics: Vec<Metric>,
    pub tables: Vec<Table>,
    pub notes: Vec<String>,
    pub steps: Vec<StepResult>,
    /// Messages intentionally left undelivered at the end (e.g. far-future delays).
    pub allow_undelivered: bool,
    shutdown_s: Option<f64>,
    /// The broker was restarted: RSS before/after compares different processes.
    restarted: bool,
}

pub fn client(conns: usize) -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(conns)
        .tcp_nodelay(true)
        .build()
        .expect("static client config is valid")
}

impl Trial {
    pub async fn start(
        ctx: Arc<RunCtx>,
        scenario: &str,
        index: u32,
        seed: u64,
    ) -> anyhow::Result<Self> {
        Self::start_with_env(ctx, scenario, index, seed, Vec::new()).await
    }

    /// Like [`start`](Self::start), with scenario-specific broker environment
    /// (applied after, and overriding, `--broker-env`).
    pub async fn start_with_env(
        ctx: Arc<RunCtx>,
        scenario: &str,
        index: u32,
        seed: u64,
        env: Vec<(String, String)>,
    ) -> anyhow::Result<Self> {
        let dir = ctx.data_root.join(format!("{scenario}-t{index}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        let http = client(64);
        let cfg = BrokerCfg {
            bin: ctx.broker_bin.clone(),
            data_dir: dir.join("data"),
            log_path: dir.join("broker.log"),
            fsync: ctx.fsync.clone(),
            rust_log: ctx.rust_log.clone(),
            cpus: ctx.broker_cpus.clone(),
            extra_env: ctx.extra_env.iter().cloned().chain(env).collect(),
            port: None,
        };
        let broker = Broker::start(cfg.clone(), &http, None)
            .await
            .context("start broker")?;
        let token = broker.token.clone();
        let api = Api::new(http.clone(), &broker.base, token.clone());
        let shared = Arc::new(Shared::default());
        let verifier = Arc::new(Verifier::default());
        let clock = Clock::new();
        let sink = Sink::start(shared.clone(), verifier.clone(), clock, seed).await?;
        let pid = Arc::new(AtomicI32::new(broker.pid));
        let recorder = Recorder::new(
            shared.clone(),
            verifier.clone(),
            api.clone(),
            pid.clone(),
            cfg.data_dir.clone(),
            cfg.log_path.clone(),
        );
        recorder.spawn();
        let startup = broker.startup.as_secs_f64();
        let mut trial = Self {
            ctx,
            index,
            seed,
            shared,
            verifier,
            clock,
            sink,
            broker,
            api,
            http,
            sender_client: client(512),
            recorder,
            pid,
            dir,
            token,
            metrics: Vec::new(),
            tables: Vec::new(),
            notes: Vec::new(),
            steps: Vec::new(),
            allow_undelivered: false,
            shutdown_s: None,
            restarted: false,
        };
        trial.metrics.push(Metric::new(
            "startup_s",
            "Broker cold start to ready",
            "s",
            startup,
            Direction::LowerBetter,
            "lifecycle",
        ));
        Ok(trial)
    }

    pub fn push(&mut self, m: Metric) {
        self.metrics.push(m);
    }

    pub fn note(&mut self, s: impl Into<String>) {
        self.notes.push(s.into());
    }

    pub fn sender(&self, cfg: SenderCfg) -> Sender {
        Sender::new(
            self.sender_client.clone(),
            &self.broker.base,
            self.token.clone(),
            cfg,
            self.shared.clone(),
            self.clock,
        )
    }

    /// Register a destination and create a queue that delivers to it.
    pub async fn queue(
        &self,
        name: &str,
        dest: DestCfg,
        parallelism: u32,
        max_retries: u32,
        backoff: Option<(u64, u64, f64)>,
    ) -> anyhow::Result<usize> {
        let idx = self.sink.add_dest(dest.parallelism(parallelism));
        self.api
            .create_queue(
                &QueueSpec {
                    name: name.into(),
                    url: self.sink.url(idx),
                    parallelism,
                    max_retries,
                    backoff,
                },
                "s3cret",
            )
            .await?;
        Ok(idx)
    }

    pub async fn phase(&self, name: &str, kind: PhaseKind) -> Option<PhaseStats> {
        self.recorder.phase(name, kind).await
    }

    pub async fn end_phase(&self) -> Option<PhaseStats> {
        self.recorder.end_phase().await
    }

    /// Build a plan whose time zero is "now" (`make` receives that instant on
    /// the shared clock, in microseconds) and run it to completion.
    pub async fn drive<F>(&self, cfg: SenderCfg, make: F) -> u64
    where
        F: FnOnce(u64) -> Box<dyn Plan>,
    {
        let started = Instant::now();
        let plan = make(self.clock.us_at(started));
        let sender = self.sender(cfg);
        sender
            .run_from(plan, Arc::new(AtomicBool::new(false)), started)
            .await
    }

    /// Wait until every acknowledged message is delivered or dead-lettered.
    /// Returns the time waited, or `None` on timeout or when delivery has made
    /// no progress for 60 s (a wedged or circuit-broken destination: waiting
    /// longer would only burn wall time).
    pub async fn wait_drained(&self, timeout: Duration) -> Option<Duration> {
        self.wait_drained_stall(timeout, Duration::from_secs(60))
            .await
    }

    /// Like [`wait_drained`](Self::wait_drained) but only gives up at `timeout`:
    /// for restarts and circuit-broken destinations, where long quiet spells
    /// are expected and the wait itself is the measurement.
    pub async fn wait_drained_patient(&self, timeout: Duration) -> Option<Duration> {
        self.wait_drained_stall(timeout, timeout).await
    }

    async fn wait_drained_stall(&self, timeout: Duration, stall: Duration) -> Option<Duration> {
        let start = Instant::now();
        let mut last_progress = Instant::now();
        let mut last_done = u64::MAX;
        // Require two consecutive empty readings so in-transit acks are included.
        let mut empty = 0;
        loop {
            if self.verifier.pending() == 0 {
                empty += 1;
                if empty >= 2 {
                    return Some(start.elapsed());
                }
            } else {
                empty = 0;
            }
            let (_, done, perm) = self.verifier.totals();
            if done + perm != last_done {
                last_done = done + perm;
                last_progress = Instant::now();
            }
            if start.elapsed() > timeout || last_progress.elapsed() > stall {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Sleep while the recorder keeps sampling.
    pub async fn idle(&self, d: Duration) {
        tokio::time::sleep(d).await;
    }

    /// Crash (SIGKILL) or gracefully stop the broker, then start it again on
    /// the same data directory. Returns startup time.
    pub async fn restart_broker(&mut self, crash: bool) -> anyhow::Result<Duration> {
        if crash {
            self.broker.kill9().await;
        } else {
            let d = self.broker.stop_graceful(Duration::from_secs(120)).await?;
            self.shutdown_s = Some(d.as_secs_f64());
        }
        self.pid.store(0, Ordering::Relaxed);
        self.restarted = true;
        let mut cfg = self.broker.cfg().clone();
        cfg.port = Some(self.broker.port);
        let fresh = Broker::start(cfg, &self.http, self.token.clone()).await?;
        self.pid.store(fresh.pid, Ordering::Relaxed);
        self.api = self.api.with_token(&fresh.base, self.token.clone());
        let startup = fresh.startup;
        self.broker = fresh;
        Ok(startup)
    }

    pub async fn finish(mut self) -> anyhow::Result<TrialResult> {
        let (series, phases, _peak) = self.recorder.finish().await;
        let snapshot = self.recorder.last_snapshot().await;
        // Across a restart "before" and "after" are different processes.
        let memory = if self.restarted {
            None
        } else {
            analysis::memory_release(&series, &phases, 15.0)
        };

        let streams = self.verifier.all();
        let mut integrity = Integrity {
            loss_expected_zero: !self.allow_undelivered,
            ..Default::default()
        };
        for s in &streams {
            integrity.accepted += s.acked.load(Ordering::Relaxed);
            integrity.delivered_unique += s.done.load(Ordering::Relaxed);
            integrity.duplicate_deliveries += s.dups.load(Ordering::Relaxed);
            integrity.order_violations += s.inversions.load(Ordering::Relaxed);
            integrity.order_violations_after_retry += s.inversions_retry.load(Ordering::Relaxed);
            integrity.lost += s.count_lost();
            integrity.malformed += s.out_of_range.load(Ordering::Relaxed);
        }
        integrity.malformed += self.shared.c.sink_malformed.load(Ordering::Relaxed);
        for d in self.sink.dests() {
            let observed = d.max_seen();
            if d.cfg.configured_parallelism > 0 && observed > d.cfg.configured_parallelism {
                integrity.concurrency_violations += 1;
            }
            integrity.concurrency.push(ConcurrencyCheck {
                dest: d.cfg.name.clone(),
                configured: d.cfg.configured_parallelism,
                observed_max: observed,
            });
        }

        let mut notes = std::mem::take(&mut self.notes);
        // Messages the broker gave up on after exhausting retries sit in the
        // DLQ: they are accounted for, not lost.
        if integrity.lost > 0 && self.broker.is_running() {
            let perm: u64 = streams.iter().map(|s| s.perm.load(Ordering::Relaxed)).sum();
            let exhausted = self.api.dlq_total().await.saturating_sub(perm);
            if exhausted > 0 {
                let moved = exhausted.min(integrity.lost);
                integrity.lost -= moved;
                notes.push(format!(
                    "{moved} acknowledged messages exhausted their retries and are in the dead-letter queue (not counted as lost)."
                ));
            }
        }
        for (what, n) in self.shared.errors.lock().iter() {
            notes.push(format!("Ingest error x{n}: {what}"));
        }
        let mut shutdown_error = None;
        if self.broker.is_running() {
            match self.broker.stop_graceful(Duration::from_secs(90)).await {
                Ok(d) => self.shutdown_s = self.shutdown_s.or(Some(d.as_secs_f64())),
                Err(e) => {
                    shutdown_error = Some(e.to_string());
                    notes.push(format!("graceful shutdown failed: {e}"));
                }
            }
        }
        if let Some(s) = self.shutdown_s {
            self.metrics.push(Metric::new(
                "shutdown_s",
                "Graceful shutdown time",
                "s",
                s,
                Direction::LowerBetter,
                "lifecycle",
            ));
        }
        self.sink.shutdown();

        let log_path = self.dir.join("broker.log");
        let (warns, errs, bytes, top) = log_stats(&log_path);
        let keep = self.ctx.keep_data;
        if !keep {
            let _ = std::fs::remove_dir_all(self.dir.join("data"));
        }
        trim_log(&log_path, 300);

        let mut metrics = std::mem::take(&mut self.metrics);
        if let Some(m) = &memory {
            let mem = |id: &str, label: &str, v: f64, dir: Direction, headline: bool| {
                let mut metric = Metric::new(id, label, "MB", v, dir, "memory");
                metric.headline = headline;
                metric
            };
            metrics.push(mem(
                "mem.idle_mb",
                "RSS at idle (before load)",
                m.idle_start_mb,
                Direction::LowerBetter,
                false,
            ));
            metrics.push(mem(
                "mem.peak_mb",
                "Peak RSS under load",
                m.peak_mb,
                Direction::LowerBetter,
                true,
            ));
            metrics.push(mem(
                "mem.settled_mb",
                "RSS after settling idle",
                m.settled_mb,
                Direction::LowerBetter,
                true,
            ));
            let mut r = Metric::new(
                "mem.retained_pct",
                "Memory still held after load (of the growth)",
                "%",
                m.retained_pct,
                Direction::LowerBetter,
                "memory",
            );
            r.headline = true;
            metrics.push(r);
            metrics.push(Metric::new(
                "disk.retained_pct",
                "Disk still held after load (of the growth)",
                "%",
                m.disk_retained_pct,
                Direction::LowerBetter,
                "memory",
            ));
        }
        let host = |name: &str| analysis::mean(&series.values(name));
        metrics.push(Metric::new(
            "host.cpu_busy_pct",
            "Whole-host CPU busy during the trial (noise indicator)",
            "%",
            host(crate::recorder::col::HOST_CPU),
            Direction::Info,
            "resources",
        ));
        metrics.push(Metric::new(
            "host.cpu_pressure_pct",
            "Kernel CPU pressure during the trial (noise indicator)",
            "%",
            host(crate::recorder::col::PSI),
            Direction::Info,
            "resources",
        ));
        metrics.push(Metric::new(
            "log.bytes",
            "Broker log volume",
            "MB",
            bytes as f64 / 1_048_576.0,
            Direction::LowerBetter,
            "resources",
        ));

        Ok(TrialResult {
            shutdown_error,
            index: self.index,
            seed: self.seed,
            duration_s: series.t.last().copied().unwrap_or(0.0),
            phases,
            series,
            steps: std::mem::take(&mut self.steps),
            memory,
            integrity: Some(integrity),
            metrics,
            tables: std::mem::take(&mut self.tables),
            notes,
            broker_log_warnings: warns,
            broker_log_errors: errs,
            broker_log_bytes: bytes,
            broker_log_top: top,
            final_counters: snapshot.map(|s| s.flat()).unwrap_or_default(),
        })
    }
}

/// Replace a possibly enormous broker log with its error lines and tail.
fn trim_log(path: &std::path::Path, tail_lines: usize) {
    let Ok(text) = std::fs::read_to_string(path) else {
        return;
    };
    let lines: Vec<&str> = text.lines().collect();
    let mut keep: Vec<&str> = lines
        .iter()
        .filter(|l| l.contains(" ERROR ") || l.contains(" WARN "))
        .take(200)
        .copied()
        .collect();
    keep.push("---- tail ----");
    keep.extend(lines.iter().rev().take(tail_lines).rev());
    let _ = std::fs::write(path, keep.join("\n"));
}
