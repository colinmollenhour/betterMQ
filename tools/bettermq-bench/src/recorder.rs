//! Once-per-second sampler that joins client counters, sink counters, broker
//! `/proc` stats, and broker Prometheus metrics into one aligned time series,
//! and accumulates per-phase aggregates for the scenario code to read.

use crate::api::Api;
use crate::metrics::{interval_quantile, Snapshot};
use crate::model::{Phase, PhaseKind, Series};
use crate::procfs::{self, ProcSample};
use crate::stats::{ld, max_ms, new_hist, q_ms, Shared};
use crate::verify::Verifier;
use hdrhistogram::Histogram;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub mod col {
    pub const OFFERED: &str = "offered_s";
    pub const ACKED: &str = "acked_s";
    pub const ERR: &str = "err_s";
    pub const DELIVERED: &str = "delivered_s";
    pub const SINK_RECV: &str = "sink_recv_s";
    pub const ACK_P50: &str = "ack_p50_ms";
    pub const ACK_P99: &str = "ack_p99_ms";
    pub const GENLAG_P99: &str = "genlag_p99_ms";
    pub const DELIV_P50: &str = "delivery_p50_ms";
    pub const DELIV_P99: &str = "delivery_p99_ms";
    pub const CRON_LATE_P99: &str = "cron_late_p99_ms";
    pub const BACKLOG: &str = "backlog";
    pub const CLIENT_INFLIGHT: &str = "client_inflight";
    pub const SINK_INFLIGHT: &str = "sink_inflight";
    pub const CPU: &str = "cpu_cores";
    pub const CPU_USER: &str = "cpu_user_cores";
    pub const CPU_SYS: &str = "cpu_sys_cores";
    pub const RSS: &str = "rss_mb";
    pub const ANON: &str = "anon_mb";
    pub const FILE: &str = "file_mb";
    pub const SWAP: &str = "swap_mb";
    pub const MAJFLT: &str = "major_faults_s";
    pub const THREADS: &str = "threads";
    pub const FDS: &str = "fds";
    pub const DISK_W: &str = "disk_w_mb_s";
    pub const DISK_R: &str = "disk_r_mb_s";
    pub const DATA: &str = "data_mb";
    pub const CTX_VOL: &str = "ctx_vol_s";
    pub const CTX_INVOL: &str = "ctx_invol_s";
    pub const HARNESS_CPU: &str = "harness_cpu_cores";
    pub const HOST_CPU: &str = "host_cpu_pct";
    pub const LOG_KB: &str = "log_kb_s";
    pub const PSI: &str = "host_cpu_psi_pct";
    pub const THREAD_PREFIX: &str = "cpu_thr:";
    pub const BM_BLOCKED: &str = "bm_hosts_blocked";
    pub const BM_INFLIGHT: &str = "bm_inflight";
    pub const BM_READY: &str = "bm_ready_depth";
    pub const BM_RETRIES: &str = "bm_retries_pending";
    pub const BM_DLQ: &str = "bm_dlq_pending";
    pub const BM_FSYNC_P99: &str = "bm_fsync_p99_ms";
    pub const BM_FSYNC_S: &str = "bm_fsync_s";
    pub const BM_REC_PER_FSYNC: &str = "bm_records_per_fsync";
    pub const BM_ACK_P99: &str = "bm_ack_p99_ms";
    pub const BM_SHARD_Q: &str = "bm_shard_queue_records";
}

/// Everything a scenario wants to know about one finished phase.
pub struct PhaseStats {
    pub start_s: f64,
    pub end_s: f64,
    pub dur_s: f64,
    pub offered: u64,
    pub requests: u64,
    pub acked: u64,
    pub rejected: u64,
    pub failed: u64,
    pub dropped: u64,
    pub delivered: u64,
    pub payload_bytes: u64,
    pub ack: Histogram<u64>,
    pub delivery: Histogram<u64>,
    pub delivery_retried: Histogram<u64>,
    pub cron_jitter: Histogram<u64>,
    pub cpu_cores: f64,
    pub cpu_sys_cores: f64,
    pub cpu_seconds: f64,
    pub rss_mb_end: f64,
    pub data_mb_end: f64,
    pub disk_write_mb: f64,
    pub log_bytes: u64,
    pub backlog_start: f64,
    pub backlog_end: f64,
}

impl PhaseStats {
    pub fn secs(&self) -> f64 {
        self.dur_s.max(1e-9)
    }
    pub fn acked_rate(&self) -> f64 {
        self.acked as f64 / self.secs()
    }
    pub fn delivered_rate(&self) -> f64 {
        self.delivered as f64 / self.secs()
    }
    pub fn error_rate(&self) -> f64 {
        if self.offered == 0 {
            0.0
        } else {
            (self.rejected + self.failed + self.dropped) as f64 / self.offered as f64
        }
    }
    pub fn ack_ms(&self, q: f64) -> f64 {
        q_ms(&self.ack, q).unwrap_or(f64::NAN)
    }
    pub fn delivery_ms(&self, q: f64) -> f64 {
        q_ms(&self.delivery, q).unwrap_or(f64::NAN)
    }
    pub fn ack_max_ms(&self) -> f64 {
        max_ms(&self.ack).unwrap_or(f64::NAN)
    }
    pub fn backlog_growth_per_s(&self) -> f64 {
        (self.backlog_end - self.backlog_start) / self.secs()
    }
}

struct Counts {
    offered: u64,
    requests: u64,
    acked: u64,
    rejected: u64,
    failed: u64,
    dropped: u64,
    delivered: u64,
    payload: u64,
    sink_recv: u64,
}

struct Acc {
    name: String,
    kind: PhaseKind,
    start_s: f64,
    c0: Counts,
    p0: Option<ProcSample>,
    t0: Instant,
    log0: u64,
    backlog0: f64,
    ack: Histogram<u64>,
    delivery: Histogram<u64>,
    delivery_retried: Histogram<u64>,
    cron_jitter: Histogram<u64>,
    disk_w0: u64,
}

struct Prev {
    pid: i32,
    threads: Vec<(String, f64)>,
    t: Instant,
    c: Counts,
    proc: Option<ProcSample>,
    me: Option<ProcSample>,
    host: Option<(u64, u64)>,
    fsync_buckets: Vec<(f64, f64)>,
    ack_buckets: Vec<(f64, f64)>,
    fsync_total: f64,
    fsync_records: f64,
    log_bytes: u64,
}

struct Inner {
    start: Instant,
    series: Series,
    phases: Vec<Phase>,
    acc: Option<Acc>,
    prev: Prev,
    last_proc: Option<ProcSample>,
    last_snapshot: Option<Snapshot>,
    last_data_mb: f64,
    last_log_bytes: u64,
    rss_peak_mb: f64,
}

pub struct Recorder {
    shared: Arc<Shared>,
    verifier: Arc<Verifier>,
    api: Api,
    pid: Arc<AtomicI32>,
    data_dir: PathBuf,
    log_path: PathBuf,
    inner: Mutex<Inner>,
    stop: AtomicBool,
}

struct Sampled {
    threads: Vec<(String, f64)>,
    psi: Option<f64>,
    proc: Option<ProcSample>,
    me: Option<ProcSample>,
    host: Option<(u64, u64)>,
    data_bytes: u64,
    log_bytes: u64,
}

fn counts(shared: &Shared, verifier: &Verifier) -> Counts {
    let c = &shared.c;
    let (_, done, _) = verifier.totals();
    Counts {
        offered: ld(&c.offered),
        requests: ld(&c.requests),
        acked: ld(&c.acked),
        rejected: ld(&c.rejected),
        failed: ld(&c.failed),
        dropped: ld(&c.dropped),
        delivered: done,
        payload: ld(&c.payload_bytes),
        sink_recv: ld(&c.sink_recv),
    }
}

fn mb(kb: u64) -> f64 {
    kb as f64 / 1024.0
}

fn some(v: f64) -> Option<f64> {
    v.is_finite().then_some(v)
}

impl Recorder {
    pub fn new(
        shared: Arc<Shared>,
        verifier: Arc<Verifier>,
        api: Api,
        pid: Arc<AtomicI32>,
        data_dir: PathBuf,
        log_path: PathBuf,
    ) -> Arc<Self> {
        let now = Instant::now();
        let prev = Prev {
            pid: pid.load(Ordering::Relaxed),
            threads: procfs::read_thread_cpu(pid.load(Ordering::Relaxed)),
            t: now,
            c: counts(&shared, &verifier),
            proc: procfs::read_proc(pid.load(Ordering::Relaxed)),
            me: procfs::read_proc(std::process::id() as i32),
            host: procfs::read_host_cpu(),
            fsync_buckets: Vec::new(),
            ack_buckets: Vec::new(),
            fsync_total: 0.0,
            fsync_records: 0.0,
            log_bytes: 0,
        };
        Arc::new(Self {
            shared,
            verifier,
            api,
            pid,
            data_dir,
            log_path,
            inner: Mutex::new(Inner {
                start: now,
                series: Series::default(),
                phases: Vec::new(),
                acc: None,
                prev,
                last_proc: None,
                last_snapshot: None,
                last_data_mb: 0.0,
                last_log_bytes: 0,
                rss_peak_mb: 0.0,
            }),
            stop: AtomicBool::new(false),
        })
    }

    /// Spawn the periodic tick loop.
    pub fn spawn(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_secs(1));
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            iv.tick().await;
            while !this.stop.load(Ordering::Relaxed) {
                iv.tick().await;
                this.tick().await;
            }
        })
    }

    pub fn halt(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub async fn last_snapshot(&self) -> Option<Snapshot> {
        self.inner.lock().await.last_snapshot.clone()
    }

    pub async fn tick(&self) {
        let mut g = self.inner.lock().await;
        if Instant::now().duration_since(g.prev.t).as_secs_f64() < 0.02 {
            return;
        }
        let pid = self.pid.load(Ordering::Relaxed);
        let data_dir = self.data_dir.clone();
        let log_path = self.log_path.clone();
        let sampled = tokio::task::spawn_blocking(move || Sampled {
            threads: procfs::read_thread_cpu(pid),
            psi: procfs::read_psi_cpu(),
            proc: procfs::read_proc(pid),
            me: procfs::read_proc(std::process::id() as i32),
            host: procfs::read_host_cpu(),
            data_bytes: procfs::dir_usage_bytes(&data_dir),
            log_bytes: std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0),
        })
        .await
        .unwrap_or(Sampled {
            threads: Vec::new(),
            psi: None,
            proc: None,
            me: None,
            host: None,
            data_bytes: 0,
            log_bytes: 0,
        });
        let snapshot = self.api.metrics_text().await.map(|t| Snapshot::parse(&t));
        // Stamp after sampling so rates use the time the data was actually read.
        let now = Instant::now();
        let dt = now.duration_since(g.prev.t).as_secs_f64().max(1e-3);
        let t_s = now.duration_since(g.start).as_secs_f64();
        // A new process starts its counters and CPU time from zero: drop the
        // old baselines so the first interval is not computed across them.
        let restarted = g.prev.pid != pid;
        if restarted {
            g.prev.proc = None;
            g.prev.threads.clear();
        }

        let cur = counts(&self.shared, &self.verifier);
        let rate = |a: u64, b: u64| a.saturating_sub(b) as f64 / dt;
        let l = &self.shared.l;
        let ack = l.ack.take();
        let gen_lag = l.gen_lag.take();
        let delivery = l.delivery.take();
        let retried = l.delivery_retried.take();
        let cron = l.cron_jitter.take();
        let backlog = self.verifier.pending() as f64;

        let mut row: Vec<(&str, Option<f64>)> = vec![
            (col::OFFERED, Some(rate(cur.offered, g.prev.c.offered))),
            (col::ACKED, Some(rate(cur.acked, g.prev.c.acked))),
            (
                col::ERR,
                Some(
                    rate(cur.rejected, g.prev.c.rejected)
                        + rate(cur.failed, g.prev.c.failed)
                        + rate(cur.dropped, g.prev.c.dropped),
                ),
            ),
            (
                col::DELIVERED,
                Some(rate(cur.delivered, g.prev.c.delivered)),
            ),
            (
                col::SINK_RECV,
                Some(rate(cur.sink_recv, g.prev.c.sink_recv)),
            ),
            (col::ACK_P50, q_ms(&ack, 0.50)),
            (col::ACK_P99, q_ms(&ack, 0.99)),
            (col::GENLAG_P99, q_ms(&gen_lag, 0.99)),
            (col::DELIV_P50, q_ms(&delivery, 0.50)),
            (col::DELIV_P99, q_ms(&delivery, 0.99)),
            (col::CRON_LATE_P99, q_ms(&cron, 0.99)),
            (col::BACKLOG, Some(backlog)),
            (
                col::CLIENT_INFLIGHT,
                Some(self.shared.c.client_inflight.load(Ordering::Relaxed) as f64),
            ),
            (
                col::SINK_INFLIGHT,
                Some(self.shared.c.sink_inflight.load(Ordering::Relaxed) as f64),
            ),
        ];

        let mut rss_now = 0.0;
        if let Some(p) = sampled.proc {
            let prev = g.prev.proc.filter(|pp| p.cpu_user_s >= pp.cpu_user_s);
            let (du, ds, dw, dr, dcv, dci, dmaj) = match prev {
                Some(pp) => (
                    p.cpu_user_s - pp.cpu_user_s,
                    p.cpu_sys_s - pp.cpu_sys_s,
                    p.write_bytes.saturating_sub(pp.write_bytes),
                    p.read_bytes.saturating_sub(pp.read_bytes),
                    p.ctx_vol.saturating_sub(pp.ctx_vol),
                    p.ctx_invol.saturating_sub(pp.ctx_invol),
                    p.majflt.saturating_sub(pp.majflt),
                ),
                None => (0.0, 0.0, 0, 0, 0, 0, 0),
            };
            rss_now = mb(p.rss_kb);
            row.extend([
                (col::CPU, Some((du + ds) / dt)),
                (col::CPU_USER, Some(du / dt)),
                (col::CPU_SYS, Some(ds / dt)),
                (col::RSS, Some(rss_now)),
                (col::ANON, Some(mb(p.anon_kb))),
                (col::FILE, Some(mb(p.file_kb))),
                (col::SWAP, Some(mb(p.swap_kb))),
                (col::MAJFLT, Some(dmaj as f64 / dt)),
                (col::THREADS, Some(p.threads as f64)),
                (col::FDS, Some(p.fds as f64)),
                (col::DISK_W, Some(dw as f64 / dt / 1_048_576.0)),
                (col::DISK_R, Some(dr as f64 / dt / 1_048_576.0)),
                (col::CTX_VOL, Some(dcv as f64 / dt)),
                (col::CTX_INVOL, Some(dci as f64 / dt)),
            ]);
        }
        if let (Some(me), Some(pm)) = (sampled.me, g.prev.me) {
            let used = (me.cpu_user_s + me.cpu_sys_s) - (pm.cpu_user_s + pm.cpu_sys_s);
            row.push((col::HARNESS_CPU, Some(used.max(0.0) / dt)));
        }
        if let (Some((b, t)), Some((pb, pt))) = (sampled.host, g.prev.host) {
            let dtot = t.saturating_sub(pt);
            if dtot > 0 {
                row.push((
                    col::HOST_CPU,
                    Some(100.0 * b.saturating_sub(pb) as f64 / dtot as f64),
                ));
            }
        }
        let mut thread_cols: Vec<(String, Option<f64>)> = Vec::new();
        for (name, cpu_s) in &sampled.threads {
            let before = g
                .prev
                .threads
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| *v)
                .unwrap_or(*cpu_s);
            thread_cols.push((
                format!("{}{name}", col::THREAD_PREFIX),
                Some((cpu_s - before).max(0.0) / dt),
            ));
        }
        row.push((col::PSI, sampled.psi));
        let data_mb = sampled.data_bytes as f64 / 1_048_576.0;
        row.push((col::DATA, Some(data_mb)));
        row.push((
            col::LOG_KB,
            Some(sampled.log_bytes.saturating_sub(g.prev.log_bytes) as f64 / 1024.0 / dt),
        ));

        let mut fsync_buckets = g.prev.fsync_buckets.clone();
        let mut ack_buckets = g.prev.ack_buckets.clone();
        let (mut fsync_total, mut fsync_records) = (g.prev.fsync_total, g.prev.fsync_records);
        if let Some(s) = &snapshot {
            let fb = s.buckets("bettermq_wal_fsync_duration_seconds_bucket");
            let ab = s.buckets("bettermq_ingest_ack_latency_ms_bucket");
            let ft = s.sum("bettermq_wal_fsync_total").unwrap_or(0.0);
            let fr = s.sum("bettermq_wal_fsync_records_total").unwrap_or(0.0);
            let (pfb, pab, pft, pfr) = if restarted {
                (fb.clone(), ab.clone(), ft, fr)
            } else {
                (
                    g.prev.fsync_buckets.clone(),
                    g.prev.ack_buckets.clone(),
                    g.prev.fsync_total,
                    g.prev.fsync_records,
                )
            };
            row.push((
                col::BM_FSYNC_P99,
                interval_quantile(&pfb, &fb, 0.99).map(|v| v * 1000.0),
            ));
            row.push((col::BM_FSYNC_S, Some((ft - pft).max(0.0) / dt)));
            let dft = ft - pft;
            row.push((col::BM_REC_PER_FSYNC, (dft > 0.0).then(|| (fr - pfr) / dft)));
            row.push((col::BM_ACK_P99, interval_quantile(&pab, &ab, 0.99)));
            row.push((
                col::BM_INFLIGHT,
                s.sum("bettermq_dispatch_in_flight_deliveries"),
            ));
            row.push((
                col::BM_READY,
                s.sum("bettermq_dispatch_committed_ready_depth"),
            ));
            row.push((col::BM_RETRIES, s.sum("bettermq_dispatch_retries_pending")));
            row.push((
                col::BM_DLQ,
                s.sum("bettermq_dispatch_dlq_pending_committed"),
            ));
            row.push((col::BM_SHARD_Q, s.sum("bettermq_shard_queue_records")));
            row.push((
                col::BM_BLOCKED,
                s.sum("bettermq_dispatch_destination_hosts_blocked"),
            ));
            fsync_buckets = fb;
            ack_buckets = ab;
            fsync_total = ft;
            fsync_records = fr;
        }

        // Append the row, back-filling columns that appear late and leaving
        // gaps (None) for columns not produced this tick.
        let len = g.series.t.len();
        g.series.t.push(t_s);
        let all_cols = row
            .into_iter()
            .map(|(n, v)| (n.to_string(), v))
            .chain(thread_cols);
        for (name, value) in all_cols {
            let c = g.series.cols.entry(name).or_insert_with(|| vec![None; len]);
            while c.len() < len {
                c.push(None);
            }
            c.push(value.and_then(some));
        }
        for c in g.series.cols.values_mut() {
            while c.len() < len + 1 {
                c.push(None);
            }
        }

        if let Some(acc) = g.acc.as_mut() {
            let _ = acc.ack.add(&ack);
            let _ = acc.delivery.add(&delivery);
            let _ = acc.delivery_retried.add(&retried);
            let _ = acc.cron_jitter.add(&cron);
        }
        g.rss_peak_mb = g.rss_peak_mb.max(rss_now);
        g.last_data_mb = data_mb;
        g.last_log_bytes = sampled.log_bytes;
        if sampled.proc.is_some() {
            g.last_proc = sampled.proc;
        }
        if snapshot.is_some() {
            g.last_snapshot = snapshot;
        }
        g.prev = Prev {
            pid,
            threads: sampled.threads,
            t: now,
            c: cur,
            proc: sampled.proc,
            me: sampled.me,
            host: sampled.host,
            fsync_buckets,
            ack_buckets,
            fsync_total,
            fsync_records,
            log_bytes: sampled.log_bytes,
        };
    }

    /// Close the current phase (if any) and return its aggregates.
    pub async fn end_phase(&self) -> Option<PhaseStats> {
        self.tick().await;
        let mut g = self.inner.lock().await;
        let acc = g.acc.take()?;
        let now = Instant::now();
        let end_s = now.duration_since(g.start).as_secs_f64();
        let dur = now.duration_since(acc.t0).as_secs_f64().max(1e-9);
        let cur = counts(&self.shared, &self.verifier);
        let p1 = g.last_proc;
        let (du, ds) = match (acc.p0, p1) {
            (Some(a), Some(b)) if b.cpu_user_s >= a.cpu_user_s => {
                (b.cpu_user_s - a.cpu_user_s, b.cpu_sys_s - a.cpu_sys_s)
            }
            _ => (0.0, 0.0),
        };
        let dw = match p1 {
            Some(b) => b.write_bytes.saturating_sub(acc.disk_w0),
            None => 0,
        };
        g.phases.push(Phase {
            name: acc.name.clone(),
            kind: acc.kind,
            start_s: acc.start_s,
            end_s,
        });
        let backlog_end = self.verifier.pending() as f64;
        Some(PhaseStats {
            start_s: acc.start_s,
            end_s,
            dur_s: dur,
            offered: cur.offered - acc.c0.offered,
            requests: cur.requests - acc.c0.requests,
            acked: cur.acked - acc.c0.acked,
            rejected: cur.rejected - acc.c0.rejected,
            failed: cur.failed - acc.c0.failed,
            dropped: cur.dropped - acc.c0.dropped,
            delivered: cur.delivered - acc.c0.delivered,
            payload_bytes: cur.payload - acc.c0.payload,
            ack: acc.ack,
            delivery: acc.delivery,
            delivery_retried: acc.delivery_retried,
            cron_jitter: acc.cron_jitter,
            cpu_cores: (du + ds) / dur,
            cpu_sys_cores: ds / dur,
            cpu_seconds: du + ds,
            rss_mb_end: p1.map(|p| mb(p.rss_kb)).unwrap_or(f64::NAN),
            data_mb_end: g.last_data_mb,
            disk_write_mb: dw as f64 / 1_048_576.0,
            log_bytes: g.last_log_bytes.saturating_sub(acc.log0),
            backlog_start: acc.backlog0,
            backlog_end,
        })
    }

    /// End the current phase and begin a new one; returns the ended phase's stats.
    pub async fn phase(&self, name: &str, kind: PhaseKind) -> Option<PhaseStats> {
        let ended = self.end_phase().await;
        let mut g = self.inner.lock().await;
        let now = Instant::now();
        let start_s = now.duration_since(g.start).as_secs_f64();
        let c0 = counts(&self.shared, &self.verifier);
        let p0 = g.last_proc;
        let log0 = g.last_log_bytes;
        g.acc = Some(Acc {
            name: name.to_string(),
            kind,
            start_s,
            c0,
            p0,
            t0: now,
            log0,
            backlog0: self.verifier.pending() as f64,
            ack: new_hist(),
            delivery: new_hist(),
            delivery_retried: new_hist(),
            cron_jitter: new_hist(),
            disk_w0: p0.map(|p| p.write_bytes).unwrap_or(0),
        });
        ended
    }

    /// Copy of the series recorded so far (after taking a fresh sample).
    pub async fn series(&self) -> Series {
        self.tick().await;
        self.inner.lock().await.series.clone()
    }

    pub async fn finish(&self) -> (Series, Vec<Phase>, f64) {
        let _ = self.end_phase().await;
        self.halt();
        let g = self.inner.lock().await;
        (g.series.clone(), g.phases.clone(), g.rss_peak_mb)
    }
}
