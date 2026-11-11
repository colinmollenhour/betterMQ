//! Backlog depth: what it costs, in memory, disk, CPU, and ingest latency, to
//! hold a deep queue, and how fast it drains once the destination returns.

use super::common::*;
use crate::analysis;
use crate::loadgen::{SenderCfg, Wire};
use crate::model::*;
use crate::recorder::col;
use crate::sink::{DestCfg, Mode};
use crate::trial::{RunCtx, Trial};
use crate::workload::{PadPool, RatePlan};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    /// Destination hangs: the queue holds ready messages.
    Ready,
    /// Messages are scheduled an hour out: the queue holds delayed messages.
    Scheduled,
}

fn depths(ctx: &RunCtx) -> Vec<usize> {
    let scale = ctx.rate_scale.max(0.1);
    let v: Vec<usize> = ctx.profile.pick(
        vec![2_000, 6_000],
        vec![10_000, 40_000, 120_000],
        vec![10_000, 50_000, 200_000, 500_000, 1_000_000],
    );
    v.into_iter()
        .map(|d| ((d as f64) * scale) as usize)
        .collect()
}

pub fn params(id: &str, ctx: &RunCtx) -> BTreeMap<String, String> {
    let mut p = BTreeMap::new();
    p.insert("depths".into(), format!("{:?} messages", depths(ctx)));
    p.insert(
        "fill".into(),
        format!("batches of 100 at {:.0} msg/s", ctx.rate(2_000.0)),
    );
    p.insert(
        "backlog_kind".into(),
        if id == "backlog-scheduled" {
            "delayed one hour (scheduler holds them)".into()
        } else {
            "ready messages behind a hung destination (8 deliveries stuck)".into()
        },
    );
    p.insert("payload_bytes".into(), "1024".into());
    p
}

pub async fn run(id: &str, ctx: Arc<RunCtx>, index: u32, seed: u64) -> anyhow::Result<TrialResult> {
    let kind = if id == "backlog-scheduled" {
        Kind::Scheduled
    } else {
        Kind::Ready
    };
    let env = vec![("BETTERMQ_HTTP_TIMEOUT_SECS".to_string(), "3600".to_string())];
    let mut trial = Trial::start_with_env(ctx.clone(), id, index, seed, env).await?;
    let dest = trial
        .queue(
            "backlog.queue",
            DestCfg::instant("backlog").latency(5.0, 0.3),
            8,
            100,
            Some((1_000, 30_000, 2.0)),
        )
        .await?;
    if kind == Kind::Ready {
        trial.sink.set_mode(dest, Mode::Hang);
    } else {
        trial.allow_undelivered = true;
    }
    let pad = PadPool::new(seed);
    let cfg = SenderCfg {
        wire: Wire::Batch(100),
        max_in_flight: 64,
        ..SenderCfg::default()
    };
    let rate = ctx.rate(2_000.0);
    trial.phase("baseline", PhaseKind::Idle).await;
    trial.idle(ctx.secs(ctx.profile.pick(4.0, 8.0, 15.0))).await;

    let mut rows = Vec::new();
    let mut depth_pts: Vec<f64> = Vec::new();
    let mut rss_pts: Vec<f64> = Vec::new();
    let mut data_pts: Vec<f64> = Vec::new();
    let mut prev = 0usize;
    let mut last_ack_p99 = f64::NAN;
    let mut last_idle_cpu = f64::NAN;
    let mut last_fill_cpu = f64::NAN;
    for (i, target) in depths(&ctx).into_iter().enumerate() {
        let n = target.saturating_sub(prev);
        if n == 0 {
            continue;
        }
        let dur = Duration::from_secs_f64(n as f64 / rate);
        let stream = trial.verifier.register(
            &format!("fill-{i}"),
            RatePlan::expected(rate, dur) + 100,
            0,
            false,
        );
        trial.phase(&format!("fill-{i}"), PhaseKind::Measure).await;
        let padc = pad.clone();
        trial
            .drive(cfg.clone(), move |t0| {
                let mut plan = RatePlan::new(
                    stream,
                    "backlog.queue",
                    rate,
                    dur,
                    1024,
                    0,
                    false,
                    t0,
                    padc,
                    seed + i as u64,
                );
                if kind == Kind::Scheduled {
                    plan.delay_ms = 3_600_000;
                }
                Box::new(plan)
            })
            .await;
        let fill = trial.end_phase().await.expect("fill phase");
        let depth_now = trial.verifier.totals().0 as usize;
        prev = target;
        trial.phase(&format!("hold-{i}"), PhaseKind::Other).await;
        trial.idle(Duration::from_secs(6)).await;
        let hold = trial.end_phase().await.expect("hold phase");
        let series = trial.recorder.series().await;
        let idle_cpu = analysis::mean(&series.window(col::CPU, hold.start_s + 1.0, hold.end_s));
        let rss = hold.rss_mb_end;
        let data = hold.data_mb_end;
        depth_pts.push(depth_now as f64);
        rss_pts.push(rss);
        data_pts.push(data);
        last_ack_p99 = fill.ack_ms(0.99);
        last_idle_cpu = idle_cpu;
        last_fill_cpu = fill.cpu_cores;
        rows.push(vec![
            depth_now.to_string(),
            format!("{:.0}/s", fill.acked_rate()),
            analysis::fmt_ms(fill.ack_ms(0.5)),
            analysis::fmt_ms(fill.ack_ms(0.99)),
            format!("{:.2}", fill.cpu_cores),
            format!("{:.2}", idle_cpu),
            format!("{rss:.0}"),
            format!("{data:.0}"),
            format!(
                "{:.0}",
                trial.shared.c.client_inflight.load(Ordering::Relaxed)
            ),
        ]);
    }
    trial.tables.push(Table {
        title: "Cost of holding a backlog".into(),
        columns: [
            "Depth",
            "Fill rate",
            "ACK p50",
            "ACK p99",
            "CPU filling (cores)",
            "CPU holding (cores)",
            "RSS (MB)",
            "Data dir (MB)",
            "In flight",
        ]
        .map(String::from)
        .to_vec(),
        rows,
    });
    trial.push(
        info(
            "backlog.max_depth",
            "Deepest backlog held",
            "msgs",
            depth_pts.last().copied().unwrap_or(0.0),
            "throughput",
        )
        .headline(),
    );
    if let Some(s) = analysis::slope(&depth_pts, &rss_pts) {
        trial.push(
            lower(
                "backlog.rss_bytes_per_msg",
                "Memory per queued message (slope of RSS vs depth)",
                "B",
                s * 1_048_576.0,
                "memory",
            )
            .headline(),
        );
    }
    if let Some(s) = analysis::slope(&depth_pts, &data_pts) {
        trial.push(lower(
            "backlog.disk_bytes_per_msg",
            "Disk per queued message (slope of data dir vs depth)",
            "B",
            s * 1_048_576.0,
            "memory",
        ));
    }
    trial.push(
        lower(
            "backlog.ack_p99_at_max_ms",
            "ACK p99 while filling to the deepest backlog",
            "ms",
            last_ack_p99,
            "latency",
        )
        .headline(),
    );
    trial.push(lower(
        "backlog.cpu_filling_cores",
        "Broker CPU while filling the deepest tranche",
        "cores",
        last_fill_cpu,
        "cpu",
    ));
    trial.push(
        lower(
            "backlog.cpu_holding_cores",
            "Broker CPU just holding the deepest backlog",
            "cores",
            last_idle_cpu,
            "cpu",
        )
        .headline(),
    );

    if kind == Kind::Ready {
        let held = trial.verifier.pending();
        trial.phase("drain", PhaseKind::Drain).await;
        trial.sink.set_mode(dest, Mode::Normal);
        let took = trial.wait_drained_patient(Duration::from_secs(900)).await;
        let ps = trial.end_phase().await.expect("drain phase");
        let secs = took.map(|d| d.as_secs_f64()).unwrap_or(ps.dur_s);
        trial.push(
            higher(
                "backlog.drain_msgs_s",
                "Drain rate once the destination returned",
                "msg/s",
                held as f64 / secs.max(1e-9),
                "throughput",
            )
            .headline(),
        );
        trial.push(lower(
            "backlog.drain_cpu_cores",
            "Broker CPU while draining",
            "cores",
            ps.cpu_cores,
            "cpu",
        ));
        if took.is_none() {
            trial.note("The backlog had not drained within 15 minutes.");
        }
    } else {
        trial.note("Scheduled messages remain pending by design; no drain phase.");
    }
    trial.phase("settle", PhaseKind::Settle).await;
    trial
        .idle(ctx.secs(ctx.profile.pick(8.0, 25.0, 60.0)))
        .await;
    trial.finish().await
}
