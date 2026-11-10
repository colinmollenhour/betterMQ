//! Repeated burst -> drain -> idle cycles. A healthy broker returns to the same
//! footprint every cycle; steady growth in settled memory, file descriptors,
//! threads, or disk across cycles is a leak or unbounded cache.

use super::common::*;
use crate::analysis;
use crate::loadgen::{SenderCfg, Wire};
use crate::model::*;
use crate::recorder::col;
use crate::sink::DestCfg;
use crate::trial::{RunCtx, Trial};
use crate::workload::{PadPool, RatePlan};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

fn shape(ctx: &RunCtx) -> (usize, usize, f64) {
    let p = ctx.profile;
    (
        p.pick(3, 5, 8),
        (p.pick(4_000.0, 20_000.0, 60_000.0) * ctx.rate_scale.max(0.1)) as usize,
        ctx.secs(p.pick(6.0, 15.0, 30.0)).as_secs_f64(),
    )
}

pub fn params(ctx: &RunCtx) -> BTreeMap<String, String> {
    let (cycles, burst, settle) = shape(ctx);
    let mut p = BTreeMap::new();
    p.insert("cycles".into(), cycles.to_string());
    p.insert(
        "burst".into(),
        format!("{burst} messages, 1 KB, batches of 100"),
    );
    p.insert("idle_between_cycles".into(), format!("{settle:.0} s"));
    p
}

pub async fn run(ctx: Arc<RunCtx>, index: u32, seed: u64) -> anyhow::Result<TrialResult> {
    let (cycles, burst, settle_s) = shape(&ctx);
    let mut trial = Trial::start(ctx.clone(), "memory-cycles", index, seed).await?;
    trial
        .queue(
            "cycle.queue",
            DestCfg::instant("cycle").latency(5.0, 0.3),
            128,
            5,
            None,
        )
        .await?;
    let pad = PadPool::new(seed);
    let cfg = SenderCfg {
        wire: Wire::Batch(100),
        max_in_flight: 64,
        ..SenderCfg::default()
    };
    let rate = ctx.rate(2_000.0);
    trial.phase("baseline", PhaseKind::Idle).await;
    trial
        .idle(ctx.secs(ctx.profile.pick(5.0, 10.0, 20.0)))
        .await;

    struct Row {
        peak: f64,
        settled: f64,
        anon: f64,
        fds: f64,
        threads: f64,
        data: f64,
    }
    let mut rows: Vec<Row> = Vec::new();
    for c in 0..cycles {
        let dur = Duration::from_secs_f64(burst as f64 / rate);
        let stream = trial.verifier.register(
            &format!("cycle-{c}"),
            RatePlan::expected(rate, dur) + 100,
            0,
            false,
        );
        let burst_ps = {
            trial.phase(&format!("burst-{c}"), PhaseKind::Measure).await;
            let padc = pad.clone();
            trial
                .drive(cfg.clone(), move |t0| {
                    Box::new(RatePlan::new(
                        stream,
                        "cycle.queue",
                        rate,
                        dur,
                        1024,
                        0,
                        false,
                        t0,
                        padc,
                        seed + c as u64,
                    ))
                })
                .await;
            trial.end_phase().await.expect("burst phase")
        };
        trial.phase(&format!("drain-{c}"), PhaseKind::Drain).await;
        trial.wait_drained(Duration::from_secs(300)).await;
        trial.phase(&format!("settle-{c}"), PhaseKind::Settle).await;
        trial.idle(Duration::from_secs_f64(settle_s)).await;
        let ps = trial.end_phase().await.expect("settle phase");
        let series = trial.recorder.series().await;
        let tail_from = (ps.end_s - 5.0).max(ps.start_s);
        let med = |name: &str| analysis::percentile(&series.window(name, tail_from, ps.end_s), 0.5);
        rows.push(Row {
            peak: series
                .window(col::RSS, burst_ps.start_s, ps.start_s)
                .into_iter()
                .fold(0.0, f64::max),
            settled: med(col::RSS),
            anon: med(col::ANON),
            fds: med(col::FDS),
            threads: med(col::THREADS),
            data: med(col::DATA),
        });
    }

    trial.tables.push(Table {
        title: "Footprint after each idle period".into(),
        columns: [
            "Cycle",
            "Peak RSS (MB)",
            "Settled RSS (MB)",
            "Anonymous (MB)",
            "Open fds",
            "Threads",
            "Data dir (MB)",
        ]
        .map(String::from)
        .to_vec(),
        rows: rows
            .iter()
            .enumerate()
            .map(|(i, r)| {
                vec![
                    (i + 1).to_string(),
                    format!("{:.0}", r.peak),
                    format!("{:.0}", r.settled),
                    format!("{:.0}", r.anon),
                    format!("{:.0}", r.fds),
                    format!("{:.0}", r.threads),
                    format!("{:.0}", r.data),
                ]
            })
            .collect(),
    });
    let xs: Vec<f64> = (1..=rows.len()).map(|i| i as f64).collect();
    // The first cycle legitimately warms caches and the allocator; judge growth from cycle 2 on.
    let tail = |f: &dyn Fn(&Row) -> f64| -> (Vec<f64>, Vec<f64>) {
        (xs[1..].to_vec(), rows[1..].iter().map(f).collect())
    };
    if rows.len() >= 3 {
        let (x, y) = tail(&|r| r.settled);
        if let Some(s) = analysis::slope(&x, &y) {
            trial.push(
                lower(
                    "cycles.rss_slope_mb_per_cycle",
                    "Settled RSS growth per cycle (from cycle 2)",
                    "MB",
                    s,
                    "memory",
                )
                .headline(),
            );
        }
        let (x, y) = tail(&|r| r.fds);
        if let Some(s) = analysis::slope(&x, &y) {
            trial.push(lower(
                "cycles.fd_slope_per_cycle",
                "Open file descriptors growth per cycle",
                "fds",
                s,
                "resources",
            ));
        }
        let (x, y) = tail(&|r| r.threads);
        if let Some(s) = analysis::slope(&x, &y) {
            trial.push(lower(
                "cycles.thread_slope_per_cycle",
                "Thread count growth per cycle",
                "threads",
                s,
                "resources",
            ));
        }
        let (x, y) = tail(&|r| r.data);
        if let Some(s) = analysis::slope(&x, &y) {
            trial.push(lower(
                "cycles.disk_slope_mb_per_cycle",
                "Data directory growth per cycle",
                "MB",
                s,
                "memory",
            ));
        }
    }
    if let (Some(first), Some(last)) = (rows.first(), rows.last()) {
        trial.push(lower(
            "cycles.settled_first_mb",
            "Settled RSS after cycle 1",
            "MB",
            first.settled,
            "memory",
        ));
        trial.push(
            lower(
                "cycles.settled_last_mb",
                "Settled RSS after the last cycle",
                "MB",
                last.settled,
                "memory",
            )
            .headline(),
        );
        trial.push(lower(
            "cycles.peak_last_mb",
            "Peak RSS in the last cycle",
            "MB",
            last.peak,
            "memory",
        ));
    }
    trial.finish().await
}
