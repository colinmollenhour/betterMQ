//! Scheduled work: a fleet of recurring jobs, from a realistic ~50-job
//! installation up to thousands of per-tenant schedules, many of them aligned
//! on the same second (the `* * * * *` thundering herd). Measures how late
//! fires are, whether any are missed, and what holding the schedule costs.

use super::common::*;
use crate::analysis;
use crate::model::*;
use crate::recorder::col;
use crate::sink::DestCfg;
use crate::stats::{max_ms, q_ms};
use crate::trial::{RunCtx, Trial};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// One compressed "minute" is 3 seconds, so a day of housekeeping fits in a
/// few minutes while keeping the relative cadence of the job mix.
const MINUTE_S: u64 = 3;
/// Period of the synthetic fleet jobs.
const FLEET_PERIOD_S: u64 = 10;

fn sizes(ctx: &RunCtx) -> Vec<usize> {
    ctx.profile.pick(
        vec![300],
        vec![500, 2_000, 5_000],
        vec![500, 2_000, 5_000, 15_000, 30_000],
    )
}

fn window_s(ctx: &RunCtx) -> f64 {
    ctx.secs(ctx.profile.pick(25.0, 40.0, 60.0)).as_secs_f64()
}

pub fn params(ctx: &RunCtx) -> BTreeMap<String, String> {
    let mut p = BTreeMap::new();
    p.insert(
        "realistic_fleet".into(),
        format!("50 jobs: 14 every minute, 1 every 2, 1 every 5, 2 every 10/15, 3 every 15-30, 1 every 30, 28 daily/weekly; 1 minute = {MINUTE_S} s"),
    );
    p.insert(
        "scale_steps".into(),
        format!("{:?} jobs firing every {FLEET_PERIOD_S} s, half aligned on the same second, half staggered", sizes(ctx)),
    );
    p.insert(
        "lateness_measure".into(),
        "arrival time modulo the job period; a fire a whole period late shows up in the fire count, not the lateness".into(),
    );
    p.insert(
        "observation_window".into(),
        format!("{:.0} s per step", window_s(ctx)),
    );
    p
}

struct FleetStep {
    label: String,
    jobs: usize,
    expected: f64,
    fired: f64,
    p50: f64,
    p99: f64,
    max: f64,
    cpu: f64,
    rss: f64,
}

pub async fn run(ctx: Arc<RunCtx>, index: u32, seed: u64) -> anyhow::Result<TrialResult> {
    let mut trial = Trial::start(ctx.clone(), "cron-fleet", index, seed).await?;
    let dest = trial.sink.add_dest(DestCfg::instant("cron"));
    let url = trial.sink.url(dest);
    let win = window_s(&ctx);
    trial.phase("baseline", PhaseKind::Idle).await;
    trial.idle(ctx.secs(ctx.profile.pick(4.0, 8.0, 15.0))).await;

    let mut steps: Vec<FleetStep> = Vec::new();
    // Fires per second of the realistic jobs, which keep running during the fleet steps.
    let real_rate: f64;
    let mut next_id: u32 = 0;

    // --- realistic installation ---
    {
        let mut jobs: Vec<(u64, &str)> = Vec::new();
        for _ in 0..14 {
            jobs.push((MINUTE_S, "aligned"));
        }
        jobs.push((2 * MINUTE_S, "aligned"));
        jobs.push((5 * MINUTE_S, "aligned"));
        jobs.push((10 * MINUTE_S, "aligned"));
        jobs.push((15 * MINUTE_S, "interval"));
        for p in [15u64, 20, 25] {
            jobs.push((p * MINUTE_S, "interval"));
        }
        jobs.push((30 * MINUTE_S, "interval"));
        // Active jobs above fire within the window; the rest only exist.
        for _ in 0..(50 - jobs.len()) {
            jobs.push((86_400, "interval"));
        }
        let mut active_rate = 0.0;
        for (period, kind) in &jobs {
            let id = next_id;
            next_id += 1;
            if *kind == "aligned" && 60 % period == 0 {
                trial
                    .api
                    .create_cron(json!({
                        "cron": format!("*/{period} * * * * *"),
                        "url": url, "secret": "s3cret", "body": {"cr": id},
                    }))
                    .await?;
                trial.sink.register_cron(id, 0, (*period * 1000) as i64);
            } else {
                let c = trial
                    .api
                    .create_cron(json!({
                        "every_seconds": period,
                        "url": url, "secret": "s3cret", "body": {"cr": id},
                    }))
                    .await?;
                trial
                    .sink
                    .register_cron(id, c.next_run_at_ms, (*period * 1000) as i64);
            }
            if *period <= 90 {
                active_rate += 1.0 / *period as f64;
            }
        }
        trial.idle(Duration::from_secs(6)).await;
        trial.phase("realistic-50", PhaseKind::Measure).await;
        trial.idle(Duration::from_secs_f64(win)).await;
        let ps = trial.end_phase().await.expect("phase open");
        let series = trial.recorder.series().await;
        steps.push(FleetStep {
            label: "realistic 50".into(),
            jobs: 50,
            expected: active_rate * win,
            fired: ps.cron_jitter.len() as f64,
            p50: q_ms(&ps.cron_jitter, 0.5).unwrap_or(f64::NAN),
            p99: q_ms(&ps.cron_jitter, 0.99).unwrap_or(f64::NAN),
            max: max_ms(&ps.cron_jitter).unwrap_or(f64::NAN),
            cpu: analysis::mean(&series.window(col::CPU, ps.start_s, ps.end_s)),
            rss: ps.rss_mb_end,
        });
        real_rate = active_rate;
    }

    // --- scale steps (cumulative) ---
    let mut total = 0usize;
    for target in sizes(&ctx) {
        let started = std::time::Instant::now();
        while total < target {
            let id = next_id;
            next_id += 1;
            if total & 1 == 0 {
                trial
                    .api
                    .create_cron(json!({
                        "cron": format!("*/{FLEET_PERIOD_S} * * * * *"),
                        "url": url, "secret": "s3cret", "body": {"cr": id},
                    }))
                    .await?;
                trial
                    .sink
                    .register_cron(id, 0, (FLEET_PERIOD_S * 1000) as i64);
            } else {
                let c = trial
                    .api
                    .create_cron(json!({
                        "every_seconds": FLEET_PERIOD_S,
                        "url": url, "secret": "s3cret", "body": {"cr": id},
                    }))
                    .await?;
                trial
                    .sink
                    .register_cron(id, c.next_run_at_ms, (FLEET_PERIOD_S * 1000) as i64);
            }
            total += 1;
        }
        let create_s = started.elapsed().as_secs_f64();
        trial.note(format!(
            "created schedules up to {target} in {create_s:.1}s"
        ));
        // Discard the first cycles: newly created interval jobs are still phase-settling.
        trial
            .phase(&format!("settle-{target}"), PhaseKind::Warmup)
            .await;
        trial.idle(Duration::from_secs(FLEET_PERIOD_S + 3)).await;
        trial
            .phase(&format!("fleet-{target}"), PhaseKind::Measure)
            .await;
        trial.idle(Duration::from_secs_f64(win)).await;
        let ps = trial.end_phase().await.expect("phase open");
        let series = trial.recorder.series().await;
        let fired_in_window = ps.cron_jitter.len() as f64;
        steps.push(FleetStep {
            label: format!("{target} jobs"),
            jobs: target + 50,
            expected: (real_rate + target as f64 / FLEET_PERIOD_S as f64) * win,
            fired: fired_in_window,
            p50: q_ms(&ps.cron_jitter, 0.5).unwrap_or(f64::NAN),
            p99: q_ms(&ps.cron_jitter, 0.99).unwrap_or(f64::NAN),
            max: max_ms(&ps.cron_jitter).unwrap_or(f64::NAN),
            cpu: analysis::mean(&series.window(col::CPU, ps.start_s, ps.end_s)),
            rss: ps.rss_mb_end,
        });
    }

    let mut rows = Vec::new();
    let mut max_ok = 0usize;
    for s in &steps {
        // Signed: negative means more fires than scheduled (duplicates).
        let missed = 100.0 * (1.0 - s.fired / s.expected.max(1.0));
        let ok = s.p99 <= 1000.0 && (-5.0..=1.0).contains(&missed);
        if ok {
            max_ok = max_ok.max(s.jobs);
        }
        rows.push(vec![
            s.label.clone(),
            s.jobs.to_string(),
            format!("{:.0}", s.expected),
            format!("{:.0}", s.fired),
            format!("{missed:+.2}%"),
            analysis::fmt_ms(s.p50),
            analysis::fmt_ms(s.p99),
            analysis::fmt_ms(s.max),
            format!("{:.2}", s.cpu),
            format!("{:.0}", s.rss),
            if ok { "ok".into() } else { "degraded".into() },
        ]);
    }
    trial.tables.push(Table {
        title: "Schedule fleet steps".into(),
        columns: [
            "Step",
            "Jobs configured",
            "Expected fires",
            "Fires seen",
            "Missed",
            "Lateness p50",
            "p99",
            "max",
            "CPU (cores)",
            "RSS (MB)",
            "Verdict",
        ]
        .map(String::from)
        .to_vec(),
        rows,
    });
    let real = &steps[0];
    trial.push(
        lower(
            "cron.realistic_lateness_p99_ms",
            "Fire lateness p99, realistic 50-job fleet",
            "ms",
            real.p99,
            "latency",
        )
        .headline(),
    );
    trial.push(lower(
        "cron.realistic_cpu_cores",
        "Broker CPU, realistic fleet",
        "cores",
        real.cpu,
        "cpu",
    ));
    trial.push(
        higher(
            "cron.max_healthy_jobs",
            "Largest fleet with p99 lateness <= 1 s and fires within -5%/+1% of schedule",
            "jobs",
            max_ok as f64,
            "throughput",
        )
        .headline(),
    );
    if let Some(last) = steps.last() {
        trial.push(lower(
            "cron.largest_lateness_p99_ms",
            &format!("Fire lateness p99 at {} jobs", last.jobs),
            "ms",
            last.p99,
            "latency",
        ));
        trial.push(lower(
            "cron.largest_cpu_cores",
            &format!("Broker CPU at {} jobs", last.jobs),
            "cores",
            last.cpu,
            "cpu",
        ));
        trial.push(lower(
            "cron.largest_rss_mb",
            &format!("RSS at {} jobs", last.jobs),
            "MB",
            last.rss,
            "memory",
        ));
    }
    trial.allow_undelivered = false;
    trial.phase("settle", PhaseKind::Settle).await;
    trial
        .idle(ctx.secs(ctx.profile.pick(8.0, 25.0, 60.0)))
        .await;
    trial.finish().await
}
