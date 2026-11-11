//! Noisy neighbour: one tenant's destination hangs and its backlog grows. Does
//! a healthy tenant on another queue notice?

use super::common::*;
use crate::loadgen::SenderCfg;
use crate::model::*;
use crate::recorder::col;
use crate::sink::{DestCfg, Mode};
use crate::stats::q_ms;
use crate::trial::{RunCtx, Trial};
use crate::workload::{PadPool, RatePlan};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

fn shape(ctx: &RunCtx) -> (f64, f64, f64, f64, f64) {
    let p = ctx.profile;
    (
        ctx.rate(100.0),
        ctx.rate(250.0),
        ctx.secs(p.pick(8.0, 20.0, 45.0)).as_secs_f64(),
        ctx.secs(p.pick(12.0, 30.0, 90.0)).as_secs_f64(),
        ctx.secs(p.pick(10.0, 20.0, 45.0)).as_secs_f64(),
    )
}

pub fn params(ctx: &RunCtx) -> BTreeMap<String, String> {
    let (fast, noisy, calm, storm, after) = shape(ctx);
    let mut p = BTreeMap::new();
    p.insert(
        "healthy_tenant".into(),
        format!("{fast:.0} msg/s, 10 ms handler, 32 parallel"),
    );
    p.insert(
        "noisy_tenant".into(),
        format!("{noisy:.0} msg/s, 16 parallel; destination hangs during the storm phase"),
    );
    p.insert(
        "schedule".into(),
        format!("{calm:.0}s calm, {storm:.0}s storm, {after:.0}s after"),
    );
    p
}

pub async fn run(ctx: Arc<RunCtx>, index: u32, seed: u64) -> anyhow::Result<TrialResult> {
    let (fast_rate, noisy_rate, calm_s, storm_s, after_s) = shape(&ctx);
    let env = vec![("BETTERMQ_HTTP_TIMEOUT_SECS".to_string(), "600".to_string())];
    let mut trial = Trial::start_with_env(ctx.clone(), "noisy-neighbor", index, seed, env).await?;
    let fast_dest = trial
        .queue(
            "tenant.healthy",
            DestCfg::instant("healthy").latency(10.0, 0.4),
            32,
            5,
            Some((500, 8_000, 2.0)),
        )
        .await?;
    let noisy_dest = trial
        .queue(
            "tenant.noisy",
            DestCfg::instant("noisy").latency(20.0, 0.4),
            16,
            50,
            Some((500, 8_000, 2.0)),
        )
        .await?;
    let _ = fast_dest;
    let total = Duration::from_secs_f64(calm_s + storm_s + after_s);
    let fast = trial
        .verifier
        .register("healthy", RatePlan::expected(fast_rate, total), 0, false);
    let noisy = trial
        .verifier
        .register("noisy", RatePlan::expected(noisy_rate, total), 0, false);
    let pad = PadPool::new(seed);
    let cfg = SenderCfg {
        max_in_flight: 512,
        ..SenderCfg::default()
    };
    trial.phase("baseline", PhaseKind::Idle).await;
    trial.idle(ctx.secs(ctx.profile.pick(4.0, 8.0, 15.0))).await;

    let fast2 = fast.clone();
    let (windows, storm_start, storm_end) = {
        let t = &trial;
        let sink = &t.sink;
        let controller = async move {
            let mut out: Vec<(String, f64, f64, f64, f64)> = Vec::new();
            let mark = |name: &str, f: &Arc<crate::verify::Stream>| {
                let a = f.ack.take();
                let d = f.delivery.take();
                (
                    name.to_string(),
                    q_ms(&a, 0.99).unwrap_or(f64::NAN),
                    q_ms(&d, 0.5).unwrap_or(f64::NAN),
                    q_ms(&d, 0.99).unwrap_or(f64::NAN),
                    d.len() as f64,
                )
            };
            t.phase("calm", PhaseKind::Measure).await;
            tokio::time::sleep(Duration::from_secs_f64(calm_s)).await;
            out.push(mark("calm", &fast2));
            let calm_ps = t.phase("storm", PhaseKind::Fault).await;
            sink.set_mode(noisy_dest, Mode::Hang);
            tokio::time::sleep(Duration::from_secs_f64(storm_s)).await;
            out.push(mark("storm", &fast2));
            let storm_ps = t.phase("after", PhaseKind::Measure).await;
            sink.set_mode(noisy_dest, Mode::Normal);
            tokio::time::sleep(Duration::from_secs_f64(after_s)).await;
            out.push(mark("after", &fast2));
            (out, calm_ps.map(|p| p.end_s), storm_ps.map(|p| p.end_s))
        };
        let (p1, p2) = (pad.clone(), pad.clone());
        let d1 = t.drive(cfg.clone(), move |t0| {
            Box::new(RatePlan::new(
                fast,
                "tenant.healthy",
                fast_rate,
                total,
                1024,
                0,
                true,
                t0,
                p1,
                seed,
            ))
        });
        let d2 = t.drive(cfg.clone(), move |t0| {
            Box::new(RatePlan::new(
                noisy,
                "tenant.noisy",
                noisy_rate,
                total,
                1024,
                0,
                true,
                t0,
                p2,
                seed ^ 0x5eed,
            ))
        });
        let ((w, a, b), _, _) = tokio::join!(controller, d1, d2);
        (w, a.unwrap_or(0.0), b.unwrap_or(0.0))
    };
    let after = trial.end_phase().await;
    let series = trial.recorder.series().await;
    let by = |n: &str| {
        windows
            .iter()
            .find(|w| w.0 == n)
            .cloned()
            .expect("window recorded")
    };
    let (calm, storm, after_w) = (by("calm"), by("storm"), by("after"));

    trial.push(lower(
        "iso.healthy_delivery_p99_calm_ms",
        "Healthy tenant delivery p99, calm",
        "ms",
        calm.3,
        "latency",
    ));
    trial.push(
        lower(
            "iso.healthy_delivery_p99_storm_ms",
            "Healthy tenant delivery p99, neighbour hung",
            "ms",
            storm.3,
            "latency",
        )
        .headline(),
    );
    trial.push(lower(
        "iso.healthy_ack_p99_storm_ms",
        "Healthy tenant ACK p99, neighbour hung",
        "ms",
        storm.1,
        "latency",
    ));
    trial.push(
        lower(
            "iso.degradation_x",
            "Healthy tenant delivery p99: storm / calm",
            "x",
            if calm.3 > 0.0 {
                storm.3 / calm.3
            } else {
                f64::NAN
            },
            "latency",
        )
        .headline(),
    );
    trial.push(lower(
        "iso.healthy_delivery_p99_after_ms",
        "Healthy tenant delivery p99, after",
        "ms",
        after_w.3,
        "latency",
    ));
    let (_, backlog_peak) = col_stats(&series, col::BACKLOG, storm_start, storm_end + after_s);
    trial.push(info(
        "iso.noisy_backlog_peak",
        "Noisy tenant's peak backlog",
        "msgs",
        backlog_peak,
        "latency",
    ));
    if let Some(a) = after {
        trial.tables.push(thread_cpu_table(
            &series,
            "Where broker CPU goes (after)",
            a.start_s,
            a.end_s,
        ));
    }

    // The noisy tenant's backlog drains once its destination returns.
    trial.phase("drain", PhaseKind::Drain).await;
    let d = trial.wait_drained_patient(Duration::from_secs(300)).await;
    trial.end_phase().await;
    trial.push(lower(
        "iso.noisy_drain_s",
        "Noisy tenant backlog drained after recovery",
        "s",
        d.map(|d| d.as_secs_f64()).unwrap_or(300.0),
        "latency",
    ));
    trial.push(higher(
        "iso.noisy_drain_completed",
        "Noisy tenant backlog fully drained (1 = yes)",
        "",
        if d.is_some() { 1.0 } else { 0.0 },
        "reliability",
    ));
    trial.phase("settle", PhaseKind::Settle).await;
    trial
        .idle(ctx.secs(ctx.profile.pick(8.0, 25.0, 60.0)))
        .await;
    trial.finish().await
}
