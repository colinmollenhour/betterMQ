//! A sudden burst on top of nominal load: how much backlog builds, how fast it
//! drains, and how long latency takes to recover.

use super::common::*;
use crate::loadgen::SenderCfg;
use crate::model::*;
use crate::recorder::{col, PhaseStats};
use crate::sink::DestCfg;
use crate::trial::{RunCtx, Trial};
use crate::workload::{PadPool, RatePlan};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

const BURST_X: f64 = 5.0;

fn shape(ctx: &RunCtx) -> (f64, f64, f64, f64) {
    let nominal = ctx.rate(100.0);
    let p = ctx.profile;
    (
        nominal,
        ctx.secs(p.pick(6.0, 15.0, 30.0)).as_secs_f64(),
        ctx.secs(p.pick(8.0, 20.0, 45.0)).as_secs_f64(),
        ctx.secs(p.pick(12.0, 30.0, 60.0)).as_secs_f64(),
    )
}

pub fn params(ctx: &RunCtx) -> BTreeMap<String, String> {
    let (nominal, a, b, c) = shape(ctx);
    let mut p = BTreeMap::new();
    p.insert("nominal".into(), format!("{nominal:.0} msg/s"));
    p.insert(
        "burst".into(),
        format!("{:.0} msg/s (x{BURST_X})", nominal * BURST_X),
    );
    p.insert(
        "schedule".into(),
        format!("{a:.0}s nominal, {b:.0}s burst, {c:.0}s recovery"),
    );
    p.insert(
        "destination".into(),
        "10 ms median handler, 128 parallel".into(),
    );
    p.insert("arrivals".into(), "Poisson, open loop".into());
    p
}

pub async fn run(ctx: Arc<RunCtx>, index: u32, seed: u64) -> anyhow::Result<TrialResult> {
    let (nominal, nom_s, burst_s, rec_s) = shape(&ctx);
    let mut trial = Trial::start(ctx.clone(), "burst-drain", index, seed).await?;
    trial
        .queue(
            "burst.queue",
            DestCfg::instant("burst").latency(10.0, 0.4),
            128,
            5,
            Some((500, 8_000, 2.0)),
        )
        .await?;
    let pad = PadPool::new(seed);
    let cfg = SenderCfg {
        max_in_flight: 512,
        ..SenderCfg::default()
    };

    trial.phase("baseline", PhaseKind::Idle).await;
    trial.idle(ctx.secs(ctx.profile.pick(4.0, 8.0, 15.0))).await;

    let mut stats: Vec<PhaseStats> = Vec::new();
    for (name, rate, secs) in [
        ("nominal", nominal, nom_s),
        ("burst", nominal * BURST_X, burst_s),
        ("recovery", nominal, rec_s),
    ] {
        let dur = Duration::from_secs_f64(secs);
        let stream = trial
            .verifier
            .register(name, RatePlan::expected(rate, dur) * 2, 0, false);
        trial.phase(name, PhaseKind::Measure).await;
        let padc = pad.clone();
        let s2 = seed.wrapping_add(stats.len() as u64);
        trial
            .drive(cfg.clone(), move |t0| {
                Box::new(RatePlan::new(
                    stream,
                    "burst.queue",
                    rate,
                    dur,
                    1024,
                    0,
                    true,
                    t0,
                    padc,
                    s2,
                ))
            })
            .await;
        stats.push(trial.end_phase().await.expect("phase open"));
    }
    let series = trial.recorder.series().await;
    let (nom, burst, rec) = (&stats[0], &stats[1], &stats[2]);

    trial.push(lower(
        "burst.ack_p99_nominal_ms",
        "ACK p99, nominal",
        "ms",
        nom.ack_ms(0.99),
        "latency",
    ));
    trial.push(
        lower(
            "burst.ack_p99_burst_ms",
            "ACK p99, during burst",
            "ms",
            burst.ack_ms(0.99),
            "latency",
        )
        .headline(),
    );
    trial.push(lower(
        "burst.delivery_p99_nominal_ms",
        "Delivery p99, nominal",
        "ms",
        nom.delivery_ms(0.99),
        "latency",
    ));
    trial.push(lower(
        "burst.delivery_p99_burst_ms",
        "Delivery p99, during burst",
        "ms",
        burst.delivery_ms(0.99),
        "latency",
    ));
    trial.push(lower(
        "burst.error_rate_pct",
        "Rejected or failed ingest during burst",
        "%",
        burst.error_rate() * 100.0,
        "reliability",
    ));
    trial.push(lower(
        "burst.cpu_cores_burst",
        "Broker CPU during burst",
        "cores",
        burst.cpu_cores,
        "cpu",
    ));
    let (_, backlog_peak) = col_stats(&series, col::BACKLOG, burst.start_s, rec.end_s);
    trial.push(
        lower(
            "burst.backlog_peak",
            "Peak undelivered backlog",
            "msgs",
            backlog_peak,
            "latency",
        )
        .headline(),
    );

    // Time from the end of the burst until the backlog is back to roughly one
    // second of nominal traffic.
    let threshold = (nominal * 1.0).max(10.0);
    let mut recovered_at = None;
    if let Some(col_v) = series.col(col::BACKLOG) {
        for (t, v) in series.t.iter().zip(col_v.iter()) {
            if *t >= burst.end_s {
                if let Some(v) = v {
                    if *v <= threshold {
                        recovered_at = Some(*t - burst.end_s);
                        break;
                    }
                }
            }
        }
    }
    match recovered_at {
        Some(s) => trial.push(
            lower(
                "burst.recovery_s",
                "Backlog back to normal after burst",
                "s",
                s,
                "latency",
            )
            .headline(),
        ),
        None => {
            trial.note(format!(
                "Backlog was still above {threshold:.0} messages {rec_s:.0}s after the burst ended."
            ));
            trial.push(
                lower(
                    "burst.recovery_s",
                    "Backlog back to normal after burst",
                    "s",
                    rec_s,
                    "latency",
                )
                .headline(),
            );
        }
    }
    trial.push(higher(
        "burst.recovered",
        "Backlog returned to normal within the recovery phase (1 = yes)",
        "",
        if recovered_at.is_some() { 1.0 } else { 0.0 },
        "reliability",
    ));
    trial.push(lower(
        "burst.delivery_p99_recovery_ms",
        "Delivery p99 in recovery phase",
        "ms",
        rec.delivery_ms(0.99),
        "latency",
    ));
    trial.tables.push(thread_cpu_table(
        &series,
        "Where broker CPU goes (burst)",
        burst.start_s,
        burst.end_s,
    ));

    trial.phase("drain", PhaseKind::Drain).await;
    trial.wait_drained(Duration::from_secs(120)).await;
    trial.phase("settle", PhaseKind::Settle).await;
    trial
        .idle(ctx.secs(ctx.profile.pick(8.0, 25.0, 60.0)))
        .await;
    trial.finish().await
}
