//! Crash and restart with a deep backlog: how long to come back, and is every
//! acknowledged message still delivered?

use super::common::*;
use crate::loadgen::{SenderCfg, Wire};
use crate::model::*;
use crate::sink::{DestCfg, Mode};
use crate::trial::{RunCtx, Trial};
use crate::workload::{PadPool, RatePlan};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

fn messages(ctx: &RunCtx) -> usize {
    (ctx.profile.pick(3_000.0, 20_000.0, 100_000.0) * ctx.rate_scale.max(0.1)) as usize
}

pub fn params(ctx: &RunCtx) -> BTreeMap<String, String> {
    let n = messages(ctx);
    let mut p = BTreeMap::new();
    p.insert(
        "backlog_at_crash".into(),
        format!("{n} acknowledged, undelivered messages (destination hung)"),
    );
    p.insert(
        "second_restart".into(),
        format!("{} more, then a graceful SIGTERM restart", n / 2),
    );
    p.insert(
        "check".into(),
        "every acknowledged message must still be delivered".into(),
    );
    p
}

pub async fn run(ctx: Arc<RunCtx>, index: u32, seed: u64) -> anyhow::Result<TrialResult> {
    let n = messages(&ctx);
    let env = vec![("BETTERMQ_HTTP_TIMEOUT_SECS".to_string(), "3600".to_string())];
    let mut trial = Trial::start_with_env(ctx.clone(), "crash-recovery", index, seed, env).await?;
    let dest = trial
        .queue(
            "recovery.queue",
            DestCfg::instant("recovery").latency(5.0, 0.3),
            8,
            100,
            Some((1_000, 30_000, 2.0)),
        )
        .await?;
    trial.sink.set_mode(dest, Mode::Hang);
    let pad = PadPool::new(seed);
    let cfg = SenderCfg {
        wire: Wire::Batch(100),
        max_in_flight: 64,
        ..SenderCfg::default()
    };
    let rate = ctx.rate(2_000.0);

    trial.phase("baseline", PhaseKind::Idle).await;
    trial.idle(ctx.secs(ctx.profile.pick(3.0, 6.0, 10.0))).await;

    // Round 1: fill, then SIGKILL.
    let dur = Duration::from_secs_f64(n as f64 / rate);
    let s1 = trial
        .verifier
        .register("round-1", RatePlan::expected(rate, dur) + 100, 0, false);
    trial.phase("fill-1", PhaseKind::Measure).await;
    let p1 = pad.clone();
    trial
        .drive(cfg.clone(), move |t0| {
            Box::new(RatePlan::new(
                s1,
                "recovery.queue",
                rate,
                dur,
                1024,
                0,
                false,
                t0,
                p1,
                seed,
            ))
        })
        .await;
    trial.end_phase().await;
    let held = trial.verifier.pending();
    trial.idle(Duration::from_secs(2)).await;
    trial.phase("crash", PhaseKind::Fault).await;
    let crash_start = trial.restart_broker(true).await?;
    trial.sink.set_mode(dest, Mode::Normal);
    let drain1 = std::time::Instant::now();
    let d1 = trial.wait_drained_patient(Duration::from_secs(600)).await;
    let drain1_s = drain1.elapsed().as_secs_f64();
    trial.end_phase().await;

    // Round 2: fill again, then a graceful restart.
    trial.sink.set_mode(dest, Mode::Hang);
    let n2 = n / 2;
    let dur2 = Duration::from_secs_f64(n2 as f64 / rate);
    let s2 = trial
        .verifier
        .register("round-2", RatePlan::expected(rate, dur2) + 100, 0, false);
    trial.phase("fill-2", PhaseKind::Measure).await;
    let p2 = pad.clone();
    trial
        .drive(cfg, move |t0| {
            Box::new(RatePlan::new(
                s2,
                "recovery.queue",
                rate,
                dur2,
                1024,
                0,
                false,
                t0,
                p2,
                seed ^ 9,
            ))
        })
        .await;
    trial.end_phase().await;
    trial.idle(Duration::from_secs(2)).await;
    trial.phase("graceful", PhaseKind::Fault).await;
    let graceful_start = trial.restart_broker(false).await?;
    trial.sink.set_mode(dest, Mode::Normal);
    let d2 = trial.wait_drained_patient(Duration::from_secs(600)).await;
    trial.end_phase().await;

    trial.push(
        lower(
            "recovery.crash_startup_s",
            "Start-up after SIGKILL with a deep backlog",
            "s",
            crash_start.as_secs_f64(),
            "lifecycle",
        )
        .headline(),
    );
    trial.push(lower(
        "recovery.graceful_startup_s",
        "Start-up after graceful stop with a backlog",
        "s",
        graceful_start.as_secs_f64(),
        "lifecycle",
    ));
    trial.push(
        lower(
            "recovery.drain_after_crash_s",
            "Backlog drained after crash recovery",
            "s",
            drain1_s,
            "latency",
        )
        .headline(),
    );
    trial.push(info(
        "recovery.backlog_at_crash",
        "Backlog held when killed",
        "msgs",
        held as f64,
        "reliability",
    ));
    let (acked, done, perm) = trial.verifier.totals();
    let lost = acked.saturating_sub(done + perm);
    trial.push(
        lower(
            "recovery.lost_messages",
            "Acknowledged messages lost",
            "msgs",
            lost as f64,
            "reliability",
        )
        .headline(),
    );
    let dups: u64 = trial
        .verifier
        .all()
        .iter()
        .map(|s| s.dups.load(Ordering::Relaxed))
        .sum();
    trial.push(lower(
        "recovery.duplicate_deliveries",
        "Duplicate deliveries after restarts",
        "msgs",
        dups as f64,
        "reliability",
    ));
    if d1.is_none() || d2.is_none() {
        trial.note("A backlog had not drained within 10 minutes of restart.");
    }
    trial.phase("settle", PhaseKind::Settle).await;
    trial
        .idle(ctx.secs(ctx.profile.pick(6.0, 15.0, 30.0)))
        .await;
    trial.finish().await
}
