//! Destination outage while ingest continues: does the retry machinery stay
//! cheap, does ingest stay isolated from delivery trouble, and how quickly does
//! the broker catch up when the destination returns?

use super::common::*;
use crate::analysis;
use crate::loadgen::SenderCfg;
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
    Errors,
    Timeouts,
}

fn kind(id: &str) -> Kind {
    if id == "outage-timeout" {
        Kind::Timeouts
    } else {
        Kind::Errors
    }
}

fn shape(ctx: &RunCtx) -> (f64, f64, f64, f64) {
    let p = ctx.profile;
    (
        ctx.rate(150.0),
        ctx.secs(p.pick(6.0, 15.0, 30.0)).as_secs_f64(),
        ctx.secs(p.pick(10.0, 25.0, 60.0)).as_secs_f64(),
        ctx.secs(p.pick(15.0, 45.0, 120.0)).as_secs_f64(),
    )
}

pub fn params(id: &str, ctx: &RunCtx) -> BTreeMap<String, String> {
    let (rate, pre, out, post) = shape(ctx);
    let mut p = BTreeMap::new();
    p.insert("ingest".into(), format!("{rate:.0} msg/s throughout"));
    p.insert(
        "schedule".into(),
        format!("{pre:.0}s healthy, {out:.0}s outage, {post:.0}s recovery"),
    );
    p.insert(
        "failure_mode".into(),
        match kind(id) {
            Kind::Errors => "destination answers 503 immediately".into(),
            Kind::Timeouts => {
                "destination accepts connections and never answers (3 s delivery timeout)".into()
            }
        },
    );
    p.insert(
        "queue".into(),
        "64 parallel, 12 retries, backoff 0.5 s x2 capped 8 s".into(),
    );
    p
}

pub async fn run(id: &str, ctx: Arc<RunCtx>, index: u32, seed: u64) -> anyhow::Result<TrialResult> {
    let k = kind(id);
    let (rate, pre_s, out_s, post_s) = shape(&ctx);
    let env = vec![("BETTERMQ_HTTP_TIMEOUT_SECS".to_string(), "3".to_string())];
    let mut trial = Trial::start_with_env(ctx.clone(), id, index, seed, env).await?;
    let dest = trial
        .queue(
            "outage.queue",
            DestCfg::instant("outage").latency(20.0, 0.4),
            64,
            12,
            Some((500, 8_000, 2.0)),
        )
        .await?;
    let total = Duration::from_secs_f64(pre_s + out_s + post_s);
    let stream = trial
        .verifier
        .register("outage", RatePlan::expected(rate, total), 0, false);
    let pad = PadPool::new(seed);
    let cfg = SenderCfg {
        max_in_flight: 512,
        ..SenderCfg::default()
    };

    trial.phase("baseline", PhaseKind::Idle).await;
    trial.idle(ctx.secs(ctx.profile.pick(4.0, 8.0, 15.0))).await;

    let mode = if k == Kind::Timeouts {
        Mode::Hang
    } else {
        Mode::Fail503
    };
    let stats = {
        let t = &trial;
        let sink = &t.sink;
        let controller = async move {
            t.phase("healthy", PhaseKind::Measure).await;
            tokio::time::sleep(Duration::from_secs_f64(pre_s)).await;
            let healthy = t.phase("outage", PhaseKind::Fault).await;
            sink.set_mode(dest, mode);
            tokio::time::sleep(Duration::from_secs_f64(out_s)).await;
            let outage = t.phase("recovery", PhaseKind::Measure).await;
            sink.set_mode(dest, Mode::Normal);
            (healthy, outage)
        };
        let drive = t.drive(cfg, move |t0| {
            Box::new(RatePlan::new(
                stream,
                "outage.queue",
                rate,
                total,
                1024,
                0,
                true,
                t0,
                pad,
                seed,
            ))
        });
        let ((healthy, outage), _) = tokio::join!(controller, drive);
        (healthy, outage)
    };
    let recovery = trial.end_phase().await;
    let (healthy, outage) = (stats.0.expect("healthy"), stats.1.expect("outage"));
    let recovery = recovery.expect("recovery");
    let series = trial.recorder.series().await;

    trial.phase("drain", PhaseKind::Drain).await;
    let drained_in = trial.wait_drained_patient(Duration::from_secs(240)).await;
    trial.end_phase().await;

    trial.push(lower(
        "outage.ack_p99_healthy_ms",
        "ACK p99, healthy",
        "ms",
        healthy.ack_ms(0.99),
        "latency",
    ));
    trial.push(
        lower(
            "outage.ack_p99_outage_ms",
            "ACK p99, during outage",
            "ms",
            outage.ack_ms(0.99),
            "latency",
        )
        .headline(),
    );
    trial.push(lower(
        "outage.error_rate_pct",
        "Rejected or failed ingest during outage",
        "%",
        outage.error_rate() * 100.0,
        "reliability",
    ));
    trial.push(lower(
        "outage.retried_delivery_p99_ms",
        "Delivery p99 of messages that needed a retry",
        "ms",
        crate::stats::q_ms(&recovery.delivery_retried, 0.99).unwrap_or(f64::NAN),
        "latency",
    ));
    trial.push(lower(
        "outage.cpu_cores_healthy",
        "Broker CPU, healthy",
        "cores",
        healthy.cpu_cores,
        "cpu",
    ));
    trial.push(
        lower(
            "outage.cpu_cores_outage",
            "Broker CPU, during outage",
            "cores",
            outage.cpu_cores,
            "cpu",
        )
        .headline(),
    );
    trial.push(lower(
        "outage.cpu_cores_recovery",
        "Broker CPU, recovery",
        "cores",
        recovery.cpu_cores,
        "cpu",
    ));
    let (_, retries_peak) = col_stats(&series, col::BM_RETRIES, outage.start_s, recovery.end_s);
    let (_, backlog_peak) = col_stats(&series, col::BACKLOG, outage.start_s, recovery.end_s + 1.0);
    trial.push(lower(
        "outage.backlog_peak",
        "Peak undelivered backlog",
        "msgs",
        backlog_peak,
        "latency",
    ));
    let (_, blocked_peak) = col_stats(
        &series,
        col::BM_BLOCKED,
        outage.start_s,
        recovery.end_s + 60.0,
    );
    trial.push(info(
        "outage.hosts_blocked_peak",
        "Destination hosts the broker circuit-broke (peak)",
        "hosts",
        blocked_peak,
        "reliability",
    ));
    trial.push(info(
        "outage.retries_pending_peak",
        "Peak retries pending (broker view)",
        "msgs",
        retries_peak,
        "latency",
    ));

    // Time from outage end until the backlog returns to ~1 s of traffic.
    let threshold = rate.max(10.0);
    let mut recovered = None;
    if let Some(c) = series.col(col::BACKLOG) {
        for (t, v) in series.t.iter().zip(c.iter()) {
            if *t >= recovery.start_s && v.is_some_and(|v| v <= threshold) {
                recovered = Some(*t - recovery.start_s);
                break;
            }
        }
    }
    let rec_s = recovered
        .or(drained_in.map(|d| d.as_secs_f64() + recovery.dur_s))
        .unwrap_or(post_s);
    trial.push(
        lower(
            "outage.recovery_s",
            "Backlog cleared after destination returned",
            "s",
            rec_s,
            "latency",
        )
        .headline(),
    );
    let (_, done, perm) = trial.verifier.totals();
    let attempts = trial.shared.c.sink_recv.load(Ordering::Relaxed) as f64;
    trial.push(lower(
        "outage.attempts_per_message",
        "Delivery attempts per message (incl. retries)",
        "x",
        attempts / (done + perm).max(1) as f64,
        "cpu",
    ));
    trial.push(lower(
        "outage.rss_peak_mb",
        "RSS peak",
        "MB",
        col_stats(&series, col::RSS, outage.start_s, recovery.end_s).1,
        "memory",
    ));
    trial.tables.push(thread_cpu_table(
        &series,
        "Where broker CPU goes (outage)",
        outage.start_s,
        outage.end_s,
    ));
    let dlq = trial.api.dlq_total().await;
    trial.push(lower(
        "outage.dlq_messages",
        "Messages dead-lettered",
        "msgs",
        dlq as f64,
        "reliability",
    ));
    trial.push(higher(
        "outage.recovered",
        "Backlog fully cleared after the destination returned (1 = yes)",
        "",
        if drained_in.is_some() || recovered.is_some() {
            1.0
        } else {
            0.0
        },
        "reliability",
    ));
    if drained_in.is_none() {
        trial.note("Backlog did not fully drain within 240 s of the end of ingest.");
    }
    let _ = analysis::mean;

    trial.phase("settle", PhaseKind::Settle).await;
    trial
        .idle(ctx.secs(ctx.profile.pick(8.0, 25.0, 60.0)))
        .await;
    trial.finish().await
}
