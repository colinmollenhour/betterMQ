//! A compressed business day of the generalised fulfillment workload: nine job
//! families with tenant skew, FIFO lanes, idempotent re-sends, delayed jobs,
//! flaky and dead-lettering destinations, and daily/hourly bursts.

use super::common::*;
use crate::analysis;
use crate::loadgen::SenderCfg;
use crate::model::*;
use crate::recorder::col;
use crate::sink::DestCfg;
use crate::stats::{max_ms, q_ms};
use crate::trial::{RunCtx, Trial};
use crate::workload::{MixPlan, PadPool, FULFILLMENT};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn shape(ctx: &RunCtx) -> (f64, f64) {
    let mean = ctx.rate(ctx.profile.pick(100.0, 200.0, 200.0));
    let run_s = ctx.secs(ctx.profile.pick(30.0, 240.0, 900.0)).as_secs_f64();
    (mean, run_s)
}

pub fn params(ctx: &RunCtx) -> BTreeMap<String, String> {
    let (mean, run_s) = shape(ctx);
    let mut p = BTreeMap::new();
    p.insert("mean_rate".into(), format!("{mean:.0} msg/s (daily mean)"));
    p.insert(
        "peak_rate".into(),
        format!("about {:.0} msg/s before bursts", mean * 1.35),
    );
    p.insert("compressed_day".into(), format!("{run_s:.0} s = 24 h"));
    p.insert("job_families".into(), FULFILLMENT.len().to_string());
    p.insert(
        "tenants".into(),
        format!("{} (Zipf 1.05 skew)", crate::workload::TENANTS),
    );
    p.insert("arrivals".into(), "Poisson, open loop".into());
    p.insert(
        "failure_injection".into(),
        "transient 503s and permanent 422s per family (see table)".into(),
    );
    p
}

pub async fn run(ctx: Arc<RunCtx>, index: u32, seed: u64) -> anyhow::Result<TrialResult> {
    let (mean, run_s) = shape(&ctx);
    let mut trial = Trial::start(ctx.clone(), "mixed-day", index, seed).await?;
    for f in FULFILLMENT {
        trial
            .queue(
                f.queue,
                DestCfg::instant(f.queue)
                    .latency(f.handler_median_ms, f.handler_sigma)
                    .failures(f.transient_permille, f.permanent_permille),
                f.parallelism,
                f.max_retries,
                Some((500, 8_000, 2.0)),
            )
            .await?;
    }
    let caps = MixPlan::capacity(mean, run_s, run_s);
    let streams: Vec<_> = FULFILLMENT
        .iter()
        .zip(&caps)
        .map(|(f, cap)| trial.verifier.register(f.queue, *cap, f.lanes, f.lanes > 0))
        .collect();
    let pad = PadPool::new(seed);

    trial.phase("baseline", PhaseKind::Idle).await;
    trial.idle(ctx.secs(ctx.profile.pick(4.0, 8.0, 15.0))).await;

    trial.phase("business-day", PhaseKind::Measure).await;
    let cfg = SenderCfg {
        max_in_flight: 512,
        ..SenderCfg::default()
    };
    let s2 = streams.clone();
    trial
        .drive(cfg, move |t0| {
            Box::new(MixPlan::new(s2, mean, run_s, run_s, t0, pad, seed))
        })
        .await;
    let day = trial.end_phase().await.expect("day phase");
    let series_day = trial.recorder.series().await;

    trial.phase("drain", PhaseKind::Drain).await;
    let drain_started = Instant::now();
    let drained = trial.wait_drained(Duration::from_secs(180)).await;
    let drain_s = drain_started.elapsed().as_secs_f64();
    trial.end_phase().await;
    if drained.is_none() {
        trial.note("Backlog had not fully drained 180 s after the day ended.");
    }

    // Headline numbers for the busy day.
    let (cpu_mean, cpu_peak) = col_stats(&series_day, col::CPU, day.start_s, day.end_s);
    let (_, rss_peak) = col_stats(&series_day, col::RSS, day.start_s, day.end_s);
    let (_, backlog_peak) = col_stats(&series_day, col::BACKLOG, day.start_s, day.end_s + 1.0);
    trial.push(
        higher(
            "mixed.ingest_msgs_s",
            "Mean ingest over the day",
            "msg/s",
            day.acked_rate(),
            "throughput",
        )
        .headline(),
    );
    trial.push(lower(
        "mixed.ack_p50_ms",
        "ACK p50",
        "ms",
        day.ack_ms(0.5),
        "latency",
    ));
    trial.push(
        lower(
            "mixed.ack_p99_ms",
            "ACK p99",
            "ms",
            day.ack_ms(0.99),
            "latency",
        )
        .headline(),
    );
    trial.push(lower(
        "mixed.ack_p999_ms",
        "ACK p99.9",
        "ms",
        day.ack_ms(0.999),
        "latency",
    ));
    trial.push(lower(
        "mixed.delivery_p50_ms",
        "Delivery p50 (first-attempt)",
        "ms",
        day.delivery_ms(0.5),
        "latency",
    ));
    trial.push(
        lower(
            "mixed.delivery_p99_ms",
            "Delivery p99 (first-attempt)",
            "ms",
            day.delivery_ms(0.99),
            "latency",
        )
        .headline(),
    );
    trial.push(lower(
        "mixed.error_rate_pct",
        "Rejected or failed ingest",
        "%",
        day.error_rate() * 100.0,
        "reliability",
    ));
    trial.push(
        lower(
            "mixed.cpu_cores_mean",
            "Broker CPU, mean",
            "cores",
            cpu_mean,
            "cpu",
        )
        .headline(),
    );
    trial.push(lower(
        "mixed.cpu_cores_peak",
        "Broker CPU, peak second",
        "cores",
        cpu_peak,
        "cpu",
    ));
    if day.acked > 0 {
        trial.push(lower(
            "mixed.cpu_s_per_million",
            "CPU per million messages (ingest + delivery)",
            "core-s",
            day.cpu_seconds / day.acked as f64 * 1e6,
            "cpu",
        ));
        trial.push(lower(
            "mixed.write_amp",
            "Disk bytes written per payload byte",
            "x",
            day.disk_write_mb * 1_048_576.0 / day.payload_bytes.max(1) as f64,
            "resources",
        ));
    }
    trial.push(lower(
        "mixed.rss_peak_mb",
        "RSS peak during the day",
        "MB",
        rss_peak,
        "memory",
    ));
    trial.push(lower(
        "mixed.backlog_peak",
        "Peak undelivered backlog",
        "msgs",
        backlog_peak,
        "latency",
    ));
    trial.push(lower(
        "mixed.drain_s",
        "Time to drain after the day ended",
        "s",
        drain_s,
        "latency",
    ));
    trial.push(info(
        "mixed.messages",
        "Messages accepted",
        "msgs",
        trial.verifier.totals().0 as f64,
        "throughput",
    ));

    // Per-family table.
    let mut rows = Vec::new();
    for (f, s) in FULFILLMENT.iter().zip(&streams) {
        let ack = s.ack.peek();
        let del = s.delivery.peek();
        let acked = s.acked.load(Ordering::Relaxed);
        rows.push(vec![
            f.queue.to_string(),
            acked.to_string(),
            q_ms(&ack, 0.5).map_or("-".into(), analysis::fmt_ms),
            q_ms(&ack, 0.99).map_or("-".into(), analysis::fmt_ms),
            q_ms(&del, 0.5).map_or("-".into(), analysis::fmt_ms),
            q_ms(&del, 0.99).map_or("-".into(), analysis::fmt_ms),
            max_ms(&del).map_or("-".into(), analysis::fmt_ms),
            s.perm.load(Ordering::Relaxed).to_string(),
            s.dups.load(Ordering::Relaxed).to_string(),
            f.parallelism.to_string(),
        ]);
    }
    trial.tables.push(Table {
        title: "Per job family".into(),
        columns: [
            "Queue",
            "Accepted",
            "ACK p50",
            "ACK p99",
            "Delivery p50",
            "Delivery p99",
            "Delivery max",
            "Dead-lettered",
            "Duplicates",
            "Parallelism",
        ]
        .map(String::from)
        .to_vec(),
        rows,
    });
    trial.tables.push(thread_cpu_table(
        &series_day,
        "Where broker CPU goes (business day)",
        day.start_s,
        day.end_s,
    ));
    let dlq = trial.api.dlq_total().await;
    trial.push(info(
        "mixed.dlq_messages",
        "Messages in dead-letter queues",
        "msgs",
        dlq as f64,
        "reliability",
    ));
    let injected: u64 = trial
        .verifier
        .all()
        .iter()
        .map(|s| s.perm.load(Ordering::Relaxed))
        .sum();
    trial.note(format!(
        "{injected} messages were rejected permanently by their destinations on purpose and should appear in the DLQ ({dlq} found)."
    ));

    trial.phase("settle", PhaseKind::Settle).await;
    trial
        .idle(ctx.secs(ctx.profile.pick(8.0, 25.0, 60.0)))
        .await;
    trial.finish().await
}
