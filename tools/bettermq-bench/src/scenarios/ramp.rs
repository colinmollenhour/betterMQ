//! Step-load ramps: raise the offered rate geometrically, hold each step long
//! enough to measure, and stop two steps after the first SLO failure. The
//! result is the sustained ceiling, the shape of the breakdown, and what each
//! step cost in CPU, memory, and disk.

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

struct Spec {
    wire: Wire,
    body_bytes: usize,
    handler_ms: f64,
    handler_sigma: f64,
    parallelism: u32,
    start_rate: f64,
    factor: f64,
    steps: usize,
    step_secs: f64,
    e2e: bool,
    in_flight: usize,
}

fn spec(id: &str, ctx: &RunCtx) -> Spec {
    let p = ctx.profile;
    let steps = p.pick(8, 12, 15);
    let step_secs = p.pick(6.0, 15.0, 25.0);
    let base = Spec {
        wire: Wire::Single,
        body_bytes: 1024,
        handler_ms: 0.0,
        handler_sigma: 0.0,
        parallelism: 256,
        start_rate: 250.0,
        factor: 1.5,
        steps,
        step_secs,
        e2e: false,
        in_flight: 512,
    };
    let mut s = match id {
        "ingest-batch" => Spec {
            wire: Wire::Batch(100),
            start_rate: 1_000.0,
            factor: 1.6,
            in_flight: 128,
            ..base
        },
        "ingest-large" => Spec {
            body_bytes: 65_536,
            start_rate: 40.0,
            ..base
        },
        "e2e-ramp" => Spec {
            handler_ms: 20.0,
            handler_sigma: 0.4,
            parallelism: 512,
            start_rate: 250.0,
            e2e: true,
            ..base
        },
        _ => base,
    };
    s.start_rate = ctx.rate(s.start_rate);
    s.step_secs = (s.step_secs * ctx.time_scale).max(3.0);
    s
}

pub fn params(id: &str, ctx: &RunCtx) -> BTreeMap<String, String> {
    let s = spec(id, ctx);
    let mut p = BTreeMap::new();
    p.insert(
        "wire".into(),
        match s.wire {
            Wire::Single => "single request per message".into(),
            Wire::Batch(n) => format!("batches of {n}"),
        },
    );
    p.insert("payload_bytes".into(), s.body_bytes.to_string());
    p.insert(
        "destination".into(),
        if s.handler_ms > 0.0 {
            format!(
                "{:.0} ms median handler, {} parallel",
                s.handler_ms, s.parallelism
            )
        } else {
            "instant (0 ms)".into()
        },
    );
    p.insert("start_rate".into(), format!("{:.0}/s", s.start_rate));
    p.insert("step_factor".into(), format!("x{}", s.factor));
    p.insert("steps_max".into(), s.steps.to_string());
    p.insert("step_secs".into(), format!("{:.0}", s.step_secs));
    p.insert("arrivals".into(), "constant spacing, open loop".into());
    p.insert(
        "slo".into(),
        format!(
            "ACK p99 <= {} ms, errors <= {}%, achieved >= {}% of offered{}",
            ctx.slo.ack_p99_ms,
            ctx.slo.max_error_rate * 100.0,
            ctx.slo.min_achieved_ratio * 100.0,
            if s.e2e {
                format!(
                    ", delivery p99 <= {} ms, no backlog growth",
                    ctx.slo.delivery_p99_ms
                )
            } else {
                String::new()
            }
        ),
    );
    p
}

struct Extra {
    cpu_seconds: f64,
    cpu_sys_share: f64,
    disk_write_mb: f64,
    payload_mb: f64,
    acked: u64,
    requests: u64,
    log_mb: f64,
}

pub async fn run(id: &str, ctx: Arc<RunCtx>, index: u32, seed: u64) -> anyhow::Result<TrialResult> {
    let sp = spec(id, &ctx);
    let mut trial = Trial::start(ctx.clone(), id, index, seed).await?;
    let mut dest = DestCfg::instant("ramp");
    if sp.handler_ms > 0.0 {
        dest = dest.latency(sp.handler_ms, sp.handler_sigma);
    }
    trial
        .queue("ramp.queue", dest, sp.parallelism, 3, None)
        .await?;
    let pad = PadPool::new(seed);
    let cfg = SenderCfg {
        wire: sp.wire,
        max_in_flight: sp.in_flight,
        ..SenderCfg::default()
    };

    trial.phase("baseline", PhaseKind::Idle).await;
    trial.idle(ctx.secs(ctx.profile.pick(4.0, 8.0, 15.0))).await;

    // Warm up connections, caches, and the allocator before measuring.
    trial.phase("warmup", PhaseKind::Warmup).await;
    {
        let rate = sp.start_rate * 0.5;
        let dur = Duration::from_secs_f64(5.0);
        let stream = trial
            .verifier
            .register("warmup", RatePlan::expected(rate, dur), 0, false);
        let pad = pad.clone();
        trial
            .drive(cfg.clone(), move |t0| {
                Box::new(RatePlan::new(
                    stream,
                    "ramp.queue",
                    rate,
                    dur,
                    sp.body_bytes,
                    0,
                    false,
                    t0,
                    pad,
                    seed,
                ))
            })
            .await;
        trial.wait_drained(Duration::from_secs(60)).await;
    }

    let mut extras: Vec<Extra> = Vec::new();
    let mut consecutive_fail = 0;
    let mut gen_limited_at: Option<f64> = None;
    for k in 0..sp.steps {
        let rate = sp.start_rate * sp.factor.powi(k as i32);
        let dur = Duration::from_secs_f64(sp.step_secs);
        let stream = trial.verifier.register(
            &format!("step-{k}"),
            RatePlan::expected(rate, dur),
            0,
            false,
        );
        trial.phase(&format!("step-{k}"), PhaseKind::Measure).await;
        let padc = pad.clone();
        trial
            .drive(cfg.clone(), move |t0| {
                Box::new(RatePlan::new(
                    stream,
                    "ramp.queue",
                    rate,
                    dur,
                    sp.body_bytes,
                    0,
                    false,
                    t0,
                    padc,
                    seed.wrapping_add(k as u64),
                ))
            })
            .await;
        let ps = trial.end_phase().await.expect("step phase open");
        let series = trial.recorder.series().await;
        let mut step = step_from_phase(
            k,
            &format!("step {}", k + 1),
            rate,
            &ps,
            &series,
            &ctx.slo,
            sp.e2e,
        );
        // A saturated generator masquerades as a slow broker; tell them apart.
        let inflight = analysis::mean(&series.window(col::CLIENT_INFLIGHT, ps.start_s, ps.end_s));
        if step.gen_lag_p99_ms > 50.0 && inflight < 0.5 * sp.in_flight as f64 {
            step.passed = false;
            step.fail_reasons.push(format!(
                "load generator saturated (scheduling lag {:.0} ms with only {:.0} of {} request slots busy): ceiling is at least this step, not a broker limit",
                step.gen_lag_p99_ms, inflight, sp.in_flight
            ));
            gen_limited_at.get_or_insert(rate);
        }
        extras.push(Extra {
            cpu_seconds: ps.cpu_seconds,
            cpu_sys_share: if ps.cpu_cores > 0.0 {
                ps.cpu_sys_cores / ps.cpu_cores
            } else {
                0.0
            },
            disk_write_mb: ps.disk_write_mb,
            payload_mb: ps.payload_bytes as f64 / 1_048_576.0,
            acked: ps.acked,
            requests: ps.requests,
            log_mb: ps.log_bytes as f64 / 1_048_576.0,
        });
        let failed = !step.passed;
        trial.steps.push(step);
        consecutive_fail = if failed { consecutive_fail + 1 } else { 0 };
        // Let the broker catch up so the next step starts from an empty backlog.
        let drain = Duration::from_secs_f64(sp.step_secs * 2.0 + 15.0);
        if trial.wait_drained(drain).await.is_none() {
            trial.note(format!(
                "Backlog did not drain within {:.0}s after step {}; stopping the ramp.",
                drain.as_secs_f64(),
                k + 1
            ));
            break;
        }
        if consecutive_fail >= 2 || gen_limited_at.is_some() {
            break;
        }
    }

    let knee = analysis::find_knee(&trial.steps);
    let ceiling_is_lower_bound = gen_limited_at.is_some();
    let sustained_idx = trial
        .steps
        .iter()
        .position(|s| Some(s.offered_msg_s) == knee.sustained_offered);
    trial.push(
        higher(
            "ramp.sustained_msgs_s",
            if ceiling_is_lower_bound {
                "Sustained ingest at SLO (lower bound: generator-limited)"
            } else {
                "Sustained ingest at SLO"
            },
            "msg/s",
            knee.sustained_achieved.unwrap_or(0.0),
            "throughput",
        )
        .headline(),
    );
    trial.push(higher(
        "ramp.peak_msgs_s",
        "Peak achieved ingest (any step)",
        "msg/s",
        knee.peak_achieved,
        "throughput",
    ));
    if let Some(i) = knee.first_fail {
        trial.push(info(
            "ramp.breakdown_msgs_s",
            "Offered rate at first SLO failure",
            "msg/s",
            trial.steps[i].offered_msg_s,
            "throughput",
        ));
    }
    if let Some(i) = sustained_idx {
        let (st, ex) = (trial.steps[i].clone(), &extras[i]);
        trial.push(lower(
            "ramp.ack_p50_ms",
            "ACK p50 at sustained rate",
            "ms",
            st.ack_p50_ms,
            "latency",
        ));
        trial.push(
            lower(
                "ramp.ack_p99_ms",
                "ACK p99 at sustained rate",
                "ms",
                st.ack_p99_ms,
                "latency",
            )
            .headline(),
        );
        trial.push(lower(
            "ramp.cpu_cores",
            "Broker CPU at sustained rate",
            "cores",
            st.cpu_cores,
            "cpu",
        ));
        if ex.requests > 0 && matches!(sp.wire, Wire::Batch(_)) {
            trial.push(info(
                "ramp.mean_batch_msgs",
                "Messages per request at the sustained step (batches fill with load)",
                "msgs",
                ex.acked as f64 / ex.requests as f64,
                "throughput",
            ));
        }
        if ex.acked > 0 {
            trial.push(
                lower(
                    "ramp.cpu_s_per_million",
                    "CPU cost per million messages",
                    "core-s",
                    ex.cpu_seconds / ex.acked as f64 * 1e6,
                    "cpu",
                )
                .headline(),
            );
            trial.push(lower(
                "ramp.log_bytes_per_msg",
                "Log bytes per message",
                "B",
                ex.log_mb * 1_048_576.0 / ex.acked as f64,
                "resources",
            ));
        }
        trial.push(info(
            "ramp.cpu_sys_pct",
            "Share of broker CPU spent in the kernel",
            "%",
            ex.cpu_sys_share * 100.0,
            "cpu",
        ));
        trial.push(lower(
            "ramp.rss_mb",
            "RSS at sustained rate",
            "MB",
            st.rss_mb,
            "memory",
        ));
        if ex.payload_mb > 0.0 {
            trial.push(lower(
                "ramp.write_amp",
                "Disk bytes written per payload byte",
                "x",
                ex.disk_write_mb / ex.payload_mb,
                "resources",
            ));
            trial.push(higher(
                "ramp.payload_mb_s",
                "Payload throughput at sustained rate",
                "MB/s",
                ex.payload_mb / st.duration_s.max(1e-9),
                "throughput",
            ));
        }
        if sp.e2e {
            trial.push(higher(
                "ramp.delivered_msgs_s",
                "Delivered at sustained rate",
                "msg/s",
                st.delivered_msg_s,
                "throughput",
            ));
            trial.push(lower(
                "ramp.delivery_p99_ms",
                "Delivery p99 at sustained rate",
                "ms",
                st.delivery_p99_ms,
                "latency",
            ));
        }
        // Latency while comfortably below the ceiling: the number users live with.
        let half = trial
            .steps
            .iter()
            .rfind(|s| s.passed && s.offered_msg_s <= 0.65 * st.offered_msg_s)
            .cloned();
        if let Some(h) = half {
            trial.push(lower(
                "ramp.ack_p99_half_ms",
                &format!(
                    "ACK p99 at {:.0}% of ceiling",
                    100.0 * h.offered_msg_s / st.offered_msg_s
                ),
                "ms",
                h.ack_p99_ms,
                "latency",
            ));
        }
    } else {
        trial.note("Even the first step violated the SLO; lower --rate-scale to find the ceiling.");
    }

    {
        let series = trial.recorder.series().await;
        let pick = sustained_idx.or(trial.steps.len().checked_sub(1));
        if let Some(i) = pick {
            let st = trial.steps[i].clone();
            let title = format!(
                "Where broker CPU goes ({} step, {} msg/s offered)",
                if sustained_idx.is_some() {
                    "sustained"
                } else {
                    "first"
                },
                fmt_rate(st.offered_msg_s)
            );
            trial
                .tables
                .push(thread_cpu_table(&series, &title, st.start_s, st.end_s));
        }
    }

    // Settle and measure how much memory comes back.
    trial.wait_drained(Duration::from_secs(90)).await;
    trial.phase("settle", PhaseKind::Settle).await;
    trial
        .idle(ctx.secs(ctx.profile.pick(8.0, 25.0, 60.0)))
        .await;
    trial.finish().await
}
