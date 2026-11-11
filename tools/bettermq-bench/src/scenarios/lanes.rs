//! FIFO lanes: ordering must hold under load, and a single hot lane's serial
//! throughput bounds how fast any one entity's events can flow.

use super::common::*;
use crate::loadgen::SenderCfg;
use crate::model::*;
use crate::sink::DestCfg;
use crate::trial::{RunCtx, Trial};
use crate::workload::{PadPool, RatePlan};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

const LANES: usize = 5_000;
const HANDLER_MS: f64 = 3.0;
const HANDLER_SIGMA: f64 = 0.3;

fn shape(ctx: &RunCtx) -> (f64, f64, f64, f64) {
    let p = ctx.profile;
    (
        ctx.rate(150.0),
        ctx.secs(p.pick(10.0, 25.0, 60.0)).as_secs_f64(),
        ctx.rate(400.0),
        ctx.secs(p.pick(8.0, 15.0, 30.0)).as_secs_f64(),
    )
}

pub fn params(ctx: &RunCtx) -> BTreeMap<String, String> {
    let (rate, dur, hot_rate, hot_dur) = shape(ctx);
    let mut p = BTreeMap::new();
    p.insert(
        "many_lanes".into(),
        format!("{rate:.0} msg/s across {LANES} FIFO lanes for {dur:.0}s"),
    );
    p.insert(
        "hot_lane".into(),
        format!("one lane offered {hot_rate:.0} msg/s for {hot_dur:.0}s (producer waits for each ACK, like a real client)"),
    );
    p.insert(
        "destination".into(),
        format!("{HANDLER_MS} ms median handler"),
    );
    p
}

pub async fn run(ctx: Arc<RunCtx>, index: u32, seed: u64) -> anyhow::Result<TrialResult> {
    let (rate, dur_s, hot_rate, hot_s) = shape(&ctx);
    let mut trial = Trial::start(ctx.clone(), "ordered-lanes", index, seed).await?;
    trial
        .queue(
            "lanes.queue",
            DestCfg::instant("lanes").latency(HANDLER_MS, HANDLER_SIGMA),
            64,
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

    // Many lanes, steady.
    let dur = Duration::from_secs_f64(dur_s);
    let many = trial
        .verifier
        .register("many-lanes", RatePlan::expected(rate, dur), LANES, true);
    trial.phase("many-lanes", PhaseKind::Measure).await;
    let p1 = pad.clone();
    trial
        .drive(cfg.clone(), move |t0| {
            Box::new(RatePlan::new(
                many,
                "lanes.queue",
                rate,
                dur,
                600,
                LANES,
                true,
                t0,
                p1,
                seed,
            ))
        })
        .await;
    let ml = trial.end_phase().await.expect("many-lanes");
    trial.wait_drained(Duration::from_secs(60)).await;

    // One hot lane pushed past its serial capacity.
    let hdur = Duration::from_secs_f64(hot_s);
    let hot = trial
        .verifier
        .register("hot-lane", RatePlan::expected(hot_rate, hdur), 1, true);
    trial.phase("hot-lane", PhaseKind::Measure).await;
    let p2 = pad.clone();
    let hot_for_plan = hot.clone();
    trial
        .drive(cfg, move |t0| {
            Box::new(RatePlan::new(
                hot_for_plan,
                "lanes.queue",
                hot_rate,
                hdur,
                600,
                1,
                false,
                t0,
                p2,
                seed ^ 7,
            ))
        })
        .await;
    let hl = trial.end_phase().await.expect("hot-lane");
    let series = trial.recorder.series().await;
    // Serial throughput of one lane: deliveries completed while it was backlogged.
    trial.phase("hot-drain", PhaseKind::Drain).await;
    let drained = trial.wait_drained(Duration::from_secs(180)).await;
    let drain = trial.end_phase().await.expect("drain");
    let total_hot = hot.done.load(Ordering::Relaxed) as f64;
    let hot_wall = hl.dur_s + drain.dur_s;
    let lane_rate = total_hot / hot_wall.max(1e-9);

    trial.push(higher(
        "lanes.many_lane_delivery_msgs_s",
        "Delivery rate across many lanes",
        "msg/s",
        ml.delivered_rate(),
        "throughput",
    ));
    trial.push(
        lower(
            "lanes.many_lane_delivery_p99_ms",
            "Delivery p99 across many lanes",
            "ms",
            ml.delivery_ms(0.99),
            "latency",
        )
        .headline(),
    );
    trial.push(lower(
        "lanes.many_lane_ack_p99_ms",
        "ACK p99 across many lanes",
        "ms",
        ml.ack_ms(0.99),
        "latency",
    ));
    trial.push(
        higher(
            "lanes.hot_lane_msgs_s",
            "One lane's serial delivery rate",
            "msg/s",
            lane_rate,
            "throughput",
        )
        .headline(),
    );
    // Only meaningful if the lane was delivery-bound: the producer waits for
    // each ACK, so an unsaturated lane just reproduces the offered rate.
    let backlogged = hl.backlog_end > 50.0 || drain.dur_s > 3.0;
    if lane_rate > 0.0 && backlogged {
        let mean_handler_ms = HANDLER_MS * (HANDLER_SIGMA * HANDLER_SIGMA / 2.0).exp();
        trial.push(lower(
            "lanes.hop_overhead_ms",
            "Broker overhead per message on one lane (1/rate - mean handler)",
            "ms",
            (1000.0 / lane_rate - mean_handler_ms).max(0.0),
            "latency",
        ));
    } else {
        trial.note("The hot lane was not backlogged (producer or ACK latency was the limit), so its per-hop broker overhead was not computed; raise the offered rate.");
    }
    trial.push(lower(
        "lanes.hot_lane_ack_p99_ms",
        "ACK p99 on the hot lane",
        "ms",
        hl.ack_ms(0.99),
        "latency",
    ));
    trial.push(lower(
        "lanes.cpu_cores_many",
        "Broker CPU, many lanes",
        "cores",
        ml.cpu_cores,
        "cpu",
    ));
    trial.tables.push(thread_cpu_table(
        &series,
        "Where broker CPU goes (many lanes)",
        ml.start_s,
        ml.end_s,
    ));
    if drained.is_none() {
        trial.note("Hot lane had not drained 180 s after ingest stopped.");
    }
    trial.phase("settle", PhaseKind::Settle).await;
    trial
        .idle(ctx.secs(ctx.profile.pick(8.0, 25.0, 60.0)))
        .await;
    trial.finish().await
}
