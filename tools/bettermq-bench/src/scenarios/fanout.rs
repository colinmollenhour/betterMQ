//! Group fan-out: one publish becomes a delivery per member. Members differ in
//! speed and health; the slow or down ones must not hold back the rest.

use super::common::*;
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

struct Member {
    name: &'static str,
    median_ms: f64,
    sigma: f64,
    transient_permille: u32,
    behaviour: &'static str,
}

const MEMBERS: &[Member] = &[
    Member {
        name: "partner-a",
        median_ms: 20.0,
        sigma: 0.4,
        transient_permille: 0,
        behaviour: "fast",
    },
    Member {
        name: "partner-b",
        median_ms: 20.0,
        sigma: 0.4,
        transient_permille: 0,
        behaviour: "fast",
    },
    Member {
        name: "partner-c",
        median_ms: 40.0,
        sigma: 0.5,
        transient_permille: 0,
        behaviour: "fast",
    },
    Member {
        name: "partner-d",
        median_ms: 40.0,
        sigma: 0.5,
        transient_permille: 0,
        behaviour: "fast",
    },
    Member {
        name: "partner-slow",
        median_ms: 400.0,
        sigma: 1.0,
        transient_permille: 0,
        behaviour: "slow (400 ms median, heavy tail)",
    },
    Member {
        name: "partner-flaky",
        median_ms: 60.0,
        sigma: 0.6,
        transient_permille: 200,
        behaviour: "20% of attempts fail with 503",
    },
    Member {
        name: "partner-down",
        median_ms: 30.0,
        sigma: 0.4,
        transient_permille: 0,
        behaviour: "down for the middle third of the run",
    },
];

fn shape(ctx: &RunCtx) -> (f64, f64) {
    (
        ctx.rate(50.0),
        ctx.secs(ctx.profile.pick(15.0, 45.0, 180.0)).as_secs_f64(),
    )
}

pub fn params(ctx: &RunCtx) -> BTreeMap<String, String> {
    let (rate, dur) = shape(ctx);
    let mut p = BTreeMap::new();
    p.insert("publishes".into(), format!("{rate:.0}/s for {dur:.0}s"));
    p.insert("members".into(), MEMBERS.len().to_string());
    p.insert(
        "amplification".into(),
        format!("{}x deliveries per publish", MEMBERS.len()),
    );
    p
}

pub async fn run(ctx: Arc<RunCtx>, index: u32, seed: u64) -> anyhow::Result<TrialResult> {
    let (rate, dur_s) = shape(&ctx);
    let mut trial = Trial::start(ctx.clone(), "fanout", index, seed).await?;
    let group = trial.api.create_group("partner-events").await?;
    let mut dests = Vec::new();
    for m in MEMBERS {
        let idx = trial.sink.add_dest(
            DestCfg::instant(m.name)
                .latency(m.median_ms, m.sigma)
                .failures(m.transient_permille, 0)
                .parallelism(16),
        );
        trial
            .api
            .add_member(&group, m.name, &trial.sink.url(idx), 16)
            .await?;
        dests.push(idx);
    }
    let dur = Duration::from_secs_f64(dur_s);
    let stream = trial.verifier.register_fanout(
        "fanout",
        RatePlan::expected(rate, dur),
        0,
        false,
        MEMBERS.len() as u32,
    );
    let pad = PadPool::new(seed);
    let cfg = SenderCfg {
        max_in_flight: 256,
        ..SenderCfg::default()
    };
    trial.phase("baseline", PhaseKind::Idle).await;
    trial.idle(ctx.secs(ctx.profile.pick(4.0, 8.0, 15.0))).await;

    trial.phase("publishing", PhaseKind::Measure).await;
    let down = *dests.last().expect("members");
    let stats = {
        let t = &trial;
        let sink = &t.sink;
        let flap = async move {
            tokio::time::sleep(Duration::from_secs_f64(dur_s / 3.0)).await;
            sink.set_mode(down, Mode::Fail503);
            tokio::time::sleep(Duration::from_secs_f64(dur_s / 3.0)).await;
            sink.set_mode(down, Mode::Normal);
        };
        let st = stream.clone();
        let g: Arc<str> = group.as_str().into();
        let drive = t.drive(cfg, move |t0| {
            let mut plan = RatePlan::new(st, "", rate, dur, 800, 0, true, t0, pad, seed);
            plan.group = Some(g);
            Box::new(plan)
        });
        tokio::join!(flap, drive);
        t.end_phase().await.expect("phase open")
    };
    let series = trial.recorder.series().await;
    trial.phase("drain", PhaseKind::Drain).await;
    let drained = trial.wait_drained(Duration::from_secs(240)).await;
    let drain = trial.end_phase().await.expect("drain");

    let published = stream.acked.load(Ordering::Relaxed) as f64;
    let delivered = stream.done.load(Ordering::Relaxed) as f64;
    trial.push(
        lower(
            "fanout.publish_ack_p99_ms",
            "Publish ACK p99",
            "ms",
            stats.ack_ms(0.99),
            "latency",
        )
        .headline(),
    );
    trial.push(higher(
        "fanout.deliveries_msgs_s",
        "Deliveries per second (all members)",
        "msg/s",
        delivered / (stats.dur_s + drain.dur_s).max(1e-9),
        "throughput",
    ));
    trial.push(lower(
        "fanout.delivery_p50_ms",
        "Delivery p50 (first-attempt)",
        "ms",
        stats.delivery_ms(0.5),
        "latency",
    ));
    trial.push(
        lower(
            "fanout.delivery_p99_ms",
            "Delivery p99 (first-attempt)",
            "ms",
            stats.delivery_ms(0.99),
            "latency",
        )
        .headline(),
    );
    trial.push(
        lower(
            "fanout.cpu_cores",
            "Broker CPU",
            "cores",
            stats.cpu_cores,
            "cpu",
        )
        .headline(),
    );
    if delivered > 0.0 {
        trial.push(lower(
            "fanout.cpu_s_per_million_deliveries",
            "CPU per million deliveries",
            "core-s",
            (stats.cpu_seconds + drain.cpu_seconds) / delivered * 1e6,
            "cpu",
        ));
    }
    trial.push(lower(
        "fanout.completeness_pct_missing",
        "Deliveries missing after drain",
        "%",
        100.0 * (published * MEMBERS.len() as f64 - delivered).max(0.0)
            / (published * MEMBERS.len() as f64).max(1.0),
        "reliability",
    ));
    let mut rows = Vec::new();
    for (m, idx) in MEMBERS.iter().zip(&dests) {
        let d = trial.sink.dests()[*idx].clone();
        rows.push(vec![
            m.name.to_string(),
            m.behaviour.to_string(),
            d.ok.load(Ordering::Relaxed).to_string(),
            published.to_string(),
            d.recv.load(Ordering::Relaxed).to_string(),
            d.fail_503.load(Ordering::Relaxed).to_string(),
            d.rejected_mode.load(Ordering::Relaxed).to_string(),
        ]);
    }
    trial.tables.push(Table {
        title: "Per member".into(),
        columns: [
            "Member",
            "Behaviour",
            "Delivered",
            "Published",
            "Attempts",
            "Injected 503",
            "Down-window 503",
        ]
        .map(String::from)
        .to_vec(),
        rows,
    });
    trial.tables.push(thread_cpu_table(
        &series,
        "Where broker CPU goes (fan-out)",
        stats.start_s,
        stats.end_s,
    ));
    let _ = col::CPU;
    if drained.is_none() {
        trial.note("Fan-out backlog had not drained 240 s after publishing stopped.");
    }
    trial.phase("settle", PhaseKind::Settle).await;
    trial
        .idle(ctx.secs(ctx.profile.pick(8.0, 25.0, 60.0)))
        .await;
    trial.finish().await
}
