//! Scenario catalogue. Each scenario runs one isolated [`Trial`] and returns
//! its [`TrialResult`]; the runner repeats and aggregates trials.

pub mod backlog;
pub mod burst;
pub mod common;
pub mod cron;
pub mod cycles;
pub mod fanout;
pub mod idle;
pub mod isolation;
pub mod lanes;
pub mod mixed;
pub mod outage;
pub mod ramp;
pub mod recovery;

use crate::model::TrialResult;
use crate::trial::RunCtx;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct Info {
    pub id: &'static str,
    pub title: &'static str,
    pub summary: &'static str,
    /// Rough wall-clock minutes per trial at smoke / standard / full.
    pub minutes: (f32, f32, f32),
}

pub const ALL: &[Info] = &[
    Info {
        id: "idle",
        title: "Idle footprint",
        summary: "Broker with a configured fleet of queues and dormant schedules but no traffic: what it costs just to exist.",
        minutes: (0.3, 0.8, 1.5),
    },
    Info {
        id: "ingest-single",
        title: "Ingest ceiling: single enqueue",
        summary: "Step-load ramp of one-message requests against an instant destination, to find the sustained ingest limit and how latency, CPU and memory behave as it is approached and passed.",
        minutes: (1.0, 4.0, 8.0),
    },
    Info {
        id: "ingest-batch",
        title: "Ingest ceiling: batched enqueue",
        summary: "Same ramp with 100-message batches, the high-throughput path.",
        minutes: (1.0, 4.0, 8.0),
    },
    Info {
        id: "ingest-large",
        title: "Ingest ceiling: 64 KB payloads",
        summary: "Single-enqueue ramp with 64 KB bodies: where bandwidth and WAL volume, not request rate, become the limit.",
        minutes: (0.8, 3.0, 6.0),
    },
    Info {
        id: "e2e-ramp",
        title: "End-to-end ramp",
        summary: "Ramp with a realistic 20 ms destination and 256-way parallelism: the point where delivery, not ingest, stops keeping up.",
        minutes: (1.0, 4.0, 8.0),
    },
    Info {
        id: "mixed-day",
        title: "Realistic mixed workload (compressed business day)",
        summary: "Nine job families with tenant skew, FIFO lanes, idempotent re-sends, delayed jobs, flaky destinations and daily bursts, at a production-like fraction of capacity. The scenario to watch for per-family latency, CPU cost, and memory behaviour.",
        minutes: (1.5, 6.0, 20.0),
    },
    Info {
        id: "burst-drain",
        title: "Burst absorption and drain",
        summary: "Nominal load, a 5x burst, then back to nominal: peak backlog, time to recover, and how latency behaves around the burst.",
        minutes: (1.0, 3.0, 6.0),
    },
    Info {
        id: "cron-fleet",
        title: "Scheduled jobs at scale",
        summary: "A realistic 50-job installation, then thousands of aligned and staggered schedules: fire lateness, missed fires, and the cost of holding the schedule.",
        minutes: (1.5, 5.0, 12.0),
    },
    Info {
        id: "outage-error",
        title: "Destination outage (errors)",
        summary: "A destination returns 503 for a while under steady ingest: retry cost, ingest isolation, and catch-up time after it returns.",
        minutes: (1.2, 3.5, 8.0),
    },
    Info {
        id: "outage-timeout",
        title: "Destination outage (hung connections)",
        summary: "A destination accepts connections and never answers: timeouts tie up delivery slots instead of failing fast.",
        minutes: (1.2, 3.5, 8.0),
    },
    Info {
        id: "noisy-neighbor",
        title: "Noisy neighbour isolation",
        summary: "One tenant's destination hangs while another tenant keeps publishing: does the healthy tenant's latency change?",
        minutes: (1.2, 3.5, 7.0),
    },
    Info {
        id: "ordered-lanes",
        title: "FIFO lanes and hot-lane throughput",
        summary: "Ordering under load across thousands of keyed lanes, and how fast one hot lane can deliver serially.",
        minutes: (1.0, 2.5, 4.0),
    },
    Info {
        id: "fanout",
        title: "Group fan-out with unhealthy members",
        summary: "One publish becomes seven deliveries; one member is slow, one flaky, one goes down mid-run.",
        minutes: (1.0, 2.5, 6.0),
    },
    Info {
        id: "backlog-depth",
        title: "Cost of a deep backlog",
        summary: "Fill a queue behind a hung destination to progressively deeper levels: memory and disk per queued message, ingest latency as depth grows, and drain rate.",
        minutes: (1.0, 4.0, 25.0),
    },
    Info {
        id: "backlog-scheduled",
        title: "Cost of a deep scheduled backlog",
        summary: "The same, for messages scheduled an hour in the future (delayed jobs, reminders).",
        minutes: (1.0, 3.5, 20.0),
    },
    Info {
        id: "memory-cycles",
        title: "Memory release across repeated bursts",
        summary: "Repeated burst, drain, idle cycles: does settled memory, fd count, thread count, or disk return to the same level each time?",
        minutes: (1.5, 5.0, 15.0),
    },
    Info {
        id: "crash-recovery",
        title: "Crash and restart with a backlog",
        summary: "SIGKILL and SIGTERM restarts with thousands of undelivered messages: restart time, no acknowledged message lost.",
        minutes: (1.0, 3.0, 8.0),
    },
];

pub fn find(id: &str) -> Option<&'static Info> {
    ALL.iter().find(|i| i.id == id)
}

pub async fn run(id: &str, ctx: Arc<RunCtx>, index: u32, seed: u64) -> anyhow::Result<TrialResult> {
    match id {
        "idle" => idle::run(ctx, index, seed).await,
        "ingest-single" | "ingest-batch" | "ingest-large" | "e2e-ramp" => {
            ramp::run(id, ctx, index, seed).await
        }
        "mixed-day" => mixed::run(ctx, index, seed).await,
        "burst-drain" => burst::run(ctx, index, seed).await,
        "cron-fleet" => cron::run(ctx, index, seed).await,
        "outage-error" | "outage-timeout" => outage::run(id, ctx, index, seed).await,
        "noisy-neighbor" => isolation::run(ctx, index, seed).await,
        "ordered-lanes" => lanes::run(ctx, index, seed).await,
        "fanout" => fanout::run(ctx, index, seed).await,
        "backlog-depth" | "backlog-scheduled" => backlog::run(id, ctx, index, seed).await,
        "memory-cycles" => cycles::run(ctx, index, seed).await,
        "crash-recovery" => recovery::run(ctx, index, seed).await,
        other => anyhow::bail!("unknown scenario `{other}`"),
    }
}

pub fn params(id: &str, ctx: &RunCtx) -> BTreeMap<String, String> {
    match id {
        "idle" => idle::params(ctx),
        "ingest-single" | "ingest-batch" | "ingest-large" | "e2e-ramp" => ramp::params(id, ctx),
        "mixed-day" => mixed::params(ctx),
        "burst-drain" => burst::params(ctx),
        "cron-fleet" => cron::params(ctx),
        "outage-error" | "outage-timeout" => outage::params(id, ctx),
        "noisy-neighbor" => isolation::params(ctx),
        "ordered-lanes" => lanes::params(ctx),
        "fanout" => fanout::params(ctx),
        "backlog-depth" | "backlog-scheduled" => backlog::params(id, ctx),
        "memory-cycles" => cycles::params(ctx),
        "crash-recovery" => recovery::params(ctx),
        _ => BTreeMap::new(),
    }
}
