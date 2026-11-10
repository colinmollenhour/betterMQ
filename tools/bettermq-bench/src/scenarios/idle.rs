//! What the broker costs when nothing is happening.

use super::common::*;
use crate::analysis;
use crate::model::*;
use crate::recorder::col;
use crate::sink::DestCfg;
use crate::trial::{RunCtx, Trial};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;

const QUEUES: usize = 40;
const CRONS: usize = 60;

pub fn params(ctx: &RunCtx) -> BTreeMap<String, String> {
    let mut p = BTreeMap::new();
    p.insert("queues".into(), QUEUES.to_string());
    p.insert("dormant_schedules".into(), CRONS.to_string());
    p.insert(
        "observe_s".into(),
        ctx.secs(ctx.profile.pick(15.0, 45.0, 120.0))
            .as_secs()
            .to_string(),
    );
    p
}

pub async fn run(ctx: Arc<RunCtx>, index: u32, seed: u64) -> anyhow::Result<TrialResult> {
    let mut trial = Trial::start(ctx.clone(), "idle", index, seed).await?;
    let dest = trial.sink.add_dest(DestCfg::instant("idle"));
    for i in 0..QUEUES {
        trial
            .api
            .create_queue(
                &crate::api::QueueSpec {
                    name: format!("idle.queue-{i:02}"),
                    url: trial.sink.url(dest),
                    parallelism: 16,
                    max_retries: 5,
                    backoff: None,
                },
                "s3cret",
            )
            .await?;
    }
    // Schedules that exist but will not fire during the observation window,
    // like nightly and weekly housekeeping jobs.
    for i in 0..CRONS {
        let every = if i % 3 == 0 { 604_800 } else { 86_400 };
        trial
            .api
            .create_cron(json!({
                "every_seconds": every,
                "url": trial.sink.url(dest),
                "secret": "s3cret",
                "body": {"cr": 900_000 + i},
            }))
            .await?;
    }
    let observe = ctx.secs(ctx.profile.pick(15.0, 45.0, 120.0));
    trial.phase("baseline", PhaseKind::Idle).await;
    trial.idle(observe).await;
    let ps = trial.end_phase().await.expect("phase was open");
    let series = trial.recorder.series().await;
    // Settle the first 5 s of the window: startup work should be over by then.
    let from = ps.start_s + 5.0;
    let end = ps.end_s;
    let cpu = analysis::mean(&series.window(col::CPU, from, end));
    let ctxsw = analysis::mean(&series.window(col::CTX_VOL, from, end));
    let thr = analysis::mean(&series.window(col::THREADS, from, end));
    let fds = analysis::mean(&series.window(col::FDS, from, end));
    let wr = analysis::mean(&series.window(col::DISK_W, from, end));
    trial.push(
        lower(
            "idle.cpu_pct",
            "Idle CPU (one core = 100%)",
            "%",
            cpu * 100.0,
            "cpu",
        )
        .headline(),
    );
    trial.push(lower("idle.rss_mb", "Idle RSS", "MB", ps.rss_mb_end, "memory").headline());
    trial.push(lower("idle.threads", "Threads", "count", thr, "resources"));
    trial.push(lower(
        "idle.fds",
        "Open file descriptors",
        "count",
        fds,
        "resources",
    ));
    trial.push(lower(
        "idle.ctx_switches_s",
        "Voluntary context switches",
        "/s",
        ctxsw,
        "cpu",
    ));
    trial.push(lower(
        "idle.disk_write_kb_s",
        "Idle disk writes",
        "KB/s",
        wr * 1024.0,
        "resources",
    ));
    trial.push(lower(
        "idle.data_mb",
        "Data directory size",
        "MB",
        ps.data_mb_end,
        "resources",
    ));
    trial.note(format!(
        "Observed {:.0}s with {QUEUES} queues and {CRONS} dormant schedules configured.",
        ps.dur_s
    ));
    trial.finish().await
}
