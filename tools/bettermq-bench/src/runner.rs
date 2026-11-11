//! Orchestrates a run: preflight, calibration, scenarios x trials, aggregation.

use crate::analysis::{self, FindingCtx};
use crate::model::*;
use crate::scenarios;
use crate::trial::RunCtx;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;

pub struct RunPlan {
    pub ctx: Arc<RunCtx>,
    pub scenarios: Vec<String>,
    pub repeats: u32,
    pub calibrate: bool,
    pub bench_cpus: String,
    pub command_line: String,
    /// Refuse to start when the preflight found anything that would make results untrustworthy.
    pub strict_env: bool,
    pub label: String,
}

fn broker_fingerprint(bin: &Path) -> (String, u64) {
    match std::fs::read(bin) {
        Ok(bytes) => (hex::encode(Sha256::digest(&bytes)), bytes.len() as u64),
        Err(_) => (String::new(), 0),
    }
}

fn repo_root(bin: &Path) -> std::path::PathBuf {
    bin.ancestors()
        .find(|p| p.join(".git").exists())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default())
}

pub fn status_from(findings: &[Finding], errored: bool) -> Status {
    if errored {
        Status::Error
    } else if findings.iter().any(|f| f.severity == Severity::Critical) {
        Status::Fail
    } else if findings.iter().any(|f| f.severity == Severity::Warn) {
        Status::Warn
    } else {
        Status::Pass
    }
}

pub async fn run(
    plan: RunPlan,
    mut on_progress: impl FnMut(&RunReport),
) -> anyhow::Result<RunReport> {
    let ctx = plan.ctx.clone();
    std::fs::create_dir_all(&ctx.data_root)?;
    let bench_list = crate::env::parse_cpu_list(&plan.bench_cpus).ok();
    let env = crate::env::collect(
        &ctx.data_root,
        ctx.broker_cpus.as_deref(),
        bench_list.as_deref(),
    );
    let root = repo_root(&ctx.broker_bin);
    let (sha, size) = broker_fingerprint(&ctx.broker_bin);
    let version = std::process::Command::new(&ctx.broker_bin)
        .arg("--version")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    let meta = RunMeta {
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        started_at: chrono::Utc::now().to_rfc3339(),
        finished_at: String::new(),
        profile: ctx.profile.name().to_string(),
        seed: ctx.seed,
        repeats: plan.repeats,
        command_line: plan.command_line.clone(),
        git_sha: crate::env::git(&root, &["rev-parse", "--short=12", "HEAD"]),
        git_dirty: !crate::env::git(&root, &["status", "--porcelain", "--untracked-files=no"])
            .is_empty(),
        git_branch: crate::env::git(&root, &["rev-parse", "--abbrev-ref", "HEAD"]),
        label: plan.label.clone(),
        broker_binary: ctx.broker_bin.display().to_string(),
        broker_sha256: sha,
        broker_size_bytes: size,
        broker_version: version,
        fsync: ctx.fsync.clone(),
        rust_log: ctx.rust_log.clone(),
        broker_cpus: ctx
            .broker_cpus
            .as_ref()
            .map(|c| format!("{c:?}"))
            .unwrap_or_else(|| "unpinned".into()),
        bench_cpus: plan.bench_cpus.clone(),
        data_root: ctx.data_root.display().to_string(),
        auth: "local API token (Bearer)".into(),
        slo: ctx.slo.clone(),
        rate_scale: ctx.rate_scale,
        time_scale: ctx.time_scale,
        broker_env: ctx
            .extra_env
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect(),
    };
    let calibration = if plan.calibrate {
        eprintln!("calibrating load generator against a null server...");
        match crate::calibrate::run(3.0).await {
            Ok(c) => {
                eprintln!(
                    "  harness ceiling ~{:.0} msg/s, floor p50 {:.0} us / p99 {:.0} us",
                    c.null_server_msgs_s, c.null_server_p50_us, c.null_server_p99_us
                );
                Some(c)
            }
            Err(e) => {
                eprintln!("  calibration failed: {e:#}");
                None
            }
        }
    } else {
        None
    };
    let mut report = RunReport {
        schema: SCHEMA_VERSION,
        meta,
        env,
        calibration,
        scenarios: Vec::new(),
    };
    for w in &report.env.warnings {
        eprintln!("warning: {w}");
    }
    if plan.strict_env && !report.env.warnings.is_empty() {
        anyhow::bail!(
            "--strict-env: refusing to run with {} environment warning(s) (listed above)",
            report.env.warnings.len()
        );
    }

    let total = plan.scenarios.len();
    for (si, id) in plan.scenarios.iter().enumerate() {
        let info = scenarios::find(id).ok_or_else(|| anyhow::anyhow!("unknown scenario `{id}`"))?;
        let mut trials: Vec<TrialResult> = Vec::new();
        let mut errors: Vec<String> = Vec::new();
        for t in 0..plan.repeats {
            eprintln!(
                "[{}/{}] {} - trial {}/{}",
                si + 1,
                total,
                id,
                t + 1,
                plan.repeats
            );
            let started = std::time::Instant::now();
            let seed = ctx.seed.wrapping_add(u64::from(t) * 7919);
            let outcome = tokio::select! {
                r = scenarios::run(id, ctx.clone(), t, seed) => r,
                _ = tokio::signal::ctrl_c() => {
                    eprintln!("interrupted; writing partial results");
                    return Ok(report);
                }
            };
            match outcome {
                Ok(trial) => {
                    eprintln!(
                        "    done in {:.0}s{}",
                        started.elapsed().as_secs_f64(),
                        headline_summary(&trial)
                    );
                    trials.push(trial);
                }
                Err(e) => {
                    eprintln!("    FAILED: {e:#}");
                    errors.push(format!("{e:#}"));
                }
            }
        }
        let mut findings = Vec::new();
        for trial in &trials {
            findings.extend(analysis::trial_findings(&FindingCtx {
                scenario: id,
                trial,
            }));
        }
        findings.sort_by_key(|f| std::cmp::Reverse(f.severity));
        findings.dedup_by(|a, b| a.title == b.title);
        let metrics = aggregate_metrics(&trials);
        let mut status = status_from(&findings, trials.is_empty());
        if !errors.is_empty() && status != Status::Error {
            // Medians now come from the surviving trials only: never report plain success.
            status = status.max(Status::Warn);
        }
        report.scenarios.push(ScenarioResult {
            id: id.clone(),
            title: info.title.to_string(),
            description: info.summary.to_string(),
            params: scenarios::params(id, &ctx),
            status,
            error: (!errors.is_empty()).then(|| errors.join("; ")),
            trials,
            metrics,
            findings,
        });
        on_progress(&report);
    }
    report.meta.finished_at = chrono::Utc::now().to_rfc3339();
    Ok(report)
}

fn headline_summary(t: &TrialResult) -> String {
    let parts: Vec<String> = t
        .metrics
        .iter()
        .filter(|m| m.headline)
        .take(4)
        .map(|m| format!("{}={:.3}{}", m.id, m.value, m.unit))
        .collect();
    if parts.is_empty() {
        String::new()
    } else {
        format!("  ({})", parts.join(", "))
    }
}

/// Combine several results files into one: trials of the same scenario are
/// pooled (so per-invocation runs interleaved with another build become one
/// multi-trial result), metrics re-aggregated, and findings re-derived.
pub fn merge(reports: Vec<RunReport>) -> anyhow::Result<RunReport> {
    anyhow::ensure!(
        reports
            .iter()
            .all(|report| !report.meta.finished_at.is_empty()),
        "cannot merge unfinished runs; rerun interrupted invocations first"
    );
    let mut iter = reports.into_iter();
    let mut out = iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("nothing to merge"))?;
    let mut order: Vec<String> = out.scenarios.iter().map(|s| s.id.clone()).collect();
    let mut by_id: std::collections::BTreeMap<String, ScenarioResult> =
        out.scenarios.drain(..).map(|s| (s.id.clone(), s)).collect();
    for r in iter {
        if out.calibration.is_none() {
            out.calibration = r.calibration.clone();
        }
        if r.meta.started_at < out.meta.started_at {
            out.meta.started_at = r.meta.started_at.clone();
        }
        if r.meta.finished_at > out.meta.finished_at {
            out.meta.finished_at = r.meta.finished_at.clone();
        }
        for w in &r.env.warnings {
            if !out.env.warnings.contains(w) {
                out.env.warnings.push(w.clone());
            }
        }
        for sc in r.scenarios {
            match by_id.get_mut(&sc.id) {
                Some(existing) => {
                    existing.trials.extend(sc.trials);
                    if let Some(e) = sc.error {
                        existing.error = Some(match existing.error.take() {
                            Some(prev) => format!("{prev}; {e}"),
                            None => e,
                        });
                    }
                }
                None => {
                    order.push(sc.id.clone());
                    by_id.insert(sc.id.clone(), sc);
                }
            }
        }
    }
    let mut max_trials = 0;
    for id in &order {
        let Some(sc) = by_id.remove(id) else { continue };
        let mut sc = sc;
        for (i, t) in sc.trials.iter_mut().enumerate() {
            t.index = i as u32;
        }
        let mut findings = Vec::new();
        for t in &sc.trials {
            findings.extend(analysis::trial_findings(&FindingCtx {
                scenario: &sc.id,
                trial: t,
            }));
        }
        findings.sort_by_key(|f| std::cmp::Reverse(f.severity));
        findings.dedup_by(|a, b| a.title == b.title);
        sc.metrics = aggregate_metrics(&sc.trials);
        let mut status = status_from(&findings, sc.trials.is_empty());
        if sc.error.is_some() && status != Status::Error {
            status = status.max(Status::Warn);
        }
        sc.status = status;
        sc.findings = findings;
        max_trials = max_trials.max(sc.trials.len());
        out.scenarios.push(sc);
    }
    out.meta.repeats = max_trials as u32;
    out.meta.command_line = format!("merged runs; first: {}", out.meta.command_line);
    Ok(out)
}

/// Recompute the ramp headline metrics from the stored steps using the current
/// knee definition. Used to re-read results recorded before a definition
/// changed. Metrics that depended on per-step byte counters that were not
/// stored (write amplification, payload MB/s, log bytes per message, kernel CPU
/// share, mean batch size) are dropped rather than left describing a
/// different step.
pub fn reanalyze(report: &mut RunReport) {
    const RAMP: &[&str] = &["ingest-single", "ingest-batch", "ingest-large", "e2e-ramp"];
    const DROP: &[&str] = &[
        "ramp.cpu_sys_pct",
        "ramp.log_bytes_per_msg",
        "ramp.write_amp",
        "ramp.payload_mb_s",
        "ramp.mean_batch_msgs",
    ];
    fn put(t: &mut TrialResult, m: Metric) {
        match t.metrics.iter_mut().find(|x| x.id == m.id) {
            Some(slot) => {
                slot.value = m.value;
                slot.label = m.label;
            }
            None => t.metrics.push(m),
        }
    }
    for sc in report
        .scenarios
        .iter_mut()
        .filter(|s| RAMP.contains(&s.id.as_str()))
    {
        let e2e = sc.id == "e2e-ramp";
        for t in &mut sc.trials {
            t.metrics.retain(|m| !DROP.contains(&m.id.as_str()));
            let knee = analysis::find_knee(&t.steps);
            let sustained = t
                .steps
                .iter()
                .find(|s| Some(s.offered_msg_s) == knee.sustained_offered)
                .cloned();
            let mk = |id: &str, label: &str, unit: &str, v: f64, dir: Direction, group: &str| {
                Metric::new(id, label, unit, v, dir, group)
            };
            put(
                t,
                mk(
                    "ramp.sustained_msgs_s",
                    "Sustained ingest at SLO",
                    "msg/s",
                    knee.sustained_achieved.unwrap_or(0.0),
                    Direction::HigherBetter,
                    "throughput",
                )
                .headline(),
            );
            put(
                t,
                mk(
                    "ramp.peak_msgs_s",
                    "Peak achieved ingest (any step)",
                    "msg/s",
                    knee.peak_achieved,
                    Direction::HigherBetter,
                    "throughput",
                ),
            );
            match knee.first_fail {
                Some(i) => put(
                    t,
                    mk(
                        "ramp.breakdown_msgs_s",
                        "Offered rate at first SLO failure",
                        "msg/s",
                        t.steps[i].offered_msg_s,
                        Direction::Info,
                        "throughput",
                    ),
                ),
                None => t.metrics.retain(|m| m.id != "ramp.breakdown_msgs_s"),
            }
            let derived = [
                "ramp.ack_p50_ms",
                "ramp.ack_p99_ms",
                "ramp.cpu_cores",
                "ramp.cpu_s_per_million",
                "ramp.rss_mb",
                "ramp.ack_p99_half_ms",
                "ramp.delivered_msgs_s",
                "ramp.delivery_p99_ms",
            ];
            let Some(st) = sustained else {
                t.metrics.retain(|m| !derived.contains(&m.id.as_str()));
                continue;
            };
            put(
                t,
                mk(
                    "ramp.ack_p50_ms",
                    "ACK p50 at sustained rate",
                    "ms",
                    st.ack_p50_ms,
                    Direction::LowerBetter,
                    "latency",
                ),
            );
            put(
                t,
                mk(
                    "ramp.ack_p99_ms",
                    "ACK p99 at sustained rate",
                    "ms",
                    st.ack_p99_ms,
                    Direction::LowerBetter,
                    "latency",
                )
                .headline(),
            );
            put(
                t,
                mk(
                    "ramp.cpu_cores",
                    "Broker CPU at sustained rate",
                    "cores",
                    st.cpu_cores,
                    Direction::LowerBetter,
                    "cpu",
                ),
            );
            if st.achieved_msg_s > 0.0 {
                // cpu_cores = cpu_seconds / dur and achieved = acked / dur, so the ratio is exact.
                put(
                    t,
                    mk(
                        "ramp.cpu_s_per_million",
                        "CPU cost per million messages",
                        "core-s",
                        st.cpu_cores / st.achieved_msg_s * 1e6,
                        Direction::LowerBetter,
                        "cpu",
                    )
                    .headline(),
                );
            }
            put(
                t,
                mk(
                    "ramp.rss_mb",
                    "RSS at sustained rate",
                    "MB",
                    st.rss_mb,
                    Direction::LowerBetter,
                    "memory",
                ),
            );
            if e2e {
                put(
                    t,
                    mk(
                        "ramp.delivered_msgs_s",
                        "Delivered at sustained rate",
                        "msg/s",
                        st.delivered_msg_s,
                        Direction::HigherBetter,
                        "throughput",
                    ),
                );
                put(
                    t,
                    mk(
                        "ramp.delivery_p99_ms",
                        "Delivery p99 at sustained rate",
                        "ms",
                        st.delivery_p99_ms,
                        Direction::LowerBetter,
                        "latency",
                    ),
                );
            }
            let half = t
                .steps
                .iter()
                .rfind(|s| s.passed && s.offered_msg_s <= 0.65 * st.offered_msg_s)
                .cloned();
            match half {
                Some(h) => put(
                    t,
                    mk(
                        "ramp.ack_p99_half_ms",
                        &format!(
                            "ACK p99 at {:.0}% of ceiling",
                            100.0 * h.offered_msg_s / st.offered_msg_s
                        ),
                        "ms",
                        h.ack_p99_ms,
                        Direction::LowerBetter,
                        "latency",
                    ),
                ),
                None => t.metrics.retain(|m| m.id != "ramp.ack_p99_half_ms"),
            }
            t.notes.push("Ramp metrics recomputed with the current knee definition (an isolated failed step followed by a passing one is a transient). Per-step byte counters were not stored, so write amplification, payload MB/s and log bytes per message are not reported for this run.".into());
        }
        sc.metrics = aggregate_metrics(&sc.trials);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rep(trial_value: f64) -> RunReport {
        let trial = TrialResult {
            metrics: vec![Metric::new(
                "m",
                "M",
                "ms",
                trial_value,
                Direction::LowerBetter,
                "latency",
            )],
            ..Default::default()
        };
        RunReport {
            schema: SCHEMA_VERSION,
            meta: RunMeta {
                finished_at: "2026-11-11T00:00:00Z".into(),
                ..Default::default()
            },
            env: EnvInfo::default(),
            calibration: None,
            scenarios: vec![ScenarioResult {
                id: "s".into(),
                title: "S".into(),
                description: String::new(),
                params: Default::default(),
                status: Status::Pass,
                error: None,
                metrics: Vec::new(),
                trials: vec![trial],
                findings: Vec::new(),
            }],
        }
    }

    #[test]
    fn merge_pools_trials_and_recomputes_medians() {
        let merged = merge(vec![rep(1.0), rep(9.0), rep(3.0)]).unwrap();
        assert_eq!(merged.scenarios.len(), 1);
        let sc = &merged.scenarios[0];
        assert_eq!(sc.trials.len(), 3);
        assert_eq!(
            sc.trials.iter().map(|t| t.index).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(sc.metrics[0].value, 3.0);
        assert_eq!(sc.metrics[0].samples.len(), 3);
        assert_eq!(merged.meta.repeats, 3);
    }

    #[test]
    fn merge_rejects_an_unfinished_input() {
        let mut interrupted = rep(2.0);
        interrupted.meta.finished_at.clear();
        assert!(merge(vec![rep(1.0), interrupted]).is_err());
    }
}
