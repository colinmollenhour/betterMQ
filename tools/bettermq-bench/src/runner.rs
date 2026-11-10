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
                    report.meta.finished_at = chrono::Utc::now().to_rfc3339();
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
