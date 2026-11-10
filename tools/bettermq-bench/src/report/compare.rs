//! Baseline vs candidate comparison, direction-aware and noise-aware.

use super::fmt;
use crate::model::*;
use std::fmt::Write as _;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Verdict {
    Improved,
    Regressed,
    Same,
    /// Changed, but the metric has no good/bad direction.
    Changed,
}

pub struct Row {
    pub scenario: String,
    pub label: String,
    pub unit: String,
    pub base: f64,
    pub cand: f64,
    pub delta: f64,
    pub verdict: Verdict,
    pub noisy: bool,
}

pub struct Comparison {
    pub rows: Vec<Row>,
    /// Metrics that regressed past the threshold.
    pub regressions: usize,
    /// Candidate-side correctness failures: scenarios that errored or failed,
    /// critical findings, and metrics that vanished or became non-finite.
    pub hard_failures: Vec<String>,
    pub improvements: usize,
    pub only_in_base: Vec<String>,
    pub only_in_cand: Vec<String>,
    pub caveats: Vec<String>,
}

fn rel(base: f64, cand: f64) -> f64 {
    if base.abs() < 1e-12 {
        if cand.abs() < 1e-12 {
            0.0
        } else {
            f64::INFINITY
        }
    } else {
        (cand - base) / base.abs()
    }
}

/// Classify one metric. A change only counts when it clears both the user's
/// threshold and twice the trial-to-trial spread seen in either run.
pub fn classify(a: &Metric, b: &Metric, threshold: f64) -> (f64, Verdict, bool) {
    let delta = rel(a.value, b.value);
    let noise = a.cv().unwrap_or(0.0).max(b.cv().unwrap_or(0.0)) * 2.0;
    let bar = threshold.max(noise);
    let noisy = noise > threshold;
    let verdict = if delta.abs() < bar {
        Verdict::Same
    } else {
        match (a.direction, delta > 0.0) {
            (Direction::HigherBetter, true) | (Direction::LowerBetter, false) => Verdict::Improved,
            (Direction::HigherBetter, false) | (Direction::LowerBetter, true) => Verdict::Regressed,
            (Direction::Info, _) => Verdict::Changed,
        }
    };
    (delta, verdict, noisy)
}

pub fn compare(a: &RunReport, b: &RunReport, threshold: f64) -> Comparison {
    let mut rows = Vec::new();
    let mut only_base = Vec::new();
    let mut only_cand = Vec::new();
    let mut hard: Vec<String> = Vec::new();
    for sa in &a.scenarios {
        match b.scenarios.iter().find(|s| s.id == sa.id) {
            None => only_base.push(sa.id.clone()),
            Some(sb) => {
                for ma in &sa.metrics {
                    if !sb.metrics.iter().any(|m| m.id == ma.id) && ma.value.is_finite() {
                        hard.push(format!(
                            "{}: metric `{}` is missing from the candidate",
                            sa.id, ma.id
                        ));
                    }
                    if let Some(mb) = sb.metrics.iter().find(|m| m.id == ma.id) {
                        if ma.value.is_finite() && !mb.value.is_finite() {
                            hard.push(format!(
                                "{}: `{}` has no value in the candidate (nothing was measured, e.g. no requests succeeded)",
                                sa.id, ma.id
                            ));
                        }
                        if !ma.value.is_finite() || !mb.value.is_finite() {
                            continue;
                        }
                        let (delta, verdict, noisy) = classify(ma, mb, threshold);
                        rows.push(Row {
                            scenario: sa.id.clone(),
                            label: ma.label.clone(),
                            unit: ma.unit.clone(),
                            base: ma.value,
                            cand: mb.value,
                            delta,
                            verdict,
                            noisy,
                        });
                    }
                }
            }
        }
    }
    for sb in &b.scenarios {
        if !a.scenarios.iter().any(|s| s.id == sb.id) {
            only_cand.push(sb.id.clone());
        }
        // Correctness regressions are judged on the candidate alone: a run that
        // loses messages or errors must fail the gate however fast it was.
        let was_bad = a.scenarios.iter().find(|s| s.id == sb.id).map(|s| s.status);
        if matches!(sb.status, Status::Error) {
            hard.push(format!(
                "{}: scenario errored ({})",
                sb.id,
                sb.error.clone().unwrap_or_default()
            ));
        } else if sb.status == Status::Fail && was_bad != Some(Status::Fail) {
            hard.push(format!("{}: scenario now FAILS correctness checks", sb.id));
        }
        for f in sb
            .findings
            .iter()
            .filter(|f| f.severity == Severity::Critical)
        {
            let existed = a
                .scenarios
                .iter()
                .filter(|s| s.id == sb.id)
                .flat_map(|s| s.findings.iter())
                .any(|g| g.title == f.title);
            if !existed {
                hard.push(format!(
                    "{}: new critical finding: {} ({})",
                    sb.id, f.title, f.detail
                ));
            }
        }
        if let Some(e) = &sb.error {
            if sb.status != Status::Error {
                hard.push(format!("{}: some trials errored: {e}", sb.id));
            }
        }
    }
    let mut caveats = Vec::new();
    let (ea, eb) = (&a.env, &b.env);
    if ea.cpu_model != eb.cpu_model || ea.logical_cpus != eb.logical_cpus {
        caveats.push(format!("Different hardware: `{}` ({} CPUs) vs `{}` ({} CPUs). Absolute numbers are not comparable.", ea.cpu_model, ea.logical_cpus, eb.cpu_model, eb.logical_cpus));
    }
    if a.meta.profile != b.meta.profile {
        caveats.push(format!(
            "Different profiles: {} vs {}.",
            a.meta.profile, b.meta.profile
        ));
    }
    if (a.meta.rate_scale - b.meta.rate_scale).abs() > 1e-9
        || (a.meta.time_scale - b.meta.time_scale).abs() > 1e-9
    {
        caveats.push(format!(
            "Different --rate-scale/--time-scale ({}x/{}x vs {}x/{}x): offered rates, backlog depths and run lengths differ.",
            a.meta.rate_scale, a.meta.time_scale, b.meta.rate_scale, b.meta.time_scale
        ));
    }
    if a.meta.slo.ack_p99_ms != b.meta.slo.ack_p99_ms
        || a.meta.slo.delivery_p99_ms != b.meta.slo.delivery_p99_ms
    {
        caveats
            .push("Different SLO thresholds: ramp 'sustained' metrics are not comparable.".into());
    }
    if a.meta.broker_env != b.meta.broker_env {
        caveats.push(format!(
            "Different extra broker environment: {:?} vs {:?}.",
            a.meta.broker_env, b.meta.broker_env
        ));
    }
    if a.meta.broker_cpus != b.meta.broker_cpus || a.meta.bench_cpus != b.meta.bench_cpus {
        caveats.push("Different CPU pinning.".into());
    }
    for sa in &a.scenarios {
        if let Some(sb) = b.scenarios.iter().find(|s| s.id == sa.id) {
            if sa.params != sb.params {
                caveats.push(format!(
                    "Scenario `{}` ran with different parameters.",
                    sa.id
                ));
            }
        }
    }
    if a.meta.seed != b.meta.seed {
        caveats.push("Different workload seeds: offered load differs between runs.".into());
    }
    if a.meta.fsync != b.meta.fsync || a.meta.rust_log != b.meta.rust_log {
        caveats.push(format!(
            "Different broker settings: fsync {} vs {}, RUST_LOG {} vs {}.",
            a.meta.fsync, b.meta.fsync, a.meta.rust_log, b.meta.rust_log
        ));
    }
    if ea.data_fs_type != eb.data_fs_type || ea.governor != eb.governor {
        caveats.push(format!(
            "Different storage or CPU governor: {} / {} vs {} / {}.",
            ea.data_fs_type, ea.governor, eb.data_fs_type, eb.governor
        ));
    }
    if a.meta.repeats < 2 || b.meta.repeats < 2 {
        caveats.push(
            "Single-trial runs have no noise estimate; treat small deltas with suspicion.".into(),
        );
    }
    rows.sort_by(|x, y| {
        let rank = |r: &Row| match r.verdict {
            Verdict::Regressed => 0,
            Verdict::Improved => 1,
            Verdict::Changed => 2,
            Verdict::Same => 3,
        };
        rank(x).cmp(&rank(y)).then(
            y.delta
                .abs()
                .partial_cmp(&x.delta.abs())
                .unwrap_or(std::cmp::Ordering::Equal),
        )
    });
    Comparison {
        hard_failures: hard,
        regressions: rows
            .iter()
            .filter(|r| r.verdict == Verdict::Regressed)
            .count(),
        improvements: rows
            .iter()
            .filter(|r| r.verdict == Verdict::Improved)
            .count(),
        rows,
        only_in_base: only_base,
        only_in_cand: only_cand,
        caveats,
    }
}

fn val(v: f64, unit: &str) -> String {
    if unit.is_empty() {
        fmt::num(v)
    } else {
        format!("{} {unit}", fmt::num(v))
    }
}

pub fn render_markdown(a: &RunReport, b: &RunReport, c: &Comparison) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "# betterMQ benchmark comparison\n");
    let _ = writeln!(
        s,
        "Baseline `{}` @ `{}` ({}) → candidate `{}` @ `{}` ({}).\n",
        a.meta.git_branch,
        a.meta.git_sha,
        a.meta.started_at,
        b.meta.git_branch,
        b.meta.git_sha,
        b.meta.started_at
    );
    let _ = writeln!(
        s,
        "**{} regression(s), {} improvement(s)**, {} metrics compared.\n",
        c.regressions,
        c.improvements,
        c.rows.len()
    );
    for cav in &c.caveats {
        let _ = writeln!(s, "> ⚠ {cav}");
    }
    if !c.caveats.is_empty() {
        s.push('\n');
    }
    for (title, v) in [
        ("Regressions", Verdict::Regressed),
        ("Improvements", Verdict::Improved),
        ("Changed (no good/bad direction)", Verdict::Changed),
    ] {
        let rows: Vec<&Row> = c.rows.iter().filter(|r| r.verdict == v).collect();
        if rows.is_empty() {
            continue;
        }
        let _ = writeln!(s, "## {title}\n\n| Scenario | Metric | Baseline | Candidate | Change |\n|---|---|---|---|---|");
        for r in rows {
            let change = if r.delta.is_finite() {
                format!("{:+.1}%", r.delta * 100.0)
            } else {
                "from 0".to_string()
            };
            let _ = writeln!(
                s,
                "| {} | {} | {} | {} | {}{} |",
                r.scenario,
                r.label,
                val(r.base, &r.unit),
                val(r.cand, &r.unit),
                change,
                if r.noisy { " (noisy)" } else { "" }
            );
        }
        s.push('\n');
    }
    let same = c.rows.iter().filter(|r| r.verdict == Verdict::Same).count();
    let _ = writeln!(
        s,
        "{same} metric(s) unchanged within the threshold and noise.\n"
    );
    if !c.only_in_base.is_empty() {
        let _ = writeln!(s, "Only in baseline: {}", c.only_in_base.join(", "));
    }
    if !c.only_in_cand.is_empty() {
        let _ = writeln!(s, "Only in candidate: {}", c.only_in_cand.join(", "));
    }
    s
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    fn m(v: f64, samples: &[f64], d: Direction) -> Metric {
        let mut x = Metric::new("id", "L", "ms", v, d, "latency");
        x.samples = samples.to_vec();
        x
    }

    #[test]
    fn direction_decides_good_or_bad() {
        let (_, v, _) = classify(
            &m(100.0, &[], Direction::LowerBetter),
            &m(150.0, &[], Direction::LowerBetter),
            0.1,
        );
        assert_eq!(v, Verdict::Regressed);
        let (_, v, _) = classify(
            &m(100.0, &[], Direction::HigherBetter),
            &m(150.0, &[], Direction::HigherBetter),
            0.1,
        );
        assert_eq!(v, Verdict::Improved);
        let (_, v, _) = classify(
            &m(100.0, &[], Direction::Info),
            &m(150.0, &[], Direction::Info),
            0.1,
        );
        assert_eq!(v, Verdict::Changed);
    }

    #[test]
    fn small_changes_and_noise_are_ignored() {
        let (_, v, _) = classify(
            &m(100.0, &[], Direction::LowerBetter),
            &m(105.0, &[], Direction::LowerBetter),
            0.1,
        );
        assert_eq!(v, Verdict::Same);
        // 20% change but trials spread by ~20% => within noise.
        let noisy = m(100.0, &[80.0, 100.0, 120.0], Direction::LowerBetter);
        let (_, v, flagged) = classify(
            &noisy,
            &m(120.0, &[100.0, 120.0, 140.0], Direction::LowerBetter),
            0.1,
        );
        assert_eq!(v, Verdict::Same);
        assert!(flagged);
    }

    fn report(status: Status, findings: Vec<Finding>, metric: f64) -> RunReport {
        RunReport {
            schema: SCHEMA_VERSION,
            meta: RunMeta::default(),
            env: EnvInfo::default(),
            calibration: None,
            scenarios: vec![ScenarioResult {
                id: "s".into(),
                title: "S".into(),
                description: String::new(),
                params: Default::default(),
                status,
                error: None,
                trials: Vec::new(),
                metrics: vec![Metric::new(
                    "m",
                    "M",
                    "ms",
                    metric,
                    Direction::LowerBetter,
                    "latency",
                )],
                findings,
            }],
        }
    }

    #[test]
    fn lost_messages_fail_the_gate_even_when_faster() {
        let base = report(Status::Pass, vec![], 100.0);
        let crit = Finding {
            severity: Severity::Critical,
            scenario: "s".into(),
            title: "Acknowledged messages were never delivered".into(),
            detail: "3 lost".into(),
        };
        let cand = report(Status::Fail, vec![crit], 50.0);
        let c = compare(&base, &cand, 0.1);
        assert_eq!(c.regressions, 0);
        assert_eq!(c.improvements, 1);
        assert!(c
            .hard_failures
            .iter()
            .any(|h| h.contains("never delivered")));
    }

    #[test]
    fn vanished_or_nan_metrics_are_hard_failures() {
        let base = report(Status::Pass, vec![], 100.0);
        let cand = report(Status::Pass, vec![], f64::NAN);
        assert!(!compare(&base, &cand, 0.1).hard_failures.is_empty());
        let mut gone = report(Status::Pass, vec![], 1.0);
        gone.scenarios[0].metrics.clear();
        assert!(!compare(&base, &gone, 0.1).hard_failures.is_empty());
    }

    #[test]
    fn zero_baselines_are_handled() {
        let (d, v, _) = classify(
            &m(0.0, &[], Direction::LowerBetter),
            &m(0.0, &[], Direction::LowerBetter),
            0.1,
        );
        assert_eq!(d, 0.0);
        assert_eq!(v, Verdict::Same);
        let (_, v, _) = classify(
            &m(0.0, &[], Direction::LowerBetter),
            &m(5.0, &[], Direction::LowerBetter),
            0.1,
        );
        assert_eq!(v, Verdict::Regressed);
    }
}
