//! Pure analysis over recorded data: SLO classification of load steps, memory
//! release, leak slopes, and the rule-based findings shown in the report.

use crate::model::*;
use crate::recorder::col;

/// Apply the SLO to a step in place and explain failures.
pub fn classify_step(step: &mut StepResult, slo: &Slo, need_delivery: bool) {
    let mut reasons = Vec::new();
    if step.offered_msg_s > 0.0 && step.achieved_msg_s < step.offered_msg_s * slo.min_achieved_ratio
    {
        reasons.push(format!(
            "throughput {:.0}/s is {:.0}% of the {:.0}/s offered",
            step.achieved_msg_s,
            100.0 * step.achieved_msg_s / step.offered_msg_s,
            step.offered_msg_s
        ));
    }
    if step.ack_p99_ms.is_nan() || step.ack_p99_ms > slo.ack_p99_ms {
        reasons.push(format!(
            "ACK p99 {} exceeds {} ms",
            fmt_ms(step.ack_p99_ms),
            slo.ack_p99_ms
        ));
    }
    if step.error_rate > slo.max_error_rate {
        reasons.push(format!(
            "error rate {:.2}% exceeds {:.2}%",
            step.error_rate * 100.0,
            slo.max_error_rate * 100.0
        ));
    }
    if need_delivery {
        if step.delivery_p99_ms.is_finite() && step.delivery_p99_ms > slo.delivery_p99_ms {
            reasons.push(format!(
                "delivery p99 {} exceeds {} ms",
                fmt_ms(step.delivery_p99_ms),
                slo.delivery_p99_ms
            ));
        }
        // Backlog growing faster than 2% of the offered rate means delivery can't keep up.
        if step.backlog_growth_per_s > 0.02 * step.offered_msg_s.max(1.0) {
            reasons.push(format!(
                "undelivered backlog grows {:.0}/s (delivery can't keep up)",
                step.backlog_growth_per_s
            ));
        }
    }
    step.passed = reasons.is_empty();
    step.fail_reasons = reasons;
}

pub fn fmt_ms(v: f64) -> String {
    if v.is_nan() {
        "n/a".into()
    } else if v >= 1000.0 {
        format!("{:.2} s", v / 1000.0)
    } else if v >= 10.0 {
        format!("{v:.0} ms")
    } else {
        format!("{v:.2} ms")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Knee {
    /// Highest passing step before the first failure (offered msg/s).
    pub sustained_offered: Option<f64>,
    pub sustained_achieved: Option<f64>,
    /// Index of the first failing step.
    pub first_fail: Option<usize>,
    /// Best achieved rate at any step, even failing ones: the raw ceiling.
    pub peak_achieved: f64,
}

/// Locate the sustained ceiling and the breakdown.
///
/// A failing step is only a *confirmed* failure when the next step also fails
/// (or it is the last one). An isolated failure followed by a pass is a
/// transient (a cold first step, a noisy second on a shared host), not the
/// limit; this mirrors the ramp, which stops after two consecutive failures.
pub fn find_knee(steps: &[StepResult]) -> Knee {
    let confirmed = |i: usize| !steps[i].passed && (i + 1 == steps.len() || !steps[i + 1].passed);
    let first_fail = (0..steps.len()).find(|i| confirmed(*i));
    let limit = first_fail.unwrap_or(steps.len());
    let sustained = steps[..limit].iter().rev().find(|s| s.passed);
    Knee {
        sustained_offered: sustained.map(|s| s.offered_msg_s),
        sustained_achieved: sustained.map(|s| s.achieved_msg_s),
        first_fail,
        peak_achieved: steps.iter().map(|s| s.achieved_msg_s).fold(0.0, f64::max),
    }
}

/// Ordinary least squares slope of `ys` against `xs`.
pub fn slope(xs: &[f64], ys: &[f64]) -> Option<f64> {
    let n = xs.len().min(ys.len());
    if n < 2 {
        return None;
    }
    let mx = xs.iter().take(n).sum::<f64>() / n as f64;
    let my = ys.iter().take(n).sum::<f64>() / n as f64;
    let (mut num, mut den) = (0.0, 0.0);
    for i in 0..n {
        num += (xs[i] - mx) * (ys[i] - my);
        den += (xs[i] - mx).powi(2);
    }
    (den > 0.0).then(|| num / den)
}

pub fn percentile(values: &[f64], q: f64) -> f64 {
    let mut v: Vec<f64> = values.iter().copied().filter(|x| x.is_finite()).collect();
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((v.len() - 1) as f64 * q).round() as usize;
    v[idx]
}

pub fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        f64::NAN
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

/// Memory/disk release across baseline -> load -> settle phases.
///
/// Needs an `Idle` phase before the load and a `Settle` phase after it. The
/// settled value is the median of the last `window_s` seconds of the final
/// settle phase, so a late GC pass counts but a transient dip doesn't.
pub fn memory_release(series: &Series, phases: &[Phase], window_s: f64) -> Option<MemoryRelease> {
    let idle = phases.iter().find(|p| p.kind == PhaseKind::Idle)?;
    let settle = phases.iter().rev().find(|p| p.kind == PhaseKind::Settle)?;
    let tail = |name: &str, p: &Phase| -> Vec<f64> {
        series.window(name, (p.end_s - window_s).max(p.start_s), p.end_s)
    };
    let med = |v: Vec<f64>| median(&v);
    let idle_mb = med(tail(col::RSS, idle));
    let settled_mb = med(tail(col::RSS, settle));
    let peak_mb = series
        .window(col::RSS, idle.end_s, f64::MAX)
        .into_iter()
        .fold(f64::NAN, f64::max);
    let post_drain_mb = series
        .window(col::RSS, settle.start_s, settle.start_s + 3.0)
        .first()
        .copied()
        .unwrap_or(f64::NAN);
    let disk_idle = med(tail(col::DATA, idle));
    let disk_settled = med(tail(col::DATA, settle));
    let disk_peak = series
        .window(col::DATA, idle.end_s, f64::MAX)
        .into_iter()
        .fold(f64::NAN, f64::max);
    if !idle_mb.is_finite() || !settled_mb.is_finite() || !peak_mb.is_finite() {
        return None;
    }
    Some(MemoryRelease {
        idle_start_mb: idle_mb,
        peak_mb,
        post_drain_mb,
        settled_mb,
        settled_anon_mb: med(tail(col::ANON, settle)),
        settled_file_mb: med(tail(col::FILE, settle)),
        retained_pct: retained_pct(idle_mb, peak_mb, settled_mb, 8.0),
        settle_secs: settle.end_s - settle.start_s,
        disk_idle_mb: disk_idle,
        disk_peak_mb: disk_peak,
        disk_settled_mb: disk_settled,
        disk_retained_pct: retained_pct(disk_idle, disk_peak, disk_settled, 16.0),
    })
}

/// Share of the load-induced growth that was still held at the end.
/// Growth under `min_growth` MB is too small to judge and reports 0.
pub fn retained_pct(idle: f64, peak: f64, settled: f64, min_growth: f64) -> f64 {
    let growth = peak - idle;
    if !growth.is_finite() || growth < min_growth {
        return 0.0;
    }
    (100.0 * (settled - idle) / growth).clamp(0.0, 200.0)
}

pub fn memory_grade(retained_pct: f64) -> &'static str {
    match retained_pct {
        p if p <= 15.0 => "excellent",
        p if p <= 35.0 => "good",
        p if p <= 65.0 => "fair",
        _ => "poor",
    }
}

pub struct FindingCtx<'a> {
    pub scenario: &'a str,
    pub trial: &'a TrialResult,
}

/// Rule-based observations for one trial.
pub fn trial_findings(ctx: &FindingCtx<'_>) -> Vec<Finding> {
    let t = ctx.trial;
    let mut out = Vec::new();
    let mut add = |severity, title: &str, detail: String| {
        out.push(Finding {
            severity,
            scenario: ctx.scenario.to_string(),
            title: title.to_string(),
            detail,
        })
    };
    if let Some(i) = &t.integrity {
        if i.accepted == 0 && t.series.values(col::OFFERED).iter().any(|rate| *rate > 0.0) {
            add(
                Severity::Critical,
                "No offered messages were acknowledged",
                "The run offered ingest work but received no successful acknowledgements; zero acknowledged loss is not a successful trial.".into(),
            );
        }
        if i.lost > 0 && i.loss_expected_zero {
            add(
                Severity::Critical,
                "Acknowledged messages were never delivered",
                format!(
                    "{} of {} acknowledged messages were lost (not delivered and not dead-lettered).",
                    i.lost, i.accepted
                ),
            );
        }
        if i.order_violations > 0 {
            if i.order_violations_after_retry >= i.order_violations {
                add(
                    Severity::Warn,
                    "FIFO lanes are not held while a message is being retried",
                    format!(
                        "{} messages were delivered after a later message for the same key because they had failed once and were retried. \
                         No reordering happened without a retry. If strict per-key order must survive failures, the lane should wait for the head message.",
                        i.order_violations
                    ),
                );
            } else {
                add(
                    Severity::Critical,
                    "FIFO order violated",
                    format!(
                        "{} messages were delivered out of order within a key ({} of them after a retry, the rest with no failure involved).",
                        i.order_violations, i.order_violations_after_retry
                    ),
                );
            }
        }
        if i.concurrency_violations > 0 {
            let worst: Vec<String> = i
                .concurrency
                .iter()
                .filter(|c| c.configured > 0 && c.observed_max > c.configured)
                .map(|c| {
                    format!(
                        "{} (cap {}, observed {})",
                        c.dest, c.configured, c.observed_max
                    )
                })
                .collect();
            add(
                Severity::Critical,
                "Queue parallelism exceeded",
                format!(
                    "Destinations saw more simultaneous deliveries than configured: {}.",
                    worst.join(", ")
                ),
            );
        }
        if i.duplicate_deliveries > 0 {
            let pct = 100.0 * i.duplicate_deliveries as f64 / i.accepted.max(1) as f64;
            add(
                Severity::Notice,
                "Duplicate deliveries",
                format!(
                    "{} messages ({pct:.3}%) were delivered more than once. Allowed by at-least-once semantics, but worth tracking.",
                    i.duplicate_deliveries
                ),
            );
        }
        if i.malformed > 0 {
            add(
                Severity::Warn,
                "Unrecognised deliveries reached the sink",
                format!(
                    "{} requests could not be matched to a sent message.",
                    i.malformed
                ),
            );
        }
    }
    if let Some(error) = &t.shutdown_error {
        add(
            Severity::Critical,
            "Graceful shutdown failed",
            error.clone(),
        );
    }
    if t.broker_log_errors > 0 {
        add(
            Severity::Warn,
            "Broker logged errors",
            format!(
                "{} ERROR and {} WARN lines in the broker log during this trial.",
                t.broker_log_errors, t.broker_log_warnings
            ),
        );
    }
    if let Some(m) = &t.memory {
        let grade = memory_grade(m.retained_pct);
        if grade == "poor" || grade == "fair" {
            let sev = if grade == "poor" {
                Severity::Warn
            } else {
                Severity::Notice
            };
            add(
                sev,
                "Memory is not fully released after load",
                format!(
                    "RSS rose from {:.0} MB to a {:.0} MB peak and was still {:.0} MB ({:.0}% of the growth) after {:.0}s idle \
                     ({:.0} MB anonymous, {:.0} MB file-backed). Anonymous memory that is retained usually points at allocator \
                     fragmentation or caches that never shrink.",
                    m.idle_start_mb, m.peak_mb, m.settled_mb, m.retained_pct, m.settle_secs, m.settled_anon_mb, m.settled_file_mb
                ),
            );
        }
        if m.disk_retained_pct > 50.0 && m.disk_peak_mb - m.disk_idle_mb > 64.0 {
            add(
                Severity::Notice,
                "Disk space is slow to be reclaimed",
                format!(
                    "Data directory grew from {:.0} MB to {:.0} MB and was still {:.0} MB ({:.0}% of the growth) after settling.",
                    m.disk_idle_mb, m.disk_peak_mb, m.disk_settled_mb, m.disk_retained_pct
                ),
            );
        }
    }
    // Was the whole host busy while this trial ran? Then every number in it
    // includes contention from other workloads.
    let pressure = mean(&t.series.values(col::PSI));
    let host_busy = mean(&t.series.values(col::HOST_CPU));
    if pressure > 10.0 || host_busy > 50.0 {
        add(
            Severity::Warn,
            "Host was CPU-contended during this trial",
            format!(
                "Whole-host CPU averaged {host_busy:.0}% busy with kernel CPU pressure {pressure:.0}%. \
                 Latency, throughput and CPU figures from this trial include interference from other workloads and are not a trustworthy baseline; re-run on a quiet machine (see --strict-env)."
            ),
        );
    }
    // Generator-limited measurement windows.
    for p in t.phases.iter().filter(|p| p.kind == PhaseKind::Measure) {
        let lag = t.series.window(col::GENLAG_P99, p.start_s, p.end_s);
        let worst = lag.iter().copied().fold(0.0, f64::max);
        let steady_lag = percentile(&lag, 0.5);
        // Lag with every request slot busy means the broker is slow to answer,
        // not that the generator is saturated.
        let all_inflight = t.series.values(col::CLIENT_INFLIGHT);
        let peak_inflight = all_inflight.iter().copied().fold(0.0, f64::max);
        let window_inflight = mean(&t.series.window(col::CLIENT_INFLIGHT, p.start_s, p.end_s));
        let slots_busy = peak_inflight > 0.0 && window_inflight >= 0.5 * peak_inflight;
        if steady_lag > 50.0 && !slots_busy {
            add(
                Severity::Warn,
                "Load generator fell behind during measurement",
                format!(
                    "In phase `{}` the generator's own scheduling lag had a median p99 of {:.0} ms (worst {:.0} ms). \
                     Latency is still measured from the intended send time, but the offered load may not have been delivered as planned.",
                    p.name, steady_lag, worst
                ),
            );
            break;
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    fn step(offered: f64, achieved: f64, p99: f64, err: f64) -> StepResult {
        StepResult {
            offered_msg_s: offered,
            achieved_msg_s: achieved,
            ack_p99_ms: p99,
            error_rate: err,
            ..Default::default()
        }
    }

    #[test]
    fn classifies_steps_against_slo() {
        let slo = Slo::default();
        let mut ok = step(1000.0, 999.0, 5.0, 0.0);
        classify_step(&mut ok, &slo, false);
        assert!(ok.passed);
        let mut slow = step(2000.0, 2000.0, 80.0, 0.0);
        classify_step(&mut slow, &slo, false);
        assert!(!slow.passed);
        assert!(slow.fail_reasons[0].contains("ACK p99"));
        let mut starved = step(4000.0, 2500.0, 10.0, 0.0);
        classify_step(&mut starved, &slo, false);
        assert!(starved
            .fail_reasons
            .iter()
            .any(|r| r.contains("throughput")));
    }

    #[test]
    fn delivery_backlog_fails_end_to_end_steps() {
        let slo = Slo::default();
        let mut s = step(1000.0, 1000.0, 5.0, 0.0);
        s.backlog_growth_per_s = 300.0;
        classify_step(&mut s, &slo, true);
        assert!(!s.passed);
        let mut t = step(1000.0, 1000.0, 5.0, 0.0);
        t.backlog_growth_per_s = 300.0;
        classify_step(&mut t, &slo, false);
        assert!(t.passed);
    }

    #[test]
    fn knee_stops_at_first_failure() {
        let slo = Slo::default();
        let mut steps = vec![
            step(100.0, 100.0, 2.0, 0.0),
            step(200.0, 200.0, 3.0, 0.0),
            step(400.0, 400.0, 90.0, 0.0),
            step(800.0, 800.0, 95.0, 0.0),
            step(1600.0, 1600.0, 4.0, 0.0), // would pass, but we already broke
        ];
        for s in &mut steps {
            classify_step(s, &slo, false);
        }
        let knee = find_knee(&steps);
        assert_eq!(knee.sustained_offered, Some(200.0));
        assert_eq!(knee.first_fail, Some(2));
        assert_eq!(knee.peak_achieved, 1600.0);
    }

    #[test]
    fn an_isolated_failure_is_a_transient_not_the_ceiling() {
        let slo = Slo::default();
        // Cold first step fails alone, then the broker is fine up to 800.
        let mut steps = vec![
            step(100.0, 100.0, 120.0, 0.0),
            step(200.0, 200.0, 3.0, 0.0),
            step(400.0, 400.0, 4.0, 0.0),
            step(800.0, 800.0, 5.0, 0.0),
            step(1600.0, 900.0, 80.0, 0.0),
            step(3200.0, 900.0, 200.0, 0.0),
        ];
        for s in &mut steps {
            classify_step(s, &slo, false);
        }
        let knee = find_knee(&steps);
        assert_eq!(knee.sustained_offered, Some(800.0));
        assert_eq!(knee.first_fail, Some(4));
    }

    #[test]
    fn slope_recovers_linear_growth() {
        let xs = [0.0, 1.0, 2.0, 3.0, 4.0];
        let ys = [10.0, 12.0, 14.0, 16.0, 18.0];
        assert!((slope(&xs, &ys).unwrap() - 2.0).abs() < 1e-9);
        assert!(slope(&[1.0], &[1.0]).is_none());
    }

    #[test]
    fn retained_pct_handles_small_growth() {
        assert_eq!(retained_pct(100.0, 104.0, 103.0, 8.0), 0.0);
        assert!((retained_pct(100.0, 300.0, 150.0, 8.0) - 25.0).abs() < 1e-9);
        assert_eq!(retained_pct(100.0, 300.0, 90.0, 8.0), 0.0);
    }

    #[test]
    fn memory_release_reads_phases() {
        let mut s = Series::default();
        for i in 0..40 {
            s.t.push(i as f64);
        }
        let rss: Vec<Option<f64>> = (0..40)
            .map(|i| {
                Some(match i {
                    0..=9 => 50.0,
                    10..=19 => 450.0,
                    _ => 150.0,
                })
            })
            .collect();
        s.cols.insert(col::RSS.into(), rss);
        let phases = vec![
            Phase {
                name: "baseline".into(),
                kind: PhaseKind::Idle,
                start_s: 0.0,
                end_s: 10.0,
            },
            Phase {
                name: "load".into(),
                kind: PhaseKind::Measure,
                start_s: 10.0,
                end_s: 20.0,
            },
            Phase {
                name: "settle".into(),
                kind: PhaseKind::Settle,
                start_s: 20.0,
                end_s: 40.0,
            },
        ];
        let m = memory_release(&s, &phases, 10.0).unwrap();
        assert_eq!(m.idle_start_mb, 50.0);
        assert_eq!(m.peak_mb, 450.0);
        assert_eq!(m.settled_mb, 150.0);
        assert!((m.retained_pct - 25.0).abs() < 1e-9);
        assert_eq!(memory_grade(m.retained_pct), "good");
    }

    #[test]
    fn findings_flag_loss_and_order() {
        let trial = TrialResult {
            integrity: Some(Integrity {
                accepted: 100,
                lost: 3,
                order_violations: 2,
                order_violations_after_retry: 0,
                loss_expected_zero: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let f = trial_findings(&FindingCtx {
            scenario: "x",
            trial: &trial,
        });
        assert!(f
            .iter()
            .any(|f| f.severity == Severity::Critical && f.title.contains("never delivered")));
        assert!(f.iter().any(|f| f.title.contains("FIFO")));
    }

    #[test]
    fn zero_acknowledgements_are_not_a_clean_integrity_result() {
        let mut trial = TrialResult {
            integrity: Some(Integrity::default()),
            ..Default::default()
        };
        trial
            .series
            .cols
            .insert(col::OFFERED.into(), vec![Some(50.0)]);
        let findings = trial_findings(&FindingCtx {
            scenario: "x",
            trial: &trial,
        });
        assert!(findings
            .iter()
            .any(|finding| finding.severity == Severity::Critical
                && finding.title.contains("acknowledged")));
        trial.series.cols.clear();
        assert!(trial_findings(&FindingCtx {
            scenario: "idle",
            trial: &trial
        })
        .is_empty());
    }

    #[test]
    fn parallelism_and_shutdown_failures_are_critical() {
        let trial = TrialResult {
            integrity: Some(Integrity {
                concurrency_violations: 1,
                ..Default::default()
            }),
            shutdown_error: Some("broker required SIGKILL".into()),
            ..Default::default()
        };
        let findings = trial_findings(&FindingCtx {
            scenario: "x",
            trial: &trial,
        });
        assert_eq!(
            findings
                .iter()
                .filter(|finding| finding.severity == Severity::Critical)
                .count(),
            2
        );
    }
}
