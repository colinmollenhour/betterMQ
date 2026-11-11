//! Helpers shared by scenarios.

use crate::analysis;
use crate::model::*;
use crate::recorder::{col, PhaseStats};

pub fn lower(id: &str, label: &str, unit: &str, value: f64, group: &str) -> Metric {
    Metric::new(id, label, unit, value, Direction::LowerBetter, group)
}

pub fn higher(id: &str, label: &str, unit: &str, value: f64, group: &str) -> Metric {
    Metric::new(id, label, unit, value, Direction::HigherBetter, group)
}

pub fn info(id: &str, label: &str, unit: &str, value: f64, group: &str) -> Metric {
    Metric::new(id, label, unit, value, Direction::Info, group)
}

/// Mean/peak of a series column inside a window.
pub fn col_stats(series: &Series, name: &str, start: f64, end: f64) -> (f64, f64) {
    let v = series.window(name, start, end);
    (
        analysis::mean(&v),
        v.iter().copied().fold(f64::NAN, f64::max),
    )
}

pub fn step_from_phase(
    index: usize,
    label: &str,
    offered: f64,
    ps: &PhaseStats,
    series: &Series,
    slo: &Slo,
    need_delivery: bool,
) -> StepResult {
    let gen_lag = series.window(col::GENLAG_P99, ps.start_s, ps.end_s);
    let mut step = StepResult {
        index,
        label: label.to_string(),
        offered_msg_s: offered,
        achieved_msg_s: ps.acked_rate(),
        duration_s: ps.dur_s,
        ack_p50_ms: ps.ack_ms(0.50),
        ack_p99_ms: ps.ack_ms(0.99),
        ack_p999_ms: ps.ack_ms(0.999),
        ack_max_ms: ps.ack_max_ms(),
        error_rate: ps.error_rate(),
        cpu_cores: ps.cpu_cores,
        rss_mb: ps.rss_mb_end,
        gen_lag_p99_ms: analysis::percentile(&gen_lag, 0.5),
        backlog_growth_per_s: ps.backlog_growth_per_s(),
        delivered_msg_s: ps.delivered_rate(),
        delivery_p99_ms: ps.delivery_ms(0.99),
        start_s: ps.start_s,
        end_s: ps.end_s,
        ..Default::default()
    };
    analysis::classify_step(&mut step, slo, need_delivery);
    step
}

pub fn fmt_rate(v: f64) -> String {
    if v >= 1_000_000.0 {
        format!("{:.2}M", v / 1e6)
    } else if v >= 10_000.0 {
        format!("{:.1}k", v / 1e3)
    } else if v >= 1_000.0 {
        format!("{:.2}k", v / 1e3)
    } else {
        format!("{v:.0}")
    }
}

/// Average broker CPU (cores) by thread group inside a window, largest first.
pub fn thread_cpu_table(series: &Series, title: &str, start: f64, end: f64) -> Table {
    let mut rows: Vec<(String, f64)> = series
        .cols
        .keys()
        .filter_map(|k| {
            k.strip_prefix(col::THREAD_PREFIX)
                .map(|n| (n.to_string(), k.clone()))
        })
        .map(|(name, key)| (name, analysis::mean(&series.window(&key, start, end))))
        .filter(|(_, v)| v.is_finite())
        .collect();
    rows.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let total: f64 = rows.iter().map(|r| r.1).sum();
    Table {
        title: title.to_string(),
        columns: vec!["Thread group".into(), "CPU (cores)".into(), "Share".into()],
        rows: rows
            .into_iter()
            .filter(|(_, v)| *v >= 0.005)
            .take(10)
            .map(|(n, v)| {
                vec![
                    n,
                    format!("{v:.2}"),
                    format!("{:.0}%", 100.0 * v / total.max(1e-9)),
                ]
            })
            .collect(),
    }
}
