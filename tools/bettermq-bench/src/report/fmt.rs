//! Number and text formatting shared by the Markdown and HTML renderers.

use crate::model::*;

pub fn num(v: f64) -> String {
    if !v.is_finite() {
        return "n/a".into();
    }
    let a = v.abs();
    if a >= 1_000.0 {
        thousands(v.round() as i64)
    } else if a >= 100.0 {
        format!("{v:.0}")
    } else if a >= 10.0 {
        format!("{v:.1}")
    } else if a >= 0.1 || a == 0.0 {
        format!("{v:.2}")
    } else {
        format!("{v:.3}")
    }
}

fn thousands(n: i64) -> String {
    let digits = n.abs().to_string();
    let groups: Vec<&str> = digits
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(|c| std::str::from_utf8(c).unwrap_or(""))
        .collect();
    let out = groups.join(",");
    if n < 0 {
        format!("-{out}")
    } else {
        out
    }
}

pub fn value(m: &Metric) -> String {
    if m.unit.is_empty() {
        num(m.value)
    } else if m.unit == "%" || m.unit == "x" {
        format!("{}{}", num(m.value), m.unit)
    } else {
        format!("{} {}", num(m.value), m.unit)
    }
}

pub fn spread(m: &Metric) -> String {
    match m.cv() {
        Some(cv) => format!("{:.1}%", cv * 100.0),
        None => "-".into(),
    }
}

pub fn status_word(s: Status) -> &'static str {
    match s {
        Status::Pass => "PASS",
        Status::Warn => "WARN",
        Status::Fail => "FAIL",
        Status::Error => "ERROR",
    }
}

pub fn severity_word(s: Severity) -> &'static str {
    match s {
        Severity::Info => "info",
        Severity::Notice => "notice",
        Severity::Warn => "warning",
        Severity::Critical => "critical",
    }
}

pub fn group_title(g: &str) -> &str {
    match g {
        "throughput" => "Throughput",
        "latency" => "Latency",
        "cpu" => "CPU",
        "memory" => "Memory and disk",
        "resources" => "Resources",
        "reliability" => "Reliability",
        "lifecycle" => "Lifecycle",
        other => other,
    }
}

pub const GROUP_ORDER: &[&str] = &[
    "throughput",
    "latency",
    "cpu",
    "memory",
    "resources",
    "reliability",
    "lifecycle",
];

/// Metrics of a scenario grouped in a stable, readable order.
pub fn grouped(metrics: &[Metric]) -> Vec<(String, Vec<&Metric>)> {
    let mut out: Vec<(String, Vec<&Metric>)> = Vec::new();
    for g in GROUP_ORDER {
        let items: Vec<&Metric> = metrics.iter().filter(|m| m.group == *g).collect();
        if !items.is_empty() {
            out.push((g.to_string(), items));
        }
    }
    let known: std::collections::HashSet<&str> = GROUP_ORDER.iter().copied().collect();
    let rest: Vec<&Metric> = metrics
        .iter()
        .filter(|m| !known.contains(m.group.as_str()))
        .collect();
    if !rest.is_empty() {
        out.push(("other".into(), rest));
    }
    out
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn formats_numbers() {
        assert_eq!(num(1234567.0), "1,234,567");
        assert_eq!(num(12.345), "12.3");
        assert_eq!(num(0.0421), "0.042");
        assert_eq!(num(f64::NAN), "n/a");
        assert_eq!(num(-2500.0), "-2,500");
    }

    #[test]
    fn groups_in_order() {
        let ms = vec![
            Metric::new("a", "A", "", 1.0, Direction::Info, "memory"),
            Metric::new("b", "B", "", 1.0, Direction::Info, "throughput"),
            Metric::new("c", "C", "", 1.0, Direction::Info, "weird"),
        ];
        let g = grouped(&ms);
        assert_eq!(g[0].0, "throughput");
        assert_eq!(g[1].0, "memory");
        assert_eq!(g[2].0, "other");
    }
}
