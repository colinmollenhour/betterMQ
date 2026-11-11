//! GitHub-flavoured Markdown report (tables only; charts live in the HTML).

use super::fmt;
use crate::analysis;
use crate::model::*;
use crate::recorder::col;
use std::fmt::Write as _;

fn esc(s: &str) -> String {
    s.replace('|', "\\|").replace('\n', " ")
}

fn md_table(head: &[&str], rows: &[Vec<String>]) -> String {
    let mut s = format!(
        "| {} |\n|{}|\n",
        head.join(" | "),
        head.iter().map(|_| "---").collect::<Vec<_>>().join("|")
    );
    for r in rows {
        let _ = writeln!(
            s,
            "| {} |",
            r.iter().map(|c| esc(c)).collect::<Vec<_>>().join(" | ")
        );
    }
    s
}

/// Unicode sparkline of a column, scaled between its own min and max.
pub fn sparkline(series: &Series, name: &str, width: usize) -> String {
    const BARS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    let v: Vec<f64> = series.values(name);
    if v.is_empty() {
        return String::new();
    }
    let bucket = v.len().div_ceil(width).max(1);
    let pts: Vec<f64> = v
        .chunks(bucket)
        .map(|c| c.iter().sum::<f64>() / c.len() as f64)
        .collect();
    let (lo, hi) = pts
        .iter()
        .fold((f64::MAX, f64::MIN), |a, x| (a.0.min(*x), a.1.max(*x)));
    pts.iter()
        .map(|x| {
            if hi - lo < 1e-9 {
                BARS[0]
            } else {
                BARS[(((x - lo) / (hi - lo)) * 7.0).round() as usize]
            }
        })
        .collect()
}

pub fn render(r: &RunReport) -> String {
    let m = &r.meta;
    let e = &r.env;
    let mut s = String::new();
    let _ = writeln!(s, "# betterMQ performance report\n");
    let _ = writeln!(
        s,
        "{} · profile **{}** · `{}` @ `{}`{} · seed {} · {} trial(s) per scenario\n",
        m.started_at,
        m.profile,
        m.git_branch,
        m.git_sha,
        if m.git_dirty { " (dirty)" } else { "" },
        m.seed,
        m.repeats
    );
    let _ = writeln!(s, "## Environment\n");
    let mut rows = vec![
        vec![
            "Machine".into(),
            format!(
                "{} · {} logical CPUs · {} MB RAM",
                e.cpu_model, e.logical_cpus, e.mem_total_mb
            ),
        ],
        vec![
            "OS".into(),
            format!("{} {} · governor `{}`", e.os, e.kernel, e.governor),
        ],
        vec![
            "Data dir".into(),
            format!("{} on {}", m.data_root, e.data_fs_type),
        ],
        vec![
            "Pinning".into(),
            format!("broker {} · harness {}", m.broker_cpus, m.bench_cpus),
        ],
        vec![
            "Broker".into(),
            format!(
                "v{} sha256 {}",
                m.broker_version,
                m.broker_sha256.chars().take(12).collect::<String>()
            ),
        ],
        vec![
            "Settings".into(),
            format!("fsync={} RUST_LOG={}", m.fsync, m.rust_log),
        ],
        vec![
            "SLO".into(),
            format!(
                "ACK p99 ≤ {} ms, errors ≤ {}%, achieved ≥ {}% of offered",
                m.slo.ack_p99_ms,
                m.slo.max_error_rate * 100.0,
                m.slo.min_achieved_ratio * 100.0
            ),
        ],
    ];
    if let Some(c) = &r.calibration {
        rows.push(vec![
            "Harness ceiling".into(),
            format!("≈ {} msg/s (null server)", fmt::num(c.null_server_msgs_s)),
        ]);
    }
    s.push_str(&md_table(&["", ""], &rows));
    for w in &e.warnings {
        let _ = writeln!(s, "\n> ⚠ {w}");
    }

    let _ = writeln!(s, "\n## Scorecard\n");
    let mut rows = Vec::new();
    for sc in &r.scenarios {
        for mt in sc.metrics.iter().filter(|m| m.headline) {
            rows.push(vec![
                sc.id.clone(),
                mt.label.clone(),
                format!("**{}**", fmt::value(mt)),
                fmt::spread(mt),
            ]);
        }
    }
    s.push_str(&md_table(
        &["Scenario", "Metric", "Value", "Trial spread"],
        &rows,
    ));

    let _ = writeln!(s, "\n## Findings\n");
    let mut all: Vec<&Finding> = r.scenarios.iter().flat_map(|s| s.findings.iter()).collect();
    all.sort_by_key(|f| std::cmp::Reverse(f.severity));
    if all.is_empty() {
        let _ = writeln!(s, "None: every correctness and resource check passed.");
    }
    for f in all {
        let _ = writeln!(
            s,
            "- **{}** [{}] in `{}`: {}",
            f.title,
            fmt::severity_word(f.severity),
            f.scenario,
            f.detail
        );
    }

    for sc in &r.scenarios {
        let _ = writeln!(
            s,
            "\n## {} `{}` — {}\n",
            sc.title,
            sc.id,
            fmt::status_word(sc.status)
        );
        let _ = writeln!(s, "{}\n", sc.description);
        if let Some(err) = &sc.error {
            let _ = writeln!(s, "> **Trial error:** {err}\n");
        }
        if !sc.params.is_empty() {
            let rows: Vec<Vec<String>> = sc
                .params
                .iter()
                .map(|(k, v)| vec![k.clone(), v.clone()])
                .collect();
            s.push_str(&md_table(&["Workload", ""], &rows));
            s.push('\n');
        }
        let multi = sc.trials.len() > 1;
        let rows: Vec<Vec<String>> = sc
            .metrics
            .iter()
            .map(|mt| {
                let mut row = vec![mt.label.clone(), fmt::value(mt)];
                if multi {
                    row.push(fmt::spread(mt));
                }
                row
            })
            .collect();
        let head: Vec<&str> = if multi {
            vec!["Metric", "Value", "Spread"]
        } else {
            vec!["Metric", "Value"]
        };
        s.push_str(&md_table(&head, &rows));
        let Some(t) = sc.trials.first() else { continue };
        if !t.steps.is_empty() {
            let _ = writeln!(s, "\n**Load steps**\n");
            let rows: Vec<Vec<String>> = t
                .steps
                .iter()
                .map(|st| {
                    vec![
                        fmt::num(st.offered_msg_s),
                        fmt::num(st.achieved_msg_s),
                        analysis::fmt_ms(st.ack_p50_ms),
                        analysis::fmt_ms(st.ack_p99_ms),
                        format!("{:.2}%", st.error_rate * 100.0),
                        format!("{:.2}", st.cpu_cores),
                        format!("{:.0}", st.rss_mb),
                        if st.passed {
                            "ok".into()
                        } else {
                            st.fail_reasons.join("; ")
                        },
                    ]
                })
                .collect();
            s.push_str(&md_table(
                &[
                    "Offered/s",
                    "Achieved/s",
                    "ACK p50",
                    "ACK p99",
                    "Errors",
                    "CPU",
                    "RSS MB",
                    "Verdict",
                ],
                &rows,
            ));
        }
        for tb in &t.tables {
            let _ = writeln!(s, "\n**{}**\n", tb.title);
            let heads: Vec<&str> = tb.columns.iter().map(String::as_str).collect();
            s.push_str(&md_table(&heads, &tb.rows));
        }
        if let Some(mr) = &t.memory {
            let _ = writeln!(
                s,
                "\n**Memory release:** {:.0} MB idle → {:.0} MB peak → {:.0} MB after {:.0}s idle ({:.0}% of growth still held, *{}*); data dir {:.0} → {:.0} → {:.0} MB.",
                mr.idle_start_mb, mr.peak_mb, mr.settled_mb, mr.settle_secs, mr.retained_pct,
                analysis::memory_grade(mr.retained_pct), mr.disk_idle_mb, mr.disk_peak_mb, mr.disk_settled_mb
            );
        }
        if let Some(i) = &t.integrity {
            let _ = writeln!(
                s,
                "\n**Correctness:** {} accepted, {} lost, {} duplicate deliveries, {} order violations, {} parallelism violations; broker log {} errors / {} warnings.",
                i.accepted, i.lost, i.duplicate_deliveries, i.order_violations, i.concurrency_violations,
                t.broker_log_errors, t.broker_log_warnings
            );
        }
        if !t.broker_log_top.is_empty() {
            let _ = writeln!(s, "\n**Most frequent broker warnings/errors**\n");
            let rows: Vec<Vec<String>> = t
                .broker_log_top
                .iter()
                .map(|(m, n)| vec![n.to_string(), format!("`{}`", m.replace('`', "'"))])
                .collect();
            s.push_str(&md_table(&["Count", "Message"], &rows));
        }
        let spark = |label: &str, c: &str| {
            let sp = sparkline(&t.series, c, 60);
            if sp.is_empty() {
                String::new()
            } else {
                format!("- {label}: `{sp}`\n")
            }
        };
        let lines = [
            spark("Broker CPU", col::CPU),
            spark("RSS", col::RSS),
            spark("Backlog", col::BACKLOG),
            spark("ACK p99", col::ACK_P99),
        ]
        .concat();
        if !lines.is_empty() {
            let _ = writeln!(s, "\n**Timeline** ({:.0}s)\n\n{lines}", t.duration_s);
        }
        for n in &t.notes {
            let _ = writeln!(s, "- _{n}_");
        }
    }
    let _ = writeln!(s, "\n---\nReproduce: `{}`", m.command_line);
    s
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn sparkline_scales() {
        let mut s = Series::default();
        s.t = (0..8).map(|i| i as f64).collect();
        s.cols
            .insert("x".into(), (0..8).map(|i| Some(i as f64)).collect());
        let sp = sparkline(&s, "x", 8);
        assert_eq!(sp.chars().count(), 8);
        assert_eq!(sp.chars().next(), Some('▁'));
        assert_eq!(sp.chars().last(), Some('█'));
        assert_eq!(sparkline(&s, "missing", 8), "");
    }

    #[test]
    fn table_cells_escape_pipes() {
        assert!(md_table(&["a"], &[vec!["x|y".into()]]).contains("x\\|y"));
    }
}
