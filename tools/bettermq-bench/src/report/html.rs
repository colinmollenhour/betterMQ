//! Self-contained HTML report (no scripts, no external assets).

use super::fmt;
use super::svg::{self, esc, Agg, Band, Chart, Line};
use crate::analysis;
use crate::model::*;
use crate::recorder::col;
use std::fmt::Write as _;

const CSS: &str = r#"
:root{color-scheme:light dark;--bg:#f6f6f4;--surface:#fcfcfb;--text:#0b0b0b;--muted:#52514e;--line:#dddcd7;--grid:#ebeae6;--axis:#b9b8b2;
--s1:#2a78d6;--s2:#eb6834;--s3:#1baf7a;--s4:#eda100;--hover:rgba(42,120,214,.08);
--band-idle:rgba(120,120,110,.07);--band-measure:rgba(42,120,214,.05);--band-fault:rgba(235,104,52,.14);--band-drain:rgba(27,175,122,.10);--band-settle:rgba(120,120,110,.12);
--good:#0f7b4a;--warn:#9a6a00;--bad:#b3261e;--crit:#8a1c16}
@media (prefers-color-scheme:dark){:root{--bg:#121211;--surface:#1a1a19;--text:#f0efec;--muted:#c3c2b7;--line:#34332f;--grid:#2a2926;--axis:#55544e;
--s1:#3987e5;--s2:#d95926;--s3:#199e70;--s4:#c98500;--hover:rgba(57,135,229,.15);
--band-idle:rgba(200,200,190,.06);--band-measure:rgba(57,135,229,.07);--band-fault:rgba(217,89,38,.20);--band-drain:rgba(25,158,112,.14);--band-settle:rgba(200,200,190,.10);
--good:#4cc38a;--warn:#e0b04a;--bad:#ff8a80;--crit:#ff6b60}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
main{max-width:1180px;margin:0 auto;padding:24px 20px 80px}
h1{font-size:26px;margin:0 0 4px}h2{font-size:20px;margin:36px 0 10px;padding-top:8px;border-top:1px solid var(--line);scroll-margin-top:64px}
h3{font-size:15px;margin:22px 0 8px}p{margin:6px 0}
.sub{color:var(--muted)}
nav{position:sticky;top:0;background:var(--bg);padding:8px 0;border-bottom:1px solid var(--line);z-index:5;font-size:13px;display:flex;flex-wrap:wrap;gap:4px 14px}
nav a{color:var(--muted);text-decoration:none}nav a:hover{color:var(--text)}
table{border-collapse:collapse;width:100%;background:var(--surface);border:1px solid var(--line);border-radius:8px;overflow:hidden;font-size:13px;margin:6px 0 12px}
th,td{padding:5px 10px;text-align:left;border-bottom:1px solid var(--line);vertical-align:top}
th{font-weight:600;color:var(--muted);background:var(--bg);font-size:12px}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
tr:last-child td{border-bottom:0}
.tiles{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:10px;margin:12px 0}
.tile{background:var(--surface);border:1px solid var(--line);border-radius:8px;padding:10px 12px}
.tile .v{font-size:24px;font-weight:650;font-variant-numeric:tabular-nums;line-height:1.2}
.tile .l{color:var(--muted);font-size:12px}.tile .s{color:var(--muted);font-size:11px}
.chip{display:inline-block;padding:1px 8px;border-radius:10px;font-size:11px;font-weight:700;border:1px solid currentColor}
.PASS{color:var(--good)}.WARN{color:var(--warn)}.FAIL,.ERROR{color:var(--bad)}
.sev-critical{color:var(--crit)}.sev-warning{color:var(--warn)}.sev-notice{color:var(--muted)}.sev-info{color:var(--muted)}
.finding{background:var(--surface);border:1px solid var(--line);border-left:4px solid currentColor;border-radius:6px;padding:8px 12px;margin:8px 0}
.finding b{color:var(--text)}.finding .d{color:var(--text);font-size:13px}.finding .where{color:var(--muted);font-size:12px}
.grid2{display:grid;grid-template-columns:repeat(auto-fill,minmax(520px,1fr));gap:12px;margin:10px 0}
.warnbox{background:var(--surface);border:1px solid var(--warn);color:var(--text);border-radius:8px;padding:8px 12px;margin:8px 0;font-size:13px}
code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
pre{background:var(--surface);border:1px solid var(--line);border-radius:8px;padding:10px;overflow:auto}
.ok{color:var(--good)}.bad{color:var(--bad)}.muted{color:var(--muted)}
.small{font-size:12px}
details{margin:8px 0}summary{cursor:pointer;color:var(--muted)}
.legendkey{display:flex;flex-wrap:wrap;gap:6px 14px;font-size:12px;color:var(--muted);margin:4px 0 10px}
.legendkey i{display:inline-block;width:14px;height:10px;border-radius:2px;margin-right:5px;vertical-align:middle;border:1px solid var(--line)}
"#;

fn table(head: &[&str], num_cols: &[usize], rows: &[Vec<String>]) -> String {
    let mut s = String::from("<table><thead><tr>");
    for (i, h) in head.iter().enumerate() {
        let _ = write!(
            s,
            "<th class=\"{}\">{}</th>",
            if num_cols.contains(&i) { "n" } else { "" },
            esc(h)
        );
    }
    s.push_str("</tr></thead><tbody>");
    for r in rows {
        s.push_str("<tr>");
        for (i, c) in r.iter().enumerate() {
            let _ = write!(
                s,
                "<td class=\"{}\">{}</td>",
                if num_cols.contains(&i) { "n" } else { "" },
                c
            );
        }
        s.push_str("</tr>");
    }
    s.push_str("</tbody></table>");
    s
}

fn chip(status: Status) -> String {
    let w = fmt::status_word(status);
    format!("<span class=\"chip {w}\">{w}</span>")
}

fn line(name: &str, slot: u8, t: &TrialResult, colname: &str, agg: Agg, dashed: bool) -> Line {
    let vals = t.series.col(colname);
    Line {
        name: name.into(),
        slot,
        pts: t
            .series
            .t
            .iter()
            .enumerate()
            .map(|(i, x)| (*x, vals.and_then(|c| c.get(i).copied().flatten())))
            .collect(),
        agg,
        dashed,
    }
}

fn bands(t: &TrialResult) -> Vec<Band> {
    t.phases
        .iter()
        .map(|p| Band {
            x0: p.start_s,
            x1: p.end_s,
            kind: p.kind,
        })
        .collect()
}

fn time_chart(title: &str, unit: &str, t: &TrialResult, lines: Vec<Line>, log: bool) -> String {
    let mut c = Chart::new(title, unit);
    c.lines = lines;
    c.bands = bands(t);
    c.y_log = log;
    svg::render(&c)
}

fn ramp_charts(t: &TrialResult, slo_ack: f64) -> String {
    if t.steps.is_empty() {
        return String::new();
    }
    let pts = |f: &dyn Fn(&StepResult) -> f64| -> Vec<(f64, Option<f64>)> {
        t.steps
            .iter()
            .map(|s| (s.offered_msg_s, Some(f(s))))
            .collect()
    };
    let mut out = String::new();
    let mut a = Chart::new("Achieved vs offered throughput", "msg/s");
    a.x_label = "offered msg/s".into();
    a.x_log = true;
    a.markers = true;
    a.lines.push(Line {
        name: "achieved".into(),
        slot: 1,
        pts: pts(&|s| s.achieved_msg_s),
        agg: Agg::Mean,
        dashed: false,
    });
    a.lines.push(Line {
        name: "offered".into(),
        slot: 3,
        pts: pts(&|s| s.offered_msg_s),
        agg: Agg::Mean,
        dashed: true,
    });
    if t.steps.iter().any(|s| s.delivered_msg_s > 0.0) {
        a.lines.push(Line {
            name: "delivered".into(),
            slot: 2,
            pts: pts(&|s| s.delivered_msg_s),
            agg: Agg::Mean,
            dashed: false,
        });
    }
    out.push_str(&svg::render(&a));
    let mut l = Chart::new("ACK latency vs offered load", "ms");
    l.x_label = "offered msg/s".into();
    l.x_log = true;
    l.y_log = true;
    l.markers = true;
    l.lines.push(Line {
        name: "p50".into(),
        slot: 3,
        pts: pts(&|s| s.ack_p50_ms),
        agg: Agg::Max,
        dashed: false,
    });
    l.lines.push(Line {
        name: "p99".into(),
        slot: 1,
        pts: pts(&|s| s.ack_p99_ms),
        agg: Agg::Max,
        dashed: false,
    });
    l.lines.push(Line {
        name: "p99.9".into(),
        slot: 2,
        pts: pts(&|s| s.ack_p999_ms),
        agg: Agg::Max,
        dashed: false,
    });
    l.hlines.push((slo_ack, format!("SLO p99 {slo_ack} ms")));
    out.push_str(&svg::render(&l));
    let mut r = Chart::new("Broker CPU vs offered load", "cores");
    r.x_label = "offered msg/s".into();
    r.x_log = true;
    r.markers = true;
    r.lines.push(Line {
        name: "broker CPU".into(),
        slot: 1,
        pts: pts(&|s| s.cpu_cores),
        agg: Agg::Mean,
        dashed: false,
    });
    out.push_str(&svg::render(&r));
    out
}

fn trial_charts(s: &ScenarioResult, t: &TrialResult, slo: &Slo) -> String {
    let mut out = String::new();
    if matches!(
        s.id.as_str(),
        "ingest-single" | "ingest-batch" | "ingest-large" | "e2e-ramp"
    ) {
        out.push_str(&ramp_charts(t, slo.ack_p99_ms));
    }
    out.push_str(&time_chart(
        "Throughput",
        "msg/s",
        t,
        vec![
            line("offered", 3, t, col::OFFERED, Agg::Mean, true),
            line("accepted (ACKed)", 1, t, col::ACKED, Agg::Mean, false),
            line("delivered", 2, t, col::DELIVERED, Agg::Mean, false),
        ],
        false,
    ));
    out.push_str(&time_chart(
        "Latency",
        "ms, log scale",
        t,
        vec![
            line("ACK p99", 1, t, col::ACK_P99, Agg::Max, false),
            line("delivery p99", 2, t, col::DELIV_P99, Agg::Max, false),
            line("ACK p50", 3, t, col::ACK_P50, Agg::Max, false),
            line(
                "cron fire lateness p99",
                4,
                t,
                col::CRON_LATE_P99,
                Agg::Max,
                false,
            ),
        ],
        true,
    ));
    out.push_str(&time_chart(
        "Broker CPU",
        "cores (1.0 = one core)",
        t,
        vec![
            line("broker", 1, t, col::CPU, Agg::Mean, false),
            line(
                "load generator + sink (harness)",
                2,
                t,
                col::HARNESS_CPU,
                Agg::Mean,
                true,
            ),
        ],
        false,
    ));
    out.push_str(&time_chart(
        "Memory and disk footprint",
        "MB",
        t,
        vec![
            line("RSS", 1, t, col::RSS, Agg::Mean, false),
            line("anonymous (heap)", 2, t, col::ANON, Agg::Mean, false),
            line("data directory", 3, t, col::DATA, Agg::Mean, false),
        ],
        false,
    ));
    out.push_str(&time_chart(
        "Undelivered backlog",
        "messages",
        t,
        vec![
            line(
                "accepted but not yet delivered",
                1,
                t,
                col::BACKLOG,
                Agg::Max,
                false,
            ),
            line(
                "in flight at destinations",
                2,
                t,
                col::BM_INFLIGHT,
                Agg::Max,
                false,
            ),
            line(
                "destination hosts circuit-broken",
                3,
                t,
                col::BM_BLOCKED,
                Agg::Max,
                false,
            ),
        ],
        false,
    ));
    out.push_str(&time_chart(
        "Disk I/O by the broker",
        "MB/s",
        t,
        vec![
            line("written", 1, t, col::DISK_W, Agg::Mean, false),
            line("read", 2, t, col::DISK_R, Agg::Mean, false),
        ],
        false,
    ));
    out.push_str(&time_chart(
        "WAL fsync p99",
        "ms",
        t,
        vec![line(
            "fsync p99 (broker metric)",
            1,
            t,
            col::BM_FSYNC_P99,
            Agg::Max,
            false,
        )],
        false,
    ));
    out.push_str(&time_chart(
        "Threads and file descriptors",
        "count",
        t,
        vec![
            line("threads", 1, t, col::THREADS, Agg::Max, false),
            line("open fds", 2, t, col::FDS, Agg::Max, false),
        ],
        false,
    ));
    out
}

fn tile(label: &str, value: String, sub: &str) -> String {
    format!(
        "<div class=\"tile\"><div class=\"l\">{}</div><div class=\"v\">{}</div><div class=\"s\">{}</div></div>",
        esc(label),
        esc(&value),
        esc(sub)
    )
}

fn find<'a>(r: &'a RunReport, scenario: &str, id: &str) -> Option<&'a Metric> {
    r.scenarios
        .iter()
        .find(|s| s.id == scenario)?
        .metrics
        .iter()
        .find(|m| m.id == id)
}

fn headline_tiles(r: &RunReport) -> String {
    let picks: &[(&str, &str, &str)] = &[
        (
            "ingest-single",
            "ramp.sustained_msgs_s",
            "Sustained ingest, single requests",
        ),
        (
            "ingest-batch",
            "ramp.sustained_msgs_s",
            "Sustained ingest, 100-message batches",
        ),
        (
            "e2e-ramp",
            "ramp.sustained_msgs_s",
            "Sustained end-to-end throughput",
        ),
        (
            "ingest-single",
            "ramp.ack_p99_half_ms",
            "ACK p99 at about half of ceiling",
        ),
        (
            "ingest-single",
            "ramp.cpu_s_per_million",
            "CPU per million messages",
        ),
        (
            "mixed-day",
            "mixed.delivery_p99_ms",
            "Delivery p99 on the mixed workload",
        ),
        (
            "mixed-day",
            "mixed.cpu_cores_mean",
            "Mean CPU on the mixed workload",
        ),
        ("idle", "idle.cpu_pct", "Idle CPU"),
        ("idle", "idle.rss_mb", "Idle memory (RSS)"),
        (
            "mixed-day",
            "mem.retained_pct",
            "Memory still held after load",
        ),
        (
            "memory-cycles",
            "cycles.rss_slope_mb_per_cycle",
            "Settled-memory growth per cycle",
        ),
        (
            "backlog-depth",
            "backlog.rss_bytes_per_msg",
            "Memory per queued message",
        ),
        (
            "cron-fleet",
            "cron.max_healthy_jobs",
            "Healthy schedule fleet size",
        ),
        (
            "burst-drain",
            "burst.recovery_s",
            "Recovery after a 5x burst",
        ),
        (
            "outage-error",
            "outage.recovery_s",
            "Catch-up after a destination outage",
        ),
        (
            "noisy-neighbor",
            "iso.degradation_x",
            "Neighbour-induced slowdown",
        ),
        (
            "ordered-lanes",
            "lanes.hot_lane_msgs_s",
            "Hot FIFO lane throughput",
        ),
        (
            "crash-recovery",
            "recovery.lost_messages",
            "Acknowledged messages lost",
        ),
    ];
    let mut s = String::from("<div class=\"tiles\">");
    for (sc, id, label) in picks {
        if let Some(m) = find(r, sc, id) {
            let sub = match m.cv() {
                Some(cv) => format!(
                    "{} · {} trials, spread {:.1}%",
                    sc,
                    m.samples.len(),
                    cv * 100.0
                ),
                None => sc.to_string(),
            };
            s.push_str(&tile(label, fmt::value(m), &sub));
        }
    }
    s.push_str("</div>");
    s
}

fn env_section(r: &RunReport) -> String {
    let e = &r.env;
    let m = &r.meta;
    let mut rows: Vec<Vec<String>> = vec![
        vec![
            "Machine".into(),
            esc(&format!(
                "{} · {} logical CPUs · {} MB RAM",
                e.cpu_model, e.logical_cpus, e.mem_total_mb
            )),
        ],
        vec!["OS".into(), esc(&format!("{} {}", e.os, e.kernel))],
        vec!["CPU governor".into(), esc(&e.governor)],
        vec![
            "Data directory".into(),
            esc(&format!(
                "{} on {}{}",
                m.data_root,
                e.data_fs_type,
                match e.data_device_rotational {
                    Some(true) => " (rotational)",
                    Some(false) => " (SSD/NVMe)",
                    None => "",
                }
            )),
        ],
        vec![
            "CPU pinning".into(),
            esc(&format!(
                "broker {} · harness {}",
                m.broker_cpus, m.bench_cpus
            )),
        ],
        vec![
            "Broker".into(),
            esc(&format!(
                "{} (v{}, {} MB, sha256 {})",
                m.broker_binary,
                m.broker_version,
                m.broker_size_bytes / 1_048_576,
                m.broker_sha256.chars().take(12).collect::<String>()
            )),
        ],
        vec![
            "Source".into(),
            esc(&format!(
                "{} @ {}{}",
                m.git_branch,
                m.git_sha,
                if m.git_dirty {
                    " (uncommitted changes)"
                } else {
                    ""
                }
            )),
        ],
        vec![
            "Broker settings".into(),
            esc(&format!(
                "BETTERMQ_FSYNC={} · RUST_LOG={} · {}",
                m.fsync, m.rust_log, m.auth
            )),
        ],
        vec![
            "Run".into(),
            esc(&format!(
                "profile {} · seed {} · {} trial(s) per scenario · {}",
                m.profile, m.seed, m.repeats, m.started_at
            )),
        ],
        vec![
            "SLO".into(),
            esc(&format!(
                "ACK p99 ≤ {} ms · errors ≤ {}% · achieved ≥ {}% of offered · delivery p99 ≤ {} ms",
                m.slo.ack_p99_ms,
                m.slo.max_error_rate * 100.0,
                m.slo.min_achieved_ratio * 100.0,
                m.slo.delivery_p99_ms
            )),
        ],
        vec![
            "Host noise before run".into(),
            esc(&format!(
                "load average {:.1} · broker CPUs {}% busy · CPU pressure {}",
                e.load_avg_start,
                e.preflight_busy_broker_cpus_pct
                    .map_or("n/a".into(), |v| format!("{v:.0}")),
                e.preflight_cpu_pressure_pct
                    .map_or("n/a".into(), |v| format!("{v:.0}%"))
            )),
        ],
    ];
    if let Some(c) = &r.calibration {
        rows.push(vec![
            "Harness ceiling".into(),
            esc(&format!(
                "≈ {} msg/s single-request against a null server; latency floor p50 {:.0} µs / p99 {:.0} µs",
                fmt::num(c.null_server_msgs_s), c.null_server_p50_us, c.null_server_p99_us
            )),
        ]);
    }
    let mut s = table(&["Item", "Value"], &[], &rows);
    for w in &e.warnings {
        let _ = write!(s, "<div class=\"warnbox\">⚠ {}</div>", esc(w));
    }
    s
}

fn scenario_section(r: &RunReport, sc: &ScenarioResult) -> String {
    let mut s = String::new();
    let _ = write!(
        s,
        "<h2 id=\"{id}\">{title} {chip}</h2><p class=\"sub\">{desc}</p>",
        id = esc(&sc.id),
        title = esc(&sc.title),
        chip = chip(sc.status),
        desc = esc(&sc.description)
    );
    if let Some(e) = &sc.error {
        let _ = write!(
            s,
            "<div class=\"warnbox\"><b>Trial error:</b> {}</div>",
            esc(e)
        );
    }
    if !sc.params.is_empty() {
        let rows: Vec<Vec<String>> = sc
            .params
            .iter()
            .map(|(k, v)| vec![esc(k), esc(v)])
            .collect();
        s.push_str("<h3>Workload</h3>");
        s.push_str(&table(&["Parameter", "Value"], &[], &rows));
    }
    for f in &sc.findings {
        let sev = fmt::severity_word(f.severity);
        let _ = write!(
            s,
            "<div class=\"finding sev-{sev}\"><b>{}</b> <span class=\"where\">[{sev}]</span><div class=\"d\">{}</div></div>",
            esc(&f.title),
            esc(&f.detail)
        );
    }
    let multi = sc.trials.len() > 1;
    for (group, items) in fmt::grouped(&sc.metrics) {
        let _ = write!(s, "<h3>{}</h3>", esc(fmt::group_title(&group)));
        let mut head = vec!["Metric", "Value"];
        if multi {
            head.push("Trial spread");
            head.push("Per trial");
        }
        let rows: Vec<Vec<String>> = items
            .iter()
            .map(|m| {
                let mut row = vec![esc(&m.label), format!("<b>{}</b>", esc(&fmt::value(m)))];
                if multi {
                    row.push(esc(&fmt::spread(m)));
                    row.push(esc(&m
                        .samples
                        .iter()
                        .map(|v| fmt::num(*v))
                        .collect::<Vec<_>>()
                        .join(" · ")));
                }
                row
            })
            .collect();
        s.push_str(&table(&head, &[1, 2], &rows));
    }
    let Some(t) = sc.trials.first() else {
        return s;
    };
    if !t.steps.is_empty() {
        s.push_str("<h3>Load steps</h3>");
        let rows: Vec<Vec<String>> = t
            .steps
            .iter()
            .map(|st| {
                let nb = |v: String| v.replace(' ', "\u{a0}");
                vec![
                    esc(&st.label).replace(' ', "\u{a0}"),
                    fmt::num(st.offered_msg_s),
                    fmt::num(st.achieved_msg_s),
                    nb(analysis::fmt_ms(st.ack_p50_ms)),
                    nb(analysis::fmt_ms(st.ack_p99_ms)),
                    nb(analysis::fmt_ms(st.ack_p999_ms)),
                    format!("{:.2}%", st.error_rate * 100.0),
                    format!("{:.2}", st.cpu_cores),
                    format!("{:.0}", st.rss_mb),
                    if st.passed {
                        "<span class=\"ok\">within SLO</span>".into()
                    } else {
                        format!(
                            "<span class=\"bad\">{}</span>",
                            esc(&st.fail_reasons.join("; "))
                        )
                    },
                ]
            })
            .collect();
        s.push_str(&table(
            &[
                "Step",
                "Offered/s",
                "Achieved/s",
                "ACK p50",
                "ACK p99",
                "ACK p99.9",
                "Errors",
                "CPU cores",
                "RSS MB",
                "Verdict",
            ],
            &[1, 2, 3, 4, 5, 6, 7, 8],
            &rows,
        ));
    }
    for tb in &t.tables {
        let _ = write!(s, "<h3>{}</h3>", esc(&tb.title));
        let rows: Vec<Vec<String>> = tb
            .rows
            .iter()
            .map(|r| r.iter().map(|c| esc(c)).collect())
            .collect();
        let heads: Vec<&str> = tb.columns.iter().map(String::as_str).collect();
        let nums: Vec<usize> = (1..heads.len()).collect();
        s.push_str(&table(&heads, &nums, &rows));
    }
    if let Some(m) = &t.memory {
        s.push_str("<h3>Memory and disk release</h3>");
        let grade = analysis::memory_grade(m.retained_pct);
        let rows = vec![
            vec![
                "RSS before load".into(),
                format!("{:.0} MB", m.idle_start_mb),
            ],
            vec!["RSS peak".into(), format!("{:.0} MB", m.peak_mb)],
            vec![
                "RSS right after drain".into(),
                format!("{:.0} MB", m.post_drain_mb),
            ],
            vec![
                format!("RSS after {:.0}s idle", m.settle_secs),
                format!(
                    "{:.0} MB ({:.0} MB anonymous, {:.0} MB file-backed)",
                    m.settled_mb, m.settled_anon_mb, m.settled_file_mb
                ),
            ],
            vec![
                "Share of the growth still held".into(),
                format!("<b>{:.0}%</b> — {grade}", m.retained_pct),
            ],
            vec![
                "Data directory before / peak / after".into(),
                format!(
                    "{:.0} / {:.0} / {:.0} MB ({:.0}% of growth still held)",
                    m.disk_idle_mb, m.disk_peak_mb, m.disk_settled_mb, m.disk_retained_pct
                ),
            ],
        ];
        s.push_str(&table(&["", "Value"], &[], &rows));
    }
    if let Some(i) = &t.integrity {
        s.push_str("<h3>Correctness</h3>");
        let ok = |good: bool| if good { "ok" } else { "bad" };
        let rows = vec![
            vec![
                "Messages accepted (ACKed)".into(),
                fmt::num(i.accepted as f64),
            ],
            vec![
                "Delivered at least once".into(),
                fmt::num(i.delivered_unique as f64),
            ],
            vec![
                "Lost (accepted, never delivered, not dead-lettered)".into(),
                format!(
                    "<span class=\"{}\">{}</span>",
                    ok(i.lost == 0 || !i.loss_expected_zero),
                    fmt::num(i.lost as f64)
                ),
            ],
            vec![
                "Duplicate deliveries".into(),
                fmt::num(i.duplicate_deliveries as f64),
            ],
            vec![
                "FIFO order violations".into(),
                format!(
                    "<span class=\"{}\">{}</span>",
                    ok(i.order_violations == 0),
                    fmt::num(i.order_violations as f64)
                ),
            ],
            vec![
                "Queues delivered above their parallelism cap".into(),
                format!(
                    "<span class=\"{}\">{}</span>",
                    ok(i.concurrency_violations == 0),
                    i.concurrency_violations
                ),
            ],
            vec![
                "Broker log".into(),
                format!(
                    "{} errors · {} warnings · {:.1} MB",
                    t.broker_log_errors,
                    t.broker_log_warnings,
                    t.broker_log_bytes as f64 / 1_048_576.0
                ),
            ],
        ];
        s.push_str(&table(&["Check", "Result"], &[], &rows));
    }
    if !t.broker_log_top.is_empty() {
        s.push_str("<h3>Most frequent broker warnings and errors</h3>");
        let rows: Vec<Vec<String>> = t
            .broker_log_top
            .iter()
            .map(|(msg, n)| vec![fmt::num(*n as f64), format!("<code>{}</code>", esc(msg))])
            .collect();
        s.push_str(&table(
            &["Count", "Message (ids and numbers normalised)"],
            &[0],
            &rows,
        ));
    }
    if !t.notes.is_empty() {
        s.push_str("<ul class=\"small muted\">");
        for n in &t.notes {
            let _ = write!(s, "<li>{}</li>", esc(n));
        }
        s.push_str("</ul>");
    }
    let charts = trial_charts(sc, t, &r.meta.slo);
    if !charts.is_empty() {
        let _ = write!(
            s,
            "<h3>Timelines{}</h3><div class=\"legendkey\"><span><i style=\"background:var(--band-idle)\"></i>idle / warm-up</span><span><i style=\"background:var(--band-measure)\"></i>measured</span><span><i style=\"background:var(--band-fault)\"></i>fault injected</span><span><i style=\"background:var(--band-drain)\"></i>drain</span><span><i style=\"background:var(--band-settle)\"></i>settle</span></div><div class=\"grid2\">{charts}</div>",
            if multi { " (trial 1)" } else { "" }
        );
    }
    s
}

pub fn render(r: &RunReport) -> String {
    let mut s = String::with_capacity(256 * 1024);
    let m = &r.meta;
    let _ = write!(
        s,
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>betterMQ performance report</title><style>{CSS}{}</style></head><body><main>",
        svg::CSS
    );
    let _ = write!(
        s,
        "<h1>betterMQ performance report</h1><p class=\"sub\">{} · profile <b>{}</b> · {} @ <code>{}</code>{} · seed {}</p>",
        esc(&m.started_at),
        esc(&m.profile),
        esc(&m.git_branch),
        esc(&m.git_sha),
        if m.git_dirty { " (dirty)" } else { "" },
        m.seed
    );
    s.push_str("<nav><a href=\"#summary\">Summary</a><a href=\"#findings\">Findings</a><a href=\"#env\">Environment</a>");
    for sc in &r.scenarios {
        let _ = write!(s, "<a href=\"#{}\">{}</a>", esc(&sc.id), esc(&sc.id));
    }
    s.push_str("<a href=\"#method\">Method</a></nav>");

    let _ = write!(s, "<h2 id=\"summary\">Summary</h2>");
    let mut counts = [0usize; 4];
    for sc in &r.scenarios {
        counts[match sc.status {
            Status::Pass => 0,
            Status::Warn => 1,
            Status::Fail => 2,
            Status::Error => 3,
        }] += 1;
    }
    let _ = write!(
        s,
        "<p>{} scenarios: <span class=\"PASS\"><b>{}</b> pass</span> · <span class=\"WARN\"><b>{}</b> warn</span> · <span class=\"FAIL\"><b>{}</b> fail</span> · <span class=\"ERROR\"><b>{}</b> error</span>. Status reflects correctness and reliability checks (loss, ordering, errors, memory release), not whether performance is “good”: performance is judged against a baseline with <code>bettermq-bench compare</code>.</p>",
        r.scenarios.len(), counts[0], counts[1], counts[2], counts[3]
    );
    s.push_str(&headline_tiles(r));
    for w in &r.env.warnings {
        let _ = write!(s, "<div class=\"warnbox\">⚠ {}</div>", esc(w));
    }

    // Scorecard table: all headline metrics
    let mut rows: Vec<Vec<String>> = Vec::new();
    for sc in &r.scenarios {
        for mt in sc.metrics.iter().filter(|m| m.headline) {
            rows.push(vec![
                format!("<a href=\"#{}\">{}</a>", esc(&sc.id), esc(&sc.id)),
                esc(&mt.label),
                format!("<b>{}</b>", esc(&fmt::value(mt))),
                esc(&fmt::spread(mt)),
            ]);
        }
    }
    s.push_str("<h3>Scorecard</h3>");
    s.push_str(&table(
        &["Scenario", "Metric", "Value", "Trial spread"],
        &[2, 3],
        &rows,
    ));

    s.push_str("<h2 id=\"findings\">Findings</h2>");
    let mut all: Vec<&Finding> = r.scenarios.iter().flat_map(|s| s.findings.iter()).collect();
    all.sort_by_key(|f| std::cmp::Reverse(f.severity));
    if all.is_empty() {
        s.push_str(
            "<p class=\"sub\">No findings: every correctness and resource check passed.</p>",
        );
    }
    for f in all {
        let sev = fmt::severity_word(f.severity);
        let _ = write!(
            s,
            "<div class=\"finding sev-{sev}\"><b>{}</b> <span class=\"where\">[{sev}] in <a href=\"#{}\">{}</a></span><div class=\"d\">{}</div></div>",
            esc(&f.title),
            esc(&f.scenario),
            esc(&f.scenario),
            esc(&f.detail)
        );
    }

    s.push_str("<h2 id=\"env\">Environment</h2>");
    s.push_str(&env_section(r));
    for sc in &r.scenarios {
        s.push_str(&scenario_section(r, sc));
    }

    s.push_str("<h2 id=\"method\">Method</h2><ul>");
    for line in [
        "<b>Open-loop load.</b> Messages have an intended send time from a seeded arrival plan. Latency is measured from that time, so queueing in an overloaded system is counted (no coordinated omission). Generator lag is tracked separately so a saturated client is not blamed on the broker.",
        "<b>Isolation.</b> Every trial starts a fresh broker on a fresh data directory. The broker and the harness (load generator + destination sink) are pinned to separate CPU sets when the machine is large enough.",
        "<b>Repeatability.</b> Workloads are generated from a seed with a fixed PRNG (identical offered load for the same seed). Trials are repeated and medians reported with their spread; use a quiet machine and a fixed CPU governor for baselines.",
        "<b>Correctness while loaded.</b> Every payload carries a stream/sequence header; the sink checks loss, duplicates, per-key FIFO order, and observed parallelism, and the broker is checked for errors in its log.",
        "<b>Resources.</b> Broker CPU, RSS (anonymous vs file-backed), threads, file descriptors, disk I/O and data-directory size are sampled from <code>/proc</code> once a second, joined with the broker's own Prometheus metrics.",
        "<b>Memory release.</b> RSS and data-directory size are recorded before load, at peak, right after the drain and after an idle settle period; the report shows how much of the growth was given back.",
        "<b>Ramps.</b> Offered load rises geometrically; a step passes if ACK p99, error rate, and achieved/offered stay within the SLO. The sustained ceiling is the last passing step before the first failure.",
    ] {
        let _ = write!(s, "<li>{line}</li>");
    }
    s.push_str("</ul>");
    let _ = write!(s, "<h3>Reproduce</h3><pre>{}</pre>", esc(&m.command_line));
    s.push_str("</main></body></html>");
    s
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    fn tiny_report() -> RunReport {
        let mut series = Series::default();
        series.t = (0..30).map(|i| i as f64).collect();
        series.cols.insert(
            col::RSS.into(),
            (0..30).map(|i| Some(50.0 + i as f64)).collect(),
        );
        series.cols.insert(
            col::ACKED.into(),
            (0..30).map(|i| Some(100.0 + i as f64)).collect(),
        );
        let trial = TrialResult {
            series,
            phases: vec![Phase {
                name: "x".into(),
                kind: PhaseKind::Measure,
                start_s: 5.0,
                end_s: 20.0,
            }],
            metrics: vec![Metric::new(
                "m",
                "A <metric>",
                "ms",
                3.5,
                Direction::LowerBetter,
                "latency",
            )
            .headline()],
            ..Default::default()
        };
        RunReport {
            schema: SCHEMA_VERSION,
            meta: RunMeta::default(),
            env: EnvInfo {
                warnings: vec!["noisy <host>".into()],
                ..Default::default()
            },
            calibration: None,
            scenarios: vec![ScenarioResult {
                id: "demo".into(),
                title: "Demo".into(),
                description: "d".into(),
                params: Default::default(),
                status: Status::Warn,
                error: None,
                metrics: aggregate_metrics(std::slice::from_ref(&trial)),
                trials: vec![trial],
                findings: vec![Finding {
                    severity: Severity::Warn,
                    scenario: "demo".into(),
                    title: "T".into(),
                    detail: "D".into(),
                }],
            }],
        }
    }

    #[test]
    fn renders_complete_escaped_document() {
        let html = render(&tiny_report());
        assert!(html.starts_with("<!doctype html>"));
        assert!(html.ends_with("</html>"));
        assert!(html.contains("A &lt;metric&gt;"));
        assert!(html.contains("noisy &lt;host&gt;"));
        assert!(html.contains("<svg"));
        assert!(!html.contains("NaN"));
        assert!(!html.contains("<script"));
    }
}
