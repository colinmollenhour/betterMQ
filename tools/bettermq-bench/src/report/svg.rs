//! Dependency-free SVG charts. Colours are CSS variables (`--s1`..`--s4`) so the
//! page theme, including dark mode, drives them; the palette is the first four
//! slots of the validated categorical order.

use crate::model::PhaseKind;
use std::fmt::Write as _;

#[derive(Clone, Copy, PartialEq)]
pub enum Agg {
    Mean,
    Max,
}

pub struct Line {
    pub name: String,
    /// 1..=4, mapped to `--s1`..`--s4`.
    pub slot: u8,
    pub pts: Vec<(f64, Option<f64>)>,
    pub agg: Agg,
    pub dashed: bool,
}

pub struct Band {
    pub x0: f64,
    pub x1: f64,
    pub kind: PhaseKind,
}

pub struct Chart {
    pub title: String,
    pub unit: String,
    pub x_label: String,
    pub lines: Vec<Line>,
    pub bands: Vec<Band>,
    /// `(x, label)` vertical reference lines.
    pub vlines: Vec<(f64, String)>,
    /// `(y, label)` horizontal reference lines (e.g. an SLO).
    pub hlines: Vec<(f64, String)>,
    pub x_log: bool,
    pub y_log: bool,
    pub markers: bool,
    pub width: f64,
    pub height: f64,
}

impl Chart {
    pub fn new(title: &str, unit: &str) -> Self {
        Self {
            title: title.into(),
            unit: unit.into(),
            x_label: "seconds".into(),
            lines: Vec::new(),
            bands: Vec::new(),
            vlines: Vec::new(),
            hlines: Vec::new(),
            x_log: false,
            y_log: false,
            markers: false,
            width: 560.0,
            height: 230.0,
        }
    }
}

pub fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn downsample(pts: &[(f64, Option<f64>)], agg: Agg, max_points: usize) -> Vec<(f64, Option<f64>)> {
    if pts.len() <= max_points {
        return pts.to_vec();
    }
    let bucket = pts.len().div_ceil(max_points);
    pts.chunks(bucket)
        .map(|c| {
            let x = c.iter().map(|p| p.0).sum::<f64>() / c.len() as f64;
            let vals: Vec<f64> = c.iter().filter_map(|p| p.1).collect();
            let y = if vals.is_empty() {
                None
            } else if agg == Agg::Max {
                Some(vals.iter().copied().fold(f64::MIN, f64::max))
            } else {
                Some(vals.iter().sum::<f64>() / vals.len() as f64)
            };
            (x, y)
        })
        .collect()
}

pub fn nice_ticks(min: f64, max: f64, target: usize) -> Vec<f64> {
    if max <= min || max.is_nan() || min.is_nan() {
        return vec![min];
    }
    let raw = (max - min) / target.max(1) as f64;
    let mag = 10f64.powf(raw.log10().floor());
    let norm = raw / mag;
    let step = mag
        * if norm < 1.5 {
            1.0
        } else if norm < 3.0 {
            2.0
        } else if norm < 7.0 {
            5.0
        } else {
            10.0
        };
    let mut out = Vec::new();
    let mut v = (min / step).ceil() * step;
    while v <= max + step * 1e-9 {
        out.push((v / step).round() * step);
        v += step;
    }
    out
}

fn log_ticks(min: f64, max: f64) -> Vec<f64> {
    let (lo, hi) = (min.log10().floor() as i32, max.log10().ceil() as i32);
    let mut out = Vec::new();
    for e in lo..=hi {
        let base = 10f64.powi(e);
        for m in [1.0, 2.0, 5.0] {
            let v = base * m;
            if v >= min * 0.999 && v <= max * 1.001 {
                out.push(v);
            }
        }
    }
    if out.len() > 8 {
        out.retain(|v| (v.log10() - v.log10().round()).abs() < 1e-9);
    }
    out
}

pub fn fmt_axis(v: f64) -> String {
    let a = v.abs();
    if a >= 1_000_000.0 {
        format!("{:.1}M", v / 1e6)
    } else if a >= 10_000.0 {
        format!("{:.0}k", v / 1e3)
    } else if a >= 1_000.0 {
        format!("{:.1}k", v / 1e3)
    } else if a >= 100.0 || (a - a.round()).abs() < 1e-9 {
        format!("{v:.0}")
    } else if a >= 1.0 {
        format!("{v:.1}")
    } else if a == 0.0 {
        "0".into()
    } else {
        format!("{v:.2}")
    }
}

fn band_class(k: PhaseKind) -> &'static str {
    match k {
        PhaseKind::Warmup | PhaseKind::Idle => "b-idle",
        PhaseKind::Measure => "b-measure",
        PhaseKind::Fault => "b-fault",
        PhaseKind::Drain => "b-drain",
        PhaseKind::Settle => "b-settle",
        PhaseKind::Other => "b-other",
    }
}

/// Render a chart; returns an empty string if there is nothing to draw.
pub fn render(c: &Chart) -> String {
    let lines: Vec<(&Line, Vec<(f64, f64)>)> = c
        .lines
        .iter()
        .map(|l| {
            let pts = downsample(&l.pts, l.agg, 180);
            let valid: Vec<(f64, f64)> = pts
                .iter()
                .filter_map(|(x, y)| {
                    y.filter(|v| v.is_finite() && (!c.y_log || *v > 0.0))
                        .map(|v| (*x, v))
                })
                .filter(|(x, _)| !c.x_log || *x > 0.0)
                .collect();
            (l, valid)
        })
        // A series that never rises above zero carries no information.
        .filter(|(_, p)| p.iter().any(|(_, y)| *y > 1e-9))
        .collect();
    if lines.is_empty() {
        return String::new();
    }
    let (ml, mr, mt, mb) = (50.0, 14.0, 10.0, 34.0);
    let (w, h) = (c.width, c.height);
    let (pw, ph) = (w - ml - mr, h - mt - mb);

    let xs = lines.iter().flat_map(|(_, p)| p.iter().map(|q| q.0));
    let (mut x0, mut x1) = xs.fold((f64::MAX, f64::MIN), |a, v| (a.0.min(v), a.1.max(v)));
    let ys = lines.iter().flat_map(|(_, p)| p.iter().map(|q| q.1));
    let (mut y0, mut y1) = ys.fold((f64::MAX, f64::MIN), |a, v| (a.0.min(v), a.1.max(v)));
    for (y, _) in &c.hlines {
        if !c.y_log || *y > 0.0 {
            y0 = y0.min(*y);
            y1 = y1.max(*y);
        }
    }
    if x1 <= x0 {
        x0 -= 1.0;
        x1 += 1.0;
    }
    if c.y_log {
        y0 = y0.max(1e-3);
        y0 = 10f64.powf(y0.log10().floor());
        y1 = 10f64.powf(y1.log10().ceil()).max(y0 * 10.0);
    } else {
        y0 = 0f64.min(y0);
        if y1 <= y0 {
            y1 = y0 + 1.0;
        }
        y1 *= 1.06;
    }
    let tx = |x: f64| -> f64 {
        if c.x_log {
            ml + pw * ((x.max(1e-9).log10() - x0.log10()) / (x1.log10() - x0.log10()).max(1e-9))
        } else {
            ml + pw * (x - x0) / (x1 - x0)
        }
    };
    let ty = |y: f64| -> f64 {
        if c.y_log {
            mt + ph
                * (1.0 - (y.max(1e-9).log10() - y0.log10()) / (y1.log10() - y0.log10()).max(1e-9))
        } else {
            mt + ph * (1.0 - (y - y0) / (y1 - y0))
        }
    };

    let mut s = String::new();
    let _ = write!(
        s,
        "<figure class=\"chart\"><figcaption>{}<span class=\"unit\">{}</span></figcaption>",
        esc(&c.title),
        if c.unit.is_empty() {
            String::new()
        } else {
            format!(" ({})", esc(&c.unit))
        }
    );
    if lines.len() > 1 {
        s.push_str("<div class=\"legend\">");
        for (l, _) in &lines {
            let _ = write!(
                s,
                "<span><i class=\"sw s{}{}\"></i>{}</span>",
                l.slot,
                if l.dashed { " dash" } else { "" },
                esc(&l.name)
            );
        }
        s.push_str("</div>");
    }
    let _ = write!(
        s,
        "<svg viewBox=\"0 0 {w} {h}\" role=\"img\" aria-label=\"{}\">",
        esc(&c.title)
    );
    // phase bands
    for b in &c.bands {
        let (a, bb) = (
            tx(b.x0.max(x0)).clamp(ml, ml + pw),
            tx(b.x1.min(x1)).clamp(ml, ml + pw),
        );
        if bb > a {
            let _ = write!(
                s,
                "<rect class=\"{}\" x=\"{a:.1}\" y=\"{mt}\" width=\"{:.1}\" height=\"{ph}\"/>",
                band_class(b.kind),
                bb - a
            );
        }
    }
    // grid + y axis
    let yt = if c.y_log {
        log_ticks(y0, y1)
    } else {
        nice_ticks(y0, y1, 4)
    };
    for v in &yt {
        let y = ty(*v);
        let _ = write!(
            s,
            "<line class=\"grid\" x1=\"{ml}\" x2=\"{:.1}\" y1=\"{y:.1}\" y2=\"{y:.1}\"/><text class=\"tick\" x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"end\">{}</text>",
            ml + pw,
            ml - 6.0,
            y + 3.5,
            fmt_axis(*v)
        );
    }
    // x axis
    let xt = if c.x_log {
        log_ticks(x0, x1)
    } else {
        nice_ticks(x0, x1, 6)
    };
    for v in &xt {
        let x = tx(*v);
        let _ = write!(
            s,
            "<line class=\"axis\" x1=\"{x:.1}\" x2=\"{x:.1}\" y1=\"{:.1}\" y2=\"{:.1}\"/><text class=\"tick\" x=\"{x:.1}\" y=\"{:.1}\" text-anchor=\"middle\">{}</text>",
            mt + ph,
            mt + ph + 4.0,
            mt + ph + 16.0,
            fmt_axis(*v)
        );
    }
    let _ = write!(
        s,
        "<line class=\"axis\" x1=\"{ml}\" x2=\"{:.1}\" y1=\"{:.1}\" y2=\"{:.1}\"/><text class=\"tick xlabel\" x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"middle\">{}</text>",
        ml + pw,
        mt + ph,
        mt + ph,
        ml + pw / 2.0,
        h - 4.0,
        esc(&c.x_label)
    );
    for (y, label) in &c.hlines {
        if !c.y_log || *y > 0.0 {
            let py = ty(*y);
            let _ = write!(
                s,
                "<line class=\"ref\" x1=\"{ml}\" x2=\"{:.1}\" y1=\"{py:.1}\" y2=\"{py:.1}\"/><text class=\"reflabel\" x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"end\">{}</text>",
                ml + pw,
                ml + pw - 3.0,
                py - 3.0,
                esc(label)
            );
        }
    }
    for (x, label) in &c.vlines {
        if *x >= x0 && *x <= x1 {
            let px = tx(*x);
            let _ = write!(
                s,
                "<line class=\"ref\" x1=\"{px:.1}\" x2=\"{px:.1}\" y1=\"{mt}\" y2=\"{:.1}\"/><text class=\"reflabel\" x=\"{:.1}\" y=\"{:.1}\">{}</text>",
                mt + ph,
                px + 3.0,
                mt + 10.0,
                esc(label)
            );
        }
    }
    // data
    for (l, pts) in &lines {
        let mut d = String::new();
        let mut pen_up = true;
        let mut prev_x = f64::NEG_INFINITY;
        let med_gap = if pts.len() > 2 {
            let mut gaps: Vec<f64> = pts.windows(2).map(|w| w[1].0 - w[0].0).collect();
            gaps.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            gaps[gaps.len() / 2]
        } else {
            f64::MAX
        };
        for (x, y) in pts {
            // break the line across gaps (missing samples)
            if !c.x_log && !c.markers && *x - prev_x > med_gap * 3.5 {
                pen_up = true;
            }
            let _ = write!(
                d,
                "{}{:.1},{:.1} ",
                if pen_up { "M" } else { "L" },
                tx(*x),
                ty(*y)
            );
            pen_up = false;
            prev_x = *x;
        }
        let _ = write!(
            s,
            "<path class=\"line s{}{}\" d=\"{}\"/>",
            l.slot,
            if l.dashed { " dash" } else { "" },
            d.trim_end()
        );
        if c.markers || pts.len() <= 24 {
            for (x, y) in pts {
                let _ = write!(
                    s,
                    "<circle class=\"dot s{}\" cx=\"{:.1}\" cy=\"{:.1}\" r=\"3.5\"/>",
                    l.slot,
                    tx(*x),
                    ty(*y)
                );
            }
        }
    }
    // hover columns: native tooltips, one per distinct x of the first line
    if let Some((_, base)) = lines.first() {
        let n = base.len();
        for (i, (x, _)) in base.iter().enumerate() {
            let left = if i == 0 {
                ml
            } else {
                (tx(base[i - 1].0) + tx(*x)) / 2.0
            };
            let right = if i + 1 == n {
                ml + pw
            } else {
                (tx(*x) + tx(base[i + 1].0)) / 2.0
            };
            let mut tip = format!("{}: {}", c.x_label, fmt_axis(*x));
            for (l, pts) in &lines {
                if let Some((_, y)) = pts.iter().min_by(|a, b| {
                    (a.0 - x)
                        .abs()
                        .partial_cmp(&(b.0 - x).abs())
                        .unwrap_or(std::cmp::Ordering::Equal)
                }) {
                    let _ = write!(tip, "\n{}: {}", l.name, fmt_axis(*y));
                }
            }
            let _ = write!(
                s,
                "<rect class=\"hit\" x=\"{left:.1}\" y=\"{mt}\" width=\"{:.1}\" height=\"{ph}\"><title>{}</title></rect>",
                (right - left).max(1.0),
                esc(&tip)
            );
        }
    }
    s.push_str("</svg></figure>");
    s
}

pub const CSS: &str = r#"
.chart{margin:0;padding:10px 12px 6px;background:var(--surface);border:1px solid var(--line);border-radius:8px}
.chart figcaption{font-weight:600;font-size:13px;margin-bottom:2px}
.chart .unit{font-weight:400;color:var(--muted)}
.chart svg{width:100%;height:auto;display:block}
.legend{display:flex;flex-wrap:wrap;gap:4px 14px;font-size:12px;color:var(--muted);margin:2px 0 4px}
.legend .sw{display:inline-block;width:14px;height:3px;border-radius:2px;vertical-align:middle;margin-right:5px}
.sw.s1{background:var(--s1)}.sw.s2{background:var(--s2)}.sw.s3{background:var(--s3)}.sw.s4{background:var(--s4)}
.sw.dash{background:repeating-linear-gradient(90deg,currentColor 0 4px,transparent 4px 7px)}
.sw.dash.s1{color:var(--s1)}.sw.dash.s2{color:var(--s2)}.sw.dash.s3{color:var(--s3)}.sw.dash.s4{color:var(--s4)}
svg .grid{stroke:var(--grid);stroke-width:1}
svg .axis{stroke:var(--axis);stroke-width:1}
svg .tick{fill:var(--muted);font-size:10.5px;font-family:inherit}
svg .ref{stroke:var(--muted);stroke-width:1;stroke-dasharray:4 3}
svg .reflabel{fill:var(--muted);font-size:10px}
svg .line{fill:none;stroke-width:2;stroke-linejoin:round;stroke-linecap:round}
svg .line.dash{stroke-dasharray:5 4}
svg .line.s1{stroke:var(--s1)}svg .line.s2{stroke:var(--s2)}svg .line.s3{stroke:var(--s3)}svg .line.s4{stroke:var(--s4)}
svg .dot{stroke:var(--surface);stroke-width:2}
svg .dot.s1{fill:var(--s1)}svg .dot.s2{fill:var(--s2)}svg .dot.s3{fill:var(--s3)}svg .dot.s4{fill:var(--s4)}
svg .hit{fill:transparent}
svg .hit:hover{fill:var(--hover)}
svg .b-idle{fill:var(--band-idle)}svg .b-measure{fill:var(--band-measure)}svg .b-fault{fill:var(--band-fault)}
svg .b-drain{fill:var(--band-drain)}svg .b-settle{fill:var(--band-settle)}svg .b-other{fill:var(--band-idle)}
"#;

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn nice_ticks_cover_range() {
        let t = nice_ticks(0.0, 97.0, 4);
        assert_eq!(t.first().copied(), Some(0.0));
        assert!(t.last().copied().unwrap() <= 97.0 + 1.0);
        assert!(t.len() >= 3);
    }

    #[test]
    fn renders_lines_and_escapes() {
        let mut c = Chart::new("a<b", "ms");
        c.lines.push(Line {
            name: "x&y".into(),
            slot: 1,
            pts: (0..10).map(|i| (i as f64, Some(i as f64))).collect(),
            agg: Agg::Mean,
            dashed: false,
        });
        c.bands.push(Band {
            x0: 2.0,
            x1: 5.0,
            kind: PhaseKind::Fault,
        });
        let svg = render(&c);
        assert!(svg.contains("a&lt;b"));
        assert!(svg.contains("<path class=\"line s1\""));
        assert!(svg.contains("b-fault"));
        assert!(!svg.contains("NaN"));
    }

    #[test]
    fn empty_chart_renders_nothing() {
        let mut c = Chart::new("t", "");
        c.lines.push(Line {
            name: "n".into(),
            slot: 1,
            pts: vec![(0.0, None)],
            agg: Agg::Mean,
            dashed: false,
        });
        assert!(render(&c).is_empty());
    }

    #[test]
    fn log_axes_drop_nonpositive_values() {
        let mut c = Chart::new("t", "");
        c.y_log = true;
        c.lines.push(Line {
            name: "n".into(),
            slot: 2,
            pts: vec![(0.0, Some(0.0)), (1.0, Some(10.0)), (2.0, Some(1000.0))],
            agg: Agg::Max,
            dashed: false,
        });
        let svg = render(&c);
        assert!(!svg.contains("NaN") && !svg.contains("inf"));
    }

    #[test]
    fn downsamples_long_series() {
        let pts: Vec<(f64, Option<f64>)> = (0..2000).map(|i| (i as f64, Some(1.0))).collect();
        assert!(downsample(&pts, Agg::Mean, 180).len() <= 180);
    }
}
