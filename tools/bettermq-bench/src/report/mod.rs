//! Report writers: results.json, Markdown, self-contained HTML, CSV series.

pub mod compare;
pub mod fmt;
pub mod html;
pub mod markdown;
pub mod svg;

use crate::model::RunReport;
use anyhow::Context;
use std::path::Path;

pub fn load(path: &Path) -> anyhow::Result<RunReport> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))
}

pub fn write_json(report: &RunReport, dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join("results.json.tmp");
    std::fs::write(&tmp, serde_json::to_vec(report)?)?;
    std::fs::rename(&tmp, dir.join("results.json"))?;
    Ok(())
}

pub fn write_all(report: &RunReport, dir: &Path) -> anyhow::Result<()> {
    write_json(report, dir)?;
    std::fs::write(dir.join("report.md"), markdown::render(report))?;
    std::fs::write(dir.join("report.html"), html::render(report))?;
    let series_dir = dir.join("series");
    std::fs::create_dir_all(&series_dir)?;
    for s in &report.scenarios {
        for t in &s.trials {
            let mut csv = String::from("t");
            for name in t.series.cols.keys() {
                csv.push(',');
                csv.push_str(name);
            }
            csv.push('\n');
            for (i, ts) in t.series.t.iter().enumerate() {
                csv.push_str(&format!("{ts:.2}"));
                for col in t.series.cols.values() {
                    csv.push(',');
                    if let Some(Some(v)) = col.get(i) {
                        csv.push_str(&format!("{v:.4}"));
                    }
                }
                csv.push('\n');
            }
            std::fs::write(series_dir.join(format!("{}-t{}.csv", s.id, t.index)), csv)?;
        }
    }
    Ok(())
}
