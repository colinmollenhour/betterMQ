//! Prometheus text scraping for the broker's own counters.

use std::collections::HashMap;

#[derive(Default, Clone, Debug)]
pub struct Snapshot {
    /// `(metric name, label string, value)`; labels are the raw `{...}` body.
    rows: Vec<(String, String, f64)>,
}

impl Snapshot {
    pub fn parse(text: &str) -> Self {
        let mut rows = Vec::new();
        for line in text.lines() {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            let Some((lhs, value)) = line.rsplit_once(' ') else {
                continue;
            };
            let Ok(value) = value.parse::<f64>() else {
                continue;
            };
            let (name, labels) = match lhs.split_once('{') {
                Some((n, rest)) => (n, rest.trim_end_matches('}')),
                None => (lhs, ""),
            };
            rows.push((name.to_string(), labels.to_string(), value));
        }
        Self { rows }
    }

    /// Sum across every label set of `name` (e.g. all shards).
    pub fn sum(&self, name: &str) -> Option<f64> {
        let mut found = false;
        let mut total = 0.0;
        for (n, _, v) in &self.rows {
            if n == name {
                found = true;
                total += v;
            }
        }
        found.then_some(total)
    }

    /// Cumulative histogram buckets summed across label sets, sorted by `le`.
    pub fn buckets(&self, name_bucket: &str) -> Vec<(f64, f64)> {
        let mut by_le: HashMap<String, f64> = HashMap::new();
        for (n, labels, v) in &self.rows {
            if n != name_bucket {
                continue;
            }
            let le = labels
                .split(',')
                .find_map(|kv| kv.strip_prefix("le=\""))
                .map(|s| s.trim_end_matches('"').to_string());
            if let Some(le) = le {
                *by_le.entry(le).or_default() += v;
            }
        }
        let mut out: Vec<(f64, f64)> = by_le
            .into_iter()
            .map(|(le, v)| {
                let edge = if le == "+Inf" {
                    f64::INFINITY
                } else {
                    le.parse().unwrap_or(f64::INFINITY)
                };
                (edge, v)
            })
            .collect();
        out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        out
    }

    /// Flat map for forensic storage: label sets are summed per metric name.
    pub fn flat(&self) -> std::collections::BTreeMap<String, f64> {
        let mut out = std::collections::BTreeMap::new();
        for (n, _, v) in &self.rows {
            if n.ends_with("_bucket") {
                continue;
            }
            *out.entry(n.clone()).or_insert(0.0) += v;
        }
        out
    }
}

/// Quantile (in the bucket's own unit) from the *difference* of two cumulative
/// bucket snapshots, i.e. the distribution of observations between scrapes.
pub fn interval_quantile(prev: &[(f64, f64)], cur: &[(f64, f64)], q: f64) -> Option<f64> {
    if cur.is_empty() {
        return None;
    }
    let delta: Vec<(f64, f64)> = cur
        .iter()
        .map(|(le, c)| {
            let p = prev
                .iter()
                .find(|(ple, _)| ple == le)
                .map(|(_, v)| *v)
                .unwrap_or(0.0);
            (*le, (c - p).max(0.0))
        })
        .collect();
    let total = delta.last().map(|(_, c)| *c).unwrap_or(0.0);
    if total <= 0.0 {
        return None;
    }
    let target = q * total;
    let mut lower = 0.0;
    let mut prev_count = 0.0;
    for (le, count) in &delta {
        if *count >= target {
            if !le.is_finite() {
                return Some(lower);
            }
            let span = count - prev_count;
            let frac = if span > 0.0 {
                (target - prev_count) / span
            } else {
                1.0
            };
            return Some(lower + (le - lower) * frac);
        }
        lower = *le;
        prev_count = *count;
    }
    Some(lower)
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    const TEXT: &str = r#"# HELP x
bettermq_wal_fsync_total{shard="0"} 3
bettermq_wal_fsync_total{shard="1"} 4
bettermq_x_bucket{shard="0",le="0.001"} 2
bettermq_x_bucket{shard="1",le="0.001"} 3
bettermq_x_bucket{shard="0",le="+Inf"} 4
bettermq_x_bucket{shard="1",le="+Inf"} 6
plain 7
"#;

    #[test]
    fn sums_across_labels() {
        let s = Snapshot::parse(TEXT);
        assert_eq!(s.sum("bettermq_wal_fsync_total"), Some(7.0));
        assert_eq!(s.sum("plain"), Some(7.0));
        assert_eq!(s.sum("missing"), None);
    }

    #[test]
    fn buckets_are_sorted_and_summed() {
        let s = Snapshot::parse(TEXT);
        let b = s.buckets("bettermq_x_bucket");
        assert_eq!(b, vec![(0.001, 5.0), (f64::INFINITY, 10.0)]);
    }

    #[test]
    fn interval_quantile_uses_delta() {
        let prev = vec![(1.0, 10.0), (10.0, 10.0), (f64::INFINITY, 10.0)];
        let cur = vec![(1.0, 10.0), (10.0, 20.0), (f64::INFINITY, 20.0)];
        // All 10 new observations fell in (1, 10].
        let p50 = interval_quantile(&prev, &cur, 0.5).unwrap();
        assert!(p50 > 1.0 && p50 <= 10.0);
        assert!(interval_quantile(&cur, &cur, 0.5).is_none());
    }
}
