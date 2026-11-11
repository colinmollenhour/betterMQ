//! Result data model. `results.json` is this structure serialised; the report
//! renderer and the compare tool consume only this, never live state.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const SCHEMA_VERSION: u32 = 1;

/// JSON has no NaN: non-finite floats are written as `null`. Read them back as NaN.
mod nan {
    use serde::{Deserialize, Deserializer};

    pub fn f64<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
        Ok(Option::<f64>::deserialize(d)?.unwrap_or(f64::NAN))
    }

    pub fn vec<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<f64>, D::Error> {
        Ok(Vec::<Option<f64>>::deserialize(d)?
            .into_iter()
            .map(|v| v.unwrap_or(f64::NAN))
            .collect())
    }

    pub fn map<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<std::collections::BTreeMap<String, f64>, D::Error> {
        Ok(
            std::collections::BTreeMap::<String, Option<f64>>::deserialize(d)?
                .into_iter()
                .map(|(k, v)| (k, v.unwrap_or(f64::NAN)))
                .collect(),
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RunReport {
    pub schema: u32,
    pub meta: RunMeta,
    pub env: EnvInfo,
    pub calibration: Option<Calibration>,
    pub scenarios: Vec<ScenarioResult>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RunMeta {
    pub tool_version: String,
    pub started_at: String,
    pub finished_at: String,
    pub profile: String,
    pub seed: u64,
    pub repeats: u32,
    pub command_line: String,
    pub git_sha: String,
    pub git_dirty: bool,
    pub git_branch: String,
    /// Human name for the broker build under test (e.g. a branch).
    #[serde(default)]
    pub label: String,
    pub broker_binary: String,
    pub broker_sha256: String,
    pub broker_size_bytes: u64,
    pub broker_version: String,
    pub fsync: String,
    pub rust_log: String,
    pub broker_cpus: String,
    pub bench_cpus: String,
    pub data_root: String,
    pub auth: String,
    pub slo: Slo,
    #[serde(default, deserialize_with = "nan::f64")]
    pub rate_scale: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub time_scale: f64,
    #[serde(default)]
    pub broker_env: Vec<String>,
}

/// Pass/fail thresholds that define "sustained" throughput and "breakdown".
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Slo {
    /// Durable-ACK p99 limit (ms), measured from the *intended* send time.
    #[serde(default, deserialize_with = "nan::f64")]
    pub ack_p99_ms: f64,
    /// Maximum fraction of offered messages that may be rejected or fail.
    #[serde(default, deserialize_with = "nan::f64")]
    pub max_error_rate: f64,
    /// Achieved throughput must reach this fraction of offered.
    #[serde(default, deserialize_with = "nan::f64")]
    pub min_achieved_ratio: f64,
    /// End-to-end delivery p99 limit (ms) where a scenario enforces it.
    #[serde(default, deserialize_with = "nan::f64")]
    pub delivery_p99_ms: f64,
}

impl Default for Slo {
    fn default() -> Self {
        Self {
            ack_p99_ms: 50.0,
            max_error_rate: 0.001,
            min_achieved_ratio: 0.97,
            delivery_p99_ms: 1_000.0,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct EnvInfo {
    pub os: String,
    pub kernel: String,
    pub cpu_model: String,
    pub logical_cpus: u32,
    pub mem_total_mb: u64,
    pub swap_total_mb: u64,
    pub governor: String,
    pub data_fs_type: String,
    pub data_device_rotational: Option<bool>,
    #[serde(default, deserialize_with = "nan::f64")]
    pub load_avg_start: f64,
    /// Share of the broker CPUs already busy with other work before the run (%).
    pub preflight_busy_broker_cpus_pct: Option<f64>,
    pub preflight_busy_bench_cpus_pct: Option<f64>,
    pub preflight_cpu_pressure_pct: Option<f64>,
    pub rustc: String,
    /// Conditions that make results less trustworthy or less comparable.
    pub warnings: Vec<String>,
}

/// Harness self-check: how fast the load generator can go against a null
/// server, so readers know when a result is generator-limited.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Calibration {
    #[serde(default, deserialize_with = "nan::f64")]
    pub null_server_msgs_s: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub null_server_p50_us: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub null_server_p99_us: f64,
    pub connections: u32,
    pub note: String,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    HigherBetter,
    LowerBetter,
    Info,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Metric {
    pub id: String,
    pub label: String,
    pub unit: String,
    #[serde(default, deserialize_with = "nan::f64")]
    pub value: f64,
    pub direction: Direction,
    /// Scorecard section, e.g. "throughput", "latency", "cpu", "memory".
    pub group: String,
    /// Shown in the report's headline scorecard.
    pub headline: bool,
    /// Per-trial values when the scenario was repeated.
    #[serde(default, deserialize_with = "nan::vec")]
    pub samples: Vec<f64>,
}

impl Metric {
    pub fn new(
        id: impl Into<String>,
        label: impl Into<String>,
        unit: impl Into<String>,
        value: f64,
        direction: Direction,
        group: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            unit: unit.into(),
            value,
            direction,
            group: group.into(),
            headline: false,
            samples: Vec::new(),
        }
    }

    pub fn headline(mut self) -> Self {
        self.headline = true;
        self
    }

    /// Relative spread of per-trial samples (stddev / mean); `None` for one trial.
    pub fn cv(&self) -> Option<f64> {
        if self.samples.len() < 2 {
            return None;
        }
        let n = self.samples.len() as f64;
        let mean = self.samples.iter().sum::<f64>() / n;
        if mean.abs() < f64::EPSILON {
            return Some(0.0);
        }
        let var = self.samples.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
        Some(var.sqrt() / mean.abs())
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Pass,
    Warn,
    Fail,
    Error,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Notice,
    Warn,
    Critical,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Finding {
    pub severity: Severity,
    pub scenario: String,
    pub title: String,
    pub detail: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Table {
    pub title: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PhaseKind {
    Warmup,
    Measure,
    Drain,
    Settle,
    Fault,
    Idle,
    Other,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Phase {
    pub name: String,
    pub kind: PhaseKind,
    #[serde(default, deserialize_with = "nan::f64")]
    pub start_s: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub end_s: f64,
}

/// Columnar 1 Hz time series, aligned on `t` (seconds since trial start).
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Series {
    pub t: Vec<f64>,
    pub cols: BTreeMap<String, Vec<Option<f64>>>,
}

impl Series {
    pub fn col(&self, name: &str) -> Option<&Vec<Option<f64>>> {
        self.cols.get(name)
    }

    pub fn values(&self, name: &str) -> Vec<f64> {
        self.col(name)
            .map(|c| c.iter().filter_map(|v| *v).collect())
            .unwrap_or_default()
    }

    /// Values of `name` whose sample time lies inside `[start, end)`.
    pub fn window(&self, name: &str, start: f64, end: f64) -> Vec<f64> {
        let Some(col) = self.col(name) else {
            return Vec::new();
        };
        self.t
            .iter()
            .zip(col.iter())
            .filter(|(t, _)| **t >= start && **t < end)
            .filter_map(|(_, v)| *v)
            .collect()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct StepResult {
    pub index: usize,
    pub label: String,
    #[serde(default, deserialize_with = "nan::f64")]
    pub offered_msg_s: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub achieved_msg_s: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub duration_s: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub ack_p50_ms: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub ack_p99_ms: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub ack_p999_ms: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub ack_max_ms: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub error_rate: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub cpu_cores: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub rss_mb: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub gen_lag_p99_ms: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub backlog_growth_per_s: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub delivered_msg_s: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub delivery_p99_ms: f64,
    pub passed: bool,
    pub fail_reasons: Vec<String>,
    #[serde(default, deserialize_with = "nan::f64")]
    pub start_s: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub end_s: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct MemoryRelease {
    #[serde(default, deserialize_with = "nan::f64")]
    pub idle_start_mb: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub peak_mb: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub post_drain_mb: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub settled_mb: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub settled_anon_mb: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub settled_file_mb: f64,
    /// (settled - idle) / (peak - idle), clamped to >= 0.
    #[serde(default, deserialize_with = "nan::f64")]
    pub retained_pct: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub settle_secs: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub disk_idle_mb: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub disk_peak_mb: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub disk_settled_mb: f64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub disk_retained_pct: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Integrity {
    pub accepted: u64,
    pub delivered_unique: u64,
    pub lost: u64,
    pub duplicate_deliveries: u64,
    pub order_violations: u64,
    /// Of those, how many involved a message that had been retried.
    #[serde(default)]
    pub order_violations_after_retry: u64,
    pub malformed: u64,
    /// Highest simultaneous handler count seen per destination vs its configured cap.
    pub concurrency: Vec<ConcurrencyCheck>,
    pub concurrency_violations: u64,
    /// Whether the scenario expected zero loss (false when DLQ-bound failures are injected).
    pub loss_expected_zero: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ConcurrencyCheck {
    pub dest: String,
    pub configured: u32,
    pub observed_max: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct TrialResult {
    pub index: u32,
    pub seed: u64,
    #[serde(default, deserialize_with = "nan::f64")]
    pub duration_s: f64,
    pub phases: Vec<Phase>,
    pub series: Series,
    pub steps: Vec<StepResult>,
    pub memory: Option<MemoryRelease>,
    pub integrity: Option<Integrity>,
    pub metrics: Vec<Metric>,
    pub tables: Vec<Table>,
    pub notes: Vec<String>,
    pub broker_log_warnings: u64,
    pub broker_log_errors: u64,
    pub broker_log_bytes: u64,
    /// Most frequent WARN/ERROR messages, normalised, with counts.
    #[serde(default)]
    pub broker_log_top: Vec<(String, u64)>,
    /// Last scrape of broker counters, kept for forensics.
    #[serde(default, deserialize_with = "nan::map")]
    pub final_counters: BTreeMap<String, f64>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ScenarioResult {
    pub id: String,
    pub title: String,
    pub description: String,
    pub params: BTreeMap<String, String>,
    pub status: Status,
    pub error: Option<String>,
    pub trials: Vec<TrialResult>,
    pub metrics: Vec<Metric>,
    pub findings: Vec<Finding>,
}

/// Aggregate per-trial metrics into scenario metrics: median value, raw samples kept.
pub fn aggregate_metrics(trials: &[TrialResult]) -> Vec<Metric> {
    let mut order: Vec<String> = Vec::new();
    let mut by_id: BTreeMap<String, Metric> = BTreeMap::new();
    for trial in trials {
        for m in &trial.metrics {
            match by_id.get_mut(&m.id) {
                Some(existing) => existing.samples.push(m.value),
                None => {
                    order.push(m.id.clone());
                    let mut first = m.clone();
                    first.samples = vec![m.value];
                    by_id.insert(m.id.clone(), first);
                }
            }
        }
    }
    order
        .into_iter()
        .filter_map(|id| by_id.remove(&id))
        .map(|mut m| {
            m.value = median(&m.samples);
            m
        })
        .collect()
}

pub fn median(values: &[f64]) -> f64 {
    let mut v: Vec<f64> = values.iter().copied().filter(|x| x.is_finite()).collect();
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    fn trial(v: f64) -> TrialResult {
        TrialResult {
            metrics: vec![Metric::new(
                "a",
                "A",
                "ms",
                v,
                Direction::LowerBetter,
                "latency",
            )],
            ..Default::default()
        }
    }

    #[test]
    fn aggregates_median_and_keeps_samples() {
        let m = aggregate_metrics(&[trial(1.0), trial(5.0), trial(3.0)]);
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].value, 3.0);
        assert_eq!(m[0].samples, vec![1.0, 5.0, 3.0]);
        assert!(m[0].cv().unwrap() > 0.5);
    }

    #[test]
    fn non_finite_values_survive_a_json_round_trip() {
        let mut m = Metric::new("a", "A", "ms", f64::NAN, Direction::LowerBetter, "latency");
        m.samples = vec![1.0, f64::NAN];
        let step = StepResult {
            ack_p99_ms: f64::NAN,
            ..Default::default()
        };
        let back: Metric = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert!(back.value.is_nan());
        assert_eq!(back.samples[0], 1.0);
        assert!(back.samples[1].is_nan());
        let back: StepResult =
            serde_json::from_str(&serde_json::to_string(&step).unwrap()).unwrap();
        assert!(back.ack_p99_ms.is_nan());
    }

    #[test]
    fn single_trial_has_no_cv() {
        let m = aggregate_metrics(&[trial(2.0)]);
        assert!(m[0].cv().is_none());
    }

    #[test]
    fn series_window_filters_by_time() {
        let mut s = Series::default();
        s.t = vec![0.0, 1.0, 2.0, 3.0];
        s.cols
            .insert("x".into(), vec![Some(1.0), None, Some(3.0), Some(4.0)]);
        assert_eq!(s.window("x", 1.0, 3.0), vec![3.0]);
    }
}
