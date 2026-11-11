//! Workload models and arrival plans.
//!
//! The "fulfillment" mix is a generalised multi-tenant order-processing
//! backend: storefront integrations push orders in, inventory and status
//! events fan through ordered per-entity lanes, slow external carrier and
//! partner APIs sit at the other end, and housekeeping jobs run on a schedule.
//! Rates, sizes, handler latencies and failure rates are hypothetical but
//! shaped like a real back-office system: heavy-tailed, skewed across tenants,
//! bursty around daily cut-offs.

use crate::loadgen::{OutMsg, Plan, Planned};
use crate::rng::{hash2, lognormal_from_hash, weighted_index, Rng, Zipf};
use crate::verify::Stream;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

/// Shared pseudo-random filler. Hex text is roughly half-compressible, which
/// is closer to real JSON than all-`x` (free to compress) or raw random bytes.
pub struct PadPool {
    text: String,
}

impl PadPool {
    pub fn new(seed: u64) -> Arc<Self> {
        let mut rng = Rng::new(seed ^ 0xFADE);
        let mut text = String::with_capacity(2 * 1024 * 1024);
        while text.len() < 2 * 1024 * 1024 {
            let _ = write!(text, "{:016x}", rng.next_u64());
        }
        Arc::new(Self { text })
    }

    pub fn slice(&self, len: usize, h: u64) -> &str {
        let len = len.min(self.text.len() / 2);
        let start = (h as usize) % (self.text.len() - len);
        &self.text[start..start + len]
    }
}

pub struct BodyFields<'a> {
    pub stream: u16,
    pub seq: u64,
    pub key_idx: u32,
    pub key_seq: u32,
    pub t_us: u64,
    pub due_us: u64,
    pub tenant: &'a str,
    pub kind: &'a str,
}

/// JSON object body of roughly `target` bytes. The verification header comes
/// first so the sink can parse it without scanning the filler.
pub fn render_body(f: &BodyFields<'_>, target: usize, pad: &PadPool, h: u64) -> String {
    let mut s = String::with_capacity(target.max(120) + 16);
    let _ = write!(
        s,
        "{{\"s\":{},\"n\":{},\"k\":{},\"q\":{},\"t\":{},\"d\":{},\"tenant\":\"{}\",\"type\":\"{}\",\"pad\":\"",
        f.stream, f.seq, f.key_idx, f.key_seq, f.t_us, f.due_us, f.tenant, f.kind
    );
    let fixed = s.len() + 2;
    if target > fixed {
        s.push_str(pad.slice(target - fixed, h));
    }
    s.push_str("\"}");
    s
}

/// Constant-spacing (or Poisson) plan against a single queue/stream.
pub struct RatePlan {
    pub stream: Arc<Stream>,
    pub queue: Arc<str>,
    /// `(length_s, rate)` stages played back to back.
    pub stages: Vec<(f64, f64)>,
    pub body_bytes: usize,
    pub keys: usize,
    pub poisson: bool,
    pub start_us: u64,
    pub pad: Arc<PadPool>,
    pub seed: u64,
    pub delay_ms: u64,
    /// Publish to this fan-out group; bodies are tagged for per-member counting.
    pub group: Option<Arc<str>>,
    rng: Rng,
    at_s: f64,
    seq: u64,
    key_seq: Vec<u32>,
}

impl RatePlan {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: Arc<Stream>,
        queue: &str,
        rate: f64,
        duration: Duration,
        body_bytes: usize,
        keys: usize,
        poisson: bool,
        start_us: u64,
        pad: Arc<PadPool>,
        seed: u64,
    ) -> Self {
        Self::staged(
            stream,
            queue,
            vec![(duration.as_secs_f64(), rate)],
            body_bytes,
            keys,
            poisson,
            start_us,
            pad,
            seed,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn staged(
        stream: Arc<Stream>,
        queue: &str,
        stages: Vec<(f64, f64)>,
        body_bytes: usize,
        keys: usize,
        poisson: bool,
        start_us: u64,
        pad: Arc<PadPool>,
        seed: u64,
    ) -> Self {
        Self {
            stream,
            queue: queue.into(),
            stages,
            body_bytes,
            keys,
            poisson,
            start_us,
            pad,
            seed,
            delay_ms: 0,
            group: None,
            rng: Rng::new(seed),
            at_s: 0.0,
            seq: 0,
            key_seq: vec![0; keys],
        }
    }

    /// Messages this plan will produce (upper bound), for ledger sizing.
    pub fn expected(rate: f64, duration: Duration) -> usize {
        (rate * duration.as_secs_f64() * 1.05) as usize + 64
    }

    fn total_s(&self) -> f64 {
        self.stages.iter().map(|(d, _)| d).sum()
    }

    fn rate_at(&self, t: f64) -> f64 {
        let mut end = 0.0;
        for (d, r) in &self.stages {
            end += d;
            if t < end {
                return *r;
            }
        }
        self.stages.last().map(|s| s.1).unwrap_or(1.0)
    }
}

impl Plan for RatePlan {
    fn next(&mut self) -> Option<Planned> {
        if self.at_s >= self.total_s() || self.seq as usize >= self.stream.cap {
            return None;
        }
        let at_s = self.at_s;
        let rate = self.rate_at(at_s).max(1e-6);
        self.at_s += if self.poisson {
            self.rng.exp(1.0 / rate)
        } else {
            1.0 / rate
        };
        let seq = self.seq;
        self.seq += 1;
        let h = hash2(self.seed, seq);
        let (key, key_idx, key_seq) = if self.keys > 0 {
            let k = (h % self.keys as u64) as usize;
            self.key_seq[k] += 1;
            (format!("lane-{k}"), k as u32, self.key_seq[k])
        } else {
            (String::new(), 0, 0)
        };
        let t_us = self.start_us + (at_s * 1e6) as u64;
        let mut body = render_body(
            &BodyFields {
                stream: self.stream.id,
                seq,
                key_idx,
                key_seq,
                t_us,
                due_us: t_us + self.delay_ms * 1000,
                tenant: "m0001",
                kind: "bench.event",
            },
            self.body_bytes,
            &self.pad,
            h,
        );
        if self.group.is_some() {
            body = body.replacen("{\"s\":", "{\"f\":", 1);
        }
        Some(Planned {
            at: Duration::from_secs_f64(at_s),
            msg: OutMsg {
                stream: self.stream.clone(),
                seq,
                queue: self.queue.clone(),
                key,
                body,
                delay_ms: self.delay_ms,
                priority: None,
                idem: None,
                group: self.group.clone(),
            },
        })
    }
}

// ---------------------------------------------------------------- fulfillment mix

#[derive(Clone, Debug)]
pub struct Family {
    pub queue: &'static str,
    pub kind: &'static str,
    /// Share of traffic at the daily mean.
    pub weight: f64,
    pub size_median: f64,
    pub size_sigma: f64,
    pub size_max: usize,
    /// FIFO per entity: number of lanes (0 = standard unordered delivery).
    pub lanes: usize,
    /// Zipf exponent over lanes (0 = uniform); hot entities build deep lanes.
    pub lane_skew: f64,
    /// `(priority, weight)`.
    pub priority: &'static [(u8, f64)],
    pub handler_median_ms: f64,
    pub handler_sigma: f64,
    pub transient_permille: u32,
    pub permanent_permille: u32,
    pub parallelism: u32,
    pub max_retries: u32,
    /// Percent of requests that re-send an earlier job with the same idempotency key.
    pub dup_pct: f64,
    /// Percent of jobs scheduled for later, and the delay range (ms).
    pub delayed_pct: f64,
    pub delay_range_ms: (u64, u64),
    /// Extra bursts layered on the daily curve: `(period_s, length_s, multiplier)` of the *compressed day*.
    pub wave: Option<(f64, f64, f64)>,
}

pub const FULFILLMENT: &[Family] = &[
    Family {
        queue: "orders.intake",
        kind: "order.received",
        weight: 18.0,
        size_median: 3200.0,
        size_sigma: 0.7,
        size_max: 120_000,
        lanes: 0,
        lane_skew: 0.0,
        priority: &[(5, 70.0), (8, 20.0), (2, 10.0)],
        handler_median_ms: 40.0,
        handler_sigma: 0.5,
        transient_permille: 5,
        permanent_permille: 1,
        parallelism: 64,
        max_retries: 5,
        dup_pct: 3.0,
        delayed_pct: 0.0,
        delay_range_ms: (0, 0),
        // storefront batch imports land on the hour (a day is 24 "hours")
        wave: Some((1.0 / 24.0, 0.00625, 4.0)),
    },
    Family {
        queue: "orders.lifecycle",
        kind: "order.status",
        weight: 30.0,
        size_median: 650.0,
        size_sigma: 0.4,
        size_max: 8_000,
        lanes: 50_000,
        lane_skew: 0.0,
        priority: &[(5, 100.0)],
        handler_median_ms: 25.0,
        handler_sigma: 0.4,
        transient_permille: 3,
        permanent_permille: 0,
        parallelism: 128,
        max_retries: 5,
        dup_pct: 0.5,
        delayed_pct: 0.0,
        delay_range_ms: (0, 0),
        wave: None,
    },
    Family {
        queue: "inventory.adjust",
        kind: "stock.adjusted",
        weight: 22.0,
        size_median: 420.0,
        size_sigma: 0.3,
        size_max: 4_000,
        lanes: 20_000,
        lane_skew: 1.05,
        priority: &[(5, 100.0)],
        handler_median_ms: 15.0,
        handler_sigma: 0.5,
        transient_permille: 2,
        permanent_permille: 0,
        parallelism: 64,
        max_retries: 5,
        dup_pct: 1.0,
        delayed_pct: 0.0,
        delay_range_ms: (0, 0),
        wave: None,
    },
    Family {
        queue: "changelog.process",
        kind: "entity.changed",
        weight: 12.0,
        size_median: 220.0,
        size_sigma: 0.2,
        size_max: 1_000,
        lanes: 0,
        lane_skew: 0.0,
        priority: &[(5, 100.0)],
        handler_median_ms: 120.0,
        handler_sigma: 0.6,
        transient_permille: 5,
        permanent_permille: 0,
        parallelism: 32,
        max_retries: 8,
        dup_pct: 35.0,
        delayed_pct: 0.0,
        delay_range_ms: (0, 0),
        wave: None,
    },
    Family {
        queue: "carrier.label",
        kind: "label.requested",
        weight: 6.0,
        size_median: 2100.0,
        size_sigma: 0.5,
        size_max: 40_000,
        lanes: 0,
        lane_skew: 0.0,
        priority: &[(5, 60.0), (8, 40.0)],
        handler_median_ms: 700.0,
        handler_sigma: 0.6,
        transient_permille: 30,
        permanent_permille: 3,
        parallelism: 128,
        max_retries: 6,
        dup_pct: 1.0,
        delayed_pct: 0.0,
        delay_range_ms: (0, 0),
        // carrier pickup cut-off wave, mid-afternoon
        wave: Some((1.0, 0.06, 3.5)),
    },
    Family {
        queue: "notify.partner",
        kind: "partner.event",
        weight: 6.0,
        size_median: 1100.0,
        size_sigma: 0.5,
        size_max: 30_000,
        lanes: 0,
        lane_skew: 0.0,
        priority: &[(5, 100.0)],
        handler_median_ms: 250.0,
        handler_sigma: 0.9,
        transient_permille: 50,
        permanent_permille: 5,
        parallelism: 64,
        max_retries: 8,
        dup_pct: 0.0,
        delayed_pct: 0.0,
        delay_range_ms: (0, 0),
        wave: None,
    },
    Family {
        queue: "email.send",
        kind: "email.queued",
        weight: 4.0,
        size_median: 1500.0,
        size_sigma: 0.6,
        size_max: 60_000,
        lanes: 0,
        lane_skew: 0.0,
        priority: &[(2, 100.0)],
        handler_median_ms: 80.0,
        handler_sigma: 0.7,
        transient_permille: 10,
        permanent_permille: 4,
        parallelism: 16,
        max_retries: 4,
        dup_pct: 0.0,
        delayed_pct: 10.0,
        delay_range_ms: (500, 5_000),
        wave: None,
    },
    Family {
        queue: "docs.render",
        kind: "document.render",
        weight: 1.5,
        size_median: 60_000.0,
        size_sigma: 0.8,
        size_max: 900_000,
        lanes: 0,
        lane_skew: 0.0,
        priority: &[(5, 100.0)],
        handler_median_ms: 400.0,
        handler_sigma: 0.6,
        transient_permille: 5,
        permanent_permille: 0,
        parallelism: 48,
        max_retries: 4,
        dup_pct: 0.0,
        delayed_pct: 0.0,
        delay_range_ms: (0, 0),
        wave: Some((1.0, 0.06, 3.0)),
    },
    Family {
        queue: "reports.build",
        kind: "report.requested",
        weight: 0.5,
        size_median: 320.0,
        size_sigma: 0.3,
        size_max: 2_000,
        lanes: 0,
        lane_skew: 0.0,
        priority: &[(2, 100.0)],
        handler_median_ms: 3000.0,
        handler_sigma: 0.5,
        transient_permille: 10,
        permanent_permille: 0,
        parallelism: 40,
        max_retries: 3,
        dup_pct: 0.0,
        delayed_pct: 60.0,
        delay_range_ms: (2_000, 15_000),
        wave: None,
    },
];

/// Piecewise-linear daily load shape, as `(fraction of day, multiplier of the mean)`.
pub const DAY_CURVE: &[(f64, f64)] = &[
    (0.00, 0.25),
    (0.25, 0.35),
    (0.37, 1.00),
    (0.50, 1.35),
    (0.62, 1.20),
    (0.75, 0.70),
    (0.90, 0.40),
    (1.00, 0.25),
];

pub fn day_multiplier(frac: f64) -> f64 {
    let f = frac.rem_euclid(1.0);
    for w in DAY_CURVE.windows(2) {
        let ((x0, y0), (x1, y1)) = (w[0], w[1]);
        if f >= x0 && f <= x1 {
            return y0 + (y1 - y0) * (f - x0) / (x1 - x0);
        }
    }
    1.0
}

/// Arrival rate multiplier for a family at `t` seconds into a compressed day.
pub fn family_multiplier(f: &Family, t: f64, day_s: f64) -> f64 {
    let frac = t / day_s;
    let mut m = day_multiplier(frac);
    if let Some((period, length, mult)) = f.wave {
        // `period` and `length` are fractions of a day.
        let phase = frac.rem_euclid(period);
        let centre = period * 0.6;
        if (phase - centre).abs() <= length / 2.0 {
            m *= mult;
        }
    }
    m
}

/// Time-averaged multiplier over a full day (used to normalise family weights).
pub fn mean_multiplier(f: &Family) -> f64 {
    let n = 2000;
    (0..n)
        .map(|i| family_multiplier(f, (i as f64 + 0.5) / n as f64, 1.0))
        .sum::<f64>()
        / n as f64
}

struct FamilyGen {
    fam: &'static Family,
    stream: Arc<Stream>,
    rng: Rng,
    rate_mean: f64,
    t: f64,
    seq: u64,
    lane_seq: Vec<u32>,
    zipf: Option<Zipf>,
    pending_dups: Vec<(u64, u32, u32, String, String)>,
    salt: u64,
}

/// Merged multi-family arrival process with the tenant skew, FIFO lanes,
/// duplicates, delays, priorities, and day curve described by [`FULFILLMENT`].
pub struct MixPlan {
    gens: Vec<FamilyGen>,
    heap: BinaryHeap<Reverse<(u64, usize)>>,
    day_s: f64,
    run_s: f64,
    start_us: u64,
    pad: Arc<PadPool>,
    tenants: Zipf,
    seed: u64,
}

pub const TENANTS: usize = 300;

impl MixPlan {
    /// `mean_rate` is the all-families mean msg/s over a day; `streams` pairs
    /// each family (same order as [`FULFILLMENT`]) with its ledger stream.
    pub fn new(
        streams: Vec<Arc<Stream>>,
        mean_rate: f64,
        day_s: f64,
        run_s: f64,
        start_us: u64,
        pad: Arc<PadPool>,
        seed: u64,
    ) -> Self {
        let total_w: f64 = FULFILLMENT.iter().map(|f| f.weight).sum();
        let mut gens = Vec::new();
        let mut heap = BinaryHeap::new();
        for (i, (fam, stream)) in FULFILLMENT.iter().zip(streams).enumerate() {
            let mut rng = Rng::new(seed).fork(i as u64 + 1);
            let share = fam.weight / total_w;
            // Wave families run hotter during waves, so normalise to keep the mean.
            let rate_mean = mean_rate * share / mean_multiplier(fam).max(1e-9);
            let first = rng.exp(1.0 / (rate_mean * family_multiplier(fam, 0.0, day_s)).max(1e-6));
            heap.push(Reverse(((first * 1e9) as u64, i)));
            gens.push(FamilyGen {
                fam,
                stream,
                rng,
                rate_mean,
                t: first,
                seq: 0,
                lane_seq: vec![0; fam.lanes],
                zipf: (fam.lanes > 0 && fam.lane_skew > 0.0)
                    .then(|| Zipf::new(fam.lanes, fam.lane_skew)),
                pending_dups: Vec::new(),
                salt: hash2(seed, i as u64 + 100),
            });
        }
        Self {
            gens,
            heap,
            day_s,
            run_s,
            start_us,
            pad,
            tenants: Zipf::new(TENANTS, 1.05),
            seed,
        }
    }

    /// Expected message count per family over `run_s` (ledger capacity).
    pub fn capacity(mean_rate: f64, day_s: f64, run_s: f64) -> Vec<usize> {
        let total_w: f64 = FULFILLMENT.iter().map(|f| f.weight).sum();
        FULFILLMENT
            .iter()
            .map(|fam| {
                let share = fam.weight / total_w;
                let rate_mean = mean_rate * share / mean_multiplier(fam).max(1e-9);
                let n = 600;
                let integral: f64 = (0..n)
                    .map(|i| {
                        family_multiplier(fam, (i as f64 + 0.5) / n as f64 * run_s, day_s)
                            * rate_mean
                    })
                    .sum::<f64>()
                    * run_s
                    / n as f64;
                (integral * 1.25) as usize + 256
            })
            .collect()
    }
}

impl Plan for MixPlan {
    fn next(&mut self) -> Option<Planned> {
        let Reverse((_, gi)) = self.heap.pop()?;
        let tenants = &self.tenants;
        let g = &mut self.gens[gi];
        let at_s = g.t;
        if at_s >= self.run_s {
            return self.next();
        }
        // Schedule this family's next arrival at the rate prevailing now.
        let rate = (g.rate_mean * family_multiplier(g.fam, at_s, self.day_s)).max(1e-6);
        g.t += g.rng.exp(1.0 / rate);
        self.heap.push(Reverse(((g.t * 1e9) as u64, gi)));

        let fam = g.fam;
        let t_us = self.start_us + (at_s * 1e6) as u64;

        // Re-send of an earlier job (idempotent client retry / enqueue-often pattern).
        if fam.dup_pct > 0.0 && !g.pending_dups.is_empty() && g.rng.f64() * 100.0 < fam.dup_pct {
            let i = g.rng.below(g.pending_dups.len() as u64) as usize;
            let (seq, key_idx, key_seq, body, idem) = g.pending_dups[i].clone();
            let key = if fam.lanes > 0 {
                format!("e{key_idx}")
            } else {
                String::new()
            };
            let _ = (key_seq, &body);
            return Some(Planned {
                at: Duration::from_secs_f64(at_s),
                msg: OutMsg {
                    stream: g.stream.clone(),
                    seq,
                    queue: fam.queue.into(),
                    key,
                    body,
                    delay_ms: 0,
                    priority: None,
                    idem: Some(idem),
                    group: None,
                },
            });
        }

        if g.seq as usize >= g.stream.cap {
            return self.next();
        }
        let seq = g.seq;
        g.seq += 1;
        let h = hash2(g.salt, seq);
        let tenant_idx = tenants.sample_unit(crate::rng::unit(hash2(h, 1)));
        let tenant = format!("m{tenant_idx:04}");
        let size = lognormal_from_hash(hash2(h, 2), fam.size_median, fam.size_sigma)
            .clamp(120.0, fam.size_max as f64) as usize;
        let prio = {
            let weights: Vec<f64> = fam.priority.iter().map(|p| p.1).collect();
            fam.priority[weighted_index(&weights, crate::rng::unit(hash2(h, 3)))].0
        };
        let (key, key_idx, key_seq) = if fam.lanes > 0 {
            let lane = match &g.zipf {
                Some(z) => z.sample_unit(crate::rng::unit(hash2(h, 4))),
                None => (hash2(h, 4) % fam.lanes as u64) as usize,
            };
            g.lane_seq[lane] += 1;
            (format!("e{lane}"), lane as u32, g.lane_seq[lane])
        } else {
            (String::new(), 0, 0)
        };
        let delay_ms =
            if fam.delayed_pct > 0.0 && crate::rng::unit(hash2(h, 5)) * 100.0 < fam.delayed_pct {
                let (lo, hi) = fam.delay_range_ms;
                lo + (crate::rng::unit(hash2(h, 6)) * (hi - lo) as f64) as u64
            } else {
                0
            };
        let body = render_body(
            &BodyFields {
                stream: g.stream.id,
                seq,
                key_idx,
                key_seq,
                t_us,
                due_us: t_us + delay_ms * 1000,
                tenant: &tenant,
                kind: fam.kind,
            },
            size,
            &self.pad,
            h,
        );
        let idem = format!("{}:{seq}", fam.queue);
        if fam.dup_pct > 0.0 && g.pending_dups.len() < 256 {
            g.pending_dups
                .push((seq, key_idx, key_seq, body.clone(), idem.clone()));
        } else if fam.dup_pct > 0.0 {
            let slot = (h % 256) as usize;
            g.pending_dups[slot] = (seq, key_idx, key_seq, body.clone(), idem.clone());
        }
        let _ = self.seed;
        Some(Planned {
            at: Duration::from_secs_f64(at_s),
            msg: OutMsg {
                stream: g.stream.clone(),
                seq,
                queue: fam.queue.into(),
                key,
                body,
                delay_ms,
                priority: Some(prio),
                idem: (fam.dup_pct > 0.0).then_some(idem),
                group: None,
            },
        })
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::verify::Verifier;

    fn mix(seed: u64, run_s: f64) -> (Vec<Arc<Stream>>, MixPlan) {
        let v = Verifier::default();
        let caps = MixPlan::capacity(400.0, 60.0, run_s);
        let streams: Vec<_> = FULFILLMENT
            .iter()
            .zip(caps)
            .map(|(f, cap)| v.register(f.queue, cap, f.lanes, f.lanes > 0))
            .collect();
        let plan = MixPlan::new(
            streams.clone(),
            400.0,
            60.0,
            run_s,
            0,
            PadPool::new(seed),
            seed,
        );
        (streams, plan)
    }

    fn drain(mut p: MixPlan, n: usize) -> Vec<(Duration, String, u64, String)> {
        let mut out = Vec::new();
        while let Some(x) = p.next() {
            out.push((x.at, x.msg.queue.to_string(), x.msg.seq, x.msg.body));
            if out.len() >= n {
                break;
            }
        }
        out
    }

    #[test]
    fn mix_is_deterministic() {
        let a = drain(mix(5, 60.0).1, 3000);
        let b = drain(mix(5, 60.0).1, 3000);
        assert_eq!(a, b);
        let c = drain(mix(6, 60.0).1, 3000);
        assert_ne!(a, c);
    }

    #[test]
    fn mix_arrivals_are_ordered_and_bounded() {
        let msgs = drain(mix(1, 60.0).1, usize::MAX);
        assert!(msgs.len() > 5_000, "got {}", msgs.len());
        assert!(
            msgs.windows(2).all(|w| w[0].0 <= w[1].0),
            "arrivals must be time-sorted"
        );
        assert!(msgs.last().unwrap().0 < Duration::from_secs(60));
    }

    #[test]
    fn mix_follows_family_weights() {
        let msgs = drain(mix(2, 60.0).1, usize::MAX);
        let total = msgs.len() as f64;
        let lifecycle = msgs.iter().filter(|m| m.1 == "orders.lifecycle").count() as f64 / total;
        let reports = msgs.iter().filter(|m| m.1 == "reports.build").count() as f64 / total;
        assert!(
            (lifecycle - 0.30).abs() < 0.05,
            "lifecycle share {lifecycle}"
        );
        assert!(reports < 0.02, "reports share {reports}");
    }

    #[test]
    fn bodies_carry_verification_header() {
        let msgs = drain(mix(3, 30.0).1, 200);
        for (_, _, _, body) in msgs {
            let v: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
            assert!(v["s"].is_u64() && v["n"].is_u64() && v["t"].is_u64());
        }
    }

    #[test]
    fn day_curve_interpolates() {
        assert!((day_multiplier(0.0) - 0.25).abs() < 1e-9);
        assert!((day_multiplier(0.5) - 1.35).abs() < 1e-9);
        assert!((day_multiplier(1.0) - day_multiplier(0.0)).abs() < 1e-9);
        let mid = day_multiplier(0.435);
        assert!(mid > 1.0 && mid < 1.35);
    }

    #[test]
    fn rate_plan_spacing_and_keys() {
        let v = Verifier::default();
        let s = v.register("r", 1000, 4, true);
        let mut p = RatePlan::new(
            s,
            "q",
            100.0,
            Duration::from_secs(1),
            500,
            4,
            false,
            0,
            PadPool::new(1),
            7,
        );
        let all: Vec<_> = std::iter::from_fn(|| p.next()).collect();
        assert_eq!(all.len(), 100);
        assert!((all[10].at.as_secs_f64() - 0.10).abs() < 1e-9);
        assert!(all.iter().all(|m| m.msg.key.starts_with("lane-")));
        assert!(all
            .iter()
            .all(|m| m.msg.body.len() >= 480 && m.msg.body.len() <= 520));
    }

    #[test]
    fn body_target_size_is_respected() {
        let pad = PadPool::new(1);
        let f = BodyFields {
            stream: 1,
            seq: 2,
            key_idx: 0,
            key_seq: 0,
            t_us: 5,
            due_us: 5,
            tenant: "m0001",
            kind: "x",
        };
        for target in [200usize, 1024, 65_536, 500_000] {
            let b = render_body(&f, target, &pad, 99);
            assert!(b.len().abs_diff(target) < 8, "{target} -> {}", b.len());
            serde_json::from_str::<serde_json::Value>(&b).unwrap();
        }
    }
}
