//! Deterministic randomness.
//!
//! The suite must replay the exact same workload from a seed on any machine and
//! any toolchain, so it does not use `rand` (its `StdRng` stream is explicitly
//! not stable across versions). SplitMix64 + xoshiro256++ are tiny, fast, and
//! fixed forever.

/// SplitMix64 step; also used as a stateless hash of `(seed, index)`.
pub fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Stateless hash of two words. Message `i` of a stream gets the same
/// attributes regardless of how many workers generate it or in what order.
pub fn hash2(a: u64, b: u64) -> u64 {
    splitmix(splitmix(a) ^ b.rotate_left(17))
}

/// Uniform `[0, 1)` from a hash word.
pub fn unit(h: u64) -> f64 {
    (h >> 11) as f64 / (1u64 << 53) as f64
}

#[derive(Clone, Debug)]
pub struct Rng {
    s: [u64; 4],
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        let mut x = seed;
        let mut s = [0u64; 4];
        for slot in &mut s {
            x = splitmix(x);
            *slot = x;
        }
        Self { s }
    }

    /// Derive an independent stream, e.g. one per scenario or per tenant.
    pub fn fork(&self, label: u64) -> Self {
        Self::new(hash2(self.s[0] ^ self.s[3], label))
    }

    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[0]
            .wrapping_add(self.s[3])
            .rotate_left(23)
            .wrapping_add(self.s[0]);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    pub fn f64(&mut self) -> f64 {
        unit(self.next_u64())
    }

    pub fn below(&mut self, n: u64) -> u64 {
        if n <= 1 {
            0
        } else {
            self.next_u64() % n
        }
    }

    /// Exponential with the given mean (Poisson inter-arrival gaps).
    pub fn exp(&mut self, mean: f64) -> f64 {
        -mean * (1.0 - self.f64()).ln()
    }
}

/// Log-normal sample driven by a hash word instead of generator state.
pub fn lognormal_from_hash(h: u64, median: f64, sigma: f64) -> f64 {
    let u1 = (1.0 - unit(h)).max(f64::MIN_POSITIVE);
    let u2 = unit(splitmix(h));
    let n = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
    median * (sigma * n).exp()
}

/// Zipf-distributed index in `0..n` (index 0 is the hottest).
#[derive(Clone, Debug)]
pub struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    pub fn new(n: usize, exponent: f64) -> Self {
        let n = n.max(1);
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0;
        for rank in 1..=n {
            acc += 1.0 / (rank as f64).powf(exponent);
            cdf.push(acc);
        }
        for v in &mut cdf {
            *v /= acc;
        }
        Self { cdf }
    }

    pub fn sample_unit(&self, u: f64) -> usize {
        match self
            .cdf
            .binary_search_by(|p| p.partial_cmp(&u).unwrap_or(std::cmp::Ordering::Less))
        {
            Ok(i) | Err(i) => i.min(self.cdf.len() - 1),
        }
    }
}

/// Weighted pick over a small table.
pub fn weighted_index(weights: &[f64], u: f64) -> usize {
    let total: f64 = weights.iter().sum();
    let mut target = u * total;
    for (i, w) in weights.iter().enumerate() {
        if target < *w {
            return i;
        }
        target -= *w;
    }
    weights.len().saturating_sub(1)
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    const GOLDEN_SEED_42: [u64; 3] = [
        14364114511653964483,
        5454468825661541484,
        330174794094209790,
    ];
    const GOLDEN_HASH_1_2: u64 = 16954145932713642056;

    #[test]
    fn stream_is_stable() {
        // Golden values: if these change, every stored baseline is invalidated.
        let mut r = Rng::new(42);
        let got: Vec<u64> = (0..3).map(|_| r.next_u64()).collect();
        assert_eq!(got, GOLDEN_SEED_42);
        assert_eq!(hash2(1, 2), GOLDEN_HASH_1_2);
    }

    #[test]
    fn same_seed_same_stream() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(1);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        let mut c = Rng::new(2);
        assert_ne!(Rng::new(1).next_u64(), c.next_u64());
    }

    #[test]
    fn hash_is_order_independent() {
        assert_eq!(hash2(5, 9), hash2(5, 9));
        assert_ne!(hash2(5, 9), hash2(9, 5));
    }

    #[test]
    fn zipf_is_skewed() {
        let z = Zipf::new(1000, 1.1);
        let mut r = Rng::new(3);
        let mut hot = 0;
        for _ in 0..20_000 {
            if z.sample_unit(r.f64()) < 10 {
                hot += 1;
            }
        }
        assert!(hot > 8_000, "top 1% should take a large share, got {hot}");
    }

    #[test]
    fn lognormal_median_holds() {
        let mut v: Vec<f64> = (0..20_001u64)
            .map(|i| lognormal_from_hash(hash2(9, i), 100.0, 0.8))
            .collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert!((v[10_000] - 100.0).abs() < 8.0);
    }

    #[test]
    fn exp_mean_holds() {
        let mut r = Rng::new(11);
        let m: f64 = (0..50_000).map(|_| r.exp(2.0)).sum::<f64>() / 50_000.0;
        assert!((m - 2.0).abs() < 0.1);
    }
}
