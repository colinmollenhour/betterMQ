//! Simple in-process rate limiter for auth-sensitive endpoints.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<Mutex<State>>,
    max: u32,
    window: Duration,
}

struct State {
    hits: HashMap<String, Vec<Instant>>,
}

impl RateLimiter {
    pub fn new(max: u32, window: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State {
                hits: HashMap::new(),
            })),
            max,
            window,
        }
    }

    /// Returns true if the request is allowed.
    pub fn check(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut state = self.inner.lock();
        let entries = state.hits.entry(key.to_string()).or_default();
        entries.retain(|t| now.duration_since(*t) < self.window);
        if entries.len() as u32 >= self.max {
            return false;
        }
        entries.push(now);
        // Opportunistic cleanup to bound map size.
        if state.hits.len() > 10_000 {
            state.hits.retain(|_, v| {
                v.retain(|t| now.duration_since(*t) < self.window);
                !v.is_empty()
            });
        }
        true
    }
}

fn trust_proxy() -> bool {
    matches!(
        std::env::var("BETTERMQ_TRUST_PROXY")
            .ok()
            .as_deref()
            .map(str::trim),
        Some("1") | Some("true") | Some("TRUE") | Some("yes")
    )
}

/// `addr/prefix` (or a bare address) from `BETTERMQ_TRUSTED_PROXIES`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cidr {
    addr: IpAddr,
    prefix: u8,
}

impl Cidr {
    fn parse(raw: &str) -> Option<Self> {
        let raw = raw.trim();
        let (addr, prefix) = match raw.split_once('/') {
            Some((a, p)) => (a.parse::<IpAddr>().ok()?, Some(p.parse::<u8>().ok()?)),
            None => (raw.parse::<IpAddr>().ok()?, None),
        };
        let max = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = prefix.unwrap_or(max);
        (prefix <= max).then_some(Self { addr, prefix })
    }

    fn contains(&self, ip: IpAddr) -> bool {
        fn masked(bits: u128, width: u32, prefix: u8) -> u128 {
            if prefix == 0 {
                0
            } else {
                bits >> (width - u32::from(prefix))
            }
        }
        match (self.addr, canonical(ip)) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                masked(u32::from(net).into(), 32, self.prefix)
                    == masked(u32::from(ip).into(), 32, self.prefix)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                masked(u128::from(net), 128, self.prefix)
                    == masked(u128::from(ip), 128, self.prefix)
            }
            _ => false,
        }
    }
}

fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
        v4 => v4,
    }
}

/// Proxies whose `X-Forwarded-For` entries are believed. Defaults to loopback
/// and private ranges (a reverse proxy on the host or the container network).
fn trusted_proxies() -> Vec<Cidr> {
    let configured: Vec<Cidr> = std::env::var("BETTERMQ_TRUSTED_PROXIES")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .filter_map(|s| {
            let cidr = Cidr::parse(s);
            if cidr.is_none() {
                tracing::warn!(entry = %s.trim(), "ignoring invalid BETTERMQ_TRUSTED_PROXIES entry");
            }
            cidr
        })
        .collect();
    if !configured.is_empty() {
        return configured;
    }
    [
        "127.0.0.0/8",
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "::1/128",
        "fc00::/7",
    ]
    .iter()
    .filter_map(|s| Cidr::parse(s))
    .collect()
}

/// Rate-limit key for a request. Without `BETTERMQ_TRUST_PROXY` this is the
/// socket peer. With it, `X-Forwarded-For` is honoured only when the peer is a
/// trusted proxy, and the client is the right-most hop that is not itself a
/// trusted proxy (left-most entries are client-controlled).
pub fn client_ip_key(headers: &axum::http::HeaderMap, peer: Option<IpAddr>) -> String {
    let trusted = if trust_proxy() {
        trusted_proxies()
    } else {
        Vec::new()
    };
    client_ip_key_with(headers, peer, &trusted)
}

fn client_ip_key_with(
    headers: &axum::http::HeaderMap,
    peer: Option<IpAddr>,
    trusted: &[Cidr],
) -> String {
    let is_trusted = |ip: IpAddr| trusted.iter().any(|c| c.contains(ip));
    if let Some(peer) = peer.filter(|p| is_trusted(*p)) {
        let hops: Vec<IpAddr> = headers
            .get_all("x-forwarded-for")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .filter_map(|hop| hop.trim().parse::<IpAddr>().ok())
            .collect();
        if let Some(client) = hops.iter().rev().find(|ip| !is_trusted(**ip)) {
            return canonical(*client).to_string();
        }
        return canonical(hops.first().copied().unwrap_or(peer)).to_string();
    }
    peer.map(|ip| canonical(ip).to_string())
        .unwrap_or_else(|| "unknown".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_burst() {
        let lim = RateLimiter::new(2, Duration::from_secs(60));
        assert!(lim.check("a"));
        assert!(lim.check("a"));
        assert!(!lim.check("a"));
        assert!(lim.check("b"));
    }

    fn xff(value: &str) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-forwarded-for", value.parse().unwrap());
        headers
    }

    fn ip(s: &str) -> Option<IpAddr> {
        Some(s.parse().unwrap())
    }

    #[test]
    fn without_proxy_trust_the_socket_peer_is_the_key() {
        let headers = xff("1.2.3.4");
        assert_eq!(
            client_ip_key_with(&headers, ip("203.0.113.9"), &[]),
            "203.0.113.9"
        );
        assert_eq!(client_ip_key_with(&headers, None, &[]), "unknown");
    }

    #[test]
    fn forwarded_for_is_ignored_from_untrusted_peers() {
        let trusted = [Cidr::parse("10.0.0.0/8").unwrap()];
        let headers = xff("1.2.3.4");
        assert_eq!(
            client_ip_key_with(&headers, ip("203.0.113.9"), &trusted),
            "203.0.113.9"
        );
    }

    #[test]
    fn spoofed_left_hops_do_not_change_the_key() {
        let trusted = [Cidr::parse("10.0.0.0/8").unwrap()];
        // Client 198.51.100.7 prepended a fake hop; the proxy appended the real one.
        let a = client_ip_key_with(&xff("6.6.6.6, 198.51.100.7"), ip("10.0.0.2"), &trusted);
        let b = client_ip_key_with(&xff("7.7.7.7, 198.51.100.7"), ip("10.0.0.2"), &trusted);
        assert_eq!(a, "198.51.100.7");
        assert_eq!(a, b);
        // Chained trusted proxies are skipped.
        assert_eq!(
            client_ip_key_with(&xff("198.51.100.7, 10.0.0.5"), ip("10.0.0.2"), &trusted),
            "198.51.100.7"
        );
    }

    #[test]
    fn cidr_matching() {
        let net = Cidr::parse("172.16.0.0/12").unwrap();
        assert!(net.contains("172.31.255.1".parse().unwrap()));
        assert!(!net.contains("172.32.0.1".parse().unwrap()));
        assert!(net.contains("::ffff:172.16.0.1".parse().unwrap()));
        assert!(Cidr::parse("::1").unwrap().contains("::1".parse().unwrap()));
        assert!(Cidr::parse("0.0.0.0/0")
            .unwrap()
            .contains("8.8.8.8".parse().unwrap()));
        assert!(Cidr::parse("10.0.0.0/33").is_none());
    }
}
