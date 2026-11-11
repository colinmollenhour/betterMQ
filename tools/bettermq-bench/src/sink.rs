//! In-process webhook receiver standing in for customers' endpoints.
//!
//! Each destination (`/d/{idx}`) has its own latency model, failure rates and
//! a runtime mode switch (normal / outage / hang) so scenarios can script
//! downstream behaviour. The sink also does the correctness bookkeeping:
//! delivery latency, duplicates, ordering, loss (via [`crate::verify`]),
//! observed handler concurrency, and cron fire lateness.

use crate::rng::{hash2, lognormal_from_hash, unit};
use crate::stats::{Clock, Shared};
use crate::verify::Verifier;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Clone, Debug)]
pub struct DestCfg {
    pub name: String,
    /// Median handler latency; 0 answers immediately.
    pub median_ms: f64,
    /// Sigma of the underlying normal for log-normal latency (tail heaviness).
    pub sigma: f64,
    /// Chance (per mille) of a retryable 503 per attempt.
    pub transient_permille: u32,
    /// Chance (per mille) of a permanent 422 (goes straight to the DLQ).
    pub permanent_permille: u32,
    /// The parallelism configured on the broker queue feeding this destination
    /// (0 = unchecked); the sink flags a violation if it observes more.
    pub configured_parallelism: u32,
    /// `Retry-After` seconds sent with 503/429 responses (0 = none).
    pub retry_after_s: u32,
}

impl DestCfg {
    pub fn instant(name: &str) -> Self {
        Self {
            name: name.into(),
            median_ms: 0.0,
            sigma: 0.0,
            transient_permille: 0,
            permanent_permille: 0,
            configured_parallelism: 0,
            retry_after_s: 0,
        }
    }

    pub fn latency(mut self, median_ms: f64, sigma: f64) -> Self {
        self.median_ms = median_ms;
        self.sigma = sigma;
        self
    }

    pub fn failures(mut self, transient_permille: u32, permanent_permille: u32) -> Self {
        self.transient_permille = transient_permille;
        self.permanent_permille = permanent_permille;
        self
    }

    pub fn parallelism(mut self, p: u32) -> Self {
        self.configured_parallelism = p;
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Normal = 0,
    /// Every request answers 503 immediately.
    Fail503 = 1,
    /// Requests are accepted and never answered (until the broker times out).
    Hang = 2,
}

pub struct Dest {
    pub cfg: DestCfg,
    mode: AtomicU8,
    median_us: AtomicU64,
    slow_milli: AtomicU32,
    inflight: AtomicU32,
    pub max_inflight: AtomicU32,
    pub recv: AtomicU64,
    pub ok: AtomicU64,
    pub fail_503: AtomicU64,
    pub fail_422: AtomicU64,
    pub rejected_mode: AtomicU64,
}

pub struct CronInfo {
    pub anchor_ms: i64,
    pub period_ms: i64,
    pub fires: AtomicU64,
}

pub struct SinkState {
    shared: Arc<Shared>,
    verifier: Arc<Verifier>,
    clock: Clock,
    seed: u64,
    dests: RwLock<Vec<Arc<Dest>>>,
    crons: RwLock<HashMap<u32, Arc<CronInfo>>>,
    stop: Notify,
    /// Fired when a hung destination recovers: stuck requests fail fast, as
    /// they would once a real endpoint resets its connections.
    release: Notify,
}

pub struct Sink {
    pub addr: SocketAddr,
    state: Arc<SinkState>,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Deserialize)]
struct Hdr {
    s: Option<u16>,
    /// Fan-out stream: deliveries are counted per member, not tracked per message.
    f: Option<u16>,
    n: Option<u64>,
    k: Option<u32>,
    q: Option<u32>,
    d: Option<u64>,
    cr: Option<u32>,
}

struct InflightGuard<'a>(&'a Dest, &'a Shared);

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::Relaxed);
        self.1.c.sink_inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

fn now_wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

async fn handle(State(st): State<Arc<SinkState>>, Path(idx): Path<usize>, body: Bytes) -> Response {
    let dest = st.dests.read().get(idx).cloned();
    let Some(dest) = dest else {
        st.shared.c.sink_malformed.fetch_add(1, Ordering::Relaxed);
        return StatusCode::NOT_FOUND.into_response();
    };
    let c = &st.shared.c;
    c.sink_recv.fetch_add(1, Ordering::Relaxed);
    dest.recv.fetch_add(1, Ordering::Relaxed);
    let now = dest.inflight.fetch_add(1, Ordering::Relaxed) + 1;
    dest.max_inflight.fetch_max(now, Ordering::Relaxed);
    c.sink_inflight.fetch_add(1, Ordering::Relaxed);
    let _guard = InflightGuard(&dest, &st.shared);

    let hdr: Option<Hdr> = serde_json::from_slice(&body).ok();
    let fan = hdr
        .as_ref()
        .and_then(|h| h.f)
        .and_then(|f| st.verifier.get(f));
    let (stream, seq, attempt) = match hdr.as_ref() {
        // Fan-out message: the member is the destination (destinations of a
        // fan-out scenario are registered first, in member order).
        Some(Hdr {
            f: Some(_),
            n: Some(n),
            ..
        }) => match &fan {
            Some(fan) => (None, *n, fan.fan_attempt(*n, idx)),
            None => {
                c.sink_malformed.fetch_add(1, Ordering::Relaxed);
                (None, 0, 1)
            }
        },
        Some(Hdr { f: Some(_), .. }) => {
            c.sink_malformed.fetch_add(1, Ordering::Relaxed);
            (None, 0, 1)
        }
        Some(Hdr {
            s: Some(s),
            n: Some(n),
            ..
        }) => match st.verifier.get(*s) {
            Some(stream) => {
                let attempt = stream.attempt(*n);
                (Some(stream), *n, attempt)
            }
            None => {
                c.sink_malformed.fetch_add(1, Ordering::Relaxed);
                (None, 0, 1)
            }
        },
        Some(Hdr { cr: Some(_), .. }) => (None, 0, 1),
        _ => {
            c.sink_malformed.fetch_add(1, Ordering::Relaxed);
            (None, 0, 1)
        }
    };

    match dest.mode.load(Ordering::Relaxed) {
        1 => {
            dest.rejected_mode.fetch_add(1, Ordering::Relaxed);
            c.sink_fail_transient.fetch_add(1, Ordering::Relaxed);
            return reject(StatusCode::SERVICE_UNAVAILABLE, dest.cfg.retry_after_s);
        }
        2 => {
            dest.rejected_mode.fetch_add(1, Ordering::Relaxed);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(300)) => {}
                () = st.stop.notified() => {}
                () = st.release.notified() => {}
            }
            return reject(StatusCode::SERVICE_UNAVAILABLE, 0);
        }
        _ => {}
    }

    // Deterministic per (seed, dest, message, attempt) so reruns inject the
    // same failures at the same messages.
    let key = hash2(
        st.seed ^ (idx as u64).wrapping_mul(0x9E37),
        seq.wrapping_mul(31) + u64::from(attempt),
    );
    let median_us = dest.median_us.load(Ordering::Relaxed);
    if median_us > 0 {
        let slow = f64::from(dest.slow_milli.load(Ordering::Relaxed)) / 1000.0;
        let us = lognormal_from_hash(key, median_us as f64, dest.cfg.sigma) * slow;
        tokio::time::sleep(Duration::from_micros(us.clamp(0.0, 120_000_000.0) as u64)).await;
    }
    let roll = (unit(hash2(key, 0xFA11)) * 1000.0) as u32;
    if roll < dest.cfg.permanent_permille {
        dest.fail_422.fetch_add(1, Ordering::Relaxed);
        c.sink_fail_permanent.fetch_add(1, Ordering::Relaxed);
        if let Some(stream) = &stream {
            stream.mark_perm(seq);
        } else if let Some(fan) = &fan {
            fan.mark_fanout_perm(seq, idx);
        }
        return StatusCode::UNPROCESSABLE_ENTITY.into_response();
    }
    if roll < dest.cfg.permanent_permille + dest.cfg.transient_permille {
        dest.fail_503.fetch_add(1, Ordering::Relaxed);
        c.sink_fail_transient.fetch_add(1, Ordering::Relaxed);
        return reject(StatusCode::SERVICE_UNAVAILABLE, dest.cfg.retry_after_s);
    }

    dest.ok.fetch_add(1, Ordering::Relaxed);
    c.sink_ok.fetch_add(1, Ordering::Relaxed);
    if let (Some(fan), Some(h)) = (&fan, &hdr) {
        if fan.mark_fanout_done(seq, idx) {
            c.sink_dups.fetch_add(1, Ordering::Relaxed);
        } else if let Some(due) = h.d {
            let late = st.clock.now_us().saturating_sub(due);
            if attempt <= 1 {
                st.shared.l.delivery.record_us(late);
                fan.delivery.record_us(late);
            } else {
                st.shared.l.delivery_retried.record_us(late);
            }
        }
    } else if let (Some(stream), Some(h)) = (&stream, &hdr) {
        let dup = stream.mark_done(seq, h.k.unwrap_or(0), h.q.unwrap_or(0), attempt);
        if dup {
            c.sink_dups.fetch_add(1, Ordering::Relaxed);
        } else if let Some(due) = h.d {
            let late = st.clock.now_us().saturating_sub(due);
            if attempt <= 1 {
                st.shared.l.delivery.record_us(late);
                stream.delivery.record_us(late);
            } else {
                st.shared.l.delivery_retried.record_us(late);
            }
        }
    } else if let Some(h) = &hdr {
        if let Some(cr) = h.cr {
            if let Some(info) = st.crons.read().get(&cr) {
                info.fires.fetch_add(1, Ordering::Relaxed);
                c.cron_fires.fetch_add(1, Ordering::Relaxed);
                let late = (now_wall_ms() - info.anchor_ms).rem_euclid(info.period_ms.max(1));
                st.shared.l.cron_jitter.record_us((late * 1000) as u64);
            }
        }
    }
    StatusCode::OK.into_response()
}

fn reject(code: StatusCode, retry_after_s: u32) -> Response {
    let mut resp = code.into_response();
    if retry_after_s > 0 {
        if let Ok(v) = HeaderValue::from_str(&retry_after_s.to_string()) {
            resp.headers_mut().insert(header::RETRY_AFTER, v);
        }
    }
    resp
}

impl Sink {
    pub async fn start(
        shared: Arc<Shared>,
        verifier: Arc<Verifier>,
        clock: Clock,
        seed: u64,
    ) -> anyhow::Result<Self> {
        let state = Arc::new(SinkState {
            shared,
            verifier,
            clock,
            seed,
            dests: RwLock::new(Vec::new()),
            crons: RwLock::new(HashMap::new()),
            stop: Notify::new(),
            release: Notify::new(),
        });
        let app = Router::new()
            .route("/d/{idx}", post(handle))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { addr, state, task })
    }

    pub fn add_dest(&self, cfg: DestCfg) -> usize {
        let mut dests = self.state.dests.write();
        let median_us = (cfg.median_ms * 1000.0) as u64;
        dests.push(Arc::new(Dest {
            cfg,
            mode: AtomicU8::new(Mode::Normal as u8),
            median_us: AtomicU64::new(median_us),
            slow_milli: AtomicU32::new(1000),
            inflight: AtomicU32::new(0),
            max_inflight: AtomicU32::new(0),
            recv: AtomicU64::new(0),
            ok: AtomicU64::new(0),
            fail_503: AtomicU64::new(0),
            fail_422: AtomicU64::new(0),
            rejected_mode: AtomicU64::new(0),
        }));
        dests.len() - 1
    }

    pub fn url(&self, idx: usize) -> String {
        format!("http://{}/d/{idx}", self.addr)
    }

    pub fn set_mode(&self, idx: usize, mode: Mode) {
        if let Some(d) = self.state.dests.read().get(idx) {
            d.mode.store(mode as u8, Ordering::Relaxed);
        }
        if mode != Mode::Hang {
            self.state.release.notify_waiters();
        }
    }

    pub fn dests(&self) -> Vec<Arc<Dest>> {
        self.state.dests.read().clone()
    }

    pub fn register_cron(&self, id: u32, anchor_ms: i64, period_ms: i64) {
        self.state.crons.write().insert(
            id,
            Arc::new(CronInfo {
                anchor_ms,
                period_ms,
                fires: AtomicU64::new(0),
            }),
        );
    }

    pub fn shutdown(&self) {
        self.state.stop.notify_waiters();
        self.task.abort();
    }
}

impl Dest {
    pub fn max_seen(&self) -> u32 {
        self.max_inflight.load(Ordering::Relaxed)
    }
}
