//! Harness self-check against a null server, so a reader can tell when a
//! result is limited by the load generator rather than the broker.

use crate::loadgen::{SenderCfg, Wire};
use crate::model::Calibration;
use crate::stats::{Clock, Shared};
use crate::trial::client;
use crate::verify::Verifier;
use crate::workload::{PadPool, RatePlan};
use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

async fn null_server() -> anyhow::Result<std::net::SocketAddr> {
    let app = Router::new().fallback(post(|| async { (StatusCode::ACCEPTED, "{\"ok\":true}") }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(addr)
}

pub async fn run(secs: f64) -> anyhow::Result<Calibration> {
    let addr = null_server().await?;
    let base = format!("http://{addr}");
    let verifier = Arc::new(Verifier::default());
    let clock = Clock::new();
    let pad = PadPool::new(1);
    let cfg = SenderCfg {
        wire: Wire::Single,
        max_in_flight: 512,
        ..SenderCfg::default()
    };

    // Latency floor at a trivial rate.
    let shared = Arc::new(Shared::default());
    let sender =
        crate::loadgen::Sender::new(client(512), &base, None, cfg.clone(), shared.clone(), clock);
    let dur = Duration::from_secs_f64(secs.min(3.0));
    let stream = verifier.register("cal-floor", 10_000, 0, false);
    let started = Instant::now();
    let plan = RatePlan::new(
        stream,
        "q",
        1000.0,
        dur,
        1024,
        0,
        false,
        clock.us_at(started),
        pad.clone(),
        1,
    );
    sender
        .run_from(Box::new(plan), Arc::new(AtomicBool::new(false)), started)
        .await;
    let floor = shared.l.ack.take();

    // Ceiling: offer far more than the client can send.
    let shared = Arc::new(Shared::default());
    let sender = crate::loadgen::Sender::new(client(512), &base, None, cfg, shared.clone(), clock);
    let offered = 400_000.0;
    let dur = Duration::from_secs_f64(secs);
    let stream = verifier.register("cal-max", RatePlan::expected(offered, dur), 0, false);
    let started = Instant::now();
    let plan = RatePlan::new(
        stream,
        "q",
        offered,
        dur,
        1024,
        0,
        false,
        clock.us_at(started),
        pad,
        2,
    );
    sender
        .run_from(Box::new(plan), Arc::new(AtomicBool::new(false)), started)
        .await;
    let elapsed = started.elapsed().as_secs_f64();
    let acked = shared.c.acked.load(Ordering::Relaxed) as f64;

    Ok(Calibration {
        null_server_msgs_s: acked / elapsed,
        null_server_p50_us: floor.value_at_quantile(0.5) as f64,
        null_server_p99_us: floor.value_at_quantile(0.99) as f64,
        connections: 512,
        note: "Single-message POSTs from this process to an in-process 202 server: an upper bound on what the harness can generate. Results approaching this are generator-limited.".into(),
    })
}
