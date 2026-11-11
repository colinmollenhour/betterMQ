//! Open-loop load generator.
//!
//! Messages carry an *intended* send time from a pre-computed arrival plan.
//! The generator sends them as close to that time as it can but never slows the
//! plan down when the broker is slow: latency is measured from the intended
//! time, so queueing delay is counted instead of silently omitted
//! ("coordinated omission"). Generator lag is recorded separately so a
//! saturated client is visible rather than blamed on the broker.

use crate::stats::{Clock, Shared};
use crate::verify::Stream;
use serde::Deserialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

pub struct OutMsg {
    pub stream: Arc<Stream>,
    pub seq: u64,
    pub queue: Arc<str>,
    /// FIFO lane key; empty for standard delivery.
    pub key: String,
    /// Pre-rendered JSON object text, spliced into the request verbatim.
    pub body: String,
    pub delay_ms: u64,
    pub priority: Option<u8>,
    pub idem: Option<String>,
    /// Publish to this fan-out group instead of a queue.
    pub group: Option<Arc<str>>,
}

pub struct Planned {
    /// Offset from the start of the run.
    pub at: Duration,
    pub msg: OutMsg,
}

pub trait Plan: Send {
    fn next(&mut self) -> Option<Planned>;
}

#[derive(Clone, Copy, Debug)]
pub enum Wire {
    /// One request per message (`/v1/enqueue`, or `/v1/publish` for groups).
    Single,
    /// Up to N messages per request (`/v1/enqueue/batch`, or `/v1/ingest/batch` above 100).
    Batch(usize),
}

#[derive(Clone, Debug)]
pub struct SenderCfg {
    pub wire: Wire,
    pub max_in_flight: usize,
    /// Messages buffered locally when all in-flight slots are busy; beyond
    /// this, new arrivals are dropped and counted (a client that gave up).
    pub max_local_queue: usize,
    pub linger: Duration,
    pub timeout: Duration,
}

impl Default for SenderCfg {
    fn default() -> Self {
        Self {
            wire: Wire::Single,
            max_in_flight: 256,
            max_local_queue: 200_000,
            linger: Duration::from_millis(2),
            timeout: Duration::from_secs(30),
        }
    }
}

pub struct Sender {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
    cfg: SenderCfg,
    shared: Arc<Shared>,
    clock: Clock,
}

struct Pending {
    intended: Instant,
    /// When the message became sendable: its intended time, or the moment the
    /// previous message for the same key was answered, whichever is later.
    eligible: Instant,
    msg: OutMsg,
}

#[derive(Deserialize, Default)]
struct BatchResp {
    #[serde(default)]
    accepted: usize,
    #[serde(default)]
    failed: usize,
    #[serde(default)]
    results: Vec<BatchItem>,
}

#[derive(Deserialize)]
struct BatchItem {
    index: usize,
    accepted: bool,
    #[serde(default)]
    duplicate: bool,
}

fn json_str(out: &mut String, s: &str) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn single_request(m: &OutMsg) -> (&'static str, String) {
    let mut s = String::with_capacity(m.body.len() + 160);
    if let Some(group) = &m.group {
        s.push_str("{\"group_id\":");
        json_str(&mut s, group);
    } else {
        s.push_str("{\"queue\":");
        json_str(&mut s, &m.queue);
    }
    if !m.key.is_empty() {
        s.push_str(",\"key\":");
        json_str(&mut s, &m.key);
    }
    s.push_str(",\"body\":");
    s.push_str(&m.body);
    if m.delay_ms > 0 {
        let _ = write!(s, ",\"delay\":{}", m.delay_ms);
    }
    if let Some(p) = m.priority {
        let _ = write!(s, ",\"priority\":{p}");
    }
    if let Some(i) = &m.idem {
        s.push_str(",\"idempotency_key\":");
        json_str(&mut s, i);
    }
    s.push('}');
    (
        if m.group.is_some() {
            "/v1/publish"
        } else {
            "/v1/enqueue"
        },
        s,
    )
}

fn batch_request(msgs: &[Pending]) -> String {
    let mut s =
        String::with_capacity(msgs.iter().map(|p| p.msg.body.len() + 140).sum::<usize>() + 16);
    s.push_str("{\"messages\":[");
    for (i, p) in msgs.iter().enumerate() {
        let m = &p.msg;
        if i > 0 {
            s.push(',');
        }
        s.push_str("{\"topic\":");
        json_str(&mut s, &m.queue);
        if !m.key.is_empty() {
            s.push_str(",\"routing_key\":");
            json_str(&mut s, &m.key);
        }
        s.push_str(",\"payload\":");
        s.push_str(&m.body);
        if m.delay_ms > 0 {
            let _ = write!(s, ",\"delay_ms\":{}", m.delay_ms);
        }
        if let Some(pr) = m.priority {
            let _ = write!(s, ",\"priority\":{pr}");
        }
        if let Some(id) = &m.idem {
            s.push_str(",\"idempotency_key\":");
            json_str(&mut s, id);
        }
        s.push('}');
    }
    s.push_str("]}");
    s
}

fn snippet(bytes: &[u8]) -> String {
    String::from_utf8_lossy(&bytes[..bytes.len().min(110)]).replace(['\n', '\r'], " ")
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

impl Sender {
    pub fn new(
        http: reqwest::Client,
        base: &str,
        token: Option<String>,
        cfg: SenderCfg,
        shared: Arc<Shared>,
        clock: Clock,
    ) -> Self {
        Self {
            http,
            base: base.trim_end_matches('/').to_string(),
            token,
            cfg,
            shared,
            clock,
        }
    }

    /// Run `plan` to completion (or until `stop`), then wait for in-flight
    /// requests. Returns the number of messages the plan produced.
    /// Like [`run`](Self::run), with the plan's time zero pinned to `started`
    /// so message timestamps built from `started` line up exactly.
    pub async fn run_from(
        &self,
        mut plan: Box<dyn Plan>,
        stop: Arc<AtomicBool>,
        started: Instant,
    ) -> u64 {
        let slots = Arc::new(Semaphore::new(self.cfg.max_in_flight));
        let mut queue: VecDeque<Pending> = VecDeque::new();
        // Keyed (FIFO) messages: a producer sends the next message for an
        // entity only after the previous one was answered, so the broker's
        // accept order equals the intended order. Others wait here.
        let mut busy_keys: HashSet<String> = HashSet::new();
        let mut blocked: HashMap<String, VecDeque<Pending>> = HashMap::new();
        let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<String>>();
        let mut peeked: Option<Planned> = plan.next();
        let mut produced = 0u64;
        let batch_n = match self.cfg.wire {
            Wire::Single => 1,
            Wire::Batch(n) => n.max(1),
        };
        let c = &self.shared.c;

        loop {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            while let Ok(keys) = done_rx.try_recv() {
                for key in keys {
                    busy_keys.remove(&key);
                    if let Some(waiting) = blocked.get_mut(&key) {
                        if let Some(mut next) = waiting.pop_front() {
                            next.eligible = Instant::now();
                            queue.push_front(next);
                        }
                        if waiting.is_empty() {
                            blocked.remove(&key);
                        }
                    }
                }
            }
            let now = Instant::now();
            let elapsed = now.duration_since(started);
            while let Some(p) = peeked.as_ref() {
                if p.at > elapsed {
                    break;
                }
                let planned = peeked.take().expect("checked above");
                produced += 1;
                c.offered.fetch_add(1, Ordering::Relaxed);
                if queue.len() >= self.cfg.max_local_queue {
                    c.dropped.fetch_add(1, Ordering::Relaxed);
                } else {
                    queue.push_back(Pending {
                        intended: started + planned.at,
                        eligible: started + planned.at,
                        msg: planned.msg,
                    });
                }
                peeked = plan.next();
            }

            while !queue.is_empty() {
                let oldest_wait = now.saturating_duration_since(queue[0].intended);
                let full = queue.len() >= batch_n;
                if !full && oldest_wait < self.cfg.linger && peeked.is_some() {
                    break;
                }
                let Ok(permit) = slots.clone().try_acquire_owned() else {
                    break;
                };
                let mut batch: Vec<Pending> = Vec::with_capacity(batch_n.min(queue.len()));
                let mut keys: Vec<String> = Vec::new();
                while batch.len() < batch_n {
                    let Some(p) = queue.pop_front() else {
                        break;
                    };
                    if !p.msg.key.is_empty() {
                        // A lane is (queue, key): same key on two queues is unrelated.
                        let lane = format!("{}\u{0}{}", p.msg.queue, p.msg.key);
                        if busy_keys.contains(&lane) {
                            blocked.entry(lane).or_default().push_back(p);
                            continue;
                        }
                        busy_keys.insert(lane.clone());
                        keys.push(lane);
                    }
                    batch.push(p);
                }
                if batch.is_empty() {
                    continue;
                }
                let this = self.snapshot();
                let tx = done_tx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    this.dispatch(batch).await;
                    if !keys.is_empty() {
                        let _ = tx.send(keys);
                    }
                });
            }

            if peeked.is_none() && queue.is_empty() && blocked.is_empty() {
                break;
            }
            let sleep_for = match (&peeked, queue.is_empty() && blocked.is_empty()) {
                (Some(p), true) => {
                    p.at.saturating_sub(started.elapsed())
                        .clamp(Duration::from_micros(200), Duration::from_millis(5))
                }
                _ => Duration::from_micros(500),
            };
            tokio::time::sleep(sleep_for).await;
        }
        // Anything still queued when we are told to stop never reached the broker.
        let stranded = queue.len() + blocked.values().map(VecDeque::len).sum::<usize>();
        c.dropped.fetch_add(stranded as u64, Ordering::Relaxed);
        let _ = tokio::time::timeout(
            self.cfg.timeout + Duration::from_secs(5),
            slots.acquire_many(self.cfg.max_in_flight as u32),
        )
        .await;
        produced
    }

    fn snapshot(&self) -> Arc<SenderSnap> {
        Arc::new(SenderSnap {
            http: self.http.clone(),
            base: self.base.clone(),
            token: self.token.clone(),
            timeout: self.cfg.timeout,
            batch: matches!(self.cfg.wire, Wire::Batch(_)),
            shared: self.shared.clone(),
            clock: self.clock,
        })
    }
}

struct SenderSnap {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
    timeout: Duration,
    batch: bool,
    shared: Arc<Shared>,
    #[allow(dead_code)]
    clock: Clock,
}

impl SenderSnap {
    async fn dispatch(&self, batch: Vec<Pending>) {
        let c = &self.shared.c;
        let n = batch.len() as u64;
        let (path, body) = if self.batch {
            let path = if batch.len() > 100 {
                "/v1/ingest/batch"
            } else {
                "/v1/enqueue/batch"
            };
            (path, batch_request(&batch))
        } else {
            single_request(&batch[0].msg)
        };
        let payload: u64 = batch.iter().map(|p| p.msg.body.len() as u64).sum();
        let wire = body.len() as u64;
        let mut req = self
            .http
            .post(format!("{}{path}", self.base))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .timeout(self.timeout)
            .body(body);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        let sent_at = Instant::now();
        c.sent.fetch_add(n, Ordering::Relaxed);
        c.requests.fetch_add(1, Ordering::Relaxed);
        c.client_inflight.fetch_add(1, Ordering::Relaxed);
        for p in &batch {
            self.shared
                .l
                .gen_lag
                .record(sent_at.saturating_duration_since(p.eligible));
        }
        let result = req.send().await;
        let (status, bytes) = match result {
            Ok(resp) => {
                let status = resp.status();
                (Some(status), resp.bytes().await.unwrap_or_default())
            }
            Err(_) => (None, Default::default()),
        };
        let done = Instant::now();
        c.client_inflight.fetch_sub(1, Ordering::Relaxed);
        c.wire_bytes.fetch_add(wire, Ordering::Relaxed);

        let mut accepted: Vec<bool> = vec![false; batch.len()];
        match status {
            Some(s) if s.is_success() => {
                if self.batch {
                    let parsed: BatchResp = serde_json::from_slice(&bytes).unwrap_or_default();
                    if parsed.failed > 0 {
                        self.shared.note_error(format!(
                            "partial batch: {} of {} failed {}",
                            parsed.failed,
                            batch.len(),
                            snippet(&bytes)
                        ));
                    }
                    if parsed.failed == 0 && parsed.accepted >= batch.len() {
                        accepted.iter_mut().for_each(|a| *a = true);
                    } else if !parsed.results.is_empty() {
                        for item in &parsed.results {
                            if item.accepted && item.index < accepted.len() {
                                accepted[item.index] = true;
                                if item.duplicate {
                                    c.duplicates_acked.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                } else {
                    accepted[0] = true;
                    if contains(&bytes, b"\"duplicate\":true") {
                        c.duplicates_acked.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Some(s) if s.as_u16() == 429 => {
                c.http_429.fetch_add(n, Ordering::Relaxed);
                self.shared.note_error(format!("429 {}", snippet(&bytes)));
            }
            Some(s) if s.as_u16() == 503 => {
                c.http_503.fetch_add(n, Ordering::Relaxed);
                self.shared.note_error(format!("503 {}", snippet(&bytes)));
            }
            Some(s) => {
                c.http_other_err.fetch_add(n, Ordering::Relaxed);
                self.shared
                    .note_error(format!("{} {}", s.as_u16(), snippet(&bytes)));
            }
            None => {
                c.transport_err.fetch_add(n, Ordering::Relaxed);
                self.shared.note_error("transport error / timeout".into());
            }
        }
        let service = done.saturating_duration_since(sent_at);
        for (p, ok) in batch.iter().zip(accepted.iter()) {
            self.shared.l.service.record(service);
            if *ok {
                p.msg.stream.mark_acked(p.msg.seq);
                c.acked.fetch_add(1, Ordering::Relaxed);
                c.payload_bytes
                    .fetch_add(p.msg.body.len() as u64, Ordering::Relaxed);
                let ack = done.saturating_duration_since(p.intended);
                self.shared.l.ack.record(ack);
                p.msg.stream.ack.record(ack);
            } else if matches!(status, Some(s) if s.as_u16() == 429 || s.as_u16() == 503) {
                c.rejected.fetch_add(1, Ordering::Relaxed);
            } else {
                c.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
        let _ = payload;
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::verify::Verifier;

    fn msg(key: &str, delay: u64, group: bool) -> OutMsg {
        let v = Verifier::default();
        OutMsg {
            stream: v.register("t", 1, 0, false),
            seq: 0,
            queue: "q".into(),
            key: key.into(),
            body: "{\"a\":1}".into(),
            delay_ms: delay,
            priority: Some(3),
            idem: Some("i\"d".into()),
            group: group.then(|| "gid".into()),
        }
    }

    #[test]
    fn single_request_is_valid_json() {
        let (path, body) = single_request(&msg("k1", 500, false));
        assert_eq!(path, "/v1/enqueue");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["queue"], "q");
        assert_eq!(v["key"], "k1");
        assert_eq!(v["delay"], 500);
        assert_eq!(v["idempotency_key"], "i\"d");
        assert_eq!(v["body"]["a"], 1);
    }

    #[test]
    fn group_publish_uses_publish_path() {
        let (path, body) = single_request(&msg("", 0, true));
        assert_eq!(path, "/v1/publish");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["group_id"], "gid");
        assert!(v.get("queue").is_none());
    }

    #[test]
    fn batch_request_is_valid_json() {
        let pending: Vec<Pending> = (0..3)
            .map(|_| Pending {
                intended: Instant::now(),
                eligible: Instant::now(),
                msg: msg("k", 0, false),
            })
            .collect();
        let v: serde_json::Value = serde_json::from_str(&batch_request(&pending)).unwrap();
        assert_eq!(v["messages"].as_array().unwrap().len(), 3);
        assert_eq!(v["messages"][0]["topic"], "q");
        assert_eq!(v["messages"][0]["routing_key"], "k");
    }
}
