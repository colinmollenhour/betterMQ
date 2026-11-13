//! Delivery ledger: proves nothing acknowledged was lost, duplicated, or
//! delivered out of order, using dense per-stream byte arrays so a 20M
//! message run costs ~40 MB and no locks on the hot path.

use crate::stats::Lat;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

pub const ACKED: u8 = 1;
pub const DONE: u8 = 2;
/// The sink permanently rejected this message on purpose (DLQ-bound).
pub const PERM: u8 = 4;
/// A matching record was read back from the broker's dead-letter queue.
const DLQ: u8 = 8;

pub struct Stream {
    pub id: u16,
    pub cap: usize,
    state: Vec<AtomicU8>,
    attempts: Vec<AtomicU8>,
    /// Fan-out only: one flag byte / attempt counter per (message, member).
    fan_state: Vec<AtomicU8>,
    fan_attempts: Vec<AtomicU8>,
    last_q: Vec<AtomicU32>,
    pub keyed: bool,
    /// Deliveries expected per accepted message (fan-out group members).
    pub fanout: u32,
    /// Cumulative ACK latency (from intended send) for this stream.
    pub ack: Lat,
    /// Cumulative first-attempt delivery latency for this stream.
    pub delivery: Lat,
    pub acked: AtomicU64,
    pub done: AtomicU64,
    pub perm: AtomicU64,
    pub dups: AtomicU64,
    pub inversions: AtomicU64,
    /// Inversions where the late message had needed a retry (the lane was not held).
    pub inversions_retry: AtomicU64,
    pub out_of_range: AtomicU64,
    /// Delivered successfully without a recorded ACK (client lost the response).
    pub unacked_delivered: AtomicU64,
}

impl Stream {
    pub fn mark_acked(&self, seq: u64) {
        if let Some(slot) = self.state.get(seq as usize) {
            let prev = slot.fetch_or(ACKED, Ordering::Relaxed);
            if prev & ACKED == 0 {
                self.acked.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record one delivery attempt; returns the 1-based attempt number.
    pub fn attempt(&self, seq: u64) -> u8 {
        match self.attempts.get(seq as usize) {
            Some(a) => a.fetch_add(1, Ordering::Relaxed).saturating_add(1),
            None => 1,
        }
    }

    /// Record a successful delivery on attempt number `attempt`. Returns
    /// `true` if it was a duplicate.
    pub fn mark_done(&self, seq: u64, key: u32, kseq: u32, attempt: u8) -> bool {
        let Some(slot) = self.state.get(seq as usize) else {
            self.out_of_range.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        let prev = slot.fetch_or(DONE, Ordering::Relaxed);
        if prev & DONE != 0 {
            self.dups.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        self.done.fetch_add(1, Ordering::Relaxed);
        if prev & ACKED == 0 {
            self.unacked_delivered.fetch_add(1, Ordering::Relaxed);
        }
        if self.keyed {
            if let Some(last) = self.last_q.get(key as usize) {
                let seen = last.fetch_max(kseq, Ordering::Relaxed);
                if kseq < seen {
                    self.inversions.fetch_add(1, Ordering::Relaxed);
                    if attempt > 1 {
                        self.inversions_retry.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        false
    }

    pub fn mark_perm(&self, seq: u64) {
        if let Some(slot) = self.state.get(seq as usize) {
            let prev = slot.fetch_or(PERM, Ordering::Relaxed);
            if prev & PERM == 0 {
                self.perm.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn fan_slot(&self, seq: u64, member: usize) -> usize {
        seq as usize * self.fanout as usize + member % self.fanout as usize
    }

    /// Record a delivery attempt to a fan-out member; returns the 1-based attempt number.
    pub fn fan_attempt(&self, seq: u64, member: usize) -> u8 {
        match self.fan_attempts.get(self.fan_slot(seq, member)) {
            Some(a) => a.fetch_add(1, Ordering::Relaxed).saturating_add(1),
            None => 1,
        }
    }

    /// One successful delivery of a fan-out message to a member. Returns
    /// `true` if that member had already received it.
    pub fn mark_fanout_done(&self, seq: u64, member: usize) -> bool {
        let Some(slot) = self.fan_state.get(self.fan_slot(seq, member)) else {
            self.out_of_range.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        let prev = slot.fetch_or(DONE, Ordering::Relaxed);
        if prev & DONE != 0 {
            self.dups.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        self.done.fetch_add(1, Ordering::Relaxed);
        false
    }

    /// A fan-out member permanently rejected this message.
    pub fn mark_fanout_perm(&self, seq: u64, member: usize) {
        if let Some(slot) = self.fan_state.get(self.fan_slot(seq, member)) {
            if slot.fetch_or(PERM, Ordering::Relaxed) & PERM == 0 {
                self.perm.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Final accounting requires a successful delivery or a confirmed DLQ row;
    /// a sink's permanent rejection alone is not proof of durable disposition.
    pub fn count_unaccounted(&self) -> u64 {
        self.count_missing(DONE | DLQ)
    }

    fn count_missing(&self, accounted: u8) -> u64 {
        if self.fanout > 1 {
            let n = self.fanout as usize;
            let mut missing = 0u64;
            for (seq, s) in self.state.iter().enumerate() {
                if s.load(Ordering::Relaxed) & ACKED == 0 {
                    continue;
                }
                for member in 0..n {
                    let v = self.fan_state[seq * n + member].load(Ordering::Relaxed);
                    if v & accounted == 0 {
                        missing += 1;
                    }
                }
            }
            return missing;
        }
        self.state
            .iter()
            .filter(|s| {
                let v = s.load(Ordering::Relaxed);
                v & ACKED != 0 && v & accounted == 0
            })
            .count() as u64
    }

    fn confirm_dlq(&self, seq: u64, member: Option<usize>) -> anyhow::Result<()> {
        let seq = usize::try_from(seq)?;
        let source = self
            .state
            .get(seq)
            .ok_or_else(|| anyhow::anyhow!("DLQ sequence out of range"))?;
        let slot = if self.fanout > 1 {
            let member = member.ok_or_else(|| anyhow::anyhow!("DLQ fan-out member is missing"))?;
            anyhow::ensure!(
                member < self.fanout as usize,
                "DLQ fan-out member out of range"
            );
            &self.fan_state[seq * self.fanout as usize + member]
        } else {
            source
        };
        let previous = slot.fetch_or(DLQ, Ordering::Relaxed);
        anyhow::ensure!(
            previous & DLQ == 0,
            "duplicate dead-letter record for one delivery"
        );
        anyhow::ensure!(
            previous & DONE == 0,
            "delivery was both successful and dead-lettered"
        );
        Ok(())
    }

    pub fn pending(&self) -> u64 {
        (self.acked.load(Ordering::Relaxed) * u64::from(self.fanout))
            .saturating_sub(self.done.load(Ordering::Relaxed) + self.perm.load(Ordering::Relaxed))
    }
}

#[derive(Default)]
pub struct Verifier {
    streams: RwLock<Vec<Arc<Stream>>>,
}

impl Verifier {
    /// Reconcile the source identity carried in a listed DLQ body. Unrelated or
    /// duplicate records must never cancel out a different missing delivery.
    pub fn confirm_dlq_message(&self, row: &serde_json::Value) -> anyhow::Result<()> {
        anyhow::ensure!(
            row["body_encoding"].as_str() != Some("base64"),
            "benchmark DLQ body unexpectedly changed from UTF-8 to binary"
        );
        let body = row["body"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("DLQ body is missing"))?;
        let body: serde_json::Value = serde_json::from_str(body)?;
        let fan = body["f"].as_u64();
        let stream = fan
            .or_else(|| body["s"].as_u64())
            .ok_or_else(|| anyhow::anyhow!("DLQ stream is missing"))?;
        let stream = self
            .get(u16::try_from(stream)?)
            .ok_or_else(|| anyhow::anyhow!("unknown DLQ stream"))?;
        let seq = body["n"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("DLQ sequence is missing"))?;
        let member = if fan.is_some() {
            let destination = row["destination_url"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("DLQ destination is missing"))?;
            let url = reqwest::Url::parse(destination)?;
            Some(
                url.path()
                    .strip_prefix("/d/")
                    .ok_or_else(|| anyhow::anyhow!("unknown DLQ destination"))?
                    .parse::<usize>()?,
            )
        } else {
            None
        };
        stream.confirm_dlq(seq, member)
    }

    pub fn register(&self, _name: &str, cap: usize, keys: usize, keyed: bool) -> Arc<Stream> {
        self.register_fanout(_name, cap, keys, keyed, 1)
    }

    pub fn register_fanout(
        &self,
        _name: &str,
        cap: usize,
        keys: usize,
        keyed: bool,
        fanout: u32,
    ) -> Arc<Stream> {
        let mut streams = self.streams.write();
        let stream = Arc::new(Stream {
            id: streams.len() as u16,
            cap,
            state: (0..cap).map(|_| AtomicU8::new(0)).collect(),
            attempts: (0..cap).map(|_| AtomicU8::new(0)).collect(),
            fan_state: (0..if fanout > 1 { cap * fanout as usize } else { 0 })
                .map(|_| AtomicU8::new(0))
                .collect(),
            fan_attempts: (0..if fanout > 1 { cap * fanout as usize } else { 0 })
                .map(|_| AtomicU8::new(0))
                .collect(),
            last_q: (0..if keyed { keys } else { 0 })
                .map(|_| AtomicU32::new(0))
                .collect(),
            keyed,
            fanout: fanout.max(1),
            ack: Lat::new(),
            delivery: Lat::new(),
            acked: AtomicU64::new(0),
            done: AtomicU64::new(0),
            perm: AtomicU64::new(0),
            dups: AtomicU64::new(0),
            inversions: AtomicU64::new(0),
            inversions_retry: AtomicU64::new(0),
            out_of_range: AtomicU64::new(0),
            unacked_delivered: AtomicU64::new(0),
        });
        streams.push(stream.clone());
        stream
    }

    pub fn get(&self, id: u16) -> Option<Arc<Stream>> {
        self.streams.read().get(id as usize).cloned()
    }

    pub fn all(&self) -> Vec<Arc<Stream>> {
        self.streams.read().clone()
    }

    /// Acked-but-undelivered messages across all streams (the live backlog).
    pub fn pending(&self) -> u64 {
        self.streams.read().iter().map(|s| s.pending()).sum()
    }

    pub fn totals(&self) -> (u64, u64, u64) {
        let streams = self.streams.read();
        let acked = streams
            .iter()
            .map(|s| s.acked.load(Ordering::Relaxed))
            .sum();
        let done = streams.iter().map(|s| s.done.load(Ordering::Relaxed)).sum();
        let perm = streams.iter().map(|s| s.perm.load(Ordering::Relaxed)).sum();
        (acked, done, perm)
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn tracks_loss_duplicates_and_order() {
        let v = Verifier::default();
        let s = v.register("t", 10, 2, true);
        for i in 0..5 {
            s.mark_acked(i);
        }
        assert!(!s.mark_done(0, 0, 1, 1));
        assert!(!s.mark_done(1, 0, 3, 1));
        assert!(!s.mark_done(2, 0, 2, 2)); // key 0: q=2 after q=3 -> inversion, after a retry
        assert!(s.mark_done(1, 0, 3, 1)); // duplicate
        s.mark_perm(3);
        s.confirm_dlq(3, None).unwrap();
        assert_eq!(s.inversions.load(Ordering::Relaxed), 1);
        assert_eq!(s.inversions_retry.load(Ordering::Relaxed), 1);
        assert_eq!(s.dups.load(Ordering::Relaxed), 1);
        assert_eq!(s.count_unaccounted(), 1); // seq 4 never delivered
        assert_eq!(s.pending(), 1);
    }

    #[test]
    fn unkeyed_streams_ignore_order() {
        let v = Verifier::default();
        let s = v.register("u", 4, 0, false);
        s.mark_acked(0);
        s.mark_acked(1);
        s.mark_done(1, 0, 9, 1);
        s.mark_done(0, 0, 1, 1);
        assert_eq!(s.inversions.load(Ordering::Relaxed), 0);
        assert_eq!(s.count_unaccounted(), 0);
    }

    #[test]
    fn fanout_tracks_each_member_and_duplicates_do_not_mask_loss() {
        let v = Verifier::default();
        let s = v.register_fanout("f", 4, 0, false, 3);
        s.mark_acked(0);
        s.mark_acked(1);
        assert_eq!(s.pending(), 6);
        assert!(!s.mark_fanout_done(0, 0));
        assert!(!s.mark_fanout_done(0, 1));
        assert!(s.mark_fanout_done(0, 1)); // member 1 got it twice
        s.mark_fanout_perm(0, 2);
        s.confirm_dlq(0, Some(2)).unwrap();
        assert!(!s.mark_fanout_done(1, 0));
        // message 0 complete; message 1 still owes members 1 and 2.
        assert_eq!(s.count_unaccounted(), 2);
        assert_eq!(s.dups.load(Ordering::Relaxed), 1);
        assert_eq!(s.fan_attempt(1, 2), 1);
        assert_eq!(s.fan_attempt(1, 2), 2);
        assert_eq!(s.fan_attempt(1, 1), 1);
    }

    #[test]
    fn permanent_rejection_is_not_proof_of_a_dead_letter() {
        let v = Verifier::default();
        let s = v.register("rejected", 4, 0, false);
        s.mark_acked(0);
        s.mark_acked(1);
        s.mark_perm(0);
        assert_eq!(
            s.pending(),
            1,
            "live drain may stop retrying permanent failures"
        );
        assert_eq!(
            s.count_unaccounted(),
            2,
            "final accounting must confirm the DLQ"
        );
        let row = serde_json::json!({"body": "{\"s\":0,\"n\":0}", "body_encoding": "utf8"});
        v.confirm_dlq_message(&row).unwrap();
        assert_eq!(s.count_unaccounted(), 1);
        assert!(v
            .confirm_dlq_message(&row)
            .unwrap_err()
            .to_string()
            .contains("duplicate"));
        assert_eq!(
            s.count_unaccounted(),
            1,
            "duplicate DLQ rows cannot mask another missing message"
        );
    }

    #[test]
    fn dead_letter_accounting_matches_the_exact_fanout_member() {
        let v = Verifier::default();
        let s = v.register_fanout("fan", 2, 0, false, 3);
        s.mark_acked(0);
        s.mark_fanout_done(0, 0);
        v.confirm_dlq_message(&serde_json::json!({
            "body": "{\"f\":0,\"n\":0}", "destination_url": "http://127.0.0.1:1234/d/1"
        }))
        .unwrap();
        assert_eq!(s.count_unaccounted(), 1);
        assert!(
            v.confirm_dlq_message(&serde_json::json!({
                "body": "{\"f\":0,\"n\":0}", "destination_url": "http://127.0.0.1:1234/d/4"
            }))
            .is_err(),
            "member IDs must not wrap modulo fanout"
        );
        assert_eq!(s.count_unaccounted(), 1);
    }

    #[test]
    fn malformed_or_unrelated_dead_letters_do_not_prove_delivery() {
        let v = Verifier::default();
        let s = v.register("single", 2, 0, false);
        s.mark_acked(0);
        for row in [
            serde_json::json!({"body":"not json"}),
            serde_json::json!({"body":"{\"s\":99,\"n\":0}"}),
            serde_json::json!({"body":"{\"s\":0,\"n\":9}"}),
            serde_json::json!({"body":"{\"s\":0,\"n\":0}", "body_encoding":"base64"}),
        ] {
            assert!(v.confirm_dlq_message(&row).is_err());
        }
        assert_eq!(s.count_unaccounted(), 1);
    }

    #[test]
    fn attempts_count_up() {
        let v = Verifier::default();
        let s = v.register("a", 2, 0, false);
        assert_eq!(s.attempt(0), 1);
        assert_eq!(s.attempt(0), 2);
        assert_eq!(s.attempt(1), 1);
    }
}
