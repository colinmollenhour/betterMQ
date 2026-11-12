//! One process-wide, bounded native writer. Grouping is per journal path and
//! preserves FIFO request order, including reads and conflicting accepts.

use super::{
    apply_event, CachedSnapshot, FanoutEvent, FanoutJournalRecord, FanoutSnapshot, FANOUT_CACHE,
    FANOUT_STATE_VERSION,
};
use super::{FanoutCommand, FanoutMemberOutcome, FanoutStore, FanoutStoreError};
use std::sync::OnceLock;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

const QUEUE_CAPACITY: usize = 256;
const MAX_GROUP_REQUESTS: usize = 64;
const MAX_GROUP_BYTES: usize = 4 * 1024 * 1024;

pub(super) enum Operation {
    Get(Uuid),
    Accept(FanoutCommand),
    Replace(FanoutCommand),
    Results(Uuid, Vec<(Uuid, FanoutMemberOutcome)>),
}

impl Operation {
    fn estimated_bytes(&self) -> usize {
        match self {
            Self::Get(_) => 32,
            Self::Accept(command) | Self::Replace(command) => {
                let payload = command.shared_payload.as_ref().map_or(0, |body| body.len());
                command
                    .members
                    .iter()
                    .fold(payload.saturating_add(512), |bytes, member| {
                        bytes
                            .saturating_add(member.request.payload.len())
                            .saturating_add(2048)
                    })
                    .saturating_add(command.outcomes.len().saturating_mul(1024))
            }
            Self::Results(_, results) => results.iter().fold(128usize, |bytes, (_, outcome)| {
                bytes
                    .saturating_add(512)
                    .saturating_add(outcome.error.as_ref().map_or(0, String::len))
            }),
        }
    }
}

pub(super) enum Outcome {
    Found(Option<FanoutCommand>),
    Accepted(FanoutCommand, bool),
    Updated(FanoutCommand),
}

struct Request {
    store: FanoutStore,
    operation: Operation,
    reply: oneshot::Sender<Result<Outcome, FanoutStoreError>>,
}

struct Writer {
    sender: mpsc::Sender<Request>,
}

static WRITER: OnceLock<Result<Writer, String>> = OnceLock::new();

impl FanoutStore {
    pub(super) fn execute_group(
        &self,
        operations: Vec<Operation>,
    ) -> Result<Vec<Result<Outcome, FanoutStoreError>>, FanoutStoreError> {
        let _lock = broker_storage::FileLock::exclusive(&self.journal_path)?;
        let mut cache = FANOUT_CACHE.get_or_init(Default::default).lock();
        let cached = cache.entry(self.journal_path.clone()).or_default();
        let state = self.load_state(cached)?;
        // Only touched commands are copied, never the full retained outbox.
        // Shared payloads remain Arc-backed in both the overlay and replies.
        let mut overlay = FanoutSnapshot::default();
        let mut events = Vec::new();
        let mut outcomes = Vec::with_capacity(operations.len());
        for operation in operations {
            let id = match &operation {
                Operation::Get(id) | Operation::Results(id, _) => *id,
                Operation::Accept(command) | Operation::Replace(command) => command.id,
            };
            if matches!(&operation, Operation::Get(_)) {
                outcomes.push(Ok(Outcome::Found(
                    overlay
                        .commands
                        .get(&id)
                        .or_else(|| state.commands.get(&id))
                        .cloned(),
                )));
                continue;
            }
            if let std::collections::hash_map::Entry::Vacant(entry) = overlay.commands.entry(id) {
                if let Some(existing) = state.commands.get(&id) {
                    entry.insert(existing.clone());
                }
            }
            let outcome = match operation {
                Operation::Get(_) => Ok(Outcome::Found(overlay.commands.get(&id).cloned())),
                Operation::Accept(command) => {
                    let inserted = !overlay.commands.contains_key(&id);
                    if inserted {
                        let event = FanoutEvent::Accept { fanout: command };
                        apply_event(&mut overlay, event.clone());
                        events.push(event);
                    }
                    Ok(Outcome::Accepted(overlay.commands[&id].clone(), inserted))
                }
                Operation::Replace(command) => {
                    let event = FanoutEvent::Replace { fanout: command };
                    apply_event(&mut overlay, event.clone());
                    events.push(event);
                    Ok(Outcome::Updated(overlay.commands[&id].clone()))
                }
                Operation::Results(_, results) => {
                    if !overlay.commands.contains_key(&id) {
                        Err(FanoutStoreError::NotFound(id))
                    } else {
                        let updated_at_ms = chrono::Utc::now().timestamp_millis();
                        for (member_id, outcome) in results {
                            let event = FanoutEvent::RecordResult {
                                fanout_id: id,
                                member_id,
                                outcome,
                                updated_at_ms,
                            };
                            apply_event(&mut overlay, event.clone());
                            events.push(event);
                        }
                        Ok(Outcome::Updated(overlay.commands[&id].clone()))
                    }
                }
            };
            outcomes.push(outcome);
        }
        self.append_events(cached, events)?;
        Ok(outcomes)
    }

    fn fence(&self) {
        FANOUT_CACHE
            .get_or_init(Default::default)
            .lock()
            .entry(self.journal_path.clone())
            .or_default()
            .poisoned = true;
    }

    pub(super) fn append_events(
        &self,
        cached: &mut CachedSnapshot,
        events: Vec<FanoutEvent>,
    ) -> Result<(), FanoutStoreError> {
        if events.is_empty() {
            return Ok(());
        }
        let mut sequence = cached.state.sequence;
        let mut records = Vec::with_capacity(events.len());
        for event in events {
            sequence = sequence.checked_add(1).ok_or_else(|| {
                FanoutStoreError::Authority("fanout journal sequence exhausted".into())
            })?;
            records.push(FanoutJournalRecord {
                version: FANOUT_STATE_VERSION,
                sequence,
                event,
            });
        }
        #[cfg(test)]
        if let Some((started, release)) = cached.gate.take() {
            let _ = started.send(());
            assert!(
                release
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .is_ok(),
                "fanout writer gate timed out"
            );
        }
        #[cfg(test)]
        if std::mem::take(&mut cached.fail_next_sync) {
            use std::io::Write;
            // Simulate visible but unconfirmed bytes, before cache application.
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.journal_path)?;
            for record in &records {
                serde_json::to_writer(&mut file, record)?;
                file.write_all(b"\n")?;
            }
            cached.poisoned = true;
            return Err(std::io::Error::other("injected uncertain fanout sync failure").into());
        }
        let result = (|| {
            broker_storage::JsonlJournal::new(&self.journal_path).append_all(&records)?;
            for record in records {
                cached.state.sequence = record.sequence;
                apply_event(&mut cached.state, record.event);
                cached.state.journal_ops += 1;
            }
            self.maybe_compact(&mut cached.state)?;
            cached.offset = self.journal_path.metadata()?.len();
            cached.snapshot_stamp = self.snapshot_stamp()?;
            Ok::<_, FanoutStoreError>(())
        })();
        if result.is_err() {
            cached.poisoned = true;
        }
        #[cfg(test)]
        if result.is_ok() {
            cached.commit_epochs += 1;
        }
        result
    }
}

pub(super) async fn submit(
    store: &FanoutStore,
    operation: Operation,
) -> Result<Outcome, FanoutStoreError> {
    let writer = WRITER
        .get_or_init(|| {
            let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
            std::thread::Builder::new()
                .name("bettermq-fanout-io".into())
                .spawn(move || run(receiver))
                .map(|_| Writer { sender })
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| FanoutStoreError::Authority(error.clone()))?;
    let (reply, response) = oneshot::channel();
    writer
        .sender
        .send(Request {
            store: store.clone(),
            operation,
            reply,
        })
        .await
        .map_err(|_| FanoutStoreError::Authority("fanout writer stopped".into()))?;
    // Once admitted, cancellation drops only this receiver. The writer owns
    // the mutation and must finish its durability barrier before dropping it.
    response.await.map_err(|_| {
        FanoutStoreError::Authority("fanout writer stopped before durable reply".into())
    })?
}

fn run(mut receiver: mpsc::Receiver<Request>) {
    let mut deferred: Option<Request> = None;
    while let Some(first) = deferred.take().or_else(|| receiver.blocking_recv()) {
        let store = first.store.clone();
        let mut bytes = first.operation.estimated_bytes();
        let mut group = vec![first];
        while group.len() < MAX_GROUP_REQUESTS && bytes < MAX_GROUP_BYTES {
            match receiver.try_recv() {
                Ok(next)
                    if next.store.journal_path == store.journal_path
                        && bytes.saturating_add(next.operation.estimated_bytes())
                            <= MAX_GROUP_BYTES =>
                {
                    bytes = bytes.saturating_add(next.operation.estimated_bytes());
                    group.push(next);
                }
                Ok(next) => {
                    deferred = Some(next);
                    break;
                }
                Err(_) => break,
            }
        }
        let (operations, replies): (Vec<_>, Vec<_>) = group
            .into_iter()
            .map(|request| (request.operation, request.reply))
            .unzip();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            store.execute_group(operations)
        }));
        let result = match result {
            Ok(result) => result,
            Err(_) => {
                store.fence();
                Err(FanoutStoreError::Authority(
                    "fanout writer panicked; restart to recover".into(),
                ))
            }
        };
        match result {
            Ok(outcomes) => {
                for (reply, outcome) in replies.into_iter().zip(outcomes) {
                    let _ = reply.send(outcome);
                }
            }
            Err(error) => {
                let message = error.to_string();
                for reply in replies {
                    let _ = reply.send(Err(FanoutStoreError::Authority(message.clone())));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream::{FuturesUnordered, StreamExt};
    use std::sync::Arc;

    fn command(id: Uuid) -> FanoutCommand {
        FanoutCommand {
            id, acceptance_hash: "frozen".into(), tenant_id: "tenant".into(), group_id: Uuid::new_v4(),
            created_at_ms: 1, updated_at_ms: 1, shared_payload: Some(Arc::new("one shared body".into())),
            members: vec![super::super::FanoutMemberCommand { member_id: Uuid::new_v4(), request: serde_json::from_value(serde_json::json!({"routing_key":"key", "payload":"", "url":"https://example.com/hook", "secret":"secret"})).unwrap() }],
            outcomes: Default::default(), completed: false, completed_at_ms: None,
        }
    }

    #[tokio::test]
    async fn bounded_writer_groups_cross_command_fsyncs_and_owns_cancelled_admission() {
        let dir = tempfile::tempdir().unwrap();
        let store = FanoutStore::open(dir.path()).unwrap();
        let first_id = Uuid::new_v4();
        let original = command(first_id);
        let (started, start) = oneshot::channel();
        let (release, gate) = std::sync::mpsc::sync_channel(1);
        FANOUT_CACHE
            .get_or_init(Default::default)
            .lock()
            .entry(store.journal_path.clone())
            .or_default()
            .gate = Some((started, gate));
        let first_store = store.clone();
        let first = tokio::spawn(async move { first_store.accept_async(original).await });
        start.await.unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let mut pending = FuturesUnordered::new();
        for _ in 0..400 {
            let store = store.clone();
            let command = command(Uuid::new_v4());
            pending.push(async move { store.accept_async(command).await });
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::select! {
                _ = pending.next() => panic!("no reply may precede the fsync gate"),
                _ = async {
                    while WRITER.get().unwrap().as_ref().unwrap().sender.capacity() != 0 { tokio::task::yield_now().await; }
                } => {}
            }
        }).await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), pending.next())
                .await
                .is_err(),
            "queue backpressure must leave runtime timers responsive"
        );
        release.send(()).unwrap();
        while let Some(result) = pending.next().await {
            assert!(result.unwrap().1);
        }
        assert_eq!(
            store
                .get_async(first_id)
                .await
                .unwrap()
                .unwrap()
                .acceptance_hash,
            "frozen"
        );
        let epochs = FANOUT_CACHE.get().unwrap().lock()[&store.journal_path].commit_epochs;
        assert!(
            epochs < 100,
            "400 ready accepts should share bounded fsync groups, got {epochs}"
        );
        store.evict_cached_state();
        assert!(
            store.get(first_id).unwrap().is_some(),
            "cancelled admitted acceptance must survive reopen"
        );
    }

    #[test]
    fn grouped_accept_conflicts_results_and_missing_ids_preserve_request_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let store = FanoutStore::open(dir.path()).unwrap();
        let id = Uuid::new_v4();
        let original = command(id);
        let member = original.members[0].member_id;
        let mut conflicting = command(id);
        conflicting.acceptance_hash = "must not replace frozen acceptance".into();
        let results = store
            .execute_group(vec![
                Operation::Accept(original),
                Operation::Accept(conflicting),
                Operation::Results(Uuid::new_v4(), Vec::new()),
                Operation::Results(
                    id,
                    vec![(
                        member,
                        FanoutMemberOutcome {
                            response: None,
                            error: Some("terminal".into()),
                            attempts: 4,
                            next_attempt_at_ms: None,
                            terminal: true,
                        },
                    )],
                ),
                Operation::Get(id),
            ])
            .unwrap();
        assert!(matches!(&results[0], Ok(Outcome::Accepted(_, true))));
        assert!(
            matches!(&results[1], Ok(Outcome::Accepted(command, false)) if command.acceptance_hash == "frozen")
        );
        assert!(matches!(&results[2], Err(FanoutStoreError::NotFound(_))));
        assert!(
            matches!(&results[4], Ok(Outcome::Found(Some(command))) if command.completed && command.outcomes[&member].attempts == 4)
        );
        store.evict_cached_state();
        let recovered = store.get(id).unwrap().unwrap();
        assert!(recovered.completed);
        assert_eq!(recovered.acceptance_hash, "frozen");
        assert_eq!(recovered.outcomes[&member].attempts, 4);
        let records = broker_storage::JsonlJournal::new(&store.journal_path)
            .read_lines()
            .unwrap();
        assert_eq!(
            records.len(),
            2,
            "existing Accept and RecordResult formats remain sufficient"
        );
    }

    #[tokio::test]
    async fn uncertain_fanout_sync_fences_reads_and_writes_until_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let store = FanoutStore::open(dir.path()).unwrap();
        let id = Uuid::new_v4();
        FANOUT_CACHE
            .get_or_init(Default::default)
            .lock()
            .entry(store.journal_path.clone())
            .or_default()
            .fail_next_sync = true;
        assert!(store.accept_async(command(id)).await.is_err());
        assert!(
            store.get_async(id).await.is_err(),
            "unconfirmed bytes cannot become a cached successful acceptance"
        );
        assert!(store.get(id).is_err());
        assert!(store.accept_async(command(id)).await.is_err());
        store.evict_cached_state(); // simulated process restart, re-sync recovered bytes
        let (recovered, inserted) = store.accept_async(command(id)).await.unwrap();
        assert!(!inserted);
        assert_eq!(recovered.acceptance_hash, "frozen");
    }

    #[test]
    fn result_batch_replay_keeps_complete_prefix_before_a_torn_record() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let store = FanoutStore::open(dir.path()).unwrap();
        let id = Uuid::new_v4();
        let mut accepted = command(id);
        let first = accepted.members[0].member_id;
        let mut second = accepted.members[0].clone();
        second.member_id = Uuid::new_v4();
        let second_id = second.member_id;
        accepted.members.push(second);
        store.accept(accepted).unwrap();
        let result = |attempts| FanoutMemberOutcome {
            response: None,
            error: Some("terminal".into()),
            attempts,
            next_attempt_at_ms: None,
            terminal: true,
        };
        store
            .record_results(id, vec![(first, result(2)), (second_id, result(3))])
            .unwrap();
        let mut journal = std::fs::OpenOptions::new()
            .append(true)
            .open(&store.journal_path)
            .unwrap();
        journal
            .write_all(br#"{"version":1,"sequence":4,"event":"#)
            .unwrap();
        journal.sync_data().unwrap();
        drop(journal);
        store.evict_cached_state();
        let recovered = store.get(id).unwrap().unwrap();
        assert!(recovered.completed);
        assert_eq!(recovered.outcomes[&first].attempts, 2);
        assert_eq!(recovered.outcomes[&second_id].attempts, 3);
        store.record_results(id, vec![(first, result(4))]).unwrap();
        store.evict_cached_state();
        assert_eq!(store.get(id).unwrap().unwrap().outcomes[&first].attempts, 4);
    }
}
