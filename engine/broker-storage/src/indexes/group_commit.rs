//! One bounded durable writer per local metadata store.

use super::{IndexError, MetadataStore, MetadataWriteBatch};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use tokio::sync::{mpsc, oneshot};

const QUEUE_CAPACITY: usize = 256;
const MAX_GROUP_REQUESTS: usize = 64;
const MAX_GROUP_OPERATIONS: usize = 4096;
const MAX_GROUP_BYTES: usize = 4 * 1024 * 1024;

/// Keeps cancellation-sensitive ownership until the caller's post-commit work
/// is finished. If the caller disappears, the writer drops it after persistence.
pub struct MetadataWriteReceipt {
    _held: Box<dyn Send>,
}

struct WriteIntent {
    batch: MetadataWriteBatch,
    held: Box<dyn Send>,
    reply: oneshot::Sender<Result<MetadataWriteReceipt, IndexError>>,
}

type Exclusive = Box<dyn FnOnce(Result<&MetadataStore, IndexError>) -> bool + Send>;

#[cfg(test)]
type WriterGate = Arc<Mutex<Option<(oneshot::Sender<()>, std::sync::mpsc::Receiver<()>)>>>;

enum Command {
    Write(WriteIntent),
    Exclusive(Exclusive),
}

pub(super) struct MetadataWriter {
    sender: Option<mpsc::Sender<Command>>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    poisoned: Arc<AtomicBool>,
    epochs: Arc<AtomicU64>,
    #[cfg(test)]
    pub(super) gate: WriterGate,
    #[cfg(test)]
    pub(super) fail_next_sync: Arc<AtomicBool>,
}

fn unavailable(message: impl Into<String>) -> IndexError {
    std::io::Error::other(message.into()).into()
}

impl MetadataWriter {
    pub(super) fn start(store: MetadataStore) -> Result<Arc<Self>, IndexError> {
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        let poisoned = Arc::new(AtomicBool::new(false));
        let epochs = Arc::new(AtomicU64::new(0));
        #[cfg(test)]
        let gate = Arc::new(Mutex::new(None));
        #[cfg(test)]
        let fail_next_sync = Arc::new(AtomicBool::new(false));
        let poison = poisoned.clone();
        let counter = epochs.clone();
        #[cfg(test)]
        let thread_gate = gate.clone();
        #[cfg(test)]
        let thread_failure = fail_next_sync.clone();
        let thread = std::thread::Builder::new()
            .name("bettermq-meta-io".into())
            .spawn(move || {
                // A panic must fence reads and future writes, not leave a seemingly
                // healthy store whose only writer has vanished.
                struct PoisonOnPanic(Arc<AtomicBool>);
                impl Drop for PoisonOnPanic {
                    fn drop(&mut self) {
                        if std::thread::panicking() {
                            self.0.store(true, Ordering::Release);
                        }
                    }
                }
                let _panic_guard = PoisonOnPanic(poison.clone());
                run(
                    store,
                    receiver,
                    poison,
                    counter,
                    #[cfg(test)]
                    thread_gate,
                    #[cfg(test)]
                    thread_failure,
                );
            })?;
        Ok(Arc::new(Self {
            sender: Some(sender),
            thread: Mutex::new(Some(thread)),
            poisoned,
            epochs,
            #[cfg(test)]
            gate,
            #[cfg(test)]
            fail_next_sync,
        }))
    }

    pub(super) fn check_ready(&self) -> Result<(), IndexError> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(unavailable(
                "metadata writer is fenced after an uncertain failure; restart to recover",
            ));
        }
        Ok(())
    }

    pub(super) async fn write(
        &self,
        batch: MetadataWriteBatch,
        held: Box<dyn Send>,
    ) -> Result<MetadataWriteReceipt, IndexError> {
        self.check_ready()?;
        let (reply, receiver) = oneshot::channel();
        self.sender
            .as_ref()
            .expect("live metadata writer")
            .send(Command::Write(WriteIntent { batch, held, reply }))
            .await
            .map_err(|_| unavailable("metadata writer stopped"))?;
        receiver
            .await
            .map_err(|_| unavailable("metadata writer stopped before its durable reply"))?
    }

    pub(super) fn write_sync(&self, batch: MetadataWriteBatch) -> Result<(), IndexError> {
        // Tokio channels need no runtime driver. The futures executor avoids
        // blocking_recv/blocking_send panics in legacy synchronous callers that
        // happen to be invoked from a current-thread Tokio runtime.
        futures::executor::block_on(self.write(batch, Box::new(()))).map(drop)
    }

    pub(super) fn exclusive<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&MetadataStore) -> Result<T, IndexError> + Send + 'static,
    ) -> Result<T, IndexError> {
        self.check_ready()?;
        let (reply, receiver) = oneshot::channel();
        let poisoned = self.poisoned.clone();
        let command = Command::Exclusive(Box::new(move |store| {
            let result = store.and_then(operation);
            let poison = matches!(result, Err(IndexError::Rocks(_)));
            if poison {
                poisoned.store(true, Ordering::Release);
            }
            let _ = reply.send(result);
            poison
        }));
        futures::executor::block_on(
            self.sender
                .as_ref()
                .expect("live metadata writer")
                .send(command),
        )
        .map_err(|_| unavailable("metadata writer stopped"))?;
        futures::executor::block_on(receiver)
            .map_err(|_| unavailable("metadata writer stopped before its durable reply"))?
    }

    pub(super) fn epoch_count(&self) -> u64 {
        self.epochs.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) fn available_queue_slots(&self) -> usize {
        self.sender.as_ref().unwrap().capacity()
    }
}

impl Drop for MetadataWriter {
    fn drop(&mut self) {
        // Closing the last sender lets the writer drain queued work before the
        // DB lock is released, so immediate reopen after drop remains safe.
        drop(self.sender.take());
        if let Some(thread) = self
            .thread
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            // An opaque guard is allowed to own the final store handle. Its
            // destruction on this writer must never try to join itself.
            if thread.thread().id() != std::thread::current().id() {
                let _ = thread.join();
            }
        }
    }
}

fn run(
    store: MetadataStore,
    mut receiver: mpsc::Receiver<Command>,
    poisoned: Arc<AtomicBool>,
    epochs: Arc<AtomicU64>,
    #[cfg(test)] gate: WriterGate,
    #[cfg(test)] fail_next_sync: Arc<AtomicBool>,
) {
    let mut deferred = None;
    while let Some(command) = deferred.take().or_else(|| receiver.blocking_recv()) {
        let first = match command {
            Command::Exclusive(operation) => {
                let result = if poisoned.load(Ordering::Acquire) {
                    Err(unavailable("metadata writer is fenced; restart to recover"))
                } else {
                    Ok(&store)
                };
                if operation(result) {
                    poisoned.store(true, Ordering::Release);
                }
                continue;
            }
            Command::Write(first) => first,
        };
        let mut operations = first.batch.operations.len();
        let mut bytes = first.batch.estimated_bytes();
        let mut group = vec![first];
        while group.len() < MAX_GROUP_REQUESTS
            && operations < MAX_GROUP_OPERATIONS
            && bytes < MAX_GROUP_BYTES
        {
            match receiver.try_recv() {
                Ok(Command::Write(next))
                    if operations.saturating_add(next.batch.operations.len())
                        <= MAX_GROUP_OPERATIONS
                        && bytes.saturating_add(next.batch.estimated_bytes())
                            <= MAX_GROUP_BYTES =>
                {
                    operations += next.batch.operations.len();
                    bytes += next.batch.estimated_bytes();
                    group.push(next);
                }
                Ok(other) => {
                    deferred = Some(other);
                    break;
                }
                Err(_) => break,
            }
        }
        let mut batch = MetadataWriteBatch::default();
        for intent in &mut group {
            batch.operations.append(&mut intent.batch.operations);
        }
        #[cfg(test)]
        if let Some((started, release)) = gate.lock().unwrap().take() {
            let _ = started.send(());
            assert!(
                release
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .is_ok(),
                "metadata writer gate timed out"
            );
        }
        let result = if poisoned.load(Ordering::Acquire) {
            Err(unavailable("metadata writer is fenced; restart to recover"))
        } else {
            #[cfg(test)]
            {
                if fail_next_sync.swap(false, Ordering::SeqCst) {
                    poisoned.store(true, Ordering::Release);
                    Err(unavailable("injected uncertain metadata sync failure"))
                } else {
                    store.write_batch_direct(batch, group.len() as u64)
                }
            }
            #[cfg(not(test))]
            store.write_batch_direct(batch, group.len() as u64)
        };
        if result.is_ok() && operations != 0 {
            epochs.fetch_add(1, Ordering::Relaxed);
        }
        if matches!(result, Err(IndexError::Rocks(_))) {
            poisoned.store(true, Ordering::Release);
        }
        let error = result.err().map(|error| error.to_string());
        for intent in group {
            let result = match &error {
                Some(message) => Err(unavailable(message.clone())),
                None => Ok(MetadataWriteReceipt { _held: intent.held }),
            };
            let _ = intent.reply.send(result);
        }
    }
}
