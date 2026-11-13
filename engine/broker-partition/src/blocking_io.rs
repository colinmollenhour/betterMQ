//! Bound broker storage work before it enters Tokio's native blocking pool.

use std::sync::{Arc, OnceLock};
use tokio::sync::Semaphore;
use tokio::task::JoinError;

static IO_CAPACITY: OnceLock<Arc<Semaphore>> = OnceLock::new();

/// Run blocking broker I/O without growing one native thread per contending
/// request. Excess callers wait asynchronously; Tokio's other blocking work is
/// not subject to this budget. Running I/O retains its permit if its caller is
/// cancelled. Closures must not recursively wait on this same I/O budget.
pub fn run_blocking_io<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
) -> impl std::future::Future<Output = Result<T, JoinError>> + Send + 'static {
    let operation = crate::tenant_scope::inherit_request_tenant(operation);
    let capacity = IO_CAPACITY
        .get_or_init(|| {
            let limit = std::env::var("BETTERMQ_BLOCKING_IO_CONCURRENCY")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(32)
                .clamp(1, Semaphore::MAX_PERMITS);
            Arc::new(Semaphore::new(limit))
        })
        .clone();
    run_with_budget(capacity, operation)
}

async fn run_with_budget<T: Send + 'static>(
    capacity: Arc<Semaphore>,
    operation: impl FnOnce() -> T + Send + 'static,
) -> Result<T, JoinError> {
    let permit = capacity
        .acquire_owned()
        .await
        .expect("broker I/O capacity is never closed");
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation()
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn excess_work_waits_without_occupying_blocking_threads() {
        let capacity = Arc::new(Semaphore::new(2));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut tasks = Vec::new();
        let mut releases = Vec::new();
        for _ in 0..12 {
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
            releases.push(release_tx);
            let (capacity, active, peak, started) = (
                capacity.clone(),
                active.clone(),
                peak.clone(),
                started_tx.clone(),
            );
            tasks.push(tokio::spawn(async move {
                run_with_budget(capacity, move || {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    started.send(()).unwrap();
                    // Dropping the test's senders on panic also unblocks every
                    // closure, so a failed assertion cannot hang runtime drop.
                    let _ = release_rx.recv();
                    active.fetch_sub(1, Ordering::SeqCst);
                })
                .await
                .unwrap();
            }));
        }
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(5), started_rx.recv())
                .await
                .unwrap()
                .unwrap();
        }
        tokio::task::yield_now().await;
        assert!(started_rx.try_recv().is_err());
        assert_eq!(active.load(Ordering::SeqCst), 2);
        for release in releases {
            let _ = release.send(());
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert_eq!(capacity.available_permits(), 2);
    }

    #[tokio::test]
    async fn cancelled_callers_do_not_release_running_io_capacity() {
        let capacity = Arc::new(Semaphore::new(1));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::sync_channel(1);
        let first_capacity = capacity.clone();
        let first = tokio::spawn(async move {
            run_with_budget(first_capacity, move || {
                started_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
            })
            .await
        });
        started_rx.await.unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(capacity.available_permits(), 0);
        let second_capacity = capacity.clone();
        let mut second = tokio::spawn(async move { run_with_budget(second_capacity, || 42).await });
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut second)
            .await
            .is_err());
        finish_tx.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), second)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            42
        );
        assert_eq!(capacity.available_permits(), 1);
    }

    #[tokio::test]
    async fn cancelled_waiters_never_start_io_or_consume_later_capacity() {
        let capacity = Arc::new(Semaphore::new(0));
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let waiting_capacity = capacity.clone();
        let waiting_called = called.clone();
        let waiting = tokio::spawn(async move {
            run_with_budget(waiting_capacity, move || {
                waiting_called.store(true, Ordering::SeqCst)
            })
            .await
        });
        tokio::task::yield_now().await;
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        capacity.add_permits(1);
        assert_eq!(run_with_budget(capacity.clone(), || 42).await.unwrap(), 42);
        assert!(!called.load(Ordering::SeqCst));
        assert_eq!(capacity.available_permits(), 1);
    }

    #[tokio::test]
    async fn panicking_work_releases_capacity() {
        let capacity = Arc::new(Semaphore::new(1));
        let result: Result<(), _> =
            run_with_budget(capacity.clone(), || panic!("I/O panic probe")).await;
        assert!(result.unwrap_err().is_panic());
        assert_eq!(capacity.available_permits(), 1);
    }
}
