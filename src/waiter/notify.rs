//! `NotifyWaiter` — async `Waiter` impl built on `tokio::sync::Notify`.
//!
//! Available behind `feature = "tokio"`. Use this backend when the wake
//! fires from a non-tokio thread (TCP reader, FFI callback, OS-thread
//! worker) and the waiter is a tokio task — the runtime multiplexes the
//! wake onto a hot worker, beating `thread::unpark` to a cold pinned
//! thread by ~2.4× on real I/O round-trips (measured: TCP-loopback
//! release_primitive at P=128, ~8.2 µs vs ~20 µs).
//!
//! ## Cost
//!
//! | Path                              |            Cost |
//! | --------------------------------- | --------------: |
//! | `wake()` no waiter                |          ~5 ns |
//! | `wake()` waiter pending           |       ~300 ns (runtime enqueue) |
//! | `wait_until()` ready on entry     |          ~50 ns (future build) |
//! | `wait_until()` await round        |       ~300 ns |
//!
//! Without I/O in the path, [`ParkWaiter`](super::ParkWaiter) is faster
//! (~50 ns wake). Use this one specifically for the OS↔tokio bridge.
//!
//! ## Lost-notify race
//!
//! The `notified()` future is built BEFORE the predicate check. Without
//! that, a `notify_one` racing between the check and the await would be
//! lost — `Notify` only "remembers" a notification if a `notified()`
//! future was already registered when it fired.
//!
//! ## Concurrency contract
//!
//! - Any number of producers may call `wake()`.
//! - Exactly one consumer task calls `wait_until` and must be polled
//!   from a tokio runtime.
//! - `set_worker` is a no-op (the runtime tracks tasks itself).

use std::future::Future;

use tokio::sync::Notify;

use super::{AsyncWaiter, Waiter};

/// Async waiter — wraps [`tokio::sync::Notify`].
///
/// `Default` produces a fresh `Notify`. No registration step.
#[derive(Default)]
pub struct NotifyWaiter {
    pub(crate) inner: Notify,
}

impl Waiter for NotifyWaiter {
    /// No-op: tokio tracks tasks itself, no thread handle needed.
    #[inline]
    fn set_worker(&self, _thread: std::thread::Thread) {}

    /// Always `true`: the runtime is the worker.
    #[inline]
    fn has_worker(&self) -> bool {
        true
    }

    #[inline]
    fn wake(&self) {
        self.inner.notify_one();
    }
}

impl AsyncWaiter for NotifyWaiter {
    fn wait_until<'a, F>(&'a self, mut ready: F) -> impl Future<Output = ()> + Send + 'a
    where
        F: FnMut() -> bool + Send + 'a,
    {
        async move {
            loop {
                // BUILD the notified() future BEFORE checking the predicate.
                // Without this, a producer firing `notify_one` between the
                // check and the await would be lost.
                let notified = self.inner.notified();
                if ready() {
                    return;
                }
                notified.await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn fast_path_already_ready() {
        let w = NotifyWaiter::default();
        w.wait_until(|| true).await;
    }

    #[tokio::test]
    async fn wake_after_state_change_releases_awaiter() {
        let w = Arc::new(NotifyWaiter::default());
        let state = Arc::new(AtomicU64::new(0));
        let w2 = w.clone();
        let s = state.clone();
        let h = tokio::spawn(async move {
            w2.wait_until(move || s.load(Ordering::Acquire) != 0).await;
        });
        // Give the awaiter time to park.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        state.store(42, Ordering::Release);
        w.wake();
        h.await.unwrap();
        assert_eq!(state.load(Ordering::Relaxed), 42);
    }

    #[tokio::test]
    async fn cross_thread_wake_from_os_thread() {
        // Intended use case: producer runs on a plain OS thread (no tokio
        // context), waiter is a tokio task.
        let w = Arc::new(NotifyWaiter::default());
        let state = Arc::new(AtomicU64::new(0));
        let w2 = w.clone();
        let s = state.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            s.store(7, Ordering::Release);
            w2.wake();
        });
        let s2 = state.clone();
        w.wait_until(move || s2.load(Ordering::Acquire) != 0).await;
        assert_eq!(state.load(Ordering::Relaxed), 7);
    }
}
