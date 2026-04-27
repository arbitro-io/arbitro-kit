//! `ParkWaiter` — OS-thread `Waiter` impl built on [`Park`](crate::gate::Park).
//!
//! Default backend. `wait_until` does the spin-then-park dance proven by
//! `Signal`/`Park` (Dekker-safe SeqCst recheck). `wake()` is a single
//! Relaxed load + conditional unpark.
//!
//! ## Cost
//!
//! | Path                              |            Cost |
//! | --------------------------------- | --------------: |
//! | `wake()` consumer-not-parked      |          ~0.3 ns |
//! | `wake()` consumer-parked          | ~7 µs (syscall) |
//! | `wait_until()` ready on entry     |          ~0.5 ns |
//! | `wait_until()` park path extra    |  +20 ns (1 SeqCst) |
//! | CPU while parked                  |              0% |
//!
//! ## Concurrency contract
//!
//! - Exactly **one consumer** thread calls `wait_until`. It must register
//!   itself first via `set_worker(thread::current())` — `wait_until` panics
//!   if reached without a registered worker (the alternative is a silent
//!   deadlock).
//! - Any number of producers may call `wake()` from any thread.

use crate::gate::Park;

use super::{BlockingWaiter, Waiter};

/// OS-thread waiter — wraps [`Park`](crate::gate::Park).
///
/// `Default` produces an unregistered waiter; the consumer must call
/// `set_worker(thread::current())` before any `wait_until`.
#[repr(transparent)]
pub struct ParkWaiter {
    inner: Park,
}

impl Default for ParkWaiter {
    #[inline]
    fn default() -> Self {
        Self { inner: Park::new() }
    }
}

impl Waiter for ParkWaiter {
    #[inline]
    fn set_worker(&self, thread: std::thread::Thread) {
        self.inner.set_worker(thread);
    }

    #[inline]
    fn has_worker(&self) -> bool {
        self.inner.has_worker()
    }

    #[inline]
    fn wake(&self) {
        self.inner.wake();
    }
}

impl BlockingWaiter for ParkWaiter {
    #[inline]
    fn wait_until<F: FnMut() -> bool>(&self, ready: F) {
        self.inner.wait_until(ready);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn fast_path_already_ready() {
        let w = ParkWaiter::default();
        // Predicate true on entry → no park, no panic about set_worker.
        w.wait_until(|| true);
    }

    #[test]
    fn wake_after_state_change_releases_parked_thread() {
        let w = Arc::new(ParkWaiter::default());
        let state = Arc::new(AtomicU64::new(0));
        let w2 = w.clone();
        let s = state.clone();
        let h = std::thread::spawn(move || {
            w2.set_worker(std::thread::current());
            w2.wait_until(|| s.load(Ordering::Acquire) != 0);
            assert_eq!(s.load(Ordering::Relaxed), 42);
        });
        std::thread::sleep(Duration::from_millis(50));
        state.store(42, Ordering::Release);
        w.wake();
        h.join().unwrap();
    }

    #[test]
    #[should_panic(expected = "Park::wait_until reached park path without set_worker")]
    fn wait_until_without_set_worker_panics() {
        let w = ParkWaiter::default();
        w.wait_until(|| false);
    }
}
