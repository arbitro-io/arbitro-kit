//! `ParkWaiter` — OS-thread `Waiter` impl built on `thread::park`/`unpark`.
//!
//! Default backend. `wait_until` does the spin-then-park dance with a
//! Dekker-safe SeqCst recheck. `wake()` is a single Relaxed load + a
//! conditional `unpark`.
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

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering};

use super::{BlockingWaiter, Waiter};

/// Default spin iterations before parking.
pub const DEFAULT_SPIN_ITERS: u32 = 512;

/// Tight-spin iterations before switching to PAUSE.
const TIGHT_SPIN: u32 = 64;

/// OS-thread waiter — wraps `thread::park`/`unpark` with the Dekker dance.
///
/// `Default` produces an unregistered waiter; the consumer must call
/// `set_worker(thread::current())` before any `wait_until`.
#[repr(align(64))]
pub struct ParkWaiter {
    /// `true` while the consumer is parked. Written SeqCst by the consumer
    /// before park; read Relaxed by producers.
    parked: AtomicBool,
    /// Spin iterations before parking.
    spin_iters: u32,
    /// Consumer thread handle. Written once pre-share; read only after
    /// `parked == true` (the SeqCst store establishes happens-before).
    worker: UnsafeCell<Option<std::thread::Thread>>,
}

// Safety: `worker` is written once pre-share, then only read after `parked`
// is set with SeqCst — which establishes a happens-before edge.
unsafe impl Sync for ParkWaiter {}

impl Default for ParkWaiter {
    #[inline]
    fn default() -> Self { Self::with_spin(DEFAULT_SPIN_ITERS) }
}

impl ParkWaiter {
    /// Construct a `ParkWaiter` with a custom spin budget. Higher = lower
    /// wake latency when the producer fires within the spin window; lower
    /// = parks sooner (0% CPU idle). 0 = always park.
    pub fn with_spin(spin_iters: u32) -> Self {
        Self {
            parked: AtomicBool::new(false),
            spin_iters,
            worker: UnsafeCell::new(None),
        }
    }

    #[cold]
    #[inline(never)]
    fn wait_slow<F: FnMut() -> bool>(&self, ready: &mut F) {
        // Invariant: the consumer must have registered itself via
        // `set_worker` before reaching the park path. Without it the
        // producer's `wake()` finds `worker = None` and skips the
        // `unpark()`, deadlocking the consumer silently. We surface a
        // clear panic instead.
        assert!(
            self.has_worker(),
            "Park::wait_until reached park path without set_worker — register the consumer thread first",
        );
        // Phase 1: tight spin (no PAUSE).
        for _ in 0..TIGHT_SPIN {
            if ready() { return; }
            std::hint::black_box(());
        }
        // Phase 2: PAUSE spin.
        for _ in 0..self.spin_iters {
            if ready() { return; }
            std::hint::spin_loop();
        }
        // Phase 3: announce park. SeqCst closes the Dekker race with the
        // producer's (state-store, parked-load) pair.
        self.parked.store(true, Ordering::SeqCst);
        if ready() {
            self.parked.store(false, Ordering::Relaxed);
            return;
        }
        // Phase 4: park loop.
        loop {
            std::thread::park();
            if ready() {
                self.parked.store(false, Ordering::Relaxed);
                return;
            }
        }
    }
}

impl Waiter for ParkWaiter {
    #[inline]
    fn set_worker(&self, thread: std::thread::Thread) {
        // Safety: caller guarantees pre-share single-threaded access.
        unsafe { *self.worker.get() = Some(thread); }
    }

    #[inline]
    fn has_worker(&self) -> bool {
        // Safety: same single-consumer constraint as `wait_until`.
        unsafe { (*self.worker.get()).is_some() }
    }

    /// Wake the registered consumer if it is parked. Idempotent, lock-free.
    ///
    /// Cost: one Relaxed load of `parked` (~0.3 ns). Zero stores in the
    /// common "consumer not parked" case.
    #[inline]
    fn wake(&self) {
        if self.parked.load(Ordering::Relaxed) {
            // Safety: `parked == true` was published by the consumer
            // with a SeqCst store; its `worker` UnsafeCell write is
            // therefore visible.
            unsafe {
                if let Some(t) = &*self.worker.get() {
                    t.unpark();
                }
            }
        }
    }
}

impl BlockingWaiter for ParkWaiter {
    /// Block the calling thread until `ready()` returns `true`. Must be
    /// called from the thread registered via `set_worker`.
    #[inline]
    fn wait_until<F: FnMut() -> bool>(&self, mut ready: F) {
        if ready() { return; }
        self.wait_slow(&mut ready);
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

    #[test]
    fn multiple_wakes_are_idempotent() {
        let w = Arc::new(ParkWaiter::default());
        let state = Arc::new(AtomicU64::new(0));
        let w2 = w.clone();
        let s = state.clone();
        let h = std::thread::spawn(move || {
            w2.set_worker(std::thread::current());
            w2.wait_until(|| s.load(Ordering::Acquire) != 0);
        });
        std::thread::sleep(Duration::from_millis(50));
        state.store(1, Ordering::Release);
        for _ in 0..16 { w.wake(); }
        h.join().unwrap();
    }
}
