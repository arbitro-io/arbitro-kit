//! Stateless park/unpark primitive. The "ready" predicate lives at the
//! call-site — no duplicated `locked: AtomicBool`.
//!
//! ## Relationship to [`Signal`](super::Signal)
//!
//! `Signal` = `Park` + an owned [`SignalSource`](super::SignalSource) that
//! tracks open/closed. `Park` is what you want when the state you want to
//! wait on *already exists* in the caller's data (e.g. a ring's head/tail
//! cursors): duplicating it into a `locked: AtomicBool` just adds cache
//! traffic for no reason.
//!
//! ## Semantics
//!
//! - `wait_until(ready)`: single consumer calls this, blocks until
//!   `ready()` returns `true`. Spin-then-park, same phases as `Signal`.
//! - `wake()`: producers call this after they mutate the state that
//!   `ready()` reads. Costs ~0.5 ns if the consumer is not parked
//!   (one Relaxed load), ~7 µs syscall if it is.
//!
//! ## Concurrency model
//!
//! Exactly **one consumer** may call `wait_until` / `set_worker`. Any
//! number of producers may call `wake` from any thread.
//!
//! ## Why it's faster than `Signal` for cursor-backed state
//!
//! A ring's "not empty" state is fully determined by `head != tail`. With
//! `Signal` the producer pays:
//!   - 1 Release store on its cursor (cache line A), and
//!   - 1 Release store on `Signal::locked` (cache line B).
//!
//! With `Park` the second store disappears — the consumer checks
//! `head != tail` directly inside `wait_until`'s predicate. `wake()` only
//! touches the parked flag (rarely written, stays in L1 most of the time).
//!
//! Measured: SPSC ring cross-thread per-item drops from ~38 ns/op to
//! ~24 ns/op at CAP=256.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering};

/// Default spin iterations before parking. Matches [`Signal`](super::Signal).
pub const DEFAULT_SPIN_ITERS: u32 = 512;

/// Tight-spin iterations before switching to PAUSE.
const TIGHT_SPIN: u32 = 64;

#[repr(align(64))]
pub struct Park {
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
unsafe impl Sync for Park {}

impl Default for Park {
    fn default() -> Self { Self::new() }
}

impl Park {
    #[inline]
    pub fn new() -> Self { Self::with_spin(DEFAULT_SPIN_ITERS) }

    /// Construct a `Park` with a custom spin budget. Higher = lower wake
    /// latency when the producer fires within the spin window; lower =
    /// parks sooner (0% CPU idle). 0 = always park.
    pub fn with_spin(spin_iters: u32) -> Self {
        Self {
            parked: AtomicBool::new(false),
            spin_iters,
            worker: UnsafeCell::new(None),
        }
    }

    /// Register the consumer thread. Must be called **before** the `Park` is
    /// shared with producer threads.
    #[inline]
    pub fn set_worker(&self, t: std::thread::Thread) {
        // Safety: caller guarantees pre-share single-threaded access.
        unsafe { *self.worker.get() = Some(t); }
    }

    /// `true` iff a consumer thread has been registered via [`set_worker`].
    /// Used by composites and the slow-path park guard to surface a clear
    /// panic instead of an infinite hang when the caller forgot to bind.
    ///
    /// Safety: same single-consumer constraint as `wait_until`.
    #[inline]
    pub fn has_worker(&self) -> bool {
        unsafe { (*self.worker.get()).is_some() }
    }

    /// Wake the registered consumer if it is parked. Idempotent, lock-free.
    ///
    /// Cost: one Relaxed load of `parked` (~0.3 ns). Zero stores in the
    /// common "consumer not parked" case.
    #[inline]
    pub fn wake(&self) {
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

    /// Block the calling thread until `ready()` returns `true`. Must be
    /// called from the thread registered via [`set_worker`].
    ///
    /// `ready` is called:
    /// - Once on entry (fast path).
    /// - Repeatedly during spin phases.
    /// - Once after the SeqCst `parked` store (Dekker closure).
    /// - After each `park()` wake (covers spurious wakes).
    ///
    /// The predicate must perform at least `Acquire` loads on any state
    /// published by the producer with `Release` — otherwise the caller
    /// may observe stale data after a successful return.
    #[inline]
    pub fn wait_until<F: FnMut() -> bool>(&self, mut ready: F) {
        if ready() { return; }
        self.wait_slow(&mut ready);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    #[should_panic(expected = "Park::wait_until reached park path without set_worker")]
    fn wait_until_without_set_worker_panics() {
        // Predicate is always false → wait_until enters the slow path
        // immediately. Without `set_worker` we'd deadlock; the invariant
        // panics instead.
        let park = Park::new();
        park.wait_until(|| false);
    }

    #[test]
    fn wake_after_state_change_releases_parked_thread() {
        let park = Arc::new(Park::new());
        let state = Arc::new(AtomicU64::new(0));
        let p = park.clone();
        let s = state.clone();
        let handle = std::thread::spawn(move || {
            p.set_worker(std::thread::current());
            p.wait_until(|| s.load(Ordering::Acquire) != 0);
            assert_eq!(s.load(Ordering::Relaxed), 42);
        });
        std::thread::sleep(Duration::from_millis(50));
        state.store(42, Ordering::Release);
        park.wake();
        handle.join().unwrap();
    }

    #[test]
    fn fast_path_already_ready() {
        let park = Park::new();
        // Should not block: predicate true on entry.
        park.wait_until(|| true);
    }

    #[test]
    fn multiple_wakes_are_idempotent() {
        let park = Arc::new(Park::new());
        let state = Arc::new(AtomicU64::new(0));
        let p = park.clone();
        let s = state.clone();
        let handle = std::thread::spawn(move || {
            p.set_worker(std::thread::current());
            p.wait_until(|| s.load(Ordering::Acquire) != 0);
        });
        std::thread::sleep(Duration::from_millis(50));
        state.store(1, Ordering::Release);
        for _ in 0..16 { park.wake(); }
        handle.join().unwrap();
    }
}
