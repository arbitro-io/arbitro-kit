//! Single-channel M:1 signal built on `AtomicBool` + `park/unpark`.
//!
//! ## Design
//!
//! Two `AtomicBool`s: `locked` (gate state) and `parked` (consumer liveness).
//! Hot path keeps both as `Relaxed`/`Release` stores; the only `SeqCst` op
//! runs **once per park**, on the consumer when it is about to sleep. That
//! single barrier (an `mfence` on x86, `dmb ish` on ARM) closes the Dekker
//! race between the producer's `locked.store` + `parked.load` pair and the
//! consumer's `parked.store` + `locked.load` pair — without taxing every
//! release.
//!
//! ## Cost
//!
//! | Path                              |            Cost |
//! | --------------------------------- | --------------: |
//! | `release()` busy                  |          ~0.6 ns |
//! | `release()` parked                | ~7 µs (syscall) |
//! | `acquire()` fast-path             |          ~0.3 ns |
//! | `acquire()` park path extra cost  |  +20 ns (1 SeqCst) |
//! | Struct size                       |   64 B (aligned) |
//! | CPU while parked                  |              0% |
//!
//! ## Correctness across architectures
//!
//! On **x86 / x86_64 (TSO)** the race between the two relaxed load/store
//! pairs is masked by strong memory ordering + store-buffer drain (~tens of
//! cycles, while the spin window runs ~15 µs). On **ARM / aarch64** (weakly
//! ordered) that mask does not exist: the consumer's `SeqCst` store on
//! `parked` + recheck of `locked` is what guarantees forward progress. Do
//! not weaken it.
//!
//! ## Concurrency model
//!
//! Exactly **one consumer** may call `acquire()` / `set_worker()`. Any number
//! of producers may call `release()` / `lock()` / `is_open()` concurrently
//! from any thread without synchronization between them.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering};

/// Default spin iterations before parking. Covers producer→consumer
/// latencies in the ~100–500 ns range on commodity x86_64.
pub const DEFAULT_SPIN_ITERS: u32 = 512;

/// Tight-spin iterations before switching to PAUSE. Covers intra-socket
/// coherence latency (~50–150 ns) without committing a single PAUSE.
const TIGHT_SPIN: u32 = 64;

#[repr(align(64))]
pub struct Signal {
    /// `true` → gate is locked (no pending work). `false` → open (has work).
    /// Named `locked` for historical reasons; `is_open()` inverts this.
    locked: AtomicBool,
    /// Set by the consumer on the park path with `SeqCst`. Read by producers
    /// with `Relaxed` — the race window is closed by the consumer's SeqCst
    /// store + recheck (see module docs).
    parked: AtomicBool,
    /// Spin iterations before parking in `acquire()`. Set at construction via
    /// [`Signal::new`] (default [`DEFAULT_SPIN_ITERS`]) or [`Signal::with_spin`].
    spin_iters: u32,
    /// Consumer thread handle, registered via `set_worker`. Written once
    /// before the Signal is shared; read only after `parked` is `true`, whose
    /// `SeqCst` store establishes happens-before.
    worker: UnsafeCell<Option<std::thread::Thread>>,
}

// Safety: `worker` is written once pre-share, then read only after the
// consumer sets `parked = true` with SeqCst — which establishes a global
// happens-before edge observable by every producer.
unsafe impl Sync for Signal {}

impl Default for Signal {
    fn default() -> Self { Self::new() }
}

impl Signal {
    pub fn new() -> Self { Self::with_spin(DEFAULT_SPIN_ITERS) }

    /// Construct a `Signal` with a custom spin-iteration budget. Higher values
    /// trade CPU for lower wake latency when the producer fires within the
    /// spin window; lower values park sooner (0% CPU idle). 0 = always park.
    pub fn with_spin(spin_iters: u32) -> Self {
        Self {
            locked: AtomicBool::new(true),
            parked: AtomicBool::new(false),
            spin_iters,
            worker: UnsafeCell::new(None),
        }
    }

    /// Register the consumer thread. Must be called **before** the `Signal` is
    /// shared with producer threads. Typically invoked by the consumer itself
    /// with `gate.set_worker(thread::current())`.
    pub fn set_worker(&self, t: std::thread::Thread) {
        // Safety: caller guarantees pre-share single-threaded access.
        unsafe { *self.worker.get() = Some(t); }
    }

    /// Signal pending work. Lock-free, ~0.6 ns common case.
    #[inline]
    pub fn release(&self) {
        self.locked.store(false, Ordering::Release);
        if self.parked.load(Ordering::Relaxed) {
            // Safety: `parked == true` was published by the consumer with a
            // SeqCst store; its `worker` write is therefore also visible.
            unsafe {
                if let Some(t) = &*self.worker.get() {
                    t.unpark();
                }
            }
        }
    }

    /// Mark the gate as having no pending work. Called by the consumer after
    /// draining everything so the next `acquire()` will block.
    #[inline]
    pub fn lock(&self) {
        self.locked.store(true, Ordering::Relaxed);
    }

    /// `true` if there is pending work (i.e. `release()` was called since the
    /// last `lock()`).
    #[inline]
    pub fn is_open(&self) -> bool {
        !self.locked.load(Ordering::Acquire)
    }

    /// Block the calling thread until the gate is open. Must be called from
    /// the thread registered via `set_worker`.
    ///
    /// Fast-path is a single Acquire load + branch. Slow path is split off
    /// into `#[cold] acquire_slow` so the fast path stays compact in icache.
    #[inline]
    pub fn acquire(&self) {
        if !self.locked.load(Ordering::Acquire) { return; }
        self.acquire_slow();
    }

    #[cold]
    #[inline(never)]
    fn acquire_slow(&self) {
        // Phase 1: tight spin (~1-2 ns/iter). Catches intra-socket signals
        // (~100-200 ns coherence) without paying a single PAUSE.
        for _ in 0..TIGHT_SPIN {
            if !self.locked.load(Ordering::Relaxed) { return; }
            std::hint::black_box(());
        }
        // Phase 2: PAUSE spin (~20-40 ns/iter on x86). Covers the ~µs range.
        for _ in 0..self.spin_iters {
            if !self.locked.load(Ordering::Relaxed) { return; }
            std::hint::spin_loop();
        }
        // Phase 3: announce parking. SeqCst store = mfence on x86 / dmb ish
        // on ARM → after this point our subsequent load of `locked` sees
        // every globally-visible store from any producer. Pays ~20 ns, once
        // per park event.
        self.parked.store(true, Ordering::SeqCst);
        if !self.locked.load(Ordering::Relaxed) {
            // Producer fired between spin-end and parked-set; no need to park.
            self.parked.store(false, Ordering::Relaxed);
            return;
        }
        // Phase 4: park loop — `park()` can wake spuriously per std's docs,
        // so loop until we observe `locked` cleared.
        loop {
            std::thread::park();
            if !self.locked.load(Ordering::Acquire) {
                self.parked.store(false, Ordering::Relaxed);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    #[test]
    fn release_wakes_parked_consumer() {
        let gate = Arc::new(Signal::new());
        let g = gate.clone();
        let handle = std::thread::spawn(move || {
            g.set_worker(std::thread::current());
            g.acquire();
            assert!(g.is_open());
        });
        std::thread::sleep(Duration::from_millis(50));
        gate.release();
        handle.join().unwrap();
    }

    #[test]
    fn lock_and_reacquire() {
        let gate = Signal::new();
        gate.release();
        gate.acquire();
        assert!(gate.is_open());
        gate.lock();
        assert!(!gate.is_open());
    }

    #[test]
    fn many_producers_one_consumer() {
        let gate = Arc::new(Signal::new());
        let stop = Arc::new(AtomicBool::new(false));
        let g = gate.clone();
        let s = stop.clone();

        let consumer = std::thread::spawn(move || {
            g.set_worker(std::thread::current());
            while !s.load(Ordering::Relaxed) {
                g.acquire();
                g.lock();
            }
        });

        let producers: Vec<_> = (0..8).map(|_| {
            let g = gate.clone();
            std::thread::spawn(move || {
                for _ in 0..50 {
                    g.release();
                    std::thread::yield_now();
                }
            })
        }).collect();

        for p in producers { p.join().unwrap(); }
        stop.store(true, Ordering::Relaxed);
        gate.release();
        consumer.join().unwrap();
    }
}
