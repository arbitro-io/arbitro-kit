//! Single-channel M:1 signal — packed OPEN + PARKED into one atomic byte.
//!
//! ## Design
//!
//! Both flags live in the **same `AtomicU8`**: bit 0 = open, bit 1 = parked.
//! Producer's `release()` and consumer's park-path both use `fetch_or` /
//! `fetch_and` RMW operations on that single location. Because every RMW
//! linearizes against every other RMW on the same atomic, there is **no
//! Dekker race possible** — one of the two RMWs always sees the other's
//! effect.
//!
//! Compare with the previous split-atomic design: that version stored
//! open/parked in two separate `AtomicBool`s and relied on a SeqCst store +
//! recheck handshake. Under heavy concurrency that handshake had a latent
//! StoreLoad-reordering window where both sides could miss the wake. The
//! packed design eliminates that window structurally.
//!
//! ## Cost
//!
//! | Path                              |              Cost |
//! | --------------------------------- | ----------------: |
//! | `release()` busy                  |   ~6 ns (`lock or`) |
//! | `release()` parked                |   ~7 µs (syscall) |
//! | `acquire()` fast-path             |   ~0.3 ns (load)  |
//! | `acquire()` park path             |  ~6 ns + park cost|
//! | Struct size                       |    64 B (aligned) |
//! | CPU while parked                  |               0% |
//!
//! Trade-off vs. the split design: the uncontended `release` path now pays
//! a `lock or` (~6 ns) instead of a relaxed store + relaxed load (~0.6 ns).
//! That's a ~10× hit on the hottest path of releases without contention,
//! but in exchange the wake protocol becomes architecturally bug-free
//! (works on x86 TSO and on weakly-ordered ARM identically) and the
//! consumer no longer needs the SeqCst mfence on every park.
//!
//! ## Concurrency model
//!
//! Exactly **one consumer** may call `acquire()` / `set_worker()`. Any number
//! of producers may call `release()` / `lock()` / `is_open()` concurrently
//! from any thread without synchronization between them.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU8, Ordering};

/// Default spin iterations before parking. Covers producer→consumer
/// latencies in the ~100–500 ns range on commodity x86_64.
pub const DEFAULT_SPIN_ITERS_PACKED: u32 = 512;

/// Tight-spin iterations before switching to PAUSE. Covers intra-socket
/// coherence latency (~50–150 ns) without committing a single PAUSE.
const TIGHT_SPIN: u32 = 64;

// ─── Bit layout ───────────────────────────────────────────────────────────

/// Bit 0 — gate is open (producer has work to deliver).
const OPEN:   u8 = 0b0000_0001;
/// Bit 1 — consumer has parked / is about to park.
const PARKED: u8 = 0b0000_0010;

// ─── SignalPacked ──────────────────────────────────────────────────────────────

#[repr(align(64))]
pub struct SignalPacked {
    /// Packed flags: bit 0 = OPEN, bit 1 = PARKED. All transitions are
    /// `fetch_or` / `fetch_and` — single-location RMW eliminates the
    /// open/parked Dekker race that the previous split-atomic design had.
    state: AtomicU8,
    /// Spin iterations before parking in `acquire()`. Set at construction.
    spin_iters: u32,
    /// Consumer thread handle, registered via `set_worker`. Written once
    /// before the SignalPacked is shared; read only after the PARKED bit has
    /// been set with AcqRel — which establishes happens-before with the
    /// `worker` write since the consumer always sets PARKED *after*
    /// `set_worker`.
    worker: UnsafeCell<Option<std::thread::Thread>>,
}

// Safety: `worker` is written once pre-share, then read only after the
// consumer sets PARKED with an AcqRel RMW — which establishes a global
// happens-before edge observable by every producer.
unsafe impl Sync for SignalPacked {}

impl Default for SignalPacked {
    fn default() -> Self { Self::new() }
}

impl SignalPacked {
    pub fn new() -> Self { Self::with_spin(DEFAULT_SPIN_ITERS_PACKED) }

    /// Construct a `SignalPacked` with a custom spin-iteration budget. Higher values
    /// trade CPU for lower wake latency when the producer fires within the
    /// spin window; lower values park sooner (0% CPU idle). 0 = always park.
    pub fn with_spin(spin_iters: u32) -> Self {
        Self {
            state: AtomicU8::new(0),
            spin_iters,
            worker: UnsafeCell::new(None),
        }
    }

    /// Register the consumer thread. Must be called **before** the `SignalPacked` is
    /// shared with producer threads. Typically invoked by the consumer itself
    /// with `gate.set_worker(thread::current())`.
    pub fn set_worker(&self, t: std::thread::Thread) {
        // Safety: caller guarantees pre-share single-threaded access.
        unsafe { *self.worker.get() = Some(t); }
    }

    /// SignalPacked pending work.
    ///
    /// Sets the OPEN bit and atomically reads back the previous full state.
    /// If PARKED was set in the previous state, the consumer is parked or
    /// about to park — we unpark it. Because the `fetch_or` linearizes
    /// against the consumer's own `fetch_or(PARKED)`, exactly one of the
    /// following holds:
    ///
    /// - Our RMW ran before the consumer's: consumer's RMW sees OPEN, skips
    ///   park.
    /// - Consumer's RMW ran before ours: ours sees PARKED, unparks.
    ///
    /// No Dekker race is possible.
    #[inline]
    pub fn release(&self) {
        let prev = self.state.fetch_or(OPEN, Ordering::AcqRel);
        if prev & PARKED != 0 {
            // Safety: PARKED was set by the consumer with an AcqRel RMW
            // *after* it had already called `set_worker`. The RMW pair
            // establishes happens-before with that earlier write.
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
        self.state.fetch_and(!OPEN, Ordering::Release);
    }

    /// `true` if there is pending work (i.e. `release()` was called since
    /// the last `lock()`).
    #[inline]
    pub fn is_open(&self) -> bool {
        self.state.load(Ordering::Acquire) & OPEN != 0
    }

    /// Block the calling thread until the gate is open. Must be called from
    /// the thread registered via `set_worker`.
    #[inline]
    pub fn acquire(&self) {
        if self.is_open() { return; }
        self.acquire_slow();
    }

    #[cold]
    #[inline(never)]
    fn acquire_slow(&self) {
        // Phase 1: tight spin (~1-2 ns/iter). Catches intra-socket signals
        // (~100-200 ns coherence) without paying a single PAUSE.
        for _ in 0..TIGHT_SPIN {
            if self.is_open() { return; }
            std::hint::black_box(());
        }
        // Phase 2: PAUSE spin (~20-40 ns/iter on x86). Covers the ~µs range.
        for _ in 0..self.spin_iters {
            if self.is_open() { return; }
            std::hint::spin_loop();
        }
        // Phase 3: announce parking via packed RMW. AcqRel: this RMW
        // linearizes against the producer's `fetch_or(OPEN)`. If our RMW
        // wins, the producer's later RMW will see PARKED and unpark us.
        // If the producer's RMW already happened, we observe OPEN here and
        // skip parking.
        let prev = self.state.fetch_or(PARKED, Ordering::AcqRel);
        if prev & OPEN != 0 {
            // Already open — clear our PARKED bit and bail.
            self.state.fetch_and(!PARKED, Ordering::Relaxed);
            return;
        }
        // Phase 4: park loop — `park()` can wake spuriously per std's docs,
        // so loop until we observe OPEN.
        loop {
            std::thread::park();
            if self.is_open() {
                self.state.fetch_and(!PARKED, Ordering::Relaxed);
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
        let gate = Arc::new(SignalPacked::new());
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
        let gate = SignalPacked::new();
        gate.release();
        gate.acquire();
        assert!(gate.is_open());
        gate.lock();
        assert!(!gate.is_open());
    }

    #[test]
    fn many_producers_one_consumer() {
        let gate = Arc::new(SignalPacked::new());
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

    #[test]
    fn canonical_acquire_drain_pattern() {
        use std::sync::atomic::AtomicU64;

        const N: u64 = 1000;
        let gate = Arc::new(SignalPacked::new());
        let produced = Arc::new(AtomicU64::new(0));
        let consumed = Arc::new(AtomicU64::new(0));
        let done     = Arc::new(AtomicBool::new(false));

        let g = gate.clone();
        let p = produced.clone();
        let c = consumed.clone();
        let d = done.clone();
        let consumer = std::thread::spawn(move || {
            g.set_worker(std::thread::current());
            loop {
                g.acquire();
                while g.is_open() {
                    let pr = p.load(Ordering::Acquire);
                    let cn = c.load(Ordering::Relaxed);
                    if pr > cn {
                        c.store(cn + 1, Ordering::Release);
                    } else {
                        g.lock();
                    }
                }
                if d.load(Ordering::Acquire) && c.load(Ordering::Relaxed) >= p.load(Ordering::Acquire) {
                    return;
                }
            }
        });

        for _ in 0..N {
            produced.fetch_add(1, Ordering::Release);
            gate.release();
        }

        done.store(true, Ordering::Release);
        gate.release();

        consumer.join().unwrap();
        assert_eq!(consumed.load(Ordering::Acquire), N);
    }
}
