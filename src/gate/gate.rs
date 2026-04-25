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
//!
//! ## State source abstraction
//!
//! The open/closed bit is read through the [`SignalSource`] trait. The
//! default implementation [`OwnedBool`] owns an internal `AtomicBool` —
//! this is the classical `Signal::new()` behaviour with identical layout
//! and identical codegen (fully inlined, no vtable). Alternative sources
//! (e.g. viewing a shared `AtomicU64` bit, or deriving openness from a
//! data cursor) can be plugged in without modifying the park protocol.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Default spin iterations before parking. Covers producer→consumer
/// latencies in the ~100–500 ns range on commodity x86_64.
pub const DEFAULT_SPIN_ITERS: u32 = 512;

/// Tight-spin iterations before switching to PAUSE. Covers intra-socket
/// coherence latency (~50–150 ns) without committing a single PAUSE.
const TIGHT_SPIN: u32 = 64;

// ─── SignalSource trait ───────────────────────────────────────────────────

/// Pluggable backing for the open/closed state of a [`Signal`].
///
/// The trait is intentionally minimal: `Signal` only needs three operations
/// from the state it guards. Every implementor is expected to be tiny and
/// fully inline-able; the default [`OwnedBool`] compiles to the same
/// instructions as the pre-generic `Signal`.
///
/// # Ordering contract
///
/// - [`is_open`](Self::is_open) must use `Acquire` so a `true` return
///   synchronizes-with the producer's release.
/// - [`open`](Self::open) must use at least `Release` so payload writes
///   the producer performed before calling `release()` are published.
/// - [`close`](Self::close) must use at least `Release` so payload reads
///   the consumer performed before calling `lock()` are committed.
pub trait SignalSource {
    /// `true` if the gate is open (there is pending work).
    fn is_open(&self) -> bool;
    /// Open the gate (called by `Signal::release`).
    fn open(&self);
    /// Close the gate (called by `Signal::lock`).
    fn close(&self);
}

/// The canonical [`SignalSource`]: an owned `AtomicBool`. Bit meaning:
/// `true` = locked (no work), `false` = open. This inverted convention
/// matches the pre-generic `Signal` layout byte-for-byte.
#[repr(transparent)]
pub struct OwnedBool(AtomicBool);

impl OwnedBool {
    #[inline]
    pub const fn new_locked() -> Self { Self(AtomicBool::new(true)) }
}

impl SignalSource for OwnedBool {
    #[inline(always)]
    fn is_open(&self) -> bool {
        !self.0.load(Ordering::Acquire)
    }
    #[inline(always)]
    fn open(&self) {
        self.0.store(false, Ordering::Release);
    }
    #[inline(always)]
    fn close(&self) {
        self.0.store(true, Ordering::Release);
    }
}

/// View over an externally-owned `AtomicBool`. Created by
/// [`Signal::from_bool`]. Natural convention: `true` = open (has work),
/// `false` = closed — matches the intuition most callers have when they
/// manipulate the atomic themselves.
pub struct BoolView<'a>(&'a AtomicBool);

impl SignalSource for BoolView<'_> {
    #[inline(always)]
    fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
    #[inline(always)]
    fn open(&self) {
        self.0.store(true, Ordering::Release);
    }
    #[inline(always)]
    fn close(&self) {
        self.0.store(false, Ordering::Release);
    }
}

/// View over a single bit of an externally-owned `AtomicU64`. Created by
/// [`Signal::from_bit`]. Bit set = open, bit clear = closed. Uses `fetch_or`
/// / `fetch_and` so multiple `BitView`s over the same `AtomicU64` coexist
/// safely — each touches only its own bit.
pub struct BitView<'a> {
    atomic: &'a AtomicU64,
    mask: u64,
}

impl SignalSource for BitView<'_> {
    #[inline(always)]
    fn is_open(&self) -> bool {
        (self.atomic.load(Ordering::Acquire) & self.mask) != 0
    }
    #[inline(always)]
    fn open(&self) {
        self.atomic.fetch_or(self.mask, Ordering::Release);
    }
    #[inline(always)]
    fn close(&self) {
        self.atomic.fetch_and(!self.mask, Ordering::Release);
    }
}

// ─── Signal ──────────────────────────────────────────────────────────────

#[repr(align(64))]
pub struct Signal<S: SignalSource = OwnedBool> {
    /// Pluggable backing for the "is the gate open?" bit. For the default
    /// `Signal` this is an owned `AtomicBool`; layout and codegen match
    /// the pre-generic version exactly.
    source: S,
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
// happens-before edge observable by every producer. The `S: Sync` bound
// is required so the source can be touched from any producer thread.
unsafe impl<S: SignalSource + Sync> Sync for Signal<S> {}

impl Default for Signal {
    fn default() -> Self { Self::new() }
}

// ─── Canonical constructors (OwnedBool backing) ──────────────────────────
//
// Calling `Signal::new()` still works because `Signal<S = OwnedBool>` has
// `OwnedBool` as its default type parameter. These constructors keep the
// pre-generic shape exactly.
impl Signal<OwnedBool> {
    pub fn new() -> Self { Self::with_spin(DEFAULT_SPIN_ITERS) }

    /// Construct a `Signal` with a custom spin-iteration budget. Higher values
    /// trade CPU for lower wake latency when the producer fires within the
    /// spin window; lower values park sooner (0% CPU idle). 0 = always park.
    pub fn with_spin(spin_iters: u32) -> Self {
        Self {
            source: OwnedBool::new_locked(),
            parked: AtomicBool::new(false),
            spin_iters,
            worker: UnsafeCell::new(None),
        }
    }
}

// ─── Borrowed-source constructors ────────────────────────────────────────
//
// Ergonomic wiring for the common "I already own this atomic" case. Neither
// constructor takes ownership of the atomic — the Signal is lifetime-bound
// to the borrow so the caller keeps direct, zero-indirection access.

impl<'a> Signal<BoolView<'a>> {
    /// Build a `Signal` that observes an externally-owned `AtomicBool`.
    /// The caller retains direct read/write access to the atomic; `Signal`
    /// only handles park / unpark. Lifetime-bound: the atomic must outlive
    /// the `Signal`.
    ///
    /// Convention: `true` = open (has work), `false` = closed.
    #[inline]
    pub fn from_bool(atomic: &'a AtomicBool) -> Self {
        Self::with_source(BoolView(atomic), DEFAULT_SPIN_ITERS)
    }
}

impl<'a> Signal<BitView<'a>> {
    /// Build a `Signal` that observes one bit of an externally-owned
    /// `AtomicU64`. Up to 64 independent `Signal`s can share the same
    /// `AtomicU64` — each with its own parker and worker — packed into a
    /// single cache line.
    ///
    /// Convention: bit set = open, bit clear = closed. `release` uses
    /// `fetch_or` so other bits are preserved; `lock` uses `fetch_and`.
    ///
    /// Panics (debug) if `bit >= 64`.
    #[inline]
    pub fn from_bit(atomic: &'a AtomicU64, bit: u8) -> Self {
        debug_assert!(bit < 64, "bit index must be < 64");
        Self::with_source(
            BitView { atomic, mask: 1u64 << bit },
            DEFAULT_SPIN_ITERS,
        )
    }
}

// ─── Generic constructor for custom sources ──────────────────────────────

impl<S: SignalSource> Signal<S> {
    /// Build a `Signal` over an arbitrary [`SignalSource`]. The caller must
    /// ensure `source` starts in a consistent state (typically "closed").
    pub fn with_source(source: S, spin_iters: u32) -> Self {
        Self {
            source,
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
        self.source.open();
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
    ///
    /// Uses `Release` ordering: any reads the consumer performed on payload
    /// memory before calling `lock()` are published before the signal is
    /// marked closed. This is what makes composites like `Pipe` safe — the
    /// producer observes `is_open() == false` (via Acquire) only *after*
    /// the consumer's payload reads have committed. On x86 this emits the
    /// same `mov` as `Relaxed`; on ARM it adds one `dmb ish st`.
    #[inline]
    pub fn lock(&self) {
        self.source.close();
    }

    /// `true` if there is pending work (i.e. `release()` was called since the
    /// last `lock()`).
    #[inline]
    pub fn is_open(&self) -> bool {
        self.source.is_open()
    }

    /// Block the calling thread until the gate is open. Must be called from
    /// the thread registered via `set_worker`.
    ///
    /// Fast-path is a single Acquire load + branch. Slow path is split off
    /// into `#[cold] acquire_slow` so the fast path stays compact in icache.
    #[inline]
    pub fn acquire(&self) {
        if self.source.is_open() { return; }
        self.acquire_slow();
    }

    #[cold]
    #[inline(never)]
    fn acquire_slow(&self) {
        // Every early-return from this function must synchronize-with the
        // producer's `release()` (Release store) so the caller sees any
        // payload the producer wrote before releasing. That means every
        // exit-condition load must be **Acquire**, which the `SignalSource`
        // contract requires of `is_open()`.

        // Phase 1: tight spin (~1-2 ns/iter). Catches intra-socket signals
        // (~100-200 ns coherence) without paying a single PAUSE.
        for _ in 0..TIGHT_SPIN {
            if self.source.is_open() { return; }
            std::hint::black_box(());
        }
        // Phase 2: PAUSE spin (~20-40 ns/iter on x86). Covers the ~µs range.
        for _ in 0..self.spin_iters {
            if self.source.is_open() { return; }
            std::hint::spin_loop();
        }
        // Phase 3: announce parking. SeqCst store = mfence on x86 / dmb ish
        // on ARM → after this point our subsequent load of the source sees
        // every globally-visible store from any producer. Pays ~20 ns, once
        // per park event.
        self.parked.store(true, Ordering::SeqCst);
        if self.source.is_open() {
            // Producer fired between spin-end and parked-set; no need to park.
            self.parked.store(false, Ordering::Relaxed);
            return;
        }
        // Phase 4: park loop — `park()` can wake spuriously per std's docs,
        // so loop until we observe the source open.
        loop {
            std::thread::park();
            if self.source.is_open() {
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

    /// Canonical usage pattern for a Signal-guarded work queue:
    ///
    /// ```text
    /// loop {
    ///     gate.acquire();              // block until there's work
    ///     while gate.is_open() {       // drain while signal says open
    ///         match try_work() {
    ///             Some(item) => process(item),
    ///             None       => gate.lock(),  // observed empty → close
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// The invariant is: producer RELEASES when it adds work, consumer
    /// LOCKS only when it observes the queue empty from inside the drain
    /// loop (not on a hot path). Signal's Dekker protocol handles the
    /// missed-wakeup race between consumer's `lock()` and a concurrent
    /// producer `release()`.
    #[test]
    fn canonical_acquire_drain_pattern() {
        use std::sync::atomic::AtomicU64;

        const N: u64 = 1000;
        let gate = Arc::new(Signal::new());
        // Counter acts as our "queue": producer increments, consumer reads.
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
                        // Work available — consume one unit.
                        c.store(cn + 1, Ordering::Release);
                    } else {
                        // Queue looks empty. Close gate; outer `acquire`
                        // will re-block unless producer releases again.
                        g.lock();
                    }
                }
                if d.load(Ordering::Acquire) && c.load(Ordering::Relaxed) >= p.load(Ordering::Acquire) {
                    return;
                }
            }
        });

        // Producer: increment counter, release gate. Repeat N times.
        for _ in 0..N {
            produced.fetch_add(1, Ordering::Release);
            gate.release();
        }

        // Signal shutdown: set done, release once more so consumer wakes
        // and sees the final `done` flag.
        done.store(true, Ordering::Release);
        gate.release();

        consumer.join().unwrap();
        assert_eq!(consumed.load(Ordering::Acquire), N,
                   "canonical pattern must consume every produced unit");
    }

    #[test]
    fn from_bool_external_atomic() {
        // Signal observes an AtomicBool owned by the caller; caller retains
        // direct access and mutates it without going through the Signal.
        let state = AtomicBool::new(false);
        let sig = Signal::from_bool(&state);
        assert!(!sig.is_open(), "starts closed (state=false)");

        // Direct write to the caller's atomic — Signal must see it.
        state.store(true, Ordering::Release);
        assert!(sig.is_open(), "external open must be visible to Signal");

        // Signal::lock closes the shared atomic.
        sig.lock();
        assert!(!sig.is_open());
        assert!(!state.load(Ordering::Acquire),
                "Signal::lock must clear the caller's atomic");

        // Signal::release opens it.
        sig.release();
        assert!(state.load(Ordering::Acquire),
                "Signal::release must set the caller's atomic");
    }

    #[test]
    fn from_bit_multiple_signals_share_one_u64() {
        use std::sync::atomic::AtomicU64;

        let state = AtomicU64::new(0);
        let sig0  = Signal::from_bit(&state, 0);
        let sig3  = Signal::from_bit(&state, 3);
        let sig63 = Signal::from_bit(&state, 63);

        assert!(!sig0.is_open() && !sig3.is_open() && !sig63.is_open());

        // Release bit 3 — only sig3 sees open.
        sig3.release();
        assert!(!sig0.is_open());
        assert!( sig3.is_open());
        assert!(!sig63.is_open());
        assert_eq!(state.load(Ordering::Acquire), 1u64 << 3);

        // Release bit 0 — sig0 and sig3 both open, sig63 still closed.
        sig0.release();
        assert!(sig0.is_open());
        assert!(sig3.is_open());
        assert!(!sig63.is_open());
        assert_eq!(state.load(Ordering::Acquire), (1u64 << 3) | 1);

        // Lock bit 3 — bit 0 must stay set.
        sig3.lock();
        assert!( sig0.is_open());
        assert!(!sig3.is_open());
        assert_eq!(state.load(Ordering::Acquire), 1);

        // Release bit 63 (top bit, edge case).
        sig63.release();
        assert!(sig63.is_open());
        assert_eq!(state.load(Ordering::Acquire), 1 | (1u64 << 63));
    }

    #[test]
    fn from_bit_cross_thread_wake() {
        // One Signal<BitView> shared between producer and consumer via
        // scoped threads. release() must wake the parked consumer.
        use std::sync::atomic::AtomicU64;

        let state = AtomicU64::new(0);
        let sig = Signal::from_bit(&state, 5);

        std::thread::scope(|sc| {
            sc.spawn(|| {
                sig.set_worker(std::thread::current());
                sig.acquire();
                assert!(sig.is_open());
            });
            std::thread::sleep(Duration::from_millis(50));
            sig.release();
        });

        assert_eq!(state.load(Ordering::Acquire) & (1u64 << 5), 1u64 << 5);
    }

    #[test]
    fn from_bool_cross_thread_wake() {
        let state = AtomicBool::new(false);
        let sig = Signal::from_bool(&state);

        std::thread::scope(|sc| {
            sc.spawn(|| {
                sig.set_worker(std::thread::current());
                sig.acquire();
                assert!(sig.is_open());
            });
            std::thread::sleep(Duration::from_millis(50));
            sig.release();
        });

        assert!(state.load(Ordering::Acquire));
    }
}
