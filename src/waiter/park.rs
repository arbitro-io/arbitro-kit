//! `ParkWaiter` — hardened OS-thread `Waiter` (lost-wake-proof T3a/T3b probe + atomic worker registration). A/B sibling of `ParkWaiter` (park.rs), which stays untouched as baseline.
//!
//! Default backend. `wait_until` does the spin-then-park dance with a
//! Dekker-safe recheck; `wake()` mirrors [`NotifyWaiter`](super::NotifyWaiter)'s
//! WakeGate two-step probe (positive-only Relaxed-class load, then an
//! authoritative RMW) so no interleaving can lose a wake.
//!
//! ## State machine
//!
//! `parked: AtomicUsize` (`UNPARKED = 0` / `PARKED = 1`) — `PARKED` from
//! the moment the consumer commits to the park path until it observes
//! the predicate true again. Every write to `parked` is an atomic RMW —
//! that is load-bearing (see proof: release sequences must survive
//! intervening writes, and the waker's probe must read the latest value
//! in the modification order). A `usize` rather than a bool so the
//! identity probe compiles to one `lock xadd` (see `PARKED` docs).
//!
//! | #   | Actor  | Op | Transition | Ordering |
//! |-----|--------|----|------------|----------|
//! | T1  | waiter | announce park | `parked.swap(PARKED)` | RMW `SeqCst` (≥ `Acquire` required — see proof; `SeqCst` for margin, cold path) |
//! | T2  | waiter | leave park path | `parked.swap(UNPARKED)` | RMW `SeqCst` — runs on every exit: predicate-true after announce, post-park predicate-true, deadline timeout |
//! | T3a | waker  | `wake()` fast probe | `parked.load(Acquire)`; if `PARKED` → deliver (T4), done | positive-only: observing `PARKED` can never lose a wake; `Acquire` (not `Relaxed` as in notify.rs) because delivery dereferences waiter-owned state — see "Registration visibility" |
//! | T3b | waker  | `wake()` authoritative probe | `fence(SeqCst)` then `parked.load(Relaxed)`; if `PARKED` → deliver (T4) | SC fence pairs with the waiter's T1 fence (SC-fence Dekker) — core-local, no peer-line traffic |
//! | T4  | waker  | deliver | lock `worker` mutex, clone `Thread`, unlock, `unpark()` | mutex + `unpark` token |
//!
//! The waiter-side order is: register worker (once, cold) → spin →
//! T1 → predicate re-check → `thread::park()` loop (re-check after every
//! return) → T2. T1 strictly precedes the re-check; T2 strictly follows
//! the last predicate observation of the episode.
//!
//! ## Happens-before proof — why no wake is ever lost
//!
//! Classic Dekker store→load race, closed with the SC-fence theorem
//! (C++20 [atomics.fences]p4, inherited by Rust): if fence FA is
//! sequenced after store A in one thread, fence FB is sequenced before
//! load D in another, and both fences are `SeqCst`, then FA and FB are
//! totally ordered by the single SC order S — and whichever fence is
//! later, the load on that side observes the store on the other.
//!
//! ```text
//! waker  : A:  data store (caller, e.g. ring cursor, ≥Relaxed)
//!          B0: parked.load(Acquire)        // T3a: if PARKED → unpark, done
//!          FA: fence(SeqCst)               // T3b, only if B0 read UNPARKED
//!          B:  parked.load(Relaxed)        //      if PARKED → unpark
//! waiter : C:  parked.swap(PARKED, SeqCst) // T1
//!          FB: fence(SeqCst)
//!          D:  predicate loads (e.g. ring cursors)
//! ```
//!
//! B0 is a *positive-only* filter: if it observes `PARKED`, the waker
//! unparks — always safe (an extra unpark at worst stores a token that
//! makes one future `park()` return early, and the park loop re-checks
//! the predicate on every return). A lost wake would require B to miss
//! C's `PARKED` *and* D to miss A's data. FA and FB are ordered in S;
//! exactly one of:
//!
//! - **`FB <_S FA`**: by the fence theorem, B (sequenced after FA)
//!   observes C's write (sequenced before FB) → B reads `PARKED` → the
//!   waker unparks. `thread::park`'s token then guarantees delivery:
//!   `unpark` either unblocks the in-progress `park`, or — if it lands
//!   before the consumer's `park` call — stores the token so `park`
//!   returns immediately (std guarantee). Std further documents that
//!   the `unpark` synchronizes-with the `park` return it unblocks, so A
//!   happens-before the post-park predicate re-check: the waiter sees
//!   the data and exits.
//! - **`FA <_S FB`**: by the fence theorem, D (sequenced after FB)
//!   observes A (sequenced before FA) → the predicate re-check after T1
//!   sees the data and the waiter returns without parking.
//!
//! Either way, no interleaving exists where the waiter parks with the
//! data unseen *and* the waker skips the unpark. A stale `PARKED` at B0
//! (waiter already un-parking) only costs one spurious unpark token —
//! benign, consumed by a later `park` whose return re-checks the
//! predicate. Cross-episode token theft is also benign: a token stored
//! by an old spurious unpark makes one `park` return early, the
//! predicate is re-checked, and if still false the waiter parks again —
//! the *current* episode's genuine unpark (guaranteed above) is still
//! outstanding and releases it. The same protocol (same ops, same
//! orderings) is model-checked exhaustively in `tests/loom_park_gate.rs`.
//!
//! ## Registration visibility — why the probes are `Acquire`
//!
//! Unlike notify.rs (whose T3a is `Relaxed` because `notify_one`
//! carries its own internal synchronization and the waker reads no
//! waiter-owned state), a positive probe here authorizes dereferencing
//! the registered `Thread` handle. The consumer registers via
//! `set_worker` (mutex write) strictly before T1 (enforced by the
//! `has_worker` assert on the park path). A waker that observes
//! `PARKED` reads a value from the release sequence headed by
//! the `SeqCst` T1 RMW; because both probes are ≥ `Acquire` on the read
//! side, the probe synchronizes-with T1, so the consumer's `set_worker`
//! critical section happens-before the waker's `worker.lock()` in T4 —
//! the lock is therefore ordered after registration in the mutex's
//! order and must observe `Some(thread)`. Without the `Acquire`, the
//! waker could in principle lock the mutex "before" the registration
//! and skip the unpark — a lost wake. `Acquire` loads are free on x86
//! (plain `mov`), so this costs nothing on the dominant target.
//!
//! ## Cost
//!
//! | Path                              |            Cost |
//! | --------------------------------- | --------------: |
//! | `wake()` consumer-not-parked      | 1 `Acquire` load + 1 uncontended `AcqRel` RMW |
//! | `wake()` consumer-parked          | ~7 µs (mutex clone + syscall) |
//! | `wait_until()` ready on entry     |          ~0.5 ns |
//! | `wait_until()` park path extra    | +2 RMWs (announce/clear, cold) |
//! | CPU while parked                  |              0% |
//!
//! Measured cost of the wake-side barrier (`mem_ring_h2h`, WSL2, 1M
//! msgs, CAP=32, saturated — the ring calls `wake()` on **every** op):
//! Ring/thread p50 11.4 ns/msg (old load-only probe) → 48.2 ns/msg
//! (T3b RMW). The regression is the StoreLoad barrier the proof
//! requires, paid twice per message because ring send/recv call
//! `wake()` unconditionally. The old number was bought with a formally
//! lost-wake-prone probe (TSO store-buffer race — deadlock in cap=1
//! sync ping-pong); correctness wins. A waker-side plain load can only
//! be made sound with OS-assisted asymmetric fences (`membarrier` /
//! `FlushProcessWriteBuffers`), which need platform deps — out of
//! scope for dep-free kit. The throughput-recovery path is gating
//! `wake()` at the caller (only on empty→nonempty / full→nonfull
//! transitions), which is ring-level information the waiter lacks.
//!
//! ## Concurrency contract
//!
//! - Exactly **one consumer** thread calls `wait_until`. It must register
//!   itself first via `set_worker(thread::current())` — `wait_until` panics
//!   if reached without a registered worker (the alternative is a silent
//!   deadlock).
//! - Any number of producers may call `wake()` from any thread.
//! - `set_worker` may be called again (re-registration) from any thread
//!   at any time; registration is mutex-serialized, so it never races
//!   with a concurrent `wake()`.

use std::sync::atomic::{fence, AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use super::{BlockingWaiter, Waiter};

/// Default spin iterations before parking.
pub const DEFAULT_SPIN_ITERS: u32 = 512;

/// Tight-spin iterations before switching to PAUSE.
const TIGHT_SPIN: u32 = 64;

/// `parked` flag values. A `usize` (not `AtomicBool`) so the waker's
/// identity probe is `fetch_add(0)` — a single `lock xadd` on x86 —
/// instead of `AtomicBool::fetch_or(false)`, which lowers to a
/// `lock cmpxchg` retry loop. Same instruction choice as notify.rs.
const UNPARKED: usize = 0;
const PARKED: usize = 1;

/// OS-thread waiter — wraps `thread::park`/`unpark` behind a parked-flag
/// gate (see module docs for the state machine and happens-before proof).
///
/// `Default` produces an unregistered waiter; the consumer must call
/// `set_worker(thread::current())` before any `wait_until`.
#[repr(align(64))]
pub struct ParkWaiter {
    /// `PARKED` while the consumer is on the park path. Written ONLY via
    /// atomic RMWs (never plain stores) — the wake protocol's release-
    /// sequence argument depends on it. See module docs.
    parked: AtomicUsize,
    /// Spin iterations before parking.
    spin_iters: u32,
    /// Hot-path guard for `has_worker()` — avoids locking `worker` on
    /// the consumer's assert path. Set (Release) after the registration
    /// is published under the mutex.
    registered: AtomicBool,
    /// Consumer thread handle. Mutex-serialized: `set_worker` may be
    /// called from any thread while wakers read it — no `UnsafeCell`,
    /// no data race by construction. Cold on both sides: registration
    /// happens once per consumer, and `wake()` only locks after the
    /// parked-flag probe says a consumer is actually parked (i.e. on
    /// the ~µs unpark path, where a tiny uncontended lock is noise).
    worker: Mutex<Option<std::thread::Thread>>,
}

impl Default for ParkWaiter {
    #[inline]
    fn default() -> Self {
        Self::with_spin(DEFAULT_SPIN_ITERS)
    }
}

impl ParkWaiter {
    /// Construct a `ParkWaiter` with a custom spin budget. Higher = lower
    /// wake latency when the producer fires within the spin window; lower
    /// = parks sooner (0% CPU idle). 0 = always park.
    pub fn with_spin(spin_iters: u32) -> Self {
        Self {
            parked: AtomicUsize::new(UNPARKED),
            spin_iters,
            registered: AtomicBool::new(false),
            worker: Mutex::new(None),
        }
    }

    /// T4 — deliver the wake: clone the registered handle under the
    /// mutex, unpark outside it (keeps the critical section tiny;
    /// `Thread` is an `Arc` internally, so the clone is one refcount).
    ///
    /// Only reached after a probe observed `parked == true`, which (per
    /// the module-docs proof) happens-after `set_worker`, so the lock
    /// observes `Some` for any correctly registered consumer. `None`
    /// (never registered) is tolerated as a no-op — the consumer-side
    /// assert is the loud failure for that misuse.
    #[cold]
    fn unpark_registered(&self) {
        let t = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(t) = t {
            t.unpark();
        }
    }

    #[cold]
    #[inline(never)]
    fn wait_slow<F: FnMut() -> bool>(&self, ready: &mut F) {
        // Invariant: the consumer must have registered itself via
        // `set_worker` before reaching the park path. Without it the
        // producer's `wake()` finds no worker and skips the `unpark()`,
        // deadlocking the consumer silently. We surface a clear panic
        // instead. (This assert is also what makes "registration
        // happens-before T1" part of the protocol — see module docs.)
        assert!(
            self.has_worker(),
            "Park::wait_until reached park path without set_worker — register the consumer thread first",
        );
        // Phase 1: tight spin (no PAUSE).
        for _ in 0..TIGHT_SPIN {
            if ready() {
                return;
            }
            std::hint::black_box(());
        }
        // Phase 2: PAUSE spin.
        for _ in 0..self.spin_iters {
            if ready() {
                return;
            }
            std::hint::spin_loop();
        }
        // Phase 3: T1 — announce park, then SC fence. The fence pairs
        // with the waker's T3b fence (SC-fence Dekker theorem): it
        // orders this `parked` write before the predicate re-check so
        // that "waker misses PARKED AND waiter misses the data" is
        // impossible. Cold path — the fence cost is irrelevant here.
        self.parked.swap(PARKED, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if ready() {
            // T2 — leave the park path. RMW for the same reason.
            self.parked.swap(UNPARKED, Ordering::SeqCst);
            return;
        }
        // Phase 4: park loop. Every `park()` return (genuine token or
        // spurious) is followed by a predicate re-check.
        loop {
            std::thread::park();
            if ready() {
                self.parked.swap(UNPARKED, Ordering::SeqCst); // T2
                return;
            }
        }
    }

    /// Block until `ready()` returns `true` or `deadline` elapses.
    /// Returns `true` if the deadline elapsed first, `false` if the
    /// predicate became true.
    ///
    /// Inherent (not part of `BlockingWaiter`) because the trait has no
    /// deadline-aware variant. Used by `OneSignal::acquire_timeout` and
    /// any other primitive that needs a timed wait specifically against
    /// the park backend.
    pub fn wait_until_deadline<F: FnMut() -> bool>(&self, deadline: Instant, mut ready: F) -> bool {
        if ready() {
            return false;
        }
        assert!(
            self.has_worker(),
            "ParkWaiter::wait_until_deadline reached park path without set_worker — register the consumer thread first",
        );
        // Tight spin.
        for _ in 0..TIGHT_SPIN {
            if ready() {
                return false;
            }
            if Instant::now() >= deadline {
                return true;
            }
            std::hint::black_box(());
        }
        // PAUSE spin.
        for _ in 0..self.spin_iters {
            if ready() {
                return false;
            }
            if Instant::now() >= deadline {
                return true;
            }
            std::hint::spin_loop();
        }
        // T1 — announce park + SC fence (see wait_slow / module docs).
        self.parked.swap(PARKED, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if ready() {
            self.parked.swap(UNPARKED, Ordering::SeqCst); // T2
            return false;
        }
        // Park loop with timeout.
        loop {
            let now = Instant::now();
            if now >= deadline {
                self.parked.swap(UNPARKED, Ordering::SeqCst); // T2
                return !ready();
            }
            std::thread::park_timeout(deadline - now);
            if ready() {
                self.parked.swap(UNPARKED, Ordering::SeqCst); // T2
                return false;
            }
        }
    }
}

impl Waiter for ParkWaiter {
    fn set_worker(&self, thread: std::thread::Thread) {
        // Mutex-serialized against concurrent `wake()` readers and other
        // registrars — re-registration from any thread is race-free.
        *self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(thread);
        // Publish for the lock-free `has_worker` guard.
        self.registered.store(true, Ordering::Release);
    }

    #[inline]
    fn has_worker(&self) -> bool {
        self.registered.load(Ordering::Acquire)
    }

    /// Wake the registered consumer if it is parked. Idempotent,
    /// callable from any thread.
    ///
    /// Two-step probe:
    /// - T3a: positive-only `Acquire` load — observing `PARKED` can
    ///   never lose a wake, so a possibly-stale positive may take the
    ///   fast exit (worst case one spurious unpark, which the park loop
    ///   tolerates).
    /// - T3b: authoritative probe — `fence(SeqCst)` then a load. The
    ///   fence is core-local (an `mfence` on x86): it orders the
    ///   caller's earlier data store before the `parked` load WITHOUT
    ///   touching the waiter's cache line, unlike an RMW probe (a
    ///   `lock xadd` pulls the peer's line exclusive on every wake —
    ///   measured 4x throughput regression on the saturated SPSC bench).
    ///   Soundness pairs with the waiter's T1 `swap` + `fence(SeqCst)`
    ///   via the classic SC-fence Dekker theorem ([atomics.fences]):
    ///   with FA = waker fence, FB = waiter fence, either FA <_S FB
    ///   (waiter's predicate re-check sees the data store → no park) or
    ///   FB <_S FA (this load sees PARKED → unpark fires). Both misses
    ///   at once are impossible.
    ///
    /// Cost, consumer not parked: one `Acquire` load + one `mfence` +
    /// one `Relaxed` load — zero cross-core cache-line traffic.
    #[inline]
    fn wake(&self) {
        // T3a — positive-only fast probe.
        if self.parked.load(Ordering::Acquire) == PARKED {
            self.unpark_registered();
            return;
        }
        // T3b — SC-fence Dekker probe (core-local; no peer-line RMW).
        fence(Ordering::SeqCst);
        if self.parked.load(Ordering::Relaxed) == PARKED {
            self.unpark_registered();
        }
    }
}

impl BlockingWaiter for ParkWaiter {
    /// Block the calling thread until `ready()` returns `true`. Must be
    /// called from the thread registered via `set_worker`.
    #[inline]
    fn wait_until<F: FnMut() -> bool>(&self, mut ready: F) {
        if ready() {
            return;
        }
        self.wait_slow(&mut ready);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
        for _ in 0..16 {
            w.wake();
        }
        h.join().unwrap();
    }

    /// `wake()` with no registered worker must be a silent no-op — both
    /// with the flag clear (probe short-circuits) and with the flag
    /// forced set (T4 reached, finds `None`).
    #[test]
    fn wake_with_no_worker_is_noop() {
        let w = ParkWaiter::default();
        w.wake(); // not parked: neither probe fires T4
        w.parked.swap(PARKED, Ordering::SeqCst);
        w.wake(); // parked but unregistered: T4 reached, `None`, no-op
        assert!(!w.has_worker());
    }

    /// Finding-2 regression: re-registration from one thread while
    /// another hammers `wake()` with the parked flag forced set, so
    /// every wake dereferences the registration concurrently with the
    /// writes. Under the old `UnsafeCell` this was a formal data race
    /// (Miri flags it); with the mutex it must be clean. Stray unpark
    /// tokens land on the registrar thread — harmless.
    #[test]
    fn re_registration_race_with_concurrent_wakes() {
        const M: usize = if cfg!(miri) { 200 } else { 20_000 };
        let w = Arc::new(ParkWaiter::default());
        w.set_worker(std::thread::current());
        // Force the delivery path: wake() only touches the registration
        // once a probe observes PARKED.
        w.parked.swap(PARKED, Ordering::SeqCst);

        let stop = Arc::new(AtomicBool::new(false));
        let (w1, s1) = (w.clone(), stop.clone());
        let registrar = std::thread::spawn(move || {
            let mut n = 0u32;
            while !s1.load(Ordering::Relaxed) {
                w1.set_worker(std::thread::current());
                n = n.wrapping_add(1);
            }
            n
        });
        let w2 = w.clone();
        let waker = std::thread::spawn(move || {
            for _ in 0..M {
                w2.wake();
            }
        });

        waker.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        assert!(registrar.join().unwrap() > 0);
        assert!(w.has_worker());
    }

    /// Finding-1 regression: the tightest possible wake-dependency loop
    /// through a real consumer — `Ring<u32, 1>` (default `ParkWaiter`)
    /// between two OS threads forces one not_full + one not_empty
    /// park/wake handoff per item. A single lost wake deadlocks the
    /// pair; the watchdog `recv_timeout` turns that into a test failure
    /// instead of a CI hang (the deadlocked threads are leaked — the
    /// test has already failed at that point).
    #[test]
    fn ring2_cap1_ping_pong_park_waiter() {
        use crate::stream::Ring;
        const N: u32 = if cfg!(miri) { 300 } else { 100_000 };
        let (mut tx, mut rx) = Ring::<u32, 1>::new();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

        let d1 = done_tx.clone();
        let producer = std::thread::spawn(move || {
            for i in 0..N {
                tx.send(i).expect("consumer alive");
            }
            let _ = d1.send(());
        });
        let consumer = std::thread::spawn(move || {
            for i in 0..N {
                assert_eq!(rx.recv(), Some(i));
            }
            let _ = done_tx.send(());
        });

        // Watchdog: a lost wake must fail the test, not hang CI.
        for _ in 0..2 {
            done_rx
                .recv_timeout(Duration::from_secs(60))
                .expect("lost wake: Ring cap=1 ping-pong deadlocked");
        }
        producer.join().unwrap();
        consumer.join().unwrap();
    }
}
