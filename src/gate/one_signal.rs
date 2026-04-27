//! `OneSignal` — single-use, payloadless, timeout-aware gate.
//!
//! The minimal "block until released" primitive. Specialised replacement
//! for `tokio::sync::oneshot` in scenarios where the **value travels
//! separately** (e.g. through a frame field, an `AtomicU64`, or a
//! pre-existing data path) and the OneSignal only carries the wake.
//!
//! Compare with siblings:
//! - [`Signal`](super::Signal) — reusable open/close (single bit).
//! - [`Park`](super::Park) — stateless park/unpark; predicate at call site.
//! - **`OneSignal`** — single-use, with `acquire_timeout`. Drop-aware:
//!   sender drop without release wakes the receiver with `Err(Closed)`.
//!
//! ## Cost
//!
//! - `release()`: 1 Release store + 1 Relaxed load (parked flag) + at
//!   most 1 `Thread::unpark` syscall (only if receiver was actually parked).
//! - `acquire()`: spin (64 tight + 512 PAUSE) → park if still pending.
//!   Most uncontended waits return without touching the kernel.
//! - `acquire_timeout(d)`: same spin window, then `park_timeout(d)` loop.
//!
//! ## Concurrency contract
//!
//! - Exactly one `Sender` produces one `release()` (or drops).
//! - Exactly one `Receiver` performs one `acquire*` (or drops).
//! - Both halves are `Send + Sync`.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const STATE_PENDING: u8 = 0;
const STATE_RELEASED: u8 = 1;
const STATE_CLOSED: u8 = 2;

/// Tight-spin iterations before switching to PAUSE. Matches `Park`.
const TIGHT_SPIN: u32 = 64;
/// PAUSE-spin iterations before parking. Matches `Park`.
const SPIN_ITERS: u32 = 512;

/// Result of a wait that did not return Ok.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AcquireError {
    /// Sender was dropped without calling `release`.
    Closed,
    /// `acquire_timeout` deadline elapsed before release.
    TimedOut,
}

#[repr(align(64))]
struct Inner {
    state: AtomicU8,
    parked: AtomicBool,
    /// Receiver thread handle. Written once via `Receiver::bind` before
    /// the inner is shared; read only after `parked == true` (SeqCst
    /// publishes the worker via the parked flag).
    worker: UnsafeCell<Option<std::thread::Thread>>,
}

unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

/// Constructor namespace.
pub struct OneSignal;

impl OneSignal {
    /// Build a fresh `(Sender, Receiver)` pair.
    #[inline]
    pub fn new() -> (Sender, Receiver) {
        let inner = Arc::new(Inner {
            state: AtomicU8::new(STATE_PENDING),
            parked: AtomicBool::new(false),
            worker: UnsafeCell::new(None),
        });
        (
            Sender {
                inner: inner.clone(),
                released: false,
            },
            Receiver { inner },
        )
    }
}

// ─── Sender ────────────────────────────────────────────────────────────────

pub struct Sender {
    inner: Arc<Inner>,
    released: bool,
}

impl Sender {
    /// Release the gate. Consumes self. The receiver wakes from
    /// `acquire*` with `Ok(())`.
    #[inline]
    pub fn release(mut self) {
        // SeqCst pairs with Receiver's `parked.store(SeqCst)` to close the
        // Dekker race: either the receiver observes RELEASED and skips park,
        // or this load sees `parked == true` and unparks. Without SeqCst
        // here, the StoreLoad reordering on x86 lets both sides miss.
        self.inner.state.store(STATE_RELEASED, Ordering::SeqCst);
        wake_if_parked(&self.inner);
        self.released = true;
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        if !self.released {
            // SeqCst — same Dekker reasoning as `release`.
            self.inner.state.store(STATE_CLOSED, Ordering::SeqCst);
            wake_if_parked(&self.inner);
        }
    }
}

// ─── Receiver ──────────────────────────────────────────────────────────────

pub struct Receiver {
    inner: Arc<Inner>,
}

impl Receiver {
    /// Register the calling thread as the receiver. Must be called from
    /// the thread that will run `acquire*`, before the first wait.
    #[inline]
    pub fn bind(&self) {
        // Safety: caller guarantees pre-share single-threaded access
        // (Receiver is owned by the caller exclusively).
        unsafe {
            *self.inner.worker.get() = Some(std::thread::current());
        }
    }

    /// Block until the sender calls `release` or is dropped.
    /// Consumes self. Spin-then-park, never times out.
    #[inline]
    pub fn acquire(self) -> Result<(), AcquireError> {
        if let Some(r) = check(&self.inner) {
            return r;
        }
        self.acquire_slow_no_timeout()
    }

    /// Block until release/drop or until `timeout` elapses.
    /// Consumes self. Returns `Err(AcquireError::TimedOut)` on deadline.
    #[inline]
    pub fn acquire_timeout(self, timeout: Duration) -> Result<(), AcquireError> {
        if let Some(r) = check(&self.inner) {
            return r;
        }
        self.acquire_slow_with_deadline(Instant::now() + timeout)
    }

    /// Non-blocking poll. Returns `Ok(Some(()))` if released, `Err(Closed)`
    /// if sender dropped, `Ok(None)` if still pending.
    #[inline]
    pub fn try_acquire(&self) -> Result<Option<()>, AcquireError> {
        match self.inner.state.load(Ordering::Acquire) {
            STATE_RELEASED => Ok(Some(())),
            STATE_CLOSED => Err(AcquireError::Closed),
            _ => Ok(None),
        }
    }

    #[cold]
    #[inline(never)]
    fn acquire_slow_no_timeout(self) -> Result<(), AcquireError> {
        // Phase 1: tight spin.
        for _ in 0..TIGHT_SPIN {
            if let Some(r) = check(&self.inner) {
                return r;
            }
            std::hint::black_box(());
        }
        // Phase 2: PAUSE spin.
        for _ in 0..SPIN_ITERS {
            if let Some(r) = check(&self.inner) {
                return r;
            }
            std::hint::spin_loop();
        }
        // Phase 3: announce park (Dekker), recheck.
        self.inner.parked.store(true, Ordering::SeqCst);
        if let Some(r) = check(&self.inner) {
            self.inner.parked.store(false, Ordering::Relaxed);
            return r;
        }
        // Phase 4: park loop.
        loop {
            std::thread::park();
            if let Some(r) = check(&self.inner) {
                self.inner.parked.store(false, Ordering::Relaxed);
                return r;
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn acquire_slow_with_deadline(self, deadline: Instant) -> Result<(), AcquireError> {
        for _ in 0..TIGHT_SPIN {
            if let Some(r) = check(&self.inner) {
                return r;
            }
            std::hint::black_box(());
        }
        for _ in 0..SPIN_ITERS {
            if let Some(r) = check(&self.inner) {
                return r;
            }
            std::hint::spin_loop();
        }
        self.inner.parked.store(true, Ordering::SeqCst);
        if let Some(r) = check(&self.inner) {
            self.inner.parked.store(false, Ordering::Relaxed);
            return r;
        }
        loop {
            let now = Instant::now();
            if now >= deadline {
                self.inner.parked.store(false, Ordering::Relaxed);
                // Final race-free check.
                return check(&self.inner).unwrap_or(Err(AcquireError::TimedOut));
            }
            std::thread::park_timeout(deadline - now);
            if let Some(r) = check(&self.inner) {
                self.inner.parked.store(false, Ordering::Relaxed);
                return r;
            }
        }
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────

#[inline]
fn check(inner: &Inner) -> Option<Result<(), AcquireError>> {
    match inner.state.load(Ordering::Acquire) {
        STATE_RELEASED => Some(Ok(())),
        STATE_CLOSED => Some(Err(AcquireError::Closed)),
        _ => None,
    }
}

#[inline]
fn wake_if_parked(inner: &Inner) {
    // SeqCst — closes the Dekker race with Receiver's `parked.store(SeqCst)`.
    // The total order across SeqCst ops guarantees: if the receiver's
    // recheck of `state` happened before this load, then `state` was already
    // RELEASED for that recheck (no park). Otherwise this load sees
    // `parked == true` and we unpark.
    if inner.parked.load(Ordering::SeqCst) {
        // Safety: parked == true was stored with SeqCst by the receiver,
        // which establishes a happens-before edge with the worker write.
        unsafe {
            if let Some(t) = &*inner.worker.get() {
                t.unpark();
            }
        }
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn release_wakes_receiver() {
        let (tx, rx) = OneSignal::new();
        let h = thread::spawn(move || {
            rx.bind();
            rx.acquire()
        });
        thread::sleep(Duration::from_millis(10));
        tx.release();
        assert_eq!(h.join().unwrap(), Ok(()));
    }

    #[test]
    fn sender_drop_returns_closed() {
        let (tx, rx) = OneSignal::new();
        let h = thread::spawn(move || {
            rx.bind();
            rx.acquire()
        });
        thread::sleep(Duration::from_millis(10));
        drop(tx);
        assert_eq!(h.join().unwrap(), Err(AcquireError::Closed));
    }

    #[test]
    fn acquire_timeout_fires() {
        let (_tx, rx) = OneSignal::new();
        rx.bind();
        let started = Instant::now();
        let result = rx.acquire_timeout(Duration::from_millis(50));
        assert_eq!(result, Err(AcquireError::TimedOut));
        assert!(started.elapsed() >= Duration::from_millis(45));
    }

    #[test]
    fn acquire_timeout_returns_ok_if_released_in_time() {
        let (tx, rx) = OneSignal::new();
        let h = thread::spawn(move || {
            rx.bind();
            rx.acquire_timeout(Duration::from_secs(5))
        });
        thread::sleep(Duration::from_millis(10));
        tx.release();
        assert_eq!(h.join().unwrap(), Ok(()));
    }

    #[test]
    fn acquire_timeout_returns_closed_if_sender_drops_in_time() {
        let (tx, rx) = OneSignal::new();
        let h = thread::spawn(move || {
            rx.bind();
            rx.acquire_timeout(Duration::from_secs(5))
        });
        thread::sleep(Duration::from_millis(10));
        drop(tx);
        assert_eq!(h.join().unwrap(), Err(AcquireError::Closed));
    }

    #[test]
    fn try_acquire_states() {
        let (tx, rx) = OneSignal::new();
        // Pending.
        assert_eq!(rx.try_acquire(), Ok(None));
        tx.release();
        // Released.
        assert_eq!(rx.try_acquire(), Ok(Some(())));
    }

    #[test]
    fn try_acquire_after_drop() {
        let (tx, rx) = OneSignal::new();
        drop(tx);
        assert_eq!(rx.try_acquire(), Err(AcquireError::Closed));
    }

    #[test]
    fn release_before_acquire_no_park() {
        // Sender releases BEFORE receiver calls acquire — should hit
        // the fast path (no park, no syscall).
        let (tx, rx) = OneSignal::new();
        tx.release();
        assert_eq!(rx.acquire(), Ok(()));
    }

    #[test]
    fn high_volume_pairs() {
        // Stress: many short-lived OneSignals back-to-back.
        for _ in 0..1000 {
            let (tx, rx) = OneSignal::new();
            let h = thread::spawn(move || {
                rx.bind();
                rx.acquire()
            });
            tx.release();
            assert_eq!(h.join().unwrap(), Ok(()));
        }
    }

    #[test]
    fn timeout_short_does_not_underflow() {
        let (_tx, rx) = OneSignal::new();
        rx.bind();
        // Zero timeout still goes through spin then deadline check.
        let r = rx.acquire_timeout(Duration::from_nanos(0));
        assert_eq!(r, Err(AcquireError::TimedOut));
    }
}
