//! `OneShot<T>` — single-use, 1:1, single-payload reply slot.
//!
//! The kit-native analog of `tokio::sync::oneshot`. Built directly on a
//! per-`Inner` `AtomicBool` + `Thread` handle (no `Signal` indirection)
//! plus a `MaybeUninit<T>` slot, with an atomic state machine
//! (`empty` → `full` | `closed`) so the receiver can distinguish
//! "value sent" from "sender dropped".
//!
//! - [`OneShot::new()`] returns `(Sender<T>, Receiver<T>)`.
//! - [`Sender::send`] consumes `self` and delivers exactly one value.
//! - [`Receiver::recv`] consumes `self` and blocks until either the
//!   value arrives or the sender is dropped (returns [`Closed`]).
//!
//! ## Cost model (zero `LOCK`-prefixed RMW on `send`)
//!
//! `send` (receiver not parked):
//!   1. Slot write
//!   2. `state.store(FULL, SeqCst)` — closes Dekker vs `parked.store(SeqCst)`
//!   3. `parked.load(Relaxed)` — false on the hot path, no syscall
//!
//! `send` (receiver parked):
//!   1–2 as above
//!   3. `parked` is true → `Thread::unpark()` (one syscall)
//!
//! `recv` (value already there): one Acquire CAS on state (`FULL → TAKEN`),
//! one `assume_init_read`. No park, no spin.
//!
//! `recv` (value not there yet):
//!   1. Tight try_take loop (`SPIN_ITERS = 512`, with `spin_loop` hint)
//!   2. `parked.store(true, SeqCst)` + recheck try_take
//!   3. `thread::park()` until state moves
//!
//! Same Dekker pattern proven on `Mpmc`'s `consumer_parked`: receiver's
//! SeqCst store + state load Acquire closes the race against sender's
//! SeqCst store + parked load Relaxed.
//!
//! ## Drop safety
//!
//! - If the [`Sender`] is dropped without sending, the [`Receiver`]
//!   wakes with `Err(Closed)`.
//! - If the [`Receiver`] is dropped before `recv`, [`Sender::send`]
//!   silently drops the value — the inner slot's `Drop` runs
//!   `assume_init_drop` if `state == FULL`.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;

/// State of the OneShot's slot. Transitions are one-way:
/// `EMPTY` → `FULL` (sender sent), or `EMPTY` → `CLOSED` (sender dropped).
/// Once `FULL`, a successful `try_recv` swaps to `TAKEN` so subsequent
/// reads return `Closed` instead of double-reading the slot.
const STATE_EMPTY: u8 = 0;
const STATE_FULL:  u8 = 1;
const STATE_CLOSED:u8 = 2;
const STATE_TAKEN: u8 = 3;

/// Spin iterations before `recv` actually parks. Mirrors the budget used
/// by every other park-based primitive in the crate.
const SPIN_ITERS: u32 = 512;

/// Error returned when the [`Sender`] dropped without sending, or when
/// the value has already been taken.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Closed;

#[repr(C, align(64))]
struct Inner<T: Send> {
    /// State machine: EMPTY / FULL / CLOSED / TAKEN.
    state:  AtomicU8,
    /// `true` when the receiver has announced a park and not yet woken.
    /// Sender reads this Relaxed; the SeqCst on `state.store` closes
    /// the Dekker race.
    parked: AtomicBool,
    /// Slot storage. Written once by the sender on `send`; read once by
    /// the receiver on a successful FULL→TAKEN CAS. Drop of `Inner`
    /// runs `assume_init_drop` if `state == FULL` (no one read it).
    slot:   UnsafeCell<MaybeUninit<T>>,
    /// Receiver thread handle. Written once via `Receiver::bind` before
    /// the inner is shared; the sender reads it only after observing
    /// `parked == true`, which the receiver published with a SeqCst
    /// store, so the `Thread` write is visible.
    worker: UnsafeCell<Option<std::thread::Thread>>,
}

// Safety: the slot is accessed only after the state machine grants
// exclusive access (CAS to FULL by sender, or CAS from FULL to TAKEN
// by receiver). The state's Release/Acquire ordering provides
// cross-thread visibility for the slot bytes.
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T: Send> Inner<T> {
    /// Wake the receiver if it has announced parking. One Relaxed load
    /// on the hot path; one `unpark()` syscall on the cold path.
    #[inline]
    fn wake_if_parked(&self) {
        if self.parked.load(Ordering::Relaxed) {
            // SAFETY: `worker` is written once in `Receiver::bind`
            // before the receiver could ever set `parked = true` (the
            // SeqCst store of `parked` synchronises with our load).
            unsafe {
                if let Some(t) = &*self.worker.get() {
                    t.unpark();
                }
            }
        }
    }
}

impl<T: Send> Drop for Inner<T> {
    fn drop(&mut self) {
        // If a value was sent but never received, drop it.
        if self.state.load(Ordering::Acquire) == STATE_FULL {
            unsafe { (*self.slot.get()).assume_init_drop(); }
        }
    }
}

/// Constructor namespace.
pub struct OneShot<T: Send>(std::marker::PhantomData<T>);

impl<T: Send> OneShot<T> {
    /// Build a new `OneShot` pair. Returns `(Sender, Receiver)`.
    #[inline]
    pub fn new() -> (Sender<T>, Receiver<T>) {
        let inner = Arc::new(Inner {
            state:  AtomicU8::new(STATE_EMPTY),
            parked: AtomicBool::new(false),
            slot:   UnsafeCell::new(MaybeUninit::uninit()),
            worker: UnsafeCell::new(None),
        });
        (
            Sender { inner: inner.clone(), sent: false },
            Receiver { inner },
        )
    }
}

/// Producer half. Fires exactly one value or drops without firing.
pub struct Sender<T: Send> {
    inner: Arc<Inner<T>>,
    sent:  bool,
}

impl<T: Send> Sender<T> {
    /// Deliver the value. Consumes `self`. Wakes the receiver via a
    /// direct `Thread::unpark` if it had announced parking.
    #[inline]
    pub fn send(mut self, value: T) {
        // Safety: state is EMPTY (we are the only sender, no concurrent
        // writer to the slot exists yet); SeqCst store after the write
        // makes the slot's contents visible to the receiver and closes
        // the Dekker race vs `parked.store(SeqCst)`.
        unsafe { (*self.inner.slot.get()).write(value); }
        self.inner.state.store(STATE_FULL, Ordering::SeqCst);
        self.inner.wake_if_parked();
        self.sent = true;
    }
}

impl<T: Send> Drop for Sender<T> {
    fn drop(&mut self) {
        if !self.sent {
            // Mark closed BEFORE waking. SeqCst closes the Dekker race
            // identically to the `send` path.
            self.inner.state.store(STATE_CLOSED, Ordering::SeqCst);
            self.inner.wake_if_parked();
        }
    }
}

/// Consumer half. Receives exactly one value or `Closed`.
pub struct Receiver<T: Send> {
    inner: Arc<Inner<T>>,
}

impl<T: Send> Receiver<T> {
    /// Register the calling thread as the consumer. Must be called from
    /// the thread that will block on [`Self::recv`], before that call.
    #[inline]
    pub fn bind(&self) {
        // SAFETY: `worker` is written once before the sender can observe
        // `parked == true`, which is the only path where the sender
        // reads it.
        unsafe {
            *self.inner.worker.get() = Some(std::thread::current());
        }
    }

    /// Block until a value is delivered or the [`Sender`] is dropped.
    /// Consumes `self`.
    ///
    /// Wait protocol: fast-path `try_take` → `SPIN_ITERS` PAUSE-spin
    /// rechecks → Dekker-fenced `thread::park()`. Catches sub-µs sender
    /// publications without touching the kernel.
    #[inline]
    pub fn recv(self) -> Result<T, Closed> {
        // Fast path.
        if let Some(r) = self.try_take() { return r; }
        // Bounded spin — sender may be ~ns away from publishing.
        for _ in 0..SPIN_ITERS {
            if let Some(r) = self.try_take() { return r; }
            std::hint::spin_loop();
        }
        // Slow path: Dekker park loop.
        loop {
            self.inner.parked.store(true, Ordering::SeqCst);
            if let Some(r) = self.try_take() {
                self.inner.parked.store(false, Ordering::Relaxed);
                return r;
            }
            std::thread::park();
            self.inner.parked.store(false, Ordering::Relaxed);
            if let Some(r) = self.try_take() { return r; }
            // Spurious wake — loop and re-park.
        }
    }

    /// Non-blocking poll. Returns `Ok(Some(v))` if a value is ready,
    /// `Err(Closed)` if the sender dropped without sending, `Ok(None)`
    /// if neither has happened yet.
    #[inline]
    pub fn try_recv(&self) -> Result<Option<T>, Closed> {
        match self.try_take() {
            Some(Ok(v)) => Ok(Some(v)),
            Some(Err(Closed)) => Err(Closed),
            None => Ok(None),
        }
    }

    /// Internal: attempt a state-machine transition that takes the slot
    /// or observes closure. Returns:
    ///   - `Some(Ok(v))`   → `FULL` → `TAKEN`, value extracted.
    ///   - `Some(Err(C))`  → `CLOSED` (sender dropped).
    ///   - `None`          → `EMPTY` (still pending).
    #[inline]
    fn try_take(&self) -> Option<Result<T, Closed>> {
        // Try to claim FULL → TAKEN atomically. Acquire so we see the
        // sender's slot write; success path means we have exclusive
        // ownership of the slot.
        match self.inner.state.compare_exchange(
            STATE_FULL,
            STATE_TAKEN,
            Ordering::Acquire,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Safety: we transitioned FULL → TAKEN; no one else
                // can read or write the slot now. The sender wrote the
                // value before storing FULL (SeqCst); our Acquire CAS
                // synchronizes-with that store.
                let v = unsafe { (*self.inner.slot.get()).assume_init_read() };
                Some(Ok(v))
            }
            Err(STATE_CLOSED) | Err(STATE_TAKEN) => Some(Err(Closed)),
            Err(_) => None, // STATE_EMPTY — still pending
        }
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn send_recv_roundtrip() {
        let (tx, rx) = OneShot::<u64>::new();
        let h = thread::spawn(move || {
            rx.bind();
            rx.recv()
        });
        thread::sleep(Duration::from_millis(10));
        tx.send(42);
        assert_eq!(h.join().unwrap(), Ok(42));
    }

    #[test]
    fn sender_dropped_returns_closed() {
        let (tx, rx) = OneShot::<u64>::new();
        let h = thread::spawn(move || {
            rx.bind();
            rx.recv()
        });
        thread::sleep(Duration::from_millis(10));
        drop(tx);
        assert_eq!(h.join().unwrap(), Err(Closed));
    }

    #[test]
    fn try_recv_empty_returns_none() {
        let (_tx, rx) = OneShot::<u64>::new();
        assert_eq!(rx.try_recv(), Ok(None));
    }

    #[test]
    fn try_recv_after_drop_returns_closed() {
        let (tx, rx) = OneShot::<u64>::new();
        drop(tx);
        assert_eq!(rx.try_recv(), Err(Closed));
    }

    #[test]
    fn try_recv_after_send_returns_value() {
        let (tx, rx) = OneShot::<u64>::new();
        tx.send(7);
        assert_eq!(rx.try_recv(), Ok(Some(7)));
        // Subsequent try_recv returns Closed (TAKEN state).
        assert_eq!(rx.try_recv(), Err(Closed));
    }

    #[test]
    fn send_after_receiver_drop_no_panic() {
        let (tx, rx) = OneShot::<u64>::new();
        drop(rx);
        tx.send(99);
        // Inner's Drop will run assume_init_drop. No panic, no leak.
    }

    #[test]
    fn box_payload_zero_copy() {
        let (tx, rx) = OneShot::<Box<Vec<u8>>>::new();
        let payload = Box::new(vec![1u8, 2, 3, 4]);
        let ptr_before = payload.as_ptr() as usize;

        let h = thread::spawn(move || {
            rx.bind();
            rx.recv().unwrap()
        });
        thread::sleep(Duration::from_millis(10));
        tx.send(payload);

        let received = h.join().unwrap();
        assert_eq!(received.as_slice(), &[1, 2, 3, 4]);
        assert_eq!(received.as_ptr() as usize, ptr_before,
                   "Box must be transferred zero-copy");
    }

    #[test]
    fn dropped_value_runs_destructor() {
        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let (tx, rx) = OneShot::<Tracked>::new();
            tx.send(Tracked(drops.clone()));
            // Receiver dropped without recv → Inner::Drop must drop the value.
            drop(rx);
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn high_volume_pairs() {
        // Stress: many short-lived OneShots in sequence.
        for _ in 0..1000 {
            let (tx, rx) = OneShot::<u64>::new();
            let h = thread::spawn(move || {
                rx.bind();
                rx.recv()
            });
            tx.send(1234);
            assert_eq!(h.join().unwrap(), Ok(1234));
        }
    }

    #[test]
    fn spin_path_catches_send_without_park() {
        // Smoke test: receiver enters recv, sender publishes within
        // the spin budget. Receiver should return without parking.
        let (tx, rx) = OneShot::<u64>::new();
        let h = thread::spawn(move || {
            rx.bind();
            rx.recv()
        });
        // No sleep — the receiver enters the spin loop and the sender's
        // publish should land within the 512-iter budget.
        tx.send(7);
        assert_eq!(h.join().unwrap(), Ok(7));
    }
}
