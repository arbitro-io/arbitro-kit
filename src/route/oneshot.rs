//! `OneShot<T, W>` — single-use, 1:1, single-payload reply slot, generic
//! over the wait/wake backend.
//!
//! The kit-native analog of `tokio::sync::oneshot`. Built on a state
//! machine (`EMPTY` → `FULL` | `CLOSED`, `FULL` → `TAKEN`), a
//! `MaybeUninit<T>` slot, and a single [`Waiter`](crate::waiter::Waiter)
//! that coordinates wake/wait. The state machine lets the receiver
//! distinguish "value sent" from "sender dropped".
//!
//! ## Runtime — pick at the type level
//!
//! - `OneShot<T>` (default `W = ParkWaiter`) — sync, OS-thread, `recv()`
//!   blocks the calling thread.
//! - `OneShot<T, NotifyWaiter>` aka [`OneShotAsync<T>`](super::OneShotAsync)
//!   (feature `tokio`) — async, `recv_async().await`. Use when the wake
//!   fires from a non-tokio thread and the waiter is a tokio task.
//! - Future runtimes (io_uring, …) — write one new `Waiter` impl and
//!   `OneShot<T, MyWaiter>` works automatically.
//!
//! ## Cost model
//!
//! `send`:
//!   1. Slot write
//!   2. `state.store(FULL, Release)`
//!   3. `waiter.wake()` — Relaxed load on hot path (no syscall)
//!
//! `recv` (value already there): one Acquire CAS on state
//! (`FULL → TAKEN`) + one `assume_init_read`. No park, no spin.
//!
//! `recv` (value not there yet): the waiter does the spin-then-park
//! dance (sync) or the notified-await loop (async). Predicate is
//! `state != EMPTY`.
//!
//! ## Drop safety
//!
//! - If the [`Sender`] is dropped without sending, the [`Receiver`]
//!   wakes with `Err(Closed)`.
//! - If the [`Receiver`] is dropped before `recv`, [`Sender::send`]
//!   silently drops the value — `Inner`'s `Drop` runs
//!   `assume_init_drop` if `state == FULL`.

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

#[cfg(feature = "tokio")]
use crate::waiter::AsyncWaiter;
use crate::waiter::BlockingWaiter;
use crate::waiter::{ParkWaiter, Waiter};

/// State of the OneShot's slot. Transitions are one-way:
/// `EMPTY` → `FULL` (sender sent), or `EMPTY` → `CLOSED` (sender dropped).
/// Once `FULL`, a successful `try_take` swaps to `TAKEN` so subsequent
/// reads return `Closed` instead of double-reading the slot.
const STATE_EMPTY: u8 = 0;
const STATE_FULL: u8 = 1;
const STATE_CLOSED: u8 = 2;
const STATE_TAKEN: u8 = 3;

/// Error returned when the [`Sender`] dropped without sending, or when
/// the value has already been taken.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Closed;

#[repr(C, align(64))]
struct Inner<T: Send, W: Waiter> {
    /// State machine: EMPTY / FULL / CLOSED / TAKEN.
    state: AtomicU8,
    /// Wait/wake backend. The trait is generic — `ParkWaiter` for sync
    /// OS-thread, `NotifyWaiter` for tokio, future impls for io_uring.
    waiter: W,
    /// Slot storage. Written once by the sender on `send`; read once by
    /// the receiver on a successful FULL→TAKEN CAS. Drop of `Inner`
    /// runs `assume_init_drop` if `state == FULL` (no one read it).
    slot: UnsafeCell<MaybeUninit<T>>,
    _marker: PhantomData<fn() -> T>,
}

// Safety: the slot is accessed only after the state machine grants
// exclusive access (write by sender on EMPTY, read by receiver on
// FULL→TAKEN CAS). The state's Release/Acquire ordering provides
// cross-thread visibility for the slot bytes.
unsafe impl<T: Send, W: Waiter> Send for Inner<T, W> {}
unsafe impl<T: Send, W: Waiter> Sync for Inner<T, W> {}

impl<T: Send, W: Waiter> Drop for Inner<T, W> {
    fn drop(&mut self) {
        // If a value was sent but never received, drop it.
        if self.state.load(Ordering::Acquire) == STATE_FULL {
            // Safety: state == FULL ⇒ slot is initialised and untaken.
            unsafe {
                (*self.slot.get()).assume_init_drop();
            }
        }
    }
}

/// Constructor namespace. Generic over payload `T` and waiter `W`.
///
/// `OneShot<T>` (default `W = ParkWaiter`) is the sync, OS-thread variant.
/// `OneShot<T, NotifyWaiter>` (alias [`OneShotAsync<T>`](super::OneShotAsync))
/// is the tokio-async variant.
pub struct OneShot<T: Send, W: Waiter = ParkWaiter>(PhantomData<(fn() -> T, fn() -> W)>);

impl<T: Send, W: Waiter> OneShot<T, W> {
    /// Build a new `OneShot` pair. Returns `(Sender, Receiver)`.
    #[inline]
    pub fn new() -> (Sender<T, W>, Receiver<T, W>) {
        let inner = Arc::new(Inner {
            state: AtomicU8::new(STATE_EMPTY),
            waiter: W::default(),
            slot: UnsafeCell::new(MaybeUninit::uninit()),
            _marker: PhantomData,
        });
        (
            Sender {
                inner: inner.clone(),
                sent: false,
            },
            Receiver { inner },
        )
    }
}

/// Producer half. Fires exactly one value or drops without firing.
pub struct Sender<T: Send, W: Waiter = ParkWaiter> {
    inner: Arc<Inner<T, W>>,
    sent: bool,
}

impl<T: Send, W: Waiter> Sender<T, W> {
    /// Deliver the value. Consumes `self`. Wakes the receiver via
    /// `W::wake` if it had announced parking.
    #[inline]
    pub fn send(mut self, value: T) {
        // Safety: state is EMPTY (we are the only sender, no concurrent
        // writer to the slot exists yet); Release store on state makes
        // the slot's contents visible to the receiver's Acquire CAS.
        unsafe {
            (*self.inner.slot.get()).write(value);
        }
        self.inner.state.store(STATE_FULL, Ordering::Release);
        self.inner.waiter.wake();
        self.sent = true;
    }
}

impl<T: Send, W: Waiter> Drop for Sender<T, W> {
    fn drop(&mut self) {
        if !self.sent {
            // Mark closed BEFORE waking. Release pairs with the
            // receiver's Acquire CAS; the wake() call observes the
            // state change.
            self.inner.state.store(STATE_CLOSED, Ordering::Release);
            self.inner.waiter.wake();
        }
    }
}

/// Consumer half. Receives exactly one value or `Closed`.
pub struct Receiver<T: Send, W: Waiter = ParkWaiter> {
    inner: Arc<Inner<T, W>>,
}

impl<T: Send, W: Waiter> Receiver<T, W> {
    /// Register the calling thread as the consumer (sync waiters only —
    /// no-op for async waiters). Must be called from the thread that
    /// will block on [`recv`](Self::recv), before that call.
    #[inline]
    pub fn bind(&self) {
        self.inner.waiter.set_worker(std::thread::current());
    }

    /// Borrow the underlying waiter. Useful when composing this oneshot
    /// into a larger topology that wants to register the consumer
    /// through a different path than `bind`.
    #[inline]
    pub fn waiter(&self) -> &W {
        &self.inner.waiter
    }

    /// Non-blocking poll. Returns `Ok(Some(v))` if a value is ready,
    /// `Err(Closed)` if the sender dropped without sending (or the
    /// value has already been taken), `Ok(None)` if neither has
    /// happened yet.
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
    ///   - `Some(Ok(v))`  → `FULL` → `TAKEN`, value extracted.
    ///   - `Some(Err(C))` → `CLOSED` or already `TAKEN`.
    ///   - `None`         → `EMPTY` (still pending).
    #[inline]
    fn try_take(&self) -> Option<Result<T, Closed>> {
        match self.inner.state.compare_exchange(
            STATE_FULL,
            STATE_TAKEN,
            Ordering::Acquire,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Safety: we transitioned FULL → TAKEN; no one else
                // can read or write the slot now. The sender wrote the
                // value before storing FULL with Release; our Acquire
                // CAS synchronizes-with that store.
                let v = unsafe { (*self.inner.slot.get()).assume_init_read() };
                Some(Ok(v))
            }
            Err(STATE_CLOSED) | Err(STATE_TAKEN) => Some(Err(Closed)),
            Err(_) => None, // STATE_EMPTY — still pending
        }
    }
}

// ── Sync recv: requires `W: BlockingWaiter` ─────────────────────────────

impl<T: Send, W: BlockingWaiter> Receiver<T, W> {
    /// Block until a value is delivered or the [`Sender`] is dropped.
    /// Consumes `self`.
    ///
    /// # Panics
    /// If `bind` was never called (sync waiters only), the underlying
    /// waiter's `wait_until` panics rather than deadlock silently.
    #[inline]
    pub fn recv(self) -> Result<T, Closed> {
        self.inner
            .waiter
            .wait_until(|| self.inner.state.load(Ordering::Acquire) != STATE_EMPTY);
        // Predicate guarantees state != EMPTY ⇒ try_take returns `Some`.
        self.try_take()
            .expect("state != EMPTY guaranteed by wait_until")
    }
}

// ── Async recv: requires `W: AsyncWaiter` ───────────────────────────────

#[cfg(feature = "tokio")]
impl<T: Send + 'static, W: AsyncWaiter + 'static> Receiver<T, W> {
    /// Async receive. Must be polled from a runtime compatible with `W`.
    ///
    /// Naming: this is `recv_async` (not `recv`) because Rust requires
    /// distinct method names even when trait bounds are disjoint. Same
    /// convention as `flume`. The sync sibling is [`recv`](Self::recv)
    /// (gated on `W: BlockingWaiter`).
    ///
    /// Returns a boxed future to sidestep an RPITIT lifetime inference
    /// limitation (rust-lang/rust#100013): the inner `wait_until` future
    /// is `impl Future + Send + 'a`, and an `async fn` body that awaits
    /// it propagates the `'a` borrow through auto-trait inference,
    /// breaking `Send + 'static` coercion at downstream `tokio::spawn`
    /// call sites. Boxing the inner future erases `'a`. One alloc per
    /// receive — amortised against the syscall-class wake.
    pub fn recv_async(
        self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, Closed>> + Send>> {
        Box::pin(do_recv_async(self.inner))
    }
}

#[cfg(feature = "tokio")]
async fn do_recv_async<T: Send + 'static, W: AsyncWaiter + 'static>(
    inner: std::sync::Arc<Inner<T, W>>,
) -> Result<T, Closed> {
    let inner_for_pred = inner.clone();
    // Box the wait_until future to erase its `'a` borrow before the outer
    // `async fn` auto-trait inference runs.
    let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> = Box::pin(
        inner
            .waiter
            .wait_until(move || inner_for_pred.state.load(Ordering::Acquire) != STATE_EMPTY),
    );
    fut.await;
    // Predicate guarantees state != EMPTY ⇒ try_take returns `Some`.
    match inner.state.compare_exchange(
        STATE_FULL,
        STATE_TAKEN,
        Ordering::Acquire,
        Ordering::Acquire,
    ) {
        Ok(_) => {
            // SAFETY: FULL → TAKEN; sender's Release pairs with our Acquire.
            let v = unsafe { (*inner.slot.get()).assume_init_read() };
            Ok(v)
        }
        Err(STATE_CLOSED) | Err(STATE_TAKEN) => Err(Closed),
        Err(_) => unreachable!("state != EMPTY guaranteed by wait_until"),
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
    #[should_panic(expected = "Park::wait_until reached park path without set_worker")]
    fn recv_without_bind_panics() {
        // Sender is alive but never sends; receiver never binds. After
        // burning the spin budget recv must reach the park path and
        // panic instead of hanging forever.
        let (_tx, rx) = OneShot::<u64>::new();
        let _ = rx.recv();
    }

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
        assert_eq!(
            received.as_ptr() as usize,
            ptr_before,
            "Box must be transferred zero-copy"
        );
    }

    #[test]
    fn dropped_value_runs_destructor() {
        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
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

    // ── Async-mirror tests (feature `tokio`) ────────────────────────

    #[cfg(feature = "tokio")]
    mod async_mirror {
        use super::super::*;
        use crate::waiter::NotifyWaiter;

        #[tokio::test]
        async fn basic_send_recv_async() {
            let (tx, rx) = OneShot::<u64, NotifyWaiter>::new();
            let h = tokio::task::spawn_blocking(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                tx.send(42);
            });
            assert_eq!(rx.recv_async().await, Ok(42));
            h.await.unwrap();
        }

        #[tokio::test]
        async fn sender_dropped_returns_closed_async() {
            let (tx, rx) = OneShot::<u64, NotifyWaiter>::new();
            drop(tx);
            assert_eq!(rx.recv_async().await, Err(Closed));
        }

        #[tokio::test]
        async fn cross_thread_wake_from_os_thread_async() {
            let (tx, rx) = OneShot::<u8, NotifyWaiter>::new();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(20));
                tx.send(99);
            });
            assert_eq!(rx.recv_async().await, Ok(99));
        }
    }
}
