//! `OneSignal` — single-use, payloadless gate, generic over `Waiter`.
//!
//! The minimal "block until released" primitive. Specialised replacement
//! for `tokio::sync::oneshot` in scenarios where the **value travels
//! separately** (e.g. through a frame field, an `AtomicU64`, or a
//! pre-existing data path) and the OneSignal only carries the wake.
//!
//! Generic over the wait/wake backend `W: Waiter`:
//! - `OneSignal<ParkWaiter>` (default): sync `acquire` + `acquire_timeout`.
//! - `OneSignal<NotifyWaiter>` (feature `tokio`): async `acquire_async`.
//! - any future `Waiter` impl (io_uring, etc.) inherits the same API.
//!
//! ## Cost (default `ParkWaiter`)
//!
//! - `release()`: 1 SeqCst store + `Waiter::wake` (~0.3 ns when not parked,
//!   one syscall otherwise).
//! - `acquire()`: spin (64 tight + 512 PAUSE) → park if still pending.
//! - `acquire_timeout(d)`: same spin window, then `park_timeout(d)` loop.
//!
//! ## Concurrency contract
//!
//! - Exactly one `Sender` produces one `release()` (or drops).
//! - Exactly one `Receiver` performs one `acquire*` (or drops).
//! - Both halves are `Send + Sync` (auto, given `W: Send + Sync`).

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::waiter::{BlockingWaiter, ParkWaiter, Waiter};

const STATE_PENDING: u8 = 0;
const STATE_RELEASED: u8 = 1;
const STATE_CLOSED: u8 = 2;

/// Result of a wait that did not return Ok.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AcquireError {
    /// Sender was dropped without calling `release`.
    Closed,
    /// `acquire_timeout` deadline elapsed before release.
    TimedOut,
}

#[repr(align(64))]
struct Inner<W: Waiter> {
    state: AtomicU8,
    waiter: W,
}

/// Constructor namespace, parameterised by the wait/wake backend.
pub struct OneSignal<W: Waiter = ParkWaiter>(PhantomData<W>);

impl<W: Waiter> OneSignal<W> {
    /// Build a fresh `(Sender, Receiver)` pair.
    #[inline]
    pub fn new() -> (Sender<W>, Receiver<W>) {
        let inner = Arc::new(Inner {
            state: AtomicU8::new(STATE_PENDING),
            waiter: W::default(),
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

pub struct Sender<W: Waiter> {
    inner: Arc<Inner<W>>,
    released: bool,
}

impl<W: Waiter> Sender<W> {
    /// Release the gate. Consumes self. The receiver wakes from
    /// `acquire*` with `Ok(())`.
    #[inline]
    pub fn release(mut self) {
        // SeqCst pairs with the Waiter's Dekker barrier (e.g. ParkWaiter's
        // SeqCst parked.store) so a wake is never lost.
        self.inner.state.store(STATE_RELEASED, Ordering::SeqCst);
        self.inner.waiter.wake();
        self.released = true;
    }
}

impl<W: Waiter> Drop for Sender<W> {
    fn drop(&mut self) {
        if !self.released {
            self.inner.state.store(STATE_CLOSED, Ordering::SeqCst);
            self.inner.waiter.wake();
        }
    }
}

// ─── Receiver ──────────────────────────────────────────────────────────────

pub struct Receiver<W: Waiter> {
    inner: Arc<Inner<W>>,
}

impl<W: Waiter> Receiver<W> {
    /// Register the calling thread as the receiver. Mandatory for
    /// `ParkWaiter`-backed signals; no-op for runtime-multiplexed waiters.
    #[inline]
    pub fn bind(&self) {
        self.inner.waiter.set_worker(std::thread::current());
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
}

// ─── Sync acquire (BlockingWaiter) ────────────────────────────────────────

impl<W: BlockingWaiter> Receiver<W> {
    /// Block until the sender calls `release` or is dropped.
    #[inline]
    pub fn acquire(self) -> Result<(), AcquireError> {
        let state = &self.inner.state;
        self.inner
            .waiter
            .wait_until(|| state.load(Ordering::Acquire) != STATE_PENDING);
        match state.load(Ordering::Acquire) {
            STATE_RELEASED => Ok(()),
            STATE_CLOSED => Err(AcquireError::Closed),
            _ => unreachable!("wait_until returned with state == PENDING"),
        }
    }
}

// ─── Async acquire (AsyncWaiter) ──────────────────────────────────────────

#[cfg(feature = "tokio")]
impl<W: crate::waiter::AsyncWaiter + 'static> Receiver<W> {
    /// Await until the sender calls `release` or is dropped.
    ///
    /// Returns a boxed future to sidestep an RPITIT lifetime inference
    /// limitation (rust-lang/rust#100013) that rejects the natural
    /// `async fn` form when the receiver is consumed by value. The single
    /// allocation per acquire is amortised against a syscall-class wake.
    pub fn acquire_async(
        self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), AcquireError>> + Send>> {
        Box::pin(do_acquire_async(self.inner))
    }
}

#[cfg(feature = "tokio")]
async fn do_acquire_async<W: crate::waiter::AsyncWaiter + 'static>(
    inner: Arc<Inner<W>>,
) -> Result<(), AcquireError> {
    let inner_for_pred = inner.clone();
    // Box the wait_until future to erase its `'a` borrow lifetime — without
    // this, the outer `async fn` body propagates the RPITIT borrow into its
    // auto-`Send`/`'static` inference and rust-lang/rust#100013 rejects the
    // resulting `Pin<Box<dyn Future + Send + 'static>>` coercion at the
    // call site (kit's only async path that returns a boxed dyn future).
    let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> = Box::pin(
        inner
            .waiter
            .wait_until(move || inner_for_pred.state.load(Ordering::Acquire) != STATE_PENDING),
    );
    fut.await;
    match inner.state.load(Ordering::Acquire) {
        STATE_RELEASED => Ok(()),
        STATE_CLOSED => Err(AcquireError::Closed),
        _ => unreachable!("wait_until returned with state == PENDING"),
    }
}

// ─── Timeout (ParkWaiter only) ────────────────────────────────────────────

impl Receiver<ParkWaiter> {
    /// Block until release/drop or until `timeout` elapses.
    /// Returns `Err(AcquireError::TimedOut)` on deadline.
    ///
    /// Concrete to `ParkWaiter` because the generic `BlockingWaiter` trait
    /// has no deadline-aware variant; timed waits are inherently tied to
    /// `thread::park_timeout`.
    #[inline]
    pub fn acquire_timeout(self, timeout: Duration) -> Result<(), AcquireError> {
        let deadline = Instant::now() + timeout;
        let state = &self.inner.state;
        let timed_out = self
            .inner
            .waiter
            .wait_until_deadline(deadline, || state.load(Ordering::Acquire) != STATE_PENDING);
        match state.load(Ordering::Acquire) {
            STATE_RELEASED => Ok(()),
            STATE_CLOSED => Err(AcquireError::Closed),
            _ if timed_out => Err(AcquireError::TimedOut),
            _ => unreachable!(),
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
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
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
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
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
        let (_tx, rx) = OneSignal::<ParkWaiter>::new();
        rx.bind();
        let started = Instant::now();
        let result = rx.acquire_timeout(Duration::from_millis(50));
        assert_eq!(result, Err(AcquireError::TimedOut));
        assert!(started.elapsed() >= Duration::from_millis(45));
    }

    #[test]
    fn acquire_timeout_returns_ok_if_released_in_time() {
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
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
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
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
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
        assert_eq!(rx.try_acquire(), Ok(None));
        tx.release();
        assert_eq!(rx.try_acquire(), Ok(Some(())));
    }

    #[test]
    fn try_acquire_after_drop() {
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
        drop(tx);
        assert_eq!(rx.try_acquire(), Err(AcquireError::Closed));
    }

    #[test]
    fn release_before_acquire_no_park() {
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
        tx.release();
        rx.bind();
        assert_eq!(rx.acquire(), Ok(()));
    }

    #[test]
    fn high_volume_pairs() {
        for _ in 0..1000 {
            let (tx, rx) = OneSignal::<ParkWaiter>::new();
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
        let (_tx, rx) = OneSignal::<ParkWaiter>::new();
        rx.bind();
        let r = rx.acquire_timeout(Duration::from_nanos(0));
        assert_eq!(r, Err(AcquireError::TimedOut));
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn async_release_wakes_awaiter() {
        use crate::waiter::NotifyWaiter;
        let (tx, rx) = OneSignal::<NotifyWaiter>::new();
        let h = tokio::spawn(async move { rx.acquire_async().await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        tx.release();
        assert_eq!(h.await.unwrap(), Ok(()));
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn async_sender_drop_returns_closed() {
        use crate::waiter::NotifyWaiter;
        let (tx, rx) = OneSignal::<NotifyWaiter>::new();
        let h = tokio::spawn(async move { rx.acquire_async().await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(tx);
        assert_eq!(h.await.unwrap(), Err(AcquireError::Closed));
    }
}
