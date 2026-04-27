//! `OneShotAsync<T>` — async sibling of [`OneShot`](super::OneShot).
//!
//! Same semantics as `OneShot` (single-use, 1:1, exactly-one payload, sender
//! consumes self on send, receiver consumes self on recv) but the receiver
//! half is async — backed by `tokio::sync::Notify`.
//!
//! ## When to reach for `OneShotAsync` instead of `OneShot`
//!
//! Same rule as [`PipeAsync`](crate::slot::PipeAsync) vs `Pipe`: use the
//! async variant when **the wake fires from a non-tokio thread** (TCP
//! reader, FFI callback, OS-thread worker) and **the waiter is a tokio
//! task**. The runtime multiplexes the wake onto a hot worker; the
//! sync `OneShot::recv` path's `Thread::unpark` lands the wake on a
//! single pinned thread that is often cache-cold.
//!
//! Measured on a TCP-loopback ack-release benchmark (release_primitive,
//! P=128): `OneShotAsync` is on par with `tokio::sync::oneshot` (~7.7
//! µs/op) and 2.5× faster than the sync `OneShot` path crossing the
//! same OS↔tokio boundary. In a pure in-process (no-I/O) bench the
//! sync `OneShot` is faster — use that one when both halves run on
//! OS threads.
//!
//! ## Implementation
//!
//! `Notify` + `AtomicU8` state machine + `UnsafeCell<MaybeUninit<T>>`.
//! Same state set as `OneShot`: EMPTY (0) → FULL (1) | CLOSED (2),
//! FULL → TAKEN (3). The Acquire CAS on `FULL → TAKEN` synchronises-with
//! the sender's `Release` store.
//!
//! Receiver builds the `notified()` future BEFORE the state check to
//! close the lost-notify race that would otherwise occur if the sender
//! delivered between the check and the await.
//!
//! ## Ownership and drop safety
//!
//! - `Sender::send` consumes `self` and is callable from any thread,
//!   no runtime context required.
//! - `Receiver::recv` consumes `self` and must be polled from a tokio
//!   runtime.
//! - If the sender drops without sending, the receiver wakes with
//!   `Err(Closed)`.
//! - If the receiver drops before recv, sender's send drops the value
//!   on the floor; the inner's `Drop` runs `assume_init_drop` if state
//!   is still `FULL`.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

const STATE_EMPTY:  u8 = 0;
const STATE_FULL:   u8 = 1;
const STATE_CLOSED: u8 = 2;
const STATE_TAKEN:  u8 = 3;

/// Sender dropped before sending.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Closed;

struct Inner<T: Send> {
    state:  AtomicU8,
    slot:   UnsafeCell<MaybeUninit<T>>,
    notify: Notify,
}

// Safety: the state machine grants exclusive slot access (only the sender
// writes, only on EMPTY; only the receiver reads, only on FULL→TAKEN);
// `Notify` is itself Send + Sync.
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T: Send> Drop for Inner<T> {
    fn drop(&mut self) {
        // Last `Arc` ref — if the slot is full and untaken, drop the value
        // to avoid leaking RAII resources.
        if self.state.load(Ordering::Acquire) == STATE_FULL {
            unsafe { (*self.slot.get()).assume_init_drop(); }
        }
    }
}

/// Send half. Consumes `self` on `send`. Callable from any thread.
pub struct Sender<T: Send> {
    inner: Arc<Inner<T>>,
    sent:  bool,
}

/// Receive half. Consumes `self` on `recv`. Must be polled from a tokio
/// runtime.
pub struct Receiver<T: Send> {
    inner: Arc<Inner<T>>,
}

/// Construct a fresh oneshot pair. Receiver-side `recv` requires a tokio
/// runtime; sender side does not.
pub struct OneShotAsync<T: Send>(std::marker::PhantomData<T>);

impl<T: Send> OneShotAsync<T> {
    /// Build a new `(Sender, Receiver)` pair.
    #[inline]
    pub fn new() -> (Sender<T>, Receiver<T>) {
        let inner = Arc::new(Inner {
            state:  AtomicU8::new(STATE_EMPTY),
            slot:   UnsafeCell::new(MaybeUninit::uninit()),
            notify: Notify::new(),
        });
        (Sender { inner: inner.clone(), sent: false }, Receiver { inner })
    }
}

impl<T: Send> Sender<T> {
    /// Deliver the value. Consumes `self`. Callable from any thread
    /// (including non-tokio threads).
    #[inline]
    pub fn send(mut self, v: T) {
        // Safety: SPSC contract — only the sender writes the slot, only
        // once. Precondition state == EMPTY (enforced by single-shot API).
        unsafe { (*self.inner.slot.get()).write(v); }
        self.inner.state.store(STATE_FULL, Ordering::Release);
        self.inner.notify.notify_one();
        self.sent = true;
    }
}

impl<T: Send> Drop for Sender<T> {
    fn drop(&mut self) {
        // Sender dropped without sending → mark CLOSED and wake the
        // receiver so it returns `Err(Closed)` instead of hanging.
        if !self.sent {
            self.inner.state.store(STATE_CLOSED, Ordering::Release);
            self.inner.notify.notify_one();
        }
    }
}

impl<T: Send> Receiver<T> {
    /// Async receive. Must be polled from a tokio runtime.
    ///
    /// Returns `Ok(v)` if the sender delivered, `Err(Closed)` if the
    /// sender dropped without sending. The `notified()` future is built
    /// BEFORE the state check to close the lost-notify race.
    pub async fn recv(self) -> Result<T, Closed> {
        loop {
            let notified = self.inner.notify.notified();
            match self.try_take() {
                Some(r) => return r,
                None => notified.await,
            }
        }
    }

    /// Non-blocking attempt. `Some(Ok)` on delivered, `Some(Err)` on
    /// closed, `None` if still empty.
    #[inline]
    pub fn try_recv(&self) -> Option<Result<T, Closed>> {
        self.try_take()
    }

    #[inline]
    fn try_take(&self) -> Option<Result<T, Closed>> {
        match self.inner.state.compare_exchange(
            STATE_FULL,
            STATE_TAKEN,
            Ordering::Acquire,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Safety: FULL→TAKEN claimed atomically; sender's slot
                // write is visible via the Acquire-paired Release store.
                let v = unsafe { (*self.inner.slot.get()).assume_init_read() };
                Some(Ok(v))
            }
            Err(STATE_CLOSED) | Err(STATE_TAKEN) => Some(Err(Closed)),
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering as AtomOrd};

    #[tokio::test]
    async fn basic_send_recv_async() {
        let (tx, rx) = OneShotAsync::<u64>::new();
        let h = tokio::task::spawn_blocking(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            tx.send(42);
        });
        assert_eq!(rx.recv().await, Ok(42));
        h.await.unwrap();
    }

    #[tokio::test]
    async fn sender_dropped_returns_closed() {
        let (tx, rx) = OneShotAsync::<u64>::new();
        drop(tx);
        assert_eq!(rx.recv().await, Err(Closed));
    }

    #[tokio::test]
    async fn cross_thread_wake_from_os_thread() {
        // Intended use case: send fires from a plain OS thread with no
        // tokio context, recv awaits inside a tokio task.
        let (tx, rx) = OneShotAsync::<u8>::new();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            tx.send(99);
        });
        assert_eq!(rx.recv().await, Ok(99));
    }

    #[test]
    fn try_recv_states() {
        let (tx, rx) = OneShotAsync::<u32>::new();
        assert_eq!(rx.try_recv(), None);
        tx.send(7);
        assert_eq!(rx.try_recv(), Some(Ok(7)));
        // After taken: subsequent try_recv reports closed/taken.
        assert_eq!(rx.try_recv(), Some(Err(Closed)));
    }

    #[test]
    fn drop_drains_inflight() {
        struct Tracked(std::sync::Arc<AtomicU64>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, AtomOrd::Relaxed); }
        }
        let drops = std::sync::Arc::new(AtomicU64::new(0));
        {
            let (tx, _rx) = OneShotAsync::<Tracked>::new();
            tx.send(Tracked(drops.clone()));
            // _rx dropped without recv — Inner::drop must run assume_init_drop.
        }
        assert_eq!(drops.load(AtomOrd::Relaxed), 1);
    }
}
