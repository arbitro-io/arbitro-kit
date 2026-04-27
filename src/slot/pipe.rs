//! Single-slot SPSC transport, generic over the wait/wake backend.
//!
//! [`Pipe<T, H, W>`] is the minimal atom between a raw waiter (no payload)
//! and [`Channel`](super::Channel) (bidirectional request/response): one
//! producer sends a `T`, one consumer receives it, a single
//! [`Waiter`](crate::waiter::Waiter) coordinates wake/wait.
//!
//! ## Wire model
//!
//! ```text
//!  producer thread/task                  consumer thread/task
//!  ────────────────────                  ────────────────────
//!  hook.on_send(&v)
//!  write    slot     ──┐
//!  has_data = true   ──┘ (Release)
//!  waiter.wake()       ────────────────► waiter.wait_until(has_data)
//!                                        read     slot
//!                                        has_data = false (Release)
//!                                        hook.on_recv(&v)
//! ```
//!
//! ## Runtime — pick at the type level
//!
//! - `Pipe<T>` (default `W = ParkWaiter`) — sync, OS-thread, `recv()` blocks.
//! - `Pipe<T, NoHook, NotifyWaiter>` aka [`PipeAsync<T>`] (feature `tokio`) —
//!   async, `recv().await`. Use this when the wake fires from a non-tokio
//!   thread and the waiter is a tokio task.
//! - Future runtimes (io_uring, ...) — write one new `Waiter` impl and
//!   `Pipe<T, NoHook, MyWaiter>` works automatically.
//!
//! ## Hooks — zero-cost observation
//!
//! The generic `H: PipeHook<T>` defaults to [`NoHook`], a ZST whose default
//! methods are empty `#[inline]` functions. The optimizer eliminates the
//! calls entirely — a `Pipe<T>` has the same cost as a hand-rolled
//! waiter+slot pair.

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::waiter::{ParkWaiter, Waiter};
#[cfg(feature = "tokio")]
use crate::waiter::AsyncWaiter;
use crate::waiter::BlockingWaiter;

/// Observer hook for a [`Pipe`].
///
/// Both methods default to empty `#[inline]` no-ops. A hook with no custom
/// behavior (notably [`NoHook`]) compiles down to zero instructions on the
/// hot path.
pub trait PipeHook<T>: Send + Sync {
    /// Called by the producer **before** the value is written to the slot.
    #[inline]
    fn on_send(&self, _v: &T) {}

    /// Called by the consumer **after** the value is taken from the slot.
    #[inline]
    fn on_recv(&self, _v: &T) {}
}

/// Default no-op hook. Zero-sized; fully elided by the optimizer.
#[derive(Default, Copy, Clone, Debug)]
pub struct NoHook;

impl<T> PipeHook<T> for NoHook {}

/// SPSC single-slot pipe.
///
/// Generic over payload `T`, observer `H: PipeHook<T>`, and waiter
/// `W: Waiter`. Defaults make `Pipe<T>` a drop-in minimal sync transport.
///
/// ## Concurrency contract
///
/// - Exactly one producer calls [`send`](Self::send).
/// - Exactly one consumer calls [`recv`](Self::recv) / [`try_recv`](Self::try_recv).
/// - For sync waiters (`W: BlockingWaiter`), the consumer must register
///   its `Thread` handle via [`set_consumer`](Self::set_consumer) before
///   calling `recv`. For async waiters (`W: AsyncWaiter`), this is a no-op.
///
/// ## Drop safety
///
/// If a value is in flight when the pipe is dropped, `Drop` runs
/// `assume_init_drop` on the slot — safe for `T` holding RAII resources.
#[repr(C)]
pub struct Pipe<T, H: PipeHook<T> = NoHook, W: Waiter = ParkWaiter> {
    waiter:   W,
    slot:     UnsafeCell<MaybeUninit<T>>,
    has_data: AtomicBool,
    hook:     H,
    _marker:  PhantomData<fn() -> T>,
}

// Safety: slot access is serialized by the `has_data` flag — the producer
// writes the slot before storing `has_data = true` (Release), the consumer
// observes via `has_data.load(Acquire)`, reads the slot, then stores
// `has_data = false` (Release). Exactly-one-producer / one-consumer is an
// unchecked runtime contract of the SPSC type.
unsafe impl<T: Send, H: PipeHook<T>, W: Waiter> Send for Pipe<T, H, W> {}
unsafe impl<T: Send, H: PipeHook<T>, W: Waiter> Sync for Pipe<T, H, W> {}

impl<T: Send, W: Waiter> Pipe<T, NoHook, W> {
    /// Create a pipe with the zero-cost default hook.
    pub fn new() -> Self {
        Self::with_hook(NoHook)
    }
}

impl<T: Send, W: Waiter> Default for Pipe<T, NoHook, W> {
    fn default() -> Self { Self::new() }
}

impl<T: Send, H: PipeHook<T>, W: Waiter> Pipe<T, H, W> {
    /// Create a pipe with a custom observer hook.
    pub fn with_hook(hook: H) -> Self {
        Self {
            waiter:   W::default(),
            slot:     UnsafeCell::new(MaybeUninit::uninit()),
            has_data: AtomicBool::new(false),
            hook,
            _marker:  PhantomData,
        }
    }

    /// Borrow the underlying waiter. Useful for composing a pipe into
    /// larger topologies (e.g. registering its worker via the waiter).
    #[inline]
    pub fn waiter(&self) -> &W { &self.waiter }

    /// Borrow the observer hook.
    #[inline]
    pub fn hook(&self) -> &H { &self.hook }

    /// Register the consumer thread. Must be called from the consumer
    /// thread before the first blocking [`recv`](Self::recv) (sync waiters
    /// only — no-op for async waiters).
    #[inline]
    pub fn set_consumer(&self, t: std::thread::Thread) {
        self.waiter.set_worker(t);
    }

    /// Send `v` across the pipe.
    ///
    /// Must only be called from the single producer, and only when the
    /// pipe is empty (the consumer drained the previous value). Breaking
    /// either invariant is a logic bug — the pipe does not check.
    #[inline]
    pub fn send(&self, v: T) {
        self.hook.on_send(&v);
        // Safety: SPSC contract — sole producer, slot is empty.
        unsafe { (*self.slot.get()).write(v); }
        self.has_data.store(true, Ordering::Release);
        self.waiter.wake();
    }

    /// Non-blocking take. Returns `Some(v)` if a value is pending, `None`
    /// otherwise.
    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        if !self.has_data.load(Ordering::Acquire) { return None; }
        // Safety: producer wrote the slot before storing `has_data = true`
        // with Release; our Acquire load synchronises-with that store.
        let v = unsafe { (*self.slot.get()).assume_init_read() };
        self.has_data.store(false, Ordering::Release);
        self.hook.on_recv(&v);
        Some(v)
    }

    /// True iff a value is pending in the slot.
    #[inline]
    pub fn has_data(&self) -> bool {
        self.has_data.load(Ordering::Acquire)
    }
}

// ── Sync recv: requires `W: BlockingWaiter` ─────────────────────────────

impl<T: Send, H: PipeHook<T>, W: BlockingWaiter> Pipe<T, H, W> {
    /// Block until the producer sends, then take the value.
    ///
    /// Must only be called from the registered consumer thread.
    ///
    /// # Panics
    /// If `set_consumer` was never called, the underlying waiter's
    /// `wait_until` panics rather than deadlock silently.
    #[inline]
    pub fn recv(&self) -> T {
        self.waiter.wait_until(|| self.has_data.load(Ordering::Acquire));
        // Safety: predicate returned true; same as `try_recv`.
        let v = unsafe { (*self.slot.get()).assume_init_read() };
        self.has_data.store(false, Ordering::Release);
        self.hook.on_recv(&v);
        v
    }
}

// ── Async recv: requires `W: AsyncWaiter` ───────────────────────────────

#[cfg(feature = "tokio")]
impl<T: Send, H: PipeHook<T>, W: AsyncWaiter> Pipe<T, H, W> {
    /// Async receive. Must be polled from a runtime compatible with `W`.
    ///
    /// Naming: this is `recv_async` (not `recv`) because Rust requires
    /// distinct method names even when trait bounds are disjoint. Same
    /// convention as `flume`. The sync sibling is [`recv`](Self::recv)
    /// (gated on `W: BlockingWaiter`).
    pub async fn recv_async(&self) -> T {
        self.waiter
            .wait_until(|| self.has_data.load(Ordering::Acquire))
            .await;
        // Safety: predicate returned true; same as `try_recv`.
        let v = unsafe { (*self.slot.get()).assume_init_read() };
        self.has_data.store(false, Ordering::Release);
        self.hook.on_recv(&v);
        v
    }
}

impl<T, H: PipeHook<T>, W: Waiter> Drop for Pipe<T, H, W> {
    fn drop(&mut self) {
        // Safety: `&mut self` ⇒ no other references. If a value is in
        // flight, drop it to avoid leaking RAII resources.
        if self.has_data.load(Ordering::Acquire) {
            unsafe { (*self.slot.get()).assume_init_drop(); }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomOrd};

    #[test]
    fn basic_send_recv() {
        let p: Arc<Pipe<u64>> = Arc::new(Pipe::new());
        let p2 = p.clone();
        let h = std::thread::spawn(move || {
            p2.set_consumer(std::thread::current());
            p2.recv()
        });
        std::thread::sleep(std::time::Duration::from_millis(10));
        p.send(42);
        assert_eq!(h.join().unwrap(), 42);
    }

    #[test]
    #[should_panic(expected = "Park::wait_until reached park path without set_worker")]
    fn recv_without_set_consumer_panics() {
        let p: Pipe<u64> = Pipe::new();
        let _ = p.recv();
    }

    #[test]
    fn try_recv_empty() {
        let p: Pipe<u64> = Pipe::new();
        assert_eq!(p.try_recv(), None);
        p.send(7);
        assert_eq!(p.try_recv(), Some(7));
        assert_eq!(p.try_recv(), None);
    }

    #[test]
    fn hook_fires_on_both_sides() {
        #[derive(Default)]
        struct Counters { sends: AtomicU64, recvs: AtomicU64 }
        impl PipeHook<u64> for Counters {
            fn on_send(&self, _: &u64) { self.sends.fetch_add(1, AtomOrd::Relaxed); }
            fn on_recv(&self, _: &u64) { self.recvs.fetch_add(1, AtomOrd::Relaxed); }
        }

        let p: Pipe<u64, Counters> = Pipe::with_hook(Counters::default());
        p.set_consumer(std::thread::current());
        for i in 0..5 {
            p.send(i);
            assert_eq!(p.recv(), i);
        }
        assert_eq!(p.hook().sends.load(AtomOrd::Relaxed), 5);
        assert_eq!(p.hook().recvs.load(AtomOrd::Relaxed), 5);
    }

    #[test]
    fn drop_drains_inflight() {
        struct Tracked(Arc<AtomicU64>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, AtomOrd::Relaxed); }
        }
        let drops = Arc::new(AtomicU64::new(0));
        {
            let p: Pipe<Tracked> = Pipe::new();
            p.send(Tracked(drops.clone()));
            // never recv — drop must drain
        }
        assert_eq!(drops.load(AtomOrd::Relaxed), 1);
    }

    #[test]
    fn box_ownership_transfer() {
        let p: Arc<Pipe<Box<Vec<u8>>>> = Arc::new(Pipe::new());
        let p2 = p.clone();
        let h = std::thread::spawn(move || {
            p2.set_consumer(std::thread::current());
            p2.recv()
        });
        std::thread::sleep(std::time::Duration::from_millis(10));
        let payload = Box::new(vec![1u8, 2, 3, 4]);
        let ptr_before = payload.as_ptr() as usize;
        p.send(payload);
        let got = h.join().unwrap();
        assert_eq!(*got, vec![1, 2, 3, 4]);
        assert_eq!(got.as_ptr() as usize, ptr_before, "heap buffer did not move");
    }

    #[test]
    fn has_data_reflects_state() {
        let p: Pipe<u32> = Pipe::new();
        p.set_consumer(std::thread::current());
        assert!(!p.has_data());
        p.send(1);
        assert!(p.has_data());
        assert_eq!(p.recv(), 1);
        assert!(!p.has_data());
    }

    // ── Async-mirror tests (feature = "tokio") ──────────────────────────

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn basic_send_recv_async() {
        use crate::waiter::NotifyWaiter;
        type PipeA<T> = Pipe<T, NoHook, NotifyWaiter>;

        let p: Arc<PipeA<u64>> = Arc::new(PipeA::new());
        let p2 = p.clone();
        let h = tokio::task::spawn_blocking(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            p2.send(42);
        });
        assert_eq!(p.recv_async().await,42);
        h.await.unwrap();
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn cross_thread_wake_from_os_thread_async() {
        use crate::waiter::NotifyWaiter;
        type PipeA<T> = Pipe<T, NoHook, NotifyWaiter>;

        let p: Arc<PipeA<u8>> = Arc::new(PipeA::new());
        let p2 = p.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            p2.send(99);
        });
        assert_eq!(p.recv_async().await,99);
    }
}
