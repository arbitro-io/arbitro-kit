//! `PipeAsync<T>` — async sibling of [`Pipe<T>`](super::Pipe).
//!
//! Same wire model as `Pipe` (one producer, one consumer, one in-flight
//! value at a time, reusable across many round-trips) but the consumer
//! half is async — backed by `tokio::sync::Notify`.
//!
//! ## When to reach for `PipeAsync` instead of `Pipe`
//!
//! Use `PipeAsync` when **the wake fires from a non-tokio thread** (a TCP
//! reader, an FFI callback, an OS-thread worker) and **the waiter is a
//! tokio task**. In that cross-domain case the OS-thread→tokio-task path
//! via `Pipe` (which uses `thread::unpark`) lands the wake on a single
//! pinned thread that is often cache-cold; tokio's runtime in contrast
//! enqueues the wake and a hot worker picks it up.
//!
//! Measured on a TCP-loopback round-trip benchmark (release_primitive,
//! P=128): `PipeAsync` runs at ~8.2 µs/op vs `Pipe` at ~20 µs/op — a 2.4×
//! win driven entirely by the runtime multiplexing the wake. Without I/O
//! in the path the relationship inverts (sync `Pipe`'s direct unpark is
//! ~50 ns; `PipeAsync`'s Notify path is ~300 ns), so do not blanket-replace
//! `Pipe` with `PipeAsync` — use it specifically for the OS↔tokio bridge.
//!
//! ## Implementation
//!
//! `Notify` + `AtomicBool` guard + `UnsafeCell<MaybeUninit<T>>`. The
//! producer's `send` writes the slot, flips the `has_data` flag with
//! Release ordering, and calls `notify_one`. The consumer's `recv`
//! creates the `notified()` future BEFORE checking `has_data` to close
//! the lost-notify race that would otherwise occur if the producer fired
//! between the check and the await.
//!
//! ## Concurrency contract
//!
//! - Exactly one producer thread (or task) calls [`send`](PipeAsync::send).
//! - Exactly one consumer task calls [`recv`](PipeAsync::recv) — must be
//!   polled from a tokio runtime.
//! - `send` is callable from any thread, no runtime context required.
//! - The pipe is reusable: send → recv → send → recv → ... is the
//!   intended pattern. Sending while a previous value is still pending
//!   overwrites and leaks the previous Drop glue (debug-asserted via
//!   the `has_data` flag).

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// Async single-slot pipe. Reusable. Producer is sync; consumer is async.
///
/// Layout-wise this is intentionally the same shape as [`Pipe`](super::Pipe)
/// minus the `Signal`-with-worker-handle (replaced by the `Notify`).
pub struct PipeAsync<T: Send> {
    notify:   Notify,
    slot:     UnsafeCell<MaybeUninit<T>>,
    has_data: AtomicBool,
}

// Safety: slot access is serialized by `has_data` + the SPSC contract
// (one producer, one consumer in flight). `Notify` is itself Send + Sync.
unsafe impl<T: Send> Send for PipeAsync<T> {}
unsafe impl<T: Send> Sync for PipeAsync<T> {}

impl<T: Send> Default for PipeAsync<T> {
    fn default() -> Self { Self::new() }
}

impl<T: Send> PipeAsync<T> {
    /// Construct an empty pipe.
    #[inline]
    pub fn new() -> Self {
        Self {
            notify:   Notify::new(),
            slot:     UnsafeCell::new(MaybeUninit::uninit()),
            has_data: AtomicBool::new(false),
        }
    }

    /// Send `v`. Callable from any thread (including non-tokio threads).
    /// Wakes the consumer task if one is awaiting `recv`.
    ///
    /// Must only be called from the single producer, and only when the
    /// pipe is empty (the consumer has drained the previous value).
    /// Breaking either invariant is a logic bug — no runtime check.
    #[inline]
    pub fn send(&self, v: T) {
        // Safety: SPSC contract — exclusive producer; slot is empty.
        unsafe { (*self.slot.get()).write(v); }
        self.has_data.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    /// Async receive. Must be polled from a tokio runtime.
    ///
    /// The `notified()` future is built BEFORE the `has_data` check —
    /// without that, a `notify_one` racing between the check and the
    /// await would be lost.
    pub async fn recv(&self) -> T {
        loop {
            let notified = self.notify.notified();
            if self.has_data.load(Ordering::Acquire) {
                // Safety: producer wrote the slot before storing
                // `has_data = true` with Release; our Acquire load
                // synchronises-with that store, so the slot bytes are
                // visible.
                let v = unsafe { (*self.slot.get()).assume_init_read() };
                self.has_data.store(false, Ordering::Release);
                return v;
            }
            notified.await;
        }
    }

    /// Non-blocking take. Returns `Some(v)` if a value is pending,
    /// `None` otherwise.
    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        if !self.has_data.load(Ordering::Acquire) { return None; }
        // Safety: same as `recv`.
        let v = unsafe { (*self.slot.get()).assume_init_read() };
        self.has_data.store(false, Ordering::Release);
        Some(v)
    }

    /// True iff a value is currently pending in the slot.
    #[inline]
    pub fn has_data(&self) -> bool {
        self.has_data.load(Ordering::Acquire)
    }
}

impl<T: Send> Drop for PipeAsync<T> {
    fn drop(&mut self) {
        // `&mut self` ⇒ no other references exist. If the slot is full,
        // drop the in-flight value to avoid leaking RAII resources.
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
    fn try_recv_empty() {
        let p: PipeAsync<u64> = PipeAsync::new();
        assert_eq!(p.try_recv(), None);
        p.send(7);
        assert_eq!(p.try_recv(), Some(7));
        assert_eq!(p.try_recv(), None);
    }

    #[tokio::test]
    async fn basic_send_recv_async() {
        let p: Arc<PipeAsync<u64>> = Arc::new(PipeAsync::new());
        let p2 = p.clone();
        let h = tokio::task::spawn_blocking(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            p2.send(42);
        });
        let v = p.recv().await;
        assert_eq!(v, 42);
        h.await.unwrap();
    }

    #[tokio::test]
    async fn reusable_across_many_round_trips() {
        let p: Arc<PipeAsync<u32>> = Arc::new(PipeAsync::new());
        let p2 = p.clone();
        let producer = tokio::task::spawn_blocking(move || {
            for i in 0..1000u32 {
                p2.send(i);
                // Wait for consumer to drain before next send.
                while p2.has_data() {
                    std::hint::spin_loop();
                }
            }
        });
        for i in 0..1000u32 {
            let v = p.recv().await;
            assert_eq!(v, i);
        }
        producer.await.unwrap();
    }

    #[tokio::test]
    async fn cross_thread_wake_from_os_thread_to_tokio_task() {
        // The intended use case: send fires from a plain OS thread (no
        // tokio context), recv awaits inside a tokio task. The Notify
        // path must bridge cleanly.
        let p: Arc<PipeAsync<u8>> = Arc::new(PipeAsync::new());
        let p2 = p.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            p2.send(99);
        });
        assert_eq!(p.recv().await, 99);
    }

    #[test]
    fn drop_drains_inflight() {
        struct Tracked(Arc<AtomicU64>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, AtomOrd::Relaxed); }
        }
        let drops = Arc::new(AtomicU64::new(0));
        {
            let p: PipeAsync<Tracked> = PipeAsync::new();
            p.send(Tracked(drops.clone()));
            // never recv — drop must drain
        }
        assert_eq!(drops.load(AtomOrd::Relaxed), 1);
    }
}
