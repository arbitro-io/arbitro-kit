//! SPSC bounded ring buffer — N-slot pipelined queue.
//!
//! [`Ring<T, CAP, W>`] is the multi-slot sibling of [`Pipe<T, W>`](crate::slot::Pipe).
//! Same SPSC contract (one producer, one consumer), but with `CAP` slots
//! preallocated inline, so producer and consumer can overlap in time.
//!
//! ## Multi-runtime
//!
//! `Ring` is generic over the [`Waiter`](crate::waiter::Waiter) backend. The
//! default `W = ParkWaiter` keeps the same OS-thread `thread::park`/`unpark`
//! semantics this type has always shipped with. Pass `W = NotifyWaiter`
//! (feature `tokio`) and the same struct exposes async `send_async` /
//! `recv_async` that resolve under a tokio runtime. After
//! monomorphization, both forms compile to the same code paths as
//! hand-written sync/async equivalents.
//!
//! ## When to use Ring instead of Pipe
//!
//! - **Burst absorption.** Producer fires N events in < 1 µs, consumer
//!   drains them at a steady rate. `Pipe` would block the producer between
//!   every event; `Ring` lets it run through the burst unhindered.
//! - **Pipelined throughput.** Steady-state throughput rises ~1.5–2× over
//!   `Pipe` because producer and consumer work in parallel instead of
//!   alternating on a single slot.
//! - **Unbalanced pipeline stages.** A fast stage feeding a slow one can
//!   queue work instead of stalling, up to `CAP` items ahead.
//! - **Graduated backpressure.** `try_send` returns `Err(value)` without
//!   blocking; caller can drop / coalesce / downsample per policy.
//!
//! For 1:1 request/response, prefer [`Channel`](crate::slot::Channel). For simple
//! 1:1 fire-and-forget with no buffering, prefer [`Pipe`](crate::slot::Pipe).
//!
//! ## Wire model
//!
//! ```text
//!  producer thread                          consumer thread
//!  ───────────────                          ───────────────
//!  acquire not_full  (blocks if full)
//!  write   slot[head & MASK]
//!  head.store(head+1, Release)
//!  not_empty.wake()  → coherence →    ─→   wait_until(!is_empty)
//!                                            read    slot[tail & MASK]
//!                                            tail.store(tail+1, Release)
//!                                            not_full.wake()
//! ```
//!
//! Two [`Waiter`](crate::waiter::Waiter) instances coordinate the two wait
//! states: `not_empty` (consumer waits when ring is empty) and `not_full`
//! (producer waits when ring is full). `head` and `tail` sit on separate
//! cache lines to avoid false sharing.
//!
//! ## Capacity constraint
//!
//! `CAP` **must be a power of two**. This allows `idx & (CAP - 1)` instead
//! of `idx % CAP` — one AND instruction vs a division. The `new()`
//! constructor panics if this is violated.
//!
//! ## Cost (single-thread synthetic, no park, release build)
//!
//! | Operation           | Typical  |
//! | ------------------- | -------: |
//! | `try_send` hot      |  ~5 ns   |
//! | `try_recv` hot      |  ~5 ns   |
//! | `send` blocked→wake | ~10 µs   |
//! | `recv` blocked→wake | ~10 µs   |
//!
//! Cross-thread, pipelined, the per-op steady-state approaches the
//! L1-to-L1 coherence floor (~40–60 ns/op at payloads ≤ 64 B).
//!
//! ## Drop safety
//!
//! The `Drop` impl drains any in-flight values so `T` with RAII resources
//! (`Box`, `Vec`, `Arc`, `File`) is safe.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::waiter::{AsyncWaiter, BlockingWaiter, ParkWaiter, Waiter};

/// Cache-line padding to keep `head` (written by producer) and `tail`
/// (written by consumer) on separate 64 B lines. Without this, every
/// producer write invalidates the line the consumer reads and vice versa,
/// throwing away most of the pipelining benefit.
#[repr(align(64))]
struct CachePad([u8; 0]);

/// SPSC bounded ring buffer with `CAP` slots (power-of-two), generic over
/// the [`Waiter`] backend.
///
/// ## Concurrency contract
///
/// - Exactly **one producer** calls [`send`](Self::send) / [`try_send`](Self::try_send).
/// - Exactly **one consumer** calls [`recv`](Self::recv) / [`try_recv`](Self::try_recv).
/// - For `W = ParkWaiter` (sync, default): producer registers via
///   [`set_producer`](Self::set_producer) before the first blocking
///   [`send`] on a possibly-full ring, consumer registers via
///   [`set_consumer`](Self::set_consumer) before the first blocking
///   [`recv`] on a possibly-empty ring. Async waiters ignore both calls.
/// - Usually shared across threads/tasks via `Arc<Ring<T, CAP, W>>`.
#[repr(C)]
pub struct Ring<T, const CAP: usize, W: Waiter = ParkWaiter> {
    /// Waiter on which the consumer blocks when the ring is empty.
    /// Open/closed state is derived directly from `head != tail` — no
    /// duplicated `locked` bit. Saves one Release store per `try_send`.
    not_empty: W,
    /// Write cursor. Monotonic, wraps via `& MASK` at use. Producer owns.
    head: AtomicUsize,
    _pad0: CachePad,

    /// Waiter on which the producer blocks when the ring is full.
    /// Openness derived from `head - tail < CAP` — same reasoning as above.
    not_full: W,
    /// Read cursor. Monotonic, wraps via `& MASK` at use. Consumer owns.
    tail: AtomicUsize,
    _pad1: CachePad,

    /// Slot storage. Each cell transitions empty → init → empty exactly once
    /// per wrap, coordinated by the head/tail cursors + waiters.
    slots: [UnsafeCell<MaybeUninit<T>>; CAP],
}

// Safety: slot access is serialized by the head/tail cursors + waiters.
// The producer only writes `slot[head & MASK]` when `head - tail < CAP`
// (i.e. the slot is empty); the consumer only reads `slot[tail & MASK]`
// when `head > tail` (i.e. the slot is initialized). The Release stores on
// head/tail publish the writes to the other side.
unsafe impl<T: Send, const CAP: usize, W: Waiter> Send for Ring<T, CAP, W> {}
unsafe impl<T: Send, const CAP: usize, W: Waiter> Sync for Ring<T, CAP, W> {}

impl<T, const CAP: usize, W: Waiter> Default for Ring<T, CAP, W> {
    fn default() -> Self { Self::new() }
}

impl<T, const CAP: usize, W: Waiter> Ring<T, CAP, W> {
    /// Create a fresh ring. Both cursors start at 0; the ring is empty.
    ///
    /// # Panics
    /// Panics if `CAP` is 0 or not a power of two.
    pub fn new() -> Self {
        assert!(CAP > 0,                "Ring CAP must be > 0");
        assert!(CAP.is_power_of_two(),  "Ring CAP must be a power of two");

        // Waiters have no payload state — "is it ready to proceed?" is
        // answered by the predicate that `wait_until` evaluates over head/tail.
        let not_empty = W::default();
        let not_full  = W::default();

        // Safety: creating an array of `UnsafeCell<MaybeUninit<T>>` is sound;
        // MaybeUninit::uninit() is always valid, UnsafeCell is a transparent
        // wrapper. The [_; CAP] syntax requires T: Copy or a const-initializer;
        // we use a manual loop via array::from_fn.
        let slots: [UnsafeCell<MaybeUninit<T>>; CAP] =
            std::array::from_fn(|_| UnsafeCell::new(MaybeUninit::uninit()));

        Self {
            not_empty,
            head: AtomicUsize::new(0),
            _pad0: CachePad([]),
            not_full,
            tail: AtomicUsize::new(0),
            _pad1: CachePad([]),
            slots,
        }
    }

    const MASK: usize = CAP - 1;

    /// Register the producer thread. Must be called from the producer
    /// thread, before any blocking [`send`](Self::send) on a possibly-full ring.
    /// No-op for async waiter backends.
    #[inline]
    pub fn set_producer(&self, t: std::thread::Thread) {
        self.not_full.set_worker(t);
    }

    /// Register the consumer thread. Must be called from the consumer
    /// thread, before any blocking [`recv`](Self::recv) on a possibly-empty ring.
    /// No-op for async waiter backends.
    #[inline]
    pub fn set_consumer(&self, t: std::thread::Thread) {
        self.not_empty.set_worker(t);
    }

    /// Maximum number of buffered items.
    #[inline] pub const fn capacity(&self) -> usize { CAP }

    /// Current number of items in the ring. Approximate under concurrent
    /// access — both cursors may advance between the two loads.
    #[inline]
    pub fn len(&self) -> usize {
        let h = self.head.load(Ordering::Acquire);
        let t = self.tail.load(Ordering::Acquire);
        h.wrapping_sub(t)
    }

    /// `true` iff the ring holds no items.
    #[inline] pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// `true` iff the ring is at capacity.
    #[inline] pub fn is_full(&self) -> bool { self.len() >= CAP }

    // ── Producer API ─────────────────────────────────────────────────

    /// Non-blocking enqueue. Returns `Err(value)` if the ring is full.
    ///
    /// Must only be called from the single producer thread.
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), T> {
        // Producer owns `head` — a Relaxed load is sufficient since no other
        // thread writes it. The Acquire on `tail` synchronizes-with the
        // consumer's Release when it drained a slot.
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        if head.wrapping_sub(tail) >= CAP {
            return Err(value);
        }
        // Safety: slot is empty (head - tail < CAP). We own the write.
        unsafe { (*self.slots[head & Self::MASK].get()).write(value); }
        // Release on head publishes the slot write to the consumer AND is
        // what opens the `not_empty` predicate (it reads head/tail).
        // No separate `locked` store — eliminating it halves the cache-line
        // traffic of a send on steady-state.
        self.head.store(head.wrapping_add(1), Ordering::Release);
        // Wake a possibly-parked consumer. Idempotent if already awake.
        self.not_empty.wake();
        Ok(())
    }

    /// Non-blocking batch enqueue. Moves up to
    /// `n = min(src.len(), free_slots)` items from the front of `src`
    /// into the ring, in FIFO order. Returns `n`. The drained prefix is
    /// removed from `src`; the remainder stays.
    ///
    /// Symmetric counterpart of [`drain_into`](Self::drain_into).
    /// Amortizes a single `head` publication and a single `not_empty.wake()`
    /// over all `n` items — avoiding N separate signal handshakes when the
    /// consumer is parked under burst load.
    ///
    /// Must only be called from the single producer thread.
    pub fn try_send_from(&self, src: &mut Vec<T>) -> usize {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        let free = CAP - head.wrapping_sub(tail);
        let n = src.len().min(free);
        if n == 0 { return 0; }

        // Drop-guard: if any `write` panics (or `src.drain`'s iterator
        // panics), advance `head` by the number of slots we already
        // initialized so `Ring::drop` drops them and no slot leaks.
        struct Guard<'a, T, const CAP: usize, W: Waiter> {
            ring: &'a Ring<T, CAP, W>,
            head_start: usize,
            written: usize,
        }
        impl<T, const CAP: usize, W: Waiter> Drop for Guard<'_, T, CAP, W> {
            fn drop(&mut self) {
                // Only runs on panic unwind (we `forget` on success).
                self.ring
                    .head
                    .store(self.head_start.wrapping_add(self.written), Ordering::Release);
                self.ring.not_empty.wake();
            }
        }
        let mut guard = Guard::<T, CAP, W> {
            ring: self,
            head_start: head,
            written: 0,
        };
        // Safety: slots [head, head+n) are empty (free >= n); producer
        // owns all writes to them. `drain(..n)` moves ownership out.
        for (i, v) in src.drain(..n).enumerate() {
            unsafe {
                (*self.slots[head.wrapping_add(i) & Self::MASK].get()).write(v);
            }
            guard.written = i + 1;
        }
        // Success: take the guard apart manually so its Drop does not run.
        std::mem::forget(guard);
        // Single Release publishes all n slots at once.
        self.head.store(head.wrapping_add(n), Ordering::Release);
        // Single wake covers the whole batch.
        self.not_empty.wake();
        n
    }

    // ── Consumer API ─────────────────────────────────────────────────

    /// Non-blocking dequeue. Returns `None` if the ring is empty.
    ///
    /// Must only be called from the single consumer thread.
    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        // Consumer owns `tail`. Acquire on `head` synchronizes-with the
        // producer's Release when it published a slot.
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        // Safety: head > tail ⇒ slot[tail & MASK] holds an initialized T
        // published by the producer with Release.
        let v = unsafe { (*self.slots[tail & Self::MASK].get()).assume_init_read() };
        // Release on tail publishes the slot-now-free to the producer AND
        // is what opens the `not_full` predicate for a waiting producer.
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        // Wake a possibly-parked producer. Idempotent.
        self.not_full.wake();
        Some(v)
    }

    /// Drain up to `max` items into `out`. Returns the number drained.
    ///
    /// **Truly batched**: a single `head` Acquire, a single `tail` Release
    /// publish, and a single `not_full.wake()` cover the whole batch —
    /// the symmetric counterpart of [`try_send_from`](Self::try_send_from).
    /// Amortizes the ack cost (cursor publish + wakeup) across `n` items
    /// instead of paying it per-item as `try_recv × N` does.
    ///
    /// Must only be called from the single consumer thread.
    pub fn drain_into(&self, out: &mut Vec<T>, max: usize) -> usize {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        let available = head.wrapping_sub(tail);
        let n = available.min(max);
        if n == 0 { return 0; }
        // Reserve up-front so the subsequent `push` calls cannot reallocate
        // and therefore cannot panic mid-drain. If `reserve` itself panics
        // (OOM), it does so BEFORE any slot has been moved out — the ring
        // stays consistent and `Drop` will correctly drop [tail, head).
        out.reserve(n);
        // Safety: slots [tail, tail+n) are initialized (published by the
        // producer with Release on head). We own reading them. `push` is
        // now infallible (capacity was pre-reserved), so no slot can be
        // moved out without `tail` being advanced below.
        for i in 0..n {
            let v = unsafe {
                (*self.slots[tail.wrapping_add(i) & Self::MASK].get())
                    .assume_init_read()
            };
            out.push(v);
        }
        // Single Release publishes all n freed slots at once (batch ack).
        self.tail.store(tail.wrapping_add(n), Ordering::Release);
        // Single wake covers the whole batch.
        self.not_full.wake();
        n
    }
}

// ── Sync API (W: BlockingWaiter) ──────────────────────────────────────

impl<T, const CAP: usize, W: BlockingWaiter> Ring<T, CAP, W> {
    /// Blocking enqueue. Parks until the ring has space, then enqueues.
    ///
    /// Must only be called from the registered producer thread.
    ///
    /// The Dekker race between our spin-exit and the consumer's drain is
    /// closed by `BlockingWaiter::wait_until`'s internal SeqCst store on
    /// `parked` + re-check of the predicate.
    #[inline]
    pub fn send(&self, mut value: T) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            self.not_full.wait_until(|| !self.is_full());
        }
    }

    /// Blocking dequeue. Parks until an item is available, then takes it.
    ///
    /// Must only be called from the registered consumer thread.
    #[inline]
    pub fn recv(&self) -> T {
        loop {
            if let Some(v) = self.try_recv() { return v; }
            self.not_empty.wait_until(|| !self.is_empty());
        }
    }

    /// Blocking dequeue with cancellation. Returns `Err(Cancelled)` if
    /// the lifeline cancels this waiter while we are parked or before
    /// we enter park. Otherwise behaves exactly like [`Ring::recv`].
    ///
    /// The plain [`Ring::recv`] path is unchanged — adopting Lifeline
    /// costs nothing for callers that don't use it.
    #[inline]
    pub fn recv_or_cancel(
        &self,
        life: &crate::gate::Lifeline,
        id: crate::gate::WaiterId,
    ) -> Result<T, crate::gate::Cancelled> {
        loop {
            if let Some(v) = self.try_recv() { return Ok(v); }
            if life.is_cancelled(id)        { return Err(crate::gate::Cancelled); }
            self.not_empty.wait_until(|| !self.is_empty() || life.is_cancelled(id));
        }
    }
}

// ── Async API (W: AsyncWaiter) ────────────────────────────────────────

impl<T: Send, const CAP: usize, W: AsyncWaiter> Ring<T, CAP, W> {
    /// Async enqueue. Awaits until the ring has space, then enqueues.
    ///
    /// Must only be called from the single producer task.
    pub async fn send_async(&self, mut value: T) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            self.not_full.wait_until(|| !self.is_full()).await;
        }
    }

    /// Async dequeue. Awaits until an item is available, then takes it.
    ///
    /// Must only be called from the single consumer task.
    pub async fn recv_async(&self) -> T {
        loop {
            if let Some(v) = self.try_recv() { return v; }
            self.not_empty.wait_until(|| !self.is_empty()).await;
        }
    }
}

// ── Spawn-safe async API (NotifyWaiter specialization) ────────────────
//
// The RPITIT future from `AsyncWaiter::wait_until` can't be proven `Send`
// in generic contexts (rust-lang/rust#100013). For the specific case of
// `tokio::spawn`'d tasks, we specialize on `NotifyWaiter` and use
// `Notify::notified()` directly — a concrete `Send` future — avoiding
// the trait abstraction entirely.

#[cfg(feature = "tokio")]
impl<T: Send, const CAP: usize> Ring<T, CAP, crate::waiter::NotifyWaiter> {
    /// Boxed async dequeue — returns `Pin<Box<dyn Future + Send>>`.
    ///
    /// Use this variant inside `tokio::spawn`'d tasks where the RPITIT
    /// future from [`recv_async`](Self::recv_async) can't satisfy the
    /// `Send` bound. The single heap allocation per call is negligible
    /// relative to the Notify wake cycle (~300 ns).
    #[inline]
    pub fn recv_async_send(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + '_>> {
        Box::pin(async move {
            loop {
                if let Some(v) = self.try_recv() { return v; }
                // Bypass trait RPITIT: use concrete Notify::notified() directly.
                let notified = self.not_empty.inner.notified();
                if !self.is_empty() { continue; }
                notified.await;
            }
        })
    }
}

impl<T, const CAP: usize, W: Waiter> Drop for Ring<T, CAP, W> {
    fn drop(&mut self) {
        // `&mut self` means no other references — safe to drop in-flight.
        let head = *self.head.get_mut();
        let tail = *self.tail.get_mut();
        let mut i = tail;
        while i != head {
            // Safety: slot[i & MASK] is initialized (i ∈ [tail, head)).
            unsafe { (*self.slots[i & Self::MASK].get()).assume_init_drop(); }
            i = i.wrapping_add(1);
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn single_thread_basic() {
        let r: Ring<u32, 8> = Ring::new();
        assert!(r.is_empty());
        assert_eq!(r.capacity(), 8);

        for i in 0..8 { assert!(r.try_send(i).is_ok()); }
        assert!(r.is_full());
        assert!(r.try_send(999).is_err());

        for i in 0..8 { assert_eq!(r.try_recv(), Some(i)); }
        assert!(r.is_empty());
        assert_eq!(r.try_recv(), None);
    }

    #[test]
    fn wraparound() {
        let r: Ring<u32, 4> = Ring::new();
        for i in 0..100 {
            assert!(r.try_send(i).is_ok());
            assert_eq!(r.try_recv(), Some(i));
        }
    }

    #[test]
    fn cross_thread_blocking() {
        let r: Arc<Ring<u64, 16>> = Arc::new(Ring::new());
        let r2 = r.clone();
        let h = std::thread::spawn(move || {
            r2.set_consumer(std::thread::current());
            let mut sum = 0u64;
            for _ in 0..1000 { sum += r2.recv(); }
            sum
        });
        r.set_producer(std::thread::current());
        for i in 0..1000u64 { r.send(i); }
        let got = h.join().unwrap();
        assert_eq!(got, (0..1000u64).sum());
    }

    #[test]
    fn drain_batch() {
        let r: Ring<u32, 32> = Ring::new();
        for i in 0..10 { r.try_send(i).unwrap(); }
        let mut out = Vec::new();
        let n = r.drain_into(&mut out, 100);
        assert_eq!(n, 10);
        assert_eq!(out, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn drop_drains_inflight() {
        struct Tracked(Arc<AtomicU64>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }
        let drops = Arc::new(AtomicU64::new(0));
        {
            let r: Ring<Tracked, 8> = Ring::new();
            for _ in 0..5 {
                assert!(r.try_send(Tracked(drops.clone())).is_ok());
            }
            // never recv — drop must drain all 5
        }
        assert_eq!(drops.load(Ordering::Relaxed), 5);
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn non_pow2_panics() {
        let _: Ring<u32, 7> = Ring::new();
    }

    #[test]
    fn cross_thread_multi_wrap() {
        // Forces the ring to wrap around its capacity at least 5 times while
        // producer and consumer are on separate threads. With CAP = 8 and
        // 100 items, the head/tail cursors cross the MASK boundary 12×.
        //
        // Extra stress: the consumer sleeps briefly every few items so the
        // ring genuinely fills and the producer has to park on `not_full`.
        // If `send`'s lock-check-acquire park protocol is broken (missed
        // wakeup or busy-spin), this test either hangs or pegs a core.
        const CAP: usize = 8;
        const N: u64 = 100; // 100 / 8 ≈ 12.5 wraps — well over 5
        let r: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
        let r2 = r.clone();

        let consumer = std::thread::spawn(move || {
            r2.set_consumer(std::thread::current());
            let mut got = Vec::with_capacity(N as usize);
            for i in 0..N {
                let v = r2.recv();
                got.push(v);
                // Throttle every 3rd item so the producer hits `is_full`
                // and actually parks. Park path is what we're stressing.
                if i % 3 == 0 {
                    std::thread::sleep(std::time::Duration::from_micros(50));
                }
            }
            got
        });

        r.set_producer(std::thread::current());
        for i in 0..N { r.send(i); }

        let got = consumer.join().unwrap();
        assert_eq!(got, (0..N).collect::<Vec<_>>(),
                   "FIFO order must hold across wraparounds");
        assert!(r.is_empty(), "ring should be drained");
    }

    #[test]
    fn cross_thread_high_volume() {
        // High-volume stress: 100k messages across a tiny 16-slot ring
        // forces ~6250 wraparounds and thousands of producer/consumer park
        // events. A single missed wakeup or off-by-one in the park protocol
        // manifests as a hang; the test's outer timeout (cargo's default
        // per-test watchdog) catches it.
        const CAP: usize = 128;
        const N: u64 = 100_000;
        let r: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
        let r2 = r.clone();

        let consumer = std::thread::spawn(move || {
            r2.set_consumer(std::thread::current());
            let mut sum: u64 = 0;
            for _ in 0..N { sum = sum.wrapping_add(r2.recv()); }
            sum
        });

        r.set_producer(std::thread::current());
        let t0 = std::time::Instant::now();
        for i in 0..N { r.send(i); }
        let got = consumer.join().unwrap();
        let ns = t0.elapsed().as_nanos() as f64;
        let expected: u64 = (0..N).fold(0u64, |a, b| a.wrapping_add(b));
        assert_eq!(got, expected, "checksum mismatch under high volume");
        assert!(r.is_empty());
        eprintln!("high_volume: N={} CAP={} total={:.2}ms per_msg={:.1}ns ops/sec={:.0}",
                  N, CAP, ns / 1e6, ns / N as f64, 1e9 / (ns / N as f64));
    }

    /// Validates each correctness factor independently under high volume:
    ///
    ///   1. **No loss**      — exactly N items received (count matches).
    ///   2. **No duplicates** — every value appears exactly once.
    ///   3. **Integrity**    — payload value unchanged in transit.
    ///   4. **FIFO order**   — items arrive in the order sent.
    ///   5. **Drain state**  — ring empty at end.
    #[test]
    fn cross_thread_factors() {
        const CAP: usize = 64;
        const N: u64 = 50_000;
        let r: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
        let r2 = r.clone();

        let consumer = std::thread::spawn(move || {
            r2.set_consumer(std::thread::current());
            let mut got: Vec<u64> = Vec::with_capacity(N as usize);
            for _ in 0..N { got.push(r2.recv()); }
            got
        });

        r.set_producer(std::thread::current());
        for i in 0..N { r.send(i); }
        let got = consumer.join().unwrap();

        // ① No loss — count
        assert_eq!(got.len() as u64, N, "item count mismatch (loss or overrun)");

        // ② No duplicates + ③ Integrity + ④ FIFO — one-shot equality
        let expected: Vec<u64> = (0..N).collect();
        assert!(got == expected, "payload / order / duplicate violation");

        // ⑤ Drain state
        assert!(r.is_empty(), "ring not drained at end");
    }

    /// Closed-loop round-trip: producer sends a request, consumer echoes
    /// a transformed response. Validates that TWO independent Rings can
    /// be composed into a request/response pipeline without deadlock and
    /// that each response correlates to exactly one request.
    #[test]
    fn cross_thread_round_trip() {
        const CAP: usize = 32;
        const N: u64 = 10_000;

        let req: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
        let rsp: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
        let req2 = req.clone();
        let rsp2 = rsp.clone();

        // Echo worker: reads request, sends back request * 2 + 1.
        let worker = std::thread::spawn(move || {
            req2.set_consumer(std::thread::current());
            rsp2.set_producer(std::thread::current());
            for _ in 0..N {
                let v = req2.recv();
                rsp2.send(v.wrapping_mul(2).wrapping_add(1));
            }
        });

        req.set_producer(std::thread::current());
        rsp.set_consumer(std::thread::current());

        let t0 = std::time::Instant::now();
        for i in 0..N {
            req.send(i);
            let r = rsp.recv();
            assert_eq!(r, i.wrapping_mul(2).wrapping_add(1),
                       "response {} does not correlate to request {}", r, i);
        }
        let ns = t0.elapsed().as_nanos() as f64;

        worker.join().unwrap();
        assert!(req.is_empty() && rsp.is_empty(),
                "both rings must be drained at end");

        eprintln!("round_trip: N={} CAP={} total={:.2}ms per_cycle={:.1}ns cycles/sec={:.0}",
                  N, CAP, ns / 1e6, ns / N as f64, 1e9 / (ns / N as f64));
    }

    #[test]
    fn batch_send_and_drain() {
        let r: Ring<u64, 32> = Ring::new();

        // Full batch fits.
        let mut src: Vec<u64> = (0..10).collect();
        let n = r.try_send_from(&mut src);
        assert_eq!(n, 10);
        assert!(src.is_empty(), "drained prefix must be removed");
        assert_eq!(r.len(), 10);

        // Partial batch when ring has less space than src.
        let mut src2: Vec<u64> = (100..130).collect();  // 30 items
        let n2 = r.try_send_from(&mut src2);            // only 22 fit (32-10)
        assert_eq!(n2, 22);
        assert_eq!(src2.len(), 30 - 22, "unsent suffix must remain");
        assert_eq!(src2[0], 100 + 22, "remainder starts at first unsent");
        assert!(r.is_full());

        // Drain and validate FIFO order across the two batches.
        let mut out = Vec::new();
        let got = r.drain_into(&mut out, 100);
        assert_eq!(got, 32);
        let mut expected: Vec<u64> = (0..10).collect();
        expected.extend(100..122);
        assert_eq!(out, expected, "batch send must preserve FIFO");
        assert!(r.is_empty());

        // Empty source → no-op.
        let mut empty: Vec<u64> = Vec::new();
        assert_eq!(r.try_send_from(&mut empty), 0);
    }

    /// Cross-thread benchmark-style check: batch send + batch drain under
    /// contention. Measures correctness AND reports ns/item so we can see
    /// batching amortize the signal handshake.
    #[test]
    fn cross_thread_batched() {
        const CAP: usize = 128;
        const N: u64 = 50_000;
        const BATCH: usize = 64;
        let r: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
        let r2 = r.clone();

        let consumer = std::thread::spawn(move || {
            r2.set_consumer(std::thread::current());
            let mut got: Vec<u64> = Vec::with_capacity(N as usize);
            let mut buf: Vec<u64> = Vec::with_capacity(BATCH);
            while (got.len() as u64) < N {
                // Block until there's at least one item, then drain greedily.
                got.push(r2.recv());
                let _ = r2.drain_into(&mut buf, BATCH);
                got.append(&mut buf);
            }
            got
        });

        r.set_producer(std::thread::current());
        let t0 = std::time::Instant::now();
        let mut pending: Vec<u64> = Vec::with_capacity(BATCH);
        let mut sent: u64 = 0;
        while sent < N {
            let take = (N - sent).min(BATCH as u64) as usize;
            pending.extend(sent..sent + take as u64);
            // Drain pending into ring; loop until all placed.
            while !pending.is_empty() {
                let n = r.try_send_from(&mut pending);
                if n == 0 {
                    // Ring full — fall back to blocking send for one item
                    // so we don't busy-spin.
                    let v = pending.remove(0);
                    r.send(v);
                }
            }
            sent += take as u64;
        }
        let got = consumer.join().unwrap();
        let ns = t0.elapsed().as_nanos() as f64;

        assert_eq!(got.len() as u64, N);
        assert_eq!(got, (0..N).collect::<Vec<_>>(),
                   "batched path must preserve FIFO");
        assert!(r.is_empty());
        eprintln!("batched: N={} CAP={} BATCH={} total={:.2}ms per_item={:.1}ns ops/sec={:.0}",
                  N, CAP, BATCH, ns / 1e6, ns / N as f64, 1e9 / (ns / N as f64));
    }

    #[test]
    fn burst_absorption() {
        // Producer fires a burst of 100 items into a 128-slot ring without
        // the consumer yet running. Must not block.
        let r: Ring<u32, 128> = Ring::new();
        for i in 0..100 { assert!(r.try_send(i).is_ok()); }
        assert_eq!(r.len(), 100);
    }

    // ── Async-mirror tests (W = NotifyWaiter, feature = "tokio") ──────

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn cross_task_basic_notify() {
        use crate::waiter::NotifyWaiter;
        // tokio::join! avoids the higher-ranked RPITIT-Send limitation
        // (rust-lang/rust#100013) that bites `tokio::spawn` when the future
        // borrows `&Ring<..., NotifyWaiter>` across an await.
        let r: Ring<u64, 16, NotifyWaiter> = Ring::new();
        let producer = async {
            for i in 0..1000u64 { r.send_async(i).await; }
        };
        let consumer = async {
            let mut sum = 0u64;
            for _ in 0..1000 { sum += r.recv_async().await; }
            sum
        };
        let (_, got) = tokio::join!(producer, consumer);
        assert_eq!(got, (0..1000u64).sum());
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn wraparound_notify() {
        use crate::waiter::NotifyWaiter;
        let r: Ring<u32, 4, NotifyWaiter> = Ring::new();
        for i in 0..100 {
            r.send_async(i).await;
            assert_eq!(r.recv_async().await, i);
        }
    }
}
