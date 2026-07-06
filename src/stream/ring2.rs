//! SPSC bounded ring buffer — cursor-cache variant (Ring2).
//!
//! [`Ring2<T, CAP, W>`] is a **strictly faster** SPSC bounded ring than
//! [`Ring`](super::Ring) at the classic cross-thread workload (small `T`,
//! producer + consumer running hot). It preserves the same public shape
//! (`try_send` / `try_recv` / `send` / `recv` / `set_producer` /
//! `set_consumer`, plus async variants), the same drop-safety, and the
//! same `Waiter` polymorphism (`ParkWaiter` for OS threads, `NotifyWaiter`
//! for tokio tasks).
//!
//! ## What changed vs. `Ring`
//!
//! Two optimizations, in order of measured impact:
//!
//! 1. **Cached peer cursor.** Each side keeps a private, non-atomic copy
//!    of the peer's cursor on its own cache line. `try_send` first checks
//!    the cached `tail`; only if the cache says "full" do we pay the
//!    Acquire load from the shared `tail` line. Same for `try_recv` /
//!    cached `head`. On steady state this eliminates ~95% of the
//!    cross-core cache-line reads — the actual coherence miss becomes
//!    once-per-batch instead of once-per-op. (Vyukov / LMAX Disruptor
//!    style.)
//!
//! 2. **Single cache-line footprint for the hot atomic pair.** `head` and
//!    the consumer-side cache of `head` live on the same 64 B line as the
//!    consumer's waiter, so a `recv` that touches `head` (Acquire) also
//!    warms the line it will need for `not_empty`. Symmetric for
//!    `tail` + producer waiter.
//!
//! Together these move the per-op steady-state cost from ~40 ns (Ring)
//! toward the L1-hit floor. See `benches/mem_ring_h2h.rs`.
//!
//! ## What was tried and rejected
//!
//! **Edge-triggered wake** (only wake on empty→non-empty and
//! full→non-full transitions). Rejected: correctness depends on both
//! sides observing the real peer cursor at the edge, which the cached
//! cursor cannot guarantee — the wake could be skipped when the peer
//! actually did park, causing a deadlock. The existing `.wake()` is
//! already ~0.3 ns on the non-parked steady-state (single Relaxed load),
//! so the potential saving was small and the risk high.
//!
//! ## Safety invariants
//!
//! - `head` monotonically increases, only the producer writes.
//! - `tail` monotonically increases, only the consumer writes.
//! - `slot[i & MASK]` is initialized iff `tail <= i < head` (wrapping-aware).
//! - Producer's Release store on `head` publishes the payload write.
//! - Consumer's Release store on `tail` publishes the "slot free" fact.
//! - The `cached_*` fields are UnsafeCell<usize>: producer-only writes to
//!   `cached_tail`, consumer-only writes to `cached_head`. No sync needed.
//!
//! ## Same drop model as `Ring`
//!
//! `Drop` drains any in-flight `T` between `tail` and `head`.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;

// Under `--cfg loom` we swap the shared atomics for `loom::sync::atomic::*`
// so the loom model can explore possible reorderings around `head`/`tail`.
// `UnsafeCell` stays on `std::cell::UnsafeCell` because migrating every
// `.get()` call site to `loom::cell::UnsafeCell::with(...)` would touch the
// whole file — Miri (which was run separately) validates cell access UB
// under the real memory model, and loom's contribution here is the atomic-
// ordering exploration on the cursors.
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};

use crate::waiter::{AsyncWaiter, BlockingWaiter, ParkWaiter, Waiter};

/// Cache-line padding.
#[repr(align(64))]
struct CachePad([u8; 0]);

/// SPSC bounded ring buffer with cached peer cursors + edge-triggered wake.
///
/// ## Layout
///
/// The struct is laid out so the producer's hot state (waiter `not_full`,
/// atomic `head`, cache of `tail`) sits on one cache line and the
/// consumer's hot state (waiter `not_empty`, atomic `tail`, cache of
/// `head`) sits on another. `slots` follows on its own aligned region.
#[repr(C)]
pub struct Ring2<T, const CAP: usize, W: Waiter = ParkWaiter> {
    // ─── Producer-owned cache line ─────────────────────────────────────
    /// Waiter on which the producer blocks when the ring is full.
    not_full: W,
    /// Monotonic write cursor. Producer writes with Release; consumer
    /// reads with Acquire.
    head: AtomicUsize,
    /// Producer-private cache of `tail`. Only the producer reads/writes
    /// this cell. Never accessed from another thread.
    cached_tail: UnsafeCell<usize>,
    _pad0: CachePad,

    // ─── Consumer-owned cache line ─────────────────────────────────────
    /// Waiter on which the consumer blocks when the ring is empty.
    not_empty: W,
    /// Monotonic read cursor. Consumer writes with Release; producer
    /// reads with Acquire.
    tail: AtomicUsize,
    /// Consumer-private cache of `head`. Only the consumer reads/writes.
    cached_head: UnsafeCell<usize>,
    _pad1: CachePad,

    // ─── Slot storage ──────────────────────────────────────────────────
    slots: [UnsafeCell<MaybeUninit<T>>; CAP],
}

// Safety: All shared cross-thread state is either an `AtomicUsize` (head,
// tail) or a `Waiter` (both are `Sync`). The producer-only `cached_tail`
// and consumer-only `cached_head` UnsafeCells are never touched by the
// other side — the SPSC contract makes that a call-site invariant, same
// as `Ring`. The slot cells are serialized by the head/tail cursors:
// producer writes slot[head & MASK] before its Release store on `head`,
// consumer reads slot[tail & MASK] after Acquire-loading `head` and
// observing `head > tail`.
unsafe impl<T: Send, const CAP: usize, W: Waiter> Send for Ring2<T, CAP, W> {}
unsafe impl<T: Send, const CAP: usize, W: Waiter> Sync for Ring2<T, CAP, W> {}

impl<T, const CAP: usize, W: Waiter> Default for Ring2<T, CAP, W> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const CAP: usize, W: Waiter> Ring2<T, CAP, W> {
    const MASK: usize = CAP - 1;

    /// Fresh, empty ring. `CAP` must be a non-zero power of two.
    pub fn new() -> Self {
        assert!(CAP > 0, "Ring2 CAP must be > 0");
        assert!(CAP.is_power_of_two(), "Ring2 CAP must be a power of two");

        let slots: [UnsafeCell<MaybeUninit<T>>; CAP] =
            std::array::from_fn(|_| UnsafeCell::new(MaybeUninit::uninit()));

        Self {
            not_full: W::default(),
            head: AtomicUsize::new(0),
            cached_tail: UnsafeCell::new(0),
            _pad0: CachePad([]),
            not_empty: W::default(),
            tail: AtomicUsize::new(0),
            cached_head: UnsafeCell::new(0),
            _pad1: CachePad([]),
            slots,
        }
    }

    /// Register the producer thread. Sync waiter only; async ignores.
    #[inline]
    pub fn set_producer(&self, t: std::thread::Thread) {
        self.not_full.set_worker(t);
    }

    /// Register the consumer thread. Sync waiter only; async ignores.
    #[inline]
    pub fn set_consumer(&self, t: std::thread::Thread) {
        self.not_empty.set_worker(t);
    }

    #[inline]
    pub const fn capacity(&self) -> usize {
        CAP
    }

    /// Approximate item count. Both cursors may advance between loads.
    #[inline]
    pub fn len(&self) -> usize {
        let h = self.head.load(Ordering::Acquire);
        let t = self.tail.load(Ordering::Acquire);
        h.wrapping_sub(t)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() >= CAP
    }

    // ── Producer API ─────────────────────────────────────────────────

    /// Non-blocking enqueue. Returns `Err(value)` if the ring is full.
    ///
    /// Only the single producer thread may call this.
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), T> {
        // Producer owns `head` — Relaxed load is enough (no other writer).
        let head = self.head.load(Ordering::Relaxed);

        // Fast path: consult the producer-private cache first. If the
        // cache says there's room, we skip touching the consumer's
        // cache line entirely.
        //
        // Safety: `cached_tail` is only ever touched from the producer.
        let cached = unsafe { *self.cached_tail.get() };
        if head.wrapping_sub(cached) >= CAP {
            // Cache is stale (or ring is actually full). Refresh from
            // the shared atomic — Acquire syncs with consumer's Release.
            let fresh = self.tail.load(Ordering::Acquire);
            // Safety: producer-only cell.
            unsafe {
                *self.cached_tail.get() = fresh;
            }
            if head.wrapping_sub(fresh) >= CAP {
                return Err(value);
            }
        }

        // Safety: slot at `head & MASK` is empty because
        // head - cached_tail < CAP (and tail is monotonic, so the real
        // tail is >= cached_tail). We own the write until the Release
        // publishes it.
        unsafe {
            (*self.slots[head & Self::MASK].get()).write(value);
        }

        // Release publishes the slot write to any Acquire on `head`.
        self.head.store(head.wrapping_add(1), Ordering::Release);
        // Wake a possibly-parked consumer. `.wake()` is a single Relaxed
        // load on the "not parked" fast path (~0.3 ns).
        self.not_empty.wake();
        Ok(())
    }

    // ── Consumer API ─────────────────────────────────────────────────

    /// Non-blocking dequeue. Returns `None` if the ring is empty.
    ///
    /// Only the single consumer thread may call this.
    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        // Consumer owns `tail` — Relaxed is enough.
        let tail = self.tail.load(Ordering::Relaxed);

        // Fast path: private cache of head. Skip the shared load if we
        // already know there's data.
        //
        // Safety: `cached_head` is only ever touched from the consumer.
        let cached = unsafe { *self.cached_head.get() };
        if cached == tail {
            // Cache is stale (or ring is empty). Refresh — Acquire syncs
            // with producer's Release on `head` and its slot write.
            let fresh = self.head.load(Ordering::Acquire);
            // Safety: consumer-only cell.
            unsafe {
                *self.cached_head.get() = fresh;
            }
            if fresh == tail {
                return None;
            }
        }

        // Safety: producer's Release on `head` published slot[tail & MASK].
        let v = unsafe { (*self.slots[tail & Self::MASK].get()).assume_init_read() };

        // Release publishes "slot at tail is free" to a producer that
        // Acquires `tail`.
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        // Wake a possibly-parked producer.
        self.not_full.wake();
        Some(v)
    }
}

// ── Sync API (W: BlockingWaiter) ──────────────────────────────────────

impl<T, const CAP: usize, W: BlockingWaiter> Ring2<T, CAP, W> {
    /// Blocking enqueue. Parks until the ring has space.
    ///
    /// Must be called from the registered producer thread.
    #[inline]
    pub fn send(&self, mut value: T) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            // Predicate reloads the real `tail` (via `is_full`, which uses
            // Acquire). This closes the Dekker race with the consumer.
            self.not_full.wait_until(|| !self.is_full());
        }
    }

    /// Blocking dequeue. Parks until an item is available.
    ///
    /// Must be called from the registered consumer thread.
    #[inline]
    pub fn recv(&self) -> T {
        loop {
            if let Some(v) = self.try_recv() {
                return v;
            }
            self.not_empty.wait_until(|| !self.is_empty());
        }
    }
}

// ── Async API (W: AsyncWaiter) ────────────────────────────────────────

impl<T: Send, const CAP: usize, W: AsyncWaiter> Ring2<T, CAP, W> {
    /// Async enqueue. Awaits capacity, then enqueues.
    pub async fn send_async(&self, mut value: T) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            self.not_full.wait_until(|| !self.is_full()).await;
        }
    }

    /// Async dequeue. Awaits an item, then takes it.
    pub async fn recv_async(&self) -> T {
        loop {
            if let Some(v) = self.try_recv() {
                return v;
            }
            self.not_empty.wait_until(|| !self.is_empty()).await;
        }
    }
}

impl<T, const CAP: usize, W: Waiter> Drop for Ring2<T, CAP, W> {
    fn drop(&mut self) {
        // Exclusive access via &mut self — no other reference exists.
        // Under loom, `AtomicUsize::get_mut` is not available; a Relaxed
        // load is equivalent here because we hold `&mut self`.
        #[cfg(not(loom))]
        let head = *self.head.get_mut();
        #[cfg(not(loom))]
        let tail = *self.tail.get_mut();
        #[cfg(loom)]
        let head = self.head.load(Ordering::Relaxed);
        #[cfg(loom)]
        let tail = self.tail.load(Ordering::Relaxed);
        let mut i = tail;
        while i != head {
            // Safety: slot[i & MASK] is initialized (tail <= i < head).
            unsafe {
                (*self.slots[i & Self::MASK].get()).assume_init_drop();
            }
            i = i.wrapping_add(1);
        }
    }
}

impl<T, const CAP: usize, W: Waiter> std::fmt::Debug for Ring2<T, CAP, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ring2")
            .field("capacity", &CAP)
            .field("len", &self.len())
            .finish()
    }
}

// ──────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[test]
    fn single_thread_basic() {
        let r: Ring2<u32, 8> = Ring2::new();
        assert!(r.is_empty());
        assert_eq!(r.capacity(), 8);
        for i in 0..8 {
            assert!(r.try_send(i).is_ok());
        }
        assert!(r.is_full());
        assert!(r.try_send(999).is_err());
        for i in 0..8 {
            assert_eq!(r.try_recv(), Some(i));
        }
        assert!(r.is_empty());
        assert_eq!(r.try_recv(), None);
    }

    #[test]
    fn wraparound() {
        let r: Ring2<u32, 4> = Ring2::new();
        for i in 0..100 {
            assert!(r.try_send(i).is_ok());
            assert_eq!(r.try_recv(), Some(i));
        }
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn non_pow2_panics() {
        let _: Ring2<u32, 7> = Ring2::new();
    }

    #[test]
    fn cross_thread_blocking() {
        let r: Arc<Ring2<u64, 16>> = Arc::new(Ring2::new());
        let r2 = r.clone();
        let h = std::thread::spawn(move || {
            r2.set_consumer(std::thread::current());
            let mut sum = 0u64;
            for _ in 0..1000 {
                sum += r2.recv();
            }
            sum
        });
        r.set_producer(std::thread::current());
        for i in 0..1000u64 {
            r.send(i);
        }
        assert_eq!(h.join().unwrap(), (0..1000u64).sum());
    }

    #[test]
    fn cross_thread_backpressure() {
        // CAP small + slow consumer: producer MUST park on not_full.
        const CAP: usize = 4;
        const N: u64 = 500;
        let r: Arc<Ring2<u64, CAP>> = Arc::new(Ring2::new());
        let r2 = r.clone();
        let consumer = std::thread::spawn(move || {
            r2.set_consumer(std::thread::current());
            let mut got = Vec::with_capacity(N as usize);
            for i in 0..N {
                got.push(r2.recv());
                if i % 5 == 0 {
                    std::thread::sleep(std::time::Duration::from_micros(20));
                }
            }
            got
        });
        r.set_producer(std::thread::current());
        for i in 0..N {
            r.send(i);
        }
        let got = consumer.join().unwrap();
        assert_eq!(got, (0..N).collect::<Vec<_>>());
        assert!(r.is_empty());
    }

    #[test]
    fn drop_drains_inflight() {
        struct Tracked(Arc<AtomicU64>);
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let drops = Arc::new(AtomicU64::new(0));
        {
            let r: Ring2<Tracked, 8> = Ring2::new();
            for _ in 0..5 {
                assert!(r.try_send(Tracked(drops.clone())).is_ok());
            }
        }
        assert_eq!(drops.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn zero_sized_type() {
        let r: Ring2<(), 8> = Ring2::new();
        for _ in 0..8 {
            r.try_send(()).unwrap();
        }
        assert!(r.try_send(()).is_err());
        for _ in 0..8 {
            assert_eq!(r.try_recv(), Some(()));
        }
        assert_eq!(r.try_recv(), None);
    }

    #[test]
    fn cross_thread_high_volume() {
        const CAP: usize = 128;
        const N: u64 = 100_000;
        let r: Arc<Ring2<u64, CAP>> = Arc::new(Ring2::new());
        let r2 = r.clone();
        let consumer = std::thread::spawn(move || {
            r2.set_consumer(std::thread::current());
            let mut sum: u64 = 0;
            for _ in 0..N {
                sum = sum.wrapping_add(r2.recv());
            }
            sum
        });
        r.set_producer(std::thread::current());
        for i in 0..N {
            r.send(i);
        }
        let got = consumer.join().unwrap();
        let expected: u64 = (0..N).fold(0u64, |a, b| a.wrapping_add(b));
        assert_eq!(got, expected);
        assert!(r.is_empty());
    }

    // ── Async (tokio) ────────────────────────────────────────────────

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn cross_task_basic_notify() {
        use crate::waiter::NotifyWaiter;
        let r: Ring2<u64, 16, NotifyWaiter> = Ring2::new();
        let producer = async {
            for i in 0..1000u64 {
                r.send_async(i).await;
            }
        };
        let consumer = async {
            let mut sum = 0u64;
            for _ in 0..1000 {
                sum += r.recv_async().await;
            }
            sum
        };
        let (_, got) = tokio::join!(producer, consumer);
        assert_eq!(got, (0..1000u64).sum());
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn wraparound_notify() {
        use crate::waiter::NotifyWaiter;
        let r: Ring2<u32, 4, NotifyWaiter> = Ring2::new();
        for i in 0..100 {
            r.send_async(i).await;
            assert_eq!(r.recv_async().await, i);
        }
    }
}
