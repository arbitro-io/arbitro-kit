//! M:N multi-producer / multi-consumer bounded channel, sharded.
//!
//! [`Mpmc<T, RING_CAP, W>`] wires `M` producers to `N` consumers through `N`
//! independent shards. Each `(producer, shard)` pair owns a dedicated
//! **SPSC mini-ring of `RING_CAP` slots**, not a single slot — so a bursting
//! producer can enqueue up to `RING_CAP` items before stalling, and the
//! consumer can drain the whole ring in one park/unpark cycle.
//!
//! ## Multi-runtime
//!
//! `Mpmc` is generic over the [`Waiter`](crate::waiter::Waiter) backend.
//! Default `W = ParkWaiter` keeps the OS-thread `park`/`unpark` semantics
//! the type has always shipped with. With `W = NotifyWaiter` (feature
//! `tokio`) the same struct exposes async `recv_async`/`send_async`.
//!
//! ## Topology
//!
//! ```text
//!   producer 0 ──┐                  shard 0 ──► consumer 0
//!   producer 1 ──┤  adaptive ──►    shard 1 ──► consumer 1
//!     ⋮          │  routing         ⋮           ⋮
//!   producer M-1 ┘                  shard N-1 ─► consumer N-1
//!
//!   shard s
//!   ├── rings[0..M]: PRing                  (each is SPSC, RING_CAP slots)
//!   ├── consumer_waiter: W                  (drives consumer wait/wake)
//!   └── drained by consumer s
//! ```
//!
//! Each `(producer p, shard s)` pair owns `shards[s].rings[p]`, a classic
//! SPSC ring with `head` / `tail` cursors. The producer is the sole writer
//! of `head`, the consumer is the sole writer of `tail`.
//!
//! ## Hot-path cost per message
//!
//! Producer `try_send(v)` — **zero `LOCK`-prefixed RMW**:
//! 1. Scan shards from cursor, pick first whose `rings[p]` isn't full
//!    (one `tail.load(Acquire)` per shard scanned, no CAS).
//! 2. Write `slots[head & MASK] = v`.
//! 3. `head.store(h+1, Release)` — publishes slot.
//! 4. `consumer_waiter.wake()` — `Waiter`-internal coalescing skips the
//!    syscall when the consumer is not parked.
//!
//! ## Limits
//!
//! - `M ≤ 255` producers.
//! - `N ≥ 1`, no upper bound (runtime-sized).
//! - `M == 0` or `N == 0` is rejected.
//! - `RING_CAP` must be a power of two ≥ 1.

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::route::hub::Shutdown;
use crate::waiter::{AsyncWaiter, BlockingWaiter, ParkWaiter, Waiter};

/// Maximum number of producers in an [`Mpmc`].
pub const MAX_MPMC_PRODUCERS: usize = 255;

// ─── Per-(producer, shard) mini-Ring (SPSC) ───────────────────────────────

/// Cache line size on x86_64 / aarch64. Used to separate `head` and `tail`
/// onto distinct cache lines so the producer's `head.store(Release)` does
/// not invalidate the consumer's cached `tail` (and vice versa).
const CACHE_LINE: usize = 64;

/// SPSC ring owned by one producer on one shard. `RING_CAP` slots, indexed
/// by `head & MASK` / `tail & MASK`. `head` only advances via the producer,
/// `tail` only via the consumer.
#[repr(C)]
struct PRing<T: Send, const RING_CAP: usize> {
    head: AtomicUsize,
    _pad_head: [u8; CACHE_LINE - core::mem::size_of::<AtomicUsize>()],
    tail: AtomicUsize,
    _pad_tail: [u8; CACHE_LINE - core::mem::size_of::<AtomicUsize>()],
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

impl<T: Send, const RING_CAP: usize> PRing<T, RING_CAP> {
    const MASK: usize = RING_CAP - 1;

    fn new() -> Self {
        let slots: Vec<UnsafeCell<MaybeUninit<T>>> =
            (0..RING_CAP).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect();
        Self {
            head: AtomicUsize::new(0),
            _pad_head: [0u8; CACHE_LINE - core::mem::size_of::<AtomicUsize>()],
            tail: AtomicUsize::new(0),
            _pad_tail: [0u8; CACHE_LINE - core::mem::size_of::<AtomicUsize>()],
            slots: slots.into_boxed_slice(),
        }
    }

    #[inline]
    fn is_full(h: usize, t: usize) -> bool {
        h.wrapping_sub(t) >= RING_CAP
    }
}

// ─── Shard ────────────────────────────────────────────────────────────────

struct Shard<T: Send, const RING_CAP: usize, W: Waiter> {
    /// `rings[p]` owned by producer `p`.
    rings: Box<[PRing<T, RING_CAP>]>,
    /// Drives the consumer's wait/wake protocol. For `W = ParkWaiter`,
    /// `wake()` performs the same Dekker-safe load + conditional unpark
    /// the previous hand-rolled `consumer_parked` flag implemented; for
    /// async waiters it is a `notify_one` on the underlying primitive.
    consumer_waiter: W,
    /// Number of producers (== `MpmcInner::m`). Cached here to avoid an
    /// extra indirection on the hot scan path.
    m: usize,
}

// Safety: same SPSC reasoning as MpmcInner — slot access is serialized
// by the (producer, shard) pairing and the head/tail cursors. The
// `consumer_waiter: W` is `Send + Sync` per the trait bound.
unsafe impl<T: Send, const RING_CAP: usize, W: Waiter> Send for Shard<T, RING_CAP, W> {}
unsafe impl<T: Send, const RING_CAP: usize, W: Waiter> Sync for Shard<T, RING_CAP, W> {}

impl<T: Send, const RING_CAP: usize, W: Waiter> Shard<T, RING_CAP, W> {
    /// Wake this shard's consumer if it's parked. Coalescing is the
    /// `Waiter` impl's responsibility (`ParkWaiter` skips the syscall
    /// when the consumer is not parked; `NotifyWaiter` always pays the
    /// runtime enqueue, but that is what tokio is for).
    #[inline]
    fn maybe_wake_consumer(&self) {
        self.consumer_waiter.wake();
    }

    /// `true` iff at least one ring in this shard has a published-but-not-
    /// consumed item. O(M).
    #[inline]
    fn any_ring_has_work(&self) -> bool {
        for p in 0..self.m {
            let ring = &self.rings[p];
            let h = ring.head.load(Ordering::Acquire);
            let t = ring.tail.load(Ordering::Relaxed);
            if h != t { return true; }
        }
        false
    }
}

// ─── Shared inner state ────────────────────────────────────────────────────

struct MpmcInner<T: Send, const RING_CAP: usize, W: Waiter> {
    shards: Box<[Shard<T, RING_CAP, W>]>,
    /// Per-producer backpressure waiter. The consumer wakes producer `p`
    /// after advancing `tail` on one of its rings.
    producer_waiters: Box<[W]>,
    shutdown: AtomicBool,
    m: usize,
    n: usize,
}

unsafe impl<T: Send, const RING_CAP: usize, W: Waiter> Sync for MpmcInner<T, RING_CAP, W> {}
unsafe impl<T: Send, const RING_CAP: usize, W: Waiter> Send for MpmcInner<T, RING_CAP, W> {}

impl<T: Send, const RING_CAP: usize, W: Waiter> MpmcInner<T, RING_CAP, W> {
    fn new(m: usize, n: usize) -> Self {
        assert!(
            RING_CAP > 0 && RING_CAP.is_power_of_two(),
            "RING_CAP must be a power of two ≥ 1"
        );

        let mut shards_vec = Vec::with_capacity(n);
        for _ in 0..n {
            let rings: Vec<PRing<T, RING_CAP>> =
                (0..m).map(|_| PRing::new()).collect();
            shards_vec.push(Shard {
                rings: rings.into_boxed_slice(),
                consumer_waiter: W::default(),
                m,
            });
        }

        let producer_waiters: Vec<W> = (0..m).map(|_| W::default()).collect();

        Self {
            shards: shards_vec.into_boxed_slice(),
            producer_waiters: producer_waiters.into_boxed_slice(),
            shutdown: AtomicBool::new(false),
            m,
            n,
        }
    }
}

impl<T: Send, const RING_CAP: usize, W: Waiter> Drop for MpmcInner<T, RING_CAP, W> {
    fn drop(&mut self) {
        for shard in self.shards.iter() {
            for p in 0..self.m {
                let ring = &shard.rings[p];
                let h = ring.head.load(Ordering::Acquire);
                let mut t = ring.tail.load(Ordering::Acquire);
                while t != h {
                    unsafe {
                        (*ring.slots[t & PRing::<T, RING_CAP>::MASK].get())
                            .assume_init_drop();
                    }
                    t = t.wrapping_add(1);
                }
            }
        }
    }
}

// ─── Public facade ─────────────────────────────────────────────────────────

/// M:N bounded channel, sharded across `N` consumers. Each `(producer,
/// shard)` pair is an SPSC ring of `RING_CAP` slots. Generic over the
/// [`Waiter`] backend; defaults to `ParkWaiter` for OS-thread `park`/`unpark`.
pub struct Mpmc<T: Send, const RING_CAP: usize = 64, W: Waiter = ParkWaiter>(
    PhantomData<(T, W)>,
);

impl<T: Send + 'static, const RING_CAP: usize, W: Waiter + 'static> Mpmc<T, RING_CAP, W> {
    /// Build an `Mpmc` with `m` producers and `n` consumer shards.
    ///
    /// # Panics
    /// - `m == 0` or `n == 0`
    /// - `m > MAX_MPMC_PRODUCERS`
    /// - `RING_CAP` is not a power of two ≥ 1
    pub fn new(
        m: usize,
        n: usize,
    ) -> (
        Vec<MpmcProducer<T, RING_CAP, W>>,
        Vec<MpmcConsumer<T, RING_CAP, W>>,
        MpmcShutdown<T, RING_CAP, W>,
    ) {
        assert!(m > 0, "Mpmc::new: m must be > 0");
        assert!(n > 0, "Mpmc::new: n must be > 0");
        assert!(
            m <= MAX_MPMC_PRODUCERS,
            "Mpmc::new: m must be <= {MAX_MPMC_PRODUCERS}"
        );

        let inner = Arc::new(MpmcInner::<T, RING_CAP, W>::new(m, n));

        let producers: Vec<MpmcProducer<T, RING_CAP, W>> = (0..m)
            .map(|p| MpmcProducer {
                inner: inner.clone(),
                my_idx: p,
                cursor: Cell::new((p % n) as u32),
                _not_sync: PhantomData,
            })
            .collect();

        let consumers: Vec<MpmcConsumer<T, RING_CAP, W>> = (0..n)
            .map(|s| MpmcConsumer {
                inner: inner.clone(),
                shard_idx: s,
                _not_sync: PhantomData,
            })
            .collect();

        let shutdown = MpmcShutdown { inner };
        (producers, consumers, shutdown)
    }
}

// ─── Producer handle ───────────────────────────────────────────────────────

/// One of the `M` producer handles returned by [`Mpmc::new`].
pub struct MpmcProducer<T: Send, const RING_CAP: usize = 64, W: Waiter = ParkWaiter> {
    inner: Arc<MpmcInner<T, RING_CAP, W>>,
    my_idx: usize,
    cursor: Cell<u32>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send, const RING_CAP: usize, W: Waiter> MpmcProducer<T, RING_CAP, W> {
    /// Numeric index of this producer (`0..m`).
    #[inline]
    pub fn index(&self) -> usize { self.my_idx }

    /// Register this thread as the producer's backpressure waiter. Must
    /// be called from the thread that will invoke [`send`](Self::send)
    /// before any send on a potentially-saturated `Mpmc`. No-op for async
    /// waiter backends.
    #[inline]
    pub fn bind(&self) {
        self.inner.producer_waiters[self.my_idx]
            .set_worker(std::thread::current());
    }

    /// `true` if at least one shard's ring for this producer has room.
    #[inline]
    pub fn has_idle_shard(&self) -> bool {
        for shard in self.inner.shards.iter() {
            let ring = &shard.rings[self.my_idx];
            let h = ring.head.load(Ordering::Relaxed);
            let t = ring.tail.load(Ordering::Acquire);
            if !PRing::<T, RING_CAP>::is_full(h, t) {
                return true;
            }
        }
        false
    }

    // ── Capacity introspection (snapshot, non-consistent) ────────────────

    #[inline]
    pub const fn capacity_per_shard(&self) -> usize { RING_CAP }

    #[inline]
    pub fn total_capacity(&self) -> usize { self.inner.n * RING_CAP }

    #[inline]
    pub fn available_in_shard(&self, s: usize) -> usize {
        let ring = &self.inner.shards[s].rings[self.my_idx];
        let h = ring.head.load(Ordering::Relaxed);
        let t = ring.tail.load(Ordering::Acquire);
        let used = h.wrapping_sub(t);
        RING_CAP.saturating_sub(used)
    }

    #[inline]
    pub fn available(&self) -> usize {
        let mut total = 0;
        for s in 0..self.inner.n {
            let ring = &self.inner.shards[s].rings[self.my_idx];
            let h = ring.head.load(Ordering::Relaxed);
            let t = ring.tail.load(Ordering::Acquire);
            let used = h.wrapping_sub(t);
            total += RING_CAP.saturating_sub(used);
        }
        total
    }

    #[inline]
    pub fn pending_in_shard(&self, s: usize) -> usize {
        let ring = &self.inner.shards[s].rings[self.my_idx];
        let h = ring.head.load(Ordering::Acquire);
        let t = ring.tail.load(Ordering::Relaxed);
        h.wrapping_sub(t)
    }

    /// Non-blocking send.
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), T> {
        let n = self.inner.n;
        let start = (self.cursor.get() as usize) % n;
        for k in 0..n {
            let s = (start + k) % n;
            let shard = &self.inner.shards[s];
            let ring = &shard.rings[self.my_idx];
            let h = ring.head.load(Ordering::Relaxed);
            let t = ring.tail.load(Ordering::Acquire);
            if PRing::<T, RING_CAP>::is_full(h, t) { continue; }
            unsafe {
                (*ring.slots[h & PRing::<T, RING_CAP>::MASK].get()).write(value);
            }
            ring.head.store(h.wrapping_add(1), Ordering::Release);
            shard.maybe_wake_consumer();
            self.cursor.set(((s + 1) % n) as u32);
            return Ok(());
        }
        Err(value)
    }

    /// Batch send. Drains as many items as fit into a single ring.
    pub fn try_send_batch(&self, items: &mut Vec<T>) -> usize {
        if items.is_empty() { return 0; }
        let n = self.inner.n;
        let start = (self.cursor.get() as usize) % n;
        for k in 0..n {
            let s = (start + k) % n;
            let shard = &self.inner.shards[s];
            let ring = &shard.rings[self.my_idx];
            let h0 = ring.head.load(Ordering::Relaxed);
            let t = ring.tail.load(Ordering::Acquire);
            let used = h0.wrapping_sub(t);
            if used >= RING_CAP { continue; }
            let avail = RING_CAP - used;
            let take = items.len().min(avail);
            let mut h = h0;
            for v in items.drain(..take) {
                unsafe {
                    (*ring.slots[h & PRing::<T, RING_CAP>::MASK].get()).write(v);
                }
                h = h.wrapping_add(1);
            }
            ring.head.store(h, Ordering::Release);
            shard.maybe_wake_consumer();
            self.cursor.set(((s + 1) % n) as u32);
            return take;
        }
        0
    }
}

impl<T: Send, const RING_CAP: usize, W: BlockingWaiter> MpmcProducer<T, RING_CAP, W> {
    /// Blocking send. Parks on the producer's backpressure waiter if every
    /// ring for this producer is full.
    #[inline]
    pub fn send(&self, mut value: T) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            self.inner.producer_waiters[self.my_idx]
                .wait_until(|| self.has_idle_shard());
        }
    }
}

impl<T: Send, const RING_CAP: usize, W: AsyncWaiter> MpmcProducer<T, RING_CAP, W> {
    /// Async send. Awaits when every ring for this producer is full.
    pub async fn send_async(&self, mut value: T) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            // Borrow Sync references explicitly — `MpmcProducer` is `!Sync`
            // (it owns a `Cell<u32>` cursor), so the closure cannot borrow
            // `self` directly and stay `Send`.
            let inner = &*self.inner;
            let my_idx = self.my_idx;
            inner.producer_waiters[my_idx]
                .wait_until(|| {
                    for shard in inner.shards.iter() {
                        let ring = &shard.rings[my_idx];
                        let h = ring.head.load(Ordering::Relaxed);
                        let t = ring.tail.load(Ordering::Acquire);
                        if !PRing::<T, RING_CAP>::is_full(h, t) {
                            return true;
                        }
                    }
                    false
                })
                .await;
        }
    }
}

// ─── Consumer handle ───────────────────────────────────────────────────────

/// One of the `N` consumer handles returned by [`Mpmc::new`]. Owns exactly
/// one shard.
pub struct MpmcConsumer<T: Send, const RING_CAP: usize = 64, W: Waiter = ParkWaiter> {
    inner: Arc<MpmcInner<T, RING_CAP, W>>,
    shard_idx: usize,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send, const RING_CAP: usize, W: Waiter> MpmcConsumer<T, RING_CAP, W> {
    /// Numeric index of this consumer's shard (`0..n`).
    #[inline]
    pub fn shard(&self) -> usize { self.shard_idx }

    /// Register this thread as the shard's drain worker. Must be called
    /// before the first blocking `recv` / `recv_batch`. No-op for async
    /// waiter backends.
    #[inline]
    pub fn bind(&self) {
        self.inner.shards[self.shard_idx]
            .consumer_waiter
            .set_worker(std::thread::current());
    }

    // ── Capacity introspection (snapshot, non-consistent) ────────────────

    #[inline]
    pub const fn capacity_per_producer(&self) -> usize { RING_CAP }

    #[inline]
    pub fn total_capacity(&self) -> usize { self.inner.m * RING_CAP }

    #[inline]
    pub fn pending(&self) -> usize {
        let shard = &self.inner.shards[self.shard_idx];
        let m = self.inner.m;
        let mut total = 0;
        for p in 0..m {
            let ring = &shard.rings[p];
            let h = ring.head.load(Ordering::Acquire);
            let t = ring.tail.load(Ordering::Relaxed);
            total += h.wrapping_sub(t);
        }
        total
    }

    #[inline]
    pub fn available(&self) -> usize {
        let shard = &self.inner.shards[self.shard_idx];
        let m = self.inner.m;
        let mut total = 0;
        for p in 0..m {
            let ring = &shard.rings[p];
            let h = ring.head.load(Ordering::Relaxed);
            let t = ring.tail.load(Ordering::Acquire);
            let used = h.wrapping_sub(t);
            total += RING_CAP.saturating_sub(used);
        }
        total
    }

    #[inline]
    pub fn pending_from(&self, p: usize) -> usize {
        let ring = &self.inner.shards[self.shard_idx].rings[p];
        let h = ring.head.load(Ordering::Acquire);
        let t = ring.tail.load(Ordering::Relaxed);
        h.wrapping_sub(t)
    }

    #[inline]
    pub fn has_pending(&self) -> bool {
        self.inner.shards[self.shard_idx].any_ring_has_work()
    }

    /// Non-blocking single-item take.
    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        let shard = &self.inner.shards[self.shard_idx];
        let m = self.inner.m;
        for p in 0..m {
            let ring = &shard.rings[p];
            let t = ring.tail.load(Ordering::Relaxed);
            let h = ring.head.load(Ordering::Acquire);
            if t == h { continue; }
            let v = unsafe {
                (*ring.slots[t & PRing::<T, RING_CAP>::MASK].get())
                    .assume_init_read()
            };
            ring.tail.store(t.wrapping_add(1), Ordering::Release);
            self.inner.producer_waiters[p].wake();
            return Some(v);
        }
        None
    }

    /// Non-blocking drain of every ready ring. Returns count.
    pub fn try_recv_batch<F: FnMut(T)>(&self, mut f: F) -> usize {
        self.drain_all(&mut f)
    }

    /// Drain every ring on this shard at least once. Loops until a full
    /// pass finds zero new items.
    #[inline]
    fn drain_all<F: FnMut(T)>(&self, f: &mut F) -> usize {
        let shard = &self.inner.shards[self.shard_idx];
        let m = self.inner.m;
        let mut count: usize = 0;
        loop {
            let mut progress = false;
            for p in 0..m {
                let ring = &shard.rings[p];
                let mut t = ring.tail.load(Ordering::Relaxed);
                let h = ring.head.load(Ordering::Acquire);
                if t == h { continue; }
                while t != h {
                    let v = unsafe {
                        (*ring.slots[t & PRing::<T, RING_CAP>::MASK].get())
                            .assume_init_read()
                    };
                    t = t.wrapping_add(1);
                    f(v);
                    count += 1;
                    progress = true;
                }
                ring.tail.store(t, Ordering::Release);
                self.inner.producer_waiters[p].wake();
            }
            if !progress { return count; }
        }
    }
}

impl<T: Send, const RING_CAP: usize, W: BlockingWaiter> MpmcConsumer<T, RING_CAP, W> {
    /// Blocking single-item take. Parks until any producer publishes to
    /// one of this shard's rings or shutdown fires.
    pub fn recv(&self) -> Result<T, Shutdown> {
        loop {
            if let Some(v) = self.try_recv() { return Ok(v); }
            if self.inner.shutdown.load(Ordering::Acquire) {
                return Err(Shutdown);
            }
            let shard = &self.inner.shards[self.shard_idx];
            shard.consumer_waiter.wait_until(|| {
                shard.any_ring_has_work()
                    || self.inner.shutdown.load(Ordering::Acquire)
            });
        }
    }

    /// Drain every currently-ready ring on this shard in one pass. Parks
    /// once if nothing is ready. Returns the number of messages delivered.
    pub fn recv_batch<F: FnMut(T)>(&self, mut f: F) -> Result<usize, Shutdown> {
        loop {
            let count = self.drain_all(&mut f);
            if count > 0 { return Ok(count); }
            if self.inner.shutdown.load(Ordering::Acquire) {
                return Err(Shutdown);
            }
            let shard = &self.inner.shards[self.shard_idx];
            shard.consumer_waiter.wait_until(|| {
                shard.any_ring_has_work()
                    || self.inner.shutdown.load(Ordering::Acquire)
            });
        }
    }
}

impl<T: Send, const RING_CAP: usize, W: AsyncWaiter> MpmcConsumer<T, RING_CAP, W> {
    /// Async single-item take.
    pub async fn recv_async(&self) -> Result<T, Shutdown> {
        loop {
            if let Some(v) = self.try_recv() { return Ok(v); }
            if self.inner.shutdown.load(Ordering::Acquire) {
                return Err(Shutdown);
            }
            let shard = &self.inner.shards[self.shard_idx];
            // Borrow only the shard + shutdown atomic (both Sync) — not
            // `self`, because `MpmcConsumer` is intentionally `!Sync`.
            let shutdown = &self.inner.shutdown;
            shard.consumer_waiter.wait_until(|| {
                shard.any_ring_has_work() || shutdown.load(Ordering::Acquire)
            }).await;
        }
    }
}

// ─── Shutdown handle ───────────────────────────────────────────────────────

pub struct MpmcShutdown<T: Send, const RING_CAP: usize = 64, W: Waiter = ParkWaiter> {
    inner: Arc<MpmcInner<T, RING_CAP, W>>,
}

impl<T: Send, const RING_CAP: usize, W: Waiter> Clone for MpmcShutdown<T, RING_CAP, W> {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}

impl<T: Send, const RING_CAP: usize, W: Waiter> MpmcShutdown<T, RING_CAP, W> {
    /// Flag as shutting down and wake every parked consumer + producer.
    /// Idempotent.
    #[inline]
    pub fn signal(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        for shard in self.inner.shards.iter() {
            shard.consumer_waiter.wake();
        }
        for w in self.inner.producer_waiters.iter() {
            w.wake();
        }
    }

    #[inline]
    pub fn is_signaled(&self) -> bool {
        self.inner.shutdown.load(Ordering::Acquire)
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[test]
    #[should_panic(expected = "Park::wait_until reached park path without set_worker")]
    fn recv_without_bind_panics() {
        let (_ps, mut cs, _sd) = Mpmc::<u64>::new(1, 1);
        let c = cs.remove(0);
        let _ = c.recv();
    }

    #[test]
    #[should_panic(expected = "Park::wait_until reached park path without set_worker")]
    fn recv_batch_without_bind_panics() {
        let (_ps, mut cs, _sd) = Mpmc::<u64>::new(1, 1);
        let c = cs.remove(0);
        let _ = c.recv_batch(|_| {});
    }

    #[test]
    fn single_shard_single_producer_roundtrip() {
        let (mut ps, mut cs, _sd) = Mpmc::<u64>::new(1, 1);
        let p = ps.remove(0);
        let c = cs.remove(0);

        let h = thread::spawn(move || {
            c.bind();
            c.recv().unwrap()
        });
        thread::sleep(Duration::from_millis(10));
        p.bind();
        p.send(42);
        assert_eq!(h.join().unwrap(), 42);
    }

    #[test]
    fn mpmc_exactly_once_delivery() {
        const M: usize = 4;
        const N: usize = 4;
        const PER: u64 = 500;

        let (ps, cs, sd) = Mpmc::<u64>::new(M, N);
        let sd2 = sd.clone();

        let sum_expected: u64 = (0..M as u64)
            .map(|i| (0..PER).map(|k| i * 10_000 + k).sum::<u64>())
            .sum();
        let got_sum = Arc::new(AtomicUsize::new(0));
        let got_count = Arc::new(AtomicUsize::new(0));

        let barrier = Arc::new(Barrier::new(M + N));

        let consumers: Vec<_> = cs.into_iter().map(|c| {
            let got_sum = got_sum.clone();
            let got_count = got_count.clone();
            let b = barrier.clone();
            thread::spawn(move || {
                c.bind();
                b.wait();
                loop {
                    match c.recv_batch(|v| {
                        got_sum.fetch_add(v as usize, Ordering::Relaxed);
                        got_count.fetch_add(1, Ordering::Relaxed);
                    }) {
                        Ok(_) => continue,
                        Err(Shutdown) => break,
                    }
                }
            })
        }).collect();

        let producers: Vec<_> = ps.into_iter().enumerate().map(|(i, p)| {
            let b = barrier.clone();
            thread::spawn(move || {
                p.bind();
                b.wait();
                for k in 0..PER { p.send(i as u64 * 10_000 + k); }
            })
        }).collect();

        for h in producers { h.join().unwrap(); }
        thread::sleep(Duration::from_millis(20));
        sd2.signal();
        for h in consumers { h.join().unwrap(); }

        assert_eq!(got_count.load(Ordering::Relaxed), (M as u64 * PER) as usize);
        assert_eq!(got_sum.load(Ordering::Relaxed) as u64, sum_expected);
    }

    #[test]
    fn multi_shard_no_deadlock() {
        const M: usize = 4;
        const N: usize = 4;
        const PER: u64 = 200;

        let (ps, cs, sd) = Mpmc::<u64>::new(M, N);
        let sd2 = sd.clone();
        let counts = Arc::new((0..N).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());

        let barrier = Arc::new(Barrier::new(M + N));

        let consumers: Vec<_> = cs.into_iter().enumerate().map(|(s, c)| {
            let counts = counts.clone();
            let b = barrier.clone();
            thread::spawn(move || {
                c.bind();
                b.wait();
                loop {
                    match c.recv_batch(|_| {
                        counts[s].fetch_add(1, Ordering::Relaxed);
                    }) {
                        Ok(_) => continue,
                        Err(Shutdown) => break,
                    }
                }
            })
        }).collect();

        let producers: Vec<_> = ps.into_iter().map(|p| {
            let b = barrier.clone();
            thread::spawn(move || {
                p.bind();
                b.wait();
                for k in 0..PER { p.send(k); }
            })
        }).collect();
        for h in producers { h.join().unwrap(); }

        thread::sleep(Duration::from_millis(20));
        sd2.signal();
        for h in consumers { h.join().unwrap(); }

        let total: usize = counts.iter().map(|c| c.load(Ordering::Relaxed)).sum();
        assert_eq!(total, M * PER as usize);
    }

    #[test]
    fn shutdown_wakes_all_parked_consumers() {
        let (_ps, cs, sd) = Mpmc::<u64>::new(2, 4);

        let consumers: Vec<_> = cs.into_iter().map(|c| {
            thread::spawn(move || {
                c.bind();
                c.recv()
            })
        }).collect();

        thread::sleep(Duration::from_millis(30));
        sd.signal();
        for h in consumers {
            assert_eq!(h.join().unwrap(), Err(Shutdown));
        }
        assert!(sd.is_signaled());
    }

    #[test]
    fn shutdown_drains_inflight() {
        let (mut ps, mut cs, sd) = Mpmc::<u64>::new(1, 1);
        let p = ps.remove(0);
        let c = cs.remove(0);

        p.try_send(99).unwrap();
        sd.signal();

        c.bind();
        let v = c.recv().unwrap();
        assert_eq!(v, 99);
        assert_eq!(c.recv(), Err(Shutdown));
    }

    #[test]
    fn try_send_err_when_all_shards_full() {
        let (mut ps, _cs, _sd) = Mpmc::<u64, 1>::new(1, 2);
        let p = ps.remove(0);
        assert!(p.try_send(1).is_ok());
        assert!(p.try_send(2).is_ok());
        assert_eq!(p.try_send(3), Err(3));
    }

    #[test]
    fn drop_drains_inflight() {
        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let (mut ps, _cs, _sd) = Mpmc::<Tracked>::new(2, 2);
            let p0 = ps.remove(0);
            let p1 = ps.remove(0);
            p0.try_send(Tracked(drops.clone())).ok().unwrap();
            p1.try_send(Tracked(drops.clone())).ok().unwrap();
        }
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    #[should_panic(expected = "m must be > 0")]
    fn rejects_zero_producers() {
        let _ = Mpmc::<u8>::new(0, 4);
    }

    #[test]
    #[should_panic(expected = "n must be > 0")]
    fn rejects_zero_consumers() {
        let _ = Mpmc::<u8>::new(4, 0);
    }

    #[test]
    #[should_panic(expected = "m must be <=")]
    fn rejects_too_many_producers() {
        let _ = Mpmc::<u8>::new(MAX_MPMC_PRODUCERS + 1, 1);
    }

    #[test]
    fn high_producer_count_above_64_works() {
        const M: usize = 100;
        let (mut ps, mut cs, _sd) = Mpmc::<u32>::new(M, 1);
        let consumer = cs.remove(0);
        consumer.bind();

        let producers: Vec<_> = ps.drain(..).collect();
        let handles: Vec<_> = producers.into_iter().enumerate().map(|(i, p)| {
            std::thread::spawn(move || {
                for v in 0..50u32 {
                    p.send((i as u32) * 1000 + v);
                }
            })
        }).collect();

        let mut got = 0usize;
        let total = M * 50;
        let mut sum = 0u64;
        while got < total {
            consumer.recv_batch(|v| { sum += v as u64; got += 1; }).unwrap();
        }
        for h in handles { h.join().unwrap(); }

        let mut expected = 0u64;
        for i in 0..M {
            for v in 0..50u32 { expected += ((i as u32) * 1000 + v) as u64; }
        }
        assert_eq!(sum, expected);
        assert_eq!(got, total);
    }

    #[test]
    fn capacity_introspection_reports_correct_state() {
        let (mut ps, mut cs, _sd) = Mpmc::<u32, 8>::new(2, 3);
        let p0 = ps.remove(0);
        let p1 = ps.remove(0);

        let c0 = cs.remove(0);
        let c1 = cs.remove(0);
        let c2 = cs.remove(0);

        assert_eq!(p0.capacity_per_shard(), 8);
        assert_eq!(p0.total_capacity(), 24);
        assert_eq!(p0.available(), 24);
        assert_eq!(p0.pending_in_shard(0), 0);

        assert_eq!(c0.capacity_per_producer(), 8);
        assert_eq!(c0.total_capacity(), 16);
        assert_eq!(c0.pending(), 0);
        assert_eq!(c0.available(), 16);
        assert_eq!(c0.has_pending(), false);

        for v in 0..5u32 { p0.try_send(v).unwrap(); }
        assert_eq!(p0.available(), 24 - 5);
        let pending_from_p0 = c0.pending_from(0) + c1.pending_from(0) + c2.pending_from(0);
        assert_eq!(pending_from_p0, 5);

        p1.try_send(1000).unwrap();
        assert_eq!(p1.available(), 24 - 1);

        let any_has = c0.has_pending() || c1.has_pending() || c2.has_pending();
        assert!(any_has);
    }

    #[test]
    fn box_ownership_zero_copy() {
        let (mut ps, mut cs, _sd) = Mpmc::<Box<Vec<u8>>>::new(1, 1);
        let p = ps.remove(0);
        let c = cs.remove(0);

        let payload = Box::new(vec![1u8, 2, 3, 4]);
        let ptr_before = payload.as_ptr() as usize;

        let h = thread::spawn(move || {
            c.bind();
            c.recv().unwrap()
        });
        thread::sleep(Duration::from_millis(10));
        p.bind();
        p.send(payload);

        let received = h.join().unwrap();
        assert_eq!(received.as_slice(), &[1, 2, 3, 4]);
        assert_eq!(received.as_ptr() as usize, ptr_before);
    }

    #[test]
    fn recv_batch_drains_multiple_producers_in_one_park() {
        let (mut ps, mut cs, _sd) = Mpmc::<u64>::new(2, 1);
        let p0 = ps.remove(0);
        let p1 = ps.remove(0);
        let c = cs.remove(0);

        p0.try_send(10).unwrap();
        p1.try_send(20).unwrap();

        c.bind();
        let mut got: Vec<u64> = Vec::new();
        let n = c.recv_batch(|v| got.push(v)).unwrap();
        assert_eq!(n, 2);
        got.sort();
        assert_eq!(got, vec![10, 20]);
    }

    #[test]
    fn cross_thread_high_volume() {
        const M: usize = 4;
        const N: usize = 2;
        const PER: u64 = 5_000;

        let (ps, cs, sd) = Mpmc::<u64>::new(M, N);
        let sd2 = sd.clone();
        let delivered = Arc::new(AtomicUsize::new(0));

        let barrier = Arc::new(Barrier::new(M + N));

        let consumers: Vec<_> = cs.into_iter().map(|c| {
            let delivered = delivered.clone();
            let b = barrier.clone();
            thread::spawn(move || {
                c.bind();
                b.wait();
                loop {
                    match c.recv_batch(|_v| {
                        delivered.fetch_add(1, Ordering::Relaxed);
                    }) {
                        Ok(_) => continue,
                        Err(Shutdown) => break,
                    }
                }
            })
        }).collect();

        let producers: Vec<_> = ps.into_iter().map(|p| {
            let b = barrier.clone();
            thread::spawn(move || {
                p.bind();
                b.wait();
                for k in 0..PER { p.send(k); }
            })
        }).collect();
        for h in producers { h.join().unwrap(); }

        thread::sleep(Duration::from_millis(50));
        sd2.signal();
        for h in consumers { h.join().unwrap(); }

        assert_eq!(delivered.load(Ordering::Relaxed), M * PER as usize);
    }

    #[test]
    fn try_send_batch_amortizes_multiple_items() {
        let (mut ps, mut cs, _sd) = Mpmc::<u64>::new(1, 1);
        let p = ps.remove(0);
        let c = cs.remove(0);

        let mut items: Vec<u64> = (0..20).collect();
        let n = p.try_send_batch(&mut items);
        assert_eq!(n, 20);
        assert!(items.is_empty());

        c.bind();
        let mut got: Vec<u64> = Vec::new();
        let k = c.recv_batch(|v| got.push(v)).unwrap();
        assert_eq!(k, 20);
        assert_eq!(got, (0..20).collect::<Vec<_>>());
    }

    // ── Async-mirror tests (W = NotifyWaiter, feature = "tokio") ──────

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn single_shard_roundtrip_notify() {
        use crate::waiter::NotifyWaiter;
        let (mut ps, mut cs, _sd) = Mpmc::<u64, 64, NotifyWaiter>::new(1, 1);
        let p = ps.remove(0);
        let c = cs.remove(0);

        let producer = async move {
            for k in 0..1000u64 { p.send_async(k).await; }
        };
        let consumer = async move {
            let mut sum = 0u64;
            for _ in 0..1000 {
                sum += c.recv_async().await.unwrap();
            }
            sum
        };
        let (_, got) = tokio::join!(producer, consumer);
        assert_eq!(got, (0..1000u64).sum());
    }
}
