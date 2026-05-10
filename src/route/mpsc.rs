//! M:1 multi-producer / single-consumer bounded channel.
//!
//! Per-producer SPSC mini-rings + consumer-side scan for wakeup. Producers
//! never execute a `LOCK`-prefixed RMW on the send path: the only atomic
//! they touch outside their own ring is the consumer's waiter wake-check
//! (a single Relaxed load in the common "consumer running" case).
//!
//! ## Hot paths
//!
//! Producer `try_send`:
//!   - load this producer's `head` (Relaxed) and `tail` (Acquire);
//!   - if full → return `Err(value)`;
//!   - write slot, `head.store(Release)`;
//!   - `consumer_waiter.wake()` — internal Relaxed parked-flag check.
//!
//! Consumer `try_recv`:
//!   - for `p in 0..M`: load (head, tail) of `ring[p]`; if non-empty, take
//!     one item and `producer_waiters[p].wake()` (only relevant under
//!     backpressure).
//!
//! Consumer `recv` park path goes through `W::wait_until(predicate)` which
//! does the Dekker recheck internally.
//!
//! ## Why this layout (vs Vyukov-style shared queue)
//!
//! Crossbeam's `channel::bounded` serialises every send through a single
//! `LOCK fetch_add` on a shared `tail`. With M producers that line bounces
//! M times per send. Here every producer owns its own ring head/tail —
//! zero coherence traffic between producers — and the consumer pays an
//! O(M) scan per drain pass, amortised across whatever burst the rings
//! hold (typically dozens to hundreds of items per pass).
//!
//! ## When to reach for `Mpsc`
//!
//! - True M:1 fan-in with anonymous producers.
//! - When the consumer is a dedicated drain thread that calls `recv` /
//!   `recv_batch` in a tight loop.
//! - For M:N fan-in, use [`super::mpmc::Mpmc`] instead.

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::route::hub::Shutdown;
use crate::waiter::{BlockingWaiter, ParkWaiter, Waiter};

/// Maximum number of producers per channel.
pub const MAX_MPSC_PRODUCERS: usize = 255;

const CACHE_LINE: usize = 64;

// ─── Per-producer mini-Ring (SPSC) ────────────────────────────────────────

/// Cache-line-padded SPSC ring shared between one producer and the consumer.
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

// ─── Shared inner state ───────────────────────────────────────────────────

struct MpscInner<T: Send, const RING_CAP: usize, W: Waiter> {
    rings: Box<[PRing<T, RING_CAP>]>,
    /// Consumer waiter — wake on publish, park on drain miss.
    consumer_waiter: W,
    /// Per-producer backpressure waiters: producer parks here when its
    /// ring is full; consumer wakes after draining the producer's ring.
    producer_waiters: Box<[W]>,
    shutdown: AtomicBool,
    m: usize,
    /// Next free ring index for `MpscProducer::clone()` to claim. Always
    /// loaded with `AcqRel` `fetch_add` on clone — never touched on the
    /// send/recv hot path. In non-cloneable mode (`Mpsc::new`) this starts
    /// at `m`, so `clone()` always trips the bounds assertion.
    next_free_idx: AtomicUsize,
}

unsafe impl<T: Send, const RING_CAP: usize, W: Waiter> Sync for MpscInner<T, RING_CAP, W> {}
unsafe impl<T: Send, const RING_CAP: usize, W: Waiter> Send for MpscInner<T, RING_CAP, W> {}

impl<T: Send, const RING_CAP: usize, W: Waiter> MpscInner<T, RING_CAP, W> {
    /// `start_idx` is the value `next_free_idx` is initialised to. For the
    /// non-cloneable [`Mpsc::new`] path it equals `m` (clones blocked); for
    /// [`Mpsc::new_cloneable`] it is `1` (first sender at idx 0, next clone
    /// claims idx 1).
    fn new(m: usize, start_idx: usize) -> Self {
        assert!(
            RING_CAP > 0 && RING_CAP.is_power_of_two(),
            "RING_CAP must be a power of two ≥ 1"
        );
        let rings: Vec<PRing<T, RING_CAP>> =
            (0..m).map(|_| PRing::new()).collect();
        let producer_waiters: Vec<W> = (0..m).map(|_| W::default()).collect();
        Self {
            rings: rings.into_boxed_slice(),
            consumer_waiter: W::default(),
            producer_waiters: producer_waiters.into_boxed_slice(),
            shutdown: AtomicBool::new(false),
            m,
            next_free_idx: AtomicUsize::new(start_idx),
        }
    }

    /// `true` iff at least one ring has a published-but-not-consumed item.
    /// O(M).
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

    /// Drain every ring at least once, looping until a full pass finds zero
    /// new items. Caller invariant: only the unique consumer must call this
    /// (enforced at the type level by `MpscConsumer` being `!Sync`).
    #[inline]
    fn drain_all<F: FnMut(T)>(&self, f: &mut F) -> usize {
        let m = self.m;
        let mut count: usize = 0;
        loop {
            let mut progress = false;
            for p in 0..m {
                let ring = &self.rings[p];
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
                self.producer_waiters[p].wake();
            }
            if !progress { return count; }
        }
    }
}

impl<T: Send, const RING_CAP: usize, W: Waiter> Drop for MpscInner<T, RING_CAP, W> {
    fn drop(&mut self) {
        for p in 0..self.m {
            let ring = &self.rings[p];
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

// ─── Public facade ────────────────────────────────────────────────────────

/// M:1 bounded channel. Each producer owns an SPSC ring of `RING_CAP`
/// slots; the single consumer drains every ring via a scan.
pub struct Mpsc<T: Send, const RING_CAP: usize = 64, W: Waiter = ParkWaiter>(
    PhantomData<(T, W)>,
);

impl<T: Send + 'static, const RING_CAP: usize, W: Waiter + 'static>
    Mpsc<T, RING_CAP, W>
{
    /// Build an `Mpsc` with `m` producers and 1 consumer.
    ///
    /// Returns `(producers, consumer, shutdown)`. The consumer is returned
    /// by value (not `Vec`) — there is exactly one. Producers returned from
    /// this constructor are **not cloneable**; for a cloneable single-handle
    /// API see [`new_cloneable`](Self::new_cloneable).
    ///
    /// # Panics
    /// - `m == 0`
    /// - `m > MAX_MPSC_PRODUCERS`
    /// - `RING_CAP` not a power of two ≥ 1
    pub fn new(
        m: usize,
    ) -> (
        Vec<MpscProducer<T, RING_CAP, W>>,
        MpscConsumer<T, RING_CAP, W>,
        MpscShutdown<T, RING_CAP, W>,
    ) {
        assert!(m > 0, "Mpsc::new: m must be > 0");
        assert!(
            m <= MAX_MPSC_PRODUCERS,
            "Mpsc::new: m must be <= {MAX_MPSC_PRODUCERS}"
        );
        // start_idx = m → all rings handed out → clone() trips the bound.
        let inner = Arc::new(MpscInner::<T, RING_CAP, W>::new(m, m));
        let producers: Vec<MpscProducer<T, RING_CAP, W>> = (0..m)
            .map(|p| MpscProducer {
                inner: inner.clone(),
                my_idx: p,
                _not_sync: PhantomData,
            })
            .collect();
        let consumer = MpscConsumer {
            inner: inner.clone(),
            _not_sync: PhantomData,
        };
        let shutdown = MpscShutdown { inner };
        (producers, consumer, shutdown)
    }

    /// Build a **cloneable** `Mpsc` with up to `max_producers` ring slots.
    ///
    /// Returns a single producer handle (idx 0) plus the consumer and
    /// shutdown handles. Each [`Clone`] of the producer atomically claims
    /// a fresh ring slot from the pool until `max_producers` is reached;
    /// further clones panic. Memory for all `max_producers` rings is
    /// allocated upfront.
    ///
    /// **Hot path is identical** to [`new`](Self::new): `try_send` /
    /// `try_recv` never touch `next_free_idx`. Only `clone()` does, with a
    /// single `AcqRel` `fetch_add`.
    ///
    /// # Panics
    /// - `max_producers == 0`
    /// - `max_producers > MAX_MPSC_PRODUCERS`
    /// - `RING_CAP` not a power of two ≥ 1
    pub fn new_cloneable(
        max_producers: usize,
    ) -> (
        MpscProducer<T, RING_CAP, W>,
        MpscConsumer<T, RING_CAP, W>,
        MpscShutdown<T, RING_CAP, W>,
    ) {
        assert!(max_producers > 0, "Mpsc::new_cloneable: max_producers must be > 0");
        assert!(
            max_producers <= MAX_MPSC_PRODUCERS,
            "Mpsc::new_cloneable: max_producers must be <= {MAX_MPSC_PRODUCERS}"
        );
        // start_idx = 1 → first sender is idx 0, next clone claims idx 1.
        let inner = Arc::new(MpscInner::<T, RING_CAP, W>::new(max_producers, 1));
        let sender = MpscProducer {
            inner: inner.clone(),
            my_idx: 0,
            _not_sync: PhantomData,
        };
        let consumer = MpscConsumer {
            inner: inner.clone(),
            _not_sync: PhantomData,
        };
        let shutdown = MpscShutdown { inner };
        (sender, consumer, shutdown)
    }
}

// ─── Producer handle ──────────────────────────────────────────────────────

pub struct MpscProducer<T: Send, const RING_CAP: usize = 64, W: Waiter = ParkWaiter> {
    inner: Arc<MpscInner<T, RING_CAP, W>>,
    my_idx: usize,
    _not_sync: PhantomData<Cell<()>>,
}

/// Clone claims a fresh ring slot from the pool. Only valid for producers
/// minted via [`Mpsc::new_cloneable`]; producers from [`Mpsc::new`] panic on
/// clone (the pool is already fully handed out).
///
/// **Cost**: one `AcqRel` `fetch_add` on `next_free_idx` and an `Arc` clone.
/// The send/recv hot path is untouched — `next_free_idx` is never read on
/// `try_send` / `try_recv`.
///
/// The new producer owns its own ring (SPSC contract preserved). The new
/// thread that drives the clone must call [`bind`](MpscProducer::bind) once
/// before its first blocking `send` (sync waiter only).
impl<T: Send, const RING_CAP: usize, W: Waiter> Clone for MpscProducer<T, RING_CAP, W> {
    fn clone(&self) -> Self {
        let idx = self.inner.next_free_idx.fetch_add(1, Ordering::AcqRel);
        assert!(
            idx < self.inner.m,
            "MpscProducer::clone: pool exhausted (max {}). Use Mpsc::new_cloneable with a larger capacity.",
            self.inner.m
        );
        Self {
            inner: self.inner.clone(),
            my_idx: idx,
            _not_sync: PhantomData,
        }
    }
}

impl<T: Send, const RING_CAP: usize, W: Waiter> MpscProducer<T, RING_CAP, W> {
    #[inline]
    pub fn index(&self) -> usize { self.my_idx }

    /// Register the current thread as this producer's worker. Must be
    /// called by the producer thread before it can be parked on
    /// backpressure.
    #[inline]
    pub fn bind(&self) {
        self.inner.producer_waiters[self.my_idx]
            .set_worker(std::thread::current());
    }

    #[inline]
    pub fn has_room(&self) -> bool {
        let ring = &self.inner.rings[self.my_idx];
        let h = ring.head.load(Ordering::Relaxed);
        let t = ring.tail.load(Ordering::Acquire);
        !PRing::<T, RING_CAP>::is_full(h, t)
    }

    #[inline]
    pub const fn capacity(&self) -> usize { RING_CAP }

    #[inline]
    pub fn available(&self) -> usize {
        let ring = &self.inner.rings[self.my_idx];
        let h = ring.head.load(Ordering::Relaxed);
        let t = ring.tail.load(Ordering::Acquire);
        let used = h.wrapping_sub(t);
        RING_CAP.saturating_sub(used)
    }

    #[inline]
    pub fn pending(&self) -> usize {
        let ring = &self.inner.rings[self.my_idx];
        let h = ring.head.load(Ordering::Acquire);
        let t = ring.tail.load(Ordering::Relaxed);
        h.wrapping_sub(t)
    }

    /// Non-blocking send. Hot path: 1 Relaxed load + 1 Acquire load + 1
    /// Release store + 1 Relaxed wake-check. **Zero `LOCK`-prefixed RMW.**
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), T> {
        let ring = &self.inner.rings[self.my_idx];
        let h = ring.head.load(Ordering::Relaxed);
        let t = ring.tail.load(Ordering::Acquire);
        if PRing::<T, RING_CAP>::is_full(h, t) {
            return Err(value);
        }
        // SAFETY: SPSC — this producer is the only writer to `ring.slots`,
        // and `head` has not been advanced yet so the consumer cannot read
        // this slot.
        unsafe {
            (*ring.slots[h & PRing::<T, RING_CAP>::MASK].get()).write(value);
        }
        ring.head.store(h.wrapping_add(1), Ordering::Release);
        self.inner.consumer_waiter.wake();
        Ok(())
    }

    /// Bulk send: drains up to `min(items.len(), available)` from `items`
    /// into the ring, with one `head.store(Release)` and one consumer
    /// wake-check at the end. Returns the number of items consumed from
    /// `items`.
    pub fn try_send_batch(&self, items: &mut Vec<T>) -> usize {
        if items.is_empty() { return 0; }
        let ring = &self.inner.rings[self.my_idx];
        let h0 = ring.head.load(Ordering::Relaxed);
        let t = ring.tail.load(Ordering::Acquire);
        let used = h0.wrapping_sub(t);
        if used >= RING_CAP { return 0; }
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
        self.inner.consumer_waiter.wake();
        take
    }
}

impl<T: Send, const RING_CAP: usize, W: BlockingWaiter> MpscProducer<T, RING_CAP, W> {
    /// Blocking send. Parks on this producer's backpressure waiter if the
    /// ring is full; returns silently on shutdown without delivering.
    #[inline]
    pub fn send(&self, mut value: T) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            self.inner.producer_waiters[self.my_idx]
                .wait_until(|| self.has_room()
                    || self.inner.shutdown.load(Ordering::Acquire));
            if self.inner.shutdown.load(Ordering::Acquire) {
                return;
            }
        }
    }
}

// ── Async send (AsyncWaiter only) ────────────────────────────────────────

#[cfg(feature = "tokio")]
impl<T: Send + 'static, const RING_CAP: usize, W: crate::waiter::AsyncWaiter + 'static>
    MpscProducer<T, RING_CAP, W>
{
    /// Async send with backpressure. Awaits until the ring has room, then
    /// enqueues. Returns silently on shutdown without delivering.
    ///
    /// This is the async sibling of [`send`](MpscProducer::send). The
    /// returned future borrows only `Sync` subfields (`Arc<MpscInner>`)
    /// across the await point — `MpscProducer` itself is `!Sync`, so we
    /// inline the try-send logic using direct field access rather than
    /// calling `&self.try_send()` which would capture the non-Send `&self`.
    pub fn send_async<'a>(
        &'a self, value: T,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        let inner = &*self.inner;
        let my_idx = self.my_idx;
        Box::pin(async move {
            loop {
                // Inline try_send: only touch Sync fields through `inner`.
                let ring = &inner.rings[my_idx];
                let h = ring.head.load(Ordering::Relaxed);
                let t = ring.tail.load(Ordering::Acquire);
                if !PRing::<T, RING_CAP>::is_full(h, t) {
                    // SAFETY: SPSC — this producer is the only writer.
                    unsafe {
                        (*ring.slots[h & PRing::<T, RING_CAP>::MASK].get()).write(value);
                    }
                    ring.head.store(h.wrapping_add(1), Ordering::Release);
                    inner.consumer_waiter.wake();
                    return;
                }
                if inner.shutdown.load(Ordering::Acquire) {
                    return;
                }
                let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> =
                    Box::pin(inner.producer_waiters[my_idx].wait_until(move || {
                        let ring = &inner.rings[my_idx];
                        let h = ring.head.load(Ordering::Relaxed);
                        let t = ring.tail.load(Ordering::Acquire);
                        !PRing::<T, RING_CAP>::is_full(h, t)
                            || inner.shutdown.load(Ordering::Acquire)
                    }));
                fut.await;
            }
        })
    }
}

// ─── Consumer handle ──────────────────────────────────────────────────────

pub struct MpscConsumer<T: Send, const RING_CAP: usize = 64, W: Waiter = ParkWaiter> {
    inner: Arc<MpscInner<T, RING_CAP, W>>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send, const RING_CAP: usize, W: Waiter> MpscConsumer<T, RING_CAP, W> {
    /// Register the consumer thread. Must be called by the consumer thread
    /// itself before any producer publishes.
    pub fn bind(&self) {
        self.inner.consumer_waiter.set_worker(std::thread::current());
    }

    #[inline]
    pub const fn capacity_per_producer(&self) -> usize { RING_CAP }

    #[inline]
    pub fn total_capacity(&self) -> usize { self.inner.m * RING_CAP }

    #[inline]
    pub fn pending(&self) -> usize {
        let m = self.inner.m;
        let mut total = 0;
        for p in 0..m {
            let ring = &self.inner.rings[p];
            let h = ring.head.load(Ordering::Acquire);
            let t = ring.tail.load(Ordering::Relaxed);
            total += h.wrapping_sub(t);
        }
        total
    }

    #[inline]
    pub fn has_pending(&self) -> bool {
        self.inner.any_ring_has_work()
    }

    /// Scan all M rings, return the first item found. Round-robin order;
    /// fairness across rings is best-effort, not guaranteed.
    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        let m = self.inner.m;
        for p in 0..m {
            let ring = &self.inner.rings[p];
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

    /// Non-blocking batch drain. Returns the number of items drained on
    /// this pass (zero means "nothing right now", not shutdown).
    pub fn try_recv_batch<F: FnMut(T)>(&self, mut f: F) -> usize {
        self.drain_all(&mut f)
    }

    /// Drain every ring at least once. Loops until a full pass finds zero
    /// new items — covers the case where draining ring `p` lets producer
    /// `p-1` (already passed) commit more work.
    #[inline]
    fn drain_all<F: FnMut(T)>(&self, f: &mut F) -> usize {
        self.inner.drain_all(f)
    }
}

// ─── Async receive (AsyncWaiter only) ────────────────────────────────────

#[cfg(feature = "tokio")]
impl<T: Send + 'static, const RING_CAP: usize, W: crate::waiter::AsyncWaiter + 'static>
    MpscConsumer<T, RING_CAP, W>
{
    /// Async receive. Yields one item, awaiting a `Notify` wake when every
    /// ring is empty. Returns `Err(Shutdown)` after shutdown is signalled
    /// and all rings are drained.
    ///
    /// Returns a boxed future bound to `&self` to sidestep the RPITIT
    /// lifetime inference limitation (rust-lang/rust#100013) — same fix
    /// pattern as `OneShotAsync::recv_async` and `PipeAsync::recv_async`.
    pub fn recv_async<'a>(
        &'a mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, Shutdown>> + Send + 'a>>
    {
        Box::pin(async move {
            loop {
                if let Some(v) = self.try_recv() {
                    return Ok(v);
                }
                if self.inner.shutdown.load(Ordering::Acquire) {
                    return Err(Shutdown);
                }
                // Box the wait_until future to detach its `'a` borrow
                // before the outer async block's auto-trait inference
                // runs.
                let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> =
                    Box::pin(self.inner.consumer_waiter.wait_until(|| {
                        self.inner.any_ring_has_work()
                            || self.inner.shutdown.load(Ordering::Acquire)
                    }));
                fut.await;
            }
        })
    }
}

// ─── Spawn-safe async (NotifyWaiter specialization, zero-box) ────────────

#[cfg(feature = "tokio")]
impl<T: Send, const RING_CAP: usize> MpscProducer<T, RING_CAP, crate::waiter::NotifyWaiter> {
    /// Spawn-safe async send — zero heap allocation.
    ///
    /// Specialized for [`NotifyWaiter`]: uses `Notify::notified()` directly
    /// instead of the trait's RPITIT `wait_until`, producing a concrete
    /// `Send` future without boxing. ~10× faster than the generic boxed
    /// [`send_async`](MpscProducer::send_async) under backpressure.
    #[inline]
    pub fn send_async_send<'a>(
        &'a self, value: T,
    ) -> impl std::future::Future<Output = ()> + Send + 'a {
        let inner = &*self.inner;
        let my_idx = self.my_idx;
        async move {
            let value = value;
            loop {
                // Build notified() BEFORE checking — lost-notify prevention.
                let notified = inner.producer_waiters[my_idx].inner.notified();
                let ring = &inner.rings[my_idx];
                let h = ring.head.load(Ordering::Relaxed);
                let t = ring.tail.load(Ordering::Acquire);
                if !PRing::<T, RING_CAP>::is_full(h, t) {
                    // SAFETY: SPSC — this producer is the only writer.
                    unsafe {
                        (*ring.slots[h & PRing::<T, RING_CAP>::MASK].get()).write(value);
                    }
                    ring.head.store(h.wrapping_add(1), Ordering::Release);
                    inner.consumer_waiter.wake();
                    return;
                }
                if inner.shutdown.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        }
    }
}

#[cfg(feature = "tokio")]
impl<T: Send, const RING_CAP: usize> MpscConsumer<T, RING_CAP, crate::waiter::NotifyWaiter> {
    /// Spawn-safe async receive — zero heap allocation.
    ///
    /// Specialized for [`NotifyWaiter`]: uses `Notify::notified()` directly,
    /// producing a concrete `Send` future without boxing.
    #[inline]
    pub fn recv_async_send<'a>(
        &'a self,
    ) -> impl std::future::Future<Output = Result<T, Shutdown>> + Send + 'a {
        let inner = &*self.inner;
        async move {
            loop {
                // Build notified() BEFORE checking — lost-notify prevention.
                let notified = inner.consumer_waiter.inner.notified();
                // Scan all rings.
                for p in 0..inner.m {
                    let ring = &inner.rings[p];
                    let t = ring.tail.load(Ordering::Relaxed);
                    let h = ring.head.load(Ordering::Acquire);
                    if t != h {
                        let v = unsafe {
                            (*ring.slots[t & PRing::<T, RING_CAP>::MASK].get())
                                .assume_init_read()
                        };
                        ring.tail.store(t.wrapping_add(1), Ordering::Release);
                        inner.producer_waiters[p].wake();
                        return Ok(v);
                    }
                }
                if inner.shutdown.load(Ordering::Acquire) {
                    return Err(Shutdown);
                }
                notified.await;
            }
        }
    }

    /// Spawn-safe async batch drain — zero heap allocation.
    ///
    /// Drains every ring at least once on each wake-up cycle; invokes `f`
    /// on every item drained. Returns `Ok(count)` after at least one item
    /// was delivered, or `Err(Shutdown)` if shutdown fired with empty rings.
    ///
    /// **Why this exists**: `recv_async_send` returns one item per `await`,
    /// paying ~70-90 ns per item in async context (Notify polling, future
    /// state machine, runtime poll). `recv_batch_async_send` amortises that
    /// cost across N items per await — typical N = dozens to thousands when
    /// producers run hot. Match for the sync `recv_batch` 14× speedup.
    ///
    /// `F` must be `Send + 'a` for the returned future to be `Send`.
    #[inline]
    pub fn recv_batch_async_send<'a, F: FnMut(T) + Send + 'a>(
        &'a self,
        mut f: F,
    ) -> impl std::future::Future<Output = Result<usize, Shutdown>> + Send + 'a {
        let inner = &*self.inner;
        async move {
            loop {
                // Build notified() BEFORE checking — lost-notify prevention.
                let notified = inner.consumer_waiter.inner.notified();
                let count = inner.drain_all(&mut f);
                if count > 0 { return Ok(count); }
                if inner.shutdown.load(Ordering::Acquire) {
                    return Err(Shutdown);
                }
                notified.await;
            }
        }
    }
}

impl<T: Send, const RING_CAP: usize, W: BlockingWaiter> MpscConsumer<T, RING_CAP, W> {
    /// Blocking receive. Drains one item, parking when every ring is
    /// empty. Returns `Err(Shutdown)` after shutdown is signalled and all
    /// rings are drained.
    pub fn recv(&self) -> Result<T, Shutdown> {
        loop {
            if let Some(v) = self.try_recv() { return Ok(v); }
            if self.inner.shutdown.load(Ordering::Acquire) {
                return Err(Shutdown);
            }
            self.inner.consumer_waiter.wait_until(|| {
                self.inner.any_ring_has_work()
                    || self.inner.shutdown.load(Ordering::Acquire)
            });
        }
    }

    /// Drain at least one full pass and invoke `f` on every item drained.
    /// Blocks (parks) when no work is found and the channel is alive.
    pub fn recv_batch<F: FnMut(T)>(&self, mut f: F) -> Result<usize, Shutdown> {
        loop {
            let count = self.drain_all(&mut f);
            if count > 0 { return Ok(count); }
            if self.inner.shutdown.load(Ordering::Acquire) {
                return Err(Shutdown);
            }
            self.inner.consumer_waiter.wait_until(|| {
                self.inner.any_ring_has_work()
                    || self.inner.shutdown.load(Ordering::Acquire)
            });
        }
    }
}

// ─── Shutdown handle ──────────────────────────────────────────────────────

pub struct MpscShutdown<T: Send, const RING_CAP: usize = 64, W: Waiter = ParkWaiter> {
    inner: Arc<MpscInner<T, RING_CAP, W>>,
}

impl<T: Send, const RING_CAP: usize, W: Waiter> Clone for MpscShutdown<T, RING_CAP, W> {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}

impl<T: Send, const RING_CAP: usize, W: Waiter> MpscShutdown<T, RING_CAP, W> {
    /// Mark the channel shut down and wake every parked endpoint
    /// (consumer + all producers blocked on backpressure).
    #[inline]
    pub fn signal(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.consumer_waiter.wake();
        for w in self.inner.producer_waiters.iter() { w.wake(); }
    }

    #[inline]
    pub fn is_signaled(&self) -> bool {
        self.inner.shutdown.load(Ordering::Acquire)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn single_producer_roundtrip() {
        let (mut ps, c, _sd) = Mpsc::<u64>::new(1);
        let p = ps.remove(0);

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
    fn mpsc_exactly_once_delivery() {
        const M: usize = 4;
        const PER: u64 = 500;

        let (ps, c, sd) = Mpsc::<u64>::new(M);
        let sd2 = sd.clone();

        let sum_expected: u64 = (0..M as u64)
            .map(|i| (0..PER).map(|k| i * 10_000 + k).sum::<u64>())
            .sum();
        let got_sum = Arc::new(AtomicUsize::new(0));
        let got_count = Arc::new(AtomicUsize::new(0));

        let barrier = Arc::new(Barrier::new(M + 1));

        let consumer_h = {
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
        };

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
        consumer_h.join().unwrap();

        assert_eq!(got_count.load(Ordering::Relaxed), (M as u64 * PER) as usize);
        assert_eq!(got_sum.load(Ordering::Relaxed) as u64, sum_expected);
    }

    #[test]
    fn try_send_full_returns_err() {
        let (mut ps, _c, _sd) = Mpsc::<u64, 1>::new(1);
        let p = ps.remove(0);
        assert!(p.try_send(1).is_ok());
        assert_eq!(p.try_send(2), Err(2));
    }

    #[test]
    fn drop_drains_inflight() {
        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let (mut ps, _c, _sd) = Mpsc::<Tracked>::new(2);
            let p0 = ps.remove(0);
            let p1 = ps.remove(0);
            p0.try_send(Tracked(drops.clone())).ok().unwrap();
            p1.try_send(Tracked(drops.clone())).ok().unwrap();
        }
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn shutdown_wakes_consumer() {
        let (_ps, c, sd) = Mpsc::<u64>::new(2);
        let h = thread::spawn(move || { c.bind(); c.recv() });
        thread::sleep(Duration::from_millis(30));
        sd.signal();
        assert_eq!(h.join().unwrap(), Err(Shutdown));
    }

    #[test]
    fn send_dropped_value_is_destructed_not_leaked() {
        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }
        let drops = Arc::new(AtomicUsize::new(0));

        let (mut ps, c, sd) = Mpsc::<Tracked, 1>::new(1);
        let p = ps.remove(0);
        p.try_send(Tracked(drops.clone())).ok().unwrap();

        let drops2 = drops.clone();
        let h = thread::spawn(move || {
            p.bind();
            p.send(Tracked(drops2));
        });
        thread::sleep(Duration::from_millis(50));
        sd.signal();
        h.join().unwrap();

        assert_eq!(drops.load(Ordering::Relaxed), 1,
            "send() must drop the orphaned value, not leak it");

        drop(sd);
        drop(c);
        assert_eq!(drops.load(Ordering::Relaxed), 2,
            "ring drain on Drop must destruct the in-flight value");
    }

    #[test]
    fn shutdown_drains_published_items_first() {
        let (mut ps, c, sd) = Mpsc::<u64, 16>::new(1);
        let p = ps.remove(0);
        for i in 0..10u64 { p.try_send(i).unwrap(); }

        sd.signal();

        let h = thread::spawn(move || {
            c.bind();
            let mut got = Vec::new();
            loop {
                match c.recv() {
                    Ok(v) => got.push(v),
                    Err(Shutdown) => break,
                }
            }
            got
        });
        let got = h.join().unwrap();
        assert_eq!(got, (0..10).collect::<Vec<u64>>(),
            "items published before shutdown must be delivered");
    }

    #[test]
    fn high_producer_count_above_64_works() {
        const M: usize = 100;
        let (mut ps, c, _sd) = Mpsc::<u32>::new(M);
        c.bind();

        let producers: Vec<_> = ps.drain(..).collect();
        let handles: Vec<_> = producers.into_iter().enumerate().map(|(i, p)| {
            std::thread::spawn(move || {
                p.bind();
                for v in 0..50u32 { p.send((i as u32) * 1000 + v); }
            })
        }).collect();

        let mut got = 0usize;
        let total = M * 50;
        let mut sum = 0u64;
        while got < total {
            c.recv_batch(|v| { sum += v as u64; got += 1; }).unwrap();
        }
        for h in handles { h.join().unwrap(); }

        let mut expected = 0u64;
        for i in 0..M {
            for v in 0..50u32 { expected += ((i as u32) * 1000 + v) as u64; }
        }
        assert_eq!(sum, expected);
        assert_eq!(got, total);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn send_async_basic_roundtrip() {
        use crate::waiter::NotifyWaiter;
        let (mut ps, mut c, sd) =
            Mpsc::<u64, 4, NotifyWaiter>::new(2);
        let p0 = ps.remove(0);
        let p1 = ps.remove(0);

        let producer = async move {
            for k in 0..100u64 { p0.send_async(k).await; }
            for k in 100..200u64 { p1.send_async(k).await; }
        };
        let consumer = async move {
            let mut sum = 0u64;
            for _ in 0..200 {
                sum += c.recv_async().await.unwrap();
            }
            sum
        };
        let (_, got) = tokio::join!(producer, consumer);
        assert_eq!(got, (0..200u64).sum());
        sd.signal();
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn send_async_backpressure() {
        use crate::waiter::NotifyWaiter;
        // RING_CAP = 2: forces backpressure quickly.
        let (mut ps, mut c, sd) =
            Mpsc::<u64, 2, NotifyWaiter>::new(1);
        let p = ps.remove(0);

        let producer = async move {
            for k in 0..50u64 { p.send_async(k).await; }
        };
        let consumer = async move {
            let mut got = Vec::new();
            for _ in 0..50 {
                got.push(c.recv_async().await.unwrap());
            }
            got
        };
        let (_, got) = tokio::join!(producer, consumer);
        assert_eq!(got, (0..50u64).collect::<Vec<_>>());
        sd.signal();
    }

    // ── Cloneable producer (Mpsc::new_cloneable) ────────────────────────────

    #[test]
    fn cloneable_single_sender_roundtrip() {
        let (sender, c, _sd) = Mpsc::<u64>::new_cloneable(4);
        let h = thread::spawn(move || {
            c.bind();
            c.recv().unwrap()
        });
        thread::sleep(Duration::from_millis(10));
        sender.bind();
        sender.send(42);
        assert_eq!(h.join().unwrap(), 42);
    }

    #[test]
    fn cloneable_clones_use_disjoint_rings() {
        let (s0, _c, _sd) = Mpsc::<u64, 4>::new_cloneable(4);
        let s1 = s0.clone();
        let s2 = s0.clone();
        let s3 = s0.clone();
        // Each clone owns a distinct ring index.
        assert_eq!(s0.index(), 0);
        assert_eq!(s1.index(), 1);
        assert_eq!(s2.index(), 2);
        assert_eq!(s3.index(), 3);
    }

    #[test]
    #[should_panic(expected = "pool exhausted")]
    fn cloneable_pool_exhaustion_panics() {
        let (s0, _c, _sd) = Mpsc::<u64, 4>::new_cloneable(2);
        let _s1 = s0.clone();   // ok — idx 1
        let _s2 = s0.clone();   // panic — pool of 2 already handed out
    }

    #[test]
    #[should_panic(expected = "pool exhausted")]
    fn non_cloneable_clone_always_panics() {
        let (mut ps, _c, _sd) = Mpsc::<u64>::new(2);
        let p = ps.remove(0);
        let _q = p.clone();   // panic — Mpsc::new mints non-cloneable producers
    }

    #[test]
    fn cloneable_clones_indices_are_dense_and_unique() {
        // MpscProducer is !Sync by design — clone on the source thread, move
        // each clone into its own worker. Fanning out 16 senders is the
        // typical pattern.
        const N: usize = 16;
        let (s0, _c, _sd) = Mpsc::<u64>::new_cloneable(N);
        let mut senders: Vec<MpscProducer<u64>> =
            (0..N - 1).map(|_| s0.clone()).collect();
        senders.insert(0, s0);

        let mut indices: Vec<usize> = senders.iter().map(|s| s.index()).collect();
        indices.sort();
        assert_eq!(indices, (0..N).collect::<Vec<_>>(),
            "every clone must claim a unique idx in [0, N)");
    }

    // ── Cloneable + NotifyWaiter (async / tokio) ──────────────────────────

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn cloneable_async_basic_roundtrip() {
        use crate::waiter::NotifyWaiter;
        // First sender (idx 0) + clone (idx 1). Both drive the same Mpsc
        // through the NotifyWaiter async path.
        let (s0, c, sd) = Mpsc::<u64, 4, NotifyWaiter>::new_cloneable(2);
        let s1 = s0.clone();
        assert_eq!(s0.index(), 0);
        assert_eq!(s1.index(), 1);

        let producer = async move {
            for k in 0..100u64 { s0.send_async(k).await; }
            for k in 100..200u64 { s1.send_async(k).await; }
        };
        let consumer = async move {
            let mut sum = 0u64;
            for _ in 0..200 {
                sum += c.recv_async_send().await.unwrap();
            }
            sum
        };
        let (_, got) = tokio::join!(producer, consumer);
        assert_eq!(got, (0..200u64).sum());
        sd.signal();
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn cloneable_async_concurrent_producers_with_backpressure() {
        use crate::waiter::NotifyWaiter;
        // RING_CAP = 2 → tight backpressure across both rings.
        let (s0, c, sd) = Mpsc::<u64, 2, NotifyWaiter>::new_cloneable(2);
        let s1 = s0.clone();

        let producer = async move {
            let p0 = async move {
                for k in 0..50u64 { s0.send_async_send(k).await; }
            };
            let p1 = async move {
                for k in 50..100u64 { s1.send_async_send(k).await; }
            };
            tokio::join!(p0, p1);
        };
        let consumer = async move {
            let mut got = Vec::with_capacity(100);
            for _ in 0..100 {
                got.push(c.recv_async_send().await.unwrap());
            }
            got
        };
        let (_, got) = tokio::join!(producer, consumer);
        // Per-ring FIFO; total set is 0..100.
        let mut sorted = got.clone();
        sorted.sort();
        assert_eq!(sorted, (0..100u64).collect::<Vec<_>>());
        sd.signal();
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn recv_batch_async_drains_multiple_rings() {
        use crate::waiter::NotifyWaiter;
        let (s0, c, sd) = Mpsc::<u64, 8, NotifyWaiter>::new_cloneable(2);
        let s1 = s0.clone();

        let producer = async move {
            for k in 0..50u64 { s0.send_async_send(k).await; }
            for k in 50..100u64 { s1.send_async_send(k).await; }
        };
        let consumer = async move {
            let mut got: Vec<u64> = Vec::with_capacity(100);
            while got.len() < 100 {
                let _ = c
                    .recv_batch_async_send(|v| got.push(v))
                    .await
                    .unwrap();
            }
            got
        };
        let (_, got) = tokio::join!(producer, consumer);
        let mut sorted = got.clone();
        sorted.sort();
        assert_eq!(sorted, (0..100u64).collect::<Vec<_>>());
        sd.signal();
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn recv_batch_async_returns_shutdown_when_empty() {
        use crate::waiter::NotifyWaiter;
        let (_s0, c, sd) = Mpsc::<u64, 4, NotifyWaiter>::new_cloneable(1);
        // Signal shutdown without sending anything.
        sd.signal();
        let r = c.recv_batch_async_send(|_| {}).await;
        assert_eq!(r, Err(Shutdown));
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn cloneable_async_pool_exhaustion_panics() {
        use crate::waiter::NotifyWaiter;
        let (s0, _c, _sd) = Mpsc::<u64, 4, NotifyWaiter>::new_cloneable(2);
        let _s1 = s0.clone();   // ok — idx 1
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _s2 = s0.clone();   // panic — pool of 2 already handed out
        }));
        assert!(result.is_err(), "third clone must panic on exhausted pool");
    }

    #[test]
    fn cloneable_multiple_senders_deliver_all() {
        const N: usize = 4;
        const PER: u64 = 250;

        let (s0, c, sd) = Mpsc::<u64>::new_cloneable(N);
        let sd2 = sd.clone();

        let got_count = Arc::new(AtomicUsize::new(0));
        let got_sum = Arc::new(AtomicUsize::new(0));
        let consumer_h = {
            let got_sum = got_sum.clone();
            let got_count = got_count.clone();
            thread::spawn(move || {
                c.bind();
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
        };

        // Build clones on the main thread (Clone takes &self, MpscProducer is !Sync).
        let mut senders: Vec<MpscProducer<u64>> =
            (0..N - 1).map(|_| s0.clone()).collect();
        senders.insert(0, s0);

        let producers: Vec<_> = senders.into_iter().enumerate().map(|(i, s)| {
            thread::spawn(move || {
                s.bind();
                for k in 0..PER { s.send(i as u64 * 10_000 + k); }
            })
        }).collect();
        for h in producers { h.join().unwrap(); }

        thread::sleep(Duration::from_millis(20));
        sd2.signal();
        consumer_h.join().unwrap();

        let expected_sum: u64 = (0..N as u64)
            .map(|i| (0..PER).map(|k| i * 10_000 + k).sum::<u64>())
            .sum();
        assert_eq!(got_count.load(Ordering::Relaxed), (N as u64 * PER) as usize);
        assert_eq!(got_sum.load(Ordering::Relaxed) as u64, expected_sum);
    }
}
