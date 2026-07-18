//! M:1 bounded channel built on per-producer [`Ring<T, CAP, NoopWaiter>`].
//! The ring's internal `wake()` is a no-op; fan-in wakes go through Mpsc's own
//! shared waiter, called **unconditionally** on every push. The waiter's own
//! gate (NotifyWaiter armed-counter / ParkWaiter parked-flag) is the sound wake
//! elision; the former caller-level `should_notify_consumer` pre-gate had a
//! store-buffering lost-wake race and is no longer used (R1, see `try_send`).

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::route::Shutdown;
use crate::stream::{Consumer, Producer, Ring, TryRecvError, TrySendError};
use crate::waiter::{BlockingWaiter, NoopWaiter, ParkWaiter, Waiter};

/// Maximum producers per Mpsc channel.
pub const MAX_MPSC_PRODUCERS: usize = 255;

// ─── Shared state ─────────────────────────────────────────────────────────

struct Inner<W: Waiter> {
    fanin_waiter: W,
    producer_waiters: Vec<W>,
    live_producers: AtomicUsize,
    shutdown: AtomicBool,
    m: usize,
}

impl<W: Waiter> Inner<W> {
    #[inline]
    fn is_shutdown_signaled(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    #[inline]
    fn all_producers_gone(&self) -> bool {
        self.live_producers.load(Ordering::Acquire) == 0
    }
}

// ─── Public facade ────────────────────────────────────────────────────────

/// M:1 bounded channel built on split-handle SPSC `Ring`s.
pub struct Mpsc<T, const CAP: usize = 64, W: Waiter = ParkWaiter>(PhantomData<(T, W)>);

impl<T: Send + 'static, const CAP: usize, W: Waiter + 'static> Mpsc<T, CAP, W> {
    /// Build an Mpsc with `m` producers and 1 consumer.
    ///
    /// Returns `(producers, consumer, shutdown)`.
    ///
    /// # Panics
    /// - `m == 0`
    /// - `m > MAX_MPSC_PRODUCERS`
    /// - `CAP` not a power of two ≥ 1
    pub fn new(
        m: usize,
    ) -> (
        Vec<MpscProducer<T, CAP, W>>,
        MpscConsumer<T, CAP, W>,
        MpscShutdown<W>,
    ) {
        assert!(m > 0, "Mpsc::new: m must be > 0");
        assert!(
            m <= MAX_MPSC_PRODUCERS,
            "Mpsc::new: m must be <= {MAX_MPSC_PRODUCERS}"
        );

        // Build M rings; keep the producers separated from consumers.
        let mut producer_halves = Vec::with_capacity(m);
        let mut consumer_halves = Vec::with_capacity(m);
        for _ in 0..m {
            let (p, c) = Ring::<T, CAP, NoopWaiter>::new();
            producer_halves.push(p);
            consumer_halves.push(Some(c));
        }

        let producer_waiters: Vec<W> = (0..m).map(|_| W::default()).collect();

        let inner = Arc::new(Inner {
            fanin_waiter: W::default(),
            producer_waiters,
            live_producers: AtomicUsize::new(m),
            shutdown: AtomicBool::new(false),
            m,
        });

        let producers: Vec<MpscProducer<T, CAP, W>> = producer_halves
            .into_iter()
            .enumerate()
            .map(|(idx, ring_producer)| MpscProducer {
                ring_producer: Some(ring_producer),
                inner: inner.clone(),
                my_idx: idx,
                _not_sync: PhantomData,
            })
            .collect();

        let consumer = MpscConsumer {
            inner: inner.clone(),
            ring_consumers: consumer_halves,
            _not_sync: PhantomData,
        };
        let shutdown = MpscShutdown { inner };

        (producers, consumer, shutdown)
    }

    /// Like [`Mpsc::new`], but hands back a leasable [`MpscProducerPool`]
    /// instead of a static `Vec<MpscProducer>`, so producer slots can be
    /// recycled across short-lived connections instead of leaked.
    ///
    /// # Panics
    /// Same as [`Mpsc::new`].
    pub fn producer_pool(
        m: usize,
    ) -> (
        Arc<MpscProducerPool<T, CAP, W>>,
        MpscConsumer<T, CAP, W>,
        MpscShutdown<W>,
    ) {
        let (producers, consumer, shutdown) = Self::new(m);
        (Self::pool_from_producers(producers), consumer, shutdown)
    }

    /// Wrap an already-built `Vec<MpscProducer>` in a pool for dynamic reuse.
    ///
    /// # Panics
    /// - `producers` is empty
    /// - `producers.len() > MAX_MPSC_PRODUCERS`
    pub fn pool_from_producers(
        producers: Vec<MpscProducer<T, CAP, W>>,
    ) -> Arc<MpscProducerPool<T, CAP, W>> {
        let m = producers.len();
        assert!(m > 0, "Mpsc::pool_from_producers: producers must be non-empty");
        assert!(
            m <= MAX_MPSC_PRODUCERS,
            "Mpsc::pool_from_producers: m must be <= {MAX_MPSC_PRODUCERS}"
        );
        // Any producer's Inner Arc keeps the channel alive for the pool.
        let inner = producers[0].inner.clone();
        let slots = producers
            .into_iter()
            .map(|p| UnsafeCell::new(Some(p)))
            .collect();
        Arc::new(MpscProducerPool {
            slots,
            occupancy: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            m,
            _inner: inner,
        })
    }
}

// ─── Producer handle ──────────────────────────────────────────────────────

/// Producer handle for one ring within an [`Mpsc`]. `Send` but `!Sync`:
/// the SPSC contract for the underlying ring is compile-time enforced.
pub struct MpscProducer<T, const CAP: usize, W: Waiter> {
    /// `Option` so `Drop` can move the ring producer out to close it before
    /// decrementing the live count.
    ring_producer: Option<Producer<T, CAP, NoopWaiter>>,
    inner: Arc<Inner<W>>,
    my_idx: usize,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send, const CAP: usize, W: Waiter> MpscProducer<T, CAP, W> {
    /// Register the current thread as this producer's backpressure worker.
    /// Required for `send`'s park path with `W = ParkWaiter`; no-op for
    /// async waiters.
    #[inline]
    pub fn bind(&self) {
        self.inner.producer_waiters[self.my_idx].set_worker(std::thread::current());
    }

    #[inline]
    pub fn index(&self) -> usize {
        self.my_idx
    }

    #[inline]
    pub const fn capacity(&self) -> usize {
        CAP
    }

    /// Items this producer has published but the consumer has not drained
    /// yet. Snapshot; races with concurrent drain. One Acquire load on the
    /// consumer-owned tail line — cheap but not free; use for metrics, not
    /// in the hot path.
    #[inline]
    pub fn pending(&self) -> usize {
        self.ring_producer.as_ref().map(|p| p.len()).unwrap_or(0)
    }

    /// Slots free in this producer's ring. `capacity - pending()`.
    #[inline]
    pub fn available(&self) -> usize {
        CAP.saturating_sub(self.pending())
    }

    /// Non-blocking send. Returns the value back on full or shutdown.
    #[inline]
    pub fn try_send(&mut self, value: T) -> Result<(), T> {
        if self.inner.shutdown.load(Ordering::Acquire) {
            return Err(value);
        }
        let rp = self
            .ring_producer
            .as_mut()
            .expect("ring_producer only None during drop");
        match rp.try_send(value) {
            Ok(()) => {
                // R1: wake unconditionally. The fan-in waiter's OWN gate
                // (NotifyWaiter armed-counter / ParkWaiter parked-flag) is the
                // sound wake elision. The former `should_notify_consumer`
                // pre-gate was a caller-level Vyukov gate whose head-store →
                // tail-load had a fence on only one side (store-buffering race):
                // both sides could read stale simultaneously and skip the ONLY
                // wake, losing it permanently (self-sustaining, not self-healing).
                // On the production NotifyWaiter path this is also ~1.8x FASTER:
                // it drops the per-send Acquire load of the consumer's hot `tail`
                // line (cache-line ping-pong) for a load of the rarely-written
                // `armed` line. (On ParkWaiter it costs an mfence per send;
                // recover with batched sends if that config is ever hot.)
                self.inner.fanin_waiter.wake();
                Ok(())
            }
            Err(TrySendError::Full(v)) => Err(v),
            Err(TrySendError::Closed(v)) => Err(v),
        }
    }

    /// Bulk send — pushes up to `min(items.len(), available)` items via
    /// Ring's `try_send_bulk`. One Release store + at most one fan-in
    /// wake per call. Returns the number consumed from `items`.
    #[inline]
    pub fn try_send_bulk(&mut self, items: &mut Vec<T>) -> usize {
        if items.is_empty() || self.inner.shutdown.load(Ordering::Acquire) {
            return 0;
        }
        let rp = self
            .ring_producer
            .as_mut()
            .expect("ring_producer only None during drop");
        let n = rp.try_send_bulk(items);
        // R1: wake unconditionally when anything was pushed — the waiter's own
        // gate handles elision (see `try_send`). The old `should_notify_consumer`
        // pre-gate could skip and lose the only wake.
        if n > 0 {
            self.inner.fanin_waiter.wake();
        }
        n
    }
}

impl<T: Send, const CAP: usize, W: BlockingWaiter> MpscProducer<T, CAP, W> {
    /// Blocking send. Parks on this producer's backpressure waiter when
    /// the ring is full; returns silently on shutdown.
    #[inline]
    pub fn send(&mut self, value: T) {
        let mut value = value;
        loop {
            if self.inner.shutdown.load(Ordering::Acquire) {
                return;
            }
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            let idx = self.my_idx;
            let inner = &*self.inner;
            let rp_ptr: *const Option<Producer<T, CAP, NoopWaiter>> = &self.ring_producer;
            self.inner.producer_waiters[idx].wait_until(|| {
                if inner.shutdown.load(Ordering::Acquire) {
                    return true;
                }
                // SAFETY: single-threaded read — we hold `&mut self`.
                let rp = unsafe { &*rp_ptr };
                match rp.as_ref() {
                    Some(p) => !p.is_full(),
                    None => true,
                }
            });
        }
    }
}

impl<T, const CAP: usize, W: Waiter> Drop for MpscProducer<T, CAP, W> {
    fn drop(&mut self) {
        drop(self.ring_producer.take());
        let prev = self.inner.live_producers.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            self.inner.fanin_waiter.wake();
        }
    }
}

// ─── Producer pool ────────────────────────────────────────────────────────

/// 4*64 = 256 bits, sized to cover `MAX_MPSC_PRODUCERS` (255).
const BITMAP_WORDS: usize = 4;

/// Dynamic pool of [`MpscProducer`] slots leased out via
/// [`MpscProducerLease`]. Slot occupancy is a fixed 256-bit atomic bitmap,
/// so `acquire`/release cost is O(1) regardless of `m`. Built by
/// [`Mpsc::producer_pool`] or [`Mpsc::pool_from_producers`].
#[repr(align(64))] // isolate the bitmap's cache lines from the slot storage
pub struct MpscProducerPool<T, const CAP: usize, W: Waiter> {
    // slots[i] is Some(producer) iff occupancy bit i is 0.
    slots: Vec<UnsafeCell<Option<MpscProducer<T, CAP, W>>>>,
    // Bit i set = slot i currently leased. Word 0 = slots 0..64, etc.
    occupancy: [AtomicU64; BITMAP_WORDS],
    m: usize,
    // Keeps the channel alive independently of any single producer/consumer.
    _inner: Arc<Inner<W>>,
}

// SAFETY: interior access to each slot's UnsafeCell is guarded by the
// occupancy bit — holding the bit == holding &mut on that slot's
// Option<MpscProducer>, so concurrent Pool access across threads is sound.
unsafe impl<T: Send, const CAP: usize, W: Waiter> Sync for MpscProducerPool<T, CAP, W> {}

impl<T: Send, const CAP: usize, W: Waiter> MpscProducerPool<T, CAP, W> {
    /// Number of producer slots configured for this pool (== `m` at build).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.m
    }

    /// Number of slots currently NOT leased. O(4) — sums popcnt over the
    /// 4 occupancy words.
    pub fn available(&self) -> usize {
        let mut free = 0usize;
        for word_i in 0..BITMAP_WORDS {
            let word_min = word_i * 64;
            if word_min >= self.m {
                break;
            }
            let live_bits = ((word_i + 1) * 64).min(self.m) - word_min;
            let mask: u64 = if live_bits == 64 {
                !0u64
            } else {
                (1u64 << live_bits) - 1
            };
            let cur = self.occupancy[word_i].load(Ordering::Relaxed);
            free += ((!cur) & mask).count_ones() as usize;
        }
        free
    }

    /// Try to lease a producer. Returns `None` iff every slot is currently
    /// held. Cold path: one relaxed load per 64-bit word plus one AcqRel
    /// `fetch_or` retry loop on the word holding a free bit.
    ///
    /// The lease hands back the *same* underlying ring a previous holder of
    /// this slot used, so [`MpscProducer::index`] is not a stable per-item
    /// tag across lease cycles: items sent before and after a lease
    /// boundary share one ring and are delivered in that ring's publish
    /// order.
    pub fn acquire(self: &Arc<Self>) -> Option<MpscProducerLease<T, CAP, W>> {
        for word_i in 0..BITMAP_WORDS {
            let word_min = word_i * 64;
            if word_min >= self.m {
                break;
            }
            let live_bits = ((word_i + 1) * 64).min(self.m) - word_min;
            let mask: u64 = if live_bits == 64 {
                !0u64
            } else {
                (1u64 << live_bits) - 1
            };
            loop {
                let cur = self.occupancy[word_i].load(Ordering::Relaxed);
                let free = (!cur) & mask;
                if free == 0 {
                    break;
                }
                let bit = free.trailing_zeros() as u64;
                let mask_bit = 1u64 << bit;
                // AcqRel: the Acquire side synchronizes-with the previous
                // lease's Release fetch_and, giving happens-before on the
                // slot's UnsafeCell contents.
                let prev = self.occupancy[word_i].fetch_or(mask_bit, Ordering::AcqRel);
                if prev & mask_bit == 0 {
                    let slot = word_min + bit as usize;
                    // SAFETY: we just won the bit; every unleased slot holds
                    // a producer by construction/release invariant.
                    let producer = unsafe { (*self.slots[slot].get()).take().unwrap() };
                    return Some(MpscProducerLease {
                        pool: self.clone(),
                        slot: slot as u16,
                        producer: ManuallyDrop::new(producer),
                    });
                }
                // Lost the race to another acquirer — retry this word.
            }
        }
        None
    }
}

/// A [`MpscProducer`] leased from an [`MpscProducerPool`]. Derefs to
/// `MpscProducer` at zero cost; returns the producer to the pool on drop
/// instead of tearing down the underlying ring, so the slot can be reused
/// by the next lease.
pub struct MpscProducerLease<T, const CAP: usize, W: Waiter> {
    pool: Arc<MpscProducerPool<T, CAP, W>>,
    slot: u16,
    producer: ManuallyDrop<MpscProducer<T, CAP, W>>,
}

impl<T: Send, const CAP: usize, W: Waiter> std::ops::Deref for MpscProducerLease<T, CAP, W> {
    type Target = MpscProducer<T, CAP, W>;
    #[inline]
    fn deref(&self) -> &MpscProducer<T, CAP, W> {
        &self.producer
    }
}

impl<T: Send, const CAP: usize, W: Waiter> std::ops::DerefMut for MpscProducerLease<T, CAP, W> {
    #[inline]
    fn deref_mut(&mut self) -> &mut MpscProducer<T, CAP, W> {
        &mut self.producer
    }
}

impl<T, const CAP: usize, W: Waiter> Drop for MpscProducerLease<T, CAP, W> {
    fn drop(&mut self) {
        // SAFETY: this Drop impl is the only call site that takes from
        // this lease's ManuallyDrop, and it runs at most once.
        let producer = unsafe { ManuallyDrop::take(&mut self.producer) };
        let slot = self.slot as usize;
        // SAFETY: we still hold the occupancy bit here, so no other thread
        // can be touching this cell concurrently.
        unsafe {
            *self.pool.slots[slot].get() = Some(producer);
        }
        let word = slot / 64;
        let bit = slot % 64;
        // Release: pairs with acquire()'s AcqRel fetch_or so the next
        // holder observes the restored Option<MpscProducer>.
        self.pool.occupancy[word].fetch_and(!(1u64 << bit), Ordering::Release);
    }
}

// ─── Consumer handle ──────────────────────────────────────────────────────

/// Single-consumer handle. `Send + !Sync`.
pub struct MpscConsumer<T, const CAP: usize, W: Waiter> {
    inner: Arc<Inner<W>>,
    ring_consumers: Vec<Option<Consumer<T, CAP, NoopWaiter>>>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send, const CAP: usize, W: Waiter> MpscConsumer<T, CAP, W> {
    /// Register the current thread as the consumer for the fan-in
    /// waiter. Required before blocking `recv` when `W = ParkWaiter`.
    #[inline]
    pub fn bind(&self) {
        self.inner.fanin_waiter.set_worker(std::thread::current());
    }

    #[inline]
    pub const fn capacity_per_producer(&self) -> usize {
        CAP
    }

    #[inline]
    pub fn total_capacity(&self) -> usize {
        self.inner.m * CAP
    }

    /// Sum of items visible across all rings from this consumer's fresh
    /// perspective. O(M). Snapshot; races with concurrent producers. Use
    /// for metrics, not in the hot recv path.
    #[inline]
    pub fn pending(&self) -> usize {
        self.ring_consumers
            .iter()
            .flatten()
            .map(|c| c.len())
            .sum()
    }

    /// `true` iff at least one ring has an item. O(M) direct-scan.
    #[inline]
    fn any_ring_has_work(&self) -> bool {
        for c in self.ring_consumers.iter().flatten() {
            if !c.is_empty() {
                return true;
            }
        }
        false
    }

    /// `true` iff the channel is shut down AND all rings drained.
    #[inline]
    fn is_finished(&self) -> bool {
        self.inner.is_shutdown_signaled()
            || (self.inner.all_producers_gone() && !self.any_ring_has_work())
    }

    /// True iff no more items can ever be sent on this channel: shutdown
    /// signaled OR every producer permanently dropped. Rings may still
    /// hold buffered items — use `pending()` for that; this is distinct
    /// from `is_finished` (the recv termination condition). Non-mutating.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.inner.is_shutdown_signaled() || self.inner.all_producers_gone()
    }

    /// O(M) scan. Returns the first item found; wakes that ring's
    /// producer backpressure waiter.
    #[inline]
    pub fn try_recv(&mut self) -> Option<T> {
        for idx in 0..self.inner.m {
            if let Some(c) = self.ring_consumers[idx].as_mut() {
                match c.try_recv() {
                    Ok(v) => {
                        self.inner.producer_waiters[idx].wake();
                        return Some(v);
                    }
                    Err(TryRecvError::Empty) | Err(TryRecvError::Closed) => continue,
                }
            }
        }
        None
    }

    /// Drain every ring at least once via [`Consumer::drain`]. Loops until
    /// a full pass finds zero items. Returns the count.
    #[inline]
    pub fn try_recv_batch<F: FnMut(T)>(&mut self, mut f: F) -> usize {
        let mut total = 0;
        loop {
            let mut progress = false;
            for idx in 0..self.inner.m {
                if let Some(c) = self.ring_consumers[idx].as_mut() {
                    let n = c.drain(&mut f);
                    if n > 0 {
                        total += n;
                        progress = true;
                        self.inner.producer_waiters[idx].wake();
                    }
                }
            }
            if !progress {
                return total;
            }
        }
    }
}

impl<T: Send, const CAP: usize, W: BlockingWaiter> MpscConsumer<T, CAP, W> {
    /// Blocking receive. Yields one item; parks on the fan-in waiter
    /// when all rings are empty. Returns `Err(Shutdown)` after shutdown
    /// AND all rings drained.
    #[inline]
    pub fn recv(&mut self) -> Result<T, Shutdown> {
        loop {
            if let Some(v) = self.try_recv() {
                return Ok(v);
            }
            if self.is_finished() {
                return Err(Shutdown);
            }
            let self_ptr: *const Self = self;
            self.inner.fanin_waiter.wait_until(|| {
                // SAFETY: predicate runs on the same thread holding `&mut self`;
                // only reads `ring_consumers`.
                let this = unsafe { &*self_ptr };
                this.any_ring_has_work() || this.is_finished()
            });
        }
    }

    #[inline]
    pub fn recv_batch<F: FnMut(T)>(&mut self, mut f: F) -> Result<usize, Shutdown> {
        loop {
            let n = self.try_recv_batch(&mut f);
            if n > 0 {
                return Ok(n);
            }
            if self.is_finished() {
                return Err(Shutdown);
            }
            let self_ptr: *const Self = self;
            self.inner.fanin_waiter.wait_until(|| {
                let this = unsafe { &*self_ptr };
                this.any_ring_has_work() || this.is_finished()
            });
        }
    }
}

impl<T: Send, const CAP: usize, W: BlockingWaiter> MpscConsumer<T, CAP, W> {
    /// Drain the channel until shutdown, calling `f` for every item.
    /// Takes ownership so the caller cannot use the consumer afterwards.
    ///
    /// Returns when either the shutdown handle has been signaled OR all
    /// producers have been dropped AND every ring is empty (same
    /// condition as [`Self::recv_batch`] returning `Err(Shutdown)`).
    ///
    /// The caller MUST have called [`Self::bind`] before invoking this on
    /// `W = ParkWaiter` (same requirement as `recv_batch`).
    ///
    /// `run_blocking` takes `self` by value, so the consumer cannot be
    /// used again afterwards:
    ///
    /// ```compile_fail
    /// use arbitro_kit::route::Mpsc;
    ///
    /// let (_ps, c, sd) = Mpsc::<u64>::new(1);
    /// sd.signal();
    /// c.run_blocking(|_v| {});
    /// c.recv(); // moved: fails to compile
    /// ```
    #[inline]
    pub fn run_blocking<F: FnMut(T)>(mut self, mut f: F) {
        loop {
            match self.recv_batch(&mut f) {
                Ok(_) => continue,
                Err(Shutdown) => return,
            }
        }
    }
}

// ─── Async send / recv (AsyncWaiter only) ─────────────────────────────────

#[cfg(feature = "tokio")]
impl<T: Send + 'static, const CAP: usize, W: crate::waiter::AsyncWaiter + 'static>
    MpscProducer<T, CAP, W>
{
    pub fn send_async<'a>(
        &'a mut self,
        value: T,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        let inner_addr = Arc::as_ptr(&self.inner) as usize;
        let idx = self.my_idx;
        let rp_addr = (&self.ring_producer) as *const _ as usize;
        Box::pin(async move {
            let mut value = value;
            loop {
                if self.inner.shutdown.load(Ordering::Acquire) {
                    return;
                }
                match self.try_send(value) {
                    Ok(()) => return,
                    Err(v) => value = v,
                }
                // SAFETY: inner is kept alive by `self` for `'a`.
                let inner_ref: &'a Inner<W> = unsafe { &*(inner_addr as *const Inner<W>) };
                let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> =
                    Box::pin(inner_ref.producer_waiters[idx].wait_until(move || {
                        if inner_ref.shutdown.load(Ordering::Acquire) {
                            return true;
                        }
                        let rp = unsafe {
                            &*(rp_addr as *const Option<Producer<T, CAP, NoopWaiter>>)
                        };
                        match rp.as_ref() {
                            Some(p) => !p.is_full(),
                            None => true,
                        }
                    }));
                fut.await;
            }
        })
    }
}

#[cfg(feature = "tokio")]
impl<T: Send + 'static, const CAP: usize, W: crate::waiter::AsyncWaiter + 'static>
    MpscConsumer<T, CAP, W>
{
    pub fn recv_async<'a>(
        &'a mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, Shutdown>> + Send + 'a>> {
        let inner_addr = Arc::as_ptr(&self.inner) as usize;
        let self_addr = self as *const Self as usize;
        Box::pin(async move {
            loop {
                if let Some(v) = self.try_recv() {
                    return Ok(v);
                }
                if self.is_finished() {
                    return Err(Shutdown);
                }
                let inner_ref: &'a Inner<W> = unsafe { &*(inner_addr as *const Inner<W>) };
                let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> =
                    Box::pin(inner_ref.fanin_waiter.wait_until(move || {
                        let this = unsafe { &*(self_addr as *const Self) };
                        this.any_ring_has_work() || this.is_finished()
                    }));
                fut.await;
            }
        })
    }

    pub fn recv_batch_async<'a, F: FnMut(T) + Send + 'a>(
        &'a mut self,
        mut f: F,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize, Shutdown>> + Send + 'a>> {
        let inner_addr = Arc::as_ptr(&self.inner) as usize;
        let self_addr = self as *const Self as usize;
        Box::pin(async move {
            loop {
                let n = self.try_recv_batch(&mut f);
                if n > 0 {
                    return Ok(n);
                }
                if self.is_finished() {
                    return Err(Shutdown);
                }
                let inner_ref: &'a Inner<W> = unsafe { &*(inner_addr as *const Inner<W>) };
                let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> =
                    Box::pin(inner_ref.fanin_waiter.wait_until(move || {
                        let this = unsafe { &*(self_addr as *const Self) };
                        this.any_ring_has_work() || this.is_finished()
                    }));
                fut.await;
            }
        })
    }
}

// ─── NotifyWaiter zero-alloc specialization ───────────────────────────────

#[cfg(feature = "tokio")]
impl<T: Send, const CAP: usize> MpscProducer<T, CAP, crate::waiter::NotifyWaiter> {
    #[inline]
    pub fn send_async_send<'a>(
        &'a mut self,
        value: T,
    ) -> impl std::future::Future<Output = ()> + Send + 'a {
        // Split borrow: `inner` (immut/Sync) and `ring_producer` (mut) are
        // disjoint fields, so their borrows can coexist inside the loop.
        let MpscProducer {
            inner,
            ring_producer,
            my_idx,
            ..
        } = self;
        let my_idx = *my_idx;
        async move {
            let mut value = value;
            loop {
                let notified = inner.producer_waiters[my_idx].inner.notified();
                if inner.shutdown.load(Ordering::Acquire) {
                    return;
                }
                let rp = match ring_producer.as_mut() {
                    Some(p) => p,
                    None => return,
                };
                match rp.try_send(value) {
                    Ok(()) => {
                        // R1: wake unconditionally (see `try_send`).
                        inner.fanin_waiter.wake();
                        return;
                    }
                    Err(TrySendError::Full(v)) => value = v,
                    Err(TrySendError::Closed(v)) => {
                        drop(v);
                        return;
                    }
                }
                notified.await;
            }
        }
    }
}

#[cfg(feature = "tokio")]
impl<T: Send, const CAP: usize> MpscConsumer<T, CAP, crate::waiter::NotifyWaiter> {
    #[inline]
    pub fn recv_async_send<'a>(
        &'a mut self,
    ) -> impl std::future::Future<Output = Result<T, Shutdown>> + Send + 'a {
        let MpscConsumer {
            inner,
            ring_consumers,
            ..
        } = self;
        async move {
            let m = inner.m;
            loop {
                let notified = inner.fanin_waiter.inner.notified();
                // idx indexes two parallel arrays (ring_consumers + producer_waiters).
                #[allow(clippy::needless_range_loop)]
                for idx in 0..m {
                    if let Some(c) = ring_consumers[idx].as_mut() {
                        if let Ok(v) = c.try_recv() {
                            inner.producer_waiters[idx].wake();
                            return Ok(v);
                        }
                    }
                }
                if inner.is_shutdown_signaled()
                    || (inner.all_producers_gone()
                        && !ring_consumers
                            .iter()
                            .flatten()
                            .any(|c| !c.is_empty()))
                {
                    return Err(Shutdown);
                }
                notified.await;
            }
        }
    }

    #[inline]
    pub fn recv_batch_async_send<'a, F: FnMut(T) + Send + 'a>(
        &'a mut self,
        mut f: F,
    ) -> impl std::future::Future<Output = Result<usize, Shutdown>> + Send + 'a {
        let MpscConsumer {
            inner,
            ring_consumers,
            ..
        } = self;
        async move {
            let m = inner.m;
            loop {
                let notified = inner.fanin_waiter.inner.notified();
                let mut total = 0usize;
                let mut round = true;
                while round {
                    round = false;
                    // idx indexes two parallel arrays (ring_consumers + producer_waiters).
                    #[allow(clippy::needless_range_loop)]
                    for idx in 0..m {
                        if let Some(c) = ring_consumers[idx].as_mut() {
                            let n = c.drain(&mut f);
                            if n > 0 {
                                total += n;
                                round = true;
                                inner.producer_waiters[idx].wake();
                            }
                        }
                    }
                }
                if total > 0 {
                    return Ok(total);
                }
                if inner.is_shutdown_signaled()
                    || (inner.all_producers_gone()
                        && !ring_consumers
                            .iter()
                            .flatten()
                            .any(|c| !c.is_empty()))
                {
                    return Err(Shutdown);
                }
                notified.await;
            }
        }
    }
}

// ─── Shutdown handle ──────────────────────────────────────────────────────

pub struct MpscShutdown<W: Waiter = ParkWaiter> {
    inner: Arc<Inner<W>>,
}

impl<W: Waiter> Clone for MpscShutdown<W> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<W: Waiter> MpscShutdown<W> {
    /// Signal shutdown and wake every parked endpoint.
    #[inline]
    pub fn signal(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.fanin_waiter.wake();
        for w in self.inner.producer_waiters.iter() {
            w.wake();
        }
    }

    #[inline]
    pub fn is_signaled(&self) -> bool {
        self.inner.shutdown.load(Ordering::Acquire)
    }

    /// Same predicate as `MpscConsumer::is_closed`: shutdown signaled OR
    /// every producer permanently dropped.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.inner.is_shutdown_signaled() || self.inner.all_producers_gone()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::while_let_loop)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn single_producer_roundtrip() {
        let (mut ps, mut c, _sd) = Mpsc::<u64>::new(1);
        let mut p = ps.remove(0);

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
    fn exactly_once_delivery_4_producers() {
        const M: usize = 4;
        const PER: u64 = 500;

        let (ps, mut c, sd) = Mpsc::<u64>::new(M);
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

        let producer_handles: Vec<_> = ps
            .into_iter()
            .enumerate()
            .map(|(i, mut p)| {
                let b = barrier.clone();
                thread::spawn(move || {
                    p.bind();
                    b.wait();
                    for k in 0..PER {
                        p.send(i as u64 * 10_000 + k);
                    }
                })
            })
            .collect();
        for h in producer_handles {
            h.join().unwrap();
        }

        thread::sleep(Duration::from_millis(20));
        sd2.signal();
        consumer_h.join().unwrap();

        assert_eq!(got_count.load(Ordering::Relaxed), (M as u64 * PER) as usize);
        assert_eq!(got_sum.load(Ordering::Relaxed) as u64, sum_expected);
    }

    #[test]
    fn try_send_full_returns_err() {
        let (mut ps, _c, _sd) = Mpsc::<u64, 1>::new(1);
        let mut p = ps.remove(0);
        assert!(p.try_send(1).is_ok());
        assert_eq!(p.try_send(2), Err(2));
    }

    #[test]
    fn drop_producers_shuts_consumer() {
        let (ps, mut c, _sd) = Mpsc::<u64>::new(2);
        drop(ps);
        // No producers, no items → recv returns Err(Shutdown).
        assert_eq!(c.recv(), Err(Shutdown));
    }

    #[test]
    fn drop_producers_delivers_inflight_first() {
        let (mut ps, mut c, _sd) = Mpsc::<u64, 16>::new(1);
        let mut p = ps.remove(0);
        for i in 0..10u64 {
            p.try_send(i).unwrap();
        }
        drop(p);

        let mut got = Vec::new();
        loop {
            match c.recv() {
                Ok(v) => got.push(v),
                Err(Shutdown) => break,
            }
        }
        assert_eq!(got, (0..10u64).collect::<Vec<u64>>());
    }

    #[test]
    fn shutdown_wakes_consumer() {
        let (_ps, mut c, sd) = Mpsc::<u64>::new(2);
        let h = thread::spawn(move || {
            c.bind();
            c.recv()
        });
        thread::sleep(Duration::from_millis(30));
        sd.signal();
        assert_eq!(h.join().unwrap(), Err(Shutdown));
    }

    #[test]
    fn drop_drains_inflight() {
        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let (mut ps, _c, _sd) = Mpsc::<Tracked>::new(2);
            let mut p0 = ps.remove(0);
            let mut p1 = ps.remove(0);
            p0.try_send(Tracked(drops.clone())).ok().unwrap();
            p1.try_send(Tracked(drops.clone())).ok().unwrap();
        }
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn high_producer_count_above_64_works() {
        const M: usize = 100;
        let (ps, mut c, _sd) = Mpsc::<u32>::new(M);
        c.bind();

        let handles: Vec<_> = ps
            .into_iter()
            .enumerate()
            .map(|(i, mut p)| {
                thread::spawn(move || {
                    p.bind();
                    for v in 0..50u32 {
                        p.send((i as u32) * 1000 + v);
                    }
                })
            })
            .collect();

        let total = M * 50;
        let mut got = 0usize;
        let mut sum = 0u64;
        while got < total {
            c.recv_batch(|v| {
                sum += v as u64;
                got += 1;
            })
            .unwrap();
        }
        for h in handles {
            h.join().unwrap();
        }

        let mut expected = 0u64;
        for i in 0..M {
            for v in 0..50u32 {
                expected += ((i as u32) * 1000 + v) as u64;
            }
        }
        assert_eq!(sum, expected);
        assert_eq!(got, total);
    }

    #[test]
    fn run_blocking_delivers_all_items_then_returns() {
        const N: u64 = 1000;
        let (mut ps, c, sd) = Mpsc::<u64>::new(1);
        let mut p = ps.remove(0);
        c.bind();

        let producer_h = thread::spawn(move || {
            p.bind();
            for i in 0..N {
                p.send(i);
            }
        });

        let count = Arc::new(AtomicUsize::new(0));
        let count2 = count.clone();
        let consumer_h = thread::spawn(move || {
            c.run_blocking(|_v| {
                count2.fetch_add(1, Ordering::Relaxed);
            });
        });

        producer_h.join().unwrap();
        // Give the consumer a chance to drain in-flight items before shutdown.
        thread::sleep(Duration::from_millis(30));
        sd.signal();
        consumer_h.join().unwrap();

        assert_eq!(count.load(Ordering::Relaxed), N as usize);
    }

    #[test]
    fn run_blocking_returns_immediately_when_already_closed() {
        let (_ps, c, sd) = Mpsc::<u64>::new(1);
        c.bind();
        sd.signal();

        let called = Arc::new(AtomicUsize::new(0));
        let called2 = called.clone();
        c.run_blocking(move |_v: u64| {
            called2.fetch_add(1, Ordering::Relaxed);
        });

        assert_eq!(called.load(Ordering::Relaxed), 0);
    }

    #[test]
    #[should_panic(expected = "boom")]
    fn run_blocking_forwards_panics_from_f() {
        let (mut ps, c, _sd) = Mpsc::<u64, 16>::new(1);
        let mut p = ps.remove(0);
        for i in 0..10u64 {
            p.try_send(i).unwrap();
        }
        drop(p);

        c.run_blocking(|v| {
            if v == 3 {
                panic!("boom");
            }
        });
    }

    #[test]
    fn is_closed_false_at_start() {
        let (_ps, c, sd) = Mpsc::<u64>::new(4);
        assert!(!c.is_closed());
        assert!(!sd.is_closed());
    }

    #[test]
    fn is_closed_true_after_shutdown_signal() {
        let (_ps, c, sd) = Mpsc::<u64>::new(4);
        sd.signal();
        assert!(c.is_closed());
        assert!(sd.is_closed());
    }

    #[test]
    fn is_closed_true_after_all_producers_drop() {
        let (ps, c, sd) = Mpsc::<u64>::new(4);
        drop(ps);
        assert!(c.is_closed());
        assert!(sd.is_closed());
    }

    #[test]
    fn is_closed_survives_pending_items() {
        let (mut ps, c, _sd) = Mpsc::<u64, 16>::new(1);
        let mut p = ps.remove(0);
        p.try_send(1).unwrap();
        p.try_send(2).unwrap();
        p.try_send(3).unwrap();
        drop(ps);
        drop(p);
        // Closed for new writes even though 3 items are still buffered.
        assert!(c.is_closed());
        assert!(c.pending() > 0);
    }

    #[test]
    fn concurrent_probe_during_recv() {
        // MpscConsumer is Send + !Sync, so it stays on the recv thread.
        // MpscShutdown (Arc<Inner<W>>-backed, Clone) is the intended
        // cross-thread probe handle — hammer is_closed() on a clone from
        // thread A while thread B drives recv_batch, and assert the latch
        // is monotonic (once true, never flips back to false).
        let (mut ps, mut c, sd) = Mpsc::<u64, 64>::new(2);
        let mut p0 = ps.remove(0);
        let mut p1 = ps.remove(0);
        c.bind();
        p0.bind();
        p1.bind();

        let consumer_h = thread::spawn(move || loop {
            match c.recv_batch(|_v| {}) {
                Ok(_) => continue,
                Err(Shutdown) => break,
            }
        });

        let sd_probe = sd.clone();
        let seen_closed = Arc::new(AtomicUsize::new(0)); // 0 = false, 1 = true
        let seen_closed2 = seen_closed.clone();
        let probe_h = thread::spawn(move || {
            for _ in 0..100_000 {
                let closed_now = sd_probe.is_closed();
                if seen_closed2.load(Ordering::Relaxed) == 1 {
                    assert!(closed_now, "is_closed must not flip back to false");
                } else if closed_now {
                    seen_closed2.store(1, Ordering::Relaxed);
                }
            }
        });

        for i in 0..1000u64 {
            p0.try_send(i).ok();
            p1.try_send(i).ok();
        }
        drop(p0);
        drop(p1);

        probe_h.join().unwrap();
        consumer_h.join().unwrap();
        assert!(sd.is_closed());
    }
}
