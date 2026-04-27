//! M:1 multi-producer / single-consumer bounded channel.
//!
//! Per-producer SPSC mini-rings + consumer-side scan for wakeup. Producers
//! never execute a `LOCK`-prefixed RMW on the send path: the only atomic
//! they touch outside their own ring is a single `AtomicBool` load
//! (`consumer_parked`) used to decide whether to `unpark` the consumer.
//!
//! ## Hot paths
//!
//! Producer `try_send`:
//!   - load this producer's `head` (Relaxed) and `tail` (Acquire);
//!   - if full → return `Err(value)`;
//!   - write slot, `head.store(Release)`;
//!   - `consumer_parked.load(Relaxed)` — if `true`, `unpark` the consumer.
//!
//! Consumer `try_recv`:
//!   - for `p in 0..M`: load (head, tail) of `ring[p]`; if non-empty, take
//!     one item and `producer_parks[p].wake()` (only relevant under
//!     backpressure).
//!
//! Consumer `recv` park path uses a Dekker recheck:
//!   - `consumer_parked.store(true, SeqCst)`
//!   - rescan all M rings + shutdown flag
//!   - if still nothing → `thread::park()`.
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
//! Bench (`benches/mpsc_overhead.rs`, x86_64, 4 P-cores + 8 E-cores):
//!
//! | Topology      | crossbeam mean | this `Mpsc` mean | speed-up |
//! |---------------|---------------:|-----------------:|---------:|
//! | 1P/1C cross   |   ~58 ns/op    |     ~6 ns/op     |   ~10×   |
//! | 4P/1C         |   ~26 ns/op    |     ~2 ns/op     |   ~13×   |
//! | 8P/1C         |   ~58 ns/op    |     ~2 ns/op     |   ~29×   |
//! | 100P/1C       |   ~65 ns/op    |    ~36 ns/op     |   ~1.8×  |
//!
//! ## When to reach for `Mpsc`
//!
//! - True M:1 fan-in with anonymous producers (no per-producer reply
//!   channel needed — use `Hub` for named ports + replies).
//! - When the consumer is a dedicated drain thread that calls `recv` /
//!   `recv_batch` in a tight loop.
//! - For M:N fan-in, use [`super::mpmc::Mpmc`] instead.

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::gate::Park;
use crate::route::hub::Shutdown;

/// Maximum number of producers per channel.
pub const MAX_MPSC_PRODUCERS: usize = 255;

const CACHE_LINE: usize = 64;

// ─── Per-producer mini-Ring (SPSC) ────────────────────────────────────────

/// Cache-line-padded SPSC ring shared between one producer and the consumer.
/// `head` and `tail` live on separate cache lines to avoid false sharing on
/// the cross-core publish path.
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

struct MpscInner<T: Send, const RING_CAP: usize> {
    rings: Box<[PRing<T, RING_CAP>]>,
    /// Set by the consumer immediately before parking (with `SeqCst`).
    /// Producers read with `Relaxed`; on `true` they `unpark` the consumer.
    consumer_parked: AtomicBool,
    /// Consumer's `Thread` handle, written once on `bind()` and read by
    /// producers under the `consumer_parked` Dekker dance.
    consumer_thread: UnsafeCell<Option<std::thread::Thread>>,
    /// Per-producer backpressure park: when a producer's ring is full, it
    /// parks here until the consumer drains and calls `wake()`.
    producer_parks: Box<[Park]>,
    shutdown: AtomicBool,
    m: usize,
}

unsafe impl<T: Send, const RING_CAP: usize> Sync for MpscInner<T, RING_CAP> {}
unsafe impl<T: Send, const RING_CAP: usize> Send for MpscInner<T, RING_CAP> {}

impl<T: Send, const RING_CAP: usize> MpscInner<T, RING_CAP> {
    fn new(m: usize) -> Self {
        assert!(
            RING_CAP > 0 && RING_CAP.is_power_of_two(),
            "RING_CAP must be a power of two ≥ 1"
        );
        let rings: Vec<PRing<T, RING_CAP>> =
            (0..m).map(|_| PRing::new()).collect();
        let producer_parks: Vec<Park> = (0..m).map(|_| Park::new()).collect();
        Self {
            rings: rings.into_boxed_slice(),
            consumer_parked: AtomicBool::new(false),
            consumer_thread: UnsafeCell::new(None),
            producer_parks: producer_parks.into_boxed_slice(),
            shutdown: AtomicBool::new(false),
            m,
        }
    }

    /// Wake the consumer if it's parked. Reads the flag with `Relaxed`;
    /// false negatives are tolerated because the consumer's Dekker recheck
    /// closes the race (it re-scans every ring after setting `parked = true`
    /// with `SeqCst`).
    #[inline]
    fn maybe_wake_consumer(&self) {
        if self.consumer_parked.load(Ordering::Relaxed) {
            // SAFETY: `consumer_thread` is written once in `bind()` before
            // any producer can publish. After binding it is read-only.
            unsafe {
                if let Some(t) = &*self.consumer_thread.get() {
                    t.unpark();
                }
            }
        }
    }

    /// `true` iff a consumer thread has been registered via `bind()`.
    /// Used by `recv` / `recv_batch` to surface a clear panic instead of an
    /// infinite hang when the caller forgot to bind.
    #[inline]
    fn has_consumer_thread(&self) -> bool {
        // SAFETY: per type invariant, only the consumer reads this field.
        unsafe { (*self.consumer_thread.get()).is_some() }
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
}

impl<T: Send, const RING_CAP: usize> Drop for MpscInner<T, RING_CAP> {
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
pub struct Mpsc<T: Send, const RING_CAP: usize = 64>(PhantomData<T>);

impl<T: Send + 'static, const RING_CAP: usize> Mpsc<T, RING_CAP> {
    /// Build an `Mpsc` with `m` producers and 1 consumer.
    ///
    /// Returns `(producers, consumer, shutdown)`. The consumer is returned
    /// by value (not `Vec`) — there is exactly one.
    ///
    /// # Panics
    /// - `m == 0`
    /// - `m > MAX_MPSC_PRODUCERS`
    /// - `RING_CAP` not a power of two ≥ 1
    pub fn new(
        m: usize,
    ) -> (
        Vec<MpscProducer<T, RING_CAP>>,
        MpscConsumer<T, RING_CAP>,
        MpscShutdown<T, RING_CAP>,
    ) {
        assert!(m > 0, "Mpsc::new: m must be > 0");
        assert!(
            m <= MAX_MPSC_PRODUCERS,
            "Mpsc::new: m must be <= {MAX_MPSC_PRODUCERS}"
        );
        let inner = Arc::new(MpscInner::<T, RING_CAP>::new(m));
        let producers: Vec<MpscProducer<T, RING_CAP>> = (0..m)
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
}

// ─── Producer handle ──────────────────────────────────────────────────────

pub struct MpscProducer<T: Send, const RING_CAP: usize = 64> {
    inner: Arc<MpscInner<T, RING_CAP>>,
    my_idx: usize,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send, const RING_CAP: usize> MpscProducer<T, RING_CAP> {
    #[inline]
    pub fn index(&self) -> usize { self.my_idx }

    /// Register the current thread as this producer's worker. Must be
    /// called by the producer thread before it can be parked on
    /// backpressure.
    #[inline]
    pub fn bind(&self) {
        self.inner.producer_parks[self.my_idx]
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
    /// Release store + 1 Relaxed load. **Zero `LOCK`-prefixed RMW.**
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
        self.inner.maybe_wake_consumer();
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
        self.inner.maybe_wake_consumer();
        take
    }

    /// Blocking send. Parks on this producer's backpressure park if the
    /// ring is full; returns silently on shutdown without delivering.
    #[inline]
    pub fn send(&self, mut value: T) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            self.inner.producer_parks[self.my_idx]
                .wait_until(|| self.has_room()
                    || self.inner.shutdown.load(Ordering::Acquire));
            if self.inner.shutdown.load(Ordering::Acquire) {
                return;
            }
        }
    }
}

// ─── Consumer handle ──────────────────────────────────────────────────────

pub struct MpscConsumer<T: Send, const RING_CAP: usize = 64> {
    inner: Arc<MpscInner<T, RING_CAP>>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send, const RING_CAP: usize> MpscConsumer<T, RING_CAP> {
    /// Register the consumer thread. Must be called by the consumer thread
    /// itself before any producer publishes.
    pub fn bind(&self) {
        // SAFETY: `consumer_thread` is written once before producers can
        // observe `consumer_parked == true`.
        unsafe {
            *self.inner.consumer_thread.get() = Some(std::thread::current());
        }
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
            self.inner.producer_parks[p].wake();
            return Some(v);
        }
        None
    }

    /// Blocking receive. Drains one item, parking on `thread::park` when
    /// every ring is empty. Returns `Err(Shutdown)` after shutdown is
    /// signalled and all rings are drained.
    ///
    /// # Panics
    /// Panics if [`bind`](Self::bind) was never called on this consumer.
    /// Without a registered consumer thread, producers' `unpark` calls
    /// have no target — `recv` would otherwise hang forever; the panic
    /// surfaces the bug at the call site instead.
    pub fn recv(&self) -> Result<T, Shutdown> {
        loop {
            if let Some(v) = self.try_recv() { return Ok(v); }
            if self.inner.shutdown.load(Ordering::Acquire) {
                return Err(Shutdown);
            }
            assert!(
                self.inner.has_consumer_thread(),
                "MpscConsumer::recv reached park path without bind() — call bind() on the consumer thread first",
            );
            // Dekker park: announce parking, recheck, then park.
            self.inner.consumer_parked.store(true, Ordering::SeqCst);
            if self.inner.any_ring_has_work() {
                self.inner.consumer_parked.store(false, Ordering::Relaxed);
                continue;
            }
            if self.inner.shutdown.load(Ordering::Acquire) {
                self.inner.consumer_parked.store(false, Ordering::Relaxed);
                return Err(Shutdown);
            }
            std::thread::park();
            self.inner.consumer_parked.store(false, Ordering::Relaxed);
        }
    }

    /// Drain at least one full pass and invoke `f` on every item drained.
    /// Blocks (parks) when no work is found and the channel is alive.
    ///
    /// # Panics
    /// Panics if [`bind`](Self::bind) was never called on this consumer
    /// — same reason as [`recv`](Self::recv).
    pub fn recv_batch<F: FnMut(T)>(&self, mut f: F) -> Result<usize, Shutdown> {
        loop {
            let count = self.drain_all(&mut f);
            if count > 0 { return Ok(count); }
            if self.inner.shutdown.load(Ordering::Acquire) {
                return Err(Shutdown);
            }
            assert!(
                self.inner.has_consumer_thread(),
                "MpscConsumer::recv_batch reached park path without bind() — call bind() on the consumer thread first",
            );
            self.inner.consumer_parked.store(true, Ordering::SeqCst);
            if self.inner.any_ring_has_work() {
                self.inner.consumer_parked.store(false, Ordering::Relaxed);
                continue;
            }
            if self.inner.shutdown.load(Ordering::Acquire) {
                self.inner.consumer_parked.store(false, Ordering::Relaxed);
                return Err(Shutdown);
            }
            std::thread::park();
            self.inner.consumer_parked.store(false, Ordering::Relaxed);
        }
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
        let m = self.inner.m;
        let mut count: usize = 0;
        loop {
            let mut progress = false;
            for p in 0..m {
                let ring = &self.inner.rings[p];
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
                self.inner.producer_parks[p].wake();
            }
            if !progress { return count; }
        }
    }
}

// ─── Shutdown handle ──────────────────────────────────────────────────────

pub struct MpscShutdown<T: Send, const RING_CAP: usize = 64> {
    inner: Arc<MpscInner<T, RING_CAP>>,
}

impl<T: Send, const RING_CAP: usize> Clone for MpscShutdown<T, RING_CAP> {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}

impl<T: Send, const RING_CAP: usize> MpscShutdown<T, RING_CAP> {
    /// Mark the channel shut down and wake every parked endpoint
    /// (consumer + all producers blocked on backpressure).
    #[inline]
    pub fn signal(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        if self.inner.consumer_parked.load(Ordering::Relaxed) {
            // SAFETY: `consumer_thread` is written once in `bind()` before
            // any producer can race us.
            unsafe {
                if let Some(t) = &*self.inner.consumer_thread.get() {
                    t.unpark();
                }
            }
        }
        for p in self.inner.producer_parks.iter() { p.wake(); }
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
    #[should_panic(expected = "MpscConsumer::recv reached park path without bind()")]
    fn recv_without_bind_panics() {
        // No producers will ever publish; rings are empty and no bind was
        // called. recv must panic instead of parking forever.
        let (_ps, c, _sd) = Mpsc::<u64>::new(1);
        let _ = c.recv();
    }

    #[test]
    #[should_panic(expected = "MpscConsumer::recv_batch reached park path without bind()")]
    fn recv_batch_without_bind_panics() {
        let (_ps, c, _sd) = Mpsc::<u64>::new(1);
        let _ = c.recv_batch(|_| {});
    }

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

    /// Orphan-check 1: `send()` blocked on backpressure when shutdown
    /// fires. Current behaviour silently drops the value (documented in
    /// `send` doc comment). This test pins that behaviour so any change
    /// is intentional.
    #[test]
    fn send_dropped_value_is_destructed_not_leaked() {
        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }
        let drops = Arc::new(AtomicUsize::new(0));

        // RING_CAP=1, 1 producer. Send one item to fill the ring.
        let (mut ps, c, sd) = Mpsc::<Tracked, 1>::new(1);
        let p = ps.remove(0);
        p.try_send(Tracked(drops.clone())).ok().unwrap(); // ring full now.

        // Spawn a thread that calls send → will park on backpressure.
        let drops2 = drops.clone();
        let h = thread::spawn(move || {
            p.bind();
            p.send(Tracked(drops2));   // value 2 — gets orphaned by shutdown.
        });
        // Give it time to enter the park.
        thread::sleep(Duration::from_millis(50));
        sd.signal();
        h.join().unwrap();

        // After shutdown returned and the producer thread joined, the
        // orphan from `send` has already been dropped. The in-ring value
        // is still alive — it lives until MpscInner drops, which requires
        // ALL handles (consumer + shutdown) to be released.
        assert_eq!(drops.load(Ordering::Relaxed), 1,
            "send() must drop the orphaned value, not leak it");

        drop(sd);
        drop(c); // last strong Arc → MpscInner::drop drains ring.
        assert_eq!(drops.load(Ordering::Relaxed), 2,
            "ring drain on Drop must destruct the in-flight value");
    }

    /// Orphan-check 2: items already published to the ring before
    /// shutdown MUST still be deliverable to the consumer. The consumer
    /// drains everything before observing `Err(Shutdown)`.
    #[test]
    fn shutdown_drains_published_items_first() {
        let (mut ps, c, sd) = Mpsc::<u64, 16>::new(1);
        let p = ps.remove(0);
        for i in 0..10u64 { p.try_send(i).unwrap(); }

        // Signal shutdown BEFORE the consumer touches anything.
        sd.signal();

        // Consumer must still receive all 10 items.
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
}
