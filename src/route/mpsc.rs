//! M:1 multi-producer / single-consumer bounded channel.
//!
//! [`Mpsc<T, RING_CAP>`] is the **single-consumer specialisation** of
//! [`super::mpmc::Mpmc`]: N is hardcoded to 1, so every producer feeds the
//! same shard. Compared with `Mpmc::<T, RING_CAP>::new(M, 1)`, this drops:
//!
//! - the per-producer adaptive `cursor` and the `for k in 0..n` shard scan,
//! - the `Shard` indirection (fields move directly into `MpscInner`),
//! - the `shard_idx` field on the consumer,
//! - the `Vec<MpmcConsumer>` return — the single consumer is returned by value.
//!
//! Hot-path semantics are otherwise identical to `Mpmc`:
//! - Each producer owns a private SPSC mini-ring of `RING_CAP` slots.
//! - The consumer drains every ready ring via the shared SignalSet bitmap.
//! - Bits are cleared only in the park path (Dekker recheck), never during
//!   drain — so amortised cost is one `fetch_or` per burst, not per item.
//!
//! When in doubt between `Mpsc` and `Mpmc(M, 1)`: pick `Mpsc`. The codegen
//! is tighter and the API is clearer for true M:1 fan-in.

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::gate::{Park, SignalId, SignalSet};
use crate::route::hub::Shutdown;

/// Maximum number of producers. Same limit as `Mpmc` because the SignalSet
/// bit layout is shared (M producer bits + 1 shutdown bit).
pub const MAX_MPSC_PRODUCERS: usize = 255;

// ─── Per-producer mini-Ring (SPSC) ────────────────────────────────────────

/// Cache line size on x86_64 / aarch64. Used to separate `head` and `tail`
/// onto distinct cache lines so the producer's `head.store(Release)` does
/// not invalidate the consumer's cached `tail` (and vice versa). Without
/// this padding, every send triggers a cross-CPU cache-line bounce on the
/// SAME line that holds both cursors — a textbook **false sharing** pitfall.
///
/// Empirically saves 5–15 % on hot SPSC paths on x86_64 (matches what
/// LMAX Disruptor, JCTools, and DPDK rte_ring all do for the same reason).
const CACHE_LINE: usize = 64;

#[repr(C)]
struct PRing<T: Send, const RING_CAP: usize> {
    /// Producer cursor — only this producer writes; consumer reads with Acquire.
    head: AtomicUsize,
    _pad_head: [u8; CACHE_LINE - core::mem::size_of::<AtomicUsize>()],
    /// Consumer cursor — only the consumer writes; producer reads with Acquire.
    tail: AtomicUsize,
    _pad_tail: [u8; CACHE_LINE - core::mem::size_of::<AtomicUsize>()],
    /// Slot storage. Boxed so the struct itself stays small and easy to
    /// stack-move during construction. The pointer here is read-only after
    /// construction, so it's never on the cache-coherence hot path.
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

// ─── Shared inner state (no Shard layer) ───────────────────────────────────

struct MpscInner<T: Send, const RING_CAP: usize> {
    /// Bit `p` = "ring[p] possibly has data". Bit `m` = shutdown.
    full_set: SignalSet,
    /// `rings[p]` owned by producer `p`. Single shard inline.
    rings: Box<[PRing<T, RING_CAP>]>,
    /// Per-chunk mask covering producer bits `0..m` only.
    full_mask_chunks: Box<[u64]>,
    shutdown_id: SignalId,
    /// Per-producer backpressure park.
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

        let mut full_set = SignalSet::with_capacity(m + 1);
        for p in 0..m {
            let name: &'static str =
                Box::leak(format!("mpsc_p{p}").into_boxed_str());
            let _ = full_set.create(name);
        }
        let shutdown_name: &'static str = Box::leak("mpsc_shutdown".into());
        let shutdown_id = full_set.create(shutdown_name);
        debug_assert_eq!(shutdown_id.index() as usize, m);

        let n_chunks = full_set.n_chunks();
        let mut full_mask_chunks: Vec<u64> = vec![0; n_chunks];
        for p in 0..m {
            let c = p / 64;
            full_mask_chunks[c] |= 1u64 << (p % 64);
        }

        let rings: Vec<PRing<T, RING_CAP>> =
            (0..m).map(|_| PRing::new()).collect();
        let producer_parks: Vec<Park> = (0..m).map(|_| Park::new()).collect();

        Self {
            full_set,
            rings: rings.into_boxed_slice(),
            full_mask_chunks: full_mask_chunks.into_boxed_slice(),
            shutdown_id,
            producer_parks: producer_parks.into_boxed_slice(),
            shutdown: AtomicBool::new(false),
            m,
        }
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

// ─── Public facade ─────────────────────────────────────────────────────────

/// M:1 bounded channel. Each producer is an SPSC ring of `RING_CAP` slots
/// feeding the same consumer.
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
                my_id: SignalId::new(p as u8),
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

// ─── Producer handle ───────────────────────────────────────────────────────

pub struct MpscProducer<T: Send, const RING_CAP: usize = 64> {
    inner: Arc<MpscInner<T, RING_CAP>>,
    my_idx: usize,
    my_id: SignalId,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send, const RING_CAP: usize> MpscProducer<T, RING_CAP> {
    #[inline]
    pub fn index(&self) -> usize { self.my_idx }

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

    /// Non-blocking send. No shard scan, no cursor — direct ring access.
    /// Returns `Err(value)` if this producer's ring is full.
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), T> {
        let ring = &self.inner.rings[self.my_idx];
        let h = ring.head.load(Ordering::Relaxed);
        let t = ring.tail.load(Ordering::Acquire);
        if PRing::<T, RING_CAP>::is_full(h, t) {
            return Err(value);
        }
        // Safety: SPSC — only this producer writes slots[head] on this ring.
        unsafe {
            (*ring.slots[h & PRing::<T, RING_CAP>::MASK].get()).write(value);
        }
        ring.head.store(h.wrapping_add(1), Ordering::Release);
        self.inner.full_set.release(self.my_id);
        Ok(())
    }

    /// Batch send: amortise the SignalSet release across multiple items.
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
        self.inner.full_set.release(self.my_id);
        take
    }

    /// Blocking send. Parks on this producer's backpressure park if the
    /// ring is full.
    #[inline]
    pub fn send(&self, mut value: T) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            self.inner.producer_parks[self.my_idx]
                .wait_until(|| self.has_room());
        }
    }
}

// ─── Consumer handle ───────────────────────────────────────────────────────

pub struct MpscConsumer<T: Send, const RING_CAP: usize = 64> {
    inner: Arc<MpscInner<T, RING_CAP>>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send, const RING_CAP: usize> MpscConsumer<T, RING_CAP> {
    #[inline]
    pub fn bind(&self) {
        self.inner.full_set.set_worker(std::thread::current());
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
        for c in 0..self.inner.full_mask_chunks.len() {
            let state = self.inner.full_set.state_chunk(c)
                & self.inner.full_mask_chunks[c];
            if state != 0 { return true; }
        }
        false
    }

    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        let m = self.inner.m;
        let n_chunks = self.inner.full_mask_chunks.len();
        for c in 0..n_chunks {
            let state = self.inner.full_set.state_chunk(c)
                & self.inner.full_mask_chunks[c];
            if state == 0 { continue; }
            let mut remaining = state;
            while remaining != 0 {
                let bit_in_chunk = remaining.trailing_zeros() as usize;
                let bit = 1u64 << bit_in_chunk;
                remaining &= !bit;
                let p = c * 64 + bit_in_chunk;
                if p >= m { break; }
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
        }
        None
    }

    pub fn recv(&self) -> Result<T, Shutdown> {
        loop {
            if let Some(v) = self.try_recv() { return Ok(v); }
            if self.inner.shutdown.load(Ordering::Acquire) {
                self.inner.full_set.lock(self.inner.shutdown_id);
                return Err(Shutdown);
            }
            if self.park_or_drain()? { /* drained, loop */ }
        }
    }

    pub fn recv_batch<F: FnMut(T)>(&self, mut f: F) -> Result<usize, Shutdown> {
        loop {
            let count = self.drain_all(&mut f);
            if count > 0 { return Ok(count); }
            if self.inner.shutdown.load(Ordering::Acquire) {
                self.inner.full_set.lock(self.inner.shutdown_id);
                return Err(Shutdown);
            }
            self.park_or_drain()?;
        }
    }

    pub fn try_recv_batch<F: FnMut(T)>(&self, mut f: F) -> usize {
        self.drain_all(&mut f)
    }

    #[inline]
    fn drain_all<F: FnMut(T)>(&self, f: &mut F) -> usize {
        let m = self.inner.m;
        let n_chunks = self.inner.full_mask_chunks.len();
        let mut count = 0;
        loop {
            let mut any_state = false;
            let mut progress = false;
            for c in 0..n_chunks {
                let state = self.inner.full_set.state_chunk(c)
                    & self.inner.full_mask_chunks[c];
                if state == 0 { continue; }
                any_state = true;
                let mut remaining = state;
                while remaining != 0 {
                    let bit_in_chunk = remaining.trailing_zeros() as usize;
                    remaining &= remaining - 1;
                    let p = c * 64 + bit_in_chunk;
                    if p >= m { break; }
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
            }
            if !any_state { return count; }
            if !progress { return count; }
        }
    }

    fn park_or_drain(&self) -> Result<bool, Shutdown> {
        let m = self.inner.m;
        let n_chunks = self.inner.full_mask_chunks.len();

        for c in 0..n_chunks {
            self.inner.full_set
                .lock_chunk_mask(c, self.inner.full_mask_chunks[c]);
        }
        std::sync::atomic::fence(Ordering::SeqCst);

        let mut any_raced = false;
        for p in 0..m {
            let ring = &self.inner.rings[p];
            let h = ring.head.load(Ordering::Acquire);
            let t = ring.tail.load(Ordering::Relaxed);
            if h != t {
                self.inner.full_set.release(SignalId::new(p as u8));
                any_raced = true;
            }
        }
        if any_raced { return Ok(true); }

        if self.inner.shutdown.load(Ordering::Acquire) {
            self.inner.full_set.lock(self.inner.shutdown_id);
            return Err(Shutdown);
        }

        self.inner.full_set.acquire_any_chunk();
        Ok(false)
    }
}

// ─── Shutdown handle ───────────────────────────────────────────────────────

pub struct MpscShutdown<T: Send, const RING_CAP: usize = 64> {
    inner: Arc<MpscInner<T, RING_CAP>>,
}

impl<T: Send, const RING_CAP: usize> Clone for MpscShutdown<T, RING_CAP> {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}

impl<T: Send, const RING_CAP: usize> MpscShutdown<T, RING_CAP> {
    #[inline]
    pub fn signal(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.full_set.release(self.inner.shutdown_id);
        for p in self.inner.producer_parks.iter() { p.wake(); }
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
    fn high_producer_count_above_64_works() {
        const M: usize = 100;
        let (mut ps, c, _sd) = Mpsc::<u32>::new(M);
        c.bind();

        let producers: Vec<_> = ps.drain(..).collect();
        let handles: Vec<_> = producers.into_iter().enumerate().map(|(i, p)| {
            std::thread::spawn(move || {
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
