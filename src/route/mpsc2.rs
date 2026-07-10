//! M:1 bounded channel built on per-producer [`Ring<T, CAP, NoopWaiter>`].
//! The ring's internal `wake()` is a no-op; fan-in wakes go through
//! Mpsc2's own shared waiter, gated by [`Producer::should_notify_consumer`].

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::route::hub::Shutdown;
use crate::stream::{Consumer, Producer, Ring, TryRecvError, TrySendError};
use crate::waiter::{BlockingWaiter, NoopWaiter, ParkWaiter, Waiter};

/// Maximum producers per Mpsc2 channel.
pub const MAX_MPSC2_PRODUCERS: usize = 255;

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
pub struct Mpsc2<T, const CAP: usize = 64, W: Waiter = ParkWaiter>(PhantomData<(T, W)>);

impl<T: Send + 'static, const CAP: usize, W: Waiter + 'static> Mpsc2<T, CAP, W> {
    /// Build an Mpsc2 with `m` producers and 1 consumer.
    ///
    /// Returns `(producers, consumer, shutdown)`.
    ///
    /// # Panics
    /// - `m == 0`
    /// - `m > MAX_MPSC2_PRODUCERS`
    /// - `CAP` not a power of two ≥ 1
    pub fn new(
        m: usize,
    ) -> (
        Vec<Mpsc2Producer<T, CAP, W>>,
        Mpsc2Consumer<T, CAP, W>,
        Mpsc2Shutdown<W>,
    ) {
        assert!(m > 0, "Mpsc2::new: m must be > 0");
        assert!(
            m <= MAX_MPSC2_PRODUCERS,
            "Mpsc2::new: m must be <= {MAX_MPSC2_PRODUCERS}"
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

        let producers: Vec<Mpsc2Producer<T, CAP, W>> = producer_halves
            .into_iter()
            .enumerate()
            .map(|(idx, ring_producer)| Mpsc2Producer {
                ring_producer: Some(ring_producer),
                inner: inner.clone(),
                my_idx: idx,
                _not_sync: PhantomData,
            })
            .collect();

        let consumer = Mpsc2Consumer {
            inner: inner.clone(),
            ring_consumers: consumer_halves,
            _not_sync: PhantomData,
        };
        let shutdown = Mpsc2Shutdown { inner };

        (producers, consumer, shutdown)
    }
}

// ─── Producer handle ──────────────────────────────────────────────────────

/// Producer handle for one ring within an [`Mpsc2`]. `Send` but `!Sync`:
/// the SPSC contract for the underlying ring is compile-time enforced.
pub struct Mpsc2Producer<T, const CAP: usize, W: Waiter> {
    /// `Option` so `Drop` can move the ring producer out to close it before
    /// decrementing the live count.
    ring_producer: Option<Producer<T, CAP, NoopWaiter>>,
    inner: Arc<Inner<W>>,
    my_idx: usize,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send, const CAP: usize, W: Waiter> Mpsc2Producer<T, CAP, W> {
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
                if rp.should_notify_consumer(1) {
                    self.inner.fanin_waiter.wake();
                }
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
        if n > 0 && rp.should_notify_consumer(n) {
            self.inner.fanin_waiter.wake();
        }
        n
    }
}

impl<T: Send, const CAP: usize, W: BlockingWaiter> Mpsc2Producer<T, CAP, W> {
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

impl<T, const CAP: usize, W: Waiter> Drop for Mpsc2Producer<T, CAP, W> {
    fn drop(&mut self) {
        drop(self.ring_producer.take());
        let prev = self.inner.live_producers.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            self.inner.fanin_waiter.wake();
        }
    }
}

// ─── Consumer handle ──────────────────────────────────────────────────────

/// Single-consumer handle. `Send + !Sync`.
pub struct Mpsc2Consumer<T, const CAP: usize, W: Waiter> {
    inner: Arc<Inner<W>>,
    ring_consumers: Vec<Option<Consumer<T, CAP, NoopWaiter>>>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send, const CAP: usize, W: Waiter> Mpsc2Consumer<T, CAP, W> {
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

impl<T: Send, const CAP: usize, W: BlockingWaiter> Mpsc2Consumer<T, CAP, W> {
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

// ─── Async send / recv (AsyncWaiter only) ─────────────────────────────────

#[cfg(feature = "tokio")]
impl<T: Send + 'static, const CAP: usize, W: crate::waiter::AsyncWaiter + 'static>
    Mpsc2Producer<T, CAP, W>
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
    Mpsc2Consumer<T, CAP, W>
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
impl<T: Send, const CAP: usize> Mpsc2Producer<T, CAP, crate::waiter::NotifyWaiter> {
    #[inline]
    pub fn send_async_send<'a>(
        &'a mut self,
        value: T,
    ) -> impl std::future::Future<Output = ()> + Send + 'a {
        // Split borrow: `inner` (immut/Sync) and `ring_producer` (mut) are
        // disjoint fields, so their borrows can coexist inside the loop.
        let Mpsc2Producer {
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
                        if rp.should_notify_consumer(1) {
                            inner.fanin_waiter.wake();
                        }
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
impl<T: Send, const CAP: usize> Mpsc2Consumer<T, CAP, crate::waiter::NotifyWaiter> {
    #[inline]
    pub fn recv_async_send<'a>(
        &'a mut self,
    ) -> impl std::future::Future<Output = Result<T, Shutdown>> + Send + 'a {
        let Mpsc2Consumer {
            inner,
            ring_consumers,
            ..
        } = self;
        async move {
            let m = inner.m;
            loop {
                let notified = inner.fanin_waiter.inner.notified();
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
        let Mpsc2Consumer {
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

pub struct Mpsc2Shutdown<W: Waiter = ParkWaiter> {
    inner: Arc<Inner<W>>,
}

impl<W: Waiter> Clone for Mpsc2Shutdown<W> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<W: Waiter> Mpsc2Shutdown<W> {
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
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn single_producer_roundtrip() {
        let (mut ps, mut c, _sd) = Mpsc2::<u64>::new(1);
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

        let (ps, mut c, sd) = Mpsc2::<u64>::new(M);
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
        let (mut ps, _c, _sd) = Mpsc2::<u64, 1>::new(1);
        let mut p = ps.remove(0);
        assert!(p.try_send(1).is_ok());
        assert_eq!(p.try_send(2), Err(2));
    }

    #[test]
    fn drop_producers_shuts_consumer() {
        let (ps, mut c, _sd) = Mpsc2::<u64>::new(2);
        drop(ps);
        // No producers, no items → recv returns Err(Shutdown).
        assert_eq!(c.recv(), Err(Shutdown));
    }

    #[test]
    fn drop_producers_delivers_inflight_first() {
        let (mut ps, mut c, _sd) = Mpsc2::<u64, 16>::new(1);
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
        let (_ps, mut c, sd) = Mpsc2::<u64>::new(2);
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
            let (mut ps, _c, _sd) = Mpsc2::<Tracked>::new(2);
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
        let (ps, mut c, _sd) = Mpsc2::<u32>::new(M);
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
}
