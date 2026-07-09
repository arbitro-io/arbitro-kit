//! SPSC bounded ring buffer — split-handle variant (Spsc2 v2).
//!
//! [`Spsc2::new`] returns a `(ProducerSpsc2, ConsumerSpsc2)` pair.
//! The handles are `Send` but neither `Clone` nor `Sync`, so the SPSC contract
//! is compile-time enforced.

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::Arc;

#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::waiter::{AsyncWaiter, BlockingWaiter, ParkWaiter, Waiter};

#[repr(align(64))]
struct CachePadded<T>(T);

/// Error returned by [`ProducerSpsc2::try_send`].
#[derive(Debug, PartialEq, Eq)]
pub enum TrySendErrorSpsc2<T> {
    /// Ring is full. The rejected value is handed back.
    Full(T),
    /// Ring is full **and** the consumer has been dropped.
    Closed(T),
}

/// Error returned by [`ConsumerSpsc2::try_recv`].
#[derive(Debug, PartialEq, Eq)]
pub enum TryRecvErrorSpsc2 {
    /// Ring is empty right now.
    Empty,
    /// Ring is empty and the producer has been dropped.
    Closed,
}

/// Shared state of the split-handle SPSC ring.
#[repr(C)]
pub struct Spsc2<T, const CAP: usize, W: Waiter = ParkWaiter> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    not_full: CachePadded<W>,
    not_empty: CachePadded<W>,
    closed: CachePadded<AtomicBool>,
    slots: [UnsafeCell<MaybeUninit<T>>; CAP],
}

unsafe impl<T: Send, const CAP: usize, W: Waiter> Send for Spsc2<T, CAP, W> {}
unsafe impl<T: Send, const CAP: usize, W: Waiter> Sync for Spsc2<T, CAP, W> {}

impl<T, const CAP: usize, W: Waiter> Spsc2<T, CAP, W> {
    const MASK: usize = CAP - 1;

    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> (ProducerSpsc2<T, CAP, W>, ConsumerSpsc2<T, CAP, W>) {
        assert!(CAP > 0, "Spsc2 CAP must be > 0");
        assert!(CAP.is_power_of_two(), "Spsc2 CAP must be a power of two");

        let slots: [UnsafeCell<MaybeUninit<T>>; CAP] =
            std::array::from_fn(|_| UnsafeCell::new(MaybeUninit::uninit()));

        let shared = Arc::new(Self {
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            not_full: CachePadded(W::default()),
            not_empty: CachePadded(W::default()),
            closed: CachePadded(AtomicBool::new(false)),
            slots,
        });

        (
            ProducerSpsc2 {
                shared: shared.clone(),
                cached_tail: 0,
                worker: None,
                _not_sync: PhantomData,
            },
            ConsumerSpsc2 {
                shared,
                cached_head: 0,
                worker: None,
                _not_sync: PhantomData,
            },
        )
    }

    #[inline]
    fn len(&self) -> usize {
        let h = self.head.0.load(Ordering::Acquire);
        let t = self.tail.0.load(Ordering::Acquire);
        h.wrapping_sub(t)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.len() >= CAP
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.closed.0.load(Ordering::Acquire)
    }

    fn close(&self) {
        self.closed.0.store(true, Ordering::Release);
        self.not_full.0.wake();
        self.not_empty.0.wake();
    }
}

impl<T, const CAP: usize, W: Waiter> Drop for Spsc2<T, CAP, W> {
    fn drop(&mut self) {
        #[cfg(not(loom))]
        let head = *self.head.0.get_mut();
        #[cfg(not(loom))]
        let tail = *self.tail.0.get_mut();
        #[cfg(loom)]
        let head = self.head.0.load(Ordering::Relaxed);
        #[cfg(loom)]
        let tail = self.tail.0.load(Ordering::Relaxed);
        let mut i = tail;
        while i != head {
            unsafe {
                (*self.slots[i & Self::MASK].get()).assume_init_drop();
            }
            i = i.wrapping_add(1);
        }
    }
}

impl<T, const CAP: usize, W: Waiter> std::fmt::Debug for Spsc2<T, CAP, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spsc2")
            .field("capacity", &CAP)
            .field("len", &self.len())
            .field("closed", &self.is_closed())
            .finish()
    }
}

// ══════════════════════════════════════════════════════════════════════
// ProducerSpsc2 handle
// ══════════════════════════════════════════════════════════════════════

pub struct ProducerSpsc2<T, const CAP: usize, W: Waiter = ParkWaiter> {
    shared: Arc<Spsc2<T, CAP, W>>,
    cached_tail: usize,
    worker: Option<std::thread::ThreadId>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T, const CAP: usize, W: Waiter> ProducerSpsc2<T, CAP, W> {
    #[inline]
    pub const fn capacity(&self) -> usize {
        CAP
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.shared.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shared.is_empty()
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.shared.is_full()
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.shared.is_closed()
    }

    #[inline]
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendErrorSpsc2<T>> {
        let shared = &*self.shared;
        let head = shared.head.0.load(Ordering::Relaxed);

        if head.wrapping_sub(self.cached_tail) >= CAP {
            self.cached_tail = shared.tail.0.load(Ordering::Acquire);
            if head.wrapping_sub(self.cached_tail) >= CAP {
                return Err(if shared.is_closed() {
                    TrySendErrorSpsc2::Closed(value)
                } else {
                    TrySendErrorSpsc2::Full(value)
                });
            }
        }

        unsafe {
            (*shared.slots[head & Spsc2::<T, CAP, W>::MASK].get()).write(value);
        }

        shared.head.0.store(head.wrapping_add(1), Ordering::Release);
        shared.not_empty.0.wake();
        Ok(())
    }

    #[inline]
    fn register(&mut self) {
        let current = std::thread::current();
        if self.worker != Some(current.id()) {
            self.worker = Some(current.id());
            self.shared.not_full.0.set_worker(current);
        }
    }
}

impl<T, const CAP: usize, W: BlockingWaiter> ProducerSpsc2<T, CAP, W> {
    #[inline]
    pub fn send(&mut self, value: T) -> Result<(), T> {
        let mut value = value;
        loop {
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendErrorSpsc2::Closed(v)) => return Err(v),
                Err(TrySendErrorSpsc2::Full(v)) => value = v,
            }
            self.register();
            let shared = &*self.shared;
            shared
                .not_full
                .0
                .wait_until(|| !shared.is_full() || shared.is_closed());
        }
    }
}

impl<T: Send, const CAP: usize, W: AsyncWaiter> ProducerSpsc2<T, CAP, W> {
    pub async fn send_async(&mut self, value: T) -> Result<(), T> {
        let mut value = value;
        loop {
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendErrorSpsc2::Closed(v)) => return Err(v),
                Err(TrySendErrorSpsc2::Full(v)) => value = v,
            }
            let shared = &*self.shared;
            shared
                .not_full
                .0
                .wait_until(|| !shared.is_full() || shared.is_closed())
                .await;
        }
    }
}

impl<T, const CAP: usize, W: Waiter> Drop for ProducerSpsc2<T, CAP, W> {
    fn drop(&mut self) {
        self.shared.close();
    }
}

impl<T, const CAP: usize, W: Waiter> std::fmt::Debug for ProducerSpsc2<T, CAP, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerSpsc2").field("ring", &*self.shared).finish()
    }
}

// ══════════════════════════════════════════════════════════════════════
// ConsumerSpsc2 handle
// ══════════════════════════════════════════════════════════════════════

pub struct ConsumerSpsc2<T, const CAP: usize, W: Waiter = ParkWaiter> {
    shared: Arc<Spsc2<T, CAP, W>>,
    cached_head: usize,
    worker: Option<std::thread::ThreadId>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T, const CAP: usize, W: Waiter> ConsumerSpsc2<T, CAP, W> {
    #[inline]
    pub const fn capacity(&self) -> usize {
        CAP
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.shared.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shared.is_empty()
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.shared.is_full()
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.shared.is_closed()
    }

    #[inline]
    pub fn try_recv(&mut self) -> Result<T, TryRecvErrorSpsc2> {
        let shared = &*self.shared;
        let tail = shared.tail.0.load(Ordering::Relaxed);

        if self.cached_head == tail {
            self.cached_head = shared.head.0.load(Ordering::Acquire);
            if self.cached_head == tail {
                if !shared.is_closed() {
                    return Err(TryRecvErrorSpsc2::Empty);
                }
                self.cached_head = shared.head.0.load(Ordering::Acquire);
                if self.cached_head == tail {
                    return Err(TryRecvErrorSpsc2::Closed);
                }
            }
        }

        let v = unsafe {
            (*shared.slots[tail & Spsc2::<T, CAP, W>::MASK].get()).assume_init_read()
        };

        shared.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        shared.not_full.0.wake();
        Ok(v)
    }

    #[inline]
    fn register(&mut self) {
        let current = std::thread::current();
        if self.worker != Some(current.id()) {
            self.worker = Some(current.id());
            self.shared.not_empty.0.set_worker(current);
        }
    }
}

impl<T, const CAP: usize, W: BlockingWaiter> ConsumerSpsc2<T, CAP, W> {
    #[inline]
    pub fn recv(&mut self) -> Option<T> {
        loop {
            match self.try_recv() {
                Ok(v) => return Some(v),
                Err(TryRecvErrorSpsc2::Closed) => return None,
                Err(TryRecvErrorSpsc2::Empty) => {}
            }
            self.register();
            let shared = &*self.shared;
            shared
                .not_empty
                .0
                .wait_until(|| !shared.is_empty() || shared.is_closed());
        }
    }
}

impl<T: Send, const CAP: usize, W: AsyncWaiter> ConsumerSpsc2<T, CAP, W> {
    pub async fn recv_async(&mut self) -> Option<T> {
        loop {
            match self.try_recv() {
                Ok(v) => return Some(v),
                Err(TryRecvErrorSpsc2::Closed) => return None,
                Err(TryRecvErrorSpsc2::Empty) => {}
            }
            let shared = &*self.shared;
            shared
                .not_empty
                .0
                .wait_until(|| !shared.is_empty() || shared.is_closed())
                .await;
        }
    }
}

impl<T, const CAP: usize, W: Waiter> Drop for ConsumerSpsc2<T, CAP, W> {
    fn drop(&mut self) {
        self.shared.close();
    }
}

impl<T, const CAP: usize, W: Waiter> std::fmt::Debug for ConsumerSpsc2<T, CAP, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerSpsc2").field("ring", &*self.shared).finish()
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
    fn handles_are_send() {
        fn assert_send<X: Send>() {}
        assert_send::<ProducerSpsc2<u32, 8>>();
        assert_send::<ConsumerSpsc2<u32, 8>>();
    }

    #[test]
    fn single_thread_basic() {
        let (mut tx, mut rx) = Spsc2::<u32, 8>::new();
        assert!(rx.is_empty());
        assert_eq!(tx.capacity(), 8);
        assert_eq!(rx.capacity(), 8);
        for i in 0..8 {
            assert!(tx.try_send(i).is_ok());
        }
        assert!(tx.is_full());
        assert_eq!(tx.try_send(999), Err(TrySendErrorSpsc2::Full(999)));
        for i in 0..8 {
            assert_eq!(rx.try_recv(), Ok(i));
        }
        assert!(rx.is_empty());
        assert_eq!(rx.try_recv(), Err(TryRecvErrorSpsc2::Empty));
    }

    #[test]
    fn wraparound() {
        let (mut tx, mut rx) = Spsc2::<u32, 4>::new();
        for i in 0..100 {
            assert!(tx.try_send(i).is_ok());
            assert_eq!(rx.try_recv(), Ok(i));
        }
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn non_pow2_panics() {
        let _ = Spsc2::<u32, 7>::new();
    }

    #[test]
    fn producer_drop_disconnects_consumer() {
        let (mut tx, mut rx) = Spsc2::<u32, 8>::new();
        for i in 0..3 {
            tx.try_send(i).unwrap();
        }
        drop(tx);
        assert_eq!(rx.recv(), Some(0));
        assert_eq!(rx.recv(), Some(1));
        assert_eq!(rx.recv(), Some(2));
        assert_eq!(rx.recv(), None);
        assert_eq!(rx.try_recv(), Err(TryRecvErrorSpsc2::Closed));
    }

    #[test]
    fn consumer_drop_disconnects_producer() {
        let (mut tx, rx) = Spsc2::<u32, 2>::new();
        drop(rx);
        assert!(tx.try_send(1).is_ok());
        assert!(tx.try_send(2).is_ok());
        assert_eq!(tx.try_send(3), Err(TrySendErrorSpsc2::Closed(3)));
        assert_eq!(tx.send(4), Err(4));
    }

    #[test]
    fn producer_drop_unblocks_parked_consumer() {
        let (tx, mut rx) = Spsc2::<u32, 4>::new();
        let h = std::thread::spawn(move || rx.recv());
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(tx);
        assert_eq!(h.join().unwrap(), None);
    }

    #[test]
    fn consumer_drop_unblocks_parked_producer() {
        let (mut tx, rx) = Spsc2::<u32, 2>::new();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        let h = std::thread::spawn(move || tx.send(3));
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(rx);
        assert_eq!(h.join().unwrap(), Err(3));
    }

    #[test]
    fn cross_thread_blocking() {
        let (mut tx, mut rx) = Spsc2::<u64, 16>::new();
        let h = std::thread::spawn(move || {
            let mut sum = 0u64;
            for _ in 0..1000 {
                sum += rx.recv().unwrap();
            }
            sum
        });
        for i in 0..1000u64 {
            tx.send(i).unwrap();
        }
        assert_eq!(h.join().unwrap(), (0..1000u64).sum());
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
            let (mut tx, rx) = Spsc2::<Tracked, 8>::new();
            for _ in 0..5 {
                assert!(tx.try_send(Tracked(drops.clone())).is_ok());
            }
            drop(rx);
            drop(tx);
        }
        assert_eq!(drops.load(Ordering::Relaxed), 5);
    }
}
