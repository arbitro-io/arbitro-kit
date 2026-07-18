//! SPSC bounded ring buffer — split-handle variant (Ring v2).
//!
//! [`Ring::new`] returns a `(Producer, Consumer)` pair (rtrb /
//! `std::sync::mpsc` style). The handles are `Send` but neither `Clone`
//! nor `Sync`, so the SPSC contract is **compile-time enforced** — v1
//! relied on a call-site convention ("only one thread may call
//! `try_send`") that the type system never checked.
//!
//! ## What changed vs. v1 (audited findings)
//!
//! 1. **Waiters moved off the hot cursor lines.** v1 placed `not_full` on
//!    the same 64 B line as `head` (producer-written every op) and
//!    `not_empty` next to `tail` (consumer-written every op). Every
//!    `wake()` load from the peer therefore hit a line the owner kept
//!    dirtying — a coherence miss per op. v2 gives each field its own
//!    `CachePadded` region. Waiter state is written only on park/unpark
//!    (rare), so its line stays in the Shared MESI state and `wake()`
//!    becomes an L1 hit.
//!
//! 2. **Cached peer cursors moved into the handles.** `cached_tail` /
//!    `cached_head` are plain `usize` fields of `Producer` / `Consumer`.
//!    No `UnsafeCell` needed — the handle is uniquely owned, methods take
//!    `&mut self`. The Vyukov-style batching behavior is unchanged: the
//!    shared atomic is refreshed only when the private cache says
//!    full/empty.
//!
//! 3. **Disconnect detection** (new; v1 had none — dropping one side left
//!    the peer blocked forever). A `closed` flag on the shared struct is
//!    set by either handle's `Drop`:
//!    - `Consumer::recv` returns `Option<T>`: `None` once the producer is
//!      gone **and** the ring is drained. In-flight items are always
//!      delivered first.
//!    - `Producer::send` returns `Result<(), T>`: `Err(value)` when the
//!      consumer is gone and the ring is full. Items accepted while the
//!      ring still has room after a consumer drop are dropped by the
//!      shared struct's drain (documented trade-off: the fast path pays
//!      zero cost for disconnect detection — `closed` is only loaded on
//!      the would-block slow path).
//!
//! ## Ownership table (audit artifact)
//!
//! | field | resides | writer (freq) | reader (freq) | line state steady |
//! |---|---|---|---|---|
//! | `head` | Shared, line 0 | producer (every send, Release) | consumer (once per empty-batch refresh; parked predicate) | Modified in producer core; consumer misses once per batch |
//! | `tail` | Shared, line 1 | consumer (every recv, Release) | producer (once per full-batch refresh; parked predicate) | Modified in consumer core; producer misses once per batch |
//! | `not_full` | Shared, line 2 | producer (park/unpark only, rare) | consumer (`wake()` Relaxed load, every recv) | Shared → consumer `wake()` is L1 hit |
//! | `not_empty` | Shared, line 3 | consumer (park/unpark only, rare) | producer (`wake()` Relaxed load, every send) | Shared → producer `wake()` is L1 hit |
//! | `closed` | Shared, line 4 | either handle, once at drop | both sides, would-block slow paths only | Shared, cold |
//! | `slots[i]` | Shared, tail region | producer (one write per send) | consumer (one read per recv) | per-slot handoff, amortized over CAP |
//! | `Producer::cached_tail` | Producer handle | producer (per full-batch refresh) | producer (every send) | private, always L1 |
//! | `Producer::worker` | Producer handle | producer (first blocking call) | producer (blocking calls) | private |
//! | `Consumer::cached_head` | Consumer handle | consumer (per empty-batch refresh) | consumer (every recv) | private, always L1 |
//! | `Consumer::worker` | Consumer handle | consumer (first blocking call) | consumer (blocking calls) | private |
//!
//! ## Thread registration
//!
//! v1's explicit `set_producer` / `set_consumer` are gone. Each handle
//! registers its current thread on the waiter the first time a blocking
//! call runs (and re-registers if the handle has since moved to a
//! different thread — handles are `Send`). Async waiters ignore
//! registration entirely.
//!
//! ## Safety invariants
//!
//! - `head` monotonically increases, only the producer writes.
//! - `tail` monotonically increases, only the consumer writes.
//! - `slot[i & MASK]` is initialized iff `tail <= i < head` (wrapping-aware).
//! - Producer's Release store on `head` publishes the payload write.
//! - Consumer's Release store on `tail` publishes the "slot free" fact.
//! - `closed` is stored Release after the dropping side's final cursor
//!   store; the peer Acquire-loads it and then re-reads the cursor, so no
//!   in-flight item can be missed.
//!
//! ## Drop model
//!
//! Handle drop sets `closed` and wakes the peer. When the last `Arc`
//! reference drops, [`Ring`]'s own `Drop` drains any in-flight `T`
//! between `tail` and `head` (same as v1).

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::Arc;

// Under `--cfg loom` we swap the shared atomics for `loom::sync::atomic::*`
// so the loom model can explore possible reorderings around `head`/`tail`/
// `closed`. `UnsafeCell` stays on `std::cell::UnsafeCell` because migrating
// every `.get()` call site to `loom::cell::UnsafeCell::with(...)` would
// touch the whole file — Miri (run separately) validates cell access UB
// under the real memory model, and loom's contribution here is the atomic-
// ordering exploration on the cursors.
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::waiter::{AsyncWaiter, BlockingWaiter, ParkWaiter, Waiter};

/// Pads (and aligns) `T` to a 64-byte cache line so adjacent fields never
/// share a line. Local definition — the kit runtime stays dependency-free,
/// so we do not pull in `crossbeam_utils::CachePadded`.
#[repr(align(64))]
struct CachePadded<T>(T);

/// Error returned by [`Producer::try_send`].
#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// Ring is full. The rejected value is handed back.
    Full(T),
    /// Ring is full **and** the consumer has been dropped — no space will
    /// ever free up. The rejected value is handed back.
    Closed(T),
}

/// Error returned by [`Consumer::try_recv`].
#[derive(Debug, PartialEq, Eq)]
pub enum TryRecvError {
    /// Ring is empty right now; the producer may still send.
    Empty,
    /// Ring is empty and the producer has been dropped — no more items
    /// will ever arrive.
    Closed,
}

/// Shared state of the split-handle SPSC ring. Not directly constructible
/// by users — [`Ring::new`] returns the [`Producer`] / [`Consumer`]
/// handle pair, which hold this behind an `Arc`.
///
/// See the module docs for the field-ownership table.
#[repr(C)]
pub struct Ring<T, const CAP: usize, W: Waiter = ParkWaiter> {
    /// Monotonic write cursor. Producer writes with Release; consumer
    /// reads with Acquire. Own cache line (line 0).
    head: CachePadded<AtomicUsize>,
    /// Monotonic read cursor. Consumer writes with Release; producer
    /// reads with Acquire. Own cache line (line 1).
    tail: CachePadded<AtomicUsize>,
    /// Waiter on which the producer blocks when the ring is full.
    /// Own cache line (line 2) — written only on park/unpark, so the
    /// consumer's per-recv `wake()` load stays an L1 hit.
    not_full: CachePadded<W>,
    /// Waiter on which the consumer blocks when the ring is empty.
    /// Own cache line (line 3) — symmetric to `not_full`.
    not_empty: CachePadded<W>,
    /// Set (once) when either handle drops. Read only on would-block
    /// slow paths. Own cache line (line 4).
    closed: CachePadded<AtomicBool>,
    /// Slot storage. Producer writes `slot[head & MASK]` before its
    /// Release store on `head`; consumer reads `slot[tail & MASK]` after
    /// Acquire-loading `head`.
    slots: [UnsafeCell<MaybeUninit<T>>; CAP],
}

// Safety: all cross-thread state is atomics or `Waiter` (which is `Sync`).
// The slot cells are serialized by the head/tail cursors: the producer
// writes slot[head & MASK] before its Release store on `head`; the
// consumer reads slot[tail & MASK] only after Acquire-loading `head` and
// observing `head > tail`. Exclusive slot access is guaranteed by the
// unique ownership of the Producer/Consumer handles (enforced at compile
// time — they are neither `Clone` nor `Sync`).
unsafe impl<T: Send, const CAP: usize, W: Waiter> Send for Ring<T, CAP, W> {}
unsafe impl<T: Send, const CAP: usize, W: Waiter> Sync for Ring<T, CAP, W> {}

impl<T, const CAP: usize, W: Waiter> Ring<T, CAP, W> {
    const MASK: usize = CAP - 1;

    /// Create a fresh ring and return its unique handle pair.
    /// `CAP` must be a non-zero power of two.
    ///
    /// Deliberately named `new` even though it returns the handle pair
    /// rather than `Self` (same shape as `rtrb::RingBuffer::new`); the
    /// shared struct is an implementation detail users never hold.
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> (Producer<T, CAP, W>, Consumer<T, CAP, W>) {
        assert!(CAP > 0, "Ring CAP must be > 0");
        assert!(CAP.is_power_of_two(), "Ring CAP must be a power of two");

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
            Producer {
                shared: shared.clone(),
                cached_tail: 0,
                worker: None,
                _not_sync: PhantomData,
            },
            Consumer {
                shared,
                cached_head: 0,
                worker: None,
                _not_sync: PhantomData,
            },
        )
    }

    /// Approximate item count. Both cursors may advance between loads.
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

    /// Mark the ring closed and wake both sides. Called from handle drops.
    fn close(&self) {
        self.closed.0.store(true, Ordering::Release);
        // Wake both waiters: only the peer can actually be parked, but
        // waking both is idempotent and keeps this path branch-free.
        self.not_full.0.wake();
        self.not_empty.0.wake();
    }
}

impl<T, const CAP: usize, W: Waiter> Drop for Ring<T, CAP, W> {
    fn drop(&mut self) {
        // Exclusive access via &mut self — no other reference exists.
        // Under loom, `AtomicUsize::get_mut` is not available; a Relaxed
        // load is equivalent here because we hold `&mut self`.
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
            // Safety: slot[i & MASK] is initialized (tail <= i < head).
            unsafe {
                (*self.slots[i & Self::MASK].get()).assume_init_drop();
            }
            i = i.wrapping_add(1);
        }
    }
}

impl<T, const CAP: usize, W: Waiter> std::fmt::Debug for Ring<T, CAP, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ring")
            .field("capacity", &CAP)
            .field("len", &self.len())
            .field("closed", &self.is_closed())
            .finish()
    }
}

// ══════════════════════════════════════════════════════════════════════
// Producer handle
// ══════════════════════════════════════════════════════════════════════

/// Sending half of a [`Ring`]. Uniquely owned: `Send` but neither
/// `Clone` nor `Sync`, so exactly one thread can produce at a time —
/// the SPSC contract is enforced by the type system.
///
/// Dropping the producer closes the ring: the consumer drains remaining
/// items, then `recv` returns `None` / `try_recv` returns
/// [`TryRecvError::Closed`].
pub struct Producer<T, const CAP: usize, W: Waiter = ParkWaiter> {
    shared: Arc<Ring<T, CAP, W>>,
    /// Private cache of the consumer's `tail`. Refreshed from the shared
    /// atomic only when the cache says "full" (once per batch).
    cached_tail: usize,
    /// Thread registered on `not_full` for blocking sends. `None` until
    /// the first blocking call; re-registered if the handle moved.
    worker: Option<std::thread::ThreadId>,
    /// Opt out of `Sync` (a `&Producer` on another thread must not exist
    /// while methods run). `Cell<()>` is `Send + !Sync`, so the handle
    /// stays `Send`.
    _not_sync: PhantomData<Cell<()>>,
}

impl<T, const CAP: usize, W: Waiter> Producer<T, CAP, W> {
    #[inline]
    pub const fn capacity(&self) -> usize {
        CAP
    }

    /// Approximate number of items currently in the ring.
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

    /// `true` once the consumer has been dropped.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.shared.is_closed()
    }

    /// Non-blocking enqueue.
    ///
    /// - `Ok(())` — enqueued.
    /// - `Err(TrySendError::Full(v))` — ring full, consumer alive.
    /// - `Err(TrySendError::Closed(v))` — ring full and consumer dropped.
    ///
    /// Note: `closed` is checked only on the full (would-block) path, so
    /// the fast path pays nothing for disconnect detection. After a
    /// consumer drop, up to `CAP` sends may still succeed; those items
    /// are dropped by the shared drain when the last handle goes away.
    #[inline]
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        let shared = &*self.shared;
        // Producer owns `head` — Relaxed load is enough (no other writer).
        let head = shared.head.0.load(Ordering::Relaxed);

        // Fast path: consult the handle-private cache first. If the cache
        // says there is room, we skip the consumer's `tail` line entirely.
        if head.wrapping_sub(self.cached_tail) >= CAP {
            // Cache is stale (or ring is actually full). Refresh from the
            // shared atomic — Acquire syncs with the consumer's Release.
            self.cached_tail = shared.tail.0.load(Ordering::Acquire);
            if head.wrapping_sub(self.cached_tail) >= CAP {
                return Err(if shared.is_closed() {
                    TrySendError::Closed(value)
                } else {
                    TrySendError::Full(value)
                });
            }
        }

        // Safety: slot at `head & MASK` is free because
        // head - cached_tail < CAP and `tail` is monotonic (real tail >=
        // cached_tail). We own the write until the Release publishes it.
        unsafe {
            (*shared.slots[head & Ring::<T, CAP, W>::MASK].get()).write(value);
        }

        // Release publishes the slot write to any Acquire on `head`.
        shared.head.0.store(head.wrapping_add(1), Ordering::Release);
        // Wake a possibly-parked consumer. On the non-parked steady state
        // this is a single Relaxed load of a rarely-written line (L1 hit
        // after the CachePadded split — see module docs).
        shared.not_empty.0.wake();
        Ok(())
    }

    /// Bulk-send. Drains up to `min(items.len(), available)` from `items`
    /// into consecutive slots, then does **one** Release store on `head`
    /// and **one** `wake()`. This amortizes ordering + waker cost across
    /// the batch (N writes + 1 fence + 1 wake vs the N of each on the
    /// per-message path).
    ///
    /// Returns the number of items consumed from the tail of `items`
    /// (uses `items.pop()` order, so the caller sees items removed from
    /// the end).
    ///
    /// Correctness: identical Store→Release publish rule as `try_send` —
    /// slot writes happen-before the final `head.store(Release)`, so the
    /// consumer's `head.load(Acquire)` synchronizes with the whole burst.
    #[inline]
    pub fn try_send_bulk(&mut self, items: &mut Vec<T>) -> usize {
        if items.is_empty() {
            return 0;
        }
        let shared = &*self.shared;
        let head0 = shared.head.0.load(Ordering::Relaxed);

        // Refresh the cursor cache once per batch, not per item.
        let mut used = head0.wrapping_sub(self.cached_tail);
        if used >= CAP {
            self.cached_tail = shared.tail.0.load(Ordering::Acquire);
            used = head0.wrapping_sub(self.cached_tail);
        }
        let avail = CAP.saturating_sub(used);
        if avail == 0 {
            return 0;
        }

        let take = items.len().min(avail);
        let mut h = head0;
        for _ in 0..take {
            // Safety: SPSC producer owns `head` until the final Release
            // store below publishes the whole burst. `cached_tail` proves
            // the range `[head0 .. head0 + take)` is unoccupied.
            let v = items.pop().expect("checked take <= len");
            unsafe {
                (*shared.slots[h & Ring::<T, CAP, W>::MASK].get()).write(v);
            }
            h = h.wrapping_add(1);
        }

        // One Release store publishes ALL slot writes at once.
        shared.head.0.store(h, Ordering::Release);
        // One wake — the consumer, if parked, wakes and drains everything.
        shared.not_empty.0.wake();
        take
    }

    /// UNSOUND as a *sole* wake gate — kept for reference; do NOT reintroduce
    /// as the only wake source (this was R1).
    ///
    /// It stores `head` (Release) then loads `tail` (Acquire) with a fence on
    /// only one side — a store-buffering shape where both the producer and a
    /// re-arming consumer can read stale simultaneously, so the gate returns
    /// `false` while the consumer parks with an item pending: the only wake is
    /// skipped and lost (self-sustaining, not self-healing). The Mpsc now wakes
    /// its fan-in waiter unconditionally and relies on the waiter's own hardened
    /// gate (armed-counter / parked-flag) for elision.
    #[allow(dead_code)]
    #[inline]
    pub fn should_notify_consumer(&mut self, n_pushed: usize) -> bool {
        let shared = &*self.shared;
        self.cached_tail = shared.tail.0.load(Ordering::Acquire);
        let head = shared.head.0.load(Ordering::Relaxed);
        head.wrapping_sub(self.cached_tail) == n_pushed
    }

    /// Register the current thread on the producer waiter if it is not
    /// already the registered one. Handles are `Send`, so a handle may
    /// legitimately block from a different thread than last time.
    #[inline]
    fn register(&mut self) {
        let current = std::thread::current();
        if self.worker != Some(current.id()) {
            self.worker = Some(current.id());
            self.shared.not_full.0.set_worker(current);
        }
    }
}

impl<T, const CAP: usize, W: BlockingWaiter> Producer<T, CAP, W> {
    /// Blocking enqueue. Parks until the ring has space.
    ///
    /// Returns `Err(value)` if the consumer has been dropped and the ring
    /// is full (the value could never be delivered).
    #[inline]
    pub fn send(&mut self, value: T) -> Result<(), T> {
        let mut value = value;
        loop {
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Closed(v)) => return Err(v),
                Err(TrySendError::Full(v)) => value = v,
            }
            self.register();
            let shared = &*self.shared;
            // Predicate reloads the real `tail` (via `is_full`, Acquire)
            // and the `closed` flag. This closes the Dekker race with the
            // consumer: the waiter re-evaluates after its SeqCst park
            // announcement.
            shared
                .not_full
                .0
                .wait_until(|| !shared.is_full() || shared.is_closed());
        }
    }
}

impl<T: Send, const CAP: usize, W: AsyncWaiter> Producer<T, CAP, W> {
    /// Async enqueue. Awaits capacity, then enqueues.
    ///
    /// Returns `Err(value)` if the consumer has been dropped and the ring
    /// is full.
    pub async fn send_async(&mut self, value: T) -> Result<(), T> {
        let mut value = value;
        loop {
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Closed(v)) => return Err(v),
                Err(TrySendError::Full(v)) => value = v,
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

impl<T, const CAP: usize, W: Waiter> Drop for Producer<T, CAP, W> {
    fn drop(&mut self) {
        self.shared.close();
    }
}

impl<T, const CAP: usize, W: Waiter> std::fmt::Debug for Producer<T, CAP, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer").field("ring", &*self.shared).finish()
    }
}

// ══════════════════════════════════════════════════════════════════════
// Consumer handle
// ══════════════════════════════════════════════════════════════════════

/// Receiving half of a [`Ring`]. Uniquely owned: `Send` but neither
/// `Clone` nor `Sync` — the SPSC contract is enforced by the type system.
///
/// Dropping the consumer closes the ring: producer `send` returns
/// `Err(value)` once the ring is full, `try_send` returns
/// [`TrySendError::Closed`].
pub struct Consumer<T, const CAP: usize, W: Waiter = ParkWaiter> {
    shared: Arc<Ring<T, CAP, W>>,
    /// Private cache of the producer's `head`. Refreshed from the shared
    /// atomic only when the cache says "empty" (once per batch).
    cached_head: usize,
    /// Thread registered on `not_empty` for blocking recvs.
    worker: Option<std::thread::ThreadId>,
    /// Opt out of `Sync`; see [`Producer`].
    _not_sync: PhantomData<Cell<()>>,
}

impl<T, const CAP: usize, W: Waiter> Consumer<T, CAP, W> {
    #[inline]
    pub const fn capacity(&self) -> usize {
        CAP
    }

    /// Approximate number of items currently in the ring.
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

    /// `true` once the producer has been dropped. Items may still be
    /// in flight — keep draining until `try_recv` says `Closed`.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.shared.is_closed()
    }

    /// Non-blocking dequeue.
    ///
    /// - `Ok(v)` — got an item.
    /// - `Err(TryRecvError::Empty)` — nothing right now, producer alive.
    /// - `Err(TryRecvError::Closed)` — ring drained and producer dropped.
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let shared = &*self.shared;
        // Consumer owns `tail` — Relaxed is enough.
        let tail = shared.tail.0.load(Ordering::Relaxed);

        // Fast path: handle-private cache of `head`. Skip the shared load
        // if we already know there is data.
        if self.cached_head == tail {
            // Cache is stale (or ring is empty). Refresh — Acquire syncs
            // with the producer's Release on `head` and its slot write.
            self.cached_head = shared.head.0.load(Ordering::Acquire);
            if self.cached_head == tail {
                if !shared.is_closed() {
                    return Err(TryRecvError::Empty);
                }
                // Closed. `closed` is stored (Release) after the
                // producer's final `head` store, so re-reading `head`
                // after the Acquire on `closed` cannot miss an in-flight
                // item published just before the drop.
                self.cached_head = shared.head.0.load(Ordering::Acquire);
                if self.cached_head == tail {
                    return Err(TryRecvError::Closed);
                }
            }
        }

        // Safety: the producer's Release on `head` published
        // slot[tail & MASK]; cached_head > tail guarantees it is initialized.
        let v = unsafe {
            (*shared.slots[tail & Ring::<T, CAP, W>::MASK].get()).assume_init_read()
        };

        // Release publishes "slot at tail is free" to a producer that
        // Acquires `tail`.
        shared.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        // Wake a possibly-parked producer (L1 hit on the steady state,
        // see module docs).
        shared.not_full.0.wake();
        Ok(v)
    }

    /// Drain everything visible; one Acquire head-load, one Release
    /// tail-store, one wake — regardless of item count. Does not block or
    /// signal `Closed`.
    #[inline]
    pub fn drain<F: FnMut(T)>(&mut self, mut f: F) -> usize {
        let shared = &*self.shared;
        let start = shared.tail.0.load(Ordering::Relaxed);
        let head = shared.head.0.load(Ordering::Acquire);
        if start == head {
            return 0;
        }

        // R2: a panic in `f` must still publish the consumed `tail` (Release).
        // Each slot is `assume_init_read` (moved out) BEFORE `f` runs; without
        // this guard an unwind leaves `shared.tail` at `start`, so `Ring::drop`
        // would `assume_init_drop` the already-moved slots again — double-drop
        // (with `T` carrying an `Arc`/`Bytes`: refcount corruption).
        struct Commit<'a> {
            slot: &'a AtomicUsize,
            tail: usize,
        }
        impl Drop for Commit<'_> {
            #[inline]
            fn drop(&mut self) {
                self.slot.store(self.tail, Ordering::Release);
            }
        }

        let mut c = Commit {
            slot: &shared.tail.0,
            tail: start,
        };
        while c.tail != head {
            let v = unsafe {
                (*shared.slots[c.tail & Ring::<T, CAP, W>::MASK].get()).assume_init_read()
            };
            // Advance BEFORE `f`, so a panic leaves `c.tail` past the moved slot.
            c.tail = c.tail.wrapping_add(1);
            f(v);
        }
        let consumed = c.tail.wrapping_sub(start);
        drop(c); // publishes the final tail (Release) on the normal path
        shared.not_full.0.wake();
        self.cached_head = head;
        consumed
    }

    /// See [`Producer::register`].
    #[inline]
    fn register(&mut self) {
        let current = std::thread::current();
        if self.worker != Some(current.id()) {
            self.worker = Some(current.id());
            self.shared.not_empty.0.set_worker(current);
        }
    }
}

impl<T, const CAP: usize, W: BlockingWaiter> Consumer<T, CAP, W> {
    /// Blocking dequeue. Parks until an item is available.
    ///
    /// Returns `None` once the producer has been dropped **and** every
    /// in-flight item has been drained.
    #[inline]
    pub fn recv(&mut self) -> Option<T> {
        loop {
            match self.try_recv() {
                Ok(v) => return Some(v),
                Err(TryRecvError::Closed) => return None,
                Err(TryRecvError::Empty) => {}
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

impl<T: Send, const CAP: usize, W: AsyncWaiter> Consumer<T, CAP, W> {
    /// Async dequeue. Awaits an item, then takes it.
    ///
    /// Returns `None` once the producer has been dropped and the ring is
    /// drained.
    pub async fn recv_async(&mut self) -> Option<T> {
        loop {
            match self.try_recv() {
                Ok(v) => return Some(v),
                Err(TryRecvError::Closed) => return None,
                Err(TryRecvError::Empty) => {}
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

impl<T, const CAP: usize, W: Waiter> Drop for Consumer<T, CAP, W> {
    fn drop(&mut self) {
        self.shared.close();
    }
}

impl<T, const CAP: usize, W: Waiter> std::fmt::Debug for Consumer<T, CAP, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer").field("ring", &*self.shared).finish()
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
        assert_send::<Producer<u32, 8>>();
        assert_send::<Consumer<u32, 8>>();
    }

    #[test]
    fn single_thread_basic() {
        let (mut tx, mut rx) = Ring::<u32, 8>::new();
        assert!(rx.is_empty());
        assert_eq!(tx.capacity(), 8);
        assert_eq!(rx.capacity(), 8);
        for i in 0..8 {
            assert!(tx.try_send(i).is_ok());
        }
        assert!(tx.is_full());
        assert_eq!(tx.try_send(999), Err(TrySendError::Full(999)));
        for i in 0..8 {
            assert_eq!(rx.try_recv(), Ok(i));
        }
        assert!(rx.is_empty());
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn wraparound() {
        let (mut tx, mut rx) = Ring::<u32, 4>::new();
        for i in 0..100 {
            assert!(tx.try_send(i).is_ok());
            assert_eq!(rx.try_recv(), Ok(i));
        }
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn non_pow2_panics() {
        let _ = Ring::<u32, 7>::new();
    }

    #[test]
    fn producer_drop_disconnects_consumer() {
        let (mut tx, mut rx) = Ring::<u32, 8>::new();
        for i in 0..3 {
            tx.try_send(i).unwrap();
        }
        drop(tx);
        // In-flight items are delivered before the disconnect surfaces.
        assert_eq!(rx.recv(), Some(0));
        assert_eq!(rx.recv(), Some(1));
        assert_eq!(rx.recv(), Some(2));
        assert_eq!(rx.recv(), None);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    }

    #[test]
    fn consumer_drop_disconnects_producer() {
        let (mut tx, rx) = Ring::<u32, 2>::new();
        drop(rx);
        // Room remains: sends still succeed (documented; drained on Drop).
        assert!(tx.try_send(1).is_ok());
        assert!(tx.try_send(2).is_ok());
        // Full + closed: surfaced as Closed, value handed back.
        assert_eq!(tx.try_send(3), Err(TrySendError::Closed(3)));
        assert_eq!(tx.send(4), Err(4));
    }

    #[test]
    fn producer_drop_unblocks_parked_consumer() {
        let (tx, mut rx) = Ring::<u32, 4>::new();
        let h = std::thread::spawn(move || rx.recv());
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(tx);
        assert_eq!(h.join().unwrap(), None);
    }

    #[test]
    fn consumer_drop_unblocks_parked_producer() {
        let (mut tx, rx) = Ring::<u32, 2>::new();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        let h = std::thread::spawn(move || tx.send(3));
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(rx);
        assert_eq!(h.join().unwrap(), Err(3));
    }

    #[test]
    fn cross_thread_blocking() {
        let (mut tx, mut rx) = Ring::<u64, 16>::new();
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
    fn cross_thread_backpressure() {
        // CAP small + slow consumer: producer MUST park on not_full.
        const CAP: usize = 4;
        const N: u64 = 500;
        let (mut tx, mut rx) = Ring::<u64, CAP>::new();
        let consumer = std::thread::spawn(move || {
            let mut got = Vec::with_capacity(N as usize);
            for i in 0..N {
                got.push(rx.recv().unwrap());
                if i % 5 == 0 {
                    std::thread::sleep(std::time::Duration::from_micros(20));
                }
            }
            (got, rx)
        });
        for i in 0..N {
            tx.send(i).unwrap();
        }
        let (got, rx) = consumer.join().unwrap();
        assert_eq!(got, (0..N).collect::<Vec<_>>());
        assert!(rx.is_empty());
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
            let (mut tx, rx) = Ring::<Tracked, 8>::new();
            for _ in 0..5 {
                assert!(tx.try_send(Tracked(drops.clone())).is_ok());
            }
            drop(rx);
            drop(tx);
        }
        assert_eq!(drops.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn zero_sized_type() {
        let (mut tx, mut rx) = Ring::<(), 8>::new();
        for _ in 0..8 {
            tx.try_send(()).unwrap();
        }
        assert_eq!(tx.try_send(()), Err(TrySendError::Full(())));
        for _ in 0..8 {
            assert_eq!(rx.try_recv(), Ok(()));
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn cross_thread_high_volume() {
        const CAP: usize = 128;
        const N: u64 = 100_000;
        let (mut tx, mut rx) = Ring::<u64, CAP>::new();
        let consumer = std::thread::spawn(move || {
            let mut sum: u64 = 0;
            for _ in 0..N {
                sum = sum.wrapping_add(rx.recv().unwrap());
            }
            (sum, rx)
        });
        for i in 0..N {
            tx.send(i).unwrap();
        }
        let (got, rx) = consumer.join().unwrap();
        let expected: u64 = (0..N).fold(0u64, |a, b| a.wrapping_add(b));
        assert_eq!(got, expected);
        assert!(rx.is_empty());
    }

    #[test]
    fn handle_moves_between_threads() {
        // First blocking call happens on thread A, handle then moves to
        // thread B — the lazy registration must follow the handle.
        let (mut tx, mut rx) = Ring::<u64, 4>::new();
        tx.send(1).unwrap();
        assert_eq!(rx.recv(), Some(1));
        let h = std::thread::spawn(move || {
            let v = rx.recv();
            (v, rx)
        });
        tx.send(2).unwrap();
        let (v, mut rx) = h.join().unwrap();
        assert_eq!(v, Some(2));
        tx.send(3).unwrap();
        assert_eq!(rx.recv(), Some(3));
    }

    // ── Async (tokio) ────────────────────────────────────────────────

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn cross_task_basic_notify() {
        use crate::waiter::NotifyWaiter;
        let (mut tx, mut rx) = Ring::<u64, 16, NotifyWaiter>::new();
        let producer = async {
            for i in 0..1000u64 {
                tx.send_async(i).await.unwrap();
            }
        };
        let consumer = async {
            let mut sum = 0u64;
            for _ in 0..1000 {
                sum += rx.recv_async().await.unwrap();
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
        let (mut tx, mut rx) = Ring::<u32, 4, NotifyWaiter>::new();
        for i in 0..100 {
            tx.send_async(i).await.unwrap();
            assert_eq!(rx.recv_async().await, Some(i));
        }
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn producer_drop_disconnects_async_consumer() {
        use crate::waiter::NotifyWaiter;
        let (mut tx, mut rx) = Ring::<u32, 8, NotifyWaiter>::new();
        tx.send_async(7).await.unwrap();
        drop(tx);
        assert_eq!(rx.recv_async().await, Some(7));
        assert_eq!(rx.recv_async().await, None);
    }
}
