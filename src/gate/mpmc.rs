//! M:N multi-producer / multi-consumer bounded channel, sharded.
//!
//! [`Mpmc<T, RING_CAP>`] wires `M` producers to `N` consumers through `N`
//! independent shards. Each `(producer, shard)` pair owns a dedicated
//! **SPSC mini-ring of `RING_CAP` slots**, not a single slot — so a bursting
//! producer can enqueue up to `RING_CAP` items before stalling, and the
//! consumer can drain the whole ring in one park/unpark cycle.
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
//!   ├── full_set: SignalSet         (M bits "ring p has data" + bit 63 shutdown)
//!   ├── rings[0..M]: PRing          (each is SPSC, RING_CAP slots)
//!   └── drained by consumer s
//! ```
//!
//! Each `(producer p, shard s)` pair owns `shards[s].rings[p]`, a classic
//! SPSC ring with `head` / `tail` cursors. The producer is the sole writer
//! of `head`, the consumer is the sole writer of `tail`.
//!
//! ## Hot-path cost per message
//!
//! Producer `try_send(v)`:
//! 1. Scan shards from cursor, pick first whose `rings[p]` isn't full.
//! 2. Write `slots[head & MASK] = v`.
//! 3. `head.store(h+1, Release)` — publishes slot.
//! 4. `full_set.release(my_id)` — `fetch_or` of bit `p` on the shard's
//!    SignalSet (wakes parked consumer if bit was clear).
//!
//! Consumer `recv_batch(f)`:
//! 1. Scan set bits in `state()`; for each, read `head`/`tail` and drain
//!    `[tail, head)` via `f`.
//! 2. **Bits are NOT cleared during drain.** This avoids a Dekker race
//!    with producer's `fetch_or` and — critically — amortizes the atomic
//!    over the whole ring burst.
//! 3. Bits are cleared only in the **park path** (recv_batch about to
//!    `acquire_any`): atomically clear all bits, SeqCst fence, recheck
//!    every ring; if a producer raced, re-set the bit and loop; else park.
//!
//! ## Why mini-ring beats 1-slot sharded design
//!
//! The previous design had 1 slot per `(p, s)`. Every send paid a full
//! `fetch_or`+`lock_mask`+producer-gate wake cycle. In bursts, producer
//! stalled after 1 send until consumer drained. This redesign:
//!
//! - **Burst capacity**: producer writes RING_CAP items without
//!   consumer coordination beyond the initial bit set.
//! - **Drain amortization**: consumer `lock_mask` is called at most once
//!   per park cycle (not per item). Producer-gate wake is called at most
//!   once per bit drained, not once per item.
//! - **Locality**: ring slots are contiguous; prefetch works.
//!
//! The cost: `M × N × RING_CAP × sizeof(T)` bytes of backing storage.
//! With defaults `RING_CAP = 64` and `T = u64`, `M = N = 8` → 32 KiB.
//!
//! ## Adaptive routing
//!
//! Producers don't pin to a fixed shard. On every `try_send` / `send`,
//! the producer scans shards starting from a round-robin cursor and
//! picks the **first shard whose ring for this producer isn't full**.
//! Cost is one `tail.load(Acquire)` per shard scanned — no CAS. The
//! cursor advances on success so consecutive sends fan out.
//!
//! ## Backpressure
//!
//! If every shard's ring for this producer is full, the producer parks
//! on its own [`Signal`] (`producer_gates[p]`). Any consumer that
//! advances `tail` on this producer's ring wakes it.
//!
//! ## Limits
//!
//! - `M ≤ 63` producers (bit 63 of every shard's `SignalSet` is reserved
//!   for [`MpmcShutdown`]).
//! - `N ≥ 1`, no upper bound (runtime-sized).
//! - `M == 0` or `N == 0` is rejected.
//! - `RING_CAP` must be a power of two ≥ 1.

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::gate::hub::Shutdown;
use crate::gate::{Signal, SignalId, SignalSet, MAX_GATES};

/// Bit 63 of every shard's `SignalSet` is reserved for shutdown.
const SHUTDOWN_BIT: u8 = (MAX_GATES - 1) as u8;

/// Maximum number of producers in an [`Mpmc`]. One bit per shard is
/// reserved for [`MpmcShutdown`], so this is `MAX_GATES - 1 = 63`.
pub const MAX_MPMC_PRODUCERS: usize = MAX_GATES - 1;

// ─── Per-(producer, shard) mini-Ring (SPSC) ───────────────────────────────

/// SPSC ring owned by one producer on one shard. `RING_CAP` slots, indexed
/// by `head & MASK` / `tail & MASK`. `head` only advances via the producer,
/// `tail` only via the consumer.
struct PRing<T: Send, const RING_CAP: usize> {
    head: AtomicUsize,
    tail: AtomicUsize,
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

impl<T: Send, const RING_CAP: usize> PRing<T, RING_CAP> {
    const MASK: usize = RING_CAP - 1;

    fn new() -> Self {
        let slots: Vec<UnsafeCell<MaybeUninit<T>>> =
            (0..RING_CAP).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect();
        Self {
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            slots: slots.into_boxed_slice(),
        }
    }

    #[inline]
    fn is_full(h: usize, t: usize) -> bool {
        h.wrapping_sub(t) >= RING_CAP
    }
}

// ─── Shard ────────────────────────────────────────────────────────────────

struct Shard<T: Send, const RING_CAP: usize> {
    /// Bit `p` = "ring[p] possibly has data". Bit 63 = shutdown.
    /// Cleared only at consumer-park time (with Dekker recheck).
    full_set: SignalSet,
    /// `rings[p]` owned by producer `p`.
    rings: Box<[PRing<T, RING_CAP>]>,
    /// Cached mask covering bits `0..m` (excludes shutdown bit).
    full_mask: u64,
    shutdown_id: SignalId,
}

// ─── Shared inner state ────────────────────────────────────────────────────

struct MpmcInner<T: Send, const RING_CAP: usize> {
    shards: Box<[Shard<T, RING_CAP>]>,
    /// Per-producer backpressure gate. Released by any consumer that
    /// advances `tail` on one of this producer's rings. Producers park
    /// here when every shard's ring for them is full.
    producer_gates: Box<[Signal]>,
    shutdown: AtomicBool,
    m: usize,
    n: usize,
}

// Safety: all mutable state (ring slots) is serialized by the SPSC
// contract per (producer, shard). Producer writes `slots[head & MASK]`
// only when its own `head.load - tail.load(Acquire) < RING_CAP`; consumer
// reads `slots[tail & MASK]` only when `head.load(Acquire) > tail`. Head
// publication is Release, so slot writes are visible by the time consumer
// observes the advanced head.
unsafe impl<T: Send, const RING_CAP: usize> Sync for MpmcInner<T, RING_CAP> {}
unsafe impl<T: Send, const RING_CAP: usize> Send for MpmcInner<T, RING_CAP> {}

impl<T: Send, const RING_CAP: usize> MpmcInner<T, RING_CAP> {
    fn new(m: usize, n: usize) -> Self {
        assert!(
            RING_CAP > 0 && RING_CAP.is_power_of_two(),
            "RING_CAP must be a power of two ≥ 1"
        );

        let mut shards_vec = Vec::with_capacity(n);
        for s in 0..n {
            let mut full_set = SignalSet::new();
            for p in 0..m {
                let name: &'static str =
                    Box::leak(format!("mpmc_s{s}_p{p}").into_boxed_str());
                let _ = full_set.create(name);
            }
            let shutdown_id = SignalId::new(SHUTDOWN_BIT);

            let full_mask: u64 = if m == 64 { !0u64 } else { (1u64 << m) - 1 };

            let rings: Vec<PRing<T, RING_CAP>> =
                (0..m).map(|_| PRing::new()).collect();

            shards_vec.push(Shard {
                full_set,
                rings: rings.into_boxed_slice(),
                full_mask,
                shutdown_id,
            });
        }

        let producer_gates: Vec<Signal> = (0..m)
            .map(|_| {
                let g = Signal::new();
                // Released initially so a never-stalled producer doesn't
                // need to wait on first send.
                g.release();
                g
            })
            .collect();

        Self {
            shards: shards_vec.into_boxed_slice(),
            producer_gates: producer_gates.into_boxed_slice(),
            shutdown: AtomicBool::new(false),
            m,
            n,
        }
    }
}

impl<T: Send, const RING_CAP: usize> Drop for MpmcInner<T, RING_CAP> {
    fn drop(&mut self) {
        // Drop any T still in [tail, head) of every ring to avoid leaking
        // RAII payloads.
        for shard in self.shards.iter() {
            for p in 0..self.m {
                let ring = &shard.rings[p];
                let h = ring.head.load(Ordering::Acquire);
                let mut t = ring.tail.load(Ordering::Acquire);
                while t != h {
                    // Safety: &mut self ⇒ exclusive access to the arc inner;
                    // slot t is initialized because producer wrote it and
                    // consumer hadn't yet advanced tail past it.
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
/// shard)` pair is an SPSC ring of `RING_CAP` slots.
///
/// Construct via [`Mpmc::new`]. This type itself is a namespace for the
/// constructor; there's no `Mpmc` instance.
pub struct Mpmc<T: Send, const RING_CAP: usize = 64>(PhantomData<T>);

impl<T: Send + 'static, const RING_CAP: usize> Mpmc<T, RING_CAP> {
    /// Build an `Mpmc` with `m` producers and `n` consumer shards.
    ///
    /// Returns `(producers, consumers, shutdown)`. Producers and
    /// consumers are `Send + !Sync` — each handle is meant to be moved
    /// to its own thread. The shutdown handle is `Send + Sync` and cheap
    /// to clone.
    ///
    /// # Panics
    /// - `m == 0` or `n == 0`
    /// - `m > MAX_MPMC_PRODUCERS` (63)
    /// - `RING_CAP` is not a power of two ≥ 1
    pub fn new(
        m: usize,
        n: usize,
    ) -> (
        Vec<MpmcProducer<T, RING_CAP>>,
        Vec<MpmcConsumer<T, RING_CAP>>,
        MpmcShutdown<T, RING_CAP>,
    ) {
        assert!(m > 0, "Mpmc::new: m must be > 0");
        assert!(n > 0, "Mpmc::new: n must be > 0");
        assert!(
            m <= MAX_MPMC_PRODUCERS,
            "Mpmc::new: m must be <= {MAX_MPMC_PRODUCERS}"
        );

        let inner = Arc::new(MpmcInner::<T, RING_CAP>::new(m, n));

        let producers: Vec<MpmcProducer<T, RING_CAP>> = (0..m)
            .map(|p| MpmcProducer {
                inner: inner.clone(),
                my_idx: p,
                my_id: SignalId::new(p as u8),
                // Stagger so first sends fan out across shards.
                cursor: Cell::new((p % n) as u32),
                _not_sync: PhantomData,
            })
            .collect();

        let consumers: Vec<MpmcConsumer<T, RING_CAP>> = (0..n)
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
pub struct MpmcProducer<T: Send, const RING_CAP: usize = 64> {
    inner: Arc<MpmcInner<T, RING_CAP>>,
    my_idx: usize,
    my_id: SignalId,
    cursor: Cell<u32>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send, const RING_CAP: usize> MpmcProducer<T, RING_CAP> {
    /// Numeric index of this producer (`0..m`). Corresponds to the bit
    /// position in every shard's SignalSet.
    #[inline]
    pub fn index(&self) -> usize { self.my_idx }

    /// Register this thread as the producer's backpressure parker. Must
    /// be called from the thread that will invoke [`send`](Self::send)
    /// before any send on a potentially-saturated `Mpmc`.
    #[inline]
    pub fn bind(&self) {
        self.inner.producer_gates[self.my_idx]
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

    /// Non-blocking send. Scans shards from the adaptive cursor, writes
    /// into the first ring that isn't full, advances the cursor. Returns
    /// `Err(value)` if every ring for this producer is full.
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
            // Safety: SPSC — only this producer writes slots[head] on this
            // ring, and the fullness check above guarantees the slot is
            // not currently held by a consumer.
            unsafe {
                (*ring.slots[h & PRing::<T, RING_CAP>::MASK].get()).write(value);
            }
            // Publish: consumer's head.load(Acquire) will see our write.
            ring.head.store(h.wrapping_add(1), Ordering::Release);
            // Level-triggered bit: "this ring has data". Always set after
            // a push, even if the bit is already set. This honors the
            // Signal contract (release on every message) and closes the
            // park_or_drain Dekker cleanly: if the consumer cleared the
            // bit between our head.store and our release, the release
            // will re-set it and acquire_any will wake.
            shard.full_set.release(self.my_id);
            self.cursor.set(((s + 1) % n) as u32);
            return Ok(());
        }
        Err(value)
    }

    /// Batch send: drain as many items as fit into a single ring in one
    /// atomic fetch_or. Returns the number actually written.
    ///
    /// Items are taken from the **front** of `items` (via `Vec::drain(..take)`).
    /// Any remainder stays in `items`; caller can re-invoke to place the
    /// rest into another shard.
    ///
    /// Amortizes the `fetch_or` and `head.store` over K items instead of
    /// paying them per-item.
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
            // Drain `take` items from the front of the Vec, writing each
            // into the ring without destructively running their drops.
            // We use Vec::drain which takes ownership of the elements.
            let mut h = h0;
            for v in items.drain(..take) {
                unsafe {
                    (*ring.slots[h & PRing::<T, RING_CAP>::MASK].get()).write(v);
                }
                h = h.wrapping_add(1);
            }
            // Single Release publishing all `take` items at once.
            ring.head.store(h, Ordering::Release);
            // Level-triggered bit: always set. See try_send.
            shard.full_set.release(self.my_id);
            self.cursor.set(((s + 1) % n) as u32);
            return take;
        }
        0
    }

    /// Blocking send. Parks on the producer's backpressure gate if every
    /// ring for this producer is full. Wakes when any consumer advances
    /// `tail` on one of this producer's rings.
    #[inline]
    pub fn send(&self, mut value: T) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            let gate = &self.inner.producer_gates[self.my_idx];
            gate.lock();
            std::sync::atomic::fence(Ordering::SeqCst);
            if self.has_idle_shard() {
                gate.release();
                continue;
            }
            gate.acquire();
        }
    }
}

// ─── Consumer handle ───────────────────────────────────────────────────────

/// One of the `N` consumer handles returned by [`Mpmc::new`]. Owns exactly
/// one shard.
pub struct MpmcConsumer<T: Send, const RING_CAP: usize = 64> {
    inner: Arc<MpmcInner<T, RING_CAP>>,
    shard_idx: usize,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send, const RING_CAP: usize> MpmcConsumer<T, RING_CAP> {
    /// Numeric index of this consumer's shard (`0..n`).
    #[inline]
    pub fn shard(&self) -> usize { self.shard_idx }

    /// Register this thread as the shard's drain worker. Must be called
    /// before the first blocking `recv` / `recv_batch`.
    #[inline]
    pub fn bind(&self) {
        self.inner.shards[self.shard_idx]
            .full_set
            .set_worker(std::thread::current());
    }

    /// Non-blocking single-item take. Reads one item from the first ring
    /// whose bit is set, if that ring actually has data.
    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        let shard = &self.inner.shards[self.shard_idx];
        let state = shard.full_set.state() & shard.full_mask;
        if state == 0 { return None; }
        let m = self.inner.m;
        let mut remaining = state;
        while remaining != 0 {
            let p = remaining.trailing_zeros() as usize;
            let bit = 1u64 << p;
            remaining &= !bit;
            if p >= m { break; }
            let ring = &shard.rings[p];
            let t = ring.tail.load(Ordering::Relaxed);
            let h = ring.head.load(Ordering::Acquire);
            if t == h { continue; } // stale bit; skip
            // Safety: SPSC — only this consumer reads from this ring.
            // h observed via Acquire ⇒ producer's slot write is visible.
            let v = unsafe {
                (*ring.slots[t & PRing::<T, RING_CAP>::MASK].get())
                    .assume_init_read()
            };
            ring.tail.store(t.wrapping_add(1), Ordering::Release);
            // Level-triggered wake: always release producer gate after a
            // pop. Mirror of the producer-side change — keeps the Signal
            // invariant "release on every advance" and eliminates the
            // was_full optimization's implicit Dekker.
            self.inner.producer_gates[p].release();
            return Some(v);
        }
        None
    }

    /// Blocking single-item take. Parks on the shard's SignalSet until any
    /// producer's bit is set or shutdown is signaled.
    pub fn recv(&self) -> Result<T, Shutdown> {
        loop {
            if let Some(v) = self.try_recv() { return Ok(v); }
            if self.inner.shutdown.load(Ordering::Acquire) {
                let shard = &self.inner.shards[self.shard_idx];
                shard.full_set.lock(shard.shutdown_id);
                return Err(Shutdown);
            }
            // Park path: Dekker-clear stale bits, then acquire_any.
            if self.park_or_drain()? { /* drained, loop */ }
        }
    }

    /// Drain every currently-ready ring on this shard in one pass. Parks
    /// once if nothing is ready. Returns the number of messages delivered.
    pub fn recv_batch<F: FnMut(T)>(&self, mut f: F) -> Result<usize, Shutdown> {
        loop {
            let count = self.drain_all(&mut f);
            if count > 0 {
                // Always return Ok on progress — even under shutdown. The
                // caller loops back and calls us again; the next drain_all
                // will either find more data (and we'll return Ok again) or
                // return 0, at which point the shutdown path below fires.
                // Returning Err too eagerly would abandon items still sitting
                // in peer producer rings of this shard, causing Drop to
                // silently destroy them.
                return Ok(count);
            }
            if self.inner.shutdown.load(Ordering::Acquire) {
                let shard = &self.inner.shards[self.shard_idx];
                shard.full_set.lock(shard.shutdown_id);
                return Err(Shutdown);
            }
            // Nothing drained this pass. Clear stale bits and park (or loop).
            self.park_or_drain()?;
        }
    }

    /// Non-blocking drain of every ready ring. Returns count.
    pub fn try_recv_batch<F: FnMut(T)>(&self, mut f: F) -> usize {
        self.drain_all(&mut f)
    }

    /// Drain all rings with `state & full_mask` bits set. Does NOT clear
    /// bits — that's the park path's job. Loops to pick up producers that
    /// re-filled a ring while we were draining peer rings.
    #[inline]
    fn drain_all<F: FnMut(T)>(&self, f: &mut F) -> usize {
        let shard = &self.inner.shards[self.shard_idx];
        let m = self.inner.m;
        let mut count = 0;
        loop {
            let state = shard.full_set.state() & shard.full_mask;
            if state == 0 { return count; }
            let mut progress = false;
            let mut remaining = state;
            while remaining != 0 {
                let p = remaining.trailing_zeros() as usize;
                remaining &= remaining - 1; // clear lowest set bit
                if p >= m { break; }
                let ring = &shard.rings[p];
                let mut t = ring.tail.load(Ordering::Relaxed);
                let h = ring.head.load(Ordering::Acquire);
                if t == h { continue; }
                // Drain [t, h).
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
                // Level-triggered wake: one release per ring drained,
                // always. Amortized over the whole batch, not per-item.
                // Matches the Signal contract — the producer's gate
                // invariant is "release whenever tail advances."
                self.inner.producer_gates[p].release();
            }
            if !progress {
                // Every set bit's ring was empty on inspection (stale).
                // Don't spin — return and let the caller decide to park.
                return count;
            }
        }
    }

    /// Park path: atomically clear all producer bits, SeqCst fence,
    /// recheck every ring. If any ring has pending data, re-set that
    /// bit and return `Ok(true)` so caller loops back to drain. Otherwise
    /// `acquire_any` and return `Ok(false)`.
    ///
    /// Returns `Err(Shutdown)` if shutdown is observed during park.
    fn park_or_drain(&self) -> Result<bool, Shutdown> {
        let shard = &self.inner.shards[self.shard_idx];
        let m = self.inner.m;
        let wake_mask = shard.full_mask | shard.shutdown_id.mask();

        // Clear all producer bits (keep shutdown bit alone).
        shard.full_set.lock_mask(shard.full_mask);
        // Dekker: ensure our clear is ordered before the recheck loads.
        std::sync::atomic::fence(Ordering::SeqCst);

        // Recheck every ring. If a producer raced (wrote and fetch_or'd
        // before our lock_mask), its ring has data with the bit now clear.
        let mut any_raced = false;
        for p in 0..m {
            let ring = &shard.rings[p];
            let h = ring.head.load(Ordering::Acquire);
            let t = ring.tail.load(Ordering::Relaxed);
            if h != t {
                shard.full_set.release(SignalId::new(p as u8));
                any_raced = true;
            }
        }
        if any_raced {
            return Ok(true); // caller loops back to drain
        }

        if self.inner.shutdown.load(Ordering::Acquire) {
            shard.full_set.lock(shard.shutdown_id);
            return Err(Shutdown);
        }

        // Truly empty. Park until producer's fetch_or or shutdown's release.
        shard.full_set.acquire_any(wake_mask);

        // Do NOT handle shutdown here. If we wake with *both* a producer
        // bit and the shutdown bit set (producer wrote, then supervisor
        // signaled), returning `Err(Shutdown)` now would skip the pending
        // producer data. The caller's loop runs `drain_all` first, so
        // shutdown is handled there — after the ring is empty.
        Ok(false)
    }
}

// ─── Shutdown handle ───────────────────────────────────────────────────────

/// Supervisor-side handle. Call [`signal`](Self::signal) to wake every
/// parked consumer (and any parked producer) with `Err(Shutdown)`. Cheap
/// to clone; `Send + Sync`.
pub struct MpmcShutdown<T: Send, const RING_CAP: usize = 64> {
    inner: Arc<MpmcInner<T, RING_CAP>>,
}

impl<T: Send, const RING_CAP: usize> Clone for MpmcShutdown<T, RING_CAP> {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}

impl<T: Send, const RING_CAP: usize> MpmcShutdown<T, RING_CAP> {
    /// Flag as shutting down and wake every parked consumer + producer.
    /// Idempotent.
    #[inline]
    pub fn signal(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        for shard in self.inner.shards.iter() {
            shard.full_set.release(shard.shutdown_id);
        }
        for g in self.inner.producer_gates.iter() {
            g.release();
        }
    }

    /// `true` once any clone of this handle has been signaled.
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

        assert_eq!(got_count.load(Ordering::Relaxed), (M as u64 * PER) as usize,
                   "every message must be delivered exactly once");
        assert_eq!(got_sum.load(Ordering::Relaxed) as u64, sum_expected,
                   "no value corruption or duplication");
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
        assert_eq!(total, M * PER as usize,
                   "every message must be delivered exactly once, \
                    even if load spreads unevenly across shards");
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
        // Use RING_CAP=1 so each shard holds exactly 1 item — matches
        // the original semantics of this test: 1 send per shard fills it.
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
    #[should_panic(expected = "m must be <= 63")]
    fn rejects_too_many_producers() {
        let _ = Mpmc::<u8>::new(64, 1);
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
        assert_eq!(received.as_ptr() as usize, ptr_before,
                   "Box must be transferred zero-copy");
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
        assert_eq!(got, vec![10, 20], "both producers drained in one batch");
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
        assert_eq!(n, 20, "all 20 items fit in one 64-slot ring");
        assert!(items.is_empty(), "Vec is drained");

        c.bind();
        let mut got: Vec<u64> = Vec::new();
        let k = c.recv_batch(|v| got.push(v)).unwrap();
        assert_eq!(k, 20);
        assert_eq!(got, (0..20).collect::<Vec<_>>());
    }
}
