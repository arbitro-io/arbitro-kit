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
//! - `M ≤ 255` producers. The shard's `SignalSet` is sized to `M+1` bits
//!   (one extra reserved for [`MpmcShutdown`]). For `M ≤ 63` the bitmap
//!   fits in a single `AtomicU64` chunk; for higher `M` the SignalSet
//!   transparently uses a chunked `Box<[AtomicU64]>` and the consumer
//!   walks chunks during drain. Hot-path cost per chunk is one `Acquire`
//!   load — at `M = 255` (4 chunks) the drain scan is still 4 loads
//!   regardless of how many bits are set.
//! - `N ≥ 1`, no upper bound (runtime-sized).
//! - `M == 0` or `N == 0` is rejected.
//! - `RING_CAP` must be a power of two ≥ 1.

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::gate::{Park, SignalId, SignalSet};
use crate::route::hub::Shutdown;

/// Maximum number of producers in an [`Mpmc`]. Limited by the `u8` index
/// in `SignalId` minus one bit reserved for [`MpmcShutdown`], so `255`.
pub const MAX_MPMC_PRODUCERS: usize = 255;

// ─── Per-(producer, shard) mini-Ring (SPSC) ───────────────────────────────

/// Cache line size on x86_64 / aarch64. Used to separate `head` and `tail`
/// onto distinct cache lines so the producer's `head.store(Release)` does
/// not invalidate the consumer's cached `tail` (and vice versa). Without
/// this padding every send triggers a cross-CPU cache-line bounce on the
/// SAME line that holds both cursors — a textbook **false sharing** pitfall.
///
/// Same trick used by LMAX Disruptor, JCTools, DPDK rte_ring for the same
/// reason. Mpsc's `PRing` carries the identical layout.
const CACHE_LINE: usize = 64;

/// SPSC ring owned by one producer on one shard. `RING_CAP` slots, indexed
/// by `head & MASK` / `tail & MASK`. `head` only advances via the producer,
/// `tail` only via the consumer.
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

// ─── Shard ────────────────────────────────────────────────────────────────

struct Shard<T: Send, const RING_CAP: usize> {
    /// Bit `p` = "ring[p] possibly has data". Bit `m` = shutdown.
    /// Cleared only at consumer-park time (with Dekker recheck).
    /// SignalSet has `ceil((m+1)/64)` chunks; for `m ≤ 63` that is 1.
    full_set: SignalSet,
    /// `rings[p]` owned by producer `p`.
    rings: Box<[PRing<T, RING_CAP>]>,
    /// Per-chunk mask covering producer bits `0..m` only (shutdown bit
    /// excluded). `full_mask_chunks[c]` is the bitmask of producer bits
    /// inside chunk `c`. Length = `full_set.n_chunks()`.
    full_mask_chunks: Box<[u64]>,
    shutdown_id: SignalId,
}

// ─── Shared inner state ────────────────────────────────────────────────────

struct MpmcInner<T: Send, const RING_CAP: usize> {
    shards: Box<[Shard<T, RING_CAP>]>,
    /// Per-producer backpressure park. The consumer wakes producer `p`
    /// after advancing `tail` on one of its rings. Producer parks here
    /// when every shard's ring for it is full — the park predicate is
    /// `has_idle_shard(p)` which reads the cursor state directly, so no
    /// separate `locked` bool is needed.
    producer_parks: Box<[Park]>,
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
            // m producers + 1 shutdown bit. SignalSet rounds up to chunks.
            let mut full_set = SignalSet::with_capacity(m + 1);
            for p in 0..m {
                let name: &'static str =
                    Box::leak(format!("mpmc_s{s}_p{p}").into_boxed_str());
                let _ = full_set.create(name);
            }
            // Shutdown is registered last → bit `m`.
            let shutdown_name: &'static str =
                Box::leak(format!("mpmc_s{s}_shutdown").into_boxed_str());
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

            shards_vec.push(Shard {
                full_set,
                rings: rings.into_boxed_slice(),
                full_mask_chunks: full_mask_chunks.into_boxed_slice(),
                shutdown_id,
            });
        }

        // Park is stateless — no initial "release" needed. The predicate
        // `has_idle_shard(p)` reads cursors directly, so a producer that
        // never stalls never touches this struct.
        let producer_parks: Vec<Park> = (0..m).map(|_| Park::new()).collect();

        Self {
            shards: shards_vec.into_boxed_slice(),
            producer_parks: producer_parks.into_boxed_slice(),
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
        self.inner.producer_parks[self.my_idx]
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
    //
    // These methods return a point-in-time view of available space and
    // pending fill. The result is **not** linearizable — a peer thread may
    // push or drain between when the snapshot is taken and when the caller
    // acts on it. Use for metrics, heuristic backpressure, and saturation
    // alerts; never as a correctness gate. The send-or-fail decision must
    // still go through `try_send` / `try_send_batch`.
    //
    // Cost: 2 atomic loads (Acquire + Relaxed) per shard scanned. Hot-path
    // operations (`try_send`, `recv_batch`) are unaffected.

    /// Per-shard ring capacity for any producer (compile-time constant).
    /// Equivalent to the type-level `RING_CAP`.
    #[inline]
    pub const fn capacity_per_shard(&self) -> usize { RING_CAP }

    /// Total slot capacity reachable from this producer = `N × RING_CAP`,
    /// where `N` is the consumer count.
    #[inline]
    pub fn total_capacity(&self) -> usize {
        self.inner.n * RING_CAP
    }

    /// Free slots in shard `s` for this producer. Returns `RING_CAP - len`
    /// where `len = head - tail`. Saturates at 0 (never negative).
    ///
    /// # Panics
    /// If `s >= N`.
    #[inline]
    pub fn available_in_shard(&self, s: usize) -> usize {
        let ring = &self.inner.shards[s].rings[self.my_idx];
        let h = ring.head.load(Ordering::Relaxed);
        let t = ring.tail.load(Ordering::Acquire);
        let used = h.wrapping_sub(t);
        RING_CAP.saturating_sub(used)
    }

    /// Total free slots across all shards for this producer. Sum of
    /// `available_in_shard(s)` for `s in 0..N`. Cost: `2N` atomic loads.
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

    /// Pending items in shard `s` from this producer = `head - tail`.
    /// Useful for symmetry with `available_in_shard`.
    ///
    /// # Panics
    /// If `s >= N`.
    #[inline]
    pub fn pending_in_shard(&self, s: usize) -> usize {
        let ring = &self.inner.shards[s].rings[self.my_idx];
        let h = ring.head.load(Ordering::Acquire);
        let t = ring.tail.load(Ordering::Relaxed);
        h.wrapping_sub(t)
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

    /// Blocking send. Parks on the producer's backpressure park if every
    /// ring for this producer is full. Wakes when any consumer advances
    /// `tail` on one of this producer's rings.
    ///
    /// The park predicate `has_idle_shard()` loads each shard's `tail`
    /// with Acquire — which synchronises-with the consumer's Release
    /// store of the advanced tail, so no extra SeqCst fence is needed
    /// here (the one inside `Park::wait_slow` closes the Dekker race).
    #[inline]
    pub fn send(&self, mut value: T) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            // Park until some consumer advances a tail we care about,
            // then retry the send. No Signal `locked` write on the
            // consumer side any more — one less store per drain.
            self.inner.producer_parks[self.my_idx]
                .wait_until(|| self.has_idle_shard());
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

    // ── Capacity introspection (snapshot, non-consistent) ────────────────
    //
    // Same caveat as the producer-side methods: snapshot only, not a
    // correctness gate. Useful for `pending` gauges and saturation
    // alarms. Hot-path drain (`recv` / `recv_batch`) is unaffected.

    /// Per-producer ring capacity (compile-time constant).
    #[inline]
    pub const fn capacity_per_producer(&self) -> usize { RING_CAP }

    /// Total slot capacity feeding this consumer = `M × RING_CAP`.
    #[inline]
    pub fn total_capacity(&self) -> usize {
        self.inner.m * RING_CAP
    }

    /// Pending items currently sitting in this shard's rings, summed
    /// across all `M` producers. Cost: `2M` atomic loads.
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

    /// Free slots across this shard's rings, summed across all `M`
    /// producers. Cost: `2M` atomic loads.
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

    /// Pending items from a specific producer in this shard.
    ///
    /// # Panics
    /// If `p >= M`.
    #[inline]
    pub fn pending_from(&self, p: usize) -> usize {
        let ring = &self.inner.shards[self.shard_idx].rings[p];
        let h = ring.head.load(Ordering::Acquire);
        let t = ring.tail.load(Ordering::Relaxed);
        h.wrapping_sub(t)
    }

    /// Cheap fast-path: any producer's ring has data on this shard?
    /// Single Acquire load per chunk + chunk-mask AND. O(chunks), not O(M).
    #[inline]
    pub fn has_pending(&self) -> bool {
        let shard = &self.inner.shards[self.shard_idx];
        for c in 0..shard.full_mask_chunks.len() {
            let state = shard.full_set.state_chunk(c) & shard.full_mask_chunks[c];
            if state != 0 { return true; }
        }
        false
    }

    /// Non-blocking single-item take. Reads one item from the first ring
    /// whose bit is set, if that ring actually has data.
    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        let shard = &self.inner.shards[self.shard_idx];
        let m = self.inner.m;
        let n_chunks = shard.full_mask_chunks.len();
        // Walk chunks in order; for each, mask producer bits and scan.
        for c in 0..n_chunks {
            let state = shard.full_set.state_chunk(c) & shard.full_mask_chunks[c];
            if state == 0 { continue; }
            let mut remaining = state;
            while remaining != 0 {
                let bit_in_chunk = remaining.trailing_zeros() as usize;
                let bit = 1u64 << bit_in_chunk;
                remaining &= !bit;
                let p = c * 64 + bit_in_chunk;
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
                // Wake producer p if it is parked. `Park::wake` reads a
                // `parked` flag with Relaxed and does nothing when the
                // producer is not parked — ~0.3 ns no-op.
                self.inner.producer_parks[p].wake();
                return Some(v);
            }
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
        let n_chunks = shard.full_mask_chunks.len();
        let mut count = 0;
        loop {
            // Snapshot: any producer bit set across any chunk?
            let mut any_state = false;
            let mut progress = false;
            for c in 0..n_chunks {
                let state = shard.full_set.state_chunk(c)
                    & shard.full_mask_chunks[c];
                if state == 0 { continue; }
                any_state = true;
                let mut remaining = state;
                while remaining != 0 {
                    let bit_in_chunk = remaining.trailing_zeros() as usize;
                    remaining &= remaining - 1; // clear lowest set bit
                    let p = c * 64 + bit_in_chunk;
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
                    // Amortized wake: one `wake()` per ring drained.
                    self.inner.producer_parks[p].wake();
                }
            }
            if !any_state { return count; }
            if !progress {
                // Every set bit's ring was empty on inspection (stale).
                return count;
            }
        }
    }

    /// Park path: atomically clear all producer bits (across every chunk),
    /// SeqCst fence, recheck every ring. If any ring has pending data,
    /// re-set that bit and return `Ok(true)` so caller loops back to drain.
    /// Otherwise park on the SignalSet (any chunk bit) and return `Ok(false)`.
    ///
    /// Returns `Err(Shutdown)` if shutdown is observed during park.
    fn park_or_drain(&self) -> Result<bool, Shutdown> {
        let shard = &self.inner.shards[self.shard_idx];
        let m = self.inner.m;
        let n_chunks = shard.full_mask_chunks.len();

        // Clear all producer bits in every chunk (keep shutdown bit alone).
        for c in 0..n_chunks {
            shard.full_set.lock_chunk_mask(c, shard.full_mask_chunks[c]);
        }
        // Dekker: ensure our clear is ordered before the recheck loads.
        std::sync::atomic::fence(Ordering::SeqCst);

        // Recheck every ring. If a producer raced (wrote and fetch_or'd
        // before our lock_chunk_mask), its ring has data with the bit
        // now clear — re-set so drain_all picks it up.
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

        // Truly empty. Park until any bit gets set anywhere — that
        // covers both producer fetch_or (in any chunk) and shutdown's
        // release (last chunk).
        shard.full_set.acquire_any_chunk();

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
        // Wake any parked producers so they observe shutdown and exit.
        // Their `send` predicate includes shutdown-awareness via the
        // next try_send → has_idle_shard loop.
        for p in self.inner.producer_parks.iter() {
            p.wake();
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
    #[should_panic(expected = "m must be <=")]
    fn rejects_too_many_producers() {
        let _ = Mpmc::<u8>::new(MAX_MPMC_PRODUCERS + 1, 1);
    }

    #[test]
    fn high_producer_count_above_64_works() {
        // Regression: before the chunked SignalSet refactor the limit was
        // 63. With chunks the cap is 255 and we can wire arbitrary M.
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
        // 2 producers × 3 shards × RING_CAP=8 = 24 slot total per producer,
        // 16 slots per shard from the consumer side.
        let (mut ps, mut cs, _sd) = Mpmc::<u32, 8>::new(2, 3);
        let p0 = ps.remove(0);
        let p1 = ps.remove(0);

        // Consumers: shard 0 / 1 / 2.
        let c0 = cs.remove(0);
        let c1 = cs.remove(0);
        let c2 = cs.remove(0);

        // Pristine state.
        assert_eq!(p0.capacity_per_shard(), 8);
        assert_eq!(p0.total_capacity(), 24);
        assert_eq!(p0.available(), 24);
        assert_eq!(p0.pending_in_shard(0), 0);

        assert_eq!(c0.capacity_per_producer(), 8);
        assert_eq!(c0.total_capacity(), 16);
        assert_eq!(c0.pending(), 0);
        assert_eq!(c0.available(), 16);
        assert_eq!(c0.has_pending(), false);

        // Push from p0 — adaptive routing fills shards in order.
        for v in 0..5u32 { p0.try_send(v).unwrap(); }
        assert_eq!(p0.available(), 24 - 5);
        // Some consumer now has pending from p0.
        let pending_from_p0 = c0.pending_from(0) + c1.pending_from(0) + c2.pending_from(0);
        assert_eq!(pending_from_p0, 5);

        // Push from p1 — independent counters.
        p1.try_send(1000).unwrap();
        assert_eq!(p1.available(), 24 - 1);

        // has_pending fast-path is true on whichever shard got a value.
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
