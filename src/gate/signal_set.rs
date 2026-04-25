//! Multi-channel M:1 signal: up to 64 gates backed by one `AtomicU64` bitmap.
//!
//! ## Model
//!
//! Each gate occupies one bit of an `AtomicU64`. Bit set = that gate is open
//! (has pending work). Bit clear = locked.
//!
//! - `release(id)`: `fetch_or(bit, Release)`. Lock-free. Wakes the consumer
//!   iff a bit flipped 0→1 and the consumer is parked.
//! - `lock(id)`: `fetch_and(!bit, Relaxed)`. Lock-free.
//! - `acquire_any(mask)` / `acquire_all(mask)` / `acquire_full()`: block the
//!   consumer until the predicate over the current state holds. Spin-then-park
//!   like [`super::Signal`].
//!
//! ## Why a bitmap and not N Signals
//!
//! Coordinating N independent `Signal`s for "wait until any of them fires" would
//! require N `Thread` handles or a shared event primitive. A single `AtomicU64`
//! collapses the whole state into one load/store on the hot path and lets the
//! consumer check any boolean combination of gates with one read.
//!
//! ## Limits
//!
//! - Max 64 gates per set (one bit each).
//! - **M producers : 1 consumer**, same as [`super::Signal`].
//! - Signal registration must happen before the set is shared (compile-time
//!   enforced: `create` takes `&mut self`).

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Maximum number of gates a `SignalSet` can host with the legacy
/// (mask: u64) API. Sets with more than 64 bits must be created via
/// [`SignalSet::with_capacity`] and use chunk-aware methods.
pub const MAX_GATES: usize = 64;

/// Handle for a gate registered in a `SignalSet`. Cheap to `Copy`.
///
/// Use [`SignalId::mask`] to combine multiple ids into a `u64` bitmask suitable
/// for [`SignalSet::acquire_any`] / [`SignalSet::acquire_all`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct SignalId(u8);

impl SignalId {
    /// Construct a `SignalId` from a raw index. Exposed for tests and for
    /// users who prefer compile-time `const` ids over runtime `create`.
    ///
    /// `SignalId` is `u8`-typed, capping the index at 255. The legacy
    /// `mask()` API only makes sense for `idx < 64` (chunk 0); higher
    /// indices are valid for chunk-aware methods (`release`, `lock`,
    /// `is_open`) on a `SignalSet` built with [`SignalSet::with_capacity`].
    pub const fn new(idx: u8) -> Self { Self(idx) }

    #[inline]
    pub const fn index(self) -> u8 { self.0 }

    /// Bit mask with only this gate's bit set.
    ///
    /// # Panics
    /// `idx >= 64`. The mask-based API is chunk-0 only; for higher
    /// indices use the chunk-aware methods directly.
    #[inline]
    pub const fn mask(self) -> u64 {
        assert!((self.0 as usize) < 64, "SignalId::mask: only valid for idx < 64");
        1u64 << self.0
    }
}

/// Default spin iterations before parking. Same rationale as
/// [`crate::gate::DEFAULT_SPIN_ITERS`].
pub const DEFAULT_SPIN_ITERS: u32 = 512;

#[repr(align(64))]
pub struct SignalSet {
    /// Chunked bitmap. `chunks[c]` bit `b` = gate `c*64 + b` is open. Read
    /// with `Acquire`, mutated with `fetch_or(Release)` / `fetch_and(Release)`.
    /// Default capacity is 1 chunk (64 bits) — same memory footprint as the
    /// previous `AtomicU64` once factor in the heap pointer.
    chunks: Box<[AtomicU64]>,
    /// Set by the consumer before parking, cleared after wake. Producers
    /// probe this flag to decide whether `unpark` is needed.
    parked: AtomicBool,
    /// Spin iterations before parking in `acquire*`. Set at construction via
    /// [`SignalSet::new`] (default [`DEFAULT_SPIN_ITERS`]) or [`SignalSet::with_spin`].
    spin_iters: u32,
    /// Registered consumer thread. Written pre-share, read only when
    /// `parked == true`.
    worker: UnsafeCell<Option<std::thread::Thread>>,
    /// Optional debug names, indexed by `SignalId.0`. Never read in hot path.
    /// Sized to `capacity_bits()` at construction.
    names: UnsafeCell<Vec<Option<&'static str>>>,
    /// Next free slot for `create()`. Mutated only pre-share via `&mut self`.
    next_id: UnsafeCell<u32>,
}

// Safety: `worker` / `names` / `next_id` obey the same discipline as
// `Signal::worker`: mutated through `&mut self` (pre-share) or only read once
// `parked == true` has synchronized the consumer's writes.
unsafe impl Sync for SignalSet {}
unsafe impl Send for SignalSet {}

impl Default for SignalSet {
    fn default() -> Self { Self::new() }
}

impl SignalSet {
    pub fn new() -> Self { Self::with_spin(DEFAULT_SPIN_ITERS) }

    /// Construct with a custom spin-iteration budget. See [`Signal::with_spin`]
    /// for semantics. 0 = always park immediately after the fast-path check.
    pub fn with_spin(spin_iters: u32) -> Self {
        Self::with_capacity_and_spin(MAX_GATES, spin_iters)
    }

    /// Construct a SignalSet with explicit bit capacity. For `n_bits ≤ 64`
    /// this is identical to `new()` (single AtomicU64 chunk). For larger
    /// `n_bits`, allocates `ceil(n_bits / 64)` chunks. Mask-based methods
    /// (`acquire_any(mask: u64)`, `lock_mask(mask: u64)`, `state()`) only
    /// address the first chunk and remain backward-compatible for the
    /// `≤64` case.
    pub fn with_capacity(n_bits: usize) -> Self {
        Self::with_capacity_and_spin(n_bits, DEFAULT_SPIN_ITERS)
    }

    fn with_capacity_and_spin(n_bits: usize, spin_iters: u32) -> Self {
        let n_chunks = ((n_bits.max(1)) + 63) / 64;
        let cap_bits = n_chunks * 64;
        let mut chunks: Vec<AtomicU64> = Vec::with_capacity(n_chunks);
        for _ in 0..n_chunks { chunks.push(AtomicU64::new(0)); }
        let names: Vec<Option<&'static str>> = vec![None; cap_bits];
        Self {
            chunks:  chunks.into_boxed_slice(),
            parked:  AtomicBool::new(false),
            spin_iters,
            worker:  UnsafeCell::new(None),
            names:   UnsafeCell::new(names),
            next_id: UnsafeCell::new(0),
        }
    }

    /// Number of `AtomicU64` chunks. `n_chunks() == 1` for the legacy
    /// (≤64-bit) API.
    #[inline]
    pub fn n_chunks(&self) -> usize { self.chunks.len() }

    /// Total bit capacity (= `n_chunks() * 64`).
    #[inline]
    pub fn capacity_bits(&self) -> usize { self.chunks.len() * 64 }

    /// Register a new gate. Returns its typed handle. Callable only while the
    /// set is unshared (`&mut self` enforces this at compile time).
    ///
    /// # Panics
    /// Panics if `capacity_bits()` gates have already been registered, or if
    /// the next index would exceed `u8::MAX` (the `SignalId` representation).
    pub fn create(&mut self, name: &'static str) -> SignalId {
        // Safety: `&mut self` guarantees exclusive access; no concurrent reads.
        let id = unsafe {
            let next = &mut *self.next_id.get();
            let cap = self.chunks.len() * 64;
            assert!((*next as usize) < cap, "SignalSet: capacity_bits exceeded");
            assert!(*next < 256, "SignalSet: SignalId is u8; max 256 gates");
            let id = *next as u8;
            *next += 1;
            (&mut *self.names.get())[id as usize] = Some(name);
            id
        };
        SignalId(id)
    }

    /// Debug name for a gate, if registered. Cold-path helper for tracing.
    pub fn name(&self, id: SignalId) -> Option<&'static str> {
        // Safety: `names` is only mutated through `&mut self`; here we only
        // read, which is sound as long as `create` is not in flight — which
        // `&mut self` rules out anyway.
        unsafe { (&*self.names.get()).get(id.0 as usize).copied().flatten() }
    }

    /// Look up a `SignalId` by its registered name. O(N) over registered gates.
    /// Cold path — do the lookup once at init, not per operation.
    pub fn id_of(&self, name: &str) -> Option<SignalId> {
        // Safety: same as `name`.
        let names = unsafe { &*self.names.get() };
        let n = unsafe { *self.next_id.get() } as usize;
        for (i, slot) in names.iter().take(n).enumerate() {
            if matches!(slot, Some(s) if *s == name) {
                return Some(SignalId(i as u8));
            }
        }
        None
    }

    /// Number of gates registered so far via `create()`.
    #[inline]
    pub fn registered(&self) -> usize {
        // Safety: `next_id` is mutated only through `&mut self` (pre-share);
        // a Relaxed read is sufficient post-share since registration is done.
        unsafe { *self.next_id.get() as usize }
    }

    /// Register the consumer thread. Must be called before the set is shared
    /// with producers. Typically the consumer calls
    /// `set.set_worker(thread::current())`.
    pub fn set_worker(&self, t: std::thread::Thread) {
        // Safety: documented pre-share contract.
        unsafe { *self.worker.get() = Some(t); }
    }

    /// Open one gate. Lock-free. Wakes the consumer iff it was parked and this
    /// call actually flipped the bit (coalesces repeated releases).
    ///
    /// Supports `id` in any chunk (≤ `capacity_bits()`).
    #[inline]
    pub fn release(&self, id: SignalId) {
        let chunk = (id.0 as usize) / 64;
        let bit = 1u64 << ((id.0 as usize) % 64);
        let prev = self.chunks[chunk].fetch_or(bit, Ordering::Release);
        if (prev & bit) == 0 && self.parked.load(Ordering::Relaxed) {
            // Safety: `parked == true` implies the consumer's `set_worker`
            // write has happened-before.
            unsafe {
                if let Some(t) = &*self.worker.get() {
                    t.unpark();
                }
            }
        }
    }

    /// Close one gate. No wake needed.
    ///
    /// Uses `Release` ordering so that any reads the consumer performed on
    /// payload memory (e.g. a `Hub` inbound slot) before calling `lock`
    /// are published before the bit is cleared.
    #[inline]
    pub fn lock(&self, id: SignalId) {
        let chunk = (id.0 as usize) / 64;
        let bit = 1u64 << ((id.0 as usize) % 64);
        self.chunks[chunk].fetch_and(!bit, Ordering::Release);
    }

    /// Close every bit in `mask` (chunk 0 only — legacy API for ≤64 bits).
    #[inline]
    pub fn lock_mask(&self, mask: u64) {
        self.chunks[0].fetch_and(!mask, Ordering::Release);
    }

    /// Close every bit in `mask` for a specific chunk. Multi-chunk variant
    /// of [`Self::lock_mask`] — used by chunked consumers (`Mpmc`) when the
    /// full producer mask spans more than 64 bits.
    #[inline]
    pub fn lock_chunk_mask(&self, chunk: usize, mask: u64) {
        self.chunks[chunk].fetch_and(!mask, Ordering::Release);
    }

    /// `true` if this specific gate is open.
    #[inline]
    pub fn is_open(&self, id: SignalId) -> bool {
        let chunk = (id.0 as usize) / 64;
        let bit = 1u64 << ((id.0 as usize) % 64);
        (self.chunks[chunk].load(Ordering::Acquire) & bit) != 0
    }

    /// `true` if **any** bit in `mask` is open (chunk 0 only — legacy API).
    #[inline]
    pub fn any_open(&self, mask: u64) -> bool {
        (self.chunks[0].load(Ordering::Acquire) & mask) != 0
    }

    /// `true` if **every** bit in `mask` is open (chunk 0 only — legacy API).
    #[inline]
    pub fn all_open(&self, mask: u64) -> bool {
        let s = self.chunks[0].load(Ordering::Acquire);
        (s & mask) == mask
    }

    /// Current raw state of chunk 0 (legacy API for ≤64 bits).
    /// For multi-chunk sets use [`state_chunk`](Self::state_chunk).
    #[inline]
    pub fn state(&self) -> u64 {
        self.chunks[0].load(Ordering::Acquire)
    }

    /// Raw state of a specific chunk (for `>64` bit sets).
    #[inline]
    pub fn state_chunk(&self, idx: usize) -> u64 {
        self.chunks[idx].load(Ordering::Acquire)
    }

    /// `true` if any bit across ALL chunks is set.
    #[inline]
    pub fn any_chunk_open(&self) -> bool {
        for c in self.chunks.iter() {
            if c.load(Ordering::Acquire) != 0 { return true; }
        }
        false
    }

    // ─────────────────────────────────────────────────────────────────────
    // Blocking waits
    // ─────────────────────────────────────────────────────────────────────

    /// Block until **any** gate in `mask` is open. Must be called from the
    /// thread registered via `set_worker`.
    #[inline]
    pub fn acquire_any(&self, mask: u64) {
        self.wait_until(|s| (s & mask) != 0);
    }

    /// Block until **every** gate in `mask` is open.
    #[inline]
    pub fn acquire_all(&self, mask: u64) {
        self.wait_until(|s| (s & mask) == mask);
    }

    /// Block until any gate is open (equivalent to `acquire_any(!0)`).
    #[inline]
    pub fn acquire(&self) {
        self.wait_until(|s| s != 0);
    }

    /// Generic spin-then-park wait over chunk 0 (legacy `u64` mask API).
    /// For multi-chunk sets use [`Self::wait_until_chunks`].
    #[inline]
    fn wait_until(&self, pred: impl Fn(u64) -> bool) {
        if pred(self.chunks[0].load(Ordering::Acquire)) { return; }
        for _ in 0..self.spin_iters {
            if pred(self.chunks[0].load(Ordering::Acquire)) { return; }
            std::hint::spin_loop();
        }
        // `parked.store(true)` needs SeqCst for the same Dekker-race reason
        // as `Signal`: producer's (fetch_or, parked.load) pair must not be
        // reorderable past consumer's (parked.store, state.load) pair.
        // Without SeqCst the producer could see parked=false, skip unpark;
        // consumer could see bits=0, then park — deadlock. Paid once per
        // park event, not per hot-path op.
        self.parked.store(true, Ordering::SeqCst);
        // Recheck after setting `parked` so a release that sneaks in between
        // the spin exit and the flag store still wakes us.
        while !pred(self.chunks[0].load(Ordering::Acquire)) {
            std::thread::park();
        }
        self.parked.store(false, Ordering::Relaxed);
    }

    /// Block until any bit across ALL chunks is set (multi-chunk variant of
    /// [`Self::acquire`]). For sets created via [`Self::with_capacity`] with
    /// `n_bits > 64`, this is the correct way to park: the legacy
    /// `acquire*(mask: u64)` only sees chunk 0.
    pub fn acquire_any_chunk(&self) {
        if self.any_chunk_open() { return; }
        for _ in 0..self.spin_iters {
            if self.any_chunk_open() { return; }
            std::hint::spin_loop();
        }
        self.parked.store(true, Ordering::SeqCst);
        while !self.any_chunk_open() {
            std::thread::park();
        }
        self.parked.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn create_and_lookup() {
        let mut set = SignalSet::new();
        let store = set.create("store");
        let drain = set.create("drain");
        assert_eq!(store.index(), 0);
        assert_eq!(drain.index(), 1);
        assert_eq!(set.id_of("store"), Some(store));
        assert_eq!(set.id_of("drain"), Some(drain));
        assert_eq!(set.id_of("nope"), None);
        assert_eq!(set.name(store), Some("store"));
    }

    #[test]
    fn release_lock_is_open() {
        let mut set = SignalSet::new();
        let a = set.create("a");
        let b = set.create("b");
        assert!(!set.is_open(a));
        set.release(a);
        assert!(set.is_open(a));
        assert!(!set.is_open(b));
        assert!(set.any_open(a.mask() | b.mask()));
        assert!(!set.all_open(a.mask() | b.mask()));
        set.release(b);
        assert!(set.all_open(a.mask() | b.mask()));
        set.lock(a);
        assert!(!set.is_open(a));
        assert!(set.is_open(b));
    }

    #[test]
    fn acquire_any_wakes_on_first_release() {
        let mut set = SignalSet::new();
        let a = set.create("a");
        let b = set.create("b");
        let set = Arc::new(set);
        let s = set.clone();

        let consumer = std::thread::spawn(move || {
            s.set_worker(std::thread::current());
            s.acquire_any(a.mask() | b.mask());
            assert!(s.any_open(a.mask() | b.mask()));
        });

        std::thread::sleep(Duration::from_millis(50));
        set.release(b);
        consumer.join().unwrap();
    }

    #[test]
    fn acquire_all_waits_for_full_mask() {
        let mut set = SignalSet::new();
        let a = set.create("a");
        let b = set.create("b");
        let set = Arc::new(set);
        let s = set.clone();

        let consumer = std::thread::spawn(move || {
            s.set_worker(std::thread::current());
            s.acquire_all(a.mask() | b.mask());
            assert!(s.all_open(a.mask() | b.mask()));
        });

        std::thread::sleep(Duration::from_millis(30));
        set.release(a);
        // Consumer must still be parked — only one bit open.
        std::thread::sleep(Duration::from_millis(30));
        set.release(b);
        consumer.join().unwrap();
    }

    #[test]
    fn many_producers_one_consumer() {
        use std::sync::atomic::AtomicBool;
        let mut set = SignalSet::new();
        let ids: Vec<_> = (0..8).map(|i| {
            set.create(Box::leak(format!("g{i}").into_boxed_str()))
        }).collect();
        let set = Arc::new(set);
        let mask_all: u64 = ids.iter().map(|id| id.mask()).fold(0, |a, b| a | b);
        let stop = Arc::new(AtomicBool::new(false));

        let s = set.clone();
        let st = stop.clone();
        let consumer = std::thread::spawn(move || {
            s.set_worker(std::thread::current());
            while !st.load(Ordering::Relaxed) {
                s.acquire_any(mask_all);
                let cur = s.state();
                s.lock_mask(cur);
            }
        });

        let producers: Vec<_> = ids.iter().copied().map(|id| {
            let s = set.clone();
            std::thread::spawn(move || {
                for _ in 0..25 {
                    s.release(id);
                    std::thread::yield_now();
                }
            })
        }).collect();

        for p in producers { p.join().unwrap(); }
        stop.store(true, Ordering::Relaxed);
        set.release(ids[0]);  // Kick the consumer if parked.
        consumer.join().unwrap();
    }
}
