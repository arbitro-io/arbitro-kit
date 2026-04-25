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

/// Maximum number of gates a `SignalSet` can host.
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
    /// # Panics
    /// Panics at compile-time (when used in `const`) or runtime if
    /// `idx >= MAX_GATES`.
    pub const fn new(idx: u8) -> Self {
        assert!((idx as usize) < MAX_GATES, "SignalId index out of range");
        Self(idx)
    }

    #[inline]
    pub const fn index(self) -> u8 { self.0 }

    /// Bit mask with only this gate's bit set.
    #[inline]
    pub const fn mask(self) -> u64 { 1u64 << self.0 }
}

/// Default spin iterations before parking. Same rationale as
/// [`crate::gate::DEFAULT_SPIN_ITERS`].
pub const DEFAULT_SPIN_ITERS: u32 = 512;

#[repr(align(64))]
pub struct SignalSet {
    /// Bit `i` set = gate `i` is open. Read with `Acquire`, mutated with
    /// `fetch_or(Release)` / `fetch_and(Relaxed)`.
    state: AtomicU64,
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
    names: UnsafeCell<[Option<&'static str>; MAX_GATES]>,
    /// Next free slot for `create()`. Mutated only pre-share via `&mut self`.
    next_id: UnsafeCell<u8>,
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
    pub const fn new() -> Self { Self::with_spin(DEFAULT_SPIN_ITERS) }

    /// Construct with a custom spin-iteration budget. See [`Signal::with_spin`]
    /// for semantics. 0 = always park immediately after the fast-path check.
    pub const fn with_spin(spin_iters: u32) -> Self {
        Self {
            state:   AtomicU64::new(0),
            parked:  AtomicBool::new(false),
            spin_iters,
            worker:  UnsafeCell::new(None),
            names:   UnsafeCell::new([None; MAX_GATES]),
            next_id: UnsafeCell::new(0),
        }
    }

    /// Register a new gate. Returns its typed handle. Callable only while the
    /// set is unshared (`&mut self` enforces this at compile time).
    ///
    /// # Panics
    /// Panics if more than [`MAX_GATES`] are registered.
    pub fn create(&mut self, name: &'static str) -> SignalId {
        // Safety: `&mut self` guarantees exclusive access; no concurrent reads.
        let id = unsafe {
            let next = &mut *self.next_id.get();
            assert!((*next as usize) < MAX_GATES, "SignalSet: MAX_GATES exceeded");
            let id = *next;
            *next += 1;
            (*self.names.get())[id as usize] = Some(name);
            id
        };
        SignalId(id)
    }

    /// Debug name for a gate, if registered. Cold-path helper for tracing.
    pub fn name(&self, id: SignalId) -> Option<&'static str> {
        // Safety: `names` is only mutated through `&mut self`; here we only
        // read, which is sound as long as `create` is not in flight — which
        // `&mut self` rules out anyway.
        unsafe { (*self.names.get())[id.0 as usize] }
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

    /// Register the consumer thread. Must be called before the set is shared
    /// with producers. Typically the consumer calls
    /// `set.set_worker(thread::current())`.
    pub fn set_worker(&self, t: std::thread::Thread) {
        // Safety: documented pre-share contract.
        unsafe { *self.worker.get() = Some(t); }
    }

    /// Open one gate. Lock-free. Wakes the consumer iff it was parked and this
    /// call actually flipped the bit (coalesces repeated releases).
    #[inline]
    pub fn release(&self, id: SignalId) {
        let bit = id.mask();
        let prev = self.state.fetch_or(bit, Ordering::Release);
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
    /// are published before the bit is cleared. Producers observing the
    /// cleared bit via Acquire therefore see a committed consumer read.
    /// Same cost as Relaxed on x86; one `dmb ish st` on ARM.
    #[inline]
    pub fn lock(&self, id: SignalId) {
        self.state.fetch_and(!id.mask(), Ordering::Release);
    }

    /// Close every bit in `mask`. Useful to clear a subset in one op.
    /// See [`lock`](Self::lock) for the `Release` ordering rationale.
    #[inline]
    pub fn lock_mask(&self, mask: u64) {
        self.state.fetch_and(!mask, Ordering::Release);
    }

    /// `true` if this specific gate is open.
    #[inline]
    pub fn is_open(&self, id: SignalId) -> bool {
        (self.state.load(Ordering::Acquire) & id.mask()) != 0
    }

    /// `true` if **any** bit in `mask` is open.
    #[inline]
    pub fn any_open(&self, mask: u64) -> bool {
        (self.state.load(Ordering::Acquire) & mask) != 0
    }

    /// `true` if **every** bit in `mask` is open.
    #[inline]
    pub fn all_open(&self, mask: u64) -> bool {
        let s = self.state.load(Ordering::Acquire);
        (s & mask) == mask
    }

    /// Current raw state. Bit `i` = gate `i` open.
    #[inline]
    pub fn state(&self) -> u64 {
        self.state.load(Ordering::Acquire)
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

    /// Generic spin-then-park wait. Predicate sees the raw state.
    #[inline]
    fn wait_until(&self, pred: impl Fn(u64) -> bool) {
        if pred(self.state.load(Ordering::Acquire)) { return; }
        for _ in 0..self.spin_iters {
            if pred(self.state.load(Ordering::Acquire)) { return; }
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
        while !pred(self.state.load(Ordering::Acquire)) {
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
