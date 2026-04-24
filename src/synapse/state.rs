//! Struct definition, construction, read-only accessors, and `Drop`.
//!
//! This file owns the concrete layout of `Synapse<T, CAP, N>` and the
//! invariants around cache-line placement. Behaviour (send/recv/wake)
//! lives in sibling files that extend this struct via additional
//! `impl` blocks.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use crate::gate::Signal;

/// Cache-line padding keeps hot atomics on separate 64 B lines.
#[repr(align(64))]
pub(super) struct CachePad(pub(super) [u8; 0]);

/// Returned by [`Synapse::recv`] after the channel has been shut down
/// and no further work will arrive. The caller is expected to break out
/// of its consumer loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shutdown;

/// SPMC bounded ring with `CAP` slots (power-of-two) and `N` consumers
/// (`N > 0`, `N ≤ 64`).
///
/// See module-level docs for the full wire model and cost breakdown.
///
/// The struct is `#[repr(C, align(64))]` so the whole allocation (even
/// when placed inside an `Arc`) lands on a 64-byte boundary. This keeps
/// `head` on its own cache line and prevents the allocator's refcount
/// header from polluting hot atomics.
#[repr(C, align(64))]
pub struct Synapse<T, const CAP: usize, const N: usize> {
    /// Write cursor. Single writer (the producer). Monotonic, `& MASK` on use.
    pub(super) head: AtomicUsize,
    pub(super) _pad0: CachePad,

    /// Claim cursor. Multiple writers (N consumers) via CAS. Monotonic.
    pub(super) tail: AtomicUsize,
    pub(super) _pad1: CachePad,

    /// Signal to parked producer when a slot frees up. Opens when any
    /// consumer advances `tail`. Producer parks here if `send` finds full.
    pub(super) not_full: Signal,

    /// Shutdown flag. Set by [`Synapse::shutdown`] to wake all consumers.
    pub(super) shutdown: AtomicBool,
    pub(super) _pad2: CachePad,

    /// Bit `i` set ⇔ consumer `i` is about to park (or is parked).
    ///
    /// Used by the producer for O(1) targeted wakeup: after publishing a
    /// slot, the producer reads this mask once, picks any set bit, clears
    /// it with `fetch_and`, and only then calls `signals[i].release()`.
    ///
    /// The consumer sets its bit with `SeqCst` RMW before the final
    /// emptiness recheck, and the producer uses `SeqCst` after its head
    /// publish before reading the mask. Together they form a Dekker
    /// closure: either the producer sees the bit (and wakes the consumer),
    /// or the consumer sees the new `head` on recheck (and retries
    /// without parking). No wakeup can be lost.
    pub(super) idle_mask: AtomicU64,
    pub(super) _pad3: CachePad,

    /// Per-consumer wakeup signals. `signals[i].release()` wakes consumer
    /// `i`; `signals[i].acquire()` parks it. Each `Signal` is already
    /// `#[repr(align(64))]` so the array naturally distributes across
    /// cache lines.
    pub(super) signals: [Signal; N],

    /// Slot storage. Each cell transitions empty → init → empty exactly
    /// once per wrap, coordinated by `head`/`tail` + the signals.
    pub(super) slots: [UnsafeCell<MaybeUninit<T>>; CAP],
}

// Safety: slot access is serialized by (head, tail, CAS claim). The
// producer only writes `slot[head & MASK]` when `head - tail < CAP` (the
// slot is empty); a consumer only reads `slot[t & MASK]` after winning
// the CAS on `tail` from `t` to `t + 1`, which proves exclusive
// ownership of that slot index. The Release store on `head` publishes
// the slot write to consumers; the AcqRel on the tail CAS synchronizes
// the claim across consumers.
unsafe impl<T: Send, const CAP: usize, const N: usize> Send for Synapse<T, CAP, N> {}
unsafe impl<T: Send, const CAP: usize, const N: usize> Sync for Synapse<T, CAP, N> {}

impl<T, const CAP: usize, const N: usize> Default for Synapse<T, CAP, N> {
    fn default() -> Self { Self::new() }
}

impl<T, const CAP: usize, const N: usize> Synapse<T, CAP, N> {
    /// Bitmask for `head & MASK` / `tail & MASK` indexing. Relies on CAP
    /// being a power of two (enforced in `new`).
    pub(super) const MASK: usize = CAP - 1;

    /// Create a fresh `Synapse`. Empty ring, all consumers unbound.
    ///
    /// # Panics
    /// - If `CAP == 0` or `CAP` is not a power of two.
    /// - If `N == 0` or `N > 64`.
    pub fn new() -> Self {
        assert!(CAP > 0,               "Synapse CAP must be > 0");
        assert!(CAP.is_power_of_two(), "Synapse CAP must be a power of two");
        assert!(N > 0,                 "Synapse N must be > 0");
        assert!(N <= 64,               "Synapse N must be <= 64");

        let signals: [Signal; N] = std::array::from_fn(|_| Signal::new());
        let slots: [UnsafeCell<MaybeUninit<T>>; CAP] =
            std::array::from_fn(|_| UnsafeCell::new(MaybeUninit::uninit()));

        let not_full = Signal::new();
        not_full.release(); // empty ring has space

        Self {
            head: AtomicUsize::new(0),
            _pad0: CachePad([]),
            tail: AtomicUsize::new(0),
            _pad1: CachePad([]),
            not_full,
            shutdown: AtomicBool::new(false),
            _pad2: CachePad([]),
            idle_mask: AtomicU64::new(0),
            _pad3: CachePad([]),
            signals,
            slots,
        }
    }

    /// Maximum number of buffered items.
    #[inline] pub const fn capacity(&self) -> usize { CAP }

    /// Number of consumer slots.
    #[inline] pub const fn consumers(&self) -> usize { N }

    /// Approximate number of pending items. Both cursors may advance
    /// between the two loads under concurrent access.
    #[inline]
    pub fn len(&self) -> usize {
        let h = self.head.load(Ordering::Acquire);
        let t = self.tail.load(Ordering::Acquire);
        h.wrapping_sub(t)
    }

    /// Approximate emptiness check.
    #[inline] pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// Approximate fullness check.
    #[inline] pub fn is_full(&self) -> bool { self.len() >= CAP }

    /// Register the producer thread so it can park on a full ring.
    /// Must be called from the producer thread before the first
    /// blocking [`send`](Self::send) that could block.
    #[inline]
    pub fn set_producer(&self, t: std::thread::Thread) {
        self.not_full.set_worker(t);
    }

    /// Register consumer `i`'s thread for unpark. Must be called from
    /// the thread that will run [`recv(i)`](Self::recv) before the
    /// first blocking `recv` on a possibly-empty ring.
    ///
    /// # Panics
    /// If `i >= N`.
    #[inline]
    pub fn bind_consumer(&self, i: usize) {
        assert!(i < N, "consumer index {} >= N={}", i, N);
        self.signals[i].set_worker(std::thread::current());
    }
}

impl<T, const CAP: usize, const N: usize> Drop for Synapse<T, CAP, N> {
    fn drop(&mut self) {
        // `&mut self` means no other references exist. Drain in-flight
        // [tail, head) — these slots hold initialized T that must be
        // dropped to avoid leaking RAII resources.
        let head = *self.head.get_mut();
        let tail = *self.tail.get_mut();
        let mut i = tail;
        while i != head {
            // Safety: slot[i & MASK] was published by the producer and
            // never claimed by a consumer.
            unsafe { (*self.slots[i & Self::MASK].get()).assume_init_drop(); }
            i = i.wrapping_add(1);
        }
    }
}
