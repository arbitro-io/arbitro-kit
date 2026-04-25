//! Linked segments for `Stream<T>` storage.
//!
//! Each segment holds a fixed number of slots indexed by global sequence
//! number. The producer allocates a new segment whenever it walks off the
//! end of the current one; the consumer frees segments as it drains past
//! them. Segments are linked through an `AtomicPtr<Segment<T>>` published
//! with Release by the producer and followed with Acquire by the consumer.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::AtomicPtr;

/// Slots per segment.
///
/// Larger = fewer allocations (1 alloc per `SEG_SIZE` messages), longer
/// runs without segment-boundary crossings, more memory granularity.
/// 256 strikes a balance: with `u64` payload that's ~2 KB per segment +
/// header — fits comfortably in L1.
pub(crate) const SEG_SIZE: usize = 256;

/// One link in the unbounded chain. Holds `SEG_SIZE` slots starting at
/// `base_seq` and a pointer to the next segment (or null).
///
/// Layout note: `base_seq` and `next` are read by both producer and
/// consumer; slots are partitioned by the global cursors so each cell
/// is written once by the producer and read once by the consumer.
#[repr(C)]
pub(crate) struct Segment<T> {
    /// Sequence number of `slots[0]`. Always a multiple of `SEG_SIZE`.
    pub(crate) base_seq: u64,

    /// Pointer to the next segment in the chain, or null if this is
    /// currently the tail. Producer publishes a new segment with
    /// Release; consumer follows with Acquire.
    pub(crate) next: AtomicPtr<Segment<T>>,

    /// Slot storage. Producer writes `slots[seq - base_seq]` exactly
    /// once. Consumer reads it exactly once. Synchronization is via
    /// the global producer cursor (Release on the cursor publishes the
    /// slot write).
    pub(crate) slots: [UnsafeCell<MaybeUninit<T>>; SEG_SIZE],
}

impl<T> Segment<T> {
    /// Allocate a fresh segment whose first slot has sequence `base_seq`.
    /// Returned as a raw pointer so the caller controls deallocation —
    /// consumer drops via `Box::from_raw` once it has drained past it.
    #[inline]
    pub(crate) fn new_boxed(base_seq: u64) -> *mut Self {
        let boxed = Box::new(Self {
            base_seq,
            next: AtomicPtr::new(ptr::null_mut()),
            slots: std::array::from_fn(|_| UnsafeCell::new(MaybeUninit::uninit())),
        });
        Box::into_raw(boxed)
    }

    /// True if `seq` falls within this segment's range.
    #[inline]
    pub(crate) fn contains(&self, seq: u64) -> bool {
        seq >= self.base_seq && seq < self.base_seq + SEG_SIZE as u64
    }

    /// Index within `slots` for `seq`. Caller must ensure `contains(seq)`.
    #[inline]
    pub(crate) fn idx(&self, seq: u64) -> usize {
        debug_assert!(self.contains(seq), "seq {} not in segment [{}, {})",
                      seq, self.base_seq, self.base_seq + SEG_SIZE as u64);
        (seq - self.base_seq) as usize
    }
}

// Safety: slot access is partitioned by the cursors in `Stream<T>`. The
// producer-only writes to `slots[seq - base_seq]` for `seq` in
// `[head_pos, tail_pos)`; the consumer-only reads them once `tail_pos`
// has been bumped (via Release/Acquire). `next` is written-once by the
// producer (Release) and read-many by the consumer (Acquire).
unsafe impl<T: Send> Send for Segment<T> {}
unsafe impl<T: Send> Sync for Segment<T> {}
