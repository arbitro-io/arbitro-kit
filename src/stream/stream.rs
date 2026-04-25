//! `Stream<T>` — top-level type, lifecycle, and cursor accessors.
//!
//! Send/receive methods live in sibling modules (`send.rs`, `recv.rs`)
//! to keep this file focused on the structural and lifecycle concerns:
//! field layout, construction, drop-time draining, and the cheap
//! cursor / verification accessors.

use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

use crate::gate::Park;
use super::segment::{Segment, SEG_SIZE};

/// 64-byte cache-line padding to keep `tail_pos` (producer-written) and
/// `head_pos` (consumer-written) on separate lines. Without this, every
/// producer Release store on `tail_pos` would invalidate the line the
/// consumer reads `head_pos` from, throwing away most of the pipelining
/// benefit.
#[repr(align(64))]
pub(crate) struct CachePad(pub(crate) [u8; 0]);

/// Unbounded sequenced log primitive (SPSC).
///
/// ## What it is
///
/// A single-producer / single-consumer append-only log. The producer
/// `send`s items and gets back a [`Receipt`](super::Receipt) carrying
/// the monotonic seq of the message; the consumer drains in order.
/// Storage is a linked list of [`Segment`]s allocated on demand —
/// **the producer never blocks** while RAM is available.
///
/// ## What it isn't
///
/// - Not bounded. There is no `CAP`. RAM is the only ceiling.
/// - Not RPC-aware. There's no reply correlation built in. Use two
///   `Stream`s for bidirectional patterns; correlation is caller's job.
/// - Not multi-producer or multi-consumer. SPSC is the MVP topology.
///
/// ## Concurrency contract
///
/// - Exactly **one producer** thread calls `send` / `send_iter`.
/// - Exactly **one consumer** thread calls `recv` / `try_recv` /
///   `recv_bulk`.
/// - Any thread may hold a [`Receipt`](super::Receipt) and call
///   `is_delivered` / `wait_delivered` — these are read-only.
/// - Both sides typically share a stream via `Arc<Stream<T>>`.
///
/// ## Verification model
///
/// The consumer publishes its progress as `head_pos` (count of items
/// drained). A `Receipt(seq)` is "delivered" once `head_pos > seq`.
/// This collapses round-trip verification into a single Acquire load —
/// the foundation of the bench's 3 ns/RT result.
#[repr(C)]
pub struct Stream<T> {
    /// Park where the consumer waits when the stream is empty.
    /// Readiness predicate is evaluated against `head_pos < tail_pos`.
    pub(crate) not_empty: Park,

    /// Producer cursor: count of items the producer has published.
    /// After writing the slot for seq=N, the producer stores N+1 with
    /// Release. The Release pairs with the consumer's Acquire to make
    /// the slot write visible cross-thread.
    pub(crate) tail_pos: AtomicU64,
    pub(crate) _pad0: CachePad,

    /// Consumer cursor: count of items the consumer has drained.
    /// After consuming the slot for seq=N, the consumer stores N+1
    /// with Release. Receipt-holders Acquire-load this to verify
    /// delivery in O(1).
    pub(crate) head_pos: AtomicU64,
    pub(crate) _pad1: CachePad,

    /// Segment the producer is currently writing into. Updated only
    /// by the producer when it allocates a new segment.
    pub(crate) tail_seg: AtomicPtr<Segment<T>>,

    /// Segment the consumer is currently reading from. Also the
    /// oldest live segment. Updated only by the consumer when it
    /// drains past a segment boundary; the consumer also frees the
    /// old segment in the same step.
    pub(crate) head_seg: AtomicPtr<Segment<T>>,

    /// When `true`, `send` / `send_iter` insert a `fence(SeqCst)`
    /// between `tail_pos.store(Release)` and `not_empty.wake()`. This
    /// closes the Dekker race between this stream's `tail.store +
    /// parked.load` and the *peer* stream's `parked.store + tail.load`
    /// in bidirectional patterns where the same thread is producer
    /// here AND consumer on another stream (e.g. `Duplex`). The fence
    /// adds ~3-5 ns per send on x86; opt-in only.
    pub(crate) strict_wake: bool,
}

// Safety: slot access is partitioned by `head_pos` and `tail_pos`. The
// producer writes `slots[seq - base_seq]` exactly once for seq in
// `[tail_pos, tail_pos + 1)` then Release-stores `tail_pos + 1`. The
// consumer reads exactly once for seq in `[head_pos, tail_pos)` after
// Acquire-loading `tail_pos`. `tail_seg` is producer-owned (mutated
// only by producer); `head_seg` is consumer-owned. Cross-thread
// synchronization on segment writes (slot stores, `next` linkage)
// flows through the Release/Acquire on `tail_pos`.
unsafe impl<T: Send> Send for Stream<T> {}
unsafe impl<T: Send> Sync for Stream<T> {}

impl<T> Default for Stream<T> {
    fn default() -> Self { Self::new() }
}

impl<T> Stream<T> {
    /// Create an empty stream with one pre-allocated segment.
    /// Both cursors start at 0; `tail_seg == head_seg == first`.
    pub fn new() -> Self {
        Self::new_inner(false)
    }

    /// Like [`new`](Self::new) but enables strict cross-thread ordering
    /// in `send` / `send_iter`. Use this when the same thread acts as
    /// producer on this stream AND as consumer on another stream
    /// (the classic bidirectional pattern in `Duplex`). Adds an
    /// `mfence` per send on x86 (~3-5 ns).
    ///
    /// For purely unidirectional use (one thread always sends, a
    /// different thread always recvs), prefer [`new`](Self::new) — the
    /// race this guards against cannot fire there.
    pub fn new_strict() -> Self {
        Self::new_inner(true)
    }

    fn new_inner(strict_wake: bool) -> Self {
        let first = Segment::<T>::new_boxed(0);
        Self {
            not_empty: Park::new(),
            tail_pos: AtomicU64::new(0),
            _pad0: CachePad([]),
            head_pos: AtomicU64::new(0),
            _pad1: CachePad([]),
            tail_seg: AtomicPtr::new(first),
            head_seg: AtomicPtr::new(first),
            strict_wake,
        }
    }

    /// Register the consumer thread for blocking-recv wakeups.
    ///
    /// Must be called from the consumer thread before its first
    /// blocking `recv` on a possibly-empty stream. Calling it on an
    /// already-set stream replaces the previous registration; only
    /// one consumer is supported in this MVP.
    #[inline]
    pub fn set_consumer(&self, t: std::thread::Thread) {
        self.not_empty.set_worker(t);
    }

    /// Total number of items the producer has published so far.
    /// One Acquire load.
    #[inline]
    pub fn tail(&self) -> u64 {
        self.tail_pos.load(Ordering::Acquire)
    }

    /// Total number of items the consumer has drained so far.
    /// One Acquire load. This is what [`Receipt::is_delivered`]
    /// compares against.
    #[inline]
    pub fn cursor(&self) -> u64 {
        self.head_pos.load(Ordering::Acquire)
    }

    /// Approximate number of unread items (`tail - cursor`).
    /// Snapshot only — both cursors may move between the two loads.
    #[inline]
    pub fn len(&self) -> u64 {
        self.tail().saturating_sub(self.cursor())
    }

    /// `true` if the stream currently has no items the consumer
    /// hasn't read yet. Snapshot only.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cursor() >= self.tail()
    }

    /// Block until the consumer has drained at least up to `seq`.
    ///
    /// **MVP**: busy-spins on `head_pos`. Future work may switch to
    /// a parked variant for callers that prefer to give up the core.
    /// Used by [`Receipt::wait_delivered`] internally.
    #[inline]
    pub fn wait_for(&self, seq: u64) {
        while self.cursor() < seq {
            std::hint::spin_loop();
        }
    }
}

impl<T> Drop for Stream<T> {
    /// Drain any unread items so RAII payloads (`Box`, `Vec`, `Arc`,
    /// `File`) are released, then free every segment.
    fn drop(&mut self) {
        // We have unique access here, so plain non-atomic reads of
        // the cursors are safe.
        let tail = *self.tail_pos.get_mut();
        let mut head = *self.head_pos.get_mut();
        let mut seg = *self.head_seg.get_mut();

        // Drop initialized payloads in slots `[head, tail)`.
        while head < tail {
            let seg_ref = unsafe { &*seg };
            if !seg_ref.contains(head) {
                let next = unsafe { (*seg).next.load(Ordering::Relaxed) };
                debug_assert!(!next.is_null());
                let old = seg;
                seg = next;
                unsafe { drop(Box::from_raw(old)); }
                continue;
            }
            let idx = seg_ref.idx(head);
            // Read + drop in one move.
            unsafe { let _ = (*seg_ref.slots[idx].get()).assume_init_read(); }
            head += 1;
        }

        // Free remaining segments (head's segment + any trailing empty
        // segments allocated past tail_pos).
        loop {
            let next = unsafe { (*seg).next.load(Ordering::Relaxed) };
            unsafe { drop(Box::from_raw(seg)); }
            if next.is_null() { break; }
            seg = next;
        }
    }
}

// Re-export SEG_SIZE for tests and downstream code that needs it.
#[allow(dead_code)]
pub(crate) const STREAM_SEG_SIZE: usize = SEG_SIZE;
