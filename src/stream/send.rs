//! Producer-side methods for `Stream<T>`.
//!
//! All methods here must be called from the single producer thread.
//! They mutate `tail_pos` and `tail_seg` and never touch consumer state.

use std::sync::atomic::Ordering;

use crate::waiter::Waiter;

use super::receipt::Receipt;
use super::segment::{Segment, SEG_SIZE};
use super::stream::Stream;

impl<T, W: Waiter> Stream<T, W> {
    /// Append one item. Returns a [`Receipt`] carrying its sequence
    /// number. **Never blocks** — if the current segment is full, a
    /// fresh one is allocated on the spot.
    ///
    /// Cost in steady state: ~5–10 ns (one atomic store on `tail_pos`,
    /// one Park wake-check, plus the slot write itself). Allocation
    /// happens once every `SEG_SIZE` calls.
    #[inline]
    pub fn send(&self, value: T) -> Receipt {
        let seq = self.tail_pos.load(Ordering::Relaxed);
        let seg = self.locate_or_alloc_seg(seq);

        // Safety: producer is the sole writer of slot[idx] for this
        // seq; the slot has not been initialized yet (or was consumed
        // and the segment freed).
        let seg_ref = unsafe { &*seg };
        let idx = seg_ref.idx(seq);
        unsafe { (*seg_ref.slots[idx].get()).write(value); }

        // Publish: Release pairs with consumer's Acquire on tail_pos
        // to make the slot write visible cross-thread.
        self.tail_pos.store(seq + 1, Ordering::Release);

        // Strict streams (Duplex) need an SC fence here to close the
        // Dekker race against the peer-stream's parked.store + recheck.
        // Cheap branch on a const-during-stream-lifetime field.
        if self.strict_wake {
            std::sync::atomic::fence(Ordering::SeqCst);
        }

        // Wake consumer if parked. The wake call itself does an early
        // Relaxed-load of the parked flag; no syscall in the common
        // case where the consumer is already running.
        self.not_empty.wake();

        Receipt::new(seq)
    }

    /// Append every item from an iterator in one batch. Returns
    /// `Some(Receipt)` for the **last** item if the iterator yielded
    /// at least one element, or `None` if it yielded nothing.
    ///
    /// Amortizes the publish + wake over the whole batch — exactly
    /// one Release store on `tail_pos` and one wake call regardless
    /// of how many items are pushed. Per-item cost reduces to "write
    /// slot + segment-bound check" (~2–3 ns/item in steady state).
    pub fn send_iter<I: IntoIterator<Item = T>>(&self, items: I) -> Option<Receipt> {
        let start_seq = self.tail_pos.load(Ordering::Relaxed);
        let mut current_seq = start_seq;

        // Cache the producer-segment pointer locally to avoid an
        // atomic load per item.
        let mut seg = self.tail_seg.load(Ordering::Relaxed);

        for value in items {
            let seg_ref = unsafe { &*seg };
            if !seg_ref.contains(current_seq) {
                seg = self.alloc_next_seg(seg);
            }
            let seg_ref = unsafe { &*seg };
            let idx = seg_ref.idx(current_seq);
            unsafe { (*seg_ref.slots[idx].get()).write(value); }
            current_seq += 1;
        }

        if current_seq == start_seq {
            return None;
        }

        // One Release for the whole batch.
        self.tail_pos.store(current_seq, Ordering::Release);

        if self.strict_wake {
            std::sync::atomic::fence(Ordering::SeqCst);
        }

        self.not_empty.wake();

        Some(Receipt::new(current_seq - 1))
    }

    // ─── internal helpers ─────────────────────────────────────────────────

    /// Locate (or allocate) the segment containing `seq` from the
    /// producer's current `tail_seg`. Returns the resulting `*mut
    /// Segment<T>`, with `tail_seg` updated.
    #[inline]
    fn locate_or_alloc_seg(&self, seq: u64) -> *mut Segment<T> {
        let seg = self.tail_seg.load(Ordering::Relaxed);
        let seg_ref = unsafe { &*seg };
        if seg_ref.contains(seq) {
            seg
        } else {
            self.alloc_next_seg(seg)
        }
    }

    /// Allocate the next segment in the chain after `cur_seg`, link
    /// it via Release on `cur_seg.next`, advance `tail_seg`, and
    /// return the new pointer.
    #[inline(never)]
    fn alloc_next_seg(&self, cur_seg: *mut Segment<T>) -> *mut Segment<T> {
        let cur_ref = unsafe { &*cur_seg };
        let new_base = cur_ref.base_seq + SEG_SIZE as u64;
        let new_seg = Segment::<T>::new_boxed(new_base);

        // Publish the link FIRST: any consumer following `next` will
        // get either null (and check tail_pos again) or a fully
        // constructed new segment.
        cur_ref.next.store(new_seg, Ordering::Release);

        // Producer-only field; Relaxed is enough.
        self.tail_seg.store(new_seg, Ordering::Relaxed);
        new_seg
    }
}
