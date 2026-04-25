//! Consumer-side methods for `Stream<T>`.
//!
//! All methods here must be called from the single consumer thread.
//! They mutate `head_pos` and `head_seg` and free segments as they
//! cross boundaries. They never mutate producer state.

use std::sync::atomic::Ordering;

use crate::gate::{Cancelled, Lifeline, WaiterId};

use super::segment::Segment;
use super::stream::Stream;

impl<T> Stream<T> {
    /// Non-blocking dequeue. Returns the next item if one is available,
    /// or `None` if the stream is currently empty.
    ///
    /// Cost: ~3–5 ns in steady state (two atomic loads on the cursors,
    /// one Release store on `head_pos`, plus the slot read).
    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        let head = self.head_pos.load(Ordering::Relaxed);
        let tail = self.tail_pos.load(Ordering::Acquire);
        if head >= tail {
            return None;
        }

        let mut seg = self.head_seg.load(Ordering::Relaxed);

        // Cross any segment boundaries until we find the one
        // containing `head`. In steady state this loop runs zero
        // times (we're inside the current segment) and runs once
        // every `SEG_SIZE` items at the wrap-around.
        loop {
            let seg_ref = unsafe { &*seg };
            if seg_ref.contains(head) { break; }
            seg = self.advance_seg(seg);
        }

        let seg_ref = unsafe { &*seg };
        let idx = seg_ref.idx(head);
        // Safety: head < tail and producer Released after writing the
        // slot, so the cell at idx is initialized and visible here.
        let value = unsafe { (*seg_ref.slots[idx].get()).assume_init_read() };

        // Publish: any holder of a `Receipt(head)` who Acquires
        // head_pos will see "delivered".
        self.head_pos.store(head + 1, Ordering::Release);
        Some(value)
    }

    /// Blocking dequeue. Parks (phased backoff via `Park`) until at
    /// least one item is available, then takes it.
    ///
    /// Must only be called from the registered consumer thread (see
    /// [`Stream::set_consumer`]).
    #[inline]
    pub fn recv(&self) -> T {
        loop {
            if let Some(v) = self.try_recv() { return v; }
            self.not_empty.wait_until(|| {
                self.head_pos.load(Ordering::Relaxed)
                    < self.tail_pos.load(Ordering::Acquire)
            });
        }
    }

    /// Blocking dequeue with cancellation. Returns `Err(Cancelled)` if
    /// the lifeline cancels this waiter while we are parked or before
    /// we enter park. Otherwise behaves exactly like [`Stream::recv`].
    ///
    /// Performance: adopting this method costs ~1 extra atomic load
    /// per spin iteration vs `recv()`. The plain `recv()` path is
    /// **unchanged** — callers that don't use Lifeline pay nothing.
    #[inline]
    pub fn recv_or_cancel(
        &self,
        life: &Lifeline,
        id: WaiterId,
    ) -> Result<T, Cancelled> {
        loop {
            if let Some(v) = self.try_recv() { return Ok(v); }
            if life.is_cancelled(id)        { return Err(Cancelled); }

            // Park's predicate runs in spin and after the SeqCst
            // `parked.store(true)`. Lifeline::cancel_* unparks the
            // registered thread; on wake the predicate becomes true
            // and `wait_until` returns.
            self.not_empty.wait_until(|| {
                self.head_pos.load(Ordering::Relaxed)
                    < self.tail_pos.load(Ordering::Acquire)
                    || life.is_cancelled(id)
            });
        }
    }

    /// Drain up to `max` items into `buf`. **Non-blocking** — returns
    /// the count actually drained, which may be 0 if the stream is
    /// empty. Drained items are appended to `buf`.
    ///
    /// This is the natural batched-recv API. Combine with `recv` for
    /// a "block on first, then drain whatever else is buffered" loop.
    pub fn recv_bulk(&self, buf: &mut Vec<T>, max: usize) -> usize {
        let mut drained = 0;
        for _ in 0..max {
            match self.try_recv() {
                Some(v) => { buf.push(v); drained += 1; }
                None => break,
            }
        }
        drained
    }

    // ─── internal helpers ─────────────────────────────────────────────────

    /// Advance `head_seg` to the next segment, freeing the old one.
    /// Caller must have established that `head` is past the current
    /// segment's range AND that `head < tail_pos` (so the producer
    /// has published a `next`).
    #[inline(never)]
    fn advance_seg(&self, cur_seg: *mut Segment<T>) -> *mut Segment<T> {
        let cur_ref = unsafe { &*cur_seg };
        let next = cur_ref.next.load(Ordering::Acquire);
        debug_assert!(
            !next.is_null(),
            "next must be linked when head crosses a segment boundary"
        );

        // Update the consumer's segment pointer first, then free the
        // old one. The producer never reads `head_seg`, so Relaxed.
        self.head_seg.store(next, Ordering::Relaxed);

        // Safety: we are the only consumer. `cur_seg` was allocated
        // by the producer via `Box::into_raw` (in `Segment::new_boxed`
        // or `Stream::new`); we now own it exclusively.
        unsafe { drop(Box::from_raw(cur_seg)); }
        next
    }
}
