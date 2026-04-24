//! Single-producer enqueue path: [`Synapse::try_send`] and [`Synapse::send`].
//!
//! Only the thread registered via [`Synapse::set_producer`] may call
//! these methods. The producer owns the `head` cursor exclusively.
//!
//! ## Hot path
//!
//! 1. Read own `head` (Relaxed — single writer).
//! 2. Read consumers' `tail` (Acquire).
//! 3. If `head - tail >= CAP`, return `Err(value)` (or park in `send`).
//! 4. Write `slot[head & MASK]` in place.
//! 5. Release-store `head + 1` → publishes slot to consumers.
//! 6. `SeqCst` fence → pairs with consumer's `idle_mask.fetch_or(SeqCst)`
//!    for Dekker closure.
//! 7. `wake_consumers()` — O(1) targeted unpark.

use std::sync::atomic::Ordering;

use super::state::Synapse;

impl<T, const CAP: usize, const N: usize> Synapse<T, CAP, N> {
    /// Non-blocking enqueue. Returns `Err(value)` if the ring is full.
    ///
    /// Must only be called from the single producer thread.
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        if head.wrapping_sub(tail) >= CAP {
            return Err(value);
        }
        // Safety: slot is empty (head - tail < CAP); producer owns the
        // write side.
        unsafe { (*self.slots[head & Self::MASK].get()).write(value); }
        // Release publishes the slot to the consumer side.
        self.head.store(head.wrapping_add(1), Ordering::Release);
        // Dekker closure: the SeqCst fence here pairs with the consumer's
        // `idle_mask.fetch_or(SeqCst)` in the park path. Without this fence
        // the Release store above and the SeqCst load inside
        // `wake_consumers` are not totally-ordered, and the load could
        // observe an empty mask **before** the new head becomes visible
        // to a consumer that is about to park — a classic lost-wakeup.
        std::sync::atomic::fence(Ordering::SeqCst);
        // Wake at most one idle consumer (O(1) via idle_mask).
        self.wake_consumers();
        Ok(())
    }

    /// Blocking enqueue. Parks on `not_full` when the ring is full, then
    /// enqueues once a consumer advances `tail`.
    ///
    /// Must only be called from the registered producer thread.
    ///
    /// ## Park protocol (lock → fence → recheck → acquire)
    ///
    /// Same canonical pattern as [`crate::gate::Ring::send`]. The external
    /// `SeqCst` fence between `not_full.lock()` and the `is_full()`
    /// recheck ensures any consumer Release on `tail` that preceded our
    /// lock is globally visible before we commit to sleeping.
    #[inline]
    pub fn send(&self, mut value: T) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            self.not_full.lock();
            std::sync::atomic::fence(Ordering::SeqCst);
            if !self.is_full() {
                self.not_full.release();
                continue;
            }
            self.not_full.acquire();
        }
    }
}
