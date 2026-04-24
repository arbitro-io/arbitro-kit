//! Single-producer enqueue path: [`Synapse::try_send`] and [`Synapse::send`].
//!
//! Only the thread registered via [`Synapse::set_producer`] may call
//! these methods. The producer owns the `head` cursor exclusively.
//!
//! ## Hot path (Vyukov per-slot handshake)
//!
//! 1. Read own `head` (Relaxed — single writer).
//! 2. Acquire-load `seq[head & MASK]`.
//! 3. If `seq != head` the slot is still owned by a consumer from the
//!    previous wrap → return `Err(value)` (or park in `send`).
//! 4. Write `slot[head & MASK]` in place.
//! 5. Release-store `seq = head + 1` → publishes slot to consumers.
//! 6. Advance `head` (Release) — cursor is purely informational under
//!    Vyukov but still useful for `len()` / `is_full()` / park rechecks.
//! 7. `SeqCst` fence → pairs with consumer's `idle_mask.fetch_or(SeqCst)`
//!    for Dekker closure on wake.
//! 8. `wake_consumers()` — O(1) targeted unpark.
//!
//! ### Why per-slot seq and not `head - tail < CAP`?
//!
//! `tail` advances when a consumer wins the CAS on it — that is, at
//! **claim time**, not at **read-commit time**. Between those two
//! moments the slot is still owned by the consumer. A producer that
//! only consults `tail` can conclude "slot free" and overwrite a slot
//! the consumer is mid-read, corrupting data. `seq` tracks the slot's
//! actual state (empty/written) per-slot, closing that window.

use std::sync::atomic::Ordering;

use super::state::Synapse;

impl<T, const CAP: usize, const N: usize> Synapse<T, CAP, N> {
    /// Non-blocking enqueue. Returns `Err(value)` if the ring is full.
    ///
    /// Must only be called from the single producer thread.
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed);
        let slot = &self.slots[head & Self::MASK];
        // Acquire-load the slot's seq. If it equals `head`, the slot is
        // empty and waiting for this exact wrap. If it's `head - CAP`
        // (i.e. one wrap behind), the slot is still owned by a consumer
        // from the previous wrap — the ring is effectively full for us.
        let seq = slot.seq.load(Ordering::Acquire);
        if seq != head {
            // Slot not ready for our wrap. Single producer, so the only
            // cause is consumer lag on slot `slot_idx`. Treat as full.
            return Err(value);
        }
        // Safety: seq == head proves the slot is empty and nobody else
        // may touch it until we publish. Producer is single-threaded.
        unsafe { (*slot.cell.get()).write(value); }
        // Release-publish the slot to any consumer waiting on this seq.
        // This is the only store that correctness depends on — the head
        // cursor below is purely informational.
        slot.seq.store(head.wrapping_add(1), Ordering::Release);
        // Advance head. Relaxed is sufficient under Vyukov: consumers
        // use `seq` (not `head`) as the read-authority for availability;
        // `head` only feeds `len()` / `is_full()` / the park recheck,
        // none of which require ordering beyond "eventually visible."
        self.head.store(head.wrapping_add(1), Ordering::Relaxed);
        // Dekker closure: the SeqCst fence here pairs with the consumer's
        // `idle_mask.fetch_or(SeqCst)` in the park path. Without this fence
        // the Release stores above and the SeqCst load inside
        // `wake_consumers` are not totally-ordered, and the load could
        // observe an empty mask **before** the new seq becomes visible
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
