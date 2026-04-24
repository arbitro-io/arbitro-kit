//! N-consumer dequeue path: [`Synapse::try_recv`] and [`Synapse::recv`].
//!
//! Any of the `N` consumer threads may call these concurrently. Claims
//! are serialized by a CAS loop on the shared `tail` cursor; once a
//! consumer wins the CAS for index `t`, it waits for the slot's `seq`
//! to reach `t + 1` (written by the producer), reads, then stores
//! `seq = t + CAP` to release the slot for the producer's next wrap.
//!
//! ## Vyukov per-slot handshake
//!
//! The check `seq[t & MASK] == t + 1` replaces `t < head` as the
//! availability test. This matters because under a stale-`head` /
//! stale-`tail` reading, we cannot distinguish "slot freshly written
//! by producer" from "slot written two wraps ago and already consumed."
//! `seq` disambiguates: each successful producer→consumer exchange
//! advances the slot's seq by exactly `CAP`, so the match
//! `seq == t + 1` is unique to the wrap `t` belongs to.
//!
//! ## Park protocol (park path of `recv`)
//!
//! 1. Fast path: try `try_recv`; if it returns a value, done.
//! 2. Observed empty → check `shutdown` flag before committing to park.
//! 3. `signals[i].lock()` (close own signal).
//! 4. `idle_mask.fetch_or(bit_i, SeqCst)` — advertise idle with a full
//!    fence (producer pairs with `SeqCst` after its head publish).
//! 5. Recheck `is_empty` / `shutdown` — if state changed, clear bit and
//!    retry the fast path.
//! 6. `signals[i].acquire()` — park. Wake clears bit defensively.
//!
//! The SeqCst RMW at step 4 closes the Dekker race with the producer's
//! SeqCst fence in [`Synapse::try_send`]: either the producer sees our
//! bit and wakes us, or we see the new head on recheck and bail.

use super::state::{Shutdown, Synapse};
use std::sync::atomic::Ordering;

impl<T, const CAP: usize, const N: usize> Synapse<T, CAP, N> {
    /// Non-blocking claim for a consumer. Returns `None` if no work is
    /// currently available. Uses a CAS loop on `tail` so multiple
    /// consumers may race without duplicating a message.
    ///
    /// May be called from any of the `N` consumer threads.
    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        loop {
            let t = self.tail.load(Ordering::Relaxed);
            let slot = &self.slots[t & Self::MASK];
            // Acquire-load the slot's seq. The producer publishes
            // `seq = t + 1` with Release when it writes slot `t & MASK`
            // at logical round `t`, so observing that value means the
            // write is visible.
            let seq = slot.seq.load(Ordering::Acquire);
            let expected = t.wrapping_add(1);
            if seq == expected {
                // Slot is written and belongs to round `t`. Race with
                // peer consumers: whoever wins the CAS owns this slot.
                match self.tail.compare_exchange_weak(
                    t,
                    t.wrapping_add(1),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // Safety: we observed `seq == t + 1` (producer's
                        // Release) and won the CAS, so no other consumer
                        // will read this slot at this wrap.
                        let v = unsafe {
                            (*slot.cell.get()).assume_init_read()
                        };
                        // Release the slot for the producer's next wrap:
                        // producer at head = t + CAP will see seq matches
                        // and may write again. Release pairs with the
                        // producer's Acquire load in `try_send`.
                        slot.seq
                            .store(t.wrapping_add(CAP), Ordering::Release);
                        // Wake a possibly-parked producer. Idempotent.
                        self.not_full.release();
                        return Some(v);
                    }
                    Err(_) => {
                        std::hint::spin_loop();
                        continue;
                    }
                }
            } else if (seq.wrapping_sub(expected) as isize) < 0 {
                // seq < expected → slot not yet written for round t
                // (or this consumer's `t` is stale behind tail). Either
                // way: nothing to claim right now.
                return None;
            } else {
                // seq > expected → another consumer already advanced
                // past round t; our `t` is stale. Retry with fresh tail.
                std::hint::spin_loop();
                continue;
            }
        }
    }

    /// Blocking claim for consumer `i`. Parks on `signals[i]` until
    /// either work arrives or [`shutdown`](Self::shutdown) is signaled.
    ///
    /// Must only be called from the thread registered via
    /// [`bind_consumer(i)`](Self::bind_consumer).
    ///
    /// # Panics
    /// If `i >= N`.
    #[inline]
    pub fn recv(&self, i: usize) -> Result<T, Shutdown> {
        assert!(i < N, "consumer index {} >= N={}", i, N);
        let bit = 1u64 << i;
        loop {
            // Fast path: try to claim a slot.
            if let Some(v) = self.try_recv() {
                return Ok(v);
            }
            // Observed empty. Check for shutdown before committing to park.
            if self.shutdown.load(Ordering::Acquire) {
                return Err(Shutdown);
            }
            // Canonical park dance: lock → advertise idle → recheck → acquire.
            self.signals[i].lock();
            // Advertise idle BEFORE the recheck, with SeqCst. This pairs
            // with the producer's SeqCst fence + idle_mask load after its
            // head publish, forming Dekker closure: either the producer
            // sees our bit and wakes us, or we see the new head on
            // recheck and bail.
            self.idle_mask.fetch_or(bit, Ordering::SeqCst);
            // Recheck via the slot's own `seq` (not via head): head is
            // Relaxed-stored under Vyukov and carries no ordering edge
            // we could rely on here, but the producer Release-stores
            // `seq` before the SeqCst fence, so a consumer that sees
            // `seq >= t + 1` has seen the producer's publish. This is
            // also one atomic load lighter than the old `is_empty` path
            // (which loaded both head and tail).
            let t = self.tail.load(Ordering::Relaxed);
            let seq_here = self.slots[t & Self::MASK].seq.load(Ordering::Acquire);
            let has_work = (seq_here.wrapping_sub(t.wrapping_add(1)) as isize) >= 0;
            if has_work || self.shutdown.load(Ordering::Acquire) {
                // Work or shutdown appeared — clear our idle bit (producer
                // may have already cleared it, `fetch_and` is idempotent)
                // and retry.
                self.idle_mask.fetch_and(!bit, Ordering::Relaxed);
                self.signals[i].release();
                continue;
            }
            // Truly empty & not shutting down — park. Signal handles the
            // final Dekker window internally.
            self.signals[i].acquire();
            // On wake, acquire leaves the signal locked. Clear our idle
            // bit (producer likely cleared it during wake, but we clear
            // defensively in case this was a spurious wake or shutdown).
            self.idle_mask.fetch_and(!bit, Ordering::Relaxed);
            // Loop back to try.
        }
    }
}
