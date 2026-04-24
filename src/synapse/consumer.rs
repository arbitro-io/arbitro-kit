//! N-consumer dequeue path: [`Synapse::try_recv`] and [`Synapse::recv`].
//!
//! Any of the `N` consumer threads may call these concurrently. Claims
//! are serialized by a CAS loop on the shared `tail` cursor; once a
//! consumer wins the CAS for index `t`, it owns `slot[t & MASK]` for
//! that wrap and may read it safely.
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
            let h = self.head.load(Ordering::Acquire);
            if t == h {
                return None;
            }
            // CAS-claim the slot: win (Ok) → we own `slot[t & MASK]`.
            // Lose (Err) → another consumer took it; retry.
            match self.tail.compare_exchange_weak(
                t,
                t.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Safety: the Acquire load of `head` synchronized-with
                    // the producer's Release, so `slot[t & MASK]` holds an
                    // initialized T. We won the CAS so nobody else will
                    // read this slot at this index this wrap.
                    let v = unsafe {
                        (*self.slots[t & Self::MASK].get()).assume_init_read()
                    };
                    // Wake a possibly-parked producer. Idempotent.
                    self.not_full.release();
                    return Some(v);
                }
                Err(_) => {
                    std::hint::spin_loop();
                    continue;
                }
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
            if !self.is_empty() || self.shutdown.load(Ordering::Acquire) {
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
