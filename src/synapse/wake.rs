//! Consumer wake helpers and shutdown.
//!
//! ## `wake_consumers` — the hot wakeup path
//!
//! Called by the producer after each successful `try_send`. Reads
//! `idle_mask` with `SeqCst`; if any consumer is advertising idle, picks
//! one (rotated by `head` for fairness), atomically clears its bit with
//! `fetch_and(AcqRel)`, and calls `signals[i].release()` on that one
//! consumer only. This is **O(1) wakeup**, independent of N.
//!
//! Consumers not advertising idle are either (a) actively draining via
//! `try_recv` (no wakeup needed) or (b) mid-park-dance and about to
//! recheck `head` — the producer's prior Release on `head` makes the
//! new data visible, so those consumers bail out of the park without
//! help. Either way, no wakeup is lost.
//!
//! ## `wake_all_consumers` — shutdown only
//!
//! Broadcasts `release()` to every signal. Used only when we need to
//! wake everyone regardless of idle state (i.e. shutdown); we don't
//! care about thundering-herd at teardown.

use std::sync::atomic::Ordering;

use super::state::Synapse;

impl<T, const CAP: usize, const N: usize> Synapse<T, CAP, N> {
    // ── Shutdown API ─────────────────────────────────────────────────

    /// Signal every consumer to exit. Sets the internal shutdown flag
    /// and releases all `N` per-consumer signals. Parked consumers wake
    /// and their next [`recv`](Self::recv) returns `Err(Shutdown)`.
    ///
    /// Pending slots remain accessible via [`try_recv`](Self::try_recv)
    /// until the ring is drained — callers that want to drain first can
    /// loop on `try_recv` until it returns `None` after shutdown.
    ///
    /// Safe to call from any thread, any number of times.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.wake_all_consumers();
        // Also wake a possibly-parked producer.
        self.not_full.release();
    }

    /// `true` iff [`shutdown`](Self::shutdown) has been called.
    #[inline]
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    // ── Internal wake helpers ────────────────────────────────────────

    /// Targeted O(1) consumer wakeup.
    ///
    /// Reads `idle_mask` once. If any consumer is advertising idle, we
    /// rotate the mask by the current `head` position (so consecutive
    /// sends pick different idle consumers — avoids starving high-
    /// indexed consumers), atomically clear the selected bit, and
    /// release only that signal.
    ///
    /// Uses `SeqCst` on the load to pair with the consumer's `SeqCst`
    /// bit-set before its final recheck (Dekker closure).
    #[inline(always)]
    pub(super) fn wake_consumers(&self) {
        let mask = self.idle_mask.load(Ordering::SeqCst);
        if mask == 0 { return; }

        // Rotate the mask by the current head position so consecutive
        // sends pick different idle consumers. Without this rotation,
        // `trailing_zeros` would always favour consumer 0 and starve
        // higher-indexed ones. `head` is already in the producer's
        // cache line (we just wrote it), so the load is effectively free.
        let rot = (self.head.load(Ordering::Relaxed) as u32) % (N as u32);
        let rotated = mask.rotate_right(rot);
        let i_rot = rotated.trailing_zeros();
        let i = ((i_rot + rot) as usize) % N;
        let bit = 1u64 << i;

        let prev = self.idle_mask.fetch_and(!bit, Ordering::AcqRel);
        if prev & bit != 0 {
            self.signals[i].release();
        } else {
            // Lost the race with another clearer (shutdown, or the
            // consumer itself bailing after its recheck). Fall through
            // to a plain scan — rare under normal load.
            let mask2 = self.idle_mask.load(Ordering::SeqCst);
            if mask2 == 0 { return; }
            let j = mask2.trailing_zeros() as usize;
            let bit_j = 1u64 << j;
            let prev2 = self.idle_mask.fetch_and(!bit_j, Ordering::AcqRel);
            if prev2 & bit_j != 0 {
                self.signals[j].release();
            }
        }
    }

    /// Broadcast release to every consumer. Used only on shutdown —
    /// we need to wake everyone regardless of idle state.
    #[inline(always)]
    pub(super) fn wake_all_consumers(&self) {
        for s in &self.signals {
            s.release();
        }
    }
}
