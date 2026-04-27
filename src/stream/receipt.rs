//! `Receipt` — delivery confirmation handle returned by `Stream::send`.
//!
//! Each `send` (or `send_iter`) returns a `Receipt` carrying the
//! monotonic sequence number of the (last) message. Holders can:
//!
//! - **Poll** with [`is_delivered`] — one Acquire load against the
//!   consumer's published cursor.
//! - **Block** with [`wait_delivered`] — busy-spin or park until the
//!   cursor passes the receipt's seq.
//!
//! `Receipt` is `Copy`. Share it freely across threads — every holder
//! checks the same shared cursor on the `Stream` it was issued from.

use crate::stream::Stream;
use crate::waiter::Waiter;

/// Sequence-number handle returned by `Stream::send` / `send_iter`.
///
/// Treat it as opaque outside of `is_delivered` / `wait_delivered` and
/// `seq()` for diagnostics. The internal value is the seq of the last
/// message included in the issuing call (so `send` returns the seq of
/// that one message; `send_iter` returns the seq of the final item).
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct Receipt(u64);

impl Receipt {
    /// Internal: build from a raw seq. `Stream` is the only producer.
    #[inline]
    pub(crate) fn new(seq: u64) -> Self { Receipt(seq) }

    /// Sequence number of the message this receipt confirms.
    ///
    /// For receipts returned by `send_iter`, this is the seq of the
    /// **last** item in the batch. The first item's seq is therefore
    /// `seq() - (batch_len - 1)`.
    #[inline]
    pub fn seq(&self) -> u64 { self.0 }

    /// Returns `true` if the consumer has drained past this receipt's
    /// message. Cost: one Acquire atomic load on the stream's cursor.
    ///
    /// "Past" means the consumer has already read the slot — the
    /// message has been delivered. This is the cheap, lock-free way
    /// to confirm delivery without blocking.
    #[inline]
    pub fn is_delivered<T, W: Waiter>(&self, stream: &Stream<T, W>) -> bool {
        stream.cursor() > self.0
    }

    /// Block the calling thread until the consumer has drained past
    /// this receipt's message.
    ///
    /// **Note**: the MVP implementation busy-spins on the cursor. Use
    /// `is_delivered` for polling, or call this only from threads where
    /// burning a core for a few microseconds is acceptable.
    pub fn wait_delivered<T, W: Waiter>(&self, stream: &Stream<T, W>) {
        stream.wait_for(self.0 + 1);
    }
}
