//! `BufferedSender<T>` — accumulator wrapper for `Stream<T>`.
//!
//! Use case: the caller produces items one at a time (a handler, a
//! parser callback, an event loop) and wants the throughput of
//! [`Stream::send_iter`] without restructuring the call sites.
//!
//! ```text
//!   caller          BufferedSender         Stream
//!     │  send(v)        │                     │
//!     │ ──────────────► │  push to local buf  │
//!     │  send(v)        │                     │
//!     │ ──────────────► │  push to local buf  │
//!     │ ...             │                     │
//!     │  send(v) [K-th] │                     │
//!     │ ──────────────► │ ─send_iter(buf)──►  │  ← single cursor publish
//!     │                 │  buf cleared        │
//! ```
//!
//! Bench (`benches/rpc_patterns.rs` sections B vs D): per-item `send`
//! through this wrapper closes the gap with explicit bulk send to
//! within ~10–15 % at K=128. The cost of the wrapper is one Vec push
//! plus a length comparison per `send`.
//!
//! ## RAII safety
//!
//! `Drop` flushes any residual items. If the wrapper goes out of
//! scope with K-1 items buffered (none have hit the threshold), they
//! are still delivered — never silently lost.

use std::sync::Arc;

use super::receipt::Receipt;
use super::stream::Stream;

/// Wraps a `Stream<T>` to provide transparent batching for callers
/// that produce items one at a time. See module-level docs.
pub struct BufferedSender<T> {
    stream: Arc<Stream<T>>,
    buf: Vec<T>,
    threshold: usize,
    /// Last receipt produced by an automatic or manual flush. Held
    /// so the caller can verify delivery of the most recent batch
    /// without having to capture flush()'s return value at every site.
    last_receipt: Option<Receipt>,
}

impl<T> BufferedSender<T> {
    /// Construct a new sender wrapping `stream`. Items push into a
    /// local `Vec`; once the Vec reaches `threshold` items, an
    /// automatic flush via [`Stream::send_iter`] drains them all in
    /// a single cursor publish.
    ///
    /// # Panics
    /// Panics if `threshold` is 0.
    pub fn new(stream: Arc<Stream<T>>, threshold: usize) -> Self {
        assert!(threshold > 0, "BufferedSender threshold must be > 0");
        Self {
            stream,
            buf: Vec::with_capacity(threshold),
            threshold,
            last_receipt: None,
        }
    }

    /// Append an item to the local buffer. Triggers an automatic
    /// flush once `buf.len() == threshold`.
    ///
    /// To recover the receipt of an auto-flushed batch, call
    /// [`Self::last_receipt`] after the auto-flush boundary.
    #[inline]
    pub fn send(&mut self, value: T) {
        self.buf.push(value);
        if self.buf.len() >= self.threshold {
            self.flush();
        }
    }

    /// Force-flush all buffered items via [`Stream::send_iter`].
    /// Returns the receipt for the **last** item flushed, or the
    /// previous `last_receipt` if the buffer was empty.
    pub fn flush(&mut self) -> Option<Receipt> {
        if self.buf.is_empty() {
            return self.last_receipt;
        }
        let r = self.stream.send_iter(self.buf.drain(..));
        if r.is_some() {
            self.last_receipt = r;
        }
        r
    }

    /// Number of items currently buffered locally, not yet sent
    /// through the underlying stream.
    #[inline]
    pub fn pending(&self) -> usize { self.buf.len() }

    /// Threshold at which auto-flush triggers.
    #[inline]
    pub fn threshold(&self) -> usize { self.threshold }

    /// Last receipt returned by a flush. Useful for verifying
    /// delivery of the most recent batch via `Receipt::is_delivered`
    /// or `Receipt::wait_delivered`.
    #[inline]
    pub fn last_receipt(&self) -> Option<Receipt> { self.last_receipt }

    /// Borrow the underlying stream — for cursor inspection,
    /// `wait_for(seq)`, etc.
    #[inline]
    pub fn stream(&self) -> &Stream<T> { &self.stream }
}

impl<T> Drop for BufferedSender<T> {
    /// RAII safety: flush any residual items so they aren't silently
    /// dropped when the sender goes out of scope.
    fn drop(&mut self) { self.flush(); }
}

// ─── Stream::buffered() helper ────────────────────────────────────────────

impl<T> Stream<T> {
    /// Build a [`BufferedSender`] that wraps this stream and
    /// auto-flushes every `threshold` items. The wrapper clones the
    /// `Arc<Stream<T>>` it's given.
    ///
    /// ```ignore
    /// let stream = Arc::new(Stream::<u64>::new());
    /// let mut tx = stream.buffered(64);
    /// for i in 0..1000 { tx.send(i); }
    /// // tx is dropped here → final flush.
    /// ```
    pub fn buffered(self: &Arc<Self>, threshold: usize) -> BufferedSender<T> {
        BufferedSender::new(self.clone(), threshold)
    }
}
