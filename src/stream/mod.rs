//! # stream
//!
//! `Stream<T>` — unbounded sequenced log primitive.
//!
//! ## What
//!
//! An append-only SPSC log. The producer `send`s items and gets a
//! [`Receipt`] carrying a monotonic sequence number; the consumer
//! drains in order. Storage is a linked list of fixed-size segments
//! allocated on demand — **the producer never blocks** while RAM is
//! available. The consumer parks via `Park` (phased backoff, ~0%
//! idle CPU) when the stream is empty.
//!
//! ## Why
//!
//! Higher-level patterns (request/response, broadcast, work-stealing)
//! all want the same thing under the hood: identity (a seq), ordering,
//! and an O(1) "did it arrive?" check. `Stream<T>` is exactly that and
//! nothing more. RPC, correlation, routing, fan-out — those compose on
//! top in caller code.
//!
//! ## Layout
//!
//! ```text
//!                 producer ─send─►        consumer ─recv─►
//!                         │                       │
//!                ┌────────▼─────────┬─────────────▼────────┐
//!                │  tail_seg        │  head_seg            │
//!                │  (writing here)  │  (reading here)      │
//!                ├──────┬──────┬────┴──┬──────┬──────┬─────┤
//!                │ seg0 │ seg1 │  …    │ segM │ segN │ ... │
//!                └──────┴──────┴───────┴──────┴──────┴─────┘
//!                                              ▲
//!                                       freed once head
//!                                       cursor passes them
//! ```
//!
//! ## Topology and patterns
//!
//! - **Fire-and-forget**: `stream.send(v)`, ignore the receipt.
//! - **Verified send**: `let r = stream.send(v); ... r.is_delivered()`.
//! - **Bidirectional**: pair two streams (one per direction). Caller
//!   correlates replies if needed.
//! - **Bulk transport**: `send_iter` + `recv_bulk` to amortize the
//!   cursor publish over a batch.
//!
//! ## Concurrency contract
//!
//! - Exactly **one producer** thread calls `send` / `send_iter`.
//! - Exactly **one consumer** thread calls `recv` / `try_recv` /
//!   `recv_bulk`. Register it via [`Stream::set_consumer`] before
//!   the first blocking recv.
//! - Any thread may hold a [`Receipt`] and call `is_delivered` /
//!   `wait_delivered`.

mod buffered;
mod duplex;
mod receipt;
mod recv;
mod segment;
mod send;
#[allow(clippy::module_inception)]
mod stream;

#[cfg(test)]
mod tests;

pub use buffered::BufferedSender;
pub use duplex::{Duplex, DuplexEnd};
pub use receipt::Receipt;
pub use stream::Stream;
