//! # stream
//!
//! **FIFO transports.** Continuous flow of payloads from one producer to
//! one consumer, with order preserved. Two flavors live here:
//!
//! - [`Ring<T, CAP>`] — **bounded** SPSC. Fixed-capacity ring; producer
//!   blocks (or returns `Err`) when full. Use when memory must stay
//!   constant and backpressure is desired.
//! - [`Stream<T>`] — **unbounded** SPSC sequenced log. Linked segments
//!   grow on demand; producer never blocks while RAM is available.
//!   Each `send` returns a [`Receipt`] for O(1) delivery verification.
//! - [`Duplex<A, B>`] — **bidirectional** pair of `Stream`s. Type-safe
//!   send / recv per direction.
//! - [`BufferedSender`] — wrapper that exposes a single-send API on top
//!   of `Stream::send_iter` for batched throughput.
//!
//! For **single-message** transports (no buffer, one in flight) see
//! [`crate::slot`]. For **multiplexed** transports (N→1, 1→N, M→N with
//! routing) see [`crate::route`].
//!
//! ## Concurrency contract (shared by `Ring` and `Stream`)
//!
//! - Exactly **one producer** thread calls `send` / `send_iter` / `try_send`.
//! - Exactly **one consumer** thread calls `recv` / `try_recv` / `recv_bulk`.
//! - Any thread may hold a [`Receipt`] and call `is_delivered` /
//!   `wait_delivered` (Stream-only).

mod buffered;
mod duplex;
mod receipt;
mod recv;
mod ring;
mod ring2;
mod segment;
mod send;
#[allow(clippy::module_inception)]
mod stream;

#[cfg(test)]
mod tests;

pub use buffered::BufferedSender;
pub use duplex::{Duplex, DuplexEnd};
pub use receipt::Receipt;
pub use ring::Ring;
pub use ring2::Ring2;
pub use stream::Stream;
