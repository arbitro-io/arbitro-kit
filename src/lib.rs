//! # arbitro-kit
//!
//! Zero-dependency synchronization and transport primitives extracted from
//! the Arbitro broker. `std`-only, publishable standalone.
//!
//! ## Modules
//!
//! - [`waiter`] — the unified wait/wake contract:
//!   [`Waiter`] / [`BlockingWaiter`] / [`AsyncWaiter`] +
//!   [`ParkWaiter`] (sync OS thread) and [`NotifyWaiter`] (async, tokio).
//! - [`gate`] — coalesced multi-channel signal: [`SignalSet`], a bitmap of
//!   up to 64 binary signals collapsing onto one consumer.
//! - [`stream`] — FIFO transport: [`Ring`], a bounded SPSC queue with
//!   split `!Clone`/`!Sync` handles, so the single-producer/single-consumer
//!   contract is enforced at compile time.
//! - [`route`] — multiplexed transports: [`Mpsc`] (M→1 fan-in),
//!   [`Mpmc`] (M→N anonymous), [`OneShot`] (one value, once).
//!
//! ## Quick start
//!
//! ```no_run
//! use arbitro_kit::stream::Ring;
//!
//! // Bounded SPSC. CAP must be a power of two.
//! let (mut tx, mut rx) = Ring::<u64, 1024>::new();
//! tx.try_send(7).unwrap();
//! assert_eq!(rx.try_recv().unwrap(), 7);
//! ```
//!
//! Every transport is generic over a [`Waiter`] backend. Default is
//! sync OS thread; opt into tokio with `feature = "tokio"` and the
//! `*Async` type aliases (e.g. [`MpscAsync`](route::MpscAsync),
//! [`OneShotAsync`](route::OneShotAsync)).
//!
//! [`SignalSet`]: gate::SignalSet
//! [`Ring`]: stream::Ring
//! [`Mpmc`]: route::Mpmc
//! [`Mpsc`]: route::Mpsc
//! [`OneShot`]: route::OneShot

#![deny(unsafe_op_in_unsafe_fn)]
// This crate's constructors are split-handle factories: `new()` returns a
// `(Producer, Consumer, Shutdown)` tuple, not `Self`. Both lints fire on that
// intentional, public API shape across every transport.
#![allow(clippy::new_ret_no_self, clippy::type_complexity)]

pub mod gate;
pub mod route;
pub mod stream;
pub mod waiter;

#[cfg(feature = "tokio")]
pub use waiter::NotifyWaiter;
pub use waiter::{AsyncWaiter, BlockingWaiter, ParkWaiter, Waiter};
