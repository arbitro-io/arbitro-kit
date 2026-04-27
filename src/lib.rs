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
//! - [`gate`] — coalesced multi-channel signal: [`SignalSet`], plus
//!   single-use [`OneSignal`] and the lifeline cancellation helper.
//! - [`slot`] — single-message transports: [`Pipe`], [`Channel`].
//! - [`stream`] — FIFO transports: [`Ring`] (bounded), [`Stream`]
//!   (unbounded), [`Duplex`] (bidirectional pair), [`BufferedSender`].
//! - [`route`] — multiplexed transports (N→1, M→N): [`Hub`], [`Mpmc`],
//!   [`Mpsc`], [`OneShot`].
//!
//! ## Quick start
//!
//! ```no_run
//! use arbitro_kit::slot::Channel;
//!
//! let (client, server) = Channel::<u64, u64>::spsc();
//! ```
//!
//! Every transport is generic over a [`Waiter`] backend. Default is
//! sync OS thread; opt into tokio with `feature = "tokio"` and the
//! `*Async` type aliases (e.g. [`PipeAsync`](slot::PipeAsync),
//! [`OneShotAsync`](route::OneShotAsync)).
//!
//! [`SignalSet`]: gate::SignalSet
//! [`OneSignal`]: gate::OneSignal
//! [`Pipe`]: slot::Pipe
//! [`Channel`]: slot::Channel
//! [`Ring`]: stream::Ring
//! [`Stream`]: stream::Stream
//! [`Duplex`]: stream::Duplex
//! [`BufferedSender`]: stream::BufferedSender
//! [`Hub`]: route::Hub
//! [`Mpmc`]: route::Mpmc
//! [`Mpsc`]: route::Mpsc
//! [`OneShot`]: route::OneShot

#![deny(unsafe_op_in_unsafe_fn)]

pub mod gate;
pub mod route;
pub mod slot;
pub mod stream;
pub mod waiter;

pub use waiter::{AsyncWaiter, BlockingWaiter, ParkWaiter, Waiter};
#[cfg(feature = "tokio")]
pub use waiter::NotifyWaiter;
