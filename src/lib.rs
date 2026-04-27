//! # arbitro-kit
//!
//! Zero-dependency synchronization and transport primitives extracted from
//! the Arbitro broker. `std`-only, publishable standalone.
//!
//! ## Modules
//!
//! - [`gate`] — synchronization primitives (no payload): [`Signal`],
//!   [`SignalSet`], [`Park`].
//! - [`slot`] — single-message transports (one in flight, no buffer):
//!   [`Pipe`], [`Channel`].
//! - [`stream`] — FIFO transports: [`Ring`] (bounded), [`Stream`]
//!   (unbounded), [`Duplex`] (bidirectional pair), [`BufferedSender`].
//! - [`route`] — multiplexed transports (N→1, M→N): [`Hub`], [`Mpmc`].
//!
//! ## Quick start
//!
//! ```no_run
//! use arbitro_kit::gate::Signal;
//! use arbitro_kit::slot::Channel;
//!
//! let sig = Signal::new();
//! let (client, server) = Channel::<u64, u64>::spsc();
//! ```
//!
//! [`Signal`]: gate::Signal
//! [`SignalSet`]: gate::SignalSet
//! [`Park`]: gate::Park
//! [`Pipe`]: slot::Pipe
//! [`Channel`]: slot::Channel
//! [`Ring`]: stream::Ring
//! [`Stream`]: stream::Stream
//! [`Duplex`]: stream::Duplex
//! [`BufferedSender`]: stream::BufferedSender
//! [`Hub`]: route::Hub
//! [`Mpmc`]: route::Mpmc

#![deny(unsafe_op_in_unsafe_fn)]

pub mod gate;
pub mod route;
pub mod slot;
pub mod stream;
pub mod waiter;

pub use waiter::{AsyncWaiter, BlockingWaiter, ParkWaiter, Waiter};
#[cfg(feature = "tokio")]
pub use waiter::NotifyWaiter;
