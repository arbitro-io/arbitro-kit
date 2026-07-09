//! # route
//!
//! **Multiplexed transports.** N→1, 1→N, or M→N with explicit routing
//! between producers and consumers.
//!
//! ## Types
//!
//! - [`Hub<In, Out, W>`] — N:1 multiplexer with **named ports**. Each port
//!   is its own slot + reply pipe. The drain side wakes via a single
//!   [`SignalSet`](crate::gate::SignalSet) bitmap, one atomic OR per
//!   send regardless of N. Shutdown bit built in.
//! - [`Mpmc<T, CAP, W>`] — M:N **anonymous** sharded channel. Per-(producer,
//!   shard) SPSC mini-rings; consumers steal across shards. Built-in
//!   shutdown and panic-safe Drop.
//! - [`Mpsc<T>`] — M:1 fan-in. Per-producer SPSC mini-ring + consumer-side
//!   scan for wakeup.
//! - [`OneShot<T, W>`] — single-use 1:1 reply slot, sender consumes self
//!   on send, receiver consumes self on recv.
//!
//! ## Async aliases (feature `tokio`)
//!
//! - [`OneShotAsync<T>`] = `OneShot<T, NotifyWaiter>`
//! - [`HubAsync<In, Out>`] = `Hub<In, Out, NotifyWaiter>`
//! - [`MpmcAsync<T, CAP>`] = `Mpmc<T, CAP, NotifyWaiter>`
//!
//! ## When to reach for `route`
//!
//! - **Multiple producers feeding one drain**: `Hub` for named ports +
//!   per-port replies; `Mpsc` for anonymous M:1 fan-in; `Mpmc` if the
//!   consumer count may ever grow beyond 1.
//! - **Single-producer, single-consumer**: don't use `route` — pick from
//!   [`crate::slot`] (single-message) or [`crate::stream`] (FIFO).

mod hub;
mod mpmc;
mod mpsc;
mod oneshot;

pub use hub::{Hub, HubDrain, HubPort, HubReply, HubShutdown, Shutdown, MAX_HUB_PORTS};
pub use mpmc::{Mpmc, MpmcConsumer, MpmcProducer, MpmcShutdown, MAX_MPMC_PRODUCERS};
pub use mpsc::{Mpsc, MpscConsumer, MpscProducer, MpscSender, MpscShutdown, MAX_MPSC_PRODUCERS};
pub use oneshot::{
    Closed as OneShotClosed, OneShot, Receiver as OneShotReceiver, Sender as OneShotSender,
};

#[cfg(feature = "tokio")]
use crate::waiter::NotifyWaiter;

/// Async sibling of [`OneShot<T>`]: `OneShot<T, NotifyWaiter>`. Receiver-side
/// `recv_async` requires a tokio runtime; sender side does not.
#[cfg(feature = "tokio")]
pub type OneShotAsync<T> = OneShot<T, NotifyWaiter>;

/// Async sibling of [`OneShotSender<T>`]: tokio-flavoured sender.
#[cfg(feature = "tokio")]
pub type OneShotAsyncSender<T> = OneShotSender<T, NotifyWaiter>;

/// Async sibling of [`OneShotReceiver<T>`]: tokio-flavoured receiver.
#[cfg(feature = "tokio")]
pub type OneShotAsyncReceiver<T> = OneShotReceiver<T, NotifyWaiter>;

/// Re-export for the legacy name. Same `Closed` ZST.
#[cfg(feature = "tokio")]
pub use oneshot::Closed as OneShotAsyncClosed;

/// Async sibling of [`Hub<In, Out>`]: `Hub<In, Out, NotifyWaiter>`.
#[cfg(feature = "tokio")]
pub type HubAsync<In, Out> = Hub<In, Out, NotifyWaiter>;

/// Async sibling of [`Mpmc<T, CAP>`]: `Mpmc<T, CAP, NotifyWaiter>`.
#[cfg(feature = "tokio")]
pub type MpmcAsync<T, const CAP: usize = 64> = Mpmc<T, CAP, NotifyWaiter>;

/// Async sibling of [`Mpsc<T, RING_CAP>`]: `Mpsc<T, RING_CAP, NotifyWaiter>`.
#[cfg(feature = "tokio")]
pub type MpscAsync<T, const RING_CAP: usize = 64> = Mpsc<T, RING_CAP, NotifyWaiter>;

/// Async sibling of [`MpscProducer<T, RING_CAP>`].
#[cfg(feature = "tokio")]
pub type MpscAsyncProducer<T, const RING_CAP: usize = 64> = MpscProducer<T, RING_CAP, NotifyWaiter>;

/// Async sibling of [`MpscConsumer<T, RING_CAP>`].
#[cfg(feature = "tokio")]
pub type MpscAsyncConsumer<T, const RING_CAP: usize = 64> = MpscConsumer<T, RING_CAP, NotifyWaiter>;

/// Async sibling of [`MpscShutdown<T, RING_CAP>`].
#[cfg(feature = "tokio")]
pub type MpscAsyncShutdown<T, const RING_CAP: usize = 64> = MpscShutdown<T, RING_CAP, NotifyWaiter>;

