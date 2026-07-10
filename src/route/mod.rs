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
//! - [`Mpsc<T, CAP, W>`] — M:1 fan-in on per-producer SPSC rings + Vyukov
//!   wake gate + amortized consumer drain.
//! - [`OneShot<T, W>`] — single-use 1:1 reply slot, sender consumes self
//!   on send, receiver consumes self on recv.

mod hub;
mod mpmc;
mod mpsc;
mod oneshot;

pub use hub::{Hub, HubDrain, HubPort, HubReply, HubShutdown, Shutdown, MAX_HUB_PORTS};
pub use mpmc::{Mpmc, MpmcConsumer, MpmcProducer, MpmcShutdown, MAX_MPMC_PRODUCERS};
pub use mpsc::{
    Mpsc, MpscConsumer, MpscProducer, MpscProducerLease, MpscProducerPool, MpscShutdown,
    MAX_MPSC_PRODUCERS,
};
pub use oneshot::{
    Closed as OneShotClosed, OneShot, Receiver as OneShotReceiver, Sender as OneShotSender,
};

#[cfg(feature = "tokio")]
use crate::waiter::NotifyWaiter;

#[cfg(feature = "tokio")]
pub type OneShotAsync<T> = OneShot<T, NotifyWaiter>;
#[cfg(feature = "tokio")]
pub type OneShotAsyncSender<T> = OneShotSender<T, NotifyWaiter>;
#[cfg(feature = "tokio")]
pub type OneShotAsyncReceiver<T> = OneShotReceiver<T, NotifyWaiter>;
#[cfg(feature = "tokio")]
pub use oneshot::Closed as OneShotAsyncClosed;

#[cfg(feature = "tokio")]
pub type HubAsync<In, Out> = Hub<In, Out, NotifyWaiter>;

#[cfg(feature = "tokio")]
pub type MpmcAsync<T, const CAP: usize = 64> = Mpmc<T, CAP, NotifyWaiter>;

#[cfg(feature = "tokio")]
pub type MpscAsync<T, const CAP: usize = 64> = Mpsc<T, CAP, NotifyWaiter>;
#[cfg(feature = "tokio")]
pub type MpscAsyncProducer<T, const CAP: usize = 64> = MpscProducer<T, CAP, NotifyWaiter>;
#[cfg(feature = "tokio")]
pub type MpscAsyncConsumer<T, const CAP: usize = 64> = MpscConsumer<T, CAP, NotifyWaiter>;
#[cfg(feature = "tokio")]
pub type MpscAsyncShutdown = MpscShutdown<NotifyWaiter>;
