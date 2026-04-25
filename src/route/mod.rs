//! # route
//!
//! **Multiplexed transports.** N→1, 1→N, or M→N with explicit routing
//! between producers and consumers. Built on top of `gate` primitives.
//!
//! ## Types
//!
//! - [`Hub<In, Out>`] — N:1 multiplexer with **named ports**. Each port
//!   is its own slot + reply pipe. The drain side wakes via a single
//!   [`SignalSet`](crate::gate::SignalSet) bitmap, one atomic OR per
//!   send regardless of N. Shutdown bit built in.
//! - [`Mpmc<T>`] — M:N **anonymous** sharded channel. Per-(producer, shard)
//!   SPSC mini-rings; consumers steal across shards. Built-in shutdown
//!   and panic-safe Drop.
//!
//! ## When to reach for `route`
//!
//! - **Multiple producers feeding one drain**: `Hub` if you want named
//!   ports and per-port replies; `Mpmc` if you want anonymous fan-in
//!   with higher throughput.
//! - **Multiple consumers behind one producer** (work-stealing): not
//!   shipped yet. `Synapse` lives here when added.
//! - **Single-producer, single-consumer**: don't use `route` — pick from
//!   [`crate::slot`] (single-message) or [`crate::stream`] (FIFO).

mod hub;
mod mpmc;

pub use hub::{Hub, HubDrain, HubPort, HubReply, HubShutdown, Shutdown, MAX_HUB_PORTS};
pub use mpmc::{Mpmc, MpmcConsumer, MpmcProducer, MpmcShutdown, MAX_MPMC_PRODUCERS};
