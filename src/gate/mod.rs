//! # gate
//!
//! Signal primitives for low-latency producer→consumer coordination.
//!
//! ## Types
//!
//! - [`Signal`] — single-channel M:1 signal. N producers can `release()`
//!   concurrently and lock-free; exactly one consumer `acquire()`s and parks.
//! - [`SignalSet`] — multi-channel M:1 signal. Up to 64 named signals backed
//!   by a single `AtomicU64` bitmap. Release one, wait for any / all / subset.
//! - [`SignalId`] — typed handle returned by `SignalSet::create()` for O(1)
//!   hot-path ops (no string lookup).
//! - [`Pipe`] — SPSC single-slot transport built on one [`Signal`]. The
//!   minimal atom between a raw `Signal` and higher-level composites. Carries
//!   an optional zero-cost observer [`PipeHook`].
//! - [`Channel`] — SPSC request/response channel built on two [`Signal`]s.
//!   Zero-copy ownership transfer; see [`Client`] / [`Server`] for the typed
//!   split API.
//!
//! ## Semantics (shared by `Signal` and `SignalSet`)
//!
//! A signal is "open" when it has pending work. `release()` opens it,
//! `lock()` closes it, `acquire*()` blocks until open.
//!
//! Both primitives are **M producers : 1 consumer**. Multiple producers may
//! call `release` / `lock` / `is_open` concurrently from any thread; only
//! one consumer thread may call `acquire*` (enforced by a single parked
//! `Thread` handle registered via `set_worker`).

mod channel;
mod gate;
mod gate_set;
mod hub;
mod pipe;
mod ring;

pub use channel::{Channel, Client, Server};
pub use gate::{Signal, DEFAULT_SPIN_ITERS};
pub use gate_set::{SignalId, SignalSet, MAX_GATES};
pub use hub::{Hub, HubDrain, HubPort, HubReply, HubShutdown, Shutdown, MAX_HUB_PORTS};
pub use pipe::{NoHook, Pipe, PipeHook};
pub use ring::Ring;
