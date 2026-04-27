//! # slot
//!
//! **One-message-in-flight transports.** No FIFO buffering — at any given
//! moment a slot holds either nothing or exactly one payload. Sending into
//! a full slot blocks (or returns `Err`, depending on the API).
//!
//! ## Types
//!
//! - [`Pipe<T, H, W>`] — SPSC single-slot transport built on a single
//!   [`Waiter`](crate::waiter::Waiter). Optional zero-cost observer hook
//!   via the `H` generic. The `W` parameter picks the wait/wake backend.
//! - [`Channel<Req, Resp, W>`] — SPSC request/response, paired single-slot.
//!   Caller blocks on `call`; server responds via `serve_one`/`serve_loop`.
//!   Zero-copy ownership transfer.
//!
//! ## Async aliases (feature `tokio`)
//!
//! - [`PipeAsync<T>`] = `Pipe<T, NoHook, NotifyWaiter>`
//! - [`ChannelAsync<Req, Resp>`] = `Channel<Req, Resp, NotifyWaiter>`
//!
//! ## When to reach for a slot vs a stream
//!
//! Use a slot when **the message is the work**: each one is processed
//! immediately and the next one waits. Use a [`stream`](crate::stream)
//! when items pipeline through a buffer.

mod channel;
mod pipe;

pub use channel::{Channel, Client, Server};
pub use pipe::{NoHook, Pipe, PipeHook};

#[cfg(feature = "tokio")]
use crate::waiter::NotifyWaiter;

/// Async sibling of [`Pipe<T>`]: `Pipe<T, NoHook, NotifyWaiter>`.
///
/// Use when the wake fires from a non-tokio thread (TCP reader, FFI
/// callback, OS-thread worker) and the waiter is a tokio task. Receive
/// via [`Pipe::recv_async`].
#[cfg(feature = "tokio")]
pub type PipeAsync<T> = Pipe<T, NoHook, NotifyWaiter>;

/// Async sibling of [`Channel<Req, Resp>`]: `Channel<Req, Resp, NotifyWaiter>`.
///
/// Use [`Channel::call_async`] / [`Channel::serve_one_async`] from inside
/// a tokio runtime. The matching split handles are
/// [`ClientAsync`] / [`ServerAsync`].
#[cfg(feature = "tokio")]
pub type ChannelAsync<Req, Resp> = Channel<Req, Resp, NotifyWaiter>;

/// Async sibling of [`Client`]: `Client<Req, Resp, NotifyWaiter>`.
#[cfg(feature = "tokio")]
pub type ClientAsync<Req, Resp> = Client<Req, Resp, NotifyWaiter>;

/// Async sibling of [`Server`]: `Server<Req, Resp, NotifyWaiter>`.
#[cfg(feature = "tokio")]
pub type ServerAsync<Req, Resp> = Server<Req, Resp, NotifyWaiter>;
