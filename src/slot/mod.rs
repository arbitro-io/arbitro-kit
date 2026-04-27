//! # slot
//!
//! **One-message-in-flight transports.** No FIFO buffering — at any given
//! moment a slot holds either nothing or exactly one payload. Sending into
//! a full slot blocks (or returns `Err`, depending on the API).
//!
//! ## Types
//!
//! - [`Pipe<T, H>`] — SPSC single-slot transport built on one [`Signal`]
//!   from `gate`. The minimal atom between a raw `Signal` and higher-level
//!   composites. Optional zero-cost observer hook via the `H` generic.
//! - [`Channel<Req, Resp>`] — SPSC request/response, paired single-slot.
//!   Caller blocks on `call`; server responds via `serve_one`/`serve_loop`.
//!   Zero-copy ownership transfer.
//!
//! ## When to reach for a slot vs a stream
//!
//! Use a slot when **the message is the work**: each one is processed
//! immediately and the next one waits. Use a [`stream`](crate::stream)
//! when items pipeline through a buffer.
//!
//! [`Signal`]: crate::gate::Signal

mod channel;
mod pipe;
#[cfg(feature = "tokio")]
mod pipe_async;

pub use channel::{Channel, Client, Server};
pub use pipe::{NoHook, Pipe, PipeHook};
#[cfg(feature = "tokio")]
pub use pipe_async::PipeAsync;
