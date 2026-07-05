//! # gate
//!
//! **Synchronization primitives.** No payload, no buffer — just the
//! mechanisms by which producers tell consumers "go" and consumers tell
//! the OS "park me".
//!
//! ## Types
//!
//! - [`SignalSet`] — multi-channel M:1 signal. Up to 64 named signals backed
//!   by a single `AtomicU64` bitmap. Release one, wait for any / all / subset.
//! - [`SignalId`] — typed handle returned by `SignalSet::create()` for O(1)
//!   hot-path ops (no string lookup).
//! - [`OneSignal`] — single-use payloadless gate with timeout support.
//!   The minimal "block until released" primitive; replaces
//!   `tokio::sync::oneshot` when the payload travels separately.
//!
//! ## Semantics
//!
//! A signal is "open" when it has pending work. `release()` opens it,
//! `lock()` closes it, `acquire*()` blocks until open.

mod lifeline;
mod one_signal;
mod signal_set;

pub use lifeline::{Cancelled, Lifeline, WaiterId, MAX_WAITERS};
pub use one_signal::{
    AcquireError as OneSignalError, OneSignal, Receiver as OneSignalReceiver,
    Sender as OneSignalSender,
};
pub use signal_set::{SignalId, SignalSet, MAX_GATES};
