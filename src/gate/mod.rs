//! # gate
//!
//! **Synchronization primitives.** No payload, no buffer — just the
//! mechanisms by which producers tell consumers "go" and consumers tell
//! the OS "park me".
//!
//! ## Types
//!
//! - [`Signal`] — single-channel M:1 signal. N producers can `release()`
//!   concurrently and lock-free; exactly one consumer `acquire()`s and parks.
//! - [`SignalSet`] — multi-channel M:1 signal. Up to 64 named signals backed
//!   by a single `AtomicU64` bitmap. Release one, wait for any / all / subset.
//! - [`SignalId`] — typed handle returned by `SignalSet::create()` for O(1)
//!   hot-path ops (no string lookup).
//! - [`Park`] — stateless park/unpark. Lower-level than `Signal`: callers
//!   carry their own readiness predicate, `Park` only handles the wake.
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

mod park;
mod signal;
mod signal_set;

pub use park::Park;
pub use signal::{BitView, BoolView, OwnedBool, Signal, SignalSource, DEFAULT_SPIN_ITERS};
pub use signal_set::{SignalId, SignalSet, MAX_GATES};
