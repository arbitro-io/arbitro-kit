//! # waiter
//!
//! **Pluggable wait/wake backend** for every kit primitive.
//!
//! The same `Pipe<T, W>`, `Channel<Req, Resp, W>`, `Ring<T, CAP, W>`, etc.
//! works on:
//! - **OS threads** (default `W = ParkWaiter`) — `thread::park`/`unpark`.
//! - **Tokio tasks** (`W = NotifyWaiter`, feature `tokio`) — `tokio::sync::Notify`.
//! - **Future runtimes** (e.g. io_uring) — write one new `Waiter` impl and
//!   every primitive in the crate inherits the new runtime automatically.
//!
//! ## Why three traits, not one
//!
//! Rust does not let one method be both `fn` and `async fn`. So we split:
//! - [`Waiter`] — common surface (registration, wake).
//! - [`BlockingWaiter`] — adds a sync `wait_until(predicate)` that blocks
//!   the calling thread.
//! - [`AsyncWaiter`] — adds an async `wait_until(predicate)` returning a
//!   future the caller awaits.
//!
//! A primitive's `recv` is either sync (when its `W: BlockingWaiter`) or
//! async (when its `W: AsyncWaiter`). Same struct, different bound, different
//! method exposed. After monomorphization, zero overhead vs the hand-written
//! sync/async equivalents.
//!
//! ## Adding io_uring
//!
//! Implement [`Waiter`] + [`AsyncWaiter`] over an io_uring CQE poll. Done.
//! Every primitive in the crate (Pipe, OneShot, Channel, Ring, Mpmc, Hub)
//! works with `W = UringWaiter` automatically. No primitive code rewrite.

use std::future::Future;

#[cfg(feature = "tokio")]
mod notify;
mod park;
mod park2;

#[cfg(feature = "tokio")]
pub use notify::NotifyWaiter;
pub use park::ParkWaiter;
pub use park2::ParkWaiter2;

/// Common surface every backend implements.
///
/// Implementors must be `Default + Send + Sync` so primitives can hold one
/// without runtime configuration. `Default::default()` produces a fresh,
/// unregistered waiter.
///
/// Two operations:
/// - [`set_worker`](Self::set_worker) — register the consumer (mandatory
///   for [`ParkWaiter`], no-op for runtime-multiplexed waiters like
///   [`NotifyWaiter`] or a future io_uring impl).
/// - [`wake`](Self::wake) — fire the wake signal. Producers call this
///   after publishing the state the consumer is waiting on.
pub trait Waiter: Default + Send + Sync {
    /// Register the thread that will block in `wait_until`. For sync
    /// (`ParkWaiter`) this MUST be called before any `wait_until`. For
    /// async waiters this is a no-op — the runtime tracks tasks itself.
    ///
    /// Callers should always pass `std::thread::current()`; async impls
    /// ignore it.
    fn set_worker(&self, _thread: std::thread::Thread) {}

    /// `true` if a worker has been registered (or if registration is
    /// not required for this backend).
    fn has_worker(&self) -> bool {
        true
    }

    /// Wake the waiting consumer (or arm a wake-on-arrival flag).
    /// Lock-free, idempotent, callable from any thread.
    fn wake(&self);
}

/// Sync extension: `wait_until` blocks the calling thread.
///
/// Implemented by [`ParkWaiter`].
pub trait BlockingWaiter: Waiter {
    /// Block the calling thread until `ready()` returns `true`. The
    /// predicate is evaluated on entry, during spin, after the Dekker
    /// barrier, and after every park wake.
    fn wait_until<F: FnMut() -> bool>(&self, ready: F);
}

/// Async extension: `wait_until` returns a future.
///
/// Implemented by [`NotifyWaiter`] (feature `tokio`) and any future
/// runtime-aware waiter (e.g. io_uring).
///
/// The trait uses RPITIT (return-position `impl Trait` in trait, stable
/// since Rust 1.75) so no GATs are required in the trait declaration.
pub trait AsyncWaiter: Waiter {
    /// Build a future that resolves when `ready()` returns `true`. The
    /// future is `Send` so it can be polled from any tokio worker.
    ///
    /// The predicate may borrow from `&self` or any longer-lived scope —
    /// the explicit `'a` ties the predicate, the future, and the
    /// receiver lifetime together so callers can write naturally:
    ///
    /// ```ignore
    /// self.waiter.wait_until(|| self.has_data.load(Ordering::Acquire)).await;
    /// ```
    fn wait_until<'a, F>(&'a self, ready: F) -> impl Future<Output = ()> + Send + 'a
    where
        F: FnMut() -> bool + Send + 'a;
}
