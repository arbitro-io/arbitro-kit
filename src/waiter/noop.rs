//! `NoopWaiter` — zero-cost `Waiter` for internal composition.
//!
//! Used to build higher-level primitives (Mpsc2, Mpmc2, …) on top of the
//! canonical [`Ring`](crate::stream::Ring) without paying Ring's internal
//! wake fence twice. The higher-level primitive holds its own real
//! `Waiter` for the M:1 or M:N fan-in path; each internal `Ring` runs
//! with `W = NoopWaiter` so its per-op `wake()` becomes a zero-instruction
//! no-op after inlining.
//!
//! ## Safety of "no-op wake"
//!
//! `wake()` doing nothing is safe iff no one is ever waiting on this
//! waiter. That's the contract for internal composition: the higher-level
//! primitive owns the ONE real waiter that consumers park on, and only
//! calls `try_send`/`try_recv` on the inner ring — never `send`/`recv`
//! (which are the only paths that call `wait_until`).
//!
//! `NoopWaiter` deliberately does NOT implement [`BlockingWaiter`] or
//! [`AsyncWaiter`], so any accidental call to `send`/`recv` on the inner
//! ring is a **compile-time error**, not a silent hang.

use super::Waiter;

/// Zero-cost `Waiter` for internal ring composition. See module docs.
#[derive(Debug, Default)]
pub struct NoopWaiter;

impl Waiter for NoopWaiter {
    /// No-op. Registration is meaningless here — no one waits on this
    /// waiter.
    #[inline(always)]
    fn set_worker(&self, _thread: std::thread::Thread) {}

    /// Always `true` — satisfies any `has_worker()` assertion in the
    /// inner ring's spin path, though the assertion is never reached
    /// because the outer primitive doesn't call `wait_until`.
    #[inline(always)]
    fn has_worker(&self) -> bool {
        true
    }

    /// No-op. Compiles to nothing.
    #[inline(always)]
    fn wake(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wake_is_noop_and_does_not_panic() {
        let w = NoopWaiter;
        for _ in 0..1_000_000 {
            w.wake();
        }
    }

    #[test]
    fn set_worker_is_noop() {
        let w = NoopWaiter::default();
        w.set_worker(std::thread::current());
        assert!(w.has_worker());
    }

    #[test]
    fn is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<NoopWaiter>();
    }
}
