//! Cancellation primitive — fire-and-forget shutdown for any number of
//! parked consumer threads.
//!
//! ## Model
//!
//! A [`Lifeline`] is a **scope** of up to 64 indexed waiters. Workers
//! register their thread handle and receive a [`WaiterId`]. From any
//! other thread you can:
//!
//! - [`Lifeline::cancel_one`]   — wake one specific waiter.
//! - [`Lifeline::cancel_mask`]  — wake any subset by bitmask.
//! - [`Lifeline::cancel_all`]   — wake every registered waiter.
//!
//! All three are **fire-and-forget**: they set the cancel state, issue
//! the `unpark()` calls, and return. They do not wait for the workers
//! to acknowledge or finish in-flight work. Each worker discovers
//! cancellation the next time it checks (cheap: 2 atomic loads).
//!
//! ## Wiring with transports
//!
//! Transports that can park (currently `Stream`, `Ring`, `Duplex`)
//! expose `recv_or_cancel(&Lifeline, WaiterId)` methods alongside
//! their normal `recv()`. The normal `recv()` is **unchanged** —
//! adopting `Lifeline` costs nothing for callers that don't use it.
//!
//! ## Cost
//!
//! - `is_cancelled(id)` hot path: 2 atomic loads (~1 ns), no lock.
//! - `cancel_one(id)`: 1 atomic OR + 1 `unpark` ≈ 100–500 ns.
//! - `cancel_all()`: 1 atomic store + N unparks (~50–100 ns each).
//! - Memory: ~1.6 KB per Lifeline (64-slot waiter table).
//!
//! ## Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::thread;
//! use arbitro_kit::gate::Lifeline;
//! use arbitro_kit::stream::Stream;
//!
//! let life = Arc::new(Lifeline::new());
//! let stream: Arc<Stream<u64>> = Arc::new(Stream::new());
//!
//! // Worker
//! let l = life.clone();
//! let s = stream.clone();
//! let h = thread::spawn(move || {
//!     s.set_consumer(thread::current());
//!     let id = l.register(thread::current());
//!     loop {
//!         match s.recv_or_cancel(&l, id) {
//!             Ok(v)        => { let _ = v; }
//!             Err(_cancel) => break,
//!         }
//!     }
//! });
//!
//! // Elsewhere: shut everything down.
//! life.cancel_all();
//! h.join().unwrap();
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;
use std::thread::Thread;

/// Maximum number of waiters per [`Lifeline`].
pub const MAX_WAITERS: usize = 64;

/// Handle returned by [`Lifeline::register`]. Identifies one waiter
/// inside a Lifeline scope (0..64).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WaiterId(pub(crate) u8);

impl WaiterId {
    /// Wrap a raw waiter index. Normally you get a [`WaiterId`] from
    /// [`Lifeline::register`]; this constructor is for callers (tests,
    /// benches, integrations) that need to reconstruct one externally.
    ///
    /// Panics if `index >= MAX_WAITERS` (64).
    #[inline]
    pub fn new(index: u8) -> Self {
        assert!(
            (index as usize) < MAX_WAITERS,
            "WaiterId index {} out of range (MAX_WAITERS = {})",
            index,
            MAX_WAITERS
        );
        Self(index)
    }

    /// The raw bit index (0..64) used in `cancel_mask`.
    #[inline]
    pub fn index(self) -> u8 {
        self.0
    }

    /// The single-bit mask for this id.
    #[inline]
    pub fn bit(self) -> u64 {
        1u64 << self.0
    }
}

/// Returned by `recv_or_cancel` when the lifeline cancels the wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cancelled;

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("operation cancelled by Lifeline")
    }
}

impl std::error::Error for Cancelled {}

/// Cancellation scope. Holds up to 64 indexed waiters; cancellation
/// can target one, a subset, or everyone, fire-and-forget.
pub struct Lifeline {
    /// Per-waiter cancel bits. `cancel_one(id)` sets `1 << id`.
    cancelled_mask: AtomicU64,
    /// Global cancel — set by `cancel_all()`. When true, every
    /// `is_cancelled(id)` returns true.
    cancelled_global: AtomicBool,
    /// Next id handed out by `register`.
    next_id: AtomicU8,
    /// Registered thread handles, indexed by id. Only touched on
    /// register / cancel — never in the hot `is_cancelled` path.
    waiters: Mutex<[Option<Thread>; MAX_WAITERS]>,
}

impl Default for Lifeline {
    fn default() -> Self {
        Self::new()
    }
}

impl Lifeline {
    /// Construct an empty Lifeline. No waiters yet.
    pub fn new() -> Self {
        // Built-in arrays of `Option<T>` need an init helper since
        // `Option::None` is not `Copy` for arbitrary `T`.
        const INIT: Option<Thread> = None;
        Self {
            cancelled_mask: AtomicU64::new(0),
            cancelled_global: AtomicBool::new(false),
            next_id: AtomicU8::new(0),
            waiters: Mutex::new([INIT; MAX_WAITERS]),
        }
    }

    /// Register `t` for cancellation wake-ups. Returns its handle.
    ///
    /// Panics if more than [`MAX_WAITERS`] (64) workers register on
    /// the same Lifeline. Use multiple Lifelines for larger scopes.
    pub fn register(&self, t: Thread) -> WaiterId {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        assert!(
            (id as usize) < MAX_WAITERS,
            "Lifeline waiter limit ({}) exceeded; create more Lifelines or shard",
            MAX_WAITERS
        );
        let mut w = self.waiters.lock().unwrap();
        w[id as usize] = Some(t);
        WaiterId(id)
    }

    /// Has this specific waiter been cancelled? Hot path: 2 atomic
    /// loads, no lock, no syscall.
    #[inline]
    pub fn is_cancelled(&self, id: WaiterId) -> bool {
        // Global wins. Cheap branch — global is the rare case.
        self.cancelled_global.load(Ordering::Acquire)
            || (self.cancelled_mask.load(Ordering::Acquire) & id.bit()) != 0
    }

    /// True if `cancel_all()` has been called on this Lifeline.
    #[inline]
    pub fn is_cancelled_all(&self) -> bool {
        self.cancelled_global.load(Ordering::Acquire)
    }

    /// Snapshot of the per-waiter cancel mask. Bit `i` set ↔ waiter
    /// `i` was individually cancelled (does not include `cancel_all`).
    #[inline]
    pub fn cancelled_mask(&self) -> u64 {
        self.cancelled_mask.load(Ordering::Acquire)
    }

    /// Cancel one specific waiter. Sets its bit, then unparks its
    /// thread. Idempotent: cancelling an already-cancelled waiter is
    /// a no-op aside from a redundant unpark.
    pub fn cancel_one(&self, id: WaiterId) {
        self.cancelled_mask.fetch_or(id.bit(), Ordering::SeqCst);
        let to_wake = {
            let w = self.waiters.lock().unwrap();
            w[id.0 as usize].clone()
        };
        if let Some(t) = to_wake {
            t.unpark();
        }
    }

    /// Cancel every waiter whose bit is set in `mask`. Bits beyond
    /// the registered range are ignored.
    pub fn cancel_mask(&self, mask: u64) {
        if mask == 0 {
            return;
        }
        self.cancelled_mask.fetch_or(mask, Ordering::SeqCst);
        let to_wake: Vec<Thread> = {
            let w = self.waiters.lock().unwrap();
            (0..MAX_WAITERS)
                .filter(|i| mask & (1u64 << i) != 0)
                .filter_map(|i| w[i].clone())
                .collect()
        };
        for t in to_wake {
            t.unpark();
        }
    }

    /// Cancel every waiter ever registered on this Lifeline. Sets the
    /// global cancel bit (so future `register` calls also see it as
    /// cancelled) and unparks all current waiters.
    pub fn cancel_all(&self) {
        self.cancelled_global.store(true, Ordering::SeqCst);
        let to_wake: Vec<Thread> = {
            let w = self.waiters.lock().unwrap();
            w.iter().filter_map(|o| o.clone()).collect()
        };
        for t in to_wake {
            t.unpark();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn register_returns_sequential_ids() {
        let l = Lifeline::new();
        let id0 = l.register(thread::current());
        let id1 = l.register(thread::current());
        assert_eq!(id0.0, 0);
        assert_eq!(id1.0, 1);
    }

    #[test]
    fn cancel_one_only_marks_target() {
        let l = Lifeline::new();
        let a = l.register(thread::current());
        let b = l.register(thread::current());
        l.cancel_one(a);
        assert!(l.is_cancelled(a));
        assert!(!l.is_cancelled(b));
    }

    #[test]
    fn cancel_all_marks_every_id() {
        let l = Lifeline::new();
        let a = l.register(thread::current());
        let b = l.register(thread::current());
        let c = l.register(thread::current());
        l.cancel_all();
        assert!(l.is_cancelled(a));
        assert!(l.is_cancelled(b));
        assert!(l.is_cancelled(c));
        assert!(l.is_cancelled_all());
    }

    #[test]
    fn cancel_mask_marks_subset() {
        let l = Lifeline::new();
        let a = l.register(thread::current());
        let _b = l.register(thread::current());
        let c = l.register(thread::current());
        l.cancel_mask(a.bit() | c.bit());
        assert!(l.is_cancelled(a));
        assert!(!l.is_cancelled(_b));
        assert!(l.is_cancelled(c));
    }

    #[test]
    fn cancel_unparks_thread() {
        let l = Arc::new(Lifeline::new());
        let woken = Arc::new(AtomicUsize::new(0));

        let l2 = l.clone();
        let w2 = woken.clone();
        let h = thread::spawn(move || {
            let id = l2.register(thread::current());
            // Park until cancelled. (The recv_or_cancel helper would
            // do this for a real transport; here we hand-roll the
            // park to test Lifeline alone.)
            while !l2.is_cancelled(id) {
                thread::park();
            }
            w2.fetch_add(1, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(20));
        l.cancel_all();
        h.join().unwrap();
        assert_eq!(woken.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cancel_one_wakes_only_target() {
        use std::sync::mpsc;
        let l = Arc::new(Lifeline::new());
        let progress = Arc::new(AtomicUsize::new(0));

        // Capture each worker's actual `WaiterId` instead of assuming
        // spawn order — `register()` returns ids sequentially but the
        // scheduler may run B before A, in which case B would be
        // `WaiterId(0)` and the hard-coded `cancel_one(WaiterId(0))`
        // would target the wrong worker (cancelling B, leaving A
        // parked forever, `h_a.join()` hangs).
        let (id_tx_a, id_rx_a) = mpsc::channel::<WaiterId>();
        let (id_tx_b, id_rx_b) = mpsc::channel::<WaiterId>();

        // Two workers, only worker A gets cancelled individually.
        let l_a = l.clone();
        let p_a = progress.clone();
        let h_a = thread::spawn(move || {
            let id = l_a.register(thread::current());
            id_tx_a.send(id).unwrap();
            while !l_a.is_cancelled(id) {
                thread::park();
            }
            p_a.fetch_add(1, Ordering::SeqCst);
        });

        let l_b = l.clone();
        let p_b = progress.clone();
        let h_b = thread::spawn(move || {
            let id = l_b.register(thread::current());
            id_tx_b.send(id).unwrap();
            while !l_b.is_cancelled(id) {
                thread::park();
            }
            p_b.fetch_add(1, Ordering::SeqCst);
        });

        let id_a = id_rx_a.recv().unwrap();
        let _id_b = id_rx_b.recv().unwrap();

        // Let both park.
        thread::sleep(Duration::from_millis(30));

        // Cancel only worker A by its captured id.
        l.cancel_one(id_a);
        h_a.join().unwrap();
        assert_eq!(progress.load(Ordering::SeqCst), 1);

        // Now cancel the rest.
        l.cancel_all();
        h_b.join().unwrap();
        assert_eq!(progress.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn cancel_after_register_is_visible() {
        let l = Lifeline::new();
        let id = l.register(thread::current());
        assert!(!l.is_cancelled(id));
        l.cancel_one(id);
        assert!(l.is_cancelled(id));
    }

    #[test]
    fn cancel_all_visible_to_late_check() {
        let l = Lifeline::new();
        let id = l.register(thread::current());
        l.cancel_all();
        assert!(l.is_cancelled(id));
        assert!(l.is_cancelled_all());
    }
}
