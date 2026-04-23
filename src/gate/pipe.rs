//! Single-slot SPSC transport built on one [`Signal`].
//!
//! [`Pipe<T, H>`] is the minimal atom between [`Signal`] (no payload) and
//! [`Channel`](super::Channel) (bidirectional request/response): one producer
//! sends a `T`, one consumer receives it, a single [`Signal`] coordinates.
//!
//! ## Wire model
//!
//! ```text
//!  producer thread                        consumer thread
//!  ───────────────                        ───────────────
//!  hook.on_send(&v)
//!  write    slot     ──┐
//!  release  signal   ──┘→ coherence →  ─→ acquire  signal
//!                                          read     slot
//!                                          lock     signal
//!                                          hook.on_recv(&v)
//! ```
//!
//! ## Why this exists
//!
//! Higher-level primitives (`Channel`, future `Duplex`, future `Hub`) are
//! just compositions of `Pipe`s. Exposing the atom lets users build their
//! own topology without reaching into `Signal` internals.
//!
//! ## Hooks — zero-cost observation
//!
//! The generic `H: PipeHook<T>` defaults to [`NoHook`], a ZST whose default
//! methods are empty `#[inline]` functions. The optimizer eliminates the
//! calls entirely — a `Pipe<T>` (i.e. `Pipe<T, NoHook>`) has **exactly** the
//! same cost as a hand-rolled `Signal + slot` pair.
//!
//! When you *do* want observation (metrics, event propagation to a `Hub`,
//! tracing spans), implement [`PipeHook<T>`] on your own type. The cost is
//! paid only by the subscriber, and only on the paths you care about.
//!
//! ```no_run
//! use arbitro_kit::gate::{Pipe, PipeHook};
//!
//! struct Counting(std::sync::atomic::AtomicU64);
//! impl PipeHook<u64> for Counting {
//!     #[inline] fn on_send(&self, _v: &u64) {
//!         self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
//!     }
//! }
//!
//! let p: Pipe<u64, Counting> =
//!     Pipe::with_hook(Counting(Default::default()));
//! p.send(1);
//! let _ = p.recv();
//! assert_eq!(p.hook().0.load(std::sync::atomic::Ordering::Relaxed), 1);
//! ```

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;

use crate::gate::Signal;

/// Observer hook for a [`Pipe`].
///
/// Both methods default to empty `#[inline]` no-ops. A hook with no custom
/// behavior (notably [`NoHook`]) compiles down to zero instructions on the
/// hot path.
///
/// `Send + Sync` supertraits let the enclosing `Pipe<T, H>` be shared
/// between the producer and consumer threads.
pub trait PipeHook<T>: Send + Sync {
    /// Called by the producer **before** the value is written to the slot
    /// and published via the signal. Runs on the producer thread.
    #[inline]
    fn on_send(&self, _v: &T) {}

    /// Called by the consumer **after** the value is taken from the slot
    /// and the signal is locked. Runs on the consumer thread.
    #[inline]
    fn on_recv(&self, _v: &T) {}
}

/// Default no-op hook. Zero-sized; fully elided by the optimizer.
#[derive(Default, Copy, Clone, Debug)]
pub struct NoHook;

impl<T> PipeHook<T> for NoHook {}

/// SPSC single-slot pipe.
///
/// Generic over payload `T` (`Send`) and observer `H: PipeHook<T>`. The
/// default `H = NoHook` makes `Pipe<T>` a drop-in minimal transport.
///
/// ## Concurrency contract
///
/// - Exactly one producer thread calls [`send`](Self::send).
/// - Exactly one consumer thread calls [`recv`](Self::recv) /
///   [`try_recv`](Self::try_recv).
/// - The consumer must register its `Thread` handle via
///   [`set_consumer`](Self::set_consumer) (or via the underlying
///   [`signal()`](Self::signal)) before calling `recv` on a pipe that
///   may block.
///
/// ## Drop safety
///
/// If a value is in flight when the pipe is dropped (producer called
/// `send` but consumer never called `recv`), the value is dropped in the
/// pipe's [`Drop`] impl — safe for `T` holding RAII resources.
#[repr(C)]
pub struct Pipe<T, H: PipeHook<T> = NoHook> {
    signal: Signal,
    slot:   UnsafeCell<MaybeUninit<T>>,
    hook:   H,
    _marker: PhantomData<fn() -> T>,
}

// Safety: slot access is serialized by the signal handshake — the producer
// writes the slot before `signal.release()` (Release store), the consumer
// observes the open state via `signal.acquire()` (Acquire load), reads the
// slot, then `signal.lock()`s. Exactly-one-producer and exactly-one-consumer
// is an unchecked runtime contract of the SPSC type.
unsafe impl<T: Send, H: PipeHook<T>> Send for Pipe<T, H> {}
unsafe impl<T: Send, H: PipeHook<T>> Sync for Pipe<T, H> {}

impl<T: Send> Pipe<T, NoHook> {
    /// Create a pipe with the zero-cost default hook.
    pub fn new() -> Self {
        Self::with_hook(NoHook)
    }
}

impl<T: Send> Default for Pipe<T, NoHook> {
    fn default() -> Self { Self::new() }
}

impl<T: Send, H: PipeHook<T>> Pipe<T, H> {
    /// Create a pipe with a custom observer hook.
    pub fn with_hook(hook: H) -> Self {
        Self {
            signal: Signal::new(),
            slot: UnsafeCell::new(MaybeUninit::uninit()),
            hook,
            _marker: PhantomData,
        }
    }

    /// Borrow the underlying [`Signal`]. Useful for composing a pipe into
    /// larger topologies (e.g. registering its worker with a coordinator).
    #[inline]
    pub fn signal(&self) -> &Signal { &self.signal }

    /// Borrow the observer hook. Handy when the hook carries state
    /// (counters, channels) that external code wants to inspect.
    #[inline]
    pub fn hook(&self) -> &H { &self.hook }

    /// Register the consumer thread. Must be called from the consumer
    /// thread before the first blocking [`recv`](Self::recv).
    #[inline]
    pub fn set_consumer(&self, t: std::thread::Thread) {
        self.signal.set_worker(t);
    }

    /// Send `v` across the pipe.
    ///
    /// Must only be called from the single producer thread, and only when
    /// the pipe is empty (the consumer has drained the previous value).
    /// Breaking either invariant is a logic bug: the pipe does not check.
    #[inline]
    pub fn send(&self, v: T) {
        self.hook.on_send(&v);
        // Safety: SPSC contract: we are the sole producer and the slot is
        // empty (consumer just `lock()`ed it, or it's the initial uninit).
        unsafe { (*self.slot.get()).write(v); }
        self.signal.release();
    }

    /// Block until the producer sends, then take the value.
    ///
    /// Must only be called from the registered consumer thread.
    #[inline]
    pub fn recv(&self) -> T {
        self.signal.acquire();
        // Safety: producer wrote the slot and published via `release()`;
        // our `acquire()` synchronizes-with it.
        let v = unsafe { (*self.slot.get()).assume_init_read() };
        self.signal.lock();
        self.hook.on_recv(&v);
        v
    }

    /// Non-blocking take. Returns `Some(v)` if a value is pending,
    /// `None` otherwise.
    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        if !self.signal.is_open() { return None; }
        // Safety: same as `recv`; `is_open` performed an Acquire load that
        // synchronized-with the producer's Release.
        let v = unsafe { (*self.slot.get()).assume_init_read() };
        self.signal.lock();
        self.hook.on_recv(&v);
        Some(v)
    }

    /// True iff a value is pending in the slot.
    #[inline]
    pub fn has_data(&self) -> bool { self.signal.is_open() }
}

impl<T, H: PipeHook<T>> Drop for Pipe<T, H> {
    fn drop(&mut self) {
        // Safety: `&mut self` means no other references exist. If the
        // signal is open, the slot holds an initialized value that must
        // be dropped to avoid leaking RAII resources.
        if self.signal.is_open() {
            unsafe { (*self.slot.get()).assume_init_drop(); }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn basic_send_recv() {
        let p: Arc<Pipe<u64>> = Arc::new(Pipe::new());
        let p2 = p.clone();
        let h = std::thread::spawn(move || {
            p2.set_consumer(std::thread::current());
            p2.recv()
        });
        std::thread::sleep(std::time::Duration::from_millis(10));
        p.send(42);
        assert_eq!(h.join().unwrap(), 42);
    }

    #[test]
    fn try_recv_empty() {
        let p: Pipe<u64> = Pipe::new();
        assert_eq!(p.try_recv(), None);
        p.send(7);
        assert_eq!(p.try_recv(), Some(7));
        assert_eq!(p.try_recv(), None);
    }

    #[test]
    fn hook_fires_on_both_sides() {
        #[derive(Default)]
        struct Counters { sends: AtomicU64, recvs: AtomicU64 }
        impl PipeHook<u64> for Counters {
            fn on_send(&self, _: &u64) { self.sends.fetch_add(1, Ordering::Relaxed); }
            fn on_recv(&self, _: &u64) { self.recvs.fetch_add(1, Ordering::Relaxed); }
        }

        let p: Pipe<u64, Counters> = Pipe::with_hook(Counters::default());
        for i in 0..5 {
            p.send(i);
            assert_eq!(p.recv(), i);
        }
        assert_eq!(p.hook().sends.load(Ordering::Relaxed), 5);
        assert_eq!(p.hook().recvs.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn drop_drains_inflight() {
        struct Tracked(Arc<AtomicU64>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }
        let drops = Arc::new(AtomicU64::new(0));
        {
            let p: Pipe<Tracked> = Pipe::new();
            p.send(Tracked(drops.clone()));
            // never recv — drop must drain
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn box_ownership_transfer() {
        let p: Arc<Pipe<Box<Vec<u8>>>> = Arc::new(Pipe::new());
        let p2 = p.clone();
        let h = std::thread::spawn(move || {
            p2.set_consumer(std::thread::current());
            p2.recv()
        });
        std::thread::sleep(std::time::Duration::from_millis(10));
        let payload = Box::new(vec![1u8, 2, 3, 4]);
        let ptr_before = payload.as_ptr() as usize;
        p.send(payload);
        let got = h.join().unwrap();
        assert_eq!(*got, vec![1, 2, 3, 4]);
        assert_eq!(got.as_ptr() as usize, ptr_before, "heap buffer did not move");
    }

    #[test]
    fn signal_accessor_exposes_primitive() {
        let p: Pipe<u32> = Pipe::new();
        assert!(!p.signal().is_open());
        p.send(1);
        assert!(p.signal().is_open());
        assert_eq!(p.recv(), 1);
        assert!(!p.signal().is_open());
    }
}
