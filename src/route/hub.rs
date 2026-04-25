//! N:1 multiplexed hub with per-port reply.
//!
//! [`Hub<In, Out>`] wires `N` producer ports to a single consumer
//! ("drain"), using [`SignalSet`] as the multiplexor and per-port
//! [`Pipe<Out>`] for replies.
//!
//! ## Topology
//!
//! ```text
//!   port 0 ──┐ in[0]    ──┐                         ┌── out[0] ──► port 0
//!   port 1 ──┤ in[1]    ──┤  SignalSet   ──► drain ─┤── out[1] ──► port 1
//!     ⋮      │                (N bits)              │      ⋮
//!   port N-1 ┘ in[N-1]  ──┘                         └── out[N-1] ► port N-1
//! ```
//!
//! ## Hot-path cost per send
//!
//! Inbound does **not** use a per-port `Signal`. The [`SignalSet`] bit IS
//! the signal. A port's `send` is:
//!
//! 1. `inbound[i].write(v)` — one store to the slot.
//! 2. `coordinator.release(id)` — one `fetch_or(Release)` on the bitmap,
//!    + a `parked.load(Relaxed)` that skips the syscall if the drain is
//!    spinning.
//!
//! That is **one atomic** in the common case, vs the two we'd pay if each
//! port used a full `Pipe<In>` (pipe.signal.release + coordinator.release).
//!
//! ## Fairness
//!
//! When multiple ports have pending messages, the drain processes them in
//! round-robin order starting from a persistent cursor. No port can starve
//! another.
//!
//! ## SPSC contract per port
//!
//! Each port is **one producer, one consumer (the drain for inbound, the
//! port's own thread for outbound)**. A port must not issue a second
//! `send` while the first is still in flight (coordinator bit still set).
//! Use [`HubPort::is_idle`] to check, or follow the natural call/reply
//! pattern via [`HubPort::call`].
//!
//! In debug builds `send` asserts the port is idle; in release it
//! overwrites (leaking any RAII resources in the prior value).
//!
//! ## Limits
//!
//! - `N ≤ 63` user ports (bit 63 of the coordinator is reserved for
//!   [`HubShutdown`] wake-ups).
//! - `N = 0` is rejected.
//!
//! ## Shutdown
//!
//! [`HubDrain::shutdown_handle`] returns a cloneable `Send + Sync`
//! [`HubShutdown`] that a supervisor thread can call to wake the drain
//! out of a blocking `recv_batch`. Blocking ops return `Err(Shutdown)`
//! once shutdown is requested.

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::Arc;

use crate::gate::{SignalId, SignalSet, MAX_GATES};
use crate::slot::Pipe;

/// Index of the reserved shutdown bit in the coordinator `SignalSet`.
/// User ports occupy `0..MAX_GATES - 1`.
const SHUTDOWN_BIT: u8 = (MAX_GATES - 1) as u8;

/// Maximum number of user ports in a [`Hub`]. One bit is reserved for
/// [`HubShutdown`], so this is `MAX_GATES - 1 = 63`.
pub const MAX_HUB_PORTS: usize = MAX_GATES - 1;

/// Returned by blocking drain ops when a [`HubShutdown::signal`] has
/// been issued. Callers should break out of their drain loop.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Shutdown;

/// Per-port inbound slot, padded to a full cache line so two ports
/// firing concurrently don't false-share their slot writes (each producer
/// writes its slot before the bitmap `fetch_or`; if N slots fit in one
/// 64 B line, multiple producers ping-pong the line).
#[repr(C, align(64))]
struct InboundSlot<In>(UnsafeCell<MaybeUninit<In>>);

/// Shared state of a hub. Users interact via [`HubPort`] / [`HubDrain`].
#[repr(C)]
pub struct Hub<In, Out> {
    /// N-bit multiplexor: bit `i` set ⇔ `inbound[i]` holds a value.
    /// Bit 63 (`shutdown_id`) doubles as the shutdown latch — once set
    /// it is never cleared, so every subsequent `recv_batch` returns
    /// `Err(Shutdown)` immediately.
    coordinator: SignalSet,
    /// `ids[i]` is the `SignalId` for port `i`.
    ids: Vec<SignalId>,
    /// Per-port inbound slot. Cache-line padded (see [`InboundSlot`]).
    inbound: Vec<InboundSlot<In>>,
    /// Per-port outbound: full `Pipe<Out>`, since the port thread may park
    /// waiting for its reply.
    outbound: Vec<Pipe<Out>>,
    /// Bitmask with one bit per registered port. Cached for `acquire_any`.
    full_mask: u64,
    /// Reserved bit for shutdown wake. Bit 63 of `coordinator` is the
    /// single source of truth for shutdown state.
    shutdown_id: SignalId,
}

// Safety: inbound slot access is serialized by the coordinator bit handshake
// — the port writes before `release` (Release), the drain reads after
// `state()` (Acquire). Each port is SPSC, so there is no concurrent access
// to the same slot. `outbound[i]` is its own SPSC Pipe and enforces the
// same discipline.
unsafe impl<In: Send, Out: Send> Sync for Hub<In, Out> {}
unsafe impl<In: Send, Out: Send> Send for Hub<In, Out> {}

impl<In: Send, Out: Send> Hub<In, Out> {
    /// Build a hub with `n` ports. Returns a [`HubDrain`] and a `Vec` of
    /// [`HubPort`]s, each one movable to its owning producer thread.
    ///
    /// # Panics
    /// - `n == 0`
    /// - `n > MAX_HUB_PORTS` (63)
    pub fn new(n: usize) -> (HubDrain<In, Out>, Vec<HubPort<In, Out>>) {
        assert!(n > 0, "Hub::new: n must be > 0");
        assert!(n <= MAX_HUB_PORTS, "Hub::new: n must be <= {MAX_HUB_PORTS}");

        let mut coordinator = SignalSet::new();
        let mut ids = Vec::with_capacity(n);
        for i in 0..n {
            // Name is cold-path debug info; we can afford the leak at init.
            let name: &'static str = Box::leak(format!("hub_port_{i}").into_boxed_str());
            ids.push(coordinator.create(name));
        }
        // Shutdown rides on a fixed reserved bit; we don't register it with
        // `create` (which allocates sequentially from bit 0). The bit is
        // fine to use directly because bitmap ops only care about the mask.
        let shutdown_id = SignalId::new(SHUTDOWN_BIT);

        let full_mask: u64 = ids.iter().map(|id| id.mask()).fold(0, |a, b| a | b);

        let mut inbound = Vec::with_capacity(n);
        let mut outbound = Vec::with_capacity(n);
        for _ in 0..n {
            inbound.push(InboundSlot(UnsafeCell::new(MaybeUninit::uninit())));
            outbound.push(Pipe::new());
        }

        let hub = Arc::new(Hub {
            coordinator,
            ids,
            inbound,
            outbound,
            full_mask,
            shutdown_id,
        });

        let ports: Vec<HubPort<In, Out>> = (0..n)
            .map(|i| HubPort {
                hub: hub.clone(),
                idx: i,
                id: hub.ids[i],
                _not_sync: PhantomData,
            })
            .collect();

        let drain = HubDrain {
            hub,
            cursor: Cell::new(0),
            _not_sync: PhantomData,
        };

        (drain, ports)
    }

    /// Number of ports.
    #[inline]
    pub fn len(&self) -> usize { self.ids.len() }

    /// Bitmask covering every port. Useful for custom `acquire_*` calls.
    #[inline]
    pub fn full_mask(&self) -> u64 { self.full_mask }
}

impl<In, Out> Drop for Hub<In, Out> {
    fn drop(&mut self) {
        // Any port whose coordinator bit is still set has a live value in
        // its inbound slot that must be dropped to avoid leaking RAII.
        // Outbound `Pipe`s self-drain in their own `Drop`.
        let state = self.coordinator.state();
        for (i, _) in self.inbound.iter().enumerate() {
            let bit = 1u64 << i;
            if state & bit != 0 {
                // Safety: exclusive access via &mut self; bit set ⇒ slot init.
                unsafe { (*self.inbound[i].0.get()).assume_init_drop(); }
            }
        }
    }
}

// ─── Port ──────────────────────────────────────────────────────────────────

/// Producer handle for one slot of a [`Hub`].
///
/// `Send` but not `Sync`: the SPSC contract forbids two threads from using
/// the same port concurrently.
pub struct HubPort<In: Send, Out: Send> {
    hub: Arc<Hub<In, Out>>,
    idx: usize,
    id:  SignalId,
    _not_sync: PhantomData<Cell<()>>,
}

impl<In: Send, Out: Send> HubPort<In, Out> {
    /// Numeric index of this port within the hub (`0..hub.len()`).
    #[inline]
    pub fn index(&self) -> usize { self.idx }

    /// Register this thread as the port's consumer of replies. Call once,
    /// from the thread that will invoke [`recv_reply`](Self::recv_reply) /
    /// [`call`](Self::call).
    #[inline]
    pub fn bind(&self) {
        self.hub.outbound[self.idx].set_consumer(std::thread::current());
    }

    /// `true` if the port has no in-flight send (coordinator bit clear).
    #[inline]
    pub fn is_idle(&self) -> bool { !self.hub.coordinator.is_open(self.id) }

    /// Send `v` to the drain. Assumes the port is idle.
    ///
    /// In debug builds this is asserted; in release, sending on a busy
    /// port overwrites the previous value and leaks its `Drop` glue.
    #[inline]
    pub fn send(&self, v: In) {
        debug_assert!(
            self.is_idle(),
            "HubPort::send called on busy port {}: caller must drain reply first",
            self.idx
        );
        // Safety: SPSC contract: exclusive producer for this slot, and the
        // slot is empty (coordinator bit was clear). The subsequent
        // `release` uses Release ordering, publishing this write.
        unsafe { (*self.hub.inbound[self.idx].0.get()).write(v); }
        self.hub.coordinator.release(self.id);
    }

    /// Non-blocking send. Returns `Err(v)` if the port is still busy.
    #[inline]
    pub fn try_send(&self, v: In) -> Result<(), In> {
        if !self.is_idle() { return Err(v); }
        // Safety: same as `send`.
        unsafe { (*self.hub.inbound[self.idx].0.get()).write(v); }
        self.hub.coordinator.release(self.id);
        Ok(())
    }

    /// Block until the drain replies, then take the `Out`.
    #[inline]
    pub fn recv_reply(&self) -> Out {
        self.hub.outbound[self.idx].recv()
    }

    /// Non-blocking reply take. `Some(o)` if the drain has replied,
    /// `None` otherwise.
    #[inline]
    pub fn try_recv_reply(&self) -> Option<Out> {
        self.hub.outbound[self.idx].try_recv()
    }

    /// Convenience: send and block for the reply. Must be called from the
    /// thread that has called [`bind`](Self::bind).
    #[inline]
    pub fn call(&self, v: In) -> Out {
        self.send(v);
        self.recv_reply()
    }
}

// ─── Drain ─────────────────────────────────────────────────────────────────

/// Consumer handle for a [`Hub`]. One per hub; `!Sync` by construction.
pub struct HubDrain<In: Send, Out: Send> {
    hub: Arc<Hub<In, Out>>,
    /// Round-robin starting point for the next `recv_batch`.
    cursor: Cell<u32>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<In: Send, Out: Send> HubDrain<In, Out> {
    /// Register this thread as the drain. Call once before any blocking
    /// `recv_*` method.
    #[inline]
    pub fn bind(&self) {
        self.hub.coordinator.set_worker(std::thread::current());
    }

    /// Number of ports.
    #[inline]
    pub fn len(&self) -> usize { self.hub.len() }

    /// Build a [`HubShutdown`] handle. The handle is cheap to clone and may
    /// be passed to supervisor threads to wake the drain out of a blocking
    /// `recv_batch`.
    #[inline]
    pub fn shutdown_handle(&self) -> HubShutdown<In, Out> {
        HubShutdown { hub: self.hub.clone() }
    }

    /// Block until at least one port has a pending message; then invoke
    /// `f(port_idx, msg, reply)` for **every** port that is currently open,
    /// in round-robin order from the persistent cursor.
    ///
    /// The cursor advances by one slot after each batch, so even if one
    /// port publishes continuously the others are visited fairly.
    pub fn recv_batch<F: FnMut(usize, In, HubReply<'_, Out>)>(
        &self,
        mut f: F,
    ) -> Result<(), Shutdown> {
        let n = self.hub.ids.len();
        // Include the shutdown bit in the wait mask so a `HubShutdown::signal`
        // wakes us even when no port has pending work.
        let shutdown_mask = self.hub.shutdown_id.mask();
        let wake_mask = self.hub.full_mask | shutdown_mask;
        self.hub.coordinator.acquire_any(wake_mask);

        let state = self.hub.coordinator.state();
        if state & shutdown_mask != 0 {
            // Bit 63 is the latch — left set so subsequent recv_batch calls
            // also return Err(Shutdown) immediately. Drain in-flight first
            // so producers' values aren't stranded.
            self.drain_available(&mut f);
            return Err(Shutdown);
        }

        let start = self.cursor.get() as usize % n;
        // Iterate only set bits in round-robin order from `start`.
        // Split the active mask into [start..n) and [0..start), then scan
        // each half via `trailing_zeros`. Skips empty slots entirely — a
        // big win for sparse hubs (N=32, 1 active: ~3× faster drain).
        let active = state & self.hub.full_mask;
        let split = if start == 0 { 0u64 } else { (1u64 << start) - 1 };
        let high = active & !split;
        let low  = active & split;
        for mut m in [high, low] {
            while m != 0 {
                let i = m.trailing_zeros() as usize;
                let bit = 1u64 << i;
                m &= m.wrapping_sub(1);
                // Safety: bit set ⇒ producer wrote the slot before the
                // Release that we observed via `state()` (Acquire).
                let v = unsafe { (*self.hub.inbound[i].0.get()).assume_init_read() };
                // Clear the bit **before** running the user callback: the
                // callback likely calls `reply.send`, which will wake the
                // port, and a fast port may re-send on the same slot before
                // we finish the loop. We must publish "slot empty" first.
                self.hub.coordinator.lock_mask(bit);
                let reply = HubReply { pipe: &self.hub.outbound[i] };
                f(i, v, reply);
            }
        }
        self.cursor.set(((start + 1) % n) as u32);
        Ok(())
    }

    /// Internal: drain whatever's currently pending without blocking. Used
    /// during shutdown to avoid leaking in-flight messages.
    fn drain_available<F: FnMut(usize, In, HubReply<'_, Out>)>(&self, f: &mut F) {
        let state = self.hub.coordinator.state();
        let mut active = state & self.hub.full_mask;
        if active == 0 { return; }
        // Iterate only set bits. Order is low-to-high since shutdown
        // doesn't care about round-robin fairness.
        while active != 0 {
            let i = active.trailing_zeros() as usize;
            let bit = 1u64 << i;
            active &= active.wrapping_sub(1);
            let v = unsafe { (*self.hub.inbound[i].0.get()).assume_init_read() };
            self.hub.coordinator.lock_mask(bit);
            let reply = HubReply { pipe: &self.hub.outbound[i] };
            f(i, v, reply);
        }
    }

    /// Non-blocking peek-and-drain. If at least one port is open, runs the
    /// batch and returns `true`; otherwise returns `false` immediately.
    pub fn try_recv_batch<F: FnMut(usize, In, HubReply<'_, Out>)>(&self, mut f: F) -> bool {
        let state = self.hub.coordinator.state();
        let active = state & self.hub.full_mask;
        if active == 0 { return false; }
        let n = self.hub.ids.len();
        let start = self.cursor.get() as usize % n;
        // Iterate only set bits in round-robin order from `start`
        // (see `recv_batch` for rationale).
        let split = if start == 0 { 0u64 } else { (1u64 << start) - 1 };
        let high = active & !split;
        let low  = active & split;
        for mut m in [high, low] {
            while m != 0 {
                let i = m.trailing_zeros() as usize;
                let bit = 1u64 << i;
                m &= m.wrapping_sub(1);
                let v = unsafe { (*self.hub.inbound[i].0.get()).assume_init_read() };
                // See rationale in `recv_batch`.
                self.hub.coordinator.lock_mask(bit);
                let reply = HubReply { pipe: &self.hub.outbound[i] };
                f(i, v, reply);
            }
        }
        self.cursor.set(((start + 1) % n) as u32);
        true
    }
}

// ─── Shutdown handle ───────────────────────────────────────────────────────

/// Supervisor-side handle used to wake a [`HubDrain`] out of a blocking
/// `recv_batch`. Cheap to clone; `Send + Sync`.
pub struct HubShutdown<In: Send, Out: Send> {
    hub: Arc<Hub<In, Out>>,
}

impl<In: Send, Out: Send> Clone for HubShutdown<In, Out> {
    fn clone(&self) -> Self { Self { hub: self.hub.clone() } }
}

impl<In: Send, Out: Send> HubShutdown<In, Out> {
    /// Flag the hub as shutting down and wake the drain. Idempotent.
    /// Bit 63 of the coordinator is the single source of truth — once set
    /// it is never cleared, so the latch is correctly observed by every
    /// future `recv_batch`.
    #[inline]
    pub fn signal(&self) {
        self.hub.coordinator.release(self.hub.shutdown_id);
    }

    /// `true` if `signal` has been called on any clone of this handle.
    #[inline]
    pub fn is_signaled(&self) -> bool {
        self.hub.coordinator.is_open(self.hub.shutdown_id)
    }
}

// ─── Reply handle ──────────────────────────────────────────────────────────

/// Reply capability handed to the drain's callback. Consume it to send
/// the `Out` back to the originating port.
///
/// Dropping a `HubReply` without calling [`send`](Self::send) leaves the
/// port's `recv_reply` blocked — intentional, to surface missing replies.
pub struct HubReply<'a, Out: Send> {
    pipe: &'a Pipe<Out>,
}

impl<'a, Out: Send> HubReply<'a, Out> {
    /// Send the reply to the port's outbound pipe and unpark if needed.
    #[inline]
    pub fn send(self, v: Out) { self.pipe.send(v); }

    /// Access the underlying outbound pipe for advanced composition.
    #[inline]
    pub fn pipe(&self) -> &Pipe<Out> { self.pipe }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn single_port_roundtrip() {
        let (drain, mut ports) = Hub::<u64, u64>::new(1);
        let p = ports.remove(0);

        let d = thread::spawn(move || {
            drain.bind();
            drain.recv_batch(|idx, msg, reply| {
                assert_eq!(idx, 0);
                reply.send(msg * 2);
            }).unwrap();
        });

        thread::sleep(Duration::from_millis(10));
        p.bind();
        assert_eq!(p.call(21), 42);
        d.join().unwrap();
    }

    #[test]
    fn four_ports_roundtrip() {
        let (drain, ports) = Hub::<u64, u64>::new(4);
        let shutdown = drain.shutdown_handle();

        let d = thread::spawn(move || {
            drain.bind();
            loop {
                match drain.recv_batch(|idx, msg, reply| {
                    reply.send(msg + idx as u64 * 1000);
                }) {
                    Ok(()) => continue,
                    Err(Shutdown) => break,
                }
            }
        });

        thread::sleep(Duration::from_millis(10));

        let handles: Vec<_> = ports.into_iter().enumerate().map(|(i, p)| {
            thread::spawn(move || {
                p.bind();
                for k in 0..50u64 {
                    let r = p.call(k);
                    assert_eq!(r, k + i as u64 * 1000);
                }
            })
        }).collect();
        for h in handles { h.join().unwrap(); }
        shutdown.signal();
        d.join().unwrap();
    }

    #[test]
    fn fair_round_robin() {
        // Two ports, both always busy; verify both get serviced.
        let (drain, mut ports) = Hub::<u64, u64>::new(2);
        let shutdown = drain.shutdown_handle();
        let p0 = ports.remove(0);
        let p1 = ports.remove(0);

        let counts = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let counts_d = counts.clone();

        let d = thread::spawn(move || {
            drain.bind();
            loop {
                match drain.recv_batch(|idx, msg, reply| {
                    counts_d[idx].fetch_add(1, Ordering::Relaxed);
                    reply.send(msg);
                }) {
                    Ok(()) => continue,
                    Err(Shutdown) => break,
                }
            }
        });

        thread::sleep(Duration::from_millis(10));

        let h0 = thread::spawn(move || {
            p0.bind();
            for k in 0..500u64 { p0.call(k); }
        });
        let h1 = thread::spawn(move || {
            p1.bind();
            for k in 0..500u64 { p1.call(k); }
        });
        h0.join().unwrap();
        h1.join().unwrap();
        shutdown.signal();
        d.join().unwrap();

        let c0 = counts[0].load(Ordering::Relaxed);
        let c1 = counts[1].load(Ordering::Relaxed);
        assert_eq!(c0, 500);
        assert_eq!(c1, 500);
    }

    #[test]
    fn shutdown_wakes_parked_drain() {
        let (drain, _ports) = Hub::<u64, u64>::new(2);
        let shutdown = drain.shutdown_handle();

        let d = thread::spawn(move || {
            drain.bind();
            let r = drain.recv_batch(|_, _, _| unreachable!("no port fired"));
            r
        });

        // Drain is definitely parked by now.
        thread::sleep(Duration::from_millis(30));
        shutdown.signal();
        let res = d.join().unwrap();
        assert_eq!(res, Err(Shutdown));
        assert!(shutdown.is_signaled());
    }

    #[test]
    fn shutdown_drains_inflight_before_returning() {
        let (drain, mut ports) = Hub::<u64, u64>::new(1);
        let shutdown = drain.shutdown_handle();
        let p = ports.remove(0);

        // Producer sends then shutdown is signaled before drain wakes.
        p.try_send(99).unwrap();
        shutdown.signal();

        let received = Arc::new(AtomicUsize::new(0));
        let received_d = received.clone();
        let d = thread::spawn(move || {
            drain.bind();
            drain.recv_batch(|_, msg, _reply| {
                received_d.store(msg as usize, Ordering::Relaxed);
            })
        });
        let res = d.join().unwrap();
        assert_eq!(res, Err(Shutdown));
        assert_eq!(received.load(Ordering::Relaxed), 99,
                   "in-flight message drained before Shutdown");
    }

    #[test]
    fn try_send_returns_err_when_busy() {
        let (_drain, mut ports) = Hub::<u64, u64>::new(1);
        let p = ports.remove(0);
        assert!(p.try_send(1).is_ok());
        // Drain never bound; the second send sees the bit still set.
        assert_eq!(p.try_send(2), Err(2));
    }

    #[test]
    fn drop_drains_inflight_inbound() {
        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let (_drain, mut ports) = Hub::<Tracked, ()>::new(2);
            let p0 = ports.remove(0);
            p0.try_send(Tracked(drops.clone())).ok().unwrap();
            // _drain drops here, then hub (when last Arc gone): bit 0 is
            // still set, so the Tracked gets dropped by Hub::drop.
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    #[should_panic(expected = "n must be <= 63")]
    fn rejects_too_many_ports() {
        let _ = Hub::<u8, u8>::new(64);
    }

    #[test]
    #[should_panic(expected = "n must be > 0")]
    fn rejects_zero_ports() {
        let _ = Hub::<u8, u8>::new(0);
    }

    #[test]
    fn box_ownership_through_hub() {
        let (drain, mut ports) = Hub::<Box<Vec<u8>>, Box<Vec<u8>>>::new(1);
        let p = ports.remove(0);

        let d = thread::spawn(move || {
            drain.bind();
            drain.recv_batch(|_, mut msg, reply| {
                for b in msg.iter_mut() { *b = b.wrapping_add(1); }
                reply.send(msg);
            }).unwrap();
        });
        thread::sleep(Duration::from_millis(10));
        p.bind();
        let payload = Box::new(vec![1u8, 2, 3, 4]);
        let ptr_before = payload.as_ptr() as usize;
        let r = p.call(payload);
        assert_eq!(*r, vec![2, 3, 4, 5]);
        assert_eq!(r.as_ptr() as usize, ptr_before, "zero-copy");
        d.join().unwrap();
    }
}
