//! N:1 multiplexed hub with per-port reply.
//!
//! [`Hub<In, Out, W>`] wires `N` producer ports to a single consumer
//! ("drain"), using [`SignalSet<W>`] as the multiplexor and per-port
//! [`Pipe<Out, _, W>`] for replies. Generic over the [`Waiter`] backend;
//! defaults to `ParkWaiter` (OS thread `park`/`unpark`) for backward
//! compatibility.
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
//! ## Limits
//!
//! - `N ≤ 63` user ports (bit 63 of the coordinator is reserved for
//!   [`HubShutdown`] wake-ups).
//! - `N = 0` is rejected.

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::Arc;

use crate::gate::{SignalId, SignalSet, MAX_GATES};
use crate::slot::{NoHook, Pipe};
use crate::waiter::{BlockingWaiter, ParkWaiter, Waiter};

/// Index of the reserved shutdown bit in the coordinator `SignalSet`.
const SHUTDOWN_BIT: u8 = (MAX_GATES - 1) as u8;

/// Maximum number of user ports in a [`Hub`]. One bit is reserved for
/// [`HubShutdown`], so this is `MAX_GATES - 1 = 63`.
pub const MAX_HUB_PORTS: usize = MAX_GATES - 1;

/// Returned by blocking drain ops when a [`HubShutdown::signal`] has
/// been issued. Callers should break out of their drain loop.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Shutdown;

/// Per-port inbound slot, padded to a full cache line.
#[repr(C, align(64))]
struct InboundSlot<In>(UnsafeCell<MaybeUninit<In>>);

/// Shared state of a hub. Users interact via [`HubPort`] / [`HubDrain`].
#[repr(C)]
pub struct Hub<In, Out, W: Waiter = ParkWaiter> {
    coordinator: SignalSet<W>,
    ids: Vec<SignalId>,
    inbound: Vec<InboundSlot<In>>,
    /// Per-port outbound: full `Pipe<Out, _, W>` so the port thread may
    /// park (or await) waiting for its reply on the same waiter backend.
    outbound: Vec<Pipe<Out, NoHook, W>>,
    full_mask: u64,
    shutdown_id: SignalId,
}

unsafe impl<In: Send, Out: Send, W: Waiter> Sync for Hub<In, Out, W> {}
unsafe impl<In: Send, Out: Send, W: Waiter> Send for Hub<In, Out, W> {}

impl<In: Send, Out: Send, W: Waiter> Hub<In, Out, W> {
    /// Build a hub with `n` ports.
    ///
    /// # Panics
    /// - `n == 0`
    /// - `n > MAX_HUB_PORTS` (63)
    pub fn new(n: usize) -> (HubDrain<In, Out, W>, Vec<HubPort<In, Out, W>>) {
        assert!(n > 0, "Hub::new: n must be > 0");
        assert!(n <= MAX_HUB_PORTS, "Hub::new: n must be <= {MAX_HUB_PORTS}");

        let mut coordinator = SignalSet::<W>::new();
        let mut ids = Vec::with_capacity(n);
        for i in 0..n {
            let name: &'static str = Box::leak(format!("hub_port_{i}").into_boxed_str());
            ids.push(coordinator.create(name));
        }
        let shutdown_id = SignalId::new(SHUTDOWN_BIT);

        let full_mask: u64 = ids.iter().map(|id| id.mask()).fold(0, |a, b| a | b);

        let mut inbound = Vec::with_capacity(n);
        let mut outbound = Vec::with_capacity(n);
        for _ in 0..n {
            inbound.push(InboundSlot(UnsafeCell::new(MaybeUninit::uninit())));
            outbound.push(Pipe::<Out, NoHook, W>::new());
        }

        let hub = Arc::new(Hub {
            coordinator,
            ids,
            inbound,
            outbound,
            full_mask,
            shutdown_id,
        });

        let ports: Vec<HubPort<In, Out, W>> = (0..n)
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

    #[inline]
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    #[inline]
    pub fn full_mask(&self) -> u64 {
        self.full_mask
    }
}

impl<In, Out, W: Waiter> Drop for Hub<In, Out, W> {
    fn drop(&mut self) {
        let state = self.coordinator.state();
        for (i, _) in self.inbound.iter().enumerate() {
            let bit = 1u64 << i;
            if state & bit != 0 {
                unsafe {
                    (*self.inbound[i].0.get()).assume_init_drop();
                }
            }
        }
    }
}

// ─── Port ──────────────────────────────────────────────────────────────────

pub struct HubPort<In: Send, Out: Send, W: Waiter = ParkWaiter> {
    hub: Arc<Hub<In, Out, W>>,
    idx: usize,
    id: SignalId,
    _not_sync: PhantomData<Cell<()>>,
}

impl<In: Send, Out: Send, W: Waiter> HubPort<In, Out, W> {
    #[inline]
    pub fn index(&self) -> usize {
        self.idx
    }

    /// Register this thread as the port's reply consumer. No-op for async
    /// waiter backends.
    #[inline]
    pub fn bind(&self) {
        self.hub.outbound[self.idx].set_consumer(std::thread::current());
    }

    #[inline]
    pub fn is_idle(&self) -> bool {
        !self.hub.coordinator.is_open(self.id)
    }

    /// Send `v` to the drain.
    #[inline]
    pub fn send(&self, v: In) {
        debug_assert!(
            self.is_idle(),
            "HubPort::send called on busy port {}: caller must drain reply first",
            self.idx
        );
        unsafe {
            (*self.hub.inbound[self.idx].0.get()).write(v);
        }
        self.hub.coordinator.release(self.id);
    }

    /// Non-blocking send. Returns `Err(v)` if the port is still busy.
    #[inline]
    pub fn try_send(&self, v: In) -> Result<(), In> {
        if !self.is_idle() {
            return Err(v);
        }
        unsafe {
            (*self.hub.inbound[self.idx].0.get()).write(v);
        }
        self.hub.coordinator.release(self.id);
        Ok(())
    }

    /// Non-blocking reply take.
    #[inline]
    pub fn try_recv_reply(&self) -> Option<Out> {
        self.hub.outbound[self.idx].try_recv()
    }
}

impl<In: Send, Out: Send, W: BlockingWaiter> HubPort<In, Out, W> {
    /// Block until the drain replies, then take the `Out`.
    #[inline]
    pub fn recv_reply(&self) -> Out {
        self.hub.outbound[self.idx].recv()
    }

    /// Convenience: send and block for the reply.
    #[inline]
    pub fn call(&self, v: In) -> Out {
        self.send(v);
        self.recv_reply()
    }
}

// ─── Drain ─────────────────────────────────────────────────────────────────

pub struct HubDrain<In: Send, Out: Send, W: Waiter = ParkWaiter> {
    hub: Arc<Hub<In, Out, W>>,
    cursor: Cell<u32>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<In: Send, Out: Send, W: Waiter> HubDrain<In, Out, W> {
    /// Register this thread as the drain. No-op for async waiter backends.
    #[inline]
    pub fn bind(&self) {
        self.hub.coordinator.set_worker(std::thread::current());
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.hub.len()
    }

    #[inline]
    pub fn shutdown_handle(&self) -> HubShutdown<In, Out, W> {
        HubShutdown {
            hub: self.hub.clone(),
        }
    }

    /// Internal: drain whatever's currently pending without blocking.
    fn drain_available<F: FnMut(usize, In, HubReply<'_, Out, W>)>(&self, f: &mut F) {
        let state = self.hub.coordinator.state();
        let mut active = state & self.hub.full_mask;
        if active == 0 {
            return;
        }
        while active != 0 {
            let i = active.trailing_zeros() as usize;
            let bit = 1u64 << i;
            active &= active.wrapping_sub(1);
            let v = unsafe { (*self.hub.inbound[i].0.get()).assume_init_read() };
            self.hub.coordinator.lock_mask(bit);
            let reply = HubReply {
                pipe: &self.hub.outbound[i],
            };
            f(i, v, reply);
        }
    }

    /// Non-blocking peek-and-drain.
    pub fn try_recv_batch<F: FnMut(usize, In, HubReply<'_, Out, W>)>(&self, mut f: F) -> bool {
        let state = self.hub.coordinator.state();
        let active = state & self.hub.full_mask;
        if active == 0 {
            return false;
        }
        let n = self.hub.ids.len();
        let start = self.cursor.get() as usize % n;
        let split = if start == 0 {
            0u64
        } else {
            (1u64 << start) - 1
        };
        let high = active & !split;
        let low = active & split;
        for mut m in [high, low] {
            while m != 0 {
                let i = m.trailing_zeros() as usize;
                let bit = 1u64 << i;
                m &= m.wrapping_sub(1);
                let v = unsafe { (*self.hub.inbound[i].0.get()).assume_init_read() };
                self.hub.coordinator.lock_mask(bit);
                let reply = HubReply {
                    pipe: &self.hub.outbound[i],
                };
                f(i, v, reply);
            }
        }
        self.cursor.set(((start + 1) % n) as u32);
        true
    }
}

impl<In: Send, Out: Send, W: BlockingWaiter> HubDrain<In, Out, W> {
    /// Block until at least one port has a pending message.
    pub fn recv_batch<F: FnMut(usize, In, HubReply<'_, Out, W>)>(
        &self,
        mut f: F,
    ) -> Result<(), Shutdown> {
        let n = self.hub.ids.len();
        let shutdown_mask = self.hub.shutdown_id.mask();
        let wake_mask = self.hub.full_mask | shutdown_mask;
        self.hub.coordinator.acquire_any(wake_mask);

        let state = self.hub.coordinator.state();
        if state & shutdown_mask != 0 {
            self.drain_available(&mut f);
            return Err(Shutdown);
        }

        let start = self.cursor.get() as usize % n;
        let active = state & self.hub.full_mask;
        let split = if start == 0 {
            0u64
        } else {
            (1u64 << start) - 1
        };
        let high = active & !split;
        let low = active & split;
        for mut m in [high, low] {
            while m != 0 {
                let i = m.trailing_zeros() as usize;
                let bit = 1u64 << i;
                m &= m.wrapping_sub(1);
                let v = unsafe { (*self.hub.inbound[i].0.get()).assume_init_read() };
                self.hub.coordinator.lock_mask(bit);
                let reply = HubReply {
                    pipe: &self.hub.outbound[i],
                };
                f(i, v, reply);
            }
        }
        self.cursor.set(((start + 1) % n) as u32);
        Ok(())
    }
}

// ─── Shutdown handle ───────────────────────────────────────────────────────

pub struct HubShutdown<In: Send, Out: Send, W: Waiter = ParkWaiter> {
    hub: Arc<Hub<In, Out, W>>,
}

impl<In: Send, Out: Send, W: Waiter> Clone for HubShutdown<In, Out, W> {
    fn clone(&self) -> Self {
        Self {
            hub: self.hub.clone(),
        }
    }
}

impl<In: Send, Out: Send, W: Waiter> HubShutdown<In, Out, W> {
    #[inline]
    pub fn signal(&self) {
        self.hub.coordinator.release(self.hub.shutdown_id);
    }

    #[inline]
    pub fn is_signaled(&self) -> bool {
        self.hub.coordinator.is_open(self.hub.shutdown_id)
    }
}

// ─── Reply handle ──────────────────────────────────────────────────────────

/// Reply capability handed to the drain's callback.
pub struct HubReply<'a, Out: Send, W: Waiter = ParkWaiter> {
    pipe: &'a Pipe<Out, NoHook, W>,
}

impl<'a, Out: Send, W: Waiter> HubReply<'a, Out, W> {
    /// Send the reply back to the originating port.
    #[inline]
    pub fn send(self, v: Out) {
        self.pipe.send(v);
    }

    /// Access the underlying outbound pipe for advanced composition.
    #[inline]
    pub fn pipe(&self) -> &Pipe<Out, NoHook, W> {
        self.pipe
    }
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
            drain
                .recv_batch(|idx, msg, reply| {
                    assert_eq!(idx, 0);
                    reply.send(msg * 2);
                })
                .unwrap();
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

        let handles: Vec<_> = ports
            .into_iter()
            .enumerate()
            .map(|(i, p)| {
                thread::spawn(move || {
                    p.bind();
                    for k in 0..50u64 {
                        let r = p.call(k);
                        assert_eq!(r, k + i as u64 * 1000);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        shutdown.signal();
        d.join().unwrap();
    }

    #[test]
    fn fair_round_robin() {
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
            for k in 0..500u64 {
                p0.call(k);
            }
        });
        let h1 = thread::spawn(move || {
            p1.bind();
            for k in 0..500u64 {
                p1.call(k);
            }
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
            drain.recv_batch(|_, _, _| unreachable!("no port fired"))
        });

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
        assert_eq!(received.load(Ordering::Relaxed), 99);
    }

    #[test]
    fn try_send_returns_err_when_busy() {
        let (_drain, mut ports) = Hub::<u64, u64>::new(1);
        let p = ports.remove(0);
        assert!(p.try_send(1).is_ok());
        assert_eq!(p.try_send(2), Err(2));
    }

    #[test]
    fn drop_drains_inflight_inbound() {
        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let (_drain, mut ports) = Hub::<Tracked, ()>::new(2);
            let p0 = ports.remove(0);
            p0.try_send(Tracked(drops.clone())).ok().unwrap();
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
            drain
                .recv_batch(|_, mut msg, reply| {
                    for b in msg.iter_mut() {
                        *b = b.wrapping_add(1);
                    }
                    reply.send(msg);
                })
                .unwrap();
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
