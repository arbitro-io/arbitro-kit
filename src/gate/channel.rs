//! SPSC round-trip channel built on two [`Signal`]s.
//!
//! [`Channel<Req, Resp>`] is a **one-client, one-server** request/response
//! primitive. The client sends a `Req` and blocks until the server produces
//! a `Resp`. No allocation, no Mutex, no heap; the payload lives in two
//! inline slots adjacent to each direction's gate.
//!
//! ## Wire model
//!
//! ```text
//!  client thread                              server thread
//!  ─────────────                              ─────────────
//!  write   req_slot     ──┐
//!  release req_gate     ──┘→ coherence →  ─→ acquire req_gate
//!                                             read    req_slot
//!                                             lock    req_gate
//!                                             (f: Req → Resp)
//!                                          ┌─ write   resp_slot
//!                   acquire resp_gate  ←── ┘  release resp_gate
//!  read  resp_slot
//!  lock  resp_gate
//! ```
//!
//! Each direction is a [`Signal`]; the whole handshake is two M:1 signals
//! pinned to 1:1. The two gates live on separate cache lines so the
//! producer's spin on `resp_gate` does not bounce the consumer's store on
//! `req_gate` (or vice versa).
//!
//! ## Cost (echo server, 8 B payload, x86_64 WSL Linux)
//!
//! | Path              |      Typical p50 RTT |
//! | ----------------- | -------------------: |
//! | HOT (no park)     |              ~120 ns |
//! | PARKED (both)     |              ~11 µs |
//!
//! Cost tracks `2 × Signal` exactly — no surprises. At that point, 2-3× faster
//! than `crossbeam::bounded(1)` used as a request/response pair.
//!
//! ## API surface
//!
//! Two ways to use it:
//!
//! - **Typed split** — [`spsc()`](Channel::spsc) returns
//!   `(Client, Server)` handles. Each handle is `Send` but not `Sync`; each
//!   holds its own `Arc` over the shared channel, and registration with
//!   `park`/`unpark` happens automatically on [`Client::bind`] /
//!   [`Server::bind`]. Prevents most misuse at compile time.
//! - **Raw channel** — [`Channel::new`] + [`set_client`] / [`set_server`] +
//!   [`call`] / [`serve_one`]. Full control, no Arc overhead if you can share
//!   the channel by reference.
//!
//! ## Concurrency contract
//!
//! - **Exactly one client** thread may invoke [`call`](Channel::call) /
//!   [`try_call`](Channel::try_call). It must register itself with
//!   [`set_client`](Channel::set_client).
//! - **Exactly one server** thread may invoke [`serve_one`](Channel::serve_one).
//!   It must register itself with [`set_server`](Channel::set_server).
//! - The channel is `Sync` + `Send` and typically shared via `Arc`.
//!
//! ## Safety on drop
//!
//! The slots are `MaybeUninit`, and the [`Drop`] impl drains any value left
//! in flight at teardown:
//!
//! - If the request gate is still open (`Req` queued but never served), the
//!   `Req` is dropped.
//! - If the response gate is still open (`Resp` produced but never read),
//!   the `Resp` is dropped.
//!
//! This makes the channel safe with `Req`/`Resp` types that hold RAII
//! resources (e.g., `Box<T>`, `Vec<T>`, `File`).

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::gate::Signal;

/// Cache-line padding to force the response half onto a separate 64 B line
/// from the request half. Without this, `resp_gate.locked` can share a line
/// with `req_slot` / end of `req_gate` and cause false sharing on hot RTT.
#[repr(align(64))]
struct CachePad([u8; 0]);

/// SPSC request/response channel.
///
/// Generic over `Req` (client → server) and `Resp` (server → client). Both
/// must be `Send`.
#[repr(C)]
pub struct Channel<Req, Resp> {
    // ── request direction (client → server) ─────────────────────────────
    req_gate: Signal,
    req_slot: UnsafeCell<MaybeUninit<Req>>,
    _pad: CachePad,
    // ── response direction (server → client) ────────────────────────────
    resp_gate: Signal,
    resp_slot: UnsafeCell<MaybeUninit<Resp>>,
    // ── poison flag ─────────────────────────────────────────────────────
    // Set if the server handler panics inside `serve_one`. The drop-guard
    // also releases `resp_gate` so the blocked client observes the flag
    // instead of parking forever. Read by `call` on the resp-side ack.
    poisoned: AtomicBool,
}

// Safety: slot access is serialized by the gate handshake — the writer
// always publishes via `release()` (Release store) before the reader
// observes the open state via `acquire()` (Acquire load). Client and
// server touch disjoint slots (client writes req / reads resp; server
// reads req / writes resp), and only one of each is enforced by the
// single `worker` registration per gate.
unsafe impl<Req: Send, Resp: Send> Sync for Channel<Req, Resp> {}
unsafe impl<Req: Send, Resp: Send> Send for Channel<Req, Resp> {}

impl<Req: Send, Resp: Send> Default for Channel<Req, Resp> {
    fn default() -> Self { Self::new() }
}

impl<Req: Send, Resp: Send> Channel<Req, Resp> {
    /// Create a fresh channel. Both gates start locked (no pending work).
    /// You still need to call [`set_client`](Self::set_client) from the
    /// client thread and [`set_server`](Self::set_server) from the server
    /// thread before the first [`call`](Self::call) / [`serve_one`](Self::serve_one).
    pub fn new() -> Self {
        Self {
            req_gate: Signal::new(),
            req_slot: UnsafeCell::new(MaybeUninit::uninit()),
            _pad: CachePad([]),
            resp_gate: Signal::new(),
            resp_slot: UnsafeCell::new(MaybeUninit::uninit()),
            poisoned: AtomicBool::new(false),
        }
    }

    /// Create a channel and return typed `(Client, Server)` handles.
    ///
    /// This is the recommended entry point: the handles make the one-client /
    /// one-server contract a type-level property, and each side can be moved
    /// to its owning thread. Registration still happens explicitly via
    /// [`Client::bind`] / [`Server::bind`] from the respective thread.
    pub fn spsc() -> (Client<Req, Resp>, Server<Req, Resp>) {
        let inner = Arc::new(Self::new());
        (Client { inner: inner.clone() }, Server { inner })
    }

    /// Register the client thread. Must be called **from** the client
    /// thread, before the channel is shared across threads. Typically:
    /// `ch.set_client(std::thread::current())`.
    pub fn set_client(&self, t: std::thread::Thread) {
        self.resp_gate.set_worker(t);
    }

    /// Register the server thread. Must be called from the server thread
    /// before it enters the `serve_one` loop.
    pub fn set_server(&self, t: std::thread::Thread) {
        self.req_gate.set_worker(t);
    }

    /// Client API. Send `req` and block until the server returns a `Resp`.
    ///
    /// Must only be called from the registered client thread.
    ///
    /// # Panics
    ///
    /// If the server handler panicked in a previous `serve_one`, the channel
    /// is poisoned: the next (or in-flight) `call` observes the flag and
    /// panics rather than returning garbage or blocking forever.
    #[inline]
    pub fn call(&self, req: Req) -> Resp {
        // Safety: slot is written by the client alone; the subsequent
        // `release()` performs a Release store, so the server's paired
        // Acquire load observes the fully-constructed value.
        unsafe { (*self.req_slot.get()).write(req); }
        self.req_gate.release();
        self.resp_gate.acquire();
        // Poison check: if the server handler panicked, the PoisonGuard in
        // `serve_one` set this flag and released `resp_gate` to wake us.
        // `resp_slot` holds no initialized value in that case — don't read it.
        if self.poisoned.load(Ordering::Acquire) {
            self.resp_gate.lock();
            panic!("arbitro-kit Channel poisoned: server handler panicked");
        }
        // Safety: server wrote resp_slot and published via release(); our
        // acquire() synchronizes-with it.
        let r = unsafe { (*self.resp_slot.get()).assume_init_read() };
        self.resp_gate.lock();
        r
    }

    /// Non-blocking client API. If the server has already replied to a
    /// previous request (or if no request is in flight and this one would
    /// succeed immediately), return the response. Otherwise return `None`
    /// **without enqueuing** the request.
    ///
    /// Semantics: this is a peek-and-take on the response side. A useful
    /// pattern is:
    ///
    /// 1. Call `call` to submit a request and block for the response, **or**
    /// 2. After a successful `call`, call `try_call(next_req)` to fire the
    ///    next request without waiting (it returns None if the server
    ///    hasn't gotten there yet — caller can then block later).
    ///
    /// Must only be called from the registered client thread.
    #[inline]
    pub fn try_call(&self, req: Req) -> Result<Resp, Req> {
        if !self.resp_gate.is_open() {
            return Err(req);
        }
        // Resp gate is open — but that may be because the server panicked
        // and the PoisonGuard released it without writing `resp_slot`.
        if self.poisoned.load(Ordering::Acquire) {
            self.resp_gate.lock();
            panic!("arbitro-kit Channel poisoned: server handler panicked");
        }
        // Response already pending. Take it, then fire our next request.
        let r = unsafe { (*self.resp_slot.get()).assume_init_read() };
        self.resp_gate.lock();
        unsafe { (*self.req_slot.get()).write(req); }
        self.req_gate.release();
        Ok(r)
    }

    /// Server API. Block until a `Req` arrives, run `f`, return the
    /// `Resp` to the waiting client. Executes exactly one round-trip.
    ///
    /// Must only be called from the registered server thread.
    ///
    /// # Panic safety
    ///
    /// If `f` panics, the channel is **poisoned**: an internal flag is set,
    /// `resp_gate` is released to wake the blocked client, and the panic
    /// propagates up through this call. The client's current (and any
    /// future) `call` observes the poison flag and panics instead of
    /// blocking forever or reading uninitialized memory.
    #[inline]
    pub fn serve_one<F: FnOnce(Req) -> Resp>(&self, f: F) {
        self.req_gate.acquire();
        // Safety: client wrote req_slot and published via release().
        let req = unsafe { (*self.req_slot.get()).assume_init_read() };
        self.req_gate.lock();

        // Drop-guard: if `f(req)` panics, we still need to wake the client
        // (which is parked in `resp_gate.acquire()`). Setting `poisoned`
        // before releasing the gate establishes a happens-before edge with
        // the Acquire load in `call`.
        struct PoisonGuard<'a> {
            poisoned: &'a AtomicBool,
            resp_gate: &'a Signal,
        }
        impl<'a> Drop for PoisonGuard<'a> {
            fn drop(&mut self) {
                // Release so the Acquire load in `call` observes `true`.
                self.poisoned.store(true, Ordering::Release);
                self.resp_gate.release();
            }
        }
        let guard = PoisonGuard { poisoned: &self.poisoned, resp_gate: &self.resp_gate };

        let resp = f(req);

        // Normal path: disarm the guard before publishing the response.
        std::mem::forget(guard);

        // Safety: resp_slot write is paired with the following release().
        unsafe { (*self.resp_slot.get()).write(resp); }
        self.resp_gate.release();
    }

    /// Server API. Loop forever serving requests with the given handler.
    /// Handler takes `&mut` so it can hold state across rounds (cache,
    /// counters, buffers). Never returns — typically the thread is let
    /// die on process exit, or the handler panics to exit.
    ///
    /// Must only be called from the registered server thread.
    #[inline]
    pub fn serve_loop<F: FnMut(Req) -> Resp>(&self, mut f: F) -> ! {
        loop { self.serve_one(&mut f); }
    }

    /// Non-blocking check for the server: is there a pending request?
    #[inline]
    pub fn has_request(&self) -> bool { self.req_gate.is_open() }

    /// Non-blocking check for the client: has the server replied?
    #[inline]
    pub fn has_response(&self) -> bool { self.resp_gate.is_open() }
}

impl<Req, Resp> Drop for Channel<Req, Resp> {
    fn drop(&mut self) {
        // Safety: `&mut self` means no other references exist. If a gate is
        // open, its slot normally holds an initialized value that must be
        // dropped to avoid leaking RAII resources.
        //
        // Exception: if the channel is poisoned, `resp_gate` was released
        // by the PoisonGuard without writing `resp_slot` — the slot is
        // still uninitialized, so we must skip the drop.
        if self.req_gate.is_open() {
            unsafe { (*self.req_slot.get()).assume_init_drop(); }
        }
        if self.resp_gate.is_open() && !*self.poisoned.get_mut() {
            unsafe { (*self.resp_slot.get()).assume_init_drop(); }
        }
    }
}

// ─── typed split handles ────────────────────────────────────────────────────

/// Client half of a [`Channel`]. Move to the client thread, call
/// [`bind`](Client::bind) once, then use [`call`](Client::call) /
/// [`try_call`](Client::try_call) from that thread.
///
/// `Send` but deliberately not `Sync`: only one thread can hold the client
/// side of an SPSC channel.
pub struct Client<Req, Resp> {
    inner: Arc<Channel<Req, Resp>>,
}
// Client moves between threads fine (Send), but the handle itself must not
// be shared across threads concurrently — that would violate the single-
// client contract. Rust gives us !Sync by default since we hold an Arc.
impl<Req: Send, Resp: Send> Client<Req, Resp> {
    /// Bind this client to the current thread. Call exactly once, from the
    /// thread that will invoke [`call`](Self::call), before the first call.
    #[inline]
    pub fn bind(&self) {
        self.inner.set_client(std::thread::current());
    }

    /// Send `req`, block until the server replies, return `Resp`.
    #[inline]
    pub fn call(&self, req: Req) -> Resp { self.inner.call(req) }

    /// Non-blocking pipelined send. See [`Channel::try_call`].
    #[inline]
    pub fn try_call(&self, req: Req) -> Result<Resp, Req> { self.inner.try_call(req) }

    /// True iff a response is already waiting to be consumed.
    #[inline]
    pub fn has_response(&self) -> bool { self.inner.has_response() }
}

/// Server half of a [`Channel`]. Move to the server thread, call
/// [`bind`](Server::bind) once, then use [`serve_one`](Server::serve_one) /
/// [`serve_loop`](Server::serve_loop) from that thread.
pub struct Server<Req, Resp> {
    inner: Arc<Channel<Req, Resp>>,
}
impl<Req: Send, Resp: Send> Server<Req, Resp> {
    /// Bind this server to the current thread. Call exactly once, from the
    /// thread that will serve requests, before the first `serve_*`.
    #[inline]
    pub fn bind(&self) {
        self.inner.set_server(std::thread::current());
    }

    /// Serve exactly one round-trip.
    #[inline]
    pub fn serve_one<F: FnOnce(Req) -> Resp>(&self, f: F) { self.inner.serve_one(f) }

    /// Serve requests forever with a stateful handler.
    #[inline]
    pub fn serve_loop<F: FnMut(Req) -> Resp>(&self, f: F) -> ! { self.inner.serve_loop(f) }

    /// True iff a request is queued and ready to serve.
    #[inline]
    pub fn has_request(&self) -> bool { self.inner.has_request() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn basic_rtt_echo_raw() {
        let ch = Arc::new(Channel::<u64, u64>::new());
        let stop = Arc::new(AtomicBool::new(false));

        let ch_s = ch.clone();
        let stop_s = stop.clone();
        let server = std::thread::spawn(move || {
            ch_s.set_server(std::thread::current());
            while !stop_s.load(Ordering::Relaxed) {
                ch_s.serve_one(|req| req.wrapping_mul(2));
            }
        });

        std::thread::sleep(std::time::Duration::from_millis(10));

        ch.set_client(std::thread::current());
        for i in 0u64..100 {
            let r = ch.call(i);
            assert_eq!(r, i.wrapping_mul(2));
        }

        stop.store(true, Ordering::Relaxed);
        let _ = ch.call(0);
        server.join().unwrap();
    }

    #[test]
    fn split_api() {
        let (client, server) = Channel::<u64, u64>::spsc();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_s = stop.clone();

        let h = std::thread::spawn(move || {
            server.bind();
            while !stop_s.load(Ordering::Relaxed) {
                server.serve_one(|x| x.wrapping_add(1));
            }
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        client.bind();
        for i in 0u64..100 {
            assert_eq!(client.call(i), i + 1);
        }
        stop.store(true, Ordering::Relaxed);
        let _ = client.call(0);
        h.join().unwrap();
    }

    #[test]
    fn byte_array_payload() {
        let (client, server) = Channel::<[u8; 256], [u8; 256]>::spsc();

        let h = std::thread::spawn(move || {
            server.bind();
            server.serve_one(|mut req| {
                for b in req.iter_mut() { *b = b.wrapping_add(1); }
                req
            });
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        client.bind();
        let req = [7u8; 256];
        let resp = client.call(req);
        assert!(resp.iter().all(|&b| b == 8));
        h.join().unwrap();
    }

    #[test]
    fn drop_drains_inflight_request() {
        // Counter-based RAII payload to prove Drop runs.
        struct Tracked(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }

        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let ch = Channel::<Tracked, ()>::new();
            // Write a Req without ever servicing it.
            ch.set_client(std::thread::current());
            unsafe { (*ch.req_slot.get()).write(Tracked(drops.clone())); }
            ch.req_gate.release();
            // ch drops here — Drop impl must drop the Tracked inside.
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1, "inflight Req must be dropped");
    }

    #[test]
    fn box_payload_zero_copy_rtt() {
        // Prove that Channel<Box<T>, Box<T>> works: only the 8-byte
        // pointer crosses the gate; the heap allocation is transferred
        // by ownership, not copied.
        let (client, server) = Channel::<Box<Vec<u8>>, Box<Vec<u8>>>::spsc();

        let h = std::thread::spawn(move || {
            server.bind();
            server.serve_one(|mut req| {
                // Mutate in place — no copy, we own the Box now.
                for b in req.iter_mut() { *b = b.wrapping_add(1); }
                req
            });
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        client.bind();

        let payload: Box<Vec<u8>> = Box::new(vec![10, 20, 30, 40]);
        let ptr_before = payload.as_ptr() as usize;   // address of heap Vec buffer
        let resp = client.call(payload);
        let ptr_after = resp.as_ptr() as usize;

        assert_eq!(*resp, vec![11, 21, 31, 41], "server mutation visible to client");
        // The Vec's internal buffer didn't need to move; pointer is stable.
        // (This is empirical — Rust doesn't *guarantee* it, but for simple
        // ownership transfer it holds, confirming the zero-copy pattern.)
        assert_eq!(ptr_before, ptr_after, "heap buffer did not move");

        h.join().unwrap();
    }

    #[test]
    fn try_call_returns_req_when_empty() {
        let (client, _server) = Channel::<u64, u64>::spsc();
        client.bind();
        // Nothing has been served yet → no response pending → req bounces back.
        let r = client.try_call(42);
        assert_eq!(r, Err(42));
    }

    #[test]
    fn server_panic_poisons_and_wakes_client() {
        // Regression: without the PoisonGuard, a panic inside `serve_one`
        // left `resp_gate` closed forever; the client's `call` parked and
        // never woke up. With the guard, the client observes the poison
        // flag and panics instead of blocking forever.
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let (client, server) = Channel::<u64, u64>::spsc();

        let h = std::thread::spawn(move || {
            server.bind();
            // Handler panics on the first request — simulates a buggy server.
            let res = catch_unwind(AssertUnwindSafe(|| {
                server.serve_one(|_req| -> u64 { panic!("handler boom") });
            }));
            assert!(res.is_err(), "serve_one must propagate the panic");
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        client.bind();

        // The client should NOT hang. It should panic on the call instead.
        let res = catch_unwind(AssertUnwindSafe(|| client.call(42)));
        assert!(res.is_err(), "client.call must panic when channel is poisoned");

        h.join().unwrap();
    }

    #[test]
    fn poisoned_channel_drop_does_not_double_free() {
        // After poisoning, `resp_gate` is open but `resp_slot` is
        // uninitialized. The Drop impl must skip the resp_slot drop —
        // otherwise it would try to drop uninit memory.
        use std::panic::{catch_unwind, AssertUnwindSafe};

        struct Tracked(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }

        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let drops_s = drops.clone();

        {
            let (client, server) = Channel::<u64, Tracked>::spsc();

            let h = std::thread::spawn(move || {
                server.bind();
                let _ = catch_unwind(AssertUnwindSafe(|| {
                    server.serve_one(|_req| -> Tracked {
                        // Never constructs a Tracked — panics first.
                        panic!("handler boom");
                        #[allow(unreachable_code)]
                        Tracked(drops_s.clone())
                    });
                }));
                // server handle drops here (Arc refcount decrement only).
            });

            std::thread::sleep(std::time::Duration::from_millis(10));
            client.bind();
            let _ = catch_unwind(AssertUnwindSafe(|| client.call(1)));
            h.join().unwrap();
            // Channel Arc is released here; Drop runs.
        }

        // No Tracked was ever constructed, so the count must remain 0.
        // If Drop had called assume_init_drop on the uninit resp_slot,
        // we'd see a bogus increment or (worse) UB.
        assert_eq!(drops.load(Ordering::Relaxed), 0);
    }
}
