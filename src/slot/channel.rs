//! SPSC round-trip channel — request/response over two [`Waiter`]s.
//!
//! [`Channel<Req, Resp, W>`] is a **one-client, one-server** RPC primitive.
//! The client sends a `Req` and blocks until the server produces a `Resp`.
//! No allocation, no Mutex, no heap; the payload lives in two inline slots
//! adjacent to each direction's gate.
//!
//! ## Wire model
//!
//! ```text
//!  client side                                 server side
//!  ─────────────                               ─────────────
//!  write    req_slot   ──┐
//!  req_open = true     ──┘ (Release)
//!  req_waiter.wake()     ────────────────►   req_waiter.wait_until(req_open)
//!                                             read    req_slot
//!                                             req_open = false (Relaxed)
//!                                             (f: Req → Resp)
//!                                          ┌─ write   resp_slot
//!                                          │  resp_open = true (Release)
//!  resp_waiter.wait_until(resp_open) ◄─────┘  resp_waiter.wake()
//!  read   resp_slot
//!  resp_open = false (Relaxed)
//! ```
//!
//! Each direction is an `(AtomicBool, W, slot)` triple — the same primitive
//! pattern as [`Pipe`](super::Pipe), but composed in pairs to provide
//! request/response semantics. The two halves live on separate cache lines.
//!
//! ## Runtime — pick at the type level
//!
//! - `Channel<Req, Resp>` (default `W = ParkWaiter`) — sync, OS-thread,
//!   `call()` / `serve_one()` block.
//! - `Channel<Req, Resp, NotifyWaiter>` (feature `tokio`) — async,
//!   `call_async().await` / `serve_one_async().await`.
//! - Future runtimes: write one new `Waiter` impl and `Channel<Req, Resp,
//!   MyWaiter>` works automatically.
//!
//! ## API surface
//!
//! Two ways to use it:
//!
//! - **Typed split** — [`spsc()`](Channel::spsc) returns `(Client, Server)`
//!   handles. Each is `Send` but not `Sync`; each holds its own `Arc`.
//! - **Raw channel** — [`Channel::new`] + [`set_client`] / [`set_server`] +
//!   [`call`] / [`serve_one`]. Full control, no Arc overhead.
//!
//! ## Concurrency contract
//!
//! - **Exactly one client** invokes [`call`](Channel::call) /
//!   [`try_call`](Channel::try_call). Sync waiters require
//!   [`set_client`](Channel::set_client) first.
//! - **Exactly one server** invokes [`serve_one`](Channel::serve_one).
//!   Sync waiters require [`set_server`](Channel::set_server) first.
//! - Channel is `Sync + Send` and typically shared via `Arc`.
//!
//! ## Safety on drop
//!
//! Slots are `MaybeUninit`; `Drop` drains any value in flight at teardown:
//!
//! - If `req_open` is set (request queued, never served), the `Req` is dropped.
//! - If `resp_open` is set AND the channel is not poisoned (server actually
//!   wrote `resp_slot`), the `Resp` is dropped.
//! - Poisoned channel ⇒ `resp_slot` is uninitialised; drop skips it.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::waiter::{ParkWaiter, Waiter, BlockingWaiter};
#[cfg(feature = "tokio")]
use crate::waiter::AsyncWaiter;

/// Cache-line padding to force the response half onto a separate 64 B line
/// from the request half.
#[repr(align(64))]
struct CachePad([u8; 0]);

/// SPSC request/response channel, generic over the waiter backend.
///
/// ## Layout
///
/// `#[repr(C, align(64))]` forces the whole struct onto a 64-byte boundary.
/// `_pad0`/`_pad1` are align-64 ZSTs that push the request and response
/// halves onto distinct cache lines, so the producer's wake on `resp_*` and
/// the consumer's wake on `req_*` never bounce a shared line.
#[repr(C, align(64))]
pub struct Channel<Req, Resp, W: Waiter = ParkWaiter> {
    // ── request direction (client → server) ─────────────────────────────
    _pad0:      CachePad,
    req_open:   AtomicBool,
    req_slot:   UnsafeCell<MaybeUninit<Req>>,
    req_waiter: W,

    // ── response direction (server → client) ────────────────────────────
    _pad1:       CachePad,
    resp_open:   AtomicBool,
    resp_slot:   UnsafeCell<MaybeUninit<Resp>>,
    resp_waiter: W,

    // ── poison flag ─────────────────────────────────────────────────────
    // Set if the server handler panics inside `serve_one`. The drop-guard
    // also opens `resp_open` so the blocked client observes the flag
    // instead of parking forever.
    poisoned: AtomicBool,
}

// Safety: slot access is serialised by the open flags — the writer publishes
// via Release before the reader observes via Acquire. Client and server
// touch disjoint slots; SPSC contract enforces single client / single server.
unsafe impl<Req: Send, Resp: Send, W: Waiter> Sync for Channel<Req, Resp, W> {}
unsafe impl<Req: Send, Resp: Send, W: Waiter> Send for Channel<Req, Resp, W> {}

impl<Req: Send, Resp: Send, W: Waiter> Default for Channel<Req, Resp, W> {
    fn default() -> Self { Self::new() }
}

impl<Req: Send, Resp: Send, W: Waiter> Channel<Req, Resp, W> {
    /// Create a fresh channel. Both halves start closed (no pending work).
    pub fn new() -> Self {
        Self {
            _pad0:       CachePad([]),
            req_open:    AtomicBool::new(false),
            req_slot:    UnsafeCell::new(MaybeUninit::uninit()),
            req_waiter:  W::default(),
            _pad1:       CachePad([]),
            resp_open:   AtomicBool::new(false),
            resp_slot:   UnsafeCell::new(MaybeUninit::uninit()),
            resp_waiter: W::default(),
            poisoned:    AtomicBool::new(false),
        }
    }

    /// Create a channel and return typed `(Client, Server)` handles.
    pub fn spsc() -> (Client<Req, Resp, W>, Server<Req, Resp, W>) {
        let inner = Arc::new(Self::new());
        (Client { inner: inner.clone() }, Server { inner })
    }

    /// Register the client thread. Must be called from the client thread
    /// before the first [`call`](Self::call). No-op for async waiters.
    pub fn set_client(&self, t: std::thread::Thread) {
        self.resp_waiter.set_worker(t);
    }

    /// Register the server thread. Must be called from the server thread
    /// before the first [`serve_one`](Self::serve_one). No-op for async waiters.
    pub fn set_server(&self, t: std::thread::Thread) {
        self.req_waiter.set_worker(t);
    }

    /// Non-blocking check for the server: is there a pending request?
    #[inline]
    pub fn has_request(&self) -> bool { self.req_open.load(Ordering::Acquire) }

    /// Non-blocking check for the client: has the server replied?
    #[inline]
    pub fn has_response(&self) -> bool { self.resp_open.load(Ordering::Acquire) }
}

// ── Sync API: requires `W: BlockingWaiter` ──────────────────────────────────

impl<Req: Send, Resp: Send, W: BlockingWaiter> Channel<Req, Resp, W> {
    /// Client API. Send `req` and block until the server returns a `Resp`.
    ///
    /// Must only be called from the registered client thread.
    ///
    /// # Panics
    /// If the server handler panicked, the channel is poisoned: `call`
    /// observes the flag and panics rather than block forever or read
    /// uninit memory.
    #[inline]
    pub fn call(&self, req: Req) -> Resp {
        // Safety: SPSC contract — sole client, req_slot is empty.
        unsafe { (*self.req_slot.get()).write(req); }
        self.req_open.store(true, Ordering::Release);
        self.req_waiter.wake();

        self.resp_waiter.wait_until(|| self.resp_open.load(Ordering::Acquire));

        if self.poisoned.load(Ordering::Acquire) {
            // Reset the open flag so a re-attempt panics afresh, never reads
            // uninit memory.
            self.resp_open.store(false, Ordering::Relaxed);
            panic!("arbitro-kit Channel poisoned: server handler panicked");
        }
        // Safety: server wrote resp_slot before storing resp_open=true with
        // Release; our wait_until predicate did an Acquire load.
        let r = unsafe { (*self.resp_slot.get()).assume_init_read() };
        self.resp_open.store(false, Ordering::Relaxed);
        r
    }

    /// Server API. Block until a `Req` arrives, run `f`, return `Resp` to
    /// the waiting client. Executes exactly one round-trip.
    ///
    /// # Panic safety
    /// If `f` panics, the channel is poisoned: the flag is set and
    /// `resp_open` is released to wake the blocked client. The panic
    /// propagates up.
    #[inline]
    pub fn serve_one<F: FnOnce(Req) -> Resp>(&self, f: F) {
        self.req_waiter.wait_until(|| self.req_open.load(Ordering::Acquire));
        // Safety: client wrote req_slot before storing req_open=true with
        // Release; our wait_until predicate did an Acquire load.
        let req = unsafe { (*self.req_slot.get()).assume_init_read() };
        self.req_open.store(false, Ordering::Relaxed);

        // Drop-guard: if `f(req)` panics, wake the parked client.
        struct PoisonGuard<'a, W: Waiter> {
            poisoned:    &'a AtomicBool,
            resp_open:   &'a AtomicBool,
            resp_waiter: &'a W,
        }
        impl<'a, W: Waiter> Drop for PoisonGuard<'a, W> {
            fn drop(&mut self) {
                self.poisoned.store(true, Ordering::Release);
                self.resp_open.store(true, Ordering::Release);
                self.resp_waiter.wake();
            }
        }
        let guard = PoisonGuard {
            poisoned:    &self.poisoned,
            resp_open:   &self.resp_open,
            resp_waiter: &self.resp_waiter,
        };

        let resp = f(req);

        // Disarm before publishing the response on the normal path.
        std::mem::forget(guard);

        // Safety: resp_slot write paired with the following Release store.
        unsafe { (*self.resp_slot.get()).write(resp); }
        self.resp_open.store(true, Ordering::Release);
        self.resp_waiter.wake();
    }

    /// Non-blocking client API — peek-and-take on the response side. If a
    /// response is already pending, take it and fire `req` for the next
    /// round-trip. Otherwise return `Err(req)` without enqueuing.
    #[inline]
    pub fn try_call(&self, req: Req) -> Result<Resp, Req> {
        if !self.resp_open.load(Ordering::Acquire) {
            return Err(req);
        }
        if self.poisoned.load(Ordering::Acquire) {
            self.resp_open.store(false, Ordering::Relaxed);
            panic!("arbitro-kit Channel poisoned: server handler panicked");
        }
        // Safety: resp_open is true and not poisoned ⇒ resp_slot is initialised.
        let r = unsafe { (*self.resp_slot.get()).assume_init_read() };
        self.resp_open.store(false, Ordering::Relaxed);

        // Fire the next request.
        unsafe { (*self.req_slot.get()).write(req); }
        self.req_open.store(true, Ordering::Release);
        self.req_waiter.wake();
        Ok(r)
    }

    /// Server API. Loop forever serving requests with a stateful handler.
    #[inline]
    pub fn serve_loop<F: FnMut(Req) -> Resp>(&self, mut f: F) -> ! {
        loop { self.serve_one(&mut f); }
    }
}

// ── Async API: requires `W: AsyncWaiter` ───────────────────────────────────

#[cfg(feature = "tokio")]
impl<Req: Send, Resp: Send, W: AsyncWaiter> Channel<Req, Resp, W> {
    /// Async client. Naming mirrors `Pipe::recv_async` — Rust does not
    /// allow a sync and async method to share a name even when bounds are
    /// disjoint.
    pub async fn call_async(&self, req: Req) -> Resp {
        unsafe { (*self.req_slot.get()).write(req); }
        self.req_open.store(true, Ordering::Release);
        self.req_waiter.wake();

        // Borrow individual Sync fields, not `&self`, to keep the future
        // Send when the channel itself is shared via Arc.
        let resp_open = &self.resp_open;
        self.resp_waiter
            .wait_until(|| resp_open.load(Ordering::Acquire))
            .await;

        if self.poisoned.load(Ordering::Acquire) {
            self.resp_open.store(false, Ordering::Relaxed);
            panic!("arbitro-kit Channel poisoned: server handler panicked");
        }
        let r = unsafe { (*self.resp_slot.get()).assume_init_read() };
        self.resp_open.store(false, Ordering::Relaxed);
        r
    }

    /// Async server — one round-trip. The handler is sync (`FnOnce(Req) → Resp`)
    /// because the panic-safety contract relies on a synchronous unwind.
    pub async fn serve_one_async<F: FnOnce(Req) -> Resp>(&self, f: F) {
        let req_open = &self.req_open;
        self.req_waiter
            .wait_until(|| req_open.load(Ordering::Acquire))
            .await;
        let req = unsafe { (*self.req_slot.get()).assume_init_read() };
        self.req_open.store(false, Ordering::Relaxed);

        struct PoisonGuard<'a, W: Waiter> {
            poisoned:    &'a AtomicBool,
            resp_open:   &'a AtomicBool,
            resp_waiter: &'a W,
        }
        impl<'a, W: Waiter> Drop for PoisonGuard<'a, W> {
            fn drop(&mut self) {
                self.poisoned.store(true, Ordering::Release);
                self.resp_open.store(true, Ordering::Release);
                self.resp_waiter.wake();
            }
        }
        let guard = PoisonGuard {
            poisoned:    &self.poisoned,
            resp_open:   &self.resp_open,
            resp_waiter: &self.resp_waiter,
        };

        let resp = f(req);

        std::mem::forget(guard);

        unsafe { (*self.resp_slot.get()).write(resp); }
        self.resp_open.store(true, Ordering::Release);
        self.resp_waiter.wake();
    }
}

impl<Req, Resp, W: Waiter> Drop for Channel<Req, Resp, W> {
    fn drop(&mut self) {
        // Safety: `&mut self` ⇒ no other refs. Drain any value left in
        // flight to avoid leaking RAII resources.
        if *self.req_open.get_mut() {
            unsafe { (*self.req_slot.get()).assume_init_drop(); }
        }
        if *self.resp_open.get_mut() && !*self.poisoned.get_mut() {
            unsafe { (*self.resp_slot.get()).assume_init_drop(); }
        }
    }
}

// ─── typed split handles ────────────────────────────────────────────────────

/// Client half of a [`Channel`]. Move to the client thread/task, call
/// [`bind`](Client::bind) once (sync only), then use `call` / `try_call`.
pub struct Client<Req, Resp, W: Waiter = ParkWaiter> {
    inner: Arc<Channel<Req, Resp, W>>,
}

impl<Req: Send, Resp: Send, W: Waiter> Client<Req, Resp, W> {
    /// Bind this client to the current thread. Required for sync waiters.
    #[inline]
    pub fn bind(&self) {
        self.inner.set_client(std::thread::current());
    }

    /// True iff a response is already waiting to be consumed.
    #[inline]
    pub fn has_response(&self) -> bool { self.inner.has_response() }
}

impl<Req: Send, Resp: Send, W: BlockingWaiter> Client<Req, Resp, W> {
    /// Send `req`, block until the server replies, return `Resp`.
    #[inline]
    pub fn call(&self, req: Req) -> Resp { self.inner.call(req) }

    /// Non-blocking pipelined send. See [`Channel::try_call`].
    #[inline]
    pub fn try_call(&self, req: Req) -> Result<Resp, Req> { self.inner.try_call(req) }
}

#[cfg(feature = "tokio")]
impl<Req: Send, Resp: Send, W: AsyncWaiter> Client<Req, Resp, W> {
    /// Async send. See [`Channel::call_async`].
    pub async fn call_async(&self, req: Req) -> Resp { self.inner.call_async(req).await }
}

/// Server half of a [`Channel`].
pub struct Server<Req, Resp, W: Waiter = ParkWaiter> {
    inner: Arc<Channel<Req, Resp, W>>,
}

impl<Req: Send, Resp: Send, W: Waiter> Server<Req, Resp, W> {
    /// Bind this server to the current thread. Required for sync waiters.
    #[inline]
    pub fn bind(&self) {
        self.inner.set_server(std::thread::current());
    }

    /// True iff a request is queued.
    #[inline]
    pub fn has_request(&self) -> bool { self.inner.has_request() }
}

impl<Req: Send, Resp: Send, W: BlockingWaiter> Server<Req, Resp, W> {
    /// Serve exactly one round-trip.
    #[inline]
    pub fn serve_one<F: FnOnce(Req) -> Resp>(&self, f: F) { self.inner.serve_one(f) }

    /// Serve requests forever with a stateful handler.
    #[inline]
    pub fn serve_loop<F: FnMut(Req) -> Resp>(&self, f: F) -> ! { self.inner.serve_loop(f) }
}

#[cfg(feature = "tokio")]
impl<Req: Send, Resp: Send, W: AsyncWaiter> Server<Req, Resp, W> {
    /// Async serve — one round-trip.
    pub async fn serve_one_async<F: FnOnce(Req) -> Resp>(&self, f: F) {
        self.inner.serve_one_async(f).await
    }
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
        struct Tracked(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }

        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let ch = Channel::<Tracked, ()>::new();
            ch.set_client(std::thread::current());
            // Write a Req without ever servicing it.
            unsafe { (*ch.req_slot.get()).write(Tracked(drops.clone())); }
            ch.req_open.store(true, Ordering::Release);
            // ch drops here — Drop impl must drop the Tracked inside.
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1, "inflight Req must be dropped");
    }

    #[test]
    fn box_payload_zero_copy_rtt() {
        let (client, server) = Channel::<Box<Vec<u8>>, Box<Vec<u8>>>::spsc();

        let h = std::thread::spawn(move || {
            server.bind();
            server.serve_one(|mut req| {
                for b in req.iter_mut() { *b = b.wrapping_add(1); }
                req
            });
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        client.bind();

        let payload: Box<Vec<u8>> = Box::new(vec![10, 20, 30, 40]);
        let ptr_before = payload.as_ptr() as usize;
        let resp = client.call(payload);
        let ptr_after = resp.as_ptr() as usize;

        assert_eq!(*resp, vec![11, 21, 31, 41]);
        assert_eq!(ptr_before, ptr_after, "heap buffer did not move");

        h.join().unwrap();
    }

    #[test]
    fn try_call_returns_req_when_empty() {
        let (client, _server) = Channel::<u64, u64>::spsc();
        client.bind();
        let r = client.try_call(42);
        assert_eq!(r, Err(42));
    }

    #[test]
    fn server_panic_poisons_and_wakes_client() {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let (client, server) = Channel::<u64, u64>::spsc();

        let h = std::thread::spawn(move || {
            server.bind();
            let res = catch_unwind(AssertUnwindSafe(|| {
                server.serve_one(|_req| -> u64 { panic!("handler boom") });
            }));
            assert!(res.is_err(), "serve_one must propagate the panic");
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        client.bind();

        let res = catch_unwind(AssertUnwindSafe(|| client.call(42)));
        assert!(res.is_err(), "client.call must panic when channel is poisoned");

        h.join().unwrap();
    }

    #[test]
    fn layout_invariants() {
        // The two halves must sit on distinct cache lines.
        let ch: Box<Channel<u64, u64>> = Box::new(Channel::new());
        let base = (&*ch) as *const _ as usize;
        let req_off  = (&ch.req_open)  as *const _ as usize - base;
        let resp_off = (&ch.resp_open) as *const _ as usize - base;
        assert_eq!(base % 64, 0, "Channel alloc must align to 64 B");
        assert!(resp_off >= req_off + 64,
            "resp half must be ≥1 cache line past req half (req_off={req_off}, resp_off={resp_off})");

        // Larger payload — same invariant.
        let ch2: Box<Channel<[u8; 256], [u8; 256]>> = Box::new(Channel::new());
        let base = (&*ch2) as *const _ as usize;
        let req_off  = (&ch2.req_open)  as *const _ as usize - base;
        let resp_off = (&ch2.resp_open) as *const _ as usize - base;
        assert_eq!(base % 64, 0);
        assert!(resp_off >= req_off + 64);
    }

    #[test]
    fn poisoned_channel_drop_does_not_double_free() {
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
                        panic!("handler boom");
                        #[allow(unreachable_code)]
                        Tracked(drops_s.clone())
                    });
                }));
            });

            std::thread::sleep(std::time::Duration::from_millis(10));
            client.bind();
            let _ = catch_unwind(AssertUnwindSafe(|| client.call(1)));
            h.join().unwrap();
        }

        assert_eq!(drops.load(Ordering::Relaxed), 0,
            "poisoned channel must not drop uninit resp_slot");
    }

    // ── Async-mirror tests (feature = "tokio") ──────────────────────────

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn basic_rtt_echo_async() {
        use crate::waiter::NotifyWaiter;
        type ChA<Req, Resp> = Channel<Req, Resp, NotifyWaiter>;

        let (client, server) = ChA::<u64, u64>::spsc();

        let server_fut = async move {
            for _ in 0u64..50 {
                server.serve_one_async(|req| req.wrapping_mul(2)).await;
            }
        };
        let client_fut = async move {
            for i in 0u64..50 {
                assert_eq!(client.call_async(i).await, i.wrapping_mul(2));
            }
        };
        // Use join! to keep both futures local — sidesteps the RPITIT-Send
        // limitation that bites tokio::spawn on AsyncWaiter futures.
        tokio::join!(server_fut, client_fut);
    }
}
