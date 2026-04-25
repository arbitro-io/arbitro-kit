//! `Duplex<A, B>` — bidirectional unbounded SPSC.
//!
//! A pair of [`Stream`]s glued together with type-level direction safety.
//! Each end has a fixed `Send` type and a fixed `Recv` type. The
//! compiler enforces that you can't send the wrong type or drain the
//! wrong direction.
//!
//! ## Topology
//!
//! ```text
//!   left                                 right
//!   ─────                                ─────
//!   send: A  ─────►  Stream<A>  ─────►  recv: A
//!   recv: B  ◄─────  Stream<B>  ◄─────  send: B
//! ```
//!
//! ## Concurrency contract (per end)
//!
//! - One thread on `left` calls `send`, another may call `recv`. Same
//!   on `right`. Internally each direction is its own SPSC `Stream`,
//!   so the strict "1 producer + 1 consumer per stream" rule is
//!   satisfied as long as `left.send`/`left.recv` are not cloned to
//!   multiple threads.
//! - Bridging across two threads on each end is the typical use:
//!   thread `T_l_send` sends; thread `T_l_recv` (registered via
//!   `set_consumer`) drains. The "one producer / one consumer per
//!   direction" rule holds.
//! - For multi-producer or multi-consumer per side, build on top of
//!   the dedicated MPSC / broadcast variants when those land.
//!
//! ## What `Duplex` adds over two raw `Stream`s
//!
//! - **Type-level direction safety**: `DuplexEnd<A, B>` documents
//!   "send A, receive B". Mixing up the wiring becomes a compile
//!   error.
//! - **Atomic construction**: one `Duplex::pair()` call produces both
//!   ends correctly wired.
//! - **No runtime overhead**: every method delegates 1:1 to the
//!   underlying `Stream` — same `send`/`recv`/`try_recv`/`recv_bulk`/
//!   `set_consumer` numbers.

use std::marker::PhantomData;
use std::sync::Arc;

use super::receipt::Receipt;
use super::stream::Stream;

/// One end of a [`Duplex`]. Owns a clone of the outbound stream and a
/// clone of the inbound stream.
///
/// Holds `Arc<Stream<Send>>` for outgoing and `Arc<Stream<Recv>>` for
/// incoming. The peer holds the same two `Arc`s but with the
/// direction labels swapped.
pub struct DuplexEnd<S, R> {
    out:   Arc<Stream<S>>,
    inbox: Arc<Stream<R>>,
}

impl<S, R> DuplexEnd<S, R> {
    // ─── outbound ─────────────────────────────────────────────────────────

    /// Send one item to the peer. Returns a [`Receipt`] for the seq
    /// of the message in the outbound stream. Never blocks.
    #[inline]
    pub fn send(&self, value: S) -> Receipt {
        self.out.send(value)
    }

    /// Send a batch of items. Returns the receipt of the last item, or
    /// `None` if the iterator yielded nothing.
    pub fn send_iter<I: IntoIterator<Item = S>>(&self, items: I) -> Option<Receipt> {
        self.out.send_iter(items)
    }

    /// Borrow the outbound stream — for cursor checks, custom flows.
    #[inline]
    pub fn out_stream(&self) -> &Stream<S> { &self.out }

    /// Total items WE have produced toward the peer.
    #[inline]
    pub fn out_tail(&self) -> u64 { self.out.tail() }

    /// Returns `true` if the peer has drained past this receipt's
    /// message in our outbound stream. Cost: one Acquire load.
    ///
    /// Convenience over `receipt.is_delivered(end.out_stream())`.
    #[inline]
    pub fn is_delivered(&self, receipt: Receipt) -> bool {
        receipt.is_delivered(&self.out)
    }

    /// Block until the peer has drained past this receipt's message.
    /// MVP busy-spins on the cursor.
    ///
    /// Convenience over `receipt.wait_delivered(end.out_stream())`.
    #[inline]
    pub fn wait_delivered(&self, receipt: Receipt) {
        receipt.wait_delivered(&self.out);
    }

    /// Block until the peer has drained at least up to `seq` in our
    /// outbound. Useful when you don't have the receipt object but
    /// know the seq.
    #[inline]
    pub fn wait_for_out(&self, seq: u64) {
        self.out.wait_for(seq);
    }

    // ─── inbound ──────────────────────────────────────────────────────────

    /// Non-blocking receive from the peer.
    #[inline]
    pub fn try_recv(&self) -> Option<R> {
        self.inbox.try_recv()
    }

    /// Blocking receive — parks (phased backoff via `Park`) until at
    /// least one item is available. Register the consumer thread via
    /// [`Self::set_consumer`] before the first call.
    #[inline]
    pub fn recv(&self) -> R {
        self.inbox.recv()
    }

    /// Drain up to `max` items into `buf`. Non-blocking.
    pub fn recv_bulk(&self, buf: &mut Vec<R>, max: usize) -> usize {
        self.inbox.recv_bulk(buf, max)
    }

    /// Register the thread that will block on `recv`. Must be called
    /// before the first blocking `recv` on this end.
    #[inline]
    pub fn set_consumer(&self, t: std::thread::Thread) {
        self.inbox.set_consumer(t);
    }

    /// Borrow the inbound stream — for cursor checks, custom flows.
    #[inline]
    pub fn in_stream(&self) -> &Stream<R> { &self.inbox }

    /// Total items WE have drained from the peer's outbound.
    #[inline]
    pub fn in_cursor(&self) -> u64 { self.inbox.cursor() }

    /// Total items the PEER has produced toward us.
    #[inline]
    pub fn peer_tail(&self) -> u64 { self.inbox.tail() }
}

// Safety: each Stream<T> is Send + Sync where T: Send. DuplexEnd
// just holds two Arcs of those, plus PhantomData.
unsafe impl<S: Send, R: Send> Send for DuplexEnd<S, R> {}
unsafe impl<S: Send, R: Send> Sync for DuplexEnd<S, R> {}

/// Bidirectional unbounded SPSC pair.
///
/// `Duplex` is a namespace type; you don't construct an instance.
/// Use [`Duplex::pair`] to create the two endpoints.
pub struct Duplex<A, B>(PhantomData<(A, B)>);

impl<A: Send + 'static, B: Send + 'static> Duplex<A, B> {
    /// Build a duplex pair.
    ///
    /// Returns `(left, right)` where:
    /// - `left.send(a: A)` → `right.recv() -> A`
    /// - `right.send(b: B)` → `left.recv() -> B`
    ///
    /// Each direction is an independent SPSC [`Stream`] constructed in
    /// "strict-wake" mode — the producer side adds an `mfence` (~3-5 ns
    /// on x86) between its publish and the wake. This closes the
    /// Dekker race that fires when the same thread is producer here
    /// and consumer on the peer stream (the bidirectional pattern
    /// inherent to `Duplex`). Plain `Stream` does not pay this cost.
    pub fn pair() -> (DuplexEnd<A, B>, DuplexEnd<B, A>) {
        let a_to_b: Arc<Stream<A>> = Arc::new(Stream::new_strict());
        let b_to_a: Arc<Stream<B>> = Arc::new(Stream::new_strict());

        let left = DuplexEnd {
            out:   a_to_b.clone(),
            inbox: b_to_a.clone(),
        };
        let right = DuplexEnd {
            out:   b_to_a,
            inbox: a_to_b,
        };
        (left, right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn pair_construction_and_basic_send_recv() {
        let (a, b) = Duplex::<u64, &'static str>::pair();
        a.send(42);
        assert_eq!(b.try_recv(), Some(42));

        b.send("hello");
        assert_eq!(a.try_recv(), Some("hello"));
    }

    #[test]
    fn each_direction_is_independent() {
        let (a, b) = Duplex::<u64, u64>::pair();
        // a sends 0,1,2 to b; b independently sends 100,101,102 to a.
        a.send(0); a.send(1); a.send(2);
        b.send(100); b.send(101); b.send(102);

        assert_eq!(b.try_recv(), Some(0));
        assert_eq!(a.try_recv(), Some(100));
        assert_eq!(b.try_recv(), Some(1));
        assert_eq!(a.try_recv(), Some(101));
        assert_eq!(b.try_recv(), Some(2));
        assert_eq!(a.try_recv(), Some(102));
        assert_eq!(a.try_recv(), None);
        assert_eq!(b.try_recv(), None);
    }

    #[test]
    fn cross_thread_blocking() {
        let (client, server) = Duplex::<u64, u64>::pair();

        let server_handle = thread::spawn(move || {
            server.set_consumer(thread::current());
            for _ in 0..100 {
                let req = server.recv();
                server.send(req.wrapping_mul(2) | 1);   // reply
            }
        });

        client.set_consumer(thread::current());
        let mut sum = 0u64;
        for i in 0..100u64 {
            client.send(i);
            sum = sum.wrapping_add(client.recv());
        }
        server_handle.join().unwrap();

        let expected: u64 = (0..100u64).map(|i| i.wrapping_mul(2) | 1).sum();
        assert_eq!(sum, expected);
    }

    #[test]
    fn cursors_track_each_direction() {
        let (a, b) = Duplex::<u64, u64>::pair();
        assert_eq!(a.out_tail(), 0);
        assert_eq!(b.out_tail(), 0);

        a.send(1); a.send(2); a.send(3);
        assert_eq!(a.out_tail(), 3);
        assert_eq!(b.peer_tail(), 3);  // b sees a's tail through its inbox

        b.send(10);
        assert_eq!(b.out_tail(), 1);
        assert_eq!(a.peer_tail(), 1);

        b.try_recv();
        assert_eq!(b.in_cursor(), 1);  // b drained 1 item from a's stream
    }

    #[test]
    fn type_level_direction() {
        // This test exists primarily as a compile-time sanity check.
        let (req_side, resp_side) = Duplex::<&'static str, u32>::pair();
        req_side.send("hello");                // OK — req_side sends &str
        // req_side.send(42u32);              // would NOT compile — wrong type
        assert_eq!(resp_side.try_recv(), Some("hello"));

        resp_side.send(42);
        // resp_side.send("oops");            // would NOT compile
        assert_eq!(req_side.try_recv(), Some(42));
    }

    #[test]
    fn fire_and_forget_with_delivery_verification() {
        let (a, b) = Duplex::<u64, u64>::pair();

        // Fire 3 items without caring about response — keep the receipts.
        let r0 = a.send(100);
        let r1 = a.send(200);
        let r2 = a.send(300);

        // Nothing drained yet — none delivered.
        assert!(!a.is_delivered(r0));
        assert!(!a.is_delivered(r1));
        assert!(!a.is_delivered(r2));

        // b drains the first two.
        assert_eq!(b.try_recv(), Some(100));
        assert_eq!(b.try_recv(), Some(200));

        // Now r0 and r1 are delivered; r2 still pending.
        assert!( a.is_delivered(r0));
        assert!( a.is_delivered(r1));
        assert!(!a.is_delivered(r2));

        b.try_recv();   // drain the last one
        assert!(a.is_delivered(r2));
    }

    #[test]
    fn wait_delivered_blocks_until_peer_drains() {
        let (a, b) = Duplex::<u64, ()>::pair();
        let r = a.send(42);
        let bb = b;
        let handle = thread::spawn(move || {
            // Brief delay before consuming, to ensure wait_delivered
            // actually waits.
            std::thread::sleep(std::time::Duration::from_millis(5));
            assert_eq!(bb.try_recv(), Some(42));
        });
        a.wait_delivered(r);   // busy-spins until peer drains
        handle.join().unwrap();
    }

    #[test]
    fn batch_send_iter_returns_last_receipt() {
        let (a, b) = Duplex::<u64, ()>::pair();
        let r = a.send_iter(0..100u64).unwrap();
        assert_eq!(r.seq(), 99);
        assert!(!a.is_delivered(r));

        // Drain everything; receipt for last item is delivered.
        for _ in 0..100 { b.try_recv(); }
        assert!(a.is_delivered(r));
    }

    #[test]
    fn drop_drains_payloads_in_both_directions() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        {
            let (a, b) = Duplex::<Tracked, Tracked>::pair();
            for _ in 0..5 { a.send(Tracked(drops.clone())); }
            for _ in 0..3 { b.send(Tracked(drops.clone())); }
            // a sent 5 to b's inbox, b sent 3 to a's inbox; nothing drained.
        } // drop both ends → drops both streams → drains 5 + 3 = 8.
        assert_eq!(drops.load(Ordering::Relaxed), 8);
    }
}
