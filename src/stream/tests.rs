//! Stream<T> unit tests — SPSC correctness, segment crossing, drop
//! safety, cross-thread blocking, receipt verification.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use super::{BufferedSender, Stream};

#[test]
fn send_recv_single_thread() {
    let s: Stream<u64> = Stream::new();
    let r = s.send(42);
    assert_eq!(r.seq(), 0);
    assert_eq!(s.tail(), 1);
    assert_eq!(s.cursor(), 0);
    assert_eq!(s.try_recv(), Some(42));
    assert_eq!(s.cursor(), 1);
    assert_eq!(s.try_recv(), None);
}

#[test]
fn send_iter_returns_last_seq() {
    let s: Stream<u64> = Stream::new();
    let r = s.send_iter([10, 20, 30]).unwrap();
    assert_eq!(r.seq(), 2); // last item, 3rd sent
    assert_eq!(s.tail(), 3);
    assert!(s.send_iter(std::iter::empty::<u64>()).is_none());
    assert_eq!(s.tail(), 3); // unchanged
}

#[test]
fn send_iter_then_recv_in_order() {
    let s: Stream<u64> = Stream::new();
    s.send_iter(0..50u64);
    for i in 0..50u64 {
        assert_eq!(s.try_recv(), Some(i));
    }
    assert_eq!(s.try_recv(), None);
}

#[test]
fn cross_segment_boundary() {
    // SEG_SIZE = 256. Send enough to span 3 segments and back.
    let s: Stream<u64> = Stream::new();
    const N: u64 = 700;
    for i in 0..N {
        s.send(i);
    }
    assert_eq!(s.tail(), N);
    for i in 0..N {
        assert_eq!(s.try_recv(), Some(i), "seq {} mismatched", i);
    }
    assert_eq!(s.cursor(), N);
    assert!(s.try_recv().is_none());
}

#[test]
fn cross_thread_spsc_blocking() {
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let s2 = s.clone();
    let handle = thread::spawn(move || {
        s2.set_consumer(thread::current());
        let mut sum = 0u64;
        for _ in 0..1000 {
            sum = sum.wrapping_add(s2.recv());
        }
        sum
    });
    for i in 0..1000u64 {
        s.send(i);
    }
    let sum = handle.join().unwrap();
    assert_eq!(sum, (0..1000u64).sum());
}

#[test]
fn cross_thread_recv_bulk() {
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let s2 = s.clone();
    let handle = thread::spawn(move || {
        s2.set_consumer(thread::current());
        let mut buf: Vec<u64> = Vec::new();
        let mut total = 0;
        while total < 500 {
            buf.clear();
            // Block on first via recv, then drain.
            buf.push(s2.recv());
            s2.recv_bulk(&mut buf, 64);
            total += buf.len();
        }
        total
    });
    for i in 0..500u64 {
        s.send(i);
    }
    let total = handle.join().unwrap();
    assert_eq!(total, 500);
}

#[test]
fn receipt_is_delivered() {
    let s: Stream<u64> = Stream::new();
    let r0 = s.send(10);
    let r1 = s.send(20);
    assert!(!r0.is_delivered(&s));
    assert!(!r1.is_delivered(&s));
    s.try_recv();
    assert!(r0.is_delivered(&s));
    assert!(!r1.is_delivered(&s));
    s.try_recv();
    assert!(r1.is_delivered(&s));
}

#[test]
fn receipt_seq_is_per_message() {
    let s: Stream<u64> = Stream::new();
    let r0 = s.send(1);
    let r1 = s.send(2);
    let r2 = s.send(3);
    assert_eq!(r0.seq(), 0);
    assert_eq!(r1.seq(), 1);
    assert_eq!(r2.seq(), 2);
}

#[test]
fn drop_drains_remaining_payload() {
    struct Tracked(Arc<AtomicUsize>);
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    {
        let s: Stream<Tracked> = Stream::new();
        // Cross at least 2 segment boundaries.
        for _ in 0..600 {
            s.send(Tracked(drops.clone()));
        }
        // Drain a few — those drops happen in try_recv.
        for _ in 0..100 {
            drop(s.try_recv().unwrap());
        }
        // The remaining 500 must be dropped when `s` goes out of scope.
    }
    assert_eq!(drops.load(Ordering::Relaxed), 600);
}

#[test]
fn box_payload_zero_copy() {
    // Sanity: send a Box, recv the Box, check pointer identity.
    let s: Stream<Box<u64>> = Stream::new();
    let b = Box::new(0xDEAD_BEEFu64);
    let raw = Box::as_ref(&b) as *const u64;
    s.send(b);
    let got = s.try_recv().unwrap();
    let got_raw = Box::as_ref(&got) as *const u64;
    assert_eq!(
        raw, got_raw,
        "Box pointer must be preserved (zero-copy transfer)"
    );
    assert_eq!(*got, 0xDEAD_BEEF);
}

#[test]
fn len_and_is_empty() {
    let s: Stream<u64> = Stream::new();
    assert!(s.is_empty());
    assert_eq!(s.len(), 0);
    s.send(1);
    s.send(2);
    s.send(3);
    assert_eq!(s.len(), 3);
    assert!(!s.is_empty());
    s.try_recv();
    assert_eq!(s.len(), 2);
}

// ─── BufferedSender tests ─────────────────────────────────────────────────

#[test]
fn buffered_accumulates_until_threshold() {
    let stream = Arc::new(Stream::<u64>::new());
    let mut tx = stream.buffered(4);
    tx.send(1);
    tx.send(2);
    tx.send(3);
    // Below threshold — nothing has been flushed yet.
    assert_eq!(stream.tail(), 0);
    assert_eq!(tx.pending(), 3);
    tx.send(4);
    // Threshold hit — auto-flush.
    assert_eq!(stream.tail(), 4);
    assert_eq!(tx.pending(), 0);
}

#[test]
fn buffered_explicit_flush() {
    let stream = Arc::new(Stream::<u64>::new());
    let mut tx = stream.buffered(64);
    for i in 0..10u64 {
        tx.send(i);
    }
    assert_eq!(stream.tail(), 0); // below threshold, nothing sent
    let r = tx.flush().unwrap();
    assert_eq!(r.seq(), 9);
    assert_eq!(stream.tail(), 10);
    assert_eq!(tx.pending(), 0);
    // Flush on empty buffer is a no-op (returns last receipt).
    let r2 = tx.flush().unwrap();
    assert_eq!(r2.seq(), 9);
}

#[test]
fn buffered_drop_flushes_residue() {
    let stream = Arc::new(Stream::<u64>::new());
    {
        let mut tx = stream.buffered(64);
        for i in 0..5u64 {
            tx.send(i);
        }
        assert_eq!(stream.tail(), 0); // nothing sent yet
    } // tx dropped → flush
    assert_eq!(stream.tail(), 5);
    for i in 0..5u64 {
        assert_eq!(stream.try_recv(), Some(i));
    }
}

#[test]
fn buffered_drop_with_payload_drops_remaining() {
    struct Tracked(Arc<AtomicUsize>);
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let stream: Arc<Stream<Tracked>> = Arc::new(Stream::new());
    {
        let mut tx = stream.buffered(64);
        for _ in 0..10 {
            tx.send(Tracked(drops.clone()));
        }
        // 10 < 64 — items live in tx's local Vec.
        assert_eq!(drops.load(Ordering::Relaxed), 0);
    } // tx drops → flush sends 10 to stream; stream still alive.
    assert_eq!(stream.tail(), 10);
    // Drop stream → drains remaining initialized slots.
    drop(stream);
    assert_eq!(drops.load(Ordering::Relaxed), 10);
}

#[test]
fn buffered_cross_thread_with_consumer() {
    let stream = Arc::new(Stream::<u64>::new());
    let s2 = stream.clone();
    let consumer = thread::spawn(move || {
        s2.set_consumer(thread::current());
        let mut sum = 0u64;
        for _ in 0..1000 {
            sum = sum.wrapping_add(s2.recv());
        }
        sum
    });
    let mut tx = stream.buffered(64);
    for i in 0..1000u64 {
        tx.send(i);
    }
    drop(tx); // flush residue (1000 % 64 = 40 final items)
    let sum = consumer.join().unwrap();
    assert_eq!(sum, (0..1000u64).sum());
}

#[test]
fn buffered_last_receipt_tracks_flushes() {
    let stream = Arc::new(Stream::<u64>::new());
    let mut tx = stream.buffered(4);
    assert!(tx.last_receipt().is_none());
    tx.send(0);
    tx.send(1);
    tx.send(2);
    tx.send(3); // auto-flush at K=4
    assert_eq!(tx.last_receipt().unwrap().seq(), 3);
    tx.send(4);
    tx.send(5);
    assert_eq!(tx.last_receipt().unwrap().seq(), 3); // unchanged before flush
    let r = tx.flush().unwrap();
    assert_eq!(r.seq(), 5);
    assert_eq!(tx.last_receipt().unwrap().seq(), 5);
}

#[test]
#[should_panic(expected = "threshold must be > 0")]
fn buffered_threshold_zero_panics() {
    let stream = Arc::new(Stream::<u64>::new());
    let _ = BufferedSender::new(stream, 0);
}

#[test]
fn many_segments_then_drain_all() {
    let s: Stream<u64> = Stream::new();
    const N: u64 = 5_000; // ~20 segments
    for i in 0..N {
        s.send(i);
    }
    let mut buf = Vec::with_capacity(N as usize);
    while !s.is_empty() {
        s.recv_bulk(&mut buf, 256);
    }
    assert_eq!(buf.len(), N as usize);
    for (i, v) in buf.iter().enumerate() {
        assert_eq!(*v, i as u64);
    }
}

#[test]
fn recv_or_cancel_returns_data_when_present() {
    use crate::gate::Lifeline;
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let life = Arc::new(Lifeline::new());
    s.send(7);
    s.set_consumer(thread::current());
    let id = life.register(thread::current());
    assert_eq!(s.recv_or_cancel(&life, id).unwrap(), 7);
}

#[test]
fn recv_or_cancel_aborts_when_cancelled_before_call() {
    use crate::gate::Lifeline;
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let life = Arc::new(Lifeline::new());
    s.set_consumer(thread::current());
    let id = life.register(thread::current());
    life.cancel_one(id);
    assert!(s.recv_or_cancel(&life, id).is_err());
}

#[test]
fn recv_or_cancel_unparks_blocked_worker() {
    use crate::gate::Lifeline;
    use std::time::Duration;
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let life = Arc::new(Lifeline::new());
    let s2 = s.clone();
    let l2 = life.clone();
    let h = thread::spawn(move || {
        s2.set_consumer(thread::current());
        let id = l2.register(thread::current());
        s2.recv_or_cancel(&l2, id)
    });
    thread::sleep(Duration::from_millis(30));
    life.cancel_all();
    let r = h.join().unwrap();
    assert!(r.is_err());
}

#[test]
fn recv_or_cancel_one_target_only() {
    use crate::gate::Lifeline;
    use std::sync::mpsc;
    use std::time::Duration;
    let s_a: Arc<Stream<u64>> = Arc::new(Stream::new());
    let s_b: Arc<Stream<u64>> = Arc::new(Stream::new());
    let life = Arc::new(Lifeline::new());

    // Capture each worker's actual `WaiterId` instead of assuming spawn
    // order — `register()` returns sequential ids but the scheduler may
    // run B before A, in which case B would be `WaiterId(0)` and the
    // hard-coded `cancel_one(WaiterId(0))` would target the wrong worker
    // (cancelling B, leaving A parked forever).
    let (id_tx_a, id_rx_a) = mpsc::channel::<crate::gate::WaiterId>();
    let (id_tx_b, id_rx_b) = mpsc::channel::<crate::gate::WaiterId>();

    let sa = s_a.clone();
    let la = life.clone();
    let h_a = thread::spawn(move || {
        sa.set_consumer(thread::current());
        let id = la.register(thread::current());
        id_tx_a.send(id).unwrap();
        sa.recv_or_cancel(&la, id).map(|_| ()).map_err(|_| ())
    });
    let sb = s_b.clone();
    let lb = life.clone();
    let h_b = thread::spawn(move || {
        sb.set_consumer(thread::current());
        let id = lb.register(thread::current());
        id_tx_b.send(id).unwrap();
        sb.recv_or_cancel(&lb, id)
    });

    let id_a = id_rx_a.recv().unwrap();
    let _id_b = id_rx_b.recv().unwrap();
    thread::sleep(Duration::from_millis(30));

    // Cancel only worker A by its captured id.
    life.cancel_one(id_a);
    let _ = h_a.join().unwrap(); // Worker A returns Err.

    // Worker B is still parked. Send data → returns Ok.
    s_b.send(42);
    assert_eq!(h_b.join().unwrap().unwrap(), 42);
}
