//! Integration tests for the full `Synapse` primitive.
//!
//! Tests live in one file because they exercise the public API end-to-end.
//! If any single test grows beyond ~50 lines of setup, split it into a
//! helper module inside this folder.

use super::state::{Shutdown, Synapse};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn layout_invariants() {
    // `Synapse` itself must align to 64 B so heap allocations (via
    // Box/Arc) don't place us on a line shared with the allocator
    // header. `head` and `tail` must live on distinct cache lines
    // to avoid false sharing between producer writes and consumer
    // CAS claims.
    let s: Box<Synapse<u64, 64, 4>> = Box::new(Synapse::new());
    let base = (&*s) as *const _ as usize;
    let head_off = (&s.head) as *const _ as usize - base;
    let tail_off = (&s.tail) as *const _ as usize - base;
    assert_eq!(base % 64, 0,      "Synapse alloc must align to 64 B");
    assert_eq!(head_off, 0,       "head at offset 0");
    assert!(tail_off >= 64,       "tail must be on a separate cache line");
    assert_eq!(tail_off % 64, 0,  "tail must align to 64 B");
}

#[test]
fn n1_matches_ring_spsc() {
    // With N=1, Synapse should behave just like a Ring SPSC.
    let s: Synapse<u64, 8, 1> = Synapse::new();
    assert!(s.is_empty());
    assert_eq!(s.capacity(), 8);
    assert_eq!(s.consumers(), 1);
    for i in 0..5 { assert!(s.try_send(i).is_ok()); }
    assert_eq!(s.len(), 5);
    for i in 0..5 {
        assert_eq!(s.try_recv(), Some(i));
    }
    assert!(s.is_empty());
    assert_eq!(s.try_recv(), None);
}

#[test]
fn try_send_returns_err_when_full() {
    let s: Synapse<u64, 4, 2> = Synapse::new();
    for i in 0..4 { assert!(s.try_send(i).is_ok()); }
    assert_eq!(s.try_send(99), Err(99));
}

#[test]
fn spmc_exactly_once_delivery() {
    // 1 producer, 4 consumers. Every message must be delivered
    // exactly once (no duplicates, no losses).
    const MSGS: u64 = 2_000;
    let s: Arc<Synapse<u64, 256, 4>> = Arc::new(Synapse::new());
    let total = Arc::new(AtomicU64::new(0));
    let count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];
    for i in 0..4 {
        let s = s.clone();
        let total = total.clone();
        let count = count.clone();
        handles.push(std::thread::spawn(move || {
            s.bind_consumer(i);
            loop {
                match s.recv(i) {
                    Ok(v) => {
                        total.fetch_add(v, Ordering::Relaxed);
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(Shutdown) => break,
                }
            }
        }));
    }

    // Producer (main thread).
    s.set_producer(std::thread::current());
    for i in 0..MSGS { s.send(i); }

    // Wait until all messages drained, then shutdown.
    while count.load(Ordering::Relaxed) < MSGS as usize {
        std::thread::sleep(Duration::from_millis(1));
    }
    s.shutdown();

    for h in handles { h.join().unwrap(); }

    let expected: u64 = (0..MSGS).sum();
    assert_eq!(total.load(Ordering::Relaxed), expected,
               "exactly-once delivery failed: got sum {} vs expected {}",
               total.load(Ordering::Relaxed), expected);
    assert_eq!(count.load(Ordering::Relaxed), MSGS as usize,
               "message count mismatch");
}

#[test]
fn spmc_fair_distribution() {
    // 4 consumers should each receive at least ~5% of messages under
    // a hot load (loose bound — fairness is best-effort, not strict
    // round-robin). Tests that no consumer starves entirely.
    const MSGS: u64 = 4_000;
    let s: Arc<Synapse<u64, 256, 4>> = Arc::new(Synapse::new());
    let counters: Arc<[AtomicUsize; 4]> = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));

    let mut handles = vec![];
    for i in 0..4 {
        let s = s.clone();
        let counters = counters.clone();
        handles.push(std::thread::spawn(move || {
            s.bind_consumer(i);
            loop {
                match s.recv(i) {
                    Ok(_) => { counters[i].fetch_add(1, Ordering::Relaxed); }
                    Err(Shutdown) => break,
                }
            }
        }));
    }

    s.set_producer(std::thread::current());
    for i in 0..MSGS { s.send(i); }

    let total_target = MSGS as usize;
    while counters.iter().map(|c| c.load(Ordering::Relaxed)).sum::<usize>() < total_target {
        std::thread::sleep(Duration::from_millis(1));
    }
    s.shutdown();
    for h in handles { h.join().unwrap(); }

    let sum: usize = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    assert_eq!(sum, total_target);

    // Loose fairness: every consumer got ≥ 5% of messages (200/4000).
    // This is far below round-robin perfect (25%) but catches
    // total-starvation bugs.
    let min_share = total_target * 5 / 100;
    for (i, c) in counters.iter().enumerate() {
        let got = c.load(Ordering::Relaxed);
        assert!(got >= min_share,
                "consumer {} received only {} / {} (below {}% starvation threshold)",
                i, got, total_target, 5);
    }
}

#[test]
fn drop_drains_inflight() {
    struct Tracked(Arc<AtomicUsize>);
    impl Drop for Tracked {
        fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    {
        let s: Synapse<Tracked, 8, 2> = Synapse::new();
        // Send 5 items, consume none. Drop should drain them all.
        for _ in 0..5 { s.try_send(Tracked(drops.clone())).ok().unwrap(); }
        // Synapse drops here.
    }
    assert_eq!(drops.load(Ordering::Relaxed), 5,
               "Drop must drain all inflight slots");
}

#[test]
fn shutdown_wakes_parked_consumers() {
    // All consumers park on an empty Synapse. Supervisor calls
    // shutdown() from another thread — every consumer must wake
    // and see Err(Shutdown).
    let s: Arc<Synapse<u64, 8, 4>> = Arc::new(Synapse::new());
    let mut handles = vec![];
    for i in 0..4 {
        let s = s.clone();
        handles.push(std::thread::spawn(move || {
            s.bind_consumer(i);
            match s.recv(i) {
                Ok(_) => panic!("no work was sent; expected Shutdown"),
                Err(Shutdown) => {}
            }
        }));
    }

    // Give consumers time to park.
    std::thread::sleep(Duration::from_millis(20));
    s.shutdown();

    for h in handles { h.join().unwrap(); }
}

#[test]
fn box_ownership_through_synapse() {
    // Prove zero-copy: the heap buffer's address is stable across
    // the Synapse transfer. Only the 8-byte pointer crosses.
    let s: Arc<Synapse<Box<Vec<u8>>, 4, 1>> = Arc::new(Synapse::new());
    let s2 = s.clone();

    let h = std::thread::spawn(move || {
        s2.bind_consumer(0);
        let v = s2.recv(0).unwrap();
        v
    });

    std::thread::sleep(Duration::from_millis(10));
    s.set_producer(std::thread::current());

    let payload: Box<Vec<u8>> = Box::new(vec![10, 20, 30, 40]);
    let ptr_before = payload.as_ptr() as usize;
    s.send(payload);

    let recv = h.join().unwrap();
    let ptr_after = recv.as_ptr() as usize;
    assert_eq!(*recv, vec![10, 20, 30, 40]);
    assert_eq!(ptr_before, ptr_after, "heap buffer must not move");
}
