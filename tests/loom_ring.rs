//! Loom concurrency scenarios for `Ring` (split-handle v2).
//!
//! ## Verification scope (honest disclosure)
//!
//! This test file uses `loom::thread::spawn` for interleaving exploration.
//! In addition, `ring.rs` swaps its shared `AtomicUsize` / `AtomicBool` /
//! `Ordering` for `loom::sync::atomic::*` under `#[cfg(loom)]`, so loom
//! **does** get to explore reorderings on the `head` / `tail` cursors and
//! the `closed` flag.
//!
//! What this catches:
//! - Weak-memory-model reorderings on `head` / `tail` / `closed` (missing
//!   `Acquire`/`Release`, `Relaxed` where it isn't safe).
//! - Algorithmic races (missed items, doubled items, double drops).
//! - Order-of-operations bugs in the send/recv/close sequence, including
//!   the "item published right before producer drop" disconnect race.
//!
//! What this does NOT catch:
//! - `UnsafeCell` access races on the slot storage. Loom's `UnsafeCell`
//!   was NOT swapped in — that would require migrating every `.get()`
//!   call site to `.with(|p| ...)`. We rely on Miri (run separately) for
//!   UB detection at the cell level under the real memory model. The v2
//!   cached cursors are plain fields of the uniquely-owned handles, so
//!   they need no cell modeling at all.
//! - Wake/park handoff correctness through the `Waiter`: scenarios use
//!   only `try_send` / `try_recv` to keep the OS-thread `ParkWaiter` out
//!   of loom's model (loom doesn't shim `thread::park`).
//!
//! Preemption bound: `LOOM_MAX_PREEMPTIONS` from the environment wins
//! when set (the CI/verification runs use 3); otherwise defaults to 2.

#![cfg(loom)]

use arbitro_kit::stream::{Consumer, Producer, Ring, TryRecvError};
use loom::thread;

/// Cap loom exploration. Respects `LOOM_MAX_PREEMPTIONS` when set;
/// defaults to preemption bound 2 otherwise.
fn model<F>(f: F)
where
    F: Fn() + Sync + Send + 'static,
{
    let mut builder = loom::model::Builder::new();
    let bound = std::env::var("LOOM_MAX_PREEMPTIONS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(2);
    builder.preemption_bound = Some(bound);
    builder.check(f);
}

/// Busy-loop `try_recv` from the consumer side until we've drained `n`
/// items. This is *not* a spin loop in production code — it exists only to
/// drive loom's scheduler forward without introducing a Waiter into the
/// model.
fn drain_n<const CAP: usize>(rx: &mut Consumer<u32, CAP>, n: usize) -> Vec<u32> {
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        match rx.try_recv() {
            Ok(v) => out.push(v),
            Err(_) => thread::yield_now(),
        }
    }
    out
}

/// Busy-loop `try_send` until enqueued. Same rationale as `drain_n`.
fn send_all<const CAP: usize>(tx: &mut Producer<u32, CAP>, values: &[u32]) {
    for &v in values {
        let mut cur = v;
        loop {
            match tx.try_send(cur) {
                Ok(()) => break,
                Err(e) => {
                    cur = e.into_value();
                    thread::yield_now();
                }
            }
        }
    }
}

trait IntoValue<T> {
    fn into_value(self) -> T;
}
impl<T> IntoValue<T> for arbitro_kit::stream::TrySendError<T> {
    fn into_value(self) -> T {
        match self {
            arbitro_kit::stream::TrySendError::Full(v) => v,
            arbitro_kit::stream::TrySendError::Closed(v) => v,
        }
    }
}

// ── Scenario A ────────────────────────────────────────────────────────
// Producer sends 2, consumer recvs 2, cap=2. No backpressure required.

#[test]
fn loom_a_two_items_cap2_no_backpressure() {
    // A static counter records how many interleavings loom actually
    // explored — a smoke check that the atomic shim inside `ring.rs`
    // is exposing enough state for meaningful exploration.
    static ITERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    model(|| {
        ITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (mut tx, mut rx) = Ring::<u32, 2>::new();
        let p = thread::spawn(move || {
            send_all::<2>(&mut tx, &[10, 20]);
        });
        let got = drain_n::<2>(&mut rx, 2);
        p.join().unwrap();
        assert_eq!(got, vec![10, 20]);
        assert!(rx.is_empty());
    });
    // If this drops to a small single-digit number, the shim likely
    // regressed to std atomics — loom would only see thread spawn/join.
    assert!(
        ITERS.load(std::sync::atomic::Ordering::Relaxed) >= 5,
        "loom explored too few interleavings — atomic shim may have regressed",
    );
}

// ── Scenario B ────────────────────────────────────────────────────────
// Producer sends 3, consumer recvs 3, cap=2. Forces the producer to
// observe "full" at least once — exercises the cached_tail refresh path.

#[test]
fn loom_b_three_items_cap2_backpressure() {
    model(|| {
        let (mut tx, mut rx) = Ring::<u32, 2>::new();
        let p = thread::spawn(move || {
            send_all::<2>(&mut tx, &[1, 2, 3]);
        });
        let got = drain_n::<2>(&mut rx, 3);
        p.join().unwrap();
        assert_eq!(got, vec![1, 2, 3]);
        assert!(rx.is_empty());
    });
}

// ── Scenario C ────────────────────────────────────────────────────────
// Consumer starts, producer sends 1, consumer recvs 1, both drop.
// Verifies no leak, no double-drop, no UB in teardown.

#[test]
fn loom_c_single_item_lifecycle() {
    model(|| {
        let (mut tx, mut rx) = Ring::<u32, 2>::new();
        let c = thread::spawn(move || {
            loop {
                match rx.try_recv() {
                    Ok(v) => return v,
                    Err(_) => thread::yield_now(),
                }
            }
        });
        while tx.try_send(42).is_err() {
            thread::yield_now();
        }
        let v = c.join().unwrap();
        assert_eq!(v, 42);
        // Handles drop both ends here — no double-drop must occur.
    });
}

// ── Scenario D ────────────────────────────────────────────────────────
// Drop the ring with unread items. The shared `Ring::drop` must drain
// each `T` exactly once. We use an Arc<AtomicUsize> drop counter as the
// payload witness.

#[test]
fn loom_d_drop_drains_unread() {
    use loom::sync::atomic::{AtomicUsize, Ordering as O};
    use loom::sync::Arc;

    model(|| {
        let drops = Arc::new(AtomicUsize::new(0));

        struct Tracked {
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.drops.fetch_add(1, O::Relaxed);
            }
        }

        let (mut tx, rx) = Ring::<Tracked, 2>::new();
        assert!(tx.try_send(Tracked { drops: drops.clone() }).is_ok());
        assert!(tx.try_send(Tracked { drops: drops.clone() }).is_ok());
        // Drop both handles with 2 items unread — the shared drain runs
        // when the second Arc reference goes away.
        drop(tx);
        drop(rx);
        assert_eq!(drops.load(O::Relaxed), 2);
    });
}

// ── Scenario E ────────────────────────────────────────────────────────
// send-recv-send interleaving with cap=1 (tightest possible). Each op
// forces a full round trip through the ring.

#[test]
fn loom_e_cap1_ping_pong() {
    model(|| {
        let (mut tx, mut rx) = Ring::<u32, 1>::new();
        let p = thread::spawn(move || {
            send_all::<1>(&mut tx, &[1, 2, 3]);
        });
        let got = drain_n::<1>(&mut rx, 3);
        p.join().unwrap();
        assert_eq!(got, vec![1, 2, 3]);
        assert!(rx.is_empty());
    });
}

// ── Scenario F ────────────────────────────────────────────────────────
// Disconnect race: producer sends 1 item and drops concurrently with the
// consumer draining. The consumer must see the item BEFORE observing
// Closed — the `closed` flag is stored Release after the final `head`
// store, and try_recv re-reads `head` after Acquire-loading `closed`.

#[test]
fn loom_f_producer_drop_delivers_inflight_then_closes() {
    model(|| {
        let (mut tx, mut rx) = Ring::<u32, 2>::new();
        let p = thread::spawn(move || {
            tx.try_send(99).unwrap();
            // tx drops here → closed set, after the send's head store.
        });
        let mut got = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(v) => got.push(v),
                Err(TryRecvError::Empty) => thread::yield_now(),
                Err(TryRecvError::Closed) => break,
            }
        }
        p.join().unwrap();
        // Closed must never surface before the in-flight item.
        assert_eq!(got, vec![99]);
    });
}
