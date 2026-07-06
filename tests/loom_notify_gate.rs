//! Loom model of the `NotifyWaiter` wake-gate protocol
//! (`src/waiter/notify.rs`).
//!
//! ## Verification scope (honest disclosure)
//!
//! `tokio::sync::Notify` is not loom-shimmed, so the real `NotifyWaiter`
//! cannot run under loom. What CAN be model-checked — and what is the
//! actual risky part of the change — is the Dekker flag protocol between
//! the waiter's (arm → predicate-check) sequence and the waker's
//! (data-store → armed-RMW-probe) sequence. This file mirrors those
//! exact atomic operations with loom atomics, standing a counter in for
//! `notify_one`.
//!
//! The property asserted across all interleavings:
//!
//! > If the waiter decided to suspend (gate armed and predicate false at
//! > its post-arm check), then at least one waker MUST have called
//! > notify.
//!
//! Combined with `tokio::sync::Notify`'s own guarantee (a `notify_one`
//! is never lost with respect to a `Notified` future created before it —
//! it either wakes the registered waiter or stores a permit the future
//! consumes on its next poll), this property implies no lost wake in the
//! real implementation. The mapping to the source is 1:1:
//!
//! | Model op                                | Source (notify.rs)                       |
//! |-----------------------------------------|------------------------------------------|
//! | `armed.fetch_add(1, SeqCst)`            | `WakeGate::notified()` (T1)              |
//! | predicate load (`Acquire`)              | caller's `ready()` re-check              |
//! | `armed.fetch_sub(1, SeqCst)`            | `DisarmOnDrop::drop` (T2)                |
//! | data store (`Release`); `armed.load(Relaxed)` else `armed.fetch_add(0, Release)` | caller publish + `WakeGate::wake()` (T3a/T3b) |
//!
//! The waker's authoritative probe is an RMW, not a load: RMWs must
//! read the latest value in the modification order (no stale read) and
//! never break release sequences. Both properties are part of the C11
//! model loom implements, so the fence-free protocol is checked as-is.
//! The preceding Relaxed load is a positive-only filter (seeing armed →
//! notify, which is always safe); loom explores the stale-read cases.
//!
//! Preemption bound: `LOOM_MAX_PREEMPTIONS` from the environment wins
//! when set (the CI/verification runs use 3); otherwise defaults to 2.

#![cfg(loom)]

use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::Arc;
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

/// Model of `WakeGate` — same atomics, same orderings as notify.rs.
struct GateModel {
    armed: AtomicUsize,
    /// Stand-in for `Notify::notify_one` calls.
    notifies: AtomicUsize,
}

impl GateModel {
    fn new() -> Self {
        Self {
            armed: AtomicUsize::new(0),
            notifies: AtomicUsize::new(0),
        }
    }

    /// T1 — `WakeGate::notified()`: arm.
    fn arm(&self) {
        self.armed.fetch_add(1, Ordering::SeqCst);
    }

    /// T2 — `DisarmOnDrop::drop`.
    fn disarm(&self) {
        self.armed.fetch_sub(1, Ordering::SeqCst);
    }

    /// T3 — `WakeGate::wake()`: positive-only Relaxed fast path (T3a),
    /// then the authoritative RMW probe (T3b). Mirrors notify.rs.
    fn wake(&self) {
        if self.armed.load(Ordering::Relaxed) != 0 {
            self.notifies.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self.armed.fetch_add(0, Ordering::Release) != 0 {
            self.notifies.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ── Scenario A ────────────────────────────────────────────────────────
// Core Dekker property, single waiter vs single waker, single shot.
// Waiter: arm → predicate check. Waker: data store → armed probe →
// conditional notify. If the waiter would suspend, the waker must have
// notified.

#[test]
fn loom_gate_a_no_lost_wake_single_waker() {
    // Smoke-check that loom actually explores interleavings.
    static ITERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    model(|| {
        ITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let gate = Arc::new(GateModel::new());
        let data = Arc::new(AtomicUsize::new(0));

        let (g, d) = (gate.clone(), data.clone());
        let waker = thread::spawn(move || {
            d.store(1, Ordering::Release); // A: publish data (ring-style)
            g.wake(); // B (RMW probe)
        });

        // Waiter (main thread): one slow-path pass of wait_until.
        gate.arm(); // C
        let would_suspend = data.load(Ordering::Acquire) == 0; // D
        waker.join().unwrap();

        if would_suspend {
            assert!(
                gate.notifies.load(Ordering::Relaxed) >= 1,
                "LOST WAKE: waiter suspends (predicate false after arming) \
                 but the waker skipped notify_one"
            );
        }
        gate.disarm();
        assert_eq!(gate.armed.load(Ordering::Relaxed), 0);
    });
    assert!(
        ITERS.load(std::sync::atomic::Ordering::Relaxed) >= 5,
        "loom explored too few interleavings — model may have degenerated",
    );
}

// ── Scenario B ────────────────────────────────────────────────────────
// Two independent wakers (any number of producers may call wake()).

#[test]
fn loom_gate_b_no_lost_wake_two_wakers() {
    model(|| {
        let gate = Arc::new(GateModel::new());
        let data = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let (g, d) = (gate.clone(), data.clone());
            handles.push(thread::spawn(move || {
                d.fetch_add(1, Ordering::Release);
                g.wake();
            }));
        }

        gate.arm();
        let would_suspend = data.load(Ordering::Acquire) == 0;
        for h in handles {
            h.join().unwrap();
        }

        if would_suspend {
            assert!(
                gate.notifies.load(Ordering::Relaxed) >= 1,
                "LOST WAKE with two concurrent wakers"
            );
        }
        gate.disarm();
    });
}

// ── Scenario C ────────────────────────────────────────────────────────
// Cancellation + re-arm lifecycle: the waiter arms, cancels WITHOUT
// checking (dropped wait future), then starts a fresh wait (arm → fence
// → check). The stale first cycle must not hide the waker from the
// second: the property still holds for the live wait.

#[test]
fn loom_gate_c_cancelled_wait_then_rearm() {
    model(|| {
        let gate = Arc::new(GateModel::new());
        let data = Arc::new(AtomicUsize::new(0));

        let (g, d) = (gate.clone(), data.clone());
        let waker = thread::spawn(move || {
            d.store(1, Ordering::Release);
            g.wake();
        });

        // Cancelled wait: arm then immediately disarm (future dropped
        // before its predicate check / await).
        gate.arm();
        gate.disarm();

        // Fresh wait cycle.
        gate.arm();
        let would_suspend = data.load(Ordering::Acquire) == 0;
        waker.join().unwrap();

        if would_suspend {
            assert!(
                gate.notifies.load(Ordering::Relaxed) >= 1,
                "LOST WAKE after a cancelled wait cycle"
            );
        }
        gate.disarm();
        assert_eq!(gate.armed.load(Ordering::Relaxed), 0);
    });
}

// ── Scenario D ────────────────────────────────────────────────────────
// Wake-then-exit lifecycle (stale-flag benignity + counter integrity):
// waiter completes a full arm → check(true-or-false) → disarm cycle
// concurrently with a waker. Whatever the interleaving, the counter
// returns to 0 and never underflows (fetch_sub on 0 would wrap and be
// caught by the == 0 assert on the next iteration's state).

#[test]
fn loom_gate_d_lifecycle_counter_integrity() {
    model(|| {
        let gate = Arc::new(GateModel::new());
        let data = Arc::new(AtomicUsize::new(0));

        let (g, d) = (gate.clone(), data.clone());
        let waker = thread::spawn(move || {
            d.store(1, Ordering::Release);
            g.wake();
        });

        // Full waiter pass mirroring wait_until's loop shape: fast-path
        // check, then armed slow-path check.
        if data.load(Ordering::Acquire) == 0 {
            gate.arm();
            let would_suspend = data.load(Ordering::Acquire) == 0;
            if would_suspend {
                // The real waiter would suspend here; the property from
                // scenario A guarantees a notify happened or will (the
                // waker hasn't run yet). Nothing further to model — the
                // suspended side is Notify's (verified) job.
            }
            gate.disarm();
        }

        waker.join().unwrap();
        assert_eq!(
            gate.armed.load(Ordering::Relaxed),
            0,
            "armed counter must return to 0 after any lifecycle"
        );
    });
}
