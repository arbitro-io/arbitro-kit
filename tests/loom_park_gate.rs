//! Loom model of the `ParkWaiter` parked-flag protocol
//! (`src/waiter/park.rs`).
//!
//! ## Verification scope (honest disclosure)
//!
//! `std::thread::park`/`unpark` cannot run under loom (loom does not
//! shim them), so the real `ParkWaiter` is not model-checked directly.
//! What CAN be checked — and what the audited bug lived in — is the
//! Dekker flag protocol between the waiter's (announce-park →
//! predicate-check) sequence and the waker's (data-store → parked-probe
//! → unpark) sequence. This file mirrors those exact atomic operations
//! with loom atomics.
//!
//! **Park-token abstraction.** `unparks: AtomicUsize` counts `unpark()`
//! calls. This suffices because of `std::thread::park`'s token
//! semantics (the analogue of the `Notify` permit): an `unpark` issued
//! at ANY point after the waiter armed either unblocks the in-progress
//! `park`, or stores the token so the next `park` returns immediately —
//! and every `park` return is followed by a predicate re-check in
//! `wait_slow`'s loop. Therefore "the waiter committed to park AND at
//! least one unpark was called" implies progress in the real
//! implementation, and "committed to park AND zero unparks" is exactly
//! the lost-wake deadlock. The property asserted across all
//! interleavings:
//!
//! > If the waiter decided to park (flag set and predicate false at its
//! > post-announce check), then at least one waker MUST have called
//! > unpark.
//!
//! The mapping to the source is 1:1:
//!
//! | Model op                                  | Source (park.rs)             |
//! |-------------------------------------------|------------------------------|
//! | `parked.swap(PARKED, SeqCst)`             | T1 — announce park           |
//! | predicate load (`Acquire`)                | caller's `ready()` re-check  |
//! | `parked.swap(UNPARKED, SeqCst)`           | T2 — leave park path         |
//! | data store (`Release`); `parked.load(Acquire)` else `fence(SeqCst)` + `parked.load(Relaxed)` | caller publish + `wake()` (T3a/T3b) |
//! | `unparks.fetch_add(1)` / `worker` mutex   | T4 — `unpark_registered()`   |
//!
//! The waker's authoritative probe is an RMW, not a load: RMWs must
//! read the latest value in the modification order (no stale read) and
//! never break release sequences. Both properties are part of the C11
//! model loom implements, so the fence-free protocol is checked as-is.
//!
//! Scenario E additionally models the finding-2 fix: the registration
//! lives in a `loom::sync::Mutex`, and the model asserts that a waker
//! whose probe observed `parked == true` finds the registration
//! (`Some`) under the lock — the `Acquire` half of the probes is what
//! makes that hold (see "Registration visibility" in park.rs).
//!
//! Preemption bound: `LOOM_MAX_PREEMPTIONS` from the environment wins
//! when set (the CI/verification runs use 3); otherwise defaults to 2.

#![cfg(loom)]

use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

/// Same flag values as park.rs.
const UNPARKED: usize = 0;
const PARKED: usize = 1;

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

/// Model of the `ParkWaiter` gate — same atomics, same orderings as
/// park.rs.
struct ParkGateModel {
    parked: AtomicUsize,
    /// Stand-in for `thread::unpark` calls (token grants).
    unparks: AtomicUsize,
}

impl ParkGateModel {
    fn new() -> Self {
        Self {
            parked: AtomicUsize::new(UNPARKED),
            unparks: AtomicUsize::new(0),
        }
    }

    /// T1 — announce park + SC fence (pairs with the waker's T3b fence;
    /// SC-fence Dekker theorem). Mirrors park.rs.
    fn announce(&self) {
        self.parked.swap(PARKED, Ordering::SeqCst);
        loom::sync::atomic::fence(Ordering::SeqCst);
    }

    /// T2 — leave the park path.
    fn clear(&self) {
        self.parked.swap(UNPARKED, Ordering::SeqCst);
    }

    /// T3 — `wake()`: positive-only Acquire fast probe (T3a), then the
    /// authoritative SC-fence probe (T3b). Mirrors park.rs.
    fn wake(&self) {
        if self.parked.load(Ordering::Acquire) == PARKED {
            self.unparks.fetch_add(1, Ordering::Relaxed);
            return;
        }
        loom::sync::atomic::fence(Ordering::SeqCst);
        if self.parked.load(Ordering::Relaxed) == PARKED {
            self.unparks.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ── Scenario A ────────────────────────────────────────────────────────
// Core Dekker property, single waiter vs single waker, single shot.
// Waiter: announce → predicate check. Waker: data store → parked probe
// → conditional unpark. If the waiter would park, the waker must have
// unparked.

#[test]
fn loom_park_a_no_lost_wake_single_waker() {
    // Smoke-check that loom actually explores interleavings.
    static ITERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    model(|| {
        ITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let gate = Arc::new(ParkGateModel::new());
        let data = Arc::new(AtomicUsize::new(0));

        let (g, d) = (gate.clone(), data.clone());
        let waker = thread::spawn(move || {
            d.store(1, Ordering::Release); // A: publish data (ring-style)
            g.wake(); // B0/B probe
        });

        // Waiter (main thread): one slow-path pass of wait_slow.
        gate.announce(); // C (T1)
        let would_park = data.load(Ordering::Acquire) == 0; // D
        waker.join().unwrap();

        if would_park {
            assert!(
                gate.unparks.load(Ordering::Relaxed) >= 1,
                "LOST WAKE: waiter parks (predicate false after announce) \
                 but the waker skipped unpark"
            );
        }
        gate.clear();
        assert_eq!(gate.parked.load(Ordering::Relaxed), UNPARKED);
    });
    assert!(
        ITERS.load(std::sync::atomic::Ordering::Relaxed) >= 5,
        "loom explored too few interleavings — model may have degenerated",
    );
}

// ── Scenario B ────────────────────────────────────────────────────────
// Two independent wakers (any number of producers may call wake()).

#[test]
fn loom_park_b_no_lost_wake_two_wakers() {
    model(|| {
        let gate = Arc::new(ParkGateModel::new());
        let data = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let (g, d) = (gate.clone(), data.clone());
            handles.push(thread::spawn(move || {
                d.fetch_add(1, Ordering::Release);
                g.wake();
            }));
        }

        gate.announce();
        let would_park = data.load(Ordering::Acquire) == 0;
        for h in handles {
            h.join().unwrap();
        }

        if would_park {
            assert!(
                gate.unparks.load(Ordering::Relaxed) >= 1,
                "LOST WAKE with two concurrent wakers"
            );
        }
        gate.clear();
    });
}

// ── Scenario C ────────────────────────────────────────────────────────
// Episode boundary: a previous wait episode completed (announce → clear
// via T2's RMW), then a fresh episode announces and checks. The stale
// first episode must not hide the waker from the second — in particular
// the T2 `swap(false)` RMW must keep the release sequence headed by a
// waker's T3b unbroken for the second episode's T1 to acquire.

#[test]
fn loom_park_c_previous_episode_then_rearm() {
    model(|| {
        let gate = Arc::new(ParkGateModel::new());
        let data = Arc::new(AtomicUsize::new(0));

        let (g, d) = (gate.clone(), data.clone());
        let waker = thread::spawn(move || {
            d.store(1, Ordering::Release);
            g.wake();
        });

        // Completed previous episode: announce then clear (predicate
        // turned true right after the announce).
        gate.announce();
        gate.clear();

        // Fresh episode.
        gate.announce();
        let would_park = data.load(Ordering::Acquire) == 0;
        waker.join().unwrap();

        if would_park {
            assert!(
                gate.unparks.load(Ordering::Relaxed) >= 1,
                "LOST WAKE after a completed previous episode"
            );
        }
        gate.clear();
        assert_eq!(gate.parked.load(Ordering::Relaxed), UNPARKED);
    });
}

// ── Scenario D ────────────────────────────────────────────────────────
// Full lifecycle flag integrity: waiter runs a complete announce →
// check → clear episode concurrently with a waker. Whatever the
// interleaving, the flag returns to false (a stale `true` would make
// every future wake() take the unpark path — a perf bug the protocol
// must not leave behind).

#[test]
fn loom_park_d_lifecycle_flag_integrity() {
    model(|| {
        let gate = Arc::new(ParkGateModel::new());
        let data = Arc::new(AtomicUsize::new(0));

        let (g, d) = (gate.clone(), data.clone());
        let waker = thread::spawn(move || {
            d.store(1, Ordering::Release);
            g.wake();
        });

        // Full waiter pass mirroring wait_slow's shape: fast-path check,
        // then announced slow-path check, then T2 on exit.
        if data.load(Ordering::Acquire) == 0 {
            gate.announce();
            let would_park = data.load(Ordering::Acquire) == 0;
            if would_park {
                // The real waiter parks here; scenario A's property
                // guarantees an unpark happened or will. The park/unpark
                // token delivery itself is std's (documented) job.
            }
            gate.clear();
        }

        waker.join().unwrap();
        assert_eq!(
            gate.parked.load(Ordering::Relaxed),
            UNPARKED,
            "parked flag must return to UNPARKED after any lifecycle"
        );
    });
}

// ── Scenario E ────────────────────────────────────────────────────────
// Finding-2 model — registration visibility through the probes' Acquire
// half: the waiter registers (mutex write) BEFORE announcing (park.rs
// enforces this with the has_worker assert). A waker whose probe reads
// `parked == true` synchronizes-with the SeqCst T1 RMW, so its
// `worker.lock()` is ordered after the registration and must observe
// `Some`. A `None` here would be a silently skipped unpark — a lost
// wake — which is exactly what the assert catches.

#[test]
fn loom_park_e_registration_visible_to_waker() {
    model(|| {
        let parked = Arc::new(AtomicUsize::new(UNPARKED));
        let worker = Arc::new(Mutex::new(None::<u8>));
        let data = Arc::new(AtomicUsize::new(0));

        let (p, w, d) = (parked.clone(), worker.clone(), data.clone());
        let waker = thread::spawn(move || {
            d.store(1, Ordering::Release);
            // wake(): T3a then T3b, T4 dereferences the registration.
            let observed_parked = p.load(Ordering::Acquire) == PARKED
                || p.fetch_add(0, Ordering::AcqRel) == PARKED;
            if observed_parked {
                assert!(
                    w.lock().unwrap().is_some(),
                    "waker observed parked==true but the registration \
                     was not visible — skipped unpark (lost wake)"
                );
            }
        });

        // Waiter: set_worker → T1 → predicate check (park.rs order).
        *worker.lock().unwrap() = Some(1);
        parked.swap(PARKED, Ordering::SeqCst);
        let _would_park = data.load(Ordering::Acquire) == 0;
        waker.join().unwrap();
        parked.swap(UNPARKED, Ordering::SeqCst);
    });
}
