//! `NotifyWaiter` — async `Waiter` impl built on `tokio::sync::Notify`.
//!
//! Available behind `feature = "tokio"`. Use this backend when the wake
//! fires from a non-tokio thread (TCP reader, FFI callback, OS-thread
//! worker) and the waiter is a tokio task — the runtime multiplexes the
//! wake onto a hot worker, beating `thread::unpark` to a cold pinned
//! thread by ~2.4× on real I/O round-trips (measured: TCP-loopback
//! release_primitive at P=128, ~8.2 µs vs ~20 µs).
//!
//! ## Wake gate (armed-flag fast path)
//!
//! `wake()` used to call `Notify::notify_one()` unconditionally — an
//! atomic RMW (SeqCst CAS on the `Notify` state word) on *every* ring
//! operation, ~100% wasted in saturated steady state where nobody is
//! waiting. It now mirrors [`ParkWaiter`](super::ParkWaiter)'s
//! parked-flag fast path: an `armed` counter is raised by the waiter
//! before it can possibly suspend, and `wake()` only touches the
//! `Notify` when the counter is non-zero. The no-waiter path is one
//! uncontended `Release` RMW on a line only the waker's peer rarely
//! writes — no `Notify` state CAS, no spurious permit, and no runtime
//! wakeups for a task that is not suspended.
//!
//! ### State machine
//!
//! `armed: AtomicUsize` — count of live [`GuardedNotified`] futures
//! (0 or 1 under the single-consumer contract; the counter, rather than
//! a bool, keeps overlapping futures safe by construction).
//!
//! | # | Actor  | Op | Transition | Ordering |
//! |---|--------|----|------------|----------|
//! | T1 | waiter | `WakeGate::notified()` | `armed.fetch_add(1)` | RMW `SeqCst` (≥ `Acquire` required — see proof) |
//! | T2 | waiter | `DisarmOnDrop::drop` | `armed.fetch_sub(1)` | RMW `SeqCst` — runs on **every** exit: predicate-true return, await completion, and cancellation (future dropped mid-await) |
//! | T3a | waker | `wake()` fast path | `armed.load(Relaxed)`; if `> 0` → `notify_one()`, done | positive-only: seeing armed can never lose a wake, so a possibly-stale load may take it |
//! | T3b | waker | `wake()` probe | `armed.fetch_add(0, Release)`; if previous `> 0` → `notify_one()` | RMW `Release` — the RMW-ness is the load-bearing part (see proof) |
//!
//! The protocol requires: T1 happens strictly *before* the waiter's
//! predicate re-check, and the guarded future is created *before* that
//! re-check as well (both are done inside `notified()` + the call-site
//! pattern `let n = gate.notified(); if ready() { return; } n.await;`).
//! T2 runs strictly *after* the waiter can no longer be suspended (drop
//! of the future), so "cleared too early" is impossible by construction.
//!
//! ### Happens-before chain — why no wake is ever lost
//!
//! This is the classic Dekker store→load race. It could be closed with
//! `SeqCst` fences on both sides, but a waker-side fence is an `mfence`
//! on every ring operation (measured: ~+8 ns/msg on the saturated
//! Ring2/tokio path). Instead the waker's flag *check* is itself an
//! atomic RMW, and two properties of RMWs close the race with no fence:
//!
//! 1. **RMWs cannot read stale values.** An atomic RMW reads the latest
//!    value in the object's modification order immediately preceding
//!    its own write (C++20 [atomics.order]p10, inherited by Rust). A
//!    plain load — even `SeqCst` — may be satisfied before the caller's
//!    older data store becomes globally visible (StoreLoad reordering);
//!    an RMW cannot be.
//! 2. **RMWs never break release sequences.** Every write to `armed`
//!    (arm, disarm, wake-probe) is an RMW, so an `Acquire` RMW that
//!    reads a value written anywhere after a `Release` RMW in the
//!    modification order synchronizes-with that `Release` RMW.
//!
//! ```text
//! waker  : A: data store (caller, e.g. ring head, ≥Relaxed)
//!          B0: armed.load(Relaxed)          // T3a: if non-zero → notify, done
//!          B: armed.fetch_add(0, Release)   // T3b: only if B0 read zero
//! waiter : C: armed.fetch_add(1, SeqCst)    // ≥ Acquire is the requirement
//!          D: predicate loads (e.g. ring cursors, Acquire)
//! ```
//!
//! B0 is a *positive-only* filter: if it happens to observe the armed
//! gate, `notify_one()` fires — always safe (waking a task that has
//! nothing to do is benign; data visibility on delivery is provided by
//! `Notify`'s own internal `SeqCst` synchronization, which the waiter's
//! post-wake predicate re-check rides on). A skipped notify can only
//! result from B0 *and* B both reading zero, and B cannot read a stale
//! zero — so the lost-wake analysis reduces to B vs C below.
//!
//! All writes to `armed` are RMWs, so B and C are totally ordered by
//! `armed`'s modification order (`<_mo`). Exactly one of:
//!
//! - `C <_mo B` and the matching disarm (T2) is not yet before B: by
//!   property 1, B reads `armed ≥ 1` → `notify_one()` fires. `Notify`
//!   then guarantees delivery: it either wakes the registered
//!   `Notified` or stores a permit that the waiter's already-created
//!   `Notified` consumes on its first/next poll.
//! - `C <_mo B` with the matching disarm also before B: the waiter
//!   already exited that wait cycle (returned, or was cancelled) — it
//!   is not suspended, so no wake is owed.
//! - `B <_mo C`: C reads the value written by B directly, or through
//!   the unbroken chain of the waiter's own intervening arm/disarm RMWs
//!   (property 2 — the release sequence headed by B survives the RMW
//!   chain). C is `Acquire`-or-stronger, so C synchronizes-with B, and
//!   A (sequenced before B) happens-before D (sequenced after C): the
//!   predicate sees the data and the waiter returns without suspending.
//!
//! Either way no interleaving exists where the waiter suspends with the
//! data unseen *and* the waker skips the notify. A stale `armed > 0`
//! (B lands between arm and a disarm whose waiter is about to exit)
//! only costs one spurious `notify_one`; the permit is consumed by the
//! next wait cycle, which re-checks the predicate — benign. The exact
//! protocol (same ops, same orderings) is model-checked exhaustively in
//! `tests/loom_notify_gate.rs`.
//!
//! ### Cancellation safety
//!
//! Tokio futures can be dropped at any await point. The disarm lives in
//! a drop guard *inside* the `GuardedNotified` future: while a task is
//! suspended on it, the guard is alive and `armed > 0`, so every
//! `wake()` reaches `notify_one`. The counter only drops when the
//! future is destroyed — at which point nobody can be suspended on it.
//! A cancelled wait therefore leaves `armed` back at 0 and, at worst, a
//! stored permit in the `Notify` (one benign spurious wake for the next
//! cycle). Lost wakes from cancellation are impossible by construction.
//!
//! ## Cost
//!
//! | Path                              |            Cost |
//! | --------------------------------- | --------------: |
//! | `wake()` no waiter                |   1 uncontended `Release` RMW |
//! | `wake()` waiter armed             |       ~300 ns (runtime enqueue) |
//! | `wait_until()` ready on entry     |          ~5 ns (predicate only) |
//! | `wait_until()` await round        |       ~300 ns |
//!
//! Without I/O in the path, [`ParkWaiter`](super::ParkWaiter) is faster
//! (~50 ns wake). Use this one specifically for the OS↔tokio bridge.
//!
//! ## Lost-notify race (Notify-level, pre-existing invariant)
//!
//! The `notified()` future is built BEFORE the predicate check. Without
//! that, a `notify_one` racing between the check and the await would be
//! lost — `Notify` only "remembers" a notification if a `notified()`
//! future was already registered (or a permit is stored) when it fired.
//! `WakeGate::notified()` preserves this: it arms the gate *and* builds
//! the inner `Notified` before the caller's predicate re-check.
//!
//! ## Concurrency contract
//!
//! - Any number of producers may call `wake()`.
//! - Exactly one consumer task calls `wait_until` and must be polled
//!   from a tokio runtime.
//! - `set_worker` is a no-op (the runtime tracks tasks itself).

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use tokio::sync::futures::Notified;
use tokio::sync::Notify;

use super::{AsyncWaiter, Waiter};

/// `Notify` + armed-waiter gate. Crate-internal: fast-path callers that
/// bypass the RPITIT `wait_until` (e.g. `Ring::recv_async_send`,
/// `Mpsc*::*_async_send`) call `.notified()` on this wrapper directly,
/// which arms the gate for them — keeping every notified-future creation
/// inside the wake-guard protocol.
#[derive(Default)]
pub(crate) struct WakeGate {
    notify: Notify,
    /// Number of live [`GuardedNotified`] futures. `wake()` skips the
    /// `notify_one` RMW entirely while this is 0.
    armed: AtomicUsize,
}

impl WakeGate {
    /// Arm the gate and build the notification future. Must be called
    /// BEFORE the caller's predicate re-check (see module docs — both
    /// the Dekker fence pairing and the Notify lost-notify invariant
    /// depend on this order).
    #[inline]
    pub(crate) fn notified(&self) -> GuardedNotified<'_> {
        // T1: publish "a waiter may suspend". `Acquire` is the proof's
        // requirement (synchronize with the waker's Release RMW when we
        // read from its release sequence); `SeqCst` is used for margin —
        // this runs only on the pre-suspend slow path.
        self.armed.fetch_add(1, Ordering::SeqCst);
        GuardedNotified {
            notified: self.notify.notified(),
            _disarm: DisarmOnDrop { armed: &self.armed },
        }
    }

    /// Fire the wake if (and only if) a waiter may be suspended.
    #[inline]
    pub(crate) fn wake(&self) {
        // T3a: positive-only fast path. A Relaxed load that SEES the
        // gate armed can just notify — firing notify_one is always safe
        // (delivery-side data visibility is Notify's own SeqCst
        // synchronization), and this keeps the armed-peer hot path at
        // exactly the pre-gate cost (no extra RMW). Staleness is only
        // dangerous in the 0 direction, which falls through to T3b.
        if self.armed.load(Ordering::Relaxed) != 0 {
            self.notify.notify_one();
            return;
        }
        // T3b: authoritative probe — an RMW, not a load. An RMW must
        // read the LATEST value in `armed`'s modification order — a
        // plain load (even SeqCst) could be satisfied before the
        // caller's data store became visible (StoreLoad reordering) and
        // miss a concurrent arm, losing the Dekker race. `Release`
        // heads a release sequence the waiter's arm acquires, ordering
        // the caller's data store before the waiter's predicate
        // re-check. See the happens-before chain in the module docs.
        if self.armed.fetch_add(0, Ordering::Release) != 0 {
            self.notify.notify_one();
        }
    }
}

/// Clears one `armed` count when dropped. Field of [`GuardedNotified`],
/// so it runs on every exit path — return-before-await, await
/// completion, and cancellation (future dropped mid-await).
struct DisarmOnDrop<'a> {
    armed: &'a AtomicUsize,
}

impl Drop for DisarmOnDrop<'_> {
    #[inline]
    fn drop(&mut self) {
        // T2: nobody can be suspended on the owning future anymore
        // (it is being destroyed). A waker reading a stale non-zero
        // value after this only issues a benign spurious notify.
        self.armed.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A [`Notified`] that keeps the wake gate armed for as long as it is
/// alive. Created by [`WakeGate::notified`].
pub(crate) struct GuardedNotified<'a> {
    notified: Notified<'a>,
    _disarm: DisarmOnDrop<'a>,
}

impl Future for GuardedNotified<'_> {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: structural pinning of `notified`. We never move it out
        // of `self`, never hand out `&mut Notified` otherwise, and the
        // struct has no `Drop` impl of its own that could move it (the
        // field drop glue runs in place).
        unsafe { self.map_unchecked_mut(|s| &mut s.notified) }.poll(cx)
    }
}

/// Async waiter — wraps [`tokio::sync::Notify`] behind an armed-waiter
/// gate (see module docs).
///
/// `Default` produces a fresh, disarmed gate. No registration step.
#[derive(Default)]
pub struct NotifyWaiter {
    pub(crate) inner: WakeGate,
}

impl Waiter for NotifyWaiter {
    /// No-op: tokio tracks tasks itself, no thread handle needed.
    #[inline]
    fn set_worker(&self, _thread: std::thread::Thread) {}

    /// Always `true`: the runtime is the worker.
    #[inline]
    fn has_worker(&self) -> bool {
        true
    }

    /// Wake the waiting consumer. Fast path (no waiter armed): one
    /// uncontended `Release` RMW — no `Notify` state CAS, no permit.
    #[inline]
    fn wake(&self) {
        self.inner.wake();
    }
}

impl AsyncWaiter for NotifyWaiter {
    // Mirror the trait's RPITIT signature verbatim (rather than `async fn`
    // sugar) so the `Send + 'a` bound stays explicit at the impl site.
    #[allow(clippy::manual_async_fn)]
    fn wait_until<'a, F>(&'a self, mut ready: F) -> impl Future<Output = ()> + Send + 'a
    where
        F: FnMut() -> bool + Send + 'a,
    {
        async move {
            // Fast path: predicate already true — no gate arming, no RMW.
            if ready() {
                return;
            }
            loop {
                // ARM the gate and build the notified() future BEFORE
                // re-checking the predicate. Order is load-bearing twice:
                // (1) Dekker — the armed store + fence must precede the
                //     predicate loads so a racing waker either sees the
                //     gate armed or we see its data (module docs);
                // (2) Notify — a `notify_one` racing between the check
                //     and the await must land on an already-created
                //     future (or its permit), not be dropped.
                let notified = self.inner.notified();
                if ready() {
                    // Guard drops here → gate disarmed.
                    return;
                }
                notified.await;
                // Guard dropped (gate disarmed). Re-check before paying
                // the re-arm RMW: after a genuine wake the predicate is
                // usually true.
                if ready() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn fast_path_already_ready() {
        let w = NotifyWaiter::default();
        w.wait_until(|| true).await;
        assert_eq!(w.inner.armed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wake_after_state_change_releases_awaiter() {
        let w = Arc::new(NotifyWaiter::default());
        let state = Arc::new(AtomicU64::new(0));
        let w2 = w.clone();
        let s = state.clone();
        let h = tokio::spawn(async move {
            w2.wait_until(move || s.load(Ordering::Acquire) != 0).await;
        });
        // Give the awaiter time to park.
        tokio::time::sleep(Duration::from_millis(20)).await;
        state.store(42, Ordering::Release);
        w.wake();
        h.await.unwrap();
        assert_eq!(state.load(Ordering::Relaxed), 42);
        assert_eq!(w.inner.armed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cross_thread_wake_from_os_thread() {
        // Intended use case: producer runs on a plain OS thread (no tokio
        // context), waiter is a tokio task.
        let w = Arc::new(NotifyWaiter::default());
        let state = Arc::new(AtomicU64::new(0));
        let w2 = w.clone();
        let s = state.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            s.store(7, Ordering::Release);
            w2.wake();
        });
        let s2 = state.clone();
        w.wait_until(move || s2.load(Ordering::Acquire) != 0).await;
        assert_eq!(state.load(Ordering::Relaxed), 7);
    }

    #[tokio::test]
    async fn wake_before_wait_no_deadlock() {
        // A wake with no armed waiter must be a harmless no-op: the next
        // wait re-checks the predicate before suspending, so no state is
        // needed from the early wake.
        let w = NotifyWaiter::default();
        let state = AtomicU64::new(0);
        state.store(1, Ordering::Release);
        w.wake(); // gate disarmed → skipped notify — must not poison anything
        tokio::time::timeout(
            Duration::from_secs(5),
            w.wait_until(|| state.load(Ordering::Acquire) != 0),
        )
        .await
        .expect("wait_until must observe pre-wake state without a wake");
        assert_eq!(w.inner.armed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wait_then_wake_releases() {
        // Plain arm → suspend → wake cycle, bounded by a timeout so a
        // lost wake is a test failure, not a hang.
        let w = Arc::new(NotifyWaiter::default());
        let state = Arc::new(AtomicU64::new(0));
        let (w2, s) = (w.clone(), state.clone());
        let h = tokio::spawn(async move {
            w2.wait_until(move || s.load(Ordering::Acquire) != 0).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        state.store(9, Ordering::Release);
        w.wake();
        tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .expect("waiter must be released by wake")
            .unwrap();
        assert_eq!(w.inner.armed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cancelled_wait_then_normal_wait_no_lost_wake() {
        let w = Arc::new(NotifyWaiter::default());
        let state = Arc::new(AtomicU64::new(0));

        // Phase 1: start a wait whose predicate never turns true, then
        // cancel it mid-await (tokio::select! drops the losing future).
        {
            let s = state.clone();
            let wait = w.wait_until(move || s.load(Ordering::Acquire) != 0);
            tokio::select! {
                _ = wait => panic!("predicate is false — wait must not complete"),
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
            // `wait` dropped here — cancellation mid-await.
        }
        // The drop guard must have disarmed the gate.
        assert_eq!(
            w.inner.armed.load(Ordering::SeqCst),
            0,
            "gate must disarm when the wait future is cancelled"
        );

        // Phase 2: a normal wait cycle after the cancellation must still
        // receive its wake (no lost-wake poisoning from phase 1).
        let (w2, s2) = (w.clone(), state.clone());
        let h = tokio::spawn(async move {
            w2.wait_until(move || s2.load(Ordering::Acquire) != 0).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        state.store(5, Ordering::Release);
        w.wake();
        tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .expect("post-cancellation wait must be released by wake")
            .unwrap();
        assert_eq!(w.inner.armed.load(Ordering::SeqCst), 0);
    }

    /// Tightest wake-dependency loop: two tasks alternate strictly, one
    /// wake per turn, 100k turns each way. If the gate ever loses a wake
    /// this deadlocks — the timeout turns that into a test failure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg_attr(miri, ignore)] // 200k await rounds — far too slow under Miri
    async fn rapid_ping_pong_100k() {
        const N: u64 = 100_000;
        let w_even = Arc::new(NotifyWaiter::default()); // wakes the "even" task
        let w_odd = Arc::new(NotifyWaiter::default()); // wakes the "odd" task
        let turn = Arc::new(AtomicU64::new(0));

        let (we, wo, t) = (w_even.clone(), w_odd.clone(), turn.clone());
        let even = tokio::spawn(async move {
            for i in 0..N {
                let want = 2 * i;
                we.wait_until(|| t.load(Ordering::Acquire) == want).await;
                t.store(want + 1, Ordering::Release);
                wo.wake();
            }
        });
        let (we2, wo2, t2) = (w_even.clone(), w_odd.clone(), turn.clone());
        let odd = tokio::spawn(async move {
            for i in 0..N {
                let want = 2 * i + 1;
                wo2.wait_until(|| t2.load(Ordering::Acquire) == want).await;
                t2.store(want + 1, Ordering::Release);
                we2.wake();
            }
        });

        tokio::time::timeout(Duration::from_secs(60), async {
            even.await.unwrap();
            odd.await.unwrap();
        })
        .await
        .expect("ping-pong deadlocked — lost wake in the gate protocol");
        assert_eq!(turn.load(Ordering::Relaxed), 2 * N);
        assert_eq!(w_even.inner.armed.load(Ordering::SeqCst), 0);
        assert_eq!(w_odd.inner.armed.load(Ordering::SeqCst), 0);
    }

    /// Same tightest-loop shape, but through the real consumer: a
    /// `Ring2<u32, 1, NotifyWaiter>` forces one not_full + one not_empty
    /// wake per item. Confirms the two independent gates (producer-side
    /// and consumer-side are separate `NotifyWaiter` instances) do not
    /// interfere.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg_attr(miri, ignore)] // 100k await rounds — far too slow under Miri
    async fn ring2_cap1_ping_pong_100k() {
        use crate::stream::Ring2;
        const N: u32 = 100_000;
        let (mut tx, mut rx) = Ring2::<u32, 1, NotifyWaiter>::new();
        // The RPITIT wait_until future cannot be proven `Send` through
        // `tokio::spawn` (rust-lang/rust#100013), so poll both halves
        // concurrently in one task via join! — every send still parks
        // on not_full until the recv side wakes it, and vice versa, so
        // a lost wake still deadlocks (caught by the timeout).
        let p = async {
            for i in 0..N {
                tx.send_async(i).await.expect("consumer alive");
            }
        };
        let c = async {
            for i in 0..N {
                assert_eq!(rx.recv_async().await, Some(i));
            }
        };
        tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(p, c);
        })
        .await
        .expect("Ring2 cap=1 ping-pong deadlocked — lost wake in the gate protocol");
    }
}
