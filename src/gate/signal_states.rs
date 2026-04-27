//! Experimental 4-state signal: UNPARKED / PENDING_PARK / PARKED / PENDING_UNPARK.
//!
//! Goal: producer hot path stays at ~0.8 ns (load + branch) when consumer
//! is awake. Pays CAS only when consumer is actually parking/parked.
//!
//! State machine:
//! ```
//!   UNPARKED ──spin fail──► PENDING_PARK ──commit──► PARKED
//!      ▲                          │                     │
//!      │                          │ work appeared       │ producer
//!      │                          ▼                     │ release
//!      └──────────────────── UNPARKED ◄── unpark ── PENDING_UNPARK
//! ```
//!
//! Single-writer rule: the consumer writes UNPARKED / PENDING_PARK / PARKED.
//! The producer writes PENDING_UNPARK. Transitions go through CAS so racing
//! producers don't double-unpark.
//!
//! Initial state: PENDING_PARK — forces one of the two sides to commit a
//! transition before any work happens, eliminating the "limbo" startup state.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU8, Ordering};

const UNPARKED:       u8 = 0;
const PENDING_PARK:   u8 = 1;
const PARKED:         u8 = 2;
const PENDING_UNPARK: u8 = 3;

const TIGHT_SPIN: u32 = 64;
const SPIN_ITERS: u32 = 512;

#[repr(align(64))]
pub struct SignalStates {
    state: AtomicU8,
    worker: UnsafeCell<Option<std::thread::Thread>>,
}

unsafe impl Sync for SignalStates {}

impl Default for SignalStates {
    fn default() -> Self { Self::new() }
}

impl SignalStates {
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(PENDING_PARK),
            worker: UnsafeCell::new(None),
        }
    }

    pub fn set_worker(&self, t: std::thread::Thread) {
        unsafe { *self.worker.get() = Some(t); }
    }

    /// Producer signals work.
    ///
    /// - Hot path (consumer UNPARKED): single Acquire load + branch.
    /// - Cold path (consumer PENDING_PARK or PARKED): CAS to PENDING_UNPARK,
    ///   call unpark only if previous was PARKED.
    #[inline]
    pub fn release(&self) {
        let mut s = self.state.load(Ordering::Acquire);
        loop {
            match s {
                UNPARKED | PENDING_UNPARK => return,
                PENDING_PARK => {
                    match self.state.compare_exchange(
                        PENDING_PARK, PENDING_UNPARK,
                        Ordering::AcqRel, Ordering::Acquire,
                    ) {
                        Ok(_)       => return,
                        Err(actual) => { s = actual; }
                    }
                }
                PARKED => {
                    match self.state.compare_exchange(
                        PARKED, PENDING_UNPARK,
                        Ordering::AcqRel, Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            unsafe {
                                if let Some(t) = &*self.worker.get() {
                                    t.unpark();
                                }
                            }
                            return;
                        }
                        Err(actual) => { s = actual; }
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    /// Consumer call: blocks until producer signals.
    ///
    /// On entry, state is either:
    /// - PENDING_PARK (initial or post-wake): we'll commit to PARKED if no work appears.
    /// - PENDING_UNPARK (producer fired before we entered): we transition to UNPARKED and return.
    /// - UNPARKED (work in progress): we just return.
    #[inline]
    pub fn acquire(&self) {
        // Outer: state-machine cycle.
        loop {
            let s = self.state.load(Ordering::Acquire);
            match s {
                PENDING_UNPARK => {
                    // Producer signaled — consume the signal and return.
                    self.state.store(UNPARKED, Ordering::Release);
                    return;
                }
                UNPARKED => return,  // already awake
                PENDING_PARK => {}    // proceed to spin then park
                PARKED => {
                    // Should not normally re-enter from PARKED without going
                    // through UNPARKED. Treat as PENDING_PARK for safety.
                }
                _ => unreachable!(),
            }

            // Inner: spin checking for PENDING_UNPARK.
            for _ in 0..TIGHT_SPIN {
                if self.state.load(Ordering::Acquire) == PENDING_UNPARK {
                    self.state.store(UNPARKED, Ordering::Release);
                    return;
                }
                std::hint::black_box(());
            }
            for _ in 0..SPIN_ITERS {
                if self.state.load(Ordering::Acquire) == PENDING_UNPARK {
                    self.state.store(UNPARKED, Ordering::Release);
                    return;
                }
                std::hint::spin_loop();
            }

            // Spin failed — commit to PARKED via CAS.
            match self.state.compare_exchange(
                PENDING_PARK, PARKED,
                Ordering::AcqRel, Ordering::Acquire,
            ) {
                Ok(_) => {
                    // We're committed. Park.
                    loop {
                        std::thread::park();
                        // On wake, state should be PENDING_UNPARK (producer's CAS).
                        let after = self.state.load(Ordering::Acquire);
                        if after == PENDING_UNPARK {
                            self.state.store(UNPARKED, Ordering::Release);
                            return;
                        }
                        // Spurious wake — re-park.
                    }
                }
                Err(PENDING_UNPARK) => {
                    // Producer beat us to it. Consume the signal.
                    self.state.store(UNPARKED, Ordering::Release);
                    return;
                }
                Err(_) => {
                    // State changed under us — restart outer loop.
                    continue;
                }
            }
        }
    }

    /// Consumer call: signal "I'm done draining, will park if no more work".
    /// Transitions UNPARKED → PENDING_PARK so the next acquire enters the
    /// commit path correctly.
    #[inline]
    pub fn lock(&self) {
        // CAS to handle the case where producer set PENDING_UNPARK between
        // our last load and now.
        let _ = self.state.compare_exchange(
            UNPARKED, PENDING_PARK,
            Ordering::AcqRel, Ordering::Acquire,
        );
        // If CAS failed (state was PENDING_UNPARK), leave it — acquire will
        // see it and consume.
    }

    #[inline]
    pub fn is_open(&self) -> bool {
        // "Open" = producer has signaled and consumer hasn't consumed yet.
        self.state.load(Ordering::Acquire) == PENDING_UNPARK
    }
}
