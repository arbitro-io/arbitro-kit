//! Measures the potential win of BYO-atomic (Signal reads head/tail directly)
//! vs the current Ring (Signal owns its own `locked: AtomicBool`).
//!
//! Both implementations are inlined in this file so we bench ONLY the hot path
//! difference. Same CAP, same cursor layout, same Dekker park protocol.
//!
//!   A. Actual `arbitro_kit::gate::Ring` — Signal has own `locked` bool.
//!      Producer: head.store + Signal::release() (2 atomic stores).
//!
//!   B. "BYO-atomic" mini-ring — no separate `locked`, park reads cursors.
//!      Producer: head.store only (1 atomic store) + wake if parked.
//!
//! Scenario: 2 threads (producer + consumer), pipelined. Measures total
//! throughput of 1000 round-trips × 5 runs.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, Thread};
use std::time::Instant;

use arbitro_kit::gate::Ring as ArbitroRing;

const CAP: usize = 256;
const ROUNDS: usize = 1000;
const WARMUP: usize = 100;
const RUNS: usize = 5;

// ════════════════════════════════════════════════════════════════════════════
// Variant B — "BYO-atomic" mini-ring
// ════════════════════════════════════════════════════════════════════════════
//
// Same structure as Ring but the park primitive reads head/tail directly
// instead of owning a `locked: AtomicBool`. This is what the refactor would
// produce if we decide to do it.

#[repr(align(64))]
struct Pad([u8; 0]);

struct ByoRing<T, const N: usize> {
    head: AtomicUsize,
    _pad0: Pad,
    tail: AtomicUsize,
    _pad1: Pad,

    // Consumer park state (for "not empty" wait).
    // No `locked` bool — state is `head != tail`.
    rx_parked: AtomicBool,
    rx_worker: UnsafeCell<Option<Thread>>,

    // Producer park state (for "not full" wait).
    // No `locked` bool — state is `head - tail < CAP`.
    tx_parked: AtomicBool,
    tx_worker: UnsafeCell<Option<Thread>>,

    slots: [UnsafeCell<MaybeUninit<T>>; N],
}

unsafe impl<T: Send, const N: usize> Send for ByoRing<T, N> {}
unsafe impl<T: Send, const N: usize> Sync for ByoRing<T, N> {}

const SPIN: u32 = 512;
const TIGHT: u32 = 64;

impl<T, const N: usize> ByoRing<T, N> {
    const MASK: usize = N - 1;

    fn new() -> Self {
        assert!(N.is_power_of_two());
        Self {
            head: AtomicUsize::new(0),
            _pad0: Pad([]),
            tail: AtomicUsize::new(0),
            _pad1: Pad([]),
            rx_parked: AtomicBool::new(false),
            rx_worker: UnsafeCell::new(None),
            tx_parked: AtomicBool::new(false),
            tx_worker: UnsafeCell::new(None),
            slots: std::array::from_fn(|_| UnsafeCell::new(MaybeUninit::uninit())),
        }
    }

    fn set_producer(&self, t: Thread) {
        unsafe { *self.tx_worker.get() = Some(t); }
    }
    fn set_consumer(&self, t: Thread) {
        unsafe { *self.rx_worker.get() = Some(t); }
    }

    #[inline]
    fn try_send(&self, value: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        if head.wrapping_sub(tail) >= N {
            return Err(value);
        }
        unsafe { (*self.slots[head & Self::MASK].get()).write(value); }
        // ── El único store ──
        self.head.store(head.wrapping_add(1), Ordering::Release);
        // ── Wake consumer si está parkeado — sin tocar ningún `locked` ──
        if self.rx_parked.load(Ordering::Relaxed) {
            unsafe {
                if let Some(t) = &*self.rx_worker.get() {
                    t.unpark();
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn try_recv(&self) -> Option<T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let v = unsafe { (*self.slots[tail & Self::MASK].get()).assume_init_read() };
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        if self.tx_parked.load(Ordering::Relaxed) {
            unsafe {
                if let Some(t) = &*self.tx_worker.get() {
                    t.unpark();
                }
            }
        }
        Some(v)
    }

    /// Block until at least one item is present.
    fn recv(&self) -> T {
        loop {
            if let Some(v) = self.try_recv() {
                return v;
            }
            // Tight spin.
            for _ in 0..TIGHT {
                if let Some(v) = self.try_recv() {
                    return v;
                }
                std::hint::black_box(());
            }
            // PAUSE spin.
            for _ in 0..SPIN {
                if let Some(v) = self.try_recv() {
                    return v;
                }
                std::hint::spin_loop();
            }
            // Park path — SeqCst barrier closes Dekker race.
            self.rx_parked.store(true, Ordering::SeqCst);
            if let Some(v) = self.try_recv() {
                self.rx_parked.store(false, Ordering::Relaxed);
                return v;
            }
            loop {
                thread::park();
                if let Some(v) = self.try_recv() {
                    self.rx_parked.store(false, Ordering::Relaxed);
                    return v;
                }
            }
        }
    }

    #[inline]
    fn has_space(&self) -> bool {
        let h = self.head.load(Ordering::Relaxed);
        let t = self.tail.load(Ordering::Acquire);
        h.wrapping_sub(t) < N
    }

    fn send(&self, mut value: T) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => value = v,
            }
            // Ring full. Spin-probe for space.
            let mut got_space = false;
            for _ in 0..TIGHT {
                if self.has_space() { got_space = true; break; }
                std::hint::black_box(());
            }
            if got_space { continue; }
            for _ in 0..SPIN {
                if self.has_space() { got_space = true; break; }
                std::hint::spin_loop();
            }
            if got_space { continue; }
            // Park.
            self.tx_parked.store(true, Ordering::SeqCst);
            if self.has_space() {
                self.tx_parked.store(false, Ordering::Relaxed);
                continue;
            }
            loop {
                thread::park();
                if self.has_space() {
                    self.tx_parked.store(false, Ordering::Relaxed);
                    break;
                }
            }
        }
    }
}

impl<T, const N: usize> Drop for ByoRing<T, N> {
    fn drop(&mut self) {
        let head = *self.head.get_mut();
        let tail = *self.tail.get_mut();
        for i in tail..head {
            unsafe { (*self.slots[i & Self::MASK].get()).assume_init_drop(); }
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Bench harness
// ════════════════════════════════════════════════════════════════════════════

struct Row {
    name: &'static str,
    ns_per_op_p50: u64,
    mops_s: f64,
}

fn header() {
    println!("\n{:<45} {:>14} {:>10}", "variant", "ns/op p50", "Mops/s");
    println!("{}", "─".repeat(71));
}
fn print_row(r: &Row) {
    println!("{:<45} {:>14} {:>10.2}", r.name, r.ns_per_op_p50, r.mops_s);
}
fn percentile(mut v: Vec<u64>, p: f64) -> u64 {
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * p) as usize]
}

// Variant A — actual arbitro_kit Ring.
fn run_arbitro_ring() -> Row {
    let mut per_op = Vec::with_capacity(RUNS);
    let mut mops = Vec::with_capacity(RUNS);

    for run_idx in 0..=RUNS {
        let is_warmup = run_idx == 0;
        let rounds = if is_warmup { WARMUP } else { ROUNDS };

        let ring: Arc<ArbitroRing<u64, CAP>> = Arc::new(ArbitroRing::new());
        let r = ring.clone();
        let consumer = thread::spawn(move || {
            r.set_consumer(thread::current());
            for _ in 0..rounds {
                let _ = r.recv();
            }
        });

        ring.set_producer(thread::current());
        let t0 = Instant::now();
        for i in 0..rounds {
            ring.send(i as u64);
        }
        consumer.join().unwrap();
        let el = t0.elapsed().as_nanos() as u64;

        if !is_warmup {
            per_op.push(el / rounds as u64);
            mops.push(rounds as f64 / (el as f64 / 1e9) / 1e6);
        }
    }
    Row {
        name: "A. arbitro_kit::Ring (Signal + locked)",
        ns_per_op_p50: percentile(per_op, 0.5),
        mops_s: mops.iter().sum::<f64>() / mops.len() as f64,
    }
}

// Variant B — BYO-atomic mini-ring.
fn run_byo_ring() -> Row {
    let mut per_op = Vec::with_capacity(RUNS);
    let mut mops = Vec::with_capacity(RUNS);

    for run_idx in 0..=RUNS {
        let is_warmup = run_idx == 0;
        let rounds = if is_warmup { WARMUP } else { ROUNDS };

        let ring: Arc<ByoRing<u64, CAP>> = Arc::new(ByoRing::new());
        let r = ring.clone();
        let consumer = thread::spawn(move || {
            r.set_consumer(thread::current());
            for _ in 0..rounds {
                let _ = r.recv();
            }
        });

        ring.set_producer(thread::current());
        let t0 = Instant::now();
        for i in 0..rounds {
            ring.send(i as u64);
        }
        consumer.join().unwrap();
        let el = t0.elapsed().as_nanos() as u64;

        if !is_warmup {
            per_op.push(el / rounds as u64);
            mops.push(rounds as f64 / (el as f64 / 1e9) / 1e6);
        }
    }
    Row {
        name: "B. BYO-atomic (head/tail is the state)",
        ns_per_op_p50: percentile(per_op, 0.5),
        mops_s: mops.iter().sum::<f64>() / mops.len() as f64,
    }
}

fn main() {
    println!("=== Ring: current Signal vs BYO-atomic head/tail ===");
    println!("CAP={CAP}  rounds={ROUNDS}  warmup={WARMUP}  runs={RUNS}");
    println!("Cross-thread pipelined producer → consumer, payload=u64");

    header();
    print_row(&run_arbitro_ring());
    print_row(&run_byo_ring());

    println!("\nDone.");
}
