//! Raw cost comparison: `SignalSet` (one shared AtomicU64, N bits)
//! vs `N` independent `Signal` instances (N separate AtomicBool lines).
//!
//! Answers the question: is SignalSet actually cheaper than N Signals in
//! the hot path, or is its single shared cache line a contention trap?
//!
//! Scenarios:
//!   A. Uncontended release — single producer fires own bit/signal in a
//!      tight loop. Measures the intrinsic cost of one release op.
//!   B. M-producer contention — M threads each fire their own bit/signal
//!      as fast as possible, no consumer. Measures how badly the shared
//!      cache line serializes under contention.
//!   C. Realistic release+wake — one consumer polls; producers release.
//!      Covers the path that Mpmc actually uses.
//!
//! Each scenario runs for a fixed wall-clock time and reports the total
//! number of releases issued (aggregate and per-producer).

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use arbitro_kit::gate::{Signal, SignalSet};

const WALL_MS: u64 = 500;
const WARMUP_MS: u64 = 100;

fn header(title: &str) {
    println!("\n── {} ──", title);
    println!(
        "{:<38} {:>10} {:>16} {:>14} {:>12}",
        "variant", "M prods", "total releases", "ns/release", "Mops/s"
    );
    println!("{}", "─".repeat(94));
}

fn row(name: &str, m: usize, total: u64, wall_ns: u64) {
    let ns_per = wall_ns as f64 / total as f64;
    let mops = (total as f64) / (wall_ns as f64 / 1e9) / 1e6;
    println!(
        "{:<38} {:>10} {:>16} {:>14.2} {:>12.2}",
        name, m, total, ns_per, mops
    );
}

// ─── A. Uncontended release ────────────────────────────────────────────────

fn uncontended_signalset() -> (u64, u64) {
    let mut set = SignalSet::new();
    let id = set.create("p0");
    let set = Arc::new(set);
    // Warmup.
    let t0 = Instant::now();
    let mut count: u64 = 0;
    while t0.elapsed().as_millis() < WARMUP_MS as u128 {
        set.release(id);
        set.lock(id);
        count += 1;
    }
    let _ = count;
    // Measurement.
    let mut total: u64 = 0;
    let t0 = Instant::now();
    let deadline = Duration::from_millis(WALL_MS);
    while t0.elapsed() < deadline {
        for _ in 0..1024 {
            set.release(id);
            set.lock(id);
        }
        total += 1024;
    }
    (total, t0.elapsed().as_nanos() as u64)
}

fn uncontended_signal() -> (u64, u64) {
    let sig = Arc::new(Signal::new());
    let t0 = Instant::now();
    let mut count: u64 = 0;
    while t0.elapsed().as_millis() < WARMUP_MS as u128 {
        sig.release();
        sig.lock();
        count += 1;
    }
    let _ = count;
    let mut total: u64 = 0;
    let t0 = Instant::now();
    let deadline = Duration::from_millis(WALL_MS);
    while t0.elapsed() < deadline {
        for _ in 0..1024 {
            sig.release();
            sig.lock();
        }
        total += 1024;
    }
    (total, t0.elapsed().as_nanos() as u64)
}

// ─── B. M producers contend (no consumer, just release spam) ──────────────
//
// We do not `lock()` between releases here — we want to stress the write
// side of the cache line. Producers just keep issuing releases. Consumer
// thread continuously drains the state (for SignalSet) or loads each
// Signal's state (for N Signals) so neither side saturates into a
// steady-state where the store commits are free.

fn contended_signalset(m: usize) -> (u64, u64) {
    let mut set = SignalSet::new();
    let ids: Vec<_> = (0..m)
        .map(|i| set.create(Box::leak(format!("p{i}").into_boxed_str()) as &'static str))
        .collect();
    let set = Arc::new(set);
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(m + 1));
    let counts: Vec<_> = (0..m).map(|_| Arc::new(AtomicUsize::new(0))).collect();

    let mut handles = Vec::new();
    for p in 0..m {
        let set = set.clone();
        let id = ids[p];
        let stop = stop.clone();
        let b = barrier.clone();
        let cnt = counts[p].clone();
        handles.push(thread::spawn(move || {
            b.wait();
            let mut c = 0usize;
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..1024 {
                    set.release(id);
                }
                c += 1024;
                // Drain this producer's bit so subsequent releases don't
                // degenerate into no-op OR-into-already-set.
                set.lock(id);
            }
            cnt.store(c, Ordering::Relaxed);
        }));
    }

    barrier.wait();
    let t0 = Instant::now();
    thread::sleep(Duration::from_millis(WALL_MS));
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }
    let total: u64 = counts.iter().map(|c| c.load(Ordering::Relaxed) as u64).sum();
    (total, t0.elapsed().as_nanos() as u64)
}

fn contended_signals(m: usize) -> (u64, u64) {
    let signals: Arc<Vec<Signal>> = Arc::new((0..m).map(|_| Signal::new()).collect());
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(m + 1));
    let counts: Vec<_> = (0..m).map(|_| Arc::new(AtomicUsize::new(0))).collect();

    let mut handles = Vec::new();
    for p in 0..m {
        let signals = signals.clone();
        let stop = stop.clone();
        let b = barrier.clone();
        let cnt = counts[p].clone();
        handles.push(thread::spawn(move || {
            b.wait();
            let mut c = 0usize;
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..1024 {
                    signals[p].release();
                }
                c += 1024;
                signals[p].lock();
            }
            cnt.store(c, Ordering::Relaxed);
        }));
    }

    barrier.wait();
    let t0 = Instant::now();
    thread::sleep(Duration::from_millis(WALL_MS));
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }
    let total: u64 = counts.iter().map(|c| c.load(Ordering::Relaxed) as u64).sum();
    (total, t0.elapsed().as_nanos() as u64)
}

// ─── C. Realistic release + wake path ─────────────────────────────────────
//
// One consumer that actually parks when idle; M producers release their
// bit/signal. Consumer uses `acquire_any` (SignalSet) or, for N Signals,
// a busy-spin scan because Signal has no native "wait on any".
//
// The N-Signal case is intentionally favorable to Signals (no park cost
// for consumer — it spins). If SignalSet still wins at M=1, it's truly
// more efficient. If Signals win at higher M, the shared cache line is
// the bottleneck.

fn realistic_signalset(m: usize) -> (u64, u64) {
    let mut set = SignalSet::new();
    let ids: Vec<_> = (0..m)
        .map(|i| set.create(Box::leak(format!("p{i}").into_boxed_str()) as &'static str))
        .collect();
    let set = Arc::new(set);
    let mask: u64 = if m == 64 { !0 } else { (1u64 << m) - 1 };
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(m + 2));
    let counts: Vec<_> = (0..m).map(|_| Arc::new(AtomicUsize::new(0))).collect();

    // Consumer: drain bits, loop.
    let consumer = {
        let set = set.clone();
        let stop = stop.clone();
        let b = barrier.clone();
        thread::spawn(move || {
            set.set_worker(thread::current());
            b.wait();
            while !stop.load(Ordering::Relaxed) {
                let st = set.state() & mask;
                if st == 0 {
                    // avoid park: spin briefly then re-check
                    for _ in 0..64 { std::hint::spin_loop(); }
                    continue;
                }
                set.lock_mask(st);
            }
        })
    };

    let mut handles = Vec::new();
    for p in 0..m {
        let set = set.clone();
        let id = ids[p];
        let stop = stop.clone();
        let b = barrier.clone();
        let cnt = counts[p].clone();
        handles.push(thread::spawn(move || {
            b.wait();
            let mut c = 0usize;
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..1024 { set.release(id); }
                c += 1024;
            }
            cnt.store(c, Ordering::Relaxed);
        }));
    }

    barrier.wait();
    let t0 = Instant::now();
    thread::sleep(Duration::from_millis(WALL_MS));
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }
    consumer.join().unwrap();
    let total: u64 = counts.iter().map(|c| c.load(Ordering::Relaxed) as u64).sum();
    (total, t0.elapsed().as_nanos() as u64)
}

fn realistic_signals(m: usize) -> (u64, u64) {
    let signals: Arc<Vec<Signal>> = Arc::new((0..m).map(|_| Signal::new()).collect());
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(m + 2));
    let counts: Vec<_> = (0..m).map(|_| Arc::new(AtomicUsize::new(0))).collect();

    // Consumer busy-scans N Signals (no native wait-on-any for raw Signals).
    let consumer = {
        let signals = signals.clone();
        let stop = stop.clone();
        let b = barrier.clone();
        thread::spawn(move || {
            b.wait();
            while !stop.load(Ordering::Relaxed) {
                for i in 0..m {
                    if signals[i].is_open() {
                        signals[i].lock();
                    }
                }
                std::hint::spin_loop();
            }
        })
    };

    let mut handles = Vec::new();
    for p in 0..m {
        let signals = signals.clone();
        let stop = stop.clone();
        let b = barrier.clone();
        let cnt = counts[p].clone();
        handles.push(thread::spawn(move || {
            b.wait();
            let mut c = 0usize;
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..1024 { signals[p].release(); }
                c += 1024;
            }
            cnt.store(c, Ordering::Relaxed);
        }));
    }

    barrier.wait();
    let t0 = Instant::now();
    thread::sleep(Duration::from_millis(WALL_MS));
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }
    consumer.join().unwrap();
    let total: u64 = counts.iter().map(|c| c.load(Ordering::Relaxed) as u64).sum();
    (total, t0.elapsed().as_nanos() as u64)
}

// ─── D. Raw atomic baseline (no arbitro-kit wrapper) ──────────────────────
//
// Sanity line: what's the hardware floor? If SignalSet/Signal are far
// above this, it's our wrapper logic; if close, it's physical.

fn raw_atomic_u64_fetch_or(m: usize) -> (u64, u64) {
    let state = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(m + 1));
    let counts: Vec<_> = (0..m).map(|_| Arc::new(AtomicUsize::new(0))).collect();

    let mut handles = Vec::new();
    for p in 0..m {
        let state = state.clone();
        let bit = 1u64 << p;
        let stop = stop.clone();
        let b = barrier.clone();
        let cnt = counts[p].clone();
        handles.push(thread::spawn(move || {
            b.wait();
            let mut c = 0usize;
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..1024 {
                    state.fetch_or(bit, Ordering::Release);
                }
                c += 1024;
                state.fetch_and(!bit, Ordering::Release);
            }
            cnt.store(c, Ordering::Relaxed);
        }));
    }

    barrier.wait();
    let t0 = Instant::now();
    thread::sleep(Duration::from_millis(WALL_MS));
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }
    let total: u64 = counts.iter().map(|c| c.load(Ordering::Relaxed) as u64).sum();
    (total, t0.elapsed().as_nanos() as u64)
}

fn raw_atomic_bool_store(m: usize) -> (u64, u64) {
    let flags: Arc<Vec<AtomicBool>> = Arc::new((0..m).map(|_| AtomicBool::new(false)).collect());
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(m + 1));
    let counts: Vec<_> = (0..m).map(|_| Arc::new(AtomicUsize::new(0))).collect();

    let mut handles = Vec::new();
    for p in 0..m {
        let flags = flags.clone();
        let stop = stop.clone();
        let b = barrier.clone();
        let cnt = counts[p].clone();
        handles.push(thread::spawn(move || {
            b.wait();
            let mut c = 0usize;
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..1024 {
                    flags[p].store(true, Ordering::Release);
                }
                c += 1024;
                flags[p].store(false, Ordering::Release);
            }
            cnt.store(c, Ordering::Relaxed);
        }));
    }

    barrier.wait();
    let t0 = Instant::now();
    thread::sleep(Duration::from_millis(WALL_MS));
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }
    let total: u64 = counts.iter().map(|c| c.load(Ordering::Relaxed) as u64).sum();
    (total, t0.elapsed().as_nanos() as u64)
}

fn main() {
    println!("=== SignalSet (shared AtomicU64) vs N separate Signals ===");
    println!("wall={WALL_MS}ms per scenario, warmup={WARMUP_MS}ms");

    header("A. Uncontended release (single thread, release + lock)");
    let (t, w) = uncontended_signalset();
    row("SignalSet 1-bit", 1, t, w);
    let (t, w) = uncontended_signal();
    row("Signal", 1, t, w);

    header("B. M-producer contention (no consumer, just release spam)");
    for &m in &[1usize, 2, 4, 8] {
        let (t, w) = contended_signalset(m);
        row("SignalSet (shared u64)", m, t, w);
        let (t, w) = contended_signals(m);
        row("N Signals (separate lines)", m, t, w);
    }

    header("C. Realistic release + consumer-drain loop");
    for &m in &[1usize, 2, 4, 8] {
        let (t, w) = realistic_signalset(m);
        row("SignalSet + acquire_any-style", m, t, w);
        let (t, w) = realistic_signals(m);
        row("N Signals + busy-scan", m, t, w);
    }

    header("D. Raw atomic floor (no arbitro wrapper)");
    for &m in &[1usize, 2, 4, 8] {
        let (t, w) = raw_atomic_u64_fetch_or(m);
        row("AtomicU64 fetch_or (shared)", m, t, w);
        let (t, w) = raw_atomic_bool_store(m);
        row("AtomicBool store (N lines)", m, t, w);
    }

    println!("\nDone.");
}
