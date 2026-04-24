//! SignalSet (current, M:1 wait-any) vs "SignalSet as factory of Signal<BitView>".
//!
//! **Scenario:** 1 producer fires N destinations round-robin. Each destination
//! has its own pending-work counter (so coalesced releases are not lost —
//! this mirrors how real queues behave: `release` is edge-triggered, data
//! tracking is the queue's job).
//!
//!   A. SignalSet (current):   1 worker, `acquire_any` + drain all N counters.
//!   B. Signal::from_bit:      N workers on shared AtomicU64 (1 cache line).
//!   C. N independent Signal:  N workers, each on its own AtomicBool (N lines).
//!
//! Reports total events / wall time → ns/event (p50 across RUNS) + Mev/s.
//! Bounded: 1000 events per run × 5 runs, warmup 100.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use arbitro_kit::gate::{BitView, OwnedBool, Signal, SignalSet};

const ROUNDS: usize = 1000;
const WARMUP: usize = 100;
const RUNS: usize = 5;

struct Row {
    name: &'static str,
    n: usize,
    p50_ns_per_event: u64,
    mev_s: f64,
}

fn header() {
    println!(
        "\n{:<40} {:>4} {:>14} {:>12}",
        "variant", "N", "ns/event p50", "Mev/s"
    );
    println!("{}", "─".repeat(74));
}
fn print_row(r: &Row) {
    println!(
        "{:<40} {:>4} {:>14} {:>12.2}",
        r.name, r.n, r.p50_ns_per_event, r.mev_s
    );
}

fn percentile(mut v: Vec<u64>, p: f64) -> u64 {
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * p) as usize]
}

// ─── A. SignalSet actual ─────────────────────────────────────────────────
fn run_signalset(n: usize) -> Row {
    let mut per_round = Vec::with_capacity(RUNS);
    let mut mevs = Vec::with_capacity(RUNS);

    for run_idx in 0..=RUNS {
        let is_warmup = run_idx == 0;
        let rounds = if is_warmup { WARMUP } else { ROUNDS };
        let total = rounds * n;

        let mut set = SignalSet::new();
        let ids: Vec<_> = (0..n)
            .map(|i| set.create(Box::leak(format!("p{i}").into_boxed_str())))
            .collect();
        let set = Arc::new(set);
        let mask = if n == 64 { !0u64 } else { (1u64 << n) - 1 };
        let produced: Arc<Vec<AtomicUsize>> =
            Arc::new((0..n).map(|_| AtomicUsize::new(0)).collect());
        let consumed: Arc<Vec<AtomicUsize>> =
            Arc::new((0..n).map(|_| AtomicUsize::new(0)).collect());
        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(2));

        let s = set.clone();
        let p = produced.clone();
        let c = consumed.clone();
        let st = stop.clone();
        let b = barrier.clone();
        let consumer = thread::spawn(move || {
            s.set_worker(thread::current());
            b.wait();
            while !st.load(Ordering::Relaxed) {
                s.acquire_any(mask);
                let bits = s.state() & mask;
                s.lock_mask(bits);
                // Drain ALL counters (not just signaled bits — producer
                // may have incremented after we observed `bits`).
                for i in 0..n {
                    let pr = p[i].load(Ordering::Acquire);
                    let cn = c[i].load(Ordering::Relaxed);
                    if pr > cn {
                        c[i].store(pr, Ordering::Release);
                    }
                }
            }
        });

        barrier.wait();
        let t0 = Instant::now();
        for i in 0..total {
            let idx = i % n;
            produced[idx].fetch_add(1, Ordering::Release);
            set.release(ids[idx]);
        }
        // Wait for total consumed across all destinations.
        loop {
            let sum: usize = consumed.iter().map(|a| a.load(Ordering::Acquire)).sum();
            if sum >= total {
                break;
            }
            thread::yield_now();
        }
        let el = t0.elapsed().as_nanos() as u64;
        stop.store(true, Ordering::Relaxed);
        for id in &ids {
            set.release(*id);
        }
        consumer.join().unwrap();

        if !is_warmup {
            per_round.push(el / total as u64);
            mevs.push(total as f64 / (el as f64 / 1e9) / 1e6);
        }
    }
    Row {
        name: "A. SignalSet actual (acquire_any)",
        n,
        p50_ns_per_event: percentile(per_round, 0.5),
        mev_s: mevs.iter().sum::<f64>() / mevs.len() as f64,
    }
}

// ─── B. Signal::from_bit factory (N workers, shared u64) ──────────────────
fn run_signal_from_bit(n: usize) -> Row {
    let mut per_round = Vec::with_capacity(RUNS);
    let mut mevs = Vec::with_capacity(RUNS);

    for run_idx in 0..=RUNS {
        let is_warmup = run_idx == 0;
        let rounds = if is_warmup { WARMUP } else { ROUNDS };
        let total = rounds * n;

        // Leak for 'static → Send across threads.
        let state: &'static AtomicU64 = Box::leak(Box::new(AtomicU64::new(0)));
        let signals: Arc<Vec<Signal<BitView<'static>>>> =
            Arc::new((0..n).map(|i| Signal::from_bit(state, i as u8)).collect());
        let produced: Arc<Vec<AtomicUsize>> =
            Arc::new((0..n).map(|_| AtomicUsize::new(0)).collect());
        let consumed: Arc<Vec<AtomicUsize>> =
            Arc::new((0..n).map(|_| AtomicUsize::new(0)).collect());
        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(n + 1));

        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let sigs = signals.clone();
            let p = produced.clone();
            let c = consumed.clone();
            let st = stop.clone();
            let b = barrier.clone();
            handles.push(thread::spawn(move || {
                sigs[i].set_worker(thread::current());
                b.wait();
                while !st.load(Ordering::Relaxed) {
                    sigs[i].acquire();
                    sigs[i].lock();
                    let pr = p[i].load(Ordering::Acquire);
                    let cn = c[i].load(Ordering::Relaxed);
                    if pr > cn {
                        c[i].store(pr, Ordering::Release);
                    }
                }
            }));
        }

        barrier.wait();
        let t0 = Instant::now();
        for i in 0..total {
            let idx = i % n;
            produced[idx].fetch_add(1, Ordering::Release);
            signals[idx].release();
        }
        loop {
            let sum: usize = consumed.iter().map(|a| a.load(Ordering::Acquire)).sum();
            if sum >= total {
                break;
            }
            thread::yield_now();
        }
        let el = t0.elapsed().as_nanos() as u64;
        stop.store(true, Ordering::Relaxed);
        for s in signals.iter() {
            s.release();
        }
        for h in handles {
            h.join().unwrap();
        }

        if !is_warmup {
            per_round.push(el / total as u64);
            mevs.push(total as f64 / (el as f64 / 1e9) / 1e6);
        }
    }
    Row {
        name: "B. Signal::from_bit factory (1 line)",
        n,
        p50_ns_per_event: percentile(per_round, 0.5),
        mev_s: mevs.iter().sum::<f64>() / mevs.len() as f64,
    }
}

// ─── C. N independent Signal (N cache lines) ─────────────────────────────
fn run_n_signals(n: usize) -> Row {
    let mut per_round = Vec::with_capacity(RUNS);
    let mut mevs = Vec::with_capacity(RUNS);

    for run_idx in 0..=RUNS {
        let is_warmup = run_idx == 0;
        let rounds = if is_warmup { WARMUP } else { ROUNDS };
        let total = rounds * n;

        let signals: Arc<Vec<Signal<OwnedBool>>> =
            Arc::new((0..n).map(|_| Signal::new()).collect());
        let produced: Arc<Vec<AtomicUsize>> =
            Arc::new((0..n).map(|_| AtomicUsize::new(0)).collect());
        let consumed: Arc<Vec<AtomicUsize>> =
            Arc::new((0..n).map(|_| AtomicUsize::new(0)).collect());
        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(n + 1));

        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let sigs = signals.clone();
            let p = produced.clone();
            let c = consumed.clone();
            let st = stop.clone();
            let b = barrier.clone();
            handles.push(thread::spawn(move || {
                sigs[i].set_worker(thread::current());
                b.wait();
                while !st.load(Ordering::Relaxed) {
                    sigs[i].acquire();
                    sigs[i].lock();
                    let pr = p[i].load(Ordering::Acquire);
                    let cn = c[i].load(Ordering::Relaxed);
                    if pr > cn {
                        c[i].store(pr, Ordering::Release);
                    }
                }
            }));
        }

        barrier.wait();
        let t0 = Instant::now();
        for i in 0..total {
            let idx = i % n;
            produced[idx].fetch_add(1, Ordering::Release);
            signals[idx].release();
        }
        loop {
            let sum: usize = consumed.iter().map(|a| a.load(Ordering::Acquire)).sum();
            if sum >= total {
                break;
            }
            thread::yield_now();
        }
        let el = t0.elapsed().as_nanos() as u64;
        stop.store(true, Ordering::Relaxed);
        for s in signals.iter() {
            s.release();
        }
        for h in handles {
            h.join().unwrap();
        }

        if !is_warmup {
            per_round.push(el / total as u64);
            mevs.push(total as f64 / (el as f64 / 1e9) / 1e6);
        }
    }
    Row {
        name: "C. N Signals independientes (N lines)",
        n,
        p50_ns_per_event: percentile(per_round, 0.5),
        mev_s: mevs.iter().sum::<f64>() / mevs.len() as f64,
    }
}

fn main() {
    println!("=== SignalSet (M:1 wait-any) vs Signal::from_bit factory (N M:1) ===");
    println!("rounds={ROUNDS}  warmup={WARMUP}  runs={RUNS}");
    println!("1 productor round-robin → N destinos con contador por destino\n");

    for &n in &[2usize, 4, 8, 16] {
        header();
        print_row(&run_signalset(n));
        print_row(&run_signal_from_bit(n));
        print_row(&run_n_signals(n));
    }

    println!("\nDone.");
}
