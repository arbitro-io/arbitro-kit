//! Synapse overhead bench — SPMC 1 producer → N consumers.
//!
//! Thesis: Synapse is the structural dual of Hub, built on the same
//! `Signal` park protocol. Consumer park/unpark matches Channel's sub-110 ns
//! cross-core floor; the CAS-claim on `tail` adds ~20–30 ns under low
//! contention. We expect to beat `crossbeam-deque`'s Worker/Stealer on
//! cross-thread steal (~150–200 ns) by ≥1.5×.
//!
//! Sections:
//!   A. Single-thread send+recv (no cross-thread wake) — isolates hot path.
//!   B. 1P / 1C cross-thread RTT — sanity vs Ring.
//!   C. 1P / NC cross-thread aggregate throughput (N = 2, 4, 8).
//!   D. vs crossbeam-deque (same 1P/NC workload).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::synapse::Synapse;
use crossbeam_deque::{Injector, Steal, Worker};

const BATCH: usize = 1000;

fn rounds() -> usize {
    std::env::var("BENCH_ROUNDS").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(500)
}
fn warmup_batches() -> usize { 10 }

fn header(title: &str) {
    println!("\n── {} ──", title);
    println!("{:<38} {:>12} {:>12} {:>12} {:>14}",
             "variant", "mean_ns/op", "p50_ns/op", "p99_ns/op", "ops/sec");
    println!("{}", "─".repeat(90));
}
fn row(name: &str, mut batch_ns: Vec<u64>, total_elapsed_ns: u64) {
    batch_ns.sort_unstable();
    let samples = batch_ns.len();
    let total_ops = samples * BATCH;
    let ops = (total_ops as f64) / (total_elapsed_ns as f64 / 1e9);
    let mean = total_elapsed_ns as f64 / total_ops as f64;
    let p50 = batch_ns[samples / 2] as f64 / BATCH as f64;
    let p99 = batch_ns[samples * 99 / 100] as f64 / BATCH as f64;
    println!("{:<38} {:>12.2} {:>12.2} {:>12.2} {:>14}",
             name, mean, p50, p99, ops as u64);
}

// ── A. Single-thread: producer pushes, same thread pops ─────────────────
// Pure hot-path cost; no park, no cross-core coherence traffic. The CAS
// on tail still executes but always succeeds on first try.
fn bench_single_thread() {
    let s: Synapse<u64, 256, 1> = Synapse::new();
    s.bind_consumer(0);

    let do_batch = || {
        for k in 0..BATCH as u64 {
            s.try_send(k).unwrap();
            std::hint::black_box(s.try_recv().unwrap());
        }
    };
    for _ in 0..warmup_batches() { do_batch(); }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let t_wall = Instant::now();
    for _ in 0..n {
        let t0 = Instant::now();
        do_batch();
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row("synapse 1P/1C single-thread", lats, t_wall.elapsed().as_nanos() as u64);
}

// ── B. 1P / 1C cross-thread RTT-style (bounded ring, flow-controlled) ────
fn bench_spsc_cross_thread() {
    let s: Arc<Synapse<u64, 256, 1>> = Arc::new(Synapse::new());
    let stop = Arc::new(AtomicBool::new(false));

    let s_c = s.clone();
    let stop_c = stop.clone();
    let consumer = thread::spawn(move || {
        s_c.bind_consumer(0);
        let mut count: u64 = 0;
        while !stop_c.load(Ordering::Relaxed) {
            match s_c.recv(0) {
                Ok(v) => { std::hint::black_box(v); count = count.wrapping_add(1); }
                Err(_) => break,
            }
        }
        count
    });

    s.set_producer(thread::current());
    // warmup
    for _ in 0..warmup_batches() {
        for k in 0..BATCH as u64 { s.send(k); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let t_wall = Instant::now();
    for _ in 0..n {
        let t0 = Instant::now();
        for k in 0..BATCH as u64 { s.send(k); }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row("synapse 1P/1C cross-thread", lats, t_wall.elapsed().as_nanos() as u64);

    // drain + stop
    stop.store(true, Ordering::Relaxed);
    s.shutdown();
    let _ = consumer.join().unwrap();
}

// ── C. 1P / NC cross-thread — aggregate throughput ──────────────────────
fn bench_spmc_cross_thread<const N: usize>(label: &str) {
    let s: Arc<Synapse<u64, 1024, N>> = Arc::new(Synapse::new());

    // Spawn N consumers. Each loops on recv until Shutdown.
    let handles: Vec<_> = (0..N).map(|i| {
        let s = s.clone();
        thread::spawn(move || {
            s.bind_consumer(i);
            let mut count: u64 = 0;
            loop {
                match s.recv(i) {
                    Ok(v) => { std::hint::black_box(v); count += 1; }
                    Err(_) => break,
                }
            }
            count
        })
    }).collect();

    s.set_producer(thread::current());
    // warmup
    for _ in 0..warmup_batches() {
        for k in 0..BATCH as u64 { s.send(k); }
    }

    let n = rounds();
    let total_ops = n * BATCH;
    let mut lats = Vec::with_capacity(n);
    let t_wall = Instant::now();
    for _ in 0..n {
        let t0 = Instant::now();
        for k in 0..BATCH as u64 { s.send(k); }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let wall_ns = t_wall.elapsed().as_nanos() as u64;
    row(label, lats, wall_ns);

    s.shutdown();
    let counts: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let total_received: u64 = counts.iter().sum();
    println!("    per-consumer receipts: {:?} (total {}, sent {})",
             counts, total_received, total_ops);
}

// ── D. crossbeam-deque comparison ────────────────────────────────────────
// Shape: one Injector (global queue) + N Workers (one per consumer thread).
// Producer pushes into the Injector; each consumer steals from Injector.
// This is the apples-to-apples SPMC analogue.
fn bench_crossbeam_deque<const N: usize>(label: &str) {
    let injector: Arc<Injector<u64>> = Arc::new(Injector::new());
    let stop = Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = (0..N).map(|_| {
        let injector = injector.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            let worker: Worker<u64> = Worker::new_fifo();
            let mut count: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                // Take from own queue first; else steal from injector.
                if let Some(v) = worker.pop() {
                    std::hint::black_box(v); count += 1;
                    continue;
                }
                match injector.steal_batch_and_pop(&worker) {
                    Steal::Success(v) => { std::hint::black_box(v); count += 1; }
                    Steal::Empty => { std::hint::spin_loop(); }
                    Steal::Retry => {}
                }
            }
            count
        })
    }).collect();

    // warmup
    for _ in 0..warmup_batches() {
        for k in 0..BATCH as u64 { injector.push(k); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let t_wall = Instant::now();
    for _ in 0..n {
        let t0 = Instant::now();
        for k in 0..BATCH as u64 { injector.push(k); }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(label, lats, t_wall.elapsed().as_nanos() as u64);

    // give consumers a moment to drain, then stop
    thread::sleep(std::time::Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    for h in handles { let _ = h.join().unwrap(); }
}

fn main() {
    println!("=== arbitro-kit Synapse overhead bench ===");
    println!("rounds={} batches × BATCH={} ops each", rounds(), BATCH);

    header("A. Single-thread send+recv (hot-path cost, no park)");
    bench_single_thread();

    header("B. 1P / 1C cross-thread (flow-controlled send)");
    bench_spsc_cross_thread();

    header("C. 1P / NC cross-thread — aggregate producer push rate");
    bench_spmc_cross_thread::<2>("synapse 1P/2C");
    bench_spmc_cross_thread::<4>("synapse 1P/4C");
    bench_spmc_cross_thread::<8>("synapse 1P/8C");

    header("D. vs crossbeam-deque (Injector + Workers)");
    bench_crossbeam_deque::<2>("crossbeam-deque 1P/2C");
    bench_crossbeam_deque::<4>("crossbeam-deque 1P/4C");
    bench_crossbeam_deque::<8>("crossbeam-deque 1P/8C");

    println!("\nDone.");
}
