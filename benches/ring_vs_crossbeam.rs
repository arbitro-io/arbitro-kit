//! Apples-to-apples: arbitro Ring vs crossbeam_channel::bounded.
//!
//! Identical methodology for both:
//!   - 1 producer thread, 1 consumer thread (SPSC).
//!   - MSGS messages of u64 payload.
//!   - Producer calls blocking `send` (waits if full).
//!   - Consumer calls blocking `recv` (waits if empty).
//!   - Timed from producer start until consumer joins.
//!   - Same CAP for both primitives in each row.
//!   - min over N runs (best-of), p50 too.
//!
//! Goal: settle whether the 22.7 ns/op Ring number from A3 vs the 67 M/s
//! crossbeam number from fanin_h2h is a real gap or a methodology artifact.

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::stream::Ring;

const MSGS: usize = 1000;
const ROUNDS: usize = 300;
const WARMUP: usize = 30;

fn pct(samples: &mut Vec<f64>, p: f64) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples[idx.min(samples.len() - 1)]
}

// ─── arbitro Ring ─────────────────────────────────────────────────────────
fn run_ring<const CAP: usize>() -> f64 {
    let r: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let r2 = r.clone();
    let consumer = thread::spawn(move || {
        r2.set_consumer(thread::current());
        let mut sum = 0u64;
        for _ in 0..MSGS { sum = sum.wrapping_add(r2.recv()); }
        sum
    });
    r.set_producer(thread::current());
    let t0 = Instant::now();
    for i in 0..MSGS as u64 { r.send(i); }
    let _ = consumer.join().unwrap();
    let ns = t0.elapsed().as_nanos() as f64;
    ns / MSGS as f64
}

// ─── crossbeam_channel::bounded ───────────────────────────────────────────
fn run_crossbeam(cap: usize) -> f64 {
    let (tx, rx) = crossbeam_channel::bounded::<u64>(cap);
    let consumer = thread::spawn(move || {
        let mut sum = 0u64;
        for _ in 0..MSGS { sum = sum.wrapping_add(rx.recv().unwrap()); }
        sum
    });
    let t0 = Instant::now();
    for i in 0..MSGS as u64 { tx.send(i).unwrap(); }
    let _ = consumer.join().unwrap();
    let ns = t0.elapsed().as_nanos() as f64;
    ns / MSGS as f64
}

fn collect<F: FnMut() -> f64>(mut f: F) -> (f64, f64) {
    for _ in 0..WARMUP { let _ = f(); }
    let mut samples: Vec<f64> = (0..ROUNDS).map(|_| f()).collect();
    let min = samples.iter().cloned().fold(f64::INFINITY, f64::min);
    let p50 = pct(&mut samples, 0.50);
    (min, p50)
}

fn row(name: &str, min: f64, p50: f64) {
    println!("{:<32} {:>10.1} {:>10.1} {:>14.0}",
             name, min, p50, 1e9 / min);
}

fn main() {
    println!("=== Ring vs crossbeam_channel::bounded — SAME methodology ===");
    println!("Both: 1P/1C threads, blocking send+recv, {} msgs, u64 payload.", MSGS);
    println!("Best of {} rounds (after {} warmup).", ROUNDS, WARMUP);
    println!();
    println!("{:<32} {:>10} {:>10} {:>14}",
             "variant", "min ns", "p50 ns", "ops/sec (min)");
    println!("{}", "─".repeat(70));

    // CAP = 16
    let (min, p50) = collect(|| run_ring::<16>());
    row("arbitro Ring<u64, 16>", min, p50);
    let (min, p50) = collect(|| run_crossbeam(16));
    row("crossbeam bounded(16)", min, p50);
    println!();

    // CAP = 64
    let (min, p50) = collect(|| run_ring::<64>());
    row("arbitro Ring<u64, 64>", min, p50);
    let (min, p50) = collect(|| run_crossbeam(64));
    row("crossbeam bounded(64)", min, p50);
    println!();

    // CAP = 256
    let (min, p50) = collect(|| run_ring::<256>());
    row("arbitro Ring<u64, 256>", min, p50);
    let (min, p50) = collect(|| run_crossbeam(256));
    row("crossbeam bounded(256)", min, p50);
    println!();

    // CAP = 1024
    let (min, p50) = collect(|| run_ring::<1024>());
    row("arbitro Ring<u64, 1024>", min, p50);
    let (min, p50) = collect(|| run_crossbeam(1024));
    row("crossbeam bounded(1024)", min, p50);

    println!("\nDone.");
}
