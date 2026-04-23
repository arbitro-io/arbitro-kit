//! Ring overhead bench — measures Ring<T, CAP> in isolation across the
//! scenarios that stress its different code paths.
//!
//! Scenarios:
//!
//!   1. **Single-thread primitive cost** — producer and consumer on the
//!      same thread, alternating try_send/try_recv. No park, no cross-core
//!      coherence — just cursor math + one Release store per side.
//!
//!   2. **Cross-thread steady-state throughput** — one producer, one
//!      consumer, distinct threads. N messages pushed end-to-end; we
//!      measure total wall-time / N. Exercises the pipelining win as CAP
//!      grows.
//!
//!   3. **Burst absorption, producer-side** — producer fires N items as
//!      fast as possible. With CAP >= N the producer never blocks; with
//!      CAP < N it parks and resumes as the consumer drains.
//!
//!   4. **Batch drain** — Produce K items, then drain them with
//!      `drain_into`. Compared against K individual `try_recv` calls.
//!      Demonstrates the amortization win.
//!
//! Run:
//!   cargo bench --bench ring_overhead 2>&1 | tee ring_overhead.log

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Instant;

use arbitro_kit::gate::Ring;

// Keep totals small per bench_safety: max ~1000 msgs per measurement.
const MSGS:   usize = 1000;
const BATCH:  usize = 1000;  // inner loop for single-thread timing precision
const ROUNDS: usize = 300;   // samples per variant (best of)

fn header(title: &str) {
    println!("\n── {} ──", title);
    println!("{:<28} {:>12} {:>12} {:>14}",
             "variant", "p50_ns/op", "min_ns/op", "ops/sec");
    println!("{}", "─".repeat(72));
}

fn row(name: &str, mut sample_ns_per_batch: Vec<u64>) {
    sample_ns_per_batch.sort_unstable();
    let per_op_ns = |v: u64| v as f64 / BATCH as f64;
    let p50 = per_op_ns(sample_ns_per_batch[sample_ns_per_batch.len() / 2]);
    let min = per_op_ns(*sample_ns_per_batch.first().unwrap());
    let ops = 1e9 / p50;
    println!("{:<28} {:>12.2} {:>12.2} {:>14.0}", name, p50, min, ops);
}

// Scenario 1 — single-thread, no park. ────────────────────────────────

fn st_ring<const CAP: usize>() -> Vec<u64> {
    let r: Ring<u64, CAP> = Ring::new();
    r.set_consumer(std::thread::current());
    r.set_producer(std::thread::current());
    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..10 { // warmup
        for i in 0..BATCH as u64 {
            r.try_send(i).unwrap();
            let _ = r.try_recv().unwrap();
        }
    }
    for _ in 0..ROUNDS {
        let t0 = Instant::now();
        for i in 0..BATCH as u64 {
            r.try_send(i).unwrap();
            let _ = r.try_recv().unwrap();
        }
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    samples
}

// Scenario 2 — cross-thread steady state. ─────────────────────────────

fn ct_ring<const CAP: usize>() -> f64 {
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

// Scenario 3 — burst, producer-side. ──────────────────────────────────

fn burst_ring<const CAP: usize>() -> f64 {
    // Producer fires MSGS items. If MSGS <= CAP, producer never blocks —
    // pure enqueue cost, no rendezvous. If MSGS > CAP, it blocks after CAP.
    let r: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let stop = Arc::new(AtomicBool::new(false));
    let r2 = r.clone();
    let s2 = stop.clone();
    let consumer = thread::spawn(move || {
        r2.set_consumer(thread::current());
        while !s2.load(Ordering::Relaxed) {
            let _ = r2.recv();
        }
    });
    thread::sleep(std::time::Duration::from_millis(5));
    r.set_producer(thread::current());
    let t0 = Instant::now();
    for i in 0..MSGS as u64 { r.send(i); }
    let ns = t0.elapsed().as_nanos() as f64;
    stop.store(true, Ordering::Relaxed);
    r.send(0);
    let _ = consumer.join().unwrap();
    ns / MSGS as f64
}

// Scenario 4 — batch drain. ───────────────────────────────────────────

fn drain_loop_vs_batch<const CAP: usize>(n: usize) -> (f64, f64) {
    let r: Ring<u64, CAP> = Ring::new();

    for i in 0..n as u64 { r.try_send(i).unwrap(); }
    let t0 = Instant::now();
    for _ in 0..n { let _ = r.try_recv().unwrap(); }
    let loop_ns = t0.elapsed().as_nanos() as f64 / n as f64;

    for i in 0..n as u64 { r.try_send(i).unwrap(); }
    let mut out = Vec::with_capacity(n);
    let t0 = Instant::now();
    let _ = r.drain_into(&mut out, n);
    let batch_ns = t0.elapsed().as_nanos() as f64 / n as f64;

    (loop_ns, batch_ns)
}

// ──────────────────────────────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────────────────────────────

fn main() {
    println!("Ring overhead bench  ({} ops × {} rounds + warmup)", BATCH, ROUNDS);
    println!("Cross-thread scenarios use N = {} messages.", MSGS);

    // ── Scenario 1 — single-thread hot loop ──────────────────────────
    header("1. single-thread (producer = consumer, no park, no coherence)");
    row("Ring<u64, 16>",   st_ring::<16>());
    row("Ring<u64, 256>",  st_ring::<256>());
    row("Ring<u64, 1024>", st_ring::<1024>());

    // ── Scenario 2 — cross-thread steady state ───────────────────────
    println!("\n── 2. cross-thread steady state ({} msgs) ──", MSGS);
    println!("{:<28} {:>14} {:>14}", "variant", "ns/op (min)", "ops/sec");
    println!("{}", "─".repeat(60));
    let ct_r16   = (0..3).map(|_| ct_ring::<16>()).fold(f64::INFINITY, f64::min);
    let ct_r256  = (0..3).map(|_| ct_ring::<256>()).fold(f64::INFINITY, f64::min);
    let ct_r1024 = (0..3).map(|_| ct_ring::<1024>()).fold(f64::INFINITY, f64::min);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 16>",   ct_r16,   1e9 / ct_r16);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 256>",  ct_r256,  1e9 / ct_r256);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 1024>", ct_r1024, 1e9 / ct_r1024);

    // ── Scenario 3 — burst absorption, producer side ────────────────
    println!("\n── 3. burst absorption, producer-side ({} msgs) ──", MSGS);
    println!("{:<28} {:>14}", "variant", "producer ns/op");
    println!("{}", "─".repeat(48));
    let b_r16   = (0..3).map(|_| burst_ring::<16>()).fold(f64::INFINITY, f64::min);
    let b_r1024 = (0..3).map(|_| burst_ring::<1024>()).fold(f64::INFINITY, f64::min);
    let b_r2048 = (0..3).map(|_| burst_ring::<2048>()).fold(f64::INFINITY, f64::min);
    println!("{:<28} {:>12.1}", "Ring<u64, 16>",   b_r16);
    println!("{:<28} {:>12.1}", "Ring<u64, 1024>", b_r1024);
    println!("{:<28} {:>12.1} (CAP > MSGS)", "Ring<u64, 2048>", b_r2048);

    // ── Scenario 4 — batch drain ────────────────────────────────────
    println!("\n── 4. batch drain ({} items) ──", MSGS);
    println!("{:<28} {:>14}", "variant", "ns/item");
    println!("{}", "─".repeat(48));
    let (lp, bt) = drain_loop_vs_batch::<2048>(MSGS);
    println!("{:<28} {:>12.2}", "loop: try_recv × N",  lp);
    println!("{:<28} {:>12.2}", "batch: drain_into",   bt);
    println!("  speedup: {:.2}×", lp / bt);

    println!("\nDone.");
}
