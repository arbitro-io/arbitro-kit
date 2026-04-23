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
//!   4. **Batch drain (single-thread)** — Produce K items, then drain
//!      them with `drain_into`. Compared against K individual `try_recv`
//!      calls. Measures the raw amortization win in cache-hot conditions.
//!
//!   5. **Cross-thread round-trip** — two Rings composed into a closed
//!      loop (request → worker → reply). Measures ns/CYCLE, not ns/op,
//!      and is the correct number for request/response workloads.
//!
//!   6. **Cross-thread batch-send + batch-drain** — producer uses
//!      `try_send_from`, consumer uses `drain_into`. Both signal
//!      handshakes amortize across the batch, and the numbers drop below
//!      the L1↔L1 per-item floor because coherence traffic is now
//!      per-BATCH, not per-item.
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

// Scenario 4b — single-thread batch SEND. ─────────────────────────────

fn send_loop_vs_batch<const CAP: usize>(n: usize) -> (f64, f64) {
    // Pair of runs: `try_send × N` vs `try_send_from(&mut Vec)`. Each run
    // starts with an empty ring; after pushing n items we drain them so
    // the ring state doesn't interfere with the next run.
    let r: Ring<u64, CAP> = Ring::new();
    let mut drain = Vec::with_capacity(n);

    // A) per-item try_send
    let t0 = Instant::now();
    for i in 0..n as u64 { r.try_send(i).unwrap(); }
    let loop_ns = t0.elapsed().as_nanos() as f64 / n as f64;
    drain.clear();
    let _ = r.drain_into(&mut drain, n);

    // B) batch try_send_from
    let mut src: Vec<u64> = (0..n as u64).collect();
    let t0 = Instant::now();
    let sent = r.try_send_from(&mut src);
    let batch_ns = t0.elapsed().as_nanos() as f64 / sent as f64;
    drain.clear();
    let _ = r.drain_into(&mut drain, n);

    (loop_ns, batch_ns)
}

// Scenario 5b — single-thread round-trip (no cross-thread coherence). ─

fn rt_single_thread<const CAP: usize>() -> f64 {
    // Same-thread closed loop over two rings. No park, no cross-core
    // coherence — measures the RAW two-cursor + two-signal cost of a
    // full round-trip with no parallelism.
    let req: Ring<u64, CAP> = Ring::new();
    let rsp: Ring<u64, CAP> = Ring::new();
    req.set_producer(thread::current());
    req.set_consumer(thread::current());
    rsp.set_producer(thread::current());
    rsp.set_consumer(thread::current());

    // warmup
    for i in 0..16u64 {
        req.try_send(i).unwrap();
        let v = req.try_recv().unwrap();
        rsp.try_send(v.wrapping_mul(2).wrapping_add(1)).unwrap();
        let _ = rsp.try_recv().unwrap();
    }

    let t0 = Instant::now();
    for i in 0..MSGS as u64 {
        req.try_send(i).unwrap();
        let v = req.try_recv().unwrap();
        rsp.try_send(v.wrapping_mul(2).wrapping_add(1)).unwrap();
        let r = rsp.try_recv().unwrap();
        debug_assert_eq!(r, i.wrapping_mul(2).wrapping_add(1));
    }
    t0.elapsed().as_nanos() as f64 / MSGS as f64
}

// Scenario 5 — cross-thread round-trip (closed loop, two rings). ──────

fn rt_ring<const CAP: usize>() -> f64 {
    // Main thread drives: req.send(i); resp = rsp.recv(); assert correlation.
    // Worker thread: req.recv() → rsp.send(f(v)). Each iteration is one
    // CLOSED-LOOP CYCLE, not one op. Measures latency, not throughput.
    let req: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let rsp: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let req2 = req.clone();
    let rsp2 = rsp.clone();

    let worker = thread::spawn(move || {
        req2.set_consumer(thread::current());
        rsp2.set_producer(thread::current());
        for _ in 0..MSGS {
            let v = req2.recv();
            rsp2.send(v.wrapping_mul(2).wrapping_add(1));
        }
    });

    req.set_producer(thread::current());
    rsp.set_consumer(thread::current());

    let t0 = Instant::now();
    for i in 0..MSGS as u64 {
        req.send(i);
        let r = rsp.recv();
        debug_assert_eq!(r, i.wrapping_mul(2).wrapping_add(1));
    }
    let ns = t0.elapsed().as_nanos() as f64;
    worker.join().unwrap();
    ns / MSGS as f64
}

// Scenario 6 — cross-thread batched send + batched drain. ─────────────

fn batched_ring<const CAP: usize, const BSZ: usize>() -> f64 {
    // Producer: try_send_from (batch). Consumer: recv one then drain_into
    // (batch). Both sides amortize their cursor publish + signal wake
    // across BSZ items.
    let r: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let r2 = r.clone();

    let consumer = thread::spawn(move || {
        r2.set_consumer(thread::current());
        let mut count: u64 = 0;
        let mut sum:   u64 = 0; // prevent loop elimination
        let mut buf: Vec<u64> = Vec::with_capacity(BSZ);
        'outer: loop {
            let v = r2.recv();              // block on not_empty
            if v == u64::MAX { break; }     // sentinel from producer
            count += 1;
            sum = sum.wrapping_add(v);
            // One batched ack covers up to BSZ additional items.
            buf.clear();
            let _ = r2.drain_into(&mut buf, BSZ);
            for &x in &buf {
                if x == u64::MAX { break 'outer; }
                count += 1;
                sum = sum.wrapping_add(x);
            }
        }
        std::hint::black_box((count, sum));
    });

    r.set_producer(thread::current());
    let mut pending: Vec<u64> = Vec::with_capacity(BSZ);
    let mut sent: u64 = 0;
    let t0 = Instant::now();
    while sent < MSGS as u64 {
        let take = ((MSGS as u64) - sent).min(BSZ as u64) as usize;
        pending.clear();
        pending.extend(sent..sent + take as u64);
        while !pending.is_empty() {
            let n = r.try_send_from(&mut pending);
            if n == 0 {
                // Ring full — fall back to a blocking single send.
                let v = pending.remove(0);
                r.send(v);
            }
        }
        sent += take as u64;
    }
    // sentinel to unblock consumer
    r.send(u64::MAX);
    let ns = t0.elapsed().as_nanos() as f64;
    consumer.join().unwrap();
    ns / MSGS as f64
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

    // ── Scenario 4 — batch drain (single-thread) ────────────────────
    println!("\n── 4. batch drain, single-thread ({} items) ──", MSGS);
    println!("{:<28} {:>14}", "variant", "ns/item");
    println!("{}", "─".repeat(48));
    let (lp, bt) = drain_loop_vs_batch::<2048>(MSGS);
    println!("{:<28} {:>12.2}", "loop: try_recv × N",  lp);
    println!("{:<28} {:>12.2}", "batch: drain_into",   bt);
    println!("  speedup: {:.2}×", lp / bt);

    // ── Scenario 4b — batch send (single-thread) ────────────────────
    println!("\n── 4b. batch send, single-thread ({} items) ──", MSGS);
    println!("{:<28} {:>14}", "variant", "ns/item");
    println!("{}", "─".repeat(48));
    let (lps, bts) = send_loop_vs_batch::<2048>(MSGS);
    println!("{:<28} {:>12.2}", "loop: try_send × N",     lps);
    println!("{:<28} {:>12.2}", "batch: try_send_from",   bts);
    println!("  speedup: {:.2}×", lps / bts);

    // ── Scenario 5a — single-thread round-trip ──────────────────────
    println!("\n── 5a. single-thread round-trip, 2 rings ({} cycles) ──", MSGS);
    println!("{:<28} {:>14} {:>14}", "variant", "ns/cycle (min)", "cycles/sec");
    println!("{}", "─".repeat(60));
    let st_rt_r32  = (0..3).map(|_| rt_single_thread::<32>()).fold(f64::INFINITY, f64::min);
    let st_rt_r256 = (0..3).map(|_| rt_single_thread::<256>()).fold(f64::INFINITY, f64::min);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 32>",  st_rt_r32,  1e9 / st_rt_r32);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 256>", st_rt_r256, 1e9 / st_rt_r256);

    // ── Scenario 5b — cross-thread round-trip ───────────────────────
    println!("\n── 5b. cross-thread round-trip, 2 rings ({} cycles) ──", MSGS);
    println!("{:<28} {:>14} {:>14}", "variant", "ns/cycle (min)", "cycles/sec");
    println!("{}", "─".repeat(60));
    let rt_r32   = (0..3).map(|_| rt_ring::<32>()).fold(f64::INFINITY, f64::min);
    let rt_r256  = (0..3).map(|_| rt_ring::<256>()).fold(f64::INFINITY, f64::min);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 32>",  rt_r32,  1e9 / rt_r32);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 256>", rt_r256, 1e9 / rt_r256);

    // ── Scenario 6 — cross-thread batched send + batched drain ──────
    println!("\n── 6. cross-thread batched (send + ack amortized, {} msgs) ──", MSGS);
    println!("{:<28} {:>14} {:>14}", "variant", "ns/item (min)", "ops/sec");
    println!("{}", "─".repeat(60));
    let bt_b16  = (0..3).map(|_| batched_ring::<128, 16>()).fold(f64::INFINITY, f64::min);
    let bt_b64  = (0..3).map(|_| batched_ring::<128, 64>()).fold(f64::INFINITY, f64::min);
    let bt_b128 = (0..3).map(|_| batched_ring::<256, 128>()).fold(f64::INFINITY, f64::min);
    println!("{:<28} {:>12.1} {:>14.0}", "CAP=128, B=16",  bt_b16,  1e9 / bt_b16);
    println!("{:<28} {:>12.1} {:>14.0}", "CAP=128, B=64",  bt_b64,  1e9 / bt_b64);
    println!("{:<28} {:>12.1} {:>14.0}", "CAP=256, B=128", bt_b128, 1e9 / bt_b128);

    println!("\nDone.");
}
