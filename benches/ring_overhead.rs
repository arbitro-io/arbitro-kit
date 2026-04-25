//! Ring overhead bench — measures Ring<T, CAP> in isolation.
//!
//! Organized into two halves:
//!
//! ## A. Flow (one-way, producer → consumer)
//!   A1. single-thread, per-item       (try_send / try_recv)
//!   A2. single-thread, batch          (try_send_from / drain_into)
//!   A3. cross-thread,  per-item       (send / recv)
//!   A4. cross-thread,  batch          (try_send_from / drain_into)
//!   A5. cross-thread,  burst          (producer-only latency)
//!
//! ## B. Round-trip (closed loop, 2 Rings, producer → worker → producer)
//!   B1. single-thread, per-item       (4 ops/cycle, no coherence)
//!   B2. single-thread, batch          (batched request + batched reply)
//!   B3. cross-thread,  per-item       (2 cross-core hops/cycle)
//!   B4. cross-thread,  batch          (amortized across batched cycles)
//!
//! Run:
//!   cargo bench --bench ring_overhead 2>&1 | tee ring_overhead.log

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Instant;

use arbitro_kit::stream::Ring;

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

// Scenario B2 — single-thread BATCHED round-trip. ─────────────────────

fn rt_single_thread_batched<const CAP: usize, const BSZ: usize>() -> f64 {
    // Same-thread closed loop but each cycle moves BSZ items per direction
    // via the batch API. Measures the raw cursor+signal cost with bulk ops
    // amortized.  ns reported is per ITEM (not per batch).
    let req: Ring<u64, CAP> = Ring::new();
    let rsp: Ring<u64, CAP> = Ring::new();
    req.set_producer(thread::current());
    req.set_consumer(thread::current());
    rsp.set_producer(thread::current());
    rsp.set_consumer(thread::current());

    let mut src = Vec::with_capacity(BSZ);
    let mut recv_buf = Vec::with_capacity(BSZ);

    // warmup
    for _ in 0..4 {
        src.clear();
        src.extend(0..BSZ as u64);
        let _ = req.try_send_from(&mut src);
        recv_buf.clear();
        let _ = req.drain_into(&mut recv_buf, BSZ);
        let mut reply: Vec<u64> = recv_buf.iter().map(|v| v.wrapping_mul(2).wrapping_add(1)).collect();
        let _ = rsp.try_send_from(&mut reply);
        recv_buf.clear();
        let _ = rsp.drain_into(&mut recv_buf, BSZ);
    }

    let cycles = MSGS / BSZ;
    let t0 = Instant::now();
    for _ in 0..cycles {
        src.clear();
        src.extend(0..BSZ as u64);
        let _ = req.try_send_from(&mut src);
        recv_buf.clear();
        let _ = req.drain_into(&mut recv_buf, BSZ);
        let mut reply: Vec<u64> = recv_buf.iter().map(|v| v.wrapping_mul(2).wrapping_add(1)).collect();
        let _ = rsp.try_send_from(&mut reply);
        recv_buf.clear();
        let _ = rsp.drain_into(&mut recv_buf, BSZ);
    }
    let total_items = (cycles * BSZ) as f64;
    t0.elapsed().as_nanos() as f64 / total_items
}

// Scenario B4 — cross-thread BATCHED round-trip. ──────────────────────

fn rt_cross_thread_batched<const CAP: usize, const BSZ: usize>() -> f64 {
    // Producer (main) sends BSZ requests, then batch-drains BSZ replies.
    // Worker batch-drains requests, transforms, batch-sends replies.
    // One "cycle" = one batch moving forward + one batch returning.
    // ns reported is per ITEM to be comparable with per-item numbers.
    let req: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let rsp: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let req2 = req.clone();
    let rsp2 = rsp.clone();

    let worker = thread::spawn(move || {
        req2.set_consumer(thread::current());
        rsp2.set_producer(thread::current());
        let mut buf: Vec<u64> = Vec::with_capacity(BSZ);
        let mut processed: usize = 0;
        while processed < MSGS {
            // Block until at least one item arrives, then batch-drain.
            let first = req2.recv();
            if first == u64::MAX { break; }
            buf.clear();
            buf.push(first.wrapping_mul(2).wrapping_add(1));
            let mut batch: Vec<u64> = Vec::with_capacity(BSZ);
            let _ = req2.drain_into(&mut batch, BSZ - 1);
            for v in batch {
                if v == u64::MAX { break; }
                buf.push(v.wrapping_mul(2).wrapping_add(1));
            }
            processed += buf.len();
            while !buf.is_empty() {
                let n = rsp2.try_send_from(&mut buf);
                if n == 0 { let v = buf.remove(0); rsp2.send(v); }
            }
        }
    });

    req.set_producer(thread::current());
    rsp.set_consumer(thread::current());

    let mut pending: Vec<u64> = Vec::with_capacity(BSZ);
    let mut recv_buf: Vec<u64> = Vec::with_capacity(BSZ);
    let mut sent: u64 = 0;
    let mut received: usize = 0;

    let t0 = Instant::now();
    while received < MSGS {
        // Send a batch of requests.
        if sent < MSGS as u64 {
            let take = ((MSGS as u64) - sent).min(BSZ as u64) as usize;
            pending.clear();
            pending.extend(sent..sent + take as u64);
            while !pending.is_empty() {
                let n = req.try_send_from(&mut pending);
                if n == 0 { let v = pending.remove(0); req.send(v); }
            }
            sent += take as u64;
        }
        // Pull a batch of replies.
        let first = rsp.recv();
        debug_assert!(first != u64::MAX);
        received += 1;
        recv_buf.clear();
        let _ = rsp.drain_into(&mut recv_buf, BSZ - 1);
        received += recv_buf.len();
    }
    let ns = t0.elapsed().as_nanos() as f64;

    req.send(u64::MAX); // unblock worker
    worker.join().unwrap();
    ns / MSGS as f64
}

// ──────────────────────────────────────────────────────────────────────
// Payload-size sweep — how Ring behaves with fat payloads.
//
// `Ring<T, CAP>` stores T inline in each slot. For big T this means two
// `memcpy`s per message (into the slot on send, out of the slot on recv).
// Sending `Box<T>` instead moves a single pointer — at the cost of a heap
// allocation per message. The crossover size tells the user when to wrap.
// ──────────────────────────────────────────────────────────────────────

// Fewer msgs for fat payloads so total wall time + memory stays bounded.
// At 64 KB payloads, pool variant allocates 2 × PMSGS boxes ≈ PMSGS × 128 KB.
// 5_000 × 128 KB = 640 MB peak — comfortable on a dev machine.
const PMSGS: usize = 5_000;

// Warmup iterations per measurement run (not timed).
const WARMUP: usize = 500;
// Number of runs per (variant, size) for min/p50 stats.
const PAYLOAD_RUNS: usize = 10;

/// XT per-item with inline `[u8; N]` payload. Producer writes the first
/// byte with the iteration counter (to defeat dead-store elimination);
/// consumer reads byte 0 of each received payload.
fn ct_ring_inline<const N: usize, const CAP: usize>() -> f64 {
    let r: Arc<Ring<[u8; N], CAP>> = Arc::new(Ring::new());
    let r2 = r.clone();
    let total = WARMUP + PMSGS;
    let consumer = thread::spawn(move || {
        r2.set_consumer(thread::current());
        let mut sum: u64 = 0;
        for _ in 0..total { sum = sum.wrapping_add(r2.recv()[0] as u64); }
        sum
    });
    r.set_producer(thread::current());
    let mut buf = [0u8; N];
    // Warmup (not timed).
    for i in 0..WARMUP as u64 { buf[0] = i as u8; r.send(buf); }
    // Timed region.
    let t0 = Instant::now();
    for i in 0..PMSGS as u64 {
        buf[0] = i as u8;
        r.send(buf);
    }
    let ns = t0.elapsed().as_nanos() as f64;
    let _ = consumer.join().unwrap();
    ns / PMSGS as f64
}

/// XT per-item with `Box<[u8; N]>` payload. Fresh `Box::new` per send —
/// includes malloc+free+memset cost. This is what users pay if they
/// naively wrap in Box without a pool.
fn ct_ring_boxed<const N: usize, const CAP: usize>() -> f64 {
    let r: Arc<Ring<Box<[u8; N]>, CAP>> = Arc::new(Ring::new());
    let r2 = r.clone();
    let total = WARMUP + PMSGS;
    let consumer = thread::spawn(move || {
        r2.set_consumer(thread::current());
        let mut sum: u64 = 0;
        for _ in 0..total { sum = sum.wrapping_add(r2.recv()[0] as u64); }
        sum
    });
    r.set_producer(thread::current());
    // Warmup (not timed).
    for i in 0..WARMUP as u64 {
        let mut b: Box<[u8; N]> = Box::new([0u8; N]);
        b[0] = i as u8;
        r.send(b);
    }
    // Timed region.
    let t0 = Instant::now();
    for i in 0..PMSGS as u64 {
        let mut b: Box<[u8; N]> = Box::new([0u8; N]);
        b[0] = i as u8;
        r.send(b);
    }
    let ns = t0.elapsed().as_nanos() as f64;
    let _ = consumer.join().unwrap();
    ns / PMSGS as f64
}

/// XT per-item with `Box<[u8; N]>` payload + **pre-allocated pool**.
/// All boxes are allocated before the timer starts; consumer reads byte 0
/// and stashes each box in a sink Vec (dropped AFTER the timer stops).
/// Result: pure pointer-move cost with zero malloc/free in the hot path.
fn ct_ring_pooled<const N: usize, const CAP: usize>() -> f64 {
    let total = WARMUP + PMSGS;
    // Pre-allocate all payloads up-front. This heap traffic is NOT timed.
    let src: Vec<Box<[u8; N]>> = (0..total).map(|_| Box::new([0u8; N])).collect();
    // Use indexed access instead of Vec::pop to keep the producer hot path
    // free of Vec bookkeeping. We walk `src` front-to-back via raw index.
    let src_ptr = src.as_ptr();

    let r: Arc<Ring<Box<[u8; N]>, CAP>> = Arc::new(Ring::new());
    let r2 = r.clone();
    let consumer = thread::spawn(move || {
        r2.set_consumer(thread::current());
        // Sink + touch byte 0 (symmetric with inline/boxed consumers).
        let mut sink: Vec<Box<[u8; N]>> = Vec::with_capacity(total);
        let mut sum: u64 = 0;
        for _ in 0..total {
            let b = r2.recv();
            sum = sum.wrapping_add(b[0] as u64);
            sink.push(b);
        }
        (sink, sum)
    });
    r.set_producer(thread::current());

    // Warmup (not timed). Read pre-allocated boxes by index and send.
    for i in 0..WARMUP as u64 {
        // Safety: we own `src`, consumer only touches what we send.
        let mut b = unsafe { std::ptr::read(src_ptr.add(i as usize)) };
        b[0] = i as u8;
        r.send(b);
    }
    // Timed region.
    let t0 = Instant::now();
    for i in 0..PMSGS as u64 {
        let mut b = unsafe { std::ptr::read(src_ptr.add(WARMUP + i as usize)) };
        b[0] = i as u8;
        r.send(b);
    }
    let ns = t0.elapsed().as_nanos() as f64;
    let (sink, _sum) = consumer.join().unwrap();
    // Leak `src` (its contents were ptr::read'd and ownership moved via
    // send). Drop `sink` AFTER the timer — not in the hot path.
    std::mem::forget(src);
    drop(sink);
    ns / PMSGS as f64
}

// ──────────────────────────────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────────────────────────────

fn main() {
    println!("Ring overhead bench  ({} ops × {} rounds + warmup)", BATCH, ROUNDS);
    println!("Cross-thread scenarios use N = {} messages.", MSGS);

    // ══════════════════════════════════════════════════════════════════
    //                    A. FLOW (one-way, producer → consumer)
    // ══════════════════════════════════════════════════════════════════
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║ A. FLOW (one-way, producer → consumer)                   ║");
    println!("╚══════════════════════════════════════════════════════════╝");

    // ── A1. single-thread, per-item ─────────────────────────────────
    header("A1. single-thread, per-item (try_send / try_recv)");
    row("Ring<u64, 16>",   st_ring::<16>());
    row("Ring<u64, 256>",  st_ring::<256>());
    row("Ring<u64, 1024>", st_ring::<1024>());

    // ── A2. single-thread, batch ────────────────────────────────────
    println!("\n── A2. single-thread, batch ({} items) ──", MSGS);
    println!("{:<28} {:>14}", "variant", "ns/item");
    println!("{}", "─".repeat(48));
    let (lps, bts) = send_loop_vs_batch::<2048>(MSGS);
    let (lp,  bt ) = drain_loop_vs_batch::<2048>(MSGS);
    println!("{:<28} {:>12.2}", "send loop: try_send × N",     lps);
    println!("{:<28} {:>12.2}", "send batch: try_send_from",   bts);
    println!("  speedup (send):  {:.2}×", lps / bts);
    println!("{:<28} {:>12.2}", "recv loop: try_recv × N",     lp);
    println!("{:<28} {:>12.2}", "recv batch: drain_into",      bt);
    println!("  speedup (recv):  {:.2}×", lp / bt);

    // ── A3. cross-thread, per-item ──────────────────────────────────
    println!("\n── A3. cross-thread, per-item ({} msgs) ──", MSGS);
    println!("{:<28} {:>14} {:>14}", "variant", "ns/op (min)", "ops/sec");
    println!("{}", "─".repeat(60));
    let ct_r16   = (0..3).map(|_| ct_ring::<16>()).fold(f64::INFINITY, f64::min);
    let ct_r256  = (0..3).map(|_| ct_ring::<256>()).fold(f64::INFINITY, f64::min);
    let ct_r1024 = (0..3).map(|_| ct_ring::<1024>()).fold(f64::INFINITY, f64::min);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 16>",   ct_r16,   1e9 / ct_r16);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 256>",  ct_r256,  1e9 / ct_r256);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 1024>", ct_r1024, 1e9 / ct_r1024);

    // ── A4. cross-thread, batch ─────────────────────────────────────
    println!("\n── A4. cross-thread, batch (send + ack amortized, {} msgs) ──", MSGS);
    println!("{:<28} {:>14} {:>14}", "variant", "ns/item (min)", "ops/sec");
    println!("{}", "─".repeat(60));
    let bt_b16  = (0..3).map(|_| batched_ring::<128, 16>()).fold(f64::INFINITY, f64::min);
    let bt_b64  = (0..3).map(|_| batched_ring::<128, 64>()).fold(f64::INFINITY, f64::min);
    let bt_b128 = (0..3).map(|_| batched_ring::<256, 128>()).fold(f64::INFINITY, f64::min);
    println!("{:<28} {:>12.1} {:>14.0}", "CAP=128, B=16",  bt_b16,  1e9 / bt_b16);
    println!("{:<28} {:>12.1} {:>14.0}", "CAP=128, B=64",  bt_b64,  1e9 / bt_b64);
    println!("{:<28} {:>12.1} {:>14.0}", "CAP=256, B=128", bt_b128, 1e9 / bt_b128);

    // ── A5. cross-thread, burst ─────────────────────────────────────
    println!("\n── A5. cross-thread, burst (producer-side, {} msgs) ──", MSGS);
    println!("{:<28} {:>14}", "variant", "producer ns/op");
    println!("{}", "─".repeat(48));
    let b_r16   = (0..3).map(|_| burst_ring::<16>()).fold(f64::INFINITY, f64::min);
    let b_r1024 = (0..3).map(|_| burst_ring::<1024>()).fold(f64::INFINITY, f64::min);
    let b_r2048 = (0..3).map(|_| burst_ring::<2048>()).fold(f64::INFINITY, f64::min);
    println!("{:<28} {:>12.1}", "Ring<u64, 16>",   b_r16);
    println!("{:<28} {:>12.1}", "Ring<u64, 1024>", b_r1024);
    println!("{:<28} {:>12.1} (CAP > MSGS)", "Ring<u64, 2048>", b_r2048);

    // ══════════════════════════════════════════════════════════════════
    //            B. ROUND-TRIP (closed loop, 2 rings, req → worker → resp)
    // ══════════════════════════════════════════════════════════════════
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║ B. ROUND-TRIP (closed loop, 2 Rings)                     ║");
    println!("╚══════════════════════════════════════════════════════════╝");

    // ── B1. single-thread, per-item ─────────────────────────────────
    println!("\n── B1. single-thread round-trip, per-item ({} cycles) ──", MSGS);
    println!("{:<28} {:>14} {:>14}", "variant", "ns/cycle (min)", "cycles/sec");
    println!("{}", "─".repeat(60));
    let st_rt_r32  = (0..3).map(|_| rt_single_thread::<32>()).fold(f64::INFINITY, f64::min);
    let st_rt_r256 = (0..3).map(|_| rt_single_thread::<256>()).fold(f64::INFINITY, f64::min);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 32>",  st_rt_r32,  1e9 / st_rt_r32);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 256>", st_rt_r256, 1e9 / st_rt_r256);

    // ── B2. single-thread, batch ────────────────────────────────────
    println!("\n── B2. single-thread round-trip, batch ({} items) ──", MSGS);
    println!("{:<28} {:>14} {:>14}", "variant", "ns/item (min)", "ops/sec");
    println!("{}", "─".repeat(60));
    let stb_r256_b64  = (0..3).map(|_| rt_single_thread_batched::<256, 64>()).fold(f64::INFINITY, f64::min);
    let stb_r512_b128 = (0..3).map(|_| rt_single_thread_batched::<512, 128>()).fold(f64::INFINITY, f64::min);
    println!("{:<28} {:>12.2} {:>14.0}", "CAP=256, B=64",  stb_r256_b64,  1e9 / stb_r256_b64);
    println!("{:<28} {:>12.2} {:>14.0}", "CAP=512, B=128", stb_r512_b128, 1e9 / stb_r512_b128);

    // ── B3. cross-thread, per-item ──────────────────────────────────
    println!("\n── B3. cross-thread round-trip, per-item ({} cycles) ──", MSGS);
    println!("{:<28} {:>14} {:>14}", "variant", "ns/cycle (min)", "cycles/sec");
    println!("{}", "─".repeat(60));
    let rt_r32   = (0..3).map(|_| rt_ring::<32>()).fold(f64::INFINITY, f64::min);
    let rt_r256  = (0..3).map(|_| rt_ring::<256>()).fold(f64::INFINITY, f64::min);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 32>",  rt_r32,  1e9 / rt_r32);
    println!("{:<28} {:>12.1} {:>14.0}", "Ring<u64, 256>", rt_r256, 1e9 / rt_r256);

    // ── B4. cross-thread, batch ─────────────────────────────────────
    println!("\n── B4. cross-thread round-trip, batch ({} items) ──", MSGS);
    println!("{:<28} {:>14} {:>14}", "variant", "ns/item (min)", "ops/sec");
    println!("{}", "─".repeat(60));
    let xtb_r128_b32  = (0..3).map(|_| rt_cross_thread_batched::<128, 32>()).fold(f64::INFINITY, f64::min);
    let xtb_r256_b128 = (0..3).map(|_| rt_cross_thread_batched::<256, 128>()).fold(f64::INFINITY, f64::min);
    println!("{:<28} {:>12.1} {:>14.0}", "CAP=128, B=32",  xtb_r128_b32,  1e9 / xtb_r128_b32);
    println!("{:<28} {:>12.1} {:>14.0}", "CAP=256, B=128", xtb_r256_b128, 1e9 / xtb_r256_b128);

    // ══════════════════════════════════════════════════════════════════
    //            C. PAYLOAD SIZE SWEEP (XT per-item, inline vs Box)
    // ══════════════════════════════════════════════════════════════════
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║ C. PAYLOAD SIZE SWEEP (XT per-item, {} msgs)          ║", PMSGS);
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("Inline: Ring<[u8; N], CAP>       — memcpy per send + per recv.");
    println!("Boxed:  Ring<Box<[u8; N]>, CAP>  — fresh Box::new per send (incl. alloc+memset).");
    println!("Pooled: Ring<Box<[u8; N]>, CAP>  — pre-allocated pool, no alloc/free timed.");
    println!("Stats over {} runs (after {} warmup iters per run). min / p50.",
             PAYLOAD_RUNS, WARMUP);
    println!();
    println!("{:<9} {:>14} {:>14} {:>14} {:>10}",
             "payload", "inline min/p50", "boxed min/p50", "pool min/p50", "winner");
    println!("{}", "─".repeat(74));

    // Take `runs` samples; return (min, p50).
    fn stats<F: FnMut() -> f64>(mut f: F) -> (f64, f64) {
        let mut xs: Vec<f64> = (0..PAYLOAD_RUNS).map(|_| f()).collect();
        xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let min = xs[0];
        let p50 = xs[xs.len() / 2];
        (min, p50)
    }
    fn fmt(x: (f64, f64)) -> String { format!("{:>5.1} / {:>5.1}", x.0, x.1) }

    let sizes: [(&str, (f64,f64), (f64,f64), (f64,f64)); 8] = [
        ("64 B",    stats(|| ct_ring_inline::<64,    256>()), stats(|| ct_ring_boxed::<64,    256>()), stats(|| ct_ring_pooled::<64,    256>())),
        ("256 B",   stats(|| ct_ring_inline::<256,   256>()), stats(|| ct_ring_boxed::<256,   256>()), stats(|| ct_ring_pooled::<256,   256>())),
        ("512 B",   stats(|| ct_ring_inline::<512,   256>()), stats(|| ct_ring_boxed::<512,   256>()), stats(|| ct_ring_pooled::<512,   256>())),
        ("1 KB",    stats(|| ct_ring_inline::<1024,  256>()), stats(|| ct_ring_boxed::<1024,  256>()), stats(|| ct_ring_pooled::<1024,  256>())),
        ("4 KB",    stats(|| ct_ring_inline::<4096,  64 >()), stats(|| ct_ring_boxed::<4096,  64 >()), stats(|| ct_ring_pooled::<4096,  64 >())),
        ("16 KB",   stats(|| ct_ring_inline::<16384, 16 >()), stats(|| ct_ring_boxed::<16384, 16 >()), stats(|| ct_ring_pooled::<16384, 16 >())),
        ("32 KB",   stats(|| ct_ring_inline::<32768, 16 >()), stats(|| ct_ring_boxed::<32768, 16 >()), stats(|| ct_ring_pooled::<32768, 16 >())),
        ("64 KB",   stats(|| ct_ring_inline::<65536, 8  >()), stats(|| ct_ring_boxed::<65536, 8  >()), stats(|| ct_ring_pooled::<65536, 8  >())),
    ];
    for (name, inl, bx, pl) in sizes {
        let winner = if pl.0 <= inl.0 && pl.0 <= bx.0 { "pool"   }
                     else if inl.0 <= bx.0           { "inline" }
                     else                            { "box"    };
        println!("{:<9} {:>14} {:>14} {:>14} {:>10}",
                 name, fmt(inl), fmt(bx), fmt(pl), winner);
    }

    println!();
    println!("Note: for publication-grade numbers run with:");
    println!("  taskset -c 0,1 ./ring_overhead --bench   (pin to P-cores)");

    println!("\nDone.");
}
