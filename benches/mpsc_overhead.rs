//! Mpsc overhead bench — M producers / 1 consumer.
//!
//! `route::Mpsc<T, RING_CAP>` is the M:1 fan-in primitive: per-producer
//! SPSC ring + consumer-side scan for wakeup. Producers pay zero
//! `LOCK`-prefixed RMW on the send path; the consumer scans M rings per
//! drain pass.
//!
//! Sections:
//!   A. Single-thread 1P/1C — hot path cost (no park).
//!   B. 1P/1C cross-thread — vs crossbeam::channel::bounded.
//!   C. MP/1C fan-in        (M = 2, 4, 8).
//!   D. crossbeam baselines (same C shapes).
//!   E. Producer-batched MP/1C via `try_send_batch` (chunk = 64).
//!
//! ## Capacity parity
//!
//! `Mpsc<T, RING_CAP>` allocates RING_CAP slots **per producer**, total =
//! `M × RING_CAP`. `crossbeam::channel::bounded(N)` is one shared queue of
//! `N` slots total. To make ns/op comparable we keep the **total capacity
//! constant at 1024** for every M:
//!
//!   M=1 → RING_CAP=1024 | M=2 → 512 | M=4 → 256 | M=8 → 128
//!
//! crossbeam stays at `bounded(1024)`. Both shapes give producers the same
//! absolute back-pressure budget.
//!
//! Conforms to bench_safety: BATCH = 1000, rounds capped, tee log expected
//! from runner, no background work.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::route::{Mpsc, MpscConsumer, MpscProducer, MpscShutdown};
use crossbeam_channel::bounded;

const BATCH: usize = 1000;

fn rounds() -> usize {
    std::env::var("BENCH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500)
}
fn warmup_batches() -> usize { 10 }

fn header(title: &str) {
    println!("\n── {} ──", title);
    println!(
        "{:<42} {:>12} {:>12} {:>12} {:>14}",
        "variant", "mean_ns/op", "p50_ns/op", "p99_ns/op", "ops/sec"
    );
    println!("{}", "─".repeat(94));
}
fn row(name: &str, mut batch_ns: Vec<u64>, total_elapsed_ns: u64) {
    batch_ns.sort_unstable();
    let samples = batch_ns.len();
    let total_ops = samples * BATCH;
    let ops = (total_ops as f64) / (total_elapsed_ns as f64 / 1e9);
    let mean = total_elapsed_ns as f64 / total_ops as f64;
    let p50 = batch_ns[samples / 2] as f64 / BATCH as f64;
    let p99 = batch_ns[samples * 99 / 100] as f64 / BATCH as f64;
    println!(
        "{:<42} {:>12.2} {:>12.2} {:>12.2} {:>14}",
        name, mean, p50, p99, ops as u64
    );
}

// ── A. Single-thread 1P/1C (RING_CAP=1024 → total cap 1024) ──────────
fn bench_single_thread() {
    let (ps, c, sd): (Vec<MpscProducer<u64, 1024>>, MpscConsumer<u64, 1024>, MpscShutdown<u64, 1024>) =
        Mpsc::<u64, 1024>::new(1);
    let p = &ps[0];

    let do_batch = || {
        for k in 0..BATCH as u64 {
            p.try_send(k).unwrap();
            std::hint::black_box(c.try_recv().unwrap());
        }
    };
    for _ in 0..warmup_batches() { do_batch(); }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let wall = Instant::now();
    for _ in 0..n {
        let t0 = Instant::now();
        do_batch();
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row("mpsc 1P/1C single-thread", lats, wall.elapsed().as_nanos() as u64);

    drop(sd);
}

// ── B. 1P/1C cross-thread (RING_CAP=1024 → total cap 1024) ───────────
fn bench_spsc_cross_thread() {
    let (mut ps, c, sd) = Mpsc::<u64, 1024>::new(1);
    let p = ps.pop().unwrap();

    let consumer = thread::spawn(move || {
        c.bind();
        let mut count: u64 = 0;
        loop {
            match c.recv() {
                Ok(v) => { std::hint::black_box(v); count = count.wrapping_add(1); }
                Err(_) => break,
            }
        }
        count
    });

    p.bind();
    for _ in 0..warmup_batches() {
        for k in 0..BATCH as u64 { p.send(k); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let wall = Instant::now();
    for _ in 0..n {
        let t0 = Instant::now();
        for k in 0..BATCH as u64 { p.send(k); }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row("mpsc 1P/1C cross-thread", lats, wall.elapsed().as_nanos() as u64);

    sd.signal();
    let _ = consumer.join().unwrap();
}

// ── C. MP/1C fan-in ───────────────────────────────────────────────────
//
// `RING_CAP` chosen so M × RING_CAP = 1024 (parity with crossbeam(1024)).
fn bench_mpsc_fanin<const M: usize, const RING_CAP: usize>(label: &str) {
    let (ps, c, sd) = Mpsc::<u64, RING_CAP>::new(M);

    let consumer = thread::spawn(move || {
        c.bind();
        let mut total: u64 = 0;
        loop {
            match c.recv_batch(|v| { std::hint::black_box(v); total += 1; }) {
                Ok(_) => {}
                Err(_) => break,
            }
        }
        total
    });

    let per_prod = BATCH / M;
    let work_round = Arc::new(AtomicU64::new(0));
    let done_round = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for p in ps.into_iter() {
        let work_round = work_round.clone();
        let done_round = done_round.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            p.bind();
            let mut last_round: u64 = 0;
            loop {
                loop {
                    if stop.load(Ordering::Acquire) { return; }
                    let r = work_round.load(Ordering::Acquire);
                    if r > last_round { last_round = r; break; }
                    std::hint::spin_loop();
                }
                for k in 0..per_prod as u64 { p.send(k); }
                done_round.fetch_add(1, Ordering::AcqRel);
            }
        }));
    }

    for _ in 0..warmup_batches() {
        done_round.store(0, Ordering::Release);
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let wall = Instant::now();
    for _ in 0..n {
        done_round.store(0, Ordering::Release);
        let t0 = Instant::now();
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(label, lats, wall.elapsed().as_nanos() as u64);

    stop.store(true, Ordering::Release);
    work_round.fetch_add(1, Ordering::AcqRel);
    for h in handles { let _ = h.join(); }
    sd.signal();
    let _ = consumer.join().unwrap();
}

// ── D. crossbeam baselines ────────────────────────────────────────────
fn bench_crossbeam_mpsc<const M: usize>(label: &str) {
    let (tx, rx) = bounded::<u64>(1024);

    let consumer = thread::spawn(move || {
        let mut count: u64 = 0;
        while let Ok(v) = rx.recv() {
            std::hint::black_box(v); count += 1;
        }
        count
    });

    let per_prod = BATCH / M;
    let work_round = Arc::new(AtomicU64::new(0));
    let done_round = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for _ in 0..M {
        let tx = tx.clone();
        let work_round = work_round.clone();
        let done_round = done_round.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut last_round: u64 = 0;
            loop {
                loop {
                    if stop.load(Ordering::Acquire) { return; }
                    let r = work_round.load(Ordering::Acquire);
                    if r > last_round { last_round = r; break; }
                    std::hint::spin_loop();
                }
                for k in 0..per_prod as u64 { let _ = tx.send(k); }
                done_round.fetch_add(1, Ordering::AcqRel);
            }
        }));
    }
    drop(tx);

    for _ in 0..warmup_batches() {
        done_round.store(0, Ordering::Release);
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let wall = Instant::now();
    for _ in 0..n {
        done_round.store(0, Ordering::Release);
        let t0 = Instant::now();
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(label, lats, wall.elapsed().as_nanos() as u64);

    stop.store(true, Ordering::Release);
    work_round.fetch_add(1, Ordering::AcqRel);
    for h in handles { let _ = h.join(); }
    let _ = consumer.join().unwrap();
}

// ── E. Producer-batched MP/1C via `try_send_batch` ────────────────────
fn bench_mpsc_batched<const M: usize, const RING_CAP: usize>(label: &str, chunk: usize) {
    let (ps, c, sd) = Mpsc::<u64, RING_CAP>::new(M);

    let consumer = thread::spawn(move || {
        c.bind();
        let mut total: u64 = 0;
        loop {
            match c.recv_batch(|v| { std::hint::black_box(v); total += 1; }) {
                Ok(_) => {}
                Err(_) => break,
            }
        }
        total
    });

    let per_prod = BATCH / M;
    let work_round = Arc::new(AtomicU64::new(0));
    let done_round = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for p in ps.into_iter() {
        let work_round = work_round.clone();
        let done_round = done_round.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            p.bind();
            let mut buf: Vec<u64> = Vec::with_capacity(chunk);
            let mut last_round: u64 = 0;
            loop {
                loop {
                    if stop.load(Ordering::Acquire) { return; }
                    let r = work_round.load(Ordering::Acquire);
                    if r > last_round { last_round = r; break; }
                    std::hint::spin_loop();
                }
                let mut sent: usize = 0;
                while sent < per_prod {
                    let want = (per_prod - sent).min(chunk);
                    buf.clear();
                    for k in 0..want as u64 { buf.push(sent as u64 + k); }
                    while !buf.is_empty() {
                        let n = p.try_send_batch(&mut buf);
                        if n == 0 {
                            // Ring full → fall back to blocking send for one item.
                            let v = buf.remove(0);
                            p.send(v);
                        }
                    }
                    sent += want;
                }
                done_round.fetch_add(1, Ordering::AcqRel);
            }
        }));
    }

    for _ in 0..warmup_batches() {
        done_round.store(0, Ordering::Release);
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let wall = Instant::now();
    for _ in 0..n {
        done_round.store(0, Ordering::Release);
        let t0 = Instant::now();
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(label, lats, wall.elapsed().as_nanos() as u64);

    stop.store(true, Ordering::Release);
    work_round.fetch_add(1, Ordering::AcqRel);
    for h in handles { let _ = h.join(); }
    sd.signal();
    let _ = consumer.join().unwrap();
}

fn main() {
    println!("=== arbitro-kit route::Mpsc overhead bench ===");
    println!("rounds={} batches × BATCH={} ops each", rounds(), BATCH);

    header("A. Single-thread 1P/1C (hot path, no park)");
    bench_single_thread();

    header("B. 1P/1C cross-thread");
    bench_spsc_cross_thread();

    header("C. MP/1C fan-in (producer-side wall time per round, total cap=1024)");
    bench_mpsc_fanin::<2, 512>("mpsc 2P/1C cap=2×512");
    bench_mpsc_fanin::<4, 256>("mpsc 4P/1C cap=4×256");
    bench_mpsc_fanin::<8, 128>("mpsc 8P/1C cap=8×128");

    header("D. crossbeam::channel::bounded(1024) baselines");
    bench_crossbeam_mpsc::<2>("crossbeam 2P/1C");
    bench_crossbeam_mpsc::<4>("crossbeam 4P/1C");
    bench_crossbeam_mpsc::<8>("crossbeam 8P/1C");

    header("E. MP/1C producer-batched via try_send_batch (chunk=64, total cap=1024)");
    bench_mpsc_batched::<2, 512>("mpsc 2P/1C batched-64 cap=2×512", 64);
    bench_mpsc_batched::<4, 256>("mpsc 4P/1C batched-64 cap=4×256", 64);
    bench_mpsc_batched::<8, 128>("mpsc 8P/1C batched-64 cap=8×128", 64);

    header("F. High-fanin 100P/1C — mpsc vs crossbeam");
    bench_mpsc_fanin::<100, 16>("mpsc 100P/1C cap=100×16");
    bench_crossbeam_mpsc::<100>("crossbeam 100P/1C bounded(1024)");

    println!("\nDone.");
}
