//! Mpmc overhead bench — M producers / N consumers, sharded.
//!
//! Thesis: `gate::Mpmc` is Hub's "bit-is-signal" pattern extended to M:N.
//! Each (producer, shard) pair is its own SPSC slot; one `fetch_or` on the
//! shard's SignalSet both publishes the slot and wakes the consumer. The
//! consumer's `acquire_any` park wakes **once** and drains the whole
//! set-bit bitmap in a single pass — batching emerges for free.
//!
//! Baseline we want to beat: `crossbeam::channel::bounded(CAP)` in the
//! MPMC shape. Secondary sanity: 1P/1C vs `Ring`.
//!
//! Sections:
//!   A. Single-thread 1P/1C — hot path cost (no park).
//!   B. 1P/1C cross-thread — vs crossbeam::channel::bounded.
//!   C. MP/1C fan-in          (M = 2, 4, 8).
//!   D. 1P/NC fan-out         (N = 2, 4, 8).
//!   E. MP/NC symmetric       (M=N = 2, 4, 8).
//!   F. vs crossbeam::channel::bounded (same C/D/E shapes).
//!
//! Conforms to bench_safety: BATCH = 1000, rounds capped, tee log expected
//! from runner, no background work.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::gate::{Mpmc, MpmcConsumer, MpmcProducer, MpmcShutdown};
use crossbeam_channel::bounded;

const BATCH: usize = 1000;
const SHARD_CAP_UNUSED: usize = 0; // Mpmc is sized by (m, n), no CAP param.

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

// ── A. Single-thread: one producer writes, same thread drains ───────────
//
// No cross-core traffic, no park. Isolates the adaptive-routing scan +
// `fetch_or` + `try_recv` bitmap pop.
fn bench_single_thread() {
    let (ps, cs, sd): (Vec<MpmcProducer<u64>>, Vec<MpmcConsumer<u64>>, MpmcShutdown<u64>) =
        Mpmc::<u64>::new(1, 1);
    let p = &ps[0];
    let c = &cs[0];

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
    row("mpmc 1P/1C single-thread", lats, wall.elapsed().as_nanos() as u64);

    drop(sd);
    let _ = SHARD_CAP_UNUSED;
}

// ── B. 1P/1C cross-thread ──────────────────────────────────────────────
fn bench_spsc_cross_thread() {
    let (mut ps, mut cs, sd) = Mpmc::<u64>::new(1, 1);
    let p = ps.pop().unwrap();
    let c = cs.pop().unwrap();

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
    row("mpmc 1P/1C cross-thread", lats, wall.elapsed().as_nanos() as u64);

    sd.signal();
    let _ = consumer.join().unwrap();
}

// ── C. MPSC: M producers → 1 consumer ─────────────────────────────────
//
// Flagship fan-in. Each round, each of M producers sends BATCH/M messages.
// We time the full set until the producer phase is done. Consumer drains
// in background using `recv_batch` for amortization.
fn bench_mpsc<const M: usize>(label: &str) {
    let (ps, cs, sd) = Mpmc::<u64>::new(M, 1);
    let c = cs.into_iter().next().unwrap();

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

    // Per-producer thread with a bounded work signal.
    let per_prod = BATCH / M;
    let work_round = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let done_round = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    let ps_vec: Vec<MpmcProducer<u64>> = ps.into_iter().collect();
    for p in ps_vec.into_iter() {
        let work_round = work_round.clone();
        let done_round = done_round.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            p.bind();
            let mut last_round: u64 = 0;
            loop {
                // Wait for next round signal.
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

    // Warmup rounds.
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
    work_round.fetch_add(1, Ordering::AcqRel); // unblock producers
    for h in handles { let _ = h.join(); }
    sd.signal();
    let _ = consumer.join().unwrap();
}

// ── D. SPMC: 1 producer → N consumers ─────────────────────────────────
fn bench_spmc<const N: usize>(label: &str) {
    let (mut ps, cs, sd) = Mpmc::<u64>::new(1, N);
    let p = ps.pop().unwrap();

    let handles: Vec<_> = cs
        .into_iter()
        .map(|c| thread::spawn(move || {
            c.bind();
            let mut count: u64 = 0;
            loop {
                match c.recv_batch(|v| { std::hint::black_box(v); count += 1; }) {
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            count
        }))
        .collect();

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
    row(label, lats, wall.elapsed().as_nanos() as u64);

    sd.signal();
    for h in handles { let _ = h.join().unwrap(); }
}

// ── E. MP/NC symmetric ────────────────────────────────────────────────
fn bench_mpmc_symmetric<const M: usize, const N: usize>(label: &str) {
    let (ps, cs, sd) = Mpmc::<u64>::new(M, N);

    let consumer_handles: Vec<_> = cs
        .into_iter()
        .map(|c| thread::spawn(move || {
            c.bind();
            let mut count: u64 = 0;
            loop {
                match c.recv_batch(|v| { std::hint::black_box(v); count += 1; }) {
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            count
        }))
        .collect();

    let per_prod = BATCH / M;
    let work_round = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let done_round = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut prod_handles = Vec::new();
    for p in ps.into_iter() {
        let work_round = work_round.clone();
        let done_round = done_round.clone();
        let stop = stop.clone();
        prod_handles.push(thread::spawn(move || {
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
    for h in prod_handles { let _ = h.join(); }
    sd.signal();
    for h in consumer_handles { let _ = h.join().unwrap(); }
}

// ── F. Crossbeam baselines ────────────────────────────────────────────
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
    let work_round = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let done_round = Arc::new(std::sync::atomic::AtomicUsize::new(0));
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

fn bench_crossbeam_spmc<const N: usize>(label: &str) {
    let (tx, rx) = bounded::<u64>(1024);

    let handles: Vec<_> = (0..N).map(|_| {
        let rx = rx.clone();
        thread::spawn(move || {
            let mut count: u64 = 0;
            while let Ok(v) = rx.recv() {
                std::hint::black_box(v); count += 1;
            }
            count
        })
    }).collect();
    drop(rx);

    for _ in 0..warmup_batches() {
        for k in 0..BATCH as u64 { let _ = tx.send(k); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let wall = Instant::now();
    for _ in 0..n {
        let t0 = Instant::now();
        for k in 0..BATCH as u64 { let _ = tx.send(k); }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(label, lats, wall.elapsed().as_nanos() as u64);

    drop(tx);
    for h in handles { let _ = h.join().unwrap(); }
}

fn bench_crossbeam_mpmc<const M: usize, const N: usize>(label: &str) {
    let (tx, rx) = bounded::<u64>(1024);

    let cons_handles: Vec<_> = (0..N).map(|_| {
        let rx = rx.clone();
        thread::spawn(move || {
            let mut count: u64 = 0;
            while let Ok(v) = rx.recv() {
                std::hint::black_box(v); count += 1;
            }
            count
        })
    }).collect();
    drop(rx);

    let per_prod = BATCH / M;
    let work_round = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let done_round = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut prod_handles = Vec::new();
    for _ in 0..M {
        let tx = tx.clone();
        let work_round = work_round.clone();
        let done_round = done_round.clone();
        let stop = stop.clone();
        prod_handles.push(thread::spawn(move || {
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
    for h in prod_handles { let _ = h.join(); }
    for h in cons_handles { let _ = h.join().unwrap(); }
}

fn main() {
    println!("=== arbitro-kit gate::Mpmc overhead bench ===");
    println!("rounds={} batches × BATCH={} ops each", rounds(), BATCH);

    header("A. Single-thread 1P/1C (hot path, no park)");
    bench_single_thread();

    header("B. 1P/1C cross-thread");
    bench_spsc_cross_thread();

    header("C. MP/1C fan-in (producer-side wall time per round)");
    bench_mpsc::<2>("mpmc 2P/1C");
    bench_mpsc::<4>("mpmc 4P/1C");
    bench_mpsc::<8>("mpmc 8P/1C");

    header("D. 1P/NC fan-out");
    bench_spmc::<2>("mpmc 1P/2C");
    bench_spmc::<4>("mpmc 1P/4C");
    bench_spmc::<8>("mpmc 1P/8C");

    header("E. MP/NC symmetric");
    bench_mpmc_symmetric::<2, 2>("mpmc 2P/2C");
    bench_mpmc_symmetric::<4, 4>("mpmc 4P/4C");
    bench_mpmc_symmetric::<8, 8>("mpmc 8P/8C");

    header("F. crossbeam::channel::bounded(1024) baselines");
    bench_crossbeam_mpsc::<2>("crossbeam 2P/1C");
    bench_crossbeam_mpsc::<4>("crossbeam 4P/1C");
    bench_crossbeam_mpsc::<8>("crossbeam 8P/1C");
    bench_crossbeam_spmc::<2>("crossbeam 1P/2C");
    bench_crossbeam_spmc::<4>("crossbeam 1P/4C");
    bench_crossbeam_spmc::<8>("crossbeam 1P/8C");
    bench_crossbeam_mpmc::<2, 2>("crossbeam 2P/2C");
    bench_crossbeam_mpmc::<4, 4>("crossbeam 4P/4C");
    bench_crossbeam_mpmc::<8, 8>("crossbeam 8P/8C");

    println!("\nDone.");
}
