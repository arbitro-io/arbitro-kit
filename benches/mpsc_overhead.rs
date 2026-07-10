//! Mpsc overhead bench — kit::Mpsc vs crossbeam::channel::bounded.
//!
//! Scenarios:
//!   A. Single-thread 1P/1C — hot path (no park, no cross-core).
//!   B. 1P/1C cross-thread   — producer sends N frames, consumer drains,
//!                             then producer signals shutdown → consumer exits.
//!
//! Conforms to bench_safety: BATCH = 1000, rounds capped, tee log expected,
//! no background work.

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::route::Mpsc;
use crossbeam_channel::bounded;

const CAP: usize = 64;
const BATCH: usize = 1000;

fn rounds() -> usize {
    std::env::var("BENCH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50)
}
fn warmup_batches() -> usize {
    5
}

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

// ── A. Single-thread 1P/1C — hot path ────────────────────────────────────
fn bench_single_thread() {
    header("A. Single-thread 1P/1C (hot path, no park)");

    // kit::Mpsc
    {
        let (mut ps, mut c, _sd) = Mpsc::<u64, CAP>::new(1);
        let mut p = ps.remove(0);
        p.bind();
        c.bind();

        let mut do_batch = || {
            let t0 = Instant::now();
            let mut sent = 0usize;
            let mut drained = 0usize;
            while drained < BATCH {
                while sent < BATCH && p.try_send(sent as u64).is_ok() {
                    sent += 1;
                }
                drained += c.try_recv_batch(|_| {});
            }
            t0.elapsed().as_nanos() as u64
        };
        for _ in 0..warmup_batches() {
            do_batch();
        }
        let mut samples = Vec::with_capacity(rounds());
        let t_start = Instant::now();
        for _ in 0..rounds() {
            samples.push(do_batch());
        }
        let total = t_start.elapsed().as_nanos() as u64;
        row("kit::Mpsc 1P/1C", samples, total);
    }

    // crossbeam bounded
    {
        let (tx, rx) = bounded::<u64>(CAP);
        let mut do_batch = || {
            let t0 = Instant::now();
            for i in 0..BATCH {
                while tx.try_send(i as u64).is_err() {
                    while rx.try_recv().is_ok() {}
                }
            }
            while rx.try_recv().is_ok() {}
            t0.elapsed().as_nanos() as u64
        };
        for _ in 0..warmup_batches() {
            do_batch();
        }
        let mut samples = Vec::with_capacity(rounds());
        let t_start = Instant::now();
        for _ in 0..rounds() {
            samples.push(do_batch());
        }
        let total = t_start.elapsed().as_nanos() as u64;
        row("crossbeam::bounded 1P/1C", samples, total);
    }
}

// ── B. 1P/1C cross-thread ────────────────────────────────────────────────
//
// Round = producer sends BATCH via send(), consumer drains BATCH via recv().
// Uses a shared counter to know when the consumer has drained the batch.
// Between rounds the producer waits for the consumer to catch up.
fn bench_cross_thread() {
    header("B. 1P/1C cross-thread (park-based wake)");

    let n_rounds = warmup_batches() + rounds();

    // kit::Mpsc
    {
        let (mut ps, mut c, _sd) = Mpsc::<u64, CAP>::new(1);
        let mut p = ps.remove(0);
        p.bind();

        let total_target: u64 = (n_rounds as u64) * (BATCH as u64);
        let done = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let done_c = Arc::clone(&done);

        let cons = thread::spawn(move || {
            c.bind();
            loop {
                let d = done_c.load(std::sync::atomic::Ordering::Acquire);
                if d >= total_target {
                    break;
                }
                match c.recv_batch(|_| {
                    done_c.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                }) {
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });

        // Warmup
        for _ in 0..warmup_batches() {
            for i in 0..BATCH {
                p.send(i as u64);
            }
        }

        let mut samples = Vec::with_capacity(rounds());
        let t_start = Instant::now();
        for _ in 0..rounds() {
            let t0 = Instant::now();
            for i in 0..BATCH {
                p.send(i as u64);
            }
            samples.push(t0.elapsed().as_nanos() as u64);
        }
        let total = t_start.elapsed().as_nanos() as u64;

        // wait for consumer to drain
        while done.load(std::sync::atomic::Ordering::Acquire) < total_target {
            std::hint::spin_loop();
        }
        drop(p);
        drop(ps);
        drop(_sd);
        let _ = cons.join();
        row("kit::Mpsc 1P/1C xthread", samples, total);
    }

    // crossbeam
    {
        let (tx, rx) = bounded::<u64>(CAP);

        let total_target: u64 = (n_rounds as u64) * (BATCH as u64);
        let done = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let done_c = Arc::clone(&done);

        let cons = thread::spawn(move || {
            let mut got = 0u64;
            while got < total_target {
                match rx.recv() {
                    Ok(_) => {
                        got += 1;
                        done_c.store(got, std::sync::atomic::Ordering::Release);
                    }
                    Err(_) => break,
                }
            }
        });

        for _ in 0..warmup_batches() {
            for i in 0..BATCH {
                tx.send(i as u64).unwrap();
            }
        }

        let mut samples = Vec::with_capacity(rounds());
        let t_start = Instant::now();
        for _ in 0..rounds() {
            let t0 = Instant::now();
            for i in 0..BATCH {
                tx.send(i as u64).unwrap();
            }
            samples.push(t0.elapsed().as_nanos() as u64);
        }
        let total = t_start.elapsed().as_nanos() as u64;

        while done.load(std::sync::atomic::Ordering::Acquire) < total_target {
            std::hint::spin_loop();
        }
        drop(tx);
        let _ = cons.join();
        row("crossbeam::bounded 1P/1C xthread", samples, total);
    }
}

fn main() {
    println!(
        "mpsc_overhead — CAP={} BATCH={} rounds={} warmup={}",
        CAP,
        BATCH,
        rounds(),
        warmup_batches()
    );

    bench_single_thread();
    bench_cross_thread();

    println!("\ndone.");
}
