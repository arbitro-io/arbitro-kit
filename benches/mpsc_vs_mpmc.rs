//! Mpsc vs Mpmc(M, 1) head-to-head — does the SC specialisation buy anything?
//!
//! Measures the same M:1 fan-in shape under both APIs:
//! - `Mpsc<T, RING_CAP>::new(M)`            ← single-consumer specialisation
//! - `Mpmc<T, RING_CAP>::new(M, 1)`         ← original M:N with N=1
//!
//! The hot-path difference is in `try_send`: Mpsc skips the adaptive
//! shard scan + cursor update because there is exactly one shard.
//!
//! Two scenarios:
//!   A. Cross-thread M producers blast into 1 consumer — total throughput.
//!   B. Single-thread try_send / try_recv steady-state (no parking).
//!
//! Conforms to bench_safety: ROUNDS = 1000, timeout-friendly, tee log
//! expected from runner, no background work.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use arbitro_kit::route::{Mpmc, Mpsc};

const RING_CAP: usize = 256;
const ROUNDS: usize = 1000;       // per bench_safety: max 1000 msgs per run
const RUNS: usize = 50;            // 50 measurements for stable cross-thread stats
const WARMUP: usize = 2;
const M_VARIANTS: &[usize] = &[4, 8, 16];

#[inline(never)]
fn run_mpsc(m: usize) -> u128 {
    let (ps, c, sd) = Mpsc::<u64, RING_CAP>::new(m);
    let total = m * ROUNDS;
    let received = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(m + 2));

    let received_c = received.clone();
    let b_c = barrier.clone();
    let consumer_h = thread::spawn(move || {
        c.bind();
        b_c.wait();
        let mut got = 0usize;
        while got < total {
            let _ = c.recv_batch(|_v| {
                received_c.fetch_add(1, Ordering::Relaxed);
            });
            got = received_c.load(Ordering::Relaxed);
        }
    });

    let producer_handles: Vec<_> = ps.into_iter().map(|p| {
        let b = barrier.clone();
        thread::spawn(move || {
            p.bind();
            b.wait();
            for v in 0..ROUNDS as u64 { p.send(v); }
        })
    }).collect();

    barrier.wait();
    let t0 = Instant::now();
    for h in producer_handles { h.join().unwrap(); }
    consumer_h.join().unwrap();
    let elapsed = t0.elapsed().as_nanos();
    sd.signal();
    elapsed
}

#[inline(never)]
fn run_mpmc(m: usize) -> u128 {
    let (ps, mut cs, sd) = Mpmc::<u64, RING_CAP>::new(m, 1);
    let c = cs.remove(0);
    let total = m * ROUNDS;
    let received = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(m + 2));

    let received_c = received.clone();
    let b_c = barrier.clone();
    let consumer_h = thread::spawn(move || {
        c.bind();
        b_c.wait();
        let mut got = 0usize;
        while got < total {
            let _ = c.recv_batch(|_v| {
                received_c.fetch_add(1, Ordering::Relaxed);
            });
            got = received_c.load(Ordering::Relaxed);
        }
    });

    let producer_handles: Vec<_> = ps.into_iter().map(|p| {
        let b = barrier.clone();
        thread::spawn(move || {
            p.bind();
            b.wait();
            for v in 0..ROUNDS as u64 { p.send(v); }
        })
    }).collect();

    barrier.wait();
    let t0 = Instant::now();
    for h in producer_handles { h.join().unwrap(); }
    consumer_h.join().unwrap();
    let elapsed = t0.elapsed().as_nanos();
    sd.signal();
    elapsed
}

/// Single-thread `try_send` then `try_recv` — measures the pure hot-path
/// cost (no cross-CPU traffic, no park/unpark). With M=1 this is the
/// cleanest comparison between the SC specialisation and the M:N general
/// path.
fn run_mpsc_st_hotpath(m: usize) -> u128 {
    let (ps, c, _sd) = Mpsc::<u64, RING_CAP>::new(m);
    let total = m * ROUNDS;
    let t0 = Instant::now();
    // Round-robin: send one to each producer, drain after every M sends.
    for round in 0..ROUNDS {
        for p in &ps {
            let _ = p.try_send(round as u64);
        }
        // Drain to keep rings from filling.
        let mut taken = 0;
        while taken < ps.len() {
            if c.try_recv().is_some() { taken += 1; }
        }
    }
    let elapsed = t0.elapsed().as_nanos();
    let _ = total;
    elapsed
}

fn run_mpmc_st_hotpath(m: usize) -> u128 {
    let (ps, mut cs, _sd) = Mpmc::<u64, RING_CAP>::new(m, 1);
    let c = cs.remove(0);
    let total = m * ROUNDS;
    let t0 = Instant::now();
    for round in 0..ROUNDS {
        for p in &ps {
            let _ = p.try_send(round as u64);
        }
        let mut taken = 0;
        while taken < ps.len() {
            if c.try_recv().is_some() { taken += 1; }
        }
    }
    let elapsed = t0.elapsed().as_nanos();
    let _ = total;
    elapsed
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() { return 0; }
    let idx = ((sorted.len() as f64 - 1.0) * p) as usize;
    sorted[idx]
}

fn measure(name: &str, m: usize, total_msgs: usize, runs: impl Fn() -> u128) {
    // Warmup
    for _ in 0..WARMUP { let _ = runs(); }
    let mut samples: Vec<u128> = (0..RUNS).map(|_| runs()).collect();
    samples.sort();
    let min = samples[0];
    let p50 = percentile(&samples, 0.5);
    let max = *samples.last().unwrap();
    let ns_per_msg_min = min as f64 / total_msgs as f64;
    let ns_per_msg_p50 = p50 as f64 / total_msgs as f64;
    let mps = (total_msgs as f64) / (min as f64 / 1e9);
    println!(
        "{:<28} M={:<3}  min={:>8.0}us  p50={:>8.0}us  max={:>8.0}us  ns/msg(min)={:>7.1}  ns/msg(p50)={:>7.1}  msg/s={:>11.0}",
        name, m,
        min as f64 / 1e3,
        p50 as f64 / 1e3,
        max as f64 / 1e3,
        ns_per_msg_min,
        ns_per_msg_p50,
        mps,
    );
}

fn main() {
    println!("=== Mpsc vs Mpmc(M,1) head-to-head ===");
    println!("RING_CAP={}  ROUNDS={}  RUNS={}  WARMUP={}", RING_CAP, ROUNDS, RUNS, WARMUP);
    println!();

    println!("── A. Cross-thread (M producer threads → 1 consumer thread) ──");
    for &m in M_VARIANTS {
        let total = m * ROUNDS;
        measure("mpsc(M)        cross", m, total, || run_mpsc(m));
        measure("mpmc(M,1)      cross", m, total, || run_mpmc(m));
        println!();
    }

    println!("── B. Single-thread try_send/try_recv (no park) ──");
    for &m in M_VARIANTS {
        let total = m * ROUNDS;
        measure("mpsc(M)         st  ", m, total, || run_mpsc_st_hotpath(m));
        measure("mpmc(M,1)       st  ", m, total, || run_mpmc_st_hotpath(m));
        println!();
    }

    println!("Done.");
}
