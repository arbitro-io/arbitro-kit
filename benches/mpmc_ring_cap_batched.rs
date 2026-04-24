//! RING_CAP sweep with BATCHED producer (try_send_batch).
//!
//! Same shapes as mpmc_ring_cap but producer drains a pre-filled Vec<u64>
//! via try_send_batch — so each call amortizes up to RING_CAP head.store +
//! full_set.release across K items.
//!
//! BATCH=1000, rounds capped, timeout+tee expected.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::gate::{Mpmc, MpmcProducer, MpmcConsumer, MpmcShutdown};

const BATCH: usize = 1000;

fn rounds() -> usize {
    std::env::var("BENCH_ROUNDS")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(100)
}

fn row(name: &str, mut ns: Vec<u64>, elapsed: u64) {
    ns.sort_unstable();
    let n = ns.len();
    let total = n * BATCH;
    let mean = elapsed as f64 / total as f64;
    let p50 = ns[n/2] as f64 / BATCH as f64;
    let p99 = ns[n*99/100] as f64 / BATCH as f64;
    let ops = (total as f64) / (elapsed as f64 / 1e9);
    println!("{:<38} {:>10.2} {:>10.2} {:>10.2} {:>14}",
             name, mean, p50, p99, ops as u64);
}

fn header(title: &str) {
    println!("\n── {} ──", title);
    println!("{:<38} {:>10} {:>10} {:>10} {:>14}",
             "variant", "mean", "p50", "p99", "ops/sec");
    println!("{}", "─".repeat(88));
}

// MP/1C batched fan-in
fn mpsc<const M: usize, const RC: usize>(label: &str) {
    let (ps, cs, sd): (Vec<MpmcProducer<u64, RC>>, Vec<MpmcConsumer<u64, RC>>, MpmcShutdown<u64, RC>) =
        Mpmc::<u64, RC>::new(M, 1);
    let c = cs.into_iter().next().unwrap();
    let consumer = thread::spawn(move || {
        c.bind();
        let mut n: u64 = 0;
        loop {
            match c.recv_batch(|v| { std::hint::black_box(v); n += 1; }) {
                Ok(_) => {} Err(_) => break,
            }
        }
        n
    });

    let per = BATCH / M;
    let work = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut hs = Vec::new();
    for p in ps.into_iter() {
        let (w, d, s) = (work.clone(), done.clone(), stop.clone());
        hs.push(thread::spawn(move || {
            p.bind();
            let mut last = 0u64;
            let mut buf: Vec<u64> = Vec::with_capacity(per);
            loop {
                loop {
                    if s.load(Ordering::Acquire) { return; }
                    let r = w.load(Ordering::Acquire);
                    if r > last { last = r; break; }
                    std::hint::spin_loop();
                }
                buf.clear();
                for k in 0..per as u64 { buf.push(k); }
                while !buf.is_empty() {
                    let sent = p.try_send_batch(&mut buf);
                    if sent == 0 { std::hint::spin_loop(); }
                }
                d.fetch_add(1, Ordering::AcqRel);
            }
        }));
    }

    for _ in 0..10 {
        done.store(0, Ordering::Release);
        work.fetch_add(1, Ordering::AcqRel);
        while done.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let wall = Instant::now();
    for _ in 0..n {
        done.store(0, Ordering::Release);
        let t0 = Instant::now();
        work.fetch_add(1, Ordering::AcqRel);
        while done.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(label, lats, wall.elapsed().as_nanos() as u64);

    stop.store(true, Ordering::Release);
    work.fetch_add(1, Ordering::AcqRel);
    for h in hs { let _ = h.join(); }
    sd.signal();
    let _ = consumer.join().unwrap();
}

// MP/NC symmetric batched
fn sym<const M: usize, const N: usize, const RC: usize>(label: &str) {
    let (ps, cs, sd): (Vec<MpmcProducer<u64, RC>>, Vec<MpmcConsumer<u64, RC>>, MpmcShutdown<u64, RC>) =
        Mpmc::<u64, RC>::new(M, N);

    let cons: Vec<_> = cs.into_iter().map(|c| thread::spawn(move || {
        c.bind();
        let mut n: u64 = 0;
        loop {
            match c.recv_batch(|v| { std::hint::black_box(v); n += 1; }) {
                Ok(_) => {} Err(_) => break,
            }
        }
        n
    })).collect();

    let per = BATCH / M;
    let work = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut hs = Vec::new();
    for p in ps.into_iter() {
        let (w, d, s) = (work.clone(), done.clone(), stop.clone());
        hs.push(thread::spawn(move || {
            p.bind();
            let mut last = 0u64;
            let mut buf: Vec<u64> = Vec::with_capacity(per);
            loop {
                loop {
                    if s.load(Ordering::Acquire) { return; }
                    let r = w.load(Ordering::Acquire);
                    if r > last { last = r; break; }
                    std::hint::spin_loop();
                }
                buf.clear();
                for k in 0..per as u64 { buf.push(k); }
                while !buf.is_empty() {
                    let sent = p.try_send_batch(&mut buf);
                    if sent == 0 { std::hint::spin_loop(); }
                }
                d.fetch_add(1, Ordering::AcqRel);
            }
        }));
    }

    for _ in 0..10 {
        done.store(0, Ordering::Release);
        work.fetch_add(1, Ordering::AcqRel);
        while done.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let wall = Instant::now();
    for _ in 0..n {
        done.store(0, Ordering::Release);
        let t0 = Instant::now();
        work.fetch_add(1, Ordering::AcqRel);
        while done.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(label, lats, wall.elapsed().as_nanos() as u64);

    stop.store(true, Ordering::Release);
    work.fetch_add(1, Ordering::AcqRel);
    for h in hs { let _ = h.join(); }
    sd.signal();
    for h in cons { let _ = h.join().unwrap(); }
}

fn main() {
    println!("=== RING_CAP sweep (BATCHED producer via try_send_batch) ===");
    println!("rounds={}  BATCH={}", rounds(), BATCH);

    header("2P/1C batched — RING_CAP sweep");
    mpsc::<2, 16>("2P/1C  RC=16");
    mpsc::<2, 64>("2P/1C  RC=64");
    mpsc::<2, 256>("2P/1C  RC=256");
    mpsc::<2, 1024>("2P/1C  RC=1024");

    header("4P/1C batched — RING_CAP sweep");
    mpsc::<4, 16>("4P/1C  RC=16");
    mpsc::<4, 64>("4P/1C  RC=64");
    mpsc::<4, 256>("4P/1C  RC=256");
    mpsc::<4, 1024>("4P/1C  RC=1024");

    header("8P/1C batched — RING_CAP sweep");
    mpsc::<8, 16>("8P/1C  RC=16");
    mpsc::<8, 64>("8P/1C  RC=64");
    mpsc::<8, 256>("8P/1C  RC=256");
    mpsc::<8, 1024>("8P/1C  RC=1024");

    header("2P/2C batched — RING_CAP sweep");
    sym::<2, 2, 16>("2P/2C  RC=16");
    sym::<2, 2, 64>("2P/2C  RC=64");
    sym::<2, 2, 256>("2P/2C  RC=256");
    sym::<2, 2, 1024>("2P/2C  RC=1024");

    header("4P/4C batched — RING_CAP sweep");
    sym::<4, 4, 16>("4P/4C  RC=16");
    sym::<4, 4, 64>("4P/4C  RC=64");
    sym::<4, 4, 256>("4P/4C  RC=256");
    sym::<4, 4, 1024>("4P/4C  RC=1024");

    header("8P/8C batched — RING_CAP sweep");
    sym::<8, 8, 16>("8P/8C  RC=16");
    sym::<8, 8, 64>("8P/8C  RC=64");
    sym::<8, 8, 256>("8P/8C  RC=256");
    sym::<8, 8, 1024>("8P/8C  RC=1024");

    println!("\nDone.");
}
