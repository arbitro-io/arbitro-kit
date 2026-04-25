//! Apples-to-apples: arbitro Ring vs disruptor (LMAX port) vs crossbeam.
//!
//! All three: 1 producer thread, 1 consumer thread (SPSC). MSGS items
//! of u64 payload. Producer fires MSGS items, consumer receives all of
//! them. Time = from producer start until last item is observed by the
//! consumer. Best of N rounds.
//!
//! For arbitro Ring and crossbeam_channel, the consumer thread is joined
//! after MSGS pulls. For disruptor, the consumer is a closure managed by
//! the framework; we spin on an AtomicU64 counter the closure increments
//! and stop the timer when it reaches MSGS.
//!
//! disruptor uses BusySpin wait strategy (their fastest), which burns
//! 100% CPU on the consumer core — matching their published benchmarks
//! for the same scenario. Ring and crossbeam use blocking recv (Park).
//! That asymmetry IS the comparison point: spin vs park.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::stream::Ring;
use disruptor::{BusySpin, Producer};

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

// ─── disruptor (LMAX port) ────────────────────────────────────────────────
struct Event { v: u64 }

fn run_disruptor(cap: usize) -> f64 {
    let counter = Arc::new(AtomicU64::new(0));
    let counter_c = counter.clone();

    let factory = || Event { v: 0 };
    let processor = move |_e: &Event, _seq: i64, _eob: bool| {
        counter_c.fetch_add(1, Ordering::Release);
    };

    let mut producer = disruptor::build_single_producer(cap, factory, BusySpin)
        .handle_events_with(processor)
        .build();

    let t0 = Instant::now();
    for i in 0..MSGS as u64 {
        producer.publish(|e| { e.v = i; });
    }
    // Wait for the consumer to have seen all MSGS items.
    while counter.load(Ordering::Acquire) < MSGS as u64 {
        std::hint::spin_loop();
    }
    let ns = t0.elapsed().as_nanos() as f64;
    // `producer` drops here → disruptor shuts down its worker thread.
    drop(producer);
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
    println!("{:<36} {:>10.1} {:>10.1} {:>14.0}",
             name, min, p50, 1e9 / min);
}

fn main() {
    println!("=== Ring vs disruptor vs crossbeam — same methodology ===");
    println!("All: 1P/1C threads, {} msgs of u64, time = first send → last recv.", MSGS);
    println!("Best of {} rounds (after {} warmup).", ROUNDS, WARMUP);
    println!("disruptor uses BusySpin (100% CPU consumer); Ring + crossbeam park.");
    println!();
    println!("{:<36} {:>10} {:>10} {:>14}",
             "variant", "min ns", "p50 ns", "ops/sec (min)");
    println!("{}", "─".repeat(74));

    for &cap in &[16usize, 64, 256, 1024] {
        let (min, p50) = match cap {
            16   => collect(|| run_ring::<16>()),
            64   => collect(|| run_ring::<64>()),
            256  => collect(|| run_ring::<256>()),
            1024 => collect(|| run_ring::<1024>()),
            _ => unreachable!(),
        };
        row(&format!("arbitro Ring<u64, {}>", cap), min, p50);

        let (min, p50) = collect(|| run_disruptor(cap));
        row(&format!("disruptor (size={})", cap), min, p50);

        let (min, p50) = collect(|| run_crossbeam(cap));
        row(&format!("crossbeam bounded({})", cap), min, p50);
        println!();
    }

    println!("Done.");
}
