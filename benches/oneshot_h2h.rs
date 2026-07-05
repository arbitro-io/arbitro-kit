//! `oneshot_h2h` — head-to-head bench:
//!   - `arbitro_kit::gate::OneSignal`     (payloadless, single-use)
//!   - `arbitro_kit::route::OneShot<T>`   (payload-carrying, single-use)
//!   - `tokio::sync::oneshot::channel<T>` (async, blocked via current_thread runtime)
//!
//! Three scenarios per primitive:
//!   A. Same-thread already-released — pure fast-path overhead.
//!   B. Cross-thread, immediate release — receiver likely catches in spin.
//!   C. Cross-thread, sender does ~50 µs busy work — receiver parks for sure.
//!
//! Methodology mirrors the rest of the bench suite: warmup + 500 rounds,
//! report min/p50/p99 ns/op + ops/sec. Each round creates a fresh pair
//! (these are single-use primitives), so per-round cost includes
//! allocation + handle setup. That's the realistic cost of using these
//! primitives — you don't reuse them.

use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use arbitro_kit::gate::OneSignal;
use arbitro_kit::route::OneShot;
use arbitro_kit::slot::Pipe;
use arbitro_kit::waiter::ParkWaiter;

const ROUNDS: usize = 500;
const WARMUP: usize = 50;

fn pct(samples: &mut [u128], q: f64) -> u128 {
    samples.sort_unstable();
    let idx = ((samples.len() as f64) * q).clamp(0.0, (samples.len() - 1) as f64) as usize;
    samples[idx]
}

fn report(label: &str, samples: &mut [u128]) {
    let mean = (samples.iter().sum::<u128>() as f64) / (samples.len() as f64);
    let p50 = pct(samples, 0.50);
    let p99 = pct(samples, 0.99);
    let min = *samples.iter().min().unwrap();
    let ops_sec = if mean > 0.0 { 1e9 / mean } else { 0.0 };
    println!(
        "{:<46}  {:>10.2}  {:>10}  {:>10}  {:>10}  {:>14.0}",
        label, mean, min, p50, p99, ops_sec
    );
}

fn header(title: &str) {
    println!("\n── {} ──", title);
    println!(
        "{:<46}  {:>10}  {:>10}  {:>10}  {:>10}  {:>14}",
        "variant", "mean_ns", "min_ns", "p50_ns", "p99_ns", "ops/sec"
    );
    println!("{}", "─".repeat(46 + 4 + 10 * 4 + 4 * 4 + 14 + 4));
}

// ─── A. Same-thread already-released ──────────────────────────────────────

fn a_one_signal() {
    // Warmup
    for _ in 0..WARMUP {
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
        rx.bind();
        tx.release();
        let _ = rx.acquire();
    }
    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
        rx.bind();
        let t0 = Instant::now();
        tx.release();
        let _ = rx.acquire();
        samples.push(t0.elapsed().as_nanos());
    }
    report("OneSignal (kit)", &mut samples);
}

fn a_oneshot_kit() {
    for _ in 0..WARMUP {
        let (tx, rx) = OneShot::<u64>::new();
        let _ = tx.send(42);
        let _ = rx.recv();
    }
    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let (tx, rx) = OneShot::<u64>::new();
        let t0 = Instant::now();
        let _ = tx.send(42);
        let _ = rx.recv();
        samples.push(t0.elapsed().as_nanos());
    }
    report("OneShot<u64> (kit)", &mut samples);
}

fn a_oneshot_tokio() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for _ in 0..WARMUP {
        let (tx, rx) = tokio::sync::oneshot::channel::<u64>();
        let _ = tx.send(42);
        let _ = rt.block_on(rx);
    }
    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let (tx, rx) = tokio::sync::oneshot::channel::<u64>();
        let t0 = Instant::now();
        let _ = tx.send(42);
        let _ = rt.block_on(rx);
        samples.push(t0.elapsed().as_nanos());
    }
    report("tokio::oneshot<u64>", &mut samples);
}

fn a_pipe_kit() {
    // Pipe is reusable, but to match the per-round allocation cost of the
    // other primitives we create a fresh one each round.
    for _ in 0..WARMUP {
        let pipe: Pipe<u64> = Pipe::new();
        pipe.send(42);
        let _ = pipe.recv();
    }
    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let pipe: Pipe<u64> = Pipe::new();
        let t0 = Instant::now();
        pipe.send(42);
        let _ = pipe.recv();
        samples.push(t0.elapsed().as_nanos());
    }
    report("Pipe<u64> (kit, slot)", &mut samples);
}

// ─── B. Cross-thread, immediate release ───────────────────────────────────

fn b_one_signal() {
    let mut samples = Vec::with_capacity(ROUNDS);
    for r in 0..(WARMUP + ROUNDS) {
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
        let barrier = Arc::new(Barrier::new(2));
        let b2 = barrier.clone();
        let handle = thread::spawn(move || {
            rx.bind();
            b2.wait();
            let t0 = Instant::now();
            let _ = rx.acquire();
            t0.elapsed().as_nanos()
        });
        barrier.wait();
        tx.release();
        let dt = handle.join().unwrap();
        if r >= WARMUP {
            samples.push(dt);
        }
    }
    report("OneSignal (kit)", &mut samples);
}

fn b_oneshot_kit() {
    let mut samples = Vec::with_capacity(ROUNDS);
    for r in 0..(WARMUP + ROUNDS) {
        let (tx, rx) = OneShot::<u64>::new();
        let barrier = Arc::new(Barrier::new(2));
        let b2 = barrier.clone();
        let handle = thread::spawn(move || {
            rx.bind(); // ← register thread for unpark
            b2.wait();
            let t0 = Instant::now();
            let _ = rx.recv();
            t0.elapsed().as_nanos()
        });
        barrier.wait();
        let _ = tx.send(42);
        let dt = handle.join().unwrap();
        if r >= WARMUP {
            samples.push(dt);
        }
    }
    report("OneShot<u64> (kit)", &mut samples);
}

fn b_oneshot_tokio() {
    let mut samples = Vec::with_capacity(ROUNDS);
    for r in 0..(WARMUP + ROUNDS) {
        let (tx, rx) = tokio::sync::oneshot::channel::<u64>();
        let barrier = Arc::new(Barrier::new(2));
        let b2 = barrier.clone();
        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            b2.wait();
            let t0 = Instant::now();
            let _ = rt.block_on(rx);
            t0.elapsed().as_nanos()
        });
        barrier.wait();
        let _ = tx.send(42);
        let dt = handle.join().unwrap();
        if r >= WARMUP {
            samples.push(dt);
        }
    }
    report("tokio::oneshot<u64>", &mut samples);
}

fn b_pipe_kit() {
    let mut samples = Vec::with_capacity(ROUNDS);
    for r in 0..(WARMUP + ROUNDS) {
        let pipe = Arc::new(Pipe::<u64>::new());
        let barrier = Arc::new(Barrier::new(2));
        let p2 = pipe.clone();
        let b2 = barrier.clone();
        let handle = thread::spawn(move || {
            p2.set_consumer(thread::current());
            b2.wait();
            let t0 = Instant::now();
            let _ = p2.recv();
            t0.elapsed().as_nanos()
        });
        barrier.wait();
        pipe.send(42);
        let dt = handle.join().unwrap();
        if r >= WARMUP {
            samples.push(dt);
        }
    }
    report("Pipe<u64> (kit, slot)", &mut samples);
}

// ─── C. Cross-thread, sender delays so receiver definitely parks ──────────
//
// The sender does ~50 µs of cheap work after the barrier before releasing.
// 50 µs >> the 64 + 512 spin budget of any of these primitives, so the
// receiver pays a full park/unpark syscall round-trip.

#[inline(never)]
fn busy_50us() {
    let t0 = Instant::now();
    let mut x: u64 = 0;
    while t0.elapsed() < Duration::from_micros(50) {
        x = x.wrapping_add(1);
        std::hint::black_box(x);
    }
    std::hint::black_box(x);
}

fn c_one_signal() {
    let mut samples = Vec::with_capacity(ROUNDS);
    for r in 0..(WARMUP + ROUNDS) {
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
        let barrier = Arc::new(Barrier::new(2));
        let b2 = barrier.clone();
        let handle = thread::spawn(move || {
            rx.bind();
            b2.wait();
            let t0 = Instant::now();
            let _ = rx.acquire();
            t0.elapsed().as_nanos()
        });
        barrier.wait();
        busy_50us();
        let t_release = Instant::now();
        tx.release();
        let dt_full = handle.join().unwrap();
        // Subtract the 50 µs busy work to isolate the wakeup cost.
        let dt_wake = dt_full.saturating_sub(t_release.elapsed().as_nanos());
        let _ = dt_wake;
        if r >= WARMUP {
            samples.push(dt_full);
        }
    }
    report("OneSignal (kit, full RT)", &mut samples);
}

fn c_oneshot_kit() {
    let mut samples = Vec::with_capacity(ROUNDS);
    for r in 0..(WARMUP + ROUNDS) {
        let (tx, rx) = OneShot::<u64>::new();
        let barrier = Arc::new(Barrier::new(2));
        let b2 = barrier.clone();
        let handle = thread::spawn(move || {
            rx.bind(); // ← register thread for unpark
            b2.wait();
            let t0 = Instant::now();
            let _ = rx.recv();
            t0.elapsed().as_nanos()
        });
        barrier.wait();
        busy_50us();
        let _ = tx.send(42);
        let dt = handle.join().unwrap();
        if r >= WARMUP {
            samples.push(dt);
        }
    }
    report("OneShot<u64> (kit, full RT)", &mut samples);
}

fn c_oneshot_tokio() {
    let mut samples = Vec::with_capacity(ROUNDS);
    for r in 0..(WARMUP + ROUNDS) {
        let (tx, rx) = tokio::sync::oneshot::channel::<u64>();
        let barrier = Arc::new(Barrier::new(2));
        let b2 = barrier.clone();
        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            b2.wait();
            let t0 = Instant::now();
            let _ = rt.block_on(rx);
            t0.elapsed().as_nanos()
        });
        barrier.wait();
        busy_50us();
        let _ = tx.send(42);
        let dt = handle.join().unwrap();
        if r >= WARMUP {
            samples.push(dt);
        }
    }
    report("tokio::oneshot<u64> (full RT)", &mut samples);
}

fn c_pipe_kit() {
    let mut samples = Vec::with_capacity(ROUNDS);
    for r in 0..(WARMUP + ROUNDS) {
        let pipe = Arc::new(Pipe::<u64>::new());
        let barrier = Arc::new(Barrier::new(2));
        let p2 = pipe.clone();
        let b2 = barrier.clone();
        let handle = thread::spawn(move || {
            p2.set_consumer(thread::current());
            b2.wait();
            let t0 = Instant::now();
            let _ = p2.recv();
            t0.elapsed().as_nanos()
        });
        barrier.wait();
        busy_50us();
        pipe.send(42);
        let dt = handle.join().unwrap();
        if r >= WARMUP {
            samples.push(dt);
        }
    }
    report("Pipe<u64> (kit, slot, full RT)", &mut samples);
}

// ─── D. Cross-thread, full ACK round-trip ─────────────────────────────────
//
// Two single-use primitives per round (request + ack). Producer starts
// the timer, fires the request, blocks on ack, stops the timer when ack
// returns. This is the apples-to-apples RT comparison: every primitive
// pays one wake forward + one wake back, plus its own allocation cost ×2.
//
// Receiver releases ack immediately on wake (no busy work between);
// p50 is dominated by 2× the wake-syscall round-trip.

fn d_one_signal() {
    let mut samples = Vec::with_capacity(ROUNDS);
    for r in 0..(WARMUP + ROUNDS) {
        let (tx_req, rx_req) = OneSignal::<ParkWaiter>::new();
        let (tx_ack, rx_ack) = OneSignal::<ParkWaiter>::new();
        let barrier = Arc::new(Barrier::new(2));
        let b2 = barrier.clone();
        let handle = thread::spawn(move || {
            rx_req.bind();
            b2.wait();
            let _ = rx_req.acquire();
            tx_ack.release();
        });
        rx_ack.bind();
        barrier.wait();
        let t0 = Instant::now();
        tx_req.release();
        let _ = rx_ack.acquire();
        let dt = t0.elapsed().as_nanos();
        handle.join().unwrap();
        if r >= WARMUP {
            samples.push(dt);
        }
    }
    report("OneSignal (kit, RT w/ ack)", &mut samples);
}

fn d_oneshot_kit() {
    let mut samples = Vec::with_capacity(ROUNDS);
    for r in 0..(WARMUP + ROUNDS) {
        let (tx_req, rx_req) = OneShot::<u64>::new();
        let (tx_ack, rx_ack) = OneShot::<u64>::new();
        let barrier = Arc::new(Barrier::new(2));
        let b2 = barrier.clone();
        let handle = thread::spawn(move || {
            rx_req.bind();
            b2.wait();
            let _ = rx_req.recv();
            let _ = tx_ack.send(7);
        });
        rx_ack.bind();
        barrier.wait();
        let t0 = Instant::now();
        let _ = tx_req.send(42);
        let _ = rx_ack.recv();
        let dt = t0.elapsed().as_nanos();
        handle.join().unwrap();
        if r >= WARMUP {
            samples.push(dt);
        }
    }
    report("OneShot<u64> (kit, RT w/ ack)", &mut samples);
}

fn d_oneshot_tokio() {
    let mut samples = Vec::with_capacity(ROUNDS);
    for r in 0..(WARMUP + ROUNDS) {
        let (tx_req, rx_req) = tokio::sync::oneshot::channel::<u64>();
        let (tx_ack, rx_ack) = tokio::sync::oneshot::channel::<u64>();
        let barrier = Arc::new(Barrier::new(2));
        let b2 = barrier.clone();
        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            b2.wait();
            rt.block_on(async move {
                let _ = rx_req.await;
                let _ = tx_ack.send(7);
            });
        });
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        barrier.wait();
        let t0 = Instant::now();
        let _ = tx_req.send(42);
        rt.block_on(async move {
            let _ = rx_ack.await;
        });
        let dt = t0.elapsed().as_nanos();
        handle.join().unwrap();
        if r >= WARMUP {
            samples.push(dt);
        }
    }
    report("tokio::oneshot<u64> (RT w/ ack)", &mut samples);
}

fn d_pipe_kit() {
    let mut samples = Vec::with_capacity(ROUNDS);
    for r in 0..(WARMUP + ROUNDS) {
        let pipe_req = Arc::new(Pipe::<u64>::new());
        let pipe_ack = Arc::new(Pipe::<u64>::new());
        let barrier = Arc::new(Barrier::new(2));
        let pr2 = pipe_req.clone();
        let pa2 = pipe_ack.clone();
        let b2 = barrier.clone();
        let handle = thread::spawn(move || {
            pr2.set_consumer(thread::current());
            b2.wait();
            let _ = pr2.recv();
            pa2.send(7);
        });
        pipe_ack.set_consumer(thread::current());
        barrier.wait();
        let t0 = Instant::now();
        pipe_req.send(42);
        let _ = pipe_ack.recv();
        let dt = t0.elapsed().as_nanos();
        handle.join().unwrap();
        if r >= WARMUP {
            samples.push(dt);
        }
    }
    report("Pipe<u64> (kit, slot, RT w/ ack)", &mut samples);
}

// ─── Driver ───────────────────────────────────────────────────────────────

fn main() {
    println!("=== arbitro-kit oneshot head-to-head ===");
    println!("rounds={ROUNDS} (+ {WARMUP} warmup)");

    header("A. Same-thread, already-released (pure fast path)");
    a_one_signal();
    a_oneshot_kit();
    a_pipe_kit();
    a_oneshot_tokio();

    header("B. Cross-thread, immediate release (likely caught in spin)");
    b_one_signal();
    b_oneshot_tokio();
    b_oneshot_kit();
    b_pipe_kit();

    header("C. Cross-thread, ~50 µs sender delay (receiver definitely parks; full RT incl. delay)");
    c_one_signal();
    c_oneshot_tokio();
    c_oneshot_kit();
    c_pipe_kit();

    header("D. Cross-thread, full ACK round-trip (request + ack-back, no delay)");
    d_one_signal();
    d_oneshot_tokio();
    d_oneshot_kit();
    d_pipe_kit();

    println!("\nDone.");
}
