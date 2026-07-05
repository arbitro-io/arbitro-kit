//! `Lifeline` overhead bench.
//!
//! Goals:
//! - Confirm `Stream::recv()` is unchanged (no regression from adding
//!   `recv_or_cancel`).
//! - Measure the cost of `Stream::recv_or_cancel` vs `Stream::recv` —
//!   the price of opting in to cancellation.
//! - Measure `Lifeline::cancel_one` / `cancel_all` round-trip from the
//!   moment cancel is called to the moment a parked worker observes
//!   it.

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::gate::Lifeline;
use arbitro_kit::stream::Stream;

const N_MSGS: u64 = 10_000;
const ROUNDS: usize = 30;
const WARMUP: usize = 10;

fn pct(samples: &mut Vec<f64>, p: f64) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples[idx.min(samples.len() - 1)]
}

fn collect<F: FnMut() -> f64>(mut f: F) -> (f64, f64) {
    for _ in 0..WARMUP {
        let _ = f();
    }
    let mut s: Vec<f64> = (0..ROUNDS).map(|_| f()).collect();
    let min = s.iter().cloned().fold(f64::INFINITY, f64::min);
    let p50 = pct(&mut s, 0.50);
    (min, p50)
}

fn row(name: &str, min: f64, p50: f64) {
    println!(
        "{:<48} {:>10.1} {:>10.1} {:>14.0}",
        name,
        min,
        p50,
        1e9 / min
    );
}

// ─── A. Hot-path overhead: recv() vs recv_or_cancel() ───────────────────
//
// Both use the same underlying Stream/Park; the cancellation variant adds
// one atomic load (lifeline.is_cancelled) per spin iteration and one
// extra branch in the predicate. We expect them within noise.

fn xt_recv_baseline() -> f64 {
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let s2 = s.clone();
    let h = thread::spawn(move || {
        s2.set_consumer(thread::current());
        for _ in 0..N_MSGS {
            let _ = s2.recv();
        }
    });
    let t0 = Instant::now();
    for i in 0..N_MSGS as u64 {
        s.send(i);
    }
    h.join().unwrap();
    t0.elapsed().as_nanos() as f64 / N_MSGS as f64
}

fn xt_recv_or_cancel() -> f64 {
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let life = Arc::new(Lifeline::new());
    let s2 = s.clone();
    let l2 = life.clone();
    let h = thread::spawn(move || {
        s2.set_consumer(thread::current());
        let id = l2.register(thread::current());
        for _ in 0..N_MSGS {
            let _ = s2.recv_or_cancel(&l2, id).unwrap();
        }
    });
    let t0 = Instant::now();
    for i in 0..N_MSGS as u64 {
        s.send(i);
    }
    h.join().unwrap();
    t0.elapsed().as_nanos() as f64 / N_MSGS as f64
}

// ─── B. is_cancelled hot path ────────────────────────────────────────────
//
// Just hammers Lifeline::is_cancelled. The whole loop should compile to
// two atomic loads + a branch.

fn st_is_cancelled() -> f64 {
    let life = Lifeline::new();
    let id = life.register(thread::current());
    let n: u64 = 1_000_000;
    let t0 = Instant::now();
    let mut hits = 0u64;
    for _ in 0..n {
        if life.is_cancelled(id) {
            hits += 1;
        }
    }
    let ns = t0.elapsed().as_nanos() as f64;
    std::hint::black_box(hits);
    ns / n as f64
}

// ─── C. Cancel latency: cancel → worker observes ─────────────────────────
//
// One worker parks waiting on an empty stream. We time from the moment
// `cancel_*` is called to the moment the worker thread joins (which it
// does immediately after `recv_or_cancel` returns Err).

fn cancel_one_latency_ns() -> f64 {
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let life = Arc::new(Lifeline::new());
    let s2 = s.clone();
    let l2 = life.clone();

    // Use a barrier-like sync via a one-shot atomic so we know the
    // worker is parked before we time the cancel.
    let parked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let p2 = parked.clone();

    let h = thread::spawn(move || {
        s2.set_consumer(thread::current());
        let id = l2.register(thread::current());
        p2.store(true, std::sync::atomic::Ordering::Release);
        let _ = s2.recv_or_cancel(&l2, id);
    });

    // Spin until worker is parked. ~µs.
    while !parked.load(std::sync::atomic::Ordering::Acquire) {
        std::hint::spin_loop();
    }
    // Slight extra so it's actually inside thread::park, not just
    // about to enter the spin window.
    std::thread::sleep(std::time::Duration::from_micros(200));

    let t0 = Instant::now();
    life.cancel_one(arbitro_kit::gate::WaiterId::new(0));
    h.join().unwrap();
    t0.elapsed().as_nanos() as f64
}

fn cancel_all_latency_n_ns(n_workers: usize) -> f64 {
    let life = Arc::new(Lifeline::new());
    let parked_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(n_workers);

    for _ in 0..n_workers {
        let l = life.clone();
        let pc = parked_count.clone();
        let s: Arc<Stream<u64>> = Arc::new(Stream::new());
        let s2 = s.clone();
        let h = thread::spawn(move || {
            s2.set_consumer(thread::current());
            let id = l.register(thread::current());
            pc.fetch_add(1, std::sync::atomic::Ordering::Release);
            let _ = s2.recv_or_cancel(&l, id);
        });
        handles.push(h);
    }

    while parked_count.load(std::sync::atomic::Ordering::Acquire) < n_workers {
        std::hint::spin_loop();
    }
    std::thread::sleep(std::time::Duration::from_micros(500));

    let t0 = Instant::now();
    life.cancel_all();
    for h in handles {
        h.join().unwrap();
    }
    t0.elapsed().as_nanos() as f64
}

fn main() {
    println!("=== Lifeline overhead bench ===");
    println!(
        "{} msgs / 1M is_cancelled() iters; best-of-{} (after {} warmup).\n",
        N_MSGS, ROUNDS, WARMUP
    );
    println!(
        "{:<48} {:>10} {:>10} {:>14}",
        "scenario", "min ns", "p50 ns", "ops/sec (min)"
    );
    println!("{}", "─".repeat(86));

    println!("\n── A. Hot-path: recv() vs recv_or_cancel() ──");
    let (m, p) = collect(xt_recv_baseline);
    row("Stream::recv() baseline", m, p);
    let (m, p) = collect(xt_recv_or_cancel);
    row("Stream::recv_or_cancel(life, id)", m, p);

    println!("\n── B. Lifeline::is_cancelled hot loop (single-thread) ──");
    let (m, p) = collect(st_is_cancelled);
    row("Lifeline::is_cancelled per call", m, p);

    println!("\n── C. Cancel latency (parked worker → observes) ──");
    // For latency, samples are noisier; smaller round count.
    let mut samples: Vec<f64> = (0..10).map(|_| cancel_one_latency_ns()).collect();
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "cancel_one → 1 worker join: min={:.0} ns, p50={:.0} ns, max={:.0} ns",
        samples[0],
        samples[samples.len() / 2],
        samples[samples.len() - 1]
    );

    for &n in &[4usize, 16, 32] {
        let mut samples: Vec<f64> = (0..10).map(|_| cancel_all_latency_n_ns(n)).collect();
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!(
            "cancel_all → {:>2} workers join: min={:.0} ns, p50={:.0} ns, max={:.0} ns",
            n,
            samples[0],
            samples[samples.len() / 2],
            samples[samples.len() - 1]
        );
    }

    println!("\nDone.");
}
