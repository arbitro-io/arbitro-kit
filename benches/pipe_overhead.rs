//! Pipe overhead bench — verifies the zero-cost abstraction claim.
//!
//! The thesis: `Pipe<T, NoHook>` costs the same as a hand-rolled
//! `Signal + UnsafeCell<MaybeUninit<T>>` pair. A ZST hook with empty
//! `#[inline]` default methods must be fully elided by the optimizer.
//!
//! Variants benchmarked (single-thread, consumer = same thread, no park):
//!   - raw_signal_slot     — baseline, what `Pipe` should match
//!   - pipe_nohook         — Pipe<T, NoHook>, expected == baseline
//!   - pipe_counting_hook  — real hook: relaxed fetch_add per side
//!   - pipe_boxed_hook     — control: Box<dyn Fn()> style (what we avoided)
//!
//! Payload is `u64` — small enough that hook cost shows, big enough to
//! exercise the slot write/read.
//!
//! Run:
//!   cargo bench --bench pipe_overhead 2>&1 | tee pipe_overhead.log

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arbitro_kit::gate::{Pipe, PipeHook, Signal};

// We batch K operations per timing sample because Instant::now() on Windows
// has ~100 ns granularity — timing a single RTT rounds to 0 or 100. With
// K=1000 ops per sample and ~500 samples, per-op latency has sub-ns precision.
const BATCH: usize = 1000;

fn rounds() -> usize {
    // Total ops = rounds * BATCH. Default 500 => 500_000 ops per variant.
    std::env::var("BENCH_ROUNDS").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(500)
}
fn warmup() -> usize { 10 } // batches

// Print helpers -----------------------------------------------------------
fn header() {
    println!("\n{:<26} {:>12} {:>12} {:>12} {:>14}",
             "variant", "mean_ns/op", "p50_ns/op", "p99_ns/op", "ops/sec");
    println!("{}", "─".repeat(80));
}
fn row(name: &str, mut batch_ns: Vec<u64>, total_elapsed_ns: u64) {
    batch_ns.sort_unstable();
    let samples = batch_ns.len();
    let total_ops = samples * BATCH;
    let ops = (total_ops as f64) / (total_elapsed_ns as f64 / 1e9);
    let mean_per_op = total_elapsed_ns as f64 / total_ops as f64;
    let p50_per_op = batch_ns[samples / 2] as f64 / BATCH as f64;
    let p99_per_op = batch_ns[samples * 99 / 100] as f64 / BATCH as f64;
    println!("{:<26} {:>12.2} {:>12.2} {:>12.2} {:>14}",
             name, mean_per_op, p50_per_op, p99_per_op, ops as u64);
}

// ── 1. raw baseline: Signal + slot assembled inline ──────────────────────
//
// Single-thread "ping-pong" with self: producer writes slot + release,
// consumer (same thread) acquire + read + lock. No cross-thread traffic,
// so we measure pure primitive cost without park/unpark.

fn bench_raw_signal_slot() {
    let sig = Signal::new();
    let slot: UnsafeCell<MaybeUninit<u64>> = UnsafeCell::new(MaybeUninit::uninit());
    sig.set_worker(std::thread::current());

    let do_batch = |i_base: u64| {
        for k in 0..BATCH as u64 {
            unsafe { (*slot.get()).write(i_base + k); }
            sig.release();
            sig.acquire();
            let v = unsafe { (*slot.get()).assume_init_read() };
            sig.lock();
            std::hint::black_box(v);
        }
    };
    for b in 0..warmup() { do_batch((b * BATCH) as u64); }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let t_wall = Instant::now();
    for b in 0..n {
        let t0 = Instant::now();
        do_batch((b * BATCH) as u64);
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row("raw_signal_slot", lats, t_wall.elapsed().as_nanos() as u64);
}

// ── 2. Pipe<T, NoHook> ────────────────────────────────────────────────────

fn bench_pipe_nohook() {
    let p: Pipe<u64> = Pipe::new();
    p.set_consumer(std::thread::current());

    let do_batch = |i_base: u64| {
        for k in 0..BATCH as u64 {
            p.send(i_base + k);
            std::hint::black_box(p.recv());
        }
    };
    for b in 0..warmup() { do_batch((b * BATCH) as u64); }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let t_wall = Instant::now();
    for b in 0..n {
        let t0 = Instant::now();
        do_batch((b * BATCH) as u64);
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row("pipe_nohook", lats, t_wall.elapsed().as_nanos() as u64);
}

// ── 3. Pipe with counting hook (real work) ────────────────────────────────

#[derive(Default)]
struct Counting { s: AtomicU64, r: AtomicU64 }
impl PipeHook<u64> for Counting {
    #[inline] fn on_send(&self, _v: &u64) { self.s.fetch_add(1, Ordering::Relaxed); }
    #[inline] fn on_recv(&self, _v: &u64) { self.r.fetch_add(1, Ordering::Relaxed); }
}

fn bench_pipe_counting() {
    let p: Pipe<u64, Counting> = Pipe::with_hook(Counting::default());
    p.set_consumer(std::thread::current());

    let do_batch = |i_base: u64| {
        for k in 0..BATCH as u64 {
            p.send(i_base + k);
            std::hint::black_box(p.recv());
        }
    };
    for b in 0..warmup() { do_batch((b * BATCH) as u64); }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let t_wall = Instant::now();
    for b in 0..n {
        let t0 = Instant::now();
        do_batch((b * BATCH) as u64);
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row("pipe_counting_hook", lats, t_wall.elapsed().as_nanos() as u64);
    // sanity: hook really ran
    let expected = ((rounds() + warmup()) * BATCH) as u64;
    assert_eq!(p.hook().s.load(Ordering::Relaxed), expected);
}

// ── 4. Control: Box<dyn Fn> style — what we DID NOT do ───────────────────
//
// Mimics what "store a callback inside Signal" would cost: an indirect
// call through a trait object per release/acquire. Uses a separate type
// so the comparison is honest (same slot shape, same Signal).

struct BoxedHookPipe {
    signal: Signal,
    slot:   UnsafeCell<MaybeUninit<u64>>,
    on_send: Box<dyn Fn(&u64) + Send + Sync>,
    on_recv: Box<dyn Fn(&u64) + Send + Sync>,
}
unsafe impl Sync for BoxedHookPipe {}
unsafe impl Send for BoxedHookPipe {}
impl BoxedHookPipe {
    fn new() -> Self {
        Self {
            signal: Signal::new(),
            slot: UnsafeCell::new(MaybeUninit::uninit()),
            // Empty closures — same semantics as NoHook, but via indirection.
            on_send: Box::new(|_| {}),
            on_recv: Box::new(|_| {}),
        }
    }
    #[inline] fn send(&self, v: u64) {
        (self.on_send)(&v);
        unsafe { (*self.slot.get()).write(v); }
        self.signal.release();
    }
    #[inline] fn recv(&self) -> u64 {
        self.signal.acquire();
        let v = unsafe { (*self.slot.get()).assume_init_read() };
        self.signal.lock();
        (self.on_recv)(&v);
        v
    }
}

fn bench_pipe_boxed() {
    let p = BoxedHookPipe::new();
    p.signal.set_worker(std::thread::current());

    let do_batch = |i_base: u64| {
        for k in 0..BATCH as u64 {
            p.send(i_base + k);
            std::hint::black_box(p.recv());
        }
    };
    for b in 0..warmup() { do_batch((b * BATCH) as u64); }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let t_wall = Instant::now();
    for b in 0..n {
        let t0 = Instant::now();
        do_batch((b * BATCH) as u64);
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row("pipe_boxed_dyn_hook", lats, t_wall.elapsed().as_nanos() as u64);
}

fn main() {
    println!("=== Pipe overhead bench (single-thread, no park) ===");
    println!("rounds={}  warmup={}", rounds(), warmup());
    println!("Thesis: pipe_nohook must match raw_signal_slot within noise.");
    header();
    bench_raw_signal_slot();
    bench_pipe_nohook();
    bench_pipe_counting();
    bench_pipe_boxed();
    println!();
}
