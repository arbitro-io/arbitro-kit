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
use std::sync::Arc;
use std::time::Instant;

use std::sync::atomic::AtomicBool;

use arbitro_kit::slot::{Pipe, PipeHook};
use arbitro_kit::waiter::{BlockingWaiter, ParkWaiter, Waiter};

// ─── `Signal` shim ─────────────────────────────────────────────────────────
//
// Restores the gate-shaped `release/acquire/lock` API on top of the
// surviving primitive (`ParkWaiter`). Equivalent to the deleted
// `gate::Signal`: a `ParkWaiter` + an `AtomicBool` "open" flag.
//
// This is the exact composition any caller would write today, and what
// `Pipe`'s internals reduce to after the `Waiter` migration.
struct Signal {
    waiter: ParkWaiter,
    open: AtomicBool,
}

impl Signal {
    fn new() -> Self {
        Self {
            waiter: ParkWaiter::default(),
            open: AtomicBool::new(false),
        }
    }
    #[inline]
    fn set_worker(&self, t: std::thread::Thread) {
        self.waiter.set_worker(t);
    }
    #[inline]
    fn release(&self) {
        self.open.store(true, Ordering::Release);
        self.waiter.wake();
    }
    #[inline]
    fn acquire(&self) {
        self.waiter.wait_until(|| self.open.load(Ordering::Acquire));
    }
    #[inline]
    fn lock(&self) {
        self.open.store(false, Ordering::Relaxed);
    }
}

// We batch K operations per timing sample because Instant::now() on Windows
// has ~100 ns granularity — timing a single RTT rounds to 0 or 100. With
// K=1000 ops per sample and ~500 samples, per-op latency has sub-ns precision.
const BATCH: usize = 1000;

fn rounds() -> usize {
    // Total ops = rounds * BATCH. Default 500 => 500_000 ops per variant.
    std::env::var("BENCH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500)
}
fn warmup() -> usize {
    10
} // batches

// Print helpers -----------------------------------------------------------
fn header() {
    println!(
        "\n{:<26} {:>12} {:>12} {:>12} {:>14}",
        "variant", "mean_ns/op", "p50_ns/op", "p99_ns/op", "ops/sec"
    );
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
    println!(
        "{:<26} {:>12.2} {:>12.2} {:>12.2} {:>14}",
        name, mean_per_op, p50_per_op, p99_per_op, ops as u64
    );
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
            unsafe {
                (*slot.get()).write(i_base + k);
            }
            sig.release();
            sig.acquire();
            let v = unsafe { (*slot.get()).assume_init_read() };
            sig.lock();
            std::hint::black_box(v);
        }
    };
    for b in 0..warmup() {
        do_batch((b * BATCH) as u64);
    }

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
    for b in 0..warmup() {
        do_batch((b * BATCH) as u64);
    }

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
struct Counting {
    s: AtomicU64,
    r: AtomicU64,
}
impl PipeHook<u64> for Counting {
    #[inline]
    fn on_send(&self, _v: &u64) {
        self.s.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    fn on_recv(&self, _v: &u64) {
        self.r.fetch_add(1, Ordering::Relaxed);
    }
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
    for b in 0..warmup() {
        do_batch((b * BATCH) as u64);
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let t_wall = Instant::now();
    for b in 0..n {
        let t0 = Instant::now();
        do_batch((b * BATCH) as u64);
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(
        "pipe_counting_hook",
        lats,
        t_wall.elapsed().as_nanos() as u64,
    );
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
    slot: UnsafeCell<MaybeUninit<u64>>,
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
    #[inline]
    fn send(&self, v: u64) {
        (self.on_send)(&v);
        unsafe {
            (*self.slot.get()).write(v);
        }
        self.signal.release();
    }
    #[inline]
    fn recv(&self) -> u64 {
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
    for b in 0..warmup() {
        do_batch((b * BATCH) as u64);
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let t_wall = Instant::now();
    for b in 0..n {
        let t0 = Instant::now();
        do_batch((b * BATCH) as u64);
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(
        "pipe_boxed_dyn_hook",
        lats,
        t_wall.elapsed().as_nanos() as u64,
    );
}

// ── 5. Cross-thread round-trip ───────────────────────────────────────────
//
// Pipe alone has no backpressure primitive. To measure cross-thread cost
// without allowing the producer to silently overwrite the slot, we build
// a closed loop with TWO Pipes: producer→consumer (fwd), consumer→producer
// (ack). Every cycle is: send fwd → recv fwd → send ack → recv ack.
//
// Each cycle = 2 single-slot handshakes across L1↔L1. "ns/cycle" is the
// request→reply latency. Compare vs Ring B3 XT round-trip per-item
// (~212 ns/cycle) — Pipe should be in the same ballpark since both are
// single-slot per direction.

fn bench_pipe_xt_round_trip() {
    const N: u64 = 5_000;
    let fwd: Arc<Pipe<u64>> = Arc::new(Pipe::new());
    let ack: Arc<Pipe<()>> = Arc::new(Pipe::new());

    let fwd_c = fwd.clone();
    let ack_c = ack.clone();
    let consumer = std::thread::spawn(move || {
        fwd_c.set_consumer(std::thread::current());
        let mut sum: u64 = 0;
        for _ in 0..N {
            let v = fwd_c.recv();
            sum = sum.wrapping_add(v);
            ack_c.send(());
        }
        sum
    });

    std::thread::sleep(std::time::Duration::from_millis(20));
    ack.set_consumer(std::thread::current());

    let t0 = Instant::now();
    for i in 0..N {
        fwd.send(i);
        ack.recv();
    }
    let elapsed = t0.elapsed().as_nanos() as u64;
    let sum = consumer.join().unwrap();
    let expected: u64 = (0..N).sum();
    assert_eq!(sum, expected);
    let per_cycle = elapsed as f64 / N as f64;
    println!(
        "{:<26} {:>12.2} {:>12} {:>12} {:>14}",
        "pipe_xt_round_trip",
        per_cycle,
        "-",
        "-",
        ((N as f64) / (elapsed as f64 / 1e9)) as u64
    );
}

// ── 6. Cross-thread burst: the single-slot stall ─────────────────────────
//
// Same round-trip shape, but with tiny payload to isolate handshake cost.
// What we want to see: Pipe cannot pipeline — every cycle waits for the
// other thread. This is the number to beat with Ring's CAP>1 pipelining.

fn bench_pipe_xt_handshake_only() {
    const N: u64 = 5_000;
    let fwd: Arc<Pipe<()>> = Arc::new(Pipe::new());
    let ack: Arc<Pipe<()>> = Arc::new(Pipe::new());

    let fwd_c = fwd.clone();
    let ack_c = ack.clone();
    let consumer = std::thread::spawn(move || {
        fwd_c.set_consumer(std::thread::current());
        for _ in 0..N {
            fwd_c.recv();
            ack_c.send(());
        }
    });
    std::thread::sleep(std::time::Duration::from_millis(20));
    ack.set_consumer(std::thread::current());

    let t0 = Instant::now();
    for _ in 0..N {
        fwd.send(());
        ack.recv();
    }
    let elapsed = t0.elapsed().as_nanos() as u64;
    let _ = consumer.join().unwrap();
    let per_cycle = elapsed as f64 / N as f64;
    println!(
        "{:<26} {:>12.2} {:>12} {:>12} {:>14}",
        "pipe_xt_handshake (unit)",
        per_cycle,
        "-",
        "-",
        ((N as f64) / (elapsed as f64 / 1e9)) as u64
    );
}

// ── 7. Cross-thread with batched payload: Pipe<Vec<u64>> ─────────────────
//
// The "right way" to do batch over Pipe without turning Pipe into Ring:
// the payload IS a batch. One send, one recv, handshake amortized over B
// items. Still round-trip shape (fwd + ack) so the producer respects the
// single-slot contract.
//
// Shows what user-level batching buys you vs Ring's built-in batch API.

fn bench_pipe_xt_batched_payload() {
    const TOTAL: usize = 10_000;
    for &batch in &[16usize, 64, 256] {
        let fwd: Arc<Pipe<Vec<u64>>> = Arc::new(Pipe::new());
        let ack: Arc<Pipe<()>> = Arc::new(Pipe::new());

        let batches = TOTAL / batch;
        let fwd_c = fwd.clone();
        let ack_c = ack.clone();
        let consumer = std::thread::spawn(move || {
            fwd_c.set_consumer(std::thread::current());
            let mut items: u64 = 0;
            for _ in 0..batches {
                let v = fwd_c.recv();
                items += v.len() as u64;
                ack_c.send(());
            }
            items
        });
        std::thread::sleep(std::time::Duration::from_millis(20));
        ack.set_consumer(std::thread::current());

        let t0 = Instant::now();
        for b in 0..batches {
            let v: Vec<u64> = (0..batch as u64).map(|k| (b * batch) as u64 + k).collect();
            fwd.send(v);
            ack.recv();
        }
        let elapsed = t0.elapsed().as_nanos() as u64;
        let items = consumer.join().unwrap();
        assert_eq!(items as usize, batches * batch);
        let per_item = elapsed as f64 / (batches * batch) as f64;
        let name = format!("pipe_xt_vec B={}", batch);
        println!(
            "{:<26} {:>12.2} {:>12} {:>12} {:>14}",
            name,
            per_item,
            "-",
            "-",
            ((batches * batch) as f64 / (elapsed as f64 / 1e9)) as u64
        );
    }
}

fn main() {
    println!("=== Pipe overhead bench ===");
    println!("rounds={}  warmup={}", rounds(), warmup());
    println!();
    println!("── A. Single-thread (no park) ──");
    println!("Thesis: pipe_nohook must match raw_signal_slot within noise.");
    header();
    bench_raw_signal_slot();
    bench_pipe_nohook();
    bench_pipe_counting();
    bench_pipe_boxed();

    println!();
    println!("── B. Cross-thread round-trip (fwd + ack = 2 Pipes) ──");
    println!("Pipe has no backpressure primitive; a second Pipe<()> ack closes");
    println!("the loop so the producer respects the single-slot contract.");
    println!(
        "{:<26} {:>12} {:>12} {:>12} {:>14}",
        "variant", "ns/cycle", "", "", "cycles/sec"
    );
    println!("{}", "─".repeat(80));
    bench_pipe_xt_round_trip();
    bench_pipe_xt_handshake_only();

    println!();
    println!("── C. Batched payload: Pipe<Vec<u64>> (user-level batching) ──");
    println!("One send/recv per batch. Shows 'the right way' to batch over Pipe");
    println!("without turning it into Ring.");
    println!(
        "{:<26} {:>12} {:>12} {:>12} {:>14}",
        "variant", "ns/item", "", "", "items/sec"
    );
    println!("{}", "─".repeat(80));
    bench_pipe_xt_batched_payload();
    println!();
}
