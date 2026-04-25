//! Hub sparse-drain overhead bench.
//!
//! Isolates the cost of `recv_batch`'s `for k in 0..n` loop when only a
//! few of N ports are active. If the loop cost is significant, switching
//! to a `trailing_zeros` iteration over set bits should win measurably.
//!
//! Scenarios (single-thread, drain = same thread via `try_recv_batch`
//! so we isolate drain-side cost from cross-thread wakeup noise):
//!
//!   - N=1,  active=1   (baseline: no waste possible)
//!   - N=8,  active=1   (drain scans 8 slots, finds 1)
//!   - N=32, active=1   (drain scans 32 slots, finds 1)
//!   - N=32, active=4   (drain scans 32 slots, finds 4)
//!   - N=32, active=32  (dense — upper bound)
//!
//! Each iteration: one active port fires → local try_recv_batch drains.
//! The measured work = `send` + `try_recv_batch`. Subtracting N=1 from
//! N=32 isolates the loop overhead.

use std::time::Instant;

use arbitro_kit::gate::Hub;

const BATCH: usize = 1000;
const WARMUP: usize = 10;
const ROUNDS: usize = 500;

fn header() {
    println!("\n{:<35} {:>12} {:>12} {:>12} {:>14}",
             "scenario", "mean_ns/op", "p50_ns/op", "p99_ns/op", "ops/sec");
    println!("{}", "─".repeat(87));
}

fn row(name: &str, mut batch_ns: Vec<u64>, total_elapsed_ns: u64) {
    batch_ns.sort_unstable();
    let samples = batch_ns.len();
    let total_ops = samples * BATCH;
    let ops = (total_ops as f64) / (total_elapsed_ns as f64 / 1e9);
    let mean = total_elapsed_ns as f64 / total_ops as f64;
    let p50 = batch_ns[samples / 2] as f64 / BATCH as f64;
    let p99 = batch_ns[samples * 99 / 100] as f64 / BATCH as f64;
    println!("{:<35} {:>12.2} {:>12.2} {:>12.2} {:>14}",
             name, mean, p50, p99, ops as u64);
}

/// Run the sparse scenario: Hub with `n` ports, `active` ports firing
/// in round-robin inside the batch. Drain is local (`try_recv_batch`).
fn bench_sparse(name: &str, n: usize, active: usize) {
    assert!(active <= n);
    let (drain, ports) = Hub::<u64, ()>::new(n);

    // We need to keep all ports alive but only use `active` of them.
    let ports: Vec<_> = ports.into_iter().collect();

    let do_batch = |base: u64| {
        for k in 0..BATCH as u64 {
            // Fire on one active port per op (rotate among active set).
            let port_idx = (k as usize) % active;
            ports[port_idx].send(base + k);
            // Drain all pending — this is the iteration we want to measure.
            drain.try_recv_batch(|_, _, _reply| {});
        }
    };

    for b in 0..WARMUP { do_batch((b * BATCH) as u64); }

    let mut lats = Vec::with_capacity(ROUNDS);
    let t_wall = Instant::now();
    for b in 0..ROUNDS {
        let t0 = Instant::now();
        do_batch((b * BATCH) as u64);
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(name, lats, t_wall.elapsed().as_nanos() as u64);
}

fn main() {
    println!("=== Hub sparse-drain overhead ===");
    println!("BATCH={} rounds={} (single-thread, try_recv_batch)", BATCH, ROUNDS);

    header();
    bench_sparse("N=1,  active=1  (baseline)",   1,  1);
    bench_sparse("N=8,  active=1  (sparse)",     8,  1);
    bench_sparse("N=32, active=1  (very sparse)",32, 1);
    bench_sparse("N=32, active=4",               32, 4);
    bench_sparse("N=32, active=32 (dense)",      32, 32);

    println!("\nInterpretation:");
    println!("  If (N=32,active=1) ≫ (N=1,active=1), the linear loop wastes");
    println!("  cycles on empty slots → switching to trailing_zeros wins.");
    println!("\nDone.");
}
