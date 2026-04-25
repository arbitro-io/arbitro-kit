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
const WINDOWS: usize = 10;

// Configurable via env: BENCH_ROUNDS=<n>  (default 10_000 batches).
// 500   ≈ snapshot (~10 ms / scenario)
// 10000 ≈ thermal / drift (~150 ms)
// 100000 ≈ long-run drift check (~1.5 s)
fn rounds() -> usize {
    std::env::var("BENCH_ROUNDS").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(10_000)
}

fn header() {
    println!("\n{:<35} {:>10} {:>10} {:>10} {:>10} {:>12}",
             "scenario", "mean", "p50", "p99", "drift%", "ops/sec");
    println!("{}", "─".repeat(93));
}

fn row(name: &str, batch_ns: Vec<u64>, total_elapsed_ns: u64) {
    let samples = batch_ns.len();
    let total_ops = samples * BATCH;
    let ops = (total_ops as f64) / (total_elapsed_ns as f64 / 1e9);
    let mean = total_elapsed_ns as f64 / total_ops as f64;

    // Drift: compare mean ns/op of last window vs first window.
    // +N% means the end of the run is N% slower than the start.
    let w_size = samples / WINDOWS;
    let window_mean_ns = |start: usize| -> f64 {
        let slice = &batch_ns[start..start + w_size];
        slice.iter().sum::<u64>() as f64 / (w_size * BATCH) as f64
    };
    let first = window_mean_ns(0);
    let last  = window_mean_ns(samples - w_size);
    let drift_pct = (last - first) / first * 100.0;

    let mut sorted = batch_ns;
    sorted.sort_unstable();
    let p50 = sorted[samples / 2] as f64 / BATCH as f64;
    let p99 = sorted[samples * 99 / 100] as f64 / BATCH as f64;
    println!("{:<35} {:>10.2} {:>10.2} {:>10.2} {:>+10.2} {:>12}",
             name, mean, p50, p99, drift_pct, ops as u64);
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

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let t_wall = Instant::now();
    for b in 0..n {
        let t0 = Instant::now();
        do_batch((b * BATCH) as u64);
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(name, lats, t_wall.elapsed().as_nanos() as u64);
}

fn main() {
    println!("=== Hub sparse-drain overhead ===");
    println!("BATCH={} rounds={} windows={} (single-thread, try_recv_batch)",
             BATCH, rounds(), WINDOWS);
    println!("Tune with: BENCH_ROUNDS=<n> (e.g. 500 = snapshot, 100000 = drift)");

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
