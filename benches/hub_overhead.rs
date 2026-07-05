//! Hub overhead bench — measures the hot-path cost of `HubPort::send` and
//! a full port → drain → reply round-trip.
//!
//! Thesis: inbound `send` costs ~1 atomic (one `fetch_or` on the
//! coordinator bitmap + one slot write), matching a bare `SignalSet::release`
//! within noise. The reply path is one full `Pipe<Out>` round-trip.
//!
//! Scenarios:
//!   - send_only_1port    — single port, fire-and-forget (drain drops reply)
//!   - rtt_1port          — single port, full call/reply
//!   - rtt_4port          — 4 ports in parallel, each does its own RTT
//!
//! The single-port single-thread variant (send_only_no_drain) uses NO
//! drain thread — we just measure raw send cost without cross-thread
//! wake. This is the purest comparison against `SignalSet::release`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::gate::SignalSet;
use arbitro_kit::route::{Hub, Shutdown};

const BATCH: usize = 1000;

fn rounds() -> usize {
    std::env::var("BENCH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500)
}
fn warmup_batches() -> usize {
    10
}

fn header(title: &str) {
    println!("\n── {} ──", title);
    println!(
        "{:<30} {:>12} {:>12} {:>12} {:>14}",
        "variant", "mean_ns/op", "p50_ns/op", "p99_ns/op", "ops/sec"
    );
    println!("{}", "─".repeat(82));
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
        "{:<30} {:>12.2} {:>12.2} {:>12.2} {:>14}",
        name, mean, p50, p99, ops as u64
    );
}

// ── Baseline: raw SignalSet::release on a single bit, single-thread ──────
// Matches what HubPort::send does minus the slot write.
fn bench_baseline_signalset_release() {
    let mut set = SignalSet::<arbitro_kit::waiter::ParkWaiter>::new();
    let id = set.create("x");
    set.set_worker(std::thread::current());

    let do_batch = || {
        for _ in 0..BATCH {
            set.release(id);
            set.lock(id); // keep cost symmetric with hub's bit cycling
        }
    };
    for _ in 0..warmup_batches() {
        do_batch();
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let t_wall = Instant::now();
    for _ in 0..n {
        let t0 = Instant::now();
        do_batch();
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(
        "signalset_release+lock (raw)",
        lats,
        t_wall.elapsed().as_nanos() as u64,
    );
}

// ── Hub send only, no drain thread running ──────────────────────────────
// Producer fires into its slot, then immediately clears the bit itself to
// reset for the next iteration. This isolates send cost from drain cost.
fn bench_hub_send_no_drain() {
    let (drain, mut ports) = Hub::<u64, ()>::new(1);
    let p = ports.remove(0);
    // Don't spawn a drain thread. We use try_recv_batch ourselves to
    // simulate the drain-side bit clear, measuring only producer cost.

    let do_batch = |i_base: u64| {
        for k in 0..BATCH as u64 {
            p.send(i_base + k);
            // Manually clear the bit + consume the slot so the next send
            // sees is_idle.
            drain.try_recv_batch(|_, _, _reply| { /* drop reply */ });
        }
    };
    for b in 0..warmup_batches() {
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
        "hub_send + local drain",
        lats,
        t_wall.elapsed().as_nanos() as u64,
    );
}

// ── Hub full RTT, one port, cross-thread ────────────────────────────────
fn bench_hub_rtt_1port() {
    let (drain, mut ports) = Hub::<u64, u64>::new(1);
    let shutdown = drain.shutdown_handle();
    let p = ports.remove(0);

    let h = thread::spawn(move || {
        drain.bind();
        loop {
            match drain.recv_batch(|_, msg, reply| {
                reply.send(msg.wrapping_add(1));
            }) {
                Ok(()) => continue,
                Err(Shutdown) => break,
            }
        }
    });

    p.bind();
    for i in 0..warmup_batches() {
        for k in 0..BATCH as u64 {
            let _ = p.call(k + i as u64);
        }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let t_wall = Instant::now();
    for b in 0..n {
        let t0 = Instant::now();
        for k in 0..BATCH as u64 {
            std::hint::black_box(p.call(k + b as u64));
        }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(
        "hub_rtt_1port (xthread)",
        lats,
        t_wall.elapsed().as_nanos() as u64,
    );

    shutdown.signal();
    h.join().unwrap();
}

// ── Hub full RTT, 4 producers, aggregated throughput ────────────────────
// Measures ops/sec under contention (4 ports all hammering the drain).
fn bench_hub_rtt_4ports() {
    let (drain, ports) = Hub::<u64, u64>::new(4);
    let shutdown = drain.shutdown_handle();

    let h = thread::spawn(move || {
        drain.bind();
        loop {
            match drain.recv_batch(|_, msg, reply| {
                reply.send(msg.wrapping_add(1));
            }) {
                Ok(()) => continue,
                Err(Shutdown) => break,
            }
        }
    });

    let n = rounds(); // per-thread batches
    let per_thread_ops = n * BATCH;
    let stop = Arc::new(AtomicBool::new(false));

    let t_wall = Instant::now();
    let handles: Vec<_> = ports
        .into_iter()
        .map(|p| {
            let stop = stop.clone();
            thread::spawn(move || {
                p.bind();
                // warmup
                for i in 0..warmup_batches() {
                    for k in 0..BATCH as u64 {
                        let _ = p.call(k + i as u64);
                    }
                }
                let t0 = Instant::now();
                for b in 0..n {
                    for k in 0..BATCH as u64 {
                        std::hint::black_box(p.call(k + b as u64));
                    }
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                }
                t0.elapsed().as_nanos() as u64
            })
        })
        .collect();

    let per_thread_ns: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let wall_ns = t_wall.elapsed().as_nanos() as u64;

    shutdown.signal();
    h.join().unwrap();

    let total_ops = per_thread_ops * 4;
    let ops_aggregate = (total_ops as f64) / (wall_ns as f64 / 1e9);
    let ns_per_op_each = per_thread_ns.iter().sum::<u64>() as f64 / (per_thread_ops as f64 * 4.0);

    println!("\n── 4-port aggregate ──");
    println!("per-thread mean ns/op : {:.2}", ns_per_op_each);
    println!("aggregate ops/sec     : {}", ops_aggregate as u64);
    println!("total ops             : {}", total_ops);
}

fn main() {
    println!("=== arbitro-kit Hub overhead bench ===");
    println!("rounds={} batches × BATCH={} ops each", rounds(), BATCH);

    header("Hot-path send cost (single-thread, no cross-thread wake)");
    bench_baseline_signalset_release();
    bench_hub_send_no_drain();

    header("Full RTT (port → drain → reply)");
    bench_hub_rtt_1port();

    bench_hub_rtt_4ports();

    println!("\nDone.");
}
