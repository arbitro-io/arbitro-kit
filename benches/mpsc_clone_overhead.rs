//! Mpsc clone overhead bench — `Mpsc::new(M)` vs `Mpsc::new_cloneable(M)`.
//!
//! Goal: prove that the cloneable API (`new_cloneable` + `MpscProducer::clone()`)
//! has zero hot-path overhead vs the original Vec-of-producers API.
//!
//! The two shapes are bytecode-identical on the send/recv hot path:
//! `next_free_idx.fetch_add` is only touched on `clone()`, never on
//! `try_send` / `try_recv`. We expect ±2% deviation between the two —
//! anything beyond that points to a regression.
//!
//! Sections:
//!   A. 1P/1C cross-thread     — `new(1)` vs `new_cloneable(1)`.
//!   B. MP/1C fan-in (M=2,4,8) — `new(M)` (Vec) vs `new_cloneable(M)` + clones.
//!   C. clone() throughput     — how many clones per second (cold path).
//!
//! Conforms to bench_safety: BATCH = 1000, BENCH_ROUNDS env-configurable,
//! tee log expected from runner, no background work.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::route::{Mpsc, MpscProducer};
use crossbeam_channel::bounded;

const BATCH: usize = 1000;

fn rounds() -> usize {
    std::env::var("BENCH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500)
}
fn warmup_batches() -> usize { 10 }

fn header(title: &str) {
    println!("\n── {} ──", title);
    println!(
        "{:<48} {:>12} {:>12} {:>12} {:>14}",
        "variant", "mean_ns/op", "p50_ns/op", "p99_ns/op", "ops/sec"
    );
    println!("{}", "─".repeat(100));
}

fn progress_start(label: &str, n: usize) -> (impl FnMut(usize, u64), Instant) {
    let label = label.to_string();
    eprintln!("    ▶ [{label}] start n={n}");
    let step = (n / 10).max(1);
    let label_for_tick = label.clone();
    let tick = move |i: usize, last_batch_ns: u64| {
        if i > 0 && i % step == 0 {
            let pct = (i * 100) / n;
            let ns_per_op = (last_batch_ns as f64) / (BATCH as f64);
            eprintln!(
                "      [{label_for_tick}] {pct}% ({i}/{n})  last={ns_per_op:.2} ns/op"
            );
        }
    };
    (tick, Instant::now())
}
fn progress_end(label: &str, t0: Instant) {
    let ms = t0.elapsed().as_millis();
    eprintln!("    ◀ [{label}] done in {ms}ms");
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
        "{:<48} {:>12.2} {:>12.2} {:>12.2} {:>14}",
        name, mean, p50, p99, ops as u64
    );
}

// ─────────────────────────────────────────────────────────────────────────
// A. 1P/1C cross-thread — old API vs cloneable API (single sender)
// ─────────────────────────────────────────────────────────────────────────

fn bench_a_old_1p() {
    let (mut ps, c, sd) = Mpsc::<u64, 1024>::new(1);
    let p = ps.pop().unwrap();

    let consumer = thread::spawn(move || {
        c.bind();
        loop {
            match c.recv() {
                Ok(v) => { std::hint::black_box(v); }
                Err(_) => break,
            }
        }
    });

    p.bind();
    for _ in 0..warmup_batches() {
        for k in 0..BATCH as u64 { p.send(k); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start("A old 1P/1C xt", n);
    let wall = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        for k in 0..BATCH as u64 { p.send(k); }
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end("A old 1P/1C xt", prog_t0);
    row("Mpsc::new(1)               1P/1C cross-thread", lats, wall.elapsed().as_nanos() as u64);

    sd.signal();
    let _ = consumer.join();
}

fn bench_a_cloneable_1p() {
    let (sender, c, sd) = Mpsc::<u64, 1024>::new_cloneable(1);

    let consumer = thread::spawn(move || {
        c.bind();
        loop {
            match c.recv() {
                Ok(v) => { std::hint::black_box(v); }
                Err(_) => break,
            }
        }
    });

    sender.bind();
    for _ in 0..warmup_batches() {
        for k in 0..BATCH as u64 { sender.send(k); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start("A cloneable 1P/1C xt", n);
    let wall = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        for k in 0..BATCH as u64 { sender.send(k); }
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end("A cloneable 1P/1C xt", prog_t0);
    row("Mpsc::new_cloneable(1)     1P/1C cross-thread", lats, wall.elapsed().as_nanos() as u64);

    sd.signal();
    let _ = consumer.join();
}

// ─────────────────────────────────────────────────────────────────────────
// B. MP/1C fan-in — Vec API vs new_cloneable + clones
// ─────────────────────────────────────────────────────────────────────────

fn run_fanin<const M: usize, const RING_CAP: usize>(
    label: &str,
    senders: Vec<MpscProducer<u64, RING_CAP>>,
    consumer_handle: thread::JoinHandle<()>,
    sd: arbitro_kit::route::MpscShutdown<u64, RING_CAP>,
) {
    let per_prod = BATCH / M;
    let work_round = Arc::new(AtomicU64::new(0));
    let done_round = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for s in senders.into_iter() {
        let work_round = work_round.clone();
        let done_round = done_round.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            s.bind();
            let mut last_round: u64 = 0;
            loop {
                loop {
                    if stop.load(Ordering::Acquire) { return; }
                    let r = work_round.load(Ordering::Acquire);
                    if r > last_round { last_round = r; break; }
                    std::hint::spin_loop();
                }
                for k in 0..per_prod as u64 { s.send(k); }
                done_round.fetch_add(1, Ordering::AcqRel);
            }
        }));
    }

    for _ in 0..warmup_batches() {
        done_round.store(0, Ordering::Release);
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start(label, n);
    let wall = Instant::now();
    for i in 0..n {
        done_round.store(0, Ordering::Release);
        let t0 = Instant::now();
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end(label, prog_t0);
    row(label, lats, wall.elapsed().as_nanos() as u64);

    stop.store(true, Ordering::Release);
    work_round.fetch_add(1, Ordering::AcqRel);
    for h in handles { let _ = h.join(); }
    sd.signal();
    let _ = consumer_handle.join();
}

fn bench_b_old<const M: usize, const RING_CAP: usize>(label: &str) {
    let (ps, c, sd) = Mpsc::<u64, RING_CAP>::new(M);
    let consumer = thread::spawn(move || {
        c.bind();
        loop {
            match c.recv_batch(|v| { std::hint::black_box(v); }) {
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });
    run_fanin::<M, RING_CAP>(label, ps, consumer, sd);
}

fn bench_b_cloneable<const M: usize, const RING_CAP: usize>(label: &str) {
    let (sender, c, sd) = Mpsc::<u64, RING_CAP>::new_cloneable(M);
    let consumer = thread::spawn(move || {
        c.bind();
        loop {
            match c.recv_batch(|v| { std::hint::black_box(v); }) {
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });
    // Build the M senders by cloning on the main thread.
    let mut senders: Vec<MpscProducer<u64, RING_CAP>> =
        (0..M - 1).map(|_| sender.clone()).collect();
    senders.insert(0, sender);
    run_fanin::<M, RING_CAP>(label, senders, consumer, sd);
}

// ─────────────────────────────────────────────────────────────────────────
// B-cb. crossbeam baseline (M senders cloned, bounded(1024))
// ─────────────────────────────────────────────────────────────────────────

fn bench_b_crossbeam<const M: usize>(label: &str) {
    let (tx, rx) = bounded::<u64>(1024);

    let consumer = thread::spawn(move || {
        while let Ok(v) = rx.recv() { std::hint::black_box(v); }
    });

    let per_prod = (BATCH / M) as u64;
    let work_round = Arc::new(AtomicU64::new(0));
    let done_round = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for _ in 0..M {
        let tx = tx.clone();
        let work_round = work_round.clone();
        let done_round = done_round.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut last_round: u64 = 0;
            loop {
                loop {
                    if stop.load(Ordering::Acquire) { return; }
                    let r = work_round.load(Ordering::Acquire);
                    if r > last_round { last_round = r; break; }
                    std::hint::spin_loop();
                }
                for k in 0..per_prod { let _ = tx.send(k); }
                done_round.fetch_add(1, Ordering::AcqRel);
            }
        }));
    }
    drop(tx);

    for _ in 0..warmup_batches() {
        done_round.store(0, Ordering::Release);
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start(label, n);
    let wall = Instant::now();
    for i in 0..n {
        done_round.store(0, Ordering::Release);
        let t0 = Instant::now();
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end(label, prog_t0);
    row(label, lats, wall.elapsed().as_nanos() as u64);

    stop.store(true, Ordering::Release);
    work_round.fetch_add(1, Ordering::AcqRel);
    for h in handles { let _ = h.join(); }
    // dropping all senders closes the channel; consumer exits
    let _ = consumer.join();
}

// ─────────────────────────────────────────────────────────────────────────
// E. Consumer-side recv throughput (saturated producers)
// ─────────────────────────────────────────────────────────────────────────
//
// Producers spam `try_send` + spin-on-full so the channel stays full as
// fast as the consumer can drain it. The consumer thread measures its OWN
// wall clock for `TOTAL` recvs — that number / TOTAL = pure consumer cost
// (cache coherence + ring read), NOT producer cost.
//
// If producers are fast enough to saturate the queue, the consumer never
// idles → measurement reflects pure recv ns/op.

const E_TOTAL: usize = 1_000_000;

/// Single-item recv (no batching). Each item pays full atomic cost.
fn bench_e_kit_recv_single<const M: usize, const RING_CAP: usize>(label: &str, cloneable: bool) {
    let (senders, c, sd): (Vec<MpscProducer<u64, RING_CAP>>, _, _) = if cloneable {
        let (s0, c, sd) = Mpsc::<u64, RING_CAP>::new_cloneable(M);
        let mut v: Vec<_> = (0..M - 1).map(|_| s0.clone()).collect();
        v.insert(0, s0);
        (v, c, sd)
    } else {
        let (ps, c, sd) = Mpsc::<u64, RING_CAP>::new(M);
        (ps, c, sd)
    };

    let stop = Arc::new(AtomicBool::new(false));
    let go = Arc::new(AtomicBool::new(false));
    let per_prod = (E_TOTAL / M) as u64;

    let mut handles = Vec::new();
    for p in senders.into_iter() {
        let stop = stop.clone();
        let go = go.clone();
        handles.push(thread::spawn(move || {
            p.bind();
            while !go.load(Ordering::Acquire) { std::hint::spin_loop(); }
            let mut sent = 0u64;
            while sent < per_prod {
                if p.try_send(sent).is_ok() {
                    sent += 1;
                } else {
                    if stop.load(Ordering::Acquire) { return; }
                    std::hint::spin_loop();
                }
            }
        }));
    }

    c.bind();
    go.store(true, Ordering::Release);
    thread::sleep(std::time::Duration::from_millis(2));

    let mut received: usize = 0;
    let t0 = Instant::now();
    while received < E_TOTAL {
        let _ = c.recv().unwrap();   // single-item, no drain_all
        received += 1;
    }
    let dt = t0.elapsed();

    stop.store(true, Ordering::Release);
    for h in handles { let _ = h.join(); }
    sd.signal();

    let mean = dt.as_nanos() as f64 / E_TOTAL as f64;
    let ops = E_TOTAL as f64 / dt.as_secs_f64();
    println!(
        "{:<48} {:>12.2} {:>12} {:>12} {:>14}",
        label, mean, "—", "—", ops as u64
    );
}

fn bench_e_kit_recv<const M: usize, const RING_CAP: usize>(label: &str, cloneable: bool) {
    let (senders, c, sd): (Vec<MpscProducer<u64, RING_CAP>>, _, _) = if cloneable {
        let (s0, c, sd) = Mpsc::<u64, RING_CAP>::new_cloneable(M);
        let mut v: Vec<_> = (0..M - 1).map(|_| s0.clone()).collect();
        v.insert(0, s0);
        (v, c, sd)
    } else {
        let (ps, c, sd) = Mpsc::<u64, RING_CAP>::new(M);
        (ps, c, sd)
    };

    let stop = Arc::new(AtomicBool::new(false));
    let go = Arc::new(AtomicBool::new(false));
    let per_prod = (E_TOTAL / M) as u64;

    let mut handles = Vec::new();
    for p in senders.into_iter() {
        let stop = stop.clone();
        let go = go.clone();
        handles.push(thread::spawn(move || {
            p.bind();
            while !go.load(Ordering::Acquire) { std::hint::spin_loop(); }
            let mut sent = 0u64;
            while sent < per_prod {
                if p.try_send(sent).is_ok() {
                    sent += 1;
                } else {
                    if stop.load(Ordering::Acquire) { return; }
                    std::hint::spin_loop();
                }
            }
        }));
    }

    c.bind();
    // Pre-fill: let producers preload the ring before consumer starts the clock.
    go.store(true, Ordering::Release);
    thread::sleep(std::time::Duration::from_millis(2));

    let mut received: usize = 0;
    let t0 = Instant::now();
    while received < E_TOTAL {
        let _ = c.recv_batch(|_v| { received += 1; }).unwrap();
    }
    let dt = t0.elapsed();

    stop.store(true, Ordering::Release);
    for h in handles { let _ = h.join(); }
    sd.signal();

    let mean = dt.as_nanos() as f64 / E_TOTAL as f64;
    let ops = E_TOTAL as f64 / dt.as_secs_f64();
    println!(
        "{:<48} {:>12.2} {:>12} {:>12} {:>14}",
        label, mean, "—", "—", ops as u64
    );
}

fn bench_e_crossbeam_recv<const M: usize>(label: &str) {
    let (tx, rx) = bounded::<u64>(1024);
    let stop = Arc::new(AtomicBool::new(false));
    let go = Arc::new(AtomicBool::new(false));
    let per_prod = (E_TOTAL / M) as u64;

    let mut handles = Vec::new();
    for _ in 0..M {
        let tx = tx.clone();
        let stop = stop.clone();
        let go = go.clone();
        handles.push(thread::spawn(move || {
            while !go.load(Ordering::Acquire) { std::hint::spin_loop(); }
            let mut sent = 0u64;
            while sent < per_prod {
                if tx.try_send(sent).is_ok() {
                    sent += 1;
                } else {
                    if stop.load(Ordering::Acquire) { return; }
                    std::hint::spin_loop();
                }
            }
        }));
    }
    drop(tx);

    go.store(true, Ordering::Release);
    thread::sleep(std::time::Duration::from_millis(2));

    let mut received: usize = 0;
    let t0 = Instant::now();
    while received < E_TOTAL {
        if rx.recv().is_ok() { received += 1; } else { break; }
    }
    let dt = t0.elapsed();

    stop.store(true, Ordering::Release);
    for h in handles { let _ = h.join(); }

    let mean = dt.as_nanos() as f64 / E_TOTAL as f64;
    let ops = E_TOTAL as f64 / dt.as_secs_f64();
    println!(
        "{:<48} {:>12.2} {:>12} {:>12} {:>14}",
        label, mean, "—", "—", ops as u64
    );
}

// ─────────────────────────────────────────────────────────────────────────
// C. clone() throughput — cold path
// ─────────────────────────────────────────────────────────────────────────
//
// Measures the cost of `MpscProducer::clone()` itself: one `AcqRel`
// `fetch_add` on `next_free_idx` + one `Arc::clone`. A clone is a one-shot
// per-thread setup, so even hundreds of ns/clone would be irrelevant — but
// we want a number.

fn bench_c_clone_throughput() {
    const N: usize = 200;
    const ROUNDS: usize = 100;

    let mut total_ns: u64 = 0;
    for _ in 0..ROUNDS {
        let (sender, _c, _sd) = Mpsc::<u64>::new_cloneable(N);
        let t0 = Instant::now();
        // Clone N-1 times; the original sender already holds idx 0.
        let _clones: Vec<MpscProducer<u64>> =
            (0..N - 1).map(|_| sender.clone()).collect();
        total_ns += t0.elapsed().as_nanos() as u64;
        std::hint::black_box(_clones);
    }
    let total_clones = ROUNDS * (N - 1);
    let mean_ns = (total_ns as f64) / (total_clones as f64);
    let ops = (total_clones as f64) / (total_ns as f64 / 1e9);
    println!(
        "{:<48} {:>12.2} {:>12} {:>12} {:>14}",
        "MpscProducer::clone() (cold path)",
        mean_ns,
        "—",
        "—",
        ops as u64
    );
}

// ─────────────────────────────────────────────────────────────────────────
// D. Ping-pong cross-thread RTT — full round-trip latency
// ─────────────────────────────────────────────────────────────────────────
//
// Two channels: A (producer → echoer) and B (echoer → producer). One
// echo thread receives from A, immediately re-sends on B. Producer thread
// sends a value on A, blocks on B.recv(). Total time measured = full RTT.
//
// Divide RTT/2 for an approximate one-way latency (the two hops are
// symmetric in this topology).
//
// Sends are serialised by the recv() barrier — no queuing effects, the
// ring is always empty when the next send fires. This is the cleanest
// measurement of the actual cross-core wake/cache-bounce cost per send.

fn bench_d_pingpong<const RING_CAP: usize>(
    label: &str,
    build: fn() -> (
        MpscProducer<u64, RING_CAP>,
        arbitro_kit::route::MpscConsumer<u64, RING_CAP>,
        arbitro_kit::route::MpscShutdown<u64, RING_CAP>,
    ),
) {
    // Ping-pong is ~25× slower per op than fire-and-forget (full RTT
    // includes two cross-thread wakes). Use a smaller per-batch count and
    // fewer rounds so the section completes within ~10s.
    const D_BATCH: usize = 100;
    let n = (rounds() / 5).max(50);

    let (p_a, c_a, sd_a) = build();
    let (p_b, c_b, sd_b) = build();

    let echo = thread::spawn(move || {
        c_a.bind();
        p_b.bind();
        loop {
            match c_a.recv() {
                Ok(v) => p_b.send(v),
                Err(_) => break,
            }
        }
    });

    p_a.bind();
    c_b.bind();

    // Warmup
    for _ in 0..1000 {
        p_a.send(0);
        let _ = c_b.recv().unwrap();
    }

    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start(label, n);
    let wall = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        for k in 0..D_BATCH as u64 {
            p_a.send(k);
            let _ = c_b.recv().unwrap();
        }
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end(label, prog_t0);

    // Custom row print — D_BATCH is different from BATCH.
    lats.sort_unstable();
    let total_ops = n * D_BATCH;
    let total_ns = wall.elapsed().as_nanos() as u64;
    let ops = (total_ops as f64) / (total_ns as f64 / 1e9);
    let mean = total_ns as f64 / total_ops as f64;
    let p50 = lats[n / 2] as f64 / D_BATCH as f64;
    let p99 = lats[n * 99 / 100] as f64 / D_BATCH as f64;
    println!(
        "{:<48} {:>12.2} {:>12.2} {:>12.2} {:>14}",
        label, mean, p50, p99, ops as u64
    );

    sd_a.signal();
    let _ = echo.join();
    drop(sd_b);
}

fn build_old_1() -> (
    MpscProducer<u64, 1024>,
    arbitro_kit::route::MpscConsumer<u64, 1024>,
    arbitro_kit::route::MpscShutdown<u64, 1024>,
) {
    let (mut ps, c, sd) = Mpsc::<u64, 1024>::new(1);
    (ps.pop().unwrap(), c, sd)
}

fn build_cloneable_1() -> (
    MpscProducer<u64, 1024>,
    arbitro_kit::route::MpscConsumer<u64, 1024>,
    arbitro_kit::route::MpscShutdown<u64, 1024>,
) {
    Mpsc::<u64, 1024>::new_cloneable(1)
}

// ─────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────

fn main() {
    println!("Mpsc clone overhead bench — old API vs new_cloneable, BATCH={BATCH}, rounds={}", rounds());

    header("A. 1P/1C cross-thread send-side (consumer drains in parallel)");
    bench_a_old_1p();
    bench_a_cloneable_1p();

    header("B. MP/1C fan-in send-side (total cap = M × RING_CAP = 1024)");
    bench_b_old::<2, 512>("Mpsc::new(2)                 2P/1C cap=2×512");
    bench_b_cloneable::<2, 512>("Mpsc::new_cloneable(2)       2P/1C cap=2×512");
    bench_b_crossbeam::<2>("crossbeam::bounded(1024)     2P/1C");
    bench_b_old::<4, 256>("Mpsc::new(4)                 4P/1C cap=4×256");
    bench_b_cloneable::<4, 256>("Mpsc::new_cloneable(4)       4P/1C cap=4×256");
    bench_b_crossbeam::<4>("crossbeam::bounded(1024)     4P/1C");
    bench_b_old::<8, 128>("Mpsc::new(8)                 8P/1C cap=8×128");
    bench_b_cloneable::<8, 128>("Mpsc::new_cloneable(8)       8P/1C cap=8×128");
    bench_b_crossbeam::<8>("crossbeam::bounded(1024)     8P/1C");

    header("C. clone() throughput — cold path");
    bench_c_clone_throughput();

    header("D. Ping-pong cross-thread RTT (full round-trip, two channels)");
    bench_d_pingpong::<1024>("Mpsc::new(1)               ping-pong RTT", build_old_1);
    bench_d_pingpong::<1024>("Mpsc::new_cloneable(1)     ping-pong RTT", build_cloneable_1);

    header("E1. Consumer recv (single-item, recv() per call) — TOTAL=1M");
    bench_e_kit_recv_single::<1, 1024>("Mpsc::new(1)                 1P/1C recv-1", false);
    bench_e_kit_recv_single::<1, 1024>("Mpsc::new_cloneable(1)       1P/1C recv-1", true);
    bench_e_kit_recv_single::<2, 512>("Mpsc::new(2)                 2P/1C recv-1", false);
    bench_e_kit_recv_single::<2, 512>("Mpsc::new_cloneable(2)       2P/1C recv-1", true);
    bench_e_kit_recv_single::<4, 256>("Mpsc::new(4)                 4P/1C recv-1", false);
    bench_e_kit_recv_single::<4, 256>("Mpsc::new_cloneable(4)       4P/1C recv-1", true);
    bench_e_kit_recv_single::<8, 128>("Mpsc::new(8)                 8P/1C recv-1", false);
    bench_e_kit_recv_single::<8, 128>("Mpsc::new_cloneable(8)       8P/1C recv-1", true);

    header("E2. Consumer recv_batch (drain_all per call) — TOTAL=1M");
    bench_e_kit_recv::<1, 1024>("Mpsc::new(1)                 1P/1C recv-batch", false);
    bench_e_kit_recv::<1, 1024>("Mpsc::new_cloneable(1)       1P/1C recv-batch", true);
    bench_e_crossbeam_recv::<1>("crossbeam::bounded(1024)     1P/1C recv");
    bench_e_kit_recv::<2, 512>("Mpsc::new(2)                 2P/1C recv-batch", false);
    bench_e_kit_recv::<2, 512>("Mpsc::new_cloneable(2)       2P/1C recv-batch", true);
    bench_e_crossbeam_recv::<2>("crossbeam::bounded(1024)     2P/1C recv");
    bench_e_kit_recv::<4, 256>("Mpsc::new(4)                 4P/1C recv-batch", false);
    bench_e_kit_recv::<4, 256>("Mpsc::new_cloneable(4)       4P/1C recv-batch", true);
    bench_e_crossbeam_recv::<4>("crossbeam::bounded(1024)     4P/1C recv");
    bench_e_kit_recv::<8, 128>("Mpsc::new(8)                 8P/1C recv-batch", false);
    bench_e_kit_recv::<8, 128>("Mpsc::new_cloneable(8)       8P/1C recv-batch", true);
    bench_e_crossbeam_recv::<8>("crossbeam::bounded(1024)     8P/1C recv");

    println!("\nDone.");
}
