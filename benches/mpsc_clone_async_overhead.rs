//! Mpsc clone overhead bench — async path (NotifyWaiter / tokio).
//!
//! Mirrors `mpsc_clone_overhead.rs` but on the tokio runtime, comparing:
//!   - `Mpsc::<_, _, NotifyWaiter>::new(M)` — Vec of producers, non-clone
//!   - `Mpsc::<_, _, NotifyWaiter>::new_cloneable(M)` — sender + clones
//!
//! Hot path: `send_async_send` / `recv_async_send` (zero-box specializations
//! for NotifyWaiter — concrete `impl Future + Send` without heap allocation).
//!
//! Sections:
//!   A. 1P/1C — single sender, single consumer task.
//!   B. MP/1C — M producer tasks, 1 consumer task.
//!   C. clone() throughput — cold-path cost on NotifyWaiter waiters.
//!
//! Conforms to bench_safety: BATCH = 1000, BENCH_ROUNDS env-configurable,
//! tee log expected from runner, no background work.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use arbitro_kit::route::{Mpsc, MpscProducer};
use arbitro_kit::waiter::NotifyWaiter;
use tokio::runtime::Builder;
use tokio::sync::Barrier;

const BATCH: usize = 1000;

fn rounds() -> usize {
    std::env::var("BENCH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}
fn warmup_batches() -> usize { 5 }

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
// A. 1P/1C — single sender vs cloneable single sender (idx=0, no clones)
// ─────────────────────────────────────────────────────────────────────────

async fn bench_a_old_async() {
    let (mut ps, c, sd) =
        Mpsc::<u64, 1024, NotifyWaiter>::new(1);
    let p = ps.pop().unwrap();

    let consumer = tokio::spawn(async move {
        loop {
            match c.recv_async_send().await {
                Ok(v) => { std::hint::black_box(v); }
                Err(_) => break,
            }
        }
    });

    // Warmup
    for _ in 0..warmup_batches() {
        for k in 0..BATCH as u64 { p.send_async_send(k).await; }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start("A old 1P/1C async", n);
    let wall = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        for k in 0..BATCH as u64 { p.send_async_send(k).await; }
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end("A old 1P/1C async", prog_t0);
    row("Mpsc::new(1)               1P/1C async", lats, wall.elapsed().as_nanos() as u64);

    sd.signal();
    let _ = consumer.await;
}

async fn bench_a_cloneable_async() {
    let (sender, c, sd) =
        Mpsc::<u64, 1024, NotifyWaiter>::new_cloneable(1);

    let consumer = tokio::spawn(async move {
        loop {
            match c.recv_async_send().await {
                Ok(v) => { std::hint::black_box(v); }
                Err(_) => break,
            }
        }
    });

    for _ in 0..warmup_batches() {
        for k in 0..BATCH as u64 { sender.send_async_send(k).await; }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start("A cloneable 1P/1C async", n);
    let wall = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        for k in 0..BATCH as u64 { sender.send_async_send(k).await; }
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end("A cloneable 1P/1C async", prog_t0);
    row("Mpsc::new_cloneable(1)     1P/1C async", lats, wall.elapsed().as_nanos() as u64);

    sd.signal();
    let _ = consumer.await;
}

// ─────────────────────────────────────────────────────────────────────────
// B. MP/1C fan-in — Vec API vs new_cloneable + clones
// ─────────────────────────────────────────────────────────────────────────
//
// M producer tasks, 1 consumer task. Each producer holds its own ring;
// senders are owned (moved into the spawned task).

async fn run_fanin_async<const M: usize, const RING_CAP: usize>(
    label: &str,
    senders: Vec<MpscProducer<u64, RING_CAP, NotifyWaiter>>,
    consumer_handle: tokio::task::JoinHandle<()>,
    sd: arbitro_kit::route::MpscShutdown<u64, RING_CAP, NotifyWaiter>,
) {
    let per_prod = (BATCH / M) as u64;
    // Two barriers: M+1 participants (M producers + main). `start` releases
    // the producers to do work; `end` rendezvous after they're done. Barriers
    // auto-reset → reusable per round, no lost-notification races.
    let start = Arc::new(Barrier::new(M + 1));
    let end = Arc::new(Barrier::new(M + 1));
    let stop = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();
    for s in senders.into_iter() {
        let start = start.clone();
        let end = end.clone();
        let stop = stop.clone();
        handles.push(tokio::spawn(async move {
            loop {
                start.wait().await;
                if stop.load(Ordering::Acquire) { return; }
                for k in 0..per_prod { s.send_async_send(k).await; }
                end.wait().await;
            }
        }));
    }

    // Warmup
    for _ in 0..warmup_batches() {
        start.wait().await;
        end.wait().await;
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start(label, n);
    let wall = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        start.wait().await;
        end.wait().await;
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end(label, prog_t0);
    row(label, lats, wall.elapsed().as_nanos() as u64);

    stop.store(true, Ordering::Release);
    start.wait().await;   // release producers so they exit on the stop check
    for h in handles { let _ = h.await; }
    sd.signal();
    let _ = consumer_handle.await;
}

async fn bench_b_old_async<const M: usize, const RING_CAP: usize>(label: &str) {
    let (ps, c, sd) =
        Mpsc::<u64, RING_CAP, NotifyWaiter>::new(M);
    let consumer = tokio::spawn(async move {
        loop {
            match c.recv_async_send().await {
                Ok(v) => { std::hint::black_box(v); }
                Err(_) => break,
            }
        }
    });
    run_fanin_async::<M, RING_CAP>(label, ps, consumer, sd).await;
}

async fn bench_b_cloneable_async<const M: usize, const RING_CAP: usize>(label: &str) {
    let (sender, c, sd) =
        Mpsc::<u64, RING_CAP, NotifyWaiter>::new_cloneable(M);
    let consumer = tokio::spawn(async move {
        loop {
            match c.recv_async_send().await {
                Ok(v) => { std::hint::black_box(v); }
                Err(_) => break,
            }
        }
    });
    let mut senders: Vec<MpscProducer<u64, RING_CAP, NotifyWaiter>> =
        (0..M - 1).map(|_| sender.clone()).collect();
    senders.insert(0, sender);
    run_fanin_async::<M, RING_CAP>(label, senders, consumer, sd).await;
}

// ─────────────────────────────────────────────────────────────────────────
// B-tokio. tokio::sync::mpsc baseline (M senders cloned)
// ─────────────────────────────────────────────────────────────────────────

async fn bench_b_tokio_mpsc<const M: usize>(label: &str) {
    // tokio::mpsc::channel(N) is one shared queue with N total slots.
    // For parity with `kit::Mpsc::new(M)` (M × RING_CAP = 1024) we use 1024.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(1024);

    let consumer = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Some(v) => { std::hint::black_box(v); }
                None => break,
            }
        }
    });

    let per_prod = (BATCH / M) as u64;
    let start = Arc::new(Barrier::new(M + 1));
    let end = Arc::new(Barrier::new(M + 1));
    let stop = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();
    for _ in 0..M {
        let tx = tx.clone();
        let start = start.clone();
        let end = end.clone();
        let stop = stop.clone();
        handles.push(tokio::spawn(async move {
            loop {
                start.wait().await;
                if stop.load(Ordering::Acquire) { return; }
                for k in 0..per_prod { let _ = tx.send(k).await; }
                end.wait().await;
            }
        }));
    }
    drop(tx);

    for _ in 0..warmup_batches() {
        start.wait().await;
        end.wait().await;
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start(label, n);
    let wall = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        start.wait().await;
        end.wait().await;
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end(label, prog_t0);
    row(label, lats, wall.elapsed().as_nanos() as u64);

    stop.store(true, Ordering::Release);
    start.wait().await;
    for h in handles { let _ = h.await; }
    let _ = consumer.await;
}

async fn bench_a_tokio_mpsc() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(1024);

    let consumer = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Some(v) => { std::hint::black_box(v); }
                None => break,
            }
        }
    });

    for _ in 0..warmup_batches() {
        for k in 0..BATCH as u64 { let _ = tx.send(k).await; }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start("A tokio::mpsc 1P/1C", n);
    let wall = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        for k in 0..BATCH as u64 { let _ = tx.send(k).await; }
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end("A tokio::mpsc 1P/1C", prog_t0);
    row("tokio::mpsc::channel(1024)  1P/1C async", lats, wall.elapsed().as_nanos() as u64);

    drop(tx);
    let _ = consumer.await;
}

// ─────────────────────────────────────────────────────────────────────────
// D. Consumer-side recv throughput (saturated producers)
// ─────────────────────────────────────────────────────────────────────────
//
// Producers spam `try_send` + `yield_now` so the channel stays full as
// fast as the consumer can drain it. Consumer measures its own wall clock
// for TOTAL recvs → pure recv ns/op.

const D_TOTAL: usize = 200_000;

/// Async batch drain — uses `recv_batch_async_send` (drain_all per await).
async fn bench_d_kit_recv_batch_async<const M: usize, const RING_CAP: usize>(
    label: &str, cloneable: bool,
) {
    let (senders, c, sd): (Vec<MpscProducer<u64, RING_CAP, NotifyWaiter>>, _, _) = if cloneable {
        let (s0, c, sd) = Mpsc::<u64, RING_CAP, NotifyWaiter>::new_cloneable(M);
        let mut v: Vec<_> = (0..M - 1).map(|_| s0.clone()).collect();
        v.insert(0, s0);
        (v, c, sd)
    } else {
        let (ps, c, sd) = Mpsc::<u64, RING_CAP, NotifyWaiter>::new(M);
        (ps, c, sd)
    };

    let stop = Arc::new(AtomicBool::new(false));
    let go = Arc::new(AtomicBool::new(false));
    let per_prod = (D_TOTAL / M) as u64;

    let mut handles = Vec::new();
    for s in senders.into_iter() {
        let stop = stop.clone();
        let go = go.clone();
        handles.push(tokio::spawn(async move {
            while !go.load(Ordering::Acquire) { tokio::task::yield_now().await; }
            let mut sent = 0u64;
            while sent < per_prod {
                if s.try_send(sent).is_ok() {
                    sent += 1;
                } else {
                    if stop.load(Ordering::Acquire) { return; }
                    tokio::task::yield_now().await;
                }
            }
        }));
    }

    go.store(true, Ordering::Release);
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    let mut received: usize = 0;
    let t0 = Instant::now();
    while received < D_TOTAL {
        let n = c.recv_batch_async_send(|_v| { received += 1; }).await.unwrap();
        let _ = n;
    }
    let dt = t0.elapsed();

    stop.store(true, Ordering::Release);
    for h in handles { let _ = h.await; }
    sd.signal();

    let mean = dt.as_nanos() as f64 / D_TOTAL as f64;
    let ops = D_TOTAL as f64 / dt.as_secs_f64();
    println!(
        "{:<48} {:>12.2} {:>12} {:>12} {:>14}",
        label, mean, "—", "—", ops as u64
    );
}

/// tokio::mpsc::Receiver::recv_many baseline — async batch drain.
async fn bench_d_tokio_recv_many<const M: usize>(label: &str) {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(1024);
    let stop = Arc::new(AtomicBool::new(false));
    let go = Arc::new(AtomicBool::new(false));
    let per_prod = (D_TOTAL / M) as u64;

    let mut handles = Vec::new();
    for _ in 0..M {
        let tx = tx.clone();
        let stop = stop.clone();
        let go = go.clone();
        handles.push(tokio::spawn(async move {
            while !go.load(Ordering::Acquire) { tokio::task::yield_now().await; }
            let mut sent = 0u64;
            while sent < per_prod {
                if tx.try_send(sent).is_ok() {
                    sent += 1;
                } else {
                    if stop.load(Ordering::Acquire) { return; }
                    tokio::task::yield_now().await;
                }
            }
        }));
    }
    drop(tx);

    go.store(true, Ordering::Release);
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    let mut buf: Vec<u64> = Vec::with_capacity(1024);
    let mut received: usize = 0;
    let t0 = Instant::now();
    while received < D_TOTAL {
        let n = rx.recv_many(&mut buf, 1024).await;
        if n == 0 { break; }
        for v in buf.drain(..) { std::hint::black_box(v); }
        received += n;
    }
    let dt = t0.elapsed();

    stop.store(true, Ordering::Release);
    for h in handles { let _ = h.await; }

    let mean = dt.as_nanos() as f64 / D_TOTAL as f64;
    let ops = D_TOTAL as f64 / dt.as_secs_f64();
    println!(
        "{:<48} {:>12.2} {:>12} {:>12} {:>14}",
        label, mean, "—", "—", ops as u64
    );
}

async fn bench_d_kit_recv_async<const M: usize, const RING_CAP: usize>(label: &str, cloneable: bool) {
    let (senders, c, sd): (Vec<MpscProducer<u64, RING_CAP, NotifyWaiter>>, _, _) = if cloneable {
        let (s0, c, sd) = Mpsc::<u64, RING_CAP, NotifyWaiter>::new_cloneable(M);
        let mut v: Vec<_> = (0..M - 1).map(|_| s0.clone()).collect();
        v.insert(0, s0);
        (v, c, sd)
    } else {
        let (ps, c, sd) = Mpsc::<u64, RING_CAP, NotifyWaiter>::new(M);
        (ps, c, sd)
    };

    let stop = Arc::new(AtomicBool::new(false));
    let go = Arc::new(AtomicBool::new(false));
    let per_prod = (D_TOTAL / M) as u64;

    let mut handles = Vec::new();
    for s in senders.into_iter() {
        let stop = stop.clone();
        let go = go.clone();
        handles.push(tokio::spawn(async move {
            while !go.load(Ordering::Acquire) { tokio::task::yield_now().await; }
            let mut sent = 0u64;
            while sent < per_prod {
                if s.try_send(sent).is_ok() {
                    sent += 1;
                } else {
                    if stop.load(Ordering::Acquire) { return; }
                    tokio::task::yield_now().await;
                }
            }
        }));
    }

    go.store(true, Ordering::Release);
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    let mut received: usize = 0;
    let t0 = Instant::now();
    while received < D_TOTAL {
        let _ = c.recv_async_send().await.unwrap();
        received += 1;
    }
    let dt = t0.elapsed();

    stop.store(true, Ordering::Release);
    for h in handles { let _ = h.await; }
    sd.signal();

    let mean = dt.as_nanos() as f64 / D_TOTAL as f64;
    let ops = D_TOTAL as f64 / dt.as_secs_f64();
    println!(
        "{:<48} {:>12.2} {:>12} {:>12} {:>14}",
        label, mean, "—", "—", ops as u64
    );
}

async fn bench_d_tokio_recv_async<const M: usize>(label: &str) {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(1024);
    let stop = Arc::new(AtomicBool::new(false));
    let go = Arc::new(AtomicBool::new(false));
    let per_prod = (D_TOTAL / M) as u64;

    let mut handles = Vec::new();
    for _ in 0..M {
        let tx = tx.clone();
        let stop = stop.clone();
        let go = go.clone();
        handles.push(tokio::spawn(async move {
            while !go.load(Ordering::Acquire) { tokio::task::yield_now().await; }
            let mut sent = 0u64;
            while sent < per_prod {
                if tx.try_send(sent).is_ok() {
                    sent += 1;
                } else {
                    if stop.load(Ordering::Acquire) { return; }
                    tokio::task::yield_now().await;
                }
            }
        }));
    }
    drop(tx);

    go.store(true, Ordering::Release);
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    let mut received: usize = 0;
    let t0 = Instant::now();
    while received < D_TOTAL {
        if rx.recv().await.is_some() { received += 1; } else { break; }
    }
    let dt = t0.elapsed();

    stop.store(true, Ordering::Release);
    for h in handles { let _ = h.await; }

    let mean = dt.as_nanos() as f64 / D_TOTAL as f64;
    let ops = D_TOTAL as f64 / dt.as_secs_f64();
    println!(
        "{:<48} {:>12.2} {:>12} {:>12} {:>14}",
        label, mean, "—", "—", ops as u64
    );
}

// ─────────────────────────────────────────────────────────────────────────
// C. clone() throughput — cold path (NotifyWaiter)
// ─────────────────────────────────────────────────────────────────────────

fn bench_c_clone_throughput_async() {
    const N: usize = 200;
    const ROUNDS: usize = 100;

    let mut total_ns: u64 = 0;
    for _ in 0..ROUNDS {
        let (sender, _c, _sd) =
            Mpsc::<u64, 64, NotifyWaiter>::new_cloneable(N);
        let t0 = Instant::now();
        let _clones: Vec<MpscProducer<u64, 64, NotifyWaiter>> =
            (0..N - 1).map(|_| sender.clone()).collect();
        total_ns += t0.elapsed().as_nanos() as u64;
        std::hint::black_box(_clones);
    }
    let total_clones = ROUNDS * (N - 1);
    let mean_ns = (total_ns as f64) / (total_clones as f64);
    let ops = (total_clones as f64) / (total_ns as f64 / 1e9);
    println!(
        "{:<48} {:>12.2} {:>12} {:>12} {:>14}",
        "MpscProducer<NotifyWaiter>::clone()",
        mean_ns,
        "—",
        "—",
        ops as u64
    );
}

// ─────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────

fn main() {
    println!(
        "Mpsc clone overhead — async (tokio + NotifyWaiter), BATCH={BATCH}, rounds={}",
        rounds()
    );

    let rt = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio rt");

    rt.block_on(async {
        header("A. 1P/1C async (consumer task drains in parallel)");
        bench_a_old_async().await;
        bench_a_cloneable_async().await;
        bench_a_tokio_mpsc().await;

        header("B. MP/1C async fan-in (total cap = M × RING_CAP = 1024)");
        bench_b_old_async::<2, 512>("Mpsc::new(2)                 2P/1C async cap=2×512").await;
        bench_b_cloneable_async::<2, 512>("Mpsc::new_cloneable(2)       2P/1C async cap=2×512").await;
        bench_b_tokio_mpsc::<2>("tokio::mpsc::channel(1024)   2P/1C async").await;
        bench_b_old_async::<4, 256>("Mpsc::new(4)                 4P/1C async cap=4×256").await;
        bench_b_cloneable_async::<4, 256>("Mpsc::new_cloneable(4)       4P/1C async cap=4×256").await;
        bench_b_tokio_mpsc::<4>("tokio::mpsc::channel(1024)   4P/1C async").await;
        bench_b_old_async::<8, 128>("Mpsc::new(8)                 8P/1C async cap=8×128").await;
        bench_b_cloneable_async::<8, 128>("Mpsc::new_cloneable(8)       8P/1C async cap=8×128").await;
        bench_b_tokio_mpsc::<8>("tokio::mpsc::channel(1024)   8P/1C async").await;

        header("D1. Consumer recv-single async (recv_async_send / rx.recv) — TOTAL=200k");
        bench_d_kit_recv_async::<1, 1024>("Mpsc::new(1)                 1P/1C recv-1 async", false).await;
        bench_d_kit_recv_async::<1, 1024>("Mpsc::new_cloneable(1)       1P/1C recv-1 async", true).await;
        bench_d_tokio_recv_async::<1>("tokio::mpsc::channel(1024)   1P/1C recv-1 async").await;
        bench_d_kit_recv_async::<2, 512>("Mpsc::new(2)                 2P/1C recv-1 async", false).await;
        bench_d_kit_recv_async::<2, 512>("Mpsc::new_cloneable(2)       2P/1C recv-1 async", true).await;
        bench_d_tokio_recv_async::<2>("tokio::mpsc::channel(1024)   2P/1C recv-1 async").await;
        bench_d_kit_recv_async::<4, 256>("Mpsc::new(4)                 4P/1C recv-1 async", false).await;
        bench_d_kit_recv_async::<4, 256>("Mpsc::new_cloneable(4)       4P/1C recv-1 async", true).await;
        bench_d_tokio_recv_async::<4>("tokio::mpsc::channel(1024)   4P/1C recv-1 async").await;
        bench_d_kit_recv_async::<8, 128>("Mpsc::new(8)                 8P/1C recv-1 async", false).await;
        bench_d_kit_recv_async::<8, 128>("Mpsc::new_cloneable(8)       8P/1C recv-1 async", true).await;
        bench_d_tokio_recv_async::<8>("tokio::mpsc::channel(1024)   8P/1C recv-1 async").await;

        header("D2. Consumer recv-batch async (recv_batch_async_send / recv_many) — TOTAL=200k");
        bench_d_kit_recv_batch_async::<1, 1024>("Mpsc::new(1)                 1P/1C recv-batch async", false).await;
        bench_d_kit_recv_batch_async::<1, 1024>("Mpsc::new_cloneable(1)       1P/1C recv-batch async", true).await;
        bench_d_tokio_recv_many::<1>("tokio::mpsc::recv_many(1024) 1P/1C recv-batch async").await;
        bench_d_kit_recv_batch_async::<2, 512>("Mpsc::new(2)                 2P/1C recv-batch async", false).await;
        bench_d_kit_recv_batch_async::<2, 512>("Mpsc::new_cloneable(2)       2P/1C recv-batch async", true).await;
        bench_d_tokio_recv_many::<2>("tokio::mpsc::recv_many(1024) 2P/1C recv-batch async").await;
        bench_d_kit_recv_batch_async::<4, 256>("Mpsc::new(4)                 4P/1C recv-batch async", false).await;
        bench_d_kit_recv_batch_async::<4, 256>("Mpsc::new_cloneable(4)       4P/1C recv-batch async", true).await;
        bench_d_tokio_recv_many::<4>("tokio::mpsc::recv_many(1024) 4P/1C recv-batch async").await;
        bench_d_kit_recv_batch_async::<8, 128>("Mpsc::new(8)                 8P/1C recv-batch async", false).await;
        bench_d_kit_recv_batch_async::<8, 128>("Mpsc::new_cloneable(8)       8P/1C recv-batch async", true).await;
        bench_d_tokio_recv_many::<8>("tokio::mpsc::recv_many(1024) 8P/1C recv-batch async").await;
    });

    header("C. clone() throughput — cold path (NotifyWaiter)");
    bench_c_clone_throughput_async();

    println!("\nDone.");
}
