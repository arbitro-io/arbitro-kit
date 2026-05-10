//! `cap_sweep` — measure Mpsc and Mpmc throughput across RING_CAP = 1, 8, 32, 64.
//!
//! Both sync (OS threads) and async (tokio) variants. 4 producers, 1 consumer.
//! BATCH defaults to 10_000; override with `BENCH_BATCH=N`.
//! Rounds default to 200; override with `BENCH_ROUNDS=N`.
//!
//! Conforms to bench_safety: configurable batch/rounds, timeout expected
//! from runner, no background work, tee log expected.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const M: usize = 4; // producers

fn batch() -> usize {
    std::env::var("BENCH_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000)
}

fn rounds() -> usize {
    std::env::var("BENCH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

fn warmup() -> usize { 10 }

fn header(title: &str) {
    println!("\n── {} ──", title);
    println!(
        "{:<52} {:>12} {:>12} {:>12} {:>14}",
        "variant", "mean_ns/op", "p50_ns/op", "p99_ns/op", "ops/sec"
    );
    println!("{}", "─".repeat(106));
}

fn row(name: &str, batch_ns: &mut Vec<u64>) {
    let batch = batch();
    let total_elapsed_ns: u64 = batch_ns.iter().sum();
    batch_ns.sort_unstable();
    let samples = batch_ns.len();
    let total_ops = samples * batch;
    let ops = (total_ops as f64) / (total_elapsed_ns as f64 / 1e9);
    let mean = total_elapsed_ns as f64 / total_ops as f64;
    let p50 = batch_ns[samples / 2] as f64 / batch as f64;
    let p99 = batch_ns[samples * 99 / 100] as f64 / batch as f64;
    println!(
        "{:<52} {:>12.2} {:>12.2} {:>12.2} {:>14.0}",
        name, mean, p50, p99, ops
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Sync: Mpsc 4P/1C with varying RING_CAP
// ═══════════════════════════════════════════════════════════════════════════

fn sync_mpsc<const CAP: usize>() {
    use arbitro_kit::route::Mpsc;

    let batch = batch();
    let per_prod = batch / M;
    let label = format!("mpsc sync  4P/1C  cap={CAP}");

    let (ps, c, sd) = Mpsc::<u64, CAP>::new(M);

    let consumer = thread::spawn(move || {
        c.bind();
        loop {
            match c.recv_batch(|v| { std::hint::black_box(v); }) {
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    let work_round = Arc::new(AtomicU64::new(0));
    let done_round = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for p in ps {
        let wr = work_round.clone();
        let dr = done_round.clone();
        let st = stop.clone();
        handles.push(thread::spawn(move || {
            p.bind();
            let mut last: u64 = 0;
            loop {
                loop {
                    if st.load(Ordering::Acquire) { return; }
                    let r = wr.load(Ordering::Acquire);
                    if r > last { last = r; break; }
                    std::hint::spin_loop();
                }
                for k in 0..per_prod as u64 { p.send(k); }
                dr.fetch_add(1, Ordering::AcqRel);
            }
        }));
    }

    for _ in 0..warmup() {
        done_round.store(0, Ordering::Release);
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let _wall = Instant::now();
    for _ in 0..n {
        done_round.store(0, Ordering::Release);
        let t0 = Instant::now();
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(&label, &mut lats);

    stop.store(true, Ordering::Release);
    work_round.fetch_add(1, Ordering::AcqRel);
    for h in handles { let _ = h.join(); }
    sd.signal();
    let _ = consumer.join();
}

// ═══════════════════════════════════════════════════════════════════════════
// Sync: Mpmc 4P/1C with varying RING_CAP
// ═══════════════════════════════════════════════════════════════════════════

fn sync_mpmc<const CAP: usize>() {
    use arbitro_kit::route::Mpmc;

    let batch = batch();
    let per_prod = batch / M;
    let label = format!("mpmc sync  4P/1C  cap={CAP}");

    let (ps, cs, sd) = Mpmc::<u64, CAP>::new(M, 1);
    let c = cs.into_iter().next().unwrap();

    let consumer = thread::spawn(move || {
        c.bind();
        loop {
            match c.recv_batch(|v| { std::hint::black_box(v); }) {
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    let work_round = Arc::new(AtomicU64::new(0));
    let done_round = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for p in ps {
        let wr = work_round.clone();
        let dr = done_round.clone();
        let st = stop.clone();
        handles.push(thread::spawn(move || {
            p.bind();
            let mut last: u64 = 0;
            loop {
                loop {
                    if st.load(Ordering::Acquire) { return; }
                    let r = wr.load(Ordering::Acquire);
                    if r > last { last = r; break; }
                    std::hint::spin_loop();
                }
                for k in 0..per_prod as u64 { p.send(k); }
                dr.fetch_add(1, Ordering::AcqRel);
            }
        }));
    }

    for _ in 0..warmup() {
        done_round.store(0, Ordering::Release);
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let _wall = Instant::now();
    for _ in 0..n {
        done_round.store(0, Ordering::Release);
        let t0 = Instant::now();
        work_round.fetch_add(1, Ordering::AcqRel);
        while done_round.load(Ordering::Acquire) < M { std::hint::spin_loop(); }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    row(&label, &mut lats);

    stop.store(true, Ordering::Release);
    work_round.fetch_add(1, Ordering::AcqRel);
    for h in handles { let _ = h.join(); }
    sd.signal();
    let _ = consumer.join();
}

// ═══════════════════════════════════════════════════════════════════════════
// Async: Mpsc 4P/1C with varying RING_CAP
// ═══════════════════════════════════════════════════════════════════════════

async fn async_mpsc_spin<const CAP: usize>() {
    use arbitro_kit::route::MpscAsync;

    let batch = batch();
    let per_prod = batch / M;
    let label = format!("mpsc async-spin   4P/1C  cap={CAP}");

    let mut samples = Vec::with_capacity(rounds());
    let total_rounds = warmup() + rounds();

    for round in 0..total_rounds {
        let (producers, mut consumer, shutdown) = MpscAsync::<u64, CAP>::new(M);

        let t0 = Instant::now();

        let prod_handles: Vec<_> = producers.into_iter().map(|p| {
            tokio::spawn(async move {
                for k in 0..per_prod as u64 {
                    let mut v = k;
                    loop {
                        match p.try_send(v) {
                            Ok(()) => break,
                            Err(returned) => {
                                v = returned;
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                }
            })
        }).collect();

        let mut count = 0;
        while count < batch {
            match consumer.recv_async().await {
                Ok(_) => count += 1,
                Err(_) => break,
            }
        }
        for h in prod_handles { h.await.unwrap(); }

        if round >= warmup() {
            samples.push(t0.elapsed().as_nanos() as u64);
        }
        shutdown.signal();
    }
    row(&label, &mut samples);
}

async fn async_mpsc_wake<const CAP: usize>() {
    use arbitro_kit::route::MpscAsync;

    let batch = batch();
    let per_prod = batch / M;
    let label = format!("mpsc async-wake   4P/1C  cap={CAP}");

    let mut samples = Vec::with_capacity(rounds());
    let total_rounds = warmup() + rounds();

    for round in 0..total_rounds {
        let (producers, consumer, shutdown) = MpscAsync::<u64, CAP>::new(M);

        let t0 = Instant::now();

        let prod_handles: Vec<_> = producers.into_iter().map(|p| {
            tokio::spawn(async move {
                for k in 0..per_prod as u64 {
                    p.send_async_send(k).await;
                }
            })
        }).collect();

        let mut count = 0;
        while count < batch {
            match consumer.recv_async_send().await {
                Ok(_) => count += 1,
                Err(_) => break,
            }
        }
        for h in prod_handles { h.await.unwrap(); }

        if round >= warmup() {
            samples.push(t0.elapsed().as_nanos() as u64);
        }
        shutdown.signal();
    }
    row(&label, &mut samples);
}

// ═══════════════════════════════════════════════════════════════════════════
// Async: Mpsc 4P/1C with join! (same-task, no spawn overhead)
// ═══════════════════════════════════════════════════════════════════════════

async fn async_mpsc_join<const CAP: usize>() {
    use arbitro_kit::route::MpscAsync;

    let batch = batch();
    let per_prod = batch / M;
    let label = format!("mpsc async-join   4P/1C  cap={CAP}");

    let mut samples = Vec::with_capacity(rounds());
    let total_rounds = warmup() + rounds();

    for round in 0..total_rounds {
        let (mut producers, consumer, shutdown) = MpscAsync::<u64, CAP>::new(M);
        let p0 = producers.remove(0);
        let p1 = producers.remove(0);
        let p2 = producers.remove(0);
        let p3 = producers.remove(0);

        let recv_count = Arc::new(AtomicUsize::new(0));
        let rc = recv_count.clone();

        let t0 = Instant::now();
        tokio::join!(
            async { for k in 0..per_prod as u64 { p0.send_async(k).await; } },
            async { for k in 0..per_prod as u64 { p1.send_async(k).await; } },
            async { for k in 0..per_prod as u64 { p2.send_async(k).await; } },
            async { for k in 0..per_prod as u64 { p3.send_async(k).await; } },
            async {
                loop {
                    if rc.load(Ordering::Relaxed) >= batch { break; }
                    match consumer.recv_async_send().await {
                        Ok(_) => { rc.fetch_add(1, Ordering::Relaxed); }
                        Err(_) => break,
                    }
                }
            },
            async {
                while recv_count.load(Ordering::Relaxed) < batch {
                    tokio::task::yield_now().await;
                }
                shutdown.signal();
            }
        );

        if round >= warmup() {
            samples.push(t0.elapsed().as_nanos() as u64);
        }
    }
    row(&label, &mut samples);
}

// ═══════════════════════════════════════════════════════════════════════════
// Async: Mpmc 4P/1C with varying RING_CAP
// ═══════════════════════════════════════════════════════════════════════════

async fn async_mpmc<const CAP: usize>() {
    use arbitro_kit::route::MpmcAsync;

    let batch = batch();
    let per_prod = batch / M;
    let label = format!("mpmc async 4P/1C  cap={CAP}");

    let mut samples = Vec::with_capacity(rounds());
    let total_rounds = warmup() + rounds();

    for round in 0..total_rounds {
        // Mpmc producers/consumers are !Sync → async futures are !Send.
        // Use tokio::join! to drive everything on the current task.
        let (producers, mut consumers, shutdown) = MpmcAsync::<u64, CAP>::new(M, 1);
        let mut ps: Vec<_> = producers.into_iter().collect();
        let p0 = ps.remove(0);
        let p1 = ps.remove(0);
        let p2 = ps.remove(0);
        let p3 = ps.remove(0);
        let c0 = consumers.remove(0);
        let recv_count = Arc::new(AtomicUsize::new(0));
        let rc = recv_count.clone();

        let t0 = Instant::now();
        tokio::join!(
            async { for k in 0..per_prod as u64 { p0.send_async(k).await; } },
            async { for k in 0..per_prod as u64 { p1.send_async(k).await; } },
            async { for k in 0..per_prod as u64 { p2.send_async(k).await; } },
            async { for k in 0..per_prod as u64 { p3.send_async(k).await; } },
            async {
                loop {
                    if rc.load(Ordering::Relaxed) >= batch { break; }
                    match c0.recv_async().await {
                        Ok(_) => { rc.fetch_add(1, Ordering::Relaxed); }
                        Err(_) => break,
                    }
                }
            },
            async {
                while recv_count.load(Ordering::Relaxed) < batch {
                    tokio::task::yield_now().await;
                }
                shutdown.signal();
            }
        );

        if round >= warmup() {
            samples.push(t0.elapsed().as_nanos() as u64);
        }
    }
    row(&label, &mut samples);
}

// ═══════════════════════════════════════════════════════════════════════════
// Async: tokio::sync::mpsc baseline (spawn pattern, same as server)
// ═══════════════════════════════════════════════════════════════════════════

async fn async_tokio_mpsc(cap: usize) {
    let batch = batch();
    let per_prod = batch / M;
    let label = format!("tokio::mpsc spawn  4P/1C  cap={cap}");

    let mut samples = Vec::with_capacity(rounds());
    let total_rounds = warmup() + rounds();

    for round in 0..total_rounds {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(cap);

        let t0 = Instant::now();

        let prod_handles: Vec<_> = (0..M).map(|_| {
            let tx = tx.clone();
            tokio::spawn(async move {
                for k in 0..per_prod as u64 {
                    tx.send(k).await.unwrap();
                }
            })
        }).collect();
        drop(tx); // drop original sender

        let mut count = 0;
        while count < batch {
            match rx.recv().await {
                Some(_) => count += 1,
                None => break,
            }
        }
        for h in prod_handles { h.await.unwrap(); }

        if round >= warmup() {
            samples.push(t0.elapsed().as_nanos() as u64);
        }
    }
    row(&label, &mut samples);
}

// ═══════════════════════════════════════════════════════════════════════════
// Driver
// ═══════════════════════════════════════════════════════════════════════════

fn main() {
    println!("=== cap_sweep: RING_CAP impact on Mpsc/Mpmc throughput ===");
    println!("batch={}, rounds={} (+ {} warmup), producers={M}", batch(), rounds(), warmup());

    header("A. Mpsc sync 4P/1C — RING_CAP sweep");
    sync_mpsc::<1>();
    sync_mpsc::<8>();
    sync_mpsc::<32>();
    sync_mpsc::<64>();

    header("B. Mpmc sync 4P/1C — RING_CAP sweep");
    sync_mpmc::<1>();
    sync_mpmc::<8>();
    sync_mpmc::<32>();
    sync_mpmc::<64>();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        header("C. Mpsc async-spin 4P/1C — try_send+yield [spawn]");
        async_mpsc_spin::<1>().await;
        async_mpsc_spin::<8>().await;
        async_mpsc_spin::<32>().await;
        async_mpsc_spin::<64>().await;

        header("D. Mpsc async-wake 4P/1C — send_async_send [spawn]");
        async_mpsc_wake::<1>().await;
        async_mpsc_wake::<8>().await;
        async_mpsc_wake::<32>().await;
        async_mpsc_wake::<64>().await;

        header("E. Mpsc async-join 4P/1C — send_async [join!]");
        async_mpsc_join::<1>().await;
        async_mpsc_join::<8>().await;
        async_mpsc_join::<32>().await;
        async_mpsc_join::<64>().await;

        header("F. Mpmc async 4P/1C — send_async [join!]");
        async_mpmc::<1>().await;
        async_mpmc::<8>().await;
        async_mpmc::<32>().await;
        async_mpmc::<64>().await;

        header("G. tokio::sync::mpsc 4P/1C — baseline [spawn]");
        async_tokio_mpsc(1).await;
        async_tokio_mpsc(8).await;
        async_tokio_mpsc(32).await;
        async_tokio_mpsc(64).await;
    });

    println!("\nDone.");
}
