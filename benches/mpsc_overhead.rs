//! Mpsc overhead bench — M producers / 1 consumer.
//!
//! `route::Mpsc<T, RING_CAP>` is the M:1 fan-in primitive: per-producer
//! SPSC ring + consumer-side scan for wakeup. Producers pay zero
//! `LOCK`-prefixed RMW on the send path; the consumer scans M rings per
//! drain pass.
//!
//! Sections:
//!   A. Single-thread 1P/1C — hot path cost (no park).
//!   B. 1P/1C cross-thread — vs crossbeam::channel::bounded.
//!   C. MP/1C fan-in        (M = 2, 4, 8).
//!   D. crossbeam baselines (same C shapes).
//!   E. Producer-batched MP/1C via `try_send_batch` (chunk = 64).
//!   F. High-fanin 100P/1C — mpsc vs crossbeam.
//!   7. Client scenario: alloc-per-msg vs ptr-reuse (1P/1C cross-thread).
//!      Simulates what `publish_async` does today (vec![] per send) vs what
//!      a pre-allocated or pooled buffer path would cost.
//!
//! ## Capacity parity
//!
//! `Mpsc<T, RING_CAP>` allocates RING_CAP slots **per producer**, total =
//! `M × RING_CAP`. `crossbeam::channel::bounded(N)` is one shared queue of
//! `N` slots total. To make ns/op comparable we keep the **total capacity
//! constant at 1024** for every M:
//!
//!   M=1 → RING_CAP=1024 | M=2 → 512 | M=4 → 256 | M=8 → 128
//!
//! crossbeam stays at `bounded(1024)`. Both shapes give producers the same
//! absolute back-pressure budget.
//!
//! Conforms to bench_safety: BATCH = 1000, rounds capped, tee log expected
//! from runner, no background work.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::route::{Mpsc, MpscConsumer, MpscProducer, MpscShutdown};
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
        "{:<42} {:>12} {:>12} {:>12} {:>14}",
        "variant", "mean_ns/op", "p50_ns/op", "p99_ns/op", "ops/sec"
    );
    println!("{}", "─".repeat(94));
}
/// Progress helper: prints full lines (with `\n`) so they pass through `tee`
/// without buffering. Each tick reports the **last batch's ns/op** so you
/// see live throughput during the run — not just dots. Call
/// `progress_start(label, n)` → returns a `tick(i, last_batch_ns)` closure
/// + an `Instant`, then `progress_end(label, t0)` at the end.
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
        "{:<42} {:>12.2} {:>12.2} {:>12.2} {:>14}",
        name, mean, p50, p99, ops as u64
    );
}

// ── A. Single-thread 1P/1C (RING_CAP=1024 → total cap 1024) ──────────
fn bench_single_thread() {
    let (ps, c, sd): (Vec<MpscProducer<u64, 1024>>, MpscConsumer<u64, 1024>, MpscShutdown<u64, 1024>) =
        Mpsc::<u64, 1024>::new(1);
    let p = &ps[0];

    let do_batch = || {
        for k in 0..BATCH as u64 {
            p.try_send(k).unwrap();
            std::hint::black_box(c.try_recv().unwrap());
        }
    };
    for _ in 0..warmup_batches() { do_batch(); }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start("A 1P/1C st", n);
    let wall = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        do_batch();
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end("A 1P/1C st", prog_t0);
    row("mpsc 1P/1C single-thread", lats, wall.elapsed().as_nanos() as u64);

    drop(sd);
}

// ── B. 1P/1C cross-thread (RING_CAP=1024 → total cap 1024) ───────────
fn bench_spsc_cross_thread() {
    let (mut ps, c, sd) = Mpsc::<u64, 1024>::new(1);
    let p = ps.pop().unwrap();

    let consumer = thread::spawn(move || {
        c.bind();
        let mut count: u64 = 0;
        loop {
            match c.recv() {
                Ok(v) => { std::hint::black_box(v); count = count.wrapping_add(1); }
                Err(_) => break,
            }
        }
        count
    });

    p.bind();
    for _ in 0..warmup_batches() {
        for k in 0..BATCH as u64 { p.send(k); }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start("B 1P/1C xt", n);
    let wall = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        for k in 0..BATCH as u64 { p.send(k); }
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end("B 1P/1C xt", prog_t0);
    row("mpsc 1P/1C cross-thread", lats, wall.elapsed().as_nanos() as u64);

    sd.signal();
    let _ = consumer.join().unwrap();
}

// ── C. MP/1C fan-in ───────────────────────────────────────────────────
//
// `RING_CAP` chosen so M × RING_CAP = 1024 (parity with crossbeam(1024)).
fn bench_mpsc_fanin<const M: usize, const RING_CAP: usize>(label: &str) {
    let (ps, c, sd) = Mpsc::<u64, RING_CAP>::new(M);

    let consumer = thread::spawn(move || {
        c.bind();
        let mut total: u64 = 0;
        loop {
            match c.recv_batch(|v| { std::hint::black_box(v); total += 1; }) {
                Ok(_) => {}
                Err(_) => break,
            }
        }
        total
    });

    let per_prod = BATCH / M;
    let work_round = Arc::new(AtomicU64::new(0));
    let done_round = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for p in ps.into_iter() {
        let work_round = work_round.clone();
        let done_round = done_round.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            p.bind();
            let mut last_round: u64 = 0;
            loop {
                loop {
                    if stop.load(Ordering::Acquire) { return; }
                    let r = work_round.load(Ordering::Acquire);
                    if r > last_round { last_round = r; break; }
                    std::hint::spin_loop();
                }
                for k in 0..per_prod as u64 { p.send(k); }
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
    let _ = consumer.join().unwrap();
}

// ── D. crossbeam baselines ────────────────────────────────────────────
fn bench_crossbeam_mpsc<const M: usize>(label: &str) {
    let (tx, rx) = bounded::<u64>(1024);

    let consumer = thread::spawn(move || {
        let mut count: u64 = 0;
        while let Ok(v) = rx.recv() {
            std::hint::black_box(v); count += 1;
        }
        count
    });

    let per_prod = BATCH / M;
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
                for k in 0..per_prod as u64 { let _ = tx.send(k); }
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
    let _ = consumer.join().unwrap();
}

// ── E. Producer-batched MP/1C via `try_send_batch` ────────────────────
fn bench_mpsc_batched<const M: usize, const RING_CAP: usize>(label: &str, chunk: usize) {
    let (ps, c, sd) = Mpsc::<u64, RING_CAP>::new(M);

    let consumer = thread::spawn(move || {
        c.bind();
        let mut total: u64 = 0;
        loop {
            match c.recv_batch(|v| { std::hint::black_box(v); total += 1; }) {
                Ok(_) => {}
                Err(_) => break,
            }
        }
        total
    });

    let per_prod = BATCH / M;
    let work_round = Arc::new(AtomicU64::new(0));
    let done_round = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for p in ps.into_iter() {
        let work_round = work_round.clone();
        let done_round = done_round.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            p.bind();
            let mut buf: Vec<u64> = Vec::with_capacity(chunk);
            let mut last_round: u64 = 0;
            loop {
                loop {
                    if stop.load(Ordering::Acquire) { return; }
                    let r = work_round.load(Ordering::Acquire);
                    if r > last_round { last_round = r; break; }
                    std::hint::spin_loop();
                }
                let mut sent: usize = 0;
                while sent < per_prod {
                    let want = (per_prod - sent).min(chunk);
                    buf.clear();
                    for k in 0..want as u64 { buf.push(sent as u64 + k); }
                    while !buf.is_empty() {
                        let n = p.try_send_batch(&mut buf);
                        if n == 0 {
                            // Ring full → fall back to blocking send for one item.
                            let v = buf.remove(0);
                            p.send(v);
                        }
                    }
                    sent += want;
                }
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
    let _ = consumer.join().unwrap();
}

// ── 7. Client scenario: alloc-per-msg vs ptr-reuse ────────────────────
//
// Simulates the 1P/1C cross-thread path that `publish_async` walks today:
//
//   alloc_per_msg:  vec![0u8; FRAME_SIZE]  +  fill  +  try_send(vec)
//                   consumer drops the Vec  (mimics write_all + dealloc)
//
//   ptr_reuse:      pre-alloc buf, fill in-place, try_send(ptr as usize)
//                   consumer ignores the value  (zero ownership transfer)
//
// The delta between the two rows is the cost the client pays for
// "one heap allocation per publish".  FRAME_SIZE = 92 B matches a
// typical PubFrame (16 B header + 8 B body + 4 B subject + 64 B payload).

const FRAME_SIZE: usize = 92;

fn bench_client_alloc_per_msg() {
    let (mut ps, c, sd) = Mpsc::<Vec<u8>, 1024>::new(1);
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
        for _ in 0..BATCH {
            let mut buf = vec![0u8; FRAME_SIZE];
            // Simulate encode: write first and last bytes so the compiler
            // cannot elide the buffer.
            buf[0] = 0xAB;
            buf[FRAME_SIZE - 1] = 0xCD;
            p.send(buf);
        }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start("7 alloc-per-msg", n);
    let wall = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        for _ in 0..BATCH {
            let mut buf = vec![0u8; FRAME_SIZE];
            buf[0] = 0xAB;
            buf[FRAME_SIZE - 1] = 0xCD;
            p.send(buf);
        }
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end("7 alloc-per-msg", prog_t0);
    row("client: alloc+fill+send (Vec per msg, 92B)", lats, wall.elapsed().as_nanos() as u64);

    sd.signal();
    let _ = consumer.join();
}

fn bench_client_ptr_reuse() {
    let (mut ps, c, sd) = Mpsc::<usize, 1024>::new(1);
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

    let mut buf = vec![0u8; FRAME_SIZE];
    p.bind();
    for _ in 0..warmup_batches() {
        for _ in 0..BATCH {
            buf[0] = 0xAB;
            buf[FRAME_SIZE - 1] = 0xCD;
            p.send(buf.as_ptr() as usize);
        }
    }

    let n = rounds();
    let mut lats = Vec::with_capacity(n);
    let (mut tick, prog_t0) = progress_start("7 ptr-reuse", n);
    let wall = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        for _ in 0..BATCH {
            buf[0] = 0xAB;
            buf[FRAME_SIZE - 1] = 0xCD;
            p.send(buf.as_ptr() as usize);
        }
        let dt = t0.elapsed().as_nanos() as u64;
        lats.push(dt);
        tick(i, dt);
    }
    progress_end("7 ptr-reuse", prog_t0);
    row("ideal:  fill+send (ptr reuse, no alloc, 92B)", lats, wall.elapsed().as_nanos() as u64);

    sd.signal();
    let _ = consumer.join();
}

fn main() {
    println!("=== arbitro-kit route::Mpsc overhead bench ===");
    println!("rounds={} batches × BATCH={} ops each", rounds(), BATCH);

    // Order: crossbeam baselines FIRST so the reference numbers print early.
    // If kit::Mpsc later doesn't beat these, you know immediately the bench
    // setup is off (instead of waiting until the end).
    header("1. crossbeam::channel::bounded(1024) baselines (REFERENCE)");
    bench_crossbeam_mpsc::<2>("crossbeam 2P/1C");
    bench_crossbeam_mpsc::<4>("crossbeam 4P/1C");
    bench_crossbeam_mpsc::<8>("crossbeam 8P/1C");

    header("2. Single-thread 1P/1C (hot path, no park)");
    bench_single_thread();

    header("3. 1P/1C cross-thread");
    bench_spsc_cross_thread();

    header("4. kit::Mpsc MP/1C fan-in (producer-side wall time per round, total cap=1024)");
    bench_mpsc_fanin::<2, 512>("mpsc 2P/1C cap=2×512");
    bench_mpsc_fanin::<4, 256>("mpsc 4P/1C cap=4×256");
    bench_mpsc_fanin::<8, 128>("mpsc 8P/1C cap=8×128");

    header("5. kit::Mpsc MP/1C producer-batched via try_send_batch (chunk=64, total cap=1024)");
    bench_mpsc_batched::<2, 512>("mpsc 2P/1C batched-64 cap=2×512", 64);
    bench_mpsc_batched::<4, 256>("mpsc 4P/1C batched-64 cap=4×256", 64);
    bench_mpsc_batched::<8, 128>("mpsc 8P/1C batched-64 cap=8×128", 64);

    header("6. High-fanin 100P/1C — mpsc vs crossbeam (slow section)");
    bench_mpsc_fanin::<100, 16>("mpsc      100P/1C cap=100×16");
    bench_crossbeam_mpsc::<100>("crossbeam 100P/1C bounded(1024)");

    header("7. Client scenario: alloc-per-msg vs ptr-reuse (1P/1C cross-thread, FRAME=92B)");
    bench_client_alloc_per_msg();
    bench_client_ptr_reuse();

    println!("\nDone.");
}
