//! A/B benchmark: `Mpsc` (hand-rolled PRing) vs `Mpsc2` (built on `Ring`).
//!
//! Three send modes exercised, all moving only pointer-sized payloads to
//! isolate the coordination primitive from memcpy cost:
//!
//! - **Single**: `T = usize`. 1 send call = 1 msg (`p.send(v)`). N sends,
//!   N slots, N cursor stores, N wakes (or 1 gated in the receiver).
//!
//! - **Bulk**: `T = usize`. 1 call ships `BATCH` items into `BATCH` slots
//!   via `try_send_bulk` — 1 amortized cursor store + 1 fan-in wake per
//!   BATCH items. The queue still sees `N` slot entries.
//!
//! - **Batch**: `T = Box<[usize; BATCH]>`. 1 slot **is** a whole batch —
//!   16 B moved per queue op (Box = ptr + niche-optimized), but the
//!   payload behind the pointer is `BATCH × 8 B`. One `alloc` per batch
//!   on the sender's side (honest cost; documented).
//!
//! Total: `PER * m` usize items transferred end-to-end per iteration in
//! every mode.
//!
//! Run (WSL, per project rules):
//! ```
//! cargo bench --bench mpsc_vs_mpsc2 --no-run
//! cp -a target/release/deps/mpsc_vs_mpsc2-<hash> /tmp/arbitro/
//! cd /tmp/arbitro && timeout 300 ./mpsc_vs_mpsc2-<hash> --bench \
//!   2>&1 | tee /tmp/bench.log
//! ```

use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::Instant;

use arbitro_kit::route::{Mpmc, Mpsc, Mpsc2, Shutdown};

const PER: u64 = 1_000_000;
const CAP: usize = 64;
const BATCH: usize = 32;
const WARMUP: usize = 1;
const ROUNDS: usize = 5;

fn header(section: &str) {
    println!("\n── {} ─────────────────────────────────────────────────", section);
    println!(
        "{:<24} {:>6} {:>14} {:>16} {:>14}",
        "impl", "M", "total msgs", "wall ms (p50)", "M msgs/s"
    );
    println!("{}", "─".repeat(80));
}

fn row(name: &str, m: usize, mut samples_ms: Vec<f64>) {
    samples_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = samples_ms[samples_ms.len() / 2];
    let total = (m as u64) * PER;
    let msgs_per_sec = (total as f64) / (p50 / 1000.0);
    println!(
        "{:<24} {:>6} {:>14} {:>16.2} {:>14.2}",
        name,
        m,
        total,
        p50,
        msgs_per_sec / 1_000_000.0
    );
}

fn measure<F: Fn() -> f64>(name: &str, m: usize, f: F) {
    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..WARMUP {
        f();
    }
    for _ in 0..ROUNDS {
        samples.push(f());
    }
    row(name, m, samples);
}

// ══════════════════════════════════════════════════════════════════════════
// Section: SINGLE — one `usize` per send/recv call
// ══════════════════════════════════════════════════════════════════════════

fn mpsc_single(m: usize) -> f64 {
    let (ps, c, sd) = Mpsc::<usize, CAP>::new(m);
    let sd2 = sd.clone();
    let barrier = Arc::new(Barrier::new(m + 1));

    let consumer_h = {
        let b = barrier.clone();
        thread::spawn(move || {
            c.bind();
            b.wait();
            let target = (m as u64) * PER;
            let mut got: u64 = 0;
            while got < target {
                match c.recv_batch(|_| got += 1) {
                    Ok(_) => {}
                    Err(Shutdown) => break,
                }
            }
        })
    };
    let handles: Vec<_> = ps
        .into_iter()
        .map(|p| {
            let b = barrier.clone();
            thread::spawn(move || {
                p.bind();
                b.wait();
                for k in 0..PER {
                    p.send(k as usize);
                }
            })
        })
        .collect();
    let t0 = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    consumer_h.join().unwrap();
    let el = t0.elapsed().as_secs_f64() * 1000.0;
    sd2.signal();
    drop(sd);
    el
}

fn mpsc2_single(m: usize) -> f64 {
    let (ps, mut c, sd) = Mpsc2::<usize, CAP>::new(m);
    let sd2 = sd.clone();
    let barrier = Arc::new(Barrier::new(m + 1));

    let consumer_h = {
        let b = barrier.clone();
        thread::spawn(move || {
            c.bind();
            b.wait();
            let target = (m as u64) * PER;
            let mut got: u64 = 0;
            while got < target {
                match c.recv_batch(|_| got += 1) {
                    Ok(_) => {}
                    Err(Shutdown) => break,
                }
            }
        })
    };
    let handles: Vec<_> = ps
        .into_iter()
        .map(|mut p| {
            let b = barrier.clone();
            thread::spawn(move || {
                p.bind();
                b.wait();
                for k in 0..PER {
                    p.send(k as usize);
                }
            })
        })
        .collect();
    let t0 = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    consumer_h.join().unwrap();
    let el = t0.elapsed().as_secs_f64() * 1000.0;
    sd2.signal();
    drop(sd);
    el
}

fn mpmc_single(m: usize) -> f64 {
    let (ps, mut cs, sd) = Mpmc::<usize, CAP>::new(m, 1);
    let sd2 = sd.clone();
    let barrier = Arc::new(Barrier::new(m + 1));
    let c = cs.remove(0);

    let consumer_h = {
        let b = barrier.clone();
        thread::spawn(move || {
            c.bind();
            b.wait();
            let target = (m as u64) * PER;
            let mut got: u64 = 0;
            while got < target {
                match c.recv_batch(|_| got += 1) {
                    Ok(_) => {}
                    Err(Shutdown) => break,
                }
            }
        })
    };
    let handles: Vec<_> = ps
        .into_iter()
        .map(|p| {
            let b = barrier.clone();
            thread::spawn(move || {
                p.bind();
                b.wait();
                for k in 0..PER {
                    p.send(k as usize);
                }
            })
        })
        .collect();
    let t0 = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    consumer_h.join().unwrap();
    let el = t0.elapsed().as_secs_f64() * 1000.0;
    sd2.signal();
    drop(sd);
    el
}

// ══════════════════════════════════════════════════════════════════════════
// Section: BULK — N usize items pushed in one call, N slots consumed
// ══════════════════════════════════════════════════════════════════════════

fn mpsc_bulk(m: usize) -> f64 {
    let (ps, c, sd) = Mpsc::<usize, CAP>::new(m);
    let sd2 = sd.clone();
    let barrier = Arc::new(Barrier::new(m + 1));

    let consumer_h = {
        let b = barrier.clone();
        thread::spawn(move || {
            c.bind();
            b.wait();
            let target = (m as u64) * PER;
            let mut got: u64 = 0;
            while got < target {
                match c.recv_batch(|_| got += 1) {
                    Ok(_) => {}
                    Err(Shutdown) => break,
                }
            }
        })
    };
    let handles: Vec<_> = ps
        .into_iter()
        .map(|p| {
            let b = barrier.clone();
            thread::spawn(move || {
                p.bind();
                b.wait();
                let mut buf: Vec<usize> = Vec::with_capacity(BATCH);
                let mut sent: u64 = 0;
                while sent < PER {
                    let want = (PER - sent).min(BATCH as u64) as usize;
                    for k in 0..want {
                        buf.push(sent as usize + k);
                    }
                    // try_send_batch on Mpsc is bulk-amortized already.
                    while !buf.is_empty() {
                        let n = p.try_send_batch(&mut buf);
                        if n == 0 {
                            std::hint::spin_loop();
                        }
                    }
                    sent += want as u64;
                }
            })
        })
        .collect();
    let t0 = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    consumer_h.join().unwrap();
    let el = t0.elapsed().as_secs_f64() * 1000.0;
    sd2.signal();
    drop(sd);
    el
}

fn mpsc2_bulk(m: usize) -> f64 {
    let (ps, mut c, sd) = Mpsc2::<usize, CAP>::new(m);
    let sd2 = sd.clone();
    let barrier = Arc::new(Barrier::new(m + 1));

    let consumer_h = {
        let b = barrier.clone();
        thread::spawn(move || {
            c.bind();
            b.wait();
            let target = (m as u64) * PER;
            let mut got: u64 = 0;
            while got < target {
                match c.recv_batch(|_| got += 1) {
                    Ok(_) => {}
                    Err(Shutdown) => break,
                }
            }
        })
    };
    let handles: Vec<_> = ps
        .into_iter()
        .map(|mut p| {
            let b = barrier.clone();
            thread::spawn(move || {
                p.bind();
                b.wait();
                let mut buf: Vec<usize> = Vec::with_capacity(BATCH);
                let mut sent: u64 = 0;
                while sent < PER {
                    let want = (PER - sent).min(BATCH as u64) as usize;
                    for k in 0..want {
                        buf.push(sent as usize + k);
                    }
                    while !buf.is_empty() {
                        let n = p.try_send_bulk(&mut buf);
                        if n == 0 {
                            std::hint::spin_loop();
                        }
                    }
                    sent += want as u64;
                }
            })
        })
        .collect();
    let t0 = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    consumer_h.join().unwrap();
    let el = t0.elapsed().as_secs_f64() * 1000.0;
    sd2.signal();
    drop(sd);
    el
}

// ══════════════════════════════════════════════════════════════════════════
// Section: BATCH — one Box<[usize; BATCH]> per queue slot
// ══════════════════════════════════════════════════════════════════════════

type BatchMsg = Box<[usize; BATCH]>;

fn mpsc_batch(m: usize) -> f64 {
    let (ps, c, sd) = Mpsc::<BatchMsg, CAP>::new(m);
    let sd2 = sd.clone();
    let barrier = Arc::new(Barrier::new(m + 1));

    let consumer_h = {
        let b = barrier.clone();
        thread::spawn(move || {
            c.bind();
            b.wait();
            let target = (m as u64) * PER;
            let mut got: u64 = 0;
            while got < target {
                match c.recv_batch(|batch| {
                    // Count every item in the batch — that's the honest
                    // per-item consumer cost (deref + iterate).
                    got += batch.len() as u64;
                }) {
                    Ok(_) => {}
                    Err(Shutdown) => break,
                }
            }
        })
    };
    let handles: Vec<_> = ps
        .into_iter()
        .map(|p| {
            let b = barrier.clone();
            thread::spawn(move || {
                p.bind();
                b.wait();
                let batches = PER / BATCH as u64;
                for i in 0..batches {
                    let base = i * BATCH as u64;
                    let arr = std::array::from_fn(|k| base as usize + k);
                    let boxed: BatchMsg = Box::new(arr);
                    p.send(boxed);
                }
            })
        })
        .collect();
    let t0 = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    consumer_h.join().unwrap();
    let el = t0.elapsed().as_secs_f64() * 1000.0;
    sd2.signal();
    drop(sd);
    el
}

fn mpsc2_batch(m: usize) -> f64 {
    let (ps, mut c, sd) = Mpsc2::<BatchMsg, CAP>::new(m);
    let sd2 = sd.clone();
    let barrier = Arc::new(Barrier::new(m + 1));

    let consumer_h = {
        let b = barrier.clone();
        thread::spawn(move || {
            c.bind();
            b.wait();
            let target = (m as u64) * PER;
            let mut got: u64 = 0;
            while got < target {
                match c.recv_batch(|batch| got += batch.len() as u64) {
                    Ok(_) => {}
                    Err(Shutdown) => break,
                }
            }
        })
    };
    let handles: Vec<_> = ps
        .into_iter()
        .map(|mut p| {
            let b = barrier.clone();
            thread::spawn(move || {
                p.bind();
                b.wait();
                let batches = PER / BATCH as u64;
                for i in 0..batches {
                    let base = i * BATCH as u64;
                    let arr = std::array::from_fn(|k| base as usize + k);
                    let boxed: BatchMsg = Box::new(arr);
                    p.send(boxed);
                }
            })
        })
        .collect();
    let t0 = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    consumer_h.join().unwrap();
    let el = t0.elapsed().as_secs_f64() * 1000.0;
    sd2.signal();
    drop(sd);
    el
}

// ══════════════════════════════════════════════════════════════════════════
// main
// ══════════════════════════════════════════════════════════════════════════

fn main() {
    println!(
        "\nMpsc vs Mpsc2 — pointer payloads only  (PER={} per producer, CAP={}, BATCH={}, warmup={}, rounds={})",
        PER, CAP, BATCH, WARMUP, ROUNDS
    );

    let ms = [1usize, 4, 16];

    // Section 1: single
    header("SINGLE  (T = usize, one send per msg)");
    for &m in &ms {
        measure("kit Mpsc (single)", m, || mpsc_single(m));
        measure("kit Mpsc2 (single)", m, || mpsc2_single(m));
        measure("kit Mpmc N=1 (single)", m, || mpmc_single(m));
        println!();
    }

    // Section 2: bulk
    header("BULK    (T = usize, BATCH items per bulk-send call)");
    for &m in &ms {
        measure("kit Mpsc (bulk)", m, || mpsc_bulk(m));
        measure("kit Mpsc2 (bulk)", m, || mpsc2_bulk(m));
        println!();
    }

    // Section 3: batch
    header("BATCH   (T = Box<[usize; BATCH]>, one boxed batch per slot)");
    for &m in &ms {
        measure("kit Mpsc (batch)", m, || mpsc_batch(m));
        measure("kit Mpsc2 (batch)", m, || mpsc2_batch(m));
        println!();
    }

    // Section 4: tokio SINGLE
    header("TOKIO SINGLE  (T = usize, NotifyWaiter, one send per msg)");
    for &m in &ms {
        measure("kit MpscAsync (single)", m, || mpsc_single_tokio(m));
        measure("kit Mpsc2Async (single)", m, || mpsc2_single_tokio(m));
        println!();
    }
}

// ══════════════════════════════════════════════════════════════════════════
// Tokio Section: SINGLE async
// ══════════════════════════════════════════════════════════════════════════

fn mpsc_single_tokio(m: usize) -> f64 {
    use arbitro_kit::route::MpscAsync;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(m + 1)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (ps, mut c, sd) = MpscAsync::<usize, CAP>::new(m);
        let sd2 = sd.clone();
        let target = (m as u64) * PER;
        let consumer = tokio::spawn(async move {
            let mut got: u64 = 0;
            while got < target {
                match c.recv_async().await {
                    Ok(_) => got += 1,
                    Err(_) => break,
                }
            }
        });
        let t0 = Instant::now();
        let mut ph: Vec<tokio::task::JoinHandle<()>> = ps
            .into_iter()
            .map(|p| {
                tokio::spawn(async move {
                    for k in 0..PER {
                        p.send_async(k as usize).await;
                    }
                })
            })
            .collect();
        for h in ph.drain(..) {
            h.await.unwrap();
        }
        consumer.await.unwrap();
        let el = t0.elapsed().as_secs_f64() * 1000.0;
        sd2.signal();
        drop(sd);
        el
    })
}

fn mpsc2_single_tokio(m: usize) -> f64 {
    use arbitro_kit::route::Mpsc2Async;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(m + 1)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (ps, mut c, sd) = Mpsc2Async::<usize, CAP>::new(m);
        let sd2 = sd.clone();
        let target = (m as u64) * PER;
        let consumer = tokio::spawn(async move {
            let mut got: u64 = 0;
            while got < target {
                match c.recv_async().await {
                    Ok(_) => got += 1,
                    Err(_) => break,
                }
            }
        });
        let t0 = Instant::now();
        let mut ph: Vec<tokio::task::JoinHandle<()>> = ps
            .into_iter()
            .map(|mut p| {
                tokio::spawn(async move {
                    for k in 0..PER {
                        p.send_async(k as usize).await;
                    }
                })
            })
            .collect();
        for h in ph.drain(..) {
            h.await.unwrap();
        }
        consumer.await.unwrap();
        let el = t0.elapsed().as_secs_f64() * 1000.0;
        sd2.signal();
        drop(sd);
        el
    })
}
