//! OneShot factorial head-to-head — kit vs tokio / crossbeam across the full
//! cube: {os-thread | tokio-runtime} × {single-thread | cross-thread} ×
//! {serial | batch}.
//!
//! - backend **os**    → `kit::OneShot<ParkWaiter>` vs `crossbeam::bounded(1)`
//! - backend **tokio** → `kit::OneShotAsync<NotifyWaiter>` vs `tokio::sync::oneshot`
//!
//! Axes:
//! - threads: **single** = producer and consumer on one thread/task (no wake,
//!   value already present); **cross** = consumer parks/awaits, a separate
//!   thread sends → the real wake round-trip.
//! - mode: **serial** = one oneshot fully round-trips before the next;
//!   **batch(N)** = N oneshots in flight at once (fire all, then collect all)
//!   to amortize handoff.
//!
//! Every cell performs OPS oneshot round-trips per timed sample, so ns/op is
//! directly comparable across cells. Conforms to bench_safety: bounded work,
//! one at a time, tee log expected, no background work.

use std::hint::black_box;
use std::thread;
use std::time::Instant;

use arbitro_kit::route::{OneShot, OneShotAsync, OneShotAsyncSender, OneShotSender};
use crossbeam_channel::{bounded, unbounded};
use tokio::sync::oneshot as tokio_oneshot;

const OPS: usize = 1000; // oneshot round-trips per timed sample
const INFLIGHT: usize = 64; // pipeline depth in batch mode

fn rounds() -> usize {
    std::env::var("BENCH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40)
}

fn header(title: &str) {
    println!("\n── {} ──", title);
    println!(
        "{:<34} {:>12} {:>12} {:>12} {:>14}",
        "variant", "mean_ns/op", "p50_ns/op", "p99_ns/op", "ops/sec"
    );
    println!("{}", "─".repeat(88));
}

fn row(name: &str, mut batch_ns: Vec<u64>) {
    batch_ns.sort_unstable();
    let samples = batch_ns.len();
    let total: u64 = batch_ns.iter().sum();
    let total_ops = samples * OPS;
    let ops = total_ops as f64 / (total as f64 / 1e9);
    let mean = total as f64 / total_ops as f64;
    let p50 = batch_ns[samples / 2] as f64 / OPS as f64;
    let p99 = batch_ns[samples * 99 / 100] as f64 / OPS as f64;
    println!(
        "{:<34} {:>12.2} {:>12.2} {:>12.2} {:>14}",
        name, mean, p50, p99, ops as u64
    );
}

/// 5 warmup batches, then `rounds()` timed batches (each OPS round-trips).
fn measure<F: FnMut()>(mut batch: F) -> Vec<u64> {
    for _ in 0..5 {
        batch();
    }
    (0..rounds())
        .map(|_| {
            let t0 = Instant::now();
            batch();
            t0.elapsed().as_nanos() as u64
        })
        .collect()
}

// ══════════════════════ OS-THREAD BACKEND ══════════════════════
// kit::OneShot<Park> vs crossbeam::bounded(1)

mod os_single_serial {
    use super::*;
    pub fn kit() -> Vec<u64> {
        measure(|| {
            for _ in 0..OPS {
                let (tx, rx) = OneShot::<u64>::new();
                tx.send(1);
                black_box(rx.recv().unwrap());
            }
        })
    }
    pub fn crossbeam() -> Vec<u64> {
        measure(|| {
            for _ in 0..OPS {
                let (tx, rx) = bounded::<u64>(1);
                tx.send(1).unwrap();
                black_box(rx.recv().unwrap());
            }
        })
    }
}

mod os_single_batch {
    use super::*;
    pub fn kit() -> Vec<u64> {
        measure(|| {
            for _ in 0..(OPS / INFLIGHT) {
                let mut rxs = Vec::with_capacity(INFLIGHT);
                for _ in 0..INFLIGHT {
                    let (tx, rx) = OneShot::<u64>::new();
                    tx.send(1);
                    rxs.push(rx);
                }
                for rx in rxs {
                    black_box(rx.recv().unwrap());
                }
            }
        })
    }
    pub fn crossbeam() -> Vec<u64> {
        measure(|| {
            for _ in 0..(OPS / INFLIGHT) {
                let mut rxs = Vec::with_capacity(INFLIGHT);
                for _ in 0..INFLIGHT {
                    let (tx, rx) = bounded::<u64>(1);
                    tx.send(1).unwrap();
                    rxs.push(rx);
                }
                for rx in rxs {
                    black_box(rx.recv().unwrap());
                }
            }
        })
    }
}

mod os_cross_serial {
    use super::*;
    pub fn kit() -> Vec<u64> {
        let (job_tx, job_rx) = unbounded::<OneShotSender<u64>>();
        let worker = thread::spawn(move || {
            while let Ok(tx) = job_rx.recv() {
                tx.send(black_box(1));
            }
        });
        let out = measure(|| {
            for _ in 0..OPS {
                let (tx, rx) = OneShot::<u64>::new();
                rx.bind();
                job_tx.send(tx).unwrap();
                black_box(rx.recv().unwrap());
            }
        });
        drop(job_tx);
        worker.join().unwrap();
        out
    }
    pub fn crossbeam() -> Vec<u64> {
        let (job_tx, job_rx) = unbounded::<crossbeam_channel::Sender<u64>>();
        let worker = thread::spawn(move || {
            while let Ok(tx) = job_rx.recv() {
                let _ = tx.send(black_box(1));
            }
        });
        let out = measure(|| {
            for _ in 0..OPS {
                let (tx, rx) = bounded::<u64>(1);
                job_tx.send(tx).unwrap();
                black_box(rx.recv().unwrap());
            }
        });
        drop(job_tx);
        worker.join().unwrap();
        out
    }
}

mod os_cross_batch {
    use super::*;
    pub fn kit() -> Vec<u64> {
        let (job_tx, job_rx) = unbounded::<OneShotSender<u64>>();
        let worker = thread::spawn(move || {
            while let Ok(tx) = job_rx.recv() {
                tx.send(black_box(1));
            }
        });
        let out = measure(|| {
            for _ in 0..(OPS / INFLIGHT) {
                let mut rxs = Vec::with_capacity(INFLIGHT);
                for _ in 0..INFLIGHT {
                    let (tx, rx) = OneShot::<u64>::new();
                    rx.bind();
                    job_tx.send(tx).unwrap();
                    rxs.push(rx);
                }
                for rx in rxs {
                    black_box(rx.recv().unwrap());
                }
            }
        });
        drop(job_tx);
        worker.join().unwrap();
        out
    }
    pub fn crossbeam() -> Vec<u64> {
        let (job_tx, job_rx) = unbounded::<crossbeam_channel::Sender<u64>>();
        let worker = thread::spawn(move || {
            while let Ok(tx) = job_rx.recv() {
                let _ = tx.send(black_box(1));
            }
        });
        let out = measure(|| {
            for _ in 0..(OPS / INFLIGHT) {
                let mut rxs = Vec::with_capacity(INFLIGHT);
                for _ in 0..INFLIGHT {
                    let (tx, rx) = bounded::<u64>(1);
                    job_tx.send(tx).unwrap();
                    rxs.push(rx);
                }
                for rx in rxs {
                    black_box(rx.recv().unwrap());
                }
            }
        });
        drop(job_tx);
        worker.join().unwrap();
        out
    }
}

// ══════════════════════ TOKIO-RUNTIME BACKEND ══════════════════════
// kit::OneShotAsync<Notify> vs tokio::sync::oneshot

fn rt_current() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}
fn rt_multi() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

mod tokio_single_serial {
    use super::*;
    pub fn kit() -> Vec<u64> {
        let rt = rt_current();
        measure(|| {
            rt.block_on(async {
                for _ in 0..OPS {
                    let (tx, rx) = OneShotAsync::<u64>::new();
                    tx.send(1);
                    black_box(rx.recv_async().await.unwrap());
                }
            });
        })
    }
    pub fn tokio() -> Vec<u64> {
        let rt = rt_current();
        measure(|| {
            rt.block_on(async {
                for _ in 0..OPS {
                    let (tx, rx) = tokio_oneshot::channel::<u64>();
                    let _ = tx.send(1);
                    black_box(rx.await.unwrap());
                }
            });
        })
    }
}

mod tokio_single_batch {
    use super::*;
    pub fn kit() -> Vec<u64> {
        let rt = rt_current();
        measure(|| {
            rt.block_on(async {
                for _ in 0..(OPS / INFLIGHT) {
                    let mut rxs = Vec::with_capacity(INFLIGHT);
                    for _ in 0..INFLIGHT {
                        let (tx, rx) = OneShotAsync::<u64>::new();
                        tx.send(1);
                        rxs.push(rx);
                    }
                    for rx in rxs {
                        black_box(rx.recv_async().await.unwrap());
                    }
                }
            });
        })
    }
    pub fn tokio() -> Vec<u64> {
        let rt = rt_current();
        measure(|| {
            rt.block_on(async {
                for _ in 0..(OPS / INFLIGHT) {
                    let mut rxs = Vec::with_capacity(INFLIGHT);
                    for _ in 0..INFLIGHT {
                        let (tx, rx) = tokio_oneshot::channel::<u64>();
                        let _ = tx.send(1);
                        rxs.push(rx);
                    }
                    for rx in rxs {
                        black_box(rx.await.unwrap());
                    }
                }
            });
        })
    }
}

mod tokio_cross_serial {
    use super::*;
    pub fn kit() -> Vec<u64> {
        let rt = rt_multi();
        let (job_tx, job_rx) = unbounded::<OneShotAsyncSender<u64>>();
        let worker = thread::spawn(move || {
            while let Ok(tx) = job_rx.recv() {
                tx.send(black_box(1));
            }
        });
        let out = measure(|| {
            rt.block_on(async {
                for _ in 0..OPS {
                    let (tx, rx) = OneShotAsync::<u64>::new();
                    job_tx.send(tx).unwrap();
                    black_box(rx.recv_async().await.unwrap());
                }
            });
        });
        drop(job_tx);
        worker.join().unwrap();
        out
    }
    pub fn tokio() -> Vec<u64> {
        let rt = rt_multi();
        let (job_tx, job_rx) = unbounded::<tokio_oneshot::Sender<u64>>();
        let worker = thread::spawn(move || {
            while let Ok(tx) = job_rx.recv() {
                let _ = tx.send(black_box(1));
            }
        });
        let out = measure(|| {
            rt.block_on(async {
                for _ in 0..OPS {
                    let (tx, rx) = tokio_oneshot::channel::<u64>();
                    job_tx.send(tx).unwrap();
                    black_box(rx.await.unwrap());
                }
            });
        });
        drop(job_tx);
        worker.join().unwrap();
        out
    }
}

mod tokio_cross_batch {
    use super::*;
    pub fn kit() -> Vec<u64> {
        let rt = rt_multi();
        let (job_tx, job_rx) = unbounded::<OneShotAsyncSender<u64>>();
        let worker = thread::spawn(move || {
            while let Ok(tx) = job_rx.recv() {
                tx.send(black_box(1));
            }
        });
        let out = measure(|| {
            rt.block_on(async {
                for _ in 0..(OPS / INFLIGHT) {
                    let mut rxs = Vec::with_capacity(INFLIGHT);
                    for _ in 0..INFLIGHT {
                        let (tx, rx) = OneShotAsync::<u64>::new();
                        job_tx.send(tx).unwrap();
                        rxs.push(rx);
                    }
                    for rx in rxs {
                        black_box(rx.recv_async().await.unwrap());
                    }
                }
            });
        });
        drop(job_tx);
        worker.join().unwrap();
        out
    }
    pub fn tokio() -> Vec<u64> {
        let rt = rt_multi();
        let (job_tx, job_rx) = unbounded::<tokio_oneshot::Sender<u64>>();
        let worker = thread::spawn(move || {
            while let Ok(tx) = job_rx.recv() {
                let _ = tx.send(black_box(1));
            }
        });
        let out = measure(|| {
            rt.block_on(async {
                for _ in 0..(OPS / INFLIGHT) {
                    let mut rxs = Vec::with_capacity(INFLIGHT);
                    for _ in 0..INFLIGHT {
                        let (tx, rx) = tokio_oneshot::channel::<u64>();
                        rxs.push(rx);
                        job_tx.send(tx).unwrap();
                    }
                    for rx in rxs {
                        black_box(rx.await.unwrap());
                    }
                }
            });
        });
        drop(job_tx);
        worker.join().unwrap();
        out
    }
}

fn main() {
    println!(
        "oneshot factorial — OPS={OPS}/sample, INFLIGHT={INFLIGHT}, rounds={}",
        rounds()
    );

    println!("\n═══════════ OS-THREAD backend (kit::OneShot<Park> vs crossbeam::bounded(1)) ═══════════");

    header("os · single-thread · serial");
    row("kit::OneShot<Park>", os_single_serial::kit());
    row("crossbeam::bounded(1)", os_single_serial::crossbeam());

    header("os · single-thread · batch(64)");
    row("kit::OneShot<Park>", os_single_batch::kit());
    row("crossbeam::bounded(1)", os_single_batch::crossbeam());

    header("os · cross-thread · serial");
    row("kit::OneShot<Park>", os_cross_serial::kit());
    row("crossbeam::bounded(1)", os_cross_serial::crossbeam());

    header("os · cross-thread · batch(64)");
    row("kit::OneShot<Park>", os_cross_batch::kit());
    row("crossbeam::bounded(1)", os_cross_batch::crossbeam());

    println!("\n═══════════ TOKIO-RUNTIME backend (kit::OneShotAsync<Notify> vs tokio::oneshot) ═══════════");

    header("tokio · single-thread · serial");
    row("kit::OneShotAsync<Notify>", tokio_single_serial::kit());
    row("tokio::oneshot", tokio_single_serial::tokio());

    header("tokio · single-thread · batch(64)");
    row("kit::OneShotAsync<Notify>", tokio_single_batch::kit());
    row("tokio::oneshot", tokio_single_batch::tokio());

    header("tokio · cross-thread · serial");
    row("kit::OneShotAsync<Notify>", tokio_cross_serial::kit());
    row("tokio::oneshot", tokio_cross_serial::tokio());

    header("tokio · cross-thread · batch(64)");
    row("kit::OneShotAsync<Notify>", tokio_cross_batch::kit());
    row("tokio::oneshot", tokio_cross_batch::tokio());

    println!("\nDone.");
}
