//! In-memory SPSC head-to-head with batching:
//! Shuttles batches of 36 messages per queue slot to amortize sync overhead.

use std::time::Instant;

const N: usize = 1_000_000;
const CAP: usize = 32;
const WARMUP: usize = 1;
const ROUNDS: usize = 5;

const BATCH_SIZE: usize = 36;
type Msg = usize;
type Batch = [Msg; BATCH_SIZE];

fn header() {
    println!(
        "\n{:<32} {:>12} {:>12} {:>12} {:>14}",
        "impl (batched x36)", "min ns/msg", "p50 ns/msg", "p99 ns/msg", "msgs/sec (p50)"
    );
    println!("{}", "─".repeat(86));
}

fn row(name: &str, mut samples_ns_per_msg: Vec<f64>) {
    samples_ns_per_msg.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = samples_ns_per_msg.len();
    let min = samples_ns_per_msg[0];
    let p50 = samples_ns_per_msg[n / 2];
    let p99_idx = ((0.99 * n as f64).ceil() as usize)
        .saturating_sub(1)
        .min(n - 1);
    let p99 = samples_ns_per_msg[p99_idx];
    let ops = 1e9 / p50;
    println!(
        "{:<32} {:>12.2} {:>12.2} {:>12.2} {:>14.0}",
        name, min, p50, p99, ops
    );
}

// ── tokio mpsc (baseline) ─────────────────────────────────────────────
mod tokio_batched_impl {
    use super::{Batch, Msg, BATCH_SIZE, CAP, N};
    use std::time::Instant;
    use tokio::sync::mpsc;

    pub fn run() -> f64 {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (tx, mut rx) = mpsc::channel::<Batch>(CAP);
            let n_batches = N / BATCH_SIZE;
            let t0 = Instant::now();
            let consumer = tokio::spawn(async move {
                while let Some(batch) = rx.recv().await {
                    for m in batch {
                        std::hint::black_box(m);
                    }
                }
            });
            for i in 0..n_batches {
                let mut batch = [0usize; BATCH_SIZE];
                for j in 0..BATCH_SIZE {
                    batch[j] = (i * BATCH_SIZE + j) as Msg;
                }
                tx.send(batch).await.unwrap();
            }
            drop(tx);
            consumer.await.unwrap();
            let ns = t0.elapsed().as_nanos() as f64;
            ns / (n_batches * BATCH_SIZE) as f64
        })
    }
}

// ── kit Ring (thread, ParkWaiter) ─────────────────────────────────────
mod kit_ring_thread {
    use super::{Batch, Msg, BATCH_SIZE, CAP, N};
    use arbitro_kit::stream::Ring;
    use std::thread;
    use std::time::Instant;

    pub fn run() -> f64 {
        let (mut tx, mut rx) = Ring::<Batch, CAP>::new();
        let n_batches = N / BATCH_SIZE;

        let consumer = thread::spawn(move || {
            for _ in 0..n_batches {
                let batch = rx.recv().unwrap();
                for m in batch {
                    std::hint::black_box(m);
                }
            }
        });

        thread::yield_now();
        let t0 = Instant::now();
        for i in 0..n_batches {
            let mut batch = [0usize; BATCH_SIZE];
            for j in 0..BATCH_SIZE {
                batch[j] = (i * BATCH_SIZE + j) as Msg;
            }
            tx.send(batch).unwrap();
        }
        consumer.join().unwrap();
        let ns = t0.elapsed().as_nanos() as f64;
        ns / (n_batches * BATCH_SIZE) as f64
    }
}

// ── kit Ring (tokio, NotifyWaiter) ────────────────────────────────────
mod kit_ring_tokio {
    use super::{Batch, Msg, BATCH_SIZE, CAP, N};
    use arbitro_kit::stream::Ring;
    use arbitro_kit::waiter::NotifyWaiter;
    use std::time::Instant;

    pub fn run() -> f64 {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (mut tx, mut rx) = Ring::<Batch, CAP, NotifyWaiter>::new();
            let n_batches = N / BATCH_SIZE;
            let t0 = Instant::now();
            let producer = async {
                for i in 0..n_batches {
                    let mut batch = [0usize; BATCH_SIZE];
                    for j in 0..BATCH_SIZE {
                        batch[j] = (i * BATCH_SIZE + j) as Msg;
                    }
                    tx.send_async(batch).await.unwrap();
                }
            };
            let consumer = async {
                for _ in 0..n_batches {
                    let batch = rx.recv_async().await.unwrap();
                    for m in batch {
                        std::hint::black_box(m);
                    }
                }
            };
            tokio::join!(producer, consumer);
            let ns = t0.elapsed().as_nanos() as f64;
            ns / (n_batches * BATCH_SIZE) as f64
        })
    }
}

mod kit_mpsc_async_tokio {
    use super::{Batch, Msg, BATCH_SIZE, CAP, N};
    use arbitro_kit::route::MpscAsync;
    use std::time::Instant;

    pub fn run() -> f64 {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (mut producers, mut consumer, _sd) =
                MpscAsync::<Batch, CAP>::new(1);
            let mut producer = producers.remove(0);
            producer.bind();
            consumer.bind();
            let n_batches = N / BATCH_SIZE;
            let t0 = Instant::now();
            let producer_task = async {
                for i in 0..n_batches {
                    let mut batch = [0usize; BATCH_SIZE];
                    for j in 0..BATCH_SIZE {
                        batch[j] = (i * BATCH_SIZE + j) as Msg;
                    }
                    producer.send_async(batch).await;
                }
            };
            let consumer_task = async {
                for _ in 0..n_batches {
                    let batch = consumer.recv_async().await.unwrap();
                    for m in batch {
                        std::hint::black_box(m);
                    }
                }
            };
            tokio::join!(producer_task, consumer_task);
            let ns = t0.elapsed().as_nanos() as f64;
            ns / (n_batches * BATCH_SIZE) as f64
        })
    }
}

fn measure<F: FnMut() -> f64>(mut f: F) -> Vec<f64> {
    for _ in 0..WARMUP {
        let _ = f();
    }
    (0..ROUNDS).map(|_| f()).collect()
}

fn main() {
    println!(
        "In-memory batched (x{}) (N={} msgs, CAP={}, batch={} B, warmup={}, rounds={})",
        BATCH_SIZE,
        N,
        CAP,
        std::mem::size_of::<Batch>(),
        WARMUP,
        ROUNDS
    );
    header();
    row("tokio mpsc [batched]", measure(tokio_batched_impl::run));
    row("kit Ring (thread) [batched]", measure(kit_ring_thread::run));
    row("kit Ring (tokio) [batched]", measure(kit_ring_tokio::run));
    row("kit MpscAsync (tokio) [batched]", measure(kit_mpsc_async_tokio::run));
    println!("Done.");
}
