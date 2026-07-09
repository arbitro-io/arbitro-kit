//! In-memory message-passing head-to-head with batching:
//! Shuttles batches of 36 messages per queue slot to amortize synchronization overhead.

use std::time::Instant;

const N: usize = 1_000_000;
const CAP: usize = 32;
const WARMUP: usize = 1;
const ROUNDS: usize = 5;

const BATCH_SIZE: usize = 36;
type Msg = usize;
type Batch = [Msg; BATCH_SIZE];

// ══════════════════════════════════════════════════════════════════════
// Reporting
// ══════════════════════════════════════════════════════════════════════

fn header() {
    println!(
        "\n{:<25} {:>12} {:>12} {:>12} {:>14}",
        "impl (batched x36)", "min ns/msg", "p50 ns/msg", "p99 ns/msg", "msgs/sec (p50)"
    );
    println!("{}", "─".repeat(79));
}

fn row(name: &str, mut samples_ns_per_msg: Vec<f64>) {
    samples_ns_per_msg.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = samples_ns_per_msg.len();
    let min = samples_ns_per_msg[0];
    let p50 = samples_ns_per_msg[n / 2];
    let p99_idx = ((0.99 * n as f64).ceil() as usize).saturating_sub(1).min(n - 1);
    let p99 = samples_ns_per_msg[p99_idx];
    let ops = 1e9 / p50;
    println!("{:<25} {:>12.2} {:>12.2} {:>12.2} {:>14.0}", name, min, p50, p99, ops);
}

// ══════════════════════════════════════════════════════════════════════
// Tokio Batched
// ══════════════════════════════════════════════════════════════════════

mod tokio_batched_impl {
    use super::{Batch, Msg, CAP, N, BATCH_SIZE};
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

            let consumer = tokio::spawn(async move {
                while let Some(batch) = rx.recv().await {
                    for m in batch {
                        std::hint::black_box(m);
                    }
                }
            });

            let t0 = Instant::now();
            let n_batches = N / BATCH_SIZE;
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

// ══════════════════════════════════════════════════════════════════════
// kit Ring Batched (v1)
// ══════════════════════════════════════════════════════════════════════

mod kit_batched_impl {
    use super::{Batch, Msg, CAP, N, BATCH_SIZE};
    use arbitro_kit::stream::Ring;
    use std::thread;
    use std::time::Instant;

    pub fn run() -> f64 {
        let ring = std::sync::Arc::new(Ring::<Batch, CAP>::new());
        let ring_c = ring.clone();
        let n_batches = N / BATCH_SIZE;

        ring.set_producer(thread::current());

        let consumer = thread::spawn(move || {
            ring_c.set_consumer(thread::current());
            for _ in 0..n_batches {
                let batch = ring_c.recv();
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
            ring.send(batch);
        }
        consumer.join().unwrap();
        let ns = t0.elapsed().as_nanos() as f64;
        ns / (n_batches * BATCH_SIZE) as f64
    }
}

// ══════════════════════════════════════════════════════════════════════
// kit Ring2 Batched (v2)
// ══════════════════════════════════════════════════════════════════════

mod kit2_batched_impl {
    use super::{Batch, Msg, CAP, N, BATCH_SIZE};
    use arbitro_kit::stream::Ring2;
    use std::thread;
    use std::time::Instant;

    pub fn run() -> f64 {
        let (mut tx, mut rx) = Ring2::<Batch, CAP>::new();
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

// ══════════════════════════════════════════════════════════════════════
// kit Spsc2 Batched (v2)
// ══════════════════════════════════════════════════════════════════════

mod spsc2_batched_impl {
    use super::{Batch, Msg, CAP, N, BATCH_SIZE};
    use arbitro_kit::stream::Spsc2;
    use std::thread;
    use std::time::Instant;

    pub fn run() -> f64 {
        let (mut tx, mut rx) = Spsc2::<Batch, CAP>::new();
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

// ══════════════════════════════════════════════════════════════════════
// kit Spsc2 Batched (tokio) (v2)
// ══════════════════════════════════════════════════════════════════════

mod spsc2_tokio_batched_impl {
    use super::{Batch, Msg, CAP, N, BATCH_SIZE};
    use arbitro_kit::stream::Spsc2;
    use arbitro_kit::waiter::NotifyWaiter;
    use std::time::Instant;

    pub fn run() -> f64 {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let (mut tx, mut rx) = Spsc2::<Batch, CAP, NotifyWaiter>::new();
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

// ══════════════════════════════════════════════════════════════════════
// Main
// ══════════════════════════════════════════════════════════════════════

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
    row("tokio (mpsc) [batched]", measure(tokio_batched_impl::run));
    row("kit Ring (thread) [batched]", measure(kit_batched_impl::run));
    row("kit Ring2 (thread) [batched]", measure(kit2_batched_impl::run));
    row("kit Spsc2 (thread) [batched]", measure(spsc2_batched_impl::run));
    row("kit Spsc2 (tokio) [batched]", measure(spsc2_tokio_batched_impl::run));
    println!("Done.");
}
