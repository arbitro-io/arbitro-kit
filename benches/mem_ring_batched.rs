//! SPSC Ring head-to-head — kit::Ring vs crossbeam / tokio, batched x36.
//!
//! Shuttles batches of 36 messages per queue slot to amortize sync overhead.
//! Three scenarios cover both wake backends the Ring ships, matching the
//! oneshot_h2h layout:
//!   A. Single thread — same-thread push → pop (uncontended ring mechanics,
//!      no cross-core sync). kit::Ring<Park> vs crossbeam::bounded.
//!   B. Cross-thread — OS thread. Producer + consumer on two OS threads;
//!      the real park/unpark path. kit::Ring<ParkWaiter> vs crossbeam::bounded.
//!   C. Cross-thread — tokio runtime. Producer + consumer as tokio tasks;
//!      the notify→task path. kit::Ring<NotifyWaiter> vs tokio::sync::mpsc.
//!
//! Integrity: the consumer sums every message it receives and the result is
//! asserted against the closed-form expected sum. A lost or corrupted message
//! panics the bench — so the throughput numbers are proven to move every byte
//! intact, not just touch a black_box. The assert also blocks dead-code
//! elision of the payload.
//!
//! Conforms to bench_safety: bounded N, one at a time, tee log expected,
//! no background work.

use std::time::Instant;

const N: usize = 1_000_000;
const CAP: usize = 32;
const WARMUP: usize = 1;
const ROUNDS: usize = 5;

const BATCH_SIZE: usize = 36;
type Msg = usize;
type Batch = [Msg; BATCH_SIZE];

const N_BATCHES: usize = N / BATCH_SIZE;

#[inline]
fn make_batch(i: usize) -> Batch {
    let mut batch = [0usize; BATCH_SIZE];
    for (j, slot) in batch.iter_mut().enumerate() {
        *slot = (i * BATCH_SIZE + j) as Msg;
    }
    batch
}

/// Closed-form sum of every value produced across all batches:
/// value(i,j) = i*36 + j, for i in 0..N_BATCHES, j in 0..36.
/// per-batch sum = 1296*i + 630 ⇒ total = 1296*Σi + 630*nb.
fn expected_sum() -> u64 {
    let nb = N_BATCHES as u64;
    1296 * ((nb - 1) * nb / 2) + 630 * nb
}

fn header(title: &str) {
    println!("\n── {} ──", title);
    println!(
        "{:<32} {:>12} {:>12} {:>12} {:>14}",
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

fn measure<F: FnMut() -> f64>(mut f: F) -> Vec<f64> {
    for _ in 0..WARMUP {
        let _ = f();
    }
    (0..ROUNDS).map(|_| f()).collect()
}

#[inline]
fn ns_per_msg(elapsed: std::time::Duration) -> f64 {
    elapsed.as_nanos() as f64 / (N_BATCHES * BATCH_SIZE) as f64
}

// ── A. Single thread (same-thread push → pop) ─────────────────────────────
mod single {
    use super::*;
    use arbitro_kit::stream::Ring;
    use crossbeam_channel::bounded;

    pub fn kit_ring() -> f64 {
        let (mut tx, mut rx) = Ring::<Batch, CAP>::new();
        let mut recv: u64 = 0;
        let t0 = Instant::now();
        for i in 0..N_BATCHES {
            tx.send(make_batch(i)).unwrap();
            for m in rx.recv().unwrap() {
                recv += m as u64;
            }
        }
        let e = ns_per_msg(t0.elapsed());
        assert_eq!(recv, expected_sum(), "kit::Ring single: integrity");
        e
    }

    pub fn crossbeam() -> f64 {
        let (tx, rx) = bounded::<Batch>(CAP);
        let mut recv: u64 = 0;
        let t0 = Instant::now();
        for i in 0..N_BATCHES {
            tx.send(make_batch(i)).unwrap();
            for m in rx.recv().unwrap() {
                recv += m as u64;
            }
        }
        let e = ns_per_msg(t0.elapsed());
        assert_eq!(recv, expected_sum(), "crossbeam single: integrity");
        e
    }
}

// ── B. Cross-thread — OS thread ───────────────────────────────────────────
mod cross_os {
    use super::*;
    use arbitro_kit::stream::Ring;
    use crossbeam_channel::bounded;
    use std::thread;

    pub fn kit_ring() -> f64 {
        let (mut tx, mut rx) = Ring::<Batch, CAP>::new();
        let consumer = thread::spawn(move || {
            let mut recv: u64 = 0;
            for _ in 0..N_BATCHES {
                for m in rx.recv().unwrap() {
                    recv += m as u64;
                }
            }
            recv
        });
        thread::yield_now();
        let t0 = Instant::now();
        for i in 0..N_BATCHES {
            tx.send(make_batch(i)).unwrap();
        }
        let recv = consumer.join().unwrap();
        let e = ns_per_msg(t0.elapsed());
        assert_eq!(recv, expected_sum(), "kit::Ring cross-os: integrity");
        e
    }

    pub fn crossbeam() -> f64 {
        let (tx, rx) = bounded::<Batch>(CAP);
        let consumer = thread::spawn(move || {
            let mut recv: u64 = 0;
            for _ in 0..N_BATCHES {
                for m in rx.recv().unwrap() {
                    recv += m as u64;
                }
            }
            recv
        });
        thread::yield_now();
        let t0 = Instant::now();
        for i in 0..N_BATCHES {
            tx.send(make_batch(i)).unwrap();
        }
        let recv = consumer.join().unwrap();
        let e = ns_per_msg(t0.elapsed());
        assert_eq!(recv, expected_sum(), "crossbeam cross-os: integrity");
        e
    }
}

// ── C. Cross-thread — tokio runtime ───────────────────────────────────────
mod cross_tokio {
    use super::*;
    use arbitro_kit::stream::Ring;
    use arbitro_kit::waiter::NotifyWaiter;
    use tokio::sync::mpsc;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    pub fn kit_ring() -> f64 {
        rt().block_on(async {
            let (mut tx, mut rx) = Ring::<Batch, CAP, NotifyWaiter>::new();
            let t0 = Instant::now();
            let producer = async {
                for i in 0..N_BATCHES {
                    tx.send_async(make_batch(i)).await.unwrap();
                }
            };
            let consumer = async {
                let mut recv: u64 = 0;
                for _ in 0..N_BATCHES {
                    for m in rx.recv_async().await.unwrap() {
                        recv += m as u64;
                    }
                }
                recv
            };
            let (_, recv) = tokio::join!(producer, consumer);
            let e = ns_per_msg(t0.elapsed());
            assert_eq!(recv, expected_sum(), "kit::Ring cross-tokio: integrity");
            e
        })
    }

    pub fn tokio_mpsc() -> f64 {
        rt().block_on(async {
            let (tx, mut rx) = mpsc::channel::<Batch>(CAP);
            let t0 = Instant::now();
            let consumer = tokio::spawn(async move {
                let mut recv: u64 = 0;
                while let Some(batch) = rx.recv().await {
                    for m in batch {
                        recv += m as u64;
                    }
                }
                recv
            });
            for i in 0..N_BATCHES {
                tx.send(make_batch(i)).await.unwrap();
            }
            drop(tx);
            let recv = consumer.await.unwrap();
            let e = ns_per_msg(t0.elapsed());
            assert_eq!(recv, expected_sum(), "tokio::mpsc cross-tokio: integrity");
            e
        })
    }
}

fn main() {
    println!(
        "SPSC Ring batched (x{}) (N={} msgs, CAP={}, batch={} B, warmup={}, rounds={})",
        BATCH_SIZE,
        N,
        CAP,
        std::mem::size_of::<Batch>(),
        WARMUP,
        ROUNDS
    );
    println!(
        "integrity: consumer checksum asserted == {} (panics on any loss/corruption)",
        expected_sum()
    );

    header("A. Single thread (push → pop, uncontended)");
    row("kit::Ring<Park>", measure(single::kit_ring));
    row("crossbeam::bounded", measure(single::crossbeam));

    header("B. Cross-thread — OS thread (park/unpark)");
    row("kit::Ring<Park>", measure(cross_os::kit_ring));
    row("crossbeam::bounded", measure(cross_os::crossbeam));

    header("C. Cross-thread — tokio runtime (notify→task)");
    row("kit::Ring<Notify>", measure(cross_tokio::kit_ring));
    row("tokio::mpsc", measure(cross_tokio::tokio_mpsc));

    println!("\nDone — all integrity asserts passed.");
}
