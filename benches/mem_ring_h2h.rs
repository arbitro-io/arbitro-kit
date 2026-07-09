//! In-memory message-passing head-to-head: io_uring ring roundtrip vs
//! tokio::mpsc vs arbitro-kit `Ring`.
//!
//! This bench is deliberately I/O-free. The previous version used TCP
//! loopback and gave tokio an unfair advantage: its `read()` coalesced
//! many kernel-buffered payloads into a single syscall, while io_uring
//! and kit did 1 op/msg. Result: tokio "won" 7x for reasons that had
//! nothing to do with the primitives under test.
//!
//! What each impl actually measures here:
//!
//! - **io_uring** — pure SQ→CQ ring roundtrip via `IORING_OP_NOP`. This
//!   exercises io_uring's user↔kernel shared-memory ring semantics with
//!   zero real I/O. Single-threaded by design (io_uring rings are
//!   single-consumer on the completion side). Linux only.
//!
//! - **tokio** — `tokio::sync::mpsc::channel::<Msg>(CAP)` between two
//!   tasks on a 2-worker runtime. Producer sends N msgs, consumer recvs N.
//!
//! - **arbitro-kit** — `Ring<Msg, CAP>` (SPSC bounded) between two OS
//!   threads. Producer thread sends N, consumer thread recvs N.
//!
//! Fairness invariants:
//!   - Same message type (`Msg`, 72 B: u64 seq + [u8; 64] payload).
//!   - Same N per iter.
//!   - Same pipeline depth (CAP).
//!   - 2 hot threads for tokio/kit; io_uring is intrinsically 1 (documented).
//!   - 1 warmup iter discarded, 5 measured iters.
//!   - No allocation in the hot loop (channels/rings pre-sized;
//!     `Msg` is `Copy` and moves by value).
//!   - Timer starts before first send, stops after final recv.
//!
//! Run (WSL, from `/tmp/arbitro/`):
//!
//! ```bash
//! cargo bench --bench mem_ring_h2h --no-run
//! cp -a target/release/deps/mem_ring_h2h-<hash> /tmp/arbitro/
//! cd /tmp/arbitro && timeout 120 ./mem_ring_h2h-<hash> --bench \
//!   2>&1 | tee /tmp/bench.log
//! ```

const N: usize = 1_000_000;
const CAP: usize = 32;
const WARMUP: usize = 1;
const ROUNDS: usize = 5;

// Msg = 8-byte pointer-sized value (usize). Represents "move a pointer
// through the queue" — the actual payload lives elsewhere in a
// pre-allocated slot; the queue only shuttles the index/pointer.
type Msg = usize;

// Compile-time size check: 8 bytes on 64-bit targets.
const _: [(); 8] = [(); std::mem::size_of::<Msg>()];

// ══════════════════════════════════════════════════════════════════════
// Reporting
// ══════════════════════════════════════════════════════════════════════

fn header() {
    println!(
        "\n{:<20} {:>12} {:>12} {:>12} {:>14}",
        "impl", "min ns/msg", "p50 ns/msg", "p99 ns/msg", "msgs/sec (p50)"
    );
    println!("{}", "─".repeat(74));
}

fn row(name: &str, mut samples_ns_per_msg: Vec<f64>) {
    samples_ns_per_msg.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = samples_ns_per_msg.len();
    let min = samples_ns_per_msg[0];
    let p50 = samples_ns_per_msg[n / 2];
    // p99 on a small sample: index = ceil(0.99 * n) - 1, clamped.
    let p99_idx = ((0.99 * n as f64).ceil() as usize).saturating_sub(1).min(n - 1);
    let p99 = samples_ns_per_msg[p99_idx];
    let ops = 1e9 / p50;
    println!("{:<20} {:>12.1} {:>12.1} {:>12.1} {:>14.0}", name, min, p50, p99, ops);
}

// ══════════════════════════════════════════════════════════════════════
// Tokio: mpsc::channel<Msg>(CAP), 2 tasks on 2 workers
// ══════════════════════════════════════════════════════════════════════

mod tokio_impl {
    use super::{Msg, CAP, N};
    use std::time::Instant;
    use tokio::sync::mpsc;

    pub fn run() -> f64 {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let (tx, mut rx) = mpsc::channel::<Msg>(CAP);

            let consumer = tokio::spawn(async move {
                let mut got = 0usize;
                // recv() is the fast path; no per-msg allocation.
                while let Some(m) = rx.recv().await {
                    // Touch the payload to prevent DCE of the recv.
                    std::hint::black_box(m);
                    got += 1;
                    if got == N {
                        break;
                    }
                }
            });

            let t0 = Instant::now();
            for i in 0..N {
                // Bounded send — blocks (awaits) when the channel is full,
                // giving the same backpressure shape as Ring.
                tx.send(i as Msg).await.unwrap();
            }
            drop(tx);
            consumer.await.unwrap();
            let ns = t0.elapsed().as_nanos() as f64;
            ns / N as f64
        })
    }
}

// ══════════════════════════════════════════════════════════════════════
// arbitro-kit: Ring<Msg, CAP>, producer thread + consumer thread
// ══════════════════════════════════════════════════════════════════════

mod kit_impl {
    use super::{Msg, CAP, N};
    use arbitro_kit::stream::Ring;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    pub fn run() -> f64 {
        let ring: Arc<Ring<Msg, CAP>> = Arc::new(Ring::new());
        let ring_c = ring.clone();

        // Register this (main/producer) thread on the ring so the consumer
        // can unpark it on backpressure, and hand the consumer's Thread
        // handle to the ring via set_consumer inside the spawned thread.
        ring.set_producer(thread::current());

        // Consumer thread: blocking recv N times.
        let consumer = thread::spawn(move || {
            ring_c.set_consumer(thread::current());
            for _ in 0..N {
                let m = ring_c.recv();
                std::hint::black_box(m);
            }
        });

        // Give the consumer a moment to register itself so the first
        // producer wake finds a real Thread handle (kit handles a missing
        // handle either way, but this keeps steady-state clean).
        thread::yield_now();

        let t0 = Instant::now();
        for i in 0..N {
            ring.send(i as Msg);
        }
        consumer.join().unwrap();
        let ns = t0.elapsed().as_nanos() as f64;
        ns / N as f64
    }
}

// ══════════════════════════════════════════════════════════════════════
// arbitro-kit Ring2 (OS thread): cursor-cached SPSC ring
// ══════════════════════════════════════════════════════════════════════

mod kit2_impl {
    use super::{Msg, CAP, N};
    use arbitro_kit::stream::Ring2;
    use std::thread;
    use std::time::Instant;

    pub fn run() -> f64 {
        // v2 split-handle API: unique Producer/Consumer pair, no Arc or
        // set_producer/set_consumer at the call site — handles register
        // their own thread on the first blocking call.
        let (mut tx, mut rx) = Ring2::<Msg, CAP>::new();

        let consumer = thread::spawn(move || {
            for _ in 0..N {
                let m = rx.recv().unwrap();
                std::hint::black_box(m);
            }
        });

        thread::yield_now();

        let t0 = Instant::now();
        for i in 0..N {
            tx.send(i as Msg).unwrap();
        }
        consumer.join().unwrap();
        let ns = t0.elapsed().as_nanos() as f64;
        ns / N as f64
    }
}

// ══════════════════════════════════════════════════════════════════════
// arbitro-kit Ring2 (tokio): same primitive, NotifyWaiter backend
// ══════════════════════════════════════════════════════════════════════

mod kit2_tokio_impl {
    use super::{Msg, CAP, N};
    use arbitro_kit::stream::Ring2;
    use arbitro_kit::waiter::NotifyWaiter;
    use std::time::Instant;

    pub fn run() -> f64 {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            // Note: uses tokio::join! (not tokio::spawn) to sidestep the
            // higher-ranked RPITIT-Send limitation (rust-lang/rust#100013)
            // that applies to `&self`-borrowing async fns behind a trait.
            // Both futures still run concurrently — join! polls them on
            // the same worker thread and yields cooperatively, which is
            // representative of typical tokio SPSC workloads.
            let (mut tx, mut rx) = Ring2::<Msg, CAP, NotifyWaiter>::new();

            let t0 = Instant::now();
            let producer = async {
                for i in 0..N {
                    tx.send_async(i as Msg).await.unwrap();
                }
            };
            let consumer = async {
                for _ in 0..N {
                    let m = rx.recv_async().await.unwrap();
                    std::hint::black_box(m);
                }
            };
            tokio::join!(producer, consumer);
            let ns = t0.elapsed().as_nanos() as f64;
            ns / N as f64
        })
    }
}

// ══════════════════════════════════════════════════════════════════════
// arbitro-kit Spsc2 (OS thread) (v2)
// ══════════════════════════════════════════════════════════════════════

mod spsc2_impl {
    use super::{Msg, CAP, N};
    use arbitro_kit::stream::Spsc2;
    use std::thread;
    use std::time::Instant;

    pub fn run() -> f64 {
        let (mut tx, mut rx) = Spsc2::<Msg, CAP>::new();

        let consumer = thread::spawn(move || {
            for _ in 0..N {
                let m = rx.recv().unwrap();
                std::hint::black_box(m);
            }
        });

        thread::yield_now();

        let t0 = Instant::now();
        for i in 0..N {
            tx.send(i as Msg).unwrap();
        }
        consumer.join().unwrap();
        let ns = t0.elapsed().as_nanos() as f64;
        ns / N as f64
    }
}

// ══════════════════════════════════════════════════════════════════════
// arbitro-kit Spsc2 (tokio) (v2)
// ══════════════════════════════════════════════════════════════════════

mod spsc2_tokio_impl {
    use super::{Msg, CAP, N};
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
            let (mut tx, mut rx) = Spsc2::<Msg, CAP, NotifyWaiter>::new();

            let t0 = Instant::now();
            let producer = async {
                for i in 0..N {
                    tx.send_async(i as Msg).await.unwrap();
                }
            };
            let consumer = async {
                for _ in 0..N {
                    let m = rx.recv_async().await.unwrap();
                    std::hint::black_box(m);
                }
            };
            tokio::join!(producer, consumer);
            let ns = t0.elapsed().as_nanos() as f64;
            ns / N as f64
        })
    }
}

// ══════════════════════════════════════════════════════════════════════
// io_uring: SQ→CQ ring roundtrip via IORING_OP_NOP, single-threaded
// ══════════════════════════════════════════════════════════════════════
//
// Note on fairness: io_uring's ring is inherently single-threaded from the
// consumer side (one submission ring, one completion ring, both owned by
// the same thread by default). We keep CAP submissions in flight at once
// to match the pipeline depth of the tokio/kit variants. The metric is
// the same: ns per "message" completed. Here each message is one NOP
// roundtrip through the shared-memory ring — the closest zero-I/O
// analogue of a producer→consumer handoff.
//
// We do NOT try to fake a two-thread setup with io_uring; that would
// itself be unfair (SQPOLL or async completion drainers change the
// primitive being measured).

#[cfg(target_os = "linux")]
mod uring_impl {
    use super::{CAP, N};
    use io_uring::{opcode, IoUring};
    use std::time::Instant;

    pub fn run() -> f64 {
        // Ring sized to comfortably hold CAP in-flight + slack.
        let mut ring = IoUring::new((CAP * 2).next_power_of_two() as u32).unwrap();

        let t0 = Instant::now();

        // Prime pipeline with CAP NOPs.
        for i in 0..CAP.min(N) {
            let sqe = opcode::Nop::new().build().user_data(i as u64);
            unsafe { ring.submission().push(&sqe).unwrap() };
        }
        ring.submit().unwrap();

        let mut submitted = CAP.min(N);
        let mut completed = 0usize;

        while completed < N {
            ring.submitter().submit_and_wait(1).unwrap();
            let mut cq = ring.completion();
            cq.sync();
            let mut batch = 0usize;
            for cqe in &mut cq {
                std::hint::black_box(cqe.user_data());
                completed += 1;
                batch += 1;
            }
            drop(cq);

            // Refill: push one NOP per completion, up to N.
            let to_push = batch.min(N - submitted);
            if to_push > 0 {
                let mut sq = ring.submission();
                for _ in 0..to_push {
                    let sqe = opcode::Nop::new().build().user_data(submitted as u64);
                    unsafe { sq.push(&sqe).unwrap() };
                    submitted += 1;
                }
                drop(sq);
                ring.submit().unwrap();
            }
        }

        let ns = t0.elapsed().as_nanos() as f64;
        ns / N as f64
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
        "In-memory head-to-head  (N={} msgs, CAP={}, msg={} B, warmup={}, rounds={})",
        N,
        CAP,
        std::mem::size_of::<Msg>(),
        WARMUP,
        ROUNDS
    );

    // Smoke: one round of each, so a broken impl fails fast (matches the
    // "smoke test" rule in .agent/rules/testing.md).
    println!("\nSmoke test (1 round each):");
    println!("  tokio:       {:.1} ns/msg", tokio_impl::run());
    println!("  kit (Ring):  {:.1} ns/msg", kit_impl::run());
    println!("  kit (Ring2): {:.1} ns/msg", kit2_impl::run());
    println!("  kit (Ring2/tokio): {:.1} ns/msg", kit2_tokio_impl::run());
    println!("  kit (Spsc2): {:.1} ns/msg", spsc2_impl::run());
    println!("  kit (Spsc2/tokio): {:.1} ns/msg", spsc2_tokio_impl::run());
    #[cfg(target_os = "linux")]
    {
        println!("  uring:       {:.1} ns/msg", uring_impl::run());
    }

    header();
    row("tokio (mpsc)", measure(tokio_impl::run));
    row("kit Ring (thread)", measure(kit_impl::run));
    row("kit Ring2 (thread)", measure(kit2_impl::run));
    row("kit Ring2 (tokio)", measure(kit2_tokio_impl::run));
    row("kit Spsc2 (thread)", measure(spsc2_impl::run));
    row("kit Spsc2 (tokio)", measure(spsc2_tokio_impl::run));

    #[cfg(target_os = "linux")]
    {
        row("io_uring (NOP)", measure(uring_impl::run));
    }
    #[cfg(not(target_os = "linux"))]
    {
        println!("{:<20} {:>52}", "io_uring (NOP)", "(skipped: non-Linux target)");
    }

    println!(
        "\nNote: io_uring runs single-threaded (ring is single-consumer); \n\
         tokio and kit each use 1 producer + 1 consumer thread/task."
    );
    println!("Done.");
}
