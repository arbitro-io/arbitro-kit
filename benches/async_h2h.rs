//! `async_h2h` — head-to-head bench: kit async primitives vs tokio::sync.
//!
//! Every kit primitive that exposes an `*_async` API is tested against the
//! closest tokio::sync equivalent under a multi-thread tokio runtime.
//!
//! Sections:
//!   A. Pipe async — single-slot 1:1 (kit PipeAsync vs tokio::sync::mpsc(1))
//!   B. Channel async — RPC round-trip (kit ChannelAsync vs tokio::oneshot pair)
//!   C. Ring async — SPSC bounded pipeline (kit Ring<NotifyWaiter> vs tokio::sync::mpsc)
//!   D. Mpsc async — M:1 fan-in (kit MpscAsync vs tokio::sync::mpsc)
//!   E. Mpmc async — M:N (kit MpmcAsync vs tokio::sync::mpsc per consumer)
//!
//! Methodology: warm up + N rounds. Each round sends BATCH messages through
//! the primitive and measures end-to-end wall time. Report ns/op, ops/sec.
//!
//! Conforms to bench_safety: BATCH = 1000, rounds capped via env, timeout
//! expected from runner, no background work.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arbitro_kit::route::{MpmcAsync, MpscAsync};
use arbitro_kit::slot::{ChannelAsync, PipeAsync};
use arbitro_kit::stream::Ring;
use arbitro_kit::waiter::NotifyWaiter;

const BATCH: usize = 1000;

fn rounds() -> usize {
    std::env::var("BENCH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300)
}
fn warmup() -> usize {
    30
}

fn pct(samples: &mut [u64], q: f64) -> u64 {
    samples.sort_unstable();
    let idx = ((samples.len() as f64) * q).clamp(0.0, (samples.len() - 1) as f64) as usize;
    samples[idx]
}

fn header(title: &str) {
    println!("\n── {} ──", title);
    println!(
        "{:<48} {:>10} {:>10} {:>10} {:>10} {:>14}",
        "variant", "mean_ns", "min_ns", "p50_ns", "p99_ns", "ops/sec"
    );
    println!("{}", "─".repeat(106));
}

fn report(label: &str, samples: &mut Vec<u64>) {
    if samples.is_empty() {
        return;
    }
    let total: u64 = samples.iter().sum();
    let n = samples.len() as u64;
    let mean = total / n;
    let min = *samples.iter().min().unwrap();
    let p50 = pct(samples, 0.50);
    let p99 = pct(samples, 0.99);
    let ops = if mean > 0 {
        (BATCH as f64 * 1e9) / mean as f64
    } else {
        0.0
    };
    println!(
        "{:<48} {:>10} {:>10} {:>10} {:>10} {:>14.0}",
        label, mean, min, p50, p99, ops
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// A. Pipe async — single-slot SPSC (fire-and-receive, reused pipe)
//
// Kit's PipeAsync is a single-slot: producer sends, consumer recv_async.
// Closest tokio equivalent: mpsc(1).
// ═══════════════════════════════════════════════════════════════════════════

async fn a_pipe_kit() {
    let rounds = rounds();
    // Pipe is single-slot: producer must wait for consumer to drain before
    // sending again. Use two pipes (req + ack) for safe ping-pong, or just
    // measure sequential send→recv_async round-trips.
    let pipe = Arc::new(PipeAsync::<u64>::new());

    // warmup
    for _ in 0..warmup() {
        for i in 0..BATCH as u64 {
            pipe.send(i);
            pipe.recv_async().await;
        }
    }

    let mut samples = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let t0 = Instant::now();
        for i in 0..BATCH as u64 {
            pipe.send(i);
            pipe.recv_async().await;
        }
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    report("PipeAsync<u64> (kit, same-task ST)", &mut samples);

    // Cross-task variant: producer on spawned task, consumer inline.
    // Use a notify to pace the producer (one-at-a-time).
    let pipe2 = Arc::new(PipeAsync::<u64>::new());
    let done = Arc::new(tokio::sync::Notify::new());

    // warmup
    for _ in 0..warmup() {
        let p = pipe2.clone();
        let d = done.clone();
        let h = tokio::spawn(async move {
            for i in 0..BATCH as u64 {
                d.notified().await; // wait for consumer to signal ready
                p.send(i);
            }
        });
        for _ in 0..BATCH {
            done.notify_one();
            pipe2.recv_async().await;
        }
        h.await.unwrap();
    }

    let mut samples2 = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let p = pipe2.clone();
        let d = done.clone();
        let h = tokio::spawn(async move {
            for i in 0..BATCH as u64 {
                d.notified().await;
                p.send(i);
            }
        });
        let t0 = Instant::now();
        for _ in 0..BATCH {
            done.notify_one();
            pipe2.recv_async().await;
        }
        h.await.unwrap();
        samples2.push(t0.elapsed().as_nanos() as u64);
    }
    report("PipeAsync<u64> (kit, cross-task)", &mut samples2);
}

async fn a_tokio_mpsc() {
    let rounds = rounds();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(1);

    // Same-task sequential for apples-to-apples with Pipe ST.
    for _ in 0..warmup() {
        for i in 0..BATCH as u64 {
            tx.send(i).await.unwrap();
            rx.recv().await.unwrap();
        }
    }
    let mut samples = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let t0 = Instant::now();
        for i in 0..BATCH as u64 {
            tx.send(i).await.unwrap();
            rx.recv().await.unwrap();
        }
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    report("tokio::sync::mpsc(1) (same-task ST)", &mut samples);

    // Cross-task: producer spawned, consumer inline.
    let (tx2, mut rx2) = tokio::sync::mpsc::channel::<u64>(1);
    for _ in 0..warmup() {
        let t = tx2.clone();
        let h = tokio::spawn(async move {
            for i in 0..BATCH as u64 {
                t.send(i).await.unwrap();
            }
        });
        for _ in 0..BATCH {
            rx2.recv().await.unwrap();
        }
        h.await.unwrap();
    }
    let mut samples2 = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let t = tx2.clone();
        let h = tokio::spawn(async move {
            for i in 0..BATCH as u64 {
                t.send(i).await.unwrap();
            }
        });
        let t0 = Instant::now();
        for _ in 0..BATCH {
            rx2.recv().await.unwrap();
        }
        h.await.unwrap();
        samples2.push(t0.elapsed().as_nanos() as u64);
    }
    report("tokio::sync::mpsc(1) (cross-task)", &mut samples2);
}

// ═══════════════════════════════════════════════════════════════════════════
// B. Channel async — RPC round-trip (call_async + serve_one_async)
//
// Kit: ChannelAsync with call_async/serve_one_async (zero-alloc, inline slots)
// Tokio baseline 1: oneshot pair per call (alloc per round-trip)
// Tokio baseline 2: mpsc(1) pair (reused, no per-call alloc)
// ═══════════════════════════════════════════════════════════════════════════

async fn b_channel_kit() {
    let rounds = rounds();
    let ch = Arc::new(ChannelAsync::<u64, u64>::new());

    // warmup — Channel's async methods are plain `async fn` (not boxed),
    // hitting the RPITIT-Send limitation. Use tokio::join! same as tests.
    for _ in 0..warmup() {
        let ch_s = ch.clone();
        let ch_c = ch.clone();
        tokio::join!(
            async {
                for _ in 0..BATCH {
                    ch_s.serve_one_async(|r| r.wrapping_mul(2)).await;
                }
            },
            async {
                for i in 0..BATCH as u64 {
                    ch_c.call_async(i).await;
                }
            }
        );
    }

    let mut samples = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let ch_s = ch.clone();
        let ch_c = ch.clone();
        let t0 = Instant::now();
        tokio::join!(
            async {
                for _ in 0..BATCH {
                    ch_s.serve_one_async(|r| r.wrapping_mul(2)).await;
                }
            },
            async {
                for i in 0..BATCH as u64 {
                    let r = ch_c.call_async(i).await;
                    std::hint::black_box(r);
                }
            }
        );
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    report("ChannelAsync<u64,u64> (kit RPC)", &mut samples);
}

async fn b_tokio_oneshot_pair() {
    let rounds = rounds();

    // warmup
    for _ in 0..warmup() {
        for i in 0..BATCH as u64 {
            let (tx_req, rx_req) = tokio::sync::oneshot::channel::<u64>();
            let (tx_resp, rx_resp) = tokio::sync::oneshot::channel::<u64>();
            tokio::spawn(async move {
                let v = rx_req.await.unwrap();
                tx_resp.send(v.wrapping_mul(2)).unwrap();
            });
            tx_req.send(i).unwrap();
            let _ = rx_resp.await.unwrap();
        }
    }

    let mut samples = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let t0 = Instant::now();
        for i in 0..BATCH as u64 {
            let (tx_req, rx_req) = tokio::sync::oneshot::channel::<u64>();
            let (tx_resp, rx_resp) = tokio::sync::oneshot::channel::<u64>();
            tokio::spawn(async move {
                let v = rx_req.await.unwrap();
                tx_resp.send(v.wrapping_mul(2)).unwrap();
            });
            tx_req.send(i).unwrap();
            let r = rx_resp.await.unwrap();
            std::hint::black_box(r);
        }
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    report("tokio::oneshot pair (RPC)", &mut samples);
}

async fn b_tokio_mpsc_pair() {
    let rounds = rounds();
    let (tx_req, mut rx_req) = tokio::sync::mpsc::channel::<u64>(1);
    let (tx_resp, mut rx_resp) = tokio::sync::mpsc::channel::<u64>(1);

    // Server task shared across warmup + bench
    let server = tokio::spawn(async move {
        while let Some(v) = rx_req.recv().await {
            if tx_resp.send(v.wrapping_mul(2)).await.is_err() {
                break;
            }
        }
    });

    // warmup
    for _ in 0..warmup() {
        for i in 0..BATCH as u64 {
            tx_req.send(i).await.unwrap();
            let _ = rx_resp.recv().await.unwrap();
        }
    }

    let mut samples = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let t0 = Instant::now();
        for i in 0..BATCH as u64 {
            tx_req.send(i).await.unwrap();
            let r = rx_resp.recv().await.unwrap();
            std::hint::black_box(r);
        }
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    report("tokio::mpsc(1) pair (RPC)", &mut samples);

    drop(tx_req);
    let _ = server.await;
}

// ═══════════════════════════════════════════════════════════════════════════
// C. Ring async — SPSC bounded pipeline (throughput)
//
// Kit: Ring<u64, 64, NotifyWaiter> with send_async/recv_async
// Tokio: mpsc(64) — same capacity, same SPSC usage pattern
// ═══════════════════════════════════════════════════════════════════════════

async fn c_ring_kit() {
    let rounds = rounds();
    let ring = Arc::new(Ring::<u64, 64, NotifyWaiter>::new());

    // warmup — Ring's async methods are plain `async fn` → use join!
    for _ in 0..warmup() {
        let r_tx = ring.clone();
        let r_rx = ring.clone();
        tokio::join!(
            async {
                for i in 0..BATCH as u64 {
                    r_tx.send_async(i).await;
                }
            },
            async {
                for _ in 0..BATCH {
                    r_rx.recv_async().await;
                }
            }
        );
    }

    let mut samples = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let r_tx = ring.clone();
        let r_rx = ring.clone();
        let t0 = Instant::now();
        tokio::join!(
            async {
                for i in 0..BATCH as u64 {
                    r_tx.send_async(i).await;
                }
            },
            async {
                for _ in 0..BATCH {
                    r_rx.recv_async().await;
                }
            }
        );
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    report("Ring<u64,64,Notify> (kit)", &mut samples);
}

async fn c_tokio_mpsc_64() {
    let rounds = rounds();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(64);

    // warmup
    for _ in 0..warmup() {
        let tx2 = tx.clone();
        let h = tokio::spawn(async move {
            for i in 0..BATCH as u64 {
                tx2.send(i).await.unwrap();
            }
        });
        for _ in 0..BATCH {
            rx.recv().await.unwrap();
        }
        h.await.unwrap();
    }

    let mut samples = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let tx2 = tx.clone();
        let h = tokio::spawn(async move {
            for i in 0..BATCH as u64 {
                tx2.send(i).await.unwrap();
            }
        });
        let t0 = Instant::now();
        for _ in 0..BATCH {
            rx.recv().await.unwrap();
        }
        h.await.unwrap();
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    report("tokio::sync::mpsc(64)", &mut samples);
}

// ═══════════════════════════════════════════════════════════════════════════
// D. Mpsc async — M:1 fan-in (4 producers → 1 consumer)
//
// Kit: MpscAsync<u64, 256> — producers use try_send + yield loop,
//      consumer uses recv_async. Fresh instance per round (producers
//      are moved into tasks).
// Tokio: mpsc(256) with 4 senders.
// ═══════════════════════════════════════════════════════════════════════════

async fn d_mpsc_kit() {
    let rounds = rounds();
    const M: usize = 4;
    const PER_PRODUCER: usize = BATCH / M; // 250

    let mut samples = Vec::with_capacity(rounds);
    let total_rounds = warmup() + rounds;

    for round in 0..total_rounds {
        let (producers, mut consumer, shutdown) = MpscAsync::<u64, 256>::new(M);

        let t0 = Instant::now();

        // Move each producer into its own task.
        let prod_handles: Vec<_> = producers
            .into_iter()
            .map(|p| {
                tokio::spawn(async move {
                    for k in 0..PER_PRODUCER as u64 {
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
            })
            .collect();

        // Consumer runs inline.
        let mut count = 0;
        while count < BATCH {
            match consumer.recv_async().await {
                Ok(_) => count += 1,
                Err(_) => break,
            }
        }
        for h in prod_handles {
            h.await.unwrap();
        }

        if round >= warmup() {
            samples.push(t0.elapsed().as_nanos() as u64);
        }
        shutdown.signal();
    }
    report("MpscAsync<u64,256> 4P/1C (kit)", &mut samples);
}

async fn d_tokio_mpsc() {
    let rounds = rounds();
    const M: usize = 4;
    const PER_PRODUCER: usize = BATCH / M;

    let mut samples = Vec::with_capacity(rounds);
    let total_rounds = warmup() + rounds;

    for round in 0..total_rounds {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(256);

        let t0 = Instant::now();
        let handles: Vec<_> = (0..M)
            .map(|_| {
                let tx2 = tx.clone();
                tokio::spawn(async move {
                    for k in 0..PER_PRODUCER as u64 {
                        tx2.send(k).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx); // drop the original sender so rx can detect end

        let mut count = 0;
        while count < BATCH {
            if rx.recv().await.is_some() {
                count += 1;
            } else {
                break;
            }
        }
        for h in handles {
            h.await.unwrap();
        }

        if round >= warmup() {
            samples.push(t0.elapsed().as_nanos() as u64);
        }
    }
    report("tokio::sync::mpsc(256) 4P/1C", &mut samples);
}

// ═══════════════════════════════════════════════════════════════════════════
// E. Mpmc async — M:N (4 producers → 2 consumers)
//
// Kit: MpmcAsync<u64, 64> — 4 producers send_async, 2 consumers recv_async.
//      Fresh instance per round.
// Tokio: no native M:N. Use 4 producers → shared mpsc(64) → 1 consumer
//        that dispatches to 2 worker channels (closest real-world pattern).
//        For a fairer shape-equivalent: N separate mpsc channels, producers
//        round-robin.
// ═══════════════════════════════════════════════════════════════════════════

async fn e_mpmc_kit() {
    let rounds = rounds();
    const M: usize = 4;
    const N: usize = 2;
    const PER_PRODUCER: usize = BATCH / M; // 250

    let mut samples = Vec::with_capacity(rounds);
    let total_rounds = warmup() + rounds;

    for round in 0..total_rounds {
        let (mut producers, mut consumers, shutdown) = MpmcAsync::<u64, 64>::new(M, N);

        let total_recv = Arc::new(AtomicUsize::new(0));

        let t0 = Instant::now();

        // MpmcProducer/Consumer are !Sync, so their async methods produce
        // !Send futures. We use tokio::join! to drive them all on the
        // current task (same pattern as the crate's unit tests).
        let p0 = producers.remove(0);
        let p1 = producers.remove(0);
        let p2 = producers.remove(0);
        let p3 = producers.remove(0);
        let c0 = consumers.remove(0);
        let c1 = consumers.remove(0);

        let total0 = total_recv.clone();
        let total1 = total_recv.clone();
        let total_sd = total_recv.clone();

        tokio::join!(
            async move {
                for k in 0..PER_PRODUCER as u64 {
                    p0.send_async(k).await;
                }
            },
            async move {
                for k in 0..PER_PRODUCER as u64 {
                    p1.send_async(k).await;
                }
            },
            async move {
                for k in 0..PER_PRODUCER as u64 {
                    p2.send_async(k).await;
                }
            },
            async move {
                for k in 0..PER_PRODUCER as u64 {
                    p3.send_async(k).await;
                }
            },
            async move {
                loop {
                    if total0.load(Ordering::Relaxed) >= BATCH {
                        break;
                    }
                    match c0.recv_async().await {
                        Ok(_) => {
                            total0.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => break,
                    }
                }
            },
            async move {
                loop {
                    if total1.load(Ordering::Relaxed) >= BATCH {
                        break;
                    }
                    match c1.recv_async().await {
                        Ok(_) => {
                            total1.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => break,
                    }
                }
            },
            async move {
                // Wait until all consumed, then signal shutdown.
                while total_sd.load(Ordering::Relaxed) < BATCH {
                    tokio::task::yield_now().await;
                }
                shutdown.signal();
            }
        );

        if round >= warmup() {
            samples.push(t0.elapsed().as_nanos() as u64);
        }
    }
    report("MpmcAsync<u64,64> 4P/2C (kit)", &mut samples);
}

async fn e_tokio_mpsc_fanout() {
    let rounds = rounds();
    const M: usize = 4;
    const N: usize = 2;
    const PER_PRODUCER: usize = BATCH / M;

    // Tokio doesn't have a native M:N channel. The closest fair comparison:
    // N separate mpsc(64) channels, producers round-robin across them,
    // N consumers each drain their own channel.
    let mut samples = Vec::with_capacity(rounds);
    let total_rounds = warmup() + rounds;

    for round in 0..total_rounds {
        let mut senders = Vec::with_capacity(N);
        let mut receivers = Vec::with_capacity(N);
        for _ in 0..N {
            let (tx, rx) = tokio::sync::mpsc::channel::<u64>(64);
            senders.push(tx);
            receivers.push(rx);
        }

        let total_recv = Arc::new(AtomicUsize::new(0));
        let t0 = Instant::now();

        // Producers round-robin across N channels.
        let prod_handles: Vec<_> = (0..M)
            .map(|p_idx| {
                let senders = senders.clone();
                tokio::spawn(async move {
                    for k in 0..PER_PRODUCER as u64 {
                        let target = (p_idx + k as usize) % N;
                        senders[target].send(k).await.unwrap();
                    }
                })
            })
            .collect();

        // N consumers.
        let cons_handles: Vec<_> = receivers
            .into_iter()
            .map(|mut rx| {
                let total = total_recv.clone();
                tokio::spawn(async move {
                    loop {
                        if total.load(Ordering::Relaxed) >= BATCH {
                            break;
                        }
                        match rx.recv().await {
                            Some(_) => {
                                total.fetch_add(1, Ordering::Relaxed);
                            }
                            None => break,
                        }
                    }
                })
            })
            .collect();

        for h in prod_handles {
            h.await.unwrap();
        }
        drop(senders); // signal EOF
        while total_recv.load(Ordering::Relaxed) < BATCH {
            tokio::task::yield_now().await;
        }
        for h in cons_handles {
            h.await.unwrap();
        }

        if round >= warmup() {
            samples.push(t0.elapsed().as_nanos() as u64);
        }
    }
    report("tokio::mpsc(64)×2 4P/2C", &mut samples);
}

// ═══════════════════════════════════════════════════════════════════════════
// Driver
// ═══════════════════════════════════════════════════════════════════════════

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    println!("=== arbitro-kit async head-to-head ===");
    println!(
        "rounds={} (+ {} warmup), batch={}",
        rounds(),
        warmup(),
        BATCH
    );

    rt.block_on(async {
        header("A. Pipe async — single-slot 1:1 (send + recv_async)");
        a_pipe_kit().await;
        a_tokio_mpsc().await;

        header("B. Channel async — RPC round-trip (call_async + serve_one_async)");
        b_channel_kit().await;
        b_tokio_oneshot_pair().await;
        b_tokio_mpsc_pair().await;

        header("C. Ring async — SPSC bounded pipeline (CAP=64, throughput)");
        c_ring_kit().await;
        c_tokio_mpsc_64().await;

        header("D. Mpsc async — M:1 fan-in (4 producers, CAP=256)");
        d_mpsc_kit().await;
        d_tokio_mpsc().await;

        header("E. Mpmc async — M:N (4P/2C, CAP=64)");
        e_mpmc_kit().await;
        e_tokio_mpsc_fanout().await;
    });

    println!("\nDone.");
}
