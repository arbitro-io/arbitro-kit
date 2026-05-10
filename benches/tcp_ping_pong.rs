//! tcp_ping_pong — sync (kit, fully OS threads) vs async (tokio, fully tasks)
//! over real TCP, with a 3-stage client-side pipeline per variant.
//!
//! ## Topology (both variants)
//!
//! ```text
//!  load_gen → [mpsc<Bytes>] → writer → [TCP] → echo → [TCP] → reader → [ack] → load_gen
//!     ^                                                                          |
//!     └────────────────── 1 ping/pong round-trip ───────────────────────────────┘
//! ```
//!
//! ## Variant A — tokio
//!
//! - load_gen / writer / reader : tokio tasks (multi-thread runtime, 4 workers)
//! - forward mpsc               : `tokio::sync::mpsc::channel<Bytes>(64)`
//! - ack channel                : `tokio::sync::mpsc::channel<u64>(64)`
//! - TCP                        : `tokio::net::TcpStream`
//! - echo server                : tokio task (read frame → write ack)
//!
//! ## Variant B — kit sync (all OS threads, no tokio in the path)
//!
//! - load_gen / writer / reader : `std::thread::spawn` (3 dedicated OS threads)
//! - forward mpsc               : `kit::Mpsc<Bytes, 64, ParkWaiter>::new(1)`
//! - ack channel                : `kit::Pipe<u64, _, ParkWaiter>` (reusable single-slot)
//! - TCP                        : `std::net::TcpStream` (blocking I/O)
//! - echo server                : `std::thread::spawn` (blocking read+write loop)
//!
//! ## Measurement
//!
//! Serial ping/pong: one frame in flight at a time, load_gen waits for the
//! ack before sending the next. This isolates the round-trip latency —
//! throughput numbers fall directly out of `1 / mean_ns`.
//!
//! Each round = `BATCH` ping/pongs back-to-back, timed end-to-end on the
//! load_gen thread. Reports mean / p50 / p99 ns per ping/pong.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_kit::route::Mpsc;
use arbitro_kit::slot::Pipe;
use arbitro_kit::waiter::ParkWaiter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ── Settings ──────────────────────────────────────────────────────────────

const FRAME_SIZE_DEFAULT: usize = 128;
const ACK_SIZE: usize = 8;
const BATCH_DEFAULT: usize = 1000;
const ROUNDS_DEFAULT: usize = 100;
const WARMUP: usize = 100;

fn env_usize(k: &str, default: usize) -> usize {
    std::env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

// ── Report ────────────────────────────────────────────────────────────────

fn header() {
    println!(
        "\n{:<48}  {:>10}  {:>10}  {:>10}  {:>14}",
        "variant", "mean_ns/op", "p50_ns/op", "p99_ns/op", "ops/sec"
    );
    println!("{}", "─".repeat(48 + 2 + 10 + 2 + 10 + 2 + 10 + 2 + 14));
}

fn report(label: &str, batch_ns: &mut [u128], batch: usize) {
    batch_ns.sort_unstable();
    let n = batch_ns.len();
    let total_ops = (n as u128) * (batch as u128);
    let total_ns: u128 = batch_ns.iter().sum();
    let mean = (total_ns as f64) / (total_ops as f64);
    let p50 = (batch_ns[n / 2] as f64) / (batch as f64);
    let p99 = (batch_ns[n * 99 / 100] as f64) / (batch as f64);
    let ops = 1e9 / mean;
    println!(
        "{:<48}  {:>10.2}  {:>10.2}  {:>10.2}  {:>14.0}",
        label, mean, p50, p99, ops
    );
}

// ── Echo servers ──────────────────────────────────────────────────────────

fn spawn_sync_echo_server(frame_size: usize) -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::Builder::new()
        .name("echo-sync".into())
        .spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream.set_nodelay(true).unwrap();
            let mut frame_buf = vec![0u8; frame_size];
            loop {
                if stream.read_exact(&mut frame_buf).is_err() {
                    break;
                }
                // Echo back only the first 8 bytes (seq) as ack.
                if stream.write_all(&frame_buf[..ACK_SIZE]).is_err() {
                    break;
                }
            }
        })
        .unwrap();
    addr
}

async fn spawn_async_echo_server(frame_size: usize) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        stream.set_nodelay(true).unwrap();
        let mut frame_buf = vec![0u8; frame_size];
        loop {
            if stream.read_exact(&mut frame_buf).await.is_err() {
                break;
            }
            if stream.write_all(&frame_buf[..ACK_SIZE]).await.is_err() {
                break;
            }
        }
    });
    addr
}

// ── Variant A — tokio + tokio::mpsc + tokio TCP ───────────────────────────

fn bench_tokio(rounds: usize, batch: usize, frame_size: usize) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut samples: Vec<u128> = Vec::with_capacity(rounds);

    rt.block_on(async {
        let addr = spawn_async_echo_server(frame_size).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (mut read_half, mut write_half) = stream.into_split();

        // Forward channel: load_gen → writer
        let (frame_tx, mut frame_rx) =
            tokio::sync::mpsc::channel::<Vec<u8>>(64);
        // Ack channel: reader → load_gen
        let (ack_tx, mut ack_rx) = tokio::sync::mpsc::channel::<u64>(64);

        // Writer task
        let writer = tokio::spawn(async move {
            while let Some(frame) = frame_rx.recv().await {
                if write_half.write_all(&frame).await.is_err() {
                    break;
                }
            }
        });

        // Reader task
        let reader = tokio::spawn(async move {
            let mut buf = [0u8; ACK_SIZE];
            loop {
                if read_half.read_exact(&mut buf).await.is_err() {
                    break;
                }
                let seq = u64::from_le_bytes(buf);
                if ack_tx.send(seq).await.is_err() {
                    break;
                }
            }
        });

        let mut frame_template = vec![0u8; frame_size];

        // Warmup
        for seq in 0..WARMUP as u64 {
            frame_template[..8].copy_from_slice(&seq.to_le_bytes());
            frame_tx.send(frame_template.clone()).await.unwrap();
            let _ = ack_rx.recv().await.unwrap();
        }

        // Measurement
        for _ in 0..rounds {
            let t0 = Instant::now();
            for seq in 0..batch as u64 {
                frame_template[..8].copy_from_slice(&seq.to_le_bytes());
                frame_tx.send(frame_template.clone()).await.unwrap();
                let _ = ack_rx.recv().await.unwrap();
            }
            samples.push(t0.elapsed().as_nanos());
        }

        // Shutdown: drop the senders so writer/reader unblock and exit.
        drop(frame_tx);
        let _ = writer.await;
        let _ = reader.await;
    });

    report("A. tokio + tokio::mpsc + tokio TCP", &mut samples, batch);
}

// ── Variant B — kit + kit::Mpsc + std::net (all sync, 3 OS threads) ───────

fn bench_kit(rounds: usize, batch: usize, frame_size: usize) {
    let addr = spawn_sync_echo_server(frame_size);
    std::thread::sleep(Duration::from_millis(50));

    let write_stream = StdTcpStream::connect(addr).unwrap();
    write_stream.set_nodelay(true).unwrap();
    let read_stream = write_stream.try_clone().unwrap();
    // Keep one more clone in main for explicit shutdown — try_clone'd
    // descriptors don't share close semantics, so dropping the two half-
    // owners leaks the socket and reader/writer hang on the syscall.
    let shutdown_stream = write_stream.try_clone().unwrap();

    // Forward: kit::Mpsc<Vec<u8>>::new(1) — load_gen is the single producer.
    let (mut producers, consumer, sd) = Mpsc::<Vec<u8>, 64, ParkWaiter>::new(1);
    let producer = producers.remove(0);

    // Ack: kit::Pipe<u64, _, ParkWaiter> — reader→load_gen, reusable.
    let ack_pipe: Arc<Pipe<u64>> = Arc::new(Pipe::new());

    let stop = Arc::new(AtomicBool::new(false));

    // Writer thread
    let stop_w = stop.clone();
    let writer = std::thread::Builder::new()
        .name("writer-kit".into())
        .spawn(move || {
            consumer.bind();
            let mut stream = write_stream;
            loop {
                match consumer.recv() {
                    Ok(frame) => {
                        if stream.write_all(&frame).is_err() {
                            break;
                        }
                    }
                    Err(_) => break, // Shutdown
                }
                if stop_w.load(Ordering::Relaxed) {
                    break;
                }
            }
        })
        .unwrap();

    // Reader thread
    let ack_pipe_w = ack_pipe.clone();
    let stop_r = stop.clone();
    let reader = std::thread::Builder::new()
        .name("reader-kit".into())
        .spawn(move || {
            let mut stream = read_stream;
            let mut buf = [0u8; ACK_SIZE];
            loop {
                if stream.read_exact(&mut buf).is_err() {
                    break;
                }
                let seq = u64::from_le_bytes(buf);
                ack_pipe_w.send(seq);
                if stop_r.load(Ordering::Relaxed) {
                    break;
                }
            }
        })
        .unwrap();

    // load_gen runs on the current thread.
    producer.bind();
    ack_pipe.set_consumer(std::thread::current());

    let mut frame_template = vec![0u8; frame_size];

    // Warmup
    for seq in 0..WARMUP as u64 {
        frame_template[..8].copy_from_slice(&seq.to_le_bytes());
        producer.send(frame_template.clone());
        let _ = ack_pipe.recv();
    }

    // Measurement
    let mut samples: Vec<u128> = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let t0 = Instant::now();
        for seq in 0..batch as u64 {
            frame_template[..8].copy_from_slice(&seq.to_le_bytes());
            producer.send(frame_template.clone());
            let _ = ack_pipe.recv();
        }
        samples.push(t0.elapsed().as_nanos());
    }

    // Shutdown: explicit TCP close so reader/writer unblock from their
    // blocking syscalls; signal kit consumer so writer wakes from recv().
    stop.store(true, Ordering::Relaxed);
    let _ = shutdown_stream.shutdown(std::net::Shutdown::Both);
    sd.signal();
    drop(producer);
    let _ = writer.join();
    let _ = reader.join();

    report("B. kit + kit::Mpsc + std::net TCP", &mut samples, batch);
}

// ── Main ──────────────────────────────────────────────────────────────────

fn main() {
    let rounds = env_usize("BENCH_ROUNDS", ROUNDS_DEFAULT);
    let batch = env_usize("BENCH_BATCH", BATCH_DEFAULT);
    let frame_size = env_usize("BENCH_FRAME", FRAME_SIZE_DEFAULT);

    println!(
        "tcp_ping_pong — frame={frame_size}B, batch={batch}/round, rounds={rounds}, warmup={WARMUP}"
    );

    header();
    bench_kit(rounds, batch, frame_size);
    bench_tokio(rounds, batch, frame_size);

    println!("\nDone.");
}
