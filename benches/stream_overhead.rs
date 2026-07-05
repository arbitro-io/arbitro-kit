//! `Stream<T>` overhead bench.
//!
//! Measures the new unbounded sequenced log primitive in isolation:
//! - Single-thread send+drain (raw cursor cost).
//! - Cross-thread one-way (send pipelined with consumer recv).
//! - Cross-thread lockstep RPC via two Streams (no Nexo helper yet).
//! - Ack-RTT — send returns Receipt, producer waits cursor cross.
//!
//! Reference rows for `Ring<u64, CAP>` are included where the
//! topology matches, so the new primitive can be sanity-checked
//! against the existing one.

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::stream::Ring;
use arbitro_kit::stream::{BufferedSender, Stream};

const N_MSGS: u64 = 10_000;
const ROUNDS: usize = 30;
const WARMUP: usize = 10;

fn pct(samples: &mut Vec<f64>, p: f64) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples[idx.min(samples.len() - 1)]
}

fn collect<F: FnMut() -> f64>(mut f: F) -> (f64, f64) {
    for _ in 0..WARMUP {
        let _ = f();
    }
    let mut samples: Vec<f64> = (0..ROUNDS).map(|_| f()).collect();
    let min = samples.iter().cloned().fold(f64::INFINITY, f64::min);
    let p50 = pct(&mut samples, 0.50);
    (min, p50)
}

fn row(name: &str, min: f64, p50: f64) {
    println!(
        "{:<40} {:>10.1} {:>10.1} {:>14.0}",
        name,
        min,
        p50,
        1e9 / min
    );
}

// ─── A. Single thread (no cross-thread sync) ──────────────────────────────
fn st_stream_send_recv() -> f64 {
    let s: Stream<u64> = Stream::new();
    let t0 = Instant::now();
    for i in 0..N_MSGS as u64 {
        s.send(i);
        let _ = s.try_recv();
    }
    t0.elapsed().as_nanos() as f64 / N_MSGS as f64
}

fn st_stream_send_iter_drain(batch: usize) -> f64 {
    let s: Stream<u64> = Stream::new();
    let mut buf: Vec<u64> = Vec::with_capacity(batch);
    let t0 = Instant::now();
    let mut sent = 0u64;
    while sent < N_MSGS {
        let take = (N_MSGS - sent).min(batch as u64);
        s.send_iter(sent..sent + take);
        sent += take;
        buf.clear();
        s.recv_bulk(&mut buf, batch);
    }
    t0.elapsed().as_nanos() as f64 / N_MSGS as f64
}

// ─── B. Cross-thread one-way ──────────────────────────────────────────────
fn xt_stream_oneway() -> f64 {
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let s2 = s.clone();
    let consumer = thread::spawn(move || {
        s2.set_consumer(thread::current());
        let mut sum = 0u64;
        for _ in 0..N_MSGS {
            sum = sum.wrapping_add(s2.recv());
        }
        sum
    });
    let t0 = Instant::now();
    for i in 0..N_MSGS as u64 {
        s.send(i);
    }
    let _ = consumer.join().unwrap();
    t0.elapsed().as_nanos() as f64 / N_MSGS as f64
}

fn xt_stream_oneway_iter(batch: usize) -> f64 {
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let s2 = s.clone();
    let consumer = thread::spawn(move || {
        s2.set_consumer(thread::current());
        let mut sum = 0u64;
        for _ in 0..N_MSGS {
            sum = sum.wrapping_add(s2.recv());
        }
        sum
    });
    let t0 = Instant::now();
    let mut sent = 0u64;
    while sent < N_MSGS {
        let take = (N_MSGS - sent).min(batch as u64);
        s.send_iter(sent..sent + take);
        sent += take;
    }
    let _ = consumer.join().unwrap();
    t0.elapsed().as_nanos() as f64 / N_MSGS as f64
}

fn xt_ring_oneway<const CAP: usize>() -> f64 {
    let r: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let r2 = r.clone();
    let consumer = thread::spawn(move || {
        r2.set_consumer(thread::current());
        let mut sum = 0u64;
        for _ in 0..N_MSGS {
            sum = sum.wrapping_add(r2.recv());
        }
        sum
    });
    r.set_producer(thread::current());
    let t0 = Instant::now();
    for i in 0..N_MSGS as u64 {
        r.send(i);
    }
    let _ = consumer.join().unwrap();
    t0.elapsed().as_nanos() as f64 / N_MSGS as f64
}

// ─── B'. Cross-thread one-way via BufferedSender (single-send API) ───────
fn xt_stream_buffered_oneway(threshold: usize) -> f64 {
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let s2 = s.clone();
    let consumer = thread::spawn(move || {
        s2.set_consumer(thread::current());
        let mut sum = 0u64;
        for _ in 0..N_MSGS {
            sum = sum.wrapping_add(s2.recv());
        }
        sum
    });
    let mut tx = s.buffered(threshold);
    let t0 = Instant::now();
    for i in 0..N_MSGS as u64 {
        tx.send(i);
    }
    drop(tx); // RAII flush of residue
    let _ = consumer.join().unwrap();
    t0.elapsed().as_nanos() as f64 / N_MSGS as f64
}

// ─── C. Cross-thread lockstep RPC (two streams) ───────────────────────────
fn xt_stream_lockstep() -> f64 {
    let req: Arc<Stream<u64>> = Arc::new(Stream::new());
    let resp: Arc<Stream<u64>> = Arc::new(Stream::new());
    let req_w = req.clone();
    let resp_w = resp.clone();
    let worker = thread::spawn(move || {
        req_w.set_consumer(thread::current());
        for _ in 0..N_MSGS {
            let v = req_w.recv();
            resp_w.send(v.wrapping_mul(2) | 1);
        }
    });
    resp.set_consumer(thread::current());
    let t0 = Instant::now();
    for i in 0..N_MSGS as u64 {
        req.send(i);
        let _ = resp.recv();
    }
    let ns = t0.elapsed().as_nanos() as f64;
    worker.join().unwrap();
    ns / N_MSGS as f64
}

// ─── D. Ack-RTT (Receipt::wait_delivered, no reply payload) ───────────────
fn xt_stream_ack_rtt_per_msg() -> f64 {
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let s2 = s.clone();
    let consumer = thread::spawn(move || {
        s2.set_consumer(thread::current());
        for _ in 0..N_MSGS {
            let _ = s2.recv();
        }
    });
    let t0 = Instant::now();
    for i in 0..N_MSGS as u64 {
        let r = s.send(i);
        r.wait_delivered(&s);
    }
    let ns = t0.elapsed().as_nanos() as f64;
    consumer.join().unwrap();
    ns / N_MSGS as f64
}

fn xt_stream_ack_rtt_batched(batch: usize) -> f64 {
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let s2 = s.clone();
    let consumer = thread::spawn(move || {
        s2.set_consumer(thread::current());
        let mut buf: Vec<u64> = Vec::with_capacity(batch);
        let mut total = 0u64;
        while total < N_MSGS {
            buf.clear();
            buf.push(s2.recv());
            s2.recv_bulk(&mut buf, batch - 1);
            total += buf.len() as u64;
        }
    });
    let t0 = Instant::now();
    let mut sent = 0u64;
    while sent < N_MSGS {
        let take = (N_MSGS - sent).min(batch as u64);
        let r = s.send_iter(sent..sent + take).unwrap();
        r.wait_delivered(&s);
        sent += take;
    }
    let ns = t0.elapsed().as_nanos() as f64;
    consumer.join().unwrap();
    ns / N_MSGS as f64
}

fn main() {
    println!("=== Stream<T> overhead bench ===");
    println!(
        "{} msgs per measurement; best-of-{} (after {} warmup).",
        N_MSGS, ROUNDS, WARMUP
    );
    println!("Payload: u64. Stream is unbounded; Ring rows for reference.\n");

    println!(
        "{:<40} {:>10} {:>10} {:>14}",
        "scenario", "min ns/op", "p50 ns/op", "ops/sec (min)"
    );
    println!("{}", "─".repeat(80));

    println!("\n── A. Single thread ──");
    let (m, p) = collect(st_stream_send_recv);
    row("Stream send + try_recv", m, p);
    let (m, p) = collect(|| st_stream_send_iter_drain(64));
    row("Stream send_iter K=64 + recv_bulk", m, p);
    let (m, p) = collect(|| st_stream_send_iter_drain(256));
    row("Stream send_iter K=256 + recv_bulk", m, p);

    println!("\n── B. Cross-thread one-way ──");
    let (m, p) = collect(xt_stream_oneway);
    row("Stream send + recv (per-item)", m, p);
    let (m, p) = collect(|| xt_stream_oneway_iter(64));
    row("Stream send_iter K=64 + recv", m, p);
    let (m, p) = collect(|| xt_stream_oneway_iter(256));
    row("Stream send_iter K=256 + recv", m, p);
    let (m, p) = collect(xt_ring_oneway::<256>);
    row("Ring<u64, 256> send + recv (ref)", m, p);
    let (m, p) = collect(xt_ring_oneway::<1024>);
    row("Ring<u64, 1024> send + recv (ref)", m, p);

    println!("\n── B'. Cross-thread one-way via BufferedSender (single-send API) ──");
    for &k in &[8usize, 32, 64, 128, 256] {
        let (m, p) = collect(|| xt_stream_buffered_oneway(k));
        row(&format!("BufferedSender K={}", k), m, p);
    }

    println!("\n── C. Cross-thread lockstep RPC (2 Streams) ──");
    let (m, p) = collect(xt_stream_lockstep);
    row("Stream lockstep RPC (2 streams)", m, p);

    println!("\n── D. Ack-RTT (Receipt::wait_delivered) ──");
    let (m, p) = collect(xt_stream_ack_rtt_per_msg);
    row("Stream ack-RTT per-msg", m, p);
    for &k in &[8usize, 32, 128, 512] {
        let (m, p) = collect(|| xt_stream_ack_rtt_batched(k));
        row(&format!("Stream ack-RTT batched K={}", k), m, p);
    }

    println!("\nDone.");
}
