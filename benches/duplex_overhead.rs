//! `Duplex<A, B>` overhead bench.
//!
//! Goals:
//! - Confirm `Duplex` has zero overhead vs two raw `Stream`s.
//! - Measure the bidirectional round-trip patterns it enables.
//! - Quantify `is_delivered` poll cost (used after fire-and-forget).
//! - Quantify `wait_delivered` cost (currently busy-spin).
//!
//! Apples-to-apples with `stream_overhead.rs`: same harness, same
//! payload (u64), same N_MSGS / ROUNDS.

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::stream::{Duplex, Stream};

const N_MSGS: u64 = 10_000;
const ROUNDS: usize = 30;
const WARMUP: usize = 10;

fn pct(samples: &mut Vec<f64>, p: f64) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples[idx.min(samples.len() - 1)]
}

fn collect<F: FnMut() -> f64>(mut f: F) -> (f64, f64) {
    for _ in 0..WARMUP { let _ = f(); }
    let mut samples: Vec<f64> = (0..ROUNDS).map(|_| f()).collect();
    let min = samples.iter().cloned().fold(f64::INFINITY, f64::min);
    let p50 = pct(&mut samples, 0.50);
    (min, p50)
}

fn row(name: &str, min: f64, p50: f64) {
    println!("{:<48} {:>10.1} {:>10.1} {:>14.0}",
             name, min, p50, 1e9 / min);
}

// ─── A. One-way reference: Duplex vs two raw Streams ──────────────────────
//
// Use one direction only; the other end's outbound is unused. Confirms
// Duplex's per-side cost equals raw Stream's cost.

fn xt_duplex_oneway() -> f64 {
    let (a, b) = Duplex::<u64, ()>::pair();
    let handle = thread::spawn(move || {
        b.set_consumer(thread::current());
        let mut sum = 0u64;
        for _ in 0..N_MSGS { sum = sum.wrapping_add(b.recv()); }
        sum
    });
    let t0 = Instant::now();
    for i in 0..N_MSGS as u64 { a.send(i); }
    let _ = handle.join().unwrap();
    t0.elapsed().as_nanos() as f64 / N_MSGS as f64
}

fn xt_stream_oneway_ref() -> f64 {
    let s: Arc<Stream<u64>> = Arc::new(Stream::new());
    let s2 = s.clone();
    let handle = thread::spawn(move || {
        s2.set_consumer(thread::current());
        let mut sum = 0u64;
        for _ in 0..N_MSGS { sum = sum.wrapping_add(s2.recv()); }
        sum
    });
    let t0 = Instant::now();
    for i in 0..N_MSGS as u64 { s.send(i); }
    let _ = handle.join().unwrap();
    t0.elapsed().as_nanos() as f64 / N_MSGS as f64
}

// ─── B. Round-trip patterns ──────────────────────────────────────────────

/// Lockstep RPC: a sends, waits for b's reply, repeat.
fn xt_duplex_rpc_lockstep() -> f64 {
    let (a, b) = Duplex::<u64, u64>::pair();
    let worker = thread::spawn(move || {
        b.set_consumer(thread::current());
        for _ in 0..N_MSGS {
            let v = b.recv();
            b.send(v.wrapping_mul(2) | 1);
        }
    });
    a.set_consumer(thread::current());
    let t0 = Instant::now();
    for i in 0..N_MSGS as u64 {
        a.send(i);
        let _ = a.recv();
    }
    let ns = t0.elapsed().as_nanos() as f64;
    worker.join().unwrap();
    ns / N_MSGS as f64
}

/// Batched RPC: a sends K, drains K replies, repeat.
fn xt_duplex_rpc_batched(batch: usize) -> f64 {
    let (a, b) = Duplex::<u64, u64>::pair();
    let worker = thread::spawn(move || {
        b.set_consumer(thread::current());
        let mut buf: Vec<u64> = Vec::with_capacity(batch);
        let mut total = 0u64;
        while total < N_MSGS {
            buf.clear();
            buf.push(b.recv());
            b.recv_bulk(&mut buf, batch - 1);
            let n = buf.len() as u64;
            for v in buf.iter_mut() { *v = v.wrapping_mul(2) | 1; }
            let _ = b.send_iter(buf.drain(..));
            total += n;
        }
    });
    a.set_consumer(thread::current());
    let t0 = Instant::now();
    let mut sent = 0u64;
    let mut received = 0u64;
    let mut send_buf: Vec<u64> = Vec::with_capacity(batch);
    let mut recv_buf: Vec<u64> = Vec::with_capacity(batch);
    while received < N_MSGS {
        while sent < N_MSGS && send_buf.len() < batch {
            send_buf.push(sent);
            sent += 1;
        }
        let _ = a.send_iter(send_buf.drain(..));
        recv_buf.clear();
        recv_buf.push(a.recv());
        a.recv_bulk(&mut recv_buf, batch - 1);
        received += recv_buf.len() as u64;
    }
    let ns = t0.elapsed().as_nanos() as f64;
    worker.join().unwrap();
    ns / N_MSGS as f64
}

// ─── C. Fire-and-forget + is_delivered poll cost ─────────────────────────
//
// Producer fires N msgs, keeps last receipt, polls is_delivered until true.
// Measures the cost of: send + (cheap) is_delivered loop + cursor advance.

fn xt_duplex_fire_and_poll() -> f64 {
    let (a, b) = Duplex::<u64, ()>::pair();
    let handle = thread::spawn(move || {
        b.set_consumer(thread::current());
        for _ in 0..N_MSGS { let _ = b.recv(); }
    });
    let t0 = Instant::now();
    let mut last_r = a.send(0);
    for i in 1..N_MSGS as u64 { last_r = a.send(i); }
    while !a.is_delivered(last_r) {
        std::hint::spin_loop();
    }
    let ns = t0.elapsed().as_nanos() as f64;
    handle.join().unwrap();
    ns / N_MSGS as f64
}

// ─── D. Wait_delivered cost (busy-spin in MVP) ───────────────────────────

fn xt_duplex_send_wait_each() -> f64 {
    let (a, b) = Duplex::<u64, ()>::pair();
    let handle = thread::spawn(move || {
        b.set_consumer(thread::current());
        for _ in 0..N_MSGS { let _ = b.recv(); }
    });
    let t0 = Instant::now();
    for i in 0..N_MSGS as u64 {
        let r = a.send(i);
        a.wait_delivered(r);   // strict per-message verification
    }
    let ns = t0.elapsed().as_nanos() as f64;
    handle.join().unwrap();
    ns / N_MSGS as f64
}

fn xt_duplex_send_iter_wait_batched(batch: usize) -> f64 {
    let (a, b) = Duplex::<u64, ()>::pair();
    let handle = thread::spawn(move || {
        b.set_consumer(thread::current());
        for _ in 0..N_MSGS { let _ = b.recv(); }
    });
    let t0 = Instant::now();
    let mut sent = 0u64;
    while sent < N_MSGS {
        let take = (N_MSGS - sent).min(batch as u64);
        let r = a.send_iter(sent..sent + take).unwrap();
        a.wait_delivered(r);
        sent += take;
    }
    let ns = t0.elapsed().as_nanos() as f64;
    handle.join().unwrap();
    ns / N_MSGS as f64
}

fn main() {
    println!("=== Duplex<A, B> overhead bench ===");
    println!("{} msgs per measurement; best-of-{} (after {} warmup).",
             N_MSGS, ROUNDS, WARMUP);
    println!("Payload: u64.\n");

    println!("{:<48} {:>10} {:>10} {:>14}",
             "scenario", "min ns", "p50 ns", "ops/sec (min)");
    println!("{}", "─".repeat(86));

    println!("\n── A. One-way: Duplex vs raw Stream (zero-overhead check) ──");
    let (m, p) = collect(xt_duplex_oneway);
    row("Duplex one-way (a → b only)",                m, p);
    let (m, p) = collect(xt_stream_oneway_ref);
    row("Stream one-way (raw reference)",             m, p);

    println!("\n── B. Bidirectional RPC patterns ──");
    let (m, p) = collect(xt_duplex_rpc_lockstep);
    row("Duplex RPC lockstep (per-msg)",              m, p);
    for &k in &[8usize, 32, 128, 512] {
        let (m, p) = collect(|| xt_duplex_rpc_batched(k));
        row(&format!("Duplex RPC batched K={}", k),       m, p);
    }

    println!("\n── C. Fire-and-forget + is_delivered poll (last-receipt) ──");
    let (m, p) = collect(xt_duplex_fire_and_poll);
    row("Duplex fire N + poll last receipt",          m, p);

    println!("\n── D. wait_delivered (busy-spin MVP) ──");
    let (m, p) = collect(xt_duplex_send_wait_each);
    row("Duplex send + wait_delivered (per-msg)",     m, p);
    for &k in &[8usize, 32, 128, 512] {
        let (m, p) = collect(|| xt_duplex_send_iter_wait_batched(k));
        row(&format!("Duplex send_iter+wait K={}", k),    m, p);
    }

    println!("\nDone.");
}
