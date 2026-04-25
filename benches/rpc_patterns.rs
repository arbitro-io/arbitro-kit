//! Req/Resp patterns: lockstep vs batched vs pipelined.
//!
//! Question: when you have a producer that wants replies, which pattern
//! is fastest using arbitro Ring?
//!
//!   A. Lockstep        — send 1, wait reply, send next (strict ping-pong)
//!   B. Batched          — send K, drain K replies, repeat
//!   C. Pipelined        — fire all, drain replies in parallel (3 threads)
//!   Z. One-way baseline — send only, no reply (reference cost)
//!
//! All variants use arbitro Ring<u64, CAP> for both directions. Worker's
//! "processing" is `v.wrapping_mul(2) | 1` — minimal, isolates transport
//! cost. Real RPC would do more, in which case batching wins by even more.
//!
//! Reports: ns per round-trip (or ns per send for Z), best-of-ROUNDS.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::stream::Ring;

const N_MSGS: u64 = 10_000;
const ROUNDS: usize = 10;
const WARMUP: usize = 3;

#[inline(always)]
fn process(v: u64) -> u64 {
    v.wrapping_mul(2) | 1
}

/// Simula `iters` iteraciones de trabajo CPU-bound. ~1.5 ns/iter en x86-64
/// release. Para targets aproximados:
///   33 iters  ≈ 50 ns
///   66 iters  ≈ 100 ns
///   333 iters ≈ 500 ns
#[inline(always)]
fn work(v: u64, iters: u32) -> u64 {
    let mut x = v;
    for i in 0..iters {
        x = x.wrapping_add(i as u64).wrapping_mul(2654435761);
    }
    std::hint::black_box(x)
}

// ─── A. Lockstep RPC (1:1 ping-pong) ──────────────────────────────────────
fn run_lockstep<const CAP: usize>() -> f64 {
    let req: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let resp: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let req_w = req.clone();
    let resp_w = resp.clone();

    let worker = thread::spawn(move || {
        req_w.set_consumer(thread::current());
        resp_w.set_producer(thread::current());
        for _ in 0..N_MSGS {
            let v = req_w.recv();
            resp_w.send(process(v));
        }
    });

    req.set_producer(thread::current());
    resp.set_consumer(thread::current());
    let t0 = Instant::now();
    for i in 0..N_MSGS {
        req.send(i);
        let _ = resp.recv();
    }
    let ns = t0.elapsed().as_nanos() as f64;
    worker.join().unwrap();
    ns / N_MSGS as f64
}

// ─── A2. Lockstep RPC, NON-BLOCKING (busy-spin both sides) ────────────────
//
// Same shape as A but both producer and worker use try_send / try_recv in
// spin loops instead of blocking send / recv. No park, no wake — pure
// cursor polling. Burns 100% CPU on both threads. This is the LMAX
// Disruptor-style "BusySpin" answer to the lockstep question.
fn run_lockstep_spin<const CAP: usize>() -> f64 {
    let req: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let resp: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let req_w = req.clone();
    let resp_w = resp.clone();

    let worker = thread::spawn(move || {
        for _ in 0..N_MSGS {
            let v = loop {
                if let Some(v) = req_w.try_recv() { break v; }
                std::hint::spin_loop();
            };
            let mut r = process(v);
            loop {
                match resp_w.try_send(r) {
                    Ok(()) => break,
                    Err(v) => { r = v; std::hint::spin_loop(); }
                }
            }
        }
    });

    let t0 = Instant::now();
    for i in 0..N_MSGS {
        let mut v = i;
        loop {
            match req.try_send(v) {
                Ok(()) => break,
                Err(x) => { v = x; std::hint::spin_loop(); }
            }
        }
        loop {
            if let Some(_) = resp.try_recv() { break; }
            std::hint::spin_loop();
        }
    }
    let ns = t0.elapsed().as_nanos() as f64;
    worker.join().unwrap();
    ns / N_MSGS as f64
}

// ─── B. Batched RPC (K send, K recv, repeat) ──────────────────────────────
fn run_batched<const CAP: usize>(batch: usize) -> f64 {
    let req: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let resp: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let req_w = req.clone();
    let resp_w = resp.clone();

    let worker = thread::spawn(move || {
        req_w.set_consumer(thread::current());
        resp_w.set_producer(thread::current());
        let mut buf: Vec<u64> = Vec::with_capacity(batch);
        let mut total = 0u64;
        while total < N_MSGS {
            buf.clear();
            // Block on first, then opportunistically drain up to `batch`.
            buf.push(req_w.recv());
            let _ = req_w.drain_into(&mut buf, batch - 1);
            // Process in place.
            for v in buf.iter_mut() { *v = process(*v); }
            // Reply: try batch send, fall back to per-item if ring filled.
            let n = buf.len();
            let _ = resp_w.try_send_from(&mut buf);
            for v in buf.drain(..) { resp_w.send(v); }
            total += n as u64;
        }
    });

    req.set_producer(thread::current());
    resp.set_consumer(thread::current());
    let t0 = Instant::now();
    let mut sent = 0u64;
    let mut received = 0u64;
    let mut send_buf: Vec<u64> = Vec::with_capacity(batch);
    let mut recv_buf: Vec<u64> = Vec::with_capacity(batch);
    while received < N_MSGS {
        // Push a batch of requests (fall back to blocking if ring full).
        while sent < N_MSGS && send_buf.len() < batch {
            send_buf.push(sent);
            sent += 1;
        }
        let _ = req.try_send_from(&mut send_buf);
        for v in send_buf.drain(..) { req.send(v); }
        // Drain a batch of replies (blocks on first, opportunistic rest).
        recv_buf.clear();
        recv_buf.push(resp.recv());
        let _ = resp.drain_into(&mut recv_buf, batch - 1);
        received += recv_buf.len() as u64;
    }
    let ns = t0.elapsed().as_nanos() as f64;
    worker.join().unwrap();
    ns / N_MSGS as f64
}

// ─── G. Ack-RTT (send + wait for delivery cursor, sin payload de respuesta) ─
//
// Esta es la métrica núcleo de Nexo: productor manda y luego solo espera
// que el cursor publicado del consumer cruce su seq. NO hay reply payload,
// NO hay segundo cursor publish del consumer al productor — solo Acquire
// loads del cursor compartido.
//
// Vs lockstep clásico: lockstep paga 4 hops cross-thread (req-wake +
// reply-wake + drain + drain). Ack-RTT paga ~2 hops (req-wake + cursor
// load). Debería ser ~la mitad.
fn run_ack_rtt<const CAP: usize>() -> f64 {
    let r: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let r2 = r.clone();
    // El "cursor publicado" del consumer. Cada recv incrementa.
    let cursor = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let cursor_c = cursor.clone();

    let consumer = thread::spawn(move || {
        r2.set_consumer(thread::current());
        for i in 0..N_MSGS {
            let _ = r2.recv();
            cursor_c.store(i as u64 + 1, Ordering::Release);
        }
    });

    r.set_producer(thread::current());
    let t0 = Instant::now();
    for i in 0..N_MSGS as u64 {
        r.send(i);
        // Espera ack: cursor del consumer pasó nuestro seq.
        while cursor.load(Ordering::Acquire) <= i {
            std::hint::spin_loop();
        }
    }
    let ns = t0.elapsed().as_nanos() as f64;
    consumer.join().unwrap();
    ns / N_MSGS as f64
}

// Variante batched del Ack-RTT: productor envía K, espera 1 ack para los K.
fn run_ack_rtt_batched<const CAP: usize>(batch: usize) -> f64 {
    let r: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let r2 = r.clone();
    let cursor = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let cursor_c = cursor.clone();

    let consumer = thread::spawn(move || {
        r2.set_consumer(thread::current());
        let mut buf: Vec<u64> = Vec::with_capacity(batch);
        let mut total = 0u64;
        while total < N_MSGS {
            buf.clear();
            buf.push(r2.recv());
            let _ = r2.drain_into(&mut buf, batch - 1);
            total += buf.len() as u64;
            // Publica cursor solo al final del batch — 1 store por K msgs.
            cursor_c.store(total, Ordering::Release);
        }
    });

    r.set_producer(thread::current());
    let t0 = Instant::now();
    let mut sent = 0u64;
    let mut send_buf: Vec<u64> = Vec::with_capacity(batch);
    while sent < N_MSGS {
        let target = (sent + batch as u64).min(N_MSGS);
        while sent < target { send_buf.push(sent); sent += 1; }
        let _ = r.try_send_from(&mut send_buf);
        for x in send_buf.drain(..) { r.send(x); }
        // Espera ack para todos los del batch.
        while cursor.load(Ordering::Acquire) < target {
            std::hint::spin_loop();
        }
    }
    let ns = t0.elapsed().as_nanos() as f64;
    consumer.join().unwrap();
    ns / N_MSGS as f64
}

// ─── F. Buffered con flush por UMBRAL O TIEMPO ─────────────────────────────
//
// Mismo accumulator que D, pero ahora flushea si:
//   - el buffer alcanza `threshold` items, O
//   - pasaron `timeout_ns` desde el último flush.
//
// Mide si el chequeo extra de tiempo (Instant::elapsed) degrada el path
// caliente saturado. Si throughput ≈ run_buffered, el flush por tiempo es
// "free" cuando nunca se dispara — y útil cuando sí (ver sporadic test).
fn run_buffered_timed<const CAP: usize>(threshold: usize, timeout_ns: u64) -> f64 {
    use std::time::Duration;
    let timeout = Duration::from_nanos(timeout_ns);

    let req: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let resp: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let req_w = req.clone(); let resp_w = resp.clone();
    let worker = thread::spawn(move || {
        req_w.set_consumer(thread::current());
        resp_w.set_producer(thread::current());
        let mut in_buf: Vec<u64> = Vec::with_capacity(threshold);
        let mut out_acc: Vec<u64> = Vec::with_capacity(threshold);
        let mut last_flush = Instant::now();
        let mut total = 0u64;
        while total < N_MSGS {
            in_buf.clear();
            in_buf.push(req_w.recv());
            let _ = req_w.drain_into(&mut in_buf, threshold - 1);
            for v in in_buf.drain(..) {
                out_acc.push(process(v));
                total += 1;
                if out_acc.len() >= threshold
                    || (!out_acc.is_empty() && last_flush.elapsed() >= timeout)
                {
                    let _ = resp_w.try_send_from(&mut out_acc);
                    for x in out_acc.drain(..) { resp_w.send(x); }
                    last_flush = Instant::now();
                }
            }
            if !out_acc.is_empty() {
                let _ = resp_w.try_send_from(&mut out_acc);
                for x in out_acc.drain(..) { resp_w.send(x); }
                last_flush = Instant::now();
            }
        }
    });
    req.set_producer(thread::current());
    resp.set_consumer(thread::current());
    let t0 = Instant::now();
    let mut sent = 0u64;
    let mut received = 0u64;
    let mut send_acc: Vec<u64> = Vec::with_capacity(threshold);
    let mut recv_buf: Vec<u64> = Vec::with_capacity(threshold);
    let mut last_flush = Instant::now();
    while received < N_MSGS {
        while sent < N_MSGS && send_acc.len() < threshold {
            send_acc.push(sent); sent += 1;
        }
        // Force flush at end of stream: si ya empujamos todos los msgs,
        // hay que vaciar el residuo o nos clavamos en resp.recv() forever.
        let force_final = sent >= N_MSGS;
        if !send_acc.is_empty()
            && (send_acc.len() >= threshold
                || last_flush.elapsed() >= timeout
                || force_final)
        {
            let _ = req.try_send_from(&mut send_acc);
            for x in send_acc.drain(..) { req.send(x); }
            last_flush = Instant::now();
        }
        recv_buf.clear();
        recv_buf.push(resp.recv());
        let _ = resp.drain_into(&mut recv_buf, threshold - 1);
        received += recv_buf.len() as u64;
    }
    let ns = t0.elapsed().as_nanos() as f64;
    worker.join().unwrap();
    ns / N_MSGS as f64
}

// ─── E. WORKLOAD: lockstep vs buffered con trabajo real por mensaje ───────
//
// Compara los dos patrones cuando el worker hace `iters` iteraciones de
// CPU-bound work por mensaje. Pregunta: ¿el batching sigue ganando cuando
// el procesamiento domina sobre el transporte?
fn run_lockstep_work<const CAP: usize>(iters: u32) -> f64 {
    let req: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let resp: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let req_w = req.clone(); let resp_w = resp.clone();
    let worker = thread::spawn(move || {
        req_w.set_consumer(thread::current());
        resp_w.set_producer(thread::current());
        for _ in 0..N_MSGS {
            let v = req_w.recv();
            resp_w.send(work(v, iters));
        }
    });
    req.set_producer(thread::current());
    resp.set_consumer(thread::current());
    let t0 = Instant::now();
    for i in 0..N_MSGS {
        req.send(i);
        let _ = resp.recv();
    }
    let ns = t0.elapsed().as_nanos() as f64;
    worker.join().unwrap();
    ns / N_MSGS as f64
}

fn run_buffered_work<const CAP: usize>(threshold: usize, iters: u32) -> f64 {
    let req: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let resp: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let req_w = req.clone(); let resp_w = resp.clone();
    let worker = thread::spawn(move || {
        req_w.set_consumer(thread::current());
        resp_w.set_producer(thread::current());
        let mut in_buf: Vec<u64> = Vec::with_capacity(threshold);
        let mut out_acc: Vec<u64> = Vec::with_capacity(threshold);
        let mut total = 0u64;
        while total < N_MSGS {
            in_buf.clear();
            in_buf.push(req_w.recv());
            let _ = req_w.drain_into(&mut in_buf, threshold - 1);
            for v in in_buf.drain(..) {
                out_acc.push(work(v, iters));
                total += 1;
                if out_acc.len() >= threshold {
                    let _ = resp_w.try_send_from(&mut out_acc);
                    for x in out_acc.drain(..) { resp_w.send(x); }
                }
            }
            if !out_acc.is_empty() {
                let _ = resp_w.try_send_from(&mut out_acc);
                for x in out_acc.drain(..) { resp_w.send(x); }
            }
        }
    });
    req.set_producer(thread::current());
    resp.set_consumer(thread::current());
    let t0 = Instant::now();
    let mut sent = 0u64;
    let mut received = 0u64;
    let mut send_acc: Vec<u64> = Vec::with_capacity(threshold);
    let mut recv_buf: Vec<u64> = Vec::with_capacity(threshold);
    while received < N_MSGS {
        while sent < N_MSGS && send_acc.len() < threshold {
            send_acc.push(sent); sent += 1;
        }
        let _ = req.try_send_from(&mut send_acc);
        for x in send_acc.drain(..) { req.send(x); }
        recv_buf.clear();
        recv_buf.push(resp.recv());
        let _ = resp.drain_into(&mut recv_buf, threshold - 1);
        received += recv_buf.len() as u64;
    }
    let ns = t0.elapsed().as_nanos() as f64;
    worker.join().unwrap();
    ns / N_MSGS as f64
}

// ─── D. Buffered "single-send" RPC (raw accumulator, no struct) ───────────
//
// API que se siente single-send: producer llama bsend(v) por mensaje. Un
// Vec local acumula. Cuando llega a K, se hace try_send_from al ring (1
// cursor publish para los K). Mismo en ambas direcciones (simétrico —
// recordar que en Nexo los dos lados son P/C).
//
// Si esto da ~ batched K=K, confirma que un accumulator transparente
// cierra el gap sin cambiar el API público.
fn run_buffered<const CAP: usize>(threshold: usize) -> f64 {
    let req: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let resp: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let req_w = req.clone();
    let resp_w = resp.clone();

    let worker = thread::spawn(move || {
        req_w.set_consumer(thread::current());
        resp_w.set_producer(thread::current());
        let mut in_buf: Vec<u64> = Vec::with_capacity(threshold);
        let mut out_acc: Vec<u64> = Vec::with_capacity(threshold);
        let mut total = 0u64;
        while total < N_MSGS {
            in_buf.clear();
            in_buf.push(req_w.recv());
            let _ = req_w.drain_into(&mut in_buf, threshold - 1);
            // Procesa cada uno y "single-send" al accumulator local.
            for v in in_buf.drain(..) {
                out_acc.push(process(v));
                total += 1;
                if out_acc.len() >= threshold {
                    // Flush.
                    let _ = resp_w.try_send_from(&mut out_acc);
                    for x in out_acc.drain(..) { resp_w.send(x); }
                }
            }
            // Drop-flush si quedó residuo (al final del trabajo).
            if !out_acc.is_empty() {
                let _ = resp_w.try_send_from(&mut out_acc);
                for x in out_acc.drain(..) { resp_w.send(x); }
            }
        }
    });

    req.set_producer(thread::current());
    resp.set_consumer(thread::current());

    let t0 = Instant::now();
    let mut sent = 0u64;
    let mut received = 0u64;
    let mut send_acc: Vec<u64> = Vec::with_capacity(threshold);
    let mut recv_buf: Vec<u64> = Vec::with_capacity(threshold);

    while received < N_MSGS {
        // Producer "single send" — cada call empuja al accumulator local.
        while sent < N_MSGS && send_acc.len() < threshold {
            send_acc.push(sent);
            sent += 1;
        }
        // Flush del accumulator local del productor.
        let _ = req.try_send_from(&mut send_acc);
        for x in send_acc.drain(..) { req.send(x); }

        // Drena replies en bulk.
        recv_buf.clear();
        recv_buf.push(resp.recv());
        let _ = resp.drain_into(&mut recv_buf, threshold - 1);
        received += recv_buf.len() as u64;
    }

    let ns = t0.elapsed().as_nanos() as f64;
    worker.join().unwrap();
    ns / N_MSGS as f64
}

// ─── C. Pipelined RPC (3 threads, no waiting) ─────────────────────────────
fn run_pipelined<const CAP: usize>() -> f64 {
    let req: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let resp: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let req_w = req.clone();
    let resp_w = resp.clone();
    let resp_c = resp.clone();
    let done = Arc::new(AtomicBool::new(false));
    let done_c = done.clone();

    let worker = thread::spawn(move || {
        req_w.set_consumer(thread::current());
        resp_w.set_producer(thread::current());
        for _ in 0..N_MSGS {
            let v = req_w.recv();
            resp_w.send(process(v));
        }
    });

    let collector = thread::spawn(move || {
        resp_c.set_consumer(thread::current());
        let mut sum = 0u64;
        for _ in 0..N_MSGS { sum = sum.wrapping_add(resp_c.recv()); }
        done_c.store(true, Ordering::Release);
        sum
    });

    req.set_producer(thread::current());
    let t0 = Instant::now();
    for i in 0..N_MSGS { req.send(i); }
    // Wait for collector to drain all replies.
    while !done.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }
    let ns = t0.elapsed().as_nanos() as f64;
    worker.join().unwrap();
    let _ = collector.join().unwrap();
    ns / N_MSGS as f64
}

// ─── Z. One-way baseline (send only, no reply) ────────────────────────────
fn run_oneway<const CAP: usize>() -> f64 {
    let r: Arc<Ring<u64, CAP>> = Arc::new(Ring::new());
    let r2 = r.clone();
    let consumer = thread::spawn(move || {
        r2.set_consumer(thread::current());
        let mut sum = 0u64;
        for _ in 0..N_MSGS { sum = sum.wrapping_add(r2.recv()); }
        sum
    });
    r.set_producer(thread::current());
    let t0 = Instant::now();
    for i in 0..N_MSGS { r.send(i); }
    let _ = consumer.join().unwrap();
    let ns = t0.elapsed().as_nanos() as f64;
    ns / N_MSGS as f64
}

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

fn row(name: &str, min_ns: f64, p50_ns: f64, baseline: Option<f64>) {
    let rps = 1e9 / min_ns;
    let rel = match baseline {
        Some(b) if b > 0.0 => format!("{:.2}×", b / min_ns),
        _ => "—".to_string(),
    };
    println!("{:<40} {:>10.1} {:>10.1} {:>14.0} {:>10}",
             name, min_ns, p50_ns, rps, rel);
}

fn main() {
    println!("=== RPC patterns: lockstep vs batched vs pipelined ===");
    println!("{} round-trips per measurement; best-of-{} (after {} warmup).", N_MSGS, ROUNDS, WARMUP);
    println!("Worker processing: 1 mul + 1 or = trivial. Numbers reflect transport cost.\n");

    println!("{:<40} {:>10} {:>10} {:>14} {:>10}",
             "scenario", "min ns/RT", "p50 ns/RT", "RT/sec (min)", "rel A");
    println!("{}", "─".repeat(90));

    // Section Z — one-way reference (not a round-trip; ns per send).
    println!("\n── Z. One-way SEND ONLY (reference, ns/send) ──");
    let (m, p) = collect(|| run_oneway::<256>());
    row("oneway Ring<u64, 256>",            m, p, None);
    let (m, p) = collect(|| run_oneway::<1024>());
    row("oneway Ring<u64, 1024>",           m, p, None);

    // Section A — lockstep.
    println!("\n── A. LOCKSTEP RPC (send + recv interleaved) ──");
    let (a16, p) = collect(|| run_lockstep::<16>());
    row("lockstep Ring<u64, 16>",           a16, p, None);
    let (m, p) = collect(|| run_lockstep::<256>());
    row("lockstep Ring<u64, 256>",          m, p, Some(a16));
    let (m, p) = collect(|| run_lockstep::<1024>());
    row("lockstep Ring<u64, 1024>",         m, p, Some(a16));

    // Section A2 — lockstep non-blocking (busy-spin).
    println!("\n── A2. LOCKSTEP RPC, NON-BLOCKING (busy-spin both sides — 100% CPU) ──");
    let (m, p) = collect(|| run_lockstep_spin::<16>());
    row("lockstep-spin Ring<u64, 16>",      m, p, Some(a16));
    let (m, p) = collect(|| run_lockstep_spin::<256>());
    row("lockstep-spin Ring<u64, 256>",     m, p, Some(a16));
    let (m, p) = collect(|| run_lockstep_spin::<1024>());
    row("lockstep-spin Ring<u64, 1024>",    m, p, Some(a16));

    // Section B — batched.
    println!("\n── B. BATCHED RPC (K send, K recv, repeat) ──");
    for &b in &[8usize, 16, 32, 64, 128] {
        let (m, p) = collect(|| run_batched::<1024>(b));
        row(&format!("batched K={} Ring<u64, 1024>", b), m, p, Some(a16));
    }

    // Section D — buffered (single-send API, accumulator under the hood).
    println!("\n── D. BUFFERED single-send RPC (local accumulator, K-flush) ──");
    for &k in &[8usize, 16, 32, 64, 128] {
        let (m, p) = collect(|| run_buffered::<1024>(k));
        row(&format!("buffered K={} Ring<u64, 1024>", k), m, p, Some(a16));
    }

    // Section G — Ack-RTT (send + wait for delivery cursor).
    println!("\n── G. ACK-RTT (send + wait cursor, sin reply payload) ──");
    let (m, p) = collect(|| run_ack_rtt::<256>());
    row("ack-rtt per-msg Ring<u64, 256>",  m, p, Some(a16));
    let (m, p) = collect(|| run_ack_rtt::<1024>());
    row("ack-rtt per-msg Ring<u64, 1024>", m, p, Some(a16));
    for &k in &[8usize, 16, 32, 64, 128] {
        let (m, p) = collect(|| run_ack_rtt_batched::<1024>(k));
        row(&format!("ack-rtt batched K={}", k), m, p, Some(a16));
    }

    // Section F — buffered con flush por umbral O tiempo.
    println!("\n── F. BUFFERED con flush K=64 OR timeout (saturated load) ──");
    for &timeout_ns in &[1_000u64, 10_000, 100_000, 1_000_000] {
        let (m, p) = collect(|| run_buffered_timed::<1024>(64, timeout_ns));
        let label = format!("buffered K=64 OR {}µs", timeout_ns / 1_000);
        row(&label, m, p, Some(a16));
    }

    // Section E — workload sweep: lockstep vs buffered K=64 con trabajo real.
    println!("\n── E. WORKLOAD SWEEP (lockstep vs buffered K=64, ns/RT incluye trabajo) ──");
    println!("{:<40} {:>10} {:>10} {:>14} {:>10}",
             "scenario (work iters)", "min ns/RT", "p50 ns/RT", "RT/sec (min)", "rel A");
    for &iters in &[0u32, 33, 66, 333, 1000] {
        let label_iters = if iters == 0 { "0 (raw)".to_string() } else { format!("{}", iters) };
        let (m, p) = collect(|| run_lockstep_work::<1024>(iters));
        row(&format!("lockstep work={}", label_iters), m, p, Some(a16));
        let (m, p) = collect(|| run_buffered_work::<1024>(64, iters));
        row(&format!("buffered K=64 work={}", label_iters), m, p, Some(a16));
        println!();
    }

    // Section C — pipelined.
    println!("\n── C. PIPELINED (3 threads — fire-and-collect) ──");
    let (m, p) = collect(|| run_pipelined::<256>());
    row("pipelined Ring<u64, 256>",         m, p, Some(a16));
    let (m, p) = collect(|| run_pipelined::<1024>());
    row("pipelined Ring<u64, 1024>",        m, p, Some(a16));

    println!("\nrel A = how many times faster than lockstep@CAP=16 baseline.");
    println!("\nDone.");
}
