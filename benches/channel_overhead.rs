//! Focused benchmark for `Channel` — with head-to-head crossbeam and
//! std::mpsc at the same payload shapes so throughput is directly comparable.
//!
//! Scope: measure the cost of a request→reply round-trip.
//! Wake-primitive tests live in `gate_overhead.rs`.
//!
//! Each scenario runs `rounds` RTTs on:
//!   - `Channel<Req, Resp>` (this crate)
//!   - `crossbeam_channel::bounded(1)` pair  (req + resp channels)
//!   - `std::sync::mpsc::sync_channel(1)` pair
//!
//! Scenarios:
//!   - **ST** (single-thread) — only for `crossbeam bounded(1)` and
//!     `mpsc sync(1)` (they work on one thread with capacity 1).
//!     `Channel` is cross-thread by design (client parks on the response
//!     gate until a *separate* server thread fires it) and has no
//!     single-thread analogue — omitted with a note.
//!   - **XT** (cross-thread) — producer + server on separate threads;
//!     full payload sweep.
//!
//! Columns:
//!   - p50_ns, p99_ns — latency distribution of a full round-trip.
//!   - ops/sec — RTTs per second.
//!   - MB/s — payload-size × ops/sec (one-way equivalent throughput). For
//!            zero-copy primitives this can exceed DRAM bandwidth because
//!            the data never physically crosses — it's "effective" throughput.
//!
//! Run: `cargo bench --bench channel_overhead`
//! Env: `BENCH_ROUNDS=1000 BENCH_WARMUP=100`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::slot::Channel;

fn rounds() -> usize {
    std::env::var("BENCH_ROUNDS").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(1000)
}
fn warmup() -> usize {
    std::env::var("BENCH_WARMUP").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(100)
}

/// Cap rounds for big-payload scenarios so the total bench stays inside the
/// bench_safety 120 s budget. Allocating 16 MB × 1000 times costs ~5 s of
/// zero-init alone; 100 rounds still gives a useful sample.
fn rounds_for_size(bytes: usize) -> usize {
    let base = rounds();
    match bytes {
        0..=4_096         => base,
        4_097..=65_536    => base / 2,
        65_537..=1_048_576 => base / 10,
        _                 => base / 50,    // 16 MB → base/50 = 20 for rounds=1000
    }.max(20)
}

struct Row {
    primitive: &'static str,
    p50_ns: u64,
    p99_ns: u64,
    ops_per_sec: u64,
    mb_per_sec: f64,
}

fn print_scenario_header(name: &str, payload_bytes: usize) {
    if payload_bytes == 0 {
        println!("\n── {} ──", name);
    } else {
        let sz = human(payload_bytes);
        println!("\n── {} (payload = {}) ──", name, sz);
    }
    println!("{:<20} {:>10} {:>10} {:>14} {:>14}",
             "primitive", "p50_ns", "p99_ns", "ops/sec", "MB/s");
    println!("{}", "─".repeat(72));
}

fn human(b: usize) -> String {
    if b >= 1 << 20 { format!("{} MB", b >> 20) }
    else if b >= 1 << 10 { format!("{} KB", b >> 10) }
    else { format!("{} B", b) }
}

fn print_row(r: Row) {
    println!("{:<20} {:>10} {:>10} {:>14} {:>14.1}",
             r.primitive, r.p50_ns, r.p99_ns, r.ops_per_sec, r.mb_per_sec);
}

/// Compute ops/sec and MB/s from latencies + total elapsed.
fn finish(
    primitive: &'static str,
    mut lats: Vec<u64>,
    elapsed_ns: u64,
    payload_bytes: usize,
) -> Row {
    lats.sort_unstable();
    let rounds = lats.len();
    let ops = (rounds as f64) / (elapsed_ns as f64 / 1e9);
    let mb = (payload_bytes as f64 * ops) / 1e6;
    Row {
        primitive,
        p50_ns: lats[rounds / 2],
        p99_ns: lats[rounds * 99 / 100],
        ops_per_sec: ops as u64,
        mb_per_sec: mb,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CROSS-THREAD runners
// ═══════════════════════════════════════════════════════════════════════════

fn xt_channel<Req, Resp, MkReq, Handler>(
    payload_bytes: usize,
    mk: MkReq,
    handler: Handler,
) -> Row
where
    Req: Send + 'static,
    Resp: Send + 'static,
    MkReq: Fn(u64) -> Req,
    Handler: Fn(Req) -> Resp + Send + 'static,
{
    let rounds = rounds_for_size(payload_bytes);
    let (client, server) = Channel::<Req, Resp>::spsc();
    let stop = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(AtomicBool::new(false));

    let stop_s = stop.clone();
    let ready_s = ready.clone();
    let h = thread::spawn(move || {
        server.bind();
        ready_s.store(true, Ordering::Release);
        while !stop_s.load(Ordering::Relaxed) {
            server.serve_one(&handler);
        }
    });

    while !ready.load(Ordering::Acquire) { thread::yield_now(); }
    client.bind();

    for i in 0..warmup() { let _ = client.call(mk(i as u64)); }

    let mut lats = Vec::with_capacity(rounds);
    let t_wall = Instant::now();
    for i in 0..rounds {
        let req = mk(i as u64);
        let t0 = Instant::now();
        let resp = client.call(req);
        lats.push(t0.elapsed().as_nanos() as u64);
        std::hint::black_box(resp);
    }
    let elapsed_ns = t_wall.elapsed().as_nanos() as u64;

    stop.store(true, Ordering::Relaxed);
    let _ = client.call(mk(0));
    h.join().unwrap();
    finish("Channel", lats, elapsed_ns, payload_bytes)
}

fn xt_cbpair<Req, Resp, MkReq, Handler>(
    payload_bytes: usize,
    mk: MkReq,
    handler: Handler,
) -> Row
where
    Req: Send + 'static,
    Resp: Send + 'static,
    MkReq: Fn(u64) -> Req,
    Handler: Fn(Req) -> Resp + Send + 'static,
{
    let rounds = rounds_for_size(payload_bytes);
    let (tx_req, rx_req) = crossbeam_channel::bounded::<Req>(1);
    let (tx_resp, rx_resp) = crossbeam_channel::bounded::<Resp>(1);

    let h = thread::spawn(move || {
        while let Ok(req) = rx_req.recv() {
            let resp = handler(req);
            if tx_resp.send(resp).is_err() { return; }
        }
    });

    for i in 0..warmup() {
        tx_req.send(mk(i as u64)).unwrap();
        let _ = rx_resp.recv().unwrap();
    }

    let mut lats = Vec::with_capacity(rounds);
    let t_wall = Instant::now();
    for i in 0..rounds {
        let req = mk(i as u64);
        let t0 = Instant::now();
        tx_req.send(req).unwrap();
        let resp = rx_resp.recv().unwrap();
        lats.push(t0.elapsed().as_nanos() as u64);
        std::hint::black_box(resp);
    }
    let elapsed_ns = t_wall.elapsed().as_nanos() as u64;

    drop(tx_req);
    h.join().unwrap();
    finish("crossbeam pair", lats, elapsed_ns, payload_bytes)
}

fn xt_mpscpair<Req, Resp, MkReq, Handler>(
    payload_bytes: usize,
    mk: MkReq,
    handler: Handler,
) -> Row
where
    Req: Send + 'static,
    Resp: Send + 'static,
    MkReq: Fn(u64) -> Req,
    Handler: Fn(Req) -> Resp + Send + 'static,
{
    let rounds = rounds_for_size(payload_bytes);
    let (tx_req, rx_req) = sync_channel::<Req>(1);
    let (tx_resp, rx_resp) = sync_channel::<Resp>(1);

    let h = thread::spawn(move || {
        while let Ok(req) = rx_req.recv() {
            let resp = handler(req);
            if tx_resp.send(resp).is_err() { return; }
        }
    });

    for i in 0..warmup() {
        tx_req.send(mk(i as u64)).unwrap();
        let _ = rx_resp.recv().unwrap();
    }

    let mut lats = Vec::with_capacity(rounds);
    let t_wall = Instant::now();
    for i in 0..rounds {
        let req = mk(i as u64);
        let t0 = Instant::now();
        tx_req.send(req).unwrap();
        let resp = rx_resp.recv().unwrap();
        lats.push(t0.elapsed().as_nanos() as u64);
        std::hint::black_box(resp);
    }
    let elapsed_ns = t_wall.elapsed().as_nanos() as u64;

    drop(tx_req);
    h.join().unwrap();
    finish("mpsc pair", lats, elapsed_ns, payload_bytes)
}

// ═══════════════════════════════════════════════════════════════════════════
// SINGLE-THREAD runners — send then recv on the same thread. Capacity 1 is
// enough; the queue acts as a single-slot mailbox. No cross-core traffic,
// so this measures the atomic + mutex overhead of each primitive's
// send/recv fast-path.
// ═══════════════════════════════════════════════════════════════════════════

fn st_cbpair_u64() -> Row {
    let (tx_req, rx_req) = crossbeam_channel::bounded::<u64>(1);
    let (tx_resp, rx_resp) = crossbeam_channel::bounded::<u64>(1);
    for i in 0..warmup() as u64 {
        tx_req.send(i).unwrap();
        let r = rx_req.recv().unwrap();
        tx_resp.send(r.wrapping_add(1)).unwrap();
        let _ = rx_resp.recv().unwrap();
    }
    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for i in 0..rounds() as u64 {
        let t0 = Instant::now();
        tx_req.send(i).unwrap();
        let r = rx_req.recv().unwrap();
        tx_resp.send(r.wrapping_add(1)).unwrap();
        let _ = rx_resp.recv().unwrap();
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;
    finish("crossbeam pair", lats, el, 8)
}

fn st_mpscpair_u64() -> Row {
    let (tx_req, rx_req) = sync_channel::<u64>(1);
    let (tx_resp, rx_resp) = sync_channel::<u64>(1);
    for i in 0..warmup() as u64 {
        tx_req.send(i).unwrap();
        let r = rx_req.recv().unwrap();
        tx_resp.send(r.wrapping_add(1)).unwrap();
        let _ = rx_resp.recv().unwrap();
    }
    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for i in 0..rounds() as u64 {
        let t0 = Instant::now();
        tx_req.send(i).unwrap();
        let r = rx_req.recv().unwrap();
        tx_resp.send(r.wrapping_add(1)).unwrap();
        let _ = rx_resp.recv().unwrap();
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;
    finish("mpsc pair", lats, el, 8)
}

// ═══════════════════════════════════════════════════════════════════════════

fn compare<Req, Resp, MkReq, Handler>(
    scenario: &str,
    payload_bytes: usize,
    mk: MkReq,
    handler: Handler,
)
where
    Req: Send + 'static,
    Resp: Send + 'static,
    MkReq: Fn(u64) -> Req + Clone,
    Handler: Fn(Req) -> Resp + Send + Clone + 'static,
{
    print_scenario_header(scenario, payload_bytes);
    print_row(xt_channel(payload_bytes, mk.clone(), handler.clone()));
    print_row(xt_cbpair(payload_bytes, mk.clone(), handler.clone()));
    print_row(xt_mpscpair(payload_bytes, mk, handler));
}

fn main() {
    println!("=== arbitro-kit channel_overhead (round-trip comparison) ===");
    println!("rounds={} (capped per size)  warmup={}", rounds(), warmup());
    println!("MB/s = payload × ops/sec (one-way equivalent). Zero-copy");
    println!("primitives can exceed DRAM bandwidth because data never moves.");

    // ── SINGLE-THREAD ─────────────────────────────────────────────────────
    // Channel has no ST analogue (client parks on a separate gate until the
    // server fires it from another thread). We still measure cb and mpsc ST
    // to give a reference for their send+recv mutex overhead without
    // cross-core coherence traffic.
    println!("\n── SINGLE-THREAD (u64 by-value, payload = 8 B) ──");
    println!("{:<20} {:>10} {:>10} {:>14} {:>14}",
             "primitive", "p50_ns", "p99_ns", "ops/sec", "MB/s");
    println!("{}", "─".repeat(72));
    println!("{:<20} {:>10} {:>10} {:>14} {:>14}",
             "Channel", "—", "—", "—", "(cross-thread by design)");
    print_row(st_cbpair_u64());
    print_row(st_mpscpair_u64());

    // ── CROSS-THREAD ──────────────────────────────────────────────────────
    // Handshake floor (Channel only has a meaningful zero-payload number)
    {
        print_scenario_header("XT Handshake floor (zero payload)", 0);
        print_row(xt_channel::<(), (), _, _>(0, |_| (), |_| ()));
        print_row(xt_cbpair::<(), (), _, _>(0, |_| (), |_| ()));
        print_row(xt_mpscpair::<(), (), _, _>(0, |_| (), |_| ()));
    }

    // Small by-value
    compare::<u64, u64, _, _>("XT u64 by-value", 8, |i| i, |r| r.wrapping_add(1));
    compare::<[u8; 64], [u8; 64], _, _>("XT [u8; 64] by-value", 64,
        |i| { let mut a = [0u8; 64]; a[0] = i as u8; a },
        |r| r,
    );
    compare::<[u8; 256], [u8; 256], _, _>("XT [u8; 256] by-value", 256,
        |i| { let mut a = [0u8; 256]; a[0] = i as u8; a },
        |r| r,
    );

    // Medium by-value
    compare::<[u8; 1024], [u8; 1024], _, _>("XT [u8; 1024] by-value", 1024,
        |i| { let mut a = [0u8; 1024]; a[0] = i as u8; a },
        |r| r,
    );
    compare::<[u8; 4096], [u8; 4096], _, _>("XT [u8; 4096] by-value", 4096,
        |i| { let mut a = [0u8; 4096]; a[0] = i as u8; a },
        |r| r,
    );

    // Ownership transfer Vec<u8> (zero-copy)
    for size in [4 * 1024, 64 * 1024, 1024 * 1024, 16 * 1024 * 1024] {
        let sz = size;
        compare::<Vec<u8>, Vec<u8>, _, _>("XT Vec<u8> ownership transfer", sz,
            move |i| {
                let mut v = vec![0u8; sz];
                v[0] = i as u8;
                v
            },
            |r| r,
        );
    }

    // Arc shared
    for size in [1024 * 1024, 16 * 1024 * 1024] {
        let sz = size;
        let shared: Arc<Vec<u8>> = Arc::new(vec![0xA5; sz]);
        let mk_shared = shared.clone();
        compare::<Arc<Vec<u8>>, Arc<Vec<u8>>, _, _>("XT Arc<Vec<u8>> shared", sz,
            move |_| mk_shared.clone(),
            |r| r,
        );
    }

    println!("\nDone.");
}
