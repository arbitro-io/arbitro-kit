//! 1-hop pointer delivery comparison.
//!
//! Measures the cost of moving a pointer from one thread to another, where
//! the consumer dereferences it and copies `size` bytes into a local buffer.
//! Three variants, same producer/consumer contract:
//!
//!  - **Channel** — `client.call(ptr)` → server copies, returns `()`.
//!    Natural backpressure: each call blocks for the ack. No spin.
//!  - **Signal + AtomicUsize** — raw: producer `release()`, consumer `acquire()`.
//!    Producer spins on `has_data` to respect the single slot.
//!  - **mpsc sync(1)** — baseline matched to 1-slot semantics.
//!  - **mpsc unbounded** — shows the buffering illusion: "fast" per-op only
//!    because the queue grows unbounded and hides real latency.
//!
//! Run:
//!   cargo run --release --example relay_8hop

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, sync_channel};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::gate::{Channel, Signal};

const MSGS: usize = 200_000;
const SIZES: &[usize] = &[16, 64, 256, 1024, 4096, 16_384];

// ─── Channel (request/response) ───────────────────────────────────────

fn via_channel(size: usize) -> f64 {
    let payload: Vec<u8> = (0..size).map(|i| i as u8).collect();
    let ptr_usize = payload.as_ptr() as usize;

    let (client, server) = Channel::<usize, ()>::spsc();

    let server_h = thread::spawn(move || {
        server.bind();
        let mut local = vec![0u8; size];
        let mut checksum: u64 = 0;
        for _ in 0..MSGS {
            server.serve_one(|p| {
                unsafe {
                    std::ptr::copy_nonoverlapping(p as *const u8, local.as_mut_ptr(), size);
                }
                checksum = checksum
                    .wrapping_add(local[0] as u64)
                    .wrapping_add(local[size - 1] as u64);
            });
        }
        checksum
    });

    client.bind();
    let t0 = Instant::now();
    for _ in 0..MSGS {
        let () = client.call(ptr_usize);
    }
    let _ = server_h.join().unwrap();
    let ns = t0.elapsed().as_nanos() as f64;

    std::hint::black_box(&payload);
    ns / MSGS as f64
}

// ─── Signal + AtomicUsize (raw 1-way) ─────────────────────────────────

#[repr(align(64))]
struct Hop {
    signal: Signal,
    slot: AtomicUsize,
}

fn via_signal(size: usize) -> f64 {
    let payload: Vec<u8> = (0..size).map(|i| i as u8).collect();
    let ptr_usize = payload.as_ptr() as usize;

    let hop = Arc::new(Hop {
        signal: Signal::new(),
        slot: AtomicUsize::new(0),
    });
    let h2 = hop.clone();

    let consumer = thread::spawn(move || {
        h2.signal.set_worker(thread::current());
        let mut local = vec![0u8; size];
        let mut checksum: u64 = 0;
        for _ in 0..MSGS {
            h2.signal.acquire();
            let p = h2.slot.load(Ordering::Acquire);
            h2.signal.lock();
            unsafe {
                std::ptr::copy_nonoverlapping(p as *const u8, local.as_mut_ptr(), size);
            }
            checksum = checksum
                .wrapping_add(local[0] as u64)
                .wrapping_add(local[size - 1] as u64);
        }
        checksum
    });

    let t0 = Instant::now();
    for _ in 0..MSGS {
        while hop.signal.is_open() {
            std::hint::spin_loop();
        }
        hop.slot.store(ptr_usize, Ordering::Release);
        hop.signal.release();
    }
    let _ = consumer.join().unwrap();
    let ns = t0.elapsed().as_nanos() as f64;

    std::hint::black_box(&payload);
    ns / MSGS as f64
}

// ─── mpsc sync(1) — fair 1-slot baseline ──────────────────────────────

fn via_mpsc_sync(size: usize) -> f64 {
    let payload: Vec<u8> = (0..size).map(|i| i as u8).collect();
    let ptr_usize = payload.as_ptr() as usize;

    let (tx, rx) = sync_channel::<usize>(1);

    let consumer = thread::spawn(move || {
        let mut local = vec![0u8; size];
        let mut checksum: u64 = 0;
        while let Ok(p) = rx.recv() {
            unsafe {
                std::ptr::copy_nonoverlapping(p as *const u8, local.as_mut_ptr(), size);
            }
            checksum = checksum
                .wrapping_add(local[0] as u64)
                .wrapping_add(local[size - 1] as u64);
        }
        checksum
    });

    let t0 = Instant::now();
    for _ in 0..MSGS {
        tx.send(ptr_usize).unwrap();
    }
    drop(tx);
    let _ = consumer.join().unwrap();
    let ns = t0.elapsed().as_nanos() as f64;

    std::hint::black_box(&payload);
    ns / MSGS as f64
}

// ─── mpsc unbounded — the "illusion" baseline ─────────────────────────

fn via_mpsc_unbounded(size: usize) -> f64 {
    let payload: Vec<u8> = (0..size).map(|i| i as u8).collect();
    let ptr_usize = payload.as_ptr() as usize;

    let (tx, rx) = mpsc::channel::<usize>();

    let consumer = thread::spawn(move || {
        let mut local = vec![0u8; size];
        let mut checksum: u64 = 0;
        while let Ok(p) = rx.recv() {
            unsafe {
                std::ptr::copy_nonoverlapping(p as *const u8, local.as_mut_ptr(), size);
            }
            checksum = checksum
                .wrapping_add(local[0] as u64)
                .wrapping_add(local[size - 1] as u64);
        }
        checksum
    });

    let t0 = Instant::now();
    for _ in 0..MSGS {
        tx.send(ptr_usize).unwrap();
    }
    drop(tx);
    let _ = consumer.join().unwrap();
    let ns = t0.elapsed().as_nanos() as f64;

    std::hint::black_box(&payload);
    ns / MSGS as f64
}

// ─── Main ─────────────────────────────────────────────────────────────

fn main() {
    println!(
        "1-hop pointer delivery (producer → consumer copies {} bytes)",
        MSGS
    );
    println!("messages = {} per run (best of 3 + warmup)\n", MSGS);
    println!(
        "{:<10} | {:>12} {:>12} {:>12} | {:>14}",
        "size", "Channel", "Signal+Atom", "mpsc sync(1)", "mpsc unbounded"
    );
    println!(
        "{:<10} | {:<38} | {}",
        "", "honest (1-slot, real backpressure)", "illusion (growing queue)"
    );
    println!("{}", "─".repeat(90));

    // Warmup.
    let _ = via_channel(64);
    let _ = via_signal(64);
    let _ = via_mpsc_sync(64);
    let _ = via_mpsc_unbounded(64);

    for &size in SIZES {
        let ch = (0..3)
            .map(|_| via_channel(size))
            .fold(f64::INFINITY, f64::min);
        let sg = (0..3)
            .map(|_| via_signal(size))
            .fold(f64::INFINITY, f64::min);
        let ms = (0..3)
            .map(|_| via_mpsc_sync(size))
            .fold(f64::INFINITY, f64::min);
        let mu = (0..3)
            .map(|_| via_mpsc_unbounded(size))
            .fold(f64::INFINITY, f64::min);
        println!(
            "{:<10} | {:>10.1}ns {:>10.1}ns {:>10.1}ns | {:>12.1}ns",
            human(size),
            ch,
            sg,
            ms,
            mu
        );
    }
}

fn human(n: usize) -> String {
    if n >= 1024 {
        format!("{} KiB", n / 1024)
    } else {
        format!("{} B", n)
    }
}
