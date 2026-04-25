//! Head-to-head fan-in throughput: arbitro Hub vs arbitro Mpmc vs
//! crossbeam_channel::bounded.
//!
//! All three: N producer threads firing as fast as they can, 1 drain
//! thread consuming. Same payload (u64), same duration. Reports msgs/sec.
//!
//! Goal: see how big the structural gap really is between Hub's
//! per-port-bitmap design and the Vyukov-style alternatives.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arbitro_kit::route::{Hub, Mpmc, Shutdown as HubShutdown};

fn duration_ms() -> u64 {
    std::env::var("BENCH_DURATION_MS").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(1000)
}

fn header() {
    println!("\n{:<40} {:>14} {:>14} {:>10}",
             "primitive / scenario", "msgs", "msgs/sec", "rel");
    println!("{}", "─".repeat(82));
}

fn row(name: &str, msgs: u64, msgs_per_sec: u64, baseline: Option<u64>) {
    let rel = match baseline {
        Some(b) if b > 0 => format!("{:.2}×", msgs_per_sec as f64 / b as f64),
        _ => "—".to_string(),
    };
    println!("{:<40} {:>14} {:>14} {:>10}", name, msgs, msgs_per_sec, rel);
}

// ─── arbitro Hub ───────────────────────────────────────────────────────────
fn run_hub(n: usize, dur: Duration) -> u64 {
    let (drain, ports) = Hub::<u64, ()>::new(n);
    let shutdown = drain.shutdown_handle();
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));

    let total_d = total.clone();
    let drain_h = thread::spawn(move || {
        drain.bind();
        loop {
            let mut count = 0u64;
            let res = drain.recv_batch(|_, _msg, _reply| { count += 1; });
            total_d.fetch_add(count, Ordering::Relaxed);
            if let Err(HubShutdown) = res { break; }
        }
    });

    let producer_handles: Vec<_> = ports.into_iter().map(|p| {
        let stop = stop.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = p.try_send(0u64);
            }
        })
    }).collect();

    thread::sleep(dur);
    stop.store(true, Ordering::Release);
    for h in producer_handles { h.join().unwrap(); }
    shutdown.signal();
    drain_h.join().unwrap();

    total.load(Ordering::Relaxed)
}

// ─── arbitro Mpmc ──────────────────────────────────────────────────────────
fn run_mpmc(n: usize, dur: Duration) -> u64 {
    // M producers, 1 consumer (single-shard so all producers funnel to one drain)
    let (producers, consumers, shutdown) = Mpmc::<u64, 64>::new(n, 1);
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));

    let consumer = consumers.into_iter().next().unwrap();
    let total_c = total.clone();
    let consumer_h = thread::spawn(move || {
        consumer.bind();
        loop {
            match consumer.recv_batch(|_v: u64| {}) {
                Ok(n) => { total_c.fetch_add(n as u64, Ordering::Relaxed); }
                Err(_) => break,
            }
        }
    });

    let producer_handles: Vec<_> = producers.into_iter().map(|p| {
        let stop = stop.clone();
        thread::spawn(move || {
            p.bind();
            while !stop.load(Ordering::Relaxed) {
                let _ = p.try_send(0u64);
            }
        })
    }).collect();

    thread::sleep(dur);
    stop.store(true, Ordering::Release);
    for h in producer_handles { h.join().unwrap(); }
    shutdown.signal();
    consumer_h.join().unwrap();

    total.load(Ordering::Relaxed)
}

// ─── crossbeam_channel::bounded ────────────────────────────────────────────
fn run_crossbeam(n: usize, cap: usize, dur: Duration) -> u64 {
    let (tx, rx) = crossbeam_channel::bounded::<u64>(cap);
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));

    let total_c = total.clone();
    let consumer_h = thread::spawn(move || {
        loop {
            match rx.recv() {
                Ok(_) => { total_c.fetch_add(1, Ordering::Relaxed); }
                Err(_) => break,
            }
        }
    });

    let producer_handles: Vec<_> = (0..n).map(|_| {
        let stop = stop.clone();
        let tx = tx.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = tx.try_send(0u64);
            }
        })
    }).collect();

    thread::sleep(dur);
    stop.store(true, Ordering::Release);
    for h in producer_handles { h.join().unwrap(); }
    drop(tx);  // close channel → consumer breaks
    consumer_h.join().unwrap();

    total.load(Ordering::Relaxed)
}

fn main() {
    println!("=== Head-to-head fan-in throughput ===");
    println!("duration={} ms (BENCH_DURATION_MS to override)", duration_ms());
    println!("Payload: u64. 1 consumer, N producers firing try_send loop.");

    let dur = Duration::from_millis(duration_ms());

    for &n in &[1usize, 2, 4, 8, 16, 32] {
        println!("\n┌─ N producers = {} ─", n);
        header();

        let hub_msgs = run_hub(n, dur);
        let hub_rate = (hub_msgs as f64 / dur.as_secs_f64()) as u64;
        row(&format!("arbitro Hub"), hub_msgs, hub_rate, None);

        let mpmc_msgs = run_mpmc(n, dur);
        let mpmc_rate = (mpmc_msgs as f64 / dur.as_secs_f64()) as u64;
        row(&format!("arbitro Mpmc (M={}, N=1, RING=64)", n),
            mpmc_msgs, mpmc_rate, Some(hub_rate));

        let cb_msgs = run_crossbeam(n, 64, dur);
        let cb_rate = (cb_msgs as f64 / dur.as_secs_f64()) as u64;
        row(&format!("crossbeam_channel::bounded(64)"),
            cb_msgs, cb_rate, Some(hub_rate));

        let cb_msgs2 = run_crossbeam(n, 1024, dur);
        let cb_rate2 = (cb_msgs2 as f64 / dur.as_secs_f64()) as u64;
        row(&format!("crossbeam_channel::bounded(1024)"),
            cb_msgs2, cb_rate2, Some(hub_rate));
    }

    println!("\nRel column: relative to arbitro Hub at the same N.");
    println!("Done.");
}
