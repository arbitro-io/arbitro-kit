//! Measures the average number of bits the drain finds set per `recv_batch`
//! wake under realistic cross-thread load.
//!
//! Why: the current `recv_batch` does one `lock_mask` atomic per bit. If the
//! drain typically wakes to find only 1 bit set, batching the clear is
//! pointless. If it wakes to find many bits, batch-clear could win ~30%.
//!
//! Setup: N producer threads fire `send` on their port as fast as possible.
//! 1 drain thread loops `recv_batch`. We count total messages drained and
//! total `recv_batch` calls; ratio = avg bits per wake.
//!
//! BENCH_DURATION_MS=<n> to override (default 1000 ms per scenario).
//! BENCH_PRODUCERS=<n>   to override fan-in degree (default sweep).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arbitro_kit::gate::{Hub, Shutdown};

fn duration_ms() -> u64 {
    std::env::var("BENCH_DURATION_MS").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(1000)
}

fn header() {
    println!("\n{:<28} {:>14} {:>14} {:>14} {:>14}",
             "scenario", "wakes", "msgs", "msgs/wake", "msgs/sec");
    println!("{}", "─".repeat(88));
}

fn run(n_ports: usize, n_producers: usize) {
    assert!(n_producers <= n_ports);
    let dur = Duration::from_millis(duration_ms());

    let (drain, ports) = Hub::<u64, ()>::new(n_ports);
    let shutdown = drain.shutdown_handle();
    let stop = Arc::new(AtomicBool::new(false));

    let total_msgs = Arc::new(AtomicU64::new(0));
    let total_wakes = Arc::new(AtomicU64::new(0));

    // Drain thread: count msgs and wakes.
    let drain_msgs = total_msgs.clone();
    let drain_wakes = total_wakes.clone();
    let drain_h = thread::spawn(move || {
        drain.bind();
        loop {
            let mut batch_count = 0u64;
            let res = drain.recv_batch(|_, _msg, _reply| {
                batch_count += 1;
            });
            drain_msgs.fetch_add(batch_count, Ordering::Relaxed);
            drain_wakes.fetch_add(1, Ordering::Relaxed);
            if let Err(Shutdown) = res { break; }
        }
    });

    // Producer threads: hammer their port. Use fire-and-forget — no recv_reply.
    // We need to clear the bit ourselves because there's no drain reply to
    // sync on. But recv_batch's callback already sees the message and the
    // bit is cleared by recv_batch — so the port can re-send when its bit
    // clears. We poll is_idle.
    let producer_handles: Vec<_> = ports.into_iter().take(n_producers).map(|p| {
        let stop = stop.clone();
        thread::spawn(move || {
            // No bind needed for fire-and-forget (no recv_reply).
            let mut k = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if p.try_send(k).is_ok() {
                    k = k.wrapping_add(1);
                }
                // If busy, retry — drain will catch up.
            }
            k
        })
    }).collect();

    // Run for `dur`, then stop.
    thread::sleep(dur);
    stop.store(true, Ordering::Release);

    let mut total_sent = 0u64;
    for h in producer_handles {
        total_sent += h.join().unwrap();
    }

    shutdown.signal();
    drain_h.join().unwrap();

    let msgs = total_msgs.load(Ordering::Relaxed);
    let wakes = total_wakes.load(Ordering::Relaxed);
    let msgs_per_wake = msgs as f64 / wakes as f64;
    let msgs_per_sec = msgs as f64 / dur.as_secs_f64();

    let scenario = format!("N={}, producers={}", n_ports, n_producers);
    println!("{:<28} {:>14} {:>14} {:>14.3} {:>14}",
             scenario, wakes, msgs, msgs_per_wake, msgs_per_sec as u64);

    // Sanity: msgs == total_sent (give or take a few in flight at stop).
    let lost = total_sent.saturating_sub(msgs);
    if lost > 100 {
        println!("  WARN: producers sent {} but drain only saw {} ({} in flight at stop)",
                 total_sent, msgs, lost);
    }
}

fn main() {
    println!("=== Hub multi-bit drain measurement ===");
    println!("duration={} ms (BENCH_DURATION_MS to override)", duration_ms());
    println!("Question: how many bits set per drain wake on average?");

    header();

    // Sweep producer count: as we add producers, msgs/wake should grow if
    // the drain is the bottleneck (multi-bit drains). If it stays ~1.0,
    // the drain wakes once per send and batching doesn't help.
    run(8, 1);
    run(8, 2);
    run(8, 4);
    run(8, 8);

    run(32, 1);
    run(32, 4);
    run(32, 16);
    run(32, 32);

    println!("\nInterpretation:");
    println!("  msgs/wake ≈ 1.0  → drain catches each msg individually; batch-clear useless.");
    println!("  msgs/wake ≫ 1.0  → drain coalesces many; batch-clear could save (msgs/wake-1)");
    println!("                     atomics per wake.");
    println!("\nDone.");
}
