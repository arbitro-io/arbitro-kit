//! Stress harness for `Mpsc` — 10 iterations, 10s watchdog each.
//!
//! Goal: verify no hang / deadlock / lost-wake at various M values.
//! Each iteration:
//!   - spawns M producers × PER msgs
//!   - one consumer draining M×PER
//!   - watchdog thread panics if the iteration doesn't complete in 10s
//!
//! If ANY iteration hangs → the whole test fails with a panic from the
//! watchdog.

use arbitro_kit::route::{Mpsc, Shutdown};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const ITERS: usize = 30;
const PER: u64 = 50_000;
const CAP: usize = 64;
const WATCHDOG: Duration = Duration::from_secs(10);

fn run_once(m: usize, iter: usize) {
    let done = Arc::new(AtomicBool::new(false));
    let done_wd = done.clone();
    let start = Instant::now();

    // Watchdog — panics if the iteration doesn't complete before WATCHDOG.
    let watchdog = thread::spawn(move || {
        while !done_wd.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(200));
            if start.elapsed() > WATCHDOG {
                // Loud, unambiguous failure.
                eprintln!(
                    "\n╔══ HANG DETECTED ══════════════════════════════════════╗\
                     \n║  Iteration {} (m={}) did not complete in {:?}.\
                     \n║  Very likely deadlock / lost-wake in Mpsc.\
                     \n╚═══════════════════════════════════════════════════════╝",
                    iter, m, WATCHDOG,
                );
                std::process::abort();
            }
        }
    });

    let (ps, mut c, sd) = Mpsc::<u64, CAP>::new(m);
    let sd2 = sd.clone();
    let barrier = Arc::new(Barrier::new(m + 1));
    let target = (m as u64) * PER;

    let consumer_h = {
        let b = barrier.clone();
        thread::spawn(move || {
            c.bind();
            b.wait();
            let mut got: u64 = 0;
            while got < target {
                match c.recv_batch(|_| got += 1) {
                    Ok(_) => {}
                    Err(Shutdown) => break,
                }
            }
            got
        })
    };

    let handles: Vec<_> = ps
        .into_iter()
        .map(|mut p| {
            let b = barrier.clone();
            thread::spawn(move || {
                p.bind();
                b.wait();
                for k in 0..PER {
                    p.send(k);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    let got = consumer_h.join().unwrap();
    sd2.signal();
    drop(sd);

    assert_eq!(got, target, "iter {}, m={}: lost messages", iter, m);

    // Signal the watchdog we're done.
    done.store(true, Ordering::Release);
    watchdog.join().unwrap();

    let elapsed = start.elapsed();
    println!("iter {:2} m={:2}  {} msgs in {:?}", iter, m, target, elapsed);
}

#[test]
fn mpsc_stress_10_iterations_no_hang() {
    // Sweep M each iteration so we hit the full range.
    let ms = [1usize, 2, 4, 8, 16];
    for i in 0..ITERS {
        run_once(ms[i % ms.len()], i);
    }
    println!("\nAll {} iterations completed cleanly.", ITERS);
}
