//! Probe: simulate Duplex's bidirectional pattern using TWO `Signal`s
//! (instead of two `Park`s + Stream segments). Tests whether the Dekker
//! race we observed in Duplex is specific to Park or inherent to the
//! park/wake protocol — Signal uses the same protocol, so it should
//! deadlock the same way.
//!
//! Pattern (per round, 1M lockstep RPCs):
//!   - 2 × Signal (a→b "data ready" and b→a "data ready").
//!   - Cursors are `AtomicU64`, played by the producer/consumer.
//!   - main:   inc cursor_a; sig_a.release(); sig_b.acquire(); read cursor_b
//!   - worker: sig_a.acquire(); read cursor_a; inc cursor_b; sig_b.release()
//!
//! 5 rounds × 1M iterations. If any round hangs ≥ 5s, the race fired.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::gate::Signal;

const N: u64 = 1_000_000;
const ROUNDS: usize = 5;

fn flush() {
    let _ = std::io::stderr().flush();
}

fn main() {
    eprintln!("=== Signal-based Duplex probe — {} rounds at N={} ===", ROUNDS, N);
    flush();

    for round in 0..ROUNDS {
        let start = Instant::now();

        let sig_a: Arc<Signal> = Arc::new(Signal::new());
        let sig_b: Arc<Signal> = Arc::new(Signal::new());
        let cur_a: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
        let cur_b: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

        let sa_w = sig_a.clone();
        let sb_w = sig_b.clone();
        let ca_w = cur_a.clone();
        let cb_w = cur_b.clone();

        let h = thread::spawn(move || {
            sa_w.set_worker(thread::current());
            for _ in 0..N {
                sa_w.acquire();
                sa_w.lock();   // consume the signal so next acquire blocks
                let v = ca_w.load(Ordering::Acquire);
                cb_w.store(v.wrapping_mul(2) | 1, Ordering::Release);
                sb_w.release();
            }
        });

        sig_b.set_worker(thread::current());
        let mut sum = 0u64;
        for i in 0..N {
            cur_a.store(i, Ordering::Release);
            sig_a.release();
            sig_b.acquire();
            sig_b.lock();   // consume the signal
            sum = sum.wrapping_add(cur_b.load(Ordering::Acquire));
        }
        h.join().unwrap();

        let dur = start.elapsed();
        eprintln!(
            "round {:>2} : {:?}  ({:.1} ns/RT)  sum={}",
            round, dur, dur.as_nanos() as f64 / N as f64, sum
        );
        flush();
    }
    eprintln!("=== all {} rounds done ===", ROUNDS);
}
