//! Duplex<A, B> standalone probe — 10 rounds of lockstep RPC at N=100K.
//!
//! Mirrors `stream_probe.rs` shape: 10 rounds, single iteration each,
//! eprintln per round so a hang is visible immediately.
//!
//! Expected: each round ~15-20 ms (lockstep RPC at ~150 ns/RT × 100K).
//! If any round takes > 1 s, that's a hang.

use std::io::Write;
use std::thread;
use std::time::Instant;

use arbitro_kit::stream::Duplex;

const N: u64 = 1_000_000;
const ROUNDS: usize = 10;

fn flush() {
    let _ = std::io::stderr().flush();
}

fn main() {
    eprintln!("=== Duplex lockstep probe — {} rounds at N={} ===", ROUNDS, N);
    flush();

    for round in 0..ROUNDS {
        let start = Instant::now();
        let (a, b) = Duplex::<u64, u64>::pair();

        let h = thread::spawn(move || {
            b.set_consumer(thread::current());
            for _ in 0..N {
                let v = b.recv();
                b.send(v.wrapping_mul(2) | 1);
            }
        });

        a.set_consumer(thread::current());
        let mut sum = 0u64;
        for i in 0..N {
            a.send(i);
            sum = sum.wrapping_add(a.recv());
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
