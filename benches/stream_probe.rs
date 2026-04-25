//! Stream<T> standalone probe — 10 rounds of one-way N=1M.
//!
//! No Duplex, no wrappers. Just a single Stream<u64> end-to-end, to
//! isolate whether the deadlock observed in Duplex is a Stream bug or
//! a Duplex-specific issue.
//!
//! Each round prints elapsed time. Expected: ~5-10 ms per round on a
//! warm machine. If any round takes > 1 s, that's a hang.

use std::sync::Arc;
use std::thread;
use std::time::Instant;
use std::io::Write;

use arbitro_kit::stream::Stream;

const N: u64 = 1_000_000;
const ROUNDS: usize = 10;

fn flush() {
    let _ = std::io::stderr().flush();
}

fn main() {
    eprintln!("=== Stream<u64> one-way probe — {} rounds at N={} ===", ROUNDS, N);
    flush();

    for round in 0..ROUNDS {
        let start = Instant::now();
        let s: Arc<Stream<u64>> = Arc::new(Stream::new());
        let s2 = s.clone();
        let h = thread::spawn(move || {
            s2.set_consumer(thread::current());
            let mut sum = 0u64;
            for _ in 0..N { sum = sum.wrapping_add(s2.recv()); }
            sum
        });
        for i in 0..N { s.send(i); }
        let _ = h.join().unwrap();
        let dur = start.elapsed();
        eprintln!("round {:>2} : {:?}  ({:.1} ns/op)", round, dur, dur.as_nanos() as f64 / N as f64);
        flush();
    }
    eprintln!("=== all {} rounds done ===", ROUNDS);
}
