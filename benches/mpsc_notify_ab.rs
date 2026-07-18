//! Mpsc<NotifyWaiter> throughput — the PRODUCTION config (server's NotifyRing).
//!
//! 1 producer OS-thread -> Mpsc fan-in -> 1 tokio-task consumer, mirroring the
//! server's drain(OS-thread) -> command(tokio-task) hand-off. This is the config
//! R1 (unconditional fan-in wake) must NOT regress; the mpsc_overhead bench uses
//! ParkWaiter, which is NOT what production uses.
//!
//!   cargo bench --bench mpsc_notify_ab --features tokio

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use arbitro_kit::route::Mpsc;
use arbitro_kit::NotifyWaiter;

const N: u64 = 5_000_000;
const CAP: usize = 8192;
const ROUNDS: usize = 7;

fn one_round(rt: &tokio::runtime::Runtime) -> f64 {
    let (mut prods, mut consumer, _sd) = Mpsc::<u64, CAP, NotifyWaiter>::new(1);
    let mut prod = prods.pop().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let bc = barrier.clone();

    let consumer_handle = rt.spawn(async move {
        let mut got = 0u64;
        let mut acc = 0u64;
        while got < N {
            match consumer.recv_async_send().await {
                Ok(v) => {
                    acc ^= v;
                    got += 1;
                }
                Err(_) => break,
            }
            // Batch-drain whatever else is queued (same as the server's
            // try_recv drain after the async wake).
            while got < N {
                match consumer.try_recv() {
                    Some(v) => {
                        acc ^= v;
                        got += 1;
                    }
                    None => break,
                }
            }
        }
        black_box(acc);
    });

    let ph = std::thread::spawn(move || {
        bc.wait();
        for i in 0..N {
            while prod.try_send(i).is_err() {
                std::thread::yield_now();
            }
        }
    });

    barrier.wait();
    let start = Instant::now();
    ph.join().unwrap();
    rt.block_on(consumer_handle).unwrap();
    start.elapsed().as_nanos() as f64 / N as f64
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _ = one_round(&rt); // warm
    let mut s: Vec<f64> = (0..ROUNDS).map(|_| one_round(&rt)).collect();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!("Mpsc<NotifyWaiter> 1P(os-thread) -> 1C(tokio-task), N={N}, CAP={CAP}, rounds={ROUNDS}");
    println!(
        "  min={:.2}  p50={:.2}  max={:.2}  ns/msg   ({:.1} M msg/s p50)",
        s[0],
        s[s.len() / 2],
        s[s.len() - 1],
        1000.0 / s[s.len() / 2]
    );
}
