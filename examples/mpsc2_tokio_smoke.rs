use arbitro_kit::route::{Mpsc2Async, MpscAsync};
use std::time::Instant;

const PER: u64 = 200_000;
const CAP: usize = 64;

async fn run_mpsc(m: usize) -> f64 {
    let (ps, mut c, sd) = MpscAsync::<usize, CAP>::new(m);
    let sd2 = sd.clone();
    let target = (m as u64) * PER;
    let consumer = tokio::spawn(async move {
        let mut got: u64 = 0;
        while got < target {
            match c.recv_async_send().await {
                Ok(_) => got += 1,
                Err(_) => break,
            }
        }
        got
    });
    let t0 = Instant::now();
    let handles: Vec<_> = ps
        .into_iter()
        .map(|p| {
            tokio::spawn(async move {
                for k in 0..PER {
                    p.send_async_send(k as usize).await;
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    consumer.await.unwrap();
    let el = t0.elapsed().as_secs_f64() * 1000.0;
    sd2.signal();
    drop(sd);
    el
}

async fn run_mpsc2(m: usize) -> f64 {
    let (ps, mut c, sd) = Mpsc2Async::<usize, CAP>::new(m);
    let sd2 = sd.clone();
    let target = (m as u64) * PER;
    let consumer = tokio::spawn(async move {
        let mut got: u64 = 0;
        while got < target {
            match c.recv_async_send().await {
                Ok(_) => got += 1,
                Err(_) => break,
            }
        }
        got
    });
    let t0 = Instant::now();
    let handles: Vec<_> = ps
        .into_iter()
        .map(|mut p| {
            tokio::spawn(async move {
                for k in 0..PER {
                    p.send_async_send(k as usize).await;
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    consumer.await.unwrap();
    let el = t0.elapsed().as_secs_f64() * 1000.0;
    sd2.signal();
    drop(sd);
    el
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    println!(
        "Tokio single-item comparison (PER={} per producer, CAP={}, worker_threads=8)",
        PER, CAP
    );
    println!(
        "{:<16} {:>4} {:>14} {:>14} {:>14}",
        "impl", "M", "wall ms", "M msgs/s", "vs baseline"
    );
    println!("{}", "─".repeat(66));
    for m in [1usize, 2, 4, 8] {
        let target = (m as u64) * PER;
        // MpscAsync
        let t = tokio::time::timeout(std::time::Duration::from_secs(60), run_mpsc(m))
            .await
            .expect("MpscAsync timeout");
        let mpsc_rate = (target as f64) / (t / 1000.0) / 1e6;
        println!("{:<16} {:>4} {:>14.2} {:>14.2} {:>14}", "MpscAsync", m, t, mpsc_rate, "1.00×");
        // Mpsc2Async
        let t2 = tokio::time::timeout(std::time::Duration::from_secs(60), run_mpsc2(m))
            .await
            .expect("Mpsc2Async timeout");
        let mpsc2_rate = (target as f64) / (t2 / 1000.0) / 1e6;
        let ratio = mpsc2_rate / mpsc_rate;
        println!("{:<16} {:>4} {:>14.2} {:>14.2} {:>13.2}×", "Mpsc2Async", m, t2, mpsc2_rate, ratio);
        println!();
    }
}
