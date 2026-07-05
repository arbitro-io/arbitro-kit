//! Actor pool — fan-out + fan-in composed from `Pipe` and `Hub`.
//!
//! Topology:
//!
//! ```text
//!                 main thread (dispatcher)
//!                 ──────────────────────
//!                 round-robin over job pipes
//!                           │
//!       ┌───────────┬───────┴───────┬───────────┐
//!       ▼           ▼               ▼           ▼
//!   Pipe<Job>   Pipe<Job>      Pipe<Job>    Pipe<Job>
//!       │           │               │           │
//!     worker_0   worker_1         worker_2    worker_3
//!       │           │               │           │
//!   HubPort<R>  HubPort<R>      HubPort<R>   HubPort<R>
//!        ╲          │             │          ╱
//!         ╲         │             │         ╱
//!          ────────── Hub drain (sink) ─────
//!                           │
//!                    verify + tally
//! ```
//!
//! The fan-OUT (1 dispatcher → N workers) is N independent `Pipe`s — one
//! per worker, the dispatcher owns all senders and picks them round-robin.
//! The fan-IN (N workers → 1 sink) is a single `Hub` with N ports.
//!
//! This is the canonical "actor pool" layout: bounded work queues with
//! natural backpressure (each pipe holds at most one job), zero-copy
//! payload transfer, and clean shutdown via `HubShutdown`.
//!
//! Run:
//!   cargo run --example actor_pool --release

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use arbitro_kit::gate::{Hub, Pipe, Shutdown};

const WORKERS: usize = 4;
const JOBS: u64 = 100_000;

/// Unit of work. In a real system this would carry a payload buffer,
/// a subject, credits, etc. Kept to a single u64 so a torn-read bug in
/// the Pipe layer cannot disguise itself as a logic bug here.
type Job = u64;

/// Result emitted by a worker. `(job_id, worker_idx, computed)` — the
/// sink uses `job_id` to verify and `worker_idx` to tally per-port load.
#[derive(Debug)]
struct Reply {
    job_id: u64,
    worker: usize,
    computed: u64,
}

fn main() {
    // ── Wire the topology ───────────────────────────────────────────
    // One Pipe per worker for inbound jobs. The dispatcher (main) owns
    // all of them; each worker takes a `.clone()` of one.
    let job_pipes: Vec<Arc<Pipe<Job>>> = (0..WORKERS).map(|_| Arc::new(Pipe::new())).collect();

    // One Hub with WORKERS ports for outbound replies. Workers use
    // `port.call(reply)` (not `send`) so each worker blocks until the
    // drain acks — this is the backpressure that keeps the drain from
    // being lapped. `Out = ()` because the ack carries no payload.
    let (drain, ports) = Hub::<Reply, ()>::new(WORKERS);
    let shutdown = drain.shutdown_handle();

    // ── Sink thread: drains the Hub, verifies results ───────────────
    let sink = thread::spawn(move || {
        drain.bind();
        let mut received: u64 = 0;
        let mut per_worker = vec![0u64; WORKERS];
        let mut sum_checked: u64 = 0;
        loop {
            match drain.recv_batch(|port_idx, reply, ack| {
                // Verify: worker doubled the value.
                assert_eq!(
                    reply.computed,
                    reply.job_id * 2,
                    "worker {} returned wrong value",
                    port_idx
                );
                assert_eq!(reply.worker, port_idx, "port_idx / worker label mismatch");
                per_worker[port_idx] += 1;
                received += 1;
                sum_checked += reply.computed;
                // Ack the worker so it can submit the next reply. This is
                // what turns the Hub into a pipelined N:1 with backpressure.
                ack.send(());
            }) {
                Ok(()) if received >= JOBS => {
                    // All jobs accounted for — exit the loop.
                    return (received, per_worker, sum_checked);
                }
                Ok(()) => continue,
                Err(Shutdown) => return (received, per_worker, sum_checked),
            }
        }
    });

    // ── Worker threads: recv Job → compute → send Reply ─────────────
    let mut worker_handles = Vec::with_capacity(WORKERS);
    for (idx, (inbox, port)) in job_pipes.iter().cloned().zip(ports).enumerate() {
        worker_handles.push(thread::spawn(move || {
            // Register both the inbound pipe and the outbound port on
            // this worker's thread. Each primitive is bound to exactly
            // one consumer / producer thread respectively.
            inbox.set_consumer(thread::current());
            port.bind();

            loop {
                let job: Job = inbox.recv();
                if job == u64::MAX {
                    break;
                } // shutdown sentinel

                // Simulated work: double the value.
                let reply = Reply {
                    job_id: job,
                    worker: idx,
                    computed: job.wrapping_mul(2),
                };
                // Blocking round-trip: drain's `ack.send(())` unblocks us.
                let _ack: () = port.call(reply);
            }
        }));
    }

    // ── Dispatcher (main): round-robin jobs across the worker pipes ─
    // We can't `send` into a `Pipe` that still has an unread value — it
    // would overwrite the slot. In a single-slot SPSC world the easiest
    // backpressure is: spin until the target pipe is empty.
    let t0 = Instant::now();
    for id in 0..JOBS {
        let target = &job_pipes[(id as usize) % WORKERS];
        while target.has_data() {
            std::hint::spin_loop();
        }
        target.send(id as Job);
    }

    // Shutdown sentinels per worker.
    for p in &job_pipes {
        while p.has_data() {
            std::hint::spin_loop();
        }
        p.send(u64::MAX);
    }

    // ── Teardown ────────────────────────────────────────────────────
    // Wait for workers so all replies have reached the Hub before we
    // signal the sink to exit.
    for h in worker_handles {
        h.join().unwrap();
    }
    // All replies are now in-flight or already drained. Give the sink
    // a chance to see them before we force-shut.
    shutdown.signal();

    let (received, per_worker, sum) = sink.join().unwrap();
    let elapsed = t0.elapsed();

    println!("=== actor_pool ===");
    println!("workers       : {}", WORKERS);
    println!("jobs          : {}", JOBS);
    println!("received      : {}", received);
    println!("per-worker    : {:?}", per_worker);
    println!("sum(2*0..N)   : {} (expected {})", sum, JOBS * (JOBS - 1));
    println!("elapsed       : {:?}", elapsed);
    println!(
        "throughput    : {:.2} M jobs/s",
        JOBS as f64 / elapsed.as_secs_f64() / 1e6
    );

    assert_eq!(received, JOBS, "sink missed replies");
    assert_eq!(sum, JOBS * (JOBS - 1), "sum mismatch");
    for (i, &count) in per_worker.iter().enumerate() {
        assert_eq!(
            count,
            JOBS / WORKERS as u64,
            "worker {} processed {} (expected uniform)",
            i,
            count
        );
    }
    println!("OK");
}
