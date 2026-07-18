//! Memory footprint of `Mpmc` at scale.
//!
//! Constructs `Mpmc<T, RING_CAP>` with `n = 10_000` consumers and varying
//! `m` (producers). Reports the resident-set delta via `/proc/self/status`
//! (Linux/WSL) plus an analytical estimate for cross-check.
//!
//! Run from WSL:
//! ```
//! wsl bash -lc "cd '/mnt/.../arbitro-kit' && cargo run --release --example mpmc_memory_10k"
//! ```

// The header comment draws an ASCII layout tree, not a strict markdown list.
#![allow(clippy::doc_overindented_list_items, clippy::doc_lazy_continuation)]

use std::fs;

use arbitro_kit::route::Mpmc;

fn rss_kb() -> Option<u64> {
    let s = fs::read_to_string("/proc/self/status").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            // Format: "VmRSS:\t   12345 kB"
            let tok = rest.split_whitespace().next()?;
            return tok.parse().ok();
        }
    }
    None
}

fn fmt_bytes(b: i64) -> String {
    let abs = b.unsigned_abs() as f64;
    let sign = if b < 0 { "-" } else { "" };
    if abs >= 1024.0 * 1024.0 * 1024.0 {
        format!("{}{:.2} GB", sign, abs / (1024.0 * 1024.0 * 1024.0))
    } else if abs >= 1024.0 * 1024.0 {
        format!("{}{:.1} MB", sign, abs / (1024.0 * 1024.0))
    } else if abs >= 1024.0 {
        format!("{}{:.1} KB", sign, abs / 1024.0)
    } else {
        format!("{}{} B", sign, b)
    }
}

/// Analytical model — must match the layout in src/route/mpmc.rs.
///
/// Per shard:
///   - `Box<[PRing<T, RING_CAP>]>` of length M
///     - PRing: 64 (head + pad) + 64 (tail + pad) + 16 (slots Box ptr+len)
///                    + RING_CAP * sizeof(MaybeUninit<T> in UnsafeCell)
///   - consumer_waiter: ParkWaiter (#[repr(align(64))]) → 64 B
///   - m: usize → 8 B
/// Total inner = MpmcInner header + N × shard + M × ParkWaiter (producer waiters)
fn analytical(m: usize, n: usize, ring_cap: usize, t_bytes: usize) -> u64 {
    let pring = 64 + 64 + 24 + ring_cap * t_bytes; // approx
    let shard = m * pring + 64 /* waiter */ + 24 /* shard hdr + slice ptr */;
    let producer_waiters = m * 64;
    let inner_hdr = 64;
    (n * shard + producer_waiters + inner_hdr) as u64
}

fn run<const RING_CAP: usize>(m: usize, n: usize) {
    let label = format!("M={}, N={}, RING_CAP={}, T=u64", m, n, RING_CAP);
    let predicted = analytical(m, n, RING_CAP, 8);

    let before = rss_kb().unwrap_or(0);

    // Build and HOLD so we measure resident memory while alive.
    let (producers, consumers, shutdown) = Mpmc::<u64, RING_CAP>::new(m, n);

    // Force first-touch on every page so RSS reflects committed memory,
    // not lazy zero pages.  Touching each consumer's first ring is enough.
    let mut tot = 0u64;
    for c in &consumers {
        tot = tot.wrapping_add(c.shard() as u64);
    }
    std::hint::black_box(tot);

    let after = rss_kb().unwrap_or(0);
    let delta_bytes = (after as i64 - before as i64) * 1024;

    println!("─── {} ───", label);
    println!("  predicted (analytical):  {}", fmt_bytes(predicted as i64));
    println!("  measured (RSS delta):    {}", fmt_bytes(delta_bytes));
    println!(
        "  per consumer (predicted): {}",
        fmt_bytes((predicted / n as u64) as i64)
    );
    println!(
        "  producers / consumers:    {} / {}",
        producers.len(),
        consumers.len()
    );
    println!();

    // Drop explicitly to free before the next scenario.
    drop(producers);
    drop(consumers);
    drop(shutdown);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // If invoked with --scenario K, run only that scenario (fresh process).
    if args.len() == 3 && args[1] == "--scenario" {
        let k: usize = args[2].parse().unwrap();
        match k {
            0 => run::<64>(8, 10_000),
            1 => run::<64>(4, 10_000),
            2 => run::<64>(1, 10_000),
            3 => run::<4>(1, 10_000),
            4 => run::<2>(1, 10_000),
            _ => {}
        }
        return;
    }

    println!("Mpmc memory footprint, N=10_000 consumers, payload T=u64 (8 B)");
    println!("RSS measured per scenario in a fresh process to avoid allocator caching.\n");

    let exe = std::env::current_exe().unwrap();
    for k in 0..5 {
        let status = std::process::Command::new(&exe)
            .arg("--scenario")
            .arg(k.to_string())
            .status()
            .unwrap();
        if !status.success() {
            eprintln!("scenario {k} failed");
        }
    }
}
