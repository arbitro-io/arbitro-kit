//! Focused benchmark for **wake primitives** only.
//!
//! Scope: measure the cost of "tell a waiting thread there's work".
//! Channel-shaped tests live in `channel_overhead.rs`.
//!
//! Primitives under test:
//!   - `Signal`                        — `ParkWaiter` + `AtomicBool` open-flag,
//!                                       i.e. the gate-shaped composition that
//!                                       used to live in `gate::Signal` before
//!                                       it was folded into `ParkWaiter`. Built
//!                                       inline so this bench keeps measuring
//!                                       the same Dekker-safe wake path.
//!   - `AtomicBool + spin`             — coherence-floor baseline, no park.
//!   - `AtomicBool + park/unpark`      — manual Dekker with std's thread park.
//!   - `Mutex<bool> + Condvar`         — idiomatic std wake.
//!   - `crossbeam Parker`              — `crossbeam_utils::sync::Parker`,
//!                                       token-based park/unpark (same shape
//!                                       as std but decoupled from the
//!                                       thread handle).
//!
//! Scenarios (each primitive runs all three):
//!   - **ST** (single-thread)        — release + acquire on the same thread;
//!                                     pure fast-path, no cross-core traffic.
//!   - **XT hot** (cross-thread hot) — consumer is in its spin window when the
//!                                     producer fires. Measures spin-catch.
//!   - **XT parked** (cross-thread)  — consumer is deep in `park()` when the
//!                                     producer fires (500µs pre-sleep). Measures
//!                                     real wake-from-park latency.
//!
//! Columns:
//!   - p50_ns, p99_ns — latency of one release→acquire round.
//!   - ops/sec        — rounds per second.
//!
//! Run: `cargo bench --bench gate_overhead`
//! Env: `BENCH_ROUNDS=1000 BENCH_WARMUP=100`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arbitro_kit::gate::OneSignal;
use arbitro_kit::waiter::{BlockingWaiter, ParkWaiter, Waiter};
use crossbeam_channel::bounded as cb_bounded;
use crossbeam_utils::sync::Parker as CbParker;

// ─── `Signal` shim ─────────────────────────────────────────────────────────
//
// Restores the gate-shaped `release/acquire/lock` API on top of the
// surviving primitive (`ParkWaiter`). Equivalent to the deleted
// `gate::Signal`: a `ParkWaiter` + an `AtomicBool` "open" flag.
//
// This is the exact composition any caller would write today — the bench
// measures it as a baseline against `Mutex+Condvar`, `crossbeam Parker`,
// etc. so the migration didn't lose the measurement.
struct Signal {
    waiter: ParkWaiter,
    open: AtomicBool,
}

impl Signal {
    fn new() -> Self {
        Self {
            waiter: ParkWaiter::default(),
            open: AtomicBool::new(false),
        }
    }
    fn with_spin(spin: u32) -> Self {
        Self {
            waiter: ParkWaiter::with_spin(spin),
            open: AtomicBool::new(false),
        }
    }
    #[inline]
    fn set_worker(&self, t: thread::Thread) {
        self.waiter.set_worker(t);
    }
    /// Producer side: open the gate and wake the consumer if parked.
    #[inline]
    fn release(&self) {
        self.open.store(true, Ordering::Release);
        self.waiter.wake();
    }
    /// Consumer side: block until the gate is open.
    #[inline]
    fn acquire(&self) {
        self.waiter.wait_until(|| self.open.load(Ordering::Acquire));
    }
    /// Consumer side: claim the open flag (close it for the next round).
    #[inline]
    fn lock(&self) {
        self.open.store(false, Ordering::Relaxed);
    }
}

fn rounds() -> usize {
    std::env::var("BENCH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
}
fn warmup() -> usize {
    std::env::var("BENCH_WARMUP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
}

/// Pre-fire sleep for the XT-parked scenario. 500µs is well beyond any
/// reasonable spin budget so the consumer is guaranteed deep in `park()`.
const XT_PARKED_PRE_SLEEP: Duration = Duration::from_micros(500);

// ─── output helpers ────────────────────────────────────────────────────────

struct Row {
    primitive: &'static str,
    p50_ns: u64,
    p99_ns: u64,
    ops_per_sec: u64,
}

fn print_scenario_header(name: &str) {
    println!("\n── {} ──", name);
    println!(
        "{:<32} {:>10} {:>10} {:>14}",
        "primitive", "p50_ns", "p99_ns", "ops/sec"
    );
    println!("{}", "─".repeat(70));
}

fn print_row(r: Row) {
    println!(
        "{:<32} {:>10} {:>10} {:>14}",
        r.primitive, r.p50_ns, r.p99_ns, r.ops_per_sec
    );
}

fn finish(primitive: &'static str, mut lats: Vec<u64>, elapsed_ns: u64) -> Row {
    lats.sort_unstable();
    let n = lats.len();
    let ops = (n as f64) / (elapsed_ns as f64 / 1e9);
    Row {
        primitive,
        p50_ns: lats[n / 2],
        p99_ns: lats[n * 99 / 100],
        ops_per_sec: ops as u64,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SINGLE-THREAD runners — one thread releases and immediately acquires.
// No cross-core traffic; the atomic stays in the caller's L1.
// ═══════════════════════════════════════════════════════════════════════════

fn st_signal() -> Row {
    let s = Signal::new();
    s.set_worker(thread::current());
    for _ in 0..warmup() {
        s.release();
        s.acquire();
        s.lock();
    }
    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for _ in 0..rounds() {
        let t0 = Instant::now();
        s.release();
        s.acquire();
        s.lock();
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;
    finish("Signal", lats, el)
}

fn st_atomic_spin() -> Row {
    let flag = AtomicBool::new(false);
    for _ in 0..warmup() {
        flag.store(true, Ordering::Release);
        while !flag.load(Ordering::Acquire) {}
        flag.store(false, Ordering::Release);
    }
    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for _ in 0..rounds() {
        let t0 = Instant::now();
        flag.store(true, Ordering::Release);
        while !flag.load(Ordering::Acquire) {}
        flag.store(false, Ordering::Release);
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;
    finish("AtomicBool + spin", lats, el)
}

fn st_atomic_park() -> Row {
    // On a single thread there's no one to unpark; simulate the Dekker
    // fast-path: set flag, load flag, clear flag. No actual park() call
    // — the flag is always observed set on the first load.
    let flag = AtomicBool::new(false);
    for _ in 0..warmup() {
        flag.store(true, Ordering::Release);
        if !flag.load(Ordering::Acquire) {
            thread::park();
        }
        flag.store(false, Ordering::Release);
    }
    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for _ in 0..rounds() {
        let t0 = Instant::now();
        flag.store(true, Ordering::Release);
        if !flag.load(Ordering::Acquire) {
            thread::park();
        }
        flag.store(false, Ordering::Release);
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;
    finish("AtomicBool + park/unpark", lats, el)
}

fn st_condvar() -> Row {
    let m = Mutex::new(false);
    let cv = Condvar::new();
    for _ in 0..warmup() {
        {
            let mut g = m.lock().unwrap();
            *g = true;
            cv.notify_one();
        }
        {
            let mut g = m.lock().unwrap();
            while !*g {
                g = cv.wait(g).unwrap();
            }
            *g = false;
        }
    }
    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for _ in 0..rounds() {
        let t0 = Instant::now();
        {
            let mut g = m.lock().unwrap();
            *g = true;
            cv.notify_one();
        }
        {
            let mut g = m.lock().unwrap();
            while !*g {
                g = cv.wait(g).unwrap();
            }
            *g = false;
        }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;
    finish("Mutex<bool> + Condvar", lats, el)
}

fn st_cb_parker() -> Row {
    // Parker's `park()` blocks until `unpark()` is called. On single thread
    // with `unpark()` fired before `park()`, park returns immediately (the
    // token is latched). This mirrors the single-thread fast-path.
    let p = CbParker::new();
    let u = p.unparker().clone();
    for _ in 0..warmup() {
        u.unpark();
        p.park();
    }
    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for _ in 0..rounds() {
        let t0 = Instant::now();
        u.unpark();
        p.park();
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;
    finish("crossbeam Parker", lats, el)
}

// ═══════════════════════════════════════════════════════════════════════════
// CROSS-THREAD HOT runners — consumer is spinning (or just about to park)
// when producer fires. Best case for primitives with spin windows.
// ═══════════════════════════════════════════════════════════════════════════

fn xt_hot_signal() -> Row {
    let sig = Arc::new(Signal::new());
    let ready = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let round_nr = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let s = sig.clone();
    let r = ready.clone();
    let d = done.clone();
    let rn = round_nr.clone();
    let h = thread::spawn(move || {
        s.set_worker(thread::current());
        r.store(true, Ordering::Release);
        let mut seen = 0usize;
        while !d.load(Ordering::Relaxed) {
            s.acquire();
            s.lock();
            seen += 1;
            rn.store(seen, Ordering::Release);
        }
    });
    while !ready.load(Ordering::Acquire) {
        thread::yield_now();
    }

    let total = warmup() + rounds();
    let mut lats = Vec::with_capacity(rounds());
    let t_wall_start_round = warmup();
    let mut t_wall = Instant::now();

    for i in 0..total {
        // Fire immediately — consumer is either in spin window or just parked
        // from the last round's `lock()`. Measures spin-catch path.
        let expect = i + 1;
        if i == t_wall_start_round {
            t_wall = Instant::now();
        }
        let t0 = Instant::now();
        sig.release();
        while round_nr.load(Ordering::Acquire) < expect {
            std::hint::spin_loop();
        }
        if i >= t_wall_start_round {
            lats.push(t0.elapsed().as_nanos() as u64);
        }
    }
    let el = t_wall.elapsed().as_nanos() as u64;

    done.store(true, Ordering::Relaxed);
    sig.release();
    h.join().unwrap();
    finish("Signal", lats, el)
}

fn xt_hot_atomic_spin() -> Row {
    // Two flags: producer→consumer (go), consumer→producer (ack).
    let go = Arc::new(AtomicBool::new(false));
    let ack = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));

    let g = go.clone();
    let a = ack.clone();
    let d = done.clone();
    let h = thread::spawn(move || loop {
        while !g.load(Ordering::Acquire) {
            if d.load(Ordering::Relaxed) {
                return;
            }
        }
        g.store(false, Ordering::Relaxed);
        a.store(true, Ordering::Release);
    });

    let total = warmup() + rounds();
    let mut lats = Vec::with_capacity(rounds());
    let t_wall_start_round = warmup();
    let mut t_wall = Instant::now();

    for i in 0..total {
        if i == t_wall_start_round {
            t_wall = Instant::now();
        }
        let t0 = Instant::now();
        go.store(true, Ordering::Release);
        while !ack.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        ack.store(false, Ordering::Relaxed);
        if i >= t_wall_start_round {
            lats.push(t0.elapsed().as_nanos() as u64);
        }
    }
    let el = t_wall.elapsed().as_nanos() as u64;

    done.store(true, Ordering::Relaxed);
    go.store(true, Ordering::Release);
    h.join().unwrap();
    finish("AtomicBool + spin", lats, el)
}

fn xt_hot_atomic_park() -> Row {
    let go = Arc::new(AtomicBool::new(false));
    let parked = Arc::new(AtomicBool::new(false));
    let ack = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let worker = Arc::new(Mutex::new(None::<thread::Thread>));

    let g = go.clone();
    let p = parked.clone();
    let a = ack.clone();
    let d = done.clone();
    let w = worker.clone();
    let h = thread::spawn(move || {
        *w.lock().unwrap() = Some(thread::current());
        loop {
            // Tight spin window (match Signal's TIGHT_SPIN = 64).
            let mut spun = 0;
            while !g.load(Ordering::Acquire) {
                if d.load(Ordering::Relaxed) {
                    return;
                }
                spun += 1;
                if spun >= 64 {
                    break;
                }
            }
            if !g.load(Ordering::Acquire) {
                p.store(true, Ordering::SeqCst);
                while !g.load(Ordering::Acquire) {
                    if d.load(Ordering::Relaxed) {
                        return;
                    }
                    thread::park();
                }
                p.store(false, Ordering::Relaxed);
            }
            g.store(false, Ordering::Relaxed);
            a.store(true, Ordering::Release);
        }
    });
    // wait for worker registered
    loop {
        if worker.lock().unwrap().is_some() {
            break;
        }
        thread::yield_now();
    }

    let total = warmup() + rounds();
    let mut lats = Vec::with_capacity(rounds());
    let t_wall_start_round = warmup();
    let mut t_wall = Instant::now();

    for i in 0..total {
        if i == t_wall_start_round {
            t_wall = Instant::now();
        }
        let t0 = Instant::now();
        go.store(true, Ordering::Release);
        if parked.load(Ordering::Relaxed) {
            if let Some(t) = worker.lock().unwrap().as_ref() {
                t.unpark();
            }
        }
        while !ack.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        ack.store(false, Ordering::Relaxed);
        if i >= t_wall_start_round {
            lats.push(t0.elapsed().as_nanos() as u64);
        }
    }
    let el = t_wall.elapsed().as_nanos() as u64;

    done.store(true, Ordering::Relaxed);
    go.store(true, Ordering::Release);
    if let Some(t) = worker.lock().unwrap().as_ref() {
        t.unpark();
    }
    h.join().unwrap();
    finish("AtomicBool + park/unpark", lats, el)
}

fn xt_hot_condvar() -> Row {
    let state = Arc::new((Mutex::new((false, false)), Condvar::new(), Condvar::new()));
    // (go, ack), cv_go, cv_ack
    let done = Arc::new(AtomicBool::new(false));

    let st = state.clone();
    let d = done.clone();
    let h = thread::spawn(move || loop {
        let mut g = st.0.lock().unwrap();
        while !g.0 {
            if d.load(Ordering::Relaxed) {
                return;
            }
            g = st.1.wait(g).unwrap();
        }
        g.0 = false;
        g.1 = true;
        drop(g);
        st.2.notify_one();
    });

    let total = warmup() + rounds();
    let mut lats = Vec::with_capacity(rounds());
    let t_wall_start_round = warmup();
    let mut t_wall = Instant::now();

    for i in 0..total {
        if i == t_wall_start_round {
            t_wall = Instant::now();
        }
        let t0 = Instant::now();
        {
            let mut g = state.0.lock().unwrap();
            g.0 = true;
            drop(g);
            state.1.notify_one();
        }
        {
            let mut g = state.0.lock().unwrap();
            while !g.1 {
                g = state.2.wait(g).unwrap();
            }
            g.1 = false;
        }
        if i >= t_wall_start_round {
            lats.push(t0.elapsed().as_nanos() as u64);
        }
    }
    let el = t_wall.elapsed().as_nanos() as u64;

    done.store(true, Ordering::Relaxed);
    {
        let mut g = state.0.lock().unwrap();
        g.0 = true;
    }
    state.1.notify_one();
    h.join().unwrap();
    finish("Mutex<bool> + Condvar", lats, el)
}

fn xt_hot_cb_parker() -> Row {
    // Parker is !Sync (owns a token), so we move it into the consumer.
    // The main thread keeps Unparkers (Send+Sync).
    let p_go = CbParker::new();
    let u_go = p_go.unparker().clone();
    let p_ack = CbParker::new();
    let u_ack = p_ack.unparker().clone();
    let done = Arc::new(AtomicBool::new(false));

    let d = done.clone();
    let h = thread::spawn(move || loop {
        p_go.park();
        if d.load(Ordering::Relaxed) {
            return;
        }
        u_ack.unpark();
    });

    let total = warmup() + rounds();
    let mut lats = Vec::with_capacity(rounds());
    let t_wall_start_round = warmup();
    let mut t_wall = Instant::now();

    for i in 0..total {
        if i == t_wall_start_round {
            t_wall = Instant::now();
        }
        let t0 = Instant::now();
        u_go.unpark();
        p_ack.park();
        if i >= t_wall_start_round {
            lats.push(t0.elapsed().as_nanos() as u64);
        }
    }
    let el = t_wall.elapsed().as_nanos() as u64;

    done.store(true, Ordering::Relaxed);
    u_go.unpark();
    h.join().unwrap();
    finish("crossbeam Parker", lats, el)
}

// ═══════════════════════════════════════════════════════════════════════════
// CROSS-THREAD PARKED runners — guarantee the consumer is parked before
// each fire by sleeping 500µs. Measures true wake-from-park latency.
// Spin primitives (AtomicBool+spin) don't have a park path — they just
// burn CPU during the sleep — so we still run them but the number is
// "coherence latency + PAUSE overhead", not "wake latency".
// ═══════════════════════════════════════════════════════════════════════════

fn xt_parked_signal() -> Row {
    let sig = Arc::new(Signal::with_spin(0)); // park immediately
    let ready = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let round_nr = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let s = sig.clone();
    let r = ready.clone();
    let d = done.clone();
    let rn = round_nr.clone();
    let h = thread::spawn(move || {
        s.set_worker(thread::current());
        r.store(true, Ordering::Release);
        let mut seen = 0usize;
        while !d.load(Ordering::Relaxed) {
            s.acquire();
            s.lock();
            seen += 1;
            rn.store(seen, Ordering::Release);
        }
    });
    while !ready.load(Ordering::Acquire) {
        thread::yield_now();
    }

    // warmup: no pre-sleep, just shake out
    for i in 0..warmup() {
        sig.release();
        while round_nr.load(Ordering::Acquire) < i + 1 {
            std::hint::spin_loop();
        }
    }

    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for i in 0..rounds() {
        thread::sleep(XT_PARKED_PRE_SLEEP);
        let expect = warmup() + i + 1;
        let t0 = Instant::now();
        sig.release();
        while round_nr.load(Ordering::Acquire) < expect {
            std::hint::spin_loop();
        }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;

    done.store(true, Ordering::Relaxed);
    sig.release();
    h.join().unwrap();
    finish("Signal", lats, el)
}

fn xt_parked_atomic_spin() -> Row {
    // Spin consumer burns CPU during the pre-sleep — we still measure it
    // as a reference point.
    let go = Arc::new(AtomicBool::new(false));
    let ack = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));

    let g = go.clone();
    let a = ack.clone();
    let d = done.clone();
    let h = thread::spawn(move || loop {
        while !g.load(Ordering::Acquire) {
            if d.load(Ordering::Relaxed) {
                return;
            }
            std::hint::spin_loop();
        }
        g.store(false, Ordering::Relaxed);
        a.store(true, Ordering::Release);
    });

    for _ in 0..warmup() {
        go.store(true, Ordering::Release);
        while !ack.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        ack.store(false, Ordering::Relaxed);
    }

    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for _ in 0..rounds() {
        thread::sleep(XT_PARKED_PRE_SLEEP);
        let t0 = Instant::now();
        go.store(true, Ordering::Release);
        while !ack.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        ack.store(false, Ordering::Relaxed);
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;

    done.store(true, Ordering::Relaxed);
    go.store(true, Ordering::Release);
    h.join().unwrap();
    finish("AtomicBool + spin", lats, el)
}

fn xt_parked_atomic_park() -> Row {
    let go = Arc::new(AtomicBool::new(false));
    let parked = Arc::new(AtomicBool::new(false));
    let ack = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let worker = Arc::new(Mutex::new(None::<thread::Thread>));

    let g = go.clone();
    let p = parked.clone();
    let a = ack.clone();
    let d = done.clone();
    let w = worker.clone();
    let h = thread::spawn(move || {
        *w.lock().unwrap() = Some(thread::current());
        loop {
            if d.load(Ordering::Relaxed) {
                return;
            }
            p.store(true, Ordering::SeqCst);
            while !g.load(Ordering::Acquire) {
                if d.load(Ordering::Relaxed) {
                    return;
                }
                thread::park();
            }
            p.store(false, Ordering::Relaxed);
            g.store(false, Ordering::Relaxed);
            a.store(true, Ordering::Release);
        }
    });
    loop {
        if worker.lock().unwrap().is_some() {
            break;
        }
        thread::yield_now();
    }

    for _ in 0..warmup() {
        go.store(true, Ordering::Release);
        if parked.load(Ordering::Relaxed) {
            if let Some(t) = worker.lock().unwrap().as_ref() {
                t.unpark();
            }
        }
        while !ack.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        ack.store(false, Ordering::Relaxed);
    }

    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for _ in 0..rounds() {
        thread::sleep(XT_PARKED_PRE_SLEEP);
        let t0 = Instant::now();
        go.store(true, Ordering::Release);
        if parked.load(Ordering::Relaxed) {
            if let Some(t) = worker.lock().unwrap().as_ref() {
                t.unpark();
            }
        }
        while !ack.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        ack.store(false, Ordering::Relaxed);
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;

    done.store(true, Ordering::Relaxed);
    go.store(true, Ordering::Release);
    if let Some(t) = worker.lock().unwrap().as_ref() {
        t.unpark();
    }
    h.join().unwrap();
    finish("AtomicBool + park/unpark", lats, el)
}

fn xt_parked_condvar() -> Row {
    let state = Arc::new((Mutex::new((false, false)), Condvar::new(), Condvar::new()));
    let done = Arc::new(AtomicBool::new(false));

    let st = state.clone();
    let d = done.clone();
    let h = thread::spawn(move || loop {
        let mut g = st.0.lock().unwrap();
        while !g.0 {
            if d.load(Ordering::Relaxed) {
                return;
            }
            g = st.1.wait(g).unwrap();
        }
        g.0 = false;
        g.1 = true;
        drop(g);
        st.2.notify_one();
    });

    for _ in 0..warmup() {
        {
            let mut g = state.0.lock().unwrap();
            g.0 = true;
        }
        state.1.notify_one();
        {
            let mut g = state.0.lock().unwrap();
            while !g.1 {
                g = state.2.wait(g).unwrap();
            }
            g.1 = false;
        }
    }

    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for _ in 0..rounds() {
        thread::sleep(XT_PARKED_PRE_SLEEP);
        let t0 = Instant::now();
        {
            let mut g = state.0.lock().unwrap();
            g.0 = true;
            drop(g);
            state.1.notify_one();
        }
        {
            let mut g = state.0.lock().unwrap();
            while !g.1 {
                g = state.2.wait(g).unwrap();
            }
            g.1 = false;
        }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;

    done.store(true, Ordering::Relaxed);
    {
        let mut g = state.0.lock().unwrap();
        g.0 = true;
    }
    state.1.notify_one();
    h.join().unwrap();
    finish("Mutex<bool> + Condvar", lats, el)
}

fn xt_parked_cb_parker() -> Row {
    let p_go = CbParker::new();
    let u_go = p_go.unparker().clone();
    let p_ack = CbParker::new();
    let u_ack = p_ack.unparker().clone();
    let done = Arc::new(AtomicBool::new(false));

    let d = done.clone();
    let h = thread::spawn(move || loop {
        p_go.park();
        if d.load(Ordering::Relaxed) {
            return;
        }
        u_ack.unpark();
    });

    for _ in 0..warmup() {
        u_go.unpark();
        p_ack.park();
    }

    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for _ in 0..rounds() {
        thread::sleep(XT_PARKED_PRE_SLEEP);
        let t0 = Instant::now();
        u_go.unpark();
        p_ack.park();
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;

    done.store(true, Ordering::Relaxed);
    u_go.unpark();
    h.join().unwrap();
    finish("crossbeam Parker", lats, el)
}

// ═══════════════════════════════════════════════════════════════════════════
// crossbeam::channel::bounded(1) — fair SPSC comparison. It's a channel,
// not a raw wake primitive, but it's the idiomatic crossbeam equivalent of
// `Signal`-as-SPSC-wake: bounded(1) acts as a 1-slot handoff with park.
// ═══════════════════════════════════════════════════════════════════════════

fn st_cb_channel() -> Row {
    let (tx, rx) = cb_bounded::<()>(1);
    for _ in 0..warmup() {
        tx.send(()).unwrap();
        rx.recv().unwrap();
    }
    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for _ in 0..rounds() {
        let t0 = Instant::now();
        tx.send(()).unwrap();
        rx.recv().unwrap();
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;
    finish("crossbeam::channel::bounded(1)", lats, el)
}

fn xt_hot_cb_channel() -> Row {
    let (tx_go, rx_go) = cb_bounded::<()>(1);
    let (tx_ack, rx_ack) = cb_bounded::<()>(1);
    let done = Arc::new(AtomicBool::new(false));

    let d = done.clone();
    let h = thread::spawn(move || {
        while rx_go.recv().is_ok() {
            if d.load(Ordering::Relaxed) {
                return;
            }
            if tx_ack.send(()).is_err() {
                return;
            }
        }
    });

    for _ in 0..warmup() {
        tx_go.send(()).unwrap();
        rx_ack.recv().unwrap();
    }

    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for _ in 0..rounds() {
        let t0 = Instant::now();
        tx_go.send(()).unwrap();
        rx_ack.recv().unwrap();
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;

    done.store(true, Ordering::Relaxed);
    let _ = tx_go.send(());
    h.join().unwrap();
    finish("crossbeam::channel::bounded(1)", lats, el)
}

fn xt_parked_cb_channel() -> Row {
    let (tx_go, rx_go) = cb_bounded::<()>(1);
    let (tx_ack, rx_ack) = cb_bounded::<()>(1);
    let done = Arc::new(AtomicBool::new(false));

    let d = done.clone();
    let h = thread::spawn(move || {
        while rx_go.recv().is_ok() {
            if d.load(Ordering::Relaxed) {
                return;
            }
            if tx_ack.send(()).is_err() {
                return;
            }
        }
    });

    for _ in 0..warmup() {
        tx_go.send(()).unwrap();
        rx_ack.recv().unwrap();
    }

    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for _ in 0..rounds() {
        thread::sleep(XT_PARKED_PRE_SLEEP);
        let t0 = Instant::now();
        tx_go.send(()).unwrap();
        rx_ack.recv().unwrap();
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;

    done.store(true, Ordering::Relaxed);
    let _ = tx_go.send(());
    h.join().unwrap();
    finish("crossbeam::channel::bounded(1)", lats, el)
}

// ═══════════════════════════════════════════════════════════════════════════
// OneSignal — single-use gate (release once, acquire once, then consumed).
// Different shape from Signal: each round needs a fresh (Sender, Receiver)
// pair. Inner state is `Arc<Inner>` so each round pays one Arc allocation.
//
// ST measures the full lifecycle (new + release + acquire) per round —
// that's the real cost a user pays per oneshot wakeup.
//
// XT pre-allocates N pairs outside the timed loop and ships Receivers to
// the consumer up front (via crossbeam channel), so the timed window
// only measures the wake itself, comparable to Signal's release→acquire.
// ═══════════════════════════════════════════════════════════════════════════

fn st_one_signal() -> Row {
    for _ in 0..warmup() {
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
        rx.bind();
        tx.release();
        rx.acquire().unwrap();
    }
    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for _ in 0..rounds() {
        let t0 = Instant::now();
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
        rx.bind();
        tx.release();
        rx.acquire().unwrap();
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;
    finish("OneSignal (new+release+acquire)", lats, el)
}

fn xt_hot_one_signal() -> Row {
    let total = warmup() + rounds();
    // Pre-allocate every (Sender, Receiver) pair.
    let mut senders = Vec::with_capacity(total);
    let (tx_rx, rx_rx) = cb_bounded::<arbitro_kit::gate::OneSignalReceiver<ParkWaiter>>(total);
    for _ in 0..total {
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
        senders.push(tx);
        tx_rx.send(rx).unwrap();
    }
    drop(tx_rx); // close the channel so the consumer sees end-of-stream

    let round_nr = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let rn = round_nr.clone();
    let h = thread::spawn(move || {
        let mut seen = 0usize;
        while let Ok(rx) = rx_rx.recv() {
            rx.bind();
            rx.acquire().unwrap();
            seen += 1;
            rn.store(seen, Ordering::Release);
        }
    });

    // Give the consumer a moment to drain the channel and park on its first
    // OneSignal so the producer fires straight into the spin window.
    thread::sleep(Duration::from_micros(50));

    let mut lats = Vec::with_capacity(rounds());
    let t_wall_start = warmup();
    let mut t_wall = Instant::now();
    for (i, tx) in senders.into_iter().enumerate() {
        if i == t_wall_start {
            t_wall = Instant::now();
        }
        let expect = i + 1;
        let t0 = Instant::now();
        tx.release();
        while round_nr.load(Ordering::Acquire) < expect {
            std::hint::spin_loop();
        }
        if i >= t_wall_start {
            lats.push(t0.elapsed().as_nanos() as u64);
        }
    }
    let el = t_wall.elapsed().as_nanos() as u64;
    h.join().unwrap();
    finish("OneSignal", lats, el)
}

fn xt_parked_one_signal() -> Row {
    let total = warmup() + rounds();
    let mut senders = Vec::with_capacity(total);
    let (tx_rx, rx_rx) = cb_bounded::<arbitro_kit::gate::OneSignalReceiver<ParkWaiter>>(total);
    for _ in 0..total {
        let (tx, rx) = OneSignal::<ParkWaiter>::new();
        senders.push(tx);
        tx_rx.send(rx).unwrap();
    }
    drop(tx_rx);

    let round_nr = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let rn = round_nr.clone();
    let h = thread::spawn(move || {
        let mut seen = 0usize;
        while let Ok(rx) = rx_rx.recv() {
            rx.bind();
            rx.acquire().unwrap();
            seen += 1;
            rn.store(seen, Ordering::Release);
        }
    });

    // Warmup: shake out the spin window without pre-sleep.
    let mut iter = senders.into_iter();
    for i in 0..warmup() {
        let tx = iter.next().unwrap();
        tx.release();
        while round_nr.load(Ordering::Acquire) < i + 1 {
            std::hint::spin_loop();
        }
    }

    let mut lats = Vec::with_capacity(rounds());
    let t_wall = Instant::now();
    for (k, tx) in iter.enumerate() {
        thread::sleep(XT_PARKED_PRE_SLEEP);
        let expect = warmup() + k + 1;
        let t0 = Instant::now();
        tx.release();
        while round_nr.load(Ordering::Acquire) < expect {
            std::hint::spin_loop();
        }
        lats.push(t0.elapsed().as_nanos() as u64);
    }
    let el = t_wall.elapsed().as_nanos() as u64;

    h.join().unwrap();
    finish("OneSignal", lats, el)
}

// ═══════════════════════════════════════════════════════════════════════════

fn main() {
    println!("=== arbitro-kit gate_overhead (wake-primitive comparison) ===");
    println!(
        "rounds={}  warmup={}  xt_parked_pre_sleep={}µs",
        rounds(),
        warmup(),
        XT_PARKED_PRE_SLEEP.as_micros()
    );

    print_scenario_header("Single-thread (release + acquire same thread)");
    print_row(st_signal());
    print_row(st_atomic_spin());
    print_row(st_atomic_park());
    print_row(st_condvar());
    print_row(st_cb_parker());
    print_row(st_cb_channel());
    print_row(st_one_signal());

    print_scenario_header("Cross-thread HOT (consumer spinning, no pre-sleep)");
    print_row(xt_hot_signal());
    print_row(xt_hot_atomic_spin());
    print_row(xt_hot_atomic_park());
    print_row(xt_hot_condvar());
    print_row(xt_hot_cb_parker());
    print_row(xt_hot_cb_channel());
    print_row(xt_hot_one_signal());

    print_scenario_header("Cross-thread PARKED (500µs pre-sleep — true wake latency)");
    print_row(xt_parked_signal());
    print_row(xt_parked_atomic_spin());
    print_row(xt_parked_atomic_park());
    print_row(xt_parked_condvar());
    print_row(xt_parked_cb_parker());
    print_row(xt_parked_cb_channel());
    print_row(xt_parked_one_signal());

    println!("\nDone.");
}
