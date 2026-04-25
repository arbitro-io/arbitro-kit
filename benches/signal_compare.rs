//! Head-to-head comparison of 1-slot signalling primitives.
//!
//! Primitives:
//!   - `Signal`                       — this crate, single-channel M:1 signal.
//!   - `SignalSet` (1 bit)            — this crate, bitmap M:1 signal.
//!   - `crossbeam_channel::bounded(1)` — industry reference.
//!   - `std::sync::mpsc::sync_channel(1)` — std reference.
//!
//! ## Isolation strategy
//!
//! Each primitive runs in **its own child process** (re-exec of this same
//! binary with an argv selector). That way:
//!
//! - **Peak RSS** read as `VmHWM` from `/proc/self/status` at the end of the
//!   child's life — reflects only that primitive's allocations.
//! - **CPU time** read as `utime + stime` from `/proc/self/stat` — only the
//!   cycles this primitive actually burned.
//! - No cross-contamination between runs (page cache, thread-local state,
//!   LLVM inlining across functions, warmup bleed-over).
//!
//! The parent process spawns children sequentially, captures their one-line
//! CSV output, and formats the final table.
//!
//! Scenarios:
//!   - `cross_thread`  — producer + consumer on separate threads; consumer
//!                       is parked when signal fires (500µs pre-fire sleep).
//!   - `single_thread` — same thread signals and receives; pure fast-path.
//!
//! Run with: `cargo bench --bench signal_compare`

use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arbitro_kit::gate::{Signal, SignalSet};
use arbitro_kit::slot::Channel;

/// Cross-thread rounds per primitive. Override with env `BENCH_CROSS_ROUNDS`
/// (useful for smoke tests: `BENCH_CROSS_ROUNDS=50` keeps the run under the
/// bench_safety smoke budget of <100 msgs).
fn cross_rounds() -> usize {
    std::env::var("BENCH_CROSS_ROUNDS").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(1000)
}
/// Single-thread rounds. Override with `BENCH_SINGLE_ROUNDS`.
fn single_rounds() -> usize {
    std::env::var("BENCH_SINGLE_ROUNDS").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(100_000)
}
const WARMUP: usize = 100;

/// Parked scenario: force the consumer to be deep in `park()` before we fire.
/// Any spin budget ≤ this value is irrelevant here — spin never catches it.
const PRE_FIRE_SLEEP_PARKED_US: u64 = 500;

/// Hot scenario: fire immediately after signalling round-start. The consumer
/// has ~µs head-start on `acquire()`, so spin budget directly affects whether
/// the wake happens via spin (no syscall) or park (syscall).
const PRE_FIRE_SLEEP_HOT_US: u64 = 0;

/// Spin budgets to sweep in the hot cross-thread test. 0 = park immediately
/// (worst for hot signals, best for idle CPU).
const SPIN_SWEEP: &[u32] = &[0, 128, 512, 2048, 8192, 32768];

const PRIMS: &[(&str, &str)] = &[
    // (argv selector, display name)
    ("gate-cross",          "Signal"),
    ("gate63-cross",        "Signal63<u63 inline>"),
    ("gatemsg-u64-cross",   "SignalMsg<u64> (8 B)"),
    ("gatemsg-64b-cross",   "SignalMsg<[u8;64]> (1 line)"),
    ("gatemsg-256b-cross",  "SignalMsg<[u8;256]> (4 lines)"),
    ("gatemsg-4k-cross",    "SignalMsg<[u8;4096]> (1 page)"),
    ("gateset-cross",       "SignalSet(1bit)"),
    ("crossbeam-cross",       "crossbeam bounded1<u64>"),
    ("crossbeam-64b-cross",   "crossbeam bounded1<[u8;64]>"),
    ("crossbeam-256b-cross",  "crossbeam bounded1<[u8;256]>"),
    ("crossbeam-4k-cross",    "crossbeam bounded1<[u8;4096]>"),
    ("mpsc-cross",            "std::mpsc sync(1)<u64>"),
    ("mpsc-64b-cross",        "std::mpsc sync(1)<[u8;64]>"),
    ("mpsc-256b-cross",       "std::mpsc sync(1)<[u8;256]>"),
    ("mpsc-4k-cross",         "std::mpsc sync(1)<[u8;4096]>"),
    ("gate-single",         "Signal"),
    ("gate63-single",       "Signal63<u63 inline>"),
    ("gatemsg-u64-single",  "SignalMsg<u64> (8 B)"),
    ("gatemsg-64b-single",  "SignalMsg<[u8;64]> (1 line)"),
    ("gatemsg-256b-single", "SignalMsg<[u8;256]> (4 lines)"),
    ("gatemsg-4k-single",   "SignalMsg<[u8;4096]> (1 page)"),
    ("gateset-single",      "SignalSet(1bit)"),
    ("crossbeam-single",      "crossbeam bounded1<u64>"),
    ("crossbeam-64b-single",  "crossbeam bounded1<[u8;64]>"),
    ("crossbeam-256b-single", "crossbeam bounded1<[u8;256]>"),
    ("crossbeam-4k-single",   "crossbeam bounded1<[u8;4096]>"),
    ("mpsc-single",           "std::mpsc sync(1)<u64>"),
    ("mpsc-64b-single",       "std::mpsc sync(1)<[u8;64]>"),
    ("mpsc-256b-single",      "std::mpsc sync(1)<[u8;256]>"),
    ("mpsc-4k-single",        "std::mpsc sync(1)<[u8;4096]>"),
];

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // ── Child mode ─────────────────────────────────────────────────────────
    if args.len() > 1 {
        child_main(&args[1..]);
        return;
    }

    // ── Parent mode ────────────────────────────────────────────────────────
    println!("=== arbitro-kit signal_compare ===");
    println!("cross_rounds={}  single_rounds={}", cross_rounds(), single_rounds());
    println!("(each primitive runs in its own child process; RAM=VmHWM peak, CPU=utime+stime)\n");

    let hdr = format!("{:<28} {:>10} {:>10} {:>10} {:>12} {:>12} {:>12}",
                      "primitive", "min_ns", "p50_ns", "p99_ns", "ops/sec", "peak_RSS_KB", "CPU_us");

    println!("-- CROSS-THREAD [PARKED] (pre-fire sleep {PRE_FIRE_SLEEP_PARKED_US}us → consumer always parked) --");
    println!("{hdr}");
    for (sel, name) in PRIMS.iter().filter(|(s, _)| s.ends_with("-cross")) {
        print_row(name, spawn_child(&[sel, "parked"]));
    }

    println!("\n-- CROSS-THREAD [HOT] (pre-fire sleep 0 → signal lands during spin window) --");
    println!("{hdr}");
    // Sweep spin values for Signal / Signal63 / SignalSet; channels have no spin knob.
    for &spin in SPIN_SWEEP {
        let name = format!("Signal[spin={spin}]");
        print_row(&name, spawn_child(&["gate-cross", "hot", &spin.to_string()]));
    }
    for &spin in SPIN_SWEEP {
        let name = format!("Signal63[spin={spin}]");
        print_row(&name, spawn_child(&["gate63-cross", "hot", &spin.to_string()]));
    }
    // SignalMsg variants at default spin only (size sweep is the point).
    print_row("SignalMsg<u64>",       spawn_child(&["gatemsg-u64-cross",  "hot"]));
    print_row("SignalMsg<[u8;64]>",   spawn_child(&["gatemsg-64b-cross",  "hot"]));
    print_row("SignalMsg<[u8;256]>",  spawn_child(&["gatemsg-256b-cross", "hot"]));
    print_row("SignalMsg<[u8;4096]>", spawn_child(&["gatemsg-4k-cross",   "hot"]));
    for &spin in SPIN_SWEEP {
        let name = format!("SignalSet[spin={spin}]");
        print_row(&name, spawn_child(&["gateset-cross", "hot", &spin.to_string()]));
    }
    print_row("crossbeam<u64>",         spawn_child(&["crossbeam-cross",      "hot"]));
    print_row("crossbeam<[u8;64]>",     spawn_child(&["crossbeam-64b-cross",  "hot"]));
    print_row("crossbeam<[u8;256]>",    spawn_child(&["crossbeam-256b-cross", "hot"]));
    print_row("crossbeam<[u8;4096]>",   spawn_child(&["crossbeam-4k-cross",   "hot"]));
    print_row("std::mpsc<u64>",         spawn_child(&["mpsc-cross",           "hot"]));
    print_row("std::mpsc<[u8;64]>",     spawn_child(&["mpsc-64b-cross",       "hot"]));
    print_row("std::mpsc<[u8;256]>",    spawn_child(&["mpsc-256b-cross",      "hot"]));
    print_row("std::mpsc<[u8;4096]>",   spawn_child(&["mpsc-4k-cross",        "hot"]));

    println!("\n-- CROSS-THREAD [RTT HOT] (client→server→client round-trip, no pre-fire sleep) --");
    println!("{hdr}");
    print_row("Channel<u64>",         spawn_child(&["gatechannel-u64-rtt",  "hot"]));
    print_row("Channel<[u8;64]>",     spawn_child(&["gatechannel-64b-rtt",  "hot"]));
    print_row("Channel<[u8;256]>",    spawn_child(&["gatechannel-256b-rtt", "hot"]));
    print_row("Channel<[u8;4096]>",   spawn_child(&["gatechannel-4k-rtt",   "hot"]));
    print_row("crossbeam pair<u64>",      spawn_child(&["cbpair-u64-rtt",       "hot"]));
    print_row("crossbeam pair<[u8;64]>",  spawn_child(&["cbpair-64b-rtt",       "hot"]));
    print_row("crossbeam pair<[u8;256]>", spawn_child(&["cbpair-256b-rtt",      "hot"]));
    print_row("crossbeam pair<[u8;4096]>",spawn_child(&["cbpair-4k-rtt",        "hot"]));
    print_row("mpsc pair<u64>",           spawn_child(&["mpscpair-u64-rtt",     "hot"]));
    print_row("mpsc pair<[u8;64]>",       spawn_child(&["mpscpair-64b-rtt",     "hot"]));
    print_row("mpsc pair<[u8;256]>",      spawn_child(&["mpscpair-256b-rtt",    "hot"]));
    print_row("mpsc pair<[u8;4096]>",     spawn_child(&["mpscpair-4k-rtt",      "hot"]));

    println!("\n-- CROSS-THREAD [RTT HOT, ZERO-COPY Vec<u8>] (ownership transfer; only 24 B of metadata crosses) --");
    println!("{hdr}");
    print_row("Channel<Vec 4KB>",     spawn_child(&["gatechannel-vec4k-rtt",  "hot"]));
    print_row("Channel<Vec 64KB>",    spawn_child(&["gatechannel-vec64k-rtt", "hot"]));
    print_row("Channel<Vec 1MB>",     spawn_child(&["gatechannel-vec1m-rtt",  "hot"]));
    print_row("crossbeam pair<Vec 4KB>",  spawn_child(&["cbpair-vec4k-rtt",       "hot"]));
    print_row("crossbeam pair<Vec 64KB>", spawn_child(&["cbpair-vec64k-rtt",      "hot"]));
    print_row("crossbeam pair<Vec 1MB>",  spawn_child(&["cbpair-vec1m-rtt",       "hot"]));

    println!("\n-- SINGLE-THREAD (fast path, no handoff) --");
    println!("{hdr}");
    for (sel, name) in PRIMS.iter().filter(|(s, _)| s.ends_with("-single")) {
        print_row(name, spawn_child(&[sel]));
    }
}

fn print_row(name: &str, row: ChildReport) {
    println!("{:<28} {:>10} {:>10} {:>10} {:>12} {:>12} {:>12}",
             name, row.min_ns, row.p50_ns, row.p99_ns,
             row.ops_per_sec, row.peak_rss_kb, row.cpu_us);
}

// ─── child → parent protocol ─────────────────────────────────────────────────

struct ChildReport {
    min_ns: u64,
    p50_ns: u64,
    p99_ns: u64,
    ops_per_sec: u64,
    peak_rss_kb: u64,
    cpu_us: u64,
}

fn spawn_child(args: &[&str]) -> ChildReport {
    let exe = std::env::current_exe().expect("current_exe");
    let mut cmd = std::process::Command::new(&exe);
    for a in args { cmd.arg(a); }
    let mut child = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("spawn child");
    let mut out = String::new();
    child.stdout.as_mut().unwrap().read_to_string(&mut out).ok();
    let status = child.wait().expect("wait child");
    if !status.success() {
        panic!("child `{args:?}` failed: {status:?} stdout={out:?}");
    }
    parse_report(&out).unwrap_or_else(|| panic!("bad child output for `{args:?}`: {out:?}"))
}

fn parse_report(s: &str) -> Option<ChildReport> {
    let line = s.lines().find(|l| l.starts_with("RESULT,"))?;
    let parts: Vec<&str> = line.split(',').collect();
    if parts.len() != 7 { return None; }
    Some(ChildReport {
        min_ns:      parts[1].parse().ok()?,
        p50_ns:      parts[2].parse().ok()?,
        p99_ns:      parts[3].parse().ok()?,
        ops_per_sec: parts[4].parse().ok()?,
        peak_rss_kb: parts[5].parse().ok()?,
        cpu_us:      parts[6].parse().ok()?,
    })
}

// ─── child_main: one primitive, then print one RESULT line ──────────────────

fn child_main(args: &[String]) {
    let selector = args[0].as_str();
    // args[1] optional: "parked" or "hot". args[2] optional: spin iters (u32).
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("parked");
    let pre_fire_us = match mode {
        "hot"    => PRE_FIRE_SLEEP_HOT_US,
        _        => PRE_FIRE_SLEEP_PARKED_US,
    };
    let spin: u32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(arbitro_kit::gate::DEFAULT_SPIN_ITERS);

    let (lats, ops_per_sec) = match selector {
        "gate-cross"          => cross_run(|r| gate_cross(r, pre_fire_us, spin)),
        "gate63-cross"        => cross_run(|r| gate63_cross(r, pre_fire_us, spin)),
        "gatemsg-u64-cross"   => cross_run(|r| gatemsg_u64_cross(r, pre_fire_us, spin)),
        "gatemsg-64b-cross"   => cross_run(|r| gatemsg_64b_cross(r, pre_fire_us, spin)),
        "gatemsg-256b-cross"  => cross_run(|r| gatemsg_256b_cross(r, pre_fire_us, spin)),
        "gatemsg-4k-cross"    => cross_run(|r| gatemsg_4k_cross(r, pre_fire_us, spin)),
        "gateset-cross"       => cross_run(|r| gateset_cross(r, pre_fire_us, spin)),
        "crossbeam-cross"       => cross_run(|r| crossbeam_cross(r, pre_fire_us)),
        "crossbeam-64b-cross"   => cross_run(|r| crossbeam_64b_cross(r, pre_fire_us)),
        "crossbeam-256b-cross"  => cross_run(|r| crossbeam_256b_cross(r, pre_fire_us)),
        "crossbeam-4k-cross"    => cross_run(|r| crossbeam_4k_cross(r, pre_fire_us)),
        "mpsc-cross"            => cross_run(|r| mpsc_cross(r, pre_fire_us)),
        "mpsc-64b-cross"        => cross_run(|r| mpsc_64b_cross(r, pre_fire_us)),
        "mpsc-256b-cross"       => cross_run(|r| mpsc_256b_cross(r, pre_fire_us)),
        "mpsc-4k-cross"         => cross_run(|r| mpsc_4k_cross(r, pre_fire_us)),
        "gatechannel-u64-rtt"   => cross_run(|r| gatechannel_u64_rtt(r, pre_fire_us)),
        "gatechannel-64b-rtt"   => cross_run(|r| gatechannel_64b_rtt(r, pre_fire_us)),
        "gatechannel-256b-rtt"  => cross_run(|r| gatechannel_256b_rtt(r, pre_fire_us)),
        "gatechannel-4k-rtt"    => cross_run(|r| gatechannel_4k_rtt(r, pre_fire_us)),
        "cbpair-u64-rtt"        => cross_run(|r| cbpair_u64_rtt(r, pre_fire_us)),
        "cbpair-64b-rtt"        => cross_run(|r| cbpair_64b_rtt(r, pre_fire_us)),
        "cbpair-256b-rtt"       => cross_run(|r| cbpair_256b_rtt(r, pre_fire_us)),
        "cbpair-4k-rtt"         => cross_run(|r| cbpair_4k_rtt(r, pre_fire_us)),
        "mpscpair-u64-rtt"      => cross_run(|r| mpscpair_u64_rtt(r, pre_fire_us)),
        "mpscpair-64b-rtt"      => cross_run(|r| mpscpair_64b_rtt(r, pre_fire_us)),
        "mpscpair-256b-rtt"     => cross_run(|r| mpscpair_256b_rtt(r, pre_fire_us)),
        "mpscpair-4k-rtt"       => cross_run(|r| mpscpair_4k_rtt(r, pre_fire_us)),
        "gatechannel-vec4k-rtt"  => cross_run(|r| gatechannel_vec4k_rtt(r, pre_fire_us)),
        "gatechannel-vec64k-rtt" => cross_run(|r| gatechannel_vec64k_rtt(r, pre_fire_us)),
        "gatechannel-vec1m-rtt"  => cross_run(|r| gatechannel_vec1m_rtt(r, pre_fire_us)),
        "cbpair-vec4k-rtt"       => cross_run(|r| cbpair_vec4k_rtt(r, pre_fire_us)),
        "cbpair-vec64k-rtt"      => cross_run(|r| cbpair_vec64k_rtt(r, pre_fire_us)),
        "cbpair-vec1m-rtt"       => cross_run(|r| cbpair_vec1m_rtt(r, pre_fire_us)),
        "gate-single"         => single_run(gate_single),
        "gate63-single"       => single_run(gate63_single),
        "gatemsg-u64-single"  => single_run(gatemsg_u64_single),
        "gatemsg-64b-single"  => single_run(gatemsg_64b_single),
        "gatemsg-256b-single" => single_run(gatemsg_256b_single),
        "gatemsg-4k-single"   => single_run(gatemsg_4k_single),
        "gateset-single"      => single_run(gateset_single),
        "crossbeam-single"       => single_run(crossbeam_single),
        "crossbeam-64b-single"   => single_run(crossbeam_64b_single),
        "crossbeam-256b-single"  => single_run(crossbeam_256b_single),
        "crossbeam-4k-single"    => single_run(crossbeam_4k_single),
        "mpsc-single"            => single_run(mpsc_single),
        "mpsc-64b-single"        => single_run(mpsc_64b_single),
        "mpsc-256b-single"       => single_run(mpsc_256b_single),
        "mpsc-4k-single"         => single_run(mpsc_4k_single),
        other => {
            eprintln!("unknown selector: {other}");
            std::process::exit(2);
        }
    };

    let mut v = lats;
    v.sort_unstable();
    let min = v[0];
    let p50 = v[v.len() / 2];
    let p99 = v[v.len() * 99 / 100];

    // Read resource usage at the very end so the primitive's entire lifetime
    // is included.
    let peak_rss = read_vm_hwm_kb();
    let cpu = read_cpu_us();

    println!("RESULT,{min},{p50},{p99},{ops_per_sec},{peak_rss},{cpu}");
}

fn cross_run<F: FnOnce(usize) -> Vec<u64>>(f: F) -> (Vec<u64>, u64) {
    let rounds = cross_rounds();
    let t = Instant::now();
    let lats = f(rounds);
    let elapsed = t.elapsed();
    let ops = (rounds as f64 / elapsed.as_secs_f64()) as u64;
    (lats, ops)
}

fn single_run<F: FnOnce(usize) -> Duration>(f: F) -> (Vec<u64>, u64) {
    let rounds = single_rounds();
    let elapsed = f(rounds);
    let per = elapsed.as_nanos() as u64 / rounds as u64;
    let ops = (rounds as f64 / elapsed.as_secs_f64()) as u64;
    // Synthesize histogram so parent formatting stays uniform.
    (vec![per; 100], ops)
}

// ─── process RAM / CPU readers (OS-specific) ────────────────────────────────

#[cfg(unix)]
fn read_vm_hwm_kb() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            return rest.split_whitespace().next()
                .and_then(|t| t.parse().ok()).unwrap_or(0);
        }
    }
    0
}

#[cfg(unix)]
fn read_cpu_us() -> u64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    if s.is_empty() { return 0; }
    let rparen = match s.rfind(')') { Some(i) => i, None => return 0 };
    let rest = &s[rparen + 1..];
    let parts: Vec<&str> = rest.split_whitespace().collect();
    // After rparen: state(0) ppid(1) ... utime(11) stime(12)
    let utime: u64 = parts.get(11).and_then(|t| t.parse().ok()).unwrap_or(0);
    let stime: u64 = parts.get(12).and_then(|t| t.parse().ok()).unwrap_or(0);
    let hz = 100u64; // USER_HZ
    (utime + stime) * 1_000_000 / hz
}

#[cfg(windows)]
#[allow(non_snake_case, non_camel_case_types)]
mod win {
    #[repr(C)]
    pub struct PROCESS_MEMORY_COUNTERS {
        pub cb: u32,
        pub PageFaultCount: u32,
        pub PeakWorkingSetSize: usize,
        pub WorkingSetSize: usize,
        pub QuotaPeakPagedPoolUsage: usize,
        pub QuotaPagedPoolUsage: usize,
        pub QuotaPeakNonPagedPoolUsage: usize,
        pub QuotaNonPagedPoolUsage: usize,
        pub PagefileUsage: usize,
        pub PeakPagefileUsage: usize,
    }
    #[repr(C)]
    pub struct FILETIME { pub low: u32, pub high: u32 }
    extern "system" {
        pub fn GetCurrentProcess() -> *mut core::ffi::c_void;
        pub fn K32GetProcessMemoryInfo(
            h: *mut core::ffi::c_void,
            ppsmem: *mut PROCESS_MEMORY_COUNTERS,
            cb: u32,
        ) -> i32;
        pub fn GetProcessTimes(
            h: *mut core::ffi::c_void,
            creation: *mut FILETIME,
            exit: *mut FILETIME,
            kernel: *mut FILETIME,
            user: *mut FILETIME,
        ) -> i32;
    }
}

#[cfg(windows)]
fn read_vm_hwm_kb() -> u64 {
    let mut pmc: win::PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
    pmc.cb = std::mem::size_of::<win::PROCESS_MEMORY_COUNTERS>() as u32;
    let ok = unsafe {
        win::K32GetProcessMemoryInfo(win::GetCurrentProcess(), &mut pmc, pmc.cb)
    };
    if ok == 0 { return 0; }
    (pmc.PeakWorkingSetSize / 1024) as u64
}

#[cfg(windows)]
fn read_cpu_us() -> u64 {
    let mut c = win::FILETIME { low: 0, high: 0 };
    let mut e = win::FILETIME { low: 0, high: 0 };
    let mut k = win::FILETIME { low: 0, high: 0 };
    let mut u = win::FILETIME { low: 0, high: 0 };
    let ok = unsafe {
        win::GetProcessTimes(win::GetCurrentProcess(), &mut c, &mut e, &mut k, &mut u)
    };
    if ok == 0 { return 0; }
    // FILETIME is in 100-ns units
    let user = ((u.high as u64) << 32) | (u.low as u64);
    let kern = ((k.high as u64) << 32) | (k.low as u64);
    (user + kern) / 10 // → microseconds
}

// ─── Ctrl: off-path Mutex+Condvar handshake ─────────────────────────────────

struct Ctrl { state: Mutex<u8>, cv: Condvar }
impl Ctrl {
    fn new() -> Self { Self { state: Mutex::new(0), cv: Condvar::new() } }
    fn set(&self, v: u8) {
        let mut s = self.state.lock().unwrap();
        *s = v;
        self.cv.notify_all();
    }
    fn wait_for(&self, v: u8) {
        let mut s = self.state.lock().unwrap();
        while *s != v { s = self.cv.wait(s).unwrap(); }
    }
}

// ─── CROSS-THREAD runners ───────────────────────────────────────────────────

fn gate_cross(rounds: usize, pre_fire_us: u64, spin: u32) -> Vec<u64> {
    let gate = Arc::new(Signal::with_spin(spin));
    let ctrl_start = Arc::new(Ctrl::new());
    let ctrl_done  = Arc::new(Ctrl::new());
    let wake_ns = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();

    let consumer = thread::spawn({
        let gate = gate.clone();
        let ctrl_start = ctrl_start.clone();
        let ctrl_done = ctrl_done.clone();
        let wake_ns = wake_ns.clone();
        let stop = stop.clone();
        move || {
            gate.set_worker(thread::current());
            loop {
                ctrl_start.wait_for(1); ctrl_start.set(0);
                if stop.load(Ordering::Relaxed) { return; }
                gate.acquire();
                wake_ns.store(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                gate.lock();
                ctrl_done.set(2);
            }
        }
    });

    for _ in 0..WARMUP {
        ctrl_start.set(1);
        if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
        gate.release();
        ctrl_done.wait_for(2); ctrl_done.set(0);
    }

    let mut lat = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        ctrl_start.set(1);
        if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
        let fire = t0.elapsed().as_nanos() as u64;
        gate.release();
        ctrl_done.wait_for(2); ctrl_done.set(0);
        lat.push(wake_ns.load(Ordering::Relaxed).saturating_sub(fire));
    }

    stop.store(true, Ordering::Relaxed);
    ctrl_start.set(1);
    gate.release();
    consumer.join().unwrap();
    lat
}

fn gateset_cross(rounds: usize, pre_fire_us: u64, spin: u32) -> Vec<u64> {
    let mut set = SignalSet::with_spin(spin);
    let g = set.create("g");
    let mask = g.mask();
    let set = Arc::new(set);
    let ctrl_start = Arc::new(Ctrl::new());
    let ctrl_done  = Arc::new(Ctrl::new());
    let wake_ns = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();

    let consumer = thread::spawn({
        let set = set.clone();
        let ctrl_start = ctrl_start.clone();
        let ctrl_done = ctrl_done.clone();
        let wake_ns = wake_ns.clone();
        let stop = stop.clone();
        move || {
            set.set_worker(thread::current());
            loop {
                ctrl_start.wait_for(1); ctrl_start.set(0);
                if stop.load(Ordering::Relaxed) { return; }
                set.acquire_any(mask);
                wake_ns.store(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                set.lock_mask(mask);
                ctrl_done.set(2);
            }
        }
    });

    for _ in 0..WARMUP {
        ctrl_start.set(1);
        if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
        set.release(g);
        ctrl_done.wait_for(2); ctrl_done.set(0);
    }

    let mut lat = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        ctrl_start.set(1);
        if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
        let fire = t0.elapsed().as_nanos() as u64;
        set.release(g);
        ctrl_done.wait_for(2); ctrl_done.set(0);
        lat.push(wake_ns.load(Ordering::Relaxed).saturating_sub(fire));
    }

    stop.store(true, Ordering::Relaxed);
    ctrl_start.set(1);
    set.release(g);
    consumer.join().unwrap();
    lat
}

fn crossbeam_cross(rounds: usize, pre_fire_us: u64) -> Vec<u64> {
    let (tx, rx) = crossbeam_channel::bounded::<u64>(1);
    let ctrl_start = Arc::new(Ctrl::new());
    let ctrl_done  = Arc::new(Ctrl::new());
    let wake_ns = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();

    let consumer = thread::spawn({
        let rx = rx.clone();
        let ctrl_start = ctrl_start.clone();
        let ctrl_done = ctrl_done.clone();
        let wake_ns = wake_ns.clone();
        let stop = stop.clone();
        move || {
            loop {
                ctrl_start.wait_for(1); ctrl_start.set(0);
                if stop.load(Ordering::Relaxed) { return; }
                let v = rx.recv().unwrap();
                wake_ns.store(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                std::hint::black_box(v);
                ctrl_done.set(2);
            }
        }
    });

    for _ in 0..WARMUP {
        ctrl_start.set(1);
        if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
        tx.send(0xDEADBEEF).unwrap();
        ctrl_done.wait_for(2); ctrl_done.set(0);
    }

    let mut lat = Vec::with_capacity(rounds);
    for i in 0..rounds {
        ctrl_start.set(1);
        if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
        let fire = t0.elapsed().as_nanos() as u64;
        tx.send(i as u64).unwrap();
        ctrl_done.wait_for(2); ctrl_done.set(0);
        lat.push(wake_ns.load(Ordering::Relaxed).saturating_sub(fire));
    }

    stop.store(true, Ordering::Relaxed);
    ctrl_start.set(1);
    let _ = tx.send(0);
    consumer.join().unwrap();
    lat
}

fn mpsc_cross(rounds: usize, pre_fire_us: u64) -> Vec<u64> {
    let (tx, rx) = sync_channel::<u64>(1);
    let ctrl_start = Arc::new(Ctrl::new());
    let ctrl_done  = Arc::new(Ctrl::new());
    let wake_ns = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();

    let consumer = thread::spawn({
        let ctrl_start = ctrl_start.clone();
        let ctrl_done = ctrl_done.clone();
        let wake_ns = wake_ns.clone();
        let stop = stop.clone();
        move || {
            loop {
                ctrl_start.wait_for(1); ctrl_start.set(0);
                if stop.load(Ordering::Relaxed) { return; }
                let v = rx.recv().unwrap();
                wake_ns.store(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                std::hint::black_box(v);
                ctrl_done.set(2);
            }
        }
    });

    for _ in 0..WARMUP {
        ctrl_start.set(1);
        if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
        tx.send(0xDEADBEEF).unwrap();
        ctrl_done.wait_for(2); ctrl_done.set(0);
    }

    let mut lat = Vec::with_capacity(rounds);
    for i in 0..rounds {
        ctrl_start.set(1);
        if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
        let fire = t0.elapsed().as_nanos() as u64;
        tx.send(i as u64).unwrap();
        ctrl_done.wait_for(2); ctrl_done.set(0);
        lat.push(wake_ns.load(Ordering::Relaxed).saturating_sub(fire));
    }

    stop.store(true, Ordering::Relaxed);
    ctrl_start.set(1);
    let _ = tx.send(0);
    consumer.join().unwrap();
    lat
}

// ─── SINGLE-THREAD runners (fast path, no handoff) ──────────────────────────

fn gate_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let gate = Signal::new();
    let t = Instant::now();
    for _ in 0..rounds {
        black_box(&gate).release();
        black_box(&gate).acquire();
        black_box(&gate).lock();
    }
    t.elapsed()
}

fn gateset_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let mut set = SignalSet::new();
    let g = set.create("g");
    let mask = g.mask();
    let t = Instant::now();
    for _ in 0..rounds {
        black_box(&set).release(black_box(g));
        black_box(&set).acquire_any(black_box(mask));
        black_box(&set).lock(black_box(g));
    }
    t.elapsed()
}

fn crossbeam_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let (tx, rx) = crossbeam_channel::bounded::<u64>(1);
    let t = Instant::now();
    for i in 0..rounds {
        black_box(&tx).send(i as u64).unwrap();
        black_box(black_box(&rx).recv().unwrap());
    }
    t.elapsed()
}

fn mpsc_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let (tx, rx) = sync_channel::<u64>(1);
    let t = Instant::now();
    for i in 0..rounds {
        black_box(&tx).send(i as u64).unwrap();
        black_box(black_box(&rx).recv().unwrap());
    }
    t.elapsed()
}

// ─── Signal63 prototype ───────────────────────────────────────────────────────
//
// Proof-of-concept: pack a 63-bit payload into the same atomic that carries
// the LOCKED flag. One `AtomicU64::store(Release)` publishes both in a single
// instruction. Consumer: one `load(Acquire)` + shift to extract value.
//
// Bit layout:  [ payload (63 bits) | LOCKED (1 bit) ]
//   - LOCKED = 1, payload = 0  → initial / closed, no data.
//   - LOCKED = 0, payload = v  → open, carrying v.
//
// Same M:1 contract as Signal (but currently tested as SPSC in the bench).
// `parked` is kept as a separate AtomicBool to match Signal's cost profile.

use std::cell::UnsafeCell;
use std::sync::atomic::AtomicU64 as StdAtomicU64;

const G63_LOCKED: u64 = 1;
const G63_TIGHT_SPIN: u32 = 64;

#[repr(align(64))]
struct Signal63 {
    /// Packed: bit 0 = LOCKED, bits 1..64 = payload.
    state: StdAtomicU64,
    /// Set by consumer with SeqCst on the park path (same discipline as Signal).
    parked: AtomicBool,
    spin_iters: u32,
    worker: UnsafeCell<Option<thread::Thread>>,
}

// Safety: identical discipline to `Signal`. See src/gate/gate.rs.
unsafe impl Sync for Signal63 {}

impl Signal63 {
    fn with_spin(spin_iters: u32) -> Self {
        Self {
            state: StdAtomicU64::new(G63_LOCKED),
            parked: AtomicBool::new(false),
            spin_iters,
            worker: UnsafeCell::new(None),
        }
    }

    fn set_worker(&self, t: thread::Thread) {
        unsafe { *self.worker.get() = Some(t); }
    }

    /// Publish `value` (max 63 bits) and open the gate in a single atomic op.
    #[inline]
    fn release(&self, value: u64) {
        debug_assert!(value < (1u64 << 63), "payload overflow (>63 bits)");
        self.state.store(value << 1, Ordering::Release);
        if self.parked.load(Ordering::Relaxed) {
            unsafe {
                if let Some(t) = &*self.worker.get() { t.unpark(); }
            }
        }
    }

    #[inline]
    fn lock(&self) {
        self.state.store(G63_LOCKED, Ordering::Relaxed);
    }

    /// Block until open, then return the 63-bit payload.
    #[inline]
    fn acquire(&self) -> u64 {
        let s = self.state.load(Ordering::Acquire);
        if s & G63_LOCKED == 0 { return s >> 1; }
        self.acquire_slow()
    }

    #[cold]
    #[inline(never)]
    fn acquire_slow(&self) -> u64 {
        for _ in 0..G63_TIGHT_SPIN {
            let s = self.state.load(Ordering::Acquire);
            if s & G63_LOCKED == 0 { return s >> 1; }
            std::hint::black_box(());
        }
        for _ in 0..self.spin_iters {
            let s = self.state.load(Ordering::Acquire);
            if s & G63_LOCKED == 0 { return s >> 1; }
            std::hint::spin_loop();
        }
        self.parked.store(true, Ordering::SeqCst);
        let s = self.state.load(Ordering::Acquire);
        if s & G63_LOCKED == 0 {
            self.parked.store(false, Ordering::Relaxed);
            return s >> 1;
        }
        loop {
            thread::park();
            let s = self.state.load(Ordering::Acquire);
            if s & G63_LOCKED == 0 {
                self.parked.store(false, Ordering::Relaxed);
                return s >> 1;
            }
        }
    }
}

fn gate63_cross(rounds: usize, pre_fire_us: u64, spin: u32) -> Vec<u64> {
    let gate = Arc::new(Signal63::with_spin(spin));
    let ctrl_start = Arc::new(Ctrl::new());
    let ctrl_done  = Arc::new(Ctrl::new());
    let wake_ns = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();

    let consumer = thread::spawn({
        let gate = gate.clone();
        let ctrl_start = ctrl_start.clone();
        let ctrl_done = ctrl_done.clone();
        let wake_ns = wake_ns.clone();
        let stop = stop.clone();
        let observed = observed.clone();
        move || {
            gate.set_worker(thread::current());
            loop {
                ctrl_start.wait_for(1); ctrl_start.set(0);
                if stop.load(Ordering::Relaxed) { return; }
                let v = gate.acquire();
                wake_ns.store(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                observed.store(v, Ordering::Relaxed);
                gate.lock();
                ctrl_done.set(2);
            }
        }
    });

    for i in 0..WARMUP {
        ctrl_start.set(1);
        if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
        gate.release(0xC0DE_0000 | i as u64);
        ctrl_done.wait_for(2); ctrl_done.set(0);
    }

    let mut lat = Vec::with_capacity(rounds);
    for i in 0..rounds {
        ctrl_start.set(1);
        if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
        let fire = t0.elapsed().as_nanos() as u64;
        let sent = 0xAB_0000_0000 | i as u64; // fits in 63 bits
        gate.release(sent);
        ctrl_done.wait_for(2); ctrl_done.set(0);
        let recvd = observed.load(Ordering::Relaxed);
        assert_eq!(recvd, sent, "Signal63 payload mismatch at round {i}");
        lat.push(wake_ns.load(Ordering::Relaxed).saturating_sub(fire));
    }

    stop.store(true, Ordering::Relaxed);
    ctrl_start.set(1);
    gate.release(0);
    consumer.join().unwrap();
    lat
}

fn gate63_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let gate = Signal63::with_spin(arbitro_kit::gate::DEFAULT_SPIN_ITERS);
    let t = Instant::now();
    let mut check: u64 = 0;
    for i in 0..rounds {
        let v = (i as u64) & ((1 << 63) - 1);
        black_box(&gate).release(v);
        let r = black_box(&gate).acquire();
        check ^= r; // keep compiler from eliding the load
        black_box(&gate).lock();
    }
    let e = t.elapsed();
    black_box(check);
    e
}

// ─── SignalMsg<T> prototype: full-value payload via MaybeUninit<T> slot ───────
//
// Transports an arbitrary `T: Send` by move. Zero-alloc (inline slot).
// Contract: SPSC — one producer, one consumer. The `Signal::release`'s Release
// store publishes the prior non-atomic write to `slot`; `Signal::acquire`'s
// Acquire load synchronizes with it, so the consumer's subsequent read of
// `slot` sees the stored value.

use std::mem::MaybeUninit;

#[repr(align(64))]
struct SignalMsg<T> {
    gate: Signal,
    slot: UnsafeCell<MaybeUninit<T>>,
}

// Safety: SPSC contract. Producer writes `slot` once per round before
// `gate.release()`; consumer reads `slot` once per round between
// `gate.acquire()` and `gate.lock()`. The Signal's Release/Acquire pair
// provides the happens-before for the non-atomic `slot` access.
unsafe impl<T: Send> Sync for SignalMsg<T> {}

impl<T: Send> SignalMsg<T> {
    fn with_spin(spin: u32) -> Self {
        Self {
            gate: Signal::with_spin(spin),
            slot: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    fn set_worker(&self, t: thread::Thread) {
        self.gate.set_worker(t);
    }

    /// Producer side: move `value` into the slot, then open the gate.
    /// Safety: only one producer may call this concurrently with any number
    /// of consumer reads between acquire/lock (SPSC).
    #[inline]
    fn release(&self, value: T) {
        // Non-atomic write. Published by the Release store inside `gate.release()`.
        unsafe { (*self.slot.get()).write(value); }
        self.gate.release();
    }

    /// Consumer side: block until open, then move the value out.
    /// Safety: caller must be the sole consumer (enforced by Signal::set_worker).
    #[inline]
    fn acquire(&self) -> T {
        self.gate.acquire();
        // Acquire load inside `gate.acquire()` synchronized-with the producer's
        // Release, so the previous `slot.write` is visible here.
        let v = unsafe { (*self.slot.get()).assume_init_read() };
        self.gate.lock();
        v
    }
}

// ── SignalMsg runners ────────────────────────────────────────────────────────

macro_rules! gatemsg_cross_impl {
    ($name:ident, $ty:ty, $mk:expr, $check_fn:ident) => {
        fn $name(rounds: usize, pre_fire_us: u64, spin: u32) -> Vec<u64> {
            let gate: Arc<SignalMsg<$ty>> = Arc::new(SignalMsg::with_spin(spin));
            let ctrl_start = Arc::new(Ctrl::new());
            let ctrl_done  = Arc::new(Ctrl::new());
            let wake_ns = Arc::new(AtomicU64::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            // Consumer tells parent what it observed (first byte/word) for assertion.
            let observed = Arc::new(AtomicU64::new(0));
            let t0 = Instant::now();

            let consumer = thread::spawn({
                let gate = gate.clone();
                let ctrl_start = ctrl_start.clone();
                let ctrl_done = ctrl_done.clone();
                let wake_ns = wake_ns.clone();
                let stop = stop.clone();
                let observed = observed.clone();
                move || {
                    gate.set_worker(thread::current());
                    loop {
                        ctrl_start.wait_for(1); ctrl_start.set(0);
                        if stop.load(Ordering::Relaxed) { return; }
                        let v = gate.acquire();
                        wake_ns.store(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        observed.store($check_fn(&v), Ordering::Relaxed);
                        ctrl_done.set(2);
                    }
                }
            });

            for i in 0..WARMUP {
                ctrl_start.set(1);
                if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
                gate.release($mk(i as u64));
                ctrl_done.wait_for(2); ctrl_done.set(0);
            }

            let mut lat = Vec::with_capacity(rounds);
            for i in 0..rounds {
                ctrl_start.set(1);
                if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
                let fire = t0.elapsed().as_nanos() as u64;
                let payload = $mk(i as u64);
                let expected = $check_fn(&payload);
                gate.release(payload);
                ctrl_done.wait_for(2); ctrl_done.set(0);
                let got = observed.load(Ordering::Relaxed);
                assert_eq!(got, expected, "payload mismatch at round {i}");
                lat.push(wake_ns.load(Ordering::Relaxed).saturating_sub(fire));
            }

            stop.store(true, Ordering::Relaxed);
            ctrl_start.set(1);
            gate.release($mk(0));
            consumer.join().unwrap();
            lat
        }
    };
}

fn check_u64(v: &u64) -> u64 { *v }
fn check_64b(v: &[u8; 64]) -> u64 {
    // First 8 bytes as u64.
    u64::from_le_bytes(v[..8].try_into().unwrap())
}
fn check_256b(v: &[u8; 256]) -> u64 {
    u64::from_le_bytes(v[..8].try_into().unwrap())
}

fn mk_u64(i: u64) -> u64 { 0xAB_0000_0000 | i }
fn mk_64b(i: u64) -> [u8; 64] {
    let mut a = [0u8; 64];
    a[..8].copy_from_slice(&(0xAB_0000_0000u64 | i).to_le_bytes());
    // Touch the last byte too so the whole buffer is "used".
    a[63] = (i as u8) ^ 0xA5;
    a
}
fn mk_256b(i: u64) -> [u8; 256] {
    let mut a = [0u8; 256];
    a[..8].copy_from_slice(&(0xAB_0000_0000u64 | i).to_le_bytes());
    a[255] = (i as u8) ^ 0xA5;
    a
}
fn check_4kb(v: &[u8; 4096]) -> u64 {
    u64::from_le_bytes(v[..8].try_into().unwrap())
}
fn mk_4kb(i: u64) -> [u8; 4096] {
    // Stack-allocated 4 KB. Avoid stack copy cost by writing only the first
    // few and last bytes — representative of real broker payload that doesn't
    // touch every byte during construction.
    let mut a = [0u8; 4096];
    a[..8].copy_from_slice(&(0xAB_0000_0000u64 | i).to_le_bytes());
    a[4095] = (i as u8) ^ 0xA5;
    a
}

gatemsg_cross_impl!(gatemsg_u64_cross,  u64,        mk_u64,  check_u64);
gatemsg_cross_impl!(gatemsg_64b_cross,  [u8; 64],   mk_64b,  check_64b);
gatemsg_cross_impl!(gatemsg_256b_cross, [u8; 256],  mk_256b, check_256b);
gatemsg_cross_impl!(gatemsg_4k_cross,   [u8; 4096], mk_4kb,  check_4kb);

fn gatemsg_u64_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let gate: SignalMsg<u64> = SignalMsg::with_spin(arbitro_kit::gate::DEFAULT_SPIN_ITERS);
    let t = Instant::now();
    let mut acc: u64 = 0;
    for i in 0..rounds {
        black_box(&gate).release(mk_u64(i as u64));
        acc ^= black_box(&gate).acquire();
    }
    black_box(acc);
    t.elapsed()
}

fn gatemsg_64b_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let gate: SignalMsg<[u8; 64]> = SignalMsg::with_spin(arbitro_kit::gate::DEFAULT_SPIN_ITERS);
    let t = Instant::now();
    let mut acc: u8 = 0;
    for i in 0..rounds {
        black_box(&gate).release(mk_64b(i as u64));
        let v = black_box(&gate).acquire();
        acc ^= v[0] ^ v[63];
    }
    black_box(acc);
    t.elapsed()
}

fn gatemsg_256b_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let gate: SignalMsg<[u8; 256]> = SignalMsg::with_spin(arbitro_kit::gate::DEFAULT_SPIN_ITERS);
    let t = Instant::now();
    let mut acc: u8 = 0;
    for i in 0..rounds {
        black_box(&gate).release(mk_256b(i as u64));
        let v = black_box(&gate).acquire();
        acc ^= v[0] ^ v[255];
    }
    black_box(acc);
    t.elapsed()
}

fn gatemsg_4k_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    // 4 KB inline — heap-box the Signal so we don't stack-allocate 4 KB repeatedly.
    let gate: Box<SignalMsg<[u8; 4096]>> =
        Box::new(SignalMsg::with_spin(arbitro_kit::gate::DEFAULT_SPIN_ITERS));
    let t = Instant::now();
    let mut acc: u8 = 0;
    for i in 0..rounds {
        black_box(gate.as_ref()).release(mk_4kb(i as u64));
        let v = black_box(gate.as_ref()).acquire();
        acc ^= v[0] ^ v[4095];
    }
    black_box(acc);
    t.elapsed()
}

// ─── Channel runners at larger payload sizes (apples-to-apples vs SignalMsg) ──
//
// crossbeam and mpsc at [u8;64] / [u8;256] to compare under same payload.

macro_rules! crossbeam_cross_sized {
    ($name:ident, $ty:ty, $mk:expr) => {
        fn $name(rounds: usize, pre_fire_us: u64) -> Vec<u64> {
            let (tx, rx) = crossbeam_channel::bounded::<$ty>(1);
            let ctrl_start = Arc::new(Ctrl::new());
            let ctrl_done  = Arc::new(Ctrl::new());
            let wake_ns = Arc::new(AtomicU64::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let t0 = Instant::now();

            let consumer = thread::spawn({
                let rx = rx.clone();
                let ctrl_start = ctrl_start.clone();
                let ctrl_done = ctrl_done.clone();
                let wake_ns = wake_ns.clone();
                let stop = stop.clone();
                move || {
                    loop {
                        ctrl_start.wait_for(1); ctrl_start.set(0);
                        if stop.load(Ordering::Relaxed) { return; }
                        let v = rx.recv().unwrap();
                        wake_ns.store(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        std::hint::black_box(v);
                        ctrl_done.set(2);
                    }
                }
            });

            for i in 0..WARMUP {
                ctrl_start.set(1);
                if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
                tx.send($mk(i as u64)).unwrap();
                ctrl_done.wait_for(2); ctrl_done.set(0);
            }

            let mut lat = Vec::with_capacity(rounds);
            for i in 0..rounds {
                ctrl_start.set(1);
                if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
                let fire = t0.elapsed().as_nanos() as u64;
                tx.send($mk(i as u64)).unwrap();
                ctrl_done.wait_for(2); ctrl_done.set(0);
                lat.push(wake_ns.load(Ordering::Relaxed).saturating_sub(fire));
            }

            stop.store(true, Ordering::Relaxed);
            ctrl_start.set(1);
            let _ = tx.send($mk(0));
            consumer.join().unwrap();
            lat
        }
    };
}

macro_rules! mpsc_cross_sized {
    ($name:ident, $ty:ty, $mk:expr) => {
        fn $name(rounds: usize, pre_fire_us: u64) -> Vec<u64> {
            let (tx, rx) = sync_channel::<$ty>(1);
            let ctrl_start = Arc::new(Ctrl::new());
            let ctrl_done  = Arc::new(Ctrl::new());
            let wake_ns = Arc::new(AtomicU64::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let t0 = Instant::now();

            let consumer = thread::spawn({
                let ctrl_start = ctrl_start.clone();
                let ctrl_done = ctrl_done.clone();
                let wake_ns = wake_ns.clone();
                let stop = stop.clone();
                move || {
                    loop {
                        ctrl_start.wait_for(1); ctrl_start.set(0);
                        if stop.load(Ordering::Relaxed) { return; }
                        let v = rx.recv().unwrap();
                        wake_ns.store(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        std::hint::black_box(v);
                        ctrl_done.set(2);
                    }
                }
            });

            for i in 0..WARMUP {
                ctrl_start.set(1);
                if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
                tx.send($mk(i as u64)).unwrap();
                ctrl_done.wait_for(2); ctrl_done.set(0);
            }

            let mut lat = Vec::with_capacity(rounds);
            for i in 0..rounds {
                ctrl_start.set(1);
                if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
                let fire = t0.elapsed().as_nanos() as u64;
                tx.send($mk(i as u64)).unwrap();
                ctrl_done.wait_for(2); ctrl_done.set(0);
                lat.push(wake_ns.load(Ordering::Relaxed).saturating_sub(fire));
            }

            stop.store(true, Ordering::Relaxed);
            ctrl_start.set(1);
            let _ = tx.send($mk(0));
            consumer.join().unwrap();
            lat
        }
    };
}

crossbeam_cross_sized!(crossbeam_64b_cross,  [u8; 64],  mk_64b);
crossbeam_cross_sized!(crossbeam_256b_cross, [u8; 256], mk_256b);
crossbeam_cross_sized!(crossbeam_4k_cross,   [u8; 4096], mk_4kb);
mpsc_cross_sized!(mpsc_64b_cross,  [u8; 64],  mk_64b);
mpsc_cross_sized!(mpsc_256b_cross, [u8; 256], mk_256b);
mpsc_cross_sized!(mpsc_4k_cross,   [u8; 4096], mk_4kb);

fn crossbeam_64b_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let (tx, rx) = crossbeam_channel::bounded::<[u8; 64]>(1);
    let t = Instant::now();
    let mut acc: u8 = 0;
    for i in 0..rounds {
        black_box(&tx).send(mk_64b(i as u64)).unwrap();
        let v = black_box(&rx).recv().unwrap();
        acc ^= v[0] ^ v[63];
    }
    black_box(acc);
    t.elapsed()
}

fn crossbeam_256b_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let (tx, rx) = crossbeam_channel::bounded::<[u8; 256]>(1);
    let t = Instant::now();
    let mut acc: u8 = 0;
    for i in 0..rounds {
        black_box(&tx).send(mk_256b(i as u64)).unwrap();
        let v = black_box(&rx).recv().unwrap();
        acc ^= v[0] ^ v[255];
    }
    black_box(acc);
    t.elapsed()
}

fn crossbeam_4k_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let (tx, rx) = crossbeam_channel::bounded::<[u8; 4096]>(1);
    let t = Instant::now();
    let mut acc: u8 = 0;
    for i in 0..rounds {
        black_box(&tx).send(mk_4kb(i as u64)).unwrap();
        let v = black_box(&rx).recv().unwrap();
        acc ^= v[0] ^ v[4095];
    }
    black_box(acc);
    t.elapsed()
}

fn mpsc_64b_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let (tx, rx) = sync_channel::<[u8; 64]>(1);
    let t = Instant::now();
    let mut acc: u8 = 0;
    for i in 0..rounds {
        black_box(&tx).send(mk_64b(i as u64)).unwrap();
        let v = black_box(&rx).recv().unwrap();
        acc ^= v[0] ^ v[63];
    }
    black_box(acc);
    t.elapsed()
}

fn mpsc_256b_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let (tx, rx) = sync_channel::<[u8; 256]>(1);
    let t = Instant::now();
    let mut acc: u8 = 0;
    for i in 0..rounds {
        black_box(&tx).send(mk_256b(i as u64)).unwrap();
        let v = black_box(&rx).recv().unwrap();
        acc ^= v[0] ^ v[255];
    }
    black_box(acc);
    t.elapsed()
}

fn mpsc_4k_single(rounds: usize) -> Duration {
    use std::hint::black_box;
    let (tx, rx) = sync_channel::<[u8; 4096]>(1);
    let t = Instant::now();
    let mut acc: u8 = 0;
    for i in 0..rounds {
        black_box(&tx).send(mk_4kb(i as u64)).unwrap();
        let v = black_box(&rx).recv().unwrap();
        acc ^= v[0] ^ v[4095];
    }
    black_box(acc);
    t.elapsed()
}

// ─── RTT (round-trip) runners: Channel vs crossbeam pair vs mpsc pair ───
//
// Each RTT round = client sends Req → server echoes → client receives Resp.
// Latency is the full wall time of that round as observed by the client.

/// Channel<Req, Resp> echo-server RTT. Server loops forever; process
/// exit cleans it up (we can't easily send a "shutdown" without special
/// shutdown sentinels in the generic payload).
macro_rules! gatechannel_rtt {
    ($name:ident, $ty:ty, $mk:expr, $check:expr) => {
        fn $name(rounds: usize, pre_fire_us: u64) -> Vec<u64> {
            let ch = Arc::new(Channel::<$ty, $ty>::new());
            let ready = Arc::new(AtomicBool::new(false));
            let ch_s = ch.clone();
            let ready_s = ready.clone();
            let _server = thread::spawn(move || {
                ch_s.set_server(thread::current());
                ready_s.store(true, Ordering::Release);
                loop {
                    // Echo back the request verbatim — keeps measurement
                    // focused on handshake cost, not payload transformation.
                    ch_s.serve_one(|req| req);
                }
            });
            while !ready.load(Ordering::Acquire) { thread::yield_now(); }
            ch.set_client(thread::current());

            for i in 0..WARMUP {
                if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
                let _ = ch.call($mk(i as u64));
            }
            let mut lats = Vec::with_capacity(rounds);
            for i in 0..rounds {
                if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
                let req = $mk(i as u64);
                let expected = $check(&req);
                let t0 = Instant::now();
                let resp = ch.call(req);
                lats.push(t0.elapsed().as_nanos() as u64);
                assert_eq!($check(&resp), expected, "Channel echo mismatch at round {i}");
            }
            // Server is forgotten on purpose; subprocess exit cleans up.
            lats
        }
    };
}

gatechannel_rtt!(gatechannel_u64_rtt,  u64,         mk_u64,  check_u64);
gatechannel_rtt!(gatechannel_64b_rtt,  [u8; 64],    mk_64b,  check_64b);
gatechannel_rtt!(gatechannel_256b_rtt, [u8; 256],   mk_256b, check_256b);
gatechannel_rtt!(gatechannel_4k_rtt,   [u8; 4096],  mk_4kb,  check_4kb);

/// crossbeam "pair": two bounded(1) channels, one each direction.
/// Industry-standard way to do request/response over channels.
macro_rules! cbpair_rtt {
    ($name:ident, $ty:ty, $mk:expr, $check:expr) => {
        fn $name(rounds: usize, pre_fire_us: u64) -> Vec<u64> {
            let (tx_req,  rx_req)  = crossbeam_channel::bounded::<$ty>(1);
            let (tx_resp, rx_resp) = crossbeam_channel::bounded::<$ty>(1);
            let _server = thread::spawn(move || {
                while let Ok(req) = rx_req.recv() {
                    if tx_resp.send(req).is_err() { return; }
                }
            });
            for i in 0..WARMUP {
                if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
                tx_req.send($mk(i as u64)).unwrap();
                let _ = rx_resp.recv().unwrap();
            }
            let mut lats = Vec::with_capacity(rounds);
            for i in 0..rounds {
                if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
                let req = $mk(i as u64);
                let expected = $check(&req);
                let t0 = Instant::now();
                tx_req.send(req).unwrap();
                let resp = rx_resp.recv().unwrap();
                lats.push(t0.elapsed().as_nanos() as u64);
                assert_eq!($check(&resp), expected, "crossbeam pair mismatch at round {i}");
            }
            lats
        }
    };
}

cbpair_rtt!(cbpair_u64_rtt,  u64,         mk_u64,  check_u64);
cbpair_rtt!(cbpair_64b_rtt,  [u8; 64],    mk_64b,  check_64b);
cbpair_rtt!(cbpair_256b_rtt, [u8; 256],   mk_256b, check_256b);
cbpair_rtt!(cbpair_4k_rtt,   [u8; 4096],  mk_4kb,  check_4kb);

/// std::mpsc "pair": two sync_channel(1) channels, one each direction.
macro_rules! mpscpair_rtt {
    ($name:ident, $ty:ty, $mk:expr, $check:expr) => {
        fn $name(rounds: usize, pre_fire_us: u64) -> Vec<u64> {
            let (tx_req,  rx_req)  = sync_channel::<$ty>(1);
            let (tx_resp, rx_resp) = sync_channel::<$ty>(1);
            let _server = thread::spawn(move || {
                while let Ok(req) = rx_req.recv() {
                    if tx_resp.send(req).is_err() { return; }
                }
            });
            for i in 0..WARMUP {
                if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
                tx_req.send($mk(i as u64)).unwrap();
                let _ = rx_resp.recv().unwrap();
            }
            let mut lats = Vec::with_capacity(rounds);
            for i in 0..rounds {
                if pre_fire_us > 0 { thread::sleep(Duration::from_micros(pre_fire_us)); }
                let req = $mk(i as u64);
                let expected = $check(&req);
                let t0 = Instant::now();
                tx_req.send(req).unwrap();
                let resp = rx_resp.recv().unwrap();
                lats.push(t0.elapsed().as_nanos() as u64);
                assert_eq!($check(&resp), expected, "mpsc pair mismatch at round {i}");
            }
            lats
        }
    };
}

mpscpair_rtt!(mpscpair_u64_rtt,  u64,         mk_u64,  check_u64);
mpscpair_rtt!(mpscpair_64b_rtt,  [u8; 64],    mk_64b,  check_64b);
mpscpair_rtt!(mpscpair_256b_rtt, [u8; 256],   mk_256b, check_256b);
mpscpair_rtt!(mpscpair_4k_rtt,   [u8; 4096],  mk_4kb,  check_4kb);

// ─── Zero-copy via ownership transfer (Vec<u8>) ─────────────────────────────
//
// `Vec<u8>` is {ptr, len, cap} = 24 bytes on stack; heap buffer of `len` lives
// in DRAM. Moving a Vec across a channel transfers the *pointer* — the heap
// bytes never move. Measurement isolates handshake + metadata copy from
// payload copy.

fn mk_vec_4k(i: u64) -> Vec<u8> {
    let mut v = vec![0u8; 4096];
    v[..8].copy_from_slice(&(0xAB_0000_0000u64 | i).to_le_bytes());
    v[4095] = (i as u8) ^ 0xA5;
    v
}
fn mk_vec_64k(i: u64) -> Vec<u8> {
    let mut v = vec![0u8; 65_536];
    v[..8].copy_from_slice(&(0xAB_0000_0000u64 | i).to_le_bytes());
    v[65_535] = (i as u8) ^ 0xA5;
    v
}
fn mk_vec_1m(i: u64) -> Vec<u8> {
    let mut v = vec![0u8; 1_048_576];
    v[..8].copy_from_slice(&(0xAB_0000_0000u64 | i).to_le_bytes());
    v[1_048_575] = (i as u8) ^ 0xA5;
    v
}
fn check_vec(v: &Vec<u8>) -> u64 {
    u64::from_le_bytes(v[..8].try_into().unwrap())
}

gatechannel_rtt!(gatechannel_vec4k_rtt,  Vec<u8>, mk_vec_4k,  check_vec);
gatechannel_rtt!(gatechannel_vec64k_rtt, Vec<u8>, mk_vec_64k, check_vec);
gatechannel_rtt!(gatechannel_vec1m_rtt,  Vec<u8>, mk_vec_1m,  check_vec);

cbpair_rtt!(cbpair_vec4k_rtt,  Vec<u8>, mk_vec_4k,  check_vec);
cbpair_rtt!(cbpair_vec64k_rtt, Vec<u8>, mk_vec_64k, check_vec);
cbpair_rtt!(cbpair_vec1m_rtt,  Vec<u8>, mk_vec_1m,  check_vec);
