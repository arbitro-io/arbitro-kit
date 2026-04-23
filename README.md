# arbitro-kit

> Zero-dependency synchronization primitives extracted from the Arbitro broker.

`arbitro-kit` is a small, `std`-only toolkit of low-latency primitives for
producer → consumer coordination. No runtime dependencies, no features,
no async runtime assumed. The whole crate compiles in a couple of seconds
and weighs a few KB of object code.

It exists for one reason: to make the moment where a producer tells a
consumer *"go"* as cheap as physics allows, and the moment where an idle
consumer tells the OS *"wake me when there's work"* cost 0% CPU.

---

## The `Signal` primitive

Everything in this crate is built on top of `Signal` — an M:1 signal that
behaves like a gate. Producers `release()` it (lock-free); a single
consumer `acquire()`s and parks when idle. Conceptually it's a single
bit, but the interplay between its two `AtomicBool`s (state + parked)
is what makes it fast:

```text
producer                               consumer
────────                               ────────
locked.store(false, Release)   ─────→  locked.load(Acquire)           // fast-path
if parked.load(Relaxed):               ↓ (if still locked)
    worker.unpark()                    tight spin (64×)
                                       PAUSE spin (512×)
                                       parked.store(true, SeqCst)     // ← once per park
                                       recheck locked → park()
```

One `SeqCst` barrier, paid **once per park event**, closes the Dekker
race between the producer's `locked.store + parked.load` pair and the
consumer's `parked.store + locked.load` pair. Every other operation on
the hot path is `Relaxed` / `Release` / `Acquire`.

### Cost (x86_64, commodity laptop, WSL Linux)

| Path                              |              Cost |
| --------------------------------- | ----------------: |
| `release()` — consumer spinning   |            ~0.6 ns |
| `release()` — consumer parked     |      ~7 µs (syscall) |
| `acquire()` fast path             |            ~0.3 ns |
| `acquire()` park extra cost       |   +20 ns (1 SeqCst) |
| Struct size                       |     64 B (aligned) |
| CPU while parked                  |                0% |

That's within noise of a raw `AtomicBool::store` in the hot case, and
within a syscall of a perfect park/unpark in the cold case. No known
cheaper M:1 signal primitive exists in safe Rust.

---

## What you can build on top

`Signal` is the atom. Three composites ship in the crate, each a thin
wrapper — most of the cost model carries over unchanged:

| Type         | Shape                                     | What it adds |
| :----------- | :---------------------------------------- | :----------- |
| `Signal`     | single-bit M:1 signal                     | the primitive itself |
| `SignalSet`  | up to 64 bits in one `AtomicU64`          | wait for any / all / subset of named signals |
| `Channel<Req, Resp>` | SPSC request/response (2 × `Signal`) | zero-copy round-trip with ownership transfer |

### Round-trip channel — head-to-head with `crossbeam` and `mpsc`

`Channel` is just **two `Signal`s on separate cache lines** plus a pair
of `MaybeUninit` slots. The cost is literally `2 × Signal` + one cache
line's worth of padding — and that shows up in the numbers:

```
── Handshake (zero payload) ──
primitive                p50_ns     p99_ns          ops/sec          MB/s
────────────────────────────────────────────────────────────────────────
Channel                     137        210         6_850_000          —
crossbeam pair              450        720         2_200_000          —
mpsc pair                 22_300     31_000            44_000          —

── [u8; 4096] by-value ──
Channel                     190        260         5_200_000       21_300
crossbeam pair              540        810         1_840_000        7_500

── Vec<u8> 1 MB (ownership transfer — zero copy) ──
Channel                     235        310         4_250_000       73_000
crossbeam pair              650        960         1_530_000       26_000

── Arc<Vec<u8>> 16 MB (shared — pointer clone, no copy) ──
Channel                     151        220         6_600_000       87_600
crossbeam pair              480        760         2_080_000       27_000
```

Throughput at 1 MB and 16 MB exceeds DRAM bandwidth because nothing
physically moves: ownership of the `Vec`/`Arc` transfers across the
signal, which is an 8-byte pointer and a Release/Acquire pair. The MB/s
column is the *effective* throughput — "if this were a copy, it would
equal this."

Reproduce with:

```bash
cargo bench --bench gate_channel_focus
```

---

## Usage

### Single signal

```rust
use arbitro_kit::gate::Signal;
use std::sync::Arc;

let sig = Arc::new(Signal::new());

// Consumer
{
    let s = sig.clone();
    std::thread::spawn(move || {
        s.set_worker(std::thread::current());
        loop {
            s.acquire();
            // do work ...
            s.lock();
        }
    });
}

// Producer — any thread, any number.
sig.release();
```

### Signal set (wait for any/all of up to 64 signals)

```rust
use arbitro_kit::gate::SignalSet;
use std::sync::Arc;

let mut flow = SignalSet::new();
let g_store = flow.create("store");
let g_drain = flow.create("drain");
let flow = Arc::new(flow);

let f = flow.clone();
std::thread::spawn(move || {
    f.set_worker(std::thread::current());
    loop {
        f.acquire_any(g_store.mask() | g_drain.mask());
        let s = f.state();
        // handle whichever fired ...
        f.lock_mask(s);
    }
});

flow.release(g_store);
```

### SPSC round-trip channel

```rust
use arbitro_kit::gate::Channel;

let (client, server) = Channel::<u64, u64>::spsc();

let h = std::thread::spawn(move || {
    server.bind();
    server.serve_loop(|req| req.wrapping_mul(2));
});

client.bind();
let r = client.call(21);
assert_eq!(r, 42);
```

Works transparently with `Box<T>`, `Vec<T>`, `Arc<T>`, `File` — any
`Send` type. Ownership transfers; the heap allocation stays put.

---

## Guarantees

- **Lock-free producer side.** `release` / `lock` / `is_open` are
  single atomic ops — no mutex, no syscall in the common case.
- **0% CPU when idle.** Consumer parks via `std::thread::park` after a
  short spin window (64 tight + 512 PAUSE by default).
- **No allocations on the hot path.** Fixed-size state, inline slots.
- **No external deps.** `std` only. `crossbeam` is a **dev-dependency**,
  used only by the comparison bench.
- **Drop-safe.** `Channel` cleans up in-flight payloads on teardown.

---

## Non-goals

- **N consumers.** Everything here is M producers : 1 consumer. Fan-out
  to multiple consumers is a different primitive.
- **Async.** Primitives are synchronous; `acquire*` parks the OS thread.
  The lock-free producer side is compatible with any async runtime, but
  `Future`-based waits are not provided.
- **Ordered multi-producer delivery.** `SignalSet` coalesces repeated
  releases on the same bit. If you need to count events, use the bit
  to wake a consumer that drains a separate queue.

---

## License

MIT.
