# `Signal` — M:1 single-bit gate

[← back to README](../README.md)

`Signal` is the atom every other primitive in `arbitro-kit` is built on.
It behaves like a gate: many producers `release()` it lock-free, a single
consumer `acquire()`s it and parks when idle. Conceptually a single bit,
but the interplay between its two `AtomicBool`s (state + parked) is what
makes it fast.

## Wire diagram

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

## Cost (x86_64, commodity laptop, WSL Linux)

| Path                              |              Cost |
| --------------------------------- | ----------------: |
| `release()` — consumer spinning   |            ~0.6 ns |
| `release()` — consumer parked     |      ~7 µs (syscall) |
| `acquire()` fast path             |            ~0.3 ns |
| `acquire()` park extra cost       |   +20 ns (1 SeqCst) |
| Struct size                       |     64 B (aligned) |
| CPU while parked                  |                0% |

Within noise of a raw `AtomicBool::store` in the hot case, and within a
syscall of a perfect park/unpark in the cold case. No known cheaper M:1
signal primitive exists in safe Rust.

## Correctness across architectures

- **x86 / x86_64 (TSO)**: the race between the two relaxed load/store
  pairs is masked by strong memory ordering + store-buffer drain (~tens
  of cycles, while the spin window runs ~15 µs).
- **ARM / aarch64 (weakly ordered)**: that mask does not exist. The
  consumer's `SeqCst` store on `parked` + recheck of `locked` is what
  guarantees forward progress. Do not weaken it.

## Concurrency model

Exactly **one consumer** may call `acquire()` / `set_worker()`. Any
number of producers may call `release()` / `lock()` / `is_open()`
concurrently from any thread without synchronization between them.

## Usage

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
