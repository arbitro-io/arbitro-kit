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
consumer `acquire()`s and parks when idle. One `SeqCst` barrier, paid
**once per park event**, closes the Dekker race; every other hot-path
op is `Relaxed` / `Release` / `Acquire`.

| Path                              |              Cost |
| --------------------------------- | ----------------: |
| `release()` — consumer spinning   |            ~0.6 ns |
| `release()` — consumer parked     |      ~7 µs (syscall) |
| `acquire()` fast path             |            ~0.3 ns |
| `acquire()` park extra cost       |   +20 ns (1 SeqCst) |
| CPU while parked                  |                0% |

**→ [docs/signal.md](docs/signal.md)** for the full wire diagram,
correctness notes across x86/ARM, and the concurrency model.

---

## What you can build on top

`Signal` is the atom. Six composites ship in the crate, each a thin
wrapper — most of the cost model carries over unchanged:

| Type         | Shape                                     | What it adds                                                        | Docs |
| :----------- | :---------------------------------------- | :------------------------------------------------------------------ | :--- |
| `Signal`     | single-bit M:1 signal                     | the primitive itself                                                | [signal.md](docs/signal.md) |
| `SignalSet`  | up to 64 bits in one `AtomicU64`          | wait for any / all / subset of named signals                        | [signalset.md](docs/signalset.md) |
| `Pipe<T, H>` | SPSC single-slot (1 × `Signal`)           | minimal payload transport with zero-cost observer hooks             | [pipe.md](docs/pipe.md) |
| `Ring<T, CAP>` | SPSC N-slot pipelined queue (2 × `Signal`) | burst absorption, pipelined throughput, batch send + batch ack      | [ring.md](docs/ring.md) |
| `Channel<Req, Resp>` | SPSC request/response (2 × `Signal`) | zero-copy round-trip with ownership transfer                        | [channel.md](docs/channel.md) |
| `Hub<In, Out>` | N:1 multiplexer (`SignalSet` + N × `Pipe`) | fanout from N producers to 1 drain, with per-port reply + shutdown  | [hub.md](docs/hub.md) |

### Quick fragments

**`Pipe<T, H>`** — 0.64 ns/op single-thread, 110 ns/cycle round-trip
cross-thread. For bulk transfer, `Pipe<Vec<T>>` at B=256 does 2.96
ns/item — **2.8× faster than `Ring`'s batch API** (ownership move
vs item-by-item copy). Optional zero-cost observer hooks via a ZST
generic; `Box<dyn Fn>` control measured at ~4× overhead, which is why
hooks are not embedded in `Signal`. [→ pipe.md](docs/pipe.md)

**`Ring<T, CAP>`** — 1.02 ns/op single-thread, 30–80 ns/op cross-thread
per-item, 8.2 ns/item batched. Both directions expose panic-safe batch
APIs (`try_send_from`, `drain_into`) that amortize cursor publish +
signal wakeup over the whole batch. Payload sweep from 64 B to 64 KB
shows pool (recycled `Box<T>`) wins above 256 B by 2–54× vs inline,
and fresh `Box::new` loses to inline below 16 KB because malloc cost
exceeds memcpy. [→ ring.md](docs/ring.md)

**`Channel<Req, Resp>`** — 137 ns p50 handshake round-trip. Zero-copy
ownership transfer: `Vec<u8>` of 1 MB transfers at ~73 GB/s effective
throughput (8-byte pointer + Release/Acquire, nothing physically
moves). Beats `crossbeam::channel` 3× on handshake latency and
`std::mpsc` 160×. [→ channel.md](docs/channel.md)

**`Hub<In, Out>`** — 12.5 ns/op send + drain local, 89 ns p50 full
cross-thread RTT. N producers coalesce into one `AtomicU64` via
`SignalSet`, one atomic OR per send regardless of N. Built-in
shutdown bit wakes the drain cleanly without external signaling.
Max 63 ports; shard across multiple Hubs for higher throughput.
[→ hub.md](docs/hub.md)

---

## Usage — quick-start snippets

See each primitive's doc file for full examples and cost breakdowns.

### Single signal

```rust
use arbitro_kit::gate::Signal;
use std::sync::Arc;

let sig = Arc::new(Signal::new());

// Consumer
let s = sig.clone();
std::thread::spawn(move || {
    s.set_worker(std::thread::current());
    loop {
        s.acquire();
        // do work ...
        s.lock();
    }
});

// Producer — any thread, any number.
sig.release();
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
assert_eq!(client.call(21), 42);
```

Works transparently with `Box<T>`, `Vec<T>`, `Arc<T>`, `File` — any
`Send` type. Ownership transfers; the heap allocation stays put.

### Single-slot pipe

```rust
use arbitro_kit::gate::Pipe;

let p: Pipe<u64> = Pipe::new();
p.send(42);
assert_eq!(p.recv(), 42);
```

---

## Guarantees

- **Lock-free producer side.** `release` / `lock` / `is_open` are
  single atomic ops — no mutex, no syscall in the common case.
- **0% CPU when idle.** Consumer parks via `std::thread::park` after a
  short spin window (64 tight + 512 PAUSE by default).
- **No allocations on the hot path.** Fixed-size state, inline slots.
- **No external deps.** `std` only. `crossbeam` is a **dev-dependency**,
  used only by the comparison bench.
- **Drop-safe.** All composites clean up in-flight payloads on teardown.
- **Panic-safe batch APIs.** `Ring::try_send_from` and `Ring::drain_into`
  are unwind-safe: partial-progress state stays consistent, no UB, no
  leaks.

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

## Roadmap

Shipped today:

- [x] `Signal` — M:1 single-bit signal with Dekker-safe park
- [x] `SignalSet` — up to 64 coalesced signals in one `AtomicU64`
- [x] `Pipe<T, H>` — SPSC single-slot with zero-cost observer hook
- [x] `Ring<T, CAP>` — SPSC bounded ring with batch send + batch ack,
      panic-safe, payload-sweep-documented
- [x] `Channel<Req, Resp>` — SPSC zero-copy request/response
- [x] `Hub<In, Out>` — N:1 multiplexer with per-port reply + shutdown

Next:

- [ ] **`BufferPool<T, CAP>`** — pre-allocated recycling pool for
      `Ring<Box<T>>`-style transports. Measured to win by 2–54× vs
      fresh `Box::new` or inline storage for payloads above 256 B.
- [ ] **`Fan<T>`** — 1:N broadcast over N `Pipe`s with per-consumer
      backpressure. Same zero-cost hook contract as `Pipe`.
- [ ] **`Queue<T>`** — MPSC unbounded built on a `Ring` per producer +
      a `SignalSet` drain. Lock-free enqueue, batched drain.
- [ ] **Async adapters** — `Future`-based `acquire` without giving up
      the synchronous lock-free producer side. Opt-in feature, no
      runtime dependency.
- [ ] **`no_std` core** — feature-gated extraction of `Signal` and
      `SignalSet` for embedded / freestanding targets (park via a
      user-provided waiter trait).
- [ ] **Loom model checks** — permutation testing of the park protocol
      under weak memory models beyond what `miri` already covers.
- [ ] **ARM64 numbers** — current cost tables are x86_64; validate on
      aarch64 (where `SeqCst` on the park path is load-bearing).

---

## Benchmarks

Every number in the docs is reproducible:

```bash
cargo bench --bench signal_compare       # Signal vs raw AtomicBool
cargo bench --bench pipe_overhead        # Pipe ST/XT + hook zero-cost claim
cargo bench --bench ring_overhead        # Ring FLOW / ROUND-TRIP / payload sweep
cargo bench --bench gate_channel_focus   # Channel vs crossbeam vs mpsc
cargo bench --bench hub_overhead         # Hub throughput + RTT
```

For publication-grade numbers on Linux, pin the producer/consumer
to dedicated cores and lock CPU frequency:

```bash
sudo cpupower frequency-set -g performance
taskset -c 0,1 cargo bench --bench ring_overhead
```

---

## License

MIT.
