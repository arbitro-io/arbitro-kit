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

`Signal` is the atom. A small family of composites ships in the crate,
each a thin wrapper — most of the cost model carries over unchanged.
A second low-level primitive, `Park`, exposes the stateless park/unpark
half of `Signal` for callers that already track readiness in their own
state (used internally by `Ring` and `Mpmc`):

The crate is organized in four modules by **what the user wants**, not
by what's under the hood:

| Module | Question it answers | Types |
| :----- | :------------------ | :---- |
| [`gate`](src/gate/)     | "How do I synchronize?" (no payload)            | `Signal`, `SignalSet`, `Park` |
| [`slot`](src/slot/)     | "1 message in flight, no buffer?"               | `Pipe`, `Channel` |
| [`stream`](src/stream/) | "FIFO of messages?"                             | `Ring`, `Stream`, `Duplex`, `BufferedSender` |
| [`route`](src/route/)   | "N→M with topology?"                            | `Hub`, `Mpmc` |

| Type         | Module | Shape                                     | What it adds                                                        | Docs |
| :----------- | :----- | :---------------------------------------- | :------------------------------------------------------------------ | :--- |
| `Signal`     | gate   | single-bit M:1 signal                     | the primitive itself; BYO-atomic via `from_bool` / `from_bit`        | [signal.md](docs/signal.md) |
| `Park`       | gate   | stateless park/unpark                     | wait on caller-owned readiness state (no duplicated `AtomicBool`)   | (used by `Ring`, `Mpmc`) |
| `SignalSet`  | gate   | up to 64 bits in one `AtomicU64`          | wait for any / all / subset of named signals                        | [signalset.md](docs/signalset.md) |
| `Lifeline`   | gate   | up to 64 indexed waiters, fire-and-forget | external cancellation: `cancel_one` / `cancel_mask` / `cancel_all`; `recv_or_cancel` opt-in across transports (+0.7 ns vs baseline) | [lifeline.md](docs/lifeline.md) |
| `Pipe<T, H>` | slot   | SPSC single-slot (1 × `Signal`)           | minimal payload transport with zero-cost observer hooks             | [pipe.md](docs/pipe.md) |
| `Channel<Req, Resp>` | slot | SPSC request/response (2 × `Signal`) | zero-copy round-trip with ownership transfer                        | [channel.md](docs/channel.md) |
| `Ring<T, CAP>` | stream | SPSC bounded ring (2 × `Park`)          | burst absorption, pipelined throughput, batch send + batch ack      | [ring.md](docs/ring.md) |
| `Stream<T>`  | stream | SPSC unbounded sequenced log (linked segments + `Park`) | fire-and-forget producer + `Receipt`-based delivery verification (3.5 ns/RT batched) | [stream.md](docs/stream.md) |
| `Duplex<A, B>` | stream | bidirectional unbounded SPSC (2 × `Stream`) | type-safe paired send/recv each direction, zero-overhead wrapper, 2.0 ns/RT verified at K=512 | [duplex.md](docs/duplex.md) |
| `Hub<In, Out>` | route  | N:1 multiplexer (`SignalSet` + N × `Pipe`) | fanout from N producers to 1 drain, with per-port reply + shutdown  | [hub.md](docs/hub.md) |
| `Mpmc<T, RING_CAP>` | route | M:N sharded channel (N × `SignalSet` + M×N SPSC mini-rings) | high-throughput broker: M producers → N consumers with batched send | [mpmc.md](docs/mpmc.md) |

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

**`Channel<Req, Resp>`** — **102 ns p50 handshake round-trip**, within
~15 ns of the physical cross-core L1↔L1 coherence floor. Zero-copy
ownership transfer: `Vec<u8>` of 1 MB transfers at ~67 GB/s effective
throughput, `Arc<Vec<u8>>` of 16 MB at ~103 TB/s effective (pointer
clone, nothing physically moves). Beats `crossbeam::channel` 3.2× on
handshake latency and `std::mpsc` 205×. Panic-safe: a handler panic
poisons the channel and wakes the blocked client cleanly.
[→ channel.md](docs/channel.md)

**`Hub<In, Out>`** — 12.5 ns/op send + drain local, 89 ns p50 full
cross-thread RTT. N producers coalesce into one `AtomicU64` via
`SignalSet`, one atomic OR per send regardless of N. Built-in
shutdown bit wakes the drain cleanly without external signaling.
Max 63 ports; shard across multiple Hubs for higher throughput.
[→ hub.md](docs/hub.md)

**`Mpmc<T, RING_CAP>`** — M:N sharded channel. Per-item `send` at
~33 ns p50 (`8P/1C`) and ~22 ns p50 (`8P/8C`). With the `try_send_batch`
path amortizing one `fetch_or` over up to `RING_CAP` items:
**0.74 ns/op p50 at `8P/8C` → ~1.03 G ops/sec, 116× faster than
`crossbeam::channel::bounded(1024)` at the same shape**. Level-triggered
bits mean the Signal contract is honored — a stray `lock_mask` can
never strand a pending message. Drop-safe, shutdown-safe,
backpressure per producer. [→ mpmc.md](docs/mpmc.md)

**`Stream<T>`** — SPSC unbounded sequenced log. **3.0 ns/op
cross-thread** send (per-item, no backpressure check), **2.9 ns/op**
batched via `send_iter K=256`. The producer never blocks; segments
grow on demand. Each `send` returns a `Receipt` for O(1) delivery
verification (`is_delivered` = one Acquire load) or blocking wait
(`wait_delivered`). `BufferedSender` wraps the per-item API to give
batched throughput via a local accumulator. [→ stream.md](docs/stream.md)

**`Duplex<A, B>`** — bidirectional unbounded SPSC over two `Stream`s.
**Zero-overhead wrapper** (3.0 vs 3.0 ns vs raw `Stream`). Each end
sends one type and receives the other, type-checked at compile time.
Fire-and-forget + `is_delivered` poll: **1.7 ns/op** = 585 M ops/s.
`send_iter` + `wait_delivered` at K=512: **2.0 ns per verified
round-trip** = 488 M RT/s — the fastest verified-RT number in the
crate. [→ duplex.md](docs/duplex.md)

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
use arbitro_kit::slot::Channel;

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
use arbitro_kit::slot::Pipe;

let p: Pipe<u64> = Pipe::new();
p.send(42);
assert_eq!(p.recv(), 42);
```

### Unbounded sequenced log (`Stream<T>`)

```rust
use arbitro_kit::stream::Stream;
use std::sync::Arc;

let stream: Arc<Stream<u64>> = Arc::new(Stream::new());

// Producer: never blocks. Receipt carries the seq for verification.
let r = stream.send(42);

// Consumer (separate thread): set_consumer first, then recv.
let s2 = stream.clone();
std::thread::spawn(move || {
    s2.set_consumer(std::thread::current());
    while let Some(v) = s2.try_recv() {
        // process v
        let _ = v;
    }
});

// Verify delivery from any thread holding the receipt — one Acquire load.
if r.is_delivered(&stream) { /* peer drained past seq */ }
```

For batched throughput with a single-send API, wrap with
`BufferedSender::new(stream.clone(), 64)` and call `tx.send(v)` —
the wrapper accumulates locally and flushes via `send_iter` every K.

### Bidirectional duplex (`Duplex<A, B>`)

```rust
use arbitro_kit::stream::Duplex;

// Each end has a fixed direction: left sends `Req` and receives `Resp`.
let (client, server) = Duplex::<Request, Response>::pair();

// Fire and forget; keep the receipt for later verification.
let r = client.send(req);

// Or send a batch and verify delivery once.
let r = client.send_iter(many_reqs).unwrap();
client.wait_delivered(r);   // blocks until peer drained the whole batch
```

`Duplex` is a zero-overhead wrapper over two `Stream`s; the
direction is enforced at compile time, so the producer can't drain
its own outbound by mistake.

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
- **Panic-safe handlers.** `Channel::serve_one` / `serve_loop` poison
  the channel on handler panic, wake the blocked client, and surface
  the failure as a panic on the next `call` — no silent hangs.

---

## Non-goals

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
- [x] `Channel<Req, Resp>` — SPSC zero-copy request/response,
      panic-safe, 64-byte aligned for sub-110 ns handshake
- [x] `Hub<In, Out>` — N:1 multiplexer with per-port reply + shutdown
- [x] `Mpmc<T, RING_CAP>` — M:N sharded channel with per-(producer,shard)
      mini-rings, level-triggered bits, batched `try_send_batch` path,
      panic-safe Drop, built-in shutdown
- [x] `Stream<T>` — SPSC unbounded sequenced log with `Receipt`-based
      delivery verification; `BufferedSender` accumulator; opt-in
      `strict_wake` mode for bidirectional patterns
- [x] `Duplex<A, B>` — bidirectional unbounded SPSC over two `Stream`s
      with type-safe direction; built on `strict_wake` so 1M-scale
      lockstep RPC is deadlock-free
- [x] `Lifeline` — fire-and-forget cancellation scope (up to 64 waiters
      per scope), with `recv_or_cancel` on `Stream` / `Ring` / `Duplex`;
      opt-in (+0.7 ns vs baseline `recv`), zero impact on existing
      `recv()` callers

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
cargo bench --bench gate_overhead        # Channel vs crossbeam vs mpsc
cargo bench --bench hub_overhead         # Hub throughput + RTT
cargo bench --bench mpmc_overhead        # Mpmc MP/NC sweep + batched + crossbeam
cargo bench --bench fanin_h2h            # Hub vs Mpmc vs crossbeam_channel fan-in
cargo bench --bench ring_vs_crossbeam    # SPSC apples-to-apples vs crossbeam
cargo bench --bench hub_sparse           # Hub drain on sparse-bit fan-in
cargo bench --bench hub_multibit         # Hub drain on multi-bit fan-in
cargo bench --bench ring_byo_atomic      # Ring with `Signal::from_bit` BYO-atomic
cargo bench --bench stream_overhead      # Stream send / send_iter / ack-RTT / lockstep
cargo bench --bench duplex_overhead      # Duplex zero-overhead check + RPC patterns + fire-and-forget
cargo bench --bench rpc_patterns         # lockstep / busy-spin / batched / buffered / ack-RTT
cargo bench --bench ring_vs_disruptor    # Ring vs LMAX-port disruptor SPSC
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
