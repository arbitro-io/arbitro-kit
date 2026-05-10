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

## The `Waiter` trait

Everything in this crate runs over a single wait/wake contract: the
[`Waiter`](src/waiter/mod.rs) trait. Producers call `wake()` lock-free;
the single consumer calls `wait_until(predicate)` and parks when idle.
One `SeqCst` barrier, paid **once per park event**, closes the Dekker
race; every other hot-path op is `Relaxed` / `Release` / `Acquire`.

| Path                              |              Cost |
| --------------------------------- | ----------------: |
| `wake()` — consumer not parked    |            ~0.3 ns |
| `wake()` — consumer parked        |      ~7 µs (syscall) |
| `wait_until()` ready on entry     |            ~0.5 ns |
| `wait_until()` park extra cost    |   +20 ns (1 SeqCst) |
| CPU while parked                  |                0% |

Three concrete backends ship today:

- **`ParkWaiter`** (default) — `std::thread::park`/`unpark`. Sync.
- **`NotifyWaiter`** (feature `tokio`) — `tokio::sync::Notify`. Async.
- **(future) `UringWaiter`** — io_uring CQE-based. Add one file,
  every primitive inherits async support.

Every primitive is `<W: Waiter = ParkWaiter>`, so existing sync
call sites compile unchanged. Switch to async by switching the type
parameter — same struct, same semantics, different backend.

**→ [docs/waiter.md](docs/waiter.md)** for the contract, the three
concrete impls, and the io_uring extension story.
**→ [docs/signal.md](docs/signal.md)** for what happened to the old
`Signal` type (folded into `ParkWaiter`).

---

## What you can build on top

The crate is organized in five modules by **what the user wants**, not
by what's under the hood:

| Module | Question it answers | Types |
| :----- | :------------------ | :---- |
| [`waiter`](src/waiter/) | "Which wake backend?"                          | `Waiter`, `BlockingWaiter`, `AsyncWaiter`, `ParkWaiter`, `NotifyWaiter` |
| [`gate`](src/gate/)     | "How do I synchronize?" (no payload)            | `OneSignal<W>`, `SignalSet<W>`, `Lifeline` |
| [`slot`](src/slot/)     | "1 message in flight, no buffer?"               | `Pipe<T, H, W>`, `Channel<Req, Resp, W>` |
| [`stream`](src/stream/) | "FIFO of messages?"                             | `Ring<T, CAP, W>`, `Stream<T, W>`, `Duplex<A, B, W>`, `BufferedSender` |
| [`route`](src/route/)   | "N→M with topology?"                            | `Hub<In, Out, W>`, `Mpmc<T, CAP, W>`, `Mpsc<T, CAP, W>`, `OneShot<T, W>` |

| Type         | Module | Shape                                     | What it adds                                                        | Docs |
| :----------- | :----- | :---------------------------------------- | :------------------------------------------------------------------ | :--- |
| `OneSignal<W>` | gate | single-use payloadless gate               | minimal "block until released"; `acquire_timeout` (sync), `acquire_async` (async); **286 ns p50 RT-w/-ack** cross-thread | (see `src/gate/one_signal.rs`) |
| `SignalSet<W>` | gate | up to 256 bits over a chunked `Box<[AtomicU64]>` | wait for any / all / subset of named signals; ≤64-bit case stays a single `AtomicU64` | [signalset.md](docs/signalset.md) |
| `Lifeline`   | gate   | up to 64 indexed waiters, fire-and-forget | external cancellation: `cancel_one` / `cancel_mask` / `cancel_all`; `recv_or_cancel` opt-in across transports (+0.7 ns vs baseline) | [lifeline.md](docs/lifeline.md) |
| `Pipe<T, H, W>` | slot | SPSC single-slot (1 × `Waiter`)          | minimal payload transport with zero-cost observer hooks; sync `recv` or async `recv_async` per backend; **191 ns p50 RT-w/-ack** | [pipe.md](docs/pipe.md) |
| `Channel<Req, Resp, W>` | slot | SPSC request/response (2 × `Waiter`) | zero-copy round-trip with ownership transfer                        | [channel.md](docs/channel.md) |
| `OneShot<T, W>` | route | single-use payload-carrying gate         | `OneSignal` + a `T` slot; **365 ns p50 RT-w/-ack** cross-thread, **67× faster than `tokio::sync::oneshot`** | (see `src/route/oneshot.rs`) |
| `Ring<T, CAP, W>` | stream | SPSC bounded ring (2 × `Waiter`)       | burst absorption, pipelined throughput, batch send + batch ack      | [ring.md](docs/ring.md) |
| `Stream<T, W>` | stream | SPSC unbounded sequenced log (linked segments + `Waiter`) | fire-and-forget producer + `Receipt`-based delivery verification (3.5 ns/RT batched) | [stream.md](docs/stream.md) |
| `Duplex<A, B, W>` | stream | bidirectional unbounded SPSC (2 × `Stream`) | type-safe paired send/recv each direction, zero-overhead wrapper, 2.0 ns/RT verified at K=512 | [duplex.md](docs/duplex.md) |
| `Hub<In, Out, W>` | route | N:1 multiplexer (`SignalSet` + N × `Pipe`) | fanout from N producers to 1 drain, with per-port reply + shutdown  | [hub.md](docs/hub.md) |
| `Mpmc<T, CAP, W>` | route | M:N sharded channel (M×N SPSC mini-rings + per-shard `AtomicBool` wake) | high-throughput broker: M producers → N consumers with batched send; **zero `LOCK`-prefixed RMW** on the producer hot path | [mpmc.md](docs/mpmc.md) |
| `Mpsc<T, CAP, W>` | route | M:1 fan-in channel (`Waiter` + M SPSC mini-rings) | single-consumer specialisation of `Mpmc`: no shard scan, no producer cursor, ~10% faster `try_send`. `new_cloneable` for `Sender::clone()`-style ergonomics; `recv_batch_async_send` for tokio drain at ~41 ns/op (2.1× over `tokio::mpsc::recv_many`) | [mpsc.md](docs/mpsc.md) |

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
clone, nothing physically moves). Panic-safe: a handler panic poisons
the channel and wakes the blocked client cleanly.
[→ channel.md](docs/channel.md)

**`Hub<In, Out>`** — 12.5 ns/op send + drain local, 89 ns p50 full
cross-thread RTT. N producers coalesce into one `AtomicU64` via
`SignalSet`, one atomic OR per send regardless of N. Built-in
shutdown bit wakes the drain cleanly without external signaling.
Max 63 ports; shard across multiple Hubs for higher throughput.
[→ hub.md](docs/hub.md)

**`Mpmc<T, RING_CAP>`** — M:N sharded channel. Per-item `send` at
~2.4 ns p50 (`8P/1C`) and ~5.5 ns p50 (`8P/8C`). With the
`try_send_batch` path amortizing one `head.store(Release)` + one
`unpark` over up to `RING_CAP` items: **0.66 ns/op p50 at `8P/8C` →
~1.44 G ops/sec**. Beats `crossbeam::channel::bounded` by **5–7×**
on M:1 fan-in and **2.4–4.8×** on symmetric M:N. The producer hot
path is **zero `LOCK`-prefixed RMW** — the previous bitmap-based
wakeup was replaced by a per-shard `AtomicBool` + CAS-coalesced
`unpark`. Consumer uses spin-then-park to absorb sub-µs publication
gaps without paying the syscall round-trip. Drop-safe,
shutdown-safe, backpressure per producer. [→ mpmc.md](docs/mpmc.md)

**`Mpsc<T, RING_CAP>`** — single-consumer specialisation of `Mpmc`.
Same per-producer SPSC mini-ring + bitmap aggregator design, with
`N = 1` hardcoded so the `Shard` indirection collapses, the producer
cursor disappears, and the `try_send` hot path becomes a direct ring
write (no shard scan, no modulo). After cache-line padding on
`head` / `tail`: **per-item ~32-44 ns p50** cross-thread (M=4-16),
**13-21% faster** than `Mpmc::new(M, 1)` in single-thread; with
`try_send_batch(K=64)` the producer hot path drops to **~15-18 ns/op
p50 — ~2-2.5× over per-item**.

**Two construction modes**: `Mpsc::new(M)` returns a `Vec` of M
non-cloneable producers (the original API), and
`Mpsc::new_cloneable(max)` returns a single `Sender::clone()`-style
handle that claims fresh ring slots on each `clone()` (cost: one
`AcqRel fetch_add` ≈ 8 ns per clone, never touched on the hot path).
Send/recv hot path is byte-for-byte identical between the two —
zero overhead for the cloneable variant.

**Async batch drain (tokio feature)**: `recv_batch_async_send` invokes
the user closure on every item drained per await. Beats
`tokio::sync::mpsc::Receiver::recv_many` by **1.3–2.1×** depending on
M; sync `recv_batch` reaches **1.65 G items/sec** at M=8 (drain_all
amortises one Acquire load + one Release store across the whole pass).
[→ mpsc.md](docs/mpsc.md)

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

### Single-use gate (`OneSignal`)

```rust
use arbitro_kit::gate::OneSignal;
use arbitro_kit::waiter::ParkWaiter;

let (tx, rx) = OneSignal::<ParkWaiter>::new();

let h = std::thread::spawn(move || {
    rx.bind();                  // register this thread for unpark
    rx.acquire().unwrap();      // blocks; returns Ok(()) on release
});

tx.release();                   // wakes the receiver
h.join().unwrap();
```

For an async receiver, swap `ParkWaiter` for `NotifyWaiter` (feature
`tokio`) and call `rx.acquire_async().await`. The struct is the same;
only the wake backend changes.

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

- **Ordered multi-producer delivery.** `SignalSet` coalesces repeated
  releases on the same bit. If you need to count events, use the bit
  to wake a consumer that drains a separate queue.
- **Bring-your-own scheduler integration.** Async support ships via
  `NotifyWaiter` (tokio) under the `tokio` feature. Other runtimes
  (smol, async-std, io_uring) are one `Waiter` impl away — see
  [docs/waiter.md](docs/waiter.md) — but the crate doesn't ship them.

---

## Roadmap

Shipped today:

- [x] **`Waiter` trait family** — every primitive is `<W: Waiter =
      ParkWaiter>`. Three concrete backends: `ParkWaiter` (sync,
      default), `NotifyWaiter` (tokio, feature-gated). Adding io_uring
      = one new file
- [x] `OneSignal<W>` — single-use payloadless gate; `acquire_timeout`
      on `ParkWaiter`, `acquire_async` on any `AsyncWaiter`
- [x] `OneShot<T, W>` — single-use payload-carrying gate; **67×
      faster than `tokio::sync::oneshot`** at full ack RT
- [x] `SignalSet<W>` — up to 256 coalesced signals over a chunked
      `Box<[AtomicU64]>`; the `≤64`-bit case still uses a single
      `AtomicU64` chunk with zero measurable overhead
- [x] `Pipe<T, H, W>` — SPSC single-slot with zero-cost observer hook
- [x] `Ring<T, CAP, W>` — SPSC bounded ring with batch send + batch
      ack, panic-safe, payload-sweep-documented
- [x] `Channel<Req, Resp, W>` — SPSC zero-copy request/response,
      panic-safe, 64-byte aligned for sub-110 ns handshake
- [x] `Hub<In, Out, W>` — N:1 multiplexer with per-port reply + shutdown
- [x] `Mpmc<T, CAP, W>` — M:N sharded channel with per-(producer,shard)
      mini-rings, batched `try_send_batch` path, panic-safe Drop,
      built-in shutdown. Zero `LOCK`-prefixed RMW on the producer hot
      path (per-shard `AtomicBool` + CAS-coalesced `unpark`); consumer
      uses spin-then-park to absorb sub-µs gaps. Supports `M ≤ 255`
      producers
- [x] `Mpsc<T, CAP, W>` — M:1 fan-in specialisation of `Mpmc` (N=1
      collapsed). No shard scan, no producer cursor; ~10% faster
      `try_send` than `Mpmc::new(M, 1)` in microbenches. Same drop /
      shutdown / backpressure guarantees as `Mpmc`. `new_cloneable`
      for `Sender::clone()`-style handle (per-clone ring claim,
      ~8 ns cold path, zero hot-path overhead).
      `recv_batch_async_send` for tokio: **2.1× over
      `tokio::mpsc::recv_many`** at M=8
- [x] `Stream<T, W>` — SPSC unbounded sequenced log with `Receipt`-based
      delivery verification; `BufferedSender` accumulator; opt-in
      `strict_wake` mode for bidirectional patterns
- [x] `Duplex<A, B, W>` — bidirectional unbounded SPSC over two
      `Stream`s with type-safe direction; built on `strict_wake` so
      1M-scale lockstep RPC is deadlock-free
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
- [ ] **`UringWaiter`** — io_uring CQE-based `Waiter` impl. Single new
      file under `src/waiter/`; every primitive in the crate inherits
      io_uring support automatically.
- [ ] **`no_std` core** — feature-gated extraction of `OneSignal` and
      `SignalSet` for embedded / freestanding targets (park via a
      user-provided `Waiter` impl).
- [ ] **Loom model checks** — permutation testing of the park protocol
      under weak memory models beyond what `miri` already covers.
- [ ] **ARM64 numbers** — current cost tables are x86_64; validate on
      aarch64 (where `SeqCst` on the park path is load-bearing).

---

## Benchmarks

Every number in the docs is reproducible:

```bash
cargo bench --bench gate_overhead        # gate primitives (ParkWaiter shim/SignalSet/OneSignal) vs crossbeam Parker / Mutex+Condvar
cargo bench --bench oneshot_h2h          # OneSignal / OneShot / Pipe / tokio::oneshot — fast path, spin, park, full ack RT
cargo bench --bench channel_overhead     # Channel vs crossbeam vs std::mpsc round-trip
cargo bench --bench pipe_overhead        # Pipe ST/XT + hook zero-cost claim
cargo bench --bench ring_overhead        # Ring FLOW / ROUND-TRIP / payload sweep
cargo bench --bench hub_overhead         # Hub throughput + RTT
cargo bench --bench mpmc_overhead        # Mpmc MP/NC sweep + batched + crossbeam
cargo bench --bench mpsc_overhead        # Mpsc MP/1C sweep + batched + crossbeam
cargo bench --bench mpsc_clone_overhead  # Mpsc new vs new_cloneable, ping-pong RTT, recv-1 vs recv-batch
cargo bench --bench mpsc_clone_async_overhead --features tokio   # Mpsc async + tokio::mpsc baseline + recv_batch_async
cargo bench --bench stream_overhead      # Stream send / send_iter / ack-RTT / lockstep
cargo bench --bench duplex_overhead      # Duplex zero-overhead check + RPC patterns + fire-and-forget
cargo bench --bench lifeline_overhead    # Lifeline cancel / recv_or_cancel overhead
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
