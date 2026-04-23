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

`Signal` is the atom. Six composites ship in the crate, each a thin
wrapper — most of the cost model carries over unchanged:

| Type         | Shape                                     | What it adds |
| :----------- | :---------------------------------------- | :----------- |
| `Signal`     | single-bit M:1 signal                     | the primitive itself |
| `SignalSet`  | up to 64 bits in one `AtomicU64`          | wait for any / all / subset of named signals |
| `Pipe<T, H>` | SPSC single-slot (1 × `Signal`)           | minimal payload transport with zero-cost observer hooks |
| `Ring<T, CAP>` | SPSC N-slot pipelined queue (2 × `Signal`) | burst absorption, pipelined throughput, batch send + batch ack |
| `Channel<Req, Resp>` | SPSC request/response (2 × `Signal`) | zero-copy round-trip with ownership transfer |
| `Hub<In, Out>` | N:1 multiplexer (`SignalSet` + N × `Pipe`) | fanout from N producers to 1 drain, with per-port reply |

### `Pipe<T, H>` — zero-cost observer hooks

`Pipe` is the minimal atom between `Signal` (no payload) and `Channel`
(bidirectional): one slot, one `Signal`, one direction. Higher-level
primitives in the crate build on it.

What makes it interesting: the generic `H: PipeHook<T>` parameter lets
you attach an observer (metrics, tracing, event propagation) with
**literally zero cost when unused**. The default `NoHook` is a ZST
whose methods are empty `#[inline]` no-ops; the optimizer elides the
calls completely.

```
── Pipe hot path (single-thread, 500 × 1000 ops, WSL /tmp/arbitro) ──
variant                    mean_ns/op   p50_ns/op   p99_ns/op    ops/sec
────────────────────────────────────────────────────────────────────────
raw_signal_slot (baseline)       0.44        0.42        0.43   2.28 B
pipe_nohook                      0.64        0.62        0.63   1.57 B
pipe_counting_hook               9.59        9.41       15.35   104 M
pipe_boxed_dyn_hook (control)    2.22        2.16        2.27   450 M
```

`pipe_nohook` matches the raw `Signal + UnsafeCell<MaybeUninit<T>>`
baseline within sub-cycle noise. The `Box<dyn Fn>` control — what we'd
pay if hooks lived on `Signal` itself — is ~4× the primitive cost in
isolation, which is why hooks are opt-in at `Pipe`, not embedded in
`Signal`.

### `Ring<T, CAP>` — SPSC bounded ring with batch ack

`Ring` is the multi-slot sibling of `Pipe`: same SPSC contract (one
producer, one consumer), but with `CAP` slots pre-allocated inline so
producer and consumer can **overlap in time** instead of alternating.

Two `Signal`s coordinate the two wait states:

- `not_empty` — consumer parks here when ring is empty.
- `not_full`  — producer parks here when ring is full.

Both sides follow the canonical **lock-check-acquire** park protocol:
only the waiter closes its signal, never the hot path — so `try_send`
and `try_recv` are lock-free in the common case.

**When to reach for `Ring` instead of `Pipe`:**

- **Burst absorption.** Producer fires N events in < 1 µs; consumer
  drains at a steady rate. `Pipe` blocks the producer between every
  event; `Ring` lets it run through the burst unhindered.
- **Pipelined throughput.** Steady-state per-item cost drops ~1.5–2×
  over `Pipe` because both sides work in parallel.
- **Graduated backpressure.** `try_send` returns `Err(value)` without
  blocking — caller can drop / coalesce / downsample per policy.

**Batch API — the main win over per-item.** Both directions expose a
bulk variant that amortizes the cursor publish and signal wakeup over
an entire batch — exactly what makes ring-buffer brokers (LMAX
Disruptor, Aeron) fast:

```rust
// Ingress: move up to `min(src.len(), free)` items in one shot.
//   → one head.store(Release) + one not_empty.release() per batch.
ring.try_send_from(&mut src_vec);

// Egress:  move up to `max` items in one shot.
//   → one tail.store(Release) + one not_full.release() per batch.
ring.drain_into(&mut out_vec, max);
```

`CAP` must be a power of two (mask-indexed, one AND vs a division).

```
── Ring hot path (x86_64 release, 1000-msg cross-thread) ──
scenario                         CAP      ns/op      ops/sec
──────────────────────────────────────────────────────────────
single-thread (no park)           16       ~1.0       ~1 G
cross-thread steady state         16       75.1       13.3 M
cross-thread steady state        256       34.1       29.3 M
cross-thread steady state       1024       27.2       36.8 M
cross-thread batched (B=64)      128        5.6      178.1 M    ← batch-ack amortized
round-trip closed loop (2 rings)  32      280.6        3.6 M    ← ns/cycle
```

Per-item 1-a-1 cost (27–52 ns) sits on the L1↔L1 cross-core coherence
floor. Batched throughput (5.6 ns/item) isn't breaking physics — it's
the same coherence cost spread over 64 items per handshake.

Reproduce with:

```bash
cargo bench --bench ring_overhead
```

### `Hub<In, Out>` — N:1 multiplexer with per-port reply

`Hub` wires N producer ports to a single consumer ("drain") using
`SignalSet` as the multiplexor. Each port has its own inbound slot
(the `SignalSet` bit IS its signal — saves one atomic per send) and
its own outbound `Pipe<Out>` for the drain's reply. Round-robin
fairness across ports prevents starvation.

Max 63 user ports (bit 63 is reserved for `HubShutdown`, which wakes
the drain out of a blocked `recv_batch` for clean teardown).

```
── Hub hot path (WSL /tmp/arbitro, 500 × 1000 ops) ──
variant                          mean_ns/op   p50    p99      ops/sec
─────────────────────────────────────────────────────────────────────
signalset_release+lock (raw)           7.71   7.33   17.54    129 M
hub_send + local drain                12.54  12.21   19.56     80 M

── Full RTT (port → drain → reply, cross-thread) ──
hub_rtt_1port                             —   89.01  163.54   11.5 M
hub_rtt_4port (aggregate)                 —      —       —    10.4 M
```

At 4 producers the drain saturates near 10M ops/sec — that's the
ceiling of a single consumer. For higher throughput, shard across
multiple Hubs.

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

### Single-slot pipe with optional hook

```rust
use arbitro_kit::gate::{Pipe, PipeHook};
use std::sync::atomic::{AtomicU64, Ordering};

// Default: zero-cost, no observer.
let p: Pipe<u64> = Pipe::new();
p.send(42);
assert_eq!(p.recv(), 42);

// Opt-in observer for metrics / event propagation.
#[derive(Default)]
struct Counter(AtomicU64);
impl PipeHook<u64> for Counter {
    fn on_send(&self, _: &u64) { self.0.fetch_add(1, Ordering::Relaxed); }
}

let p: Pipe<u64, Counter> = Pipe::with_hook(Counter::default());
for i in 0..100 { p.send(i); let _ = p.recv(); }
assert_eq!(p.hook().0.load(Ordering::Relaxed), 100);
```

### N:1 hub with per-port reply

```rust
use arbitro_kit::gate::{Hub, Shutdown};

let (drain, ports) = Hub::<u64, u64>::new(4);
let shutdown = drain.shutdown_handle();

// Drain thread: handle any port that fires, reply to that port.
let d = std::thread::spawn(move || {
    drain.bind();
    loop {
        match drain.recv_batch(|port_idx, msg, reply| {
            reply.send(msg + port_idx as u64 * 1000);
        }) {
            Ok(()) => continue,
            Err(Shutdown) => break,
        }
    }
});

// Each port moves to its own producer thread.
for (i, port) in ports.into_iter().enumerate() {
    std::thread::spawn(move || {
        port.bind();
        for k in 0..100u64 {
            let reply = port.call(k);
            assert_eq!(reply, k + i as u64 * 1000);
        }
    });
}

// Supervisor signals shutdown; drain wakes and exits cleanly.
shutdown.signal();
d.join().unwrap();
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

## Roadmap

Shipped today:

- [x] `Signal` — M:1 single-bit signal with Dekker-safe park
- [x] `SignalSet` — up to 64 coalesced signals in one `AtomicU64`
- [x] `Pipe<T, H>` — SPSC single-slot with zero-cost observer hook
- [x] `Ring<T, CAP>` — SPSC bounded ring with batch send + batch ack
- [x] `Channel<Req, Resp>` — SPSC zero-copy request/response
- [x] `Hub<In, Out>` — N:1 multiplexer with per-port reply + shutdown

Next:

- [ ] **`Fan<T>` — 1:N broadcast** over N `Pipe`s with per-consumer
      backpressure. Same zero-cost hook contract as `Pipe`.
- [ ] **`Queue<T>` — MPSC unbounded** built on a `Ring` per producer +
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

Explicit non-goals stay as below.

---

## License

MIT.
