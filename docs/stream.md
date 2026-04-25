# `Stream<T>` — unbounded sequenced log

[← back to README](../README.md)

`Stream<T>` is an **append-only SPSC log**. The producer `send`s items
and gets back a `Receipt` carrying the message's monotonic sequence
number; the consumer drains in order. Storage is a linked list of
fixed-size segments (`SEG_SIZE = 256`) allocated on demand.

The defining property: **the producer never blocks** while RAM is
available. There is no `CAP`. Backpressure is the caller's call.

## When to reach for `Stream<T>`

- **You don't know how big the burst will be**, and you don't want
  the producer parked while the consumer catches up.
- **You want delivery verification without a reply path.** The
  `Receipt` exposes `is_delivered` (one atomic load) and
  `wait_delivered` (block until cursor crosses) — no second stream,
  no RPC scaffolding.
- **You produce items one at a time but want batched throughput.**
  `BufferedSender` accumulates locally and flushes via `send_iter`;
  the per-`send` API stays single-item.
- **You want a foundation for higher-level patterns.** Two `Stream`s
  paired = bidirectional Nexo. One `Stream` + N reader-cursors =
  broadcast. The primitive is deliberately minimal so policies (RPC,
  fan-out, retention) compose on top.

## When NOT to use it

- **You need a hard memory bound at the type level.** Use `Ring<T, CAP>`
  — you trade flexibility for the contract that allocations stop.
- **You need real-time / no-allocation guarantees** (audio thread,
  embedded, kernel). `Stream` allocates a new segment every 256
  messages; `Ring` allocates once at startup.
- **You need MPSC, MPMC, or work-stealing.** `Stream` is SPSC in this
  MVP. Use `Mpmc` (M:N) or `Hub` (N:1 named) instead.

## Mental model

```
producer ─send──►        consumer ─recv──►
        │                        │
┌───────▼─────────┬──────────────▼────────┐
│  tail_seg       │  head_seg              │
│  (writing here) │  (reading here)        │
├──────┬──────┬───┴───┬──────┬──────┬─────┤
│ seg0 │ seg1 │  …    │ segM │ segN │ ... │
└──────┴──────┴───────┴──────┴──────┴─────┘
                                  ▲
                       freed by consumer
                       once cursor passes
```

Each segment holds 256 slots. The producer allocates a new segment
when it walks off the end of the current one; the consumer frees old
segments as it drains past them. The cross-thread fence is one
Release/Acquire pair on `tail_pos` (the producer cursor).

## API surface

```rust
let stream: Arc<Stream<u64>> = Arc::new(Stream::new());

// Producer side (single thread).
let r = stream.send(42);                     // Receipt(0)
let r = stream.send_iter(0..1000);           // Receipt(999), 1 cursor publish

// Consumer side (single thread, register first for blocking recv).
stream.set_consumer(thread::current());
let v = stream.recv();                        // blocks (Park, phased backoff)
let v = stream.try_recv();                    // None if empty
let n = stream.recv_bulk(&mut buf, 256);      // drain whatever is there

// Verification — any thread holding the receipt.
r.is_delivered(&stream);                      // 1 Acquire load
r.wait_delivered(&stream);                    // busy-spin until cursor crosses

// Cursors / introspection.
stream.tail();      // total produced
stream.cursor();    // total drained (what is_delivered checks)
stream.len();       // tail - cursor (snapshot)
```

## `BufferedSender` — single-send API with bulk performance

When items arrive one at a time (event handler, parser callback) and
you want the throughput of `send_iter`, wrap the stream:

```rust
use arbitro_kit::stream::BufferedSender;

let mut tx = BufferedSender::new(stream.clone(), 64);
loop {
    let item = next_item();
    tx.send(item);   // accumulates locally; flushes every 64 via send_iter
}
// tx is dropped → final flush of any residue.
```

Performance: ~10–15 % over the explicit bulk path. The wrapper costs
one Vec push and a length comparison per `send`.

## Performance

Numbers from `benches/stream_overhead.rs`, best-of-30, u64 payload, WSL
on a single CCX. **Re-run before quoting** — these are reference
points, not contractual.

### One-way SPSC

| Path | Stream | Ring CAP=1024 (ref) |
|---|---:|---:|
| Single-thread send + try_recv | 1.3 ns | 0.7 ns |
| Cross-thread send + recv (per-item) | **3.1 ns** | 5.1 ns |
| Cross-thread send_iter K=256 + recv | **2.9 ns** | — |
| BufferedSender K=256 (single-send API) | 3.5 ns | — |

`Stream` beats bounded `Ring` cross-thread because the producer never
checks "is the ring full?" — segments grow on demand.

### Round-trip patterns

| Pattern | ns/RT | RT/sec |
|---|---:|---:|
| Lockstep RPC (2 streams, full reply) | 135 | 7.4 M |
| Ack-RTT per-msg (Receipt + wait_delivered) | 116 | 8.6 M |
| Ack-RTT batched K=8 | 20.1 | 50 M |
| Ack-RTT batched K=32 | 8.1 | 124 M |
| Ack-RTT batched K=128 | 4.4 | 228 M |
| **Ack-RTT batched K=512** | **3.5** | **287 M** |

The headline: **3.5 ns per verified round-trip at K=512** with the
`Receipt` mechanism, while keeping `Park` (0 % idle CPU). For
comparison, `disruptor`'s `BusySpin` SPSC one-way bench reports ~8.4
ns *with two cores burning permanently*. Stream's ack-RTT at K=512
matches the single-direction-only Disruptor number while doing a full
verified round-trip and not pinning cores.

### Reproduce

```bash
cargo bench --bench stream_overhead       # full Stream sweep
cargo bench --bench rpc_patterns          # lockstep / busy-spin / batched / ack-RTT
cargo bench --bench ring_vs_crossbeam     # SPSC reference vs crossbeam_channel
```

## Concurrency contract

- Exactly **one producer** thread calls `send` / `send_iter`.
- Exactly **one consumer** thread calls `recv` / `try_recv` /
  `recv_bulk`. Register it via `set_consumer(thread::current())`
  before the first blocking `recv`.
- Any thread may hold a `Receipt` and call `is_delivered` /
  `wait_delivered` — these are read-only against the cursor.
- The stream is typically shared across threads via `Arc<Stream<T>>`.

## Memory model

- **Slot data** is `UnsafeCell<MaybeUninit<T>>` — plain memory, not
  atomic. Cross-thread visibility is established by the cursor
  Release/Acquire pair.
- **`tail_pos`** (producer cursor) and **`head_pos`** (consumer cursor)
  are `AtomicU64` on separate cache lines. The producer's Release on
  `tail_pos` after a slot write makes the slot visible; the consumer's
  Acquire load establishes the happens-before.
- **Segment links** (`next: AtomicPtr<Segment<T>>`) are published with
  Release by the producer and followed with Acquire by the consumer.
  Old segments are freed by the consumer once it drains past them.

## Patterns built on top

`Stream<T>` is intentionally minimal so higher-level patterns compose:

- **Bidirectional (Nexo-style)**: pair two streams, one per direction.
  Caller correlates replies if needed.
- **RPC**: producer sends, consumer processes, replies on a reverse
  stream. Caller handles correlation. See `benches/rpc_patterns.rs`
  for the lockstep vs batched comparison that established 3.5 ns/RT
  as the practical floor.
- **Fan-out / broadcast**: not yet built. Two architectures sketched
  in `docs/research/stream_brainstorm.md`: multi-SPSC tee for low N,
  shared-log + per-consumer cursors for high N.

## Safety

- **Drop-safe**: in-flight items are drained on `Drop` of the stream,
  so `T` with RAII resources (`Box`, `Vec`, `Arc`, `File`) is safe.
- **Send / Sync**: `Stream<T>: Send + Sync` whenever `T: Send`. Slot
  access is partitioned by the cursors, so the underlying
  `UnsafeCell` is safe.
- **No internal `unsafe` exposure**: every public API is safe Rust;
  the `unsafe` blocks are confined to slot reads/writes and segment
  linkage, gated by the cursor protocol.

## Limitations and roadmap

- SPSC only in this MVP. Multi-producer (MPSC) and multi-consumer
  (SPMC, MPMC) variants are deferred.
- `Receipt::wait_delivered` busy-spins on the cursor. A parked
  variant is on the roadmap for callers that prefer to give up the
  core while waiting.
- No fan-out / broadcast yet. Designs in
  `docs/research/stream_brainstorm.md`.
