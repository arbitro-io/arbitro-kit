# `Ring<T, CAP>` — SPSC bounded ring with batch ack

[← back to README](../README.md)

`Ring` is the multi-slot sibling of `Pipe`: same SPSC contract (one
producer, one consumer), but with `CAP` slots pre-allocated inline so
producer and consumer can **overlap in time** instead of alternating.

Two `Signal`s coordinate the two wait states:

- `not_empty` — consumer parks here when ring is empty.
- `not_full`  — producer parks here when ring is full.

Both sides follow the canonical **lock-check-acquire** park protocol:
only the waiter closes its signal, never the hot path — so `try_send`
and `try_recv` are lock-free in the common case.

`CAP` must be a power of two (mask-indexed, one AND vs a division).

## When to reach for `Ring` instead of `Pipe`

- **Burst absorption.** Producer fires N events in < 1 µs; consumer
  drains at a steady rate. `Pipe` blocks the producer between every
  event; `Ring` lets it run through the burst unhindered.
- **Pipelined throughput.** Steady-state per-item cost drops ~1.5–2×
  over `Pipe` because both sides work in parallel.
- **Graduated backpressure.** `try_send` returns `Err(value)` without
  blocking — caller can drop / coalesce / downsample per policy.

## Batch API — the main win over per-item

Both directions expose a bulk variant that amortizes the cursor publish
and signal wakeup over an entire batch — exactly what makes ring-buffer
brokers (LMAX Disruptor, Aeron) fast:

```rust
// Ingress: move up to `min(src.len(), free)` items in one shot.
//   → one head.store(Release) + one not_empty.release() per batch.
ring.try_send_from(&mut src_vec);

// Egress: move up to `max` items in one shot.
//   → one tail.store(Release) + one not_full.release() per batch.
ring.drain_into(&mut out_vec, max);
```

Both batch APIs are **panic-safe**. `drain_into` pre-reserves the output
`Vec` so `push` cannot fail mid-drain (would otherwise cause UB via
double-drop). `try_send_from` uses a drop-guard that advances `head` by
the number of slots actually written on unwind, preventing leaks.

## Cost — FLOW (one-way, producer → consumer)

Reproduce with `cargo bench --bench ring_overhead`.

```
── A1. single-thread, per-item (try_send / try_recv) ──
variant                     p50_ns/op   min_ns/op     ops/sec
──────────────────────────────────────────────────────────────
Ring<u64, 16>                    1.03       1.02       970 M
Ring<u64, 256>                   1.02       1.02       976 M
Ring<u64, 1024>                  1.03       1.02       971 M

── A2. single-thread, batch (1000 items) ──
variant                            ns/item
─────────────────────────────────────────────
send loop:  try_send × N            0.91
send batch: try_send_from           0.61    ← 1.50× speedup
recv loop:  try_recv × N            0.52
recv batch: drain_into              0.43    ← 1.22× speedup

── A3. cross-thread, per-item (1000 msgs) ──
variant                     ns/op (min)      ops/sec
─────────────────────────────────────────────────────
Ring<u64, 16>                    83.2        12 M
Ring<u64, 256>                   95.3        10 M
Ring<u64, 1024>                  37.5        27 M

── A4. cross-thread, batch (amortized, 1000 msgs) ──
variant                   ns/item (min)      ops/sec
─────────────────────────────────────────────────────
CAP=128, B=16                    17.1        59 M
CAP=128, B=64                    11.8        85 M
CAP=256, B=128                    8.2       122 M    ← best

── A5. cross-thread, burst (producer-side, 1000 msgs) ──
variant                   producer ns/op
────────────────────────────────────────
Ring<u64, 16>                    64.6
Ring<u64, 1024>                  18.5
Ring<u64, 2048>                  15.9    (CAP > MSGS)
```

Per-item 1-a-1 cost (30–80 ns) sits on the L1↔L1 cross-core coherence
floor. Batched throughput (~8 ns/item) isn't breaking physics — it's
the same coherence cost spread over 128 items per handshake.

## Cost — ROUND-TRIP (closed loop, 2 rings)

```
── B1. single-thread round-trip, per-item (1000 cycles) ──
variant                  ns/cycle (min)    cycles/sec
──────────────────────────────────────────────────────
Ring<u64, 32>                      2.1        484 M
Ring<u64, 256>                     2.1        487 M

── B2. single-thread round-trip, batch (1000 items) ──
variant                   ns/item (min)      ops/sec
─────────────────────────────────────────────────────
CAP=256, B=64                     1.47        680 M
CAP=512, B=128                    1.29        777 M

── B3. cross-thread round-trip, per-item (1000 cycles) ──
variant                  ns/cycle (min)    cycles/sec
──────────────────────────────────────────────────────
Ring<u64, 32>                    267.1        3.7 M
Ring<u64, 256>                   244.7        4.1 M

── B4. cross-thread round-trip, batch (1000 items) ──
variant                   ns/item (min)      ops/sec
─────────────────────────────────────────────────────
CAP=128, B=32                    28.3         35 M
CAP=256, B=128                   14.5         69 M
```

## Cost — PAYLOAD SIZE SWEEP

How `Ring<T, CAP>` behaves as payload `T` grows. Three variants:

- **Inline** — `Ring<[u8; N], CAP>`: payload stored inside each slot.
  Two `memcpy(N)` per message (producer → slot, slot → consumer).
- **Fresh Box** — `Ring<Box<[u8; N]>, CAP>`: `Box::new` per send.
  Pointer-only slot, but pays malloc + free + memset per message.
- **Pool** — `Ring<Box<[u8; N]>, CAP>` with pre-allocated buffers
  recycled externally. Pure pointer-move, no heap traffic in hot path.

Measured cross-thread per-item, 10 runs with 500 warmup iters each,
`taskset -c 0,1` for CPU pinning. min / p50 ns/op:

```
payload   inline min/p50   fresh-box min/p50   pool min/p50      winner
────────────────────────────────────────────────────────────────────────
64 B        63.4 /   71.8   137.1 /  162.2    10.6 /   32.6      pool (≈ inline)
256 B       71.1 /   80.5   220.4 /  271.0    21.4 /   47.7      pool
512 B       96.4 /  105.1   185.0 /  269.3    16.4 /   47.5      pool
1 KB        96.9 /  113.8   255.2 /  303.6    14.5 /   33.9      pool
4 KB       142.0 /  178.1   452.5 /  516.9    31.7 /   41.1      pool
16 KB      658.0 /  705.7   581.6 /  665.9    41.4 /   46.3      pool  ← inline loses
32 KB     1488.9 / 1520.7   516.4 /  584.8    44.3 /   50.2      pool
64 KB     3892.7 / 4183.9   919.3 /  948.6    58.4 /   77.2      pool
```

### What the numbers mean

1. **Pool is flat at ~10–60 ns regardless of payload size.** This is
   the absolute floor of `Ring` as a transport: cross-thread cache
   coherence + pointer-move + cursor publish. Nothing else touches the
   payload bytes.
2. **Inline scales linearly with N.** 64 B → 64 KB is 63 → 3893 ns
   (62× growth). 2× `memcpy(N)` dominates above ~1 KB.
3. **Fresh Box** pays malloc+free+memset per message. For payloads
   ≤ 16 KB it's slower than inline because the allocator overhead
   exceeds the memcpy cost. Above 16 KB the allocator returns zeroed
   pages from `mmap` directly, so the crossover happens there.
4. **Crossover inline-vs-fresh-box is at 16 KB.**

### Design rule

```
Payload size     │ Best strategy
─────────────────┼─────────────────────────────────────────────────
≤ 128 B          │ inline — pool adds no measurable win
128 B – 16 KB    │ pool > inline ≫ fresh-box (2–7× vs fresh-box)
> 16 KB          │ pool ≫ fresh-box ≫ inline (10–54× vs inline)
```

**Pool always wins above 256 B**, often by 2–15×. If your payload is
bigger than a handful of bytes, recycle buffers. A `BufferPool<T>`
utility shipping with the crate is on the roadmap.

## Safety

- **Panic-safe batch APIs** — see Batch API section above.
- **Drop drains in-flight items** — `Ring::drop` iterates
  `[tail, head)` and drops initialized slots, so RAII payloads (`Box`,
  `Vec`, `File`) are never leaked on teardown.
- **SPSC contract** — documented, not runtime-enforced (standard for
  this class of primitive). Double-producing or double-consuming is
  a logic bug, not a UB gate — but it will corrupt state.

## Usage

```rust
use arbitro_kit::gate::Ring;
use std::sync::Arc;

let r: Arc<Ring<u64, 256>> = Arc::new(Ring::new());
let r2 = r.clone();

let consumer = std::thread::spawn(move || {
    r2.set_consumer(std::thread::current());
    for _ in 0..100 {
        let v = r2.recv();
        println!("got {}", v);
    }
});

r.set_producer(std::thread::current());
for i in 0..100u64 { r.send(i); }
consumer.join().unwrap();
```
