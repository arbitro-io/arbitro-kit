# `Mpmc<T, RING_CAP>` — M:N sharded multi-producer / multi-consumer channel

[← back to README](../README.md)

`Mpmc` is the M:N extension of `Hub`'s "bit-is-signal" pattern. It wires
`M` producers to `N` consumers through `N` independent shards. Each
`(producer, shard)` pair owns a dedicated **SPSC mini-ring of
`RING_CAP` slots**, so a bursting producer can enqueue up to `RING_CAP`
items before stalling, and the consumer drains the whole bitmap in a
single park/unpark cycle.

It's the primitive you reach for when you need a **high-throughput
broker** — ingesting from many writers and fanning out to a worker
pool, with `0%` CPU when idle and zero heap allocation on the hot path.

## Topology

```text
  producer 0 ──┐                  shard 0 ──► consumer 0
  producer 1 ──┤  adaptive ──►    shard 1 ──► consumer 1
    ⋮          │  routing         ⋮           ⋮
  producer M-1 ┘                  shard N-1 ─► consumer N-1

  shard s
  ├── full_set: SignalSet          (M bits "ring p has data" + 1 shutdown bit)
  ├── rings[0..M]: PRing           (each is SPSC, RING_CAP slots)
  └── drained by consumer s
```

The shard's `SignalSet` is allocated with `with_capacity(M + 1)` —
`ceil((M+1) / 64)` chunks of 64 bits each. Producer bits are at
indices `0..M`, the shutdown bit is at index `M` (can sit in any
chunk). For `M ≤ 63` everything fits in chunk 0 and the cost is
identical to the previous monolithic-`AtomicU64` design; for higher
`M` the consumer walks one Acquire load per chunk in the drain
scan — still O(chunks), not O(M).

- **Per-pair SPSC ring.** `shards[s].rings[p]` is owned by producer `p`
  writing head, consumer `s` reading tail.
- **Level-triggered bits.** After every push, the producer sets its
  bit unconditionally (`fetch_or`). After every drain, the consumer
  releases the producer's backpressure gate unconditionally. Bits mean
  "this ring currently has data" — they are only cleared in the
  consumer's park path, with a Dekker recheck.
- **Adaptive routing.** Producers don't pin to a shard. On every send,
  they scan shards from a round-robin cursor and pick the first ring
  that isn't full. Cursor advances on success so consecutive sends fan
  out.
- **Backpressure per producer.** If every shard's ring for this
  producer is full, the producer parks on its own `Signal`. Any
  consumer that advances `tail` on one of this producer's rings wakes
  it.

## Cost — `Mpmc` numbers

Measured on WSL x86_64, 500 rounds × 1000 ops, `RING_CAP = 64`.

```
── A. Single-thread 1P/1C (hot path, no park) ──
shape                                 p50_ns/op   p99_ns/op       ops/sec
────────────────────────────────────────────────────────────────────────
Mpmc 1P/1C single-thread                  10.84       23.21    88_299_331

── B. 1P/1C cross-thread ──
Mpmc 1P/1C cross-thread                   81.27      169.41    12_154_020

── C. MP/1C fan-in (producer wall-time per round, 1000 ops split across M) ──
Mpmc 2P/1C                                57.37      176.73    16_290_605
Mpmc 4P/1C                                38.40      154.54    23_519_381
Mpmc 8P/1C                                33.05      758.80    15_892_315

── D. 1P/NC fan-out ──
Mpmc 1P/2C                                85.34      103.83    11_667_064
Mpmc 1P/4C                                49.48       74.38    19_479_972
Mpmc 1P/8C                                45.40    7_244.32     2_103_546

── E. MP/NC symmetric (per-item send) ──
Mpmc 2P/2C                                56.84       99.46    17_000_544
Mpmc 4P/4C                                32.95      137.53    26_290_811
Mpmc 8P/8C                                21.57    2_328.70     5_953_160

── G. MP/NC producer-batched (try_send_batch, chunk=64) ──
Mpmc 2P/2C batched-64                      1.89        3.78   503_892_062
Mpmc 4P/4C batched-64                      0.95        1.67   721_022_120
Mpmc 8P/8C batched-64                      0.74        7.26 1_032_543_712
```

**At `8P/8C` with batched sends, `Mpmc` sustains ~1.03 G ops/sec** —
about 29× the per-item `send` path on the same primitive. The batch
win isn't algorithmic magic: it's amortizing one `fetch_or` (the
only cache-line-contended op) and one `head.store` over up to 64
messages. Per-item `send` pays one atomic RMW on a cross-core cache
line per message; batched pays one per chunk.

Reproduce with:

```bash
cargo bench --bench mpmc_overhead
cargo bench --bench fanin_h2h
```

## When to use per-item vs batched

| Pattern                                    | API                       |
| :----------------------------------------- | :------------------------ |
| RPC / UI events / sparse messages          | `send()` — ~80 ns latency |
| Log streams, metrics, ingest, broker fans  | `try_send_batch(&mut v)`  |
| Mixed (some sparse, some bursty)           | Start with `send()`, switch to batch when you measure `fetch_or` as hot |

The batch API trades a little caller complexity (manage a `Vec<T>`,
loop until drained, fall back to `send()` on a full stall) for ~30×
less CPU on bursty workloads.

## Capacity introspection

Both handles expose snapshot-only inspection methods for metrics,
saturation alarms, and heuristic backpressure. Each returns a
**point-in-time** view: a peer thread may push or drain between when
the snapshot is taken and when the caller reads the result. Use them
for observability, never as a correctness gate — the actual
"can I send?" question is still answered by `try_send` /
`try_send_batch` returning `Ok` or `Err`.

| Producer side                  | Returns                                       |
| :----------------------------- | :-------------------------------------------- |
| `capacity_per_shard()`         | `RING_CAP` (compile-time)                     |
| `total_capacity()`             | `N × RING_CAP`                                |
| `available_in_shard(s)`        | free slots in shard `s` for this producer     |
| `available()`                  | sum of `available_in_shard(s)` over `0..N`    |
| `pending_in_shard(s)`          | `head − tail` for this producer in shard `s`  |
| `has_idle_shard()`             | `bool` — already existed; cheaper fast path   |

| Consumer side                  | Returns                                       |
| :----------------------------- | :-------------------------------------------- |
| `capacity_per_producer()`      | `RING_CAP` (compile-time)                     |
| `total_capacity()`             | `M × RING_CAP`                                |
| `pending()`                    | sum of `head − tail` across all `M` rings     |
| `available()`                  | sum of free slots across all `M` rings        |
| `pending_from(p)`              | per-producer pending in this shard            |
| `has_pending()`                | O(chunks) fast path — any bit set anywhere    |

### Cost

Each non-`const` method is a small fixed number of atomic loads
(one `Acquire` + one `Relaxed` per ring inspected). They never
modify state and never compete with `try_send` / `recv` for cache
lines, so the hot path is byte-identical with or without these
calls in the program. At `M = 150` consumers, `pending()` reads
`2 × 150 = 300` atomic loads ≈ 50 ns from L1 — well below any
metric scrape interval.

`has_pending()` is the cheapest signal: one `Acquire` load per
chunk plus a chunk-mask AND. For `M ≤ 64` that's one load total.

### Example: gauge + saturation alarm

```rust
// Periodic metrics task (run on a timer, not in the hot path).
loop {
    metrics.gauge("mpmc.pending",   consumer.pending() as f64);
    metrics.gauge("mpmc.available", consumer.available() as f64);

    if consumer.pending() > consumer.total_capacity() * 9 / 10 {
        warn!("Mpmc shard {} above 90% fill", consumer.shard());
    }
    sleep(Duration::from_secs(1)).await;
}
```

## Level-triggered bits (why this doesn't deadlock)

Earlier drafts optimized the `fetch_or` to fire **only on the
empty→non-empty transition** — but that pattern doesn't compose with
`SignalSet`'s Dekker park. It made the bit mean "a transition
happened" rather than "data is present." A consumer's `lock_mask`
could clear the bit between a producer's `head.store` and the
then-skipped `release`, and the consumer would park with data still
in the ring. Symptom at shutdown: `Drop` silently destroying pending
items.

The fix (the current code) is to unconditionally `fetch_or` on every
push and unconditionally release the backpressure gate on every
drain. Bit set ⟺ ring has data. This honors the Signal contract —
release on every message — and closes the park Dekker cleanly:

```text
Consumer park path:
  1. lock_mask(Release)   — clear all producer bits on this shard
  2. fence(SeqCst)
  3. for each p: head.load(Acquire) on rings[p]
     if h != t → ring has data → re-release the bit, loop back to drain
  4. else → acquire_any(wake_mask) — park until any producer re-sets a bit
```

If a producer raced (wrote and fetch_or'd between step 1 and step 3),
step 3 observes `h != t` and the consumer re-releases the bit and
loops without parking. If the race lost (consumer's fence happened
before producer's head.store in SC order), the producer's
unconditional `fetch_or` wakes the parked consumer via `acquire_any`.

## Drop safety

`Mpmc`'s `Drop` walks every ring and calls `assume_init_drop()` on
each slot in `[tail, head)`. RAII payloads (`File`, sockets, `Box<T>`)
are never leaked, even if the channel drops with unconsumed messages.

## Shutdown

`MpmcShutdown::signal()` sets a flag + releases the shutdown bit
(index `M`) on every shard's `SignalSet` + releases every producer's
backpressure gate. Consumers return `Err(Shutdown)` from `recv` /
`recv_batch` **after** draining any pending messages. Producers
observing a full send path during shutdown complete normally if
space becomes available (rings don't refuse writes on shutdown —
drain priority is the consumer's).

## High-M throughput (TCP-loaded broker scenario)

End-to-end throughput sweep on a `mpsc_overhead` bench (M conns →
TCP → `Mpmc` → 1 sync decoder). The chunked SignalSet activates
transparently from `M = 64`:

| M     | `Mpmc` shared | chunks |
| ----: | ------------: | -----: |
| 16    |       8.1 ms  |    1   |
| 32    |      20.6 ms  |    1   |
| 60    |      38.8 ms  |    1   |
| 100   |      63.4 ms  |    2   |
| 150   |      99.5 ms  |    3   |

Throughput per producer stays roughly constant as `M` and the
chunk count grow (rounds reduced for the high-M points to avoid
TCP TIME_WAIT exhaustion on `localhost`).

## Limits

- `M ≤ 255` producers. The shard's `SignalSet` is sized to `M + 1`
  bits; chunks are added as needed (`ceil((M+1)/64)`). The cap comes
  from `SignalId` being a `u8` — high enough for any realistic
  fan-in and small enough to keep the producer struct cache-friendly.
- `N ≥ 1`, no upper bound (runtime-sized).
- `RING_CAP` must be a power of two ≥ 1. Default: 64.
- Backing storage: `M × N × RING_CAP × sizeof(T)` bytes. Defaults
  `M = N = 8`, `RING_CAP = 64`, `T = u64` → 32 KiB.

## Usage

### Per-item

```rust
use arbitro_kit::route::Mpmc;

let (mut producers, mut consumers, shutdown) = Mpmc::<u64>::new(4, 2);

let handles: Vec<_> = consumers.into_iter().map(|c| std::thread::spawn(move || {
    c.bind();
    let mut count = 0;
    while let Ok(_) = c.recv_batch(|v| { count += 1; /* handle v */ }) {}
    count
})).collect();

for (i, p) in producers.drain(..).enumerate() {
    std::thread::spawn(move || {
        p.bind();
        for k in 0..1000 { p.send((i * 1000 + k) as u64); }
    });
}

// ... later:
shutdown.signal();
for h in handles { let _ = h.join(); }
```

### Batched (high-throughput broker)

```rust
use arbitro_kit::route::Mpmc;

let (mut producers, _cs, _sd) = Mpmc::<u64>::new(1, 1);
let p = producers.remove(0);
p.bind();

let mut chunk: Vec<u64> = Vec::with_capacity(64);
for epoch in 0..10_000 {
    chunk.clear();
    chunk.extend(epoch * 64 .. epoch * 64 + 64);
    while !chunk.is_empty() {
        let n = p.try_send_batch(&mut chunk);
        if n == 0 {
            // Every shard full for this producer → park once on any advance.
            let v = chunk.remove(0);
            p.send(v);
        }
    }
}
```
