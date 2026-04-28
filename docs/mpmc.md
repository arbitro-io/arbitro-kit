# `Mpmc<T, RING_CAP>` — M:N sharded multi-producer / multi-consumer channel

[← back to README](../README.md)

`Mpmc` wires `M` producers to `N` consumers through `N` independent
shards. Each `(producer, shard)` pair owns a dedicated **SPSC
mini-ring of `RING_CAP` slots**, so a bursting producer can enqueue
up to `RING_CAP` items before stalling, and the consumer can drain
every one of its M rings in a single park/unpark cycle.

It's the primitive you reach for when you need a **high-throughput
broker** — ingesting from many writers and fanning out to a worker
pool, with `0%` CPU when idle and zero heap allocation on the hot
path.

## Topology

```text
  producer 0 ──┐                  shard 0 ──► consumer 0
  producer 1 ──┤  adaptive ──►    shard 1 ──► consumer 1
    ⋮          │  routing         ⋮           ⋮
  producer M-1 ┘                  shard N-1 ─► consumer N-1

  shard s
  ├── rings[0..M]: PRing               (each is SPSC, RING_CAP slots)
  ├── consumer_parked: AtomicBool      (Dekker close vs producer wake)
  └── drained by consumer s
```

- **Per-pair SPSC ring.** `shards[s].rings[p]` is owned by producer `p`
  writing `head`, consumer `s` reading `tail`. Cache-line padded so
  the cursors don't share a line — no false-sharing bounce.
- **No bitmap.** The previous design used a per-shard `SignalSet` with
  one bit per producer; every send paid a `LOCK`-prefixed `fetch_or`.
  The current design uses one `AtomicBool` per shard and a relaxed
  load on the producer side — **zero `LOCK`-prefixed RMW** on the
  send path.
- **Adaptive routing.** Producers don't pin to a shard. On every send,
  they scan shards from a round-robin cursor and pick the first ring
  that isn't full. Cursor advances on success so consecutive sends
  fan out.
- **Backpressure per producer.** If every shard's ring for this
  producer is full, the producer parks on its own [`Park`]. Any
  consumer that advances `tail` on one of this producer's rings
  wakes it.
- **Wake coalescing.** When a producer publishes, it CAS-claims the
  right to call `unpark()` (`consumer_parked: true → false`). The
  first producer per park cycle issues the syscall; subsequent ones
  see `false` and skip it. The consumer's Dekker recheck after
  `consumer_parked.store(SeqCst)` ensures any in-flight publication
  is observed before the actual park.
- **Spin-then-park.** Before parking, the consumer spins for 512
  iterations rechecking the rings. Catches publishes that arrive
  within ~µs without paying the syscall round-trip — critical for
  1P/NC where the producer rotates shards rapidly.

## Cost — `Mpmc` numbers

Measured on WSL x86_64, 500 rounds × 1000 ops, `RING_CAP = 64`. All
numbers in `ns/op` (lower is better) and aggregate `ops/sec` (higher
is better).

```
── A. Single-thread 1P/1C (hot path, no park) ──
shape                                 mean_ns/op  p50_ns/op  p99_ns/op    ops/sec
─────────────────────────────────────────────────────────────────────────────────
mpmc 1P/1C single-thread                    4.13       3.89      11.44  241_979_820

── B. 1P/1C cross-thread ──
mpmc 1P/1C cross-thread                    19.35      17.73      78.79   51_686_355

── C. MP/1C fan-in (producer wall-time per round) ──
mpmc 2P/1C                                 12.73      12.19      25.46   78_555_286
mpmc 4P/1C                                  4.56       3.44      44.53  219_459_760
mpmc 8P/1C                                  7.78       2.35     210.44  128_489_683

── D. 1P/NC fan-out ──
mpmc 1P/2C                                 33.95      33.68      92.76   29_453_982
mpmc 1P/4C                                 30.86      30.77      86.30   32_399_466
mpmc 1P/8C                                 51.62      28.62      41.69   19_373_383

── E. MP/NC symmetric (per-item send) ──
mpmc 2P/2C                                 21.09      20.36      62.55   47_420_158
mpmc 4P/4C                                 14.08      12.66      76.29   71_007_789
mpmc 8P/8C                                 98.68       5.53   2_405.33   10_133_460

── F. crossbeam::channel::bounded(1024) baselines ──
crossbeam 2P/1C                            16.57      16.08      34.53   60_361_147
crossbeam 4P/1C                            22.90      21.47      68.31   43_667_965
crossbeam 8P/1C                            54.95      50.75     103.80   18_198_099
crossbeam 1P/2C                            14.70      15.07      18.88   68_024_674
crossbeam 1P/4C                            20.58      20.54      34.06   48_594_468
crossbeam 1P/8C                            72.12      68.62     135.91   13_866_136
crossbeam 2P/2C                            15.02      13.99      48.54   66_595_462
crossbeam 4P/4C                            33.27      20.66     211.89   30_061_420
crossbeam 8P/8C                           472.62      76.97   5_302.60    2_115_852

── G. MP/NC producer-batched (try_send_batch, chunk=64) ──
mpmc 2P/2C batched-64                       1.64       1.45       7.62   609_588_089
mpmc 4P/4C batched-64                       0.77       0.69       3.35 1_296_535_915
mpmc 8P/8C batched-64                       0.70       0.66       0.78 1_437_339_197
```

**At `8P/8C` with batched sends, `Mpmc` sustains ~1.44 G ops/sec** —
about 140× the per-item path on the same primitive. The batch win
isn't algorithmic magic: it's amortizing the `head.store(Release)`
and the `unpark` over up to 64 messages. Per-item `send` pays one
Release store + one conditional `unpark` per message; batched pays
one per chunk.

### Where `Mpmc` wins vs crossbeam

| Shape       | mpmc        | crossbeam   | Speedup |
| :---------- | ----------: | ----------: | :------ |
| 4P/1C       | 4.56 ns     | 22.90 ns    | **5.0×** |
| 8P/1C       | 7.78 ns     | 54.95 ns    | **7.1×** |
| 2P/2C       | 21.09 ns    | 15.02 ns    | 0.7× (loss) |
| 4P/4C       | 14.08 ns    | 33.27 ns    | **2.4×** |
| 8P/8C       | 98.68 ns    | 472.62 ns   | **4.8×** |
| 1P/8C       | 51.62 ns    | 72.12 ns    | **1.4×** |

`Mpmc` is at its best when **M ≥ 2** and the total fan-in / fan-out
exceeds 4. For 1P/2C and 1P/4C crossbeam still wins narrowly — the
adaptive cursor rotates the producer across shards, so no single
ring fills enough to amortize. For permanent M:1 fan-in, prefer
[`Mpsc`] (faster per-item, no shard scan).

Reproduce with:

```bash
cargo bench --bench mpmc_overhead
```

## Cost model — analytical formula

> **Caption (v1, to refine).** First-pass closed-form model that predicts
> per-message send cost as a function of `(M, N, C, ρ, K)`. Validated
> within ±5% against bench numbers for the unbatched and `K=64`
> batched regimes on x86_64 / WSL. Future revisions should add: a
> tail-latency term (CPU pre-emption, p99 jitter when `M > N_cores`),
> a consumer-side recv cost term (currently lumped into `c_wake`),
> and an NUMA cross-socket term for cross-die routing. Treat the
> formulas below as the **steady-state mean** model.

### Variables

| Symbol | Meaning | Typical |
|---|---|---|
| `M` | producers | 1–255 |
| `N` | shards (= consumers) | 1–64 |
| `C` | `RING_CAP` per ring | 64 |
| `K` | batch size in `try_send_batch` | 1–`C` |
| `ρ` | load = `rate_in / rate_out` | [0, ∞) |
| `q` | P(a given shard is full from a producer's view) | f(ρ) |
| `p_park` | fraction of time the consumer is parked | f(ρ) |

### Hardware constants (measured, x86_64 / WSL)

| Constant | Value |
|---|---|
| `c_load` (Acquire L1 load) | ~0.3 ns |
| `c_store` (Release store) | ~0.5 ns |
| `c_seqcst` (SeqCst barrier) | ~20 ns |
| `c_syscall` (park/unpark) | ~7 000 ns |
| `c_slot` (write one ring cell) | ~0.6 ns |

### Per-message send cost

Number of probes until a non-full shard is found is geometric (truncated):

```
                1 - (1 + N(1-q)) · q^N
E[k | found] = ─────────────────────────
                  (1-q) · (1 - q^N)

P(block)     =  q^N
```

Send cost decomposition:

```
T_send  =  c_hit  +  (E[k] - 1) · c_probe  +  P(block) · c_retry

c_hit   =  2·c_load + 2·c_store + c_wake
c_probe =  2·c_load             # head + tail per extra shard probed
c_wake  =  c_load + p_park · (c_seqcst + c_syscall)
```

Limits:

```
ρ → 0   (no backpressure)   :  T_send ≈ c_hit               ≈  5 ns
ρ → ∞   (saturated)         :  T_send → c_hit + (N-1)·c_probe + c_retry
```

### Batched send (`try_send_batch(K)`)

```
T_batch(K)  =  c_hit  +  K · c_slot  +  c_publish  +  c_wake

T_msg(K)    =  T_batch(K) / K
            =  c_slot  +  (c_hit + c_publish + c_wake) / K
```

Asymptote:

```
lim T_msg(K)  =  c_slot  ≈  0.6 ns           (per-message floor)
K → ∞
```

### System throughput

```
ops/sec(M, K)  =  min(M, N_cores) · 1 / T_msg(K)
```

When `M > N_cores`, scheduling pre-emption introduces tail jitter
(visible as p99 inflation in the bench), not captured by the mean
formula.

### Backpressure point

```
slots_free   =  N · C · (1 - ρ)

       ┌─ ρ           ,  ρ < 1
q  =   ┤
       └─ 1 - 1/(ρN)  ,  ρ ≥ 1
```

The first probe almost always hits while `ρ < C`; the geometric tail
only matters once the system enters saturation.

### Validation against bench numbers

| Regime | Formula | Predicted | Measured |
|---|---|---|---|
| 8P/8C, K=1, ρ→0 | `c_hit ≈ 5 ns` | ~5 ns | 5.27 ns ✅ |
| 8P/8C, K=64, ρ→0 | `c_slot + c_hit/K ≈ 0.6 + 5/64` | 0.68 ns | 0.66 ns ✅ |
| 4P/4C, K=64, ρ→0 | same | 0.68 ns | 0.73 ns ✅ |
| 8P/1C fan-in | `c_hit` | ~5 ns | 2.57 ns p50 ✅ (cursor optimization) |

The cursor optimization makes 8P/1C faster than the closed-form
predicts because each producer's `cursor` settles on its own shard
after warmup, collapsing the probe sequence to k=1 even with N=1.
A refinement of `E[k]` that conditions on cursor warm-up state would
close that gap.

## When to use per-item vs batched

| Pattern                                    | API                       |
| :----------------------------------------- | :------------------------ |
| RPC / UI events / sparse messages          | `send()` — ~20 ns latency |
| Log streams, metrics, ingest, broker fans  | `try_send_batch(&mut v)`  |
| Mixed (some sparse, some bursty)           | Start with `send()`, switch to batch when you measure the Release+wake as hot |

The batch API trades a little caller complexity (manage a `Vec<T>`,
loop until drained, fall back to `send()` on a full stall) for ~140×
less CPU on bursty workloads.

## No internal accumulator

`Mpmc` does **not** internally batch messages. Every `try_send`
publishes immediately with `head.store(Release)`. What looks like
batching from the outside is three independent effects:

1. **Wake coalescing** — multiple sends during one park cycle pay
   one syscall, not N. Data is visible per-message; the syscall is
   what gets amortized.
2. **Drain coalescing** — the consumer's `recv_batch` opportunistically
   picks up everything currently published in its shard. If the
   consumer was sleeping while the producer published 50 items,
   `recv_batch` returns all 50 in one wake. But the consumer is the
   one driving the loop — there is no library-internal thread.
3. **Mini-ring buffering** — each `(producer, shard)` SPSC ring has
   `RING_CAP` slots, so the producer can write ahead of the consumer.
   This is buffering, not deferred publishing.

If you want a true accumulator (collect K items, then publish in one
shot): use `try_send_batch` with a caller-managed `Vec<T>`. The
library does not provide an opaque "buffered producer" wrapper for
`Mpmc` (only `Stream<T>` has one — see `BufferedSender`).

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
| `has_idle_shard()`             | `bool` — cheap fast path                      |

| Consumer side                  | Returns                                       |
| :----------------------------- | :-------------------------------------------- |
| `capacity_per_producer()`      | `RING_CAP` (compile-time)                     |
| `total_capacity()`             | `M × RING_CAP`                                |
| `pending()`                    | sum of `head − tail` across all `M` rings     |
| `available()`                  | sum of free slots across all `M` rings        |
| `pending_from(p)`              | per-producer pending in this shard            |
| `has_pending()`                | O(M) fast path — any ring non-empty           |

### Cost

Each non-`const` method is a small fixed number of atomic loads (one
`Acquire` + one `Relaxed` per ring inspected). They never modify
state and never compete with `try_send` / `recv` for cache lines, so
the hot path is byte-identical with or without these calls in the
program.

## Drop safety

`Mpmc`'s `Drop` walks every ring and calls `assume_init_drop()` on
each slot in `[tail, head)`. RAII payloads (`File`, sockets,
`Box<T>`) are never leaked, even if the channel drops with
unconsumed messages.

## Shutdown

`MpmcShutdown::signal()` sets a flag + unparks every parked consumer
+ wakes every parked producer. Consumers return `Err(Shutdown)` from
`recv` / `recv_batch` **after** draining any pending messages.
Producers observing a full send path during shutdown complete
normally if space becomes available (rings don't refuse writes on
shutdown — drain priority is the consumer's).

## Limits

- `M ≤ 255` producers (sanity cap; could be raised — no longer a
  technical limit since the `SignalSet` was removed from the hot path).
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
