# `Mpsc<T, RING_CAP>` — M:1 multi-producer / single-consumer channel

[← back to README](../README.md)

`Mpsc` is the **single-consumer specialisation** of [`Mpmc`](mpmc.md).
With `N` hardcoded to 1, the `Shard` indirection collapses, the adaptive
producer cursor disappears, and the API returns a single consumer by
value instead of a `Vec`. The hot-path semantics are identical to
`Mpmc::new(M, 1)`: per-producer SPSC mini-rings, a shared bitmap of
"ring p has data", level-triggered park.

It's the primitive you reach for when you need a **fan-in queue** —
many producers feeding one drainer, with `0%` CPU when idle and zero
heap allocation on the hot path.

## When to use `Mpsc` vs `Mpmc::new(M, 1)`

Both express the same M:1 topology. Pick `Mpsc` when:

- You **know** there will be exactly one consumer for the channel's
  lifetime. The type signature reflects that intent.
- You want the producer hot path to skip the shard scan and cursor
  bookkeeping (~10% faster `try_send` in microbenches).
- You prefer the cleaner API: one consumer returned by value, no
  `cs.remove(0)` boilerplate.

Pick `Mpmc::new(M, 1)` when:

- The number of consumers may grow later (you'd switch from `Mpsc` to
  `Mpmc` anyway, so start there).
- You're already using `Mpmc` elsewhere and consistency matters more
  than the marginal speedup.

## Topology

```text
  producer 0 ──┐
  producer 1 ──┤
    ⋮          │  ──► full_set: SignalSet (M bits + 1 shutdown)
  producer M-1 ┘  ──► rings[0..M]: PRing  (each is SPSC, RING_CAP slots)
                        │
                        └─► consumer drains the whole bitmap per wake
```

Compared with [`Mpmc`](mpmc.md):

- **No `Shard` struct.** `full_set`, `rings`, `full_mask_chunks`,
  `shutdown_id`, `producer_parks` live directly on `MpscInner` —
  one less indirection per send and per drain.
- **No producer cursor.** `try_send` writes directly to
  `inner.rings[my_idx]`. No shard scan loop, no modulo, no
  `cursor.set` call.
- **Single consumer returned by value.** `Mpsc::<T>::new(M)` returns
  `(Vec<MpscProducer>, MpscConsumer, MpscShutdown)` — no `Vec<Consumer>`
  with a single element.

Everything else carries over: per-pair SPSC ring, level-triggered bits,
Dekker park-or-drain, per-producer backpressure park.

## Cost — `Mpsc` vs `Mpmc(M, 1)` head-to-head

Measured on x86_64 (i9-12900K, 24 logical cores), `RING_CAP = 256`,
`ROUNDS = 1000`, **50 runs**, p50 reported.

```
── A. Cross-thread (M producer threads → 1 consumer thread) ──
M       Mpsc p50 ns/msg     Mpmc(M,1) p50 ns/msg     Speedup
────────────────────────────────────────────────────────────
4         45.3                42.0                    Mpmc 8% faster
8         35.2                39.2                    Mpsc 10% faster
16        32.5                33.2                    tie

── B. Single-thread try_send/try_recv (no park) ──
M       Mpsc p50 ns/msg     Mpmc(M,1) p50 ns/msg     Speedup
────────────────────────────────────────────────────────────
4          9.2                10.3                    Mpsc 12% faster
8          9.7                10.2                    Mpsc  5% faster
16        11.4                12.6                    Mpsc 10% faster
```

The single-thread numbers isolate the producer hot-path cost (no
park/unpark overhead, no cross-CPU traffic). Mpsc's advantage there
is consistent ~7-12%, which matches the prediction: the saved
operations are `cursor.get`, `cursor.set`, the modulo `(start + k) % n`,
and one branch.

Reproduce with:

```bash
cargo bench --bench mpsc_vs_mpmc
```

## Drop safety

`Mpsc`'s `Drop` walks every ring and calls `assume_init_drop()` on
each slot in `[tail, head)`. RAII payloads (`File`, sockets, `Box<T>`)
are never leaked, even if the channel drops with unconsumed messages.

## Shutdown

`MpscShutdown::signal()` sets a flag + releases the shutdown bit
(index `M`) on the `SignalSet` + wakes every parked producer. The
consumer returns `Err(Shutdown)` from `recv` / `recv_batch` **after**
draining any pending messages.

## Limits

- `M ≤ 255` producers (same as `Mpmc`). Chunks are added as needed
  (`ceil((M+1)/64)`).
- `RING_CAP` must be a power of two ≥ 1. Default: 64.
- Backing storage: `M × RING_CAP × sizeof(T)` bytes — half of `Mpmc`'s
  for the same `M` (no `N` factor).

## Usage

### Per-item

```rust
use arbitro_kit::route::Mpsc;

let (mut producers, consumer, shutdown) = Mpsc::<u64>::new(4);

let consumer_h = std::thread::spawn(move || {
    consumer.bind();
    let mut count = 0;
    while let Ok(_) = consumer.recv_batch(|v| { count += 1; let _ = v; }) {}
    count
});

for (i, p) in producers.drain(..).enumerate() {
    std::thread::spawn(move || {
        p.bind();
        for k in 0..1000 { p.send((i * 1000 + k) as u64); }
    });
}

// ... later:
shutdown.signal();
let _ = consumer_h.join();
```

### Batched (high-throughput fan-in)

```rust
use arbitro_kit::route::Mpsc;

let (mut producers, _consumer, _shutdown) = Mpsc::<u64>::new(1);
let p = producers.remove(0);
p.bind();

let mut chunk: Vec<u64> = Vec::with_capacity(64);
for epoch in 0..10_000 {
    chunk.clear();
    chunk.extend(epoch * 64 .. epoch * 64 + 64);
    while !chunk.is_empty() {
        let n = p.try_send_batch(&mut chunk);
        if n == 0 {
            // Ring full → park once on consumer advance.
            let v = chunk.remove(0);
            p.send(v);
        }
    }
}
```
