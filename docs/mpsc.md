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
`ROUNDS = 1000`, **50 runs**, p50 reported. After the cache-line
padding fix on `PRing::head` / `PRing::tail`.

```
── A. Cross-thread per-item — M producers blast `try_send`, 1 consumer drains ──
M       Mpsc p50 ns/msg     Mpmc(M,1) p50 ns/msg     Speedup
────────────────────────────────────────────────────────────
4         43.7                46.3                    tie / Mpsc 6% faster
8         37.6                36.8                    tie
16        31.6                38.8                    Mpsc 19% faster

── B. Single-thread try_send/try_recv (no park) ──
M       Mpsc p50 ns/msg     Mpmc(M,1) p50 ns/msg     Speedup
────────────────────────────────────────────────────────────
4          9.5                10.9                    Mpsc 13% faster
8          9.5                12.1                    Mpsc 21% faster
16        12.3                13.6                    Mpsc 10% faster

── C. Cross-thread BATCHED via `try_send_batch(K=64)` ──
M       Mpsc p50 ns/msg     Mpmc(M,1) p50 ns/msg     Speedup vs A
────────────────────────────────────────────────────────────────
4         18.2                18.0                    A → C: 2.4× faster
8         15.2                16.1                    A → C: 2.5× faster
16        14.9                15.0                    A → C: 2.1× faster
```

The single-thread numbers isolate the producer hot-path cost (no
park/unpark overhead, no cross-CPU traffic). Mpsc's advantage there
is consistent 10-21%, which matches the prediction: the saved
operations are `cursor.get`, `cursor.set`, the modulo `(start + k) % n`,
and one branch.

The **batched** numbers (section C) show why `try_send_batch` matters
when the caller naturally has K items at once. With `BATCH_K = 64`,
one `fetch_or` + one `head.store(Release)` covers all 64 items
instead of 64 of each. The producer hot-path cost drops from ~33-46
ns/op to ~15-18 ns/op — roughly **2-2.5× faster**. Mpsc and Mpmc are
basically tied here because the cursor scan in Mpmc happens once per
batch, not per item.

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

When the caller naturally has multiple items at once, `try_send_batch`
amortizes the SignalSet `fetch_or` (the only contended atomic on the
producer hot path) and the `head.store(Release)` across the whole chunk.
Measured ~2-2.5× faster than per-item `try_send` cross-thread (see
section C in the cost table above). The **caller decides** when to
batch — the kit can't aggregate behind your back without a separate
buffering layer.

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
