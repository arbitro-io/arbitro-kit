# `SignalSet` — coalesced signals over a chunked `AtomicU64` bitmap

[← back to README](../README.md)

`SignalSet` packs named signals into a `Box<[AtomicU64]>` chunked bitmap
plus the same park/unpark machinery as `Signal`. Producers set named bits
lock-free; the consumer waits for **any**, **all**, or a **subset** of
signals to fire, then drains the bitfield with one Acquire load per chunk.

By default a fresh `SignalSet::new()` allocates a single 64-bit chunk —
identical layout and cost to the previous monolithic `AtomicU64`
implementation. Use `SignalSet::with_capacity(n_bits)` to host more bits;
the bitmap is split into `ceil(n_bits / 64)` chunks of 64 bits each.
Composites like `Mpmc` already do this transparently for `M > 63`.

`SignalId` is `u8` internally, so the per-set ceiling is **256 bits**
(four chunks). Hub still reserves bit 63 of chunk 0 for shutdown by
design; `Mpmc` places its shutdown bit at index `M` (which can sit in
any chunk) and lifts the limit to `M ≤ 255` producers.

## Why

The alternative — one `Signal` per channel — costs one atomic store and
one unpark per channel. `SignalSet` folds every channel into one atomic
OR. For an N:1 multiplexer, total atomic ops on the hot path drop from
`O(N)` to `O(1)`.

## Semantics — coalescing

Repeated `release(bit)` calls set the same bit. If a producer releases
the same signal 10 times before the consumer wakes, the consumer sees
**one** firing. Use this primitive for **edge-triggered events**
(e.g. "drain me"), not for counting.

If you need to count events, use the bit to wake a consumer that
drains a separate queue.

## Usage

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

## Cost

See the `Signal` numbers — `SignalSet` adds one atomic OR over that
path, plus one mask-AND + compare on the consumer side. Within a
couple of ns of raw `Signal`. Chunked storage adds **zero measurable
overhead** for the ≤64-bit case (the `Box` pointer stays L1-resident):

| Scenario           | pre-chunked | chunked  |  delta |
| ------------------ | ----------: | -------: | -----: |
| Uncontended 1-bit  |    7.10 ns  | 7.12 ns  | +0.3%  |
| Shared `M=1`       |    3.57 ns  | 3.51 ns  | -1.7%  |
| Shared `M=8`       |    8.57 ns  | 8.83 ns  | +2.2%  |

Reproduce with:

```bash
cargo bench --bench signalset_factory
cargo bench --bench signalset_vs_signals
```

## Chunked API surface

For sets with `> 64` bits, the legacy `u64`-mask methods (`state()`,
`acquire_any(mask)`, `lock_mask(mask)`, `any_open(mask)`,
`all_open(mask)`) operate on **chunk 0 only** — preserved for
backward compatibility with low-N callers. Multi-chunk callers use:

| Method                              | Purpose                                 |
| ----------------------------------- | --------------------------------------- |
| `with_capacity(n_bits)`             | Allocate `ceil(n_bits / 64)` chunks     |
| `n_chunks()` / `capacity_bits()`    | Inspect the chunk topology              |
| `state_chunk(c)`                    | Acquire-load chunk `c`                  |
| `lock_chunk_mask(c, mask)`          | Clear bits in chunk `c`                 |
| `any_chunk_open()`                  | True if any bit is set in any chunk     |
| `acquire_any_chunk()`               | Park until any bit lights up anywhere   |

Hot-path `release(id)` / `lock(id)` / `is_open(id)` are chunk-aware
already — `chunk = id/64`, `bit = 1 << (id%64)`. You don't need to
choose between APIs unless you want to drain or wait on more than 64
bits at once.
