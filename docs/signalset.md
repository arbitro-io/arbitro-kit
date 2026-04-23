# `SignalSet` — up to 64 coalesced signals in one `AtomicU64`

[← back to README](../README.md)

`SignalSet` packs up to 64 named signals into a single `AtomicU64` plus
the same park/unpark machinery as `Signal`. Producers set named bits
lock-free; the consumer waits for **any**, **all**, or a **subset** of
signals to fire, then drains the whole bitfield in one load.

Bit 63 is reserved for the shutdown/control bit used by composites like
`Hub`; user-visible ports have 63 bits maximum.

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
couple of ns of raw `Signal`.
