# `Channel<Req, Resp>` — SPSC zero-copy round-trip

[← back to README](../README.md)

`Channel` is literally **two `Signal`s on separate cache lines** plus a
pair of `MaybeUninit` slots. The cost is `2 × Signal` + one cache line
worth of padding — and that shows up in the numbers.

Use `Channel` when you want a simple request→reply handshake between
exactly one client and one server, with ownership of both the request
and response transferring across the wire.

## Head-to-head — `Channel` vs `crossbeam` vs `mpsc`

Measured on WSL x86_64, 1000 ops × 100 warmup, median of 3 runs.

```
── Handshake (zero payload) ──
primitive             p50_ns    p99_ns         ops/sec        MB/s
────────────────────────────────────────────────────────────────────
Channel                  102       139       8_092_448          —
crossbeam pair           327       396       2_518_745          —
mpsc pair             20_903    75_941          43_199          —

── u64 by-value (8 B) ──
Channel                  123       184       6_220_723         49.8
crossbeam pair           322       371       2_049_894         16.4

── [u8; 256] by-value ──
Channel                  174       243       5_221_850      1_336
crossbeam pair           375       414       2_671_767        684

── [u8; 4096] by-value ──
Channel                1_034     2_440         891_051      3_650
crossbeam pair         1_625     1_872         627_764      2_571

── Vec<u8> ownership transfer (64 KB — zero copy) ──
Channel                  125       254         957_685     62_763

── Vec<u8> ownership transfer (1 MB — zero copy) ──
Channel                  239    91_252          63_953     67_060
crossbeam pair        12_629    37_509          36_199     37_958

── Vec<u8> ownership transfer (16 MB — zero copy) ──
Channel               16_149    33_984           2_636     44_241
crossbeam pair        36_445    57_243           2_389     40_093

── Arc<Vec<u8>> shared (1 MB — pointer clone, no copy) ──
Channel                  136       210       6_191_183  6_491_927
crossbeam pair           341       374       2_761_058  2_895_179

── Arc<Vec<u8>> shared (16 MB — pointer clone, no copy) ──
Channel                  142       185       6_110_601 102_518_888
crossbeam pair           305       426       2_894_356  48_559_236
```

**Handshake floor: 102 ns p50** — within ~15 ns of the physical cross-core
L1↔L1 coherence floor (~80–100 ns RTT on x86_64). Beats `crossbeam` 3.2×
and `mpsc` 205× at zero payload.

Throughput at 1 MB and 16 MB exceeds DRAM bandwidth because **nothing
physically moves**: ownership of the `Vec`/`Arc` transfers across the
signal, which is an 8-byte pointer and a Release/Acquire pair. The MB/s
column is the *effective* throughput — "if this were a copy, it would
equal this."

Reproduce with:

```bash
cargo bench --bench gate_channel_focus
```

## Zero-copy semantics

`Channel::call(v)` moves `v` into the server's slot. No `memcpy` of the
payload, no heap allocation, no refcount bump — just a pointer write +
one atomic Release. Works transparently with any `Send` type:

- `Box<T>` — pointer move, heap buffer stays put.
- `Vec<T>` — moves `(ptr, len, cap)`, heap buffer stays put.
- `Arc<T>` — clones the refcount (one atomic), shares the buffer.
- `File`, sockets, owned FDs — ownership of the handle transfers.

## Drop safety

If a `call` is in flight when the channel is dropped (client sent a
request but server panicked), both the request and response slots are
correctly dropped on teardown. RAII resources are never leaked.

## Panic safety

If the closure passed to `serve_one` (or `serve_loop`) panics, the
channel is **poisoned**:

1. An internal `PoisonGuard` sets an `AtomicBool` flag with Release
   ordering and releases `resp_gate`, waking the blocked client.
2. The client's `call` / `try_call` observe the flag with Acquire
   ordering after their own `acquire()` and panic instead of reading
   the uninitialized `resp_slot` or blocking forever.
3. The `Drop` impl honors the flag and skips `resp_slot.assume_init_drop()`
   for the poisoned case (the slot was never written).

Without this, a buggy handler could leave the client parked forever even
after the server thread had died. The poison check adds one Acquire load
of a cold `AtomicBool` per `call` — branch well-predicted, zero measurable
impact on the 102 ns p50 handshake.

## Cache-line layout

`Channel<Req, Resp>` is `#[repr(C, align(64))]` and its first field
`req_gate` (a `Signal`) is itself `align(64)`. This guarantees:

- The whole struct allocates on a 64-byte boundary (even inside an `Arc`,
  whose 16 B refcount header would otherwise shift it).
- `req_gate` occupies its own cache line (offset 0..64).
- `resp_gate` occupies a separate cache line (auto-padded to next 64 B).

A `layout_invariants` test pins these offsets at runtime across two
monomorphisations. Before the struct-level `align(64)` was added, the
handshake measured at 126 ns p50 — the alignment fix cut 24 ns (−19%)
from every operation.

## Usage

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
