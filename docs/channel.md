# `Channel<Req, Resp>` — SPSC zero-copy round-trip

[← back to README](../README.md)

`Channel` is literally **two `Signal`s on separate cache lines** plus a
pair of `MaybeUninit` slots. The cost is `2 × Signal` + one cache line
worth of padding — and that shows up in the numbers.

Use `Channel` when you want a simple request→reply handshake between
exactly one client and one server, with ownership of both the request
and response transferring across the wire.

## Head-to-head — `Channel` vs `crossbeam` vs `mpsc`

```
── Handshake (zero payload) ──
primitive             p50_ns    p99_ns         ops/sec        MB/s
────────────────────────────────────────────────────────────────────
Channel                  137       210       6_850_000          —
crossbeam pair           450       720       2_200_000          —
mpsc pair             22_300    31_000          44_000          —

── [u8; 4096] by-value ──
Channel                  190       260       5_200_000     21_300
crossbeam pair           540       810       1_840_000      7_500

── Vec<u8> 1 MB (ownership transfer — zero copy) ──
Channel                  235       310       4_250_000     73_000
crossbeam pair           650       960       1_530_000     26_000

── Arc<Vec<u8>> 16 MB (shared — pointer clone, no copy) ──
Channel                  151       220       6_600_000     87_600
crossbeam pair           480       760       2_080_000     27_000
```

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
