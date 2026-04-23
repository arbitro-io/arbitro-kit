# `Pipe<T, H>` — SPSC single-slot with zero-cost observer hook

[← back to README](../README.md)

`Pipe` is the minimal atom between `Signal` (no payload) and `Channel`
(bidirectional): one slot, one `Signal`, one direction. Higher-level
primitives (`Channel`, `Hub`) build on it.

What makes it interesting: the generic `H: PipeHook<T>` parameter lets
you attach an observer (metrics, tracing, event propagation) with
**literally zero cost when unused**. The default `NoHook` is a ZST
whose methods are empty `#[inline]` no-ops; the optimizer elides the
calls completely.

## Single-thread cost

`pipe_nohook` matches the raw `Signal + UnsafeCell<MaybeUninit<T>>`
baseline within sub-cycle noise. The `Box<dyn Fn>` control — what we'd
pay if hooks lived on `Signal` itself — is ~4× the primitive cost,
which is why hooks are opt-in at `Pipe`, not embedded in `Signal`.

```
── A. single-thread (no park), 500 × 1000 ops ──
variant                    mean_ns/op   p50_ns/op   p99_ns/op    ops/sec
────────────────────────────────────────────────────────────────────────
raw_signal_slot (baseline)       0.44        0.42        0.43    2.28 B
pipe_nohook                      0.64        0.62        0.63    1.57 B
pipe_counting_hook               9.64        9.41       16.73     104 M
pipe_boxed_dyn_hook (control)    2.54        2.45        2.91     395 M
```

## Cross-thread cost — round-trip (2 Pipes)

Pipe has no built-in backpressure; a second `Pipe<()>` as ack channel
closes the loop so the producer respects the single-slot contract.
Every cycle = 2 handshakes across L1↔L1.

```
── B. cross-thread round-trip, 5000 cycles ──
variant                    ns/cycle    cycles/sec
──────────────────────────────────────────────────
pipe_xt_round_trip            110.0     9.1 M
pipe_xt_handshake (unit)       87.6    11.4 M
```

Pipe is **~2× faster than `Ring` per-cycle** when used round-trip
(Ring pays head + tail + 2 signals per handshake). Use `Pipe` / `Channel`
when you need minimum request→reply latency.

## Batch via payload: `Pipe<Vec<T>>`

One send/recv per batch — handshake amortized over B items. This is the
*right way* to batch over `Pipe` without turning it into `Ring`.

```
── C. Pipe<Vec<u64>> round-trip, 10_000 items ──
variant               ns/item    items/sec
─────────────────────────────────────────────
B=16                    34.9     28.7 M
B=64                    10.4     96.4 M
B=256                    2.96    337 M
```

At B=256, `Pipe<Vec<u64>>` is **2.8× faster than `Ring`'s batch API**
for raw throughput, because the `Vec` transfers by ownership move
(pointer + len + cap) instead of copying item-by-item into slots.

**Rule of thumb:** for pure throughput, `Pipe<Vec<T>>` beats `Ring`
batch. `Ring` wins when you need pipelining with per-item granularity.

## When to reach for Pipe vs Ring

| Case                                             | Use             |
| ------------------------------------------------ | --------------- |
| 1 req → 1 resp, minimum latency                  | `Pipe` / `Channel` |
| Bulk transfer, no per-item backpressure          | `Pipe<Vec<T>>`  |
| Producer burst, consumer steady                  | `Ring<T, CAP>`  |
| Pipelining with per-item cursor                  | `Ring<T, CAP>`  |

## Usage

```rust
use arbitro_kit::gate::{Pipe, PipeHook};
use std::sync::atomic::{AtomicU64, Ordering};

// Default: zero-cost, no observer.
let p: Pipe<u64> = Pipe::new();
p.send(42);
assert_eq!(p.recv(), 42);

// Opt-in observer for metrics / event propagation.
#[derive(Default)]
struct Counter(AtomicU64);
impl PipeHook<u64> for Counter {
    fn on_send(&self, _: &u64) { self.0.fetch_add(1, Ordering::Relaxed); }
}

let p: Pipe<u64, Counter> = Pipe::with_hook(Counter::default());
for i in 0..100 { p.send(i); let _ = p.recv(); }
assert_eq!(p.hook().0.load(Ordering::Relaxed), 100);
```

Reproduce all `Pipe` numbers with:

```bash
cargo bench --bench pipe_overhead
```
