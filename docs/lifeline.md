# `Lifeline` — fire-and-forget cancellation

[← back to README](../README.md)

`Lifeline` is a cancellation scope. Up to **64 waiters** register on
one scope, each gets a [`WaiterId`]. From any other thread you can
wake one specific waiter, a subset by mask, or every waiter at once,
without waiting for them to acknowledge.

It is the answer to the question: *"how do I cleanly shut down a pool
of workers parked on `Stream::recv` / `Ring::recv` / `Duplex::recv`?"*

## Wire model

```text
                   ┌────────────┐
                   │  Lifeline  │
                   │  scope     │
                   ├────────────┤
                   │ mask  u64  │  ← cancel_one / cancel_mask set bits
                   │ global bool│  ← cancel_all flips
                   │ waiters[64]│  ← Thread handles, set on register()
                   └────┬───────┘
                        │
       ┌────────────────┼────────────────┐
       ▼                ▼                ▼
   worker 0         worker 1         worker N (≤63)
   recv_or_cancel   recv_or_cancel   recv_or_cancel
   (life, id_0)     (life, id_1)     (life, id_N)

   Inside recv_or_cancel:
       Park.wait_until(|| ring_has_data || life.is_cancelled(id))

   cancel_*(...)  ──► fetch_or on mask + unpark()s ──► workers wake
                     and return Err(Cancelled)
```

## API surface

```rust
use arbitro_kit::gate::{Lifeline, WaiterId, Cancelled};

let life = Lifeline::new();

// On each worker thread, before parking:
let id: WaiterId = life.register(std::thread::current());

// Hot-path check (2 atomic loads, no lock):
if life.is_cancelled(id) { /* abort */ }

// From any thread:
life.cancel_one(id);          // wake one waiter
life.cancel_mask(0b0000_0101);// wake the bits set in the mask
life.cancel_all();             // wake every registered waiter
```

Transports that park expose a paired method:

```rust
match stream.recv_or_cancel(&life, id) {
    Ok(v)        => process(v),
    Err(Cancelled) => break,    // graceful exit
}
```

Methods provided:
- `Stream::recv_or_cancel(life, id) -> Result<T, Cancelled>`
- `Ring::recv_or_cancel(life, id)   -> Result<T, Cancelled>`
- `DuplexEnd::recv_or_cancel(life, id) -> Result<R, Cancelled>`

The plain `recv()` paths are **unchanged**. Adopting `Lifeline` is
strictly opt-in.

## Performance

Numbers from `benches/lifeline_overhead.rs`, best-of-30, u64 payload,
WSL on a single CCX. Re-run before quoting.

### Hot path

| Path | Cost | Δ vs baseline |
|---|---:|---:|
| `Stream::recv()` baseline                   | 3.2 ns | — |
| `Stream::recv_or_cancel(life, id)`          | 3.9 ns | **+0.7 ns** |
| `Lifeline::is_cancelled(id)` (1 M iter loop)| 0.2 ns | — |

The `+0.7 ns` is one extra atomic load per spin iteration plus a
slightly larger predicate (`!is_empty || is_cancelled`). The branch
predicts to "data first" in steady state.

### Cancel latency

Time from `cancel_*` call to all targeted workers joining:

| Operation | min | p50 | max |
|---|---:|---:|---:|
| `cancel_one` → 1 worker join          | 31 µs  | 39 µs  | 50 µs  |
| `cancel_all` → 4 worker joins         | 49 µs  | 84 µs  | 145 µs |
| `cancel_all` → 16 worker joins        | 202 µs | 333 µs | 517 µs |
| `cancel_all` → 32 worker joins        | 526 µs | 716 µs | 860 µs |

Latency is dominated by thread wake-up + scheduler dispatch (~10 µs
per worker on Linux), not by `Lifeline` itself. The `cancel_*` call
returns in well under 1 µs; the rest is the OS bringing each worker
back from `thread::park`.

### Reproduce

```bash
cargo bench --bench lifeline_overhead
```

## When NOT to use it

- **You don't need cancellation.** Plain `recv()` is faster and the
  surface area is smaller.
- **You need synchronous shutdown** (caller blocks until every worker
  has acknowledged). `Lifeline` is fire-and-forget: it issues the
  unparks and returns. Callers wanting synchronous shutdown should
  combine `cancel_*` with their own join/wait barrier.
- **You need more than 64 waiters per scope.** `MAX_WAITERS = 64`
  matches the bitmap width. Use multiple Lifelines or shard your
  worker pool.

## Concurrency contract

- **`register`** can be called from any thread, but the `Thread`
  handle passed in must be the one that will later block in
  `recv_or_cancel`.
- **`cancel_*`** can be called from any thread.
- **`is_cancelled`** can be called from any thread.
- **No re-registration**: once `register` returns a `WaiterId`, that
  id is fixed for the lifetime of the `Lifeline`. Dropping the
  worker thread and respawning is fine; the new thread should
  register anew and get a new id.

## Memory model

- `cancelled_global: AtomicBool` and `cancelled_mask: AtomicU64` —
  written `SeqCst` by `cancel_*`, read `Acquire` by `is_cancelled`.
- `waiters` table is behind a `Mutex` — only touched by `register`
  and `cancel_*`. Hot-path readers never lock.
- The `unpark()` calls happen **after** the cancel flag is published,
  so a freshly-woken worker re-checking `is_cancelled` is guaranteed
  to see the new state (Acquire on the load, SeqCst on the store).

## Composition patterns

### Group shutdown

```rust
let life = Arc::new(Lifeline::new());
for shard in shards {
    let l = life.clone();
    spawn(move || {
        let id = l.register(thread::current());
        loop {
            match shard.recv_or_cancel(&l, id) {
                Ok(item) => handle(item),
                Err(_)   => break,
            }
        }
    });
}

// Time to shut down the whole group:
life.cancel_all();
```

### Targeted shutdown (drain one slow worker)

Keep the `WaiterId` returned by `register` somewhere central (e.g. a
`HashMap<ShardKey, WaiterId>`) and pass it back as you need to:

```rust
life.cancel_one(workers[&"shard-3"]);
```

### Hierarchical scopes (manual)

If you need a tree of cancellation, compose multiple `Lifeline`s and
let workers check both:

```rust
let parent = Lifeline::new();
let child  = Lifeline::new();
// In worker: returns Err if either fired.
loop {
    match stream.recv_or_cancel(&child, child_id) {
        Ok(v) => handle(v),
        Err(_) => break,
    }
    if parent.is_cancelled(parent_id) { break; }
}
```

A native hierarchical scope (parent → children with cascade) is on
the roadmap; for now the manual pattern above works.
