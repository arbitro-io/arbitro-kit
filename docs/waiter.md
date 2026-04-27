# `waiter` — the unified wait/wake contract

`arbitro-kit` ships **one family of primitives** (`Pipe`, `Channel`, `OneShot`,
`Ring`, `Mpmc`, `Hub`, `SignalSet`) that work over **any wait/wake backend**.
Sync (OS-thread `park`/`unpark`), async (`tokio::sync::Notify`), and future
runtimes (io_uring) plug in via a single trait: [`Waiter`](../src/waiter/mod.rs).

## Why

Before the lift, the crate had two parallel families:

- **Sync** (`Pipe`, `Channel`, `OneShot`, `Ring`, `Mpmc`, `Hub`) on
  `thread::park`/`unpark`.
- **Async** (`PipeAsync`, `OneShotAsync`) on `tokio::sync::Notify`.

Adding a third runtime (io_uring completion-driven) under that shape would
have meant `PipeUring`, `OneShotUring`, `ChannelUring`, …, `HubUring` —
*N* new files per primitive per runtime, copy-pasted byte for byte except
for the wake/wait calls.

The `Waiter` trait collapses that into **one new file per runtime**.
Every primitive in the crate inherits multi-runtime support automatically,
because each is generic over `W: Waiter` with a default of `ParkWaiter`.

## The contract

```rust
pub trait Waiter: Default + Send + Sync {
    /// Register the consumer. No-op for runtime-multiplexed waiters
    /// (Notify, io_uring); mandatory for `ParkWaiter`.
    fn set_worker(&self, _: std::thread::Thread) {}
    fn has_worker(&self) -> bool { true }

    /// Wake whoever is waiting (or arm a wake-on-arrival flag).
    fn wake(&self);
}

/// Sync extension — blocks the caller's thread.
pub trait BlockingWaiter: Waiter {
    fn wait_until<F: FnMut() -> bool>(&self, ready: F);
}

/// Async extension — returns a future the caller awaits.
/// Uses RPITIT (Rust 1.75+) so no GATs in the trait.
pub trait AsyncWaiter: Waiter {
    fn wait_until<'a, F>(&'a self, ready: F)
        -> impl Future<Output = ()> + Send + 'a
    where F: FnMut() -> bool + Send + 'a;
}
```

Two extension traits instead of one unified `wait_until` because Rust does
not let one method be both sync and async. A primitive that wants a sync
`recv` requires `W: BlockingWaiter`; the async `recv_async` requires
`W: AsyncWaiter`. Callers pick the bound at use-site.

## Built-in implementations

| Impl            | Implements              | Wraps                                       | Wake cost (parked / hot)        |
|-----------------|-------------------------|---------------------------------------------|---------------------------------|
| `ParkWaiter`    | `Waiter + BlockingWaiter` | OS thread, Dekker-safe SeqCst recheck     | ~7 µs syscall / ~0.3 ns Relaxed load |
| `NotifyWaiter`  | `Waiter + AsyncWaiter`  | `tokio::sync::Notify`                       | runtime-multiplexed (~300 ns)   |
| *(future)* `UringWaiter` | `Waiter + AsyncWaiter` | per-task io_uring CQE poll       | TBD                             |

## How to pick

| You have…                                           | Pick                          | Type alias                |
|-----------------------------------------------------|-------------------------------|---------------------------|
| Both halves on OS threads, no runtime               | default `W = ParkWaiter`      | `Pipe<T>`, `Channel<R,S>` |
| Wake fires from non-tokio thread, waiter is a task  | `NotifyWaiter`                | `PipeAsync<T>`, `ChannelAsync<R,S>`, `OneShotAsync<T>`, `HubAsync<I,O>`, `MpmcAsync<T,CAP>` |
| Both halves inside a tokio runtime                  | `NotifyWaiter`                | same as above             |
| Kernel-driven completions                           | `UringWaiter` (when shipped)  | TBD                       |

The default is **always sync OS-thread** — `Pipe::<u64>::new()` compiles to
the same code as before the lift. Picking a different backend is an opt-in
type-parameter override.

## Performance

Every primitive is **generic, not dyn**. After monomorphization
`Pipe<T, ParkWaiter>::recv` compiles to byte-identical code as the
pre-lift `Pipe::recv`. No virtual dispatch, no boxing.

The acceptance gate during the lift was **±2%** vs the pre-lift baseline
on every primitive's bench. Bigger drift means a missing `#[inline]` —
the trait abstraction leaking cost.

## Adding a new runtime — the io_uring sketch

The full effort to bring io_uring support to **every primitive** in the
crate is one file: `src/waiter/uring.rs` (feature-gated). Skeleton:

```rust
// src/waiter/uring.rs    (cfg(feature = "io-uring"))

use std::sync::atomic::{AtomicUsize, Ordering};
use crate::waiter::{Waiter, AsyncWaiter};

pub struct UringWaiter {
    /// Sequence counter. `wake` bumps it; `wait_until` reads it before
    /// each predicate check, then submits a NOP-with-userdata SQE so the
    /// kernel hands a completion back when something else does work.
    seq: AtomicUsize,
    // …per-task ring handle, registered userdata, etc.
}

impl Default for UringWaiter { /* … */ }

impl Waiter for UringWaiter {
    fn wake(&self) {
        self.seq.fetch_add(1, Ordering::Release);
        // Submit a 0-byte NOP so the consumer's CQE poll wakes.
        // …
    }
}

impl AsyncWaiter for UringWaiter {
    fn wait_until<'a, F>(&'a self, mut ready: F)
        -> impl Future<Output = ()> + Send + 'a
    where F: FnMut() -> bool + Send + 'a
    {
        async move {
            loop {
                let snapshot = self.seq.load(Ordering::Acquire);
                if ready() { return; }
                // Park: submit NOP-with-userdata, await CQE.
                self.cqe_when_seq_advances(snapshot).await;
            }
        }
    }
}
```

That's it. Every primitive — `Pipe<T, UringWaiter>`,
`Channel<Req, Resp, UringWaiter>`, `Hub<In, Out, UringWaiter>`,
`Mpmc<T, CAP, UringWaiter>` — automatically gains io_uring support
because they're all generic over `W: Waiter`.

The non-trivial part is bridging io_uring's completion model (kernel
pushes events) to the predicate-based `wait_until` contract. The
sketch above uses a **sequence number + NOP completion**: the wake
side bumps the seq and submits a NOP; the wait side snapshots the
seq before the predicate check, and awaits a CQE that fires when a
NOP completes. That closes the lost-wake window between predicate
re-check and park, exactly as `ParkWaiter`'s SeqCst-recheck does on
OS threads.

## Internal shape

```
waiter/
├── mod.rs       — Waiter, BlockingWaiter, AsyncWaiter
├── park.rs      — ParkWaiter (default; wraps gate::Park)
└── notify.rs    — NotifyWaiter (feature = "tokio")
```

`gate/{signal,park}.rs` are kept as private building blocks — `Park`
backs `ParkWaiter`'s spin-then-park dance, and `Mpsc` + `stream/stream.rs`
still use it directly until they're lifted in a future pass.
