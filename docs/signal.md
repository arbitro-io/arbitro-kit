# `Signal` — moved into `ParkWaiter`

[← back to README](../README.md)

> **Heads up.** The standalone `Signal` type was removed during the
> [`Waiter`](waiter.md) migration. Its Dekker-safe park/unpark dance
> now lives inside [`ParkWaiter`](../src/waiter/park.rs), the default
> backend for every primitive in the crate.

## What replaced it

| Old role of `Signal` | New home |
|---|---|
| Internal park/unpark engine driving every primitive | [`ParkWaiter`](../src/waiter/park.rs) — same Dekker dance, same costs, generic-ready |
| Public single-use M:1 gate | [`OneSignal<W>`](../src/gate/one_signal.rs) — single-use, payloadless, with `acquire_timeout` and async via `OneSignal<NotifyWaiter>` |
| Multi-channel coalesced wake | [`SignalSet<W>`](../src/gate/signal_set.rs) — up to 256 named channels |

## Why the change

Before the migration, `Signal` and `Park` were two parallel sync-only
gates wired into every primitive by hand. Adding async support meant
duplicating `*Async` types byte-for-byte. The [`Waiter`](waiter.md)
trait collapsed both into a single backend abstraction:

- Every primitive is now `<W: Waiter = ParkWaiter>`.
- Sync stays the default (zero source churn for existing callers).
- `<W = NotifyWaiter>` (feature `tokio`) gives the async variants
  for free.
- Future runtimes (io_uring, etc.) plug in as one new `Waiter` impl.

## Cost (unchanged from the old `Signal`)

| Path                              |              Cost |
| --------------------------------- | ----------------: |
| `wake()` — consumer not parked    |            ~0.3 ns |
| `wake()` — consumer parked        |      ~7 µs (syscall) |
| `wait_until()` ready on entry     |            ~0.5 ns |
| `wait_until()` park extra cost    |   +20 ns (1 SeqCst) |
| CPU while parked                  |                0% |

Numbers are reproducible via `cargo bench --bench gate_overhead` —
the bench keeps a `Signal` shim wrapping `ParkWaiter` so head-to-head
comparisons against `crossbeam Parker` / `Mutex+Condvar` /
`AtomicBool+park` stay on the table.

## See also

- [`waiter.md`](waiter.md) — the trait, the three concrete impls, and the
  io_uring extension story.
- [`signalset.md`](signalset.md) — public bitmap gate primitive.
