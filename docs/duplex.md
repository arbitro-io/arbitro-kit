# `Duplex<A, B>` — bidirectional unbounded SPSC

[← back to README](../README.md)

`Duplex<A, B>` pairs two [`Stream`](stream.md)s into one type — one
direction carries `A`, the other carries `B`. Each end has a fixed
`Send` and `Recv` type, enforced at compile time. The wrapper is
**zero-overhead**: sending and receiving on a `DuplexEnd` deserialise
1:1 to the underlying `Stream::send` / `Stream::recv`, so all the
performance numbers from `stream.md` carry over verbatim.

## Wire model

```
   left                                    right
   ─────                                   ─────
   send: A   ──────►   Stream<A>   ──────►  recv: A
   recv: B   ◄──────   Stream<B>   ◄──────  send: B
```

Each direction is an independent SPSC stream:

- One thread on `left` calls `send`; another may call `recv`.
- Same on `right`.
- The compiler prevents type confusion (you can't `send` a `B` from
  the `left` end, you can't drain `A` from the `right` end).

## When to reach for `Duplex<A, B>`

- **Request/response over `Stream`**: build RPC by hand without manually
  wiring two `Arc<Stream<_>>`s and remembering which clone is whose.
- **Fire-and-forget with verification on the producer side**: each
  `send` returns a [`Receipt`](stream.md#receipt) that the producer
  can later poll (`is_delivered`) or block on (`wait_delivered`). The
  RPC reply path is optional.
- **Bidirectional event loops**: both peers want to push messages
  asynchronously and drain in batches.
- **Anything you would have built with two `Arc<Stream<_>>`s**: the
  ergonomics are better and the type system catches direction mistakes.

## When NOT to use it

- **Pure unidirectional flow** — use `Stream<T>` directly. `Duplex`
  buys you nothing if only one side ever sends.
- **Bounded RPC with strict in-flight limit of 1** — that is what
  [`Channel<Req, Resp>`](channel.md) is for. `Duplex` is unbounded
  in both directions.
- **Real-time / no-allocation** — `Duplex` allocates segments on
  demand (it is built on `Stream`). Use [`Channel`](channel.md) or
  paired [`Pipe`](pipe.md)s if you need static memory.

## Why `strict_wake` is on by default here

`Duplex` constructs both inner streams with `Stream::new_strict()`,
which adds one `fence(SeqCst)` per `send` (~3–5 ns on x86). This
closes a Dekker race between the producer's cursor publish and the
consumer's park-state load. The race is benign in pure one-way
`Stream` traffic but becomes reachable in bidirectional / lockstep
patterns where each thread alternates between producing one direction
and parking on the other — exactly what `Duplex` is designed for.
Without the fence, lockstep RPC eventually deadlocks at the 1M-message
scale; with it, 5M+ verified RPC iterations run clean. The cost is
amortised away by `send_iter` (one fence per batch, not per item) —
see the K-batched numbers below.

## API surface

```rust
use arbitro_kit::stream::Duplex;

// Construct a paired duplex. Returns (left, right) endpoints.
let (left, right) = Duplex::<Request, Response>::pair();

// Outbound (each end sends its `Send` type).
let r = left.send(req);                          // Receipt for the seq
let r = left.send_iter(items).unwrap();          // bulk; receipt of last item

// Inbound (each end receives its `Recv` type).
let v = left.try_recv();                         // Option<Response>
let v = left.recv();                             // blocks (Park, phased backoff)
let n = left.recv_bulk(&mut buf, 256);           // non-blocking drain into buf

// Register the consumer thread for blocking-recv wakeups.
left.set_consumer(std::thread::current());

// Verification helpers (delegates to underlying Stream).
left.is_delivered(r);                            // 1 Acquire load
left.wait_delivered(r);                          // busy-spin until peer drains
left.wait_for_out(seq);                          // wait by raw seq
```

Plus introspection: `out_tail`, `in_cursor`, `peer_tail`,
`out_stream`, `in_stream`.

## Performance

Numbers from `benches/duplex_overhead.rs`, best-of-30, u64 payload,
WSL on a single CCX. Re-run before quoting.

### Zero-overhead vs raw `Stream`

| Path | `Stream<u64>` | `Duplex<u64, ()>` (one-way) |
|---|---:|---:|
| Cross-thread send + recv (per-item) | 3.0 ns | **3.0 ns** |

The wrapper costs nothing: the methods `#[inline]` to direct
delegations, and the only extra state is one extra `Arc` per end.

### Bidirectional RPC

| Pattern | ns/RT | RT/sec |
|---|---:|---:|
| Lockstep (per-msg, full reply) | 140 | 7.1 M |
| Batched K=8 | 26.8 | 37 M |
| Batched K=32 | 13.7 | 73 M |
| Batched K=128 | 8.3 | 121 M |
| **Batched K=512** | **6.3** | **159 M** |

### Fire-and-forget + delivery verification

| Pattern | ns/op | ops/sec |
|---|---:|---:|
| Send N + poll last receipt | **1.7** | **585 M** |

Producer fires `N` items, holds the last `Receipt`, polls
`is_delivered` once at the end. The per-message cost amortises down
to the slot-write throughput.

### Verified round-trip with `wait_delivered`

| Pattern | ns/RT | RT/sec |
|---|---:|---:|
| Per-msg (busy-spin lockstep) | 117.9 | 8.5 M |
| Batched K=8 | 17.4 | 57 M |
| Batched K=32 | 6.7 | 149 M |
| Batched K=128 | 3.5 | 283 M |
| **Batched K=512** | **2.0** | **488 M** |

`Duplex::send_iter(..)` + `Duplex::wait_delivered(r)` at K=512 yields
**2.0 ns per verified round-trip** — the fastest verified-RT in the
crate, faster than `Stream`'s own ack-RTT bench because the consumer
side has zero work between drains.

### Reproduce

```bash
cargo bench --bench duplex_overhead     # full Duplex sweep
cargo bench --bench stream_overhead     # underlying Stream numbers
```

## Concurrency contract

- One thread on each end calls `send`; one (possibly different) thread
  on each end calls `recv`. Internally each direction is its own SPSC
  `Stream`, so the "1 producer + 1 consumer per stream" rule must be
  honoured per direction.
- A `DuplexEnd` is `Send + Sync` whenever its `Send` and `Recv` types
  are `Send`. You can move it to another thread or share it via
  `Arc`, but the SPSC rule still applies.
- Multiple producers per side or multiple consumers per side require
  the dedicated MPSC / broadcast variants (not yet shipped).

## Composition with other primitives

- **Build RPC**: combine `send_iter` with `wait_delivered` on the
  client side, and `recv_bulk` + `send_iter` on the server side. See
  the bench's `Duplex RPC batched` scenarios.
- **Build pub-sub on top**: a hub of `Duplex<Topic, Event>` per
  subscriber gives a typed bidirectional fan-out. Slow consumers
  isolate naturally because each `Duplex` owns its own segment chain.

## Safety

- **Drop-safe**: dropping either `DuplexEnd` releases its share of
  both `Arc<Stream<_>>`s; in-flight payloads in either direction
  are drained on the final drop.
- **Type-safe direction**: `left.send(value)` only accepts `A`;
  `left.recv()` only returns `B`. Sending the wrong type, or draining
  the wrong direction, is a compile error.
- **No internal `unsafe` exposure**: every public API is safe Rust;
  the only `unsafe` lives in `Stream` itself, gated by the cursor
  protocol.

## Limitations and roadmap

- SPSC per direction. Multi-producer or multi-consumer per side is
  out of scope here; pick `Mpmc`, `Hub`, or wait for the broadcast
  variant.
- `wait_delivered` busy-spins on the cursor (inherits the limitation
  from `Stream`). A parked variant is on the roadmap.
- No built-in correlation for RPC replies — by design. If you send
  multiple in-flight requests on one `Duplex`, the caller is
  responsible for matching replies to requests (e.g. via a
  request-id field in the payload).
