# Stream<T> — Optimization & Fan-out Brainstorm

Joint output of the `stream-research` team (mathematician + engineer).
Discussion-only document — no code changes proposed here. The artifact's
purpose is to fix conclusions that were reached after ~5 exchanges so a
later implementation pass has a defensible starting point.

## 1. Where the current 3 ns floor comes from

### 1.1 Cost model: dependency-chain framing

The earlier additive budget (`T_slot_write + T_slot_read + T_park_check + …`)
mispriced the dominant cost. On a modern OoO x86 (Skylake-derived /
Zen3+), L1d sustains 2 loads/cycle of *throughput* at 4–5 cycles of
*latency*. Independent loads pipeline; dependent loads serialise. The
binding cost in `Stream<T>` is the length of the consumer's
**dependent-load chain**, not the sum of per-instruction times.

For the per-message `try_recv` path today the chain is:

```
1. load head_seg  (AtomicPtr, Relaxed)         — gives  seg
2. load seg.base_seq                            — depends on (1)
3. compute idx = head - base_seq                — depends on (2)
4. load seg.slots[idx]                          — depends on (3)
```

Three dependent L1 hits. At ~4 cycles each that is ~12 cycles ≈ 4 ns of
pure dependency latency; OoO partially overlaps it with the next
iteration's independent work (the Release publish on `head_pos`, the
`tail_pos` Acquire), landing at the measured **3.1 ns**.

In `send_iter` / `recv_bulk` the loop is inside one function call, so
the compiler can hoist `seg` and `base_seq` into registers across
iterations. Per-iteration the chain shortens to **2 dependent loads**
(idx-compute → slot load). That predicts ~2 cycles savings, ≈ 0.3 ns —
which is exactly the 3.1 → **2.8 ns** delta observed for `send_iter
K=256`. The model is tight.

### 1.2 What is *not* the bottleneck

- **Atomic ordering on `tail_pos` / `head_pos`.** Release/Acquire
  on x86 are plain `mov`s. The cross-thread coherence cost is
  amortised: in steady state the producer runs ahead by B ≈ SEG_SIZE
  slots (the K=256 batch result is the smoking gun), so a single line
  ping-pong covers ~256 messages. WSL on a single CCX puts that hop at
  ~10–15 ns; amortised ≈ 0.05 ns/msg. Atomics cost essentially nothing
  here.
- **`Park::wake()` on every `send`.** It is a single Relaxed load on a
  producer-private cache line (consumer only writes when actually
  parking). In steady state the line lives in L1d S-state — ~0.3 ns,
  unrecoverable without changing the wake protocol.
- **L1 throughput on slot R/W.** `mov` to/from L1d sustains 1 store
  and 2 loads per cycle. The compiler pipelines independent slot
  accesses across iterations. Per-msg cost is throughput-bound at
  ~0.3–0.5 ns combined, well inside the budget.

The dependency chain is the lever. Everything else is rounding.

## 2. Optimizations (ranked by leverage)

Each item below is **claim → math → tradeoff**. The numbers are
predictions from the dep-chain model, **not measurements**; an
implementation pass would need to validate them with the existing
`benches/stream_overhead.rs`.

### P0 — Seq-stamped flat ring with linked-segment overflow

**Claim.** Fold the per-message synchronisation into the slot itself.
Each slot becomes `Cell { seq: AtomicU64, value: UnsafeCell<MaybeUninit<T>> }`.
Producer writes `value` first, then Release-stores `seq = N+1`.
Consumer Acquire-loads `cell.seq`; if it equals `head + 1` the value is
visible (Release/Acquire pairing) and is taken. This is the LMAX
Disruptor publish protocol [1].

A producer-sticky overflow flag handles the unbounded contract: once
the ring is full (`tail - head ≥ CAP`) the producer flips
`in_overflow: AtomicBool` and switches to the existing linked-segment
chain; the consumer falls back to the segment path only after observing
"ring slot empty AND `in_overflow` is set". The hot path branch on
`in_overflow` only fires on the empty case, so it does not tax the fast
path. This state-machine shape — fast inline path with a sticky
fallback — mirrors `io_uring`'s SQ/CQ + overflow [3].

**Math.** Consumer's critical path collapses to one dependent L1 load:

```
1. idx = head & MASK                            — 1 cycle ALU
2. load cell.seq                                 — 4–5 cycles L1
3. branch on (seq == head+1)
4. extract cell.value                            — same cache line as (2)
```

The `value` load shares the line brought in by step 2, so it adds zero
incremental latency. **One dependent L1 hit on the critical path.**
That is asymptotically optimal: any cross-thread queue must do at least
one load to observe published state; the seq-stamped slot makes that
load *also* the value extraction.

Predicted: 3.1 ns → **~1.5 ns** per-msg one-way (matches Disruptor's
published SPSC numbers on similar HW).

**Tradeoffs.**
- Slot grows to ~16 B for `T = u64` (seq + value). Cache density
  halves, but L1d easily absorbs CAP=4096 × 16 B = 64 KB.
- For `T` larger than what fits with the seq stamp on one cache line,
  the value spans multiple lines. The protocol still works (Release on
  seq orders all prior value writes), but the consumer pays an extra
  line fill on the value load — degrades gracefully back toward today's
  cost rather than improving over it.
- Ring is bounded; unbounded contract preserved by the linked-segment
  overflow path. Net: zero-alloc only if you stay inside the ring.
- `tail_pos` is no longer load-bearing for synchronisation — it is
  retained (lazily updated, e.g. once per batch) only for `len()` /
  `cursor()` introspection.

### P1 — Software prefetch of slot[head + k]

**Claim.** After loading `cell` at index `head & MASK`, issue
`prefetcht0` for `(head + k) & MASK` (k ≈ 8). Hides the next
iteration's dependent L1 latency under the current iteration's
arithmetic.

**Math.** Each prefetch retires from the load port and does not block
retirement. With dep-chain depth 1 (post-P0), the prefetch shifts the
next iteration's load from "cold-in-L1" (~5 cycles) to "already-in-L1"
(~0). Saves ~3 cycles ≈ **~1 ns/msg**, taking us to **~0.5–1 ns**
fast-path.

**Tradeoffs.**
- Prefetch *requires* P0 first: in the segmented design, computing the
  prefetch address itself needs a `base_seq` lookup, which is the cost
  we are trying to remove. Order: P0 then P1.
- Tuning k is workload-dependent. k=8 is a starting point for u64
  payloads; larger T may want smaller k.
- On platforms with strong hardware prefetch (most server x86), the
  hardware may already prefetch sequential slots — gain shrinks to
  ~0.3 ns. Verify with bench.

### P2 — Drop `tail_pos` from the synchronisation hot path

**Claim.** Once P0 is in place, `tail_pos` is no longer the cross-thread
fence; the slot's `seq` is. Remove the per-send Release on `tail_pos`,
and the per-recv Acquire on `tail_pos`. Keep the field as a relaxed
counter for `len()` / `cursor()`, optionally updated only at segment
boundaries.

**Math.** Eliminates one Release store on the producer per send and
one Acquire load on the consumer per recv. On x86 these are plain
`mov`s, so the *instruction* cost is ~0.3 ns each. The bigger gain is
**removing a shared cache line from the hot path entirely**: with both
sides referring only to local-state (their own cursor) and slot-local
seq stamps, the only cross-thread line is the slot's cache line itself.
Predicted incremental save: **~0.3–0.5 ns/msg**, plus reduced false-share
risk at scale.

**Tradeoffs.**
- `len()` / `cursor()` become approximate (lag by up to a batch). Both
  are documented as snapshots already — the contract holds.
- `Receipt::wait_delivered` still needs a publish counter the producer
  releases; `head_pos` continues to play that role (consumer
  Release-stores after taking from a slot). No change there.

### P3 — SIMD bulk receive in `recv_bulk`

**Claim.** When draining ≥ 4 slots in one call and `T: Copy +
size_of::<T>() ≤ 16`, load 4 cells with one SIMD vector load and
publish them all with one Release on `head_pos`.

**Math.** L1 sustains 2× 32-byte loads/cycle on AVX2, so 4 × 16-byte
cells fold into a single 64-byte load (one cache line). Hides 4×
latency under one L1 hit. Per-bulk-iter cost trends toward
**~1.0–1.3 ns/msg** in the bulk path.

**Tradeoffs.**
- Orthogonal — only helps `recv_bulk`. Per-message `try_recv`
  unaffected.
- Constraints: `T: Copy`, fixed-size, alignment. Falls back to scalar
  loop otherwise.
- Adds an `unsafe` SIMD path with portability concerns. Worth it only
  after P0 + P1 ship and the bulk path is shown in profiling to matter.

### Predicted final numbers

| Scenario                    | Today  | P0     | P0+P1   | P0+P1+P2 |
|-----------------------------|--------|--------|---------|----------|
| send + recv (per-msg)       | 3.1 ns | ~1.6   | ~1.4    | ~1.2     |
| send_iter K=256 + recv      | 2.8 ns | ~1.4   | ~1.1    | ~1.0     |
| ack-RTT batched K=512       | 3.4 ns | ~1.7   | ~1.5    | ~1.3     |

These are model predictions. Validation gate before merging any of
P0–P3: re-run `benches/stream_overhead.rs` under the canonical WSL
+ `/tmp/arbitro/` protocol from `.agent/rules/testing.md`.

## 3. Fan-out architectures (1 producer, N consumers, broadcast)

Two designs, picked for different N regimes. They are not mutually
exclusive — both could ship as separate primitives.

### FO-1 — Multi-SPSC fanout (low to mid N, `T: Clone`)

```
producer ──► Stream[0]  ──► consumer 0
        ──► Stream[1]  ──► consumer 1
        ──► …
        ──► Stream[N-1] ──► consumer N-1
```

Producer holds `Box<[Stream<T>]>`. `broadcast(v)` clones `v` into each
ring with `send`. Each consumer owns one SPSC stream end-to-end.

**Cost analysis.**
- Producer: O(N) sends per broadcast in *latency*, but the N stores
  target N independent cache lines and N independent seq stamps with
  no inter-iteration dependency. Modern OoO with IPC ≈ 4 pipelines
  these: **N + ~4 cycles total**. At N = 8, ≈ 4 ns/broadcast wall-clock,
  plus N × clone cost.
- Consumer: identical to SPSC, ~1.4 ns/recv.
- Memory: N × (ring + overflow chain). Independent.

**Tradeoffs.**
- **Failure isolation is the headline property.** If consumer `i`
  stalls for τ seconds at producer rate λ, only stream `i` accumulates
  λτ × s bytes of pinned memory. Other consumers proceed at full rate.
  This is what makes FO-1 the safe default.
- Requires `T: Clone` (or `Arc<U>`).
- Crossover point: at ~N=8, two limits start to bite — store-port
  saturation (~1 store/cycle) and L1d capacity if each ring is large.
  Beyond N≈8, prefer FO-2.

### FO-2 — Shared log + per-consumer cursor + epoch retire (high N or zero-copy)

```
                       ┌── cursor[0] ─► consumer 0
                       │
producer ─► one ring ──┼── cursor[1] ─► consumer 1
                       │
                       └── cursor[N-1] ─► consumer N-1
```

Single seq-stamped ring. Each consumer holds its own `head_pos[i]`.
Producer publishes once. Slot retirement gated by `min(head_pos[i])`
(epoch-style); a slot is reusable when *all* cursors have passed it.
This mirrors the Aeron log buffer [2] and the LMAX Disruptor's
multicast topology [1].

**Per-consumer recv:** `cell.seq >= head_pos[i] + 1` (≥, not ==,
because the slot is shared). Still **one dependent L1 load**.

**Cost analysis.**
- Producer: O(1) per broadcast — one slot write, one seq stamp,
  regardless of N.
- Consumer: same as SPSC, ~1.4 ns. The cell line is read by all N
  consumers; in steady state it lives in shared L1/L2 across cores
  with no invalidation traffic (read-only after the producer's stamp).
- Memory: bounded by the lag of the slowest consumer.

**The slow-consumer problem (formal).** Let λ = producer rate, μᵢ =
consumer i's rate, μ_min = min μᵢ, s = slot size. If μ_min ≥ λ in
steady state, retained memory is bounded by ring CAP × s. If consumer
i stalls for τ seconds, retained memory grows by **ΔL = λ × τ × s**.
At λ = 300 M msg/s and s = 16 B, that is **4.8 GB/s of pinned RAM
growth**. A 100 ms GC pause = 480 MB pinned; a 1 s network hiccup
≈ 4.8 GB. Without an explicit bound, FO-2 is unbounded in failure.

**Mitigation: lag-threshold eviction.** Every consumer is configured
with a `lag_threshold` (e.g. CAP/2). Once `tail - head_pos[i] >
threshold`, consumer i is **disconnected** — its cursor is removed
from the min-set, future reads return `Disconnected`, and its slot
retention is released. This bounds memory to `CAP × s` regardless of
consumer behaviour, at the cost of explicit "I missed messages"
semantics for the slow consumer. Same shape as Aeron's
"unblocked publication" handling.

**Zero-copy view.** Consumer reads can return `Ref<'_, T>` borrowed
from the slot, with lifetime tied to a guard the consumer drops to
advance its cursor. Avoids `Clone` / move on `T`. Adds
`Ref`-lifetime gymnastics to the API.

**Tradeoffs.**
- O(1) producer cost is the win; failure radius is the cost.
- Needs eviction policy to be safe in production.
- API drift: consumers can no longer "take ownership" of `T` by move
  in the zero-copy variant — they get borrowed views.

### Picking between FO-1 and FO-2

| Property                | FO-1 (multi-SPSC) | FO-2 (shared log) |
|-------------------------|-------------------|-------------------|
| Producer cost           | O(N)              | O(1)              |
| Consumer cost           | ~1.4 ns           | ~1.4 ns           |
| Memory bound (no stall) | N × CAP × s       | CAP × s           |
| Failure radius          | per-stream        | global (need eviction) |
| `T` requirement         | `Clone`           | `Copy` or `Ref<T>`|
| Best at                 | N ≤ ~8            | N ≥ ~8 or zero-copy |

Default recommendation: **FO-1 for low N**, **FO-2 with
lag-threshold eviction for high N or zero-copy fan-out**.

## 4. Open questions

1. **Exact ring CAP for P0.** 4096 × 16 B = 64 KB fits in L1d on most
   x86, but `T` larger than u64 pushes that out of L1. Need a
   per-`T` heuristic, or a const-generic `CAP` parameter on `Stream<T>`.
2. **Crossover N for FO-1 → FO-2.** "≈8" is a model estimate from
   store-port saturation. Real number depends on `sizeof::<T>()`,
   clone cost, and core L1d size. Needs a microbench parametrised on N.
3. **Eviction policy for FO-2 stalled consumers.** Drop, log + drop,
   backpressure (block producer), or reroute to overflow chain? Each
   gives a different failure-mode contract. Pick before implementation.
4. **Receipt semantics in FO-2.** With N consumers, `is_delivered`
   means... delivered to whom? Options: any one consumer (`max
   head_pos`), all consumers (`min head_pos`), a named consumer.
   Probably needs a typed `BroadcastReceipt` distinct from the SPSC
   `Receipt`.
5. **Validation budget.** Each predicted number above is a model
   output, not a measurement. Before any of P0–P3 merges, the
   `stream_overhead` bench should be re-run under
   `.agent/rules/testing.md` protocol and the predictions checked.

---

### References

- [1] LMAX Disruptor — sequencer + slot-stamped publish protocol.
  Origin of the seq-stamped slot in P0 and the shared-log multicast
  in FO-2.
- [2] Aeron — log buffer with epoch-based retire and per-subscription
  cursors. Shape of FO-2's memory management.
- [3] `io_uring` — SQ/CQ ring with overflow list. Shape of P0's
  producer-sticky overflow state machine.
