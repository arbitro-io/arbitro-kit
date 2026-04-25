# Pending: Bucketed zerocopy macro for wire serialization

> **Status**: deferred. Idea is sound for narrow cases; not justified yet
> for general use in arbitro-kit. Re-open when the criteria below are
> met by a concrete workload.

## The idea

A proc-macro `#[zerocopy_bucketed]` that takes one struct declaration
with `[u8]` fields annotated by size buckets and generates **N
fixed-size variants** plus a dispatch enum, so the receiver can do
`from_bytes(&[u8]) -> &Variant` in ~1–2 ns with no parsing, no
allocation.

```rust
// what the user writes
#[zerocopy_bucketed]
pub struct InboundMessage {
    #[bucket(min = 4, max = 226, steps = 8)]
    subject: [u8],
    #[bucket(min = 4, max = 4096, steps = 12, mode = "power_of_2")]
    payload: [u8],
}

// what the macro generates (sketch)
mod __inbound_message_bucketed {
    #[repr(C, packed)] pub struct V0  { pub _tag: u16, pub subject: [u8;   4], pub payload: [u8;    4] }
    #[repr(C, packed)] pub struct V1  { pub _tag: u16, pub subject: [u8;   4], pub payload: [u8;    8] }
    // ... 96 variants total
}

pub enum InboundMessage<'a> {
    V0(&'a __inbound_message_bucketed::V0),
    V1(&'a __inbound_message_bucketed::V1),
    // ...
}

impl<'a> InboundMessage<'a> {
    pub fn from_bytes(buf: &'a [u8]) -> Option<Self> {
        let tag = u16::from_le_bytes([buf[0], buf[1]]);
        match tag { /* ... cast and return ... */ }
    }
}
```

## Why the idea is appealing

- **True O(1) cast** on receive (~1 ns). No parsing.
- **Compile-time safety**: the receiver gets a typed reference, not
  raw bytes.
- **No external schema** (no `.proto`, no build step).
- **Aligned with `Stream<T>`'s philosophy**: move pointers / bytes,
  don't parse on the hot path.

## Why we did not ship it

### 1. Combinatorial explosion

`steps × steps` per pair of fields: `8 × 12 = 96` variants here.
A 3-field struct: `8 × 12 × 8 = 768`. A 4-field struct: ~6 000.
Each variant is a separate Rust type. The dispatch enum, the match
arms, the codegen all scale linearly with the variant count. I-cache
pressure, compile time, and binary size compound fast.

### 2. Wasted bytes per message

Buckets allocate the **upper bound** of each step. A 10-byte payload
in a `[u8; 32]` slot wastes 22 bytes. Under power-law message-size
distributions (the common case), the median message lands far below
its bucket maximum and pays heavy padding.

At scale: 300 M msg/s × 22 B padding = **6.6 GB/s of pinned memory
wasted** in the worst direction. The very throughput regime where
zerocopy seems to matter is the regime where the waste matters too.

### 3. No actual-length signal inside a bucket

Knowing the bucket only tells you the **maximum** size. The receiver
still needs the **actual** size of each `[u8]` field. Three ways, all
imperfect:

- **Sentinel** (null-terminated): doesn't work for arbitrary binary
  data.
- **Length prefix per field** (1–2 bytes each): eats the bucket budget
  and partially defeats the zerocopy benefit.
- **Implicit from context**: fragile, only works for tightly-coupled
  producer/consumer pairs.

### 4. Wire-format compat breaks easily

Adding one new bucket changes every tag value past it. Every
`steps = N → N + 1` is a wire-format break. Forward-compat needs
explicit tag versioning.

### 5. The cheap alternative is *almost* as fast

```rust
#[repr(C, packed)]
struct WireHeader { tag: u16, subject_len: u16, payload_len: u16 }
// followed by [u8] bytes

let h = WireHeader::ref_from_prefix(buf)?;       // ~1 ns cast
let subject = &buf[6..6 + h.subject_len as usize];
let payload = &buf[6 + h.subject_len as usize..];
```

5–8 ns end-to-end. Zero waste. No macro. The bucketed approach saves
~4 ns per message at the cost of all the problems above. **In any
real workload that 4 ns is rarely the bottleneck** (network, disk,
business logic almost always dominate).

### 6. In-process messaging does not need it at all

`Stream<Box<T>>` already moves a pointer in ~3 ns and the receiver
gets a typed `Box<T>` for **any** `T`. The wire-format question only
arises when bytes leave the process — TCP, IPC, shared memory.

## What we would build *first* before reviving this

1. **`Stream<Box<T>>` benchmarks for cross-thread message passing**.
   Confirm 3 ns/msg holds for representative `T` sizes.
2. **A simple `(tag, len, data)` wire codec** with `zerocopy`-derived
   header and slice views. Measure decode latency under a realistic
   message-size distribution.
3. **Integration with `rkyv`** for the cases that need variable-size
   zero-copy with offsets.
4. **Profile a real consumer**. Which step dominates? If wire decode
   is < 5 % of CPU, this macro saves nothing in practice.

Only if all three above are shipped and decode latency is
*measurably* the bottleneck do we revisit the macro.

## Re-open criteria

Build the macro when **all** of these hold:

- [ ] A concrete workload exists with a closed message vocabulary
      (subject and payload sizes cluster around a small known set).
- [ ] Profiling shows wire decode > 10 % of total CPU on the receiver
      hot path.
- [ ] Simpler alternatives (`(tag, len, data)`, `rkyv`) have been
      benched and shown insufficient.
- [ ] Message-size distribution is tight enough that bucket padding
      waste is bounded (e.g. ≤ 15 %).
- [ ] Schema stability is ≥ months — not a moving target requiring
      tag-version logic on every iteration.

## Design pointers for future implementer

When (if) we reopen, the prior scaffold is in git history. Key
choices to make on day one, before writing code:

1. **Tag size**: `u8` (256 variants) vs `u16` (65 k). `u16` is the
   safer default.
2. **Step distribution**: linear vs `power_of_2`. Power-of-two
   aligns with cache lines and avoids weird sizes — should be the
   default.
3. **Overflow policy**: `error` (return `None` on `pick_bucket`) vs
   `spill` (emit a follow-up message). `error` first; `spill` later
   if needed.
4. **Field-actual-length signaling**: pick one of (per-field length
   prefix in bucket payload, sentinel byte, context-implicit) and
   commit before generating code. Mixing breaks zerocopy.
5. **Versioning**: reserve 1 or 2 high tag bits for schema version
   so future bucket additions don't break existing receivers.

## Related work

| Project / Crate | What it does | Why it does NOT cover this niche |
|---|---|---|
| `zerocopy-derive` | `FromBytes` / `IntoBytes` derives | Per-struct, no auto-bucketing |
| `enum_variant_type` | Generates structs from enum variants | Inverse direction |
| `light-zero-copy-derive` | Borsh-compatible zero-copy with meta extraction | Not bucketed |
| `rkyv` | Zero-copy with offsets/pointers | Variable-size; no bucketing |
| `buffa` (Anthropic) | Protobuf with zero-copy views | Not bucketed |
| `musli-zerocopy` | Incremental validation | Not bucketed |
| `epserde-rs` | ε-copy serialization | Not bucketed |
| `flatbuffers` / `capnp` | Schema-based zero-copy reads | External schema, build step |

The combination "size-graded variant generation from a single Rust
struct, zero-copy castable" appears genuinely uncovered. That alone
isn't a reason to ship it — most uncovered niches are uncovered for a
reason — but it is a reason the design space is worth a deliberate
choice.

## Bottom line

Save this for the day a real workload shows wire decode on the hot
path. Until then, **`Stream<Box<T>>` for in-process** and a plain
`(tag, len, data)` codec for cross-process is the right level of
abstraction.
