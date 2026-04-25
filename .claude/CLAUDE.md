# arbitro-kit — Claude session bootstrap

This file is auto-loaded at the start of every Claude session. Read it
end-to-end before doing anything. **Do not skip.**

## Authoritative rules

The project ships hard rules under `.agent/rules/`. Each file is mandatory.
Read the full content of each file the first time you need to act on the
topic; do not paraphrase from memory.

| File | Topic | When to read |
|---|---|---|
| `.agent/rules/testing.md` | Build / test / bench under WSL | Before ANY `cargo build/test/bench` |

When new rule files appear, read them.

## Hard rules summary (do not violate — always check the source file)

These are the most-violated points in past sessions. Source of truth lives in
`.agent/rules/testing.md`; keep this list in sync.

### Build / test / bench (from `.agent/rules/testing.md`)

- **WSL is mandatory** for build / test / bench / execution.
- **Source under `/mnt/`** is allowed for compilation only.
- **Performance benchmarks must run from `/tmp/arbitro/`**, never from `/mnt/`
  (9P bridge distorts TCP/disk/memory/latency results).
- **Move/copy compiled bench executable** before running it. Use `cp -a`,
  **never `rsync`** (rsync is forbidden).
- Always: `timeout 120 ./bench`, `tee /tmp/bench.log`, **one bench at a time**,
  **never run benches in background**.
- Tests can run directly via `cargo test --lib` from `/mnt/...` (compilation
  is fine on the mount; only perf execution must avoid it).

### Canonical bench command pattern

```bash
# 1. Compile (from /mnt)
wsl bash -lc "cd '/mnt/d/.../arbitro-kit' && cargo bench --bench <name> --no-run 2>&1 | tail -5"

# 2. Copy + run (from /tmp/arbitro/)
wsl bash -lc "cp -a '/mnt/d/.../target/release/deps/<name>-<hash>' /tmp/arbitro/ && \
  cd /tmp/arbitro && timeout 120 ./<name>-<hash> --bench 2>&1 | tee /tmp/bench.log"
```

If the executable hash conflicts with a running copy, `rm -f /tmp/arbitro/<name>-<hash>`
first (`Text file busy` otherwise).

### Workspace boundaries

- **Only modify files inside `arbitro-io/`**. Never touch `@automatizadovip/`
  outside that subtree.

### Documentation language

- All `.md` files and code documentation: **English only**, no exceptions.
  (User-facing chat replies follow the user's language.)

### Bench code conventions

- Criterion-style benches live in `benches/`. Examples in `examples/`.
  Never at crate root or ad-hoc folders.
- Default `BENCH_ROUNDS` should be configurable via env var when feasible
  (see `benches/hub_sparse.rs` for the pattern).
- No `try_recv` spin loops in production code (`feedback_no_spin`). Park-based
  primitives are both faster AND ~0% CPU when idle.

## Architecture context (current state — update when major changes ship)

arbitro-kit ships zero-dep synchronization primitives. Topology map:

```
Pipe        — 1:1 named slot (Signal + slot)
Channel     — 1:1 RPC (req/resp pipe pair, fixed 1-slot)
Hub         — N:1 named ports (SignalSet bitmap + per-port slots + per-port reply Pipe)
Mpmc        — M:N anonymous (Vyukov-style sharded rings, per-(P,S) SPSC)
Ring        — SPSC bounded
Signal/SignalSet — primitives the rest are built on
Park        — stateless park/unpark (cursor-state primitive, used by Ring/Mpmc)
```

### Picking a primitive (rules of thumb — run the bench for actual numbers)

- **SPSC bounded** → `Ring`. Beats `crossbeam_channel::bounded` under real
  backpressure (apples-to-apples bench: `benches/ring_vs_crossbeam.rs`).
- **N:1 fan-in, named ports, fairness, low N (≤8)** → `Hub`.
- **N:1 fan-in, high throughput, anonymous, N≥4** → `Mpmc`. Beats both
  Hub and crossbeam at scale (bench: `benches/fanin_h2h.rs`).
- **1:1 RPC** → `Channel`. **1:1 named slot** → `Pipe`.

When in doubt, run `benches/<name>.rs` and read the actual numbers. Do not
quote stale numbers from memory.

## How to behave in this repo

1. Before any build/test/bench: re-read `.agent/rules/testing.md` if it has
   been ≥1 turn since you last consulted it.
2. Before suggesting a new bench: confirm it goes under `benches/`.
3. Before running a bench: confirm WSL, `/tmp/arbitro/`, `cp -a`, timeout,
   tee log, one at a time.
4. Never use `rsync`. Never write `.md` in non-English. Never modify outside
   `arbitro-io/`.
5. When you spot a recurring violation pattern: propose updating either this
   file or the rule under `.agent/rules/`.
