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

arbitro-kit ships zero-dep synchronization primitives. The surface was pruned
to only what the arbitro server/client actually use plus two kept-on-purpose
extras (see AUDIT.md). Topology map:

```
Ring        — SPSC bounded (canonical; split !Clone/!Sync handles)   [server: drain events]
Mpsc        — M:1 fan-in on per-producer SPSC Rings + Vyukov gate    [server: NotifyRing]
Mpmc        — M:N anonymous (sharded per-(P,S) SPSC rings, stealing)  [kept — not yet consumed]
OneShot     — single value, once (1:1)                                [kept + client: pending]
SignalSet   — bitmap of ≤64 binary signals → 1 consumer              [common: gate]
Park/Notify/Noop — the Waiter backends (park/unpark · tokio · poll)  [injected into all above]
```

Removed in the prune (were unused by server/client): `Pipe`, `Channel`, `Hub`,
`OneSignal`, `Lifeline`, `Stream`, `Duplex`, `signal_packed`/`signal_states`.
`route::Shutdown` (the shutdown-return marker used by Mpsc/Mpmc) now lives in
`route/mod.rs`, not `hub.rs`.

### Picking a primitive (rules of thumb — run the bench for actual numbers)

- **SPSC bounded** → `Ring`.
- **M:1 fan-in** → `Mpsc`.
- **M:N anonymous, high throughput** → `Mpmc`.
- **single value once** → `OneShot`. **N binary signals → 1 consumer** → `SignalSet`.

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
