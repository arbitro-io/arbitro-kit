---
trigger: always_on
description: Mandatory rules for compiling and running benchmarks and tests
---

# BUILD, TEST & BENCH — MANDATORY

All builds, tests, and benchmarks must run inside WSL.

Source code may live under `/mnt/`, but performance execution must not. Windows-mounted paths use the 9P bridge and can distort TCP, disk, memory, and latency results.

## Compile

Compile from the project source directory when needed:

```bash
wsl bash -lc "cd /mnt/d/.../arbitro && cargo bench --bench <name> --no-run 2>&1"
Run Performance Benchmarks

For TCP, disk, memory, or latency benchmarks, compile first, move/copy the compiled benchmark executable to native WSL storage, then run it from /tmp/arbitro.

wsl bash -lc "
  mkdir -p /tmp/arbitro &&
  cp -a <compiled-bench-executable> /tmp/arbitro/ &&
  cd /tmp/arbitro &&
  timeout 120 ./<compiled-bench-executable-name> --bench 2>&1 | tee /tmp/bench.log
"
Rules
WSL is mandatory for build, test, bench, and execution.
Source under /mnt/ is allowed for compilation.
Never run performance benchmarks from /mnt/.
Performance benchmarks must run from /tmp/arbitro.
Move/copy the compiled benchmark executable before running it.
Use cp -a, never rsync.
Always use timeout 120.
Always log with tee /tmp/bench.log.
Run one benchmark at a time.
Never run benchmarks in background.