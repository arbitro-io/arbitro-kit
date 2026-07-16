# arbitro-kit — Security + Performance Audit (Fable, per-module)

Zero-dependency concurrency primitives; hot-path critical for the arbitro broker.
Five parallel adversarial audits (gate, route, slot, stream, waiter), two axes:
**soundness/security** and **performance**. Each finding is `file:line` + a concrete
failure interleaving. Reachability is tagged so triage is honest:

- **LIVE** = reachable now from safe code / normal use.
- **FORMAL** = real model hole, unobservable on current x86/ARM targets (store buffers drain in ns), but one hardware/compiler change from live.
- **LATENT** = dead code / misuse-only, bites the day it's activated.

---

## TIER 0 — CRITICAL soundness (fix first)

| ID | Where | Reach | Defect | Fix |
|----|-------|-------|--------|-----|
| R-S1 | `route/mpsc.rs:247-277,440` | **LIVE** | `MpscConsumer` has **no `Drop`** and `Inner` has no `live_consumers` (Mpmc has both). Consumer dropped while a producer is blocked on a full ring → producer parks forever (**deadlock**); or consumer dropped with space free → `try_send` returns `Closed`, predicate `!is_full()` true → **100%-CPU livelock**. | Mirror Mpmc: `live_consumers`/closed flag + `Drop` on `MpscConsumer` that wakes all `producer_waiters`; `send`/`try_send` must distinguish `Closed` from `Full`. |
| R-S5 | `route/mpmc.rs:385-409` | **LIVE** | **Dead-shard black hole**: `live_consumers` is one global count, no per-shard liveness. A dropped consumer's shard keeps accepting `try_send` (returns `Ok`) → up to `M×RING_CAP` msgs/shard silently stranded until teardown = **silent acknowledged-message loss** in a broker. | Per-shard `alive: AtomicBool` cleared in `MpmcConsumer::drop`; producers skip dead shards; `try_send` fails when all dead. |
| ST-F1 | `stream/{stream,recv,send,duplex,buffered}.rs` | **LIVE** | `Stream<T>` is `Sync` with `send(&self)`/`try_recv(&self)`. Two threads sharing `Arc<Stream>` → `assume_init_read` same slot = **double-drop / data race, from 100% safe code**. Same via `DuplexEnd` + `Stream::buffered()` twice. Ring v2 already fixed this exact v1 bug with split handles; Stream/Duplex never got it. | Split `StreamProducer`/`StreamConsumer` (Ring-v2 pattern), or `&mut self` + drop `Sync`, or `unsafe fn`. |
| SL-S1 | `slot/channel.rs:298-319` | **LIVE** | `Channel::call_async` not cancel-safe: the only `await` is *after* the request is published. Future dropped mid-await (tokio `select!`/`timeout`) → retry reads/writes the same `resp_slot`/`req_slot` the server is writing → **data race UB + response misattribution**. Exactly the async RPC path this module exists for. | 3-state per-direction atomic (EMPTY/FULL/CONSUMING) + CAS take + epoch tag to drain stale resp; or a `CallGuard` that poisons on drop-mid-flight. |
| G-F1/F2 | `gate/signal_states.rs`, `gate/signal_packed.rs` | **LATENT** | **Orphan files** (not in `mod.rs`, never compiled/tested) still carrying the pre-fix `UnsafeCell<Option<Thread>>` worker pattern: F1 = designed-in lost-wakeup deadlock (`release()` no-ops when `UNPARKED`); F2 = `set_worker(&self)` data-race UB reachable from safe API → `unsafe impl Sync` unsound. One `mod` line from production. | **Delete** both files, or move to `experiments/` outside `src/`, or feature-gate *after* porting park.rs's `Mutex<Option<Thread>>` fix. |

## TIER 1 — HIGH

| ID | Where | Reach | Defect | Fix |
|----|-------|-------|--------|-----|
| ST-F3 / R-S2 | `stream/ring.rs:429-435` (used by `route/mpsc.rs:217,240`) | **FORMAL** | `should_notify_consumer` Vyukov wake-gate has **no `fence(SeqCst)` between `head.store(Release)` and `tail.load(Acquire)`** (SB/Dekker litmus). When the gate skips wake and the consumer's final predicate misses the store-buffered head → parks with an item queued; every later push also skips → permanent stall until shutdown. route rings use `NoopWaiter` (no fence anywhere). | `fence(SeqCst)` at top of `should_notify_consumer` before the `tail` load (standard eventcount requirement). |
| ST-F2 | `stream/ring.rs:614-633` | **LIVE** | `Ring::Consumer::drain` stores `tail` only *after* the loop; user callback `f` panics on item k → 0..k unwind-dropped, `tail` not advanced → `Ring::drop` re-drains same k slots → **double drop UB**. `route/mpsc.rs:530,824` forwards user callbacks here. | Commit `tail` per item, or a drop-guard that advances `tail` on unwind. |
| R-S3 | `route/hub.rs:173-183` | **LIVE** | `HubPort::send` (safe fn) guards the write-only-when-idle invariant with **`debug_assert!` only** → release build: double `send` races the drain's `assume_init_read` → data race UB + leak of prior `In`. | Do the `try_send` Acquire check unconditionally (panic/`Err`), or `unsafe fn send_unchecked`. |
| R-S4 | `route/mpsc.rs:626-716` | **LIVE (Miri)** | Async `send_async`/`recv_async` cache a raw pointer as `usize` before the loop, then call `&mut self` methods each iter (invalidates the tag under Stacked/Tree Borrows) and deref it → **UB / Miri-fail**; `usize` round-trip strips provenance. Sync versions are fine (recreate ptr after the `&mut`). | Restructure like the `NotifyWaiter` specializations (`mpsc.rs:722-849`) — split-borrow disjoint fields, no raw pointers. |
| R-S6 | `route/hub.rs:205-218,359-369` | **LIVE** | `HubShutdown::signal` wakes only the drain, not ports parked in `recv_reply`/`call` → a drain that exits without replying strands the port **forever**. | Close-on-shutdown wake for outbound pipes; `recv_reply` returns `Result<Out, Shutdown>`. |
| ST-F4 | `stream/send.rs:63-98` + `segment.rs` | **LIVE (on reuse)** | `send_iter` panic mid-batch after a segment boundary desyncs `tail_seg` from `tail_pos` (never stored) → on reuse `idx(seq)` underflows u64 (guard is `debug_assert`) → **OOB slot write**; also leaks unpublished items. | Drop-guard publishes `tail_pos` on unwind; make `idx` a hard `assert` on the cold boundary path. |
| SL-S2 | `slot/pipe.rs:155`, `slot/channel.rs:185` | **LIVE (misuse)** | `send`/`call` write the slot unconditionally; nothing (type or assert) prevents two producers (`&self`, `Sync`) → `MaybeUninit::write` over a value racing the consumer's `assume_init_read` = UB. | `debug_assert!(!has_data/!req_open)` (free in release), and consider split-handle like the rest. |

## TIER 2 — MEDIUM (soundness + correctness)

| ID | Where | Defect | Fix |
|----|-------|--------|-----|
| R-S7 | `route/hub.rs:78` | `Box::leak(format!("hub_port_{i}"))` per Hub → **unbounded permanent leak / slow DoS** (Hub-per-connection). | Static name table (n≤63) or `SignalSet::create` takes `Cow<'static,str>`. |
| R-S8 | `route/mpmc.rs:412-443` | `try_send_batch` skips the `is_terminal_for_producer` check `try_send` does → enqueues into dead rings; widens R-S5. | Add the guard. |
| G-F4 | `gate/signal_set.rs:257-275` | `lock*` discards `fetch_and` prev → release published between snapshot and lock is silently consumed; system liveness rests on an **undocumented** "drain after lock" contract every caller must honor. | Return `prev` from `lock*`, add `take_state()=swap(0)`, document. |
| G-F5 | `gate/lifeline.rs:157` | `next_id: AtomicU8` `fetch_add` wraps → after 256 register attempts `WaiterId` aliases → `cancel_one` wakes the wrong thread. | `fetch_update` bounded, `AtomicU8→AtomicUsize`. |
| W-F1 | `waiter/park.rs:377-378` | T3b does `fence(SeqCst); parked.load(Relaxed)` but the proof + **loom Scenario E** assume `Acquire` (a pre-load SC-fence does not make the load acquire). Formal gap + **loom verifies a stronger protocol than shipped**. | `Relaxed→Acquire` (free on x86); fix doc + make loom model the real fence+load. |
| ST-F5 | `stream/ring.rs` + `tests/loom_ring.rs` | Loom does **not** shim `UnsafeCell` → weakening `head`'s `Release` to `Relaxed` would still pass all loom tests (zero evidence for the load-bearing publish rule). Segmented `Stream` has no loom at all. | Miri with `-Zmiri-many-seeds` in CI; loom models for drain/wake-gate/segments. |
| ST-F6 | `stream/ring.rs:387-423` | `try_send_bulk` uses `pop()` → **reverses per-batch order**, violating the module's FIFO contract; re-exported as plain "bulk send". | Drain from front. |
| SL-S3 | `slot/channel.rs:196-201` | Poison path clears `resp_open` → a retry **deadlocks** (contradicts the "panics afresh" comment). | Don't clear on poison / check poison at `call` entry. |
| misc | `one_signal.rs:197`, `signal_set.rs:329`, `lifeline.rs:163` | `Instant::now()+timeout` overflow panic on `Duration::MAX`; `acquire_any(0)` parks forever; `Mutex::lock().unwrap()` propagates poison on cancel = shutdown-DoS (park.rs uses `into_inner` — inconsistent). | `checked_add`; `debug_assert!(mask!=0)`; align on `into_inner`. |

## TIER 3 — PERFORMANCE

**Dominant hot-path cost — unconditional `wake()` = one `mfence` per op (both directions):**
- W-F2 / ST-P1 / R-P6: `ParkWaiter::wake` runs `fence(SeqCst)` on the not-parked (saturated) path; ring/channel call it every op. Project's own bench: **11.4 → 48.2 ns/msg (4×)** (`park.rs:109-121`). **Fix = edge-trigger `wake()` at the callers** (empty→nonempty / full→nonfull) — but this MUST carry ST-F3/R-S2's `fence(SeqCst)` or it resurrects the lost-wake. Single highest-value perf item.
- ST-P2: `strict_wake` SeqCst fence is redundant with `ParkWaiter::wake`'s own fence for `W=ParkWaiter` (Duplex) → double mfence/send; gate on waiters lacking an SC fence.

**`SeqCst`→`Release` (each is a full barrier `xchg` → plain `mov`, paid per fire):**
- G-F7 `one_signal.rs:87,96`; G-F10 `lifeline.rs:157,194,210,227`; (route already has zero SeqCst — good).

**False sharing / missing `#[repr(align(64))]`:**
- R-P1 `mpmc.rs:73` (PRing), R-P3 `mpsc.rs:76`/`mpmc.rs:157` (adjacent waiters), R-P7 (`fanin_waiter` vs `shutdown`); G-F8/F9/F11 (SignalSet boxed chunks, lifeline hot atomics vs Mutex); ST-P6 (`tail_seg`/`head_seg`); SL-P3/F5 (Pipe/WakeGate no pad); W-F5 (`WakeGate`/`NotifyWaiter` no align).

**Allocation on hot path:**
- SL-P1 `pipe.rs:227` (**2 heap allocs per `recv_async`** — double `Box::pin`); R-P5 `mpsc.rs:626` (boxed generic async paths — route through `NotifyWaiter` specializations); ST-P3 (`Ring::new`/`Segment::new_boxed` build huge slot arrays on the **stack** → memcpy + stack-overflow risk; use `Box::new_uninit`).

**Other perf:**
- R-P2 `mpsc.rs:506` (`try_recv` O(M) scan from 0 → starvation; add Mpmc's rotating cursor); W-F3 `park.rs:287` (`Instant::now()` per spin iter, ~576×); ST-P4/P5 (busy-spin `wait_for`; per-item `recv_bulk` publishes); R-P4 (2 RMWs/hub msg).

## Loom / Miri coverage (biggest gaps first)
1. **route has ZERO loom** — model: mpsc wake-gate (catches R-S1+R-S2), hub port double-send (R-S3), mpmc PRing+drop+shutdown, MpscProducerPool bitmap, oneshot 5-state.
2. **slot has ZERO loom** — Pipe handoff + Channel round-trip + poison race + S1 cancellation.
3. **stream**: no loom for `drain`, `should_notify_consumer`, segmented Stream, consumer-drop→Closed; and loom can't catch ordering weakening (see ST-F5) → **needs Miri `-Zmiri-many-seeds` in CI**.
4. **gate**: no loom for SignalSet composed protocol (G-F4), OneSignal, Lifeline.
5. **waiter**: loom Scenario E must match the shipped fence+load (W-F1).

## SPSC ring — independent verification (2nd Fable pass)

A second, independent Fable reviewer re-derived the `stream/ring.rs` (SPSC) findings from source. **Nothing was refuted.**

- **ST-F2** — CONFIRMED **LIVE**. Single-threaded even: `tail.store` at ring.rs:629 runs *after* the loop, so a panicking user callback (forwarded from `route/mpsc.rs:530,824`) leaves moved-out slots that `Ring::drop` re-drops. The existing panic test uses `u64` (no `Drop`) so the UB is untested. **Worst ring-local defect.**
- **ST-F3 / R-S2** — CONFIRMED, with a **record correction**: the SB hole is admitted by bare **x86-TSO** too (not "C11-model only") — but it is *timing-shielded* by ParkWaiter's mandatory 64+512-iteration pre-park spin (store buffer drains in ns vs a µs spin window), and it is **architecturally absent on ARMv8** (STLR/LDAR RCsc forbids the reorder). Fix (`fence(SeqCst)` before ring.rs:432) is mandatory **before** the edge-triggered-wake perf work.
- **ST-F6** — CONFIRMED but attenuated: the reversal is *documented* at ring.rs:379-381; the real incongruence is the undocumented "Bulk send" re-export at `route/mpsc.rs:228-231`.
- **ST-F5, P1, P3** — CONFIRMED against the code + the project's own measurements.
- **Split-handle SPSC soundness (the part Fable built)** — VERIFIED SOUND: `!Clone`/`!Sync` enforced via `PhantomData<Cell<()>>` (ring.rs:295,531) → the `Stream` double-drop class is impossible at the `Ring` level by construction; all four cursor-publication legs correctly Release/Acquire-ordered; `wrapping_sub` occupancy wrap-safe (conservative stale cursors); disconnect protocol (closed-after-final-head-store + re-read) correct.

**Bottom line:** the SPSC ring's core transport is sound as Fable left it. The confirmed-real defects are **ST-F2** (LIVE double-drop — top priority), **ST-F3** (wake-gate, timing-shielded), **ST-F5/F6/P1/P3**.

## Recommended remediation order
1. **T0 soundness** (R-S1, R-S5, ST-F1, SL-S1) + **delete the two orphan gate files** (G-F1/F2). These are LIVE UB / data-loss / deadlock from safe code.
2. **T1** — the wake-gate fence (ST-F3/R-S2) *together with* the edge-triggered wake (T3 perf) so the perf fix ships correct; then R-S3/S4/S6, ST-F2/F4, SL-S2.
3. Add **loom models** for route + slot + stream-drain and **Miri in CI** — otherwise T0/T1 fixes are unverified.
4. **T2** correctness + **T3 perf** (edge-wake already in step 2; then SeqCst→Release sweep, align/padding sweep, hot-path alloc removal).
