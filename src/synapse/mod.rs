//! # synapse
//!
//! SPMC (1 producer → N consumers) work-stealing primitive. Sibling module
//! to [`gate`], not a submodule — while `Synapse` is built on top of
//! [`gate::Signal`] for its park protocol, its wire model and contract are
//! distinct enough that it lives in its own namespace.
//!
//! ## Relationship to `gate`
//!
//! | Primitive              | Lives in   | Topology    |
//! | ---------------------- | ---------- | ----------- |
//! | `Signal`, `SignalSet`  | `gate`     | M:1 signal  |
//! | `Pipe`, `Ring`         | `gate`     | SPSC        |
//! | `Channel`              | `gate`     | SPSC req/resp |
//! | `Hub`                  | `gate`     | N:1 multiplex |
//! | **`Synapse`**          | **`synapse`** | **1:N work-stealing** |
//!
//! `Synapse` is the structural dual of `Hub` (which fans N→1). Rather
//! than nest it inside `gate` we promote it to a top-level module: it
//! carries its own cost model, its own tests, and its own docs. Users
//! typing `arbitro_kit::synapse::Synapse` get an intent-revealing path.
//!
//! ## Wire model
//!
//! ```text
//!                    ┌─────────────────────────────┐
//!  producer ──push─► │ head  (single writer)       │ Release store
//!                    │ tail  (CAS claim by N)      │ AcqRel CAS
//!                    │ slots[CAP]                  │
//!                    │ shutdown: AtomicBool        │
//!                    │ idle_mask: AtomicU64        │ O(1) targeted wake
//!                    │ signals[N]  ← one Signal per consumer
//!                    └─────────────────────────────┘
//!                           ▲          ▲          ▲
//!                    consumer_0  consumer_1  consumer_{N-1}
//! ```
//!
//! ## Concurrency contract
//!
//! - **Exactly one producer** calls [`Synapse::try_send`] / [`Synapse::send`].
//! - **N consumers** may call [`Synapse::try_recv`] / [`Synapse::recv`].
//!   Each consumer registers itself with [`Synapse::bind_consumer`] once
//!   from its own thread before the first blocking `recv`.
//! - All claims are serialized by a CAS loop on the shared tail.
//!
//! ## File layout
//!
//! This module is split by responsibility to keep each file small and
//! focused:
//!
//! - [`state`] — struct definition, construction, accessors, `Drop`.
//! - [`producer`] — `try_send` / `send` (the single-producer side).
//! - [`consumer`] — `try_recv` / `recv` (the N-consumer side).
//! - [`wake`] — `idle_mask` targeted wakeup + `shutdown` helpers.
//! - `tests` — integration tests for the full primitive.
//!
//! [`gate`]: crate::gate
//! [`gate::Signal`]: crate::gate::Signal

mod state;
mod producer;
mod consumer;
mod wake;

#[cfg(test)]
mod tests;

pub use state::{Synapse, Shutdown};
