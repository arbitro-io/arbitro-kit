//! # arbitro-kit
//!
//! Zero-dependency synchronization and utility primitives extracted from the
//! Arbitro ecosystem. `std`-only, publishable standalone.
//!
//! ## Modules
//!
//! - [`gate`] — low-latency M:1 signal primitives and SPSC round-trip channel.
//!
//! ## Quick start
//!
//! ```no_run
//! use arbitro_kit::gate::{Signal, Channel};
//!
//! let sig = Signal::new();
//! let (client, server) = Channel::<u64, u64>::spsc();
//! ```

#![deny(unsafe_op_in_unsafe_fn)]

pub mod gate;
