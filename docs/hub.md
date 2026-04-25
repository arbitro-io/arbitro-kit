# `Hub<In, Out>` — N:1 multiplexer with per-port reply

[← back to README](../README.md)

`Hub` wires N producer ports to a single consumer ("drain") using
`SignalSet` as the multiplexor. Each port has its own inbound slot (the
`SignalSet` bit IS its signal — saves one atomic per send) and its own
outbound `Pipe<Out>` for the drain's reply. Round-robin fairness across
ports prevents starvation.

Max **63 user ports** (bit 63 of the coordinator `SignalSet` is reserved
for `HubShutdown`, which wakes the drain out of a blocked `recv_batch`
for clean teardown — and is the *single* source of truth for shutdown,
no shadow `AtomicBool`).

The drain iterates only the **set bits** of the coordinator state via
`trailing_zeros`, so cost scales with the number of active ports per
wake, not with the configured `N`. Inbound slots are 64-byte padded so
two adjacent ports never share a cache line.

## Wire model

```text
port 0 ──send(In)──┐
port 1 ──send(In)──┤──► SignalSet bit OR ──► drain.recv_batch()
...                │        (one atomic OR per port send)
port N ──send(In)──┘

drain replies per port ──► Pipe<Out>_i ──► port_i.call() receives
```

## Cost

```
── Hub hot path (WSL /tmp/arbitro, 500 × 1000 ops) ──
variant                        mean_ns/op   p50    p99      ops/sec
───────────────────────────────────────────────────────────────────
signalset_release+lock (raw)         7.71   7.33   17.54    129 M
hub_send + local drain              12.54  12.21   19.56     80 M

── Full RTT (port → drain → reply, cross-thread) ──
hub_rtt_1port                          —    89.01  163.54    11.5 M
hub_rtt_4port (aggregate)              —       —       —     10.4 M
```

At 4 producers the drain saturates near 10M ops/sec — that's the
ceiling of a single consumer. For higher throughput, shard across
multiple `Hub`s.

Reproduce with:

```bash
cargo bench --bench hub_overhead   # Hub send + drain + RTT
cargo bench --bench hub_sparse     # drain cost on sparse-bit fan-in
cargo bench --bench hub_multibit   # drain cost when many bits fire per wake
cargo bench --bench fanin_h2h      # Hub vs Mpmc vs crossbeam_channel
```

## Shutdown

`drain.shutdown_handle()` returns a cheap handle that any supervisor
thread can clone. Calling `.signal()` sets bit 63, which wakes the
drain out of `recv_batch` with `Err(Shutdown)`. The drain can then
do its cleanup and exit cleanly without any external signaling
infrastructure.

## Usage

```rust
use arbitro_kit::gate::{Hub, Shutdown};

let (drain, ports) = Hub::<u64, u64>::new(4);
let shutdown = drain.shutdown_handle();

// Drain thread: handle any port that fires, reply to that port.
let d = std::thread::spawn(move || {
    drain.bind();
    loop {
        match drain.recv_batch(|port_idx, msg, reply| {
            reply.send(msg + port_idx as u64 * 1000);
        }) {
            Ok(()) => continue,
            Err(Shutdown) => break,
        }
    }
});

// Each port moves to its own producer thread.
for (i, port) in ports.into_iter().enumerate() {
    std::thread::spawn(move || {
        port.bind();
        for k in 0..100u64 {
            let reply = port.call(k);
            assert_eq!(reply, k + i as u64 * 1000);
        }
    });
}

// Supervisor signals shutdown; drain wakes and exits cleanly.
shutdown.signal();
d.join().unwrap();
```
