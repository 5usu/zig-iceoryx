# zig-iceoryx

**Zero-copy shared-memory publish/subscribe for Zig, modelled on the architecture of
[Eclipse iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2). 34 ns one-way latency
on a shared core, ~100 ns across cores, for 8 KB messages.**

> Not affiliated with or endorsed by the Eclipse Foundation or the iceoryx project.
> "iceoryx" is their trademark; this is an independent experiment that borrows the
> architecture and ports the publish-subscribe latency benchmark.

Publishers write a message once into shared memory; every subscriber, in any process,
reads it in place. No serialization, no copies, no system calls on the data path.

## Results

One-way latency, median of 5 interleaved runs (1M round trips each), 8 KB payload,
iceoryx2's own ping-pong benchmark (`benchmarks/publish-subscribe`) ported 1:1.
Intel i3-1305U (1 P-core + 4 E-cores), Linux 7.2.

| Cores | iceoryx2 `ipc` (Rust) | zig-iceoryx v1 | **zig-iceoryx v2** |
|---|---:|---:|---:|
| 0,1 — hyperthreads of one P-core | 161 ns | 52 ns | **34 ns** |
| 0,2 — P-core ↔ E-core | 368 ns | 276 ns | **111 ns** |
| 2,3 — E-core ↔ E-core | 394 ns | 291 ns | **103 ns** |
| 0,2 with `--send-copy` (fill 8 KB per send) | 354 ns | 261 ns | **208 ns** |

Two threads and two separate processes (`--processes`) measure the same.

**Read this before quoting the numbers.** iceoryx2 does much more per message than this
library (node liveness, crash cleanup, events, typed payloads, many safety checks). The
comparison shows what a minimal design costs, not that one language beats another. See
[What's missing](#whats-missing).

## The interesting part: v1 → v2

v1 was a straightforward Zig port of iceoryx2's design: index queues between publisher
and subscriber, chunks returned through a completion queue. It was faster than iceoryx2,
but only by ~25% across cores.

v2 changed no language feature, only **how many cache lines cross between cores per
message**: roughly 7 in v1, about 1 in v2. Across cores that took latency from ~280 ns
to ~105 ns.

| v1 (per message) | v2 |
|---|---|
| separate shared write index, read by the subscriber | readiness lives in the ring cell: `seq == pos + 1` |
| publisher reads the subscriber's read index on every send | publisher caches it; re-reads only when the ring looks full |
| buffer-size limit enforced by the publisher (needs that handshake) | subscriber enforces "keep newest N" on receive, behind a 32-deep ring |
| returned chunks polled on every loan and send | collected in a batch only when the free list is empty |
| subscriber reads the chunk header to learn the payload length | length travels in the cell next to the chunk index |

Both versions are in the repo: `src/zc.zig` (v2) and `src/zc_v1.zig` (v1).

## How it works

```
 publisher process                         subscriber process(es)
 ┌──────────────────────┐                 ┌───────────────────────┐
 │ loan() → chunk 7     │                 │ receive() → chunk 7   │
 │ write payload        │                 │ read payload in place │
 │ send()               │                 │ release()             │
 └─────────┬────────────┘                 └──────────▲────────────┘
           │            /dev/shm (mapped by both)    │
           │  ┌──────────────────────────────────┐   │
           ├─►│ data segment: chunk pool         │◄──┤  read-only for subscribers
           │  └──────────────────────────────────┘   │
           │  ┌──────────────────────────────────┐   │
           └─►│ delivery ring: {seq, chunk|len}  │───┘  one per publisher→subscriber
              │ return ring:   chunk indices     │◄──── subscriber hands chunks back
              └──────────────────────────────────┘
              service segment: config, port slots, change counter (discovery)
```

- **Service segment**: config plus publisher/subscriber slots. Creating or dropping a
  port bumps a change counter; ports check it on send/receive (one shared, read-mostly
  cache line) and connect or disconnect.
- **Data segment** (one per publisher): fixed-size chunks. Only the publisher allocates,
  so the free list and reference counts are plain process-local memory, no atomics.
- **Connection** (one per publisher/subscriber pair): a single-producer delivery ring
  of sequence-tagged cells, plus a return ring. With safe overflow on (the iceoryx2
  default), a subscriber that falls behind loses the oldest samples, never the newest,
  and the publisher never blocks.

## Build and run

Needs Zig 0.15.2 and Linux.

```bash
zig build test --summary all     # unit + concurrent stress tests
zig build --release=fast         # builds zig-out/bin/bench-pubsub and bench-pubsub-v1
./zig-out/bin/bench-pubsub       # 10M round trips, cores 0 and 1, 8 KB
```

With mise: `mise exec zig@0.15.2 -- zig build ...`

Benchmark flags (same as iceoryx2's benchmark where they overlap):

| Flag | Default | |
|---|---|---|
| `-i N` | 10000000 | round trips |
| `-p BYTES` | 8192 | payload size |
| `--cpu-core-participant-1 C` | 0 | core for participant 1 |
| `--cpu-core-participant-2 C` | 1 | core for participant 2 |
| `--send-copy` | off | fill the payload on every send |
| `--processes` | off | two forked processes instead of two threads |

## Using the library

```zig
const zc = @import("zc");

// process A
var svc = try zc.Service.create("camera", .{ .max_subscribers = 4, .subscriber_max_buffer_size = 1 });
defer svc.deinit();
var publisher = try svc.publisher(allocator, 4096); // max payload bytes
defer publisher.deinit();

const sample = try publisher.loanSliceUninit(5);
@memcpy(sample.payload, "hello");
_ = publisher.send(sample); // returns the number of subscribers reached

// process B
var svc = try zc.Service.open("camera");
defer svc.deinit();
var subscriber = try svc.subscriber(allocator);
defer subscriber.deinit();

while (true) {
    if (try subscriber.receive()) |s| {
        defer subscriber.release(s);
        std.debug.print("{s}\n", .{s.payload});
    }
}
```

As a dependency: `zig fetch --save git+https://github.com/5usu/zig-iceoryx`, then
`b.dependency("zig_iceoryx", .{}).module("zc")`.

## What's missing

This is an experiment, not production middleware. Compared with iceoryx2 (or other
shared-memory IPC libraries):

- **Busy-waiting only.** A subscriber waiting for data burns a full core. There is no
  futex/event sleep yet; adding one will cost some latency.
- **No crash handling.** A crashed process leaves files in `/dev/shm`, and chunks a dead
  subscriber held are not reclaimed.
- **Trusts shared memory.** Indices written by a peer are not validated; a buggy or
  malicious peer can cause out-of-bounds reads.
- **Pub/sub only.** No request/response, events, waitsets, history or user headers.
- **Fixed max payload per publisher**, chosen at creation.
- **Linux only**, Zig only (no C API).
- **Memory for speed.** Each subscriber can hold up to `ring_size` (default 32) chunks,
  so a publisher reserves `max_subscribers × (ring_size + borrowed) + loaned + 1`
  chunks. With safe overflow off, the publisher must check the subscriber on every
  send, so that mode doesn't get the full speedup.
- **Only latency measured.** Throughput has not been benchmarked.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
`src/bench_pubsub.zig` is a port of iceoryx2's benchmark (Copyright Contributors to the
Eclipse Foundation, Apache-2.0 OR MIT).
