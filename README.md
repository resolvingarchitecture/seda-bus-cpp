<div align="center">
  <h1>seda-bus-cpp</h1>
  <p><strong>Resolving Architecture &mdash; Clarity in Design</strong></p>
  <p>A small, broker-less, <strong>staged</strong> message bus for C++20.</p>
</div>

Work is decomposed into stages (`Channel`s) connected by bounded queues. One
shared thread pool drains every stage; each stage has its own concurrency
limit so none can monopolise the pool. There is no broker.

The envelope carried on the bus is `ra::common::Envelope` — the same wrapper
`seda-bus-java` uses via `ra-common-java`, and `seda-bus-python`/`seda-bus-ts`
use via their own `ra-common` ports (as of their `0.2.0` rewire) — aliased
here as `ra::seda_bus::Envelope`. `seda-bus-cpp` therefore depends on its
[`ra-common-cpp`](../../common/ra-common-cpp/) sibling; beyond that, no
dependency outside the standard library (`std::thread`, `<mutex>`,
`<condition_variable>`, `<atomic>`).

**Header-only.** Include `seda_bus/seda_bus.hpp` for everything, or pick
`seda_bus/envelope.hpp` / `seda_bus/bus.hpp` individually.

```cpp
#include "seda_bus/seda_bus.hpp"
using namespace ra::seda_bus;
using namespace std::chrono_literals;

Bus bus(4);  // 4 shared worker threads

bus.Channel("ingest",    ChannelConfig{}.Capacity(1000));
bus.Channel("transform", ChannelConfig{}.Capacity(1000).Concurrency(4));
bus.Channel("sink",      ChannelConfig{}.Capacity(1000));

bus.Subscribe("ingest",    [](Envelope&) { return true; });
bus.Subscribe("transform", [](Envelope& e) {
    auto s = EnvelopePayload(e).get<std::string>();
    std::transform(s.begin(), s.end(), s.begin(), ::toupper);
    SetPayload(e, s);
    return true;
});
bus.Subscribe("sink", [](Envelope& e) {
    // ... consume EnvelopePayload(e)
    return true;
});

bus.Publish(
    MakeEnvelope("ingest", std::string("hello"), {"transform", "sink"}),
    1000ms);

bus.Shutdown(5000ms);
```

`MakeEnvelope(to, payload, slip, sender, headers)` / `TargetService(env)` are
thin ergonomic helpers over `ra::common::Envelope`'s richer routing API
(`AddRoute` / `GetRoute` / `Ratchet`) — the same shape as
`seda-bus-python`'s `make_envelope`/`target_service`. `payload` is
`nlohmann::json` (any JSON-representable value), read back with
`EnvelopePayload(env)`.

## Features

|                           |                                                                           |
|---------------------------|---------------------------------------------------------------------------|
| **Bounded stages**        | each channel has a capacity — admission control                           |
| **Back-pressure policy**  | `Block` / `Reject` / `DropNewest` / `DropOldest` per stage                |
| **Per-stage concurrency** | how many envelopes a stage may process at once                            |
| **Delivery**              | `PointToPoint` (round-robin) or `PubSub` (fan-out)                        |
| **Routing slips**         | an envelope carries an itinerary of stages to visit                       |
| **Retry + dead-letter**   | nacked envelopes retry up to `max_attempts`, then route to a DLQ          |
| **Metrics**               | per-stage enqueued / delivered / nacked / dropped / dead-lettered / depth |
| **Graceful shutdown**     | stop accepting, drain within a timeout, then join the pool                |

`Bus` wraps a `shared_ptr` internally, so copies are cheap and share the same
stages, worker pool, and pending completion callbacks — matching
`seda-bus-rust`'s `Arc<Inner>` + `Clone`.

Per-hop delivery `attempts` are tracked on the *channel*, keyed by envelope
id — not on the envelope itself — since `ra::common::Envelope` has no such
field (mirrors `seda-bus-java`'s `SEDAMessageChannel.attempts` and
`seda-bus-python`'s `Channel._attempts`).

## Building

Assumes the monorepo layout (`ra-common-cpp` checked out as a sibling at
`../../common/ra-common-cpp`, matching `seda-bus-ts`'s
`"@resolvingarchitecture/ra-common": "file:../../common/ra-common-ts"`):

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

`SEDA_BUS_BUILD_TESTS` (default `ON`) builds the doctest suite;
`SEDA_BUS_BUILD_EXAMPLES` (default `ON`) builds `examples/pipeline.cpp`.
`ra-common-cpp` is pulled in via `add_subdirectory` with its own
`RA_COMMON_BUILD_TESTS` forced off, so its test/doctest target doesn't build
twice.

## Correctness suite coverage

See [`seda-bus-design/CORRECTNESS_SUITE.md`](../seda-bus-design/CORRECTNESS_SUITE.md) for what
C1–C7 mean. All in `tests/test_bus.cpp` unless noted.

| # | Property | Test case(s) |
|---|----------|---------------|
| C1 | Backpressure: Block / Reject / DropNewest / DropOldest | `"backpressure Block waits for room instead of rejecting"`, `"backpressure rejects when the queue is full"`, `"backpressure DropNewest matches Reject's contract when full"`, `"backpressure DropOldest always admits by evicting the front"` |
| C2 | Retry → dead-letter | `"nack retries then dead-letters"`, `"nack then succeeds on the final attempt delivers exactly once"`, `"a channel with no consumers dead-letters immediately"` |
| C3 | Consumer failure isolation | `"a consumer that throws on every third envelope doesn't take down the bus"` |
| C4 | Shutdown accounting | `"shutdown accounting: drained implies delivered + dead_lettered == published"`, `"shutdown accounting holds even when the timeout expires first"` |
| C5 | Config validation | `"config validation: capacity/concurrency/max_attempts are clamped, never zero"` (both the fluent builder and a raw-struct bypass — see `Channel::NormalizeConfig` in `bus.hpp`) |
| C6 | No resource leak across repeated lifecycles | `"no thread-count growth across repeated bus lifecycles"` (`/proc/self/status`, Linux-only; skips with a `WARN` elsewhere) |
| C7 | Concurrency correctness | `"concurrent producers deliver exactly once"` |

`tests/test_two_lock_queue.cpp` additionally unit-tests `TwoLockQueue`'s
`PushFront` headroom-overflow guard directly — a lower-level invariant check
underneath C2/C6, not itself one of C1–C7.

## What this is not

SEDA's original design also included a **controller** that watched per-stage
latency and queue depth at runtime and re-tuned thread allocation and shed
load automatically. That adaptive controller is not implemented here — every
setting is static configuration. See the shared
[`seda-bus-design/DESIGN.md`](../seda-bus-design/DESIGN.md) §3 for what a `2.0` controller would need.

## Companion implementations

- [`seda-bus-java`](../seda-bus-java/) — the original; `ra-common` integration, guaranteed delivery, datatype channels, LIFO routing slip.
- [`seda-bus-rust`](../seda-bus-rust/) — the closest concurrency-model match (real hand-rolled OS-thread pool), but still on its own standalone `Envelope`, not yet rewired onto `ra-common-rust`.
- [`seda-bus-python`](../seda-bus-python/) — carries `ra_common.Envelope` as of its `0.2.0` rewire; built to exercise free-threaded (PEP 703) CPython.
- [`seda-bus-ts`](../seda-bus-ts/) — carries `ra_common`'s `Envelope` as of its `0.2.0` rewire; event-loop model with an optional `Worker`-thread transport.

`seda-bus-cpp` follows `seda-bus-rust`'s concurrency model (real OS threads,
hand-rolled pool, atomic-CAS permits) but `seda-bus-python`/`-ts`'s envelope
choice (`ra-common`'s `Envelope`, not a standalone one) — Rust is the one
outlier still pending that rewire.

See [`seda-bus-design/DESIGN.md`](../seda-bus-design/DESIGN.md) for the shared design and a full
comparison table across all five ports.
