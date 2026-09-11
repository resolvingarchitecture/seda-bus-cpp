# seda-bus-cpp — design notes

A C++20 port of [`seda-bus-rust`](../seda-bus-rust/) for its concurrency
model and [`seda-bus-python`](../seda-bus-python/)/[`seda-bus-ts`](../seda-bus-ts/)
for its envelope choice, tracking the shared design in
[`seda-bus/DESIGN.md`](../DESIGN.md). First C++ member of the `seda-bus`
family, following [`ra-common-cpp`](../../common/ra-common-cpp/)'s
conventions (header-only, C++20, `doctest`, `PascalCase` methods,
`snake_case_` members).

## Decisions

- **Header-only.** Same rationale as `ra-common-cpp`: no separate
  compilation step, faster to keep the bus's handful of tightly-coupled
  classes in sync, at the cost of consumers recompiling when these headers
  change.
- **Depends on `ra-common-cpp`; the envelope is `ra::common::Envelope`,
  aliased as `ra::seda_bus::Envelope`.** This was the initial build's one
  wrong call, corrected after review: the original version gave
  `seda-bus-cpp` its own lightweight `Envelope` struct on the assumption that
  only `seda-bus-java` depends on `ra-common` — true of `seda-bus/DESIGN.md`'s
  comparison table, but that table was stale. `seda-bus-python` and
  `seda-bus-ts` were deliberately rewired onto `ra_common.Envelope` /
  `ra-common`'s `Envelope` at their `0.2.0` (2026-09-10, user-requested) —
  three of four prior ports now share one envelope type; only
  `seda-bus-rust` remains a standalone outlier, not yet rewired. `seda-bus-cpp`
  follows the majority (and more current) precedent.
  - `envelope.hpp` is now a thin wrapper: `using Envelope = ra::common::Envelope;`
    plus `MakeEnvelope(to, payload, slip, sender, headers)` /
    `TargetService(env)` / `EnvelopePayload(env)` / `SetPayload(env, v)` —
    the same ergonomic helpers `seda-bus-python/src/seda_bus/envelope.py`
    (`make_envelope`/`target_service`) layers over the same richer type.
  - `payload` is `nlohmann::json` (any JSON-representable value), not
    `std::vector<uint8_t>` — matches Python/TS's content being `Any`/`unknown`
    stored via `ra_common::Envelope::AddContent`/`Content()`.
  - Routing no longer uses a `slip: VecDeque<String>` FIFO field on the
    envelope. `ra::common::DynamicRoutingSlip` is a **LIFO stack** of `Route`
    objects: `MakeEnvelope` pushes the itinerary tail-first then `to` last
    (`AddRoute` does `push_front`), so the first `NextRoute()`/`CurrentRoute()`
    pop yields `to`, matching Java/Python/TS. `Bus::Publish` resolves the
    target channel via `TargetService(env)` (`env.GetRoute()->service()`,
    which lazily pops the first hop on first access); `Impl::CompleteHop`
    checks `env.dynamic_routing_slip.PeekAtNextRoute() != nullptr` then calls
    `env.Ratchet()` to advance (pop the next hop) before republishing — the
    C++ spelling of Python's `_complete_hop`'s
    `peek_at_next_route()`/`ratchet()` pair.
  - **Per-hop `attempts` moved off the envelope and onto the channel**,
    keyed by envelope id (`Channel::BumpAttempt`/`ClearAttempt`,
    `std::unordered_map<std::string, uint32_t>` + a dedicated mutex) — since
    `ra::common::Envelope` has no `attempts` field. Mirrors
    `seda-bus-java`'s `SEDAMessageChannel.attempts` and
    `seda-bus-python`'s `Channel._attempts`. Cleared on both successful
    delivery and final dead-letter so the map never grows unbounded.
  - `Envelope` is **move-only** (it owns `std::unique_ptr<Route>` /
    `std::unique_ptr<messaging::Message>`), which is why `Channel`'s queue,
    `Requeue`, `DeadLetter`, `CompleteHop`, etc. were already written
    exclusively in terms of `std::move` — no changes needed there when the
    type switched from a plain copyable-by-construction struct to a
    move-only one.
  - Build: `CMakeLists.txt` pulls `ra-common-cpp` in via
    `add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../../common/ra-common-cpp ...)`
    (the CMake analogue of `seda-bus-ts`'s
    `"@resolvingarchitecture/ra-common": "file:../../common/ra-common-ts"`),
    forcing `RA_COMMON_BUILD_TESTS` off so its own doctest target doesn't
    build twice, then `target_link_libraries(seda_bus INTERFACE ra_common)`.
- **`seda-bus-rust` is still the closest sibling for concurrency, not
  `seda-bus-java`.** C++ has real OS threads and no garbage collector, so the
  concurrency model — hand-rolled fixed thread pool, one shared pool draining
  every stage, atomic CAS permits, `Mutex` + `Condvar` per-stage queue —
  translates almost mechanically from Rust:
  - `Pool` (`pool.hpp`): a `std::deque<std::function<void()>>` job queue
    guarded by `std::mutex`/`std::condition_variable`, N worker threads.
    Rust used an `mpsc` channel; C++ has no standard MPMC queue, so a
    guarded deque plays the same role. `Join()` lets every already-queued
    job run before joining threads (Rust's `Job::Stop` sentinel interleaves
    with `Job::Run` in FIFO order per-worker via the channel, which is a
    looser guarantee — this port's variant is strictly stronger, which is
    fine since nothing depends on the weaker behavior).
  - `Channel::TryAcquire`/`Release` (`bus.hpp`): the same `AtomicUsize` +
    `compare_exchange_weak` CAS loop as Rust's hand-rolled permits, not a
    semaphore type — ported almost line-for-line.
  - `Channel::Offer` under `Backpressure::Block`: `std::condition_variable`
    `wait_until(deadline)`, re-checked by the enclosing `while` loop exactly
    like Rust's `wait_timeout` + manual re-check.
- **`std::shared_mutex` for the channel registry and each stage's consumer
  list**, where Rust used `RwLock`. Plain `std::mutex` for the DLQ map,
  completion-callback map, and (new) per-channel attempts map, matching
  Rust's plain `Mutex` there too — the channel registry and consumer lists
  are read far more than written (every `Publish`/`Process` call), so
  allowing concurrent readers is worth the extra type; the others are not on
  as hot a path.
- **`Bus` is a value type wrapping `std::shared_ptr<detail::Impl>`**, mirroring
  Rust's `#[derive(Clone)] struct Bus(Arc<Inner>)`. `detail::Impl` inherits
  `std::enable_shared_from_this` so `Schedule()` can hand a
  `shared_ptr<Impl>` into a pool job's closure — the C++ equivalent of Rust's
  `let bus = self.clone(); move || bus.drain(chan)`.
- **`Consumer` is `std::function<bool(Envelope&)>`**, not a virtual interface.
  A stage's handler is conceptually a closure in every port (Rust's blanket
  `impl<F: Fn(&mut Envelope) -> bool> Consumer for F`, Python's
  `Callable[[Envelope], bool]`, TS's function type) — `std::function` is the
  direct C++ analogue and lets lambdas subscribe directly, no adapter class
  needed. `ra-common-cpp` uses virtual interfaces elsewhere (`Service`) where
  the Java original is an interface with multiple methods; a consumer here
  is one method, so a `std::function` is the better fit.
- **A thrown exception from a consumer is caught and treated as a nack**
  (`detail::SafeReceive`), exactly like Rust's `catch_unwind` around
  `Consumer::receive` — a misbehaving consumer must not take down a worker
  thread or crash the process.
- **`ChannelConfig` is an immutable fluent builder** (`Capacity(n)`,
  `Concurrency(n)`, `SetDelivery(d)`, `SetBackpressure(b)`, `MaxAttempts(n)`,
  each returning a modified copy) — same shape as Rust's consuming builder
  (`ChannelConfig::default().capacity(..)`). The setters are named
  `SetDelivery`/`SetBackpressure` rather than `Delivery`/`Backpressure` only
  to keep call sites unambiguous to a reader scanning past the enum types of
  the same name, even though C++ scoping would not actually conflict.
- **`BATCH` is 16** (`kBatch` in `bus.hpp`), matching Rust and Python, not
  Java's 64 or TS's 32.
- **No persistence, no datatype channels, no pull model** — same gaps as
  Rust/Python/TS (§2.1 of the shared design). Only `seda-bus-java` has these.

## What's deliberately not ported

Same as every non-Java sibling: no adaptive controller (shared design §3), no
retry backoff, no priority queues, unbounded dead-letter channels by default
(mitigated the same way as Rust: `SetDeadLetterChannel` gives the DLQ
`Capacity(4096)` + `DropOldest` — a stronger default than `seda-bus-python`'s,
which reuses the DLQ channel's already-registered config or plain defaults if
none; kept here as a deliberate divergence, not an oversight).

## Testing

`tests/` is a `doctest` suite (vendored single header, same copy
`ra-common-cpp` uses) with two files: `test_envelope.cpp` (`MakeEnvelope`,
`TargetService`, payload round-trip, sender/headers, and the
`Ratchet`/`PeekAtNextRoute` slip-walk) and `test_bus.cpp` — a port of
`seda-bus-rust/tests/bus.rs`'s nine integration tests (round-robin, pub/sub
fan-out, routing-slip itinerary, back-pressure, retry/dead-letter, drain-on-
shutdown, pause/resume, unknown channel, and a six-thread/3000-envelope
exactly-once stress test), adapted to `MakeEnvelope`'s JSON payload (plain
ints/strings instead of `vector<uint8_t>`, which incidentally dropped the
`memcpy` the byte-payload version needed). Since C++ has no built-in
`mpsc::channel`, a small `TestQueue<T>` helper (mutex + condition_variable +
deque) stands in for it.

A ThreadSanitizer run was attempted for extra confidence on the concurrency
code but could not be validated cleanly in this sandbox (TSan refuses to
start without an ASLR workaround, and even then produced "double lock of a
mutex" reports that traced back to plain, correctly-scoped `std::lock_guard`s
in unrelated test cases — consistent with stale shadow-memory state from
stack-address reuse across test cases, not a real bug). Correctness rests on
the doctest suite plus manual review of `Pool::Join()`'s `thread::join()`
happens-before guarantee.
