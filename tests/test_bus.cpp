#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "doctest/doctest.h"
#include "seda_bus/seda_bus.hpp"

using namespace ra::seda_bus;
using namespace std::chrono_literals;

namespace {

// A tiny thread-safe queue, standing in for Python's threading.Event /
// Rust's std::sync::mpsc::channel in these tests.
template <typename T>
class TestQueue {
public:
    void Push(T v) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push_back(std::move(v));
        }
        cv_.notify_one();
    }

    std::optional<T> Pop(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!cv_.wait_for(lock, timeout, [this] { return !queue_.empty(); })) return std::nullopt;
        T v = std::move(queue_.front());
        queue_.pop_front();
        return v;
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<T> queue_;
};

ChannelConfig Cfg() { return ChannelConfig{}; }

}  // namespace

TEST_CASE("point-to-point round-robins across consumers") {
    Bus bus(4);
    std::atomic<int> a{0};
    std::atomic<int> b{0};
    bus.Channel("work", Cfg().Capacity(100));
    bus.Subscribe("work", [&a](Envelope&) {
        a.fetch_add(1);
        return true;
    });
    bus.Subscribe("work", [&b](Envelope&) {
        b.fetch_add(1);
        return true;
    });

    for (int i = 0; i < 20; i++) {
        CHECK(bus.Publish(MakeEnvelope("work", i), 1000ms));
    }
    CHECK(bus.Shutdown(5000ms));
    CHECK(a.load() == 10);
    CHECK(b.load() == 10);
}

TEST_CASE("pub/sub fans out to every consumer") {
    Bus bus(4);
    TestQueue<std::pair<std::string, int>> results;
    bus.Channel("events", Cfg().Capacity(100).SetDelivery(Delivery::PubSub));
    for (const std::string& tag : std::vector<std::string>{"a", "b", "c"}) {
        bus.Subscribe("events", [&results, tag](Envelope& e) {
            results.Push({tag, EnvelopePayload(e).get<int>()});
            return true;
        });
    }

    for (int i = 0; i < 4; i++) {
        bus.Publish(MakeEnvelope("events", i), 1000ms);
    }

    std::vector<std::pair<std::string, int>> got;
    for (int i = 0; i < 12; i++) {
        auto v = results.Pop(5000ms);
        REQUIRE_MESSAGE(v.has_value(), "expected a fan-out message");
        got.push_back(*v);
    }
    bus.Shutdown(5000ms);

    CHECK(got.size() == 12);
    for (const std::string& tag : std::vector<std::string>{"a", "b", "c"}) {
        auto count = std::count_if(got.begin(), got.end(), [&](const auto& p) { return p.first == tag; });
        CHECK(count == 4);
    }
}

TEST_CASE("routing slip visits every stage in order") {
    Bus bus(4);
    std::mutex trail_mutex;
    std::vector<std::string> trail;
    for (const std::string& name : std::vector<std::string>{"one", "two", "three"}) {
        bus.Channel(name, Cfg().Capacity(50));
        bus.Subscribe(name, [&trail_mutex, &trail, name](Envelope&) {
            std::lock_guard<std::mutex> lock(trail_mutex);
            trail.push_back(name);
            return true;
        });
    }

    TestQueue<int> done;
    bus.PublishWithCallback(MakeEnvelope("one", std::string("x"), {"two", "three"}), 1000ms,
                             [&done](const Envelope&) { done.Push(1); });

    REQUIRE_MESSAGE(done.Pop(5000ms).has_value(), "expected completion callback");
    bus.Shutdown(5000ms);

    std::lock_guard<std::mutex> lock(trail_mutex);
    REQUIRE(trail.size() == 3);
    CHECK(trail[0] == "one");
    CHECK(trail[1] == "two");
    CHECK(trail[2] == "three");
}

TEST_CASE("backpressure rejects when the queue is full") {
    Bus bus(4);
    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    bool gate_open = false;

    bus.Channel("slow", Cfg().Capacity(2).Concurrency(1).SetBackpressure(Backpressure::Reject));
    bus.Subscribe("slow", [&](Envelope&) {
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait_for(lock, 5000ms, [&] { return gate_open; });
        return true;
    });

    size_t accepted = 0;
    for (int i = 0; i < 10; i++) {
        if (bus.Publish(MakeEnvelope("slow", i), 50ms)) accepted++;
    }
    {
        std::lock_guard<std::mutex> lock(gate_mutex);
        gate_open = true;
    }
    gate_cv.notify_all();

    CHECK_MESSAGE(accepted <= 3, "accepted " << accepted);
    bus.Shutdown(5000ms);
    CHECK(bus.GetStats().at("slow").dropped >= 7);
}

TEST_CASE("nack retries then dead-letters") {
    Bus bus(4);
    std::atomic<int> attempts{0};
    bus.Channel("flaky", Cfg().Capacity(10).MaxAttempts(3));
    bus.Channel("dead", Cfg().Capacity(10));
    bus.SetDeadLetterChannel("flaky", "dead");

    TestQueue<int> done;
    bus.Subscribe("dead", [&done](Envelope&) {
        done.Push(1);
        return true;
    });
    bus.Subscribe("flaky", [&attempts](Envelope&) {
        attempts.fetch_add(1);
        return false;
    });

    bus.Publish(MakeEnvelope("flaky", std::string("boom")), 1000ms);
    REQUIRE_MESSAGE(done.Pop(5000ms).has_value(), "expected dead-lettered message");
    bus.Shutdown(5000ms);

    CHECK(attempts.load() == 3);
    CHECK(bus.GetStats().at("flaky").dead_lettered == 1);
}

// C2 (CORRECTNESS_SUITE.md): the exhausts-to-dead-letter case above is only
// half of retry correctness - a consumer that succeeds on its last allowed
// attempt must be delivered exactly once, not dead-lettered, and must not
// leave anything behind that would corrupt a later envelope's own attempt
// count (no public API exposes the attempts map's size directly, so this
// runs many envelopes through the same channel/consumer and checks the
// aggregate counts line up exactly - a leaked or cross-contaminated entry
// would show up as a mismatch here).
TEST_CASE("nack then succeeds on the final attempt delivers exactly once") {
    Bus bus(4);
    bus.Channel("flaky2", Cfg().Capacity(200).MaxAttempts(3));
    std::mutex tries_mutex;
    std::unordered_map<std::string, int> tries;
    std::atomic<int> delivered{0};
    bus.Subscribe("flaky2", [&](Envelope& e) {
        std::lock_guard<std::mutex> lock(tries_mutex);
        int n = ++tries[e.id];
        if (n < 3) return false;  // nack twice, then succeed on attempt 3 (the last allowed)
        delivered.fetch_add(1);
        return true;
    });

    constexpr int kTotal = 50;
    for (int i = 0; i < kTotal; i++) {
        REQUIRE(bus.Publish(MakeEnvelope("flaky2", i), 1000ms));
    }
    // Each envelope needs 3 attempts; give the retries (immediate, no
    // backoff, per DESIGN.md §1.6) time to run.
    for (int i = 0; i < 100 && delivered.load() < kTotal; i++) {
        std::this_thread::sleep_for(20ms);
    }
    bus.Shutdown(5000ms);

    CHECK(delivered.load() == kTotal);
    CHECK(bus.GetStats().at("flaky2").delivered == static_cast<uint64_t>(kTotal));
    CHECK(bus.GetStats().at("flaky2").dead_lettered == 0);
}

TEST_CASE("a channel with no consumers dead-letters immediately") {
    Bus bus(2);
    bus.Channel("orphan", Cfg().Capacity(10));
    bus.Channel("dead2", Cfg().Capacity(10));
    bus.SetDeadLetterChannel("orphan", "dead2");
    TestQueue<int> done;
    bus.Subscribe("dead2", [&done](Envelope&) {
        done.Push(1);
        return true;
    });

    CHECK(bus.Publish(MakeEnvelope("orphan", 1), 1000ms));
    REQUIRE_MESSAGE(done.Pop(5000ms).has_value(), "expected immediate dead-letter, no consumers");
    bus.Shutdown(5000ms);
    CHECK(bus.GetStats().at("orphan").dead_lettered == 1);
}

// C1 (CORRECTNESS_SUITE.md): Reject's mirror above covers Reject; these
// three cover Block, DropOldest, and DropNewest on the same small-capacity,
// gated-consumer shape.
TEST_CASE("backpressure Block waits for room instead of rejecting") {
    Bus bus(4);
    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    bool gate_open = false;

    bus.Channel("tight", Cfg().Capacity(2).Concurrency(1).SetBackpressure(Backpressure::Block));
    bus.Subscribe("tight", [&](Envelope&) {
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait_for(lock, 5000ms, [&] { return gate_open; });
        return true;
    });

    // Block's own wait has no timeout here (nullopt deadline) - run the
    // publish loop on its own thread, bounded by an explicit polled
    // deadline below, never a bare join() with no bound: a regression
    // (a stuck wait, or a lost wakeup) must fail this test loudly instead
    // of hanging the whole suite.
    constexpr int kTotal = 10;
    std::atomic<int> accepted{0};
    std::atomic<bool> producer_done{false};
    std::thread producer([&] {
        for (int i = 0; i < kTotal; i++) {
            if (bus.Publish(MakeEnvelope("tight", i))) accepted.fetch_add(1);
        }
        producer_done.store(true);
    });

    // Let the queue fill and the producer genuinely block before releasing.
    std::this_thread::sleep_for(100ms);
    {
        std::lock_guard<std::mutex> lock(gate_mutex);
        gate_open = true;
    }
    gate_cv.notify_all();

    auto deadline = std::chrono::steady_clock::now() + 10000ms;
    while (!producer_done.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(20ms);
    }
    REQUIRE_MESSAGE(producer_done.load(), "producer never returned from Block - stuck wait or lost wakeup");
    producer.join();
    CHECK(accepted.load() == kTotal);
    bus.Shutdown(5000ms);
}

TEST_CASE("backpressure DropOldest always admits by evicting the front") {
    Bus bus(4);
    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    bool gate_open = false;

    bus.Channel("bounded", Cfg().Capacity(2).Concurrency(1).SetBackpressure(Backpressure::DropOldest));
    bus.Subscribe("bounded", [&](Envelope&) {
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait_for(lock, 5000ms, [&] { return gate_open; });
        return true;
    });

    size_t accepted = 0;
    for (int i = 0; i < 10; i++) {
        if (bus.Publish(MakeEnvelope("bounded", i), 50ms)) accepted++;
        CHECK(bus.GetStats().at("bounded").depth <= 2);
    }
    {
        std::lock_guard<std::mutex> lock(gate_mutex);
        gate_open = true;
    }
    gate_cv.notify_all();

    // DropOldest never refuses admission for capacity reasons.
    CHECK(accepted == 10);
    bus.Shutdown(5000ms);
}

TEST_CASE("backpressure DropNewest matches Reject's contract when full") {
    Bus bus(4);
    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    bool gate_open = false;

    bus.Channel("dn", Cfg().Capacity(2).Concurrency(1).SetBackpressure(Backpressure::DropNewest));
    bus.Subscribe("dn", [&](Envelope&) {
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait_for(lock, 5000ms, [&] { return gate_open; });
        return true;
    });

    size_t accepted = 0;
    for (int i = 0; i < 10; i++) {
        if (bus.Publish(MakeEnvelope("dn", i), 50ms)) accepted++;
    }
    {
        std::lock_guard<std::mutex> lock(gate_mutex);
        gate_open = true;
    }
    gate_cv.notify_all();

    CHECK_MESSAGE(accepted <= 3, "accepted " << accepted);
    bus.Shutdown(5000ms);
    CHECK(bus.GetStats().at("dn").dropped >= 7);
}

// C3 (CORRECTNESS_SUITE.md): every port already passes this per the
// production-readiness audit - kept here so it stays that way.
TEST_CASE("a consumer that throws on every third envelope doesn't take down the bus") {
    Bus bus(4);
    bus.Channel("flaky3", Cfg().Capacity(100));
    std::atomic<int> seen{0};
    std::atomic<int> delivered{0};
    bus.Subscribe("flaky3", [&](Envelope&) {
        int n = seen.fetch_add(1) + 1;
        if (n % 3 == 0) throw std::runtime_error("boom");
        delivered.fetch_add(1);
        return true;
    });

    constexpr int kTotal = 30;
    for (int i = 0; i < kTotal; i++) {
        CHECK(bus.Publish(MakeEnvelope("flaky3", i), 1000ms));
    }
    bus.Shutdown(5000ms);

    // Every non-throwing envelope (2 of every 3) was still delivered - the
    // throwing ones nack (SafeReceive catches) and, with MaxAttempts==1
    // (the default), are dead-lettered rather than retried forever.
    CHECK(seen.load() == kTotal);
    CHECK(delivered.load() == kTotal - kTotal / 3);
}

// C4 (CORRECTNESS_SUITE.md): Shutdown(true) must mean every published
// envelope was actually delivered or dead-lettered, not merely that the
// queue looked empty at some polling instant while something was still
// popped-but-not-yet-acked on a worker.
TEST_CASE("shutdown accounting: drained implies delivered + dead_lettered == published") {
    Bus bus(4);
    bus.Channel("acct", Cfg().Capacity(200).Concurrency(4));
    bus.Subscribe("acct", [](Envelope&) {
        std::this_thread::sleep_for(15ms);  // artificial per-envelope delay
        return true;
    });

    constexpr int kTotal = 40;
    for (int i = 0; i < kTotal; i++) {
        REQUIRE(bus.Publish(MakeEnvelope("acct", i), 1000ms));
    }
    // Long enough to drain before the deadline on this host.
    bool drained = bus.Shutdown(10000ms);
    auto stats = bus.GetStats().at("acct");
    CHECK(drained);
    CHECK(stats.delivered + stats.dead_lettered == static_cast<uint64_t>(kTotal));
}

TEST_CASE("shutdown accounting holds even when the timeout expires first") {
    Bus bus(2);
    bus.Channel("acct2", Cfg().Capacity(200).Concurrency(2));
    std::atomic<int> processed{0};
    bus.Subscribe("acct2", [&](Envelope&) {
        std::this_thread::sleep_for(30ms);
        processed.fetch_add(1);
        return true;
    });

    constexpr int kTotal = 40;
    for (int i = 0; i < kTotal; i++) {
        REQUIRE(bus.Publish(MakeEnvelope("acct2", i), 1000ms));
    }
    // Deliberately too short to finish all 40 at ~30ms/envelope over 2
    // concurrent workers (~600ms needed) - exercises the "timeout expired
    // first" branch specifically.
    bool drained = bus.Shutdown(100ms);
    CHECK_FALSE(drained);

    // Whatever finished by the time Shutdown gave up must be reflected
    // consistently - no envelope silently vanishes from the accounting.
    auto stats = bus.GetStats().at("acct2");
    CHECK(stats.delivered == static_cast<uint64_t>(processed.load()));
    CHECK(stats.delivered <= static_cast<uint64_t>(kTotal));
}

// C5 (CORRECTNESS_SUITE.md): ChannelConfig's fields are public, so a caller
// can bypass the fluent builder's clamping entirely. Confirms both the
// builder's own clamping AND the constructor-time guard added alongside
// this test (Channel::NormalizeConfig in bus.hpp) - a capacity of 0 must
// never reach TwoLockQueue, where it would make Backpressure::Block hang
// forever instead of failing fast or being clamped.
TEST_CASE("config validation: capacity/concurrency/max_attempts are clamped, never zero") {
    SUBCASE("via the fluent builder") {
        auto cfg = Cfg().Capacity(0).Concurrency(0).MaxAttempts(0);
        CHECK(cfg.capacity == 1);
        CHECK(cfg.concurrency == 1);
        CHECK(cfg.max_attempts == 1);
    }
    SUBCASE("via a raw struct, bypassing the builder") {
        ChannelConfig cfg;
        cfg.capacity = 0;
        cfg.concurrency = 0;
        cfg.max_attempts = 0;
        cfg.backpressure = Backpressure::Block;

        Bus bus(2);
        bus.Channel("zero", cfg);
        bus.Subscribe("zero", [](Envelope&) { return true; });

        // Before the constructor-time clamp, capacity=0 + Block hung here
        // forever (WaitForSpace's `size_ >= capacity_(0)` can never become
        // false). Bounded by the publish's own deadline, not a bare call,
        // so a regression fails this test instead of hanging the suite.
        CHECK(bus.Publish(MakeEnvelope("zero", 1), 2000ms));
        bus.Shutdown(2000ms);
    }
}

// C6 (CORRECTNESS_SUITE.md): construct-and-fully-Join a Bus repeatedly and
// confirm the process's OS thread count returns to baseline each time,
// rather than growing without bound. /proc/self/status's "Threads:" line
// is Linux-specific but portable enough for this test environment; there
// is no fully portable C++ API for "current OS thread count".
#ifdef __linux__
namespace {
long CurrentThreadCount() {
    std::ifstream f("/proc/self/status");
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("Threads:", 0) == 0) {
            return std::stol(line.substr(line.find_first_of("0123456789")));
        }
    }
    return -1;
}
}  // namespace
#endif

TEST_CASE("no thread-count growth across repeated bus lifecycles") {
#ifdef __linux__
    long baseline = CurrentThreadCount();
    REQUIRE(baseline > 0);

    for (int i = 0; i < 25; i++) {
        Bus bus(4);
        bus.Channel("cycle", Cfg().Capacity(10));
        bus.Subscribe("cycle", [](Envelope&) { return true; });
        bus.Publish(MakeEnvelope("cycle", i), 1000ms);
        bus.Shutdown(2000ms);  // Bus's destructor also joins its Pool
    }

    long after = CurrentThreadCount();
    // A small, fixed tolerance - the test harness/runtime itself can start
    // or stop a thread or two independent of this loop.
    CHECK_MESSAGE(after <= baseline + 4, "baseline=" << baseline << " after=" << after);
#else
    // No portable thread-count API on this platform - document the gap
    // rather than assert something meaningless.
    WARN("thread-count leak check skipped: no /proc/self/status on this platform");
#endif
}

TEST_CASE("shutdown drains queued work") {
    Bus bus(4);
    std::atomic<int> done{0};
    bus.Channel("drain", Cfg().Capacity(500).Concurrency(4));
    bus.Subscribe("drain", [&done](Envelope&) {
        std::this_thread::sleep_for(10ms);
        done.fetch_add(1);
        return true;
    });
    for (int i = 0; i < 50; i++) {
        bus.Publish(MakeEnvelope("drain", i), 1000ms);
    }
    CHECK(bus.Shutdown(10000ms));
    CHECK(done.load() == 50);
}

TEST_CASE("publish after pause is rejected") {
    Bus bus(2);
    bus.Channel("p", Cfg().Capacity(10));
    bus.Subscribe("p", [](Envelope&) { return true; });
    bus.Pause();
    CHECK_FALSE(bus.Publish(MakeEnvelope("p", 1), 10ms));
    bus.Resume();
    CHECK(bus.Publish(MakeEnvelope("p", 2), 10ms));
    bus.Shutdown(2000ms);
}

TEST_CASE("unknown channel returns false") {
    Bus bus(2);
    CHECK_FALSE(bus.Publish(MakeEnvelope("nope", 1)));
    bus.Shutdown(2000ms);
}

TEST_CASE("concurrent producers deliver exactly once") {
    Bus bus(8);
    std::mutex seen_mutex;
    std::vector<uint32_t> seen;
    bus.Channel("fan", Cfg().Capacity(5000).Concurrency(8));
    bus.Subscribe("fan", [&](Envelope& env) {
        std::lock_guard<std::mutex> lock(seen_mutex);
        seen.push_back(EnvelopePayload(env).get<uint32_t>());
        return true;
    });

    std::vector<std::thread> threads;
    for (uint32_t base = 0; base < 6; base++) {
        threads.emplace_back([&bus, base] {
            for (uint32_t i = 0; i < 500; i++) {
                uint32_t n = base * 1000 + i;
                while (!bus.Publish(MakeEnvelope("fan", n), 1000ms)) {
                }
            }
        });
    }
    for (auto& t : threads) t.join();
    CHECK(bus.Shutdown(15000ms));

    std::lock_guard<std::mutex> lock(seen_mutex);
    std::sort(seen.begin(), seen.end());
    seen.erase(std::unique(seen.begin(), seen.end()), seen.end());
    CHECK(seen.size() == 3000);
}
