#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
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
