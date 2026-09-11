// A three-stage pipeline: ingest -> transform -> sink, via a routing slip.
//
//   cmake --build build --target pipeline && ./build/examples/pipeline

#include <algorithm>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "seda_bus/seda_bus.hpp"

using namespace ra::seda_bus;
using namespace std::chrono_literals;

namespace {
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
}  // namespace

int main() {
    Bus bus(4);

    bus.Channel("ingest", ChannelConfig{}.Capacity(100));
    bus.Channel("transform", ChannelConfig{}.Capacity(100).Concurrency(2));
    bus.Channel("sink", ChannelConfig{}.Capacity(100));

    bus.Subscribe("ingest", [](Envelope& e) {
        e.SetHeader("seen_by", "ingest");
        return true;
    });
    bus.Subscribe("transform", [](Envelope& e) {
        std::string s = EnvelopePayload(e).get<std::string>();
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::toupper(c); });
        SetPayload(e, s);
        return true;
    });

    TestQueue<std::string> results;
    bus.Subscribe("sink", [&results](Envelope& e) {
        results.Push(EnvelopePayload(e).get<std::string>());
        return true;
    });

    for (const std::string& word : std::vector<std::string>{"alpha", "bravo", "charlie", "delta", "echo"}) {
        bus.Publish(MakeEnvelope("ingest", word, {"transform", "sink"}), 1000ms);
    }

    std::vector<std::string> out;
    for (int i = 0; i < 5; i++) {
        if (auto v = results.Pop(5000ms)) out.push_back(*v);
    }
    std::sort(out.begin(), out.end());

    std::cout << "sink saw: ";
    for (const auto& s : out) std::cout << s << " ";
    std::cout << "\n";

    bus.Shutdown(5000ms);
    for (const auto& [name, s] : bus.GetStats()) {
        std::cout << "  " << name << " depth=" << s.depth << " enqueued=" << s.enqueued
                  << " delivered=" << s.delivered << " nacked=" << s.nacked << " dropped=" << s.dropped
                  << " dead_lettered=" << s.dead_lettered << "\n";
    }
}
