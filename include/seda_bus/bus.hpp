#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "seda_bus/envelope.hpp"
#include "seda_bus/pool.hpp"

// The bus: a registry of stages drained by one shared worker pool.

namespace ra::seda_bus {

// Envelopes a single drain task handles before releasing its permit and
// rescheduling. Amortises scheduling cost without starving other stages.
inline constexpr size_t kBatch = 16;

enum class Delivery {
    // One consumer handles each envelope (round-robin across consumers).
    PointToPoint,
    // Every consumer handles every envelope.
    PubSub,
};

enum class Backpressure {
    // Producer blocks (up to the publish timeout) until there is room.
    Block,
    // Publish() returns false immediately when the stage queue is full.
    Reject,
    // Silently discard the envelope being offered.
    DropNewest,
    // Evict the oldest queued envelope to make room.
    DropOldest,
};

// A consumer handles envelopes for a stage. Return true to ack, false to
// nack (the envelope is retried up to the stage's max_attempts, then
// dead-lettered). Throwing is treated as a nack.
using Consumer = std::function<bool(Envelope&)>;

// Invoked once an envelope finishes its itinerary (every routing-slip hop
// acked).
using CompleteCallback = std::function<void(const Envelope&)>;

struct ChannelConfig {
    size_t capacity = 1024;
    size_t concurrency = 1;
    Delivery delivery = Delivery::PointToPoint;
    Backpressure backpressure = Backpressure::Block;
    uint32_t max_attempts = 1;

    ChannelConfig Capacity(size_t n) const {
        ChannelConfig c = *this;
        c.capacity = std::max<size_t>(1, n);
        return c;
    }
    ChannelConfig Concurrency(size_t n) const {
        ChannelConfig c = *this;
        c.concurrency = std::max<size_t>(1, n);
        return c;
    }
    ChannelConfig SetDelivery(Delivery d) const {
        ChannelConfig c = *this;
        c.delivery = d;
        return c;
    }
    ChannelConfig SetBackpressure(Backpressure b) const {
        ChannelConfig c = *this;
        c.backpressure = b;
        return c;
    }
    ChannelConfig MaxAttempts(uint32_t n) const {
        ChannelConfig c = *this;
        c.max_attempts = std::max<uint32_t>(1, n);
        return c;
    }
};

struct Stats {
    size_t depth = 0;
    uint64_t enqueued = 0;
    uint64_t delivered = 0;
    uint64_t nacked = 0;
    uint64_t dropped = 0;
    uint64_t dead_lettered = 0;
};

namespace detail {

inline void Warn(const std::string& msg) { std::cerr << "[seda_bus] " << msg << "\n"; }

class Channel {
public:
    Channel(std::string name, ChannelConfig cfg)
        : name_(std::move(name)), cfg_(cfg), permits_(cfg.concurrency) {}

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    const std::string& Name() const { return name_; }
    const ChannelConfig& Config() const { return cfg_; }

    size_t Depth() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    // Admit an envelope, honouring the stage's back-pressure policy. `deadline`
    // is absent for an unbounded wait under Block, or a point in time after
    // which admission gives up.
    bool Offer(Envelope env, std::optional<std::chrono::steady_clock::time_point> deadline) {
        std::unique_lock<std::mutex> lock(mutex_);
        while (queue_.size() >= cfg_.capacity) {
            if (cfg_.backpressure == Backpressure::Reject || cfg_.backpressure == Backpressure::DropNewest) {
                dropped_.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            if (cfg_.backpressure == Backpressure::DropOldest) {
                queue_.pop_front();
                dropped_.fetch_add(1, std::memory_order_relaxed);
                break;
            }
            // Block.
            if (!deadline) {
                not_full_.wait(lock);
            } else {
                auto now = std::chrono::steady_clock::now();
                if (now >= *deadline) {
                    dropped_.fetch_add(1, std::memory_order_relaxed);
                    return false;
                }
                not_full_.wait_until(lock, *deadline);
            }
        }
        queue_.push_back(std::move(env));
        enqueued_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    std::optional<Envelope> Poll() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) return std::nullopt;
        Envelope env = std::move(queue_.front());
        queue_.pop_front();
        not_full_.notify_one();
        return env;
    }

    void Requeue(Envelope env) {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push_front(std::move(env));
    }

    bool TryAcquire() {
        size_t cur = permits_.load(std::memory_order_acquire);
        while (cur != 0) {
            if (permits_.compare_exchange_weak(cur, cur - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return true;
            }
        }
        return false;
    }

    void Release() { permits_.fetch_add(1, std::memory_order_acq_rel); }

    void AddConsumer(Consumer c) {
        std::unique_lock<std::shared_mutex> lock(consumers_mutex_);
        consumers_.push_back(std::move(c));
    }

    std::vector<Consumer> SnapshotConsumers() const {
        std::shared_lock<std::shared_mutex> lock(consumers_mutex_);
        return consumers_;
    }

    size_t NextRoundRobin(size_t n) { return rr_.fetch_add(1, std::memory_order_relaxed) % n; }

    void RecordDelivered() { delivered_.fetch_add(1, std::memory_order_relaxed); }
    void RecordNacked() { nacked_.fetch_add(1, std::memory_order_relaxed); }
    void RecordDeadLettered() { dead_lettered_.fetch_add(1, std::memory_order_relaxed); }

    // Per-hop delivery attempts, keyed by envelope id (mirrors
    // SEDAMessageChannel.attempts in seda-bus-java / Channel._attempts in
    // seda-bus-python). Lives on the channel, not the envelope, because
    // ra::common::Envelope has no attempts field of its own.
    uint32_t BumpAttempt(const std::string& id) {
        std::lock_guard<std::mutex> lock(attempts_mutex_);
        return ++attempts_[id];
    }
    void ClearAttempt(const std::string& id) {
        std::lock_guard<std::mutex> lock(attempts_mutex_);
        attempts_.erase(id);
    }

    Stats StatsSnapshot() const {
        return Stats{
            Depth(),
            enqueued_.load(std::memory_order_relaxed),
            delivered_.load(std::memory_order_relaxed),
            nacked_.load(std::memory_order_relaxed),
            dropped_.load(std::memory_order_relaxed),
            dead_lettered_.load(std::memory_order_relaxed),
        };
    }

private:
    std::string name_;
    ChannelConfig cfg_;

    mutable std::mutex mutex_;
    std::condition_variable not_full_;
    std::deque<Envelope> queue_;

    mutable std::shared_mutex consumers_mutex_;
    std::vector<Consumer> consumers_;

    std::atomic<size_t> rr_{0};
    std::atomic<size_t> permits_;

    std::mutex attempts_mutex_;
    std::unordered_map<std::string, uint32_t> attempts_;

    std::atomic<uint64_t> enqueued_{0};
    std::atomic<uint64_t> delivered_{0};
    std::atomic<uint64_t> nacked_{0};
    std::atomic<uint64_t> dropped_{0};
    std::atomic<uint64_t> dead_lettered_{0};
};

inline bool SafeReceive(const Consumer& c, Envelope& env) {
    try {
        return c(env);
    } catch (const std::exception& e) {
        Warn("consumer threw handling " + env.id + ": " + e.what());
        return false;
    } catch (...) {
        Warn("consumer threw handling " + env.id);
        return false;
    }
}

// Owns every bit of bus state. Held by Bus via a shared_ptr so Bus is cheap
// to copy (every copy shares the same stages, pool, and callbacks) and so
// pool jobs can keep it alive across a drain without keeping the whole Bus
// object alive.
class Impl : public std::enable_shared_from_this<Impl> {
public:
    explicit Impl(size_t workers) : pool_(workers == 0 ? DefaultWorkerCount() : workers) {}

    size_t WorkerCount() const { return pool_.Size(); }

    // Register a stage. Re-registering a name is a no-op.
    void EnsureChannel(const std::string& name, ChannelConfig cfg) {
        std::unique_lock<std::shared_mutex> lock(channels_mutex_);
        if (channels_.find(name) == channels_.end()) {
            channels_.emplace(name, std::make_shared<Channel>(name, cfg));
        }
    }

    // Attach a consumer to a stage. Creates the stage with defaults if needed.
    void Subscribe(const std::string& name, Consumer consumer) {
        auto ch = GetOrCreate(name, ChannelConfig{});
        ch->AddConsumer(std::move(consumer));
    }

    // Route dead letters from `source` to the channel named `dlq`.
    void SetDeadLetterChannel(const std::string& source, const std::string& dlq) {
        GetOrCreate(dlq, ChannelConfig{}.Capacity(4096).SetBackpressure(Backpressure::DropOldest));
        std::lock_guard<std::mutex> lock(dlq_mutex_);
        dlq_[source] = dlq;
    }

    std::unordered_map<std::string, Stats> GetStats() const {
        std::shared_lock<std::shared_mutex> lock(channels_mutex_);
        std::unordered_map<std::string, Stats> out;
        for (const auto& [name, ch] : channels_) out.emplace(name, ch->StatsSnapshot());
        return out;
    }

    // Publish an envelope to the channel named by its current route
    // (TargetService(env) — env.GetRoute()->service()).
    bool Publish(Envelope env, std::optional<std::chrono::milliseconds> timeout) {
        if (!running_.load(std::memory_order_acquire) || !accepting_.load(std::memory_order_acquire)) {
            return false;
        }
        auto target = TargetService(env);
        if (!target) {
            Warn("envelope has no current route; dropping envelope " + env.id);
            return false;
        }
        std::shared_ptr<Channel> ch;
        {
            std::shared_lock<std::shared_mutex> lock(channels_mutex_);
            auto it = channels_.find(*target);
            if (it == channels_.end()) {
                Warn("no channel '" + *target + "'; dropping envelope " + env.id);
                return false;
            }
            ch = it->second;
        }
        std::optional<std::chrono::steady_clock::time_point> deadline;
        if (timeout) deadline = std::chrono::steady_clock::now() + *timeout;
        if (!ch->Offer(std::move(env), deadline)) return false;
        Schedule(ch);
        return true;
    }

    // Publish and invoke on_complete once the envelope finishes its
    // itinerary (all routing-slip hops acked).
    bool PublishWithCallback(Envelope env, std::optional<std::chrono::milliseconds> timeout,
                              CompleteCallback on_complete) {
        std::string id = env.id;
        {
            std::lock_guard<std::mutex> lock(callbacks_mutex_);
            callbacks_[id] = std::move(on_complete);
        }
        bool ok = Publish(std::move(env), timeout);
        if (!ok) {
            std::lock_guard<std::mutex> lock(callbacks_mutex_);
            callbacks_.erase(id);
        }
        return ok;
    }

    void Pause() { accepting_.store(false, std::memory_order_release); }

    void Resume() {
        if (running_.load(std::memory_order_acquire)) accepting_.store(true, std::memory_order_release);
    }

    // Stop accepting, drain queued work (up to timeout), then stop the pool.
    // Returns true if everything drained.
    bool Shutdown(std::chrono::milliseconds timeout) {
        accepting_.store(false, std::memory_order_release);
        bool drained = AwaitDrain(timeout);
        running_.store(false, std::memory_order_release);
        pool_.Join();
        return drained;
    }

    // Stop accepting and stop the pool without waiting for queues to drain.
    void ShutdownNow() {
        accepting_.store(false, std::memory_order_release);
        running_.store(false, std::memory_order_release);
        pool_.Join();
    }

private:
    std::shared_ptr<Channel> GetOrCreate(const std::string& name, ChannelConfig cfg) {
        std::unique_lock<std::shared_mutex> lock(channels_mutex_);
        auto it = channels_.find(name);
        if (it != channels_.end()) return it->second;
        auto ch = std::make_shared<Channel>(name, cfg);
        channels_.emplace(name, ch);
        return ch;
    }

    void Schedule(const std::shared_ptr<Channel>& ch) {
        auto self = shared_from_this();
        while (ch->Depth() > 0 && ch->TryAcquire()) {
            bool ok = pool_.Execute([self, ch] { self->Drain(ch); });
            if (!ok) {
                ch->Release();
                return;
            }
        }
    }

    void Drain(const std::shared_ptr<Channel>& ch) {
        for (size_t i = 0; i < kBatch; i++) {
            if (!running_.load(std::memory_order_acquire)) break;
            auto env = ch->Poll();
            if (!env) break;
            Process(ch, std::move(*env));
        }
        ch->Release();
        if (running_.load(std::memory_order_acquire)) Schedule(ch);
    }

    void Process(const std::shared_ptr<Channel>& ch, Envelope env) {
        auto consumers = ch->SnapshotConsumers();
        if (consumers.empty()) {
            Warn("channel '" + ch->Name() + "' has no consumers; dead-lettering " + env.id);
            DeadLetter(ch, std::move(env));
            return;
        }

        uint32_t attempt = ch->BumpAttempt(env.id);
        bool ok;
        if (ch->Config().delivery == Delivery::PubSub) {
            ok = true;
            for (const auto& c : consumers) ok = SafeReceive(c, env) && ok;
        } else {
            size_t idx = ch->NextRoundRobin(consumers.size());
            ok = SafeReceive(consumers[idx], env);
        }

        if (ok) {
            ch->RecordDelivered();
            ch->ClearAttempt(env.id);
            CompleteHop(std::move(env));
        } else if (attempt < ch->Config().max_attempts) {
            ch->RecordNacked();
            ch->Requeue(std::move(env));
        } else {
            ch->RecordNacked();
            ch->ClearAttempt(env.id);
            DeadLetter(ch, std::move(env));
        }
    }

    void CompleteHop(Envelope env) {
        if (env.dynamic_routing_slip.PeekAtNextRoute() != nullptr) {
            env.Ratchet();
            Publish(std::move(env), std::chrono::milliseconds(5000));
            return;
        }
        CompleteCallback cb;
        {
            std::lock_guard<std::mutex> lock(callbacks_mutex_);
            auto it = callbacks_.find(env.id);
            if (it != callbacks_.end()) {
                cb = std::move(it->second);
                callbacks_.erase(it);
            }
        }
        if (cb) cb(env);
    }

    void DeadLetter(const std::shared_ptr<Channel>& ch, Envelope env) {
        ch->RecordDeadLettered();
        std::optional<std::string> dlq_name;
        {
            std::lock_guard<std::mutex> lock(dlq_mutex_);
            auto it = dlq_.find(ch->Name());
            if (it != dlq_.end()) dlq_name = it->second;
        }
        std::string id = env.id;
        if (dlq_name) {
            std::shared_ptr<Channel> dlq_ch;
            {
                std::shared_lock<std::shared_mutex> lock(channels_mutex_);
                auto it = channels_.find(*dlq_name);
                if (it != channels_.end()) dlq_ch = it->second;
            }
            if (dlq_ch) {
                dlq_ch->Offer(std::move(env), std::chrono::steady_clock::now());
                Schedule(dlq_ch);
            }
        }
        std::lock_guard<std::mutex> lock(callbacks_mutex_);
        callbacks_.erase(id);
    }

    bool AwaitDrain(std::chrono::milliseconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        for (;;) {
            bool empty = true;
            {
                std::shared_lock<std::shared_mutex> lock(channels_mutex_);
                for (const auto& [name, ch] : channels_) {
                    if (ch->Depth() != 0) {
                        empty = false;
                        break;
                    }
                }
            }
            if (empty) return true;
            if (std::chrono::steady_clock::now() >= deadline) return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }

    static size_t DefaultWorkerCount() {
        unsigned n = std::thread::hardware_concurrency();
        return n == 0 ? 4 : n;
    }

    mutable std::shared_mutex channels_mutex_;
    std::unordered_map<std::string, std::shared_ptr<Channel>> channels_;

    std::mutex dlq_mutex_;
    std::unordered_map<std::string, std::string> dlq_;

    std::mutex callbacks_mutex_;
    std::unordered_map<std::string, CompleteCallback> callbacks_;

    Pool pool_;
    std::atomic<bool> running_{true};
    std::atomic<bool> accepting_{true};
};

}  // namespace detail

// A staged, broker-less message bus. Cheap to copy — every copy shares the
// same stages, worker pool, and pending callbacks (a shared_ptr<Impl> inside).
class Bus {
public:
    // Create and start a bus with `workers` shared threads (defaults to the
    // number of available cores when 0).
    explicit Bus(size_t workers = 0) : impl_(std::make_shared<detail::Impl>(workers)) {}

    size_t Workers() const { return impl_->WorkerCount(); }

    // Register a stage. Re-registering a name is a no-op.
    Bus& Channel(std::string name, ChannelConfig cfg = ChannelConfig{}) {
        impl_->EnsureChannel(name, cfg);
        return *this;
    }

    // Attach a consumer to a stage. Creates the stage with defaults if needed.
    Bus& Subscribe(std::string channel, Consumer consumer) {
        impl_->Subscribe(channel, std::move(consumer));
        return *this;
    }

    // Route dead letters from `source` to the channel named `dlq`.
    Bus& SetDeadLetterChannel(std::string source, std::string dlq) {
        impl_->SetDeadLetterChannel(source, dlq);
        return *this;
    }

    std::unordered_map<std::string, Stats> GetStats() const { return impl_->GetStats(); }

    // Publish an envelope to the channel named by env.to. Returns whether it
    // was accepted.
    bool Publish(Envelope env, std::optional<std::chrono::milliseconds> timeout = std::nullopt) {
        return impl_->Publish(std::move(env), timeout);
    }

    // Publish and invoke on_complete once the envelope finishes its
    // itinerary (all routing-slip hops acked).
    bool PublishWithCallback(Envelope env, std::optional<std::chrono::milliseconds> timeout,
                              CompleteCallback on_complete) {
        return impl_->PublishWithCallback(std::move(env), timeout, std::move(on_complete));
    }

    // Stop and resume accepting Publish() calls; in-flight work finishes
    // either way.
    void Pause() { impl_->Pause(); }
    void Resume() { impl_->Resume(); }

    // Stop accepting, wait until every queue is drained (or timeout elapses),
    // then stop the pool. Returns whether it drained fully.
    bool Shutdown(std::chrono::milliseconds timeout) { return impl_->Shutdown(timeout); }

    // Stop the pool without waiting.
    void ShutdownNow() { impl_->ShutdownNow(); }

private:
    std::shared_ptr<detail::Impl> impl_;
};

}  // namespace ra::seda_bus
