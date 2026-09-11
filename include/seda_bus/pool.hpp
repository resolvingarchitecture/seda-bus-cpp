#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

// A minimal fixed-size thread pool shared by every stage of the bus.

namespace ra::seda_bus::detail {

class Pool {
public:
    explicit Pool(size_t size) : size_(size == 0 ? 1 : size) {
        workers_.reserve(size_);
        for (size_t i = 0; i < size_; i++) {
            workers_.emplace_back([this] { WorkerLoop(); });
        }
    }

    ~Pool() { Join(); }

    Pool(const Pool&) = delete;
    Pool& operator=(const Pool&) = delete;

    size_t Size() const { return size_; }

    // Submit work. Returns false if the pool has been shut down.
    bool Execute(std::function<void()> f) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopped_) return false;
            jobs_.push_back(std::move(f));
        }
        cv_.notify_one();
        return true;
    }

    // Stop accepting new work, let every queued job run, then join every
    // worker thread.
    void Join() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopped_) return;
            stopped_ = true;
        }
        cv_.notify_all();
        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
        workers_.clear();
    }

private:
    void WorkerLoop() {
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return stopped_ || !jobs_.empty(); });
                if (jobs_.empty()) {
                    if (stopped_) return;
                    continue;
                }
                job = std::move(jobs_.front());
                jobs_.pop_front();
            }
            // A throwing job must not take down the worker.
            try {
                job();
            } catch (...) {
            }
        }
    }

    size_t size_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> jobs_;
    bool stopped_ = false;
    std::vector<std::thread> workers_;
};

}  // namespace ra::seda_bus::detail
