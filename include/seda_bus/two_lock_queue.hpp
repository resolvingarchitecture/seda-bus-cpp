#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdio>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

// A bounded, blocking MPMC FIFO queue using separate head/tail locks
// (Michael & Scott's classic two-lock queue shape, extended with a
// capacity bound) instead of one mutex guarding both ends - backed by a
// pre-allocated circular buffer, not a per-item linked-list allocation.
//
// Why two locks: `Channel` previously used one `std::mutex` for both
// `Offer` (push) and `Poll` (pop), so every producer and every consumer on
// a stage contended on the same lock regardless of which end they
// touched. Under `par` (8 producers + up to 8 concurrent consumers on one
// channel) this measured as a real, Docker-specific throughput collapse -
// see seda-bus-compare/RESULTS.md's "Mutex vs. lock-free" section, which
// named a two-lock queue as the lowest-risk improvement path over a full
// lock-free rewrite.
//
// Why a circular buffer, not the textbook linked-list two-lock queue: a
// first pass used `new`/`delete` per node (the textbook design). It fixed
// `par` but broke `seq` - Docker-verified: `seq`, previously the cleanest
// case in this whole report, developed the same bimodal cold-stall
// pattern other implementations pay for their allocator/OS interaction
// (seda-bus-compare/RESULTS.md's "Latency" section documents the same
// class of artifact elsewhere). Since this queue is already bounded by
// `capacity`, there is no need to allocate per item at all - a
// pre-allocated `capacity + 1`-slot ring, indices instead of pointers,
// removes the allocator from the hot path entirely while keeping the same
// two-lock algorithm and the same capacity-under-tail-lock correctness
// argument (a concurrent pop can only free a slot, never race a push into
// claiming one it shouldn't).
template <typename T>
class TwoLockQueue {
public:
    // `requeue_headroom` sizes the extra slots `PushFront` (Requeue) can use
    // beyond `capacity` without a capacity check of its own - matching the
    // original std::deque-backed Channel, which let Requeue grow the queue
    // unconditionally. Bounded by the channel's own `concurrency`: at most
    // that many envelopes can be popped-but-not-yet-acked (in flight, each
    // already removed from the queue by its own Poll) at once, so that's
    // the most that could all be requeued back "at the same time" without
    // any of it representing real, unbounded growth beyond what
    // `concurrency` already implies. Pass 0 if the caller never requeues
    // (e.g. max_attempts == 1, this benchmark's own default).
    TwoLockQueue(size_t capacity, size_t requeue_headroom)
        : capacity_(capacity), slots_(capacity + requeue_headroom + 1) {}

    TwoLockQueue(const TwoLockQueue&) = delete;
    TwoLockQueue& operator=(const TwoLockQueue&) = delete;

    size_t Size() const { return size_.load(std::memory_order_acquire); }

    enum class OnFull { Reject, DropOldest, Block };

    // `on_drop` is invoked (with the discarded value) whenever `DropOldest`
    // silently evicts an old entry to make room - the only case where a
    // caller-visible side effect happens without Push itself returning
    // false, so callers that track a "dropped" stat need the hook.
    template <typename ShouldGiveUp, typename OnDrop>
    bool Push(T value, OnFull on_full, ShouldGiveUp should_give_up, OnDrop on_drop) {
        std::unique_lock<std::mutex> tlock(tail_mutex_);
        while (size_.load(std::memory_order_acquire) >= capacity_) {
            switch (on_full) {
                case OnFull::Reject:
                    return false;
                case OnFull::DropOldest: {
                    // Rare path - take both locks (tail already held).
                    std::optional<T> evicted;
                    {
                        std::lock_guard<std::mutex> hlock(head_mutex_);
                        evicted = PopFrontLocked();
                    }
                    if (evicted) on_drop(std::move(*evicted));
                    break;
                }
                case OnFull::Block:
                    if (should_give_up()) return false;
                    tlock.unlock();
                    if (!WaitForSpace(should_give_up)) return false;
                    tlock.lock();
                    break;
            }
        }
        PushBackLocked(std::move(value));
        return true;
    }

    // No capacity check against `capacity_` - used by Requeue (nack retry),
    // which must never be dropped by the same policy governing fresh
    // admission. Rare path (only when max_attempts > 1) - takes both locks.
    //
    // Still guards the true physical bound (`slots_.size()`), found missing
    // by an independent production-readiness audit: `requeue_headroom` (see
    // the constructor) is sized to the channel's `concurrency`, on the
    // invariant that at most that many envelopes can be popped-but-not-yet-
    // acked at once. If that invariant is ever violated - a future caller
    // sizing headroom below its actual concurrency, say - blindly backing
    // `head_` up and overwriting slots_[head_] would silently corrupt a
    // still-live entry (stale data returned by a later Pop, and `size_` no
    // longer matching what's actually live) instead of failing loudly. If
    // genuinely full, evict the current oldest entry first - the same safe
    // drop Push's DropOldest path already uses - so the buffer stays
    // internally consistent: a dropped envelope and a loud stderr warning
    // beat corrupted queue state.
    void PushFront(T value) {
        std::lock_guard<std::mutex> tlock(tail_mutex_);
        std::lock_guard<std::mutex> hlock(head_mutex_);
        if (size_.load(std::memory_order_acquire) >= slots_.size()) {
            std::fprintf(stderr,
                "[seda_bus] TwoLockQueue::PushFront: no requeue headroom "
                "left (capacity=%zu, slots=%zu) - dropping the oldest entry "
                "instead of corrupting the buffer. requeue_headroom is "
                "undersized for this channel's actual concurrency.\n",
                capacity_, slots_.size());
            PopFrontLocked();
        }
        // head_ points one-before-the-front; back it up by one slot and
        // place the requeued value there, so it's the next Pop(). (When the
        // guard above just evicted the true front, this reclaims exactly
        // that freed slot: PopFrontLocked advanced head_ forward past it,
        // so Prev(head_) here lands back on it.)
        head_ = Prev(head_);
        slots_[head_] = std::move(value);
        size_.fetch_add(1, std::memory_order_acq_rel);
    }

    std::optional<T> Pop() {
        std::lock_guard<std::mutex> hlock(head_mutex_);
        auto v = PopFrontLocked();
        if (v) space_cv_.notify_one();
        return v;
    }

private:
    size_t Next(size_t i) const { return (i + 1) % slots_.size(); }
    size_t Prev(size_t i) const { return (i == 0 ? slots_.size() : i) - 1; }

    // Caller must hold tail_mutex_.
    void PushBackLocked(T value) {
        slots_[tail_] = std::move(value);
        tail_ = Next(tail_);
        size_.fetch_add(1, std::memory_order_acq_rel);
    }

    // Caller must hold head_mutex_.
    std::optional<T> PopFrontLocked() {
        if (size_.load(std::memory_order_acquire) == 0) return std::nullopt;
        std::optional<T> value = std::move(slots_[head_]);
        slots_[head_].reset();
        head_ = Next(head_);
        size_.fetch_sub(1, std::memory_order_acq_rel);
        return value;
    }

    template <typename ShouldGiveUp>
    bool WaitForSpace(ShouldGiveUp should_give_up) {
        std::unique_lock<std::mutex> wlock(wait_mutex_);
        while (size_.load(std::memory_order_acquire) >= capacity_) {
            if (should_give_up()) return false;
            space_cv_.wait_for(wlock, std::chrono::milliseconds(5));
        }
        return true;
    }

    size_t capacity_;
    std::vector<std::optional<T>> slots_;  // capacity_ + 1 pre-allocated slots
    size_t head_ = 0;                      // next slot to Pop
    size_t tail_ = 0;                      // next slot to Push
    std::atomic<size_t> size_{0};

    mutable std::mutex head_mutex_;
    std::mutex tail_mutex_;

    // Only touched by a blocked Push (rare) and every Pop's notify (cheap
    // when nobody is waiting - libstdc++/libc++ both fast-path an empty
    // waiter set without a syscall).
    std::mutex wait_mutex_;
    std::condition_variable space_cv_;
};
