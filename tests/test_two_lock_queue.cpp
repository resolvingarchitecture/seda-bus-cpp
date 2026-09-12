#include <string>
#include <vector>

#include "doctest/doctest.h"
#include "seda_bus/two_lock_queue.hpp"

namespace {
bool NeverGiveUp() { return false; }
void NoDrop(std::string&&) {}
}  // namespace

TEST_CASE("PushFront evicts the oldest entry instead of corrupting state when headroom is exhausted") {
    // capacity=2, requeue_headroom=0 -> 3 physical slots (capacity +
    // headroom + 1). Exhausting the queue's one spare slot and then calling
    // PushFront again is the exact invariant violation an independent
    // production-readiness audit flagged as unguarded: a caller (or a
    // future config bug) requeuing more concurrently than
    // requeue_headroom actually accounts for.
    TwoLockQueue<std::string> q(/*capacity=*/2, /*requeue_headroom=*/0);
    using OnFull = TwoLockQueue<std::string>::OnFull;

    REQUIRE(q.Push("a", OnFull::Reject, NeverGiveUp, NoDrop));
    REQUIRE(q.Push("b", OnFull::Reject, NeverGiveUp, NoDrop));
    q.PushFront("retry-1");  // uses the queue's one spare slot - now physically full
    CHECK(q.Size() == 3);

    // No headroom left at all. Must not corrupt state - evicts the current
    // front ("retry-1") instead of silently overwriting live slot data.
    q.PushFront("retry-2");
    CHECK(q.Size() == 3);  // evicted one to make room; net size unchanged

    std::vector<std::string> drained;
    while (auto v = q.Pop()) drained.push_back(*v);
    CHECK(drained == std::vector<std::string>{"retry-2", "a", "b"});
}

TEST_CASE("PushFront within headroom never evicts") {
    TwoLockQueue<int> q(/*capacity=*/2, /*requeue_headroom=*/2);
    auto never_give_up = [] { return false; };
    auto no_drop = [](int&&) {};
    REQUIRE(q.Push(1, TwoLockQueue<int>::OnFull::Reject, never_give_up, no_drop));
    REQUIRE(q.Push(2, TwoLockQueue<int>::OnFull::Reject, never_give_up, no_drop));
    q.PushFront(10);
    q.PushFront(20);
    CHECK(q.Size() == 4);

    std::vector<int> drained;
    while (auto v = q.Pop()) drained.push_back(*v);
    CHECK(drained == std::vector<int>{20, 10, 1, 2});
}
