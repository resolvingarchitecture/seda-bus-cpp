#include "doctest/doctest.h"
#include "seda_bus/envelope.hpp"

using namespace ra::seda_bus;

TEST_CASE("make_envelope gets a unique id and a current route of 'to'") {
    Envelope e = MakeEnvelope("work", 42);
    CHECK_FALSE(e.id.empty());
    REQUIRE(TargetService(e).has_value());
    CHECK(*TargetService(e) == "work");
    CHECK(EnvelopePayload(e) == 42);

    Envelope e2 = MakeEnvelope("work", 1);
    CHECK(e.id != e2.id);
}

TEST_CASE("payload accessors round-trip arbitrary JSON") {
    Envelope e = MakeEnvelope("ingest", std::string("hello"));
    CHECK(EnvelopePayload(e) == "hello");
    SetPayload(e, 7);
    CHECK(EnvelopePayload(e) == 7);
}

TEST_CASE("sender and headers are set on the envelope") {
    Envelope e = MakeEnvelope("ingest", nullptr, {}, std::string("producer-1"), {{"k", "v"}});
    REQUIRE(e.client.has_value());
    CHECK(*e.client == "producer-1");
    CHECK(e.Header("k") == "v");
}

TEST_CASE("slip is visited to, then each hop in order, via TargetService + Ratchet") {
    Envelope e = MakeEnvelope("one", nullptr, {"two", "three"});

    REQUIRE(TargetService(e).has_value());
    CHECK(*TargetService(e) == "one");

    REQUIRE(e.dynamic_routing_slip.PeekAtNextRoute() != nullptr);
    e.Ratchet();
    REQUIRE(TargetService(e).has_value());
    CHECK(*TargetService(e) == "two");

    REQUIRE(e.dynamic_routing_slip.PeekAtNextRoute() != nullptr);
    e.Ratchet();
    REQUIRE(TargetService(e).has_value());
    CHECK(*TargetService(e) == "three");

    CHECK(e.dynamic_routing_slip.PeekAtNextRoute() == nullptr);
}
