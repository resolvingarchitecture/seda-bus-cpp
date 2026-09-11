#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "nlohmann/json.hpp"
#include "ra_common/envelope.hpp"

// The bus carries ra::common::Envelope (the same wrapper seda-bus-java uses
// via ra-common-java, and seda-bus-python/seda-bus-ts use via their own
// ra-common ports as of their 0.2.0 rewire). Routing is driven by the
// envelope's DynamicRoutingSlip: each hop targets route->service(); the slip
// is walked one hop at a time with Envelope::Ratchet().
//
// These helpers keep the ergonomic MakeEnvelope(to, payload, slip)/
// TargetService shape from earlier seda-bus-cpp versions on top of the
// richer ra-common type.

namespace ra::seda_bus {

using Envelope = ra::common::Envelope;

namespace detail {
// seda-bus routes by service, not operation; ra-common still wants a value
// there. Stamped on every route seda-bus creates.
inline constexpr const char* kOp = "RECEIVE";
}  // namespace detail

// Build a document envelope addressed to channel `to`, then visiting each
// name in `slip` in order.
inline Envelope MakeEnvelope(const std::string& to, nlohmann::json payload = nullptr,
                              std::vector<std::string> slip = {}, std::optional<std::string> sender = std::nullopt,
                              const std::unordered_map<std::string, std::string>& headers = {}) {
    Envelope env = Envelope::Document();
    // ra-common slips are LIFO: push the itinerary tail-first, then `to`
    // last, so Ratchet()/GetRoute() yields `to`, then slip[0], slip[1], ...
    for (auto it = slip.rbegin(); it != slip.rend(); ++it) {
        env.AddRoute(*it, detail::kOp);
    }
    env.AddRoute(to, detail::kOp);
    if (!payload.is_null()) env.AddContent(std::move(payload));
    if (sender) env.client = *sender;
    for (const auto& [k, v] : headers) env.SetHeader(k, v);
    return env;
}

// The channel name the envelope is currently headed for.
inline std::optional<std::string> TargetService(Envelope& env) {
    auto* route = env.GetRoute();
    return route != nullptr ? route->service() : std::nullopt;
}

// The document CONTENT value (what MakeEnvelope stored).
inline nlohmann::json EnvelopePayload(const Envelope& env) { return env.Content(); }

inline void SetPayload(Envelope& env, nlohmann::json payload) { env.AddContent(std::move(payload)); }

}  // namespace ra::seda_bus
