#pragma once

// A small, broker-less, staged message bus.
//
// Work is decomposed into stages (Bus::Channel) connected by bounded queues.
// One shared thread pool drains every stage; each stage has its own
// concurrency limit so none can monopolise the pool.
//
// The envelope is ra::common::Envelope (aliased as ra::seda_bus::Envelope),
// the same wrapper seda-bus-java/-python/-ts carry via their ra-common ports.
//
//   ra::seda_bus::Bus bus(4);  // 4 shared worker threads
//   bus.Channel("upper", ra::seda_bus::ChannelConfig{}.Capacity(64));
//   bus.Subscribe("upper", [](ra::seda_bus::Envelope& e) {
//       auto s = ra::seda_bus::EnvelopePayload(e).get<std::string>();
//       std::transform(s.begin(), s.end(), s.begin(), ::toupper);
//       ra::seda_bus::SetPayload(e, s);
//       return true;
//   });
//   bus.Publish(ra::seda_bus::MakeEnvelope("upper", std::string("hello")),
//               std::chrono::seconds(1));
//   bus.Shutdown(std::chrono::seconds(2));
//
// What this is not: SEDA's original design also included a controller that
// watched per-stage latency and queue depth at runtime and re-tuned thread
// allocation and shed load automatically. That adaptive controller is future
// work; this is the static-configuration core it would build on.

#include "seda_bus/bus.hpp"
#include "seda_bus/envelope.hpp"
