#include "scheduler/ReadyBarrier.hpp"
#include "scheduler/SpscRing.hpp"

#include "core/Events.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

#if defined(__clang__)
constexpr auto compiler = "Clang " __clang_version__;
#elif defined(__GNUC__)
constexpr auto compiler = "GCC " __VERSION__;
#else
constexpr auto compiler = "unknown";
#endif

#if defined(__APPLE__)
constexpr auto operating_system = "macOS";
#elif defined(__linux__)
constexpr auto operating_system = "Linux";
#elif defined(_WIN32)
constexpr auto operating_system = "Windows";
#else
constexpr auto operating_system = "unknown";
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
constexpr auto architecture = "arm64";
#elif defined(__x86_64__) || defined(_M_X64)
constexpr auto architecture = "x86_64";
#else
constexpr auto architecture = "unknown";
#endif

std::int64_t percentile(const std::vector<std::int64_t> &sorted,
                        double fraction) {
  const auto index = static_cast<std::size_t>(
      fraction * static_cast<double>(sorted.size() - 1));
  return sorted[index];
}

} // namespace

int main() {
  constexpr std::size_t ring_capacity = 1;
  constexpr cmf::Sequence warmup_iterations = 10'000;
  constexpr cmf::Sequence measured_iterations = 100'000;

  cmf::scheduler::SpscRing<cmf::ScheduledEvent> events(ring_capacity);
  cmf::scheduler::ReadyBarrier ready;
  std::thread consumer([&] {
    cmf::ScheduledEvent event{cmf::MarketDelivery{}};
    while (events.pop_wait(event)) {
      if (!ready.publish_processed(event.dispatch_sequence())) {
        return;
      }
    }
  });

  const cmf::MarketDelivery delivery{1, 0, 0, 1, {}, {}};
  for (cmf::Sequence sequence = 1; sequence <= warmup_iterations; ++sequence) {
    if (!events.push_wait(cmf::ScheduledEvent{delivery, sequence}) ||
        !ready.wait_until(sequence)) {
      return 1;
    }
  }

  std::vector<std::int64_t> samples;
  samples.reserve(measured_iterations);
  for (cmf::Sequence index = 1; index <= measured_iterations; ++index) {
    const auto sequence = warmup_iterations + index;
    const auto start = Clock::now();
    if (!events.push_wait(cmf::ScheduledEvent{delivery, sequence}) ||
        !ready.wait_until(sequence)) {
      return 1;
    }
    const auto end = Clock::now();
    samples.push_back(
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count());
  }

  events.close();
  consumer.join();

  std::sort(samples.begin(), samples.end());
  const auto total =
      std::accumulate(samples.begin(), samples.end(), std::int64_t{0});
  const auto mean = total / static_cast<std::int64_t>(samples.size());

  std::cout << "ready-signal round trip (nanoseconds)\n"
            << "build_type=" << BACKTESTER_BENCHMARK_BUILD_TYPE
            << " compiler=" << compiler << " os=" << operating_system
            << " arch=" << architecture << '\n'
            << "ring_capacity=" << ring_capacity
            << " wait_strategy=atomic_epoch_wait"
            << " warmup=" << warmup_iterations
            << " measured=" << measured_iterations << '\n'
            << "min=" << samples.front() << " p50=" << percentile(samples, 0.50)
            << " p95=" << percentile(samples, 0.95)
            << " p99=" << percentile(samples, 0.99) << " mean=" << mean << '\n';
}
