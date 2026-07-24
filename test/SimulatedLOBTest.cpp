#include "main/SimulatedLOB.hpp"

#include "MiniTest.hpp"

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {
MarketDataEvent makeAdd(const std::string &orderId, char side, double price,
                        long long size) {
  MarketDataEvent e;
  e.action = 'A';
  e.side = side;
  e.price = std::to_string(price);
  e.size = size;
  e.orderId = orderId;
  e.instrumentId = 1;
  return e;
}
} // namespace

TEST_CASE(
    "SimulatedLOB keeps independent engine-private historical consumption",
    "[SimulatedLOB]") {
  auto historical = std::make_shared<HistoricalLOB>();
  historical->applyEvent(makeAdd("ask-1", 'A', 101.0, 10));
  historical->applyEvent(makeAdd("bid-1", 'B', 100.0, 20));

  auto engineA = std::make_shared<EngineView>("engine-A");
  auto engineB = std::make_shared<EngineView>("engine-B");

  SimulatedLOB viewA(historical, engineA);
  SimulatedLOB viewB(historical, engineB);

  const auto fillA = viewA.placeLimitOrder("A-buy-1", 'B', 101.0, 4);
  REQUIRE(fillA.hasFill());
  REQUIRE(fillA.filledSize == 4);
  REQUIRE(fillA.restingSize == 0);

  REQUIRE(viewA.bestAsk().has_value());
  REQUIRE(*viewA.bestAsk() == 101.0);
  REQUIRE(viewA.bestAskSize() == 6);

  // Engine B still sees the full historical liquidity because Engine A's
  // market impact is private to Engine A's overlay.
  REQUIRE(viewB.bestAsk().has_value());
  REQUIRE(*viewB.bestAsk() == 101.0);
  REQUIRE(viewB.bestAskSize() == 10);

  // The shared basement is not mutated by synthetic fills.
  REQUIRE(historical->bestAskSize() == 10);
}

TEST_CASE("SimulatedLOB exposes only the current engine's synthetic orders",
          "[SimulatedLOB]") {
  auto historical = std::make_shared<HistoricalLOB>();
  historical->applyEvent(makeAdd("ask-1", 'A', 101.0, 10));
  historical->applyEvent(makeAdd("bid-1", 'B', 100.0, 20));

  auto engineA = std::make_shared<EngineView>("engine-A");
  auto engineB = std::make_shared<EngineView>("engine-B");

  SimulatedLOB viewA(historical, engineA);
  SimulatedLOB viewB(historical, engineB);

  const auto restA = viewA.placeLimitOrder("A-sell-passive", 'A', 102.0, 7);
  REQUIRE_FALSE(restA.hasFill());
  REQUIRE(restA.isResting());
  REQUIRE(engineA->liveOwnOrderCount() == 1);

  const BookSnapshot snapA = viewA.snapshot(5);
  const BookSnapshot snapB = viewB.snapshot(5);

  bool aSeesOwnAsk = false;
  for (const BookLevel &level : snapA.asks) {
    if (level.price == 102.0 && level.size == 7)
      aSeesOwnAsk = true;
  }

  bool bSeesEngineAAsk = false;
  for (const BookLevel &level : snapB.asks) {
    if (level.price == 102.0)
      bSeesEngineAAsk = true;
  }

  REQUIRE(aSeesOwnAsk);
  REQUIRE_FALSE(bSeesEngineAAsk);

  REQUIRE(viewA.cancelOwnOrder("A-sell-passive"));
  REQUIRE(engineA->liveOwnOrderCount() == 0);
}

TEST_CASE("Multiple EngineViews can trade concurrently on one HistoricalLOB",
          "[SimulatedLOB]") {
  auto historical = std::make_shared<HistoricalLOB>();
  historical->applyEvent(makeAdd("ask-1", 'A', 101.0, 100));
  historical->applyEvent(makeAdd("bid-1", 'B', 100.0, 100));

  constexpr int engineCount = 16;
  std::atomic<bool> allThreadsOk{true};
  std::vector<std::shared_ptr<EngineView>> engines;
  std::vector<std::thread> threads;
  engines.reserve(engineCount);

  for (int i = 0; i < engineCount; ++i)
    engines.push_back(
        std::make_shared<EngineView>("engine-" + std::to_string(i)));

  for (int i = 0; i < engineCount; ++i) {
    threads.emplace_back([historical,
                          view = engines[static_cast<std::size_t>(i)], i,
                          &allThreadsOk]() {
      SimulatedLOB lob(historical, view);
      const auto result =
          lob.placeLimitOrder("buy-" + std::to_string(i), 'B', 101.0, 1);
      if (result.filledSize != 1 || lob.bestAskSize() != 99)
        allThreadsOk.store(false);
    });
  }

  for (std::thread &t : threads)
    t.join();

  REQUIRE(allThreadsOk.load());
  REQUIRE(historical->bestAskSize() == 100);
}
