#include "market/HistoricalLOBStore.hpp"
#include "trading/SimulatedLOB.hpp"

#include "MiniTest.hpp"

#include <array>

namespace {

using namespace cmf;

market::MarketDataEvent add(Sequence sequence, ExchangeOrderId order_id,
                            Side side, PriceTicks price, Quantity quantity) {
  return market::MarketDataEvent{
      100,  100,   1,        order_id, sequence, market::MarketAction::Add,
      side, price, quantity, 128};
}

constexpr std::array<InstrumentMeta, 1> instruments{
    InstrumentMeta{1, 1, 1'000'000'000, 1}};

} // namespace

TEST_CASE("Typed SimulatedLOB owns the M5 golden matching decision",
          "[SimulatedLOB]") {
  market::HistoricalLOBStore books;
  auto &book = books.apply(add(1, 11, Side::Sell, 101'000'000'000, 2));
  trading::SimulatedLOB simulated(instruments);

  const auto fills =
      simulated.accept(1, 1, Side::Buy, 101'000'000'000, 20, 1, &book);

  REQUIRE(fills.size() == 1);
  REQUIRE(fills[0].client_order_id == 1);
  REQUIRE(fills[0].price == 101'000'000'000);
  REQUIRE(fills[0].quantity == 20);
  REQUIRE(fills[0].liquidity_source == LiquiditySource::QuoteCross);
  REQUIRE(fills[0].trigger_source_sequence == 1);
  REQUIRE(book.best_ask()->quantity == 2);
}

TEST_CASE("Typed EngineViews independently use infinite quote liquidity",
          "[SimulatedLOB]") {
  market::HistoricalLOBStore books;
  auto &book = books.apply(add(1, 11, Side::Sell, 101, 3));
  trading::SimulatedLOB first(instruments);
  trading::SimulatedLOB second(instruments);

  const auto first_fills = first.accept(1, 1, Side::Buy, 101, 20, 1, &book);
  const auto second_fills = second.accept(1, 1, Side::Buy, 101, 30, 1, &book);

  REQUIRE(first_fills.size() == 1);
  REQUIRE(first_fills[0].quantity == 20);
  REQUIRE(second_fills.size() == 1);
  REQUIRE(second_fills[0].quantity == 30);
  REQUIRE(book.best_ask()->quantity == 3);
}

TEST_CASE("One price-only trade fills all eligible buys and sells",
          "[SimulatedLOB]") {
  trading::SimulatedLOB simulated(instruments);
  REQUIRE(simulated.accept(1, 1, Side::Buy, 100, 40, 1, nullptr).empty());
  REQUIRE(simulated.accept(2, 1, Side::Sell, 100, 50, 2, nullptr).empty());

  const PriceCrossSignal trade{
      1, 200, 205, 7, PriceCrossSource::Trade, std::nullopt, std::nullopt, 100};
  const auto fills = simulated.on_signal(trade);

  REQUIRE(fills.size() == 2);
  REQUIRE(fills[0].client_order_id == 1);
  REQUIRE(fills[0].quantity == 40);
  REQUIRE(fills[0].price == 100);
  REQUIRE(fills[0].liquidity_source == LiquiditySource::TradeCross);
  REQUIRE(fills[0].trigger_source_sequence == 7);
  REQUIRE(fills[1].client_order_id == 2);
  REQUIRE(fills[1].quantity == 50);
  REQUIRE(fills[1].price == 100);
  REQUIRE(fills[1].liquidity_source == LiquiditySource::TradeCross);
  REQUIRE(fills[1].trigger_source_sequence == 7);
}
