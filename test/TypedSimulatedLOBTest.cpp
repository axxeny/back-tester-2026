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
      simulated.accept(1, 1, Side::Buy, 101'000'000'000, 2, 1, &book);

  REQUIRE(fills.size() == 1);
  REQUIRE(fills[0].client_order_id == 1);
  REQUIRE(fills[0].price == 101'000'000'000);
  REQUIRE(fills[0].quantity == 2);
  REQUIRE(book.best_ask()->quantity == 2);
}

TEST_CASE("Typed EngineViews keep historical consumption isolated",
          "[SimulatedLOB]") {
  market::HistoricalLOBStore books;
  auto &book = books.apply(add(1, 11, Side::Sell, 101, 3));
  trading::SimulatedLOB first(instruments);
  trading::SimulatedLOB second(instruments);

  const auto first_fills = first.accept(1, 1, Side::Buy, 101, 2, 1, &book);
  const auto second_fills = second.accept(1, 1, Side::Buy, 101, 3, 1, &book);

  REQUIRE(first_fills.size() == 1);
  REQUIRE(first_fills[0].quantity == 2);
  REQUIRE(second_fills.size() == 1);
  REQUIRE(second_fills[0].quantity == 3);
  REQUIRE(book.best_ask()->quantity == 3);
}
