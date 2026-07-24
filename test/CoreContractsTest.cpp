#include "common/BasicTypes.hpp"
#include "core/Contracts.hpp"

#include "MiniTest.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <vector>

using namespace cmf;

namespace {

template <typename Enum> constexpr auto encoded(Enum value) noexcept {
  return static_cast<std::underlying_type_t<Enum>>(value);
}

static_assert(sizeof(TimestampNs) == 8);
static_assert(sizeof(PriceTicks) == 8);
static_assert(sizeof(Quantity) == 8);
static_assert(sizeof(InstrumentId) == 8);
static_assert(sizeof(ClOrdId) == 8);
static_assert(sizeof(ExchangeOrderId) == 8);
static_assert(sizeof(Sequence) == 8);
static_assert(std::is_signed_v<TimestampNs>);
static_assert(std::is_signed_v<PriceTicks>);
static_assert(std::is_signed_v<Quantity>);
static_assert(std::is_signed_v<InstrumentId>);
static_assert(std::is_unsigned_v<ClOrdId>);
static_assert(std::is_unsigned_v<ExchangeOrderId>);
static_assert(std::is_unsigned_v<Sequence>);
static_assert(std::is_same_v<NanoTime, TimestampNs>);
static_assert(std::is_same_v<OrderId, ExchangeOrderId>);
static_assert(std::is_same_v<Price, PriceTicks>);
static_assert(std::is_same_v<SecurityId, InstrumentId>);

static_assert(std::is_same_v<std::underlying_type_t<Side>, std::int8_t>);
static_assert(std::is_same_v<std::underlying_type_t<OrderState>, std::uint8_t>);
static_assert(
    std::is_same_v<std::underlying_type_t<RejectReason>, std::uint8_t>);
static_assert(
    std::is_same_v<std::underlying_type_t<EventPriority>, std::uint8_t>);
static_assert(
    std::is_same_v<std::underlying_type_t<CommandType>, std::uint8_t>);
static_assert(
    std::is_same_v<std::underlying_type_t<OrderLogEventType>, std::uint8_t>);
static_assert(
    std::is_same_v<std::underlying_type_t<LiquiditySource>, std::uint8_t>);

static_assert(encoded(Side::Sell) == -1);
static_assert(encoded(Side::None) == 0);
static_assert(encoded(Side::Buy) == 1);
static_assert(encoded(OrderState::PendingNew) == 0);
static_assert(encoded(OrderState::Open) == 1);
static_assert(encoded(OrderState::PartiallyFilled) == 2);
static_assert(encoded(OrderState::Filled) == 3);
static_assert(encoded(OrderState::PendingCancel) == 4);
static_assert(encoded(OrderState::Cancelled) == 5);
static_assert(encoded(OrderState::Rejected) == 6);
static_assert(encoded(RejectReason::None) == 0);
static_assert(encoded(RejectReason::UnknownInstrument) == 1);
static_assert(encoded(RejectReason::InvalidSide) == 2);
static_assert(encoded(RejectReason::NonPositiveQuantity) == 3);
static_assert(encoded(RejectReason::InvalidPrice) == 4);
static_assert(encoded(RejectReason::TickMisalignment) == 5);
static_assert(encoded(RejectReason::DuplicateClientOrderId) == 6);
static_assert(encoded(RejectReason::UnsupportedOrderType) == 7);
static_assert(encoded(RejectReason::UnsupportedTimeInForce) == 8);
static_assert(encoded(RejectReason::UnknownOrder) == 9);
static_assert(encoded(RejectReason::AlreadyTerminal) == 10);
static_assert(encoded(EventPriority::MarketData) == 0);
static_assert(encoded(EventPriority::NewOrder) == 1);
static_assert(encoded(EventPriority::Cancel) == 2);
static_assert(encoded(CommandType::NewOrder) == 0);
static_assert(encoded(CommandType::Cancel) == 1);
static_assert(encoded(OrderLogEventType::Submit) == 0);
static_assert(encoded(OrderLogEventType::Accepted) == 1);
static_assert(encoded(OrderLogEventType::Fill) == 2);
static_assert(encoded(OrderLogEventType::CancelRequest) == 3);
static_assert(encoded(OrderLogEventType::Cancelled) == 4);
static_assert(encoded(OrderLogEventType::Reject) == 5);
static_assert(encoded(LiquiditySource::HistoricalDisplayed) == 0);

static_assert(std::is_trivially_copyable_v<BookLevel>);
static_assert(std::is_trivially_copyable_v<BookUpdateView>);
static_assert(std::is_trivially_copyable_v<TradeView>);
static_assert(std::is_trivially_copyable_v<FillView>);
static_assert(std::is_trivially_copyable_v<RejectView>);
static_assert(std::is_trivially_copyable_v<NewOrderCommand>);
static_assert(std::is_trivially_copyable_v<CancelCommand>);
static_assert(std::is_trivially_copyable_v<ScheduledKey>);
static_assert(std::is_trivially_copyable_v<ScheduledEvent>);
static_assert(std::is_trivially_copyable_v<OrderQueryRow>);
static_assert(std::is_trivially_copyable_v<PositionSnapshot>);
static_assert(std::is_trivially_copyable_v<FillResultRow>);
static_assert(std::is_trivially_copyable_v<OrderLogResultRow>);
static_assert(std::is_trivially_copyable_v<PnlPoint>);
static_assert(std::is_aggregate_v<InstrumentMeta>);
static_assert(std::is_aggregate_v<BacktestConfig>);
static_assert(std::is_aggregate_v<DateRange>);
static_assert(std::is_aggregate_v<BookUpdateView>);
static_assert(std::is_aggregate_v<TradeView>);
static_assert(std::is_aggregate_v<FillView>);
static_assert(std::is_aggregate_v<RejectView>);
static_assert(std::is_aggregate_v<NewOrderCommand>);
static_assert(std::is_aggregate_v<CancelCommand>);

} // namespace

TEST_CASE("Core contracts - config and date policies", "[CoreContracts]") {
  const BacktestConfig config;
  REQUIRE(config.market_data_latency_ns == 0);
  REQUIRE(config.order_latency_ns == 0);
  REQUIRE(config.book_depth == 15);

  const BacktestConfig invalid_config{-1, -2, 0};
  REQUIRE(invalid_config.market_data_latency_ns == -1);
  REQUIRE(invalid_config.order_latency_ns == -2);
  REQUIRE(invalid_config.book_depth == 0);

  const DateRange range{100, 200};
  REQUIRE_FALSE(range.contains_historical(99));
  REQUIRE(range.contains_historical(100));
  REQUIRE(range.contains_historical(200));
  REQUIRE_FALSE(range.contains_historical(201));
  REQUIRE(range.allows_command_arrival(200));
  REQUIRE_FALSE(range.allows_command_arrival(201));

  const DateRange full_range;
  REQUIRE(full_range.contains_historical(
      std::numeric_limits<TimestampNs>::lowest()));
  REQUIRE(
      full_range.contains_historical(std::numeric_limits<TimestampNs>::max()));
}

TEST_CASE("Core contracts - scheduled key order", "[CoreContracts]") {
  std::vector<ScheduledKey> keys{
      {101, EventPriority::MarketData, 1}, {100, EventPriority::Cancel, 1},
      {100, EventPriority::NewOrder, 2},   {100, EventPriority::MarketData, 2},
      {100, EventPriority::NewOrder, 1},   {100, EventPriority::MarketData, 1},
  };

  std::sort(keys.begin(), keys.end());

  const std::vector<ScheduledKey> expected{
      {100, EventPriority::MarketData, 1}, {100, EventPriority::MarketData, 2},
      {100, EventPriority::NewOrder, 1},   {100, EventPriority::NewOrder, 2},
      {100, EventPriority::Cancel, 1},     {101, EventPriority::MarketData, 1},
  };
  REQUIRE(keys == expected);
}

TEST_CASE("Core contracts - callback and command aggregates",
          "[CoreContracts]") {
  const std::array<BookLevel, 1> bids{{{100, 5}}};
  const std::array<BookLevel, 1> asks{{{101, 6}}};
  const BookUpdateView book{7, 1'000, 1'050, 11, true, bids, asks};
  const TradeView trade{7, 1'001, 1'051, 12, Side::Buy, 101, 2};
  const FillView fill{7, 41, Side::Buy, 101, 2, 3, 1'002, 1'052, 13};
  const RejectView reject{7, 42, RejectReason::InvalidPrice, 1'003, 1'053, 14};
  const NewOrderCommand new_order{41, 7, Side::Buy, 101, 5, 1'050, 1'075, 21};
  const CancelCommand cancel{41, 7, 1'060, 1'085, 22};

  REQUIRE(book.instrument_id == 7);
  REQUIRE(book.exchange_ts_ns == 1'000);
  REQUIRE(book.engine_ts_ns == 1'050);
  REQUIRE(book.sequence == 11);
  REQUIRE(book.bids.front().quantity == 5);
  REQUIRE(trade.instrument_id == 7);
  REQUIRE(trade.exchange_ts_ns == 1'001);
  REQUIRE(trade.engine_ts_ns == 1'051);
  REQUIRE(trade.sequence == 12);
  REQUIRE(fill.instrument_id == 7);
  REQUIRE(fill.exchange_ts_ns == 1'002);
  REQUIRE(fill.engine_ts_ns == 1'052);
  REQUIRE(fill.fill_sequence == 13);
  REQUIRE(reject.instrument_id == 7);
  REQUIRE(reject.exchange_ts_ns == 1'003);
  REQUIRE(reject.engine_ts_ns == 1'053);
  REQUIRE(reject.sequence == 14);
  REQUIRE(new_order.instrument_id == 7);
  REQUIRE(new_order.submit_engine_ts_ns == 1'050);
  REQUIRE(new_order.scheduled_arrival_ts_ns == 1'075);
  REQUIRE(new_order.command_sequence == 21);
  REQUIRE(cancel.instrument_id == 7);
  REQUIRE(cancel.submit_engine_ts_ns == 1'060);
  REQUIRE(cancel.scheduled_arrival_ts_ns == 1'085);
  REQUIRE(cancel.command_sequence == 22);
}

TEST_CASE("Core contracts - remaining value aggregates", "[CoreContracts]") {
  const InstrumentMeta instrument{7, 5, 100};
  const ScheduledEvent scheduled{
      {1'075, EventPriority::NewOrder, 21}, 7, 1'075, 31};
  const OrderQueryRow order{
      7, 41, OrderState::PartiallyFilled, Side::Buy, 101, 5, 2, 3, 21};
  const PositionSnapshot position{7, 2, 101.0, 0.0, 4.0};
  const FillResultRow fill_result{
      1'075, 1'075,     7,
      41,    Side::Buy, 101,
      2,     3,         LiquiditySource::HistoricalDisplayed};
  const OrderLogResultRow order_log{1'075,
                                    7,
                                    41,
                                    OrderLogEventType::Fill,
                                    OrderState::PartiallyFilled,
                                    Side::Buy,
                                    101,
                                    5,
                                    2,
                                    3,
                                    RejectReason::None};
  const PnlPoint pnl{1'075, 4.0};

  REQUIRE(instrument.instrument_id == 7);
  REQUIRE(scheduled.key.source_or_command_sequence == 21);
  REQUIRE(scheduled.dispatch_sequence == 31);
  REQUIRE(order.remaining_quantity == 3);
  REQUIRE(position.net_quantity == 2);
  REQUIRE(fill_result.liquidity_source == LiquiditySource::HistoricalDisplayed);
  REQUIRE(order_log.event_type == OrderLogEventType::Fill);
  REQUIRE(pnl.total_pnl == 4.0);
}
