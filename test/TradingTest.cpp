#include "market/HistoricalLOBStore.hpp"
#include "scheduler/SchedulerRuntime.hpp"
#include "trading/TradingEngine.hpp"

#include "MiniTest.hpp"

#include <array>
#include <stdexcept>
#include <vector>

namespace {

using namespace cmf;
using namespace cmf::market;
using namespace cmf::scheduler;
using namespace cmf::trading;

MarketDataEvent market_event(Sequence seq, TimestampNs time,
                             ExchangeOrderId order_id, MarketAction action,
                             Side side, PriceTicks price, Quantity quantity) {
  return MarketDataEvent{time,   time, 1,     order_id, seq,
                         action, side, price, quantity, 128};
}

struct RecordingStrategy : Strategy {
  std::vector<FillView> fills;
  std::vector<RejectView> rejects;
  std::vector<PositionSnapshot> positions_in_fill;
  std::vector<std::size_t> open_counts_in_fill;

  void on_fill(const FillView &fill, StrategyContext &context) override {
    fills.push_back(fill);
    positions_in_fill.push_back(context.position(fill.instrument_id));
    open_counts_in_fill.push_back(
        context.open_orders(fill.instrument_id).size());
  }

  void on_reject(const RejectView &reject, StrategyContext &) override {
    rejects.push_back(reject);
  }
};

struct RecordingRecorder final : Recorder {
  std::vector<OrderLogResultRow> orders;
  std::vector<FillResultRow> fills;
  std::vector<RejectView> rejects;

  void on_order_event(const OrderLogResultRow &row) override {
    orders.push_back(row);
  }
  void on_fill(const FillResultRow &row) override { fills.push_back(row); }
  void on_reject(const RejectView &row) override { rejects.push_back(row); }
};

struct SubmitOnFirstMarket : RecordingStrategy {
  ClOrdId order_id{};
  Side side{Side::Buy};
  PriceTicks price{102};
  Quantity quantity{10};
  bool submitted{};

  void on_book_update(const BookUpdateView &,
                      StrategyContext &context) override {
    if (!submitted) {
      submitted = true;
      order_id = context.submit_limit(1, side, price, quantity);
    }
  }
};

BookUpdateView empty_book_view(TimestampNs time, Sequence seq) {
  return BookUpdateView{1, time, time, seq, false, {}, {}};
}

const std::array<InstrumentMeta, 1> instruments{InstrumentMeta{1, 1, 100, 10}};

} // namespace

TEST_CASE("Trading engine validates instrument metadata", "[Trading]") {
  HistoricalLOBStore books;
  RecordingStrategy strategy;
  RecordingRecorder recorder;
  bool rejected = false;
  try {
    const std::array invalid{InstrumentMeta{1, 0, 100, 1}};
    TradingEngine engine(invalid, BacktestConfig{}, books, strategy, recorder);
    (void)engine;
  } catch (const std::invalid_argument &) {
    rejected = true;
  }
  REQUIRE(rejected);
}

TEST_CASE("Real runtime delays order and sweeps multiple historical orders",
          "[Trading]") {
  HistoricalLOBStore books;
  books.apply(market_event(1, 1, 101, MarketAction::Add, Side::Sell, 101, 4));
  books.apply(market_event(2, 2, 102, MarketAction::Add, Side::Sell, 102, 6));

  SubmitOnFirstMarket strategy;
  RecordingRecorder recorder;
  TradingEngine engine(instruments, BacktestConfig{0, 25, 1}, books, strategy,
                       recorder);
  const std::array<BookLevel, 1> asks{{BookLevel{101, 10}}};
  const BookUpdateView view{1, 100, 100, 3, false, {}, asks};
  const std::array events{
      ScheduledEvent{MarketDelivery{1, 100, 100, 3, view, {}}}};
  SchedulerRuntime runtime(SchedulerRuntimeConfig{DateRange{}, 1, 8, 16});
  runtime.run(events, engine);

  REQUIRE(strategy.order_id == 1);
  REQUIRE(strategy.fills.size() == 2);
  REQUIRE(strategy.fills[0].engine_ts_ns == 125);
  REQUIRE(strategy.fills[0].price == 101);
  REQUIRE(strategy.fills[0].quantity == 4);
  REQUIRE(strategy.fills[1].price == 102);
  REQUIRE(strategy.fills[1].quantity == 6);
  REQUIRE(strategy.positions_in_fill.back().net_quantity == 10);
  REQUIRE(strategy.open_counts_in_fill.back() == 0);
  REQUIRE(runtime.processed_sequence() == 2);
}

TEST_CASE("Resting order fills later and private consumption tracks identity",
          "[Trading]") {
  HistoricalLOBStore books;
  books.apply(market_event(1, 1, 50, MarketAction::Add, Side::Sell, 101, 5));
  struct RevisionStrategy final : SubmitOnFirstMarket {
    HistoricalLOBStore &books;
    int callback{};

    explicit RevisionStrategy(HistoricalLOBStore &store) : books(store) {
      price = 100;
      quantity = 8;
    }

    void on_book_update(const BookUpdateView &view,
                        StrategyContext &context) override {
      SubmitOnFirstMarket::on_book_update(view, context);
      ++callback;
      if (callback == 2) {
        books.apply(
            market_event(2, 200, 50, MarketAction::Modify, Side::Sell, 100, 3));
      } else if (callback == 3) {
        books.apply(
            market_event(3, 300, 50, MarketAction::Fill, Side::Sell, 100, 1));
      } else if (callback == 4) {
        books.apply(
            market_event(4, 400, 50, MarketAction::Cancel, Side::Sell, 100, 0));
      } else if (callback == 5) {
        books.apply(
            market_event(5, 500, 50, MarketAction::Add, Side::Sell, 100, 6));
      }
    }
  } strategy(books);
  RecordingRecorder recorder;
  TradingEngine engine(instruments, BacktestConfig{0, 0, 1}, books, strategy,
                       recorder);
  const auto view100 = empty_book_view(100, 10);
  const auto view200 = empty_book_view(200, 20);
  const auto view300 = empty_book_view(300, 30);
  const auto view400 = empty_book_view(400, 40);
  const auto view500 = empty_book_view(500, 50);
  const auto view600 = empty_book_view(600, 60);
  const std::array events{
      ScheduledEvent{MarketDelivery{1, 100, 100, 10, view100, {}}},
      ScheduledEvent{MarketDelivery{1, 200, 200, 20, view200, {}}},
      ScheduledEvent{MarketDelivery{1, 300, 300, 30, view300, {}}},
      ScheduledEvent{MarketDelivery{1, 400, 400, 40, view400, {}}},
      ScheduledEvent{MarketDelivery{1, 500, 500, 50, view500, {}}},
      ScheduledEvent{MarketDelivery{1, 600, 600, 60, view600, {}}}};
  SchedulerRuntime runtime(SchedulerRuntimeConfig{DateRange{}, 1, 8, 16});
  runtime.run(events, engine);

  REQUIRE(strategy.fills.size() == 2);
  REQUIRE(strategy.fills[0].quantity == 3);
  REQUIRE(strategy.fills[0].engine_ts_ns == 300);
  REQUIRE(strategy.fills[1].quantity == 5);
  REQUIRE(strategy.fills[1].engine_ts_ns == 600);
  REQUIRE(engine.position(1).net_quantity == 8);
}

TEST_CASE("Own orders use price-time priority and EngineViews stay isolated",
          "[Trading]") {
  HistoricalLOBStore books;
  books.apply(market_event(1, 1, 90, MarketAction::Add, Side::Sell, 101, 3));

  struct TwoOrders final : RecordingStrategy {
    std::vector<ClOrdId> ids;
    bool submitted{};
    void on_book_update(const BookUpdateView &,
                        StrategyContext &context) override {
      if (!submitted) {
        submitted = true;
        ids.push_back(context.submit_limit(1, Side::Buy, 101, 2));
        ids.push_back(context.submit_limit(1, Side::Buy, 101, 2));
      }
    }
  } first, second;
  RecordingRecorder first_recorder;
  RecordingRecorder second_recorder;
  TradingEngine first_engine(instruments, BacktestConfig{0, 0, 1}, books, first,
                             first_recorder);
  TradingEngine second_engine(instruments, BacktestConfig{0, 0, 1}, books,
                              second, second_recorder);
  const auto view = empty_book_view(100, 2);
  const std::array events{
      ScheduledEvent{MarketDelivery{1, 100, 100, 2, view, {}}}};
  SchedulerRuntime first_runtime(SchedulerRuntimeConfig{DateRange{}, 1, 8, 16});
  SchedulerRuntime second_runtime(
      SchedulerRuntimeConfig{DateRange{}, 1, 8, 16});
  first_runtime.run(events, first_engine);
  second_runtime.run(events, second_engine);

  REQUIRE(first.fills.size() == 2);
  REQUIRE(first.fills[0].client_order_id == first.ids[0]);
  REQUIRE(first.fills[0].quantity == 2);
  REQUIRE(first.fills[1].client_order_id == first.ids[1]);
  REQUIRE(first.fills[1].quantity == 1);
  REQUIRE(second.fills.size() == 2);
  REQUIRE(second_engine.position(1).net_quantity == 3);
}

TEST_CASE("Cancel is delayed and equal-time market fill wins", "[Trading]") {
  HistoricalLOBStore books;
  books.apply(market_event(1, 1, 70, MarketAction::Add, Side::Sell, 101, 2));
  struct CancelStrategy final : RecordingStrategy {
    HistoricalLOBStore &books;
    ClOrdId id{};
    int callbacks{};

    explicit CancelStrategy(HistoricalLOBStore &store) : books(store) {}

    void on_book_update(const BookUpdateView &,
                        StrategyContext &context) override {
      ++callbacks;
      if (callbacks == 1) {
        id = context.submit_limit(1, Side::Buy, 100, 2);
      } else if (callbacks == 2) {
        REQUIRE(context.cancel_order(id));
        books.apply(
            market_event(2, 111, 70, MarketAction::Modify, Side::Sell, 100, 2));
      }
    }
  } strategy(books);
  RecordingRecorder recorder;
  TradingEngine engine(instruments, BacktestConfig{0, 10, 1}, books, strategy,
                       recorder);
  const auto view100 = empty_book_view(100, 10);
  const auto view111 = empty_book_view(111, 11);
  const auto view121 = empty_book_view(121, 12);
  const std::array events{
      ScheduledEvent{MarketDelivery{1, 100, 100, 10, view100, {}}},
      ScheduledEvent{MarketDelivery{1, 111, 111, 11, view111, {}}},
      ScheduledEvent{MarketDelivery{1, 121, 121, 12, view121, {}}}};
  SchedulerRuntime runtime(SchedulerRuntimeConfig{DateRange{}, 1, 8, 16});
  runtime.run(events, engine);

  REQUIRE(strategy.fills.size() == 1);
  REQUIRE(strategy.fills[0].engine_ts_ns == 121);
  REQUIRE(strategy.fills[0].remaining_quantity == 0);
  REQUIRE(strategy.rejects.size() == 1);
  REQUIRE(strategy.rejects[0].reason == RejectReason::AlreadyTerminal);
}

TEST_CASE("Invalid orders and unknown cancels reject deterministically",
          "[Trading]") {
  HistoricalLOBStore books;
  struct InvalidStrategy final : RecordingStrategy {
    bool done{};
    void on_book_update(const BookUpdateView &,
                        StrategyContext &context) override {
      if (!done) {
        done = true;
        (void)context.submit_limit(2, Side::Buy, 100, 1);
        (void)context.submit_limit(1, Side::None, 100, 1);
        (void)context.submit_limit(1, Side::Buy, 101, 1);
        (void)context.submit_limit(1, Side::Buy, 100, 0);
        REQUIRE_FALSE(context.cancel_order(999));
      }
    }
  } strategy;
  RecordingRecorder recorder;
  const std::array ticked{InstrumentMeta{1, 2, 100, 1}};
  TradingEngine engine(ticked, BacktestConfig{}, books, strategy, recorder);
  const auto view = empty_book_view(100, 1);
  const std::array events{
      ScheduledEvent{MarketDelivery{1, 100, 100, 1, view, {}}}};
  SchedulerRuntime runtime(SchedulerRuntimeConfig{DateRange{}, 1, 8, 16});
  runtime.run(events, engine);

  REQUIRE(strategy.rejects.size() == 5);
  REQUIRE(strategy.rejects[0].reason == RejectReason::UnknownInstrument);
  REQUIRE(strategy.rejects[1].reason == RejectReason::InvalidSide);
  REQUIRE(strategy.rejects[2].reason == RejectReason::TickMisalignment);
  REQUIRE(strategy.rejects[3].reason == RejectReason::NonPositiveQuantity);
  REQUIRE(strategy.rejects[4].reason == RejectReason::UnknownOrder);
}

TEST_CASE("Sell sweep respects limit and leaves only protected remainder",
          "[Trading]") {
  HistoricalLOBStore books;
  books.apply(market_event(1, 1, 81, MarketAction::Add, Side::Buy, 100, 2));
  books.apply(market_event(2, 2, 82, MarketAction::Add, Side::Buy, 99, 4));
  books.apply(market_event(3, 3, 83, MarketAction::Add, Side::Buy, 98, 20));
  SubmitOnFirstMarket strategy;
  strategy.side = Side::Sell;
  strategy.price = 99;
  strategy.quantity = 8;
  RecordingRecorder recorder;
  TradingEngine engine(instruments, BacktestConfig{}, books, strategy,
                       recorder);
  const auto view = empty_book_view(100, 4);
  const std::array events{
      ScheduledEvent{MarketDelivery{1, 100, 100, 4, view, {}}}};
  SchedulerRuntime runtime(SchedulerRuntimeConfig{DateRange{}, 1, 8, 16});
  runtime.run(events, engine);

  REQUIRE(strategy.fills.size() == 2);
  REQUIRE(strategy.fills[0].price == 100);
  REQUIRE(strategy.fills[0].quantity == 2);
  REQUIRE(strategy.fills[1].price == 99);
  REQUIRE(strategy.fills[1].quantity == 4);
  const auto open = engine.open_orders(1);
  REQUIRE(open.size() == 1);
  REQUIRE(open[0].remaining_quantity == 2);
  REQUIRE(engine.position(1).net_quantity == -6);
}

TEST_CASE("Commands are globally monotonic and normal cancel is terminal",
          "[Trading]") {
  HistoricalLOBStore books;
  struct SequencingStrategy final : RecordingStrategy {
    std::array<ClOrdId, 2> ids{};
    int callbacks{};

    void on_book_update(const BookUpdateView &,
                        StrategyContext &context) override {
      ++callbacks;
      if (callbacks == 1) {
        ids[0] = context.submit_limit(1, Side::Buy, 90, 1);
        ids[1] = context.submit_limit(1, Side::Sell, 110, 1);
      } else if (callbacks == 2) {
        REQUIRE(context.cancel_order(ids[0]));
      }
    }
  } strategy;
  RecordingRecorder recorder;
  TradingEngine engine(instruments, BacktestConfig{0, 5, 1}, books, strategy,
                       recorder);
  const auto view100 = empty_book_view(100, 1);
  const auto view110 = empty_book_view(110, 2);
  const std::array events{
      ScheduledEvent{MarketDelivery{1, 100, 100, 1, view100, {}}},
      ScheduledEvent{MarketDelivery{1, 110, 110, 2, view110, {}}}};
  std::vector<Sequence> command_sequences;
  SchedulerRuntime runtime(SchedulerRuntimeConfig{DateRange{}, 1, 8, 16});
  runtime.run(events, [&](const ScheduledEvent &event, CommandSink &commands) {
    if (event.priority() != EventPriority::MarketData) {
      command_sequences.push_back(event.key().source_or_command_sequence);
    }
    engine(event, commands);
  });

  REQUIRE(command_sequences == std::vector<Sequence>({1, 2, 3}));
  const auto open = engine.open_orders(1);
  REQUIRE(open.size() == 1);
  REQUIRE(open[0].client_order_id == strategy.ids[1]);
  REQUIRE(recorder.orders.back().event_type == OrderLogEventType::Cancelled);
  REQUIRE(recorder.orders.back().state == OrderState::Cancelled);
}

TEST_CASE("PositionKeeper uses exact multiplier-scaled FIFO realized inputs",
          "[Trading]") {
  PositionKeeper positions;
  positions.register_instrument(InstrumentMeta{1, 1, 100, 10});
  positions.apply_fill(1, Side::Buy, 100, 2);
  positions.apply_fill(1, Side::Sell, 110, 1);
  auto snapshot = positions.position(1);
  REQUIRE(snapshot.net_quantity == 1);
  REQUIRE(snapshot.average_open_price_ticks == 100.0);
  REQUIRE(snapshot.realized_pnl == 1.0);
  positions.apply_fill(1, Side::Sell, 90, 2);
  snapshot = positions.position(1);
  REQUIRE(snapshot.net_quantity == -1);
  REQUIRE(snapshot.average_open_price_ticks == 90.0);
  REQUIRE(snapshot.realized_pnl == 0.0);
}

TEST_CASE("Twenty scripted trading runs are deterministic", "[Trading]") {
  std::vector<PriceTicks> baseline_prices;
  std::vector<Quantity> baseline_quantities;
  for (int iteration = 0; iteration < 20; ++iteration) {
    HistoricalLOBStore books;
    books.apply(market_event(1, 1, 101, MarketAction::Add, Side::Sell, 101, 2));
    books.apply(market_event(2, 2, 102, MarketAction::Add, Side::Sell, 102, 3));
    SubmitOnFirstMarket strategy;
    strategy.quantity = 5;
    RecordingRecorder recorder;
    TradingEngine engine(instruments, BacktestConfig{0, 7, 1}, books, strategy,
                         recorder);
    const auto view = empty_book_view(100, 3);
    const std::array events{
        ScheduledEvent{MarketDelivery{1, 100, 100, 3, view, {}}}};
    SchedulerRuntime runtime(SchedulerRuntimeConfig{DateRange{}, 1, 8, 16});
    runtime.run(events, engine);

    std::vector<PriceTicks> prices;
    std::vector<Quantity> quantities;
    for (const auto &fill : strategy.fills) {
      prices.push_back(fill.price);
      quantities.push_back(fill.quantity);
    }
    if (iteration == 0) {
      baseline_prices = prices;
      baseline_quantities = quantities;
    } else {
      REQUIRE(prices == baseline_prices);
      REQUIRE(quantities == baseline_quantities);
    }
  }
}
