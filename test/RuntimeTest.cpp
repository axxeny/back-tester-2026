#include "runtime/BacktestRuntime.hpp"

#include "MiniTest.hpp"
#include "TempFile.hpp"
#include "market/JsonlReader.hpp"

#include <array>
#include <fstream>
#include <string>
#include <vector>

namespace {

using namespace cmf;

class RuntimeStrategy final : public trading::Strategy {
public:
  void on_book_update(const BookUpdateView &view,
                      trading::StrategyContext &context) override {
    callback_order.push_back("book");
    sequences.push_back(view.sequence);
    bid_counts.push_back(view.bids.size());
    ask_counts.push_back(view.asks.size());
    if (order_id == 0) {
      order_id = context.submit_limit(view.instrument_id, Side::Buy,
                                      101'000'000'000, 2);
      submit_time = context.now_ns();
    }
  }

  void on_fill(const FillView &fill,
               trading::StrategyContext &context) override {
    callback_order.push_back("fill");
    fill_time = fill.engine_ts_ns;
    position_in_fill = context.position(fill.instrument_id).net_quantity;
  }

  std::vector<std::string> callback_order;
  std::vector<Sequence> sequences;
  std::vector<std::size_t> bid_counts;
  std::vector<std::size_t> ask_counts;
  ClOrdId order_id{};
  TimestampNs submit_time{};
  TimestampNs fill_time{};
  Quantity position_in_fill{};
};

} // namespace

TEST_CASE("Runtime streams atomic group into real trading and results",
          "[Runtime]") {
  TempFile source("back-tester-runtime-atomic.jsonl");
  {
    std::ofstream output(source.getPath());
    output
        << R"({"ts_recv":"1970-01-01T00:00:00.000000100Z","hd":{"ts_event":"1970-01-01T00:00:00.000000100Z","instrument_id":1},"action":"A","side":"B","price":"99","size":3,"order_id":"10","flags":0,"sequence":1})"
        << '\n'
        << R"({"ts_recv":"1970-01-01T00:00:00.000000100Z","hd":{"ts_event":"1970-01-01T00:00:00.000000100Z","instrument_id":1},"action":"A","side":"A","price":"101","size":2,"order_id":"11","flags":128,"sequence":2})"
        << '\n';
  }

  RuntimeStrategy strategy;
  const std::vector instruments{InstrumentMeta{1, 1, 1'000'000'000, 1}};
  const auto frozen =
      runtime::run_backtest(strategy, source.getPath().string(), DateRange{},
                            BacktestConfig{0, 5, 15}, instruments);

  REQUIRE(strategy.callback_order ==
          std::vector<std::string>({"book", "fill"}));
  REQUIRE(strategy.sequences == std::vector<Sequence>({2}));
  REQUIRE(strategy.bid_counts == std::vector<std::size_t>({1}));
  REQUIRE(strategy.ask_counts == std::vector<std::size_t>({1}));
  REQUIRE(strategy.fill_time == strategy.submit_time + 5);
  REQUIRE(strategy.position_in_fill == 2);
  REQUIRE(frozen.fills().size() == 1);
  REQUIRE(frozen.order_log().size() == 3);
}

TEST_CASE("Runtime rejects an unterminated atomic source group", "[Runtime]") {
  TempFile source("back-tester-runtime-unterminated.jsonl");
  {
    std::ofstream output(source.getPath());
    output
        << R"({"ts_recv":"1970-01-01T00:00:00.000000100Z","hd":{"ts_event":"1970-01-01T00:00:00.000000100Z","instrument_id":1},"action":"A","side":"B","price":"99","size":3,"order_id":"10","flags":0,"sequence":1})"
        << '\n';
  }
  RuntimeStrategy strategy;
  bool rejected = false;
  try {
    (void)runtime::run_backtest(
        strategy, source.getPath().string(), DateRange{},
        BacktestConfig{0, 5, 15},
        std::vector{InstrumentMeta{1, 1, 1'000'000'000, 1}});
  } catch (const market::SourceError &) {
    rejected = true;
  }
  REQUIRE(rejected);
}
