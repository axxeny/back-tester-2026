#include "runtime/BacktestRuntime.hpp"

#include "market/HistoricalLOBStore.hpp"
#include "market/JsonlReader.hpp"
#include "scheduler/SchedulerRuntime.hpp"
#include "trading/TradingEngine.hpp"

#include <algorithm>
#include <limits>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace cmf::runtime {
namespace {

[[nodiscard]] TimestampNs checked_delivery_time(TimestampNs exchange_time,
                                                TimestampNs latency) {
  TimestampNs result{};
  if (__builtin_add_overflow(exchange_time, latency, &result)) {
    throw market::SourceError({}, 0, {}, "market delivery timestamp overflow");
  }
  return result;
}

[[nodiscard]] bool equal_levels(const std::vector<BookLevel> &left,
                                const std::vector<BookLevel> &right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(),
                    [](const BookLevel &a, const BookLevel &b) {
                      return a.price == b.price && a.quantity == b.quantity;
                    });
}

struct CachedDepth {
  std::vector<BookLevel> bids;
  std::vector<BookLevel> asks;
};

class JsonlScheduledSource {
public:
  JsonlScheduledSource(std::string path,
                       market::JsonlReader::InstrumentMap instruments,
                       market::HistoricalLOBStore &books, DateRange range,
                       BacktestConfig config)
      : path_(path), reader_(std::move(path), std::move(instruments)),
        books_(books), range_(range), config_(config) {
    group_.reserve(8);
    bids_.reserve(config.book_depth);
    asks_.reserve(config.book_depth);
    trades_.reserve(8);
  }

  bool next(ScheduledEvent &scheduled) {
    for (;;) {
      market::MarketDataEvent first;
      if (!reader_.next(first)) {
        return false;
      }

      const InstrumentId instrument_id = first.instrument_id;
      const TimestampNs exchange_time = first.exchange_ts_ns;
      bool contains_clear = false;
      group_.clear();
      group_.push_back(first);
      while (!group_.back().is_last_in_group()) {
        market::MarketDataEvent next_event;
        if (!reader_.next(next_event)) {
          throw market::SourceError(
              path_, reader_.row(), {},
              "unterminated atomic market group at end of file");
        }
        if (next_event.instrument_id != instrument_id ||
            next_event.exchange_ts_ns != exchange_time) {
          throw market::SourceError(
              path_, reader_.row(), {},
              "atomic market group changed instrument or exchange timestamp");
        }
        group_.push_back(next_event);
      }

      if (exchange_time < range_.start_ts_ns) {
        continue;
      }
      if (exchange_time > range_.end_ts_ns) {
        return false;
      }

      trades_.clear();
      for (const auto &event : group_) {
        books_.apply(event);
        contains_clear =
            contains_clear || event.action == market::MarketAction::Clear;
        if (event.action == market::MarketAction::Trade) {
          trades_.push_back(TradeView{
              event.instrument_id,
              event.exchange_ts_ns,
              checked_delivery_time(event.exchange_ts_ns,
                                    config_.market_data_latency_ns),
              event.source_sequence,
              event.side,
              event.price_ticks.value(),
              event.quantity,
          });
        }
      }

      const auto *book = books_.find(instrument_id);
      if (book == nullptr) {
        throw std::logic_error("historical store lost applied instrument");
      }
      bids_.clear();
      asks_.clear();
      for (const auto &level : book->top_bids(config_.book_depth)) {
        bids_.push_back(BookLevel{level.price, level.quantity});
      }
      for (const auto &level : book->top_asks(config_.book_depth)) {
        asks_.push_back(BookLevel{level.price, level.quantity});
      }

      auto &previous = previous_depth_[instrument_id];
      const bool changed = !previous.has_value() ||
                           !equal_levels(previous->bids, bids_) ||
                           !equal_levels(previous->asks, asks_);
      const TimestampNs engine_time =
          checked_delivery_time(exchange_time, config_.market_data_latency_ns);
      std::optional<BookUpdateView> book_update;
      if (changed) {
        previous = CachedDepth{bids_, asks_};
        book_update.emplace(BookUpdateView{
            instrument_id,
            exchange_time,
            engine_time,
            group_.back().source_sequence,
            contains_clear,
            bids_,
            asks_,
        });
      }
      scheduled = ScheduledEvent{MarketDelivery{
          instrument_id,
          exchange_time,
          engine_time,
          group_.back().source_sequence,
          book_update,
          trades_,
      }};
      return true;
    }
  }

private:
  std::string path_;
  market::JsonlReader reader_;
  market::HistoricalLOBStore &books_;
  DateRange range_;
  BacktestConfig config_;
  std::unordered_map<InstrumentId, std::optional<CachedDepth>> previous_depth_;
  std::vector<market::MarketDataEvent> group_;
  std::vector<BookLevel> bids_;
  std::vector<BookLevel> asks_;
  std::vector<TradeView> trades_;
};

void validate(DateRange range, BacktestConfig config,
              const std::vector<InstrumentMeta> &instruments) {
  if (range.start_ts_ns > range.end_ts_ns) {
    throw std::invalid_argument("date range start must not exceed end");
  }
  if (config.market_data_latency_ns < 0 || config.order_latency_ns <= 0 ||
      config.book_depth == 0) {
    throw std::invalid_argument("market latency must be non-negative, order "
                                "latency and depth positive");
  }
  if (instruments.empty()) {
    throw std::invalid_argument("at least one instrument is required");
  }
  std::unordered_set<InstrumentId> ids;
  ids.reserve(instruments.size());
  for (const auto &meta : instruments) {
    if (meta.instrument_id <= 0 || meta.tick_size_ticks <= 0 ||
        meta.price_scale <= 0 || meta.contract_multiplier <= 0) {
      throw std::invalid_argument("instrument metadata must be positive");
    }
    if (!ids.insert(meta.instrument_id).second) {
      throw std::invalid_argument("duplicate instrument metadata");
    }
  }
}

} // namespace

std::vector<InstrumentMeta>
discover_databento_instruments(const std::string &data_path) {
  market::JsonlReader reader(data_path,
                             market::JsonlReader::databento_nanounit_policy());
  std::unordered_set<InstrumentId> unique_ids;
  market::MarketDataEvent event;
  while (reader.next(event)) {
    unique_ids.insert(event.instrument_id);
  }
  std::vector<InstrumentMeta> instruments;
  instruments.reserve(unique_ids.size());
  const auto policy = market::JsonlReader::databento_nanounit_policy();
  for (const InstrumentId instrument_id : unique_ids) {
    instruments.push_back(InstrumentMeta{
        instrument_id,
        policy.tick_size_ticks,
        policy.price_scale,
        policy.contract_multiplier,
    });
  }
  std::sort(instruments.begin(), instruments.end(),
            [](const InstrumentMeta &left, const InstrumentMeta &right) {
              return left.instrument_id < right.instrument_id;
            });
  return instruments;
}

results::FrozenResults run_backtest(trading::Strategy &strategy,
                                    const std::string &data_path,
                                    DateRange date_range, BacktestConfig config,
                                    std::vector<InstrumentMeta> instruments) {
  validate(date_range, config, instruments);
  market::JsonlReader::InstrumentMap metadata;
  metadata.reserve(instruments.size());
  for (const auto &meta : instruments) {
    metadata.emplace(meta.instrument_id, meta);
  }

  market::HistoricalLOBStore books;
  results::ResultRecorder recorder(
      instruments, results::ResultReserveEstimate{64, 128, 128, 16});
  trading::TradingEngine engine(instruments, config, books, strategy, recorder);
  JsonlScheduledSource source(data_path, std::move(metadata), books, date_range,
                              config);
  scheduler::SchedulerRuntime scheduler(
      scheduler::SchedulerRuntimeConfig{date_range, 1, 64, 4096});
  scheduler.run(source, [&](const ScheduledEvent &event,
                            scheduler::CommandSink &commands) {
    engine(event, commands);
    const auto *delivery = std::get_if<MarketDelivery>(&event.payload());
    if (delivery == nullptr || !delivery->book_update.has_value()) {
      return;
    }
    const auto *book = books.find(delivery->instrument_id);
    const auto bid = book->best_bid();
    const auto ask = book->best_ask();
    recorder.on_book_mark(
        delivery->instrument_id, delivery->engine_ts_ns,
        bid.has_value() ? std::optional<PriceTicks>{bid->price} : std::nullopt,
        ask.has_value() ? std::optional<PriceTicks>{ask->price} : std::nullopt);
  });
  return recorder.freeze();
}

} // namespace cmf::runtime
