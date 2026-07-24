#pragma once

#include "core/BacktestConfig.hpp"
#include "market/HistoricalLOBStore.hpp"
#include "scheduler/SchedulerRuntime.hpp"
#include "trading/PositionKeeper.hpp"
#include "trading/Strategy.hpp"

#include <deque>
#include <map>
#include <set>
#include <span>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace cmf::trading {

class TradingError : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

class TradingEngine final : public StrategyContext {
public:
  TradingEngine(std::span<const InstrumentMeta> instruments,
                BacktestConfig config, const market::HistoricalLOBStore &books,
                Strategy &strategy, Recorder &recorder);

  void operator()(const ScheduledEvent &event,
                  scheduler::CommandSink &commands);

  [[nodiscard]] ClOrdId submit_limit(InstrumentId instrument_id, Side side,
                                     PriceTicks limit_price_ticks,
                                     Quantity quantity) override;
  bool cancel_order(ClOrdId client_order_id) override;
  [[nodiscard]] TimestampNs now_ns() const noexcept override { return now_ns_; }
  [[nodiscard]] PositionSnapshot
  position(InstrumentId instrument_id) const override;
  [[nodiscard]] std::span<const OrderQueryRow>
  open_orders(InstrumentId instrument_id) override;

private:
  struct OwnOrder {
    OrderQueryRow query;
    bool cancel_requested{};
  };

  struct RestingKey {
    PriceTicks price{};
    Sequence arrival_sequence{};
    ClOrdId client_order_id{};
  };

  struct BuyFirst {
    bool operator()(const RestingKey &left,
                    const RestingKey &right) const noexcept;
  };

  struct SellFirst {
    bool operator()(const RestingKey &left,
                    const RestingKey &right) const noexcept;
  };

  struct ConsumptionKey {
    InstrumentId instrument_id{};
    Side historical_side{Side::None};
    ExchangeOrderId exchange_order_id{};
    Sequence liquidity_revision{};

    bool operator==(const ConsumptionKey &) const = default;
  };

  struct ConsumptionHash {
    std::size_t operator()(const ConsumptionKey &key) const noexcept;
  };

  struct InstrumentOrders {
    std::map<RestingKey, ClOrdId, BuyFirst> buys;
    std::map<RestingKey, ClOrdId, SellFirst> sells;
  };

  [[nodiscard]] TimestampNs delayed_arrival() const;
  [[nodiscard]] Sequence next_command_sequence();
  [[nodiscard]] ClOrdId next_client_order_id();
  [[nodiscard]] const InstrumentMeta *
  find_instrument(InstrumentId instrument_id) const noexcept;
  [[nodiscard]] RejectReason validate_order(InstrumentId instrument_id,
                                            Side side, PriceTicks price,
                                            Quantity quantity) const noexcept;

  void process_market(const MarketDelivery &delivery);
  void process_new(const NewOrderCommand &command);
  void process_cancel(const CancelCommand &command);
  void reevaluate(InstrumentId instrument_id, TimestampNs exchange_ts_ns);
  void match_order(OwnOrder &order, const market::LimitOrderBook &book,
                   TimestampNs exchange_ts_ns);
  void apply_fill(OwnOrder &order, PriceTicks price, Quantity quantity,
                  TimestampNs exchange_ts_ns);
  void insert_resting(const OwnOrder &order);
  void erase_resting(const OwnOrder &order);
  void emit_order_event(const OwnOrder &order, OrderLogEventType event_type,
                        RejectReason reason = RejectReason::None);
  void emit_reject(InstrumentId instrument_id, ClOrdId client_order_id,
                   RejectReason reason, TimestampNs exchange_ts_ns);
  void reject_new(OwnOrder &order, RejectReason reason);
  void ensure_active_sink() const;
  void drain_deferred_rejects();

  template <typename Callback>
  void invoke_strategy_callback(Callback &&callback) {
    ++callback_depth_;
    try {
      std::forward<Callback>(callback)();
    } catch (...) {
      --callback_depth_;
      throw;
    }
    --callback_depth_;
    if (callback_depth_ == 0 && !draining_rejects_) {
      drain_deferred_rejects();
    }
  }

  BacktestConfig config_;
  const market::HistoricalLOBStore &books_;
  Strategy &strategy_;
  Recorder &recorder_;
  PositionKeeper positions_;
  std::unordered_map<InstrumentId, InstrumentMeta> instruments_;
  std::unordered_map<ClOrdId, OwnOrder> orders_;
  std::unordered_map<InstrumentId, InstrumentOrders> resting_;
  std::unordered_map<InstrumentId, std::set<ClOrdId>> open_order_ids_;
  std::unordered_map<ConsumptionKey, Quantity, ConsumptionHash> consumption_;
  std::vector<OrderQueryRow> query_buffer_;
  std::deque<RejectView> deferred_rejects_;
  scheduler::CommandSink *active_commands_{};
  TimestampNs now_ns_{};
  Sequence next_command_sequence_{};
  ClOrdId next_client_order_id_{};
  Sequence next_fill_sequence_{};
  Sequence next_reject_sequence_{};
  std::size_t callback_depth_{};
  bool draining_rejects_{};
  bool has_time_{};
};

} // namespace cmf::trading
