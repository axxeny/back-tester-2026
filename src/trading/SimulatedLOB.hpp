#pragma once

#include "core/BacktestConfig.hpp"
#include "core/Events.hpp"
#include "market/LimitOrderBook.hpp"

#include <map>
#include <span>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace cmf::trading {

class SimulatedLOBError : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

struct SyntheticFill {
  ClOrdId client_order_id{};
  PriceTicks price{};
  Quantity quantity{};
};

// The typed private overlay. It owns only this engine's resting orders and
// historical consumption; the shared HistoricalLOB remains immutable here.
class EngineView {
public:
  explicit EngineView(std::span<const InstrumentMeta> instruments);

private:
  friend class SimulatedLOB;

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
  struct PrivateOrder {
    InstrumentId instrument_id{};
    ClOrdId client_order_id{};
    Side side{Side::None};
    PriceTicks limit_price{};
    Quantity remaining_quantity{};
    Sequence arrival_sequence{};
  };
  struct InstrumentOrders {
    std::map<RestingKey, ClOrdId, BuyFirst> buys;
    std::map<RestingKey, ClOrdId, SellFirst> sells;
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

  std::unordered_map<ClOrdId, PrivateOrder> orders_;
  std::unordered_map<InstrumentId, InstrumentOrders> resting_;
  std::unordered_map<ConsumptionKey, Quantity, ConsumptionHash> consumption_;
};

// The sole synthetic-fill authority. TradingEngine supplies accepted/cancelled
// lifecycle events and applies the returned decisions to state and callbacks.
class SimulatedLOB {
public:
  explicit SimulatedLOB(std::span<const InstrumentMeta> instruments);

  [[nodiscard]] std::span<const SyntheticFill>
  accept(ClOrdId client_order_id, InstrumentId instrument_id, Side side,
         PriceTicks limit_price, Quantity remaining_quantity,
         Sequence arrival_sequence, const market::LimitOrderBook *book);

  [[nodiscard]] std::span<const SyntheticFill>
  on_market(InstrumentId instrument_id, const market::LimitOrderBook &book);

  void cancel(ClOrdId client_order_id);

  [[nodiscard]] const EngineView &engine_view() const noexcept { return view_; }

private:
  void reevaluate(InstrumentId instrument_id,
                  const market::LimitOrderBook &book);
  void match(EngineView::PrivateOrder &order,
             const market::LimitOrderBook &book);
  void insert_resting(const EngineView::PrivateOrder &order);
  void erase_resting(const EngineView::PrivateOrder &order);

  EngineView view_;
  std::vector<SyntheticFill> fills_;
};

} // namespace cmf::trading
