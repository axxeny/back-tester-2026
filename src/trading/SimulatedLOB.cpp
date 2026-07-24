#include "trading/SimulatedLOB.hpp"

#include <algorithm>
#include <functional>
#include <tuple>

namespace cmf::trading {
namespace {

[[nodiscard]] std::size_t mix(std::size_t seed, std::size_t value) noexcept {
  return seed ^ (value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U));
}

} // namespace

bool EngineView::BuyFirst::operator()(const RestingKey &left,
                                      const RestingKey &right) const noexcept {
  return std::tuple{-left.price, left.arrival_sequence, left.client_order_id} <
         std::tuple{-right.price, right.arrival_sequence,
                    right.client_order_id};
}

bool EngineView::SellFirst::operator()(const RestingKey &left,
                                       const RestingKey &right) const noexcept {
  return std::tie(left.price, left.arrival_sequence, left.client_order_id) <
         std::tie(right.price, right.arrival_sequence, right.client_order_id);
}

std::size_t EngineView::ConsumptionHash::operator()(
    const ConsumptionKey &key) const noexcept {
  std::size_t result = std::hash<InstrumentId>{}(key.instrument_id);
  result = mix(result, std::hash<int>{}(static_cast<int>(key.historical_side)));
  result = mix(result, std::hash<ExchangeOrderId>{}(key.exchange_order_id));
  return mix(result, std::hash<Sequence>{}(key.liquidity_revision));
}

EngineView::EngineView(std::span<const InstrumentMeta> instruments) {
  resting_.reserve(instruments.size());
  for (const auto &instrument : instruments) {
    if (!resting_.try_emplace(instrument.instrument_id).second) {
      throw std::invalid_argument("duplicate EngineView instrument");
    }
  }
}

SimulatedLOB::SimulatedLOB(std::span<const InstrumentMeta> instruments)
    : view_(instruments) {
  fills_.reserve(8);
}

std::span<const SyntheticFill>
SimulatedLOB::accept(ClOrdId client_order_id, InstrumentId instrument_id,
                     Side side, PriceTicks limit_price,
                     Quantity remaining_quantity, Sequence arrival_sequence,
                     const market::LimitOrderBook *book) {
  fills_.clear();
  if (view_.resting_.find(instrument_id) == view_.resting_.end()) {
    throw SimulatedLOBError("accepted order references unknown instrument");
  }
  EngineView::PrivateOrder order{
      instrument_id, client_order_id,    side,
      limit_price,   remaining_quantity, arrival_sequence};
  const auto [iterator, inserted] =
      view_.orders_.emplace(client_order_id, order);
  if (!inserted) {
    throw SimulatedLOBError("duplicate private order");
  }
  insert_resting(iterator->second);
  if (book != nullptr) {
    reevaluate(instrument_id, *book);
  }
  return fills_;
}

std::span<const SyntheticFill>
SimulatedLOB::on_market(InstrumentId instrument_id,
                        const market::LimitOrderBook &book) {
  fills_.clear();
  reevaluate(instrument_id, book);
  return fills_;
}

void SimulatedLOB::cancel(ClOrdId client_order_id) {
  const auto iterator = view_.orders_.find(client_order_id);
  if (iterator == view_.orders_.end()) {
    throw SimulatedLOBError("cancel references unknown private order");
  }
  erase_resting(iterator->second);
  view_.orders_.erase(iterator);
}

void SimulatedLOB::reevaluate(InstrumentId instrument_id,
                              const market::LimitOrderBook &book) {
  auto instrument = view_.resting_.find(instrument_id);
  if (instrument == view_.resting_.end()) {
    throw SimulatedLOBError("market update references unknown instrument");
  }

  auto process = [&](auto &index) {
    while (!index.empty()) {
      const ClOrdId id = index.begin()->second;
      auto own = view_.orders_.find(id);
      if (own == view_.orders_.end()) {
        throw SimulatedLOBError("resting-order index is inconsistent");
      }
      const Quantity before = own->second.remaining_quantity;
      match(own->second, book);
      if (own->second.remaining_quantity == 0) {
        view_.orders_.erase(own);
      } else if (own->second.remaining_quantity == before) {
        break;
      }
    }
  };
  process(instrument->second.buys);
  process(instrument->second.sells);
}

void SimulatedLOB::match(EngineView::PrivateOrder &order,
                         const market::LimitOrderBook &book) {
  erase_resting(order);
  book.for_each_marketable_liquidity(
      order.side, order.limit_price,
      [&](const market::HistoricalOrderSlice &slice) {
        const EngineView::ConsumptionKey key{order.instrument_id, slice.side,
                                             slice.exchange_order_id,
                                             slice.liquidity_revision};
        const auto consumed = view_.consumption_.find(key);
        const Quantity consumed_quantity =
            consumed == view_.consumption_.end() ? 0 : consumed->second;
        const Quantity available =
            std::max<Quantity>(0, slice.remaining_quantity - consumed_quantity);
        if (available == 0) {
          return true;
        }
        const Quantity fill_quantity =
            std::min(order.remaining_quantity, available);
        view_.consumption_[key] = consumed_quantity + fill_quantity;
        order.remaining_quantity -= fill_quantity;
        fills_.push_back(
            SyntheticFill{order.client_order_id, slice.price, fill_quantity});
        return order.remaining_quantity != 0;
      });
  if (order.remaining_quantity != 0) {
    insert_resting(order);
  }
}

void SimulatedLOB::insert_resting(const EngineView::PrivateOrder &order) {
  if (order.remaining_quantity == 0) {
    return;
  }
  const EngineView::RestingKey key{order.limit_price, order.arrival_sequence,
                                   order.client_order_id};
  auto &instrument = view_.resting_.at(order.instrument_id);
  if (order.side == Side::Buy) {
    instrument.buys.emplace(key, order.client_order_id);
  } else if (order.side == Side::Sell) {
    instrument.sells.emplace(key, order.client_order_id);
  } else {
    throw SimulatedLOBError("private order has invalid side");
  }
}

void SimulatedLOB::erase_resting(const EngineView::PrivateOrder &order) {
  const EngineView::RestingKey key{order.limit_price, order.arrival_sequence,
                                   order.client_order_id};
  auto &instrument = view_.resting_.at(order.instrument_id);
  if (order.side == Side::Buy) {
    instrument.buys.erase(key);
  } else if (order.side == Side::Sell) {
    instrument.sells.erase(key);
  }
}

} // namespace cmf::trading
