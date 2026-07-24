#include "trading/PositionKeeper.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace cmf::trading {

void PositionKeeper::register_instrument(const InstrumentMeta &meta) {
  if (meta.instrument_id <= 0 || meta.tick_size_ticks <= 0 ||
      meta.price_scale <= 0 || meta.contract_multiplier <= 0) {
    throw std::invalid_argument("instrument metadata values must be positive");
  }
  const auto [iterator, inserted] =
      positions_.emplace(meta.instrument_id, Position{meta, 0, 0, {}});
  (void)iterator;
  if (!inserted) {
    throw std::invalid_argument("duplicate instrument metadata");
  }
}

void PositionKeeper::apply_fill(InstrumentId instrument_id, Side side,
                                PriceTicks price, Quantity quantity) {
  auto iterator = positions_.find(instrument_id);
  if (iterator == positions_.end()) {
    throw std::invalid_argument("fill references unknown instrument");
  }
  if (side == Side::None || price <= 0 || quantity <= 0) {
    throw std::invalid_argument("invalid fill");
  }

  auto &position = iterator->second;
  Quantity remaining = quantity;
  while (remaining > 0 && !position.lots.empty() &&
         position.lots.front().side != side) {
    auto &lot = position.lots.front();
    const Quantity closing = std::min(remaining, lot.quantity);
    const __int128 delta =
        lot.side == Side::Buy
            ? static_cast<__int128>(price) - static_cast<__int128>(lot.price)
            : static_cast<__int128>(lot.price) - static_cast<__int128>(price);
    const __int128 realized =
        delta * static_cast<__int128>(closing) *
        static_cast<__int128>(position.meta.contract_multiplier);
    const __int128 accumulated =
        static_cast<__int128>(position.realized_numerator) + realized;
    if (accumulated < std::numeric_limits<std::int64_t>::min() ||
        accumulated > std::numeric_limits<std::int64_t>::max()) {
      throw std::overflow_error("realized PnL numerator overflow");
    }
    position.realized_numerator = static_cast<std::int64_t>(accumulated);
    lot.quantity -= closing;
    remaining -= closing;
    if (lot.quantity == 0) {
      position.lots.pop_front();
    }
  }
  if (remaining > 0) {
    position.lots.push_back(Position::Lot{side, price, remaining});
  }

  const __int128 signed_fill = side == Side::Buy
                                   ? static_cast<__int128>(quantity)
                                   : -static_cast<__int128>(quantity);
  const __int128 updated =
      static_cast<__int128>(position.net_quantity) + signed_fill;
  if (updated < std::numeric_limits<Quantity>::min() ||
      updated > std::numeric_limits<Quantity>::max()) {
    throw std::overflow_error("position quantity overflow");
  }
  position.net_quantity = static_cast<Quantity>(updated);
}

PositionSnapshot PositionKeeper::position(InstrumentId instrument_id) const {
  const auto iterator = positions_.find(instrument_id);
  if (iterator == positions_.end()) {
    return PositionSnapshot{instrument_id};
  }
  const auto &position = iterator->second;
  long double weighted_ticks = 0;
  long double open_quantity = 0;
  for (const auto &lot : position.lots) {
    weighted_ticks += static_cast<long double>(lot.price) *
                      static_cast<long double>(lot.quantity);
    open_quantity += static_cast<long double>(lot.quantity);
  }
  const double average =
      open_quantity == 0 ? 0.0
                         : static_cast<double>(weighted_ticks / open_quantity);
  return PositionSnapshot{instrument_id, position.net_quantity, average,
                          static_cast<double>(position.realized_numerator) /
                              static_cast<double>(position.meta.price_scale),
                          0.0};
}

} // namespace cmf::trading
