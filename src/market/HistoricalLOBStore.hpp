#pragma once

#include "market/LimitOrderBook.hpp"

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <vector>

namespace cmf::market {

class HistoricalLOBStore {
public:
  LimitOrderBook &apply(const MarketDataEvent &event);

  [[nodiscard]] LimitOrderBook *find(InstrumentId instrument_id) noexcept;
  [[nodiscard]] const LimitOrderBook *
  find(InstrumentId instrument_id) const noexcept;
  [[nodiscard]] std::size_t size() const noexcept { return books_.size(); }
  [[nodiscard]] std::vector<InstrumentId> instrument_ids() const;

private:
  std::unordered_map<InstrumentId, std::unique_ptr<LimitOrderBook>> books_;
};

} // namespace cmf::market
