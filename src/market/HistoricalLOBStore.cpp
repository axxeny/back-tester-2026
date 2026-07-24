#include "market/HistoricalLOBStore.hpp"

#include <algorithm>

namespace cmf::market {

LimitOrderBook &HistoricalLOBStore::apply(const MarketDataEvent &event) {
  auto &book = books_[event.instrument_id];
  if (!book) {
    book = std::make_unique<LimitOrderBook>();
  }
  book->apply(event);
  return *book;
}

LimitOrderBook *HistoricalLOBStore::find(InstrumentId instrument_id) noexcept {
  const auto iterator = books_.find(instrument_id);
  return iterator == books_.end() ? nullptr : iterator->second.get();
}

const LimitOrderBook *
HistoricalLOBStore::find(InstrumentId instrument_id) const noexcept {
  const auto iterator = books_.find(instrument_id);
  return iterator == books_.end() ? nullptr : iterator->second.get();
}

std::vector<InstrumentId> HistoricalLOBStore::instrument_ids() const {
  std::vector<InstrumentId> result;
  result.reserve(books_.size());
  for (const auto &[instrument_id, book] : books_) {
    (void)book;
    result.push_back(instrument_id);
  }
  std::sort(result.begin(), result.end());
  return result;
}

} // namespace cmf::market
