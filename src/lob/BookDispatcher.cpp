#include "lob/BookDispatcher.hpp"

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <utility>

namespace cmf {

namespace {

bool summaryLess(const BookSummary &lhs, const BookSummary &rhs) {
  const bool lhs_active = lhs.best_bid.has_value() || lhs.best_ask.has_value();
  const bool rhs_active = rhs.best_bid.has_value() || rhs.best_ask.has_value();
  if (lhs_active != rhs_active) {
    return lhs_active > rhs_active;
  }
  if (lhs.orders != rhs.orders) {
    return lhs.orders > rhs.orders;
  }
  return lhs.instrument_id < rhs.instrument_id;
}

std::string formatPrice(std::int64_t price) {
  if (price == UNDEF_PRICE) {
    return "undef";
  }
  const bool negative = price < 0;
  const auto magnitude = negative
                             ? static_cast<std::uint64_t>(-(price + 1)) + 1ULL
                             : static_cast<std::uint64_t>(price);
  constexpr std::uint64_t kPriceScale = 1'000'000'000ULL;
  const auto whole = magnitude / kPriceScale;
  const auto fractional = magnitude % kPriceScale;

  std::ostringstream out;
  if (negative) {
    out << '-';
  }
  out << whole << '.' << std::setw(9) << std::setfill('0') << fractional;
  return out.str();
}

std::string formatTop(const std::optional<PriceLevel> &level) {
  if (!level.has_value()) {
    return "none";
  }
  return formatPrice(level->price) + 'x' + std::to_string(level->size);
}

std::string formatLevels(const std::vector<PriceLevel> &levels) {
  std::ostringstream out;
  out << '[';
  for (std::size_t i = 0; i < levels.size(); ++i) {
    if (i != 0) {
      out << ", ";
    }
    out << formatPrice(levels[i].price) << 'x' << levels[i].size;
  }
  out << ']';
  return out.str();
}

std::string formatSnapshot(const BookSnapshotData &snapshot) {
  std::ostringstream out;
  out << "instrument=" << snapshot.instrument_id << " orders="
      << snapshot.orders << " bid_levels=" << snapshot.bid_levels
      << " ask_levels=" << snapshot.ask_levels
      << " best_bid=" << formatTop(snapshot.best_bid)
      << " best_ask=" << formatTop(snapshot.best_ask)
      << " bids=" << formatLevels(snapshot.bids)
      << " asks=" << formatLevels(snapshot.asks);
  return out.str();
}

} // namespace

BookDispatcher::BookDispatcher(DispatchOptions options) : options_(options) {
  if (options_.async_snapshots && options_.max_snapshots > 0 &&
      options_.snapshot_queue_capacity > 0) {
    snapshot_queue_ =
        std::make_unique<BlockingQueue<PendingSnapshot>>(options_.snapshot_queue_capacity);
    snapshot_worker_ = std::thread(&BookDispatcher::snapshotWorkerLoop, this);
  }
}

BookDispatcher::~BookDispatcher() { finish(); }

void BookDispatcher::apply(const MarketDataEvent &event) {
  ++stats_.total_events;
  switch (event.action) {
  case MdAction::Add:
    ++stats_.adds;
    break;
  case MdAction::Cancel:
    ++stats_.cancels;
    break;
  case MdAction::Modify:
    ++stats_.modifies;
    break;
  case MdAction::Clear:
    ++stats_.clears;
    break;
  case MdAction::Trade:
    ++stats_.trades;
    break;
  case MdAction::Fill:
    ++stats_.fills;
    break;
  case MdAction::None:
    ++stats_.none;
    break;
  }

  bool ambiguous = false;
  const auto instrument_id = resolveInstrumentId(event, ambiguous);
  if (!instrument_id.has_value()) {
    if (ambiguous) {
      ++stats_.ambiguous_routes;
    } else {
      ++stats_.unresolved_routes;
    }
    return;
  }

  LimitOrderBook &book = getOrCreateBook(*instrument_id);
  const ApplyResult result = book.apply(event);
  if (result.missing_order) {
    ++stats_.missing_order_events;
  }
  if (result.ignored) {
    ++stats_.ignored_events;
  }
  maybeCaptureSnapshot(book, event);
}

void BookDispatcher::finish() {
  closeSnapshotWorker();
}

std::vector<BookSummary>
BookDispatcher::finalSummaries(std::size_t depth) const {
  std::vector<BookSummary> summaries;
  summaries.reserve(books_.size());
  for (const auto &[instrument_id, book] : books_) {
    (void)instrument_id;
    summaries.push_back(summarise(book, depth));
  }
  std::sort(summaries.begin(), summaries.end(), summaryLess);
  return summaries;
}

std::optional<BookSummary>
BookDispatcher::summaryForInstrument(std::uint32_t instrument_id,
                                     std::size_t depth) const {
  const auto it = books_.find(instrument_id);
  if (it == books_.end()) {
    return std::nullopt;
  }
  return summarise(it->second, depth);
}

LimitOrderBook &BookDispatcher::getOrCreateBook(std::uint32_t instrument_id) {
  const auto [it, inserted] = books_.try_emplace(instrument_id, instrument_id);
  if (inserted) {
    stats_.instruments = books_.size();
  }
  return it->second;
}

std::optional<std::uint32_t>
BookDispatcher::resolveInstrumentId(const MarketDataEvent &event,
                                    bool &ambiguous) const {
  if (event.instrument_id != 0) {
    return event.instrument_id;
  }
  if (event.order_id == 0) {
    return std::nullopt;
  }

  std::optional<std::uint32_t> resolved;
  for (const auto &[instrument_id, book] : books_) {
    if (!book.hasOrder(event.publisher_id, event.order_id)) {
      continue;
    }
    if (resolved.has_value()) {
      ambiguous = true;
      return std::nullopt;
    }
    resolved = instrument_id;
  }
  return resolved;
}

BookSummary BookDispatcher::summarise(const BookSnapshotData &snapshot) {
  return BookSummary{
      .instrument_id = snapshot.instrument_id,
      .orders = snapshot.orders,
      .bid_levels = snapshot.bid_levels,
      .ask_levels = snapshot.ask_levels,
      .best_bid = snapshot.best_bid,
      .best_ask = snapshot.best_ask,
      .snapshot = formatSnapshot(snapshot),
  };
}

BookSummary BookDispatcher::summarise(const LimitOrderBook &book,
                                      std::size_t depth) const {
  return summarise(book.snapshotData(depth));
}

void BookDispatcher::maybeCaptureSnapshot(const LimitOrderBook &book,
                                          const MarketDataEvent &event) {
  if (stats_.snapshots >= options_.max_snapshots) {
    return;
  }
  const bool is_first = stats_.total_events == 1;
  const bool hits_interval =
      options_.snapshot_every > 0 &&
      stats_.total_events % options_.snapshot_every == 0;
  if (!is_first && !hits_interval) {
    return;
  }

  const PendingSnapshot pending{
      .event_index = stats_.total_events,
      .ts_recv = event.ts_recv,
      .snapshot = book.snapshotData(options_.snapshot_depth),
  };
  if (snapshot_queue_ != nullptr) {
    if (snapshot_queue_->push(pending)) {
      ++stats_.snapshots;
      return;
    }
  }

  {
    std::lock_guard lock{snapshots_mutex_};
    snapshots_.push_back(CapturedSnapshot{
        .event_index = pending.event_index,
        .ts_recv = pending.ts_recv,
        .book = summarise(pending.snapshot),
    });
    stats_.snapshots = snapshots_.size();
  }
}

void BookDispatcher::closeSnapshotWorker() {
  if (snapshots_closed_) {
    return;
  }
  snapshots_closed_ = true;
  if (snapshot_queue_ != nullptr) {
    snapshot_queue_->close();
  }
  if (snapshot_worker_.joinable()) {
    snapshot_worker_.join();
  }
  std::lock_guard lock{snapshots_mutex_};
  if (!snapshots_.empty()) {
    std::sort(snapshots_.begin(), snapshots_.end(),
              [](const CapturedSnapshot &lhs, const CapturedSnapshot &rhs) {
                return lhs.event_index < rhs.event_index;
              });
  }
}

void BookDispatcher::snapshotWorkerLoop() {
  PendingSnapshot pending;
  while (snapshot_queue_ != nullptr && snapshot_queue_->pop(pending)) {
    CapturedSnapshot captured{
        .event_index = pending.event_index,
        .ts_recv = pending.ts_recv,
        .book = summarise(pending.snapshot),
    };
    std::lock_guard lock{snapshots_mutex_};
    snapshots_.push_back(std::move(captured));
  }
}

} // namespace cmf
