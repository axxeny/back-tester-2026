// Sequential dispatcher that routes merged events into one LOB per instrument.

#pragma once

#include "lob/LimitOrderBook.hpp"
#include "ingest/BlockingQueue.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace cmf {

struct DispatchOptions {
  std::size_t snapshot_every = 100'000;
  std::size_t max_snapshots = 3;
  std::size_t snapshot_depth = 5;
  bool async_snapshots = true;
  std::size_t snapshot_queue_capacity = 32;
};

struct DispatchStats {
  std::size_t total_events = 0;
  std::size_t instruments = 0;
  std::size_t snapshots = 0;
  std::size_t missing_order_events = 0;
  std::size_t ignored_events = 0;
  std::size_t unresolved_routes = 0;
  std::size_t ambiguous_routes = 0;
  std::size_t adds = 0;
  std::size_t cancels = 0;
  std::size_t modifies = 0;
  std::size_t clears = 0;
  std::size_t trades = 0;
  std::size_t fills = 0;
  std::size_t none = 0;
};

struct BookSummary {
  std::uint32_t instrument_id{0};
  std::size_t orders = 0;
  std::size_t bid_levels = 0;
  std::size_t ask_levels = 0;
  std::optional<PriceLevel> best_bid;
  std::optional<PriceLevel> best_ask;
  std::string snapshot;
};

struct CapturedSnapshot {
  std::size_t event_index = 0;
  std::uint64_t ts_recv = UNDEF_TIMESTAMP;
  BookSummary book;
};

class DispatcherEngine {
public:
  virtual ~DispatcherEngine() = default;
  virtual void apply(const MarketDataEvent &event) = 0;
  virtual void finish() = 0;
  [[nodiscard]] virtual const DispatchStats &stats() const noexcept = 0;
  [[nodiscard]] virtual const std::vector<CapturedSnapshot> &
  snapshots() const noexcept = 0;
  [[nodiscard]] virtual std::vector<BookSummary>
  finalSummaries(std::size_t depth = 1) const = 0;
};

class BookDispatcher : public DispatcherEngine {
public:
  explicit BookDispatcher(DispatchOptions options = {});
  ~BookDispatcher() override;

  void apply(const MarketDataEvent &event) override;
  void finish() override;

  [[nodiscard]] const DispatchStats &stats() const noexcept override {
    return stats_;
  }
  [[nodiscard]] const std::vector<CapturedSnapshot> &snapshots() const noexcept override {
    return snapshots_;
  }
  [[nodiscard]] std::vector<BookSummary>
  finalSummaries(std::size_t depth = 1) const override;
  [[nodiscard]] std::optional<BookSummary>
  summaryForInstrument(std::uint32_t instrument_id, std::size_t depth = 1) const;

private:
  struct PendingSnapshot {
    std::size_t event_index = 0;
    std::uint64_t ts_recv = UNDEF_TIMESTAMP;
    BookSnapshotData snapshot;
  };

  LimitOrderBook &getOrCreateBook(std::uint32_t instrument_id);
  std::optional<std::uint32_t>
  resolveInstrumentId(const MarketDataEvent &event, bool &ambiguous) const;
  [[nodiscard]] static BookSummary summarise(const BookSnapshotData &snapshot);
  [[nodiscard]] BookSummary summarise(const LimitOrderBook &book,
                                      std::size_t depth) const;
  void maybeCaptureSnapshot(const LimitOrderBook &book,
                            const MarketDataEvent &event);
  void closeSnapshotWorker();
  void snapshotWorkerLoop();

  DispatchOptions options_{};
  DispatchStats stats_{};
  std::map<std::uint32_t, LimitOrderBook> books_;
  std::vector<CapturedSnapshot> snapshots_;
  std::unique_ptr<BlockingQueue<PendingSnapshot>> snapshot_queue_;
  std::thread snapshot_worker_;
  std::mutex snapshots_mutex_;
  bool snapshots_closed_ = false;
};

} // namespace cmf
