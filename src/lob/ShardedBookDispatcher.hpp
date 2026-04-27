// Bonus dispatcher: one central routing thread, N worker-owned LOB shards.

#pragma once

#include "lob/BookDispatcher.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

namespace cmf {

struct ShardedDispatchOptions {
  std::size_t worker_count = 2;
  std::size_t queue_capacity = 128;
};

class ShardedBookDispatcher : public DispatcherEngine {
public:
  ShardedBookDispatcher(DispatchOptions dispatch_options,
                        ShardedDispatchOptions shard_options);
  ~ShardedBookDispatcher() override;

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

private:
  struct WorkerCommand {
    enum class Kind {
      ApplyEvent,
      CaptureSnapshot,
    };

    Kind kind{Kind::ApplyEvent};
    MarketDataEvent event{};
    std::size_t event_index = 0;
    std::uint64_t ts_recv = UNDEF_TIMESTAMP;
  };

  struct OrderKey {
    std::uint16_t publisher_id{0};
    std::uint64_t order_id{0};

    bool operator==(const OrderKey &) const = default;
  };

  struct OrderKeyHash {
    std::size_t operator()(const OrderKey &key) const noexcept;
  };

  struct OrderRouteState {
    std::uint32_t instrument_id{0};
    MdSide side{MdSide::None};
    std::int64_t price{UNDEF_PRICE};
    std::uint32_t size{0};
  };

  struct WorkerState {
    explicit WorkerState(DispatchOptions dispatch_options,
                         std::size_t queue_capacity);

    BookDispatcher dispatcher;
    BlockingQueue<WorkerCommand> queue;
    std::thread thread;
  };

  std::size_t workerIndex(std::uint32_t instrument_id) const noexcept;
  void workerLoop(WorkerState &worker);
  static bool isRestingOrder(MdSide side, std::int64_t price,
                             std::uint32_t size) noexcept;
  static OrderKey makeOrderKey(const MarketDataEvent &event) noexcept;
  std::optional<std::uint32_t> resolveInstrumentId(const MarketDataEvent &event,
                                                   bool &ambiguous) const;
  void applyRouteModel(const MarketDataEvent &event, std::uint32_t instrument_id);
  void upsertRoute(const OrderKey &key, std::uint32_t instrument_id, MdSide side,
                   std::int64_t price, std::uint32_t size);
  void eraseRoute(const OrderKey &key, std::uint32_t instrument_id);
  void eraseInstrumentRoutes(std::uint32_t instrument_id);
  void maybeCaptureSnapshot(std::uint32_t worker_index, std::uint32_t instrument_id,
                            const MarketDataEvent &event);
  void aggregateWorkerState();

  DispatchOptions dispatch_options_{};
  ShardedDispatchOptions shard_options_{};
  DispatchStats stats_{};
  std::vector<std::unique_ptr<WorkerState>> workers_;
  std::vector<CapturedSnapshot> snapshots_;
  mutable std::mutex snapshots_mutex_;
  std::unordered_map<OrderKey, std::vector<OrderRouteState>, OrderKeyHash>
      order_routes_;
  bool finished_ = false;
};

} // namespace cmf
