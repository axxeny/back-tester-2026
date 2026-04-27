#include "lob/ShardedBookDispatcher.hpp"

#include <algorithm>

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

} // namespace

ShardedBookDispatcher::WorkerState::WorkerState(DispatchOptions dispatch_options,
                                                std::size_t queue_capacity)
    : dispatcher([&] {
        DispatchOptions worker_options = dispatch_options;
        worker_options.snapshot_every = 0;
        worker_options.max_snapshots = 0;
        worker_options.async_snapshots = false;
        return worker_options;
      }()),
      queue(queue_capacity) {}

std::size_t
ShardedBookDispatcher::OrderKeyHash::operator()(const OrderKey &key) const
    noexcept {
  const std::size_t lhs = std::hash<std::uint16_t>{}(key.publisher_id);
  const std::size_t rhs = std::hash<std::uint64_t>{}(key.order_id);
  return lhs ^ (rhs + 0x9e3779b97f4a7c15ULL + (lhs << 6U) + (lhs >> 2U));
}

ShardedBookDispatcher::ShardedBookDispatcher(DispatchOptions dispatch_options,
                                             ShardedDispatchOptions shard_options)
    : dispatch_options_(dispatch_options), shard_options_(shard_options) {
  if (shard_options_.worker_count < 2) {
    throw std::invalid_argument("ShardedBookDispatcher: worker_count must be >= 2");
  }
  if (shard_options_.queue_capacity == 0) {
    throw std::invalid_argument(
        "ShardedBookDispatcher: queue_capacity must be >= 1");
  }

  workers_.reserve(shard_options_.worker_count);
  for (std::size_t i = 0; i < shard_options_.worker_count; ++i) {
    workers_.push_back(
        std::make_unique<WorkerState>(dispatch_options_, shard_options_.queue_capacity));
  }
  for (auto &worker : workers_) {
    worker->thread = std::thread(&ShardedBookDispatcher::workerLoop, this,
                                 std::ref(*worker));
  }
}

ShardedBookDispatcher::~ShardedBookDispatcher() { finish(); }

void ShardedBookDispatcher::apply(const MarketDataEvent &event) {
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

  MarketDataEvent routed = event;
  routed.instrument_id = *instrument_id;
  applyRouteModel(routed, *instrument_id);

  const std::size_t worker_index = workerIndex(*instrument_id);
  workers_[worker_index]->queue.push(WorkerCommand{
      .kind = WorkerCommand::Kind::ApplyEvent,
      .event = routed,
  });
  maybeCaptureSnapshot(worker_index, *instrument_id, routed);
}

void ShardedBookDispatcher::finish() {
  if (finished_) {
    return;
  }
  finished_ = true;

  for (auto &worker : workers_) {
    worker->queue.close();
  }
  for (auto &worker : workers_) {
    if (worker->thread.joinable()) {
      worker->thread.join();
    }
    worker->dispatcher.finish();
  }
  aggregateWorkerState();
  std::lock_guard lock{snapshots_mutex_};
  std::sort(snapshots_.begin(), snapshots_.end(),
            [](const CapturedSnapshot &lhs, const CapturedSnapshot &rhs) {
              return lhs.event_index < rhs.event_index;
            });
}

std::vector<BookSummary>
ShardedBookDispatcher::finalSummaries(std::size_t depth) const {
  std::vector<BookSummary> summaries;
  for (const auto &worker : workers_) {
    auto part = worker->dispatcher.finalSummaries(depth);
    summaries.insert(summaries.end(),
                     std::make_move_iterator(part.begin()),
                     std::make_move_iterator(part.end()));
  }
  std::sort(summaries.begin(), summaries.end(), summaryLess);
  return summaries;
}

std::size_t
ShardedBookDispatcher::workerIndex(std::uint32_t instrument_id) const noexcept {
  return static_cast<std::size_t>(instrument_id) % workers_.size();
}

void ShardedBookDispatcher::workerLoop(WorkerState &worker) {
  WorkerCommand command;
  while (worker.queue.pop(command)) {
    switch (command.kind) {
    case WorkerCommand::Kind::ApplyEvent:
      worker.dispatcher.apply(command.event);
      break;
    case WorkerCommand::Kind::CaptureSnapshot: {
      const auto summary = worker.dispatcher.summaryForInstrument(
          command.event.instrument_id, dispatch_options_.snapshot_depth);
      if (!summary.has_value()) {
        break;
      }
      std::lock_guard lock{snapshots_mutex_};
      snapshots_.push_back(CapturedSnapshot{
          .event_index = command.event_index,
          .ts_recv = command.ts_recv,
          .book = *summary,
      });
      break;
    }
    }
  }
}

bool ShardedBookDispatcher::isRestingOrder(MdSide side, std::int64_t price,
                                           std::uint32_t size) noexcept {
  return (side == MdSide::Bid || side == MdSide::Ask) && price != UNDEF_PRICE &&
         size > 0;
}

ShardedBookDispatcher::OrderKey
ShardedBookDispatcher::makeOrderKey(const MarketDataEvent &event) noexcept {
  return OrderKey{event.publisher_id, event.order_id};
}

std::optional<std::uint32_t>
ShardedBookDispatcher::resolveInstrumentId(const MarketDataEvent &event,
                                           bool &ambiguous) const {
  if (event.instrument_id != 0) {
    return event.instrument_id;
  }
  if (event.order_id == 0) {
    return std::nullopt;
  }

  const auto it = order_routes_.find(makeOrderKey(event));
  if (it == order_routes_.end() || it->second.empty()) {
    return std::nullopt;
  }
  if (it->second.size() > 1) {
    ambiguous = true;
    return std::nullopt;
  }
  return it->second.front().instrument_id;
}

void ShardedBookDispatcher::applyRouteModel(const MarketDataEvent &event,
                                            std::uint32_t instrument_id) {
  if (event.order_id == 0 && event.action != MdAction::Clear) {
    return;
  }

  const OrderKey key = makeOrderKey(event);
  switch (event.action) {
  case MdAction::Add:
    if (isRestingOrder(event.side, event.price, event.size)) {
      upsertRoute(key, instrument_id, event.side, event.price, event.size);
    }
    break;
  case MdAction::Cancel: {
    auto it = order_routes_.find(key);
    if (it == order_routes_.end()) {
      break;
    }
    auto route = std::find_if(it->second.begin(), it->second.end(),
                              [&](const OrderRouteState &state) {
                                return state.instrument_id == instrument_id;
                              });
    if (route == it->second.end()) {
      break;
    }
    const std::uint32_t cancel_size =
        event.size == 0 ? route->size : std::min(event.size, route->size);
    if (cancel_size >= route->size) {
      eraseRoute(key, instrument_id);
    } else {
      route->size -= cancel_size;
    }
    break;
  }
  case MdAction::Modify: {
    auto it = order_routes_.find(key);
    if (it == order_routes_.end()) {
      break;
    }
    auto route = std::find_if(it->second.begin(), it->second.end(),
                              [&](const OrderRouteState &state) {
                                return state.instrument_id == instrument_id;
                              });
    if (route == it->second.end()) {
      break;
    }
    MdSide updated_side = route->side;
    if (event.side != MdSide::None) {
      updated_side = event.side;
    }
    std::int64_t updated_price = route->price;
    if (event.price != UNDEF_PRICE) {
      updated_price = event.price;
    }
    if (!isRestingOrder(updated_side, updated_price, event.size)) {
      eraseRoute(key, instrument_id);
      break;
    }
    route->side = updated_side;
    route->price = updated_price;
    route->size = event.size;
    break;
  }
  case MdAction::Clear:
    eraseInstrumentRoutes(instrument_id);
    break;
  case MdAction::Trade:
  case MdAction::Fill:
  case MdAction::None:
    break;
  }
}

void ShardedBookDispatcher::upsertRoute(const OrderKey &key,
                                        std::uint32_t instrument_id,
                                        MdSide side, std::int64_t price,
                                        std::uint32_t size) {
  auto &routes = order_routes_[key];
  const auto it = std::find_if(routes.begin(), routes.end(),
                               [&](const OrderRouteState &state) {
                                 return state.instrument_id == instrument_id;
                               });
  if (it != routes.end()) {
    it->side = side;
    it->price = price;
    it->size = size;
    return;
  }
  routes.push_back(OrderRouteState{
      .instrument_id = instrument_id,
      .side = side,
      .price = price,
      .size = size,
  });
}

void ShardedBookDispatcher::eraseRoute(const OrderKey &key,
                                       std::uint32_t instrument_id) {
  const auto it = order_routes_.find(key);
  if (it == order_routes_.end()) {
    return;
  }
  auto &routes = it->second;
  routes.erase(std::remove_if(routes.begin(), routes.end(),
                              [&](const OrderRouteState &state) {
                                return state.instrument_id == instrument_id;
                              }),
               routes.end());
  if (routes.empty()) {
    order_routes_.erase(it);
  }
}

void ShardedBookDispatcher::eraseInstrumentRoutes(std::uint32_t instrument_id) {
  for (auto it = order_routes_.begin(); it != order_routes_.end();) {
    auto &routes = it->second;
    routes.erase(std::remove_if(routes.begin(), routes.end(),
                                [&](const OrderRouteState &state) {
                                  return state.instrument_id == instrument_id;
                                }),
                 routes.end());
    if (routes.empty()) {
      it = order_routes_.erase(it);
    } else {
      ++it;
    }
  }
}

void ShardedBookDispatcher::maybeCaptureSnapshot(std::uint32_t worker_index,
                                                 std::uint32_t instrument_id,
                                                 const MarketDataEvent &event) {
  if (stats_.snapshots >= dispatch_options_.max_snapshots) {
    return;
  }
  const bool is_first = stats_.total_events == 1;
  const bool hits_interval =
      dispatch_options_.snapshot_every > 0 &&
      stats_.total_events % dispatch_options_.snapshot_every == 0;
  if (!is_first && !hits_interval) {
    return;
  }
  workers_[worker_index]->queue.push(WorkerCommand{
      .kind = WorkerCommand::Kind::CaptureSnapshot,
      .event = MarketDataEvent{.instrument_id = instrument_id},
      .event_index = stats_.total_events,
      .ts_recv = event.ts_recv,
  });
  ++stats_.snapshots;
}

void ShardedBookDispatcher::aggregateWorkerState() {
  stats_.missing_order_events = 0;
  stats_.ignored_events = 0;
  stats_.instruments = 0;
  for (const auto &worker : workers_) {
    const auto &worker_stats = worker->dispatcher.stats();
    stats_.missing_order_events += worker_stats.missing_order_events;
    stats_.ignored_events += worker_stats.ignored_events;
    stats_.instruments += worker_stats.instruments;
  }
}

} // namespace cmf
