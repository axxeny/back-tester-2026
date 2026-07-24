#include "trading/TradingEngine.hpp"

#include <algorithm>
#include <limits>
#include <tuple>
#include <utility>

namespace cmf::trading {
namespace {

[[nodiscard]] bool is_terminal(OrderState state) noexcept {
  return state == OrderState::Filled || state == OrderState::Cancelled ||
         state == OrderState::Rejected;
}

[[nodiscard]] std::size_t mix(std::size_t seed, std::size_t value) noexcept {
  return seed ^ (value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U));
}

} // namespace

bool TradingEngine::BuyFirst::operator()(
    const RestingKey &left, const RestingKey &right) const noexcept {
  return std::tuple{-left.price, left.arrival_sequence, left.client_order_id} <
         std::tuple{-right.price, right.arrival_sequence,
                    right.client_order_id};
}

bool TradingEngine::SellFirst::operator()(
    const RestingKey &left, const RestingKey &right) const noexcept {
  return std::tie(left.price, left.arrival_sequence, left.client_order_id) <
         std::tie(right.price, right.arrival_sequence, right.client_order_id);
}

std::size_t TradingEngine::ConsumptionHash::operator()(
    const ConsumptionKey &key) const noexcept {
  std::size_t result = std::hash<InstrumentId>{}(key.instrument_id);
  result = mix(result, std::hash<int>{}(static_cast<int>(key.historical_side)));
  result = mix(result, std::hash<ExchangeOrderId>{}(key.exchange_order_id));
  return mix(result, std::hash<Sequence>{}(key.liquidity_revision));
}

TradingEngine::TradingEngine(std::span<const InstrumentMeta> instruments,
                             BacktestConfig config,
                             const market::HistoricalLOBStore &books,
                             Strategy &strategy, Recorder &recorder)
    : config_(config), books_(books), strategy_(strategy), recorder_(recorder) {
  if (config_.market_data_latency_ns < 0 || config_.order_latency_ns < 0 ||
      config_.book_depth == 0) {
    throw std::invalid_argument("invalid backtest configuration");
  }
  instruments_.reserve(instruments.size());
  resting_.reserve(instruments.size());
  for (const auto &meta : instruments) {
    positions_.register_instrument(meta);
    const auto [iterator, inserted] =
        instruments_.emplace(meta.instrument_id, meta);
    (void)iterator;
    if (!inserted) {
      throw std::invalid_argument("duplicate instrument metadata");
    }
    resting_.try_emplace(meta.instrument_id);
  }
  query_buffer_.reserve(32);
}

TimestampNs TradingEngine::delayed_arrival() const {
  if (config_.order_latency_ns > 0 &&
      now_ns_ >
          std::numeric_limits<TimestampNs>::max() - config_.order_latency_ns) {
    throw TradingError("scheduled command timestamp overflow");
  }
  return now_ns_ + config_.order_latency_ns;
}

Sequence TradingEngine::next_command_sequence() {
  if (next_command_sequence_ == std::numeric_limits<Sequence>::max()) {
    throw TradingError("command sequence exhausted");
  }
  return ++next_command_sequence_;
}

ClOrdId TradingEngine::next_client_order_id() {
  if (next_client_order_id_ == std::numeric_limits<ClOrdId>::max()) {
    throw TradingError("client order id exhausted");
  }
  return ++next_client_order_id_;
}

const InstrumentMeta *
TradingEngine::find_instrument(InstrumentId instrument_id) const noexcept {
  const auto iterator = instruments_.find(instrument_id);
  return iterator == instruments_.end() ? nullptr : &iterator->second;
}

RejectReason TradingEngine::validate_order(InstrumentId instrument_id,
                                           Side side, PriceTicks price,
                                           Quantity quantity) const noexcept {
  const auto *meta = find_instrument(instrument_id);
  if (meta == nullptr) {
    return RejectReason::UnknownInstrument;
  }
  if (side == Side::None) {
    return RejectReason::InvalidSide;
  }
  if (quantity <= 0) {
    return RejectReason::NonPositiveQuantity;
  }
  if (price <= 0) {
    return RejectReason::InvalidPrice;
  }
  if (price % meta->tick_size_ticks != 0) {
    return RejectReason::TickMisalignment;
  }
  return RejectReason::None;
}

void TradingEngine::ensure_active_sink() const {
  if (active_commands_ == nullptr) {
    throw TradingError("strategy command submitted outside an event callback");
  }
}

ClOrdId TradingEngine::submit_limit(InstrumentId instrument_id, Side side,
                                    PriceTicks limit_price_ticks,
                                    Quantity quantity) {
  ensure_active_sink();
  const ClOrdId client_order_id = next_client_order_id();
  OwnOrder order{OrderQueryRow{instrument_id, client_order_id,
                               OrderState::PendingNew, side, limit_price_ticks,
                               quantity, 0, quantity, 0}};
  const auto [iterator, inserted] = orders_.emplace(client_order_id, order);
  if (!inserted) {
    throw TradingError("generated duplicate client order id");
  }
  emit_order_event(iterator->second, OrderLogEventType::Submit);

  const RejectReason reason =
      validate_order(instrument_id, side, limit_price_ticks, quantity);
  if (reason != RejectReason::None) {
    reject_new(iterator->second, reason);
    return client_order_id;
  }

  const Sequence command_sequence = next_command_sequence();
  const NewOrderCommand command{client_order_id,   instrument_id,   side,
                                limit_price_ticks, quantity,        now_ns_,
                                delayed_arrival(), command_sequence};
  if (!active_commands_->push(command)) {
    throw TradingError("command sink closed while submitting order");
  }
  return client_order_id;
}

bool TradingEngine::cancel_order(ClOrdId client_order_id) {
  ensure_active_sink();
  const auto iterator = orders_.find(client_order_id);
  if (iterator == orders_.end()) {
    emit_reject(0, client_order_id, RejectReason::UnknownOrder, now_ns_);
    return false;
  }
  auto &order = iterator->second;
  if (is_terminal(order.query.state)) {
    emit_reject(order.query.instrument_id, client_order_id,
                RejectReason::AlreadyTerminal, now_ns_);
    return false;
  }
  if (order.query.state == OrderState::PendingNew) {
    emit_reject(order.query.instrument_id, client_order_id,
                RejectReason::UnknownOrder, now_ns_);
    return false;
  }
  if (order.cancel_requested) {
    emit_reject(order.query.instrument_id, client_order_id,
                RejectReason::AlreadyTerminal, now_ns_);
    return false;
  }
  order.cancel_requested = true;
  order.query.state = OrderState::PendingCancel;
  emit_order_event(order, OrderLogEventType::CancelRequest);
  const Sequence command_sequence = next_command_sequence();
  const CancelCommand command{client_order_id, order.query.instrument_id,
                              now_ns_, delayed_arrival(), command_sequence};
  if (!active_commands_->push(command)) {
    throw TradingError("command sink closed while submitting cancel");
  }
  return true;
}

PositionSnapshot TradingEngine::position(InstrumentId instrument_id) const {
  return positions_.position(instrument_id);
}

std::span<const OrderQueryRow>
TradingEngine::open_orders(InstrumentId instrument_id) {
  query_buffer_.clear();
  for (const auto &[client_order_id, order] : orders_) {
    (void)client_order_id;
    if (order.query.instrument_id == instrument_id &&
        !is_terminal(order.query.state) && order.query.remaining_quantity > 0) {
      query_buffer_.push_back(order.query);
    }
  }
  std::sort(query_buffer_.begin(), query_buffer_.end(),
            [](const auto &left, const auto &right) {
              return left.client_order_id < right.client_order_id;
            });
  return query_buffer_;
}

void TradingEngine::operator()(const ScheduledEvent &event,
                               scheduler::CommandSink &commands) {
  const TimestampNs event_time = event.key().scheduled_ts_ns;
  if (has_time_ && event_time < now_ns_) {
    throw TradingError("trading virtual clock moved backwards");
  }
  now_ns_ = event_time;
  has_time_ = true;
  active_commands_ = &commands;
  try {
    std::visit(
        [this](const auto &payload) {
          using Payload = std::decay_t<decltype(payload)>;
          if constexpr (std::is_same_v<Payload, MarketDelivery>) {
            process_market(payload);
          } else if constexpr (std::is_same_v<Payload, NewOrderCommand>) {
            process_new(payload);
          } else {
            process_cancel(payload);
          }
        },
        event.payload());
  } catch (...) {
    active_commands_ = nullptr;
    throw;
  }
  active_commands_ = nullptr;
}

void TradingEngine::process_market(const MarketDelivery &delivery) {
  if (find_instrument(delivery.instrument_id) == nullptr) {
    throw TradingError("market delivery references unknown instrument");
  }
  reevaluate(delivery.instrument_id, delivery.exchange_ts_ns);
  for (const auto &trade : delivery.trades) {
    strategy_.on_trade(trade, *this);
  }
  if (delivery.book_update.has_value()) {
    strategy_.on_book_update(*delivery.book_update, *this);
  }
}

void TradingEngine::process_new(const NewOrderCommand &command) {
  const auto iterator = orders_.find(command.client_order_id);
  if (iterator == orders_.end()) {
    emit_reject(command.instrument_id, command.client_order_id,
                RejectReason::UnknownOrder, command.scheduled_arrival_ts_ns);
    return;
  }
  auto &order = iterator->second;
  if (order.query.state != OrderState::PendingNew) {
    emit_reject(command.instrument_id, command.client_order_id,
                RejectReason::AlreadyTerminal, command.scheduled_arrival_ts_ns);
    return;
  }
  if (command.instrument_id != order.query.instrument_id ||
      command.side != order.query.side ||
      command.limit_price_ticks != order.query.limit_price_ticks ||
      command.quantity != order.query.order_quantity) {
    reject_new(order, RejectReason::DuplicateClientOrderId);
    return;
  }

  const auto *book = books_.find(command.instrument_id);
  order.query.exchange_arrival_sequence = command.command_sequence;
  order.query.state = OrderState::Open;
  emit_order_event(order, OrderLogEventType::Accepted);
  insert_resting(order);
  if (book != nullptr) {
    reevaluate(command.instrument_id, command.scheduled_arrival_ts_ns);
  }
}

void TradingEngine::process_cancel(const CancelCommand &command) {
  const auto iterator = orders_.find(command.client_order_id);
  if (iterator == orders_.end()) {
    emit_reject(command.instrument_id, command.client_order_id,
                RejectReason::UnknownOrder, command.scheduled_arrival_ts_ns);
    return;
  }
  auto &order = iterator->second;
  if (is_terminal(order.query.state)) {
    emit_reject(order.query.instrument_id, order.query.client_order_id,
                RejectReason::AlreadyTerminal, command.scheduled_arrival_ts_ns);
    return;
  }
  if (!order.cancel_requested) {
    emit_reject(order.query.instrument_id, order.query.client_order_id,
                RejectReason::UnknownOrder, command.scheduled_arrival_ts_ns);
    return;
  }
  erase_resting(order);
  order.query.state = OrderState::Cancelled;
  order.cancel_requested = false;
  emit_order_event(order, OrderLogEventType::Cancelled);
}

void TradingEngine::reevaluate(InstrumentId instrument_id,
                               TimestampNs exchange_ts_ns) {
  const auto *book = books_.find(instrument_id);
  if (book == nullptr) {
    return;
  }
  auto orders = resting_.find(instrument_id);
  if (orders == resting_.end()) {
    return;
  }

  while (!orders->second.buys.empty()) {
    const ClOrdId id = orders->second.buys.begin()->second;
    auto own = orders_.find(id);
    if (own == orders_.end()) {
      throw TradingError("resting buy index is inconsistent");
    }
    const Quantity before = own->second.query.remaining_quantity;
    match_order(own->second, *book, exchange_ts_ns);
    if (own->second.query.remaining_quantity == before) {
      break;
    }
  }
  while (!orders->second.sells.empty()) {
    const ClOrdId id = orders->second.sells.begin()->second;
    auto own = orders_.find(id);
    if (own == orders_.end()) {
      throw TradingError("resting sell index is inconsistent");
    }
    const Quantity before = own->second.query.remaining_quantity;
    match_order(own->second, *book, exchange_ts_ns);
    if (own->second.query.remaining_quantity == before) {
      break;
    }
  }
}

void TradingEngine::match_order(OwnOrder &order,
                                const market::LimitOrderBook &book,
                                TimestampNs exchange_ts_ns) {
  book.for_each_marketable_liquidity(
      order.query.side, order.query.limit_price_ticks,
      [&](const market::HistoricalOrderSlice &slice) {
        const ConsumptionKey key{order.query.instrument_id, slice.side,
                                 slice.exchange_order_id,
                                 slice.liquidity_revision};
        const auto consumed = consumption_.find(key);
        const Quantity consumed_quantity =
            consumed == consumption_.end() ? 0 : consumed->second;
        const Quantity available =
            std::max<Quantity>(0, slice.remaining_quantity - consumed_quantity);
        if (available == 0) {
          return true;
        }
        const Quantity fill_quantity =
            std::min(order.query.remaining_quantity, available);
        consumption_[key] = consumed_quantity + fill_quantity;
        apply_fill(order, slice.price, fill_quantity, exchange_ts_ns);
        return order.query.remaining_quantity != 0;
      });
}

void TradingEngine::apply_fill(OwnOrder &order, PriceTicks price,
                               Quantity quantity, TimestampNs exchange_ts_ns) {
  erase_resting(order);
  order.query.filled_quantity += quantity;
  order.query.remaining_quantity -= quantity;
  order.query.state = order.query.remaining_quantity == 0
                          ? OrderState::Filled
                          : OrderState::PartiallyFilled;
  positions_.apply_fill(order.query.instrument_id, order.query.side, price,
                        quantity);
  emit_order_event(order, OrderLogEventType::Fill);

  if (next_fill_sequence_ == std::numeric_limits<Sequence>::max()) {
    throw TradingError("fill sequence exhausted");
  }
  const FillView fill{order.query.instrument_id,
                      order.query.client_order_id,
                      order.query.side,
                      price,
                      quantity,
                      order.query.remaining_quantity,
                      exchange_ts_ns,
                      now_ns_,
                      ++next_fill_sequence_};
  recorder_.on_fill(FillResultRow{
      fill.exchange_ts_ns, fill.engine_ts_ns, fill.instrument_id,
      fill.client_order_id, fill.side, fill.price, fill.quantity,
      fill.remaining_quantity, LiquiditySource::HistoricalDisplayed});
  if (order.query.remaining_quantity != 0) {
    insert_resting(order);
  }
  strategy_.on_fill(fill, *this);
}

void TradingEngine::insert_resting(const OwnOrder &order) {
  if (order.query.remaining_quantity == 0 || is_terminal(order.query.state)) {
    return;
  }
  const RestingKey key{order.query.limit_price_ticks,
                       order.query.exchange_arrival_sequence,
                       order.query.client_order_id};
  auto &instrument = resting_.at(order.query.instrument_id);
  if (order.query.side == Side::Buy) {
    instrument.buys.emplace(key, order.query.client_order_id);
  } else {
    instrument.sells.emplace(key, order.query.client_order_id);
  }
}

void TradingEngine::erase_resting(const OwnOrder &order) {
  if (order.query.exchange_arrival_sequence == 0) {
    return;
  }
  const RestingKey key{order.query.limit_price_ticks,
                       order.query.exchange_arrival_sequence,
                       order.query.client_order_id};
  auto &instrument = resting_.at(order.query.instrument_id);
  if (order.query.side == Side::Buy) {
    instrument.buys.erase(key);
  } else if (order.query.side == Side::Sell) {
    instrument.sells.erase(key);
  }
}

void TradingEngine::emit_order_event(const OwnOrder &order,
                                     OrderLogEventType event_type,
                                     RejectReason reason) {
  recorder_.on_order_event(OrderLogResultRow{
      now_ns_,
      order.query.instrument_id,
      order.query.client_order_id,
      event_type,
      order.query.state,
      order.query.side,
      order.query.limit_price_ticks,
      order.query.order_quantity,
      order.query.filled_quantity,
      order.query.remaining_quantity,
      reason,
  });
}

void TradingEngine::emit_reject(InstrumentId instrument_id,
                                ClOrdId client_order_id, RejectReason reason,
                                TimestampNs exchange_ts_ns) {
  if (next_reject_sequence_ == std::numeric_limits<Sequence>::max()) {
    throw TradingError("reject sequence exhausted");
  }
  const RejectView reject{instrument_id, client_order_id,
                          reason,        exchange_ts_ns,
                          now_ns_,       ++next_reject_sequence_};
  recorder_.on_reject(reject);
  strategy_.on_reject(reject, *this);
}

void TradingEngine::reject_new(OwnOrder &order, RejectReason reason) {
  order.query.state = OrderState::Rejected;
  emit_order_event(order, OrderLogEventType::Reject, reason);
  emit_reject(order.query.instrument_id, order.query.client_order_id, reason,
              now_ns_);
}

} // namespace cmf::trading
