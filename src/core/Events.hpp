#pragma once

#include "core/Types.hpp"

#include <compare>
#include <span>

namespace cmf {

struct BookLevel {
  PriceTicks price{};
  Quantity quantity{};
};

struct BookUpdateView {
  InstrumentId instrument_id{};
  TimestampNs exchange_ts_ns{};
  TimestampNs engine_ts_ns{};
  Sequence sequence{};
  bool is_snapshot{};
  std::span<const BookLevel> bids;
  std::span<const BookLevel> asks;
};

struct TradeView {
  InstrumentId instrument_id{};
  TimestampNs exchange_ts_ns{};
  TimestampNs engine_ts_ns{};
  Sequence sequence{};
  Side aggressor_side{Side::None};
  PriceTicks price{};
  Quantity quantity{};
};

struct FillView {
  InstrumentId instrument_id{};
  ClOrdId client_order_id{};
  Side side{Side::None};
  PriceTicks price{};
  Quantity quantity{};
  Quantity remaining_quantity{};
  TimestampNs exchange_ts_ns{};
  TimestampNs engine_ts_ns{};
  Sequence fill_sequence{};
};

struct RejectView {
  InstrumentId instrument_id{};
  ClOrdId client_order_id{};
  RejectReason reason{RejectReason::None};
  TimestampNs exchange_ts_ns{};
  TimestampNs engine_ts_ns{};
  Sequence sequence{};
};

struct NewOrderCommand {
  ClOrdId client_order_id{};
  InstrumentId instrument_id{};
  Side side{Side::None};
  PriceTicks limit_price_ticks{};
  Quantity quantity{};
  TimestampNs submit_engine_ts_ns{};
  TimestampNs scheduled_arrival_ts_ns{};
  Sequence command_sequence{};
};

struct CancelCommand {
  ClOrdId client_order_id{};
  InstrumentId instrument_id{};
  TimestampNs submit_engine_ts_ns{};
  TimestampNs scheduled_arrival_ts_ns{};
  Sequence command_sequence{};
};

struct ScheduledKey {
  TimestampNs scheduled_ts_ns{};
  EventPriority priority{EventPriority::MarketData};
  Sequence source_or_command_sequence{};

  auto operator<=>(const ScheduledKey &) const = default;
};

struct ScheduledEvent {
  ScheduledKey key;
  InstrumentId instrument_id{};
  TimestampNs exchange_ts_ns{};
  Sequence dispatch_sequence{};
};

struct OrderQueryRow {
  InstrumentId instrument_id{};
  ClOrdId client_order_id{};
  OrderState state{OrderState::PendingNew};
  Side side{Side::None};
  PriceTicks limit_price_ticks{};
  Quantity order_quantity{};
  Quantity filled_quantity{};
  Quantity remaining_quantity{};
  Sequence exchange_arrival_sequence{};
};

struct PositionSnapshot {
  InstrumentId instrument_id{};
  Quantity net_quantity{};
  double average_open_price_ticks{};
  double realized_pnl{};
  double unrealized_pnl{};
};

} // namespace cmf
